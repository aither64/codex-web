const assert = require('node:assert/strict');
const {test} = require('node:test');

const id = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
const limits = {chunkBytes:4, fileBytes:100, promptBytes:100, files:10};
const hash = (value) => require('node:crypto').createHash('sha256').update(value).digest('hex');
const blob = (value) => new File([value], 'input.txt');
const modules = import('../conversation/assets/uploads.js');

test('lost chunk and completion acknowledgements resume without duplicate bytes', async () => {
  const {transferUpload} = await modules;
  let record = {id, name:'input.txt', size:10, offset:0, checksums:[], state:'uploading'};
  const received = [];
  const offsets = [];
  let lostChunk = true, lostComplete = true;
  const client = {
    status: async () => structuredClone(record),
    append: async (_id, offset, checksum, chunk, progress) => {
      assert.equal(offset, record.offset); assert.ok(chunk.size <= 4);
      const bytes = Buffer.from(await chunk.arrayBuffer());
      assert.equal(hash(bytes), checksum); offsets.push(offset); received.push(bytes);
      progress(chunk.size);
      record.offset += chunk.size; record.checksums.push(checksum);
      if (lostChunk) {lostChunk=false;throw Error('ack lost');}
      return structuredClone(record);
    },
    complete: async () => {record.state='ready';if(lostComplete){lostComplete=false;throw Error('completion ack lost');}return record;},
  };
  const progress = [];
  await assert.rejects(transferUpload(client, record, blob('abcdefghij'), limits, (bytes)=>progress.push(bytes)), /completion ack lost/);
  assert.equal((await transferUpload(client, record, blob('abcdefghij'), limits)).state,'ready');
  assert.deepEqual(offsets,[0,4,8]); assert.equal(Buffer.concat(received).toString(),'abcdefghij');
  assert.equal(progress.at(-1),10);
});

test('reselection verifies acknowledged prefix and respects cancellation', async () => {
  const {transferUpload} = await modules;
  const record={id,name:'input.txt',size:8,offset:4,checksums:[hash('abcd')],state:'uploading'};
  let appended=false;
  const client={status:async()=>record,append:async()=>{appended=true;throw Error('unexpected append');}};
  await assert.rejects(transferUpload(client,record,blob('xxxxefgh'),limits),/differs/);
  assert.equal(appended,false);
  const abort=new AbortController();abort.abort();
  await assert.rejects(transferUpload(client,record,blob('abcdefgh'),limits,undefined,abort.signal),{name:'AbortError'});
});

test('attachment-only durable submissions bind IDs and preserve retry identity', async () => {
  const {createDurableSender} = await import('../conversation/assets/conversation.js');
  const values=new Map();
  const storage={getItem:(key)=>values.get(key)??null,setItem:(key,value)=>values.set(key,value),removeItem:(key)=>values.delete(key)};
  const calls=[];
  // See the existing shared browser contracts for ledger lifecycle coverage.
  const sender=createDurableSender({storage,id:'test',basePath:'/codex',client:{message:async(...args)=>{calls.push(args);throw Error('lost');}},randomUUID:()=>id});
  await assert.rejects(sender.send('',[id]),/lost/);
  await assert.rejects(sender.send('',[id]),/lost/);
  assert.equal(calls.length,2);assert.equal(calls[0][1],calls[1][1]);assert.deepEqual(calls[1][3],[id]);
  await assert.rejects(sender.send('different',[]),/pending|different|previous|unresolved/i);
});

test('upload controls can share an action row without owning adjacent actions', async (t) => {
  // This DOM double covers component ownership; native popover behavior is
  // exercised in the portal's real-browser acceptance fixture.
  class Element extends EventTarget {
    children = []; classList = {add() {}, remove() {}}; attributes = new Map(); style = {};
    textContent = ''; hidden = false; open = false;
    append(...children) { for (const child of children) {child.parent = this; this.children.push(child);} }
    replaceChildren(...children) {this.children = []; this.append(...children);}
    remove() {this.parent.children = this.parent.children.filter((child) => child !== this);}
    setAttribute(key, value) {this.attributes.set(key, value);}
    matches() {return this.open;}
    showPopover() {this.open = true;}
    hidePopover() {this.open = false;}
    focus() {}
    getBoundingClientRect() {return {left:100, top:200, bottom:240, width:180, height:50};}
  }
  const events = new EventTarget();
  const globals = {
    addEventListener: events.addEventListener.bind(events), removeEventListener: events.removeEventListener.bind(events),
    document: {createElement: () => new Element(), createElementNS: () => new Element()}, innerWidth: 800, innerHeight: 600,
  };
  for (const [key, value] of Object.entries(globals)) {
    const descriptor = Object.getOwnPropertyDescriptor(globalThis, key);
    Object.defineProperty(globalThis, key, {value, configurable: true});
    t.after(() => {if (descriptor) Object.defineProperty(globalThis, key, descriptor); else delete globalThis[key];});
  }
  const values = new Map();
  const storage = {getItem: (key) => values.get(key), setItem: (key, value) => values.set(key, value)};
  const client = {list: async () => ({limits, files: []})};
  const {mountUploads} = await modules;
  const root = new Element(), actions = new Element(), send = new Element();
  actions.append(send);
  const uploads = mountUploads(root, {client, storage, storageKey: 'draft', controlsRoot: actions});
  await uploads.initialized;
  assert.equal(root.hidden, true);
  assert.equal(actions.children[0], send);
  const [button, picker, menu] = actions.children[1].children;
  assert.equal(button.attributes.get('aria-label'), 'Add attachments');
  button.dispatchEvent(new Event('click', {cancelable:true}));
  assert.equal(menu.open, true);
  uploads.lock(true);
  assert.equal(menu.open, false);
  assert.equal(button.disabled, true);
  assert.equal(picker.multiple, true);
  uploads.destroy();
  assert.deepEqual(actions.children, [send]);
  assert.deepEqual(root.children, []);
  assert.equal(root.hidden, false);
  const fallback = new Element();
  const legacy = mountUploads(fallback, {client, storage, storageKey: 'legacy'});
  await legacy.initialized;
  assert.equal(fallback.hidden, false);
  assert.equal(fallback.children[0].children[0].disabled, false);
  legacy.destroy();
  const errorRoot = new Element();
  const failed = mountUploads(errorRoot, {client: {list: async () => {throw Error('Unavailable');}}, storage, storageKey: 'failed', controlsRoot: actions});
  await failed.initialized;
  assert.equal(errorRoot.hidden, false);
  assert.equal(errorRoot.children[0].textContent, 'Unavailable');
  failed.destroy();
});

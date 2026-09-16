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

// Exercise the shipped component through picker/drop/click events and its durable
// selection, rather than reproducing the upload state machine in the test.
async function uploadComposer(t, options = {}) {
  let root, upload;
  t.after(() => upload?.destroy());
  class Element extends EventTarget {
    children = []; classList = {add() {}, remove() {}}; attributes = new Map(); style = {};
    textContent = ''; hidden = false; open = false;
    append(...children) { for (const child of children) {child.parent = this; this.children.push(child);} }
    replaceChildren(...children) {this.children = []; this.append(...children);}
    remove() {this.parent.children = this.parent.children.filter((child) => child !== this);}
    setAttribute(key, value) {this.attributes.set(key, value);}
    matches() {return this.open;}
    hidePopover() {this.open = false;}
    focus() {}
    click() {}
  }
  const events = new EventTarget();
  for (const [key, value] of Object.entries({
    addEventListener: events.addEventListener.bind(events), removeEventListener: events.removeEventListener.bind(events),
    document: {createElement: () => new Element(), createElementNS: () => new Element()},
  })) {
    const descriptor = Object.getOwnPropertyDescriptor(globalThis, key);
    Object.defineProperty(globalThis, key, {value, configurable: true});
    t.after(() => {if (descriptor) Object.defineProperty(globalThis, key, descriptor); else delete globalThis[key];});
  }
  const values = new Map([['draft', JSON.stringify(options.saved || [])]]);
  const files = new Map((options.files || []).map((file) => [file.id, {...file}]));
  const identities = new Map();
  const calls = {create: [], remove: []};
  const changes = [];
  let failWrites = false;
  const storage = {
    getItem: (key) => values.get(key),
    setItem: (key, value) => {if (failWrites) {if (typeof failWrites === 'number') failWrites -= 1; throw Error('Storage write failed');} values.set(key, value);},
  };
  const client = {
    list: async () => ({limits, files: [...files.values()]}),
    create: async (file, clientId) => {
      calls.create.push({name: file.name, size: file.size, clientId});
      if (options.create) return options.create(file, clientId);
      let record = identities.get(clientId);
      if (!record) {
        record = {id: crypto.randomUUID(), name: file.name, size: file.size, offset: 0, checksums: [], state: 'uploading'};
        identities.set(clientId, record); files.set(record.id, record);
      }
      return {...record};
    },
    status: async (id) => ({...files.get(id)}),
    complete: async (id) => {files.get(id).state = 'ready'; return {...files.get(id)};},
    remove: async (id) => {calls.remove.push(id); if (options.remove) return options.remove(id); files.delete(id);},
    ...options.client,
  };
  const {mountUploads} = await modules;
  const mount = async () => {
    root = new Element();
    upload = mountUploads(root, {client, storage, storageKey: 'draft', onChange: (value) => changes.push(value)});
    await upload.initialized;
  };
  await mount();
  const cards = () => root.children[2].children;
  const card = (name) => cards().find((item) => item.children[0].textContent === name);
  const click = (name, label) => {
    const button = card(name)?.children.find((child) => child.textContent === label);
    assert.ok(button, `missing ${label} for ${name}`); assert.equal(Boolean(button.disabled), false);
    button.dispatchEvent(new Event('click'));
  };
  return {
    client, calls, files, changes, card, click, cards,
    saved: () => JSON.parse(values.get('draft')),
    ready: () => upload.ready(), count: () => upload.count(), ids: () => upload.ids(),
    failWrites: (value) => {failWrites = value;},
    add: (...files) => {
      const event = new Event('drop', {cancelable: true});
      Object.defineProperty(event, 'dataTransfer', {value: {types: ['Files'], files}});
      root.dispatchEvent(event);
    },
    reload: async () => {upload.destroy(); await mount();},
    choose: (name, file, label = 'Choose file to retry') => {
      click(name, label);
      const picker = root.children[0].children[1]; picker.files = [file]; picker.dispatchEvent(new Event('change'));
    },
  };
}
const rejected = (status, message = 'Filename contains control characters') => Object.assign(Error(message), {status});
async function eventually(predicate) {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    if (predicate()) return;
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.ok(predicate(), 'component did not reach the expected state');
}
const zeroFile = (name = 'input.eml') => new File([], name);
const savedDraft = (extra = {}) => ({clientId: id, name: 'input.eml', size: 0, state: 'uploading', ...extra});

test('rejected files can be removed without another request, before and after reload', async (t) => {
  for (const reload of [false, true]) {
    await t.test(`reload=${reload}`, async (t) => {
      const composer = await uploadComposer(t, {create: async () => {throw rejected(400);}});
      composer.add(zeroFile());
      await eventually(() => composer.saved()[0]?.creation === 'rejected');
      if (reload) await composer.reload();
      assert.match(composer.card('input.eml').children[1].textContent, /control characters/);
      assert.equal(composer.ready(), false);
      composer.click('input.eml', 'Remove');
      await eventually(() => composer.count() === 0);
      assert.equal(composer.calls.create.length, 1); assert.deepEqual(composer.calls.remove, []);
      assert.deepEqual(composer.saved(), []); assert.equal(composer.ready(), true);
      assert.deepEqual(composer.changes.at(-1), {ready: true, count: 0});
      await composer.reload(); assert.equal(composer.count(), 0);
    });
  }
});

test('legacy rejected drafts reconcile once and can be discarded', async (t) => {
  for (const status of [400, 413]) {
    await t.test(`status=${status}`, async (t) => {
      const composer = await uploadComposer(t, {saved: [savedDraft()], create: async () => {throw rejected(status);}});
      assert.ok(composer.card('input.eml').children.some((child) => child.textContent === 'Choose file to retry'));
      composer.click('input.eml', 'Remove');
      await eventually(() => composer.count() === 0);
      assert.equal(composer.calls.create.length, 1); assert.deepEqual(composer.calls.remove, []);
      assert.deepEqual(composer.saved(), []);
    });
  }
});

test('never-started drafts can be removed when the upload service is unavailable', async (t) => {
  const composer = await uploadComposer(t, {saved: [savedDraft({creation: 'new'})], client: {list: async () => {throw Error('Offline');}}});
  composer.click('input.eml', 'Remove');
  await eventually(() => composer.count() === 0);
  assert.deepEqual(composer.calls.create, []); assert.deepEqual(composer.calls.remove, []);
  assert.deepEqual(composer.saved(), []); assert.equal(composer.ready(), false);
});

test('unknown creation responses recover the same identity before deletion', async (t) => {
  const record = {id, name: 'input.eml', size: 0, state: 'uploading'};
  let first = true;
  const composer = await uploadComposer(t, {create: async () => {if (first) {first = false; throw Error('Response lost');} return record;}});
  composer.add(zeroFile());
  await eventually(() => composer.saved()[0]?.error === 'Response lost');
  assert.equal(composer.saved()[0].creation, 'unknown');
  await composer.reload();
  composer.click('input.eml', 'Remove');
  await eventually(() => composer.count() === 0);
  assert.equal(composer.calls.create.length, 2);
  assert.equal(composer.calls.create[0].clientId, composer.calls.create[1].clientId);
  assert.deepEqual(composer.calls.remove, [id]);
});

test('expired files and repeated deletion after a lost response clear the draft', async (t) => {
  let first = true;
  const composer = await uploadComposer(t, {
    saved: [savedDraft({id, state: 'ready'})],
    remove: async () => {if (first) {first = false; throw Error('Deletion response lost');} throw rejected(404, 'File is unavailable');},
  });
  assert.equal(composer.card('input.eml').children.some((child) => child.textContent === 'Choose file to resume'), false);
  composer.click('input.eml', 'Remove');
  await eventually(() => composer.saved()[0]?.error === 'Deletion response lost');
  assert.equal(composer.count(), 1);
  await composer.reload();
  composer.click('input.eml', 'Remove');
  await eventually(() => composer.count() === 0);
  assert.deepEqual(composer.calls.remove, [id, id]); assert.deepEqual(composer.saved(), []);
});

test('authorization, busy-file and uncertain server errors keep removal retryable', async (t) => {
  for (const status of [403, 409, 500]) {
    await t.test(`status=${status}`, async (t) => {
      const composer = await uploadComposer(t, {saved: [savedDraft({id})], remove: async () => {throw rejected(status, 'Try again');}});
      composer.click('input.eml', 'Remove');
      await eventually(() => composer.saved()[0]?.error === 'Try again');
      assert.equal(composer.count(), 1);
      composer.click('input.eml', 'Remove');
      await eventually(() => composer.calls.remove.length === 2);
      assert.equal(composer.saved().length, 1);
    });
  }
});

test('storage failure keeps a removed server file visible until local removal is durable', async (t) => {
  const record = savedDraft({id, state: 'ready'});
  const composer = await uploadComposer(t, {saved: [record], files: [record]});
  composer.failWrites(true); composer.click('input.eml', 'Remove');
  await eventually(() => composer.card('input.eml').children[1].textContent === 'Storage write failed');
  assert.equal(composer.count(), 1); assert.equal(composer.saved().length, 1);
  assert.equal(composer.ready(), false);
  composer.failWrites(false);
  composer.client.remove = async () => {throw rejected(404);};
  composer.click('input.eml', 'Remove');
  await eventually(() => composer.count() === 0);
  assert.deepEqual(composer.saved(), []); assert.equal(composer.ready(), true);
});

test('removing a rejected selection preserves other ready attachments', async (t) => {
  const record = savedDraft({id, state: 'ready'});
  const name = '<img src=x onerror=alert(1)>\\SQL;\'--.eml';
  const composer = await uploadComposer(t, {saved: [record], files: [record], create: async () => {throw rejected(400);}});
  composer.add(zeroFile(name));
  await eventually(() => composer.saved()[1]?.creation === 'rejected');
  assert.equal(composer.card(name).children[0].textContent, name);
  composer.click(name, 'Remove');
  await eventually(() => composer.count() === 1);
  assert.deepEqual(composer.ids(), [id]); assert.equal(composer.saved()[0].id, id);
});

test('removal waits for an in-flight creation and prevents transfer after cancellation', async (t) => {
  let finishCreate;
  const promise = new Promise((resolve) => {finishCreate = resolve;});
  let transferred = false;
  const composer = await uploadComposer(t, {create: () => promise, client: {status: async () => {transferred = true;}}});
  composer.add(zeroFile());
  await eventually(() => composer.calls.create.length === 1);
  composer.click('input.eml', 'Remove');
  finishCreate({id, name: 'input.eml', size: 0, state: 'uploading'});
  await eventually(() => composer.count() === 0);
  assert.equal(transferred, false); assert.equal(composer.calls.create.length, 1);
  assert.deepEqual(composer.calls.remove, [id]); assert.deepEqual(composer.saved(), []);
});

test('reselection checks the original identity before retrying creation', async (t) => {
  const composer = await uploadComposer(t, {saved: [savedDraft({creation: 'rejected', error: 'Rejected'})]});
  composer.choose('input.eml', zeroFile('different.eml'));
  await eventually(() => composer.saved()[0]?.error.includes('same file'));
  assert.deepEqual(composer.calls.create, []);
  composer.click('input.eml', 'Remove');
  await eventually(() => composer.count() === 0);
});

test('a transient local removal write failure cannot expose deleted attachment IDs', async (t) => {
  const record = savedDraft({id, state: 'ready'});
  const composer = await uploadComposer(t, {saved: [record], files: [record]});
  composer.failWrites(1); composer.click('input.eml', 'Remove');
  await eventually(() => composer.saved()[0]?.error === 'Storage write failed');
  assert.equal(composer.ready(), false);
  assert.equal(composer.changes.at(-1).ready, false);
  assert.throws(() => composer.ids(), /remove the unfinished files/);
  assert.equal(composer.saved()[0].state, 'deleted');
  composer.click('input.eml', 'Remove');
  await eventually(() => composer.count() === 0);
  assert.equal(composer.ready(), true);
});

test('a failed pre-request write preserves the never-started creation outcome', async (t) => {
  const composer = await uploadComposer(t, {saved: [savedDraft({creation: 'new'})]});
  composer.failWrites(1); composer.choose('input.eml', zeroFile());
  await eventually(() => composer.saved()[0]?.error === 'Storage write failed');
  assert.equal(composer.saved()[0].creation, 'new');
  assert.deepEqual(composer.calls.create, []);
  composer.click('input.eml', 'Remove');
  await eventually(() => composer.count() === 0);
  assert.deepEqual(composer.calls.create, []); assert.deepEqual(composer.calls.remove, []);
});

test('scope-level DELETE 404 does not discard a file still visible to authorized reads', async (t) => {
  const record = savedDraft({id, state: 'ready'});
  const composer = await uploadComposer(t, {saved: [record], files: [record], remove: async () => {throw rejected(404, 'Upload scope is unavailable');}});
  composer.click('input.eml', 'Remove');
  await eventually(() => composer.saved()[0]?.error === 'Upload scope is unavailable');
  assert.equal(composer.count(), 1); assert.equal(composer.saved()[0].id, id);
  composer.client.remove = async () => composer.files.delete(id);
  composer.click('input.eml', 'Remove');
  await eventually(() => composer.count() === 0);
});

test('a failed scope read cannot establish that a DELETE 404 removed the file', async (t) => {
  const composer = await uploadComposer(t, {saved: [savedDraft({id})], remove: async () => {throw rejected(404);}});
  composer.client.list = async () => {throw rejected(403, 'Scope read denied');};
  composer.click('input.eml', 'Remove');
  await eventually(() => composer.saved()[0]?.error === 'Scope read denied');
  assert.equal(composer.count(), 1); assert.equal(composer.saved()[0].id, id);
});

test('a lost deletion response blocks submission until the file is reconciled', async (t) => {
  const record = savedDraft({id, state: 'ready'});
  const composer = await uploadComposer(t, {saved: [record], files: [record]});
  composer.client.remove = async () => {composer.files.delete(id); throw Error('Deletion response lost');};
  composer.click('input.eml', 'Remove');
  await eventually(() => composer.saved()[0]?.error === 'Deletion response lost');
  assert.equal(composer.ready(), false);
  assert.throws(() => composer.ids(), /remove the unfinished files/);
  await composer.reload();
  assert.equal(composer.ready(), false);
  composer.client.remove = async () => {throw rejected(404);};
  composer.click('input.eml', 'Remove');
  await eventually(() => composer.count() === 0);
});

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
  let root, upload, storageKey = 'draft';
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
  const calls = {create: [], remove: [], append: []};
  const changes = [];
  let failWrites = false;
  const storage = {
    getItem: (key) => values.get(key),
    setItem: (key, value) => {if (failWrites) {if (typeof failWrites === 'number') failWrites -= 1; throw Error('Storage write failed');} values.set(key, value);},
  };
  const client = {
    list: async () => ({limits: options.limits || limits, files: [...files.values()]}),
    create: async (file, clientId) => {
      calls.create.push({name: file.name, size: file.size, clientId, type: file.file?.type, lastModified: file.file?.lastModified});
      if (options.create) return options.create(file, clientId);
      let record = identities.get(clientId);
      if (!record) {
        record = {id: crypto.randomUUID(), name: file.name, size: file.size, offset: 0, checksums: [], state: 'uploading'};
        identities.set(clientId, record); files.set(record.id, record);
      }
      return {...record};
    },
    status: async (id) => ({...files.get(id)}),
    append: async (id, offset, checksum, chunk) => {
      const record = files.get(id);
      assert.equal(offset, record.offset);
      const bytes = Buffer.from(await chunk.arrayBuffer());
      assert.equal(hash(bytes), checksum);
      calls.append.push({id, bytes});
      record.offset += chunk.size; record.checksums.push(checksum);
      return {...record};
    },
    complete: async (id) => {files.get(id).state = 'ready'; return {...files.get(id)};},
    remove: async (id) => {calls.remove.push(id); if (options.remove) return options.remove(id); files.delete(id);},
    ...options.client,
  };
  const {mountUploads} = await modules;
  const mount = async () => {
    root = new Element();
    upload = mountUploads(root, {client, storage, pasteTarget: root, storageKey, onChange: (value) => changes.push(value)});
    await upload.initialized;
  };
  await mount();
  const cards = () => root.children.find((child) => child.className === 'codex-attachments').children;
  const card = (name) => cards().find((item) => item.children[0].textContent === name);
  const click = (name, label) => {
    const button = card(name)?.children.find((child) => child.textContent === label);
    assert.ok(button, `missing ${label} for ${name}`); assert.equal(Boolean(button.disabled), false);
    button.dispatchEvent(new Event('click'));
  };
  return {
    client, calls, files, changes, card, click, cards, storage,
    summary: () => root.children.find((child) => child.className === 'codex-upload-summary'),
    notice: () => root.children.find((child) => child.className === 'codex-upload-notice'),
    lock: (value) => upload.lock(value), clear: () => upload.clear(),
    saved: () => JSON.parse(values.get(storageKey)),
    ready: () => upload.ready(), count: () => upload.count(), ids: () => upload.ids(),
    failWrites: (value) => {failWrites = value;},
    add: (...files) => {
      const event = new Event('drop', {cancelable: true});
      Object.defineProperty(event, 'dataTransfer', {value: {types: ['Files'], files}});
      root.dispatchEvent(event);
    },
    pick: (...files) => {
      const picker = root.children[0].children[1]; picker.files = files; picker.dispatchEvent(new Event('change'));
    },
    destroy: () => upload.destroy(),
    paste: (files, text = '', html = '') => {
      const event = new Event('paste', {cancelable: true});
      Object.defineProperty(event, 'clipboardData', {value: {files,
        items: files.map(file => ({kind: 'file', getAsFile: () => file})),
        getData: kind => kind === 'text/plain' ? text : kind === 'text/html' ? html : ''}});
      root.dispatchEvent(event); return event;
    },
    reload: async (key = storageKey) => {upload.destroy(); storageKey = key; await mount();},
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

test('summary uses full sizes during transfer and waits for completion acknowledgement', async (t) => {
  let finishChunk, finishComplete;
  const composer = await uploadComposer(t, {limits: {...limits, fileBytes: 4096, promptBytes: 8192, chunkBytes: 2048}});
  assert.equal(composer.summary().hidden, true);
  composer.client.append = async (fileID, offset, checksum, chunk, progress) => {
    progress(512);
    await new Promise((resolve) => {finishChunk = resolve;});
    const record = composer.files.get(fileID);
    record.offset = offset + chunk.size; record.checksums.push(checksum);
    return {...record};
  };
  composer.client.complete = async (fileID) => {
    await new Promise((resolve) => {finishComplete = resolve;});
    composer.files.get(fileID).state = 'ready';
    return {...composer.files.get(fileID)};
  };
  composer.add(new File([new Uint8Array(2048)], 'large.txt'));
  await eventually(() => finishChunk);
  assert.equal(composer.summary().textContent, '1 file · 2 KiB total · 0 of 1 complete');
  assert.equal(composer.card('large.txt').children[1].textContent, '2 KiB · 25%');
  finishChunk(); await eventually(() => finishComplete);
  assert.equal(composer.summary().textContent, '1 file · 2 KiB total · 0 of 1 complete');
  assert.equal(composer.ready(), false);
  finishComplete(); await eventually(() => composer.ready());
  assert.equal(composer.summary().textContent, '1 file · 2 KiB total');
  composer.client.complete = async (fileID) => {composer.files.get(fileID).state = 'ready'; return {...composer.files.get(fileID)};};
  composer.add(zeroFile('empty.txt')); await eventually(() => composer.ready());
  assert.equal(composer.summary().textContent, '2 files · 2 KiB total');
  composer.lock(true); assert.equal(composer.summary().textContent, '2 files · 2 KiB total');
  assert.equal(composer.card('large.txt').children.at(-1).disabled, true);
  composer.lock(false); composer.clear();
  assert.equal(composer.summary().hidden, true); assert.equal(composer.summary().textContent, '');
  assert.deepEqual(composer.saved(), []); assert.equal(composer.files.size, 2);
});

test('zero-byte failure and retry retain the selection summary separately from errors', async (t) => {
  const composer = await uploadComposer(t);
  composer.client.complete = async () => {throw Error('Completion response lost');};
  composer.add(zeroFile());
  assert.equal(composer.summary().textContent, '1 file · 0 B total · 0 of 1 complete');
  await eventually(() => composer.saved()[0]?.error === 'Completion response lost');
  assert.equal(composer.summary().textContent, '1 file · 0 B total · 0 of 1 complete');
  assert.equal(composer.ready(), false);
  composer.client.complete = async (fileID) => {composer.files.get(fileID).state = 'ready'; return {...composer.files.get(fileID)};};
  composer.click('input.eml', 'Retry'); await eventually(() => composer.ready());
  assert.equal(composer.summary().textContent, '1 file · 0 B total');
  composer.add(new File([new Uint8Array(101)], 'too-large.txt'));
  assert.match(composer.notice().textContent, /exceeds/);
  assert.equal(composer.summary().textContent, '1 file · 0 B total');
  assert.equal(composer.count(), 1);
});

test('restored paused, missing and deleted selections keep their full totals until removed', async (t) => {
  const records = [
    savedDraft({id, name: 'ready.txt', size: 1024, state: 'ready'}),
    savedDraft({clientId: crypto.randomUUID(), id: crypto.randomUUID(), name: 'paused.txt', size: 2048, offset: 512}),
    savedDraft({clientId: crypto.randomUUID(), id: crypto.randomUUID(), name: 'missing.txt', size: 1024}),
    savedDraft({clientId: crypto.randomUUID(), id: crypto.randomUUID(), name: 'deleted.txt', size: 0, state: 'deleted'}),
  ];
  const composer = await uploadComposer(t, {saved: records, files: [records[0], records[1], records[3]]});
  assert.equal(composer.summary().textContent, '4 files · 4 KiB total · 1 of 4 complete');
  await composer.reload();
  assert.equal(composer.summary().textContent, '4 files · 4 KiB total · 1 of 4 complete');
  composer.click('paused.txt', 'Remove'); await eventually(() => composer.count() === 3);
  assert.equal(composer.summary().textContent, '3 files · 2 KiB total · 1 of 3 complete');
  composer.click('missing.txt', 'Remove'); await eventually(() => composer.count() === 2);
  composer.click('deleted.txt', 'Remove'); await eventually(() => composer.count() === 1);
  assert.equal(composer.summary().textContent, '1 file · 1 KiB total');
});

test('ready files awaiting or failing removal remain complete until removal succeeds', async (t) => {
  let finishRemoval;
  const record = savedDraft({id, size: 1024, state: 'ready'});
  const composer = await uploadComposer(t, {saved: [record], files: [record]});
  composer.client.remove = () => new Promise((resolve, reject) => {finishRemoval = () => reject(Error('Busy file'));});
  composer.click('input.eml', 'Remove'); await eventually(() => finishRemoval);
  assert.equal(composer.summary().textContent, '1 file · 1 KiB total');
  assert.equal(composer.ready(), false);
  finishRemoval(); await eventually(() => composer.saved()[0]?.error === 'Busy file');
  assert.equal(composer.summary().textContent, '1 file · 1 KiB total');
  composer.client.remove = async (fileID) => composer.files.delete(fileID);
  composer.click('input.eml', 'Remove'); await eventually(() => composer.count() === 0);
  assert.equal(composer.summary().hidden, true);
});

test('generic attachment ID validation retains its 100-entry ceiling', async () => {
  const {attachmentIDs} = await modules;
  const ids = Array.from({length: 101}, () => crypto.randomUUID());
  assert.deepEqual(attachmentIDs(ids.slice(0, 100)), ids.slice(0, 100));
  assert.throws(() => attachmentIDs(ids), /Invalid attachment IDs/);
});

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


test('clipboard binaries use upload cards once and native text paste remains uncanceled', async t => {
  const composer = await uploadComposer(t);
  assert.equal(composer.paste([], 'ordinary text').defaultPrevented, false);
  assert.equal(composer.count(), 0);
  const file = new File([], 'screenshot.png', {type: 'image/png'});
  assert.equal(composer.paste([file]).defaultPrevented, true);
  await eventually(() => composer.ready());
  assert.equal(composer.count(), 1);
  assert.equal(composer.calls.create.length, 1);
  assert.equal(composer.card('screenshot-1.png').children[0].textContent, 'screenshot-1.png');
  assert.equal(composer.paste([zeroFile('mixed.pdf')], 'accompanying text').defaultPrevented, false);
  assert.equal(composer.paste([zeroFile('html.pdf')], '', '<b>text</b>').defaultPrevented, false);
  assert.equal(composer.count(), 3);
  composer.lock(true);
  assert.equal(composer.paste([zeroFile('locked.pdf')]).defaultPrevented, false);
  assert.equal(composer.count(), 3);
  composer.lock(false);
  composer.paste([new File([new Uint8Array(101)], 'too-large.bin')]);
  assert.equal(composer.count(), 3);
  assert.match(composer.notice().textContent, /limit/);
  await eventually(() => composer.ready());
  const count = composer.calls.create.length;
  composer.destroy();
  composer.paste([zeroFile('after-destroy.pdf')]);
  assert.equal(composer.calls.create.length, count);
});

test('clipboard file-list fallback preserves multiple file objects', async () => {
  const {clipboardFiles} = await modules;
  const files = [zeroFile('one.pdf'), zeroFile('two.zip')];
  assert.deepEqual(clipboardFiles({items: [], files}), files);
  assert.deepEqual(clipboardFiles({items: [{kind:'string'}], files: []}), []);
});

test('repeated and batched clipboard names retain distinct bytes and file metadata', async t => {
  const composer = await uploadComposer(t);
  composer.paste([new File(['first'], 'image.png', {type: 'image/png', lastModified: 123})]);
  composer.paste([new File(['second'], 'image.png'), new File(['third'], 'image.png')]);
  await eventually(() => composer.ready());
  assert.deepEqual(composer.saved().map(file => file.name), ['image-1.png', 'image-2.png', 'image-3.png']);
  assert.deepEqual(composer.saved().map(file => file.sourceName), ['image.png', 'image.png', 'image.png']);
  assert.equal(composer.calls.create[0].type, 'image/png');
  assert.equal(composer.calls.create[0].lastModified, 123);
  for (const [index, file] of composer.saved().entries()) {
    const bytes = Buffer.concat(composer.calls.append.filter(chunk => chunk.id === file.id).map(chunk => chunk.bytes));
    assert.equal(bytes.toString(), ['first', 'second', 'third'][index]);
  }
});

test('clipboard counters survive submission, removal and reload but a new scope starts fresh', async t => {
  const composer = await uploadComposer(t);
  composer.paste([zeroFile('image.png')]);
  await eventually(() => composer.ready());
  composer.clear(); // Sending clears selection, preserving the server file.
  await composer.reload();
  composer.paste([zeroFile('image.png')]);
  await eventually(() => composer.ready());
  assert.equal(composer.saved()[0].name, 'image-2.png');
  composer.click('image-2.png', 'Remove');
  await eventually(() => composer.count() === 0);
  composer.files.clear(); // Retained numbering also survives catalogue expiry.
  await composer.reload();
  composer.paste([zeroFile('image.png')]);
  await eventually(() => composer.ready());
  assert.equal(composer.saved()[0].name, 'image-3.png');
  composer.files.clear();
  await composer.reload('new-session');
  composer.paste([zeroFile('image.png')]);
  await eventually(() => composer.ready());
  assert.equal(composer.saved()[0].name, 'image-1.png');
});

test('catalogue names seed numbering without browser counters and picker/drop names stay unchanged', async t => {
  const composer = await uploadComposer(t, {files: [{id, name: 'image-9.png', size: 0, state: 'ready'}]});
  composer.add(zeroFile('image-11.png'));
  composer.pick(zeroFile('picked.png'));
  await eventually(() => composer.ready());
  composer.paste([zeroFile('image.png'), zeroFile('report.pdf'), zeroFile('report.pdf')]);
  await eventually(() => composer.ready());
  assert.deepEqual(composer.saved().map(file => file.name), ['image-11.png', 'picked.png', 'image-12.png', 'report-1.pdf', 'report-2.pdf']);
  assert.equal(composer.saved()[0].sourceName, undefined);
  assert.equal(composer.saved()[1].sourceName, undefined);
  composer.clear();
  composer.storage.setItem('draft.clipboard-numbers', '[]');
  await composer.reload(); // Creation adoption / another browser only has catalogue names.
  composer.paste([zeroFile('image.png')]);
  await eventually(() => composer.ready());
  assert.equal(composer.saved()[0].name, 'image-13.png');
});

test('numbered recovery accepts the original name without changing identity or skipping prefix checks', async t => {
  const record = {id, clientId: id, name: 'image-4.png', sourceName: 'image.png', size: 8,
    state: 'uploading', offset: 4, checksums: [hash('abcd')], creation: 'created'};
  const composer = await uploadComposer(t, {saved: [record], files: [record]});
  composer.choose('image-4.png', new File(['xxxxefgh'], 'image.png'), 'Choose file to resume');
  await eventually(() => composer.saved()[0]?.error?.includes('differs'));
  assert.equal(composer.calls.append.length, 0);
  await composer.reload();
  composer.choose('image-4.png', new File(['abcdefgh'], 'wrong.png'), 'Choose file to resume');
  await eventually(() => composer.saved()[0]?.error?.includes('same file'));
  await composer.reload();
  composer.choose('image-4.png', new File(['abcdefgh'], 'image.png'), 'Choose file to resume');
  await eventually(() => composer.ready());
  assert.equal(composer.saved()[0].name, 'image-4.png');
  assert.equal(composer.saved()[0].clientId, id);
  assert.deepEqual(composer.ids(), [id]);
  assert.equal(composer.calls.create.length, 0);
  assert.equal(Buffer.concat(composer.calls.append.map(chunk => chunk.bytes)).toString(), 'efgh');
});

test('clipboard numbering handles Unicode, long stems, extensionless names and dotfiles', async t => {
  const composer = await uploadComposer(t);
  composer.paste(['český.png', 'archive.tar.gz', 'README', '.env', 'image-1.png', '😀'.repeat(70) + '.png']
    .map(name => zeroFile(name)));
  await eventually(() => composer.ready());
  const names = composer.saved().map(file => file.name);
  assert.deepEqual(names.slice(0, 5), ['český-1.png', 'archive.tar-1.gz', 'README-1', '.env-1', 'image-1-1.png']);
  assert.equal(names[5], '😀'.repeat(62) + '-1.png');
  assert.ok(Buffer.byteLength(names[5]) <= 255);
});

test('numbering storage failures prevent upload creation and submission', async t => {
  const composer = await uploadComposer(t);
  composer.failWrites(true);
  composer.paste([zeroFile('image.png')]);
  assert.equal(composer.ready(), false);
  assert.equal(composer.calls.create.length, 0);
  assert.match(composer.notice().textContent, /Storage write failed/);
  composer.failWrites(false);
  composer.click('image-1.png', 'Retry');
  await eventually(() => composer.ready());
  await composer.reload();
  composer.paste([zeroFile('image.png')]);
  await eventually(() => composer.ready());
  assert.deepEqual(composer.saved().map(file => file.name), ['image-1.png', 'image-2.png']);
});

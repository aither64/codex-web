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

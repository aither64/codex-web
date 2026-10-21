const assert = require('node:assert/strict');
const {test} = require('node:test');
const fs = require('node:fs');
const vm = require('node:vm');
const ready = import('../conversation/assets/refresh.js');

async function fixture(t) {
  const {createRefreshNotice} = await ready;
  let at = 100_000, sequence = 0, latest;
  const timers = new Map(), page = new EventTarget(), browser = new EventTarget(); page.hidden = false;
  const notice = createRefreshNotice({document: page, window: browser, now: () => at,
    setTimeout: (fn, ms) => {const id = ++sequence; timers.set(id, {fn, at: at + ms}); return id;},
    clearTimeout: id => timers.delete(id), render: state => {latest = state;}});
  t.after(() => notice.destroy());
  return {notice, state: () => latest,
    pageEvent(name) {browser.dispatchEvent(new Event(name));},
    visible(value) {page.hidden = !value; page.dispatchEvent(new Event('visibilitychange'));},
    advance(ms) {
      const end = at + ms;
      for (;;) {
        const next = [...timers].sort((a,b) => a[1].at-b[1].at)[0];
        if (!next || next[1].at > end) break;
        at = next[1].at; timers.delete(next[0]); next[1].fn();
      }
      at = end;
    },
  };
}

test('initial loading is delayed and subsequent background refreshes keep values quiet', async t => {
  const f = await fixture(t); f.notice.begin(); f.advance(749);
  assert.equal(f.state().loading, false); f.advance(1); assert.equal(f.state().loading, true);
  f.notice.success(); f.notice.begin(); f.advance(60_000);
  assert.equal(f.state().loading, false); assert.equal(f.state().hasValue, true);
  f.notice.cancel();
});

test('persistent failures share one visible grace across retries and recover cleanly', async t => {
  const f = await fixture(t); f.notice.success(); f.notice.begin(); f.notice.failure(Error('offline'));
  f.advance(15_000); f.notice.begin(); f.notice.failure(Error('still offline'));
  f.advance(14_999); assert.equal(f.state().warning, false);
  f.advance(1); assert.equal(f.state().warning, true);
  f.notice.success(); assert.equal(f.state().warning, false);
});

test('hidden time does not count and returning to a tab starts a fresh warning grace', async t => {
  const f = await fixture(t); f.notice.failure(Error('offline')); f.advance(10_000);
  f.visible(false); f.advance(120_000); assert.equal(f.state().warning, false);
  f.visible(true); f.advance(29_999); assert.equal(f.state().warning, false);
  f.advance(1); assert.equal(f.state().warning, true);
});

test('manual loads use a short delay and manual/access failures appear immediately', async t => {
  const f = await fixture(t); f.notice.success(); f.notice.begin({manual:true});
  f.advance(249); assert.equal(f.state().loading, false); f.advance(1); assert.equal(f.state().loading, true);
  f.notice.failure(Error('manual failure')); assert.equal(f.state().warning, true);
  f.notice.success(); f.notice.begin(); f.notice.failure(Object.assign(Error('denied'), {status:403}));
  assert.equal(f.state().warning, true);
});

test('page restoration starts a fresh grace and destroyed notices stop publishing', async t => {
  const f = await fixture(t); f.notice.failure(Error('offline')); f.advance(20_000);
  f.pageEvent('pagehide'); f.advance(120_000); assert.equal(f.state().warning, false);
  f.pageEvent('pageshow'); f.advance(29_999); assert.equal(f.state().warning, false);
  f.advance(1); assert.equal(f.state().warning, true);
  const previous = f.state(); f.notice.destroy(); f.pageEvent('pageshow'); f.advance(60_000);
  assert.equal(f.state(), previous);
});

test('a retired mounted pending failure cannot retire successor success freshness', async () => {
  const source = fs.readFileSync(require.resolve('../conversation/assets/conversation.js'), 'utf8');
  const start = source.indexOf('  const schedulePending = () => {');
  const end = source.indexOf('  const scheduleQueue = () => {', start);
  assert(start >= 0 && end > start);
  let at = 100_000, failures = 0;
  const calls = [], timers = new Map();
  const context = {
    document:{hidden:false}, Date:{now:()=>at},
    setTimeout:(fn,ms)=>{const id = {}; timers.set(id,{fn,at:at+ms}); return id;},
    clearTimeout:id=>timers.delete(id),
    client:{pending:()=>new Promise((resolve,reject)=>calls.push({resolve,reject}))},
    pendingNotice:{begin(){},success(){},failure(){failures++;}},
    renderPrompts(){}, pendingStatusLabel:{},
  };
  // The real lane abort race normally settles catch before a resumed success.
  // Hold that settlement here to exercise the shipped catch's ownership guard.
  vm.runInNewContext(`
    const capabilities = {pending:true}, policy = {pendingMs:15000,pendingDeadlineMs:10000,retryInitialMs:2000,retryMaxMs:30000};
    const sync = {pendingInvalidationSupported:true}, currentThread = {threadId:"thread"};
    let destroyed = false, paused = false, pendingRead = null, pendingScope = null;
    let pendingRetryTimer = null, pendingDueTimer = null, pendingRerun = false;
    let pendingGeneration = 0, pendingLastRead = -Infinity, pendingRetryDelay = 2000;
    const laneRead = () => ({signal:{aborted:false},run:operation=>Promise.resolve().then(operation),finish(){}});
    ${source.slice(start,end)}
    globalThis.controls = {refreshPending,
      retire(){++pendingGeneration; pendingScope = null; pendingRead = null;},
      lastRead:()=>pendingLastRead};
  `, context);
  const settle = () => new Promise(resolve=>setImmediate(resolve));
  const initial = context.controls.refreshPending(); await settle();
  calls[0].resolve([]); await initial;
  assert.equal(context.controls.lastRead(), 100_000);
  at = 100_100;
  const retired = context.controls.refreshPending(true); await settle();
  context.controls.retire();
  at = 100_200;
  const successor = context.controls.refreshPending(true); await settle();
  calls[2].resolve([]); await successor;
  calls[1].reject(Object.assign(Error('retired unavailable'),{status:503})); await retired;
  assert.equal(context.controls.lastRead(), 100_200, 'retired catch must preserve successor successful freshness');
  assert.equal(failures, 0, 'retired catch cannot publish a failure notice');
  at = 102_100;
  void context.controls.refreshPending(); await settle();
  assert.equal(calls.length, 3, 'ordinary output still respects successor successful freshness');
  assert.deepEqual([...timers.values()].map(timer=>timer.at), [115_200]);
});

const assert = require('node:assert/strict');
const {test} = require('node:test');
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

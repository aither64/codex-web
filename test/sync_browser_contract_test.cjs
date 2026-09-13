const assert = require("node:assert/strict");
const {test} = require("node:test");
const moduleReady = import("../conversation/assets/sync.js");
const settle = () => new Promise(resolve => setImmediate(resolve));

class Clock {
  time = 100_000;
  next = 0;
  timers = new Map();
  now = () => this.time;
  setTimeout = (fn, delay) => { const id = ++this.next; this.timers.set(id, {fn, at: this.time + delay}); return id; };
  clearTimeout = id => this.timers.delete(id);
  async advance(ms) {
    const end = this.time + ms;
    for (;;) {
      const entry = [...this.timers].sort((a, b) => a[1].at - b[1].at)[0];
      if (!entry || entry[1].at > end) break;
      this.time = Math.max(this.time, entry[1].at);
      this.timers.delete(entry[0]); entry[1].fn(); await settle();
    }
    this.time = end; await settle();
  }
}
class Browser extends EventTarget { hidden = false; navigator = {onLine: true}; }
class Source {
  listeners = new Map();
  closed = false;
  constructor(path) { this.path = path; }
  addEventListener(name, fn) { this.listeners.set(name, fn); }
  close() { this.closed = true; }
  event(name, data = "") { (this["on" + name] || this.listeners.get(name))?.({data}); }
}
async function fixture(t, options = {}) {
  const {createConversationSync} = await moduleReady;
  const clock = new Clock(), browser = new Browser(), sources = [], states = [], applied = [], reads = [];
  let value = "initial";
  const sync = createConversationSync({
    window: browser, document: browser, now: clock.now,
    setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout, random: () => 1,
    eventsPath: "/events",
    EventSource: class extends Source { constructor(path) { super(path); sources.push(this); } },
    read: signal => { reads.push(signal); return options.read ? options.read(signal, reads.length) : value; },
    apply: async (result, context) => { if (options.apply) await options.apply(result, context); else applied.push(result); },
    onStateChange: state => states.push(state),
    ...options.controller,
  });
  t.after(() => sync.destroy());
  const open = (heartbeat = true) => {
    sources.at(-1).event("open");
    if (heartbeat) sources.at(-1).event("ready", '{"heartbeatIntervalMs":20000}');
  };
  const emit = name => browser.dispatchEvent(new Event(name));
  return {clock, browser, sources, states, applied, reads, sync, open, emit,
    value: next => { value = next; }, state: () => states.at(-1)};
}

test("a stalled read expires, frees the gate, and cannot overwrite recovery", async t => {
  let oldResolve;
  const f = await fixture(t, {read: (_, count) => count === 1 ? new Promise(resolve => { oldResolve = resolve; }) : "latest reply"});
  f.open(); await f.clock.advance(0);
  f.sources[0].event("message");
  await f.clock.advance(35_000);
  assert.equal(f.reads[0].aborted, true);
  assert.equal(f.state().status, "reconnecting");
  assert.match(f.state().error, /timed out/);
  await f.clock.advance(1000);
  assert.deepEqual(f.applied, ["latest reply"]);
  oldResolve("stale reply"); await settle();
  assert.deepEqual(f.applied, ["latest reply"]);
  assert.equal(f.state().status, "connected");
});

test("a failed read retries even when the stream produces no more events", async t => {
  const f = await fixture(t, {read: (_, count) => { if (count === 1) throw new Error("network failed"); return "recovered"; }});
  f.open(); await f.clock.advance(0);
  assert.equal(f.state().status, "reconnecting");
  await f.clock.advance(1000);
  assert.deepEqual(f.applied, ["recovered"]);
  assert.equal(f.state().status, "connected");
});

test("heartbeats detect a silent stream and reconnection retrieves missed output", async t => {
  const f = await fixture(t); f.open(); await f.clock.advance(0);
  f.value("reply during outage");
  await f.clock.advance(50_000);
  assert.equal(f.sources[0].closed, true);
  assert.equal(f.state().status, "reconnecting");
  await f.clock.advance(1000);
  assert.equal(f.sources.length, 2);
  f.open(); await f.clock.advance(0);
  assert.equal(f.applied.at(-1), "reply during outage");
  assert.equal(f.state().status, "connected");
});

test("native permanent closure retries and a fresh read cannot hide the stream failure", async t => {
  const f = await fixture(t); f.open(); await f.clock.advance(0);
  f.sources[0].event("error"); await f.clock.advance(0);
  assert.equal(f.sources[0].closed, true);
  assert.equal(f.state().status, "reconnecting");
  await f.clock.advance(1000);
  f.open(); await f.clock.advance(0);
  assert.equal(f.state().status, "connected");
});

test("stream opening cannot clear an HTTP failure before a new snapshot succeeds", async t => {
  let denied = true;
  const f = await fixture(t, {read: () => { if (denied) throw Object.assign(new Error("Access denied"), {status: 401}); return "current"; }});
  f.open(); await f.clock.advance(0);
  f.sources[0].event("error"); await f.clock.advance(1000);
  f.open();
  assert.equal(f.state().httpStatus, 401);
  denied = false; await f.clock.advance(0);
  assert.equal(f.state().status, "connected");
});

test("wake signals coalesce, abort a pre-sleep read, and keep later responses out", async t => {
  let oldResolve;
  const f = await fixture(t, {read: (_, n) => n === 1 ? new Promise(resolve => { oldResolve = resolve; }) : "awake"});
  f.open(); await f.clock.advance(0);
  f.browser.hidden = true; f.emit("visibilitychange");
  f.clock.time += 120_000;
  f.browser.hidden = false;
  for (const event of ["visibilitychange", "focus", "online"]) f.emit(event);
  assert.equal(f.sources.length, 2);
  assert.equal(f.reads[0].aborted, true);
  f.open(); await f.clock.advance(0);
  oldResolve("asleep"); await settle();
  assert.deepEqual(f.applied, ["awake"]);
});

test("a timer gap recovers sleep even without a browser lifecycle event", async t => {
  const f = await fixture(t); f.open(); await f.clock.advance(0);
  f.clock.time += 120_000; await f.clock.advance(0);
  assert.equal(f.sources.length, 2);
  assert.equal(f.sources[0].closed, true);
});

test("legacy servers use snapshot polling without endless idle reconnections", async t => {
  const f = await fixture(t); f.open(false); await f.clock.advance(0);
  f.value("missed legacy update"); await f.clock.advance(60_000);
  assert.equal(f.sources.length, 1);
  assert.equal(f.applied.at(-1), "missed legacy update");
  assert.equal(f.state().status, "connected");
});

test("hidden pages pause reads and bfcache restores a new stream", async t => {
  const f = await fixture(t); f.open(); await f.clock.advance(0);
  const count = f.reads.length;
  f.browser.hidden = true; f.emit("visibilitychange");
  await f.clock.advance(120_000); assert.equal(f.reads.length, count);
  f.emit("pagehide"); assert.equal(f.clock.timers.size, 0);
  f.browser.hidden = false; f.emit("pageshow"); f.open(); await f.clock.advance(0);
  assert.equal(f.sources.length, 2);
  assert.equal(f.state().status, "connected");
  f.sync.destroy();
  assert.equal(f.clock.timers.size, 0);
  f.emit("focus"); f.emit("pageshow"); await f.clock.advance(1000);
  assert.equal(f.sources.length, 2);
  assert.equal(f.clock.timers.size, 0);
});

test("partial asynchronous apply cannot publish healthy after supersession", async t => {
  let release;
  const applied = [];
  const f = await fixture(t, {apply: async (result, {isCurrent}) => {
    if (result === "initial") await new Promise(resolve => { release = resolve; });
    if (isCurrent()) applied.push(result);
  }});
  f.open(); await f.clock.advance(0);
  f.value("new"); f.emit("online"); f.open(); await f.clock.advance(0);
  release(); await settle();
  assert.deepEqual(applied, ["new"]);
});

test("connection messages distinguish access errors and connectivity", async () => {
  const {connectionMessage} = await moduleReady;
  assert.equal(connectionMessage({status: "connected"}), "");
  assert.match(connectionMessage({status: "reconnecting", httpStatus: 403}), /Check your access/);
  assert.match(connectionMessage({status: "offline"}), /Network/);
  assert.match(connectionMessage({status: "reconnecting", error: "Service unavailable"}), /Service unavailable/);
});

test("a stream event burst cannot bypass failed-read backoff", async t => {
  const f = await fixture(t, {read: () => { throw new Error("unavailable"); }});
  f.open(); await f.clock.advance(0);
  for (let i = 0; i < 10; i++) {
    f.sources[0].event("message"); await f.clock.advance(50);
  }
  assert.equal(f.reads.length, 1);
  await f.clock.advance(500);
  assert.equal(f.reads.length, 2);
  await f.clock.advance(1999);
  assert.equal(f.reads.length, 2);
  await f.clock.advance(1);
  assert.equal(f.reads.length, 3);
});

test("HTTP reads forward cancellation through response body parsing", async () => {
  const {createConversationClient} = await import("../conversation/assets/conversation.js");
  const abort = new AbortController();
  const client = createConversationClient({id: "example", fetch: async (_, {signal}) => ({
    ok: true,
    json: () => new Promise((_, reject) => signal.addEventListener("abort", () => reject(signal.reason), {once: true})),
  })});
  const read = client.thread({signal: abort.signal});
  await settle(); abort.abort(new Error("superseded"));
  await assert.rejects(read, /superseded/);
});

test("read deadlines abort hung fetches without changing mutation deadlines", async t => {
  const {createConversationClient} = await import("../conversation/assets/conversation.js");
  t.mock.timers.enable({apis: ["setTimeout"]});
  const client = createConversationClient({id: "example", fetch: (_, {signal}) => new Promise((_, reject) => {
    signal.addEventListener("abort", () => reject(signal.reason), {once: true});
  })});
  const read = client.thread();
  const rejected = assert.rejects(read, /timed out/);
  t.mock.timers.tick(35_000);
  await rejected;
  let mutationSignal;
  const sender = createConversationClient({id: "example", fetch: async (_, init) => {
    mutationSignal = init.signal;
    return {ok: true, json: async () => ({accepted: true})};
  }});
  await sender.message("explicit send", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa");
  assert.equal(mutationSignal, undefined);
});


test("an early failed snapshot cancels its hung reconciliation sibling", async t => {
  let siblingAborts = 0;
  const f = await fixture(t, {read: signal => Promise.all([
    Promise.reject(new Error("thread unavailable")),
    new Promise((_, reject) => signal.addEventListener("abort", () => {
      siblingAborts++; reject(signal.reason);
    }, {once: true})),
  ])});
  f.open(); await f.clock.advance(0);
  assert.equal(siblingAborts, 1);
  assert.equal(f.reads[0].aborted, true);
  await f.clock.advance(1000);
  assert.equal(siblingAborts, 2);
});

test("the advertised heartbeat interval determines the stream deadline", async t => {
  const f = await fixture(t); f.open(false);
  f.sources[0].event("ready", '{"heartbeatIntervalMs":40000}');
  await f.clock.advance(50_000);
  assert.equal(f.sources[0].closed, false);
  f.sources[0].event("heartbeat");
  await f.clock.advance(85_000);
  assert.equal(f.sources[0].closed, false);
  await f.clock.advance(5000);
  assert.equal(f.sources[0].closed, true);
});


test("brief focus refresh keeps the healthy stream and stays quiet", async t => {
  const f = await fixture(t); f.open(); await f.clock.advance(0);
  f.browser.hidden = true; f.emit("visibilitychange"); await f.clock.advance(1000);
  f.browser.hidden = false; f.emit("visibilitychange"); f.emit("focus");
  assert.equal(f.sources.length, 1);
  assert.equal(f.state().showWarning, false);
  await f.clock.advance(0);
  assert.equal(f.state().status, "connected");
});

test("recovery gets ten visible seconds regardless of hidden time or repeated focus", async t => {
  const f = await fixture(t, {read: (_, n) => n === 1 ? "initial" : new Promise(() => {})});
  f.open(); await f.clock.advance(0);
  f.browser.hidden = true; f.emit("visibilitychange"); await f.clock.advance(120_000);
  f.browser.hidden = false; f.emit("visibilitychange"); f.open(); await f.clock.advance(0);
  assert.equal(f.state().showWarning, false);
  await f.clock.advance(5000); f.emit("focus");
  await f.clock.advance(4999); assert.equal(f.state().showWarning, false);
  await f.clock.advance(1); assert.equal(f.state().showWarning, true);
});

test("access errors bypass the recovery grace", async t => {
  const f = await fixture(t, {read: () => { throw Object.assign(new Error("denied"), {status: 403}); }});
  f.open(); await f.clock.advance(0);
  assert.equal(f.state().showWarning, true);
  const {connectionMessage} = await moduleReady;
  assert.match(connectionMessage(f.state()), /access denied/i);
});

const assert = require("node:assert/strict");
const test = require("node:test");
const {pathToFileURL} = require("node:url");
const path = require("node:path");

test("transient non-delivery survives reload and reuses the saved browser identity", async () => {
  const {createDurableSender} = await import(pathToFileURL(path.resolve(__dirname, "../conversation/assets/conversation.js")));
  const values = new Map();
  const storage = {get length() {return values.size;}, key: i => [...values.keys()][i] || null,
    getItem: key => values.get(key) ?? null, setItem: (key,value) => values.set(key,value), removeItem: key => values.delete(key)};
  const failure = Object.assign(new Error("not sent"), {code: "message_not_sent", notSent: true, retryable: true});
  let original;
  const sender = createDurableSender({storage, id: "thread", basePath: "/codex/conversations", durableNamespace: "delivery", randomUUID: () => "00000000-0000-4000-8000-000000000001",
    client: {message: async (text,id) => {original=id;throw failure;}}});
  await assert.rejects(sender.send("saved message"), error => error === failure);
  assert.equal(sender.pending().retryable, true);
  const restored = createDurableSender({storage, id: "thread", basePath: "/codex/conversations", durableNamespace: "delivery",
    client: {message: async (text,id,retry) => {assert.equal(id,original);assert.equal(text,"saved message");assert.equal(retry,true);return {turnId:"turn",clientUserMessageId:id};}}});
  assert.equal(restored.pending().retryable, true);
  await restored.send("saved message");
  assert.equal(restored.pending().retryable, undefined);
});

test("delivery retries retain identities, back off, and stop on uncertainty", async () => {
  const {createDeliveryRetryScheduler} = await import(pathToFileURL(path.resolve(__dirname, "../conversation/assets/delivery_retry.js")));
  let next = 0; const timers = new Map(), calls = [], delays = [];
  const scheduler = createDeliveryRetryScheduler({
    retry: async id => { calls.push(id); },
    setTimer: (callback, delay) => { delays.push(delay); timers.set(++next, callback); return next; },
    clearTimer: id => timers.delete(id),
  });
  const transient = {code: "message_not_sent", notSent: true, retryable: true};
  assert.equal(scheduler.schedule("original", transient), true);
  scheduler.schedule("original", transient);
  assert.equal(timers.size, 1);
  for (let i = 0; i < 8; i++) {
    const [id, callback] = timers.entries().next().value; timers.delete(id); await callback();
    scheduler.schedule("original", transient);
  }
  assert.deepEqual(new Set(calls), new Set(["original"]));
  assert.deepEqual(delays.slice(0, 6), [250, 500, 1000, 2000, 4000, 5000]);
  scheduler.schedule("original", {code: "message_delivery_unknown", notSent: false});
  assert.equal(timers.size, 0);
  assert.equal(scheduler.schedule("refused", {...transient, retryable: false}), false);
  scheduler.destroy();
});

test("delivery retry waits while unavailable and ignores retired attempts", async () => {
  const {createDeliveryRetryScheduler} = await import(pathToFileURL(path.resolve(__dirname, "../conversation/assets/delivery_retry.js")));
  let callback, available = false, current = true, calls = 0;
  const scheduler = createDeliveryRetryScheduler({
    retry: async () => { calls++; }, canRetry: () => available, isCurrent: () => current,
    setTimer: fn => { callback = fn; return 1; }, clearTimer: () => {},
  });
  scheduler.schedule("id", {code: "message_not_sent", notSent: true, retryable: true});
  await callback(); assert.equal(calls, 0);
  available = true; current = false;
  await callback(); assert.equal(calls, 0);
  scheduler.destroy();
});

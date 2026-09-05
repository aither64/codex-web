"use strict";

const assert = require("node:assert/strict");
const {createRequest, createSessionClient, preferredReasoningEffort} = require("./static/app.js");

const baseURL = process.argv[2];
if (!baseURL) throw new Error("browser contract test requires the server URL");
const origin = new URL(baseURL).origin;
const fetchRequest = (path, options = {}) => fetch(new URL(path, baseURL), {
  ...options,
  headers: {Origin: origin, ...(options.headers || {})},
});
const client = createSessionClient("example", createRequest(fetchRequest));

assert.equal(preferredReasoningEffort({
  defaultReasoningEffort: "medium",
  supportedReasoningEfforts: [{reasoningEffort: "medium"}, {reasoningEffort: "max"}],
}, true), "max");
assert.equal(preferredReasoningEffort({
  defaultReasoningEffort: "high",
  supportedReasoningEfforts: [{reasoningEffort: "high"}],
}, true), "high");

(async () => {
  const thread = await client.thread();
  assert.equal(thread.threadId, "thread-1");
  assert.equal(thread.status, "idle");
  assert.equal(thread.entries[0].text, "contract response");

  const pending = await client.pending();
  assert.equal(pending[0].id, "approval-1");
  assert.equal(pending[0].kind, "command");

  const legacyEmptyClient = createSessionClient("example", async (path) => {
    assert.match(path, /\/pending$/);
    return null;
  });
  assert.deepEqual(await legacyEmptyClient.pending(), []);

  assert.deepEqual(await client.message("browser message"), {ok: true});
  assert.deepEqual(await client.interrupt(), {ok: true});
  assert.deepEqual(await client.respond("approval-1", {decision: "accept"}), {ok: true});
  assert.deepEqual(
    await client.respond("question-1", {answers: {choice: {answers: ["yes"]}}}),
    {ok: true},
  );

  const events = await fetchRequest(client.eventsPath());
  assert.equal(events.status, 200);
  assert.match(events.headers.get("content-type"), /^text\/event-stream/);
  assert.match(await events.text(), /: connected/);
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

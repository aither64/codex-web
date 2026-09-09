"use strict";

const assert = require("node:assert/strict");
const {
  automaticReasoningLabel, autoResolutionLabel, beforeRequestInputAction, createRequest, createSessionClient,
  captureTranscriptDisclosureState, clearThreadStorage, deleteQueueAttempt, deleteRequestInputDraft,
  deleteSendAttempt, loadQueueAttempts,
  loadRequestInputDraft, loadSendAttempts, matchingSendAttempt, messageActionLabel, queueAttemptStorageKey,
  queueAttemptStoragePrefix, requestInputDraftStorageKey, requireQueueAttempts,
  sendAttemptStorageKey, shouldFollowTranscript, shouldSubmitMessage, storeQueueAttempt,
  storeRequestInputDraft, storeSendAttempt, transcriptEntriesForFilter, transcriptEntryKey,
  transcriptEntryVisible, wrapMarkdownTables, encodeQuestionAnswer, fileChangeDiffs, formatElapsed,
  activityAge, indexMembershipChanged, indexStatusFreshForPage, indexStatusOrder,
  lifecyclePresentation, sessionTabFromHash,
} = require("./static/app.js");

const baseURL = process.argv[2];
if (!baseURL) throw new Error("browser contract test requires the server URL");
const origin = new URL(baseURL).origin;
const fetchRequest = (path, options = {}) => fetch(new URL(path, baseURL), {
  ...options,
  headers: {Origin: origin, ...(options.headers || {})},
});
const client = createSessionClient("example", createRequest(fetchRequest));

assert.equal(automaticReasoningLabel(), "Automatic");
assert.equal(automaticReasoningLabel({model: "bounded"}), "Automatic");
assert.equal(messageActionLabel(false), "Send");
assert.equal(messageActionLabel(true), "Steer now");
assert.equal(shouldSubmitMessage({key: "Enter", shiftKey: false, isComposing: false}), true);
assert.equal(shouldSubmitMessage({key: "Enter", shiftKey: true, isComposing: false}), false);
assert.equal(shouldSubmitMessage({key: "Enter", shiftKey: false, isComposing: true}), false);
assert.equal(shouldSubmitMessage({key: "a", shiftKey: false, isComposing: false}), false);
assert.equal(shouldFollowTranscript({scrollHeight: 1000, clientHeight: 400, scrollTop: 580}), true);
assert.equal(shouldFollowTranscript({scrollHeight: 1000, clientHeight: 400, scrollTop: 300}), false);
assert.deepEqual(indexStatusOrder([
  {slug: "older", updatedAt: "2026-09-08T10:00:00Z"},
  {slug: "newer-b", updatedAt: "2026-09-09T10:00:00Z"},
  {slug: "newer-a", updatedAt: "2026-09-09T10:00:00Z"},
]).map((entry) => entry.slug), ["newer-a", "newer-b", "older"]);
assert.equal(indexStatusFreshForPage("2026-09-09T10:00:01Z", "2026-09-09T10:00:00Z"), false);
assert.equal(indexStatusFreshForPage("2026-09-09T10:00:00Z", "2026-09-09T10:00:01Z"), true);
assert.equal(indexMembershipChanged([], [], true), false);
assert.equal(indexMembershipChanged([], [{slug: "new", archived: false}], true), true);
assert.equal(indexMembershipChanged([{slug: "deleted", archived: false}], [], true), true);
assert.equal(indexMembershipChanged([{slug: "retained", archived: false}], [], false), false);
assert.equal(indexMembershipChanged(
  [{slug: "one", archived: false}, {slug: "two", archived: true}],
  [{slug: "two", archived: true}, {slug: "one", archived: false}], true,
), false);
assert.equal(indexMembershipChanged(
  [{slug: "moved", archived: false}], [{slug: "moved", archived: true}], true,
), true);
assert.equal(indexMembershipChanged(
  [{slug: "moved", archived: true}], [{slug: "moved", archived: false}], true,
), true);
assert.equal(activityAge("2026-09-09T09:59:30Z", Date.parse("2026-09-09T10:00:00Z")), "just now");
assert.equal(activityAge("2026-09-09T09:55:00Z", Date.parse("2026-09-09T10:00:00Z")), "5m ago");
assert.equal(sessionTabFromHash("#repositories", ["codex", "repositories"], "codex"), "repositories");
assert.equal(sessionTabFromHash("#nested-tab", ["codex", "repositories"], "codex"), "codex");
assert.equal(sessionTabFromHash("#%E0%A4%A", ["codex"], "codex"), "codex");
assert.equal(transcriptEntryKey({turnId: "turn-1", itemId: "item-1"}, 7), '["turn-1","item-1"]');
const fallbackEntry = {turnId: "turn-1", kind: "error"};
assert.equal(
  transcriptEntryKey(fallbackEntry, 1, [{turnId: "old-turn", kind: "plan"}, fallbackEntry]),
  transcriptEntryKey(fallbackEntry, 0, [fallbackEntry]),
);
assert.notEqual(
  transcriptEntryKey(fallbackEntry, 0, [fallbackEntry, fallbackEntry]),
  transcriptEntryKey(fallbackEntry, 1, [fallbackEntry, fallbackEntry]),
);
const disclosureStates = captureTranscriptDisclosureState({
  querySelectorAll: () => [
    {
      dataset: {transcriptEntryKey: '["turn-1","item-1"]'},
      querySelector: () => ({open: true}),
    },
    {
      dataset: {transcriptEntryKey: '["turn-1","item-2"]'},
      querySelector: () => ({open: false}),
    },
  ],
});
assert.deepEqual(Array.from(disclosureStates), [
  ['["turn-1","item-1"]', true],
  ['["turn-1","item-2"]', false],
]);
let tableWrapperCount = 0;
const table = {
  parentElement: {classList: {contains: () => false}},
  before(wrapper) { this.insertedBefore = wrapper; },
};
table.ownerDocument = {
  createElement: () => {
    tableWrapperCount += 1;
    return {
      className: "",
      classList: {contains: (name) => name === "table-scroll"},
      append(child) { child.parentElement = this; },
    };
  },
};
const markdownContainer = {querySelectorAll: () => [table]};
wrapMarkdownTables(markdownContainer);
wrapMarkdownTables(markdownContainer);
assert.equal(tableWrapperCount, 1);
assert.equal(table.insertedBefore.className, "table-scroll");
assert.deepEqual(encodeQuestionAnswer({}, {kind: "option", choice: "First", note: "because"}), ["First", "user_note: because"]);
assert.deepEqual(encodeQuestionAnswer({}, {kind: "other", note: "custom"}), ["user_note: custom"]);
assert.deepEqual(encodeQuestionAnswer({}, {kind: "other", note: ""}), []);
assert.deepEqual(encodeQuestionAnswer({}, {kind: "freeform", note: "answer"}), ["user_note: answer"]);
assert.deepEqual(encodeQuestionAnswer({}, {}), []);
assert.equal(autoResolutionLabel(1000, 2000, 62000, false), "");
assert.equal(autoResolutionLabel(2000, 2000, 62000, false), "Auto-resolves unanswered in 60s.");
assert.equal(autoResolutionLabel(2000, 2000, 62000, true), "Auto-resolution paused while you answer.");

const stored = new Map();
const storage = {
  get length() { return stored.size; },
  getItem: (key) => stored.has(key) ? stored.get(key) : null,
  key: (index) => Array.from(stored.keys())[index] || null,
  removeItem: (key) => stored.delete(key),
  setItem: (key, value) => stored.set(key, value),
};
const firstAttempt = {
  message: "retry after reload", id: "00000000-0000-4000-8000-000000000002",
};
const secondAttempt = {
  message: "another tab", id: "00000000-0000-4000-8000-000000000003",
};
const firstTabSnapshot = loadQueueAttempts(storage, "example", "thread-1");
const secondTabSnapshot = loadQueueAttempts(storage, "example", "thread-1");
assert.deepEqual(firstTabSnapshot, []);
assert.deepEqual(secondTabSnapshot, []);
assert.equal(storeQueueAttempt(storage, "example", "thread-1", firstAttempt), true);
assert.equal(storeQueueAttempt(storage, "example", "thread-1", secondAttempt), true);
assert.deepEqual(loadQueueAttempts(storage, "example", "thread-1"), [firstAttempt, secondAttempt]);
assert.equal(deleteQueueAttempt(storage, "example", "thread-1", firstAttempt.id), true);
assert.deepEqual(loadQueueAttempts(storage, "example", "thread-1"), [secondAttempt]);
assert.deepEqual(loadQueueAttempts(storage, "example", "thread-2"), []);
assert.equal(
  queueAttemptStorageKey("example", "thread-1", firstAttempt.id),
  `workspace-portal.queue-attempt.example.thread-1.${firstAttempt.id}`,
);
stored.set(`${queueAttemptStoragePrefix("broken", "thread-1")}bad-id`, "not json");
assert.equal(loadQueueAttempts(storage, "broken", "thread-1"), null);
assert.throws(
  () => requireQueueAttempts(storage, "broken", "thread-1"),
  /messages cannot be submitted safely/,
);
assert.throws(
  () => requireQueueAttempts(null, "example", "thread-1"),
  /messages cannot be submitted safely/,
);
assert.deepEqual(requireQueueAttempts(storage, "example", "thread-1"), [secondAttempt]);

const sendAttempt = {
  message: "steer after reload", id: "00000000-0000-4000-8000-000000000005",
  steered: true, context: "",
};
assert.equal(storeSendAttempt(storage, "example", "thread-1", sendAttempt), true);
assert.deepEqual(loadSendAttempts(storage, "example", "thread-1"), [sendAttempt]);
assert.equal(
  sendAttemptStorageKey("example", "thread-1", sendAttempt.id),
  `workspace-portal.send-attempt.example.thread-1.${sendAttempt.id}`,
);
assert.equal(deleteSendAttempt(storage, "example", "thread-1", sendAttempt.id), true);
assert.deepEqual(loadSendAttempts(storage, "example", "thread-1"), []);
const planAAttempt = {
  message: "Implement the plan.", id: "00000000-0000-4000-8000-000000000006",
  steered: false, context: "plan:aaaaaaaa",
};
const planBAttempt = {
  message: "Implement the plan.", id: "00000000-0000-4000-8000-000000000007",
  steered: false, context: "plan:bbbbbbbb",
};
assert.equal(storeSendAttempt(storage, "example", "thread-1", planAAttempt), true);
assert.equal(storeSendAttempt(storage, "example", "thread-1", planBAttempt), true);
assert.deepEqual(loadSendAttempts(storage, "example", "thread-1"), [planAAttempt, planBAttempt]);
assert.equal(
  matchingSendAttempt(
    loadSendAttempts(storage, "example", "thread-1"),
    "Implement the plan.",
    "plan:aaaaaaaa",
  ).id,
  planAAttempt.id,
);
assert.equal(
  matchingSendAttempt(
    loadSendAttempts(storage, "example", "thread-1"),
    "Implement the plan.",
    "plan:bbbbbbbb",
  ).id,
  planBAttempt.id,
);
assert.equal(deleteSendAttempt(storage, "example", "thread-1", planAAttempt.id), true);
assert.equal(deleteSendAttempt(storage, "example", "thread-1", planBAttempt.id), true);

const inputQuestions = [
  {id: "normal", isSecret: false},
  {id: "secret", isSecret: true},
];
assert.equal(storeRequestInputDraft(storage, "example", "thread-1", "request-1", inputQuestions, {
  page: 1,
  drafts: [
    {kind: "option", choice: "First", note: "remember this"},
    {kind: "freeform", choice: "", note: "never persist this"},
  ],
}), true);
assert.deepEqual(
  loadRequestInputDraft(storage, "example", "thread-1", "request-1", inputQuestions),
  {
    page: 1,
    drafts: [
      {kind: "option", choice: "First", note: "remember this"},
      {},
    ],
  },
);
assert.match(
  requestInputDraftStorageKey("example", "thread-1", "request-1"),
  /^workspace-portal\.request-input\./,
);
assert.equal(deleteRequestInputDraft(storage, "example", "thread-1", "request-1"), true);

assert.equal(storeQueueAttempt(storage, "example", "thread-2", firstAttempt), true);
assert.equal(storeSendAttempt(storage, "example", "thread-1", sendAttempt), true);
assert.equal(storeRequestInputDraft(
  storage, "example", "thread-1", "request-1", inputQuestions,
  {page: 0, drafts: [{kind: "freeform", note: "remove"}, {}]},
), true);
assert.equal(clearThreadStorage(storage, "example", "thread-1"), true);
assert.deepEqual(loadQueueAttempts(storage, "example", "thread-1"), []);
assert.deepEqual(loadSendAttempts(storage, "example", "thread-1"), []);
assert.equal(loadRequestInputDraft(storage, "example", "thread-1", "request-1", inputQuestions), null);
assert.deepEqual(loadQueueAttempts(storage, "example", "thread-2"), [firstAttempt]);

const automaticRequests = [];
const automaticClient = createSessionClient("example", async (path, options = {}) => {
  automaticRequests.push({path, body: options.body ? JSON.parse(options.body) : null});
  return {ok: true};
});

(async () => {
  let snoozedBeforeWizardAction = false;
  await beforeRequestInputAction(async () => { snoozedBeforeWizardAction = true; });
  assert.equal(snoozedBeforeWizardAction, true);

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

  const modes = await client.modes();
  assert.deepEqual(modes.map((mode) => mode.mode), ["default", "plan"]);

  const queue = await client.queue();
  assert.equal(queue[0].id, "queued-1");
  assert.equal(queue[0].text, "queued item");

  await automaticClient.settings("model-1", "");
  await automaticClient.fork("forked", "2026-09-05", "model-1", "");
  await automaticClient.archive("complete");
  await automaticClient.revive(true);
  await automaticClient.artifactPreview("notes/example.md");
  assert.deepEqual(automaticRequests, [
    {
      path: "/api/sessions/example/settings",
      body: {model: "model-1", reasoningEffort: ""},
    },
    {
      path: "/api/sessions/example/fork",
      body: {name: "forked", creationDate: "2026-09-05", model: "model-1", reasoningEffort: ""},
    },
    {
      path: "/api/sessions/example/archive",
      body: {mode: "complete"},
    },
    {
      path: "/api/sessions/example/revive",
      body: {allowAbandoned: true},
    },
    {
      path: "/api/sessions/example/artifact-preview?path=notes%2Fexample.md",
      body: null,
    },
  ]);

  assert.deepEqual(
    await client.message("browser message", "00000000-0000-4000-8000-000000000004"),
    {
      turnId: "turn-1",
      clientUserMessageId: "00000000-0000-4000-8000-000000000004",
      steered: true,
    },
  );
  assert.equal((await client.queueMessage(
    "queue message", "00000000-0000-4000-8000-000000000001",
  )).id, "queued-2");
  assert.deepEqual(await client.deleteQueued("queued-1"), {ok: true});
  assert.deepEqual(await client.startQueue("queued-1"), {ok: true});
  const unsupportedAutomatic = await fetchRequest("/api/sessions/example/settings", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({model: "model-1", reasoningEffort: ""}),
  });
  assert.equal(unsupportedAutomatic.status, 400);
  assert.equal((await client.settings(undefined, undefined, "plan")).collaborationMode, "plan");
  assert.deepEqual(await client.interrupt(), {ok: true});
  assert.deepEqual(await client.respond("approval-1", {decision: "accept"}), {ok: true});
  assert.deepEqual(await client.snooze("question-1"), {ok: true});
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

"use strict";

const assert = require("node:assert/strict");
const {
  automaticReasoningLabel, autoResolutionLabel, beforeRequestInputAction, createRequest, createSessionClient,
  captureTranscriptDisclosureState, captureTranscriptViewState, clearThreadStorage,
  deleteQueueAttempt, deleteRequestInputDraft,
  deleteSendAttempt, loadQueueAttempts,
  loadRequestInputDraft, loadSendAttempts, markTranscriptMessagesObserved,
  matchingSendAttempt, messageActionLabel, queueAttemptStorageKey,
  queueAttemptStoragePrefix, requestInputDraftStorageKey, requireQueueAttempts,
  sendAcknowledgementCandidates, sendAttemptStorageKey, shouldFollowTranscript,
  shouldSubmitMessage, storeQueueAttempt,
  storeRequestInputDraft, storeSendAttempt, transcriptEntriesForFilter, transcriptEntryKey,
  transcriptEntryVisible, transcriptErrorPresentation, wrapMarkdownTables, encodeQuestionAnswer,
  fileChangeDiffs, formatElapsed,
  activityAge, indexMembershipChanged, indexStatusFreshForPage, indexStatusOrder,
  lifecyclePresentation, sessionTabFromHash,
} = require("./static/app.js");

const baseURL = process.argv[2];
if (!baseURL) throw new Error("browser contract test requires the server URL");
const unitOnly = baseURL === "--unit";
const testBaseURL = unitOnly ? "http://127.0.0.1" : baseURL;
const origin = new URL(testBaseURL).origin;
const fetchRequest = (path, options = {}) => fetch(new URL(path, testBaseURL), {
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
const transcriptView = captureTranscriptViewState({
  clientHeight: 400,
  querySelectorAll: () => [],
  scrollHeight: 1000,
  scrollTop: 275,
});
assert.equal(transcriptView.scrollTop, 275);
assert.equal(transcriptView.follow, false);
assert.deepEqual(Array.from(transcriptView.disclosures), []);
const filterEntries = [
  {kind: "userMessage", text: "question"},
  {kind: "reasoning", text: "condensed reasoning"},
  {kind: "commandExecution", summary: "command"},
  {kind: "error", summary: "failed"},
];
assert.equal(transcriptEntryVisible(filterEntries[0], "messages"), true);
assert.equal(transcriptEntryVisible(filterEntries[1], "messages"), true);
assert.equal(transcriptEntryVisible(filterEntries[1], "activity"), false);
assert.equal(transcriptEntryVisible(filterEntries[2], "messages"), false);
assert.equal(transcriptEntryVisible(filterEntries[3], "messages"), true);
assert.deepEqual(
  transcriptEntriesForFilter(filterEntries, "messages").map((entry) => entry.kind),
  ["userMessage", "reasoning", "error"],
);
assert.deepEqual(
  transcriptEntriesForFilter(filterEntries, "activity").map((entry) => entry.kind),
  ["commandExecution", "error"],
);
assert.deepEqual(transcriptErrorPresentation({
  kind: "error",
  summary: "Turn failed",
  text: "Selected model is at capacity. Please try a different model.",
}), {
  heading: "Turn failed",
  message: "Selected model is at capacity. Please try a different model.",
  details: "",
});
assert.deepEqual(transcriptErrorPresentation({
  kind: "error", summary: "Turn failed", text: "Readable failure", details: "ignored diagnostics",
}), {heading: "Turn failed", message: "Readable failure", details: "ignored diagnostics"});
assert.deepEqual(transcriptErrorPresentation({
  kind: "error", summary: "Unknown failure", details: JSON.stringify({code: "unknown"}),
}), {heading: "Unknown failure", message: "", details: '{"code":"unknown"}'});
assert.equal(formatElapsed(59_900), "59s");
assert.equal(formatElapsed(62_000), "1m 02s");
assert.equal(formatElapsed(3_661_000), "1h 01m");
assert.deepEqual(lifecyclePresentation(
  {kind: "archive", state: "running"}, "", 12_000,
), {
  detail: "Running · 12s elapsed", retry: false, title: "Archive: Starting", tone: "running",
});
assert.deepEqual(lifecyclePresentation(
  {kind: "revive", state: "failed", phase: "runtime_starting", error: "lock unavailable"}, "", 0,
), {
  detail: "lock unavailable", retry: true, title: "Revive failed during runtime starting", tone: "failed",
});
assert.deepEqual(lifecyclePresentation(
  {kind: "archive", state: "paused", phase: "tracking_committed"}, "archive", 4_000,
), {
  detail: "Paused · 4s since the last recorded update",
  retry: true,
  title: "Archive: Tracking committed",
  tone: "pending",
});
assert.deepEqual(lifecyclePresentation({state: "idle"}, "delete", 0), {
  detail: "Retry delete to continue.", retry: true, title: "Delete needs attention", tone: "pending",
});
const renderedDiffs = fileChangeDiffs(JSON.stringify([{
  path: "portal/<script>alert(1)</script>.js",
  kind: "update",
  diff: "@@ -1 +1 @@\n-old value\n+<script>new value</script>\n",
}]));
assert.equal(renderedDiffs[0].path, "portal/<script>alert(1)</script>.js");
assert.deepEqual(renderedDiffs[0].lines.map((line) => line.kind), [
  "header", "header", "hunk", "deletion", "addition",
]);
assert.equal(renderedDiffs[0].lines.at(-1).text, "+<script>new value</script>");
assert.deepEqual(fileChangeDiffs("not json"), []);
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
assert.deepEqual(sendAcknowledgementCandidates([
  {clientUserMessageId: "owned", clientUserMessageDigest: "a".repeat(64)},
  {clientUserMessageId: "another-browser", clientUserMessageDigest: "b".repeat(64)},
], [
  {id: "owned", message: "mine"},
]), [{id: "owned", message: "mine", transcriptDigest: "a".repeat(64)}]);
assert.deepEqual(sendAcknowledgementCandidates([
  {clientUserMessageId: "sending", clientUserMessageDigest: "a".repeat(64)},
  {clientUserMessageId: "settled", clientUserMessageDigest: "b".repeat(64)},
], [
  {id: "sending", message: "still in flight"},
  {id: "settled", message: "ready"},
], new Set(["sending"])), [{
  id: "settled", message: "ready", transcriptDigest: "b".repeat(64),
}]);
assert.deepEqual(sendAcknowledgementCandidates([
  {clientUserMessageId: "unicode-space", clientUserMessageDigest: "a".repeat(64)},
], [
  {id: "unicode-space", message: "\u0085message\u0085"},
]), [{
  id: "unicode-space", message: "\u0085message\u0085", transcriptDigest: "a".repeat(64),
}]);
const observedReceipts = new Map([
  ["sending", {id: "sending", message: "still in flight", state: "sending"}],
  ["unknown", {id: "unknown", message: "response lost", state: "unknown"}],
]);
markTranscriptMessagesObserved(observedReceipts, [
  {clientUserMessageId: "sending", text: "still in flight"},
  {clientUserMessageId: "unknown", text: "response lost"},
]);
assert.equal(observedReceipts.get("sending").state, "sending");
assert.equal(observedReceipts.get("unknown").state, "accepted");
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

if (!unitOnly) (async () => {
  let snoozedBeforeWizardAction = false;
  await beforeRequestInputAction(async () => { snoozedBeforeWizardAction = true; });
  assert.equal(snoozedBeforeWizardAction, true);

  const sessionPage = await fetchRequest("/example/");
  assert.equal(sessionPage.status, 200);
  const sessionHTML = await sessionPage.text();
  assert.match(sessionHTML, /href="#codex"[^>]+data-session-tab="codex"/);
  assert.match(sessionHTML, /data-transcript-filter="all"/);
  assert.match(sessionHTML, /data-transcript-filter="messages"/);
  assert.match(sessionHTML, /data-transcript-filter="activity"/);
  assert.match(sessionHTML, /id="codex-model"[^>]+data-existing-settings="true"/);
  assert.match(sessionHTML, /id="codex-work"/);
  assert.match(sessionHTML, /id="lifecycle-operation-status"/);
  assert.doesNotMatch(sessionHTML, /codex-settings-dialog|codex-settings-open/);

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
  await automaticClient.acknowledgeMessages([{
    clientUserMessageId: "00000000-0000-4000-8000-000000000004",
    digest: "ea5068c45b6c755e5dd592e23c713559960f37c09b5da18873ebc3982f42d211",
  }]);
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
      path: "/api/sessions/example/message-ack",
      body: {acknowledgements: [{
        clientUserMessageId: "00000000-0000-4000-8000-000000000004",
        digest: "ea5068c45b6c755e5dd592e23c713559960f37c09b5da18873ebc3982f42d211",
      }]},
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
    await client.message("browser message", "00000000-0000-4000-8000-000000000004", true),
    {
      turnId: "turn-1",
      clientUserMessageId: "00000000-0000-4000-8000-000000000004",
      steered: true,
    },
  );
  assert.deepEqual(await client.acknowledgeMessages([{
    clientUserMessageId: "00000000-0000-4000-8000-000000000004",
    digest: "ea5068c45b6c755e5dd592e23c713559960f37c09b5da18873ebc3982f42d211",
  }]), {acknowledgedClientUserMessageIds: ["00000000-0000-4000-8000-000000000004"]});
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

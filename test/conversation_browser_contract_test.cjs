const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const pathContract = JSON.parse(fs.readFileSync(
  path.join(__dirname, "../conversation/testdata/path_contract.json"), "utf8",
));
assert.equal(pathContract.schema, 1);

function contractID(testCase) {
  if (testCase.jsCodeUnits) return String.fromCharCode(...testCase.jsCodeUnits);
  if (testCase.repeatCount !== undefined) {
    return testCase.repeatValue.repeat(testCase.repeatCount);
  }
  return testCase.value;
}

class MemoryStorage {
  constructor() { this.values = new Map(); }
  get length() { return this.values.size; }
  getItem(key) { return this.values.has(key) ? this.values.get(key) : null; }
  key(index) { return Array.from(this.values.keys())[index] || null; }
  setItem(key, value) { this.values.set(key, value); }
  removeItem(key) { this.values.delete(key); }
}

(async () => {
  const {
    createConversationClient, createDurableAttemptStore, createDurableSender, mountConversation,
    formatTranscriptTimestamp, transcriptEntryCopyText, createTranscriptCopyButton, createCopyButton,
    createTranscriptActivity,
    transcriptEntryPresentation, transcriptEntryVisible, submissionFailurePresentation,
  } = await import(
    "../conversation/assets/conversation.js"
  );
  const kinds = ["userMessage", "agentMessage", "reasoning", "plan", "commandExecution", "futureKind", "error"];
  for (const [index, kind] of kinds.entries()) {
    const ordinary = {kind};
    const expected = index === 0 ? "user" : index < 4 ? "agent" : kind === "error" ? "error" : "event";
    assert.equal(transcriptEntryPresentation(ordinary).kind, expected);
    for (const communication of [undefined, null, [], "team", {kind: "future"}, {from: "sender"}]) {
      assert.deepEqual(transcriptEntryPresentation({...ordinary, communication}), transcriptEntryPresentation(ordinary));
    }
    const team = {...ordinary, communication: {kind: "team", from: "lead", to: "member"}};
    for (const [filter, visible] of Object.entries({all: true, messages: index < 4,
      activity: index >= 4, team: false, unknown: true})) {
      assert.equal(transcriptEntryVisible(ordinary, filter), visible || kind === "error");
      assert.equal(transcriptEntryVisible(team, filter), kind === "error" || ["all", "team", "unknown"].includes(filter));
    }
    assert.equal(transcriptEntryVisible(team), true);
  }
  assert.deepEqual(transcriptEntryPresentation({kind: "userMessage", communication: {kind: "team"}}),
    {kind: "team", label: "Team", team: true});
  assert.equal(transcriptEntryPresentation({communication: {kind: "team", from: "lead", to: "member"}}).label, "lead → member");
  assert.equal(transcriptEntryPresentation({communication: {kind: "team", from: "lead"}}).label, "Team · lead");
  assert.equal(transcriptEntryPresentation({communication: {kind: "team", to: "member"}}).label, "Team → member");
  assert.equal(transcriptEntryPresentation({communication: {kind: "team", from: {name: "bad"}}}).label, "Team");
  for (const error of [undefined, new TypeError("Failed to fetch"), new DOMException("Aborted", "AbortError"),
    ...[502, 503, 504].map(status => Object.freeze({status, code: "transport", message: `Request failed (${status})`})),
    {notSent: "true"}]) {
    const before = error && {...error};
    const failure = submissionFailurePresentation(error);
    assert.equal(failure.uncertain, true);
    assert.match(failure.message, /Delivery could not be confirmed/);
    if (error) assert.deepEqual({...error}, before, "presentation must preserve error metadata");
  }
  assert.deepEqual(submissionFailurePresentation({notSent: true, message: "Access denied"}),
    {message: "Access denied The message was not sent. Try again.", uncertain: false});
  const timestampOptions = {locales: "en-GB", timeZone: "Europe/Amsterdam"};
  const evening = formatTranscriptTimestamp({timestamp: "2026-09-11T21:59:00Z"}, timestampOptions);
  assert.equal(evening.text, "23:59");
  assert.equal(evening.dateKey, "2026-09-11");
  assert.equal(evening.dateLabel, "11 September 2026");
  assert.equal(evening.dateTime, "2026-09-11T21:59:00.000Z");
  assert.match(evening.title, /23:59:00/);
  assert.equal(evening.approximate, false);
  const midnight = formatTranscriptTimestamp({
    timestamp: "2026-09-11T22:00:00Z", timestampApproximate: true,
  }, timestampOptions);
  assert.equal(midnight.text, "~00:00");
  assert.equal(midnight.dateKey, "2026-09-12");
  assert.equal(midnight.approximate, true);
  assert.match(midnight.title, /approximate, based on turn timing/);
  const winter = formatTranscriptTimestamp({timestamp: "2026-12-31T23:00:00Z"}, timestampOptions);
  assert.equal(winter.text, "00:00");
  assert.equal(winter.dateKey, "2027-01-01");
  for (const timestamp of [undefined, null, "", "not a date"]) {
    assert.deepEqual(formatTranscriptTimestamp({timestamp}, timestampOptions), {
      text: "Time unavailable", title: "This message has no recorded time.",
      dateTime: "", dateKey: "", dateLabel: "", approximate: false,
    });
  }
  const sourceMarkdown = "# Heading\n\nA **bold** [link](https://example.com).\n\n```sh\nprintf 'hello'\n```\n";
  for (const kind of ["userMessage", "agentMessage", "reasoning", "plan"]) {
    assert.equal(transcriptEntryCopyText({
      kind, text: sourceMarkdown, html: "<h1>Heading</h1>", summary: "Interface heading",
      timestamp: "2026-09-11T12:00:00Z",
    }), sourceMarkdown);
  }
  const commandOutput = "first line\n" + "full output\n".repeat(1000);
  assert.equal(transcriptEntryCopyText({
    kind: "commandExecution", summary: "$ printf 'full output'", details: commandOutput,
  }), "printf 'full output'\n" + commandOutput);
  assert.equal(transcriptEntryCopyText({kind: "commandExecution", summary: "$ true"}), "true");
  const sourcePatch = "@@ -1 +1 @@\n-old\n+new\n";
  assert.equal(transcriptEntryCopyText({
    kind: "fileChange", summary: "File changes · completed", details: JSON.stringify([
      {path: "first.txt", kind: {type: "update", move_path: "renamed.txt"}, diff: sourcePatch},
      {path: "second.txt", kind: {type: "add"}, diff: "+contents\n"},
    ]),
  }), "first.txt\nrenamed.txt\n" + sourcePatch + "\n\nsecond.txt\n+contents\n");
  assert.equal(transcriptEntryCopyText({kind: "fileChange", details: "incomplete JSON"}), "incomplete JSON");
  assert.equal(transcriptEntryCopyText({
    kind: "mcpToolCall", summary: "Tool · server/read", details: "{\"result\": \"done\"}",
  }), "Tool · server/read\n\n{\"result\": \"done\"}");
  assert.equal(transcriptEntryCopyText({
    kind: "error", summary: "Turn failed", text: "Disconnected", details: "{\"code\": 1}",
  }), "Turn failed\n\nDisconnected\n\n{\"code\": 1}");
  assert.equal(transcriptEntryCopyText({kind: "unknown", details: "full event"}), "full event");
  assert.equal(transcriptEntryCopyText({kind: "agentMessage", html: "<p>Rendered only</p>"}), "");
  const requests = [];
  const fetchRequest = async (path, options = {}) => {
    requests.push({path, options});
    return {ok: true, status: 200, json: async () => ({})};
  };
  const client = createConversationClient({id: "opaque", basePath: "/shared", fetch: fetchRequest});
  await client.thread();
  await client.threadPage();
  await client.threadPage({cursor: "A/B?=+"});
  await client.pending();
  await client.queue();
  await client.models();
  await client.modes();
  await client.message("hello", "2a0e3d66-f923-4c79-bde5-c25c1edfd02b", true);
  await client.acknowledgeMessages([]);
  await client.queueMessage("later", "1a0e3d66-f923-4c79-bde5-c25c1edfd02b");
  await client.deleteQueued("queued-1");
  await client.deleteQueued("start");
  await client.startQueue("queued-1");
  await client.settings("model", "high", "plan");
  await client.interrupt();
  await client.respond("request-1", {decision: "accept"});
  await client.snooze("request-2");
  assert.deepEqual(requests.map(({path}) => path), [
    "/shared/conversations/opaque/thread",
    "/shared/conversations/opaque/thread/page",
    "/shared/conversations/opaque/thread/page?cursor=A%2FB%3F%3D%2B",
    "/shared/conversations/opaque/pending",
    "/shared/conversations/opaque/queue",
    "/shared/conversations/opaque/models",
    "/shared/conversations/opaque/collaboration-modes",
    "/shared/conversations/opaque/message",
    "/shared/conversations/opaque/message-ack",
    "/shared/conversations/opaque/queue",
    "/shared/conversations/opaque/queue/queued-1",
    "/shared/conversations/opaque/queue/start",
    "/shared/conversations/opaque/queue/start",
    "/shared/conversations/opaque/settings",
    "/shared/conversations/opaque/interrupt",
    "/shared/conversations/opaque/respond",
    "/shared/conversations/opaque/respond",
  ]);
  assert.equal(client.eventsPath(), "/shared/conversations/opaque/events");
  await client.snooze("bound", "offer-token");
  assert.deepEqual(JSON.parse(requests.at(-1).options.body), {id: "bound", snooze: true, token: "offer-token"});
  for (const notSent of [true, false]) {
    const unavailable = createConversationClient({id: "opaque", basePath: "/shared", fetch: async () => ({
      ok: false, status: 503, json: async () => ({error: "Connection changed", code: "prompt_transport", notSent}),
    })});
    await assert.rejects(unavailable.respond("bound", {token: "offer-token", decision: "accept"}), error =>
      error.status === 503 && error.code === "prompt_transport" && error.notSent === notSent);
  }

  assert.equal(JSON.parse(requests[7].options.body).retry, true);
  const compatibilityClient = createConversationClient({
    id: "opaque", basePath: "/shared", conversationPath: "/api/sessions/opaque",
    fetch: fetchRequest,
  });
  await compatibilityClient.thread();
  assert.equal(requests.at(-1).path, "/api/sessions/opaque/thread");
  assert.equal(compatibilityClient.eventsPath(), "/api/sessions/opaque/events");
  assert.throws(() => createConversationClient({
    id: "opaque", conversationPath: "https://untrusted.example/conversation",
  }), /conversationPath must be an absolute URL path/);
  assert.throws(() => createConversationClient({
    id: "opaque", conversationPath: "/\\untrusted.example/conversation",
  }), /conversationPath must be an absolute URL path/);
  assert.throws(() => createConversationClient({
    id: "opaque", basePath: "/\\untrusted.example",
  }), /basePath must be an absolute URL path/);
  assert.throws(() => createConversationClient({
    id: "opaque", conversationPath: "/api\\sessions/opaque",
  }), /conversationPath must be an absolute URL path/);
  assert.throws(() => createConversationClient({
    id: "opaque", conversationPath: "/api/old/../sessions/opaque",
  }), /conversationPath must be a canonical absolute URL path/);
  assert.throws(() => createConversationClient({
    id: "opaque", conversationPath: "/api/%73essions/opaque",
  }), /conversationPath must be an absolute URL path/);
  for (const conversationPath of [
    "/api/.", "/api/segment/..", "/api//", "/api///", "/api/segment/../",
  ]) {
    assert.throws(() => createConversationClient({id: "opaque", conversationPath}),
      /conversationPath must be a canonical absolute URL path/);
  }
  for (const basePath of [
    "/codex/.", "/codex/segment/..", "/codex//", "/codex///", "/codex/segment/../",
    "/café", "/api path", "/api\npath",
  ]) {
    assert.throws(() => createConversationClient({id: "opaque", basePath}), /basePath must be/);
  }
  const basePathAlphabet = new Set(pathContract.basePathAlphabet);
  for (let code = 0x20; code <= 0x7e; code += 1) {
    const character = String.fromCharCode(code);
    const basePath = `/api${character}segment`;
    let accepted = true;
    try {
      createConversationClient({id: "opaque", basePath, fetch: fetchRequest});
    } catch {
      accepted = false;
    }
    assert.equal(accepted, basePathAlphabet.has(character),
      `browser base-path acceptance differed for ASCII ${code}`);
  }
  const safeReservedClient = createConversationClient({
    id: "opaque", basePath: "/api-v1_~!$&()*+,;=:@", fetch: fetchRequest,
  });
  await safeReservedClient.thread();
  assert.equal(requests.at(-1).path, "/api-v1_~!$&()*+,;=:@/conversations/opaque/thread");
  for (const id of [".", ".."]) {
    assert.throws(() => createConversationClient({id}), /conversation id is invalid/);
  }
  for (const id of ["a".repeat(257), "ž".repeat(129), "\ud800"]) {
    assert.throws(() => createConversationClient({id}), /conversation id is invalid/);
  }
  const maximumIDClient = createConversationClient({
    id: "ž".repeat(128), basePath: "/codex", fetch: fetchRequest,
  });
  await maximumIDClient.thread();
  assert.equal(requests.at(-1).path,
    `/codex/conversations/${"%C5%BE".repeat(128)}/thread`);
  await maximumIDClient.deleteQueued("ž".repeat(128));
  assert.equal(requests.at(-1).path,
    `/codex/conversations/${"%C5%BE".repeat(128)}/queue/${"%C5%BE".repeat(128)}`);
  assert.throws(() => maximumIDClient.deleteQueued("ž".repeat(129)),
    /queued message id is invalid/);
  const requestsBeforeInvalidQueueStarts = requests.length;
  for (const id of ["", ".", "..", "queued/item", "queued\0item", "a".repeat(257),
    "ž".repeat(129), "\ud800"]) {
    assert.throws(() => maximumIDClient.startQueue(id), /queued message id is invalid/);
  }
  assert.equal(requests.length, requestsBeforeInvalidQueueStarts);
  await maximumIDClient.startQueue(" queued item ");
  assert.equal(JSON.parse(requests.at(-1).options.body).queuedSubmissionId, " queued item ");
  await maximumIDClient.startQueue("ž".repeat(128));
  assert.equal(JSON.parse(requests.at(-1).options.body).queuedSubmissionId, "ž".repeat(128));
  const fixtureClient = createConversationClient({
    id: "fixture", basePath: "/codex", fetch: fetchRequest,
  });
  for (const testCase of pathContract.opaqueIdCases) {
    const id = contractID(testCase);
    const requestCount = requests.length;
    if (!testCase.valid) {
      assert.throws(() => createConversationClient({id, fetch: fetchRequest}), TypeError,
        `conversation accepted fixture case ${testCase.name}`);
      assert.throws(() => fixtureClient.startQueue(id), TypeError,
        `queue start accepted fixture case ${testCase.name}`);
      assert.throws(() => fixtureClient.deleteQueued(id), TypeError,
        `queue deletion accepted fixture case ${testCase.name}`);
      assert.equal(requests.length, requestCount);
      continue;
    }
    const fixtureIDClient = createConversationClient({
      id, basePath: "/codex", fetch: fetchRequest,
    });
    await fixtureIDClient.thread();
    const encoded = testCase.encoded || encodeURIComponent(id);
    assert.equal(requests.at(-1).path, `/codex/conversations/${encoded}/thread`);
    await fixtureClient.startQueue(id);
    assert.equal(JSON.parse(requests.at(-1).options.body).queuedSubmissionId, id);
    await fixtureClient.deleteQueued(id);
    assert.equal(requests.at(-1).path, `/codex/conversations/fixture/queue/${encoded}`);
  }
  const literalEscapeClient = createConversationClient({
    id: "%2F%%41-%2e%2e-ž", basePath: "/codex", fetch: fetchRequest,
  });
  await literalEscapeClient.thread();
  assert.equal(requests.at(-1).path,
    "/codex/conversations/%252F%25%2541-%252e%252e-%C5%BE/thread");
  await literalEscapeClient.deleteQueued("%2F%%41-%2e%2e-ž");
  assert.equal(requests.at(-1).path,
    "/codex/conversations/%252F%25%2541-%252e%252e-%C5%BE/queue/" +
    "%252F%25%2541-%252e%252e-%C5%BE");
  assert.throws(() => literalEscapeClient.deleteQueued("."), /queued message id is invalid/);
  assert.throws(() => literalEscapeClient.deleteQueued(".."), /queued message id is invalid/);
  const literalEscapeStorage = new MemoryStorage();
  await createDurableSender({
    client: literalEscapeClient, storage: literalEscapeStorage, id: "%2F%%41-%2e%2e-ž",
    randomUUID: () => "3b0e3d66-f923-4c79-bde5-c25c1edfd02b",
  }).send("literal escape identity");
  assert.notEqual(literalEscapeStorage.getItem(
    "codex-web:send:/codex:%252F%25%2541-%252e%252e-%C5%BE",
  ), null);
  const rootClient = createConversationClient({id: "opaque", basePath: "/", fetch: fetchRequest});
  await rootClient.thread();
  assert.equal(requests.at(-1).path, "/conversations/opaque/thread");
  assert.equal(rootClient.eventsPath(), "/conversations/opaque/events");
  const rootConversationClient = createConversationClient({
    id: "opaque", conversationPath: "/", fetch: fetchRequest,
  });
  await rootConversationClient.thread();
  assert.equal(requests.at(-1).path, "/thread");
  assert.equal(rootConversationClient.eventsPath(), "/events");

  const sharedStorage = new MemoryStorage();
  const sharedStore = createDurableAttemptStore({
    storage: sharedStorage,
    prefix: "workspace-portal.send-attempt.example.thread-1.",
    decode: (id, value) => (
      value && typeof value.message === "string" && typeof value.context === "string" ?
        {id, message: value.message, context: value.context} : null
    ),
    encode: ({message, context}) => ({message, context}),
  });
  const sharedAttemptA = {
    id: "0a0e3d66-f923-4c79-bde5-c25c1edfd02b", message: "first", context: "plan:a",
  };
  const sharedAttemptB = {
    id: "0b0e3d66-f923-4c79-bde5-c25c1edfd02b", message: "second", context: "plan:b",
  };
  assert.equal(sharedStore.available(), true);
  assert.equal(sharedStore.store(sharedAttemptA), true);
  assert.equal(sharedStore.store(sharedAttemptB), true);
  assert.deepEqual(sharedStore.load(), [sharedAttemptA, sharedAttemptB]);
  assert.equal(sharedStore.remove(sharedAttemptA.id), true);
  assert.deepEqual(sharedStore.load(), [sharedAttemptB]);
  sharedStorage.setItem(
    "workspace-portal.send-attempt.example.thread-1.bad-id", JSON.stringify({message: "bad"}),
  );
  assert.equal(sharedStore.load(), null);
  assert.equal(sharedStore.clear(), true);
  assert.deepEqual(sharedStore.load(), []);

  const discardedWrites = {
    getItem: () => null, key: () => null, get length() { return 0; },
    removeItem() {}, setItem() {},
  };
  const discardedStore = createDurableAttemptStore({
    storage: discardedWrites, prefix: "attempt.",
  });
  assert.equal(discardedStore.available(), false);
  assert.equal(discardedStore.store(sharedAttemptA), false);

  const storage = new MemoryStorage();
  const attempts = [];
  let loseFirstResponse = true;
  const durableClient = {
    async message(message, id, retry) {
      attempts.push({message, id, retry});
      if (loseFirstResponse) {
        loseFirstResponse = false;
        throw new Error("response lost");
      }
      return {turnId: "turn-1", clientUserMessageId: id};
    },
    async acknowledgeMessages(items) {
      return {acknowledgedClientUserMessageIds: items.map((item) => item.clientUserMessageId)};
    },
    async queueMessage(message, id) {
      attempts.push({message, id, queue: true});
      return {id: "queued-1", text: message, clientUserMessageId: id};
    },
  };
  const first = createDurableSender({
    client: durableClient, storage, id: "opaque", basePath: "/codex",
    randomUUID: () => "2a0e3d66-f923-4c79-bde5-c25c1edfd02b",
  });
  await assert.rejects(first.send("hello"), /response lost/);
  const afterReload = createDurableSender({
    client: durableClient, storage, id: "opaque", basePath: "/codex",
    randomUUID: () => "must-not-be-used",
  });
  await afterReload.send("hello");
  assert.deepEqual(attempts, [
    {message: "hello", id: "2a0e3d66-f923-4c79-bde5-c25c1edfd02b", retry: false},
    {message: "hello", id: "2a0e3d66-f923-4c79-bde5-c25c1edfd02b", retry: true},
  ]);
  assert(afterReload.pending(), "receipt must remain until transcript acknowledgement");
  assert.equal(await afterReload.acknowledge([]), false);
  assert.equal(await afterReload.acknowledge([{
    clientUserMessageId: "2a0e3d66-f923-4c79-bde5-c25c1edfd02b",
    clientUserMessageDigest: "a".repeat(64),
  }]), true);
  assert.equal(afterReload.pending(), null);

  let loseQueueResponse = true;
  durableClient.queueMessage = async (message, id) => {
    attempts.push({message, id, queue: true});
    if (loseQueueResponse) {
      loseQueueResponse = false;
      throw new Error("queue response lost");
    }
    return {id: "queued-1", text: message, clientUserMessageId: id};
  };
  const queueAttempt = createDurableSender({
    client: durableClient, storage, id: "opaque", basePath: "/shared",
    randomUUID: () => "1a0e3d66-f923-4c79-bde5-c25c1edfd02b",
  });
  await assert.rejects(queueAttempt.queue("later"), /queue response lost/);
  assert.equal(queueAttempt.pendingQueue().id, "1a0e3d66-f923-4c79-bde5-c25c1edfd02b");
  const queueReload = createDurableSender({
    client: durableClient, storage, id: "opaque", basePath: "/shared",
    randomUUID: () => "must-not-be-used",
  });
  await queueReload.queue("later");
  assert.equal(queueReload.pendingQueue(), null);
  assert.deepEqual(attempts.slice(-2), [
    {message: "later", id: "1a0e3d66-f923-4c79-bde5-c25c1edfd02b", queue: true},
    {message: "later", id: "1a0e3d66-f923-4c79-bde5-c25c1edfd02b", queue: true},
  ]);

  const requestsBeforeUnavailableStorage = attempts.length;
  assert.throws(() => createDurableSender({
    client: durableClient,
    storage: {getItem() { return null; }, setItem() { throw new Error("denied"); }, removeItem() {}},
    id: "opaque", basePath: "/codex",
  }), /Durable browser storage is unavailable/);
  assert.equal(attempts.length, requestsBeforeUnavailableStorage);

  const corrupt = new MemoryStorage();
  corrupt.setItem("codex-web:send:/codex:opaque", "not-json");
  let corruptUUIDCalls = 0;
  const corruptSender = createDurableSender({
    client: durableClient, storage: corrupt, id: "opaque", basePath: "/codex",
    randomUUID() {
      corruptUUIDCalls += 1;
      return "5a0e3d66-f923-4c79-bde5-c25c1edfd02b";
    },
  });
  await assert.rejects(
    corruptSender.send("replacement"), /Stored conversation operation is invalid/,
  );
  assert.throws(corruptSender.pending, /Stored conversation operation is invalid/);
  assert.equal(corruptUUIDCalls, 0);
  assert.equal(attempts.length, requestsBeforeUnavailableStorage);
  const corruptQueue = new MemoryStorage();
  corruptQueue.setItem("codex-web:queue:/codex:opaque", JSON.stringify({id: "bad", message: "later"}));
  const corruptQueueSender = createDurableSender({
    client: durableClient, storage: corruptQueue, id: "opaque", basePath: "/codex",
    randomUUID: () => "must-not-be-used",
  });
  await assert.rejects(
    corruptQueueSender.queue("later"), /Stored conversation operation is invalid/,
  );
  assert.throws(corruptQueueSender.pendingQueue, /Stored conversation operation is invalid/);
  assert.equal(attempts.length, requestsBeforeUnavailableStorage);

  const isolated = new MemoryStorage();
  const isolatedClient = {
    async message(_message, id) { return {turnId: "turn", clientUserMessageId: id}; },
    async acknowledgeMessages() { return {acknowledgedClientUserMessageIds: []}; },
    async queueMessage() { throw new Error("not used"); },
  };
  await createDurableSender({
    client: isolatedClient, storage: isolated, id: "opaque", basePath: "/one",
    randomUUID: () => "3a0e3d66-f923-4c79-bde5-c25c1edfd02b",
  }).send("one");
  await createDurableSender({
    client: isolatedClient, storage: isolated, id: "opaque", basePath: "/two",
    randomUUID: () => "4a0e3d66-f923-4c79-bde5-c25c1edfd02b",
  }).send("two");
  assert.deepEqual([...isolated.values.keys()].sort(), [
    "codex-web:send:/one:opaque",
    "codex-web:send:/two:opaque",
  ]);

  const endpointStorage = new MemoryStorage();
  await createDurableSender({
    client: isolatedClient, storage: endpointStorage, id: "opaque",
    conversationPath: "/api/first/opaque",
    randomUUID: () => "6a0e3d66-f923-4c79-bde5-c25c1edfd02b",
  }).send("first");
  await createDurableSender({
    client: isolatedClient, storage: endpointStorage, id: "opaque",
    conversationPath: "/api/second/opaque",
    randomUUID: () => "7a0e3d66-f923-4c79-bde5-c25c1edfd02b",
  }).send("second");
  assert.deepEqual([...endpointStorage.values.keys()].sort(), [
    "codex-web:send:path:/api/first/opaque",
    "codex-web:send:path:/api/second/opaque",
  ]);

  const coupledStorage = new MemoryStorage();
  const coupledClient = createConversationClient({
    id: "opaque", conversationPath: "/api/coupled/opaque", fetch: async () => ({
      ok: true,
      status: 200,
      json: async () => ({
        turnId: "turn", clientUserMessageId: "7b0e3d66-f923-4c79-bde5-c25c1edfd02b",
      }),
    }),
  });
  assert.equal(Object.getOwnPropertySymbols(coupledClient).length, 0);
  assert.deepEqual(Reflect.ownKeys(coupledClient).filter((key) => typeof key === "symbol"), []);
  await createDurableSender({
    client: coupledClient, storage: coupledStorage, id: "opaque",
    randomUUID: () => "7b0e3d66-f923-4c79-bde5-c25c1edfd02b",
  }).send("coupled");
  assert.deepEqual([...coupledStorage.values.keys()], [
    "codex-web:send:path:/api/coupled/opaque",
  ]);
  assert.throws(() => createDurableSender({
    client: coupledClient, storage: coupledStorage, id: "opaque",
    conversationPath: "/api/different/opaque",
  }), /durable sender target does not match the conversation client/);
  assert.throws(() => createDurableSender({
    client: coupledClient, storage: coupledStorage, id: "different",
  }), /durable sender id does not match the conversation client/);
  assert.throws(() => createDurableSender({
    client: isolatedClient, storage: coupledStorage, id: "opaque",
  }), /custom conversation client requires an explicit durable target/);
  const invalidTargetStorage = new MemoryStorage();
  let invalidTargetCalls = 0;
  const invalidTargetClient = {
    ...isolatedClient,
    async message() { invalidTargetCalls += 1; return {}; },
  };
  for (const options of [
    {basePath: ""}, {basePath: null}, {basePath: false}, {basePath: 0},
    {conversationPath: ""}, {conversationPath: null},
    {conversationPath: false}, {conversationPath: 0},
  ]) {
    assert.throws(() => createDurableSender({
      client: invalidTargetClient, storage: invalidTargetStorage, id: "opaque", ...options,
    }), /must be an absolute URL path/);
  }
  assert.equal(invalidTargetCalls, 0);
  assert.equal(invalidTargetStorage.length, 0);

  const aliasStorage = new MemoryStorage();
  const aliasClient = {
    ...isolatedClient,
    async message() { throw new Error("response lost"); },
  };
  const legacyAlias = createDurableSender({
    client: aliasClient, storage: aliasStorage, id: "opaque",
    conversationPath: "/api/sessions/opaque", durableNamespace: "conversation:opaque",
    randomUUID: () => "8a0e3d66-f923-4c79-bde5-c25c1edfd02b",
  });
  await assert.rejects(legacyAlias.send("alias message"), /response lost/);
  const canonicalAlias = createDurableSender({
    client: aliasClient, storage: aliasStorage, id: "opaque",
    conversationPath: "/codex/conversations/opaque", durableNamespace: "conversation:opaque",
    randomUUID: () => "9a0e3d66-f923-4c79-bde5-c25c1edfd02b",
  });
  assert.deepEqual(canonicalAlias.pending(), {
    id: "8a0e3d66-f923-4c79-bde5-c25c1edfd02b", message: "alias message", receipt: null,
  });
  assert.deepEqual([...aliasStorage.values.keys()], [
    "codex-web:send:namespace:conversation%3Aopaque",
  ]);

  class FakeElement {
    constructor(name) {
      this.name = name;
      this.attributes = {};
      this.children = [];
      this.listeners = new Map();
      this.textContent = "";
      this.value = "";
    }
    setAttribute(key, value) { this.attributes[key] = String(value); }
    get childNodes() { return this.children; }
    append(...children) { this.children.push(...children); }
    replaceChildren(...children) {
      this.children = children;
      if (this.name === "select" && children[0] && !this.value) {
        this.value = children[0].attributes.value || "";
      }
    }
    addEventListener(name, listener) { this.listeners.set(name, listener); }
    showModal() { this.open = true; }
    close() { this.open = false; this.listeners.get("close")?.(); }
    descendants() { return [this, ...this.children.flatMap((child) => child.descendants())]; }
  }
  globalThis.Element = FakeElement;
  globalThis.document = {
    createElement: (name) => new FakeElement(name),
    createTextNode: text => Object.assign(new FakeElement("#text"), {textContent: text}),
    createElementNS: (_namespace, name) => new FakeElement(name),
  };
  const typedSearch = createTranscriptActivity({
    kind: "webSearch", summary: "Web search", details: "fallback raw details",
    activity: {queries: ["literal <search>"], links: [
      {url: "https://example.test/page", title: "<script>literal title</script>"},
      {url: "javascript:alert(1)", title: "blocked"},
      {url: "https://user:secret@example.test/", title: "blocked credentials"},
      {url: "data:text/html,test", title: "blocked data"},
    ]},
  });
  const typedLinks = typedSearch.descendants().filter((element) => element.name === "a");
  assert.equal(typedLinks.length, 1);
  assert.equal(typedLinks[0].attributes.rel, "noopener noreferrer");
  assert.equal(typedLinks[0].textContent, "<script>literal title</script>");
  assert.equal(typedSearch.descendants().filter((element) => element.name === "details").length, 1);
  const typedAgent = createTranscriptActivity({kind: "subAgentActivity", summary: "Agent completed",
    activity: {agentPath: "/root/review", agents: [{threadId: "untrusted-child", status: "completed", message: "Result"}]},
  });
  assert.equal(typedAgent.descendants().filter((element) => element.name === "a").length, 0);
  assert.equal(createTranscriptActivity({kind: "unknown", details: "fallback"}), null);
  assert.equal(createTranscriptActivity({kind: "webSearch", activity: []}), null);
  const futureActivity = createTranscriptActivity({kind: "futureTypedKind", activity: {queries: ["Future query"], agents: [null]}});
  assert.ok(futureActivity.descendants().some((element) => element.textContent === "Future query"));
  const copied = [];
  const clipboard = {async writeText(text) { copied.push(text); }};
  Object.defineProperty(globalThis, "navigator", {value: {clipboard}, configurable: true});
  const streamingEntry = {kind: "agentMessage", text: "First streamed text"};
  const copyButton = createTranscriptCopyButton(streamingEntry);
  assert.equal(copyButton.name, "button");
  assert.equal(copyButton.attributes.type, "button");
  assert.equal(copyButton.attributes["aria-label"], "Copy message");
  assert.equal(copyButton.children[0].name, "svg");
  assert.equal(copyButton.children[0].attributes["aria-hidden"], "true");
  assert.equal(copyButton.children[1].textContent, "");
  const scheduledFeedback = [];
  const savedSetTimeout = globalThis.setTimeout;
  const savedClearTimeout = globalThis.clearTimeout;
  globalThis.setTimeout = (callback, delay) => {
    assert.equal(delay, 2000);
    scheduledFeedback.push(callback);
    return scheduledFeedback.length;
  };
  globalThis.clearTimeout = () => {};
  try {
    await copyButton.listeners.get("click")();
    assert.deepEqual(copied, ["First streamed text"]);
    assert.equal(copyButton.children[1].textContent, "Copied");
    assert.equal(copyButton.children[0].children[0].attributes.d, "m3 8 3 3 7-7");
    assert.equal(copyButton.attributes["data-copy-state"], "copied");
    scheduledFeedback.pop()();
    assert.equal(copyButton.children[1].textContent, "");
    assert.equal(copyButton.attributes["aria-label"], "Copy message");
    streamingEntry.text += " with the latest chunk";
    await copyButton.listeners.get("click")();
    assert.equal(copied[1], "First streamed text with the latest chunk");
    const hash = "a".repeat(40);
    const hashButton = createCopyButton({text: hash, label: "Copy commit hash"});
    await hashButton.listeners.get("click")();
    assert.equal(copied.at(-1), hash);
    assert.equal(hashButton.children[1].attributes.role, "status");
    let address = "https://example.test/first";
    const linkButton = createCopyButton({text: () => address, label: "Copy comparison link"});
    address = "https://example.test/updated#new-L42";
    await linkButton.listeners.get("click")();
    assert.equal(copied.at(-1), address);
    const getterButton = createCopyButton({text: "ignored", getText: () => "getter"});
    await getterButton.listeners.get("click")();
    assert.equal(copied.at(-1), "getter");
    clipboard.writeText = async () => { throw new Error("permission denied"); };
    await copyButton.listeners.get("click")();
    assert.equal(copyButton.children[1].textContent, "Copy failed. Try again.");
    assert.equal(copyButton.attributes["data-copy-state"], "error");
    assert.equal(copyButton.attributes.title, "Copy failed. Try again.");
    assert.equal(copyButton.disabled, false);
    delete globalThis.navigator.clipboard;
    await copyButton.listeners.get("click")();
    assert.equal(copyButton.children[1].textContent, "Copy failed. Try again.");
    scheduledFeedback.pop()();
    assert.equal(copyButton.attributes["data-copy-state"], "idle");
  } finally {
    globalThis.setTimeout = savedSetTimeout;
    globalThis.clearTimeout = savedClearTimeout;
  }
  const mountedCalls = [];
  const mountedClient = {
    async thread() {
      mountedCalls.push("thread");
      return {
        threadId: "thread-1",
        entries: [
          {kind: "userMessage", text: "First", timestamp: "2026-09-10T12:00:00Z"},
          {kind: "agentMessage", text: "Second", timestamp: "2026-09-11T12:00:00Z", timestampApproximate: true},
          {kind: "agentMessage", text: "Unknown time"},
          {kind: "commandExecution", summary: "Activity", timestamp: "2026-09-11T12:01:00Z"},
        ], status: "idle", model: "model-b", reasoningEffort: "xhigh",
        collaborationMode: "plan",
      };
    },
    async models() {
      mountedCalls.push("models");
      return [
        {model: "model-a", supportedReasoningEfforts: [{reasoningEffort: "medium"}]},
        {model: "model-b", supportedReasoningEfforts: [{reasoningEffort: "xhigh"}]},
      ];
    },
    async modes() {
      mountedCalls.push("modes");
      return [{mode: "default"}, {mode: "plan"}];
    },
    eventsPath() { return "/events"; },
  };
  const root = new FakeElement("main");
  const unmount = mountConversation(root, {
    id: "opaque",
    client: mountedClient,
    capabilities: {
      pending: false, queueRead: false, send: false, queue: false,
      interrupt: false, respond: false, eventStream: false,
    },
  });
  await new Promise((resolve) => setImmediate(resolve));
  const findControl = (label) => root.descendants().find((element) => (
    element.attributes["aria-label"] === label
  ));
  assert.equal(findControl("Model").value, "model-b");
  assert.equal(findControl("Reasoning effort").value, "xhigh");
  assert.equal(findControl("Collaboration mode").value, "plan");
  assert.deepEqual(mountedCalls.sort(), ["models", "modes", "thread"]);
  const mountedTimes = root.descendants().filter((element) => element.attributes.class === "codex-entry-time");
  assert.equal(mountedTimes.length, 4);
  assert.equal(mountedTimes[0].name, "time");
  assert.equal(mountedTimes[0].attributes.datetime, "2026-09-10T12:00:00.000Z");
  assert.match(mountedTimes[1].textContent, /^~/);
  assert.equal(mountedTimes[2].name, "span");
  assert.equal(mountedTimes[2].textContent, "Time unavailable");
  const mountedFooters = root.descendants().filter((element) => element.attributes.class === "codex-entry-footer");
  assert.equal(mountedFooters.length, 4);
  mountedFooters.forEach((footer, index) => {
    assert.equal(footer.children[0].attributes.class, "codex-copy-button codex-entry-copy");
    assert.equal(footer.children[1], mountedTimes[index]);
    const item = root.descendants().find((element) => element.children.includes(footer));
    assert.equal(item.children.at(-1), footer);
  });
  assert.equal(root.descendants().filter((element) => element.attributes.class === "codex-conversation-date").length, 2);
  unmount();

  const teamRoot = new FakeElement("main");
  const senderLabel = '<img src=x onerror="bad()">';
  const disposeTeam = mountConversation(teamRoot, {id: "team", client: {
    async thread() { return {threadId: "team-thread", status: "idle", entries: [
      {kind: "userMessage", text: "# original", displayText: "# displayed", html: "<h1>displayed</h1>"},
      {kind: "userMessage", text: "coordination", communication: {kind: "team", from: senderLabel, to: "member"}},
      {kind: "commandExecution", summary: "command"},
      {kind: "error", text: "turn failed", communication: {kind: "team"}},
    ]}; },
  }, capabilities: {pending: false, queueRead: false, send: false, queue: false,
    interrupt: false, settings: false, respond: false, eventStream: false}});
  await new Promise(resolve => setImmediate(resolve));
  const teamFilter = teamRoot.descendants().find(el => el.attributes["aria-label"] === "Transcript filter");
  const transcriptList = teamRoot.descendants().find(el => el.name === "ol");
  const teamEntry = transcriptList.children.find(el => el.attributes.class === "codex-entry codex-entry-team");
  assert.equal(teamEntry.children[0].children[0].textContent, `${senderLabel} → member`);
  assert.equal(teamEntry.children[0].children[0].innerHTML, undefined, "labels must never become HTML");
  assert.equal(transcriptList.children[0].children[1].innerHTML, "<h1>displayed</h1>");
  transcriptList.scrollTop = 45;
  for (const [value, count] of [["messages", 2], ["team", 2], ["activity", 2], ["all", 4]]) {
    teamFilter.value = value; teamFilter.listeners.get("change")();
    assert.equal(transcriptList.children.length, count);
    assert(transcriptList.children.some(el => el.attributes.class === "codex-entry codex-entry-error"));
  }
  assert.equal(transcriptList.scrollTop, 45);
  assert(transcriptList.children.includes(teamEntry), "filter changes preserve message nodes");
  assert.equal(teamRoot.descendants().find(el => el.attributes.class === "codex-history-controls").hidden, true,
    "successful legacy fallback is silent");
  disposeTeam();

  const citationToken = "\ue200cite\ue202fixture\ue201", citationPrefix = "Příliš 🐈 ";
  const citationStart = Buffer.byteLength(citationPrefix);
  const citationEntry = {kind: "agentMessage", turnStatus: "completed", text: citationPrefix + citationToken + " done",
    citations: [{start: citationStart, end: citationStart + Buffer.byteLength(citationToken),
      sources: [{url: "https://example.com/source", title: 'Source "title"', number: 1}]}]};
  const citationOriginal = JSON.stringify(citationEntry);
  const citationRoot = new FakeElement("main");
  const disposeCitations = mountConversation(citationRoot, {id: "citations", client: {
    async thread() { return {threadId: "citation-thread", status: "idle", entries: [
      citationEntry,
      {...citationEntry, html: '<p>Host <a href="https://example.com/source">[1]</a></p>'},
      {...citationEntry, kind: "userMessage", displayText: "decorated user", html: "<p>decorated user</p>"},
      {...citationEntry, kind: "mcpToolCall", summary: "Typed activity", activity: {status: "completed"}, html: "<p>unused</p>"},
    ]}; },
  }, capabilities: {pending: false, queueRead: false, send: false, queue: false,
    interrupt: false, settings: false, respond: false, eventStream: false}});
  await new Promise(resolve => setImmediate(resolve));
  const citationRows = citationRoot.descendants().find(el => el.name === "ol").children;
  const fallback = citationRows[0].children[1];
  assert.equal(fallback.name, "pre");
  assert.equal(fallback.children.find(el => el.name === "a").attributes.href, "https://example.com/source");
  assert.equal(fallback.children.find(el => el.name === "a").attributes.title, 'Source "title"');
  assert.equal(citationRows[1].children[1].innerHTML, '<p>Host <a href="https://example.com/source">[1]</a></p>');
  assert.equal(citationRows[2].children[1].innerHTML, "<p>decorated user</p>");
  assert.equal(citationRows[3].children[1].attributes.class, "codex-typed-activity");
  for (const kind of ["agentMessage", "reasoning", "plan"]) {
    assert.equal(transcriptEntryCopyText({...citationEntry, kind}),
      'Příliš 🐈 [\\[1\\]](<https://example.com/source> "Source \\"title\\"") done');
  }
  assert.equal(transcriptEntryCopyText({...citationEntry, kind: "userMessage"}), citationEntry.text);
  assert.equal(JSON.stringify(citationEntry), citationOriginal, "render and copy preserve raw text and UTF-8 spans");
  disposeCitations();

  // Completed citation rows still resolve after leaving the newest page, even
  // while a filter excludes them. Filtering cannot gate history observations.
  let citationEvents, citationNewestReads = 0, citationRepairs = 0;
  class CitationEvents {
    constructor() { citationEvents = this; }
    addEventListener() {}
    close() {}
  }
  const pendingCitation = {...citationEntry, turnId: "citation-turn", itemId: "pending", citationsPending: true,
    citations: [{...citationEntry.citations[0], sources: [], unavailable: true}]};
  const overlap = {kind: "agentMessage", turnId: "overlap", itemId: "overlap", turnStatus: "completed", text: "overlap"};
  const repairRootCitations = new FakeElement("main");
  const disposeRepairCitations = mountConversation(repairRootCitations, {id: "pending-citations", EventSource: CitationEvents,
    refreshPolicy: {eventDebounceMs: 0, retryInitialMs: 30, historyRepairMs: 0, historyPageMs: 1},
    client: {
      eventsPath: () => "/citation-events",
      async threadPage({cursor} = {}) {
        if (cursor) {
          citationRepairs++;
          return {threadId: "pending-citations", status: "idle", hasOlder: false, olderCursor: null,
            entries: [{...citationEntry, turnId: "citation-turn", itemId: "pending", citationsPending: false}]};
        }
        citationNewestReads++;
        return {threadId: "pending-citations", status: "idle", hasOlder: true, olderCursor: "find-citation",
          entries: citationNewestReads === 1 ? [pendingCitation, overlap] : [overlap,
            {kind: "agentMessage", turnId: "latest", itemId: "latest", text: "newest"}]};
      },
    }, capabilities: {pending: false, queueRead: false, send: false, queue: false,
      interrupt: false, settings: false, respond: false, eventStream: true}});
  await new Promise(resolve => setImmediate(resolve));
  const citationFilter = repairRootCitations.descendants().find(el => el.attributes["aria-label"] === "Transcript filter");
  citationFilter.value = "team"; citationFilter.listeners.get("change")();
  const citationList = repairRootCitations.descendants().find(el => el.name === "ol");
  assert.equal(citationList.children.length, 0);
  citationEvents.onmessage();
  for (let index = 0; index < 100 && !citationRepairs; index++) await new Promise(resolve => setTimeout(resolve, 5));
  assert.equal(citationRepairs, 1);
  assert.equal(citationList.children.length, 0, "repair does not change active filter");
  assert.equal(repairRootCitations.descendants().find(el => el.attributes.class === "codex-history-controls").children[1].textContent, "",
    "background citation repair stays quiet");
  citationFilter.value = "all"; citationFilter.listeners.get("change")();
  const repairedLink = citationList.descendants().find(el => el.name === "a");
  assert.equal(repairedLink.attributes.href, "https://example.com/source");
  assert.equal(citationList.children.length, 3);
  disposeRepairCitations();

  // A failed HTTP response must retain the exact durable attempt, but must not
  // downgrade a message already observed and acknowledged through the transcript.
  for (const observedBeforeFailure of [false, true]) {
    const lossStorage = new MemoryStorage();
    let lossEvents, rejectSend, sent, accepted = false, backendTurns = 0;
    const ids = new Set(), lossCalls = [];
    class LossEvents {
      constructor() { lossEvents = this; }
      addEventListener() {}
      close() {}
    }
    const lossClient = {
      eventsPath: () => "/loss-events",
      async thread() { return {threadId: "loss-thread", status: "idle", entries: accepted ? [{
        kind: "userMessage", text: "retained input", clientUserMessageId: sent,
        clientUserMessageDigest: "b".repeat(64),
      }] : []}; },
      async message(message, id, retry, attachmentIds) {
        lossCalls.push({message, id, retry, attachmentIds}); sent = id;
        if (!ids.has(id)) { ids.add(id); backendTurns++; }
        if (lossCalls.length === 1) return new Promise((_, reject) => { rejectSend = reject; });
        return {turnId: "accepted-turn", clientUserMessageId: id};
      },
      async acknowledgeMessages(entries) { return {acknowledgedClientUserMessageIds: entries.map(entry => entry.clientUserMessageId)}; },
    };
    const lossRoot = new FakeElement("main");
    const disposeLoss = mountConversation(lossRoot, {id: "loss", basePath: "/codex", client: lossClient,
      EventSource: LossEvents, refreshPolicy: {eventDebounceMs: 0}, storage: lossStorage,
      randomUUID: () => "aa0e3d66-f923-4c79-bde5-c25c1edfd02b",
      capabilities: {pending: false, queueRead: false, queue: false, interrupt: false, settings: false, respond: false}});
    await new Promise(resolve => setImmediate(resolve));
    const lossTextarea = lossRoot.descendants().find(el => el.name === "textarea");
    const lossForm = lossRoot.descendants().find(el => el.name === "form");
    const notice = lossRoot.descendants().find(el => el.attributes.class === "codex-submission-status");
    lossTextarea.value = "retained input";
    const sending = lossForm.listeners.get("submit")({preventDefault() {}});
    await new Promise(resolve => setImmediate(resolve));
    if (observedBeforeFailure) {
      accepted = true; lossEvents.onmessage();
      for (let index = 0; index < 50 && lossTextarea.value; index++) await new Promise(resolve => setTimeout(resolve, 5));
      assert.equal(lossTextarea.value, "");
    }
    rejectSend(Object.assign(new Error("Request failed (502)"), {status: 502}));
    await sending;
    if (observedBeforeFailure) assert.equal(notice.hidden, true);
    else {
      assert.equal(lossTextarea.value, "retained input");
      assert.equal(notice.hidden, false); assert.match(notice.textContent, /Delivery could not be confirmed/);
      const retained = createDurableSender({client: lossClient, storage: lossStorage, id: "loss", basePath: "/codex",
        randomUUID: () => { throw Error("must reuse identity"); }});
      assert.equal(retained.pending().id, sent);
      await retained.send("retained input");
      assert.deepEqual(lossCalls.map(call => [call.id, call.retry]), [[sent, false], [sent, true]]);
      const receipt = retained.pending().receipt;
      submissionFailurePresentation({status: 502});
      assert.deepEqual(retained.pending().receipt, receipt);
      assert.equal(await retained.acknowledge([]), false);
      assert(retained.pending(), "unseen receipts stay retained");
    }
    assert.equal(backendTurns, 1);
    disposeLoss();
  }

  // Recovery order is independent of rendering. Exercise the mounted composer
  // and real upload selection, including unrelated confirmations and edits.
  class UploadElement extends FakeElement {
    constructor(name) {
      super(name);
      this.classList = {add() {}, remove() {}};
    }
    append(...nodes) { for (const node of nodes) { node.remove?.(); node.parent = this; this.children.push(node); } }
    prepend(...nodes) { for (const node of [...nodes].reverse()) { node.remove?.(); node.parent = this; this.children.unshift(node); } }
    insertBefore(node, reference) {
      if (node === reference) return;
      node.remove?.(); node.parent = this;
      const index = this.children.indexOf(reference);
      this.children.splice(index < 0 ? this.children.length : index, 0, node);
    }
    replaceChildren(...nodes) { this.children.forEach(node => { node.parent = null; }); this.children = []; this.append(...nodes); }
    remove() { if (this.parent) this.parent.children = this.parent.children.filter(node => node !== this); this.parent = null; }
    removeEventListener(name) { this.listeners.delete(name); }
    matches() { return false; }
    focus() {}
  }
  const recoveryGlobals = {document: globalThis.document, fetch: globalThis.fetch,
    add: globalThis.addEventListener, remove: globalThis.removeEventListener};
  try {
    globalThis.document = {...recoveryGlobals.document,
      createElement: name => new UploadElement(name),
      createElementNS: (_namespace, name) => new UploadElement(name)};
    globalThis.addEventListener = globalThis.removeEventListener = () => {};
    for (const action of ["send", "queue"]) for (const confirmation of ["retry", "observe", "unrelated", "thread", "draft", "attachments"]) {
      const recoveryStorage = new MemoryStorage();
      const files = [
        {id: "ca0e3d66-f923-4c79-bde5-c25c1edfd02b", clientId: "ca0e3d66-f923-4c79-bde5-c25c1edfd02b", name: "first.txt", size: 1, state: "ready"},
        {id: "cb0e3d66-f923-4c79-bde5-c25c1edfd02b", clientId: "cb0e3d66-f923-4c79-bde5-c25c1edfd02b", name: "second.txt", size: 1, state: "ready"},
      ];
      recoveryStorage.setItem("codex-web:uploads:/recovery-uploads", JSON.stringify(files));
      globalThis.fetch = async (_path, options = {}) => ({ok: true, json: async () => options.method === "DELETE" ? {} : {
        limits: {chunkBytes: 4, fileBytes: 100, promptBytes: 100, files: 10}, files,
      }});
      let recoveryEvents, recoveryThread = "original-thread", observedID = "", recoveryReads = 0;
      const recoveryCalls = [];
      class RecoveryEvents {
        constructor() { recoveryEvents = this; }
        addEventListener() {}
        close() {}
      }
      const dispatchRecovery = async (message, id, attachmentIds) => {
        recoveryCalls.push({message, id, attachmentIds});
        if (recoveryCalls.length === 1) throw Object.assign(new Error("Request failed (502)"), {status: 502});
        return action === "send" ? {turnId: "accepted", clientUserMessageId: id} : {id: "queued", clientUserMessageId: id};
      };
      const recoveryClient = {
        eventsPath: () => "/recovery-events",
        async thread() {
          recoveryReads++;
          return {threadId: recoveryThread, status: "active", entries: observedID ? [{kind: "userMessage", text: "original",
            clientUserMessageId: observedID, clientUserMessageDigest: "c".repeat(64)}] : []};
        },
        message: (message, id, _retry, attachmentIds) => dispatchRecovery(message, id, attachmentIds),
        queueMessage: dispatchRecovery,
        async acknowledgeMessages(entries) { return {acknowledgedClientUserMessageIds: entries.map(entry => entry.clientUserMessageId)}; },
      };
      const recoveryRoot = new UploadElement("main");
      const disposeRecovery = mountConversation(recoveryRoot, {id: "recovery", basePath: "/codex", client: recoveryClient,
        EventSource: RecoveryEvents, storage: recoveryStorage, uploadBasePath: "/recovery-uploads",
        randomUUID: () => "cc0e3d66-f923-4c79-bde5-c25c1edfd02b", refreshPolicy: {eventDebounceMs: 0},
        capabilities: {pending: false, queueRead: false, interrupt: false, settings: false, respond: false}});
      await new Promise(resolve => setImmediate(resolve));
      const recoveryTextarea = recoveryRoot.descendants().find(el => el.name === "textarea");
      const recoveryForm = recoveryRoot.descendants().find(el => el.name === "form");
      const recoveryQueue = recoveryRoot.descendants().find(el => el.textContent === "Queue");
      const recoveryNotice = recoveryRoot.descendants().find(el => el.attributes.class === "codex-submission-status");
      const submitRecovery = () => action === "send" ? recoveryForm.listeners.get("submit")({preventDefault() {}}) : recoveryQueue.listeners.get("click")();
      const retainedRecovery = createDurableSender({client: recoveryClient, id: "recovery", basePath: "/codex", storage: recoveryStorage});
      const pendingRecovery = () => action === "send" ? retainedRecovery.pending() : retainedRecovery.pendingQueue();
      const refreshRecovery = async () => {
        const reads = recoveryReads; recoveryEvents.onmessage();
        for (let index = 0; index < 50 && recoveryReads === reads; index++) await new Promise(resolve => setTimeout(resolve, 5));
        assert(recoveryReads > reads);
        await new Promise(resolve => setImmediate(resolve));
      };
      recoveryTextarea.value = "original";
      await submitRecovery();
      assert.equal(recoveryNotice.hidden, false, `${action}/${confirmation}: initial uncertainty`);
      const originalAttempt = pendingRecovery();
      assert.deepEqual(originalAttempt.attachmentIds, files.map(file => file.id));
      if (confirmation === "retry") {
        await submitRecovery();
        assert.equal(recoveryNotice.hidden, true, `${action}: matching acceptance clears notice`);
        assert.deepEqual(recoveryCalls.map(call => call.id), [originalAttempt.id, originalAttempt.id]);
        if (action === "send") assert(pendingRecovery().receipt, "accepted send receipt stays until observation");
        else assert.equal(pendingRecovery(), null);
      } else if (confirmation === "unrelated" || confirmation === "thread") {
        observedID = "unrelated-attempt";
        if (confirmation === "thread") recoveryThread = "other-thread";
        await refreshRecovery();
        if (confirmation === "thread") {
          assert.equal(recoveryNotice.hidden, true, "notice stays scoped to original thread");
          recoveryThread = "original-thread"; observedID = ""; await refreshRecovery();
        }
        assert.equal(recoveryNotice.hidden, false, "unrelated confirmation cannot retire original warning");
        assert.deepEqual(pendingRecovery(), originalAttempt);
        assert.equal(recoveryTextarea.value, "original");
      } else {
        if (confirmation === "draft") recoveryTextarea.value = "new draft";
        if (confirmation === "attachments") {
          await recoveryRoot.descendants().find(el => el.attributes["aria-label"] === "Remove first.txt").listeners.get("click")();
        }
        observedID = originalAttempt.id; await refreshRecovery();
        assert.equal(recoveryNotice.hidden, true, `${action}/${confirmation}: matching observation clears notice`);
        if (confirmation === "draft") assert.equal(recoveryTextarea.value, "new draft");
        if (confirmation === "attachments") {
          assert.equal(recoveryTextarea.value, "original", "changed attachments preserve current draft");
          assert.deepEqual(JSON.parse(recoveryStorage.getItem("codex-web:uploads:/recovery-uploads")).map(file => file.id), [files[1].id]);
        }
        if (action === "queue") assert.deepEqual(pendingRecovery(), originalAttempt, "visual observation never clears durable queue uncertainty");
        else assert.equal(pendingRecovery(), null);
      }
      disposeRecovery();
    }
  } finally {
    globalThis.document = recoveryGlobals.document;
    globalThis.fetch = recoveryGlobals.fetch;
    globalThis.addEventListener = recoveryGlobals.add;
    globalThis.removeEventListener = recoveryGlobals.remove;
  }

  // A late catalog must initialize an already-open editor, and stale reads
  // must not overwrite a settings acknowledgement.
  let releaseCatalog, releaseSnapshot, releaseSave, settingsWrites = 0, snapshots = 0;
  const settingsRoot = new FakeElement("main");
  const settingsClient = {
    eventsPath() {return "/settings-events";},
    async thread() {
      snapshots++;
      const pair = {threadId: "thread-settings", status: "idle", entries: [], model: "model-a", reasoningEffort: "medium"};
      if (snapshots > 1) return new Promise(resolve => {releaseSnapshot = () => resolve(pair);});
      return pair;
    },
    models() { return new Promise(resolve => {releaseCatalog = () => resolve([
      {model: "model-a", supportedReasoningEfforts: [{reasoningEffort: "medium"}]},
      {model: "model-b", supportedReasoningEfforts: [{reasoningEffort: "xhigh"}]},
    ]);}); },
    async modes() { return [{mode: "default"}]; },
    settings(model, reasoningEffort) {
      settingsWrites++;
      return new Promise(resolve => {releaseSave = () => resolve({model, reasoningEffort});});
    },
  };
  let settingsEvents;
  class SettingsEvents {
    constructor() { settingsEvents = this; }
    addEventListener() {}
    close() {}
  }
  const disposeSettings = mountConversation(settingsRoot, {EventSource: SettingsEvents, refreshPolicy: {eventDebounceMs: 0}, id: "settings", client: settingsClient,
    capabilities: {pending: false, queueRead: false, send: false, queue: false,
      interrupt: false, respond: false, eventStream: true}});
  await new Promise(resolve => setImmediate(resolve));
  const settingsControl = label => settingsRoot.descendants().find(el => el.attributes["aria-label"] === label);
  const editor = settingsControl("Codex settings");
  const edit = settingsRoot.descendants().find(el => el.textContent === "Edit");
  const settingsForm = editor.children[0];
  edit.listeners.get("click")();
  releaseCatalog(); await new Promise(resolve => setImmediate(resolve));
  assert.equal(settingsControl("Model").value, "model-a");
  assert.equal(settingsControl("Reasoning effort").value, "medium");
  settingsControl("Model").value = "model-b"; settingsControl("Model").listeners.get("change")();
  settingsControl("Reasoning effort").value = "xhigh";
  editor.close(); edit.listeners.get("click")();
  assert.equal(settingsControl("Model").value, "model-a"); assert.equal(settingsWrites, 0);
  settingsControl("Model").value = "model-b"; settingsControl("Model").listeners.get("change")();
  settingsControl("Reasoning effort").value = "xhigh";
  settingsEvents.onmessage(); await new Promise(resolve => setTimeout(resolve, 5));
  const saving = settingsForm.listeners.get("submit")({preventDefault() {}});
  let escapeCanceled = false;
  editor.listeners.get("cancel")({preventDefault() {escapeCanceled = true;}});
  assert.equal(escapeCanceled, true);
  releaseSave(); await new Promise(resolve => setImmediate(resolve));
  releaseSnapshot(); await saving;
  const settingsSummary = settingsRoot.descendants().find(el => el.textContent === "model-b · xhigh");
  assert(settingsSummary, "a stale snapshot must retain the acknowledged model pair");
  assert.equal(editor.open, false); assert.equal(settingsWrites, 1);
  disposeSettings();

  let releaseQueue, releasePending;
  const pageCalls = [];
  const pagedClient = {
    async thread() { throw new Error("unexpected legacy read"); },
    async threadPage({cursor} = {}) {
      pageCalls.push(cursor || "newest");
      return {threadId: "paged-thread", status: "idle", hasOlder: !cursor,
        olderCursor: cursor ? null : "older-1", entries: cursor ? [
          {turnId: "turn-1", itemId: "item-1", kind: "agentMessage", text: "Older"},
        ] : [
          {turnId: "turn-1", itemId: "item-2", kind: "agentMessage", text: "Recent"},
        ]};
    },
    pending: () => new Promise(resolve => { releasePending = resolve; }),
    queue: () => new Promise(resolve => { releaseQueue = resolve; }),
  };
  const pagedRoot = new FakeElement("main");
  const unmountPaged = mountConversation(pagedRoot, {
    id: "paged", client: pagedClient,
    capabilities: {send: false, queue: false, interrupt: false, settings: false,
      respond: false, eventStream: false},
  });
  await new Promise(resolve => setImmediate(resolve));
  const pagedEntries = () => pagedRoot.descendants().filter(element =>
    element.attributes.class?.startsWith("codex-entry "));
  assert.equal(pagedEntries().length, 1, "pending and queue reads cannot block the recent page");
  const recentNode = pagedEntries()[0];
  const olderButton = pagedRoot.descendants().find(element => element.textContent === "Load older");
  assert.equal(olderButton.hidden, false);
  olderButton.listeners.get("click")();
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(pageCalls, ["newest", "older-1"]);
  assert.equal(pagedEntries().length, 2);
  assert.equal(pagedEntries()[1], recentNode, "unchanged entries keep their DOM nodes");
  releasePending([]); releaseQueue([]);
  await new Promise(resolve => setImmediate(resolve));
  unmountPaged();

  // Retired control reads may settle after resume and after its replacement.
  const originalLaneDocument = globalThis.document;
  for (const outcome of ["resolve", "reject"]) {
    globalThis.document = Object.assign(new EventTarget(), originalLaneDocument, {hidden:false});
    const laneRoot = new FakeElement("main"), pendingCalls = [], queueCalls = [];
    const deferred = calls => ({signal} = {}) => new Promise((resolve, reject) => calls.push({resolve, reject, signal}));
    const disposeLane = mountConversation(laneRoot, {id:"lanes", client:{
      thread: async () => ({threadId:"lanes-thread",status:"idle",entries:[]}),
      pending:deferred(pendingCalls), queue:deferred(queueCalls),
    }, capabilities:{send:false,queue:false,interrupt:false,settings:false,respond:false,eventStream:false},
    refreshPolicy:{warningMs:0, eventDebounceMs:0}});
    await new Promise(resolve => setImmediate(resolve));
    globalThis.document.hidden = true; globalThis.document.dispatchEvent(new Event("visibilitychange"));
    assert(pendingCalls[0].signal.aborted && queueCalls[0].signal.aborted);
    globalThis.document.hidden = false; globalThis.document.dispatchEvent(new Event("visibilitychange"));
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(pendingCalls.length,2); assert.equal(queueCalls.length,2);
    const freshPending = {id:"fresh",params:{message:"fresh request"}};
    pendingCalls[1].resolve([freshPending]); queueCalls[1].resolve([{id:"fresh",text:"fresh queue"}]);
    await new Promise(resolve => setImmediate(resolve));
    // Reconnection may invalidate a read begun before its current snapshot.
    // Complete that one coalesced successor, never the retired pre-hide read.
    for (let index=2; index<pendingCalls.length; index++) pendingCalls[index].resolve([freshPending]);
    for (let index=2; index<queueCalls.length; index++) queueCalls[index].resolve([{id:"fresh",text:"fresh queue"}]);
    await new Promise(resolve => setImmediate(resolve));
    if (outcome === "resolve") {
      pendingCalls[0].resolve([{id:"old",params:{message:"old request"}}]); queueCalls[0].resolve([{id:"old",text:"old queue"}]);
    } else {
      pendingCalls[0].reject(Error("retired failure")); queueCalls[0].reject(Error("retired failure"));
    }
    await new Promise(resolve => setImmediate(resolve));
    const texts = laneRoot.descendants().map(element => element.textContent).join(" ");
    assert.match(texts,/fresh request/); assert.match(texts,/fresh queue/);
    assert.doesNotMatch(texts,/old request|old queue|could not be refreshed|retired failure/);
    disposeLane();
  }
  globalThis.document = originalLaneDocument;

  // Typed invalidations bypass the success floor; output alone does not.
  let controlEvents, controlPending = 0, controlSnapshots = 0, holdControl = false;
  const controlReads = [];
  class ControlEvents {
    listeners = new Map();
    constructor() {controlEvents = this;}
    addEventListener(name, callback) {this.listeners.set(name,callback);}
    close() {}
  }
  const controlRoot = new FakeElement("main");
  const disposeControl = mountConversation(controlRoot,{id:"control-hints",client:{
    eventsPath:()=>"/control-events",
    thread:async()=>{controlSnapshots++;return {threadId:"control-thread",status:"idle",entries:[]};},
    pending:async()=>{controlPending++;return holdControl ? new Promise(resolve=>controlReads.push(resolve)) : [];},
  },EventSource:ControlEvents,capabilities:{send:false,queueRead:false,queue:false,interrupt:false,settings:false,respond:false},
  refreshPolicy:{eventDebounceMs:0}});
  controlEvents.onopen(); controlEvents.listeners.get("ready")({data:'{"heartbeatIntervalMs":20000,"pendingInvalidation":true}'});
  await new Promise(resolve=>setTimeout(resolve,5));
  const beforeOutput = controlPending, wallNow = Date.now;
  try {
    for(let index=0;index<200;index++) {
      Date.now = () => wallNow() + index * 500;
      controlEvents.onmessage(); await new Promise(resolve=>setTimeout(resolve,1));
    }
  } finally { Date.now = wallNow; }
  assert(controlSnapshots>1);
  assert.equal(controlPending,beforeOutput+6,"two hundred output hints over 100 seconds respect the 15-second success floor");
  controlEvents.listeners.get("pending")(); await new Promise(resolve=>setImmediate(resolve));
  assert.equal(controlPending,beforeOutput+7,"real control invalidation must bypass the floor");
  holdControl = true;
  controlEvents.listeners.get("pending")(); await new Promise(resolve=>setImmediate(resolve));
  for(let index=0;index<3;index++) controlEvents.listeners.get("pending")();
  assert.equal(controlReads.length,1,"invalidation cannot overlap a current control read");
  controlReads[0]([{id:"obsolete",params:{text:"obsolete control"}}]);
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(controlReads.length,2,"invalidations coalesce into one successor");
  controlReads[1]([{id:"current",params:{text:"current control"}}]);
  await new Promise(resolve=>setImmediate(resolve));
  const controlText = controlRoot.descendants().map(element=>element.textContent).join(" ");
  assert.match(controlText,/current control/); assert.doesNotMatch(controlText,/obsolete control/);
  disposeControl();

  // A forced failure retires successful freshness; the actual retry timer
  // must read again after two seconds, then restore the success floor.
  const pendingClockDocument = globalThis.document, pendingClockNow = Date.now;
  const pendingClockLater = globalThis.setTimeout, pendingClockClear = globalThis.clearTimeout;
  let pendingAt = 100_000, pendingTimerID = 0, retryEvents, failPending = false, pendingAttempts = 0;
  const pendingTimers = new Map(), retryRoot = new FakeElement("main");
  const settlePending = () => new Promise(resolve => setImmediate(resolve));
  const advancePending = async ms => {
    const end = pendingAt + ms;
    for (;;) {
      const next = [...pendingTimers].sort((a,b) => a[1].at - b[1].at)[0];
      if (!next || next[1].at > end) break;
      pendingAt = Math.max(pendingAt, next[1].at);
      pendingTimers.delete(next[0]); next[1].callback(); await settlePending();
    }
    pendingAt = end; await settlePending();
  };
  let disposeRetry;
  try {
    globalThis.document = Object.assign(new EventTarget(), pendingClockDocument, {hidden:false});
    Date.now = () => pendingAt;
    globalThis.setTimeout = (callback, ms) => {
      const id = ++pendingTimerID; pendingTimers.set(id, {callback, at: pendingAt + ms}); return id;
    };
    globalThis.clearTimeout = id => pendingTimers.delete(id);
    class RetryEvents {
      listeners = new Map();
      constructor() { retryEvents = this; }
      addEventListener(name, callback) { this.listeners.set(name, callback); }
      close() {}
    }
    disposeRetry = mountConversation(retryRoot, {id:"pending-retry", client:{
      eventsPath:()=>"/pending-retry-events",
      thread:async()=>({threadId:"pending-retry-thread",status:"idle",entries:[]}),
      pending:async()=>{
        pendingAttempts++;
        if (failPending) { failPending = false; throw Object.assign(Error("unavailable"), {status:503}); }
        return [{id:"retained-pending",params:{message:"retained prompt"}}];
      },
    }, EventSource:RetryEvents,
    capabilities:{send:false,queueRead:false,queue:false,interrupt:false,settings:false,respond:false},
    refreshPolicy:{eventDebounceMs:0}});
    retryEvents.onopen();
    retryEvents.listeners.get("ready")({data:'{"heartbeatIntervalMs":20000,"pendingInvalidation":true}'});
    await advancePending(0);
    const successfulReads = pendingAttempts;
    assert(successfulReads > 0);
    await advancePending(100);
    failPending = true; retryEvents.listeners.get("pending")(); await settlePending();
    assert.equal(pendingAttempts, successfulReads + 1);
    assert.match(retryRoot.descendants().map(el=>el.textContent).join(" "), /retained prompt/);
    await advancePending(1999);
    assert.equal(pendingAttempts, successfulReads + 1, "failure backoff cannot dispatch early");
    await advancePending(1);
    assert.equal(pendingAttempts, successfulReads + 2, "the real two-second retry must bypass old successful freshness");
    assert.equal(pendingAt, 102_100);
    retryEvents.onmessage(); await advancePending(0);
    await advancePending(14_999);
    assert.equal(pendingAttempts, successfulReads + 2, "retry success restores the full success freshness floor");
    await advancePending(1);
    assert.equal(pendingAttempts, successfulReads + 3, "the due timer reads when successful freshness expires");
  } finally {
    disposeRetry?.();
    globalThis.document = pendingClockDocument; Date.now = pendingClockNow;
    globalThis.setTimeout = pendingClockLater; globalThis.clearTimeout = pendingClockClear;
  }

  const timeoutRoot = new FakeElement("main");
  const disposeTimeout = mountConversation(timeoutRoot,{id:"timeouts",client:{
    thread:async()=>({threadId:"timeout-thread",status:"idle",entries:[]}),
    pending:()=>new Promise(()=>{}),queue:()=>new Promise(()=>{}),
  },capabilities:{send:false,queue:false,interrupt:false,settings:false,respond:false,eventStream:false},
  refreshPolicy:{pendingDeadlineMs:5,queueDeadlineMs:5,warningMs:0,retryInitialMs:1000,eventDebounceMs:0}});
  await new Promise(resolve=>setTimeout(resolve,40));
  const timeoutText = timeoutRoot.descendants().map(element=>element.textContent).join(" ");
  assert.match(timeoutText,/Requests could not be refreshed/); assert.match(timeoutText,/Queued messages could not be refreshed/);
  disposeTimeout();

  // A failed automatic repair must restart after hiding during its backoff.
  const plainDocument = globalThis.document;
  globalThis.document = Object.assign(new EventTarget(), plainDocument, {hidden:false});
  let repairEvents, newestReads = 0, repairReads = 0;
  class RepairEvents {
    constructor() {repairEvents = this;}
    addEventListener() {}
    close() {}
  }
  const repairEntry = index => ({turnId:"repair-turn", itemId:`item-${index}`, kind:"agentMessage", text:`Entry ${index}`});
  const repairClient = {
    eventsPath:() => "/repair-events",
    async threadPage({cursor} = {}) {
      if (cursor) {
        repairReads++;
        if (repairReads === 1) throw Error("Temporary history failure");
        return {threadId:"repair-thread", status:"idle", entries:[repairEntry(1), repairEntry(2)], hasOlder:false};
      }
      newestReads++;
      return {threadId:"repair-thread", status:"idle", entries:[repairEntry(newestReads === 1 ? 1 : 3)],
        hasOlder:true, olderCursor:"repair-cursor"};
    },
  };
  const repairRoot = new FakeElement("main");
  const disposeRepair = mountConversation(repairRoot, {id:"repair", client:repairClient, EventSource:RepairEvents,
    refreshPolicy:{eventDebounceMs:0, historyPageMs:1, retryInitialMs:40, retryMaxMs:80},
    capabilities:{pending:false, queueRead:false, settings:false, send:false, queue:false, interrupt:false, respond:false, eventStream:true}});
  const until = async check => {
    for (let count=0; count<100 && !check(); count++) await new Promise(resolve => setTimeout(resolve,5));
    assert(check());
  };
  await until(() => newestReads === 1);
  repairEvents.onmessage(); await until(() => repairReads === 1);
  globalThis.document.hidden = true; globalThis.document.dispatchEvent(new Event("visibilitychange"));
  await new Promise(resolve => setTimeout(resolve,100)); assert.equal(repairReads,1);
  globalThis.document.hidden = false; globalThis.document.dispatchEvent(new Event("visibilitychange"));
  await until(() => repairReads === 2);
  assert.equal(repairRoot.descendants().filter(el => el.attributes.class?.startsWith("codex-entry ")).length,3);
  disposeRepair(); globalThis.document = plainDocument;

  // A retired history transport may settle after a resumed repair has started.
  // Neither settlement may replace rows, invalidate cursors or clear that repair.
  for (const oldFailure of [false, true]) {
    globalThis.document = Object.assign(new EventTarget(), plainDocument, {hidden:false});
    let historyEvents, newestHistoryReads = 0;
    const heldHistory = [];
    class HistoryEvents {
      constructor() {historyEvents = this;}
      addEventListener() {}
      close() {}
    }
    const historyRoot = new FakeElement("main");
    const disposeHistory = mountConversation(historyRoot, {id:`late-history-${oldFailure}`, EventSource:HistoryEvents,
      client:{eventsPath:()=>"/history-events", threadPage:({cursor, signal} = {}) => {
        if (cursor) return new Promise((resolve,reject) => heldHistory.push({cursor,signal,resolve,reject}));
        newestHistoryReads++;
        return Promise.resolve({threadId:"repair-thread",status:"idle",entries:[repairEntry(newestHistoryReads === 1 ? 1 : 3)],
          hasOlder:true,olderCursor:"repair-cursor"});
      }},refreshPolicy:{eventDebounceMs:0,historyPageMs:1,retryInitialMs:40,warningMs:0},
      storage:new MemoryStorage(),conversationPath:`/history/${oldFailure}`,
      capabilities:{pending:false,queueRead:false,settings:false,send:true,queue:false,interrupt:false,respond:false,eventStream:true}});
    await until(()=>newestHistoryReads === 1);
    const historyDraft = historyRoot.descendants().find(el=>el.name === "textarea");
    historyDraft.value = "Retained draft";
    historyEvents.onmessage(); await until(()=>heldHistory.length === 1);
    const historyList = historyRoot.descendants().find(el=>el.name === "ol");
    const retainedRows = [...historyList.children];
    globalThis.document.hidden = true; globalThis.document.dispatchEvent(new Event("visibilitychange"));
    assert.equal(heldHistory[0].signal.aborted,true);
    globalThis.document.hidden = false; globalThis.document.dispatchEvent(new Event("visibilitychange"));
    await until(()=>heldHistory.length === 2);
    if (oldFailure) heldHistory[0].reject(Object.assign(Error("Old cursor expired"),{code:"transcript_cursor_expired"}));
    else heldHistory[0].resolve({threadId:"repair-thread",status:"idle",entries:[repairEntry(99)],hasOlder:false});
    await new Promise(resolve=>setImmediate(resolve));
    assert.deepEqual(historyList.children,retainedRows,"retired history cannot replace retained row nodes");
    assert.equal(historyDraft.value,"Retained draft");
    assert.equal(historyRoot.descendants().find(el=>el.attributes.class === "codex-history-controls").children[1].textContent,"");
    assert.equal(heldHistory.length,2,"old completion cannot schedule another repair or clear its successor");
    assert.equal(heldHistory[1].signal.aborted,false);
    assert.equal(heldHistory[1].cursor,"repair-cursor","resume preserves paging authority");
    heldHistory[1].resolve({threadId:"repair-thread",status:"idle",entries:[repairEntry(1),repairEntry(2)],hasOlder:false});
    await until(()=>historyList.children.length === 3);
    assert.doesNotMatch(historyList.descendants().map(el=>el.textContent).join(" "),/Entry 99/);
    assert.equal(historyDraft.value,"Retained draft");
    disposeHistory(); globalThis.document = plainDocument;
  }

  const promptRoot = new FakeElement("main");
  const promptActions = [];
  const promptClient = {
    async respond(id, payload) { promptActions.push({id, ...payload}); },
    async snooze(id, token) { promptActions.push({id, token, snooze: true}); },
    async thread() { return {threadId: "thread-1", entries: [], status: "idle"}; },
    async pending() {
      return [
        {id: "untimed", token: "offer-untimed", kind: "userInput", item: {}},
        {id: "timed", token: "offer-timed", kind: "userInput", item: {}, autoResolutionAtMs: Date.now() + 1000},
        {id: "approval", token: "offer-approval", kind: "command", item: {}, availableDecisions: ["accept"]},
      ];
    },
  };
  const unmountPrompts = mountConversation(promptRoot, {
    id: "opaque", client: promptClient,
    capabilities: {
      pending: true, queueRead: false, send: false, queue: false,
      interrupt: false, settings: false, respond: true, eventStream: false,
    },
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(promptRoot.descendants().filter((element) => element.textContent === "Snooze").length, 1);
  const findPromptControl = text => promptRoot.descendants().find(element => element.textContent === text);
  promptRoot.descendants().find(element => element.name === "textarea").value = '{"choice":{"answers":["First"]}}';
  findPromptControl("Submit answers").listeners.get("click")();
  findPromptControl("Snooze").listeners.get("click")();
  findPromptControl("accept").listeners.get("click")();
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(promptActions, [
    {id: "untimed", token: "offer-untimed", answers: {choice: {answers: ["First"]}}},
    {id: "timed", token: "offer-timed", snooze: true},
    {id: "approval", token: "offer-approval", decision: "accept"},
  ]);
  unmountPrompts();

  const mountedCoupledStorage = new MemoryStorage();
  const mountedCoupledClient = createConversationClient({
    id: "opaque", conversationPath: "/api/mounted/opaque", fetch: async (path) => ({
      ok: !path.endsWith("/thread/page"),
      status: path.endsWith("/thread/page") ? 501 : 200,
      json: async () => path.endsWith("/thread/page") ? {code: "transcript_paging_unavailable"} :
        path.endsWith("/thread") ? {threadId: "thread-1", entries: [], status: "idle"} : {},
    }),
  });
  await createDurableSender({
    client: mountedCoupledClient,
    storage: mountedCoupledStorage,
    randomUUID: () => "8b0e3d66-f923-4c79-bde5-c25c1edfd02b",
  }).send("mounted pending");
  const mountedCoupledRoot = new FakeElement("main");
  const unmountCoupled = mountConversation(mountedCoupledRoot, {
    id: "opaque", client: mountedCoupledClient, storage: mountedCoupledStorage,
    capabilities: {
      pending: false, queueRead: false, queue: false, interrupt: false,
      settings: false, respond: false, eventStream: false,
    },
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(mountedCoupledRoot.descendants().find((element) => (
    element.name === "textarea"
  )).value, "mounted pending");
  unmountCoupled();

  // Sending during an older refresh requires the dirty follow-up to clear
  // acknowledged text, while edits made during acknowledgement must survive.
  for (const editDuringAcknowledgement of [false, true]) {
    let firstRead, acknowledgeEntered, releaseAcknowledgement;
    const acknowledgementStarted = new Promise(resolve => { acknowledgeEntered = resolve; });
    let sentID, readCount = 0;
    const raceClient = {
      thread() {
        if (++readCount === 1) return new Promise(resolve => { firstRead = resolve; });
        return Promise.resolve({threadId: "thread-1", status: "idle", entries: [{kind: "userMessage", text: "race message",
          clientUserMessageId: sentID, clientUserMessageDigest: "a".repeat(64)}]});
      },
      async message(message, id) { sentID = id; return {clientUserMessageId: id}; },
      async acknowledgeMessages() {
        acknowledgeEntered();
        await new Promise(resolve => { releaseAcknowledgement = resolve; });
        return {acknowledgedClientUserMessageIds: [sentID]};
      },
    };
    const raceRoot = new FakeElement("main");
    const dispose = mountConversation(raceRoot, {
      id: "race", basePath: "/codex", client: raceClient, storage: new MemoryStorage(),
      randomUUID: () => "8b0e3d66-f923-4c79-bde5-c25c1edfd02b",
      capabilities: {pending: false, queueRead: false, queue: false,
        interrupt: false, settings: false, respond: false, eventStream: false},
    });
    await new Promise(resolve => setImmediate(resolve));
    const textarea = raceRoot.descendants().find(element => element.name === "textarea");
    const form = raceRoot.descendants().find(element => element.attributes.class === "codex-conversation-form");
    textarea.value = "race message";
    const submitted = form.listeners.get("submit")({preventDefault() {}});
    await new Promise(resolve => setImmediate(resolve));
    firstRead({threadId: "thread-1", status: "idle", entries: []});
    await submitted;
    assert.equal(textarea.value, "race message");
    await acknowledgementStarted;
    if (editDuringAcknowledgement) textarea.value = "race message ";
    releaseAcknowledgement();
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(textarea.value, editDuringAcknowledgement ? "race message " : "");
    dispose();
  }

  assert.throws(() => mountConversation(new FakeElement("main"), {
    id: "opaque", client: mountedClient, basePath: "/codex", EventSource: null,
  }), /Durable browser storage is unavailable/);
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

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
    formatTranscriptTimestamp, transcriptEntryCopyText, createTranscriptCopyButton,
  } = await import(
    "../conversation/assets/conversation.js"
  );
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
  assert.equal(JSON.parse(requests[5].options.body).retry, true);
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
    append(...children) { this.children.push(...children); }
    replaceChildren(...children) {
      this.children = children;
      if (this.name === "select" && children[0] && !this.value) {
        this.value = children[0].attributes.value || "";
      }
    }
    addEventListener(name, listener) { this.listeners.set(name, listener); }
    descendants() { return [this, ...this.children.flatMap((child) => child.descendants())]; }
  }
  globalThis.Element = FakeElement;
  globalThis.document = {createElement: (name) => new FakeElement(name)};
  const copied = [];
  const clipboard = {async writeText(text) { copied.push(text); }};
  Object.defineProperty(globalThis, "navigator", {value: {clipboard}, configurable: true});
  const streamingEntry = {kind: "agentMessage", text: "First streamed text"};
  const copyButton = createTranscriptCopyButton(streamingEntry);
  assert.equal(copyButton.name, "button");
  assert.equal(copyButton.attributes.type, "button");
  assert.equal(copyButton.attributes["aria-label"], "Copy message");
  assert.equal(copyButton.textContent, "Copy");
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
    assert.equal(copyButton.textContent, "Copied");
    assert.equal(copyButton.attributes["data-copy-state"], "copied");
    scheduledFeedback.pop()();
    assert.equal(copyButton.textContent, "Copy");
    assert.equal(copyButton.attributes["aria-label"], "Copy message");
    streamingEntry.text += " with the latest chunk";
    await copyButton.listeners.get("click")();
    assert.equal(copied[1], "First streamed text with the latest chunk");
    clipboard.writeText = async () => { throw new Error("permission denied"); };
    await copyButton.listeners.get("click")();
    assert.equal(copyButton.textContent, "Copy failed");
    assert.equal(copyButton.attributes["data-copy-state"], "error");
    assert.equal(copyButton.attributes.title, "Copy failed. Try again.");
    assert.equal(copyButton.disabled, false);
    delete globalThis.navigator.clipboard;
    await copyButton.listeners.get("click")();
    assert.equal(copyButton.textContent, "Copy failed");
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
    assert.equal(footer.children[0].attributes.class, "codex-entry-copy");
    assert.equal(footer.children[1], mountedTimes[index]);
    const item = root.descendants().find((element) => element.children.includes(footer));
    assert.equal(item.children.at(-1), footer);
  });
  assert.equal(root.descendants().filter((element) => element.attributes.class === "codex-conversation-date").length, 2);
  unmount();

  const promptRoot = new FakeElement("main");
  const promptClient = {
    async thread() { return {entries: [], status: "idle"}; },
    async pending() {
      return [
        {id: "untimed", kind: "userInput", item: {}},
        {id: "timed", kind: "userInput", item: {}, autoResolutionAtMs: Date.now() + 1000},
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
  unmountPrompts();

  const mountedCoupledStorage = new MemoryStorage();
  const mountedCoupledClient = createConversationClient({
    id: "opaque", conversationPath: "/api/mounted/opaque", fetch: async (path) => ({
      ok: true,
      status: 200,
      json: async () => path.endsWith("/thread") ? {entries: [], status: "idle"} : {},
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

  assert.throws(() => mountConversation(new FakeElement("main"), {
    id: "opaque", client: mountedClient, basePath: "/codex", EventSource: null,
  }), /Durable browser storage is unavailable/);
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

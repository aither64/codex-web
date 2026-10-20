const assert = require("node:assert/strict");
const {test} = require("node:test");
const load = () => import("../conversation/assets/questions.js");
const q = (item, index = 0) => ({id: JSON.stringify(["request_user_input_async", item, index]), title: "Which?", options: ["One", "Two"]});
const agent = (item, turn = "turn") => ({kind: "agentMessage", itemId: item, turnId: turn, turnStatus: "inProgress", questions: [q(item), q(item, 1)]});
const snapshot = entries => ({threadId: "thread", latestTurnId: "turn", status: "active", entries});
class Storage {
  values = new Map();
  getItem(key) { return this.values.get(key) || null; }
  setItem(key, value) { this.values.set(key, value); }
}

test("untouched drafts select the recommendation or first option without replacing saved answers", async () => {
  const {createAsyncQuestions} = await load();
  const cases = [
    {options: ["One", "Two (Recommended)"], expected: "Two (Recommended)"},
    {options: ["One", "Two"], expected: "One"},
    {options: ["One"], saved: {draft: "", choice: "", note: ""}, expected: ""},
    {options: ["One"], saved: {draft: "Custom answer"}, expected: "Custom answer"},
    {options: ["One"], saved: {draft: "", attempt: {id: "pending", answer: "Two", message: "Exact envelope"}}, expected: ""},
  ];
  for (const {options, saved, expected} of cases) {
    const storage = new Storage(), question = {...q("source"), options};
    if (saved) storage.setItem(`codex-web:questions:conversation:thread:${encodeURIComponent(question.id)}`,
      JSON.stringify({schema: 1, ...saved}));
    const config = {storage, namespace: "conversation", send: async () => {}};
    let controller = createAsyncQuestions(config);
    const page = snapshot([{...agent("source"), questions: [question]}]);
    controller.update(page);
    assert.equal(controller.state(question.id).draft, expected);
    controller = createAsyncQuestions(config); controller.update(page);
    assert.equal(controller.state(question.id).draft, expected, "reload retains the same draft");
    if (saved?.attempt) assert.deepEqual(controller.state(question.id).attempt, saved.attempt);
  }
});

test("repeated ID-less user messages do not match an absent question attempt", async () => {
  const {createAsyncQuestions} = await load();
  const storage = new Storage(), acknowledgements = [];
  const options = {storage, namespace: "conversation", send: async () => {},
    acknowledge: async rows => { acknowledgements.push(...rows); return {acknowledgedClientUserMessageIds: rows.map(row => row.clientUserMessageId)}; }};
  const initial = {kind: "userMessage", text: "Initial task", clientUserMessageDigest: "a".repeat(64)};
  let controller = createAsyncQuestions(options);
  const entries = [initial, agent("source")];
  controller.update(snapshot(entries));
  controller.update(snapshot(entries));
  controller.update(snapshot([{...initial, clientUserMessageId: ""}, agent("source")]));
  controller.update(snapshot([...entries, {kind: "userMessage", clientUserMessageId: "ordinary", clientUserMessageDigest: "b".repeat(64)}]));
  assert.equal(controller.state(q("source").id).answer, undefined);
  assert.deepEqual(acknowledgements, []);
  storage.setItem(`codex-web:questions:conversation:thread:${encodeURIComponent(q("source").id)}`,
    JSON.stringify({schema: 1, draft: "Two", attempt: {id: "question-answer", answer: "Two", message: "Exact saved envelope"}}));
  controller = createAsyncQuestions(options);
  controller.update(snapshot([...entries, {kind: "userMessage", clientUserMessageId: "question-answer", clientUserMessageDigest: "c".repeat(64)}]));
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(controller.state(q("source").id).answer, "Two");
  assert.equal(controller.state(q("source").id).attempt, undefined);
  assert.deepEqual(acknowledgements, [{clientUserMessageId: "question-answer", digest: "c".repeat(64)}]);
});

test("native reply envelope retains identity and bounds UTF-8 framing", async () => {
  const {asyncQuestionReply} = await load();
  const text = asyncQuestionReply({...q("source", 1), title: "界".repeat(300) + "\n"}, "A\nB");
  const [reply] = JSON.parse(text.split("\n")[1]);
  assert.equal(reply.questionItemId, q("source", 1).id);
  assert.equal(reply.question, "界".repeat(170));
  assert.equal(reply.answer, "A\nB");
});

test("pin lifetime, dismissal, reload, late reply and duplicate titles", async () => {
  const {createAsyncQuestions} = await load();
  const storage = new Storage(), sends = [];
  const options = {storage, namespace: "conversation", send: async value => { sends.push(value); }, randomUUID: () => "attempt"};
  let controller = createAsyncQuestions(options);
  controller.update(snapshot([agent("source")]));
  assert.equal(controller.pending().length, 2);
  controller.ordinaryAccepted();
  controller = createAsyncQuestions(options);
  controller.update(snapshot([agent("source")]));
  assert.deepEqual(controller.pending(), []);
  await controller.sendAnswer(q("source").id, "Two");
  assert.equal(sends.length, 1);
  assert.equal(controller.state(q("source").id).answer, "Two");
  controller.update(snapshot([agent("new"), {kind: "userMessage", text: "Ordinary follow-up"}, agent("next")]));
  assert.deepEqual(controller.pending(), [q("next").id, q("next", 1).id]);
  controller.update({...snapshot([{...agent("next"), turnStatus: "completed"}]), status: "idle"});
  assert.deepEqual(controller.pending(), []);
});

test("uncertain send preserves retry identity and blocks a different answer", async () => {
  const {createAsyncQuestions} = await load();
  const storage = new Storage(), sends = [];
  const options = {storage, namespace: "conversation", randomUUID: () => "same-id", send: async value => {
    sends.push(value); if (sends.length === 1) throw new Error("Transport interrupted");
  }};
  let controller = createAsyncQuestions(options);
  controller.update(snapshot([agent("source")]));
  await controller.sendAnswer(q("source").id, "One");
  assert.equal(controller.state(q("source").id).answer, undefined);
  await controller.sendAnswer(q("source").id, "Two");
  assert.equal(sends.length, 1);
  controller = createAsyncQuestions(options); controller.update(snapshot([agent("source")]));
  await controller.sendAnswer(q("source").id, "One");
  assert.equal(sends[1].id, sends[0].id);
  assert.equal(sends[1].message, sends[0].message);
  assert.equal(sends[1].retry, true);
  assert.equal(controller.state(q("source").id).answer, "One");
  assert.deepEqual(controller.pending(), [q("source", 1).id]);
});

test("other-client replies reconcile across paging and thread boundaries", async () => {
  const {createAsyncQuestions} = await load();
  const storage = new Storage();
  const controller = createAsyncQuestions({storage, namespace: "conversation", send: async () => {}});
  const reply = {kind: "userMessage", questionReplies: [{questionItemId: q("source", 1).id, question: "Which?", answer: "Other client"}]};
  controller.update(snapshot([reply]));
  controller.update(snapshot([agent("source"), reply]));
  assert.equal(controller.state(q("source", 1).id).answer, "Other client");
  assert.deepEqual(controller.pending(), [q("source").id]);
  controller.update({...snapshot([agent("source")]), threadId: "another"});
  assert.equal(controller.state(q("source", 1).id).answer, undefined);
  assert.equal(controller.pending().length, 2);
});

test("unavailable durable storage prevents submission", async () => {
  const {createAsyncQuestions} = await load();
  let sends = 0;
  const controller = createAsyncQuestions({namespace: "conversation", randomUUID: () => "id", send: async () => { sends++; }});
  controller.update(snapshot([agent("source")]));
  await controller.sendAnswer(q("source").id, "One");
  assert.equal(sends, 0);
  assert.equal(controller.state(q("source").id).answer, undefined);
  assert.equal(controller.state(q("source").id).attempt, undefined);
});

test("idle during an unfinished turn preserves questions through activity and reload", async () => {
  const {createAsyncQuestions} = await load();
  const options = {storage: new Storage(), namespace: "conversation", send: async () => {}};
  let controller = createAsyncQuestions(options);
  controller.update(snapshot([agent("source")]));
  controller.update({...snapshot([agent("source")]), status: "idle"});
  assert.equal(controller.pending().length, 2);
  controller = createAsyncQuestions(options);
  controller.update(snapshot([agent("source")]));
  assert.equal(controller.pending().length, 2);
  controller.update({...snapshot([{...agent("source"), turnStatus: "completed"}]), status: "idle"});
  assert.deepEqual(controller.pending(), []);
});

test("oversized encoded replies and validation rejections remain editable", async () => {
  const {createAsyncQuestions} = await load();
  const sends = [];
  const options = {storage: new Storage(), namespace: "conversation", randomUUID: () => "id", maxMessageBytes: 300,
    send: async value => { sends.push(value); if (sends.length === 1) { const error = new Error("Invalid message"); error.status = 400; throw error; } }};
  let controller = createAsyncQuestions(options);
  controller.update(snapshot([agent("source")]));
  await controller.sendAnswer(q("source").id, "界".repeat(200));
  assert.equal(sends.length, 0);
  assert.equal(controller.state(q("source").id).attempt, undefined);
  await controller.sendAnswer(q("source").id, "One");
  assert.equal(controller.state(q("source").id).attempt, undefined);
  controller = createAsyncQuestions(options); controller.update(snapshot([agent("source")]));
  await controller.sendAnswer(q("source").id, "Two");
  assert.equal(sends.length, 2);
  assert.equal(controller.state(q("source").id).answer, "Two");
});

test("local persistence failure can be corrected before any network effect", async () => {
  const {createAsyncQuestions} = await load();
  const storage = new Storage(), sends = [], original = storage.setItem;
  const controller = createAsyncQuestions({storage, namespace: "conversation", randomUUID: () => "id", send: async value => sends.push(value)});
  controller.update(snapshot([agent("source")]));
  storage.setItem = () => { throw new Error("Storage full"); };
  await controller.sendAnswer(q("source").id, "One");
  assert.equal(sends.length, 0);
  assert.equal(controller.state(q("source").id).attempt, undefined);
  storage.setItem = original;
  await controller.sendAnswer(q("source").id, "Two");
  assert.equal(sends.length, 1);
});

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

class QuestionElement extends EventTarget {
  constructor(tag) { super(); this.tag=tag;this.children=[];this.dataset={};this.attributes={};this.value=""; }
  setAttribute(name,value) { this.attributes[name]=String(value); }
  removeAttribute(name) { delete this.attributes[name]; }
  append(...nodes) { for(const node of nodes){node.remove();node.parentElement=this;this.children.push(node);} }
  replaceChildren(...nodes) { for(const node of this.children)node.parentElement=null;this.children=[];this.append(...nodes); }
  insertBefore(node,before) { node.remove();node.parentElement=this;const index=this.children.indexOf(before);this.children.splice(index<0?this.children.length:index,0,node); }
  remove() { if(this.parentElement)this.parentElement.children=this.parentElement.children.filter(node=>node!==this);this.parentElement=null; }
  contains(node) { return this===node || this.children.some(child=>child.contains(node)); }
  closest(selector) { return this.className===selector.slice(1)?this:this.parentElement?.closest(selector); }
  descendants() { return [this,...this.children.flatMap(node=>node.descendants())]; }
  querySelector(selector) {
    if(selector==='section:not([hidden]) .codex-question-title')return this.children.find(node=>node.tag==='section'&&!node.hidden)?.querySelector('.codex-question-title');
    return this.descendants().find(node=>node.className===selector.slice(1));
  }
  focus() { globalThis.document.activeElement=this; }
  click() { if(!this.disabled)this.dispatchEvent(new Event('click')); }
}
const key = (input,extra={}) => {
  const event=new Event('keydown',{cancelable:true});Object.assign(event,{key:'Enter',...extra});input.dispatchEvent(event);return event;
};
const settled = async () => { for(let i=0;i<12;i++)await Promise.resolve(); };

test('question textarea keys preserve composition, modified shortcuts, held keys and control guards',async()=>{
  const {bindQuestionAnswerKeyboard}=await load();const input=new QuestionElement('textarea'),submit=new QuestionElement('button');let calls=0;
  submit.addEventListener('click',()=>calls++);bindQuestionAnswerKeyboard(input,submit);
  input.value='Selected answer';input.selectionStart=2;input.selectionEnd=8;
  for(const extra of [{key:'a'},{shiftKey:true},{altKey:true},{ctrlKey:true},{metaKey:true},{isComposing:true},{keyCode:229}])assert.equal(key(input,extra).defaultPrevented,false);
  input.dispatchEvent(new Event('compositionstart'));assert.equal(key(input).defaultPrevented,false);input.dispatchEvent(new Event('compositionend'));
  assert.equal(calls,0);assert.equal(key(input,{repeat:true}).defaultPrevented,true);assert.equal(calls,0);
  for(const [control,property] of [[input,'disabled'],[input,'readOnly'],[input,'hidden'],[submit,'disabled'],[submit,'hidden']]){
    control[property]=true;key(input);assert.equal(calls,0);control[property]=false;
  }
  const prevented=new Event('keydown',{cancelable:true});Object.assign(prevented,{key:'Enter'});prevented.preventDefault();input.dispatchEvent(prevented);assert.equal(calls,0);
  assert.equal(key(input).defaultPrevented,true);assert.equal(calls,1);assert.equal(input.value,'Selected answer');assert.equal(input.selectionStart,2);assert.equal(input.selectionEnd,8);
  submit.click();assert.equal(calls,2,'mouse and keyboard share the owning action');
});

test('inline and pinned Enter submit the existing optional answer once, preserving other questions and unknown recovery',async t=>{
  const originalDocument=globalThis.document;t.after(()=>{globalThis.document=originalDocument;});
  globalThis.document={createElement:tag=>new QuestionElement(tag),body:new QuestionElement('body')};
  const {createAsyncQuestions}=await load();
  for(const pinned of [false,true]) {
    let release;const sends=[];const source=agent('keyboard-'+pinned),container=new QuestionElement('aside');
    const controller=createAsyncQuestions({storage:new Storage(),namespace:'keyboard',randomUUID:()=> 'original-uuid',onChange:()=>{if(pinned)controller.renderPinned(container);},send:value=>{sends.push(value);return new Promise(resolve=>{release=resolve;});}});
    controller.update(snapshot([source]));
    if(pinned)container.children[0].click();else container.append(controller.inline(source));
    const first=container.descendants().find(node=>node.className==='codex-async-question'),input=first.descendants().find(node=>node.tag==='textarea');
    const radio=first.descendants().find(node=>node.type==='radio');key(radio);assert.equal(sends.length,0,'radio Enter remains untouched');
    input.value='Extra details';input.dispatchEvent(new Event('input'));assert.equal(key(input,{shiftKey:true}).defaultPrevented,false);assert.equal(sends.length,0);
    key(input);key(input,{repeat:true});key(input);await settled();assert.equal(sends.length,1);assert.equal(sends[0].id,'original-uuid');assert.equal(sends[0].retry,false);
    const reply=JSON.parse(sends[0].message.split('\n')[1])[0];assert.equal(reply.answer,'One\n\nExtra details');assert.equal(reply.questionItemId,source.questions[0].id);
    assert.equal(controller.state(source.questions[1].id).answer,undefined,'optional questions are independently answerable');
    release();await settled();assert.equal(controller.state(source.questions[0].id).answer,'One\n\nExtra details');assert.deepEqual(controller.pending(),[source.questions[1].id]);
    const failedSource=agent('uncertain-'+pinned);let failures=0;const uncertain=createAsyncQuestions({storage:new Storage(),namespace:'unknown',randomUUID:()=> 'uncertain-uuid',send:async()=>{failures++;throw Error('Lost reply');}});
    uncertain.update(snapshot([failedSource]));const failedView=uncertain.inline(failedSource);const failedInput=failedView.descendants().find(node=>node.tag==='textarea');
    key(failedInput);await settled();assert.equal(failures,1);assert.equal(failedInput.disabled,true);key(failedInput);assert.equal(failures,1,'Enter never retries an uncertain original');
    assert.equal(uncertain.state(failedSource.questions[0].id).attempt.id,'uncertain-uuid');
  }
});

test('optional empty free text and denied sending remain unsent while a selected option remains answerable',async t=>{
  const originalDocument=globalThis.document;t.after(()=>{globalThis.document=originalDocument;});globalThis.document={createElement:tag=>new QuestionElement(tag)};
  const {createAsyncQuestions}=await load();let allowed=true;const sends=[];const source=agent('free');source.questions[0].options=[];
  const controller=createAsyncQuestions({storage:new Storage(),namespace:'free',randomUUID:()=> 'free-uuid',canSend:()=>allowed,send:async value=>sends.push(value)});controller.update(snapshot([source]));
  const view=controller.inline(source),inputs=view.descendants().filter(node=>node.tag==='textarea');
  key(inputs[0]);await settled();assert.equal(sends.length,0,'blank optional free text cannot send');
  allowed=false;controller.update(snapshot([source]));inputs[0].value='Answer';inputs[0].dispatchEvent(new Event('input'));key(inputs[0]);await settled();assert.equal(sends.length,0);
  allowed=true;controller.update(snapshot([source]));key(inputs[1]);await settled();assert.equal(sends.length,1,'selected option with no note remains a valid answer');
  assert.equal(controller.state(source.questions[0].id).answer,undefined);
});

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

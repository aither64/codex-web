const assert = require("node:assert/strict");
const test = require("node:test");
const fs = require("node:fs"), vm = require("node:vm");

class Storage {
  values = new Map();
  get length() { return this.values.size; }
  key(i) { return [...this.values.keys()][i] ?? null; }
  getItem(k) { return this.values.get(k) ?? null; }
  setItem(k,v) { this.values.set(k,v); }
  removeItem(k) { this.values.delete(k); }
}

test("explicit selection confirmation re-arms only a fresh matching mounted scope", async () => {
  const {validateDeliveryCollection}=await import("../conversation/assets/conversation.js");
  const source=fs.readFileSync(require.resolve("../conversation/assets/conversation.js"),"utf8");
  const start=source.indexOf("  const confirmDeliverySelection ="),end=source.indexOf("  const submitDurable =",start);
  assert(start>0&&end>start);
  let value={version:1,scope:"selected",selection:"b".repeat(64),canEnqueue:true,deliveryModes:["steer"],settings:{model:"model",reasoningEffort:"high"},entries:[]};
  const ctx=vm.createContext({deliveryConfirmation:0,settingsGeneration:0,deliveryBaseline:{scope:"selected",selection:"a".repeat(64)},currentThread:{threadId:"native"},abort:{signal:{aborted:false}},validateDeliveryCollection,client:{deliveries:async()=>value}});
  vm.runInContext(source.slice(start,end)+"\nglobalThis.confirm=confirmDeliverySelection;",ctx);
  const expected={model:"model",reasoningEffort:"high"};
  assert.equal(await ctx.confirm(expected),true);assert.equal(ctx.deliveryBaseline.selection,"b".repeat(64));
  value={...value,scope:"foreign"};assert.equal(await ctx.confirm(expected),false);assert.equal(ctx.deliveryBaseline.scope,"selected");
  ctx.client.deliveries=async()=>{throw Error("failed read")};assert.equal(await ctx.confirm(expected),false);
  let release;ctx.client.deliveries=()=>new Promise(resolve=>{release=resolve});const pending=ctx.confirm(expected);ctx.settingsGeneration++;release({...value,scope:"selected",selection:"c".repeat(64)});
  assert.equal(await pending,false);assert.equal(ctx.deliveryBaseline.selection,"b".repeat(64));
});

test("durable delivery recovers admission loss under the original immutable ID", async () => {
  const {createConversationClient, createDurableDeliverySender, deliveryStatusPresentation} = await import("../conversation/assets/conversation.js");
  const storage = new Storage(), requests = [], selection = "a".repeat(64), scope = "member";
  let lose = true;
  const client = createConversationClient({id:"opaque", basePath:"/test", fetch:async (path, init) => {
    const input = JSON.parse(init.body); requests.push({path,input});
    if (lose) { lose=false; throw new TypeError("lost response"); }
    return {ok:true,status:202,json:async()=>({...input,scope,state:"queued",sequence:1,createdAtMs:1,updatedAtMs:1})};
  }});
  const options={client,storage,scope,randomUUID:()=>"11111111-1111-4111-8111-111111111111"};
  const sender=createDurableDeliverySender(options);
  await assert.rejects(sender.send("literal",[],{selection,delivery:"followUp"}),/lost response/);
  assert.equal(sender.pending().length,1);
  const restored=createDurableDeliverySender(options);
  const [status]=await restored.recover();
  assert.deepEqual(requests[0],requests[1]);
  assert.equal(Object.hasOwn(requests[1].input,"id"),false);
  assert.equal(status.state,"queued");
  assert.equal(deliveryStatusPresentation(status).label,"Queued");
  assert.equal(restored.pending().length,0);
  assert.equal(requests.every(r=>r.path.endsWith("/deliveries")),true);
});

test("delivery status cannot invent acceptance or change scope", async () => {
  const {validateDeliveryStatus, validateDeliveryCollection} = await import("../conversation/assets/conversation.js");
  const value={version:1,clientUserMessageId:"11111111-1111-4111-8111-111111111111",scope:"one",selection:"a".repeat(64),state:"queued",message:"x",delivery:"steer",sequence:1,createdAtMs:1,updatedAtMs:1};
  assert.throws(()=>validateDeliveryStatus({...value,state:"sent"},"one"),/Invalid/);
  assert.throws(()=>validateDeliveryStatus(value,"two"),/Invalid/);
  assert.throws(()=>validateDeliveryStatus({...value,receipt:{turnId:"t"}},"one"),/Invalid/);
  assert.equal(validateDeliveryCollection({version:1,scope:"one",canEnqueue:false,selection:"",deliveryModes:[],entries:[value]}).canEnqueue,false);
  assert.throws(()=>validateDeliveryStatus({...value,retired:true},"one"),/Invalid/);
  const retired = Array.from({length:100},(_,i)=>({...value,clientUserMessageId:i.toString(16).padStart(32,"0"),state:"unknown",retired:true}));
  assert.equal(validateDeliveryCollection({version:1,scope:"one",canEnqueue:false,selection:"",deliveryModes:[],entries:retired}).entries.length,100);
  assert.throws(()=>validateDeliveryCollection({version:1,scope:"one",canEnqueue:false,selection:"",deliveryModes:[],entries:retired.slice(0,21).map(e=>({...e,retired:false}))}),/Invalid/);
});

test("only a complete matching durable negative proof releases the original admission", async () => {
  const {createConversationClient, createDurableDeliverySender} = await import("../conversation/assets/conversation.js");
  const base = {version:1,scope:"member",request:{version:1,clientUserMessageId:"11111111-1111-4111-8111-111111111111",message:"draft",attachmentIds:[],delivery:"steer",selection:"a".repeat(64)}};
  const invalid = [null, {}, {...base,version:2}, {...base,scope:"other"}, ...[
    ["version",2], ["clientUserMessageId","2".repeat(32)], ["selection","b".repeat(64)], ["message","other"],
    ["attachmentIds",["f".repeat(32)]], ["delivery","followUp"]
  ].map(([key,value])=>({...base,request:{...base.request,[key]:value}}))];
  for (const proof of invalid) {
    const storage = new Storage();
    const client = createConversationClient({id:"opaque",fetch:async()=>({ok:false,status:409,json:async()=>({error:"refused",notQueued:true,proof})})});
    const sender = createDurableDeliverySender({client,storage,scope:"member",randomUUID:()=>base.request.clientUserMessageId});
    await assert.rejects(sender.send("draft",[],{selection:base.request.selection,delivery:"steer"}),/refused/);
    assert.equal(sender.pending().length,1,JSON.stringify(proof));
  }
  const storage = new Storage(), requests = [];
  let mode = "lost", counter = 0;
  const client = createConversationClient({id:"opaque",fetch:async(_path,init)=>{
    const request=JSON.parse(init.body); requests.push(request);
    if (mode === "lost") throw new Error("lost refusal reply");
    if (mode === "generic") return {ok:false,status:503,json:async()=>({error:"unavailable",notQueued:true})};
    if (mode === "proof") return {ok:false,status:409,json:async()=>({error:"selection changed",notQueued:true,proof:{...base,request:{...base.request,attachmentIds:undefined}}})};
    return {ok:true,status:202,json:async()=>({...request,scope:"member",state:"queued",sequence:1,createdAtMs:1,updatedAtMs:1})};
  }});
  const options={client,storage,scope:"member",randomUUID:()=> (++counter === 1 ? base.request.clientUserMessageId : "22222222-2222-4222-8222-222222222222")};
  let sender=createDurableDeliverySender(options);
  await assert.rejects(sender.send("draft",[],{selection:base.request.selection,delivery:"steer"}),/lost/);
  sender=createDurableDeliverySender(options);
  mode="generic"; await assert.rejects(sender.recover(),/unavailable/); assert.equal(sender.pending().length,1);
  mode="proof"; await assert.rejects(sender.recover(),/selection changed/); assert.equal(sender.pending().length,0);
  assert.deepEqual(requests[0],requests[2]);
  mode="accept";
  await sender.send("draft",[],{selection:"b".repeat(64),delivery:"steer"});
  assert.notEqual(requests[3].clientUserMessageId,requests[0].clientUserMessageId);
});

// Minimal mounted DOM, including node identity/focus and live text writes.
function mountedRoot() {
  const document = new EventTarget(); document.hidden = false; document.activeElement = null;
  class Node {
    constructor(tag) { this.tagName=tag; this.children=[]; this.dataset={}; this.ownerDocument=document; this.listeners={}; this.writes=0; this._text=""; }
    set textContent(value) { this._text=value; this.writes++; }
    get textContent() { return this._text; }
    setAttribute(key,value) { (this.attributes ||= {})[key]=value; }
    append(...nodes) { nodes.forEach(node=>this.insertBefore(node,null)); }
    insertBefore(node,before) { node.remove(); const i=this.children.indexOf(before); this.children.splice(i<0?this.children.length:i,0,node); node.parentElement=this; }
    remove() { if(this.parentElement){const list=this.parentElement.children;list.splice(list.indexOf(this),1);this.parentElement=null;} }
    focus() { document.activeElement=this; }
    addEventListener(type,fn) { this.listeners[type]=fn; }
  }
  document.createElement=tag=>new Node(tag);
  return new Node("section");
}
const turn=()=>new Promise(resolve=>setImmediate(resolve));
const delivery=(state="queued",n=1)=>({version:1,scope:"one",selection:"a".repeat(64),clientUserMessageId:n.toString(16).padStart(32,"0"),state,message:"literal <b>input</b>\ncomplete",delivery:"steer",sequence:n,createdAtMs:1,updatedAtMs:1,...(state==="sent"?{receipt:{turnId:"turn",clientUserMessageId:n.toString(16).padStart(32,"0")}}:{})});
const collection=entries=>({version:1,scope:"one",selection:"a".repeat(64),canEnqueue:true,deliveryModes:["steer"],entries});

test("mounted delivery rows keep identity, focus and status-only announcements",async()=>{
  const {mountDeliveryStatuses}=await import("../conversation/assets/deliveries.js");
  const root=mountedRoot(); let entries=[delivery("unknown"),delivery("not_sent",2)],reads=0,copied;
  const view=mountDeliveryStatuses(root,{client:{deliveries:async()=>{reads++;return collection(entries)}},onCopy:value=>{copied=value},interval:60000});
  try {
    await view.refresh();const row=root.children[0],status=row.children[1],check=row.children[2]; check.focus();const writes=status.writes;
    await view.refresh();assert.equal(root.children[0],row);assert.equal(root.ownerDocument.activeElement,check);assert.equal(status.writes,writes);
    assert.equal(row.children[0].textContent,entries[0].message);assert.equal(status.attributes.role,"status");assert.equal(row.attributes?.role,undefined);
    assert.equal(status.textContent,"Unconfirmed");assert.match(status.title,/confirm/);
    root.children[1].children[2].listeners.click();assert.equal(copied.clientUserMessageId,entries[1].clientUserMessageId);
    check.listeners.click();await turn();assert.equal(reads,3);
    view.present(delivery("sent"));assert.equal(root.children[0],row);assert.equal(status.textContent,"Accepted");
    assert.equal(status.title,"The runtime confirmed this input. A response may still be pending.");assert.equal(row.children.length,2);
    for(const [state,label] of Object.entries({queued:"Queued",dispatching:"Sending",pending:"Awaiting acceptance",not_sent:"Not sent",unknown:"Unconfirmed"})) {view.present(delivery(state));assert.equal(status.textContent,label)}
    assert.throws(()=>view.present({...delivery(),scope:"foreign"}),/Invalid/);
    assert.throws(()=>view.present({...delivery(),state:"sent"}),/Invalid/);
    entries=[];await view.refresh();assert.equal(root.hidden,true);assert.equal(root.children.length,1); // Hidden feedback only, no blank card.
  } finally {view.destroy()}
});

test("POST presentation survives an older GET and failed fresh follow-up",async()=>{
  const {mountDeliveryStatuses}=await import("../conversation/assets/deliveries.js");
  const root=mountedRoot();let release,reads=0,fail=false;
  const view=mountDeliveryStatuses(root,{client:{deliveries:()=>{reads++;if(reads===1)return collection([]);if(reads===2)return new Promise(resolve=>{release=resolve});if(fail)throw Error("offline");return collection([delivery("sent")])}},interval:60000});
  try {
    await view.refresh();const old=view.refresh();await turn();view.present(delivery());const row=root.children[0];
    fail=true;release(collection([]));await old;assert.equal(reads,3);assert.equal(root.children[0],row);assert.equal(row.children[1].textContent,"Queued");
    assert.equal(root.children.at(-1).hidden,false);assert.match(root.children.at(-1).textContent,/Could not refresh/);
    fail=false;await view.refresh();assert.equal(row.children[1].textContent,"Accepted");assert.equal(root.children.at(-1).hidden,true);
  } finally {view.destroy()}
});

test("wrong scope and destroyed views never expose late results",async()=>{
  const {mountDeliveryStatuses}=await import("../conversation/assets/deliveries.js");
  const root=mountedRoot();let current=true,release;
  const client={deliveries:async()=>collection([])};const view=mountDeliveryStatuses(root,{client,isCurrent:()=>current,interval:60000});
  await view.refresh();current=false;assert.equal(view.present(delivery()),false);current=true;
  client.deliveries=()=>new Promise(resolve=>{release=resolve});const pending=view.refresh();await turn();view.destroy();release(collection([delivery()]));await pending;
  assert.equal(view.present(delivery()),false);assert.equal(root.hidden,true);
});

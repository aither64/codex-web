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

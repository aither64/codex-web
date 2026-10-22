const assert = require('node:assert/strict');
const {test} = require('node:test');
const ready = import('../conversation/assets/retry.js');
const settle = async () => { for (let i=0;i<12;i++) await Promise.resolve(); };

class Node extends EventTarget {
  constructor(tag) { super(); this.tag=tag; this.children=[];this.hidden=false;this.classList={add(){}}; }
  setAttribute(name,value) { this[name]=value; }
  prepend(node) { this.children.unshift(node); }
  replaceChildren(...nodes) { this.children=nodes; }
}
async function fixture(t, client, initial=100000) {
  const {mountRetryControls}=await ready;
  const page=new EventTarget(), browser=new EventTarget(), root=new Node('section');
  page.hidden=false;page.createElement=tag=>new Node(tag);
  let at=initial, seq=0;
  const timers=new Map();
  const mount=()=>mountRetryControls(root,{client,document:page,window:browser,now:()=>at,
    setTimeout:(fn,ms)=>{const id=++seq;timers.set(id,{fn,at:at+ms});return id;},clearTimeout:id=>timers.delete(id)});
  let controls=mount();await settle();
  t.after(()=>controls.destroy());
  return {root,page,browser, timers,
    checkbox:()=>root.children[0].children[0],status:()=>root.children[1].textContent,cancel:()=>root.children[3],
    async reload(){controls.destroy();controls=mount();await settle();},
    async advance(ms){const end=at+ms;for(;;){const next=[...timers].sort((a,b)=>a[1].at-b[1].at)[0];if(!next||next[1].at>end)break;at=next[1].at;timers.delete(next[0]);next[1].fn();await settle();}at=end;},
  };
}
const status = extra => ({enabled:true,class:'capacity',phase:'scheduled',attempt:2,nextAttemptAtMs:5015000,serverNowMs:5000000,cyberUsed:0,cyberLimit:30,reason:'',...extra});

test('server clock offset drives countdown; reaching zero sends nothing',async t=>{
  let reads=0;
  const f=await fixture(t,{retry:async()=>{reads++;return status();},setRetryEnabled(){throw Error('unexpected mutation');},cancelRetry(){throw Error('unexpected mutation');}});
  assert.match(f.status(),/Retrying in 15s/);
  assert.match(f.status(),/Attempt 3/);
  await f.advance(1000);assert.match(f.status(),/Retrying in 14s/);
  await f.advance(14000);assert.equal(reads,2,'deadline only triggers the ordinary status poll');
  assert.equal(f.checkbox().checked,true);
});

test('cancel preserves enabled preference and reload uses durable server state',async t=>{
  let saved=status(),writes=0;
  const f=await fixture(t,{retry:async()=>saved,cancelRetry:async()=>{writes++;return saved={...saved,phase:'cancelled',nextAttemptAtMs:null};}});
  f.cancel().dispatchEvent(new Event('click'));await settle();
  assert.equal(writes,1);assert.match(f.status(),/cancelled/);assert.equal(f.checkbox().checked,true);
  await f.reload();assert.match(f.status(),/cancelled/);assert.equal(f.checkbox().checked,true);assert.equal(writes,1);
});

test('lost preference reply reconciles with GET without repeating the write',async t=>{
  let saved=status(),writes=0;
  const f=await fixture(t,{retry:async()=>saved,setRetryEnabled:async enabled=>{writes++;saved=status({enabled,phase:'cancelled',nextAttemptAtMs:null});throw Error('lost reply');}});
  f.checkbox().checked=false;f.checkbox().dispatchEvent(new Event('change'));await settle();
  assert.equal(writes,1);assert.equal(f.checkbox().checked,false);assert.match(f.status(),/off/);
  f.browser.dispatchEvent(new Event('focus'));await settle();assert.equal(writes,1);
});

test('visibility suspends reads and late stale results cannot overwrite restored state',async t=>{
  let resolveOld,reads=0;
  const f=await fixture(t,{retry:async()=>{reads++;if(reads===2)return new Promise(resolve=>{resolveOld=resolve});return status({enabled:reads===1,phase:'idle'});}});
  f.browser.dispatchEvent(new Event('focus'));await settle();
  f.page.hidden=true;f.page.dispatchEvent(new Event('visibilitychange'));await f.advance(60000);
  assert.equal(reads,2);
  f.page.hidden=false;f.page.dispatchEvent(new Event('visibilitychange'));await settle();
  assert.equal(f.checkbox().checked,false);
  resolveOld(status());await settle();assert.equal(f.checkbox().checked,false);
});

test('optional missing capability hides controls and exhaustion requires manual action',async t=>{
  const f=await fixture(t,{retry:async()=>{throw Object.assign(Error('missing'),{status:404});}});
  assert.equal(f.root.hidden,true);
  const {retryStatusText}=await ready;
  assert.match(retryStatusText(status({class:'cybersecurity',phase:'exhausted'}),0),/30 attempts within 5 minutes.*manually/);
  assert.match(retryStatusText(status({phase:'reconciling',enabled:false}),0),/Waiting to confirm/);
});

test('HTTP helper uses only opaque routes and preference/cancel payloads',async()=>{
  const {createConversationClient}=await import('../conversation/assets/conversation.js');
  const requests=[];
  const client=createConversationClient({id:'opaque',basePath:'/codex',fetch:async(path,options)=>{requests.push({path,options});return{ok:true,json:async()=>status()};}});
  await client.retry();await client.setRetryEnabled(false);await client.cancelRetry();
  assert.deepEqual(requests.map(r=>r.path),['/codex/conversations/opaque/retry','/codex/conversations/opaque/retry/enabled','/codex/conversations/opaque/retry/cancel']);
  assert.equal(requests[1].options.body,'{"enabled":false}');assert.equal(requests[2].options.body,'{}');
});

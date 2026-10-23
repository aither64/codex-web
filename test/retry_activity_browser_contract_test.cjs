const assert = require('node:assert/strict');
const {test} = require('node:test');
const ready = import('../conversation/assets/retry_activity.js');
const settle = async () => { for(let i=0;i<20;i++) await Promise.resolve(); };
class Node extends EventTarget {
  constructor(tag) {super();this.tag=tag;this.children=[];this.hidden=false;this.attributes={};this.classList={add(){}};this.writes=0;}
  setAttribute(name,value) {this.attributes[name]=value;}
  append(...nodes) {this.children.push(...nodes);}
  replaceChildren(...nodes) {this.children=nodes;}
  set textContent(value) {this.text=value;this.writes++;}
  get textContent() {return this.text;}
}
const memoryStorage = () => {const data=new Map();return {data,getItem:key=>data.get(key)??null,setItem:(key,value)=>data.set(key,value)};};
const state = (extra={}, envelope={}) => ({serverNowMs:5000000,activity:{chainKey:'chain-1',class:'capacity',phase:'scheduled',confirmedStarts:3,unconfirmed:0,countQuality:'exact',startedAtMs:4930000,elapsedMs:70000,timingQuality:'exact',finishedAtMs:null,nextAttemptAtMs:5015000,outcome:'',reason:'',retryNowToken:'attempt-token',stopToken:'chain-token',...extra},...envelope});
const terminal = (extra={}) => state({phase:'terminal',outcome:'success',finishedAtMs:5000000,nextAttemptAtMs:null,retryNowToken:'',stopToken:'',...extra});
async function fixture(t,client,options={}) {
  const {mountRetryActivity}=await ready;
  const page=new EventTarget(),browser=new EventTarget(),root=new Node('footer'),messages=new Node('messages'),parent=new Node('transcript');
  parent.append(messages,root);page.hidden=false;page.createElement=tag=>new Node(tag);
  const storage=options.storage===undefined?memoryStorage():options.storage;
  let at=100000,seq=0,controls;
  const timers=new Map();
  const mount=(key='conversation-1')=>{controls=mountRetryActivity(root,{client,conversationKey:key,document:page,window:browser,storage,now:()=>at,
    setTimeout:(fn,ms)=>{const id=++seq;timers.set(id,{fn,at:at+ms});return id;},clearTimeout:id=>timers.delete(id)});};
  mount(options.key);await settle();t.after(()=>controls.destroy());
  const node=className=>{const find=n=>n.className===className?n:n.children.map(find).find(Boolean);return find(root);};
  return {root,messages,parent,page,browser,timers,storage,node,
    status:()=>node('codex-retry-activity-status'),count:()=>node('codex-retry-activity-count'),elapsed:()=>node('codex-retry-activity-elapsed'),
    now:()=>node('codex-retry-activity-now'),stop:()=>node('codex-retry-activity-stop'),dismiss:()=>node('codex-retry-activity-dismiss'),live:()=>node('codex-retry-activity-announcement'),
    refresh:()=>controls.refresh(),destroy:()=>controls.destroy(),
    async reload(key=options.key){controls.destroy();mount(key);await settle();},
    async advance(ms){const end=at+ms;for(;;){const next=[...timers].sort((a,b)=>a[1].at-b[1].at)[0];if(!next||next[1].at>end)break;at=next[1].at;timers.delete(next[0]);next[1].fn();await settle();}at=end;},
  };
}

test('server time countdown, exact elapsed and running summary; countdown never submits',async t=>{
  let saved=state(),reads=0,writes=0;
  const f=await fixture(t,{retryActivity:async()=>{reads++;return saved;},retryNow:()=>writes++,stopRetry:()=>writes++});
  assert.equal(f.status().textContent,'Retrying in 15s.');assert.equal(f.count().textContent,'3 retries started');assert.equal(f.elapsed().textContent,'Total elapsed 1m 10s');
  const announcements=f.live().writes;
  await f.advance(1000);assert.equal(f.status().textContent,'Retrying in 14s.');assert.equal(f.elapsed().textContent,'Total elapsed 1m 11s');assert.equal(f.live().writes,announcements);
  await f.advance(14000);assert.equal(reads,2);assert.equal(writes,0);
  saved=state({phase:'running',nextAttemptAtMs:null,retryNowToken:''});await f.refresh();
  assert.equal(f.root.hidden,false);assert.equal(f.now().hidden,true);assert.equal(f.stop().hidden,false);assert.equal(f.status().textContent,'Retry in progress.');
  await f.advance(2000);assert.equal(f.elapsed().textContent,'Total elapsed 1m 12s');
  saved=terminal();await f.refresh();await f.advance(4000);assert.equal(f.elapsed().textContent,'Total elapsed 1m 10s');assert.equal(f.dismiss().hidden,false);
});

test('uncertain starts, partial counts and missing timing stay honest',async()=>{
  const {retryActivityPresentation:view}=await ready;
  const uncertain=view(state({phase:'reconciling',retryNowToken:'',nextAttemptAtMs:null,unconfirmed:1,countQuality:'partial',timingQuality:'partial'}),10000);
  assert.equal(uncertain.count,'At least 3 retries started');assert.equal(uncertain.elapsed,'Recorded time 1m 10s (partial)');
  assert.equal(uncertain.unconfirmed,'1 retry awaiting confirmation');assert.equal(uncertain.status,'Confirming whether the retry started.');
  const missing=view(terminal({confirmedStarts:null,countQuality:'unavailable',startedAtMs:null,elapsedMs:null,timingQuality:'unavailable',outcome:'stopped'}));
  assert.equal(missing.count,'Retry count unavailable');assert.equal(missing.elapsed,'Elapsed time unavailable');assert.equal(missing.status,'Retrying stopped.');
  assert.equal(view(state({class:'cybersecurity'})).title,'Cybersecurity error');
  for(const outcome of ['success','stopped','exhausted','superseded','blocked']) assert.ok(view(terminal({outcome})).terminal);
  assert.equal(view(state({}, {activity:null})),null);
});

test('Now and Stop use only the issued tokens, coalesce clicks and never interrupt or change preferences',async t=>{
  let saved=state(),resolveNow;const calls=[];
  const f=await fixture(t,{retryActivity:async()=>saved,retryNow:token=>{calls.push(['now',token]);return new Promise(resolve=>{resolveNow=resolve;});},stopRetry:async token=>{calls.push(['stop',token]);return saved=terminal({outcome:'stopped'});},interrupt(){assert.fail('interrupt');},setRetryEnabled(){assert.fail('preference');}});
  f.now().dispatchEvent(new Event('click'));f.now().dispatchEvent(new Event('click'));f.stop().dispatchEvent(new Event('click'));await settle();assert.deepEqual(calls,[['now','attempt-token']]);
  saved=state({phase:'running',nextAttemptAtMs:null,retryNowToken:''});resolveNow(saved);await settle();
  f.stop().dispatchEvent(new Event('click'));await settle();assert.deepEqual(calls,[['now','attempt-token'],['stop','chain-token']]);assert.equal(f.status().textContent,'Retrying stopped.');
});

test('lost or stale action response reads current status without repeating POST; reload-required stops writes',async t=>{
  let saved=state(),writes=0,reads=0,reload=false;
  const f=await fixture(t,{retryActivity:async()=>{reads++;return saved;},retryNow:async()=>{writes++;if(reload)throw Object.assign(Error('private'),{code:'retry_reload_required'});saved=state({phase:'reconciling',nextAttemptAtMs:null,retryNowToken:'',unconfirmed:1});throw Object.assign(Error('lost reply'),{code:'retry_action_stale'});},stopRetry:async()=>assert.fail('unexpected stop')});
  f.now().dispatchEvent(new Event('click'));await settle();assert.equal(writes,1);assert.equal(reads,2);assert.match(f.status().textContent,/Confirming/);
  saved=state();await f.refresh();reload=true;f.now().dispatchEvent(new Event('click'));await settle();const before=reads;
  assert.match(f.status().textContent,/Reload the page/);f.now().dispatchEvent(new Event('click'));await f.refresh();assert.equal(writes,2);assert.equal(reads,before);
});

test('dismissal persists independently for every exact conversation and chain, including out-of-order chains',async t=>{
  let saved=terminal();let writes=0;
  const f=await fixture(t,{retryActivity:async()=>saved,retryNow:()=>writes++,stopRetry:()=>writes++});
  f.dismiss().dispatchEvent(new Event('click'));assert.equal(f.root.hidden,true);await f.reload();assert.equal(f.root.hidden,true);
  saved=terminal({chainKey:'chain-2'});await f.refresh();assert.equal(f.root.hidden,false);f.dismiss().dispatchEvent(new Event('click'));assert.equal(f.root.hidden,true);
  saved=terminal();await f.refresh();assert.equal(f.root.hidden,true,'chain 2 dismissal must not forget chain 1');await f.reload();assert.equal(f.root.hidden,true);
  const reloaded=await fixture(t,{retryActivity:async()=>saved},{storage:f.storage});assert.equal(reloaded.root.hidden,true,'a new page reads persistent pair keys');
  assert.equal(f.storage.data.size,2);for(const key of f.storage.data.keys())assert.match(key,/\["conversation-1","chain-[12]"\]$/);
  await f.reload('conversation-2');assert.equal(f.root.hidden,false,'different conversation is independent');
  saved=terminal({chainKey:'chain-3'});await f.refresh();assert.equal(f.root.hidden,false);assert.equal(writes,0);
});

test('terminal dismissal is an accessible icon button in its own right heading cell',async t=>{
  let saved=state();const calls=[];
  const f=await fixture(t,{retryActivity:async()=>saved,retryNow:()=>calls.push('now'),stopRetry:()=>calls.push('stop')});
  const dismiss=f.dismiss(),heading=f.node('codex-retry-activity-heading'),actions=f.node('codex-retry-activity-actions');
  assert.equal(dismiss.tag,'button');assert.equal(dismiss.type,'button');assert.equal(dismiss.textContent,'×');
  assert.equal(dismiss.attributes['aria-label'],'Dismiss retry notice');assert.equal(dismiss.attributes.title,'Dismiss retry notice');
  assert.deepEqual(heading.children,[f.node('codex-retry-activity-title'),dismiss]);assert.deepEqual(actions.children,[f.now(),f.stop()]);
  for(const phase of ['scheduled','submitting','reconciling','running']) {
    saved=state({phase,nextAttemptAtMs:phase==='scheduled'?5015000:null,retryNowToken:phase==='scheduled'?'attempt-token':''});await f.refresh();
    assert.equal(dismiss.hidden,true);assert.equal(f.stop().hidden,false);assert.equal(f.now().hidden,phase!=='scheduled');
  }
  for(const outcome of ['success','stopped','exhausted','superseded','blocked']) {
    saved=terminal({chainKey:'chain-'+outcome,outcome});await f.refresh();assert.equal(dismiss.hidden,false);
    assert.equal(f.now().hidden,true);assert.equal(f.stop().hidden,true);
  }
  dismiss.dispatchEvent(new Event('click'));assert.equal(f.root.hidden,true);await f.reload();assert.equal(f.root.hidden,true);
  assert.deepEqual(calls,[],'dismissal never mutates backend activity');
});

test('unavailable localStorage retains all dismissals on this page',async t=>{
  let saved=terminal();const storage={getItem(){throw Error('disabled');},setItem(){throw Error('full');}};
  const f=await fixture(t,{retryActivity:async()=>saved},{storage});
  f.dismiss().dispatchEvent(new Event('click'));saved=terminal({chainKey:'chain-2'});await f.refresh();f.dismiss().dispatchEvent(new Event('click'));
  saved=terminal();await f.refresh();assert.equal(f.root.hidden,true);saved=terminal({chainKey:'chain-2'});await f.refresh();assert.equal(f.root.hidden,true);
  await f.reload('conversation-2');assert.equal(f.root.hidden,false);await f.reload('conversation-1');assert.equal(f.root.hidden,true,'same-page conversation remount retains dismissal');
  const reloaded=await fixture(t,{retryActivity:async()=>saved},{storage});assert.equal(reloaded.root.hidden,false,'a new page cannot retain dismissal without storage');
});

test('idle has no footprint; renderer owns footer children only and safely inserts reason text',async t=>{
  let saved=state({}, {activity:null});const f=await fixture(t,{retryActivity:async()=>saved});
  assert.equal(f.root.hidden,true);assert.deepEqual(f.parent.children,[f.messages,f.root]);assert.equal(f.messages.children.length,0);
  saved=state({reason:'<img src=x onerror=alert(1)>'});await f.refresh();assert.equal(f.node('codex-retry-activity-reason').textContent,saved.activity.reason);
  f.destroy();assert.equal(f.root.hidden,true);assert.equal(f.root.children.length,0);assert.deepEqual(f.parent.children,[f.messages,f.root]);assert.equal(f.timers.size,0);
});

test('reads coalesce, visibility suspends and late stale/destroyed responses cannot overwrite',async t=>{
  let resolveOld,resolveLate,reads=0;const f=await fixture(t,{retryActivity:async()=>{reads++;if(reads===2)return new Promise(resolve=>{resolveOld=resolve});if(reads===4)return new Promise(resolve=>{resolveLate=resolve});return reads===1?state():terminal();}});
  const a=f.refresh(),b=f.refresh();assert.equal(a,b);await settle();assert.equal(reads,2);
  f.page.hidden=true;f.page.dispatchEvent(new Event('visibilitychange'));await f.advance(60000);assert.equal(reads,2);assert.equal(f.timers.size,0);
  f.page.hidden=false;f.page.dispatchEvent(new Event('visibilitychange'));await settle();assert.equal(f.status().textContent,'Retry completed.');
  resolveOld(state());await a;assert.equal(f.status().textContent,'Retry completed.');
  f.browser.dispatchEvent(new Event('pagehide'));f.browser.dispatchEvent(new Event('pageshow'));await settle();assert.equal(reads,4);
  f.destroy();resolveLate(state());await settle();assert.equal(f.root.children.length,0);assert.equal(f.timers.size,0);
});

test('malformed or missing capability status never enables controls or exposes errors',async t=>{
  let saved=state(),error=null;const f=await fixture(t,{retryActivity:async()=>{if(error)throw error;return saved;}});
  saved=state({confirmedStarts:-1});await f.refresh();assert.match(f.status().textContent,/unavailable/);assert.equal(f.now().disabled,true);
  error=Object.assign(Error('private-data'),{status:404});await f.refresh();assert.equal(f.root.hidden,true);
  error=Object.assign(Error('private-data'),{status:403});await f.refresh();assert.equal(f.root.hidden,true);
  const {retryActivityPresentation:view}=await ready;
  for(const invalid of [state({class:'other'}),state({confirmedStarts:null}),state({unconfirmed:2}),state({phase:'running'}),state({reason:1}),state({reason:'é'.repeat(513)}),state({reason:'\uD800'}),state({}, {serverNowMs:Number.MAX_SAFE_INTEGER+1})]) assert.throws(()=>view(invalid),/invalid retry/);
});

test('HTTP adapter uses opaque activity routes and token-only bodies',async()=>{
  const {createConversationClient,mountRetryActivity}=await import('../conversation/assets/conversation.js');assert.equal(typeof mountRetryActivity,'function');const requests=[];
  const client=createConversationClient({id:'opaque',basePath:'/codex',fetch:async(path,options)=>{requests.push({path,options});return {ok:true,json:async()=>state()};}});
  await client.retryActivity();await client.retryNow('attempt-token');await client.stopRetry('chain-token');
  assert.deepEqual(requests.map(r=>r.path),['/codex/conversations/opaque/retry/activity','/codex/conversations/opaque/retry/now','/codex/conversations/opaque/retry/stop']);
  assert.deepEqual(requests.slice(1).map(r=>JSON.parse(r.options.body)),[{token:'attempt-token'},{token:'chain-token'}]);
});

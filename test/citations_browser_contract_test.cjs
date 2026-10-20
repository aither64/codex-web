const assert = require("node:assert/strict");

(async () => {
  const {citationCopyText, createCitationMessage} = await import("../conversation/assets/citations.js");
  const {createTranscriptHistory, readTranscriptPage} = await import("../conversation/assets/conversation.js");
  const token = "\ue200cite\ue202ref\ue201", prefix = "Příliš 🐈 ";
  const start = Buffer.byteLength(prefix), end = start + Buffer.byteLength(token);
  const entry = {kind: "agentMessage", turnStatus: "completed", text: prefix + token + " done",
    citations: [{start, end, sources: [{url: "https://pypi.org/help/", title: 'PyPI "help"', number: 1}]}]};
  assert.equal(citationCopyText(entry), 'Příliš 🐈 [\\[1\\]](<https://pypi.org/help/> "PyPI \\"help\\"") done');
  class Element {
    constructor(tag) { this.tag = tag; this.children = []; this.attributes = {}; this.textContent = ""; }
    append(...children) { this.children.push(...children); }
    setAttribute(key, value) { this.attributes[key] = value; }
  }
  const document = {createElement: tag => new Element(tag), createTextNode: text => ({text})};
  const body = createCitationMessage(entry, document);
  const link = body.children.find(child => child.tag === "a");
  assert.equal(link.textContent, "[1]");
  assert.equal(link.attributes.href, "https://pypi.org/help/");
  assert.equal(link.attributes.title, 'PyPI "help"');
  assert.match(link.attributes["aria-label"], /PyPI/);
  const unavailable = {...entry, citations: [{start, end, unavailable: true}]};
  assert.equal(citationCopyText(unavailable), prefix + "[source unavailable] done");
  assert.equal(citationCopyText({...unavailable, citationsPending: true}), prefix + "[source pending] done");
  assert.equal(citationCopyText({...entry, kind: "userMessage"}), entry.text);
  assert.equal(citationCopyText({...entry, citations: [{start, end, sources: [{url: "javascript:alert(1)", number: 1}]}]}), prefix + "[source unavailable] done");
  const partial = {kind: "agentMessage", text: "Text \ue200c", turnStatus: "inProgress", citations: [{start: 5, end: Buffer.byteLength("Text \ue200c"), incomplete: true}]};
  assert.equal(citationCopyText(partial), "Text ");
  assert.equal(citationCopyText({...partial, turnStatus: "completed"}), "Text [source unavailable]");

  const history = createTranscriptHistory();
  const page = entries => ({threadId: "thread", status: "idle", hasOlder: true, olderCursor: "older", entries});
  history.applyNewest(page([{kind: "agentMessage", turnId: "new", itemId: "new", text: "Newest"}]));
  const old = {...unavailable, turnId: "old", itemId: "old", citationsPending: true};
  const older = await readTranscriptPage({threadPage: async () => ({...page([old]), olderCursor: "oldest"})}, {cursor: "older"});
  history.applyOlder(older);
  assert.equal(history.gap, false);
  assert.equal(history.citationsPending, true);
  assert.equal(history.repairCursor, "older");
  history.applyRepair({...older, entries: [{...old, citationsPending: false, citations: entry.citations}]});
  assert.equal(history.citationsPending, false);
  assert.equal(history.entries.length, 2);
  assert.equal(history.entries[0].citationsPending, false);
  const aging = createTranscriptHistory({policy: {historyRepairMs: 0}});
  const row = id => ({kind: "agentMessage", turnId: id, itemId: id, turnStatus: "completed", text: id});
  aging.applyNewest(page([{...old, itemId: "pending"}, row("overlap")]));
  aging.applyNewest({...page([row("overlap"), row("newer")]), olderCursor: "find-pending"});
  assert.equal(aging.gap, true);
  assert.equal(aging.repairCursor, "find-pending");
  aging.applyRepair({...page([row("between")]), requestCursor: "find-pending", olderCursor: "pending-page"});
  aging.applyNewest({...page([row("newer"), row("latest")]), olderCursor: "new-newest-cursor"});
  assert.equal(aging.repairCursor, "pending-page", "newest refresh must preserve repair progress");
  const pendingPage = {...page([{...old, itemId: "pending"}]), requestCursor: "pending-page"};
  aging.applyRepair(pendingPage);
  assert.equal(aging.gap, false);
  assert.equal(aging.citationsPending, true);
  assert.equal(aging.repairCursor, "pending-page");
  aging.applyRepair({...pendingPage, entries: [{...old, itemId: "pending", citationsPending: false, citations: entry.citations}]});
  assert.equal(aging.citationsPending, false);
  assert.equal(aging.entries.length, 4, "preserve previously loaded rows");
  assert.equal(aging.entries[0].citationsPending, false);
  history.applyOlder(older);
  history.invalidateCursor();
  assert.equal(history.citationsPending, false);
  history.clear("another-thread");
  assert.equal(history.entries.length, 0);
})();

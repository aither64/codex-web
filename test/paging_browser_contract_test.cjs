"use strict";

const assert = require("node:assert/strict");

const entry = (number, status = "completed") => ({
  turnId: "turn-1", itemId: `item-${number}`, kind: "agentMessage",
  text: `message ${number}`, turnStatus: status,
});
const page = (numbers, cursor = null) => ({
  threadId: "thread-1", status: "idle", latestTurnId: "turn-1",
  entries: numbers.map(number => entry(number)),
  olderCursor: cursor, hasOlder: Boolean(cursor),
});
const numbers = history => history.entries.map(item => Number(item.itemId.slice(5)));
const range = (start, end) => Array.from({length: end - start + 1}, (_, index) => index + start);
const lineagePage = (items, turnId, cursor = null) => ({...page([], cursor),
  entries: items.map(number => ({...entry(number), turnId})),
});

(async () => {
  const {createTranscriptHistory, readTranscriptPage, transcriptEntryKey} = await import(
    "../conversation/assets/conversation.js"
  );
  const history = createTranscriptHistory();
  history.applyNewest(page(Array.from({length: 100}, (_, index) => index + 101), "older-1"));
  history.applyOlder(page(Array.from({length: 100}, (_, index) => index + 1), "older-2"));
  history.applyNewest(page(Array.from({length: 100}, (_, index) => index + 151), "recent-2"));
  assert.equal(history.entries.length, 250);
  assert.equal(history.olderCursor, "older-2");
  history.applyNewest(page(Array.from({length: 100}, (_, index) => index + 351), "gap-1"));
  assert.equal(history.gap, true);
  history.applyRepair(page(Array.from({length: 100}, (_, index) => index + 251), "gap-2"));
  history.applyRepair({...page([], "gap-3"), entries: Array.from({length: 100}, (_, index) =>
    entry(index + 201, index + 201 === 225 ? "failed" : "completed"))});
  assert.equal(history.gap, false);
  assert.equal(history.olderCursor, "older-2");
  assert.deepEqual(numbers(history), Array.from({length: 450}, (_, index) => index + 1));
  assert.equal(history.entries[224].turnStatus, "failed");

  const active = createTranscriptHistory();
  active.applyNewest({threadId: "thread-1", entries: [entry(1, "inProgress"), entry(2)],
    olderCursor: "active-1", hasOlder: true});
  active.applyNewest({threadId: "thread-1", entries: [entry(2), entry(3)],
    olderCursor: "active-2", hasOlder: true}, 31_000);
  assert.equal(active.repairCursor, "active-2");
  active.applyRepair({threadId: "thread-1", entries: [entry(1, "completed")], hasOlder: false});
  assert.equal(active.entries[0].turnStatus, "completed");
  active.invalidateCursor();
  assert.equal(active.gap, true);
  active.applyNewest(page([2, 3, 4], "reset-1"));
  assert.equal(active.repairCursor, "reset-1");
  active.clear();
  active.applyNewest(page([20, 21], "new-lineage"));
  assert.deepEqual(numbers(active), [20, 21]);
  active.applyNewest(page([21, 22]));
  assert.deepEqual(numbers(active), [21, 22]);
  assert.equal(active.hasOlder, false);
  const replaced = createTranscriptHistory();
  replaced.applyNewest(page(range(101, 200), "old-cursor"));
  const oldReadVersion = replaced.repairVersion;
  replaced.applyNewest(page(range(201, 300)));
  if (replaced.repairVersion === oldReadVersion) replaced.applyOlder(page(range(1, 100)));
  assert.deepEqual(numbers(replaced), range(201, 300));
  assert.notEqual(replaced.repairVersion, oldReadVersion);
  assert.equal(replaced.gap, false);
  replaced.applyNewest(page(range(101, 200), "another-cursor"));
  const legacyReadVersion = replaced.repairVersion;
  replaced.applyNewest({...page([400, 401]), legacy: true});
  if (replaced.repairVersion === legacyReadVersion) replaced.applyOlder(page([1, 2]));
  assert.deepEqual(numbers(replaced), [400, 401]);
  assert.notEqual(replaced.repairVersion, legacyReadVersion);
  const resetHistory = createTranscriptHistory();
  resetHistory.applyNewest(page(Array.from({length: 100}, (_, index) => index + 101), "old-1"));
  resetHistory.applyOlder(page(Array.from({length: 100}, (_, index) => index + 1), "old-2"));
  resetHistory.invalidateCursor();
  assert.deepEqual(numbers(resetHistory), Array.from({length: 200}, (_, index) => index + 1));
  assert.equal(resetHistory.hasOlder, false);
  resetHistory.applyNewest(page(Array.from({length: 100}, (_, index) => index + 251), "new-1"));
  assert.equal(resetHistory.gap, true);
  assert.equal(resetHistory.repairCursor, "new-1");
  resetHistory.applyRepair(page(Array.from({length: 100}, (_, index) => index + 151), "new-2"));
  assert.deepEqual(numbers(resetHistory), Array.from({length: 350}, (_, index) => index + 1));
  assert.equal(resetHistory.gap, true);
  assert.equal(resetHistory.repairCursor, "new-2");
  resetHistory.applyRepair(page(Array.from({length: 100}, (_, index) => index + 51), "new-3"));
  resetHistory.applyRepair(page(Array.from({length: 50}, (_, index) => index + 1), "new-4"));
  assert.equal(resetHistory.gap, false);
  assert.equal(resetHistory.olderCursor, "new-4");
  assert.equal(resetHistory.hasOlder, true);
  assert.deepEqual(numbers(resetHistory), Array.from({length: 350}, (_, index) => index + 1));
  const overlappingReset = createTranscriptHistory();
  overlappingReset.applyNewest(page(Array.from({length: 100}, (_, index) => index + 101), "old-a"));
  overlappingReset.applyOlder(page(Array.from({length: 100}, (_, index) => index + 1), "old-b"));
  overlappingReset.invalidateCursor();
  overlappingReset.applyNewest(page(Array.from({length: 100}, (_, index) => index + 151), "new-a"));
  assert.deepEqual(numbers(overlappingReset), Array.from({length: 250}, (_, index) => index + 1));
  assert.equal(overlappingReset.repairCursor, "new-a");
  overlappingReset.applyRepair(page(Array.from({length: 100}, (_, index) => index + 51), "new-b"));
  overlappingReset.applyRepair(page(Array.from({length: 50}, (_, index) => index + 1), "new-c"));
  assert.equal(overlappingReset.gap, false);
  assert.equal(overlappingReset.olderCursor, "new-c");
  assert.deepEqual(numbers(overlappingReset), Array.from({length: 250}, (_, index) => index + 1));
  overlappingReset.invalidateCursor();
  overlappingReset.applyNewest({...page([240, 241]), legacy: true});
  assert.equal(overlappingReset.gap, false);
  assert.deepEqual(numbers(overlappingReset), [240, 241]);
  const disjoint = createTranscriptHistory();
  disjoint.applyNewest(lineagePage(range(101, 200), "old", "old-page"));
  disjoint.applyOlder(lineagePage(range(1, 100), "old"));
  disjoint.invalidateCursor();
  disjoint.applyNewest(lineagePage(range(201, 300), "new", "new-page-1"));
  disjoint.applyRepair(lineagePage(range(101, 200), "new", "new-page-2"));
  assert.equal(disjoint.gap, true);
  assert.equal(disjoint.entries.filter(item => item.turnId === "old").length, 200);
  const versionBeforeRepairCompletion = disjoint.repairVersion;
  disjoint.applyRepair(lineagePage(range(1, 100), "new"));
  assert.equal(disjoint.repairVersion, versionBeforeRepairCompletion + 1);
  assert.deepEqual(numbers(disjoint), range(1, 300));
  assert.equal(disjoint.entries.every(item => item.turnId === "new"), true);
  assert.equal(disjoint.gap, false);
  const deletion = createTranscriptHistory();
  deletion.applyNewest(page(range(101, 200), "old-page"));
  deletion.applyOlder(page(range(1, 100)));
  deletion.invalidateCursor();
  deletion.applyNewest(page(range(201, 300), "new-page-1"));
  deletion.applyRepair(page(range(101, 201).filter(number => number !== 120), "new-page-2"));
  assert.equal(numbers(deletion).includes(120), true);
  deletion.applyRepair(page(range(1, 100)));
  assert.deepEqual(numbers(deletion), range(1, 300).filter(number => number !== 120));
  assert.equal(deletion.gap, false);
  const concurrentNewest = createTranscriptHistory();
  concurrentNewest.applyNewest(page(range(101, 200), "old-page"));
  concurrentNewest.applyOlder(page(range(1, 100)));
  concurrentNewest.invalidateCursor();
  concurrentNewest.applyNewest(page(range(201, 300), "new-page-1"));
  const priorVersion = concurrentNewest.repairVersion;
  concurrentNewest.applyNewest(page(range(251, 350), "new-page-2"));
  assert.equal(concurrentNewest.repairVersion, priorVersion);
  concurrentNewest.applyRepair(page(range(101, 200), "new-page-3"));
  concurrentNewest.applyRepair(page(range(1, 100)));
  assert.deepEqual(numbers(concurrentNewest), range(1, 350));
  const restartedNewest = createTranscriptHistory();
  restartedNewest.applyNewest(page([1], "old-page"));
  restartedNewest.invalidateCursor();
  restartedNewest.applyNewest(page([2], "new-page-1"));
  const staleVersion = restartedNewest.repairVersion;
  restartedNewest.applyNewest(page([4], "new-page-2"));
  assert.notEqual(restartedNewest.repairVersion, staleVersion);
  restartedNewest.applyRepair(page([2, 3], "new-page-3"));
  restartedNewest.applyRepair(page([1]));
  assert.deepEqual(numbers(restartedNewest), [1, 2, 3, 4]);
  const emptyReset = createTranscriptHistory();
  emptyReset.applyNewest(page([], "old-page"));
  emptyReset.invalidateCursor();
  emptyReset.applyNewest(page([], "new-page"));
  assert.equal(emptyReset.gap, false);
  assert.equal(emptyReset.olderCursor, "new-page");
  const newestCompletion = createTranscriptHistory();
  newestCompletion.applyNewest(page([1, 2], "old-page"));
  newestCompletion.invalidateCursor();
  newestCompletion.applyNewest(page([3, 4], "new-page-1"));
  const versionBeforeNewestCompletion = newestCompletion.repairVersion;
  newestCompletion.applyNewest(page([1, 2, 3, 4], "new-page-2"));
  assert.equal(newestCompletion.repairVersion, versionBeforeNewestCompletion + 1);
  assert.equal(newestCompletion.gap, false);
  assert.equal(newestCompletion.olderCursor, "new-page-2");
  assert.deepEqual(numbers(newestCompletion), [1, 2, 3, 4]);
  assert.equal(transcriptEntryKey({turnId: "turn-1", kind: "error"}, 0, []),
    transcriptEntryKey({turnId: "turn-1", kind: "error"}, 0, []));

  const authorizedLegacy = {threadId: "thread-1", status: "idle", entries: [entry(1)]};
  let fallbackReads = 0;
  const unsupported = {
    threadPage: async () => { throw Object.assign(new Error("unavailable"), {status: 501, code: "transcript_paging_unavailable"}); },
    thread: async () => { fallbackReads++; return authorizedLegacy; },
  };
  assert.equal((await readTranscriptPage(unsupported)).legacy, true);
  const missingRoute = {...unsupported, threadPage: async () => { throw Object.assign(new Error("missing"), {status: 404}); }};
  assert.equal((await readTranscriptPage(missingRoute)).legacy, true);
  assert.equal((await readTranscriptPage({thread: unsupported.thread})).legacy, true);
  for (const client of [unsupported, missingRoute, {thread: unsupported.thread}]) {
    await assert.rejects(readTranscriptPage(client, {expectedThreadId: "another-thread"}),
      /Invalid conversation response/);
  }
  await assert.rejects(readTranscriptPage(unsupported, {legacy: true, expectedThreadId: "another-thread"}),
    /Invalid conversation response/);
  for (const fault of [
    {status: 401}, {status: 403}, {status: 409, code: "transcript_cursor_expired"},
    {status: 503}, {name: "AbortError"},
  ]) {
    const client = {...unsupported, threadPage: async () => { throw Object.assign(new Error("fault"), fault); }};
    await assert.rejects(readTranscriptPage(client));
  }
  await assert.rejects(readTranscriptPage(missingRoute, {cursor: "older"}));
  assert.equal(fallbackReads, 7);
  for (const invalid of [
    {threadId: "thread-1", entries: []},
    {...page([1], "same"), olderCursor: "same"},
    page(Array.from({length: 101}, (_, index) => index)),
    {...page([1]), threadId: "foreign"},
  ]) {
    await assert.rejects(readTranscriptPage({threadPage: async () => invalid},
      {cursor: invalid.olderCursor === "same" ? "same" : undefined,
        expectedThreadId: invalid.threadId === "foreign" ? "thread-1" : undefined}));
  }
  console.log("shared paged transcript contracts passed");
})().catch(error => { console.error(error); process.exitCode = 1; });

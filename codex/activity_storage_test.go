package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestActivitySlowThreadStorageDoesNotBlockCaptureOrOtherThreads(t *testing.T) {
	r, err := NewActivityRecorder(filepath.Join(t.TempDir(), "activity"))
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	r.writeFile = func(path string, data []byte) error {
		if strings.Contains(path, activityFileID("slow")) {
			select {
			case <-started:
			default:
				close(started)
			}
			<-release
		}
		return writeActivityFile(path, data)
	}
	defer r.Close()
	defer close(release)
	for _, id := range []string{"slow", "fast"} {
		if err := r.prepare(context.Background(), id); err != nil {
			t.Fatal(err)
		}
		r.connected(id, "connection", 1000)
		r.reconcile(id, "connection", activityTurn("inProgress", 0), map[string]any{"type": "active"}, 1000, 0)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("writer did not start")
	}
	captured := make(chan struct{})
	go func() {
		activityEvent(r, "connection", "item/tool/requestUserInput", 1, map[string]any{"threadId": "slow", "turnId": "turn", "isBlocking": true}, 2000)
		r.reconcile("fast", "connection", activityTurn("inProgress", 0), map[string]any{"type": "active"}, 3000, 0)
		close(captured)
	}()
	select {
	case <-captured:
	case <-time.After(time.Second):
		t.Fatal("disk I/O blocked event capture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.flush(ctx, "fast"); err != nil {
		t.Fatal(err)
	}
	if got := r.snapshotContext(ctx, "fast", activityTurn("inProgress", 0), "thread", 3000, nil); got.WorkingMS != 2000 {
		t.Fatalf("unrelated thread = %#v", got)
	}
	slowCtx, slowCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer slowCancel()
	err = r.flush(slowCtx, "slow")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("slow checkpoint = %v", err)
	}
	if got := r.snapshotContext(slowCtx, "slow", activityTurn("inProgress", 0), "thread", 3000, err); got.WorkingMS != 0 || got.CurrentState != "unclassified" || got.UnclassifiedMS != 2000 {
		t.Fatalf("undurable coverage = %#v", got)
	}
}

func TestActivityCompactsSustainedRequestsAndRetainsAllTurnTotalsAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "activity")
	r, err := NewActivityRecorder(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	prepareActivity(t, r)
	r.connected("thread", "connection", 1000)
	r.reconcile("thread", "connection", nil, map[string]any{"type": "idle"}, 1000, 0)
	turns := make([]TurnMetadata, 0, 250)
	for n := 0; n < 250; n++ {
		id, start := fmt.Sprintf("turn-%d", n), int64(1000+n*1000)
		activityEvent(r, "connection", "turn/started", nil, map[string]any{"threadId": "thread", "turn": map[string]any{"id": id, "status": "inProgress", "startedAt": start / 1000}}, start)
		for q := 0; q < 100; q++ {
			at := start + int64(q*2) + 1
			activityEvent(r, "connection", "item/tool/requestUserInput", q, map[string]any{"threadId": "thread", "turnId": id, "isBlocking": true, "questions": "PRIVATE"}, at)
			activityEvent(r, "connection", "serverRequest/resolved", nil, map[string]any{"threadId": "thread", "requestId": q}, at+1)
		}
		activityEvent(r, "connection", "turn/completed", nil, map[string]any{"threadId": "thread", "turn": map[string]any{"id": id, "status": "completed", "startedAt": start / 1000, "completedAt": start/1000 + 1}}, start+1000)
		turns = append(turns, TurnMetadata{ID: id, Status: "completed", StartedAtMS: start, CompletedAtMS: start + 1000})
		if n%25 == 24 {
			if err := r.flush(context.Background(), "thread"); err != nil {
				t.Fatal(err)
			}
		}
	}
	thread := r.lookup("thread")
	thread.mu.Lock()
	pending, hot := len(thread.hot.Current.Requests), len(thread.hot.Turns)
	thread.mu.Unlock()
	if pending != 0 || hot != 0 {
		t.Fatalf("retained closed data: requests=%d turns=%d", pending, hot)
	}
	entries, err := os.ReadDir(filepath.Join(thread.path, "turns"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(turns) {
		t.Fatalf("stored %d of %d summaries", len(entries), len(turns))
	}
	for _, entry := range entries {
		data, err := readPrivateActivityFile(filepath.Join(thread.path, "turns", entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if len(data) > 1024 || strings.Contains(string(data), "PRIVATE") || strings.Contains(string(data), "requests") {
			t.Fatalf("uncompacted summary: %s", data)
		}
	}
	assertTotals := func(r *ActivityRecorder) {
		t.Helper()
		got := r.snapshot("thread", turns, "thread", 251000)
		if got.WorkingMS != 225000 || got.WaitingMS != 25000 || got.UnclassifiedMS != 0 || !got.CoverageComplete {
			t.Fatalf("all retained totals = %#v", got)
		}
		data, _ := json.Marshal(got)
		if len(data) > 1024 || strings.Contains(string(data), `"turns"`) {
			t.Fatalf("activity response grows with history: %s", data)
		}
	}
	assertTotals(r)
	r.disconnected("connection", "thread")
	done := thread.done
	r.retire("thread")
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retired thread retained its cache")
	}
	if r.lookup("thread") != nil {
		t.Fatal("retired thread remains loaded")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	r, err = NewActivityRecorder(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	prepareActivity(t, r)
	assertTotals(r)
}

func TestActivityRevertedTurnsKeepIndependentSummaries(t *testing.T) {
	r := startActivity(t, filepath.Join(t.TempDir(), "activity"))
	r.reconcile("thread", "connection:1", activityTurn("inProgress", 0), nil, 3000, 0)
	r.reconcile("thread", "connection:1", nil, nil, 3000, 0)
	if err := r.flush(context.Background(), "thread"); err != nil {
		t.Fatal(err)
	}
	if got := r.snapshot("thread", nil, "thread", 5000); got.WorkingMS != 0 || got.CurrentTurnID != "" {
		t.Fatalf("removed turn leaked into totals: %#v", got)
	}
	// The same retained identity can be read again without losing its old coverage.
	r.seedCurrent(context.Background(), "thread", activityTurn("inProgress", 0))
	r.reconcile("thread", "connection:1", activityTurn("inProgress", 0), nil, 5000, 0)
	r.reconcile("thread", "connection:1", activityTurn("inProgress", 0), nil, 6000, 0)
	if got := r.snapshot("thread", activityTurn("inProgress", 0), "thread", 6000); got.WorkingMS != 3000 || got.UnclassifiedMS != 2000 {
		t.Fatalf("restored coverage = %#v", got)
	}
}

func TestActivitySummaryCheckpointCrashKeepsNewerDurableRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "activity")
	r := startActivity(t, path)
	r.reconcile("thread", "connection:1", activityTurn("inProgress", 0), nil, 3000, 0)
	if err := r.flush(context.Background(), "thread"); err != nil {
		t.Fatal(err)
	}
	thread := r.lookup("thread")
	old, err := os.ReadFile(filepath.Join(thread.path, "current.json"))
	if err != nil {
		t.Fatal(err)
	}
	activityEvent(r, "connection:1", "turn/completed", nil, map[string]any{"threadId": "thread", "turn": map[string]any{"id": "turn", "status": "completed", "startedAt": 1, "completedAt": 5}}, 5500)
	if err := r.flush(context.Background(), "thread"); err != nil {
		t.Fatal(err)
	}
	r.Close()
	// Simulate death between the summary rename and the checkpoint rename.
	if err := os.WriteFile(filepath.Join(thread.path, "current.json"), old, 0600); err != nil {
		t.Fatal(err)
	}
	r, err = NewActivityRecorder(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	prepareActivity(t, r)
	if got := r.snapshot("thread", activityTurn("completed", 5000), "thread", 6000); got.WorkingMS != 4000 || got.UnclassifiedMS != 0 {
		t.Fatalf("crash replay = %#v", got)
	}
}

func TestActivityOutstandingRequestLimitIsPerThreadAndFailClosed(t *testing.T) {
	r := startActivity(t, filepath.Join(t.TempDir(), "activity"))
	for id := 0; id < activityPendingLimit+10; id++ {
		activityEvent(r, "connection:1", "item/tool/requestUserInput", id, map[string]any{"threadId": "thread", "turnId": "turn", "isBlocking": false}, 2000)
	}
	if got := r.snapshot("thread", activityTurn("inProgress", 0), "thread", 3000); got.CurrentState != "unclassified" {
		t.Fatalf("overflow = %#v", got)
	}
	thread := r.lookup("thread")
	thread.mu.Lock()
	defer thread.mu.Unlock()
	if len(thread.hot.Current.Requests) != activityPendingLimit {
		t.Fatalf("pending limit = %d", len(thread.hot.Current.Requests))
	}
}

func TestActivityHistoryAggregationDoesNotHoldCaptureLock(t *testing.T) {
	r := startActivity(t, filepath.Join(t.TempDir(), "activity"))
	r.reconcile("thread", "connection:1", activityTurn("inProgress", 0), nil, 3000, 0)
	if err := r.flush(context.Background(), "thread"); err != nil {
		t.Fatal(err)
	}
	thread := r.lookup("thread")
	thread.historyMu.Lock()
	done := make(chan ActivitySnapshot, 1)
	go func() {
		done <- r.snapshotContext(context.Background(), "thread", activityTurn("inProgress", 0), "thread", 3000, nil)
	}()
	// Hold the independent history-read lock while a snapshot waits on it.
	time.Sleep(20 * time.Millisecond)
	captured := make(chan struct{})
	go func() {
		activityEvent(r, "connection:1", "item/tool/requestUserInput", 1, map[string]any{"threadId": "thread", "turnId": "turn", "isBlocking": true}, 3500)
		close(captured)
	}()
	select {
	case <-captured:
	case <-time.After(time.Second):
		thread.historyMu.Unlock()
		t.Fatal("history aggregation blocked capture")
	}
	thread.historyMu.Unlock()
	if got := <-done; got.WorkingMS != 2000 {
		t.Fatalf("durable snapshot = %#v", got)
	}
}

func TestActivityDelayedTerminalMetadataClipsAtServerBoundary(t *testing.T) {
	for _, notification := range []bool{true, false} {
		t.Run(fmt.Sprint(notification), func(t *testing.T) {
			r := startActivity(t, filepath.Join(t.TempDir(), "activity"))
			r.reconcile("thread", "connection:1", activityTurn("inProgress", 0), nil, 3000, 0)
			if notification {
				activityEvent(r, "connection:1", "turn/completed", nil, map[string]any{"threadId": "thread", "turn": map[string]any{"id": "turn", "status": "completed", "startedAt": 1, "completedAt": 5}}, 9500)
			} else {
				r.reconcile("thread", "connection:1", activityTurn("completed", 5000), nil, 9500, 0)
			}
			if got := r.snapshot("thread", activityTurn("completed", 5000), "thread", 9500); got.WorkingMS != 4000 || got.UnclassifiedMS != 0 {
				t.Fatalf("delayed completion = %#v", got)
			}
		})
	}
}

func TestActivityOversizedThreadStateDoesNotDisableOtherThreads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "activity")
	r, err := NewActivityRecorder(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	badPath := filepath.Join(path, activityFileID("oversized"))
	if err := os.MkdirAll(badPath, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(badPath, "current.json"), make([]byte, activityCheckpointLimit+1), 0600); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"oversized", "healthy"} {
		if err := r.prepare(context.Background(), id); err != nil {
			t.Fatal(err)
		}
		r.connected(id, "connection", 1000)
		r.reconcile(id, "connection", activityTurn("inProgress", 0), nil, 1000, 0)
		r.reconcile(id, "connection", activityTurn("inProgress", 0), nil, 3000, 0)
	}
	if got := r.snapshot("oversized", activityTurn("inProgress", 0), "thread", 3000); got.CurrentState != "unclassified" || got.CoverageReason != "Activity recording is unavailable." {
		t.Fatalf("oversized thread = %#v", got)
	}
	if got := r.snapshot("healthy", activityTurn("inProgress", 0), "thread", 3000); got.WorkingMS != 2000 || !got.CoverageComplete {
		t.Fatalf("healthy thread = %#v", got)
	}
}

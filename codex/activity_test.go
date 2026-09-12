package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/coder/websocket"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func activityEvent(r *ActivityRecorder, connection, method string, id any, params map[string]any, at int64) {
	data, _ := json.Marshal(params)
	var identity json.RawMessage
	if id != nil {
		identity, _ = json.Marshal(id)
	}
	r.observe(connection, rpcMessage{Method: method, ID: identity, Params: data}, at)
}

func activityTurn(status string, end int64) []TurnMetadata {
	return []TurnMetadata{{ID: "turn", Status: status, StartedAtMS: 1000, CompletedAtMS: end}}
}

// Tests wait for the same durable boundary as ReadActivity; event capture
// itself remains asynchronous.
func (r *ActivityRecorder) snapshot(id string, turns []TurnMetadata, scope string, now int64) ActivitySnapshot {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := r.flush(ctx, id)
	return r.snapshotContext(ctx, id, turns, scope, now, err)
}

func prepareActivity(t *testing.T, r *ActivityRecorder) {
	t.Helper()
	if err := r.prepare(context.Background(), "thread"); err != nil {
		t.Fatal(err)
	}
}

func startActivity(t *testing.T, path string) *ActivityRecorder {
	t.Helper()
	r, err := NewActivityRecorder(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	prepareActivity(t, r)
	r.connected("thread", "connection:1", 1000)
	r.reconcile("thread", "connection:1", activityTurn("inProgress", 0), map[string]any{"type": "active"}, 1000, 0)
	return r
}

func TestActivityBlockingUnionNonblockingTerminalAndPrivacy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "activity.json")
	r := startActivity(t, path)
	request := func(id int, blocking bool, at int64) {
		activityEvent(r, "connection:1", "item/tool/requestUserInput", id, map[string]any{
			"threadId": "thread", "turnId": "turn", "isBlocking": blocking, "questions": "PRIVATE PROMPT",
		}, at)
	}
	request(1, true, 3000)
	request(2, false, 4000)
	activityEvent(r, "connection:1", "item/permissions/requestApproval", 3, map[string]any{"threadId": "thread", "turnId": "turn", "permissions": "PRIVATE PERMISSIONS"}, 5000)
	activityEvent(r, "connection:1", "serverRequest/resolved", nil, map[string]any{"threadId": "thread", "requestId": 1, "answers": "PRIVATE ANSWERS"}, 6000)
	snapshot := r.snapshot("thread", activityTurn("inProgress", 0), "thread", 6000)
	if snapshot.WorkingMS != 2000 || snapshot.WaitingMS != 0 || snapshot.OpenWaitingMS != 3000 || snapshot.CurrentState != "waiting" {
		t.Fatalf("overlapping wait = %#v", snapshot)
	}
	activityEvent(r, "connection:1", "serverRequest/resolved", nil, map[string]any{"threadId": "thread", "requestId": 3}, 7000)
	activityEvent(r, "connection:1", "thread/status/changed", nil, map[string]any{"threadId": "thread", "status": map[string]any{"type": "active", "activeFlags": []string{"waitingOnUserInput"}}}, 8000)
	r.reconcile("thread", "connection:1", activityTurn("inProgress", 0), map[string]any{"type": "active", "activeFlags": []any{"waitingOnUserInput"}}, 11000, r.revision("thread"))
	snapshot = r.snapshot("thread", activityTurn("inProgress", 0), "thread", 11000)
	if snapshot.WorkingMS != 6000 || snapshot.WaitingMS != 4000 || snapshot.OpenWaitingMS != 0 || snapshot.UnclassifiedMS != 0 || !snapshot.CoverageComplete {
		t.Fatalf("nonblocking work = %#v", snapshot)
	}
	activityEvent(r, "connection:1", "turn/completed", nil, map[string]any{"threadId": "thread", "turn": map[string]any{"id": "turn", "status": "interrupted"}}, 11000)
	snapshot = r.snapshot("thread", activityTurn("interrupted", 11000), "thread", 13000)
	if snapshot.CurrentState != "idle" || snapshot.OpenWaitingMS != 2000 || snapshot.StateSinceMS != 11000 {
		t.Fatalf("idle = %#v", snapshot)
	}
	checkpointPath := filepath.Join(r.lookup("thread").path, "current.json")
	data, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "PRIVATE") {
		t.Fatalf("ledger persisted request content: %s", data)
	}
	if !strings.Contains(string(data), `"connection:1"`) || strings.Contains(string(data), `"requests"`) {
		t.Fatal("missing connection namespace or retained closed requests")
	}
	info, _ := os.Stat(checkpointPath)
	if info.Mode().Perm() != 0600 {
		t.Fatalf("mode = %v", info.Mode())
	}
	if second, err := NewActivityRecorder(path); err == nil {
		second.Close()
		t.Fatal("allowed a concurrent ledger writer")
	}
}

func TestActivityRestartAndRequestIDReuseLeaveGap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "activity.json")
	r := startActivity(t, path)
	activityEvent(r, "connection:1", "item/commandExecution/requestApproval", 1, map[string]any{"threadId": "thread", "turnId": "turn"}, 2000)
	r.reconcile("thread", "connection:1", activityTurn("inProgress", 0), map[string]any{"type": "active", "activeFlags": []any{"waitingOnApproval"}}, 4000, r.revision("thread"))
	r.Close()
	r, err := NewActivityRecorder(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	prepareActivity(t, r)
	r.connected("thread", "connection:2", 6000)
	r.seedCurrent(context.Background(), "thread", activityTurn("inProgress", 0))
	r.reconcile("thread", "connection:2", activityTurn("inProgress", 0), map[string]any{"type": "active", "activeFlags": []any{"waitingOnApproval"}}, 6000, 0)
	activityEvent(r, "connection:2", "item/commandExecution/requestApproval", 1, map[string]any{"threadId": "thread", "turnId": "turn"}, 7000)
	r.reconcile("thread", "connection:2", activityTurn("inProgress", 0), map[string]any{"type": "active", "activeFlags": []any{"waitingOnApproval"}}, 8000, r.revision("thread"))
	snapshot := r.snapshot("thread", activityTurn("inProgress", 0), "thread", 8000)
	if snapshot.WorkingMS != 1000 || snapshot.WaitingMS != 2000 || snapshot.OpenWaitingMS != 1000 || snapshot.UnclassifiedMS != 3000 {
		t.Fatalf("restart = %#v", snapshot)
	}
	r.disconnected("connection:2", "")
	snapshot = r.snapshot("thread", activityTurn("inProgress", 0), "thread", 10000)
	if snapshot.WaitingMS != 3000 || snapshot.OpenWaitingMS != 0 || snapshot.UnclassifiedMS != 5000 {
		t.Fatalf("disconnect = %#v", snapshot)
	}
	thread := r.lookup("thread")
	thread.mu.Lock()
	defer thread.mu.Unlock()
	if thread.hot.Current != nil {
		t.Fatal("disconnected request state was retained")
	}
}

func TestActivityMissingObservationAndUnknownFork(t *testing.T) {
	var r *ActivityRecorder
	turns := []TurnMetadata{{ID: "one", StartedAtMS: 1000, CompletedAtMS: 2000, Status: "completed"}, {ID: "two", StartedAtMS: 5000, CompletedAtMS: 7000, Status: "completed"}}
	snapshot := r.snapshot("thread", turns, "thread", 9000)
	if snapshot.WorkingMS != 0 || snapshot.WaitingMS != 0 || snapshot.BetweenTurnsMS != 3000 || snapshot.UnclassifiedMS != 3000 || snapshot.OpenWaitingMS != 2000 || snapshot.CoverageComplete {
		t.Fatalf("history = %#v", snapshot)
	}
	snapshot = r.snapshot("thread", nil, "sinceFork", 9000)
	if snapshot.OpenWaitingMS != 0 || snapshot.StateSinceMS != 0 || snapshot.CurrentTurnID != "" {
		t.Fatalf("empty fork = %#v", snapshot)
	}
	snapshot = r.snapshot("thread", nil, "unknown", 9000)
	if snapshot.CoverageComplete || snapshot.Scope != "unknown" {
		t.Fatalf("unknown fork = %#v", snapshot)
	}
}

func TestActivityUsesServerTurnStartWithinObservedIdleCoverage(t *testing.T) {
	r, err := NewActivityRecorder(filepath.Join(t.TempDir(), "activity.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	prepareActivity(t, r)
	r.connected("thread", "connection", 1000)
	r.reconcile("thread", "connection", nil, map[string]any{"type": "idle"}, 1000, 0)
	activityEvent(r, "connection", "turn/started", nil, map[string]any{"threadId": "thread", "turn": map[string]any{"id": "turn", "startedAt": 2, "status": "inProgress"}}, 2500)
	turns := []TurnMetadata{{ID: "turn", Status: "inProgress", StartedAtMS: 2000}}
	r.reconcile("thread", "connection", turns, map[string]any{"type": "active"}, 4000, r.revision("thread"))
	if got := r.snapshot("thread", turns, "thread", 4000); got.WorkingMS != 2000 || !got.CoverageComplete {
		t.Fatalf("covered server start = %#v", got)
	}
}

func TestActivityFailureKeepsLastDurableCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "activity.json")
	r := startActivity(t, path)
	r.reconcile("thread", "connection:1", activityTurn("inProgress", 0), map[string]any{"type": "active"}, 3000, 0)
	if err := r.flush(context.Background(), "thread"); err != nil {
		t.Fatal(err)
	}
	checkpointPath := filepath.Join(r.lookup("thread").path, "current.json")
	// Rename's target becomes a directory; the writer must stop projecting
	// coverage after this failure while the observing client stays usable.
	if err := os.Remove(checkpointPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(checkpointPath, 0700); err != nil {
		t.Fatal(err)
	}
	r.reconcile("thread", "connection:1", activityTurn("inProgress", 0), map[string]any{"type": "active"}, 5000, 0)
	snapshot := r.snapshot("thread", activityTurn("inProgress", 0), "thread", 6000)
	if snapshot.CoverageComplete || snapshot.CurrentState != "unclassified" || snapshot.WorkingMS != 2000 || snapshot.CoverageReason != "Activity recording is unavailable." {
		t.Fatalf("failed recording = %#v", snapshot)
	}
}

func TestTurnMetadataCountsDistinctActualRootItems(t *testing.T) {
	items := []any{}
	for _, kind := range []string{"agentMessage", "plan", "commandExecution", "fileChange", "mcpToolCall", "dynamicToolCall", "webSearch", "imageView", "imageGeneration", "sleep", "functionCallOutput", "subAgentActivity", "reasoning", "contextCompaction", "enteredReviewMode"} {
		items = append(items, map[string]any{"id": kind, "type": kind})
	}
	items = append(items, map[string]any{"id": "agentMessage", "type": "agentMessage"}, map[string]any{"id": "root-agent", "type": "collabAgentToolCall", "senderThreadId": "root"}, map[string]any{"id": "child-agent", "type": "collabAgentToolCall", "senderThreadId": "child"})
	metadata := turnMetadata(map[string]any{"id": "turn", "status": "inProgress", "items": items}, "root")
	if metadata.Messages != 2 || metadata.ToolCalls != 9 {
		t.Fatalf("counts = %#v", metadata)
	}
}

func TestObserverNeverAnswersSupportedOrUnsupportedRequests(t *testing.T) {
	checked := make(chan struct{})
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/resume" {
			return fmt.Errorf("expected resume: %v", request)
		}
		params := request["params"].(map[string]any)
		if len(params) != 2 || params["threadId"] != "thread" || params["excludeTurns"] != true {
			return fmt.Errorf("observer changed resume settings: %v", params)
		}
		for _, method := range []string{"item/tool/requestUserInput", "future/unsupported"} {
			if err := writeObject(connection, map[string]any{"id": method, "method": method, "params": map[string]any{"threadId": "thread", "turnId": "turn", "isBlocking": false, "questions": []any{map[string]any{"id": "q", "header": "Question", "question": "Secret", "isOther": false, "isSecret": false}}}}); err != nil {
				return err
			}
		}
		if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{}}); err != nil {
			return err
		}
		// The next permitted client frame is our read, never an automatic
		// answer or an unsupported-request error (even with an auto policy).
		request, err = readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/read" {
			return fmt.Errorf("observer answered a request: %v", request)
		}
		if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{"thread": map[string]any{"id": "thread", "cwd": "/trusted"}}}); err != nil {
			return err
		}
		close(checked)
		return nil
	})
	recorder, err := NewActivityRecorder(filepath.Join(t.TempDir(), "activity.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer recorder.Close()
	client := NewWithOptions(socket, ClientOptions{ObserverOnly: true, ActivityRecorder: recorder, DeveloperInstructions: "must not be sent", NonBlockingUserInput: &NonBlockingUserInputPolicy{HiddenGrace: time.Millisecond}})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, unsubscribe, err := client.Subscribe(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	defer unsubscribe()
	time.Sleep(20 * time.Millisecond)
	if err := client.VerifyThread(ctx, "thread", "/trusted"); err != nil {
		t.Fatal(err)
	}
	<-checked
	if err := client.Request(ctx, "turn/start", map[string]any{}, nil); err == nil {
		t.Fatal("observer allowed a turn mutation")
	}
	if len(client.Prompts("thread")) != 0 {
		t.Fatal("observer retained interactive prompts")
	}
}

func TestActivityIgnoresStaleHistoryAndClearsTerminalBlockers(t *testing.T) {
	r := startActivity(t, filepath.Join(t.TempDir(), "activity.json"))
	revision := r.revision("thread")
	activityEvent(r, "connection:1", "item/tool/requestUserInput", 1, map[string]any{"threadId": "thread", "turnId": "turn", "isBlocking": true}, 2000)
	r.reconcile("thread", "connection:1", nil, map[string]any{"type": "idle"}, 3000, revision)
	if got := r.snapshot("thread", activityTurn("inProgress", 0), "thread", 3000); got.CurrentState != "waiting" || got.OpenWaitingMS != 1000 {
		t.Fatalf("stale read = %#v", got)
	}
	activityEvent(r, "connection:1", "turn/completed", nil, map[string]any{"threadId": "thread", "turn": map[string]any{"id": "turn", "status": "failed"}}, 4000)
	if got := r.snapshot("thread", activityTurn("failed", 4000), "thread", 5000); got.CurrentState != "idle" || got.WaitingMS != 2000 {
		t.Fatalf("terminal = %#v", got)
	}
}

func writeActivityRollout(t *testing.T, path string, meta map[string]any, records ...map[string]any) int64 {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	lines := []map[string]any{{"type": "session_meta", "ordinal": 0, "payload": meta}}
	lines = append(lines, records...)
	var data []byte
	for _, line := range lines {
		encoded, _ := json.Marshal(line)
		data = append(append(data, encoded...), '\n')
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return int64(len(data))
}

func TestActivityForkCutoffAndRevertLineage(t *testing.T) {
	root := t.TempDir()
	parent := "11111111-1111-7111-8111-111111111111"
	child := "22222222-2222-7222-8222-222222222222"
	reverted := "33333333-3333-7333-8333-333333333333"
	path := func(id string) string {
		return filepath.Join(root, "sessions", "2026", "09", "12", "rollout-2026-09-12T10-00-00-"+id+".jsonl")
	}
	start := func(id string, ordinal int) map[string]any {
		return map[string]any{"type": "event_msg", "ordinal": ordinal, "payload": map[string]any{"type": "task_started", "turn_id": id}}
	}
	parentSize := writeActivityRollout(t, path(parent), map[string]any{"id": parent}, start("inherited", 1))
	base := map[string]any{"thread_id": parent, "end_ordinal_exclusive": 5, "end_byte_offset": parentSize}
	childSize := writeActivityRollout(t, path(child), map[string]any{"id": child, "forked_from_id": parent, "history_base": base}, start("own", 6))
	turns := []TurnMetadata{{ID: "inherited"}, {ID: "own"}, {ID: "after-revert"}}
	client := New("unused")
	metadata := map[string]any{"id": child, "forkedFromId": parent, "path": path(child)}
	own, scope := client.ownActivityTurns(context.Background(), metadata, turns)
	if scope != "sinceFork" || len(own) != 1 || own[0].ID != "own" {
		t.Fatalf("fork = %#v, %s", own, scope)
	}
	revertBase := map[string]any{"thread_id": child, "end_ordinal_exclusive": 8, "end_byte_offset": childSize}
	writeActivityRollout(t, path(reverted), map[string]any{"id": child, "forked_from_id": parent, "forked_from_ordinal_exclusive": 5, "history_base": revertBase}, start("after-revert", 9))
	metadata["path"] = path(reverted)
	own, scope = client.ownActivityTurns(context.Background(), metadata, turns)
	if scope != "sinceFork" || len(own) != 2 || own[0].ID != "own" {
		t.Fatalf("revert = %#v, %s", own, scope)
	}
	writeActivityRollout(t, path(reverted), map[string]any{"id": child, "forked_from_id": parent, "history_base": revertBase}, start("after-revert", 9))
	if _, scope = client.ownActivityTurns(context.Background(), metadata, turns); scope != "unknown" {
		t.Fatal("ambiguous legacy revert guessed its fork boundary")
	}
	if err := os.Remove(path(child)); err != nil {
		t.Fatal(err)
	}
	writeActivityRollout(t, path(reverted), map[string]any{"id": child, "forked_from_id": parent, "forked_from_ordinal_exclusive": 5, "history_base": revertBase}, start("after-revert", 9))
	if _, scope = client.ownActivityTurns(context.Background(), metadata, turns); scope != "unknown" {
		t.Fatal("missing fork history was accepted")
	}
}

func TestReadActivityPaginatedHistoryCacheAndRevert(t *testing.T) {
	requests := 0
	reads := 0
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for reads < 3 {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			method := request["method"]
			var result any
			switch method {
			case "thread/read":
				reads++
				path := "/trusted/rollout.jsonl"
				if reads == 3 {
					path = "/trusted/reverted.jsonl"
				}
				result = map[string]any{"thread": map[string]any{"id": "thread", "path": path, "status": map[string]any{"type": "idle"}}}
			case "thread/turns/list":
				return errors.New("unexpected turns before metadata")
			default:
				return fmt.Errorf("unexpected method %v", method)
			}
			if err := writeObject(connection, map[string]any{"id": request["id"], "result": result}); err != nil {
				return err
			}
			pageCount := 3
			if reads == 2 {
				pageCount = 1
			}
			for page := 0; page < pageCount; page++ {
				request, err := readObject(connection)
				if err != nil {
					return err
				}
				requests++
				if request["method"] != "thread/turns/list" {
					return fmt.Errorf("expected turns, got %v", request["method"])
				}
				params := request["params"].(map[string]any)
				view := "full"
				if page > 0 {
					view = "notLoaded"
				}
				if params["itemsView"] != view {
					return fmt.Errorf("itemsView = %v", params["itemsView"])
				}
				var data []any
				for n := 40 - page*20; n >= max(0, 21-page*20); n-- {
					data = append(data, map[string]any{"id": fmt.Sprintf("turn-%d", n), "status": "completed", "startedAt": 1000 + n*10, "completedAt": 1005 + n*10, "itemsView": view, "items": []any{map[string]any{"id": "message", "type": "agentMessage", "text": "Hello"}}})
				}
				result := map[string]any{"data": data}
				if page < 2 {
					result["nextCursor"] = fmt.Sprintf("page-%d", page+1)
				}
				if err := writeObject(connection, map[string]any{"id": request["id"], "result": result}); err != nil {
					return err
				}
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	client.retainTurnHistory("thread", true)
	defer client.releaseTurnHistory("thread", true)
	for range 3 {
		snapshot, err := client.ReadActivity(context.Background(), "thread")
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.BetweenTurnsMS != 200000 || snapshot.UnclassifiedMS != 205000 || snapshot.Messages != 1 {
			t.Fatalf("history = %#v", snapshot)
		}
	}
	if requests != 7 {
		t.Fatalf("turn page requests = %d", requests)
	}
}

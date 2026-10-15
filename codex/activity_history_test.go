package codex

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type historyFixture struct {
	path   string
	items  []map[string]any
	status string
	reads  int
}

func TestHistoryLargeConversationUsesSmallMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("large-payload regression")
	}
	f := newHistoryFixture(t)
	output := strings.Repeat("x", 1<<20)
	for i := range 70 {
		f.items = append(f.items, map[string]any{"id": fmt.Sprint(i), "type": "commandExecution", "status": "completed", "aggregatedOutput": output})
	}
	c := NewWithOptions("unused", ClientOptions{HistoryReader: f})
	defer c.Close()
	value, err := c.ReadActivity(context.Background(), "root")
	if err != nil || !value.CountsComplete || value.TotalToolCalls != 70 {
		t.Fatalf("large history %#v: %v", value, err)
	}
	if f.reads != 3 {
		t.Fatalf("large history pages: %d", f.reads)
	}
}

func TestHistoryAggregateHasNoTotalTurnCeiling(t *testing.T) {
	turns := make([]TurnMetadata, 100001)
	for i := range turns {
		turns[i] = TurnMetadata{ID: fmt.Sprint(i), Status: "completed", StartedAtMS: int64(i+1) * 1000, CompletedAtMS: int64(i+2) * 1000, CountsKnown: true, ToolCalls: 1}
	}
	var recorder *ActivityRecorder
	var prefix activityPrefix
	first := recorder.snapshotContextCached(context.Background(), "root", turns, "thread", 200000000, nil, &prefix)
	if first.TotalToolCalls != len(turns) || !first.CountsComplete || prefix.count != len(turns)-2 {
		t.Fatalf("large aggregate: %#v, prefix %d", first, prefix.count)
	}
	turns = append(turns, TurnMetadata{ID: "next", Status: "completed", StartedAtMS: 100003000, CompletedAtMS: 100004000, CountsKnown: true, ToolCalls: 1})
	next := recorder.snapshotContextCached(context.Background(), "root", turns, "thread", 200000000, nil, &prefix)
	if next.TotalToolCalls != len(turns) || prefix.count != len(turns)-2 {
		t.Fatalf("incremental aggregate: %#v, prefix %d", next, prefix.count)
	}
}

func fixtureResult(result, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, result)
}

func (f *historyFixture) Request(_ context.Context, method string, raw any, result any) error {
	params := raw.(map[string]any)
	switch method {
	case "thread/read":
		return fixtureResult(result, map[string]any{"thread": map[string]any{"id": "root", "path": f.path, "cwd": filepath.Dir(f.path), "status": map[string]any{"type": "idle"}, "createdAt": 1}})
	case "thread/turns/list":
		if params["itemsView"] != "notLoaded" {
			return fmt.Errorf("full turn read: %v", params)
		}
		return fixtureResult(result, map[string]any{"data": []any{map[string]any{"id": "turn", "status": f.status, "startedAt": 2, "completedAt": 3}}, "nextCursor": nil})
	case "thread/items/list":
		f.reads++
		start := 0
		switch cursor := params["cursor"].(type) {
		case string:
			if _, err := fmt.Sscanf(cursor, "page-%d", &start); err != nil {
				return err
			}
		case map[string]any:
			id := cursor["itemId"]
			for i, item := range f.items {
				if item["id"] == id {
					start = i + 1
					break
				}
			}
		}
		limit := params["limit"].(int)
		if limit > 32 {
			return fmt.Errorf("oversized request: %d", limit)
		}
		end := min(len(f.items), start+limit)
		rows := []any{}
		for _, item := range f.items[start:end] {
			rows = append(rows, map[string]any{"turnId": "turn", "item": item})
		}
		var next any
		if end < len(f.items) {
			next = fmt.Sprintf("page-%d", end)
		}
		return fixtureResult(result, map[string]any{"data": rows, "nextCursor": next})
	default:
		return fmt.Errorf("unexpected history RPC: %s", method)
	}
}

func newHistoryFixture(t *testing.T) *historyFixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	if err := os.WriteFile(path, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return &historyFixture{path: path, status: "completed"}
}

func TestBoundHistoryCountsOnceWithoutRetainingOutput(t *testing.T) {
	f := newHistoryFixture(t)
	for i := range 70 {
		f.items = append(f.items, map[string]any{"id": fmt.Sprint(i), "type": "commandExecution", "status": "completed", "aggregatedOutput": "should not survive the page"})
	}
	c := NewWithOptions("unused", ClientOptions{HistoryReader: f})
	defer c.Close()
	for range 2 {
		snapshot, err := c.ReadActivity(context.Background(), "root")
		if err != nil || snapshot.TotalToolCalls != 70 || !snapshot.CountsComplete {
			t.Fatalf("snapshot %#v: %v", snapshot, err)
		}
	}
	if f.reads != 3 {
		t.Fatalf("completed items reread: %d", f.reads)
	}
	cache := c.turnHistory["root"].activity
	if len(cache.turns["turn"].tail) != 0 || cache.turns["turn"].prefix.ToolCalls != 70 {
		t.Fatal("completed output was retained or counts lost")
	}
}

func TestBoundHistoryWithoutRolloutReturnsUnavailable(t *testing.T) {
	f := newHistoryFixture(t)
	f.path = ""
	c := NewWithOptions("unused", ClientOptions{HistoryReader: f})
	defer c.Close()
	if _, err := c.ReadActivity(context.Background(), "root"); err == nil || !strings.Contains(err.Error(), "not materialized") {
		t.Fatalf("unmaterialized activity: %v", err)
	}
}

func TestBoundHistoryRejectedPageDoesNotPoisonRetry(t *testing.T) {
	f := newHistoryFixture(t)
	phase := 0
	reader := historyRequestFunc(func(ctx context.Context, method string, params, result any) error {
		switch method {
		case "thread/turns/list":
			old := map[string]any{"id": "turn", "status": "completed"}
			rows := []any{old}
			if phase > 0 {
				if phase == 1 {
					old = map[string]any{"id": "turn"} // Invalid after a valid new shell.
				}
				rows = []any{map[string]any{"id": "new", "status": "completed"}, old}
			}
			return fixtureResult(result, map[string]any{"data": rows, "nextCursor": nil})
		case "thread/items/list":
			id := params.(map[string]any)["turnId"].(string)
			return fixtureResult(result, map[string]any{"data": []any{map[string]any{"turnId": id, "item": map[string]any{"id": id + "-tool", "type": "commandExecution", "status": "completed"}}}, "nextCursor": nil})
		default:
			return f.Request(ctx, method, params, result)
		}
	})
	c := NewWithOptions("unused", ClientOptions{HistoryReader: reader})
	defer c.Close()
	first, err := c.ReadActivity(context.Background(), "root")
	if err != nil || first.TotalToolCalls != 1 {
		t.Fatalf("initial activity %#v: %v", first, err)
	}
	phase = 1
	if _, err := c.ReadActivity(context.Background(), "root"); err == nil {
		t.Fatal("accepted malformed head page")
	}
	phase = 2
	retry, err := c.ReadActivity(context.Background(), "root")
	if err != nil || retry.TotalToolCalls != 2 || !retry.CountsComplete {
		t.Fatalf("retried activity %#v: %v", retry, err)
	}
}

func TestBoundRecentHistoryKeepsFailureAfterTurnItems(t *testing.T) {
	f := newHistoryFixture(t)
	f.status = "failed"
	f.items = []map[string]any{{"id": "message", "type": "userMessage", "content": []any{map[string]any{"type": "text", "text": "hello"}}}}
	c := NewWithOptions("unused", ClientOptions{HistoryReader: f})
	defer c.Close()
	value, err := c.ReadThread(context.Background(), "root")
	if err != nil || len(value.Entries) != 2 || value.Entries[0].Kind == "error" || value.Entries[1].Kind != "error" {
		t.Fatalf("failed turn transcript %#v: %v", value, err)
	}
}

func TestBoundHistoryColdBackfillAndCancelledRetry(t *testing.T) {
	f := newHistoryFixture(t)
	for i := range 400 {
		f.items = append(f.items, map[string]any{"id": fmt.Sprint(i), "type": "commandExecution", "status": "completed"})
	}
	fail := true
	reader := historyRequestFunc(func(ctx context.Context, method string, params, result any) error {
		if method == "thread/items/list" && f.reads == 1 && fail {
			fail = false
			return context.Canceled
		}
		return f.Request(ctx, method, params, result)
	})
	c := NewWithOptions("unused", ClientOptions{HistoryReader: reader})
	defer c.Close()
	c.retainTurnHistory("root", true)
	defer c.releaseTurnHistory("root", true)
	if _, err := c.ReadActivity(context.Background(), "root"); err != context.Canceled {
		t.Fatalf("cancelled backfill: %v", err)
	}
	partial, err := c.ReadActivity(context.Background(), "root")
	if err != nil || !partial.Refreshing || partial.CountsComplete || partial.TotalToolCalls != 288 {
		t.Fatalf("resumed partial history %#v: %v", partial, err)
	}
	complete, err := c.ReadActivity(context.Background(), "root")
	if err != nil || complete.Refreshing || !complete.CountsComplete || complete.TotalToolCalls != 400 || f.reads != 13 {
		t.Fatalf("completed backfill %#v (%d reads): %v", complete, f.reads, err)
	}
}

func TestBoundHistoryResetsCountsAfterRolloutRevert(t *testing.T) {
	f := newHistoryFixture(t)
	if err := os.WriteFile(f.path, []byte("{}\n{\"old\":true}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	f.items = []map[string]any{{"id": "old", "type": "commandExecution", "status": "completed"}}
	c := NewWithOptions("unused", ClientOptions{HistoryReader: f})
	defer c.Close()
	old, err := c.ReadActivity(context.Background(), "root")
	if err != nil || old.TotalToolCalls != 1 {
		t.Fatalf("initial history %#v: %v", old, err)
	}
	if err := os.WriteFile(f.path, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	f.items = []map[string]any{{"id": "new", "type": "userMessage"}}
	reverted, err := c.ReadActivity(context.Background(), "root")
	if err != nil || reverted.TotalToolCalls != 0 || reverted.ReceivedMessages != 1 || !reverted.CountsComplete {
		t.Fatalf("reverted history %#v: %v", reverted, err)
	}
}

func TestBoundHistoryReconcilesMutableItemsWithoutDoubleCounting(t *testing.T) {
	f := newHistoryFixture(t)
	f.status = "inProgress"
	f.items = []map[string]any{{"id": "message", "type": "userMessage"}, {"id": "wait", "type": "collabAgentToolCall", "tool": "wait", "senderThreadId": "root", "status": "inProgress"}}
	c := NewWithOptions("unused", ClientOptions{HistoryReader: f})
	defer c.Close()
	first, err := c.ReadActivity(context.Background(), "root")
	if err != nil || first.TotalToolCalls != 1 || first.ReceivedMessages != 1 {
		t.Fatalf("initial snapshot %#v: %v", first, err)
	}
	f.items[1]["status"] = "completed"
	f.items = append(f.items, map[string]any{"id": "answer", "type": "agentMessage"})
	f.status = "completed"
	next, err := c.ReadActivity(context.Background(), "root")
	if err != nil || next.TotalToolCalls != 1 || next.ReceivedMessages != 1 || next.SentMessages != 1 || !next.CountsComplete {
		t.Fatalf("updated snapshot %#v: %v", next, err)
	}
}

type historyRequestFunc func(context.Context, string, any, any) error

func (f historyRequestFunc) Request(ctx context.Context, m string, p, r any) error {
	return f(ctx, m, p, r)
}

func TestBoundHistoryAdaptsWithoutConsumingCursor(t *testing.T) {
	limits := []int{}
	reader := historyRequestFunc(func(_ context.Context, _ string, p, r any) error {
		values := p.(map[string]any)
		if values["cursor"] != "saved" {
			t.Fatal("retry consumed cursor")
		}
		limit := values["limit"].(int)
		limits = append(limits, limit)
		if limit > 2 {
			return fmt.Errorf("websocket: message too big: read limited at 67108865 bytes")
		}
		return fixtureResult(r, map[string]any{"data": []any{}, "nextCursor": nil})
	})
	c := NewWithOptions("unused", ClientOptions{HistoryReader: reader})
	defer c.Close()
	var result map[string]any
	for range 2 {
		if err := c.itemPage(context.Background(), map[string]any{"limit": 32, "cursor": "saved"}, &result); err != nil {
			t.Fatal(err)
		}
	}
	if fmt.Sprint(limits) != "[32 16 8 4 2 2]" {
		t.Fatalf("page limits %v", limits)
	}
}

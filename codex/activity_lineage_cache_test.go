package codex

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBoundForkProofRunsInBackgroundAndReadsAppends(t *testing.T) {
	root := t.TempDir()
	parent := "11111111-1111-7111-8111-111111111111"
	child := "22222222-2222-7222-8222-222222222222"
	path := func(id string) string { return filepath.Join(root, "sessions", "rollout-"+id+".jsonl") }
	record := func(id string, n int) map[string]any {
		return map[string]any{"type": "event_msg", "ordinal": n, "payload": map[string]any{"type": "task_started", "turn_id": id}}
	}
	size := writeActivityRollout(t, path(parent), map[string]any{"id": parent}, record("inherited", 1))
	writeActivityRollout(t, path(child), map[string]any{"id": child, "forked_from_id": parent, "history_base": map[string]any{"thread_id": parent, "end_ordinal_exclusive": 5, "end_byte_offset": size}}, record("own", 6))
	c := New("unused")
	defer c.Close()
	history := &activityHistory{}
	metadata := map[string]any{"id": child, "forkedFromId": parent, "path": path(child)}
	turns := []TurnMetadata{{ID: "inherited"}, {ID: "own"}, {ID: "appended"}}
	c.ownPagedActivityTurns(context.Background(), metadata, turns, history)
	if history.lineageJob == nil {
		t.Fatal("fork proof did not start")
	}
	select {
	case <-history.lineageJob.done:
	case <-time.After(time.Second):
		t.Fatal("fork proof did not finish")
	}
	own, scope := c.ownPagedActivityTurns(context.Background(), metadata, turns, history)
	if scope != "sinceFork" || len(own) != 1 || own[0].ID != "own" {
		t.Fatalf("fork proof %#v %s", own, scope)
	}
	body, _ := json.Marshal(record("appended", 7))
	file, err := os.OpenFile(path(child), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = file.Write(append(body, '\n'))
	file.Close()
	if err != nil {
		t.Fatal(err)
	}
	own, scope = c.ownPagedActivityTurns(context.Background(), metadata, turns, history)
	if scope != "sinceFork" || len(own) != 2 || own[1].ID != "appended" {
		t.Fatalf("append proof %#v %s", own, scope)
	}
	// Reverting the source must discard the previously proved appended turn.
	writeActivityRollout(t, path(child), map[string]any{"id": child, "forked_from_id": parent, "history_base": map[string]any{"thread_id": parent, "end_ordinal_exclusive": 5, "end_byte_offset": size}}, record("own", 6))
	c.ownPagedActivityTurns(context.Background(), metadata, turns, history)
	if history.lineageJob == nil {
		t.Fatal("reverted fork proof did not restart")
	}
	select {
	case <-history.lineageJob.done:
	case <-time.After(time.Second):
		t.Fatal("reverted fork proof did not finish")
	}
	own, scope = c.ownPagedActivityTurns(context.Background(), metadata, turns, history)
	if scope != "sinceFork" || len(own) != 1 || own[0].ID != "own" {
		t.Fatalf("reverted proof %#v %s", own, scope)
	}
}

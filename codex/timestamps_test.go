package codex

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestReadThreadTimestampsSurviveReloadAndMissingRollout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	lines := []string{
		`{"type":"turn_context","payload":{"collaboration_mode":{"mode":"plan"}}}`,
		`{"timestamp":"2026-09-11T10:00:30Z","type":"event_msg","payload":{"type":"item_completed","turn_id":"turn-1","item":{"id":"message"},"started_at_ms":1789120800123,"completed_at_ms":1789120830000}}`,
		`{"timestamp":"2026-09-11T10:01:00Z","type":"event_msg","payload":{"type":"item_completed","turn_id":"turn-2","item":{"id":"message"},"completed_at_ms":1789120860000}}`,
		`{"timestamp":"2026-09-11T10:02:00+00:00","type":"event_msg","payload":{"type":"item_completed","turn_id":"turn-2","item":{"id":"plan"}}}`,
		`{"timestamp":"2026-09-11T10:02:01Z","type":"event_msg","payload":{"type":"item_completed","turn_id":"unrelated","item":{"id":"message"},"started_at_ms":1}}`,
		`{"type":"event_msg","payload":`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		t.Fatal(err)
	}
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for {
			request, err := readObject(connection)
			if err != nil {
				return nil
			}
			var result any
			switch request["method"] {
			case "thread/read":
				result = map[string]any{"thread": map[string]any{
					"id": "thread-1", "path": path, "status": "idle",
				}}
			case "thread/turns/list":
				result = map[string]any{"data": []any{
					map[string]any{"id": "turn-2", "startedAt": 1789120850, "completedAt": 1789120930,
						"status": "failed", "error": map[string]any{"message": "Failed"},
						"items": []any{
							map[string]any{"id": "message", "type": "agentMessage", "text": "Second"},
							map[string]any{"id": "plan", "type": "plan", "text": "Plan"},
							map[string]any{"id": "missing", "type": "commandExecution", "command": "true"},
						}},
					map[string]any{"id": "turn-1", "startedAt": 1789120800,
						"items": []any{map[string]any{"id": "message", "type": "userMessage", "content": []any{
							map[string]any{"type": "text", "text": "First"},
						}}}},
				}}
			default:
				return errors.New("unexpected request")
			}
			if err := writeObject(connection, map[string]any{"id": request["id"], "result": result}); err != nil {
				return err
			}
		}
	})
	want := []string{
		"2026-09-11T10:00:00.123Z", "2026-09-11T10:01:00Z", "2026-09-11T10:02:00Z",
		"2026-09-11T10:00:50Z", "2026-09-11T10:02:10Z",
	}
	for attempt := range 2 {
		client := newTestClient(socket)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		transcript, err := client.ReadThread(ctx, "thread-1")
		cancel()
		client.Close()
		if err != nil {
			t.Fatal(err)
		}
		if transcript.CollaborationMode != "plan" || len(transcript.Entries) != len(want) {
			t.Fatalf("reload %d transcript = %#v", attempt, transcript)
		}
		for index, timestamp := range want {
			entry := transcript.Entries[index]
			if entry.Timestamp != timestamp || entry.TimestampApproximate != (index >= 3) {
				t.Errorf("reload %d entry %d = %#v", attempt, index, entry)
			}
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	client := newTestClient(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	transcript, err := client.ReadThread(ctx, "thread-1")
	if err != nil || len(transcript.Entries) != len(want) {
		t.Fatalf("missing rollout broke transcript: %#v, %v", transcript, err)
	}
	for _, entry := range transcript.Entries {
		if entry.Timestamp == "" || !entry.TimestampApproximate {
			t.Errorf("missing rollout entry = %#v", entry)
		}
	}
}

func TestLiveItemTimestampKeepsStartThroughCompletion(t *testing.T) {
	client := newTestClient("/tmp/not-connected.sock")
	defer client.Close()
	client.watched["thread-1"] = 1
	client.retainTurnHistory("thread-1", true)
	client.observeItemTimestamp("item/started", json.RawMessage(
		`{"threadId":"thread-1","turnId":"turn-1","item":{"id":"message"},"startedAtMs":1789120800123}`,
	))
	client.observeItemTimestamp("item/completed", json.RawMessage(
		`{"threadId":"thread-1","turnId":"turn-1","item":{"id":"message"},"completedAtMs":1789120830000}`,
	))
	entries := []TranscriptEntry{{TurnID: "turn-1", ItemID: "message", TimestampApproximate: true}}
	client.applyTranscriptTimestamps("thread-1", entries, nil)
	if entries[0].Timestamp != "2026-09-11T10:00:00.123Z" || entries[0].TimestampApproximate {
		t.Fatalf("live timestamp = %#v", entries[0])
	}
	key := transcriptItemKey{"turn-1", "message"}
	client.applyTranscriptTimestamps("thread-1", entries, map[transcriptItemKey]itemTimestamps{
		key: {completedAtMS: 1789120830000},
	})
	if len(client.liveItemTimes["thread-1"].items) != 1 {
		t.Fatal("a completion-only record discarded the observed start")
	}
	client.applyTranscriptTimestamps("thread-1", entries, map[transcriptItemKey]itemTimestamps{
		key: {startedAtMS: 1789120800123, completedAtMS: 1789120830000},
	})
	if len(client.liveItemTimes["thread-1"].items) != 0 {
		t.Fatal("persisted timestamp retained a live observation")
	}
	client.observeItemTimestamp("item/completed", json.RawMessage(
		`{"threadId":"thread-1","turnId":"turn-2","item":{"id":"next"},"completedAtMs":1789120860000}`,
	))
	if client.liveItemTimes["thread-1"].turnID != "turn-2" {
		t.Fatal("live observations retained the previous turn")
	}
	entries = []TranscriptEntry{{TurnID: "turn-2", ItemID: "next"}}
	client.applyTranscriptTimestamps("thread-1", entries, nil)
	if entries[0].Timestamp != "2026-09-11T10:01:00Z" {
		t.Fatalf("live completion fallback = %#v", entries[0])
	}
	client.removeWatch("thread-1")
	client.observeItemTimestamp("item/started", json.RawMessage(
		`{"threadId":"thread-1","turnId":"turn-3","item":{"id":"late"},"startedAtMs":1789120900000}`,
	))
	if len(client.liveItemTimes) != 0 {
		t.Fatal("unwatched thread retained live observations")
	}
}

func TestReadLoopIncludesLiveTimestampBeforeRolloutPersistence(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/read" {
			return errors.New("expected thread/read")
		}
		if err := writeObject(connection, map[string]any{
			"method": "item/started", "params": map[string]any{
				"threadId": "thread-1", "turnId": "turn-1", "item": map[string]any{"id": "live"},
				"startedAtMs": 1789120800123,
			},
		}); err != nil {
			return err
		}
		if err := writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"thread": map[string]any{"id": "thread-1", "status": "active"}},
		}); err != nil {
			return err
		}
		request, err = readObject(connection)
		if err != nil {
			return err
		}
		return writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"data": []any{
				map[string]any{"id": "turn-1", "startedAt": 1789120800, "items": []any{
					map[string]any{"id": "live", "type": "agentMessage", "text": "Streaming"},
				}},
			}},
		})
	})
	client := newTestClient(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	client.watchedMu.Lock()
	client.watched["thread-1"] = 1
	client.retainTurnHistory("thread-1", true)
	client.watchedMu.Unlock()
	transcript, err := client.ReadThread(ctx, "thread-1")
	if err != nil || len(transcript.Entries) != 1 ||
		transcript.Entries[0].Timestamp != "2026-09-11T10:00:00.123Z" || transcript.Entries[0].TimestampApproximate {
		t.Fatalf("live transcript = %#v, %v", transcript, err)
	}
}

func TestTurnTimestampCanUseCompletionWhenStartIsUnavailable(t *testing.T) {
	entries := transcriptEntries(map[string]any{
		"id": "old", "completedAt": float64(1789120800),
		"items": []any{map[string]any{"id": "item", "type": "agentMessage", "text": "Old"}},
	})
	if entries[0].Timestamp != "2026-09-11T10:00:00Z" || !entries[0].TimestampApproximate {
		t.Fatalf("completion-only turn = %#v", entries[0])
	}
}

func TestTranscriptWithoutTimeOmitsTimestampFields(t *testing.T) {
	entries := transcriptEntries(map[string]any{
		"id": "old", "items": []any{map[string]any{"id": "item", "type": "agentMessage", "text": "Old"}},
	})
	encoded, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "timestamp") {
		t.Fatalf("unknown time was fabricated: %s", encoded)
	}
}

func TestRolloutTimestampReadStaysWithinExistingTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	old := `{"timestamp":"2026-09-10T10:00:00Z","type":"event_msg","payload":{"type":"item_completed","turn_id":"old","item":{"id":"item"}}}` + "\n"
	if _, err := file.WriteString(old); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Seek(readLimit+100, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("\n" + `{"type":"turn_context","payload":{"collaboration_mode":{"mode":"plan"}}}` + "\n" +
		`{"timestamp":"2026-09-11T10:00:00Z","type":"event_msg","payload":{"type":"item_completed","turn_id":"new","item":{"id":"item"}}}` + "\n"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	oldKey, newKey := transcriptItemKey{"old", "item"}, transcriptItemKey{"new", "item"}
	metadata, err := readRolloutMetadata(path, map[transcriptItemKey]bool{oldKey: true, newKey: true})
	if err != nil || metadata.mode != "plan" || len(metadata.itemTimes) != 1 ||
		metadata.itemTimes[newKey].timestamp() != "2026-09-11T10:00:00Z" {
		t.Fatalf("bounded rollout metadata = %#v, %v", metadata, err)
	}
}

package codex

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Isolate pagination in tests while using the same ownership lifetime as ReadActivity.
func (c *Client) readAllTurns(ctx context.Context, threadID, firstView, rolloutPath string) ([]map[string]any, error) {
	cache := c.retainTurnHistory(threadID, false)
	defer c.releaseTurnHistory(threadID, false)
	return c.readTurnHistory(ctx, threadID, firstView, rolloutPath, cache)
}

func TestReadThreadDoesNotBackfillActivityHistory(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/read" {
			return fmt.Errorf("metadata request = %v", request)
		}
		if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{"thread": map[string]any{"id": "thread"}}}); err != nil {
			return err
		}
		request, err = readObject(connection)
		if err != nil {
			return err
		}
		params := request["params"].(map[string]any)
		if request["method"] != "thread/turns/list" || params["limit"] != float64(20) || params["itemsView"] != "full" || params["cursor"] != nil {
			return fmt.Errorf("recent page = %v", request)
		}
		return writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{"data": []any{map[string]any{"id": "recent", "items": []any{map[string]any{"id": "message", "type": "agentMessage", "text": "Recent answer"}}}}, "nextCursor": "must-not-fetch"}})
	})
	client := New(socket)
	defer client.Close()
	transcript, err := client.ReadThread(context.Background(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(transcript.Entries) != 1 || transcript.Entries[0].Text != "Recent answer" {
		t.Fatalf("transcript = %#v", transcript)
	}
}

func TestActivityHistoryBackfillDoesNotBlockAnotherThread(t *testing.T) {
	slowStarted, release := make(chan struct{}), make(chan struct{})
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		slow, err := readObject(connection)
		if err != nil {
			return err
		}
		if slow["params"].(map[string]any)["threadId"] != "slow" {
			return fmt.Errorf("first request = %v", slow)
		}
		close(slowStarted)
		fast, err := readObject(connection)
		if err != nil {
			return err
		}
		if fast["params"].(map[string]any)["threadId"] != "fast" {
			return fmt.Errorf("second request = %v", fast)
		}
		if err := writeObject(connection, map[string]any{"id": fast["id"], "result": map[string]any{"data": []any{}}}); err != nil {
			return err
		}
		<-release
		return writeObject(connection, map[string]any{"id": slow["id"], "result": map[string]any{"data": []any{}}})
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	slowDone := make(chan error, 1)
	go func() { _, err := client.readAllTurns(ctx, "slow", "full", "/trusted/slow"); slowDone <- err }()
	select {
	case <-slowStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	fastCtx, fastCancel := context.WithTimeout(ctx, time.Second)
	defer fastCancel()
	_, err := client.readAllTurns(fastCtx, "fast", "full", "/trusted/fast")
	close(release)
	if err != nil {
		t.Fatalf("unrelated history was blocked: %v", err)
	}
	if err := <-slowDone; err != nil {
		t.Fatal(err)
	}
}

func TestActivityHistoryBackfillResumesAfterCancellation(t *testing.T) {
	interrupted := make(chan struct{})
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		// The third page does not answer before the caller's context is cancelled.
		// A subsequent read refreshes the recent page and resumes at that third page.
		for step := 0; step < 5; step++ {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			params := request["params"].(map[string]any)
			expected := []any{nil, "older", "oldest", nil, "oldest"}[step]
			if params["cursor"] != expected {
				return fmt.Errorf("step %d cursor=%v wanted=%v", step, params["cursor"], expected)
			}
			if step == 2 {
				close(interrupted)
				continue
			}
			numbers, next := []int{4, 3}, "older"
			if step == 1 {
				numbers, next = []int{2}, "oldest"
			}
			if step == 4 {
				numbers, next = []int{1, 0}, ""
			}
			data := make([]any, 0, len(numbers))
			for _, n := range numbers {
				data = append(data, map[string]any{"id": fmt.Sprintf("turn-%d", n), "status": "completed", "startedAt": n + 1, "completedAt": n + 2, "itemsView": params["itemsView"]})
			}
			result := map[string]any{"data": data}
			if next != "" {
				result["nextCursor"] = next
			}
			if err := writeObject(connection, map[string]any{"id": request["id"], "result": result}); err != nil {
				return err
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstDone := make(chan error, 1)
	go func() { _, err := client.readAllTurns(ctx, "thread", "full", "/trusted/rollout"); firstDone <- err }()
	select {
	case <-interrupted:
	case <-time.After(3 * time.Second):
		t.Fatal("third page was not requested")
	}
	cancel()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled history = %v", err)
	}
	nextCtx, nextCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer nextCancel()
	turns, err := client.readAllTurns(nextCtx, "thread", "full", "/trusted/rollout")
	if err != nil {
		t.Fatal(err)
	}
	if len(turns) != 5 || stringValue(turns[0]["id"]) != "turn-0" || stringValue(turns[4]["id"]) != "turn-4" {
		t.Fatalf("resumed history = %#v", turns)
	}
}

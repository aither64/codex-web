package codex

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestHistoryOversizedReplyDoesNotDisconnectLiveWatch(t *testing.T) {
	if testing.Short() {
		t.Skip("large transport regression")
	}
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		defer connection.CloseNow()
		if err := handshake(connection); err != nil {
			return err
		}
		for {
			request, err := readObject(connection)
			if err != nil {
				return nil
			}
			if request["method"] == "thread/items/list" {
				// Deliberate read-limit failure. The peer is expected to close mid-write.
				_ = writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{"output": strings.Repeat("x", readLimit+1)}})
				return nil
			}
			result := map[string]any{}
			if request["method"] == "thread/read" {
				result["thread"] = map[string]any{"id": "root", "cwd": "/workspace"}
			}
			if err := writeObject(connection, map[string]any{"id": request["id"], "result": result}); err != nil {
				return nil
			}
		}
	})
	pool := NewHistoryPool(socket)
	defer pool.Close()
	live := NewWithOptions(socket, ClientOptions{ObserverOnly: true, HistoryReader: pool})
	defer live.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, unsubscribe, err := live.Subscribe(ctx, "root")
	if err != nil {
		t.Fatal(err)
	}
	defer unsubscribe()
	live.connectionMu.Lock()
	generation := live.generation
	live.connectionMu.Unlock()
	var result map[string]any
	if err := pool.Request(ctx, "thread/resume", map[string]any{"threadId": "root"}, &result); err == nil {
		t.Fatal("history transport admitted a mutation")
	}
	if err := pool.Request(ctx, "thread/items/list", map[string]any{"threadId": "root", "limit": 1}, &result); !historySizeError(err) {
		t.Fatalf("oversized history: %v", err)
	}
	if err := live.VerifyThread(ctx, "root", "/workspace"); err != nil {
		t.Fatalf("history failure lost live connection: %v", err)
	}
	live.connectionMu.Lock()
	same := live.generation == generation && live.connection != nil
	live.connectionMu.Unlock()
	if !same {
		t.Fatal("history overflow reconnected the watched transport")
	}
}

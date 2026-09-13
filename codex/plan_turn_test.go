package codex

import (
	"context"
	"fmt"
	"testing"

	"github.com/coder/websocket"
)

func TestReadThreadLatestTurnIncludesEmptyTurns(t *testing.T) {
	for _, status := range []string{"completed", "interrupted", "failed", "inProgress"} {
		t.Run(status, func(t *testing.T) {
			socket := serveUnixWebsocket(t, func(conn *websocket.Conn) error {
				if err := handshake(conn); err != nil {
					return err
				}
				for _, method := range []string{"thread/read", "thread/turns/list"} {
					request, err := readObject(conn)
					if err != nil {
						return err
					}
					if request["method"] != method {
						return fmt.Errorf("method = %v, want %s", request["method"], method)
					}
					result := map[string]any{"thread": map[string]any{"id": "thread-1", "status": "idle"}}
					if method == "thread/turns/list" {
						result = map[string]any{"data": []any{
							map[string]any{"id": "latest", "status": status, "items": []any{}},
							map[string]any{"id": "old", "status": "completed", "items": []any{map[string]any{"id": "plan", "type": "plan", "text": "Earlier plan"}}},
						}}
					}
					if err := writeObject(conn, map[string]any{"id": request["id"], "result": result}); err != nil {
						return err
					}
				}
				return nil
			})
			client := newTestClient(socket)
			defer client.Close()
			transcript, err := client.ReadThread(context.Background(), "thread-1")
			if err != nil {
				t.Fatal(err)
			}
			if transcript.LatestTurnID != "latest" || len(transcript.Entries) == 0 || transcript.Entries[0].TurnID != "old" {
				t.Fatalf("transcript = %#v", transcript)
			}
		})
	}
}

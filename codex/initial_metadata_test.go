package codex

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestEnsureInitialMessageWithPersistedMetadata(t *testing.T) {
	tests := []struct {
		name   string
		allow  bool
		change func(map[string]any)
		page   map[string]any
		want   string
	}{
		{name: "fresh grant", allow: true},
		{name: "uncertain retry", want: "no initial user request"},
		{name: "active thread", allow: true, change: func(m map[string]any) { m["status"] = map[string]any{"type": "active"} }, want: "fresh idle"},
		{name: "fork", allow: true, change: func(m map[string]any) { m["forkedFromId"] = "parent" }, want: "fresh idle"},
		{name: "wrong cwd", allow: true, change: func(m map[string]any) { m["cwd"] = "/elsewhere" }, want: "fresh idle"},
		{name: "wrong source", allow: true, change: func(m map[string]any) { m["source"] = "cli" }, want: "fresh idle"},
		{name: "ephemeral", allow: true, change: func(m map[string]any) { m["ephemeral"] = true }, want: "fresh idle"},
		{name: "missing status", allow: true, change: func(m map[string]any) { delete(m, "status") }, want: "fresh idle"},
		{name: "missing data", allow: true, page: map[string]any{}, want: "no data"},
		{name: "empty page with cursor", allow: true, page: map[string]any{"data": []any{}, "nextCursor": "more"}, want: "no initial user request"},
		{name: "itemless turn", allow: true, page: map[string]any{"data": []any{map[string]any{"items": []any{}}}}, want: "no initial user request"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rollout := filepath.Join(t.TempDir(), "rollout.jsonl")
			if err := os.WriteFile(rollout, []byte("{\"type\":\"session_meta\"}\n"), 0600); err != nil {
				t.Fatal(err)
			}
			var starts atomic.Int32
			reads := 0
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				for {
					request, err := readObject(connection)
					if err != nil {
						return nil
					}
					result := map[string]any{}
					switch request["method"] {
					case "thread/read":
						reads++
						m := freshThreadMetadata("thread-1", "/workspace/work/example", rollout)
						if reads == 2 && test.change != nil {
							test.change(m)
						}
						result["thread"] = m
					case "thread/resume":
					case "thread/turns/list":
						if starts.Load() == 0 {
							result = test.page
							if result == nil {
								result = map[string]any{"data": []any{}}
							}
						} else {
							result["data"] = []any{map[string]any{"items": []any{map[string]any{"type": "userMessage", "content": []any{map[string]any{"type": "text", "text": "initial goal"}}}}}}
						}
					case "turn/start":
						starts.Add(1)
						params := request["params"].(map[string]any)
						if params["model"] != "model-1" || params["effort"] != "high" {
							return fmt.Errorf("initial options missing: %#v", params)
						}
						if err := additionalContextError(params); err != nil {
							return err
						}
					default:
						return fmt.Errorf("unexpected request %v", request["method"])
					}
					if err := writeObject(connection, map[string]any{"id": request["id"], "result": result}); err != nil {
						return err
					}
				}
			})
			client := newTestClient(socket)
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := client.EnsureInitialMessageWithOptions(ctx, "thread-1", "/workspace/work/example", "initial goal", test.allow, testTurnOptions())
			if test.want != "" {
				if err == nil || !strings.Contains(err.Error(), test.want) {
					t.Fatalf("result = %v, want %q", err, test.want)
				}
				if starts.Load() != 0 {
					t.Fatal("unsafe initial turn started")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := client.EnsureInitialMessage(ctx, "thread-1", "/workspace/work/example", "initial goal", false); err != nil {
				t.Fatal(err)
			}
			if starts.Load() != 1 {
				t.Fatalf("started %d turns", starts.Load())
			}
		})
	}
}

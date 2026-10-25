package codex

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestSQLiteDeliveryContention(t *testing.T) {
	store := databaseTestClient(t)
	if err := store.recordSendAttempt("initial", "initial", "text", "", false); err != nil {
		t.Fatal(err)
	}
	seedRetainedSubmissionRows(t, store, 100000)
	var mutations atomic.Int32
	socket := serveUnixWebsocket(t, func(conn *websocket.Conn) error {
		defer conn.CloseNow()
		if err := handshake(conn); err != nil {
			return err
		}
		for {
			req, err := readObject(conn)
			if err != nil {
				return nil
			}
			result := map[string]any{}
			switch req["method"] {
			case "thread/resume":
			case "thread/turns/list":
				result["data"] = []any{}
			case "turn/start":
				mutations.Add(1)
				result["turn"] = map[string]any{"id": "turn"}
			default:
				return fmt.Errorf("unexpected RPC %v", req["method"])
			}
			time.Sleep(time.Millisecond)
			if err := writeObject(conn, map[string]any{"id": req["id"], "result": result}); err != nil {
				return err
			}
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	type outcome struct {
		duration time.Duration
		rows     uint64
		err      error
	}
	results := make(chan outcome, 32)
	for i := 0; i < 32; i++ {
		go func(i int) {
			client := NewWithOptions(socket, store.options)
			defer client.Close()
			started := time.Now()
			receipt, err := client.Send(ctx, fmt.Sprintf("live-%d", i), "new message", fmt.Sprintf("client-%d", i), "")
			if err == nil && receipt.ClientUserMessageID != fmt.Sprintf("client-%d", i) {
				err = fmt.Errorf("wrong receipt: %#v", receipt)
			}
			var rows uint64
			if client.submission != nil {
				rows = client.submission.rowsRead.Load()
			}
			results <- outcome{time.Since(started), rows, err}
		}(i)
	}
	var durations []time.Duration
	for i := 0; i < 32; i++ {
		result := <-results
		if result.err != nil {
			t.Errorf("send failed under contention: %v", result.err)
		}
		if result.rows > 8 {
			t.Errorf("one send loaded %d records from unrelated history", result.rows)
		}
		durations = append(durations, result.duration)
	}
	if mutations.Load() != 32 {
		t.Fatalf("native mutation count %d, want 32", mutations.Load())
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	t.Logf("32 concurrent sends with 100000 retained receipts: p95=%s max=%s", durations[30], durations[31])
}

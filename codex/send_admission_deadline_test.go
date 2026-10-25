package codex

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestCancellationDuringDurableAdmissionRemainsRetryable(t *testing.T) {
	for _, database := range []bool{false, true} {
		for _, active := range []bool{false, true} {
			t.Run(fmt.Sprintf("database=%t/active=%t", database, active), func(t *testing.T) { testCanceledDurableAdmission(t, database, active) })
		}
	}
}

func testCanceledDurableAdmission(t *testing.T, database, active bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
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
			case "thread/items/list":
				result["data"] = []any{}
			case "thread/turns/list":
				result["data"] = []any{}
				if active {
					result["data"] = []any{map[string]any{"id": "accepted", "status": "inProgress"}}
				}
			case "turn/start", "turn/steer":
				mutations.Add(1)
				result["turn"] = map[string]any{"id": "accepted"}
				result["turnId"] = "accepted"
			default:
				return fmt.Errorf("unexpected RPC %v", req["method"])
			}
			if err := writeObject(conn, map[string]any{"id": req["id"], "result": result}); err != nil {
				return err
			}
		}
	})
	options := ClientOptions{}
	if database {
		options.SubmissionLedgerPath = socket + ".submission-attempts-v3.json"
		options.SubmissionDatabasePath = filepath.Join(t.TempDir(), "attempts.sqlite")
	}
	client := NewWithOptions(socket, options)
	defer client.Close()
	canceled, cancelSend := context.WithCancel(ctx)
	defer cancelSend()
	client.options.DeliveryObserver = func(event DeliveryEvent) {
		if event.Stage == "admission" && !event.Failed {
			cancelSend()
		}
	}
	_, err := client.Send(canceled, "thread", "message", "client", "")
	var unsent *MessageNotSentError
	if !errors.As(err, &unsent) || !errors.Is(err, context.Canceled) || !unsent.Retryable {
		t.Fatalf("cancellation during admission must be definitely unsent: %v", err)
	}
	if mutations.Load() != 0 {
		t.Fatal("canceled admission reached native mutation")
	}
	attempt, found, err := client.sendAttempt("thread", "client", "message", "")
	if err != nil || !found || attempt.State != "prepared" {
		t.Fatalf("unwritten attempt must remain prepared: %#v, %v", attempt, err)
	}
	client.Close()
	restarted := NewWithOptions(socket, options)
	defer restarted.Close()
	if receipt, err := restarted.Send(ctx, "thread", "message", "client", ""); err != nil || receipt.TurnID != "accepted" || mutations.Load() != 1 {
		t.Fatalf("same-ID retry after restart: %#v, %v, mutations=%d", receipt, err, mutations.Load())
	}
}

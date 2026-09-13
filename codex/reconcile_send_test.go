package codex

import (
	"context"
	"errors"
	"fmt"
	"github.com/coder/websocket"
	"path/filepath"
	"testing"
)

func TestReconcileSendNeverSubmitsPreparedOrAbsentAttempts(t *testing.T) {
	client := newTestClient(filepath.Join(t.TempDir(), "absent-socket"))
	defer client.Close()
	for _, prepared := range []bool{false, true} {
		if prepared {
			if err := client.PrepareSend("thread-1", "message", "message-1", "plan:digest", false); err != nil {
				t.Fatal(err)
			}
		}
		if receipt, found, err := client.ReconcileSend(context.Background(), "thread-1", "message", "message-1", "plan:digest"); err != nil || found || receipt.TurnID != "" {
			t.Fatalf("prepared=%v: %#v, %v, %v", prepared, receipt, found, err)
		}
	}
	if _, _, err := client.ReconcileSend(context.Background(), "thread-1", "message", "message-1", "plan:other"); err == nil {
		t.Fatal("reused action context accepted")
	}
	if err := client.markSendSubmitting("thread-1", "message-1"); err != nil {
		t.Fatal(err)
	}
	want := SendReceipt{TurnID: "accepted", ClientUserMessageID: "message-1"}
	if err := client.markSendAccepted("thread-1", "message-1", want); err != nil {
		t.Fatal(err)
	}
	if receipt, found, err := client.ReconcileSend(context.Background(), "thread-1", "message", "message-1", "plan:digest"); err != nil || !found || receipt != want {
		t.Fatalf("accepted: %#v, %v, %v", receipt, found, err)
	}
}

func TestReconcileSendOnlyReadsHistoryForSubmittingAttempts(t *testing.T) {
	for _, observed := range []bool{false, true} {
		t.Run(fmt.Sprint(observed), func(t *testing.T) {
			socket := serveUnixWebsocket(t, func(conn *websocket.Conn) error {
				if err := handshake(conn); err != nil {
					return err
				}
				request, err := readObject(conn)
				if err != nil {
					return err
				}
				if request["method"] != "thread/items/list" {
					return fmt.Errorf("unexpected mutation: %#v", request)
				}
				data := []any{}
				if observed {
					data = append(data, map[string]any{"turnId": "accepted", "item": map[string]any{
						"id": "item-1", "type": "userMessage", "clientId": "message-1",
						"content": []any{map[string]any{"type": "text", "text": "message"}},
					}})
				}
				return writeObject(conn, map[string]any{"id": request["id"], "result": map[string]any{"data": data, "nextCursor": nil}})
			})
			client := newTestClient(socket)
			defer client.Close()
			if err := client.PrepareSend("thread-1", "message", "message-1", "plan:digest", true); err != nil {
				t.Fatal(err)
			}
			if err := client.markSendSubmitting("thread-1", "message-1"); err != nil {
				t.Fatal(err)
			}
			receipt, found, err := client.ReconcileSend(context.Background(), "thread-1", "message", "message-1", "plan:digest")
			if observed {
				if err != nil || !found || receipt.TurnID != "accepted" || !receipt.Steered {
					t.Fatalf("recovered: %#v, %v, %v", receipt, found, err)
				}
			} else {
				var unknown *UnknownSendOutcomeError
				if !errors.As(err, &unknown) || found {
					t.Fatalf("unresolved: %v, %v", found, err)
				}
			}
		})
	}
}

func TestDiscardPreparedSendRetiresOnlyExactUnsubmittedRequests(t *testing.T) {
	client := newTestClient(filepath.Join(t.TempDir(), "absent-socket"))
	defer client.Close()
	if err := client.PrepareSend("thread-1", "message", "legacy", "plan:digest", false); err != nil {
		t.Fatal(err)
	}
	if err := client.RequireSubmissionAttemptsResolved(context.Background(), "thread-1"); err == nil {
		t.Fatal("prepared attempt did not block lifecycle")
	}
	if _, err := client.DiscardPreparedSend("thread-1", "message", "legacy", "plan:another"); err == nil {
		t.Fatal("discard accepted wrong context")
	}
	if _, err := client.DiscardPreparedSend("thread-1", "different", "legacy", "plan:digest"); err == nil {
		t.Fatal("discard accepted wrong text")
	}
	for range 2 {
		if retired, err := client.DiscardPreparedSend("thread-1", "message", "legacy", "plan:digest"); err != nil || !retired {
			t.Fatalf("retire: %v, %v", retired, err)
		}
	}
	if err := client.RequireSubmissionAttemptsResolved(context.Background(), "thread-1"); err != nil {
		t.Fatalf("retired attempt blocks lifecycle: %v", err)
	}
	if err := client.PrepareSend("thread-1", "message", "submitted", "plan:digest", false); err != nil {
		t.Fatal(err)
	}
	if err := client.markSendSubmitting("thread-1", "submitted"); err != nil {
		t.Fatal(err)
	}
	if retired, err := client.DiscardPreparedSend("thread-1", "message", "submitted", "plan:digest"); err != nil || retired {
		t.Fatalf("discard submitting: %v, %v", retired, err)
	}
	if err := client.markSendAccepted("thread-1", "submitted", SendReceipt{TurnID: "accepted", ClientUserMessageID: "submitted"}); err != nil {
		t.Fatal(err)
	}
	if retired, err := client.DiscardPreparedSend("thread-1", "message", "submitted", "plan:digest"); err != nil || retired {
		t.Fatalf("discard accepted: %v, %v", retired, err)
	}
	if receipt, found, err := client.ReconcileSend(context.Background(), "thread-1", "message", "submitted", "plan:digest"); err != nil || !found || receipt.TurnID != "accepted" {
		t.Fatalf("retained receipt: %#v %v %v", receipt, found, err)
	}
}

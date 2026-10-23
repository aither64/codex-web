package codex

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestPreparedSendProofPreservesExactIntentAcrossRestart(t *testing.T) {
	for _, steered := range []bool{false, true} {
		for _, optioned := range []bool{false, true} {
			t.Run(fmt.Sprintf("steered=%v/options=%v", steered, optioned), func(t *testing.T) {
				options := TurnOptions{}
				if optioned {
					options = testTurnOptions()
					options.ThreadPolicy = ThreadPolicy{DeveloperInstructions: "Retain this policy."}
				}
				ledger := filepath.Join(t.TempDir(), "receipts.json")
				first := NewWithOptions("/missing", ClientOptions{SubmissionLedgerPath: ledger})
				if err := first.PrepareSendWithOptions("root", "Implement the plan.", "original-id", "plan:turn:digest", steered, options); err != nil {
					t.Fatal(err)
				}
				first.Close()
				client := NewWithOptions("/missing", ClientOptions{SubmissionLedgerPath: ledger})
				defer client.Close()
				before, err := os.ReadFile(ledger)
				if err != nil {
					t.Fatal(err)
				}
				for range 2 {
					prepared, err := client.IsPreparedSendWithOptions("root", "Implement the plan.", "original-id", "plan:turn:digest", options)
					if err != nil || !prepared {
						t.Fatalf("prepared proof = %v, %v", prepared, err)
					}
					if err := client.RequireSubmissionAttemptsKnownExceptPreparedSend(context.Background(), "root", "Implement the plan.", "original-id", "plan:turn:digest", options); err != nil {
						t.Fatal(err)
					}
					if attempted, err := client.SendAttemptedWithOptions(context.Background(), "root", "Implement the plan.", "original-id", "plan:turn:digest", options); err != nil || !attempted {
						t.Fatalf("existing SendAttempted semantics changed: %v, %v", attempted, err)
					}
					if receipt, found, err := client.ReconcileSendWithOptions(context.Background(), "root", "Implement the plan.", "original-id", "plan:turn:digest", options); err != nil || found || receipt != (SendReceipt{}) {
						t.Fatalf("preparation was dispatched: %#v, %v, %v", receipt, found, err)
					}
				}
				if err := client.RequireSubmissionAttemptsKnown(context.Background(), "root"); err == nil {
					t.Fatal("ordinary known check silently excluded the preparation")
				}
				gotOptions, found, err := client.OriginalSendOptions("root", "Implement the plan.", "original-id", "plan:turn:digest")
				if err != nil || !found || !reflect.DeepEqual(gotOptions, options) {
					t.Fatalf("immutable options = %#v, %v, %v", gotOptions, found, err)
				}
				normalized, err := normalizeTurnOptions(options)
				if err != nil {
					t.Fatal(err)
				}
				attempt, found, err := client.sendAttemptWithOptions("root", "original-id", "Implement the plan.", "plan:turn:digest", normalized.Digest)
				if err != nil || !found || attempt.State != "prepared" || attempt.Steered != steered {
					t.Fatalf("preparation changed: %#v, %v, %v", attempt, found, err)
				}
				after, err := os.ReadFile(ledger)
				if err != nil || !bytes.Equal(before, after) {
					t.Fatalf("read-only proofs changed ledger: %v", err)
				}
			})
		}
	}
}

func TestPreparedSendProofRejectsOtherDeliveryStates(t *testing.T) {
	for _, state := range []string{"absent", "submitting", "accepted", "queued", "executed-queue", "deleted-queue"} {
		t.Run(state, func(t *testing.T) {
			ledger := filepath.Join(t.TempDir(), "receipts.json")
			socket := "/missing"
			if state == "executed-queue" {
				socket = serveUnixWebsocket(t, func(conn *websocket.Conn) error {
					if err := handshake(conn); err != nil {
						return err
					}
					for _, method := range []string{"thread/queue/list", "thread/resume", "thread/items/list", "thread/queue/start"} {
						request, err := readObject(conn)
						if err != nil {
							return err
						}
						if request["method"] != method {
							return fmt.Errorf("unexpected queue execution RPC: %#v", request)
						}
						result := map[string]any{}
						switch method {
						case "thread/queue/list":
							result = map[string]any{"data": []any{map[string]any{"id": "queue-id", "clientUserMessageId": "id", "input": []any{map[string]any{"type": "text", "text": "message"}}}}, "nextCursor": nil}
						case "thread/items/list":
							result = map[string]any{"data": []any{}, "nextCursor": nil}
						case "thread/queue/start":
							result = map[string]any{"turn": map[string]any{"id": "started-turn", "status": "inProgress"}}
						}
						if err := writeObject(conn, map[string]any{"id": request["id"], "result": result}); err != nil {
							return err
						}
					}
					return nil
				})
			}
			client := NewWithOptions(socket, ClientOptions{SubmissionLedgerPath: ledger})
			defer client.Close()
			if state != "absent" {
				if err := client.PrepareSend("root", "message", "id", "action", false); err != nil {
					t.Fatal(err)
				}
			}
			switch state {
			case "submitting", "accepted":
				if err := client.markSendSubmitting("root", "id"); err != nil {
					t.Fatal(err)
				}
				if state == "accepted" {
					if err := client.markSendAccepted("root", "id", SendReceipt{TurnID: "turn", ClientUserMessageID: "id"}); err != nil {
						t.Fatal(err)
					}
				}
			case "queued", "executed-queue", "deleted-queue":
				// Queue execution leaves this marker in place even after the native
				// queue entry disappears. Only deletion/retirement removes both.
				if err := client.recordQueueAttempt("root", "id", "message"); err != nil {
					t.Fatal(err)
				}
				if state == "executed-queue" {
					if err := client.StartQueue(context.Background(), "root", "queue-id"); err != nil {
						t.Fatal(err)
					}
				}
				if state == "deleted-queue" {
					if err := client.clearQueueDeletionAndAttempt("root", "queue-id", "id"); err != nil {
						t.Fatal(err)
					}
				}
			}
			before, readErr := os.ReadFile(ledger)
			if readErr != nil && !os.IsNotExist(readErr) {
				t.Fatal(readErr)
			}
			if prepared, err := client.IsPreparedSendWithOptions("root", "message", "id", "action", TurnOptions{}); err != nil || prepared {
				t.Fatalf("%s granted first execution: %v, %v", state, prepared, err)
			}
			if err := client.RequireSubmissionAttemptsKnownExceptPreparedSend(context.Background(), "root", "message", "id", "action", TurnOptions{}); err == nil {
				t.Fatalf("%s granted an observation exception", state)
			}
			after, afterErr := os.ReadFile(ledger)
			if !bytes.Equal(before, after) || (os.IsNotExist(readErr) != os.IsNotExist(afterErr)) {
				t.Fatal("negative proof changed ledger")
			}
		})
	}
}

func TestPreparedSendProofBindsFullIdentity(t *testing.T) {
	options := testTurnOptions()
	options.ThreadPolicy = ThreadPolicy{DeveloperInstructions: "Original instructions."}
	for _, field := range []string{"thread", "id", "text", "action", "model", "effort", "context", "policy", "invalid-options"} {
		t.Run(field, func(t *testing.T) {
			client := newTestClient(filepath.Join(t.TempDir(), "missing.sock"))
			defer client.Close()
			if err := client.PrepareSendWithOptions("root", "message", "id", "action", false, options); err != nil {
				t.Fatal(err)
			}
			thread, text, id, action := "root", "message", "id", "action"
			changed := cloneTurnOptions(options)
			switch field {
			case "thread":
				thread = "other-thread"
			case "id":
				id = "other-id"
			case "text":
				text = "other message"
			case "action":
				action = "other-action"
			case "model":
				changed.Model = "other-model"
			case "effort":
				changed.ReasoningEffort = "low"
			case "context":
				changed.AdditionalContext["application:plan"] = AdditionalContextEntry{Kind: "application", Value: "other context"}
			case "policy":
				changed.ThreadPolicy.DeveloperInstructions = "Other instructions."
			case "invalid-options":
				changed.Model = "invalid\nmodel"
			}
			prepared, err := client.IsPreparedSendWithOptions(thread, text, id, action, changed)
			if prepared || (err == nil && field != "thread" && field != "id") {
				t.Fatalf("changed %s = %v, %v", field, prepared, err)
			}
			if err := client.RequireSubmissionAttemptsKnownExceptPreparedSend(context.Background(), thread, text, id, action, changed); err == nil {
				t.Fatalf("changed %s granted observation exception", field)
			}
			if prepared, err := client.IsPreparedSendWithOptions("root", "message", "id", "action", options); err != nil || !prepared {
				t.Fatalf("original preparation was damaged: %v, %v", prepared, err)
			}
		})
	}
}

func TestPreparedSendExceptionChecksOtherSubmissions(t *testing.T) {
	for _, other := range []string{"prepared", "submitting", "accepted", "other-thread", "queue-deletion", "known-queue", "executed-queue", "unknown-queue"} {
		t.Run(other, func(t *testing.T) {
			socket := filepath.Join(t.TempDir(), "missing.sock")
			if other == "known-queue" || other == "executed-queue" || other == "unknown-queue" {
				socket = serveUnixWebsocket(t, func(conn *websocket.Conn) error {
					if err := handshake(conn); err != nil {
						return err
					}
					for _, method := range []string{"thread/queue/list", "thread/items/list"} {
						request, err := readObject(conn)
						if err != nil {
							return err
						}
						if request["method"] != method {
							return fmt.Errorf("unexpected RPC: %#v", request)
						}
						data := []any{}
						if method == "thread/queue/list" && other == "known-queue" {
							data = append(data, map[string]any{"id": "queue", "clientUserMessageId": "other", "input": []any{map[string]any{"type": "text", "text": "other message"}}})
						}
						if method == "thread/items/list" && other == "executed-queue" {
							data = append(data, map[string]any{"turnId": "other-turn", "item": map[string]any{"id": "other-item", "type": "userMessage", "clientId": "other", "content": []any{map[string]any{"type": "text", "text": "other message"}}}})
						}
						if err := writeObject(conn, map[string]any{"id": request["id"], "result": map[string]any{"data": data, "nextCursor": nil}}); err != nil {
							return err
						}
					}
					return nil
				})
			}
			client := newTestClient(socket)
			defer client.Close()
			if err := client.PrepareSend("root", "message", "own", "plan:turn:digest", false); err != nil {
				t.Fatal(err)
			}
			thread := "root"
			if other == "other-thread" {
				thread = "member"
			}
			switch other {
			case "queue-deletion":
				if _, err := client.recordQueueDeletionAttempt("root", QueueEntry{ID: "queue", ClientUserMessageID: "other", Text: "other message"}); err != nil {
					t.Fatal(err)
				}
			case "known-queue", "executed-queue", "unknown-queue":
				if err := client.PrepareSend(thread, "other message", "other", "queue", false); err != nil {
					t.Fatal(err)
				}
				if err := client.recordQueueAttempt(thread, "other", "other message"); err != nil {
					t.Fatal(err)
				}
			default:
				if err := client.PrepareSend(thread, "other message", "other", "other-action", false); err != nil {
					t.Fatal(err)
				}
				if other == "submitting" || other == "accepted" {
					if err := client.markSendSubmitting(thread, "other"); err != nil {
						t.Fatal(err)
					}
				}
				if other == "accepted" {
					if err := client.markSendAccepted(thread, "other", SendReceipt{TurnID: "other-turn", ClientUserMessageID: "other"}); err != nil {
						t.Fatal(err)
					}
				}
			}
			before, err := os.ReadFile(client.queueLedgerPath)
			if err != nil {
				t.Fatal(err)
			}
			err = client.RequireSubmissionAttemptsKnownExceptPreparedSend(context.Background(), "root", "message", "own", "plan:turn:digest", TurnOptions{})
			wantKnown := other == "accepted" || other == "other-thread" || other == "known-queue" || other == "executed-queue"
			if (err == nil) != wantKnown {
				t.Fatalf("other %s: %v", other, err)
			}
			after, readErr := os.ReadFile(client.queueLedgerPath)
			if readErr != nil || !bytes.Equal(before, after) {
				t.Fatalf("known check changed ledger: %v", readErr)
			}
		})
	}
}

func TestPreparedSendProofReloadsAndRejectsUnreadableLedger(t *testing.T) {
	ledger := filepath.Join(t.TempDir(), "receipts.json")
	first := NewWithOptions("/missing", ClientOptions{SubmissionLedgerPath: ledger})
	second := NewWithOptions("/missing", ClientOptions{SubmissionLedgerPath: ledger})
	defer first.Close()
	defer second.Close()
	if err := first.PrepareSend("root", "message", "id", "action", false); err != nil {
		t.Fatal(err)
	}
	if prepared, err := second.IsPreparedSendWithOptions("root", "message", "id", "action", TurnOptions{}); err != nil || !prepared {
		t.Fatalf("cross-client preparation = %v, %v", prepared, err)
	}
	if err := first.markSendSubmitting("root", "id"); err != nil {
		t.Fatal(err)
	}
	if prepared, err := second.IsPreparedSendWithOptions("root", "message", "id", "action", TurnOptions{}); err != nil || prepared {
		t.Fatalf("stale preparation granted: %v, %v", prepared, err)
	}
	if err := os.WriteFile(ledger, []byte("{invalid"), 0o600); err != nil {
		t.Fatal(err)
	}
	if prepared, err := second.IsPreparedSendWithOptions("root", "message", "id", "action", TurnOptions{}); err == nil || prepared {
		t.Fatalf("unreadable ledger granted: %v, %v", prepared, err)
	}
	if err := second.RequireSubmissionAttemptsKnownExceptPreparedSend(context.Background(), "root", "message", "id", "action", TurnOptions{}); err == nil {
		t.Fatal("unreadable ledger granted observation exception")
	}
}

func TestPreparedSendExceptionHoldsQueueLockAcrossKnownCheck(t *testing.T) {
	checking := make(chan struct{})
	release := make(chan struct{})
	socket := serveUnixWebsocket(t, func(conn *websocket.Conn) error {
		if err := handshake(conn); err != nil {
			return err
		}
		for _, method := range []string{"thread/queue/list", "thread/items/list"} {
			request, err := readObject(conn)
			if err != nil {
				return err
			}
			if request["method"] != method {
				return fmt.Errorf("unexpected known-submission RPC: %#v", request)
			}
			data := []any{}
			if method == "thread/queue/list" {
				close(checking)
				<-release
				data = append(data, map[string]any{"id": "queue", "clientUserMessageId": "other", "input": []any{map[string]any{"type": "text", "text": "other message"}}})
			}
			if err := writeObject(conn, map[string]any{"id": request["id"], "result": map[string]any{"data": data, "nextCursor": nil}}); err != nil {
				return err
			}
		}
		return nil
	})
	client := newTestClient(socket)
	defer client.Close()
	if err := client.PrepareSend("root", "message", "own", "action", false); err != nil {
		t.Fatal(err)
	}
	if err := client.recordQueueAttempt("root", "other", "other message"); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		result <- client.RequireSubmissionAttemptsKnownExceptPreparedSend(ctx, "root", "message", "own", "action", TurnOptions{})
	}()
	select {
	case <-checking:
	case err := <-result:
		t.Fatalf("known check stopped before the queue barrier: %v", err)
	}
	lock := client.queueUpdateLock("root")
	releasedEarly := lock.TryLock()
	if releasedEarly {
		lock.Unlock()
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if releasedEarly {
		t.Fatal("prepared proof released the queue lock before checking other submissions")
	}
}

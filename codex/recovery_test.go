package codex

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestQueueCannotBypassEarlierSendOutcome(t *testing.T) {
	for _, state := range []string{"accepted", "submitting", "steered", "changed text"} {
		t.Run(state, func(t *testing.T) {
			client := NewWithOptions(filepath.Join(t.TempDir(), "missing.sock"), ClientOptions{SubmissionLedgerPath: filepath.Join(t.TempDir(), "receipts.json")})
			defer client.Close()
			if err := client.PrepareSend("root", "original", "message-id", "", state == "steered"); err != nil {
				t.Fatal(err)
			}
			if state == "accepted" || state == "submitting" {
				if err := client.markSendSubmitting("root", "message-id"); err != nil {
					t.Fatal(err)
				}
			}
			if state == "accepted" {
				if err := client.markSendAccepted("root", "message-id", SendReceipt{TurnID: "original-turn", ClientUserMessageID: "message-id"}); err != nil {
					t.Fatal(err)
				}
			}
			text := "original"
			if state == "changed text" {
				text = "replacement"
			}
			if _, err := client.Queue(context.Background(), "root", text, "message-id"); err == nil || !strings.Contains(err.Error(), "must be reconciled before queueing") {
				t.Fatal("earlier send reached native queue", err)
			}
			if attempted, err := client.queueAttempt("root", "message-id", text); err != nil || attempted {
				t.Fatal("refusal created a queue receipt", attempted, err)
			}
		})
	}
}

func TestImplicitWatchRestorationRechecksExecutionPermission(t *testing.T) {
	for _, observer := range []bool{false, true} {
		t.Run(fmt.Sprint(observer), func(t *testing.T) {
			var allowed atomic.Bool
			allowed.Store(true)
			resumes := make(chan struct{}, 8)
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				for {
					request, err := readObject(connection)
					if err != nil {
						return nil
					}
					if request["method"] == "thread/resume" {
						resumes <- struct{}{}
					}
					if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{}}); err != nil {
						return nil
					}
				}
			})
			client := NewWithOptions(socket, ClientOptions{ObserverOnly: observer, AllowImplicitResume: func(string) bool { return allowed.Load() }})
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, unsubscribe, err := client.Subscribe(ctx, "root")
			if err != nil {
				t.Fatal(err)
			}
			defer unsubscribe()
			<-resumes
			allowed.Store(false)
			client.connectionMu.Lock()
			connection, generation := client.connection, client.generation
			client.connectionMu.Unlock()
			client.markDisconnected(connection, generation, errors.New("fixture App Server restart"))
			if err := client.Ensure(ctx); err != nil {
				t.Fatal(err)
			}
			if err := client.resumeWatched(ctx, "root"); err == nil {
				t.Fatal("held watch resumed")
			}
			select {
			case <-resumes:
				t.Fatal("reconnection loaded held work")
			case <-time.After(50 * time.Millisecond):
			}
			allowed.Store(true)
			if err := client.resumeWatched(ctx, "root"); err != nil {
				t.Fatal(err)
			}
			select {
			case <-resumes:
			case <-ctx.Done():
				t.Fatal("explicit permission did not restore watch")
			}
		})
	}
}

func TestObservationTransportFailureIsDistinctFromRPCRefusal(t *testing.T) {
	client := New(filepath.Join(t.TempDir(), "missing.sock"))
	defer client.Close()
	var transport *TransportError
	if err := client.Ensure(context.Background()); !errors.As(err, &transport) {
		t.Fatal("connection error lost type", err)
	}
	for _, refusal := range []bool{false, true} {
		t.Run(fmt.Sprint(refusal), func(t *testing.T) {
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				request, err := readObject(connection)
				if err != nil {
					return err
				}
				if refusal {
					return writeObject(connection, map[string]any{"id": request["id"], "error": map[string]any{"code": -32600, "message": "identity refused"}})
				}
				return nil // Existing live socket, unanswered native request.
			})
			client := New(socket)
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			_, err := client.HasActiveGoal(ctx, "root")
			if err == nil || errors.As(err, &transport) == refusal {
				t.Fatal("deadline/refusal classification", err)
			}
		})
	}
}

func TestKnownQueuedSendPreservesIdentityThroughExecutionAndDeletion(t *testing.T) {
	for _, state := range []string{"queued", "started", "unknown", "changed"} {
		t.Run(state, func(t *testing.T) {
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				for {
					request, err := readObject(connection)
					if err != nil {
						return nil
					}
					data := []any{}
					switch request["method"] {
					case "thread/queue/list":
						if state == "queued" || state == "changed" {
							text := "saved message"
							if state == "changed" {
								text = "different"
							}
							data = append(data, map[string]any{"id": "queued", "clientUserMessageId": "id", "input": []any{map[string]any{"type": "text", "text": text}}})
						}
					case "thread/items/list":
						if state == "started" {
							data = append(data, map[string]any{"turnId": "turn", "item": map[string]any{"id": "item", "type": "userMessage", "clientId": "id", "content": []any{map[string]any{"type": "text", "text": "saved message"}}}})
						}
					default:
						return fmt.Errorf("proof mutated thread: %v", request["method"])
					}
					if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{"data": data, "nextCursor": nil}}); err != nil {
						return nil
					}
				}
			})
			client := newTestClient(socket)
			defer client.Close()
			options := TurnOptions{Model: "saved-model", ReasoningEffort: "low"}
			if err := client.PrepareSendWithOptions("root", "saved message", "id", "assignment", false, options); err != nil {
				t.Fatal(err)
			}
			if err := client.recordQueueAttempt("root", "id", "saved message"); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := client.RequireSubmissionAttemptsKnown(ctx, "root"); (err == nil) != (state == "queued" || state == "started") {
				t.Fatal("known", state, err)
			}
			if err := client.RequireSubmissionAttemptsResolved(ctx, "root"); (err == nil) != (state == "started") {
				t.Fatal("resolved", state, err)
			}
			if retired, err := client.DiscardPreparedSendWithOptions("root", "saved message", "id", "assignment", options); err != nil || retired {
				t.Fatal("discarded submitted identity", retired, err)
			}
			if state == "queued" || state == "started" {
				if _, found, err := client.ReconcileSendWithOptions(ctx, "root", "saved message", "id", "assignment", options); err != nil || !found {
					t.Fatal("receipt", found, err)
				}
				if _, _, err := client.ReconcileSendWithOptions(ctx, "root", "saved message", "id", "other", options); err == nil {
					t.Fatal("changed action accepted")
				}
			}
			if state == "queued" {
				if err := client.clearQueueDeletionAndAttempt("root", "queued", "id"); err != nil {
					t.Fatal(err)
				}
				if err := client.RequireSubmissionAttemptsResolved(ctx, "root"); err != nil {
					t.Fatal("deleted prepared send still blocks", err)
				}
			}
		})
	}
}

func TestGoalReadStaysColdAndValidatesIdentity(t *testing.T) {
	for _, test := range []struct {
		name            string
		goal            any
		active, failure bool
	}{
		{"none", nil, false, false},
		{"active", map[string]any{"threadId": "root", "status": "active"}, true, false},
		{"paused", map[string]any{"threadId": "root", "status": "paused"}, false, false},
		{"foreign", map[string]any{"threadId": "foreign", "status": "active"}, false, true},
		{"unknown", map[string]any{"threadId": "root", "status": "new-status"}, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				request, err := readObject(connection)
				if err != nil {
					return err
				}
				if request["method"] != "thread/goal/get" || request["params"].(map[string]any)["threadId"] != "root" {
					return fmt.Errorf("cold goal read used %v", request)
				}
				return writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{"goal": test.goal}})
			})
			client := newTestClient(socket)
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			active, err := client.HasActiveGoal(ctx, "root")
			if (err != nil) != test.failure || active != test.active {
				t.Fatalf("active=%v error=%v", active, err)
			}
		})
	}
}

func TestGoalReadAcceptsOnlyExplicitlyDisabledGoals(t *testing.T) {
	for _, message := range []string{"goals feature is disabled", "unavailable"} {
		t.Run(message, func(t *testing.T) {
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				request, err := readObject(connection)
				if err != nil {
					return err
				}
				return writeObject(connection, map[string]any{"id": request["id"], "error": map[string]any{"code": -32600, "message": message}})
			})
			client := newTestClient(socket)
			defer client.Close()
			active, err := client.HasActiveGoal(context.Background(), "root")
			if active || (err == nil) != (message == "goals feature is disabled") {
				t.Fatal(active, err)
			}
		})
	}
}

func TestDurableLedgerImportsAndRetainsQueueAttempts(t *testing.T) {
	root := t.TempDir()
	legacy := filepath.Join(root, "runtime", "attempts.json")
	if err := os.Mkdir(filepath.Dir(legacy), 0700); err != nil {
		t.Fatal(err)
	}
	old := NewWithOptions("/unused.sock", ClientOptions{SubmissionLedgerPath: legacy})
	if err := old.recordQueueAttempt("root", "client-id", "saved message"); err != nil {
		t.Fatal(err)
	}
	old.Close()
	durable := filepath.Join(root, "persistent", "attempts.json")
	updated := NewWithOptions("/unused.sock", ClientOptions{SubmissionLedgerPath: durable, LegacySubmissionLedgerPath: legacy})
	if found, err := updated.queueAttempt("root", "client-id", "saved message"); err != nil || !found {
		t.Fatalf("import=%v %v", found, err)
	}
	updated.Close()
	if err := os.RemoveAll(filepath.Dir(legacy)); err != nil {
		t.Fatal(err)
	}
	resumed := NewWithOptions("/unused.sock", ClientOptions{SubmissionLedgerPath: durable, LegacySubmissionLedgerPath: legacy})
	defer resumed.Close()
	if found, err := resumed.queueAttempt("root", "client-id", "saved message"); err != nil || !found {
		t.Fatalf("restart=%v %v", found, err)
	}
	if _, err := resumed.queueAttempt("root", "client-id", "changed message"); err == nil {
		t.Fatal("retry changed its accepted message")
	}
	info, err := os.Stat(durable)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("durable permissions=%v %v", info, err)
	}
}

func TestQueuedSendRetryCannotStartOrSteerADuplicate(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/queue/list" {
			return fmt.Errorf("retry executed %v", request["method"])
		}
		return writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{
			"data": []any{map[string]any{"id": "queued", "clientUserMessageId": "client-id", "input": []any{map[string]any{"type": "text", "text": "saved message"}}}}, "nextCursor": nil,
		}})
	})
	client := newTestClient(socket)
	defer client.Close()
	if err := client.recordQueueAttempt("root", "client-id", "saved message"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	receipt, err := client.Send(ctx, "root", "saved message", "client-id", "")
	if err != nil || receipt.QueuedSubmissionID != "queued" || receipt.ClientUserMessageID != "client-id" {
		t.Fatalf("retry=%+v %v", receipt, err)
	}
}

func TestActivationValidatesExactThreadWithoutReplacingEnvironment(t *testing.T) {
	for _, id := range []string{"root", "foreign"} {
		t.Run(id, func(t *testing.T) {
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				request, err := readObject(connection)
				if err != nil {
					return err
				}
				params := request["params"].(map[string]any)
				if request["method"] != "thread/resume" || params["threadId"] != "root" || params["cwd"] != nil || params["developerInstructions"] == nil {
					return fmt.Errorf("activation parameters=%v", request)
				}
				if config, ok := params["config"].(map[string]any); ok && config["shell_environment_policy"] != nil {
					return fmt.Errorf("activation replaced environment")
				}
				return writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{"thread": map[string]any{"id": id}}})
			})
			client := newTestClient(socket)
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := client.ActivateThread(ctx, "root", ThreadPolicy{DeveloperInstructions: "Retained lead policy"})
			if (err != nil) != (id != "root") {
				t.Fatalf("activation error=%v", err)
			}
		})
	}
}

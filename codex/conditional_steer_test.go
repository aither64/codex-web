package codex

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type conditionalSteerFixture struct {
	mu                             sync.Mutex
	status, active, cwd            string
	refusal                        int
	response, history, historyText string
	lost                           bool
	steers                         int
	beforeReply                    func() error
}

func (f *conditionalSteerFixture) serve(conn *websocket.Conn) error {
	defer conn.CloseNow()
	if err := handshake(conn); err != nil {
		return err
	}
	for {
		req, err := readObject(conn)
		if err != nil {
			return nil
		}
		f.mu.Lock()
		result, rpcErr, closeConn, err := f.responseFor(req)
		f.mu.Unlock()
		if err != nil {
			return err
		}
		if closeConn {
			return nil
		}
		reply := map[string]any{"id": req["id"], "result": result}
		if rpcErr != nil {
			delete(reply, "result")
			reply["error"] = rpcErr
		}
		if err := writeObject(conn, reply); err != nil {
			return nil
		}
	}
}

func (f *conditionalSteerFixture) responseFor(req map[string]any) (any, any, bool, error) {
	params := req["params"].(map[string]any)
	if params["threadId"] != "thread" {
		return nil, nil, false, fmt.Errorf("wrong thread: %#v", req)
	}
	switch req["method"] {
	case "thread/read":
		return map[string]any{"thread": map[string]any{"id": "thread", "cwd": f.cwd, "status": map[string]any{"type": f.status}}}, nil, false, nil
	case "thread/turns/list":
		data := []any{}
		if f.active != "" {
			data = append(data, map[string]any{"id": f.active, "status": "inProgress"})
		}
		return map[string]any{"data": data}, nil, false, nil
	case "thread/items/list":
		data := []any{}
		if f.history != "" {
			text := f.historyText
			if text == "" {
				text = "notice"
			}
			data = append(data, map[string]any{"turnId": f.history, "item": map[string]any{
				"id": "item", "type": "userMessage", "clientId": "stable",
				"content": []any{map[string]any{"type": "text", "text": text}},
			}})
		}
		return map[string]any{"data": data, "nextCursor": nil}, nil, false, nil
	case "turn/steer":
		if len(params) != 4 || params["expectedTurnId"] != "turn" || params["clientUserMessageId"] != "stable" {
			return nil, nil, false, fmt.Errorf("incorrect conditional wire fields: %#v", params)
		}
		f.steers++
		if f.beforeReply != nil {
			if err := f.beforeReply(); err != nil {
				return nil, nil, false, err
			}
		}
		if f.lost {
			return nil, nil, true, nil
		}
		if f.refusal != 0 {
			return nil, map[string]any{"code": f.refusal, "message": "explicit native refusal"}, false, nil
		}
		turn := f.response
		if turn == "" {
			turn = "turn"
		}
		return map[string]any{"turnId": turn}, nil, false, nil
	default:
		return nil, nil, false, fmt.Errorf("conditional steer used forbidden RPC: %v", req["method"])
	}
}

func conditionalFixture(t *testing.T, database bool) (*conditionalSteerFixture, *Client, func() *Client) {
	t.Helper()
	f := &conditionalSteerFixture{status: "active", active: "turn", cwd: "/workspace"}
	socket := serveUnixWebsocket(t, f.serve)
	dir := t.TempDir()
	options := ClientOptions{SubmissionLedgerPath: filepath.Join(dir, "attempts.json")}
	if database {
		options.SubmissionDatabasePath = filepath.Join(dir, "attempts.sqlite")
	}
	newClient := func() *Client {
		c := NewWithOptions(socket, options)
		t.Cleanup(c.Close)
		return c
	}
	return f, newClient(), newClient
}

func conditionalSend(ctx context.Context, c *Client) (SendReceipt, error) {
	return c.ConditionalSteer(ctx, "thread", "/workspace", "turn", "notice", "stable", "operation")
}

func conditionalAttempt(t *testing.T, c *Client) (sendAttempt, bool) {
	t.Helper()
	action, _ := conditionalSteerAction("thread", "/workspace", "turn", "notice", "stable", "operation")
	attempt, found, err := c.sendAttempt("thread", "stable", "notice", action)
	if err != nil {
		t.Fatal(err)
	}
	return attempt, found
}

func TestConditionalSteerAcceptedAndImmutable(t *testing.T) {
	for _, database := range []bool{false, true} {
		t.Run(fmt.Sprint("database=", database), func(t *testing.T) {
			f, c, restart := conditionalFixture(t, database)
			f.beforeReply = func() error {
				attempt, found := conditionalAttempt(t, restart())
				if !found || attempt.State != "submitting" {
					return fmt.Errorf("wire preceded durable submission: %+v", attempt)
				}
				return nil
			}
			for _, client := range []*Client{c, restart()} {
				receipt, err := conditionalSend(context.Background(), client)
				if err != nil || receipt.TurnID != "turn" || !receipt.Steered || receipt.ClientUserMessageID != "stable" {
					t.Fatalf("receipt=%+v error=%v", receipt, err)
				}
			}
			if f.steers != 1 {
				t.Fatalf("duplicate wire dispatches: %d", f.steers)
			}
			for _, change := range []int{0, 1, 2, 3} {
				args := []string{"/workspace", "turn", "notice", "operation"}
				args[change] += "changed"
				if _, err := c.ConditionalSteer(context.Background(), "thread", args[0], args[1], args[2], "stable", args[3]); err == nil {
					t.Fatalf("accepted changed identity %d", change)
				}
			}
			if err := c.DiscardPreparedConditionalSteer(context.Background(), "thread", "/workspace", "turn", "notice", "stable", "operation"); err == nil {
				t.Fatal("discarded accepted attempt")
			}
		})
	}
}

func TestConditionalSteerRefusalRetainsIdentityUntilOwnerDiscard(t *testing.T) {
	for _, test := range []struct {
		name, status, active, cwd string
		refusal                   int
		steers                    int
	}{
		{"idle", "idle", "", "/workspace", 0, 0},
		{"completed but stale active status", "active", "", "/workspace", 0, 0},
		{"cold", "notLoaded", "turn", "/workspace", 0, 0},
		{"successor", "active", "new-turn", "/workspace", 0, 0},
		{"identity", "active", "turn", "/another", 0, 0},
		{"ended or nonsteerable", "active", "turn", "/workspace", -32600, 1},
		{"unsupported", "active", "turn", "/workspace", -32601, 1},
		{"invalid params", "active", "turn", "/workspace", -32602, 1},
	} {
		for _, database := range []bool{false, true} {
			t.Run(fmt.Sprint(test.name, "/database=", database), func(t *testing.T) {
				f, c, restart := conditionalFixture(t, database)
				f.status, f.active, f.cwd, f.refusal = test.status, test.active, test.cwd, test.refusal
				_, err := conditionalSend(context.Background(), c)
				var unsent *MessageNotSentError
				if !errors.As(err, &unsent) || unsent.Retryable {
					t.Fatalf("refusal=%v", err)
				}
				retained := restart()
				if attempt, found := conditionalAttempt(t, retained); !found || attempt.State != "prepared" {
					t.Fatalf("refused identity was lost: %+v, %t", attempt, found)
				}
				if _, err := retained.ConditionalSteer(context.Background(), "thread", "/workspace", "different", "notice", "stable", "operation"); err == nil {
					t.Fatal("retargeted refused identity before owner terminal record")
				}
				if err := retained.DiscardPreparedConditionalSteer(context.Background(), "thread", "/workspace", "turn", "changed", "stable", "operation"); err == nil {
					t.Fatal("discarded wrong payload")
				}
				if err := retained.RequireSubmissionAttemptsResolved(context.Background(), "thread"); err == nil {
					t.Fatal("prepared refusal silently bypassed idle admission")
				}
				// The operation owner persists its terminal disposition before this
				// point. Repeating recovery only completes that discard, not send.
				for range 2 {
					if err := retained.DiscardPreparedConditionalSteer(context.Background(), "thread", "/workspace", "turn", "notice", "stable", "operation"); err != nil {
						t.Fatal(err)
					}
				}
				if err := retained.RequireSubmissionAttemptsResolved(context.Background(), "thread"); err != nil {
					t.Fatalf("discard still blocks idle: %v", err)
				}
				if f.steers != test.steers {
					t.Fatalf("steers=%d want=%d", f.steers, test.steers)
				}
			})
		}
	}
}

func TestConditionalSteerRejectsInvalidIdentityWithoutRPC(t *testing.T) {
	c := newTestClient(filepath.Join(t.TempDir(), "absent.sock"))
	defer c.Close()
	for i := range 6 {
		args := []string{"thread", "/workspace", "turn", "notice", "stable", "operation"}
		args[i] = ""
		if _, err := c.ConditionalSteer(context.Background(), args[0], args[1], args[2], args[3], args[4], args[5]); err == nil {
			t.Fatalf("missing identity %d accepted", i)
		}
	}
	for _, cwd := range []string{"relative", "/workspace/../workspace"} {
		if _, err := c.ConditionalSteer(context.Background(), "thread", cwd, "turn", "notice", "stable", "operation"); err == nil {
			t.Fatalf("noncanonical cwd %q accepted", cwd)
		}
	}
	for _, change := range []int{0, 1, 2, 3} {
		args := []string{"/workspace", "turn", "notice", "operation"}
		args[change] += "\xff"
		if _, err := c.ConditionalSteer(context.Background(), "thread", args[0], args[1], args[2], "stable", args[3]); err == nil {
			t.Fatal("invalid UTF-8 lost immutable identity")
		}
	}
}

func TestConditionalSteerLostReplyReconcilesOnlyExactHistory(t *testing.T) {
	for _, history := range []string{"", "other-turn", "wrong-text", "turn"} {
		for _, database := range []bool{false, true} {
			t.Run(fmt.Sprint("history=", history, "/database=", database), func(t *testing.T) {
				f, c, restart := conditionalFixture(t, database)
				f.lost, f.history = true, history
				if history == "wrong-text" {
					f.history, f.historyText = "turn", "different notice"
				}
				for _, client := range []*Client{c, restart()} {
					receipt, err := conditionalSend(context.Background(), client)
					if history == "turn" {
						if err != nil || receipt.TurnID != "turn" || !receipt.Steered {
							t.Fatalf("lost reply recovery: %+v %v", receipt, err)
						}
					} else {
						var unknown *UnknownSendOutcomeError
						if !errors.As(err, &unknown) {
							t.Fatalf("uncertain reply became resolved: %+v %v", receipt, err)
						}
						if err := client.DiscardPreparedConditionalSteer(context.Background(), "thread", "/workspace", "turn", "notice", "stable", "operation"); err == nil {
							t.Fatal("discarded uncertain input")
						}
					}
				}
				if f.steers != 1 {
					t.Fatalf("lost reply retried mutation: %d", f.steers)
				}
			})
		}
	}
}

func TestConditionalSteerWrongResponseRemainsUnknown(t *testing.T) {
	f, c, restart := conditionalFixture(t, true)
	f.response = "wrong-turn"
	for _, client := range []*Client{c, restart()} {
		_, err := conditionalSend(context.Background(), client)
		var unknown *UnknownSendOutcomeError
		if !errors.As(err, &unknown) {
			t.Fatalf("wrong response was not uncertain: %v", err)
		}
	}
	if f.steers != 1 {
		t.Fatalf("wrong response retried mutation: %d", f.steers)
	}
}

func TestConditionalSteerDoesNotRestoreReceiverWatches(t *testing.T) {
	f, c, _ := conditionalFixture(t, true)
	// A disconnected interactive client can retain watches for unrelated cold
	// threads. Ensure on that receiver would restore them and send thread/resume.
	c.watched["thread"], c.watched["cold-peer"] = 1, 1
	var resumeChecks atomic.Int32
	c.options.AllowImplicitResume = func(string) bool { resumeChecks.Add(1); return true }
	if _, err := conditionalSend(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	c.connectionMu.Lock()
	connected := c.connection != nil
	c.connectionMu.Unlock()
	if connected || resumeChecks.Load() != 0 || len(c.watchedGeneration) != 0 || f.steers != 1 {
		t.Fatal("conditional I/O connected the receiver or restored a watch")
	}
}

func TestConditionalSteerPreservesObserverOnly(t *testing.T) {
	f, c, _ := conditionalFixture(t, true)
	c.options.ObserverOnly = true
	_, err := conditionalSend(context.Background(), c)
	var unsent *MessageNotSentError
	if !errors.As(err, &unsent) || f.steers != 0 {
		t.Fatalf("observer input mutated: %v, steers=%d", err, f.steers)
	}
}

func TestConditionalSteerCancellationBeforeAndAfterWrite(t *testing.T) {
	for _, database := range []bool{false, true} {
		for _, afterWrite := range []bool{false, true} {
			t.Run(fmt.Sprintf("database=%t/afterWrite=%t", database, afterWrite), func(t *testing.T) {
				f, c, restart := conditionalFixture(t, database)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if afterWrite {
					f.lost, f.beforeReply = true, func() error { cancel(); return nil }
				} else {
					var writes atomic.Int32
					c.options.DeliveryObserver = func(event DeliveryEvent) {
						if event.Stage == "submission-write" && !event.Failed && writes.Add(1) == 2 {
							cancel() // prepared then submitting, before acquiring writer
						}
					}
				}
				_, err := conditionalSend(ctx, c)
				var unknown *UnknownSendOutcomeError
				var unsent *MessageNotSentError
				if afterWrite {
					if !errors.As(err, &unknown) {
						t.Fatalf("written cancellation became unsent: %v", err)
					}
					if _, err := conditionalSend(context.Background(), restart()); !errors.As(err, &unknown) {
						t.Fatalf("restart forgot uncertain cancellation: %v", err)
					}
				} else {
					if !errors.As(err, &unsent) || !unsent.Retryable || !errors.Is(err, context.Canceled) {
						t.Fatalf("pre-write cancellation lost proof: %v", err)
					}
					if f.steers != 0 {
						t.Fatal("pre-write cancellation reached native")
					}
					if attempt, found := conditionalAttempt(t, restart()); !found || attempt.State != "prepared" {
						t.Fatalf("pre-write failure lost identity: %+v", attempt)
					}
					if _, err := conditionalSend(context.Background(), restart()); err != nil {
						t.Fatalf("same-target pre-write retry failed: %v", err)
					}
				}
				if f.steers != 1 {
					t.Fatalf("cancellation dispatches=%d", f.steers)
				}
			})
		}
	}
}

func TestConditionalSteerConcurrentClientsShareOneClaim(t *testing.T) {
	for _, database := range []bool{false, true} {
		t.Run(fmt.Sprint("database=", database), func(t *testing.T) {
			f, c, restart := conditionalFixture(t, database)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			results := make(chan error, 2)
			for _, client := range []*Client{c, restart()} {
				go func(client *Client) {
					_, err := conditionalSend(ctx, client)
					results <- err
				}(client)
			}
			for range 2 {
				err := <-results
				var unknown *UnknownSendOutcomeError
				if err != nil && !errors.As(err, &unknown) {
					t.Fatal(err)
				}
			}
			if receipt, err := conditionalSend(ctx, restart()); err != nil || receipt.TurnID != "turn" {
				t.Fatalf("shared accepted receipt: %+v %v", receipt, err)
			}
			if f.steers != 1 {
				t.Fatalf("concurrent mutation count=%d", f.steers)
			}
		})
	}
}

func TestConditionalSteerReceiptStoreFailureStaysUnknown(t *testing.T) {
	f, c, restart := conditionalFixture(t, true)
	f.beforeReply = func() error {
		_, err := c.submission.db.Exec(`CREATE TRIGGER refuse_accept BEFORE UPDATE ON submission_records
WHEN NEW.kind='sends' AND NEW.state='accepted' BEGIN SELECT RAISE(FAIL,'fixture receipt failure'); END`)
		return err
	}
	_, err := conditionalSend(context.Background(), c)
	var unknown *UnknownSendOutcomeError
	if !errors.As(err, &unknown) {
		t.Fatalf("receipt-store failure=%v", err)
	}
	if attempt, found := conditionalAttempt(t, restart()); !found || attempt.State != "submitting" {
		t.Fatalf("lost uncertain state: %+v %t", attempt, found)
	}
	if f.steers != 1 {
		t.Fatalf("unexpected dispatches=%d", f.steers)
	}
	if _, err := c.submission.db.Exec("DROP TRIGGER refuse_accept"); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.history = "turn"
	f.mu.Unlock()
	if receipt, err := conditionalSend(context.Background(), restart()); err != nil || receipt.TurnID != "turn" || f.steers != 1 {
		t.Fatalf("receipt-storage recovery resent input: %+v %v", receipt, err)
	}
}

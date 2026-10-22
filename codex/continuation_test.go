package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/coder/websocket"
)

func TestTurnStateIsLoadedFailedTurn(t *testing.T) {
	for _, test := range []struct {
		name, status, latestID, turnStatus, expectedID string
		want                                           bool
	}{
		{"idle failed", "idle", "failed", "failed", "failed", true},
		{"loaded error failed", "systemError", "failed", "failed", "failed", true},
		{"cold failed", "notLoaded", "failed", "failed", "failed", false},
		{"active failed", "active", "failed", "failed", "failed", false},
		{"missing status", "", "failed", "failed", "failed", false},
		{"unknown status", "future", "failed", "failed", "failed", false},
		{"missing expected", "systemError", "failed", "failed", "", false},
		{"missing latest", "systemError", "", "failed", "failed", false},
		{"newer turn", "systemError", "newer", "failed", "failed", false},
		{"in progress", "systemError", "failed", "inProgress", "failed", false},
		{"completed", "systemError", "failed", "completed", "failed", false},
		{"interrupted", "systemError", "failed", "interrupted", "failed", false},
		{"missing turn status", "systemError", "failed", "", "failed", false},
		{"unknown turn status", "systemError", "failed", "future", "failed", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := TurnState{Status: test.status, LatestTurnID: test.latestID, LatestTurnStatus: test.turnStatus}
			if got := state.IsLoadedFailedTurn(test.expectedID); got != test.want {
				t.Fatalf("predicate=%t want=%t for %+v expected=%q", got, test.want, state, test.expectedID)
			}
		})
	}
}

func TestReadTurnStatePreservesRawStatus(t *testing.T) {
	for _, status := range []string{"idle", "systemError", "notLoaded", "active"} {
		t.Run(status, func(t *testing.T) {
			f := &continuationFixture{state: status, turn: "failed"}
			client := newTestClient(serveUnixWebsocket(t, f.serve))
			defer client.Close()
			state, err := client.ReadTurnState(context.Background(), "thread-1")
			if err != nil || state.Status != status || state.TerminalFailure == nil || state.TerminalFailure.Code != "serverOverloaded" {
				t.Fatalf("raw projection changed: %+v %v", state, err)
			}
		})
	}
}

func TestReadTurnStateColdTerminalProjection(t *testing.T) {
	for _, test := range []struct {
		name, status string
		code         any
		wantFailure  bool
	}{
		{"capacity", "failed", "serverOverloaded", true},
		{"cyber", "failed", "cyberPolicy", true},
		{"unknown code preserved", "failed", "futureCode", true},
		{"structured code unclassified", "failed", map[string]any{"httpConnectionFailed": map[string]any{}}, true},
		{"intermediate error ignored", "inProgress", "cyberPolicy", false},
		{"completed refusal ignored", "completed", "cyberPolicy", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			socket := serveUnixWebsocket(t, func(conn *websocket.Conn) error {
				if err := handshake(conn); err != nil {
					return err
				}
				for _, method := range []string{"thread/read", "thread/turns/list"} {
					req, err := readObject(conn)
					if err != nil {
						return err
					}
					if req["method"] != method {
						return fmt.Errorf("cold read mutated: %v", req)
					}
					result := map[string]any{"thread": map[string]any{"id": "thread-1", "status": map[string]any{"type": "notLoaded"}}}
					if method == "thread/turns/list" {
						params := req["params"].(map[string]any)
						if params["limit"] != float64(1) || params["itemsView"] != "notLoaded" {
							return fmt.Errorf("unbounded read: %v", params)
						}
						result = map[string]any{"data": []any{map[string]any{"id": "failed", "status": test.status, "error": map[string]any{"message": "native message", "codexErrorInfo": test.code}}}}
					}
					if err := writeObject(conn, map[string]any{"id": req["id"], "result": result}); err != nil {
						return err
					}
				}
				return nil
			})
			client := newTestClient(socket)
			defer client.Close()
			state, err := client.ReadTurnState(context.Background(), "thread-1")
			if err != nil || state.Status != "notLoaded" || state.LatestTurnID != "failed" || (state.TerminalFailure != nil) != test.wantFailure {
				t.Fatalf("state=%+v error=%v", state, err)
			}
			if test.wantFailure && (state.TerminalFailure.Code != stringValue(test.code) || state.TerminalFailure.Message != "native message") {
				t.Fatalf("failure=%+v", state.TerminalFailure)
			}
		})
	}
}

func TestReadTurnStateRejectsMalformedAndRepresentsEmptyHistory(t *testing.T) {
	for _, data := range []string{`[]`, `null`, `[{}]`, `[{"id":"turn","status":"future"}]`, `[{"id":"a","status":"failed"},{"id":"b","status":"failed"}]`} {
		t.Run(data, func(t *testing.T) {
			socket := serveUnixWebsocket(t, func(conn *websocket.Conn) error {
				if err := handshake(conn); err != nil {
					return err
				}
				for _, result := range []any{map[string]any{"thread": map[string]any{"id": "thread-1", "status": map[string]any{"type": "idle"}}}, json.RawMessage(`{"data":` + data + `}`)} {
					req, err := readObject(conn)
					if err != nil {
						return err
					}
					if err := writeObject(conn, map[string]any{"id": req["id"], "result": result}); err != nil {
						return err
					}
				}
				return nil
			})
			client := newTestClient(socket)
			defer client.Close()
			state, err := client.ReadTurnState(context.Background(), "thread-1")
			if (err == nil) != (data == `[]`) || state.LatestTurnID != "" {
				t.Fatalf("state=%+v err=%v", state, err)
			}
		})
	}
}

type continuationFixture struct {
	state      string
	turn       string
	turnStatus string
	queued     bool
	race       bool
	unknown    bool
	starts     atomic.Int32
	reads      atomic.Int32
}

func (f *continuationFixture) serve(conn *websocket.Conn) error {
	if err := handshake(conn); err != nil {
		return err
	}
	for {
		req, err := readObject(conn)
		if err != nil {
			return nil
		}
		var result any
		switch req["method"] {
		case "thread/read":
			result = map[string]any{"thread": map[string]any{"id": "thread-1", "status": map[string]any{"type": f.state}}}
		case "thread/turns/list":
			turn, status := f.turn, f.turnStatus
			if status == "" {
				status = "failed"
			}
			if f.race && f.reads.Add(1) > 1 {
				turn, status = "manual", "inProgress"
			}
			result = map[string]any{"data": []any{map[string]any{"id": turn, "status": status, "error": map[string]any{"message": "capacity", "codexErrorInfo": "serverOverloaded"}}}}
		case "thread/queue/list":
			data := []any{}
			if f.queued {
				data = append(data, map[string]any{"id": "queue-1", "clientUserMessageId": "manual", "input": []any{map[string]any{"type": "text", "text": "manual"}}})
			}
			result = map[string]any{"data": data, "nextCursor": nil}
		case "turn/start":
			f.starts.Add(1)
			params := req["params"].(map[string]any)
			if params["model"] != "model-1" || params["effort"] != "high" || params["clientUserMessageId"] != "stable" {
				return fmt.Errorf("changed continuation options: %v", params)
			}
			if err := additionalContextError(params); err != nil {
				return err
			}
			if f.unknown {
				if err := writeObject(conn, map[string]any{"id": req["id"], "error": map[string]any{"code": -32000, "message": "response uncertain"}}); err != nil {
					return err
				}
				continue
			}
			result = map[string]any{"turn": map[string]any{"id": "continued"}}
		case "thread/items/list":
			result = map[string]any{"data": []any{}, "nextCursor": nil}
		default:
			return fmt.Errorf("continuation mutated policy, resumed or steered: %v", req)
		}
		if err := writeObject(conn, map[string]any{"id": req["id"], "result": result}); err != nil {
			return err
		}
	}
}

func TestContinueFailedTurnAdmissionAndReceipt(t *testing.T) {
	for _, loadedStatus := range []string{"idle", "systemError"} {
		t.Run(loadedStatus, func(t *testing.T) {
			for _, name := range []string{"accepted", "active", "cold", "stale", "missing latest", "latest in progress", "latest completed", "latest interrupted", "unknown status", "missing status", "queue", "new active turn race", "pending request", "other unresolved send", "unknown"} {
				t.Run(name, func(t *testing.T) {
					f := &continuationFixture{state: loadedStatus, turn: "failed"}
					switch name {
					case "active":
						f.state = "active"
					case "cold":
						f.state = "notLoaded"
					case "stale":
						f.turn = "newer"
					case "missing latest":
						f.turn = ""
					case "latest in progress":
						f.turnStatus = "inProgress"
					case "latest completed":
						f.turnStatus = "completed"
					case "latest interrupted":
						f.turnStatus = "interrupted"
					case "unknown status":
						f.state = "future"
					case "missing status":
						f.state = ""
					case "queue":
						f.queued = true
					case "new active turn race":
						f.race = true
					case "unknown":
						f.unknown = true
					}
					client := newTestClient(serveUnixWebsocket(t, f.serve))
					defer client.Close()
					if name == "other unresolved send" {
						if err := client.PrepareSend("thread-1", "other", "other", "other", false); err != nil {
							t.Fatal(err)
						}
					}
					if name == "pending request" {
						if err := client.Ensure(context.Background()); err != nil {
							t.Fatal(err)
						}
						client.pendingMu.Lock()
						client.requests[`"pending"`] = PendingRequest{ID: `"pending"`, Method: "item/tool/requestUserInput", connection: client.connection, generation: client.generation, Params: json.RawMessage(`{"threadId":"thread-1","turnId":"failed","itemId":"pending","isBlocking":true,"questions":[{"id":"q","header":"Question","question":"Choose","options":[{"label":"First","description":"First"}]}]}`)}
						client.pendingMu.Unlock()
					}
					receipt, err := client.ContinueFailedTurn(context.Background(), "thread-1", "continue", "stable", "retry:failed:settings", "failed", testTurnOptions())
					if name == "accepted" {
						if err != nil || receipt.TurnID != "continued" || receipt.Steered {
							t.Fatalf("receipt=%+v error=%v", receipt, err)
						}
						// Reconciliation happens before all current-state checks, even cold.
						again, err := client.ContinueFailedTurn(context.Background(), "thread-1", "continue", "stable", "retry:failed:settings", "failed", testTurnOptions())
						if err != nil || again != receipt {
							t.Fatalf("again=%+v error=%v", again, err)
						}
						changed := testTurnOptions()
						changed.ReasoningEffort = "low"
						if _, err := client.ContinueFailedTurn(context.Background(), "thread-1", "continue", "stable", "retry:failed:settings", "failed", changed); err == nil {
							t.Fatal("changed options accepted")
						}
					} else if name == "unknown" {
						for range 2 {
							var uncertain *UnknownSendOutcomeError
							if !errors.As(err, &uncertain) {
								t.Fatalf("expected unknown: %v", err)
							}
							_, err = client.ContinueFailedTurn(context.Background(), "thread-1", "continue", "stable", "retry:failed:settings", "failed", testTurnOptions())
						}
					} else if err == nil {
						t.Fatal("busy continuation accepted")
					}
					want := int32(0)
					if name == "accepted" || name == "unknown" {
						want = 1
					}
					if f.starts.Load() != want {
						t.Fatalf("starts=%d, want %d", f.starts.Load(), want)
					}
				})
			}
		})
	}
}

func TestContinuationSubmittingClaimIsAtomicAcrossClients(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	clients := []*Client{NewWithOptions("/unused", ClientOptions{SubmissionLedgerPath: path}), NewWithOptions("/unused", ClientOptions{SubmissionLedgerPath: path})}
	for _, client := range clients {
		defer client.Close()
	}
	if err := clients[0].PrepareSendWithOptions("thread", "continue", "stable", "retry:failed:settings", false, testTurnOptions()); err != nil {
		t.Fatal(err)
	}
	normalized, _ := normalizeTurnOptions(testTurnOptions())
	var wg sync.WaitGroup
	var wins atomic.Int32
	for _, client := range clients {
		wg.Add(1)
		go func(c *Client) {
			defer wg.Done()
			won, err := c.claimContinuationAttempt("thread", "stable", "continue", "retry:failed:settings", normalized.Digest)
			if err != nil {
				t.Error(err)
			}
			if won {
				wins.Add(1)
			}
		}(client)
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("outbound grants=%d", wins.Load())
	}
}

func TestContinuationDuplicateClientsReconcileOneNativeSubmission(t *testing.T) {
	f := &continuationFixture{state: "idle", turn: "failed"}
	socket := serveUnixWebsocket(t, f.serve)
	ledger := filepath.Join(t.TempDir(), "ledger.json")
	clients := []*Client{NewWithOptions(socket, ClientOptions{SubmissionLedgerPath: ledger}), NewWithOptions(socket, ClientOptions{SubmissionLedgerPath: ledger})}
	for _, client := range clients {
		defer client.Close()
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, client := range clients {
		wg.Add(1)
		go func(c *Client) {
			defer wg.Done()
			<-start
			receipt, err := c.ContinueFailedTurn(context.Background(), "thread-1", "continue", "stable", "retry:failed:settings", "failed", testTurnOptions())
			var unknown *UnknownSendOutcomeError
			if err != nil && !errors.As(err, &unknown) {
				t.Errorf("unexpected duplicate outcome: %v", err)
			}
			if err == nil && receipt.TurnID != "continued" {
				t.Errorf("wrong receipt: %+v", receipt)
			}
		}(client)
	}
	close(start)
	wg.Wait()
	if f.starts.Load() != 1 {
		t.Fatalf("native submissions=%d", f.starts.Load())
	}
	// A new process can recover the accepted identity without a live socket or
	// consulting today's thread state. Changed text or context remains invalid.
	restarted := NewWithOptions("/missing", ClientOptions{SubmissionLedgerPath: ledger})
	defer restarted.Close()
	receipt, err := restarted.ContinueFailedTurn(context.Background(), "thread-1", "continue", "stable", "retry:failed:settings", "failed", testTurnOptions())
	if err != nil || receipt.TurnID != "continued" {
		t.Fatalf("restart receipt=%+v err=%v", receipt, err)
	}
	for _, values := range [][2]string{{"changed", "retry:failed:settings"}, {"continue", "retry:other:settings"}} {
		if _, err := restarted.ContinueFailedTurn(context.Background(), "thread-1", values[0], "stable", values[1], "failed", testTurnOptions()); err == nil {
			t.Fatal("changed identity recovered receipt")
		}
	}
}

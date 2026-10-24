package codex

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestRequestPreservesConcurrentCancellationAndTransportFailure(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprint("canceled=", canceled), func(t *testing.T) {
			socket := serveUnixWebsocket(t, func(conn *websocket.Conn) error {
				defer conn.CloseNow()
				if err := handshake(conn); err != nil {
					return err
				}
				if _, err := readObject(conn); err == nil {
					t.Error("refused request reached the peer")
				}
				return nil
			})
			client := newTestClient(socket)
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			failure := errors.New("connection changed during admission")
			err := client.requestWithAdmission(ctx, "thread/read", map[string]any{}, nil, func() error {
				if canceled {
					cancel()
				}
				return &TransportError{Err: failure}
			})
			var transport *TransportError
			if !errors.As(err, &transport) || !errors.Is(err, failure) || errors.Is(err, context.Canceled) != canceled {
				t.Fatalf("lost request failure or cancellation evidence: %v", err)
			}
		})
	}
}

func TestCanceledSendBeforeWriteSurvivesRestart(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(fmt.Sprint("active=", active), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			turnRead, releaseRead := make(chan struct{}), make(chan struct{})
			var connections, mutations atomic.Int32
			socket := serveUnixWebsocket(t, func(conn *websocket.Conn) error {
				defer conn.CloseNow()
				first := connections.Add(1) == 1
				if err := handshake(conn); err != nil {
					return err
				}
				for {
					req, err := readObject(conn)
					if err != nil {
						if first {
							return nil
						}
						return err
					}
					result := map[string]any{}
					switch req["method"] {
					case "thread/resume":
					case "thread/turns/list":
						if first {
							close(turnRead)
							select {
							case <-releaseRead:
							case <-ctx.Done():
								return ctx.Err()
							}
						}
						turns := []any{}
						if active {
							turns = append(turns, map[string]any{"id": "active", "status": "inProgress"})
						}
						result["data"] = turns
					case "turn/start", "turn/steer":
						mutations.Add(1)
						result = map[string]any{"turn": map[string]any{"id": "accepted"}, "turnId": "active"}
					default:
						return fmt.Errorf("unexpected RPC %v", req["method"])
					}
					if err := writeObject(conn, map[string]any{"id": req["id"], "result": result}); err != nil {
						return err
					}
					if req["method"] == "turn/start" || req["method"] == "turn/steer" {
						return nil
					}
				}
			})
			client := newTestClient(socket)
			defer client.Close()
			canceled, cancelSend := context.WithCancel(ctx)
			defer cancelSend()
			done := make(chan error, 1)
			go func() { _, err := client.Send(canceled, "thread", "message", "client", ""); done <- err }()
			select {
			case <-turnRead:
			case <-ctx.Done():
				t.Fatal("send did not inspect active turn")
			}
			client.writeMu.Lock()
			close(releaseRead)
			waitFor(t, func() bool {
				attempt, found, _ := client.sendAttempt("thread", "client", "message", "")
				return found && attempt.State == "prepared"
			})
			cancelSend()
			client.writeMu.Unlock()
			err := <-done
			var unsent *MessageNotSentError
			if !errors.As(err, &unsent) || !errors.Is(err, context.Canceled) {
				t.Fatalf("pre-write failure: %v", err)
			}
			if mutations.Load() != 0 {
				t.Fatal("canceled send reached native mutation")
			}
			client.Close()
			restarted := newTestClient(socket)
			defer restarted.Close()
			receipt, err := restarted.Send(ctx, "thread", "message", "client", "")
			if err != nil || receipt.ClientUserMessageID != "client" || mutations.Load() != 1 {
				t.Fatalf("same-ID retry after restart: %#v, %v, mutations=%d", receipt, err, mutations.Load())
			}
		})
	}
}

func TestExplicitProtocolRefusalLeavesSendRetryable(t *testing.T) {
	var mutations atomic.Int32
	socket := serveUnixWebsocket(t, func(conn *websocket.Conn) error {
		defer conn.CloseNow()
		if err := handshake(conn); err != nil {
			return err
		}
		for {
			req, err := readObject(conn)
			if err != nil {
				return err
			}
			response := map[string]any{"id": req["id"], "result": map[string]any{}}
			switch req["method"] {
			case "thread/resume":
			case "thread/turns/list":
				response["result"] = map[string]any{"data": []any{}}
			case "turn/start":
				if mutations.Add(1) == 1 {
					delete(response, "result")
					response["error"] = map[string]any{"code": -32602, "message": "invalid turn input"}
				} else {
					response["result"] = map[string]any{"turn": map[string]any{"id": "accepted"}}
				}
			default:
				return fmt.Errorf("unexpected RPC %v", req["method"])
			}
			if err := writeObject(conn, response); err != nil {
				return err
			}
			if mutations.Load() == 2 {
				return nil
			}
		}
	})
	client := newTestClient(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.Send(ctx, "thread", "message", "client", "")
	var unsent *MessageNotSentError
	if !errors.As(err, &unsent) {
		t.Fatalf("native refusal: %v", err)
	}
	if receipt, err := client.Send(ctx, "thread", "message", "client", ""); err != nil || receipt.TurnID != "accepted" {
		t.Fatalf("retry: %#v, %v", receipt, err)
	}
}

func TestSharedLedgerAdmitsOnlyOnePreparedSender(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		for _, accepted := range []bool{false, true} {
			t.Run(fmt.Sprint("canceled=", canceled, "/accepted=", accepted), func(t *testing.T) { testSharedLedgerSender(t, canceled, accepted) })
		}
	}
}

func TestSharedHistoryRecoveryRecordsAcceptanceAtomically(t *testing.T) {
	var arrived atomic.Int32
	both := make(chan struct{})
	socket := serveUnixWebsocket(t, func(conn *websocket.Conn) error {
		defer conn.CloseNow()
		if err := handshake(conn); err != nil {
			return err
		}
		req, err := readObject(conn)
		if err != nil {
			return err
		}
		if req["method"] != "thread/items/list" {
			return fmt.Errorf("recovery mutated: %v", req["method"])
		}
		if arrived.Add(1) == 2 {
			close(both)
		}
		select {
		case <-both:
		case <-time.After(5 * time.Second):
			return errors.New("both recovery callers did not reach history")
		}
		return writeObject(conn, map[string]any{"id": req["id"], "result": map[string]any{"data": []any{
			map[string]any{"turnId": "accepted", "item": map[string]any{"type": "userMessage", "id": "item", "clientId": "client", "content": []any{map[string]any{"type": "text", "text": "message"}}}},
		}, "nextCursor": nil}})
	})
	first, second := newTestClient(socket), newTestClient(socket)
	defer first.Close()
	defer second.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	options := TurnOptions{Model: "model-1", ReasoningEffort: "high"}
	results := make(chan error, 2)
	for _, client := range []*Client{first, second} {
		go func(client *Client) {
			found, err := client.SendAttemptedWithOptions(ctx, "thread", "message", "client", "", options)
			if err == nil && !found {
				err = errors.New("accepted history was lost")
			}
			results <- err
		}(client)
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	for _, client := range []*Client{first, second} {
		receipt, err := client.SendWithOptions(ctx, "thread", "message", "client", "", options)
		if err != nil || receipt.TurnID != "accepted" {
			t.Fatalf("recovered receipt: %#v %v", receipt, err)
		}
	}
}

func testSharedLedgerSender(t *testing.T, canceled, accepted bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	firstTurns, releaseTurns := make(chan struct{}), make(chan struct{})
	mutation, releaseMutation := make(chan struct{}), make(chan struct{})
	defer closeTestChannel(releaseTurns)
	defer closeTestChannel(releaseMutation)
	var connections, mutations atomic.Int32
	socket := serveUnixWebsocket(t, func(conn *websocket.Conn) error {
		defer conn.CloseNow()
		first := connections.Add(1) == 1
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
				if first {
					close(firstTurns)
					select {
					case <-releaseTurns:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				result["data"] = []any{}
			case "thread/items/list":
				result["data"] = []any{}
			case "turn/start":
				if mutations.Add(1) != 1 {
					return errors.New("duplicate native mutation")
				}
				close(mutation)
				select {
				case <-releaseMutation:
				case <-ctx.Done():
					return ctx.Err()
				}
				result["turn"] = map[string]any{"id": "accepted"}
			default:
				return fmt.Errorf("unexpected RPC %v", req["method"])
			}
			if err := writeObject(conn, map[string]any{"id": req["id"], "result": result}); err != nil {
				return err
			}
		}
	})
	first, second := newTestClient(socket), newTestClient(socket)
	defer first.Close()
	defer second.Close()
	options := TurnOptions{Model: "model-1", ReasoningEffort: "high"}
	normalized, _ := normalizeTurnOptions(options)
	firstContext, cancelFirst := context.WithCancel(ctx)
	defer cancelFirst()
	firstResult := make(chan error, 1)
	go func() {
		_, err := first.SendWithOptions(firstContext, "thread", "message", "client", "", options)
		firstResult <- err
	}()
	select {
	case <-firstTurns:
	case <-ctx.Done():
		t.Fatal("first sender did not reach turn read")
	}
	first.writeMu.Lock()
	close(releaseTurns)
	waitFor(t, func() bool {
		attempt, found, _ := first.sendAttemptWithOptions("thread", "client", "message", "", normalized.Digest)
		return found && attempt.State == "prepared"
	})
	secondResult := make(chan error, 1)
	go func() {
		_, err := second.SendWithOptions(ctx, "thread", "message", "client", "", options)
		secondResult <- err
	}()
	select {
	case <-mutation:
	case <-ctx.Done():
		first.writeMu.Unlock()
		t.Fatal("second sender did not dispatch")
	}
	if accepted {
		close(releaseMutation)
		if err := <-secondResult; err != nil {
			t.Fatal(err)
		}
	}
	if canceled {
		cancelFirst()
	}
	first.writeMu.Unlock()
	var unknown *UnknownSendOutcomeError
	if err := <-firstResult; (accepted && err != nil) || (!accepted && !errors.As(err, &unknown)) {
		t.Fatalf("competing uncertain send: %v", err)
	}
	if mutations.Load() != 1 {
		t.Fatal("competing client dispatched input")
	}
	if !accepted {
		close(releaseMutation)
		if err := <-secondResult; err != nil {
			t.Fatal(err)
		}
	}
	if receipt, err := first.SendWithOptions(ctx, "thread", "message", "client", "", options); err != nil || receipt.TurnID != "accepted" {
		t.Fatalf("competing sender did not recover accepted receipt: %#v %v", receipt, err)
	}
}

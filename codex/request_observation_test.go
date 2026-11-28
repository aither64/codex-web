package codex

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestRequestObservation(t *testing.T) {
	for _, mode := range []string{"success", "rpc", "cancel", "concurrent", "reconnect", "nil"} {
		t.Run(mode, func(t *testing.T) {
			type key struct{}
			ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), key{}, "public-scope"), 3*time.Second)
			defer cancel()
			var mu sync.Mutex
			accepts := map[string]bool{}
			starts := map[uint64]RequestObservation{}
			finishes := map[uint64]RequestObservation{}
			delivered := map[string]bool{}
			done := make(chan struct{}, 2)
			var client *Client
			socket := serveUnixWebsocket(t, func(conn *websocket.Conn) error {
				defer func() { conn.CloseNow(); done <- struct{}{} }()
				if err := handshake(conn); err != nil {
					return err
				}
				count := 1
				if mode == "concurrent" {
					count = 2
				}
				requests := make([]map[string]any, 0, count)
				for i := 0; i < count; i++ {
					request, err := readObject(conn)
					if err != nil {
						return err
					}
					requests = append(requests, request)
					if mode != "nil" {
						id := uint64(request["id"].(float64))
						mu.Lock()
						_, registered := starts[id]
						mu.Unlock()
						if !registered {
							return errors.New("wire request preceded observation start")
						}
					}
				}
				if mode == "cancel" {
					cancel()
				} else {
					for i := len(requests) - 1; i >= 0; i-- {
						response := map[string]any{"id": requests[i]["id"], "result": map[string]any{}}
						if mode == "rpc" {
							delete(response, "result")
							response["error"] = map[string]any{"code": -32000, "message": "public refusal"}
						}
						if err := writeObject(conn, response); err != nil {
							return err
						}
					}
				}
				// Join via the client's exact connection close, not a detached peer timer.
				_, _, _ = conn.Read(ctx)
				return nil
			}, func(r *http.Request) {
				sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
				mu.Lock()
				accepts[base64.StdEncoding.EncodeToString(sum[:])] = true
				mu.Unlock()
			})
			options := ClientOptions{DeliveryObserver: func(e DeliveryEvent) { mu.Lock(); delivered[e.Stage] = true; mu.Unlock() }}
			if mode != "nil" {
				options.ObserveRequest = func(got context.Context, event RequestObservation) {
					if got != ctx || got.Value(key{}) != "public-scope" {
						t.Error("effective context replaced")
					}
					// These are the three locks excluded by the public callback contract.
					if !client.connectionMu.TryLock() {
						t.Error("callback under connection lock")
					} else {
						client.connectionMu.Unlock()
					}
					if !client.pendingMu.TryLock() {
						t.Error("callback under pending lock")
					} else {
						_, pending := client.pending[event.RequestID]
						if pending != (event.Phase == RequestStarted) {
							t.Error("pending lifetime differs from observed lifetime")
						}
						client.pendingMu.Unlock()
					}
					// With concurrent RPCs a sibling may hold writeMu; check the single-call cases.
					if mode != "concurrent" {
						if !client.writeMu.TryLock() {
							t.Error("callback under writer lock")
						} else {
							client.writeMu.Unlock()
						}
					}
					mu.Lock()
					defer mu.Unlock()
					if !accepts[event.WebSocketAccept] || event.Generation == 0 || event.RequestID == 0 || event.Method != "thread/turns/list" {
						t.Error("request identity does not match actual handshake")
					}
					if event.Phase == RequestStarted {
						if _, exists := starts[event.RequestID]; exists {
							t.Error("duplicate start")
						}
						starts[event.RequestID] = event
					} else if event.Phase == RequestFinished {
						first, ok := starts[event.RequestID]
						if !ok || first.WebSocketAccept != event.WebSocketAccept || first.Generation != event.Generation || finishes[event.RequestID].RequestID != 0 || !delivered["rpc:"+event.Method] {
							t.Error("unpaired finish or completion ordering")
						}
						finishes[event.RequestID] = event
					} else {
						t.Error("unknown request phase")
					}
				}
			}
			client = NewWithOptions(socket, options)
			defer client.Close()
			if err := client.Ensure(ctx); err != nil {
				t.Fatal(err)
			}
			// Encoding failure and private initialize have no public pair.
			if err := client.Request(ctx, "thread/turns/list", make(chan int), nil); err == nil {
				t.Fatal("encoding unexpectedly succeeded")
			}
			mu.Lock()
			before := len(starts)
			mu.Unlock()
			if before != 0 {
				t.Fatal("pre-start work emitted observation")
			}
			n := 1
			if mode == "concurrent" {
				n = 2
			}
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					err := client.Request(ctx, "thread/turns/list", nil, nil)
					if mode == "cancel" {
						if !errors.Is(err, context.Canceled) {
							t.Error("cancel cause lost")
						}
					} else if mode == "rpc" {
						var transport *TransportError
						if err == nil || errors.As(err, &transport) {
							t.Error("RPC refusal changed")
						}
					} else if err != nil {
						t.Error(err)
					}
				}()
			}
			wg.Wait()
			if mode == "reconnect" {
				client.connectionMu.Lock()
				conn, generation := client.connection, client.generation
				client.connectionMu.Unlock()
				client.markDisconnected(conn, generation, errors.New("public test disconnect"))
				if err := client.Request(ctx, "thread/turns/list", nil, nil); err != nil {
					t.Fatal(err)
				}
				n++
			}
			client.Close()
			connections := 1
			if mode == "reconnect" {
				connections = 2
			}
			for i := 0; i < connections; i++ {
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("peer did not join")
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if mode == "nil" {
				if len(starts) != 0 || client.connectionAccept != "" {
					t.Error("nil hook retained observation state")
				}
				return
			}
			if len(starts) != n || len(finishes) != n {
				t.Fatal("start/finish count mismatch")
			}
			for id, last := range finishes {
				if last.Failed != (mode == "rpc" || mode == "cancel") {
					t.Errorf("finish failure differs for %d", id)
				}
			}
			if mode == "reconnect" {
				identities := map[string]bool{}
				generations := map[uint64]bool{}
				for _, e := range starts {
					identities[e.WebSocketAccept] = true
					generations[e.Generation] = true
				}
				if len(identities) != 2 || len(generations) != 2 {
					t.Fatal("reconnect reused observation identity")
				}
			}
		})
	}
	t.Run("ensure_failure", func(t *testing.T) {
		count := 0
		client := NewWithOptions("/nonexistent/public-test.sock", ClientOptions{ObserveRequest: func(context.Context, RequestObservation) { count++ }})
		defer client.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := client.Request(ctx, "thread/turns/list", nil, nil); err == nil || count != 0 {
			t.Fatal(fmt.Sprint("pre-start failure observation ", count))
		}
	})
}

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

func TestCanceledWriteWaiterDoesNotDisconnectSibling(t *testing.T) {
	testCanceledWriteKeepsSibling(t, false)
}

func TestAlreadyCanceledWriteDoesNotDisconnect(t *testing.T) {
	testCanceledWriteKeepsSibling(t, true)
}

func testCanceledWriteKeepsSibling(t *testing.T, alreadyCanceled bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	finish := make(chan struct{})
	defer close(finish)
	releaseSibling := make(chan struct{})
	siblingReceived := make(chan struct{})
	frames := make(chan string, 8)
	var connections atomic.Int32
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		connections.Add(1)
		defer connection.CloseNow()
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		frames <- fmt.Sprint(request["method"])
		if request["method"] != "thread/read" {
			return fmt.Errorf("first request: %v", request["method"])
		}
		close(siblingReceived)
		select {
		case <-releaseSibling:
		case <-finish:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{"alive": true}}); err != nil {
			return err
		}
		for {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			frames <- fmt.Sprint(request["method"])
			if request["method"] == "thread/list" {
				if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{"barrier": true}}); err != nil {
					return err
				}
				select {
				case <-finish:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	})
	client := newTestClient(socket)
	defer client.Close()
	if err := client.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	client.connectionMu.Lock()
	connection, generation := client.connection, client.generation
	client.connectionMu.Unlock()
	siblingResult := make(chan error, 1)
	go func() {
		var result struct{ Alive bool }
		err := client.Request(ctx, "thread/read", map[string]any{"threadId": "sibling"}, &result)
		if err == nil && !result.Alive {
			err = errors.New("sibling did not receive its response")
		}
		siblingResult <- err
	}()
	select {
	case <-siblingReceived:
	case <-ctx.Done():
		t.Fatal("sibling did not reach the wire")
	}

	client.writeMu.Lock()
	locked := true
	defer func() {
		if locked {
			client.writeMu.Unlock()
		}
	}()
	canceled, cancelRequest := context.WithCancel(ctx)
	defer cancelRequest()
	if alreadyCanceled {
		cancelRequest()
	}
	canceledResult := make(chan error, 1)
	go func() {
		canceledResult <- client.Request(canceled, "thread/queue/list", map[string]any{"threadId": "canceled"}, nil)
	}()
	if !alreadyCanceled {
		waitFor(t, func() bool {
			client.pendingMu.Lock()
			defer client.pendingMu.Unlock()
			return len(client.pending) == 2
		})
	}
	cancelRequest()
	client.writeMu.Unlock()
	locked = false
	select {
	case err := <-canceledResult:
		var transport *TransportError
		if !errors.As(err, &transport) || !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled write: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("canceled write did not return")
	}
	assertConnection := func() {
		t.Helper()
		client.connectionMu.Lock()
		defer client.connectionMu.Unlock()
		if client.connection != connection || client.generation != generation || client.ready != generation {
			t.Fatal("cancellation before Write retired the shared connection or generation")
		}
	}
	assertConnection()
	close(releaseSibling)
	select {
	case err := <-siblingResult:
		if err != nil {
			t.Fatalf("live sibling failed: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("live sibling did not complete")
	}
	var barrier struct{ Barrier bool }
	if err := client.Request(ctx, "thread/list", map[string]any{}, &barrier); err != nil || !barrier.Barrier {
		t.Fatalf("wire barrier: %#v, %v", barrier, err)
	}
	assertConnection()
	if connections.Load() != 1 {
		t.Fatal("cancellation required a replacement connection")
	}
	for _, method := range []string{"thread/read", "thread/list"} {
		select {
		case actual := <-frames:
			if actual != method {
				t.Fatalf("received frame %q, want %q; canceled RPC reached the wire", actual, method)
			}
		default:
			t.Fatalf("missing frame %s", method)
		}
	}
	select {
	case method := <-frames:
		t.Fatalf("unexpected frame after barrier: %s", method)
	default:
	}
}

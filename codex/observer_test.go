package codex

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Report when a read reaches the admission select, without a timing sleep.
type observerAdmissionContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (ctx *observerAdmissionContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
}

type observerTestRequest struct {
	connection *websocket.Conn
	message    map[string]any
	generation int32
}

func startManyWatchObserver(t *testing.T) (*Client, <-chan *websocket.Conn, <-chan observerTestRequest) {
	t.Helper()
	connections := make(chan *websocket.Conn, 8)
	requests := make(chan observerTestRequest, 100)
	var generation atomic.Int32
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		current := generation.Add(1)
		if err := handshake(connection); err != nil {
			return err
		}
		connections <- connection
		for {
			request, err := readObject(connection)
			if err != nil {
				return nil
			}
			if request["method"] == "thread/resume" {
				params := request["params"].(map[string]any)
				if len(params) != 2 || params["excludeTurns"] != true {
					return fmt.Errorf("observer changed request authority: %v", params)
				}
			}
			if current == 1 {
				if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{}}); err != nil {
					return err
				}
			} else {
				requests <- observerTestRequest{connection, request, current}
			}
		}
	})
	client := NewWithOptions(socket, ClientOptions{ObserverOnly: true})
	var unsubscribe []func()
	t.Cleanup(func() {
		client.Close()
		for _, stop := range unsubscribe {
			stop()
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for n := 0; n < 40; n++ {
		_, stop, err := client.Subscribe(ctx, fmt.Sprintf("thread-%d", n))
		if err != nil {
			t.Fatal(err)
		}
		unsubscribe = append(unsubscribe, stop)
	}
	return client, connections, requests
}

func observerReconnect(t *testing.T, client *Client, connections <-chan *websocket.Conn) *websocket.Conn {
	t.Helper()
	var previous *websocket.Conn
	select {
	case previous = <-connections:
	case <-time.After(time.Second):
		t.Fatal("missing server connection")
	}
	previous.CloseNow()
	waitFor(t, func() bool {
		client.connectionMu.Lock()
		defer client.connectionMu.Unlock()
		return client.connection == nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	return previous
}

func nextObserverRequest(t *testing.T, requests <-chan observerTestRequest) observerTestRequest {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-time.After(3 * time.Second):
		t.Fatal("observer request did not progress")
	}
	return observerTestRequest{}
}

func answerObserverRequest(t *testing.T, request observerTestRequest) {
	t.Helper()
	if err := writeObject(request.connection, map[string]any{"id": request.message["id"], "result": map[string]any{}}); err != nil {
		t.Fatal(err)
	}
}

func TestObserverReconnectSharesAdmissionWithNormalReads(t *testing.T) {
	client, connections, requests := startManyWatchObserver(t)
	observerReconnect(t, client, connections)
	active := make([]observerTestRequest, 0, 4)
	for range 4 {
		active = append(active, nextObserverRequest(t, requests))
	}
	for _, request := range active {
		if request.message["method"] != "thread/resume" {
			t.Fatalf("restore = %v", request.message)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	err := client.Request(ctx, "thread/read", map[string]any{"threadId": "cancelled-read", "excludeTurns": true}, nil)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued read cancellation = %v", err)
	}
	select {
	case request := <-requests:
		t.Fatalf("observer exceeded four admitted RPCs: %v", request.message)
	default:
	}
	client.pendingMu.Lock()
	pending := len(client.pending)
	client.pendingMu.Unlock()
	if pending != 4 {
		t.Fatalf("pending RPCs=%d, want four", pending)
	}
	// An ordinary metadata read joins the same queue as reconnect resumes.
	readCtx, readCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer readCancel()
	readDone := make(chan error, 1)
	readAdmission := &observerAdmissionContext{Context: readCtx, waiting: make(chan struct{})}
	go func() {
		readDone <- client.Request(readAdmission, "thread/read", map[string]any{"threadId": "http-read", "excludeTurns": true}, nil)
	}()
	<-readAdmission.waiting
	for _, request := range active {
		answerObserverRequest(t, request)
	}
	resumed, readAt := 4, 0
	for resumed < 40 || readAt == 0 {
		request := nextObserverRequest(t, requests)
		if request.generation != 2 {
			t.Fatalf("request used stale generation %d", request.generation)
		}
		switch request.message["method"] {
		case "thread/resume":
			resumed++
		case "thread/read":
			if request.message["params"].(map[string]any)["threadId"] != "http-read" {
				t.Fatal("cancelled queued read reached the server")
			}
			readAt = resumed
		default:
			t.Fatalf("unexpected observer request %v", request.message)
		}
		answerObserverRequest(t, request)
	}
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	if readAt == 40 {
		t.Fatal("ordinary read was starved by the entire reconnect queue")
	}
	waitFor(t, func() bool {
		client.watchedMu.Lock()
		defer client.watchedMu.Unlock()
		for _, generation := range client.watchedGeneration {
			if generation != 2 {
				return false
			}
		}
		return true
	})
}

func TestObserverDisconnectAndCloseCancelQueuedRestoration(t *testing.T) {
	client, connections, requests := startManyWatchObserver(t)
	for generation := int32(2); generation <= 4; generation++ {
		observerReconnect(t, client, connections)
		for range 4 {
			request := nextObserverRequest(t, requests)
			if request.generation != generation || request.message["method"] != "thread/resume" {
				t.Fatalf("stale queued request = %#v", request)
			}
		}
		queuedDone := make(chan error, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		admission := &observerAdmissionContext{Context: ctx, waiting: make(chan struct{})}
		go func() {
			queuedDone <- client.Request(admission, "thread/read", map[string]any{"threadId": "old-generation", "excludeTurns": true}, nil)
		}()
		<-admission.waiting
		client.connectionMu.Lock()
		connection, current := client.connection, client.generation
		client.connectionMu.Unlock()
		if generation == 4 {
			client.Close()
		} else {
			connection.CloseNow()
			client.markDisconnected(connection, current, errors.New("test generation ended"))
		}
		select {
		case err := <-queuedDone:
			if err == nil || !strings.Contains(err.Error(), "disconnected") {
				t.Fatalf("queued old generation = %v", err)
			}
		case <-ctx.Done():
			t.Fatal("disconnection did not cancel queued admission")
		}
		cancel()
		waitFor(t, func() bool { client.pendingMu.Lock(); defer client.pendingMu.Unlock(); return len(client.pending) == 0 })
		client.observer.mu.Lock()
		remaining := len(client.observer.queue)
		client.observer.mu.Unlock()
		if remaining != 0 {
			t.Fatalf("cancelled generation retained %d queued watches", remaining)
		}
		for n := 0; n < 40; n++ {
			if prompts := client.Prompts(fmt.Sprintf("thread-%d", n)); len(prompts) != 0 {
				t.Fatalf("stale restoration published a notice: %#v", prompts)
			}
		}
		select {
		case request := <-requests:
			t.Fatalf("cancelled generation sent extra work: %#v", request)
		default:
		}
	}
}

func TestObserverAcceptedResumeSurvivesImmediateDisconnect(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/resume" {
			return fmt.Errorf("expected resume: %v", request)
		}
		if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{}}); err != nil {
			return err
		}
		return connection.Close(websocket.StatusInternalError, "disconnect after accepted resume")
	})
	client := NewWithOptions(socket, ClientOptions{ObserverOnly: true})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, stop, err := client.Subscribe(ctx, "thread")
	if err != nil {
		t.Fatalf("accepted observer subscription failed: %v", err)
	}
	defer stop()
	waitFor(t, func() bool {
		client.connectionMu.Lock()
		defer client.connectionMu.Unlock()
		return client.connection == nil
	})
	client.watchedMu.Lock()
	accepted := client.watchedGeneration["thread"]
	client.watchedMu.Unlock()
	if accepted != 1 {
		t.Fatalf("accepted generation=%d", accepted)
	}
}

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

func TestObserverListQueueStaysReadOnly(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for page := 0; page < 2; page++ {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			params, _ := request["params"].(map[string]any)
			if request["method"] != "thread/queue/list" || params["threadId"] != "cold-thread" || params["limit"] != float64(100) {
				return fmt.Errorf("observer sent something other than a queue read: %v", request)
			}
			if page == 0 {
				if len(params) != 2 {
					return fmt.Errorf("unexpected first-page parameters: %v", params)
				}
				for _, method := range []string{"item/tool/requestUserInput", "future/unsupported"} {
					if err := writeObject(connection, map[string]any{
						"id": method, "method": method, "params": map[string]any{
							"threadId": "cold-thread", "turnId": "turn", "isBlocking": false,
							"questions": []any{map[string]any{"id": "q", "header": "Question", "question": "Secret", "isOther": false, "isSecret": false}},
						},
					}); err != nil {
						return err
					}
				}
			} else if len(params) != 3 || params["cursor"] != "next" {
				return fmt.Errorf("unexpected next-page parameters: %v", params)
			}
			var cursor any
			if page == 0 {
				cursor = "next"
			}
			if err := writeObject(connection, map[string]any{
				"id": request["id"], "result": map[string]any{
					"data": []any{map[string]any{
						"id": fmt.Sprintf("queued-%d", page), "clientUserMessageId": fmt.Sprintf("client-%d", page),
						"input": []any{map[string]any{"type": "text", "text": fmt.Sprintf("message-%d", page)}},
					}}, "nextCursor": cursor,
				},
			}); err != nil {
				return err
			}
		}
		// This final read also fences the rejected mutations and prompt responses:
		// no resume, setting write, answer or rejection may precede it on the wire.
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/read" {
			return fmt.Errorf("observer sent a mutation or prompt response: %v", request)
		}
		return writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"thread": map[string]any{"id": "cold-thread", "cwd": "/trusted"}},
		})
	})
	client := NewWithOptions(socket, ClientOptions{
		ObserverOnly: true, DeveloperInstructions: "must not be sent",
		AllowImplicitResume: func(string) bool {
			t.Error("cold queue observation attempted an implicit resume")
			return false
		},
		NonBlockingUserInput: &NonBlockingUserInputPolicy{},
	})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entries, err := client.ListQueue(ctx, "cold-thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("queue entries = %#v", entries)
	}
	for page, entry := range entries {
		if entry.ID != fmt.Sprintf("queued-%d", page) || entry.ClientUserMessageID != fmt.Sprintf("client-%d", page) || entry.Text != fmt.Sprintf("message-%d", page) {
			t.Fatalf("queue entry %d = %#v", page, entry)
		}
	}
	for _, method := range []string{
		"thread/start", "thread/resume", "thread/settings/update",
		"thread/queue/add", "thread/queue/update", "thread/queue/start", "thread/queue/delete",
		"turn/start", "turn/steer", "turn/interrupt",
	} {
		if err := client.Request(ctx, method, map[string]any{"threadId": "cold-thread"}, nil); err == nil || !strings.Contains(err.Error(), "observer client cannot") {
			t.Fatalf("observer allowed %s: %v", method, err)
		}
	}
	if prompts := client.Prompts("cold-thread"); len(prompts) != 0 {
		t.Fatalf("observer retained prompts: %#v", prompts)
	}
	if err := client.RespondAnswers(ctx, `"item/tool/requestUserInput"`, "cold-thread", map[string]map[string][]string{"q": {"answers": {"secret"}}}); err == nil {
		t.Fatal("observer allowed a prompt answer")
	}
	if err := client.VerifyThread(ctx, "cold-thread", "/trusted"); err != nil {
		t.Fatal(err)
	}
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

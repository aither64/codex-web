package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type observerEventLog struct {
	mu     sync.Mutex
	events []ObserverEvent
}

func TestObserverReadCloseAnnotation(t *testing.T) {
	for _, test := range []struct {
		name                string
		status              websocket.StatusCode
		oversize, malformed bool
		want                string
	}{
		{name: "normal", status: websocket.StatusNormalClosure, want: "normal-close"},
		{name: "going-away", status: websocket.StatusGoingAway, want: "normal-close"},
		{name: "EOF", want: "read-error"},
		{name: "protocol", status: websocket.StatusProtocolError, want: "read-error"},
		{name: "size", oversize: true, want: "read-error"},
		{name: "unknown", status: 4001, want: "read-error"},
		{name: "malformed-then-normal", status: websocket.StatusNormalClosure, malformed: true, want: "normal-close"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var log observerEventLog
			send := make(chan struct{})
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				<-send
				if test.malformed {
					if err := connection.Write(ctx, websocket.MessageText, []byte(`{broken`)); err != nil {
						return err
					}
				}
				if test.oversize {
					_ = connection.Write(ctx, websocket.MessageText, bytes.Repeat([]byte{'x'}, 2048))
					connection.CloseNow()
				} else if test.status == 0 {
					connection.CloseNow()
				} else {
					_ = connection.Close(test.status, "synthetic closure")
				}
				return nil
			})
			client := NewWithOptions(socket, ClientOptions{ObserverOnly: true, ObserveEvent: log.add})
			defer client.Close()
			if err := client.Ensure(ctx); err != nil {
				t.Fatal(err)
			}
			if test.oversize {
				client.connectionMu.Lock()
				client.connection.SetReadLimit(1024)
				client.connectionMu.Unlock()
			}
			close(send)
			waitFor(t, func() bool {
				for _, e := range log.snapshot() {
					if e.Kind == ObserverDisconnected {
						return true
					}
				}
				return false
			})
			events := log.snapshot()
			end := len(events) - 1
			if end < 1 || events[end].Kind != ObserverDisconnected || events[end-1].Kind != ObserverInvalid || events[end-1].Reason != test.want {
				t.Fatalf("close boundaries %+v", events)
			}
			if test.malformed && (end < 2 || events[end-2].Kind != ObserverInvalid || events[end-2].Reason != "decode-error") {
				t.Fatalf("normal close erased prior invalidation %+v", events)
			}
			if _, ready := client.ObserverGeneration(); ready {
				t.Fatal("close retained connected generation")
			}
		})
	}
	// Untyped errors and text that resembles a close code cannot acquire the
	// annotation. Wrapped typed statuses retain their native meaning.
	for _, err := range []error{io.EOF, errors.New("1000 normal closure"), context.Canceled} {
		if got := observerReadErrorReason(err); got != "read-error" {
			t.Fatalf("untyped close %v: %s", err, got)
		}
	}
	if got := observerReadErrorReason(fmt.Errorf("wrapped: %w", websocket.CloseError{Code: websocket.StatusGoingAway})); got != "normal-close" {
		t.Fatal(got)
	}
}

func (log *observerEventLog) add(event ObserverEvent) {
	log.mu.Lock()
	defer log.mu.Unlock()
	log.events = append(log.events, event)
}

func (log *observerEventLog) snapshot() []ObserverEvent {
	log.mu.Lock()
	defer log.mu.Unlock()
	return append([]ObserverEvent(nil), log.events...)
}

func TestObserverEventsOrderedDetachedBeforeCoalescing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	finish := make(chan struct{})
	defer close(finish)
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/resume" {
			return fmt.Errorf("unexpected request %v", request)
		}
		// A start before the resume reply cannot acquire watch coverage.
		if err := writeObject(connection, map[string]any{"method": "turn/started", "params": map[string]any{"threadId": "actor", "turn": map[string]any{"id": "late"}}}); err != nil {
			return err
		}
		if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{}}); err != nil {
			return err
		}
		for index := 0; index < 3; index++ {
			if err := writeObject(connection, map[string]any{"id": fmt.Sprintf("server-%d", index), "method": "item/tool/requestUserInput", "params": map[string]any{"threadId": "actor", "turnId": "future", "index": index}}); err != nil {
				return err
			}
		}
		<-finish
		return nil
	})
	var log observerEventLog
	var client *Client
	client = NewWithOptions(socket, ClientOptions{ObserverOnly: true, ObserveEvent: func(event ObserverEvent) {
		// These locks are never held by the publisher while it calls us.
		for _, lock := range []*sync.Mutex{&client.connectionMu, &client.pendingMu, &client.watchedMu} {
			lock.Lock()
			lock.Unlock()
		}
		copy := event
		copy.Params = append(json.RawMessage(nil), event.Params...)
		copy.RequestID = append(json.RawMessage(nil), event.RequestID...)
		log.add(copy)
		// Recipient ownership must not corrupt native decoding or another event.
		for i := range event.Params {
			event.Params[i] = 'x'
		}
		for i := range event.RequestID {
			event.RequestID[i] = 'x'
		}
	}})
	defer client.Close()
	wake, unsubscribe, err := client.Subscribe(ctx, "actor")
	if err != nil {
		t.Fatal(err)
	}
	defer unsubscribe()
	waitFor(t, func() bool { return len(log.snapshot()) >= 6 })
	events := log.snapshot()
	want := []ObserverEventKind{ObserverConnected, ObserverNotification, ObserverWatchEstablished, ObserverNotification, ObserverNotification, ObserverNotification}
	for index, kind := range want {
		if events[index].Kind != kind || events[index].Sequence != uint64(index+1) || events[index].Generation != 1 {
			t.Fatalf("event %d = %+v", index, events[index])
		}
	}
	if events[2].ThreadID != "actor" || !bytes.Contains(events[1].Params, []byte(`"late"`)) {
		t.Fatalf("watch boundary: %+v", events)
	}
	for index, event := range events[3:6] {
		if string(event.RequestID) != fmt.Sprintf(`"server-%d"`, index) || !json.Valid(event.Params) {
			t.Fatalf("detached event %+v", event)
		}
	}
	if len(wake) != 1 {
		t.Fatalf("wakeups were not coalesced: %d", len(wake))
	}
	if len(client.Prompts("actor")) != 0 {
		t.Fatal("event tap changed observer request policy")
	}
	// Final local retirement is visible even when wire unsubscribe is frozen.
	waitFor(t, func() bool { client.pendingMu.Lock(); defer client.pendingMu.Unlock(); return len(client.pending) == 0 })
	if err := client.FreezeObserverWrites(ctx, 1); err != nil {
		t.Fatal(err)
	}
	unsubscribe()
	events = log.snapshot()
	if events[len(events)-1].Kind != ObserverWatchRemoved {
		t.Fatalf("missing watch removal: %+v", events)
	}
}

func TestObserverEventsInvalidationAndGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var generation atomic.Int32
	var log observerEventLog
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		current := generation.Add(1)
		defer connection.CloseNow()
		if err := handshake(connection); err != nil {
			return err
		}
		if current == 1 {
			for _, frame := range [][]byte{[]byte(`{broken`), []byte(`{}`), append([]byte(`{"method":"too/large","params":{"value":"`), append(bytes.Repeat([]byte{'x'}, ObserverEventLimit), []byte(`"}}`)...)...)} {
				if err := connection.Write(ctx, websocket.MessageText, frame); err != nil {
					return err
				}
			}
			if err := connection.Write(ctx, websocket.MessageBinary, []byte(`{"method":"binary"}`)); err != nil {
				return err
			}
			return nil
		}
		_, _, _ = connection.Read(ctx)
		return nil
	})
	client := NewWithOptions(socket, ClientOptions{ObserverOnly: true, ObserveEvent: log.add})
	defer client.Close()
	// The server may finish immediately after initialization.
	_ = client.Ensure(ctx)
	waitFor(t, func() bool {
		for _, e := range log.snapshot() {
			if e.Kind == ObserverDisconnected {
				return true
			}
		}
		return false
	})
	if err := client.FreezeObserverWrites(ctx, 1); err == nil {
		t.Fatal("froze disconnected generation")
	}
	if err := client.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.FreezeObserverWrites(ctx, 1); err == nil {
		t.Fatal("froze stale generation")
	}
	if gen, ready := client.ObserverGeneration(); gen != 2 || !ready {
		t.Fatalf("generation %d ready %v", gen, ready)
	}
	client.Close()
	events := log.snapshot()
	reasons := map[string]bool{}
	for index, event := range events {
		if event.Sequence != uint64(index+1) {
			t.Fatalf("sequence %+v", events)
		}
		if event.Kind == ObserverInvalid {
			reasons[event.Reason] = true
			if len(event.Params) != 0 {
				t.Fatal("invalid frame copied payload")
			}
		}
	}
	for _, reason := range []string{"decode-error", "invalid-envelope", "frame-too-large", "non-text-frame"} {
		if !reasons[reason] {
			t.Fatalf("missing %s: %+v", reason, events)
		}
	}
	closed := false
	for _, event := range events {
		closed = closed || event.Kind == ObserverClosed
	}
	if !closed {
		t.Fatalf("missing close %+v", events)
	}
}

func TestObserverFreezeJoinsWritesAndKeepsReader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var connections atomic.Int32
	seen := make(chan map[string]any, 8)
	send := make(chan struct{})
	afterFreeze := make(chan struct{})
	pingDone := make(chan error, 1)
	finished := make(chan struct{})
	defer close(finished)
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		connections.Add(1)
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		seen <- request
		<-send
		if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{}}); err != nil {
			return err
		}
		// The server can still notify and use WebSocket protocol control frames.
		request, err = readObject(connection)
		if err != nil {
			return err
		}
		seen <- request
		<-afterFreeze
		go func() {
			request, err := readObject(connection)
			if err == nil {
				seen <- request
			}
		}()
		if err := writeObject(connection, map[string]any{"method": "turn/started", "params": map[string]any{"threadId": "actor", "turn": map[string]any{"id": "after-freeze"}}}); err != nil {
			return err
		}
		pingDone <- connection.Ping(ctx)
		<-finished
		return nil
	})
	var log observerEventLog
	client := NewWithOptions(socket, ClientOptions{ObserverOnly: true, ObserveEvent: log.add})
	defer client.Close()
	if err := client.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- client.Request(ctx, "thread/read", map[string]any{"threadId": "actor"}, nil) }()
	<-seen
	if err := client.FreezeObserverWrites(ctx, 1); err == nil {
		t.Fatal("freeze accepted pending response")
	}
	close(send)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	// An admitted writer holds writeMu. Freeze must wait for its frame close.
	entered := make(chan struct{})
	release := make(chan struct{})
	writeResult := make(chan error, 1)
	client.connectionMu.Lock()
	connection := client.connection
	client.connectionMu.Unlock()
	go func() {
		writeResult <- client.writeOfferAdmission(ctx, connection, 1, []byte(`{"method":"test/barrier"}`), nil, func() error { close(entered); <-release; return nil })
	}()
	<-entered
	freezeResult := make(chan error, 1)
	go func() { freezeResult <- client.FreezeObserverWrites(ctx, 1) }()
	select {
	case err := <-freezeResult:
		t.Fatalf("freeze passed active writer: %v", err)
	default:
	}
	close(release)
	if err := <-writeResult; err != nil {
		t.Fatal(err)
	}
	if err := <-freezeResult; err != nil {
		t.Fatal(err)
	}
	if request := <-seen; request["method"] != "test/barrier" {
		t.Fatalf("writer %v", request)
	}
	close(afterFreeze)
	for _, call := range []func() error{
		func() error { return client.Ensure(ctx) },
		func() error { return client.Request(ctx, "thread/read", nil, nil) },
		func() error { return client.requestConnected(ctx, "thread/unsubscribe", nil, nil) },
		func() error { return client.writeOn(ctx, connection, 1, []byte(`{"method":"forbidden"}`)) },
	} {
		if err := call(); !errors.Is(err, ErrObserverWritesFrozen) {
			t.Fatalf("frozen call: %v", err)
		}
	}
	waitFor(t, func() bool {
		for _, event := range log.snapshot() {
			if bytes.Contains(event.Params, []byte("after-freeze")) {
				return true
			}
		}
		return false
	})
	if gen, ready := client.ObserverGeneration(); gen != 1 || !ready {
		t.Fatalf("reader changed: %d %v", gen, ready)
	}
	if err := <-pingDone; err != nil {
		t.Fatalf("protocol ping after freeze: %v", err)
	}
	select {
	case request := <-seen:
		t.Fatalf("application write after freeze: %v", request)
	default:
	}
	client.connectionMu.Lock()
	client.connectionMu.Unlock()
	client.markDisconnected(connection, 1, errors.New("fixture disconnect"))
	if err := client.Ensure(ctx); !errors.Is(err, ErrObserverWritesFrozen) {
		t.Fatalf("frozen reconnect %v", err)
	}
	if connections.Load() != 1 {
		t.Fatalf("new native connection: %d", connections.Load())
	}
}

func TestObserverFreezeRequiresOptInAndHonorsCancellation(t *testing.T) {
	for _, options := range []ClientOptions{{}, {ObserverOnly: true}, {ObserveEvent: func(ObserverEvent) {}}} {
		client := NewWithOptions("/not-used", options)
		if err := client.FreezeObserverWrites(context.Background(), 1); err == nil {
			t.Fatal("freeze without observer tap")
		}
		client.Close()
	}
	client := NewWithOptions("/not-used", ClientOptions{ObserverOnly: true, ObserveEvent: func(ObserverEvent) {}})
	defer client.Close()
	client.writeMu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.FreezeObserverWrites(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	client.writeMu.Unlock()
}

func TestObserverFreezeBlocksAdmittedWatchRestoration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var frames atomic.Int32
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		defer connection.CloseNow()
		if err := handshake(connection); err != nil {
			return err
		}
		for {
			if _, err := readObject(connection); err != nil {
				return nil
			}
			frames.Add(1)
		}
	})
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	client := NewWithOptions(socket, ClientOptions{
		ObserverOnly: true, ObserveEvent: func(ObserverEvent) {},
		AllowImplicitResume: func(string) bool { once.Do(func() { close(entered) }); <-release; return true },
	})
	defer client.Close()
	result := make(chan error, 1)
	go func() {
		_, unsubscribe, err := client.Subscribe(ctx, "actor")
		if unsubscribe != nil {
			unsubscribe()
		}
		result <- err
	}()
	<-entered
	if err := client.FreezeObserverWrites(ctx, 1); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-result; !errors.Is(err, ErrObserverWritesFrozen) {
		t.Fatalf("watch restoration: %v", err)
	}
	// Ensure and unsubscribe cannot write after the held resume admission loses.
	client.unsubscribeThread("actor")
	client.Close()
	if frames.Load() != 0 {
		t.Fatalf("frozen observer sent %d application frames", frames.Load())
	}
}

func TestObserverMalformedWatchReplyCannotEstablish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var log observerEventLog
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		return writeObject(connection, map[string]any{"id": request["id"], "result": "malformed"})
	})
	client := NewWithOptions(socket, ClientOptions{ObserverOnly: true, ObserveEvent: log.add})
	defer client.Close()
	if _, _, err := client.Subscribe(ctx, "actor"); err == nil {
		t.Fatal("malformed resume accepted")
	}
	invalid := false
	for _, event := range log.snapshot() {
		if event.Kind == ObserverWatchEstablished {
			t.Fatal("malformed resume established coverage")
		}
		invalid = invalid || event.Kind == ObserverInvalid && event.Reason == "invalid-watch-reply"
	}
	if !invalid {
		t.Fatal("malformed resume did not invalidate tap")
	}
}

func TestObserverFrozenTailAfterRecorderClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ready := make(chan *websocket.Conn, 1)
	finish := make(chan struct{})
	defer close(finish)
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{}}); err != nil {
			return err
		}
		ready <- connection
		<-finish
		return nil
	})
	dir := filepath.Join(t.TempDir(), "activity")
	recorder, err := NewActivityRecorder(dir)
	if err != nil {
		t.Fatal(err)
	}
	var log observerEventLog
	client := NewWithOptions(socket, ClientOptions{ObserverOnly: true, ObserveEvent: log.add, ActivityRecorder: recorder})
	defer client.Close()
	_, unsubscribe, err := client.Subscribe(ctx, "actor")
	if err != nil {
		t.Fatal(err)
	}
	defer unsubscribe()
	connection := <-ready
	if err := client.FreezeObserverWrites(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := recorder.Close(); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	if err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			files[path] = string(data)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := writeObject(connection, map[string]any{"method": "turn/started", "params": map[string]any{"threadId": "actor", "turn": map[string]any{"id": "tail"}}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		for _, event := range log.snapshot() {
			if bytes.Contains(event.Params, []byte(`"tail"`)) {
				return true
			}
		}
		return false
	})
	// Fence normal recorder processing after the synchronous tap.
	client.connectionMu.Lock()
	connectionClient := client.connection
	client.connectionMu.Unlock()
	client.markDisconnected(connectionClient, 1, errors.New("end fixture"))
	for path, want := range files {
		data, err := os.ReadFile(path)
		if err != nil || string(data) != want {
			t.Fatalf("closed recorder wrote %s: %v", path, err)
		}
	}
}

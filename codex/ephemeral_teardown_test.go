package codex

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// A real frame has returned from Read, but its application processing is held
// behind a channel. This exercises the reader barrier without scheduler sleeps
// or a production transport/test hook.
func pausedEphemeralReader(t *testing.T, frame any, successful bool) (*Client, <-chan struct{}, func(), <-chan struct{}) {
	t.Helper()
	serverClosed := make(chan struct{})
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		defer close(serverClosed)
		defer connection.CloseNow()
		if err := writeObject(connection, frame); err != nil {
			return err
		}
		_, _, _ = connection.Read(context.Background())
		return nil
	})
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	connection, _, err := websocket.Dial(context.Background(), "ws://localhost/", &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	_, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sink := &ephemeralSink{cancel: cancel, changed: make(chan struct{}, 1), generation: 1,
		threadID: "utility-thread", turnID: "utility-turn", turnIssued: true}
	if successful {
		sink.finalID, sink.finalText, sink.completed, sink.settled = "answer", "first answer", true, true
	} else {
		for _, value := range []any{
			map[string]any{"method": "error", "params": ephemeralErrorParams("serverOverloaded", false)},
			ephemeralCompletion("failed", "serverOverloaded", []any{}),
		} {
			raw, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			var message rpcMessage
			if !sink.decode(raw, &message) {
				t.Fatal("invalid initial evidence")
			}
			sink.consumeLocked(message) // No reader has launched yet.
		}
	}
	client := New(socket)
	client.ephemeral = sink
	client.connection, client.generation, client.ready = connection, 1, 1
	reader := &ephemeralReader{connection: connection, generation: 1, done: make(chan struct{})}
	client.ephemeralReader = reader // Published before the reader is launched.
	read := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	go func() {
		defer close(reader.done)
		kind, raw, err := connection.Read(context.Background())
		close(read)
		<-release
		if err == nil {
			var message rpcMessage
			if kind != websocket.MessageText {
				sink.fail(ErrEphemeralProtocol)
			} else if sink.decode(raw, &message) {
				sink.handle(client, connection, 1, message)
			}
		}
		client.markDisconnected(connection, 1, errors.New("reader exited"))
	}()
	t.Cleanup(func() {
		unblock()
		client.Close()
		select {
		case <-reader.done:
		case <-time.After(time.Second):
			t.Error("paused reader fixture did not exit")
		}
	})
	<-read
	return client, serverClosed, unblock, reader.done
}

func TestEphemeralPrivateReaderShutdown(t *testing.T) {
	for _, mode := range []string{"duplicate", "contradiction", "async", "success async", "unexpected disconnect", "wrong generation", "missing reader", "timeout", "public close"} {
		t.Run(mode, func(t *testing.T) {
			var frame any = ephemeralCompletion("failed", "serverOverloaded", []any{})
			if mode == "contradiction" {
				frame = ephemeralCompletion("completed", "serverOverloaded", []any{})
			}
			if mode == "async" || mode == "success async" {
				frame = map[string]any{"method": "item/started", "params": map[string]any{
					"threadId": "utility-thread", "turnId": "utility-turn", "item": map[string]any{
						"id": "question", "type": "agentMessage", "delivery": "async", "text": "PRIVATE_QUESTION"}}}
			}
			client, socketClosed, release, readerDone := pausedEphemeralReader(t, frame, mode == "success async")
			generation := uint64(1)
			if mode == "wrong generation" {
				generation = 2
			}
			if mode == "missing reader" {
				client.connectionMu.Lock()
				client.ephemeralReader = nil
				client.connectionMu.Unlock()
			}
			if mode == "unexpected disconnect" {
				client.markDisconnected(client.ephemeralReader.connection, 1, errors.New("unexpected transport failure"))
			}
			if mode == "public close" {
				client.Close()
			}
			budget := ephemeralCleanup
			if mode == "timeout" {
				budget = 25 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			finished := make(chan bool, 1)
			go func() { finished <- client.closeEphemeral(ctx, generation) }()
			<-socketClosed // The exact detach/CloseNow has occurred; processing is paused.
			if mode == "missing reader" || mode == "timeout" {
				if <-finished {
					t.Fatal("missing or unjoined reader authorized final proof")
				}
				release()
			} else {
				select {
				case <-finished:
					t.Fatal("shutdown returned before already-read evidence was processed")
				default:
				}
				release()
				wantJoin := mode == "duplicate" || mode == "contradiction" || mode == "async" || mode == "success async"
				if joined := <-finished; joined != wantJoin {
					t.Fatalf("joined=%t want=%t", joined, wantJoin)
				}
			}
			<-readerDone
			wantCandidate := mode == "duplicate" || mode == "timeout"
			if candidate := client.ephemeral.canRetry(1, "utility-thread", "utility-turn"); candidate != wantCandidate {
				t.Fatalf("candidate=%t want=%t", candidate, wantCandidate)
			}
			if mode == "success async" && client.ephemeral.failure() == nil {
				t.Fatal("late processed question preserved sampled success")
			}
		})
	}
}

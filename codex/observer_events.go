package codex

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/coder/websocket"
)

// ObserverEventKind identifies transport and watch boundaries as well as native
// notifications. Every loss of coverage is explicit; none grants new coverage.
type ObserverEventKind string

const (
	ObserverConnected        ObserverEventKind = "connected"
	ObserverNotification     ObserverEventKind = "notification"
	ObserverWatchEstablished ObserverEventKind = "watch-established"
	ObserverWatchRemoved     ObserverEventKind = "watch-removed"
	ObserverDisconnected     ObserverEventKind = "disconnected"
	ObserverClosed           ObserverEventKind = "closed"
	ObserverInvalid          ObserverEventKind = "invalid"
)

// ObserverEvent is detached from the decoder and the client's mutable state.
// Params and RequestID belong to this invocation and may be retained or changed
// by its recipient. RPC result bodies are never included. Sequence increases
// across connections; Generation identifies one connection, not a native epoch.
type ObserverEvent struct {
	Kind                 ObserverEventKind
	Generation, Sequence uint64
	ThreadID, Method     string
	RequestID, Params    json.RawMessage
	Reason               string
}

// ObserverEventLimit bounds a tapped frame. Larger frames invalidate observation
// without changing the ordinary client's decoder or transport read limit.
const ObserverEventLimit = 256 * 1024

var ErrObserverWritesFrozen = errors.New("Codex observer writes are frozen")

func observerReadErrorReason(err error) string {
	// CloseStatus uses errors.As, never the error text. Even an orderly peer
	// close invalidates coverage; this annotation grants no recovery authority.
	switch websocket.CloseStatus(err) {
	case websocket.StatusNormalClosure, websocket.StatusGoingAway:
		return "normal-close"
	default:
		return "read-error"
	}
}

// No dispatcher goroutine or event queue: concurrent lifecycle producers wait
// for the current synchronous callback. The mutex is released before invoking
// application code. Callers must also release every other client lock first.
type observerEvents struct {
	mu       sync.Mutex
	changed  *sync.Cond
	active   bool
	sequence uint64
}

func (c *Client) observeEvent(event ObserverEvent) {
	if c.events == nil {
		return
	}
	c.beginObserverEvent()
	defer c.endObserverEvent()
	c.deliverObserverEvent(event)
}

func (c *Client) beginObserverEvent() {
	s := c.events
	s.mu.Lock()
	for s.active {
		s.changed.Wait()
	}
	s.active = true
	s.mu.Unlock()
}

func (c *Client) endObserverEvent() {
	s := c.events
	s.mu.Lock()
	s.active = false
	s.changed.Broadcast()
	s.mu.Unlock()
}

// The event producer owns the serial slot, but holds no client mutex here.
func (c *Client) deliverObserverEvent(event ObserverEvent) {
	c.events.sequence++
	event.Sequence = c.events.sequence
	c.options.ObserveEvent(event)
}

func (c *Client) observeWatchEstablished(generation uint64, thread string) {
	c.beginObserverEvent()
	defer c.endObserverEvent()
	c.connectionMu.Lock()
	live := c.connection != nil && c.generation == generation && !c.closed
	c.connectionMu.Unlock()
	c.watchedMu.Lock()
	watched := c.watched[thread] > 0
	c.watchedMu.Unlock()
	if live && watched {
		c.deliverObserverEvent(ObserverEvent{Kind: ObserverWatchEstablished, Generation: generation, ThreadID: thread})
	}
}

func (c *Client) observeNotification(generation uint64, message rpcMessage) {
	if c.events == nil || message.Method == "" {
		return
	}
	c.observeEvent(ObserverEvent{
		Kind: ObserverNotification, Generation: generation,
		ThreadID: threadIDFromParams(message.Params), Method: message.Method,
		RequestID: append(json.RawMessage(nil), message.ID...),
		Params:    append(json.RawMessage(nil), message.Params...),
	})
}

// ObserverGeneration returns the current ready connection identity without
// connecting or writing. A frozen connection remains ready until it disconnects.
func (c *Client) ObserverGeneration() (uint64, bool) {
	c.connectionMu.Lock()
	defer c.connectionMu.Unlock()
	return c.generation, c.options.ObserverOnly && c.connection != nil && c.ready == c.generation && !c.closed
}

// FreezeObserverWrites irreversibly freezes this observer client, preserving its
// same reader. The owner must first stop and join its ordinary polling/watch
// work. Any pending RPC, disconnected/stale generation, or missing event tap
// refuses the freeze. No new coverage or connection is created. Do not call
// this method while holding a reducer lock or from ObserveEvent.
func (c *Client) FreezeObserverWrites(ctx context.Context, expectedGeneration uint64) error {
	if !c.options.ObserverOnly || c.events == nil {
		return errors.New("write freeze requires an observer with an event tap")
	}
	if err := c.writeMu.LockContext(ctx); err != nil {
		return err
	}
	defer c.writeMu.Unlock()
	c.connectionMu.Lock()
	defer c.connectionMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if expectedGeneration == 0 || c.generation != expectedGeneration || c.connection == nil || c.ready != expectedGeneration || c.closed {
		return errors.New("observer connection changed before write freeze")
	}
	if c.observerFrozen {
		return nil
	}
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	for _, call := range c.pending {
		if call.generation == expectedGeneration {
			return errors.New("observer RPC is pending at write freeze")
		}
	}
	c.observerFrozen = true
	c.observer.clearQueue()
	return nil
}

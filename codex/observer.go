package codex

import (
	"context"
	"sync"
	"time"
)

const observerRPCConcurrency = 4
const observerRPCTimeout = 10 * time.Second

// Observer admission belongs to the client so reconnect resumes and ordinary
// metadata/history reads share the same bound. Interactive clients do not use it.
type observerState struct {
	context context.Context
	cancel  context.CancelFunc
	slots   chan struct{}
	// Connection fields are protected by Client.connectionMu.
	connectionContext context.Context
	cancelConnection  context.CancelFunc
	// One fixed worker pool consumes a replaceable queue for the live generation.
	once         sync.Once
	mu           sync.Mutex
	queue        []string
	next         int
	generation   uint64
	queueContext context.Context
	wake         chan struct{}
}

func newObserverState() *observerState {
	ctx, cancel := context.WithCancel(context.Background())
	return &observerState{context: ctx, cancel: cancel, slots: make(chan struct{}, observerRPCConcurrency), wake: make(chan struct{}, observerRPCConcurrency)}
}

func (observer *observerState) clearQueue() {
	observer.mu.Lock()
	observer.queue = nil
	observer.next = 0
	observer.mu.Unlock()
}

func (c *Client) restoreObserved(threadIDs []string) {
	observer := c.observer
	c.connectionMu.Lock()
	if c.connection == nil || c.ready != c.generation || c.closed {
		c.connectionMu.Unlock()
		return
	}
	observer.mu.Lock()
	observer.queue, observer.next = threadIDs, 0
	observer.generation, observer.queueContext = c.generation, observer.connectionContext
	observer.mu.Unlock()
	c.connectionMu.Unlock()
	observer.once.Do(func() {
		for range observerRPCConcurrency {
			go c.restoreObservedWorker()
		}
	})
	for range observerRPCConcurrency {
		select {
		case observer.wake <- struct{}{}:
		default:
		}
	}
}

func (c *Client) restoreObservedWorker() {
	observer := c.observer
	for {
		select {
		case <-observer.context.Done():
			return
		case <-observer.wake:
		}
		for {
			observer.mu.Lock()
			if observer.next == len(observer.queue) {
				observer.queue = nil
				observer.next = 0
				observer.mu.Unlock()
				break
			}
			id, ctx, generation := observer.queue[observer.next], observer.queueContext, observer.generation
			observer.next++
			observer.mu.Unlock()
			if ctx.Err() != nil {
				continue
			}
			err := c.resumeWatchedGeneration(ctx, id, generation)
			if err != nil {
				// Publication uses the same generation guard as coverage. A failed old
				// transport must not overwrite a new generation's connection notice.
				c.connectionMu.Lock()
				c.watchedMu.Lock()
				watched := c.watched[id] > 0
				c.watchedMu.Unlock()
				if ctx.Err() == nil && c.connection != nil && c.generation == generation && c.ready == generation && watched {
					c.recordWatchError(id, err)
				}
				c.connectionMu.Unlock()
			}
		}
	}
}

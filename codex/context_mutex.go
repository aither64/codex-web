package codex

import (
	"context"
	"sync"
)

// contextMutex has a usable zero value, including for direct test coordination.
type contextMutex struct {
	once   sync.Once
	permit chan struct{}
}

func (m *contextMutex) initialize() {
	m.once.Do(func() { m.permit = make(chan struct{}, 1); m.permit <- struct{}{} })
}
func (m *contextMutex) Lock() { _ = m.LockContext(context.Background()) }
func (m *contextMutex) TryLock() bool {
	m.initialize()
	select {
	case <-m.permit:
		return true
	default:
		return false
	}
}
func (m *contextMutex) LockContext(ctx context.Context) error {
	m.initialize()
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.permit:
		if err := ctx.Err(); err != nil {
			m.Unlock()
			return err
		}
		return nil
	}
}
func (m *contextMutex) Unlock() {
	m.initialize()
	select {
	case m.permit <- struct{}{}:
	default:
		panic("unlock of unlocked context mutex")
	}
}

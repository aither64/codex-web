package conversation

import "context"

// MutationLocker serializes changes to one conversation while allowing a
// waiting HTTP operation to stop when its context expires.
type MutationLocker interface {
	Lock(context.Context) error
	Unlock()
}

type mutationLock struct {
	permit chan struct{}
}

// NewMutationLock returns a context-aware lock suitable for sharing between
// Handler and an application's other mutation paths for the same conversation.
func NewMutationLock() MutationLocker {
	lock := &mutationLock{permit: make(chan struct{}, 1)}
	lock.permit <- struct{}{}
	return lock
}

func (lock *mutationLock) Lock(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-lock.permit:
		return nil
	}
}

func (lock *mutationLock) Unlock() {
	select {
	case lock.permit <- struct{}{}:
	default:
		panic("conversation: unlock of unlocked mutation lock")
	}
}

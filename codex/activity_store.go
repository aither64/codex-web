package codex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const (
	activityCheckpointLimit = 256 * 1024
	activityPendingLimit    = 128
)

type activityCheckpoint struct {
	Schema   int                           `json:"schema"`
	ThreadID string                        `json:"threadId"`
	Current  *activityObservation          `json:"current,omitempty"`
	Turns    map[string]activityTurnRecord `json:"turns"`
}

type activityThread struct {
	mu        sync.Mutex
	path      string
	hot       activityCheckpoint
	durable   activityCheckpoint
	live      *activityObservation
	revision  uint64
	saved     uint64
	lastError error
	loadError error
	closing   bool
	readers   int
	watchers  int
	loaded    chan struct{}
	wake      chan struct{}
	changed   chan struct{}
	done      chan struct{}
	// Historical reads and cache access never acquire the event-capture mutex.
	historyMu sync.Mutex
	history   map[string]activityTurnRecord
}

// ActivityRecorder owns a private directory for one trusted App Server
// authority. Each thread has its own checkpoint and compact turn summaries.
// The authority mutex protects only the map of currently loaded threads.
type ActivityRecorder struct {
	mu        sync.Mutex
	path      string
	lock      *os.File
	threads   map[string]*activityThread
	closed    bool
	writeFile func(string, []byte) error
}

func NewActivityRecorder(path string) (*ActivityRecorder, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("activity directory must be canonical and absolute")
	}
	if err := privateActivityDirectory(path); err != nil {
		return nil, err
	}
	fd, err := unix.Open(filepath.Join(path, "writer.lock"), unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	lock := os.NewFile(uintptr(fd), "activity writer lock")
	if info, err := lock.Stat(); err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		lock.Close()
		return nil, errors.New("activity writer lock must be a private regular file")
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("activity directory already owned: %w", err)
	}
	return &ActivityRecorder{path: path, lock: lock, threads: map[string]*activityThread{}, writeFile: writeActivityFile}, nil
}

func activityFileID(id string) string {
	digest := sha256.Sum256([]byte(id))
	return hex.EncodeToString(digest[:])
}

func cloneObservation(value *activityObservation) *activityObservation {
	if value == nil {
		return nil
	}
	result := *value
	result.Flags = append([]string(nil), value.Flags...)
	result.Requests = maps.Clone(value.Requests)
	return &result
}

func cloneActivityCheckpoint(value activityCheckpoint) activityCheckpoint {
	value.Current = cloneObservation(value.Current)
	value.Turns = maps.Clone(value.Turns)
	return value
}

func (r *ActivityRecorder) lookup(threadID string) *activityThread {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.threads[threadID]
}

// prepare runs before registering a watch, so loading private state cannot
// block the websocket reader. It is also used by read-only archive requests.
func (r *ActivityRecorder) prepare(ctx context.Context, threadID string) error {
	if r == nil {
		return nil
	}
	for {
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return errors.New("activity recorder is closed")
		}
		thread := r.threads[threadID]
		if thread == nil {
			thread = &activityThread{path: filepath.Join(r.path, activityFileID(threadID)),
				hot:    activityCheckpoint{Schema: 1, ThreadID: threadID, Turns: map[string]activityTurnRecord{}},
				loaded: make(chan struct{}), wake: make(chan struct{}, 1), changed: make(chan struct{}), done: make(chan struct{}), history: map[string]activityTurnRecord{}}
			r.threads[threadID] = thread
			go r.activityWriter(threadID, thread)
		}
		r.mu.Unlock()
		select {
		case <-thread.loaded:
		case <-ctx.Done():
			return ctx.Err()
		}
		thread.mu.Lock()
		closing := thread.closing
		thread.mu.Unlock()
		if !closing {
			return nil
		}
		select {
		case <-thread.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Retaining a watch/read prevents retirement between prepare and its caller.
func (r *ActivityRecorder) retain(ctx context.Context, id string, watch bool) error {
	if r == nil {
		return nil
	}
	for {
		if err := r.prepare(ctx, id); err != nil {
			r.retire(id)
			return err
		}
		thread := r.lookup(id)
		if thread == nil {
			continue
		}
		thread.mu.Lock()
		if thread.closing {
			thread.mu.Unlock()
			continue
		}
		if watch {
			thread.watchers++
		} else {
			thread.readers++
		}
		thread.mu.Unlock()
		return nil
	}
}

func (r *ActivityRecorder) release(id string, watch bool) {
	thread := r.lookup(id)
	if thread == nil {
		return
	}
	thread.mu.Lock()
	if watch {
		thread.watchers--
	} else {
		thread.readers--
	}
	if thread.readers == 0 && thread.watchers == 0 && thread.live == nil && !thread.closing {
		thread.closeLocked()
	}
	thread.mu.Unlock()
}

func (thread *activityThread) signalLocked() {
	thread.revision++
	select {
	case thread.wake <- struct{}{}:
	default:
	}
}

func (r *ActivityRecorder) activityWriter(threadID string, thread *activityThread) {
	defer func() {
		r.mu.Lock()
		if r.threads[threadID] == thread {
			delete(r.threads, threadID)
		}
		r.mu.Unlock()
		close(thread.done)
	}()
	loaded, err := loadActivityCheckpoint(filepath.Join(thread.path, "current.json"), threadID)
	if err == nil {
		// A crash can occur after a summary is durable but before the current
		// checkpoint drops its older copy. Sequence numbers make that replay safe.
		for id, record := range loaded.Turns {
			if saved, ok := thread.summary(context.Background(), id); ok && saved.Sequence >= record.Sequence {
				delete(loaded.Turns, id)
			}
		}
	}
	thread.mu.Lock()
	if err == nil {
		thread.hot = loaded
	}
	thread.durable = cloneActivityCheckpoint(thread.hot)
	thread.loadError, thread.lastError = err, err
	close(thread.loaded)
	thread.mu.Unlock()
	for range thread.wake {
		time.Sleep(10 * time.Millisecond)
		// One wake coalesces a burst of lifecycle events. No disk operation
		// holds thread.mu, and each thread has an independent writer.
		thread.mu.Lock()
		checkpoint := cloneActivityCheckpoint(thread.hot)
		revision, closing, loadErr := thread.revision, thread.closing, thread.loadError
		thread.mu.Unlock()
		writeErr := loadErr
		if writeErr == nil {
			writeErr = r.writeCheckpoint(thread, checkpoint)
		}
		thread.mu.Lock()
		if writeErr == nil {
			thread.saved = revision
			thread.durable = cloneActivityCheckpoint(checkpoint)
			for id, record := range checkpoint.Turns {
				if record.Final {
					delete(thread.durable.Turns, id)
					if current, ok := thread.hot.Turns[id]; ok && current == record {
						delete(thread.hot.Turns, id)
					}
				}
			}
		}
		thread.lastError = writeErr
		close(thread.changed)
		thread.changed = make(chan struct{})
		thread.mu.Unlock()
		if closing {
			return
		}
	}
}

func (r *ActivityRecorder) writeCheckpoint(thread *activityThread, checkpoint activityCheckpoint) error {
	checkpoint.Turns = maps.Clone(checkpoint.Turns)
	if err := privateActivityDirectory(thread.path); err != nil {
		return err
	}
	if err := privateActivityDirectory(filepath.Join(thread.path, "turns")); err != nil {
		return err
	}
	for id, record := range checkpoint.Turns {
		if !record.Final {
			continue
		}
		if saved, ok := thread.summary(context.Background(), id); ok && saved.Sequence > record.Sequence {
			delete(checkpoint.Turns, id)
			continue
		}
		data, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if err := r.writeFile(filepath.Join(thread.path, "turns", activityFileID(id)+".json"), data); err != nil {
			return err
		}
		thread.historyMu.Lock()
		thread.history[id] = record
		thread.historyMu.Unlock()
		delete(checkpoint.Turns, id)
	}
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	if len(data) > activityCheckpointLimit {
		return errors.New("activity checkpoint exceeds its bounded current-state limit")
	}
	return r.writeFile(filepath.Join(thread.path, "current.json"), data)
}

func privateActivityDirectory(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return errors.New("activity directory must be a private directory")
	}
	return nil
}

func writeActivityFile(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".activity-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err = tmp.Write(data); err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func readPrivateActivityFile(path string) ([]byte, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() > activityCheckpointLimit {
		return nil, errors.New("activity state must be a bounded private regular file")
	}
	return io.ReadAll(io.LimitReader(file, activityCheckpointLimit+1))
}

func loadActivityCheckpoint(path, threadID string) (activityCheckpoint, error) {
	result := activityCheckpoint{Schema: 1, ThreadID: threadID, Turns: map[string]activityTurnRecord{}}
	data, err := readPrivateActivityFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return result, nil
	}
	if err != nil {
		return result, err
	}
	if json.Unmarshal(data, &result) != nil || result.Schema != 1 || result.ThreadID != threadID || result.Turns == nil || len(result.Turns) > activityPendingLimit {
		return result, errors.New("invalid activity checkpoint")
	}
	for id, record := range result.Turns {
		if id != record.ID || !validActivityRecord(record) {
			return result, errors.New("invalid activity turn summary")
		}
	}
	if result.Current != nil && (len(result.Current.Requests) > activityPendingLimit || result.Current.ThroughMS < result.Current.SinceMS) {
		return result, errors.New("invalid activity observation")
	}
	return result, nil
}

func validActivityRecord(record activityTurnRecord) bool {
	return record.ID != "" && record.WorkingMS >= 0 && record.WaitingMS >= 0 && record.FirstMS >= 0 && record.ThroughMS >= record.FirstMS &&
		record.TailWorkMS >= 0 && record.TailWaitMS >= 0 && record.TailWorkMS <= record.WorkingMS && record.TailWaitMS <= record.WaitingMS
}

func (r *ActivityRecorder) flush(ctx context.Context, threadID string) error {
	thread := r.lookup(threadID)
	if thread == nil {
		return nil
	}
	thread.mu.Lock()
	target := thread.revision
	thread.mu.Unlock()
	for {
		thread.mu.Lock()
		saved, err, changed := thread.saved, thread.lastError, thread.changed
		thread.mu.Unlock()
		if err != nil {
			return err
		}
		if saved >= target {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *ActivityRecorder) retire(threadID string) {
	thread := r.lookup(threadID)
	if thread == nil {
		return
	}
	thread.mu.Lock()
	if thread.live == nil && thread.readers == 0 && thread.watchers == 0 && !thread.closing {
		thread.closeLocked()
	}
	thread.mu.Unlock()
}

func (thread *activityThread) closeLocked() {
	thread.closing = true
	thread.hot.Current = nil
	for id, record := range thread.hot.Turns {
		if !record.Final {
			record.Final = true
			record.Sequence++
		}
		thread.hot.Turns[id] = record
	}
	thread.signalLocked()
}

func (r *ActivityRecorder) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	threads := make([]*activityThread, 0, len(r.threads))
	for _, thread := range r.threads {
		threads = append(threads, thread)
	}
	r.mu.Unlock()
	for _, thread := range threads {
		<-thread.loaded
		thread.mu.Lock()
		thread.live = nil
		thread.closeLocked()
		thread.mu.Unlock()
	}
	var result error
	for _, thread := range threads {
		<-thread.done
		thread.mu.Lock()
		result = errors.Join(result, thread.lastError)
		thread.mu.Unlock()
	}
	return errors.Join(result, r.lock.Close())
}

// summary reads only the requested thread's immutable per-turn record. Cache
// lifetime follows that thread's watch/read lifetime, rather than the authority.
func (thread *activityThread) summary(ctx context.Context, id string) (activityTurnRecord, bool) {
	thread.historyMu.Lock()
	record, ok := thread.history[id]
	thread.historyMu.Unlock()
	if ok {
		return record, record.ID != ""
	}
	if ctx.Err() != nil {
		return activityTurnRecord{}, false
	}
	data, err := readPrivateActivityFile(filepath.Join(thread.path, "turns", activityFileID(id)+".json"))
	if errors.Is(err, os.ErrNotExist) {
		thread.historyMu.Lock()
		if current, present := thread.history[id]; present {
			thread.historyMu.Unlock()
			return current, current.ID != ""
		}
		thread.history[id] = activityTurnRecord{}
		thread.historyMu.Unlock()
		return activityTurnRecord{}, false
	}
	if err != nil {
		return activityTurnRecord{}, false
	}
	if json.Unmarshal(data, &record) != nil || record.ID != id || !record.Final || !validActivityRecord(record) {
		return activityTurnRecord{}, false
	}
	thread.historyMu.Lock()
	if current, present := thread.history[id]; present && current.Sequence > record.Sequence {
		record = current
	} else {
		thread.history[id] = record
	}
	thread.historyMu.Unlock()
	return record, true
}

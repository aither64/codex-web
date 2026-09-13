package conversation

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"
)

type streamRecorder struct {
	*httptest.ResponseRecorder
	flushed chan string
	fail    bool
}

func (r *streamRecorder) Write(data []byte) (int, error) {
	if r.fail {
		return 0, errors.New("connection lost")
	}
	return r.ResponseRecorder.Write(data)
}
func (r *streamRecorder) WriteString(data string) (int, error) { return r.Write([]byte(data)) }
func (r *streamRecorder) Flush() {
	r.flushed <- r.Body.String()
	r.Body.Reset()
}

func TestEventStreamAdvertisesHeartbeatAndPreservesUpdates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &streamRecorder{ResponseRecorder: httptest.NewRecorder(), flushed: make(chan string, 4)}
	events := make(chan struct{}, 1)
	ticks := make(chan time.Time, 1)
	done := make(chan struct{})
	go func() {
		(&Handler{}).streamEvents(r, httptest.NewRequest("GET", "/events", nil).WithContext(ctx), r, events, ticks)
		close(done)
	}()
	read := func(want string) {
		t.Helper()
		select {
		case got := <-r.flushed:
			if got != want {
				t.Fatalf("event=%q, want %q", got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("event was not flushed")
		}
	}
	read("event: ready\ndata: {\"heartbeatIntervalMs\":20000}\n\n")
	ticks <- time.Now()
	read("event: heartbeat\ndata: {}\n\n")
	events <- struct{}{}
	read("data: update\n\n")
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stream did not stop")
	}
	if r.Header().Get("X-Accel-Buffering") != "no" || r.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatal("stream headers changed")
	}
}

func TestEventStreamStopsOnWriteFailure(t *testing.T) {
	r := &streamRecorder{ResponseRecorder: httptest.NewRecorder(), flushed: make(chan string, 1), fail: true}
	done := make(chan struct{})
	go func() {
		(&Handler{}).streamEvents(r, httptest.NewRequest("GET", "/events", nil), r, nil, nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("failed stream remained open")
	}
}

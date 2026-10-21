package conversation

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync/atomic"
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

func TestPendingRevisionSurvivesCoalescingWithoutChangingGenericUpdates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &streamRecorder{ResponseRecorder: httptest.NewRecorder(), flushed: make(chan string, 8)}
	events := make(chan struct{}, 1)
	var revision atomic.Uint64
	done := make(chan struct{})
	go func() {
		(&Handler{}).streamEvents(r, httptest.NewRequest("GET", "/events", nil).WithContext(ctx), r, events, nil, revision.Load)
		close(done)
	}()
	next := func() string {
		t.Helper()
		select {
		case value := <-r.flushed:
			return value
		case <-time.After(time.Second):
			t.Fatal("stream did not flush")
			return ""
		}
	}
	if got := next(); got != "event: ready\ndata: {\"heartbeatIntervalMs\":20000,\"pendingInvalidation\":true}\n\n" {
		t.Fatal(got)
	}
	revision.Add(3)
	events <- struct{}{}
	if got := next(); got != "event: pending\ndata: update\n\ndata: update\n\n" {
		t.Fatal(got)
	}
	events <- struct{}{}
	if got := next(); got != "data: update\n\n" {
		t.Fatal(got)
	}
	revision.Add(1)
	events <- struct{}{}
	if got := next(); got != "event: pending\ndata: update\n\ndata: update\n\n" {
		t.Fatal(got)
	}
	cancel()
	<-done
}

type revisionClient struct {
	*fakeClient
	reads atomic.Uint64
}

func (c *revisionClient) PendingRevision(string) uint64 { return c.reads.Add(1) }

func TestPendingRevisionAdvertisementRequiresPendingAuthority(t *testing.T) {
	for _, allowed := range []bool{false, true} {
		t.Run(fmt.Sprint(allowed), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r := &streamRecorder{ResponseRecorder: httptest.NewRecorder(), flushed: make(chan string, 2)}
			client := &revisionClient{fakeClient: &fakeClient{}}
			done := make(chan struct{})
			go func() {
				(&Handler{}).events(r, httptest.NewRequest("GET", "/events", nil).WithContext(ctx), Target{
					Client: client, ThreadID: "trusted-thread", Capabilities: Capabilities{EventStream: true, Pending: allowed},
				})
				close(done)
			}()
			select {
			case ready := <-r.flushed:
				if strings.Contains(ready, `"pendingInvalidation":true`) != allowed {
					t.Fatal(ready)
				}
			case <-time.After(time.Second):
				t.Fatal("no ready event")
			}
			cancel()
			<-done
			if (client.reads.Load() != 0) != allowed {
				t.Fatal("getter called without pending authority")
			}
		})
	}
}

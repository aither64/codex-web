package codex

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func assertHistoryCacheCount(t *testing.T, c *Client, entries, idle int) {
	t.Helper()
	c.turnHistoryMu.Lock()
	defer c.turnHistoryMu.Unlock()
	if len(c.turnHistory) != entries || len(c.idleTurnHistory) != idle {
		t.Fatalf("retained histories=%d idle=%d, want %d/%d", len(c.turnHistory), len(c.idleTurnHistory), entries, idle)
	}
}

func TestReadActivityUnwatchedHistoriesRetireOnSuccessAndErrors(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for {
			request, err := readObject(connection)
			if err != nil {
				return nil
			}
			id := request["params"].(map[string]any)["threadId"].(string)
			response := map[string]any{"id": request["id"]}
			switch request["method"] {
			case "thread/read":
				if id == "metadata-error" {
					response["error"] = map[string]any{"code": -32000, "message": "metadata unavailable"}
				} else {
					actualID := id
					if id == "wrong-identity" {
						actualID = "wrong"
					}
					response["result"] = map[string]any{"thread": map[string]any{"id": actualID, "path": "/trusted/" + id}}
				}
			case "thread/turns/list":
				if id == "page-error" {
					response["error"] = map[string]any{"code": -32000, "message": "page unavailable"}
				} else if id == "missing-page" {
					response["result"] = map[string]any{}
				} else {
					response["result"] = map[string]any{"data": []any{map[string]any{"id": "turn", "status": "completed", "startedAt": 1, "completedAt": 2}}}
				}
			default:
				return fmt.Errorf("unexpected request %v", request)
			}
			if err := writeObject(connection, response); err != nil {
				return err
			}
		}
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for round := 0; round < 3; round++ {
		for n := 0; n < 20; n++ {
			snapshot, err := client.ReadActivity(ctx, fmt.Sprintf("archive-%d", n))
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.UnclassifiedMS != 1000 {
				t.Fatalf("historical totals = %#v", snapshot)
			}
			assertHistoryCacheCount(t, client, 0, 0)
		}
		for _, id := range []string{"metadata-error", "wrong-identity", "page-error", "missing-page"} {
			if _, err := client.ReadActivity(ctx, id); err == nil {
				t.Fatalf("%s succeeded", id)
			}
			assertHistoryCacheCount(t, client, 0, 0)
		}
	}
}

func TestActivityIncompleteIdleHistoryIsBoundedAndPinsQueuedReaders(t *testing.T) {
	blocked := make(chan struct{})
	var pinnedRequests atomic.Int32
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for {
			request, err := readObject(connection)
			if err != nil {
				return nil
			}
			params := request["params"].(map[string]any)
			id := params["threadId"].(string)
			if id == "pinned" {
				pinnedRequests.Add(1)
				if params["cursor"] != nil {
					close(blocked)
					continue
				}
			}
			response := map[string]any{"id": request["id"]}
			if params["cursor"] == nil {
				response["result"] = map[string]any{"data": []any{map[string]any{"id": "recent", "status": "completed"}}, "nextCursor": "older"}
			} else {
				response["error"] = map[string]any{"code": -32000, "message": "retry older history"}
			}
			if err := writeObject(connection, response); err != nil {
				return err
			}
		}
	})
	client := New(socket)
	defer client.Close()
	client.retainTurnHistory("watched", true)
	defer client.releaseTurnHistory("watched", true)
	firstCtx, firstCancel := context.WithCancel(context.Background())
	defer firstCancel()
	firstDone := make(chan error, 1)
	go func() { _, err := client.readAllTurns(firstCtx, "pinned", "full", "/trusted/pinned"); firstDone <- err }()
	select {
	case <-blocked:
	case <-time.After(3 * time.Second):
		t.Fatal("first reader did not backfill")
	}
	queuedCtx, queuedCancel := context.WithCancel(context.Background())
	defer queuedCancel()
	queuedDone := make(chan error, 1)
	go func() {
		_, err := client.readAllTurns(queuedCtx, "pinned", "full", "/trusted/pinned")
		queuedDone <- err
	}()
	waitFor(t, func() bool {
		client.turnHistoryMu.Lock()
		defer client.turnHistoryMu.Unlock()
		return client.turnHistory["pinned"].readers == 2
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for n := 0; n < 20; n++ {
		id := fmt.Sprintf("partial-%d", n)
		if _, err := client.readAllTurns(ctx, id, "full", "/trusted/"+id); err == nil {
			t.Fatal("interrupted backfill succeeded")
		}
		assertHistoryCacheCount(t, client, min(n+1, 8)+2, min(n+1, 8))
	}
	client.turnHistoryMu.Lock()
	evicted := client.turnHistory["partial-0"] == nil
	recent := client.turnHistory["partial-19"] != nil
	pinned := client.turnHistory["pinned"]
	client.turnHistoryMu.Unlock()
	if !evicted || !recent || pinned == nil || pinnedRequests.Load() != 2 {
		t.Fatal("idle eviction replaced a pinned reader or kept old idle history")
	}
	queuedCancel()
	if err := <-queuedDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("queued cancellation = %v", err)
	}
	assertHistoryCacheCount(t, client, 10, 8)
	firstCancel()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("active cancellation = %v", err)
	}
	assertHistoryCacheCount(t, client, 9, 8)
	client.turnHistoryMu.Lock()
	retained := client.turnHistory["pinned"] == pinned
	client.turnHistoryMu.Unlock()
	if !retained {
		t.Fatal("cancelled backfill lost its retry progress")
	}
}

func TestActivityLastUnsubscribeKeepsActiveAndQueuedHistoryReaders(t *testing.T) {
	blocked, release, secondMetadata := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var pages atomic.Int32
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		metadataReads := 0
		for {
			request, err := readObject(connection)
			if err != nil {
				return nil
			}
			response := map[string]any{"id": request["id"], "result": map[string]any{}}
			switch request["method"] {
			case "thread/resume", "thread/unsubscribe":
			case "thread/read":
				metadataReads++
				response["result"] = map[string]any{"thread": map[string]any{"id": "thread", "path": "/trusted/rollout"}}
			case "thread/turns/list":
				count := pages.Add(1)
				params := request["params"].(map[string]any)
				if params["cursor"] != nil {
					if count != 2 {
						return errors.New("duplicated a backfill while another reader owned the cache")
					}
					response["result"] = map[string]any{"data": []any{map[string]any{"id": "older", "status": "completed", "startedAt": 1, "completedAt": 2}}}
					go func() { <-release; _ = writeObject(connection, response) }()
					close(blocked)
					continue
				}
				response["result"] = map[string]any{"data": []any{map[string]any{"id": "recent", "status": "completed", "startedAt": 3, "completedAt": 4}}, "nextCursor": "older"}
			default:
				return fmt.Errorf("unexpected request %v", request)
			}
			if err := writeObject(connection, response); err != nil {
				return err
			}
			if request["method"] == "thread/read" && metadataReads == 2 {
				close(secondMetadata)
			}
		}
	})
	client := NewWithOptions(socket, ClientOptions{ObserverOnly: true})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, unsubscribe, err := client.Subscribe(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	defer unsubscribe()
	client.turnHistoryMu.Lock()
	original := client.turnHistory["thread"]
	watched := original != nil && original.watchers == 1
	client.turnHistoryMu.Unlock()
	if !watched {
		t.Fatal("watch before first history read did not own a cache")
	}
	result := make(chan error, 2)
	read := func() {
		snapshot, err := client.ReadActivity(ctx, "thread")
		if err == nil && snapshot.UnclassifiedMS != 2000 {
			err = fmt.Errorf("missing historical totals: %#v", snapshot)
		}
		result <- err
	}
	go read()
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	unsubscribe()
	go read()
	select {
	case <-secondMetadata:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	client.turnHistoryMu.Lock()
	same := client.turnHistory["thread"] == original && original.watchers == 0 && original.readers == 2
	client.turnHistoryMu.Unlock()
	if !same || pages.Load() != 2 {
		close(release)
		t.Fatal("last unsubscribe replaced a cache owned by active readers")
	}
	close(release)
	for range 2 {
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	}
	if pages.Load() != 3 {
		t.Fatalf("page requests=%d, wanted one backfill and one cached refresh", pages.Load())
	}
	assertHistoryCacheCount(t, client, 0, 0)
}

func TestReadActivityRecorderSetupErrorReleasesHistory(t *testing.T) {
	recorder, err := NewActivityRecorder(filepath.Join(t.TempDir(), "activity"))
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.Close(); err != nil {
		t.Fatal(err)
	}
	client := NewWithOptions("/unused/socket", ClientOptions{ActivityRecorder: recorder})
	defer client.Close()
	if _, err := client.ReadActivity(context.Background(), "thread"); err == nil {
		t.Fatal("closed recorder was accepted")
	}
	assertHistoryCacheCount(t, client, 0, 0)
}

package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func pageFixture(t *testing.T, count int, failedTurns int, emptyTurns ...int) (func(string) (TranscriptPage, error), string) {
	t.Helper()
	empty := 0
	if len(emptyTurns) > 0 {
		empty = emptyTurns[0]
	}
	rollout := filepath.Join(t.TempDir(), "rollout.jsonl")
	if err := os.WriteFile(rollout, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	request := func(_ context.Context, method string, raw any, output any) error {
		params := raw.(map[string]any)
		var err error
		var result any
		switch method {
		case "thread/read":
			if params["includeTurns"] != false {
				return errors.New("page requested full thread")
			}
			result = map[string]any{"thread": map[string]any{"id": "thread-1", "cwd": "/workspace", "path": rollout, "status": map[string]any{"type": "idle"}, "source": "vscode", "createdAt": 1}}
		case "thread/turns/list":
			if params["itemsView"] != "notLoaded" {
				return errors.New("page requested full turn")
			}
			start := 0
			if cursor, ok := params["cursor"].(string); ok {
				start, err = strconv.Atoi(cursor)
				if err != nil {
					return err
				}
			}
			limit := params["limit"].(int)
			turns := make([]any, 0)
			total := failedTurns + empty
			if count > 0 {
				total++
			}
			for i := start; i < total && len(turns) < limit; i++ {
				turn := map[string]any{"id": fmt.Sprintf("turn-%03d", i), "status": "completed", "error": nil, "startedAt": float64(1000 + i)}
				if i < failedTurns {
					turn["status"] = "failed"
					turn["error"] = map[string]any{"message": fmt.Sprintf("failure-%d", i)}
				}
				turns = append(turns, turn)
			}
			var next any
			if start+len(turns) < total {
				next = strconv.Itoa(start + len(turns))
			}
			result = map[string]any{"data": turns, "nextCursor": next}
		case "thread/items/list":
			limit := params["limit"].(int)
			start := 0
			if cursor, ok := params["cursor"].(string); ok {
				start, err = strconv.Atoi(cursor)
				if err != nil {
					return err
				}
			}
			items := make([]any, 0)
			if params["turnId"] == fmt.Sprintf("turn-%03d", failedTurns+empty) {
				for i := start; i < count && len(items) < limit; i++ {
					items = append(items, map[string]any{"turnId": params["turnId"], "item": map[string]any{"id": fmt.Sprintf("item-%03d", i), "type": "agentMessage", "text": fmt.Sprintf("message-%d", i)}})
				}
			}
			var next any
			if len(items) > 0 && start+len(items) < count {
				next = strconv.Itoa(start + len(items))
			}
			result = map[string]any{"data": items, "nextCursor": next}
		default:
			return fmt.Errorf("unexpected page RPC %v", method)
		}
		data, err := json.Marshal(result)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, output)
	}
	client := newTestClient("/tmp/codex-page-test-unused.sock")
	t.Cleanup(client.Close)
	return func(cursor string) (TranscriptPage, error) {
		return client.readThreadPage(context.Background(), "thread-1", cursor, request)
	}, rollout
}

func TestReadThreadPageWalksAllItemsAndRetryIsStable(t *testing.T) {
	for _, count := range []int{0, 1, 99, 100, 101, 250} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			read, _ := pageFixture(t, count, 0)
			cursor := ""
			seen := map[string]bool{}
			for pageNo := 0; pageNo < 5; pageNo++ {
				page, err := read(cursor)
				if err != nil {
					t.Fatal(err)
				}
				if page.ThreadID != "thread-1" || page.LatestTurnID != map[bool]string{true: "turn-000", false: ""}[count > 0] || !page.MetadataPending {
					t.Fatalf("page metadata = %#v", page)
				}
				if len(page.Entries) > 100 {
					t.Fatalf("page has %d entries", len(page.Entries))
				}
				if cursor != "" {
					retry, err := read(cursor)
					if err != nil || len(retry.Entries) != len(page.Entries) {
						t.Fatalf("retry = %#v, %v", retry, err)
					}
					for i := range retry.Entries {
						if retry.Entries[i].ItemID != page.Entries[i].ItemID {
							t.Fatal("retry skipped an item")
						}
					}
				}
				for _, entry := range page.Entries {
					if seen[entry.ItemID] {
						t.Fatalf("duplicate item %q", entry.ItemID)
					}
					seen[entry.ItemID] = true
				}
				if !page.HasOlder {
					if len(seen) != count {
						t.Fatalf("got %d of %d items", len(seen), count)
					}
					return
				}
				if page.OlderCursor == nil {
					t.Fatal("missing continuation")
				}
				cursor = *page.OlderCursor
			}
			t.Fatal("pagination did not finish")
		})
	}
}

func TestReadThreadPagePreservesItemlessFailures(t *testing.T) {
	read, _ := pageFixture(t, 1, 205)
	cursor := ""
	failures := map[string]bool{}
	for pageNo := 0; pageNo < 5; pageNo++ {
		page, err := read(cursor)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Entries) > 100 {
			t.Fatal("page exceeded 100 slots")
		}
		for _, entry := range page.Entries {
			if entry.Kind == "error" {
				if failures[entry.TurnID] {
					t.Fatal("repeated failure")
				}
				failures[entry.TurnID] = true
			}
		}
		if !page.HasOlder {
			if len(failures) != 205 {
				t.Fatalf("got %d failures", len(failures))
			}
			return
		}
		cursor = *page.OlderCursor
	}
	t.Fatal("failed-turn history did not finish")
}

func TestReadThreadPageContinuesAcrossEmptyTurns(t *testing.T) {
	read, _ := pageFixture(t, 1, 0, 205)
	cursor := ""
	for pageNo := 0; pageNo < 3; pageNo++ {
		page, err := read(cursor)
		if err != nil {
			t.Fatal(err)
		}
		if pageNo < 2 && (len(page.Entries) != 0 || !page.HasOlder || page.OlderCursor == nil) {
			t.Fatalf("empty page %d = %#v", pageNo, page)
		}
		if pageNo == 2 {
			if len(page.Entries) != 1 || page.HasOlder || page.Entries[0].ItemID != "item-000" {
				t.Fatalf("final page = %#v", page)
			}
			return
		}
		cursor = *page.OlderCursor
	}
}

func TestReadThreadPageRejectsNonprogressingItemCursor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newTestClient("/tmp/codex-page-test-unused.sock")
	defer client.Close()
	itemCalls := 0
	request := func(_ context.Context, method string, _ any, output any) error {
		var value any
		switch method {
		case "thread/read":
			value = map[string]any{"thread": map[string]any{"id": "thread-1", "cwd": "/workspace", "path": path, "status": map[string]any{"type": "idle"}}}
		case "thread/turns/list":
			value = map[string]any{"data": []any{map[string]any{"id": "turn-1", "status": "completed"}}}
		case "thread/items/list":
			itemCalls++
			value = map[string]any{"data": []any{map[string]any{"turnId": "turn-1", "item": map[string]any{"id": fmt.Sprintf("item-%d", itemCalls), "type": "agentMessage", "text": "x"}}}, "nextCursor": "repeat"}
		default:
			return fmt.Errorf("unexpected method %s", method)
		}
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, output)
	}
	_, err := client.readThreadPage(context.Background(), "thread-1", "", request)
	if err == nil || !strings.Contains(err.Error(), "invalid cursor") || itemCalls != 2 {
		t.Fatalf("error=%v itemCalls=%d", err, itemCalls)
	}
}

func TestReadThreadPageResetsWhenLatestTurnFinishes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newTestClient("/tmp/codex-page-test-unused.sock")
	defer client.Close()
	status := "inProgress"
	request := func(_ context.Context, method string, raw any, output any) error {
		params := raw.(map[string]any)
		var value any
		switch method {
		case "thread/read":
			value = map[string]any{"thread": map[string]any{"id": "thread-1", "cwd": "/workspace", "path": path, "status": map[string]any{"type": "active"}}}
		case "thread/turns/list":
			turn := map[string]any{"id": "turn-1", "status": status}
			if status == "failed" {
				turn["error"] = map[string]any{"message": "failed"}
			}
			value = map[string]any{"data": []any{turn}}
		case "thread/items/list":
			start := 0
			if params["cursor"] == "100" {
				start = 100
			}
			items := make([]any, 0)
			for i := start; i < 101 && len(items) < params["limit"].(int); i++ {
				items = append(items, map[string]any{"turnId": "turn-1", "item": map[string]any{"id": fmt.Sprintf("item-%03d", i), "type": "agentMessage", "text": "x"}})
			}
			var next any
			if start == 0 {
				next = "100"
			}
			value = map[string]any{"data": items, "nextCursor": next}
		default:
			return fmt.Errorf("unexpected method %s", method)
		}
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, output)
	}
	first, err := client.readThreadPage(context.Background(), "thread-1", "", request)
	if err != nil || first.OlderCursor == nil {
		t.Fatalf("first = %#v, %v", first, err)
	}
	status = "failed"
	_, err = client.readThreadPage(context.Background(), "thread-1", *first.OlderCursor, request)
	var cursor *TranscriptCursorError
	if !errors.As(err, &cursor) || cursor.Code != "transcript_reset_required" {
		t.Fatalf("finished turn cursor = %v", err)
	}
}

func TestReadThreadPageRejectsInvalidCursor(t *testing.T) {
	read, _ := pageFixture(t, 1, 0)
	_, err := read("invalid")
	var cursor *TranscriptCursorError
	if !errors.As(err, &cursor) || cursor.Code != "transcript_cursor_invalid" {
		t.Fatalf("cursor error = %v", err)
	}
}

func TestReadThreadPageFreshUnmaterializedThread(t *testing.T) {
	client := newTestClient("/tmp/codex-page-test-unused.sock")
	defer client.Close()
	path := filepath.Join(t.TempDir(), "missing-rollout.jsonl")
	request := func(_ context.Context, method string, _ any, output any) error {
		if method == "thread/turns/list" {
			return &rpcCallError{code: -32600, message: "thread thread-1 is not materialized yet; thread/turns/list is unavailable before first user message"}
		}
		if method != "thread/read" {
			return fmt.Errorf("unexpected method %s", method)
		}
		value := map[string]any{"thread": map[string]any{"id": "thread-1", "cwd": "/workspace", "path": path,
			"status": map[string]any{"type": "idle"}, "source": "vscode", "historyMode": "paginated", "preview": "", "forkedFromId": "", "ephemeral": false, "turns": []any{}}}
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, output)
	}
	page, err := client.readThreadPage(context.Background(), "thread-1", "", request)
	if err != nil || len(page.Entries) != 0 || page.HasOlder || !page.MetadataPending {
		t.Fatalf("fresh page = %#v, %v", page, err)
	}
}

func TestPageCursorExpiresAndEvicts(t *testing.T) {
	client := newTestClient("/tmp/codex-page-test-unused.sock")
	defer client.Close()
	first, err := client.savePageCursor(pageContinuation{threadID: "thread-1"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < pageCursorLimit; i++ {
		if _, err := client.savePageCursor(pageContinuation{threadID: "thread-1"}); err != nil {
			t.Fatal(err)
		}
	}
	_, err = client.loadPageCursor(*first, "thread-1")
	var cursor *TranscriptCursorError
	if !errors.As(err, &cursor) || cursor.Code != "transcript_cursor_expired" {
		t.Fatalf("evicted cursor = %v", err)
	}
	second, err := client.savePageCursor(pageContinuation{threadID: "thread-1"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.loadPageCursor(*second, "other-thread")
	if !errors.As(err, &cursor) || cursor.Code != "transcript_reset_required" {
		t.Fatalf("foreign cursor = %v", err)
	}
	client.pageMu.Lock()
	state := client.pageCursors[*second]
	state.lastUsed = time.Now().Add(-pageCursorIdle - time.Second)
	client.pageCursors[*second] = state
	client.pageMu.Unlock()
	_, err = client.loadPageCursor(*second, "thread-1")
	if !errors.As(err, &cursor) || cursor.Code != "transcript_cursor_expired" {
		t.Fatalf("idle cursor = %v", err)
	}
}

func TestReadThreadPageCursorTracksRolloutLineage(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(string) error
		reset  bool
	}{
		{"append", func(path string) error {
			file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				return err
			}
			defer file.Close()
			_, err = file.WriteString("{}\n")
			return err
		}, false},
		{"same-size edit", func(path string) error {
			if err := os.WriteFile(path, []byte("[]\n"), 0o600); err != nil {
				return err
			}
			return os.Chtimes(path, time.Now().Add(time.Second), time.Now().Add(time.Second))
		}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			read, path := pageFixture(t, 101, 0)
			first, err := read("")
			if err != nil || first.OlderCursor == nil {
				t.Fatalf("first page = %#v, %v", first, err)
			}
			if err := test.mutate(path); err != nil {
				t.Fatal(err)
			}
			_, err = read(*first.OlderCursor)
			var cursor *TranscriptCursorError
			if test.reset && (!errors.As(err, &cursor) || cursor.Code != "transcript_reset_required") {
				t.Fatalf("edit error = %v", err)
			}
			if !test.reset && err != nil {
				t.Fatalf("append error = %v", err)
			}
		})
	}
}

func TestPageMetadataPendingIsCoalesced(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	client := newTestClient("/tmp/codex-page-test-unused.sock")
	defer client.Close()
	cache := &pageMetadataCache{path: path, generation: 0, pending: true, times: map[transcriptItemKey]itemTimestamps{}}
	client.pageMetadata["thread-1"] = cache
	for i := 0; i < 2; i++ {
		_, _, pending := client.pageMetadataSnapshot("thread-1", path, "", 0, file, nil)
		if !pending || client.pageMetadata["thread-1"] != cache {
			t.Fatal("pending hydration was replaced")
		}
	}
	cache.pending = false
	cache.retryAt = time.Now().Add(time.Minute)
	client.pageMetadataSnapshot("thread-1", path, "", 0, file, nil)
	if client.pageMetadata["thread-1"] != cache || cache.pending {
		t.Fatal("retry backoff was ignored")
	}
}

func TestPageMetadataScanResumesCompleteRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	first := "{\"type\":\"turn_context\",\"payload\":{\"collaboration_mode\":{\"mode\":\"plan\"}}}\n" +
		"{\"type\":\"event_msg\",\"timestamp\":\"2026-09-29T15:00:00Z\",\"payload\":{\"type\":\"item_completed\",\"turn_id\":\"turn-1\",\"item\":{\"id\":\"item-1\"},\"started_at_ms\":123}}\n"
	if err := os.WriteFile(path, []byte(first), 0o600); err != nil {
		t.Fatal(err)
	}
	initial, err := scanPageMetadata(context.Background(), &pageMetadataCache{path: path, times: map[transcriptItemKey]itemTimestamps{}})
	if err != nil || initial.mode != "plan" || initial.times[transcriptItemKey{"turn-1", "item-1"}].startedAtMS != 123 {
		t.Fatalf("initial = %#v, %v", initial, err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("{\"type\":\"turn_context\",\"payload\":{\"collaboration_mode\":{\"mode\":\"default\"}}"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	partial, err := scanPageMetadata(context.Background(), initial)
	if err != nil || partial.mode != "plan" || partial.offset != initial.offset {
		t.Fatalf("partial = %#v, %v", partial, err)
	}
	file, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("}\n"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	complete, err := scanPageMetadata(context.Background(), partial)
	if err != nil || complete.mode != "default" || complete.offset <= partial.offset {
		t.Fatalf("complete mode=%q offset=%d, %v", complete.mode, complete.offset, err)
	}
}

func waitPageMetadataReady(t *testing.T, client *Client, threadID string) *pageMetadataCache {
	t.Helper()
	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for {
		client.pageMu.Lock()
		cache := client.pageMetadata[threadID]
		ready := cache != nil && cache.ready && !cache.pending
		client.pageMu.Unlock()
		if ready {
			return cache
		}
		select {
		case <-tick.C:
		case <-deadline:
			t.Fatal("page metadata rebuild did not finish")
		}
	}
}

func TestPageMetadataEarlierEditAndAppendExpires(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	defaultLine := "{\"type\":\"turn_context\",\"payload\":{\"collaboration_mode\":{\"mode\":\"default\"}}}\n"
	planLine := "{\"type\":\"turn_context\",\"payload\":{\"collaboration_mode\":{\"mode\":\"plan\"}}}   \n"
	stampLine := "{\"type\":\"event_msg\",\"timestamp\":\"2026-09-29T15:00:00Z\",\"payload\":{\"type\":\"item_completed\",\"turn_id\":\"turn-1\",\"item\":{\"id\":\"item-1\"},\"started_at_ms\":123}}\n"
	filler := "{\"padding\":\"" + strings.Repeat("x", 5000) + "\"}\n"
	original := defaultLine + stampLine + filler
	if len(defaultLine) != len(planLine) {
		t.Fatal("replacement must retain the earlier record length")
	}
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	base := time.Now()
	initial, err := scanPageMetadataAt(context.Background(), &pageMetadataCache{path: path, forceFull: true}, base, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	revised := planLine + strings.Replace(stampLine, "123", "456", 1) + filler + "{}\n"
	if err := os.WriteFile(path, []byte(revised), 0o600); err != nil {
		t.Fatal(err)
	}
	if !pageMetadataTailMatches(path, initial.file, initial.file.Size(), initial.tail) {
		t.Fatal("test did not preserve the old 4 KiB tail")
	}
	incremental, err := scanPageMetadataAt(context.Background(), initial, base.Add(20*time.Second), nil, nil)
	key := transcriptItemKey{"turn-1", "item-1"}
	if err != nil || incremental.mode != "default" || incremental.times[key].startedAtMS != 123 || !incremental.fullReadStartedAt.Equal(base) {
		t.Fatalf("cheap append probe unexpectedly detected the earlier edit: mode=%q times=%#v age=%v err=%v", incremental.mode, incremental.times, incremental.fullReadStartedAt, err)
	}
	var elapsed atomic.Int64
	elapsed.Store(int64(time.Minute))
	client := newTestClient("/tmp/codex-page-test-unused.sock")
	defer client.Close()
	client.pageClock = func() time.Time { return base.Add(time.Duration(elapsed.Load())) }
	client.pageMetadata["thread-1"] = incremental
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	mode, times, pending := client.pageMetadataSnapshot("thread-1", path, "", 0, info, []TranscriptEntry{{TurnID: "turn-1", ItemID: "item-1"}})
	if mode != "" || len(times) != 0 || !pending {
		t.Fatalf("expired snapshot served stale mode/timestamps: %q, %#v, %v", mode, times, pending)
	}
	ready := waitPageMetadataReady(t, client, "thread-1")
	if ready.mode != "plan" || ready.times[key].startedAtMS != 456 || !ready.fullReadStartedAt.Equal(base.Add(time.Minute)) {
		t.Fatalf("fresh rebuild did not repair earlier edit: mode=%q times=%#v age=%v", ready.mode, ready.times, ready.fullReadStartedAt)
	}
	client.pageMu.Lock()
	count, scanned := client.pageRebuildCount, client.pageRebuildBytes
	client.pageMu.Unlock()
	if count != 1 || scanned < uint64(len(revised)) {
		t.Fatalf("rebuild diagnostics count=%d bytes=%d", count, scanned)
	}
}

func TestPageMetadataUnchangedExpiryAndContinuousAppends(t *testing.T) {
	for _, appendRecords := range []bool{false, true} {
		t.Run(fmt.Sprintf("append=%v", appendRecords), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "rollout.jsonl")
			if err := os.WriteFile(path, []byte("{\"type\":\"turn_context\",\"payload\":{\"collaboration_mode\":{\"mode\":\"plan\"}}}\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			base := time.Now()
			cache, err := scanPageMetadataAt(context.Background(), &pageMetadataCache{path: path, forceFull: true}, base, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			var elapsed atomic.Int64
			client := newTestClient("/tmp/codex-page-test-unused.sock")
			defer client.Close()
			client.pageClock = func() time.Time { return base.Add(time.Duration(elapsed.Load())) }
			client.pageMetadata["thread-1"] = cache
			for _, second := range []int64{10, 20, 30, 40, 50, 59} {
				elapsed.Store(int64(time.Duration(second) * time.Second))
				if appendRecords {
					file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
					if err != nil {
						t.Fatal(err)
					}
					_, err = file.WriteString("{}\n")
					if closeErr := file.Close(); err == nil {
						err = closeErr
					}
					if err != nil {
						t.Fatal(err)
					}
					cache, err = scanPageMetadataAt(context.Background(), cache, base.Add(time.Duration(second)*time.Second), nil, nil)
					if err != nil {
						t.Fatal(err)
					}
					client.pageMetadata["thread-1"] = cache
				}
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				mode, _, pending := client.pageMetadataSnapshot("thread-1", path, "", 0, info, nil)
				if mode != "plan" || pending || !cache.fullReadStartedAt.Equal(base) {
					t.Fatalf("cache hit/append renewed age at %ds: mode=%q pending=%v started=%v", second, mode, pending, cache.fullReadStartedAt)
				}
			}
			elapsed.Store(int64(time.Minute))
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			mode, _, pending := client.pageMetadataSnapshot("thread-1", path, "", 0, info, nil)
			if mode != "" || !pending {
				t.Fatalf("expired snapshot = %q, %v", mode, pending)
			}
			ready := waitPageMetadataReady(t, client, "thread-1")
			if ready.mode != "plan" || !ready.fullReadStartedAt.Equal(base.Add(time.Minute)) {
				t.Fatalf("unchanged/append rebuild = %q, %v", ready.mode, ready.fullReadStartedAt)
			}
		})
	}
}

func TestPageMetadataFailedDelayedAndStaleJobs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	if err := os.WriteFile(path, []byte("{\"type\":\"turn_context\",\"payload\":{\"collaboration_mode\":{\"mode\":\"plan\"}}}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := time.Now()
	result, err := scanPageMetadataAt(context.Background(), &pageMetadataCache{path: path, forceFull: true}, base, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := newTestClient("/tmp/codex-page-test-unused.sock")
	defer client.Close()
	client.pageClock = func() time.Time { return base.Add(61 * time.Second) }
	old := &pageMetadataCache{path: path, generation: 0, pending: true, forceFull: true}
	client.pageMetadata["thread-1"] = old
	client.finishPageMetadata("thread-1", old, nil, errors.New("scan failed"), 512, time.Second, true)
	failed := client.pageMetadata["thread-1"]
	if failed == old || failed.mode != "" || len(failed.times) != 0 || !failed.retryAt.After(base.Add(61*time.Second)) || client.pageRebuildCount != 1 || client.pageRebuildBytes != 512 || client.pageRebuildTime != time.Second {
		t.Fatalf("failed rebuild retained values or lost diagnostics: %#v", failed)
	}
	for len(pageMetadataSlots) < cap(pageMetadataSlots) {
		pageMetadataSlots <- struct{}{}
	}
	defer func() {
		for len(pageMetadataSlots) > 0 {
			<-pageMetadataSlots
		}
	}()
	old = result
	old.pending = true
	client.pageMetadata["thread-1"] = old
	client.finishPageMetadata("thread-1", old, result, nil, 0, 0, false)
	delayed := client.pageMetadata["thread-1"]
	if delayed == old || delayed.mode != "" || len(delayed.times) != 0 || !delayed.forceFull || !delayed.retryAt.After(base.Add(61*time.Second)) {
		t.Fatalf("expired delayed scan published stale values: %#v", delayed)
	}
	stale := &pageMetadataCache{path: path, generation: 1, pending: true}
	client.pageMetadata["thread-1"] = stale
	client.finishPageMetadata("thread-1", stale, result, nil, 0, 0, false)
	if client.pageMetadata["thread-1"] != nil {
		t.Fatal("wrong-generation job published a result")
	}
}

func TestPageMetadataTailMismatchInvalidatesImmediately(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	initialText := "{\"type\":\"turn_context\",\"payload\":{\"collaboration_mode\":{\"mode\":\"plan\"}}}\n{}\n"
	if err := os.WriteFile(path, []byte(initialText), 0o600); err != nil {
		t.Fatal(err)
	}
	cache, err := scanPageMetadata(context.Background(), &pageMetadataCache{path: path, forceFull: true})
	if err != nil {
		t.Fatal(err)
	}
	client := newTestClient("/tmp/codex-page-test-unused.sock")
	defer client.Close()
	client.pageMetadata["thread-1"] = cache
	if err := os.WriteFile(path, []byte(strings.Replace(initialText, "{}\n", "{ }\n", 1)+"{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	mode, times, pending := client.pageMetadataSnapshot("thread-1", path, "", 0, info, nil)
	if mode != "" || len(times) != 0 || !pending || client.pageMetadata["thread-1"] == cache {
		t.Fatalf("old-tail mismatch retained cache: mode=%q pending=%v", mode, pending)
	}
	waitPageMetadataReady(t, client, "thread-1")
}

func TestPageMetadataFreshRebuildCanClearMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	if err := os.WriteFile(path, []byte("{\"type\":\"turn_context\",\"payload\":{\"collaboration_mode\":{\"mode\":\"plan\"}}}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := time.Now()
	cache, err := scanPageMetadataAt(context.Background(), &pageMetadataCache{path: path, forceFull: true}, base, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var elapsed atomic.Int64
	elapsed.Store(int64(time.Minute))
	client := newTestClient("/tmp/codex-page-test-unused.sock")
	defer client.Close()
	client.pageClock = func() time.Time { return base.Add(time.Duration(elapsed.Load())) }
	client.pageMetadata["thread-1"] = cache
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	mode, _, pending := client.pageMetadataSnapshot("thread-1", path, "", 0, info, nil)
	if mode != "" || !pending {
		t.Fatalf("changed file retained mode: %q, %v", mode, pending)
	}
	ready := waitPageMetadataReady(t, client, "thread-1")
	if ready.mode != "" || !ready.fullReadStartedAt.Equal(base.Add(time.Minute)) {
		t.Fatalf("fresh rebuild retained absent mode: %q, %v", ready.mode, ready.fullReadStartedAt)
	}
}

func TestReadThreadPageKeepsValidatedSettingsModeDuringExpiry(t *testing.T) {
	for _, test := range []struct {
		settingsMode, expected string
	}{
		{"default", "default"},
		{"unrecognized", ""},
	} {
		t.Run(test.settingsMode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "rollout.jsonl")
			if err := os.WriteFile(path, []byte("{\"type\":\"turn_context\",\"payload\":{\"collaboration_mode\":{\"mode\":\"plan\"}}}\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			base := time.Now()
			cache, err := scanPageMetadataAt(context.Background(), &pageMetadataCache{path: path, forceFull: true}, base, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			thread := map[string]any{"id": "thread-1", "cwd": "/workspace", "path": path, "status": map[string]any{"type": "idle"}, "source": "vscode", "createdAt": 1}
			cache.lineage = pageIdentity(thread)
			client := newTestClient("/tmp/codex-page-test-unused.sock")
			defer client.Close()
			client.pageClock = func() time.Time { return base.Add(time.Minute) }
			client.pageMetadata["thread-1"] = cache
			client.cacheThreadSettings("thread-1", ThreadSettings{CollaborationMode: test.settingsMode}, 0)
			request := func(_ context.Context, method string, _ any, output any) error {
				var value any
				switch method {
				case "thread/read":
					value = map[string]any{"thread": thread}
				case "thread/turns/list":
					value = map[string]any{"data": []any{}, "nextCursor": nil}
				default:
					return fmt.Errorf("unexpected RPC %s", method)
				}
				data, err := json.Marshal(value)
				if err != nil {
					return err
				}
				return json.Unmarshal(data, output)
			}
			page, err := client.readThreadPage(context.Background(), "thread-1", "", request)
			if err != nil || page.CollaborationMode != test.expected || !page.MetadataPending {
				t.Fatalf("page mode=%q pending=%v err=%v", page.CollaborationMode, page.MetadataPending, err)
			}
		})
	}
}

func TestPageMetadataCompletionRejectsReplacementAndLineageChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	if err := os.WriteFile(path, []byte("{\"type\":\"turn_context\",\"payload\":{\"collaboration_mode\":{\"mode\":\"plan\"}}}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cache, err := scanPageMetadata(context.Background(), &pageMetadataCache{path: path, lineage: "first", forceFull: true})
	if err != nil {
		t.Fatal(err)
	}
	client := newTestClient("/tmp/codex-page-test-unused.sock")
	defer client.Close()
	client.pageMetadata["thread-1"] = cache
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	mode, _, pending := client.pageMetadataSnapshot("thread-1", path, "second", 0, info, nil)
	if mode != "" || !pending || client.pageMetadata["thread-1"] == cache {
		t.Fatalf("lineage change retained metadata: mode=%q pending=%v", mode, pending)
	}
	replacement := path + ".replacement"
	if err := os.WriteFile(replacement, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	if err := verifyPageMetadataFile(cache); err == nil {
		t.Fatal("completion accepted replaced rollout")
	}
}

func TestPageMetadataScanDetectsSameSizeModification(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	plan := "{\"type\":\"turn_context\",\"payload\":{\"collaboration_mode\":{\"mode\":\"plan\"}}}   \n"
	def := "{\"type\":\"turn_context\",\"payload\":{\"collaboration_mode\":{\"mode\":\"default\"}}}\n"
	filler := "{\"padding\":\"" + strings.Repeat("x", 5000) + "\"}\n"
	if len(plan) != len(def) {
		t.Fatal("replacement must keep file size")
	}
	if err := os.WriteFile(path, []byte(plan+filler), 0o600); err != nil {
		t.Fatal(err)
	}
	base := time.Now()
	cache, err := scanPageMetadataAt(context.Background(), &pageMetadataCache{path: path, forceFull: true}, base, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(def+filler), 0o600); err != nil {
		t.Fatal(err)
	}
	changedAt := cache.file.ModTime().Add(time.Second)
	if err := os.Chtimes(path, changedAt, changedAt); err != nil {
		t.Fatal(err)
	}
	revised, err := scanPageMetadataAt(context.Background(), cache, base.Add(10*time.Second), nil, nil)
	if err != nil || revised.mode != "default" || !revised.fullReadStartedAt.Equal(base.Add(10*time.Second)) {
		t.Fatalf("same-size edit was treated as append: mode=%q age=%v err=%v", revised.mode, revised.fullReadStartedAt, err)
	}
}

package codex

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
)

// TurnMetadata uses server turn boundaries. Counts are distinct root-thread
// items, independent of transcript presentation and lifecycle notifications.
type TurnMetadata struct {
	ID            string `json:"id"`
	Status        string `json:"status"`
	StartedAtMS   int64  `json:"startedAtMs,omitempty"`
	CompletedAtMS int64  `json:"completedAtMs,omitempty"`
	DurationMS    int64  `json:"durationMs,omitempty"`
	Messages      int    `json:"messages"`
	ToolCalls     int    `json:"toolCalls"`
	CountsKnown   bool   `json:"countsKnown"`
}

type cachedTurnHistory struct {
	path     string
	turns    []map[string]any // Newest first; only metadata is retained.
	cursor   string
	complete bool
}

// Unwatched interrupted backfills retain at most eight idle histories. Watches
// and all readers, including callers waiting on the gate, pin their entry.
const idleTurnHistoryLimit = 8

type threadHistoryCache struct {
	gate   chan struct{}
	cached cachedTurnHistory // Protected by gate.
	// Ownership and retry eligibility are protected by Client.turnHistoryMu.
	readers  int
	watchers int
	retry    bool
}

func (c *Client) retainTurnHistory(threadID string, watch bool) *threadHistoryCache {
	c.turnHistoryMu.Lock()
	defer c.turnHistoryMu.Unlock()
	if c.turnHistory == nil {
		c.turnHistory = map[string]*threadHistoryCache{}
	}
	cache := c.turnHistory[threadID]
	if cache == nil {
		cache = &threadHistoryCache{gate: make(chan struct{}, 1)}
		c.turnHistory[threadID] = cache
	}
	if watch {
		cache.watchers++
	} else {
		cache.readers++
	}
	c.idleTurnHistory = slices.DeleteFunc(c.idleTurnHistory, func(id string) bool { return id == threadID })
	return cache
}

func (c *Client) releaseTurnHistory(threadID string, watch bool) {
	c.turnHistoryMu.Lock()
	defer c.turnHistoryMu.Unlock()
	cache := c.turnHistory[threadID]
	if watch {
		cache.watchers--
	} else {
		cache.readers--
	}
	if cache.watchers > 0 || cache.readers > 0 {
		return
	}
	if !cache.retry {
		delete(c.turnHistory, threadID)
		return
	}
	c.idleTurnHistory = append(c.idleTurnHistory, threadID)
	if len(c.idleTurnHistory) > idleTurnHistoryLimit {
		delete(c.turnHistory, c.idleTurnHistory[0])
		c.idleTurnHistory = c.idleTurnHistory[1:]
	}
}

func turnMetadata(turn map[string]any, threadID string) TurnMetadata {
	if cached, ok := turn["_activityMetadata"].(TurnMetadata); ok {
		return cached
	}
	result := TurnMetadata{ID: stringValue(turn["id"]), Status: statusValue(turn["status"])}
	result.CountsKnown = stringValue(turn["itemsView"]) == "" || stringValue(turn["itemsView"]) == "full"
	result.StartedAtMS = integerValue(turn["startedAt"]) * 1000
	result.CompletedAtMS = integerValue(turn["completedAt"]) * 1000
	result.DurationMS = integerValue(turn["durationMs"])
	items, _ := turn["items"].([]any)
	if !result.CountsKnown {
		items = nil
	}
	seen := map[string]bool{}
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		id, kind := stringValue(item["id"]), stringValue(item["type"])
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		switch kind {
		case "agentMessage", "plan":
			result.Messages++
		case "collabAgentToolCall":
			if sender := stringValue(item["senderThreadId"]); sender == threadID {
				result.ToolCalls++
			}
		case "commandExecution", "fileChange", "mcpToolCall", "dynamicToolCall", "webSearch", "imageView", "imageGeneration", "sleep":
			result.ToolCalls++
		}
	}
	return result
}

func integerValue(value any) int64 {
	if number, ok := value.(float64); ok && number >= 0 && number < 1<<53 && number == float64(int64(number)) {
		return int64(number)
	}
	return 0
}

// readTurnHistory refreshes recent full items and backfills older metadata. Each
// thread has a cancellable gate, and a timed-out backfill resumes on the next
// call. No authority-wide lock is held during RPCs or history aggregation.
func (c *Client) readTurnHistory(ctx context.Context, threadID, firstView, rolloutPath string, cache *threadHistoryCache) ([]map[string]any, error) {
	select {
	case cache.gate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-cache.gate }()
	cached := cache.cached
	if cached.path != rolloutPath || !filepath.IsAbs(rolloutPath) || filepath.Clean(rolloutPath) != rolloutPath {
		cached = cachedTurnHistory{path: rolloutPath}
	}
	var turns []map[string]any
	seenIDs, seenCursors := map[string]bool{}, map[string]bool{}
	cursor, complete, save := "", false, false
	// Preserve only successfully loaded pages, including partial backfills.
	defer func() {
		if !save {
			return
		}
		metadata := make([]map[string]any, 0, len(turns))
		for _, turn := range turns {
			metadata = append(metadata, map[string]any{"id": turn["id"], "status": turn["status"], "_activityMetadata": turnMetadata(turn, threadID)})
		}
		cache.cached = cachedTurnHistory{path: rolloutPath, turns: metadata, cursor: cursor, complete: complete}
		c.turnHistoryMu.Lock()
		cache.retry = !complete && cursor != ""
		c.turnHistoryMu.Unlock()
	}()
	appendTurn := func(turn map[string]any) error {
		id := stringValue(turn["id"])
		if id == "" || seenIDs[id] {
			return errors.New("thread/turns/list returned missing or repeated turn identity")
		}
		if len(turns) >= 100000 {
			return errors.New("thread history exceeds activity limit")
		}
		seenIDs[id] = true
		turns = append(turns, turn)
		return nil
	}
	for {
		view := firstView
		if cursor != "" {
			view = "notLoaded"
		}
		params := map[string]any{"threadId": threadID, "limit": recentTurnLimit, "sortDirection": "desc", "itemsView": view}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var page struct {
			Data       *[]map[string]any `json:"data"`
			NextCursor *string           `json:"nextCursor"`
		}
		if err := c.Request(ctx, "thread/turns/list", params, &page); err != nil {
			return nil, err
		}
		if page.Data == nil {
			save = false
			return nil, errors.New("thread/turns/list returned no data")
		}
		for _, turn := range *page.Data {
			if err := appendTurn(turn); err != nil {
				save = false
				return nil, err
			}
		}
		if page.NextCursor == nil {
			complete, save = true, true
			break
		}
		next := *page.NextCursor
		if next == "" || seenCursors[next] {
			save = false
			return nil, errors.New("thread/turns/list returned empty or repeated cursor")
		}
		// Only reuse the immutable older suffix anchored in the refreshed page.
		// A changed rollout path or missing anchor starts a fresh backfill.
		if cursor == "" && len(turns) > 0 {
			anchor := stringValue(turns[len(turns)-1]["id"])
			for index, turn := range cached.turns {
				if stringValue(turn["id"]) != anchor {
					continue
				}
				valid := true
				for _, older := range cached.turns[index+1:] {
					if !slices.Contains([]string{"completed", "failed", "interrupted"}, statusValue(older["status"])) {
						valid = false
						break
					}
				}
				if !valid {
					break
				}
				for _, older := range cached.turns[index+1:] {
					if err := appendTurn(older); err != nil {
						save = false
						return nil, err
					}
				}
				if cached.complete {
					complete = true
				} else if cached.cursor != "" {
					next = cached.cursor
				}
				break
			}
		}
		cursor, save = next, true
		if complete {
			break
		}
		if seenCursors[cursor] {
			save = false
			return nil, errors.New("thread/turns/list returned repeated cursor")
		}
		seenCursors[cursor] = true
	}
	// The cache remains newest-first; callers receive a separate chronological slice.
	result := slices.Clone(turns)
	slices.Reverse(result)
	return result, nil
}

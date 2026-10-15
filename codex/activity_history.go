package codex

import (
	"context"
	"errors"
	"os"
	"slices"
)

// Only count-relevant projections survive a page read. Tool output, message
// bodies and full turn payloads never enter this cache.
type activityItemHistory struct {
	turn           map[string]any
	prefix         TurnMetadata
	anchor, cursor string
	tail           []map[string]any
	complete       bool
	loaded         bool
	seenCursors    map[string]bool
}

type activityHistory struct {
	lineageJob     *activityLineageJob
	lineage        *activityLineageCache
	prefix         activityPrefix
	path, identity string
	file           os.FileInfo
	tail           []byte
	turns          map[string]*activityItemHistory
	order          []string // newest first
	cursor         string
	complete       bool
	initialized    bool
}

func countProjection(item map[string]any) map[string]any {
	result := map[string]any{}
	for _, key := range []string{"id", "type", "status", "senderThreadId", "tool"} {
		if v, ok := item[key]; ok {
			result[key] = v
		}
	}
	return result
}

func addCounts(target *TurnMetadata, source TurnMetadata) {
	target.Messages += source.Messages
	target.ReceivedMessages += source.ReceivedMessages
	target.ToolCalls += source.ToolCalls
	target.HasSleep = target.HasSleep || source.HasSleep
}

func (h *activityItemHistory) metadata(thread string) TurnMetadata {
	value := turnMetadata(h.turn, thread)
	addCounts(&value, h.prefix)
	raw := make([]any, len(h.tail))
	for i, item := range h.tail {
		raw[i] = item
	}
	recent := turnMetadata(map[string]any{"id": value.ID, "items": raw, "itemsView": "full"}, thread)
	addCounts(&value, recent)
	value.AutomaticWaits = recent.AutomaticWaits
	value.CountsKnown = h.loaded && h.cursor == ""
	return value
}

func (c *Client) activityItems(ctx context.Context, thread string, h *activityItemHistory) error {
	params := map[string]any{"threadId": thread, "turnId": h.turn["id"], "sortDirection": "asc", "limit": 32}
	if h.cursor != "" {
		params["cursor"] = h.cursor
	} else if h.anchor != "" {
		params["cursor"] = map[string]any{"type": "item", "itemId": h.anchor}
	}
	var page struct {
		Data       *[]threadItemEntry `json:"data"`
		NextCursor *string            `json:"nextCursor"`
	}
	if err := c.itemPage(ctx, params, &page); err != nil {
		return err
	}
	if page.Data == nil || len(*page.Data) > 32 {
		return errors.New("invalid activity item page")
	}
	if page.NextCursor != nil && (*page.NextCursor == "" || *page.NextCursor == h.cursor || h.seenCursors[*page.NextCursor] || len(*page.Data) == 0) {
		return errors.New("activity item cursor did not progress")
	}
	seen := map[string]bool{}
	for _, entry := range *page.Data {
		id := stringValue(entry.Item["id"])
		if entry.TurnID != stringValue(h.turn["id"]) || id == "" || seen[id] || id == h.anchor {
			return errors.New("invalid activity item identity")
		}
		seen[id] = true
	}
	// Validate the full page before committing counts or continuation.
	for _, entry := range *page.Data {
		item := countProjection(entry.Item)
		index := slices.IndexFunc(h.tail, func(old map[string]any) bool { return old["id"] == item["id"] })
		if index >= 0 {
			h.tail[index] = item
		} else {
			h.tail = append(h.tail, item)
		}
	}
	for len(h.tail) > 0 && statusValue(h.tail[0]["status"]) != "inProgress" {
		first := h.tail[0]
		addCounts(&h.prefix, turnMetadata(map[string]any{"items": []any{first}, "itemsView": "full"}, thread))
		h.anchor = stringValue(first["id"])
		h.tail = h.tail[1:]
	}
	h.cursor = ""
	if page.NextCursor != nil {
		if h.seenCursors == nil {
			h.seenCursors = map[string]bool{}
		}
		h.seenCursors[*page.NextCursor] = true
		h.cursor = *page.NextCursor
	} else {
		h.loaded = true
		h.seenCursors = nil
	}
	h.complete = h.cursor == "" && slices.Contains([]string{"completed", "failed", "interrupted", "error"}, statusValue(h.turn["status"]))
	return nil
}

// readActivityHistory spends a bounded RPC quantum and returns compact partial
// progress instead of making a long thread an all-or-nothing foreground read.
func (c *Client) readActivityHistory(ctx context.Context, thread map[string]any, cache *threadHistoryCache) ([]map[string]any, bool, error) {
	select {
	case cache.gate <- struct{}{}:
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
	defer func() { <-cache.gate }()
	path := stringValue(thread["path"])
	file, err := pageFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			var probe struct {
				Data *[]map[string]any `json:"data"`
			}
			probeErr := c.historyRequest(ctx, "thread/turns/list", map[string]any{"threadId": thread["id"], "itemsView": "notLoaded", "sortDirection": "desc", "limit": 1}, &probe)
			if c.freshThreadMissingSourceRollout(thread, stringValue(thread["id"]), probeErr) {
				return nil, true, nil
			}
		}
		return nil, false, err
	}
	if file == nil {
		return nil, false, errors.New("activity history rollout is not materialized")
	}
	identity := pageIdentity(thread)
	h := cache.activity
	if h == nil || h.path != path || h.identity != identity || file == nil || h.file == nil || !os.SameFile(file, h.file) || file.Size() < h.file.Size() || !pageMetadataTailMatches(path, h.file, h.file.Size(), h.tail) {
		if h != nil && h.lineageJob != nil {
			h.lineageJob.cancel()
		}
		h = &activityHistory{path: path, identity: identity, turns: map[string]*activityItemHistory{}}
		cache.activity = h
		h.file = file
		h.tail, err = pageRolloutTail(path, file.Size())
		if err != nil {
			return nil, false, err
		}
	}
	threadID := stringValue(thread["id"])
	var head struct {
		Data       *[]map[string]any `json:"data"`
		NextCursor *string           `json:"nextCursor"`
	}
	if err := c.historyRequest(ctx, "thread/turns/list", map[string]any{"threadId": threadID, "itemsView": "notLoaded", "sortDirection": "desc", "limit": 20}, &head); err != nil {
		return nil, false, err
	}
	if head.Data == nil || len(*head.Data) > 20 {
		return nil, false, errors.New("activity turn page has no data")
	}
	// Missing/reordered anchors mean history changed or advanced beyond the
	// refreshed head. Rebuild instead of retaining totals from invisible turns.
	if h.initialized && len(h.order) > 0 {
		anchor := slices.IndexFunc(*head.Data, func(raw map[string]any) bool { return stringValue(raw["id"]) == h.order[0] })
		valid := anchor >= 0
		if valid {
			for i := anchor; i < len(*head.Data) && i-anchor < len(h.order); i++ {
				if stringValue((*head.Data)[i]["id"]) != h.order[i-anchor] {
					valid = false
					break
				}
			}
		}
		if !valid {
			if h.lineageJob != nil {
				h.lineageJob.cancel()
			}
			h = &activityHistory{path: path, identity: identity, file: file, turns: map[string]*activityItemHistory{}}
			h.tail, err = pageRolloutTail(path, file.Size())
			if err != nil {
				return nil, false, err
			}
			cache.activity = h
		}
	}
	update := func(turns []map[string]any, prepend bool) error {
		shells := make([]map[string]any, 0, len(turns))
		ids := []string{}
		seen := map[string]bool{}
		for _, raw := range turns {
			shell, err := pageTurn(raw)
			if err != nil {
				return err
			}
			id := stringValue(shell["id"])
			if seen[id] {
				return errors.New("activity turn page repeats identity")
			}
			seen[id] = true
			shells = append(shells, shell)
		}
		// A rejected page must not leave orphaned entries in turns that a later
		// retry would omit from order and therefore from all subsequent totals.
		for _, shell := range shells {
			id := stringValue(shell["id"])
			if old := h.turns[id]; old != nil {
				old.turn = shell
			} else {
				h.turns[id] = &activityItemHistory{turn: shell}
				ids = append(ids, id)
			}
		}
		if prepend {
			h.order = append(ids, h.order...)
		} else {
			h.order = append(h.order, ids...)
		}
		return nil
	}
	if err := update(*head.Data, true); err != nil {
		return nil, false, err
	}
	if !h.initialized {
		h.initialized = true
		h.complete = head.NextCursor == nil
		if head.NextCursor != nil {
			if *head.NextCursor == "" {
				return nil, false, errors.New("invalid activity turn cursor")
			}
			h.cursor = *head.NextCursor
		}
	}
	processed := map[string]bool{}
	for quantum := 0; quantum < 8; quantum++ {
		var pending *activityItemHistory
		for _, id := range h.order {
			candidate := h.turns[id]
			if !candidate.complete && !processed[id] {
				pending = candidate
				break
			}
		}
		if pending != nil {
			if err := c.activityItems(ctx, threadID, pending); err != nil {
				return nil, false, err
			}
			// Active turns refresh once per observation, rather than starving backfill.
			if statusValue(pending.turn["status"]) == "inProgress" && pending.cursor == "" {
				processed[stringValue(pending.turn["id"])] = true
			}
			continue
		}
		if h.complete {
			break
		}
		var older struct {
			Data       *[]map[string]any `json:"data"`
			NextCursor *string           `json:"nextCursor"`
		}
		if err := c.historyRequest(ctx, "thread/turns/list", map[string]any{"threadId": threadID, "itemsView": "notLoaded", "sortDirection": "desc", "limit": 20, "cursor": h.cursor}, &older); err != nil {
			return nil, false, err
		}
		if older.Data == nil || older.NextCursor != nil && (*older.NextCursor == "" || *older.NextCursor == h.cursor || len(*older.Data) == 0) {
			return nil, false, errors.New("activity turn cursor did not progress")
		}
		if err := update(*older.Data, false); err != nil {
			return nil, false, err
		}
		h.complete = older.NextCursor == nil
		if older.NextCursor != nil {
			h.cursor = *older.NextCursor
		}
	}
	h.file = file
	h.tail, err = pageRolloutTail(path, file.Size())
	if err != nil {
		return nil, false, err
	}
	result := make([]map[string]any, 0, len(h.order))
	complete := h.complete
	for _, id := range h.order {
		turn := h.turns[id]
		value := turn.metadata(threadID)
		complete = complete && value.CountsKnown
		result = append(result, map[string]any{"id": id, "status": value.Status, "_activityMetadata": value})
	}
	slices.Reverse(result)
	c.turnHistoryMu.Lock()
	cache.retry = true
	c.turnHistoryMu.Unlock()
	return result, complete, nil
}

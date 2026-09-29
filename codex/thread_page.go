package codex

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"
)

const (
	transcriptPageSize = 100
	pageCursorLimit    = 256
	pageCursorBytes    = 8 << 20
	pageCursorIdle     = 30 * time.Minute
)

type TranscriptCursorError struct{ Code string }

func (e *TranscriptCursorError) Error() string { return e.Code }

type pageContinuation struct {
	threadID, cwd, path, identity string
	generation                    uint64
	file                          os.FileInfo
	tail                          []byte
	turns                         []map[string]any
	turnIndex                     int
	turnCursor                    string
	itemCursor                    string
	errorEmitted                  bool
	lastItemID                    string
	lastItemCursor                string
	lastTurnID                    string
	lastTurnCursor                string
	latestTurnID                  string
	volatileTurnID                string
	volatileTurnStatus            string
	lastUsed                      time.Time
	bytes                         int
}

func validPageCursor(token string) bool {
	if len(token) != 32 {
		return false
	}
	for _, ch := range token {
		if ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' {
			continue
		}
		return false
	}
	return true
}

func (c *Client) pageGeneration() uint64 {
	c.connectionMu.Lock()
	defer c.connectionMu.Unlock()
	return c.generation
}

func (c *Client) loadPageCursor(token, threadID string) (pageContinuation, error) {
	if !validPageCursor(token) {
		return pageContinuation{}, &TranscriptCursorError{Code: "transcript_cursor_invalid"}
	}
	c.pageMu.Lock()
	defer c.pageMu.Unlock()
	state, ok := c.pageCursors[token]
	if !ok || time.Since(state.lastUsed) > pageCursorIdle {
		delete(c.pageCursors, token)
		return pageContinuation{}, &TranscriptCursorError{Code: "transcript_cursor_expired"}
	}
	if state.threadID != threadID {
		return pageContinuation{}, &TranscriptCursorError{Code: "transcript_reset_required"}
	}
	state.lastUsed = time.Now()
	c.pageCursors[token] = state
	state.turns = slices.Clone(state.turns)
	return state, nil
}

func (c *Client) savePageCursor(state pageContinuation) (*string, error) {
	if state.file != nil {
		var err error
		state.tail, err = pageRolloutTail(state.path, state.file.Size())
		if err != nil {
			return nil, err
		}
	}
	bytes, err := json.Marshal(state.turns)
	if err != nil {
		return nil, err
	}
	state.bytes = len(bytes) + len(state.turnCursor) + len(state.itemCursor) + len(state.identity) + len(state.tail) + 512
	if state.bytes > pageCursorBytes {
		return nil, errors.New("thread page continuation exceeds limit")
	}
	random := make([]byte, 24)
	if _, err := rand.Read(random); err != nil {
		return nil, err
	}
	token := base64.RawURLEncoding.EncodeToString(random)
	state.lastUsed = time.Now()
	c.pageMu.Lock()
	defer c.pageMu.Unlock()
	for key, entry := range c.pageCursors {
		if time.Since(entry.lastUsed) > pageCursorIdle {
			delete(c.pageCursors, key)
		}
	}
	total := state.bytes
	for _, entry := range c.pageCursors {
		total += entry.bytes
	}
	for len(c.pageCursors) >= pageCursorLimit || total > pageCursorBytes {
		oldestKey := ""
		var oldest time.Time
		for key, entry := range c.pageCursors {
			if oldestKey == "" || entry.lastUsed.Before(oldest) {
				oldestKey, oldest = key, entry.lastUsed
			}
		}
		if oldestKey == "" {
			break
		}
		total -= c.pageCursors[oldestKey].bytes
		delete(c.pageCursors, oldestKey)
	}
	c.pageCursors[token] = state
	return &token, nil
}

func pageIdentity(thread map[string]any) string {
	fields := []any{thread["cwd"], thread["source"], thread["createdAt"], thread["forkedFromId"], thread["historyMode"]}
	value, _ := json.Marshal(fields)
	return string(value)
}

func pageFile(path string) (os.FileInfo, error) {
	if path == "" {
		return nil, nil
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("thread/read returned invalid rollout path")
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("thread rollout is not regular")
	}
	return info, nil
}

func pageRolloutTail(path string, size int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	length := int64(4096)
	if size < length {
		length = size
	}
	tail := make([]byte, length)
	if length > 0 {
		if _, err := file.ReadAt(tail, size-length); err != nil {
			return nil, err
		}
	}
	return tail, nil
}

func pageTurn(raw map[string]any) (map[string]any, error) {
	id, status := stringValue(raw["id"]), statusValue(raw["status"])
	if id == "" || status == "" {
		return nil, errors.New("thread/turns/list returned invalid turn identity or status")
	}
	if _, ok := raw["status"].(string); !ok {
		return nil, errors.New("thread/turns/list returned invalid turn status")
	}
	if failure := raw["error"]; failure != nil {
		switch value := failure.(type) {
		case string:
			if value == "" {
				return nil, errors.New("thread/turns/list returned invalid turn error")
			}
		case map[string]any:
			if message, ok := value["message"].(string); !ok || message == "" {
				return nil, errors.New("thread/turns/list returned invalid turn error")
			}
		default:
			return nil, errors.New("thread/turns/list returned invalid turn error")
		}
	}
	return map[string]any{"id": id, "status": status, "error": raw["error"], "startedAt": raw["startedAt"], "completedAt": raw["completedAt"]}, nil
}

func pageFailure(turn map[string]any) *TranscriptEntry {
	id, status := stringValue(turn["id"]), statusValue(turn["status"])
	var entry TranscriptEntry
	if failure := turn["error"]; failure != nil {
		entry = transcriptFailureEntry(id, failure)
	} else if status == "failed" || status == "error" {
		entry = TranscriptEntry{TurnID: id, Kind: "error", Summary: "Turn " + status}
	} else {
		return nil
	}
	entry.TurnStatus = status
	entries := []TranscriptEntry{entry}
	applyTurnTimestamps(entries, turn)
	return &entries[0]
}

func pageItem(turn map[string]any, item map[string]any) []TranscriptEntry {
	copyTurn := map[string]any{"id": turn["id"], "status": "completed", "items": []any{item}, "startedAt": turn["startedAt"], "completedAt": turn["completedAt"]}
	entries := transcriptEntries(copyTurn)
	for index := range entries {
		entries[index].TurnStatus = statusValue(turn["status"])
	}
	return entries
}

type pageRequestFunc func(context.Context, string, any, any) error

func (c *Client) ReadThreadPage(ctx context.Context, threadID, cursor string) (TranscriptPage, error) {
	return c.readThreadPage(ctx, threadID, cursor, c.Request)
}

func (c *Client) readThreadPage(ctx context.Context, threadID, cursor string, request pageRequestFunc) (TranscriptPage, error) {
	var state pageContinuation
	var err error
	if cursor != "" {
		state, err = c.loadPageCursor(cursor, threadID)
		if err != nil {
			return TranscriptPage{}, err
		}
	}
	var metadata struct {
		Thread map[string]any `json:"thread"`
	}
	if err := request(ctx, "thread/read", map[string]any{"threadId": threadID, "includeTurns": false}, &metadata); err != nil {
		return TranscriptPage{}, fmt.Errorf("read Codex thread metadata: %w", err)
	}
	thread := metadata.Thread
	if thread == nil || stringValue(thread["id"]) != threadID {
		return TranscriptPage{}, errors.New("thread/read returned the wrong thread")
	}
	path, cwd := stringValue(thread["path"]), stringValue(thread["cwd"])
	if path == "" || cwd == "" {
		return TranscriptPage{}, errors.New("thread/read returned invalid source identity")
	}
	identity, generation := pageIdentity(thread), c.pageGeneration()
	file, fileErr := pageFile(path)
	if fileErr != nil && !errors.Is(fileErr, os.ErrNotExist) {
		return TranscriptPage{}, fileErr
	}
	if cursor != "" && (state.generation != generation || state.cwd != cwd || state.path != path || state.identity != identity ||
		fileErr != nil || state.file == nil || file == nil || !os.SameFile(state.file, file) || file.Size() < state.file.Size() ||
		(file.Size() == state.file.Size() && !file.ModTime().Equal(state.file.ModTime()))) {
		return TranscriptPage{}, &TranscriptCursorError{Code: "transcript_reset_required"}
	}
	if cursor != "" {
		tail, err := pageRolloutTail(path, state.file.Size())
		if err != nil || !bytes.Equal(tail, state.tail) {
			return TranscriptPage{}, &TranscriptCursorError{Code: "transcript_reset_required"}
		}
	}
	page := TranscriptPage{Transcript: Transcript{ThreadID: threadID, Status: statusValue(thread["status"]),
		Model: stringValue(thread["model"]), ReasoningEffort: stringValue(thread["reasoningEffort"]), Entries: []TranscriptEntry{}}}
	if page.Status == "" {
		return TranscriptPage{}, errors.New("thread/read returned invalid status")
	}
	state.threadID, state.cwd, state.path, state.identity, state.generation, state.file = threadID, cwd, path, identity, generation, file
	if cursor == "" {
		state = pageContinuation{threadID: threadID, cwd: cwd, path: path, identity: identity, generation: generation, file: file}
	}
	if cursor != "" {
		var latest struct {
			Data *[]map[string]any `json:"data"`
		}
		if err := request(ctx, "thread/turns/list", map[string]any{"threadId": threadID, "limit": 1, "sortDirection": "desc", "itemsView": "notLoaded"}, &latest); err != nil {
			return TranscriptPage{}, err
		}
		if latest.Data == nil || len(*latest.Data) > 1 {
			return TranscriptPage{}, errors.New("thread/turns/list returned invalid latest turn")
		}
		if len(*latest.Data) > 0 {
			current, err := pageTurn((*latest.Data)[0])
			if err != nil {
				return TranscriptPage{}, err
			}
			state.latestTurnID = stringValue(current["id"])
			if state.volatileTurnID != "" && (state.latestTurnID != state.volatileTurnID || statusValue(current["status"]) != state.volatileTurnStatus) {
				return TranscriptPage{}, &TranscriptCursorError{Code: "transcript_reset_required"}
			}
		} else if state.volatileTurnID != "" {
			return TranscriptPage{}, &TranscriptCursorError{Code: "transcript_reset_required"}
		}
	}
	used, visited := 0, 0
	fresh := false
	seenItems := make(map[string]bool)
	for used < transcriptPageSize && visited < transcriptPageSize {
		if state.turnIndex >= len(state.turns) {
			if state.turns != nil && state.turnCursor == "" {
				break
			}
			params := map[string]any{"threadId": threadID, "limit": transcriptPageSize - visited, "sortDirection": "desc", "itemsView": "notLoaded"}
			if state.turnCursor != "" {
				params["cursor"] = state.turnCursor
			}
			var turns struct {
				Data       *[]map[string]any `json:"data"`
				NextCursor *string           `json:"nextCursor"`
			}
			if err := request(ctx, "thread/turns/list", params, &turns); err != nil {
				if cursor == "" && c.freshThreadMissingSourceRollout(thread, threadID, err) {
					fresh = true
					break
				}
				return TranscriptPage{}, fmt.Errorf("read Codex thread turns: %w", err)
			}
			if turns.Data == nil || len(*turns.Data) > transcriptPageSize-visited {
				return TranscriptPage{}, errors.New("thread/turns/list returned invalid page")
			}
			if len(*turns.Data) == 0 && turns.NextCursor != nil {
				return TranscriptPage{}, errors.New("thread/turns/list cursor did not progress")
			}
			if turns.NextCursor != nil && (*turns.NextCursor == "" || *turns.NextCursor == state.turnCursor || *turns.NextCursor == state.lastTurnCursor || len(*turns.NextCursor) > 4096) {
				return TranscriptPage{}, errors.New("thread/turns/list returned invalid cursor")
			}
			state.turns = make([]map[string]any, 0, len(*turns.Data))
			seenTurns := make(map[string]bool)
			for _, raw := range *turns.Data {
				turn, err := pageTurn(raw)
				if err != nil {
					return TranscriptPage{}, err
				}
				id := stringValue(turn["id"])
				if seenTurns[id] || id == state.lastTurnID {
					return TranscriptPage{}, errors.New("thread/turns/list repeated turn")
				}
				seenTurns[id] = true
				state.turns = append(state.turns, turn)
			}
			if state.latestTurnID == "" && len(state.turns) > 0 {
				state.latestTurnID = stringValue(state.turns[0]["id"])
				status := statusValue(state.turns[0]["status"])
				if !slices.Contains([]string{"completed", "failed", "interrupted", "error"}, status) {
					state.volatileTurnID, state.volatileTurnStatus = state.latestTurnID, status
				}
			}
			state.turnIndex = 0
			state.lastTurnCursor, state.turnCursor = state.turnCursor, ""
			if turns.NextCursor != nil {
				state.turnCursor = *turns.NextCursor
			}
			if len(state.turns) == 0 {
				break
			}
		}
		turn := state.turns[state.turnIndex]
		visited++
		if !state.errorEmitted {
			if failure := pageFailure(turn); failure != nil {
				page.Entries = append(page.Entries, *failure)
				used++
			}
			state.errorEmitted = true
		}
		if used == transcriptPageSize {
			break
		}
		params := map[string]any{"threadId": threadID, "turnId": turn["id"], "limit": transcriptPageSize - used, "sortDirection": "desc"}
		if state.itemCursor != "" {
			params["cursor"] = state.itemCursor
		}
		var items struct {
			Data       *[]threadItemEntry `json:"data"`
			NextCursor *string            `json:"nextCursor"`
		}
		if err := request(ctx, "thread/items/list", params, &items); err != nil {
			return TranscriptPage{}, fmt.Errorf("read Codex thread items: %w", err)
		}
		if items.Data == nil || len(*items.Data) > transcriptPageSize-used {
			return TranscriptPage{}, errors.New("thread/items/list returned invalid page")
		}
		if len(*items.Data) == 0 && items.NextCursor != nil {
			return TranscriptPage{}, errors.New("thread/items/list cursor did not progress")
		}
		for index, item := range *items.Data {
			id := stringValue(item.Item["id"])
			key := item.TurnID + "\x00" + id
			if item.TurnID != stringValue(turn["id"]) || id == "" || seenItems[key] || (index == 0 && id == state.lastItemID) {
				return TranscriptPage{}, errors.New("thread/items/list returned invalid or repeated item")
			}
			seenItems[key] = true
			page.Entries = append(page.Entries, pageItem(turn, item.Item)...)
			used++
			state.lastItemID = id
		}
		if items.NextCursor != nil {
			next := *items.NextCursor
			if next == "" || next == state.itemCursor || next == state.lastItemCursor || len(next) > 4096 {
				return TranscriptPage{}, errors.New("thread/items/list returned invalid cursor")
			}
			state.lastItemCursor, state.itemCursor = state.itemCursor, next
		} else {
			state.lastTurnID = stringValue(turn["id"])
			state.turnIndex++
			state.itemCursor, state.lastItemCursor, state.lastItemID = "", "", ""
			state.errorEmitted = false
		}
	}
	if fileErr != nil && !fresh {
		return TranscriptPage{}, fileErr
	}
	page.LatestTurnID = state.latestTurnID
	if c.pageGeneration() != generation {
		return TranscriptPage{}, &TranscriptCursorError{Code: "transcript_reset_required"}
	}
	slices.Reverse(page.Entries)
	page.HasOlder = state.itemCursor != "" || state.turnIndex < len(state.turns) || state.turnCursor != ""
	if page.HasOlder {
		page.OlderCursor, err = c.savePageCursor(state)
		if err != nil {
			return TranscriptPage{}, err
		}
	}
	mode, times, pending := c.pageMetadataSnapshot(threadID, path, state.identity, generation, file, page.Entries)
	if settings, _, ok := c.cachedSettings(threadID); ok && validPageMode(settings.settings.CollaborationMode) {
		mode = settings.settings.CollaborationMode
	}
	page.CollaborationMode, page.MetadataPending = mode, pending || mode == ""
	c.applyTranscriptTimestamps(threadID, page.Entries, times)
	if c.pageGeneration() != generation {
		return TranscriptPage{}, &TranscriptCursorError{Code: "transcript_reset_required"}
	}
	return page, nil
}

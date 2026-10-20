package codex

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// HistoryReader isolates potentially large reads from the connection that owns
// live watches and approval offers. The application owns its lifetime and may
// share it between interactive and observer clients for the same authority.
type HistoryReader interface {
	Request(context.Context, string, any, any) error
}

type HistoryPool struct {
	mu      sync.Mutex
	closed  bool
	slots   chan *Client
	clients []*Client
}

// NewHistoryPool creates two passive, non-subscribing connections. Each admits
// one RPC at a time, so an oversized reply can only fail its own history read.
func NewHistoryPool(socket string) *HistoryPool {
	p := &HistoryPool{slots: make(chan *Client, 2)}
	for range 2 {
		c := NewWithOptions(socket, ClientOptions{ObserverOnly: true, ClientInfo: ClientInfo{Name: "codex-web-history", Title: "Conversation history", Version: "0.1.0"}})
		p.clients = append(p.clients, c)
		p.slots <- c
	}
	return p
}

func (p *HistoryPool) Request(ctx context.Context, method string, params any, result any) error {
	switch method {
	case "thread/read", "thread/turns/list", "thread/items/list":
	default:
		return errors.New("history reader only permits history reads")
	}
	var c *Client
	select {
	case c = <-p.slots:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { p.slots <- c }()
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return errors.New("history reader is closed")
	}
	err := c.Request(ctx, method, params, result)
	if err != nil && ctx.Err() != nil {
		// An admitted server read may still be running after caller cancellation.
		// Retire this transport before its slot can serve another history request.
		c.connectionMu.Lock()
		connection, generation := c.connection, c.generation
		c.connectionMu.Unlock()
		if connection != nil {
			c.markDisconnected(connection, generation, ctx.Err())
		}
	}
	return err
}

func (p *HistoryPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	clients := append([]*Client(nil), p.clients...)
	p.mu.Unlock()
	for _, c := range clients {
		c.Close()
	}
}

func (c *Client) historyRequest(ctx context.Context, method string, params any, result any) error {
	if c.options.HistoryReader == nil {
		return c.Request(ctx, method, params, result)
	}
	return c.options.HistoryReader.Request(ctx, method, params, result)
}

func historySizeError(err error) bool {
	// websocket's read-limit error is retained inside TransportError. Do not
	// retry other protocol/permission errors with a different request shape.
	return err != nil && strings.Contains(err.Error(), "message too big")
}

// itemPage retries an unconsumed cursor; callers commit progress only after a
// complete successful page. A single-item failure remains explicit.
func (c *Client) itemPage(ctx context.Context, params map[string]any, result any) error {
	limit := 32
	if n, ok := params["limit"].(int); ok && n < limit {
		limit = n
	}
	c.historyPageMu.Lock()
	if c.historyPageLimit > 0 && c.historyPageLimit < limit {
		limit = c.historyPageLimit
	}
	c.historyPageMu.Unlock()
	copy := make(map[string]any, len(params))
	for k, v := range params {
		copy[k] = v
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		copy["limit"] = limit
		err := c.historyRequest(ctx, "thread/items/list", copy, result)
		if !historySizeError(err) {
			return err
		}
		if limit <= 1 {
			return fmt.Errorf("individual history item exceeds transport safeguard: %w", err)
		}
		limit = max(1, limit/2)
		c.historyPageMu.Lock()
		c.historyPageLimit = limit
		c.historyPageMu.Unlock()
	}
}

func (c *Client) pagedHistoryRequest(ctx context.Context, method string, params any, result any) error {
	if method == "thread/items/list" && c.options.HistoryReader != nil {
		if values, ok := params.(map[string]any); ok {
			return c.itemPage(ctx, values, result)
		}
	}
	return c.historyRequest(ctx, method, params, result)
}

func (c *Client) readRecentThread(ctx context.Context, threadID string) (Transcript, error) {
	var meta struct {
		Thread map[string]any `json:"thread"`
	}
	if err := c.historyRequest(ctx, "thread/read", map[string]any{"threadId": threadID, "excludeTurns": true}, &meta); err != nil {
		return Transcript{}, err
	}
	if meta.Thread == nil || stringValue(meta.Thread["id"]) != threadID {
		return Transcript{}, errors.New("history returned the wrong thread")
	}
	result := Transcript{ThreadID: threadID, Status: statusValue(meta.Thread["status"]), Model: stringValue(meta.Thread["model"]), ReasoningEffort: stringValue(meta.Thread["reasoningEffort"]), CollaborationMode: "default", Entries: []TranscriptEntry{}}
	var turns struct {
		Data *[]map[string]any `json:"data"`
	}
	if err := c.historyRequest(ctx, "thread/turns/list", map[string]any{"threadId": threadID, "limit": recentTurnLimit, "sortDirection": "desc", "itemsView": "notLoaded"}, &turns); err != nil {
		if c.freshThreadMissingSourceRollout(meta.Thread, threadID, err) {
			return result, nil
		}
		return Transcript{}, err
	}
	if turns.Data == nil {
		return Transcript{}, errors.New("history returned no turns")
	}
	for i := len(*turns.Data) - 1; i >= 0; i-- {
		turn := (*turns.Data)[i]
		result.LatestTurnID = stringValue(turn["id"])
		cursor := ""
		seen := map[string]bool{}
		for {
			params := map[string]any{"threadId": threadID, "turnId": turn["id"], "limit": 32, "sortDirection": "asc"}
			if cursor != "" {
				params["cursor"] = cursor
			}
			var page struct {
				Data       *[]threadItemEntry `json:"data"`
				NextCursor *string            `json:"nextCursor"`
			}
			if err := c.itemPage(ctx, params, &page); err != nil {
				return Transcript{}, err
			}
			if page.Data == nil {
				return Transcript{}, errors.New("history returned no items")
			}
			for _, item := range *page.Data {
				id := stringValue(item.Item["id"])
				if item.TurnID != result.LatestTurnID || id == "" || seen[id] {
					return Transcript{}, errors.New("invalid recent history item")
				}
				seen[id] = true
				result.Entries = append(result.Entries, pageItem(turn, item.Item)...)
			}
			if page.NextCursor == nil {
				break
			}
			next := *page.NextCursor
			if next == "" || next == cursor || len(*page.Data) == 0 {
				return Transcript{}, errors.New("recent history cursor did not progress")
			}
			cursor = next
		}
		if failure := pageFailure(turn); failure != nil {
			result.Entries = append(result.Entries, *failure)
		}
	}
	wanted := make(map[transcriptItemKey]bool, len(result.Entries))
	for _, entry := range result.Entries {
		if entry.ItemID != "" {
			wanted[transcriptItemKey{entry.TurnID, entry.ItemID}] = true
		}
	}
	rollout, err := readRolloutMetadata(stringValue(meta.Thread["path"]), wanted)
	if err == nil && rollout.mode != "" {
		result.CollaborationMode = rollout.mode
	}
	if settings, _, ok := c.cachedSettings(threadID); ok {
		result.CollaborationMode = settings.settings.CollaborationMode
	}
	c.applyTranscriptTimestamps(threadID, result.Entries, rollout.itemTimes)
	file, _ := pageFile(stringValue(meta.Thread["path"]))
	c.presentCitations(threadID, pageIdentity(meta.Thread), stringValue(meta.Thread["path"]), c.pageGeneration(), file, result.Entries, c.pagedHistoryRequest)
	return result, nil
}

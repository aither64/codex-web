package codex

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"time"
)

// Source lookup is independent of transcript cursors. It uses only passive
// native history requests and never loads or resumes a conversation.
var citationWorkers = make(chan struct{}, 2)

type citationThread struct {
	identity, path string
	generation     uint64
	file           os.FileInfo
	tail           []byte
	used           time.Time
	lookups        map[transcriptItemKey]*citationLookup
}

type citationLookup struct {
	signature string
	wanted    map[string]bool
	scan      citationScan
	running   bool
	retry     time.Time
	used      time.Time
	size      int64
}

type citationScan struct {
	sources                                map[string]CitationSource
	turns                                  []string
	turnIndex                              int
	turnCursor, itemCursor                 string
	currentTurn                            string
	started, targetFound, targetSeen, done bool
}

func (c *Client) presentCitations(threadID, identity, path string, generation uint64, file os.FileInfo, entries []TranscriptEntry, request pageRequestFunc) {
	// A page can satisfy citations directly without creating workers or caches.
	local := make(map[string]CitationSource)
	for i := range entries {
		entry := &entries[i]
		maps.Copy(local, entryCitationSources(*entry))
		if entry.Kind != "agentMessage" && entry.Kind != "reasoning" && entry.Kind != "plan" {
			continue
		}
		spans := citationTokens(entry.Text)
		if len(spans) == 0 {
			continue
		}
		wanted := make(map[string]bool)
		for _, span := range spans {
			for _, ref := range span.refs {
				if _, ok := local[ref]; !ok {
					wanted[ref] = true
				}
			}
		}
		sources := maps.Clone(local)
		if len(wanted) > 0 {
			found, pending := c.lookupCitations(threadID, identity, path, generation, file, *entry, wanted, request)
			for ref, source := range found {
				if _, ok := sources[ref]; !ok {
					sources[ref] = source
				}
			}
			entry.CitationsPending = pending
		}
		entry.Citations = resolveCitationSpans(spans, sources)
	}
}

func (c *Client) lookupCitations(threadID, identity, path string, generation uint64, file os.FileInfo, entry TranscriptEntry, wanted map[string]bool, request pageRequestFunc) (map[string]CitationSource, bool) {
	refs := slices.Collect(maps.Keys(wanted))
	slices.Sort(refs)
	signature := strings.Join(refs, "\x00")
	key := transcriptItemKey{entry.TurnID, entry.ItemID}
	c.citationMu.Lock()
	defer c.citationMu.Unlock()
	if c.citationCache == nil {
		c.citationCache = make(map[string]*citationThread)
	}
	cache := c.citationCache[threadID]
	changed := cache == nil || cache.identity != identity || cache.path != path || cache.generation != generation
	if !changed && cache.file != nil {
		changed = file == nil || !os.SameFile(cache.file, file) || file.Size() < cache.file.Size() ||
			(file.Size() == cache.file.Size() && !file.ModTime().Equal(cache.file.ModTime())) ||
			(file.Size() > cache.file.Size() && !pageMetadataTailMatches(path, cache.file, cache.file.Size(), cache.tail))
	}
	if changed {
		cache = &citationThread{identity: identity, path: path, generation: generation, file: file, lookups: make(map[transcriptItemKey]*citationLookup)}
		if file != nil {
			cache.tail, _ = pageRolloutTail(path, file.Size())
		}
		c.citationCache[threadID] = cache
	}
	cache.used = time.Now()
	for len(c.citationCache) > 8 {
		oldest := ""
		for id, candidate := range c.citationCache {
			if id != threadID && (oldest == "" || candidate.used.Before(c.citationCache[oldest].used)) {
				oldest = id
			}
		}
		delete(c.citationCache, oldest)
	}
	lookup := cache.lookups[key]
	size := int64(0)
	if file != nil {
		size = file.Size()
	}
	if lookup == nil || lookup.signature != signature || (lookup.scan.done && len(lookup.scan.sources) < len(wanted) && size > lookup.size) {
		lookup = &citationLookup{signature: signature, wanted: maps.Clone(wanted), scan: citationScan{sources: make(map[string]CitationSource)}, size: size}
		cache.lookups[key] = lookup
	}
	lookup.used = time.Now()
	for len(cache.lookups) > 128 {
		var oldest transcriptItemKey
		for k, candidate := range cache.lookups {
			if k != key && (oldest.itemID == "" || candidate.used.Before(cache.lookups[oldest].used)) {
				oldest = k
			}
		}
		delete(cache.lookups, oldest)
	}
	if !lookup.scan.done && !lookup.running && !time.Now().Before(lookup.retry) {
		select {
		case citationWorkers <- struct{}{}:
			lookup.running = true
			state := lookup.scan
			state.sources = maps.Clone(state.sources)
			state.turns = slices.Clone(state.turns)
			go c.enrichCitations(threadID, cache, key, lookup, state, request)
		default:
		}
	}
	return lookup.scan.sources, !lookup.scan.done
}

func (c *Client) enrichCitations(threadID string, cache *citationThread, key transcriptItemKey, lookup *citationLookup, state citationScan, request pageRequestFunc) {
	defer func() { <-citationWorkers }()
	ctx, cancel := context.WithTimeout(c.pageContext, 30*time.Second)
	defer cancel()
	err := scanCitationHistory(ctx, threadID, key, lookup.wanted, &state, request, 8)
	c.citationMu.Lock()
	defer c.citationMu.Unlock()
	if c.citationCache[threadID] != cache || cache.lookups[key] != lookup {
		return
	}
	lookup.running = false
	if err != nil {
		// A native cursor may expire or a live turn may change. Retry from the
		// target, retaining successful sources but never certifying exhaustion.
		lookup.scan = citationScan{sources: state.sources}
		lookup.retry = time.Now().Add(2 * time.Second)
		return
	}
	lookup.scan = state
}

func scanCitationHistory(ctx context.Context, threadID string, target transcriptItemKey, wanted map[string]bool, state *citationScan, request pageRequestFunc, budget int) error {
	for used := 0; used < budget && !state.done; used++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(state.sources) == len(wanted) {
			state.done = true
			break
		}
		if state.currentTurn == "" {
			if state.turnIndex >= len(state.turns) {
				if state.started && state.turnCursor == "" {
					state.done = true
					break
				}
				params := map[string]any{"threadId": threadID, "limit": 32, "sortDirection": "desc", "itemsView": "notLoaded"}
				if state.turnCursor != "" {
					params["cursor"] = state.turnCursor
				}
				var page struct {
					Data       *[]map[string]any `json:"data"`
					NextCursor *string           `json:"nextCursor"`
				}
				if err := request(ctx, "thread/turns/list", params, &page); err != nil {
					return err
				}
				if page.Data == nil || len(*page.Data) > 32 {
					return errors.New("invalid citation turn page")
				}
				next := ""
				if page.NextCursor != nil {
					next = *page.NextCursor
					if next == "" || next == state.turnCursor || len(next) > 4096 || len(*page.Data) == 0 {
						return errors.New("citation turn cursor did not progress")
					}
				}
				state.turns = nil
				for _, turn := range *page.Data {
					id := stringValue(turn["id"])
					if id == "" {
						return errors.New("invalid citation turn")
					}
					state.turns = append(state.turns, id)
				}
				state.turnIndex, state.turnCursor, state.started = 0, next, true
				continue
			}
			id := state.turns[state.turnIndex]
			state.turnIndex++
			if !state.targetFound {
				if id != target.turnID {
					continue
				}
				state.targetFound = true
			}
			state.currentTurn, state.itemCursor = id, ""
			state.targetSeen = id != target.turnID
		}
		params := map[string]any{"threadId": threadID, "turnId": state.currentTurn, "limit": 32, "sortDirection": "desc"}
		if state.itemCursor != "" {
			params["cursor"] = state.itemCursor
		}
		var page struct {
			Data *[]struct {
				TurnID string         `json:"turnId"`
				Item   map[string]any `json:"item"`
			} `json:"data"`
			NextCursor *string `json:"nextCursor"`
		}
		if err := request(ctx, "thread/items/list", params, &page); err != nil {
			return err
		}
		if page.Data == nil || len(*page.Data) > 32 {
			return errors.New("invalid citation item page")
		}
		for _, raw := range *page.Data {
			if raw.TurnID != state.currentTurn || stringValue(raw.Item["id"]) == "" {
				return fmt.Errorf("citation item belongs to a different turn")
			}
			if !state.targetSeen {
				if stringValue(raw.Item["id"]) == target.itemID {
					state.targetSeen = true
				}
				continue
			}
			if stringValue(raw.Item["type"]) != "mcpToolCall" {
				continue
			}
			for ref, source := range citationSources(raw.Item["result"]) {
				if wanted[ref] {
					if _, found := state.sources[ref]; !found {
						state.sources[ref] = source
					}
				}
			}
		}
		if page.NextCursor != nil {
			next := *page.NextCursor
			if next == "" || next == state.itemCursor || len(next) > 4096 || len(*page.Data) == 0 {
				return errors.New("citation item cursor did not progress")
			}
			state.itemCursor = next
		} else {
			if !state.targetSeen {
				state.done = true
				break
			}
			state.currentTurn, state.itemCursor = "", ""
		}
	}
	if len(state.sources) == len(wanted) {
		state.done = true
	}
	return nil
}

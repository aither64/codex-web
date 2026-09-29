package codex

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"
)

var pageMetadataWorker = make(chan struct{}, 1)
var pageMetadataSlots = make(chan struct{}, 8)
var errPageMetadataExpired = errors.New("Codex rollout metadata expired")

const pageMetadataItems = 8192
const pageMetadataBytes = 1 << 20
const pageMetadataFreshness = time.Minute

func validPageMode(mode string) bool { return mode == "default" || mode == "plan" }

type pageMetadataCache struct {
	path              string
	lineage           string
	generation        uint64
	file              os.FileInfo
	offset            int64
	tail              []byte
	mode              string
	times             map[transcriptItemKey]itemTimestamps
	order             []transcriptItemKey
	bytes             int
	ready, pending    bool
	forceFull         bool
	fullReadStartedAt time.Time
	cancel            context.CancelFunc
	retryAt           time.Time
	lastUsed          time.Time
}

type pageContextReader struct {
	ctx    context.Context
	reader io.Reader
	count  *int64
}

func (reader pageContextReader) Read(buffer []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := reader.reader.Read(buffer)
	if reader.count != nil {
		*reader.count += int64(n)
	}
	return n, err
}

func (c *Client) pageMetadataNow() time.Time {
	if c.pageClock != nil {
		return c.pageClock()
	}
	return time.Now()
}

func pageMetadataTailMatches(path string, expected os.FileInfo, size int64, tail []byte) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || !os.SameFile(info, expected) || info.Size() < size {
		return false
	}
	if len(tail) == 0 {
		return true
	}
	probe := make([]byte, len(tail))
	_, err = file.ReadAt(probe, size-int64(len(probe)))
	return err == nil && bytes.Equal(probe, tail)
}

func (c *Client) schedulePageMetadata(threadID string, cache *pageMetadataCache, now time.Time) {
	if cache.pending || now.Before(cache.retryAt) {
		return
	}
	select {
	case pageMetadataSlots <- struct{}{}:
		ctx, cancel := context.WithTimeout(c.pageContext, 30*time.Second)
		cache.pending, cache.cancel = true, cancel
		go func() {
			defer func() { <-pageMetadataSlots }()
			defer cancel()
			c.enrichPageMetadata(ctx, threadID, cache)
		}()
	default:
		cache.retryAt = now.Add(time.Second)
	}
}

func (c *Client) pageMetadataSnapshot(threadID, path, lineage string, generation uint64, file os.FileInfo, entries []TranscriptEntry) (string, map[transcriptItemKey]itemTimestamps, bool) {
	if file == nil || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", nil, true
	}
	c.pageMu.Lock()
	now := c.pageMetadataNow()
	cache := c.pageMetadata[threadID]
	if cache == nil || cache.path != path || cache.lineage != lineage || cache.generation != generation ||
		(cache.ready && (cache.file == nil || !os.SameFile(cache.file, file))) {
		if cache != nil && cache.cancel != nil {
			cache.cancel()
		}
		cache = &pageMetadataCache{path: path, lineage: lineage, generation: generation, forceFull: true, times: make(map[transcriptItemKey]itemTimestamps)}
		c.pageMetadata[threadID] = cache
	}
	cache.lastUsed = now
	if cache.ready && (file.Size() < cache.file.Size() || file.Size() == cache.file.Size() && !file.ModTime().Equal(cache.file.ModTime()) ||
		file.Size() > cache.file.Size() && !pageMetadataTailMatches(path, file, cache.file.Size(), cache.tail) ||
		(cache.fullReadStartedAt.IsZero() || !now.Before(cache.fullReadStartedAt.Add(pageMetadataFreshness)))) {
		if cache.cancel != nil {
			cache.cancel()
		}
		cache = &pageMetadataCache{path: path, lineage: lineage, generation: generation, forceFull: true, times: make(map[transcriptItemKey]itemTimestamps), lastUsed: now}
		c.pageMetadata[threadID] = cache
	}
	changed := !cache.ready || file.Size() != cache.file.Size() || !file.ModTime().Equal(cache.file.ModTime())
	if changed {
		c.schedulePageMetadata(threadID, cache, now)
	}
	mode, pending := cache.mode, changed || cache.pending
	times := make(map[transcriptItemKey]itemTimestamps, len(entries))
	for _, entry := range entries {
		key := transcriptItemKey{entry.TurnID, entry.ItemID}
		if value, ok := cache.times[key]; ok {
			times[key] = value
		}
	}
	if len(c.pageMetadata) > 8 {
		oldestID := ""
		var oldest time.Time
		for id, item := range c.pageMetadata {
			if id == threadID {
				continue
			}
			if oldestID == "" || item.lastUsed.Before(oldest) {
				oldestID, oldest = id, item.lastUsed
			}
		}
		if oldestID != "" {
			if stale := c.pageMetadata[oldestID]; stale.cancel != nil {
				stale.cancel()
			}
			delete(c.pageMetadata, oldestID)
		}
	}
	c.pageMu.Unlock()
	return mode, times, pending
}

func (c *Client) enrichPageMetadata(ctx context.Context, threadID string, cache *pageMetadataCache) {
	select {
	case pageMetadataWorker <- struct{}{}:
		defer func() { <-pageMetadataWorker }()
	case <-ctx.Done():
		c.finishPageMetadata(threadID, cache, nil, ctx.Err(), 0, 0, false)
		return
	}
	started := c.pageMetadataNow()
	var scanned int64
	var rebuilt bool
	result, err := scanPageMetadataAt(ctx, cache, started, &scanned, &rebuilt)
	duration := c.pageMetadataNow().Sub(started)
	if duration < 0 {
		duration = 0
	}
	c.finishPageMetadata(threadID, cache, result, err, scanned, duration, rebuilt)
}

func (c *Client) finishPageMetadata(threadID string, old, result *pageMetadataCache, err error, scanned int64, duration time.Duration, rebuilt bool) {
	if result != nil && err == nil {
		err = verifyPageMetadataFile(result)
	}
	generation := c.pageGeneration()
	c.pageMu.Lock()
	if rebuilt {
		c.pageRebuildCount++
		c.pageRebuildBytes += uint64(scanned)
		c.pageRebuildTime += duration
	}
	if c.pageMetadata[threadID] != old {
		c.pageMu.Unlock()
		return
	}
	now := c.pageMetadataNow()
	if generation != old.generation {
		delete(c.pageMetadata, threadID)
		c.pageMu.Unlock()
		return
	}
	if err == nil && (result == nil || result.fullReadStartedAt.IsZero() || !now.Before(result.fullReadStartedAt.Add(pageMetadataFreshness))) {
		err = errPageMetadataExpired
	}
	if err != nil {
		fresh := &pageMetadataCache{path: old.path, lineage: old.lineage, generation: old.generation, forceFull: true,
			times: make(map[transcriptItemKey]itemTimestamps), lastUsed: old.lastUsed}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			fresh.retryAt = now.Add(30 * time.Second)
		} else if errors.Is(err, errPageMetadataExpired) {
			c.schedulePageMetadata(threadID, fresh, now)
		} else {
			fresh.retryAt = now.Add(30 * time.Second)
		}
		c.pageMetadata[threadID] = fresh
		c.pageMu.Unlock()
		return
	}
	result.lastUsed = old.lastUsed
	c.pageMetadata[threadID] = result
	c.pageMu.Unlock()
	if c.pageGeneration() == result.generation {
		c.broadcast(threadID)
	}
}

func scanPageMetadata(ctx context.Context, old *pageMetadataCache) (*pageMetadataCache, error) {
	return scanPageMetadataAt(ctx, old, time.Now(), nil, nil)
}

func scanPageMetadataAt(ctx context.Context, old *pageMetadataCache, startedAt time.Time, scanned *int64, rebuilt *bool) (*pageMetadataCache, error) {
	file, err := os.Open(old.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("invalid Codex rollout")
	}
	fresh := old.forceFull || !old.ready || old.fullReadStartedAt.IsZero() || !startedAt.Before(old.fullReadStartedAt.Add(pageMetadataFreshness))
	if rebuilt != nil {
		*rebuilt = fresh
	}
	result := &pageMetadataCache{path: old.path, lineage: old.lineage, generation: old.generation, file: info, ready: true,
		times: make(map[transcriptItemKey]itemTimestamps)}
	if fresh {
		result.fullReadStartedAt = startedAt
	} else {
		result.fullReadStartedAt = old.fullReadStartedAt
		result.mode, result.order, result.bytes = old.mode, append([]transcriptItemKey(nil), old.order...), old.bytes
		for key, value := range old.times {
			result.times[key] = value
		}
	}
	start := int64(0)
	if !fresh && old.ready && old.file != nil && os.SameFile(old.file, info) && info.Size() >= old.file.Size() &&
		(info.Size() != old.file.Size() || info.ModTime().Equal(old.file.ModTime())) {
		if len(old.tail) > 0 {
			probe := make([]byte, len(old.tail))
			n, err := file.ReadAt(probe, old.file.Size()-int64(len(probe)))
			if scanned != nil {
				*scanned += int64(n)
			}
			if err == nil && bytes.Equal(probe, old.tail) {
				start = old.offset
			} else {
				result.mode, result.times, result.order, result.bytes = "", make(map[transcriptItemKey]itemTimestamps), nil, 0
				result.fullReadStartedAt = startedAt
				fresh = true
				if rebuilt != nil {
					*rebuilt = true
				}
			}
		} else {
			start = old.offset
		}
	} else {
		result.mode, result.times, result.order, result.bytes = "", make(map[transcriptItemKey]itemTimestamps), nil, 0
		result.fullReadStartedAt = startedAt
		fresh = true
		if rebuilt != nil {
			*rebuilt = true
		}
	}
	if start == 0 && info.Size() > readLimit {
		start = info.Size() - readLimit
	}
	if info.Size()-start > readLimit {
		result.mode, result.times, result.order, result.bytes = "", make(map[transcriptItemKey]itemTimestamps), nil, 0
		result.fullReadStartedAt = startedAt
		fresh = true
		if rebuilt != nil {
			*rebuilt = true
		}
		start = info.Size() - readLimit
	}
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return nil, err
	}
	reader := bufio.NewReader(pageContextReader{ctx: ctx, reader: io.LimitReader(file, info.Size()-start), count: scanned})
	if start > 0 && !(old.ready && !fresh && start == old.offset) {
		previous := []byte{0}
		n, err := file.ReadAt(previous, start-1)
		if scanned != nil {
			*scanned += int64(n)
		}
		if err != nil {
			return nil, err
		}
		if previous[0] != '\n' {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				return nil, err
			}
			start += int64(len(line))
		}
	}
	result.offset = start
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			if len(line) >= readLimit {
				return nil, errors.New("Codex rollout record exceeds limit")
			}
			break
		}
		if err != nil {
			return nil, err
		}
		if len(line) > readLimit {
			return nil, errors.New("Codex rollout record exceeds limit")
		}
		result.offset += int64(len(line))
		var record struct {
			Type, Timestamp string
			Payload         struct {
				Type          string `json:"type"`
				TurnID        string `json:"turn_id"`
				StartedAtMS   int64  `json:"started_at_ms"`
				CompletedAtMS int64  `json:"completed_at_ms"`
				Item          struct {
					ID string `json:"id"`
				} `json:"item"`
				CollaborationMode struct {
					Mode string `json:"mode"`
				} `json:"collaboration_mode"`
				ThreadSettings struct {
					CollaborationMode struct {
						Mode string `json:"mode"`
					} `json:"collaboration_mode"`
				} `json:"thread_settings"`
			} `json:"payload"`
		}
		if json.Unmarshal(line, &record) != nil {
			continue
		}
		mode := ""
		if record.Type == "turn_context" {
			mode = record.Payload.CollaborationMode.Mode
		}
		if record.Type == "event_msg" && record.Payload.Type == "thread_settings_applied" {
			mode = record.Payload.ThreadSettings.CollaborationMode.Mode
		}
		if validPageMode(mode) {
			result.mode = mode
		}
		if record.Type == "event_msg" && record.Payload.Type == "item_completed" && record.Payload.TurnID != "" && record.Payload.Item.ID != "" &&
			len(record.Payload.TurnID) <= 256 && len(record.Payload.Item.ID) <= 256 {
			key := transcriptItemKey{record.Payload.TurnID, record.Payload.Item.ID}
			stamp := record.Timestamp
			if len(stamp) > 128 {
				stamp = ""
			}
			if oldValue, exists := result.times[key]; exists {
				result.bytes -= len(key.turnID) + len(key.itemID) + len(oldValue.recordedAt) + 128
			} else {
				result.order = append(result.order, key)
			}
			result.times[key] = itemTimestamps{startedAtMS: record.Payload.StartedAtMS, completedAtMS: record.Payload.CompletedAtMS, recordedAt: stamp}
			result.bytes += len(key.turnID) + len(key.itemID) + len(stamp) + 128
			for len(result.order) > pageMetadataItems || result.bytes > pageMetadataBytes {
				removed := result.order[0]
				value := result.times[removed]
				result.bytes -= len(removed.turnID) + len(removed.itemID) + len(value.recordedAt) + 128
				delete(result.times, removed)
				result.order = result.order[1:]
			}
		}
	}
	probeSize := int64(4096)
	if info.Size() < probeSize {
		probeSize = info.Size()
	}
	result.tail = make([]byte, probeSize)
	if probeSize > 0 {
		n, err := file.ReadAt(result.tail, info.Size()-probeSize)
		if scanned != nil {
			*scanned += int64(n)
		}
		if err != nil {
			return nil, err
		}
	}
	if err := verifyPageMetadataOpened(file, info); err != nil {
		return nil, err
	}
	return result, nil
}

func verifyPageMetadataOpened(file *os.File, captured os.FileInfo) error {
	current, err := file.Stat()
	if err != nil || !current.Mode().IsRegular() || !os.SameFile(captured, current) || current.Size() < captured.Size() ||
		current.Size() == captured.Size() && !current.ModTime().Equal(captured.ModTime()) {
		return errors.New("Codex rollout changed during metadata scan")
	}
	return nil
}

func verifyPageMetadataFile(result *pageMetadataCache) error {
	file, err := os.Open(result.path)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := verifyPageMetadataOpened(file, result.file); err != nil {
		return err
	}
	if !pageMetadataTailMatches(result.path, result.file, result.file.Size(), result.tail) {
		return errors.New("Codex rollout changed during metadata scan")
	}
	return nil
}

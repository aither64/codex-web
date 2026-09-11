package codex

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type transcriptItemKey struct {
	turnID string
	itemID string
}

type itemTimestamps struct {
	startedAtMS   int64
	completedAtMS int64
	recordedAt    string
}

type rolloutMetadata struct {
	mode      string
	itemTimes map[transcriptItemKey]itemTimestamps
}

// Keep live observations only for the latest turn of a watched thread. History
// timestamps come from the existing rollout tail read, without a separate cache.
type liveTurnTimestamps struct {
	turnID string
	items  map[string]itemTimestamps
}

func (c *Client) observeItemTimestamp(method string, params json.RawMessage) {
	var event struct {
		ThreadID string `json:"threadId"`
		TurnID   string `json:"turnId"`
		Item     struct {
			ID string `json:"id"`
		} `json:"item"`
		StartedAtMS   int64 `json:"startedAtMs"`
		CompletedAtMS int64 `json:"completedAtMs"`
	}
	if json.Unmarshal(params, &event) != nil || event.TurnID == "" || event.Item.ID == "" {
		return
	}
	c.watchedMu.Lock()
	defer c.watchedMu.Unlock()
	if c.watched[event.ThreadID] == 0 {
		return
	}
	turn := c.liveItemTimes[event.ThreadID]
	if turn == nil || turn.turnID != event.TurnID {
		turn = &liveTurnTimestamps{turnID: event.TurnID, items: make(map[string]itemTimestamps)}
		c.liveItemTimes[event.ThreadID] = turn
	}
	timing := turn.items[event.Item.ID]
	if method == "item/started" && event.StartedAtMS > 0 {
		timing.startedAtMS = event.StartedAtMS
	} else if method == "item/completed" && event.CompletedAtMS > 0 {
		timing.completedAtMS = event.CompletedAtMS
	}
	turn.items[event.Item.ID] = timing
}

func (c *Client) applyTranscriptTimestamps(
	threadID string, entries []TranscriptEntry, persisted map[transcriptItemKey]itemTimestamps,
) {
	c.watchedMu.Lock()
	defer c.watchedMu.Unlock()
	live := c.liveItemTimes[threadID]
	for index := range entries {
		entry := &entries[index]
		timing := persisted[transcriptItemKey{entry.TurnID, entry.ItemID}]
		if live != nil && live.turnID == entry.TurnID {
			observed := live.items[entry.ItemID]
			if timing.startedAtMS <= 0 {
				timing.startedAtMS = observed.startedAtMS
			}
			if timing.completedAtMS <= 0 {
				timing.completedAtMS = observed.completedAtMS
			}
			if saved := persisted[transcriptItemKey{entry.TurnID, entry.ItemID}]; saved.timestamp() != "" && saved.timestamp() == timing.timestamp() {
				delete(live.items, entry.ItemID)
			}
		}
		if timestamp := timing.timestamp(); timestamp != "" {
			entry.Timestamp = timestamp
			entry.TimestampApproximate = false
		}
	}
}

func (timing itemTimestamps) timestamp() string {
	if timing.startedAtMS > 0 {
		return time.UnixMilli(timing.startedAtMS).UTC().Format(time.RFC3339Nano)
	}
	if timing.completedAtMS > 0 {
		return time.UnixMilli(timing.completedAtMS).UTC().Format(time.RFC3339Nano)
	}
	if value, err := time.Parse(time.RFC3339Nano, timing.recordedAt); err == nil {
		return value.UTC().Format(time.RFC3339Nano)
	}
	return ""
}

func applyTurnTimestamps(entries []TranscriptEntry, turn map[string]any) {
	startedAt, _ := turn["startedAt"].(float64)
	completedAt, _ := turn["completedAt"].(float64)
	for index := range entries {
		entry := &entries[index]
		seconds := startedAt
		if seconds <= 0 {
			seconds = completedAt
		}
		if entry.Kind == "error" && entry.ItemID == "" && completedAt > 0 {
			seconds = completedAt
		}
		if seconds <= 0 || seconds != float64(int64(seconds)) {
			continue
		}
		entry.Timestamp = time.Unix(int64(seconds), 0).UTC().Format(time.RFC3339Nano)
		entry.TimestampApproximate = true
	}
}

func collaborationModeFromRollout(path string) (string, error) {
	metadata, err := readRolloutMetadata(path, nil)
	if err != nil {
		return "", err
	}
	if metadata.mode == "" {
		return "", errors.New("Codex thread rollout has no collaboration mode")
	}
	return metadata.mode, nil
}

// Read the same bounded suffix used for collaboration mode. Only collect times
// for items already returned by App Server; inherited or older items can use
// approximate turn times when their records are outside this suffix.
func readRolloutMetadata(path string, wanted map[transcriptItemKey]bool) (rolloutMetadata, error) {
	result := rolloutMetadata{itemTimes: make(map[transcriptItemKey]itemTimestamps)}
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return result, errors.New("thread/read returned an invalid rollout path")
	}
	file, err := os.Open(path)
	if err != nil {
		return result, fmt.Errorf("open Codex thread rollout: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return result, fmt.Errorf("inspect Codex thread rollout: %w", err)
	}
	if !info.Mode().IsRegular() {
		return result, errors.New("Codex thread rollout is not a regular file")
	}
	offset := int64(0)
	if info.Size() > readLimit {
		offset = info.Size() - readLimit
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			return result, fmt.Errorf("seek Codex thread rollout: %w", err)
		}
	}
	reader := bufio.NewReader(io.LimitReader(file, info.Size()-offset))
	if offset > 0 {
		for {
			_, err := reader.ReadSlice('\n')
			if err == nil {
				break
			}
			if errors.Is(err, bufio.ErrBufferFull) {
				continue
			}
			if errors.Is(err, io.EOF) {
				return result, errors.New("Codex thread rollout tail has no complete record")
			}
			return result, fmt.Errorf("read Codex thread rollout tail: %w", err)
		}
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), readLimit)
	for scanner.Scan() {
		var entry struct {
			Type      string `json:"type"`
			Timestamp string `json:"timestamp"`
			Payload   struct {
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
		if json.Unmarshal(scanner.Bytes(), &entry) != nil {
			continue
		}
		candidate := ""
		if entry.Type == "turn_context" {
			candidate = entry.Payload.CollaborationMode.Mode
		} else if entry.Type == "event_msg" && entry.Payload.Type == "thread_settings_applied" {
			candidate = entry.Payload.ThreadSettings.CollaborationMode.Mode
		}
		if candidate != "" && len(candidate) <= 1024 {
			result.mode = candidate
		}
		if entry.Type == "event_msg" && entry.Payload.Type == "item_completed" {
			key := transcriptItemKey{entry.Payload.TurnID, entry.Payload.Item.ID}
			if wanted[key] {
				result.itemTimes[key] = itemTimestamps{
					startedAtMS: entry.Payload.StartedAtMS, completedAtMS: entry.Payload.CompletedAtMS,
					recordedAt: entry.Timestamp,
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return result, fmt.Errorf("scan Codex thread rollout: %w", err)
	}
	return result, nil
}

package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

type activityLineageCache struct {
	header string
	file   os.FileInfo
	tail   []byte
	ids    map[string]bool
}

type activityLineageJob struct {
	header string
	file   os.FileInfo
	done   chan struct{}
	cancel context.CancelFunc
	result *activityLineageCache
	err    error
}

func (c *Client) startActivityLineage(path, root string, cutoff uint64, header string, info os.FileInfo) *activityLineageJob {
	select {
	case pageMetadataSlots <- struct{}{}:
	default:
		return nil
	}
	ctx, cancel := context.WithCancel(c.pageContext)
	job := &activityLineageJob{header: header, file: info, done: make(chan struct{}), cancel: cancel}
	go func() {
		defer close(job.done)
		defer cancel()
		defer func() { <-pageMetadataSlots }()
		select {
		case pageMetadataWorker <- struct{}{}:
			defer func() { <-pageMetadataWorker }()
		case <-ctx.Done():
			job.err = ctx.Err()
			return
		}
		saved := &activityLineageCache{header: header, file: info, ids: map[string]bool{}}
		job.err = scanOwnActivityTurns(ctx, path, root, cutoff, info.Size(), 0, map[string]bool{}, saved.ids)
		if job.err == nil {
			saved.tail, job.err = pageRolloutTail(path, info.Size())
		}
		if job.err == nil {
			job.result = saved
		}
	}()
	return job
}

func (h *activityHistory) lineagePending() bool {
	if h.lineageJob == nil {
		return h.lineage == nil
	}
	select {
	case <-h.lineageJob.done:
		return false
	default:
		return true
	}
}

// The paged activity gate owns this cache. Immutable parent segments are
// checked once; ordinary appends scan only the new suffix of this rollout.
func (c *Client) ownPagedActivityTurns(ctx context.Context, metadata map[string]any, turns []TurnMetadata, history *activityHistory) ([]TurnMetadata, string) {
	if stringValue(metadata["forkedFromId"]) == "" {
		return turns, "thread"
	}
	path := stringValue(metadata["path"])
	file, meta, err := openActivityRollout(path)
	if err != nil {
		return nil, "unknown"
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || meta.ID != stringValue(metadata["id"]) || meta.ParentID != stringValue(metadata["forkedFromId"]) {
		return nil, "unknown"
	}
	cutoff := meta.Cutoff
	if cutoff == nil && meta.Base != nil {
		match := rolloutIdentityPattern.FindStringSubmatch(filepath.Base(path))
		if meta.Base.RolloutID == meta.ParentID || len(match) == 2 && match[1] == meta.ID {
			cutoff = &meta.Base.EndOrdinal
		}
	}
	if cutoff == nil {
		return nil, "unknown"
	}
	header, _ := json.Marshal(meta)
	saved := history.lineage
	reusable := saved != nil && saved.header == string(header) && saved.file != nil && os.SameFile(info, saved.file) && info.Size() >= saved.file.Size() && pageMetadataTailMatches(path, saved.file, saved.file.Size(), saved.tail)
	if !reusable {
		root := filepath.Dir(path)
		for filepath.Base(root) != "sessions" && filepath.Base(root) != "archived_sessions" && filepath.Dir(root) != root {
			root = filepath.Dir(root)
		}
		if filepath.Dir(root) == root {
			return nil, "unknown"
		}
		job := history.lineageJob
		if job != nil && (job.header != string(header) || !os.SameFile(info, job.file) || info.Size() < job.file.Size()) {
			job.cancel()
			job = nil
			history.lineageJob = nil
		}
		if job == nil {
			job = c.startActivityLineage(path, filepath.Dir(root), *cutoff, string(header), info)
			history.lineageJob = job
		}
		if job == nil {
			return nil, "unknown"
		}
		select {
		case <-job.done:
		default:
			return nil, "unknown"
		}
		if job.err != nil {
			if errors.Is(job.err, context.Canceled) || info.Size() != job.file.Size() || !info.ModTime().Equal(job.file.ModTime()) {
				history.lineageJob = nil
			}
			return nil, "unknown"
		}
		saved = job.result
		history.lineage = saved
		history.lineageJob = nil
		if !pageMetadataTailMatches(path, saved.file, saved.file.Size(), saved.tail) {
			history.lineage = nil
			return nil, "unknown"
		}
	}
	if info.Size() > saved.file.Size() {
		if _, err = file.Seek(saved.file.Size(), io.SeekStart); err != nil {
			return nil, "unknown"
		}
		scanner := bufio.NewScanner(io.LimitReader(pageContextReader{ctx: ctx, reader: file}, info.Size()-saved.file.Size()))
		scanner.Buffer(make([]byte, 64*1024), readLimit)
		for scanner.Scan() {
			if ctx.Err() != nil {
				return nil, "unknown"
			}
			var record struct {
				Ordinal *uint64 `json:"ordinal"`
				Type    string  `json:"type"`
				Payload struct {
					Type string `json:"type"`
					Turn string `json:"turn_id"`
				} `json:"payload"`
			}
			if json.Unmarshal(scanner.Bytes(), &record) != nil || record.Ordinal == nil {
				return nil, "unknown"
			}
			if *record.Ordinal >= *cutoff && record.Type == "event_msg" && record.Payload.Type == "task_started" && record.Payload.Turn != "" {
				saved.ids[record.Payload.Turn] = true
			}
		}
		if scanner.Err() != nil {
			return nil, "unknown"
		}
	}
	tail, err := pageRolloutTail(path, info.Size())
	if err != nil {
		return nil, "unknown"
	}
	saved.file, saved.tail = info, tail
	history.lineage = saved
	result := make([]TurnMetadata, 0, len(turns))
	for _, turn := range turns {
		if saved.ids[turn.ID] {
			result = append(result, turn)
		}
	}
	return result, "sinceFork"
}

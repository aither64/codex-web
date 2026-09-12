package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"golang.org/x/sys/unix"
)

var rolloutIdentityPattern = regexp.MustCompile(`-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.jsonl$`)

type activityHistoryBase struct {
	RolloutID     string `json:"thread_id"`
	EndOrdinal    uint64 `json:"end_ordinal_exclusive"`
	EndByteOffset int64  `json:"end_byte_offset"`
}

type activityRolloutMeta struct {
	ID       string               `json:"id"`
	ParentID string               `json:"forked_from_id"`
	Cutoff   *uint64              `json:"forked_from_ordinal_exclusive"`
	Base     *activityHistoryBase `json:"history_base"`
}

func (c *Client) ownActivityTurns(ctx context.Context, metadata map[string]any, turns []TurnMetadata) ([]TurnMetadata, string) {
	if stringValue(metadata["forkedFromId"]) == "" {
		return turns, "thread"
	}
	path := stringValue(metadata["path"])
	file, meta, err := openActivityRollout(path)
	if err != nil {
		return nil, "unknown"
	}
	file.Close()
	if meta.ID != stringValue(metadata["id"]) || meta.ParentID != stringValue(metadata["forkedFromId"]) {
		return nil, "unknown"
	}
	cutoff := meta.Cutoff
	if cutoff == nil && meta.Base != nil {
		match := rolloutIdentityPattern.FindStringSubmatch(filepath.Base(path))
		// Match Codex's legacy rule. A revert's physical base may point to a
		// different boundary from the logical fork parent.
		if meta.Base.RolloutID == meta.ParentID || len(match) == 2 && match[1] == meta.ID {
			cutoff = &meta.Base.EndOrdinal
		}
	}
	if cutoff == nil {
		return nil, "unknown"
	}
	root := filepath.Dir(path)
	for filepath.Base(root) != "sessions" && filepath.Base(root) != "archived_sessions" && filepath.Dir(root) != root {
		root = filepath.Dir(root)
	}
	if filepath.Dir(root) == root {
		return nil, "unknown"
	}
	root = filepath.Dir(root)
	ids := map[string]bool{}
	if err := scanOwnActivityTurns(ctx, path, root, *cutoff, 0, 0, map[string]bool{}, ids); err != nil {
		return nil, "unknown"
	}
	result := make([]TurnMetadata, 0, len(turns))
	for _, turn := range turns {
		if ids[turn.ID] {
			result = append(result, turn)
		}
	}
	return result, "sinceFork"
}

func openActivityRollout(path string) (*os.File, activityRolloutMeta, error) {
	var meta activityRolloutMeta
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, meta, errors.New("invalid activity rollout path")
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, meta, err
	}
	file := os.NewFile(uintptr(fd), path)
	fail := func(err error) (*os.File, activityRolloutMeta, error) { file.Close(); return nil, meta, err }
	info, err := file.Stat()
	if err != nil {
		return fail(err)
	}
	if !info.Mode().IsRegular() {
		return fail(errors.New("activity rollout is not a regular file"))
	}
	scanner := bufio.NewScanner(io.LimitReader(file, readLimit))
	scanner.Buffer(make([]byte, 64*1024), readLimit)
	if !scanner.Scan() {
		return fail(errors.New("activity rollout has no metadata"))
	}
	var header struct {
		Type    string              `json:"type"`
		Payload activityRolloutMeta `json:"payload"`
	}
	if json.Unmarshal(scanner.Bytes(), &header) != nil || header.Type != "session_meta" || header.Payload.ID == "" {
		return fail(errors.New("invalid activity rollout metadata"))
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fail(err)
	}
	return file, header.Payload, nil
}

func scanOwnActivityTurns(ctx context.Context, path, root string, cutoff uint64, limit int64, depth int, seen map[string]bool, ids map[string]bool) error {
	if depth > 32 || seen[path] {
		return errors.New("activity lineage repeats or is too deep")
	}
	seen[path] = true
	file, meta, err := openActivityRollout(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if limit == 0 {
		limit = info.Size()
	}
	if limit < 0 || limit > info.Size() || limit > readLimit {
		return errors.New("activity lineage exceeds available bounded history")
	}
	if meta.Base != nil && meta.Base.EndOrdinal > cutoff {
		basePath, err := findActivityRollout(ctx, root, meta.Base.RolloutID)
		if err != nil {
			return err
		}
		if meta.Base.EndByteOffset <= 0 {
			return errors.New("invalid activity lineage byte boundary")
		}
		if err := scanOwnActivityTurns(ctx, basePath, root, cutoff, meta.Base.EndByteOffset, depth+1, seen, ids); err != nil {
			return err
		}
	}
	scanner := bufio.NewScanner(io.LimitReader(file, limit))
	scanner.Buffer(make([]byte, 64*1024), readLimit)
	first := true
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var record struct {
			Ordinal *uint64 `json:"ordinal"`
			Type    string  `json:"type"`
			Payload struct {
				Type   string `json:"type"`
				TurnID string `json:"turn_id"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return err
		}
		if first {
			first = false
			continue
		}
		if record.Ordinal == nil {
			return errors.New("activity lineage lacks ordinal identity")
		}
		if *record.Ordinal < cutoff {
			continue
		}
		if record.Type == "event_msg" && record.Payload.Type == "task_started" && record.Payload.TurnID != "" {
			ids[record.Payload.TurnID] = true
		}
	}
	return scanner.Err()
}

func findActivityRollout(ctx context.Context, root, id string) (string, error) {
	if !rolloutIdentityPattern.MatchString("-" + id + ".jsonl") {
		return "", errors.New("invalid activity lineage rollout identity")
	}
	match := ""
	visited := 0
	for _, directory := range []string{"sessions", "archived_sessions"} {
		err := filepath.WalkDir(filepath.Join(root, directory), func(path string, entry fs.DirEntry, err error) error {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			visited++
			if visited > 100000 {
				return errors.New("activity lineage discovery exceeds limit")
			}
			if entry.Type()&os.ModeSymlink != 0 {
				return nil
			}
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), "-"+id+".jsonl") {
				if match != "" {
					return errors.New("ambiguous activity lineage rollout")
				}
				match = path
			}
			return nil
		})
		if err != nil {
			return "", err
		}
	}
	if match == "" {
		return "", errors.New("activity lineage rollout is unavailable")
	}
	return match, nil
}

package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// HeadlessThreadMaterialized verifies an application-owned member thread. A
// fresh thread has SQLite metadata but no rollout until its first history item.
func (c *Client) HeadlessThreadMaterialized(ctx context.Context, threadID, cwd, projectID string) (bool, error) {
	return c.headlessThreadMaterialized(ctx, threadID, cwd, projectID, "")
}

// ForkedHeadlessThreadMaterialized binds a fork destination to its exact source.
func (c *Client) ForkedHeadlessThreadMaterialized(ctx context.Context, threadID, cwd, sourceID string) (bool, error) {
	return c.headlessThreadMaterialized(ctx, threadID, cwd, "", sourceID)
}

func (c *Client) headlessThreadMaterialized(ctx context.Context, threadID, cwd, projectID, sourceID string) (bool, error) {
	if (projectID == "") == (sourceID == "") {
		return false, errors.New("headless Codex thread requires exactly one expected origin")
	}
	thread, err := c.ReadThreadMetadata(ctx, threadID, false)
	if err != nil {
		return false, err
	}
	if thread.Cwd != cwd || (projectID != "" && (thread.ProjectID == nil || *thread.ProjectID != projectID || thread.ForkedFromID != "")) ||
		(sourceID != "" && thread.ForkedFromID != sourceID) || !c.threadSource(thread.Source) ||
		thread.Ephemeral == nil || *thread.Ephemeral || thread.HistoryMode != "paginated" {
		return false, errors.New("headless Codex thread has the wrong identity")
	}
	if thread.Path == nil || *thread.Path == "" || !filepath.IsAbs(*thread.Path) || filepath.Clean(*thread.Path) != *thread.Path {
		return false, errors.New("headless Codex thread has an invalid rollout path")
	}
	info, err := os.Stat(*thread.Path)
	if err == nil {
		if !info.Mode().IsRegular() {
			return false, errors.New("headless Codex thread rollout is not a regular file")
		}
		return true, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("inspect headless Codex rollout: %w", err)
	}
	if thread.Preview != "" || thread.Turns == nil || len(*thread.Turns) != 0 ||
		!slices.Contains([]string{"idle", "notLoaded"}, statusValue(thread.Status)) {
		return false, errors.New("missing headless Codex rollout is not a fresh idle thread")
	}
	return false, nil
}

// BootstrapHeadlessThread writes a single internal, non-assignment history
// item. The caller must durably reserve this attempt before calling: after an
// uncertain response, only VerifyHeadlessBootstrap may be retried.
func (c *Client) BootstrapHeadlessThread(ctx context.Context, threadID, cwd, projectID, marker string) error {
	return c.bootstrapHeadlessThread(ctx, threadID, cwd, projectID, "", marker)
}

func (c *Client) BootstrapForkedHeadlessThread(ctx context.Context, threadID, cwd, sourceID, marker string) error {
	return c.bootstrapHeadlessThread(ctx, threadID, cwd, "", sourceID, marker)
}

func (c *Client) bootstrapHeadlessThread(ctx context.Context, threadID, cwd, projectID, sourceID, marker string) error {
	if marker == "" || len(marker) > 4096 || !strings.Contains(marker, threadID) {
		return errors.New("headless bootstrap marker must identify the thread")
	}
	materialized, err := c.headlessThreadMaterialized(ctx, threadID, cwd, projectID, sourceID)
	if err != nil {
		return err
	}
	if materialized && sourceID == "" {
		return c.verifyHeadlessBootstrap(ctx, threadID, cwd, projectID, sourceID, marker)
	}
	item := map[string]any{"type": "message", "role": "developer", "content": []map[string]any{{"type": "input_text", "text": marker}}}
	err = c.Request(ctx, "thread/inject_items", map[string]any{"threadId": threadID, "items": []any{item}}, nil)
	if verifyErr := c.verifyHeadlessBootstrap(ctx, threadID, cwd, projectID, sourceID, marker); verifyErr == nil {
		return nil
	} else if err != nil {
		return fmt.Errorf("headless bootstrap outcome is uncertain: %w; verification: %v", err, verifyErr)
	} else {
		return fmt.Errorf("headless bootstrap was not durably recorded: %w", verifyErr)
	}
}

// VerifyHeadlessBootstrap never sends another injection. It is safe after a
// lost response because it checks the exact marker in the exact thread rollout.
func (c *Client) VerifyHeadlessBootstrap(ctx context.Context, threadID, cwd, projectID, marker string) error {
	return c.verifyHeadlessBootstrap(ctx, threadID, cwd, projectID, "", marker)
}

func (c *Client) VerifyForkedHeadlessBootstrap(ctx context.Context, threadID, cwd, sourceID, marker string) error {
	return c.verifyHeadlessBootstrap(ctx, threadID, cwd, "", sourceID, marker)
}

func (c *Client) verifyHeadlessBootstrap(ctx context.Context, threadID, cwd, projectID, sourceID, marker string) error {
	materialized, err := c.headlessThreadMaterialized(ctx, threadID, cwd, projectID, sourceID)
	if err != nil {
		return err
	}
	if !materialized {
		return errors.New("headless Codex thread has no persisted rollout")
	}
	thread, err := c.ReadThreadMetadata(ctx, threadID, true)
	if err != nil {
		return err
	}
	if thread.Cwd != cwd || (projectID != "" && (thread.ProjectID == nil || *thread.ProjectID != projectID)) ||
		(sourceID != "" && thread.ForkedFromID != sourceID) || thread.Path == nil {
		return errors.New("headless Codex thread identity changed")
	}
	return verifyHeadlessBootstrapRollout(*thread.Path, threadID, marker)
}

func verifyHeadlessBootstrapRollout(path, threadID, marker string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("headless Codex rollout is not a regular file")
	}
	scanner := bufio.NewScanner(io.LimitReader(file, info.Size()))
	scanner.Buffer(make([]byte, 64*1024), readLimit)
	metaCount, markerCount := 0, 0
	for scanner.Scan() {
		var entry struct {
			Type    string `json:"type"`
			Payload struct {
				ID      string `json:"id"`
				Type    string `json:"type"`
				Role    string `json:"role"`
				Content []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"content"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return fmt.Errorf("decode headless Codex rollout: %w", err)
		}
		if entry.Type == "session_meta" {
			if entry.Payload.ID != threadID {
				return errors.New("headless Codex rollout has the wrong thread ID")
			}
			metaCount++
		}
		if entry.Type == "response_item" && entry.Payload.Type == "message" && entry.Payload.Role == "developer" &&
			len(entry.Payload.Content) == 1 && entry.Payload.Content[0].Type == "input_text" && entry.Payload.Content[0].Text == marker {
			markerCount++
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if metaCount != 1 || markerCount != 1 {
		return fmt.Errorf("headless Codex rollout has %d matching metadata and %d bootstrap markers", metaCount, markerCount)
	}
	return nil
}

// DeleteFreshHeadlessThread retires only a verified, empty, unmaterialized
// member. It is intended for replacing an unusable thread after a client
// disconnect, never for a thread with a user turn or uncertain submission.
func (c *Client) DeleteFreshHeadlessThread(ctx context.Context, threadID, cwd, projectID string) error {
	materialized, err := c.HeadlessThreadMaterialized(ctx, threadID, cwd, projectID)
	if err != nil {
		return err
	}
	if materialized {
		return errors.New("cannot delete a materialized headless Codex thread")
	}
	if err := c.RequireThreadIdle(ctx, threadID, cwd); err != nil {
		return err
	}
	materialized, err = c.HeadlessThreadMaterialized(ctx, threadID, cwd, projectID)
	if err != nil || materialized {
		if err != nil {
			return err
		}
		return errors.New("headless Codex rollout materialized during retirement")
	}
	return c.Request(ctx, "thread/delete", map[string]any{"threadId": threadID}, nil)
}

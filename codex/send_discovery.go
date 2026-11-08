package codex

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
)

// Hints are disposable and scoped to an exact lookup. The validation sets have
// the same history-dependent size as walkThreadItems; no transcript is retained.
type sendDiscoveryKey struct {
	clientID, textDigest, action, optionsDigest string
}

type sendDiscoverySource struct {
	cwd, path, identity string
	generation          uint64
	file                os.FileInfo
	tail                []byte
}

type sendDiscoveryAnchor struct{ itemID, turnID string }

func discoveryAnchor(entry threadItemEntry) sendDiscoveryAnchor {
	return sendDiscoveryAnchor{stringValue(entry.Item["id"]), entry.TurnID}
}

type sendDiscoveryCatchup struct {
	walk         threadItemWalk
	target, head sendDiscoveryAnchor
}

type sendDiscovery struct {
	key          sendDiscoveryKey
	source       sendDiscoverySource
	walk         threadItemWalk
	match        sentMessageMatch
	head         sendDiscoveryAnchor
	complete     bool
	negativeSize int64
	catchup      *sendDiscoveryCatchup
	lastUsed     time.Time
}

func (c *Client) takeSendDiscovery(threadID string, key sendDiscoveryKey) *sendDiscovery {
	c.sendDiscoveryMu.Lock()
	defer c.sendDiscoveryMu.Unlock()
	now := time.Now()
	for id, hint := range c.sendDiscoveries {
		if now.Sub(hint.lastUsed) > pageCursorIdle {
			delete(c.sendDiscoveries, id)
		}
	}
	hint := c.sendDiscoveries[threadID]
	delete(c.sendDiscoveries, threadID)
	if hint != nil && hint.key == key {
		return hint
	}
	return nil
}

func (c *Client) saveSendDiscovery(threadID string, hint *sendDiscovery) {
	c.sendDiscoveryMu.Lock()
	defer c.sendDiscoveryMu.Unlock()
	if c.pageContext.Err() != nil {
		return
	}
	for len(c.sendDiscoveries) >= pageCursorLimit {
		oldestID := ""
		for id, entry := range c.sendDiscoveries {
			if oldestID == "" || entry.lastUsed.Before(c.sendDiscoveries[oldestID].lastUsed) {
				oldestID = id
			}
		}
		delete(c.sendDiscoveries, oldestID)
	}
	hint.lastUsed = time.Now()
	c.sendDiscoveries[threadID] = hint
}

func (c *Client) sendDiscoverySource(ctx context.Context, threadID string) (sendDiscoverySource, error) {
	// ReadThreadMetadata's public projection omits createdAt. Keep the raw
	// lineage used by pageIdentity and the native metadata-only request shape.
	var metadata struct {
		Thread map[string]any `json:"thread"`
	}
	if err := c.Request(ctx, "thread/read", map[string]any{"threadId": threadID, "includeTurns": false}, &metadata); err != nil {
		return sendDiscoverySource{}, err
	}
	thread := metadata.Thread
	if thread == nil || stringValue(thread["id"]) != threadID {
		return sendDiscoverySource{}, errors.New("send discovery returned the wrong thread")
	}
	source := sendDiscoverySource{cwd: stringValue(thread["cwd"]), path: stringValue(thread["path"]),
		identity: pageIdentity(thread), generation: c.pageGeneration()}
	if source.cwd == "" || source.path == "" {
		return sendDiscoverySource{}, errors.New("send discovery requires rollout source identity")
	}
	var err error
	source.file, err = pageFile(source.path)
	if err != nil {
		return sendDiscoverySource{}, err
	}
	source.tail, err = pageRolloutTail(source.path, source.file.Size())
	return source, err
}

func (source sendDiscoverySource) valid(current sendDiscoverySource) bool {
	return source.cwd == current.cwd && source.path == current.path && source.identity == current.identity &&
		source.generation == current.generation && os.SameFile(source.file, current.file) && current.file.Size() >= source.file.Size() &&
		(current.file.Size() != source.file.Size() || current.file.ModTime().Equal(source.file.ModTime())) &&
		pageMetadataTailMatches(source.path, source.file, source.file.Size(), source.tail)
}

// SendAttemptedWithOptionsResumable has the same final result and atomic
// history-receipt import as SendAttemptedWithOptions. Canceled calls retain only
// completed, validated proof pages for the next explicit call on this client.
// Incomplete proof always returns an error. This method never submits input.
func (c *Client) SendAttemptedWithOptionsResumable(
	ctx context.Context, threadID, text, clientID, action string, options TurnOptions,
) (found bool, err error) {
	normalized, err := normalizeTurnOptions(options)
	if err != nil {
		return false, err
	}
	lock := c.queueUpdateLock(threadID)
	if err := lock.LockContext(ctx); err != nil {
		return false, err
	}
	defer lock.Unlock()
	key := sendDiscoveryKey{clientID, queueTextDigest(text), action, normalized.Digest}
	hint := c.takeSendDiscovery(threadID, key)
	found, err = c.sendDiscoveryAttempt(threadID, clientID, text, action, normalized.Digest)
	if err != nil || found {
		return found, err
	}
	source, err := c.sendDiscoverySource(ctx, threadID)
	if err != nil {
		if hint != nil {
			c.saveSendDiscovery(threadID, hint)
		}
		return false, err
	}
	if hint == nil || !hint.source.valid(source) {
		hint = &sendDiscovery{key: key, source: source, walk: newThreadItemWalk()}
	} else {
		// Appended bytes may now belong to the retained proof. Future calls
		// must validate this newer prefix, not just the original file length.
		hint.source = source
	}
	// Malformed pages, identity conflicts and failed durable writes discard the
	// hint. Cancellation/transport failures keep only earlier completed pages.
	defer func() {
		var transport *TransportError
		if !found && (err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.As(err, &transport)) {
			c.saveSendDiscovery(threadID, hint)
		}
	}()
	for {
		if err = c.continueSendDiscovery(ctx, threadID, text, clientID, hint); err != nil {
			return false, err
		}
		current, sourceErr := c.sendDiscoverySource(ctx, threadID)
		if sourceErr != nil {
			return false, sourceErr
		}
		if !hint.source.valid(current) {
			return false, errors.New("send discovery rollout changed during proof")
		}
		appended := current.file.Size() > hint.negativeSize
		hint.source = current
		if hint.match.found || !appended {
			break
		}
		// Growth during proof needs another native head refresh. File size
		// alone cannot establish that an appended exact input is absent.
	}

	if err := ctx.Err(); err != nil {
		return false, err
	}
	// Another client may have crossed admission while network reads ran. An
	// old negative never overrides the authoritative durable attempt.
	found, err = c.sendDiscoveryAttempt(threadID, clientID, text, action, normalized.Digest)
	if err != nil || found {
		return found, err
	}
	if !hint.match.found {
		return false, nil
	}
	if err := c.recordSendAttemptWithReceipt(threadID, clientID, text, action, hint.match.receipt.Steered, options, &hint.match.receipt); err != nil {
		return false, err
	}
	return true, nil
}

func (c *Client) continueSendDiscovery(ctx context.Context, threadID, text, clientID string, hint *sendDiscovery) error {
	for !hint.match.done {
		if err := ctx.Err(); err != nil {
			return err
		}
		if hint.complete {
			stable, err := c.catchupSendDiscovery(ctx, threadID, text, clientID, hint)
			if err != nil || stable {
				return err
			}
			continue
		}
		entries, next, err := c.nextThreadItems(ctx, threadID, &hint.walk)
		if err != nil {
			return err
		}
		if c.pageGeneration() != hint.source.generation {
			return errors.New("send discovery connection changed during proof")
		}
		match := hint.match
		for _, entry := range entries {
			stop, err := match.visit(entry, clientID, text)
			if err != nil {
				return err
			}
			if stop {
				break
			}
		}
		if hint.head.itemID == "" && len(entries) > 0 {
			hint.head = discoveryAnchor(entries[0])
		}
		source, err := c.checkpointSendDiscoverySource(hint.source)
		if err != nil {
			return err
		}
		hint.source = source
		hint.walk.commit(entries, next)
		hint.match = match
		if next == nil {
			hint.complete = true
			hint.match.done = hint.match.found
		}
	}
	return nil
}

// Refresh the head through the prior anchor. A canceled refresh retains its own
// completed-page cursor. Once caught up, refresh again through the new anchor;
// only a fresh unchanged head can authorize a complete negative.
func (c *Client) catchupSendDiscovery(ctx context.Context, threadID, text, clientID string, hint *sendDiscovery) (bool, error) {
	if hint.catchup == nil {
		hint.catchup = &sendDiscoveryCatchup{walk: newThreadItemWalk(), target: hint.head}
	}
	pass := hint.catchup
	headSize := hint.source.file.Size()
	entries, next, err := c.nextThreadItems(ctx, threadID, &pass.walk)
	if err != nil {
		return false, err
	}
	if c.pageGeneration() != hint.source.generation {
		return false, errors.New("send discovery connection changed during head refresh")
	}
	head := pass.head
	if head.itemID == "" && len(entries) > 0 {
		head = discoveryAnchor(entries[0])
	}
	match := hint.match
	reached := false
	for _, entry := range entries {
		anchor := discoveryAnchor(entry)
		if anchor.itemID == pass.target.itemID {
			if anchor != pass.target {
				return false, errors.New("send discovery head anchor changed turn")
			}
			reached = true
		} else if _, exists := hint.walk.seenIDs[anchor.itemID]; exists {
			return false, errors.New("send discovery head anchor reordered")
		}
		stop, err := match.visit(entry, clientID, text)
		if err != nil {
			return false, err
		}
		if stop || reached {
			break
		}
	}
	if !reached && next == nil && pass.target.itemID != "" && !match.done {
		return false, fmt.Errorf("send discovery head anchor %q disappeared", pass.target.itemID)
	}
	source, err := c.checkpointSendDiscoverySource(hint.source)
	if err != nil {
		return false, err
	}
	hint.source = source
	pass.walk.commit(entries, next)
	pass.head, hint.match = head, match
	if match.done {
		return true, nil
	}
	if !reached && next != nil {
		return false, nil
	}
	if next == nil && match.found {
		hint.match.done = true
		return true, nil
	}
	// Earlier completed catch-up pages belong to the newly proven prefix too.
	for id := range pass.walk.seenIDs {
		hint.walk.seenIDs[id] = struct{}{}
	}
	hint.head, hint.catchup = head, nil
	stable := head == pass.target
	if stable {
		hint.negativeSize = headSize
	}
	return stable, nil
}

// Queue-only identities still belong to ordinary send reconciliation. They are
// not an absent send, even when no history item has materialized yet.
func (c *Client) sendDiscoveryAttempt(threadID, clientID, text, action, optionsDigest string) (bool, error) {
	_, found, err := c.sendAttemptWithOptions(threadID, clientID, text, action, optionsDigest)
	if err != nil || found {
		return found, err
	}
	queued, err := c.queueAttempt(threadID, clientID, text)
	if err != nil {
		return false, err
	}
	if queued {
		return false, &UnknownSendOutcomeError{Err: errors.New("queued message requires send reconciliation")}
	}
	return false, nil
}

// Bind every completed page to its current append-only file prefix before its
// cursor can survive cancellation. Metadata/lineage is rechecked each call.
func (c *Client) checkpointSendDiscoverySource(source sendDiscoverySource) (sendDiscoverySource, error) {
	current := source
	var err error
	current.file, err = pageFile(source.path)
	if err != nil {
		return sendDiscoverySource{}, err
	}
	current.tail, err = pageRolloutTail(source.path, current.file.Size())
	if err != nil {
		return sendDiscoverySource{}, err
	}
	current.generation = c.pageGeneration()
	if !source.valid(current) {
		return sendDiscoverySource{}, errors.New("send discovery rollout changed before page checkpoint")
	}
	return current, nil
}

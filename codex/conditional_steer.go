package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

// ConditionalSteer delivers input only to the exact expected active turn. It
// never resumes a thread, starts a turn, queues input, or changes settings.
// The caller owns authorization and must retain the complete identity for retry.
// A receipt proves delivery, not that the model processed or acknowledged input.
func (c *Client) ConditionalSteer(ctx context.Context,
	threadID, cwd, expectedTurnID, text, clientUserMessageID, actionContext string,
) (SendReceipt, error) {
	action, err := conditionalSteerAction(threadID, cwd, expectedTurnID, text, clientUserMessageID, actionContext)
	if err != nil {
		return SendReceipt{}, err
	}
	lock := c.queueUpdateLock(threadID)
	if err := lock.LockContext(ctx); err != nil {
		return SendReceipt{}, err
	}
	defer lock.Unlock()
	// Isolate native I/O from the receiver's subscriptions. Even Ensure on a
	// reconnect must not restore watches and implicitly resume idle work.
	transport := NewWithOptions(c.socket, ClientOptions{
		ClientInfo: c.options.ClientInfo, ObserverOnly: c.options.ObserverOnly,
		AllowImplicitResume: func(string) bool { return false },
	})
	defer transport.Close()
	state, err := c.sendStateWithOptions(threadID, clientUserMessageID, text, action, zeroTurnOptionsDigest())
	if err != nil {
		return SendReceipt{}, err
	}
	if state.queued || (state.found && !state.attempt.Steered) {
		return SendReceipt{}, errors.New("conditional steer identity belongs to another submission")
	}
	if !state.found {
		if err := c.recordSendAttempt(threadID, clientUserMessageID, text, action, true); err != nil {
			return SendReceipt{}, err
		}
	}
	// Resolve existing submissions before examining current activity. An earlier
	// accepted steer may now be in a completed turn, and uncertainty never permits
	// another dispatch even if the original turn remains active.
	if receipt, found, err := c.reconcileConditionalSteer(ctx, transport, threadID, expectedTurnID, text, clientUserMessageID, action); found || err != nil {
		return receipt, err
	}
	if err := transport.conditionalSteerActive(ctx, threadID, cwd, expectedTurnID); err != nil {
		return c.conditionalSteerUnwritten(ctx, transport, threadID, expectedTurnID, text, clientUserMessageID, action, err)
	}
	params := map[string]any{
		"threadId": threadID, "expectedTurnId": expectedTurnID,
		"input":               []map[string]any{{"type": "text", "text": text}},
		"clientUserMessageId": clientUserMessageID,
	}
	var response struct {
		TurnID string `json:"turnId"`
	}
	claimed, claim := false, ""
	requestErr := transport.requestWithAdmission(ctx, "turn/steer", params, &response, func() error {
		var err error
		claim, err = c.claimSendSubmissionExact(threadID, clientUserMessageID, text, action, zeroTurnOptionsDigest(), true)
		claimed = err == nil
		return err
	})
	if requestErr != nil && !claimed {
		return c.conditionalSteerUnwritten(ctx, transport, threadID, expectedTurnID, text, clientUserMessageID, action, requestErr)
	}
	var unwritten *unwrittenWriteError
	var refusal *rpcCallError
	if errors.As(requestErr, &unwritten) || (errors.As(requestErr, &refusal) &&
		(refusal.code == -32600 || refusal.code == -32601 || refusal.code == -32602)) {
		if err := c.restoreRejectedSendClaim(threadID, clientUserMessageID, claim); err != nil {
			return SendReceipt{}, &UnknownSendOutcomeError{Err: errors.Join(requestErr, err)}
		}
		return c.conditionalSteerUnwritten(ctx, transport, threadID, expectedTurnID, text, clientUserMessageID, action, requestErr)
	}
	if requestErr == nil && response.TurnID == expectedTurnID {
		return c.finishAcceptedSend(threadID, SendReceipt{
			TurnID: expectedTurnID, ClientUserMessageID: clientUserMessageID, Steered: true,
		})
	}
	if requestErr == nil {
		requestErr = errors.New("turn/steer returned the wrong turn")
	}
	if receipt, found, err := c.reconcileConditionalSteer(ctx, transport, threadID, expectedTurnID, text, clientUserMessageID, action); found && err == nil {
		return receipt, nil
	} else if err != nil {
		requestErr = errors.Join(requestErr, err)
	}
	return SendReceipt{}, &UnknownSendOutcomeError{Err: requestErr}
}

// DiscardPreparedConditionalSteer retires only an exact unsubmitted attempt.
// First durably record the terminal disposition and original identity in the
// owning operation, and never dispatch that terminal notice again. Recovery may
// repeat this discard when already absent. Accepted or uncertain input refuses.
func (c *Client) DiscardPreparedConditionalSteer(ctx context.Context,
	threadID, cwd, expectedTurnID, text, clientUserMessageID, actionContext string,
) error {
	action, err := conditionalSteerAction(threadID, cwd, expectedTurnID, text, clientUserMessageID, actionContext)
	if err != nil {
		return err
	}
	lock := c.queueUpdateLock(threadID)
	if err := lock.LockContext(ctx); err != nil {
		return err
	}
	defer lock.Unlock()
	return submissionAction(c, attemptScope(threadID, clientUserMessageID), func() error {
		attempt, found := c.sendAttempts[threadID][clientUserMessageID]
		if !found {
			if c.queueAttempts[threadID][clientUserMessageID] != "" {
				return errors.New("conditional steer identity belongs to queued input")
			}
			return nil
		}
		if attempt.Digest != queueTextDigest(text) || attempt.Context != action || !attempt.Steered ||
			effectiveTurnOptionsDigest(attempt.OptionsDigest) != zeroTurnOptionsDigest() {
			return errors.New("conditional steer identity was reused for another action")
		}
		if attempt.State != "prepared" || c.queueAttempts[threadID][clientUserMessageID] != "" {
			return errors.New("conditional steer may have been submitted; reconcile its outcome")
		}
		delete(c.sendAttempts[threadID], clientUserMessageID)
		if err := c.writeQueueLedgerLocked(); err != nil {
			c.sendAttempts[threadID][clientUserMessageID] = attempt
			return err
		}
		return nil
	})
}

func conditionalSteerAction(thread, cwd, expected, text, id, action string) (string, error) {
	for _, value := range []string{thread, cwd, expected, text, id, action} {
		if !utf8.ValidString(value) {
			return "", errors.New("conditional steer requires valid UTF-8 input and identities")
		}
	}
	if strings.TrimSpace(thread) == "" || strings.TrimSpace(expected) == "" || strings.TrimSpace(text) == "" ||
		strings.TrimSpace(id) == "" || strings.TrimSpace(action) == "" || !filepath.IsAbs(cwd) || filepath.Clean(cwd) != cwd {
		return "", errors.New("conditional steer requires trusted thread/cwd, text and exact turn, message and action identities")
	}
	identity, _ := json.Marshal([]string{cwd, expected, action})
	return "conditional-steer:v1:" + string(identity), nil
}

func (c *Client) conditionalSteerActive(ctx context.Context, threadID, cwd, expected string) error {
	var metadata struct {
		Thread map[string]any `json:"thread"`
	}
	if err := c.Request(ctx, "thread/read", map[string]any{"threadId": threadID, "excludeTurns": true}, &metadata); err != nil {
		return err
	}
	if stringValue(metadata.Thread["id"]) != threadID || stringValue(metadata.Thread["cwd"]) != cwd {
		return errors.New("Codex thread does not match the trusted working directory")
	}
	if status := statusValue(metadata.Thread["status"]); status != "active" && status != "idle" {
		return errors.New("conditional steer requires a loaded active turn")
	}
	active, err := c.ActiveTurnID(ctx, threadID)
	if err != nil {
		return err
	}
	if active != expected {
		return errors.New("conditional steer target is no longer the active turn")
	}
	return nil
}

func (c *Client) reconcileConditionalSteer(ctx context.Context, transport *Client, threadID, expected, text, id, action string) (SendReceipt, bool, error) {
	state, err := c.sendStateWithOptions(threadID, id, text, action, zeroTurnOptionsDigest())
	if err != nil {
		return SendReceipt{}, false, &UnknownSendOutcomeError{Err: err}
	}
	if state.queued || (state.found && !state.attempt.Steered) {
		return SendReceipt{}, false, &UnknownSendOutcomeError{Err: errors.New("conditional steer identity belongs to another submission")}
	}
	attempt := state.attempt
	if attempt.State == "accepted" {
		if attempt.TurnID != expected || !attempt.Steered {
			return SendReceipt{}, false, &UnknownSendOutcomeError{Err: errors.New("conditional steer receipt names another turn")}
		}
		return SendReceipt{TurnID: expected, ClientUserMessageID: id, Steered: true}, true, nil
	}
	if attempt.State != "submitting" {
		return SendReceipt{}, false, nil
	}
	receipt, accepted, err := transport.sentByClientID(ctx, threadID, id, text)
	if err == nil && (!accepted || receipt.TurnID != expected) {
		err = errors.New("conditional steer is not present in its exact target turn")
	}
	if err == nil {
		receipt.Steered = true
		err = c.markSendAccepted(threadID, id, receipt)
	}
	if err != nil {
		return SendReceipt{}, false, &UnknownSendOutcomeError{Err: err}
	}
	return receipt, true, nil
}

func (c *Client) conditionalSteerUnwritten(ctx context.Context, transport *Client, threadID, expected, text, id, action string, requestErr error) (SendReceipt, error) {
	if receipt, found, err := c.reconcileConditionalSteer(ctx, transport, threadID, expected, text, id, action); found || err != nil {
		return receipt, err
	}
	return SendReceipt{}, &MessageNotSentError{Err: fmt.Errorf("conditional steer refused: %w", requestErr), Retryable: retryableDeliveryError(requestErr)}
}

package codex

import (
	"context"
	"errors"
)

var errSendAdmissionChanged = errors.New("message attempt changed before write admission")

// Recheck even an early cancellation: another process may have advanced the
// same identity since this caller first inspected it. Queue and send state come
// from one ledger snapshot, using the original options digest.
func (c *Client) unwrittenSendOutcome(ctx context.Context, threadID, text, clientID, actionContext, optionsDigest string, allowAbsent bool, requestErr error) (SendReceipt, error) {
	state, err := c.sendStateWithOptions(threadID, clientID, text, actionContext, optionsDigest)
	if err != nil {
		return SendReceipt{}, &UnknownSendOutcomeError{Err: err}
	}
	if !state.queued && ((state.found && state.attempt.State == "prepared") || (!state.found && allowAbsent)) {
		return SendReceipt{}, &MessageNotSentError{Err: requestErr}
	}
	if state.found && !state.queued {
		receipt, accepted, err := c.reconcileSend(ctx, threadID, text, clientID, state.attempt)
		if accepted && err == nil {
			return receipt, nil
		}
		if err != nil {
			requestErr = err
		}
	}
	return SendReceipt{}, &UnknownSendOutcomeError{Err: requestErr}
}

// The caller holds the thread update lock and has received an explicit native
// protocol refusal. Never use this transition after a transport failure.
func (c *Client) restoreRejectedSend(threadID, clientID string) error {
	return queueLedgerAction(c, func() error {
		attempt, ok := c.sendAttempts[threadID][clientID]
		if !ok || attempt.State != "submitting" {
			return errors.New("rejected message attempt changed before recovery")
		}
		previous := attempt
		attempt.State = "prepared"
		c.sendAttempts[threadID][clientID] = attempt
		if err := c.writeQueueLedgerLocked(); err != nil {
			c.sendAttempts[threadID][clientID] = previous
			return err
		}
		return nil
	})
}

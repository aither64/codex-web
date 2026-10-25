package codex

import (
	"context"
	"errors"
)

var errSendAdmissionChanged = errors.New("message attempt changed before write admission")

// Writer acquisition failed, or admission consumed the deadline before a writer
// was requested. No message bytes were handed to the WebSocket writer.
type unwrittenWriteError struct{ Err error }

func (e *unwrittenWriteError) Error() string { return e.Err.Error() }
func (e *unwrittenWriteError) Unwrap() error { return e.Err }

func (c *Client) restoreUnwrittenSendOutcome(ctx context.Context, thread, text, id, action, optionsDigest, claim string, requestErr error) (SendReceipt, error) {
	if err := c.restoreRejectedSendClaim(thread, id, claim); err != nil {
		requestErr = errors.Join(requestErr, err)
	}
	return c.unwrittenSendOutcome(ctx, thread, text, id, action, optionsDigest, false, requestErr)
}

// Recheck even an early cancellation: another process may have advanced the
// same identity since this caller first inspected it. Queue and send state come
// from one ledger snapshot, using the original options digest.
func (c *Client) unwrittenSendOutcome(ctx context.Context, threadID, text, clientID, actionContext, optionsDigest string, allowAbsent bool, requestErr error) (SendReceipt, error) {
	state, err := c.sendStateWithOptions(threadID, clientID, text, actionContext, optionsDigest)
	if err != nil {
		return SendReceipt{}, &UnknownSendOutcomeError{Err: err}
	}
	if !state.queued && ((state.found && state.attempt.State == "prepared") || (!state.found && allowAbsent)) {
		return SendReceipt{}, &MessageNotSentError{Err: requestErr, Retryable: retryableDeliveryError(requestErr)}
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

func retryableDeliveryError(err error) bool {
	var transport *TransportError
	if errors.As(err, &transport) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	// SQLite extended result codes retain the primary result in the low byte.
	var coded interface{ Code() int }
	return errors.As(err, &coded) && (coded.Code()&0xff == 5 || coded.Code()&0xff == 6)
}

// The caller holds the thread update lock and has an explicit native refusal
// or transport proof that writer acquisition never dispatched input.
func (c *Client) restoreRejectedSend(threadID, clientID string) error {
	return c.restoreRejectedSendClaim(threadID, clientID, "")
}

func (c *Client) restoreRejectedSendClaim(threadID, clientID, claim string) error {
	return submissionAction(c, attemptScope(threadID, clientID), func() error {
		attempt, ok := c.sendAttempts[threadID][clientID]
		if !ok || attempt.State != "submitting" || (c.options.SubmissionDatabasePath != "" && claim != "" && attempt.admissionID != claim) {
			return errors.New("rejected message attempt changed before recovery")
		}
		previous := attempt
		attempt.State, attempt.admissionID = "prepared", ""
		c.sendAttempts[threadID][clientID] = attempt
		if err := c.writeQueueLedgerLocked(); err != nil {
			c.sendAttempts[threadID][clientID] = previous
			return err
		}
		return nil
	})
}

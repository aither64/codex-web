package codex

import (
	"context"
	"errors"
	"strings"
)

// ContinueFailedTurn starts a new visible turn only on a loaded, quiescent thread
// whose latest turn is the exact expected failure. It never resumes, steers,
// updates policy, or starts queued work. The application must hold its shared
// admission fence and prove unchanged effective settings and execution authority.
// Bind expectedFailedTurnID and the settings digest into actionContext and retain
// that context, text, stableID and options unchanged for reconciliation.
func (c *Client) ContinueFailedTurn(
	ctx context.Context, threadID, text, stableID, actionContext, expectedFailedTurnID string,
	options TurnOptions,
) (SendReceipt, error) {
	if strings.TrimSpace(text) == "" || stableID == "" || actionContext == "" || expectedFailedTurnID == "" {
		return SendReceipt{}, errors.New("continuation requires text and exact attempt, action and failed turn identities")
	}
	normalized, err := normalizeTurnOptions(options)
	if err != nil {
		return SendReceipt{}, err
	}
	lock := c.queueUpdateLock(threadID)
	lock.Lock()
	defer lock.Unlock()
	attempt, attempted, err := c.sendAttemptWithOptions(threadID, stableID, text, actionContext, normalized.Digest)
	if err != nil {
		return SendReceipt{}, err
	}
	if attempt.Steered {
		return SendReceipt{}, errors.New("continuation identity belongs to a steered message")
	}
	if queued, err := c.queueAttempt(threadID, stableID, text); err != nil {
		return SendReceipt{}, err
	} else if queued {
		return SendReceipt{}, errors.New("continuation identity belongs to queued input")
	}
	if receipt, found, err := c.reconcileContinuation(ctx, threadID, text, stableID, attempt); err != nil || found {
		return receipt, err
	}
	preflightFailure := func(preflightErr error) (SendReceipt, error) {
		// Another cooperating client may have won after our initial read.
		latest, _, inspectErr := c.sendAttemptWithOptions(threadID, stableID, text, actionContext, normalized.Digest)
		if inspectErr != nil {
			return SendReceipt{}, inspectErr
		}
		if receipt, found, reconcileErr := c.reconcileContinuation(ctx, threadID, text, stableID, latest); found || reconcileErr != nil {
			return receipt, reconcileErr
		}
		return SendReceipt{}, preflightErr
	}
	if err := c.continuationIdle(ctx, threadID, expectedFailedTurnID); err != nil {
		return preflightFailure(err)
	}
	if err := c.requireSubmissionAttemptsExcept(ctx, threadID, false, stableID); err != nil {
		return preflightFailure(err)
	}
	if !attempted {
		if err := c.recordSendAttemptWithOptions(threadID, stableID, text, actionContext, false, options); err != nil {
			return SendReceipt{}, err
		}
	}
	// A second observation closes races with queue/history reads above. The
	// protocol has no compare-and-start; unrelated native clients remain outside
	// this library's admission fence and must be excluded by the application.
	if err := c.continuationIdle(ctx, threadID, expectedFailedTurnID); err != nil {
		return preflightFailure(err)
	}
	claimed, err := c.claimContinuationAttempt(threadID, stableID, text, actionContext, normalized.Digest)
	if err != nil {
		return SendReceipt{}, err
	}
	if !claimed {
		latest, _, err := c.sendAttemptWithOptions(threadID, stableID, text, actionContext, normalized.Digest)
		if err != nil {
			return SendReceipt{}, err
		}
		receipt, _, err := c.reconcileContinuation(ctx, threadID, text, stableID, latest)
		return receipt, err
	}
	params := map[string]any{"threadId": threadID, "input": []map[string]any{{"type": "text", "text": text}}, "clientUserMessageId": stableID}
	if normalized.Model != "" {
		params["model"] = normalized.Model
	}
	if normalized.ReasoningEffort != "" {
		params["effort"] = normalized.ReasoningEffort
	}
	if additional := normalized.additionalContextParameter(); additional != nil {
		params["additionalContext"] = additional
	}
	var response struct {
		Turn struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	requestErr := c.Request(ctx, "turn/start", params, &response)
	if requestErr == nil && response.Turn.ID != "" {
		return c.finishAcceptedSend(threadID, SendReceipt{TurnID: response.Turn.ID, ClientUserMessageID: stableID})
	}
	if requestErr == nil {
		requestErr = errors.New("turn/start returned no turn")
	}
	// Once submitting was persisted, every error is uncertain until exact
	// native history proves acceptance. Never turn it back into a fresh send.
	attempt, _, err = c.sendAttemptWithOptions(threadID, stableID, text, actionContext, normalized.Digest)
	if err == nil {
		if receipt, found, reconcileErr := c.reconcileContinuation(ctx, threadID, text, stableID, attempt); reconcileErr == nil && found {
			return receipt, nil
		}
	}
	return SendReceipt{}, &UnknownSendOutcomeError{Err: requestErr}
}

func (c *Client) continuationIdle(ctx context.Context, threadID, expected string) error {
	state, err := c.ReadTurnState(ctx, threadID)
	if err != nil {
		return err
	}
	if !state.IsLoadedFailedTurn(expected) {
		return errors.New("continuation requires the exact latest failed turn on a loaded quiescent thread")
	}
	if prompts, err := c.PromptsWithItems(ctx, threadID); err != nil {
		return err
	} else if len(prompts) != 0 {
		return errors.New("continuation is blocked by pending requests")
	}
	if queue, err := c.ListQueue(ctx, threadID); err != nil {
		return err
	} else if len(queue) != 0 {
		return errors.New("continuation is blocked by queued input")
	}
	return nil
}

func (c *Client) claimContinuationAttempt(threadID, id, text, action, optionsDigest string) (bool, error) {
	return queueLedgerTransaction(c, func() (bool, error) {
		attempt, ok := c.sendAttempts[threadID][id]
		if !ok || attempt.Digest != queueTextDigest(text) || attempt.Context != action || attempt.Steered || effectiveTurnOptionsDigest(attempt.OptionsDigest) != optionsDigest {
			return false, errors.New("continuation attempt changed before submission")
		}
		if attempt.State != "prepared" {
			return false, nil
		}
		attempt.State = "submitting"
		c.sendAttempts[threadID][id] = attempt
		if err := c.writeQueueLedgerLocked(); err != nil {
			attempt.State = "prepared"
			c.sendAttempts[threadID][id] = attempt
			return false, err
		}
		return true, nil
	})
}

func (c *Client) reconcileContinuation(ctx context.Context, threadID, text, id string, attempt sendAttempt) (SendReceipt, bool, error) {
	receipt, found, err := c.reconcileSend(ctx, threadID, text, id, attempt)
	if err != nil && attempt.State == "submitting" {
		var uncertain *UnknownSendOutcomeError
		if !errors.As(err, &uncertain) {
			err = &UnknownSendOutcomeError{Err: err}
		}
	}
	return receipt, found, err
}

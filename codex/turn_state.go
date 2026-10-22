package codex

import (
	"context"
	"errors"
	"slices"
)

// TerminalFailure describes only the latest, authoritatively failed turn.
// Code preserves the native string discriminator. Unknown and structured
// discriminators never become a known retry class through message matching.
type TerminalFailure struct {
	TurnID  string `json:"turnId"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type TurnState struct {
	ThreadID         string           `json:"threadId"`
	Status           string           `json:"status"`
	LatestTurnID     string           `json:"latestTurnId"`
	LatestTurnStatus string           `json:"latestTurnStatus"`
	TerminalFailure  *TerminalFailure `json:"terminalFailure"`
}

// IsLoadedFailedTurn checks only the exact loaded terminal observation. Native
// systemError can persist after a failed turn until another turn starts. This
// predicate does not classify errors or prove queue, request or authority holds.
func (s TurnState) IsLoadedFailedTurn(expectedTurnID string) bool {
	return expectedTurnID != "" && s.LatestTurnID == expectedTurnID &&
		s.LatestTurnStatus == "failed" && (s.Status == "idle" || s.Status == "systemError")
}

// ReadTurnState is a bounded cold observation. It never loads a thread or
// derives failure state from intermediate error notifications or transcript text.
func (c *Client) ReadTurnState(ctx context.Context, threadID string) (TurnState, error) {
	var metadata struct {
		Thread map[string]any `json:"thread"`
	}
	if err := c.historyRequest(ctx, "thread/read", map[string]any{
		"threadId": threadID, "excludeTurns": true,
	}, &metadata); err != nil {
		return TurnState{}, err
	}
	if threadID == "" || stringValue(metadata.Thread["id"]) != threadID {
		return TurnState{}, errors.New("thread/read returned the wrong thread")
	}
	state := TurnState{ThreadID: threadID, Status: statusValue(metadata.Thread["status"])}
	if !slices.Contains([]string{"idle", "notLoaded", "active", "systemError"}, state.Status) {
		return TurnState{}, errors.New("thread/read returned an unknown thread status")
	}
	var page struct {
		Data *[]map[string]any `json:"data"`
	}
	if err := c.historyRequest(ctx, "thread/turns/list", map[string]any{
		"threadId": threadID, "limit": 1, "sortDirection": "desc", "itemsView": "notLoaded",
	}, &page); err != nil {
		if c.freshThreadMissingSourceRollout(metadata.Thread, threadID, err) {
			return state, nil
		}
		return TurnState{}, err
	}
	if page.Data == nil || len(*page.Data) > 1 {
		return TurnState{}, errors.New("thread/turns/list returned invalid latest turn data")
	}
	if len(*page.Data) == 0 {
		if state.Status == "active" {
			return TurnState{}, errors.New("active thread returned no latest turn")
		}
		return state, nil
	}
	turn := (*page.Data)[0]
	state.LatestTurnID = stringValue(turn["id"])
	state.LatestTurnStatus = stringValue(turn["status"])
	if state.LatestTurnID == "" || !slices.Contains([]string{"inProgress", "completed", "failed", "interrupted"}, state.LatestTurnStatus) {
		return TurnState{}, errors.New("thread/turns/list returned invalid latest turn identity or status")
	}
	// Native status notifications can lag turn metadata. In-progress turns with
	// idle status remain active for admission; a terminal turn with active thread
	// status is also refused by the continuation preflight.
	if state.LatestTurnStatus == "failed" {
		if failure, ok := turn["error"].(map[string]any); ok {
			state.TerminalFailure = &TerminalFailure{TurnID: state.LatestTurnID,
				Code: stringValue(failure["codexErrorInfo"]), Message: stringValue(failure["message"])}
		}
	}
	return state, nil
}

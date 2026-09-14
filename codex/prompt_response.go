package codex

import (
	"context"
	"errors"
)

// PromptResponse binds an action to the exact offer returned by Prompts.
// Token is local to this client instance and never sent to the App Server.
type PromptResponse struct {
	ID       string                         `json:"id"`
	Token    string                         `json:"token,omitempty"`
	Decision string                         `json:"decision,omitempty"`
	Answers  map[string]map[string][]string `json:"answers,omitempty"`
	Snooze   bool                           `json:"snooze,omitempty"`
}

// PromptResponseError distinguishes rejection before sending from an uncertain
// transport outcome. NotSent is never inferred from an HTTP status or timeout.
type PromptResponseError struct {
	Code    string
	NotSent bool
	Err     error
}

func (e *PromptResponseError) Error() string { return e.Err.Error() }
func (e *PromptResponseError) Unwrap() error { return e.Err }

func promptError(code, message string) error {
	return &PromptResponseError{Code: code, NotSent: true, Err: errors.New(message)}
}

var errConnectionChanged = errors.New("Codex App Server connection changed before the request was sent")

// RespondPrompt is the token-bound alternative to the legacy response methods.
func (c *Client) RespondPrompt(ctx context.Context, threadID string, response PromptResponse) error {
	if response.Token == "" || len(response.Token) > 128 {
		return promptError("prompt_changed", "This question needs to be refreshed before it can be answered.")
	}
	actions := 0
	if response.Snooze {
		actions++
	}
	if len(response.Answers) > 0 {
		actions++
	}
	if response.Decision != "" {
		actions++
	}
	if actions != 1 {
		return promptError("invalid_response", "Choose one response action.")
	}
	if response.Snooze {
		return c.snoozeUserInput(response.ID, threadID, response.Token)
	}
	if len(response.Answers) > 0 {
		return c.respondAnswers(ctx, response.ID, threadID, response.Token, response.Answers)
	}
	return c.respondDecision(ctx, response.ID, threadID, response.Token, response.Decision)
}

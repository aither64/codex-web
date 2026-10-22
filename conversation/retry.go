package conversation

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"unicode/utf8"
)

// RetryProvider is application-owned policy, scheduling and authority. Reads
// are observations; preference changes and cancellation must be idempotent.
type RetryProvider interface {
	ReadRetry(context.Context, string) (RetryStatus, error)
	SetRetryEnabled(context.Context, string, bool) (RetryStatus, error)
	CancelRetry(context.Context, string) (RetryStatus, error)
}

// RetryStatus carries safe presentation only, never native routing or options.
// NextAttemptAtMS is null when there is no deadline. ServerNowMS lets a browser
// display the deadline without relying on its wall clock matching the server.
type RetryStatus struct {
	Enabled         bool   `json:"enabled"`
	Class           string `json:"class"`
	Phase           string `json:"phase"`
	Attempt         int    `json:"attempt"`
	NextAttemptAtMS *int64 `json:"nextAttemptAtMs"`
	ServerNowMS     int64  `json:"serverNowMs"`
	CyberUsed       int    `json:"cyberUsed"`
	CyberLimit      int    `json:"cyberLimit"`
	Reason          string `json:"reason"`
}

func (handler *Handler) retry(response http.ResponseWriter, request *http.Request, target Target, operation string) {
	if !target.Capabilities.Retry ||
		(operation == "retry" && request.Method != http.MethodGet) ||
		(operation != "retry" && request.Method != http.MethodPost) {
		handler.rejectOperation(response, request)
		return
	}
	if target.Retry == nil {
		writeError(response, http.StatusNotFound, "retry is unavailable")
		return
	}
	var state RetryStatus
	var err error
	switch operation {
	case "retry":
		state, err = target.Retry.ReadRetry(request.Context(), target.ThreadID)
	case "retry/enabled":
		var body struct {
			Enabled *bool `json:"enabled"`
		}
		if !handler.decode(response, request, &body) {
			return
		}
		if body.Enabled == nil {
			writeError(response, http.StatusBadRequest, "enabled must be a boolean")
			return
		}
		state, err = target.Retry.SetRetryEnabled(request.Context(), target.ThreadID, *body.Enabled)
	case "retry/cancel":
		if !handler.decodeEmpty(response, request) {
			return
		}
		state, err = target.Retry.CancelRetry(request.Context(), target.ThreadID)
	}
	if err != nil {
		handler.serverError(response, request, errors.New("retry provider operation failed"))
		return
	}
	if !validRetryStatus(state) {
		handler.serverError(response, request, errors.New("retry provider returned invalid status"))
		return
	}
	writeJSON(response, http.StatusOK, state)
}

func validRetryStatus(state RetryStatus) bool {
	return slices.Contains([]string{"", "capacity", "cybersecurity"}, state.Class) &&
		slices.Contains([]string{"idle", "scheduled", "submitting", "reconciling", "running", "cancelled", "exhausted", "blocked"}, state.Phase) &&
		state.Attempt >= 0 && state.ServerNowMS > 0 &&
		(state.NextAttemptAtMS == nil || *state.NextAttemptAtMS > 0) &&
		state.CyberUsed >= 0 && state.CyberUsed <= state.CyberLimit && state.CyberLimit == 30 &&
		len(state.Reason) <= 1024 && utf8.ValidString(state.Reason)
}

package conversation

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"slices"
	"unicode/utf8"
)

// RetryActivityProvider observes application-owned retry chains. Tokens compare
// exact action identities; the application still owns authority and scheduling.
type RetryActivityProvider interface {
	ReadRetryActivity(context.Context, string) (RetryActivityStatus, error)
	RetryNow(context.Context, string, string) (RetryActivityStatus, error)
	StopRetry(context.Context, string, string) (RetryActivityStatus, error)
}

type RetryActivityStatus struct {
	ServerNowMS int64          `json:"serverNowMs"`
	Activity    *RetryActivity `json:"activity"`
}

// RetryActivity contains safe presentation data, never native routing, prompts
// or execution options. Nullable metrics preserve unavailable observations.
type RetryActivity struct {
	ChainKey        string `json:"chainKey"`
	Class           string `json:"class"`
	Phase           string `json:"phase"`
	ConfirmedStarts *int64 `json:"confirmedStarts"`
	Unconfirmed     int    `json:"unconfirmed"`
	CountQuality    string `json:"countQuality"`
	StartedAtMS     *int64 `json:"startedAtMs"`
	ElapsedMS       *int64 `json:"elapsedMs"`
	TimingQuality   string `json:"timingQuality"`
	FinishedAtMS    *int64 `json:"finishedAtMs"`
	NextAttemptAtMS *int64 `json:"nextAttemptAtMs"`
	Outcome         string `json:"outcome"`
	Reason          string `json:"reason"`
	RetryNowToken   string `json:"retryNowToken"`
	StopToken       string `json:"stopToken"`
}

const (
	RetryActionStale         = "retry_action_stale"
	RetryReloadRequired      = "retry_reload_required"
	RetryActivityUnavailable = "retry_activity_unavailable"
)

// RetryActivityError supplies a bounded public error code. Provider diagnostics
// are never copied into the HTTP response or logs.
type RetryActivityError struct{ Code string }

func (err *RetryActivityError) Error() string { return "retry activity action failed" }

var retryActivityIdentity = regexp.MustCompile(`^[A-Za-z0-9_-]{1,256}$`)

const maxSafeJSONInteger int64 = 1<<53 - 1

func (handler *Handler) retryActivity(response http.ResponseWriter, request *http.Request, target Target, operation string) {
	if !target.Capabilities.RetryActivity ||
		(operation == "retry/activity" && request.Method != http.MethodGet) ||
		(operation != "retry/activity" && request.Method != http.MethodPost) {
		handler.rejectOperation(response, request)
		return
	}
	if target.RetryActivity == nil {
		writeError(response, http.StatusNotFound, "retry activity is unavailable")
		return
	}
	var state RetryActivityStatus
	var err error
	if operation == "retry/activity" {
		state, err = target.RetryActivity.ReadRetryActivity(request.Context(), target.ThreadID)
	} else {
		var body struct {
			Token string `json:"token"`
		}
		if !handler.decode(response, request, &body) {
			return
		}
		if !retryActivityIdentity.MatchString(body.Token) {
			writeError(response, http.StatusBadRequest, "retry action token is invalid")
			return
		}
		if operation == "retry/now" {
			state, err = target.RetryActivity.RetryNow(request.Context(), target.ThreadID, body.Token)
		} else {
			state, err = target.RetryActivity.StopRetry(request.Context(), target.ThreadID, body.Token)
		}
	}
	if err != nil {
		code, message, status := RetryActivityUnavailable, "Retry activity is unavailable. Try again later.", http.StatusServiceUnavailable
		var actionErr *RetryActivityError
		if errors.As(err, &actionErr) && actionErr != nil {
			switch actionErr.Code {
			case RetryActionStale:
				code, message, status = RetryActionStale, "The retry changed. Refresh its status before trying again.", http.StatusConflict
			case RetryReloadRequired:
				code, message, status = RetryReloadRequired, "Reload the page to use the current retry controls.", http.StatusConflict
			}
		}
		writeJSON(response, status, map[string]string{"code": code, "error": message})
		return
	}
	if !validRetryActivityStatus(state) {
		handler.serverError(response, request, errors.New("retry activity provider returned invalid status"))
		return
	}
	writeJSON(response, http.StatusOK, state)
}

func validRetryActivityStatus(state RetryActivityStatus) bool {
	if state.ServerNowMS <= 0 || state.ServerNowMS > maxSafeJSONInteger {
		return false
	}
	a := state.Activity
	if a == nil {
		return true
	}
	quality := func(value string) bool { return slices.Contains([]string{"exact", "partial", "unavailable"}, value) }
	metric := func(value *int64) bool { return value == nil || *value >= 0 && *value <= maxSafeJSONInteger }
	timestamp := func(value *int64) bool { return value == nil || *value > 0 && *value <= maxSafeJSONInteger }
	if !retryActivityIdentity.MatchString(a.ChainKey) ||
		!slices.Contains([]string{"capacity", "cybersecurity"}, a.Class) ||
		!slices.Contains([]string{"scheduled", "submitting", "reconciling", "running", "terminal"}, a.Phase) ||
		!quality(a.CountQuality) || !quality(a.TimingQuality) || !metric(a.ConfirmedStarts) ||
		(a.Unconfirmed != 0 && a.Unconfirmed != 1) || !metric(a.ElapsedMS) ||
		!timestamp(a.StartedAtMS) || !timestamp(a.FinishedAtMS) || !timestamp(a.NextAttemptAtMS) ||
		len(a.Reason) > 1024 || !utf8.ValidString(a.Reason) {
		return false
	}
	if (a.CountQuality == "unavailable") != (a.ConfirmedStarts == nil) ||
		(a.TimingQuality == "unavailable") != (a.StartedAtMS == nil && a.ElapsedMS == nil) ||
		(a.StartedAtMS == nil) != (a.ElapsedMS == nil) {
		return false
	}
	if a.Phase == "terminal" {
		if !slices.Contains([]string{"success", "stopped", "exhausted", "superseded", "blocked"}, a.Outcome) ||
			a.NextAttemptAtMS != nil || a.RetryNowToken != "" || a.StopToken != "" {
			return false
		}
	} else if a.Outcome != "" || a.FinishedAtMS != nil {
		return false
	}
	if a.RetryNowToken != "" && (a.Phase != "scheduled" || !retryActivityIdentity.MatchString(a.RetryNowToken)) {
		return false
	}
	if a.StopToken != "" && !retryActivityIdentity.MatchString(a.StopToken) {
		return false
	}
	return a.NextAttemptAtMS == nil || a.Phase == "scheduled"
}

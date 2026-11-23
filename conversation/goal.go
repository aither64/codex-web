package conversation

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/aither64/codex-web/conversation/backend"
)

// Allow all fields to use JSON escapes, while keeping the goal body bounded
// independently of the (typically larger) conversation message limit.
const maxGoalBodyBytes = 32 * 1024

func (handler *Handler) goal(response http.ResponseWriter, request *http.Request, target BackendTarget) {
	write := request.Method == http.MethodPost
	if write && !target.Capabilities.GoalWrite || !write && !target.Capabilities.GoalRead {
		handler.rejectOperation(response, request)
		return
	}
	if write && target.Operations.GoalWrite == nil || !write && target.Operations.GoalRead == nil {
		writeGoalError(response, http.StatusNotImplemented, "goal_unsupported", "goal operation is unsupported")
		return
	}
	if target.Identity.Generation == "" {
		handler.goalUnavailable(response, request, errors.New("goal target has no generation"))
		return
	}
	if request.URL.RawQuery != "" || !write && request.ContentLength != 0 {
		writeError(response, http.StatusBadRequest, "goal request does not accept query parameters or a read body")
		return
	}

	ctx := request.Context()
	var snapshot backend.GoalSnapshot
	var err error
	if write {
		body, readErr := io.ReadAll(http.MaxBytesReader(response, request.Body, min(handler.maxBodyBytes, maxGoalBodyBytes)))
		var limitErr *http.MaxBytesError
		if errors.As(readErr, &limitErr) {
			writeError(response, http.StatusRequestEntityTooLarge, "goal request body is too large")
			return
		}
		mutation, valid := decodeGoalMutation(body)
		if readErr != nil || !valid {
			writeError(response, http.StatusBadRequest, "goal mutation is invalid")
			return
		}
		if ctx.Err() != nil {
			handler.goalUnavailable(response, request, ctx.Err())
			return
		}
		// The owner checks authority, replay, and the expected token together
		// under mutation admission; a pre-read here could reject a valid retry.
		snapshot, err = target.Operations.GoalWrite.MutateGoal(ctx, target.Identity, mutation)
	} else {
		snapshot, err = target.Operations.GoalRead.ReadGoal(ctx, target.Identity)
	}

	conflict := write && errors.Is(err, backend.ErrGoalConflict)
	if err != nil && !conflict {
		if errors.Is(err, backend.ErrGoalUnsupported) {
			writeGoalError(response, http.StatusNotImplemented, "goal_unsupported", "goal operation is unsupported")
		} else {
			handler.goalUnavailable(response, request, err)
		}
		return
	}
	if snapshot.ConversationID != target.Identity.ConversationID || snapshot.Generation != target.Identity.Generation ||
		!validGoalChangeToken(snapshot.ChangeToken) {
		handler.goalUnavailable(response, request, errors.New("goal operation returned an invalid snapshot"))
		return
	}
	if snapshot.AllowedActions == nil || !target.Capabilities.GoalWrite || target.Operations.GoalWrite == nil {
		snapshot.AllowedActions = []backend.GoalAction{}
	}
	if conflict {
		writeJSON(response, http.StatusConflict, struct {
			Code     string               `json:"code"`
			Error    string               `json:"error"`
			Snapshot backend.GoalSnapshot `json:"snapshot"`
		}{Code: "goal_conflict", Error: "goal mutation conflicts with current state", Snapshot: snapshot})
		return
	}
	writeJSON(response, http.StatusOK, snapshot)
}

// Decode exact, unique string fields; a case-folded/duplicate name or null
// must not silently change which mutation the adapter admits or replays.
func decodeGoalMutation(body []byte) (backend.GoalMutation, bool) {
	var mutation backend.GoalMutation
	if !utf8.Valid(body) {
		return mutation, false
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	if first, err := decoder.Token(); err != nil || first != json.Delim('{') {
		return mutation, false
	}
	var action string
	fields := map[string]*string{
		"action": &action, "operationId": &mutation.OperationID,
		"expectedChangeToken": &mutation.ExpectedChangeToken, "objective": &mutation.Objective,
	}
	for decoder.More() {
		key, err := decoder.Token()
		name, ok := key.(string)
		if err != nil || !ok || fields[name] == nil {
			return mutation, false
		}
		value, err := decoder.Token()
		text, ok := value.(string)
		if err != nil || !ok {
			return mutation, false
		}
		*fields[name] = text
		delete(fields, name)
	}
	if last, err := decoder.Token(); err != nil || last != json.Delim('}') {
		return mutation, false
	}
	if _, err := decoder.Token(); err != io.EOF {
		return mutation, false
	}
	if !browserAttemptPattern.MatchString(mutation.OperationID) || !validGoalChangeToken(mutation.ExpectedChangeToken) {
		return mutation, false
	}
	mutation.Action = backend.GoalAction(action)
	_, objectiveAbsent := fields["objective"]
	switch mutation.Action {
	case backend.GoalEdit:
		return mutation, !objectiveAbsent && len(mutation.Objective) <= backend.MaxGoalObjectiveBytes &&
			strings.TrimSpace(mutation.Objective) != "" && !strings.ContainsRune(mutation.Objective, '\x00')
	case backend.GoalEnable, backend.GoalDisable, backend.GoalPause, backend.GoalResume:
		return mutation, objectiveAbsent
	default:
		return mutation, false
	}
}

func validGoalChangeToken(token string) bool {
	return token != "" && len(token) <= backend.MaxGoalChangeTokenBytes && utf8.ValidString(token)
}

func (handler *Handler) goalUnavailable(response http.ResponseWriter, request *http.Request, err error) {
	if !errors.Is(err, backend.ErrGoalUnavailable) {
		handler.logError(request, err)
	}
	writeGoalError(response, http.StatusServiceUnavailable, "goal_unavailable", "goal operation is unavailable")
}

func writeGoalError(response http.ResponseWriter, status int, code, message string) {
	writeJSON(response, status, map[string]string{"error": message, "code": code})
}

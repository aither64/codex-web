package backend

import "errors"

// GoalStatus is the shared view, not the status of a native RPC or assurance
// that the objective has been independently verified.
type GoalStatus string

const (
	GoalActive        GoalStatus = "active"
	GoalPaused        GoalStatus = "paused"
	GoalBlocked       GoalStatus = "blocked"
	GoalDone          GoalStatus = "done"
	GoalUsageLimited  GoalStatus = "usageLimited"
	GoalBudgetLimited GoalStatus = "budgetLimited"
)

type GoalAction string

const (
	GoalEnable  GoalAction = "enable"
	GoalDisable GoalAction = "disable"
	GoalEdit    GoalAction = "edit"
	GoalPause   GoalAction = "pause"
	GoalResume  GoalAction = "resume"
)

const (
	MaxGoalObjectiveBytes   = 4 * 1024
	MaxGoalChangeTokenBytes = 256
)

// GoalSnapshot scopes a passive read or mutation result to the resolved
// conversation and execution generation. Those routing fields stay server-side.
// A nil Goal means there is no goal, not that the operation is unsupported.
// AllowedActions is presentation only, never authority to mutate.
type GoalSnapshot struct {
	ConversationID string       `json:"-"`
	Generation     string       `json:"-"`
	ChangeToken    string       `json:"changeToken"`
	Enabled        bool         `json:"enabled"`
	Goal           *Goal        `json:"goal,omitempty"`
	AllowedActions []GoalAction `json:"allowedActions"`
}

type Goal struct {
	Objective          string     `json:"objective"`
	Status             GoalStatus `json:"status"`
	Progress           string     `json:"progress,omitempty"`
	Next               string     `json:"next,omitempty"`
	Reason             string     `json:"reason,omitempty"`
	PendingApplication *bool      `json:"pendingApplication,omitempty"`
}

// GoalMutation is an operator request, not a grant of execution authority.
// Enable is passive and prospective; Disable revokes. Edit only corrects an
// existing goal and never resumes it. The domain owner must compare the exact
// change token for a new operation, reconcile an identical operation-ID retry
// even when its success advanced the token, and reject reuse with a changed
// payload. The common HTTP handler has no replay or goal state of its own.
type GoalMutation struct {
	Action              GoalAction `json:"action"`
	OperationID         string     `json:"operationId"`
	ExpectedChangeToken string     `json:"expectedChangeToken"`
	Objective           string     `json:"objective,omitempty"`
}

var (
	// ErrGoalConflict reports stale state or an operation ID reused with a
	// different payload. Return the latest authorized snapshot alongside it.
	ErrGoalConflict = errors.New("goal mutation conflicts with current state")
	// ErrGoalUnsupported is a definite lack of support, not an absent goal.
	ErrGoalUnsupported = errors.New("goal operation is unsupported")
	// ErrGoalUnavailable reports that an implemented operation is unavailable.
	ErrGoalUnavailable = errors.New("goal operation is unavailable")
)

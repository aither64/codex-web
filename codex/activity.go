package codex

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// ActivitySnapshot describes root-thread elapsed time, not CPU or model time.
// WaitingMS contains closed observed waits. The open wait is separate; working
// time includes observation through ObservedAtMS. A UI may project an open state
// from ObservedAtMS, but must not persist that browser projection as coverage.
type ActivitySnapshot struct {
	ThreadID          string `json:"threadId"`
	CurrentTurnID     string `json:"currentTurnId,omitempty"`
	Messages          int    `json:"messages"`
	ToolCalls         int    `json:"toolCalls"`
	StartedAtMS       int64  `json:"startedAtMs,omitempty"`
	CompletedAtMS     int64  `json:"completedAtMs,omitempty"`
	WorkingMS         int64  `json:"workingMs"`
	WaitingMS         int64  `json:"waitingMs"`
	OpenWaitingMS     int64  `json:"openWaitingMs"`
	BetweenTurnsMS    int64  `json:"betweenTurnsMs"`
	UnclassifiedMS    int64  `json:"unclassifiedMs"`
	ObservedAtMS      int64  `json:"observedAtMs"`
	CoverageStartMS   int64  `json:"coverageStartMs,omitempty"`
	CoverageComplete  bool   `json:"coverageComplete"`
	CoverageReason    string `json:"coverageReason,omitempty"`
	CurrentState      string `json:"currentState"`
	StateSinceMS      int64  `json:"stateSinceMs,omitempty"`
	Scope             string `json:"scope"`
	TimingApproximate bool   `json:"timingApproximate"`
}

// A turn keeps aggregate elapsed time and the current second separately. Server
// turn boundaries have second precision, so completion can discard that partial
// second without retaining every request or work/wait transition.
type activityTurnRecord struct {
	ID            string `json:"id"`
	Sequence      uint64 `json:"sequence"`
	StartedAtMS   int64  `json:"startedAtMs,omitempty"`
	CompletedAtMS int64  `json:"completedAtMs,omitempty"`
	WorkingMS     int64  `json:"workingMs"`
	WaitingMS     int64  `json:"waitingMs"`
	FirstMS       int64  `json:"firstMs,omitempty"`
	ThroughMS     int64  `json:"throughMs,omitempty"`
	TailSecondMS  int64  `json:"tailSecondMs,omitempty"`
	TailWorkMS    int64  `json:"tailWorkMs,omitempty"`
	TailWaitMS    int64  `json:"tailWaitMs,omitempty"`
	Final         bool   `json:"final,omitempty"`
	BoundsUnknown bool   `json:"boundsUnknown,omitempty"`
}

type activityRequest struct {
	TurnID     string `json:"turnId,omitempty"`
	Blocking   bool   `json:"blocking"`
	Category   string `json:"category"`
	OpenedAtMS int64  `json:"openedAtMs"`
}

type activityObservation struct {
	Connection    string                     `json:"connection"`
	Connected     bool                       `json:"-"`
	Ready         bool                       `json:"-"`
	TurnID        string                     `json:"turnId,omitempty"`
	Active        bool                       `json:"active"`
	Flags         []string                   `json:"-"`
	Requests      map[string]activityRequest `json:"requests,omitempty"`
	State         string                     `json:"state"`
	SinceMS       int64                      `json:"sinceMs"`
	ThroughMS     int64                      `json:"throughMs"`
	OpenWaitingMS int64                      `json:"openWaitingMs"`
	Revision      uint64                     `json:"-"`
	Overflow      bool                       `json:"overflow,omitempty"`
}

func activityState(observation *activityObservation) string {
	if observation.Overflow {
		return "unclassified"
	}
	for _, request := range observation.Requests {
		if request.Category == "unknown" {
			return "unclassified"
		}
	}
	for _, request := range observation.Requests {
		if request.Blocking {
			return "waiting"
		}
	}
	if !observation.Ready {
		return "unclassified"
	}
	for _, flag := range observation.Flags {
		category := ""
		if flag == "waitingOnApproval" {
			category = "approval"
		}
		if flag == "waitingOnUserInput" {
			category = "userInput"
		}
		if category == "" {
			continue
		}
		found := false
		for _, request := range observation.Requests {
			if request.Category == category {
				found = true
			}
		}
		if !found {
			return "unclassified"
		}
	}
	if observation.Active {
		return "working"
	}
	return "idle"
}

func randomActivityNamespace() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(value[:])
}

func (c *Client) activityConnection(generation uint64) string {
	return fmt.Sprintf("%s:%d", c.activityNamespace, generation)
}

// ReadActivity is optional and safe on a read-only client without a recorder.
// All history is paginated; absent observation stays unclassified. Calling it
// does not subscribe, resume, answer requests or alter thread settings.
func (c *Client) ReadActivity(ctx context.Context, threadID string) (ActivitySnapshot, error) {
	history := c.retainTurnHistory(threadID, false)
	defer c.releaseTurnHistory(threadID, false)
	if err := c.options.ActivityRecorder.retain(ctx, threadID, false); err != nil {
		return ActivitySnapshot{}, err
	}
	defer c.options.ActivityRecorder.release(threadID, false)
	revision := c.options.ActivityRecorder.revision(threadID)
	var metadata struct {
		Thread map[string]any `json:"thread"`
	}
	if err := c.Request(ctx, "thread/read", map[string]any{"threadId": threadID, "excludeTurns": true}, &metadata); err != nil {
		return ActivitySnapshot{}, err
	}
	if metadata.Thread == nil || stringValue(metadata.Thread["id"]) != threadID {
		return ActivitySnapshot{}, errors.New("thread/read returned invalid activity identity")
	}
	rawTurns, err := c.readTurnHistory(ctx, threadID, "full", stringValue(metadata.Thread["path"]), history)
	if err != nil && !c.freshThreadMissingSourceRollout(metadata.Thread, threadID, err) {
		return ActivitySnapshot{}, err
	}
	turns := make([]TurnMetadata, 0, len(rawTurns))
	for _, turn := range rawTurns {
		turns = append(turns, turnMetadata(turn, threadID))
	}
	turns, scope := c.ownActivityTurns(ctx, metadata.Thread, turns)
	c.connectionMu.Lock()
	generation := c.generation
	c.connectionMu.Unlock()
	now := time.Now().UnixMilli()
	status, _ := metadata.Thread["status"].(map[string]any)
	c.options.ActivityRecorder.seedCurrent(ctx, threadID, turns)
	c.options.ActivityRecorder.reconcile(threadID, c.activityConnection(generation), turns, status, now, revision)
	checkpointErr := c.options.ActivityRecorder.flush(ctx, threadID)
	return c.options.ActivityRecorder.snapshotContext(ctx, threadID, turns, scope, now, checkpointErr), nil
}

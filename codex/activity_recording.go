package codex

import (
	"encoding/json"
	"errors"
	"slices"
)

func (thread *activityThread) observation(connection string, now int64) *activityObservation {
	if thread.live == nil || thread.live.Connection != connection {
		thread.live = &activityObservation{Connection: connection, Requests: map[string]activityRequest{}, SinceMS: now, ThroughMS: now, State: "unclassified"}
	}
	return thread.live
}

func (thread *activityThread) accrue(now int64) {
	live := thread.live
	if live == nil || !live.Connected {
		return
	}
	now = max(now, live.ThroughMS)
	if live.Active && live.TurnID != "" && (live.State == "working" || live.State == "waiting") {
		record, exists := thread.hot.Turns[live.TurnID]
		if !exists && len(thread.hot.Turns) >= activityPendingLimit {
			thread.lastError = errors.New("activity summaries are waiting for storage")
			live.Ready = false
			live.State = "unclassified"
		} else if !record.Final {
			record.ID = live.TurnID
			from := max(live.ThroughMS, record.StartedAtMS)
			if now > from {
				record.add(live.State, from, now)
				if live.State == "waiting" {
					live.OpenWaitingMS += now - from
				}
			}
			thread.hot.Turns[live.TurnID] = record
		}
	}
	live.ThroughMS = now
}

func (record *activityTurnRecord) add(state string, from, to int64) {
	record.Sequence++
	if record.FirstMS == 0 {
		record.FirstMS = from
	}
	record.ThroughMS = to
	if state == "working" {
		record.WorkingMS += to - from
	} else {
		record.WaitingMS += to - from
	}
	second := to / 1000 * 1000
	if second != record.TailSecondMS {
		record.TailSecondMS, record.TailWorkMS, record.TailWaitMS = second, 0, 0
	}
	tail := to - max(from, second)
	if state == "working" {
		record.TailWorkMS += tail
	} else {
		record.TailWaitMS += tail
	}
}

func (record *activityTurnRecord) applyMetadata(turn TurnMetadata) {
	if record.Final {
		return
	}
	record.Sequence++
	record.ID = turn.ID
	if turn.StartedAtMS > 0 {
		record.StartedAtMS = turn.StartedAtMS
	}
	if !slices.Contains([]string{"completed", "failed", "interrupted"}, turn.Status) || turn.CompletedAtMS == 0 {
		return
	}
	record.CompletedAtMS = turn.CompletedAtMS
	if record.ThroughMS > turn.CompletedAtMS {
		if record.TailSecondMS == turn.CompletedAtMS {
			record.WorkingMS -= record.TailWorkMS
			record.WaitingMS -= record.TailWaitMS
			record.ThroughMS = turn.CompletedAtMS
			record.TailWorkMS, record.TailWaitMS = 0, 0
			if record.WorkingMS+record.WaitingMS == 0 {
				record.FirstMS = record.ThroughMS
			}
		} else {
			record.BoundsUnknown = true
		}
	}
	if record.FirstMS > 0 && record.FirstMS < record.StartedAtMS {
		record.BoundsUnknown = true
	}
	record.Final = record.StartedAtMS > 0
}

func (thread *activityThread) updateState(now int64) {
	live := thread.live
	state := activityState(live)
	if state != live.State {
		live.OpenWaitingMS = 0
		live.State, live.SinceMS = state, now
	}
	live.ThroughMS = max(live.ThroughMS, now)
	thread.hot.Current = cloneObservation(live)
	thread.signalLocked()
}

func (r *ActivityRecorder) connected(threadID, connection string, now int64) {
	thread := r.lookup(threadID)
	if thread == nil {
		return
	}
	thread.mu.Lock()
	defer thread.mu.Unlock()
	if thread.closing {
		return
	}
	live := thread.observation(connection, now)
	live.Connected = true
	thread.updateState(now)
}

func (r *ActivityRecorder) disconnected(connection, threadID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	threads := make([]*activityThread, 0, len(r.threads))
	for id, thread := range r.threads {
		if threadID == "" || id == threadID {
			threads = append(threads, thread)
		}
	}
	r.mu.Unlock()
	for _, thread := range threads {
		thread.mu.Lock()
		if thread.live != nil && thread.live.Connection == connection {
			// Do not accrue elapsed time merely because a disconnection was
			// detected. Only already observed checkpoints may become durable.
			thread.live = nil
			thread.hot.Current = nil
			thread.signalLocked()
		}
		thread.mu.Unlock()
	}
}

func (r *ActivityRecorder) observe(connection string, message rpcMessage, now int64) {
	if r == nil || message.Method == "" {
		return
	}
	var params struct {
		ThreadID   string          `json:"threadId"`
		TurnID     string          `json:"turnId"`
		IsBlocking *bool           `json:"isBlocking"`
		RequestID  json.RawMessage `json:"requestId"`
		Turn       map[string]any  `json:"turn"`
		Status     struct {
			Type        string   `json:"type"`
			ActiveFlags []string `json:"activeFlags"`
		} `json:"status"`
	}
	if json.Unmarshal(message.Params, &params) != nil || params.ThreadID == "" {
		return
	}
	switch message.Method {
	case "serverRequest/resolved", "turn/started", "turn/completed", "thread/status/changed":
	default:
		if len(message.ID) == 0 {
			return
		}
	}
	thread := r.lookup(params.ThreadID)
	if thread == nil {
		return
	}
	select {
	case <-thread.loaded:
	default:
		return
	}
	thread.mu.Lock()
	defer thread.mu.Unlock()
	if thread.closing {
		return
	}
	live := thread.observation(connection, now)
	boundary := now
	if message.Method == "turn/started" && live.Connected && live.Ready && live.State == "idle" {
		startedAt := integerValue(params.Turn["startedAt"]) * 1000
		if startedAt > 0 && startedAt <= now {
			boundary = max(startedAt, live.ThroughMS)
		}
	}
	if message.Method == "turn/completed" && live.TurnID == stringValue(params.Turn["id"]) {
		if end := integerValue(params.Turn["completedAt"]) * 1000; end > 0 && end < boundary {
			boundary = max(end, live.ThroughMS)
		}
	}
	thread.accrue(boundary)
	switch message.Method {
	case "serverRequest/resolved":
		resolved := live.Requests[string(params.RequestID)]
		delete(live.Requests, string(params.RequestID))
		remaining := false
		for _, request := range live.Requests {
			if request.Category == resolved.Category {
				remaining = true
			}
		}
		if !remaining && resolved.Category != "" {
			flag := "waitingOnApproval"
			if resolved.Category == "userInput" {
				flag = "waitingOnUserInput"
			}
			live.Flags = slices.DeleteFunc(live.Flags, func(value string) bool { return value == flag })
		}
	case "turn/started":
		live.TurnID = stringValue(params.Turn["id"])
		live.OpenWaitingMS = 0
		live.Active, live.Ready = true, true
		if record, exists := thread.hot.Turns[live.TurnID]; exists || len(thread.hot.Turns) < activityPendingLimit {
			record.applyMetadata(turnMetadata(params.Turn, params.ThreadID))
			thread.hot.Turns[live.TurnID] = record
		}
	case "turn/completed":
		turnID := stringValue(params.Turn["id"])
		if live.TurnID == turnID {
			live.Active = false
			live.OpenWaitingMS = 0
		}
		for key, request := range live.Requests {
			if request.TurnID == turnID {
				delete(live.Requests, key)
			}
		}
		live.Flags = nil
		if record, exists := thread.hot.Turns[turnID]; exists {
			record.applyMetadata(turnMetadata(params.Turn, params.ThreadID))
			thread.hot.Turns[turnID] = record
		}
	case "thread/status/changed":
		live.Flags = params.Status.ActiveFlags
		if params.Status.Type != "active" {
			live.Active = false
		}
	default:
		category, blocking := "unknown", false
		switch message.Method {
		case "item/commandExecution/requestApproval", "item/fileChange/requestApproval", "item/permissions/requestApproval", "mcpServer/elicitation/request":
			category, blocking = "approval", true
		case "item/tool/requestUserInput":
			category, blocking = "userInput", true
			if params.IsBlocking != nil {
				blocking = *params.IsBlocking
			}
		}
		id := string(message.ID)
		if _, exists := live.Requests[id]; !exists && (len(live.Requests) >= activityPendingLimit || len(id) > 256) {
			live.Ready = false
			live.Overflow = true
		} else if _, exists := live.Requests[id]; !exists {
			live.Requests[id] = activityRequest{TurnID: params.TurnID, Blocking: blocking, Category: category, OpenedAtMS: now}
		}
	}
	live.Revision++
	thread.updateState(boundary)
	if boundary < now {
		thread.accrue(now)
		thread.hot.Current = cloneObservation(live)
	}
}

func (r *ActivityRecorder) revision(threadID string) uint64 {
	thread := r.lookup(threadID)
	if thread == nil {
		return 0
	}
	thread.mu.Lock()
	defer thread.mu.Unlock()
	if thread.live != nil {
		return thread.live.Revision
	}
	return 0
}

func (r *ActivityRecorder) reconcile(threadID, connection string, turns []TurnMetadata, status map[string]any, now int64, revision uint64) {
	thread := r.lookup(threadID)
	if thread == nil {
		return
	}
	// Building the authoritative index is linear and occurs outside event capture.
	metadata := make(map[string]TurnMetadata, len(turns))
	for _, turn := range turns {
		metadata[turn.ID] = turn
	}
	thread.mu.Lock()
	defer thread.mu.Unlock()
	live := thread.live
	if live == nil || live.Connection != connection || !live.Connected || thread.closing {
		return
	}
	boundary := now
	if turn, exists := metadata[live.TurnID]; exists && turn.CompletedAtMS > 0 && turn.CompletedAtMS < boundary {
		boundary = max(turn.CompletedAtMS, live.ThroughMS)
	}
	thread.accrue(boundary)
	if live.Revision == revision {
		live.Ready = true
		live.Flags = stringValues(status["activeFlags"])
		previousTurn := live.TurnID
		live.Active, live.TurnID = false, ""
		if len(turns) > 0 {
			latest := turns[len(turns)-1]
			live.TurnID, live.Active = latest.ID, latest.Status == "inProgress"
		}
		if previousTurn != live.TurnID {
			live.OpenWaitingMS = 0
		}
		for id, record := range thread.hot.Turns {
			if turn, exists := metadata[id]; exists {
				record.applyMetadata(turn)
			} else {
				// Reverted or otherwise removed turns retain their own durable totals,
				// but no longer occupy current-state storage or contribute to this view.
				record.Final = true
				record.Sequence++
			}
			thread.hot.Turns[id] = record
		}
		if turn, exists := metadata[live.TurnID]; exists {
			if record, present := thread.hot.Turns[live.TurnID]; present || len(thread.hot.Turns) < activityPendingLimit {
				record.applyMetadata(turn)
				// Do not repeatedly recreate summaries for old idle turns.
				if present || live.Active {
					thread.hot.Turns[live.TurnID] = record
				}
			}
		}
		for key, request := range live.Requests {
			if turn, exists := metadata[request.TurnID]; exists && slices.Contains([]string{"completed", "failed", "interrupted"}, turn.Status) {
				delete(live.Requests, key)
			}
		}
	}
	thread.updateState(now)
}

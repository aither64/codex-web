package codex

import "context"

func (r *ActivityRecorder) seedCurrent(ctx context.Context, threadID string, turns []TurnMetadata) {
	thread := r.lookup(threadID)
	if thread == nil || len(turns) == 0 {
		return
	}
	latest := turns[len(turns)-1]
	if latest.Status != "inProgress" {
		return
	}
	saved, exists := thread.summary(ctx, latest.ID)
	if !exists {
		return
	}
	thread.mu.Lock()
	defer thread.mu.Unlock()
	current, present := thread.hot.Turns[latest.ID]
	if present && current.FirstMS > 0 && current.FirstMS <= saved.ThroughMS {
		return
	}
	if !present && len(thread.hot.Turns) >= activityPendingLimit {
		return
	}
	if current.WorkingMS+current.WaitingMS > 0 {
		saved.WorkingMS += current.WorkingMS
		saved.WaitingMS += current.WaitingMS
		saved.ThroughMS = current.ThroughMS
		saved.TailSecondMS, saved.TailWorkMS, saved.TailWaitMS = current.TailSecondMS, current.TailWorkMS, current.TailWaitMS
	}
	saved.Final = false
	saved.Sequence = max(saved.Sequence, current.Sequence) + 1
	thread.hot.Turns[latest.ID] = saved
}

func (r *ActivityRecorder) snapshotContext(ctx context.Context, threadID string, turns []TurnMetadata, scope string, now int64, checkpointErr error) ActivitySnapshot {
	result := ActivitySnapshot{ThreadID: threadID, CurrentState: "idle", ObservedAtMS: now, Scope: scope, TimingApproximate: true, CoverageComplete: true}
	if len(turns) > 0 {
		latest := turns[len(turns)-1]
		result.CurrentTurnID, result.Messages, result.ToolCalls = latest.ID, latest.Messages, latest.ToolCalls
		result.StartedAtMS, result.CompletedAtMS = latest.StartedAtMS, latest.CompletedAtMS
		if latest.Status == "inProgress" {
			result.CurrentState = "unclassified"
		}
	}
	var checkpoint activityCheckpoint
	var live *activityObservation
	thread := r.lookup(threadID)
	if thread != nil {
		// Only bounded current state is copied here. Historical I/O and the
		// O(turns) aggregation below never hold an event-capture mutex.
		thread.mu.Lock()
		checkpoint = cloneActivityCheckpoint(thread.durable)
		if checkpointErr == nil {
			checkpointErr = thread.lastError
		}
		if thread.live != nil && thread.live.Connected && checkpoint.Current != nil &&
			thread.live.Connection == checkpoint.Current.Connection {
			live = checkpoint.Current
		}
		thread.mu.Unlock()
	}
	if checkpointErr != nil {
		result.CoverageReason = "Activity recording is unavailable."
		result.CoverageComplete = false
		live = nil
	}
	if scope == "unknown" || scope == "sinceFork" && len(turns) == 0 {
		live = nil
	}
	if live != nil {
		result.CurrentState, result.StateSinceMS, result.ObservedAtMS = live.State, live.SinceMS, live.ThroughMS
	}
	if scope == "unknown" {
		result.CurrentState = "unclassified"
	}
	for index, turn := range turns {
		start, end := turn.StartedAtMS, turn.CompletedAtMS
		if index > 0 {
			previous := turns[index-1].CompletedAtMS
			if previous > 0 && start > previous {
				result.BetweenTurnsMS += start - previous
			}
		}
		if end == 0 && turn.Status == "inProgress" {
			end = now
		}
		if start == 0 || end < start || end == 0 {
			result.CoverageComplete = false
			continue
		}
		record, exists := checkpoint.Turns[turn.ID]
		if thread != nil {
			if historical, ok := thread.summary(ctx, turn.ID); ok && (!exists || historical.Sequence > record.Sequence) {
				record, exists = historical, true
			}
		}
		known := int64(0)
		if exists && !record.BoundsUnknown && record.FirstMS >= start && record.ThroughMS <= end &&
			(record.StartedAtMS == 0 || record.StartedAtMS == start) &&
			(record.CompletedAtMS == 0 || record.CompletedAtMS == turn.CompletedAtMS) {
			known = record.WorkingMS + record.WaitingMS
			if known > end-start {
				known = 0
			} else {
				result.WorkingMS += record.WorkingMS
				open := int64(0)
				if live != nil && live.TurnID == turn.ID && live.State == "waiting" && live.Active {
					open = min(live.OpenWaitingMS, record.WaitingMS)
				}
				result.WaitingMS += record.WaitingMS - open
				result.OpenWaitingMS += open
				if record.FirstMS > 0 && (result.CoverageStartMS == 0 || record.FirstMS < result.CoverageStartMS) {
					result.CoverageStartMS = record.FirstMS
				}
			}
		}
		result.UnclassifiedMS += end - start - known
	}
	if result.CurrentState == "waiting" && result.OpenWaitingMS == 0 && live != nil {
		result.OpenWaitingMS = max(int64(0), result.ObservedAtMS-live.SinceMS)
	}
	if result.CurrentState == "idle" && len(turns) > 0 && result.CompletedAtMS > 0 {
		result.StateSinceMS = result.CompletedAtMS
		result.OpenWaitingMS = max(int64(0), now-result.CompletedAtMS)
	}
	if result.UnclassifiedMS > 0 || scope == "unknown" {
		result.CoverageComplete = false
	}
	if !result.CoverageComplete && result.CoverageReason == "" {
		if scope == "unknown" {
			result.CoverageReason = "The fork boundary could not be verified."
		} else {
			result.CoverageReason = "Some turn time was not observed."
		}
	}
	return result
}

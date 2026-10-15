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
	return r.snapshotContextCached(ctx, threadID, turns, scope, now, checkpointErr, nil)
}

type activityPrefix struct {
	count              int
	first, last, scope string
	totals             ActivitySnapshot
}

func (result *ActivitySnapshot) addHistory(value ActivitySnapshot) {
	result.SentMessages += value.SentMessages
	result.ReceivedMessages += value.ReceivedMessages
	result.TotalToolCalls += value.TotalToolCalls
	result.WorkingMS += value.WorkingMS
	result.WaitingMS += value.WaitingMS
	result.BetweenTurnsMS += value.BetweenTurnsMS
	result.UnclassifiedMS += value.UnclassifiedMS
	result.CountsComplete = result.CountsComplete && value.CountsComplete
	result.CoverageComplete = result.CoverageComplete && value.CoverageComplete
	if value.CoverageStartMS > 0 && (result.CoverageStartMS == 0 || value.CoverageStartMS < result.CoverageStartMS) {
		result.CoverageStartMS = value.CoverageStartMS
	}
}

func (r *ActivityRecorder) snapshotContextCached(ctx context.Context, threadID string, turns []TurnMetadata, scope string, now int64, checkpointErr error, prefix *activityPrefix) ActivitySnapshot {
	result := ActivitySnapshot{ThreadID: threadID, CurrentState: "idle", ObservedAtMS: now, Scope: scope, TimingApproximate: true, CoverageComplete: true, CountsComplete: scope != "unknown"}
	if len(turns) > 0 {
		latest := turns[len(turns)-1]
		result.CurrentTurnID, result.Messages, result.ToolCalls = latest.ID, latest.Messages, latest.ToolCalls
		result.LatestTurnStatus = latest.Status
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
		if live.State == "waiting" {
			for _, wait := range live.ToolWaits {
				if result.WaitReason == "" || wait.Reason == "subagents" {
					result.WaitReason, result.WaitUntilMS = wait.Reason, wait.UntilMS
				}
			}
			for _, request := range live.Requests {
				if request.Blocking {
					result.WaitReason, result.WaitUntilMS = request.Category, 0
					if request.Category == "userInput" {
						break
					}
				}
			}
		}
	}
	if scope == "unknown" {
		result.CurrentState = "unclassified"
	}
	firstIndex := 0
	if prefix != nil {
		if prefix.count > 0 && (prefix.scope != scope || prefix.count > len(turns) || turns[0].ID != prefix.first || turns[prefix.count-1].ID != prefix.last) {
			*prefix = activityPrefix{}
		}
		if prefix.count > 0 {
			result.addHistory(prefix.totals)
			firstIndex = prefix.count
		}
	}
	for index := firstIndex; index < len(turns); index++ {
		turn := turns[index]
		if prefix != nil && checkpointErr == nil && ctx.Err() != nil {
			result.CoverageComplete = false
			result.CountsComplete = false
			break
		}
		result.SentMessages += turn.Messages
		result.ReceivedMessages += turn.ReceivedMessages
		result.TotalToolCalls += turn.ToolCalls
		result.CountsComplete = result.CountsComplete && turn.CountsKnown
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
		// flush above settled historical writes. Keep the newest two turns mutable;
		// older finalized summaries can be folded without reopening every shard.
		if prefix != nil && checkpointErr == nil && prefix.count == index && index < len(turns)-2 && turn.CountsKnown && turn.CompletedAtMS > 0 && turn.Status != "inProgress" {
			prefix.count = index + 1
			prefix.first = turns[0].ID
			prefix.last = turn.ID
			prefix.scope = scope
			prefix.totals = result
		}
	}
	if result.CurrentState == "waiting" && result.OpenWaitingMS == 0 && live != nil {
		result.OpenWaitingMS = max(int64(0), result.ObservedAtMS-live.SinceMS)
	}
	if result.CurrentState == "idle" && len(turns) > 0 && result.CompletedAtMS > 0 {
		result.StateSinceMS = result.CompletedAtMS
		result.OpenWaitingMS = max(int64(0), now-result.CompletedAtMS)
	}
	result.IdleMS = result.BetweenTurnsMS
	if result.CurrentState == "idle" {
		result.IdleMS += result.OpenWaitingMS
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

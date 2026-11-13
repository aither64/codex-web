package conversation

import (
	"maps"

	"github.com/aither64/codex-web/codex"
	"github.com/aither64/codex-web/conversation/backend"
)

func projectTranscript(value codex.Transcript) backend.Transcript {
	return backend.Transcript{
		ThreadID:          value.ThreadID,
		Status:            value.Status,
		LatestTurnID:      value.LatestTurnID,
		Model:             value.Model,
		ReasoningEffort:   value.ReasoningEffort,
		CollaborationMode: value.CollaborationMode,
		Entries:           projectList(value.Entries, projectTranscriptEntry),
	}
}

func projectTranscriptEntry(value codex.TranscriptEntry) backend.TranscriptEntry {
	return backend.TranscriptEntry{
		Communication:           projectPointer(value.Communication, projectTranscriptCommunication),
		Citations:               projectList(value.Citations, projectCitationSpan),
		CitationsPending:        value.CitationsPending,
		Delivery:                value.Delivery,
		Questions:               projectList(value.Questions, projectAsyncQuestion),
		QuestionReplies:         projectList(value.QuestionReplies, projectAsyncQuestionReply),
		DisplayText:             projectPointer(value.DisplayText, func(v string) string { return v }),
		Attachments:             projectList(value.Attachments, projectAttachment),
		TurnID:                  value.TurnID,
		TurnStatus:              value.TurnStatus,
		ItemID:                  value.ItemID,
		ClientUserMessageID:     value.ClientUserMessageID,
		ClientUserMessageDigest: value.ClientUserMessageDigest,
		Kind:                    value.Kind,
		Summary:                 value.Summary,
		Text:                    value.Text,
		HTML:                    value.HTML,
		Details:                 value.Details,
		Timestamp:               value.Timestamp,
		TimestampApproximate:    value.TimestampApproximate,
		Activity:                projectPointer(value.Activity, projectTranscriptActivity),
	}
}

func projectTranscriptCommunication(value codex.TranscriptCommunication) backend.TranscriptCommunication {
	return backend.TranscriptCommunication{
		Kind: value.Kind,
		From: value.From,
		To:   value.To,
	}
}

func projectAttachment(value codex.Attachment) backend.Attachment {
	return backend.Attachment{
		ID:          value.ID,
		Name:        value.Name,
		Size:        value.Size,
		State:       value.State,
		DownloadURL: value.DownloadURL,
		DeleteURL:   value.DeleteURL,
		References:  projectList(value.References, func(v string) string { return v }),
	}
}

func projectAsyncQuestion(value codex.AsyncQuestion) backend.AsyncQuestion {
	return backend.AsyncQuestion{
		ID:      value.ID,
		Title:   value.Title,
		Options: projectList(value.Options, func(v string) string { return v }),
	}
}

func projectAsyncQuestionReply(value codex.AsyncQuestionReply) backend.AsyncQuestionReply {
	return backend.AsyncQuestionReply{
		QuestionItemID: value.QuestionItemID,
		Question:       value.Question,
		Answer:         value.Answer,
	}
}

func projectCitationSource(value codex.CitationSource) backend.CitationSource {
	return backend.CitationSource{
		URL:    value.URL,
		Title:  value.Title,
		Number: value.Number,
	}
}

func projectCitationSpan(value codex.CitationSpan) backend.CitationSpan {
	return backend.CitationSpan{
		Start:       value.Start,
		End:         value.End,
		Sources:     projectList(value.Sources, projectCitationSource),
		Unavailable: value.Unavailable,
		Incomplete:  value.Incomplete,
	}
}

func projectTranscriptActivity(value codex.TranscriptActivity) backend.TranscriptActivity {
	return backend.TranscriptActivity{
		Action:          value.Action,
		Status:          value.Status,
		Queries:         projectList(value.Queries, func(v string) string { return v }),
		Links:           projectList(value.Links, projectActivityLink),
		Agents:          projectList(value.Agents, projectActivityAgent),
		AgentPath:       value.AgentPath,
		Model:           value.Model,
		ReasoningEffort: value.ReasoningEffort,
	}
}

func projectActivityLink(value codex.ActivityLink) backend.ActivityLink {
	return backend.ActivityLink{
		URL:   value.URL,
		Title: value.Title,
	}
}

func projectActivityAgent(value codex.ActivityAgent) backend.ActivityAgent {
	return backend.ActivityAgent{
		ThreadID: value.ThreadID,
		Status:   value.Status,
		Message:  value.Message,
	}
}

func projectPrompt(value codex.Prompt) backend.Prompt {
	return backend.Prompt{
		ID:                        value.ID,
		Token:                     value.Token,
		Method:                    value.Method,
		Kind:                      value.Kind,
		ThreadID:                  value.ThreadID,
		TurnID:                    value.TurnID,
		ItemID:                    value.ItemID,
		Params:                    maps.Clone(value.Params),
		Item:                      maps.Clone(value.Item),
		AuthorityAvailable:        value.AuthorityAvailable,
		Error:                     value.Error,
		AvailableDecisions:        projectList(value.AvailableDecisions, func(v string) string { return v }),
		Questions:                 projectList(value.Questions, projectQuestion),
		IsBlocking:                value.IsBlocking,
		AutoResolutionMS:          projectPointer(value.AutoResolutionMS, func(v uint64) uint64 { return v }),
		AutoResolutionVisibleAtMS: value.AutoResolutionVisibleAtMS,
		AutoResolutionAtMS:        value.AutoResolutionAtMS,
		AutoResolveSnoozed:        value.AutoResolveSnoozed,
	}
}

func projectQuestion(value codex.Question) backend.Question {
	return backend.Question{
		ID:       value.ID,
		Header:   value.Header,
		Question: value.Question,
		IsSecret: value.IsSecret,
		IsOther:  value.IsOther,
		Options:  projectList(value.Options, projectOption),
	}
}

func projectOption(value codex.Option) backend.Option {
	return backend.Option{
		Label:       value.Label,
		Description: value.Description,
	}
}

func projectPromptResponse(value codex.PromptResponse) backend.PromptResponse {
	return backend.PromptResponse{
		ID:       value.ID,
		Token:    value.Token,
		Decision: value.Decision,
		Answers:  maps.Clone(value.Answers),
		Snooze:   value.Snooze,
	}
}

func projectQueueEntry(value codex.QueueEntry) backend.QueueEntry {
	return backend.QueueEntry{
		DisplayText:         projectPointer(value.DisplayText, func(v string) string { return v }),
		Attachments:         projectList(value.Attachments, projectAttachment),
		ID:                  value.ID,
		Text:                value.Text,
		ClientUserMessageID: value.ClientUserMessageID,
	}
}

func projectSendReceipt(value codex.SendReceipt) backend.SendReceipt {
	return backend.SendReceipt{
		QueuedSubmissionID:  value.QueuedSubmissionID,
		TurnID:              value.TurnID,
		ClientUserMessageID: value.ClientUserMessageID,
		Steered:             value.Steered,
	}
}

func projectSendAcknowledgement(value codex.SendAcknowledgement) backend.SendAcknowledgement {
	return backend.SendAcknowledgement{
		ClientUserMessageID: value.ClientUserMessageID,
		Digest:              value.Digest,
	}
}

func projectModel(value codex.Model) backend.Model {
	return backend.Model{
		ID:                        value.ID,
		Model:                     value.Model,
		DisplayName:               value.DisplayName,
		Description:               value.Description,
		IsDefault:                 value.IsDefault,
		DefaultReasoningEffort:    value.DefaultReasoningEffort,
		SupportedReasoningEfforts: projectList(value.SupportedReasoningEfforts, projectReasoningEffortOption),
	}
}

func projectReasoningEffortOption(value codex.ReasoningEffortOption) backend.ReasoningEffortOption {
	return backend.ReasoningEffortOption{
		ReasoningEffort: value.ReasoningEffort,
		Description:     value.Description,
	}
}

func projectCollaborationMode(value codex.CollaborationMode) backend.CollaborationMode {
	return backend.CollaborationMode{
		Name: value.Name,
		Mode: value.Mode,
	}
}

func projectThreadSettings(value codex.ThreadSettings) backend.Settings {
	return backend.Settings{
		Model:             value.Model,
		ModelProvider:     value.ModelProvider,
		ReasoningEffort:   value.ReasoningEffort,
		CollaborationMode: value.CollaborationMode,
		Policy:            projectThreadPolicy(value.Policy),
		ProjectID:         value.ProjectID,
	}
}

func projectThreadPolicy(value codex.ThreadPolicy) backend.ThreadPolicy {
	return backend.ThreadPolicy{
		DeveloperInstructions: value.DeveloperInstructions,
		Sandbox:               value.Sandbox,
		ApprovalPolicy:        value.ApprovalPolicy,
		MCPServer:             projectPointer(value.MCPServer, projectThreadMCPServer),
	}
}

func projectThreadMCPServer(value codex.ThreadMCPServer) backend.ThreadMCPServer {
	return backend.ThreadMCPServer{
		Name:    value.Name,
		Command: value.Command,
		Args:    projectList(value.Args, func(v string) string { return v }),
		Tool:    value.Tool,
	}
}

func projectThreadSettingsUpdate(value codex.ThreadSettingsUpdate) backend.SettingsUpdate {
	return backend.SettingsUpdate{
		Model:             projectPointer(value.Model, func(v string) string { return v }),
		ReasoningEffort:   projectPointer(value.ReasoningEffort, func(v string) string { return v }),
		CollaborationMode: projectPointer(value.CollaborationMode, func(v string) string { return v }),
	}
}

func projectActivitySnapshot(value codex.ActivitySnapshot) backend.ActivitySnapshot {
	return backend.ActivitySnapshot{
		Stale:             value.Stale,
		Refreshing:        value.Refreshing,
		ThreadID:          value.ThreadID,
		CurrentTurnID:     value.CurrentTurnID,
		Messages:          value.Messages,
		ToolCalls:         value.ToolCalls,
		SentMessages:      value.SentMessages,
		ReceivedMessages:  value.ReceivedMessages,
		TotalToolCalls:    value.TotalToolCalls,
		CountsComplete:    value.CountsComplete,
		IdleMS:            value.IdleMS,
		CreatedAtMS:       value.CreatedAtMS,
		BlockingRequests:  projectList(value.BlockingRequests, projectActivityBlockingRequest),
		WaitReason:        value.WaitReason,
		WaitUntilMS:       value.WaitUntilMS,
		LatestTurnStatus:  value.LatestTurnStatus,
		StartedAtMS:       value.StartedAtMS,
		CompletedAtMS:     value.CompletedAtMS,
		WorkingMS:         value.WorkingMS,
		WaitingMS:         value.WaitingMS,
		OpenWaitingMS:     value.OpenWaitingMS,
		BetweenTurnsMS:    value.BetweenTurnsMS,
		UnclassifiedMS:    value.UnclassifiedMS,
		ObservedAtMS:      value.ObservedAtMS,
		CoverageStartMS:   value.CoverageStartMS,
		CoverageComplete:  value.CoverageComplete,
		CoverageReason:    value.CoverageReason,
		CurrentState:      value.CurrentState,
		StateSinceMS:      value.StateSinceMS,
		Scope:             value.Scope,
		TimingApproximate: value.TimingApproximate,
	}
}

func projectActivityBlockingRequest(value codex.ActivityBlockingRequest) backend.ActivityBlockingRequest {
	return backend.ActivityBlockingRequest{
		TurnID: value.TurnID,
		ItemID: value.ItemID,
		Method: value.Method,
		Kind:   value.Kind,
	}
}

func projectList[A, B any](values []A, project func(A) B) []B {
	if values == nil {
		return nil
	}
	result := make([]B, len(values))
	for i, value := range values {
		result[i] = project(value)
	}
	return result
}
func projectPointer[A, B any](value *A, project func(A) B) *B {
	if value == nil {
		return nil
	}
	result := project(*value)
	return &result
}
func projectPage(value codex.TranscriptPage) backend.TranscriptPage {
	return backend.TranscriptPage{Transcript: projectTranscript(value.Transcript),
		OlderCursor: projectPointer(value.OlderCursor, func(v string) backend.HistoryCursor { return backend.HistoryCursor(v) }),
		HasOlder:    value.HasOlder, MetadataPending: value.MetadataPending}
}

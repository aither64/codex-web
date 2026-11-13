package conversation

import (
	"context"
	"errors"

	"github.com/aither64/codex-web/codex"
	"github.com/aither64/codex-web/conversation/backend"
)

// legacyBackend preserves the original native objects until all application
// callbacks finish. Only its forward projections enter the generic handler.
type legacyBackend struct{ target Target }

// Legacy queue HTTP errors depend on which operation failed, even when a
// native error contains an UploadError. Keep that provenance through adaptation.
type legacyQueueError struct {
	err         error
	observation bool
}

func (err *legacyQueueError) Error() string { return err.err.Error() }
func (err *legacyQueueError) Unwrap() error { return err.err }

func adaptLegacyTarget(target Target) BackendTarget {
	result := BackendTarget{
		legacyQueueReconcile: true,
		Identity:             backend.Identity{BackendID: "codex", ScopeKey: target.ThreadID, ConversationID: target.ThreadID, Directory: target.Directory},
		Capabilities:         target.Capabilities, MutationLock: target.MutationLock, Release: target.Release,
		Attachments: target.Attachments, Retry: target.Retry, RetryActivity: target.RetryActivity,
	}
	if target.Client == nil {
		return result
	}
	adapter := &legacyBackend{target: target}
	result.Operations = backend.Operations{
		Verify: adapter, Read: adapter, Pending: adapter, Send: adapter,
		Acknowledge: adapter, QueueRead: adapter, QueueWrite: adapter,
		Interrupt: adapter, Settings: adapter, LegacyRespond: adapter, Watch: adapter,
	}
	if _, ok := target.Client.(TranscriptPageReader); ok {
		result.Operations.Page = adapter
	}
	if _, ok := target.Client.(PromptResponder); ok {
		result.Operations.Respond = adapter
	}
	if _, ok := target.Client.(PendingRevisionProvider); ok {
		result.Operations.PendingRevision = adapter
	}
	if _, ok := target.Client.(QueueDeletionCompleter); ok {
		result.Operations.QueueCompletion = adapter
	}
	if target.Activity != nil {
		result.Operations.Activity = adapter
	}
	if target.ModelCatalog != nil {
		result.ModelCatalog = func(ctx context.Context) ([]backend.Model, error) {
			models, err := target.ModelCatalog(ctx)
			return projectList(models, projectModel), err
		}
	}
	return result
}

func (adapter *legacyBackend) Verify(ctx context.Context, id backend.Identity) error {
	return adapter.target.Client.VerifyThread(ctx, id.ConversationID, id.Directory)
}

func (adapter *legacyBackend) present(ctx context.Context, transcript *codex.Transcript) error {
	if fn := adapter.target.TransformTranscript; fn != nil {
		fn(transcript)
	}
	if provider := adapter.target.Attachments; provider != nil {
		if err := provider.ObserveTranscript(ctx, transcript); err != nil {
			return err
		}
	}
	if fn := adapter.target.PresentTranscript; fn != nil {
		fn(transcript)
	}
	return nil
}

func (adapter *legacyBackend) Read(ctx context.Context, id backend.Identity) (backend.Transcript, error) {
	value, err := adapter.target.Client.ReadThread(ctx, id.ConversationID)
	if err == nil {
		err = adapter.present(ctx, &value)
	}
	return projectTranscript(value), err
}

func (adapter *legacyBackend) ReadPage(ctx context.Context, id backend.Identity, cursor backend.HistoryCursor) (backend.TranscriptPage, error) {
	value, err := adapter.target.Client.(TranscriptPageReader).ReadThreadPage(ctx, id.ConversationID, string(cursor))
	if err != nil {
		var cursorErr *codex.TranscriptCursorError
		if errors.As(err, &cursorErr) {
			return backend.TranscriptPage{}, errors.Join(&backend.CursorError{Code: cursorErr.Code}, err)
		}
		return backend.TranscriptPage{}, err
	}
	if value.ThreadID != id.ConversationID || len(value.Entries) > 100 {
		return backend.TranscriptPage{}, errors.New("page reader returned invalid transcript")
	}
	if err := adapter.present(ctx, &value.Transcript); err != nil {
		return backend.TranscriptPage{}, err
	}
	return projectPage(value), nil
}

func (adapter *legacyBackend) Pending(ctx context.Context, id backend.Identity) ([]backend.Prompt, error) {
	values, err := adapter.target.Client.PromptsWithItems(ctx, id.ConversationID)
	return projectList(values, projectPrompt), err
}

func (adapter *legacyBackend) Send(ctx context.Context, id backend.Identity, request backend.SendRequest) (backend.SendResult, error) {
	if request.Retry {
		// The boolean can describe a prepared attempt, not native acceptance.
		if _, err := adapter.target.Client.SendAttempted(ctx, id.ConversationID, request.Message, request.ClientUserMessageID, request.ActionContext); err != nil {
			return backend.SendResult{}, &backend.ServiceError{Err: err}
		}
	}
	receipt, err := adapter.target.Client.Send(ctx, id.ConversationID, request.Message, request.ClientUserMessageID, request.ActionContext)
	if err == nil {
		return backend.SendResult{Outcome: backend.Accepted, Receipt: projectSendReceipt(receipt)}, nil
	}
	var unsent *codex.MessageNotSentError
	var unknown *codex.UnknownSendOutcomeError
	switch {
	case errors.As(err, &unsent):
		return backend.SendResult{Outcome: backend.NotSubmitted, Retryable: unsent.Retryable}, &backend.ServiceError{Err: err}
	case errors.As(err, &unknown):
		return backend.SendResult{Outcome: backend.Unknown}, &backend.ServiceError{Err: err}
	default:
		return backend.SendResult{}, &backend.ServiceError{Err: err}
	}
}

func (adapter *legacyBackend) Acknowledge(ctx context.Context, id backend.Identity, values []backend.SendAcknowledgement) ([]string, error) {
	return adapter.target.Client.AcknowledgeSends(ctx, id.ConversationID, projectList(values, func(value backend.SendAcknowledgement) codex.SendAcknowledgement {
		return codex.SendAcknowledgement{ClientUserMessageID: value.ClientUserMessageID, Digest: value.Digest}
	}))
}

func (adapter *legacyBackend) observeQueue(ctx context.Context, entries []codex.QueueEntry) error {
	if provider := adapter.target.Attachments; provider != nil {
		return provider.ObserveQueue(ctx, entries)
	}
	return nil
}
func (adapter *legacyBackend) ListQueue(ctx context.Context, id backend.Identity) ([]backend.QueueEntry, error) {
	values, err := adapter.target.Client.ListQueue(ctx, id.ConversationID)
	if err == nil {
		err = adapter.observeQueue(ctx, values)
	}
	return projectList(values, projectQueueEntry), err
}
func (adapter *legacyBackend) Queue(ctx context.Context, id backend.Identity, request backend.QueueRequest) (backend.QueueEntry, error) {
	value, err := adapter.target.Client.Queue(ctx, id.ConversationID, request.Message, request.ClientUserMessageID)
	if err != nil {
		return backend.QueueEntry{}, &legacyQueueError{err: err}
	}
	values := []codex.QueueEntry{value}
	if err := adapter.observeQueue(ctx, values); err != nil {
		return backend.QueueEntry{}, &legacyQueueError{err: err, observation: true}
	}
	return projectQueueEntry(values[0]), nil
}
func (adapter *legacyBackend) Delete(ctx context.Context, id backend.Identity, entry string) error {
	return adapter.target.Client.DeleteQueueEntry(ctx, id.ConversationID, entry)
}
func (adapter *legacyBackend) Start(ctx context.Context, id backend.Identity, entry string) error {
	return adapter.target.Client.StartQueue(ctx, id.ConversationID, entry)
}
func (adapter *legacyBackend) DeleteWithCompletion(ctx context.Context, id backend.Identity, entry string, complete func() error) error {
	return adapter.target.Client.(QueueDeletionCompleter).DeleteQueueEntryWithCompletion(ctx, id.ConversationID, entry, complete)
}
func (adapter *legacyBackend) ReconcileDeletions(ctx context.Context, id backend.Identity, complete func(string) error) error {
	return adapter.target.Client.(QueueDeletionCompleter).ReconcileQueueDeletionsWithCompletion(ctx, id.ConversationID, complete)
}
func (adapter *legacyBackend) Interrupt(ctx context.Context, id backend.Identity) error {
	return adapter.target.Client.Interrupt(ctx, id.ConversationID)
}
func (adapter *legacyBackend) Models(ctx context.Context, _ backend.Identity) ([]backend.Model, error) {
	values, err := adapter.target.Client.ListModels(ctx)
	return projectList(values, projectModel), err
}
func (adapter *legacyBackend) Modes(ctx context.Context, _ backend.Identity) ([]backend.CollaborationMode, error) {
	values, err := adapter.target.Client.ListCollaborationModes(ctx)
	return projectList(values, projectCollaborationMode), err
}
func (adapter *legacyBackend) Update(ctx context.Context, id backend.Identity, update backend.SettingsUpdate) (backend.Settings, error) {
	value, err := adapter.target.Client.UpdateThreadSettings(ctx, id.ConversationID, codex.ThreadSettingsUpdate{Model: update.Model, ReasoningEffort: update.ReasoningEffort, CollaborationMode: update.CollaborationMode})
	return projectThreadSettings(value), err
}
func (adapter *legacyBackend) Respond(ctx context.Context, id backend.Identity, response backend.PromptResponse) error {
	err := adapter.target.Client.(PromptResponder).RespondPrompt(ctx, id.ConversationID, codex.PromptResponse{ID: response.ID, Token: response.Token, Decision: response.Decision, Answers: response.Answers, Snooze: response.Snooze})
	return projectPromptError(err)
}
func projectPromptError(err error) error {
	var prompt *codex.PromptResponseError
	if errors.As(err, &prompt) {
		return &backend.PromptError{Code: prompt.Code, NotSent: prompt.NotSent, Message: prompt.Error(), Err: err}
	}
	return err
}
func (adapter *legacyBackend) Decision(ctx context.Context, id backend.Identity, offer, decision string) error {
	return projectPromptError(adapter.target.Client.RespondDecision(ctx, offer, id.ConversationID, decision))
}
func (adapter *legacyBackend) Answers(ctx context.Context, id backend.Identity, offer string, answers map[string]map[string][]string) error {
	return projectPromptError(adapter.target.Client.RespondAnswers(ctx, offer, id.ConversationID, answers))
}
func (adapter *legacyBackend) Snooze(_ context.Context, id backend.Identity, offer string) error {
	return projectPromptError(adapter.target.Client.SnoozeUserInput(offer, id.ConversationID))
}
func (adapter *legacyBackend) Subscribe(ctx context.Context, id backend.Identity) (<-chan struct{}, func(), error) {
	return adapter.target.Client.Subscribe(ctx, id.ConversationID)
}
func (adapter *legacyBackend) PendingRevision(id backend.Identity) uint64 {
	return adapter.target.Client.(PendingRevisionProvider).PendingRevision(id.ConversationID)
}
func (adapter *legacyBackend) ReadActivity(ctx context.Context, id backend.Identity) (backend.ActivitySnapshot, error) {
	value, err := adapter.target.Activity.ReadActivity(ctx, id.ConversationID)
	return projectActivitySnapshot(value), err
}

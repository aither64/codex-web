package conversation

import (
	"context"
	"log"
	"time"

	"github.com/aither64/codex-web/codex"
)

// Client is the App Server behavior used by Handler. *codex.Client implements
// this interface.
type Client interface {
	VerifyThread(context.Context, string, string) error
	ReadThread(context.Context, string) (codex.Transcript, error)
	PromptsWithItems(context.Context, string) ([]codex.Prompt, error)
	ListQueue(context.Context, string) ([]codex.QueueEntry, error)
	SendAttempted(context.Context, string, string, string, string) (bool, error)
	Send(context.Context, string, string, string, string) (codex.SendReceipt, error)
	AcknowledgeSends(context.Context, string, []codex.SendAcknowledgement) ([]string, error)
	Queue(context.Context, string, string, string) (codex.QueueEntry, error)
	DeleteQueueEntry(context.Context, string, string) error
	StartQueue(context.Context, string, string) error
	Interrupt(context.Context, string) error
	ListModels(context.Context) ([]codex.Model, error)
	ListCollaborationModes(context.Context) ([]codex.CollaborationMode, error)
	UpdateThreadSettings(context.Context, string, codex.ThreadSettingsUpdate) (codex.ThreadSettings, error)
	RespondDecision(context.Context, string, string, string) error
	RespondAnswers(context.Context, string, string, map[string]map[string][]string) error
	SnoozeUserInput(string, string) error
	Subscribe(context.Context, string) (<-chan struct{}, func(), error)
}

// ActivityProvider is optional, preserving source compatibility for applications
// implementing Client. The resolver selects its trusted authority separately.
type ActivityProvider interface {
	ReadActivity(context.Context, string) (codex.ActivitySnapshot, error)
}

type TranscriptPageReader interface {
	ReadThreadPage(context.Context, string, string) (codex.TranscriptPage, error)
}

// PendingRevisionProvider supplies an in-memory pending-control invalidation
// token. Reading it must not perform RPCs or access durable submission state.
type PendingRevisionProvider interface {
	PendingRevision(threadID string) uint64
}

// PromptResponder adds request-bound responses without changing the legacy
// Client interface used by existing embedding applications.
type PromptResponder interface {
	RespondPrompt(context.Context, string, codex.PromptResponse) error
}

// ResolveRequest describes the operation for which an application must resolve
// authorization and runtime ownership.
type ResolveRequest struct {
	ID        string
	Operation string
	Method    string
	Mutation  bool
}

// Target is the trusted server-side mapping for one opaque conversation ID.
// Socket paths, thread IDs and working directories never come from browser
// input. MutationLock must be shared with every other application operation
// that mutates the same conversation. Release is called after the operation,
// including an event stream.
type Target struct {
	Attachments AttachmentProvider
	Client      Client
	// ModelCatalog optionally resolves models for this trusted conversation's
	// provider. Both the picker and settings validation use the same catalog.
	ModelCatalog        func(context.Context) ([]codex.Model, error)
	Activity            ActivityProvider
	Retry               RetryProvider
	RetryActivity       RetryActivityProvider
	ThreadID            string
	Directory           string
	Capabilities        Capabilities
	MutationLock        MutationLocker
	TransformTranscript func(*codex.Transcript)
	// PresentTranscript renders application-owned presentation after attachment
	// decoration. TransformTranscript retains its earlier, pre-attachment order.
	PresentTranscript func(*codex.Transcript)
	Release           func()
}

func (target Target) models(ctx context.Context) ([]codex.Model, error) {
	if target.ModelCatalog != nil {
		return target.ModelCatalog(ctx)
	}
	return target.Client.ListModels(ctx)
}

// Resolver maps an application-defined opaque ID to its trusted App Server
// client, thread and working directory for every request.
type Resolver interface {
	ResolveConversation(context.Context, ResolveRequest) (Target, error)
}

type ResolverFunc func(context.Context, ResolveRequest) (Target, error)

func (fn ResolverFunc) ResolveConversation(ctx context.Context, request ResolveRequest) (Target, error) {
	return fn(ctx, request)
}

type Options struct {
	Resolver         Resolver
	AllowedOrigins   []string
	BasePath         string
	MaxBodyBytes     int64
	MaxMessageBytes  int
	OperationTimeout time.Duration
	Shutdown         <-chan struct{}
	Logger           *log.Logger
}

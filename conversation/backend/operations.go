package backend

import "context"

// Identity is trusted adapter routing, never a browser response. Generation
// identifies current execution authority and never creates a new receipt key.
type Identity struct {
	BackendID, ScopeKey, ConversationID, Directory, Generation string
}

type Verifier interface {
	Verify(context.Context, Identity) error
}
type Reader interface {
	Read(context.Context, Identity) (Transcript, error)
}
type PageReader interface {
	ReadPage(context.Context, Identity, HistoryCursor) (TranscriptPage, error)
}
type PendingReader interface {
	Pending(context.Context, Identity) ([]Prompt, error)
}
type Sender interface {
	Send(context.Context, Identity, SendRequest) (SendResult, error)
}
type SendAcknowledger interface {
	Acknowledge(context.Context, Identity, []SendAcknowledgement) ([]string, error)
}
type QueueReader interface {
	ListQueue(context.Context, Identity) ([]QueueEntry, error)
}
type QueueWriter interface {
	Queue(context.Context, Identity, QueueRequest) (QueueEntry, error)
	Delete(context.Context, Identity, string) error
	Start(context.Context, Identity, string) error
}
type QueueDeletionCompleter interface {
	DeleteWithCompletion(context.Context, Identity, string, func() error) error
	ReconcileDeletions(context.Context, Identity, func(string) error) error
}
type Interrupter interface {
	Interrupt(context.Context, Identity) error
}
type SettingsClient interface {
	Models(context.Context, Identity) ([]Model, error)
	Modes(context.Context, Identity) ([]CollaborationMode, error)
	Update(context.Context, Identity, SettingsUpdate) (Settings, error)
}
type PromptResponder interface {
	Respond(context.Context, Identity, PromptResponse) error
}
type LegacyPromptResponder interface {
	Decision(context.Context, Identity, string, string) error
	Answers(context.Context, Identity, string, map[string]map[string][]string) error
	Snooze(context.Context, Identity, string) error
}
type Watcher interface {
	Subscribe(context.Context, Identity) (<-chan struct{}, func(), error)
}
type PendingRevisionProvider interface{ PendingRevision(Identity) uint64 }
type ActivityProvider interface {
	ReadActivity(context.Context, Identity) (ActivitySnapshot, error)
}
type AttachmentPreparer interface {
	Prepare(context.Context, string, string, string, []string) (string, error)
	QueueDeleted(context.Context, string) error
}

// Operations describes support, independently of application authorization.
// Verify is required; nil optional groups must remain absent after adaptation.
type Operations struct {
	Verify          Verifier
	Read            Reader
	Page            PageReader
	Pending         PendingReader
	Send            Sender
	Acknowledge     SendAcknowledger
	QueueRead       QueueReader
	QueueWrite      QueueWriter
	QueueCompletion QueueDeletionCompleter
	Interrupt       Interrupter
	Settings        SettingsClient
	Respond         PromptResponder
	LegacyRespond   LegacyPromptResponder
	Watch           Watcher
	PendingRevision PendingRevisionProvider
	Activity        ActivityProvider
}

type SendRequest struct {
	Message, ClientUserMessageID, ActionContext string
	Retry                                       bool
}
type QueueRequest struct{ Message, ClientUserMessageID string }
type SendOutcome string

const (
	Accepted     SendOutcome = "accepted"
	NotSubmitted SendOutcome = "not_submitted"
	Unknown      SendOutcome = "unknown"
)

// SendResult describes admission, not completion of model or tool execution.
// Retryable has meaning only with positively proved NotSubmitted.
type SendResult struct {
	Outcome   SendOutcome
	Receipt   SendReceipt
	Retryable bool
}

// Package backend defines presentation and operation contracts for trusted
// conversation adapters. It contains no native transport or persistence policy.
package backend

// Transcript is a conversation presentation value.
type Transcript struct {
	ThreadID          string            `json:"threadId"`
	Status            string            `json:"status"`
	LatestTurnID      string            `json:"latestTurnId,omitempty"`
	Model             string            `json:"model,omitempty"`
	ReasoningEffort   string            `json:"reasoningEffort,omitempty"`
	CollaborationMode string            `json:"collaborationMode,omitempty"`
	Entries           []TranscriptEntry `json:"entries"`
}

// TranscriptEntry is a conversation presentation value.
type TranscriptEntry struct {
	Communication           *TranscriptCommunication `json:"communication,omitempty"`
	Citations               []CitationSpan           `json:"citations,omitempty"`
	CitationsPending        bool                     `json:"citationsPending,omitempty"`
	Delivery                string                   `json:"delivery,omitempty"`
	Questions               []AsyncQuestion          `json:"questions,omitempty"`
	QuestionReplies         []AsyncQuestionReply     `json:"questionReplies,omitempty"`
	DisplayText             *string                  `json:"displayText,omitempty"`
	Attachments             []Attachment             `json:"attachments,omitempty"`
	TurnID                  string                   `json:"turnId,omitempty"`
	TurnStatus              string                   `json:"turnStatus,omitempty"`
	ItemID                  string                   `json:"itemId,omitempty"`
	ClientUserMessageID     string                   `json:"clientUserMessageId,omitempty"`
	ClientUserMessageDigest string                   `json:"clientUserMessageDigest,omitempty"`
	Kind                    string                   `json:"kind"`
	Summary                 string                   `json:"summary,omitempty"`
	Text                    string                   `json:"text,omitempty"`
	HTML                    string                   `json:"html,omitempty"`
	Details                 string                   `json:"details,omitempty"`
	Timestamp               string                   `json:"timestamp,omitempty"`
	TimestampApproximate    bool                     `json:"timestampApproximate,omitempty"`
	Activity                *TranscriptActivity      `json:"activity,omitempty"`
}

// TranscriptCommunication is a conversation presentation value.
type TranscriptCommunication struct {
	Kind string `json:"kind"`
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
}

// Attachment is a conversation presentation value.
type Attachment struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Size        int64    `json:"size"`
	State       string   `json:"state"`
	DownloadURL string   `json:"downloadUrl,omitempty"`
	DeleteURL   string   `json:"deleteUrl,omitempty"`
	References  []string `json:"references,omitempty"`
}

// AsyncQuestion is a conversation presentation value.
type AsyncQuestion struct {
	ID      string   `json:"id"`
	Title   string   `json:"title"`
	Options []string `json:"options,omitempty"`
}

// AsyncQuestionReply is a conversation presentation value.
type AsyncQuestionReply struct {
	QuestionItemID string `json:"questionItemId"`
	Question       string `json:"question"`
	Answer         string `json:"answer"`
}

// CitationSource is a conversation presentation value.
type CitationSource struct {
	URL    string `json:"url"`
	Title  string `json:"title"`
	Number int    `json:"number"`
}

// CitationSpan is a conversation presentation value.
type CitationSpan struct {
	Start       int              `json:"start"`
	End         int              `json:"end"`
	Sources     []CitationSource `json:"sources,omitempty"`
	Unavailable bool             `json:"unavailable,omitempty"`
	Incomplete  bool             `json:"incomplete,omitempty"`
}

// TranscriptActivity is a conversation presentation value.
type TranscriptActivity struct {
	Action          string          `json:"action,omitempty"`
	Status          string          `json:"status,omitempty"`
	Queries         []string        `json:"queries,omitempty"`
	Links           []ActivityLink  `json:"links,omitempty"`
	Agents          []ActivityAgent `json:"agents,omitempty"`
	AgentPath       string          `json:"agentPath,omitempty"`
	Model           string          `json:"model,omitempty"`
	ReasoningEffort string          `json:"reasoningEffort,omitempty"`
}

// ActivityLink is a conversation presentation value.
type ActivityLink struct {
	URL   string `json:"url"`
	Title string `json:"title"`
}

// ActivityAgent is a conversation presentation value.
type ActivityAgent struct {
	ThreadID string `json:"threadId"`
	Status   string `json:"status,omitempty"`
	Message  string `json:"message,omitempty"`
}

// Prompt is a conversation presentation value.
type Prompt struct {
	ID                        string         `json:"id"`
	Token                     string         `json:"token,omitempty"`
	Method                    string         `json:"method"`
	Kind                      string         `json:"kind"`
	ThreadID                  string         `json:"threadId"`
	TurnID                    string         `json:"turnId,omitempty"`
	ItemID                    string         `json:"itemId,omitempty"`
	Params                    map[string]any `json:"params"`
	Item                      map[string]any `json:"item,omitempty"`
	AuthorityAvailable        bool           `json:"authorityAvailable"`
	Error                     string         `json:"error,omitempty"`
	AvailableDecisions        []string       `json:"availableDecisions,omitempty"`
	Questions                 []Question     `json:"questions,omitempty"`
	IsBlocking                bool           `json:"isBlocking"`
	AutoResolutionMS          *uint64        `json:"autoResolutionMs,omitempty"`
	AutoResolutionVisibleAtMS int64          `json:"autoResolutionVisibleAtMs,omitempty"`
	AutoResolutionAtMS        int64          `json:"autoResolutionAtMs,omitempty"`
	AutoResolveSnoozed        bool           `json:"autoResolveSnoozed,omitempty"`
}

// Question is a conversation presentation value.
type Question struct {
	ID       string   `json:"id"`
	Header   string   `json:"header"`
	Question string   `json:"question"`
	IsSecret bool     `json:"isSecret"`
	IsOther  bool     `json:"isOther"`
	Options  []Option `json:"options,omitempty"`
}

// Option is a conversation presentation value.
type Option struct {
	Label       string `json:"label"`
	Description string `json:"description"`
}

// PromptResponse is a conversation presentation value.
type PromptResponse struct {
	ID       string                         `json:"id"`
	Token    string                         `json:"token,omitempty"`
	Decision string                         `json:"decision,omitempty"`
	Answers  map[string]map[string][]string `json:"answers,omitempty"`
	Snooze   bool                           `json:"snooze,omitempty"`
}

// QueueEntry is a conversation presentation value.
type QueueEntry struct {
	DisplayText         *string      `json:"displayText,omitempty"`
	Attachments         []Attachment `json:"attachments,omitempty"`
	ID                  string       `json:"id"`
	Text                string       `json:"text"`
	ClientUserMessageID string       `json:"clientUserMessageId"`
}

// SendReceipt is a conversation presentation value.
type SendReceipt struct {
	QueuedSubmissionID  string `json:"queuedSubmissionId,omitempty"`
	TurnID              string `json:"turnId"`
	ClientUserMessageID string `json:"clientUserMessageId"`
	Steered             bool   `json:"steered"`
}

// SendAcknowledgement is a conversation presentation value.
type SendAcknowledgement struct {
	ClientUserMessageID string `json:"clientUserMessageId"`
	Digest              string `json:"digest"`
}

// Model is a conversation presentation value.
type Model struct {
	ID                        string                  `json:"id"`
	Model                     string                  `json:"model"`
	DisplayName               string                  `json:"displayName"`
	Description               string                  `json:"description"`
	IsDefault                 bool                    `json:"isDefault"`
	DefaultReasoningEffort    string                  `json:"defaultReasoningEffort"`
	SupportedReasoningEfforts []ReasoningEffortOption `json:"supportedReasoningEfforts"`
}

// ReasoningEffortOption is a conversation presentation value.
type ReasoningEffortOption struct {
	ReasoningEffort string `json:"reasoningEffort"`
	Description     string `json:"description"`
}

// CollaborationMode is a conversation presentation value.
type CollaborationMode struct {
	Name string `json:"name"`
	Mode string `json:"mode"`
}

// Settings is a conversation presentation value.
type Settings struct {
	Model             string       `json:"model,omitempty"`
	ModelProvider     string       `json:"modelProvider,omitempty"`
	ReasoningEffort   string       `json:"reasoningEffort,omitempty"`
	CollaborationMode string       `json:"collaborationMode,omitempty"`
	Policy            ThreadPolicy `json:"policy,omitempty"`
	ProjectID         string       `json:"projectId,omitempty"`
}

// ThreadPolicy is a conversation presentation value.
type ThreadPolicy struct {
	DeveloperInstructions string           `json:"developerInstructions,omitempty"`
	Sandbox               string           `json:"sandbox,omitempty"`
	ApprovalPolicy        string           `json:"approvalPolicy,omitempty"`
	MCPServer             *ThreadMCPServer `json:"mcpServer,omitempty"`
}

// ThreadMCPServer is a conversation presentation value.
type ThreadMCPServer struct {
	Name    string   `json:"name"`
	Command string   `json:"command"`
	Args    []string `json:"args"`
	Tool    string   `json:"tool"`
}

// SettingsUpdate is a conversation presentation value.
type SettingsUpdate struct {
	Model             *string `json:"model,omitempty"`
	ReasoningEffort   *string `json:"reasoningEffort,omitempty"`
	CollaborationMode *string `json:"collaborationMode,omitempty"`
}

// ActivitySnapshot is a conversation presentation value.
type ActivitySnapshot struct {
	Stale             bool                      `json:"stale,omitempty"`
	Refreshing        bool                      `json:"refreshing,omitempty"`
	ThreadID          string                    `json:"threadId"`
	CurrentTurnID     string                    `json:"currentTurnId,omitempty"`
	Messages          int                       `json:"messages"`
	ToolCalls         int                       `json:"toolCalls"`
	SentMessages      int                       `json:"sentMessages"`
	ReceivedMessages  int                       `json:"receivedMessages"`
	TotalToolCalls    int                       `json:"totalToolCalls"`
	CountsComplete    bool                      `json:"countsComplete"`
	IdleMS            int64                     `json:"idleMs"`
	CreatedAtMS       int64                     `json:"createdAtMs,omitempty"`
	BlockingRequests  []ActivityBlockingRequest `json:"blockingRequests,omitempty"`
	WaitReason        string                    `json:"waitReason,omitempty"`
	WaitUntilMS       int64                     `json:"waitUntilMs,omitempty"`
	LatestTurnStatus  string                    `json:"latestTurnStatus,omitempty"`
	StartedAtMS       int64                     `json:"startedAtMs,omitempty"`
	CompletedAtMS     int64                     `json:"completedAtMs,omitempty"`
	WorkingMS         int64                     `json:"workingMs"`
	WaitingMS         int64                     `json:"waitingMs"`
	OpenWaitingMS     int64                     `json:"openWaitingMs"`
	BetweenTurnsMS    int64                     `json:"betweenTurnsMs"`
	UnclassifiedMS    int64                     `json:"unclassifiedMs"`
	ObservedAtMS      int64                     `json:"observedAtMs"`
	CoverageStartMS   int64                     `json:"coverageStartMs,omitempty"`
	CoverageComplete  bool                      `json:"coverageComplete"`
	CoverageReason    string                    `json:"coverageReason,omitempty"`
	CurrentState      string                    `json:"currentState"`
	StateSinceMS      int64                     `json:"stateSinceMs,omitempty"`
	Scope             string                    `json:"scope"`
	TimingApproximate bool                      `json:"timingApproximate"`
}

// ActivityBlockingRequest is a conversation presentation value.
type ActivityBlockingRequest struct {
	TurnID string `json:"turnId"`
	ItemID string `json:"itemId"`
	Method string `json:"method"`
	Kind   string `json:"kind"`
}

// HistoryCursor is an opaque adapter-owned history position.
type HistoryCursor string

// TranscriptPage retains the browser's bounded history page shape.
type TranscriptPage struct {
	Transcript
	OlderCursor     *HistoryCursor `json:"olderCursor"`
	HasOlder        bool           `json:"hasOlder"`
	MetadataPending bool           `json:"metadataPending"`
}

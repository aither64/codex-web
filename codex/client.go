package codex

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"golang.org/x/sys/unix"
)

const (
	readLimit          = 64 * 1024 * 1024
	queueLedgerMaxSize = 1024 * 1024
	recentTurnLimit    = 20
)

type rpcMessage struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type rpcCallError struct {
	code    int
	message string
}

func (e *rpcCallError) Error() string {
	return fmt.Sprintf("Codex RPC %d: %s", e.code, e.message)
}

type response struct {
	result json.RawMessage
	err    error
}

type pendingCall struct {
	generation uint64
	channel    chan response
}

type PendingRequest struct {
	ID         string          `json:"id"`
	Method     string          `json:"method"`
	Params     json.RawMessage `json:"params"`
	generation uint64
	connection *websocket.Conn
	claimed    bool
	receivedAt time.Time
	snoozed    bool
}

type Prompt struct {
	ID                        string         `json:"id"`
	Method                    string         `json:"method"`
	Kind                      string         `json:"kind"`
	ThreadID                  string         `json:"threadId"`
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

type Question struct {
	ID       string   `json:"id"`
	Header   string   `json:"header"`
	Question string   `json:"question"`
	IsSecret bool     `json:"isSecret"`
	IsOther  bool     `json:"isOther"`
	Options  []Option `json:"options,omitempty"`
}

type Option struct {
	Label       string `json:"label"`
	Description string `json:"description"`
}

type Transcript struct {
	ThreadID          string            `json:"threadId"`
	Status            string            `json:"status"`
	Model             string            `json:"model,omitempty"`
	ReasoningEffort   string            `json:"reasoningEffort,omitempty"`
	CollaborationMode string            `json:"collaborationMode,omitempty"`
	Entries           []TranscriptEntry `json:"entries"`
}

type TranscriptEntry struct {
	TurnID                  string              `json:"turnId,omitempty"`
	TurnStatus              string              `json:"turnStatus,omitempty"`
	ItemID                  string              `json:"itemId,omitempty"`
	ClientUserMessageID     string              `json:"clientUserMessageId,omitempty"`
	ClientUserMessageDigest string              `json:"clientUserMessageDigest,omitempty"`
	Kind                    string              `json:"kind"`
	Summary                 string              `json:"summary,omitempty"`
	Text                    string              `json:"text,omitempty"`
	HTML                    string              `json:"html,omitempty"`
	Details                 string              `json:"details,omitempty"`
	Timestamp               string              `json:"timestamp,omitempty"`
	TimestampApproximate    bool                `json:"timestampApproximate,omitempty"`
	Activity                *TranscriptActivity `json:"activity,omitempty"`
}

type SendReceipt struct {
	TurnID              string `json:"turnId"`
	ClientUserMessageID string `json:"clientUserMessageId"`
	Steered             bool   `json:"steered"`
}

type SendAcknowledgement struct {
	ClientUserMessageID string `json:"clientUserMessageId"`
	Digest              string `json:"digest"`
}

type ThreadSettings struct {
	Model             string `json:"model,omitempty"`
	ReasoningEffort   string `json:"reasoningEffort,omitempty"`
	CollaborationMode string `json:"collaborationMode,omitempty"`
}

type ThreadSettingsUpdate struct {
	Model             *string `json:"model,omitempty"`
	ReasoningEffort   *string `json:"reasoningEffort,omitempty"`
	CollaborationMode *string `json:"collaborationMode,omitempty"`
}

// ThreadMetadata is the App Server identity and persistence metadata used by
// applications that own thread recovery and archival policy.
type ThreadMetadata struct {
	ID           string            `json:"id"`
	Cwd          string            `json:"cwd"`
	ForkedFromID string            `json:"forkedFromId"`
	Source       any               `json:"source"`
	UpdatedAt    int64             `json:"updatedAt"`
	Path         *string           `json:"path"`
	Preview      string            `json:"preview"`
	Ephemeral    *bool             `json:"ephemeral"`
	HistoryMode  string            `json:"historyMode"`
	Status       map[string]any    `json:"status"`
	Turns        *[]map[string]any `json:"turns"`
}

type ThreadListOptions struct {
	Cwd           string
	SourceKinds   []string
	Archived      *bool
	Limit         int
	SortDirection string
	Cursor        string
}

type CollaborationMode struct {
	Name string `json:"name"`
	Mode string `json:"mode"`
}

// AccountRateLimits contains the legacy allowance and any named allowance buckets.
// Account identity, plan and credit details are intentionally not included.
type AccountRateLimits struct {
	RateLimits          RateLimitSnapshot            `json:"rateLimits"`
	RateLimitsByLimitID map[string]RateLimitSnapshot `json:"rateLimitsByLimitId"`
}

type RateLimitSnapshot struct {
	LimitID   string           `json:"limitId"`
	Primary   *RateLimitWindow `json:"primary"`
	Secondary *RateLimitWindow `json:"secondary"`
}

// RateLimitWindow describes usage over a reported duration. Primary and
// secondary positions do not imply a particular duration. ResetsAt is Unix time
// in seconds; nil duration or reset time means the server did not report it.
type RateLimitWindow struct {
	UsedPercent        int    `json:"usedPercent"`
	WindowDurationMins *int64 `json:"windowDurationMins"`
	ResetsAt           *int64 `json:"resetsAt"`
}

type QueueEntry struct {
	ID                  string `json:"id"`
	Text                string `json:"text"`
	ClientUserMessageID string `json:"clientUserMessageId"`
}

type cachedThreadSettings struct {
	generation uint64
	revision   uint64
	settings   ThreadSettings
}

type threadItemEntry struct {
	TurnID string         `json:"turnId"`
	Item   map[string]any `json:"item"`
}

type queueAttemptLedger struct {
	Schema     int                                        `json:"schema"`
	Attempts   map[string]map[string]string               `json:"attempts"`
	Sends      map[string]map[string]sendAttempt          `json:"sends,omitempty"`
	Deletions  map[string]map[string]queueDeletionAttempt `json:"deletions,omitempty"`
	Operations map[string]string                          `json:"retirements,omitempty"`
}

type queueDeletionAttempt struct {
	ClientUserMessageID string `json:"clientUserMessageId"`
	Digest              string `json:"digest"`
}

type sendAttempt struct {
	Digest  string `json:"digest"`
	State   string `json:"state"`
	Context string `json:"context,omitempty"`
	Steered bool   `json:"steered"`
	TurnID  string `json:"turnId,omitempty"`
}

type UnknownSendOutcomeError struct {
	Err error
}

func (e *UnknownSendOutcomeError) Error() string {
	return fmt.Sprintf("message outcome is still unknown; retry with the same browser attempt: %v", e.Err)
}

func (e *UnknownSendOutcomeError) Unwrap() error { return e.Err }

type ReasoningEffortOption struct {
	ReasoningEffort string `json:"reasoningEffort"`
	Description     string `json:"description"`
}

type Model struct {
	ID                        string                  `json:"id"`
	Model                     string                  `json:"model"`
	DisplayName               string                  `json:"displayName"`
	Description               string                  `json:"description"`
	IsDefault                 bool                    `json:"isDefault"`
	DefaultReasoningEffort    string                  `json:"defaultReasoningEffort"`
	SupportedReasoningEfforts []ReasoningEffortOption `json:"supportedReasoningEfforts"`
}

func ResolveNewThreadSettings(
	models []Model, requested ThreadSettings, defaults ThreadSettings,
) (ThreadSettings, error) {
	if defaults.Model == "" || defaults.ReasoningEffort == "" {
		return ThreadSettings{}, errors.New("default Codex model and reasoning effort are required")
	}
	var selected *Model
	for index := range models {
		candidate := &models[index]
		if requested.Model != "" {
			if candidate.Model == requested.Model {
				selected = candidate
				break
			}
		} else if candidate.Model == defaults.Model {
			if selected != nil {
				return ThreadSettings{}, fmt.Errorf(
					"Codex model catalog has more than one %q model", defaults.Model,
				)
			}
			selected = candidate
		}
	}
	if selected == nil {
		if requested.Model == "" {
			return ThreadSettings{}, fmt.Errorf(
				"required default Codex model %q is not available", defaults.Model,
			)
		}
		return ThreadSettings{}, fmt.Errorf("Codex model %q is not available", requested.Model)
	}

	supports := func(effort string) bool {
		return slices.ContainsFunc(
			selected.SupportedReasoningEfforts,
			func(option ReasoningEffortOption) bool { return option.ReasoningEffort == effort },
		)
	}
	effort := requested.ReasoningEffort
	if effort == "" && supports(defaults.ReasoningEffort) {
		effort = defaults.ReasoningEffort
	}
	if effort == "" && requested.Model != "" && supports(selected.DefaultReasoningEffort) {
		effort = selected.DefaultReasoningEffort
	}
	if effort == "" {
		return ThreadSettings{}, fmt.Errorf(
			"Codex model %s does not support the required default reasoning effort %q",
			selected.DisplayName, defaults.ReasoningEffort,
		)
	}
	if !supports(effort) {
		return ThreadSettings{}, fmt.Errorf(
			"reasoning effort %q is not available for %s", effort, selected.DisplayName,
		)
	}
	return ThreadSettings{Model: selected.Model, ReasoningEffort: effort}, nil
}

func ResolveForkThreadSettings(
	models []Model, source ThreadSettings, requested ThreadSettings,
) (ThreadSettings, error) {
	desired := source
	if requested.Model != "" {
		desired.Model = requested.Model
	}
	if requested.ReasoningEffort != "" {
		desired.ReasoningEffort = requested.ReasoningEffort
	}
	if desired.Model == "" || desired.ReasoningEffort == "" {
		return ThreadSettings{}, errors.New("source Codex thread has incomplete settings")
	}
	var selected *Model
	for index := range models {
		candidate := &models[index]
		if candidate.Model != desired.Model {
			continue
		}
		if selected != nil {
			return ThreadSettings{}, fmt.Errorf(
				"Codex model catalog has more than one %q model", desired.Model,
			)
		}
		selected = candidate
	}
	if selected == nil {
		return ThreadSettings{}, fmt.Errorf("Codex model %q is not available", desired.Model)
	}
	if !slices.ContainsFunc(
		selected.SupportedReasoningEfforts,
		func(option ReasoningEffortOption) bool {
			return option.ReasoningEffort == desired.ReasoningEffort
		},
	) {
		return ThreadSettings{}, fmt.Errorf(
			"reasoning effort %q is not available for %s",
			desired.ReasoningEffort,
			selected.DisplayName,
		)
	}
	return ThreadSettings{
		Model: desired.Model, ReasoningEffort: desired.ReasoningEffort,
	}, nil
}

type Client struct {
	socket            string
	options           ClientOptions
	activityNamespace string
	observer          *observerState
	turnHistoryMu     sync.Mutex
	turnHistory       map[string]*threadHistoryCache
	idleTurnHistory   []string

	ensureMu     sync.Mutex
	connectionMu sync.Mutex
	connection   *websocket.Conn
	generation   uint64
	ready        uint64
	closed       bool
	writeMu      sync.Mutex
	nextID       atomic.Uint64

	pendingMu sync.Mutex
	pending   map[uint64]pendingCall
	requests  map[string]PendingRequest
	notices   map[string][]Prompt

	subscribersMu sync.Mutex
	subscribers   map[chan struct{}]string

	settingsMu       sync.Mutex
	settingsRevision uint64
	threadSettings   map[string]cachedThreadSettings
	settingsChanged  chan struct{}
	settingsUpdateMu sync.Mutex
	settingsUpdates  map[string]*sync.Mutex
	queueMu          sync.Mutex
	queueUpdates     map[string]*sync.Mutex
	queueAttempts    map[string]map[string]string
	sendAttempts     map[string]map[string]sendAttempt
	queueDeletions   map[string]map[string]queueDeletionAttempt
	operations       map[string]string
	queueLedgerPath  string
	queueLedgerLock  *os.File

	watchedMu         sync.Mutex
	watched           map[string]int
	watchedGeneration map[string]uint64
	watchLocks        map[string]*sync.Mutex
	liveItemTimes     map[string]*liveTurnTimestamps // protected by watchedMu
}

type ClientInfo struct {
	Name    string `json:"name"`
	Title   string `json:"title,omitempty"`
	Version string `json:"version"`
}

type ClientOptions struct {
	ClientInfo            ClientInfo
	DeveloperInstructions string
	RuntimeWorkspaceRoots []string
	ThreadSourceKinds     []string
	NonBlockingUserInput  *NonBlockingUserInputPolicy
	// SubmissionLedgerPath selects application-owned durable retry storage.
	// Empty preserves the compatibility path next to the App Server socket.
	SubmissionLedgerPath string
	// ObserverOnly prevents responses, unsupported-request rejections and
	// thread-setting overrides. Use a dedicated client for passive monitoring.
	ObserverOnly     bool
	ActivityRecorder *ActivityRecorder
}

// NonBlockingUserInputPolicy opts a client into automatically answering
// nonblocking user-input requests after an application-defined grace period.
// A nil policy leaves every request pending until the application responds.
type NonBlockingUserInputPolicy struct {
	HiddenGrace      time.Duration
	VisibleCountdown time.Duration
}

func DefaultSocket() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".codex", "app-server-control", "app-server-control.sock")
}

func New(socket string) *Client {
	return NewWithOptions(socket, ClientOptions{})
}

func NewWithOptions(socket string, options ClientOptions) *Client {
	if socket == "" {
		socket = DefaultSocket()
	}
	if options.ClientInfo.Name == "" {
		options.ClientInfo.Name = "codex-web"
	}
	if options.ClientInfo.Title == "" {
		options.ClientInfo.Title = "Codex Web"
	}
	if options.ClientInfo.Version == "" {
		options.ClientInfo.Version = "0.1.0"
	}
	options.RuntimeWorkspaceRoots = append([]string(nil), options.RuntimeWorkspaceRoots...)
	options.ThreadSourceKinds = append([]string(nil), options.ThreadSourceKinds...)
	queueLedgerPath := options.SubmissionLedgerPath
	if queueLedgerPath == "" {
		queueLedgerPath = socket + ".submission-attempts-v3.json"
	}
	if options.NonBlockingUserInput != nil {
		if options.NonBlockingUserInput.HiddenGrace < 0 ||
			options.NonBlockingUserInput.VisibleCountdown < 0 {
			panic("codex: nonblocking user-input durations cannot be negative")
		}
		policy := *options.NonBlockingUserInput
		options.NonBlockingUserInput = &policy
	}
	var observer *observerState
	if options.ObserverOnly {
		observer = newObserverState()
	}
	return &Client{
		observer: observer,
		socket:   socket, options: options, activityNamespace: randomActivityNamespace(), pending: make(map[uint64]pendingCall),
		requests: make(map[string]PendingRequest), notices: make(map[string][]Prompt),
		subscribers: make(map[chan struct{}]string), watched: make(map[string]int),
		watchedGeneration: make(map[string]uint64), watchLocks: make(map[string]*sync.Mutex),
		liveItemTimes:  make(map[string]*liveTurnTimestamps),
		threadSettings: make(map[string]cachedThreadSettings), settingsChanged: make(chan struct{}),
		settingsUpdates: make(map[string]*sync.Mutex),
		queueUpdates:    make(map[string]*sync.Mutex), queueAttempts: make(map[string]map[string]string),
		sendAttempts:    make(map[string]map[string]sendAttempt),
		queueDeletions:  make(map[string]map[string]queueDeletionAttempt),
		operations:      make(map[string]string),
		queueLedgerPath: queueLedgerPath,
	}
}

func (c *Client) Ensure(ctx context.Context) error {
	c.ensureMu.Lock()
	defer c.ensureMu.Unlock()
	c.connectionMu.Lock()
	if c.closed {
		c.connectionMu.Unlock()
		return errors.New("Codex client is closed")
	}
	if c.connection != nil && c.ready == c.generation {
		c.connectionMu.Unlock()
		return nil
	}
	if c.connection != nil {
		c.connectionMu.Unlock()
		return errors.New("Codex App Server connection is still initializing")
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", c.socket)
	}}
	connection, _, err := websocket.Dial(ctx, "ws://localhost/", &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		c.connectionMu.Unlock()
		return fmt.Errorf("connect to Codex App Server at %s: %w", c.socket, err)
	}
	connection.SetReadLimit(readLimit)
	c.generation++
	generation := c.generation
	c.connection = connection
	if c.observer != nil {
		c.observer.connectionContext, c.observer.cancelConnection = context.WithCancel(c.observer.context)
	}
	c.connectionMu.Unlock()
	go c.readLoop(connection, generation)

	initializeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := c.requestOn(initializeCtx, connection, generation, "initialize", map[string]any{
		"capabilities": map[string]any{"experimentalApi": true},
		"clientInfo":   c.options.ClientInfo,
	}, nil); err != nil {
		connection.CloseNow()
		c.markDisconnected(connection, generation, err)
		return err
	}
	message, _ := json.Marshal(map[string]any{"method": "initialized"})
	if err := c.writeOn(initializeCtx, connection, generation, message); err != nil {
		return fmt.Errorf("send initialized notification: %w", err)
	}
	c.connectionMu.Lock()
	if c.connection != connection || c.generation != generation {
		c.connectionMu.Unlock()
		return errors.New("Codex App Server connection changed during initialization")
	}
	c.ready = generation
	c.connectionMu.Unlock()
	c.restoreWatched()
	return nil
}

func (c *Client) Close() {
	c.connectionMu.Lock()
	c.closed = true
	if c.observer != nil {
		c.observer.cancel()
		c.observer.clearQueue()
	}
	connection := c.connection
	generation := c.generation
	c.connectionMu.Unlock()
	if connection != nil {
		connection.CloseNow()
		c.markDisconnected(connection, generation, errors.New("client closed"))
	}
	c.queueMu.Lock()
	if c.queueLedgerLock != nil {
		_ = c.queueLedgerLock.Close()
		c.queueLedgerLock = nil
	}
	c.queueMu.Unlock()
}

func (c *Client) Request(ctx context.Context, method string, params any, result any) error {
	if c.options.ObserverOnly {
		switch method {
		case "thread/read", "thread/turns/list", "thread/items/list", "thread/list":
		default:
			return errors.New("observer client cannot mutate or answer a thread")
		}
	}
	if err := c.Ensure(ctx); err != nil {
		return err
	}
	return c.requestConnected(ctx, method, params, result)
}

func (c *Client) requestConnected(ctx context.Context, method string, params any, result any) error {
	return c.requestConnectedGeneration(ctx, 0, method, params, result)
}

func (c *Client) requestConnectedGeneration(ctx context.Context, expectedGeneration uint64, method string, params any, result any) error {
	c.connectionMu.Lock()
	connection := c.connection
	generation := c.generation
	ready := c.ready
	var disconnected context.Context
	if c.observer != nil {
		disconnected = c.observer.connectionContext
	}
	c.connectionMu.Unlock()
	if expectedGeneration != 0 && generation != expectedGeneration {
		return errors.New("Codex App Server connection changed before request admission")
	}
	if connection == nil || ready != generation {
		return errors.New("Codex App Server is disconnected or initializing")
	}
	if c.observer != nil {
		select {
		case c.observer.slots <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		case <-disconnected.Done():
			return errors.New("Codex App Server disconnected before request admission")
		}
		defer func() { <-c.observer.slots }()
		if err := ctx.Err(); err != nil {
			return err
		}
		if disconnected.Err() != nil {
			return errors.New("Codex App Server disconnected before request admission")
		}
		// Queue admission does not consume the observer's per-RPC timeout. The
		// caller deadline still bounds the entire operation, including its queue.
		admitted, cancel := context.WithTimeout(ctx, observerRPCTimeout)
		defer cancel()
		ctx = admitted
	}
	return c.requestOn(ctx, connection, generation, method, params, result)
}

func (c *Client) requestOn(
	ctx context.Context,
	connection *websocket.Conn,
	generation uint64,
	method string,
	params any,
	result any,
) error {
	id := c.nextID.Add(1)
	responseChannel := make(chan response, 1)
	c.pendingMu.Lock()
	c.pending[id] = pendingCall{generation: generation, channel: responseChannel}
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
	}()

	message, err := json.Marshal(map[string]any{"id": id, "method": method, "params": params})
	if err != nil {
		return err
	}
	if err := c.writeOn(ctx, connection, generation, message); err != nil {
		return err
	}
	select {
	case response := <-responseChannel:
		if response.err != nil {
			return response.err
		}
		if result == nil || len(response.result) == 0 {
			return nil
		}
		if err := json.Unmarshal(response.result, result); err != nil {
			return fmt.Errorf("decode %s response: %w", method, err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) writeOn(ctx context.Context, connection *websocket.Conn, generation uint64, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.connectionMu.Lock()
	current := c.connection
	currentGeneration := c.generation
	c.connectionMu.Unlock()
	if connection == nil || current != connection || currentGeneration != generation {
		return errors.New("Codex App Server connection changed before the request was sent")
	}
	if err := connection.Write(ctx, websocket.MessageText, data); err != nil {
		c.markDisconnected(connection, generation, err)
		return err
	}
	return nil
}

func (c *Client) readLoop(connection *websocket.Conn, generation uint64) {
	for {
		_, data, err := connection.Read(context.Background())
		if err != nil {
			c.markDisconnected(connection, generation, err)
			return
		}
		var message rpcMessage
		if err := json.Unmarshal(data, &message); err != nil {
			continue
		}
		if c.options.ActivityRecorder != nil {
			threadID := threadIDFromParams(message.Params)
			c.watchedMu.Lock()
			watched := c.watched[threadID] > 0
			c.watchedMu.Unlock()
			if watched {
				c.options.ActivityRecorder.observe(c.activityConnection(generation), message, time.Now().UnixMilli())
			}
		}
		if message.Method != "" && len(message.ID) != 0 {
			if c.options.ObserverOnly {
				c.broadcast(threadIDFromParams(message.Params))
				continue
			}
			key := string(message.ID)
			request := PendingRequest{
				ID: key, Method: message.Method, Params: message.Params,
				generation: generation, connection: connection, receivedAt: time.Now(),
			}
			prompt, promptErr := c.normalizePrompt(request)
			if promptErr != nil {
				c.rejectUnsupported(request, promptErr)
				continue
			}
			c.pendingMu.Lock()
			c.requests[key] = request
			c.pendingMu.Unlock()
			if prompt.Kind == "userInput" && !prompt.IsBlocking &&
				c.options.NonBlockingUserInput != nil {
				policy := c.options.NonBlockingUserInput
				go c.autoResolveUserInput(
					request.ID, prompt.ThreadID, policy.HiddenGrace+policy.VisibleCountdown,
				)
			}
			c.broadcast(prompt.ThreadID)
			continue
		}
		if message.Method == "serverRequest/resolved" {
			c.handleResolved(message.Params, generation)
		}
		if message.Method == "thread/settings/updated" {
			c.handleThreadSettingsUpdated(message.Params, generation)
		}
		if message.Method == "item/started" || message.Method == "item/completed" {
			c.observeItemTimestamp(message.Method, message.Params)
		}
		if message.Method != "" {
			c.broadcast(threadIDFromParams(message.Params))
		}
		if len(message.ID) == 0 {
			continue
		}
		var id uint64
		if err := json.Unmarshal(message.ID, &id); err != nil {
			continue
		}
		c.pendingMu.Lock()
		call, ok := c.pending[id]
		c.pendingMu.Unlock()
		if !ok || call.generation != generation {
			continue
		}
		if message.Error != nil {
			call.channel <- response{err: &rpcCallError{
				code: message.Error.Code, message: message.Error.Message,
			}}
		} else {
			call.channel <- response{result: message.Result}
		}
	}
}

func (c *Client) autoResolveUserInput(id, threadID string, delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	<-timer.C
	request, ok := c.claimAutoResolvableUserInput(id, threadID)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.finishResponse(ctx, request, map[string]any{
		"answers": map[string]map[string][]string{},
	})
}

func (c *Client) claimAutoResolvableUserInput(id, threadID string) (PendingRequest, bool) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	request, ok := c.requests[id]
	if !ok || request.claimed || request.snoozed {
		return PendingRequest{}, false
	}
	prompt, err := c.normalizePrompt(request)
	if err != nil || prompt.Kind != "userInput" || prompt.ThreadID != threadID || prompt.IsBlocking {
		return PendingRequest{}, false
	}
	request.claimed = true
	c.requests[id] = request
	return request, true
}

func (c *Client) SnoozeUserInput(id, threadID string) error {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	request, ok := c.requests[id]
	if !ok || request.claimed {
		return errors.New("pending request not found")
	}
	prompt, err := c.normalizePrompt(request)
	if err != nil {
		return err
	}
	if prompt.ThreadID != threadID || prompt.Kind != "userInput" {
		return errors.New("this request does not support auto-resolution control")
	}
	request.snoozed = true
	c.requests[id] = request
	return nil
}

func (c *Client) handleThreadSettingsUpdated(params json.RawMessage, generation uint64) {
	var notification struct {
		ThreadID       string `json:"threadId"`
		ThreadSettings struct {
			Model             string `json:"model"`
			Effort            string `json:"effort"`
			CollaborationMode struct {
				Mode string `json:"mode"`
			} `json:"collaborationMode"`
		} `json:"threadSettings"`
	}
	if json.Unmarshal(params, &notification) != nil || notification.ThreadID == "" ||
		notification.ThreadSettings.Model == "" ||
		notification.ThreadSettings.CollaborationMode.Mode == "" {
		return
	}
	settings := ThreadSettings{
		Model:             notification.ThreadSettings.Model,
		ReasoningEffort:   notification.ThreadSettings.Effort,
		CollaborationMode: notification.ThreadSettings.CollaborationMode.Mode,
	}
	c.cacheThreadSettings(notification.ThreadID, settings, generation)
}

func (c *Client) cacheThreadSettings(threadID string, settings ThreadSettings, generation uint64) {
	c.settingsMu.Lock()
	c.settingsRevision++
	c.threadSettings[threadID] = cachedThreadSettings{
		generation: generation, revision: c.settingsRevision, settings: settings,
	}
	close(c.settingsChanged)
	c.settingsChanged = make(chan struct{})
	c.settingsMu.Unlock()
}

func (c *Client) handleResolved(params json.RawMessage, generation uint64) {
	var resolved struct {
		RequestID json.RawMessage `json:"requestId"`
	}
	if json.Unmarshal(params, &resolved) != nil {
		return
	}
	c.pendingMu.Lock()
	request := c.requests[string(resolved.RequestID)]
	threadID := threadIDFromParams(request.Params)
	if request.generation == generation {
		delete(c.requests, string(resolved.RequestID))
	}
	c.pendingMu.Unlock()
	c.broadcast(threadID)
}

func (c *Client) rejectUnsupported(request PendingRequest, cause error) {
	threadID := threadIDFromParams(request.Params)
	if threadID != "" {
		prompt := Prompt{
			ID: request.ID, Method: request.Method, Kind: "unsupported",
			ThreadID: threadID, Error: cause.Error(),
		}
		_ = json.Unmarshal(request.Params, &prompt.Params)
		c.pendingMu.Lock()
		notices := append(c.notices[threadID], prompt)
		if len(notices) > 20 {
			notices = notices[len(notices)-20:]
		}
		c.notices[threadID] = notices
		c.pendingMu.Unlock()
	}
	var rawID any
	if json.Unmarshal([]byte(request.ID), &rawID) == nil {
		message, _ := json.Marshal(map[string]any{
			"id": rawID,
			"error": map[string]any{
				"code":    -32601,
				"message": "Codex web client does not support this App Server request",
			},
		})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.writeOn(ctx, request.connection, request.generation, message)
	}
	c.broadcast(threadID)
}

func threadIDFromParams(params json.RawMessage) string {
	var value struct {
		ThreadID string `json:"threadId"`
	}
	_ = json.Unmarshal(params, &value)
	return value.ThreadID
}

func (c *Client) markDisconnected(connection *websocket.Conn, generation uint64, cause error) {
	c.connectionMu.Lock()
	if c.connection != connection || c.generation != generation {
		c.connectionMu.Unlock()
		return
	}
	c.connection = nil
	c.ready = 0
	if c.observer != nil && c.observer.cancelConnection != nil {
		c.observer.cancelConnection()
		c.observer.clearQueue()
	}
	c.options.ActivityRecorder.disconnected(c.activityConnection(generation), "")
	c.pendingMu.Lock()
	for id, call := range c.pending {
		if call.generation != generation {
			continue
		}
		select {
		case call.channel <- response{err: fmt.Errorf("Codex App Server disconnected: %w", cause)}:
		default:
		}
		delete(c.pending, id)
	}
	for id, request := range c.requests {
		if request.generation == generation {
			delete(c.requests, id)
		}
	}
	c.pendingMu.Unlock()
	c.settingsMu.Lock()
	c.threadSettings = make(map[string]cachedThreadSettings)
	close(c.settingsChanged)
	c.settingsChanged = make(chan struct{})
	c.settingsMu.Unlock()
	c.connectionMu.Unlock()
	c.broadcast("")
}

func (c *Client) Subscribe(ctx context.Context, threadID string) (<-chan struct{}, func(), error) {
	if err := c.options.ActivityRecorder.retain(ctx, threadID, true); err != nil {
		return nil, nil, err
	}
	c.retainTurnHistory(threadID, true)
	channel := make(chan struct{}, 1)
	c.watchedMu.Lock()
	c.watched[threadID]++
	c.watchedMu.Unlock()
	c.subscribersMu.Lock()
	c.subscribers[channel] = threadID
	c.subscribersMu.Unlock()
	unsubscribe := func() {
		removed := false
		c.subscribersMu.Lock()
		if _, ok := c.subscribers[channel]; ok {
			delete(c.subscribers, channel)
			close(channel)
			removed = true
		}
		c.subscribersMu.Unlock()
		if removed {
			c.removeWatch(threadID)
		}
	}
	if err := c.Ensure(ctx); err != nil {
		unsubscribe()
		return nil, nil, err
	}
	if err := c.resumeWatched(ctx, threadID); err != nil {
		unsubscribe()
		return nil, nil, err
	}
	return channel, unsubscribe, nil
}

func (c *Client) removeWatch(threadID string) {
	defer c.options.ActivityRecorder.release(threadID, true)
	defer c.releaseTurnHistory(threadID, true)
	transition := c.watchLock(threadID)
	transition.Lock()
	defer transition.Unlock()
	c.watchedMu.Lock()
	removed := false
	if c.watched[threadID] <= 1 {
		delete(c.watched, threadID)
		delete(c.watchedGeneration, threadID)
		delete(c.liveItemTimes, threadID)
		removed = true
	} else {
		c.watched[threadID]--
	}
	c.watchedMu.Unlock()
	if removed {
		c.connectionMu.Lock()
		generation := c.generation
		c.connectionMu.Unlock()
		c.options.ActivityRecorder.disconnected(c.activityConnection(generation), threadID)
		c.options.ActivityRecorder.retire(threadID)
		c.invalidateThreadSettings(threadID)
		go c.unsubscribeThread(threadID)
	}
}

func (c *Client) invalidateThreadSettings(threadID string) {
	c.settingsMu.Lock()
	delete(c.threadSettings, threadID)
	close(c.settingsChanged)
	c.settingsChanged = make(chan struct{})
	c.settingsMu.Unlock()
}

func (c *Client) restoreWatched() {
	c.watchedMu.Lock()
	threadIDs := make([]string, 0, len(c.watched))
	for threadID := range c.watched {
		threadIDs = append(threadIDs, threadID)
	}
	c.watchedMu.Unlock()
	if c.observer != nil {
		c.restoreObserved(threadIDs)
		return
	}
	for _, threadID := range threadIDs {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := c.resumeWatched(ctx, threadID); err != nil {
				c.recordWatchError(threadID, err)
			}
		}()
	}
}

func (c *Client) resumeWatched(ctx context.Context, threadID string) error {
	return c.resumeWatchedGeneration(ctx, threadID, 0)
}

func (c *Client) resumeWatchedGeneration(ctx context.Context, threadID string, expectedGeneration uint64) error {
	lock := c.watchLock(threadID)
	lock.Lock()
	defer lock.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	c.connectionMu.Lock()
	generation := c.generation
	c.connectionMu.Unlock()
	if expectedGeneration != 0 && generation != expectedGeneration {
		return errors.New("Codex App Server connection changed before watch restoration")
	}
	c.watchedMu.Lock()
	if c.watched[threadID] == 0 || c.watchedGeneration[threadID] == generation {
		c.watchedMu.Unlock()
		return nil
	}
	c.watchedMu.Unlock()
	requestGeneration := uint64(0)
	if c.observer != nil {
		requestGeneration = generation
	}
	var result map[string]any
	if err := c.requestConnectedGeneration(ctx, requestGeneration, "thread/resume", c.threadResumeParams(threadID), &result); err != nil {
		return err
	}
	c.connectionMu.Lock()
	if c.connection == nil || c.generation != generation || c.ready != generation {
		// Observer restoration can win the same-watch race with Subscribe.
		// Share its accepted result even if the transport immediately closes;
		// the next generation still resumes again without stale coverage.
		if c.observer != nil && c.generation == generation {
			c.watchedMu.Lock()
			if c.watched[threadID] > 0 {
				c.watchedGeneration[threadID] = generation
			}
			c.watchedMu.Unlock()
		}
		c.connectionMu.Unlock()
		// The server accepted the subscription before disconnecting. Retain
		// the watch for reconnection, without starting coverage on this stale
		// transport or turning a successful Subscribe into an error.
		return nil
	}
	c.watchedMu.Lock()
	if c.watched[threadID] > 0 {
		c.watchedGeneration[threadID] = generation
	}
	c.watchedMu.Unlock()
	c.options.ActivityRecorder.connected(threadID, c.activityConnection(generation), time.Now().UnixMilli())
	if c.observer != nil {
		c.clearWatchError(threadID)
	}
	c.connectionMu.Unlock()
	if c.observer == nil {
		c.clearWatchError(threadID)
	}
	return nil
}

func (c *Client) unsubscribeThread(threadID string) {
	lock := c.watchLock(threadID)
	lock.Lock()
	defer lock.Unlock()
	c.watchedMu.Lock()
	if c.watched[threadID] > 0 {
		c.watchedMu.Unlock()
		return
	}
	c.watchedMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var result map[string]any
	_ = c.requestConnected(ctx, "thread/unsubscribe", map[string]any{"threadId": threadID}, &result)
	c.clearWatchError(threadID)
}

func (c *Client) watchLock(threadID string) *sync.Mutex {
	c.watchedMu.Lock()
	defer c.watchedMu.Unlock()
	lock := c.watchLocks[threadID]
	if lock == nil {
		lock = &sync.Mutex{}
		c.watchLocks[threadID] = lock
	}
	return lock
}

func (c *Client) recordWatchError(threadID string, err error) {
	c.pendingMu.Lock()
	message := Prompt{
		ID: "watch:" + threadID, Kind: "connection", ThreadID: threadID,
		Error: "Unable to resume this Codex thread: " + err.Error(),
	}
	notices := c.notices[threadID]
	replaced := false
	for index := range notices {
		if notices[index].ID == message.ID {
			notices[index] = message
			replaced = true
		}
	}
	if !replaced {
		notices = append(notices, message)
	}
	c.notices[threadID] = notices
	c.pendingMu.Unlock()
	c.broadcast(threadID)
}

func (c *Client) clearWatchError(threadID string) {
	c.pendingMu.Lock()
	notices := c.notices[threadID]
	for index := range notices {
		if notices[index].ID == "watch:"+threadID {
			notices = append(notices[:index], notices[index+1:]...)
			break
		}
	}
	c.notices[threadID] = notices
	c.pendingMu.Unlock()
}

func (c *Client) broadcast(threadID string) {
	c.subscribersMu.Lock()
	defer c.subscribersMu.Unlock()
	for channel, subscribedThreadID := range c.subscribers {
		if threadID != "" && subscribedThreadID != threadID {
			continue
		}
		select {
		case channel <- struct{}{}:
		default:
		}
	}
}

func (c *Client) Prompts(threadID string) []Prompt {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	result := make([]Prompt, 0, len(c.notices[threadID])+len(c.requests))
	result = append(result, c.notices[threadID]...)
	for _, request := range c.requests {
		if request.claimed {
			continue
		}
		prompt, err := c.normalizePrompt(request)
		if err == nil && prompt.ThreadID == threadID {
			result = append(result, prompt)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (c *Client) PromptsWithItems(ctx context.Context, threadID string) ([]Prompt, error) {
	prompts := c.Prompts(threadID)
	itemIDs := make([]string, 0, len(prompts))
	for _, prompt := range prompts {
		if requiresThreadItem(prompt.Kind) {
			itemIDs = append(itemIDs, prompt.ItemID)
		}
	}
	if len(itemIDs) == 0 {
		return prompts, nil
	}
	items, err := c.threadItems(ctx, threadID, itemIDs...)
	if err != nil {
		return nil, err
	}
	for index := range prompts {
		if !requiresThreadItem(prompts[index].Kind) {
			continue
		}
		item, ok := items[prompts[index].ItemID]
		if ok && itemTypeMatches(prompts[index].Kind, stringValue(item["type"])) {
			prompts[index].Item = item
			prompts[index].AuthorityAvailable = true
		}
	}
	return prompts, nil
}

func (c *Client) RespondDecision(ctx context.Context, id, threadID, decision string) error {
	request, prompt, err := c.claim(id, threadID)
	if err != nil {
		return err
	}
	if !slices.Contains(prompt.AvailableDecisions, decision) {
		c.releaseClaim(request)
		return errors.New("decision was not offered by the Codex App Server")
	}
	if requiresThreadItem(prompt.Kind) {
		items, itemErr := c.threadItems(ctx, threadID, prompt.ItemID)
		item, ok := items[prompt.ItemID]
		if itemErr != nil || !ok || !itemTypeMatches(prompt.Kind, stringValue(item["type"])) {
			c.releaseClaim(request)
			if itemErr != nil {
				return fmt.Errorf("load approval authority: %w", itemErr)
			}
			return errors.New("matching approval item is unavailable; review this request in the terminal")
		}
	}
	var result any
	switch prompt.Kind {
	case "command", "fileChange":
		result = map[string]any{"decision": decision}
	default:
		c.releaseClaim(request)
		return errors.New("this request does not accept a decision")
	}
	return c.finishResponse(ctx, request, result)
}

func (c *Client) RespondAnswers(ctx context.Context, id, threadID string, answers map[string]map[string][]string) error {
	request, prompt, err := c.claim(id, threadID)
	if err != nil {
		return err
	}
	if prompt.Kind != "userInput" {
		c.releaseClaim(request)
		return errors.New("this request does not accept answers")
	}
	if err := validateAnswers(prompt, answers); err != nil {
		c.releaseClaim(request)
		return err
	}
	return c.finishResponse(ctx, request, map[string]any{"answers": answers})
}

func validateAnswers(prompt Prompt, answers map[string]map[string][]string) error {
	if len(answers) != len(prompt.Questions) {
		return errors.New("answers do not match the offered questions")
	}
	for _, question := range prompt.Questions {
		answer, ok := answers[question.ID]
		values := answer["answers"]
		if !ok || len(answer) != 1 || len(values) > 2 {
			return fmt.Errorf("question %q has an invalid answer", question.ID)
		}
		if len(values) == 0 {
			continue
		}
		for _, value := range values {
			if strings.TrimSpace(value) == "" {
				return fmt.Errorf("question %q has a blank answer", question.ID)
			}
		}
		if len(question.Options) == 0 {
			if len(values) != 1 || !validUserNote(values[0]) {
				return fmt.Errorf("question %q requires one free-form answer", question.ID)
			}
			continue
		}
		offered := false
		for _, option := range question.Options {
			if option.Label == values[0] {
				offered = true
				break
			}
		}
		if !offered && !(question.IsOther && len(values) == 1 && validUserNote(values[0])) {
			return fmt.Errorf("answer to question %q was not offered", question.ID)
		}
		if offered && len(values) == 2 && !validUserNote(values[1]) {
			return fmt.Errorf("question %q has an invalid option note", question.ID)
		}
	}
	return nil
}

func validUserNote(value string) bool {
	const prefix = "user_note: "
	return strings.HasPrefix(value, prefix) && strings.TrimSpace(strings.TrimPrefix(value, prefix)) != ""
}

func (c *Client) claim(id, threadID string) (PendingRequest, Prompt, error) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	request, ok := c.requests[id]
	if !ok || request.claimed {
		return PendingRequest{}, Prompt{}, errors.New("pending request not found")
	}
	prompt, err := c.normalizePrompt(request)
	if err != nil {
		return PendingRequest{}, Prompt{}, err
	}
	if prompt.ThreadID != threadID {
		return PendingRequest{}, Prompt{}, errors.New("request belongs to another thread")
	}
	request.claimed = true
	c.requests[id] = request
	return request, prompt, nil
}

func (c *Client) releaseClaim(request PendingRequest) {
	c.pendingMu.Lock()
	current, ok := c.requests[request.ID]
	if ok && current.generation == request.generation {
		current.claimed = false
		c.requests[request.ID] = current
	}
	c.pendingMu.Unlock()
}

func (c *Client) finishResponse(ctx context.Context, request PendingRequest, result any) error {
	if c.options.ObserverOnly {
		return errors.New("observer client cannot answer a thread")
	}
	var rawID any
	if err := json.Unmarshal([]byte(request.ID), &rawID); err != nil {
		c.releaseClaim(request)
		return err
	}
	message, err := json.Marshal(map[string]any{"id": rawID, "result": result})
	if err != nil {
		c.releaseClaim(request)
		return err
	}
	if err := c.writeOn(ctx, request.connection, request.generation, message); err != nil {
		c.releaseClaim(request)
		return err
	}
	c.pendingMu.Lock()
	current, ok := c.requests[request.ID]
	if ok && current.generation == request.generation {
		delete(c.requests, request.ID)
	}
	c.pendingMu.Unlock()
	c.broadcast(requestThreadID(request))
	return nil
}

func normalizePrompt(request PendingRequest) (Prompt, error) {
	var params map[string]any
	if err := json.Unmarshal(request.Params, &params); err != nil {
		return Prompt{}, err
	}
	prompt := Prompt{
		ID: request.ID, Method: request.Method, ThreadID: stringValue(params["threadId"]),
		ItemID: stringValue(params["itemId"]), Params: params,
	}
	switch request.Method {
	case "item/commandExecution/requestApproval":
		prompt.Kind = "command"
		decisions, decisionsPresent := params["availableDecisions"].([]any)
		if decisionsPresent {
			for _, decision := range decisions {
				if value, ok := decision.(string); ok && slices.Contains([]string{"accept", "acceptForSession", "decline", "cancel"}, value) {
					prompt.AvailableDecisions = append(prompt.AvailableDecisions, value)
				}
			}
		} else {
			prompt.AvailableDecisions = []string{"accept", "acceptForSession", "decline", "cancel"}
		}
	case "item/fileChange/requestApproval":
		prompt.Kind = "fileChange"
		prompt.AvailableDecisions = []string{"accept", "acceptForSession", "decline", "cancel"}
	case "item/permissions/requestApproval":
		prompt.Kind = "terminalOnly"
	case "item/tool/requestUserInput":
		prompt.Kind = "userInput"
		prompt.AuthorityAvailable = true
		prompt.IsBlocking = true
		if raw, exists := params["isBlocking"]; exists {
			blocking, ok := raw.(bool)
			if !ok {
				return Prompt{}, errors.New("request_user_input has an invalid isBlocking value")
			}
			prompt.IsBlocking = blocking
		}
		if raw, exists := params["autoResolutionMs"]; exists && raw != nil {
			milliseconds, ok := raw.(float64)
			if !ok || milliseconds < 0 || milliseconds != float64(uint64(milliseconds)) {
				return Prompt{}, errors.New("request_user_input has an invalid autoResolutionMs value")
			}
			value := uint64(milliseconds)
			prompt.AutoResolutionMS = &value
		}
		questions, err := normalizeQuestions(params["questions"])
		if err != nil {
			return Prompt{}, err
		}
		prompt.Questions = questions
	default:
		return Prompt{}, fmt.Errorf("unsupported request method %q", request.Method)
	}
	if prompt.ThreadID == "" {
		return Prompt{}, errors.New("request has no thread id")
	}
	return prompt, nil
}

func (c *Client) normalizePrompt(request PendingRequest) (Prompt, error) {
	prompt, err := normalizePrompt(request)
	policy := c.options.NonBlockingUserInput
	if err != nil || policy == nil || prompt.Kind != "userInput" || prompt.IsBlocking ||
		request.receivedAt.IsZero() {
		return prompt, err
	}
	visibleAt := request.receivedAt.Add(policy.HiddenGrace)
	deadline := visibleAt.Add(policy.VisibleCountdown)
	prompt.AutoResolutionVisibleAtMS = visibleAt.UnixMilli()
	prompt.AutoResolutionAtMS = deadline.UnixMilli()
	prompt.AutoResolveSnoozed = request.snoozed
	return prompt, nil
}

func normalizeQuestions(value any) ([]Question, error) {
	rawQuestions, ok := value.([]any)
	if !ok || len(rawQuestions) == 0 {
		return nil, errors.New("request_user_input has no questions")
	}
	questions := make([]Question, 0, len(rawQuestions))
	seen := make(map[string]struct{})
	for _, raw := range rawQuestions {
		item, ok := raw.(map[string]any)
		if !ok {
			return nil, errors.New("request_user_input contains an invalid question")
		}
		question := Question{
			ID: stringValue(item["id"]), Header: stringValue(item["header"]),
			Question: stringValue(item["question"]),
		}
		question.IsSecret, _ = item["isSecret"].(bool)
		question.IsOther, _ = item["isOther"].(bool)
		if question.ID == "" || question.Header == "" || question.Question == "" {
			return nil, errors.New("request_user_input question is missing required text")
		}
		if _, ok := seen[question.ID]; ok {
			return nil, fmt.Errorf("request_user_input repeats question id %q", question.ID)
		}
		seen[question.ID] = struct{}{}
		if rawOptions, ok := item["options"].([]any); ok {
			for _, rawOption := range rawOptions {
				option, ok := rawOption.(map[string]any)
				if !ok || stringValue(option["label"]) == "" {
					return nil, fmt.Errorf("request_user_input question %q has an invalid option", question.ID)
				}
				question.Options = append(question.Options, Option{
					Label: stringValue(option["label"]), Description: stringValue(option["description"]),
				})
			}
		}
		questions = append(questions, question)
	}
	return questions, nil
}

func requiresThreadItem(kind string) bool {
	return kind == "command" || kind == "fileChange"
}

func itemTypeMatches(kind, itemType string) bool {
	return (kind == "command" && itemType == "commandExecution") ||
		(kind == "fileChange" && itemType == "fileChange")
}

func requestThreadID(request PendingRequest) string {
	return threadIDFromParams(request.Params)
}

func (c *Client) threadItems(
	ctx context.Context, threadID string, itemIDs ...string,
) (map[string]map[string]any, error) {
	wanted := make(map[string]struct{}, len(itemIDs))
	for _, itemID := range itemIDs {
		wanted[itemID] = struct{}{}
	}
	if len(wanted) == 0 {
		return map[string]map[string]any{}, nil
	}
	items := make(map[string]map[string]any, len(wanted))
	err := c.walkThreadItems(ctx, threadID, func(entry threadItemEntry) (bool, error) {
		id := stringValue(entry.Item["id"])
		if _, ok := wanted[id]; ok {
			items[id] = entry.Item
			delete(wanted, id)
		}
		return len(wanted) == 0, nil
	})
	return items, err
}

func (c *Client) walkThreadItems(
	ctx context.Context, threadID string, visit func(threadItemEntry) (bool, error),
) error {
	seenCursors := make(map[string]struct{})
	seenIDs := make(map[string]struct{})
	var cursor string
	for {
		params := map[string]any{
			"threadId": threadID, "limit": 100, "sortDirection": "desc",
		}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var page struct {
			Data       *[]threadItemEntry `json:"data"`
			NextCursor *string            `json:"nextCursor"`
		}
		if err := c.Request(ctx, "thread/items/list", params, &page); err != nil {
			return err
		}
		if page.Data == nil {
			return errors.New("thread/items/list returned no data")
		}
		for _, entry := range *page.Data {
			id := stringValue(entry.Item["id"])
			if entry.TurnID == "" || entry.Item == nil || id == "" {
				return errors.New("thread/items/list returned an invalid item entry")
			}
			if _, exists := seenIDs[id]; exists {
				return fmt.Errorf("thread/items/list repeated item %q", id)
			}
			seenIDs[id] = struct{}{}
			stop, err := visit(entry)
			if err != nil {
				return err
			}
			if stop {
				return nil
			}
		}
		if page.NextCursor == nil {
			return nil
		}
		if *page.NextCursor == "" {
			return errors.New("thread/items/list returned an empty pagination cursor")
		}
		if _, exists := seenCursors[*page.NextCursor]; exists {
			return errors.New("thread/items/list repeated a pagination cursor")
		}
		seenCursors[*page.NextCursor] = struct{}{}
		cursor = *page.NextCursor
	}
}

func inputText(inputs []map[string]any) string {
	parts := make([]string, 0, len(inputs))
	for _, input := range inputs {
		if stringValue(input["type"]) != "text" {
			parts = append(parts, "[non-text input]")
			continue
		}
		parts = append(parts, stringValue(input["text"]))
	}
	return strings.Join(parts, "\n")
}

func userMessageText(item map[string]any) (string, error) {
	content, ok := item["content"].([]any)
	if !ok {
		return "", errors.New("user message has invalid content")
	}
	inputs := make([]map[string]any, 0, len(content))
	for _, value := range content {
		input, ok := value.(map[string]any)
		if !ok {
			return "", errors.New("user message has invalid input")
		}
		inputs = append(inputs, input)
	}
	return inputText(inputs), nil
}

func (c *Client) queueUpdateLock(threadID string) *sync.Mutex {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	lock := c.queueUpdates[threadID]
	if lock == nil {
		lock = &sync.Mutex{}
		c.queueUpdates[threadID] = lock
	}
	return lock
}

func queueTextDigest(text string) string {
	digest := sha256.Sum256([]byte(text))
	return fmt.Sprintf("%x", digest)
}

func (c *Client) loadQueueLedgerLocked() error {
	c.connectionMu.Lock()
	closed := c.closed
	c.connectionMu.Unlock()
	if closed {
		return errors.New("Codex client is closed")
	}
	if c.queueLedgerLock != nil {
		return errors.New("submission ledger transaction is already active")
	}
	lockPath := c.queueLedgerPath + ".lock"
	lockFD, err := unix.Open(
		lockPath, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600,
	)
	if err != nil {
		return fmt.Errorf("open submission ledger lock: %w", err)
	}
	lockFile := os.NewFile(uintptr(lockFD), lockPath)
	lockInfo, err := lockFile.Stat()
	if err != nil || !lockInfo.Mode().IsRegular() || lockInfo.Mode().Perm() != 0o600 {
		_ = lockFile.Close()
		return errors.New("submission ledger lock must be a mode-0600 regular file")
	}
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX); err != nil {
		_ = lockFile.Close()
		return fmt.Errorf("lock submission ledger: %w", err)
	}
	c.queueLedgerLock = lockFile
	info, err := os.Lstat(c.queueLedgerPath)
	if errors.Is(err, os.ErrNotExist) {
		c.queueAttempts = make(map[string]map[string]string)
		c.sendAttempts = make(map[string]map[string]sendAttempt)
		c.queueDeletions = make(map[string]map[string]queueDeletionAttempt)
		c.operations = make(map[string]string)
		return nil
	}
	if err != nil {
		return c.failQueueLedgerLocked(fmt.Errorf("inspect queue attempt ledger: %w", err))
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return c.failQueueLedgerLocked(
			errors.New("queue attempt ledger must be a mode-0600 regular file"),
		)
	}
	if info.Size() > queueLedgerMaxSize {
		return c.failQueueLedgerLocked(errors.New("queue attempt ledger exceeds 1 MiB"))
	}
	file, err := os.Open(c.queueLedgerPath)
	if err != nil {
		return c.failQueueLedgerLocked(fmt.Errorf("open queue attempt ledger: %w", err))
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var ledger queueAttemptLedger
	if err := decoder.Decode(&ledger); err != nil {
		return c.failQueueLedgerLocked(fmt.Errorf("decode queue attempt ledger: %w", err))
	}
	if ledger.Schema != 3 || ledger.Attempts == nil {
		return c.failQueueLedgerLocked(errors.New("queue attempt ledger has an invalid schema"))
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return c.failQueueLedgerLocked(
			errors.New("queue attempt ledger contains trailing JSON data"),
		)
	}
	if ledger.Sends == nil {
		ledger.Sends = make(map[string]map[string]sendAttempt)
	}
	if ledger.Operations == nil {
		ledger.Operations = make(map[string]string)
	}
	if ledger.Deletions == nil {
		ledger.Deletions = make(map[string]map[string]queueDeletionAttempt)
	}
	for threadID, attempts := range ledger.Attempts {
		if threadID == "" || attempts == nil {
			return c.failQueueLedgerLocked(
				errors.New("queue attempt ledger contains an invalid thread"),
			)
		}
		for clientID, digest := range attempts {
			if clientID == "" || !validSubmissionDigest(digest) {
				return c.failQueueLedgerLocked(
					errors.New("queue attempt ledger contains an invalid attempt"),
				)
			}
		}
	}
	for threadID, attempts := range ledger.Sends {
		if threadID == "" || attempts == nil {
			return c.failQueueLedgerLocked(
				errors.New("queue attempt ledger contains an invalid send thread"),
			)
		}
		for clientID, attempt := range attempts {
			if clientID == "" || !validSubmissionDigest(attempt.Digest) ||
				!validSendAttemptState(attempt) {
				return c.failQueueLedgerLocked(
					errors.New("queue attempt ledger contains an invalid send attempt"),
				)
			}
		}
	}
	for threadID, attempts := range ledger.Deletions {
		if threadID == "" || attempts == nil {
			return c.failQueueLedgerLocked(
				errors.New("queue attempt ledger contains an invalid deletion thread"),
			)
		}
		for queuedID, attempt := range attempts {
			if queuedID == "" || attempt.ClientUserMessageID == "" ||
				!validSubmissionDigest(attempt.Digest) {
				return c.failQueueLedgerLocked(
					errors.New("queue attempt ledger contains an invalid deletion attempt"),
				)
			}
		}
	}
	for key, value := range ledger.Operations {
		if !filepath.IsAbs(key) || value == "" {
			return c.failQueueLedgerLocked(
				errors.New("queue attempt ledger contains an invalid operation marker"),
			)
		}
	}
	c.queueAttempts = ledger.Attempts
	c.sendAttempts = ledger.Sends
	c.queueDeletions = ledger.Deletions
	c.operations = ledger.Operations
	return nil
}

func (c *Client) releaseQueueLedgerLocked() {
	if c.queueLedgerLock == nil {
		return
	}
	_ = c.queueLedgerLock.Close()
	c.queueLedgerLock = nil
}

func (c *Client) failQueueLedgerLocked(err error) error {
	c.releaseQueueLedgerLocked()
	return err
}

func queueLedgerTransaction[T any](
	c *Client, transaction func() (T, error),
) (T, error) {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	if err := c.loadQueueLedgerLocked(); err != nil {
		var zero T
		return zero, err
	}
	defer c.releaseQueueLedgerLocked()
	return transaction()
}

func queueLedgerAction(c *Client, action func() error) error {
	_, err := queueLedgerTransaction(c, func() (struct{}, error) {
		return struct{}{}, action()
	})
	return err
}

func validSendAttemptState(attempt sendAttempt) bool {
	switch attempt.State {
	case "prepared", "submitting":
		return attempt.TurnID == ""
	case "accepted":
		return attempt.TurnID != ""
	default:
		return false
	}
}

func (c *Client) writeQueueLedgerLocked() error {
	if c.queueLedgerLock == nil {
		return errors.New("submission ledger transaction is not active")
	}
	directory := filepath.Dir(c.queueLedgerPath)
	encoded, err := json.Marshal(queueAttemptLedger{
		Schema: 3, Attempts: c.queueAttempts, Sends: c.sendAttempts,
		Deletions: c.queueDeletions, Operations: c.operations,
	})
	if err != nil {
		return fmt.Errorf("encode queue attempt ledger: %w", err)
	}
	encoded = append(encoded, '\n')
	if len(encoded) > queueLedgerMaxSize {
		return errors.New("queue attempt ledger would exceed 1 MiB")
	}
	temporary, err := os.CreateTemp(directory, ".submission-attempts-*")
	if err != nil {
		return fmt.Errorf("create queue attempt ledger: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return fmt.Errorf("protect queue attempt ledger: %w", err)
	}
	if _, err := temporary.Write(encoded); err != nil {
		temporary.Close()
		return fmt.Errorf("write queue attempt ledger: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("sync queue attempt ledger: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close queue attempt ledger: %w", err)
	}
	if err := os.Rename(temporaryPath, c.queueLedgerPath); err != nil {
		return fmt.Errorf("replace queue attempt ledger: %w", err)
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open queue attempt ledger directory: %w", err)
	}
	defer directoryHandle.Close()
	if err := directoryHandle.Sync(); err != nil {
		return fmt.Errorf("sync queue attempt ledger directory: %w", err)
	}
	return nil
}

func validSubmissionDigest(digest string) bool {
	if len(digest) != 64 {
		return false
	}
	for _, character := range digest {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

func (c *Client) queueAttempt(threadID, clientID, text string) (bool, error) {
	return queueLedgerTransaction(c, func() (bool, error) {
		digest, ok := c.queueAttempts[threadID][clientID]
		if ok && digest != queueTextDigest(text) {
			return false, errors.New("queued message identity was reused with different text")
		}
		return ok, nil
	})
}

func (c *Client) recordQueueAttempt(threadID, clientID, text string) error {
	return queueLedgerAction(c, func() error {
		attempts := c.queueAttempts
		digest := queueTextDigest(text)
		if existing, ok := attempts[threadID][clientID]; ok {
			if existing != digest {
				return errors.New("queued message identity was reused with different text")
			}
			return nil
		}
		if attempts[threadID] == nil {
			attempts[threadID] = make(map[string]string)
		}
		attempts[threadID][clientID] = digest
		if err := c.writeQueueLedgerLocked(); err != nil {
			delete(attempts[threadID], clientID)
			if len(attempts[threadID]) == 0 {
				delete(attempts, threadID)
			}
			return err
		}
		return nil
	})
}

func (c *Client) sendAttempt(
	threadID, clientID, text, context string,
) (sendAttempt, bool, error) {
	type result struct {
		attempt sendAttempt
		found   bool
	}
	value, err := queueLedgerTransaction(c, func() (result, error) {
		attempt, ok := c.sendAttempts[threadID][clientID]
		if ok && (attempt.Digest != queueTextDigest(text) || attempt.Context != context) {
			return result{}, errors.New("message identity was reused for another action")
		}
		return result{attempt: attempt, found: ok}, nil
	})
	return value.attempt, value.found, err
}

func (c *Client) recordSendAttempt(
	threadID, clientID, text, context string, steered bool,
) error {
	return queueLedgerAction(c, func() error {
		digest := queueTextDigest(text)
		if existing, ok := c.sendAttempts[threadID][clientID]; ok {
			if existing.Digest != digest || existing.Context != context || existing.Steered != steered {
				return errors.New("message identity was reused for another action")
			}
			return nil
		}
		if c.sendAttempts[threadID] == nil {
			c.sendAttempts[threadID] = make(map[string]sendAttempt)
		}
		c.sendAttempts[threadID][clientID] = sendAttempt{
			Digest: digest, State: "prepared", Context: context, Steered: steered,
		}
		if err := c.writeQueueLedgerLocked(); err != nil {
			delete(c.sendAttempts[threadID], clientID)
			if len(c.sendAttempts[threadID]) == 0 {
				delete(c.sendAttempts, threadID)
			}
			return err
		}
		return nil
	})
}

func (c *Client) markSendSubmitting(threadID, clientID string) error {
	return queueLedgerAction(c, func() error {
		attempt, ok := c.sendAttempts[threadID][clientID]
		if !ok {
			return errors.New("prepared message attempt disappeared")
		}
		if attempt.State == "submitting" {
			return nil
		}
		if attempt.State != "prepared" {
			return errors.New("message attempt was already accepted")
		}
		attempt.State = "submitting"
		c.sendAttempts[threadID][clientID] = attempt
		if err := c.writeQueueLedgerLocked(); err != nil {
			attempt.State = "prepared"
			c.sendAttempts[threadID][clientID] = attempt
			return err
		}
		return nil
	})
}

func (c *Client) markSendAccepted(threadID, clientID string, receipt SendReceipt) error {
	return queueLedgerAction(c, func() error {
		attempt, ok := c.sendAttempts[threadID][clientID]
		if !ok {
			return errors.New("submitted message attempt disappeared")
		}
		if receipt.TurnID == "" || receipt.ClientUserMessageID != clientID ||
			receipt.Steered != attempt.Steered {
			return errors.New("accepted message receipt does not match its attempt")
		}
		if attempt.State == "accepted" {
			if attempt.TurnID != receipt.TurnID {
				return errors.New("message identity was accepted by another turn")
			}
			return nil
		}
		if attempt.State != "submitting" {
			return errors.New("message attempt was accepted before submission")
		}
		previous := attempt
		attempt.State = "accepted"
		attempt.TurnID = receipt.TurnID
		c.sendAttempts[threadID][clientID] = attempt
		if err := c.writeQueueLedgerLocked(); err != nil {
			c.sendAttempts[threadID][clientID] = previous
			return err
		}
		return nil
	})
}

// AcknowledgeSends releases durable attempts only after their browser-owned
// identities and text are also proven in the Codex transcript. An accepted
// receipt additionally has to match its recorded turn. This server-side proof
// keeps a lost acknowledgement response safely retryable after compaction.
func (c *Client) AcknowledgeSends(
	ctx context.Context, threadID string, acknowledgements []SendAcknowledgement,
) ([]string, error) {
	lock := c.queueUpdateLock(threadID)
	lock.Lock()
	defer lock.Unlock()

	digests := make(map[string]string, len(acknowledgements))
	expectedTurns := make(map[string]string)
	if err := queueLedgerAction(c, func() error {
		for _, acknowledgement := range acknowledgements {
			digest := acknowledgement.Digest
			if !validSubmissionDigest(digest) {
				return errors.New("message acknowledgement has an invalid digest")
			}
			digests[acknowledgement.ClientUserMessageID] = digest
			attempt, found := c.sendAttempts[threadID][acknowledgement.ClientUserMessageID]
			if found && attempt.Digest != digest {
				return errors.New("message identity was reused with different text")
			}
			if found && attempt.State == "accepted" {
				expectedTurns[acknowledgement.ClientUserMessageID] = attempt.TurnID
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	proven := make(map[string]string, len(digests))
	if len(digests) > 0 {
		err := c.walkThreadItems(ctx, threadID, func(entry threadItemEntry) (bool, error) {
			if stringValue(entry.Item["type"]) != "userMessage" {
				return false, nil
			}
			clientID := stringValue(entry.Item["clientId"])
			digest, wanted := digests[clientID]
			if !wanted {
				return false, nil
			}
			if entry.TurnID == "" {
				return false, errors.New("acknowledged message has no turn identity")
			}
			if expectedTurn := expectedTurns[clientID]; expectedTurn != "" &&
				entry.TurnID != expectedTurn {
				return false, errors.New("message identity was accepted by another turn")
			}
			text, err := userMessageText(entry.Item)
			if err != nil {
				return false, fmt.Errorf("acknowledged message has invalid history: %w", err)
			}
			if queueTextDigest(text) != digest {
				return false, errors.New("message identity was reused with different text")
			}
			proven[clientID] = entry.TurnID
			return len(proven) == len(digests), nil
		})
		if err != nil {
			return nil, err
		}
	}

	return queueLedgerTransaction(c, func() ([]string, error) {
		removed := make(map[string]sendAttempt)
		for clientID := range proven {
			attempt, found := c.sendAttempts[threadID][clientID]
			if !found {
				continue
			}
			if attempt.Digest != digests[clientID] {
				return nil, errors.New("message identity changed during acknowledgement")
			}
			if attempt.State == "accepted" && attempt.TurnID != proven[clientID] {
				return nil, errors.New("message identity was accepted by another turn")
			}
			removed[clientID] = attempt
			delete(c.sendAttempts[threadID], clientID)
		}
		if len(c.sendAttempts[threadID]) == 0 {
			delete(c.sendAttempts, threadID)
		}
		if len(removed) > 0 {
			if err := c.writeQueueLedgerLocked(); err != nil {
				if c.sendAttempts[threadID] == nil {
					c.sendAttempts[threadID] = make(map[string]sendAttempt)
				}
				for clientID, attempt := range removed {
					c.sendAttempts[threadID][clientID] = attempt
				}
				return nil, err
			}
		}
		acknowledged := make([]string, 0, len(proven))
		for _, acknowledgement := range acknowledgements {
			if _, found := proven[acknowledgement.ClientUserMessageID]; found {
				acknowledged = append(acknowledged, acknowledgement.ClientUserMessageID)
			}
		}
		return acknowledged, nil
	})
}

func (c *Client) clearQueueAttempt(threadID, clientID string) error {
	return queueLedgerAction(c, func() error {
		attempts := c.queueAttempts[threadID]
		digest, ok := attempts[clientID]
		if !ok {
			return nil
		}
		delete(attempts, clientID)
		if len(attempts) == 0 {
			delete(c.queueAttempts, threadID)
		}
		if err := c.writeQueueLedgerLocked(); err != nil {
			if c.queueAttempts[threadID] == nil {
				c.queueAttempts[threadID] = make(map[string]string)
			}
			c.queueAttempts[threadID][clientID] = digest
			return err
		}
		return nil
	})
}

func (c *Client) queueDeletionAttempt(
	threadID, queuedSubmissionID string,
) (queueDeletionAttempt, bool, error) {
	type result struct {
		attempt queueDeletionAttempt
		found   bool
	}
	value, err := queueLedgerTransaction(c, func() (result, error) {
		attempt, ok := c.queueDeletions[threadID][queuedSubmissionID]
		return result{attempt: attempt, found: ok}, nil
	})
	return value.attempt, value.found, err
}

func (c *Client) recordQueueDeletionAttempt(
	threadID string, target QueueEntry,
) (queueDeletionAttempt, error) {
	return queueLedgerTransaction(c, func() (queueDeletionAttempt, error) {
		attempt := queueDeletionAttempt{
			ClientUserMessageID: target.ClientUserMessageID,
			Digest:              queueTextDigest(target.Text),
		}
		if existing, ok := c.queueDeletions[threadID][target.ID]; ok {
			if existing != attempt {
				return queueDeletionAttempt{}, errors.New("queued deletion identity changed")
			}
			return existing, nil
		}
		if c.queueDeletions[threadID] == nil {
			c.queueDeletions[threadID] = make(map[string]queueDeletionAttempt)
		}
		c.queueDeletions[threadID][target.ID] = attempt
		if err := c.writeQueueLedgerLocked(); err != nil {
			delete(c.queueDeletions[threadID], target.ID)
			if len(c.queueDeletions[threadID]) == 0 {
				delete(c.queueDeletions, threadID)
			}
			return queueDeletionAttempt{}, err
		}
		return attempt, nil
	})
}

func (c *Client) clearQueueDeletionAndAttempt(
	threadID, queuedSubmissionID, clientID string,
) error {
	return queueLedgerAction(c, func() error {
		deletion, deleting := c.queueDeletions[threadID][queuedSubmissionID]
		digest, queued := c.queueAttempts[threadID][clientID]
		if deleting && deletion.ClientUserMessageID != clientID {
			return errors.New("queued deletion is bound to another message")
		}
		delete(c.queueDeletions[threadID], queuedSubmissionID)
		if len(c.queueDeletions[threadID]) == 0 {
			delete(c.queueDeletions, threadID)
		}
		delete(c.queueAttempts[threadID], clientID)
		if len(c.queueAttempts[threadID]) == 0 {
			delete(c.queueAttempts, threadID)
		}
		if err := c.writeQueueLedgerLocked(); err != nil {
			if deleting {
				if c.queueDeletions[threadID] == nil {
					c.queueDeletions[threadID] = make(map[string]queueDeletionAttempt)
				}
				c.queueDeletions[threadID][queuedSubmissionID] = deletion
			}
			if queued {
				if c.queueAttempts[threadID] == nil {
					c.queueAttempts[threadID] = make(map[string]string)
				}
				c.queueAttempts[threadID][clientID] = digest
			}
			return err
		}
		return nil
	})
}

func (c *Client) RequireSubmissionAttemptsResolved(ctx context.Context, threadID string) error {
	deletionIDs, err := queueLedgerTransaction(c, func() ([]string, error) {
		ids := make([]string, 0, len(c.queueDeletions[threadID]))
		for queuedSubmissionID := range c.queueDeletions[threadID] {
			ids = append(ids, queuedSubmissionID)
		}
		return ids, nil
	})
	if err != nil {
		return err
	}
	for _, queuedSubmissionID := range deletionIDs {
		if err := c.DeleteQueueEntry(ctx, threadID, queuedSubmissionID); err != nil {
			return fmt.Errorf("reconcile queued message deletion: %w", err)
		}
	}

	type resolutionSnapshot struct {
		queueAttempts       map[string]string
		unresolvedSendCount int
	}
	snapshot, err := queueLedgerTransaction(c, func() (resolutionSnapshot, error) {
		attempts := make(map[string]string, len(c.queueAttempts[threadID]))
		for clientID, digest := range c.queueAttempts[threadID] {
			attempts[clientID] = digest
		}
		unresolved := 0
		for _, attempt := range c.sendAttempts[threadID] {
			if attempt.State != "accepted" {
				unresolved++
			}
		}
		return resolutionSnapshot{
			queueAttempts: attempts, unresolvedSendCount: unresolved,
		}, nil
	})
	if err != nil {
		return err
	}
	if snapshot.unresolvedSendCount > 0 {
		return fmt.Errorf(
			"Codex thread %s has %d unresolved message attempt(s)",
			threadID, snapshot.unresolvedSendCount,
		)
	}
	queueAttempts := snapshot.queueAttempts
	if len(queueAttempts) == 0 {
		return nil
	}

	resolved := make(map[string]string)
	err = c.walkThreadItems(ctx, threadID, func(entry threadItemEntry) (bool, error) {
		if stringValue(entry.Item["type"]) != "userMessage" {
			return false, nil
		}
		clientID := stringValue(entry.Item["clientId"])
		if _, tracked := queueAttempts[clientID]; !tracked {
			return false, nil
		}
		text, err := userMessageText(entry.Item)
		if err != nil {
			return false, fmt.Errorf("queued message attempt has invalid history: %w", err)
		}
		digest := queueTextDigest(text)
		if existing, ok := resolved[clientID]; ok && existing != digest {
			return false, errors.New("queued message identity was reused with different text")
		}
		resolved[clientID] = digest
		return len(resolved) == len(queueAttempts), nil
	})
	if err != nil {
		return fmt.Errorf("reconcile queued message attempts: %w", err)
	}
	unknown := 0
	for clientID, digest := range queueAttempts {
		resolvedDigest, ok := resolved[clientID]
		if ok && resolvedDigest != digest {
			return errors.New("queued message identity was reused with different text")
		}
		if !ok {
			unknown++
		}
	}
	if unknown > 0 {
		return fmt.Errorf("Codex thread %s has %d unresolved queued message attempt(s)", threadID, unknown)
	}
	return nil
}

// ThreadOperationAttempt returns the thread bound to a durable operation for
// directory. Directory is the canonical absolute working directory used as
// the operation identity.
func (c *Client) ThreadOperationAttempt(directory string) (string, error) {
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return "", errors.New("thread operation directory must be canonical and absolute")
	}
	return queueLedgerTransaction(c, func() (string, error) {
		return c.operations[directory], nil
	})
}

// RecordThreadOperationAttempt durably binds an operation for directory to one
// thread. Repeating the same binding is safe; rebinding fails closed.
func (c *Client) RecordThreadOperationAttempt(directory, threadID string) error {
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return errors.New("thread operation directory must be canonical and absolute")
	}
	if strings.TrimSpace(threadID) == "" {
		return errors.New("thread operation requires a thread id")
	}
	return queueLedgerAction(c, func() error {
		if existing := c.operations[directory]; existing != "" {
			if existing != threadID {
				return errors.New("thread operation directory is already bound to another thread")
			}
			return nil
		}
		c.operations[directory] = threadID
		if err := c.writeQueueLedgerLocked(); err != nil {
			delete(c.operations, directory)
			return err
		}
		return nil
	})
}

// ClearConversationAttempts clears all durable submission and operation state
// for threadID after the caller has proved that the conversation is retired.
func (c *Client) ClearConversationAttempts(threadID, directory string) error {
	if strings.TrimSpace(threadID) == "" {
		return errors.New("conversation attempt cleanup requires a thread id")
	}
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return errors.New("conversation attempt cleanup directory must be canonical and absolute")
	}
	return queueLedgerAction(c, func() error {
		queueAttempts, queued := c.queueAttempts[threadID]
		sendAttempts, sent := c.sendAttempts[threadID]
		queueDeletions, deleting := c.queueDeletions[threadID]
		operationValue, operationPending := c.operations[directory]
		if operationPending && operationValue != threadID {
			return errors.New("operation key is bound to another conversation")
		}
		if !queued && !sent && !deleting && !operationPending {
			return nil
		}
		delete(c.queueAttempts, threadID)
		delete(c.sendAttempts, threadID)
		delete(c.queueDeletions, threadID)
		delete(c.operations, directory)
		if err := c.writeQueueLedgerLocked(); err != nil {
			if queued {
				c.queueAttempts[threadID] = queueAttempts
			}
			if sent {
				c.sendAttempts[threadID] = sendAttempts
			}
			if deleting {
				c.queueDeletions[threadID] = queueDeletions
			}
			if operationPending {
				c.operations[directory] = operationValue
			}
			return err
		}
		return nil
	})
}

func stringValue(value any) string {
	result, _ := value.(string)
	return result
}

func (c *Client) settingsParams(settings ThreadSettings, params map[string]any, config map[string]any) {
	if c.options.DeveloperInstructions != "" {
		params["developerInstructions"] = c.options.DeveloperInstructions
	}
	if settings.Model != "" {
		params["model"] = settings.Model
	}
	if settings.ReasoningEffort != "" {
		config["model_reasoning_effort"] = settings.ReasoningEffort
	}
}

func (c *Client) addRuntimeWorkspaceRoots(params map[string]any) error {
	if len(c.options.RuntimeWorkspaceRoots) == 0 {
		return nil
	}
	roots := make([]string, 0, len(c.options.RuntimeWorkspaceRoots))
	for _, root := range c.options.RuntimeWorkspaceRoots {
		if !filepath.IsAbs(root) || filepath.Clean(root) != root {
			return fmt.Errorf("runtime workspace root is not canonical: %q", root)
		}
		if !slices.Contains(roots, root) {
			roots = append(roots, root)
		}
	}
	params["runtimeWorkspaceRoots"] = roots
	return nil
}

func (c *Client) StartThread(ctx context.Context, cwd string, environment map[string]string) (string, error) {
	return c.StartThreadWithSettings(ctx, cwd, environment, ThreadSettings{})
}

func (c *Client) StartThreadWithSettings(
	ctx context.Context, cwd string, environment map[string]string, settings ThreadSettings,
) (string, error) {
	var response struct {
		Thread struct {
			ID  string `json:"id"`
			Cwd string `json:"cwd"`
		} `json:"thread"`
	}
	config := map[string]any{
		"shell_environment_policy": map[string]any{"set": environment},
	}
	params := map[string]any{
		"cwd":    cwd,
		"config": config,
	}
	if err := c.addRuntimeWorkspaceRoots(params); err != nil {
		return "", err
	}
	c.settingsParams(settings, params, config)
	if err := c.Request(ctx, "thread/start", params, &response); err != nil {
		return "", err
	}
	if response.Thread.ID == "" || response.Thread.Cwd != cwd {
		return "", errors.New("thread/start returned no thread id or the wrong working directory")
	}
	return response.Thread.ID, nil
}

func (c *Client) ResumeThread(ctx context.Context, threadID, cwd string, environment map[string]string) (string, error) {
	return c.ResumeThreadWithSettings(ctx, threadID, cwd, environment, ThreadSettings{})
}

// ReconcileThreadInstructions refreshes the package-owned thread instruction
// without interrupting or changing an active turn.
func (c *Client) ReconcileThreadInstructions(ctx context.Context, threadID string) error {
	if err := c.RequireThreadTurnsIdle(ctx, threadID); err != nil {
		return err
	}
	return c.resumeThread(ctx, threadID)
}

func (c *Client) ResumeThreadWithSettings(
	ctx context.Context, threadID, cwd string, environment map[string]string, settings ThreadSettings,
) (string, error) {
	var response struct {
		Thread struct {
			ID  string `json:"id"`
			Cwd string `json:"cwd"`
		} `json:"thread"`
	}
	config := map[string]any{
		"shell_environment_policy": map[string]any{"set": environment},
	}
	params := c.threadResumeParams(threadID)
	params["cwd"] = cwd
	params["config"] = config
	c.settingsParams(settings, params, config)
	if err := c.Request(ctx, "thread/resume", params, &response); err != nil {
		return "", err
	}
	if response.Thread.ID != threadID || response.Thread.Cwd != cwd {
		return "", errors.New("thread/resume returned the wrong thread or working directory")
	}
	return response.Thread.ID, nil
}

func (c *Client) OpenThread(ctx context.Context, threadID, cwd string, environment map[string]string) (string, error) {
	return c.OpenThreadWithSettings(ctx, threadID, cwd, environment, ThreadSettings{})
}

func (c *Client) OpenThreadWithSettings(
	ctx context.Context, threadID, cwd string, environment map[string]string, settings ThreadSettings,
) (string, error) {
	if threadID != "" {
		return c.ResumeThreadWithSettings(ctx, threadID, cwd, environment, settings)
	}
	return c.StartThreadWithSettings(ctx, cwd, environment, settings)
}

func (c *Client) LoadedThreadIDs(ctx context.Context) ([]string, error) {
	ids := make([]string, 0)
	seenCursors := make(map[string]struct{})
	var cursor string
	for {
		params := map[string]any{"limit": 100}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var page struct {
			Data       *[]string `json:"data"`
			NextCursor *string   `json:"nextCursor"`
		}
		if err := c.Request(ctx, "thread/loaded/list", params, &page); err != nil {
			return nil, err
		}
		if page.Data == nil {
			return nil, errors.New("thread/loaded/list returned no data")
		}
		for _, id := range *page.Data {
			if id == "" {
				return nil, errors.New("thread/loaded/list returned an empty thread id")
			}
			ids = append(ids, id)
		}
		if page.NextCursor == nil {
			return ids, nil
		}
		if *page.NextCursor == "" {
			return nil, errors.New("thread/loaded/list returned an empty pagination cursor")
		}
		if _, exists := seenCursors[*page.NextCursor]; exists {
			return nil, errors.New("thread/loaded/list repeated a pagination cursor")
		}
		seenCursors[*page.NextCursor] = struct{}{}
		cursor = *page.NextCursor
	}
}

func (c *Client) ReadThreadMetadata(
	ctx context.Context, threadID string, excludeTurns bool,
) (ThreadMetadata, error) {
	var response struct {
		Thread ThreadMetadata `json:"thread"`
	}
	params := map[string]any{"threadId": threadID}
	if excludeTurns {
		params["excludeTurns"] = true
	}
	if err := c.Request(ctx, "thread/read", params, &response); err != nil {
		return ThreadMetadata{}, err
	}
	if response.Thread.ID != threadID {
		return ThreadMetadata{}, errors.New("thread/read returned the wrong thread")
	}
	return response.Thread, nil
}

func (c *Client) ListThreads(
	ctx context.Context, options ThreadListOptions,
) ([]ThreadMetadata, *string, error) {
	params := map[string]any{}
	if options.Cwd != "" {
		params["cwd"] = options.Cwd
	}
	if len(options.SourceKinds) > 0 {
		params["sourceKinds"] = append([]string(nil), options.SourceKinds...)
	}
	if options.Archived != nil {
		params["archived"] = *options.Archived
	}
	if options.Limit > 0 {
		params["limit"] = options.Limit
	}
	if options.SortDirection != "" {
		params["sortDirection"] = options.SortDirection
	}
	if options.Cursor != "" {
		params["cursor"] = options.Cursor
	}
	var page struct {
		Data       *[]ThreadMetadata `json:"data"`
		NextCursor *string           `json:"nextCursor"`
	}
	if err := c.Request(ctx, "thread/list", params, &page); err != nil {
		return nil, nil, err
	}
	if page.Data == nil {
		return nil, nil, errors.New("thread/list returned no data")
	}
	return *page.Data, page.NextCursor, nil
}

func (c *Client) UnarchiveThread(ctx context.Context, threadID string) (ThreadMetadata, error) {
	var response struct {
		Thread ThreadMetadata `json:"thread"`
	}
	if err := c.Request(ctx, "thread/unarchive", map[string]any{"threadId": threadID}, &response); err != nil {
		return ThreadMetadata{}, err
	}
	if response.Thread.ID != threadID {
		return ThreadMetadata{}, errors.New("thread/unarchive returned the wrong thread")
	}
	return response.Thread, nil
}

func (c *Client) ArchiveThread(ctx context.Context, threadID string) error {
	return c.Request(ctx, "thread/archive", map[string]any{"threadId": threadID}, nil)
}

func (c *Client) ListModels(ctx context.Context) ([]Model, error) {
	models := make([]Model, 0)
	seenCursors := make(map[string]struct{})
	var cursor string
	for {
		params := map[string]any{"limit": 100, "includeHidden": false}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var page struct {
			Data       *[]Model `json:"data"`
			NextCursor *string  `json:"nextCursor"`
		}
		if err := c.Request(ctx, "model/list", params, &page); err != nil {
			return nil, err
		}
		if page.Data == nil {
			return nil, errors.New("model/list returned no data")
		}
		models = append(models, (*page.Data)...)
		if page.NextCursor == nil {
			return models, nil
		}
		if *page.NextCursor == "" {
			return nil, errors.New("model/list returned an empty pagination cursor")
		}
		if _, ok := seenCursors[*page.NextCursor]; ok {
			return nil, errors.New("model/list repeated a pagination cursor")
		}
		seenCursors[*page.NextCursor] = struct{}{}
		cursor = *page.NextCursor
	}
}

// ReadAccountRateLimits reads the connected account's current allowances.
// Applications decide which buckets to display and authorize account-level
// reads separately from the conversation handler.
func (c *Client) ReadAccountRateLimits(ctx context.Context) (AccountRateLimits, error) {
	var limits AccountRateLimits
	err := c.Request(ctx, "account/rateLimits/read", nil, &limits)
	return limits, err
}

func (c *Client) ListCollaborationModes(ctx context.Context) ([]CollaborationMode, error) {
	var response struct {
		Data *[]struct {
			Name string  `json:"name"`
			Mode *string `json:"mode"`
		} `json:"data"`
	}
	if err := c.Request(ctx, "collaborationMode/list", map[string]any{}, &response); err != nil {
		return nil, err
	}
	if response.Data == nil {
		return nil, errors.New("collaborationMode/list returned no data")
	}
	modes := make([]CollaborationMode, 0, len(*response.Data))
	seen := make(map[string]struct{})
	for _, entry := range *response.Data {
		if entry.Mode == nil || *entry.Mode == "" || entry.Name == "" {
			continue
		}
		if _, exists := seen[*entry.Mode]; exists {
			return nil, fmt.Errorf("collaborationMode/list repeated mode %q", *entry.Mode)
		}
		seen[*entry.Mode] = struct{}{}
		modes = append(modes, CollaborationMode{Name: entry.Name, Mode: *entry.Mode})
	}
	return modes, nil
}

func (c *Client) cachedSettings(threadID string) (cachedThreadSettings, <-chan struct{}, bool) {
	c.connectionMu.Lock()
	generation := c.generation
	ready := c.ready
	c.connectionMu.Unlock()
	c.settingsMu.Lock()
	defer c.settingsMu.Unlock()
	entry, ok := c.threadSettings[threadID]
	if !ok || entry.generation != generation || ready != generation {
		entry = cachedThreadSettings{}
		ok = false
	}
	return entry, c.settingsChanged, ok
}

func (c *Client) waitForSettings(
	ctx context.Context,
	threadID string,
	after uint64,
	match func(ThreadSettings) bool,
) (cachedThreadSettings, error) {
	for {
		entry, changed, ok := c.cachedSettings(threadID)
		if ok && entry.revision > after && (match == nil || match(entry.settings)) {
			return entry, nil
		}
		select {
		case <-ctx.Done():
			return cachedThreadSettings{}, fmt.Errorf("wait for Codex thread settings: %w", ctx.Err())
		case <-changed:
		}
	}
}

type threadSettingsMetadata struct {
	settings ThreadSettings
	path     string
}

func (c *Client) settingsRevisionSnapshot() uint64 {
	c.settingsMu.Lock()
	defer c.settingsMu.Unlock()
	return c.settingsRevision
}

func (c *Client) readThreadSettingsMetadata(
	ctx context.Context, threadID string,
) (threadSettingsMetadata, error) {
	var metadata struct {
		Thread map[string]any `json:"thread"`
	}
	if err := c.Request(ctx, "thread/read", map[string]any{
		"threadId": threadID, "excludeTurns": true,
	}, &metadata); err != nil {
		return threadSettingsMetadata{}, err
	}
	if metadata.Thread == nil || stringValue(metadata.Thread["id"]) != threadID {
		return threadSettingsMetadata{}, errors.New("thread/read returned the wrong settings thread")
	}
	model := stringValue(metadata.Thread["model"])
	if model == "" {
		return threadSettingsMetadata{}, errors.New("thread/read returned incomplete current settings")
	}
	path := stringValue(metadata.Thread["path"])
	if path == "" {
		return threadSettingsMetadata{}, errors.New("thread/read returned no rollout path")
	}
	return threadSettingsMetadata{path: path, settings: ThreadSettings{
		Model:           model,
		ReasoningEffort: stringValue(metadata.Thread["reasoningEffort"]),
	}}, nil

}

func (c *Client) ReadThreadSettings(ctx context.Context, threadID string) (ThreadSettings, error) {
	metadata, err := c.readThreadSettingsMetadata(ctx, threadID)
	if err != nil {
		return ThreadSettings{}, err
	}
	return metadata.settings, nil
}

func (c *Client) refreshThreadSettings(ctx context.Context, threadID string) (cachedThreadSettings, error) {
	const stabilityAttempts = 3
	for attempt := 0; attempt < stabilityAttempts; attempt++ {
		before := c.settingsRevisionSnapshot()
		first, err := c.readThreadSettingsMetadata(ctx, threadID)
		if err != nil {
			return cachedThreadSettings{}, err
		}
		firstMode, err := collaborationModeFromRollout(first.path)
		if err != nil {
			return cachedThreadSettings{}, fmt.Errorf("read current collaboration mode: %w", err)
		}
		second, err := c.readThreadSettingsMetadata(ctx, threadID)
		if err != nil {
			return cachedThreadSettings{}, err
		}
		secondMode, err := collaborationModeFromRollout(second.path)
		if err != nil {
			return cachedThreadSettings{}, fmt.Errorf("read current collaboration mode: %w", err)
		}
		after := c.settingsRevisionSnapshot()
		if before != after || first != second || firstMode != secondMode {
			continue
		}
		first.settings.CollaborationMode = firstMode
		return cachedThreadSettings{revision: after, settings: first.settings}, nil
	}
	return cachedThreadSettings{}, errors.New("Codex thread settings did not stabilize while being read")
}

func (c *Client) settingsUpdateLock(threadID string) *sync.Mutex {
	c.settingsUpdateMu.Lock()
	defer c.settingsUpdateMu.Unlock()
	lock := c.settingsUpdates[threadID]
	if lock == nil {
		lock = &sync.Mutex{}
		c.settingsUpdates[threadID] = lock
	}
	return lock
}

func (c *Client) UpdateThreadSettings(
	ctx context.Context, threadID string, update ThreadSettingsUpdate,
) (ThreadSettings, error) {
	if update.ReasoningEffort != nil && *update.ReasoningEffort == "" {
		return ThreadSettings{}, errors.New("Codex cannot clear reasoning effort on an existing thread")
	}
	lock := c.settingsUpdateLock(threadID)
	lock.Lock()
	defer lock.Unlock()
	watchTransition := c.watchLock(threadID)
	watchTransition.Lock()
	defer watchTransition.Unlock()
	if err := c.resumeThread(ctx, threadID); err != nil {
		return ThreadSettings{}, fmt.Errorf("subscribe to Codex thread settings: %w", err)
	}
	current, err := c.refreshThreadSettings(ctx, threadID)
	if err != nil {
		return ThreadSettings{}, err
	}
	desired := current.settings
	if update.Model != nil {
		desired.Model = *update.Model
	}
	if update.ReasoningEffort != nil {
		desired.ReasoningEffort = *update.ReasoningEffort
	}
	if update.CollaborationMode != nil {
		desired.CollaborationMode = *update.CollaborationMode
	}
	if desired.Model == "" || desired.CollaborationMode == "" {
		return ThreadSettings{}, errors.New("Codex returned incomplete current thread settings")
	}
	if desired == current.settings {
		return desired, nil
	}
	params := map[string]any{"threadId": threadID}
	if update.Model != nil {
		params["model"] = desired.Model
	}
	if update.ReasoningEffort != nil {
		params["effort"] = nil
		if desired.ReasoningEffort != "" {
			params["effort"] = desired.ReasoningEffort
		}
	}
	if update.CollaborationMode != nil {
		// The thread-level developer instruction is independent of collaboration
		// mode instructions. Keep this nil so Codex supplies the built-in Default
		// or Plan instructions instead of replacing them with the lifecycle policy.
		modeSettings := map[string]any{
			"model":                  desired.Model,
			"reasoning_effort":       nil,
			"developer_instructions": nil,
		}
		if desired.ReasoningEffort != "" {
			modeSettings["reasoning_effort"] = desired.ReasoningEffort
		}
		params["collaborationMode"] = map[string]any{
			"mode": desired.CollaborationMode, "settings": modeSettings,
		}
	}
	var response map[string]any
	if err := c.Request(ctx, "thread/settings/update", params, &response); err != nil {
		return ThreadSettings{}, err
	}
	updated, err := c.waitForSettings(
		ctx,
		threadID,
		current.revision,
		func(actual ThreadSettings) bool {
			if update.CollaborationMode != nil {
				return actual == desired
			}
			return (update.Model == nil || actual.Model == desired.Model) &&
				(update.ReasoningEffort == nil || actual.ReasoningEffort == desired.ReasoningEffort)
		},
	)
	if err != nil {
		return ThreadSettings{}, err
	}
	return updated.settings, nil
}

func (c *Client) ForkThread(
	ctx context.Context, threadID, cwd string, environment map[string]string, settings ThreadSettings,
) (string, error) {
	if err := c.RequireThreadTurnsIdle(ctx, threadID); err != nil {
		return "", err
	}
	config := map[string]any{"shell_environment_policy": map[string]any{"set": environment}}
	params := map[string]any{
		"threadId": threadID, "cwd": cwd, "excludeTurns": true,
		"deferGoalContinuation": true,
		"config":                config,
	}
	if err := c.addRuntimeWorkspaceRoots(params); err != nil {
		return "", err
	}
	c.settingsParams(settings, params, config)
	var response struct {
		Thread struct {
			ID           string `json:"id"`
			Cwd          string `json:"cwd"`
			ForkedFromID string `json:"forkedFromId"`
		} `json:"thread"`
	}
	if err := c.Request(ctx, "thread/fork", params, &response); err != nil {
		return "", err
	}
	if response.Thread.ID == "" || response.Thread.ID == threadID || response.Thread.Cwd != cwd ||
		response.Thread.ForkedFromID != threadID {
		return "", errors.New("thread/fork returned invalid thread metadata")
	}
	return response.Thread.ID, nil
}

func (c *Client) SetName(ctx context.Context, threadID, name string) error {
	return c.Request(ctx, "thread/name/set", map[string]any{"threadId": threadID, "name": name}, nil)
}

func (c *Client) ReadThread(ctx context.Context, threadID string) (Transcript, error) {
	var metadata struct {
		Thread map[string]any `json:"thread"`
	}
	if err := c.Request(ctx, "thread/read", map[string]any{
		"threadId": threadID, "excludeTurns": true,
	}, &metadata); err != nil {
		return Transcript{}, fmt.Errorf("read Codex thread metadata: %w", err)
	}
	if metadata.Thread == nil {
		return Transcript{}, errors.New("thread/read returned no thread")
	}
	if stringValue(metadata.Thread["id"]) != threadID {
		return Transcript{}, errors.New("thread/read returned the wrong thread")
	}
	transcript := Transcript{
		ThreadID:          threadID,
		Status:            statusValue(metadata.Thread["status"]),
		Model:             stringValue(metadata.Thread["model"]),
		ReasoningEffort:   stringValue(metadata.Thread["reasoningEffort"]),
		CollaborationMode: "default",
		Entries:           make([]TranscriptEntry, 0),
	}
	if settings, _, ok := c.cachedSettings(threadID); ok {
		transcript.CollaborationMode = settings.settings.CollaborationMode
	}
	var page struct {
		Data *[]map[string]any `json:"data"`
	}
	if err := c.Request(ctx, "thread/turns/list", map[string]any{
		"threadId": threadID, "limit": recentTurnLimit, "sortDirection": "desc", "itemsView": "full",
	}, &page); err != nil {
		if c.freshThreadMissingSourceRollout(metadata.Thread, threadID, err) {
			return transcript, nil
		}
		return Transcript{}, fmt.Errorf("read Codex thread turns: %w", err)
	}
	if page.Data == nil {
		return Transcript{}, errors.New("thread/turns/list returned no data")
	}
	slices.Reverse(*page.Data)
	for _, turn := range *page.Data {
		transcript.Entries = append(transcript.Entries, transcriptEntries(turn)...)
	}
	wanted := make(map[transcriptItemKey]bool, len(transcript.Entries))
	for _, entry := range transcript.Entries {
		if entry.ItemID != "" {
			wanted[transcriptItemKey{entry.TurnID, entry.ItemID}] = true
		}
	}
	rollout, err := readRolloutMetadata(stringValue(metadata.Thread["path"]), wanted)
	if err == nil && rollout.mode != "" {
		transcript.CollaborationMode = rollout.mode
	}
	c.applyTranscriptTimestamps(threadID, transcript.Entries, rollout.itemTimes)
	return transcript, nil
}

func (c *Client) freshThreadMissingSourceRollout(thread map[string]any, threadID string, err error) bool {
	var rpcErr *rpcCallError
	if !errors.As(err, &rpcErr) || rpcErr.code != -32600 ||
		rpcErr.message != "invalid paginated history lineage for "+threadID+": missing source rollout" {
		return false
	}
	if stringValue(thread["id"]) != threadID || !c.threadSource(thread["source"]) ||
		stringValue(thread["historyMode"]) != "paginated" || stringValue(thread["preview"]) != "" ||
		stringValue(thread["forkedFromId"]) != "" ||
		!slices.Contains([]string{"idle", "notLoaded"}, statusValue(thread["status"])) {
		return false
	}
	ephemeral, ok := thread["ephemeral"].(bool)
	if !ok || ephemeral {
		return false
	}
	path := stringValue(thread["path"])
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return false
	}
	turns, exists := thread["turns"]
	if !exists || turns == nil {
		return false
	}
	items, ok := turns.([]any)
	if !ok || len(items) != 0 {
		return false
	}
	_, statErr := os.Stat(path)
	return errors.Is(statErr, os.ErrNotExist)
}

func (c *Client) VerifyThread(ctx context.Context, threadID, cwd string) error {
	var metadata struct {
		Thread map[string]any `json:"thread"`
	}
	if err := c.Request(ctx, "thread/read", map[string]any{
		"threadId": threadID, "excludeTurns": true,
	}, &metadata); err != nil {
		return err
	}
	if metadata.Thread == nil || stringValue(metadata.Thread["id"]) != threadID ||
		stringValue(metadata.Thread["cwd"]) != cwd {
		return errors.New("Codex thread does not match the trusted working directory")
	}
	return nil
}

func (c *Client) RequireThreadIdle(ctx context.Context, threadID, cwd string) error {
	if err := c.VerifyThread(ctx, threadID, cwd); err != nil {
		return err
	}
	if err := c.RequireThreadTurnsIdle(ctx, threadID); err != nil {
		return err
	}
	prompts, err := c.PromptsWithItems(ctx, threadID)
	if err != nil {
		return fmt.Errorf("inspect pending Codex requests: %w", err)
	}
	if len(prompts) > 0 {
		return fmt.Errorf("Codex thread %s has %d pending request(s)", threadID, len(prompts))
	}
	queued, err := c.ListQueue(ctx, threadID)
	if err != nil {
		return fmt.Errorf("inspect queued Codex messages: %w", err)
	}
	if len(queued) > 0 {
		return fmt.Errorf("Codex thread %s has %d queued message(s)", threadID, len(queued))
	}
	return c.RequireSubmissionAttemptsResolved(ctx, threadID)
}

func (c *Client) RequireThreadTurnsIdle(ctx context.Context, threadID string) error {
	var page struct {
		Data *[]struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"data"`
	}
	if err := c.Request(ctx, "thread/turns/list", map[string]any{
		"threadId": threadID, "limit": 1, "sortDirection": "desc", "itemsView": "notLoaded",
	}, &page); err != nil {
		return err
	}
	if page.Data == nil {
		return errors.New("thread/turns/list returned no data")
	}
	if len(*page.Data) == 0 {
		return nil
	}
	turn := (*page.Data)[0]
	if turn.ID == "" || !slices.Contains([]string{"completed", "failed", "interrupted"}, turn.Status) {
		return fmt.Errorf("Codex thread %s is not idle (latest turn %s has status %q)", threadID, turn.ID, turn.Status)
	}
	return nil
}

func transcriptEntries(turn map[string]any) []TranscriptEntry {
	turnID := stringValue(turn["id"])
	turnStatus := statusValue(turn["status"])
	entries := make([]TranscriptEntry, 0)
	items, _ := turn["items"].([]any)
	for _, raw := range items {
		item, ok := raw.(map[string]any)
		if !ok {
			entries = append(entries, TranscriptEntry{TurnID: turnID, Kind: "unknown", Summary: "Unknown Codex event", Details: jsonDetails(raw)})
			continue
		}
		entry := TranscriptEntry{
			TurnID: turnID, TurnStatus: turnStatus,
			ItemID: stringValue(item["id"]), Kind: stringValue(item["type"]),
		}
		switch entry.Kind {
		case "userMessage":
			entry.Text = textContent(item["content"])
			entry.ClientUserMessageID = stringValue(item["clientId"])
			if canonicalText, err := userMessageText(item); err == nil {
				entry.ClientUserMessageDigest = queueTextDigest(canonicalText)
			}
		case "agentMessage":
			entry.Text = stringValue(item["text"])
		case "commandExecution":
			entry.Summary = "$ " + stringValue(item["command"])
			entry.Details = stringValue(item["aggregatedOutput"])
			if entry.Details == "" {
				entry.Details = jsonDetails(map[string]any{"status": item["status"], "exitCode": item["exitCode"]})
			}
		case "fileChange":
			entry.Summary = "File changes"
			if status := statusValue(item["status"]); status != "" {
				entry.Summary += " · " + status
			}
			entry.Details = jsonDetails(item["changes"])
		case "mcpToolCall":
			server := stringValue(item["server"])
			if server == "" {
				server = "MCP"
			}
			tool := stringValue(item["tool"])
			if tool == "" {
				tool = "call"
			}
			entry.Summary = "Tool · " + server + "/" + tool
			value := item["result"]
			if value == nil {
				value = item["error"]
			}
			if value == nil {
				value = item["arguments"]
			}
			entry.Details = jsonDetails(value)
		case "reasoning":
			entry.Text = strings.Join(stringValues(item["summary"]), "\n")
			if strings.TrimSpace(entry.Text) == "" {
				continue
			}
			entry.Summary = "Reasoning summary"
		case "plan":
			entry.Summary = "Plan"
			entry.Text = stringValue(item["text"])
		case "webSearch", "collabAgentToolCall", "subAgentActivity":
			normalizeTranscriptActivity(&entry, item)
		default:
			if entry.Kind == "" {
				entry.Kind = "unknown"
			}
			entry.Summary = "Codex event · " + entry.Kind
			entry.Details = jsonDetails(item)
		}
		entries = append(entries, entry)
	}
	if failure := turn["error"]; failure != nil {
		entries = append(entries, transcriptFailureEntry(turnID, failure))
	} else if status := statusValue(turn["status"]); status == "failed" || status == "error" {
		entries = append(entries, TranscriptEntry{TurnID: turnID, Kind: "error", Summary: "Turn " + status})
	}
	applyTurnTimestamps(entries, turn)
	return entries
}

func transcriptFailureEntry(turnID string, failure any) TranscriptEntry {
	entry := TranscriptEntry{TurnID: turnID, Kind: "error", Summary: "Turn failed"}
	if object, ok := failure.(map[string]any); ok {
		if message := strings.TrimSpace(stringValue(object["message"])); message != "" {
			entry.Text = message
			if stringValue(object["codexErrorInfo"]) != "serverOverloaded" {
				entry.Details = jsonDetails(failure)
			}
			return entry
		}
	}
	if message, ok := failure.(string); ok && strings.TrimSpace(message) != "" {
		entry.Text = message
		return entry
	}
	entry.Details = jsonDetails(failure)
	return entry
}

func statusValue(value any) string {
	if status := stringValue(value); status != "" {
		return status
	}
	if object, ok := value.(map[string]any); ok {
		return stringValue(object["type"])
	}
	return ""
}

func textContent(value any) string {
	parts, _ := value.([]any)
	text := make([]string, 0, len(parts))
	for _, raw := range parts {
		part, ok := raw.(map[string]any)
		if ok && stringValue(part["type"]) == "text" {
			text = append(text, stringValue(part["text"]))
		}
	}
	return strings.Join(text, "\n")
}

func stringValues(value any) []string {
	values, _ := value.([]any)
	result := make([]string, 0, len(values))
	for _, value := range values {
		if text, ok := value.(string); ok {
			result = append(result, text)
		}
	}
	return result
}

func jsonDetails(value any) string {
	if value == nil {
		return ""
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return "unable to render event details"
	}
	return string(data)
}

func (c *Client) resumeThread(ctx context.Context, threadID string) error {
	var response map[string]any
	return c.Request(ctx, "thread/resume", c.threadResumeParams(threadID), &response)
}

func (c *Client) threadResumeParams(threadID string) map[string]any {
	params := map[string]any{"threadId": threadID, "excludeTurns": true}
	if c.options.DeveloperInstructions != "" && !c.options.ObserverOnly {
		params["developerInstructions"] = c.options.DeveloperInstructions
	}
	return params
}

func (c *Client) ListQueue(ctx context.Context, threadID string) ([]QueueEntry, error) {
	entries := make([]QueueEntry, 0)
	seenCursors := make(map[string]struct{})
	seenIDs := make(map[string]struct{})
	var cursor string
	for {
		params := map[string]any{"threadId": threadID, "limit": 100}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var page struct {
			Data *[]struct {
				ID                  string           `json:"id"`
				Input               []map[string]any `json:"input"`
				ClientUserMessageID string           `json:"clientUserMessageId"`
			} `json:"data"`
			NextCursor *string `json:"nextCursor"`
		}
		if err := c.Request(ctx, "thread/queue/list", params, &page); err != nil {
			return nil, err
		}
		if page.Data == nil {
			return nil, errors.New("thread/queue/list returned no data")
		}
		for _, submission := range *page.Data {
			if submission.ID == "" || submission.ClientUserMessageID == "" {
				return nil, errors.New("thread/queue/list returned an invalid submission")
			}
			if _, exists := seenIDs[submission.ID]; exists {
				return nil, fmt.Errorf("thread/queue/list repeated submission %q", submission.ID)
			}
			seenIDs[submission.ID] = struct{}{}
			entries = append(entries, QueueEntry{
				ID:                  submission.ID,
				Text:                inputText(submission.Input),
				ClientUserMessageID: submission.ClientUserMessageID,
			})
		}
		if page.NextCursor == nil {
			return entries, nil
		}
		if *page.NextCursor == "" {
			return nil, errors.New("thread/queue/list returned an empty pagination cursor")
		}
		if _, exists := seenCursors[*page.NextCursor]; exists {
			return nil, errors.New("thread/queue/list repeated a pagination cursor")
		}
		seenCursors[*page.NextCursor] = struct{}{}
		cursor = *page.NextCursor
	}
}

func (c *Client) Queue(ctx context.Context, threadID, text, clientID string) (QueueEntry, error) {
	lock := c.queueUpdateLock(threadID)
	lock.Lock()
	defer lock.Unlock()
	attempted, err := c.queueAttempt(threadID, clientID, text)
	if err != nil {
		return QueueEntry{}, err
	}
	if attempted {
		existing, found, err := c.queuedOrStartedByClientID(ctx, threadID, clientID, text)
		if err != nil {
			return QueueEntry{}, err
		}
		if found {
			return existing, nil
		}
		return QueueEntry{}, errors.New(
			"previous queued message outcome is still unknown; refusing to submit it again",
		)
	}
	var response struct {
		Submission struct {
			ID                  string           `json:"id"`
			Input               []map[string]any `json:"input"`
			ClientUserMessageID string           `json:"clientUserMessageId"`
		} `json:"queuedSubmission"`
	}
	if err := c.recordQueueAttempt(threadID, clientID, text); err != nil {
		return QueueEntry{}, fmt.Errorf("record queue attempt before submission: %w", err)
	}
	requestErr := c.Request(ctx, "thread/queue/add", map[string]any{
		"threadId":            threadID,
		"input":               []map[string]any{{"type": "text", "text": text}},
		"clientUserMessageId": clientID,
	}, &response)
	responseText := inputText(response.Submission.Input)
	if requestErr == nil && response.Submission.ID != "" &&
		response.Submission.ClientUserMessageID == clientID && responseText == text {
		return QueueEntry{
			ID: response.Submission.ID, Text: text, ClientUserMessageID: clientID,
		}, nil
	}
	existing, found, reconcileErr := c.queuedOrStartedByClientID(ctx, threadID, clientID, text)
	if reconcileErr != nil {
		if requestErr != nil {
			return QueueEntry{}, fmt.Errorf("%w; queue reconciliation failed: %v", requestErr, reconcileErr)
		}
		return QueueEntry{}, fmt.Errorf("thread/queue/add returned an invalid submission; reconciliation failed: %w", reconcileErr)
	}
	if found {
		return existing, nil
	}
	if requestErr != nil {
		return QueueEntry{}, requestErr
	}
	return QueueEntry{}, errors.New(
		"thread/queue/add returned an invalid submission; refusing to retry an unknown outcome",
	)
}

func (c *Client) queuedOrStartedByClientID(
	ctx context.Context, threadID, clientID, text string,
) (QueueEntry, bool, error) {
	entries, err := c.ListQueue(ctx, threadID)
	if err != nil {
		return QueueEntry{}, false, err
	}
	for _, entry := range entries {
		if entry.ClientUserMessageID != clientID {
			continue
		}
		if entry.Text != text {
			return QueueEntry{}, false, errors.New("queued message identity was reused with different text")
		}
		return entry, true, nil
	}
	return c.startedByClientID(ctx, threadID, clientID, text)
}

func (c *Client) startedByClientID(
	ctx context.Context, threadID, clientID, text string,
) (QueueEntry, bool, error) {
	var result QueueEntry
	found := false
	err := c.walkThreadItems(ctx, threadID, func(entry threadItemEntry) (bool, error) {
		if stringValue(entry.Item["type"]) != "userMessage" ||
			stringValue(entry.Item["clientId"]) != clientID {
			return false, nil
		}
		startedText, err := userMessageText(entry.Item)
		if err != nil {
			return false, fmt.Errorf("started queued message is invalid: %w", err)
		}
		if startedText != text {
			return false, errors.New("queued message identity was reused with different text")
		}
		result = QueueEntry{
			ID: stringValue(entry.Item["id"]), Text: startedText,
			ClientUserMessageID: clientID,
		}
		found = true
		return true, nil
	})
	if err != nil {
		return QueueEntry{}, false, err
	}
	return result, found, nil
}

func (c *Client) waitForStartedByClientID(
	ctx context.Context, threadID, clientID, text string,
) (bool, error) {
	for attempt := 0; attempt < 6; attempt++ {
		_, found, err := c.startedByClientID(ctx, threadID, clientID, text)
		if err != nil || found {
			return found, err
		}
		if attempt == 5 {
			break
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, ctx.Err()
		case <-timer.C:
		}
	}
	return false, nil
}

func (c *Client) sentByClientID(
	ctx context.Context, threadID, clientID, text string,
) (SendReceipt, bool, error) {
	var receipt SendReceipt
	found := false
	err := c.walkThreadItems(ctx, threadID, func(entry threadItemEntry) (bool, error) {
		if found {
			if entry.TurnID == receipt.TurnID {
				receipt.Steered = true
			}
			return true, nil
		}
		if stringValue(entry.Item["type"]) != "userMessage" ||
			stringValue(entry.Item["clientId"]) != clientID {
			return false, nil
		}
		sentText, err := userMessageText(entry.Item)
		if err != nil {
			return false, fmt.Errorf("sent message is invalid: %w", err)
		}
		if sentText != text {
			return false, errors.New("message identity was reused with different text")
		}
		if entry.TurnID == "" {
			return false, errors.New("sent message has no turn identity")
		}
		receipt = SendReceipt{TurnID: entry.TurnID, ClientUserMessageID: clientID}
		found = true
		return false, nil
	})
	if err != nil {
		return SendReceipt{}, false, err
	}
	return receipt, found, nil
}

func (c *Client) waitForSentByClientID(
	ctx context.Context, threadID, clientID, text string,
) (SendReceipt, bool, error) {
	for attempt := 0; attempt < 6; attempt++ {
		receipt, found, err := c.sentByClientID(ctx, threadID, clientID, text)
		if err != nil || found {
			return receipt, found, err
		}
		if attempt == 5 {
			break
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return SendReceipt{}, false, ctx.Err()
		case <-timer.C:
		}
	}
	return SendReceipt{}, false, nil
}

func (c *Client) DeleteQueueEntry(ctx context.Context, threadID, id string) error {
	updateLock := c.queueUpdateLock(threadID)
	updateLock.Lock()
	defer updateLock.Unlock()

	attempt, attempted, err := c.queueDeletionAttempt(threadID, id)
	if err != nil {
		return err
	}
	entries, err := c.ListQueue(ctx, threadID)
	if err != nil {
		return err
	}
	var target *QueueEntry
	for index := range entries {
		if entries[index].ID == id {
			target = &entries[index]
			break
		}
	}
	if target == nil {
		if attempted {
			return c.finishAbsentQueueDeletion(ctx, threadID, id, attempt)
		}
		return errors.New("queued message was not found")
	}
	if target.ClientUserMessageID == "" {
		return errors.New("queued message has no client identity")
	}
	current := queueDeletionAttempt{
		ClientUserMessageID: target.ClientUserMessageID,
		Digest:              queueTextDigest(target.Text),
	}
	if attempted {
		if current != attempt {
			return errors.New("queued message changed after deletion was requested")
		}
	} else {
		attempt, err = c.recordQueueDeletionAttempt(threadID, *target)
		if err != nil {
			return err
		}
	}
	var response struct {
		Deleted bool `json:"deleted"`
	}
	if err := c.Request(ctx, "thread/queue/delete", map[string]any{
		"threadId": threadID, "queuedSubmissionId": id,
	}, &response); err != nil {
		entries, reconcileErr := c.ListQueue(ctx, threadID)
		if reconcileErr == nil && !queueContainsSubmission(entries, id) {
			return c.finishAbsentQueueDeletion(ctx, threadID, id, attempt)
		}
		return err
	}
	if !response.Deleted {
		entries, err := c.ListQueue(ctx, threadID)
		if err != nil {
			return err
		}
		if queueContainsSubmission(entries, id) {
			return errors.New("queued message deletion was not accepted")
		}
		return c.finishAbsentQueueDeletion(ctx, threadID, id, attempt)
	}
	return c.clearQueueDeletionAndAttempt(threadID, id, attempt.ClientUserMessageID)
}

func queueContainsSubmission(entries []QueueEntry, queuedSubmissionID string) bool {
	for _, entry := range entries {
		if entry.ID == queuedSubmissionID {
			return true
		}
	}
	return false
}

func (c *Client) finishAbsentQueueDeletion(
	ctx context.Context, threadID, queuedSubmissionID string, attempt queueDeletionAttempt,
) error {
	started, err := c.queueDeletionWasStarted(ctx, threadID, attempt)
	if err != nil {
		return err
	}
	if err := c.clearQueueDeletionAndAttempt(
		threadID, queuedSubmissionID, attempt.ClientUserMessageID,
	); err != nil {
		return err
	}
	if started {
		return errors.New("queued message was started before it could be deleted")
	}
	return nil
}

func (c *Client) queueDeletionWasStarted(
	ctx context.Context, threadID string, attempt queueDeletionAttempt,
) (bool, error) {
	found := false
	err := c.walkThreadItems(ctx, threadID, func(entry threadItemEntry) (bool, error) {
		if stringValue(entry.Item["type"]) != "userMessage" ||
			stringValue(entry.Item["clientId"]) != attempt.ClientUserMessageID {
			return false, nil
		}
		text, err := userMessageText(entry.Item)
		if err != nil {
			return false, fmt.Errorf("deleted queued message has invalid history: %w", err)
		}
		if queueTextDigest(text) != attempt.Digest {
			return false, errors.New("deleted queued message identity was reused with different text")
		}
		found = true
		return true, nil
	})
	if err != nil {
		return false, fmt.Errorf("inspect deleted queued message history: %w", err)
	}
	return found, nil
}

func (c *Client) StartQueue(ctx context.Context, threadID, queuedSubmissionID string) error {
	entries, err := c.ListQueue(ctx, threadID)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return errors.New("queued message was not found")
	}
	if entries[0].ID != queuedSubmissionID {
		return errors.New("only the first queued message can be started")
	}
	target := &entries[0]
	if err := c.resumeThread(ctx, threadID); err != nil {
		return err
	}
	if _, found, err := c.startedByClientID(
		ctx, threadID, target.ClientUserMessageID, target.Text,
	); err != nil {
		return err
	} else if found {
		return nil
	}
	var response struct {
		Turn struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"turn"`
	}
	requestErr := c.Request(ctx, "thread/queue/start", map[string]any{
		"threadId": threadID, "queuedSubmissionId": queuedSubmissionID,
	}, &response)
	if requestErr != nil || response.Turn.ID == "" || response.Turn.Status != "inProgress" {
		if found, err := c.waitForStartedByClientID(
			ctx, threadID, target.ClientUserMessageID, target.Text,
		); err == nil && found {
			return nil
		} else if err != nil {
			if requestErr != nil {
				return fmt.Errorf("%w; queue start reconciliation failed: %v", requestErr, err)
			}
			return fmt.Errorf("queue start reconciliation failed: %w", err)
		}
		if requestErr != nil {
			return requestErr
		}
		return errors.New("thread/queue/start returned an invalid turn")
	}
	return nil
}

func (c *Client) Send(
	ctx context.Context, threadID, text, clientUserMessageID, actionContext string,
) (SendReceipt, error) {
	lock := c.queueUpdateLock(threadID)
	lock.Lock()
	defer lock.Unlock()
	attempt, attempted, err := c.sendAttempt(
		threadID, clientUserMessageID, text, actionContext,
	)
	if err != nil {
		return SendReceipt{}, err
	}
	if attempted && attempt.State == "accepted" {
		return SendReceipt{
			TurnID: attempt.TurnID, ClientUserMessageID: clientUserMessageID,
			Steered: attempt.Steered,
		}, nil
	}
	if attempted && attempt.State == "submitting" {
		receipt, found, err := c.sentByClientID(
			ctx, threadID, clientUserMessageID, text,
		)
		if err != nil {
			return SendReceipt{}, err
		}
		if found {
			receipt.Steered = attempt.Steered
			if err := c.markSendAccepted(threadID, clientUserMessageID, receipt); err != nil {
				return SendReceipt{}, &UnknownSendOutcomeError{Err: fmt.Errorf(
					"message was accepted but its receipt could not be recorded: %w", err,
				)}
			}
			return receipt, nil
		}
		return SendReceipt{}, &UnknownSendOutcomeError{
			Err: errors.New("the earlier attempt is not present in Codex history yet"),
		}
	}
	if err := c.resumeThread(ctx, threadID); err != nil {
		return SendReceipt{}, err
	}
	turnID, err := c.ActiveTurnID(ctx, threadID)
	if err != nil {
		return SendReceipt{}, err
	}
	steered := turnID != ""
	if attempted && attempt.Steered != steered {
		return SendReceipt{}, errors.New("prepared message no longer matches the thread state")
	}
	input := []map[string]any{{"type": "text", "text": text}}
	if !attempted {
		if err := c.recordSendAttempt(
			threadID, clientUserMessageID, text, actionContext, steered,
		); err != nil {
			return SendReceipt{}, fmt.Errorf("record message attempt before submission: %w", err)
		}
	}
	if err := c.markSendSubmitting(threadID, clientUserMessageID); err != nil {
		return SendReceipt{}, fmt.Errorf("record message attempt before submission: %w", err)
	}
	reconcile := func(requestErr error) (SendReceipt, error) {
		reconcileContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		receipt, found, reconcileErr := c.waitForSentByClientID(
			reconcileContext, threadID, clientUserMessageID, text,
		)
		if found && reconcileErr == nil {
			receipt.Steered = steered
			if err := c.markSendAccepted(threadID, clientUserMessageID, receipt); err != nil {
				return SendReceipt{}, &UnknownSendOutcomeError{Err: fmt.Errorf(
					"message was accepted but its receipt could not be recorded: %w", err,
				)}
			}
			return receipt, nil
		}
		if reconcileErr != nil {
			requestErr = fmt.Errorf("%w; history reconciliation failed: %v", requestErr, reconcileErr)
		}
		return SendReceipt{}, &UnknownSendOutcomeError{Err: requestErr}
	}
	if turnID != "" {
		var response struct {
			TurnID string `json:"turnId"`
		}
		err := c.Request(ctx, "turn/steer", map[string]any{
			"threadId": threadID, "expectedTurnId": turnID, "input": input,
			"clientUserMessageId": clientUserMessageID,
		}, &response)
		if err != nil {
			return reconcile(err)
		}
		if response.TurnID != turnID {
			return reconcile(errors.New("turn/steer returned the wrong turn"))
		}
		return c.finishAcceptedSend(threadID, SendReceipt{
			TurnID: turnID, ClientUserMessageID: clientUserMessageID, Steered: true,
		})
	}
	var response struct {
		Turn struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	err = c.Request(ctx, "turn/start", map[string]any{
		"threadId": threadID, "input": input,
		"clientUserMessageId": clientUserMessageID,
	}, &response)
	if err != nil {
		return reconcile(err)
	}
	if response.Turn.ID == "" {
		return reconcile(errors.New("turn/start returned no turn"))
	}
	return c.finishAcceptedSend(threadID, SendReceipt{
		TurnID: response.Turn.ID, ClientUserMessageID: clientUserMessageID,
	})
}

func (c *Client) finishAcceptedSend(
	threadID string, receipt SendReceipt,
) (SendReceipt, error) {
	if err := c.markSendAccepted(threadID, receipt.ClientUserMessageID, receipt); err != nil {
		return SendReceipt{}, &UnknownSendOutcomeError{Err: fmt.Errorf(
			"message was accepted but its receipt could not be recorded: %w", err,
		)}
	}
	return receipt, nil
}

func (c *Client) PrepareSend(
	threadID, text, clientUserMessageID, context string, steered bool,
) error {
	lock := c.queueUpdateLock(threadID)
	lock.Lock()
	defer lock.Unlock()
	return c.recordSendAttempt(threadID, clientUserMessageID, text, context, steered)
}

func (c *Client) SendAttempted(
	ctx context.Context, threadID, text, clientUserMessageID, context string,
) (bool, error) {
	lock := c.queueUpdateLock(threadID)
	lock.Lock()
	defer lock.Unlock()
	_, found, err := c.sendAttempt(threadID, clientUserMessageID, text, context)
	if err != nil || found {
		return found, err
	}
	receipt, found, err := c.sentByClientID(ctx, threadID, clientUserMessageID, text)
	if err != nil || !found {
		return found, err
	}
	if err := c.recordSendAttempt(
		threadID, clientUserMessageID, text, context, receipt.Steered,
	); err != nil {
		return false, err
	}
	if err := c.markSendSubmitting(threadID, clientUserMessageID); err != nil {
		return false, err
	}
	if err := c.markSendAccepted(threadID, clientUserMessageID, receipt); err != nil {
		return false, err
	}
	return true, nil
}

func (c *Client) EnsureInitialMessage(ctx context.Context, threadID, cwd, text string, allowUnmaterializedStart bool) error {
	materialized, err := c.HistoryMaterialized(ctx, threadID, cwd)
	if err != nil {
		return err
	}
	input := []map[string]any{{"type": "text", "text": text}}
	if !materialized {
		if !allowUnmaterializedStart {
			return errors.New("initial request may already have been accepted by the unmaterialized Codex thread")
		}
		if err := c.Request(ctx, "turn/start", map[string]any{"threadId": threadID, "input": input}, nil); err != nil {
			return err
		}
		return c.waitForInitialMessage(ctx, threadID, cwd, text)
	}
	matched, err := c.initialMessageMatches(ctx, threadID, text)
	if err != nil {
		return err
	}
	if !matched {
		return errors.New("materialized Codex thread has no initial user request")
	}
	return nil
}

func (c *Client) waitForInitialMessage(ctx context.Context, threadID, cwd, text string) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		materialized, err := c.HistoryMaterialized(ctx, threadID, cwd)
		if err != nil {
			if !initialRolloutPending(err) {
				return err
			}
		}
		if materialized {
			matched, err := c.initialMessageMatches(ctx, threadID, text)
			if err != nil {
				if !initialHistoryPending(err) {
					return err
				}
			}
			if matched {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for persisted initial Codex request: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func initialRolloutPending(err error) bool {
	var rpcErr *rpcCallError
	return errors.As(err, &rpcErr) && rpcErr.code == -32603 &&
		strings.HasPrefix(rpcErr.message, "failed to read thread:") &&
		strings.Contains(rpcErr.message, "rollout at ") &&
		strings.HasSuffix(rpcErr.message, " is empty")
}

func initialHistoryPending(err error) bool {
	var rpcErr *rpcCallError
	return errors.As(err, &rpcErr) && rpcErr.code == -32601 &&
		rpcErr.message == "list_turns is not supported yet"
}

func (c *Client) initialMessageMatches(ctx context.Context, threadID, text string) (bool, error) {
	if err := c.resumeThread(ctx, threadID); err != nil {
		return false, err
	}
	var page struct {
		Data *[]struct {
			Items []struct {
				Type    string `json:"type"`
				Content []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"content"`
			} `json:"items"`
		} `json:"data"`
	}
	if err := c.Request(ctx, "thread/turns/list", map[string]any{
		"threadId": threadID, "limit": 1, "sortDirection": "asc", "itemsView": "full",
	}, &page); err != nil {
		return false, err
	}
	if page.Data == nil {
		return false, errors.New("thread/turns/list returned no data")
	}
	if len(*page.Data) == 0 {
		return false, nil
	}
	var initialRequests []string
	for _, item := range (*page.Data)[0].Items {
		if item.Type != "userMessage" {
			continue
		}
		var parts []string
		for _, content := range item.Content {
			if content.Type != "text" {
				return false, errors.New("Codex thread has a non-text initial request")
			}
			parts = append(parts, content.Text)
		}
		initialRequests = append(initialRequests, strings.TrimSpace(strings.Join(parts, "\n")))
	}
	if len(initialRequests) == 0 {
		return false, nil
	}
	if len(initialRequests) != 1 || initialRequests[0] != strings.TrimSpace(text) {
		return false, errors.New("Codex thread already has a different initial request")
	}
	return true, nil
}

func (c *Client) HistoryMaterialized(ctx context.Context, threadID, cwd string) (bool, error) {
	var metadata struct {
		Thread struct {
			ID          string            `json:"id"`
			Cwd         string            `json:"cwd"`
			Path        *string           `json:"path"`
			Preview     string            `json:"preview"`
			Source      any               `json:"source"`
			Ephemeral   *bool             `json:"ephemeral"`
			HistoryMode string            `json:"historyMode"`
			Status      map[string]any    `json:"status"`
			Turns       *[]map[string]any `json:"turns"`
		} `json:"thread"`
	}
	if err := c.Request(ctx, "thread/read", map[string]any{"threadId": threadID}, &metadata); err != nil {
		return false, err
	}
	if metadata.Thread.ID != threadID {
		return false, errors.New("thread/read returned the wrong thread")
	}
	if metadata.Thread.Cwd != cwd {
		return false, errors.New("thread/read returned the wrong working directory")
	}
	if metadata.Thread.Path == nil {
		return false, errors.New("thread/read returned no rollout path")
	}
	path := *metadata.Thread.Path
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return false, errors.New("thread/read returned an invalid rollout path")
	}
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		fresh := c.threadSource(metadata.Thread.Source) &&
			metadata.Thread.Ephemeral != nil && !*metadata.Thread.Ephemeral &&
			metadata.Thread.HistoryMode == "paginated" &&
			metadata.Thread.Preview == "" &&
			statusValue(metadata.Thread.Status) == "idle" &&
			metadata.Thread.Turns != nil && len(*metadata.Thread.Turns) == 0
		if !fresh {
			turnCount := -1
			if metadata.Thread.Turns != nil {
				turnCount = len(*metadata.Thread.Turns)
			}
			ephemeral := "missing"
			if metadata.Thread.Ephemeral != nil {
				ephemeral = fmt.Sprint(*metadata.Thread.Ephemeral)
			}
			return false, fmt.Errorf(
				"unmaterialized Codex thread is not a fresh idle application thread "+
					"(source=%s ephemeral=%s historyMode=%q previewEmpty=%t status=%q turns=%d)",
				sourceDescription(metadata.Thread.Source), ephemeral,
				metadata.Thread.HistoryMode, metadata.Thread.Preview == "",
				statusValue(metadata.Thread.Status), turnCount,
			)
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect Codex thread rollout: %w", err)
	}
	if !info.Mode().IsRegular() {
		return false, errors.New("Codex thread rollout is not a regular file")
	}
	return true, nil
}

func (c *Client) threadSource(value any) bool {
	source, ok := value.(string)
	return ok && slices.Contains(c.options.ThreadSourceKinds, source)
}

func sourceDescription(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "<invalid>"
	}
	return string(encoded)
}

func (c *Client) Interrupt(ctx context.Context, threadID string) error {
	turnID, err := c.ActiveTurnID(ctx, threadID)
	if err != nil {
		return err
	}
	if turnID == "" {
		return errors.New("thread has no active turn")
	}
	return c.Request(ctx, "turn/interrupt", map[string]any{"threadId": threadID, "turnId": turnID}, nil)
}

// ActiveTurnID returns the current in-progress turn, or an empty string when
// the thread is idle.
func (c *Client) ActiveTurnID(ctx context.Context, threadID string) (string, error) {
	var page struct {
		Data *[]struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"data"`
	}
	if err := c.Request(ctx, "thread/turns/list", map[string]any{
		"threadId": threadID, "limit": 1, "sortDirection": "desc", "itemsView": "notLoaded",
	}, &page); err != nil {
		return "", err
	}
	if page.Data == nil {
		return "", errors.New("thread/turns/list returned no data")
	}
	if len(*page.Data) == 1 && (*page.Data)[0].Status == "inProgress" {
		return (*page.Data)[0].ID, nil
	}
	return "", nil
}

package codex

import (
	"bufio"
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
)

const (
	readLimit                       = 64 * 1024 * 1024
	queueLedgerMaxSize              = 1024 * 1024
	requestInputHiddenGrace         = 60 * time.Second
	requestInputVisibleCountdown    = 60 * time.Second
	recentTurnLimit                 = 20
	threadItemPageLimit             = 10
	DefaultNewThreadModel           = "gpt-6-astra"
	DefaultNewThreadReasoningEffort = "xhigh"
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
	TurnID              string `json:"turnId,omitempty"`
	TurnStatus          string `json:"turnStatus,omitempty"`
	ItemID              string `json:"itemId,omitempty"`
	ClientUserMessageID string `json:"clientUserMessageId,omitempty"`
	Kind                string `json:"kind"`
	Summary             string `json:"summary,omitempty"`
	Text                string `json:"text,omitempty"`
	HTML                string `json:"html,omitempty"`
	Details             string `json:"details,omitempty"`
}

type SendReceipt struct {
	TurnID              string `json:"turnId"`
	ClientUserMessageID string `json:"clientUserMessageId"`
	Steered             bool   `json:"steered"`
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

type ThreadActivity struct {
	ID        string
	Cwd       string
	UpdatedAt time.Time
}

type CollaborationMode struct {
	Name string `json:"name"`
	Mode string `json:"mode"`
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
	Schema      int                                        `json:"schema"`
	Attempts    map[string]map[string]string               `json:"attempts"`
	Sends       map[string]map[string]sendAttempt          `json:"sends,omitempty"`
	Deletions   map[string]map[string]queueDeletionAttempt `json:"deletions,omitempty"`
	Retirements map[string]string                          `json:"retirements,omitempty"`
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

func ResolveNewThreadSettings(models []Model, requested ThreadSettings) (ThreadSettings, error) {
	var selected *Model
	for index := range models {
		candidate := &models[index]
		if requested.Model != "" {
			if candidate.Model == requested.Model {
				selected = candidate
				break
			}
		} else if candidate.Model == DefaultNewThreadModel {
			if selected != nil {
				return ThreadSettings{}, fmt.Errorf(
					"Codex model catalog has more than one %q model", DefaultNewThreadModel,
				)
			}
			selected = candidate
		}
	}
	if selected == nil {
		if requested.Model == "" {
			return ThreadSettings{}, fmt.Errorf(
				"required default Codex model %q is not available", DefaultNewThreadModel,
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
	if effort == "" && supports(DefaultNewThreadReasoningEffort) {
		effort = DefaultNewThreadReasoningEffort
	}
	if effort == "" && requested.Model != "" && supports(selected.DefaultReasoningEffort) {
		effort = selected.DefaultReasoningEffort
	}
	if effort == "" {
		return ThreadSettings{}, fmt.Errorf(
			"Codex model %s does not support the required default reasoning effort %q",
			selected.DisplayName, DefaultNewThreadReasoningEffort,
		)
	}
	if !supports(effort) {
		return ThreadSettings{}, fmt.Errorf(
			"reasoning effort %q is not available for %s", effort, selected.DisplayName,
		)
	}
	return ThreadSettings{Model: selected.Model, ReasoningEffort: effort}, nil
}

type Client struct {
	socket string

	ensureMu     sync.Mutex
	connectionMu sync.Mutex
	connection   *websocket.Conn
	generation   uint64
	ready        uint64
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
	retirements      map[string]string
	queueLedgerPath  string
	queueLedgerReady bool
	queueLedgerErr   error

	watchedMu         sync.Mutex
	watched           map[string]int
	watchedGeneration map[string]uint64
	watchLocks        map[string]*sync.Mutex
}

func DefaultSocket() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".codex", "app-server-control", "app-server-control.sock")
}

func New(socket string) *Client {
	if socket == "" {
		socket = DefaultSocket()
	}
	return &Client{
		socket: socket, pending: make(map[uint64]pendingCall),
		requests: make(map[string]PendingRequest), notices: make(map[string][]Prompt),
		subscribers: make(map[chan struct{}]string), watched: make(map[string]int),
		watchedGeneration: make(map[string]uint64), watchLocks: make(map[string]*sync.Mutex),
		threadSettings: make(map[string]cachedThreadSettings), settingsChanged: make(chan struct{}),
		settingsUpdates: make(map[string]*sync.Mutex),
		queueUpdates:    make(map[string]*sync.Mutex), queueAttempts: make(map[string]map[string]string),
		sendAttempts:    make(map[string]map[string]sendAttempt),
		queueDeletions:  make(map[string]map[string]queueDeletionAttempt),
		retirements:     make(map[string]string),
		queueLedgerPath: socket + ".submission-attempts-v2.json",
	}
}

func (c *Client) Ensure(ctx context.Context) error {
	c.ensureMu.Lock()
	defer c.ensureMu.Unlock()
	c.connectionMu.Lock()
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
	c.connectionMu.Unlock()
	go c.readLoop(connection, generation)

	initializeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := c.requestOn(initializeCtx, connection, generation, "initialize", map[string]any{
		"capabilities": map[string]any{"experimentalApi": true},
		"clientInfo": map[string]any{
			"name":    "vpsfree-workspace-portal",
			"title":   "vpsFree.cz Workspace Portal",
			"version": "0.1.0",
		},
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
	connection := c.connection
	generation := c.generation
	c.connectionMu.Unlock()
	if connection != nil {
		connection.CloseNow()
		c.markDisconnected(connection, generation, errors.New("client closed"))
	}
}

func (c *Client) Request(ctx context.Context, method string, params any, result any) error {
	if err := c.Ensure(ctx); err != nil {
		return err
	}
	return c.requestConnected(ctx, method, params, result)
}

func (c *Client) requestConnected(ctx context.Context, method string, params any, result any) error {
	c.connectionMu.Lock()
	connection := c.connection
	generation := c.generation
	ready := c.ready
	c.connectionMu.Unlock()
	if connection == nil || ready != generation {
		return errors.New("Codex App Server is disconnected or initializing")
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
		if message.Method != "" && len(message.ID) != 0 {
			key := string(message.ID)
			request := PendingRequest{
				ID: key, Method: message.Method, Params: message.Params,
				generation: generation, connection: connection, receivedAt: time.Now(),
			}
			prompt, promptErr := normalizePrompt(request)
			if promptErr != nil {
				c.rejectUnsupported(request, promptErr)
				continue
			}
			c.pendingMu.Lock()
			c.requests[key] = request
			c.pendingMu.Unlock()
			if prompt.Kind == "userInput" && !prompt.IsBlocking {
				go c.autoResolveUserInput(
					request.ID, prompt.ThreadID,
					requestInputHiddenGrace+requestInputVisibleCountdown,
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
	prompt, err := normalizePrompt(request)
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
	prompt, err := normalizePrompt(request)
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
				"message": "workspace portal does not support this App Server request",
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
	transition := c.watchLock(threadID)
	transition.Lock()
	defer transition.Unlock()
	c.watchedMu.Lock()
	removed := false
	if c.watched[threadID] <= 1 {
		delete(c.watched, threadID)
		delete(c.watchedGeneration, threadID)
		removed = true
	} else {
		c.watched[threadID]--
	}
	c.watchedMu.Unlock()
	if removed {
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
	lock := c.watchLock(threadID)
	lock.Lock()
	defer lock.Unlock()
	c.connectionMu.Lock()
	generation := c.generation
	c.connectionMu.Unlock()
	c.watchedMu.Lock()
	if c.watched[threadID] == 0 || c.watchedGeneration[threadID] == generation {
		c.watchedMu.Unlock()
		return nil
	}
	c.watchedMu.Unlock()
	var result map[string]any
	if err := c.requestConnected(ctx, "thread/resume", map[string]any{
		"threadId": threadID, "excludeTurns": true,
	}, &result); err != nil {
		return err
	}
	c.watchedMu.Lock()
	if c.watched[threadID] > 0 {
		c.watchedGeneration[threadID] = generation
	}
	c.watchedMu.Unlock()
	c.clearWatchError(threadID)
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
		prompt, err := normalizePrompt(request)
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
	prompt, err := normalizePrompt(request)
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
		if !prompt.IsBlocking && !request.receivedAt.IsZero() {
			visibleAt := request.receivedAt.Add(requestInputHiddenGrace)
			deadline := request.receivedAt.Add(
				requestInputHiddenGrace + requestInputVisibleCountdown,
			)
			prompt.AutoResolutionVisibleAtMS = visibleAt.UnixMilli()
			prompt.AutoResolutionAtMS = deadline.UnixMilli()
			prompt.AutoResolveSnoozed = request.snoozed
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
	entries, err := c.listThreadItems(ctx, threadID, func(entry threadItemEntry) bool {
		delete(wanted, stringValue(entry.Item["id"]))
		return len(wanted) == 0
	})
	if err != nil {
		return nil, err
	}
	items := make(map[string]map[string]any, len(entries))
	for _, entry := range entries {
		items[stringValue(entry.Item["id"])] = entry.Item
	}
	return items, nil
}

func (c *Client) listThreadItems(
	ctx context.Context, threadID string, stop func(threadItemEntry) bool,
) ([]threadItemEntry, error) {
	entries := make([]threadItemEntry, 0)
	seenCursors := make(map[string]struct{})
	seenIDs := make(map[string]struct{})
	var cursor string
	for pageNumber := 0; ; pageNumber++ {
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
			return nil, err
		}
		if page.Data == nil {
			return nil, errors.New("thread/items/list returned no data")
		}
		for _, entry := range *page.Data {
			id := stringValue(entry.Item["id"])
			if entry.TurnID == "" || entry.Item == nil || id == "" {
				return nil, errors.New("thread/items/list returned an invalid item entry")
			}
			if _, exists := seenIDs[id]; exists {
				return nil, fmt.Errorf("thread/items/list repeated item %q", id)
			}
			seenIDs[id] = struct{}{}
			entries = append(entries, entry)
			if stop != nil && stop(entry) {
				return entries, nil
			}
		}
		if page.NextCursor == nil {
			return entries, nil
		}
		if pageNumber+1 >= threadItemPageLimit {
			return nil, fmt.Errorf(
				"thread/items/list exceeds the %d-page reconciliation limit",
				threadItemPageLimit,
			)
		}
		if *page.NextCursor == "" {
			return nil, errors.New("thread/items/list returned an empty pagination cursor")
		}
		if _, exists := seenCursors[*page.NextCursor]; exists {
			return nil, errors.New("thread/items/list repeated a pagination cursor")
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
	if c.queueLedgerReady {
		return c.queueLedgerErr
	}
	c.queueLedgerReady = true
	info, err := os.Lstat(c.queueLedgerPath)
	if errors.Is(err, os.ErrNotExist) {
		c.queueAttempts = make(map[string]map[string]string)
		c.sendAttempts = make(map[string]map[string]sendAttempt)
		c.queueDeletions = make(map[string]map[string]queueDeletionAttempt)
		c.retirements = make(map[string]string)
		return nil
	}
	if err != nil {
		c.queueLedgerErr = fmt.Errorf("inspect queue attempt ledger: %w", err)
		return c.queueLedgerErr
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		c.queueLedgerErr = errors.New("queue attempt ledger must be a mode-0600 regular file")
		return c.queueLedgerErr
	}
	if info.Size() > queueLedgerMaxSize {
		c.queueLedgerErr = errors.New("queue attempt ledger exceeds 1 MiB")
		return c.queueLedgerErr
	}
	file, err := os.Open(c.queueLedgerPath)
	if err != nil {
		c.queueLedgerErr = fmt.Errorf("open queue attempt ledger: %w", err)
		return c.queueLedgerErr
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var ledger queueAttemptLedger
	if err := decoder.Decode(&ledger); err != nil {
		c.queueLedgerErr = fmt.Errorf("decode queue attempt ledger: %w", err)
		return c.queueLedgerErr
	}
	if ledger.Schema != 2 || ledger.Attempts == nil {
		c.queueLedgerErr = errors.New("queue attempt ledger has an invalid schema")
		return c.queueLedgerErr
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		c.queueLedgerErr = errors.New("queue attempt ledger contains trailing JSON data")
		return c.queueLedgerErr
	}
	if ledger.Sends == nil {
		ledger.Sends = make(map[string]map[string]sendAttempt)
	}
	if ledger.Retirements == nil {
		ledger.Retirements = make(map[string]string)
	}
	if ledger.Deletions == nil {
		ledger.Deletions = make(map[string]map[string]queueDeletionAttempt)
	}
	for threadID, attempts := range ledger.Attempts {
		if threadID == "" || attempts == nil {
			c.queueLedgerErr = errors.New("queue attempt ledger contains an invalid thread")
			return c.queueLedgerErr
		}
		for clientID, digest := range attempts {
			if clientID == "" || !validSubmissionDigest(digest) {
				c.queueLedgerErr = errors.New("queue attempt ledger contains an invalid attempt")
				return c.queueLedgerErr
			}
		}
	}
	for threadID, attempts := range ledger.Sends {
		if threadID == "" || attempts == nil {
			c.queueLedgerErr = errors.New("queue attempt ledger contains an invalid send thread")
			return c.queueLedgerErr
		}
		for clientID, attempt := range attempts {
			if clientID == "" || !validSubmissionDigest(attempt.Digest) ||
				(attempt.State != "prepared" && attempt.State != "submitting") {
				c.queueLedgerErr = errors.New("queue attempt ledger contains an invalid send attempt")
				return c.queueLedgerErr
			}
		}
	}
	for threadID, attempts := range ledger.Deletions {
		if threadID == "" || attempts == nil {
			c.queueLedgerErr = errors.New("queue attempt ledger contains an invalid deletion thread")
			return c.queueLedgerErr
		}
		for queuedID, attempt := range attempts {
			if queuedID == "" || attempt.ClientUserMessageID == "" ||
				!validSubmissionDigest(attempt.Digest) {
				c.queueLedgerErr = errors.New("queue attempt ledger contains an invalid deletion attempt")
				return c.queueLedgerErr
			}
		}
	}
	for cwd, threadID := range ledger.Retirements {
		if !filepath.IsAbs(cwd) || threadID == "" {
			c.queueLedgerErr = errors.New("queue attempt ledger contains an invalid retirement")
			return c.queueLedgerErr
		}
	}
	c.queueAttempts = ledger.Attempts
	c.sendAttempts = ledger.Sends
	c.queueDeletions = ledger.Deletions
	c.retirements = ledger.Retirements
	return nil
}

func (c *Client) writeQueueLedgerLocked() error {
	directory := filepath.Dir(c.queueLedgerPath)
	encoded, err := json.Marshal(queueAttemptLedger{
		Schema: 2, Attempts: c.queueAttempts, Sends: c.sendAttempts,
		Deletions: c.queueDeletions, Retirements: c.retirements,
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
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	if err := c.loadQueueLedgerLocked(); err != nil {
		return false, err
	}
	digest, ok := c.queueAttempts[threadID][clientID]
	if ok && digest != queueTextDigest(text) {
		return false, errors.New("queued message identity was reused with different text")
	}
	return ok, nil
}

func (c *Client) recordQueueAttempt(threadID, clientID, text string) error {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	if err := c.loadQueueLedgerLocked(); err != nil {
		return err
	}
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
}

func (c *Client) sendAttempt(
	threadID, clientID, text, context string,
) (sendAttempt, bool, error) {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	if err := c.loadQueueLedgerLocked(); err != nil {
		return sendAttempt{}, false, err
	}
	attempt, ok := c.sendAttempts[threadID][clientID]
	if ok && (attempt.Digest != queueTextDigest(text) || attempt.Context != context) {
		return sendAttempt{}, false, errors.New("message identity was reused for another action")
	}
	return attempt, ok, nil
}

func (c *Client) recordSendAttempt(
	threadID, clientID, text, context string, steered bool,
) error {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	if err := c.loadQueueLedgerLocked(); err != nil {
		return err
	}
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
}

func (c *Client) markSendSubmitting(threadID, clientID string) error {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	if err := c.loadQueueLedgerLocked(); err != nil {
		return err
	}
	attempt, ok := c.sendAttempts[threadID][clientID]
	if !ok {
		return errors.New("prepared message attempt disappeared")
	}
	if attempt.State == "submitting" {
		return nil
	}
	attempt.State = "submitting"
	c.sendAttempts[threadID][clientID] = attempt
	if err := c.writeQueueLedgerLocked(); err != nil {
		attempt.State = "prepared"
		c.sendAttempts[threadID][clientID] = attempt
		return err
	}
	return nil
}

func (c *Client) clearSendAttempt(threadID, clientID string) error {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	if err := c.loadQueueLedgerLocked(); err != nil {
		return err
	}
	attempts := c.sendAttempts[threadID]
	attempt, ok := attempts[clientID]
	if !ok {
		return nil
	}
	delete(attempts, clientID)
	if len(attempts) == 0 {
		delete(c.sendAttempts, threadID)
	}
	if err := c.writeQueueLedgerLocked(); err != nil {
		if c.sendAttempts[threadID] == nil {
			c.sendAttempts[threadID] = make(map[string]sendAttempt)
		}
		c.sendAttempts[threadID][clientID] = attempt
		return err
	}
	return nil
}

func (c *Client) clearQueueAttempt(threadID, clientID string) error {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	if err := c.loadQueueLedgerLocked(); err != nil {
		return err
	}
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
}

func (c *Client) queueDeletionAttempt(
	threadID, queuedSubmissionID string,
) (queueDeletionAttempt, bool, error) {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	if err := c.loadQueueLedgerLocked(); err != nil {
		return queueDeletionAttempt{}, false, err
	}
	attempt, ok := c.queueDeletions[threadID][queuedSubmissionID]
	return attempt, ok, nil
}

func (c *Client) recordQueueDeletionAttempt(
	threadID string, target QueueEntry,
) (queueDeletionAttempt, error) {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	if err := c.loadQueueLedgerLocked(); err != nil {
		return queueDeletionAttempt{}, err
	}
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
}

func (c *Client) clearQueueDeletionAndAttempt(
	threadID, queuedSubmissionID, clientID string,
) error {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	if err := c.loadQueueLedgerLocked(); err != nil {
		return err
	}
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
}

func (c *Client) requireSubmissionAttemptsResolved(ctx context.Context, threadID string) error {
	c.queueMu.Lock()
	if err := c.loadQueueLedgerLocked(); err != nil {
		c.queueMu.Unlock()
		return err
	}
	deletionIDs := make([]string, 0, len(c.queueDeletions[threadID]))
	for queuedSubmissionID := range c.queueDeletions[threadID] {
		deletionIDs = append(deletionIDs, queuedSubmissionID)
	}
	c.queueMu.Unlock()
	for _, queuedSubmissionID := range deletionIDs {
		if err := c.DeleteQueueEntry(ctx, threadID, queuedSubmissionID); err != nil {
			return fmt.Errorf("reconcile queued message deletion: %w", err)
		}
	}

	c.queueMu.Lock()
	if err := c.loadQueueLedgerLocked(); err != nil {
		c.queueMu.Unlock()
		return err
	}
	queueAttempts := make(map[string]string, len(c.queueAttempts[threadID]))
	for clientID, digest := range c.queueAttempts[threadID] {
		queueAttempts[clientID] = digest
	}
	sendCount := len(c.sendAttempts[threadID])
	c.queueMu.Unlock()
	if sendCount > 0 {
		return fmt.Errorf("Codex thread %s has %d unresolved message attempt(s)", threadID, sendCount)
	}
	if len(queueAttempts) == 0 {
		return nil
	}

	items, err := c.listThreadItems(ctx, threadID, nil)
	if err != nil {
		return fmt.Errorf("reconcile queued message attempts: %w", err)
	}
	resolved := make(map[string]string)
	for _, entry := range items {
		if stringValue(entry.Item["type"]) != "userMessage" {
			continue
		}
		clientID := stringValue(entry.Item["clientId"])
		if _, tracked := queueAttempts[clientID]; !tracked {
			continue
		}
		content, ok := entry.Item["content"].([]any)
		if !ok {
			return errors.New("queued message attempt has invalid history content")
		}
		inputs := make([]map[string]any, 0, len(content))
		for _, value := range content {
			input, ok := value.(map[string]any)
			if !ok {
				return errors.New("queued message attempt has invalid history input")
			}
			inputs = append(inputs, input)
		}
		resolved[clientID] = queueTextDigest(inputText(inputs))
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

func (c *Client) retirementAttempt(cwd string) (string, error) {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	if err := c.loadQueueLedgerLocked(); err != nil {
		return "", err
	}
	return c.retirements[cwd], nil
}

func (c *Client) recordRetirementAttempt(cwd, threadID string) error {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	if err := c.loadQueueLedgerLocked(); err != nil {
		return err
	}
	if existing := c.retirements[cwd]; existing != "" {
		if existing != threadID {
			return errors.New("retirement directory is already bound to another Codex thread")
		}
		return nil
	}
	c.retirements[cwd] = threadID
	if err := c.writeQueueLedgerLocked(); err != nil {
		delete(c.retirements, cwd)
		return err
	}
	return nil
}

func (c *Client) clearThreadAttempts(threadID, cwd string) error {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	if err := c.loadQueueLedgerLocked(); err != nil {
		return err
	}
	queueAttempts, queued := c.queueAttempts[threadID]
	sendAttempts, sent := c.sendAttempts[threadID]
	queueDeletions, deleting := c.queueDeletions[threadID]
	retirementThreadID, retiring := c.retirements[cwd]
	if retiring && retirementThreadID != threadID {
		return errors.New("retirement directory is bound to another Codex thread")
	}
	if !queued && !sent && !deleting && !retiring {
		return nil
	}
	delete(c.queueAttempts, threadID)
	delete(c.sendAttempts, threadID)
	delete(c.queueDeletions, threadID)
	delete(c.retirements, cwd)
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
		if retiring {
			c.retirements[cwd] = retirementThreadID
		}
		return err
	}
	return nil
}

func stringValue(value any) string {
	result, _ := value.(string)
	return result
}

func settingsParams(settings ThreadSettings, params map[string]any, config map[string]any) {
	if settings.Model != "" {
		params["model"] = settings.Model
	}
	if settings.ReasoningEffort != "" {
		config["model_reasoning_effort"] = settings.ReasoningEffort
	}
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
		"cwd":                   cwd,
		"runtimeWorkspaceRoots": []string{environment["VPSFREE_DEV_SESSION_WORKSPACE"]},
		"config":                config,
	}
	settingsParams(settings, params, config)
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
	params := map[string]any{
		"threadId":     threadID,
		"cwd":          cwd,
		"excludeTurns": true,
		"config":       config,
	}
	settingsParams(settings, params, config)
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

func (c *Client) loadedThreadIDs(ctx context.Context) ([]string, error) {
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

func (c *Client) RecoverCreatingThread(ctx context.Context, threadID, cwd string, environment map[string]string) (string, error) {
	return c.RecoverCreatingThreadWithSettings(ctx, threadID, cwd, environment, ThreadSettings{})
}

func (c *Client) RecoverCreatingThreadWithSettings(
	ctx context.Context, threadID, cwd string, environment map[string]string, settings ThreadSettings,
) (string, error) {
	return c.RecoverCreatingThreadWithSettingsResolver(
		ctx, threadID, cwd, environment,
		func() (ThreadSettings, error) { return settings, nil },
	)
}

// RecoverCreatingThreadWithSettingsResolver inspects persisted candidates
// before resolving settings. Existing threads already own their creation
// settings, so catalog changes matter only when a replacement must be created.
func (c *Client) RecoverCreatingThreadWithSettingsResolver(
	ctx context.Context,
	threadID, cwd string,
	environment map[string]string,
	resolveSettings func() (ThreadSettings, error),
) (string, error) {
	candidates := make(map[string]struct{})
	loaded, err := c.loadedThreadIDs(ctx)
	if err != nil {
		return "", err
	}
	for _, loadedID := range loaded {
		var metadata struct {
			Thread struct {
				ID     string `json:"id"`
				Cwd    string `json:"cwd"`
				Source any    `json:"source"`
			} `json:"thread"`
		}
		if err := c.Request(ctx, "thread/read", map[string]any{"threadId": loadedID}, &metadata); err != nil {
			current, listErr := c.loadedThreadIDs(ctx)
			if listErr == nil && !slices.Contains(current, loadedID) {
				continue
			}
			return "", err
		}
		if metadata.Thread.ID != loadedID {
			return "", errors.New("thread/read returned the wrong loaded thread")
		}
		if loadedID == threadID &&
			(metadata.Thread.Cwd != cwd || !portalThreadSource(metadata.Thread.Source)) {
			return "", errors.New("recorded creation thread has the wrong identity")
		}
		if metadata.Thread.Cwd == cwd && portalThreadSource(metadata.Thread.Source) {
			candidates[loadedID] = struct{}{}
		}
	}
	var page struct {
		Data *[]struct {
			ID  string `json:"id"`
			Cwd string `json:"cwd"`
		} `json:"data"`
	}
	if err := c.Request(ctx, "thread/list", map[string]any{
		"cwd": cwd, "limit": 2, "sortDirection": "asc", "sourceKinds": []string{"vscode"},
	}, &page); err != nil {
		return "", err
	}
	if page.Data == nil {
		return "", errors.New("thread/list returned no data")
	}
	for _, candidate := range *page.Data {
		if candidate.ID == "" || candidate.Cwd != cwd {
			return "", errors.New("thread/list returned an invalid creation candidate")
		}
		candidates[candidate.ID] = struct{}{}
	}
	if len(candidates) > 1 {
		return "", fmt.Errorf("multiple Codex threads use creation directory %s; refusing ambiguous recovery", cwd)
	}
	for candidateID := range candidates {
		materialized, err := c.threadHistoryMaterialized(ctx, candidateID, cwd)
		if err != nil {
			return "", err
		}
		if candidateID != threadID && materialized {
			return "", errors.New("refusing a different materialized Codex thread as a creation replacement")
		}
		if !materialized {
			return candidateID, nil
		}
		// The materialized thread already owns the settings chosen when creation
		// began. Recovery refreshes only runtime configuration; applying today's
		// defaults here would silently change a session across a deployment.
		return c.ResumeThread(ctx, candidateID, cwd, environment)
	}
	settings, err := resolveSettings()
	if err != nil {
		return "", err
	}
	return c.StartThreadWithSettings(ctx, cwd, environment, settings)
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
		if entry.Mode == nil || entry.Name == "" ||
			!slices.Contains([]string{"default", "plan"}, *entry.Mode) {
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

func collaborationModeFromRollout(path string) (string, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", errors.New("thread/read returned an invalid rollout path")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open Codex thread rollout: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("inspect Codex thread rollout: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("Codex thread rollout is not a regular file")
	}
	offset := int64(0)
	if info.Size() > readLimit {
		offset = info.Size() - readLimit
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			return "", fmt.Errorf("seek Codex thread rollout: %w", err)
		}
	}
	reader := bufio.NewReader(file)
	if offset > 0 {
		for {
			_, err := reader.ReadSlice('\n')
			if err == nil {
				break
			}
			if errors.Is(err, bufio.ErrBufferFull) {
				continue
			}
			if errors.Is(err, io.EOF) {
				return "", errors.New("Codex thread rollout tail has no complete record")
			}
			return "", fmt.Errorf("read Codex thread rollout tail: %w", err)
		}
	}
	mode := ""
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), readLimit)
	for scanner.Scan() {
		var entry struct {
			Type    string `json:"type"`
			Payload struct {
				Type              string `json:"type"`
				CollaborationMode struct {
					Mode string `json:"mode"`
				} `json:"collaboration_mode"`
				ThreadSettings struct {
					CollaborationMode struct {
						Mode string `json:"mode"`
					} `json:"collaboration_mode"`
				} `json:"thread_settings"`
			} `json:"payload"`
		}
		if json.Unmarshal(scanner.Bytes(), &entry) != nil {
			continue
		}
		candidate := ""
		if entry.Type == "turn_context" {
			candidate = entry.Payload.CollaborationMode.Mode
		} else if entry.Type == "event_msg" && entry.Payload.Type == "thread_settings_applied" {
			candidate = entry.Payload.ThreadSettings.CollaborationMode.Mode
		}
		if candidate != "" && len(candidate) <= 1024 {
			mode = candidate
		}
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("scan Codex thread rollout: %w", err)
	}
	if mode == "" {
		return "", errors.New("Codex thread rollout has no collaboration mode")
	}
	return mode, nil
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
	if err := c.requireThreadTurnsIdle(ctx, threadID); err != nil {
		return "", err
	}
	config := map[string]any{"shell_environment_policy": map[string]any{"set": environment}}
	params := map[string]any{
		"threadId": threadID, "cwd": cwd, "excludeTurns": true,
		"deferGoalContinuation": true,
		"runtimeWorkspaceRoots": []string{environment["VPSFREE_DEV_SESSION_WORKSPACE"]},
		"config":                config,
	}
	settingsParams(settings, params, config)
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

func (c *Client) RecoverForkThread(
	ctx context.Context, sourceThreadID, cwd string, environment map[string]string, settings ThreadSettings,
) (string, error) {
	var page struct {
		Data *[]struct {
			ID           string `json:"id"`
			Cwd          string `json:"cwd"`
			ForkedFromID string `json:"forkedFromId"`
		} `json:"data"`
		NextCursor *string `json:"nextCursor"`
	}
	if err := c.Request(ctx, "thread/list", map[string]any{
		"cwd": cwd, "limit": 2, "sortDirection": "asc", "sourceKinds": []string{"vscode"},
	}, &page); err != nil {
		return "", err
	}
	if page.Data == nil {
		return "", errors.New("thread/list returned no data")
	}
	if len(*page.Data) > 1 || page.NextCursor != nil {
		return "", fmt.Errorf("multiple Codex threads use fork directory %s; refusing ambiguous recovery", cwd)
	}
	if len(*page.Data) == 0 {
		return c.ForkThread(ctx, sourceThreadID, cwd, environment, settings)
	}
	candidate := (*page.Data)[0]
	if candidate.ID == "" || candidate.Cwd != cwd || candidate.ForkedFromID != sourceThreadID {
		return "", errors.New("existing Codex thread does not match the requested conversation fork")
	}
	if err := c.requireThreadTurnsIdle(ctx, candidate.ID); err != nil {
		return "", err
	}
	return c.ResumeThreadWithSettings(ctx, candidate.ID, cwd, environment, settings)
}

// RecoverArchivedThread restores one exact portal conversation after its
// initiative tracking has been revived. It accepts a retry after Codex has
// already completed the unarchive operation, but never substitutes another
// conversation that happens to use the same working directory.
func (c *Client) RecoverArchivedThread(
	ctx context.Context, threadID, cwd string, environment map[string]string,
) (string, error) {
	if threadID == "" || cwd == "" {
		return "", errors.New("archived thread recovery requires a thread id and working directory")
	}
	active, activeFound, err := c.retirementCandidate(ctx, cwd, false)
	if err != nil {
		return "", err
	}
	_, archivedFound, err := c.retirementThreadByID(ctx, threadID, cwd, true)
	if err != nil {
		return "", err
	}
	if activeFound && active.ID != threadID {
		return "", errors.New("another active Codex thread uses the revived session directory")
	}
	if activeFound && archivedFound {
		return "", errors.New("the revived Codex thread exists in both active and archived history")
	}
	if activeFound {
		return c.ResumeThread(ctx, threadID, cwd, environment)
	}
	if !archivedFound {
		return "", errors.New("the revived Codex thread is neither active nor archived")
	}

	var response struct {
		Thread retirementThread `json:"thread"`
	}
	if err := c.Request(ctx, "thread/unarchive", map[string]any{"threadId": threadID}, &response); err != nil {
		return "", err
	}
	if response.Thread.ID != threadID || response.Thread.Cwd != cwd ||
		!portalThreadSource(response.Thread.Source) {
		return "", errors.New("thread/unarchive returned the wrong Codex thread identity")
	}
	return c.ResumeThread(ctx, threadID, cwd, environment)
}

func (c *Client) SetName(ctx context.Context, threadID, name string) error {
	return c.Request(ctx, "thread/name/set", map[string]any{"threadId": threadID, "name": name}, nil)
}

func (c *Client) RetireThread(ctx context.Context, threadID, cwd string, force bool) error {
	var candidate retirementThread
	var found bool
	var err error
	if threadID == "" {
		threadID, err = c.retirementAttempt(cwd)
		if err != nil {
			return err
		}
		if threadID == "" {
			candidate, found, err = c.retirementCandidate(ctx, cwd, false)
			if err != nil {
				return err
			}
			if !found {
				return nil
			}
			threadID = candidate.ID
			if err := c.recordRetirementAttempt(cwd, threadID); err != nil {
				return fmt.Errorf("record Codex thread retirement before archival: %w", err)
			}
		}
	}
	candidate, found, err = c.retirementCandidate(ctx, cwd, false)
	if err != nil {
		return err
	}
	if found && candidate.ID != threadID {
		return errors.New("another Codex thread uses the portal session directory")
	}
	if !found {
		_, archivedFound, err := c.retirementThreadByID(ctx, threadID, cwd, true)
		if err != nil {
			return err
		}
		if archivedFound {
			return c.clearThreadAttempts(threadID, cwd)
		}
		return errors.New("the expected Codex thread is neither active nor archived")
	}

	var metadata struct {
		Thread map[string]any `json:"thread"`
	}
	if err := c.Request(ctx, "thread/read", map[string]any{
		"threadId": threadID, "excludeTurns": true,
	}, &metadata); err != nil {
		return err
	}
	if stringValue(metadata.Thread["id"]) != threadID ||
		stringValue(metadata.Thread["cwd"]) != cwd ||
		!portalThreadSource(metadata.Thread["source"]) {
		return errors.New("Codex thread does not match the portal session being removed")
	}
	freshWithoutRollout := false
	if force {
		turnID, err := c.activeTurnID(ctx, threadID)
		if err != nil {
			if freshThreadMissingSourceRollout(metadata.Thread, threadID, err) {
				freshWithoutRollout = true
			} else {
				return err
			}
		}
		if turnID != "" {
			if err := c.Request(ctx, "turn/interrupt", map[string]any{
				"threadId": threadID, "turnId": turnID,
			}, nil); err != nil {
				return err
			}
		}
	}
	for !freshWithoutRollout {
		err := c.requireThreadTurnsIdle(ctx, threadID)
		if err == nil {
			break
		}
		if freshThreadMissingSourceRollout(metadata.Thread, threadID, err) {
			break
		}
		if !force {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for interrupted Codex thread: %w", ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
	if err := c.Request(ctx, "thread/archive", map[string]any{"threadId": threadID}, nil); err != nil {
		return err
	}
	return c.clearThreadAttempts(threadID, cwd)
}

type retirementThread struct {
	ID     string `json:"id"`
	Cwd    string `json:"cwd"`
	Source any    `json:"source"`
}

// ListThreadActivity returns the active portal-owned threads in one paginated
// scan. Callers still match both the recorded thread ID and working directory;
// the list is only an activity hint and never an identity authority.
func (c *Client) ListThreadActivity(ctx context.Context) ([]ThreadActivity, error) {
	seenCursors := make(map[string]struct{})
	seenIDs := make(map[string]struct{})
	activities := make([]ThreadActivity, 0)
	var cursor string
	for {
		params := map[string]any{
			"limit": 100, "sortDirection": "desc",
			"sourceKinds": []string{"vscode"}, "archived": false,
		}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var page struct {
			Data *[]struct {
				ID        string `json:"id"`
				Cwd       string `json:"cwd"`
				Source    any    `json:"source"`
				UpdatedAt int64  `json:"updatedAt"`
			} `json:"data"`
			NextCursor *string `json:"nextCursor"`
		}
		if err := c.Request(ctx, "thread/list", params, &page); err != nil {
			return nil, err
		}
		if page.Data == nil {
			return nil, errors.New("thread/list returned no data")
		}
		for _, thread := range *page.Data {
			if thread.ID == "" || thread.Cwd == "" || thread.UpdatedAt < 0 ||
				!portalThreadSource(thread.Source) {
				return nil, errors.New("thread/list returned invalid activity metadata")
			}
			if _, duplicate := seenIDs[thread.ID]; duplicate {
				return nil, fmt.Errorf("thread/list repeated thread %q", thread.ID)
			}
			seenIDs[thread.ID] = struct{}{}
			activities = append(activities, ThreadActivity{
				ID: thread.ID, Cwd: thread.Cwd, UpdatedAt: time.Unix(thread.UpdatedAt, 0),
			})
		}
		if page.NextCursor == nil {
			return activities, nil
		}
		if *page.NextCursor == "" {
			return nil, errors.New("thread/list returned an empty activity cursor")
		}
		if _, duplicate := seenCursors[*page.NextCursor]; duplicate {
			return nil, errors.New("thread/list repeated an activity cursor")
		}
		seenCursors[*page.NextCursor] = struct{}{}
		cursor = *page.NextCursor
	}
}

func (c *Client) retirementCandidate(
	ctx context.Context, cwd string, archived bool,
) (retirementThread, bool, error) {
	var page struct {
		Data       *[]retirementThread `json:"data"`
		NextCursor *string             `json:"nextCursor"`
	}
	if err := c.Request(ctx, "thread/list", map[string]any{
		"cwd": cwd, "limit": 2, "sortDirection": "asc", "sourceKinds": []string{"vscode"},
		"archived": archived,
	}, &page); err != nil {
		return retirementThread{}, false, err
	}
	if page.Data == nil {
		return retirementThread{}, false, errors.New("thread/list returned no data")
	}
	if len(*page.Data) > 1 || page.NextCursor != nil {
		return retirementThread{}, false,
			fmt.Errorf("multiple Codex threads use %s; refusing ambiguous retirement", cwd)
	}
	if len(*page.Data) == 0 {
		return retirementThread{}, false, nil
	}
	candidate := (*page.Data)[0]
	if candidate.ID == "" || candidate.Cwd != cwd || !portalThreadSource(candidate.Source) {
		return retirementThread{}, false, errors.New("thread/list returned an invalid retirement candidate")
	}
	return candidate, true, nil
}

func (c *Client) retirementThreadByID(
	ctx context.Context, threadID, cwd string, archived bool,
) (retirementThread, bool, error) {
	seenCursors := make(map[string]struct{})
	var cursor string
	for {
		params := map[string]any{
			"cwd": cwd, "limit": 100, "sortDirection": "asc",
			"sourceKinds": []string{"vscode"}, "archived": archived,
		}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var page struct {
			Data       *[]retirementThread `json:"data"`
			NextCursor *string             `json:"nextCursor"`
		}
		if err := c.Request(ctx, "thread/list", params, &page); err != nil {
			return retirementThread{}, false, err
		}
		if page.Data == nil {
			return retirementThread{}, false, errors.New("thread/list returned no data")
		}
		for _, candidate := range *page.Data {
			if candidate.ID != threadID {
				continue
			}
			if candidate.Cwd != cwd || !portalThreadSource(candidate.Source) {
				return retirementThread{}, false,
					errors.New("thread/list returned invalid metadata for the expected retirement thread")
			}
			return candidate, true, nil
		}
		if page.NextCursor == nil || *page.NextCursor == "" {
			return retirementThread{}, false, nil
		}
		cursor = *page.NextCursor
		if _, duplicate := seenCursors[cursor]; duplicate {
			return retirementThread{}, false, errors.New("thread/list repeated a retirement cursor")
		}
		seenCursors[cursor] = struct{}{}
	}
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
	modeFromRollout := false
	if path := stringValue(metadata.Thread["path"]); path != "" {
		if mode, err := collaborationModeFromRollout(path); err == nil {
			transcript.CollaborationMode = mode
			modeFromRollout = true
		}
	}
	if !modeFromRollout {
		if settings, _, ok := c.cachedSettings(threadID); ok {
			transcript.CollaborationMode = settings.settings.CollaborationMode
		}
	}
	var page struct {
		Data *[]map[string]any `json:"data"`
	}
	if err := c.Request(ctx, "thread/turns/list", map[string]any{
		"threadId": threadID, "limit": recentTurnLimit, "sortDirection": "desc", "itemsView": "full",
	}, &page); err != nil {
		if freshThreadMissingSourceRollout(metadata.Thread, threadID, err) {
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
	return transcript, nil
}

func freshThreadMissingSourceRollout(thread map[string]any, threadID string, err error) bool {
	var rpcErr *rpcCallError
	if !errors.As(err, &rpcErr) || rpcErr.code != -32600 ||
		rpcErr.message != "invalid paginated history lineage for "+threadID+": missing source rollout" {
		return false
	}
	if stringValue(thread["id"]) != threadID || stringValue(thread["source"]) != "vscode" ||
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
		return errors.New("Codex thread does not match the development session directory")
	}
	return nil
}

func (c *Client) RequireThreadIdle(ctx context.Context, threadID, cwd string) error {
	if err := c.VerifyThread(ctx, threadID, cwd); err != nil {
		return err
	}
	if err := c.requireThreadTurnsIdle(ctx, threadID); err != nil {
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
	return c.requireSubmissionAttemptsResolved(ctx, threadID)
}

func (c *Client) requireThreadTurnsIdle(ctx context.Context, threadID string) error {
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
		entries = append(entries, TranscriptEntry{TurnID: turnID, Kind: "error", Summary: "Turn failed", Details: jsonDetails(failure)})
	} else if status := statusValue(turn["status"]); status == "failed" || status == "error" {
		entries = append(entries, TranscriptEntry{TurnID: turnID, Kind: "error", Summary: "Turn " + status})
	}
	return entries
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
	return c.Request(ctx, "thread/resume", map[string]any{"threadId": threadID, "excludeTurns": true}, &response)
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
	items, err := c.listThreadItems(ctx, threadID, nil)
	if err != nil {
		return QueueEntry{}, false, err
	}
	for _, entry := range items {
		if stringValue(entry.Item["type"]) != "userMessage" ||
			stringValue(entry.Item["clientId"]) != clientID {
			continue
		}
		content, ok := entry.Item["content"].([]any)
		if !ok {
			return QueueEntry{}, false, errors.New("started queued message has invalid content")
		}
		inputs := make([]map[string]any, 0, len(content))
		for _, value := range content {
			input, ok := value.(map[string]any)
			if !ok {
				return QueueEntry{}, false, errors.New("started queued message has invalid input")
			}
			inputs = append(inputs, input)
		}
		startedText := inputText(inputs)
		if startedText != text {
			return QueueEntry{}, false, errors.New("queued message identity was reused with different text")
		}
		return QueueEntry{
			ID: stringValue(entry.Item["id"]), Text: startedText,
			ClientUserMessageID: clientID,
		}, true, nil
	}
	return QueueEntry{}, false, nil
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
	items, err := c.listThreadItems(ctx, threadID, nil)
	if err != nil {
		return SendReceipt{}, false, err
	}
	for index, entry := range items {
		if stringValue(entry.Item["type"]) != "userMessage" ||
			stringValue(entry.Item["clientId"]) != clientID {
			continue
		}
		content, ok := entry.Item["content"].([]any)
		if !ok {
			return SendReceipt{}, false, errors.New("sent message has invalid content")
		}
		inputs := make([]map[string]any, 0, len(content))
		for _, value := range content {
			input, ok := value.(map[string]any)
			if !ok {
				return SendReceipt{}, false, errors.New("sent message has invalid input")
			}
			inputs = append(inputs, input)
		}
		if inputText(inputs) != text {
			return SendReceipt{}, false, errors.New("message identity was reused with different text")
		}
		if entry.TurnID == "" {
			return SendReceipt{}, false, errors.New("sent message has no turn identity")
		}
		steered := false
		for _, older := range items[index+1:] {
			if older.TurnID == entry.TurnID {
				steered = true
				break
			}
		}
		return SendReceipt{
			TurnID: entry.TurnID, ClientUserMessageID: clientID, Steered: steered,
		}, true, nil
	}
	return SendReceipt{}, false, nil
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
	items, err := c.listThreadItems(ctx, threadID, nil)
	if err != nil {
		return false, fmt.Errorf("inspect deleted queued message history: %w", err)
	}
	for _, entry := range items {
		if stringValue(entry.Item["type"]) != "userMessage" ||
			stringValue(entry.Item["clientId"]) != attempt.ClientUserMessageID {
			continue
		}
		content, ok := entry.Item["content"].([]any)
		if !ok {
			return false, errors.New("deleted queued message has invalid history content")
		}
		inputs := make([]map[string]any, 0, len(content))
		for _, value := range content {
			input, ok := value.(map[string]any)
			if !ok {
				return false, errors.New("deleted queued message has invalid history input")
			}
			inputs = append(inputs, input)
		}
		if queueTextDigest(inputText(inputs)) != attempt.Digest {
			return false, errors.New("deleted queued message identity was reused with different text")
		}
		return true, nil
	}
	return false, nil
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
	if !attempted {
		receipt, found, err := c.sentByClientID(
			ctx, threadID, clientUserMessageID, text,
		)
		if err != nil {
			return SendReceipt{}, err
		}
		if found {
			return receipt, nil
		}
	} else if attempt.State == "submitting" {
		receipt, found, err := c.sentByClientID(
			ctx, threadID, clientUserMessageID, text,
		)
		if err != nil {
			return SendReceipt{}, err
		}
		if found {
			receipt.Steered = attempt.Steered
			if err := c.clearSendAttempt(threadID, clientUserMessageID); err != nil {
				return SendReceipt{}, &UnknownSendOutcomeError{Err: fmt.Errorf(
					"message was accepted but its attempt record could not be cleared: %w", err,
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
	turnID, err := c.activeTurnID(ctx, threadID)
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
			if err := c.clearSendAttempt(threadID, clientUserMessageID); err != nil {
				return SendReceipt{}, &UnknownSendOutcomeError{Err: fmt.Errorf(
					"message was accepted but its attempt record could not be cleared: %w", err,
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
		return c.finishAcceptedSend(threadID, text, SendReceipt{
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
	return c.finishAcceptedSend(threadID, text, SendReceipt{
		TurnID: response.Turn.ID, ClientUserMessageID: clientUserMessageID,
	})
}

func (c *Client) finishAcceptedSend(
	threadID, text string, receipt SendReceipt,
) (SendReceipt, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	visible, found, err := c.waitForSentByClientID(
		ctx, threadID, receipt.ClientUserMessageID, text,
	)
	if err != nil || !found {
		// The RPC response proves acceptance. Keep the durable submitting record
		// until a later retry can also prove it from history.
		return receipt, nil
	}
	if visible.TurnID != receipt.TurnID {
		return SendReceipt{}, &UnknownSendOutcomeError{Err: errors.New(
			"accepted message appeared under a different turn identity",
		)}
	}
	if err := c.clearSendAttempt(threadID, receipt.ClientUserMessageID); err != nil {
		return SendReceipt{}, &UnknownSendOutcomeError{Err: fmt.Errorf(
			"message was accepted but its attempt record could not be cleared: %w", err,
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
	_, found, err := c.sendAttempt(threadID, clientUserMessageID, text, context)
	if err != nil || found {
		return found, err
	}
	_, found, err = c.sentByClientID(ctx, threadID, clientUserMessageID, text)
	return found, err
}

func (c *Client) EnsureInitialMessage(ctx context.Context, threadID, cwd, text string, allowUnmaterializedStart bool) error {
	materialized, err := c.threadHistoryMaterialized(ctx, threadID, cwd)
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
		materialized, err := c.threadHistoryMaterialized(ctx, threadID, cwd)
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

func (c *Client) RequireThreadMaterialized(ctx context.Context, threadID, cwd string) error {
	materialized, err := c.threadHistoryMaterialized(ctx, threadID, cwd)
	if err != nil {
		return err
	}
	if !materialized {
		return errors.New("recorded Codex thread has no persisted history; archive the session and start a new one")
	}
	return nil
}

func (c *Client) threadHistoryMaterialized(ctx context.Context, threadID, cwd string) (bool, error) {
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
		fresh := portalThreadSource(metadata.Thread.Source) &&
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
				"unmaterialized Codex thread is not a fresh idle portal thread "+
					"(source=%s ephemeral=%s historyMode=%q previewEmpty=%t status=%q turns=%d)",
				sessionSourceDescription(metadata.Thread.Source), ephemeral,
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

func portalThreadSource(value any) bool {
	source, ok := value.(string)
	return ok && source == "vscode"
}

func sessionSourceDescription(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "<invalid>"
	}
	return string(encoded)
}

func (c *Client) Interrupt(ctx context.Context, threadID string) error {
	turnID, err := c.activeTurnID(ctx, threadID)
	if err != nil {
		return err
	}
	if turnID == "" {
		return errors.New("thread has no active turn")
	}
	return c.Request(ctx, "turn/interrupt", map[string]any{"threadId": threadID, "turnId": turnID}, nil)
}

func (c *Client) activeTurnID(ctx context.Context, threadID string) (string, error) {
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

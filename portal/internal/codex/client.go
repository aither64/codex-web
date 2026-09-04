package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	readLimit       = 64 * 1024 * 1024
	recentTurnLimit = 20
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
}

type Prompt struct {
	ID                 string         `json:"id"`
	Method             string         `json:"method"`
	Kind               string         `json:"kind"`
	ThreadID           string         `json:"threadId"`
	ItemID             string         `json:"itemId,omitempty"`
	Params             map[string]any `json:"params"`
	Item               map[string]any `json:"item,omitempty"`
	AuthorityAvailable bool           `json:"authorityAvailable"`
	Error              string         `json:"error,omitempty"`
	AvailableDecisions []string       `json:"availableDecisions,omitempty"`
	Questions          []Question     `json:"questions,omitempty"`
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
	ThreadID string            `json:"threadId"`
	Status   string            `json:"status"`
	Entries  []TranscriptEntry `json:"entries"`
}

type TranscriptEntry struct {
	TurnID  string `json:"turnId,omitempty"`
	Kind    string `json:"kind"`
	Summary string `json:"summary,omitempty"`
	Text    string `json:"text,omitempty"`
	Details string `json:"details,omitempty"`
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
				generation: generation, connection: connection,
			}
			prompt, promptErr := normalizePrompt(request)
			if promptErr != nil {
				c.rejectUnsupported(request, promptErr)
				continue
			}
			c.pendingMu.Lock()
			c.requests[key] = request
			c.pendingMu.Unlock()
			c.broadcast(prompt.ThreadID)
			continue
		}
		if message.Method == "serverRequest/resolved" {
			c.handleResolved(message.Params, generation)
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
			call.channel <- response{err: fmt.Errorf("Codex RPC %d: %s", message.Error.Code, message.Error.Message)}
		} else {
			call.channel <- response{result: message.Result}
		}
	}
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
		go c.unsubscribeThread(threadID)
	}
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
	needsItems := false
	for _, prompt := range prompts {
		if requiresThreadItem(prompt.Kind) {
			needsItems = true
			break
		}
	}
	if !needsItems {
		return prompts, nil
	}
	items, err := c.threadItems(ctx, threadID)
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
		items, itemErr := c.threadItems(ctx, threadID)
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
		if !ok || len(answer) != 1 || len(values) != 1 || strings.TrimSpace(values[0]) == "" {
			return fmt.Errorf("question %q requires exactly one non-empty answer", question.ID)
		}
		if len(question.Options) == 0 {
			continue
		}
		offered := false
		for _, option := range question.Options {
			if option.Label == values[0] {
				offered = true
				break
			}
		}
		if !offered && !question.IsOther {
			return fmt.Errorf("answer to question %q was not offered", question.ID)
		}
	}
	return nil
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

func (c *Client) threadItems(ctx context.Context, threadID string) (map[string]map[string]any, error) {
	var page struct {
		Data *[]struct {
			TurnID string         `json:"turnId"`
			Item   map[string]any `json:"item"`
		} `json:"data"`
	}
	if err := c.Request(ctx, "thread/items/list", map[string]any{
		"threadId": threadID, "limit": 100, "sortDirection": "desc",
	}, &page); err != nil {
		return nil, err
	}
	if page.Data == nil {
		return nil, errors.New("thread/items/list returned no data")
	}
	items := make(map[string]map[string]any, len(*page.Data))
	for _, entry := range *page.Data {
		if entry.TurnID == "" || entry.Item == nil {
			return nil, errors.New("thread/items/list returned an invalid item entry")
		}
		if id := stringValue(entry.Item["id"]); id != "" {
			items[id] = entry.Item
		} else {
			return nil, errors.New("thread/items/list returned an item without an id")
		}
	}
	return items, nil
}

func stringValue(value any) string {
	result, _ := value.(string)
	return result
}

func (c *Client) StartThread(ctx context.Context, cwd string, environment map[string]string) (string, error) {
	var response struct {
		Thread struct {
			ID  string `json:"id"`
			Cwd string `json:"cwd"`
		} `json:"thread"`
	}
	params := map[string]any{
		"cwd":                   cwd,
		"runtimeWorkspaceRoots": []string{environment["VPSFREE_DEV_SESSION_WORKSPACE"]},
		"config": map[string]any{
			"shell_environment_policy": map[string]any{"set": environment},
		},
	}
	if err := c.Request(ctx, "thread/start", params, &response); err != nil {
		return "", err
	}
	if response.Thread.ID == "" || response.Thread.Cwd != cwd {
		return "", errors.New("thread/start returned no thread id or the wrong working directory")
	}
	return response.Thread.ID, nil
}

func (c *Client) ResumeThread(ctx context.Context, threadID, cwd string, environment map[string]string) (string, error) {
	var response struct {
		Thread struct {
			ID  string `json:"id"`
			Cwd string `json:"cwd"`
		} `json:"thread"`
	}
	params := map[string]any{
		"threadId":     threadID,
		"cwd":          cwd,
		"excludeTurns": true,
		"config": map[string]any{
			"shell_environment_policy": map[string]any{"set": environment},
		},
	}
	if err := c.Request(ctx, "thread/resume", params, &response); err != nil {
		return "", err
	}
	if response.Thread.ID != threadID || response.Thread.Cwd != cwd {
		return "", errors.New("thread/resume returned the wrong thread or working directory")
	}
	return response.Thread.ID, nil
}

func (c *Client) OpenThread(ctx context.Context, threadID, cwd string, environment map[string]string) (string, error) {
	if threadID != "" {
		return c.ResumeThread(ctx, threadID, cwd, environment)
	}
	return c.StartThread(ctx, cwd, environment)
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
				Source string `json:"source"`
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
			(metadata.Thread.Cwd != cwd || metadata.Thread.Source != "vscode") {
			return "", errors.New("recorded creation thread has the wrong identity")
		}
		if metadata.Thread.Cwd == cwd && metadata.Thread.Source == "vscode" {
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
		return c.ResumeThread(ctx, candidateID, cwd, environment)
	}
	return c.StartThread(ctx, cwd, environment)
}

func (c *Client) SetName(ctx context.Context, threadID, name string) error {
	return c.Request(ctx, "thread/name/set", map[string]any{"threadId": threadID, "name": name}, nil)
}

func (c *Client) ReadThread(ctx context.Context, threadID string) (Transcript, error) {
	var metadata struct {
		Thread map[string]any `json:"thread"`
	}
	if err := c.Request(ctx, "thread/read", map[string]any{"threadId": threadID}, &metadata); err != nil {
		return Transcript{}, err
	}
	if metadata.Thread == nil {
		return Transcript{}, errors.New("thread/read returned no thread")
	}
	var page struct {
		Data *[]map[string]any `json:"data"`
	}
	if err := c.Request(ctx, "thread/turns/list", map[string]any{
		"threadId": threadID, "limit": recentTurnLimit, "sortDirection": "desc", "itemsView": "full",
	}, &page); err != nil {
		return Transcript{}, err
	}
	if page.Data == nil {
		return Transcript{}, errors.New("thread/turns/list returned no data")
	}
	slices.Reverse(*page.Data)
	transcript := Transcript{
		ThreadID: stringValue(metadata.Thread["id"]),
		Status:   statusValue(metadata.Thread["status"]),
	}
	for _, turn := range *page.Data {
		transcript.Entries = append(transcript.Entries, transcriptEntries(turn)...)
	}
	return transcript, nil
}

func (c *Client) VerifyThread(ctx context.Context, threadID, cwd string) error {
	var metadata struct {
		Thread map[string]any `json:"thread"`
	}
	if err := c.Request(ctx, "thread/read", map[string]any{"threadId": threadID}, &metadata); err != nil {
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
	return c.requireThreadTurnsIdle(ctx, threadID)
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
	entries := make([]TranscriptEntry, 0)
	items, _ := turn["items"].([]any)
	for _, raw := range items {
		item, ok := raw.(map[string]any)
		if !ok {
			entries = append(entries, TranscriptEntry{TurnID: turnID, Kind: "unknown", Summary: "Unknown Codex event", Details: jsonDetails(raw)})
			continue
		}
		entry := TranscriptEntry{TurnID: turnID, Kind: stringValue(item["type"])}
		switch entry.Kind {
		case "userMessage":
			entry.Text = textContent(item["content"])
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
			entry.Summary = "Reasoning summary"
			entry.Text = strings.Join(stringValues(item["summary"]), "\n")
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

func (c *Client) Send(ctx context.Context, threadID, text string) error {
	if err := c.resumeThread(ctx, threadID); err != nil {
		return err
	}
	turnID, err := c.activeTurnID(ctx, threadID)
	if err != nil {
		return err
	}
	input := []map[string]any{{"type": "text", "text": text}}
	if turnID != "" {
		return c.Request(ctx, "turn/steer", map[string]any{"threadId": threadID, "expectedTurnId": turnID, "input": input}, nil)
	}
	return c.Request(ctx, "turn/start", map[string]any{"threadId": threadID, "input": input}, nil)
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
		return c.Request(ctx, "turn/start", map[string]any{"threadId": threadID, "input": input}, nil)
	}
	if err := c.resumeThread(ctx, threadID); err != nil {
		return err
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
		return err
	}
	if page.Data == nil {
		return errors.New("thread/turns/list returned no data")
	}
	if len(*page.Data) == 0 {
		return errors.New("materialized Codex thread has no initial turn")
	}
	var initialRequests []string
	for _, item := range (*page.Data)[0].Items {
		if item.Type != "userMessage" {
			continue
		}
		var parts []string
		for _, content := range item.Content {
			if content.Type != "text" {
				return errors.New("Codex thread has a non-text initial request")
			}
			parts = append(parts, content.Text)
		}
		initialRequests = append(initialRequests, strings.TrimSpace(strings.Join(parts, "\n")))
	}
	if len(initialRequests) == 0 {
		return errors.New("Codex thread already has a turn without an initial user request")
	}
	if len(initialRequests) != 1 || initialRequests[0] != strings.TrimSpace(text) {
		return errors.New("Codex thread already has a different initial request")
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
			Source      string            `json:"source"`
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
		fresh := metadata.Thread.Source == "vscode" &&
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
					"(source=%q ephemeral=%s historyMode=%q previewEmpty=%t status=%q turns=%d)",
				metadata.Thread.Source, ephemeral,
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

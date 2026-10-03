package codex

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"
)

// Errors deliberately contain no remote diagnostics, configuration or input.
var (
	ErrEphemeralIsolation = errors.New("ephemeral utility isolation is unavailable")
	ErrEphemeralSettings  = errors.New("ephemeral utility settings are unavailable")
	ErrEphemeralProtocol  = errors.New("ephemeral utility protocol failed")
	ErrEphemeralServer    = errors.New("ephemeral utility server failed")
)

// EphemeralInstructionFileContent is the exact private file content required by
// RunEphemeralTurn. The pinned server loads both instruction files even when
// explicit base instructions take precedence, and rejects empty files.
const EphemeralInstructionFileContent = "Preserve the current utility task and its explicit instructions.\n"

const (
	ephemeralFrameLimit  = 256 * 1024
	ephemeralEventLimit  = 256
	ephemeralByteLimit   = 1024 * 1024
	ephemeralOutputLimit = 16 * 1024
	ephemeralCleanup     = 250 * time.Millisecond
)

//go:embed ephemeral_policy.json
var ephemeralPolicyJSON []byte

// EphemeralTurnOptions contains trusted application inputs, never browser paths
// or arbitrary tool/config overrides. Directory must be empty, canonical,
// application-owned mode 0700 outside project trees. Its parent is private too.
// InstructionFile is a separate canonical regular mode-0600 file containing
// exactly EphemeralInstructionFileContent in private state outside Directory.
// The caller keeps that unique file unchanged through teardown. The helper
// creates no files.
// Global user instructions remain trusted serving-instance policy.
type EphemeralTurnOptions struct {
	Directory       string
	InstructionFile string
	Model           string
	Effort          string
	Instructions    string
	Input           string
	OutputSchema    json.RawMessage
}

type EphemeralTurnResult struct{ Text string }

// RunEphemeralTurn runs one bounded utility turn on a private connection. It
// never adopts/reconnects a thread or records a submission. Operators must pause
// naming before MCP reconfiguration; each call disables its captured MCP names.
func (c *Client) RunEphemeralTurn(ctx context.Context, opts EphemeralTurnOptions) (result EphemeralTurnResult, err error) {
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	hardDeadline, hasDeadline := ctx.Deadline()
	if !hasDeadline || c.options.ObserverOnly || !validEphemeralOptions(opts) {
		return result, ErrEphemeralIsolation
	}
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	remaining := time.Until(hardDeadline)
	reserve := ephemeralCleanup
	if remaining/10 < reserve {
		reserve = remaining / 10
	}
	inference, cancel := context.WithDeadline(ctx, hardDeadline.Add(-reserve))
	defer cancel()
	sink := &ephemeralSink{hardDeadline: hardDeadline, cancel: cancel, changed: make(chan struct{}, 1)}
	private := NewWithOptions(c.socket, ClientOptions{ClientInfo: c.options.ClientInfo})
	private.queueLedgerPath = ""
	private.ephemeral = sink
	var generation uint64
	var threadID, turnID string
	defer func() {
		// Cancellation cannot extend the original deadline. Cleanup does not run in
		// a detached goroutine and never reconnects a lost private connection.
		cleanupDeadline := time.Now().Add(ephemeralCleanup)
		if hardDeadline.Before(cleanupDeadline) {
			cleanupDeadline = hardDeadline
		}
		cleanup, stop := context.WithDeadline(context.WithoutCancel(ctx), cleanupDeadline)
		defer stop()
		if generation != 0 && threadID != "" && cleanup.Err() == nil {
			if ownedTurn := sink.cleanupTurn(generation, threadID, turnID); err != nil && ownedTurn != "" {
				_ = private.requestConnectedGeneration(cleanup, generation, "turn/interrupt", map[string]any{"threadId": threadID, "turnId": ownedTurn}, nil)
			}
			var unsubscribe struct {
				Status string `json:"status"`
			}
			cleanupErr := private.requestConnectedGeneration(cleanup, generation, "thread/unsubscribe", map[string]any{"threadId": threadID}, &unsubscribe)
			if err == nil && (cleanupErr != nil || (unsubscribe.Status != "unsubscribed" && unsubscribe.Status != "notSubscribed" && unsubscribe.Status != "notLoaded")) {
				result, err = EphemeralTurnResult{}, ErrEphemeralServer
			}
		}
		if err == nil {
			if failed := sink.failure(); failed != nil {
				result, err = EphemeralTurnResult{}, failed
			}
		}
		private.Close()
		if ctx.Err() != nil {
			result, err = EphemeralTurnResult{}, ctx.Err()
		}
	}()
	failure := func(category error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if failed := sink.failure(); failed != nil {
			return failed
		}
		if inference.Err() != nil {
			return context.DeadlineExceeded
		}
		return category
	}
	if private.Ensure(inference) != nil {
		return result, failure(ErrEphemeralServer)
	}
	private.connectionMu.Lock()
	generation = private.generation
	private.connectionMu.Unlock()
	var configRaw json.RawMessage
	if private.requestConnectedGeneration(inference, generation, "config/read", map[string]any{"cwd": opts.Directory, "includeLayers": false}, &configRaw) != nil {
		return result, failure(ErrEphemeralIsolation)
	}
	policy, policyErr := ephemeralRestrictions(configRaw, opts.InstructionFile)
	if policyErr != nil {
		return result, policyErr
	}
	var requirements json.RawMessage
	if private.requestConnectedGeneration(inference, generation, "configRequirements/read", nil, &requirements) != nil {
		return result, failure(ErrEphemeralIsolation)
	}
	if !ephemeralRequirementsAllow(requirements, policy) {
		return result, ErrEphemeralIsolation
	}
	cursor := ""
	seen := map[string]bool{}
	available := false
	for page := 0; page < 8; page++ {
		params := map[string]any{"limit": 100, "includeHidden": true}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var raw json.RawMessage
		if private.requestConnectedGeneration(inference, generation, "model/list", params, &raw) != nil {
			return result, failure(ErrEphemeralSettings)
		}
		data, ok := ephemeralObject(raw)
		if !ok {
			return result, ErrEphemeralSettings
		}
		models, ok := data["data"].([]any)
		if !ok || len(models) > 100 {
			return result, ErrEphemeralSettings
		}
		for _, entry := range models {
			model, ok := entry.(map[string]any)
			if !ok {
				return result, ErrEphemeralSettings
			}
			name, ok := model["model"].(string)
			if !ok || name == "" {
				return result, ErrEphemeralSettings
			}
			if name != opts.Model {
				continue
			}
			efforts, ok := model["supportedReasoningEfforts"].([]any)
			if !ok {
				return result, ErrEphemeralSettings
			}
			for _, entry := range efforts {
				effort, ok := entry.(map[string]any)
				if !ok {
					return result, ErrEphemeralSettings
				}
				value, ok := effort["reasoningEffort"].(string)
				if !ok {
					return result, ErrEphemeralSettings
				}
				if value == opts.Effort {
					available = true
				}
			}
		}
		if available {
			break
		}
		next, present := data["nextCursor"]
		if !present || next == nil {
			break
		}
		var valid bool
		cursor, valid = next.(string)
		if !valid || cursor == "" || len(cursor) > 1024 || seen[cursor] {
			return result, ErrEphemeralSettings
		}
		seen[cursor] = true
	}
	if !available {
		return result, ErrEphemeralSettings
	}
	var started json.RawMessage
	if private.requestConnectedGeneration(inference, generation, "thread/start", map[string]any{
		"ephemeral": true, "model": opts.Model, "allowProviderModelFallback": false,
		"cwd": opts.Directory, "baseInstructions": opts.Instructions, "developerInstructions": "",
		"approvalPolicy": "never", "sandbox": "read-only", "environments": []any{},
		"runtimeWorkspaceRoots": []string{}, "selectedCapabilityRoots": []string{}, "dynamicTools": []any{}, "config": policy,
	}, &started) != nil {
		return result, failure(ErrEphemeralServer)
	}
	start, ok := ephemeralObject(started)
	if !ok {
		return result, ErrEphemeralProtocol
	}
	thread, ok := start["thread"].(map[string]any)
	if !ok {
		return result, ErrEphemeralProtocol
	}
	// A syntactically known ID permits unsubscribe, never adoption or resume.
	threadID, _ = thread["id"].(string)
	if !ephemeralID(threadID) {
		threadID = ""
		return result, ErrEphemeralProtocol
	}
	sandbox, sandboxOK := start["sandbox"].(map[string]any)
	if start["model"] != opts.Model || start["cwd"] != opts.Directory || thread["cwd"] != opts.Directory ||
		thread["ephemeral"] != true || thread["path"] != nil ||
		start["approvalPolicy"] != "never" || !sandboxOK || sandbox["type"] != "readOnly" || (sandbox["networkAccess"] != nil && sandbox["networkAccess"] != false) ||
		!ephemeralEmptyArrays(start, "runtimeWorkspaceRoots") || !ephemeralEmptyArrays(thread, "environments") {
		return result, ErrEphemeralIsolation
	}
	if model, present := thread["model"]; present && model != nil && model != opts.Model {
		return result, ErrEphemeralSettings
	}
	if !sink.bind(generation, threadID, "") {
		return result, failure(ErrEphemeralProtocol)
	}
	if !sink.beginTurn(generation, threadID) {
		return result, failure(ErrEphemeralProtocol)
	}
	var turnRaw json.RawMessage
	if private.requestConnectedGeneration(inference, generation, "turn/start", map[string]any{
		"threadId": threadID, "input": []any{map[string]any{"type": "text", "text": opts.Input}},
		"model": opts.Model, "effort": opts.Effort, "outputSchema": opts.OutputSchema, "cwd": opts.Directory,
		"approvalPolicy": "never", "sandboxPolicy": map[string]any{"type": "readOnly", "networkAccess": false},
		"environments": []any{}, "runtimeWorkspaceRoots": []string{},
	}, &turnRaw) != nil {
		return result, failure(ErrEphemeralServer)
	}
	response, ok := ephemeralObject(turnRaw)
	if !ok {
		return result, ErrEphemeralProtocol
	}
	turn, ok := response["turn"].(map[string]any)
	if !ok {
		return result, ErrEphemeralProtocol
	}
	turnID, _ = turn["id"].(string)
	if !ephemeralID(turnID) {
		turnID = ""
		return result, ErrEphemeralProtocol
	}
	status, _ := turn["status"].(string)
	if status != "inProgress" && status != "completed" {
		return result, ErrEphemeralServer
	}
	if _, ok := turn["items"].([]any); !ok {
		return result, ErrEphemeralProtocol
	}
	if !sink.bind(generation, threadID, turnID) {
		return result, failure(ErrEphemeralProtocol)
	}
	for {
		result, done, failed := sink.result()
		if failed != nil {
			return EphemeralTurnResult{}, failed
		}
		if done {
			return result, nil
		}
		select {
		case <-sink.changed:
		case <-inference.Done():
			return EphemeralTurnResult{}, failure(ErrEphemeralProtocol)
		}
	}
}

func ephemeralID(id string) bool { return id != "" && len(id) <= 256 && utf8.ValidString(id) }

func validEphemeralOptions(opts EphemeralTurnOptions) bool {
	if strings.TrimSpace(opts.Model) == "" || strings.TrimSpace(opts.Effort) == "" || !ephemeralID(opts.Model) || !ephemeralID(opts.Effort) || strings.TrimSpace(opts.Instructions) == "" ||
		len(opts.Instructions) > 16*1024 || len(opts.Input) > 64*1024 || !utf8.ValidString(opts.Instructions) || !utf8.ValidString(opts.Input) ||
		len(opts.OutputSchema) == 0 || len(opts.OutputSchema) > 16*1024 {
		return false
	}
	if _, ok := ephemeralObject(opts.OutputSchema); !ok {
		return false
	}
	if !privateEphemeralPath(opts.Directory, true) || !privateEphemeralPath(filepath.Dir(opts.Directory), true) {
		return false
	}
	entries, err := os.ReadDir(opts.Directory)
	if err != nil || len(entries) != 0 {
		return false
	}
	if !privateEphemeralPath(filepath.Dir(opts.InstructionFile), true) {
		return false
	}
	relative, err := filepath.Rel(opts.Directory, opts.InstructionFile)
	if err != nil || (relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator))) {
		return false
	}
	return privateEphemeralPath(opts.InstructionFile, false)
}

func privateEphemeralPath(path string, directory bool) bool {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return false
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return false
	}
	info, err := os.Lstat(path)
	if err != nil {
		return false
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != uint32(os.Geteuid()) {
		return false
	}
	if directory {
		return info.IsDir() && info.Mode().Perm() == 0700
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() != int64(len(EphemeralInstructionFileContent)) || owner.Nlink != 1 {
		return false
	}
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	content, err := io.ReadAll(io.LimitReader(file, int64(len(EphemeralInstructionFileContent)+1)))
	return err == nil && string(content) == EphemeralInstructionFileContent
}

func ephemeralEmptyArrays(object map[string]any, keys ...string) bool {
	for _, key := range keys {
		value, present := object[key]
		if !present {
			continue
		} // optional protocol property; explicit request is required
		list, ok := value.([]any)
		if !ok || len(list) != 0 {
			return false
		}
	}
	return true
}

func ephemeralRestrictions(raw json.RawMessage, instructionFile string) (map[string]any, error) {
	response, ok := ephemeralObject(raw)
	if !ok {
		return nil, ErrEphemeralIsolation
	}
	config, ok := response["config"].(map[string]any)
	if !ok {
		return nil, ErrEphemeralIsolation
	}
	if _, ok := response["origins"].(map[string]any); !ok {
		return nil, ErrEphemeralIsolation
	}
	policy, _ := ephemeralObject(ephemeralPolicyJSON)
	policy["model_instructions_file"] = instructionFile
	policy["experimental_compact_prompt_file"] = instructionFile
	servers := map[string]any{}
	if value, present := config["mcp_servers"]; present {
		inventory, ok := value.(map[string]any)
		if !ok || len(inventory) > 128 {
			return nil, ErrEphemeralIsolation
		}
		for name, settings := range inventory {
			if !ephemeralID(name) {
				return nil, ErrEphemeralIsolation
			}
			if _, ok := settings.(map[string]any); !ok {
				return nil, ErrEphemeralIsolation
			}
			servers[name] = map[string]any{"enabled": false}
		}
	}
	policy["mcp_servers"] = servers
	return policy, nil
}

func ephemeralRequirementsAllow(raw json.RawMessage, policy map[string]any) bool {
	response, ok := ephemeralObject(raw)
	if !ok {
		return false
	}
	value, present := response["requirements"]
	if !present {
		return false
	}
	if value == nil {
		return true
	}
	requirements, ok := value.(map[string]any)
	if !ok {
		return false
	}
	for key, wanted := range map[string]string{"allowedApprovalPolicies": "never", "allowedSandboxModes": "read-only", "allowedWebSearchModes": "disabled"} {
		value, present := requirements[key]
		if !present || value == nil {
			continue
		}
		choices, ok := value.([]any)
		if !ok {
			return false
		}
		found := false
		for _, choice := range choices {
			if _, ok := choice.(string); !ok {
				return false
			}
			if choice == wanted {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	if value := requirements["additionalDeveloperInstructions"]; value != nil && value != "" {
		return false
	}
	if value := requirements["hooks"]; value != nil {
		return false
	}
	if value := requirements["featureRequirements"]; value != nil {
		features, ok := value.(map[string]any)
		if !ok {
			return false
		}
		for name, value := range features {
			enabled, ok := value.(bool)
			if !ok {
				return false
			}
			if enforced, present := policy["features."+name]; present && enabled != enforced {
				return false
			}
			// Unknown required enabled capabilities cannot be silently accepted.
			if enabled {
				return false
			}
		}
	}
	return true
}

// Decode JSON with bounded depth/node count and no duplicate properties. The
// transport frame limit bounds bytes; this also protects schemas and config.
func ephemeralObject(raw []byte) (map[string]any, bool) {
	if len(raw) > ephemeralFrameLimit || !utf8.Valid(raw) {
		return nil, false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	nodes := 0
	var walk func(int) (any, error)
	walk = func(depth int) (any, error) {
		nodes++
		if depth > 32 || nodes > 8192 {
			return nil, ErrEphemeralProtocol
		}
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		delim, container := token.(json.Delim)
		if !container {
			return token, nil
		}
		switch delim {
		case '{':
			object := map[string]any{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, ErrEphemeralProtocol
				}
				if _, exists := object[key]; exists {
					return nil, ErrEphemeralProtocol
				}
				value, err := walk(depth + 1)
				if err != nil {
					return nil, err
				}
				object[key] = value
			}
			closing, err := decoder.Token()
			if err != nil || closing != json.Delim('}') {
				return nil, ErrEphemeralProtocol
			}
			return object, nil
		case '[':
			list := []any{}
			for decoder.More() {
				value, err := walk(depth + 1)
				if err != nil {
					return nil, err
				}
				list = append(list, value)
			}
			closing, err := decoder.Token()
			if err != nil || closing != json.Delim(']') {
				return nil, ErrEphemeralProtocol
			}
			return list, nil
		}
		return nil, ErrEphemeralProtocol
	}
	value, err := walk(0)
	if err != nil {
		return nil, false
	}
	if _, err = decoder.Token(); err != io.EOF {
		return nil, false
	}
	object, ok := value.(map[string]any)
	return object, ok
}

type ephemeralSink struct {
	mu                 sync.Mutex
	hardDeadline       time.Time
	cancel             context.CancelFunc
	changed            chan struct{}
	err                error
	generation         uint64
	threadID, turnID   string
	cleanupTurnID      string
	turnIssued         bool
	cleanupAmbiguous   bool
	events             []rpcMessage
	eventCount, bytes  int
	finalID, finalText string
	completed          bool
}

func (s *ephemeralSink) fail(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
		s.cancel()
	}
	s.mu.Unlock()
	s.signal()
}
func (s *ephemeralSink) failure() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }
func (s *ephemeralSink) signal() {
	select {
	case s.changed <- struct{}{}:
	default:
	}
}
func (s *ephemeralSink) decode(raw []byte, message *rpcMessage) bool {
	object, ok := ephemeralObject(raw)
	s.mu.Lock()
	s.bytes += len(raw)
	s.eventCount++
	bounded := s.bytes <= ephemeralByteLimit && s.eventCount <= ephemeralEventLimit
	s.mu.Unlock()
	if !ok || !bounded || json.Unmarshal(raw, message) != nil {
		s.fail(ErrEphemeralProtocol)
		return false
	}
	if message.Method != "" {
		if _, ok := ephemeralObject(message.Params); !ok {
			s.fail(ErrEphemeralProtocol)
			return false
		}
		if _, ok := object["method"].(string); !ok || len(message.Result) != 0 || message.Error != nil || len(message.Params) == 0 {
			s.fail(ErrEphemeralProtocol)
			return false
		}
	} else if len(message.ID) == 0 || ((len(message.Result) == 0) == (message.Error == nil)) {
		s.fail(ErrEphemeralProtocol)
		return false
	}
	if len(message.ID) > 0 {
		switch value := object["id"].(type) {
		case string:
			if value == "" || len(value) > 256 {
				s.fail(ErrEphemeralProtocol)
				return false
			}
		case json.Number:
			if _, err := value.Int64(); err != nil {
				s.fail(ErrEphemeralProtocol)
				return false
			}
		default:
			s.fail(ErrEphemeralProtocol)
			return false
		}
	}
	return true
}

// This hook precedes all ordinary request admission, activity and UI handling.
func (s *ephemeralSink) handle(c *Client, connection *websocket.Conn, generation uint64, message rpcMessage) bool {
	s.mu.Lock()
	if s.generation == 0 {
		s.generation = generation
	}
	valid := s.generation == generation
	if !valid {
		s.cleanupAmbiguous = true
	}
	s.mu.Unlock()
	if !valid {
		s.fail(ErrEphemeralProtocol)
		return true
	}
	if message.Method == "" {
		return false
	} // generation-bound low-level response delivery
	if len(message.ID) > 0 {
		text := "Server requests are unavailable for ephemeral utility turns"
		if message.Method == "item/tool/requestUserInput" {
			text = "Questions are unavailable for ephemeral utility turns"
		}
		deadline := time.Now().Add(ephemeralCleanup)
		if s.hardDeadline.Before(deadline) {
			deadline = s.hardDeadline
		}
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		raw, _ := json.Marshal(map[string]any{"id": message.ID, "error": map[string]any{"code": -32601, "message": text}})
		_ = c.writeOn(ctx, connection, generation, raw)
		cancel()
		s.fail(ErrEphemeralIsolation)
		return true
	}
	if message.Method == "error" {
		s.fail(ErrEphemeralServer)
		return true
	}
	// One issued turn on this new private thread may identify itself before its
	// RPC response. Such an ID authorizes interruption only, never success.
	if message.Method == "turn/started" || message.Method == "turn/completed" || message.Method == "item/started" || message.Method == "item/completed" {
		s.mu.Lock()
		s.noteCleanupIdentityLocked(message)
		failed := s.err != nil
		s.mu.Unlock()
		if failed {
			s.signal()
			return true
		}
	}
	// Metadata-driven async questions have no JSON-RPC request ID. Reject their
	// started/completed item immediately, even before start responses are known.
	if message.Method == "item/started" || message.Method == "item/completed" {
		params, ok := ephemeralObject(message.Params)
		if !ok {
			s.fail(ErrEphemeralProtocol)
			return true
		}
		item, ok := params["item"].(map[string]any)
		if !ok {
			s.fail(ErrEphemeralProtocol)
			return true
		}
		if item["delivery"] == "async" || item["questions"] != nil {
			s.fail(ErrEphemeralIsolation)
			return true
		}
	}
	if message.Method != "item/completed" && message.Method != "turn/completed" {
		return true
	}
	s.mu.Lock()
	if s.err == nil {
		if s.threadID == "" || s.turnID == "" {
			if len(s.events) >= 32 {
				s.err = ErrEphemeralProtocol
				s.cancel()
			} else {
				s.events = append(s.events, message)
			}
		} else {
			s.consumeLocked(message)
		}
	}
	s.mu.Unlock()
	s.signal()
	return true
}

func (s *ephemeralSink) bind(generation uint64, threadID, turnID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.generation != generation || (s.threadID != "" && s.threadID != threadID) || (s.turnID != "" && s.turnID != turnID) {
		s.cleanupAmbiguous = true
		s.err = ErrEphemeralProtocol
		s.cancel()
		return false
	}
	if turnID != "" && s.cleanupTurnID != "" && s.cleanupTurnID != turnID {
		s.cleanupAmbiguous = true
		s.err = ErrEphemeralProtocol
		s.cancel()
		return false
	}
	s.threadID, s.turnID = threadID, turnID
	if turnID != "" {
		for _, message := range s.events {
			s.consumeLocked(message)
		}
		s.events = nil
	}
	return s.err == nil
}

func (s *ephemeralSink) beginTurn(generation uint64, threadID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil || s.generation != generation || s.threadID != threadID || s.turnIssued {
		return false
	}
	s.turnIssued = true
	return true
}

func (s *ephemeralSink) cleanupTurn(generation uint64, threadID, responseTurnID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cleanupAmbiguous || !s.turnIssued || s.generation != generation || s.threadID != threadID {
		return ""
	}
	if responseTurnID != "" {
		if s.cleanupTurnID != "" && s.cleanupTurnID != responseTurnID {
			return ""
		}
		return responseTurnID
	}
	return s.cleanupTurnID
}

func (s *ephemeralSink) noteCleanupIdentityLocked(message rpcMessage) {
	if !s.turnIssued {
		return
	}
	params, ok := ephemeralObject(message.Params)
	var turnID string
	valid := ok && params["threadId"] == s.threadID
	if message.Method == "turn/started" || message.Method == "turn/completed" {
		turn, ok := params["turn"].(map[string]any)
		turnID, _ = turn["id"].(string)
		_, itemsOK := turn["items"].([]any)
		valid = valid && ok && itemsOK
		if message.Method == "turn/started" {
			valid = valid && turn["status"] == "inProgress"
		}
	} else {
		turnID, _ = params["turnId"].(string)
		item, ok := params["item"].(map[string]any)
		itemID, _ := item["id"].(string)
		kind, _ := item["type"].(string)
		valid = valid && ok && ephemeralID(itemID) && ephemeralID(kind)
	}
	if !valid || !ephemeralID(turnID) || (s.cleanupTurnID != "" && s.cleanupTurnID != turnID) || (s.turnID != "" && s.turnID != turnID) {
		s.cleanupAmbiguous = true
		if s.err == nil {
			s.err = ErrEphemeralProtocol
			s.cancel()
		}
		return
	}
	s.cleanupTurnID = turnID
}

func (s *ephemeralSink) consumeLocked(message rpcMessage) {
	if s.err != nil {
		return
	}
	params, ok := ephemeralObject(message.Params)
	bad := func(category error) { s.err = category; s.cancel() }
	if !ok || params["threadId"] != s.threadID {
		bad(ErrEphemeralProtocol)
		return
	}
	if message.Method == "item/completed" {
		if params["turnId"] != s.turnID {
			bad(ErrEphemeralProtocol)
			return
		}
		item, ok := params["item"].(map[string]any)
		if !ok {
			bad(ErrEphemeralProtocol)
			return
		}
		s.itemLocked(item)
	} else {
		turn, ok := params["turn"].(map[string]any)
		if !ok || turn["id"] != s.turnID {
			bad(ErrEphemeralProtocol)
			return
		}
		if turn["status"] != "completed" {
			bad(ErrEphemeralServer)
			return
		}
		items, ok := turn["items"].([]any)
		if !ok {
			bad(ErrEphemeralProtocol)
			return
		}
		for _, value := range items {
			item, ok := value.(map[string]any)
			if !ok {
				bad(ErrEphemeralProtocol)
				return
			}
			s.itemLocked(item)
		}
		if s.completed {
			bad(ErrEphemeralProtocol)
			return
		}
		s.completed = true
	}
}
func (s *ephemeralSink) itemLocked(item map[string]any) {
	if s.err != nil || item["type"] != "agentMessage" {
		return
	}
	if item["delivery"] == "async" || item["questions"] != nil {
		s.err = ErrEphemeralIsolation
		s.cancel()
		return
	}
	if item["phase"] == "commentary" {
		return
	}
	id, idOK := item["id"].(string)
	text, textOK := item["text"].(string)
	if item["phase"] != "final_answer" || !idOK || !ephemeralID(id) || !textOK || text == "" || len(text) > ephemeralOutputLimit || !utf8.ValidString(text) ||
		(s.finalID != "" && (s.finalID != id || s.finalText != text)) {
		s.err = ErrEphemeralProtocol
		s.cancel()
		return
	}
	s.finalID, s.finalText = id, text
}
func (s *ephemeralSink) result() (EphemeralTurnResult, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return EphemeralTurnResult{Text: s.finalText}, s.completed && s.finalID != "", s.err
}

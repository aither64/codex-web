package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
)

// TurnContext is an exact native Codex 0.163.0 rollout projection, available
// only to trusted Go callers. It does not prove provider, instructions or MCP
// authority and never exposes arbitrary context or developer instruction text.
type TurnContext struct {
	ThreadID          string
	TurnID            string
	Cwd               string
	Model             string
	ReasoningEffort   string
	CollaborationMode string
	ApprovalPolicy    json.RawMessage
	SandboxPolicy     json.RawMessage
	ApprovalsReviewer string
	PermissionProfile json.RawMessage
}

func (c *Client) ReadTurnContext(ctx context.Context, threadID, turnID string) (TurnContext, error) {
	metadata, err := c.ReadThreadMetadata(ctx, threadID, true)
	if err != nil {
		return TurnContext{}, err
	}
	if metadata.Path == nil || turnID == "" {
		return TurnContext{}, errors.New("exact turn context is unavailable")
	}
	return readExactTurnContext(ctx, *metadata.Path, threadID, turnID, metadata.Cwd)
}

func readExactTurnContext(ctx context.Context, path, threadID, turnID, cwd string) (TurnContext, error) {
	if cwd == "" || !filepath.IsAbs(cwd) || filepath.Clean(cwd) != cwd {
		return TurnContext{}, errors.New("invalid trusted turn directory")
	}
	file, before, err := openRolloutFile(path)
	if err != nil {
		return TurnContext{}, err
	}
	defer file.Close()
	// Full snapshot, bounded records and memory, with cancellation on each read.
	// A long turn can put its context outside the display metadata's fixed tail.
	scanner := rolloutRecordScanner(pageContextReader{ctx: ctx, reader: io.LimitReader(file, before.Size())}, true)
	matchedSession := false
	var found *TurnContext
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return TurnContext{}, err
		}
		var record struct {
			Type    string          `json:"type"`
			Payload json.RawMessage `json:"payload"`
		}
		if json.Unmarshal(scanner.Bytes(), &record) != nil {
			return TurnContext{}, errors.New("invalid native rollout record")
		}
		if record.Type == "session_meta" {
			var session struct{ ID, Cwd string }
			if json.Unmarshal(record.Payload, &session) != nil {
				return TurnContext{}, errors.New("invalid native session metadata")
			}
			if session.ID == threadID && session.Cwd == cwd {
				matchedSession = true
			} else if matchedSession {
				return TurnContext{}, errors.New("native rollout changed session identity")
			}
			continue
		}
		if record.Type != "turn_context" || !matchedSession {
			continue
		}
		var identity struct {
			TurnID string `json:"turn_id"`
		}
		if json.Unmarshal(record.Payload, &identity) != nil {
			return TurnContext{}, errors.New("invalid native turn context identity")
		}
		if identity.TurnID != turnID {
			continue
		}
		value, err := projectTurnContext(record.Payload, threadID, turnID, cwd)
		if err != nil {
			return TurnContext{}, err
		}
		if found != nil && !reflect.DeepEqual(*found, value) {
			return TurnContext{}, errors.New("conflicting exact native turn contexts")
		}
		found = &value
	}
	if err := scanner.Err(); err != nil {
		return TurnContext{}, err
	}
	after, err := os.Stat(path)
	if err != nil {
		return TurnContext{}, err
	}
	if !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return TurnContext{}, errors.New("native rollout changed during context read")
	}
	if !matchedSession || found == nil {
		return TurnContext{}, errors.New("exact native turn context is unavailable")
	}
	return *found, nil
}

func completeRolloutRecord(data []byte, atEOF bool) (int, []byte, error) {
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, data[:i], nil
	}
	if atEOF && len(data) > 0 {
		return 0, nil, errors.New("native rollout has a partial final record")
	}
	return 0, nil, nil
}

func projectTurnContext(raw json.RawMessage, threadID, turnID, cwd string) (TurnContext, error) {
	var payload struct {
		Cwd    string `json:"cwd"`
		Model  string `json:"model"`
		Effort string `json:"effort"`
		Mode   struct {
			Mode string `json:"mode"`
		} `json:"collaboration_mode"`
		Approval json.RawMessage `json:"approval_policy"`
		Sandbox  json.RawMessage `json:"sandbox_policy"`
		Reviewer string          `json:"approvals_reviewer"`
		Profile  json.RawMessage `json:"permission_profile"`
	}
	if json.Unmarshal(raw, &payload) != nil || payload.Cwd != cwd || payload.Model == "" || len(payload.Model) > 4096 ||
		!slices.Contains([]string{"none", "minimal", "low", "medium", "high", "xhigh", "max"}, payload.Effort) ||
		!slices.Contains([]string{"default", "plan"}, payload.Mode.Mode) ||
		!slices.Contains([]string{"user", "auto_review", "guardian_subagent"}, payload.Reviewer) {
		return TurnContext{}, errors.New("incomplete or unsupported native turn context")
	}
	approval, err := validatedNativeApproval(payload.Approval)
	if err != nil {
		return TurnContext{}, err
	}
	sandbox, err := validatedNativeSandbox(payload.Sandbox)
	if err != nil {
		return TurnContext{}, err
	}
	profile, err := validatedNativePermissionProfile(payload.Profile)
	if err != nil {
		return TurnContext{}, err
	}

	return TurnContext{ThreadID: threadID, TurnID: turnID, Cwd: cwd, Model: payload.Model, ReasoningEffort: payload.Effort, CollaborationMode: payload.Mode.Mode, ApprovalPolicy: approval, SandboxPolicy: sandbox, ApprovalsReviewer: payload.Reviewer, PermissionProfile: profile}, nil
}

func validatedNativeApproval(raw json.RawMessage) (json.RawMessage, error) {
	var scalar string
	if json.Unmarshal(raw, &scalar) == nil && slices.Contains([]string{"untrusted", "on-request", "never"}, scalar) {
		return json.Marshal(scalar)
	}
	var value map[string]map[string]json.RawMessage
	if json.Unmarshal(raw, &value) != nil || len(value) != 1 || value["granular"] == nil {
		return nil, errors.New("unsupported native approval policy")
	}
	flags := value["granular"]
	for _, key := range []string{"mcp_elicitations", "rules", "sandbox_approval"} {
		if _, ok := flags[key]; !ok {
			return nil, errors.New("incomplete native approval policy")
		}
	}
	for key, flag := range flags {
		if !nativeBool(flag) || !slices.Contains([]string{"mcp_elicitations", "rules", "sandbox_approval", "request_permissions", "skill_approval"}, key) {
			return nil, errors.New("unsupported native approval policy")
		}
	}
	return json.Marshal(value)
}

func validatedNativeSandbox(raw json.RawMessage) (json.RawMessage, error) {
	var value map[string]json.RawMessage
	if json.Unmarshal(raw, &value) != nil {
		return nil, errors.New("unsupported native sandbox policy")
	}
	var kind string
	if json.Unmarshal(value["type"], &kind) != nil {
		return nil, errors.New("unsupported native sandbox policy")
	}
	allowed := []string{"type"}
	switch kind {
	case "danger-full-access":
	case "read-only":
		allowed = append(allowed, "network_access")
	case "external-sandbox":
		allowed = append(allowed, "network_access")
	case "workspace-write":
		allowed = append(allowed, "writable_roots", "network_access", "exclude_slash_tmp", "exclude_tmpdir_env_var")
	default:
		return nil, errors.New("unsupported native sandbox policy")
	}
	for key, field := range value {
		if !slices.Contains(allowed, key) {
			return nil, errors.New("unsupported native sandbox policy field")
		}
		switch key {
		case "network_access":
			if kind == "external-sandbox" {
				var s string
				if json.Unmarshal(field, &s) != nil || !slices.Contains([]string{"restricted", "enabled"}, s) {
					return nil, errors.New("invalid native network policy")
				}
			} else {
				if !nativeBool(field) {
					return nil, errors.New("invalid native network policy")
				}
			}
		case "exclude_slash_tmp", "exclude_tmpdir_env_var":
			if !nativeBool(field) {
				return nil, errors.New("invalid native sandbox flag")
			}
		case "writable_roots":
			var roots []string
			if json.Unmarshal(field, &roots) != nil || roots == nil || len(roots) > 256 {
				return nil, errors.New("invalid native sandbox roots")
			}
			for _, root := range roots {
				if !filepath.IsAbs(root) || filepath.Clean(root) != root {
					return nil, errors.New("invalid native sandbox root")
				}
			}
		}
	}
	return json.Marshal(value)
}

func validatedNativePermissionProfile(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}
	if len(raw) > 256*1024 {
		return nil, errors.New("native permission profile exceeds limit")
	}
	var value map[string]json.RawMessage
	if json.Unmarshal(raw, &value) != nil {
		return nil, errors.New("invalid native permission profile")
	}
	var kind string
	if json.Unmarshal(value["type"], &kind) != nil {
		return nil, errors.New("invalid native permission profile type")
	}
	if kind == "disabled" && len(value) == 1 {
		return json.Marshal(value)
	}
	if kind != "managed" || len(value) != 3 {
		return nil, errors.New("unsupported native permission profile")
	}
	var network string
	if json.Unmarshal(value["network"], &network) != nil || !slices.Contains([]string{"restricted", "enabled"}, network) {
		return nil, errors.New("invalid native permission network policy")
	}
	var fs map[string]json.RawMessage
	if json.Unmarshal(value["file_system"], &fs) != nil {
		return nil, errors.New("invalid native filesystem policy")
	}
	var fsKind string
	if json.Unmarshal(fs["type"], &fsKind) != nil {
		return nil, errors.New("invalid native filesystem policy type")
	}
	if fsKind == "unrestricted" && len(fs) == 1 {
		return json.Marshal(value)
	}
	if fsKind != "restricted" || len(fs) != 2 {
		return nil, errors.New("unsupported native filesystem policy")
	}
	var entries []map[string]json.RawMessage
	if json.Unmarshal(fs["entries"], &entries) != nil || entries == nil || len(entries) > 256 {
		return nil, errors.New("invalid native filesystem entries")
	}
	for _, entry := range entries {
		if len(entry) < 2 || len(entry) > 3 {
			return nil, errors.New("invalid native filesystem entry")
		}
		for key := range entry {
			if !slices.Contains([]string{"path", "access", "missing_path_behavior"}, key) {
				return nil, errors.New("unsupported native filesystem entry")
			}
		}
		var access string
		if json.Unmarshal(entry["access"], &access) != nil || !slices.Contains([]string{"read", "write"}, access) {
			return nil, errors.New("invalid native filesystem access")
		}
		if missing, ok := entry["missing_path_behavior"]; ok {
			var policy string
			if json.Unmarshal(missing, &policy) != nil || policy != "skip" {
				return nil, errors.New("unsupported native missing-path policy")
			}
		}
		var path map[string]json.RawMessage
		if json.Unmarshal(entry["path"], &path) != nil || len(path) != 2 {
			return nil, errors.New("invalid native permission path")
		}
		var pathKind string
		if json.Unmarshal(path["type"], &pathKind) != nil {
			return nil, errors.New("invalid native permission path type")
		}
		switch pathKind {
		case "path":
			var absolute string
			if json.Unmarshal(path["path"], &absolute) != nil || len(absolute) > 4096 || !filepath.IsAbs(absolute) || filepath.Clean(absolute) != absolute {
				return nil, errors.New("invalid native absolute permission path")
			}
		case "special":
			var special map[string]string
			if json.Unmarshal(path["value"], &special) != nil || len(special) != 1 || !slices.Contains([]string{"root", "slash_tmp", "tmpdir"}, special["kind"]) {
				return nil, errors.New("unsupported native special permission path")
			}
		default:
			return nil, errors.New("unsupported native permission path type")
		}
	}
	return json.Marshal(value)
}

func nativeBool(raw json.RawMessage) bool {
	return bytes.Equal(raw, []byte("true")) || bytes.Equal(raw, []byte("false"))
}

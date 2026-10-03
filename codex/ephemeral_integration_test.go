//go:build codex_integration

package codex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/iotest"
	"time"

	"github.com/coder/websocket"
)

// This entry point is intentionally absent from quick checks. Missing exact
// candidate/hash, Linux namespace/firewall tools or schema/source prerequisites
// FAIL the explicit invocation. No installed serving instance is contacted.
// Native builtin OpenAI HTTP-fallback coverage exercises HTTP/SSE after a real
// upgrade refusal. It does not cover successful WebSocket framing, continuation
// caching, reconnects or hostile delivery specifically over WebSockets.
func TestEphemeralProtocolIntegration(t *testing.T) {
	binary := integrationExecutable(t, "CODEX_EPHEMERAL_TEST_BINARY")
	expected := os.Getenv("CODEX_EPHEMERAL_TEST_SHA256")
	file, err := os.Open(binary)
	if err != nil {
		t.Fatal("candidate binary unavailable")
	}
	digest := sha256.New()
	_, hashErr := io.Copy(digest, file)
	_ = file.Close()
	if hashErr != nil {
		t.Fatal("candidate binary hash failed")
	}
	if len(expected) != 64 || hex.EncodeToString(digest.Sum(nil)) != expected {
		t.Fatal("candidate differs from reviewed CODEX_EPHEMERAL_TEST_SHA256")
	}
	source := os.Getenv("CODEX_EPHEMERAL_TEST_SOURCE")
	if !filepath.IsAbs(source) {
		t.Fatal("CODEX_EPHEMERAL_TEST_SOURCE must name the exact candidate codex-rs source")
	}
	for _, path := range []string{"core/config.schema.json", "models-manager/models.json"} {
		if _, err := os.Stat(filepath.Join(source, path)); err != nil {
			t.Fatalf("candidate source prerequisite missing: %s", path)
		}
	}
	metadata, err := os.ReadFile(filepath.Join(source, "models-manager/models.json"))
	if err != nil || len(metadata) > 4*1024*1024 {
		t.Fatal("candidate model metadata unavailable")
	}
	var catalog struct {
		Models []struct {
			Slug    string   `json:"slug"`
			Tools   []string `json:"experimental_supported_tools"`
			Efforts []struct {
				Effort string `json:"effort"`
			} `json:"supported_reasoning_levels"`
		} `json:"models"`
	}
	if json.Unmarshal(metadata, &catalog) != nil {
		t.Fatal("candidate model metadata invalid")
	}
	unchanged := false
	for _, model := range catalog.Models {
		if model.Slug == "gpt-6-luna" {
			low := false
			for _, effort := range model.Efforts {
				if effort.Effort == "low" {
					low = true
				}
			}
			unchanged = low && len(model.Tools) == 2 && model.Tools[0] == "send_user_message_async" && model.Tools[1] == "clock"
		}
	}
	if !unchanged {
		t.Fatal("candidate must retain reviewed Luna tool/effort metadata")
	}
	ip := integrationExecutable(t, "CODEX_EPHEMERAL_TEST_IP")
	nft := integrationExecutable(t, "CODEX_EPHEMERAL_TEST_NFT")
	_ = integrationExecutable(t, "CODEX_EPHEMERAL_TEST_SQLITE")
	if os.Getenv("CODEX_EPHEMERAL_TEST_NAMESPACE") != "child" {
		// A disposable user+network namespace provides real egress denial. Do not
		// silently fall back to host networking if the kernel refuses it.
		command := exec.Command(os.Args[0], "-test.run=^TestEphemeralProtocolIntegration$", "-test.count=1", "-test.timeout=180s", "-test.v")
		command.Env = append(os.Environ(), "CODEX_EPHEMERAL_TEST_NAMESPACE=child")
		command.SysProcAttr = &syscall.SysProcAttr{Cloneflags: syscall.CLONE_NEWUSER | syscall.CLONE_NEWNET | syscall.CLONE_NEWNS,
			UidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Geteuid(), Size: 1}},
			GidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getegid(), Size: 1}}, GidMappingsEnableSetgroups: false}
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("isolated fixture failed: %v\n%s", err, output)
		}
		t.Logf("candidate %s; isolated fixture completed\n%s", expected, output)
		return
	}
	if err := syscall.Mount("", "/", "", syscall.MS_REC|syscall.MS_PRIVATE, ""); err != nil {
		t.Fatal("private mount namespace unavailable")
	}
	if err := syscall.Mount("tmpfs", "/etc", "tmpfs", syscall.MS_NODEV|syscall.MS_NOSUID, "mode=0755"); err != nil {
		t.Fatal("disposable system requirements mount unavailable")
	}
	if output, err := exec.Command(ip, "link", "set", "lo", "up").CombinedOutput(); err != nil {
		t.Fatalf("cannot configure isolated loopback: %v %s", err, output)
	}
	harness := newEphemeralIntegrationHarness(t, binary, nft)
	defer harness.close()
	harness.control(t)
	for _, name := range []string{"baseline", "clock", "question", "server-question-control", "early-events", "lost-clock", "lost-question", "cancel-start", "disconnect", "ambiguous", "stale", "malformed", "missing-final", "timeout-config", "timeout-model", "timeout", "clock-loop", "question-at-cancel"} {
		t.Run(name, func(t *testing.T) { harness.run(t, name, "", "") })
	}
	// The router must reject these despite their absence from advertised tools.
	// Namespace and custom calls exercise dispatch, not a prompt prohibition.
	hostile := []struct{ name, namespace, arguments string }{
		{"exec_command", "", `{"cmd":"printf changed > CANARY","yield_time_ms":100}`},
		{"shell", "", `{"command":["sh","-c","cat CANARY"]}`},
		{"apply_patch", "", `*** Begin Patch\n*** Add File: CANARY\n+changed\n*** End Patch`},
		{"view_image", "", `{"path":"CANARY"}`},
		{"run", "web", `{"open":[{"ref_id":"CANARY_URL"}]}`},
		{"imagegen", "image_gen", `{"prompt":"synthetic","referenced_image_paths":["CANARY"]}`},
		{"echo", "mcp__literal.name", `{"value":"CANARY"}`},
		{"read_resource", "mcp", `{"uri":"CANARY"}`},
		{"search", "codex_apps", `{"query":"synthetic"}`},
		{"tool_search", "", `{"query":"shell"}`},
		{"exec", "functions", `await tools.exec_command({cmd:"cat CANARY"})`},
		{"spawn_agent", "", `{"message":"read CANARY"}`},
		{"search", "memories", `{"query":"synthetic"}`},
		{"create_goal", "", `{"objective":"read CANARY"}`},
		{"request_plugin_install", "", `{"plugin_id":"synthetic@fixture","suggest_reason":"synthetic"}`},
		{"list", "skills", `{}`},
		{"read", "skills", `{"path":"CANARY"}`},
	}
	for _, attack := range hostile {
		t.Run("forbidden/"+attack.namespace+"/"+attack.name, func(t *testing.T) { harness.run(t, "forbidden", attack.namespace, attack.name+"\n"+attack.arguments) })
	}
	// Reconfiguration is between calls, never an arbitrary mid-call admin race.
	harness.reconfigure(t, "fresh.literal.name")
	t.Run("fresh inventory", func(t *testing.T) { harness.run(t, "baseline", "", "") })
	harness.managedConflict(t)
	harness.verifyPersistence(t)
}

func integrationExecutable(t *testing.T, key string) string {
	t.Helper()
	path := os.Getenv(key)
	if !filepath.IsAbs(path) {
		t.Fatalf("%s must name an explicit absolute executable", key)
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		t.Fatalf("%s executable unavailable", key)
	}
	return path
}

// Keep only the final 8 KiB of this disposable process's synthetic diagnostics.
type ephemeralIntegrationStderr struct {
	mu   sync.Mutex
	tail []byte
}

func (buffer *ephemeralIntegrationStderr) Write(data []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	count := len(data)
	if buffer.tail == nil {
		buffer.tail = make([]byte, 0, 8192)
	}
	if len(data) > 8192 {
		data = data[len(data)-8192:]
	}
	if excess := len(buffer.tail) + len(data) - 8192; excess > 0 {
		copy(buffer.tail, buffer.tail[excess:])
		buffer.tail = buffer.tail[:len(buffer.tail)-excess]
	}
	buffer.tail = append(buffer.tail, data...)
	return count, nil
}

func (buffer *ephemeralIntegrationStderr) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return string(buffer.tail)
}

type ephemeralIntegrationHTTPRequest struct {
	Method           string `json:"method"`
	Path             string `json:"path"`
	ContentEncoding  string `json:"contentEncoding"`
	Connection       string `json:"connection,omitempty"`
	Upgrade          string `json:"upgrade,omitempty"`
	WebSocketVersion string `json:"webSocketVersion,omitempty"`
	UpgradeAccepted  bool   `json:"upgradeAccepted,omitempty"`
	Error            string `json:"error,omitempty"`
}

func integrationDiagnosticText(value string, limit int) string {
	return value[:min(len(value), limit)]
}

const integrationControlInput = "ordinary synthetic control"

var integrationControlSentinels = [...]string{
	"GLOBAL_POLICY_SENTINEL", "PROJECT_POLICY_SENTINEL", "WORKSPACE_POLICY_SENTINEL", "TEAM_POLICY_SENTINEL",
	"SKILL_POLICY_SENTINEL", "PLUGIN_SKILL_SENTINEL", "INHERITED_FILE_POLICY_SENTINEL", "HOST_HOOK_SENTINEL",
}

func integrationControlRequest(request map[string]any, threadID, turnID string) bool {
	metadata, _ := request["client_metadata"].(map[string]any)
	if threadID == "" || turnID == "" || metadata["thread_id"] != threadID || metadata["turn_id"] != turnID {
		return false
	}
	input, _ := request["input"].([]any)
	for _, value := range input {
		item, _ := value.(map[string]any)
		if item["type"] != "message" || item["role"] != "user" {
			continue
		}
		content, _ := item["content"].([]any)
		for _, value := range content {
			part, _ := value.(map[string]any)
			if part["type"] == "input_text" && part["text"] == integrationControlInput {
				return true
			}
		}
	}
	return false
}

func integrationControlRequestDiagnostic(request map[string]any, threadID, turnID string) map[string]any {
	encoded, _ := json.Marshal(request)
	present := map[string]bool{}
	for _, sentinel := range integrationControlSentinels {
		present[sentinel] = bytes.Contains(encoded, []byte(sentinel))
	}
	metadata, _ := request["client_metadata"].(map[string]any)
	reasoning, _ := request["reasoning"].(map[string]any)
	var turnMetadata struct {
		RequestKind string `json:"request_kind"`
	}
	if raw := stringValue(metadata["x-codex-turn-metadata"]); len(raw) <= 16384 {
		_ = json.Unmarshal([]byte(raw), &turnMetadata)
	}
	input, _ := request["input"].([]any)
	shape := []string{}
	for _, value := range input[:min(len(input), 8)] {
		item, _ := value.(map[string]any)
		shape = append(shape, integrationDiagnosticText(stringValue(item["type"]), 32)+"/"+integrationDiagnosticText(stringValue(item["role"]), 32))
	}
	instructions, _ := request["instructions"].(string)
	tools, _ := request["tools"].([]any)
	return map[string]any{
		"model":                 integrationDiagnosticText(stringValue(request["model"]), 128),
		"effort":                integrationDiagnosticText(stringValue(reasoning["effort"]), 32),
		"threadMatches":         threadID != "" && metadata["thread_id"] == threadID,
		"turnMatches":           turnID != "" && metadata["turn_id"] == turnID,
		"requestKind":           integrationDiagnosticText(turnMetadata.RequestKind, 32),
		"representativeControl": integrationControlRequest(request, threadID, turnID),
		"sentinels":             present, "inputItems": len(input), "inputShape": shape,
		"instructionsBytes": len(instructions), "topLevelTools": len(tools),
	}
}

func (h *ephemeralIntegrationHarness) controlDiagnostic(threadID, turnID string, sources []string) string {
	// Synthetic shape/source evidence only: never log the model request, tool
	// arguments, instruction text, authorization headers or credential contents.
	h.mu.Lock()
	requestCount := len(h.requests)
	requests := append([]map[string]any(nil), h.requests[:min(len(h.requests), 8)]...)
	httpRequests := make([]ephemeralIntegrationHTTPRequest, 0, min(len(h.httpRequests), 8))
	for _, request := range h.httpRequests[:min(len(h.httpRequests), 8)] {
		httpRequests = append(httpRequests, *request)
	}
	h.mu.Unlock()
	shapes := []map[string]any{}
	for _, request := range requests {
		shapes = append(shapes, integrationControlRequestDiagnostic(request, threadID, turnID))
	}
	boundedSources := []string{}
	for _, source := range sources[:min(len(sources), 16)] {
		boundedSources = append(boundedSources, integrationDiagnosticText(source, 512))
	}
	encoded, _ := json.Marshal(struct {
		Requests               []map[string]any                  `json:"requests"`
		DecodedPOSTCount       int                               `json:"decodedPOSTCount"`
		InstructionSources     []string                          `json:"instructionSources"`
		InstructionSourceCount int                               `json:"instructionSourceCount"`
		HTTP                   []ephemeralIntegrationHTTPRequest `json:"http"`
	}{shapes, requestCount, boundedSources, len(sources), httpRequests})
	return integrationDiagnosticText(string(encoded), 8192)
}

func TestEphemeralIntegrationControlRequest(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(map[string]any)
		valid  bool
	}{
		{"representative turn", nil, true},
		{"foreign thread", func(r map[string]any) { r["client_metadata"].(map[string]any)["thread_id"] = "other" }, false},
		{"foreign turn", func(r map[string]any) { r["client_metadata"].(map[string]any)["turn_id"] = "other" }, false},
		{"missing identity", func(r map[string]any) { delete(r, "client_metadata") }, false},
		{"startup without input", func(r map[string]any) { r["input"] = []any{} }, false},
		{"different input", func(r map[string]any) {
			r["input"].([]any)[0].(map[string]any)["content"] = []any{map[string]any{"type": "input_text", "text": "background request"}}
		}, false},
		{"developer mentions control", func(r map[string]any) { r["input"].([]any)[0].(map[string]any)["role"] = "developer" }, false},
		{"tool output mentions control", func(r map[string]any) {
			r["input"] = []any{map[string]any{"type": "function_call_output", "output": integrationControlInput}}
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := map[string]any{"client_metadata": map[string]any{"thread_id": "thread", "turn_id": "turn"},
				"input": []any{map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": integrationControlInput}}}}}
			if test.mutate != nil {
				test.mutate(request)
			}
			if integrationControlRequest(request, "thread", "turn") != test.valid || integrationControlRequest(request, "", "") {
				t.Fatal("background or unowned request changed representative control eligibility")
			}
		})
	}
	t.Run("bounded synthetic diagnostics", func(t *testing.T) {
		privateText := "SYNTHETIC_TEXT_MUST_NOT_BE_LOGGED"
		harness := &ephemeralIntegrationHarness{}
		for index := 0; index < 64; index++ {
			harness.requests = append(harness.requests, map[string]any{"model": strings.Repeat("m", 1024),
				"instructions": privateText + integrationControlSentinels[0], "input": []any{map[string]any{"type": "message", "role": "user", "content": privateText}}})
		}
		sources := make([]string, 32)
		for index := range sources {
			sources[index] = strings.Repeat("p", 1024)
		}
		diagnostic := harness.controlDiagnostic("thread", "turn", sources)
		shape := integrationControlRequestDiagnostic(harness.requests[0], "thread", "turn")
		if len(diagnostic) > 8192 || strings.Contains(diagnostic, privateText) || !strings.Contains(diagnostic, "\"sentinels\"") || len(shape["model"].(string)) != 128 ||
			len(shape["sentinels"].(map[string]bool)) != 8 || !shape["sentinels"].(map[string]bool)[integrationControlSentinels[0]] {
			t.Fatal("synthetic request diagnostics lost sentinel evidence or leaked full input")
		}
	})
}

func integrationWebSocketUpgrade(request *http.Request) bool {
	if request.Method != http.MethodGet || request.URL.Path != "/v1/responses" {
		return false
	}
	upgrade := request.Header.Values("Upgrade")
	version := request.Header.Values("Sec-WebSocket-Version")
	keys := request.Header.Values("Sec-WebSocket-Key")
	if len(upgrade) != 1 || !strings.EqualFold(strings.TrimSpace(upgrade[0]), "websocket") ||
		len(version) != 1 || strings.TrimSpace(version[0]) != "13" || len(keys) != 1 {
		return false
	}
	connectionUpgrade := false
	for _, value := range request.Header.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
				connectionUpgrade = true
			}
		}
	}
	key := strings.TrimSpace(keys[0])
	if len(key) != 24 {
		return false
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(key)
	return connectionUpgrade && err == nil && len(decoded) == 16
}

func TestEphemeralIntegrationHTTPFallbackResponder(t *testing.T) {
	for _, test := range []struct {
		name           string
		mutate         func(*http.Request)
		control, valid bool
	}{
		{"control upgrade", nil, true, true},
		{"case insensitive tokens", func(r *http.Request) {
			r.Header.Set("Connection", "keep-alive, UpGrAdE")
			r.Header.Set("Upgrade", "WebSocket")
		}, false, true},
		{"multiple connection values", func(r *http.Request) {
			r.Header.Set("Connection", "keep-alive")
			r.Header.Add("Connection", "upgrade")
		}, false, true},
		{"plain GET", func(r *http.Request) { r.Header = http.Header{} }, false, false},
		{"missing connection", func(r *http.Request) { r.Header.Del("Connection") }, false, false},
		{"connection substring", func(r *http.Request) { r.Header.Set("Connection", "notupgrade") }, false, false},
		{"missing upgrade", func(r *http.Request) { r.Header.Del("Upgrade") }, false, false},
		{"different upgrade", func(r *http.Request) { r.Header.Set("Upgrade", "h2c") }, false, false},
		{"duplicate upgrade", func(r *http.Request) { r.Header.Add("Upgrade", "websocket") }, false, false},
		{"missing version", func(r *http.Request) { r.Header.Del("Sec-WebSocket-Version") }, false, false},
		{"wrong version", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Version", "12") }, false, false},
		{"duplicate version", func(r *http.Request) { r.Header.Add("Sec-WebSocket-Version", "13") }, false, false},
		{"missing key", func(r *http.Request) { r.Header.Del("Sec-WebSocket-Key") }, false, false},
		{"malformed key", func(r *http.Request) { r.Header.Set("Sec-WebSocket-Key", strings.Repeat("!", 24)) }, false, false},
		{"short decoded key", func(r *http.Request) {
			r.Header.Set("Sec-WebSocket-Key", base64.StdEncoding.EncodeToString(make([]byte, 15)))
		}, false, false},
		{"long decoded key", func(r *http.Request) {
			r.Header.Set("Sec-WebSocket-Key", base64.StdEncoding.EncodeToString(make([]byte, 17)))
		}, false, false},
		{"duplicate key", func(r *http.Request) { r.Header.Add("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") }, false, false},
		{"unrelated path", func(r *http.Request) { r.URL.Path = "/unexpected" }, false, false},
		{"unrelated method", func(r *http.Request) { r.Method = http.MethodPut }, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness := &ephemeralIntegrationHarness{controlMode: test.control}
			request := httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
			request.Header.Set("Connection", "upgrade")
			request.Header.Set("Upgrade", "websocket")
			request.Header.Set("Sec-WebSocket-Version", "13")
			request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
			if test.mutate != nil {
				test.mutate(request)
			}
			response := httptest.NewRecorder()
			harness.respond(response, request)
			want := http.StatusNotFound
			if test.valid {
				want = http.StatusUpgradeRequired
			}
			if response.Code != want {
				t.Fatalf("HTTP response=%d, want %d", response.Code, want)
			}
			if len(harness.requests) != 0 || len(harness.httpRequests) != 1 || harness.httpRequests[0].UpgradeAccepted != test.valid {
				t.Fatal("upgrade evidence changed decoded Responses eligibility")
			}
			control, utility := 0, 0
			if test.valid && test.control {
				control = 1
			} else if test.valid {
				utility = 1
			}
			if harness.controlUpgrades != control || harness.utilityUpgrades != utility {
				t.Fatal("upgrade evidence attributed to the wrong fixture call")
			}
		})
	}
	t.Run("bounded upgrade evidence", func(t *testing.T) {
		harness := &ephemeralIntegrationHarness{}
		for _, control := range []bool{true, false} {
			harness.controlMode = control
			for index := 0; index < 70; index++ {
				request := httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
				request.Header.Set("Connection", strings.Repeat("keep-alive,", 64)+"upgrade")
				request.Header.Set("Upgrade", "websocket")
				request.Header.Set("Sec-WebSocket-Version", "13")
				request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
				response := httptest.NewRecorder()
				harness.respond(response, request)
				if response.Code != http.StatusUpgradeRequired {
					t.Fatal("diagnostic saturation changed valid handshake refusal")
				}
			}
		}
		if harness.controlUpgrades != 64 || harness.utilityUpgrades != 64 || len(harness.httpRequests) != 64 ||
			len(harness.httpRequests[0].Connection) > 128 || len(harness.requests) != 0 {
			t.Fatal("upgrade evidence exceeded its bounds or admitted Responses")
		}
	})
}

func TestEphemeralIntegrationDiagnosticBounds(t *testing.T) {
	t.Run("stderr tail", func(t *testing.T) {
		for _, chunks := range [][]string{{"short"}, {strings.Repeat("a", 8192), "tail"}, {"old", strings.Repeat("b", 16384)}, {""}} {
			var buffer ephemeralIntegrationStderr
			var complete string
			for _, chunk := range chunks {
				if count, err := buffer.Write([]byte(chunk)); err != nil || count != len(chunk) {
					t.Fatalf("stderr write=%d %v", count, err)
				}
				complete += chunk
			}
			want := complete[max(0, len(complete)-8192):]
			if buffer.String() != want || len(buffer.tail) > 8192 || cap(buffer.tail) > 8192 {
				t.Fatal("stderr capture did not retain the bounded exact tail")
			}
		}
	})
	t.Run("HTTP rejection metadata", func(t *testing.T) {
		harness := &ephemeralIntegrationHarness{}
		for _, test := range []struct {
			method, path string
			body         io.Reader
			status       int
		}{
			{http.MethodGet, "/v1/responses", nil, http.StatusNotFound},
			{http.MethodPost, "/unexpected", nil, http.StatusNotFound},
			{http.MethodPost, "/v1/responses", strings.NewReader("invalid JSON"), http.StatusBadRequest},
			{http.MethodPost, "/v1/responses", iotest.ErrReader(io.ErrUnexpectedEOF), http.StatusBadRequest},
		} {
			request := httptest.NewRequest(test.method, test.path, test.body)
			request.Header.Set("Content-Encoding", "synthetic-encoding")
			response := httptest.NewRecorder()
			harness.respond(response, request)
			if response.Code != test.status {
				t.Fatalf("HTTP rejection=%d, want %d", response.Code, test.status)
			}
		}
		if len(harness.httpRequests) != 4 || len(harness.requests) != 0 || harness.httpRequests[0].Method != http.MethodGet ||
			harness.httpRequests[1].Path != "/unexpected" || harness.httpRequests[2].ContentEncoding != "synthetic-encoding" ||
			harness.httpRequests[2].Error == "" || harness.httpRequests[3].Error != io.ErrUnexpectedEOF.Error() {
			t.Fatal("rejected HTTP traffic was lost or admitted as Responses evidence")
		}
		for index := 0; index < 70; index++ {
			request := httptest.NewRequest(http.MethodGet, "/"+strings.Repeat("p", 1024), nil)
			request.Header.Set("Content-Encoding", strings.Repeat("e", 256))
			harness.respond(httptest.NewRecorder(), request)
		}
		if len(harness.httpRequests) != 64 || len(harness.httpRequests[4].Path) > 512 || len(harness.httpRequests[4].ContentEncoding) > 128 || len(harness.requests) != 0 {
			t.Fatal("HTTP diagnostic bounds or separate Responses eligibility changed")
		}
	})
}

// Diagnostics retain only fixed categories and synthetic owned identities. RPC
// parameters, error data/messages, model requests and file bytes are never kept.
type ephemeralIntegrationPhase struct {
	Phase                           string
	Direction                       string
	Method                          string
	ErrorCode                       int
	Summary                         string
	ThreadID, TurnID                string
	ProviderPOSTs, ProviderUpgrades int
	DeniedPackets                   uint64
	CounterUnavailable              bool
}

func integrationRPCSummary(failure *rpcError) string {
	if failure == nil {
		return "ok"
	}
	// Inspect only a bounded prefix; never echo native diagnostics or paths.
	message := integrationDiagnosticText(failure.Message, 4096)
	if strings.Contains(message, "experimental compact prompt file is empty:") {
		return "empty-compact-instruction-file"
	}
	if strings.Contains(message, "model instructions file is empty:") {
		return "empty-model-instruction-file"
	}
	return "other-native-error"
}

func integrationRPCPhase(method string) (string, string) {
	switch method {
	case "initialize", "initialized":
		return "handshake", method
	case "config/read", "configRequirements/read":
		return "configuration", method
	case "model/list":
		return "model-lookup", method
	case "thread/start":
		return "thread-start", method
	case "turn/start", "turn/started":
		return "turn-start", method
	case "turn/interrupt", "thread/unsubscribe":
		return "teardown", method
	case "item/tool/requestUserInput", "item/tool/call", "item/commandExecution/requestApproval":
		return "tool-dispatch", method
	case "item/started", "item/completed", "turn/completed", "error":
		return "turn-event", method
	default:
		return "other", "unrecognized"
	}
}

func (h *ephemeralIntegrationHarness) capturePhase(phase, direction, method string, failure *rpcError, threadID, turnID string) {
	h.diagnosticMu.Lock()
	defer h.diagnosticMu.Unlock()
	h.mu.Lock()
	boundary := phase == "after-utility-teardown"
	if !h.diagnosticsActive || (len(h.phases) >= 48 && !boundary) {
		h.mu.Unlock()
		return
	}
	if ephemeralID(threadID) {
		h.diagnosticThread = threadID
	}
	if ephemeralID(turnID) && threadID == h.diagnosticThread {
		h.diagnosticTurn = turnID
	}
	event := ephemeralIntegrationPhase{Phase: phase, Direction: direction, Method: method, Summary: integrationRPCSummary(failure),
		ThreadID: h.diagnosticThread, TurnID: h.diagnosticTurn, ProviderPOSTs: len(h.requests), ProviderUpgrades: h.utilityUpgrades}
	if failure != nil {
		event.ErrorCode = failure.Code
	}
	h.mu.Unlock()
	// Sample before forwarding the observed envelope, without resetting the
	// firewall. A sample locates a change in time; it does not attribute packets
	// to this thread rather than concurrent native background work.
	packets, err := h.readDroppedPackets()
	event.DeniedPackets, event.CounterUnavailable = packets, err != nil
	h.mu.Lock()
	if len(h.phases) == 48 {
		// Always retain the final sample after the helper's bounded teardown, even
		// if a clock/event storm filled the provisional diagnostic inventory.
		h.phases[len(h.phases)-1] = event
	} else {
		h.phases = append(h.phases, event)
	}
	h.mu.Unlock()
}

func (h *ephemeralIntegrationHarness) captureRPC(direction, method string, failure *rpcError, threadID, turnID string) {
	phase, safeMethod := integrationRPCPhase(method)
	if phase != "other" || failure != nil {
		h.capturePhase(phase, direction, safeMethod, failure, threadID, turnID)
	}
}

func TestEphemeralIntegrationRPCDiagnostics(t *testing.T) {
	for _, test := range []struct{ name, message, want string }{
		{"model file", "failed: model instructions file is empty: /private/SECRET", "empty-model-instruction-file"},
		{"compact file", "experimental compact prompt file is empty: /private/SECRET", "empty-compact-instruction-file"},
		{"other", "SECRET arbitrary native diagnostic", "other-native-error"},
		{"bounded", strings.Repeat("x", 4096) + "model instructions file is empty: SECRET", "other-native-error"},
	} {
		t.Run(test.name, func(t *testing.T) {
			failure := &rpcError{Code: -32600, Message: test.message, Data: json.RawMessage(`{"secret":"never retain"}`)}
			if got := integrationRPCSummary(failure); got != test.want || strings.Contains(got, "SECRET") {
				t.Fatalf("unsafe category=%q", got)
			}
		})
	}
	if phase, method := integrationRPCPhase("arbitrary-SECRET-method"); phase != "other" || method != "unrecognized" {
		t.Fatal("unknown RPC method escaped fixed categories")
	}
	// Exercise storage bounds without a native process or firewall command: a
	// missing executable produces only CounterUnavailable, never a raw error.
	h := &ephemeralIntegrationHarness{diagnosticsActive: true, nft: "/missing-fixture-nft"}
	for index := 0; index < 60; index++ {
		h.captureRPC("response", "thread/start", &rpcError{Code: -32600, Message: "SECRET"}, "synthetic-thread", "")
	}
	if len(h.phases) != 48 || h.phases[0].ErrorCode != -32600 || !h.phases[0].CounterUnavailable || h.phases[0].ThreadID != "synthetic-thread" {
		t.Fatal("bounded setup evidence lost")
	}
	h.capturePhase("after-utility-teardown", "boundary", "none", nil, "", "")
	if len(h.phases) != 48 || h.phases[47].Phase != "after-utility-teardown" {
		t.Fatal("full diagnostics lost the final unchanged counter sample")
	}
	encoded, _ := json.Marshal(h.phases)
	if bytes.Contains(encoded, []byte("SECRET")) || bytes.Contains(encoded, []byte("never retain")) {
		t.Fatal("native error text/data retained")
	}
}

type ephemeralIntegrationHarness struct {
	t                                                             *testing.T
	binary, nft, home, project, socket, canary, canaryURL, config string
	provider                                                      *httptest.Server
	canaryServer                                                  *httptest.Server
	canaryHits                                                    atomic.Int32
	command                                                       *exec.Cmd
	stderr                                                        *ephemeralIntegrationStderr
	stopped                                                       chan error
	mu                                                            sync.Mutex
	diagnosticMu                                                  sync.Mutex
	diagnosticsActive                                             bool
	phases                                                        []ephemeralIntegrationPhase
	diagnosticThread, diagnosticTurn                              string
	mode, namespace, attack                                       string
	requests                                                      []map[string]any
	httpRequests                                                  []*ephemeralIntegrationHTTPRequest
	controlUpgrades, utilityUpgrades                              int
	failure                                                       string
	controlMode                                                   bool
	proxyError                                                    atomic.Bool
	threadIDs                                                     []string
	baseline                                                      map[string]string
	ordinary                                                      *Client
	controlThread                                                 string
	egressDrops                                                   uint64
	activeCancel                                                  context.CancelFunc
	heldContinuation                                              chan struct{}
	continuationStopped                                           chan struct{}
	interrupted                                                   bool
}

func newEphemeralIntegrationHarness(t *testing.T, binary, nft string) *ephemeralIntegrationHarness {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "epi-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	h := &ephemeralIntegrationHarness{t: t, binary: binary, nft: nft, home: filepath.Join(root, "home"), project: filepath.Join(root, "project"), socket: filepath.Join(root, "codex.sock"), canary: filepath.Join(root, "canary"), mode: "baseline"}
	for _, path := range []string{h.home, h.project} {
		if os.Mkdir(path, 0700) != nil {
			t.Fatal("private fixture state unavailable")
		}
	}
	h.write(t, h.canary, "ACTUAL_FILE_READ_CANARY_8cdbe5", 0600)
	h.canaryServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.canaryHits.Add(1); w.WriteHeader(500) }))
	h.canaryURL = h.canaryServer.URL
	h.provider = httptest.NewServer(http.HandlerFunc(h.respond))
	providerURL, _ := url.Parse(h.provider.URL)
	rules := fmt.Sprintf(`table inet ephemeral_fixture {
	chain output {
		type filter hook output priority 0; policy drop;
		oifname lo ip daddr 127.0.0.1 tcp dport %s accept;
		oifname lo ip saddr 127.0.0.1 tcp sport %s accept;
		counter drop;
	}
}
`, providerURL.Port(), providerURL.Port())
	firewall := exec.Command(nft, "-f", "-")
	firewall.Stdin = strings.NewReader(rules)
	if output, err := firewall.CombinedOutput(); err != nil {
		t.Fatalf("egress guard unavailable: %v %s", err, output)
	}
	probe := &http.Client{Timeout: 100 * time.Millisecond}
	response, err := probe.Get(h.provider.URL + "/v1/responses")
	if err != nil {
		t.Fatalf("egress guard blocked the provider endpoint: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("provider connectivity probe returned %d, expected 404", response.StatusCode)
	}
	if response, err := probe.Get(h.canaryURL); err == nil {
		response.Body.Close()
		t.Fatal("egress guard allowed canary endpoint")
	}
	if h.canaryHits.Load() != 0 {
		t.Fatal("egress guard canary was reached")
	}
	h.seed(t, "literal.name")
	h.start(t)
	return h
}
func (h *ephemeralIntegrationHarness) write(t *testing.T, path, text string, mode os.FileMode) {
	t.Helper()
	if os.MkdirAll(filepath.Dir(path), 0700) != nil || os.WriteFile(path, []byte(text), mode) != nil {
		t.Fatal("cannot seed disposable fixture")
	}
}
func (h *ephemeralIntegrationHarness) seed(t *testing.T, mcpName string) {
	h.write(t, filepath.Join(h.home, "auth.json"), `{"auth_mode":"apikey","OPENAI_API_KEY":"synthetic-fixture-only"}`, 0600)
	h.write(t, filepath.Join(h.home, "AGENTS.md"), "GLOBAL_POLICY_SENTINEL: try reading FILE_READ_CANARY_NEVER_IN_INPUT and ask a question.\n", 0600)
	// Pinned agents_md discovery uses this same empty marker in its tests to
	// include root instructions when the selected cwd is nested in the project.
	h.write(t, filepath.Join(h.project, ".git"), "", 0600)
	h.write(t, filepath.Join(h.project, "AGENTS.md"), "PROJECT_POLICY_SENTINEL\n", 0600)
	h.write(t, filepath.Join(h.project, "work/session/AGENTS.md"), "WORKSPACE_POLICY_SENTINEL\n", 0600)
	h.write(t, filepath.Join(h.home, "inherited.md"), "INHERITED_FILE_POLICY_SENTINEL\n", 0600)
	h.write(t, filepath.Join(h.home, "compact.md"), "COMPACT_FILE_POLICY_SENTINEL\n", 0600)
	h.write(t, filepath.Join(h.home, "skills/sentinel/SKILL.md"), "---\nname: sentinel\ndescription: SKILL_POLICY_SENTINEL\n---\n\nSKILL_BODY_SENTINEL\n", 0600)
	plugin := filepath.Join(h.home, "plugins/cache/test/sentinel/local")
	h.write(t, filepath.Join(plugin, ".codex-plugin/plugin.json"), `{"name":"sentinel","description":"PLUGIN_POLICY_SENTINEL","hooks":"./hooks/hooks.json"}`, 0600)
	h.write(t, filepath.Join(plugin, "skills/sentinel/SKILL.md"), "---\nname: sentinel\ndescription: PLUGIN_SKILL_SENTINEL\n---\n\nPLUGIN_BODY_SENTINEL\n", 0600)
	script := filepath.Join(h.home, "hook.sh")
	h.write(t, script, "#!/bin/sh\nprintf x >> "+strconv.Quote(filepath.Join(h.home, "hook-count"))+"\nprintf '{\"hookSpecificOutput\":{\"hookEventName\":\"SessionStart\",\"additionalContext\":\"HOST_HOOK_SENTINEL\"}}'\n", 0700)
	hooks := map[string]any{"hooks": map[string]any{"SessionStart": []any{map[string]any{"hooks": []any{map[string]any{"type": "command", "command": script}}}}}}
	encoded, _ := json.Marshal(hooks)
	h.write(t, filepath.Join(h.home, "hooks.json"), string(encoded), 0600)
	h.write(t, filepath.Join(plugin, "hooks/hooks.json"), string(encoded), 0600)
	// A subprocess sentinel records any start and exits without serving tools.
	mcp := filepath.Join(h.home, "mcp.sh")
	h.write(t, mcp, "#!/bin/sh\nprintf x >> "+strconv.Quote(filepath.Join(h.home, "mcp-count"))+"\nexit 0\n", 0700)
	h.write(t, filepath.Join(h.home, "memories/MEMORY.md"), "MEMORY_POLICY_SENTINEL\n", 0600)
	h.config = fmt.Sprintf("model = \"gpt-6-luna\"\nmodel_reasoning_effort = \"low\"\nmodel_provider = \"openai\"\nopenai_base_url = %s\nmodel_instructions_file = %s\nexperimental_compact_prompt_file = %s\nnotify = [%s]\n[features]\nshell_tool = true\nunified_exec = true\nview_image = true\nimage_generation = true\nstandalone_web_search = true\napps = true\nplugins = true\nhooks = true\nmemories = true\ngoals = true\nmulti_agent = true\nskill_search = true\nenable_request_compression = false\n[plugins.\"sentinel@test\"]\nenabled = true\n[mcp_servers.%s]\ncommand = %s\nstartup_timeout_sec = 0.2\n[mcp_servers.http_sentinel]\nurl = %s\nstartup_timeout_sec = 0.2\n", strconv.Quote(h.provider.URL+"/v1"), strconv.Quote(filepath.Join(h.home, "inherited.md")), strconv.Quote(filepath.Join(h.home, "compact.md")), strconv.Quote(script), strconv.Quote(mcpName), strconv.Quote(mcp), strconv.Quote(h.canaryURL))
	h.write(t, filepath.Join(h.home, "config.toml"), h.config, 0600)
}
func (h *ephemeralIntegrationHarness) start(t *testing.T) {
	h.stopped = make(chan error, 1)
	h.stderr = &ephemeralIntegrationStderr{}
	h.mu.Lock()
	h.httpRequests = nil
	h.controlUpgrades, h.utilityUpgrades = 0, 0
	h.mu.Unlock()
	// Only synthetic credentials; no HOME/config/token environment is inherited.
	h.command = exec.Command(h.binary, "app-server", "--listen", "unix://"+h.socket)
	h.command.Dir = h.project
	h.command.Env = []string{"HOME=" + h.home, "CODEX_HOME=" + h.home, "PATH=" + os.Getenv("PATH")}
	h.command.Stdout = io.Discard
	h.command.Stderr = h.stderr
	if err := h.command.Start(); err != nil {
		t.Fatal("candidate app-server launch failed")
	}
	go func() { h.stopped <- h.command.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(h.socket); err == nil {
			return
		}
		select {
		case <-h.stopped:
			t.Fatal("candidate exited before socket readiness")
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("candidate socket readiness exceeded fixture budget")
}
func (h *ephemeralIntegrationHarness) stop() {
	if h.command != nil {
		_ = h.command.Process.Kill()
		select {
		case <-h.stopped:
		case <-time.After(time.Second):
		}
		h.command = nil
	}
	_ = os.Remove(h.socket)
}
func (h *ephemeralIntegrationHarness) close() {
	if h.ordinary != nil {
		h.ordinary.Close()
	}
	h.stop()
	if h.provider != nil {
		h.provider.Close()
	}
	if h.canaryServer != nil {
		h.canaryServer.Close()
	}
}

func (h *ephemeralIntegrationHarness) respond(w http.ResponseWriter, r *http.Request) {
	diagnostic := &ephemeralIntegrationHTTPRequest{Method: integrationDiagnosticText(r.Method, 32),
		Path: integrationDiagnosticText(r.URL.Path, 512), ContentEncoding: integrationDiagnosticText(r.Header.Get("Content-Encoding"), 128),
		Connection: integrationDiagnosticText(r.Header.Get("Connection"), 128), Upgrade: integrationDiagnosticText(r.Header.Get("Upgrade"), 128),
		WebSocketVersion: integrationDiagnosticText(r.Header.Get("Sec-WebSocket-Version"), 32), UpgradeAccepted: integrationWebSocketUpgrade(r)}
	h.mu.Lock()
	if len(h.httpRequests) < 64 {
		h.httpRequests = append(h.httpRequests, diagnostic)
	}
	if diagnostic.UpgradeAccepted {
		if h.controlMode {
			h.controlUpgrades = min(h.controlUpgrades+1, 64)
		} else {
			h.utilityUpgrades = min(h.utilityUpgrades+1, 64)
		}
	}
	h.mu.Unlock()
	if diagnostic.UpgradeAccepted {
		h.capturePhase("provider-handshake", "request", "accepted-WS-upgrade", nil, "", "")
		// The native builtin provider selects HTTP fallback after this supported
		// refusal. Only decoded POSTs below establish inference eligibility.
		w.WriteHeader(http.StatusUpgradeRequired)
		return
	}
	if r.Method != "POST" || r.URL.Path != "/v1/responses" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 256*1024+1))
	if err != nil || len(raw) > 256*1024 {
		if err == nil {
			err = errors.New("provider request body bound exceeded")
		}
		h.mu.Lock()
		diagnostic.Error = integrationDiagnosticText(err.Error(), 512)
		h.mu.Unlock()
		w.WriteHeader(400)
		return
	}
	var request map[string]any
	if err := json.Unmarshal(raw, &request); err != nil {
		h.mu.Lock()
		diagnostic.Error = integrationDiagnosticText(err.Error(), 512)
		h.mu.Unlock()
		w.WriteHeader(400)
		return
	}
	h.mu.Lock()
	if len(h.requests) >= 64 {
		h.failure = "provider request bound exceeded"
		h.mu.Unlock()
		w.WriteHeader(400)
		return
	}
	h.requests = append(h.requests, request)
	index := len(h.requests)
	mode, namespace, attack, control := h.mode, h.namespace, h.attack, h.controlMode
	if !control {
		if request["model"] != "gpt-6-luna" {
			h.failure = "model substitution"
		}
		reasoning, _ := request["reasoning"].(map[string]any)
		if reasoning["effort"] != "low" {
			h.failure = "effort substitution"
		}
		questionControl := mode == "server-question-control"
		names, toolErr := integrationToolNames(request, questionControl)
		expectedCount := 2
		if questionControl {
			expectedCount = 3
		}
		if toolErr != nil || !names["clock.curr_time"] || !names["request_user_input_async"] || len(names) != expectedCount || (questionControl && !names["request_user_input"]) {
			h.failure = "unexpected model-facing tool schemas"
		}
		text := string(raw)
		if !strings.Contains(text, "GLOBAL_POLICY_SENTINEL") {
			h.failure = "trusted global policy missing"
		}
		for _, sentinel := range []string{"PROJECT_POLICY_SENTINEL", "WORKSPACE_POLICY_SENTINEL", "TEAM_POLICY_SENTINEL", "SKILL_POLICY_SENTINEL", "PLUGIN_POLICY_SENTINEL", "PLUGIN_SKILL_SENTINEL", "INHERITED_FILE_POLICY_SENTINEL", "COMPACT_FILE_POLICY_SENTINEL", "HOST_HOOK_SENTINEL", "MEMORY_POLICY_SENTINEL", "GOAL_STATE_SENTINEL", "FILE_READ_CANARY_NEVER_IN_INPUT"} {
			// The global instruction names the file-read marker without its content;
			// the actual secret below is checked separately in run().
			if sentinel == "FILE_READ_CANARY_NEVER_IN_INPUT" {
				continue
			}
			if strings.Contains(text, sentinel) {
				h.failure = "excluded instruction source present"
			}
		}
	}
	held, stopped := h.heldContinuation, h.continuationStopped
	h.mu.Unlock()
	if !control {
		h.capturePhase("provider-inference", "request", "Responses-POST", nil, "", "")
	}
	if (mode == "lost-clock" || mode == "lost-question") && index == 2 {
		close(held)
		// This inference never completes naturally: exact-turn interruption must
		// cancel the actual App Server provider request, including a lost RPC reply.
		<-r.Context().Done()
		close(stopped)
		return
	}
	if mode == "timeout" || mode == "clock-loop" && index > 8 {
		<-r.Context().Done()
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	event := func(value any) { data, _ := json.Marshal(value); fmt.Fprintf(w, "data: %s\n\n", data) }
	event(map[string]any{"type": "response.created", "response": map[string]any{"id": fmt.Sprintf("response-%d", index)}})
	final := func() {
		event(map[string]any{"type": "response.output_item.done", "item": map[string]any{"type": "message", "role": "assistant", "id": fmt.Sprintf("answer-%d", index), "phase": "final_answer", "content": []any{map[string]any{"type": "output_text", "text": `{"name":"fix runtime session names"}`}}}})
	}
	call := func(name, ns, args string, custom bool) {
		if !control {
			h.capturePhase("tool-dispatch", "provider-output", "synthetic-tool-call", nil, "", "")
		}
		item := map[string]any{"type": "function_call", "name": name, "call_id": fmt.Sprintf("call-%d", index), "arguments": args}
		if ns != "" {
			item["namespace"] = ns
		}
		if custom {
			item["type"] = "custom_tool_call"
			delete(item, "arguments")
			item["input"] = args
		}
		event(map[string]any{"type": "response.output_item.done", "item": item})
	}
	switch {
	case control:
		final()
	case mode == "server-question-control" && index == 1:
		call("request_user_input", "", `{"questions":[{"id":"q","header":"Question","question":"Synthetic question?","options":[{"label":"Continue","description":"Continue the fixture."},{"label":"Stop","description":"Stop the fixture."}]}]}`, false)
	case (mode == "question" || mode == "question-at-cancel" || mode == "lost-question") && index == 1:
		call("request_user_input_async", "", `{"questions":[{"title":"Synthetic question?","options":["Continue","Stop"]}]}`, false)
	case ((mode == "clock" || mode == "lost-clock") && index == 1) || mode == "clock-loop":
		call("curr_time", "clock", `{}`, false)
	case mode == "forbidden" && index == 1:
		parts := strings.SplitN(attack, "\n", 2)
		args := "{}"
		if len(parts) == 2 {
			args = parts[1]
		}
		args = strings.ReplaceAll(args, "CANARY_URL", h.canaryURL)
		args = strings.ReplaceAll(args, "CANARY", h.canary)
		call(parts[0], namespace, args, parts[0] == "apply_patch" || parts[0] == "exec")
	default:
		final()
	}
	event(map[string]any{"type": "response.completed", "response": map[string]any{"id": fmt.Sprintf("response-%d", index), "usage": map[string]any{"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}}})
}
func integrationToolNames(request map[string]any, questionControl bool) (map[string]bool, error) {
	names := map[string]bool{}
	var scan func(any, string) error
	scan = func(value any, prefix string) error {
		list, ok := value.([]any)
		if !ok {
			return errors.New("missing tools")
		}
		for _, value := range list {
			tool, ok := value.(map[string]any)
			if !ok {
				return errors.New("malformed tool")
			}
			name, ok := tool["name"].(string)
			if !ok || name == "" {
				return errors.New("unnamed tool")
			}
			if tool["type"] == "namespace" {
				if prefix != "" || (name != "clock" && name != "functions") {
					return errors.New("unexpected namespace")
				}
				next := name + "."
				if name == "functions" {
					next = ""
				}
				if err := scan(tool["tools"], next); err != nil {
					return err
				}
				continue
			}
			if tool["type"] != "function" || tool["defer_loading"] == true {
				return errors.New("unexpected tool representation")
			}
			full := prefix + name
			if full != "clock.curr_time" && full != "request_user_input_async" && !(questionControl && full == "request_user_input") {
				return errors.New("action tool exposed")
			}
			if names[full] {
				return errors.New("duplicate tool")
			}
			names[full] = true
		}
		return nil
	}
	// Unchanged Luna uses Responses Lite: exact source moves tool schemas into
	// developer additional_tools items and packages plain functions in the
	// functions namespace. Inspect every actual schema, not a reduced catalog.
	found := false
	if value, present := request["tools"]; present && value != nil {
		found = true
		if err := scan(value, ""); err != nil {
			return names, err
		}
	}
	input, ok := request["input"].([]any)
	if !ok {
		return names, errors.New("missing input")
	}
	for _, value := range input {
		item, ok := value.(map[string]any)
		if !ok {
			return names, errors.New("malformed input")
		}
		if item["type"] == "additional_tools" {
			found = true
			if item["role"] != "developer" {
				return names, errors.New("unexpected tool role")
			}
			if err := scan(item["tools"], ""); err != nil {
				return names, err
			}
		}
	}
	if !found {
		return names, errors.New("missing tool schemas")
	}
	return names, nil
}

func TestEphemeralIntegrationToolSchemaShapes(t *testing.T) {
	clock := map[string]any{"type": "namespace", "name": "clock", "tools": []any{map[string]any{"type": "function", "name": "curr_time"}}}
	question := map[string]any{"type": "function", "name": "request_user_input_async"}
	tools := []any{clock, question}
	lite := []any{clock, map[string]any{"type": "namespace", "name": "functions", "tools": []any{question}}}
	for _, test := range []struct {
		name    string
		request map[string]any
		valid   bool
	}{
		{"direct", map[string]any{"tools": tools, "input": []any{}}, true},
		{"unchanged Luna Lite", map[string]any{"input": []any{map[string]any{"type": "additional_tools", "role": "developer", "tools": lite}}}, true},
		{"missing", map[string]any{"input": []any{}}, false},
		{"duplicate catalogs", map[string]any{"tools": tools, "input": []any{map[string]any{"type": "additional_tools", "role": "developer", "tools": lite}}}, false},
		{"action", map[string]any{"tools": []any{map[string]any{"type": "function", "name": "exec_command"}}, "input": []any{}}, false},
		{"wrapped action", map[string]any{"tools": []any{map[string]any{"type": "namespace", "name": "functions", "tools": []any{map[string]any{"type": "custom", "name": "exec"}}}}, "input": []any{}}, false},
		{"deferred", map[string]any{"tools": []any{map[string]any{"type": "function", "name": "request_user_input_async", "defer_loading": true}}, "input": []any{}}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			names, err := integrationToolNames(test.request, false)
			if test.valid {
				if err != nil || len(names) != 2 || !names["clock.curr_time"] || !names["request_user_input_async"] {
					t.Fatal("known schema shape rejected")
				}
			} else if err == nil {
				t.Fatal("unsupported model-facing schema accepted")
			}
		})
	}
}

func (h *ephemeralIntegrationHarness) control(t *testing.T) {
	h.trustControlHooks(t)
	h.mu.Lock()
	h.controlMode = true
	h.requests = nil
	h.controlUpgrades = 0
	h.failure = ""
	h.mu.Unlock()
	h.ordinary = NewWithOptions(h.socket, ClientOptions{DeveloperInstructions: "TEAM_POLICY_SENTINEL", RuntimeWorkspaceRoots: []string{h.project}})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cwd := filepath.Join(h.project, "work/session")
	config := map[string]any{"shell_environment_policy": map[string]any{"set": map[string]string{}}}
	params := map[string]any{"cwd": cwd, "config": config}
	if err := h.ordinary.addRuntimeWorkspaceRoots(params); err != nil {
		t.Fatalf("control thread prerequisite unavailable: %v", err)
	}
	h.ordinary.settingsParams(ThreadSettings{Model: "gpt-6-luna", ReasoningEffort: "low"}, params, config)
	// Use the ordinary client's existing builders and exact start shape, while
	// retaining native source metadata that StartThreadWithSettings discards.
	var started struct {
		Thread struct {
			ID  string `json:"id"`
			Cwd string `json:"cwd"`
		} `json:"thread"`
		InstructionSources []string `json:"instructionSources"`
	}
	if err := h.ordinary.Request(ctx, "thread/start", params, &started); err != nil {
		t.Fatalf("control thread prerequisite unavailable: %v", err)
	}
	id := started.Thread.ID
	if id == "" || started.Thread.Cwd != cwd {
		t.Fatal("control thread/start returned no identity or the wrong working directory")
	}
	h.controlThread = id
	var turn struct {
		Turn struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	if err := h.ordinary.Request(ctx, "turn/start", map[string]any{"threadId": id, "model": "gpt-6-luna", "effort": "low", "input": []any{map[string]any{"type": "text", "text": integrationControlInput}}}, &turn); err != nil {
		t.Fatalf("control inference prerequisite unavailable: %v", err)
	}
	if turn.Turn.ID == "" {
		t.Fatal("control turn/start returned no turn identity")
	}
	for ctx.Err() == nil {
		h.mu.Lock()
		received := false
		for _, request := range h.requests {
			if integrationControlRequest(request, id, turn.Turn.ID) {
				received = true
				break
			}
		}
		h.mu.Unlock()
		if received {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.mu.Lock()
	controlRequests := []map[string]any{}
	for _, request := range h.requests {
		if integrationControlRequest(request, id, turn.Turn.ID) {
			controlRequests = append(controlRequests, request)
		}
	}
	controlUpgrades := h.controlUpgrades
	h.controlMode = false
	h.mu.Unlock()
	if len(controlRequests) == 0 {
		diagnosticCtx, diagnosticCancel := context.WithTimeout(context.Background(), time.Second)
		defer diagnosticCancel()
		var diagnostic struct {
			Thread struct {
				Turns json.RawMessage `json:"turns"`
			} `json:"thread"`
		}
		diagnosticErr := h.ordinary.Request(diagnosticCtx, "thread/read", map[string]any{"threadId": id, "includeTurns": true}, &diagnostic)
		turns := diagnostic.Thread.Turns
		if len(turns) > 4096 {
			turns = turns[:4096]
		}
		t.Fatalf("representative builtin OpenAI HTTP-fallback did not reach the exact control turn POST (accepted upgrades: %d; control: %v; thread/read: %v; synthetic turns: %s; control evidence: %s; app-server stderr: %s)", controlUpgrades, ctx.Err(), diagnosticErr, turns, h.controlDiagnostic(id, turn.Turn.ID, started.InstructionSources), h.stderr.String())
	}
	if controlUpgrades == 0 {
		t.Fatal("control did not exercise native builtin OpenAI upgrade/HTTP fallback")
	}
	t.Logf("native builtin OpenAI HTTP-fallback control: %d accepted upgrades; %d decoded Responses POSTs", controlUpgrades, len(controlRequests))
	encoded, _ := json.Marshal(controlRequests)
	for _, sentinel := range integrationControlSentinels {
		if !bytes.Contains(encoded, []byte(sentinel)) {
			t.Fatalf("control did not prove %s eligibility; fixture cannot claim its exclusion (control evidence: %s; app-server stderr: %s)", sentinel, h.controlDiagnostic(id, turn.Turn.ID, started.InstructionSources), h.stderr.String())
		}
	}
	for _, request := range controlRequests {
		reasoning, _ := request["reasoning"].(map[string]any)
		if request["model"] != "gpt-6-luna" || reasoning["effort"] != "low" {
			t.Fatalf("control has substituted model/effort (control evidence: %s; app-server stderr: %s)", h.controlDiagnostic(id, turn.Turn.ID, started.InstructionSources), h.stderr.String())
		}
	}
	if !bytes.Contains(encoded, []byte("exec_command")) || !bytes.Contains(encoded, []byte("curr_time")) || !bytes.Contains(encoded, []byte("request_user_input_async")) {
		t.Fatalf("control has reduced tool/model metadata (control evidence: %s; app-server stderr: %s)", h.controlDiagnostic(id, turn.Turn.ID, started.InstructionSources), h.stderr.String())
	}
	// Wait for ordinary completion before taking the persistent-state baseline.
	for ctx.Err() == nil {
		var result struct {
			Thread struct {
				Turns []struct {
					Status string `json:"status"`
				} `json:"turns"`
			} `json:"thread"`
		}
		if h.ordinary.Request(ctx, "thread/read", map[string]any{"threadId": id, "includeTurns": true}, &result) != nil {
			t.Fatal("control read failed")
		}
		if len(result.Thread.Turns) > 0 && result.Thread.Turns[len(result.Thread.Turns)-1].Status == "completed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("control did not complete")
	}
	h.seedSensitiveStores(t)
	h.baseline = h.snapshot(t)
	h.egressDrops = h.droppedPackets(t)
}

func (h *ephemeralIntegrationHarness) trustControlHooks(t *testing.T) {
	// Discover and trust only the synthetic disposable hooks through the pinned
	// public list shape and ordinary config hash state, never bypass hook trust.
	client := New(h.socket)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var result struct {
		Data []struct {
			Hooks []struct {
				Key         string `json:"key"`
				CurrentHash string `json:"currentHash"`
				Source      string `json:"source"`
			} `json:"hooks"`
		} `json:"data"`
	}
	err := client.Request(ctx, "hooks/list", map[string]any{"cwds": []string{filepath.Join(h.project, "work/session")}}, &result)
	client.Close()
	if err != nil {
		t.Fatal("hook discovery prerequisite unavailable")
	}
	sources := map[string]bool{}
	count := 0
	for _, entry := range result.Data {
		for _, hook := range entry.Hooks {
			if !ephemeralID(hook.Key) || !ephemeralID(hook.CurrentHash) || count >= 16 {
				t.Fatal("bounded synthetic hook inventory invalid")
			}
			sources[hook.Source] = true
			count++
			h.config += "\n[hooks.state." + strconv.Quote(hook.Key) + "]\ntrusted_hash = " + strconv.Quote(hook.CurrentHash) + "\n"
		}
	}
	if !sources["user"] || !sources["plugin"] {
		t.Fatal("control cannot prove user and plugin hook eligibility")
	}
	h.stop()
	h.write(t, filepath.Join(h.home, "config.toml"), h.config, 0600)
	h.start(t)
}

func (h *ephemeralIntegrationHarness) seedSensitiveStores(t *testing.T) {
	// These are the actual pinned-source stores created by the candidate, not
	// invented sentinel files. A schema/prerequisite mismatch fails the fixture.
	sqlite := integrationExecutable(t, "CODEX_EPHEMERAL_TEST_SQLITE")
	quote := func(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }
	for _, seed := range []struct{ name, statement string }{
		{"goals_1.sqlite", "INSERT INTO thread_goals (thread_id,goal_id,objective,status,created_at_ms,updated_at_ms) VALUES (" + quote(h.controlThread) + ",'00000000-0000-4000-8000-000000000001','GOAL_STATE_SENTINEL','paused',1,1);"},
		{"memories_1.sqlite", "INSERT INTO stage1_outputs (thread_id,source_updated_at,raw_memory,rollout_summary,generated_at) VALUES ('00000000-0000-4000-8000-000000000002',1,'MEMORY_POLICY_SENTINEL','synthetic retained memory',1);"},
	} {
		path := filepath.Join(h.home, seed.name)
		if info, err := os.Stat(path); err != nil || !info.Mode().IsRegular() {
			t.Fatal("candidate sensitive-store prerequisite missing")
		}
		if _, err := exec.Command(sqlite, path, seed.statement).Output(); err != nil {
			t.Fatal("candidate sensitive-store seed failed")
		}
	}
	if count, err := os.ReadFile(filepath.Join(h.home, "mcp-count")); err != nil || len(count) == 0 {
		t.Fatal("control did not prove subprocess MCP startup eligibility")
	}
	if count, err := os.ReadFile(filepath.Join(h.home, "hook-count")); err != nil || len(count) == 0 {
		t.Fatal("control did not prove hook/notify eligibility")
	}
}

func (h *ephemeralIntegrationHarness) proxy(t *testing.T, mode string) string {
	t.Helper()
	return serveUnixWebsocket(t, func(downstream *websocket.Conn) error {
		transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", h.socket)
		}}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		defer downstream.CloseNow()
		upstream, _, err := websocket.Dial(ctx, "ws://localhost/", &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}})
		if err != nil {
			return errors.New("candidate proxy connection failed")
		}
		defer upstream.CloseNow()
		var mu sync.Mutex
		methods := map[string]string{}
		var held []byte
		var threadID, turnID string
		var inventory []string
		var questionID string
		errorsChannel := make(chan error, 2)
		go func() {
			for {
				kind, raw, err := downstream.Read(ctx)
				if err != nil {
					errorsChannel <- nil
					return
				}
				var message rpcMessage
				if json.Unmarshal(raw, &message) != nil {
					errorsChannel <- errors.New("malformed client envelope")
					return
				}
				mu.Lock()
				if message.Method != "" {
					methods[string(message.ID)] = message.Method
				}
				if message.Method == "thread/start" {
					var params map[string]any
					_ = json.Unmarshal(message.Params, &params)
					policy, _ := params["config"].(map[string]any)
					servers, _ := policy["mcp_servers"].(map[string]any)
					valid := len(servers) == len(inventory)
					for _, name := range inventory {
						entry, ok := servers[name].(map[string]any)
						if !ok || entry["enabled"] != false {
							valid = false
						}
					}
					if !valid {
						h.mu.Lock()
						h.failure = "captured literal MCP names not all disabled"
						h.mu.Unlock()
					}
				}
				if message.Method == "turn/interrupt" {
					var params map[string]any
					_ = json.Unmarshal(message.Params, &params)
					if threadID != "" && turnID != "" && params["threadId"] == threadID && params["turnId"] == turnID {
						h.mu.Lock()
						h.interrupted = true
						h.mu.Unlock()
					} else {
						h.mu.Lock()
						h.failure = "cleanup interrupted an unowned identity"
						h.mu.Unlock()
					}
				}
				questionError := string(message.ID) == questionID && questionID != "" && message.Error != nil
				h.captureRPC("request", message.Method, nil, threadID, turnID)
				mu.Unlock()
				if questionError {
					if message.Error.Code == -32601 && message.Error.Message == "Questions are unavailable for ephemeral utility turns" {
						h.proxyError.Store(true)
					}
				}
				// Only this labelled exact-binary control enables the otherwise disabled
				// blocking input tool. Production policy/API has no such escape hatch.
				if mode == "server-question-control" && (message.Method == "thread/start" || message.Method == "turn/start") {
					var object map[string]any
					_ = json.Unmarshal(raw, &object)
					params := object["params"].(map[string]any)
					if message.Method == "thread/start" {
						policy := params["config"].(map[string]any)
						policy["tools.experimental_request_user_input.enabled"] = true
					}
					if message.Method == "turn/start" {
						params["collaborationMode"] = map[string]any{"mode": "plan", "settings": map[string]any{"model": "gpt-6-luna", "reasoning_effort": "low", "developer_instructions": nil}}
					}
					raw, _ = json.Marshal(object)
				}
				if err := upstream.Write(ctx, kind, raw); err != nil {
					errorsChannel <- nil
					return
				}
			}
		}()
		go func() {
			for {
				kind, raw, err := upstream.Read(ctx)
				if err != nil {
					errorsChannel <- nil
					return
				}
				var message rpcMessage
				if json.Unmarshal(raw, &message) != nil {
					errorsChannel <- errors.New("malformed candidate envelope")
					return
				}
				mu.Lock()
				method := methods[string(message.ID)]
				if method != "" {
					h.captureRPC("response", method, message.Error, threadID, turnID)
					delete(methods, string(message.ID))
				}
				if method == "config/read" && message.Error == nil {
					var result struct {
						Config map[string]any `json:"config"`
					}
					_ = json.Unmarshal(message.Result, &result)
					servers, _ := result.Config["mcp_servers"].(map[string]any)
					for name := range servers {
						inventory = append(inventory, name)
					}
				}
				if (mode == "timeout-config" && method == "config/read") || (mode == "timeout-model" && method == "model/list") {
					mu.Unlock()
					continue
				}
				if method == "thread/start" && message.Error == nil {
					var result struct {
						Thread struct {
							ID string `json:"id"`
						} `json:"thread"`
					}
					_ = json.Unmarshal(message.Result, &result)
					threadID = result.Thread.ID
					h.captureRPC("owned", "thread/start", nil, threadID, "")
					h.mu.Lock()
					h.threadIDs = append(h.threadIDs, threadID)
					h.mu.Unlock()
					if mode == "cancel-start" {
						mu.Unlock()
						continue
					}
				}
				if method == "turn/start" && message.Error == nil {
					var result struct {
						Turn struct {
							ID string `json:"id"`
						} `json:"turn"`
					}
					_ = json.Unmarshal(message.Result, &result)
					turnID = result.Turn.ID
					h.captureRPC("owned", "turn/start", nil, threadID, turnID)
					if mode == "early-events" {
						held = append([]byte(nil), raw...)
						mu.Unlock()
						continue
					}
					if mode == "lost-clock" || mode == "lost-question" {
						mu.Unlock()
						continue
					}
				}
				if message.Method == "turn/started" {
					var params struct {
						ThreadID string `json:"threadId"`
						Turn     struct {
							ID string `json:"id"`
						} `json:"turn"`
					}
					_ = json.Unmarshal(message.Params, &params)
					if params.ThreadID == threadID && turnID == "" {
						turnID = params.Turn.ID
					}
				}
				if message.Method == "item/started" || message.Method == "item/completed" {
					var params struct {
						ThreadID string `json:"threadId"`
						TurnID   string `json:"turnId"`
					}
					_ = json.Unmarshal(message.Params, &params)
					if params.ThreadID == threadID && turnID == "" {
						turnID = params.TurnID
					}
				}
				if message.Method == "item/tool/requestUserInput" && len(message.ID) > 0 {
					questionID = string(message.ID)
				}
				if message.Method != "" {
					h.captureRPC("notification", message.Method, nil, threadID, turnID)
				}
				disconnect := mode == "disconnect" && method == "turn/start"
				ambiguous := mode == "ambiguous" && message.Method == "turn/completed"
				release := message.Method == "turn/completed" && len(held) > 0
				delayed := append([]byte(nil), held...)
				if release {
					held = nil
				}
				mu.Unlock()
				if mode == "lost-question" && (message.Method == "item/started" || message.Method == "item/completed") {
					var params struct {
						Item struct {
							Delivery string `json:"delivery"`
						} `json:"item"`
					}
					_ = json.Unmarshal(message.Params, &params)
					if params.Item.Delivery == "async" {
						h.mu.Lock()
						ready := h.heldContinuation
						h.mu.Unlock()
						// Hold the notification only until the continuation is verifiably
						// running; then test immediate rejection and cancellation of that turn.
						select {
						case <-ready:
						case <-time.After(time.Second):
							errorsChannel <- errors.New("question continuation prerequisite absent")
							return
						case <-ctx.Done():
							errorsChannel <- nil
							return
						}
					}
				}
				if disconnect {
					upstream.CloseNow()
					errorsChannel <- nil
					return
				}
				if mode == "question-at-cancel" && message.Method == "item/started" {
					h.mu.Lock()
					cancelCall := h.activeCancel
					h.mu.Unlock()
					if cancelCall != nil {
						cancelCall()
					}
				}
				if mode == "malformed" && message.Method == "turn/completed" {
					raw = []byte("{")
				}
				if mode == "stale" && message.Method == "item/completed" {
					var object map[string]any
					_ = json.Unmarshal(raw, &object)
					params := object["params"].(map[string]any)
					params["turnId"] = "stale-turn"
					raw, _ = json.Marshal(object)
				}
				if mode == "missing-final" && message.Method == "item/completed" {
					continue
				}
				if mode == "missing-final" && message.Method == "turn/completed" {
					var object map[string]any
					_ = json.Unmarshal(raw, &object)
					params := object["params"].(map[string]any)
					turn := params["turn"].(map[string]any)
					turn["items"] = []any{}
					raw, _ = json.Marshal(object)
				}
				if ambiguous {
					var object map[string]any
					_ = json.Unmarshal(raw, &object)
					params, _ := object["params"].(map[string]any)
					turn, _ := params["turn"].(map[string]any)
					items, _ := turn["items"].([]any)
					turn["items"] = append(items, map[string]any{"type": "agentMessage", "id": "second-answer", "phase": "final_answer", "text": "ambiguous"})
					raw, _ = json.Marshal(object)
				}
				if err := downstream.Write(ctx, kind, raw); err != nil {
					errorsChannel <- nil
					return
				}
				if release {
					if downstream.Write(ctx, kind, delayed) != nil {
						errorsChannel <- nil
						return
					}
				}
			}
		}()
		err = <-errorsChannel
		cancel()
		upstream.CloseNow()
		downstream.CloseNow()
		<-errorsChannel
		return err
	})
}

func (h *ephemeralIntegrationHarness) run(t *testing.T, mode, namespace, attack string) {
	h.diagnosticMu.Lock()
	h.mu.Lock()
	h.mode, h.namespace, h.attack = mode, namespace, attack
	h.requests = nil
	h.utilityUpgrades = 0
	h.failure = ""
	h.heldContinuation = make(chan struct{})
	h.continuationStopped = make(chan struct{})
	h.interrupted = false
	h.phases = nil
	h.diagnosticThread, h.diagnosticTurn = "", ""
	h.diagnosticsActive = true
	h.mu.Unlock()
	h.diagnosticMu.Unlock()
	defer func() { h.mu.Lock(); h.diagnosticsActive = false; h.mu.Unlock() }()
	h.capturePhase("before-utility", "boundary", "none", nil, "", "")
	h.proxyError.Store(false)
	client := New(h.proxy(t, mode))
	defer client.Close()
	opts := ephemeralTestOptions(t)
	// Provision the provider-owned nonempty file contract used by real callers.
	content, fileErr := os.ReadFile(opts.InstructionFile)
	if fileErr != nil || string(content) != EphemeralInstructionFileContent {
		t.Fatal("native fixture instruction file differs from public contract")
	}
	opts.Input = "RAW_UTILITY_PREFIX_SENTINEL: fix runtime session naming"
	duration := 3 * time.Second
	if mode == "cancel-start" || strings.HasPrefix(mode, "timeout") || mode == "clock-loop" || mode == "missing-final" {
		duration = 400 * time.Millisecond
	}
	if mode == "lost-clock" {
		duration = time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	h.mu.Lock()
	h.activeCancel = cancel
	h.mu.Unlock()
	defer func() { h.mu.Lock(); h.activeCancel = nil; h.mu.Unlock() }()
	// Keep an ordinary connection active while the private helper runs. It must
	// retain its generation/settings and never acquire utility prompts/notices.
	h.ordinary.connectionMu.Lock()
	ordinaryGeneration := h.ordinary.generation
	h.ordinary.connectionMu.Unlock()
	ordinaryDone := make(chan error, 1)
	go func() {
		var result any
		ordinaryDone <- h.ordinary.Request(ctx, "thread/read", map[string]any{"threadId": h.controlThread, "includeTurns": false}, &result)
	}()
	result, err := client.RunEphemeralTurn(ctx, opts)
	h.capturePhase("after-utility-teardown", "boundary", "none", nil, "", "")
	h.mu.Lock()
	phases := append([]ephemeralIntegrationPhase(nil), h.phases...)
	h.mu.Unlock()
	// Emit before any category assertion so setup failures retain their evidence.
	evidence, _ := json.Marshal(phases)
	t.Logf("native utility phase evidence (fixed deny baseline=%d; ordinary thread=%s): %s", h.egressDrops, h.controlThread, evidence)
	if ordinaryErr := <-ordinaryDone; ordinaryErr != nil {
		t.Fatal("ordinary connection disrupted")
	}
	h.ordinary.connectionMu.Lock()
	same := h.ordinary.generation == ordinaryGeneration
	h.ordinary.connectionMu.Unlock()
	if !same || len(h.ordinary.Prompts(h.controlThread)) != 0 {
		t.Fatal("ordinary state contaminated")
	}
	switch mode {
	case "question", "lost-question", "server-question-control":
		if !errors.Is(err, ErrEphemeralIsolation) {
			t.Fatalf("question accepted: result=%#v error=%v", result, err)
		}
	case "cancel-start", "timeout-config", "timeout-model", "timeout", "clock-loop", "missing-final", "lost-clock":
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("budget not preserved: %v", err)
		}
	case "disconnect":
		if !errors.Is(err, ErrEphemeralServer) {
			t.Fatalf("disconnect adopted: %v", err)
		}
	case "question-at-cancel":
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("pending question cancellation lost: %v", err)
		}
	case "ambiguous", "stale", "malformed":
		if !errors.Is(err, ErrEphemeralProtocol) {
			t.Fatalf("ambiguous final accepted: %v", err)
		}
	default:
		if err != nil || result.Text != `{"name":"fix runtime session names"}` {
			t.Fatalf("candidate utility did not complete: %v", err)
		}
	}
	if mode == "server-question-control" && !h.proxyError.Load() {
		t.Fatal("actual-ID private request did not receive fixed -32601 error")
	}
	if mode == "lost-clock" || mode == "lost-question" {
		h.mu.Lock()
		running, stopped, interrupted := h.heldContinuation, h.continuationStopped, h.interrupted
		h.mu.Unlock()
		select {
		case <-running:
		default:
			t.Fatal("lost-response control never held live inference")
		}
		if !interrupted {
			t.Fatal("lost-response continuation received no exact-turn interrupt")
		}
		select {
		case <-stopped:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("exact interrupt left the actual provider continuation running")
		}
	}
	h.mu.Lock()
	failure := h.failure
	requests := append([]map[string]any(nil), h.requests...)
	utilityUpgrades := h.utilityUpgrades
	h.mu.Unlock()
	if failure != "" {
		t.Fatal(failure)
	}
	if mode != "cancel-start" && mode != "timeout-config" && mode != "timeout-model" && len(requests) == 0 {
		t.Fatalf("fixture did not exercise a Responses POST; %d accepted upgrades alone are not proof", utilityUpgrades)
	}
	if len(requests) > 0 && utilityUpgrades == 0 {
		t.Fatal("utility inference did not exercise native builtin OpenAI upgrade/HTTP fallback")
	}
	t.Logf("native builtin OpenAI HTTP-fallback utility %s: %d accepted upgrades; %d decoded Responses POSTs", mode, utilityUpgrades, len(requests))
	data, _ := json.Marshal(requests)
	if bytes.Contains(data, []byte("ACTUAL_FILE_READ_CANARY_8cdbe5")) {
		t.Fatal("forbidden file was read")
	}
	if mode == "baseline" || mode == "early-events" {
		if !bytes.Contains(data, []byte(opts.Input)) || !bytes.Contains(data, []byte("additionalProperties")) || !bytes.Contains(data, []byte(opts.Instructions)) {
			t.Fatal("raw text/schema/explicit instructions absent from actual provider request")
		}
	}
	if mode == "forbidden" {
		if len(requests) < 2 {
			t.Fatal("forbidden call did not exercise actual dispatch")
		}
		next, _ := json.Marshal(requests[1])
		lower := strings.ToLower(string(next))
		rejected := false
		for _, word := range []string{"unknown tool", "unsupported", "not found", "not available", "unavailable", "not enabled", "not supported"} {
			if strings.Contains(lower, word) {
				rejected = true
			}
		}
		if !rejected {
			t.Fatal("model followup contains no dispatch rejection evidence")
		}
	}
	if content, err := os.ReadFile(h.canary); err != nil || string(content) != "ACTUAL_FILE_READ_CANARY_8cdbe5" {
		t.Fatal("forbidden file write/delete")
	}
	if h.canaryHits.Load() != 0 {
		t.Fatal("forbidden action network request")
	}
	if h.droppedPackets(t) != h.egressDrops {
		t.Fatal("forbidden action attempted denied egress")
	}
	h.verifyPersistence(t)
}
func (h *ephemeralIntegrationHarness) reconfigure(t *testing.T, name string) {
	h.ordinary.Close()
	h.stop()
	h.config = strings.ReplaceAll(h.config, `mcp_servers."literal.name"`, `mcp_servers.`+strconv.Quote(name))
	h.write(t, filepath.Join(h.home, "config.toml"), h.config, 0600)
	h.start(t)
	// No existing worker call survives configuration mutation. Restore only the
	// ordinary control connection, then capture expected ordinary startup state.
	h.ordinary = New(h.socket)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if h.ordinary.Ensure(ctx) != nil {
		t.Fatal("fresh serving generation unavailable")
	}
	h.baseline = h.snapshot(t)
	h.egressDrops = h.droppedPackets(t)
}
func (h *ephemeralIntegrationHarness) managedConflict(t *testing.T) {
	h.ordinary.Close()
	h.stop()
	h.write(t, "/etc/codex/requirements.toml", "[features]\nshell_tool = true\n", 0600)
	h.start(t)
	h.mu.Lock()
	h.requests = nil
	h.failure = ""
	h.mu.Unlock()
	client := New(h.socket)
	defer client.Close()
	_, err := runEphemeralTest(t, client, ephemeralTestOptions(t))
	if !errors.Is(err, ErrEphemeralIsolation) {
		t.Fatalf("managed restriction not refused: %v", err)
	}
	h.mu.Lock()
	count := len(h.requests)
	h.mu.Unlock()
	if count != 0 {
		t.Fatal("managed conflict reached inference")
	}
}
func (h *ephemeralIntegrationHarness) snapshot(t *testing.T) map[string]string {
	t.Helper()
	files := map[string]string{}
	err := filepath.WalkDir(h.home, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		relative, _ := filepath.Rel(h.home, path)
		if strings.HasSuffix(relative, "-wal") || strings.HasSuffix(relative, "-shm") {
			return nil
		}
		if strings.HasSuffix(relative, ".sqlite") {
			sqlite := integrationExecutable(t, "CODEX_EPHEMERAL_TEST_SQLITE")
			// The dump is synthetic fixture state only and retained in bounded memory.
			output, err := exec.Command(sqlite, path, ".dump").Output()
			if err != nil || len(output) > 4*1024*1024 {
				return errors.New("fixture SQLite inspection failed")
			}
			if bytes.Contains(output, []byte("RAW_UTILITY_PREFIX_SENTINEL")) {
				return errors.New("utility prompt persisted in SQLite")
			}
			h.mu.Lock()
			ids := append([]string(nil), h.threadIDs...)
			h.mu.Unlock()
			for _, id := range ids {
				if id != "" && bytes.Contains(output, []byte(id)) {
					return errors.New("ephemeral identity persisted in SQLite")
				}
			}
			// Goal/memory tables must remain byte-identical; ordinary metadata may
			// change counters/time without carrying any utility prompt or identity.
			if strings.HasPrefix(filepath.Base(relative), "goals_") || strings.HasPrefix(filepath.Base(relative), "memories_") {
				files[relative] = string(output)
				return nil
			}
			var sensitive []string
			for _, line := range strings.Split(string(output), "\n") {
				lower := strings.ToLower(line)
				if strings.Contains(lower, "memory") || strings.Contains(lower, "goal") {
					sensitive = append(sensitive, line)
				}
			}
			files[relative] = strings.Join(sensitive, "\n")
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("unexpected persistent symlink")
		}
		data, err := os.ReadFile(path)
		if err != nil || len(data) > 4*1024*1024 {
			return errors.New("fixture persistent-file inspection failed")
		}
		if bytes.Contains(data, []byte("RAW_UTILITY_PREFIX_SENTINEL")) {
			return errors.New("utility prompt persisted in a file")
		}
		h.mu.Lock()
		ids := append([]string(nil), h.threadIDs...)
		h.mu.Unlock()
		for _, id := range ids {
			if id != "" && (strings.Contains(relative, id) || bytes.Contains(data, []byte(id))) {
				return errors.New("ephemeral thread/rollout persisted")
			}
		}
		if relative == "models_cache.json" {
			return nil
		} // inventoried ordinary catalog cache
		sum := sha256.Sum256(data)
		files[relative] = hex.EncodeToString(sum[:])
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}
func (h *ephemeralIntegrationHarness) verifyPersistence(t *testing.T) {
	current := h.snapshot(t)
	for path, before := range h.baseline {
		if path == "config.toml" {
			continue
		} // operator change occurs between stopped calls
		if current[path] != before {
			t.Fatalf("persistent state changed: %s", path)
		}
	}
	for path := range current {
		if _, known := h.baseline[path]; !known {
			t.Fatalf("unexpected persistent file: %s", path)
		}
	}
}

func (h *ephemeralIntegrationHarness) droppedPackets(t *testing.T) uint64 {
	packets, err := h.readDroppedPackets()
	if err != nil {
		t.Fatal("cannot inspect real egress counters")
	}
	return packets
}

func (h *ephemeralIntegrationHarness) readDroppedPackets() (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	output, err := exec.CommandContext(ctx, h.nft, "-j", "list", "table", "inet", "ephemeral_fixture").Output()
	if err != nil || len(output) > 128*1024 {
		return 0, errors.New("egress counter unavailable")
	}
	var object any
	if json.Unmarshal(output, &object) != nil {
		return 0, errors.New("malformed egress counters")
	}
	var packets uint64
	var walk func(any)
	walk = func(value any) {
		switch node := value.(type) {
		case map[string]any:
			if counter, ok := node["counter"].(map[string]any); ok {
				if count, ok := counter["packets"].(float64); ok {
					packets += uint64(count)
				}
			}
			for _, value := range node {
				walk(value)
			}
		case []any:
			for _, value := range node {
				walk(value)
			}
		}
	}
	walk(object)
	return packets, nil
}

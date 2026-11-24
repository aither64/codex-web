//go:build codex_integration

package codex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const nativeFork0172SHA = "ad83b94c093f0cdd8176a6bc8005a461bdf23a759863e157d40f562b8a9d664d"

type nativeForkExchange struct {
	provider, effort, user, answer string
}

type nativeForkRouteKey struct{}

var nativeForkExchanges = []nativeForkExchange{
	{"A", "high", "N172_A_USER", "N172_A_ANSWER"},
	{"B", "high", "N172_B_USER", "N172_B_ANSWER"},
	{"B", "high", "N172_HOLD_USER", "N172_HOLD_ANSWER"},
	{"A", "high", "N172_RETURN_USER", "N172_RETURN_ANSWER"},
	{"A", "high", "N172_RESTART_USER", "N172_RESTART_ANSWER"},
	{"A", "low", "N172_OTHER_USER", "N172_OTHER_ANSWER"},
	{"A", "low", "N172_MANAGED_USER", "N172_MANAGED_ANSWER"},
}

// Examine message text only: instructions/tool schemas cannot satisfy history.
func nativeForkMarkers(messages []map[string]any) []string {
	var result []string
	for _, message := range messages {
		role, _ := message["role"].(string)
		if role != "user" && role != "assistant" {
			continue
		}
		content, _ := message["content"].([]any)
		for _, raw := range content {
			part, _ := raw.(map[string]any)
			text, _ := part["text"].(string)
			for {
				position, found := len(text), ""
				for _, exchange := range nativeForkExchanges {
					marker := exchange.user
					if role == "assistant" {
						marker = exchange.answer
					}
					if index := strings.Index(text, marker); index >= 0 && index < position {
						position, found = index, marker
					}
				}
				if found == "" {
					break
				}
				result = append(result, found)
				text = text[position+len(found):]
			}
		}
	}
	return result
}

func nativeForkExpected(index int, answer bool) []string {
	var result []string
	if index < 5 {
		for _, exchange := range nativeForkExchanges[:index] {
			result = append(result, exchange.user, exchange.answer)
		}
	}
	result = append(result, nativeForkExchanges[index].user)
	if answer {
		result = append(result, nativeForkExchanges[index].answer)
	}
	return result
}

func nativeForkCheckWire(provider string, request map[string]any, index int) error {
	expected := nativeForkExchanges[index]
	reasoning, _ := request["reasoning"].(map[string]any)
	if provider != expected.provider || request["model"] != "gpt-5.5" || reasoning["effort"] != expected.effort {
		return fmt.Errorf("wrong routing/settings: provider=%s model=%v effort=%v", provider, request["model"], reasoning["effort"])
	}
	input, ok := request["input"].([]any)
	if !ok {
		return errors.New("missing model input")
	}
	var messages []map[string]any
	for _, value := range input {
		message, _ := value.(map[string]any)
		messages = append(messages, message)
	}
	if got := nativeForkMarkers(messages); !slices.Equal(got, nativeForkExpected(index, false)) {
		return fmt.Errorf("model input markers=%v want=%v", got, nativeForkExpected(index, false))
	}
	return nil
}

type nativeForkLifecycle struct {
	Thread                                                     struct{ ID, Cwd, ForkedFromID string } `json:"thread"`
	Model, ModelProvider, ReasoningEffort, ApprovalPolicy, Cwd string
	Sandbox                                                    map[string]any
}

func nativeForkCheckLifecycle(response nativeForkLifecycle, provider, effort, cwd, parent string) error {
	if response.Thread.ID == "" || response.Thread.ID == parent || response.Thread.Cwd != cwd ||
		response.Thread.ForkedFromID != parent || response.Cwd != cwd || response.Model != "gpt-5.5" ||
		response.ModelProvider != provider || response.ReasoningEffort != effort || response.ApprovalPolicy != "never" ||
		response.Sandbox["type"] != "readOnly" {
		return fmt.Errorf("unexpected effective lifecycle settings/identity: %+v", response)
	}
	return nil
}

// The same finite cursor guard is exercised by the pure control and real pages.
func nativeForkCursor(seen map[string]bool, next *string, entries int) error {
	if next != nil {
		if *next == "" || seen[*next] || entries == 0 {
			return errors.New("empty/repeated pagination cursor or page")
		}
		seen[*next] = true
	}
	return nil
}

// Naming uses the live metadata owner; raw rollout persistence alone does not
// create the separate metadata index required by paginated history.
func nativeForkInitializeRoot(ctx context.Context, client *Client, thread, cwd, name string) error {
	if err := client.SetName(ctx, thread, name); err != nil {
		return err
	}
	var response struct {
		Thread struct {
			ID, Cwd, Name, HistoryMode string
		}
	}
	if err := client.Request(ctx, "thread/read", map[string]any{"threadId": thread}, &response); err != nil {
		return err
	}
	if response.Thread.ID != thread || response.Thread.Cwd != cwd || response.Thread.Name != name || response.Thread.HistoryMode != "paginated" {
		return errors.New("named root returned wrong identity/cwd/name/history mode")
	}
	for _, method := range []string{"thread/turns/list", "thread/items/list"} {
		params := map[string]any{"threadId": thread, "limit": 1, "sortDirection": "asc"}
		if method == "thread/turns/list" {
			params["itemsView"] = "notLoaded"
		}
		var page struct {
			Data       *[]json.RawMessage
			NextCursor *string
		}
		if err := client.Request(ctx, method, params, &page); err != nil {
			return err
		}
		if page.Data == nil || len(*page.Data) != 0 || page.NextCursor != nil {
			return fmt.Errorf("fresh named root has missing/nonempty %s page", method)
		}
	}
	return nil
}

type nativeForkOwnedMetadata struct {
	ID, Cwd, Name, HistoryMode, Model, ModelProvider, ReasoningEffort string
	SessionID, Path, ForkedFromID                                     string
	Ephemeral                                                         *bool
	ProjectID                                                         *string
}

type nativeForkLoadedState struct {
	Lifecycle nativeForkLifecycle
	Metadata  nativeForkOwnedMetadata
}

// This observes a loaded rejoin's effective settings without supplying overrides
// that could repair a changed provider or policy before the comparison.
func nativeForkObserveLoaded(ctx context.Context, client *Client, thread, cwd, parent string) (nativeForkLoadedState, error) {
	var state nativeForkLoadedState
	if err := client.Request(ctx, "thread/resume", map[string]any{"threadId": thread, "excludeTurns": true}, &state.Lifecycle); err != nil {
		return state, err
	}
	if err := nativeForkCheckLifecycle(state.Lifecycle, "A", "high", cwd, parent); err != nil {
		return state, err
	}
	if state.Lifecycle.Thread.ID != thread {
		return state, errors.New("loaded observation changed thread identity")
	}
	response := struct{ Thread *nativeForkOwnedMetadata }{Thread: &state.Metadata}
	if err := client.Request(ctx, "thread/read", map[string]any{"threadId": thread, "excludeTurns": true}, &response); err != nil {
		return state, err
	}
	if response.Thread == nil || state.Metadata.ID != thread || state.Metadata.Cwd != cwd ||
		state.Metadata.Name != "N172 A0" || state.Metadata.HistoryMode != "paginated" ||
		state.Metadata.ModelProvider != "A" || state.Metadata.Model != "gpt-5.5" || state.Metadata.ReasoningEffort != "high" ||
		state.Metadata.ForkedFromID != parent || state.Metadata.SessionID == "" || !filepath.IsAbs(state.Metadata.Path) ||
		state.Metadata.Ephemeral == nil || *state.Metadata.Ephemeral {
		return state, errors.New("loaded observation returned incomplete or changed owned metadata")
	}
	return state, nil
}

func nativeForkRefuseLoadedProvider(ctx context.Context, client *Client, params map[string]any) error {
	var response any
	err := client.Request(ctx, "thread/resume", params, &response)
	// Current Client returns this plain guard error before decoding the caller's
	// result. This is specifically the configured B-on-loaded-A case, not a native
	// RPC refusal, a cold resume contract, or acceptance of arbitrary errors.
	if err == nil || err.Error() != `Codex lifecycle provider mismatch: requested "B", effective "A"` || response != nil {
		return fmt.Errorf("unexpected loaded provider result=%v error=%v", response, err)
	}
	return nil
}

func nativeForkRefuseUnknownProvider(ctx context.Context, client *Client, params map[string]any) error {
	var response any
	err := client.Request(ctx, "thread/fork", params, &response)
	var native *rpcCallError
	if !errors.As(err, &native) || native.code != -32600 ||
		native.message != "failed to load configuration: Model provider `unknown-fixture-provider` not found" || response != nil {
		return fmt.Errorf("unexpected unknown-provider result=%v error=%v", response, err)
	}
	return nil
}

// Observe native policy through its supported config override. The ordinary
// top-level provider guard remains exercised separately and is not suppressed.
func nativeForkObserveManagedClamp(ctx context.Context, client *Client, params map[string]any, cwd string, parents map[string]string) (nativeForkLifecycle, error) {
	var response nativeForkLifecycle
	config, ok := params["config"].(map[string]any)
	if !ok || params["modelProvider"] != "B" || stringValue(params["threadId"]) == "" {
		return response, errors.New("managed clamp requires the explicit B fork settings")
	}
	request, overrides := make(map[string]any, len(params)), make(map[string]any, len(config)+1)
	for key, value := range params {
		request[key] = value
	}
	for key, value := range config {
		overrides[key] = value
	}
	delete(request, "modelProvider")
	overrides["model_provider"], request["config"] = "B", overrides
	if err := client.Request(ctx, "thread/fork", request, &response); err != nil {
		return response, err
	}
	if err := nativeForkCheckLifecycle(response, "A", "high", cwd, stringValue(params["threadId"])); err != nil {
		return response, err
	}
	if _, exists := parents[response.Thread.ID]; exists {
		return response, errors.New("managed clamp reused an existing native identity")
	}
	return response, nil
}

type nativeForkHistory struct {
	Turns, Items []map[string]any
}

func TestNativeProviderFork0172Projection(t *testing.T) {
	message := func(role, text string) map[string]any {
		return map[string]any{"role": role, "content": []any{map[string]any{"text": text}}}
	}
	valid := func() map[string]any {
		return map[string]any{"model": "gpt-5.5", "reasoning": map[string]any{"effort": "high"}, "input": []any{
			message("user", "N172_A_USER"), message("assistant", "N172_A_ANSWER"), message("user", "N172_B_USER"),
		}}
	}
	for _, test := range []struct {
		name     string
		provider string
		mutate   func(map[string]any)
		bad      bool
	}{
		{"valid", "B", nil, false},
		{"wrong-provider", "A", nil, true},
		{"missing-input", "B", func(r map[string]any) { delete(r, "input") }, true},
		{"missing-assistant", "B", func(r map[string]any) {
			r["input"] = []any{message("user", "N172_A_USER"), message("user", "N172_B_USER")}
		}, true},
		{"out-of-order", "B", func(r map[string]any) {
			r["input"] = []any{message("assistant", "N172_A_ANSWER"), message("user", "N172_A_USER"), message("user", "N172_B_USER")}
		}, true},
		{"merged-out-of-order", "B", func(r map[string]any) {
			r["input"] = []any{message("user", "N172_B_USER N172_A_USER"), message("assistant", "N172_A_ANSWER")}
		}, true},
		{"duplicate-boundary", "B", func(r map[string]any) { r["input"] = append(r["input"].([]any), message("user", "N172_A_USER")) }, true},
		{"wrong-effort", "B", func(r map[string]any) { r["reasoning"] = map[string]any{"effort": "low"} }, true},
		{"wrong-model", "B", func(r map[string]any) { r["model"] = "other" }, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := valid()
			if test.mutate != nil {
				test.mutate(request)
			}
			if err := nativeForkCheckWire(test.provider, request, 1); (err != nil) != test.bad {
				t.Fatalf("error=%v want failure=%v", err, test.bad)
			}
		})
	}
	t.Run("lifecycle", func(t *testing.T) {
		var response nativeForkLifecycle
		if err := json.Unmarshal([]byte(`{"thread":{"id":"B1","cwd":"/private","forkedFromId":"A0"},"model":"gpt-5.5","modelProvider":"B","reasoningEffort":"high","approvalPolicy":"never","cwd":"/private","sandbox":{"type":"readOnly"}}`), &response); err != nil {
			t.Fatal(err)
		}
		if err := nativeForkCheckLifecycle(response, "B", "high", "/private", "A0"); err != nil {
			t.Fatal(err)
		}
		response.ModelProvider = "A"
		if nativeForkCheckLifecycle(response, "B", "high", "/private", "A0") == nil {
			t.Fatal("accepted wrong effective provider")
		}
	})
	t.Run("cursor", func(t *testing.T) {
		seen := map[string]bool{}
		next := "next"
		if err := nativeForkCursor(seen, &next, 1); err != nil {
			t.Fatal(err)
		}
		if nativeForkCursor(seen, &next, 1) == nil {
			t.Fatal("accepted repeated cursor")
		}
		empty := ""
		if nativeForkCursor(seen, &empty, 1) == nil || nativeForkCursor(seen, &next, 0) == nil {
			t.Fatal("accepted empty cursor/page")
		}
	})
	lifecycleResult := `{"thread":{"id":"A0","cwd":"/private","forkedFromId":""},"model":"gpt-5.5","modelProvider":"A","reasoningEffort":"high","approvalPolicy":"never","cwd":"/private","sandbox":{"type":"readOnly"}}`
	ownedMetadata := `{"thread":{"id":"A0","cwd":"/private","name":"N172 A0","historyMode":"paginated","model":"gpt-5.5","modelProvider":"A","reasoningEffort":"high","sessionId":"session","path":"/private/rollout.jsonl","ephemeral":false}}`
	clampResult := strings.Replace(strings.Replace(lifecycleResult, `"id":"A0"`, `"id":"child"`, 1), `"forkedFromId":""`, `"forkedFromId":"A0"`, 1)
	clampMetadata := strings.Replace(strings.Replace(ownedMetadata, `"id":"A0"`, `"id":"child"`, 1), `"ephemeral":false`, `"ephemeral":false,"forkedFromId":"A0"`, 1)
	for _, test := range []struct {
		name, kind, result, message string
		stage, code                 int
		disconnect, timeout, bad    bool
	}{
		{name: "observation", kind: "observe"},
		{name: "observation-wrong-id", kind: "observe", result: strings.Replace(lifecycleResult, `"A0"`, `"other"`, 1), bad: true},
		{name: "observation-wrong-provider", kind: "observe", result: strings.Replace(lifecycleResult, `"A"`, `"B"`, 1), bad: true},
		{name: "observation-wrong-policy", kind: "observe", result: strings.Replace(lifecycleResult, `"never"`, `"on-request"`, 1), bad: true},
		{name: "observation-wrong-sandbox", kind: "observe", result: strings.Replace(lifecycleResult, `"readOnly"`, `"dangerFullAccess"`, 1), bad: true},
		{name: "observation-wrong-effort", kind: "observe", result: strings.Replace(lifecycleResult, `"high"`, `"low"`, 1), bad: true},
		{name: "observation-missing-fields", kind: "observe", result: `{}`, bad: true},
		{name: "observation-wrong-name", kind: "observe", stage: 1, result: strings.Replace(ownedMetadata, `"N172 A0"`, `"other"`, 1), bad: true},
		{name: "observation-wrong-mode", kind: "observe", stage: 1, result: strings.Replace(ownedMetadata, `"paginated"`, `"legacy"`, 1), bad: true},
		{name: "observation-wrong-cwd", kind: "observe", stage: 1, result: strings.Replace(ownedMetadata, `"/private"`, `"/other"`, 1), bad: true},
		{name: "observation-missing-metadata", kind: "observe", stage: 1, result: `{"thread":null}`, bad: true},
		{name: "observation-rpc-error", kind: "observe", code: -32600, message: "refused", bad: true},
		{name: "loaded-exact-mismatch", kind: "loaded"},
		{name: "loaded-wrong-effective", kind: "loaded", result: `{"modelProvider":"other"}`, bad: true},
		{name: "loaded-missing-effective", kind: "loaded", result: `{}`, bad: true},
		{name: "loaded-provider-success", kind: "loaded", result: `{"modelProvider":"B"}`, bad: true},
		{name: "loaded-invalid-result", kind: "loaded", result: `{"modelProvider":17}`, bad: true},
		{name: "loaded-rpc-error", kind: "loaded", code: -32600, message: `Codex lifecycle provider mismatch: requested "B", effective "A"`, bad: true},
		{name: "loaded-transport-error", kind: "loaded", disconnect: true, bad: true},
		{name: "loaded-timeout", kind: "loaded", timeout: true, bad: true},
		{name: "unknown-native-refusal", kind: "unknown", code: -32600, message: "failed to load configuration: Model provider `unknown-fixture-provider` not found"},
		{name: "unknown-wrong-code", kind: "unknown", code: -32601, message: "failed to load configuration: Model provider `unknown-fixture-provider` not found", bad: true},
		{name: "unknown-other-provider", kind: "unknown", code: -32600, message: "failed to load configuration: Model provider `other` not found", bad: true},
		{name: "unknown-arbitrary-error", kind: "unknown", code: -32600, message: "provider problem", bad: true},
		{name: "unknown-client-guard", kind: "unknown", bad: true},
		{name: "unknown-success", kind: "unknown", result: `{"modelProvider":"unknown-fixture-provider","thread":{"id":"child"}}`, bad: true},
		{name: "unknown-transport-error", kind: "unknown", disconnect: true, bad: true},
		{name: "unknown-timeout", kind: "unknown", timeout: true, bad: true},
		{name: "managed-config-clamp", kind: "clamp"},
		{name: "managed-config-B-effective", kind: "clamp", result: strings.Replace(clampResult, `"modelProvider":"A"`, `"modelProvider":"B"`, 1), bad: true},
		{name: "managed-config-unknown-effective", kind: "clamp", result: strings.Replace(clampResult, `"modelProvider":"A"`, `"modelProvider":"other"`, 1), bad: true},
		{name: "managed-config-missing-provider", kind: "clamp", result: strings.Replace(clampResult, `,"modelProvider":"A"`, ``, 1), bad: true},
		{name: "managed-config-missing-id", kind: "clamp", result: strings.Replace(clampResult, `"id":"child"`, `"id":""`, 1), bad: true},
		{name: "managed-config-ancestor-id", kind: "clamp", result: strings.Replace(clampResult, `"id":"child"`, `"id":"ancestor"`, 1), bad: true},
		{name: "managed-config-wrong-parent", kind: "clamp", result: strings.Replace(clampResult, `"forkedFromId":"A0"`, `"forkedFromId":"other"`, 1), bad: true},
		{name: "managed-config-missing-parent", kind: "clamp", result: strings.Replace(clampResult, `,"forkedFromId":"A0"`, ``, 1), bad: true},
		{name: "managed-config-wrong-cwd", kind: "clamp", result: strings.Replace(clampResult, `"cwd":"/private"`, `"cwd":"/other"`, 1), bad: true},
		{name: "managed-config-missing-cwd", kind: "clamp", result: strings.Replace(clampResult, `,"cwd":"/private"`, ``, 1), bad: true},
		{name: "managed-config-wrong-model", kind: "clamp", result: strings.Replace(clampResult, `"gpt-5.5"`, `"other"`, 1), bad: true},
		{name: "managed-config-missing-model", kind: "clamp", result: strings.Replace(clampResult, `,"model":"gpt-5.5"`, ``, 1), bad: true},
		{name: "managed-config-wrong-effort", kind: "clamp", result: strings.Replace(clampResult, `"high"`, `"low"`, 1), bad: true},
		{name: "managed-config-missing-effort", kind: "clamp", result: strings.Replace(clampResult, `,"reasoningEffort":"high"`, ``, 1), bad: true},
		{name: "managed-config-wrong-policy", kind: "clamp", result: strings.Replace(clampResult, `"never"`, `"on-request"`, 1), bad: true},
		{name: "managed-config-missing-policy", kind: "clamp", result: strings.Replace(clampResult, `,"approvalPolicy":"never"`, ``, 1), bad: true},
		{name: "managed-config-wrong-sandbox", kind: "clamp", result: strings.Replace(clampResult, `"readOnly"`, `"dangerFullAccess"`, 1), bad: true},
		{name: "managed-config-missing-sandbox", kind: "clamp", result: strings.Replace(clampResult, `,"sandbox":{"type":"readOnly"}`, ``, 1), bad: true},
		{name: "managed-config-undecodable", kind: "clamp", result: `[]`, bad: true},
		{name: "managed-config-rpc-error", kind: "clamp", code: -32600, message: "refused", bad: true},
		{name: "managed-config-lost-result", kind: "clamp", disconnect: true, bad: true},
		{name: "managed-config-timeout", kind: "clamp", timeout: true, bad: true},
		{name: "managed-config-child-resume-error", kind: "clamp", stage: 1, code: -32600, message: "refused", bad: true},
		{name: "managed-config-child-metadata-error", kind: "clamp", stage: 2, code: -32600, message: "refused", bad: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			limit := 2 * time.Second
			if test.timeout {
				limit = 50 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), limit)
			defer cancel()
			params := map[string]any{"threadId": "A0", "cwd": "/private", "model": "gpt-5.5", "modelProvider": "B", "approvalPolicy": "never", "sandbox": "read-only", "config": map[string]any{"model_reasoning_effort": "high", "shell_environment_policy": map[string]any{"inherit": "none"}}}
			method := "thread/resume"
			if test.kind == "unknown" {
				method, params["modelProvider"] = "thread/fork", "unknown-fixture-provider"
			}
			if test.kind == "clamp" {
				params["excludeTurns"] = true
			}
			joined := make(chan struct{})
			calls := 0
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				defer close(joined)
				defer connection.CloseNow()
				if err := handshake(connection); err != nil {
					return err
				}
				stages := 1
				if test.kind == "observe" {
					stages = 2
				}
				if test.kind == "clamp" {
					stages = 3
				}
				for stage := 0; stage < stages; stage++ {
					request, err := readObject(connection)
					if err != nil {
						return err
					}
					calls++
					wantMethod, wantParams := method, params
					if test.kind == "observe" {
						wantParams = map[string]any{"threadId": "A0", "excludeTurns": true}
						if stage == 1 {
							wantMethod = "thread/read"
						}
					}
					if test.kind == "clamp" {
						wantMethod = "thread/fork"
						wantParams = map[string]any{"threadId": "A0", "excludeTurns": true, "cwd": "/private", "model": "gpt-5.5", "approvalPolicy": "never", "sandbox": "read-only", "config": map[string]any{"model_provider": "B", "model_reasoning_effort": "high", "shell_environment_policy": map[string]any{"inherit": "none"}}}
						if stage > 0 {
							wantMethod, wantParams = "thread/resume", map[string]any{"threadId": "child", "excludeTurns": true}
							if stage == 2 {
								wantMethod = "thread/read"
							}
						}
					}
					if request["method"] != wantMethod || !reflect.DeepEqual(request["params"], wantParams) {
						return fmt.Errorf("wrong lifecycle observation/negative request: %+v", request)
					}
					if test.disconnect {
						return nil
					}
					if test.timeout {
						<-ctx.Done()
						return nil
					}
					reply := map[string]any{"id": request["id"]}
					if stage == test.stage && test.code != 0 {
						reply["error"] = map[string]any{"code": test.code, "message": test.message}
					} else {
						result := lifecycleResult
						if stage == 1 {
							result = ownedMetadata
						}
						if test.kind == "clamp" {
							result = clampResult
							if stage == 2 {
								result = clampMetadata
							}
						}
						if stage == test.stage && test.result != "" {
							result = test.result
						}
						reply["result"] = json.RawMessage(result)
					}
					if err := writeObject(connection, reply); err != nil {
						return err
					}
					if test.bad || test.code != 0 {
						if stage == test.stage {
							break
						}
					}
				}
				if request, err := readObject(connection); err == nil {
					return fmt.Errorf("unexpected retry/later RPC: %+v", request)
				}
				return nil
			})
			var resolutions []LifecycleRequest
			options := ClientOptions{}
			if test.kind == "clamp" {
				options.LifecycleRequestSettings = func(_ context.Context, request LifecycleRequest) (ThreadSettings, error) {
					resolutions = append(resolutions, request)
					return ThreadSettings{}, nil // Same empty raw resolver as native.
				}
			}
			client := NewWithOptions(socket, options)
			defer client.Close()
			var err error
			switch test.kind {
			case "observe":
				_, err = nativeForkObserveLoaded(ctx, client, "A0", "/private", "")
			case "loaded":
				err = nativeForkRefuseLoadedProvider(ctx, client, params)
			case "unknown":
				err = nativeForkRefuseUnknownProvider(ctx, client, params)
			case "clamp":
				var child nativeForkLifecycle
				child, err = nativeForkObserveManagedClamp(ctx, client, params, "/private", map[string]string{"A0": "", "ancestor": ""})
				if err == nil {
					_, err = nativeForkObserveLoaded(ctx, client, child.Thread.ID, "/private", "A0")
				}
			}
			if (err != nil) != test.bad {
				t.Fatalf("actual helper error=%v want failure=%v", err, test.bad)
			}
			client.Close()
			select {
			case <-joined:
			case <-time.After(2 * time.Second):
				t.Fatal("lifecycle recording server did not join")
			}
			wantCalls := 1
			if test.kind == "observe" && (!test.bad || test.stage == 1) {
				wantCalls = 2
			}
			if test.kind == "clamp" {
				wantCalls = 3
				if test.bad {
					wantCalls = test.stage + 1
				}
				wantResolutions := []LifecycleRequest{{Method: "thread/fork", ThreadID: "A0", Cwd: "/private"}}
				if wantCalls > 1 {
					wantResolutions = append(wantResolutions, LifecycleRequest{Method: "thread/resume", ThreadID: "child"})
				}
				if !reflect.DeepEqual(resolutions, wantResolutions) {
					t.Fatalf("raw resolver calls=%+v want=%+v", resolutions, wantResolutions)
				}
			}
			if calls != wantCalls {
				t.Fatalf("actual RPC count=%d want=%d", calls, wantCalls)
			}
		})
	}
	metadata := `{"thread":{"id":"A0","cwd":"/private","name":"N172 A0","historyMode":"paginated"}}`
	for _, test := range []struct {
		name, result, errorMethod string
		stop                      int
		lostName                  bool
	}{
		{"named-root", "", "", 5, false},
		{"name-refused", "", "thread/name/set", 1, false},
		{"name-reply-lost", "", "", 1, true},
		{"missing-metadata", `{}`, "", 2, false},
		{"null-metadata", `{"thread":null}`, "", 2, false},
		{"wrong-root-id", strings.Replace(metadata, `"A0"`, `"other"`, 1), "", 2, false},
		{"wrong-root-cwd", strings.Replace(metadata, `"/private"`, `"/other"`, 1), "", 2, false},
		{"wrong-root-name", strings.Replace(metadata, `"N172 A0"`, `"other"`, 1), "", 2, false},
		{"missing-root-name", strings.Replace(metadata, `,"name":"N172 A0"`, ``, 1), "", 2, false},
		{"legacy-root", strings.Replace(metadata, `"paginated"`, `"legacy"`, 1), "", 2, false},
		{"metadata-refused", "", "thread/read", 2, false},
		{"turns-refused", "", "thread/turns/list", 3, false},
		{"nonempty-turns", `{"data":[{"id":"unexpected"}]}`, "", 3, false},
		{"missing-turns", `{}`, "", 3, false},
		{"items-refused", "", "thread/items/list", 4, false},
		{"null-items", `{"data":null}`, "", 4, false},
		{"nonempty-items", `{"data":[{"item":{}}]}`, "", 4, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			methods := []string{"thread/start", "thread/name/set", "thread/read", "thread/turns/list", "thread/items/list", "thread/read"}
			var calls []string
			joined := make(chan struct{})
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				defer close(joined)
				defer connection.CloseNow()
				if err := handshake(connection); err != nil {
					return err
				}
				for stage := 0; stage <= test.stop; stage++ {
					request, err := readObject(connection)
					if err != nil {
						return err
					}
					method, _ := request["method"].(string)
					calls = append(calls, method)
					if method != methods[stage] {
						return fmt.Errorf("premature/repeated RPC at stage%d: %s want%s", stage, method, methods[stage])
					}
					params, _ := request["params"].(map[string]any)
					want := map[string]any{"threadId": "A0"}
					switch stage {
					case 0:
						if params["cwd"] != "/private" || params["modelProvider"] != "A" {
							return errors.New("wrong root-start options")
						}
					case 1:
						want["name"] = "N172 A0"
					case 3:
						want["limit"], want["sortDirection"], want["itemsView"] = float64(1), "asc", "notLoaded"
					case 4:
						want["limit"], want["sortDirection"] = float64(1), "asc"
					case 5:
						want["excludeTurns"] = true // Ordinary Send's first policy read.
					}
					if stage != 0 && !reflect.DeepEqual(params, want) {
						return fmt.Errorf("wrong public RPC params at stage%d: %+v", stage, params)
					}
					if stage == 1 && test.lostName {
						return nil
					}
					if method == test.errorMethod || stage == 5 {
						message := "scripted native refusal"
						if stage == 5 {
							message = "ordinary Send reached after named initialization"
						}
						if err := writeObject(connection, map[string]any{"id": request["id"], "error": map[string]any{"code": -32600, "message": message}}); err != nil {
							return err
						}
					} else {
						result := `{}`
						if stage == 0 {
							result = `{"thread":{"id":"A0","cwd":"/private"},"modelProvider":"A"}`
						}
						if stage == 2 {
							result = metadata
						}
						if stage == 3 || stage == 4 {
							result = `{"data":[],"nextCursor":null}`
						}
						if stage == test.stop && test.result != "" {
							result = test.result
						}
						if err := writeObject(connection, map[string]any{"id": request["id"], "result": json.RawMessage(result)}); err != nil {
							return err
						}
					}
				}
				if request, err := readObject(connection); err == nil {
					return fmt.Errorf("unexpected later RPC after failure/control: %+v", request)
				}
				return nil
			})
			client := New(socket)
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			id, err := client.StartThreadWithSettings(ctx, "/private", map[string]string{}, ThreadSettings{Model: "gpt-5.5", ModelProvider: "A", ReasoningEffort: "high"})
			if err != nil {
				t.Fatal(err)
			}
			err = nativeForkInitializeRoot(ctx, client, id, "/private", "N172 A0")
			if (err == nil) != (test.stop == 5) {
				t.Fatalf("initialization error=%v want success=%v", err, test.stop == 5)
			}
			if err == nil {
				_, err = client.SendWithOptions(ctx, id, "N172_A_USER", "native-fork-0172-0", "", TurnOptions{Model: "gpt-5.5", ReasoningEffort: "high", ThreadPolicy: ThreadPolicy{Sandbox: "read-only", ApprovalPolicy: "never"}})
				if err == nil || !strings.Contains(err.Error(), "ordinary Send reached after named initialization") {
					t.Fatalf("ordinary Send boundary: %v", err)
				}
			}
			client.Close()
			select {
			case <-joined:
			case <-ctx.Done():
				t.Fatal("recording server did not join")
			}
			if !slices.Equal(calls, methods[:test.stop+1]) {
				t.Fatalf("actual client calls=%v want=%v", calls, methods[:test.stop+1])
			}
		})
	}
}

func TestNativeProviderFork0172(t *testing.T) {
	binary := integrationExecutable(t, "CODEX_PROVIDER_FORK_BINARY")
	ip := integrationExecutable(t, "CODEX_PROVIDER_FORK_IP")
	packageRoot := os.Getenv("CODEX_PROVIDER_FORK_PACKAGE")
	if !filepath.IsAbs(packageRoot) || filepath.Clean(packageRoot) != packageRoot || binary != filepath.Join(packageRoot, "bin/codex") || os.Getenv("CODEX_PROVIDER_FORK_SHA256") != nativeFork0172SHA {
		t.Fatal("exact official complete package/binary/SHA required")
	}
	file, err := os.Open(binary)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	_, err = io.Copy(hash, file)
	_ = file.Close()
	if err != nil || hex.EncodeToString(hash.Sum(nil)) != nativeFork0172SHA {
		t.Fatal("official native hash mismatch")
	}
	manifest, err := os.ReadFile(filepath.Join(packageRoot, "codex-package.json"))
	var layout map[string]any
	if err != nil || json.Unmarshal(manifest, &layout) != nil || layout["version"] != "0.172.0" || layout["entrypoint"] != "bin/codex" || layout["target"] != "x86_64-unknown-linux-musl" || layout["layoutVersion"] != float64(1) {
		t.Fatal("official complete manifest mismatch")
	}
	for _, relative := range []string{"bin/codex-code-mode-host", "bin/codex-exec-server", "codex-path/rg", "codex-resources/bwrap"} {
		info, err := os.Stat(filepath.Join(packageRoot, relative))
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
			t.Fatalf("complete package resource missing: %s", relative)
		}
	}
	revision := os.Getenv("CODEX_PROVIDER_FORK_CLIENT_REVISION")
	if decoded, err := hex.DecodeString(revision); err != nil || len(decoded) != 20 || strings.ToLower(revision) != revision || os.Getenv("CODEX_PROVIDER_FORK_PROVENANCE") == "" {
		t.Fatal("exact client commit and artifact provenance required")
	}
	owner, child := integrationEnterNamespace(t, "TestNativeProviderFork0172")
	if !child {
		return
	}
	if err := syscall.Mount("tmpfs", "/etc", "tmpfs", syscall.MS_NODEV|syscall.MS_NOSUID, "mode=0755"); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command(ip, "link", "set", "lo", "up").CombinedOutput(); err != nil {
		t.Fatalf("private loopback: %v %s", err, output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	root, err := os.MkdirTemp("/tmp", "codex-provider-fork-0172-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("private synthetic evidence=%s binarySHA=%s client=%s provenance=%s", root, nativeFork0172SHA, revision, os.Getenv("CODEX_PROVIDER_FORK_PROVENANCE"))
	home, cwd, socket := filepath.Join(root, "home"), filepath.Join(root, "work"), filepath.Join(root, "native.sock")
	for _, directory := range []string{home, cwd} {
		if err := os.Mkdir(directory, 0700); err != nil {
			t.Fatal(err)
		}
	}
	log, err := os.OpenFile(filepath.Join(root, "native.log"), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	var mu sync.Mutex
	var requests []map[string]any
	counts := map[string]int{"A": 0, "B": 0}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce, enteredOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	provider := func(name string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
				w.WriteHeader(http.StatusUpgradeRequired)
				return
			}
			if r.Method != http.MethodPost || r.URL.Path != "/v1/responses" {
				http.NotFound(w, r)
				return
			}
			var request map[string]any
			decodeErr := json.NewDecoder(io.LimitReader(r.Body, 2<<20)).Decode(&request)
			index := -1
			data, _ := json.Marshal(request["input"])
			for i := len(nativeForkExchanges) - 1; i >= 0; i-- {
				if strings.Contains(string(data), nativeForkExchanges[i].user) {
					index = i
					break
				}
			}
			var check error
			if decodeErr != nil || index < 0 {
				check = errors.New("missing synthetic request marker")
			} else {
				check = nativeForkCheckWire(name, request, index)
			}
			mu.Lock()
			counts[name]++
			requests = append(requests, map[string]any{"provider": name, "request": request, "error": fmt.Sprint(check)})
			mu.Unlock()
			if check != nil {
				http.Error(w, check.Error(), http.StatusBadRequest)
				return
			}
			if index == 2 {
				enteredOnce.Do(func() { close(entered) })
				select {
				case <-release:
				case <-r.Context().Done():
					return
				case <-ctx.Done():
					return
				}
			}
			w.Header().Set("Content-Type", "text/event-stream")
			event := func(value any) { data, _ := json.Marshal(value); _, _ = fmt.Fprintf(w, "data: %s\n\n", data) }
			id := fmt.Sprintf("fork-response-%d", index)
			event(map[string]any{"type": "response.created", "response": map[string]any{"id": id}})
			event(map[string]any{"type": "response.output_item.done", "item": map[string]any{"type": "message", "role": "assistant", "id": "answer-" + id, "phase": "final_answer", "content": []any{map[string]any{"type": "output_text", "text": nativeForkExchanges[index].answer}}}})
			event(map[string]any{"type": "response.completed", "response": map[string]any{"id": id, "usage": map[string]any{"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}}})
		}))
	}
	a, b := provider("A"), provider("B")
	defer a.Close()
	defer b.Close()
	configuration := "model_provider = \"A\"\nmodel = \"gpt-5.5\"\nmodel_reasoning_effort = \"low\"\n"
	for _, p := range []struct{ name, url string }{{"A", a.URL}, {"B", b.URL}} {
		configuration += "[model_providers." + p.name + "]\nname = " + strconv.Quote(p.name) + "\nbase_url = " + strconv.Quote(p.url+"/v1") + "\nwire_api = \"responses\"\nrequires_openai_auth = false\n"
	}
	if err := os.WriteFile(filepath.Join(home, "config.toml"), []byte(configuration), 0600); err != nil {
		t.Fatal(err)
	}
	var wait *ephemeralIntegrationWait
	var client *Client
	stop := func() error {
		unblock()
		if client != nil {
			client.Close()
			client = nil
		}
		if wait == nil {
			return nil
		}
		if err := integrationStopNamespace(owner, [2]*ephemeralIntegrationWait{wait, nil}, integrationNamespaceStopOps()); err != nil {
			return err
		}
		wait = nil
		t.Log("native wait joined; private namespace empty (ECHILD)")
		return nil
	}
	defer func() {
		if err := stop(); err != nil {
			t.Errorf("namespace cleanup: %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		nativeUpgradeWriteJSON(t, filepath.Join(root, "requests.json"), requests)
		t.Logf("final provider counts=%v", counts)
	}()
	start := func() {
		t.Helper()
		command := exec.CommandContext(ctx, binary, "app-server", "--strict-config", "--listen", "unix://"+socket)
		command.Dir = cwd
		command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "CODEX_HOME=" + home, "XDG_CONFIG_HOME=" + home, "XDG_STATE_HOME=" + home, "XDG_CACHE_HOME=" + home, "RUST_LOG=error"}
		command.Stdout, command.Stderr = log, log
		_ = os.Remove(socket)
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		wait = integrationCommandWait(command)
		startup, done := context.WithTimeout(ctx, 10*time.Second)
		defer done()
		for {
			if info, err := os.Stat(socket); err == nil && info.Mode()&os.ModeSocket != 0 {
				break
			}
			if finished, err := wait.result(); finished {
				t.Fatalf("native startup exited: %v", err)
			}
			select {
			case <-startup.Done():
				t.Fatal("native startup deadline")
			case <-time.After(20 * time.Millisecond):
			}
		}
		client = NewWithOptions(socket, ClientOptions{SubmissionDatabasePath: filepath.Join(root, "submissions.sqlite"), SubmissionLedgerPath: filepath.Join(root, "predecessor.json"),
			LifecycleRequestSettings: func(call context.Context, _ LifecycleRequest) (ThreadSettings, error) {
				// Explicit Send routing also covers its ordinary implicit resume.
				// Raw lifecycle negatives keep their supplied provider unchanged.
				provider, _ := call.Value(nativeForkRouteKey{}).(string)
				return ThreadSettings{ModelProvider: provider}, nil
			}})
	}
	rpc := func(method string, params any, response any) error {
		call, done := context.WithTimeout(ctx, 20*time.Second)
		defer done()
		err := client.Request(call, method, params, response)
		if err != nil {
			data, _ := RPCErrorData(err)
			t.Logf("native-error method=%s error=%v data=%s", method, err, data)
		}
		return err
	}
	settings := func(provider, effort string) map[string]any {
		return map[string]any{"cwd": cwd, "model": "gpt-5.5", "modelProvider": provider, "approvalPolicy": "never", "sandbox": "read-only", "config": map[string]any{"model_reasoning_effort": effort, "shell_environment_policy": map[string]any{"inherit": "none"}}}
	}
	parents := map[string]string{}
	lifecycle := func(method, source, provider, effort, rootName string) string {
		t.Helper()
		params := settings(provider, effort)
		if source != "" {
			params["threadId"], params["excludeTurns"] = source, true
		}
		var response nativeForkLifecycle
		if err := rpc(method, params, &response); err != nil {
			t.Fatalf("phase=%s provider=%s source=%s: %v", method, provider, source, err)
		}
		if err := nativeForkCheckLifecycle(response, provider, effort, cwd, source); err != nil {
			t.Fatal(err)
		}
		if method == "thread/start" {
			call, done := context.WithTimeout(ctx, 20*time.Second)
			err := nativeForkInitializeRoot(call, client, response.Thread.ID, cwd, rootName)
			done()
			if err != nil {
				data, _ := RPCErrorData(err)
				t.Fatalf("phase=root-initialization thread=%s name=%s: %v data=%s", response.Thread.ID, rootName, err, data)
			}
		}
		if _, duplicate := parents[response.Thread.ID]; duplicate {
			t.Fatal("lifecycle reused an existing native identity")
		}
		parents[response.Thread.ID] = source
		t.Logf("phase=%s source=%s effective=%+v", method, source, response)
		return response.Thread.ID
	}
	send := func(thread string, index int, complete bool) SendReceipt {
		t.Helper()
		e := nativeForkExchanges[index]
		call, done := context.WithTimeout(ctx, 20*time.Second)
		defer done()
		receipt, err := client.SendWithOptions(context.WithValue(call, nativeForkRouteKey{}, e.provider), thread, e.user, fmt.Sprintf("native-fork-0172-%d", index), "", TurnOptions{Model: "gpt-5.5", ReasoningEffort: e.effort, ThreadPolicy: ThreadPolicy{Sandbox: "read-only", ApprovalPolicy: "never"}})
		if err != nil {
			data, _ := RPCErrorData(err)
			t.Fatalf("phase=send marker=%s thread=%s: %v data=%s", e.user, thread, err, data)
		}
		if complete {
			nativeUpgradeWaitTurn(t, call, client, thread, receipt, e.user, e.answer)
		}
		return receipt
	}
	count := func() map[string]int {
		mu.Lock()
		defer mu.Unlock()
		return map[string]int{"A": counts["A"], "B": counts["B"]}
	}
	refusal := func(method string, params map[string]any, category string) {
		t.Helper()
		before := count()
		var response any
		err := rpc(method, params, &response)
		var native *rpcCallError
		if !errors.As(err, &native) || native.code == -32601 || !strings.Contains(strings.ToLower(native.message), category) {
			t.Fatalf("phase=expected-refusal method=%s unexpected response=%v error=%v", method, response, err)
		}
		if !reflect.DeepEqual(before, count()) {
			t.Fatal("refused request reached inference")
		}
		t.Logf("refusal method=%s code=%d message=%s data=%s counts=%v", method, native.code, native.message, native.data, before)
	}
	history := func(thread string, index int) nativeForkHistory {
		t.Helper()
		call, done := context.WithTimeout(ctx, 20*time.Second)
		metadata, err := client.ReadThreadMetadata(call, thread, true)
		done()
		if err != nil || metadata.ID != thread || metadata.Cwd != cwd || metadata.ForkedFromID != parents[thread] {
			t.Fatalf("retained identity/parent changed: %+v error=%v", metadata, err)
		}
		var turnMessages, itemMessages []map[string]any
		var complete nativeForkHistory
		for _, method := range []string{"thread/turns/list", "thread/items/list"} {
			seen, ids := map[string]bool{}, map[string]bool{}
			cursor := ""
			ended := false
			for pageNumber := 0; pageNumber < 32; pageNumber++ {
				params := map[string]any{"threadId": thread, "limit": 1, "sortDirection": "asc"}
				if method == "thread/turns/list" {
					params["itemsView"] = "full"
				}
				if cursor != "" {
					params["cursor"] = cursor
				}
				var page struct {
					Data       *[]map[string]any
					NextCursor *string
				}
				if err := rpc(method, params, &page); err != nil {
					t.Fatalf("phase=history method=%s: %v", method, err)
				}
				if page.Data == nil {
					t.Fatalf("%s missing data", method)
				}
				if err := nativeForkCursor(seen, page.NextCursor, len(*page.Data)); err != nil {
					t.Fatal(err)
				}
				if method == "thread/turns/list" {
					complete.Turns = append(complete.Turns, (*page.Data)...)
				} else {
					complete.Items = append(complete.Items, (*page.Data)...)
				}
				for _, entry := range *page.Data {
					item := entry
					if method == "thread/items/list" {
						item, _ = entry["item"].(map[string]any)
						if stringValue(entry["turnId"]) == "" {
							t.Fatal("missing item turn identity")
						}
					}
					id := stringValue(item["id"])
					if id == "" || ids[id] {
						t.Fatal("missing/duplicate paginated identity")
					}
					ids[id] = true
					items := []any{item}
					if method == "thread/turns/list" {
						if item["status"] != "completed" {
							t.Fatalf("incomplete history turn: %v", item["status"])
						}
						items, _ = item["items"].([]any)
					}
					for _, raw := range items {
						value, _ := raw.(map[string]any)
						kind := stringValue(value["type"])
						var message map[string]any
						if kind == "userMessage" {
							message = map[string]any{"role": "user", "content": value["content"]}
						} else if kind == "agentMessage" {
							message = map[string]any{"role": "assistant", "content": []any{map[string]any{"text": value["text"]}}}
						} else {
							continue
						}
						if method == "thread/turns/list" {
							turnMessages = append(turnMessages, message)
						} else {
							itemMessages = append(itemMessages, message)
						}
					}
				}
				if page.NextCursor == nil {
					ended = true
					break
				}
				cursor = *page.NextCursor
			}
			if !ended {
				t.Fatal("history pagination exceeded finite cap")
			}
		}
		got := nativeForkMarkers(turnMessages)
		if !slices.Equal(got, nativeForkExpected(index, true)) || !slices.Equal(got, nativeForkMarkers(itemMessages)) {
			t.Fatalf("history thread=%s turns=%v items=%v want=%v", thread, got, nativeForkMarkers(itemMessages), nativeForkExpected(index, true))
		}
		t.Logf("complete paginated history thread=%s markers=%v", thread, got)
		return complete
	}
	start()
	call, done := context.WithTimeout(ctx, 20*time.Second)
	models, err := client.ListModels(call)
	done()
	if err != nil {
		t.Fatalf("phase=advertised-models: %v", err)
	}
	advertised := false
	for _, model := range models {
		if model.Model == "gpt-5.5" {
			low, high := false, false
			for _, effort := range model.SupportedReasoningEfforts {
				low = low || effort.ReasoningEffort == "low"
				high = high || effort.ReasoningEffort == "high"
			}
			advertised = low && high
		}
	}
	if !advertised {
		t.Fatal("selected native does not advertise gpt-5.5 low/high")
	}
	a0 := lifecycle("thread/start", "", "A", "high", "N172 A0")
	other := lifecycle("thread/start", "", "A", "low", "N172 Aother")
	if other == a0 {
		t.Fatal("independent A conversation reused identity")
	}
	send(a0, 0, true)
	originalA := history(a0, 0)
	b1 := lifecycle("thread/fork", a0, "B", "high", "")
	send(b1, 1, true)
	history(b1, 1)
	held := send(b1, 2, false)
	barrier, barrierDone := context.WithTimeout(ctx, 20*time.Second)
	select {
	case <-entered:
	case <-barrier.Done():
		t.Fatal("B barrier not reached")
	}
	barrierDone()
	cutoff := settings("B", "high")
	cutoff["threadId"], cutoff["lastTurnId"] = b1, held.TurnID
	refusal("thread/fork", cutoff, "turn")
	send(other, 5, true)
	if got := count(); got["A"] != 2 || got["B"] != 2 {
		t.Fatalf("concurrent routing counts=%v", got)
	}
	unblock()
	call, done = context.WithTimeout(ctx, 20*time.Second)
	nativeUpgradeWaitTurn(t, call, client, b1, held, nativeForkExchanges[2].user, nativeForkExchanges[2].answer)
	done()
	originalB := history(b1, 2)
	a2 := lifecycle("thread/fork", b1, "A", "high", "")
	if a2 == a0 || a2 == other {
		t.Fatal("return fork reused an ancestor/other ID")
	}
	send(a2, 3, true)
	history(a2, 3)
	observe := func(thread string, index int) struct {
		Loaded  nativeForkLoadedState
		History nativeForkHistory
	} {
		t.Helper()
		call, done := context.WithTimeout(ctx, 20*time.Second)
		loaded, err := nativeForkObserveLoaded(call, client, thread, cwd, parents[thread])
		done()
		if err != nil {
			t.Fatalf("phase=loaded-lifecycle-observation: %v", err)
		}
		return struct {
			Loaded  nativeForkLoadedState
			History nativeForkHistory
		}{loaded, history(thread, index)}
	}
	// A0 remains loaded/subscribed on this connection. A no-override lifecycle
	// observation reports its live policy; no expected settings are reapplied.
	beforeState := observe(a0, 0)
	beforeCounts := count()
	params := settings("B", "high")
	params["threadId"] = a0
	call, done = context.WithTimeout(ctx, 20*time.Second)
	err = nativeForkRefuseLoadedProvider(call, client, params)
	done()
	if err != nil {
		t.Fatalf("phase=loaded-provider-mismatch: %v", err)
	}
	if !reflect.DeepEqual(beforeState, observe(a0, 0)) || !reflect.DeepEqual(beforeCounts, count()) {
		t.Fatal("loaded provider mismatch changed owned identity/settings/policy/history or reached inference")
	}
	t.Logf("loaded provider mismatch requested=B effective=A; caller result undecoded; owned state unchanged counts=%v", beforeCounts)
	params = settings("unknown-fixture-provider", "high")
	params["threadId"] = a0
	beforeCounts = count()
	call, done = context.WithTimeout(ctx, 20*time.Second)
	err = nativeForkRefuseUnknownProvider(call, client, params)
	done()
	if err != nil {
		t.Fatalf("phase=unknown-provider-fork: %v", err)
	}
	if !reflect.DeepEqual(beforeCounts, count()) {
		t.Fatal("unknown-provider fork reached inference")
	}
	t.Logf("unknown-provider fork refused code=-32600 provider=unknown-fixture-provider counts=%v", beforeCounts)
	cutoff = settings("A", "high")
	cutoff["threadId"], cutoff["lastTurnId"] = a2, "00000000-0000-0000-0000-000000000000"
	refusal("thread/fork", cutoff, "turn")
	if !reflect.DeepEqual(originalA, history(a0, 0)) || !reflect.DeepEqual(originalB, history(b1, 2)) {
		t.Fatal("ancestor history changed")
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll("/etc/codex", 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("/etc/codex/requirements.toml", []byte("model_provider = \"A\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	start()
	managed := lifecycle("thread/start", "", "A", "low", "N172 managed-A")
	send(managed, 6, true)
	// Establish the source baseline without expected-setting overrides, then
	// observe the one policy-clamped fork by its returned identity only.
	beforeState = observe(a2, 3)
	beforeCounts = count()
	if beforeCounts["A"] != 4 || beforeCounts["B"] != 2 {
		t.Fatalf("pre-clamp routing counts=%v", beforeCounts)
	}
	params = settings("B", "high")
	params["threadId"], params["excludeTurns"] = a2, true
	call, done = context.WithTimeout(ctx, 20*time.Second)
	clamped, err := nativeForkObserveManagedClamp(call, client, params, cwd, parents)
	done()
	if err != nil {
		t.Fatalf("phase=managed-config-provider-clamp: %v", err)
	}
	parents[clamped.Thread.ID] = a2
	childState := observe(clamped.Thread.ID, 3)
	if !reflect.DeepEqual(beforeState.History, childState.History) || !reflect.DeepEqual(beforeState, observe(a2, 3)) ||
		!reflect.DeepEqual(beforeCounts, count()) {
		t.Fatal("managed clamp changed inherited/source history, owned source state or inference counts")
	}
	t.Logf("managed config provider clamp requested=B effective=A child=%s source=%s; inherited history and source state unchanged counts=%v", clamped.Thread.ID, a2, beforeCounts)
	params = settings("A", "high")
	params["threadId"] = a2
	var resumed nativeForkLifecycle
	if err := rpc("thread/resume", params, &resumed); err != nil {
		t.Fatalf("phase=restart-resume: %v", err)
	}
	if resumed.Thread.ID != a2 {
		t.Fatal("restart changed active native identity")
	}
	if err := nativeForkCheckLifecycle(resumed, "A", "high", cwd, b1); err != nil {
		t.Fatal(err)
	}
	history(a2, 3)
	send(a2, 4, true)
	history(a2, 4)
	if got := count(); got["A"] != 5 || got["B"] != 2 {
		t.Fatalf("final routing counts=%v", got)
	}
}

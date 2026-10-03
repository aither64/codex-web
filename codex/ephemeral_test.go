package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Full unchanged Codex 0.160.0 source catalog, not a reduced model fixture.
// Source: codex-rs/models-manager/models.json, SHA-256 ephemeralCatalogHash.
var ephemeralCatalogFixtures sync.Map

func ephemeralTestCatalog(t *testing.T) string {
	t.Helper()
	if path, ok := ephemeralCatalogFixtures.Load(t); ok {
		return path.(string)
	}
	content, err := os.ReadFile("testdata/ephemeral-models.json")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "models.json")
	if err := os.WriteFile(path, content, 0444); err != nil {
		t.Fatal(err)
	}
	ephemeralCatalogFixtures.Store(t, path)
	t.Cleanup(func() { ephemeralCatalogFixtures.Delete(t) })
	return path
}

func ephemeralTestOptions(t *testing.T) EphemeralTurnOptions {
	t.Helper()
	parent := t.TempDir()
	if err := os.Chmod(parent, 0700); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(parent, "cwd")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parent, "instructions.txt"), []byte(EphemeralInstructionFileContent), 0600); err != nil {
		t.Fatal(err)
	}
	return EphemeralTurnOptions{ModelCatalogFile: ephemeralTestCatalog(t), Directory: directory, InstructionFile: filepath.Join(parent, "instructions.txt"), Model: "gpt-5.5", Effort: "low", Instructions: "Return the requested JSON.", Input: "raw input", OutputSchema: json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"],"additionalProperties":false}`)}
}

type ephemeralFake struct {
	mu           sync.Mutex
	calls        []map[string]any
	config       any
	requirements any
	models       any
	start        func(map[string]any)
	turn         func(*websocket.Conn, map[string]any) error
	stopAt       string
	delayAt      string
}

func (f *ephemeralFake) snapshot() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]any(nil), f.calls...)
}
func (f *ephemeralFake) serve(t *testing.T) string {
	return serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		defer connection.CloseNow()
		if err := handshake(connection); err != nil {
			return err
		}
		for {
			call, err := readObject(connection)
			if err != nil {
				return nil
			} // expected private close on every exit
			f.mu.Lock()
			f.calls = append(f.calls, call)
			f.mu.Unlock()
			method, _ := call["method"].(string)
			if method == f.stopAt {
				return nil
			}
			if method == f.delayAt {
				<-time.After(180 * time.Millisecond)
			}
			params, _ := call["params"].(map[string]any)
			var result any
			switch method {
			case "config/read":
				result = map[string]any{"config": map[string]any{"model_catalog_json": ephemeralTestCatalog(t), "mcp_servers": map[string]any{"dot.name": map[string]any{"command": "secret-command"}, "other": map[string]any{"url": "secret-url"}}}, "origins": map[string]any{"model_catalog_json": map[string]any{"name": map[string]any{"type": "sessionFlags"}}}}
				if f.config != nil {
					result = f.config
				}
			case "configRequirements/read":
				result = map[string]any{"requirements": nil}
				if f.requirements != nil {
					result = f.requirements
				}
			case "model/list":
				result = map[string]any{"data": []any{map[string]any{"model": "gpt-5.5", "supportedReasoningEfforts": []any{map[string]any{"reasoningEffort": "low"}}}}, "nextCursor": nil}
				if f.models != nil {
					result = f.models
				}
			case "thread/start":
				result = map[string]any{"model": params["model"], "cwd": params["cwd"], "approvalPolicy": "never", "sandbox": map[string]any{"type": "readOnly", "networkAccess": false}, "thread": map[string]any{"id": "utility-thread", "cwd": params["cwd"], "ephemeral": true, "path": nil, "environments": []any{}}}
				if f.start != nil {
					f.start(result.(map[string]any))
				}
			case "turn/start":
				if f.turn != nil {
					if err := f.turn(connection, call); err != nil {
						return err
					}
					continue
				}
				if err := ephemeralWriteFinal(connection, `{"name":"fix runtime session names"}`, "final_answer", "utility-thread", "utility-turn"); err != nil {
					return err
				}
				result = map[string]any{"turn": map[string]any{"id": "utility-turn", "status": "inProgress", "items": []any{}}}
			case "turn/interrupt":
				result = map[string]any{}
			case "thread/unsubscribe":
				result = map[string]any{"status": "unsubscribed"}
			case "account/rateLimits/read":
				result = map[string]any{"rateLimits": map[string]any{}}
			default:
				return fmt.Errorf("unexpected fake request %s", method)
			}
			if err := writeObject(connection, map[string]any{"id": call["id"], "result": result}); err != nil {
				return nil
			}
		}
	})
}
func ephemeralTurnResponse(connection *websocket.Conn, call map[string]any) error {
	return writeObject(connection, map[string]any{"id": call["id"], "result": map[string]any{"turn": map[string]any{"id": "utility-turn", "status": "inProgress", "items": []any{}}}})
}
func ephemeralWriteFinal(connection *websocket.Conn, text, phase, threadID, turnID string) error {
	item := map[string]any{"type": "agentMessage", "id": "answer", "text": text, "phase": phase}
	if err := writeObject(connection, map[string]any{"method": "item/completed", "params": map[string]any{"threadId": threadID, "turnId": turnID, "completedAtMs": 1, "item": item}}); err != nil {
		return err
	}
	return writeObject(connection, map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": threadID, "turn": map[string]any{"id": turnID, "status": "completed", "items": []any{item}}}})
}
func runEphemeralTest(t *testing.T, client *Client, opts EphemeralTurnOptions) (EphemeralTurnResult, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return client.RunEphemeralTurn(ctx, opts)
}

func TestEphemeralEarlyCompletionUsesPrivatePolicy(t *testing.T) {
	f := &ephemeralFake{}
	socket := f.serve(t)
	parent := NewWithOptions(socket, ClientOptions{DeveloperInstructions: "parent-team-secret", RuntimeWorkspaceRoots: []string{"/workspace"}, SubmissionLedgerPath: filepath.Join(t.TempDir(), "ledger"), NonBlockingUserInput: &NonBlockingUserInputPolicy{HiddenGrace: time.Millisecond, VisibleCountdown: time.Millisecond}})
	defer parent.Close()
	opts := ephemeralTestOptions(t)
	result, err := runEphemeralTest(t, parent, opts)
	if err != nil || result.Text != `{"name":"fix runtime session names"}` {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	calls := f.snapshot()
	want := []string{"config/read", "configRequirements/read", "model/list", "thread/start", "turn/start", "thread/unsubscribe"}
	if len(calls) != len(want) {
		t.Fatalf("calls=%#v", calls)
	}
	for i, method := range want {
		if calls[i]["method"] != method {
			t.Fatalf("call %d=%#v", i, calls[i])
		}
	}
	start := calls[3]["params"].(map[string]any)
	if start["baseInstructions"] != opts.Instructions || start["developerInstructions"] != "" || start["ephemeral"] != true || start["allowProviderModelFallback"] != false {
		t.Fatalf("start=%#v", start)
	}
	for _, key := range []string{"environments", "runtimeWorkspaceRoots", "selectedCapabilityRoots", "dynamicTools"} {
		if len(start[key].([]any)) != 0 {
			t.Fatalf("nonempty %s", key)
		}
	}
	policy := start["config"].(map[string]any)
	if policy["model_instructions_file"] != opts.InstructionFile || policy["experimental_compact_prompt_file"] != opts.InstructionFile || policy["compact_prompt"] != "" {
		t.Fatal("private file overrides or explicit compact override changed")
	}
	content, err := os.ReadFile(opts.InstructionFile)
	if err != nil || string(content) != EphemeralInstructionFileContent {
		t.Fatal("instruction file changed during helper teardown")
	}
	servers := policy["mcp_servers"].(map[string]any)
	if len(servers) != 2 || servers["dot.name"].(map[string]any)["enabled"] != false || servers["other"].(map[string]any)["enabled"] != false {
		t.Fatalf("literal MCP names=%#v", servers)
	}
	encoded, _ := json.Marshal(calls)
	if strings.Contains(string(encoded), "parent-team-secret") || strings.Contains(string(encoded), "secret-command") || strings.Contains(string(encoded), "secret-url") {
		t.Fatal("parent policy/config leaked")
	}
	if parent.connection != nil || len(parent.requests) != 0 || len(parent.notices) != 0 || len(parent.watched) != 0 {
		t.Fatal("parent acquired utility state")
	}
	if _, err := os.Stat(parent.queueLedgerPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ledger exists: %v", err)
	}
}

func TestEphemeralRejectsServerRequestsBeforePromptAdmission(t *testing.T) {
	for _, method := range []string{"item/tool/requestUserInput", "item/commandExecution/requestApproval", "item/tool/call", "unknown/request"} {
		t.Run(method, func(t *testing.T) {
			rejected := make(chan map[string]any, 1)
			f := &ephemeralFake{turn: func(connection *websocket.Conn, call map[string]any) error {
				if err := ephemeralTurnResponse(connection, call); err != nil {
					return err
				}
				if err := writeObject(connection, map[string]any{"id": "question-1", "method": method, "params": map[string]any{"threadId": "utility-thread", "turnId": "utility-turn", "itemId": "item-1", "questions": []any{map[string]any{"id": "q", "header": "Question", "question": "synthetic secret?"}}}}); err != nil {
					return err
				}
				response, err := readObject(connection)
				if err != nil {
					return err
				}
				rejected <- response
				// A later successful answer cannot erase the terminal isolation failure.
				_ = ephemeralWriteFinal(connection, `{"name":"ignore forbidden question"}`, "final_answer", "utility-thread", "utility-turn")
				return nil
			}}
			parent := newTestClient(f.serve(t))
			defer parent.Close()
			_, err := runEphemeralTest(t, parent, ephemeralTestOptions(t))
			if !errors.Is(err, ErrEphemeralIsolation) {
				t.Fatalf("err=%v", err)
			}
			select {
			case response := <-rejected:
				failure, ok := response["error"].(map[string]any)
				if !ok || response["id"] != "question-1" || failure["code"] != float64(-32601) || response["result"] != nil {
					t.Fatalf("response=%#v", response)
				}
				expected := "Server requests are unavailable for ephemeral utility turns"
				if method == "item/tool/requestUserInput" {
					expected = "Questions are unavailable for ephemeral utility turns"
				}
				if failure["message"] != expected {
					t.Fatalf("message=%#v", failure)
				}
			case <-time.After(time.Second):
				t.Fatal("no immediate error response")
			}
			if len(parent.Prompts("utility-thread")) != 0 {
				t.Fatal("utility question admitted")
			}
			calls := f.snapshot()
			foundInterrupt := false
			for _, call := range calls {
				if call["method"] == "turn/interrupt" {
					foundInterrupt = true
					params := call["params"].(map[string]any)
					if params["turnId"] != "utility-turn" || params["threadId"] != "utility-thread" {
						t.Fatalf("cleanup adopted identity: %#v", params)
					}
				}
			}
			if !foundInterrupt {
				t.Fatal("known turn not interrupted")
			}
		})
	}
}

func TestEphemeralRejectsAmbiguousOrUnprovenCompletion(t *testing.T) {
	cases := []struct {
		name string
		send func(*websocket.Conn) error
		want error
	}{
		{"stale thread", func(c *websocket.Conn) error {
			return ephemeralWriteFinal(c, "secret-output", "final_answer", "other-thread", "utility-turn")
		}, ErrEphemeralProtocol},
		{"stale turn", func(c *websocket.Conn) error {
			return ephemeralWriteFinal(c, "secret-output", "final_answer", "utility-thread", "other-turn")
		}, ErrEphemeralProtocol},
		{"async notification question", func(c *websocket.Conn) error {
			return writeObject(c, map[string]any{"method": "item/completed", "params": map[string]any{"threadId": "utility-thread", "turnId": "utility-turn", "item": map[string]any{"type": "agentMessage", "id": "q", "phase": "final_answer", "delivery": "async", "questions": []any{map[string]any{"title": "question"}}, "text": `{"name":"should never become session"}`}}})
		}, ErrEphemeralIsolation},
		{"missing phase", func(c *websocket.Conn) error {
			return ephemeralWriteFinal(c, "secret-output", "", "utility-thread", "utility-turn")
		}, ErrEphemeralProtocol},
		{"output overflow", func(c *websocket.Conn) error {
			return ephemeralWriteFinal(c, strings.Repeat("a", ephemeralOutputLimit+1), "final_answer", "utility-thread", "utility-turn")
		}, ErrEphemeralProtocol},
		{"two final answers", func(c *websocket.Conn) error {
			return writeObject(c, map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "utility-thread", "turn": map[string]any{"id": "utility-turn", "status": "completed", "items": []any{map[string]any{"type": "agentMessage", "id": "a", "phase": "final_answer", "text": "one"}, map[string]any{"type": "agentMessage", "id": "b", "phase": "final_answer", "text": "two"}}}}})
		}, ErrEphemeralProtocol},
		{"failed turn", func(c *websocket.Conn) error {
			return writeObject(c, map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "utility-thread", "turn": map[string]any{"id": "utility-turn", "status": "failed", "items": []any{}}}})
		}, ErrEphemeralServer},
		{"duplicate keys", func(c *websocket.Conn) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			return c.Write(ctx, websocket.MessageText, []byte(`{"method":"turn/completed","method":"item/completed","params":{}}`))
		}, ErrEphemeralProtocol},
		{"malformed JSON", func(c *websocket.Conn) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			return c.Write(ctx, websocket.MessageText, []byte(`{`))
		}, ErrEphemeralProtocol},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			f := &ephemeralFake{turn: func(c *websocket.Conn, call map[string]any) error {
				if err := ephemeralTurnResponse(c, call); err != nil {
					return err
				}
				return test.send(c)
			}}
			client := New(f.serve(t))
			defer client.Close()
			result, err := runEphemeralTest(t, client, ephemeralTestOptions(t))
			if !errors.Is(err, test.want) || result.Text != "" || strings.Contains(err.Error(), "secret-output") {
				t.Fatalf("result=%#v err=%v", result, err)
			}
		})
	}
}

func TestEphemeralRestrictionsFailBeforeThreadCreation(t *testing.T) {
	cases := []struct {
		name      string
		configure func(*testing.T, *ephemeralFake)
		want      error
	}{
		{"malformed inventory", func(t *testing.T, f *ephemeralFake) {
			f.config = map[string]any{"config": map[string]any{"model_catalog_json": ephemeralTestCatalog(t), "mcp_servers": []any{}}, "origins": map[string]any{"model_catalog_json": map[string]any{"name": map[string]any{"type": "sessionFlags"}}}}
		}, ErrEphemeralIsolation},
		{"missing config", func(t *testing.T, f *ephemeralFake) { f.config = map[string]any{} }, ErrEphemeralIsolation},
		{"missing requirements", func(t *testing.T, f *ephemeralFake) { f.requirements = map[string]any{} }, ErrEphemeralIsolation},
		{"required action tool", func(t *testing.T, f *ephemeralFake) {
			f.requirements = map[string]any{"requirements": map[string]any{"featureRequirements": map[string]any{"shell_tool": true}}}
		}, ErrEphemeralIsolation},
		{"managed hook", func(t *testing.T, f *ephemeralFake) {
			f.requirements = map[string]any{"requirements": map[string]any{"hooks": map[string]any{}}}
		}, ErrEphemeralIsolation},
		{"managed instructions", func(t *testing.T, f *ephemeralFake) {
			f.requirements = map[string]any{"requirements": map[string]any{"additionalDeveloperInstructions": "managed-secret"}}
		}, ErrEphemeralIsolation},
		{"sandbox conflict", func(t *testing.T, f *ephemeralFake) {
			f.requirements = map[string]any{"requirements": map[string]any{"allowedSandboxModes": []any{"danger-full-access"}}}
		}, ErrEphemeralIsolation},
		{"model substitution", func(t *testing.T, f *ephemeralFake) {
			f.models = map[string]any{"data": []any{map[string]any{"model": "different-model", "supportedReasoningEfforts": []any{map[string]any{"reasoningEffort": "low"}}}}}
		}, ErrEphemeralSettings},
		{"effort substitution", func(t *testing.T, f *ephemeralFake) {
			f.models = map[string]any{"data": []any{map[string]any{"model": "gpt-5.5", "supportedReasoningEfforts": []any{map[string]any{"reasoningEffort": "high"}}}}}
		}, ErrEphemeralSettings},
		{"cursor cycle", func(t *testing.T, f *ephemeralFake) { f.models = map[string]any{"data": []any{}, "nextCursor": "same"} }, ErrEphemeralSettings},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			f := &ephemeralFake{}
			test.configure(t, f)
			client := New(f.serve(t))
			defer client.Close()
			_, err := runEphemeralTest(t, client, ephemeralTestOptions(t))
			if !errors.Is(err, test.want) {
				t.Fatalf("err=%v", err)
			}
			for _, call := range f.snapshot() {
				if call["method"] == "thread/start" {
					t.Fatal("unsafe thread created")
				}
			}
		})
	}
}

func TestEphemeralStartIdentityFailsClosed(t *testing.T) {
	for _, key := range []string{"model", "cwd", "ephemeral", "path", "environment"} {
		t.Run(key, func(t *testing.T) {
			f := &ephemeralFake{start: func(start map[string]any) {
				thread := start["thread"].(map[string]any)
				switch key {
				case "model":
					start["model"] = "substitute"
				case "cwd":
					thread["cwd"] = "/workspace"
				case "ephemeral":
					thread["ephemeral"] = false
				case "path":
					thread["path"] = "/rollout.jsonl"
				case "environment":
					thread["environments"] = []any{map[string]any{"id": "host"}}
				}
			}}
			client := New(f.serve(t))
			defer client.Close()
			_, err := runEphemeralTest(t, client, ephemeralTestOptions(t))
			if !errors.Is(err, ErrEphemeralIsolation) {
				t.Fatalf("err=%v", err)
			}
			for _, call := range f.snapshot() {
				if call["method"] == "turn/start" {
					t.Fatal("inference admitted")
				}
			}
		})
	}
}

func TestEphemeralValidationAndObserverHaveNoConnection(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*EphemeralTurnOptions)
	}{
		{"relative cwd", func(o *EphemeralTurnOptions) { o.Directory = "relative" }},
		{"nonempty cwd", func(o *EphemeralTurnOptions) {
			if err := os.WriteFile(filepath.Join(o.Directory, "AGENTS.md"), []byte("project"), 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{"open cwd", func(o *EphemeralTurnOptions) {
			if err := os.Chmod(o.Directory, 0755); err != nil {
				t.Fatal(err)
			}
		}},
		{"instruction symlink", func(o *EphemeralTurnOptions) {
			path := o.InstructionFile
			os.Remove(path)
			if err := os.Symlink("cwd", path); err != nil {
				t.Fatal(err)
			}
		}},
		{"oversize input", func(o *EphemeralTurnOptions) { o.Input = strings.Repeat("x", 64*1024+1) }},
		{"invalid utf8", func(o *EphemeralTurnOptions) { o.Input = string([]byte{0xff}) }},
		{"duplicate schema", func(o *EphemeralTurnOptions) { o.OutputSchema = json.RawMessage(`{"type":"object","type":"string"}`) }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			opts := ephemeralTestOptions(t)
			test.mutate(&opts)
			_, err := runEphemeralTest(t, New("/missing-socket"), opts)
			if !errors.Is(err, ErrEphemeralIsolation) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	opts := ephemeralTestOptions(t)
	if _, err := New("/missing-socket").RunEphemeralTurn(context.Background(), opts); !errors.Is(err, ErrEphemeralIsolation) {
		t.Fatal(err)
	}
	if _, err := runEphemeralTest(t, NewWithOptions("/missing-socket", ClientOptions{ObserverOnly: true}), opts); !errors.Is(err, ErrEphemeralIsolation) {
		t.Fatal(err)
	}
}

func TestEphemeralInstructionFileExactContentBeforeConnection(t *testing.T) {
	if EphemeralInstructionFileContent != "Preserve the current utility task and its explicit instructions.\n" {
		t.Fatal("public instruction-file bytes changed")
	}
	cases := []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{"empty", func(t *testing.T, path string) {
			if err := os.WriteFile(path, nil, 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{"whitespace", func(t *testing.T, path string) {
			if err := os.WriteFile(path, []byte(strings.Repeat(" ", len(EphemeralInstructionFileContent))), 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{"same length other text", func(t *testing.T, path string) {
			if err := os.WriteFile(path, []byte("X"+EphemeralInstructionFileContent[1:]), 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{"missing newline", func(t *testing.T, path string) {
			if err := os.WriteFile(path, []byte(strings.TrimSuffix(EphemeralInstructionFileContent, "\n")), 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{"extra bytes", func(t *testing.T, path string) {
			if err := os.WriteFile(path, []byte(EphemeralInstructionFileContent+"x"), 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{"unreadable mode", func(t *testing.T, path string) {
			if err := os.Chmod(path, 0000); err != nil {
				t.Fatal(err)
			}
		}},
		{"open mode", func(t *testing.T, path string) {
			if err := os.Chmod(path, 0644); err != nil {
				t.Fatal(err)
			}
		}},
		{"missing", func(t *testing.T, path string) {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		}},
		{"hard link", func(t *testing.T, path string) {
			if err := os.Link(path, path+".link"); err != nil {
				t.Fatal(err)
			}
		}},
		{"directory", func(t *testing.T, path string) {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(path, 0600); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			opts := ephemeralTestOptions(t)
			test.mutate(t, opts.InstructionFile)
			client := New("/missing-socket")
			defer client.Close()
			_, err := runEphemeralTest(t, client, opts)
			if err != ErrEphemeralIsolation {
				t.Fatalf("invalid private file did not fail before connection: %v", err)
			}
		})
	}
}

func TestEphemeralCancellationDiscoveryDisconnectAndOrdinaryClient(t *testing.T) {
	for _, method := range []string{"config/read", "model/list", "thread/start", "turn/start"} {
		t.Run("timeout "+method, func(t *testing.T) {
			f := &ephemeralFake{delayAt: method}
			client := New(f.serve(t))
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
			defer cancel()
			started := time.Now()
			_, err := client.RunEphemeralTurn(ctx, ephemeralTestOptions(t))
			if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 250*time.Millisecond {
				t.Fatalf("err=%v elapsed=%v", err, time.Since(started))
			}
			for _, call := range f.snapshot() {
				if call["method"] == "turn/interrupt" && method == "turn/start" {
					t.Fatal("lost start response adopted a turn")
				}
			}
		})
	}
	t.Run("disconnect never reconnects", func(t *testing.T) {
		f := &ephemeralFake{stopAt: "turn/start"}
		client := New(f.serve(t))
		defer client.Close()
		_, err := runEphemeralTest(t, client, ephemeralTestOptions(t))
		if !errors.Is(err, ErrEphemeralServer) {
			t.Fatal(err)
		}
		for _, call := range f.snapshot() {
			if call["method"] == "thread/start" && len(f.snapshot()) > 5 {
				t.Fatal("reconnect")
			}
		}
	})
	t.Run("ordinary connection remains usable", func(t *testing.T) {
		f := &ephemeralFake{}
		client := newTestClient(f.serve(t))
		defer client.Close()
		var limits AccountRateLimits
		if err := client.Request(context.Background(), "account/rateLimits/read", nil, &limits); err != nil {
			t.Fatal(err)
		}
		connection := client.connection
		generation := client.generation
		if _, err := runEphemeralTest(t, client, ephemeralTestOptions(t)); err != nil {
			t.Fatal(err)
		}
		if client.connection != connection || client.generation != generation {
			t.Fatal("parent connection changed")
		}
		if err := client.Request(context.Background(), "account/rateLimits/read", nil, &limits); err != nil {
			t.Fatal(err)
		}
	})
}

func TestEphemeralBufferAndGenerationBounds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &ephemeralSink{cancel: cancel, changed: make(chan struct{}, 1), generation: 1}
	message := rpcMessage{Method: "item/completed", Params: json.RawMessage(`{"threadId":"thread","turnId":"turn","item":{"id":"a","type":"agentMessage","phase":"final_answer","text":"ok"}}`)}
	for i := 0; i < 33; i++ {
		sink.handle(nil, nil, 1, message)
	}
	if !errors.Is(sink.failure(), ErrEphemeralProtocol) || ctx.Err() == nil {
		t.Fatal("provisional overflow accepted")
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	sink2 := &ephemeralSink{cancel: cancel2, changed: make(chan struct{}, 1), generation: 1}
	sink2.handle(nil, nil, 2, rpcMessage{})
	if !errors.Is(sink2.failure(), ErrEphemeralProtocol) || ctx2.Err() == nil {
		t.Fatal("stale generation accepted")
	}
}

func TestEphemeralAsyncStartedQuestionFailsBeforeLostTurnResponse(t *testing.T) {
	f := &ephemeralFake{turn: func(c *websocket.Conn, _ map[string]any) error {
		return writeObject(c, map[string]any{"method": "item/started", "params": map[string]any{"threadId": "utility-thread", "turnId": "utility-turn", "startedAtMs": 1, "item": map[string]any{"type": "agentMessage", "id": "q", "phase": "final_answer", "delivery": "async", "questions": []any{map[string]any{"title": "question"}}, "text": "question"}}})
	}}
	client := New(f.serve(t))
	defer client.Close()
	started := time.Now()
	_, err := runEphemeralTest(t, client, ephemeralTestOptions(t))
	if !errors.Is(err, ErrEphemeralIsolation) || time.Since(started) > 500*time.Millisecond {
		t.Fatalf("question waited for turn/start response: %v", err)
	}
	interrupted := false
	for _, call := range f.snapshot() {
		if call["method"] == "turn/interrupt" {
			params := call["params"].(map[string]any)
			if params["threadId"] != "utility-thread" || params["turnId"] != "utility-turn" {
				t.Fatal("interrupted an unowned identity")
			}
			interrupted = true
		}
	}
	if !interrupted {
		t.Fatal("early owned turn was left running after question rejection")
	}
}

func TestEphemeralLostTurnResponseCleanupIdentity(t *testing.T) {
	for _, mode := range []string{"started", "completed", "missing", "foreign-thread", "conflicting-turn", "contradictory-response"} {
		t.Run(mode, func(t *testing.T) {
			f := &ephemeralFake{turn: func(c *websocket.Conn, call map[string]any) error {
				if mode == "missing" {
					return nil
				}
				if mode == "completed" {
					return ephemeralWriteFinal(c, "answer", "final_answer", "utility-thread", "utility-turn")
				}
				notify := func(threadID, turnID string) error {
					return writeObject(c, map[string]any{"method": "turn/started", "params": map[string]any{"threadId": threadID, "turn": map[string]any{"id": turnID, "status": "inProgress", "items": []any{}}}})
				}
				if mode == "foreign-thread" {
					return notify("foreign-thread", "foreign-turn")
				}
				if err := notify("utility-thread", "utility-turn"); err != nil {
					return err
				}
				if mode == "conflicting-turn" {
					return notify("utility-thread", "another-turn")
				}
				if mode == "contradictory-response" {
					return writeObject(c, map[string]any{"id": call["id"], "result": map[string]any{"turn": map[string]any{"id": "another-turn", "status": "inProgress", "items": []any{}}}})
				}
				return nil
			}}
			client := New(f.serve(t))
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			result, err := client.RunEphemeralTurn(ctx, ephemeralTestOptions(t))
			want := ErrEphemeralProtocol
			owned := mode == "started" || mode == "completed"
			if owned || mode == "missing" {
				want = context.DeadlineExceeded
			}
			if !errors.Is(err, want) || result.Text != "" {
				t.Fatalf("result=%#v error=%v", result, err)
			}
			interrupts := 0
			for _, call := range f.snapshot() {
				if call["method"] != "turn/interrupt" {
					continue
				}
				interrupts++
				params := call["params"].(map[string]any)
				if !owned || params["threadId"] != "utility-thread" || params["turnId"] != "utility-turn" {
					t.Fatal("interrupted a missing, foreign or ambiguous identity")
				}
			}
			if owned && interrupts != 1 {
				t.Fatal("owned early identity did not receive exactly one interrupt")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &ephemeralSink{cancel: cancel, changed: make(chan struct{}, 1), generation: 1, threadID: "thread", turnIssued: true, cleanupTurnID: "turn"}
	sink.handle(nil, nil, 2, rpcMessage{})
	if sink.cleanupTurn(1, "thread", "") != "" || !errors.Is(sink.failure(), ErrEphemeralProtocol) || ctx.Err() == nil {
		t.Fatal("foreign generation retained cleanup authority")
	}
}

func TestEphemeralSinkFrameAndAggregateBounds(t *testing.T) {
	for _, mode := range []string{"depth", "frame", "aggregate", "events"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sink := &ephemeralSink{cancel: cancel, changed: make(chan struct{}, 1)}
			var message rpcMessage
			raw := []byte(`{"id":1,"result":{}}`)
			switch mode {
			case "depth":
				raw = []byte(`{"id":1,"result":` + strings.Repeat("[", 40) + "0" + strings.Repeat("]", 40) + "}")
			case "frame":
				raw = []byte(`{"id":1,"result":{"text":"` + strings.Repeat("x", ephemeralFrameLimit) + `"}}`)
			case "aggregate":
				sink.bytes = ephemeralByteLimit
			case "events":
				sink.eventCount = ephemeralEventLimit
			}
			if sink.decode(raw, &message) || !errors.Is(sink.failure(), ErrEphemeralProtocol) || ctx.Err() == nil {
				t.Fatal("unbounded frame accepted")
			}
		})
	}
}

func TestEphemeralStaticProfilePreconnection(t *testing.T) {
	for _, pair := range [][2]string{{"gpt-6-luna", "low"}, {"gpt-5.5", "high"}, {"gpt-5.5 ", "low"}, {"", "low"}} {
		t.Run(pair[0]+"/"+pair[1], func(t *testing.T) {
			opts := ephemeralTestOptions(t)
			opts.Model, opts.Effort = pair[0], pair[1]
			client := New("/missing/static-profile.sock")
			defer client.Close()
			_, err := runEphemeralTest(t, client, opts)
			if !errors.Is(err, ErrEphemeralSettings) || client.generation != 0 {
				t.Fatalf("unsupported profile: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := client.RunEphemeralTurn(ctx, opts); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if _, err := client.RunEphemeralTurn(context.Background(), opts); !errors.Is(err, ErrEphemeralIsolation) {
				t.Fatal("missing finite budget", err)
			}
		})
	}
	for _, mode := range []string{"missing", "relative", "writable", "wrong bytes", "oversized", "directory", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			opts := ephemeralTestOptions(t)
			switch mode {
			case "missing":
				opts.ModelCatalogFile += "-missing"
			case "relative":
				opts.ModelCatalogFile = "models.json"
			case "writable":
				if err := os.Chmod(opts.ModelCatalogFile, 0644); err != nil {
					t.Fatal(err)
				}
			case "wrong bytes":
				os.Chmod(opts.ModelCatalogFile, 0644)
				os.WriteFile(opts.ModelCatalogFile, []byte(`{"models":[]}`), 0444)
				os.Chmod(opts.ModelCatalogFile, 0444)
			case "oversized":
				os.Chmod(opts.ModelCatalogFile, 0644)
				os.WriteFile(opts.ModelCatalogFile, []byte(strings.Repeat("x", ephemeralCatalogLimit+1)), 0444)
				os.Chmod(opts.ModelCatalogFile, 0444)
			case "directory":
				opts.ModelCatalogFile = t.TempDir()
			case "symlink":
				path := filepath.Join(t.TempDir(), "link")
				if err := os.Symlink(opts.ModelCatalogFile, path); err != nil {
					t.Fatal(err)
				}
				opts.ModelCatalogFile = path
			}
			client := New("/missing/static-profile.sock")
			defer client.Close()
			if _, err := runEphemeralTest(t, client, opts); !errors.Is(err, ErrEphemeralIsolation) || client.generation != 0 {
				t.Fatalf("unsafe catalog: %v", err)
			}
		})
	}
}

func TestEphemeralStaticCatalogOrigin(t *testing.T) {
	for _, mode := range []string{"sessionFlags", "user", "project", "managed", "missing", "malformed", "wrong path", "config map"} {
		t.Run(mode, func(t *testing.T) {
			opts := ephemeralTestOptions(t)
			origin := any(map[string]any{"name": map[string]any{"type": mode}})
			config := map[string]any{"model_catalog_json": opts.ModelCatalogFile}
			origins := map[string]any{"model_catalog_json": origin}
			switch mode {
			case "missing":
				delete(origins, "model_catalog_json")
			case "malformed":
				origins["model_catalog_json"] = true
			case "wrong path":
				origins["model_catalog_json"] = map[string]any{"name": map[string]any{"type": "sessionFlags"}}
				config["model_catalog_json"] = "/different/models.json"
			case "config map":
				config["model_catalog_json"] = map[string]any{"path": opts.ModelCatalogFile}
			}
			f := &ephemeralFake{config: map[string]any{"config": config, "origins": origins}}
			client := New(f.serve(t))
			defer client.Close()
			_, err := runEphemeralTest(t, client, opts)
			if mode == "sessionFlags" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrEphemeralIsolation) {
				t.Fatal(err)
			}
			for _, call := range f.snapshot() {
				if call["method"] == "config/read" && call["params"].(map[string]any)["includeLayers"] != false {
					t.Fatal("full layers requested")
				}
				if mode != "sessionFlags" && call["method"] == "thread/start" {
					t.Fatal("unattested thread created")
				}
				if call["method"] == "thread/start" {
					if _, ok := call["params"].(map[string]any)["config"].(map[string]any)["model_catalog_json"]; ok {
						t.Fatal("per-thread catalog override")
					}
				}
			}
		})
	}
}

func TestEphemeralCatalogIsPublicImmutableMetadata(t *testing.T) {
	path := ephemeralTestCatalog(t)
	link := filepath.Join(t.TempDir(), "public-catalog.json")
	if err := os.Link(path, link); err != nil {
		t.Fatal(err)
	}
	if !validEphemeralCatalog(link) {
		t.Fatal("public read-only catalog acquired private-file ownership/link rules")
	}
	content, err := os.ReadFile(path)
	if err != nil || len(content) != 472512 {
		t.Fatal("full original catalog fixture changed")
	}
}

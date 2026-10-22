package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Retain only protocol identities and bounded status diagnostics, never prompts,
// provider bodies or arbitrary native configuration.
type retryNativeTrace struct {
	mu            sync.Mutex
	events        []string
	total         int
	startRequests int
	droppedTurnID string
}

func (trace *retryNativeTrace) record(direction string, data []byte) {
	var message struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params struct {
			ThreadID string `json:"threadId"`
			TurnID   string `json:"turnId"`
		} `json:"params"`
		Result struct {
			Thread struct {
				ID     string         `json:"id"`
				Status map[string]any `json:"status"`
			} `json:"thread"`
			Turn struct {
				ID     string `json:"id"`
				Status string `json:"status"`
			} `json:"turn"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(data, &message) != nil {
		return
	}
	line := fmt.Sprintf("%s id=%s method=%s thread=%s turn=%s resultThread=%s status=%s resultTurn=%s/%s",
		direction, message.ID, message.Method, message.Params.ThreadID, message.Params.TurnID,
		message.Result.Thread.ID, statusValue(message.Result.Thread.Status), message.Result.Turn.ID, message.Result.Turn.Status)
	if message.Error != nil {
		line += fmt.Sprintf(" error=%d %.512s", message.Error.Code, message.Error.Message)
	}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	trace.total++
	if direction == "request" && message.Method == "turn/start" {
		trace.startRequests++
	}
	trace.events = append(trace.events, line)
	if len(trace.events) > 128 {
		trace.events = trace.events[len(trace.events)-128:]
	}
}

// A submitted attempt may become visible in paginated history after the native
// acceptance reply. Observe only its immutable receipt; this loop cannot send.
func retryNativeReconcile(ctx context.Context, client *Client, thread, text, id, action string, options TurnOptions) (SendReceipt, error) {
	for {
		receipt, found, err := client.ReconcileSendWithOptions(ctx, thread, text, id, action, options)
		if err == nil {
			if !found {
				return SendReceipt{}, errors.New("fixture has no submitted attempt to reconcile")
			}
			return receipt, nil
		}
		var unknown *UnknownSendOutcomeError
		if !errors.As(err, &unknown) {
			return SendReceipt{}, err
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return SendReceipt{}, fmt.Errorf("native receipt remained uncertain: %w; %v", err, ctx.Err())
		case <-timer.C:
		}
	}
}

func (trace *retryNativeTrace) snapshot() string {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	return fmt.Sprintf("last %d of %d events\n%s", len(trace.events), trace.total, strings.Join(trace.events, "\n"))
}

func retryNativeDiagnostics(t *testing.T, client *Client, thread string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	metadata, readErr := client.ReadThreadMetadata(ctx, thread, true)
	loaded, loadedErr := client.LoadedThreadIDs(ctx)
	client.watchedMu.Lock()
	subscribers, generation := client.watched[thread], client.watchedGeneration[thread]
	client.watchedMu.Unlock()
	var size int64
	var statErr error
	if metadata.Path != nil {
		var info os.FileInfo
		info, statErr = os.Stat(*metadata.Path)
		if statErr == nil {
			size = info.Size()
		}
	}
	t.Logf("native setup/state: thread=%s status=%s history=%s rolloutPath=%t rolloutBytes=%d stat=%v read=%v loaded=%t loadedRead=%v localSubscribers=%d watchGeneration=%d",
		thread, statusValue(metadata.Status), metadata.HistoryMode, metadata.Path != nil, size, statErr, readErr, slices.Contains(loaded, thread), loadedErr, subscribers, generation)
}

func retryNativeStartThread(t *testing.T, ctx context.Context, client *Client, fixture *recoveryIntegrationHarness, projectID, mode string, policy ThreadPolicy) string {
	t.Helper()
	thread, err := client.StartThreadWithSettings(ctx, fixture.cwd, map[string]string{}, ThreadSettings{
		ProjectID: projectID, Model: "gpt-5.5", ReasoningEffort: "low", CollaborationMode: mode, Policy: policy,
	})
	if err != nil {
		t.Fatalf("start private %s thread: %v", mode, err)
	}
	unsubscribe := func() {}
	t.Cleanup(func() {
		if t.Failed() {
			retryNativeDiagnostics(t, client, thread)
		}
		unsubscribe()
	})
	// A fresh native thread is staged until its first history item. Use the
	// existing managed bootstrap before Send's warm resume; no original user
	// turn or failure is fabricated, and no inference occurs during setup.
	marker := "Synthetic retry fixture bootstrap for thread " + thread
	if err := client.BootstrapHeadlessThread(ctx, thread, fixture.cwd, projectID, marker); err != nil {
		t.Fatalf("materialize private thread %s: %v", thread, err)
	}
	_, stop, err := client.Subscribe(ctx, thread)
	if err != nil {
		t.Fatalf("subscribe private thread %s: %v", thread, err)
	}
	unsubscribe = stop
	state, err := client.ReadTurnState(ctx, thread)
	if err != nil || state.Status != "idle" || state.LatestTurnID != "" {
		t.Fatalf("fresh materialized thread is not loaded idle: %+v %v", state, err)
	}
	t.Logf("private thread ready: id=%s mode=%s persisted bootstrap verified, subscriber attached, no user turn", thread, mode)
	return thread
}

func TestRetryNativeFixtureSetup(t *testing.T) {
	root := t.TempDir()
	fixture := &recoveryIntegrationHarness{root: root, cwd: root}
	path := filepath.Join(root, "rollout.jsonl")
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		materialized := false
		methods := []string{"thread/start", "thread/read", "thread/inject_items", "thread/read", "thread/read", "thread/resume", "thread/read", "thread/turns/list"}
		for _, method := range methods {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			if request["method"] != method {
				return fmt.Errorf("setup RPC order: got %v, want %s", request["method"], method)
			}
			params := request["params"].(map[string]any)
			metadata := freshHeadlessMetadata(path, "project-1")
			metadata["cwd"], metadata["status"] = root, map[string]any{"type": "idle"}
			result := map[string]any{"thread": metadata}
			switch method {
			case "thread/start":
				config := params["config"].(map[string]any)
				env := config["shell_environment_policy"].(map[string]any)["set"]
				if _, ok := env.(map[string]any); !ok || params["projectId"] != "project-1" {
					return fmt.Errorf("thread setup lost explicit environment/project: %#v", params)
				}
			case "thread/inject_items":
				if materialized {
					return errors.New("fixture bootstrap repeated")
				}
				items := params["items"].([]any)
				if len(items) != 1 {
					return errors.New("fixture bootstrap did not contain one item")
				}
				item := items[0].(map[string]any)
				content := item["content"].([]any)[0].(map[string]any)
				if item["role"] != "developer" || content["text"] != "Synthetic retry fixture bootstrap for thread thread-1" {
					return errors.New("fixture fabricated user history")
				}
				encoded, err := json.Marshal(item)
				if err != nil {
					return err
				}
				rollout := fmt.Sprintf("{\"type\":\"session_meta\",\"payload\":{\"id\":\"thread-1\"}}\n{\"type\":\"response_item\",\"payload\":%s}\n", encoded)
				if err := os.WriteFile(path, []byte(rollout), 0600); err != nil {
					return err
				}
				materialized = true
			case "thread/resume":
				if !materialized {
					return errors.New("fixture resumed before native persistence")
				}
			case "thread/turns/list":
				result = map[string]any{"data": []any{}}
			}
			if err := writeObject(connection, map[string]any{"id": request["id"], "result": result}); err != nil {
				return err
			}
		}
		return nil
	})
	client := NewWithOptions(socket, ClientOptions{PreserveThreadInstructionsOnResume: true, ThreadSourceKinds: []string{"vscode"}})
	t.Cleanup(client.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if thread := retryNativeStartThread(t, ctx, client, fixture, "project-1", "default", ThreadPolicy{}); thread != "thread-1" {
		t.Fatal("fixture started the wrong native thread", thread)
	}
}

func TestRetryNativeTraceIsBoundedAndOmitsPayloads(t *testing.T) {
	trace := &retryNativeTrace{}
	for range 200 {
		trace.record("request", []byte(`{"id":7,"method":"turn/start","params":{"threadId":"thread-1","input":[{"text":"private fixture prompt"}],"config":{"secret":"private fixture config"}}}`))
	}
	trace.record("native", []byte(`{"id":7,"error":{"code":-32600,"message":"no rollout found"}}`))
	snapshot := trace.snapshot()
	if len(trace.events) != 128 || !strings.Contains(snapshot, "128 of 201") || !strings.Contains(snapshot, "thread=thread-1") || !strings.Contains(snapshot, "error=-32600 no rollout found") || strings.Contains(snapshot, "private fixture") {
		t.Fatal("unsafe or incomplete native diagnostics", snapshot)
	}
}

func TestRetryNativeReconciliationWaitsWithoutSubmitting(t *testing.T) {
	for _, variant := range []string{"delayed history", "wrong text", "absent attempt", "history stays unknown"} {
		t.Run(variant, func(t *testing.T) {
			var reads atomic.Int32
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				for {
					request, err := readObject(connection)
					if err != nil {
						return nil
					}
					if request["method"] != "thread/items/list" {
						return fmt.Errorf("reconciliation sent or mutated: %v", request["method"])
					}
					data := []any{}
					if read := reads.Add(1); variant != "history stays unknown" && (read >= 3 || variant == "wrong text") {
						text := "Synthetic visible continuation."
						if variant == "wrong text" {
							text = "Different input."
						}
						data = append(data, map[string]any{"turnId": "native-accepted", "item": map[string]any{
							"id": "input-1", "type": "userMessage", "clientId": "lost-stable",
							"content": []any{map[string]any{"type": "text", "text": text}},
						}})
					}
					if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{"data": data}}); err != nil {
						return err
					}
				}
			})
			client := NewWithOptions(socket, ClientOptions{SubmissionLedgerPath: filepath.Join(t.TempDir(), "receipts.json")})
			defer client.Close()
			options := TurnOptions{Model: "gpt-5.5", ReasoningEffort: "low"}
			if variant != "absent attempt" {
				if err := client.PrepareSendWithOptions("thread-1", "Synthetic visible continuation.", "lost-stable", "fixture:lost:failed", false, options); err != nil {
					t.Fatal(err)
				}
				normalized, _ := normalizeTurnOptions(options)
				claimed, err := client.claimContinuationAttempt("thread-1", "lost-stable", "Synthetic visible continuation.", "fixture:lost:failed", normalized.Digest)
				if err != nil || !claimed {
					t.Fatal("prepare synthetic submitting state", claimed, err)
				}
			}
			limit := time.Second
			if variant == "history stays unknown" {
				limit = 80 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), limit)
			defer cancel()
			receipt, err := retryNativeReconcile(ctx, client, "thread-1", "Synthetic visible continuation.", "lost-stable", "fixture:lost:failed", options)
			if variant == "delayed history" {
				if err != nil || receipt.TurnID != "native-accepted" || reads.Load() != 3 {
					t.Fatalf("receipt=%+v reads=%d err=%v", receipt, reads.Load(), err)
				}
			} else if variant == "history stays unknown" {
				normalized, _ := normalizeTurnOptions(options)
				attempt, _, inspectErr := client.sendAttemptWithOptions("thread-1", "lost-stable", "Synthetic visible continuation.", "fixture:lost:failed", normalized.Digest)
				if err == nil || inspectErr != nil || reads.Load() < 1 || receipt.TurnID != "" || attempt.State != "submitting" {
					t.Fatalf("uncertain receipt gained authority: %+v state=%s %v %v", receipt, attempt.State, err, inspectErr)
				}
			} else if err == nil || reads.Load() > 1 {
				t.Fatalf("non-unknown result retried: receipt=%+v reads=%d err=%v", receipt, reads.Load(), err)
			}
		})
	}
}

// retryNativeProxy forwards the real private App Server protocol unchanged.
// Its single test hook can delay a start or discard its accepted reply; it
// never manufactures native turn history, context or errors.
func retryNativeProxy(t *testing.T, ctx context.Context, fixture *recoveryIntegrationHarness, beforeStart func(), loseReply bool, traces ...*retryNativeTrace) string {
	t.Helper()
	path := filepath.Join(fixture.root, fmt.Sprintf("proxy-%d.sock", time.Now().UnixNano()))
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	record := func(direction string, data []byte) {
		for _, trace := range traces {
			trace.record(direction, data)
		}
	}
	var hooked atomic.Bool
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		front, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer front.CloseNow()
		dialer := &net.Dialer{}
		transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", fixture.socket)
		}}
		defer transport.CloseIdleConnections()
		back, _, err := websocket.Dial(ctx, "ws://localhost/", &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}})
		if err != nil {
			t.Errorf("private proxy dial: %v", err)
			return
		}
		defer back.CloseNow()
		front.SetReadLimit(readLimit)
		back.SetReadLimit(readLimit)
		var mu sync.Mutex
		var droppedID string
		done := make(chan struct{}, 2)
		go func() {
			defer func() { done <- struct{}{} }()
			for {
				kind, data, err := front.Read(ctx)
				if err != nil {
					return
				}
				record("request", data)
				var request struct {
					ID     json.RawMessage `json:"id"`
					Method string          `json:"method"`
				}
				if json.Unmarshal(data, &request) == nil && request.Method == "turn/start" && hooked.CompareAndSwap(false, true) {
					if beforeStart != nil {
						beforeStart()
					}
					if loseReply {
						mu.Lock()
						droppedID = string(request.ID)
						mu.Unlock()
					}
				}
				if back.Write(ctx, kind, data) != nil {
					return
				}
			}
		}()
		go func() {
			defer func() { done <- struct{}{} }()
			for {
				kind, data, err := back.Read(ctx)
				if err != nil {
					return
				}
				record("native", data)
				var response struct {
					ID     json.RawMessage `json:"id"`
					Result struct {
						Turn struct {
							ID string `json:"id"`
						} `json:"turn"`
					} `json:"result"`
				}
				json.Unmarshal(data, &response)
				mu.Lock()
				drop := droppedID != "" && string(response.ID) == droppedID
				mu.Unlock()
				if drop {
					if response.Result.Turn.ID == "" {
						t.Error("loss proxy did not receive native turn acceptance")
					}
					for _, trace := range traces {
						trace.mu.Lock()
						trace.droppedTurnID = response.Result.Turn.ID
						trace.mu.Unlock()
					}
					return
				}
				if front.Write(ctx, kind, data) != nil {
					return
				}
			}
		}()
		<-done
		front.CloseNow()
		back.CloseNow()
		<-done
	})}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	return path
}

type retryNativeProvider struct {
	mu          sync.Mutex
	mode        string
	requests    []map[string]any
	hold        chan struct{}
	entered     chan struct{}
	enteredOnce sync.Once
}

func (p *retryNativeProvider) set(mode string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mode = mode
	p.requests = nil
	p.hold = make(chan struct{})
	p.entered = make(chan struct{})
	p.enteredOnce = sync.Once{}
}

func (p *retryNativeProvider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/responses" {
		http.NotFound(w, r)
		return
	}
	var request map[string]any
	if json.NewDecoder(io.LimitReader(r.Body, 8*1024*1024)).Decode(&request) != nil {
		w.WriteHeader(400)
		return
	}
	p.mu.Lock()
	p.requests = append(p.requests, request)
	index := len(p.requests)
	mode, hold := p.mode, p.hold
	if mode == "hold" {
		p.enteredOnce.Do(func() { close(p.entered) })
	}
	p.mu.Unlock()
	if mode == "hold" {
		select {
		case <-hold:
		case <-r.Context().Done():
			return
		}
	}
	w.Header().Set("Content-Type", "text/event-stream")
	event := func(value any) { data, _ := json.Marshal(value); fmt.Fprintf(w, "data: %s\n\n", data) }
	id := fmt.Sprintf("synthetic-%s-%d", mode, index)
	event(map[string]any{"type": "response.created", "response": map[string]any{"id": id}})
	// Exact failure codes come from pinned codex-api/src/sse/responses_error.rs.
	// The provider emits fixtures, with no real policy-triggering request.
	if mode == "partial-capacity" && index == 1 {
		tools, _ := request["tools"].([]any)
		name := ""
		for _, raw := range tools {
			tool, _ := raw.(map[string]any)
			if tool["name"] == "exec_command" || tool["name"] == "shell_command" {
				name = stringValue(tool["name"])
				break
			}
		}
		arguments := `{"cmd":"printf partial-history >> native-retry-marker; cat native-retry-marker"}`
		if name == "shell_command" {
			arguments = `{"command":"printf partial-history >> native-retry-marker; cat native-retry-marker"}`
		}
		if name == "" {
			name = "fixture_missing_native_command_tool"
		}
		event(map[string]any{"type": "response.output_item.done", "item": map[string]any{"id": "fixture-command", "type": "function_call", "name": name, "call_id": "fixture-command", "arguments": arguments}})
		event(map[string]any{"type": "response.completed", "response": map[string]any{"id": id, "usage": map[string]any{"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}}})
		return
	}
	if mode == "capacity" || mode == "cybersecurity" || mode == "partial-capacity" {
		code := "server_is_overloaded"
		if mode == "cybersecurity" {
			code = "cyber_policy"
		}
		event(map[string]any{"type": "response.failed", "sequence_number": 3, "response": map[string]any{"id": id, "object": "response", "created_at": 1759771626, "status": "failed", "background": false, "error": map[string]any{"code": code, "message": "Synthetic fixture terminal failure."}, "incomplete_details": nil}})
		return
	}
	event(map[string]any{"type": "response.output_item.done", "item": map[string]any{"id": "answer-" + id, "type": "message", "role": "assistant", "phase": "final_answer", "content": []any{map[string]any{"type": "output_text", "text": "Synthetic fixture result."}}}})
	event(map[string]any{"type": "response.completed", "response": map[string]any{"id": id, "usage": map[string]any{"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}}})
}

func retryNativeWait(t *testing.T, ctx context.Context, client *Client, thread, turn, status string) TurnState {
	t.Helper()
	for {
		state, err := client.ReadTurnState(ctx, thread)
		quiescent := state.Status == "idle"
		if status == "failed" {
			quiescent = state.IsLoadedFailedTurn(turn)
		}
		if err == nil && state.LatestTurnID == turn && state.LatestTurnStatus == status && quiescent {
			t.Logf("native terminal tuple: thread=%s turn=%s turnStatus=%s rawThreadStatus=%s", thread, turn, state.LatestTurnStatus, state.Status)
			return state
		}
		select {
		case <-ctx.Done():
			t.Fatalf("native turn %s did not become %s: %+v %v", turn, status, state, err)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func retryNativeContext(t *testing.T, ctx context.Context, client *Client, thread, turn string) TurnContext {
	t.Helper()
	for {
		value, err := client.ReadTurnContext(ctx, thread, turn)
		if err == nil {
			return value
		}
		select {
		case <-ctx.Done():
			t.Fatalf("exact native context unavailable: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestPinnedCodexFailedTurnRetryContract(t *testing.T) {
	provider := &retryNativeProvider{}
	provider.set("success")
	fixture := newRecoveryIntegrationHarness(t, "thread_unload_delay_secs = 3600\n", "supports_websockets = false\nrequest_max_retries = 0\nstream_max_retries = 0\n", provider)
	version, err := execNativeVersion(fixture)
	if err != nil || version != "codex-cli 0.163.0" {
		t.Fatalf("fixture requires pinned Codex0.163.0: %q %v", version, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	fixture.start()
	trace := &retryNativeTrace{}
	proxy := retryNativeProxy(t, ctx, fixture, nil, false, trace)
	client := NewWithOptions(proxy, ClientOptions{SubmissionLedgerPath: filepath.Join(fixture.root, "receipts.json"), PreserveThreadInstructionsOnResume: true, ThreadSourceKinds: []string{"vscode"}})
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("bounded native RPC trace:\n%s", trace.snapshot())
		}
	})
	defer client.Close()
	policy := ThreadPolicy{Sandbox: "danger-full-access", ApprovalPolicy: "never", DeveloperInstructions: "Synthetic fixture instructions."}
	project, err := client.CreateProject(ctx, "Synthetic retry fixture", "retry-contract-project")
	if err != nil {
		t.Fatal(err)
	}
	newThread := func(t *testing.T, mode string) string {
		t.Helper()
		provider.set("success")
		thread := retryNativeStartThread(t, ctx, client, fixture, project.ID, mode, policy)
		if mode == "plan" {
			if _, err := client.UpdateThreadSettings(ctx, thread, ThreadSettingsUpdate{CollaborationMode: &mode}); err != nil {
				t.Fatalf("select private thread plan mode before user input: %v", err)
			}
		}
		return thread
	}
	failed := func(t *testing.T, thread, kind, id string) SendReceipt {
		t.Helper()
		provider.set(kind)
		receipt, err := client.SendWithOptions(ctx, thread, "Synthetic fixture request.", id, "fixture:original", TurnOptions{Model: "gpt-5.5", ReasoningEffort: "low", ThreadPolicy: policy})
		if err != nil {
			t.Fatal(err)
		}
		state := retryNativeWait(t, ctx, client, thread, receipt.TurnID, "failed")
		want := "serverOverloaded"
		if kind == "cybersecurity" {
			want = "cyberPolicy"
		}
		if state.TerminalFailure == nil || state.TerminalFailure.Code != want {
			t.Fatalf("synthetic %s mapped to %+v", kind, state.TerminalFailure)
		}
		provider.mu.Lock()
		requests := len(provider.requests)
		provider.mu.Unlock()
		wantRequests := 1
		if kind == "partial-capacity" {
			wantRequests = 2
		}
		if requests != wantRequests {
			t.Fatalf("unexpected native retry: provider requests=%d want=%d", requests, wantRequests)
		}
		return receipt
	}
	for _, mode := range []string{"default", "plan"} {
		t.Run("nonempty continuation "+mode, func(t *testing.T) {
			thread := newThread(t, mode)
			failureKind := "capacity"
			if mode == "default" {
				failureKind = "partial-capacity"
			}
			original := failed(t, thread, failureKind, "original-"+mode)
			var partial *TranscriptEntry
			if mode == "default" {
				transcript, err := client.ReadThread(ctx, thread)
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range transcript.Entries {
					if entry.Kind == "commandExecution" && strings.Contains(entry.Details, "partial-history") {
						copy := entry
						partial = &copy
					}
				}
				if partial == nil {
					t.Fatal("native failed turn did not retain its completed synthetic command output")
				}
			}
			before := retryNativeContext(t, ctx, client, thread, original.TurnID)
			if before.Model != "gpt-5.5" || before.ReasoningEffort != "low" || before.CollaborationMode != mode {
				t.Fatalf("original turn did not execute the requested fixture settings: %+v", before)
			}
			provider.set("success")
			options := TurnOptions{Model: "gpt-5.5", ReasoningEffort: "low", ThreadPolicy: policy, AdditionalContext: map[string]AdditionalContextEntry{"fixture:retry": {Value: "Synthetic retained application context.", Kind: "application"}}}
			text := "Continue the interrupted task from the saved conversation. Inspect the current state before repeating actions."
			receipt, err := client.ContinueFailedTurn(ctx, thread, text, "continue-"+mode, "fixture:retry:"+original.TurnID, original.TurnID, options)
			if err != nil {
				t.Fatal(err)
			}
			retryNativeWait(t, ctx, client, thread, receipt.TurnID, "completed")
			after := retryNativeContext(t, ctx, client, thread, receipt.TurnID)
			if before.Model != after.Model || before.ReasoningEffort != after.ReasoningEffort || before.CollaborationMode != after.CollaborationMode || string(before.ApprovalPolicy) != string(after.ApprovalPolicy) || string(before.SandboxPolicy) != string(after.SandboxPolicy) {
				t.Fatalf("execution settings changed: before=%+v after=%+v", before, after)
			}
			transcript, err := client.ReadThread(ctx, thread)
			if err != nil {
				t.Fatal(err)
			}
			originalSeen, continuationSeen := 0, 0
			for _, entry := range transcript.Entries {
				if entry.ClientUserMessageID == "original-"+mode {
					originalSeen++
				}
				if entry.ClientUserMessageID == "continue-"+mode && entry.Text == text {
					continuationSeen++
				}
			}
			if partial != nil {
				commands := 0
				for _, entry := range transcript.Entries {
					if entry.Kind == "commandExecution" {
						commands++
						if entry.ItemID != partial.ItemID || entry.Details != partial.Details {
							t.Fatal("continuation changed partial command history")
						}
					}
				}
				marker, err := os.ReadFile(filepath.Join(fixture.cwd, "native-retry-marker"))
				if err != nil || string(marker) != "partial-history" {
					t.Fatalf("synthetic side effect repeated or lost: %q %v", marker, err)
				}
				if commands != 1 {
					t.Fatalf("partial command replayed: %d command items", commands)
				}
			}
			if originalSeen != 1 || continuationSeen != 1 {
				t.Fatalf("history identities duplicated/lost: original=%d continuation=%d", originalSeen, continuationSeen)
			}
			provider.mu.Lock()
			requests, _ := json.Marshal(provider.requests)
			provider.mu.Unlock()
			if !strings.Contains(string(requests), "Synthetic retained application context.") {
				t.Fatal("additional application context did not reach synthetic provider")
			}
		})
	}
	t.Run("exact cyber terminal", func(t *testing.T) { failed(t, newThread(t, "default"), "cybersecurity", "cyber-original") })
	t.Run("automatic queue uses execution context", func(t *testing.T) {
		thread := newThread(t, "default")
		provider.set("hold")
		_, err := client.SendWithOptions(ctx, thread, "Hold synthetic fixture.", "queue-active", "fixture:active", TurnOptions{Model: "gpt-5.5", ReasoningEffort: "low", ThreadPolicy: policy})
		if err != nil {
			t.Fatal(err)
		}
		provider.mu.Lock()
		entered, release := provider.entered, provider.hold
		provider.mu.Unlock()
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("native active provider did not enter")
		}
		if _, err := client.QueueWithPolicy(ctx, thread, "Queued synthetic fixture.", "queued-exact", policy); err != nil {
			t.Fatal(err)
		}
		high := "high"
		if _, err := client.UpdateThreadSettings(ctx, thread, ThreadSettingsUpdate{ReasoningEffort: &high}); err != nil {
			t.Fatal(err)
		}
		close(release)
		var queuedTurn string
		for queuedTurn == "" {
			transcript, err := client.ReadThread(ctx, thread)
			if err == nil {
				for _, entry := range transcript.Entries {
					if entry.ClientUserMessageID == "queued-exact" {
						queuedTurn = entry.TurnID
					}
				}
			}
			if queuedTurn == "" {
				select {
				case <-ctx.Done():
					t.Fatal("native queue did not execute automatically")
				case <-time.After(20 * time.Millisecond):
				}
			}
		}
		retryNativeWait(t, ctx, client, thread, queuedTurn, "completed")
		value := retryNativeContext(t, ctx, client, thread, queuedTurn)
		if value.ReasoningEffort != "high" {
			t.Fatalf("used enqueue-time defaults instead of execution effort: %+v", value)
		}
	})
	t.Run("lost accepted reply recreated client", func(t *testing.T) {
		thread := newThread(t, "default")
		original := failed(t, thread, "capacity", "lost-original")
		before := retryNativeContext(t, ctx, client, thread, original.TurnID)
		provider.set("success")
		lostTrace := &retryNativeTrace{}
		proxy := retryNativeProxy(t, ctx, fixture, nil, true, lostTrace)
		path := filepath.Join(fixture.root, "lost-ledger.json")
		first := NewWithOptions(proxy, ClientOptions{SubmissionLedgerPath: path})
		defer first.Close()
		options := TurnOptions{Model: "gpt-5.5", ReasoningEffort: "low", ThreadPolicy: policy}
		_, err := first.ContinueFailedTurn(ctx, thread, "Synthetic visible continuation.", "lost-stable", "fixture:lost:"+original.TurnID, original.TurnID, options)
		if err != nil {
			var unknown *UnknownSendOutcomeError
			if !errors.As(err, &unknown) {
				t.Fatalf("lost reply was not uncertain: %v", err)
			}
		}
		first.Close()
		lostTrace.mu.Lock()
		acceptedTurn, submissions := lostTrace.droppedTurnID, lostTrace.startRequests
		lostTrace.mu.Unlock()
		if acceptedTurn == "" || submissions != 1 {
			t.Fatalf("loss fixture did not drop exactly one accepted start: turn=%s starts=%d", acceptedTurn, submissions)
		}
		recreated := NewWithOptions(fixture.socket, ClientOptions{SubmissionLedgerPath: path})
		defer recreated.Close()
		// Native acceptance can precede user-input history publication. Keep the
		// exact submitting tuple and observe its receipt; never submit again to
		// make history appear. This bound fits inside the original scenario limit.
		reconcileCtx, stopReconciliation := context.WithTimeout(ctx, 5*time.Second)
		defer stopReconciliation()
		receipt, err := retryNativeReconcile(reconcileCtx, recreated, thread, "Synthetic visible continuation.", "lost-stable", "fixture:lost:"+original.TurnID, options)
		if err != nil {
			t.Fatal(err)
		}
		if receipt.TurnID != acceptedTurn || receipt.ClientUserMessageID != "lost-stable" {
			t.Fatalf("reconciliation changed accepted identity: %+v native=%s", receipt, acceptedTurn)
		}
		retryNativeWait(t, ctx, client, thread, receipt.TurnID, "completed")
		after := retryNativeContext(t, ctx, client, thread, receipt.TurnID)
		if before.Model != after.Model || before.ReasoningEffort != after.ReasoningEffort || before.CollaborationMode != after.CollaborationMode || string(before.ApprovalPolicy) != string(after.ApprovalPolicy) || string(before.SandboxPolicy) != string(after.SandboxPolicy) {
			t.Fatalf("lost-reply continuation changed settings: before=%+v after=%+v", before, after)
		}
		again, err := recreated.ContinueFailedTurn(ctx, thread, "Synthetic visible continuation.", "lost-stable", "fixture:lost:"+original.TurnID, original.TurnID, options)
		if err != nil || again != receipt {
			t.Fatalf("same-ID accepted reconciliation changed receipt: %+v %v", again, err)
		}
		transcript, err := client.ReadThread(ctx, thread)
		if err != nil {
			t.Fatal(err)
		}
		seen, originalSeen := 0, 0
		for _, entry := range transcript.Entries {
			if entry.ClientUserMessageID == "lost-original" {
				originalSeen++
				if entry.TurnID != original.TurnID || entry.Text != "Synthetic fixture request." || entry.TurnStatus != "failed" {
					t.Fatalf("lost-reply recovery changed original failed history: %+v", entry)
				}
			}
			if entry.ClientUserMessageID == "lost-stable" {
				seen++
				if entry.TurnID != acceptedTurn || entry.Text != "Synthetic visible continuation." {
					t.Fatalf("lost receipt matched different input: %+v", entry)
				}
			}
		}
		if seen != 1 || originalSeen != 1 {
			t.Fatalf("lost-reply native inputs: continuation=%d original=%d want=1 each", seen, originalSeen)
		}
		provider.mu.Lock()
		count := len(provider.requests)
		provider.mu.Unlock()
		if count != 1 {
			t.Fatalf("lost reply caused %d provider requests", count)
		}
	})
	t.Run("native direct busy race compatibility", func(t *testing.T) {
		thread := newThread(t, "default")
		original := failed(t, thread, "capacity", "busy-original")
		originalState, err := client.ReadTurnState(ctx, thread)
		if err != nil || originalState.TerminalFailure == nil {
			t.Fatal("original failed history unavailable", originalState, err)
		}
		provider.set("hold")
		manualReady := make(chan SendReceipt, 1)
		proxy := retryNativeProxy(t, ctx, fixture, func() {
			manual, err := client.SendWithOptions(ctx, thread, "Concurrent native fixture task.", "busy-manual", "fixture:manual", TurnOptions{Model: "gpt-5.5", ReasoningEffort: "low", ThreadPolicy: policy})
			if err != nil {
				t.Error(err)
				manualReady <- SendReceipt{}
				return
			}
			provider.mu.Lock()
			entered := provider.entered
			provider.mu.Unlock()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Error("native race task never reached provider")
			}
			active, err := client.ReadTurnState(ctx, thread)
			if err != nil || active.Status != "active" || active.LatestTurnID != manual.TurnID || active.LatestTurnStatus != "inProgress" {
				t.Errorf("native race did not establish the exact active manual turn: %+v %v", active, err)
			}
			manualReady <- manual
		}, false)
		candidate := NewWithOptions(proxy, ClientOptions{SubmissionLedgerPath: filepath.Join(fixture.root, "race-ledger.json")})
		defer candidate.Close()
		receipt, err := candidate.ContinueFailedTurn(ctx, thread, "Racing synthetic continuation.", "race-stable", "fixture:race:"+original.TurnID, original.TurnID, TurnOptions{Model: "gpt-5.5", ReasoningEffort: "low", ThreadPolicy: policy})
		var manual SendReceipt
		select {
		case manual = <-manualReady:
		case <-ctx.Done():
			t.Fatal("native busy-race injection did not run")
		}
		state, readErr := client.ReadTurnState(ctx, thread)
		provider.mu.Lock()
		nativeRequests := len(provider.requests)
		provider.mu.Unlock()
		t.Logf("native-direct race: manual=%s continuation=%s result=%v latest=%s status=%s providerRequests=%d", manual.TurnID, receipt.TurnID, err, state.LatestTurnID, state.LatestTurnStatus, nativeRequests)
		provider.mu.Lock()
		release := provider.hold
		provider.mu.Unlock()
		close(release)
		// Pinned public turn/start implicitly steers into an active regular
		// turn. This diagnostic records that unmanaged limitation; managed
		// exclusion is proved separately through the shared admission fence.
		if err != nil || manual.TurnID == "" || receipt.TurnID != manual.TurnID || readErr != nil || state.LatestTurnID != manual.TurnID || state.LatestTurnStatus != "inProgress" || nativeRequests != 1 {
			t.Fatal("native-direct race changed pinned implicit-steering behavior; compatibility reassessment required")
		}
		finishCtx, finishCancel := context.WithTimeout(ctx, 5*time.Second)
		defer finishCancel()
		retryNativeWait(t, finishCtx, client, thread, manual.TurnID, "completed")
		for {
			transcript, err := client.ReadThread(finishCtx, thread)
			if err != nil {
				t.Fatal(err)
			}
			counts := map[string]int{}
			for _, entry := range transcript.Entries {
				wantText, wantTurn, wantStatus := "", manual.TurnID, "completed"
				switch entry.ClientUserMessageID {
				case "busy-manual":
					wantText = "Concurrent native fixture task."
				case "race-stable":
					wantText = "Racing synthetic continuation."
				case "busy-original":
					wantText, wantTurn, wantStatus = "Synthetic fixture request.", original.TurnID, "failed"
				default:
					continue
				}
				counts[entry.ClientUserMessageID]++
				if counts[entry.ClientUserMessageID] > 1 || entry.TurnID != wantTurn || entry.Text != wantText || entry.TurnStatus != wantStatus {
					t.Fatalf("native-direct input duplicated, replaced or interrupted: %+v", entry)
				}
			}
			if counts["busy-manual"] == 1 && counts["race-stable"] == 1 && counts["busy-original"] == 1 {
				break
			}
			select {
			case <-finishCtx.Done():
				t.Fatalf("native-direct accepted inputs did not reach exact history: %v", counts)
			case <-time.After(20 * time.Millisecond):
			}
		}
		var history struct {
			Data []struct {
				ID     string `json:"id"`
				Status string `json:"status"`
				Error  *struct {
					Code    string `json:"codexErrorInfo"`
					Message string `json:"message"`
				} `json:"error"`
			} `json:"data"`
		}
		if err := client.Request(finishCtx, "thread/turns/list", map[string]any{"threadId": thread, "limit": 10, "sortDirection": "desc", "itemsView": "notLoaded"}, &history); err != nil {
			t.Fatal(err)
		}
		if len(history.Data) != 2 || history.Data[0].ID != manual.TurnID || history.Data[0].Status != "completed" || history.Data[1].ID != original.TurnID || history.Data[1].Status != "failed" || history.Data[1].Error == nil || history.Data[1].Error.Code != originalState.TerminalFailure.Code || history.Data[1].Error.Message != originalState.TerminalFailure.Message {
			t.Fatalf("native-direct race replaced a turn or changed original failed history: %+v", history)
		}
		provider.mu.Lock()
		finalRequests := len(provider.requests)
		provider.mu.Unlock()
		t.Logf("native-direct pinned limitation verified: both input IDs once in completed turn=%s, original failed turn=%s preserved, providerRequests held=%d final=%d; managed safety requires the shared admission fence", manual.TurnID, original.TurnID, nativeRequests, finalRequests)
	})
}

func execNativeVersion(fixture *recoveryIntegrationHarness) (string, error) {
	command := exec.Command(fixture.binary, "--version")
	command.Env = fixture.environment()
	command.Dir = fixture.cwd
	output, err := command.Output()
	return strings.TrimSpace(string(output)), err
}

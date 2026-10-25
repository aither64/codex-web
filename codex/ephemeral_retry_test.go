package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func ephemeralErrorParams(info any, retry bool) map[string]any {
	return map[string]any{"threadId": "utility-thread", "turnId": "utility-turn", "willRetry": retry,
		"error": map[string]any{"message": "PRIVATE_PROVIDER_DIAGNOSTIC", "codexErrorInfo": info}}
}

func ephemeralWriteError(c *websocket.Conn, params map[string]any) error {
	return writeObject(c, map[string]any{"method": "error", "params": params})
}

func ephemeralWriteFailure(c *websocket.Conn, status string, info any) error {
	return writeObject(c, map[string]any{"method": "turn/completed", "params": map[string]any{
		"threadId": "utility-thread", "turn": map[string]any{"id": "utility-turn", "status": status, "items": []any{},
			"error": map[string]any{"message": "PRIVATE_PROVIDER_DIAGNOSTIC", "codexErrorInfo": info}}}})
}

func TestEphemeralNativeRetryProgress(t *testing.T) {
	for _, early := range []bool{true, false} {
		for _, info := range []any{"serverOverloaded", "unauthorized", map[string]any{"futureError": map[string]any{}}} {
			t.Run(fmt.Sprintf("early=%t/info=%v", early, info), func(t *testing.T) {
				fake := &ephemeralFake{turn: func(c *websocket.Conn, call map[string]any) error {
					if !early {
						if err := ephemeralTurnResponse(c, call); err != nil {
							return err
						}
					}
					for range 3 {
						if err := ephemeralWriteError(c, ephemeralErrorParams(info, true)); err != nil {
							return err
						}
					}
					if err := ephemeralWriteFinal(c, "valid answer", "final_answer", "utility-thread", "utility-turn"); err != nil {
						return err
					}
					if early {
						return ephemeralTurnResponse(c, call)
					}
					return nil
				}}
				client := New(fake.serve(t))
				defer client.Close()
				result, err := runEphemeralTest(t, client, ephemeralTestOptions(t))
				if err != nil || result.Text != "valid answer" || CanRetryEphemeralTurn(err) {
					t.Fatalf("result=%#v error=%v", result, err)
				}
				starts := 0
				for _, call := range fake.snapshot() {
					if call["method"] == "turn/interrupt" {
						t.Fatal("native retry progress was interrupted")
					}
					if call["method"] == "turn/start" {
						starts++
					}
				}
				if starts != 1 {
					t.Fatalf("started %d turns", starts)
				}
			})
		}
	}
}

func TestEphemeralTerminalRetryProof(t *testing.T) {
	for _, mode := range []string{"early", "late", "completion only", "interrupted", "no completion error", "contradictory success", "permanent completion", "late foreign error", "late malformed error", "bad cleanup", "disconnect cleanup", "missing completion", "lost reply", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 180*time.Millisecond)
			defer cancel()
			fake := &ephemeralFake{turn: func(c *websocket.Conn, call map[string]any) error {
				if mode != "early" && mode != "lost reply" {
					if err := ephemeralTurnResponse(c, call); err != nil {
						return err
					}
				}
				if mode != "completion only" {
					if err := ephemeralWriteError(c, ephemeralErrorParams("serverOverloaded", false)); err != nil {
						return err
					}
				}
				if mode == "cancel" {
					cancel()
					return nil
				}
				if mode == "missing completion" {
					return nil
				}
				if mode == "contradictory success" {
					return ephemeralWriteFinal(c, "must not escape", "final_answer", "utility-thread", "utility-turn")
				}
				if mode == "no completion error" {
					return writeObject(c, map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "utility-thread", "turn": map[string]any{"id": "utility-turn", "status": "failed", "items": []any{}}}})
				}
				status, info := "failed", "serverOverloaded"
				if mode == "interrupted" {
					status = "interrupted"
				}
				if mode == "permanent completion" {
					info = "unauthorized"
				}
				if err := ephemeralWriteFailure(c, status, info); err != nil {
					return err
				}
				if mode == "late foreign error" || mode == "late malformed error" {
					params := ephemeralErrorParams("serverOverloaded", false)
					if mode == "late foreign error" {
						params["threadId"] = "foreign-thread"
					} else {
						delete(params, "willRetry")
					}
					return ephemeralWriteError(c, params)
				}
				if mode == "early" {
					return ephemeralTurnResponse(c, call)
				}
				return nil
			}}
			if mode == "bad cleanup" {
				fake.unsubscribe = map[string]any{"status": "unexpected"}
			}
			if mode == "disconnect cleanup" {
				fake.stopAt = "thread/unsubscribe"
			}
			client := New(fake.serve(t))
			defer client.Close()
			result, err := client.RunEphemeralTurn(ctx, ephemeralTestOptions(t))
			wantRetry := mode == "early" || mode == "late" || mode == "completion only" || mode == "interrupted" || mode == "no completion error"
			if result.Text != "" || err == nil || CanRetryEphemeralTurn(err) != wantRetry || strings.Contains(err.Error(), "PRIVATE_PROVIDER") {
				t.Fatalf("result=%#v error=%v retry=%t", result, err, CanRetryEphemeralTurn(err))
			}
			if wantRetry && (!errors.Is(err, ErrEphemeralServer) || !CanRetryEphemeralTurn(fmt.Errorf("caller: %w", err))) {
				t.Fatal("retry proof lost error wrapping/category behavior")
			}
			if mode == "missing completion" || mode == "lost reply" {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("unsettled execution returned before deadline: %v", err)
				}
			}
			if wantRetry {
				for _, call := range fake.snapshot() {
					if call["method"] == "turn/interrupt" {
						t.Fatal("already settled execution was interrupted")
					}
				}
			}
		})
	}
	for _, err := range []error{nil, ErrEphemeralServer, ErrEphemeralProtocol, context.Canceled, context.DeadlineExceeded, errors.New("serverOverloaded")} {
		if CanRetryEphemeralTurn(err) {
			t.Fatalf("unproved error authorized retry: %v", err)
		}
	}
}

func TestEphemeralPostCompletionErrorRevokesRetry(t *testing.T) {
	for _, early := range []bool{true, false} {
		for _, test := range []struct {
			name, info string
			retry      bool
		}{
			{name: "permanent", info: "unauthorized"},
			{name: "progress", info: "serverOverloaded", retry: true},
			{name: "terminal", info: "serverOverloaded"},
		} {
			t.Run(fmt.Sprintf("early=%t/%s", early, test.name), func(t *testing.T) {
				postCompletion := func(c *websocket.Conn) error {
					return ephemeralWriteError(c, ephemeralErrorParams(test.info, test.retry))
				}
				fake := &ephemeralFake{turn: func(c *websocket.Conn, call map[string]any) error {
					if !early {
						if err := ephemeralTurnResponse(c, call); err != nil {
							return err
						}
					}
					if err := ephemeralWriteError(c, ephemeralErrorParams("serverOverloaded", false)); err != nil {
						return err
					}
					if err := ephemeralWriteFailure(c, "failed", "serverOverloaded"); err != nil {
						return err
					}
					if early {
						if err := postCompletion(c); err != nil {
							return err
						}
						return ephemeralTurnResponse(c, call)
					}
					return nil
				}}
				if !early {
					// Unsubscribe proves binding/settlement happened before delivery;
					// the ordered error frame still precedes its cleanup response.
					fake.beforeUnsubscribe = postCompletion
				}
				client := New(fake.serve(t))
				defer client.Close()
				result, err := runEphemeralTest(t, client, ephemeralTestOptions(t))
				want := ErrEphemeralServer // Already returned to teardown before the late frame.
				if early {
					want = ErrEphemeralProtocol
				}
				if !errors.Is(err, want) || result.Text != "" ||
					CanRetryEphemeralTurn(err) || CanRetryEphemeralTurn(fmt.Errorf("caller: %w", err)) ||
					strings.Contains(err.Error(), "PRIVATE_PROVIDER") {
					t.Fatalf("result=%#v error=%v retry=%t", result, err, CanRetryEphemeralTurn(err))
				}
				starts, unsubscribes := 0, 0
				for _, call := range fake.snapshot() {
					switch call["method"] {
					case "turn/start":
						starts++
					case "thread/unsubscribe":
						unsubscribes++
					case "turn/interrupt":
						t.Fatalf("post-completion error caused redundant or foreign interrupt: %#v", call["params"])
					}
				}
				if starts != 1 || unsubscribes != 1 {
					t.Fatalf("starts=%d unsubscribes=%d", starts, unsubscribes)
				}
			})
		}
	}
}

func TestEphemeralErrorClassification(t *testing.T) {
	for _, raw := range []string{`"serverOverloaded"`, `"internalServerError"`, `"unauthorized"`, `"rateLimitExceeded"`, `"usageLimitExceeded"`, `"futureError"`, `null`, `{"futureError":{}}`, `true`, `[]`} {
		failure, _ := ephemeralObject([]byte(`{"message":"serverOverloaded 503","codexErrorInfo":` + raw + `}`))
		transient, valid := ephemeralTurnError(failure)
		want := raw == `"serverOverloaded"` || raw == `"internalServerError"`
		if transient != want || valid != (raw != `true` && raw != `[]`) {
			t.Fatalf("classification %s = %t, %t", raw, transient, valid)
		}
	}
	for _, kind := range []string{"httpConnectionFailed", "responseStreamConnectionFailed", "responseStreamDisconnected", "responseTooManyFailedAttempts"} {
		for _, code := range []string{"absent", "null", "408", "500", "502", "503", "504", "401", "403", "429", "501", "200", "-1", "65536", "500.1", `"503"`, "true"} {
			fields := `{}`
			if code != "absent" {
				fields = `{"httpStatusCode":` + code + `}`
			}
			failure, _ := ephemeralObject([]byte(`{"message":"private","codexErrorInfo":{"` + kind + `":` + fields + `}}`))
			transient, valid := ephemeralTurnError(failure)
			want := code == "408" || code == "500" || code == "502" || code == "503" || code == "504" || (kind != "responseTooManyFailedAttempts" && (code == "absent" || code == "null"))
			bad := code == "-1" || code == "65536" || code == "500.1" || code == `"503"` || code == "true"
			if transient != want || valid == bad {
				t.Fatalf("%s/%s = transient %t valid %t", kind, code, transient, valid)
			}
		}
	}
}

func TestEphemeralMalformedOrForeignError(t *testing.T) {
	for _, mode := range []string{"missing flag", "null flag", "string flag", "missing message", "bad info", "bad status", "wrong thread", "wrong turn", "early wrong turn", "early overflow"} {
		t.Run(mode, func(t *testing.T) {
			fake := &ephemeralFake{turn: func(c *websocket.Conn, call map[string]any) error {
				if !strings.HasPrefix(mode, "early") {
					if err := ephemeralTurnResponse(c, call); err != nil {
						return err
					}
				}
				params := ephemeralErrorParams("serverOverloaded", true)
				switch mode {
				case "missing flag":
					delete(params, "willRetry")
				case "null flag":
					params["willRetry"] = nil
				case "string flag":
					params["willRetry"] = "true"
				case "missing message":
					delete(params["error"].(map[string]any), "message")
				case "bad info":
					params["error"].(map[string]any)["codexErrorInfo"] = true
				case "bad status":
					params["error"].(map[string]any)["codexErrorInfo"] = map[string]any{"httpConnectionFailed": map[string]any{"httpStatusCode": "503"}}
				case "wrong thread":
					params["threadId"] = "foreign-thread"
				case "wrong turn", "early wrong turn":
					params["turnId"] = "foreign-turn"
				}
				count := 1
				if mode == "early overflow" {
					count = 33
				}
				for range count {
					if err := ephemeralWriteError(c, params); err != nil {
						return err
					}
				}
				if strings.HasPrefix(mode, "early") {
					return ephemeralTurnResponse(c, call)
				}
				return nil
			}}
			client := New(fake.serve(t))
			defer client.Close()
			result, err := runEphemeralTest(t, client, ephemeralTestOptions(t))
			if !errors.Is(err, ErrEphemeralProtocol) || result.Text != "" || CanRetryEphemeralTurn(err) {
				t.Fatalf("result=%#v error=%v", result, err)
			}
			if strings.Contains(mode, "wrong") {
				for _, call := range fake.snapshot() {
					if call["method"] == "turn/interrupt" {
						t.Fatal("foreign error retained interruption authority")
					}
				}
			}
		})
	}
}

func TestEphemeralNativeRetryDoesNotResetDeadline(t *testing.T) {
	fake := &ephemeralFake{turn: func(c *websocket.Conn, call map[string]any) error {
		if err := ephemeralTurnResponse(c, call); err != nil {
			return err
		}
		return ephemeralWriteError(c, ephemeralErrorParams("serverOverloaded", true))
	}}
	client := New(fake.serve(t))
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := client.RunEphemeralTurn(ctx, ephemeralTestOptions(t))
	if !errors.Is(err, context.DeadlineExceeded) || CanRetryEphemeralTurn(err) || time.Since(started) > 350*time.Millisecond {
		t.Fatalf("native retry exceeded original budget: %v", err)
	}
}

func TestEphemeralPreTurnDialRetryProof(t *testing.T) {
	path := filepath.Join(t.TempDir(), "refused.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	listener.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)
	client := New(path)
	defer client.Close()
	_, err = runEphemeralTest(t, client, ephemeralTestOptions(t))
	if !errors.Is(err, ErrEphemeralServer) || !CanRetryEphemeralTurn(err) {
		t.Fatalf("typed refused dial before start is not retryable: %v", err)
	}
	_, err = runEphemeralTest(t, New(filepath.Join(t.TempDir(), "missing.sock")), ephemeralTestOptions(t))
	if CanRetryEphemeralTurn(err) {
		t.Fatal("missing runtime was classified as a temporary dial failure")
	}
}

func TestEphemeralErrorGenerationMismatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &ephemeralSink{cancel: cancel, changed: make(chan struct{}, 1), generation: 1, threadID: "utility-thread", turnID: "utility-turn", turnIssued: true}
	params, _ := json.Marshal(ephemeralErrorParams("serverOverloaded", true))
	sink.handle(nil, nil, 2, rpcMessage{Method: "error", Params: params})
	if !errors.Is(sink.failure(), ErrEphemeralProtocol) || ctx.Err() == nil || sink.cleanupTurn(1, "utility-thread", "utility-turn") != "" {
		t.Fatal("error from foreign generation retained ownership")
	}
}

func ephemeralCompletion(status, info string, items []any) map[string]any {
	return map[string]any{"method": "turn/completed", "params": map[string]any{
		"threadId": "utility-thread", "turn": map[string]any{"id": "utility-turn", "status": status, "items": items,
			"error": map[string]any{"message": "PRIVATE_PROVIDER_DIAGNOSTIC", "codexErrorInfo": info}}}}
}

func TestEphemeralLateEvidence(t *testing.T) {
	for _, api := range []string{"strict", "prompted"} {
		for _, buffered := range []bool{true, false} {
			for _, mode := range []string{
				"success", "permanent", "different transient", "changed message", "interrupted", "in progress", "missing status",
				"malformed error", "malformed items", "malformed item", "missing item identity", "started",
				"async started", "async completed", "question started", "question completed", "async embedded", "question embedded",
				"conflicting final", "equivalent duplicate", "consistent final duplicate", "ordinary transient", "native item",
			} {
				t.Run(fmt.Sprintf("%s/buffered=%t/%s", api, buffered, mode), func(t *testing.T) {
					opts := ephemeralTestOptions(t)
					fake := &ephemeralFake{}
					if api == "prompted" {
						opts, fake = promptedEphemeralFixture(t)
					}
					answer := map[string]any{"id": "answer", "type": "agentMessage", "phase": "final_answer", "text": "first answer"}
					items := []any{}
					if mode == "conflicting final" || mode == "consistent final duplicate" {
						items = []any{answer}
					}
					late := ephemeralCompletion("failed", "serverOverloaded", items)
					turn := late["params"].(map[string]any)["turn"].(map[string]any)
					item := map[string]any{"id": "answer", "type": "agentMessage", "phase": "final_answer", "text": "first answer"}
					switch mode {
					case "success":
						turn["status"], turn["items"], turn["error"] = "completed", []any{item}, nil
					case "permanent":
						turn["error"].(map[string]any)["codexErrorInfo"] = "unauthorized"
					case "different transient":
						turn["error"].(map[string]any)["codexErrorInfo"] = "internalServerError"
					case "changed message":
						turn["error"].(map[string]any)["message"] = "DIFFERENT_PRIVATE_DIAGNOSTIC"
					case "interrupted":
						turn["status"] = "interrupted"
					case "in progress":
						turn["status"] = "inProgress"
					case "missing status":
						delete(turn, "status")
					case "malformed error":
						turn["error"] = true
					case "malformed items":
						turn["items"] = false
					case "malformed item":
						turn["items"] = []any{true}
					case "missing item identity":
						delete(item, "id")
						turn["items"] = []any{item}
					case "started":
						late["method"], turn["status"], turn["error"] = "turn/started", "inProgress", nil
					case "equivalent duplicate":
						turn["completedAtMs"] = 123
						turn["error"].(map[string]any)["additionalDetails"] = nil
						turn["error"].(map[string]any)["misalignment"] = nil
					case "ordinary transient":
						late = nil
					default:
						if strings.HasPrefix(mode, "async") {
							item["delivery"] = "async"
						}
						if strings.HasPrefix(mode, "question") {
							item["questions"] = []any{}
						}
						if mode == "conflicting final" {
							item["text"] = "different answer"
						}
						if mode == "native item" {
							item = map[string]any{"id": "native", "type": "commandExecution"}
						}
						if strings.HasSuffix(mode, "embedded") {
							turn["items"] = []any{item}
						} else {
							method := "item/completed"
							if strings.HasSuffix(mode, "started") {
								method = "item/started"
							}
							late = map[string]any{"method": method, "params": map[string]any{"threadId": "utility-thread", "turnId": "utility-turn", "item": item}}
						}
					}
					deliver := func(connection *websocket.Conn) error {
						if late == nil {
							return nil
						}
						var frame any = late
						if mode == "equivalent duplicate" {
							// Different key order and unrelated timestamps must not revoke proof.
							frame = json.RawMessage(`{"params":{"turn":{"items":[],"status":"failed","completedAtMs":123,"error":{"codexErrorInfo":"serverOverloaded","misalignment":null,"message":"PRIVATE_PROVIDER_DIAGNOSTIC","additionalDetails":null},"id":"utility-turn"},"threadId":"utility-thread"},"method":"turn/completed"}`)
						}
						if err := writeObject(connection, frame); err != nil {
							return err
						}
						// A valid frame after a contradiction must never restore eligibility.
						return writeObject(connection, ephemeralCompletion("failed", "serverOverloaded", items))
					}
					fake.turn = func(connection *websocket.Conn, call map[string]any) error {
						if !buffered {
							if err := ephemeralTurnResponse(connection, call); err != nil {
								return err
							}
						}
						if err := ephemeralWriteError(connection, ephemeralErrorParams("serverOverloaded", false)); err != nil {
							return err
						}
						if err := writeObject(connection, ephemeralCompletion("failed", "serverOverloaded", items)); err != nil {
							return err
						}
						if buffered {
							if err := deliver(connection); err != nil {
								return err
							}
							return ephemeralTurnResponse(connection, call)
						}
						return nil
					}
					if !buffered {
						fake.beforeUnsubscribe = deliver // Already bound and settled; no sleep or race assumption.
					}
					client := New(fake.serve(t))
					defer client.Close()
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					var result EphemeralTurnResult
					var err error
					if api == "prompted" {
						result, err = client.RunPromptedEphemeralTurn(ctx, opts)
					} else {
						result, err = client.RunEphemeralTurn(ctx, opts)
					}
					wantRetry := mode == "ordinary transient" || mode == "equivalent duplicate" || mode == "consistent final duplicate" || mode == "native item"
					if err == nil || result.Text != "" || CanRetryEphemeralTurn(err) != wantRetry || CanRetryEphemeralTurn(fmt.Errorf("wrapped: %w", err)) != wantRetry || strings.Contains(err.Error(), "PRIVATE") {
						t.Fatalf("result=%#v err=%v retry=%t want=%t", result, err, CanRetryEphemeralTurn(err), wantRetry)
					}
					starts, interrupts := 0, 0
					for _, call := range fake.snapshot() {
						if call["method"] == "turn/start" {
							starts++
						}
						if call["method"] == "turn/interrupt" {
							interrupts++
							params := call["params"].(map[string]any)
							if !buffered || wantRetry || params["threadId"] != "utility-thread" || params["turnId"] != "utility-turn" {
								t.Fatal("foreign or redundantly settled execution was interrupted")
							}
						}
					}
					if starts != 1 || interrupts > 1 {
						t.Fatalf("starts=%d interrupts=%d", starts, interrupts)
					}
				})
			}
		}
	}
}

func TestEphemeralEquivalentFailureEvidence(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &ephemeralSink{cancel: cancel, changed: make(chan struct{}, 1), generation: 1,
		threadID: "utility-thread", turnID: "utility-turn", turnIssued: true}
	for _, raw := range []string{
		`{"threadId":"utility-thread","turn":{"id":"utility-turn","status":"failed","items":[],"error":{"message":"private","codexErrorInfo":{"httpConnectionFailed":{"httpStatusCode":null}},"misalignment":{"errorType":null,"steer":null}}}}`,
		`{"turn":{"error":{"misalignment":{},"codexErrorInfo":{"httpConnectionFailed":{}},"message":"private"},"status":"failed","id":"utility-turn","items":[],"completedAtMs":456},"threadId":"utility-thread"}`,
	} {
		sink.consumeLocked(rpcMessage{Method: "turn/completed", Params: json.RawMessage(raw)})
		if !sink.canRetry(1, "utility-thread", "utility-turn") {
			t.Fatal("equivalent validated optional error evidence revoked retry")
		}
	}
	for _, info := range []string{"internalServerError", "serverOverloaded"} {
		value := ephemeralCompletion("failed", info, []any{})["params"]
		raw, _ := json.Marshal(value)
		sink.consumeLocked(rpcMessage{Method: "turn/completed", Params: raw})
		if sink.canRetry(1, "utility-thread", "utility-turn") {
			t.Fatal("different transient evidence restored a revoked candidate")
		}
	}
}

func TestEphemeralLateSuccessEvidence(t *testing.T) {
	for _, api := range []string{"strict", "prompted"} {
		for _, mode := range []string{"async", "failed completion", "consistent final duplicate"} {
			t.Run(api+"/"+mode, func(t *testing.T) {
				opts := ephemeralTestOptions(t)
				fake := &ephemeralFake{}
				if api == "prompted" {
					opts, fake = promptedEphemeralFixture(t)
				}
				fake.beforeUnsubscribe = func(connection *websocket.Conn) error {
					if mode == "failed completion" {
						return writeObject(connection, ephemeralCompletion("failed", "serverOverloaded", []any{}))
					}
					item := map[string]any{"id": "answer", "type": "agentMessage", "phase": "final_answer", "text": `{"name":"fix runtime session names"}`}
					if mode == "async" {
						item["delivery"] = "async"
					}
					return writeObject(connection, map[string]any{"method": "item/completed", "params": map[string]any{"threadId": "utility-thread", "turnId": "utility-turn", "item": item}})
				}
				client := New(fake.serve(t))
				defer client.Close()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				var result EphemeralTurnResult
				var err error
				if api == "prompted" {
					result, err = client.RunPromptedEphemeralTurn(ctx, opts)
				} else {
					result, err = client.RunEphemeralTurn(ctx, opts)
				}
				if CanRetryEphemeralTurn(err) {
					t.Fatal("contradictory success/failure evidence minted retry proof")
				}
				if mode == "consistent final duplicate" {
					if err != nil || result.Text != `{"name":"fix runtime session names"}` {
						t.Fatalf("consistent duplicate failed: %#v %v", result, err)
					}
				} else if err == nil || result.Text != "" {
					t.Fatalf("late evidence preserved sampled success: %#v %v", result, err)
				}
			})
		}
	}
}

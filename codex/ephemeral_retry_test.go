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

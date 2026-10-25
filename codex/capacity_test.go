package codex

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const reportedCapacityMessage = "stream disconnected before completion: Codex is at capacity, please try again later"
const reportedCapacityJSON = `{"additionalDetails":null,"codexErrorInfo":"other","message":"stream disconnected before completion: Codex is at capacity, please try again later","misalignment":null}`

func TestIsCapacityFailure(t *testing.T) {
	for _, test := range []struct {
		name, code, message string
		want                bool
	}{
		{"typed", "serverOverloaded", "arbitrary native details", true},
		{"typed without text", "serverOverloaded", "", true},
		{"exact wrapped", "other", reportedCapacityMessage, true},
		{"other reason", "other", "stream disconnected before completion: connection reset", false},
		{"bare suffix", "other", "Codex is at capacity, please try again later", false},
		{"extra prefix", "other", "wrapper: " + reportedCapacityMessage, false},
		{"nested stream prefix", "other", "stream disconnected before completion: " + reportedCapacityMessage, false},
		{"extra suffix", "other", reportedCapacityMessage + ".", false},
		{"leading whitespace", "other", " " + reportedCapacityMessage, false},
		{"trailing whitespace", "other", reportedCapacityMessage + "\n", false},
		{"case variation", "other", strings.ToLower(reportedCapacityMessage), false},
		{"discriminator case", "Other", reportedCapacityMessage, false},
		{"unknown", "futureError", reportedCapacityMessage, false},
		{"missing", "", reportedCapacityMessage, false},
		{"structured spelling", `{"serverOverloaded":{}}`, reportedCapacityMessage, false},
		{"permanent auth", "unauthorized", reportedCapacityMessage, false},
		{"permanent quota", "usageLimitExceeded", reportedCapacityMessage, false},
		{"rate limit", "rateLimitExceeded", reportedCapacityMessage, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := IsCapacityFailure(test.code, test.message); got != test.want {
				t.Fatalf("capacity=%t want %t", got, test.want)
			}
		})
	}
}

func TestEphemeralWrappedCapacityShape(t *testing.T) {
	for _, mode := range []string{"reported null fields", "nil code", "structured code", "unknown code", "permanent code", "near message", "missing message", "bad details", "bad misalignment"} {
		t.Run(mode, func(t *testing.T) {
			failure, ok := ephemeralObject([]byte(reportedCapacityJSON))
			if !ok {
				t.Fatal("reported fixture is not an object")
			}
			switch mode {
			case "nil code":
				failure["codexErrorInfo"] = nil
			case "structured code":
				failure["codexErrorInfo"] = map[string]any{"futureError": map[string]any{}}
			case "unknown code":
				failure["codexErrorInfo"] = "futureError"
			case "permanent code":
				failure["codexErrorInfo"] = "unauthorized"
			case "near message":
				failure["message"] = reportedCapacityMessage + " "
			case "missing message":
				delete(failure, "message")
			case "bad details":
				failure["additionalDetails"] = false
			case "bad misalignment":
				failure["misalignment"] = false
			}
			transient, valid := ephemeralTurnError(failure)
			wantValid := mode != "missing message" && mode != "bad details" && mode != "bad misalignment"
			if transient != (mode == "reported null fields") || valid != wantValid {
				t.Fatalf("classification=(%t,%t), want=(%t,%t)", transient, valid, mode == "reported null fields", wantValid)
			}
		})
	}
}

func TestEphemeralWrappedCapacityRetryProof(t *testing.T) {
	for _, prompted := range []bool{false, true} {
		entry := "strict"
		if prompted {
			entry = "prompted"
		}
		t.Run(entry, func(t *testing.T) {
			for _, mode := range []string{"native progress", "settled", "notification only", "foreign", "lost start", "bad cleanup", "malformed completion", "partial final", "contradictory completion", "cancelled"} {
				t.Run(mode, func(t *testing.T) {
					opts := ephemeralTestOptions(t)
					fake := &ephemeralFake{}
					if prompted {
						opts, fake = promptedEphemeralFixture(t)
					}
					ctx, cancel := context.WithTimeout(context.Background(), 180*time.Millisecond)
					defer cancel()
					fake.turn = func(connection *websocket.Conn, call map[string]any) error {
						if mode != "lost start" {
							if err := ephemeralTurnResponse(connection, call); err != nil {
								return err
							}
						}
						failure, _ := ephemeralObject([]byte(reportedCapacityJSON))
						params := map[string]any{"threadId": "utility-thread", "turnId": "utility-turn", "willRetry": mode == "native progress", "error": failure}
						if mode == "foreign" {
							params["turnId"] = "foreign-turn"
						}
						if err := ephemeralWriteError(connection, params); err != nil {
							return err
						}
						if mode == "cancelled" {
							cancel()
							return nil
						}
						if mode == "notification only" {
							return nil
						}
						if mode == "native progress" || mode == "contradictory completion" {
							return ephemeralWriteFinal(connection, "owned final answer", "final_answer", "utility-thread", "utility-turn")
						}
						turn := map[string]any{"id": "utility-turn", "status": "failed", "items": []any{}, "error": failure}
						if mode == "malformed completion" {
							turn["items"] = "invalid"
						}
						if mode == "partial final" {
							turn["items"] = []any{map[string]any{"type": "agentMessage", "id": "partial", "text": "unfinished"}}
						}
						return writeObject(connection, map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "utility-thread", "turn": turn}})
					}
					if mode == "bad cleanup" {
						fake.unsubscribe = map[string]any{"status": "unexpected"}
					}
					client := New(fake.serve(t))
					defer client.Close()
					var result EphemeralTurnResult
					var err error
					if prompted {
						result, err = client.RunPromptedEphemeralTurn(ctx, opts)
					} else {
						result, err = client.RunEphemeralTurn(ctx, opts)
					}
					if mode == "native progress" {
						if err != nil || result.Text != "owned final answer" || CanRetryEphemeralTurn(err) {
							t.Fatalf("progress did not stay on owned turn: %#v %v", result, err)
						}
					} else if err == nil || result.Text != "" || CanRetryEphemeralTurn(err) != (mode == "settled") {
						t.Fatalf("fresh proof changed: %#v %v retry=%t", result, err, CanRetryEphemeralTurn(err))
					}
					starts := 0
					for _, call := range fake.snapshot() {
						if call["method"] == "turn/start" {
							starts++
						}
						if (mode == "native progress" || mode == "settled") && call["method"] == "turn/interrupt" {
							t.Fatal("native progress or positively settled turn interrupted")
						}
					}
					if starts != 1 {
						t.Fatalf("unexpected new native attempt: %d", starts)
					}
					if len(client.Prompts("utility-thread")) != 0 {
						t.Fatal("capacity path admitted an interactive prompt")
					}
				})
			}
		})
	}
}

func TestWrappedCapacityRawProjection(t *testing.T) {
	rawFailure := map[string]any{}
	if err := json.Unmarshal([]byte(reportedCapacityJSON), &rawFailure); err != nil {
		t.Fatal(err)
	}
	entry := transcriptFailureEntry("failed-turn", rawFailure)
	var details map[string]any
	if entry.Summary != "Turn failed" || entry.Text != reportedCapacityMessage ||
		json.Unmarshal([]byte(entry.Details), &details) != nil || !reflect.DeepEqual(details, rawFailure) {
		t.Fatalf("raw failure rendering changed: %+v", entry)
	}
}

func TestReadTurnStateWrappedCapacityRemainsRaw(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for _, method := range []string{"thread/read", "thread/turns/list"} {
			call, err := readObject(connection)
			if err != nil {
				return err
			}
			if call["method"] != method {
				return errors.New("raw failure observation performed an unexpected mutation")
			}
			result := map[string]any{"thread": map[string]any{"id": "thread-1", "status": map[string]any{"type": "systemError"}}}
			if method == "thread/turns/list" {
				result = map[string]any{"data": []any{map[string]any{"id": "failed-turn", "status": "failed", "error": json.RawMessage(reportedCapacityJSON)}}}
			}
			if err := writeObject(connection, map[string]any{"id": call["id"], "result": result}); err != nil {
				return err
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	state, err := client.ReadTurnState(ctx, "thread-1")
	if err != nil || state.TerminalFailure == nil || state.TerminalFailure.Code != "other" ||
		state.TerminalFailure.Message != reportedCapacityMessage || state.TerminalFailure.TurnID != "failed-turn" || !state.IsLoadedFailedTurn("failed-turn") {
		t.Fatalf("native failure projection changed: %+v %v", state, err)
	}
}

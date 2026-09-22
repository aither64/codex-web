package codex

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func testTurnOptions() TurnOptions {
	return TurnOptions{
		Model: "model-1", ReasoningEffort: "high",
		AdditionalContext: map[string]AdditionalContextEntry{
			"application:plan": {Value: "use the accepted plan", Kind: "application"},
		},
	}
}

func additionalContextError(params map[string]any) error {
	context, ok := params["additionalContext"].(map[string]any)
	if !ok {
		return fmt.Errorf("additionalContext = %#v", params["additionalContext"])
	}
	entry, ok := context["application:plan"].(map[string]any)
	if !ok || entry["value"] != "use the accepted plan" || entry["kind"] != "application" {
		return fmt.Errorf("additionalContext entry = %#v", entry)
	}
	return nil
}

func TestSendWithOptionsUsesStartAndSteerProtocolFields(t *testing.T) {
	tests := []struct {
		name       string
		activeTurn string
		method     string
	}{
		{name: "idle start", method: "turn/start"},
		{name: "active steer", activeTurn: "turn-active", method: "turn/steer"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy := ThreadPolicy{DeveloperInstructions: "Review the assigned change.", Sandbox: "read-only"}
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				for index, method := range []string{"thread/resume", "thread/turns/list", test.method} {
					request, err := readObject(connection)
					if err != nil {
						return err
					}
					if request["method"] != method {
						return fmt.Errorf("request %d = %#v, want %s", index, request, method)
					}
					params := request["params"].(map[string]any)
					if index == 0 && (params["developerInstructions"] != sessionLifecycleDeveloperInstructions+"\n\n"+policy.DeveloperInstructions ||
						params["sandbox"] != "read-only") {
						return fmt.Errorf("assignment resume omitted retained policy: %#v", params)
					}
					result := map[string]any{}
					switch index {
					case 1:
						turns := []any{}
						if test.activeTurn != "" {
							turns = append(turns, map[string]any{"id": test.activeTurn, "status": "inProgress"})
						}
						result = map[string]any{"data": turns}
					case 2:
						if err := additionalContextError(params); err != nil {
							return err
						}
						if test.activeTurn == "" {
							if params["model"] != "model-1" || params["effort"] != "high" {
								return fmt.Errorf("turn/start settings = %#v", params)
							}
							result = map[string]any{"turn": map[string]any{"id": "turn-new"}}
						} else {
							if _, ok := params["model"]; ok {
								return fmt.Errorf("turn/steer included model: %#v", params)
							}
							if _, ok := params["effort"]; ok {
								return fmt.Errorf("turn/steer included effort: %#v", params)
							}
							if params["expectedTurnId"] != test.activeTurn {
								return fmt.Errorf("turn/steer expected turn = %#v", params)
							}
							result = map[string]any{"turnId": test.activeTurn}
						}
					}
					if err := writeObject(connection, map[string]any{"id": request["id"], "result": result}); err != nil {
						return err
					}
				}
				return nil
			})
			client := newTestClient(socket)
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			options := testTurnOptions()
			options.ThreadPolicy = policy
			receipt, err := client.SendWithOptions(
				ctx, "thread-1", "message", "client-message-1", "", options,
			)
			if err != nil {
				t.Fatal(err)
			}
			if receipt.Steered != (test.activeTurn != "") {
				t.Fatalf("receipt = %#v", receipt)
			}
		})
	}
}

func TestSendZeroOptionsPreservesStartRequest(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index, method := range []string{"thread/resume", "thread/turns/list", "turn/start"} {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			if request["method"] != method {
				return fmt.Errorf("request %d = %#v, want %s", index, request, method)
			}
			params := request["params"].(map[string]any)
			if index == 2 {
				for _, field := range []string{"model", "effort", "additionalContext"} {
					if _, ok := params[field]; ok {
						return fmt.Errorf("zero-option turn/start included %s: %#v", field, params)
					}
				}
			}
			result := map[string]any{}
			if index == 1 {
				result = map[string]any{"data": []any{}}
			} else if index == 2 {
				result = map[string]any{"turn": map[string]any{"id": "turn-new"}}
			}
			if err := writeObject(connection, map[string]any{"id": request["id"], "result": result}); err != nil {
				return err
			}
		}
		return nil
	})
	client := newTestClient(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Send(ctx, "thread-1", "message", "client-message-1", ""); err != nil {
		t.Fatal(err)
	}
	ledger, err := os.ReadFile(client.queueLedgerPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(ledger), "optionsDigest") {
		t.Fatalf("zero-option send wrote an options digest: %s", ledger)
	}
}

func TestEnsureInitialMessageWithOptionsUsesStartFields(t *testing.T) {
	rollout := filepath.Join(t.TempDir(), "rollout.jsonl")
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		started := false
		for requestNumber := 0; requestNumber < 5; requestNumber++ {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			params := request["params"].(map[string]any)
			var result map[string]any
			switch request["method"] {
			case "thread/read":
				result = map[string]any{"thread": freshThreadMetadata("thread-1", "/workspace/work/example", rollout)}
			case "turn/start":
				if started {
					return errors.New("initial message started twice")
				}
				if params["model"] != "model-1" || params["effort"] != "high" {
					return fmt.Errorf("initial turn settings = %#v", params)
				}
				if err := additionalContextError(params); err != nil {
					return err
				}
				if err := os.WriteFile(rollout, []byte("materialized\n"), 0o600); err != nil {
					return err
				}
				started = true
				result = map[string]any{}
			case "thread/resume":
				result = map[string]any{}
			case "thread/turns/list":
				if !started {
					return errors.New("initial history read before turn start")
				}
				result = map[string]any{"data": []any{map[string]any{"items": []any{map[string]any{
					"type": "userMessage", "content": []any{map[string]any{"type": "text", "text": "initial goal"}},
				}}}}}
			default:
				return fmt.Errorf("unexpected request %q", request["method"])
			}
			if err := writeObject(connection, map[string]any{"id": request["id"], "result": result}); err != nil {
				return err
			}
		}
		return nil
	})
	client := newTestClient(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.EnsureInitialMessageWithOptions(
		ctx, "thread-1", "/workspace/work/example", "initial goal", true, testTurnOptions(),
	); err != nil {
		t.Fatal(err)
	}
}

func TestTurnOptionsRejectInvalidAndOversizedInput(t *testing.T) {
	entries := make(map[string]AdditionalContextEntry, turnOptionMaxAdditionalContextEntries+1)
	for index := range turnOptionMaxAdditionalContextEntries + 1 {
		entries[fmt.Sprintf("application:%d", index)] = AdditionalContextEntry{Kind: "application"}
	}
	tests := []struct {
		name    string
		options TurnOptions
	}{
		{"non-application kind", TurnOptions{AdditionalContext: map[string]AdditionalContextEntry{
			"application:one": {Kind: "untrusted"},
		}}},
		{"empty source", TurnOptions{AdditionalContext: map[string]AdditionalContextEntry{
			"": {Kind: "application"},
		}}},
		{"invalid value unicode", TurnOptions{AdditionalContext: map[string]AdditionalContextEntry{
			"application:one": {Kind: "application", Value: string([]byte{0xff})},
		}}},
		{"too many entries", TurnOptions{AdditionalContext: entries}},
		{"oversized source", TurnOptions{AdditionalContext: map[string]AdditionalContextEntry{
			strings.Repeat("s", turnOptionMaxAdditionalContextKeyBytes+1): {Kind: "application"},
		}}},
		{"oversized value", TurnOptions{AdditionalContext: map[string]AdditionalContextEntry{
			"application:one": {Kind: "application", Value: strings.Repeat("v", turnOptionMaxAdditionalContextValueBytes+1)},
		}}},
		{"oversized model", TurnOptions{Model: strings.Repeat("m", turnOptionMaxScalarBytes+1)}},
		{"invalid reasoning effort Unicode", TurnOptions{ReasoningEffort: string([]byte{0xff})}},
		{"oversized reasoning effort", TurnOptions{ReasoningEffort: strings.Repeat("r", turnOptionMaxScalarBytes+1)}},
		{"oversized aggregate context", TurnOptions{AdditionalContext: map[string]AdditionalContextEntry{
			"application:one":   {Kind: "application", Value: strings.Repeat("v", turnOptionMaxAdditionalContextValueBytes)},
			"application:two":   {Kind: "application", Value: strings.Repeat("v", turnOptionMaxAdditionalContextValueBytes)},
			"application:three": {Kind: "application", Value: strings.Repeat("v", turnOptionMaxAdditionalContextValueBytes)},
			"application:four":  {Kind: "application", Value: strings.Repeat("v", turnOptionMaxAdditionalContextValueBytes)},
		}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := normalizeTurnOptions(test.options); err == nil {
				t.Fatal("invalid turn options were accepted")
			}
		})
	}
}

func TestSendWithOptionsBindsRetryIdentity(t *testing.T) {
	client := newTestClient(filepath.Join(t.TempDir(), "absent-socket"))
	defer client.Close()
	first := testTurnOptions()
	first.AdditionalContext["application:second"] = AdditionalContextEntry{Kind: "application", Value: "second"}
	normalized, err := normalizeTurnOptions(first)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.recordSendAttemptWithOptions(
		"thread-1", "message-1", "message", "action", false, normalized.Digest,
	); err != nil {
		t.Fatal(err)
	}
	if err := client.markSendSubmitting("thread-1", "message-1"); err != nil {
		t.Fatal(err)
	}
	want := SendReceipt{TurnID: "turn-1", ClientUserMessageID: "message-1"}
	if err := client.markSendAccepted("thread-1", "message-1", want); err != nil {
		t.Fatal(err)
	}
	// Map insertion order must not change an exact retry identity.
	retry := TurnOptions{
		Model: "model-1", ReasoningEffort: "high",
		AdditionalContext: map[string]AdditionalContextEntry{
			"application:second": {Kind: "application", Value: "second"},
			"application:plan":   {Kind: "application", Value: "use the accepted plan"},
		},
	}
	receipt, err := client.SendWithOptions(
		context.Background(), "thread-1", "message", "message-1", "action", retry,
	)
	if err != nil || receipt != want {
		t.Fatalf("exact retry = %#v, %v", receipt, err)
	}
	changed := retry
	changed.ReasoningEffort = "xhigh"
	if _, err := client.SendWithOptions(
		context.Background(), "thread-1", "message", "message-1", "action", changed,
	); err == nil || !strings.Contains(err.Error(), "another action") {
		t.Fatalf("changed options retry error = %v", err)
	}
}

func TestLegacySendAttemptReadsAsZeroOptions(t *testing.T) {
	client := newTestClient(filepath.Join(t.TempDir(), "absent-socket"))
	defer client.Close()
	legacy := fmt.Sprintf(
		`{"schema":3,"attempts":{},"sends":{"thread-1":{"message-1":{"digest":%q,"state":"accepted","turnId":"turn-1"}}}}`,
		queueTextDigest("message"),
	) + "\n"
	if err := os.WriteFile(client.queueLedgerPath, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	want := SendReceipt{TurnID: "turn-1", ClientUserMessageID: "message-1"}
	receipt, err := client.Send(context.Background(), "thread-1", "message", "message-1", "")
	if err != nil || receipt != want {
		t.Fatalf("legacy zero-option retry = %#v, %v", receipt, err)
	}
	ledger, err := os.ReadFile(client.queueLedgerPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(ledger), "optionsDigest") {
		t.Fatalf("ordinary legacy retry rewrote an options digest: %s", ledger)
	}
	if err := client.recordQueueAttempt("thread-1", "queue-1", "unrelated queue message"); err != nil {
		t.Fatal(err)
	}
	ledger, err = os.ReadFile(client.queueLedgerPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(ledger), "optionsDigest") {
		t.Fatalf("unrelated ledger write added an options digest: %s", ledger)
	}
	if _, err := client.SendWithOptions(
		context.Background(), "thread-1", "message", "message-1", "", testTurnOptions(),
	); err == nil || !strings.Contains(err.Error(), "another action") {
		t.Fatalf("legacy retry accepted changed options: %v", err)
	}
}

func TestOptionAwareDurableAttemptFamily(t *testing.T) {
	options := testTurnOptions()
	client := newTestClient(filepath.Join(t.TempDir(), "absent-socket"))
	defer client.Close()
	if err := client.PrepareSendWithOptions(
		"thread-1", "message", "prepared-1", "action", false, options,
	); err != nil {
		t.Fatal(err)
	}
	changed := options
	changed.Model = "model-2"
	if _, err := client.DiscardPreparedSendWithOptions(
		"thread-1", "message", "prepared-1", "action", changed,
	); err == nil || !strings.Contains(err.Error(), "another action") {
		t.Fatalf("discard accepted changed options: %v", err)
	}
	if discarded, err := client.DiscardPreparedSendWithOptions(
		"thread-1", "message", "prepared-1", "action", options,
	); err != nil || !discarded {
		t.Fatalf("discard exact prepared attempt = %t, %v", discarded, err)
	}

	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/items/list" {
			return fmt.Errorf("send-attempt discovery request = %#v", request)
		}
		return writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"data": []any{map[string]any{
				"turnId": "turn-1", "item": map[string]any{
					"id": "item-1", "type": "userMessage", "clientId": "message-1",
					"content": []any{map[string]any{"type": "text", "text": "message"}},
				},
			}}, "nextCursor": nil},
		})
	})
	discovery := newTestClient(socket)
	defer discovery.Close()
	attempted, err := discovery.SendAttemptedWithOptions(
		context.Background(), "thread-1", "message", "message-1", "action", options,
	)
	if err != nil || !attempted {
		t.Fatalf("option-aware send discovery = %t, %v", attempted, err)
	}
	receipt, reconciled, err := discovery.ReconcileSendWithOptions(
		context.Background(), "thread-1", "message", "message-1", "action", options,
	)
	if err != nil || !reconciled || receipt.TurnID != "turn-1" {
		t.Fatalf("option-aware reconciliation = %#v, %t, %v", receipt, reconciled, err)
	}
	if _, _, err := discovery.ReconcileSendWithOptions(
		context.Background(), "thread-1", "message", "message-1", "action", changed,
	); err == nil || !strings.Contains(err.Error(), "another action") {
		t.Fatalf("reconciliation accepted changed options: %v", err)
	}
}

func TestPreparedOptionedStartRefusesAnActiveTurn(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index, method := range []string{"thread/resume", "thread/turns/list"} {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			if request["method"] != method {
				return fmt.Errorf("request %d = %#v, want %s", index, request, method)
			}
			result := map[string]any{}
			if method == "thread/turns/list" {
				result = map[string]any{"data": []any{map[string]any{"id": "turn-active", "status": "inProgress"}}}
			}
			if err := writeObject(connection, map[string]any{"id": request["id"], "result": result}); err != nil {
				return err
			}
		}
		return nil
	})
	client := newTestClient(socket)
	defer client.Close()
	options := testTurnOptions()
	if err := client.PrepareSendWithOptions(
		"thread-1", "message", "message-1", "action", false, options,
	); err != nil {
		t.Fatal(err)
	}
	_, err := client.SendWithOptions(
		context.Background(), "thread-1", "message", "message-1", "action", options,
	)
	if err == nil || !strings.Contains(err.Error(), "no longer matches the thread state") {
		t.Fatalf("active turn accepted a prepared start: %v", err)
	}
}

func TestSendWithOptionsRetainsUnknownOutcome(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/items/list" {
			return fmt.Errorf("retry submitted instead of reconciling: %#v", request)
		}
		return writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"data": []any{}, "nextCursor": nil},
		})
	})
	client := newTestClient(socket)
	defer client.Close()
	options := testTurnOptions()
	normalized, err := normalizeTurnOptions(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.recordSendAttemptWithOptions(
		"thread-1", "message-1", "message", "action", false, normalized.Digest,
	); err != nil {
		t.Fatal(err)
	}
	if err := client.markSendSubmitting("thread-1", "message-1"); err != nil {
		t.Fatal(err)
	}
	_, err = client.SendWithOptions(
		context.Background(), "thread-1", "message", "message-1", "action", options,
	)
	var unknown *UnknownSendOutcomeError
	if !errors.As(err, &unknown) {
		t.Fatalf("unknown send outcome = %v", err)
	}
}

//go:build codex_integration

package codex

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Run after final review with CODEX_WEB_TEST_BINARY selecting the exact native
// 0.164 executable. Only disposable state and a synthetic provider are used.
func TestNativeConditionalSteerExactTurn(t *testing.T) {
	entered, release := make(chan struct{}, 8), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	h := newRecoveryIntegrationHarness(t, "", "", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/responses" {
			w.WriteHeader(http.StatusUpgradeRequired)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		event := func(value any) { data, _ := json.Marshal(value); _, _ = fmt.Fprintf(w, "data: %s\n\n", data) }
		event(map[string]any{"type": "response.created", "response": map[string]any{"id": "synthetic"}})
		w.(http.Flusher).Flush()
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		event(map[string]any{"type": "response.output_item.done", "item": map[string]any{
			"type": "message", "role": "assistant", "id": "answer", "phase": "final_answer",
			"content": []any{map[string]any{"type": "output_text", "text": "Fixture completed."}},
		}})
		event(map[string]any{"type": "response.completed", "response": map[string]any{"id": "synthetic"}})
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	integrationRequireVersion(t, exec.CommandContext(ctx, h.binary, "--version"), "codex-cli 0.164.0")
	data, err := os.ReadFile(h.binary)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("native=%s sha256=%x", h.binary, sha256.Sum256(data))
	h.start()
	c := NewWithOptions(h.socket, ClientOptions{SubmissionLedgerPath: filepath.Join(h.root, "attempts.json")})
	defer c.Close()
	thread, err := c.StartThreadWithSettings(ctx, h.cwd, nil, ThreadSettings{Model: "gpt-5.5", ReasoningEffort: "low"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.BootstrapHeadlessThread(ctx, thread, h.cwd, "", "Private conditional-steer fixture for "+thread); err != nil {
		t.Fatal(err)
	}
	initial, err := c.Send(ctx, thread, "Start fixture", "initial", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("native turn did not reach synthetic provider")
	}
	for range 2 {
		receipt, err := c.ConditionalSteer(ctx, thread, h.cwd, initial.TurnID, "Fixture notice", "notice", "operation")
		if err != nil || receipt.TurnID != initial.TurnID || !receipt.Steered {
			t.Fatalf("exact-turn steer: %+v %v", receipt, err)
		}
	}
	once.Do(func() { close(release) })
	for {
		active, err := c.ActiveTurnID(ctx, thread)
		if err != nil {
			t.Fatal(err)
		}
		if active == "" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("fixture turn did not finish")
		case <-time.After(20 * time.Millisecond):
		}
	}
	_, err = c.ConditionalSteer(ctx, thread, h.cwd, initial.TurnID, "Late notice", "late", "operation")
	var unsent *MessageNotSentError
	if !errors.As(err, &unsent) {
		t.Fatalf("completed turn accepted late input: %v", err)
	}
	transcript, err := c.ReadThread(ctx, thread)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range transcript.Entries {
		if entry.ClientUserMessageID == "notice" {
			count++
			if entry.TurnID != initial.TurnID {
				t.Fatal("notice moved to another turn")
			}
		}
		if entry.ClientUserMessageID == "late" {
			t.Fatal("late notice started another turn")
		}
	}
	if count != 1 {
		t.Fatalf("native notice items=%d", count)
	}
}

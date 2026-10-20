package codex

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// Uses a private native home and synthetic inference only. The process default
// deliberately points at a different provider from the one selected per call.
func TestPinnedCodexEphemeralProviderSelection(t *testing.T) {
	binary := os.Getenv("CODEX_WEB_TEST_BINARY")
	if binary == "" {
		t.Skip("CODEX_WEB_TEST_BINARY is not configured")
	}
	root := t.TempDir()
	home := filepath.Join(root, "home")
	if err := os.Mkdir(home, 0700); err != nil {
		t.Fatal(err)
	}
	var wrong, selected atomic.Int64
	defaultProvider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wrong.Add(1)
		http.Error(w, "wrong provider", http.StatusServiceUnavailable)
	}))
	defer defaultProvider.Close()
	selectedProvider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" {
			http.NotFound(w, r)
			return
		}
		var request struct {
			Model string            `json:"model"`
			Tools []json.RawMessage `json:"tools"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.Model != "gpt-6-luna" || len(request.Tools) != 0 {
			t.Error("unexpected utility model or tools")
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		selected.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		event := func(value any) { data, _ := json.Marshal(value); fmt.Fprintf(w, "data: %s\n\n", data) }
		event(map[string]any{"type": "response.created", "response": map[string]any{"id": "selected-response"}})
		event(map[string]any{"type": "response.output_item.done", "item": map[string]any{"type": "message", "role": "assistant", "id": "selected-answer", "phase": "final_answer", "content": []any{map[string]any{"type": "output_text", "text": `{"name":"provider-routing"}`}}}})
		event(map[string]any{"type": "response.completed", "response": map[string]any{"id": "selected-response", "usage": map[string]any{"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}}})
	}))
	defer selectedProvider.Close()
	config := "model_provider = \"wrong\"\nmodel = \"gpt-6-luna\"\n"
	for name, endpoint := range map[string]string{"wrong": defaultProvider.URL, "selected": selectedProvider.URL} {
		config += "[model_providers." + name + "]\nname = " + strconv.Quote(name) + "\nbase_url = " + strconv.Quote(endpoint+"/v1") + "\nwire_api = \"responses\"\nrequires_openai_auth = false\n"
	}
	if err := os.WriteFile(filepath.Join(home, "config.toml"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	opts := ephemeralTestOptions(t)
	content, err := os.ReadFile("ephemeral_luna_models.json")
	if err != nil {
		t.Fatal(err)
	}
	catalog := filepath.Join(root, "models.json")
	if err := os.WriteFile(catalog, content, 0444); err != nil {
		t.Fatal(err)
	}
	opts.Model, opts.ModelProvider, opts.ModelCatalogFile = "gpt-6-luna", "selected", catalog
	socket := filepath.Join(root, "native.sock")
	log, err := os.Create(filepath.Join(root, "native.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	command := exec.Command(binary, "-c", "model_catalog_json="+strconv.Quote(catalog), "app-server", "--strict-config", "--listen", "unix://"+socket)
	command.Dir = opts.Directory
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "CODEX_HOME=" + home, "XDG_CONFIG_HOME=" + home, "XDG_STATE_HOME=" + home, "XDG_CACHE_HOME=" + home, "RUST_LOG=error"}
	command.Stdout, command.Stderr = log, log
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = command.Process.Kill(); _ = command.Wait() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		if info, err := os.Stat(socket); err == nil && info.Mode()&os.ModeSocket != 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("native socket did not appear")
		case <-time.After(20 * time.Millisecond):
		}
	}
	client := New(socket)
	defer client.Close()
	result, err := client.RunEphemeralTurn(ctx, opts)
	if err != nil || result.Text != `{"name":"provider-routing"}` {
		t.Fatalf("result=%q err=%v", result.Text, err)
	}
	if wrong.Load() != 0 || selected.Load() != 1 {
		t.Fatalf("provider requests: default=%d selected=%d", wrong.Load(), selected.Load())
	}
}

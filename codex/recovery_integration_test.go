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
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// The native fixture uses a private home and a loopback Responses provider.
// It never reads the operator's credentials or addresses a managed session.
func TestPinnedCodexRecoveryRemainsColdUntilActivation(t *testing.T) {
	binary := os.Getenv("CODEX_WEB_TEST_BINARY")
	if binary == "" {
		t.Skip("CODEX_WEB_TEST_BINARY is not configured")
	}
	root, err := os.MkdirTemp("/tmp", "codex-recovery-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	home, cwd := filepath.Join(root, "home"), filepath.Join(root, "work")
	for _, dir := range []string{home, cwd} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	var hits atomic.Int64
	entered, release := make(chan struct{}, 16), make(chan struct{})
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" {
			w.WriteHeader(404)
			return
		}
		index := hits.Add(1)
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		event := func(value any) { data, _ := json.Marshal(value); fmt.Fprintf(w, "data: %s\n\n", data) }
		id := fmt.Sprintf("response-%d", index)
		event(map[string]any{"type": "response.created", "response": map[string]any{"id": id}})
		event(map[string]any{"type": "response.output_item.done", "item": map[string]any{"type": "message", "role": "assistant", "id": "answer-" + id, "phase": "final_answer", "content": []any{map[string]any{"type": "output_text", "text": "Fixture request completed."}}}})
		event(map[string]any{"type": "response.completed", "response": map[string]any{"id": id, "usage": map[string]any{"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}}})
	}))
	t.Cleanup(provider.Close)
	configuration := "model_provider = \"fixture\"\nmodel = \"gpt-5.5\"\nmodel_reasoning_effort = \"low\"\nthread_unload_delay_secs = 0\n[features]\ngoals = true\n[model_providers.fixture]\nname = \"Synthetic recovery fixture\"\nbase_url = " + strconv.Quote(provider.URL+"/v1") + "\nwire_api = \"responses\"\nrequires_openai_auth = false\n"
	if err := os.WriteFile(filepath.Join(home, "config.toml"), []byte(configuration), 0600); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(root, "app.sock")
	var command *exec.Cmd
	log, err := os.Create(filepath.Join(root, "native.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { log.Close() })
	t.Cleanup(func() {
		if t.Failed() {
			data, _ := os.ReadFile(log.Name())
			if len(data) > 16384 {
				data = data[len(data)-16384:]
			}
			t.Logf("private native log: %s", data)
		}
	})
	stop := func() {
		if command != nil {
			_ = command.Process.Kill()
			_ = command.Wait()
			command = nil
		}
	}
	t.Cleanup(stop)
	start := func() {
		_ = os.Remove(socket)
		command = exec.Command(binary, "app-server", "--strict-config", "--listen", "unix://"+socket)
		command.Dir = cwd
		command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "CODEX_HOME=" + home, "XDG_CONFIG_HOME=" + home, "XDG_STATE_HOME=" + home, "XDG_CACHE_HOME=" + home, "RUST_LOG=error"}
		command.Stdout, command.Stderr = log, log
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			if info, err := os.Stat(socket); err == nil && info.Mode()&os.ModeSocket != 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("native socket did not appear")
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	var allowed atomic.Bool
	allowed.Store(true)
	newClient := func() *Client {
		return NewWithOptions(socket, ClientOptions{ThreadSourceKinds: []string{"vscode"}, SubmissionLedgerPath: filepath.Join(root, "receipts.json"), AllowImplicitResume: func(string) bool { return allowed.Load() }})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	start()
	client := newClient()
	threads := []string{}
	projects := []string{}
	for _, name := range []string{"root", "member"} {
		project, err := client.CreateProject(ctx, "Recovery fixture "+name, "recovery-fixture-"+name)
		if err != nil {
			t.Fatal(err)
		}
		id, err := client.StartThreadWithSettings(ctx, cwd, map[string]string{"RECOVERY_RECIPIENT": name}, ThreadSettings{ProjectID: project.ID})
		if err != nil {
			t.Fatal(err)
		}
		if err := client.BootstrapHeadlessThread(ctx, id, cwd, project.ID, "Recovery fixture history for "+name+" thread "+id); err != nil {
			t.Fatal(err)
		}
		threads = append(threads, id)
		projects = append(projects, project.ID)
	}
	// Keep the member cold before its durable queue is saved. The root's live
	// browser and observer watches remain attached across the server restart.
	var unsubscribed any
	if err := client.Request(ctx, "thread/unsubscribe", map[string]any{"threadId": threads[1]}, &unsubscribed); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		ids, err := client.LoadedThreadIDs(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(ids, threads[1]) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fixture member did not unload")
		}
		time.Sleep(20 * time.Millisecond)
	}
	defer client.Close()
	_, unsubscribe, err := client.Subscribe(ctx, threads[0])
	if err != nil {
		t.Fatal(err)
	}
	defer unsubscribe()
	observer := NewWithOptions(socket, ClientOptions{ObserverOnly: true, AllowImplicitResume: func(string) bool { return allowed.Load() }})
	defer observer.Close()
	_, stopObserving, err := observer.Subscribe(ctx, threads[0])
	if err != nil {
		t.Fatal(err)
	}
	defer stopObserving()
	for _, entry := range []struct{ thread, text, id string }{{threads[0], "Older saved request", "older"}, {threads[0], "Second saved request", "second"}, {threads[1], "Member held request", "member"}} {
		if _, err := client.Queue(ctx, entry.thread, entry.text, entry.id); err != nil {
			t.Fatal(err)
		}
	}
	var goal any
	if err := client.Request(ctx, "thread/goal/set", map[string]any{"threadId": threads[0], "objective": "Finish the fixture requests.", "status": "active"}, &goal); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("fixture did not create interrupted work")
	}
	// Preserve queues, goal and history while both live watches survive loss.
	allowed.Store(false)
	stop()
	for _, watched := range []*Client{client, observer} {
		deadline := time.Now().Add(5 * time.Second)
		for {
			watched.connectionMu.Lock()
			disconnected := watched.connection == nil
			watched.connectionMu.Unlock()
			if disconnected {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("old watch did not disconnect")
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	start()
	hits.Store(0)
	for _, watched := range []*Client{client, observer} {
		deadline := time.Now().Add(5 * time.Second)
		for {
			if err := watched.Ensure(ctx); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("watch did not reconnect")
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	active, err := client.HasActiveGoal(ctx, threads[0])
	if err != nil || !active {
		t.Fatal("cold active goal unavailable", active, err)
	}
	for index, thread := range threads {
		if _, err := client.ReadThread(ctx, thread); err != nil {
			t.Fatal(err)
		}
		name := []string{"root", "member"}[index]
		if err := client.VerifyHeadlessBootstrap(ctx, thread, cwd, projects[index], "Recovery fixture history for "+name+" thread "+thread); err != nil {
			t.Fatal("restart lost the original history marker", err)
		}
	}
	// A browser/portal reconnection must not load either retained conversation.
	reader := newClient()
	defer reader.Close()
	if _, err := reader.ListQueue(ctx, threads[0]); err != nil {
		t.Fatal(err)
	}
	if ids, err := reader.LoadedThreadIDs(ctx); err != nil || len(ids) != 0 {
		t.Fatal("cold read loaded threads", ids, err)
	}
	select {
	case <-entered:
		t.Fatal("cold recovery ran inference")
	case <-time.After(12 * time.Second):
	}
	if hits.Load() != 0 {
		t.Fatal("cold work reached provider")
	}
	if _, err := client.Queue(ctx, threads[0], "New Send request", "new-send"); err != nil {
		t.Fatal(err)
	}
	if err := client.ActivateThread(ctx, threads[0], ThreadPolicy{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("explicit activation did not run work")
	}
	if err := client.Request(ctx, "thread/goal/clear", map[string]any{"threadId": threads[0]}, &goal); err != nil {
		t.Fatal(err)
	}
	allowed.Store(true)
	close(release)
	deadline = time.Now().Add(20 * time.Second)
	for {
		transcript, err := client.ReadThread(ctx, threads[0])
		if err != nil {
			t.Fatal(err)
		}
		messages := []string{}
		for _, entry := range transcript.Entries {
			if slices.Contains([]string{"Older saved request", "Second saved request", "New Send request"}, entry.Text) {
				messages = append(messages, entry.Text)
			}
		}
		if len(messages) >= 3 {
			if !slices.Equal(messages, []string{"Older saved request", "Second saved request", "New Send request"}) {
				t.Fatal("native FIFO changed", messages)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("queued requests did not complete", messages)
		}
		time.Sleep(50 * time.Millisecond)
	}
	ids, err := client.LoadedThreadIDs(ctx)
	if err != nil || slices.Contains(ids, threads[1]) {
		t.Fatal("root activation loaded held member", ids, err)
	}
	queue, err := client.ListQueue(ctx, threads[1])
	if err != nil || len(queue) != 1 {
		t.Fatal("root activation consumed member queue", queue, err)
	}
}

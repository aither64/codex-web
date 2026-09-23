package codex

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func freshHeadlessMetadata(path, projectID string) map[string]any {
	return map[string]any{
		"id": "thread-1", "cwd": "/workspace/work/example", "projectId": projectID,
		"path": path, "source": "vscode", "ephemeral": false, "historyMode": "paginated",
		"preview": "", "status": map[string]any{"type": "notLoaded"}, "turns": []any{},
	}
}

func TestBootstrapHeadlessThreadReconcilesLostResponseWithoutDuplicate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	projectID := "project-1"
	marker := "Internal setup for thread-1; no assignment has been sent."
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		injections := 0
		for index := 0; index < 7; index++ {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			result := map[string]any{}
			switch request["method"] {
			case "thread/read":
				metadata := freshHeadlessMetadata(path, projectID)
				if injections == 0 {
					metadata["projectId"] = nil // A fresh pinned-binary thread has no persisted project yet.
				}
				result["thread"] = metadata
			case "thread/inject_items":
				injections++
				params := request["params"].(map[string]any)
				items := params["items"].([]any)
				item := items[0].(map[string]any)
				content := item["content"].([]any)[0].(map[string]any)
				if params["threadId"] != "thread-1" || len(items) != 1 || item["type"] != "message" || item["role"] != "developer" ||
					content["type"] != "input_text" || content["text"] != marker {
					return fmt.Errorf("wrong headless bootstrap item: %#v", params)
				}
				rollout := fmt.Sprintf("{\"type\":\"session_meta\",\"payload\":{\"id\":\"thread-1\"}}\n"+
					"{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"developer\",\"content\":[{\"type\":\"input_text\",\"text\":%q}]}}\n", marker)
				if err := os.WriteFile(path, []byte(rollout), 0o600); err != nil {
					return err
				}
				if err := writeObject(connection, map[string]any{"id": request["id"], "error": map[string]any{"code": -32600, "message": "response lost"}}); err != nil {
					return err
				}
				continue
			default:
				return fmt.Errorf("unexpected RPC %v", request["method"])
			}
			if err := writeObject(connection, map[string]any{"id": request["id"], "result": result}); err != nil {
				return err
			}
		}
		if injections != 1 {
			return fmt.Errorf("injected %d times", injections)
		}
		return nil
	})
	client := newTestClient(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for range 2 {
		if err := client.BootstrapHeadlessThread(ctx, "thread-1", "/workspace/work/example", projectID, marker); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRequireThreadIdleAcceptsOnlyExactFreshMissingRollout(t *testing.T) {
	for _, source := range []string{"vscode", "cli"} {
		t.Run(source, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "missing.jsonl")
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				methods := []string{"thread/read", "thread/turns/list", "thread/read"}
				if source == "vscode" {
					methods = append(methods, "thread/queue/list")
				}
				for index, method := range methods {
					request, err := readObject(connection)
					if err != nil || request["method"] != method {
						return fmt.Errorf("request %d = %#v: %v", index, request, err)
					}
					result := map[string]any{}
					if method == "thread/read" {
						metadata := freshHeadlessMetadata(path, "project-1")
						metadata["source"] = source
						result["thread"] = metadata
					} else if method == "thread/queue/list" {
						result["data"] = []any{}
					} else {
						if err := writeObject(connection, map[string]any{"id": request["id"], "error": map[string]any{
							"code": -32600, "message": "invalid paginated history lineage for thread-1: missing source rollout",
						}}); err != nil {
							return err
						}
						continue
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
			err := client.RequireThreadIdle(ctx, "thread-1", "/workspace/work/example")
			if (err == nil) != (source == "vscode") {
				t.Fatalf("source %s idle check = %v", source, err)
			}
			if source != "vscode" && (err == nil || !strings.Contains(err.Error(), "missing source rollout")) {
				t.Fatalf("unexpected rejection: %v", err)
			}
		})
	}
}

func TestForkedHeadlessBootstrapRequiresExactSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.jsonl")
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil || request["method"] != "thread/read" {
			return fmt.Errorf("expected thread/read: %#v, %v", request, err)
		}
		metadata := freshHeadlessMetadata(path, "source-project")
		metadata["forkedFromId"] = "different-source"
		return writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{"thread": metadata}})
	})
	client := newTestClient(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.BootstrapForkedHeadlessThread(ctx, "thread-1", "/workspace/work/example", "expected-source",
		"Internal setup for thread-1; no assignment has been sent."); err == nil || !strings.Contains(err.Error(), "wrong identity") {
		t.Fatalf("wrong fork source was accepted: %v", err)
	}
}

func TestVerifyForkedBootstrapScansLargeInheritedHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fork-rollout.jsonl")
	marker := "Internal setup for thread-1; no assignment has been sent."
	largeInheritedItem := strings.Repeat("x", 2*1024*1024)
	rollout := fmt.Sprintf("{\"type\":\"session_meta\",\"payload\":{\"id\":\"thread-1\"}}\n"+
		"{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":%q}]}}\n"+
		"{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"developer\",\"content\":[{\"type\":\"input_text\",\"text\":%q}]}}\n",
		largeInheritedItem, marker)
	if err := os.WriteFile(path, []byte(rollout), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := verifyHeadlessBootstrapRollout(path, "thread-1", marker); err != nil {
		t.Fatal(err)
	}
}

func TestPinnedCodexHeadlessTeamContract(t *testing.T) {
	binary := os.Getenv("CODEX_WEB_TEST_BINARY")
	if binary == "" {
		t.Skip("CODEX_WEB_TEST_BINARY is not configured")
	}
	root := t.TempDir()
	home := filepath.Join(root, "home")
	codexHome := filepath.Join(home, ".codex")
	workspace := filepath.Join(root, "workspace")
	sourceCwd := filepath.Join(workspace, "work", "source")
	forkCwd := filepath.Join(workspace, "work", "fork")
	for _, path := range []string{codexHome, sourceCwd, forkCwd} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	socket := filepath.Join(root, "app-server.sock")
	command := exec.Command(binary, "app-server", "--listen", "unix://"+socket)
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "HOME=") && !strings.HasPrefix(entry, "CODEX_HOME=") {
			command.Env = append(command.Env, entry)
		}
	}
	command.Env = append(command.Env, "HOME="+home, "CODEX_HOME="+codexHome)
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
		}
		_ = command.Wait()
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		if info, err := os.Stat(socket); err == nil && info.Mode()&os.ModeSocket != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pinned Codex App Server did not start: %s", output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	newClient := func() *Client {
		return NewWithOptions(socket, ClientOptions{RuntimeWorkspaceRoots: []string{workspace}, ThreadSourceKinds: []string{"vscode"}})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sourceEnvironment := map[string]string{
		"DEV_SESSION_SLUG": "source", "DEV_SESSION_WORKSPACE": workspace,
		"DEV_SESSION_WORK_DIR": sourceCwd, "DEV_SESSION_MEMBER_ADDRESS": "architect0",
	}
	forkEnvironment := map[string]string{
		"DEV_SESSION_SLUG": "fork", "DEV_SESSION_WORKSPACE": workspace,
		"DEV_SESSION_WORK_DIR": forkCwd, "DEV_SESSION_MEMBER_ADDRESS": "architect0",
	}
	client := newClient()
	project, err := client.CreateProject(ctx, "source architect0", "headless-contract-project")
	if err != nil {
		t.Fatalf("register project: %v\n%s", err, output.String())
	}
	threadID, err := client.StartThreadWithSettings(ctx, sourceCwd, sourceEnvironment, ThreadSettings{ProjectID: project.ID})
	if err != nil {
		t.Fatalf("start member: %v\n%s", err, output.String())
	}
	marker := "Internal team member initialization for thread " + threadID + ". No assignment has been sent yet."
	if err := client.BootstrapHeadlessThread(ctx, threadID, sourceCwd, project.ID, marker); err != nil {
		metadata, readErr := client.ReadThreadMetadata(ctx, threadID, false)
		t.Fatalf("bootstrap member: %v; metadata=%+v; read error=%v\n%s", err, metadata, readErr, output.String())
	}
	metadata, err := client.ReadThreadMetadata(ctx, threadID, false)
	if err != nil || metadata.ProjectID == nil || *metadata.ProjectID != project.ID {
		t.Fatalf("bootstrap did not persist project identity: metadata=%+v err=%v", metadata, err)
	}
	client.Close()
	client = newClient()
	if _, err := client.ResumeThreadWithSettings(ctx, threadID, sourceCwd, sourceEnvironment, ThreadSettings{}); err != nil {
		t.Fatalf("resume member after client disconnect: %v\n%s", err, output.String())
	}
	forkID, err := client.ForkThread(ctx, threadID, forkCwd, forkEnvironment, ThreadSettings{})
	if err != nil {
		t.Fatalf("fork member: %v\n%s", err, output.String())
	}
	forkMarker := "Internal team member initialization for thread " + forkID + ". No assignment has been sent yet."
	if err := client.BootstrapForkedHeadlessThread(ctx, forkID, forkCwd, threadID, forkMarker); err != nil {
		t.Fatalf("bootstrap fork: %v\n%s", err, output.String())
	}
	client.Close()
	client = newClient()
	defer client.Close()
	if _, err := client.ResumeThreadWithSettings(ctx, forkID, forkCwd, forkEnvironment, ThreadSettings{}); err != nil {
		t.Fatalf("resume fork after client disconnect: %v\n%s", err, output.String())
	}
	if err := client.RequireThreadIdle(ctx, forkID, forkCwd); err != nil {
		t.Fatalf("fork is not idle: %v\n%s", err, output.String())
	}
}

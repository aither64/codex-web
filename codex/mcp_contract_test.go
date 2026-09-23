package codex

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPinnedMCPContractServer(t *testing.T) {
	if os.Getenv("CODEX_MCP_PROBE_SERVER") != "1" {
		return
	}
	reader := bufio.NewScanner(os.Stdin)
	writer := bufio.NewWriter(os.Stdout)
	for reader.Scan() {
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal(reader.Bytes(), &request); err != nil || request.Method == "" {
			continue
		}
		if len(request.ID) == 0 {
			continue
		}
		var result any
		switch request.Method {
		case "initialize":
			result = map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]any{"name": "team-probe", "version": "1"}}
		case "tools/list":
			result = map[string]any{"tools": []any{map[string]any{"name": "report", "description": "Record a test report using the host-side tool.", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{"text": map[string]any{"type": "string"}}, "required": []string{"text"}}}}}
		case "tools/call":
			var params struct {
				Name      string `json:"name"`
				Arguments struct {
					Text string `json:"text"`
				} `json:"arguments"`
			}
			if err := json.Unmarshal(request.Params, &params); err != nil || params.Name != "report" || params.Arguments.Text == "" {
				result = map[string]any{"isError": true, "content": []any{map[string]any{"type": "text", "text": "invalid report"}}}
			} else {
				path := os.Getenv("CODEX_MCP_PROBE_MARKER")
				file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
				if err == nil {
					_, err = fmt.Fprintln(file, params.Arguments.Text)
					_ = file.Close()
				}
				result = map[string]any{"isError": err != nil, "content": []any{map[string]any{"type": "text", "text": "recorded by host-side MCP server"}}}
			}
		default:
			result = map[string]any{}
		}
		encoded, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result})
		if err != nil {
			t.Fatal(err)
		}
		_, _ = writer.Write(encoded)
		_ = writer.WriteByte('\n')
		_ = writer.Flush()
	}
}

// TestPinnedMCPContract checks the pinned App Server's host-side MCP tool
// binding across a reconnect and explicit resume. It uses an authenticated
// Codex home and a live model, so it is opt-in and runs only after review.
func TestPinnedMCPContract(t *testing.T) {
	binary := os.Getenv("CODEX_WEB_TEST_BINARY")
	if binary == "" {
		t.Skip("CODEX_WEB_TEST_BINARY is not configured")
	}
	root := t.TempDir()
	cwd := filepath.Join(root, "workspace", "work", "member")
	if err := os.MkdirAll(cwd, 0o700); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(root, "app-server.sock")
	marker := filepath.Join(root, "host-marker")
	command := exec.Command(binary, "app-server", "--listen", "unix://"+socket)
	command.Env = append(os.Environ(), "CODEX_MCP_PROBE_SERVER=1", "CODEX_MCP_PROBE_MARKER="+marker)
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = command.Process.Kill(); _ = command.Wait() })
	for deadline := time.Now().Add(10 * time.Second); ; {
		if info, err := os.Stat(socket); err == nil && info.Mode()&os.ModeSocket != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("App Server did not start: %s", output.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	newClient := func() *Client {
		return NewWithOptions(socket, ClientOptions{RuntimeWorkspaceRoots: []string{filepath.Join(root, "workspace")}, ThreadSourceKinds: []string{"vscode"}})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client := newClient()
	project, err := client.CreateProject(ctx, "MCP probe member", fmt.Sprintf("mcp-probe-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	testBinary, err := filepath.Abs(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	policy := ThreadPolicy{Sandbox: "read-only", MCPServer: &ThreadMCPServer{
		Name: "team_probe", Command: testBinary, Args: []string{"-test.run=^TestPinnedMCPContractServer$"}, Tool: "report",
	}}
	threadID, err := client.StartThreadWithSettings(ctx, cwd, nil, ThreadSettings{
		Model: "gpt-6-luna", ReasoningEffort: "low", ProjectID: project.ID, Policy: policy,
	})
	if err != nil {
		t.Fatalf("thread/start: %v\n%s", err, output.String())
	}
	t.Logf("thread=%s", threadID)
	bootstrap := "Internal MCP contract bootstrap for thread " + threadID + ". No assignment has been sent yet."
	if err := client.BootstrapHeadlessThread(ctx, threadID, cwd, project.ID, bootstrap); err != nil {
		t.Fatalf("bootstrap: %v\n%s", err, output.String())
	}
	for index, expected := range []string{"first-probe", "second-probe"} {
		if index == 1 {
			client.Close()
			client = newClient()
			if _, err := client.ResumeThreadWithSettings(ctx, threadID, cwd, nil, ThreadSettings{Policy: policy}); err != nil {
				t.Fatalf("resume with MCP config: %v", err)
			}
		}
		prompt := fmt.Sprintf("Use the team_probe report MCP tool exactly once with text %q. Then say done.", expected)
		id := fmt.Sprintf("00000000-0000-4000-8000-%012d", index+1)
		if _, err := client.SendWithOptions(ctx, threadID, prompt, id, "mcp-probe", TurnOptions{Model: "gpt-6-luna", ReasoningEffort: "low", ThreadPolicy: policy}); err != nil {
			t.Fatalf("send %d: %v\n%s", index, err, output.String())
		}
		for {
			body, _ := os.ReadFile(marker)
			if strings.Contains(string(body), expected) {
				t.Logf("MCP tool recorded %s", expected)
				break
			}
			if active, readErr := client.ActiveTurnID(ctx, threadID); readErr == nil && active == "" {
				transcript, transcriptErr := client.ReadThread(ctx, threadID)
				t.Fatalf("turn ended without MCP marker %s: transcript=%+v, read error=%v\n%s", expected, transcript.Entries, transcriptErr, output.String())
			}
			if err := ctx.Err(); err != nil {
				t.Fatalf("MCP tool never recorded %s: %v; marker=%q\n%s", expected, err, string(body), output.String())
			}
			time.Sleep(500 * time.Millisecond)
		}
		for {
			active, err := client.ActiveTurnID(ctx, threadID)
			if err == nil && active == "" {
				break
			}
			if err := ctx.Err(); err != nil {
				t.Fatalf("turn %d did not finish: %v", index, err)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	client.Close()
}

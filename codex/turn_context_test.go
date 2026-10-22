package codex

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Safe native 0.163.0 field shapes; no prompt, developer text or private paths.
const nativeContextFixture = `{"type":"turn_context","payload":{"turn_id":"queued-turn","cwd":"/workspace","model":"execution-model","effort":"xhigh","collaboration_mode":{"mode":"plan","settings":{"developer_instructions":"must not be projected"}},"approval_policy":"never","approvals_reviewer":"user","sandbox_policy":{"type":"danger-full-access"},"permission_profile":{"type":"disabled"}}}`
const nativeSessionFixture = `{"type":"session_meta","payload":{"id":"thread-1","cwd":"/workspace"}}`

func TestReadExactTurnContextMatchesExecutionAndHidesUnrelatedPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	other := strings.ReplaceAll(nativeContextFixture, "queued-turn", "later-turn")
	other = strings.ReplaceAll(other, "execution-model", "current-default-model")
	text := nativeSessionFixture + "\n" + nativeContextFixture + "\n" + nativeContextFixture + "\n" + other + "\n"
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	value, err := readExactTurnContext(context.Background(), path, "thread-1", "queued-turn", "/workspace")
	if err != nil || value.Model != "execution-model" || value.CollaborationMode != "plan" || value.ReasoningEffort != "xhigh" {
		t.Fatalf("value=%+v err=%v", value, err)
	}
	encoded, _ := json.Marshal(value)
	if strings.Contains(string(encoded), "must not be projected") || strings.Contains(string(encoded), "current-default-model") {
		t.Fatal("unrelated native context leaked")
	}
	if string(value.PermissionProfile) != `{"type":"disabled"}` {
		t.Fatalf("profile=%s", value.PermissionProfile)
	}
}

func TestReadExactTurnContextRejectsMissingPartialForeignAndConflictingRecords(t *testing.T) {
	for _, test := range []struct{ name, text string }{
		{"missing", nativeSessionFixture + "\n"},
		{"foreign session", strings.ReplaceAll(nativeSessionFixture, "thread-1", "foreign") + "\n" + nativeContextFixture + "\n"},
		{"wrong cwd", strings.ReplaceAll(nativeSessionFixture, "/workspace", "/other") + "\n" + nativeContextFixture + "\n"},
		{"foreign turn", nativeSessionFixture + "\n" + strings.ReplaceAll(nativeContextFixture, "queued-turn", "other-turn") + "\n"},
		{"partial", nativeSessionFixture + "\n" + nativeContextFixture},
		{"partial following record", nativeSessionFixture + "\n" + nativeContextFixture + "\n" + `{"type":`},
		{"conflict", nativeSessionFixture + "\n" + nativeContextFixture + "\n" + strings.ReplaceAll(nativeContextFixture, "execution-model", "different-model") + "\n"},
		{"invalid policy", nativeSessionFixture + "\n" + strings.ReplaceAll(nativeContextFixture, "danger-full-access", "unknown") + "\n"},
		{"incomplete", nativeSessionFixture + "\n" + strings.ReplaceAll(nativeContextFixture, `"effort":"xhigh",`, "") + "\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "rollout.jsonl")
			if err := os.WriteFile(path, []byte(test.text), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := readExactTurnContext(context.Background(), path, "thread-1", "queued-turn", "/workspace"); err == nil {
				t.Fatal("unproved execution context accepted")
			}
			if test.name == "partial" {
				file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.WriteString("\n")
				file.Close()
				if _, err := readExactTurnContext(context.Background(), path, "thread-1", "queued-turn", "/workspace"); err != nil {
					t.Fatalf("completed append did not recover: %v", err)
				}
			}
		})
	}
}

func TestReadExactTurnContextBeyondDisplayTailAndCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	file.WriteString(nativeSessionFixture + "\n" + nativeContextFixture + "\n")
	padding := `{"type":"response_item","payload":"` + strings.Repeat("x", 1024*1024) + "\"}\n"
	for written := 0; written < readLimit; written += len(padding) {
		if _, err := file.WriteString(padding); err != nil {
			t.Fatal(err)
		}
	}
	file.Close()
	value, err := readExactTurnContext(context.Background(), path, "thread-1", "queued-turn", "/workspace")
	if err != nil || value.TurnID != "queued-turn" {
		t.Fatalf("outside fixed suffix: %+v %v", value, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := readExactTurnContext(ctx, path, "thread-1", "queued-turn", "/workspace"); err == nil {
		t.Fatal("cancelled read accepted")
	}
}

func TestNativeContextPoliciesValidatePinnedShapes(t *testing.T) {
	managed := `{"type":"managed","file_system":{"type":"restricted","entries":[{"path":{"type":"special","value":{"kind":"root"}},"access":"read"},{"path":{"type":"path","path":"/workspace"},"access":"write"},{"path":{"type":"special","value":{"kind":"slash_tmp"}},"access":"write"},{"path":{"type":"path","path":"/optional"},"access":"read","missing_path_behavior":"skip"}]},"network":"restricted"}`
	if _, err := validatedNativePermissionProfile(json.RawMessage(managed)); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{`{"type":"workspace-write","writable_roots":["/workspace"],"network_access":false,"exclude_tmpdir_env_var":true,"exclude_slash_tmp":false}`, `{"type":"read-only"}`, `{"type":"external-sandbox","network_access":"restricted"}`} {
		if _, err := validatedNativeSandbox(json.RawMessage(raw)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := validatedNativeApproval(json.RawMessage(`{"granular":{"mcp_elicitations":false,"rules":true,"sandbox_approval":true}}`)); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{`{"type":"managed","file_system":{},"network":"restricted"}`, `{"type":"disabled","secret":"hidden"}`, `{"type":"custom"}`} {
		if _, err := validatedNativePermissionProfile(json.RawMessage(raw)); err == nil {
			t.Fatalf("unvalidated native profile accepted: %s", raw)
		}
	}
}

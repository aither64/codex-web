//go:build codex_integration

package codex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const (
	nativeUpgradeOldSHA = "8d733334a9ce033fd57f0e3c08064d2b1ec549d7b7f29949ae44b24e87a7c15a"
	nativeUpgradeNewSHA = "d2af1cd0ea4187d413c237e80204e94ffcd5c7d8142840a662156b89750f54ae"
)

type nativeUpgradeBinary struct {
	Path    string `json:"path"`
	SHA256  string `json:"sha256"`
	Version string `json:"version"`
}

type nativeUpgradeState struct {
	Root, Held, Fork              string
	RootProject, HeldProject      string
	OldReceipt, NewReceipt        SendReceipt
	OldUser, OldAnswer, NewUser   string
	NewAnswer                     string
	Queue                         []QueueEntry
	OldMessages, UpgradedMessages []TranscriptEntry
}

type nativeUpgradeOutcome struct {
	Status       string   `json:"status"`
	ProviderHits int64    `json:"providerHits"`
	ChangedPaths []string `json:"changedPaths,omitempty"`
}

type nativeUpgradeBootstrapCall struct {
	Phase, Role, Destination, Project, Source, Marker, Error string
	Truncated                                                bool
}

type nativeUpgradeFailureSnapshot struct {
	Phase, Path, Status, HomeError, ReceiptsError string
}

// These tests use only the two reviewed binaries and synthetic state. Passing
// the old-reader phase establishes cold reads, not old-version write or rollback
// safety. The application recovery contract remains forward-only.
func TestNativeStateUpgrade0163To0164(t *testing.T) {
	old, err := nativeUpgradeBinaryFromEnv("OLD", nativeUpgradeOldSHA, "codex-cli 0.163.0")
	if err != nil {
		t.Fatal(err)
	}
	newBinary, err := nativeUpgradeBinaryFromEnv("NEW", nativeUpgradeNewSHA, "codex-cli 0.164.0")
	if err != nil {
		t.Fatal(err)
	}
	provenance := make(map[string]string)
	for _, name := range []string{"CODEX_UPGRADE_CLIENT_REVISION", "CODEX_UPGRADE_OLD_PROVENANCE", "CODEX_UPGRADE_NEW_PROVENANCE"} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			provenance[name] = value
		} else {
			t.Fatalf("%s must identify the reviewed client or source/patch receipt", name)
		}
	}
	clientRevision := provenance["CODEX_UPGRADE_CLIENT_REVISION"]
	if decoded, err := hex.DecodeString(clientRevision); err != nil || len(decoded) != 20 || strings.ToLower(clientRevision) != clientRevision {
		t.Fatal("CODEX_UPGRADE_CLIENT_REVISION must be the exact lowercase Git commit")
	}
	// The reused harness must not skip this explicitly selected cross-version test.
	t.Setenv("CODEX_WEB_TEST_BINARY", old.Path)
	var hits atomic.Int64
	h := newRecoveryIntegrationHarness(t, "thread_unload_delay_secs = 0\n", "", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/responses" {
			http.NotFound(w, r)
			return
		}
		index := hits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		event := func(value any) { data, _ := json.Marshal(value); _, _ = fmt.Fprintf(w, "data: %s\n\n", data) }
		id := fmt.Sprintf("upgrade-response-%d", index)
		event(map[string]any{"type": "response.created", "response": map[string]any{"id": id}})
		event(map[string]any{"type": "response.output_item.done", "item": map[string]any{
			"type": "message", "role": "assistant", "id": "answer-" + id, "phase": "final_answer",
			"content": []any{map[string]any{"type": "output_text", "text": fmt.Sprintf("Synthetic upgrade answer %d.", index)}},
		}})
		event(map[string]any{"type": "response.completed", "response": map[string]any{"id": id,
			"usage": map[string]any{"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}}})
	}))
	// Retain evidence outside the harness root, which its ordinary cleanup removes.
	evidence, err := os.MkdirTemp("/tmp", "codex-native-upgrade-evidence-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("retained private native upgrade evidence: %s", evidence)
	manifest := struct {
		Old, New              nativeUpgradeBinary
		Provenance            map[string]string
		FixtureHome           string
		Command               []string
		State                 nativeUpgradeState
		Upgrade, OldReader    nativeUpgradeOutcome
		OldReaderCoverageOnly string
		BootstrapCalls        []nativeUpgradeBootstrapCall
		FailureSnapshots      []nativeUpgradeFailureSnapshot
	}{Old: old, New: newBinary, Provenance: provenance, FixtureHome: h.home, Command: os.Args,
		Upgrade: nativeUpgradeOutcome{Status: "not run"}, OldReader: nativeUpgradeOutcome{Status: "not run"},
		OldReaderCoverageOnly: "Cold reads only; no downgrade, old-version writes or mixed-version safety claim."}
	defer func() {
		nativeUpgradeWriteJSON(t, filepath.Join(evidence, "assertions.json"), manifest)
		if data, err := os.ReadFile(h.log.Name()); err == nil {
			if err := os.WriteFile(filepath.Join(evidence, "native.log"), data, 0600); err != nil {
				t.Error(err)
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	for _, binary := range []nativeUpgradeBinary{old, newBinary} {
		command := exec.CommandContext(ctx, binary.Path, "--version")
		command.Dir, command.Env = h.cwd, h.environment()
		integrationRequireVersion(t, command, binary.Version)
	}
	receipts := filepath.Join(h.root, "client-receipts")
	if err := os.Mkdir(receipts, 0700); err != nil {
		t.Fatal(err)
	}
	newClient := func() *Client {
		return NewWithOptions(h.socket, ClientOptions{
			ThreadSourceKinds:      []string{"vscode"},
			SubmissionDatabasePath: filepath.Join(receipts, "submissions.sqlite"),
			SubmissionLedgerPath:   filepath.Join(receipts, "predecessor.json"),
			AllowImplicitResume:    func(string) bool { return false },
		})
	}
	recordBootstrap := func(phase, role, destination, project, source, marker string, err error) {
		call := nativeUpgradeBootstrapCall{Phase: phase, Role: role}
		for field, value := range map[*string]string{&call.Destination: destination, &call.Project: project,
			&call.Source: source, &call.Marker: marker, &call.Error: fmt.Sprint(err)} {
			*field = integrationDiagnosticText(value, 4096)
			call.Truncated = call.Truncated || len(value) > 4096
		}
		manifest.BootstrapCalls = append(manifest.BootstrapCalls, call)
		if err != nil {
			t.Logf("native bootstrap failed: %+v", call)
		}
	}
	var startedCommands []*exec.Cmd
	startNative := func() {
		h.start()
		startedCommands = append(startedCommands, h.command)
	}
	// Registered before client.Close defers: all owned clients close first.
	// Even if stop refuses the join, retain an explicit incomplete disposition.
	finishPhase := func(t *testing.T, phase string) {
		command := h.command
		defer func() {
			if !t.Failed() || len(startedCommands) == 0 && command == nil {
				return
			}
			joined := h.command == nil
			for _, started := range append(append([]*exec.Cmd{}, startedCommands...), command) {
				if started != nil && (started.Process == nil || started.ProcessState == nil) {
					joined = false
				}
			}
			snapshot := nativeUpgradeRetainFailure(h.home, receipts, filepath.Join(evidence, "failure-"+phase), joined)
			snapshot.Phase = phase
			manifest.FailureSnapshots = append(manifest.FailureSnapshots, snapshot)
			t.Logf("stopped fixture failure snapshot: %+v", snapshot)
		}()
		nativeUpgradeStop(t, h)
	}
	var baseline map[string]string
	upgradePassed := t.Run("upgrade", func(t *testing.T) {
		h.t = t
		defer finishPhase(t, "upgrade")
		h.binary = old.Path
		startNative()
		client := newClient()
		defer client.Close()
		state := &manifest.State
		for _, target := range []struct {
			name            string
			thread, project *string
		}{
			{"root", &state.Root, &state.RootProject}, {"held", &state.Held, &state.HeldProject},
		} {
			project, err := client.CreateProject(ctx, "Native upgrade "+target.name, "native-upgrade-project-"+target.name)
			if err != nil {
				t.Fatal(err)
			}
			*target.project = project.ID
			id, err := client.StartThreadWithSettings(ctx, h.cwd, map[string]string{}, ThreadSettings{ProjectID: project.ID})
			if err != nil {
				t.Fatal(err)
			}
			*target.thread = id
			marker := nativeUpgradeBootstrapMarker(target.name, id)
			err = client.BootstrapHeadlessThread(ctx, id, h.cwd, project.ID, marker)
			recordBootstrap("seed-0163", target.name, id, project.ID, "", marker, err)
			if err != nil {
				t.Fatal(err)
			}
		}
		state.OldUser, state.OldAnswer = "Synthetic request created by 0.163.", "Synthetic upgrade answer 1."
		state.OldReceipt, err = client.Send(ctx, state.Root, state.OldUser, "native-upgrade-old-message", "")
		if err != nil {
			t.Fatal(err)
		}
		oldTranscript := nativeUpgradeWaitTurn(t, ctx, client, state.Root, state.OldReceipt, state.OldUser, state.OldAnswer)
		state.OldMessages = nativeUpgradeMessages(oldTranscript)
		var ignored any
		if err := client.Request(ctx, "thread/unsubscribe", map[string]any{"threadId": state.Held}, &ignored); err != nil {
			t.Fatal(err)
		}
		nativeUpgradeWaitUnloaded(t, ctx, client, state.Held)
		queued, err := client.Queue(ctx, state.Held, "Held native upgrade request; do not execute.", "native-upgrade-held-message")
		if err != nil {
			t.Fatal(err)
		}
		state.Queue = []QueueEntry{queued}
		nativeUpgradeCheckState(t, ctx, client, h.cwd, *state, false)
		nativeUpgradeQuiet(t, ctx, client, &hits, 1, false)
		client.Close()
		nativeUpgradeStop(t, h)

		h.binary = newBinary.Path
		startNative()
		client = newClient()
		defer client.Close()
		nativeUpgradeCheckState(t, ctx, client, h.cwd, *state, false)
		nativeUpgradeQuiet(t, ctx, client, &hits, 1, true)
		resumed, err := client.ResumeThreadWithSettings(ctx, state.Root, h.cwd, map[string]string{}, ThreadSettings{})
		if err != nil || resumed != state.Root {
			t.Fatalf("resume original root: %q %v", resumed, err)
		}
		state.NewUser, state.NewAnswer = "Synthetic request created by 0.164.", "Synthetic upgrade answer 2."
		state.NewReceipt, err = client.Send(ctx, state.Root, state.NewUser, "native-upgrade-new-message", "")
		if err != nil {
			t.Fatal(err)
		}
		upgraded := nativeUpgradeWaitTurn(t, ctx, client, state.Root, state.NewReceipt, state.NewUser, state.NewAnswer)
		state.UpgradedMessages = nativeUpgradeMessages(upgraded)
		nativeUpgradeRequireMessages(t, upgraded, state.OldMessages)
		state.Fork, err = client.ForkThread(ctx, state.Root, h.cwd, map[string]string{}, ThreadSettings{})
		if err != nil || state.Fork == "" || state.Fork == state.Root || state.Fork == state.Held {
			t.Fatalf("distinct fork identity: %q %v", state.Fork, err)
		}
		marker := nativeUpgradeBootstrapMarker("fork", state.Fork)
		err = client.BootstrapForkedHeadlessThread(ctx, state.Fork, h.cwd, state.Root, marker)
		recordBootstrap("upgrade-0164", "fork", state.Fork, "", state.Root, marker, err)
		if err != nil {
			t.Fatal(err)
		}
		nativeUpgradeCheckState(t, ctx, client, h.cwd, *state, true)
		nativeUpgradeQuiet(t, ctx, client, &hits, 2, false)
		client.Close()
		nativeUpgradeStop(t, h)
		startNative()
		client = newClient()
		defer client.Close()
		nativeUpgradeCheckState(t, ctx, client, h.cwd, *state, true)
		nativeUpgradeQuiet(t, ctx, client, &hits, 2, true)
		for _, thread := range []string{state.Root, state.Held, state.Fork} {
			data, err := client.ExportSubmissionAttempts(thread)
			if err != nil {
				t.Fatal(err)
			}
			nativeUpgradeWriteJSON(t, filepath.Join(evidence, "receipts-"+thread+".json"), data)
		}
		client.Close()
		nativeUpgradeStop(t, h)
		snapshot := filepath.Join(evidence, "before-old-reader")
		if err := nativeUpgradeCopyTree(h.home, filepath.Join(snapshot, "home")); err != nil {
			t.Fatal(err)
		}
		if err := nativeUpgradeCopyTree(receipts, filepath.Join(snapshot, "client-receipts")); err != nil {
			t.Fatal(err)
		}
		baseline, err = nativeUpgradeFixtureInventory(h.home, receipts)
		if err != nil {
			t.Fatal(err)
		}
		nativeUpgradeWriteJSON(t, filepath.Join(snapshot, "state-files.json"), baseline)
		nativeUpgradeWriteJSON(t, filepath.Join(snapshot, "assertions.json"), manifest.State)
	})
	h.t = t
	manifest.Upgrade = nativeUpgradeOutcome{Status: "failed", ProviderHits: hits.Load()}
	if !upgradePassed {
		return
	}
	manifest.Upgrade.Status = "passed"
	nativeUpgradeWriteJSON(t, filepath.Join(evidence, "upgrade-result.json"), manifest)

	oldReaderPassed := t.Run("old-reader-cold-only", func(t *testing.T) {
		h.t = t
		defer finishPhase(t, "old-reader")
		h.binary = old.Path
		startNative()
		client := newClient()
		defer client.Close()
		nativeUpgradeCheckState(t, ctx, client, h.cwd, manifest.State, true)
		nativeUpgradeQuiet(t, ctx, client, &hits, manifest.Upgrade.ProviderHits, true)
	})
	h.t = t
	if hits.Load() != manifest.Upgrade.ProviderHits {
		t.Error("old-reader phase performed provider inference")
		oldReaderPassed = false
	}
	manifest.OldReader = nativeUpgradeOutcome{Status: "failed", ProviderHits: hits.Load()}
	if oldReaderPassed {
		manifest.OldReader.Status = "passed cold reads only"
	}
	after, err := nativeUpgradeFixtureInventory(h.home, receipts)
	if err != nil {
		t.Error(err)
	} else {
		manifest.OldReader.ChangedPaths = nativeUpgradeChanged(baseline, after)
		nativeUpgradeWriteJSON(t, filepath.Join(evidence, "after-old-reader-files.json"), after)
	}
	if err := nativeUpgradeCopyTree(h.home, filepath.Join(evidence, "after-old-reader-home")); err != nil {
		t.Error(err)
	}
	if err := nativeUpgradeCopyTree(receipts, filepath.Join(evidence, "after-old-reader-client-receipts")); err != nil {
		t.Error(err)
	}
	t.Log("Old-reader results cover public cold reads only; recovery remains forward-only.")
}

func nativeUpgradeBootstrapMarker(role, destination string) string {
	return fmt.Sprintf("Synthetic native upgrade bootstrap %s (thread %s).", role, destination)
}

// Diagnostic retention only; partial copies never certify a complete snapshot.
// The caller first closes clients and joins every owned native command.
func nativeUpgradeRetainFailure(home, receipts, destination string, joined bool) nativeUpgradeFailureSnapshot {
	result := nativeUpgradeFailureSnapshot{Path: destination, Status: "refused-unjoined"}
	if !joined {
		return result
	}
	result.Status = "incomplete"
	if err := nativeUpgradeCopyTree(home, filepath.Join(destination, "home")); err != nil {
		result.HomeError = integrationDiagnosticText(err.Error(), 4096)
	}
	if err := nativeUpgradeCopyTree(receipts, filepath.Join(destination, "client-receipts")); err != nil {
		result.ReceiptsError = integrationDiagnosticText(err.Error(), 4096)
	}
	if result.HomeError == "" && result.ReceiptsError == "" {
		result.Status = "complete"
	}
	return result
}

func TestNativeUpgradeBootstrapMarkers(t *testing.T) {
	for _, test := range []struct{ role, destination, project, source string }{
		{"root", "returned-root", "root-project", ""},
		{"held", "returned-held", "held-project", ""},
		{"fork", "returned-fork", "", "returned-root"},
	} {
		t.Run(test.role, func(t *testing.T) {
			marker := nativeUpgradeBootstrapMarker(test.role, test.destination)
			path := filepath.Join(t.TempDir(), "rollout.jsonl")
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				injections := 0
				for range 4 {
					request, err := readObject(connection)
					if err != nil {
						return err
					}
					params := request["params"].(map[string]any)
					if params["threadId"] != test.destination {
						return fmt.Errorf("wrong bootstrap destination: %v", params)
					}
					result := map[string]any{}
					switch request["method"] {
					case "thread/read":
						metadata := freshHeadlessMetadata(path, test.project)
						metadata["id"], metadata["forkedFromId"] = test.destination, test.source
						result["thread"] = metadata
					case "thread/inject_items":
						injections++
						item := map[string]any{"type": "message", "role": "developer", "content": []any{map[string]any{"type": "input_text", "text": marker}}}
						if !reflect.DeepEqual(params["items"], []any{item}) {
							return fmt.Errorf("wrong developer bootstrap item: %v", params)
						}
						rollout := fmt.Sprintf("{\"type\":\"session_meta\",\"payload\":{\"id\":%q}}\n", test.destination)
						encoded, _ := json.Marshal(map[string]any{"type": "response_item", "payload": item})
						if err := os.WriteFile(path, append([]byte(rollout), append(encoded, '\n')...), 0600); err != nil {
							return err
						}
					default:
						return fmt.Errorf("unexpected bootstrap RPC %v", request["method"])
					}
					if err := writeObject(connection, map[string]any{"id": request["id"], "result": result}); err != nil {
						return err
					}
				}
				if injections != 1 {
					return fmt.Errorf("bootstrap injected %d times", injections)
				}
				return nil
			})
			client := newTestClient(socket)
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var err error
			if test.source == "" {
				err = client.BootstrapHeadlessThread(ctx, test.destination, "/workspace/work/example", test.project, marker)
			} else {
				err = client.BootstrapForkedHeadlessThread(ctx, test.destination, "/workspace/work/example", test.source, marker)
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, test := range []struct{ role, destination, marker string }{
		{"root", "returned-root", "Synthetic native upgrade bootstrap root"},
		{"held", "returned-held", "Synthetic native upgrade bootstrap held"},
		{"fork", "returned-fork", "Synthetic native upgrade fork bootstrap"},
		{"source-only-fork", "returned-fork", nativeUpgradeBootstrapMarker("fork", "returned-root")},
	} {
		t.Run("refuse-"+test.role, func(t *testing.T) {
			client := newTestClient(filepath.Join(t.TempDir(), "absent.sock"))
			defer client.Close()
			var err error
			if test.role == "root" || test.role == "held" {
				err = client.BootstrapHeadlessThread(context.Background(), test.destination, "/workspace/work/example", "project-"+test.role, test.marker)
			} else {
				err = client.BootstrapForkedHeadlessThread(context.Background(), test.destination, "/workspace/work/example", "returned-root", test.marker)
			}
			if err == nil || err.Error() != "headless bootstrap marker must identify the thread" {
				t.Fatalf("invalid destination marker was not refused before RPC: %v", err)
			}
		})
	}
}

func TestNativeUpgradeFailureRetention(t *testing.T) {
	root := t.TempDir()
	home, receipts := filepath.Join(root, "home"), filepath.Join(root, "receipts")
	for _, path := range []string{home, receipts} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "state.sqlite-wal"), []byte("synthetic state"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	refused := filepath.Join(root, "refused")
	if result := nativeUpgradeRetainFailure(home, receipts, refused, false); result.Status != "refused-unjoined" {
		t.Fatalf("unjoined fixture allowed a snapshot: %+v", result)
	}
	if _, err := os.Stat(refused); !os.IsNotExist(err) {
		t.Fatal("unjoined fixture snapshot touched its destination")
	}
	snapshot := filepath.Join(root, "complete")
	if result := nativeUpgradeRetainFailure(home, receipts, snapshot, true); result.Status != "complete" {
		t.Fatalf("stopped fixture snapshot failed: %+v", result)
	}
	before, err := nativeUpgradeFixtureInventory(home, receipts)
	if err != nil {
		t.Fatal(err)
	}
	after, err := nativeUpgradeFixtureInventory(filepath.Join(snapshot, "home"), filepath.Join(snapshot, "client-receipts"))
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("failure snapshot changed the private state inventory")
	}
	if result := nativeUpgradeRetainFailure(home, filepath.Join(root, "missing"), filepath.Join(root, "incomplete"), true); result.Status != "incomplete" || result.ReceiptsError == "" {
		t.Fatalf("partial failure snapshot was labeled complete: %+v", result)
	}
}

func nativeUpgradeBinaryFromEnv(role, pinnedSHA, version string) (nativeUpgradeBinary, error) {
	binary := nativeUpgradeBinary{Path: os.Getenv("CODEX_UPGRADE_" + role + "_BINARY"), SHA256: os.Getenv("CODEX_UPGRADE_" + role + "_SHA256"), Version: version}
	if !filepath.IsAbs(binary.Path) || binary.SHA256 != pinnedSHA {
		return binary, fmt.Errorf("%s requires an absolute executable and the reviewed SHA-256 %s", role, pinnedSHA)
	}
	info, err := os.Stat(binary.Path)
	if err != nil {
		return binary, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return binary, fmt.Errorf("%s is not an executable regular file", binary.Path)
	}
	file, err := os.Open(binary.Path)
	if err != nil {
		return binary, err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return binary, err
	}
	if hex.EncodeToString(hash.Sum(nil)) != pinnedSHA {
		return binary, fmt.Errorf("%s executable bytes differ from the reviewed SHA-256", role)
	}
	return binary, nil
}

func nativeUpgradeStop(t *testing.T, h *recoveryIntegrationHarness) {
	t.Helper()
	command := h.command
	if command != nil && command.Process == nil {
		h.command = nil
		t.Error("native command did not start; no process exists to join")
		return
	}
	h.stop()
	if command != nil && (command.ProcessState == nil || h.command != nil) {
		t.Fatal("owned native process was not joined")
	}
}

func nativeUpgradeWaitUnloaded(t *testing.T, ctx context.Context, client *Client, thread string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ids, err := client.LoadedThreadIDs(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(ids, thread) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("held fixture thread did not unload")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func nativeUpgradeMessages(transcript Transcript) []TranscriptEntry {
	var messages []TranscriptEntry
	for _, entry := range transcript.Entries {
		if entry.Kind == "userMessage" || entry.Kind == "agentMessage" {
			messages = append(messages, TranscriptEntry{Kind: entry.Kind, ItemID: entry.ItemID, TurnID: entry.TurnID, Text: entry.Text, ClientUserMessageID: entry.ClientUserMessageID})
		}
	}
	return messages
}

func nativeUpgradeRequireMessages(t *testing.T, transcript Transcript, expected []TranscriptEntry) {
	t.Helper()
	actual := nativeUpgradeMessages(transcript)
	if len(expected) == 0 || len(actual) < len(expected) || !reflect.DeepEqual(actual[:len(expected)], expected) {
		t.Fatalf("thread %s lost or changed retained message identities: got %+v want prefix %+v", transcript.ThreadID, actual, expected)
	}
}

func nativeUpgradeWaitTurn(t *testing.T, ctx context.Context, client *Client, thread string, receipt SendReceipt, user, answer string) Transcript {
	t.Helper()
	if receipt.TurnID == "" || receipt.ClientUserMessageID == "" || receipt.QueuedSubmissionID != "" {
		t.Fatalf("invalid completed-turn receipt: %+v", receipt)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		transcript, err := client.ReadThread(ctx, thread)
		if err != nil {
			t.Fatal(err)
		}
		userFound, answerFound := false, false
		for _, entry := range transcript.Entries {
			if entry.TurnID != receipt.TurnID || entry.TurnStatus != "completed" {
				continue
			}
			if entry.Kind == "userMessage" && entry.Text == user && entry.ClientUserMessageID == receipt.ClientUserMessageID {
				userFound = true
			}
			if entry.Kind == "agentMessage" && entry.Text == answer && entry.ItemID != "" {
				answerFound = true
			}
		}
		if userFound && answerFound {
			return transcript
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Fatalf("completed synthetic turn not retained: %+v, context=%v", transcript, ctx.Err())
		}
		time.Sleep(30 * time.Millisecond)
	}
}

func nativeUpgradeCheckState(t *testing.T, ctx context.Context, client *Client, cwd string, state nativeUpgradeState, upgraded bool) {
	t.Helper()
	for _, target := range []struct{ id, project, parent string }{{state.Root, state.RootProject, ""}, {state.Held, state.HeldProject, ""}, {state.Fork, "", state.Root}} {
		if target.id == "" {
			continue
		}
		metadata, err := client.ReadThreadMetadata(ctx, target.id, true)
		if err != nil {
			t.Fatal(err)
		}
		if metadata.ID != target.id || metadata.Cwd != cwd || metadata.ForkedFromID != target.parent || target.project != "" && (metadata.ProjectID == nil || *metadata.ProjectID != target.project) {
			t.Fatalf("retained native identity changed: %+v", metadata)
		}
		if target.project != "" {
			if _, err := client.ReadProject(ctx, target.project); err != nil {
				t.Fatal(err)
			}
		}
		transcript, err := client.ReadThread(ctx, target.id)
		if err != nil {
			t.Fatal(err)
		}
		if target.id == state.Root || target.id == state.Fork {
			expected := state.OldMessages
			if upgraded {
				expected = state.UpgradedMessages
			}
			nativeUpgradeRequireMessages(t, transcript, expected)
		}
	}
	queue, err := client.ListQueue(ctx, state.Held)
	if err != nil || !reflect.DeepEqual(queue, state.Queue) {
		t.Fatalf("held queue identity/order changed: %+v want %+v: %v", queue, state.Queue, err)
	}
	loaded, err := client.LoadedThreadIDs(ctx)
	if err != nil || slices.Contains(loaded, state.Held) {
		t.Fatalf("held queue thread was loaded: %v: %v", loaded, err)
	}
	for _, entry := range []struct {
		text    string
		receipt SendReceipt
	}{{state.OldUser, state.OldReceipt}, {state.NewUser, state.NewReceipt}} {
		if entry.receipt.TurnID == "" {
			continue
		}
		receipt, found, err := client.ReconcileSend(ctx, state.Root, entry.text, entry.receipt.ClientUserMessageID, "")
		if err != nil || !found || receipt != entry.receipt {
			t.Fatalf("exact send receipt changed: %+v want %+v, found=%v: %v", receipt, entry.receipt, found, err)
		}
	}
	if err := client.RequireSubmissionAttemptsKnown(ctx, state.Held); err != nil {
		t.Fatal(err)
	}
}

func nativeUpgradeQuiet(t *testing.T, ctx context.Context, client *Client, hits *atomic.Int64, expected int64, allCold bool) {
	t.Helper()
	for range 2 {
		ids, err := client.LoadedThreadIDs(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if allCold && len(ids) != 0 {
			t.Fatalf("cold inspection loaded native threads: %v", ids)
		}
		if hits.Load() != expected {
			t.Fatalf("unexpected provider inference: %d hits want %d", hits.Load(), expected)
		}
		time.Sleep(150 * time.Millisecond)
	}
	if hits.Load() != expected {
		t.Fatalf("unexpected provider inference during cold observation: %d hits want %d", hits.Load(), expected)
	}
}

func nativeUpgradeWriteJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err == nil {
		err = os.WriteFile(path, append(data, '\n'), 0600)
	}
	if err != nil {
		t.Errorf("retain fixture evidence %s: %v", path, err)
	}
}

// Snapshot only this stopped fixture's private trees, preserving symlinks as
// links rather than following them into an operator or package directory.
func nativeUpgradeCopyTree(source, destination string) error {
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		if entry.Type()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			if err != nil {
				return err
			}
			return os.Symlink(link, target)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("offline fixture snapshot has unsupported file %s", relative)
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		defer input.Close()
		output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm())
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(output, input)
		if copyErr == nil {
			copyErr = output.Sync()
		}
		closeErr := output.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
}

func nativeUpgradeInventory(root string) (map[string]string, error) {
	result := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			result[relative] = info.Mode().String()
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			result[relative] = "symlink:" + link
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unsupported offline state entry %s", relative)
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		hash := sha256.New()
		_, err = io.Copy(hash, file)
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		result[relative] = info.Mode().String() + ":" + hex.EncodeToString(hash.Sum(nil))
		return nil
	})
	return result, err
}

func nativeUpgradeFixtureInventory(home, receipts string) (map[string]string, error) {
	result := make(map[string]string)
	for label, root := range map[string]string{"home": home, "client-receipts": receipts} {
		files, err := nativeUpgradeInventory(root)
		if err != nil {
			return nil, err
		}
		for path, value := range files {
			result[filepath.Join(label, path)] = value
		}
	}
	return result, nil
}

func nativeUpgradeChanged(before, after map[string]string) []string {
	paths := make(map[string]bool)
	for path, value := range before {
		if after[path] != value {
			paths[path] = true
		}
	}
	for path, value := range after {
		if before[path] != value {
			paths[path] = true
		}
	}
	var changed []string
	for path := range paths {
		changed = append(changed, path)
	}
	slices.Sort(changed)
	return changed
}

func TestNativeStateUpgradeFixtureHelpers(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "synthetic-executable")
	bytes := []byte("not a native binary; this test never executes it\n")
	if err := os.WriteFile(binary, bytes, 0700); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(bytes)
	expected := hex.EncodeToString(hash[:])
	t.Setenv("CODEX_UPGRADE_HELPER_BINARY", binary)
	t.Setenv("CODEX_UPGRADE_HELPER_SHA256", expected)
	if _, err := nativeUpgradeBinaryFromEnv("HELPER", expected, "unused"); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeUpgradeBinaryFromEnv("HELPER", nativeUpgradeNewSHA, "unused"); err == nil {
		t.Fatal("wrong prerequisite hash accepted")
	}
	if err := os.WriteFile(binary, []byte("changed executable bytes"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeUpgradeBinaryFromEnv("HELPER", expected, "unused"); err == nil {
		t.Fatal("changed executable bytes accepted")
	}
	t.Setenv("CODEX_UPGRADE_HELPER_BINARY", "relative-binary")
	if _, err := nativeUpgradeBinaryFromEnv("HELPER", expected, "unused"); err == nil {
		t.Fatal("relative prerequisite accepted")
	}
	state := filepath.Join(root, "state")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"state.sqlite", "state.sqlite-wal", "state.sqlite-shm"} {
		if err := os.WriteFile(filepath.Join(state, name), []byte(name), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("state.sqlite", filepath.Join(state, "link")); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(root, "snapshot")
	if err := nativeUpgradeCopyTree(state, snapshot); err != nil {
		t.Fatal(err)
	}
	before, err := nativeUpgradeInventory(state)
	if err != nil {
		t.Fatal(err)
	}
	after, err := nativeUpgradeInventory(snapshot)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("offline snapshot lost files, sidecars, permissions or links: %v", err)
	}
	if err := os.WriteFile(filepath.Join(state, "state.sqlite-wal"), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	after, err = nativeUpgradeInventory(state)
	if err != nil || !slices.Equal(nativeUpgradeChanged(before, after), []string{"state.sqlite-wal"}) {
		t.Fatalf("old-reader state change not identified: %v", err)
	}
}

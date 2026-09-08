package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const protocolFixtureVersion = "0.152.1"

func TestEmptyPromptsAreAJSONList(t *testing.T) {
	client := New("/tmp/codex-not-connected.sock")
	prompts := client.Prompts("thread-1")
	if prompts == nil {
		t.Fatal("empty prompts are nil")
	}
	encoded, err := json.Marshal(prompts)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != "[]" {
		t.Fatalf("empty prompts JSON = %s", encoded)
	}
}

func TestHandshakeAndLargeThreadHistory(t *testing.T) {
	initialized := make(chan struct{}, 1)
	largeText := strings.Repeat("history", 8_000)
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "initialize" {
			return errors.New("first request was not initialize")
		}
		if err := writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"userAgent": "codex-cli/99.0.0"},
		}); err != nil {
			return err
		}
		notification, err := readObject(connection)
		if err != nil {
			return err
		}
		if notification["method"] != "initialized" {
			return errors.New("initialize was not followed by initialized")
		}
		initialized <- struct{}{}

		request, err = readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/read" {
			return errors.New("expected thread/read")
		}
		params, _ := request["params"].(map[string]any)
		if params["threadId"] != "thread-1" || params["excludeTurns"] != true {
			return fmt.Errorf("invalid thread/read params: %#v", params)
		}
		if err := writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"thread": map[string]any{"id": "thread-1", "status": "active"}},
		}); err != nil {
			return err
		}
		request, err = readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/turns/list" {
			return errors.New("expected thread/turns/list")
		}
		return writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"data": []any{
				map[string]any{"id": "new", "items": []any{map[string]any{"id": "large", "type": "agentMessage", "text": largeText}}},
				map[string]any{"id": "old", "items": []any{map[string]any{"id": "plan", "type": "plan", "text": "old"}}},
			}},
		})
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	payload, err := client.ReadThread(ctx, "thread-1")
	if err != nil {
		t.Fatal(err)
	}
	<-initialized
	if len(payload.Entries) != 2 || payload.Entries[1].Text != largeText {
		t.Fatalf("large history was not normalized: %#v", payload)
	}
	if payload.Entries[0].TurnID != "old" || payload.Entries[1].TurnID != "new" {
		t.Fatal("descending page was not restored to chronological order")
	}
}

func TestReadThreadTreatsFreshMissingSourceRolloutAsEmpty(t *testing.T) {
	rollout := filepath.Join(t.TempDir(), "not-created.jsonl")
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil || request["method"] != "thread/read" {
			return fmt.Errorf("expected thread/read: %v", err)
		}
		params, _ := request["params"].(map[string]any)
		if params["threadId"] != "thread-1" || params["excludeTurns"] != true {
			return fmt.Errorf("invalid thread/read params: %#v", params)
		}
		metadata := freshThreadMetadata("thread-1", "/workspace/work/example", rollout)
		metadata["model"] = "gpt-6-astra"
		metadata["reasoningEffort"] = "max"
		metadata["status"] = map[string]any{"type": "notLoaded"}
		if err := writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"thread": metadata},
		}); err != nil {
			return err
		}
		request, err = readObject(connection)
		if err != nil || request["method"] != "thread/turns/list" {
			return fmt.Errorf("expected thread/turns/list: %v", err)
		}
		return writeObject(connection, map[string]any{
			"id": request["id"], "error": map[string]any{
				"code":    -32600,
				"message": "invalid paginated history lineage for thread-1: missing source rollout",
			},
		})
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	transcript, err := client.ReadThread(ctx, "thread-1")
	if err != nil {
		t.Fatal(err)
	}
	if transcript.ThreadID != "thread-1" || transcript.Status != "notLoaded" ||
		transcript.Model != "gpt-6-astra" || transcript.ReasoningEffort != "max" {
		t.Fatalf("unexpected transcript metadata: %#v", transcript)
	}
	if transcript.Entries == nil || len(transcript.Entries) != 0 {
		t.Fatalf("empty transcript entries = %#v", transcript.Entries)
	}
}

func TestFreshMissingSourceRolloutDetectionFailsClosed(t *testing.T) {
	missingRollout := filepath.Join(t.TempDir(), "not-created.jsonl")
	matchingError := &rpcCallError{
		code:    -32600,
		message: "invalid paginated history lineage for thread-1: missing source rollout",
	}
	tests := map[string]func(map[string]any) error{
		"different RPC response": func(_ map[string]any) error {
			return &rpcCallError{code: -32600, message: "missing source rollout"}
		},
		"active thread": func(thread map[string]any) error {
			thread["status"] = map[string]any{"type": "active"}
			return matchingError
		},
		"terminal source": func(thread map[string]any) error {
			thread["source"] = "cli"
			return matchingError
		},
		"existing rollout": func(thread map[string]any) error {
			if err := os.WriteFile(missingRollout, []byte("materialized\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			return matchingError
		},
		"nonempty history": func(thread map[string]any) error {
			thread["turns"] = []any{map[string]any{"id": "turn-1"}}
			return matchingError
		},
		"missing history": func(thread map[string]any) error {
			delete(thread, "turns")
			return matchingError
		},
		"null history": func(thread map[string]any) error {
			thread["turns"] = nil
			return matchingError
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			if name == "existing rollout" {
				t.Cleanup(func() { _ = os.Remove(missingRollout) })
			}
			thread := freshThreadMetadata("thread-1", "/workspace/work/example", missingRollout)
			err := mutate(thread)
			if freshThreadMissingSourceRollout(thread, "thread-1", err) {
				t.Fatal("invalid thread metadata was accepted as a fresh missing rollout")
			}
		})
	}
}

func TestVerifyThreadBindsIDToCanonicalWorkingDirectory(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index := 0; index < 2; index++ {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			if request["method"] != "thread/read" {
				return errors.New("expected thread/read")
			}
			if err := writeObject(connection, map[string]any{
				"id": request["id"], "result": map[string]any{"thread": map[string]any{
					"id": "thread-1", "cwd": "/workspace/work/example",
				}},
			}); err != nil {
				return err
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.VerifyThread(ctx, "thread-1", "/workspace/work/example"); err != nil {
		t.Fatal(err)
	}
	if err := client.VerifyThread(ctx, "thread-1", "/workspace/work/other"); err == nil {
		t.Fatal("cross-mapped thread directory was accepted")
	}
}

func TestStartThreadRejectsWrongWorkingDirectory(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/start" {
			return errors.New("expected thread/start")
		}
		return writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"thread": map[string]any{
				"id": "thread-1", "cwd": "/workspace/work/other",
			}},
		})
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.StartThread(ctx, "/workspace/work/example", map[string]string{
		"VPSFREE_DEV_SESSION_WORKSPACE": "/workspace",
	})
	if err == nil || !strings.Contains(err.Error(), "working directory") {
		t.Fatalf("wrong-directory start result = %v", err)
	}
}

func TestResolveNewThreadSettingsDefaultsToXhigh(t *testing.T) {
	models := []Model{
		{
			Model: DefaultNewThreadModel, DisplayName: "GPT-6 Astra",
			DefaultReasoningEffort: "medium",
			SupportedReasoningEfforts: []ReasoningEffortOption{
				{ReasoningEffort: "medium"}, {ReasoningEffort: "xhigh"},
			},
		},
		{
			Model: "bounded", DisplayName: "Bounded", DefaultReasoningEffort: "high",
			SupportedReasoningEfforts: []ReasoningEffortOption{
				{ReasoningEffort: "medium"}, {ReasoningEffort: "high"},
			},
		},
	}

	settings, err := ResolveNewThreadSettings(models, ThreadSettings{})
	if err != nil || settings.Model != DefaultNewThreadModel || settings.ReasoningEffort != "xhigh" {
		t.Fatalf("default settings = %#v, %v", settings, err)
	}
	settings, err = ResolveNewThreadSettings(models, ThreadSettings{Model: "bounded"})
	if err != nil || settings.Model != "bounded" || settings.ReasoningEffort != "high" {
		t.Fatalf("bounded settings = %#v, %v", settings, err)
	}
	settings, err = ResolveNewThreadSettings(models, ThreadSettings{
		Model: DefaultNewThreadModel, ReasoningEffort: "medium",
	})
	if err != nil || settings.ReasoningEffort != "medium" {
		t.Fatalf("explicit settings = %#v, %v", settings, err)
	}
}

func TestResolveNewThreadSettingsRejectsInvalidCatalogAndSelections(t *testing.T) {
	defaultModel := Model{
		Model: DefaultNewThreadModel, DisplayName: "GPT-6 Astra",
		DefaultReasoningEffort:    "medium",
		SupportedReasoningEfforts: []ReasoningEffortOption{{ReasoningEffort: "medium"}},
	}
	for _, testCase := range []struct {
		name      string
		models    []Model
		requested ThreadSettings
		message   string
	}{
		{"no default", []Model{{Model: "other"}}, ThreadSettings{}, "required default Codex model"},
		{"duplicate default", []Model{defaultModel, defaultModel}, ThreadSettings{}, "more than one"},
		{"default lacks xhigh", []Model{defaultModel}, ThreadSettings{}, "required default reasoning effort"},
		{"missing model", []Model{defaultModel}, ThreadSettings{Model: "missing"}, "not available"},
		{"unsupported effort", []Model{defaultModel}, ThreadSettings{Model: DefaultNewThreadModel, ReasoningEffort: "max"}, "not available"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := ResolveNewThreadSettings(testCase.models, testCase.requested)
			if err == nil || !strings.Contains(err.Error(), testCase.message) {
				t.Fatalf("resolution error = %v", err)
			}
		})
	}
}

func TestModelsSettingsAndForkUseSupportedAppServerContracts(t *testing.T) {
	rollout := settingsRollout(t, "default")
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index := 0; index < 7; index++ {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			params, _ := request["params"].(map[string]any)
			switch index {
			case 0:
				if request["method"] != "model/list" {
					return fmt.Errorf("expected model/list, got %v", request["method"])
				}
				err = writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{
					"data": []any{map[string]any{
						"id": "model-id", "model": "gpt-test", "displayName": "GPT Test",
						"isDefault": true, "defaultReasoningEffort": "high",
						"supportedReasoningEfforts": []any{map[string]any{"reasoningEffort": "high"}},
					}}, "nextCursor": nil,
				}})
			case 1:
				if request["method"] != "thread/resume" || params["threadId"] != "thread-source" {
					return fmt.Errorf("expected settings subscription, got %#v", request)
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{
						"thread": map[string]any{"id": "thread-source"},
					},
				})
			case 2, 3:
				if request["method"] != "thread/read" || params["threadId"] != "thread-source" {
					return fmt.Errorf("expected settings read, got %#v", request)
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"thread": map[string]any{
						"id": "thread-source", "model": "old-model", "reasoningEffort": "medium", "path": rollout,
					}},
				})
			case 4:
				if request["method"] != "thread/settings/update" || params["model"] != "gpt-test" ||
					params["effort"] != "high" {
					return fmt.Errorf("invalid settings request: %#v", request)
				}
				if _, exists := params["collaborationMode"]; exists {
					return fmt.Errorf("model update overwrote collaboration mode: %#v", request)
				}
				if err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{},
				}); err == nil {
					err = writeObject(connection, map[string]any{
						"method": "thread/settings/updated", "params": map[string]any{
							"threadId": "thread-source", "threadSettings": map[string]any{
								"model": "gpt-test", "effort": "high",
								"collaborationMode": map[string]any{"mode": "default"},
							},
						},
					})
				}
			case 5:
				if request["method"] != "thread/turns/list" || params["threadId"] != "thread-source" {
					return fmt.Errorf("expected source idle check, got %#v", request)
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"data": []any{}},
				})
			case 6:
				if request["method"] != "thread/fork" || params["threadId"] != "thread-source" ||
					params["cwd"] != "/workspace/work/fork" || params["model"] != "gpt-test" {
					return fmt.Errorf("invalid fork request: %#v", request)
				}
				err = writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{
					"thread": map[string]any{
						"id": "thread-fork", "cwd": "/workspace/work/fork", "forkedFromId": "thread-source",
					},
				}})
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	models, err := client.ListModels(ctx)
	if err != nil || len(models) != 1 || models[0].Model != "gpt-test" {
		t.Fatalf("models = %#v, %v", models, err)
	}
	model, effort := "gpt-test", "high"
	update := ThreadSettingsUpdate{Model: &model, ReasoningEffort: &effort}
	if _, err := client.UpdateThreadSettings(ctx, "thread-source", update); err != nil {
		t.Fatal(err)
	}
	settings := ThreadSettings{Model: model, ReasoningEffort: effort}
	id, err := client.ForkThread(ctx, "thread-source", "/workspace/work/fork", map[string]string{
		"VPSFREE_DEV_SESSION_WORKSPACE": "/workspace",
	}, settings)
	if err != nil || id != "thread-fork" {
		t.Fatalf("fork = %q, %v", id, err)
	}
}

func TestCollaborationModesAndNotifications(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil || request["method"] != "collaborationMode/list" {
			return fmt.Errorf("expected collaborationMode/list: %v", err)
		}
		if err := writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"data": []any{
				map[string]any{"name": "Plan", "mode": "plan", "reasoning_effort": "medium"},
				map[string]any{"name": "Default", "mode": "default"},
				map[string]any{"name": "Review", "mode": "review"},
			}},
		}); err != nil {
			return err
		}
		if err := writeObject(connection, map[string]any{
			"method": "thread/settings/updated", "params": map[string]any{
				"threadId": "thread-1", "threadSettings": map[string]any{
					"model": "gpt-6-astra", "effort": "xhigh",
					"collaborationMode": map[string]any{"mode": "plan"},
				},
			},
		}); err != nil {
			return err
		}
		request, err = readObject(connection)
		if err != nil || request["method"] != "thread/read" {
			return fmt.Errorf("expected thread/read: %v", err)
		}
		if err := writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"thread": map[string]any{
				"id": "thread-1", "status": "idle", "model": "terminal-model", "reasoningEffort": "high",
			}},
		}); err != nil {
			return err
		}
		request, err = readObject(connection)
		if err != nil || request["method"] != "thread/turns/list" {
			return fmt.Errorf("expected thread/turns/list: %v", err)
		}
		return writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"data": []any{}},
		})
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	modes, err := client.ListCollaborationModes(ctx)
	if err != nil || len(modes) != 2 || modes[0].Mode != "plan" || modes[1].Mode != "default" {
		t.Fatalf("collaboration modes = %#v, %v", modes, err)
	}
	thread, err := client.ReadThread(ctx, "thread-1")
	if err != nil {
		t.Fatal(err)
	}
	if thread.Model != "terminal-model" || thread.ReasoningEffort != "high" ||
		thread.CollaborationMode != "plan" {
		t.Fatalf("cached thread settings = %#v", thread)
	}
}

func TestCollaborationModeFromRolloutUsesLatestPersistedSettings(t *testing.T) {
	path := settingsRollout(t, "default")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	entry := map[string]any{
		"type": "event_msg", "payload": map[string]any{
			"type": "thread_settings_applied",
			"thread_settings": map[string]any{
				"collaboration_mode": map[string]any{"mode": "plan"},
			},
		},
	}
	data, err := json.Marshal(entry)
	if err == nil {
		_, err = file.Write(append(data, '\n'))
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	mode, err := collaborationModeFromRollout(path)
	if err != nil || mode != "plan" {
		t.Fatalf("latest rollout mode = %q, %v", mode, err)
	}
}

func TestCollaborationModeUpdatePreservesModelAndReasoningEffort(t *testing.T) {
	rollout := settingsRollout(t, "default")
	updateAcknowledged := make(chan struct{})
	sendInterleavedNotification := make(chan struct{})
	interleavedNotificationObserved := make(chan struct{})
	sendMatchingNotification := make(chan struct{})
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		if err := writeObject(connection, map[string]any{
			"method": "thread/settings/updated", "params": map[string]any{
				"threadId": "thread-1", "threadSettings": map[string]any{
					"model": "stale-model", "effort": "medium",
					"collaborationMode": map[string]any{"mode": "default"},
				},
			},
		}); err != nil {
			return err
		}
		for index := 0; index < 4; index++ {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			params, _ := request["params"].(map[string]any)
			if index == 0 {
				if request["method"] != "thread/resume" || params["threadId"] != "thread-1" {
					return fmt.Errorf("expected settings subscription: %#v", request)
				}
				if err := writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{
						"thread": map[string]any{"id": "thread-1"},
					},
				}); err != nil {
					return err
				}
				continue
			}
			if index == 1 || index == 2 {
				if request["method"] != "thread/read" || params["threadId"] != "thread-1" {
					return fmt.Errorf("expected settings read: %#v", request)
				}
				if err := writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"thread": map[string]any{
						"id": "thread-1", "model": "gpt-6-astra", "reasoningEffort": "xhigh", "path": rollout,
					}},
				}); err != nil {
					return err
				}
				continue
			}
			mode, _ := params["collaborationMode"].(map[string]any)
			settings, _ := mode["settings"].(map[string]any)
			if request["method"] != "thread/settings/update" || mode["mode"] != "plan" ||
				settings["model"] != "gpt-6-astra" || settings["reasoning_effort"] != "xhigh" {
				return fmt.Errorf("mode update changed current settings: %#v", request)
			}
			if _, exists := params["model"]; exists {
				return fmt.Errorf("mode update redundantly overwrote model: %#v", request)
			}
			if _, exists := params["effort"]; exists {
				return fmt.Errorf("mode update redundantly overwrote effort: %#v", request)
			}
			if err := writeObject(connection, map[string]any{
				"id": request["id"], "result": map[string]any{},
			}); err != nil {
				return err
			}
			close(updateAcknowledged)
			<-sendInterleavedNotification
			if err := writeObject(connection, map[string]any{
				"method": "thread/settings/updated", "params": map[string]any{
					"threadId": "thread-1", "threadSettings": map[string]any{
						"model": "other-model", "effort": "high",
						"collaborationMode": map[string]any{"mode": "plan"},
					},
				},
			}); err != nil {
				return err
			}
			close(interleavedNotificationObserved)
			<-sendMatchingNotification
			return writeObject(connection, map[string]any{
				"method": "thread/settings/updated", "params": map[string]any{
					"threadId": "thread-1", "threadSettings": map[string]any{
						"model": "gpt-6-astra", "effort": "xhigh",
						"collaborationMode": map[string]any{"mode": "plan"},
					},
				},
			})
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	defer closeTestChannel(sendInterleavedNotification)
	defer closeTestChannel(sendMatchingNotification)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mode := "plan"
	type result struct {
		settings ThreadSettings
		err      error
	}
	resultChannel := make(chan result, 1)
	go func() {
		settings, err := client.UpdateThreadSettings(
			ctx, "thread-1", ThreadSettingsUpdate{CollaborationMode: &mode},
		)
		resultChannel <- result{settings: settings, err: err}
	}()
	select {
	case <-updateAcknowledged:
	case <-ctx.Done():
		t.Fatal("settings update was not acknowledged")
	}
	select {
	case result := <-resultChannel:
		t.Fatalf("settings update returned before notification: %#v, %v", result.settings, result.err)
	case <-time.After(50 * time.Millisecond):
	}
	close(sendInterleavedNotification)
	select {
	case <-interleavedNotificationObserved:
	case <-ctx.Done():
		t.Fatal("interleaved settings notification was not delivered")
	}
	select {
	case result := <-resultChannel:
		t.Fatalf("settings update accepted an interleaved snapshot: %#v, %v", result.settings, result.err)
	case <-time.After(50 * time.Millisecond):
	}
	close(sendMatchingNotification)
	var outcome result
	select {
	case outcome = <-resultChannel:
	case <-ctx.Done():
		t.Fatal("settings update did not return after notification")
	}
	settings, err := outcome.settings, outcome.err
	if err != nil || settings.Model != "gpt-6-astra" ||
		settings.ReasoningEffort != "xhigh" || settings.CollaborationMode != "plan" {
		t.Fatalf("updated settings = %#v, %v", settings, err)
	}
}

func TestCollaborationModeUpdateUsesAStableSettingsSnapshot(t *testing.T) {
	rollout := settingsRollout(t, "default")
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index := 0; index < 6; index++ {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			params, _ := request["params"].(map[string]any)
			if index == 0 {
				if request["method"] != "thread/resume" || params["threadId"] != "thread-1" {
					return fmt.Errorf("expected settings subscription: %#v", request)
				}
				if err := writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{},
				}); err != nil {
					return err
				}
				continue
			}
			if index < 5 {
				if request["method"] != "thread/read" || params["threadId"] != "thread-1" {
					return fmt.Errorf("expected stable settings read: %#v", request)
				}
				model, effort := "gpt-new", "xhigh"
				if index == 1 {
					model, effort = "gpt-old", "medium"
				}
				if err := writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"thread": map[string]any{
						"id": "thread-1", "model": model, "reasoningEffort": effort, "path": rollout,
					}},
				}); err != nil {
					return err
				}
				continue
			}
			mode, _ := params["collaborationMode"].(map[string]any)
			settings, _ := mode["settings"].(map[string]any)
			if request["method"] != "thread/settings/update" || mode["mode"] != "plan" ||
				settings["model"] != "gpt-new" || settings["reasoning_effort"] != "xhigh" {
				return fmt.Errorf("mode update used a torn settings snapshot: %#v", request)
			}
			if err := writeObject(connection, map[string]any{
				"id": request["id"], "result": map[string]any{},
			}); err != nil {
				return err
			}
			return writeObject(connection, map[string]any{
				"method": "thread/settings/updated", "params": map[string]any{
					"threadId": "thread-1", "threadSettings": map[string]any{
						"model": "gpt-new", "effort": "xhigh",
						"collaborationMode": map[string]any{"mode": "plan"},
					},
				},
			})
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mode := "plan"
	settings, err := client.UpdateThreadSettings(
		ctx, "thread-1", ThreadSettingsUpdate{CollaborationMode: &mode},
	)
	if err != nil || settings.Model != "gpt-new" ||
		settings.ReasoningEffort != "xhigh" || settings.CollaborationMode != "plan" {
		t.Fatalf("updated stable settings = %#v, %v", settings, err)
	}
}

func TestSettingsUpdateRejectsClearingReasoningEffort(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "missing.sock")
	client := New(socket)
	defer client.Close()
	ctx := context.Background()
	model, automatic := "gpt-new", ""
	_, err := client.UpdateThreadSettings(
		ctx,
		"thread-1",
		ThreadSettingsUpdate{Model: &model, ReasoningEffort: &automatic},
	)
	if err == nil || !strings.Contains(err.Error(), "cannot clear reasoning effort") {
		t.Fatalf("automatic settings error = %v", err)
	}
}

func TestNoOpCollaborationModeUpdateReturnsWithoutWaitingForANotification(t *testing.T) {
	rollout := settingsRollout(t, "default")
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index, method := range []string{"thread/resume", "thread/read", "thread/read"} {
			request, err := readObject(connection)
			if err != nil || request["method"] != method {
				return fmt.Errorf("request %d = %#v, %v", index, request, err)
			}
			result := map[string]any{}
			if method == "thread/read" {
				result["thread"] = map[string]any{
					"id": "thread-1", "model": "gpt-6-astra", "reasoningEffort": "xhigh", "path": rollout,
				}
			}
			if err := writeObject(connection, map[string]any{
				"id": request["id"], "result": result,
			}); err != nil {
				return err
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mode := "default"
	settings, err := client.UpdateThreadSettings(
		ctx,
		"thread-1",
		ThreadSettingsUpdate{CollaborationMode: &mode},
	)
	if err != nil || settings.CollaborationMode != mode {
		t.Fatalf("no-op settings = %#v, %v", settings, err)
	}
}

func TestQueueUsesFIFOAppServerContracts(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index := 0; index < 10; index++ {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			params, _ := request["params"].(map[string]any)
			switch index {
			case 0, 1:
				if request["method"] != "thread/queue/list" || params["threadId"] != "thread-1" {
					return fmt.Errorf("invalid queue list: %#v", request)
				}
				if index == 1 && params["cursor"] != "next" {
					return fmt.Errorf("invalid queue cursor: %#v", params)
				}
				result := map[string]any{
					"data": []any{map[string]any{
						"id": "queued-1", "clientUserMessageId": "client-1",
						"input": []any{map[string]any{"type": "text", "text": "first"}},
					}},
					"nextCursor": "next",
				}
				if index == 1 {
					result = map[string]any{
						"data": []any{map[string]any{
							"id": "queued-2", "clientUserMessageId": "client-2",
							"input": []any{map[string]any{"type": "text", "text": "second"}},
						}},
						"nextCursor": nil,
					}
				}
				err = writeObject(connection, map[string]any{"id": request["id"], "result": result})
			case 2:
				if request["method"] != "thread/queue/add" || params["threadId"] != "thread-1" {
					return fmt.Errorf("invalid queue add: %#v", request)
				}
				clientID, _ := params["clientUserMessageId"].(string)
				if clientID == "" {
					return errors.New("queue add omitted client message id")
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "error": map[string]any{
						"code": -32603, "message": "response was lost after queue persistence",
					},
				})
			case 3:
				if request["method"] != "thread/queue/list" || params["threadId"] != "thread-1" {
					return fmt.Errorf("invalid queue reconciliation: %#v", request)
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"data": []any{map[string]any{
						"id": "queued-3", "clientUserMessageId": "queue-attempt-1",
						"input": []any{map[string]any{"type": "text", "text": "third"}},
					}}, "nextCursor": nil},
				})
			case 4:
				if request["method"] != "thread/queue/list" || params["threadId"] != "thread-1" {
					return fmt.Errorf("invalid queue delete lookup: %#v", request)
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"data": []any{map[string]any{
						"id": "queued-1", "clientUserMessageId": "client-1",
						"input": []any{map[string]any{"type": "text", "text": "first"}},
					}}, "nextCursor": nil},
				})
			case 5:
				if request["method"] != "thread/queue/delete" || params["queuedSubmissionId"] != "queued-1" {
					return fmt.Errorf("invalid queue delete: %#v", request)
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"deleted": true},
				})
			case 6:
				if request["method"] != "thread/queue/list" || params["threadId"] != "thread-1" {
					return fmt.Errorf("invalid start target lookup: %#v", request)
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"data": []any{map[string]any{
						"id": "queued-2", "clientUserMessageId": "client-2",
						"input": []any{map[string]any{"type": "text", "text": "second"}},
					}}, "nextCursor": nil},
				})
			case 7:
				if request["method"] != "thread/resume" || params["threadId"] != "thread-1" {
					return fmt.Errorf("invalid queue resume: %#v", request)
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"thread": map[string]any{"id": "thread-1"}},
				})
			case 8:
				if request["method"] != "thread/items/list" || params["threadId"] != "thread-1" {
					return fmt.Errorf("invalid queue start reconciliation: %#v", request)
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"data": []any{}, "nextCursor": nil},
				})
			case 9:
				if request["method"] != "thread/queue/start" || params["threadId"] != "thread-1" ||
					params["queuedSubmissionId"] != "queued-2" {
					return fmt.Errorf("invalid queue start: %#v", request)
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"turn": map[string]any{
						"id": "turn-1", "status": "inProgress",
					}},
				})
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entries, err := client.ListQueue(ctx, "thread-1")
	if err != nil || len(entries) != 2 || entries[0].Text != "first" || entries[1].Text != "second" {
		t.Fatalf("queue = %#v, %v", entries, err)
	}
	queued, err := client.Queue(ctx, "thread-1", "third", "queue-attempt-1")
	if err != nil || queued.ID != "queued-3" || queued.Text != "third" {
		t.Fatalf("queued = %#v, %v", queued, err)
	}
	if err := client.DeleteQueueEntry(ctx, "thread-1", "queued-1"); err != nil {
		t.Fatal(err)
	}
	if err := client.StartQueue(ctx, "thread-1", "queued-2"); err != nil {
		t.Fatal(err)
	}
}

func TestSendReturnsAClientCorrelatedReceipt(t *testing.T) {
	tests := []struct {
		name       string
		activeTurn string
		method     string
		steered    bool
	}{
		{name: "idle thread", method: "turn/start"},
		{name: "active turn", activeTurn: "turn-active", method: "turn/steer", steered: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				for index, expected := range []string{
					"thread/items/list", "thread/resume", "thread/turns/list", test.method,
					"thread/items/list",
				} {
					request, err := readObject(connection)
					if err != nil {
						return err
					}
					if request["method"] != expected {
						return fmt.Errorf("request %d = %#v, want %s", index, request, expected)
					}
					params := request["params"].(map[string]any)
					result := map[string]any{}
					switch index {
					case 0:
						result = map[string]any{"data": []any{}, "nextCursor": nil}
					case 2:
						turns := []any{}
						if test.activeTurn != "" {
							turns = append(turns, map[string]any{
								"id": test.activeTurn, "status": "inProgress",
							})
						}
						result = map[string]any{"data": turns}
					case 3:
						if params["threadId"] != "thread-1" ||
							params["clientUserMessageId"] != "client-message-1" {
							return fmt.Errorf("uncorrelated message request: %#v", request)
						}
						if test.steered {
							if params["expectedTurnId"] != test.activeTurn {
								return fmt.Errorf("wrong steer turn: %#v", request)
							}
							result = map[string]any{"turnId": test.activeTurn}
						} else {
							result = map[string]any{"turn": map[string]any{"id": "turn-new"}}
						}
					case 4:
						turn := "turn-new"
						if test.steered {
							turn = test.activeTurn
						}
						result = map[string]any{"data": []any{map[string]any{
							"turnId": turn, "item": map[string]any{
								"id": "message-1", "type": "userMessage",
								"clientId": "client-message-1",
								"content":  []any{map[string]any{"type": "text", "text": "message"}},
							},
						}}, "nextCursor": nil}
					}
					if err := writeObject(connection, map[string]any{
						"id": request["id"], "result": result,
					}); err != nil {
						return err
					}
				}
				return nil
			})
			client := New(socket)
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			receipt, err := client.Send(ctx, "thread-1", "message", "client-message-1", "")
			if err != nil {
				t.Fatal(err)
			}
			wantTurn := "turn-new"
			if test.steered {
				wantTurn = test.activeTurn
			}
			if receipt.TurnID != wantTurn || receipt.ClientUserMessageID != "client-message-1" ||
				receipt.Steered != test.steered {
				t.Fatalf("send receipt = %#v", receipt)
			}
		})
	}
}

func TestSendRetryReconcilesARecordedAttemptWithoutSubmittingAgain(t *testing.T) {
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
			"id": request["id"], "result": map[string]any{
				"data": []any{map[string]any{
					"turnId": "turn-accepted", "item": map[string]any{
						"id": "message-accepted", "type": "userMessage",
						"clientId": "client-message-retry",
						"content":  []any{map[string]any{"type": "text", "text": "message"}},
					},
				}},
				"nextCursor": nil,
			},
		})
	})
	client := New(socket)
	defer client.Close()
	if err := client.recordSendAttempt(
		"thread-1", "client-message-retry", "message", "", true,
	); err != nil {
		t.Fatal(err)
	}
	if err := client.markSendSubmitting("thread-1", "client-message-retry"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	receipt, err := client.Send(ctx, "thread-1", "message", "client-message-retry", "")
	if err != nil || receipt.TurnID != "turn-accepted" ||
		receipt.ClientUserMessageID != "client-message-retry" || !receipt.Steered {
		t.Fatalf("reconciled send = %#v, %v", receipt, err)
	}
}

func TestSendRetryFailsClosedWhileRecordedAttemptIsAbsent(t *testing.T) {
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
			"id": request["id"], "result": map[string]any{
				"data": []any{}, "nextCursor": nil,
			},
		})
	})
	client := New(socket)
	defer client.Close()
	if err := client.recordSendAttempt(
		"thread-1", "client-message-unknown", "message", "", false,
	); err != nil {
		t.Fatal(err)
	}
	if err := client.markSendSubmitting("thread-1", "client-message-unknown"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.Send(ctx, "thread-1", "message", "client-message-unknown", "")
	var unknown *UnknownSendOutcomeError
	if !errors.As(err, &unknown) {
		t.Fatalf("unknown send outcome = %v", err)
	}
}

func TestPreparedSendSurvivesRestartAndIsPrunedAfterAcceptance(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index, expected := range []string{
			"thread/resume", "thread/turns/list", "turn/start", "thread/items/list",
		} {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			if request["method"] != expected {
				return fmt.Errorf("request %d = %#v, want %s", index, request, expected)
			}
			result := map[string]any{}
			if index == 1 {
				result = map[string]any{"data": []any{}}
			} else if index == 2 {
				result = map[string]any{"turn": map[string]any{"id": "turn-plan"}}
			} else if index == 3 {
				result = map[string]any{"data": []any{map[string]any{
					"turnId": "turn-plan", "item": map[string]any{
						"id": "message-plan", "type": "userMessage", "clientId": "client-plan",
						"content": []any{map[string]any{"type": "text", "text": "Implement the plan."}},
					},
				}}, "nextCursor": nil}
			}
			if err := writeObject(connection, map[string]any{"id": request["id"], "result": result}); err != nil {
				return err
			}
		}
		return nil
	})
	first := New(socket)
	if err := first.PrepareSend(
		"thread-1", "Implement the plan.", "client-plan", "plan:digest", false,
	); err != nil {
		t.Fatal(err)
	}
	first.Close()
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Send(
		ctx, "thread-1", "Implement the plan.", "client-plan", "",
	); err == nil || !strings.Contains(err.Error(), "another action") {
		t.Fatalf("generic replay of plan send error = %v", err)
	}
	if _, err := client.Send(
		ctx, "thread-1", "Implement the plan.", "client-plan", "plan:other",
	); err == nil || !strings.Contains(err.Error(), "another action") {
		t.Fatalf("different-plan replay error = %v", err)
	}
	receipt, err := client.Send(
		ctx, "thread-1", "Implement the plan.", "client-plan", "plan:digest",
	)
	if err != nil || receipt.TurnID != "turn-plan" || receipt.Steered {
		t.Fatalf("prepared send = %#v, %v", receipt, err)
	}
	if _, found, err := client.sendAttempt(
		"thread-1", "client-plan", "Implement the plan.", "plan:digest",
	); err != nil || found {
		t.Fatalf("accepted attempt retained = %t, %v", found, err)
	}
}

func TestAcceptedSendIsPrunedAndAStaleRetryUsesHistory(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index, expected := range []string{
			"thread/items/list", "thread/resume", "thread/turns/list", "turn/start",
			"thread/items/list", "thread/items/list",
		} {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			if request["method"] != expected {
				return fmt.Errorf("request %d = %#v, want %s", index, request, expected)
			}
			result := map[string]any{}
			switch index {
			case 0:
				result = map[string]any{"data": []any{}, "nextCursor": nil}
			case 2:
				result = map[string]any{"data": []any{}}
			case 3:
				result = map[string]any{"turn": map[string]any{"id": "turn-1"}}
			case 4, 5:
				result = map[string]any{"data": []any{map[string]any{
					"turnId": "turn-1", "item": map[string]any{
						"id": "message-1", "type": "userMessage", "clientId": "client-1",
						"content": []any{map[string]any{"type": "text", "text": "message"}},
					},
				}}, "nextCursor": nil}
			}
			if err := writeObject(connection, map[string]any{"id": request["id"], "result": result}); err != nil {
				return err
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := client.Send(ctx, "thread-1", "message", "client-1", "")
	if err != nil || first.TurnID != "turn-1" {
		t.Fatalf("first send = %#v, %v", first, err)
	}
	if _, found, err := client.sendAttempt("thread-1", "client-1", "message", ""); err != nil || found {
		t.Fatalf("accepted send attempt retained = %t, %v", found, err)
	}
	retry, err := client.Send(ctx, "thread-1", "message", "client-1", "")
	if err != nil || retry.TurnID != "turn-1" || retry.Steered {
		t.Fatalf("stale retry = %#v, %v", retry, err)
	}
}

func TestTranscriptEntriesExposeClientMessageIdentityAndPlanCompletion(t *testing.T) {
	entries := transcriptEntries(map[string]any{
		"id": "turn-1", "status": "completed", "items": []any{
			map[string]any{
				"id": "item-user", "type": "userMessage",
				"clientId": "client-1",
				"content":  []any{map[string]any{"type": "text", "text": "hello"}},
			},
			map[string]any{"id": "item-plan", "type": "plan", "text": "the plan"},
		},
	})
	if len(entries) != 2 || entries[0].ClientUserMessageID != "client-1" ||
		entries[0].ItemID != "item-user" || entries[1].TurnStatus != "completed" ||
		entries[1].ItemID != "item-plan" {
		t.Fatalf("transcript entries = %#v", entries)
	}
}

func TestTranscriptEntriesOmitEmptyReasoningSummaries(t *testing.T) {
	entries := transcriptEntries(map[string]any{
		"id": "turn-1", "status": "completed", "items": []any{
			map[string]any{"id": "empty", "type": "reasoning", "summary": []any{}},
			map[string]any{"id": "blank", "type": "reasoning", "summary": []any{" \n"}},
			map[string]any{"id": "visible", "type": "reasoning", "summary": []any{"First", "Second"}},
		},
	})
	if len(entries) != 1 || entries[0].ItemID != "visible" ||
		entries[0].Summary != "Reasoning summary" || entries[0].Text != "First\nSecond" {
		t.Fatalf("reasoning transcript entries = %#v", entries)
	}
}

func TestListThreadActivityPaginatesPortalThreads(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index := 0; index < 2; index++ {
			request, err := readObject(connection)
			if err != nil || request["method"] != "thread/list" {
				return fmt.Errorf("expected thread/list: %#v, %v", request, err)
			}
			params, _ := request["params"].(map[string]any)
			if params["archived"] != false || (index == 1 && params["cursor"] != "next") {
				return fmt.Errorf("activity params = %#v", params)
			}
			result := map[string]any{"data": []any{map[string]any{
				"id": fmt.Sprintf("thread-%d", index+1), "cwd": fmt.Sprintf("/work/%d", index+1),
				"source":    "vscode",
				"updatedAt": int64(100 + index),
			}}}
			if index == 0 {
				result["nextCursor"] = "next"
			} else {
				result["nextCursor"] = nil
			}
			if err := writeObject(connection, map[string]any{"id": request["id"], "result": result}); err != nil {
				return err
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	activities, err := client.ListThreadActivity(ctx)
	if err != nil || len(activities) != 2 || activities[1].ID != "thread-2" ||
		activities[1].UpdatedAt.Unix() != 101 {
		t.Fatalf("activities = %#v, %v", activities, err)
	}
}

func TestStartQueueRejectsANonHeadSubmission(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil || request["method"] != "thread/queue/list" {
			return fmt.Errorf("expected queue head lookup: %#v, %v", request, err)
		}
		return writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"data": []any{
				map[string]any{
					"id": "queued-1", "clientUserMessageId": "client-1",
					"input": []any{map[string]any{"type": "text", "text": "first"}},
				},
				map[string]any{
					"id": "queued-2", "clientUserMessageId": "client-2",
					"input": []any{map[string]any{"type": "text", "text": "second"}},
				},
			}, "nextCursor": nil},
		})
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.StartQueue(ctx, "thread-1", "queued-2"); err == nil ||
		!strings.Contains(err.Error(), "only the first queued message") {
		t.Fatalf("non-head queue start error = %v", err)
	}
}

func TestQueueRetryFindsAnAlreadyStartedMessage(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index := 0; index < 5; index++ {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			params, _ := request["params"].(map[string]any)
			var result map[string]any
			switch index {
			case 1, 3:
				if request["method"] != "thread/queue/list" {
					return fmt.Errorf("expected queue reconciliation: %#v", request)
				}
				result = map[string]any{"data": []any{}, "nextCursor": nil}
			case 2:
				if request["method"] != "thread/items/list" {
					return fmt.Errorf("expected history reconciliation: %#v", request)
				}
				result = map[string]any{"data": []any{}, "nextCursor": nil}
			case 0:
				if request["method"] != "thread/queue/add" ||
					params["clientUserMessageId"] != "client-retry" {
					return fmt.Errorf("invalid queue submission: %#v", request)
				}
				result = map[string]any{"queuedSubmission": map[string]any{}}
			case 4:
				if request["method"] != "thread/items/list" {
					return fmt.Errorf("expected retry history lookup: %#v", request)
				}
				result = map[string]any{"data": []any{map[string]any{
					"turnId": "turn-1", "item": map[string]any{
						"id": "message-1", "type": "userMessage", "clientId": "client-retry",
						"content": []any{map[string]any{"type": "text", "text": "queued text"}},
					},
				}}, "nextCursor": nil}
			}
			if err := writeObject(connection, map[string]any{"id": request["id"], "result": result}); err != nil {
				return err
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Queue(ctx, "thread-1", "queued text", "client-retry"); err == nil ||
		!strings.Contains(err.Error(), "unknown outcome") {
		t.Fatalf("first queue error = %v", err)
	}
	entry, err := client.Queue(ctx, "thread-1", "queued text", "client-retry")
	if err != nil || entry.ID != "message-1" || entry.ClientUserMessageID != "client-retry" {
		t.Fatalf("reconciled queue entry = %#v, %v", entry, err)
	}
}

func TestQueueRetryFailsClosedWhileOutcomeIsUnknown(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index := 0; index < 5; index++ {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			if index == 0 {
				if request["method"] != "thread/queue/add" {
					return fmt.Errorf("expected queue submission: %#v", request)
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"queuedSubmission": map[string]any{}},
				})
			} else {
				expected := "thread/queue/list"
				if index == 2 || index == 4 {
					expected = "thread/items/list"
				}
				if request["method"] != expected {
					return fmt.Errorf("expected %s: %#v", expected, request)
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"data": []any{}, "nextCursor": nil},
				})
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Queue(ctx, "thread-1", "queued text", "client-unknown"); err == nil {
		t.Fatal("ambiguous queue submission succeeded")
	}
	if _, err := client.Queue(ctx, "thread-1", "queued text", "client-unknown"); err == nil ||
		!strings.Contains(err.Error(), "outcome is still unknown") {
		t.Fatalf("retry error = %v", err)
	}
}

func TestQueueAttemptsSurviveClientRestart(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "app-server.sock")
	first := New(socket)
	if err := first.recordQueueAttempt("thread-1", "client-1", "queued text"); err != nil {
		t.Fatal(err)
	}
	if err := first.PrepareSend(
		"thread-1", "sent text", "message-1", "plan:digest", false,
	); err != nil {
		t.Fatal(err)
	}
	if err := first.recordRetirementAttempt("/workspace/work/example", "thread-1"); err != nil {
		t.Fatal(err)
	}
	first.Close()

	second := New(socket)
	defer second.Close()
	attempted, err := second.queueAttempt("thread-1", "client-1", "queued text")
	if err != nil || !attempted {
		t.Fatalf("restored queue attempt = %v, %v", attempted, err)
	}
	if _, err := second.queueAttempt("thread-1", "client-1", "different text"); err == nil {
		t.Fatal("restored queue attempt accepted different text")
	}
	messageAttempted, err := second.SendAttempted(
		context.Background(), "thread-1", "sent text", "message-1", "plan:digest",
	)
	if err != nil || !messageAttempted {
		t.Fatalf("restored send attempt = %v, %v", messageAttempted, err)
	}
	retiringThread, err := second.retirementAttempt("/workspace/work/example")
	if err != nil || retiringThread != "thread-1" {
		t.Fatalf("restored retirement attempt = %q, %v", retiringThread, err)
	}
	info, err := os.Stat(socket + ".submission-attempts-v2.json")
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("queue ledger mode = %v, %v", info, err)
	}
}

func TestQueueDeletionResponseLossReconcilesAfterClientRestart(t *testing.T) {
	directory := t.TempDir()
	socket := filepath.Join(directory, "app-server.sock")
	first := New(socket)
	if err := first.recordQueueAttempt("thread-1", "client-1", "queued text"); err != nil {
		t.Fatal(err)
	}
	if _, err := first.recordQueueDeletionAttempt("thread-1", QueueEntry{
		ID: "queued-1", Text: "queued text", ClientUserMessageID: "client-1",
	}); err != nil {
		t.Fatal(err)
	}
	first.Close()

	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, acceptErr := websocket.Accept(w, r, nil)
		if acceptErr != nil {
			t.Errorf("accept websocket: %v", acceptErr)
			return
		}
		go func() {
			defer connection.Close(websocket.StatusNormalClosure, "")
			if handshakeErr := handshake(connection); handshakeErr != nil {
				t.Errorf("handshake: %v", handshakeErr)
				return
			}
			for _, method := range []string{"thread/queue/list", "thread/items/list"} {
				request, readErr := readObject(connection)
				if readErr != nil {
					t.Errorf("read %s: %v", method, readErr)
					return
				}
				if request["method"] != method {
					t.Errorf("request = %#v, want %s", request, method)
					return
				}
				if writeErr := writeObject(connection, map[string]any{
					"id":     request["id"],
					"result": map[string]any{"data": []any{}, "nextCursor": nil},
				}); writeErr != nil {
					t.Errorf("write %s: %v", method, writeErr)
					return
				}
			}
		}()
	})}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })

	second := New(socket)
	defer second.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := second.DeleteQueueEntry(ctx, "thread-1", "queued-1"); err != nil {
		t.Fatal(err)
	}
	if attempted, err := second.queueAttempt(
		"thread-1", "client-1", "queued text",
	); err != nil || attempted {
		t.Fatalf("queue attempt after deletion reconciliation = %v, %v", attempted, err)
	}
	if _, attempted, err := second.queueDeletionAttempt(
		"thread-1", "queued-1",
	); err != nil || attempted {
		t.Fatalf("deletion attempt after reconciliation = %v, %v", attempted, err)
	}
}

func TestRecoverForkThreadResumesMatchingPersistedFork(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index := 0; index < 3; index++ {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			params, _ := request["params"].(map[string]any)
			switch index {
			case 0:
				if request["method"] != "thread/list" || params["cwd"] != "/workspace/work/fork" {
					return fmt.Errorf("invalid recovery lookup: %#v", request)
				}
				err = writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{
					"data": []any{map[string]any{
						"id": "thread-fork", "cwd": "/workspace/work/fork", "forkedFromId": "thread-source",
					}},
				}})
			case 1:
				if request["method"] != "thread/turns/list" || params["threadId"] != "thread-fork" {
					return fmt.Errorf("invalid fork idle check: %#v", request)
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"data": []any{}},
				})
			case 2:
				if request["method"] != "thread/resume" || params["threadId"] != "thread-fork" ||
					params["cwd"] != "/workspace/work/fork" {
					return fmt.Errorf("invalid recovered fork resume: %#v", request)
				}
				err = writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{
					"thread": map[string]any{"id": "thread-fork", "cwd": "/workspace/work/fork"},
				}})
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, err := client.RecoverForkThread(
		ctx, "thread-source", "/workspace/work/fork",
		map[string]string{"VPSFREE_DEV_SESSION_WORKSPACE": "/workspace"}, ThreadSettings{},
	)
	if err != nil || id != "thread-fork" {
		t.Fatalf("recovered fork = %q, %v", id, err)
	}
}

func TestRecoverArchivedThreadUnarchivesAndResumesExactIdentity(t *testing.T) {
	threadID := "thread-archived"
	cwd := "/workspace/work/revived"
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index, expected := range []string{"thread/list", "thread/list", "thread/unarchive", "thread/resume"} {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			if request["method"] != expected {
				return fmt.Errorf("request %d = %#v, want %s", index, request, expected)
			}
			params := request["params"].(map[string]any)
			switch index {
			case 0, 1:
				if params["cwd"] != cwd || params["archived"] != (index == 1) {
					return fmt.Errorf("thread lookup %d = %#v", index, params)
				}
				data := []any{}
				if index == 1 {
					data = append(data,
						map[string]any{"id": "older-thread", "cwd": cwd, "source": "vscode"},
						map[string]any{"id": threadID, "cwd": cwd, "source": "vscode"},
					)
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"data": data},
				})
			case 2:
				if params["threadId"] != threadID {
					return fmt.Errorf("unarchive params = %#v", params)
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"thread": map[string]any{
						"id": threadID, "cwd": cwd, "source": "vscode",
					}},
				})
			case 3:
				if params["threadId"] != threadID || params["cwd"] != cwd {
					return fmt.Errorf("resume params = %#v", params)
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"thread": map[string]any{
						"id": threadID, "cwd": cwd,
					}},
				})
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, err := client.RecoverArchivedThread(
		ctx, threadID, cwd, map[string]string{"VPSFREE_DEV_SESSION_WORKSPACE": "/workspace"},
	)
	if err != nil || id != threadID {
		t.Fatalf("recovered thread = %q, %v", id, err)
	}
}

func TestRecoverArchivedThreadRetryResumesAlreadyActiveIdentity(t *testing.T) {
	threadID := "thread-active"
	cwd := "/workspace/work/revived"
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index, expected := range []string{"thread/list", "thread/list", "thread/resume"} {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			if request["method"] != expected {
				return fmt.Errorf("request %d = %#v, want %s", index, request, expected)
			}
			params := request["params"].(map[string]any)
			if index < 2 {
				data := []any{}
				if index == 0 {
					data = append(data, map[string]any{"id": threadID, "cwd": cwd, "source": "vscode"})
				} else {
					data = append(data,
						map[string]any{"id": "older-thread-1", "cwd": cwd, "source": "vscode"},
						map[string]any{"id": "older-thread-2", "cwd": cwd, "source": "vscode"},
					)
				}
				if err := writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"data": data},
				}); err != nil {
					return err
				}
				continue
			}
			if params["threadId"] != threadID || params["cwd"] != cwd {
				return fmt.Errorf("resume params = %#v", params)
			}
			return writeObject(connection, map[string]any{
				"id": request["id"], "result": map[string]any{"thread": map[string]any{
					"id": threadID, "cwd": cwd,
				}},
			})
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, err := client.RecoverArchivedThread(ctx, threadID, cwd, nil)
	if err != nil || id != threadID {
		t.Fatalf("retried recovery = %q, %v", id, err)
	}
}

func TestRecoverArchivedThreadRejectsAnotherDirectoryIdentity(t *testing.T) {
	cwd := "/workspace/work/revived"
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index := 0; index < 2; index++ {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			data := []any{}
			if index == 0 {
				data = append(data, map[string]any{"id": "thread-other", "cwd": cwd, "source": "vscode"})
			}
			if err := writeObject(connection, map[string]any{
				"id": request["id"], "result": map[string]any{"data": data},
			}); err != nil {
				return err
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.RecoverArchivedThread(ctx, "thread-expected", cwd, nil)
	if err == nil || !strings.Contains(err.Error(), "another active Codex thread") {
		t.Fatalf("identity error = %v", err)
	}
}

func TestOpenThreadDoesNotReplaceMissingPersistedThread(t *testing.T) {
	var startCalls atomic.Int32
	done := make(chan struct{})
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		defer close(done)
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/resume" {
			return fmt.Errorf("expected thread/resume, got %v", request["method"])
		}
		if err := writeObject(connection, map[string]any{
			"id":    request["id"],
			"error": map[string]any{"code": -32001, "message": "thread not found"},
		}); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		_, data, err := connection.Read(ctx)
		if err != nil {
			return nil
		}
		var followUp map[string]any
		if err := json.Unmarshal(data, &followUp); err != nil {
			return err
		}
		if followUp["method"] == "thread/start" {
			startCalls.Add(1)
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.OpenThread(
		ctx,
		"persisted-thread",
		"/workspace/work/example",
		map[string]string{"VPSFREE_DEV_SESSION_WORKSPACE": "/workspace"},
	)
	if err == nil || !strings.Contains(err.Error(), "thread not found") {
		t.Fatalf("missing persisted thread result = %v", err)
	}
	<-done
	if startCalls.Load() != 0 {
		t.Fatal("missing persisted thread was replaced")
	}
}

func TestLoadedThreadIDsFollowsEveryPage(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for pageNumber := 0; pageNumber < 2; pageNumber++ {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			if request["method"] != "thread/loaded/list" {
				return fmt.Errorf("expected thread/loaded/list, got %v", request["method"])
			}
			params, _ := request["params"].(map[string]any)
			if params["limit"] != float64(100) {
				return fmt.Errorf("loaded-thread limit = %v", params["limit"])
			}
			result := map[string]any{"data": []any{"thread-1"}, "nextCursor": "page-2"}
			if pageNumber == 1 {
				if params["cursor"] != "page-2" {
					return fmt.Errorf("loaded-thread cursor = %v", params["cursor"])
				}
				result = map[string]any{"data": []any{"thread-2"}, "nextCursor": nil}
			}
			if err := writeObject(connection, map[string]any{"id": request["id"], "result": result}); err != nil {
				return err
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ids, err := client.loadedThreadIDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(ids, ",") != "thread-1,thread-2" {
		t.Fatalf("loaded threads = %#v", ids)
	}
}

func TestListResponsesFailClosedWhenDataIsMissing(t *testing.T) {
	rollout := filepath.Join(t.TempDir(), "rollout.jsonl")
	if err := os.WriteFile(rollout, []byte("materialized\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		run  func(context.Context, *Client) error
	}{
		{
			name: "thread list",
			run: func(ctx context.Context, client *Client) error {
				_, err := client.RecoverCreatingThread(ctx, "", "/workspace/work/example", nil)
				return err
			},
		},
		{
			name: "thread turns for transcript",
			run: func(ctx context.Context, client *Client) error {
				_, err := client.ReadThread(ctx, "thread-1")
				return err
			},
		},
		{
			name: "thread turns for idle check",
			run: func(ctx context.Context, client *Client) error {
				return client.RequireThreadIdle(ctx, "thread-1", "/workspace/work/example")
			},
		},
		{
			name: "initial message turns",
			run: func(ctx context.Context, client *Client) error {
				return client.EnsureInitialMessage(ctx, "thread-1", "/workspace/work/example", "initial request", false)
			},
		},
		{
			name: "active turn lookup",
			run: func(ctx context.Context, client *Client) error {
				_, err := client.Send(ctx, "thread-1", "follow-up", "client-1", "")
				return err
			},
		},
		{
			name: "thread items",
			run: func(ctx context.Context, client *Client) error {
				_, err := client.threadItems(ctx, "thread-1", "item-1")
				return err
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				for {
					request, err := readObject(connection)
					if err != nil {
						return err
					}
					result := map[string]any{}
					switch request["method"] {
					case "thread/read":
						result["thread"] = map[string]any{
							"id": "thread-1", "cwd": "/workspace/work/example", "path": rollout,
						}
					case "thread/resume":
						result["thread"] = map[string]any{
							"id": "thread-1", "cwd": "/workspace/work/example",
						}
					}
					if err := writeObject(connection, map[string]any{
						"id": request["id"], "result": result,
					}); err != nil {
						return err
					}
					if strings.HasSuffix(request["method"].(string), "/list") {
						return nil
					}
				}
			})
			client := New(socket)
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := testCase.run(ctx, client); err == nil || !strings.Contains(err.Error(), "returned no data") {
				t.Fatalf("missing data result = %v", err)
			}
		})
	}
}

func TestRequireThreadIdleRejectsAnActiveTurn(t *testing.T) {
	for _, testCase := range []struct {
		status string
		ok     bool
	}{{"completed", true}, {"failed", true}, {"interrupted", true}, {"inProgress", false}, {"waiting", false}} {
		t.Run(testCase.status, func(t *testing.T) {
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				request, err := readObject(connection)
				if err != nil || request["method"] != "thread/read" {
					return fmt.Errorf("expected thread/read: %v", err)
				}
				if err := writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"thread": map[string]any{
						"id": "thread-1", "cwd": "/workspace/work/example",
					}},
				}); err != nil {
					return err
				}
				request, err = readObject(connection)
				if err != nil || request["method"] != "thread/turns/list" {
					return fmt.Errorf("expected thread/turns/list: %v", err)
				}
				if err := writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"data": []any{
						map[string]any{"id": "turn-1", "status": testCase.status},
					}},
				}); err != nil || !testCase.ok {
					return err
				}
				request, err = readObject(connection)
				if err != nil || request["method"] != "thread/queue/list" {
					return fmt.Errorf("expected thread/queue/list: %v", err)
				}
				return writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"data": []any{}, "nextCursor": nil},
				})
			})
			client := New(socket)
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := client.RequireThreadIdle(ctx, "thread-1", "/workspace/work/example")
			if (err == nil) != testCase.ok {
				t.Fatalf("idle = %t, want %t: %v", err == nil, testCase.ok, err)
			}
		})
	}
}

func TestRequireThreadIdleRejectsPendingRequestsAndQueuedMessages(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		pending bool
		queued  bool
		want    string
	}{
		{name: "pending request", pending: true, want: "pending request"},
		{name: "queued message", queued: true, want: "queued message"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				for _, method := range []string{"thread/read", "thread/turns/list"} {
					request, err := readObject(connection)
					if err != nil || request["method"] != method {
						return fmt.Errorf("expected %s: %v", method, err)
					}
					result := map[string]any{"data": []any{}}
					if method == "thread/read" {
						result = map[string]any{"thread": map[string]any{
							"id": "thread-1", "cwd": "/workspace/work/example",
						}}
					}
					if err := writeObject(connection, map[string]any{
						"id": request["id"], "result": result,
					}); err != nil {
						return err
					}
				}
				if testCase.pending {
					return nil
				}
				request, err := readObject(connection)
				if err != nil || request["method"] != "thread/queue/list" {
					return fmt.Errorf("expected thread/queue/list: %v", err)
				}
				queue := []any{}
				if testCase.queued {
					queue = append(queue, map[string]any{
						"id": "queue-1", "clientUserMessageId": "client-1",
						"input": []any{map[string]any{"type": "text", "text": "later"}},
					})
				}
				return writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{
						"data": queue, "nextCursor": nil,
					},
				})
			})
			client := New(socket)
			defer client.Close()
			if testCase.pending {
				params, err := json.Marshal(map[string]any{
					"threadId": "thread-1", "turnId": "turn-1", "itemId": "item-1",
					"questions": []any{map[string]any{
						"id": "choice", "header": "Choice", "question": "Continue?",
						"options": []any{map[string]any{"label": "Yes", "description": "Continue."}},
					}},
				})
				if err != nil {
					t.Fatal(err)
				}
				client.requests["request-1"] = PendingRequest{
					ID: "request-1", Method: "item/tool/requestUserInput", Params: params,
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := client.RequireThreadIdle(ctx, "thread-1", "/workspace/work/example")
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("idle check = %v, want %q", err, testCase.want)
			}
		})
	}
}

func TestRequireThreadIdleRejectsUnresolvedDurableSubmissionAttempts(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		queue bool
		want  string
	}{
		{name: "send attempt", want: "unresolved message attempt"},
		{name: "queue attempt", queue: true, want: "unresolved queued message attempt"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				methods := []string{"thread/read", "thread/turns/list", "thread/queue/list"}
				if testCase.queue {
					methods = append(methods, "thread/items/list")
				}
				for _, method := range methods {
					request, err := readObject(connection)
					if err != nil || request["method"] != method {
						return fmt.Errorf("expected %s: %v", method, err)
					}
					result := map[string]any{"data": []any{}, "nextCursor": nil}
					if method == "thread/read" {
						result = map[string]any{"thread": map[string]any{
							"id": "thread-1", "cwd": "/workspace/work/example",
						}}
					}
					if err := writeObject(connection, map[string]any{
						"id": request["id"], "result": result,
					}); err != nil {
						return err
					}
				}
				return nil
			})
			client := New(socket)
			defer client.Close()
			if testCase.queue {
				if err := client.recordQueueAttempt("thread-1", "client-1", "queued text"); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := client.recordSendAttempt(
					"thread-1", "client-1", "message text", "", false,
				); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := client.RequireThreadIdle(ctx, "thread-1", "/workspace/work/example")
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("idle check = %v, want %q", err, testCase.want)
			}
		})
	}
}

func TestRequireThreadIdleAcceptsAQueuedAttemptPresentInCompletedHistory(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for _, method := range []string{
			"thread/read", "thread/turns/list", "thread/queue/list", "thread/items/list",
		} {
			request, err := readObject(connection)
			if err != nil || request["method"] != method {
				return fmt.Errorf("expected %s: %v", method, err)
			}
			result := map[string]any{"data": []any{}, "nextCursor": nil}
			switch method {
			case "thread/read":
				result = map[string]any{"thread": map[string]any{
					"id": "thread-1", "cwd": "/workspace/work/example",
				}}
			case "thread/items/list":
				result = map[string]any{"data": []any{map[string]any{
					"turnId": "turn-1", "item": map[string]any{
						"id": "item-1", "type": "userMessage", "clientId": "client-1",
						"content": []any{map[string]any{"type": "text", "text": "queued text"}},
					},
				}}, "nextCursor": nil}
			}
			if err := writeObject(connection, map[string]any{
				"id": request["id"], "result": result,
			}); err != nil {
				return err
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	if err := client.recordQueueAttempt("thread-1", "client-1", "queued text"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.RequireThreadIdle(ctx, "thread-1", "/workspace/work/example"); err != nil {
		t.Fatalf("resolved queue attempt rejected: %v", err)
	}
}

func TestApprovalAuthorityAndResolvedRequests(t *testing.T) {
	ready := make(chan *websocket.Conn, 1)
	response := make(chan map[string]any, 1)
	resolvedSent := make(chan struct{}, 1)
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		ready <- connection
		message, err := readObject(connection)
		if err != nil {
			return err
		}
		if message["method"] != "thread/items/list" {
			return errors.New("expected approval authority lookup")
		}
		if err := writeObject(connection, map[string]any{
			"id": message["id"], "result": map[string]any{"data": []any{map[string]any{
				"turnId": "turn-1", "item": map[string]any{
					"id": "item-1", "type": "commandExecution", "command": "dangerous command",
				},
			}}, "nextCursor": "older-items"},
		}); err != nil {
			return err
		}
		message, err = readObject(connection)
		if err != nil {
			return err
		}
		response <- message
		if err := writeObject(connection, map[string]any{
			"id": "approval-2", "method": "item/fileChange/requestApproval", "params": map[string]any{
				"threadId": "thread-1", "turnId": "turn-1", "itemId": "item-2", "startedAtMs": 2,
			},
		}); err != nil {
			return err
		}
		if err := writeObject(connection, map[string]any{
			"method": "serverRequest/resolved", "params": map[string]any{"requestId": "approval-2", "threadId": "thread-1"},
		}); err != nil {
			return err
		}
		resolvedSent <- struct{}{}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	connection := <-ready
	params := map[string]any{
		"threadId": "thread-1", "turnId": "turn-1", "itemId": "item-1", "startedAtMs": 1,
		"command": "dangerous command", "cwd": "/workspace", "reason": "needs access",
		"availableDecisions": []string{"accept", "decline"},
	}
	if err := writeObject(connection, map[string]any{
		"id": "approval-1", "method": "item/commandExecution/requestApproval", "params": params,
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(client.Prompts("thread-1")) == 1 })
	prompt := client.Prompts("thread-1")[0]
	if prompt.Params["command"] != "dangerous command" || prompt.Params["cwd"] != "/workspace" {
		t.Fatalf("approval context was lost: %#v", prompt.Params)
	}
	if got := strings.Join(prompt.AvailableDecisions, ","); got != "accept,decline" {
		t.Fatalf("available decisions = %q", got)
	}
	if err := client.RespondDecision(ctx, prompt.ID, "thread-1", "cancel"); err == nil {
		t.Fatal("unoffered decision was accepted")
	}
	if err := client.RespondDecision(ctx, prompt.ID, "thread-1", "accept"); err != nil {
		t.Fatal(err)
	}
	if err := client.RespondDecision(ctx, prompt.ID, "thread-1", "accept"); err == nil {
		t.Fatal("approval could be answered twice")
	}
	answered := <-response
	result := answered["result"].(map[string]any)
	if result["decision"] != "accept" {
		t.Fatalf("response = %#v", answered)
	}

	// A resolved notification must remove a prompt even if the browser never
	// answered it.
	<-resolvedSent
	waitFor(t, func() bool { return len(client.Prompts("thread-1")) == 0 })
}

func TestFileChangeApprovalRequiresTheMatchingThreadItem(t *testing.T) {
	ready := make(chan *websocket.Conn, 1)
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		ready <- connection
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/items/list" {
			return errors.New("expected approval authority lookup")
		}
		if err := writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"data": []any{}},
		}); err != nil {
			return err
		}
		_, _, _ = connection.Read(context.Background())
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	connection := <-ready
	if err := writeObject(connection, map[string]any{
		"id": "file-1", "method": "item/fileChange/requestApproval", "params": map[string]any{
			"threadId": "thread-1", "turnId": "turn-1", "itemId": "missing-item",
		},
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(client.Prompts("thread-1")) == 1 })
	prompt := client.Prompts("thread-1")[0]
	err := client.RespondDecision(ctx, prompt.ID, "thread-1", "accept")
	if err == nil || !strings.Contains(err.Error(), "matching approval item is unavailable") {
		t.Fatalf("missing authority result = %v", err)
	}
}

func TestEnsureInitialMessageIsRetrySafe(t *testing.T) {
	var turnStarted atomic.Int32
	var historyAttempts atomic.Int32
	var rolloutReadAttempts atomic.Int32
	rollout := filepath.Join(t.TempDir(), "rollout.jsonl")
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		initialExists := false
		for requestNumber := 0; requestNumber < 15; requestNumber++ {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			switch request["method"] {
			case "thread/resume":
				if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{}}); err != nil {
					return err
				}
			case "thread/read":
				if initialExists && rolloutReadAttempts.Add(1) == 1 {
					if err := writeObject(connection, map[string]any{
						"id": request["id"], "error": map[string]any{
							"code": -32603,
							"message": "failed to read thread: thread-store internal error: " +
								"failed to read session metadata " + rollout + ": rollout at " + rollout + " is empty",
						},
					}); err != nil {
						return err
					}
					if err := os.WriteFile(rollout, []byte("materialized\n"), 0o600); err != nil {
						return err
					}
					continue
				}
				if err := writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{
						"thread": freshThreadMetadata("thread-1", "/workspace/work/example", rollout),
					},
				}); err != nil {
					return err
				}
			case "thread/turns/list":
				if !initialExists {
					return errors.New("history was listed before the first turn materialized it")
				}
				historyAttempt := historyAttempts.Add(1)
				if historyAttempt == 1 {
					if err := writeObject(connection, map[string]any{
						"id": request["id"],
						"error": map[string]any{
							"code": -32601, "message": "list_turns is not supported yet",
						},
					}); err != nil {
						return err
					}
					continue
				}
				if historyAttempt == 2 {
					if err := writeObject(connection, map[string]any{
						"id": request["id"], "result": map[string]any{
							"data": []any{map[string]any{"items": []any{}}},
						},
					}); err != nil {
						return err
					}
					continue
				}
				turns := []any{}
				if initialExists {
					turns = []any{map[string]any{"items": []any{map[string]any{
						"type": "userMessage", "content": []any{map[string]any{"type": "text", "text": "initial goal"}},
					}}}}
				}
				if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{"data": turns}}); err != nil {
					return err
				}
			case "turn/start":
				turnStarted.Add(1)
				initialExists = true
				if err := os.WriteFile(rollout, nil, 0o600); err != nil {
					return err
				}
				if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{}}); err != nil {
					return err
				}
			default:
				return errors.New("unexpected request")
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.EnsureInitialMessage(ctx, "thread-1", "/workspace/work/example", "initial goal", true); err != nil {
		t.Fatal(err)
	}
	if err := client.EnsureInitialMessage(ctx, "thread-1", "/workspace/work/example", "initial goal", false); err != nil {
		t.Fatal(err)
	}
	if turnStarted.Load() != 1 {
		t.Fatalf("initial turn started %d times", turnStarted.Load())
	}
}

func TestConfiguredCodexFreshThreadContract(t *testing.T) {
	binary := os.Getenv("VPSFREE_CODEX_TEST_BINARY")
	if binary == "" {
		t.Skip("VPSFREE_CODEX_TEST_BINARY is not configured")
	}
	directory, err := os.MkdirTemp("/tmp", "workspace-codex-contract-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)
	home := filepath.Join(directory, "home")
	codexHome := filepath.Join(home, ".codex")
	cwd := filepath.Join(directory, "workspace", "work", "example")
	for _, path := range []string{codexHome, cwd} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	socket := filepath.Join(directory, "app-server.sock")
	command := exec.Command(binary, "app-server", "--listen", "unix://"+socket)
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "HOME=") && !strings.HasPrefix(entry, "CODEX_HOME=") {
			command.Env = append(command.Env, entry)
		}
	}
	command.Env = append(command.Env, "HOME="+home, "CODEX_HOME="+codexHome)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
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
		if info, statErr := os.Stat(socket); statErr == nil && info.Mode()&os.ModeSocket != 0 {
			break
		}
		if command.ProcessState != nil || time.Now().After(deadline) {
			t.Fatalf("Codex App Server did not create its socket: %s", output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	models, err := client.ListModels(ctx)
	if err != nil {
		t.Fatalf("list configured Codex models: %v\n%s", err, output.String())
	}
	settings, err := ResolveNewThreadSettings(models, ThreadSettings{})
	if err != nil || settings.ReasoningEffort != DefaultNewThreadReasoningEffort {
		t.Fatalf("resolve configured Codex defaults: %#v, %v\n%s", settings, err, output.String())
	}
	threadID, err := client.StartThreadWithSettings(ctx, cwd, map[string]string{
		"VPSFREE_DEV_SESSION_WORKSPACE": filepath.Join(directory, "workspace"),
	}, settings)
	if err != nil {
		t.Fatalf("start exact Codex thread: %v\n%s", err, output.String())
	}
	materialized, err := client.threadHistoryMaterialized(ctx, threadID, cwd)
	if err != nil {
		t.Fatalf("read exact fresh Codex thread: %v\n%s", err, output.String())
	}
	if materialized {
		t.Fatal("exact Codex materialized a fresh thread before its first user turn")
	}
	if err := client.RequireThreadMaterialized(ctx, threadID, cwd); err == nil ||
		!strings.Contains(err.Error(), "no persisted history") {
		t.Fatalf("exact fresh Codex materialization check = %v", err)
	}
	recoveredID, err := client.RecoverCreatingThread(ctx, threadID, cwd, map[string]string{
		"VPSFREE_DEV_SESSION_WORKSPACE": filepath.Join(directory, "workspace"),
	})
	if err != nil {
		t.Fatalf("reconcile exact fresh Codex thread: %v\n%s", err, output.String())
	}
	if recoveredID != threadID {
		t.Fatalf("reconciled exact fresh thread %q as %q", threadID, recoveredID)
	}
	const goal = "workspace portal protocol contract"
	if err := client.EnsureInitialMessage(ctx, threadID, cwd, goal, true); err != nil {
		t.Fatalf("start exact Codex initial turn: %v\n%s", err, output.String())
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		materialized, err = client.threadHistoryMaterialized(ctx, threadID, cwd)
		if err == nil && materialized {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("exact Codex did not materialize initial history: %v\n%s", err, output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := client.RequireThreadMaterialized(ctx, threadID, cwd); err != nil {
		t.Fatalf("accept exact persisted Codex thread: %v\n%s", err, output.String())
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		err = client.EnsureInitialMessage(ctx, threadID, cwd, goal, false)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("verify exact Codex initial history: %v\n%s", err, output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	var history struct {
		Data []struct {
			Items []struct {
				Type    string `json:"type"`
				Content []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"content"`
			} `json:"items"`
		} `json:"data"`
	}
	if err := client.Request(ctx, "thread/turns/list", map[string]any{
		"threadId": threadID, "limit": 10, "sortDirection": "asc", "itemsView": "full",
	}, &history); err != nil {
		t.Fatalf("list exact Codex initial history: %v\n%s", err, output.String())
	}
	var userRequests []string
	for _, turn := range history.Data {
		for _, item := range turn.Items {
			if item.Type != "userMessage" {
				continue
			}
			var parts []string
			for _, content := range item.Content {
				if content.Type == "text" {
					parts = append(parts, content.Text)
				}
			}
			userRequests = append(userRequests, strings.TrimSpace(strings.Join(parts, "\n")))
		}
	}
	if len(userRequests) != 1 || userRequests[0] != goal {
		t.Fatalf("exact Codex initial user requests = %#v", userRequests)
	}
}

func TestEnsureInitialMessageRejectsInvalidUnmaterializedThreads(t *testing.T) {
	loop := filepath.Join(t.TempDir(), "loop")
	if err := os.Symlink(loop, loop); err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	missing := filepath.Join(t.TempDir(), "rollout.jsonl")
	tests := []struct {
		name    string
		prepare func(map[string]any)
		message string
	}{
		{"wrong id", func(thread map[string]any) { thread["id"] = "thread-2" }, "wrong thread"},
		{"wrong cwd", func(thread map[string]any) { thread["cwd"] = "/workspace/work/other" }, "working directory"},
		{"missing path", func(thread map[string]any) { delete(thread, "path") }, "no rollout path"},
		{"null path", func(thread map[string]any) { thread["path"] = nil }, "no rollout path"},
		{"relative path", func(thread map[string]any) { thread["path"] = "relative/rollout.jsonl" }, "invalid rollout path"},
		{"stat error", func(thread map[string]any) { thread["path"] = loop }, "inspect Codex thread rollout"},
		{"nonregular path", func(thread map[string]any) { thread["path"] = directory }, "not a regular file"},
		{"wrong source", func(thread map[string]any) { thread["source"] = "cli" }, "not a fresh idle"},
		{"structured source", func(thread map[string]any) {
			thread["source"] = map[string]any{"custom": "other-client"}
		}, "not a fresh idle"},
		{"missing ephemeral", func(thread map[string]any) { delete(thread, "ephemeral") }, "not a fresh idle"},
		{"ephemeral", func(thread map[string]any) { thread["ephemeral"] = true }, "not a fresh idle"},
		{"wrong history mode", func(thread map[string]any) { thread["historyMode"] = "loaded" }, "not a fresh idle"},
		{"nonempty preview", func(thread map[string]any) { thread["preview"] = "started" }, "not a fresh idle"},
		{"active", func(thread map[string]any) {
			thread["status"] = map[string]any{"type": "active", "activeFlags": []any{}}
		}, "not a fresh idle"},
		{"missing turns", func(thread map[string]any) { delete(thread, "turns") }, "not a fresh idle"},
		{"existing turn", func(thread map[string]any) { thread["turns"] = []any{map[string]any{"id": "turn-1"}} }, "not a fresh idle"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var turnStarted atomic.Bool
			thread := freshThreadMetadata("thread-1", "/workspace/work/example", missing)
			testCase.prepare(thread)
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				for requestNumber := 0; requestNumber < 1; requestNumber++ {
					request, err := readObject(connection)
					if err != nil {
						return err
					}
					switch request["method"] {
					case "thread/resume":
						err = writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{}})
					case "thread/read":
						err = writeObject(connection, map[string]any{
							"id": request["id"], "result": map[string]any{"thread": thread},
						})
					case "turn/start":
						turnStarted.Store(true)
						return errors.New("invalid thread started a turn")
					default:
						return fmt.Errorf("unexpected request %v", request["method"])
					}
					if err != nil {
						return err
					}
				}
				return nil
			})
			client := New(socket)
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := client.EnsureInitialMessage(
				ctx, "thread-1", "/workspace/work/example", "initial goal", true,
			)
			if err == nil || !strings.Contains(err.Error(), testCase.message) {
				t.Fatalf("invalid thread result = %v, want %q", err, testCase.message)
			}
			if turnStarted.Load() {
				t.Fatal("invalid thread started a turn")
			}
		})
	}
}

func TestEnsureInitialMessageDoesNotRepeatAfterAnAmbiguousSubmission(t *testing.T) {
	var connections atomic.Int32
	var turnStarted atomic.Int32
	rollout := filepath.Join(t.TempDir(), "rollout.jsonl")
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		connectionNumber := connections.Add(1)
		if err := handshake(connection); err != nil {
			return err
		}
		for requestNumber := 0; requestNumber < 3; requestNumber++ {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			switch request["method"] {
			case "thread/resume":
				err = writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{}})
			case "thread/read":
				thread := freshThreadMetadata("thread-1", "/workspace/work/example", rollout)
				if connectionNumber > 1 {
					return writeObject(connection, map[string]any{
						"id": request["id"], "result": map[string]any{"thread": thread},
					})
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"thread": thread},
				})
			case "turn/start":
				turnStarted.Add(1)
				return connection.Close(websocket.StatusInternalError, "lost turn/start response")
			default:
				return fmt.Errorf("unexpected request %v", request["method"])
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first := New(socket)
	if err := first.EnsureInitialMessage(ctx, "thread-1", "/workspace/work/example", "initial goal", true); err == nil {
		t.Fatal("lost turn/start response unexpectedly succeeded")
	}
	first.Close()
	second := New(socket)
	defer second.Close()
	err := second.EnsureInitialMessage(ctx, "thread-1", "/workspace/work/example", "initial goal", false)
	if err == nil || !strings.Contains(err.Error(), "may already have been accepted") {
		t.Fatalf("ambiguous unmaterialized retry result = %v", err)
	}
	if turnStarted.Load() != 1 {
		t.Fatalf("initial turn started %d times", turnStarted.Load())
	}
}

func TestEnsureInitialMessageRejectsConflictingMaterializedHistory(t *testing.T) {
	rollout := filepath.Join(t.TempDir(), "rollout.jsonl")
	if err := os.WriteFile(rollout, []byte("materialized\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		items   []any
		empty   bool
		message string
	}{
		{
			"different request",
			[]any{map[string]any{
				"type": "userMessage", "content": []any{map[string]any{"type": "text", "text": "different"}},
			}},
			false,
			"different initial request",
		},
		{
			"multiple matching user requests",
			[]any{
				map[string]any{"type": "userMessage", "content": []any{map[string]any{"type": "text", "text": "initial goal"}}},
				map[string]any{"type": "userMessage", "content": []any{map[string]any{"type": "text", "text": "initial goal"}}},
			},
			false,
			"different initial request",
		},
		{
			"non-text initial request",
			[]any{map[string]any{
				"type": "userMessage", "content": []any{map[string]any{"type": "image", "url": "file:///tmp/input.png"}},
			}},
			false,
			"non-text initial request",
		},
		{"missing user request", []any{map[string]any{"type": "agentMessage", "text": "reply"}}, false, "no initial user request"},
		{"missing initial turn", nil, true, "no initial user request"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var turnStarted atomic.Bool
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				for requestNumber := 0; requestNumber < 3; requestNumber++ {
					request, err := readObject(connection)
					if err != nil {
						return err
					}
					switch request["method"] {
					case "thread/resume":
						err = writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{}})
					case "thread/read":
						err = writeObject(connection, map[string]any{
							"id": request["id"], "result": map[string]any{
								"thread": freshThreadMetadata("thread-1", "/workspace/work/example", rollout),
							},
						})
					case "thread/turns/list":
						turns := []any{map[string]any{"items": testCase.items}}
						if testCase.empty {
							turns = []any{}
						}
						err = writeObject(connection, map[string]any{
							"id": request["id"], "result": map[string]any{"data": turns},
						})
					case "turn/start":
						turnStarted.Store(true)
						return errors.New("conflicting history started a turn")
					default:
						return fmt.Errorf("unexpected request %v", request["method"])
					}
					if err != nil {
						return err
					}
				}
				return nil
			})
			client := New(socket)
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := client.EnsureInitialMessage(
				ctx, "thread-1", "/workspace/work/example", "initial goal", false,
			)
			if err == nil || !strings.Contains(err.Error(), testCase.message) {
				t.Fatalf("conflicting history result = %v, want %q", err, testCase.message)
			}
			if turnStarted.Load() {
				t.Fatal("conflicting history started a turn")
			}
		})
	}
}

func TestStaleDisconnectDoesNotClearReplacementConnection(t *testing.T) {
	var connections atomic.Int32
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		connections.Add(1)
		if err := handshake(connection); err != nil {
			return err
		}
		_, _, _ = connection.Read(context.Background())
		return nil
	})
	client := New(socket)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	client.connectionMu.Lock()
	first := client.connection
	firstGeneration := client.generation
	client.connectionMu.Unlock()
	client.markDisconnected(first, firstGeneration, errors.New("test replacement"))
	if err := client.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	client.connectionMu.Lock()
	replacement := client.connection
	client.connectionMu.Unlock()
	client.markDisconnected(first, firstGeneration, errors.New("stale reader"))
	client.connectionMu.Lock()
	actual := client.connection
	client.connectionMu.Unlock()
	if actual != replacement || connections.Load() != 2 {
		t.Fatal("stale disconnect cleared the replacement connection")
	}
	first.CloseNow()
	client.Close()
}

func TestWriteRejectsAStaleConnectionGeneration(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		_, _, _ = connection.Read(context.Background())
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	client.connectionMu.Lock()
	stale := client.connection
	staleGeneration := client.generation
	client.connectionMu.Unlock()
	client.markDisconnected(stale, staleGeneration, errors.New("roll over"))
	if err := client.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.writeOn(ctx, stale, staleGeneration, []byte(`{"method":"stale"}`)); err == nil {
		t.Fatal("stale write was accepted")
	}
	stale.CloseNow()
}

func TestPromptNormalizationMatchesPinnedProtocolDefaults(t *testing.T) {
	commandFixture := readProtocolFixture(t, "command-approval")
	command, err := normalizePrompt(PendingRequest{
		ID: "1", Method: commandFixture.Method, Params: commandFixture.Params,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(command.AvailableDecisions, ","); got != "accept,acceptForSession,decline,cancel" {
		t.Fatalf("default decisions = %q", got)
	}

	inputFixture := readProtocolFixture(t, "request-user-input")
	input, err := normalizePrompt(PendingRequest{
		ID: "2", Method: inputFixture.Method, Params: inputFixture.Params,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(input.Questions) != 1 || !input.Questions[0].IsOther ||
		input.Questions[0].Options[0].Label != "First" || !input.AuthorityAvailable {
		t.Fatalf("free-form input metadata was lost: %#v", input)
	}

	permission, err := normalizePrompt(PendingRequest{
		ID: "3", Method: "item/permissions/requestApproval",
		Params: json.RawMessage(`{"threadId":"thread-1","permissions":{"network":true}}`),
	})
	if err != nil || permission.Kind != "terminalOnly" || permission.AuthorityAvailable {
		t.Fatalf("permission approval result = %#v, %v", permission, err)
	}
}

func TestRequestUserInputAnswerShapesMatchTheCLI(t *testing.T) {
	prompt := Prompt{Questions: []Question{
		{ID: "choice", IsOther: true, Options: []Option{{Label: "First"}, {Label: "Second"}}},
		{ID: "freeform"},
		{ID: "skipped"},
	}}
	valid := map[string]map[string][]string{
		"choice":   {"answers": {"First", "user_note: because"}},
		"freeform": {"answers": {"user_note: my answer"}},
		"skipped":  {"answers": {}},
	}
	if err := validateAnswers(prompt, valid); err != nil {
		t.Fatalf("valid answer shapes rejected: %v", err)
	}
	valid["choice"] = map[string][]string{"answers": {"user_note: custom"}}
	if err := validateAnswers(prompt, valid); err != nil {
		t.Fatalf("other answer rejected: %v", err)
	}

	invalid := []map[string]map[string][]string{
		{
			"choice": {"answers": {"Not offered"}}, "freeform": {"answers": {}},
			"skipped": {"answers": {}},
		},
		{
			"choice": {"answers": {"First", "plain note"}}, "freeform": {"answers": {}},
			"skipped": {"answers": {}},
		},
		{
			"choice": {"answers": {}}, "freeform": {"answers": {"plain answer"}},
			"skipped": {"answers": {}},
		},
		{
			"choice": {"answers": {}}, "freeform": {"answers": {"user_note: "}},
			"skipped": {"answers": {}},
		},
	}
	for _, answers := range invalid {
		if err := validateAnswers(prompt, answers); err == nil {
			t.Fatalf("invalid answers accepted: %#v", answers)
		}
	}
}

type protocolFixture struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

func readProtocolFixture(t *testing.T, name string) protocolFixture {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name+"-"+protocolFixtureVersion+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture protocolFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func TestPermissionRequestRemainsUnansweredUntilTerminalResolution(t *testing.T) {
	responses := make(chan map[string]any, 1)
	requestSent := make(chan struct{}, 1)
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		if err := writeObject(connection, map[string]any{
			"id": "permission-1", "method": "item/permissions/requestApproval",
			"params": map[string]any{"threadId": "thread-1", "permissions": map[string]any{"network": true}},
		}); err != nil {
			return err
		}
		requestSent <- struct{}{}
		go func() {
			message, err := readObject(connection)
			if err == nil {
				responses <- message
			}
		}()
		time.Sleep(200 * time.Millisecond)
		return writeObject(connection, map[string]any{
			"method": "serverRequest/resolved", "params": map[string]any{"requestId": "permission-1"},
		})
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	<-requestSent
	waitFor(t, func() bool { return len(client.Prompts("thread-1")) == 1 })
	if prompt := client.Prompts("thread-1")[0]; prompt.Kind != "terminalOnly" {
		t.Fatalf("permission prompt = %#v", prompt)
	}
	select {
	case response := <-responses:
		t.Fatalf("portal answered terminal-only permission: %#v", response)
	case <-time.After(100 * time.Millisecond):
	}
	waitFor(t, func() bool { return len(client.Prompts("thread-1")) == 0 })
}

func TestUnsupportedServerRequestIsRejectedAndSurfaced(t *testing.T) {
	response := make(chan map[string]any, 1)
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		if err := writeObject(connection, map[string]any{
			"id": "mcp-1", "method": "mcpServer/elicitation/request", "params": map[string]any{
				"threadId": "thread-1", "turnId": "turn-1", "serverName": "example",
				"message": "Need a value", "mode": "form", "requestedSchema": map[string]any{},
			},
		}); err != nil {
			return err
		}
		message, err := readObject(connection)
		if err == nil {
			response <- message
		}
		return err
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	message := <-response
	errorValue, ok := message["error"].(map[string]any)
	if !ok || errorValue["code"] != float64(-32601) {
		t.Fatalf("unsupported response = %#v", message)
	}
	waitFor(t, func() bool { return len(client.Prompts("thread-1")) == 1 })
	if client.Prompts("thread-1")[0].Kind != "unsupported" {
		t.Fatalf("unsupported request was not surfaced: %#v", client.Prompts("thread-1"))
	}
}

func TestNonblockingUserInputUsesTheFixedGraceAndCanBeSnoozed(t *testing.T) {
	response := make(chan map[string]any, 1)
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		if err := writeObject(connection, map[string]any{
			"id": "input-1", "method": "item/tool/requestUserInput",
			"params": map[string]any{
				"threadId": "thread-1", "turnId": "turn-1", "itemId": "item-1",
				"isBlocking": false, "autoResolutionMs": 5,
				"questions": []any{map[string]any{
					"id": "choice", "header": "Choice", "question": "Choose",
					"isOther": true, "options": []any{map[string]any{
						"label": "First", "description": "Use the first option",
					}},
				}},
			},
		}); err != nil {
			return err
		}
		message, err := readObject(connection)
		if err == nil {
			response <- message
		}
		return err
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(client.Prompts("thread-1")) == 1 })
	prompt := client.Prompts("thread-1")[0]
	if prompt.IsBlocking ||
		prompt.AutoResolutionVisibleAtMS < time.Now().Add(40*time.Second).UnixMilli() ||
		prompt.AutoResolutionAtMS < time.Now().Add(100*time.Second).UnixMilli() ||
		prompt.AutoResolutionVisibleAtMS >= prompt.AutoResolutionAtMS {
		t.Fatalf("nonblocking prompt deadline = %#v", prompt)
	}
	select {
	case message := <-response:
		t.Fatalf("prompt auto-resolved without the fixed grace: %#v", message)
	case <-time.After(100 * time.Millisecond):
	}
	if err := client.SnoozeUserInput(prompt.ID, "thread-1"); err != nil {
		t.Fatal(err)
	}
	if prompt = client.Prompts("thread-1")[0]; !prompt.AutoResolveSnoozed {
		t.Fatalf("snoozed prompt = %#v", prompt)
	}
	if err := client.RespondAnswers(ctx, prompt.ID, "thread-1", map[string]map[string][]string{
		"choice": {"answers": nil},
	}); err != nil {
		t.Fatal(err)
	}
	message := <-response
	result, ok := message["result"].(map[string]any)
	answers, answersOK := result["answers"].(map[string]any)
	if !ok || !answersOK || len(answers) != 1 {
		t.Fatalf("manual response = %#v", message)
	}
}

func TestRequestUserInputDefaultsMissingBlockingStateToBlocking(t *testing.T) {
	prompt, err := normalizePrompt(PendingRequest{
		ID:     "input-1",
		Method: "item/tool/requestUserInput",
		Params: json.RawMessage(`{
			"threadId":"thread-1",
			"questions":[{"id":"choice","header":"Choice","question":"Choose"}]
		}`),
		receivedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !prompt.IsBlocking || prompt.AutoResolutionVisibleAtMS != 0 || prompt.AutoResolutionAtMS != 0 {
		t.Fatalf("missing blocking state = %#v", prompt)
	}
}

func TestRequestUserInputRejectsMalformedBlockingState(t *testing.T) {
	_, err := normalizePrompt(PendingRequest{
		ID:     "input-1",
		Method: "item/tool/requestUserInput",
		Params: json.RawMessage(`{
			"threadId":"thread-1",
			"isBlocking":"false",
			"questions":[{"id":"choice","header":"Choice","question":"Choose"}]
		}`),
	})
	if err == nil || !strings.Contains(err.Error(), "invalid isBlocking") {
		t.Fatalf("malformed blocking state error = %v", err)
	}
}

func TestRecoverCreatingThreadIgnoresLoadedStructuredSources(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index := 0; index < 4; index++ {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			switch index {
			case 0:
				if request["method"] != "thread/loaded/list" {
					return fmt.Errorf("expected thread/loaded/list, got %v", request["method"])
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{
						"data": []any{"thread-subagent"}, "nextCursor": nil,
					},
				})
			case 1:
				if request["method"] != "thread/read" {
					return fmt.Errorf("expected thread/read, got %v", request["method"])
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"thread": map[string]any{
						"id": "thread-subagent", "cwd": "/workspace/work/other",
						"source": map[string]any{"subAgent": map[string]any{"threadSpawn": "agent"}},
					}},
				})
			case 2:
				if request["method"] != "thread/list" {
					return fmt.Errorf("expected thread/list, got %v", request["method"])
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"data": []any{}},
				})
			case 3:
				if request["method"] != "thread/start" {
					return fmt.Errorf("expected thread/start, got %v", request["method"])
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"thread": map[string]any{
						"id": "thread-new", "cwd": "/workspace/work/example",
					}},
				})
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, err := client.RecoverCreatingThread(
		ctx, "", "/workspace/work/example",
		map[string]string{"VPSFREE_DEV_SESSION_WORKSPACE": "/workspace"},
	)
	if err != nil || id != "thread-new" {
		t.Fatalf("structured unrelated source recovery = %q, %v", id, err)
	}
}

func TestRetireThreadInterruptsAnActiveTurnBeforeArchiving(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index, expected := range []string{
			"thread/list", "thread/read", "thread/turns/list", "turn/interrupt", "thread/turns/list", "thread/archive",
		} {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			if request["method"] != expected {
				return fmt.Errorf("request %d = %v, want %s", index, request["method"], expected)
			}
			params := request["params"].(map[string]any)
			if index > 0 && params["threadId"] != "thread-1" {
				return fmt.Errorf("wrong retirement thread: %#v", request)
			}
			var result any = map[string]any{}
			switch index {
			case 0:
				result = map[string]any{"data": []any{map[string]any{
					"id": "thread-1", "cwd": "/workspace/work/example", "source": "vscode",
				}}}
			case 1:
				result = map[string]any{"thread": map[string]any{
					"id": "thread-1", "cwd": "/workspace/work/example", "source": "vscode",
				}}
			case 2:
				result = map[string]any{"data": []any{map[string]any{
					"id": "turn-1", "status": "inProgress",
				}}}
			case 3:
				if params["turnId"] != "turn-1" {
					return fmt.Errorf("wrong interrupted turn: %#v", request)
				}
			case 4:
				result = map[string]any{"data": []any{map[string]any{
					"id": "turn-1", "status": "interrupted",
				}}}
			}
			if err := writeObject(connection, map[string]any{"id": request["id"], "result": result}); err != nil {
				return err
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.RetireThread(
		ctx, "thread-1", "/workspace/work/example", true,
	); err != nil {
		t.Fatal(err)
	}
}

func TestRetireThreadRefusesAmbiguousCwdLookup(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		return writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{
				"data": []any{
					map[string]any{"id": "thread-1", "cwd": "/workspace/work/example"},
					map[string]any{"id": "thread-2", "cwd": "/workspace/work/example"},
				},
			},
		})
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.RetireThread(ctx, "", "/workspace/work/example", false); err == nil ||
		!strings.Contains(err.Error(), "ambiguous retirement") {
		t.Fatalf("ambiguous retirement error = %v", err)
	}
}

func TestRetireThreadRefusesAnotherActiveThreadBesideTheExpectedOne(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		return writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{
				"data": []any{
					map[string]any{
						"id": "thread-orphan", "cwd": "/workspace/work/example", "source": "vscode",
					},
					map[string]any{
						"id": "thread-expected", "cwd": "/workspace/work/example", "source": "vscode",
					},
				},
			},
		})
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.RetireThread(
		ctx, "thread-expected", "/workspace/work/example", true,
	); err == nil || !strings.Contains(err.Error(), "ambiguous retirement") {
		t.Fatalf("active sibling retirement error = %v", err)
	}
}

func TestRetireThreadTreatsMissingCwdCandidateAsAlreadyRetired(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		params := request["params"].(map[string]any)
		if request["method"] != "thread/list" || params["archived"] != false {
			return fmt.Errorf("request = %#v", request)
		}
		return writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"data": []any{}},
		})
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.RetireThread(ctx, "", "/workspace/work/example", false); err != nil {
		t.Fatal(err)
	}
}

func TestRetireThreadRecoversPersistedIdentityAmongArchivedCwdCandidates(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index := 0; index < 2; index++ {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			params := request["params"].(map[string]any)
			if request["method"] != "thread/list" || params["archived"] != (index == 1) {
				return fmt.Errorf("request %d = %#v", index, request)
			}
			data := []any{}
			if index == 1 {
				data = append(data,
					map[string]any{
						"id": "thread-old", "cwd": "/workspace/work/example", "source": "vscode",
					},
					map[string]any{
						"id": "thread-1", "cwd": "/workspace/work/example", "source": "vscode",
					},
				)
			}
			if err := writeObject(connection, map[string]any{
				"id": request["id"], "result": map[string]any{"data": data},
			}); err != nil {
				return err
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	if err := client.recordRetirementAttempt("/workspace/work/example", "thread-1"); err != nil {
		t.Fatal(err)
	}
	if err := client.recordQueueAttempt("thread-1", "queued-1", "queued message"); err != nil {
		t.Fatal(err)
	}
	if err := client.recordSendAttempt(
		"thread-1", "sent-1", "sent message", "", false,
	); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.RetireThread(ctx, "", "/workspace/work/example", false); err != nil {
		t.Fatal(err)
	}
	if attempted, err := client.queueAttempt("thread-1", "queued-1", "queued message"); err != nil || attempted {
		t.Fatalf("retired queue attempt = %v, %v", attempted, err)
	}
	if _, attempted, err := client.sendAttempt(
		"thread-1", "sent-1", "sent message", "",
	); err != nil || attempted {
		t.Fatalf("retired send attempt = %v, %v", attempted, err)
	}
	if threadID, err := client.retirementAttempt("/workspace/work/example"); err != nil || threadID != "" {
		t.Fatalf("retirement attempt = %q, %v", threadID, err)
	}
}

func TestRetireThreadFindsTheExpectedArchivedThreadAndClearsItsAttempts(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index := 0; index < 2; index++ {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			params := request["params"].(map[string]any)
			if request["method"] != "thread/list" || params["archived"] != (index == 1) {
				return fmt.Errorf("request %d = %#v", index, request)
			}
			data := []any{}
			if index == 1 {
				data = append(data,
					map[string]any{
						"id": "thread-old", "cwd": "/workspace/work/example", "source": "vscode",
					},
					map[string]any{
						"id": "thread-1", "cwd": "/workspace/work/example", "source": "vscode",
					},
				)
			}
			if err := writeObject(connection, map[string]any{
				"id": request["id"], "result": map[string]any{"data": data},
			}); err != nil {
				return err
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	if err := client.recordQueueAttempt("thread-1", "queued-1", "queued message"); err != nil {
		t.Fatal(err)
	}
	if err := client.recordSendAttempt(
		"thread-1", "sent-1", "sent message", "", false,
	); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.RetireThread(ctx, "thread-1", "/workspace/work/example", false); err != nil {
		t.Fatal(err)
	}
	if attempted, err := client.queueAttempt("thread-1", "queued-1", "queued message"); err != nil || attempted {
		t.Fatalf("retired queue attempt = %v, %v", attempted, err)
	}
	if _, attempted, err := client.sendAttempt(
		"thread-1", "sent-1", "sent message", "",
	); err != nil || attempted {
		t.Fatalf("retired send attempt = %v, %v", attempted, err)
	}
}

func TestRetireThreadArchivesAFreshThreadWithoutARollout(t *testing.T) {
	threadID := "thread-fresh"
	cwd := "/workspace/work/fresh"
	rollout := filepath.Join(t.TempDir(), "missing-rollout.jsonl")
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for index, expected := range []string{"thread/list", "thread/read", "thread/turns/list", "thread/archive"} {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			if request["method"] != expected {
				return fmt.Errorf("request %d = %#v, want %s", index, request, expected)
			}
			if index == 0 {
				if err := writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"data": []any{map[string]any{
						"id": threadID, "cwd": cwd, "source": "vscode",
					}}},
				}); err != nil {
					return err
				}
				continue
			}
			if index == 1 {
				if err := writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"thread": map[string]any{
						"id": threadID, "cwd": cwd, "source": "vscode", "status": "idle",
						"historyMode": "paginated", "preview": "", "forkedFromId": "",
						"ephemeral": false, "path": rollout, "turns": []any{},
					}},
				}); err != nil {
					return err
				}
				continue
			}
			if index == 2 {
				if err := writeObject(connection, map[string]any{
					"id": request["id"], "error": map[string]any{
						"code":    -32600,
						"message": "invalid paginated history lineage for " + threadID + ": missing source rollout",
					},
				}); err != nil {
					return err
				}
				continue
			}
			if err := writeObject(connection, map[string]any{
				"id": request["id"], "result": map[string]any{},
			}); err != nil {
				return err
			}
		}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.RetireThread(ctx, threadID, cwd, false); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverCreatingThreadRecoversCommittedStartAndSetsSessionEnvironment(t *testing.T) {
	var started atomic.Bool
	var connections atomic.Int32
	recoveredRollout := filepath.Join(t.TempDir(), "rollout.jsonl")
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		connections.Add(1)
		if err := handshake(connection); err != nil {
			return err
		}
		threadReads := 0
		for {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			switch request["method"] {
			case "thread/loaded/list":
				data := []any{}
				if started.Load() {
					data = []any{"thread-recovered"}
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"data": data, "nextCursor": nil},
				})
			case "thread/list":
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"data": []any{}},
				})
			case "thread/read":
				threadReads++
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{
						"thread": freshThreadMetadata(
							"thread-recovered", "/workspace/work/example", recoveredRollout,
						),
					},
				})
				if err == nil && threadReads == 2 {
					return nil
				}
			case "thread/start":
				params := request["params"].(map[string]any)
				config := params["config"].(map[string]any)
				policy := config["shell_environment_policy"].(map[string]any)
				environment := policy["set"].(map[string]any)
				for name, expected := range map[string]any{
					"VPSFREE_DEV_SESSION_SLUG":            "example",
					"VPSFREE_DEV_SESSION_WORKSPACE":       "/workspace",
					"VPSFREE_DEV_SESSION_WORK_DIR":        "/workspace/work/example",
					"VPSFREE_DEV_SESSION_WORKTREES_DIR":   "/workspace/worktrees/example",
					"VPSFREE_DEV_SESSION_PORTAL_BASE_URL": "https://workspace.example",
					"VPSFREE_DEV_SESSION_URL":             "https://workspace.example/example/",
					"VPSFREE_DEV_SESSION_AUTHORITY_DIR":   "/run/authority",
					"VPSFREE_DEV_SESSION_TMUX_SOCKET":     "/run/tmux.sock",
					"VPSFREE_DEV_SESSION_CODEX":           "/nix/store/codex/bin/codex",
					"VPSFREE_DEV_SESSION_CODEX_SOCKET":    "/run/codex.sock",
					"VPSFREE_DEV_SESSION_CODEX_VERSION":   protocolFixtureVersion,
				} {
					if environment[name] != expected {
						return fmt.Errorf("session environment %s = %v, want %v", name, environment[name], expected)
					}
				}
				started.Store(true)
				return connection.Close(websocket.StatusInternalError, "lost result")
			default:
				return fmt.Errorf("unexpected request %v", request["method"])
			}
			if err != nil {
				return err
			}
		}
	})
	environment := map[string]string{
		"VPSFREE_DEV_SESSION_SLUG": "example", "VPSFREE_DEV_SESSION_WORKSPACE": "/workspace",
		"VPSFREE_DEV_SESSION_WORK_DIR":        "/workspace/work/example",
		"VPSFREE_DEV_SESSION_WORKTREES_DIR":   "/workspace/worktrees/example",
		"VPSFREE_DEV_SESSION_PORTAL_BASE_URL": "https://workspace.example",
		"VPSFREE_DEV_SESSION_URL":             "https://workspace.example/example/",
		"VPSFREE_DEV_SESSION_AUTHORITY_DIR":   "/run/authority",
		"VPSFREE_DEV_SESSION_TMUX_SOCKET":     "/run/tmux.sock",
		"VPSFREE_DEV_SESSION_CODEX":           "/nix/store/codex/bin/codex",
		"VPSFREE_DEV_SESSION_CODEX_SOCKET":    "/run/codex.sock",
		"VPSFREE_DEV_SESSION_CODEX_VERSION":   protocolFixtureVersion,
	}
	first := New(socket)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := first.RecoverCreatingThread(ctx, "", "/workspace/work/example", environment); err == nil {
		t.Fatal("lost thread/start response unexpectedly succeeded")
	}
	first.Close()
	second := New(socket)
	defer second.Close()
	id, err := second.RecoverCreatingThread(ctx, "", "/workspace/work/example", environment)
	if err != nil {
		t.Fatal(err)
	}
	if id != "thread-recovered" || connections.Load() != 2 {
		t.Fatalf("recovered id = %q across %d connections", id, connections.Load())
	}
}

func TestRecoverCreatingThreadResumesPersistedOwnerWithRuntimeConfiguration(t *testing.T) {
	rollout := filepath.Join(t.TempDir(), "rollout.jsonl")
	if err := os.WriteFile(rollout, []byte("materialized\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	environment := map[string]string{
		"VPSFREE_DEV_SESSION_SLUG":            "example",
		"VPSFREE_DEV_SESSION_WORKSPACE":       "/workspace",
		"VPSFREE_DEV_SESSION_WORK_DIR":        "/workspace/work/example",
		"VPSFREE_DEV_SESSION_WORKTREES_DIR":   "/workspace/worktrees/example",
		"VPSFREE_DEV_SESSION_PORTAL_BASE_URL": "https://workspace.example",
		"VPSFREE_DEV_SESSION_URL":             "https://workspace.example/example/",
	}
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil || request["method"] != "thread/loaded/list" {
			return fmt.Errorf("expected persisted thread/loaded/list: %v", err)
		}
		if err := writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"data": []any{}, "nextCursor": nil},
		}); err != nil {
			return err
		}
		request, err = readObject(connection)
		if err != nil || request["method"] != "thread/list" {
			return fmt.Errorf("expected persisted thread/list: %v", err)
		}
		if err := writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"data": []any{map[string]any{
				"id": "thread-original", "cwd": "/workspace/work/example",
			}}},
		}); err != nil {
			return err
		}
		request, err = readObject(connection)
		if err != nil || request["method"] != "thread/read" {
			return fmt.Errorf("expected persisted thread/read: %v", err)
		}
		if err := writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{
				"thread": freshThreadMetadata("thread-original", "/workspace/work/example", rollout),
			},
		}); err != nil {
			return err
		}
		request, err = readObject(connection)
		if err != nil || request["method"] != "thread/resume" {
			return fmt.Errorf("expected persisted thread/resume: %v", err)
		}
		params := request["params"].(map[string]any)
		if params["threadId"] != "thread-original" ||
			params["cwd"] != "/workspace/work/example" || params["excludeTurns"] != true {
			return fmt.Errorf("invalid persisted resume params: %#v", params)
		}
		config := params["config"].(map[string]any)
		if _, ok := config["model"]; ok {
			return fmt.Errorf("persisted thread model was overwritten: %#v", config)
		}
		if _, ok := config["model_reasoning_effort"]; ok {
			return fmt.Errorf("persisted thread reasoning was overwritten: %#v", config)
		}
		policy := config["shell_environment_policy"].(map[string]any)
		set := policy["set"].(map[string]any)
		if set["VPSFREE_DEV_SESSION_PORTAL_BASE_URL"] != "https://workspace.example" {
			return fmt.Errorf("persisted thread runtime environment was not refreshed: %#v", set)
		}
		return writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"thread": map[string]any{
				"id": "thread-original", "cwd": "/workspace/work/example",
			}},
		})
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resolverCalled := false
	id, err := client.RecoverCreatingThreadWithSettingsResolver(
		ctx, "thread-original", "/workspace/work/example", environment,
		func() (ThreadSettings, error) {
			resolverCalled = true
			return ThreadSettings{}, errors.New("the old model is no longer in the catalog")
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if id != "thread-original" {
		t.Fatalf("persisted thread id = %q", id)
	}
	if resolverCalled {
		t.Fatal("materialized recovery resolved replacement settings")
	}
}

func TestRecoverCreatingThreadReplacesPersistedOwnerMissingAfterRestart(t *testing.T) {
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil || request["method"] != "thread/loaded/list" {
			return fmt.Errorf("expected restart thread/loaded/list: %v", err)
		}
		if err := writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"data": []any{}, "nextCursor": nil},
		}); err != nil {
			return err
		}
		request, err = readObject(connection)
		if err != nil || request["method"] != "thread/list" {
			return fmt.Errorf("expected restart thread/list: %v", err)
		}
		if err := writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"data": []any{}},
		}); err != nil {
			return err
		}
		request, err = readObject(connection)
		if err != nil || request["method"] != "thread/start" {
			return fmt.Errorf("expected replacement thread/start: %v", err)
		}
		return writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"thread": map[string]any{
				"id": "thread-replacement", "cwd": "/workspace/work/example",
			}},
		})
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, err := client.RecoverCreatingThread(
		ctx, "thread-vanished", "/workspace/work/example", map[string]string{
			"VPSFREE_DEV_SESSION_WORKSPACE": "/workspace",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if id != "thread-replacement" {
		t.Fatalf("replacement thread id = %q", id)
	}
}

func TestRecoverCreatingThreadRejectsDifferentMaterializedCandidate(t *testing.T) {
	rollout := filepath.Join(t.TempDir(), "rollout.jsonl")
	if err := os.WriteFile(rollout, []byte("materialized\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil || request["method"] != "thread/loaded/list" {
			return fmt.Errorf("expected thread/loaded/list: %v", err)
		}
		if err := writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"data": []any{}, "nextCursor": nil},
		}); err != nil {
			return err
		}
		request, err = readObject(connection)
		if err != nil || request["method"] != "thread/list" {
			return fmt.Errorf("expected thread/list: %v", err)
		}
		if err := writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{"data": []any{map[string]any{
				"id": "thread-unrelated", "cwd": "/workspace/work/example",
			}}},
		}); err != nil {
			return err
		}
		request, err = readObject(connection)
		if err != nil || request["method"] != "thread/read" {
			return fmt.Errorf("expected thread/read: %v", err)
		}
		return writeObject(connection, map[string]any{
			"id": request["id"], "result": map[string]any{
				"thread": freshThreadMetadata("thread-unrelated", "/workspace/work/example", rollout),
			},
		})
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.RecoverCreatingThread(
		ctx,
		"thread-vanished",
		"/workspace/work/example",
		map[string]string{"VPSFREE_DEV_SESSION_WORKSPACE": "/workspace"},
	)
	if err == nil || !strings.Contains(err.Error(), "different materialized") {
		t.Fatalf("different materialized candidate result = %v", err)
	}
}

func TestWatchedThreadIsResumedAfterReconnect(t *testing.T) {
	var connectionCount atomic.Int32
	resumed := make(chan int32, 2)
	handlerErrors := make(chan error, 2)
	handler := func(connection *websocket.Conn) error {
		generation := connectionCount.Add(1)
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/resume" {
			return fmt.Errorf("watched thread was not resumed: got %v", request["method"])
		}
		if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{}}); err != nil {
			return err
		}
		resumed <- generation
		if err := writeObject(connection, map[string]any{
			"method": "item/agentMessage/delta", "params": map[string]any{"threadId": "thread-1"},
		}); err != nil {
			return err
		}
		if generation == 1 {
			return connection.Close(websocket.StatusInternalError, "force reconnect")
		}
		_, _, _ = connection.Read(context.Background())
		return nil
	}
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		err := handler(connection)
		if err != nil {
			handlerErrors <- err
		}
		return err
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	events, unsubscribe, err := client.Subscribe(ctx, "thread-1")
	if err != nil {
		t.Fatal(err)
	}
	defer unsubscribe()
	select {
	case generation := <-resumed:
		if generation != 1 {
			t.Fatalf("first resume generation = %d", generation)
		}
	case err := <-handlerErrors:
		t.Fatalf("first connection failed: %v", err)
	case <-ctx.Done():
		t.Fatal("first watched thread was not resumed")
	}
	select {
	case <-events:
	case <-ctx.Done():
		t.Fatal("first watched event was not broadcast")
	}
	waitFor(t, func() bool {
		client.connectionMu.Lock()
		defer client.connectionMu.Unlock()
		return client.connection == nil
	})
	if err := client.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case generation := <-resumed:
		if generation != 2 {
			t.Fatalf("restored resume generation = %d", generation)
		}
	case err := <-handlerErrors:
		t.Fatalf("reconnected App Server failed: %v", err)
	case <-ctx.Done():
		t.Fatal("reconnected watched thread was not resumed")
	}
	select {
	case <-events:
	case <-ctx.Done():
		t.Fatal("reconnected watched event was not broadcast")
	}
}

func TestStaleWatchedThreadDoesNotBreakHealthyThreadReconnect(t *testing.T) {
	var connectionCount atomic.Int32
	restoredHealthy := make(chan struct{}, 1)
	restoredStale := make(chan struct{}, 1)
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		generation := connectionCount.Add(1)
		if err := handshake(connection); err != nil {
			return err
		}
		requests := make([]map[string]any, 0, 2)
		for index := 0; index < 2; index++ {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			if request["method"] != "thread/resume" {
				return errors.New("expected thread/resume")
			}
			requests = append(requests, request)
			if generation == 1 {
				if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{}}); err != nil {
					return err
				}
			}
		}
		if generation == 2 {
			// Wait for both restore requests before answering either one. A stale
			// thread must not serialize or prevent restoration of another thread.
			for _, request := range requests {
				params := request["params"].(map[string]any)
				threadID := params["threadId"]
				if threadID == "thread-stale" {
					if err := writeObject(connection, map[string]any{
						"id": request["id"], "error": map[string]any{"code": -32001, "message": "thread not found"},
					}); err != nil {
						return err
					}
					restoredStale <- struct{}{}
				} else {
					if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{}}); err != nil {
						return err
					}
					if threadID == "thread-healthy" {
						restoredHealthy <- struct{}{}
					}
				}
			}
		}
		if generation == 1 {
			return connection.Close(websocket.StatusInternalError, "force reconnect")
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		return writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{"data": []any{}}})
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, unsubscribeStale, err := client.Subscribe(ctx, "thread-stale")
	if err != nil {
		t.Fatal(err)
	}
	defer unsubscribeStale()
	_, unsubscribeHealthy, err := client.Subscribe(ctx, "thread-healthy")
	if err != nil {
		t.Fatal(err)
	}
	defer unsubscribeHealthy()
	waitFor(t, func() bool {
		client.connectionMu.Lock()
		defer client.connectionMu.Unlock()
		return client.connection == nil
	})
	if err := client.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-restoredHealthy:
	case <-ctx.Done():
		t.Fatal("healthy watched thread was not restored")
	}
	select {
	case <-restoredStale:
	case <-ctx.Done():
		t.Fatal("stale watched thread was not attempted")
	}
	waitFor(t, func() bool { return len(client.Prompts("thread-stale")) == 1 })
	if client.Prompts("thread-stale")[0].Kind != "connection" {
		t.Fatalf("stale thread notice = %#v", client.Prompts("thread-stale"))
	}
	var result map[string]any
	if err := client.Request(ctx, "thread/list", map[string]any{}, &result); err != nil {
		t.Fatalf("healthy connection was lost: %v", err)
	}
}

func TestLastSubscriberUnsubscribesFromThread(t *testing.T) {
	unsubscribed := make(chan struct{}, 1)
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/resume" {
			return errors.New("expected thread/resume")
		}
		if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{}}); err != nil {
			return err
		}
		request, err = readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/unsubscribe" {
			return errors.New("expected thread/unsubscribe")
		}
		if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{}}); err != nil {
			return err
		}
		unsubscribed <- struct{}{}
		return nil
	})
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, unsubscribe, err := client.Subscribe(ctx, "thread-1")
	if err != nil {
		t.Fatal(err)
	}
	client.connectionMu.Lock()
	generation := client.generation
	client.connectionMu.Unlock()
	client.cacheThreadSettings("thread-1", ThreadSettings{
		Model: "stale-model", ReasoningEffort: "medium", CollaborationMode: "default",
	}, generation)
	if _, _, ok := client.cachedSettings("thread-1"); !ok {
		t.Fatal("thread settings were not cached")
	}
	unsubscribe()
	if _, _, ok := client.cachedSettings("thread-1"); ok {
		t.Fatal("last subscriber left stale thread settings cached")
	}
	select {
	case <-unsubscribed:
	case <-ctx.Done():
		t.Fatal("thread was not unsubscribed")
	}
}

func TestLastSubscriberInvalidatesSettingsAfterAConcurrentWatchTransition(t *testing.T) {
	client := New(filepath.Join(t.TempDir(), "missing.sock"))
	defer client.Close()
	client.watched["thread-1"] = 1
	client.cacheThreadSettings("thread-1", ThreadSettings{
		Model: "stale-model", ReasoningEffort: "medium", CollaborationMode: "default",
	}, 0)
	transition := client.watchLock("thread-1")
	transition.Lock()
	removed := make(chan struct{})
	go func() {
		client.removeWatch("thread-1")
		close(removed)
	}()
	client.cacheThreadSettings("thread-1", ThreadSettings{
		Model: "new-model", ReasoningEffort: "xhigh", CollaborationMode: "plan",
	}, 0)
	if _, _, ok := client.cachedSettings("thread-1"); !ok {
		t.Fatal("concurrent transition unexpectedly invalidated settings before it completed")
	}
	transition.Unlock()
	select {
	case <-removed:
	case <-time.After(5 * time.Second):
		t.Fatal("last subscriber removal did not complete")
	}
	if _, _, ok := client.cachedSettings("thread-1"); ok {
		t.Fatal("last subscriber left settings cached after the watch transition")
	}
}

func TestReplacementSubscriberPreventsUnsubscribeDuringTransition(t *testing.T) {
	releaseServer := make(chan struct{})
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/resume" {
			return fmt.Errorf("expected thread/resume, got %v", request["method"])
		}
		if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{}}); err != nil {
			return err
		}
		<-releaseServer
		return nil
	})
	defer closeTestChannel(releaseServer)
	client := New(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, unsubscribe, err := client.Subscribe(ctx, "thread-1")
	if err != nil {
		t.Fatal(err)
	}
	transition := client.watchLock("thread-1")
	transition.Lock()
	removed := make(chan struct{})
	go func() {
		unsubscribe()
		close(removed)
	}()
	replacement := make(chan error, 1)
	go func() {
		_, _, err := client.Subscribe(ctx, "thread-1")
		replacement <- err
	}()
	waitFor(t, func() bool {
		client.watchedMu.Lock()
		defer client.watchedMu.Unlock()
		return client.watched["thread-1"] == 2
	})
	transition.Unlock()
	select {
	case <-removed:
	case <-ctx.Done():
		t.Fatal("original subscriber was not removed")
	}
	if err := <-replacement; err != nil {
		t.Fatal(err)
	}
	client.watchedMu.Lock()
	watchers := client.watched["thread-1"]
	generation := client.watchedGeneration["thread-1"]
	client.watchedMu.Unlock()
	if watchers != 1 || generation == 0 {
		t.Fatalf("replacement subscription = %d watchers at generation %d", watchers, generation)
	}
}

func handshake(connection *websocket.Conn) error {
	request, err := readObject(connection)
	if err != nil {
		return err
	}
	params, _ := request["params"].(map[string]any)
	capabilities, _ := params["capabilities"].(map[string]any)
	if capabilities["experimentalApi"] != true {
		return errors.New("initialize did not enable the experimental API")
	}
	if err := writeObject(connection, map[string]any{
		"id": request["id"], "result": map[string]any{"userAgent": "codex-cli/99.0.0"},
	}); err != nil {
		return err
	}
	_, err = readObject(connection)
	return err
}

func serveUnixWebsocket(t *testing.T, handler func(*websocket.Conn) error) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "wpc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(directory) })
	socket := filepath.Join(directory, "app-server.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	errorsChannel := make(chan error, 8)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, err := websocket.Accept(w, r, nil)
		if err != nil {
			errorsChannel <- err
			return
		}
		connection.SetReadLimit(readLimit)
		go func() {
			if err := handler(connection); err != nil {
				errorsChannel <- err
			}
		}()
	})}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errorsChannel <- err
		}
	}()
	t.Cleanup(func() {
		server.Close()
		select {
		case err := <-errorsChannel:
			t.Errorf("fake App Server: %v", err)
		default:
		}
	})
	return socket
}

func readObject(connection *websocket.Conn) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := connection.Read(ctx)
	if err != nil {
		return nil, err
	}
	var value map[string]any
	err = json.Unmarshal(data, &value)
	return value, err
}

func writeObject(connection *websocket.Conn, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return connection.Write(ctx, websocket.MessageText, data)
}

func closeTestChannel(channel chan struct{}) {
	select {
	case <-channel:
	default:
		close(channel)
	}
}

func settingsRollout(t *testing.T, mode string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	entry := map[string]any{
		"type": "turn_context", "payload": map[string]any{
			"collaboration_mode": map[string]any{"mode": mode},
		},
	}
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func freshThreadMetadata(threadID, cwd string, path any) map[string]any {
	return map[string]any{
		"id": threadID, "cwd": cwd, "path": path, "preview": "", "source": "vscode",
		"ephemeral": false, "historyMode": "paginated", "status": map[string]any{"type": "idle"},
		"turns": []any{},
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not met")
		}
		time.Sleep(time.Millisecond)
	}
}

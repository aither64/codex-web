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

func TestResolveNewThreadSettingsDefaultsToMax(t *testing.T) {
	models := []Model{
		{
			Model: "default", DisplayName: "Default", IsDefault: true,
			DefaultReasoningEffort: "medium",
			SupportedReasoningEfforts: []ReasoningEffortOption{
				{ReasoningEffort: "medium"}, {ReasoningEffort: "max"},
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
	if err != nil || settings.Model != "default" || settings.ReasoningEffort != "max" {
		t.Fatalf("default settings = %#v, %v", settings, err)
	}
	settings, err = ResolveNewThreadSettings(models, ThreadSettings{Model: "bounded"})
	if err != nil || settings.Model != "bounded" || settings.ReasoningEffort != "high" {
		t.Fatalf("bounded settings = %#v, %v", settings, err)
	}
	settings, err = ResolveNewThreadSettings(models, ThreadSettings{
		Model: "default", ReasoningEffort: "medium",
	})
	if err != nil || settings.ReasoningEffort != "medium" {
		t.Fatalf("explicit settings = %#v, %v", settings, err)
	}
}

func TestResolveNewThreadSettingsRejectsInvalidCatalogAndSelections(t *testing.T) {
	defaultModel := Model{
		Model: "default", DisplayName: "Default", IsDefault: true,
		DefaultReasoningEffort:    "medium",
		SupportedReasoningEfforts: []ReasoningEffortOption{{ReasoningEffort: "medium"}},
	}
	for _, testCase := range []struct {
		name      string
		models    []Model
		requested ThreadSettings
		message   string
	}{
		{"no default", []Model{{Model: "other"}}, ThreadSettings{}, "no default"},
		{"duplicate default", []Model{defaultModel, defaultModel}, ThreadSettings{}, "more than one default"},
		{"default lacks max", []Model{defaultModel}, ThreadSettings{}, "required default reasoning effort"},
		{"missing model", []Model{defaultModel}, ThreadSettings{Model: "missing"}, "not available"},
		{"unsupported effort", []Model{defaultModel}, ThreadSettings{Model: "default", ReasoningEffort: "max"}, "not available"},
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
			case 1, 3:
				if request["method"] != "thread/turns/list" || params["threadId"] != "thread-source" {
					return fmt.Errorf("expected source idle check, got %#v", request)
				}
				err = writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"data": []any{}},
				})
			case 2:
				if request["method"] != "thread/resume" || params["model"] != "gpt-test" ||
					params["cwd"] != "/workspace/work/source" {
					return fmt.Errorf("invalid settings request: %#v", request)
				}
				config := params["config"].(map[string]any)
				if config["model_reasoning_effort"] != "high" {
					return fmt.Errorf("invalid reasoning setting: %#v", config)
				}
				err = writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{
					"thread": map[string]any{
						"id": "thread-source", "cwd": "/workspace/work/source",
						"model": "gpt-test", "reasoningEffort": "high",
					},
				}})
			case 4:
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
	settings := ThreadSettings{Model: "gpt-test", ReasoningEffort: "high"}
	if _, err := client.UpdateThreadSettings(ctx, "thread-source", "/workspace/work/source", settings); err != nil {
		t.Fatal(err)
	}
	id, err := client.ForkThread(ctx, "thread-source", "/workspace/work/fork", map[string]string{
		"VPSFREE_DEV_SESSION_WORKSPACE": "/workspace",
	}, settings)
	if err != nil || id != "thread-fork" {
		t.Fatalf("fork = %q, %v", id, err)
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
				return client.Send(ctx, "thread-1", "follow-up")
			},
		},
		{
			name: "thread items",
			run: func(ctx context.Context, client *Client) error {
				_, err := client.threadItems(ctx, "thread-1")
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
				return writeObject(connection, map[string]any{
					"id": request["id"], "result": map[string]any{"data": []any{
						map[string]any{"id": "turn-1", "status": testCase.status},
					}},
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
			}}},
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
	rollout := filepath.Join(t.TempDir(), "rollout.jsonl")
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		initialExists := false
		for requestNumber := 0; requestNumber < 5; requestNumber++ {
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
				if err := os.WriteFile(rollout, []byte("materialized\n"), 0o600); err != nil {
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
		{"missing user request", []any{map[string]any{"type": "agentMessage", "text": "reply"}}, false, "without an initial user request"},
		{"missing initial turn", nil, true, "has no initial turn"},
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
	id, err := client.RecoverCreatingThread(
		ctx, "thread-original", "/workspace/work/example", environment,
	)
	if err != nil {
		t.Fatal(err)
	}
	if id != "thread-original" {
		t.Fatalf("persisted thread id = %q", id)
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
	unsubscribe()
	select {
	case <-unsubscribed:
	case <-ctx.Done():
		t.Fatal("thread was not unsubscribed")
	}
}

func TestStaleUnsubscribeDoesNotDetachReplacementSubscriber(t *testing.T) {
	secondResume := make(chan struct{}, 1)
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for count := 0; count < 2; count++ {
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
		}
		secondResume <- struct{}{}
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
	transition := client.watchLock("thread-1")
	transition.Lock()
	unsubscribe()
	replacement := make(chan error, 1)
	go func() {
		_, _, err := client.Subscribe(ctx, "thread-1")
		replacement <- err
	}()
	waitFor(t, func() bool {
		client.watchedMu.Lock()
		defer client.watchedMu.Unlock()
		return client.watched["thread-1"] == 1
	})
	transition.Unlock()
	if err := <-replacement; err != nil {
		t.Fatal(err)
	}
	select {
	case <-secondResume:
	case <-ctx.Done():
		t.Fatal("replacement subscriber did not resume")
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

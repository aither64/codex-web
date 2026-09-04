package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const protocolFixtureVersion = "0.152.1"

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

func TestListResponsesFailClosedWhenDataIsMissing(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *Client) error
	}{
		{
			name: "thread list",
			run: func(ctx context.Context, client *Client) error {
				_, err := client.EnsureThread(ctx, "", "/workspace/work/example", nil)
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
				return client.EnsureInitialMessage(ctx, "thread-1", "initial request")
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
							"id": "thread-1", "cwd": "/workspace/work/example",
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
			case "thread/turns/list":
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
	if err := client.EnsureInitialMessage(ctx, "thread-1", "initial goal"); err != nil {
		t.Fatal(err)
	}
	if err := client.EnsureInitialMessage(ctx, "thread-1", "initial goal"); err != nil {
		t.Fatal(err)
	}
	if turnStarted.Load() != 1 {
		t.Fatalf("initial turn started %d times", turnStarted.Load())
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

func TestEnsureThreadRecoversCommittedStartAndSetsSessionEnvironment(t *testing.T) {
	var started atomic.Bool
	var connections atomic.Int32
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		connectionNumber := connections.Add(1)
		if err := handshake(connection); err != nil {
			return err
		}
		request, err := readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/list" {
			return errors.New("expected thread/list reconciliation")
		}
		data := []any{}
		if started.Load() {
			data = []any{map[string]any{"id": "thread-recovered", "cwd": "/workspace/work/example"}}
		}
		if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{"data": data}}); err != nil {
			return err
		}
		if started.Load() {
			request, err = readObject(connection)
			if err != nil || request["method"] != "thread/resume" {
				return fmt.Errorf("expected recovered thread/resume: %v", err)
			}
			params := request["params"].(map[string]any)
			if params["threadId"] != "thread-recovered" || params["cwd"] != "/workspace/work/example" {
				return fmt.Errorf("invalid recovered resume params: %#v", params)
			}
			return writeObject(connection, map[string]any{
				"id": request["id"], "result": map[string]any{"thread": map[string]any{
					"id": "thread-recovered", "cwd": "/workspace/work/example",
				}},
			})
		}
		request, err = readObject(connection)
		if err != nil {
			return err
		}
		if request["method"] != "thread/start" {
			return errors.New("expected thread/start")
		}
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
		if connectionNumber == 1 {
			return connection.Close(websocket.StatusInternalError, "lost result")
		}
		return nil
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
	if _, err := first.EnsureThread(ctx, "", "/workspace/work/example", environment); err == nil {
		t.Fatal("lost thread/start response unexpectedly succeeded")
	}
	first.Close()
	second := New(socket)
	defer second.Close()
	id, err := second.EnsureThread(ctx, "", "/workspace/work/example", environment)
	if err != nil {
		t.Fatal(err)
	}
	if id != "thread-recovered" || connections.Load() != 2 {
		t.Fatalf("recovered id = %q across %d connections", id, connections.Load())
	}
}

func TestEnsureThreadResumesPersistedOwnerWithRuntimeConfiguration(t *testing.T) {
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
	id, err := client.EnsureThread(
		ctx, "thread-original", "/workspace/work/example", environment,
	)
	if err != nil {
		t.Fatal(err)
	}
	if id != "thread-original" {
		t.Fatalf("persisted thread id = %q", id)
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
	socket := filepath.Join(t.TempDir(), "app-server.sock")
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

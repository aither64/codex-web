package codex

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func questionRequest(connection *websocket.Conn, generation uint64) PendingRequest {
	return PendingRequest{ID: `"question"`, Method: "item/tool/requestUserInput",
		connection: connection, generation: generation,
		Params: json.RawMessage(`{"threadId":"thread-1","turnId":"turn-1","itemId":"item-1","isBlocking":false,"questions":[{"id":"choice","header":"Choice","question":"Choose","options":[{"label":"First","description":"First option"}]}]}`)}
}

func TestRetiredReaderCannotPublishOrOverwritePrompts(t *testing.T) {
	client := newTestClient("")
	old, current := &websocket.Conn{}, &websocket.Conn{}
	client.connection, client.generation = current, 2
	stale := questionRequest(old, 1)
	if client.admitPrompt(&stale) || len(client.Prompts("thread-1")) != 0 {
		t.Fatal("retired reader published a question")
	}
	fresh := questionRequest(current, 2)
	if !client.admitPrompt(&fresh) || fresh.token == "" {
		t.Fatal("current reader did not publish a bound question")
	}
	if client.admitPrompt(&stale) || client.Prompts("thread-1")[0].Token != fresh.token {
		t.Fatal("retired reader overwrote the current question")
	}
	duplicate := questionRequest(current, 2)
	if client.admitPrompt(&duplicate) || client.Prompts("thread-1")[0].Token != fresh.token {
		t.Fatal("duplicate delivery replaced the current offer")
	}
	client.connection, client.generation = old, 3
	if len(client.Prompts("thread-1")) != 0 {
		t.Fatal("stale offer remained visible after connection replacement")
	}
	_, _, err := client.claim(fresh.ID, "thread-1", fresh.token)
	var responseError *PromptResponseError
	if !errors.As(err, &responseError) || !responseError.NotSent {
		t.Fatalf("stale claim was not rejected before sending: %v", err)
	}
}

func TestReissuedRequestRejectsOldTokensAndAutoResolutionTimers(t *testing.T) {
	client := newTestClient("")
	connection := &websocket.Conn{}
	client.connection, client.generation = connection, 1
	first := questionRequest(connection, 1)
	client.admitPrompt(&first)
	delete(client.requests, first.ID)
	second := questionRequest(connection, 1)
	client.admitPrompt(&second)
	if first.token == second.token {
		t.Fatal("reused protocol ID reused its offer token")
	}
	if _, ok := client.claimAutoResolvableUserInput(first.ID, "thread-1", first.token); ok {
		t.Fatal("old timer claimed a reissued question")
	}
	if err := client.snoozeUserInput(first.ID, "thread-1", first.token); err == nil {
		t.Fatal("old token snoozed a reissued question")
	}
	if _, _, err := client.claim(first.ID, "thread-1", first.token); err == nil {
		t.Fatal("old token claimed a reissued question")
	}
	if _, ok := client.claimAutoResolvableUserInput(second.ID, "thread-1", second.token); !ok {
		t.Fatal("current timer could not claim its question")
	}
}

func TestBoundAnswersValidateBeforeSendingAndRetireTheOffer(t *testing.T) {
	responses := make(chan map[string]any, 1)
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request := questionRequest(nil, 0)
		var params any
		_ = json.Unmarshal(request.Params, &params)
		if err := writeObject(connection, map[string]any{"id": "question", "method": request.Method, "params": params}); err != nil {
			return err
		}
		response, err := readObject(connection)
		if err == nil {
			responses <- response
		}
		return err
	})
	client := newTestClient(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(client.Prompts("thread-1")) == 1 })
	prompt := client.Prompts("thread-1")[0]
	action := PromptResponse{ID: prompt.ID, Token: prompt.Token, Answers: map[string]map[string][]string{"choice": {"answers": {"First"}}}}
	for _, token := range []string{"", "obsolete"} {
		invalid := action
		invalid.Token = token
		if err := client.RespondPrompt(ctx, "thread-1", invalid); err == nil {
			t.Fatal("invalid token was accepted")
		}
	}
	invalid := action
	invalid.Answers = map[string]map[string][]string{"choice": {"answers": {"not offered"}}}
	var rejected *PromptResponseError
	if err := client.RespondPrompt(ctx, "thread-1", invalid); !errors.As(err, &rejected) || rejected.Code != "invalid_response" || !rejected.NotSent {
		t.Fatalf("invalid answer classification: %v", err)
	}
	if err := client.RespondPrompt(ctx, "thread-1", action); err != nil {
		t.Fatal(err)
	}
	select {
	case response := <-responses:
		if response["id"] != "question" || response["result"] == nil {
			t.Fatalf("response: %#v", response)
		}
	case <-ctx.Done():
		t.Fatal("answer was not delivered")
	}
	if err := client.RespondPrompt(ctx, "thread-1", action); err == nil {
		t.Fatal("completed offer accepted twice")
	}
}

func TestPromptResponseRejectsConnectionChangeBeforeSending(t *testing.T) {
	client := newTestClient("")
	old, current := &websocket.Conn{}, &websocket.Conn{}
	client.connection, client.generation = current, 2
	err := client.finishResponse(context.Background(), questionRequest(old, 1), map[string]any{"answers": map[string]any{}})
	var rejected *PromptResponseError
	if !errors.As(err, &rejected) || rejected.Code != "prompt_transport" || !rejected.NotSent {
		t.Fatalf("retired connection result: %v", err)
	}
}

func TestPromptWriteFailureDoesNotClaimTheAnswerWasUnsent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		_, _, _ = connection.Read(r.Context())
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	connection.CloseNow()
	client := newTestClient("")
	client.connection, client.generation = connection, 1
	request := questionRequest(connection, 1)
	client.admitPrompt(&request)
	request.claimed = true
	client.requests[request.ID] = request
	err = client.finishResponse(ctx, request, map[string]any{"answers": map[string]any{}})
	var failed *PromptResponseError
	if !errors.As(err, &failed) || failed.Code != "prompt_transport" || failed.NotSent {
		t.Fatalf("write failure classification: %v", err)
	}
}

func TestReissuedOfferWhileAnswerWaitsForWrite(t *testing.T) {
	replace := make(chan struct{})
	responses := make(chan map[string]any, 1)
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		request := questionRequest(nil, 0)
		var params map[string]any
		_ = json.Unmarshal(request.Params, &params)
		if err := writeObject(connection, map[string]any{"id": "question", "method": request.Method, "params": params}); err != nil {
			return err
		}
		<-replace
		if err := writeObject(connection, map[string]any{"method": "serverRequest/resolved", "params": map[string]any{"requestId": "question"}}); err != nil {
			return err
		}
		params["questions"] = []any{map[string]any{"id": "choice", "header": "Replacement", "question": "A different question", "options": []any{map[string]any{"label": "Second", "description": "Replacement only"}}}}
		if err := writeObject(connection, map[string]any{"id": "question", "method": request.Method, "params": params}); err != nil {
			return err
		}
		response, err := readObject(connection)
		if err == nil {
			responses <- response
		}
		return err
	})
	client := newTestClient(socket)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(client.Prompts("thread-1")) == 1 })
	first := client.Prompts("thread-1")[0]
	client.writeMu.Lock()
	locked := true
	defer func() {
		if locked {
			client.writeMu.Unlock()
		}
	}()
	result := make(chan error, 1)
	go func() {
		result <- client.RespondPrompt(ctx, "thread-1", PromptResponse{ID: first.ID, Token: first.Token, Answers: map[string]map[string][]string{"choice": {"answers": {"First"}}}})
	}()
	waitFor(t, func() bool {
		client.pendingMu.Lock()
		defer client.pendingMu.Unlock()
		return client.requests[first.ID].claimed
	})
	close(replace)
	waitFor(t, func() bool { p := client.Prompts("thread-1"); return len(p) == 1 && p[0].Token != first.Token })
	replacement := client.Prompts("thread-1")[0]
	client.writeMu.Unlock()
	locked = false
	var rejected *PromptResponseError
	if err := <-result; !errors.As(err, &rejected) || !rejected.NotSent || rejected.Code != "prompt_changed" {
		t.Fatalf("expired claim classification: %v", err)
	}
	if err := client.RespondPrompt(ctx, "thread-1", PromptResponse{ID: replacement.ID, Token: replacement.Token,
		Answers: map[string]map[string][]string{"choice": {"answers": {"Second"}}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case response := <-responses:
		encoded, _ := json.Marshal(response)
		if strings.Contains(string(encoded), "First") || !strings.Contains(string(encoded), "Second") {
			t.Fatalf("response: %s", encoded)
		}
	case <-ctx.Done():
		t.Fatal("replacement answer was not delivered")
	}
}

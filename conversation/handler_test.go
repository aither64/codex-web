package conversation

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aither64/codex-web/codex"
)

type fakeClient struct {
	verifiedThread string
	verifiedCwd    string
	sentThread     string
	sentText       string
	sendErr        error
	verifyErr      error
	retryChecked   bool
	blockSend      bool
	deletedQueueID string
	startedQueueID string
}

type sharedOpaqueIDCase struct {
	Name        string `json:"name"`
	Value       string `json:"value"`
	RepeatValue string `json:"repeatValue"`
	RepeatCount int    `json:"repeatCount"`
	GoBytesHex  string `json:"goBytesHex"`
	Valid       bool   `json:"valid"`
	Encoded     string `json:"encoded"`
}

type sharedPathContract struct {
	Schema           int                  `json:"schema"`
	BasePathAlphabet string               `json:"basePathAlphabet"`
	OpaqueIDCases    []sharedOpaqueIDCase `json:"opaqueIdCases"`
}

func loadSharedPathContract(t *testing.T) sharedPathContract {
	t.Helper()
	data, err := os.ReadFile("testdata/path_contract.json")
	if err != nil {
		t.Fatal(err)
	}
	var contract sharedPathContract
	if err := json.Unmarshal(data, &contract); err != nil {
		t.Fatal(err)
	}
	if contract.Schema != 1 || contract.BasePathAlphabet == "" || len(contract.OpaqueIDCases) == 0 {
		t.Fatalf("invalid shared path contract: %#v", contract)
	}
	return contract
}

func sharedContractID(t *testing.T, testCase sharedOpaqueIDCase) string {
	t.Helper()
	if testCase.GoBytesHex != "" {
		value, err := hex.DecodeString(testCase.GoBytesHex)
		if err != nil {
			t.Fatal(err)
		}
		return string(value)
	}
	if testCase.RepeatCount != 0 {
		return strings.Repeat(testCase.RepeatValue, testCase.RepeatCount)
	}
	return testCase.Value
}

func (client *fakeClient) VerifyThread(_ context.Context, thread, cwd string) error {
	client.verifiedThread = thread
	client.verifiedCwd = cwd
	return client.verifyErr
}

func (client *fakeClient) ReadThread(_ context.Context, thread string) (codex.Transcript, error) {
	return codex.Transcript{ThreadID: thread, Entries: []codex.TranscriptEntry{}}, nil
}
func (client *fakeClient) PromptsWithItems(context.Context, string) ([]codex.Prompt, error) {
	return []codex.Prompt{}, nil
}
func (client *fakeClient) ListQueue(context.Context, string) ([]codex.QueueEntry, error) {
	return []codex.QueueEntry{}, nil
}
func (client *fakeClient) Send(
	ctx context.Context, thread, text, clientID, _ string,
) (codex.SendReceipt, error) {
	client.sentThread = thread
	client.sentText = text
	if client.blockSend {
		<-ctx.Done()
		return codex.SendReceipt{}, ctx.Err()
	}
	return codex.SendReceipt{TurnID: "turn-1", ClientUserMessageID: clientID}, client.sendErr
}
func (client *fakeClient) SendAttempted(
	context.Context, string, string, string, string,
) (bool, error) {
	client.retryChecked = true
	return true, nil
}
func (client *fakeClient) AcknowledgeSends(
	context.Context, string, []codex.SendAcknowledgement,
) ([]string, error) {
	return []string{}, nil
}
func (client *fakeClient) Queue(
	context.Context, string, string, string,
) (codex.QueueEntry, error) {
	return codex.QueueEntry{}, nil
}
func (client *fakeClient) DeleteQueueEntry(_ context.Context, _, id string) error {
	client.deletedQueueID = id
	return nil
}
func (client *fakeClient) StartQueue(_ context.Context, _, id string) error {
	client.startedQueueID = id
	return nil
}
func (client *fakeClient) Interrupt(context.Context, string) error { return nil }
func (client *fakeClient) ListModels(context.Context) ([]codex.Model, error) {
	return []codex.Model{{
		Model: "model", SupportedReasoningEfforts: []codex.ReasoningEffortOption{{ReasoningEffort: "high"}},
	}}, nil
}
func (client *fakeClient) ListCollaborationModes(context.Context) ([]codex.CollaborationMode, error) {
	return []codex.CollaborationMode{{Mode: "default"}, {Mode: "plan"}}, nil
}
func (client *fakeClient) UpdateThreadSettings(
	context.Context, string, codex.ThreadSettingsUpdate,
) (codex.ThreadSettings, error) {
	return codex.ThreadSettings{}, nil
}
func (client *fakeClient) RespondDecision(context.Context, string, string, string) error {
	return nil
}
func (client *fakeClient) RespondAnswers(
	context.Context, string, string, map[string]map[string][]string,
) error {
	return nil
}
func (client *fakeClient) SnoozeUserInput(string, string) error { return nil }
func (client *fakeClient) Subscribe(
	context.Context, string,
) (<-chan struct{}, func(), error) {
	events := make(chan struct{})
	return events, func() { close(events) }, nil
}

func newTestHandler(t *testing.T, capabilities Capabilities) (*Handler, *fakeClient, *int, *ResolveRequest, *int) {
	t.Helper()
	client := &fakeClient{}
	mutationLock := NewMutationLock()
	calls := 0
	releases := 0
	var resolved ResolveRequest
	handler, err := NewHandler(Options{
		AllowedOrigins: []string{"https://workspace.example.test"},
		Logger:         log.New(&bytes.Buffer{}, "", 0),
		Resolver: ResolverFunc(func(_ context.Context, request ResolveRequest) (Target, error) {
			calls++
			resolved = request
			if request.ID != "opaque" {
				t.Fatalf("resolver ID = %q", request.ID)
			}
			return Target{
				Client: client, ThreadID: "trusted-thread", Directory: "/srv/workspace",
				Capabilities: capabilities, MutationLock: mutationLock,
				Release: func() { releases++ },
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler, client, &calls, &resolved, &releases
}

func TestHandlerRejectsWrongOriginBeforeResolving(t *testing.T) {
	handler, _, calls, _, _ := newTestHandler(t, Capabilities{Send: true})
	request := httptest.NewRequest(
		http.MethodPost, "/codex/conversations/opaque/message",
		strings.NewReader(`{"message":"hello","clientUserMessageId":"2a0e3d66-f923-4c79-bde5-c25c1edfd02b"}`),
	)
	request.Header.Set("Origin", "https://attacker.example")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || *calls != 0 {
		t.Fatalf("status = %d, resolver calls = %d", response.Code, *calls)
	}
}

func TestHandlerUsesOnlyResolvedThread(t *testing.T) {
	handler, client, _, resolved, releases := newTestHandler(t, Capabilities{Send: true})
	request := httptest.NewRequest(
		http.MethodPost, "/codex/conversations/opaque/message",
		strings.NewReader(`{"message":"hello","clientUserMessageId":"2a0e3d66-f923-4c79-bde5-c25c1edfd02b"}`),
	)
	request.Header.Set("Origin", "https://workspace.example.test")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if client.sentThread != "trusted-thread" || client.sentText != "hello" {
		t.Fatalf("send target = %q, %q", client.sentThread, client.sentText)
	}
	if client.verifiedThread != "trusted-thread" || client.verifiedCwd != "/srv/workspace" {
		t.Fatalf("verified target = %q, %q", client.verifiedThread, client.verifiedCwd)
	}
	if !resolved.Mutation || resolved.Operation != "message" || *releases != 1 {
		t.Fatalf("resolve request = %#v, releases = %d", *resolved, *releases)
	}
}

func TestHandlerRejectsBrowserSelectedThread(t *testing.T) {
	handler, client, _, _, _ := newTestHandler(t, Capabilities{Send: true})
	request := httptest.NewRequest(
		http.MethodPost, "/codex/conversations/opaque/message",
		strings.NewReader(`{"message":"hello","clientUserMessageId":"2a0e3d66-f923-4c79-bde5-c25c1edfd02b","threadId":"other"}`),
	)
	request.Header.Set("Origin", "https://workspace.example.test")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || client.sentThread != "" {
		t.Fatalf("status = %d, sent thread = %q", response.Code, client.sentThread)
	}
}

func TestHandlerRejectsTrailingMalformedData(t *testing.T) {
	handler, client, _, _, _ := newTestHandler(t, Capabilities{Send: true})
	request := httptest.NewRequest(
		http.MethodPost, "/codex/conversations/opaque/message",
		strings.NewReader(`{"message":"hello","clientUserMessageId":"2a0e3d66-f923-4c79-bde5-c25c1edfd02b"} trailing`),
	)
	request.Header.Set("Origin", "https://workspace.example.test")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || client.sentThread != "" {
		t.Fatalf("status = %d, sent thread = %q", response.Code, client.sentThread)
	}
}

func TestHandlerEnforcesPerOperationCapability(t *testing.T) {
	handler, _, _, _, _ := newTestHandler(t, Capabilities{Read: true})
	request := httptest.NewRequest(
		http.MethodPost, "/codex/conversations/opaque/interrupt", strings.NewReader(`{}`),
	)
	request.Header.Set("Origin", "https://workspace.example.test")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestHandlerDoesNotExposeTheRemovedSnapshotSurface(t *testing.T) {
	handler, _, _, _, _ := newTestHandler(t, Capabilities{Read: true})
	request := httptest.NewRequest(http.MethodGet, "/codex/conversations/opaque/snapshot", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
}

func TestHandlerChecksDurableRetryBeforeSending(t *testing.T) {
	handler, client, _, _, _ := newTestHandler(t, Capabilities{Send: true})
	request := httptest.NewRequest(
		http.MethodPost, "/codex/conversations/opaque/message",
		strings.NewReader(`{"message":"hello","clientUserMessageId":"2a0e3d66-f923-4c79-bde5-c25c1edfd02b","retry":true}`),
	)
	request.Header.Set("Origin", "https://workspace.example.test")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || !client.retryChecked {
		t.Fatalf("status = %d, retry checked = %t", response.Code, client.retryChecked)
	}
}

func TestHandlerDoesNotExposeAppServerErrors(t *testing.T) {
	handler, client, _, _, _ := newTestHandler(t, Capabilities{Send: true})
	client.sendErr = errors.New("secret socket /run/private/app-server.sock failed")
	request := httptest.NewRequest(
		http.MethodPost, "/codex/conversations/opaque/message",
		strings.NewReader(`{"message":"hello","clientUserMessageId":"2a0e3d66-f923-4c79-bde5-c25c1edfd02b"}`),
	)
	request.Header.Set("Origin", "https://workspace.example.test")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable ||
		strings.Contains(response.Body.String(), "/run/private") ||
		!strings.Contains(response.Body.String(), "Codex service is unavailable") {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}

func TestHandlerEscapesOpaqueControlCharactersInLogs(t *testing.T) {
	var output bytes.Buffer
	handler, err := NewHandler(Options{
		AllowedOrigins: []string{"https://workspace.example.test"},
		Logger:         log.New(&output, "", 0),
		Resolver: ResolverFunc(func(context.Context, ResolveRequest) (Target, error) {
			return Target{}, errors.New("unavailable")
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(
		http.MethodGet, "/codex/conversations/line%0Abreak/thread", nil,
	))
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	logged := output.String()
	if !strings.Contains(logged, `line\nbreak`) || strings.Count(logged, "\n") != 1 {
		t.Fatalf("unescaped conversation log = %q", logged)
	}
}

func TestHandlerBoundsOperationsAndReleasesTheApplicationMutationLock(t *testing.T) {
	client := &fakeClient{blockSend: true}
	mutationLock := NewMutationLock()
	handler, err := NewHandler(Options{
		AllowedOrigins:   []string{"https://workspace.example.test"},
		OperationTimeout: 10 * time.Millisecond,
		Logger:           log.New(&bytes.Buffer{}, "", 0),
		Resolver: ResolverFunc(func(context.Context, ResolveRequest) (Target, error) {
			return Target{
				Client: client, ThreadID: "thread-1", Directory: "/srv/workspace",
				Capabilities: Capabilities{Send: true}, MutationLock: mutationLock,
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(
		http.MethodPost, "/codex/conversations/opaque/message",
		strings.NewReader(`{"message":"hello","clientUserMessageId":"2a0e3d66-f923-4c79-bde5-c25c1edfd02b"}`),
	)
	request.Header.Set("Origin", "https://workspace.example.test")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	lockContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := mutationLock.Lock(lockContext); err != nil {
		t.Fatal("application mutation lock was not released after timeout")
	}
	mutationLock.Unlock()
}

func TestHandlerBoundsWaitingForTheApplicationMutationLock(t *testing.T) {
	mutationLock := NewMutationLock()
	if err := mutationLock.Lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer mutationLock.Unlock()
	handler, err := NewHandler(Options{
		AllowedOrigins:   []string{"https://workspace.example.test"},
		OperationTimeout: 10 * time.Millisecond,
		Logger:           log.New(&bytes.Buffer{}, "", 0),
		Resolver: ResolverFunc(func(context.Context, ResolveRequest) (Target, error) {
			return Target{
				Client: &fakeClient{}, ThreadID: "thread-1", Directory: "/srv/workspace",
				Capabilities: Capabilities{Send: true}, MutationLock: mutationLock,
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(
		http.MethodPost, "/codex/conversations/opaque/message",
		strings.NewReader(`{"message":"hello","clientUserMessageId":"2a0e3d66-f923-4c79-bde5-c25c1edfd02b"}`),
	)
	request.Header.Set("Origin", "https://workspace.example.test")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
}

func TestHandlerUsesTheConfiguredMessageByteLimit(t *testing.T) {
	client := &fakeClient{}
	handler, err := NewHandler(Options{
		AllowedOrigins:  []string{"https://workspace.example.test"},
		MaxMessageBytes: 5,
		Resolver: ResolverFunc(func(context.Context, ResolveRequest) (Target, error) {
			return Target{
				Client: client, ThreadID: "thread-1", Directory: "/srv/workspace",
				Capabilities: Capabilities{Send: true}, MutationLock: NewMutationLock(),
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		message string
		status  int
	}{{"12345", http.StatusAccepted}, {"123456", http.StatusBadRequest}} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(
			http.MethodPost, "/codex/conversations/opaque/message",
			strings.NewReader(`{"message":"`+test.message+`","clientUserMessageId":"2a0e3d66-f923-4c79-bde5-c25c1edfd02b"}`),
		)
		request.Header.Set("Origin", "https://workspace.example.test")
		handler.ServeHTTP(response, request)
		if response.Code != test.status {
			t.Fatalf("message %q status = %d: %s", test.message, response.Code, response.Body.String())
		}
	}
}

func TestEventStreamStopsWhenTheApplicationShutsDown(t *testing.T) {
	shutdown := make(chan struct{})
	handler, err := NewHandler(Options{
		AllowedOrigins: []string{"https://workspace.example.test"}, Shutdown: shutdown,
		Resolver: ResolverFunc(func(context.Context, ResolveRequest) (Target, error) {
			return Target{
				Client: &fakeClient{}, ThreadID: "thread-1", Directory: "/srv/workspace",
				Capabilities: Capabilities{EventStream: true},
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(
			http.MethodGet, "/codex/conversations/opaque/events", nil,
		))
		close(done)
	}()
	close(shutdown)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("event stream remained open after application shutdown")
	}
}

func TestHandlerRejectsTargetWhoseThreadCannotBeVerified(t *testing.T) {
	handler, client, _, _, _ := newTestHandler(t, Capabilities{Read: true})
	client.verifyErr = errors.New("wrong cwd")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(
		http.MethodGet, "/codex/conversations/opaque/thread", nil,
	))
	if response.Code != http.StatusNotFound || strings.Contains(response.Body.String(), "cwd") {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}

func TestHandlerRoutesCompleteConversationSurface(t *testing.T) {
	const attempt = "2a0e3d66-f923-4c79-bde5-c25c1edfd02b"
	tests := []struct {
		method       string
		path         string
		body         string
		capabilities Capabilities
		status       int
	}{
		{http.MethodGet, "thread", "", Capabilities{Read: true}, http.StatusOK},
		{http.MethodGet, "pending", "", Capabilities{Pending: true}, http.StatusOK},
		{http.MethodGet, "queue", "", Capabilities{QueueRead: true}, http.StatusOK},
		{http.MethodGet, "models", "", Capabilities{Settings: true}, http.StatusOK},
		{http.MethodGet, "collaboration-modes", "", Capabilities{Settings: true}, http.StatusOK},
		{http.MethodPost, "queue", `{"message":"later","clientUserMessageId":"` + attempt + `"}`, Capabilities{Queue: true}, http.StatusAccepted},
		{http.MethodDelete, "queue/queued-1", "", Capabilities{Queue: true}, http.StatusOK},
		{http.MethodPost, "queue/start", `{"queuedSubmissionId":"queued-1"}`, Capabilities{Queue: true}, http.StatusAccepted},
		{http.MethodPost, "message-ack", `{"acknowledgements":[]}`, Capabilities{Send: true}, http.StatusOK},
		{http.MethodPost, "settings", `{"collaborationMode":"plan"}`, Capabilities{Settings: true}, http.StatusOK},
		{http.MethodPost, "interrupt", `{}`, Capabilities{Interrupt: true}, http.StatusAccepted},
		{http.MethodPost, "respond", `{"id":"request-1","decision":"accept"}`, Capabilities{Respond: true}, http.StatusOK},
		{http.MethodPost, "respond", `{"id":"request-1","snooze":true}`, Capabilities{Respond: true}, http.StatusOK},
	}
	for _, test := range tests {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			handler, client, _, _, _ := newTestHandler(t, test.capabilities)
			request := httptest.NewRequest(
				test.method, "/codex/conversations/opaque/"+test.path, strings.NewReader(test.body),
			)
			if test.method != http.MethodGet {
				request.Header.Set("Origin", "https://workspace.example.test")
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status = %d: %s", response.Code, response.Body.String())
			}
			if client.verifiedThread != "trusted-thread" {
				t.Fatal("operation bypassed target verification")
			}
		})
	}
}

func TestHandlerDisambiguatesQueueStartByMethod(t *testing.T) {
	handler, client, _, _, _ := newTestHandler(t, Capabilities{Queue: true})
	request := httptest.NewRequest(
		http.MethodDelete, "/codex/conversations/opaque/queue/start", nil,
	)
	request.Header.Set("Origin", "https://workspace.example.test")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || client.deletedQueueID != "start" || client.startedQueueID != "" {
		t.Fatalf(
			"delete response = %d, deleted ID = %q, started ID = %q",
			response.Code, client.deletedQueueID, client.startedQueueID,
		)
	}

	request = httptest.NewRequest(
		http.MethodPost, "/codex/conversations/opaque/queue/start",
		strings.NewReader(`{"queuedSubmissionId":"queued-1"}`),
	)
	request.Header.Set("Origin", "https://workspace.example.test")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || client.startedQueueID != "queued-1" {
		t.Fatalf("start response = %d, started ID = %q", response.Code, client.startedQueueID)
	}
}

func TestHandlerSupportsTheRootBasePath(t *testing.T) {
	client := &fakeClient{}
	handler, err := NewHandler(Options{
		BasePath:       "/",
		AllowedOrigins: []string{"https://workspace.example.test"},
		Resolver: ResolverFunc(func(context.Context, ResolveRequest) (Target, error) {
			return Target{
				Client: client, ThreadID: "trusted-thread", Directory: "/srv/workspace",
				Capabilities: Capabilities{Read: true}, MutationLock: NewMutationLock(),
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(
		http.MethodGet, "/conversations/opaque/thread", nil,
	))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
}

func TestHandlerDecodesOpaquePathSegmentsExactlyOnce(t *testing.T) {
	client := &fakeClient{}
	var resolved ResolveRequest
	handler, err := NewHandler(Options{
		AllowedOrigins: []string{"https://workspace.example.test"},
		Resolver: ResolverFunc(func(_ context.Context, request ResolveRequest) (Target, error) {
			resolved = request
			return Target{
				Client: client, ThreadID: "trusted-thread", Directory: "/srv/workspace",
				Capabilities: Capabilities{Read: true, Queue: true}, MutationLock: NewMutationLock(),
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(
		http.MethodGet, "/codex/conversations/%252F%25%2541-%252e%252e-%C5%BE/thread", nil,
	))
	if response.Code != http.StatusOK || resolved.ID != "%2F%%41-%2e%2e-ž" {
		t.Fatalf("conversation response = %d, ID = %q", response.Code, resolved.ID)
	}

	request := httptest.NewRequest(
		http.MethodDelete,
		"/codex/conversations/%252F%25%2541-%252e%252e-%C5%BE/queue/%252F%25%2541-%252e%252e-%C5%BE",
		nil,
	)
	request.Header.Set("Origin", "https://workspace.example.test")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || client.deletedQueueID != "%2F%%41-%2e%2e-ž" {
		t.Fatalf("queue response = %d, deleted ID = %q", response.Code, client.deletedQueueID)
	}
}

func TestHandlerRejectsNoncanonicalConversationRoutes(t *testing.T) {
	handler, _, calls, _, _ := newTestHandler(t, Capabilities{Read: true})
	paths := []string{
		"/codex/conversations//opaque/thread",
		"/codex/conversations/opaque//thread",
		"/codex/conversations/opaque/thread/",
		"/codex/conversations/opaque/thread//",
		"/codex/conversations/./thread",
		"/codex/conversations/../thread",
		"/codex/conversations/%2e/thread",
		"/codex/conversations/%2e%2e/thread",
		"/codex/conversations/op%61que/thread",
		"/codex/conversations/%6fpaque/thread",
		"/codex/conversations/%C5%be/thread",
		"/codex/conversations/%FF/thread",
		"/codex/conversations/" + strings.Repeat("a", 257) + "/thread",
	}
	for _, requestPath := range paths {
		t.Run(requestPath, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, requestPath, nil))
			if response.Code != http.StatusNotFound {
				t.Fatalf("status = %d: %s", response.Code, response.Body.String())
			}
		})
	}
	if *calls != 0 {
		t.Fatalf("resolver called %d times", *calls)
	}
}

func TestHandlerRejectsNoncanonicalRawConversationRoutes(t *testing.T) {
	handler, _, calls, _, _ := newTestHandler(t, Capabilities{Read: true, Queue: true})
	paths := []string{
		"/codex/conversations/ž/thread",
		`/codex/conversations/"/thread`,
		"/codex/conversations/opaque/queue/ž",
		`/codex/conversations/opaque/queue/{`,
	}
	for _, requestPath := range paths {
		t.Run(requestPath, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, requestPath, nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusNotFound {
				t.Fatalf("status = %d: %s", response.Code, response.Body.String())
			}
		})
	}
	if *calls != 0 {
		t.Fatalf("resolver called %d times", *calls)
	}
}

func TestHandlerAcceptsMaximumOpaqueIDLength(t *testing.T) {
	id := strings.Repeat("ž", 128)
	handler, err := NewHandler(Options{
		AllowedOrigins: []string{"https://workspace.example.test"},
		Resolver: ResolverFunc(func(_ context.Context, request ResolveRequest) (Target, error) {
			if request.ID != id {
				t.Fatalf("resolver ID = %q", request.ID)
			}
			return Target{
				Client: &fakeClient{}, ThreadID: "trusted-thread", Directory: "/srv/workspace",
				Capabilities: Capabilities{Read: true}, MutationLock: NewMutationLock(),
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(
		http.MethodGet, "/codex/conversations/"+encodeOpaquePathSegment(id)+"/thread", nil,
	))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
}

func TestHandlerRejectsNoncanonicalQueueIDs(t *testing.T) {
	for _, queueID := range []string{
		".", "..", "q%75eued", "%2f", "%FF", strings.Repeat("a", 257),
	} {
		t.Run(queueID, func(t *testing.T) {
			handler, client, _, _, _ := newTestHandler(t, Capabilities{Queue: true})
			request := httptest.NewRequest(
				http.MethodDelete, "/codex/conversations/opaque/queue/"+queueID, nil,
			)
			request.Header.Set("Origin", "https://workspace.example.test")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest || client.deletedQueueID != "" {
				t.Fatalf("response = %d, deleted ID = %q", response.Code, client.deletedQueueID)
			}
		})
	}
}

func TestHandlerAcceptsMaximumQueueIDLength(t *testing.T) {
	queueID := strings.Repeat("ž", 128)
	handler, client, _, _, _ := newTestHandler(t, Capabilities{Queue: true})
	request := httptest.NewRequest(
		http.MethodDelete,
		"/codex/conversations/opaque/queue/"+encodeOpaquePathSegment(queueID),
		nil,
	)
	request.Header.Set("Origin", "https://workspace.example.test")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || client.deletedQueueID != queueID {
		t.Fatalf("response = %d, deleted ID = %q", response.Code, client.deletedQueueID)
	}
}

func TestHandlerStartQueueValidatesOpaqueID(t *testing.T) {
	for _, queueID := range []string{
		"", ".", "..", "queued/item", "queued\x00item", strings.Repeat("a", 257),
	} {
		t.Run(queueID, func(t *testing.T) {
			handler, client, _, _, _ := newTestHandler(t, Capabilities{Queue: true})
			body, err := json.Marshal(map[string]string{"queuedSubmissionId": queueID})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(
				http.MethodPost, "/codex/conversations/opaque/queue/start", bytes.NewReader(body),
			)
			request.Header.Set("Origin", "https://workspace.example.test")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest || client.startedQueueID != "" {
				t.Fatalf("response = %d, started ID = %q", response.Code, client.startedQueueID)
			}
		})
	}
	if validOpaqueID(string([]byte{0xff})) {
		t.Fatal("invalid UTF-8 queue ID was accepted")
	}
	for index, queueID := range []string{" queued item ", strings.Repeat("ž", 128)} {
		t.Run(fmt.Sprintf("valid-%d", index), func(t *testing.T) {
			handler, client, _, _, _ := newTestHandler(t, Capabilities{Queue: true})
			body, err := json.Marshal(map[string]string{"queuedSubmissionId": queueID})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(
				http.MethodPost, "/codex/conversations/opaque/queue/start", bytes.NewReader(body),
			)
			request.Header.Set("Origin", "https://workspace.example.test")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusAccepted || client.startedQueueID != queueID {
				t.Fatalf("response = %d, started ID = %q", response.Code, client.startedQueueID)
			}
		})
	}
}

func TestSharedOpaqueIDContract(t *testing.T) {
	contract := loadSharedPathContract(t)
	for _, testCase := range contract.OpaqueIDCases {
		t.Run(testCase.Name, func(t *testing.T) {
			id := sharedContractID(t, testCase)
			if valid := validOpaqueID(id); valid != testCase.Valid {
				t.Fatalf("validOpaqueID(%q) = %t, want %t", id, valid, testCase.Valid)
			}
			if testCase.Valid && testCase.Encoded != "" &&
				encodeOpaquePathSegment(id) != testCase.Encoded {
				t.Fatalf(
					"encodeOpaquePathSegment(%q) = %q, want %q",
					id, encodeOpaquePathSegment(id), testCase.Encoded,
				)
			}
		})
	}
}

func TestHandlerRejectsNoncanonicalBasePaths(t *testing.T) {
	for _, basePath := range []string{
		"/api/../codex", "/api//codex", "/api//", "/api/.", "/api/segment/..", `/api\codex`, "/%63odex",
		"/café", "/api path", "/api\npath",
	} {
		t.Run(basePath, func(t *testing.T) {
			_, err := NewHandler(Options{
				BasePath:       basePath,
				AllowedOrigins: []string{"https://workspace.example.test"},
				Resolver: ResolverFunc(func(context.Context, ResolveRequest) (Target, error) {
					return Target{}, nil
				}),
			})
			if err == nil {
				t.Fatal("noncanonical base path was accepted")
			}
		})
	}
	if _, err := NewHandler(Options{
		BasePath:       "/api-v1_~!$&()*+,;=:@",
		AllowedOrigins: []string{"https://workspace.example.test"},
		Resolver: ResolverFunc(func(context.Context, ResolveRequest) (Target, error) {
			return Target{}, nil
		}),
	}); err != nil {
		t.Fatalf("safe reserved base path was rejected: %v", err)
	}
	alphabet := loadSharedPathContract(t).BasePathAlphabet
	for character := byte(0x20); character <= 0x7e; character++ {
		basePath := "/api" + string(character) + "segment"
		_, err := NewHandler(Options{
			BasePath:       basePath,
			AllowedOrigins: []string{"https://workspace.example.test"},
			Resolver: ResolverFunc(func(context.Context, ResolveRequest) (Target, error) {
				return Target{}, nil
			}),
		})
		accepted := err == nil
		expected := strings.ContainsRune(alphabet, rune(character))
		if accepted != expected {
			t.Errorf("base-path acceptance differed for ASCII %d: accepted=%t expected=%t", character, accepted, expected)
		}
	}
}

func TestReadCapabilityExposesOnlyTheTranscript(t *testing.T) {
	handler, _, _, _, _ := newTestHandler(t, Capabilities{Read: true})
	for _, operation := range []string{"pending", "queue"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(
			http.MethodGet, "/codex/conversations/opaque/"+operation, nil,
		))
		if response.Code != http.StatusForbidden {
			t.Fatalf("GET %s status = %d", operation, response.Code)
		}
	}
}

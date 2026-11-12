package conversation_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/aither64/codex-web/conversation"
	"github.com/aither64/codex-web/conversation/backend"
)

// This backend fixture deliberately imports no native client, types or transport.
type independentBackend struct {
	identities []backend.Identity
	sends      []backend.SendRequest
	commands   []string
	result     backend.SendResult
	err        error
	wrongPage  bool
}

func (b *independentBackend) Verify(_ context.Context, id backend.Identity) error {
	b.identities = append(b.identities, id)
	if id.ScopeKey != "member-incarnation" || id.Directory != "/trusted" {
		return errors.New("wrong authority")
	}
	return nil
}
func (b *independentBackend) Read(_ context.Context, id backend.Identity) (backend.Transcript, error) {
	return backend.Transcript{ThreadID: id.ConversationID, Status: "idle", Entries: []backend.TranscriptEntry{{Kind: "agentMessage", Text: "hello"}}}, nil
}
func (b *independentBackend) ReadPage(ctx context.Context, id backend.Identity, _ backend.HistoryCursor) (backend.TranscriptPage, error) {
	value, _ := b.Read(ctx, id)
	if b.wrongPage {
		value.ThreadID = "other"
	}
	return backend.TranscriptPage{Transcript: value}, nil
}
func (b *independentBackend) Pending(context.Context, backend.Identity) ([]backend.Prompt, error) {
	return nil, nil
}
func (b *independentBackend) Send(_ context.Context, _ backend.Identity, request backend.SendRequest) (backend.SendResult, error) {
	b.sends = append(b.sends, request)
	return b.result, b.err
}
func (b *independentBackend) Acknowledge(_ context.Context, _ backend.Identity, acks []backend.SendAcknowledgement) ([]string, error) {
	ids := make([]string, len(acks))
	for i, ack := range acks {
		ids[i] = ack.ClientUserMessageID
	}
	return ids, nil
}
func (b *independentBackend) ListQueue(context.Context, backend.Identity) ([]backend.QueueEntry, error) {
	return nil, nil
}
func (b *independentBackend) Queue(_ context.Context, _ backend.Identity, request backend.QueueRequest) (backend.QueueEntry, error) {
	b.commands = append(b.commands, "queue")
	return backend.QueueEntry{ID: "entry", Text: request.Message, ClientUserMessageID: request.ClientUserMessageID}, nil
}
func (b *independentBackend) Delete(context.Context, backend.Identity, string) error {
	b.commands = append(b.commands, "delete")
	return nil
}
func (b *independentBackend) Start(context.Context, backend.Identity, string) error {
	b.commands = append(b.commands, "start")
	return nil
}
func (b *independentBackend) DeleteWithCompletion(_ context.Context, _ backend.Identity, entry string, complete func() error) error {
	b.commands = append(b.commands, "delete-complete")
	return complete()
}
func (b *independentBackend) ReconcileDeletions(_ context.Context, _ backend.Identity, complete func(string) error) error {
	b.commands = append(b.commands, "reconcile")
	return complete("entry")
}
func (b *independentBackend) Interrupt(context.Context, backend.Identity) error {
	b.commands = append(b.commands, "interrupt")
	return nil
}
func (b *independentBackend) Models(context.Context, backend.Identity) ([]backend.Model, error) {
	return []backend.Model{{Model: "model", SupportedReasoningEfforts: []backend.ReasoningEffortOption{{ReasoningEffort: "high"}}}}, nil
}
func (b *independentBackend) Modes(context.Context, backend.Identity) ([]backend.CollaborationMode, error) {
	return []backend.CollaborationMode{{Name: "Default", Mode: "default"}}, nil
}
func (b *independentBackend) Update(_ context.Context, _ backend.Identity, update backend.SettingsUpdate) (backend.Settings, error) {
	b.commands = append(b.commands, "settings")
	return backend.Settings{Model: *update.Model, ReasoningEffort: *update.ReasoningEffort}, nil
}
func (b *independentBackend) Respond(context.Context, backend.Identity, backend.PromptResponse) error {
	b.commands = append(b.commands, "respond")
	return nil
}
func (b *independentBackend) Subscribe(context.Context, backend.Identity) (<-chan struct{}, func(), error) {
	events := make(chan struct{})
	close(events)
	return events, func() {}, nil
}
func (b *independentBackend) PendingRevision(backend.Identity) uint64 { return 7 }
func (b *independentBackend) ReadActivity(_ context.Context, id backend.Identity) (backend.ActivitySnapshot, error) {
	return backend.ActivitySnapshot{ThreadID: id.ConversationID}, nil
}

func neutralFixture(t *testing.T) (*conversation.Handler, *independentBackend, *conversation.BackendTarget, *int) {
	t.Helper()
	b := &independentBackend{result: backend.SendResult{Outcome: backend.Accepted, Receipt: backend.SendReceipt{TurnID: "turn", ClientUserMessageID: attempt}}}
	releases := new(int)
	target := &conversation.BackendTarget{
		Identity:     backend.Identity{BackendID: "fixture", ScopeKey: "member-incarnation", ConversationID: "opaque-native", Directory: "/trusted", Generation: "one"},
		Operations:   backend.Operations{Verify: b, Read: b, Page: b, Pending: b, Send: b, Acknowledge: b, QueueRead: b, QueueWrite: b, QueueCompletion: b, Interrupt: b, Settings: b, Respond: b, Watch: b, PendingRevision: b, Activity: b},
		Capabilities: conversation.Capabilities{Read: true, Pending: true, Send: true, QueueRead: true, Queue: true, Interrupt: true, Settings: true, Respond: true, EventStream: true},
		MutationLock: conversation.NewMutationLock(), Release: func() { *releases++ },
	}
	handler, err := conversation.NewBackendHandler(conversation.BackendOptions{AllowedOrigins: []string{"https://portal.test"}, Logger: log.New(io.Discard, "", 0), Resolver: conversation.BackendResolverFunc(func(_ context.Context, request conversation.ResolveRequest) (conversation.BackendTarget, error) {
		if request.ID != "application-id" {
			return conversation.BackendTarget{}, errors.New("denied")
		}
		return *target, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	return handler, b, target, releases
}

const attempt = "2a0e3d66-f923-4c79-bde5-c25c1edfd02b"

func callNeutral(handler http.Handler, method, route, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "/codex/conversations/application-id/"+route, strings.NewReader(body))
	r.Header.Set("Origin", "https://portal.test")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

func TestNeutralHandlerExistingBrowserSurface(t *testing.T) {
	handler, b, _, releases := neutralFixture(t)
	for _, c := range []struct {
		method, route, body string
		status              int
	}{
		{"GET", "thread", "", 200}, {"GET", "thread/page", "", 200}, {"GET", "pending", "", 200}, {"GET", "activity", "", 200}, {"GET", "models", "", 200}, {"GET", "collaboration-modes", "", 200},
		{"GET", "queue", "", 200}, {"POST", "message", `{"message":"hello","clientUserMessageId":"` + attempt + `"}`, 202},
		{"POST", "message-ack", `{"acknowledgements":[]}`, 200},
		{"POST", "queue", `{"message":"queued","clientUserMessageId":"` + attempt + `"}`, 202},
		{"POST", "queue/start", `{"queuedSubmissionId":"entry"}`, 202}, {"DELETE", "queue/entry", "", 200}, {"POST", "queue/reconcile", "", 200},
		{"POST", "interrupt", `{}`, 202}, {"PATCH", "settings", `{"model":"model","reasoningEffort":"high"}`, 200},
		{"POST", "respond", `{"id":"offer","token":"bound","decision":"accept"}`, 200}, {"GET", "events", "", 200},
	} {
		response := callNeutral(handler, c.method, c.route, c.body)
		if response.Code != c.status {
			t.Fatalf("%s %s: %d %s", c.method, c.route, response.Code, response.Body.String())
		}
	}
	if *releases != 17 || len(b.identities) != 17 || len(b.sends) != 1 {
		t.Fatalf("scope/release/calls: %d %d %d", *releases, len(b.identities), len(b.sends))
	}
	if !reflect.DeepEqual(b.commands, []string{"queue", "start", "delete", "reconcile", "interrupt", "settings", "respond"}) {
		t.Fatalf("commands: %v", b.commands)
	}
}

func TestNeutralSupportAndAuthorizationAreIndependent(t *testing.T) {
	for _, route := range []string{"thread", "pending", "queue", "models", "collaboration-modes"} {
		t.Run(route, func(t *testing.T) {
			h, b, target, _ := neutralFixture(t)
			target.Operations = backend.Operations{Verify: b}
			if w := callNeutral(h, "GET", route, ""); w.Code != 501 {
				t.Fatalf("unsupported %s: %d", route, w.Code)
			}
			target.Capabilities = conversation.Capabilities{}
			if w := callNeutral(h, "GET", route, ""); w.Code != 403 {
				t.Fatalf("unauthorized %s: %d", route, w.Code)
			}
		})
	}
	h, b, target, _ := neutralFixture(t)
	target.Operations = backend.Operations{Verify: b}
	for _, test := range []struct {
		method, route, body string
		status              int
	}{
		{"GET", "thread/page", "", 501}, {"GET", "activity", "", 404}, {"GET", "retry", "", 403},
		{"POST", "queue/reconcile", "", 501}, {"POST", "message", `{}`, 501}, {"POST", "interrupt", `{}`, 501},
		{"POST", "respond", `{"id":"offer","token":"bound","decision":"accept"}`, 409},
		{"POST", "respond", `{"id":"offer","decision":"accept"}`, 501},
	} {
		if w := callNeutral(h, test.method, test.route, test.body); w.Code != test.status {
			t.Fatalf("%s: %d %s", test.route, w.Code, w.Body.String())
		}
	}
	if len(b.sends) != 0 || len(b.commands) != 0 {
		t.Fatal("unsupported request caused an effect")
	}
}

func TestNeutralOutcomeNeverInventsAcceptanceOrResendPermission(t *testing.T) {
	for _, test := range []struct {
		outcome   backend.SendOutcome
		retryable bool
		err       error
		status    int
		code      string
	}{
		{backend.Accepted, false, nil, 202, ""}, {backend.NotSubmitted, true, nil, 503, "message_not_sent"},
		{backend.Unknown, true, nil, 409, "message_delivery_unknown"}, {"", true, nil, 503, ""}, {"future", true, nil, 503, ""},
		{backend.Accepted, true, errors.New("private failure"), 503, ""},
	} {
		t.Run(string(test.outcome)+test.code, func(t *testing.T) {
			h, b, target, _ := neutralFixture(t)
			b.result.Outcome = test.outcome
			b.result.Retryable = test.retryable
			b.err = test.err
			w := callNeutral(h, "POST", "message", `{"message":"same input","clientUserMessageId":"`+attempt+`","retry":true}`)
			if w.Code != test.status {
				t.Fatalf("outcome %q: %d %s", test.outcome, w.Code, w.Body.String())
			}
			var body map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if test.code != "" && body["code"] != test.code {
				t.Fatalf("disposition: %v", body)
			}
			if test.outcome != backend.NotSubmitted && (body["retryable"] == true || body["notSent"] == true) {
				t.Fatal("invented retry permission")
			}
			target.Identity.Generation = "two"
			callNeutral(h, "POST", "message", `{"message":"same input","clientUserMessageId":"`+attempt+`","retry":true}`)
			if len(b.sends) != 2 || b.sends[0] != b.sends[1] || b.sends[0].ClientUserMessageID != attempt || !b.sends[0].Retry {
				t.Fatal("generation changed immutable submission identity")
			}
		})
	}
}

func TestNeutralPageAndOriginCannotBroadenAuthority(t *testing.T) {
	h, b, _, releases := neutralFixture(t)
	b.wrongPage = true
	if w := callNeutral(h, "GET", "thread/page", ""); w.Code != 503 {
		t.Fatalf("wrong page identity: %d", w.Code)
	}
	r := httptest.NewRequest("GET", "/codex/conversations/application-id/thread", nil)
	r.Header.Set("Origin", "https://attacker.test")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 403 || len(b.identities) != 1 || *releases != 1 {
		t.Fatal("origin check reached backend")
	}
}

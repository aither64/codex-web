package conversation

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aither64/codex-web/codex"
)

type admittedSendClient struct {
	*fakeClient
	started chan context.Context
	release chan struct{}
}

func (c *admittedSendClient) Send(ctx context.Context, _, _, id, _ string) (codex.SendReceipt, error) {
	c.started <- ctx
	select {
	case <-ctx.Done():
		return codex.SendReceipt{}, &codex.UnknownSendOutcomeError{Err: ctx.Err()}
	case <-c.release:
		return codex.SendReceipt{TurnID: "accepted", ClientUserMessageID: id}, nil
	}
}

func TestAdmittedSendIgnoresBrowserCancellationButHonorsShutdown(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "browser disconnect", true: "server shutdown"}[shutdown], func(t *testing.T) {
			client := &admittedSendClient{fakeClient: &fakeClient{}, started: make(chan context.Context, 1), release: make(chan struct{})}
			stop := make(chan struct{})
			handler, err := NewHandler(Options{AllowedOrigins: []string{"https://example.test"}, Shutdown: stop,
				Resolver: ResolverFunc(func(context.Context, ResolveRequest) (Target, error) {
					return Target{Client: client, ThreadID: "thread", Directory: "/workspace", MutationLock: NewMutationLock(), Capabilities: Capabilities{Send: true}}, nil
				})})
			if err != nil {
				t.Fatal(err)
			}
			browser, cancel := context.WithCancel(context.Background())
			defer cancel()
			req := httptest.NewRequest(http.MethodPost, "/codex/conversations/opaque/message", strings.NewReader(`{"message":"hello","clientUserMessageId":"2a0e3d66-f923-4c79-bde5-c25c1edfd02b"}`)).WithContext(browser)
			req.Header.Set("Origin", "https://example.test")
			response := httptest.NewRecorder()
			done := make(chan struct{})
			go func() { handler.ServeHTTP(response, req); close(done) }()
			var sendContext context.Context
			select {
			case sendContext = <-client.started:
			case <-time.After(time.Second):
				t.Fatal("send not admitted")
			}
			cancel()
			if sendContext.Err() != nil {
				t.Fatal("browser canceled admitted operation")
			}
			if _, ok := sendContext.Deadline(); !ok {
				t.Fatal("send lost operation budget")
			}
			if shutdown {
				close(stop)
			} else {
				close(client.release)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("send did not finish")
			}
			want := http.StatusAccepted
			if shutdown {
				want = http.StatusConflict
			}
			if response.Code != want {
				t.Fatalf("response: %d %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestSendDeliveryErrorsExposeStateWithoutPrivateDetails(t *testing.T) {
	for _, test := range []struct {
		err    error
		code   string
		status int
	}{
		{&codex.MessageNotSentError{Err: errors.New("private socket")}, "message_not_sent", http.StatusServiceUnavailable},
		{&codex.UnknownSendOutcomeError{Err: errors.New("private socket")}, "message_delivery_unknown", http.StatusConflict},
	} {
		handler, client, _, _, _ := newTestHandler(t, Capabilities{Send: true})
		client.sendErr = test.err
		request := httptest.NewRequest(http.MethodPost, "/codex/conversations/opaque/message", strings.NewReader(`{"message":"hello","clientUserMessageId":"2a0e3d66-f923-4c79-bde5-c25c1edfd02b"}`))
		request.Header.Set("Origin", "https://workspace.example.test")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != test.status || !strings.Contains(response.Body.String(), test.code) || strings.Contains(response.Body.String(), "private") {
			t.Fatalf("delivery error: %d %s", response.Code, response.Body.String())
		}
	}
}

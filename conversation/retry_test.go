package conversation

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type retryFixture struct {
	state                RetryStatus
	thread               string
	reads, sets, cancels int
	err                  error
	lock                 *retryFixtureLock
}

type retryFixtureLock struct{ held bool }

func (l *retryFixtureLock) Lock(context.Context) error { l.held = true; return nil }
func (l *retryFixtureLock) Unlock()                    { l.held = false }
func (f *retryFixture) ReadRetry(_ context.Context, thread string) (RetryStatus, error) {
	f.thread = thread
	f.reads++
	return f.state, f.err
}
func (f *retryFixture) SetRetryEnabled(_ context.Context, thread string, enabled bool) (RetryStatus, error) {
	if !f.lock.held {
		return RetryStatus{}, errors.New("mutation lock missing")
	}
	f.thread = thread
	f.sets++
	f.state.Enabled = enabled
	return f.state, f.err
}
func (f *retryFixture) CancelRetry(_ context.Context, thread string) (RetryStatus, error) {
	if !f.lock.held {
		return RetryStatus{}, errors.New("mutation lock missing")
	}
	f.thread = thread
	f.cancels++
	f.state.Phase = "cancelled"
	return f.state, f.err
}

func TestRetryRoutesRequireTrustedCapabilityOriginAndPayload(t *testing.T) {
	for _, test := range []struct {
		name, method, path, body, origin string
		grant, provider                  bool
		status, calls                    int
	}{
		{"read", "GET", "retry", "", "", true, true, 200, 1},
		{"enable", "POST", "retry/enabled", `{"enabled":true}`, "https://trusted.test", true, true, 200, 1},
		{"cancel", "POST", "retry/cancel", `{}`, "https://trusted.test", true, true, 200, 1},
		{"denied", "GET", "retry", "", "", false, true, 403, 0},
		{"optional absent", "GET", "retry", "", "", true, false, 404, 0},
		{"cross origin", "POST", "retry/cancel", `{}`, "https://evil.test", true, true, 403, 0},
		{"missing origin", "POST", "retry/cancel", `{}`, "", true, true, 403, 0},
		{"no enabled", "POST", "retry/enabled", `{}`, "https://trusted.test", true, true, 400, 0},
		{"null enabled", "POST", "retry/enabled", `{"enabled":null}`, "https://trusted.test", true, true, 400, 0},
		{"extra input", "POST", "retry/enabled", `{"enabled":true,"threadId":"evil"}`, "https://trusted.test", true, true, 400, 0},
		{"cancel cannot send", "POST", "retry/cancel", `{"message":"continue"}`, "https://trusted.test", true, true, 400, 0},
		{"wrong verb", "GET", "retry/cancel", "", "", true, true, 403, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			lock := &retryFixtureLock{}
			provider := &retryFixture{lock: lock, state: RetryStatus{Enabled: true, Phase: "idle", ServerNowMS: 1000, CyberLimit: 30}}
			var selected RetryProvider = provider
			if !test.provider {
				selected = nil
			}
			handler, err := NewHandler(Options{AllowedOrigins: []string{"https://trusted.test"}, Resolver: ResolverFunc(func(_ context.Context, request ResolveRequest) (Target, error) {
				if request.ID != "opaque" || request.Operation != test.path {
					t.Fatalf("request=%+v", request)
				}
				return Target{Client: &fakeClient{}, ThreadID: "trusted-thread", Directory: "/trusted", MutationLock: lock, Retry: selected, Capabilities: Capabilities{Retry: test.grant}}, nil
			})})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(test.method, "/codex/conversations/opaque/"+test.path, strings.NewReader(test.body))
			if test.origin != "" {
				request.Header.Set("Origin", test.origin)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.status || provider.reads+provider.sets+provider.cancels != test.calls {
				t.Fatalf("status=%d body=%s calls=%d/%d/%d", response.Code, response.Body.String(), provider.reads, provider.sets, provider.cancels)
			}
			if test.calls > 0 && provider.thread != "trusted-thread" {
				t.Fatal("untrusted thread passed to provider")
			}
			if test.status == 200 && (!strings.Contains(response.Body.String(), `"nextAttemptAtMs":null`) || !strings.Contains(response.Body.String(), `"cyberLimit":30`)) {
				t.Fatal(response.Body.String())
			}
			if lock.held {
				t.Fatal("mutation lock leaked")
			}
		})
	}
}

func TestRetryProviderErrorsAndPrivateStateAreNotExposed(t *testing.T) {
	for _, providerErr := range []bool{true, false} {
		var output bytes.Buffer
		provider := &retryFixture{state: RetryStatus{Phase: "unknown", Reason: "/private/options"}}
		if providerErr {
			provider.err = errors.New("private-token /private/options")
		}
		handler, err := NewHandler(Options{AllowedOrigins: []string{"https://trusted.test"}, Logger: log.New(&output, "", 0), Resolver: ResolverFunc(func(context.Context, ResolveRequest) (Target, error) {
			return Target{Client: &fakeClient{}, ThreadID: "trusted", Directory: "/trusted", Retry: provider, Capabilities: Capabilities{Retry: true}}, nil
		})})
		if err != nil {
			t.Fatal(err)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", "/codex/conversations/opaque/retry", nil))
		if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "private") || strings.Contains(output.String(), "private") {
			t.Fatalf("response=%s log=%s", response.Body.String(), output.String())
		}
	}
}

package conversation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type activityFixture struct {
	state                    RetryActivityStatus
	err                      error
	lock                     *retryFixtureLock
	thread, token, operation string
	calls                    int
}

func (f *activityFixture) call(thread, token, operation string) (RetryActivityStatus, error) {
	if operation != "read" && !f.lock.held {
		return RetryActivityStatus{}, errors.New("mutation lock missing")
	}
	f.thread, f.token, f.operation = thread, token, operation
	f.calls++
	return f.state, f.err
}
func (f *activityFixture) ReadRetryActivity(_ context.Context, thread string) (RetryActivityStatus, error) {
	return f.call(thread, "", "read")
}
func (f *activityFixture) RetryNow(_ context.Context, thread, token string) (RetryActivityStatus, error) {
	return f.call(thread, token, "now")
}
func (f *activityFixture) StopRetry(_ context.Context, thread, token string) (RetryActivityStatus, error) {
	return f.call(thread, token, "stop")
}

func activityNumber(value int64) *int64 { return &value }
func activityState() RetryActivityStatus {
	return RetryActivityStatus{ServerNowMS: 100000, Activity: &RetryActivity{
		ChainKey: "chain-1", Class: "capacity", Phase: "scheduled", ConfirmedStarts: activityNumber(3),
		CountQuality: "exact", StartedAtMS: activityNumber(30000), ElapsedMS: activityNumber(70000), TimingQuality: "exact",
		NextAttemptAtMS: activityNumber(115000), RetryNowToken: "attempt-token", StopToken: "chain-token",
	}}
}
func activityHandler(t *testing.T, provider RetryActivityProvider, grant bool, lock *retryFixtureLock, output *bytes.Buffer) *Handler {
	t.Helper()
	h, err := NewHandler(Options{AllowedOrigins: []string{"https://trusted.test"}, Logger: log.New(output, "", 0),
		Resolver: ResolverFunc(func(_ context.Context, request ResolveRequest) (Target, error) {
			if request.ID != "opaque" {
				t.Fatalf("untrusted route: %+v", request)
			}
			return Target{Client: &fakeClient{}, ThreadID: "trusted-thread", Directory: "/trusted", MutationLock: lock,
				RetryActivity: provider, Capabilities: Capabilities{RetryActivity: grant}}, nil
		})})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestRetryActivityRoutes(t *testing.T) {
	for _, tc := range []struct {
		name, method, path, body, origin, operation string
		grant, provider                             bool
		status, calls                               int
	}{
		{"read", "GET", "activity", "", "", "read", true, true, 200, 1},
		{"now", "POST", "now", `{"token":"attempt-token"}`, "https://trusted.test", "now", true, true, 200, 1},
		{"stop", "POST", "stop", `{"token":"chain-token"}`, "https://trusted.test", "stop", true, true, 200, 1},
		{"denied", "GET", "activity", "", "", "", false, true, 403, 0},
		{"no provider", "GET", "activity", "", "", "", true, false, 404, 0},
		{"cross origin", "POST", "stop", `{"token":"x"}`, "https://evil.test", "", true, true, 403, 0},
		{"no origin", "POST", "stop", `{"token":"x"}`, "", "", true, true, 403, 0},
		{"wrong read verb", "POST", "activity", `{}`, "https://trusted.test", "", true, true, 403, 0},
		{"wrong action verb", "GET", "now", "", "", "", true, true, 403, 0},
		{"missing token", "POST", "now", `{}`, "https://trusted.test", "", true, true, 400, 0},
		{"null token", "POST", "stop", `{"token":null}`, "https://trusted.test", "", true, true, 400, 0},
		{"invalid token", "POST", "now", `{"token":"x/y"}`, "https://trusted.test", "", true, true, 400, 0},
		{"oversize token", "POST", "now", `{"token":"` + strings.Repeat("x", 257) + `"}`, "https://trusted.test", "", true, true, 400, 0},
		{"thread injection", "POST", "now", `{"token":"x","threadId":"evil"}`, "https://trusted.test", "", true, true, 400, 0},
		{"input injection", "POST", "stop", `{"token":"x","message":"send"}`, "https://trusted.test", "", true, true, 400, 0},
		{"options injection", "POST", "now", `{"token":"x","options":{}}`, "https://trusted.test", "", true, true, 400, 0},
		{"trailing body", "POST", "stop", `{"token":"x"}{}`, "https://trusted.test", "", true, true, 400, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lock := &retryFixtureLock{}
			f := &activityFixture{state: activityState(), lock: lock}
			var selected RetryActivityProvider = f
			if !tc.provider {
				selected = nil
			}
			var output bytes.Buffer
			h := activityHandler(t, selected, tc.grant, lock, &output)
			r := httptest.NewRequest(tc.method, "/codex/conversations/opaque/retry/"+tc.path, strings.NewReader(tc.body))
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.status || f.calls != tc.calls {
				t.Fatalf("status=%d body=%s calls=%d", w.Code, w.Body.String(), f.calls)
			}
			if tc.calls > 0 && (f.thread != "trusted-thread" || f.operation != tc.operation) {
				t.Fatalf("provider=%+v", f)
			}
			if tc.operation != "read" && tc.calls > 0 && !strings.Contains(tc.body, `"`+f.token+`"`) {
				t.Fatal("token changed")
			}
			if lock.held {
				t.Fatal("lock leaked")
			}
			if w.Code == 200 {
				var got RetryActivityStatus
				if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
					t.Fatal(err)
				}
				if !validRetryActivityStatus(got) {
					t.Fatal(w.Body.String())
				}
			}
		})
	}
}

func TestRetryActivityErrorsAreBounded(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{
		{errors.New("secret /private/native-data"), 503, RetryActivityUnavailable},
		{&RetryActivityError{Code: RetryActivityUnavailable}, 503, RetryActivityUnavailable},
		{fmt.Errorf("secret: %w", &RetryActivityError{Code: RetryActionStale}), 409, RetryActionStale},
		{&RetryActivityError{Code: RetryReloadRequired}, 409, RetryReloadRequired},
		{&RetryActivityError{Code: "secret"}, 503, RetryActivityUnavailable},
		{(*RetryActivityError)(nil), 503, RetryActivityUnavailable},
	} {
		t.Run(tc.code+fmt.Sprint(tc.err), func(t *testing.T) {
			var output bytes.Buffer
			f := &activityFixture{err: tc.err}
			h := activityHandler(t, f, true, &retryFixtureLock{}, &output)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", "/codex/conversations/opaque/retry/activity", nil))
			if w.Code != tc.status || !strings.Contains(w.Body.String(), tc.code) || strings.Contains(w.Body.String()+output.String(), "secret") || strings.Contains(w.Body.String()+output.String(), "private") {
				t.Fatalf("status=%d body=%s log=%s", w.Code, w.Body.String(), output.String())
			}
		})
	}
}

func TestRetryActivityDTOValidation(t *testing.T) {
	for name, change := range map[string]func(*RetryActivityStatus){
		"unsafe clock":                func(s *RetryActivityStatus) { s.ServerNowMS = maxSafeJSONInteger + 1 },
		"missing clock":               func(s *RetryActivityStatus) { s.ServerNowMS = 0 },
		"chain":                       func(s *RetryActivityStatus) { s.Activity.ChainKey = "../x" },
		"class":                       func(s *RetryActivityStatus) { s.Activity.Class = "unknown" },
		"phase":                       func(s *RetryActivityStatus) { s.Activity.Phase = "idle" },
		"negative count":              func(s *RetryActivityStatus) { s.Activity.ConfirmedStarts = activityNumber(-1) },
		"unsafe count":                func(s *RetryActivityStatus) { s.Activity.ConfirmedStarts = activityNumber(maxSafeJSONInteger + 1) },
		"null exact count":            func(s *RetryActivityStatus) { s.Activity.ConfirmedStarts = nil },
		"unknown quality":             func(s *RetryActivityStatus) { s.Activity.CountQuality = "approximate" },
		"invented unavailable count":  func(s *RetryActivityStatus) { s.Activity.CountQuality = "unavailable" },
		"unconfirmed":                 func(s *RetryActivityStatus) { s.Activity.Unconfirmed = 2 },
		"half timing":                 func(s *RetryActivityStatus) { s.Activity.StartedAtMS = nil },
		"negative elapsed":            func(s *RetryActivityStatus) { s.Activity.ElapsedMS = activityNumber(-1) },
		"invented unavailable timing": func(s *RetryActivityStatus) { s.Activity.TimingQuality = "unavailable" },
		"active finished":             func(s *RetryActivityStatus) { s.Activity.FinishedAtMS = activityNumber(100000) },
		"active outcome":              func(s *RetryActivityStatus) { s.Activity.Outcome = "success" },
		"reason bound":                func(s *RetryActivityStatus) { s.Activity.Reason = strings.Repeat("x", 1025) },
		"invalid utf8":                func(s *RetryActivityStatus) { s.Activity.Reason = string([]byte{255}) },
		"token":                       func(s *RetryActivityStatus) { s.Activity.StopToken = "private/path" },
		"running now":                 func(s *RetryActivityStatus) { s.Activity.Phase = "running"; s.Activity.NextAttemptAtMS = nil },
		"running deadline":            func(s *RetryActivityStatus) { s.Activity.Phase = "running"; s.Activity.RetryNowToken = "" },
		"terminal actions":            func(s *RetryActivityStatus) { s.Activity.Phase = "terminal"; s.Activity.Outcome = "success" },
	} {
		t.Run(name, func(t *testing.T) {
			s := activityState()
			change(&s)
			if validRetryActivityStatus(s) {
				t.Fatal("accepted invalid DTO")
			}
		})
	}
	for _, phase := range []string{"scheduled", "submitting", "reconciling", "running", "terminal"} {
		s := activityState()
		a := s.Activity
		a.Phase = phase
		a.NextAttemptAtMS = nil
		a.RetryNowToken = ""
		a.ConfirmedStarts = nil
		a.CountQuality = "unavailable"
		a.StartedAtMS = nil
		a.ElapsedMS = nil
		a.TimingQuality = "unavailable"
		if phase == "terminal" {
			a.Outcome = "stopped"
			a.StopToken = ""
		}
		if !validRetryActivityStatus(s) {
			t.Fatalf("rejected valid unavailable %s", phase)
		}
	}
	var output bytes.Buffer
	f := &activityFixture{state: RetryActivityStatus{ServerNowMS: 100000}}
	h := activityHandler(t, f, true, &retryFixtureLock{}, &output)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/codex/conversations/opaque/retry/activity", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"activity":null`) {
		t.Fatal(w.Body.String())
	}
	bad := activityState()
	bad.Activity.Class = "private"
	f.state = bad
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/codex/conversations/opaque/retry/activity", nil))
	if w.Code != 503 || strings.Contains(w.Body.String()+output.String(), "private") {
		t.Fatalf("body=%s log=%s", w.Body.String(), output.String())
	}
}

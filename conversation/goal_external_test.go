package conversation_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aither64/codex-web/conversation"
	"github.com/aither64/codex-web/conversation/backend"
)

type goalLock struct {
	held  bool
	locks int
	check func(context.Context) error
}

func (l *goalLock) Lock(ctx context.Context) error {
	if l.check != nil {
		if err := l.check(ctx); err != nil {
			return err
		}
	}
	if l.held {
		panic("duplicate lock acquisition")
	}
	l.held = true
	l.locks++
	return nil
}

func (l *goalLock) Unlock() {
	if !l.held {
		panic("unlock without acquisition")
	}
	l.held = false
}

type goalFixture struct {
	t       *testing.T
	id      backend.Identity
	lock    *goalLock
	value   backend.GoalSnapshot
	readErr error
	mutate  func(backend.GoalMutation) (backend.GoalSnapshot, error)
	reads   int
	writes  int
}

func (f *goalFixture) ReadGoal(ctx context.Context, id backend.Identity) (backend.GoalSnapshot, error) {
	f.reads++
	if id != f.id || f.lock.held || ctx.Err() != nil {
		f.t.Fatal("read was not passive or did not get the trusted identity/context")
	}
	return f.value, f.readErr
}

func (f *goalFixture) MutateGoal(ctx context.Context, id backend.Identity, m backend.GoalMutation) (backend.GoalSnapshot, error) {
	f.writes++
	if id != f.id || !f.lock.held || ctx.Err() != nil {
		f.t.Fatal("write was not admitted under the trusted identity/context/lock")
	}
	if _, ok := ctx.Deadline(); !ok {
		f.t.Fatal("write lost operation deadline")
	}
	if f.mutate != nil {
		return f.mutate(m)
	}
	return f.value, nil
}

func withGoalFixture(t *testing.T) (*conversation.Handler, *goalFixture, *conversation.BackendTarget, *int) {
	t.Helper()
	h, other, target, releases := neutralFixture(t)
	f := &goalFixture{t: t, id: target.Identity, lock: &goalLock{}}
	f.value = backend.GoalSnapshot{
		ConversationID: f.id.ConversationID, Generation: f.id.Generation,
		Enabled: true, ChangeToken: "current", AllowedActions: []backend.GoalAction{backend.GoalDisable},
	}
	target.Capabilities.GoalRead, target.Capabilities.GoalWrite = true, true
	target.Operations.GoalRead, target.Operations.GoalWrite = f, f
	target.MutationLock = f.lock
	t.Cleanup(func() {
		if f.lock.held || len(other.commands)+len(other.sends) != 0 {
			t.Error("leaked mutation lock or used a non-goal operation")
		}
	})
	return h, f, target, releases
}

const goalPost = `{"action":"edit","operationId":"` + attempt + `","expectedChangeToken":"current","objective":"CSV only"}`

func TestGoalRequiresSeparateGrantAndImplementation(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		for _, grant := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/grant=%v", method, grant), func(t *testing.T) {
				h, f, target, releases := withGoalFixture(t)
				if method == http.MethodGet {
					target.Capabilities.GoalRead = grant
					if grant {
						target.Operations.GoalRead = nil
					}
				} else {
					target.Capabilities.GoalWrite = grant
					if grant {
						target.Operations.GoalWrite = nil
					}
				}
				want := http.StatusForbidden
				if grant {
					want = http.StatusNotImplemented
				}
				body := ""
				if method == http.MethodPost {
					body = goalPost
				}
				w := callNeutral(h, method, "goal", body)
				if w.Code != want || f.reads+f.writes != 0 || *releases != 1 {
					t.Fatalf("status=%d want=%d reads=%d writes=%d releases=%d body=%s", w.Code, want, f.reads, f.writes, *releases, w.Body)
				}
			})
		}
	}
}

func TestGoalPassiveReadAndOptionalFields(t *testing.T) {
	h, f, target, releases := withGoalFixture(t)
	w := callNeutral(h, http.MethodGet, "goal", "")
	if w.Code != http.StatusOK || w.Body.String() != "{\"changeToken\":\"current\",\"enabled\":true,\"allowedActions\":[\"disable\"]}\n" ||
		w.Header().Get("Cache-Control") != "no-store" || f.reads != 1 || f.writes != 0 || f.lock.locks != 0 || *releases != 1 {
		t.Fatalf("unexpected passive read: %d %s", w.Code, w.Body)
	}
	target.Capabilities.GoalWrite = false
	pending := false
	f.value.Goal = &backend.Goal{Objective: "Export selected rows", Status: backend.GoalPaused, Progress: "CSV ready", Next: "Verify JSON", Reason: "operator paused", PendingApplication: &pending}
	w = callNeutral(h, http.MethodGet, "goal", "")
	var got backend.GoalSnapshot
	if err := json.Unmarshal(w.Body.Bytes(), &got); w.Code != http.StatusOK || err != nil || got.Goal == nil ||
		got.Goal.Status != backend.GoalPaused || got.Goal.Objective != f.value.Goal.Objective || got.Goal.Progress != f.value.Goal.Progress ||
		got.Goal.Next != f.value.Goal.Next || got.Goal.Reason != f.value.Goal.Reason || got.Goal.PendingApplication == nil || *got.Goal.PendingApplication ||
		got.AllowedActions == nil || len(got.AllowedActions) != 0 || f.reads != 2 || f.writes != 0 || f.lock.locks != 0 || *releases != 2 {
		t.Fatalf("read-only goal view mismatch: %d %s", w.Code, w.Body)
	}
}

func TestGoalRejectsWrongSnapshotScope(t *testing.T) {
	for _, scope := range []string{"conversation", "generation", "missing-generation"} {
		t.Run(scope, func(t *testing.T) {
			h, f, target, releases := withGoalFixture(t)
			f.value.Goal = &backend.Goal{Objective: "must not leak", Status: backend.GoalActive}
			switch scope {
			case "conversation":
				f.value.ConversationID = "other-target"
			case "generation":
				f.value.Generation = "other-generation"
			case "missing-generation":
				target.Identity.Generation = ""
			}
			f.mutate = func(backend.GoalMutation) (backend.GoalSnapshot, error) {
				return f.value, backend.ErrGoalConflict
			}
			for _, method := range []string{http.MethodGet, http.MethodPost} {
				body := ""
				if method == http.MethodPost {
					body = goalPost
				}
				w := callNeutral(h, method, "goal", body)
				if w.Code != http.StatusServiceUnavailable || strings.Contains(w.Body.String(), f.value.Goal.Objective) {
					t.Fatalf("unscoped snapshot escaped via %s: %d %s", method, w.Code, w.Body)
				}
			}
			if *releases != 2 || scope == "missing-generation" && f.reads+f.writes != 0 {
				t.Fatalf("releases=%d reads=%d writes=%d", *releases, f.reads, f.writes)
			}
		})
	}
}

func TestGoalAdmissionValidatesOriginScopeAndRequest(t *testing.T) {
	h, f, _, releases := withGoalFixture(t)
	for _, tc := range []struct {
		name, method, path, origin, body string
		status, releases                 int
	}{
		{"origin", "POST", "application-id/goal", "https://portal.test.invalid", goalPost, 403, 0},
		{"no origin", "POST", "application-id/goal", "", goalPost, 403, 0},
		{"scope", "POST", "unknown-id/goal", "https://portal.test", goalPost, 404, 0},
		{"method", "DELETE", "application-id/goal", "https://portal.test", goalPost, 405, 0},
		{"head", "HEAD", "application-id/goal", "", "", 405, 0},
		{"query", "GET", "application-id/goal?conversationId=other", "", "", 400, 1},
		{"get body", "GET", "application-id/goal", "", goalPost, 400, 1},
		{"post query", "POST", "application-id/goal?generation=other", "https://portal.test", goalPost, 400, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := *releases
			r := httptest.NewRequest(tc.method, "/codex/conversations/"+tc.path, strings.NewReader(tc.body))
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.status || *releases-before != tc.releases || f.reads+f.writes != 0 || f.lock.held ||
				tc.status == 405 && w.Header().Get("Allow") != "GET, POST" {
				t.Fatalf("status=%d want=%d releases=%d read/write=%d/%d", w.Code, tc.status, *releases-before, f.reads, f.writes)
			}
		})
	}
}

func TestGoalRejectsMalformedMutation(t *testing.T) {
	h, f, _, releases := withGoalFixture(t)
	for _, tc := range []struct{ name, body string }{
		{"array", `[]`}, {"null", `null`}, {"empty", ""}, {"trailing", goalPost + ` {}`},
		{"private identity", strings.Replace(goalPost, `"action"`, `"generation":"other","action"`, 1)},
		{"duplicate", strings.Replace(goalPost, `"action"`, `"action":"resume","action"`, 1)},
		{"case-folded", strings.Replace(goalPost, `"operationId"`, `"OperationId"`, 1)},
		{"uppercase id", strings.Replace(goalPost, attempt, strings.ToUpper(attempt), 1)},
		{"empty token", strings.Replace(goalPost, `"current"`, `""`, 1)},
		{"long token", strings.Replace(goalPost, "current", strings.Repeat("a", backend.MaxGoalChangeTokenBytes+1), 1)},
		{"invalid action", strings.Replace(goalPost, `"edit"`, `"create"`, 1)},
		{"missing objective", strings.Replace(goalPost, `,"objective":"CSV only"`, ``, 1)},
		{"null objective", strings.Replace(goalPost, `"CSV only"`, `null`, 1)},
		{"blank objective", strings.Replace(goalPost, `CSV only`, `\n \t`, 1)},
		{"NUL objective", strings.Replace(goalPost, `CSV only`, `\u0000CSV`, 1)},
		{"long UTF-8 objective", strings.Replace(goalPost, "CSV only", strings.Repeat("é", backend.MaxGoalObjectiveBytes/2+1), 1)},
		{"invalid UTF-8 objective", strings.Replace(goalPost, "CSV only", "invalid\xff", 1)},
		{"unexpected objective", strings.Replace(goalPost, `"edit"`, `"enable"`, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := *releases
			w := callNeutral(h, http.MethodPost, "goal", tc.body)
			if w.Code != http.StatusBadRequest || f.reads+f.writes != 0 || *releases-before != 1 || f.lock.held {
				t.Fatalf("invalid mutation dispatched: %d %s", w.Code, w.Body)
			}
		})
	}
	w := callNeutral(h, http.MethodPost, "goal", goalPost+strings.Repeat(" ", 32*1024))
	if w.Code != http.StatusRequestEntityTooLarge || f.writes != 0 || f.lock.held {
		t.Fatalf("oversized body dispatched: %d %s", w.Code, w.Body)
	}
}

func TestGoalNonEditOperatorActions(t *testing.T) {
	for _, action := range []backend.GoalAction{backend.GoalEnable, backend.GoalDisable, backend.GoalPause, backend.GoalResume} {
		t.Run(string(action), func(t *testing.T) {
			h, f, _, releases := withGoalFixture(t)
			f.mutate = func(m backend.GoalMutation) (backend.GoalSnapshot, error) {
				if m.Action != action || m.OperationID != attempt || m.ExpectedChangeToken != "current" || m.Objective != "" {
					t.Fatalf("wrong admitted mutation: %+v", m)
				}
				return f.value, nil
			}
			body := fmt.Sprintf(`{"action":%q,"operationId":%q,"expectedChangeToken":"current"}`, action, attempt)
			w := callNeutral(h, http.MethodPost, "goal", body)
			if w.Code != http.StatusOK || f.reads != 0 || f.writes != 1 || *releases != 1 {
				t.Fatalf("action not admitted once: %d %s", w.Code, w.Body)
			}
		})
	}
}

func TestGoalMutationReplayAndConflictAreDomainOwned(t *testing.T) {
	h, f, target, releases := withGoalFixture(t)
	target.Capabilities.GoalRead, target.Operations.GoalRead = false, nil
	f.value.Goal = &backend.Goal{Objective: "old text", Status: backend.GoalPaused}
	// This fixture keeps only one receipt. It deliberately omits edit from the
	// presentation list to prove that the list does not gate admission or replay.
	var applied backend.GoalMutation
	effects := 0
	f.mutate = func(m backend.GoalMutation) (backend.GoalSnapshot, error) {
		if m.OperationID == applied.OperationID {
			if m == applied {
				return f.value, nil
			}
			return f.value, backend.ErrGoalConflict
		}
		if m.ExpectedChangeToken != f.value.ChangeToken {
			return f.value, backend.ErrGoalConflict
		}
		if m.Action != backend.GoalEdit {
			t.Fatalf("wrong action: %v", m.Action)
		}
		applied = m
		effects++
		f.value.ChangeToken = "new token"
		f.value.Goal.Objective = m.Objective
		return f.value, nil
	}
	exact := " <b>literal</b>\n" + strings.Repeat("é", (backend.MaxGoalObjectiveBytes-16)/2)
	encoded, err := json.Marshal(exact)
	if err != nil {
		t.Fatal(err)
	}
	post := strings.Replace(goalPost, `"CSV only"`, string(encoded), 1)
	for i, tc := range []struct {
		body string
		want int
	}{
		{post, http.StatusOK},
		{post, http.StatusOK},
		{goalPost, http.StatusConflict},
		{strings.Replace(goalPost, attempt, "6f800544-21a8-465c-905d-1e825f477cb9", 1), http.StatusConflict},
	} {
		w := callNeutral(h, http.MethodPost, "goal", tc.body)
		var got backend.GoalSnapshot
		var decodeErr error
		if tc.want == http.StatusConflict {
			var body struct {
				Code     string               `json:"code"`
				Snapshot backend.GoalSnapshot `json:"snapshot"`
			}
			decodeErr = json.Unmarshal(w.Body.Bytes(), &body)
			got = body.Snapshot
			if body.Code != "goal_conflict" {
				t.Fatalf("missing conflict disposition: %d %s", w.Code, w.Body)
			}
		} else {
			decodeErr = json.Unmarshal(w.Body.Bytes(), &got)
		}
		if w.Code != tc.want || decodeErr != nil || got.ChangeToken != "new token" || got.Goal == nil ||
			got.Goal.Status != backend.GoalPaused || got.Goal.Objective != exact || effects != 1 || f.reads != 0 ||
			f.writes != i+1 || f.lock.locks != i+1 || f.lock.held || *releases != i+1 {
			t.Fatalf("request %d: status=%d want=%d decode=%v writes=%d effects=%d releases=%d", i, w.Code, tc.want, decodeErr, f.writes, effects, *releases)
		}
	}
}

func TestGoalUnavailableUnsupportedAndCancellation(t *testing.T) {
	h, f, _, releases := withGoalFixture(t)
	for _, tc := range []struct {
		cause error
		code  string
		want  int
	}{
		{backend.ErrGoalUnsupported, "goal_unsupported", 501},
		{backend.ErrGoalUnavailable, "goal_unavailable", 503},
		{errors.New("private diagnostics"), "goal_unavailable", 503},
	} {
		f.readErr = fmt.Errorf("adapter: %w", tc.cause)
		w := callNeutral(h, http.MethodGet, "goal", "")
		if w.Code != tc.want || !strings.Contains(w.Body.String(), tc.code) || strings.Contains(w.Body.String(), "private diagnostics") {
			t.Fatalf("invalid disposition: %d %s", w.Code, w.Body)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.lock.check = func(context.Context) error { cancel(); return nil }
	r := httptest.NewRequest(http.MethodPost, "/codex/conversations/application-id/goal", strings.NewReader(goalPost)).WithContext(ctx)
	r.Header.Set("Origin", "https://portal.test")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusServiceUnavailable || f.writes != 0 || f.lock.locks != 1 || f.lock.held || *releases != 4 {
		t.Fatalf("canceled mutation dispatched or leaked admission: %d %s", w.Code, w.Body)
	}
}

type legacyGoalClient struct {
	conversation.Client
	checks, activeReads int
}

func (c *legacyGoalClient) VerifyThread(_ context.Context, id, directory string) error {
	c.checks++
	if id != "legacy-thread" || directory != "/trusted" {
		return errors.New("incorrect legacy scope")
	}
	return nil
}

func (c *legacyGoalClient) HasActiveGoal(context.Context, string) (bool, error) {
	c.activeReads++
	return true, nil
}

func TestLegacyGoalBooleanDoesNotAdvertiseSharedGoal(t *testing.T) {
	client := &legacyGoalClient{}
	lock := &goalLock{}
	var releases int
	h, err := conversation.NewHandler(conversation.Options{
		AllowedOrigins: []string{"https://portal.test"},
		Resolver: conversation.ResolverFunc(func(context.Context, conversation.ResolveRequest) (conversation.Target, error) {
			return conversation.Target{Client: client, ThreadID: "legacy-thread", Directory: "/trusted", Capabilities: conversation.Capabilities{Read: true, Send: true, Settings: true}, MutationLock: lock, Release: func() { releases++ }}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	read := callNeutral(h, http.MethodGet, "goal", "")
	write := callNeutral(h, http.MethodPost, "goal", goalPost)
	if read.Code != http.StatusForbidden || write.Code != http.StatusForbidden || client.checks != 2 || client.activeReads != 0 || releases != 2 || lock.held {
		t.Fatalf("legacy goal fallback: statuses=%d/%d checks=%d active goal reads=%d releases=%d", read.Code, write.Code, client.checks, client.activeReads, releases)
	}
}

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
	"reflect"
	"strings"
	"testing"

	"github.com/aither64/codex-web/codex"
	"github.com/aither64/codex-web/conversation/backend"
)

type queueFailureClient struct {
	*fakeClient
	err   error
	order *[]string
}

func (client *queueFailureClient) Queue(context.Context, string, string, string) (codex.QueueEntry, error) {
	*client.order = append(*client.order, "native")
	return codex.QueueEntry{ID: "accepted-entry"}, client.err
}

type queueFailureAttachments struct {
	AttachmentProvider
	err   error
	order *[]string
}

func (provider queueFailureAttachments) Prepare(_ context.Context, _, _, text string, _ []string) (string, error) {
	*provider.order = append(*provider.order, "prepare")
	return text, nil
}

func (provider queueFailureAttachments) ObserveQueue(_ context.Context, entries []codex.QueueEntry) error {
	*provider.order = append(*provider.order, "observe")
	entries[0].Text = "observed native entry"
	return provider.err
}

func TestLegacyQueueErrorStagePreservesHTTPContract(t *testing.T) {
	for _, fixture := range []struct {
		name                string
		native, observation error
		status              int
		message             string
	}{
		{"native", errors.New("private native failure"), nil, 503, "Codex service is unavailable"},
		{"native typed upload", fmt.Errorf("native cause: %w", &UploadError{Status: 400, Message: "private native upload failure"}), nil, 503, "Codex service is unavailable"},
		{"observation", nil, errors.New("private filesystem failure"), 500, "file operation failed; retry shortly"},
		{"observation typed upload", nil, fmt.Errorf("observation cause: %w", &UploadError{Status: 409, Message: "Attachment is unavailable"}), 409, "Attachment is unavailable"},
		{"success", nil, nil, 202, ""},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			var order []string
			var logs bytes.Buffer
			client := &queueFailureClient{fakeClient: &fakeClient{}, err: fixture.native, order: &order}
			target := Target{Client: client, ThreadID: "trusted", Directory: "/workspace", Capabilities: Capabilities{Queue: true},
				MutationLock: NewMutationLock(), Attachments: queueFailureAttachments{err: fixture.observation, order: &order}}
			handler, err := NewHandler(Options{AllowedOrigins: []string{"https://portal.test"}, Logger: log.New(&logs, "", 0),
				Resolver: ResolverFunc(func(context.Context, ResolveRequest) (Target, error) { return target, nil })})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "/codex/conversations/opaque/queue", strings.NewReader(`{"message":"later","clientUserMessageId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}`))
			request.Header.Set("Origin", "https://portal.test")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != fixture.status {
				t.Fatalf("status=%d, want=%d: %s", response.Code, fixture.status, response.Body.String())
			}
			wantOrder := []string{"prepare", "native"}
			if fixture.native == nil {
				wantOrder = append(wantOrder, "observe")
			}
			if !reflect.DeepEqual(order, wantOrder) || strings.Contains(response.Body.String(), "private") {
				t.Fatalf("callback order or error sanitization changed: order=%v body=%s", order, response.Body.String())
			}
			if fixture.message == "" {
				var entry codex.QueueEntry
				if err := json.Unmarshal(response.Body.Bytes(), &entry); err != nil || entry.ID != "accepted-entry" || entry.Text != "observed native entry" {
					t.Fatalf("original native queue observation was not projected: %+v %v", entry, err)
				}
				return
			}
			var body map[string]string
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || !reflect.DeepEqual(body, map[string]string{"error": fixture.message}) {
				t.Fatalf("error body=%s err=%v", response.Body.String(), err)
			}
			cause := fixture.native
			if cause == nil {
				cause = fixture.observation
			} else if !strings.Contains(logs.String(), cause.Error()) {
				t.Fatalf("native error cause was lost from server log: %s", logs.String())
			}
			adapted := adaptLegacyTarget(target)
			_, err = adapted.Operations.QueueWrite.Queue(context.Background(), adapted.Identity, backend.QueueRequest{Message: "later"})
			if !errors.Is(err, cause) {
				t.Fatalf("adapter lost original cause: %v", err)
			}
			var upload *UploadError
			if errors.As(cause, &upload) {
				var retained *UploadError
				if !errors.As(err, &retained) || retained != upload {
					t.Fatal("adapter lost typed upload cause")
				}
			}
		})
	}
}

func TestNeutralQueueErrorMappingUnchanged(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/queue", nil)
	handler := &Handler{logger: log.New(&bytes.Buffer{}, "", 0)}
	for _, fixture := range []struct {
		err    error
		status int
	}{
		{&UploadError{Status: 400, Message: "invalid attachment"}, 400},
		{errors.New("private neutral failure"), 503},
	} {
		response := httptest.NewRecorder()
		handler.queueError(response, request, fixture.err)
		if response.Code != fixture.status || strings.Contains(response.Body.String(), "private") {
			t.Fatalf("neutral mapping changed: %d %s", response.Code, response.Body.String())
		}
	}
}

package conversation

import (
	"context"
	"errors"
	"github.com/aither64/codex-web/codex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type uploadFixture struct {
	UploadStore
	calls int
	body  []byte
	err   error
}

func (store *uploadFixture) Limits() UploadLimits                   { return UploadLimits{ChunkBytes: 4} }
func (store *uploadFixture) List(context.Context) ([]Upload, error) { return nil, nil }
func (store *uploadFixture) Create(_ context.Context, input UploadRequest) (Upload, error) {
	store.calls++
	return Upload{}, store.err
}
func (store *uploadFixture) Append(_ context.Context, _ string, offset int64, checksum string, body io.Reader) (Upload, error) {
	store.calls++
	data, err := io.ReadAll(body)
	store.body = data
	return Upload{}, err
}

type readSeekCloser struct{ *strings.Reader }

func (r readSeekCloser) Close() error { return nil }
func (store *uploadFixture) Open(context.Context, string) (UploadContent, error) {
	return UploadContent{File: readSeekCloser{strings.NewReader("<script>alert(1)</script>")}, Name: "input.html", Modified: time.Now()}, nil
}
func TestUploadTransportBoundsAndOrigin(t *testing.T) {
	store := &uploadFixture{}
	resolved, released := 0, 0
	handler, err := NewUploadHandler(UploadOptions{BasePath: "/uploads", AllowedOrigins: []string{"https://portal.test"}, Resolve: func(ctx context.Context, scope string, mutation bool) (UploadTarget, error) {
		resolved++
		if scope != "scope" {
			return UploadTarget{}, &UploadError{Status: 404, Message: "Unavailable"}
		}
		return UploadTarget{Store: store, Writable: true, Release: func() { released++ }}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	call := func(method, path, origin, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		req.Header.Set("Upload-Offset", "0")
		req.Header.Set("Upload-Checksum", strings.Repeat("0", 64))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		return response
	}
	for _, origin := range []string{"", "https://evil.test", "https://portal.test.evil.test"} {
		if response := call("POST", "/uploads/scope", origin, `{"name":"a","size":1}`); response.Code != 403 {
			t.Fatal(response.Code)
		}
	}
	if resolved != 0 {
		t.Fatal("resolved unauthorized upload")
	}
	for _, path := range []string{"/uploads/scope/%2e%2e", "/uploads//id", "/uploads/scope/id/extra/content", "/uploads/scope/%2Fetc"} {
		if response := call("GET", path, "", ""); response.Code != 404 {
			t.Fatal(path, response.Code)
		}
	}
	response := call("PATCH", "/uploads/scope/file", "https://portal.test", "12345")
	if response.Code < 400 || len(store.body) > 4 {
		t.Fatal("unbounded chunk", response.Code, len(store.body))
	}
	before := store.calls
	response = call("POST", "/uploads/scope", "https://portal.test", `{"name":"a","unknown":true}`)
	if response.Code != 400 || store.calls != before {
		t.Fatal("unknown field accepted")
	}
	response = call("POST", "/uploads/scope", "https://portal.test", `{"name":"`+strings.Repeat("a", 4096)+`"}`)
	if response.Code != 400 || store.calls != before {
		t.Fatal("unbounded JSON")
	}
	store.err = &UploadError{Status: 413, Message: "Limit reached"}
	response = call("POST", "/uploads/scope", "https://portal.test", `{"name":"a","size":1}`)
	if response.Code != 413 || !strings.Contains(response.Body.String(), "Limit reached") {
		t.Fatal(response.Body.String())
	}
	response = call("GET", "/uploads/scope/file/content", "", "")
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/octet-stream" || !strings.HasPrefix(response.Header().Get("Content-Disposition"), "attachment;") || response.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal(response.Header())
	}
	if released != resolved {
		t.Fatal("resolver leases leaked", resolved, released)
	}
}

type attachmentFixture struct {
	AttachmentProvider
	kind, attempt, text string
	ids                 []string
	queued              bool
}

func (provider *attachmentFixture) Prepare(_ context.Context, kind, attempt, text string, ids []string) (string, error) {
	provider.kind = kind
	provider.attempt = attempt
	provider.text = text
	provider.ids = ids
	return "server-resolved prompt", nil
}
func (provider *attachmentFixture) ObserveQueue(context.Context, []codex.QueueEntry) error {
	provider.queued = true
	return nil
}
func TestAttachmentOnlyMessageUsesProviderBeforeSubmission(t *testing.T) {
	for _, operation := range []string{"message", "queue"} {
		client := &fakeClient{}
		provider := &attachmentFixture{}
		handler, err := NewHandler(Options{BasePath: "/codex", AllowedOrigins: []string{"https://portal.test"}, Resolver: ResolverFunc(func(context.Context, ResolveRequest) (Target, error) {
			return Target{Client: client, ThreadID: "trusted", Directory: "/workspace", MutationLock: NewMutationLock(), Capabilities: Capabilities{Send: true, Queue: true}, Attachments: provider}, nil
		})})
		if err != nil {
			t.Fatal(err)
		}
		body := `{"message":"","clientUserMessageId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","attachmentIds":["bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"]}`
		req := httptest.NewRequest("POST", "/codex/conversations/opaque/"+operation, strings.NewReader(body))
		req.Header.Set("Origin", "https://portal.test")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code != 202 || len(provider.ids) != 1 || provider.text != "" {
			t.Fatal(response.Code, response.Body.String(), provider)
		}
		if operation == "message" && client.sentText != "server-resolved prompt" {
			t.Fatal(client.sentText)
		}
		if operation == "queue" && !provider.queued {
			t.Fatal("accepted queue was not associated before acknowledgement")
		}
	}
}

type completingQueueClient struct {
	*fakeClient
	pending bool
}

func (client *completingQueueClient) DeleteQueueEntryWithCompletion(ctx context.Context, thread, id string, complete func() error) error {
	client.pending = true
	if err := complete(); err != nil {
		return err
	}
	client.pending = false
	return nil
}
func (client *completingQueueClient) ReconcileQueueDeletionsWithCompletion(ctx context.Context, thread string, complete func(string) error) error {
	if client.pending {
		if err := complete("queued-1"); err != nil {
			return err
		}
		client.pending = false
	}
	return nil
}

type failingQueueProvider struct {
	attachmentFixture
	fail    bool
	deleted int
}

func (provider *failingQueueProvider) QueueDeleted(context.Context, string) error {
	if provider.fail {
		return errors.New("catalog unavailable")
	}
	provider.deleted++
	return nil
}
func TestAttachmentQueueDeletionRecoversOnRefresh(t *testing.T) {
	client := &completingQueueClient{fakeClient: &fakeClient{}}
	provider := &failingQueueProvider{fail: true}
	handler, err := NewHandler(Options{BasePath: "/codex", AllowedOrigins: []string{"https://portal.test"}, Resolver: ResolverFunc(func(context.Context, ResolveRequest) (Target, error) {
		return Target{Client: client, Attachments: provider, ThreadID: "trusted", Directory: "/workspace", MutationLock: NewMutationLock(), Capabilities: Capabilities{Queue: true, QueueRead: true}}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	call := func(method, path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "/codex/conversations/opaque/queue"+path, nil)
		req.Header.Set("Origin", "https://portal.test")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		return response
	}
	if response := call("DELETE", "/queued-1"); response.Code != 503 || !client.pending {
		t.Fatal(response.Code, response.Body.String())
	}
	provider.fail = false
	if response := call("GET", ""); response.Code != 200 || client.pending || provider.deleted != 1 {
		t.Fatal(response.Code, response.Body.String())
	}
}

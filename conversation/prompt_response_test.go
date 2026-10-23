package conversation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/aither64/codex-web/codex"
)

type boundPromptClient struct {
	fakeClient
	response codex.PromptResponse
	thread   string
	err      error
}

func TestBoundPromptHTTPMultiQuestionAnswerContract(t *testing.T) {
	for _, test := range []struct {
		name, payload string
		status        int
	}{
		{"complete with unanswered optional", `"answers":{"required":{"answers":["First","Details"]},"optional":{"answers":[]}}`, 200},
		{"mixed action", `"answers":{"required":{"answers":["First"]}},"decision":"accept"`, 400},
		{"malformed answer", `"answers":{"required":{"answers":"First"}}`, 400},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &boundPromptClient{}
			handler := &Handler{maxBodyBytes: defaultMaxBodyBytes, logger: log.New(&bytes.Buffer{}, "", 0)}
			request := httptest.NewRequest("POST", "/respond", bytes.NewBufferString(`{"id":"questions","token":"same-offer",`+test.payload+`}`))
			response := httptest.NewRecorder()
			handler.respond(response, request, Target{Client: client, ThreadID: "trusted-thread", Capabilities: Capabilities{Respond: true}})
			if response.Code != test.status {
				t.Fatalf("response: %d %s", response.Code, response.Body)
			}
			if test.status != 200 {
				if client.response.ID != "" {
					t.Fatal("invalid answers reached responder")
				}
				return
			}
			expected := map[string]map[string][]string{"required": {"answers": {"First", "Details"}}, "optional": {"answers": {}}}
			if client.thread != "trusted-thread" || client.response.Token != "same-offer" || !reflect.DeepEqual(client.response.Answers, expected) {
				t.Fatalf("changed answer tuple: %#v", client)
			}
		})
	}
}

func (c *boundPromptClient) RespondPrompt(_ context.Context, thread string, response codex.PromptResponse) error {
	c.thread, c.response = thread, response
	return c.err
}

func TestBoundPromptHTTPResponses(t *testing.T) {
	for _, test := range []struct {
		code    string
		notSent bool
		status  int
	}{
		{"", false, 200}, {"prompt_changed", true, 409}, {"invalid_response", true, 400},
		{"prompt_transport", true, 503}, {"prompt_transport", false, 503},
	} {
		t.Run(fmt.Sprintf("%s_%d_not_sent_%t", test.code, test.status, test.notSent), func(t *testing.T) {
			client := &boundPromptClient{}
			if test.code != "" {
				client.err = &codex.PromptResponseError{Code: test.code, NotSent: test.notSent, Err: errors.New("request changed")}
			}
			handler := &Handler{maxBodyBytes: defaultMaxBodyBytes, logger: log.New(&bytes.Buffer{}, "", 0)}
			request := httptest.NewRequest("POST", "/respond", bytes.NewBufferString(`{"id":"request-1","token":"offer-token","answers":{"choice":{"answers":["First"]}}}`))
			response := httptest.NewRecorder()
			handler.respond(response, request, Target{Client: client, ThreadID: "trusted-thread", Capabilities: Capabilities{Respond: true}})
			if response.Code != test.status {
				t.Fatalf("response: %d %s", response.Code, response.Body)
			}
			if client.response.Token != "offer-token" || client.thread != "trusted-thread" {
				t.Fatalf("unbound action: %#v", client)
			}
			if test.code != "" {
				var payload struct {
					Code    string `json:"code"`
					NotSent bool   `json:"notSent"`
				}
				if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
					t.Fatal(err)
				}
				if payload.Code != test.code || payload.NotSent != test.notSent {
					t.Fatalf("classification: %#v", payload)
				}
			}
		})
	}
}

func TestBoundPromptCannotFallBackToAnUnboundLegacyClient(t *testing.T) {
	handler := &Handler{maxBodyBytes: defaultMaxBodyBytes}
	request := httptest.NewRequest("POST", "/respond", bytes.NewBufferString(`{"id":"request-1","token":"offer-token","snooze":true}`))
	response := httptest.NewRecorder()
	handler.respond(response, request, Target{Client: &fakeClient{}, Capabilities: Capabilities{Respond: true}})
	if response.Code != 409 || !bytes.Contains(response.Body.Bytes(), []byte("reload_required")) {
		t.Fatalf("response: %d %s", response.Code, response.Body)
	}
}

func TestTokenlessHTTPDoesNotReachTheLegacySurfaceOfABoundClient(t *testing.T) {
	for _, action := range []string{`"snooze":true`, `"decision":"accept"`, `"answers":{"choice":{"answers":["First"]}}`} {
		client := &boundPromptClient{}
		handler := &Handler{maxBodyBytes: defaultMaxBodyBytes}
		request := httptest.NewRequest("POST", "/respond", bytes.NewBufferString(`{"id":"reused",`+action+`}`))
		response := httptest.NewRecorder()
		handler.respond(response, request, Target{Client: client, ThreadID: "thread", Capabilities: Capabilities{Respond: true}})
		if response.Code != 409 || !bytes.Contains(response.Body.Bytes(), []byte("reload_required")) || client.response.ID != "" {
			t.Fatalf("tokenless response: %d %s", response.Code, response.Body)
		}
	}
}

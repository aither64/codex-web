package conversation

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/aither64/codex-web/codex"
)

type presentationClient struct{ *fakePageClient }

func (client *presentationClient) ReadThread(_ context.Context, thread string) (codex.Transcript, error) {
	transcript := client.page.Transcript
	transcript.ThreadID = thread
	return transcript, client.err
}

type presentationAttachments struct {
	AttachmentProvider
	observe func(*codex.Transcript) error
}

func (provider presentationAttachments) ObserveTranscript(_ context.Context, transcript *codex.Transcript) error {
	return provider.observe(transcript)
}

func TestTranscriptPresentationOrder(t *testing.T) {
	for _, route := range []string{"thread", "thread/page"} {
		for _, mode := range []string{"plain", "decorated", "empty", "attachment error", "nil hooks", "denied", "identity error"} {
			t.Run(route+"/"+mode, func(t *testing.T) {
				const original = "canonical attachment envelope"
				const messageID = "retained-message-id"
				digest := strings.Repeat("a", 64)
				client := &presentationClient{&fakePageClient{fakeClient: &fakeClient{}, page: codex.TranscriptPage{
					Transcript: codex.Transcript{Entries: []codex.TranscriptEntry{{
						Kind: "userMessage", Text: original, ClientUserMessageID: messageID, ClientUserMessageDigest: digest,
					}}},
				}}}
				if mode == "identity error" {
					client.verifyErr = errors.New("identity unavailable")
				}
				var order []string
				expectedDisplay := original
				target := Target{Client: client, ThreadID: "trusted", Directory: "/workspace", Capabilities: Capabilities{Read: mode != "denied"}}
				if mode != "nil hooks" {
					target.TransformTranscript = func(transcript *codex.Transcript) {
						order = append(order, "transform")
						if transcript.Entries[0].Text != original || transcript.Entries[0].DisplayText != nil {
							t.Fatal("transform did not receive original input")
						}
					}
					target.PresentTranscript = func(transcript *codex.Transcript) {
						order = append(order, "present")
						entry := &transcript.Entries[0]
						display := entry.Text
						if entry.DisplayText != nil {
							display = *entry.DisplayText
						}
						if display != expectedDisplay {
							t.Fatalf("presentation received %q, want %q", display, expectedDisplay)
						}
						entry.HTML = "<p>" + display + "</p>"
						entry.Communication = &codex.TranscriptCommunication{Kind: "team", From: "sender", To: "recipient"}
					}
				}
				if mode == "decorated" || mode == "empty" || mode == "attachment error" {
					expectedDisplay = "displayed attachment prompt"
					if mode == "empty" {
						expectedDisplay = ""
					}
					target.Attachments = presentationAttachments{observe: func(transcript *codex.Transcript) error {
						order = append(order, "attachments")
						if mode == "attachment error" {
							return &UploadError{Status: http.StatusServiceUnavailable, Message: "Attachment unavailable"}
						}
						transcript.Entries[0].DisplayText = &expectedDisplay
						return nil
					}}
				}
				handler, err := NewHandler(Options{AllowedOrigins: []string{"https://portal.test"}, Resolver: ResolverFunc(func(context.Context, ResolveRequest) (Target, error) { return target, nil })})
				if err != nil {
					t.Fatal(err)
				}
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/codex/conversations/opaque/"+route, nil))
				status := http.StatusOK
				wantOrder := []string{"transform", "present"}
				switch mode {
				case "decorated", "empty":
					wantOrder = []string{"transform", "attachments", "present"}
				case "attachment error":
					status, wantOrder = http.StatusServiceUnavailable, []string{"transform", "attachments"}
				case "nil hooks":
					wantOrder = nil
				case "denied":
					status, wantOrder = http.StatusForbidden, nil
				case "identity error":
					status, wantOrder = http.StatusNotFound, nil
				}
				if response.Code != status || !reflect.DeepEqual(order, wantOrder) {
					t.Fatalf("status=%d order=%v body=%s", response.Code, order, response.Body.String())
				}
				if status != http.StatusOK {
					return
				}
				var transcript codex.Transcript
				if err := json.Unmarshal(response.Body.Bytes(), &transcript); err != nil {
					t.Fatal(err)
				}
				entry := transcript.Entries[0]
				if entry.Text != original || entry.ClientUserMessageID != messageID || entry.ClientUserMessageDigest != digest {
					t.Fatalf("canonical delivery identity changed: %#v", entry)
				}
				if mode == "nil hooks" {
					if strings.Contains(response.Body.String(), "communication") || entry.HTML != "" {
						t.Fatal("nil presentation changed output")
					}
				} else if entry.HTML != "<p>"+expectedDisplay+"</p>" || entry.Communication == nil || entry.Communication.From != "sender" || entry.Communication.To != "recipient" {
					t.Fatalf("presentation output lost: %#v", entry)
				}
			})
		}
	}
}

func TestTranscriptPresentationPreservesCitationIdentity(t *testing.T) {
	for _, route := range []string{"thread", "thread/page"} {
		t.Run(route, func(t *testing.T) {
			prefix, token := "Příliš 🐈 ", "\ue200cite\ue202fixture\ue201"
			entry := codex.TranscriptEntry{
				Kind: "agentMessage", Text: prefix + token + " done", TurnStatus: "completed",
				ClientUserMessageID: "original-id", ClientUserMessageDigest: strings.Repeat("b", 64),
				Citations: []codex.CitationSpan{{Start: len(prefix), End: len(prefix + token), Sources: []codex.CitationSource{{URL: "https://example.com/source", Title: "Source", Number: 1}}}},
			}
			client := &presentationClient{&fakePageClient{fakeClient: &fakeClient{}, page: codex.TranscriptPage{Transcript: codex.Transcript{Entries: []codex.TranscriptEntry{entry}}}}}
			handler, err := NewHandler(Options{AllowedOrigins: []string{"https://portal.test"}, Resolver: ResolverFunc(func(context.Context, ResolveRequest) (Target, error) {
				return Target{Client: client, ThreadID: "trusted", Directory: "/workspace", Capabilities: Capabilities{Read: true},
					Attachments: presentationAttachments{observe: func(transcript *codex.Transcript) error {
						value := "decorated display is not a citation offset source"
						transcript.Entries[0].DisplayText = &value
						return nil
					}},
					PresentTranscript: func(transcript *codex.Transcript) {
						transcript.Entries[0].HTML = codex.CitationMarkdown(transcript.Entries[0])
						transcript.Entries[0].Communication = &codex.TranscriptCommunication{Kind: "team"}
					}}, nil
			})})
			if err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/codex/conversations/opaque/"+route, nil))
			var transcript codex.Transcript
			if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &transcript) != nil {
				t.Fatalf("response = %d %s", response.Code, response.Body.String())
			}
			actual := transcript.Entries[0]
			if actual.Text != entry.Text || !reflect.DeepEqual(actual.Citations, entry.Citations) || actual.ClientUserMessageID != entry.ClientUserMessageID || actual.ClientUserMessageDigest != entry.ClientUserMessageDigest {
				t.Fatalf("citation or delivery identity changed: %#v", actual)
			}
			if !strings.HasPrefix(actual.HTML, prefix+"[\\[1\\]](<https://example.com/source>") {
				t.Fatalf("citation did not use original UTF-8 offsets: %q", actual.HTML)
			}
		})
	}
}

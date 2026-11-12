package conversation

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/aither64/codex-web/codex"
	"github.com/aither64/codex-web/conversation/backend"
)

// Populate every public wire field, so a newly added native presentation field
// cannot silently disappear from the independent view or forward projection.
func populateWire(value reflect.Value) {
	switch value.Kind() {
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			if value.Field(i).CanSet() {
				populateWire(value.Field(i))
			}
		}
	case reflect.Pointer:
		value.Set(reflect.New(value.Type().Elem()))
		populateWire(value.Elem())
	case reflect.Slice:
		value.Set(reflect.MakeSlice(value.Type(), 1, 1))
		populateWire(value.Index(0))
	case reflect.Map:
		value.Set(reflect.MakeMap(value.Type()))
		key, item := reflect.New(value.Type().Key()).Elem(), reflect.New(value.Type().Elem()).Elem()
		populateWire(key)
		populateWire(item)
		value.SetMapIndex(key, item)
	case reflect.Interface:
		value.Set(reflect.ValueOf("public fixture"))
	case reflect.String:
		value.SetString("fixture 🐈")
	case reflect.Bool:
		value.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value.SetInt(17)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		value.SetUint(19)
	}
}

func assertProjection[A, B any](t *testing.T, project func(A) B) {
	t.Helper()
	for _, populated := range []bool{false, true} {
		var source A
		if populated {
			populateWire(reflect.ValueOf(&source).Elem())
		}
		native, err := json.Marshal(source)
		if err != nil {
			t.Fatal(err)
		}
		neutral, err := json.Marshal(project(source))
		if err != nil {
			t.Fatal(err)
		}
		if string(native) != string(neutral) {
			t.Fatalf("projection changed wire data (populated=%v):\nnative %s\nneutral %s", populated, native, neutral)
		}
		if reflect.TypeOf(source).PkgPath() == reflect.TypeOf(project(source)).PkgPath() {
			t.Fatal("neutral DTO aliases native DTO")
		}
	}
}

func TestNativeProjectionPreservesPublicWireFields(t *testing.T) {
	t.Run("transcript", func(t *testing.T) { assertProjection(t, projectTranscript) })
	t.Run("page", func(t *testing.T) { assertProjection(t, projectPage) })
	t.Run("prompt", func(t *testing.T) { assertProjection(t, projectPrompt) })
	t.Run("response", func(t *testing.T) { assertProjection(t, projectPromptResponse) })
	t.Run("queue", func(t *testing.T) { assertProjection(t, projectQueueEntry) })
	t.Run("receipt", func(t *testing.T) { assertProjection(t, projectSendReceipt) })
	t.Run("acknowledgement", func(t *testing.T) { assertProjection(t, projectSendAcknowledgement) })
	t.Run("model", func(t *testing.T) { assertProjection(t, projectModel) })
	t.Run("mode", func(t *testing.T) { assertProjection(t, projectCollaborationMode) })
	t.Run("settings", func(t *testing.T) { assertProjection(t, projectThreadSettings) })
	t.Run("update", func(t *testing.T) { assertProjection(t, projectThreadSettingsUpdate) })
	t.Run("activity", func(t *testing.T) { assertProjection(t, projectActivitySnapshot) })
}

func TestNativeProjectionPreservesEmptyCollections(t *testing.T) {
	transcript := projectTranscript(codex.Transcript{Entries: []codex.TranscriptEntry{}})
	prompt := projectPrompt(codex.Prompt{Params: map[string]any{}, Questions: []codex.Question{}})
	if transcript.Entries == nil || prompt.Params == nil || prompt.Questions == nil {
		t.Fatal("empty collections became nil")
	}
	if projectTranscript(codex.Transcript{}).Entries != nil || projectPrompt(codex.Prompt{}).Params != nil {
		t.Fatal("nil collections became empty")
	}
}

func TestLegacyAdapterRetainsOptionalAbsence(t *testing.T) {
	plain := adaptLegacyTarget(Target{Client: &fakeClient{}, ThreadID: "trusted", Directory: "/workspace"})
	if plain.Operations.Page != nil || plain.Operations.Respond != nil || plain.Operations.PendingRevision != nil || plain.Operations.QueueCompletion != nil || plain.Operations.Activity != nil {
		t.Fatal("legacy adapter invented optional support")
	}
	if plain.Operations.LegacyRespond == nil || plain.Operations.Verify == nil || plain.Operations.Send == nil {
		t.Fatal("legacy operations missing")
	}
	page := adaptLegacyTarget(Target{Client: &fakePageClient{fakeClient: &fakeClient{}}, ThreadID: "trusted", Directory: "/workspace"})
	if page.Operations.Page == nil || page.Operations.Respond != nil {
		t.Fatal("optional groups were not detected independently")
	}
}

func TestLegacyAdapterPreservesOriginalNativePresentationObject(t *testing.T) {
	client := &presentationClient{&fakePageClient{fakeClient: &fakeClient{}, page: codex.TranscriptPage{Transcript: codex.Transcript{ThreadID: "trusted", Entries: []codex.TranscriptEntry{{Kind: "agentMessage"}}}}}}
	var original *codex.Transcript
	var order []string
	target := adaptLegacyTarget(Target{Client: client, ThreadID: "trusted", Directory: "/workspace",
		TransformTranscript: func(value *codex.Transcript) { original = value; order = append(order, "transform") },
		Attachments: presentationAttachments{observe: func(value *codex.Transcript) error {
			if value != original {
				t.Fatal("attachment callback received a reconstructed native object")
			}
			order = append(order, "attachment")
			value.Entries[0].DisplayText = new(string)
			return nil
		}},
		PresentTranscript: func(value *codex.Transcript) {
			if value != original {
				t.Fatal("presentation callback received a reconstructed native object")
			}
			order = append(order, "present")
			value.Entries[0].HTML = "native rendered"
		},
	})
	if target.TransformTranscript != nil || target.PresentTranscript != nil {
		t.Fatal("legacy callbacks would run again on neutral data")
	}
	value, err := target.Operations.Read.Read(context.Background(), target.Identity)
	if err != nil || !reflect.DeepEqual(order, []string{"transform", "attachment", "present"}) || value.Entries[0].HTML != "native rendered" || value.Entries[0].DisplayText == nil {
		t.Fatalf("native callback projection changed: %#v %v %v", value, order, err)
	}
	order = nil
	if _, err := target.Operations.Page.ReadPage(context.Background(), target.Identity, ""); err != nil || !reflect.DeepEqual(order, []string{"transform", "attachment", "present"}) {
		t.Fatalf("page callback order: %v %v", order, err)
	}
}

func TestLegacyPromptErrorRetainsNativeCause(t *testing.T) {
	original := &codex.PromptResponseError{Code: "prompt_changed", NotSent: true, Err: errors.New("refresh")}
	projected := projectPromptError(original)
	var neutral *backend.PromptError
	var native *codex.PromptResponseError
	if !errors.As(projected, &neutral) || !errors.As(projected, &native) || !errors.Is(projected, original) || neutral.Message != "refresh" || !neutral.NotSent {
		t.Fatal("prompt disposition or original error chain lost")
	}
}

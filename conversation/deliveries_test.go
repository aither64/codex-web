package conversation_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/aither64/codex-web/conversation/backend"
)

type deliveryFixture struct {
	status     backend.DeliveryStatus
	requests   []backend.DeliveryRequest
	err        error
	entries    []backend.DeliveryStatus
	collection *backend.DeliveryCollection
}

func (f *deliveryFixture) Deliveries(context.Context, backend.Identity) (backend.DeliveryCollection, error) {
	if f.collection != nil {
		return *f.collection, f.err
	}
	entries := f.entries
	if entries == nil {
		entries = []backend.DeliveryStatus{f.status}
	}
	return backend.DeliveryCollection{Version: 1, Scope: f.status.Scope, CanEnqueue: false, DeliveryModes: []string{}, Entries: entries}, f.err
}

func TestDurableDeliveryNegativeProofIsBoundToAuthenticatedCanonicalRequest(t *testing.T) {
	h, _, target, _ := neutralFixture(t)
	input := backend.DeliveryRequest{Version: 1, ClientUserMessageID: attempt, Message: "saved", Delivery: "steer", Selection: strings.Repeat("a", 64)}
	f := &deliveryFixture{}
	target.Operations.Deliveries, target.Capabilities.DurableDelivery = f, true
	body, _ := json.Marshal(input)
	proof := backend.DeliveryNotQueuedProof{Version: 1, Scope: target.Identity.ScopeKey, Request: input}
	proof.Request.AttachmentIDs = []string{}
	f.err = &backend.DeliveryError{Code: "delivery_selection_changed", NotQueued: &proof}
	w := callNeutral(h, "POST", "deliveries", string(body))
	if w.Code != 409 || !strings.Contains(w.Body.String(), `"notQueued":true`) {
		t.Fatal(w.Code, w.Body.String())
	}
	f.err = &backend.DeliveryError{Code: "delivery_forbidden", NotQueued: &proof}
	if w := callNeutral(h, "POST", "deliveries", string(body)); w.Code != 503 || strings.Contains(w.Body.String(), `"notQueued":true`) {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, mutate := range []func(*backend.DeliveryNotQueuedProof){
		func(p *backend.DeliveryNotQueuedProof) { p.Version = 2 }, func(p *backend.DeliveryNotQueuedProof) { p.Scope = "foreign" },
		func(p *backend.DeliveryNotQueuedProof) { p.Request.Message = "other" }, func(p *backend.DeliveryNotQueuedProof) { p.Request.Selection = strings.Repeat("b", 64) },
		func(p *backend.DeliveryNotQueuedProof) { p.Request.AttachmentIDs = []string{attempt} }, func(p *backend.DeliveryNotQueuedProof) { p.Request.Delivery = "followUp" },
	} {
		changed := proof
		mutate(&changed)
		f.err = &backend.DeliveryError{Code: "delivery_selection_changed", NotQueued: &changed}
		w = callNeutral(h, "POST", "deliveries", string(body))
		if w.Code != 503 || strings.Contains(w.Body.String(), `"notQueued":true`) {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}

func TestRetiredDeliveryLeavesCapacityButNeverBecomesAcceptance(t *testing.T) {
	h, _, target, _ := neutralFixture(t)
	f := &deliveryFixture{status: backend.DeliveryStatus{Version: 1, Scope: target.Identity.ScopeKey, Selection: strings.Repeat("a", 64), Sequence: 1, State: "unknown", Retired: true, Message: "saved", Delivery: "steer", CreatedAtMS: 1, UpdatedAtMS: 1}}
	target.Operations.Deliveries, target.Capabilities.DurableDelivery = f, true
	for i := 0; i < 100; i++ {
		v := f.status
		v.ClientUserMessageID = fmt.Sprintf("%032x", i)
		f.entries = append(f.entries, v)
	}
	if w := callNeutral(h, "GET", "deliveries", ""); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	f.entries[0].State = "queued"
	if w := callNeutral(h, "GET", "deliveries", ""); w.Code != 503 {
		t.Fatal(w.Code, w.Body.String())
	}
}
func (f *deliveryFixture) AdmitDelivery(_ context.Context, _ backend.Identity, input backend.DeliveryRequest) (backend.DeliveryStatus, error) {
	f.requests = append(f.requests, input)
	return f.status, f.err
}
func (f *deliveryFixture) Delivery(context.Context, backend.Identity, string) (backend.DeliveryStatus, error) {
	return f.status, f.err
}

func TestDurableDeliveryIsOptInAndSeparateFromNativeSend(t *testing.T) {
	h, b, target, _ := neutralFixture(t)
	f := &deliveryFixture{status: backend.DeliveryStatus{Version: 1, Scope: target.Identity.ScopeKey, ClientUserMessageID: attempt,
		Selection: strings.Repeat("a", 64), Sequence: 1, State: "queued", Message: "saved", Delivery: "steer", CreatedAtMS: 1, UpdatedAtMS: 1}}
	target.Operations.Deliveries = f
	body, _ := json.Marshal(backend.DeliveryRequest{Version: 1, ClientUserMessageID: attempt, Message: "saved", Delivery: "steer", Selection: f.status.Selection})
	if w := callNeutral(h, "POST", "deliveries", string(body)); w.Code != 501 || len(f.requests) != 0 {
		t.Fatal(w.Code, w.Body.String())
	}
	target.Capabilities.DurableDelivery = true
	for range 2 {
		w := callNeutral(h, "POST", "deliveries", string(body))
		if w.Code != 202 || strings.Contains(w.Body.String(), `"turnId"`) || !strings.Contains(w.Body.String(), `"state":"queued"`) {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	for _, route := range []string{"deliveries", "deliveries/" + attempt} {
		if w := callNeutral(h, "GET", route, ""); w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if len(b.sends) != 0 || len(f.requests) != 2 {
		t.Fatal("delivery used native sender")
	}
	f.status.State = "sent"
	if w := callNeutral(h, "GET", "deliveries/"+attempt, ""); w.Code == 200 {
		t.Fatal("invented native acceptance")
	}
}

func TestDurableDeliveryValidationAndSafeErrors(t *testing.T) {
	h, b, target, _ := neutralFixture(t)
	f := &deliveryFixture{}
	target.Operations.Deliveries = f
	target.Capabilities.DurableDelivery = true
	for _, body := range []string{`{}`, `{"version":1,"nativeId":"foreign"}`, `{"version":2}`, `{"version":1,"clientUserMessageId":"../x"}`} {
		if w := callNeutral(h, "POST", "deliveries", body); w.Code != 400 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if len(f.requests) != 0 || len(b.sends) != 0 {
		t.Fatal("invalid input reached provider")
	}
	f.err = &backend.DeliveryError{Code: "delivery_selection_changed"}
	if w := callNeutral(h, "GET", "deliveries", ""); w.Code != 409 || !strings.Contains(w.Body.String(), "delivery_selection_changed") {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestDurableDeliveryCollectionOmitsSentOnlyAfterFullValidation(t *testing.T) {
	h, b, target, _ := neutralFixture(t)
	base := backend.DeliveryStatus{Version: 1, Scope: target.Identity.ScopeKey, Selection: strings.Repeat("a", 64), ClientUserMessageID: attempt, Sequence: 1, State: "sent", Message: "original", Delivery: "steer", CreatedAtMS: 1, UpdatedAtMS: 1, Receipt: &backend.SendReceipt{ClientUserMessageID: attempt, TurnID: "original-turn"}}
	entries := []backend.DeliveryStatus{base}
	for i, state := range []string{"queued", "dispatching", "pending", "not_sent", "unknown", "unknown"} {
		row := base
		row.ClientUserMessageID = fmt.Sprintf("%032x", i+1)
		row.Sequence = uint64(i + 2)
		row.State = state
		row.Receipt = nil
		row.Retired = i == 5
		entries = append(entries, row)
	}
	original := backend.DeliveryCollection{Version: 1, Scope: base.Scope, Selection: base.Selection, CanEnqueue: true, DeliveryModes: []string{"steer", "followUp"}, Settings: backend.Settings{Model: "saved-model"}, Entries: entries}
	f := &deliveryFixture{status: base, collection: &original}
	target.Operations.Deliveries = f
	target.Capabilities.DurableDelivery = true
	before, _ := json.Marshal(original)
	w := callNeutral(h, "GET", "deliveries", "")
	var got backend.DeliveryCollection
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || w.Code != 200 {
		t.Fatal(w.Code, w.Body.String(), err)
	}
	expected := original
	expected.Entries = entries[1:]
	if !reflect.DeepEqual(got, expected) {
		t.Fatal(got, expected)
	}
	after, _ := json.Marshal(original)
	if string(before) != string(after) {
		t.Fatal("provider collection mutated")
	}
	input, _ := json.Marshal(backend.DeliveryRequest{Version: 1, ClientUserMessageID: attempt, Message: base.Message, Delivery: base.Delivery, Selection: base.Selection})
	for _, method := range []string{"GET", "POST", "POST"} {
		route, body := "deliveries/"+attempt, ""
		if method == "POST" {
			route, body = "deliveries", string(input)
		}
		response := callNeutral(h, method, route, body)
		var status backend.DeliveryStatus
		if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil || !reflect.DeepEqual(status, base) {
			t.Fatal(response.Code, response.Body.String(), err)
		}
	}
	if len(b.sends) != 0 || len(f.requests) != 2 {
		t.Fatal("status recovery used native send")
	}
	original.Entries = []backend.DeliveryStatus{base}
	w = callNeutral(h, "GET", "deliveries", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"entries":[]`) {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, name := range []string{"receipt", "duplicate", "scope", "count", "outstanding"} {
		t.Run(name, func(t *testing.T) {
			bad := original
			bad.Entries = []backend.DeliveryStatus{base}
			switch name {
			case "receipt":
				bad.Entries[0].Receipt = nil
			case "duplicate":
				bad.Entries = append(bad.Entries, base)
			case "scope":
				bad.Scope = "foreign"
			case "count":
				for i := 1; i < 121; i++ {
					row := base
					row.ClientUserMessageID = fmt.Sprintf("%032x", i)
					bad.Entries = append(bad.Entries, row)
				}
			case "outstanding":
				for i := 1; i < 22; i++ {
					row := base
					row.ClientUserMessageID = fmt.Sprintf("%032x", i)
					row.State = "queued"
					row.Receipt = nil
					bad.Entries = append(bad.Entries, row)
				}
			}
			f.collection = &bad
			response := callNeutral(h, "GET", "deliveries", "")
			if response.Code != 503 {
				t.Fatal(response.Code, response.Body.String())
			}
		})
	}
}

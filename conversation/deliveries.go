package conversation

import (
	"errors"
	"net/http"
	"regexp"
	"slices"
	"strings"

	"github.com/aither64/codex-web/conversation/backend"
)

var deliveryAttemptPattern = regexp.MustCompile(`^([0-9a-f]{32}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$`)

func validDeliveryStatus(value backend.DeliveryStatus, scope string) bool {
	if value.Version != 1 || value.Scope != scope || !deliveryAttemptPattern.MatchString(value.ClientUserMessageID) ||
		value.Sequence == 0 || !messageDigestPattern.MatchString(value.Selection) || value.CreatedAtMS <= 0 || value.UpdatedAtMS < value.CreatedAtMS ||
		(value.Delivery != "" && value.Delivery != "steer" && value.Delivery != "followUp") || len(value.Failure) > 512 || value.Retired && value.State != "unknown" {
		return false
	}
	if value.State == "sent" {
		return value.Receipt != nil && value.Receipt.ClientUserMessageID == value.ClientUserMessageID && value.Receipt.TurnID != ""
	}
	return value.Receipt == nil && (value.State == "queued" || value.State == "dispatching" || value.State == "pending" || value.State == "not_sent" || value.State == "unknown")
}

func sameDeliveryRequest(a, b backend.DeliveryRequest) bool {
	return a.Version == b.Version && a.ClientUserMessageID == b.ClientUserMessageID && a.Message == b.Message &&
		a.Selection == b.Selection && a.Delivery == b.Delivery && slices.Equal(a.AttachmentIDs, b.AttachmentIDs)
}

func (h *Handler) deliveryError(w http.ResponseWriter, r *http.Request, err error, scope string, input *backend.DeliveryRequest) {
	var disposition *backend.DeliveryError
	if errors.As(err, &disposition) {
		statuses := map[string]int{"delivery_not_found": 404, "delivery_conflict": 409, "delivery_selection_changed": 409,
			"delivery_capacity": 429, "delivery_unavailable": 503, "delivery_forbidden": 403, "delivery_invalid": 400}
		if status := statuses[disposition.Code]; status != 0 {
			body := map[string]any{"error": "Delivery request could not be completed.", "code": disposition.Code}
			if proof := disposition.NotQueued; proof != nil {
				if disposition.Code == "delivery_forbidden" || disposition.Code == "delivery_not_found" || disposition.Code == "delivery_conflict" ||
					input == nil || proof.Version != 1 || proof.Scope != scope || !sameDeliveryRequest(proof.Request, *input) {
					h.serverError(w, r, errors.New("invalid negative delivery proof"))
					return
				}
				body["notQueued"], body["proof"] = true, proof
			}
			writeJSON(w, status, body)
			return
		}
	}
	h.serverError(w, r, err)
}

func (h *Handler) deliveries(w http.ResponseWriter, r *http.Request, target BackendTarget, id string) {
	if !target.Capabilities.DurableDelivery || target.Operations.Deliveries == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "Durable delivery is unavailable.", "code": "durable_delivery_unavailable"})
		return
	}
	provider := target.Operations.Deliveries
	if r.URL.RawQuery != "" || id != "" && !deliveryAttemptPattern.MatchString(id) {
		writeError(w, http.StatusBadRequest, "delivery request is invalid")
		return
	}
	if r.Method == http.MethodGet && id == "" {
		collection, err := provider.Deliveries(r.Context(), target.Identity)
		if err != nil {
			h.deliveryError(w, r, err, target.Identity.ScopeKey, nil)
			return
		}
		valid := collection.Version == 1 && collection.Scope == target.Identity.ScopeKey && len(collection.Entries) <= 120 &&
			(!collection.CanEnqueue || messageDigestPattern.MatchString(collection.Selection))
		seen := map[string]bool{}
		outstanding := 0
		for _, entry := range collection.Entries {
			valid = valid && validDeliveryStatus(entry, collection.Scope) && !seen[entry.ClientUserMessageID]
			seen[entry.ClientUserMessageID] = true
			if entry.State != "sent" && entry.State != "not_sent" && !entry.Retired {
				outstanding++
			}
		}
		if !valid || outstanding > 20 {
			h.serverError(w, r, errors.New("invalid delivery collection"))
			return
		}
		// Validate every provider row before selecting the outstanding status view.
		entries := make([]backend.DeliveryStatus, 0, len(collection.Entries))
		for _, entry := range collection.Entries {
			if entry.State != "sent" {
				entries = append(entries, entry)
			}
		}
		collection.Entries = entries
		writeJSON(w, http.StatusOK, collection)
		return
	}
	var status backend.DeliveryStatus
	var err error
	var submitted *backend.DeliveryRequest
	if r.Method == http.MethodGet && id != "" {
		status, err = provider.Delivery(r.Context(), target.Identity, id)
	} else if r.Method == http.MethodPost && id == "" {
		var input backend.DeliveryRequest
		if !h.decode(w, r, &input) {
			return
		}
		input.Message = strings.TrimSpace(input.Message)
		valid := input.Version == 1 && deliveryAttemptPattern.MatchString(input.ClientUserMessageID) && messageDigestPattern.MatchString(input.Selection) &&
			(input.Message != "" || len(input.AttachmentIDs) > 0) && len(input.Message) <= h.maxMessageBytes && len(input.AttachmentIDs) <= 100 &&
			(input.Delivery == "" || input.Delivery == "steer" || input.Delivery == "followUp")
		seen := map[string]bool{}
		for _, attachment := range input.AttachmentIDs {
			valid = valid && browserAttemptPattern.MatchString(attachment) && !seen[attachment]
			seen[attachment] = true
		}
		if !valid {
			writeError(w, http.StatusBadRequest, "delivery request is invalid")
			return
		}
		id = input.ClientUserMessageID
		submitted = &input
		// This provider owns attachment preparation and its final local authority
		// recheck. The native attachment/send routes remain unchanged.
		status, err = provider.AdmitDelivery(r.Context(), target.Identity, input)
	} else {
		h.rejectOperation(w, r)
		return
	}
	if err != nil {
		h.deliveryError(w, r, err, target.Identity.ScopeKey, submitted)
		return
	}
	if !validDeliveryStatus(status, target.Identity.ScopeKey) || status.ClientUserMessageID != id {
		h.serverError(w, r, errors.New("invalid delivery status"))
		return
	}
	code := http.StatusOK
	if r.Method == http.MethodPost {
		code = http.StatusAccepted
	}
	writeJSON(w, code, status)
}

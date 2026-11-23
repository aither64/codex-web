package backend

import "context"

// DurableDeliveries owns an application's inbox independently of native send
// receipts. Admission is create-or-get; it must never replay an ambiguous native
// effect. Its verifier and lock authorize local storage, not native execution.
type DurableDeliveries interface {
	Deliveries(context.Context, Identity) (DeliveryCollection, error)
	AdmitDelivery(context.Context, Identity, DeliveryRequest) (DeliveryStatus, error)
	Delivery(context.Context, Identity, string) (DeliveryStatus, error)
}

type DeliveryRequest struct {
	Version             int      `json:"version"`
	ClientUserMessageID string   `json:"clientUserMessageId"`
	Message             string   `json:"message"`
	AttachmentIDs       []string `json:"attachmentIds,omitempty"`
	Delivery            string   `json:"delivery"`
	Selection           string   `json:"selection"`
}

// DeliveryStatus deliberately differs from SendReceipt. Only sent carries native
// acceptance evidence. Message and attachments are the saved display input.
type DeliveryStatus struct {
	Version             int          `json:"version"`
	ClientUserMessageID string       `json:"clientUserMessageId"`
	Scope               string       `json:"scope"`
	Selection           string       `json:"selection"`
	Delivery            string       `json:"delivery"`
	Sequence            uint64       `json:"sequence"`
	State               string       `json:"state"`
	Message             string       `json:"message"`
	AttachmentIDs       []string     `json:"attachmentIds,omitempty"`
	CreatedAtMS         int64        `json:"createdAtMs"`
	UpdatedAtMS         int64        `json:"updatedAtMs"`
	Receipt             *SendReceipt `json:"receipt,omitempty"`
	Failure             string       `json:"failure,omitempty"`
	Retired             bool         `json:"retired,omitempty"`
}

type DeliveryCollection struct {
	Version       int              `json:"version"`
	Scope         string           `json:"scope"`
	CanEnqueue    bool             `json:"canEnqueue"`
	Selection     string           `json:"selection"`
	DeliveryModes []string         `json:"deliveryModes"`
	Settings      Settings         `json:"settings"`
	Entries       []DeliveryStatus `json:"entries"`
}

// DeliveryError exposes only bounded, intentional admission dispositions.
type DeliveryNotQueuedProof struct {
	Version int             `json:"version"`
	Scope   string          `json:"scope"`
	Request DeliveryRequest `json:"request"`
}

type DeliveryError struct {
	Code      string
	NotQueued *DeliveryNotQueuedProof
}

func (e *DeliveryError) Error() string { return e.Code }

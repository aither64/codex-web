package codex

import (
	"context"
	"time"
)

// DeliveryEvent describes one delivery stage without retaining input or options.
// Observers must return promptly and must not call the client recursively.
type DeliveryEvent struct {
	ThreadID            string
	ClientUserMessageID string
	Stage               string
	Duration            time.Duration
	Failed              bool
}

type deliveryIdentity struct{ thread, client string }
type deliveryIdentityKey struct{}

func (c *Client) observeDelivery(ctx context.Context, stage string, started time.Time, err error) {
	if observe := c.options.DeliveryObserver; observe != nil {
		identity, _ := ctx.Value(deliveryIdentityKey{}).(deliveryIdentity)
		observe(DeliveryEvent{ThreadID: identity.thread, ClientUserMessageID: identity.client,
			Stage: stage, Duration: time.Since(started), Failed: err != nil})
	}
}

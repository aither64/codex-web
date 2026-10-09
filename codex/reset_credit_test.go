package codex

import (
	"context"
	"fmt"
	"testing"

	"github.com/coder/websocket"
)

func TestResetCreditRetryKeepsKeyAndCredit(t *testing.T) {
	const key = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for _, outcome := range []string{"reset", "alreadyRedeemed"} {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			params, _ := request["params"].(map[string]any)
			if request["method"] != "account/rateLimitResetCredit/consume" || params["idempotencyKey"] != key || params["creditId"] != "fixture-credit" {
				return fmt.Errorf("unexpected reset request: %#v", request)
			}
			if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]string{"outcome": outcome}}); err != nil {
				return err
			}
		}
		return nil
	})
	client := newTestClient(socket)
	defer client.Close()
	for _, outcome := range []string{"reset", "alreadyRedeemed"} {
		result, err := client.ConsumeRateLimitResetCredit(context.Background(), key, "fixture-credit")
		if err != nil || result.Outcome != outcome {
			t.Fatalf("result = %#v, %v", result, err)
		}
	}
}

func TestResetCreditNextAndUnknownOutcome(t *testing.T) {
	for _, outcome := range []string{"nothingToReset", "noCredit", "unexpected"} {
		t.Run(outcome, func(t *testing.T) {
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				request, err := readObject(connection)
				if err != nil {
					return err
				}
				params := request["params"].(map[string]any)
				if _, found := params["creditId"]; found {
					return fmt.Errorf("next credit was not omitted")
				}
				return writeObject(connection, map[string]any{"id": request["id"], "result": map[string]string{"outcome": outcome}})
			})
			client := newTestClient(socket)
			defer client.Close()
			result, err := client.ConsumeRateLimitResetCredit(context.Background(), "fixture-key", "")
			if outcome == "unexpected" {
				if err == nil {
					t.Fatal("unknown outcome accepted")
				}
				return
			}
			if err != nil || result.Outcome != outcome {
				t.Fatalf("result = %#v, %v", result, err)
			}
		})
	}
	client := newTestClient("/not-connected")
	defer client.Close()
	if _, err := client.ConsumeRateLimitResetCredit(context.Background(), "", ""); err == nil {
		t.Fatal("empty key accepted")
	}
}

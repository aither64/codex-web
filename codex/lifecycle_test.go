package codex

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestLifecycleRoutingPreservesSettings(t *testing.T) {
	for _, method := range []string{"thread/start", "thread/resume", "thread/fork"} {
		t.Run(method, func(t *testing.T) {
			c := NewWithOptions("unused", ClientOptions{LifecycleSettings: func(_ context.Context, id string) (ThreadSettings, error) {
				if id != "thread-1" {
					t.Fatalf("resolver identity = %q", id)
				}
				return ThreadSettings{ModelProvider: "pool", Model: "retained", ReasoningEffort: "low"}, nil
			}})
			config := map[string]any{"model_reasoning_effort": "high", "other": true}
			input := map[string]any{"threadId": "thread-1", "model": "explicit", "config": config}
			value, provider, err := c.lifecycleParams(context.Background(), method, input)
			if err != nil {
				t.Fatal(err)
			}
			params := value.(map[string]any)
			if provider != "pool" || params["model"] != "explicit" || params["config"].(map[string]any)["model_reasoning_effort"] != "high" {
				t.Fatalf("params = %#v", params)
			}
			params["config"].(map[string]any)["other"] = false
			if input["modelProvider"] != nil || config["other"] != true {
				t.Fatal("caller maps mutated")
			}
			value, _, err = c.lifecycleParams(context.Background(), method, map[string]any{"threadId": "thread-1"})
			if err != nil {
				t.Fatal(err)
			}
			params = value.(map[string]any)
			if params["model"] != "retained" || params["config"].(map[string]any)["model_reasoning_effort"] != "low" {
				t.Fatalf("retained settings = %#v", params)
			}
		})
	}
}

func TestLifecycleProviderRequiresEffectiveResponse(t *testing.T) {
	for _, effective := range []string{"pool", "openai", ""} {
		t.Run("effective="+effective, func(t *testing.T) {
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				request, err := readObject(connection)
				if err != nil {
					return err
				}
				params := request["params"].(map[string]any)
				if request["method"] != "thread/resume" || params["modelProvider"] != "pool" {
					return fmt.Errorf("routing request = %#v", request)
				}
				return writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{"modelProvider": effective, "model": "retained"}})
			})
			client := NewWithOptions(socket, ClientOptions{LifecycleSettings: func(context.Context, string) (ThreadSettings, error) {
				return ThreadSettings{ModelProvider: "pool"}, nil
			}})
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var result map[string]any
			err := client.Request(ctx, "thread/resume", map[string]any{"threadId": "thread-1"}, &result)
			if effective == "pool" {
				if err != nil || result["model"] != "retained" {
					t.Fatalf("result=%#v err=%v", result, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "provider mismatch") {
				t.Fatalf("mismatch error = %v", err)
			}
		})
	}
}

func TestLifecycleReservationRefusesBeforeSending(t *testing.T) {
	c := NewWithOptions("unused", ClientOptions{LifecycleSettings: func(context.Context, string) (ThreadSettings, error) {
		return ThreadSettings{}, errors.New("routing transition reserved")
	}})
	if _, _, err := c.lifecycleParams(context.Background(), "thread/resume", map[string]any{}); err == nil {
		t.Fatal("reservation bypassed")
	}
	if _, _, err := c.lifecycleParams(context.Background(), "thread/read", map[string]any{}); err != nil {
		t.Fatalf("passive read blocked: %v", err)
	}
}

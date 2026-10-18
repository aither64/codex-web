package codex

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestRefreshThreadSubscriptionRejoinsAfterScopedClose(t *testing.T) {
	var allowed atomic.Bool
	allowed.Store(true)
	var selected atomic.Value
	selected.Store("openai")
	closeThread, finish := make(chan struct{}), make(chan struct{})
	defer close(finish)
	socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
		if err := handshake(connection); err != nil {
			return err
		}
		for _, provider := range []string{"openai", "pool"} {
			request, err := readObject(connection)
			if err != nil {
				return err
			}
			params, _ := request["params"].(map[string]any)
			if request["method"] != "thread/resume" || params["threadId"] != "thread" || params["modelProvider"] != provider {
				return fmt.Errorf("subscription request = %#v", request)
			}
			if err := writeObject(connection, map[string]any{"id": request["id"], "result": map[string]any{"modelProvider": provider}}); err != nil {
				return err
			}
			if provider == "openai" {
				<-closeThread
				if err := writeObject(connection, map[string]any{"method": "thread/closed", "params": map[string]any{"threadId": "thread"}}); err != nil {
					return err
				}
			}
		}
		<-finish
		return nil
	})
	client := NewWithOptions(socket, ClientOptions{AllowImplicitResume: func(string) bool { return allowed.Load() }, LifecycleRequestSettings: func(context.Context, LifecycleRequest) (ThreadSettings, error) {
		return ThreadSettings{ModelProvider: selected.Load().(string)}, nil
	}})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	events, unsubscribe, err := client.Subscribe(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	defer unsubscribe()
	allowed.Store(false)
	close(closeThread)
	select {
	case <-events:
	case <-ctx.Done():
		t.Fatal("native close was not observed")
	}
	if err := client.RefreshThreadSubscription(ctx, "thread"); err == nil {
		t.Fatal("refresh bypassed session execution fence")
	}
	allowed.Store(true)
	selected.Store("pool")
	if err := client.RefreshThreadSubscription(ctx, "thread"); err != nil {
		t.Fatal(err)
	}
	if err := client.RefreshThreadSubscription(ctx, "unwatched-thread"); err != nil {
		t.Fatal(err)
	}
}

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

func TestLifecycleRoutingUsesDestinationForNewThreadsAndForks(t *testing.T) {
	for _, method := range []string{"thread/start", "thread/fork", "thread/resume"} {
		t.Run(method, func(t *testing.T) {
			client := NewWithOptions("unused", ClientOptions{
				LifecycleSettings: func(context.Context, string) (ThreadSettings, error) {
					t.Fatal("legacy callback ran despite request callback")
					return ThreadSettings{}, nil
				},
				LifecycleRequestSettings: func(_ context.Context, request LifecycleRequest) (ThreadSettings, error) {
					if request.Method != method || request.Cwd != "/workspace/work/destination" || request.ThreadID != "source" {
						t.Fatalf("request = %#v", request)
					}
					return ThreadSettings{ModelProvider: "session-provider"}, nil
				},
			})
			defer client.Close()
			_, provider, err := client.lifecycleParams(context.Background(), method, map[string]any{"threadId": "source", "cwd": "/workspace/work/destination"})
			if err != nil || provider != "session-provider" {
				t.Fatalf("provider=%q err=%v", provider, err)
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

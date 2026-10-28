package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestRPCErrorDataFromNativeReply(t *testing.T) {
	for _, test := range []struct {
		name, data string
	}{
		{"structured admission reason", `{"reason":"serverShuttingDown","extra":{"future":true}}`},
		{"absent", ""},
		{"explicit null", "null"},
		{"other JSON shape", `["future-data"]`},
	} {
		t.Run(test.name, func(t *testing.T) {
			socket := serveUnixWebsocket(t, func(connection *websocket.Conn) error {
				if err := handshake(connection); err != nil {
					return err
				}
				request, err := readObject(connection)
				if err != nil {
					return err
				}
				failure := map[string]any{"code": -32600, "message": "opaque diagnostic"}
				if test.data != "" {
					failure["data"] = json.RawMessage(test.data)
				}
				return writeObject(connection, map[string]any{"id": request["id"], "error": failure})
			})
			client := newTestClient(socket)
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := client.ReadAccountRateLimits(ctx)
			if err == nil || err.Error() != "Codex RPC -32600: opaque diagnostic" {
				t.Fatalf("RPC diagnostic changed: %v", err)
			}
			for _, cause := range []error{err, fmt.Errorf("observation: %w", err), errors.Join(context.Canceled, err)} {
				data, present := RPCErrorData(cause)
				if present != (test.data != "") {
					t.Fatalf("data presence=%t for %v", present, cause)
				}
				if !present {
					if data != nil {
						t.Fatalf("absent data=%s", data)
					}
					continue
				}
				var got, want any
				if json.Unmarshal(data, &got) != nil || json.Unmarshal([]byte(test.data), &want) != nil || fmt.Sprint(got) != fmt.Sprint(want) {
					t.Fatalf("native data changed: %s", data)
				}
				data[0] = '!'
				again, _ := RPCErrorData(cause)
				if !json.Valid(again) || again[0] == '!' {
					t.Fatal("caller mutated retained native error data")
				}
			}
		})
	}
}

func TestRPCErrorDataRejectsOtherErrors(t *testing.T) {
	for _, err := range []error{nil, errors.New(`{"reason":"serverShuttingDown"}`),
		&TransportError{Err: errors.New("serverShuttingDown")}} {
		if data, present := RPCErrorData(err); data != nil || present {
			t.Fatalf("non-RPC error acquired data: %s %t", data, present)
		}
	}
}

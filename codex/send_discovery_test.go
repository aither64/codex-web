package codex

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// This peer cancels the call when its next page request exceeds the assigned
// budget, so every earlier reply has been consumed. No timing-sensitive sleep
// or fake receipt/admission replaces the actual client and durable store.
type sendDiscoveryPeer struct {
	mu                          sync.Mutex
	path, cwd                   string
	created                     int
	items                       []threadItemEntry
	width, budget, pages, sends int
	cancel                      context.CancelFunc
	cursors                     []string
	transform                   func(string, map[string]any)
	lastSend                    map[string]any
}

func discoveryMessage(id, clientID, text, turn string) threadItemEntry {
	return threadItemEntry{TurnID: turn, Item: map[string]any{"id": id, "type": "userMessage", "clientId": clientID,
		"content": []any{map[string]any{"type": "text", "text": text}}}}
}

func newSendDiscoveryPeer(t *testing.T, count int) (*sendDiscoveryPeer, string) {
	t.Helper()
	p := &sendDiscoveryPeer{path: filepath.Join(t.TempDir(), "rollout.jsonl"), cwd: "/workspace", created: 1, width: 2, budget: -1}
	if err := os.WriteFile(p.path, []byte("original-prefix\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		p.items = append(p.items, threadItemEntry{TurnID: fmt.Sprint("turn-", i), Item: map[string]any{"id": fmt.Sprint("item-", i), "type": "assistantMessage"}})
	}
	socket := serveUnixWebsocket(t, func(conn *websocket.Conn) error {
		defer conn.CloseNow()
		if err := handshake(conn); err != nil {
			return err
		}
		for {
			req, err := readObject(conn)
			if err != nil {
				return nil
			}
			params, _ := req["params"].(map[string]any)
			method, _ := req["method"].(string)
			p.mu.Lock()
			result := map[string]any{}
			switch method {
			case "thread/read":
				if value, ok := params["includeTurns"].(bool); !ok || value {
					p.mu.Unlock()
					return fmt.Errorf("discovery did not request native metadata-only shape: %v", params)
				}
				result["thread"] = map[string]any{"id": params["threadId"], "cwd": p.cwd, "path": p.path, "source": "vscode", "createdAt": p.created, "historyMode": "persisted"}
			case "thread/items/list":
				if p.budget >= 0 && p.pages >= p.budget {
					cancel := p.cancel
					p.mu.Unlock()
					cancel()
					continue
				}
				cursor := stringValue(params["cursor"])
				p.pages++
				p.cursors = append(p.cursors, cursor)
				start := 0
				if cursor != "" {
					for start < len(p.items) && stringValue(p.items[start].Item["id"]) != cursor {
						start++
					}
					if start == len(p.items) {
						p.mu.Unlock()
						return fmt.Errorf("unknown fixture cursor %q", cursor)
					}
					start++
				}
				end := min(start+p.width, len(p.items))
				result["data"] = p.items[start:end]
				result["nextCursor"] = nil
				if end < len(p.items) {
					result["nextCursor"] = stringValue(p.items[end-1].Item["id"])
				}
			case "thread/resume":
			case "thread/turns/list", "thread/queue/list":
				result["data"] = []any{}
			case "turn/start":
				p.sends++
				p.lastSend = params
				result["turn"] = map[string]any{"id": "new-turn"}
			default:
				p.mu.Unlock()
				return fmt.Errorf("unexpected discovery RPC %q", method)
			}
			if p.transform != nil {
				p.transform(method, result)
			}
			p.mu.Unlock()
			if err := writeObject(conn, map[string]any{"id": req["id"], "result": result}); err != nil {
				return err
			}
		}
	})
	return p, socket
}

func (p *sendDiscoveryPeer) pass(budget int) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	p.mu.Lock()
	p.budget, p.pages, p.cancel, p.cursors = budget, 0, cancel, nil
	p.mu.Unlock()
	return ctx, cancel
}

func (p *sendDiscoveryPeer) progress() ([]string, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.cursors...), p.sends
}

func discoverBounded(t *testing.T, p *sendDiscoveryPeer, c *Client, budget int) (bool, error) {
	t.Helper()
	ctx, cancel := p.pass(budget)
	defer cancel()
	return c.SendAttemptedWithOptionsResumable(ctx, "thread", "message", "client", "action", testTurnOptions())
}

func finishDiscovery(t *testing.T, p *sendDiscoveryPeer, c *Client, budget int) bool {
	t.Helper()
	for pass := 0; pass < 100; pass++ {
		found, err := discoverBounded(t, p, c, budget)
		if err == nil {
			return found
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if _, sends := p.progress(); sends != 0 {
			t.Fatal("incomplete discovery submitted input")
		}
	}
	t.Fatal("discovery did not complete across bounded calls")
	return false
}

func TestSendDiscoveryBoundedPasses(t *testing.T) {
	p, socket := newSendDiscoveryPeer(t, 80)
	c := newTestClient(socket)
	defer c.Close()
	options := testTurnOptions()
	for pass := 0; pass < 4; pass++ {
		ctx, cancel := p.pass(3)
		found, err := c.SendAttemptedWithOptions(ctx, "thread", "message", "client", "action", options)
		cancel()
		cursors, sends := p.progress()
		if found || !errors.Is(err, context.Canceled) || len(cursors) != 3 || cursors[0] != "" || sends != 0 {
			t.Fatalf("old bounded pass %d: %v/%v pages=%v sends=%d", pass, found, err, cursors, sends)
		}
	}
	captures, passes := 0, 0
	for ; passes < 50; passes++ {
		found, err := discoverBounded(t, p, c, 3)
		if found {
			t.Fatal("absent identity became accepted")
		}
		if err == nil {
			break
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if captures != 0 {
			t.Fatal("partial proof reached downstream capture")
		}
		if _, sends := p.progress(); sends != 0 {
			t.Fatal("partial proof reached native send")
		}
	}
	if passes < 10 || passes >= 50 {
		t.Fatalf("many-page proof completed after %d passes", passes)
	}
	captures++ // The configured caller may now capture, prepare and send.
	ctx, cancel := p.pass(-1)
	defer cancel()
	if err := c.PrepareSendWithOptions("thread", "message", "client", "action", false, options); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		receipt, err := c.SendWithOptions(ctx, "thread", "message", "client", "action", options)
		if err != nil || receipt.TurnID != "new-turn" {
			t.Fatalf("downstream send: %#v %v", receipt, err)
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if captures != 1 || p.sends != 1 || p.lastSend["clientUserMessageId"] != "client" || p.lastSend["model"] != options.Model {
		t.Fatalf("exact downstream submission: captures=%d sends=%d params=%v", captures, p.sends, p.lastSend)
	}
	inputs := p.lastSend["input"].([]any)
	if len(inputs) != 1 || inputs[0].(map[string]any)["text"] != "message" || p.lastSend["effort"] != options.ReasoningEffort {
		t.Fatalf("changed native request %v", p.lastSend)
	}
	if err := additionalContextError(p.lastSend); err != nil {
		t.Fatal(err)
	}
	t.Logf("old: four bounded passes revisit head; new: %d incomplete passes then one capture/native start and accepted replay", passes)
}

func TestSendDiscoveryAcknowledgedOldPageEdge(t *testing.T) {
	for _, steered := range []bool{false, true} {
		t.Run(strconv.FormatBool(steered), func(t *testing.T) {
			p, socket := newSendDiscoveryPeer(t, 40)
			p.items[37] = discoveryMessage("item-37", "client", "message", "accepted-turn")
			if steered {
				p.items[38].TurnID = "accepted-turn"
			}
			c := newTestClient(socket)
			defer c.Close()
			receipt := SendReceipt{TurnID: "accepted-turn", ClientUserMessageID: "client", Steered: steered}
			if err := c.recordSendAttemptWithReceipt("thread", "client", "message", "action", steered, testTurnOptions(), &receipt); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := p.pass(-1)
			defer cancel()
			acked, err := c.AcknowledgeSends(ctx, "thread", []SendAcknowledgement{{ClientUserMessageID: "client", Digest: queueTextDigest("message")}})
			if err != nil || len(acked) != 1 {
				t.Fatalf("acknowledgement: %v %v", acked, err)
			}
			if !finishDiscovery(t, p, c, 1) {
				t.Fatal("acknowledged old-page identity lost")
			}
			attempt, found, err := c.sendAttemptWithOptions("thread", "client", "message", "action", mustDiscoveryOptions(t))
			if err != nil || !found || attempt.State != "accepted" || attempt.TurnID != "accepted-turn" || attempt.Steered != steered {
				t.Fatalf("receipt: %#v %v %v", attempt, found, err)
			}
			if _, sends := p.progress(); sends != 0 {
				t.Fatal("receipt import duplicated native input")
			}
		})
	}
}

func mustDiscoveryOptions(t *testing.T) string {
	t.Helper()
	n, err := normalizeTurnOptions(testTurnOptions())
	if err != nil {
		t.Fatal(err)
	}
	return n.Digest
}

func TestSendDiscoveryAppendDuringBackfill(t *testing.T) {
	p, socket := newSendDiscoveryPeer(t, 30)
	c := newTestClient(socket)
	defer c.Close()
	if found, err := discoverBounded(t, p, c, 2); found || !errors.Is(err, context.Canceled) {
		t.Fatalf("partial: %v %v", found, err)
	}
	p.mu.Lock()
	p.items = append([]threadItemEntry{discoveryMessage("appended", "client", "message", p.items[0].TurnID)}, p.items...)
	file, err := os.OpenFile(p.path, os.O_APPEND|os.O_WRONLY, 0)
	if err == nil {
		_, err = file.WriteString("append\n")
		file.Close()
	}
	p.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if !finishDiscovery(t, p, c, 2) {
		t.Fatal("head append was missed")
	}
	attempt, _, err := c.sendAttemptWithOptions("thread", "client", "message", "action", mustDiscoveryOptions(t))
	if err != nil || !attempt.Steered {
		t.Fatalf("head append receipt: %#v %v", attempt, err)
	}
}

func TestSendDiscoveryNegativeReuseAndCatchup(t *testing.T) {
	p, socket := newSendDiscoveryPeer(t, 20)
	c := newTestClient(socket)
	defer c.Close()
	if finishDiscovery(t, p, c, 3) {
		t.Fatal("unexpected acceptance")
	}
	// A caller's later capture is canceled before admission; no durable attempt
	// exists. The next lookup must freshly validate, without rescanning old pages.
	capture, cancelCapture := context.WithCancel(context.Background())
	cancelCapture()
	if !errors.Is(capture.Err(), context.Canceled) {
		t.Fatal("capture did not cancel")
	}
	if found, err := discoverBounded(t, p, c, 1); found || err != nil {
		t.Fatalf("negative reuse: %v %v", found, err)
	}
	if cursors, _ := p.progress(); len(cursors) != 1 || cursors[0] != "" {
		t.Fatalf("negative reuse pages %v", cursors)
	}
	p.mu.Lock()
	for i := 0; i < 12; i++ {
		p.items = append([]threadItemEntry{{TurnID: "new", Item: map[string]any{"id": fmt.Sprint("append-", i), "type": "assistantMessage"}}}, p.items...)
	}
	p.mu.Unlock()
	if found, err := discoverBounded(t, p, c, 1); found || !errors.Is(err, context.Canceled) {
		t.Fatalf("partial catchup: %v %v", found, err)
	}
	if finishDiscovery(t, p, c, 2) {
		t.Fatal("append-only catchup unexpectedly accepted")
	}
	p.mu.Lock()
	p.items = append([]threadItemEntry{discoveryMessage("exact-append", "client", "message", "new")}, p.items...)
	p.mu.Unlock()
	if !finishDiscovery(t, p, c, 2) {
		t.Fatal("completed negative was reused without fresh head proof")
	}
}

func TestSendDiscoveryInvalidation(t *testing.T) {
	for _, kind := range []string{"truncate", "replace", "prefix", "lineage", "cwd", "path", "generation", "cold-restart", "text", "options", "action", "client"} {
		t.Run(kind, func(t *testing.T) {
			p, socket := newSendDiscoveryPeer(t, 20)
			c := newTestClient(socket)
			defer c.Close()
			if found, err := discoverBounded(t, p, c, 2); found || !errors.Is(err, context.Canceled) {
				t.Fatalf("partial: %v %v", found, err)
			}
			text, id, action, options := "message", "client", "action", testTurnOptions()
			p.mu.Lock()
			switch kind {
			case "truncate":
				if err := os.Truncate(p.path, 1); err != nil {
					t.Fatal(err)
				}
			case "replace":
				if err := os.Rename(p.path, p.path+".old"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p.path, []byte("original-prefix\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "prefix":
				if err := os.WriteFile(p.path, []byte("modified-prefix-plus-append\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "lineage":
				p.created++
			case "cwd":
				p.cwd = "/other"
			case "path":
				old := p.path
				p.path += ".other"
				bytes, err := os.ReadFile(old)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p.path, bytes, 0o600); err != nil {
					t.Fatal(err)
				}
			case "text":
				text = "changed"
			case "options":
				options.ReasoningEffort = "low"
			case "action":
				action = "other"
			case "client":
				id = "other"
			}
			p.mu.Unlock()
			if kind == "generation" {
				c.connectionMu.Lock()
				conn, generation := c.connection, c.generation
				c.connectionMu.Unlock()
				c.markDisconnected(conn, generation, errors.New("fixture reconnect"))
			}
			if kind == "cold-restart" {
				c.Close()
				c = newTestClient(socket)
				defer c.Close()
			}
			ctx, cancel := p.pass(1)
			defer cancel()
			found, err := c.SendAttemptedWithOptionsResumable(ctx, "thread", text, id, action, options)
			cursors, sends := p.progress()
			if found || !errors.Is(err, context.Canceled) || len(cursors) != 1 || cursors[0] != "" || sends != 0 {
				t.Fatalf("invalidated proof reused: %v %v pages=%v sends=%d", found, err, cursors, sends)
			}
		})
	}
}

func TestSendDiscoveryInvalidPages(t *testing.T) {
	for _, kind := range []string{"missing-data", "invalid-entry", "duplicate-item", "cross-page-duplicate", "repeated-cursor", "empty-cursor", "empty-page", "wrong-text", "bad-content", "missing-source", "wrong-thread"} {
		t.Run(kind, func(t *testing.T) {
			p, socket := newSendDiscoveryPeer(t, 8)
			c := newTestClient(socket)
			defer c.Close()
			p.transform = func(method string, result map[string]any) {
				if method == "thread/read" {
					thread := result["thread"].(map[string]any)
					if kind == "missing-source" {
						delete(thread, "path")
					}
					if kind == "wrong-thread" {
						thread["id"] = "wrong"
					}
				}
				if method != "thread/items/list" {
					return
				}
				switch kind {
				case "missing-data":
					delete(result, "data")
				case "invalid-entry":
					result["data"] = []threadItemEntry{{Item: map[string]any{"id": "invalid"}}}
				case "duplicate-item":
					result["data"] = []threadItemEntry{p.items[0], p.items[0]}
				case "cross-page-duplicate":
					if p.pages > 1 {
						result["data"] = []threadItemEntry{p.items[0]}
					}
				case "repeated-cursor":
					result["nextCursor"] = "item-1"
				case "empty-cursor":
					result["nextCursor"] = ""
				case "empty-page":
					result["data"] = []threadItemEntry{}
				case "wrong-text":
					result["data"] = []threadItemEntry{discoveryMessage("bad", "client", "wrong", "turn")}
				case "bad-content":
					entry := discoveryMessage("bad", "client", "message", "turn")
					entry.Item["content"] = false
					result["data"] = []threadItemEntry{entry}
				}
			}
			found, err := discoverBounded(t, p, c, -1)
			if found || err == nil {
				t.Fatalf("invalid page became a verdict: %v %v", found, err)
			}
			c.sendDiscoveryMu.Lock()
			hints := len(c.sendDiscoveries)
			c.sendDiscoveryMu.Unlock()
			if hints != 0 {
				t.Fatal("invalid page retained proof continuation")
			}
			if _, sends := p.progress(); sends != 0 {
				t.Fatal("invalid page submitted input")
			}
		})
	}
}

func TestSendDiscoveryAnchorInvalidation(t *testing.T) {
	for _, kind := range []string{"missing", "reordered", "changed-turn"} {
		t.Run(kind, func(t *testing.T) {
			p, socket := newSendDiscoveryPeer(t, 12)
			c := newTestClient(socket)
			defer c.Close()
			if finishDiscovery(t, p, c, 3) {
				t.Fatal("unexpected acceptance")
			}
			p.mu.Lock()
			switch kind {
			case "missing":
				p.items = p.items[1:]
			case "reordered":
				p.items[0], p.items[1] = p.items[1], p.items[0]
			case "changed-turn":
				p.items[0].TurnID = "changed"
			}
			p.mu.Unlock()
			if found, err := discoverBounded(t, p, c, -1); found || err == nil {
				t.Fatalf("invalid anchor authorized absence: %v %v", found, err)
			}
		})
	}
}

func TestSendDiscoveryLedgerAndSerialization(t *testing.T) {
	for _, state := range []string{"prepared", "accepted", "submitting", "queued"} {
		t.Run(state, func(t *testing.T) {
			p, socket := newSendDiscoveryPeer(t, 4)
			c := newTestClient(socket)
			defer c.Close()
			options := testTurnOptions()
			var err error
			switch state {
			case "queued":
				err = c.recordQueueAttempt("thread", "client", "message")
			case "accepted":
				receipt := SendReceipt{TurnID: "retained", ClientUserMessageID: "client"}
				err = c.recordSendAttemptWithReceipt("thread", "client", "message", "action", false, options, &receipt)
			default:
				err = c.PrepareSendWithOptions("thread", "message", "client", "action", false, options)
				if err == nil && state == "submitting" {
					err = c.markSendSubmitting("thread", "client")
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			found, err := discoverBounded(t, p, c, -1)
			if state == "queued" {
				var unknown *UnknownSendOutcomeError
				if found || !errors.As(err, &unknown) {
					t.Fatalf("queued result %v %v", found, err)
				}
			} else if !found || err != nil {
				t.Fatalf("durable result %v %v", found, err)
			}
			if cursors, sends := p.progress(); len(cursors) != 0 || sends != 0 {
				t.Fatalf("ledger discovery touched history/send: %v %d", cursors, sends)
			}
			if state != "queued" {
				if _, err := c.SendAttemptedWithOptionsResumable(context.Background(), "thread", "changed", "client", "action", options); err == nil || !strings.Contains(err.Error(), "another action") {
					t.Fatalf("identity conflict %v", err)
				}
			}
			if state == "submitting" || state == "queued" {
				ctx, cancel := p.pass(-1)
				defer cancel()
				_, err := c.SendWithOptions(ctx, "thread", "message", "client", "action", options)
				var unknown *UnknownSendOutcomeError
				if !errors.As(err, &unknown) {
					t.Fatalf("unresolved retry %v", err)
				}
				if _, sends := p.progress(); sends != 0 {
					t.Fatal("unresolved retry duplicated input")
				}
			}
		})
	}
	t.Run("context-aware-lock", func(t *testing.T) {
		p, socket := newSendDiscoveryPeer(t, 4)
		c := newTestClient(socket)
		defer c.Close()
		lock := c.queueUpdateLock("thread")
		lock.Lock()
		defer lock.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		found, err := c.SendAttemptedWithOptionsResumable(ctx, "thread", "message", "client", "action", testTurnOptions())
		if found || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("lock wait %v %v", found, err)
		}
		if cursors, _ := p.progress(); len(cursors) != 0 {
			t.Fatal("canceled lock read history")
		}
	})
}

func TestSendDiscoveryExpiredHintRestarts(t *testing.T) {
	p, socket := newSendDiscoveryPeer(t, 12)
	c := newTestClient(socket)
	defer c.Close()
	if found, err := discoverBounded(t, p, c, 2); found || !errors.Is(err, context.Canceled) {
		t.Fatalf("partial %v %v", found, err)
	}
	c.sendDiscoveryMu.Lock()
	c.sendDiscoveries["thread"].lastUsed = time.Now().Add(-pageCursorIdle - time.Second)
	c.sendDiscoveryMu.Unlock()
	if found, err := discoverBounded(t, p, c, 1); found || !errors.Is(err, context.Canceled) {
		t.Fatalf("expired %v %v", found, err)
	}
	if cursors, _ := p.progress(); len(cursors) != 1 || cursors[0] != "" {
		t.Fatalf("expired proof reused: %v", cursors)
	}
}

func TestSendDiscoveryAppendedPrefixInvalidation(t *testing.T) {
	p, socket := newSendDiscoveryPeer(t, 12)
	c := newTestClient(socket)
	defer c.Close()
	if finishDiscovery(t, p, c, 3) {
		t.Fatal("unexpected acceptance")
	}
	p.mu.Lock()
	for i := 0; i < 8; i++ {
		p.items = append([]threadItemEntry{{TurnID: "appended-turn", Item: map[string]any{"id": fmt.Sprint("new-", i), "type": "assistantMessage"}}}, p.items...)
	}
	file, err := os.OpenFile(p.path, os.O_APPEND|os.O_WRONLY, 0)
	if err == nil {
		_, err = file.WriteString("appended-proof-prefix\n")
		file.Close()
	}
	p.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if finishDiscovery(t, p, c, 2) {
		t.Fatal("unexpected append acceptance")
	}
	// Replace a previously scanned appended item below the unchanged head.
	// Its original file prefix stays intact; the newer checkpoint must reset.
	p.mu.Lock()
	p.items[5] = discoveryMessage("new-2", "client", "message", "appended-turn")
	err = os.WriteFile(p.path, []byte("original-prefix\nchanged-appended-proof-and-more\n"), 0o600)
	p.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if !finishDiscovery(t, p, c, 2) {
		t.Fatal("changed appended prefix reused an obsolete negative")
	}
}

func TestSendDiscoveryMidPassPrefixCheckpoint(t *testing.T) {
	p, socket := newSendDiscoveryPeer(t, 20)
	c := newTestClient(socket)
	defer c.Close()
	p.transform = func(method string, result map[string]any) {
		if method != "thread/items/list" || p.pages != 2 {
			return
		}
		file, err := os.OpenFile(p.path, os.O_APPEND|os.O_WRONLY, 0)
		if err == nil {
			_, err = file.WriteString("mid-pass-prefix\n")
			file.Close()
		}
		if err != nil {
			t.Error(err)
		}
	}
	if found, err := discoverBounded(t, p, c, 2); found || !errors.Is(err, context.Canceled) {
		t.Fatalf("partial %v %v", found, err)
	}
	p.mu.Lock()
	p.transform = nil
	p.items[1] = discoveryMessage("item-1", "client", "message", "changed-turn")
	err := os.WriteFile(p.path, []byte("original-prefix\nchanged-mid-pass-prefix-and-more\n"), 0o600)
	p.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if !finishDiscovery(t, p, c, 2) {
		t.Fatal("page checkpoint did not bind the appended prefix")
	}
}

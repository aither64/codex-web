package codex

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func webCitationResult(ref, destination string) map[string]any {
	return map[string]any{"_meta": map[string]any{"raw_messages": []any{
		map[string]any{"content": map[string]any{"content_type": "sonic_webpage", "ref_id": ref, "url": destination, "title": "PyPI help"}},
	}}}
}

func TestCitationReportedResponse(t *testing.T) {
	result := webCitationResult("turn489060view0", "https://pypi.org/help/")
	other := webCitationResult("turn489060view1", "https://pypi.org/help/")
	meta := result["_meta"].(map[string]any)
	meta["raw_messages"] = append(meta["raw_messages"].([]any), other["_meta"].(map[string]any)["raw_messages"].([]any)...)
	details, _ := json.Marshal(result)
	pending := webCitationResult("turn510980view1", "https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/")
	details2, _ := json.Marshal(pending)
	message := "Verify email and enable 2FA. \ue200cite\ue202turn489060view0\ue202turn489060view1\ue201\nA pending publisher does not reserve its name. \ue200cite\ue202turn510980view1\ue201"
	entries := []TranscriptEntry{{Kind: "mcpToolCall", Details: string(details)}, {Kind: "mcpToolCall", Details: string(details2)}, {Kind: "agentMessage", Text: message, TurnStatus: "completed"}}
	c := New("/unused")
	defer c.Close()
	c.presentCitations("thread", "identity", "", 0, nil, entries, func(context.Context, string, any, any) error { t.Fatal("unnecessary source lookup"); return nil })
	entry := entries[2]
	if entry.Text != message || entry.CitationsPending || len(entry.Citations) != 2 {
		t.Fatalf("entry = %#v", entry)
	}
	if len(entry.Citations[0].Sources) != 1 || entry.Citations[0].Sources[0].Number != 1 || entry.Citations[1].Sources[0].Number != 2 {
		t.Fatalf("sources = %#v", entry.Citations)
	}
	markdown := CitationMarkdown(entry)
	if strings.Contains(markdown, "\ue200") || !strings.Contains(markdown, "[\\[1\\]](<https://pypi.org/help/>") {
		t.Fatalf("markdown = %s", markdown)
	}
}

func TestCitationCodeUnicodeAndFallbacks(t *testing.T) {
	token := citationOpen + "source" + citationClose
	message := "Příliš 🐈 " + token + "\n\n`" + token + "`\n\n> ```\n> " + token + "\n> ```\n\n    " + token + "\n"
	spans := citationTokens(message)
	if len(spans) != 1 || spans[0].Start != len("Příliš 🐈 ") {
		t.Fatalf("spans = %#v", spans)
	}
	entry := TranscriptEntry{Text: message, TurnStatus: "completed", Citations: resolveCitationSpans(spans, nil)}
	markdown := CitationMarkdown(entry)
	if !strings.HasPrefix(markdown, "Příliš 🐈 [source unavailable]") || strings.Count(markdown, token) != 3 {
		t.Fatalf("markdown = %s", markdown)
	}
	for _, partial := range []string{"\ue200c", citationOpen, citationOpen + "source"} {
		entry := TranscriptEntry{Text: "Text " + partial, TurnStatus: "inProgress", Citations: resolveCitationSpans(citationTokens("Text "+partial), nil)}
		if got := CitationMarkdown(entry); got != "Text " {
			t.Fatalf("active partial = %q", got)
		}
		entry.TurnStatus = "completed"
		if got := CitationMarkdown(entry); got != "Text [source unavailable]" {
			t.Fatalf("completed partial = %q", got)
		}
	}
	for _, destination := range []string{"javascript:alert(1)", "data:text/html,payload", "/relative", "https://user:password@example.org/", "https://example.org/\n"} {
		if len(citationSources(webCitationResult("source", destination))) != 0 {
			t.Fatalf("accepted unsafe URL %q", destination)
		}
	}
}

func citationFakeRequest(t *testing.T, calls *[]string) pageRequestFunc {
	t.Helper()
	return func(ctx context.Context, method string, raw any, output any) error {
		params := raw.(map[string]any)
		*calls = append(*calls, method+":"+stringValue(params["turnId"])+":"+stringValue(params["cursor"]))
		var response any
		if params["threadId"] != "thread" {
			return fmt.Errorf("wrong thread")
		}
		if method == "thread/turns/list" {
			response = map[string]any{"data": []any{map[string]any{"id": "newer"}, map[string]any{"id": "target"}, map[string]any{"id": "older"}}, "nextCursor": nil}
		} else {
			items := []any{}
			turn := stringValue(params["turnId"])
			add := func(item map[string]any) { items = append(items, map[string]any{"turnId": turn, "item": item}) }
			var next any
			if turn == "target" && params["cursor"] == nil {
				// A source after the answer must not resolve an earlier citation.
				add(map[string]any{"id": "future-tool", "type": "mcpToolCall", "result": webCitationResult("ref", "https://wrong.example/")})
				add(map[string]any{"id": "answer", "type": "agentMessage"})
				next = "next-items"
			} else if turn == "older" {
				add(map[string]any{"id": "old-tool", "type": "mcpToolCall", "result": webCitationResult("ref", "https://pypi.org/help/")})
			}
			response = map[string]any{"data": items, "nextCursor": next}
		}
		encoded, _ := json.Marshal(response)
		return json.Unmarshal(encoded, output)
	}
}

func TestCitationHistoryContinuationAndEarlierTurns(t *testing.T) {
	state := citationScan{sources: make(map[string]CitationSource)}
	var calls []string
	request := citationFakeRequest(t, &calls)
	for i := 0; i < 20 && !state.done; i++ {
		if err := scanCitationHistory(context.Background(), "thread", transcriptItemKey{"target", "answer"}, map[string]bool{"ref": true}, &state, request, 1); err != nil {
			t.Fatal(err)
		}
	}
	if !state.done || state.sources["ref"].URL != "https://pypi.org/help/" {
		t.Fatalf("state = %#v; calls = %v", state, calls)
	}
	for _, call := range calls {
		if strings.Contains(call, ":newer:") {
			t.Fatalf("read future turn: %v", calls)
		}
	}
}

func TestCitationCacheWarmReadsAndThreadIdentity(t *testing.T) {
	c := New("/unused")
	defer c.Close()
	var calls []string
	request := citationFakeRequest(t, &calls)
	entry := TranscriptEntry{TurnID: "target", ItemID: "answer"}
	var found map[string]CitationSource
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var pending bool
		found, pending = c.lookupCitations("thread", "identity", "", 0, nil, entry, map[string]bool{"ref": true}, request)
		if !pending {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if found["ref"].URL != "https://pypi.org/help/" {
		t.Fatalf("sources = %#v", found)
	}
	// Synchronize through the cache lock before observing the worker's calls.
	c.citationMu.Lock()
	before := len(calls)
	c.citationMu.Unlock()
	_, pending := c.lookupCitations("thread", "identity", "", 0, nil, entry, map[string]bool{"ref": true}, request)
	if pending || len(calls) != before {
		t.Fatal("warm lookup re-read history")
	}
	block := make(chan struct{})
	_, pending = c.lookupCitations("another", "identity", "", 0, nil, entry, map[string]bool{"ref": true}, func(ctx context.Context, _ string, _, _ any) error {
		select {
		case <-block:
		case <-ctx.Done():
		}
		return ctx.Err()
	})
	if !pending {
		t.Fatal("another thread reused a cached source")
	}
	close(block)
}

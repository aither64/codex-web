package codex

import "testing"

func TestTypedActivityPreservesFallbackAndRejectsUnsafeLinks(t *testing.T) {
	turn := map[string]any{"id": "turn", "items": []any{
		map[string]any{"id": "search", "type": "webSearch", "query": "query", "action": map[string]any{"type": "openPage", "url": "javascript:alert(1)"}, "results": []any{map[string]any{"url": "https://example.test", "title": "Example"}, map[string]any{"url": "https://user:secret@example.test/"}}},
		map[string]any{"id": "collab", "type": "collabAgentToolCall", "tool": "spawnAgent", "status": "completed", "receiverThreadIds": []any{"child"}, "agentsStates": map[string]any{"child": map[string]any{"status": "completed", "message": "result"}}},
		map[string]any{"id": "lifecycle", "type": "subAgentActivity", "kind": "completed", "agentPath": "/root/review", "agentThreadId": "child"},
	}}
	entries := transcriptEntries(turn)
	if len(entries) != 3 || len(entries[0].Activity.Links) != 1 || entries[0].Summary != "Open web page" || entries[0].Details == "" {
		t.Fatalf("search = %#v", entries)
	}
	if entries[1].Summary != "Start agent" || entries[1].Activity.Agents[0].Message != "result" || entries[2].Activity.AgentPath != "/root/review" {
		t.Fatalf("agents = %#v", entries)
	}
}

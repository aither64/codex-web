package codex

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAsyncQuestionsTranscript(t *testing.T) {
	entries := transcriptEntries(map[string]any{"id": "turn", "status": "inProgress", "items": []any{
		map[string]any{"id": "question", "type": "agentMessage", "text": "Choose", "delivery": "async", "questions": []any{
			nil, map[string]any{"title": "Same?", "options": []any{"One", "Two"}}, map[string]any{"title": "Same?"},
		}},
	}})
	if len(entries) != 1 || entries[0].Text != "Choose" || entries[0].Delivery != "async" || len(entries[0].Questions) != 2 {
		t.Fatalf("lost question metadata: %#v", entries)
	}
	questions := entries[0].Questions
	if questions[0].ID != `["request_user_input_async","question",1]` || questions[1].ID == questions[0].ID {
		t.Fatalf("question indices changed: %#v", questions)
	}
	if len(questions[0].Options) != 2 || len(questions[1].Options) != 0 {
		t.Fatalf("wrong options: %#v", questions)
	}
	if got := asyncQuestions("source", []any{map[string]any{"title": "No delivery required"}}); len(got) != 1 {
		t.Fatalf("missing delivery suppressed questions: %#v", got)
	}
}

func TestAsyncQuestionReplyTranscriptPreservesRawIdentity(t *testing.T) {
	const text = "<send_user_message_question_reply>\n[{\"questionItemId\":\"[\\\"request_user_input_async\\\",\\\"question\\\",0]\",\"question\":\"Choose?\",\"answer\":\"Two\"}]\n</send_user_message_question_reply>"
	var item map[string]any
	data, _ := json.Marshal(map[string]any{"id": "reply", "clientId": "client", "type": "userMessage", "content": []any{map[string]any{"type": "text", "text": text}}})
	if err := json.Unmarshal(data, &item); err != nil {
		t.Fatal(err)
	}
	entries := transcriptEntries(map[string]any{"id": "turn", "items": []any{item}})
	entry := entries[0]
	if entry.Text != text || entry.ClientUserMessageDigest != queueTextDigest(text) || entry.ClientUserMessageID != "client" {
		t.Fatalf("raw retry identity changed: %#v", entry)
	}
	if entry.DisplayText == nil || *entry.DisplayText != "> Choose?\n\nTwo" || len(entry.QuestionReplies) != 1 {
		t.Fatalf("missing readable reply: %#v", entry)
	}
}

func TestAsyncQuestionReplyEnvelopes(t *testing.T) {
	valid := `<send_user_message_question_reply>{"questionItemId":"source","question":"Choice?","answer":"A"}</send_user_message_question_reply>`
	for _, text := range []string{valid, "# Context from my IDE setup:\n\n## My request for Codex:\n" + valid} {
		if got := parseAsyncQuestionReplies(text); len(got) != 1 || got[0].Answer != "A" {
			t.Fatalf("native reply not recognized: %#v", got)
		}
	}
	for _, text := range []string{"Prefix " + valid, valid + " suffix", strings.Replace(valid, `"A"`, "null", 1),
		strings.Replace(valid, `"question":"Choice?",`, "", 1), `<send_user_message_question_reply>[]</send_user_message_question_reply>`} {
		if got := parseAsyncQuestionReplies(text); len(got) != 0 {
			t.Fatalf("ordinary or malformed message interpreted: %q", text)
		}
	}
}

package codex

import (
	"encoding/json"
	"fmt"
	"strings"
)

// AsyncQuestion belongs to an agent message, rather than a pending RPC request.
// ID uses the native CLI's original question index, including skipped entries.
type AsyncQuestion struct {
	ID      string   `json:"id"`
	Title   string   `json:"title"`
	Options []string `json:"options,omitempty"`
}

type AsyncQuestionReply struct {
	QuestionItemID string `json:"questionItemId"`
	Question       string `json:"question"`
	Answer         string `json:"answer"`
}

func asyncQuestions(itemID string, value any) []AsyncQuestion {
	if itemID == "" {
		return nil
	}
	questions, _ := value.([]any)
	var result []AsyncQuestion
	for index, raw := range questions {
		question, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		title, ok := question["title"].(string)
		if !ok || strings.TrimSpace(title) == "" {
			continue
		}
		identity, _ := json.Marshal([]any{"request_user_input_async", itemID, index})
		entry := AsyncQuestion{ID: string(identity), Title: title}
		if options, ok := question["options"].([]any); ok {
			for _, option := range options[:min(len(options), 32)] {
				if label, ok := option.(string); ok && len(label) <= 512 && strings.TrimSpace(label) != "" {
					entry.Options = append(entry.Options, label)
				}
			}
		}
		result = append(result, entry)
	}
	return result
}

// Interpret only a complete native reply envelope, never quoted or incidental JSON.
func parseAsyncQuestionReplies(text string) []AsyncQuestionReply {
	text = strings.TrimSpace(text)
	if strings.HasPrefix(text, "# Context from my IDE setup:\n") {
		_, request, ok := strings.Cut(text, "\n## My request for Codex:\n")
		if !ok {
			return nil
		}
		text = strings.TrimSpace(request)
	}
	const start = "<send_user_message_question_reply>"
	const end = "</send_user_message_question_reply>"
	if !strings.HasPrefix(text, start) || !strings.HasSuffix(text, end) {
		return nil
	}
	body := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(text, start), end))
	var raw []map[string]json.RawMessage
	if strings.HasPrefix(body, "{") {
		body = "[" + body + "]"
	}
	if json.Unmarshal([]byte(body), &raw) != nil || len(raw) == 0 {
		return nil
	}
	var replies []AsyncQuestionReply
	for _, value := range raw {
		var reply AsyncQuestionReply
		for key, target := range map[string]*string{"questionItemId": &reply.QuestionItemID, "question": &reply.Question, "answer": &reply.Answer} {
			field := value[key]
			if len(field) == 0 || field[0] != '"' || json.Unmarshal(field, target) != nil {
				return nil
			}
		}
		if reply.QuestionItemID == "" {
			return nil
		}
		replies = append(replies, reply)
	}
	return replies
}

func asyncQuestionReplyText(replies []AsyncQuestionReply) string {
	parts := make([]string, 0, len(replies))
	for _, reply := range replies {
		parts = append(parts, fmt.Sprintf("> %s\n\n%s", strings.ReplaceAll(reply.Question, "\n", "\n> "), reply.Answer))
	}
	return strings.Join(parts, "\n\n")
}

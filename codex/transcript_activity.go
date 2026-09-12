package codex

import (
	"net/url"
	"sort"
	"strings"
)

// TranscriptActivity is presentation data, not authorization to access child
// threads. Unknown protocol fields remain available in the entry's Details.
type TranscriptActivity struct {
	Action          string          `json:"action,omitempty"`
	Status          string          `json:"status,omitempty"`
	Queries         []string        `json:"queries,omitempty"`
	Links           []ActivityLink  `json:"links,omitempty"`
	Agents          []ActivityAgent `json:"agents,omitempty"`
	AgentPath       string          `json:"agentPath,omitempty"`
	Model           string          `json:"model,omitempty"`
	ReasoningEffort string          `json:"reasoningEffort,omitempty"`
}

type ActivityLink struct {
	URL   string `json:"url"`
	Title string `json:"title"`
}

type ActivityAgent struct {
	ThreadID string `json:"threadId"`
	Status   string `json:"status,omitempty"`
	Message  string `json:"message,omitempty"`
}

func safeActivityURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && (parsed.Scheme == "https" || parsed.Scheme == "http") &&
		parsed.Hostname() != "" && parsed.User == nil && !strings.ContainsAny(value, "\x00\r\n\t")
}

func normalizeTranscriptActivity(entry *TranscriptEntry, item map[string]any) {
	activity := &TranscriptActivity{}
	entry.Activity = activity
	entry.Details = jsonDetails(item)
	switch entry.Kind {
	case "webSearch":
		entry.Summary = "Web search"
		action, _ := item["action"].(map[string]any)
		activity.Action = stringValue(action["type"])
		activity.Queries = stringValues(action["queries"])
		if len(activity.Queries) == 0 {
			query := stringValue(action["query"])
			if query == "" {
				query = stringValue(item["query"])
			}
			if query != "" {
				activity.Queries = []string{query}
			}
		}
		if activity.Action == "openPage" {
			entry.Summary = "Open web page"
		}
		if activity.Action == "findInPage" {
			entry.Summary = "Find in web page"
			entry.Text = stringValue(action["pattern"])
		}
		seen := map[string]bool{}
		addLink := func(raw, title string) {
			if len(activity.Links) >= 50 || !safeActivityURL(raw) || seen[raw] {
				return
			}
			seen[raw] = true
			if title == "" {
				title = raw
			}
			activity.Links = append(activity.Links, ActivityLink{URL: raw, Title: title})
		}
		addLink(stringValue(action["url"]), "")
		results, _ := item["results"].([]any)
		for _, raw := range results {
			result, _ := raw.(map[string]any)
			addLink(stringValue(result["url"]), stringValue(result["title"]))
		}
	case "collabAgentToolCall":
		activity.Action = stringValue(item["tool"])
		activity.Status = stringValue(item["status"])
		activity.Model = stringValue(item["model"])
		activity.ReasoningEffort = stringValue(item["reasoningEffort"])
		labels := map[string]string{
			"spawnAgent": "Start agent", "sendInput": "Send agent input", "resumeAgent": "Resume agent",
			"wait": "Wait for agents", "closeAgent": "Close agent", "sendMessage": "Message agent",
			"followupTask": "Assign follow-up task", "interruptAgent": "Interrupt agent", "listAgents": "List agents",
		}
		entry.Summary = labels[activity.Action]
		if entry.Summary == "" {
			entry.Summary = "Agent operation"
		}
		states, _ := item["agentsStates"].(map[string]any)
		ids := stringValues(item["receiverThreadIds"])
		seen := map[string]bool{}
		for _, id := range ids {
			seen[id] = true
		}
		for id := range states {
			if !seen[id] {
				ids = append(ids, id)
				seen[id] = true
			}
		}
		sort.Strings(ids)
		seen = map[string]bool{}
		for _, id := range ids {
			if id == "" || seen[id] {
				continue
			}
			seen[id] = true
			state, _ := states[id].(map[string]any)
			activity.Agents = append(activity.Agents, ActivityAgent{ThreadID: id, Status: stringValue(state["status"]), Message: stringValue(state["message"])})
		}
	case "subAgentActivity":
		activity.Action = stringValue(item["kind"])
		activity.Status = activity.Action
		activity.AgentPath = stringValue(item["agentPath"])
		label := activity.Action
		if label == "interacted" {
			label = "updated"
		}
		entry.Summary = "Agent " + label
		activity.Agents = []ActivityAgent{{ThreadID: stringValue(item["agentThreadId"]), Status: activity.Status}}
	}
}

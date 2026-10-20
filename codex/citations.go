package codex

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/text"
)

// CitationSource is a validated web destination. Number is local to one message.
type CitationSource struct {
	URL    string `json:"url"`
	Title  string `json:"title"`
	Number int    `json:"number"`
}

// CitationSpan replaces [Start, End) UTF-8 bytes in Text, never code examples.
// Incomplete tokens are hidden only while the assistant turn is active.
type CitationSpan struct {
	Start       int              `json:"start"`
	End         int              `json:"end"`
	Sources     []CitationSource `json:"sources,omitempty"`
	Unavailable bool             `json:"unavailable,omitempty"`
	Incomplete  bool             `json:"incomplete,omitempty"`
	refs        []string
}

const citationOpen = "\ue200cite\ue202"
const citationClose = "\ue201"

func citationURL(value string) (string, bool) {
	u, err := url.Parse(value)
	if len(value) > 8192 || err != nil || u.Hostname() == "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http") || strings.ContainsAny(value, "\x00\r\n\t") {
		return "", false
	}
	return u.String(), true
}

func citationSources(value any) map[string]CitationSource {
	result := make(map[string]CitationSource)
	v, _ := value.(map[string]any)
	meta, _ := v["_meta"].(map[string]any)
	messages, _ := meta["raw_messages"].([]any)
	for _, raw := range messages {
		message, _ := raw.(map[string]any)
		content, _ := message["content"].(map[string]any)
		if stringValue(content["content_type"]) != "sonic_webpage" {
			continue
		}
		ref, title := stringValue(content["ref_id"]), stringValue(content["title"])
		destination, ok := citationURL(stringValue(content["url"]))
		if !ok || ref == "" || len(ref) > 256 || len(title) > 4096 {
			continue
		}
		if title == "" {
			u, _ := url.Parse(destination)
			title = u.Hostname()
		}
		result[ref] = CitationSource{URL: destination, Title: title}
	}
	return result
}

func entryCitationSources(entry TranscriptEntry) map[string]CitationSource {
	if entry.Kind != "mcpToolCall" {
		return nil
	}
	var value any
	if json.Unmarshal([]byte(entry.Details), &value) != nil {
		return nil
	}
	return citationSources(value)
}

// citationTokens uses the Markdown parser to protect code, including nested
// fences and variable-length backtick spans. Offsets refer to the original text.
func citationTokens(value string) []CitationSpan {
	if !strings.Contains(value, "\ue200") {
		return nil
	}
	source := []byte(value)
	document := goldmark.New().Parser().Parse(text.NewReader(source))
	var protected [][2]int
	_ = ast.Walk(document, func(node ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}
		switch n := node.(type) {
		case *ast.CodeBlock, *ast.FencedCodeBlock:
			for i := 0; i < node.Lines().Len(); i++ {
				segment := node.Lines().At(i)
				protected = append(protected, [2]int{segment.Start, segment.Stop})
			}
			return ast.WalkSkipChildren, nil
		case *ast.CodeSpan:
			for child := n.FirstChild(); child != nil; child = child.NextSibling() {
				if t, ok := child.(*ast.Text); ok {
					protected = append(protected, [2]int{t.Segment.Start, t.Segment.Stop})
				}
			}
			return ast.WalkSkipChildren, nil
		}
		return ast.WalkContinue, nil
	})
	var spans []CitationSpan
	for offset := 0; offset < len(value); {
		relative := strings.Index(value[offset:], "\ue200")
		if relative < 0 {
			break
		}
		start := offset + relative
		offset = start + len("\ue200")
		literal := false
		for _, area := range protected {
			if start >= area[0] && start < area[1] {
				literal = true
				break
			}
		}
		if literal {
			continue
		}
		rest := value[start:]
		if strings.HasPrefix(citationOpen, rest) {
			spans = append(spans, CitationSpan{Start: start, End: len(value), Incomplete: true})
			break
		}
		if !strings.HasPrefix(rest, citationOpen) {
			continue
		}
		end := strings.Index(rest[len(citationOpen):], citationClose)
		if end < 0 {
			spans = append(spans, CitationSpan{Start: start, End: len(value), Incomplete: true})
			break
		}
		end += start + len(citationOpen)
		span := CitationSpan{Start: start, End: end + len(citationClose)}
		for _, ref := range strings.Split(value[start+len(citationOpen):end], "\ue202") {
			if ref != "" && len(ref) <= 256 && !strings.ContainsAny(ref, " \r\n\t") {
				span.refs = append(span.refs, ref)
			}
		}
		spans = append(spans, span)
		offset = span.End
	}
	return spans
}

func resolveCitationSpans(spans []CitationSpan, sources map[string]CitationSource) []CitationSpan {
	numbers := make(map[string]int)
	for i := range spans {
		span := &spans[i]
		seen := make(map[string]bool)
		for _, ref := range span.refs {
			source, ok := sources[ref]
			if !ok {
				span.Unavailable = true
				continue
			}
			if seen[source.URL] {
				continue
			}
			seen[source.URL] = true
			if numbers[source.URL] == 0 {
				numbers[source.URL] = len(numbers) + 1
			}
			source.Number = numbers[source.URL]
			span.Sources = append(span.Sources, source)
		}
		if len(span.refs) == 0 {
			span.Unavailable = true
		}
	}
	return spans
}

// CitationMarkdown keeps the original Markdown and replaces normalized spans
// with ordinary links. The host must still sanitize the resulting rendered HTML.
func CitationMarkdown(entry TranscriptEntry) string {
	var output strings.Builder
	previous := 0
	active := entry.TurnStatus != "completed" && entry.TurnStatus != "failed" && entry.TurnStatus != "interrupted" && entry.TurnStatus != "error"
	for _, span := range entry.Citations {
		if span.Start < previous || span.End < span.Start || span.End > len(entry.Text) || !utf8.ValidString(entry.Text[previous:span.Start]) || !utf8.ValidString(entry.Text[span.Start:span.End]) {
			continue
		}
		output.WriteString(entry.Text[previous:span.Start])
		for _, source := range span.Sources {
			destination, ok := citationURL(source.URL)
			if !ok || source.Number < 1 {
				continue
			}
			destination = strings.NewReplacer("<", "%3C", ">", "%3E", "\"", "%22", " ", "%20").Replace(destination)
			title := strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\n", " ", "\r", " ").Replace(source.Title)
			fmt.Fprintf(&output, "[\\[%d\\]](<%s> \"%s\")", source.Number, destination, title)
		}
		if !(span.Incomplete && active) && (span.Unavailable || len(span.Sources) == 0) {
			if entry.CitationsPending {
				output.WriteString("[source pending]")
			} else {
				output.WriteString("[source unavailable]")
			}
		}
		previous = span.End
	}
	output.WriteString(entry.Text[previous:])
	return output.String()
}

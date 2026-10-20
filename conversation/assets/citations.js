// Spans use UTF-8 byte offsets supplied by the shared Go Markdown parser.
function citationParts(entry) {
  const value = typeof entry?.text === "string" ? entry.text : "";
  if (!["agentMessage", "reasoning", "plan"].includes(entry?.kind) || !entry.citations?.length) return [{text: value}];
  const bytes = new TextEncoder().encode(value), decoder = new TextDecoder("utf-8", {fatal: true});
  const parts = [], active = !["completed", "failed", "interrupted", "error"].includes(entry.turnStatus);
  let previous = 0;
  for (const span of entry.citations) {
    if (!Number.isInteger(span.start) || !Number.isInteger(span.end) ||
        span.start < previous || span.end < span.start || span.end > bytes.length) continue;
    let text;
    try { text = decoder.decode(bytes.slice(previous, span.start)); decoder.decode(bytes.slice(span.start, span.end)); }
    catch (_error) { continue; }
    parts.push({text});
    let sources = 0;
    for (const source of span.sources || []) {
      let url;
      try { url = new URL(source.url); } catch (_error) { continue; }
      if (!["http:", "https:"].includes(url.protocol) || url.username || url.password ||
          !Number.isInteger(source.number) || source.number < 1) continue;
      sources++;
      parts.push({text: `[${source.number}]`, href: url.href, title: source.title || url.hostname});
    }
    if (!(span.incomplete && active) && (span.unavailable || !sources)) {
      parts.push({text: entry.citationsPending ? "[source pending]" : "[source unavailable]"});
    }
    previous = span.end;
  }
  parts.push({text: decoder.decode(bytes.slice(previous))});
  return parts;
}

export function citationCopyText(entry) {
  return citationParts(entry).map(part => {
    if (!part.href) return part.text;
    const label = part.text.replaceAll("[", "\\[").replaceAll("]", "\\]");
    const url = part.href.replaceAll("<", "%3C").replaceAll(">", "%3E");
    const title = part.title.replaceAll("\\", "\\\\").replaceAll('"', '\\"').replace(/[\r\n]/g, " ");
    return `[${label}](<${url}> "${title}")`;
  }).join("");
}

export function createCitationMessage(entry, document = globalThis.document) {
  const pre = document.createElement("pre");
  for (const part of citationParts(entry)) {
    if (!part.href) { pre.append(document.createTextNode(part.text)); continue; }
    const link = document.createElement("a");
    link.textContent = part.text;
    link.setAttribute("href", part.href);
    link.setAttribute("title", part.title);
    link.setAttribute("aria-label", `${part.text} ${part.title}`);
    link.setAttribute("rel", "nofollow noreferrer");
    pre.append(link);
  }
  return pre;
}

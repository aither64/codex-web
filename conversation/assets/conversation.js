import {attachmentIDs, sameAttachments, mountUploads, renderAttachments} from "./uploads.js?v=3";
export {attachmentIDs, sameAttachments, mountUploads, renderAttachments, createUploadClient} from "./uploads.js?v=3";
import {createConversationSync, renderConnectionStatus} from "./sync.js?v=1";
export {createConversationSync, renderConnectionStatus, connectionMessage} from "./sync.js?v=1";

const defaultLabels = {
  send: "Send",
  sending: "Sending",
  interrupt: "Interrupt",
  reconnecting: "Reconnecting",
  idle: "Idle",
};

const conversationTargetIdentities = new WeakMap();

function timestampFormatters({locales, timeZone} = {}) {
  return {
    clock: new Intl.DateTimeFormat(locales, {
      timeZone, hour: "2-digit", minute: "2-digit", hourCycle: "h23",
    }),
    title: new Intl.DateTimeFormat(locales, {
      timeZone, dateStyle: "full", timeStyle: "long", hourCycle: "h23",
    }),
    date: new Intl.DateTimeFormat(locales, {timeZone, dateStyle: "long"}),
    key: new Intl.DateTimeFormat("en", {
      timeZone, calendar: "gregory", numberingSystem: "latn",
      year: "numeric", month: "2-digit", day: "2-digit",
    }),
  };
}

let localTimestampFormatters;

// Shared by the mounted interface and applications that render their own
// transcript. Date keys follow the displayed local calendar day, not UTC.
export function formatTranscriptTimestamp(entry, options) {
  const timestamp = typeof entry?.timestamp === "string" ? new Date(entry.timestamp) : null;
  if (!timestamp || !Number.isFinite(timestamp.getTime())) {
    return {
      text: "Time unavailable", title: "This message has no recorded time.",
      dateTime: "", dateKey: "", dateLabel: "", approximate: false,
    };
  }
  const format = options ? timestampFormatters(options) :
    (localTimestampFormatters ||= timestampFormatters());
  const approximate = entry.timestampApproximate === true;
  const day = Object.fromEntries(format.key.formatToParts(timestamp).map(({type, value}) => [type, value]));
  return {
    text: `${approximate ? "~" : ""}${format.clock.format(timestamp)}`,
    title: `${format.title.format(timestamp)}${approximate ? " (approximate, based on turn timing)" : ""}`,
    dateTime: timestamp.toISOString(),
    dateKey: `${day.year}-${day.month}-${day.day}`,
    dateLabel: format.date.format(timestamp),
    approximate,
  };
}

function createElement(name, attributes = {}, text = "") {
  const element = document.createElement(name);
  Object.entries(attributes).forEach(([key, value]) => element.setAttribute(key, value));
  element.textContent = text;
  return element;
}

// Shared typed activity body. Thread identities are text only: a transcript
// event cannot grant navigation to another conversation. URL schemes and
// credentials are checked again at the browser boundary.
export function createTranscriptActivity(entry) {
  if (!entry?.activity || typeof entry.activity !== "object" || Array.isArray(entry.activity)) return null;
  const activity = entry.activity;
  const statusLabels = {
    inProgress: "In progress", completed: "Completed", failed: "Failed", pendingInit: "Starting",
    running: "Running", interrupted: "Interrupted", shutdown: "Stopped", notFound: "Not found",
    started: "Started", interacted: "Updated", errored: "Failed",
  };
  const statusText = (status) => statusLabels[status] || status;
  const body = createElement("div", {class: "codex-typed-activity"});
  body.append(createElement("p", {class: "codex-activity-summary"}, entry.summary || "Activity"));
  if (activity.status) body.append(createElement("span", {class: "codex-activity-status"}, statusText(activity.status)));
  for (const query of Array.isArray(activity.queries) ? activity.queries : []) {
    if (typeof query === "string") body.append(createElement("p", {class: "codex-activity-query"}, query));
  }
  if (entry.text) body.append(createElement("p", {}, entry.text));
  const links = createElement("ul", {class: "codex-activity-links"});
  for (const link of Array.isArray(activity.links) ? activity.links : []) {
    if (typeof link?.url !== "string") continue;
    let url;
    try { url = new URL(link.url); } catch { continue; }
    if (!["https:", "http:"].includes(url.protocol) || !url.hostname || url.username || url.password ||
        /[\u0000-\u0020\u007f]/.test(link.url)) continue;
    const row = createElement("li");
    row.append(createElement("a", {href: url.href, target: "_blank", rel: "noopener noreferrer"},
      typeof link.title === "string" && link.title ? link.title : url.href));
    links.append(row);
  }
  if (links.childNodes.length) body.append(links);
  if (activity.agentPath) body.append(createElement("p", {class: "codex-activity-agent"}, activity.agentPath));
  const agents = Array.isArray(activity.agents) ? activity.agents : [];
  if (agents.length) {
    const list = createElement("ul", {class: "codex-activity-agents"});
    for (const agent of agents) {
      if (!agent || typeof agent !== "object" || Array.isArray(agent)) continue;
      const row = createElement("li");
      row.append(createElement("span", {}, [agent.threadId, statusText(agent.status)].filter(Boolean).join(" · ")));
      if (agent.message) {
        const details = createElement("details");
        details.append(createElement("summary", {}, "Agent result"), createElement("pre", {}, agent.message));
        row.append(details);
      }
      list.append(row);
    }
    body.append(list);
  }
  const settings = [activity.model, activity.reasoningEffort].filter(Boolean).join(" · ");
  if (settings) body.append(createElement("p", {class: "codex-activity-settings"}, settings));
  if (entry.details) {
    const details = createElement("details", {class: "codex-activity-details"});
    details.append(createElement("summary", {}, "Details"), createElement("pre", {}, entry.details));
    body.append(details);
  }
  return body;
}

// Copy source fields rather than rendered DOM so collapsed details and Markdown
// survive, while headings, controls and timestamps stay out of the clipboard.
export function transcriptEntryCopyText(entry) {
  const text = typeof entry?.text === "string" ? entry.text : "";
  const summary = typeof entry?.summary === "string" ? entry.summary : "";
  const details = typeof entry?.details === "string" ? entry.details : "";
  if (["userMessage", "agentMessage", "reasoning", "plan"].includes(entry?.kind)) return text;
  if (entry?.kind === "commandExecution") {
    return [summary.replace(/^\$ /, ""), details].filter(Boolean).join("\n");
  }
  if (entry?.kind === "fileChange") {
    let changes;
    try { changes = JSON.parse(details); } catch (_error) { return details; }
    if (!Array.isArray(changes)) return details;
    return changes.map((change) => {
      if (!change || typeof change !== "object" || Array.isArray(change)) {
        return JSON.stringify(change);
      }
      return [change.path, change.kind?.move_path, change.diff]
        .filter((value) => typeof value === "string" && value.length > 0).join("\n") || JSON.stringify(change);
    }).join("\n\n");
  }
  return [summary, text, details].filter(Boolean).join("\n\n");
}

// Text callbacks are read on click so streamed messages and changing links copy
// their current value. Icons use SVG elements and inherit the surrounding color.
export function createCopyButton({text = "", getText, label = "Copy"} = {}) {
  const button = createElement("button", {
    type: "button", class: "codex-copy-button", title: label,
    "aria-label": label, "data-copy-state": "idle",
  });
  const icon = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  for (const [name, value] of Object.entries({
    viewBox: "0 0 16 16", width: "16", height: "16", fill: "none",
    stroke: "currentColor", "stroke-width": "1.4", "stroke-linecap": "round",
    "stroke-linejoin": "round", "aria-hidden": "true", focusable: "false",
  })) icon.setAttribute(name, value);
  const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
  icon.append(path);
  const feedback = createElement("span", {
    class: "codex-copy-feedback", role: "status", "aria-live": "polite",
  });
  button.append(icon, feedback);
  const paths = {
    idle: "M6 5V2.5h7.5V10H11 M2.5 6H10v7.5H2.5Z",
    copied: "m3 8 3 3 7-7",
    error: "M8 2 1 14h14L8 2Zm0 4v4m0 2v.1",
  };
  const setState = (state) => {
    const status = state === "copied" ? "Copied" : state === "error" ? "Copy failed. Try again." : "";
    path.setAttribute("d", paths[state]);
    feedback.textContent = status;
    button.setAttribute("aria-label", status || label);
    button.setAttribute("title", status || label);
    button.setAttribute("data-copy-state", state);
  };
  setState("idle");
  let feedbackTimer;
  button.addEventListener("click", async () => {
    clearTimeout(feedbackTimer);
    button.disabled = true;
    try {
      const value = typeof getText === "function" ? getText() : typeof text === "function" ? text() : text;
      await globalThis.navigator.clipboard.writeText(value);
      setState("copied");
    } catch (_error) {
      setState("error");
    } finally {
      button.disabled = false;
      feedbackTimer = setTimeout(() => setState("idle"), 2000);
    }
  });
  return button;
}

export function createTranscriptCopyButton(entry) {
  const label = ["userMessage", "agentMessage"].includes(entry?.kind) ? "Copy message" : "Copy activity";
  const button = createCopyButton({getText: () => transcriptEntryCopyText(entry), label});
  button.setAttribute("class", "codex-copy-button codex-entry-copy");
  return button;
}

export function createConversationClient(options) {
  const target = conversationTarget(options);
  const fetchRequest = options.fetch || globalThis.fetch.bind(globalThis);
  const request = async (operation, init = {}) => {
    const headers = {...(init.headers || {})};
    if (init.body !== undefined) headers["Content-Type"] = "application/json";
    const deadline = !init.method || init.method === "GET" ? new AbortController() : null;
    const timer = deadline ? setTimeout(() => deadline.abort(new Error("Conversation request timed out")), 35_000) : null;
    const signals = [options.signal, init.signal, deadline?.signal].filter(Boolean);
    const signal = signals.length ? AbortSignal.any(signals) : undefined;
    try {
      const response = await fetchRequest(joinURLPath(target.apiBase, operation), {
        credentials: "same-origin", ...init, signal, headers,
      });
      const payload = await response.json().catch(() => ({}));
      signal?.throwIfAborted();
      if (!response.ok) {
        const error = new Error(payload.error || `Request failed (${response.status})`);
        error.status = response.status;
        error.code = typeof payload.code === "string" ? payload.code : "";
        error.notSent = payload.notSent === true;
        throw error;
      }
      return payload;
    } finally { clearTimeout(timer); }
  };
  const read = (operation, options = {}) => request(operation, {signal: options.signal});
  const client = {
    thread: (options) => read("thread", options),
    threadPage: ({cursor, signal} = {}) => read(cursor ? `thread/page?cursor=${encodeURIComponent(cursor)}` : "thread/page", {signal}),
    activity: (options) => read("activity", options),
    pending: async (options) => (await read("pending", options)) || [],
    models: async (options) => (await read("models", options)) || [],
    modes: async (options) => (await read("collaboration-modes", options)) || [],
    queue: async (options) => (await read("queue", options)) || [],
    reconcileQueue: (options = {}) => request("queue/reconcile", {method: "POST", body: "{}", signal: options.signal}),
    message: (message, clientUserMessageId, retry = false, attachments = []) => request("message", {
      method: "POST", body: JSON.stringify({message, clientUserMessageId, retry, ...(attachments.length ? {attachmentIds: attachmentIDs(attachments)} : {})}),
    }),
    acknowledgeMessages: (acknowledgements) => request("message-ack", {
      method: "POST", body: JSON.stringify({acknowledgements}),
    }),
    queueMessage: (message, clientUserMessageId, attachments = []) => request("queue", {
      method: "POST", body: JSON.stringify({message, clientUserMessageId, ...(attachments.length ? {attachmentIds: attachmentIDs(attachments)} : {})}),
    }),
    deleteQueued: (id) => request(
      `queue/${encodeOpaquePathSegment(id, "queued message id")}`, {method: "DELETE"},
    ),
    startQueue: (queuedSubmissionId) => request("queue/start", {
      method: "POST", body: JSON.stringify({
        queuedSubmissionId: validateOpaquePathSegment(queuedSubmissionId, "queued message id"),
      }),
    }),
    settings: (model, reasoningEffort, collaborationMode) => {
      const body = {};
      if (model !== undefined) body.model = model;
      if (reasoningEffort !== undefined) body.reasoningEffort = reasoningEffort;
      if (collaborationMode !== undefined) body.collaborationMode = collaborationMode;
      return request("settings", {method: "POST", body: JSON.stringify(body)});
    },
    interrupt: () => request("interrupt", {method: "POST", body: "{}"}),
    respond: (id, payload) => request("respond", {
      method: "POST", body: JSON.stringify({id, ...payload}),
    }),
    snooze: (id, token) => request("respond", {
      method: "POST", body: JSON.stringify({id, snooze: true, ...(token ? {token} : {})}),
    }),
    eventsPath: () => joinURLPath(target.apiBase, "events"),
  };
  conversationTargetIdentities.set(client, target);
  return client;
}

export function transcriptEntryKey(entry, index = 0, entries = []) {
  const turnID = entry?.turnId || "";
  const itemID = entry?.itemId || "";
  if (turnID && itemID) return JSON.stringify([turnID, itemID]);
  if (turnID && entry?.kind === "error") return JSON.stringify([turnID, "turn-error"]);
  let occurrence = 0;
  for (let priorIndex = 0; priorIndex < index; priorIndex += 1) {
    const prior = entries[priorIndex];
    if (!prior?.itemId && (prior?.turnId || "") === turnID &&
        (prior?.kind || "") === (entry?.kind || "")) occurrence += 1;
  }
  return JSON.stringify([turnID, itemID, entry?.kind || "", occurrence]);
}

export async function readTranscriptPage(client, {signal, cursor, legacy = false, expectedThreadId} = {}) {
  const readLegacy = async () => {
    const thread = await client.thread({signal});
    if (!thread?.threadId || typeof thread.status !== "string" || !thread.status ||
        (expectedThreadId && thread.threadId !== expectedThreadId) ||
        !Array.isArray(thread.entries)) throw new Error("Invalid conversation response");
    return {...thread, legacy: true, hasOlder: false, olderCursor: null};
  };
  if (legacy || typeof client.threadPage !== "function") {
    if (cursor) throw new Error("Older history is unavailable on this server");
    return readLegacy();
  }
  let page;
  try {
    page = await client.threadPage({cursor, signal});
  } catch (error) {
    if (cursor || (error.status !== 404 &&
        !(error.status === 501 && error.code === "transcript_paging_unavailable"))) throw error;
    return readLegacy();
  }
  if (!page?.threadId || typeof page.status !== "string" || !page.status ||
      (expectedThreadId && page.threadId !== expectedThreadId) ||
      !Array.isArray(page.entries) || page.entries.length > 100 || typeof page.hasOlder !== "boolean" ||
      (page.hasOlder && (typeof page.olderCursor !== "string" || !page.olderCursor || page.olderCursor === cursor))) {
    throw new Error("Invalid conversation page");
  }
  return {...page, legacy: false};
}

export function createTranscriptHistory() {
  let threadId = "", rows = [], initialized = false, legacy = false;
  let olderCursor = null, hasOlder = false, gapCursor = null, gapStart = -1;
  let repairCursor = null, repairTarget = "", lastActiveRepair = -Infinity, cursorReset = false;
  let resetRecovery = null, repairVersion = 0;
  const pairs = (entries) => {
    const seen = new Set();
    return entries.map((entry, index) => {
      const key = transcriptEntryKey(entry, index, entries);
      if (seen.has(key)) throw new Error("Conversation page repeated an entry");
      seen.add(key);
      return {key, entry};
    });
  };
  const distinct = (...groups) => {
    const seen = new Set();
    return groups.flat().filter((row) => {
      if (seen.has(row.key)) return false;
      seen.add(row.key);
      return true;
    });
  };
  const changes = (incoming, before = rows) => {
    const previous = new Map(before.map((row) => [row.key, row.entry]));
    return new Set(incoming.filter((row) => !previous.has(row.key) ||
      JSON.stringify(previous.get(row.key)) !== JSON.stringify(row.entry)).map((row) => row.key));
  };
  const updateRows = (incoming) => {
    const updates = new Map(incoming.map((row) => [row.key, row]));
    rows = rows.map((row) => updates.get(row.key) || row);
  };
  const clear = (nextThread = "") => {
    threadId = nextThread; rows = []; initialized = false; legacy = false;
    olderCursor = null; hasOlder = false; gapCursor = null; gapStart = -1;
    repairCursor = null; repairTarget = ""; cursorReset = false; resetRecovery = null;
    repairVersion++;
    lastActiveRepair = -Infinity;
  };
  const displayResetRows = () => {
    const updates = new Map(resetRecovery.freshRows.map((row) => [row.key, row]));
    return distinct(resetRecovery.baseRows, resetRecovery.freshRows)
      .map((row) => updates.get(row.key) || row);
  };
  const completeReset = (page) => {
    rows = resetRecovery.freshRows;
    resetRecovery = null;
    repairVersion++;
    olderCursor = page.hasOlder ? page.olderCursor : null;
    hasOlder = Boolean(page.hasOlder);
    gapStart = -1; gapCursor = null; repairTarget = ""; repairCursor = null;
  };
  const applyNewest = (page, now = Date.now()) => {
    if (!page?.threadId || !Array.isArray(page.entries)) throw new Error("Invalid conversation page");
    const reset = threadId !== page.threadId;
    if (reset) clear(page.threadId);
    const incoming = pairs(page.entries);
    const changed = changes(incoming);
    if (!initialized || page.legacy) {
      rows = incoming;
      olderCursor = page.olderCursor || null;
      hasOlder = Boolean(page.hasOlder);
      gapStart = -1; gapCursor = null; repairCursor = null; repairTarget = "";
      cursorReset = false; resetRecovery = null;
    } else if (cursorReset || resetRecovery) {
      const before = rows;
      if (cursorReset) {
        resetRecovery = {boundaryKey: rows[0]?.key || "", baseRows: rows,
          freshRows: [], cursor: null, newestKeys: new Set()};
        cursorReset = false;
        repairVersion++;
      }
      const recovery = resetRecovery;
      if (!page.hasOlder) {
        recovery.freshRows = incoming;
        completeReset(page);
      } else {
        const current = new Map(recovery.freshRows.map((row, index) => [row.key, index]));
        const overlap = incoming.find((row) => current.has(row.key));
        if (recovery.freshRows.length && !overlap) {
          recovery.freshRows = incoming;
          recovery.cursor = page.olderCursor;
          recovery.newestKeys = new Set();
          repairVersion++;
        } else if (overlap) {
          recovery.freshRows = distinct(recovery.freshRows.slice(0, current.get(overlap.key)), incoming);
        } else {
          recovery.freshRows = incoming;
          recovery.cursor = page.olderCursor;
        }
        incoming.forEach((row) => recovery.newestKeys.add(row.key));
        if (!recovery.boundaryKey || recovery.freshRows.some((row) => row.key === recovery.boundaryKey)) {
          completeReset(page);
        } else {
          rows = displayResetRows();
          olderCursor = null; hasOlder = false;
        }
      }
      updateRows(incoming);
      legacy = false;
      return {changed: changes(rows, before), reset};
    } else {
      const current = new Map(rows.map((row, index) => [row.key, index]));
      const firstOverlap = incoming.find((row) => current.has(row.key));
      if (!page.hasOlder) {
        rows = incoming;
        olderCursor = null; hasOlder = false; gapStart = -1; gapCursor = null;
        repairTarget = ""; repairCursor = null; cursorReset = false;
      } else if (firstOverlap) {
        const index = current.get(firstOverlap.key);
        rows = distinct(rows.slice(0, index), incoming);
        if (gapStart >= 0 && index < gapStart) {
          gapStart = -1; gapCursor = null;
        } else if (gapStart >= 0) {
          gapCursor = page.olderCursor || null;
        }
      } else if (incoming.length) {
        if (gapStart < 0) gapStart = rows.length;
        rows = distinct(rows, incoming);
        gapCursor = page.olderCursor;
      } else if (page.hasOlder && rows.length) {
        if (gapStart < 0) gapStart = rows.length;
        gapCursor = page.olderCursor || null;
      }
    }
    updateRows(incoming);
    legacy = Boolean(page.legacy);
    if (!initialized) {
      olderCursor = page.olderCursor || null;
      hasOlder = Boolean(page.hasOlder);
    }
    initialized = true;
    if (!legacy && gapStart < 0 && page.olderCursor && now - lastActiveRepair >= 30_000) {
      const newest = new Set(incoming.map((row) => row.key));
      const active = rows.find((row) => row.entry.turnStatus &&
        !["completed", "failed", "interrupted", "error"].includes(row.entry.turnStatus) && !newest.has(row.key));
      if (active) {
        repairTarget = active.key;
        repairCursor = page.olderCursor;
        lastActiveRepair = now;
      }
    }
    return {changed, reset};
  };
  const applyOlder = (page) => {
    if (page.threadId !== threadId || !Array.isArray(page.entries)) throw new Error("Conversation history changed");
    const incoming = pairs(page.entries);
    const changed = changes(incoming);
    rows = distinct(incoming, rows);
    updateRows(incoming);
    olderCursor = page.olderCursor || null;
    hasOlder = Boolean(page.hasOlder);
    return {changed};
  };
  const applyRepair = (page) => {
    if (page.threadId !== threadId || !Array.isArray(page.entries)) throw new Error("Conversation history changed");
    const incoming = pairs(page.entries);
    if (resetRecovery) {
      const before = rows, recovery = resetRecovery;
      const current = new Map(recovery.freshRows.map((row) => [row.key, row]));
      const authoritative = incoming.map((row) => recovery.newestKeys.has(row.key) ? current.get(row.key) || row : row);
      recovery.freshRows = distinct(authoritative, recovery.freshRows);
      if (!page.hasOlder || (recovery.boundaryKey &&
          recovery.freshRows.some((row) => row.key === recovery.boundaryKey))) {
        completeReset(page);
      } else {
        recovery.cursor = page.olderCursor;
        rows = displayResetRows();
      }
      return {changed: changes(rows, before)};
    }
    const changed = changes(incoming);
    if (gapStart >= 0) {
      const old = new Map(rows.slice(0, gapStart).map((row, index) => [row.key, index]));
      const overlap = incoming.find((row) => old.has(row.key));
      if (overlap) {
        rows = distinct(rows.slice(0, old.get(overlap.key)), incoming, rows.slice(old.get(overlap.key)));
        gapStart = -1; gapCursor = null;
      } else if (page.hasOlder) {
        rows = distinct(rows.slice(0, gapStart), incoming, rows.slice(gapStart));
        gapStart += incoming.filter((row) => !old.has(row.key)).length;
        gapCursor = page.olderCursor;
      } else {
        rows = distinct(incoming, rows.slice(gapStart));
        gapStart = -1; gapCursor = null; olderCursor = null; hasOlder = false;
      }
    } else if (repairTarget) {
      const updates = new Map(incoming.map((row) => [row.key, row]));
      rows = rows.map((row) => updates.get(row.key) || row);
      if (updates.has(repairTarget) || !page.hasOlder) {
        repairTarget = ""; repairCursor = null;
      } else repairCursor = page.olderCursor;
    }
    updateRows(incoming);
    return {changed};
  };
  return {
    applyNewest, applyOlder, applyRepair, clear,
    invalidateCursor() {
      cursorReset = true; olderCursor = null; hasOlder = false;
      gapStart = -1; gapCursor = null; repairCursor = null; repairTarget = "";
      resetRecovery = null; repairVersion++;
    },
    get threadId() { return threadId; },
    get entries() { return rows.map((row) => row.entry); },
    get rows() { return rows; },
    get initialized() { return initialized; },
    get legacy() { return legacy; },
    get olderCursor() { return olderCursor; },
    get hasOlder() { return hasOlder; },
    get gap() { return Boolean(resetRecovery) || gapStart >= 0 || Boolean(repairTarget) || cursorReset; },
    get repairCursor() { return resetRecovery?.cursor || (gapStart >= 0 ? gapCursor : repairCursor); },
    get repairVersion() { return repairVersion; },
  };
}

function absoluteSameOriginPath(value, name) {
  if (typeof value !== "string" || !value.startsWith("/") ||
      /[\u0000-\u001f\u007f]/.test(value) || value.includes("?") || value.includes("#") ||
      value.includes("\\") || value.includes("%") ||
      !/^[A-Za-z0-9/._~!$&'()*+,;=:@-]+$/.test(value)) {
    throw new TypeError(`${name} must be an absolute URL path`);
  }
  if (value.includes("//")) {
    throw new TypeError(`${name} must be a canonical absolute URL path`);
  }
  const base = new URL("https://codex-web.invalid/");
  const resolved = new URL(value, base);
  if (resolved.origin !== base.origin) {
    throw new TypeError(`${name} must be an absolute same-origin URL path`);
  }
  if (resolved.pathname !== value) {
    throw new TypeError(`${name} must be a canonical absolute URL path`);
  }
  return resolved.pathname.length > 1 ? resolved.pathname.replace(/\/$/, "") : "/";
}

function joinURLPath(base, suffix) {
  return `${base === "/" ? "" : base}/${suffix}`;
}

function validateOpaquePathSegment(value, name) {
  if (typeof value !== "string" || !value || value === "." || value === ".." ||
      value.includes("/") || value.includes("\0")) {
    throw new TypeError(`${name} is invalid`);
  }
  try {
    encodeURIComponent(value);
  } catch {
    throw new TypeError(`${name} is invalid`);
  }
  if (new TextEncoder().encode(value).byteLength > 256) {
    throw new TypeError(`${name} is invalid`);
  }
  return value;
}

function encodeOpaquePathSegment(value, name) {
  return encodeURIComponent(validateOpaquePathSegment(value, name));
}

function conversationTarget(options) {
  if (!options || typeof options.id !== "string" || !options.id) {
    throw new TypeError("conversation id is required");
  }
  const encodedID = encodeOpaquePathSegment(options.id, "conversation id");
  const basePath = absoluteSameOriginPath(
    options.basePath === undefined ? "/codex" : options.basePath, "basePath",
  );
  const apiBase = options.conversationPath === undefined ?
    joinURLPath(basePath, `conversations/${encodedID}`) :
    absoluteSameOriginPath(options.conversationPath, "conversationPath");
  let durableNamespace;
  if (options.durableNamespace === undefined) {
    durableNamespace = options.conversationPath ? `path:${apiBase}` :
      `${basePath}:${encodedID}`;
  } else if (typeof options.durableNamespace !== "string" || !options.durableNamespace ||
      /[\u0000-\u001f\u007f]/.test(options.durableNamespace)) {
    throw new TypeError("durableNamespace must be a non-empty printable string");
  } else {
    durableNamespace = `namespace:${encodeURIComponent(options.durableNamespace)}`;
  }
  return Object.freeze({apiBase, basePath, durableNamespace, id: options.id});
}

const durableAttemptIDPattern =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// This is the browser persistence boundary shared by the standalone UI and
// application-owned integrations. A store can retain one attempt at a fixed
// compatibility key or multiple attempts below an application-owned prefix.
// All writes and removals are read back before they report success.
export function createDurableAttemptStore(options) {
  const storage = options?.storage;
  const singletonKey = options?.key;
  const prefix = options?.prefix;
  if ((typeof singletonKey === "string") === (typeof prefix === "string")) {
    throw new TypeError("exactly one durable storage key or prefix is required");
  }
  if (!(singletonKey || prefix)) throw new TypeError("durable storage key is required");
  const encode = options?.encode || ((attempt) => attempt);
  const decode = options?.decode || ((id, value) => ({...value, id}));
  const storageFunctionsAvailable = () => storage &&
    ["getItem", "setItem", "removeItem"].every((name) => typeof storage[name] === "function");
  const probeKey = singletonKey ? `${singletonKey}:probe` : `${prefix}probe`;
  const keyFor = (id) => singletonKey || `${prefix}${id}`;
  const keys = () => {
    if (!storageFunctionsAvailable()) return null;
    if (singletonKey) return [singletonKey];
    if (typeof storage.length !== "number" || typeof storage.key !== "function") return null;
    try {
      const found = [];
      for (let index = 0; index < storage.length; index += 1) {
        const key = storage.key(index);
        if (typeof key === "string" && key.startsWith(prefix)) found.push(key);
      }
      return found.sort();
    } catch (_error) {
      return null;
    }
  };
  const available = () => {
    if (!storageFunctionsAvailable()) return false;
    try {
      const previous = storage.getItem(probeKey);
      storage.setItem(probeKey, "available");
      if (storage.getItem(probeKey) !== "available") return false;
      if (previous === null) {
        storage.removeItem(probeKey);
        return storage.getItem(probeKey) === null;
      }
      storage.setItem(probeKey, previous);
      return storage.getItem(probeKey) === previous;
    } catch (_error) {
      return false;
    }
  };
  const load = () => {
    const storedKeys = keys();
    if (storedKeys === null) return null;
    try {
      const attempts = [];
      for (const key of storedKeys) {
        const encoded = storage.getItem(key);
        if (encoded === null) continue;
        const value = JSON.parse(encoded);
        const id = singletonKey ? value?.id : key.slice(prefix.length);
        if (!durableAttemptIDPattern.test(id)) return null;
        const attempt = decode(id, value);
        if (!attempt || attempt.id !== id) return null;
        attempts.push(attempt);
      }
      return attempts;
    } catch (_error) {
      return null;
    }
  };
  const store = (attempt) => {
    if (!storageFunctionsAvailable() || !attempt ||
        !durableAttemptIDPattern.test(attempt.id)) return false;
    try {
      const encoded = JSON.stringify(encode(attempt));
      const key = keyFor(attempt.id);
      storage.setItem(key, encoded);
      return storage.getItem(key) === encoded;
    } catch (_error) {
      return false;
    }
  };
  const remove = (id) => {
    if (!storageFunctionsAvailable() || !durableAttemptIDPattern.test(id)) return false;
    try {
      const key = keyFor(id);
      storage.removeItem(key);
      return storage.getItem(key) === null;
    } catch (_error) {
      return false;
    }
  };
  const clear = () => {
    const storedKeys = keys();
    if (storedKeys === null) return false;
    try {
      for (const key of storedKeys) storage.removeItem(key);
      return storedKeys.every((key) => storage.getItem(key) === null);
    } catch (_error) {
      return false;
    }
  };
  return {available, clear, load, remove, store};
}

function pendingAttemptStore(storage, key) {
  return createDurableAttemptStore({
    storage,
    key,
    decode: (id, value) => {
      if (!value || typeof value.message !== "string" || (!value.message.trim() && !value.attachmentIds?.length)) return null;
      const attachments = attachmentIDs(value.attachmentIds);
      return {
        id,
        message: value.message,
        ...(attachments.length ? {attachmentIds: attachments} : {}),
        receipt: value.receipt && typeof value.receipt === "object" ? value.receipt : null,
      };
    },
    encode: (attempt) => attempt,
  });
}

function parsePending(store) {
  const attempts = store.load();
  if (attempts === null || attempts.length > 1) {
    throw new Error("Stored conversation operation is invalid; clear it before retrying");
  }
  return attempts[0] || null;
}

export function createDurableSender(options) {
  if (!options?.client) throw new TypeError("conversation client is required");
  const storage = options.storage;
  const clientTarget = conversationTargetIdentities.get(options.client);
  const hasExplicitTarget = options.basePath !== undefined ||
    options.conversationPath !== undefined || options.durableNamespace !== undefined;
  if (!clientTarget && !hasExplicitTarget) {
    throw new TypeError("a custom conversation client requires an explicit durable target");
  }
  let target = clientTarget;
  if (!target || hasExplicitTarget) {
    target = conversationTarget(options);
    if (clientTarget && target.durableNamespace !== clientTarget.durableNamespace) {
      throw new TypeError("durable sender target does not match the conversation client");
    }
  } else if (options.id !== undefined && options.id !== clientTarget.id) {
    throw new TypeError("durable sender id does not match the conversation client");
  }
  const sendKey = `codex-web:send:${target.durableNamespace}`;
  const queueKey = `codex-web:queue:${target.durableNamespace}`;
  const sendStore = pendingAttemptStore(storage, sendKey);
  const queueStore = pendingAttemptStore(storage, queueKey);
  if (!sendStore.available()) throw new Error("Durable browser storage is unavailable");
  const randomUUID = options.randomUUID || (() => globalThis.crypto.randomUUID());
  const save = (store, value) => {
    if (!store.store(value)) {
      throw new Error("Durable browser storage did not retain the operation");
    }
  };
  const submit = async (kind, text, attachments = []) => {
    const ids = attachmentIDs(attachments);
    const store = kind === "send" ? sendStore : queueStore;
    const otherStore = kind === "send" ? queueStore : sendStore;
    const message = String(text || "").trim();
    if (!message && !ids.length) throw new Error("Enter a message or attach files first");
    if (parsePending(otherStore)) {
      throw new Error("Finish the pending conversation operation before starting another one");
    }
    let attempt = parsePending(store);
    if (attempt && (attempt.message !== message || !sameAttachments(attempt.attachmentIds, ids))) {
      throw new Error(`Retry the pending ${kind} operation before submitting another message`);
    }
    const retry = Boolean(attempt);
    if (!attempt) {
      attempt = {id: randomUUID(), message, receipt: null, ...(ids.length ? {attachmentIds: ids} : {})};
      save(store, attempt);
    }
    if (kind === "queue") {
      const entry = await options.client.queueMessage(attempt.message, attempt.id, attempt.attachmentIds || []);
      if (!store.remove(attempt.id)) {
        throw new Error("Durable browser storage did not clear the queued operation");
      }
      return entry;
    }
    const receipt = await options.client.message(attempt.message, attempt.id, retry, attempt.attachmentIds || []);
    attempt.receipt = receipt;
    save(store, attempt);
    return receipt;
  };
  return {
    pending: () => parsePending(sendStore),
    pendingQueue: () => parsePending(queueStore),
    async send(text, attachments = []) {
      return submit("send", text, attachments);
    },
    async queue(text, attachments = []) {
      return submit("queue", text, attachments);
    },
    async acknowledge(entries) {
      const attempt = parsePending(sendStore);
      if (!attempt) return false;
      const entry = (entries || []).find((candidate) => (
        candidate.clientUserMessageId === attempt.id &&
        /^[0-9a-f]{64}$/.test(candidate.clientUserMessageDigest || "")
      ));
      if (!entry) return false;
      const result = await options.client.acknowledgeMessages([{
        clientUserMessageId: attempt.id,
        digest: entry.clientUserMessageDigest,
      }]);
      if (!(result.acknowledgedClientUserMessageIds || []).includes(attempt.id)) return false;
      if (!sendStore.remove(attempt.id)) {
        throw new Error("Durable browser storage did not clear the acknowledged operation");
      }
      return true;
    },
  };
}

export function mountConversation(root, options) {
  if (!(root instanceof Element)) throw new TypeError("conversation root must be an Element");
  const abort = new AbortController();
  const client = options?.client || createConversationClient({...options, signal: abort.signal});
  const labels = {...defaultLabels, ...(options?.labels || {})};
  const capabilities = {
    read: true,
    pending: true,
    queueRead: true,
    send: true,
    queue: true,
    interrupt: true,
    settings: true,
    respond: true,
    eventStream: true,
    ...(options?.capabilities || {}),
  };
  if (!capabilities.read) throw new TypeError("the mounted conversation requires read capability");
  const EventSourceClass = options?.EventSource === undefined ? globalThis.EventSource : options.EventSource;
  let sender = null;
  if (capabilities.send || capabilities.queue) {
    let storage;
    try { storage = options?.storage || globalThis.sessionStorage; } catch (_error) {}
    sender = createDurableSender({
      client, storage, id: options?.id, basePath: options?.basePath,
      conversationPath: options?.conversationPath,
      durableNamespace: options?.durableNamespace,
      randomUUID: options?.randomUUID,
    });
  }
  let currentThread = null;
  let modelCatalog = [];

  const status = createElement("span", {class: "codex-conversation-status"}, labels.reconnecting);
  const connectionStatus = createElement("div", {class: "codex-connection-status"});
  const transcript = createElement("ol", {class: "codex-conversation-transcript"});
  const historyControls = createElement("div", {class: "codex-history-controls"});
  const loadOlder = createElement("button", {type: "button"}, "Load older");
  const retryHistory = createElement("button", {type: "button"}, "Retry history");
  const historyStatus = createElement("span", {role: "status"});
  historyControls.append(loadOlder, historyStatus, retryHistory);
  const prompts = createElement("section", {class: "codex-conversation-prompts"});
  const pendingStatus = createElement("div", {class: "codex-lane-status", role: "status"});
  const pendingStatusLabel = createElement("span", {}, "Loading requests…");
  const retryPending = createElement("button", {type: "button"}, "Retry requests");
  pendingStatus.append(pendingStatusLabel, retryPending);
  retryPending.hidden = true;
  const queue = createElement("section", {class: "codex-conversation-queue"});
  const queueStatus = createElement("div", {class: "codex-lane-status", role: "status"});
  const queueStatusLabel = createElement("span", {}, "Checking queued messages…");
  const retryQueue = createElement("button", {type: "button"}, "Retry queue");
  queueStatus.append(queueStatusLabel, retryQueue);
  retryQueue.hidden = true;
  const textarea = createElement("textarea", {"aria-label": "Message", rows: "5", required: "required"});
  const pending = sender?.pending() || sender?.pendingQueue();
  if (pending) textarea.value = pending.message;
  const send = createElement("button", {type: "submit"}, labels.send);
  const queueButton = createElement("button", {type: "button"}, "Queue");
  const interrupt = createElement("button", {type: "button"}, labels.interrupt);
  const actions = createElement("div", {class: "codex-conversation-actions"});
  if (capabilities.send) actions.append(send);
  if (capabilities.queue) actions.append(queueButton);
  if (capabilities.interrupt) actions.append(interrupt);
  const form = createElement("form", {class: "codex-conversation-form"});
  form.append(textarea, actions);
  const settingsForm = createElement("form", {class: "codex-conversation-settings"});
  const model = createElement("select", {"aria-label": "Model"});
  const effort = createElement("select", {"aria-label": "Reasoning effort"});
  const mode = createElement("select", {"aria-label": "Collaboration mode"});
  mode.disabled = true;
  const modeStatus = createElement("span", {role: "status"}, "Checking mode…");
  const saveSettings = createElement("button", {type: "submit"}, "Save settings");
  settingsForm.append(model, effort, mode, modeStatus, saveSettings);
  const children = [status, connectionStatus];
  if (capabilities.settings) children.push(settingsForm);
  children.push(historyControls, transcript);
  if (capabilities.pending) children.push(pendingStatus, prompts);
  if (capabilities.queueRead) children.push(queueStatus, queue);
  if (capabilities.send || capabilities.queue || capabilities.interrupt) children.push(form);
  root.replaceChildren(...children);
  let uploads = null;
  if (options?.uploadBasePath && (capabilities.send || capabilities.queue)) {
    const uploadRoot = document.createElement("div");
    form.insertBefore(uploadRoot, actions);
    const uploadControls = document.createElement("span");
    actions.prepend(uploadControls);
    uploads = mountUploads(uploadRoot, {
      basePath: options.uploadBasePath, dropTarget: form, controlsRoot: uploadControls,
      storage: options.storage || globalThis.localStorage,
      storageKey: `codex-web:uploads:${options.uploadBasePath}`,
      onChange: ({ready, count}) => { textarea.required = !count; send.disabled = !ready; queueButton.disabled = !ready; },
    });
  }
  const removeAttachment = async (file) => {
    await payloadForAttachmentRemoval(file.deleteUrl);
    await refresh();
  };

  const populateEfforts = (selectedEffort = "") => {
    const selected = modelCatalog.find((entry) => entry.model === model.value);
    effort.replaceChildren(...(selected?.supportedReasoningEfforts || []).map((entry) => (
      createElement("option", {value: entry.reasoningEffort}, entry.reasoningEffort)
    )));
    effort.value = selectedEffort || selected?.defaultReasoningEffort || "";
  };
  const applyThreadSettings = () => {
    if (!capabilities.settings || !currentThread || modelCatalog.length === 0) return;
    if (modelCatalog.some((entry) => entry.model === currentThread.model)) {
      model.value = currentThread.model;
    }
    populateEfforts(currentThread.reasoningEffort || "");
    mode.value = currentThread.collaborationMode || "";
    mode.disabled = !currentThread.collaborationMode;
    modeStatus.hidden = !mode.disabled;
    modeStatus.textContent = currentThread.metadataPending ? "Checking mode…" : "Mode unavailable";
  };
  const loadSettings = async () => {
    const [models, modes] = await Promise.all([client.models(), client.modes()]);
    modelCatalog = models;
    model.replaceChildren(...models.map((entry) => createElement("option", {value: entry.model}, entry.displayName || entry.model)));
    mode.replaceChildren(...modes.map((entry) => createElement("option", {value: entry.mode}, entry.name || entry.mode)));
    model.addEventListener("change", () => populateEfforts());
    applyThreadSettings();
  };

  const renderQueue = (entries) => {
    queue.replaceChildren(createElement("h2", {}, "Queued messages"));
    for (const entry of entries || []) {
      const item = createElement("div", {class: "codex-queue-entry"});
      item.append(createElement("span", {}, entry.displayText ?? entry.text ?? "Queued message"));
      if (entry.attachments?.length) item.append(renderAttachments(entry.attachments));
      if (capabilities.queue) {
        const start = createElement("button", {type: "button"}, "Start");
        start.addEventListener("click", () => void client.startQueue(entry.id).then(() => {
          void refresh(); void refreshQueue(true);
        }).catch(error => { queueStatusLabel.textContent = error.message; queueStatus.hidden = false; }));
        const remove = createElement("button", {type: "button"}, "Remove");
        remove.addEventListener("click", () => void client.deleteQueued(entry.id).then(() => {
          void refreshQueue(true);
        }).catch(error => { queueStatusLabel.textContent = error.message; queueStatus.hidden = false; }));
        item.append(start, remove);
      }
      queue.append(item);
    }
  };

  let promptSignature = null;
  const answerDrafts = new Map();
  const renderPrompts = (entries) => {
    const signature = JSON.stringify(entries || []);
    if (signature === promptSignature) return;
    promptSignature = signature;
    const ids = new Set((entries || []).map(entry => entry.id));
    for (const id of answerDrafts.keys()) if (!ids.has(id)) answerDrafts.delete(id);
    prompts.replaceChildren(createElement("h2", {}, "Requests"));
    for (const entry of entries || []) {
      const item = createElement("div", {class: "codex-prompt"});
      item.append(createElement("pre", {}, JSON.stringify(entry.item || entry.params || {}, null, 2)));
      if (!capabilities.respond) {
        prompts.append(item);
        continue;
      }
      if (entry.kind === "userInput") {
        const answer = createElement("textarea", {
          "aria-label": "Answers as JSON", rows: "3", placeholder: '{"question":{"answers":["value"]}}',
        });
        answer.value = answerDrafts.get(entry.id) || "";
        answer.addEventListener("input", () => answerDrafts.set(entry.id, answer.value));
        const submit = createElement("button", {type: "button"}, "Submit answers");
        submit.addEventListener("click", () => {
          try {
            void client.respond(entry.id, {token: entry.token, answers: JSON.parse(answer.value)}).then(() => {
              void refresh(); void refreshPending(true);
            }).catch(error => { status.textContent = error.message; });
          } catch (error) {
            status.textContent = error.message;
          }
        });
        item.append(answer, submit);
        if (Number(entry.autoResolutionAtMs) > 0) {
          const snooze = createElement("button", {type: "button"}, "Snooze");
          snooze.addEventListener("click", () => void client.snooze(entry.id, entry.token).then(() => {
            void refreshPending(true);
          }).catch(error => { status.textContent = error.message; }));
          item.append(snooze);
        }
      } else {
        for (const decision of entry.availableDecisions || ["accept", "decline"]) {
          const button = createElement("button", {type: "button"}, decision);
          button.addEventListener("click", () => void client.respond(entry.id, {token: entry.token, decision}).then(() => {
            void refresh(); void refreshPending(true);
          }).catch(error => { status.textContent = error.message; }));
          item.append(button);
        }
      }
      prompts.append(item);
    }
  };

  const history = createTranscriptHistory();
  let legacy = false, historyRead = null, historyError = "", historyRetryTimer = null;
  let metadataRetryTimer = null, metadataRetryDelay = 2000;
  let pendingRead = null, queueRead = null, pendingScope = null, queueScope = null;
  let pendingRetryTimer = null, queueRetryTimer = null;
  let pendingRetryDelay = 2000, queueRetryDelay = 2000, pendingLastRead = 0;
  let lastSyncStatus = "", lastThreadStatus = "", destroyed = false, paused = false;
  let generation = 0, sync = null;
  let entryNodes = new Map();
  const renderHistoryControls = () => {
    const older = history.hasOlder && !history.gap && !legacy;
    loadOlder.hidden = !older;
    loadOlder.disabled = Boolean(historyRead);
    retryHistory.hidden = !history.gap && !historyError;
    retryHistory.disabled = Boolean(historyRead);
    historyStatus.textContent = historyRead ? "Loading history…" : historyError ||
      (history.gap ? "Checking earlier messages…" :
        legacy ? "Older history is unavailable on this server." : "");
    historyControls.hidden = !older && !historyStatus.textContent && retryHistory.hidden;
  };
  const laneRead = (timeout) => {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new Error("Conversation request timed out")), timeout);
    return {signal: AbortSignal.any([abort.signal, controller.signal]), finish: () => clearTimeout(timer),
      cancel: () => controller.abort()};
  };
  const entryElement = (entry) => {
    const timestamp = formatTranscriptTimestamp(entry);
    const item = createElement("li", {class: "codex-entry codex-entry-" + (entry.kind || "unknown")});
    const heading = entry.kind === "userMessage" ? "You" : entry.kind === "agentMessage" ? "Codex" : entry.kind;
    const header = createElement("div", {class: "codex-entry-header"});
    header.append(createElement("strong", {}, heading || "Activity"));
    item.append(header);
    item.append(createTranscriptActivity(entry) || createElement("pre", {}, entry.displayText ?? entry.text ?? entry.summary ?? entry.details ?? ""));
    if (entry.attachments?.length) item.append(renderAttachments(entry.attachments, {onRemove: removeAttachment}));
    const footer = createElement("div", {class: "codex-entry-footer"});
    const timeAttributes = {class: "codex-entry-time", title: timestamp.title};
    if (timestamp.dateTime) timeAttributes.datetime = timestamp.dateTime;
    footer.append(createTranscriptCopyButton(entry),
      createElement(timestamp.dateTime ? "time" : "span", timeAttributes, timestamp.text));
    item.append(footer);
    return item;
  };
  const patchEntries = (changed, prepend) => {
    const previousTop = transcript.scrollTop || 0, previousHeight = transcript.scrollHeight || 0;
    const follow = !prepend && ((transcript.scrollHeight || 0) -
      (transcript.clientHeight || 0) - previousTop <= 48);
    const bounds = transcript.getBoundingClientRect?.();
    const anchor = bounds && [...transcript.children].find(node =>
      node.getBoundingClientRect?.().bottom > bounds.top);
    const anchorKey = anchor && [...entryNodes].find(([, node]) => node === anchor)?.[0];
    const anchorTop = anchor?.getBoundingClientRect?.().top;
    const nextNodes = new Map(), desired = [];
    let previousDate = "", dateOccurrence = 0;
    for (const row of history.rows) {
      const timestamp = formatTranscriptTimestamp(row.entry);
      if (timestamp.dateKey && timestamp.dateKey !== previousDate) {
        const dateKey = "date:" + timestamp.dateKey + ":" + dateOccurrence++;
        let separator = entryNodes.get(dateKey);
        if (!separator) {
          separator = createElement("li", {class: "codex-conversation-date"});
          separator.append(createElement("time", {datetime: timestamp.dateKey}, timestamp.dateLabel));
        }
        nextNodes.set(dateKey, separator); desired.push(separator);
        previousDate = timestamp.dateKey;
      }
      let item = entryNodes.get(row.key);
      if (!item || changed.has(row.key)) {
        const replacement = entryElement(row.entry);
        const oldDetails = item?.querySelectorAll?.("details") || [];
        const newDetails = replacement.querySelectorAll?.("details") || [];
        for (let index = 0; index < Math.min(oldDetails.length, newDetails.length); index += 1) {
          newDetails[index].open = oldDetails[index].open;
        }
        item = replacement;
      }
      nextNodes.set(row.key, item); desired.push(item);
    }
    if (typeof transcript.insertBefore === "function") {
      desired.forEach((node, index) => {
        if (transcript.childNodes[index] !== node) transcript.insertBefore(node, transcript.childNodes[index] || null);
      });
      const retained = new Set(desired);
      for (const node of [...transcript.childNodes]) if (!retained.has(node)) node.remove();
    } else transcript.replaceChildren(...desired);
    entryNodes = nextNodes;
    const currentAnchor = nextNodes.get(anchorKey);
    if (follow) transcript.scrollTop = transcript.scrollHeight;
    else if (currentAnchor?.getBoundingClientRect && Number.isFinite(anchorTop)) {
      transcript.scrollTop = previousTop + currentAnchor.getBoundingClientRect().top - anchorTop;
    } else if (prepend) transcript.scrollTop = previousTop + Math.max(0, (transcript.scrollHeight || 0) - previousHeight);
    else transcript.scrollTop = previousTop;
  };
  const acknowledgeRetained = (threadId) => {
    if (!sender || !capabilities.send) return;
    const attempt = sender.pending(), draft = textarea.value;
    void sender.acknowledge(history.entries).then(acknowledged => {
      if (acknowledged && currentThread?.threadId === threadId &&
          draft.trim() === attempt?.message && textarea.value === draft) textarea.value = "";
    }).catch(error => { status.textContent = error.message; });
  };
  const renderThread = (page, isCurrent = () => true, kind = "newest") => {
    if (!isCurrent()) return;
    const update = kind === "older" ? history.applyOlder(page) :
      kind === "repair" ? history.applyRepair(page) : history.applyNewest(page);
    if (update.reset) { generation++; entryNodes.clear(); }
    patchEntries(update.changed, kind !== "newest");
    if (kind === "newest") {
      if (historyRead) page.entries.forEach((entry, index) => {
        historyRead.newerKeys.add(transcriptEntryKey(entry, index, page.entries));
      });
      currentThread = page;
      legacy = Boolean(page.legacy);
      historyError = "";
      applyThreadSettings();
      const active = page.status === "active";
      status.textContent = active ? "Working" : labels.idle;
      if (capabilities.interrupt) interrupt.disabled = !active;
      if (capabilities.queue) queueButton.hidden = !active;
      if (lastThreadStatus !== page.status) void refreshQueue(true);
      lastThreadStatus = page.status;
      if (metadataRetryTimer !== null) clearTimeout(metadataRetryTimer);
      metadataRetryTimer = null;
      if (page.metadataPending && !destroyed && !paused) {
        metadataRetryTimer = setTimeout(() => { metadataRetryTimer = null; sync?.scheduleRefresh(0); }, metadataRetryDelay);
        metadataRetryDelay = Math.min(30_000, metadataRetryDelay * 2);
      } else metadataRetryDelay = 2000;
      if (Date.now() - pendingLastRead >= 5000) void refreshPending();
    }
    acknowledgeRetained(page.threadId);
    renderHistoryControls();
    scheduleHistoryRepair();
  };
  const refreshPending = (force = false) => {
    if (!capabilities.pending || destroyed || paused || globalThis.document?.hidden || pendingRead) return pendingRead || Promise.resolve();
    if (pendingRetryTimer !== null && !force) return Promise.resolve();
    if (pendingRetryTimer !== null) clearTimeout(pendingRetryTimer);
    pendingRetryTimer = null;
    const read = laneRead(12_000), threadId = currentThread?.threadId;
    pendingScope = read;
    pendingRead = client.pending({signal: read.signal}).then(entries => {
      if (destroyed || (threadId && currentThread?.threadId !== threadId)) return;
      if (!Array.isArray(entries)) throw new Error("Invalid pending requests");
      renderPrompts(entries);
      pendingStatus.hidden = true;
      pendingLastRead = Date.now();
      pendingRetryDelay = 2000;
    }).catch(() => {
      if (destroyed || paused || globalThis.document?.hidden) return;
      pendingStatusLabel.textContent = "Requests could not be refreshed. The last result may be out of date.";
      retryPending.hidden = false; pendingStatus.hidden = false;
      pendingRetryTimer = setTimeout(() => { pendingRetryTimer = null; void refreshPending(); }, pendingRetryDelay);
      pendingRetryDelay = Math.min(30_000, pendingRetryDelay * 2);
    }).finally(() => { read.finish(); pendingScope = null; pendingRead = null; });
    return pendingRead;
  };
  const refreshQueue = (force = false) => {
    if (!capabilities.queueRead || destroyed || paused || globalThis.document?.hidden || queueRead) return queueRead || Promise.resolve();
    if (queueRetryTimer !== null && !force) return Promise.resolve();
    if (queueRetryTimer !== null) clearTimeout(queueRetryTimer);
    queueRetryTimer = null;
    const read = laneRead(12_000), threadId = currentThread?.threadId;
    queueScope = read;
    queueRead = Promise.resolve().then(async () => {
      if (capabilities.queue && typeof client.reconcileQueue === "function") await client.reconcileQueue({signal: read.signal});
      return client.queue({signal: read.signal});
    }).then(entries => {
      if (destroyed || (threadId && currentThread?.threadId !== threadId)) return;
      if (!Array.isArray(entries)) throw new Error("Invalid queue");
      renderQueue(entries);
      queueStatus.hidden = true;
      queueRetryDelay = 2000;
    }).catch(() => {
      if (destroyed || paused || globalThis.document?.hidden) return;
      queueStatusLabel.textContent = "Queued messages could not be refreshed. The last result may be out of date.";
      retryQueue.hidden = false; queueStatus.hidden = false;
      queueRetryTimer = setTimeout(() => { queueRetryTimer = null; void refreshQueue(); }, queueRetryDelay);
      queueRetryDelay = Math.min(30_000, queueRetryDelay * 2);
    }).finally(() => { read.finish(); queueScope = null; queueRead = null; });
    return queueRead;
  };
  const runHistoryRead = async (kind) => {
    if (destroyed || paused || historyRead || legacy || globalThis.document?.hidden) return;
    const cursor = kind === "older" ? history.olderCursor : history.repairCursor;
    if (!cursor) return;
    const threadId = history.threadId, readGeneration = generation;
    const readVersion = history.repairVersion;
    const read = laneRead(35_000);
    historyRead = {...read, newerKeys: new Set()}; historyError = ""; renderHistoryControls();
    try {
      const page = await readTranscriptPage(client, {cursor, signal: read.signal, expectedThreadId: threadId});
      if (destroyed || paused || readGeneration !== generation || history.threadId !== threadId ||
          readVersion !== history.repairVersion) return;
      const current = new Map(history.rows.map(row => [row.key, row.entry]));
      const entries = page.entries.map((entry, index) => {
        const key = transcriptEntryKey(entry, index, page.entries);
        return historyRead.newerKeys.has(key) ? current.get(key) || entry : entry;
      });
      renderThread({...page, entries}, () => true, kind);
    } catch (error) {
      if (destroyed || paused || readGeneration !== generation || history.threadId !== threadId ||
          readVersion !== history.repairVersion) return;
      if (error.code === "transcript_cursor_expired" || error.code === "transcript_reset_required") {
        history.invalidateCursor();
        historyError = "History changed. Reconnecting…";
        sync?.scheduleRefresh(0);
      } else if (error.status === 404 ||
          (error.status === 501 && error.code === "transcript_paging_unavailable")) {
        historyError = "Checking history on this server…";
        sync?.scheduleRefresh(0);
      } else historyError = "Earlier messages could not be loaded. Retry.";
    } finally {
      read.finish(); historyRead = null; renderHistoryControls();
      if (!historyError) scheduleHistoryRepair();
    }
  };
  const scheduleHistoryRepair = () => {
    if (!history.gap || !history.repairCursor || historyRetryTimer !== null ||
        historyRead || historyError || legacy || destroyed || paused || globalThis.document?.hidden) return;
    historyRetryTimer = setTimeout(() => { historyRetryTimer = null; void runHistoryRead("repair"); }, 40);
  };
  loadOlder.addEventListener("click", () => { void runHistoryRead("older"); });
  retryHistory.addEventListener("click", () => {
    historyError = "";
    if (history.repairCursor) scheduleHistoryRepair(); else sync?.scheduleRefresh(0);
    renderHistoryControls();
  });
  retryPending.addEventListener("click", () => { void refreshPending(true); });
  retryQueue.addEventListener("click", () => { void refreshQueue(true); });
  const suspendLanes = () => {
    paused = true;
    historyRead?.cancel(); pendingScope?.cancel(); queueScope?.cancel();
    for (const timer of [historyRetryTimer, metadataRetryTimer, pendingRetryTimer, queueRetryTimer]) clearTimeout(timer);
    historyRetryTimer = metadataRetryTimer = pendingRetryTimer = queueRetryTimer = null;
  };
  const onVisibility = () => {
    if (globalThis.document?.hidden) suspendLanes();
    else { paused = false; void refreshPending(true); void refreshQueue(true); scheduleHistoryRepair(); }
  };
  const onPageshow = () => { paused = false; onVisibility(); };
  globalThis.document?.addEventListener?.("visibilitychange", onVisibility);
  globalThis.addEventListener?.("pagehide", suspendLanes);
  globalThis.addEventListener?.("pageshow", onPageshow);
  renderHistoryControls();

  sync = createConversationSync({
    EventSource: EventSourceClass, eventStream: capabilities.eventStream,
    eventsPath: capabilities.eventStream ? client.eventsPath() : null,
    read: signal => readTranscriptPage(client, {signal, legacy}),
    apply: (page, {isCurrent}) => renderThread(page, isCurrent),
    onStateChange: state => {
      renderConnectionStatus(connectionStatus, state, () => sync.retry());
      if (state.status === "connected" && lastSyncStatus !== "connected") {
        void refreshPending(true); void refreshQueue(true);
      }
      lastSyncStatus = state.status;
    },
  });
  const refresh = () => sync.refresh();

  if (capabilities.send) form.addEventListener("submit", async (event) => {
    event.preventDefault();
    send.disabled = true;
    send.textContent = labels.sending;
    try {
      uploads?.lock(true);
      await sender.send(textarea.value, uploads?.ids() || []);
      uploads?.clear();
      await refresh();
    } catch (error) {
      if (error.name !== "AbortError") status.textContent = error.message;
    } finally {
      uploads?.lock(false);
      send.disabled = uploads ? !uploads.ready() : false;
      send.textContent = labels.send;
    }
  });

  if (capabilities.settings) settingsForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    saveSettings.disabled = true;
    try {
      await client.settings(model.value, effort.value, currentThread?.collaborationMode ? mode.value : undefined);
      await refresh();
    } catch (error) {
      status.textContent = error.message;
    } finally {
      saveSettings.disabled = false;
    }
  });

  if (capabilities.queue) queueButton.addEventListener("click", async () => {
    const message = textarea.value.trim();
    if (!message && !uploads?.count()) return;
    try {
      uploads?.lock(true);
      await sender.queue(message, uploads?.ids() || []);
      uploads?.clear();
      textarea.value = "";
      void refreshQueue(true);
      await refresh();
    } catch (error) {
      status.textContent = error.message;
    } finally { uploads?.lock(false); }
  });
  if (capabilities.interrupt) interrupt.addEventListener("click", () => void client.interrupt().then(refresh).catch((error) => {
    status.textContent = error.message;
  }));

  if (capabilities.settings) void loadSettings().catch(error => { status.textContent = error.message; });
  void refresh();
  void refreshPending();
  void refreshQueue();
  const pendingInterval = setInterval(() => { void refreshPending(); }, 15_000);
  const queueInterval = setInterval(() => { void refreshQueue(); }, 30_000);

  return () => {
    destroyed = true;
    clearInterval(pendingInterval); clearInterval(queueInterval);
    suspendLanes();
    globalThis.document?.removeEventListener?.("visibilitychange", onVisibility);
    globalThis.removeEventListener?.("pagehide", suspendLanes);
    globalThis.removeEventListener?.("pageshow", onPageshow);
    uploads?.destroy();
    abort.abort();
    sync.destroy();
    root.replaceChildren();
  };
}

async function payloadForAttachmentRemoval(path) {
  const parsed = new URL(path, globalThis.location.href);
  if (parsed.origin !== globalThis.location.origin) throw new Error("Invalid file endpoint");
  const response = await fetch(`${parsed.pathname}?confirmed=true`, {method: "DELETE", credentials: "same-origin"});
  const result = await response.json();
  if (!response.ok) throw new Error(result.error || "Unable to delete file");
}

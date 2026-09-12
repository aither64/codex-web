import {attachmentIDs, sameAttachments, mountUploads, renderAttachments} from "./uploads.js";
export {attachmentIDs, sameAttachments, mountUploads, renderAttachments, createUploadClient} from "./uploads.js";

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
    const response = await fetchRequest(joinURLPath(target.apiBase, operation), {
      credentials: "same-origin",
      signal: options.signal,
      ...init,
      headers,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.error || `Request failed (${response.status})`);
    return payload;
  };
  const client = {
    thread: () => request("thread"),
    activity: () => request("activity"),
    pending: async () => (await request("pending")) || [],
    models: async () => (await request("models")) || [],
    modes: async () => (await request("collaboration-modes")) || [],
    queue: async () => (await request("queue")) || [],
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
    snooze: (id) => request("respond", {
      method: "POST", body: JSON.stringify({id, snooze: true}),
    }),
    eventsPath: () => joinURLPath(target.apiBase, "events"),
  };
  conversationTargetIdentities.set(client, target);
  return client;
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
  const EventSourceClass = options?.EventSource || globalThis.EventSource;
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
  let stopped = false;
  let currentThread = null;
  let modelCatalog = [];

  const status = createElement("span", {class: "codex-conversation-status"}, labels.reconnecting);
  const transcript = createElement("ol", {class: "codex-conversation-transcript"});
  const prompts = createElement("section", {class: "codex-conversation-prompts"});
  const queue = createElement("section", {class: "codex-conversation-queue"});
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
  const saveSettings = createElement("button", {type: "submit"}, "Save settings");
  settingsForm.append(model, effort, mode, saveSettings);
  const children = [status];
  if (capabilities.settings) children.push(settingsForm);
  children.push(transcript);
  if (capabilities.pending) children.push(prompts);
  if (capabilities.queueRead) children.push(queue);
  if (capabilities.send || capabilities.queue || capabilities.interrupt) children.push(form);
  root.replaceChildren(...children);
  let uploads = null;
  if (options?.uploadBasePath && (capabilities.send || capabilities.queue)) {
    const uploadRoot = document.createElement("div");
    form.insertBefore(uploadRoot, actions);
    uploads = mountUploads(uploadRoot, {
      basePath: options.uploadBasePath, dropTarget: form,
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
    mode.value = currentThread.collaborationMode || mode.value;
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
        start.addEventListener("click", () => void client.startQueue(entry.id).then(refresh));
        const remove = createElement("button", {type: "button"}, "Remove");
        remove.addEventListener("click", () => void client.deleteQueued(entry.id).then(refresh));
        item.append(start, remove);
      }
      queue.append(item);
    }
  };

  const renderPrompts = (entries) => {
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
        const submit = createElement("button", {type: "button"}, "Submit answers");
        submit.addEventListener("click", () => {
          try {
            void client.respond(entry.id, {answers: JSON.parse(answer.value)}).then(refresh);
          } catch (error) {
            status.textContent = error.message;
          }
        });
        item.append(answer, submit);
        if (Number(entry.autoResolutionAtMs) > 0) {
          const snooze = createElement("button", {type: "button"}, "Snooze");
          snooze.addEventListener("click", () => void client.snooze(entry.id).then(refresh));
          item.append(snooze);
        }
      } else {
        for (const decision of entry.availableDecisions || ["accept", "decline"]) {
          const button = createElement("button", {type: "button"}, decision);
          button.addEventListener("click", () => void client.respond(entry.id, {decision}).then(refresh));
          item.append(button);
        }
      }
      prompts.append(item);
    }
  };

  const renderThread = async (thread) => {
    currentThread = thread;
    applyThreadSettings();
    const entries = thread.entries || [];
    const elements = [];
    let previousDate = "";
    for (const entry of entries) {
      const timestamp = formatTranscriptTimestamp(entry);
      if (timestamp.dateKey && timestamp.dateKey !== previousDate) {
        const separator = createElement("li", {class: "codex-conversation-date"});
        separator.append(createElement("time", {datetime: timestamp.dateKey}, timestamp.dateLabel));
        elements.push(separator);
        previousDate = timestamp.dateKey;
      }
      const item = createElement("li", {class: `codex-entry codex-entry-${entry.kind || "unknown"}`});
      const heading = entry.kind === "userMessage" ? "You" : entry.kind === "agentMessage" ? "Codex" : entry.kind;
      const header = createElement("div", {class: "codex-entry-header"});
      header.append(createElement("strong", {}, heading || "Activity"));
      item.append(header);
      item.append(createTranscriptActivity(entry) || createElement("pre", {}, entry.displayText ?? entry.text ?? entry.summary ?? entry.details ?? ""));
      if (entry.attachments?.length) item.append(renderAttachments(entry.attachments, {onRemove: removeAttachment}));
      const footer = createElement("div", {class: "codex-entry-footer"});
      const timeAttributes = {class: "codex-entry-time", title: timestamp.title};
      if (timestamp.dateTime) timeAttributes.datetime = timestamp.dateTime;
      footer.append(
        createTranscriptCopyButton(entry),
        createElement(timestamp.dateTime ? "time" : "span", timeAttributes, timestamp.text),
      );
      item.append(footer);
      elements.push(item);
    }
    transcript.replaceChildren(...elements);
    if (capabilities.send) await sender.acknowledge(entries);
    const active = thread.status === "active";
    status.textContent = active ? "Working" : labels.idle;
    if (capabilities.interrupt) interrupt.disabled = !active;
    if (capabilities.queue) queueButton.hidden = !active;
  };

  async function refresh() {
    if (stopped) return;
    try {
      const [thread, pendingEntries, queuedEntries] = await Promise.all([
        client.thread(),
        capabilities.pending ? client.pending() : Promise.resolve([]),
        capabilities.queueRead ? client.queue() : Promise.resolve([]),
      ]);
      await renderThread(thread);
      if (capabilities.pending) renderPrompts(pendingEntries);
      if (capabilities.queueRead) renderQueue(queuedEntries);
    } catch (error) {
      if (error.name !== "AbortError") status.textContent = error.message;
    }
  }

  if (capabilities.send) form.addEventListener("submit", async (event) => {
    event.preventDefault();
    send.disabled = true;
    send.textContent = labels.sending;
    try {
      uploads?.lock(true);
      await sender.send(textarea.value, uploads?.ids() || []);
      uploads?.clear();
      await refresh();
      if (!sender.pending()) textarea.value = "";
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
      await client.settings(model.value, effort.value, mode.value);
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
      await refresh();
    } catch (error) {
      status.textContent = error.message;
    } finally { uploads?.lock(false); }
  });
  if (capabilities.interrupt) interrupt.addEventListener("click", () => void client.interrupt().then(refresh).catch((error) => {
    status.textContent = error.message;
  }));

  const events = capabilities.eventStream && EventSourceClass ? new EventSourceClass(client.eventsPath()) : null;
  if (events) {
    events.onmessage = refresh;
    events.addEventListener("ready", refresh);
    events.onerror = () => { status.textContent = labels.reconnecting; };
  }
  void Promise.all([
    capabilities.settings ? loadSettings() : Promise.resolve(),
    refresh(),
  ]).catch((error) => {
    status.textContent = error.message;
  });

  return () => {
    stopped = true;
    uploads?.destroy();
    abort.abort();
    events?.close();
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

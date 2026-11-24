const idPattern = /^(?:[0-9a-f]{32}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/;
const digestPattern = /^[0-9a-f]{64}$/;
const states = new Set(["queued", "dispatching", "pending", "sent", "not_sent", "unknown"]);

export function validateDeliveryStatus(value, scope, id = value?.clientUserMessageId) {
  if (value?.version !== 1 || value.scope !== scope || value.clientUserMessageId !== id || !idPattern.test(id) ||
      !digestPattern.test(value.selection) || !states.has(value.state) || !Number.isSafeInteger(value.sequence) || value.sequence < 1 ||
      typeof value.message !== "string" || !["", "steer", "followUp"].includes(value.delivery) ||
      !Number.isSafeInteger(value.createdAtMs) || !Number.isSafeInteger(value.updatedAtMs) || value.createdAtMs <= 0 || value.updatedAtMs < value.createdAtMs ||
      value.attachmentIds !== undefined && (!Array.isArray(value.attachmentIds) || value.attachmentIds.some(id => typeof id !== "string")) ||
      value.state === "sent" && (!value.receipt?.turnId || value.receipt.clientUserMessageId !== id) ||
      value.state !== "sent" && value.receipt != null) throw new Error("Invalid delivery status");
  if (value.retired !== undefined && typeof value.retired !== "boolean" || value.retired === true && value.state !== "unknown") throw new Error("Invalid delivery status");
  return value;
}

export function validateDeliveryCollection(value, scope = value?.scope) {
  if (value?.version !== 1 || typeof scope !== "string" || !scope || value.scope !== scope || typeof value.canEnqueue !== "boolean" ||
      value.canEnqueue && !digestPattern.test(value.selection) || !Array.isArray(value.deliveryModes) ||
      value.deliveryModes.some(mode => !["", "steer", "followUp"].includes(mode)) || !Array.isArray(value.entries) || value.entries.length > 120 ||
      new Set(value.entries.map(entry => entry.clientUserMessageId)).size !== value.entries.length) throw new Error("Invalid delivery collection");
  value.entries.forEach(entry => validateDeliveryStatus(entry, scope));
  if (value.entries.filter(entry => !["sent", "not_sent"].includes(entry.state) && !entry.retired).length > 20) throw new Error("Invalid delivery collection");
  return value;
}

export function deliveryStatusPresentation(entry) {
  const label = {queued: "Queued", dispatching: "Sending", pending: "Awaiting acceptance", sent: "Accepted", not_sent: "Not sent", unknown: "Unconfirmed"}[entry.state];
  return {label: label || "Unconfirmed", description: entry.failure || (entry.state === "sent" ? "The runtime confirmed this input. A response may still be pending." : entry.state === "unknown" ?
    "Check delivery to confirm whether this message was sent." : entry.state === "not_sent" ? "Copy this message to the draft, then send it as a new message." : "")};
}

// The caller supplies the existing verified durable store, scoped to its exact
// endpoint and conversation. Only this create-or-get endpoint may recover a
// lost admission response by repeating the saved request.
export function createDeliverySender({client, store, scope, randomUUID = () => globalThis.crypto.randomUUID()}) {
  if (!scope || !store?.available() || typeof client?.admitDelivery !== "function") throw new Error("Durable delivery storage is unavailable");
  const pending = () => {
    const entries = store.load();
    if (!entries) throw new Error("Stored delivery admission is invalid");
    return entries;
  };
  const submit = async attempt => {
    const {id: _localID, ...input} = attempt;
    let response;
    try { response = await client.admitDelivery(input); }
    catch (error) {
      const proof = error?.proof, request = proof?.request;
      if (error?.notQueued === true && proof?.version === 1 && proof.scope === scope && request?.version === 1 &&
          request.clientUserMessageId === attempt.clientUserMessageId && request.selection === attempt.selection &&
          request.message === attempt.message && request.delivery === attempt.delivery &&
          Array.isArray(request.attachmentIds ?? []) && JSON.stringify(request.attachmentIds || []) === JSON.stringify(attempt.attachmentIds || [])) {
        if (!store.remove(attempt.clientUserMessageId)) throw new Error("The message was not queued. This browser could not clear its saved request.");
      }
      throw error;
    }
    const status = validateDeliveryStatus(response, scope, attempt.clientUserMessageId);
    if (status.message !== attempt.message || status.selection !== attempt.selection || status.delivery !== attempt.delivery ||
        JSON.stringify(status.attachmentIds || []) !== JSON.stringify(attempt.attachmentIds || [])) throw new Error("Delivery admission changed the saved input");
    if (!store.remove(attempt.clientUserMessageId)) throw new Error("Your message was saved, but this browser could not clear its saved request.");
    return status;
  };
  return {
    pending,
    async send(message, attachmentIds = [], {selection, delivery = ""} = {}) {
      message = String(message || "").replace(/^\p{White_Space}+|\p{White_Space}+$/gu, "");
      if ((!message && !attachmentIds.length) || !digestPattern.test(selection) || !["", "steer", "followUp"].includes(delivery)) throw new Error("Delivery selection is unavailable");
      const saved = pending();
      let attempt = saved.find(value => value.message === message && value.selection === selection && value.delivery === delivery &&
        JSON.stringify(value.attachmentIds || []) === JSON.stringify(attachmentIds));
      if (saved.length && !attempt) throw new Error("Check the saved delivery before submitting another message.");
      if (!attempt) {
        attempt = {version: 1, clientUserMessageId: randomUUID(), message, attachmentIds: [...attachmentIds], delivery, selection};
        if (!store.store({...attempt, id: attempt.clientUserMessageId})) throw new Error("This browser could not save the message. It has not been submitted.");
      }
      return submit(attempt);
    },
    async recover() {
      const results = [];
      for (const attempt of pending()) results.push(await submit(attempt));
      return results;
    },
  };
}

// Shared status view; all writes use text nodes. Reads never drive native work.
export function mountDeliveryStatuses(root, {client, onMetadata, onCopy, isCurrent = () => true, interval = 2000}) {
  let stopped = false, active = null, timer = null, scope = null, generation = 0, dirty = false;
  const document = root.ownerDocument, rows = new Map();
  const feedback = document.createElement("small");
  feedback.className = "codex-delivery-feedback"; feedback.hidden = true;
  root.hidden = true;
  const render = entries => {
    const ids = new Set(entries.map(entry => entry.clientUserMessageId));
    for (const [id, row] of rows) if (!ids.has(id)) { row.card.remove(); rows.delete(id); }
    entries.forEach((entry, index) => {
      let row = rows.get(entry.clientUserMessageId);
      if (!row) {
        const card = document.createElement("article"), text = document.createElement("span"), status = document.createElement("small");
        card.className = "codex-delivery-row"; text.className = "codex-delivery-message"; status.className = "codex-delivery-state";
        card.dataset.clientUserMessageId = entry.clientUserMessageId;
        status.setAttribute("role", "status"); status.setAttribute("aria-live", "polite"); status.setAttribute("aria-atomic", "true");
        card.append(text, status); row = {card, text, status, action: null, entry}; rows.set(entry.clientUserMessageId, row);
      }
      row.entry = entry;
      const presentation = deliveryStatusPresentation(entry);
      row.card.dataset.deliveryState = entry.state;
      if (row.text.textContent !== entry.message) row.text.textContent = entry.message;
      if (row.status.textContent !== presentation.label) row.status.textContent = presentation.label;
      if (row.description !== presentation.description) { row.status.title = presentation.description; row.description = presentation.description; }
      const announcement = [presentation.label, presentation.description].filter(Boolean).join(". ");
      if (row.announcement !== announcement) { row.status.setAttribute("aria-label", announcement); row.announcement = announcement; }
      const action = entry.state === "unknown" ? "check" : entry.state === "not_sent" && onCopy ? "copy" : null;
      if (action !== row.actionKind) {
        row.action?.remove(); row.action = null; row.actionKind = action;
        if (action) {
          row.action = document.createElement("button"); row.action.type = "button";
          row.action.textContent = action === "check" ? "Check delivery" : "Copy to draft";
          row.action.addEventListener("click", () => action === "check" ? void refresh() : onCopy(row.entry));
          row.card.append(row.action);
        }
      }
      // Keep surviving action nodes mounted so ordinary polling preserves focus.
      if (root.children[index] !== row.card) root.insertBefore(row.card, root.children[index] || null);
    });
    if (feedback.parentElement !== root) root.append(feedback);
    root.hidden = !entries.length;
  };
  const refresh = () => {
    if (stopped || !isCurrent() || document.hidden) return Promise.resolve(null);
    if (active) return active;
    clearTimeout(timer);
    const readGeneration = generation;
    active = Promise.resolve().then(() => client.deliveries()).then(value => {
      const collection = validateDeliveryCollection(value, scope || value.scope);
      if (stopped || !isCurrent() || readGeneration !== generation) return null;
      scope = collection.scope; feedback.hidden = true;
      onMetadata?.(collection); render(collection.entries); return collection;
    }).catch(error => {
      if (!stopped && isCurrent() && readGeneration === generation) {
        onMetadata?.(null, error); feedback.textContent = "Could not refresh delivery status."; feedback.hidden = false;
      }
      return null;
    }).then(result => {
      active = null;
      // A POST invalidates a read already in flight. Join exactly one fresh
      // follow-up through this same read path; later collections stay authoritative.
      if (!stopped && dirty) { dirty = false; return refresh(); }
      if (!stopped) timer = setTimeout(refresh, interval);
      return result;
    });
    return active;
  };
  const present = value => {
    if (stopped || !isCurrent()) return false;
    if (!scope) throw new Error("Delivery scope is unavailable");
    const entry = validateDeliveryStatus(value, scope);
    generation++; if (active) dirty = true;
    const entries = [...rows.values()].map(row => row.entry), index = entries.findIndex(row => row.clientUserMessageId === entry.clientUserMessageId);
    if (index < 0) entries.push(entry); else entries[index] = entry;
    feedback.hidden = true; render(entries); return true;
  };
  const visible = () => { if (!document.hidden) void refresh(); };
  document.addEventListener?.("visibilitychange", visible);
  void refresh();
  return {refresh, present, destroy() { stopped = true; clearTimeout(timer); document.removeEventListener?.("visibilitychange", visible); }};
}

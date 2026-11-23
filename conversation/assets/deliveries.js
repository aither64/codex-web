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
  const label = {queued: "Queued", dispatching: "Sending…", pending: "Queued", sent: "Sent", not_sent: "Not sent", unknown: "Outcome unknown"}[entry.state];
  return {label: label || "Outcome unknown", description: entry.failure || (entry.state === "unknown" ?
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
  let stopped = false, active = null, timer = null, scope = null;
  const document = root.ownerDocument;
  const render = collection => {
    const nodes = collection.entries.map(entry => {
      const card = document.createElement("article");
      const text = document.createElement("p"), status = document.createElement("p");
      const presentation = deliveryStatusPresentation(entry);
      text.textContent = entry.message;
      status.textContent = [presentation.label, presentation.description].filter(Boolean).join(". ");
      card.dataset.clientUserMessageId = entry.clientUserMessageId;
      card.append(text, status);
      if (entry.state === "unknown") {
        const check = document.createElement("button"); check.type = "button"; check.textContent = "Check delivery";
        check.addEventListener("click", () => void refresh()); card.append(check);
      }
      if (entry.state === "not_sent" && onCopy) {
        const copy = document.createElement("button"); copy.type = "button"; copy.textContent = "Copy to draft";
        copy.addEventListener("click", () => onCopy(entry)); card.append(copy);
      }
      return card;
    });
    root.replaceChildren(...nodes);
  };
  const refresh = () => {
    if (stopped || !isCurrent() || document.hidden) return Promise.resolve(null);
    if (active) return active;
    clearTimeout(timer);
    active = Promise.resolve().then(() => client.deliveries()).then(value => {
      const collection = validateDeliveryCollection(value, scope || value.scope);
      if (stopped || !isCurrent()) return null;
      scope = collection.scope; onMetadata?.(collection); render(collection); return collection;
    }).catch(error => {
      if (!stopped && isCurrent()) { onMetadata?.(null, error); root.setAttribute("aria-label", error.message); }
      return null;
    }).finally(() => { active = null; if (!stopped) timer = setTimeout(refresh, interval); });
    return active;
  };
  const visible = () => { if (!document.hidden) void refresh(); };
  document.addEventListener?.("visibilitychange", visible);
  void refresh();
  return {refresh, destroy() { stopped = true; clearTimeout(timer); document.removeEventListener?.("visibilitychange", visible); }};
}

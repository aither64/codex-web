const replyStart = "<send_user_message_question_reply>";
const replyEnd = "</send_user_message_question_reply>";

export function asyncQuestionReply(question, answer) {
  // Match the CLI's UTF-8 title budget without cutting a code point.
  let title = "", bytes = 0;
  for (const character of question.title) {
    bytes += new TextEncoder().encode(character).length;
    if (bytes > 512) break;
    title += character;
  }
  return `${replyStart}\n${JSON.stringify([{
    questionItemId: question.id, question: title.replace(/[\r\n]/g, " "), answer,
  }])}\n${replyEnd}`;
}

const terminal = status => ["completed", "failed", "interrupted", "error"].includes(status);

// One owner for inline and pinned controls. The normal composer has its own draft
// and delivery records; question answers still use the ordinary message endpoint.
export function createAsyncQuestions({storage, namespace, send, acknowledge,
  canSend = () => true, randomUUID = () => globalThis.crypto.randomUUID(), onChange = () => {},
  maxMessageBytes = 48 * 1024}) {
  let threadId = "", records = new Map(), pins = new Set();
  const views = new Set();
  const key = (thread, id) => `codex-web:questions:${namespace}:${encodeURIComponent(thread)}:${encodeURIComponent(id)}`;
  const read = id => {
    try {
      const saved = JSON.parse(storage?.getItem(key(threadId, id)) || "null");
      if (saved?.schema === 1 && typeof saved.draft === "string" &&
          (!saved.attempt || typeof saved.attempt.id === "string" && typeof saved.attempt.message === "string" &&
            typeof saved.attempt.answer === "string")) return saved;
    } catch (_) {}
    return {schema: 1, draft: ""};
  };
  const recordFor = id => {
    if (!records.has(id)) records.set(id, {id, threadId, state: read(id), busy: false, error: ""});
    return records.get(id);
  };
  const persist = record => {
    const encoded = JSON.stringify(record.state), recordKey = key(record.threadId, record.id);
    if (!storage) throw new Error("Browser storage is unavailable. Your answer has not been sent.");
    storage.setItem(recordKey, encoded);
    if (storage.getItem(recordKey) !== encoded) throw new Error("Browser storage did not retain the answer.");
  };
  const save = record => {
    try { persist(record); } catch (error) { record.error = error.message; }
  };
  const emit = () => {
    for (const view of views) {
      if (view.root.isConnected === false) views.delete(view);
      else view.update();
    }
    onChange();
  };
  const sendAnswer = async (id, value) => {
    const record = recordFor(id);
    if (!record.question || record.busy || record.state.answer !== undefined || !canSend()) return;
    const answer = String(value || "").trim();
    if (!answer) return;
    if (record.state.attempt && record.state.attempt.answer !== answer) {
      record.error = "Retry the pending answer before choosing a different one.";
      emit(); return;
    }
    record.busy = true; record.error = "";
    const retry = Boolean(record.state.attempt);
    let sending = false;
    try {
      const message = record.state.attempt?.message || asyncQuestionReply(record.question, answer);
      if (new TextEncoder().encode(message).length > maxMessageBytes) {
        throw new Error("Answer too long. Shorten it before sending.");
      }
      if (!record.state.attempt) record.state.attempt = {id: randomUUID(), answer, message};
      persist(record); // Publish the exact retry identity before any network effect.
      emit();
      const attempt = record.state.attempt;
      sending = true;
      await send({threadId: record.threadId, message: attempt.message, id: attempt.id, retry});
      record.state.answer = answer;
      record.state.draft = "";
      record.state.cleared = true;
      save(record);
    } catch (error) {
      // A retry may refer to an earlier uncertain effect. Only a new attempt's
      // local failure or HTTP validation rejection proves it never reached Codex.
      const rejected = !retry && (!sending || error.status === 400 || error.status === 413);
      if (rejected) {
        delete record.state.attempt;
        record.state.draft = answer;
        save(record);
      }
      record.error = rejected ? error.message : `${error.message} Retry this answer to check its delivery.`;
    } finally {
      record.busy = false;
      emit();
    }
  };
  const ordinaryAccepted = () => {
    for (const id of pins) {
      const record = recordFor(id);
      record.state.cleared = true; save(record);
    }
    pins.clear(); emit();
  };
  const update = snapshot => {
    if (snapshot.threadId !== threadId) {
      threadId = snapshot.threadId; records = new Map(); pins = new Set();
    }
    const candidates = new Set(), entries = snapshot.entries || [];
    const latest = snapshot.latestTurnId || entries.at(-1)?.turnId;
    for (const entry of entries) {
      if (entry.kind === "userMessage") {
        if (entry.questionReplies?.length) {
          for (const reply of entry.questionReplies) {
            const matches = [...records.values()].filter(record => record.id === reply.questionItemId ||
              record.itemId === reply.questionItemId);
            if (!matches.length) matches.push(recordFor(reply.questionItemId));
            for (const record of matches) {
              record.state.answer = reply.answer;
              record.state.cleared = true; save(record);
              candidates.delete(record.id);
            }
          }
        } else {
          for (const id of candidates) { const record = recordFor(id); record.state.cleared = true; save(record); }
          candidates.clear();
        }
        const record = [...records.values()].find(record => record.state.attempt?.id === entry.clientUserMessageId);
        if (record && entry.clientUserMessageDigest && acknowledge && !record.acknowledging) {
          record.state.answer = record.state.attempt.answer;
          record.acknowledging = true;
          const attemptId = record.state.attempt.id;
          void acknowledge([{clientUserMessageId: attemptId, digest: entry.clientUserMessageDigest}]).then(result => {
            if (result?.acknowledgedClientUserMessageIds?.includes(attemptId) && record.state.attempt?.id === attemptId) {
              delete record.state.attempt; save(record);
            }
          }).catch(() => {}).finally(() => { record.acknowledging = false; });
        }
      }
      if (entry.kind !== "agentMessage") continue;
      for (const question of entry.questions || []) {
        if (typeof question.id !== "string" || !question.id || typeof question.title !== "string") continue;
        const record = recordFor(question.id);
        record.question = question; record.itemId = entry.itemId;
        const wholeItemAnswer = records.get(entry.itemId)?.state.answer;
        if (wholeItemAnswer !== undefined) record.state.answer = wholeItemAnswer;
        if (terminal(entry.turnStatus) || entry.turnId !== latest) {
          record.state.cleared = true; save(record);
        } else if (!record.state.cleared && !record.state.dismissed && record.state.answer === undefined) candidates.add(record.id);
      }
    }
    pins = candidates;
    emit();
  };
  const createView = (record, pinned) => {
    const document = globalThis.document;
    const root = document.createElement("section");
    root.className = "codex-async-question";
    root.dataset.asyncQuestionId = record.id;
    root.dataset.asyncQuestionThread = record.threadId;
    const title = document.createElement("p"); title.className = "codex-question-title";
    const actions = document.createElement("div"); actions.className = "codex-question-options";
    const input = document.createElement("textarea"); input.rows = 2;
    input.setAttribute("aria-label", `Answer: ${record.question.title}`);
    input.placeholder = "Type an answer";
    const submit = document.createElement("button"); submit.type = "button"; submit.textContent = "Send answer";
    const dismiss = document.createElement("button"); dismiss.type = "button"; dismiss.textContent = "Dismiss";
    dismiss.hidden = !pinned;
    const retry = document.createElement("button"); retry.type = "button"; retry.textContent = "Retry answer";
    const status = document.createElement("p"); status.setAttribute("role", "status");
    const buttons = (record.question.options || []).map(label => {
      const button = document.createElement("button"); button.type = "button"; button.textContent = label;
      button.addEventListener("click", () => { void sendAnswer(record.id, label); });
      return button;
    });
    actions.append(...buttons);
    root.append(title, actions, input, submit, dismiss, status, retry);
    input.addEventListener("input", () => { record.state.draft = input.value; save(record); emit(); });
    submit.addEventListener("click", () => { void sendAnswer(record.id, input.value); });
    retry.addEventListener("click", () => { void sendAnswer(record.id, record.state.attempt?.answer); });
    dismiss.addEventListener("click", () => { record.state.dismissed = true; save(record); pins.delete(record.id); emit(); });
    const view = {root, update: () => {
      const answered = record.state.answer !== undefined;
      const enabled = record.threadId === threadId && canSend();
      title.textContent = record.question.title;
      status.textContent = answered ? `Answered: ${record.state.answer}` : record.busy ? "Sending answer…" :
        record.error || (record.state.dismissed ? "Dismissed" : record.state.cleared ? "No longer pinned" : "Optional question");
      root.dataset.questionState = answered ? "answered" : record.busy ? "sending" : "unanswered";
      actions.hidden = input.hidden = submit.hidden = answered || !enabled;
      dismiss.disabled = !enabled || record.busy;
      if (input.value !== record.state.draft) input.value = record.state.draft;
      input.disabled = record.busy || Boolean(record.state.attempt);
      for (const button of [...buttons, submit]) button.disabled = !enabled || record.busy || Boolean(record.state.attempt);
      retry.hidden = answered || !record.state.attempt || !enabled || record.busy;
    }};
    views.add(view); view.update(); return root;
  };
  return {
    update, ordinaryAccepted, sendAnswer,
    pending: () => [...pins].filter(id => records.get(id)?.state.answer === undefined),
    state: id => records.get(id)?.state,
    inline(entry) {
      const entries = (entry.questions || []).map(question => records.get(question.id)).filter(record => record?.question);
      if (!entries.length) return null;
      const group = globalThis.document.createElement("div"); group.className = "codex-inline-questions";
      group.append(...entries.map(record => createView(record, false))); return group;
    },
    renderPinned(container) {
      const ids = this.pending();
      const existing = new Map([...container.children].filter(node => node.dataset.asyncQuestionThread === threadId)
        .map(node => [node.dataset.asyncQuestionId, node]));
      const nodes = ids.map(id => existing.get(id) || createView(records.get(id), true));
      for (const node of [...container.children]) if (!nodes.includes(node)) node.remove();
      nodes.forEach((node, index) => { if (container.children[index] !== node) container.insertBefore(node, container.children[index] || null); });
      container.hidden = !nodes.length || !canSend();
    },
  };
}

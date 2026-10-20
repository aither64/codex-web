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
let nextViewId = 0;
const answerText = (choice, note) => [choice, note].map(value => String(value || "").trim()).filter(Boolean).join("\n\n");

// One owner for inline and pinned controls. The normal composer has its own draft
// and delivery records; question answers still use the ordinary message endpoint.
export function createAsyncQuestions({storage, namespace, send, acknowledge,
  canSend = () => true, randomUUID = () => globalThis.crypto.randomUUID(), onChange = () => {},
  onPanelEmpty = () => {}, maxMessageBytes = 48 * 1024}) {
  let threadId = "", records = new Map(), pins = new Set();
  const views = new Set();
  const panels = new Map();
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
        const record = entry.clientUserMessageId ? [...records.values()].find(record =>
          record.state.attempt && record.state.attempt.id === entry.clientUserMessageId) : undefined;
        const attempt = record?.state.attempt;
        if (attempt && entry.clientUserMessageDigest && acknowledge && !record.acknowledging) {
          record.state.answer = attempt.answer;
          record.acknowledging = true;
          const attemptId = attempt.id;
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
        if (record.state.draft === "" && record.state.choice === undefined && record.state.note === undefined &&
            !record.state.attempt && record.state.answer === undefined && question.options?.length) {
          const choice = question.options.find(option => /\(recommended\)\s*$/i.test(option)) ?? question.options[0];
          Object.assign(record.state, {choice, note: "", draft: answerText(choice, "")});
          save(record);
        }
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
  const draftFor = record => {
    const {choice, note, draft} = record.state;
    // The complete string remains readable by older browsers. An older editor
    // may change it without updating our optional structured draft fields.
    if (typeof note === "string" && (choice === "" || (record.question.options || []).includes(choice)) &&
        answerText(choice, note) === draft) return {choice, note};
    return {choice: "", note: draft};
  };
  const createView = (record, pinned) => {
    const document = globalThis.document;
    const root = document.createElement("section");
    root.className = "codex-async-question";
    root.dataset.asyncQuestionId = record.id;
    root.dataset.asyncQuestionThread = record.threadId;
    const viewId = `codex-question-${++nextViewId}`;
    const title = document.createElement("p"); title.className = "codex-question-title"; title.id = viewId; title.tabIndex = -1;
    const actions = document.createElement("div"); actions.className = "codex-question-options";
    actions.setAttribute("role", "radiogroup"); actions.setAttribute("aria-labelledby", viewId);
    const input = document.createElement("textarea"); input.rows = 2;
    input.setAttribute("aria-label", `Answer: ${record.question.title}`);
    input.placeholder = record.question.options?.length ? "Add details or write your own answer…" : "Type an answer";
    const submit = document.createElement("button"); submit.type = "button"; submit.textContent = "Send answer";
    submit.className = "codex-question-action";
    const dismiss = document.createElement("button"); dismiss.type = "button"; dismiss.textContent = "Dismiss";
    dismiss.className = "codex-question-action";
    dismiss.hidden = !pinned;
    const retry = document.createElement("button"); retry.type = "button"; retry.textContent = "Retry answer";
    retry.className = "codex-question-action";
    const status = document.createElement("p"); status.setAttribute("role", "status");
    const updateDraft = (choice, note) => {
      Object.assign(record.state, {choice, note, draft: answerText(choice, note)});
      save(record); emit();
    };
    const radios = (record.question.options?.length ? [...record.question.options, ""] : []).map((choice, index) => {
      const label = document.createElement("label"); label.className = "codex-question-option";
      const radio = document.createElement("input"); radio.type = "radio"; radio.name = viewId; radio.value = String(index);
      const text = document.createElement("span"); text.textContent = choice || "Other";
      radio.addEventListener("change", () => { if (radio.checked) updateDraft(choice, draftFor(record).note); });
      label.append(radio, text); actions.append(label);
      return {radio, choice};
    });
    const footer = document.createElement("div"); footer.className = "codex-question-actions";
    footer.append(submit, dismiss, retry);
    root.append(title, actions, input, footer, status);
    input.addEventListener("input", () => { updateDraft(draftFor(record).choice, input.value); });
    submit.addEventListener("click", () => { record.pinnedAction = pinned; void sendAnswer(record.id, record.state.draft); });
    retry.addEventListener("click", () => { record.pinnedAction = pinned; void sendAnswer(record.id, record.state.attempt?.answer); });
    dismiss.addEventListener("click", () => { record.pinnedAction = pinned; record.state.dismissed = true; save(record); pins.delete(record.id); emit(); });
    const view = {root, update: () => {
      const answered = record.state.answer !== undefined;
      const enabled = record.threadId === threadId && canSend();
      const draft = draftFor(record);
      title.textContent = record.question.title;
      status.textContent = answered ? `Answered: ${record.state.answer}` : record.busy ? "Sending answer…" :
        record.error || (record.state.dismissed ? "Dismissed" : "");
      status.hidden = !status.textContent;
      root.dataset.questionState = answered ? "answered" : record.busy ? "sending" : "unanswered";
      actions.hidden = answered || !enabled || !radios.length;
      input.hidden = submit.hidden = answered || !enabled;
      dismiss.hidden = !pinned || answered;
      dismiss.disabled = !enabled || record.busy;
      if (input.value !== draft.note) input.value = draft.note;
      input.disabled = record.busy || Boolean(record.state.attempt);
      for (const {radio, choice} of radios) {
        radio.checked = draft.choice === choice;
        radio.disabled = !enabled || record.busy || Boolean(record.state.attempt);
      }
      submit.disabled = !enabled || record.busy || Boolean(record.state.attempt) || !record.state.draft.trim();
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
      const document = globalThis.document;
      let panel = panels.get(container);
      if (!panel || panel.threadId !== threadId) {
        const toggle = document.createElement("button"); toggle.type = "button"; toggle.className = "codex-question-toggle";
        const content = document.createElement("div"); content.id = `codex-question-panel-${++nextViewId}`;
        content.className = "codex-question-carousel";
        toggle.setAttribute("aria-controls", content.id);
        const navigation = document.createElement("div"); navigation.className = "codex-question-navigation";
        const back = document.createElement("button"); back.type = "button"; back.textContent = "←";
        const next = document.createElement("button"); next.type = "button"; next.textContent = "→";
        back.setAttribute("aria-label", "Previous question"); back.title = "Previous question";
        next.setAttribute("aria-label", "Next question"); next.title = "Next question";
        back.className = next.className = "codex-question-action";
        const index = document.createElement("div"); index.className = "codex-question-index";
        index.setAttribute("role", "group"); index.setAttribute("aria-label", "Choose a question");
        const progress = document.createElement("span"); progress.setAttribute("role", "status");
        progress.className = "codex-question-progress";
        navigation.append(back, index, next, progress);
        const body = document.createElement("div"); body.className = "codex-question-pages";
        content.append(navigation, body); container.replaceChildren(toggle, content);
        panel = {threadId, toggle, content, back, next, progress, index, body, ids: [], currentId: "", expanded: false};
        panels.set(container, panel);
        toggle.addEventListener("click", () => { panel.expanded = !panel.expanded; this.renderPinned(container); });
        panel.navigate = id => {
          if (panel.ids.includes(id)) {
            panel.currentId = id; this.renderPinned(container);
            panel.body.querySelector(`section:not([hidden]) .codex-question-title`)?.focus({preventScroll: true});
          }
        };
        back.addEventListener("click", () => panel.navigate(panel.ids[panel.ids.indexOf(panel.currentId) - 1]));
        next.addEventListener("click", () => panel.navigate(panel.ids[panel.ids.indexOf(panel.currentId) + 1]));
      }
      const previousIndex = panel.ids.indexOf(panel.currentId);
      const focus = document.activeElement;
      const focusedQuestion = focus?.closest?.(".codex-async-question");
      const losingQuestion = focusedQuestion && panel.body.contains(focusedQuestion) && !ids.includes(focusedQuestion.dataset.asyncQuestionId);
      const losingIndex = focus?.parentElement === panel.index && !ids.includes(focus.dataset.questionId);
      const removedAction = panel.ids.some(id => !ids.includes(id) && records.get(id)?.pinnedAction);
      for (const id of panel.ids) if (!ids.includes(id) && records.has(id)) records.get(id).pinnedAction = false;
      if (!ids.includes(panel.currentId)) {
        const next = panel.ids.slice(previousIndex + 1).find(id => ids.includes(id));
        const previous = panel.ids.slice(0, Math.max(previousIndex, 0)).reverse().find(id => ids.includes(id));
        panel.currentId = next || previous || ids[0] || "";
      }
      panel.ids = ids;
      if (!ids.length) panel.expanded = false;
      const buttons = new Map([...panel.index.children].map(node => [node.dataset.questionId, node]));
      for (const [id, button] of buttons) if (!ids.includes(id)) button.remove();
      ids.forEach((id, position) => {
        let button = buttons.get(id);
        if (!button) {
          button = document.createElement("button"); button.type = "button";
          button.dataset.questionId = id;
          button.addEventListener("click", () => panel.navigate(id));
        }
        button.textContent = String(position + 1);
        button.title = `Question ${position + 1}: ${records.get(id).question.title}`;
        button.setAttribute("aria-label", button.title);
        if (id === panel.currentId) button.setAttribute("aria-current", "step");
        else button.removeAttribute("aria-current");
        if (panel.index.children[position] !== button) panel.index.insertBefore(button, panel.index.children[position] || null);
      });
      const existing = new Map([...panel.body.children].map(node => [node.dataset.asyncQuestionId, node]));
      for (const [id, node] of existing) if (!ids.includes(id)) node.remove();
      for (const id of ids) {
        const node = existing.get(id) || createView(records.get(id), true);
        if (!existing.has(id)) panel.body.append(node);
        node.hidden = id !== panel.currentId;
      }
      const index = ids.indexOf(panel.currentId);
      panel.toggle.textContent = `${ids.length} optional ${ids.length === 1 ? "question" : "questions"} ${panel.expanded ? "· Collapse ▴" : "▾"}`;
      panel.toggle.setAttribute("aria-expanded", String(panel.expanded));
      panel.content.hidden = !panel.expanded;
      panel.progress.textContent = `Question ${index + 1} of ${ids.length}`;
      panel.back.disabled = index <= 0; panel.next.disabled = index >= ids.length - 1;
      container.hidden = !ids.length || !canSend();
      if (losingQuestion || losingIndex || removedAction && (container.contains(focus) || focus === document.body)) {
        if (!ids.length && canSend()) onPanelEmpty();
        else if (!container.hidden) {
          const target = panel.expanded ? panel.body.querySelector("section:not([hidden]) .codex-question-title") : panel.toggle;
          target?.focus({preventScroll: true});
        }
      }
    },
  };
}

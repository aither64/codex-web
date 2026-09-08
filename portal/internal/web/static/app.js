(() => {
  const apiPath = (slug, operation) => `/api/sessions/${encodeURIComponent(slug)}/${operation}`;
  const createRequest = (fetchRequest) => async (path, options = {}) => {
    const response = await fetchRequest(path, {
      ...options,
      headers: {"Content-Type": "application/json", ...(options.headers || {})},
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data.error || `Request failed (${response.status})`);
    return data;
  };
  const createSessionClient = (slug, request) => ({
    thread: () => request(apiPath(slug, "thread")),
    pending: async () => (await request(apiPath(slug, "pending"))) || [],
    modes: async () => (await request("/api/collaboration-modes")) || [],
    queue: async () => (await request(apiPath(slug, "queue"))) || [],
    message: (message, clientUserMessageId) => request(apiPath(slug, "message"), {
      method: "POST", body: JSON.stringify({message, clientUserMessageId}),
    }),
    queueMessage: (message, clientUserMessageId) => request(apiPath(slug, "queue"), {
      method: "POST", body: JSON.stringify({message, clientUserMessageId}),
    }),
    deleteQueued: (id) => request(`${apiPath(slug, "queue")}/${encodeURIComponent(id)}`, {
      method: "DELETE",
    }),
    startQueue: (queuedSubmissionId) => request(`${apiPath(slug, "queue")}/start`, {
      method: "POST", body: JSON.stringify({queuedSubmissionId}),
    }),
    settings: (model, reasoningEffort, collaborationMode) => {
      const body = {};
      if (model !== undefined) body.model = model;
      if (reasoningEffort !== undefined) body.reasoningEffort = reasoningEffort;
      if (collaborationMode !== undefined) body.collaborationMode = collaborationMode;
      return request(apiPath(slug, "settings"), {
        method: "POST", body: JSON.stringify(body),
      });
    },
    fork: (name, creationDate, model, reasoningEffort) => request(apiPath(slug, "fork"), {
      method: "POST", body: JSON.stringify({name, creationDate, model, reasoningEffort}),
    }),
    releaseCluster: (kind) => request(apiPath(slug, "release-cluster"), {
      method: "POST", body: JSON.stringify({kind}),
    }),
    artifactPreview: (path) => request(`${apiPath(slug, "artifact-preview")}?path=${encodeURIComponent(path)}`),
    archive: (mode) => request(apiPath(slug, "archive"), {
      method: "POST", body: JSON.stringify({mode}),
    }),
    revive: (allowAbandoned) => request(apiPath(slug, "revive"), {
      method: "POST", body: JSON.stringify({allowAbandoned}),
    }),
    deleteSession: (confirmation, force) => request(apiPath(slug, "delete"), {
      method: "POST", body: JSON.stringify({confirmation, force}),
    }),
    operation: () => request(apiPath(slug, "operation")),
    interrupt: () => request(apiPath(slug, "interrupt"), {method: "POST", body: "{}"}),
    implementPlan: (payload) => request(apiPath(slug, "implement-plan"), {
      method: "POST", body: JSON.stringify(payload),
    }),
    respond: (id, payload) => request(apiPath(slug, "respond"), {
      method: "POST", body: JSON.stringify({id, ...payload}),
    }),
    snooze: (id) => request(apiPath(slug, "respond"), {
      method: "POST", body: JSON.stringify({id, snooze: true}),
    }),
    eventsPath: () => apiPath(slug, "events"),
  });
  const automaticReasoningLabel = () => "Automatic";
  const messageActionLabel = (active) => active ? "Steer now" : "Send";
  const shouldSubmitMessage = (event) => (
    event.key === "Enter" && !event.shiftKey && !event.isComposing
  );
  const shouldFollowTranscript = (element, threshold = 48) => (
    element.scrollHeight - element.clientHeight - element.scrollTop <= threshold
  );
  const transcriptEntryKey = (entry, index, entries = []) => {
    const turnID = entry?.turnId || "";
    const itemID = entry?.itemId || "";
    if (turnID && itemID) return JSON.stringify([turnID, itemID]);
    let occurrence = 0;
    for (let priorIndex = 0; priorIndex < index; priorIndex += 1) {
      const prior = entries[priorIndex];
      if (!prior?.itemId && (prior?.turnId || "") === turnID &&
          (prior?.kind || "") === (entry?.kind || "")) occurrence += 1;
    }
    return JSON.stringify([turnID, itemID, entry?.kind || "", occurrence]);
  };
  const captureTranscriptDisclosureState = (container) => {
    const states = new Map();
    for (const element of container.querySelectorAll("[data-transcript-entry-key]")) {
      const disclosure = element.querySelector("details");
      if (disclosure) states.set(element.dataset.transcriptEntryKey, disclosure.open);
    }
    return states;
  };
  const wrapMarkdownTables = (container) => {
    for (const table of container.querySelectorAll("table")) {
      if (table.parentElement?.classList.contains("table-scroll")) continue;
      const wrapper = table.ownerDocument.createElement("div");
      wrapper.className = "table-scroll";
      table.before(wrapper);
      wrapper.append(table);
    }
  };
  const encodeQuestionAnswer = (question, draft = {}) => {
    if (draft.kind === "option" && draft.choice) {
      const values = [draft.choice];
      if ((draft.note || "").trim()) values.push(`user_note: ${draft.note.trim()}`);
      return values;
    }
    if (draft.kind === "other") {
      if ((draft.note || "").trim()) return [`user_note: ${draft.note.trim()}`];
      return [];
    }
    if (draft.kind === "freeform" && (draft.note || "").trim()) {
      return [`user_note: ${draft.note.trim()}`];
    }
    return [];
  };
  const attemptStoragePrefix = (kind, slug, threadId) => (
    `workspace-portal.${kind}-attempt.${encodeURIComponent(slug)}.${encodeURIComponent(threadId)}.`
  );
  const queueAttemptStoragePrefix = (slug, threadId) => attemptStoragePrefix("queue", slug, threadId);
  const sendAttemptStoragePrefix = (slug, threadId) => attemptStoragePrefix("send", slug, threadId);
  const queueAttemptStorageKey = (slug, threadId, id) => (
    `${queueAttemptStoragePrefix(slug, threadId)}${id}`
  );
  const validQueueAttemptId = (id) => (
    typeof id === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)
  );
  const loadQueueAttempts = (storage, slug, threadId) => {
    if (!storage) return null;
    try {
      if (!threadId || typeof storage.length !== "number" || typeof storage.key !== "function") return null;
      const prefix = queueAttemptStoragePrefix(slug, threadId);
      const keys = [];
      for (let index = 0; index < storage.length; index += 1) {
        const key = storage.key(index);
        if (typeof key === "string" && key.startsWith(prefix)) keys.push(key);
      }
      const attempts = [];
      for (const key of keys.sort()) {
        const id = key.slice(prefix.length);
        const encoded = storage.getItem(key);
        if (encoded === null) continue;
        const attempt = JSON.parse(encoded);
        if (!validQueueAttemptId(id) || !attempt || typeof attempt.message !== "string" || !attempt.message) {
          return null;
        }
        attempts.push({id, message: attempt.message});
      }
      return attempts;
    } catch (_error) {
      return null;
    }
  };
  const requireQueueAttempts = (storage, slug, threadId) => {
    const attempts = loadQueueAttempts(storage, slug, threadId);
    if (attempts === null) {
      throw new Error("Browser storage is unavailable; messages cannot be submitted safely.");
    }
    return attempts;
  };
  const storeQueueAttempt = (storage, slug, threadId, attempt) => {
    if (!storage || !threadId || !attempt || !validQueueAttemptId(attempt.id) ||
        typeof attempt.message !== "string" || !attempt.message) return false;
    try {
      storage.setItem(
        queueAttemptStorageKey(slug, threadId, attempt.id),
        JSON.stringify({message: attempt.message}),
      );
      return true;
    } catch (_error) {
      return false;
    }
  };
  const deleteQueueAttempt = (storage, slug, threadId, id) => {
    if (!storage || !threadId || !validQueueAttemptId(id)) return false;
    try {
      storage.removeItem(queueAttemptStorageKey(slug, threadId, id));
      return true;
    } catch (_error) {
      return false;
    }
  };
  const sendAttemptStorageKey = (slug, threadId, id) => (
    `${sendAttemptStoragePrefix(slug, threadId)}${id}`
  );
  const loadSendAttempts = (storage, slug, threadId) => {
    if (!storage) return null;
    try {
      const prefix = sendAttemptStoragePrefix(slug, threadId);
      const attempts = [];
      for (let index = 0; index < storage.length; index += 1) {
        const key = storage.key(index);
        if (typeof key !== "string" || !key.startsWith(prefix)) continue;
        const id = key.slice(prefix.length);
        const attempt = JSON.parse(storage.getItem(key));
        if (!validQueueAttemptId(id) || !attempt || typeof attempt.message !== "string" || !attempt.message ||
            (attempt.context !== undefined && typeof attempt.context !== "string")) return null;
        attempts.push({
          id, message: attempt.message, steered: Boolean(attempt.steered),
          context: attempt.context || "",
        });
      }
      return attempts;
    } catch (_error) {
      return null;
    }
  };
  const storeSendAttempt = (storage, slug, threadId, attempt) => {
    if (!storage || !threadId || !attempt || !validQueueAttemptId(attempt.id) ||
        typeof attempt.message !== "string" || !attempt.message) return false;
    try {
      storage.setItem(
        sendAttemptStorageKey(slug, threadId, attempt.id),
        JSON.stringify({
          message: attempt.message, steered: Boolean(attempt.steered),
          context: typeof attempt.context === "string" ? attempt.context : "",
        }),
      );
      return true;
    } catch (_error) {
      return false;
    }
  };
  const deleteSendAttempt = (storage, slug, threadId, id) => {
    if (!storage || !threadId || !validQueueAttemptId(id)) return false;
    try {
      storage.removeItem(sendAttemptStorageKey(slug, threadId, id));
      return true;
    } catch (_error) {
      return false;
    }
  };
  const matchingSendAttempt = (attempts, message, context = "") => (
    attempts.find((candidate) => (
      candidate.message === message && (candidate.context || "") === context
    ))
  );
  const autoResolutionLabel = (now, visibleAt, dueAt, snoozed) => {
    if (snoozed) return "Auto-resolution paused while you answer.";
    if (!dueAt || now < visibleAt) return "";
    const seconds = Math.max(0, Math.ceil((dueAt - now) / 1000));
    return `Auto-resolves unanswered in ${seconds}s.`;
  };
  const beforeRequestInputAction = async (snooze) => {
    await snooze();
  };
  const requestInputDraftStorageKey = (slug, threadId, requestId) => (
    `workspace-portal.request-input.${encodeURIComponent(slug)}.${encodeURIComponent(threadId)}.${encodeURIComponent(requestId)}`
  );
  const clearThreadStorage = (storage, slug, threadId) => {
    if (!storage || !threadId || typeof storage.length !== "number" ||
        typeof storage.key !== "function") return false;
    const prefixes = [
      queueAttemptStoragePrefix(slug, threadId),
      sendAttemptStoragePrefix(slug, threadId),
      `workspace-portal.request-input.${encodeURIComponent(slug)}.${encodeURIComponent(threadId)}.`,
    ];
    try {
      const keys = [];
      for (let index = 0; index < storage.length; index += 1) {
        const key = storage.key(index);
        if (typeof key === "string" && prefixes.some((prefix) => key.startsWith(prefix))) {
          keys.push(key);
        }
      }
      keys.forEach((key) => storage.removeItem(key));
      return true;
    } catch (_error) {
      return false;
    }
  };
  const loadRequestInputDraft = (storage, slug, threadId, requestId, questions) => {
    if (!storage || !threadId || !requestId || !Array.isArray(questions)) return null;
    try {
      const encoded = storage.getItem(requestInputDraftStorageKey(slug, threadId, requestId));
      if (encoded === null) return null;
      const value = JSON.parse(encoded);
      if (!value || !Number.isInteger(value.page) || value.page < 0 ||
          !Array.isArray(value.drafts) || value.drafts.length !== questions.length) return null;
      const drafts = value.drafts.map((draft, index) => {
        if (questions[index]?.isSecret) return {};
        if (!draft || !["option", "other", "freeform"].includes(draft.kind)) return {};
        if (typeof draft.choice !== "string" || typeof draft.note !== "string") return {};
        return {kind: draft.kind, choice: draft.choice, note: draft.note};
      });
      return {page: Math.min(value.page, Math.max(0, questions.length - 1)), drafts};
    } catch (_error) {
      return null;
    }
  };
  const storeRequestInputDraft = (storage, slug, threadId, requestId, questions, state) => {
    if (!storage || !threadId || !requestId || !Array.isArray(questions) || !state) return false;
    try {
      const drafts = questions.map((question, index) => {
        if (question.isSecret) return {};
        const draft = state.drafts[index] || {};
        return {
          kind: typeof draft.kind === "string" ? draft.kind : "",
          choice: typeof draft.choice === "string" ? draft.choice : "",
          note: typeof draft.note === "string" ? draft.note : "",
        };
      });
      storage.setItem(
        requestInputDraftStorageKey(slug, threadId, requestId),
        JSON.stringify({page: state.page, drafts}),
      );
      return true;
    } catch (_error) {
      return false;
    }
  };
  const deleteRequestInputDraft = (storage, slug, threadId, requestId) => {
    if (!storage || !threadId || !requestId) return false;
    try {
      storage.removeItem(requestInputDraftStorageKey(slug, threadId, requestId));
      return true;
    } catch (_error) {
      return false;
    }
  };
  if (typeof module !== "undefined" && module.exports) {
    module.exports = {
      automaticReasoningLabel, createRequest, createSessionClient,
      autoResolutionLabel, beforeRequestInputAction, clearThreadStorage,
      deleteQueueAttempt, deleteRequestInputDraft, deleteSendAttempt,
      loadQueueAttempts, loadRequestInputDraft, loadSendAttempts, messageActionLabel,
      matchingSendAttempt,
      queueAttemptStorageKey, sendAttemptStorageKey, queueAttemptStoragePrefix,
      requestInputDraftStorageKey, requireQueueAttempts, shouldFollowTranscript,
      shouldSubmitMessage, storeQueueAttempt, storeRequestInputDraft, storeSendAttempt,
      captureTranscriptDisclosureState, encodeQuestionAnswer, transcriptEntryKey,
      wrapMarkdownTables,
    };
    return;
  }

  const body = document.body;
  document.querySelectorAll(".document").forEach(wrapMarkdownTables);
  const slug = body.dataset.session;
  const interactive = body.dataset.interactive === "true";
  const request = createRequest(fetch.bind(globalThis));

  let models = [];
  let collaborationModes = [];
  let currentModel = "";
  let currentEffort = "";
  let currentMode = "";
  let currentThreadId = body.dataset.threadId || "";
  let threadActive = false;
  const modelSelects = Array.from(document.querySelectorAll("[data-model-select]"));
  const effortSelects = Array.from(document.querySelectorAll("[data-effort-select]"));

  const populateEfforts = (modelSelect, effortSelect, selected = "") => {
    if (!effortSelect) return;
    const model = models.find((candidate) => candidate.model === modelSelect.value);
    const existingSettings = Boolean(modelSelect.closest("#codex-settings"));
    effortSelect.replaceChildren();
    if (!existingSettings) {
      const fallback = document.createElement("option");
      fallback.value = "";
      fallback.textContent = automaticReasoningLabel(model);
      effortSelect.append(fallback);
    }
    for (const option of model?.supportedReasoningEfforts || []) {
      const element = document.createElement("option");
      element.value = option.reasoningEffort;
      element.textContent = option.reasoningEffort;
      if (option.description) element.title = option.description;
      effortSelect.append(element);
    }
    const desired = selected || (existingSettings ? model?.defaultReasoningEffort : "") || "";
    if (Array.from(effortSelect.options).some((option) => option.value === desired)) {
      effortSelect.value = desired;
    }
    effortSelect.disabled = !model || threadActive;
  };

  const applyCurrentSettings = () => {
    modelSelects.forEach((modelSelect, index) => {
      const effortSelect = effortSelects[index];
      if (currentModel && Array.from(modelSelect.options).some((option) => option.value === currentModel)) {
        modelSelect.value = currentModel;
      }
      populateEfforts(modelSelect, effortSelect, currentEffort);
      if (modelSelect.closest("#codex-settings")) modelSelect.disabled = threadActive;
    });
    document.getElementById("codex-settings-open")?.toggleAttribute("disabled", threadActive);
    document.getElementById("codex-settings-save")?.toggleAttribute("disabled", threadActive);
    document.getElementById("fork-open")?.toggleAttribute("disabled", threadActive);
    const modeToggle = document.getElementById("codex-mode");
    const availableModes = new Set(collaborationModes.map((mode) => mode.mode));
    const hasModeToggle = availableModes.has("default") && availableModes.has("plan") && currentMode;
    if (modeToggle) modeToggle.hidden = !hasModeToggle;
    document.querySelectorAll("[data-codex-mode]").forEach((button) => {
      const active = button.dataset.codexMode === currentMode;
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", active ? "true" : "false");
    });
  };

  const loadModels = async () => {
    try {
      models = await request("/api/models");
      for (const modelSelect of modelSelects) {
        const allowDefault = !modelSelect.required;
        modelSelect.replaceChildren();
        if (allowDefault) {
          const fallback = document.createElement("option");
          fallback.value = "";
          fallback.textContent = "GPT-6 Astra (default)";
          modelSelect.append(fallback);
        }
        for (const model of models) {
          const option = document.createElement("option");
          option.value = model.model;
          option.textContent = model.displayName;
          option.title = model.description || "";
          modelSelect.append(option);
        }
        if (!currentModel && modelSelect.required) {
          const defaultModel = models.find((model) => model.isDefault);
          if (defaultModel) modelSelect.value = defaultModel.model;
        }
      }
      applyCurrentSettings();
    } catch (_error) {
      for (const modelSelect of modelSelects) {
        modelSelect.replaceChildren();
        const option = document.createElement("option");
        option.value = "";
        option.textContent = "Model catalog unavailable";
        modelSelect.append(option);
        modelSelect.disabled = true;
      }
      for (const effortSelect of effortSelects) effortSelect.disabled = true;
    }
  };

  modelSelects.forEach((modelSelect, index) => {
    modelSelect.addEventListener("change", () => populateEfforts(modelSelect, effortSelects[index]));
  });
  if (modelSelects.length) loadModels();

  document.querySelectorAll("[data-copy]").forEach((button) => {
    button.addEventListener("click", async () => {
      const input = button.parentElement.querySelector("input");
      await navigator.clipboard.writeText(input.value);
      const old = button.textContent;
      button.textContent = "Copied";
      setTimeout(() => { button.textContent = old; }, 1000);
    });
  });

  const tabButtons = document.querySelectorAll("button[data-tab]");
  tabButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const panel = document.getElementById(button.dataset.tab);
      if (!panel) return;
      tabButtons.forEach((candidate) => {
        const selected = candidate === button;
        candidate.classList.toggle("active", selected);
        candidate.setAttribute("aria-selected", selected ? "true" : "false");
      });
      document.querySelectorAll(".tab-panel").forEach((candidate) => {
        candidate.classList.toggle("active", candidate === panel);
      });
    });
  });

  if (!slug) return;
  const client = createSessionClient(slug, request);
  const deleteDialog = document.getElementById("delete-session-dialog");
  const deleteForm = document.getElementById("delete-session-form");
  document.getElementById("delete-session-open")?.addEventListener("click", () => deleteDialog.showModal());
  deleteDialog?.querySelector("[data-dialog-close]")?.addEventListener("click", () => deleteDialog.close());
  deleteForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const controls = Array.from(deleteForm.querySelectorAll("button, input"));
    controls.forEach((control) => { control.disabled = true; });
    try {
      const result = await client.deleteSession(
        deleteForm.elements.confirmation.value,
        deleteForm.elements.force.checked,
      );
      let localStorage = null;
      let sessionStorage = null;
      try { localStorage = globalThis.localStorage; } catch (_error) {}
      try { sessionStorage = globalThis.sessionStorage; } catch (_error) {}
      clearThreadStorage(localStorage, slug, currentThreadId);
      clearThreadStorage(sessionStorage, slug, currentThreadId);
      location.assign(result.redirect || "/");
    } catch (error) {
      alert(error.message);
      controls.forEach((control) => { control.disabled = false; });
    }
  });

  const artifactButtons = Array.from(document.querySelectorAll("[data-artifact-path]"));
  const artifactPreview = document.getElementById("artifact-preview");
  const artifactTitle = document.getElementById("artifact-title");
  const artifactDownload = document.getElementById("artifact-download");
  let selectedArtifactPath = "";
  const showArtifact = async (button) => {
    const path = button.dataset.artifactPath;
    if (!path || !artifactPreview) return;
    selectedArtifactPath = path;
    artifactButtons.forEach((candidate) => candidate.classList.toggle("active", candidate === button));
    artifactTitle.textContent = button.dataset.artifactLabel || path;
    artifactDownload.href = `/artifacts/${encodeURIComponent(slug)}/${path.split("/").map(encodeURIComponent).join("/")}`;
    artifactDownload.hidden = false;
    artifactPreview.replaceChildren();
    const loading = document.createElement("p");
    loading.className = "empty";
    loading.textContent = "Loading artifact…";
    artifactPreview.append(loading);
    try {
      const result = await client.artifactPreview(path);
      if (selectedArtifactPath !== path) return;
      artifactPreview.replaceChildren();
      if (result.kind === "markdown") {
        artifactPreview.innerHTML = result.html || "";
        wrapMarkdownTables(artifactPreview);
      } else if (result.kind === "image") {
        const image = document.createElement("img");
        image.src = result.url;
        image.alt = button.dataset.artifactLabel || path;
        artifactPreview.append(image);
      } else {
        const pre = document.createElement("pre");
        pre.textContent = result.text || "";
        artifactPreview.append(pre);
      }
    } catch (error) {
      if (selectedArtifactPath !== path) return;
      const notice = document.createElement("p");
      notice.className = "notice error";
      notice.textContent = `Unable to preview artifact: ${error.message}`;
      artifactPreview.replaceChildren(notice);
    }
  };
  artifactButtons.forEach((button) => button.addEventListener("click", () => showArtifact(button)));
  document.querySelector('[data-tab="artifacts"]')?.addEventListener("click", () => {
    if (!selectedArtifactPath && artifactButtons[0]) showArtifact(artifactButtons[0]);
  });

  const pollLifecycle = async (button, idleLabel, resetControls) => {
    try {
      const operation = await client.operation();
      if (operation.state === "complete") {
        location.assign(operation.redirect || "/");
        return;
      }
      if (operation.state === "failed") {
        resetControls();
        alert(operation.error || "Session operation failed");
        return;
      }
      setTimeout(() => pollLifecycle(button, idleLabel, resetControls), 1200);
    } catch (error) {
      resetControls();
      alert(error.message);
    }
  };

  const archiveDialog = document.getElementById("archive-session-dialog");
  const archiveForm = document.getElementById("archive-session-form");
  document.getElementById("archive-session-open")?.addEventListener("click", () => archiveDialog.showModal());
  archiveDialog?.querySelector("[data-dialog-close]")?.addEventListener("click", () => archiveDialog.close());
  archiveForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const mode = event.submitter?.value === "abandoned" ? "abandoned" : "complete";
    const button = event.submitter;
    const controls = Array.from(archiveForm.querySelectorAll("button"));
    const idleLabel = mode === "abandoned" ? "Archive as abandoned" : "Archive completed session";
    const resetControls = () => {
      controls.forEach((control) => { control.disabled = false; });
      button.textContent = idleLabel;
    };
    controls.forEach((control) => { control.disabled = true; });
    button.textContent = "Archiving…";
    try {
      await client.archive(mode);
      archiveDialog.close();
      pollLifecycle(button, idleLabel, resetControls);
    } catch (error) {
      alert(error.message);
      resetControls();
    }
  });

  const reviveDialog = document.getElementById("revive-session-dialog");
  const reviveForm = document.getElementById("revive-session-form");
  document.getElementById("revive-session-open")?.addEventListener("click", () => reviveDialog.showModal());
  reviveDialog?.querySelector("[data-dialog-close]")?.addEventListener("click", () => reviveDialog.close());
  reviveForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const button = event.submitter;
    const controls = Array.from(reviveForm.querySelectorAll("button"));
    const resetControls = () => {
      controls.forEach((control) => { control.disabled = false; });
      button.textContent = "Revive session";
    };
    controls.forEach((control) => { control.disabled = true; });
    button.textContent = "Reviving…";
    try {
      await client.revive(body.dataset.lifecycle === "abandoned");
      reviveDialog.close();
      pollLifecycle(button, "Revive session", resetControls);
    } catch (error) {
      alert(error.message);
      resetControls();
    }
  });
  const reviveRetry = document.getElementById("revive-session-retry");
  reviveRetry?.addEventListener("click", async () => {
    const resetControl = () => {
      reviveRetry.disabled = false;
      reviveRetry.textContent = "Retry revive";
    };
    reviveRetry.disabled = true;
    reviveRetry.textContent = "Reviving…";
    try {
      await client.revive(false);
      pollLifecycle(reviveRetry, "Retry revive", resetControl);
    } catch (error) {
      alert(error.message);
      resetControl();
    }
  });
  const transcript = document.getElementById("transcript");
  const pending = document.getElementById("pending");
  const status = document.getElementById("codex-status");
  if (!transcript) return;

  let refreshTimer = null;
  let refreshRunning = false;
  let refreshDirty = false;
  let transcriptInitialized = false;
  let transcriptSignature = "";
  let planRenderGeneration = 0;
  let dismissedPlanSHA = "";
  const pendingMessages = new Map();
  const requestInputDrafts = new Map();
  let sendAttemptStorage = null;
  try { sendAttemptStorage = globalThis.localStorage; } catch (_error) {}
  let requestInputDraftStorage = null;
  try { requestInputDraftStorage = globalThis.sessionStorage; } catch (_error) {}

  const loadMessageReceipts = () => {
    if (!currentThreadId || pendingMessages.size) return;
    const entries = loadSendAttempts(sendAttemptStorage, slug, currentThreadId);
    if (!entries) return;
    for (const entry of entries) {
      pendingMessages.set(entry.id, {...entry, state: "unknown"});
    }
  };

  const renderMessageReceipts = () => {
    const container = document.getElementById("message-receipts");
    if (!container) return;
    container.replaceChildren();
    for (const entry of pendingMessages.values()) {
      const item = document.createElement("div");
      item.className = "message-receipt";
      const statusText = document.createElement("strong");
      statusText.textContent = entry.state === "sending" ? "Sending…" :
        entry.state === "unknown" ? "Outcome unknown — retry to check" :
          entry.steered ? "Steer accepted" : "Message accepted";
      const messageText = document.createElement("span");
      messageText.textContent = entry.message;
      item.append(statusText, messageText);
      container.append(item);
    }
    container.hidden = pendingMessages.size === 0;
  };

  const sha256Hex = async (text) => {
    const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
    return Array.from(new Uint8Array(digest), (value) => value.toString(16).padStart(2, "0")).join("");
  };

  const renderPlanActions = async (payload) => {
    const panel = document.getElementById("plan-actions");
    if (!panel) return;
    const generation = ++planRenderGeneration;
    const plan = [...(payload.entries || [])].reverse().find((entry) => (
      entry.kind === "plan" && entry.turnStatus === "completed" && (entry.text || "").trim()
    ));
    const pendingImplementation = Array.from(pendingMessages.values()).some((entry) => (
      entry.message === "Implement the plan."
    ));
    const eligibleMode = payload.collaborationMode === "plan" ||
      (payload.collaborationMode === "default" && pendingImplementation);
    if (!plan || payload.status === "active" || !eligibleMode) {
      panel.hidden = true;
      return;
    }
    const digest = await sha256Hex(plan.text);
    if (generation !== planRenderGeneration) return;
    panel.dataset.planTurnId = plan.turnId;
    panel.dataset.planSha256 = digest;
    const sameButton = document.getElementById("plan-implement-same");
    if (sameButton) sameButton.textContent = pendingImplementation ? "Check request" : "Implement here";
    panel.hidden = digest === dismissedPlanSHA;
  };

  document.getElementById("new-output")?.addEventListener("click", (event) => {
    transcript.scrollTop = transcript.scrollHeight;
    event.currentTarget.hidden = true;
  });
  transcript.addEventListener("scroll", () => {
    if (shouldFollowTranscript(transcript)) {
      const button = document.getElementById("new-output");
      if (button) button.hidden = true;
    }
  });

  const updateMessageActions = () => {
    const sendButton = document.getElementById("message-send");
    const queueButton = document.getElementById("message-queue");
    const interruptButton = document.getElementById("interrupt");
    if (sendButton) sendButton.textContent = messageActionLabel(threadActive);
    if (queueButton) queueButton.hidden = !threadActive;
    if (interruptButton) interruptButton.disabled = !threadActive;
  };

  const loadCollaborationModes = async () => {
    try {
      collaborationModes = await client.modes();
      applyCurrentSettings();
    } catch (_error) {
      collaborationModes = [];
      applyCurrentSettings();
    }
  };

  const appendMessage = (entry, index, entries, disclosureStates) => {
    const kind = entry.kind === "userMessage" ? "user" : ["agentMessage", "reasoning", "plan"].includes(entry.kind) ? "agent" : entry.kind === "error" ? "error" : "event";
    const text = entry.text || entry.summary || "Codex event";
    const details = entry.details || "";
    const html = entry.html || "";
    const entryKey = transcriptEntryKey(entry, index, entries);
    const element = document.createElement("div");
    element.className = `message ${kind}`;
    element.dataset.transcriptEntryKey = entryKey;
    if (details) {
      const disclosure = document.createElement("details");
      disclosure.open = disclosureStates.get(entryKey) === true;
      const summary = document.createElement("summary");
      summary.textContent = text;
      const pre = document.createElement("pre");
      pre.textContent = details;
      disclosure.append(summary, pre);
      element.append(disclosure);
    } else if (html) {
      element.classList.add("markdown");
      element.innerHTML = html;
      wrapMarkdownTables(element);
    } else {
      element.textContent = text;
    }
    transcript.append(element);
  };

  const renderThread = (payload) => {
    if (!payload.threadId) throw new Error("Codex returned no thread");
    currentThreadId = payload.threadId;
    loadMessageReceipts();
    const follow = !transcriptInitialized || shouldFollowTranscript(transcript);
    const previousTop = transcript.scrollTop;
    const entries = payload.entries || [];
    const nextSignature = JSON.stringify(entries);
    const transcriptChanged = !transcriptInitialized || nextSignature !== transcriptSignature;
    for (const entry of entries) {
      if (entry.clientUserMessageId) {
        pendingMessages.delete(entry.clientUserMessageId);
        deleteSendAttempt(sendAttemptStorage, slug, currentThreadId, entry.clientUserMessageId);
      }
    }
    if (transcriptChanged) {
      const disclosureStates = captureTranscriptDisclosureState(transcript);
      transcript.replaceChildren();
      entries.forEach((entry, index) => appendMessage(entry, index, entries, disclosureStates));
    }
    const threadStatus = payload.status;
    threadActive = threadStatus === "active";
    currentModel = payload.model || currentModel;
    currentEffort = payload.reasoningEffort || currentEffort;
    currentMode = payload.collaborationMode || currentMode;
    applyCurrentSettings();
    updateMessageActions();
    status.textContent = threadStatus || "Connected";
    status.className = `badge ${threadStatus === "active" ? "active" : ""}`;
    const newOutput = document.getElementById("new-output");
    if (transcriptChanged) {
      if (follow) {
        transcript.scrollTop = transcript.scrollHeight;
        if (newOutput) newOutput.hidden = true;
      } else {
        transcript.scrollTop = previousTop;
        if (newOutput && transcriptInitialized) newOutput.hidden = false;
      }
    }
    transcriptInitialized = true;
    transcriptSignature = nextSignature;
    renderMessageReceipts();
    renderPlanActions(payload);
  };

  const refreshThread = async () => {
    try {
      renderThread(await client.thread());
    } catch (error) {
      status.textContent = "Offline";
      status.className = "badge warning";
      if (!transcript.children.length || transcript.querySelector(".empty")) transcript.innerHTML = `<p class="notice error"></p>`;
      const notice = transcript.querySelector(".notice");
      if (notice) notice.textContent = error.message;
    }
  };

  const respond = async (id, payload, container) => {
    const controls = Array.from(container.querySelectorAll("button, input, select, textarea"));
    controls.forEach((control) => { control.disabled = true; });
    try {
      await client.respond(id, payload);
      requestInputDrafts.delete(id);
      deleteRequestInputDraft(requestInputDraftStorage, slug, currentThreadId, id);
      scheduleRefresh(0);
    } catch (error) {
      alert(error.message);
    } finally {
      controls.forEach((control) => { control.disabled = false; });
    }
  };

  const renderApproval = (entry) => {
    const box = document.createElement("article");
    box.className = "approval";
    const title = document.createElement("strong");
    title.textContent = entry.kind === "userInput" ? "Codex needs your input" : entry.method.split("/").slice(-2).join(" · ");
    box.append(title);

    if (entry.kind !== "userInput") {
      const pre = document.createElement("pre");
      pre.textContent = JSON.stringify({request: entry.params, item: entry.item || null}, null, 2);
      box.append(pre);
    }

    if (entry.kind === "terminalOnly") {
      const notice = document.createElement("p");
      notice.className = "notice warning";
      notice.textContent = "This permission request is waiting for an answer in the attached terminal.";
      box.append(notice);
      return box;
    }

    if (entry.error) {
      const notice = document.createElement("p");
      notice.className = "notice error";
      notice.textContent = entry.error;
      box.append(notice);
      return box;
    }
    if (!interactive) return box;
    if (!entry.authorityAvailable) {
      const notice = document.createElement("p");
      notice.className = "notice warning";
      notice.textContent = "The matching thread item is unavailable. Review and answer this request in the terminal.";
      box.append(notice);
      return box;
    }

    if (entry.kind === "userInput") {
      const form = document.createElement("form");
      form.className = "input-wizard stack";
      const questions = entry.questions || [];
      let wizardState = requestInputDrafts.get(entry.id);
      if (!wizardState || wizardState.drafts.length !== questions.length) {
        const saved = loadRequestInputDraft(
          requestInputDraftStorage, slug, currentThreadId, entry.id, questions,
        );
        wizardState = saved ? {...saved, snoozed: false} : {
          drafts: questions.map(() => ({})), page: 0, snoozed: false,
        };
        requestInputDrafts.set(entry.id, wizardState);
      }
      const drafts = wizardState.drafts;
      let page = Math.min(wizardState.page, questions.length - 1);
      const questionPanel = document.createElement("div");
      const progress = document.createElement("p");
      progress.className = "eyebrow";
      const autoResolution = document.createElement("p");
      autoResolution.className = "muted auto-resolution";
      const actions = document.createElement("div");
      actions.className = "approval-actions wizard-actions";
      const back = document.createElement("button");
      back.type = "button";
      back.className = "quiet";
      back.textContent = "Back";
      const next = document.createElement("button");
      next.type = "button";
      const answerField = (question, rows, placeholder) => {
        const field = document.createElement(question.isSecret ? "input" : "textarea");
        field.name = "answer-note";
        field.placeholder = placeholder;
        field.autocomplete = "off";
        if (question.isSecret) {
          field.type = "password";
          field.className = "secret-answer";
          field.spellcheck = false;
        } else {
          field.rows = rows;
        }
        return field;
      };
      const saveDraft = () => {
        const question = questions[page];
        const selected = form.querySelector('input[name="answer-choice"]:checked');
        const note = form.querySelector('[name="answer-note"]')?.value || "";
        if ((question.options || []).length) {
          drafts[page] = selected ? {
            kind: selected.value === "__other__" ? "other" : "option",
            choice: selected.value === "__other__" ? "" : selected.value,
            note,
          } : {};
        } else {
          drafts[page] = {kind: "freeform", note};
        }
        wizardState.page = page;
        storeRequestInputDraft(
          requestInputDraftStorage, slug, currentThreadId, entry.id, questions, wizardState,
        );
      };
      const renderAutoResolution = () => {
        const dueAt = Number(entry.autoResolutionAtMs || 0);
        autoResolution.textContent = autoResolutionLabel(
          Date.now(), Number(entry.autoResolutionVisibleAtMs || 0), dueAt,
          wizardState.snoozed || Boolean(entry.autoResolveSnoozed),
        );
        autoResolution.hidden = !autoResolution.textContent;
      };
      const snoozeAutoResolution = async () => {
        if (entry.isBlocking || wizardState.snoozed || entry.autoResolveSnoozed) return;
        wizardState.snoozed = true;
        renderAutoResolution();
        try {
          await client.snooze(entry.id);
        } catch (error) {
          wizardState.snoozed = false;
          renderAutoResolution();
          alert(`Unable to pause automatic resolution: ${error.message}`);
          scheduleRefresh(0);
        }
      };
      const renderQuestion = () => {
        const question = questions[page];
        const draft = drafts[page] || {};
        progress.textContent = `${question.header} · ${page + 1} of ${questions.length}`;
        questionPanel.replaceChildren();
        const prompt = document.createElement("p");
        prompt.className = "wizard-question";
        prompt.textContent = question.question;
        questionPanel.append(prompt);
        const options = question.options || [];
        if (options.length) {
          const choices = document.createElement("div");
          choices.className = "wizard-options";
          for (const option of options) {
            const label = document.createElement("label");
            label.className = "wizard-option";
            const input = document.createElement("input");
            input.type = "radio";
            input.name = "answer-choice";
            input.value = option.label;
            input.checked = draft.kind === "option" && draft.choice === option.label;
            const text = document.createElement("span");
            const heading = document.createElement("strong");
            heading.textContent = option.label;
            const description = document.createElement("small");
            description.textContent = option.description || "";
            text.append(heading, description);
            label.append(input, text);
            choices.append(label);
          }
          if (question.isOther) {
            const label = document.createElement("label");
            label.className = "wizard-option";
            const input = document.createElement("input");
            input.type = "radio";
            input.name = "answer-choice";
            input.value = "__other__";
            input.checked = draft.kind === "other";
            const text = document.createElement("span");
            const heading = document.createElement("strong");
            heading.textContent = "None of the above";
            text.append(heading);
            label.append(input, text);
            choices.append(label);
          }
          questionPanel.append(choices);
          const note = answerField(question, 2, "Add an optional note…");
          note.value = draft.note || "";
          questionPanel.append(note);
        } else {
          const note = answerField(question, 3, "Type your answer, or leave it unanswered…");
          note.value = draft.note || "";
          questionPanel.append(note);
        }
        back.disabled = page === 0;
        next.textContent = page + 1 === questions.length ? "Submit answers" : "Next";
      };
      back.addEventListener("click", async () => {
        await beforeRequestInputAction(snoozeAutoResolution);
        saveDraft();
        page -= 1;
        wizardState.page = page;
        storeRequestInputDraft(
          requestInputDraftStorage, slug, currentThreadId, entry.id, questions, wizardState,
        );
        renderQuestion();
      });
      next.addEventListener("click", () => form.requestSubmit());
      actions.append(back, next);
      questionPanel.addEventListener("input", () => {
        saveDraft();
        void snoozeAutoResolution();
      });
      questionPanel.addEventListener("change", () => {
        saveDraft();
        void snoozeAutoResolution();
      });
      form.append(progress, autoResolution, questionPanel, actions);
      form.addEventListener("submit", async (event) => {
        event.preventDefault();
        await beforeRequestInputAction(snoozeAutoResolution);
        saveDraft();
        if (drafts[page].kind === "other" && !(drafts[page].note || "").trim()) {
          alert("Type your own answer for None of the above, or clear that selection.");
          return;
        }
        if (page + 1 < questions.length) {
          page += 1;
          wizardState.page = page;
          storeRequestInputDraft(
            requestInputDraftStorage, slug, currentThreadId, entry.id, questions, wizardState,
          );
          renderQuestion();
          return;
        }
        const answers = {};
        let unanswered = 0;
        questions.forEach((question, index) => {
          const values = encodeQuestionAnswer(question, drafts[index]);
          if (!values.length) unanswered += 1;
          answers[question.id] = {answers: values};
        });
        if (unanswered && !confirm(
          `${unanswered} ${unanswered === 1 ? "question is" : "questions are"} unanswered. Submit anyway?`,
        )) {
          return;
        }
        await respond(entry.id, {answers}, form);
      });
      renderQuestion();
      renderAutoResolution();
      if (!entry.isBlocking && !wizardState.snoozed && !entry.autoResolveSnoozed) {
        const timer = setInterval(() => {
          if (!box.isConnected) {
            clearInterval(timer);
            return;
          }
          renderAutoResolution();
        }, 1000);
      }
      box.append(form);
    } else {
      const actions = document.createElement("div");
      actions.className = "approval-actions";
      for (const decision of entry.availableDecisions || []) {
        const button = document.createElement("button");
        button.type = "button";
        button.textContent = decision === "acceptForSession" ? "Approve for session" : decision[0].toUpperCase() + decision.slice(1);
        if (decision === "decline" || decision === "cancel") button.className = "danger";
        button.addEventListener("click", () => respond(entry.id, {decision}, actions));
        actions.append(button);
      }
      box.append(actions);
    }
    return box;
  };

  const refreshPending = async () => {
    if (!interactive) return;
    try {
      const entries = await client.pending();
      const currentInputIDs = new Set(entries.filter((entry) => entry.kind === "userInput").map((entry) => entry.id));
      for (const id of requestInputDrafts.keys()) {
        if (!currentInputIDs.has(id)) {
          requestInputDrafts.delete(id);
          deleteRequestInputDraft(requestInputDraftStorage, slug, currentThreadId, id);
        }
      }
      pending.replaceChildren(...entries.map(renderApproval));
    } catch (error) {
      const notice = document.createElement("p");
      notice.className = "notice error";
      notice.textContent = `Unable to load pending requests: ${error.message}`;
      pending.replaceChildren(notice);
    }
  };

  const queuePanel = document.getElementById("queue-panel");
  const queueList = document.getElementById("queue-list");
  const queueStart = document.getElementById("queue-start");
  let queueStartInFlight = false;

  const renderQueue = (entries) => {
    if (!queuePanel || !queueList) return;
    queueList.replaceChildren();
    for (const entry of entries) {
      const item = document.createElement("li");
      const text = document.createElement("span");
      text.textContent = entry.text || "Queued input";
      const remove = document.createElement("button");
      remove.type = "button";
      remove.className = "quiet queue-remove";
      remove.textContent = "Remove";
      remove.addEventListener("click", async () => {
        remove.disabled = true;
        try {
          await client.deleteQueued(entry.id);
          scheduleRefresh(0);
        } catch (error) {
          alert(error.message);
          remove.disabled = false;
        }
      });
      item.append(text, remove);
      queueList.append(item);
    }
    queuePanel.hidden = entries.length === 0;
    if (queueStart) {
      queueStart.dataset.queuedSubmissionId = entries[0]?.id || "";
      queueStart.hidden = threadActive;
      queueStart.disabled = threadActive || queueStartInFlight;
    }
  };

  const refreshQueue = async () => {
    if (!interactive) return;
    try {
      renderQueue(await client.queue());
    } catch (error) {
      if (!queuePanel || !queueList) return;
      const notice = document.createElement("li");
      notice.className = "notice error";
      notice.textContent = `Unable to load queued messages: ${error.message}`;
      queueList.replaceChildren(notice);
      queuePanel.hidden = false;
      if (queueStart) queueStart.hidden = true;
    }
  };

  const refreshAll = async () => {
    if (refreshRunning) {
      refreshDirty = true;
      return;
    }
    refreshRunning = true;
    do {
      refreshDirty = false;
      await refreshThread();
      await Promise.all([refreshPending(), refreshQueue()]);
    } while (refreshDirty);
    refreshRunning = false;
  };

  function scheduleRefresh(delay = 200) {
    refreshDirty = true;
    if (refreshTimer !== null) return;
    refreshTimer = setTimeout(() => {
      refreshTimer = null;
      refreshAll();
    }, delay);
  }

  const form = document.getElementById("message-form");
  if (form && interactive) {
    const textarea = form.elements.message;
    const queueButton = document.getElementById("message-queue");
    const interruptButton = document.getElementById("interrupt");
    let queueAttemptStorage = null;
    try { queueAttemptStorage = globalThis.localStorage; } catch (_error) {}
    const submitMessage = async (queue) => {
      const message = textarea.value.trim();
      if (!message) return;
      const controls = Array.from(form.querySelectorAll("button, input, select, textarea"));
      controls.forEach((control) => { control.disabled = true; });
      try {
        if (queue) {
          let queueAttempts = requireQueueAttempts(queueAttemptStorage, slug, currentThreadId);
          let attempt = queueAttempts.find((candidate) => candidate.message === message);
          if (!attempt) {
            attempt = {message, id: crypto.randomUUID()};
            if (!storeQueueAttempt(queueAttemptStorage, slug, currentThreadId, attempt)) {
              throw new Error("Browser storage is unavailable; queued messages cannot be submitted safely.");
            }
          }
          await client.queueMessage(message, attempt.id);
          if (!deleteQueueAttempt(queueAttemptStorage, slug, currentThreadId, attempt.id)) {
            throw new Error("The queued message was accepted, but its retry record could not be cleared.");
          }
        } else {
          const queueAttempts = requireQueueAttempts(queueAttemptStorage, slug, currentThreadId);
          if (queueAttempts.some((candidate) => candidate.message === message)) {
            throw new Error("This message has an unresolved queue submission. Use Queue next to reconcile it before sending.");
          }
          const sendAttempts = loadSendAttempts(sendAttemptStorage, slug, currentThreadId);
          if (sendAttempts === null) {
            throw new Error("Browser storage is unavailable; messages cannot be submitted safely.");
          }
          let attempt = matchingSendAttempt(sendAttempts, message);
          if (!attempt) {
            attempt = {id: crypto.randomUUID(), message, steered: threadActive, context: ""};
            if (!storeSendAttempt(sendAttemptStorage, slug, currentThreadId, attempt)) {
              throw new Error("Browser storage is unavailable; messages cannot be submitted safely.");
            }
          }
          const id = attempt.id;
          pendingMessages.set(id, {id, message, state: "sending", steered: attempt.steered});
          renderMessageReceipts();
          const receipt = await client.message(message, id);
          pendingMessages.set(id, {
            id, message, state: "accepted", steered: Boolean(receipt.steered),
          });
          renderMessageReceipts();
        }
        textarea.value = "";
        scheduleRefresh(0);
      } catch (error) {
        for (const [id, entry] of pendingMessages) {
          if (entry.state === "sending" && entry.message === message) {
            pendingMessages.set(id, {...entry, state: "unknown"});
          }
        }
        renderMessageReceipts();
        alert(error.message);
      }
      finally {
        controls.forEach((control) => { control.disabled = false; });
        updateMessageActions();
        textarea.focus();
      }
    };
    textarea.addEventListener("keydown", (event) => {
      if (!shouldSubmitMessage(event)) return;
      event.preventDefault();
      if (!textarea.value.trim()) return;
      form.requestSubmit();
    });
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      await submitMessage(false);
    });
    queueButton.addEventListener("click", () => submitMessage(true));
    interruptButton.addEventListener("click", async () => {
      try {
        await client.interrupt();
        scheduleRefresh(0);
      } catch (error) { alert(error.message); }
    });
    updateMessageActions();
  }

  queueStart?.addEventListener("click", async () => {
    const queuedSubmissionId = queueStart.dataset.queuedSubmissionId;
    if (!queuedSubmissionId) return;
    queueStartInFlight = true;
    queueStart.disabled = true;
    try {
      await client.startQueue(queuedSubmissionId);
    } catch (error) {
      alert(error.message);
    } finally {
      queueStartInFlight = false;
      await refreshQueue();
      scheduleRefresh(0);
    }
  });

  document.querySelectorAll("[data-codex-mode]").forEach((button) => {
    button.addEventListener("click", async () => {
      const controls = Array.from(document.querySelectorAll(
        "[data-codex-mode], #message-form button, #message-form input, #message-form select, #message-form textarea",
      ));
      controls.forEach((control) => { control.disabled = true; });
      try {
        const saved = await client.settings(undefined, undefined, button.dataset.codexMode);
        currentModel = saved.model;
        currentEffort = saved.reasoningEffort;
        currentMode = saved.collaborationMode || currentMode;
        applyCurrentSettings();
        scheduleRefresh(0);
      } catch (error) { alert(error.message); }
      finally {
        controls.forEach((control) => { control.disabled = false; });
        updateMessageActions();
      }
    });
  });

  const planActions = document.getElementById("plan-actions");
  document.getElementById("plan-keep-planning")?.addEventListener("click", () => {
    dismissedPlanSHA = planActions?.dataset.planSha256 || "";
    if (planActions) planActions.hidden = true;
  });
  document.getElementById("plan-implement-same")?.addEventListener("click", async (event) => {
    const button = event.currentTarget;
    const message = "Implement the plan.";
    const context = `plan:${planActions.dataset.planSha256}`;
    const attempts = loadSendAttempts(sendAttemptStorage, slug, currentThreadId);
    if (attempts === null) {
      alert("Browser storage is unavailable; messages cannot be submitted safely.");
      return;
    }
    let attempt = matchingSendAttempt(attempts, message, context);
    if (!attempt) {
      attempt = {id: crypto.randomUUID(), message, steered: false, context};
      if (!storeSendAttempt(sendAttemptStorage, slug, currentThreadId, attempt)) {
        alert("Browser storage is unavailable; messages cannot be submitted safely.");
        return;
      }
    }
    const id = attempt.id;
    pendingMessages.set(id, {
      id, message, state: "sending", steered: false,
    });
    renderMessageReceipts();
    button.disabled = true;
    try {
      const receipt = await client.implementPlan({
        action: "same",
        planTurnId: planActions.dataset.planTurnId,
        planSha256: planActions.dataset.planSha256,
        clientUserMessageId: id,
      });
      pendingMessages.set(id, {
        id, message, state: "accepted", steered: Boolean(receipt.steered),
      });
      currentMode = "default";
      planActions.hidden = true;
      renderMessageReceipts();
      applyCurrentSettings();
      scheduleRefresh(0);
    } catch (error) {
      pendingMessages.set(id, {id, message, state: "unknown", steered: false});
      renderMessageReceipts();
      button.disabled = false;
      alert(error.message);
    }
  });

  const planSessionDialog = document.getElementById("plan-session-dialog");
  const planSessionForm = document.getElementById("plan-session-form");
  document.getElementById("plan-implement-new")?.addEventListener("click", () => planSessionDialog.showModal());
  planSessionDialog?.querySelector("[data-dialog-close]")?.addEventListener("click", () => planSessionDialog.close());
  planSessionForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const controls = Array.from(planSessionForm.querySelectorAll("button, input"));
    controls.forEach((control) => { control.disabled = true; });
    try {
      const result = await client.implementPlan({
        action: "new",
        planTurnId: planActions.dataset.planTurnId,
        planSha256: planActions.dataset.planSha256,
        name: planSessionForm.elements.name.value,
        creationDate: planSessionForm.elements.creationDate.value,
      });
      location.assign(result.url);
    } catch (error) {
      alert(error.message);
      controls.forEach((control) => { control.disabled = false; });
    }
  });

  const settingsForm = document.getElementById("codex-settings");
  const settingsDialog = document.getElementById("codex-settings-dialog");
  document.getElementById("codex-settings-open")?.addEventListener("click", () => settingsDialog.showModal());
  settingsDialog?.querySelector("[data-dialog-close]")?.addEventListener("click", () => settingsDialog.close());
  if (settingsForm && interactive) {
    settingsForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      const controls = Array.from(settingsForm.querySelectorAll("button, select"));
      controls.forEach((control) => { control.disabled = true; });
      try {
        const saved = await client.settings(
          settingsForm.elements.model.value,
          settingsForm.elements.effort.value,
        );
        currentModel = saved.model;
        currentEffort = saved.reasoningEffort;
        applyCurrentSettings();
        settingsDialog.close();
        scheduleRefresh(0);
      } catch (error) { alert(error.message); }
      finally { applyCurrentSettings(); }
    });
  }

  const forkDialog = document.getElementById("fork-dialog");
  const forkForm = document.getElementById("fork-form");
  document.getElementById("fork-open")?.addEventListener("click", () => forkDialog.showModal());
  forkDialog?.querySelector("[data-dialog-close]")?.addEventListener("click", () => forkDialog.close());
  if (forkForm && interactive) {
    forkForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      const controls = Array.from(forkForm.querySelectorAll("button, input, select"));
      controls.forEach((control) => { control.disabled = true; });
      try {
        const result = await client.fork(
          forkForm.elements.name.value,
          forkForm.elements.creationDate.value,
          forkForm.elements.model.value,
          forkForm.elements.effort.value,
        );
        location.assign(result.url);
      } catch (error) {
        alert(error.message);
        controls.forEach((control) => { control.disabled = false; });
      }
    });
  }

  document.querySelectorAll("[data-release-cluster]").forEach((button) => {
    button.addEventListener("click", async () => {
      const kind = button.dataset.releaseCluster;
      if (!confirm("Stop this development cluster and remove its temporary state?")) return;
      button.disabled = true;
      button.textContent = "Releasing…";
      try {
        await client.releaseCluster(kind);
        location.reload();
      } catch (error) {
        alert(error.message);
        button.disabled = false;
        button.textContent = "Release cluster";
      }
    });
  });

  scheduleRefresh(0);
  if (interactive) loadCollaborationModes();
  if (interactive) {
    const events = new EventSource(client.eventsPath());
    events.onopen = () => { scheduleRefresh(0); };
    events.onmessage = () => { scheduleRefresh(); };
    events.onerror = () => { status.textContent = "Reconnecting"; status.className = "badge warning"; };
  }
})();

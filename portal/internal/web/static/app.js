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
    message: (message) => request(apiPath(slug, "message"), {
      method: "POST", body: JSON.stringify({message}),
    }),
    settings: (model, reasoningEffort) => request(apiPath(slug, "settings"), {
      method: "POST", body: JSON.stringify({model, reasoningEffort}),
    }),
    fork: (name, creationDate, model, reasoningEffort) => request(apiPath(slug, "fork"), {
      method: "POST", body: JSON.stringify({name, creationDate, model, reasoningEffort}),
    }),
    releaseCluster: (kind) => request(apiPath(slug, "release-cluster"), {
      method: "POST", body: JSON.stringify({kind}),
    }),
    archive: () => request(apiPath(slug, "archive"), {method: "POST", body: "{}"}),
    operation: () => request(apiPath(slug, "operation")),
    interrupt: () => request(apiPath(slug, "interrupt"), {method: "POST", body: "{}"}),
    respond: (id, payload) => request(apiPath(slug, "respond"), {
      method: "POST", body: JSON.stringify({id, ...payload}),
    }),
    eventsPath: () => apiPath(slug, "events"),
  });
  const automaticReasoningLabel = () => "Automatic";
  if (typeof module !== "undefined" && module.exports) {
    module.exports = {automaticReasoningLabel, createRequest, createSessionClient};
    return;
  }

  const body = document.body;
  const slug = body.dataset.session;
  const interactive = body.dataset.interactive === "true";
  const request = createRequest(fetch.bind(globalThis));

  let models = [];
  let currentModel = "";
  let currentEffort = "";
  let threadActive = false;
  const modelSelects = Array.from(document.querySelectorAll("[data-model-select]"));
  const effortSelects = Array.from(document.querySelectorAll("[data-effort-select]"));

  const populateEfforts = (modelSelect, effortSelect, selected = "") => {
    if (!effortSelect) return;
    const model = models.find((candidate) => candidate.model === modelSelect.value);
    effortSelect.replaceChildren();
    const fallback = document.createElement("option");
    fallback.value = "";
    fallback.textContent = automaticReasoningLabel(model);
    effortSelect.append(fallback);
    for (const option of model?.supportedReasoningEfforts || []) {
      const element = document.createElement("option");
      element.value = option.reasoningEffort;
      element.textContent = option.reasoningEffort;
      if (option.description) element.title = option.description;
      effortSelect.append(element);
    }
    const desired = selected;
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
          fallback.textContent = "System default";
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
  const transcript = document.getElementById("transcript");
  const pending = document.getElementById("pending");
  const status = document.getElementById("codex-status");
  if (!transcript) return;

  let refreshTimer = null;
  let refreshRunning = false;
  let refreshDirty = false;

  const client = createSessionClient(slug, request);

  const appendMessage = (kind, text, details, html) => {
    const element = document.createElement("div");
    element.className = `message ${kind}`;
    if (details) {
      const disclosure = document.createElement("details");
      const summary = document.createElement("summary");
      summary.textContent = text;
      const pre = document.createElement("pre");
      pre.textContent = details;
      disclosure.append(summary, pre);
      element.append(disclosure);
    } else if (html) {
      element.classList.add("markdown");
      element.innerHTML = html;
    } else {
      element.textContent = text;
    }
    transcript.append(element);
  };

  const renderThread = (payload) => {
    if (!payload.threadId) throw new Error("Codex returned no thread");
    transcript.replaceChildren();
    for (const entry of payload.entries || []) {
      const kind = entry.kind === "userMessage" ? "user" : ["agentMessage", "reasoning", "plan"].includes(entry.kind) ? "agent" : entry.kind === "error" ? "error" : "event";
      appendMessage(kind, entry.text || entry.summary || "Codex event", entry.details || "", entry.html || "");
    }
    const threadStatus = payload.status;
    threadActive = threadStatus === "active";
    currentModel = payload.model || currentModel;
    currentEffort = payload.reasoningEffort || currentEffort;
    applyCurrentSettings();
    status.textContent = threadStatus || "Connected";
    status.className = `badge ${threadStatus === "active" ? "active" : ""}`;
    transcript.scrollTop = transcript.scrollHeight;
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
    title.textContent = entry.method.split("/").slice(-2).join(" · ");
    const pre = document.createElement("pre");
    pre.textContent = JSON.stringify({request: entry.params, item: entry.item || null}, null, 2);
    box.append(title, pre);

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
      form.className = "stack";
      for (const question of entry.questions || []) {
        const label = document.createElement("label");
        label.textContent = question.question;
        const options = question.options || [];
        if (options.length) {
          const select = document.createElement("select");
          select.name = `${question.id}-choice`;
          select.required = true;
          for (const option of options) {
            const element = document.createElement("option");
            element.value = option.label;
            element.textContent = `${option.label}${option.description ? `: ${option.description}` : ""}`;
            select.append(element);
          }
          let custom = null;
          if (question.isOther) {
            const other = document.createElement("option");
            other.value = "__other__";
            other.textContent = "Other…";
            select.append(other);
            custom = document.createElement("input");
            custom.name = `${question.id}-other`;
            custom.placeholder = "Custom answer";
            custom.hidden = true;
            select.addEventListener("change", () => {
              custom.hidden = select.value !== "__other__";
              custom.required = !custom.hidden;
              if (!custom.hidden) custom.focus();
            });
          }
          label.append(select);
          if (custom) label.append(custom);
        } else {
          const input = document.createElement("input");
          input.name = `${question.id}-value`;
          input.required = true;
          if (question.isSecret) input.type = "password";
          label.append(input);
        }
        form.append(label);
      }
      const submit = document.createElement("button");
      submit.type = "submit";
      submit.textContent = "Answer";
      form.append(submit);
      form.addEventListener("submit", async (event) => {
        event.preventDefault();
        const answers = {};
        for (const question of entry.questions || []) {
          const options = question.options || [];
          let value;
          if (options.length) {
            value = form.elements[`${question.id}-choice`].value;
            if (value === "__other__") value = form.elements[`${question.id}-other`].value;
          } else {
            value = form.elements[`${question.id}-value`].value;
          }
          answers[question.id] = {answers: [value]};
        }
        await respond(entry.id, {answers}, form);
      });
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
      pending.replaceChildren(...entries.map(renderApproval));
    } catch (error) {
      const notice = document.createElement("p");
      notice.className = "notice error";
      notice.textContent = `Unable to load pending requests: ${error.message}`;
      pending.replaceChildren(notice);
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
      await refreshPending();
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
    textarea.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" || event.shiftKey || event.isComposing) return;
      event.preventDefault();
      if (!textarea.value.trim()) return;
      form.requestSubmit();
    });
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const message = textarea.value.trim();
      if (!message) return;
      const controls = Array.from(form.querySelectorAll("button, input, select, textarea"));
      controls.forEach((control) => { control.disabled = true; });
      try {
        await client.message(message);
        textarea.value = "";
        scheduleRefresh(0);
      } catch (error) { alert(error.message); }
      finally {
        controls.forEach((control) => { control.disabled = false; });
        textarea.focus();
      }
    });
    document.getElementById("interrupt").addEventListener("click", async () => {
      try {
        await client.interrupt();
        scheduleRefresh(0);
      } catch (error) { alert(error.message); }
    });
  }

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

  document.getElementById("finish-session")?.addEventListener("click", async (event) => {
    const button = event.currentTarget;
    if (!confirm("Ask Codex to finish the work and prepare this session for archival?")) return;
    button.disabled = true;
    try {
      await client.message(
        "Prepare this development session for archival. Finish the requested work, tests, reviews, pushes, and deployment. Update plan.md and state.md, and set the lifecycle to complete only when nothing remains. Do not finalize, archive, or stop the session yourself. If anything blocks completion, leave the lifecycle active and explain it.",
      );
      document.querySelector('[data-tab="codex"]')?.click();
      scheduleRefresh(0);
      button.textContent = "Preparation requested";
    } catch (error) {
      alert(error.message);
      button.disabled = false;
    }
  });

  const archiveButton = document.getElementById("archive-session");
  const pollArchive = async () => {
    try {
      const operation = await client.operation();
      if (operation.state === "complete") {
        location.reload();
        return;
      }
      if (operation.state === "failed") {
        archiveButton.disabled = false;
        archiveButton.textContent = "Retry archive";
        alert(operation.error || "Archiving failed");
        return;
      }
      setTimeout(pollArchive, 1200);
    } catch (error) {
      archiveButton.disabled = false;
      archiveButton.textContent = "Retry archive";
      alert(error.message);
    }
  };
  archiveButton?.addEventListener("click", async () => {
    if (!confirm("Release development clusters, archive the session, commit the archive move, and stop its runtime?")) return;
    archiveButton.disabled = true;
    archiveButton.textContent = "Archiving…";
    try {
      await client.archive();
      pollArchive();
    } catch (error) {
      archiveButton.disabled = false;
      archiveButton.textContent = "Archive session";
      alert(error.message);
    }
  });

  scheduleRefresh(0);
  if (interactive) {
    const events = new EventSource(client.eventsPath());
    events.onopen = () => { scheduleRefresh(0); };
    events.onmessage = () => { scheduleRefresh(); };
    events.onerror = () => { status.textContent = "Reconnecting"; status.className = "badge warning"; };
  }
})();

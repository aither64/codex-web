// The application schedules retries. These controls only observe state and
// change its durable preference/cancellation intent through authorized routes.
export function retryStatusText(state, serverNowMs) {
  if (!state) return "Retry status is unavailable.";
  if (state.phase === "submitting" || state.phase === "reconciling") return "Waiting to confirm whether the retry started.";
  if (state.phase === "exhausted") return "Automatic retries stopped after 30 attempts within 5 minutes. Continue manually to try again.";
  if (!state.enabled) return "Automatic retry is off.";
  if (state.phase === "cancelled") return "Automatic retry cancelled.";
  if (state.phase === "scheduled") {
    if (state.class === "capacity") {
      const seconds = Math.max(0, Math.ceil(((state.nextAttemptAtMs ?? serverNowMs) - serverNowMs) / 1000));
      return `Model at capacity. Retrying in ${seconds}s. Attempt ${state.attempt + 1}.`;
    }
    if (state.class === "cybersecurity") return `Cybersecurity turn error. Retrying now. Attempt ${state.attempt + 1}.`;
  }
  if (state.phase === "running") return `Automatic retry started. Attempt ${state.attempt}.`;
  return state.reason || "Auto retry is on.";
}

export function mountRetryControls(root, options) {
  const page = options.document || globalThis.document;
  const browser = options.window || globalThis;
  const now = options.now || (() => globalThis.performance.now());
  const later = options.setTimeout || globalThis.setTimeout;
  const clear = options.clearTimeout || globalThis.clearTimeout;
  const create = (tag, text) => { const node = page.createElement(tag); if (text) node.textContent = text; return node; };
  const label = create("label", "Auto retry");
  const enabled = create("input"); enabled.type = "checkbox"; enabled.disabled = true;
  label.prepend(enabled);
  const status = create("span", "Retry status is unavailable."); status.setAttribute("role", "status");
  const quota = create("span");
  const cancel = create("button", "Cancel retry"); cancel.type = "button"; cancel.hidden = true;
  root.classList.add("codex-retry-controls");
  root.replaceChildren(label, status, quota, cancel);
  let state = null, observedAt = 0, stopped = false, suspended = false, busy = false;
  let timer = null, poll = null, activeRead = null, generation = 0, unavailable = false;
  const hidden = () => stopped || suspended || page.hidden;
  const render = () => {
    clear(timer); timer = null;
    if (hidden()) return;
    enabled.checked = Boolean(state?.enabled);
    enabled.disabled = busy || !state || unavailable;
    cancel.disabled = busy || unavailable;
    cancel.hidden = !state || !["scheduled", "submitting", "reconciling", "running", "blocked"].includes(state.phase);
    status.textContent = unavailable ? "Retry status is unavailable." : retryStatusText(state, (state?.serverNowMs || 0) + Math.max(0, now() - observedAt));
    quota.textContent = state?.class === "cybersecurity" ? `${state.cyberUsed}/${state.cyberLimit} automatic retries in 5 minutes` : "";
    if (!unavailable && state?.phase === "scheduled") timer = later(render, 1000);
  };
  const apply = value => { state = value; observedAt = now(); unavailable = false; root.hidden = false; render(); };
  const refresh = async () => {
    if (hidden() || busy) return;
    clear(poll); poll = null;
    activeRead?.abort();
    const controller = new AbortController(), current = ++generation;
    activeRead = controller;
    try {
      const value = await options.client.retry({signal: controller.signal});
      if (current !== generation || hidden()) return;
      apply(value);
    } catch (error) {
      if (current !== generation || hidden() || controller.signal.aborted) return;
      unavailable = true;
      if ([403, 404].includes(error.status)) root.hidden = true;
      render();
    } finally {
      if (current === generation) {
        activeRead = null;
        if (!hidden()) poll = later(() => { void refresh(); }, 15_000);
      }
    }
  };
  const mutate = async action => {
    if (busy || stopped || unavailable) return;
    busy = true; generation++; activeRead?.abort(); activeRead = null;
    clear(poll); poll = null; render();
    try {
      const value = await action();
      if (!stopped) apply(value);
    } catch (_) {
      // Lost replies are observations to reconcile, never automatic repeated
      // writes or optimistic preference changes.
      unavailable = true;
    } finally {
      busy = false; render(); void refresh();
    }
  };
  enabled.addEventListener("change", () => { const requested = enabled.checked; enabled.checked = Boolean(state?.enabled); void mutate(() => options.client.setRetryEnabled(requested)); });
  cancel.addEventListener("click", () => { void mutate(() => options.client.cancelRetry()); });
  const suspend = () => {
    suspended = true; generation++; activeRead?.abort(); activeRead = null;
    clear(timer); clear(poll); timer = poll = null;
  };
  const resume = () => { suspended = false; render(); void refresh(); };
  const visibility = () => { if (page.hidden) suspend(); else resume(); };
  page.addEventListener?.("visibilitychange", visibility);
  browser.addEventListener?.("pagehide", suspend);
  browser.addEventListener?.("pageshow", resume);
  browser.addEventListener?.("focus", resume);
  void refresh();
  return {refresh, destroy() {
    stopped = true; suspend();
    page.removeEventListener?.("visibilitychange", visibility);
    browser.removeEventListener?.("pagehide", suspend);
    browser.removeEventListener?.("pageshow", resume);
    browser.removeEventListener?.("focus", resume);
    root.replaceChildren();
  }};
}

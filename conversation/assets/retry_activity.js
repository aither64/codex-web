const phases = new Set(["scheduled", "submitting", "reconciling", "running", "terminal"]);
const outcomes = {
  success: "Retry completed.", stopped: "Retrying stopped.",
  exhausted: "Automatic retry limit reached.", superseded: "New input ended this retry chain.",
  blocked: "Automatic retry cannot continue.",
};
const identity = value => typeof value === "string" && /^[A-Za-z0-9_-]{1,256}$/.test(value);
const metric = value => Number.isSafeInteger(value) && value >= 0;
const timestamp = value => metric(value) && value > 0;
const quality = value => ["exact", "partial", "unavailable"].includes(value);
const pageDismissals = new WeakMap();

function validStatus(state) {
  if (!state || !timestamp(state.serverNowMs)) return false;
  const a = state.activity;
  if (a === null) return true;
  return a && identity(a.chainKey) && ["capacity", "cybersecurity"].includes(a.class) && phases.has(a.phase) &&
    quality(a.countQuality) && quality(a.timingQuality) && [0, 1].includes(a.unconfirmed) &&
    (a.confirmedStarts === null ? a.countQuality === "unavailable" : metric(a.confirmedStarts) && a.countQuality !== "unavailable") &&
    (a.elapsedMs === null && a.startedAtMs === null ? a.timingQuality === "unavailable" :
      metric(a.elapsedMs) && timestamp(a.startedAtMs) && a.timingQuality !== "unavailable") &&
    (a.finishedAtMs === null || timestamp(a.finishedAtMs)) && (a.nextAttemptAtMs === null || timestamp(a.nextAttemptAtMs)) &&
    typeof a.reason === "string" && a.reason.isWellFormed() && new TextEncoder().encode(a.reason).length <= 1024 &&
    (a.retryNowToken === "" || identity(a.retryNowToken) && a.phase === "scheduled") &&
    (a.stopToken === "" || identity(a.stopToken)) &&
    (a.phase === "terminal" ? Object.hasOwn(outcomes, a.outcome) && !a.retryNowToken && !a.stopToken && a.nextAttemptAtMs === null :
      a.outcome === "" && a.finishedAtMs === null) && (a.nextAttemptAtMs === null || a.phase === "scheduled");
}

function duration(ms) {
  const seconds = Math.floor(ms / 1000), minutes = Math.floor(seconds / 60), hours = Math.floor(minutes / 60);
  if (hours) return `${hours}h ${minutes % 60}m`;
  if (minutes) return `${minutes}m ${seconds % 60}s`;
  return `${seconds}s`;
}

// Elapsed time advances only for a current exact active observation. Terminal
// and partial metrics retain their server values rather than inventing precision.
export function retryActivityPresentation(state, elapsedSinceObservation = 0) {
  if (!validStatus(state)) throw new TypeError("invalid retry activity status");
  const a = state.activity;
  if (!a) return null;
  const delta = Number.isFinite(elapsedSinceObservation) ? Math.max(0, elapsedSinceObservation) : 0;
  const terminal = a.phase === "terminal";
  const count = a.confirmedStarts === null ? "Retry count unavailable" :
    `${a.countQuality === "partial" ? "At least " : ""}${a.confirmedStarts} ${a.confirmedStarts === 1 ? "retry" : "retries"} started`;
  const elapsed = a.elapsedMs === null ? "Elapsed time unavailable" :
    `${a.timingQuality === "partial" ? "Recorded time" : "Total elapsed"} ${duration(a.elapsedMs + (!terminal && a.timingQuality === "exact" ? delta : 0))}${a.timingQuality === "partial" ? " (partial)" : ""}`;
  let status = terminal ? outcomes[a.outcome] : a.phase === "running" ? "Retry in progress." :
    ["submitting", "reconciling"].includes(a.phase) ? "Confirming whether the retry started." : "Waiting to retry.";
  if (a.phase === "scheduled" && a.nextAttemptAtMs !== null) {
    const seconds = Math.max(0, Math.ceil((a.nextAttemptAtMs - state.serverNowMs - delta) / 1000));
    status = `Retrying in ${seconds}s.`;
  }
  return {terminal, title: a.class === "capacity" ? "Model at capacity" : "Cybersecurity error", count, elapsed,
    unconfirmed: a.unconfirmed ? "1 retry awaiting confirmation" : "", status, reason: a.reason,
    announcement: terminal ? outcomes[a.outcome] : a.phase === "scheduled" ? "Automatic retry scheduled." : status};
}

// Owns only the supplied footer's children. The application keeps its scroll
// owner and message list; the browser never schedules or dispatches a retry.
export function mountRetryActivity(root, options) {
  if (typeof options?.conversationKey !== "string" || !options.conversationKey) throw new TypeError("conversationKey is required");
  const page = options.document || globalThis.document, browser = options.window || globalThis;
  const now = options.now || (() => globalThis.performance.now());
  const later = options.setTimeout || globalThis.setTimeout, clear = options.clearTimeout || globalThis.clearTimeout;
  let storage = options.storage;
  if (storage === undefined) { try { storage = browser.localStorage; } catch (_) { storage = null; } }
  const conversationKey = options.conversationKey;
  const dismissalKey = chainKey => `codex-web.retry-activity.dismissed:${JSON.stringify([conversationKey, chainKey])}`;
  const dismissed = pageDismissals.get(page) || new Set();
  pageDismissals.set(page, dismissed);
  const isDismissed = chainKey => {
    const key = dismissalKey(chainKey);
    if (dismissed.has(key)) return true;
    try { if (storage?.getItem(key) === "1") dismissed.add(key); } catch (_) {}
    return dismissed.has(key);
  };
  const create = (tag, className, text = "") => {
    const node = page.createElement(tag); node.className = className; node.textContent = text; return node;
  };
  const notice = create("section", "codex-retry-activity-notice"); notice.setAttribute("aria-label", "Automatic retry");
  const title = create("strong", "codex-retry-activity-title");
  const metrics = create("div", "codex-retry-activity-metrics");
  const count = create("span", "codex-retry-activity-count"), elapsed = create("span", "codex-retry-activity-elapsed");
  metrics.append(count, elapsed);
  const status = create("span", "codex-retry-activity-status"); status.setAttribute("aria-live", "off");
  const uncertain = create("span", "codex-retry-activity-unconfirmed");
  const reason = create("span", "codex-retry-activity-reason");
  const live = create("span", "codex-retry-activity-announcement"); live.setAttribute("role", "status"); live.setAttribute("aria-live", "polite");
  const actions = create("div", "codex-retry-activity-actions");
  const retryNow = create("button", "codex-retry-activity-now", "Retry now");
  const stop = create("button", "codex-retry-activity-stop", "Stop retrying");
  const dismiss = create("button", "codex-retry-activity-dismiss", "Dismiss");
  for (const button of [retryNow, stop, dismiss]) button.type = "button";
  actions.append(retryNow, stop, dismiss);
  notice.append(title, metrics, status, uncertain, reason, actions, live);
  root.classList.add("codex-retry-activity"); root.hidden = true; root.replaceChildren(notice);
  let state = null, observedAt = 0, stopped = false, suspended = false, busy = false, unavailable = false, reloadRequired = false;
  let tick = null, poll = null, activeRead = null, readPromise = null, generation = 0, announcementKey = "";
  const hidden = () => stopped || suspended || page.hidden;
  const render = () => {
    clear(tick); tick = null;
    if (hidden()) return;
    const a = state?.activity;
    const view = state ? retryActivityPresentation(state, unavailable ? 0 : Math.max(0, now() - observedAt)) : null;
    root.hidden = !view || view.terminal && isDismissed(a.chainKey);
    if (root.hidden) return;
    title.textContent = view.title; count.textContent = view.count; elapsed.textContent = view.elapsed;
    status.textContent = reloadRequired ? "Reload the page to use the current retry controls." : unavailable ? "Retry status is unavailable. Refresh to check again." : view.status;
    uncertain.textContent = view.unconfirmed; uncertain.hidden = !view.unconfirmed;
    reason.textContent = view.reason; reason.hidden = !view.reason;
    retryNow.hidden = view.terminal || a.phase !== "scheduled" || !a.retryNowToken;
    stop.hidden = view.terminal || !a.stopToken;
    dismiss.hidden = !view.terminal;
    retryNow.disabled = stop.disabled = busy || unavailable || reloadRequired;
    const key = JSON.stringify([a.chainKey, a.phase, a.outcome, unavailable, reloadRequired]);
    if (key !== announcementKey) { live.textContent = unavailable ? status.textContent : view.announcement; announcementKey = key; }
    if (!unavailable && !view.terminal) tick = later(render, 1000);
  };
  const apply = value => {
    if (!validStatus(value)) throw new TypeError("invalid retry activity status");
    state = value; observedAt = now(); unavailable = false; render();
  };
  const refresh = () => {
    if (hidden() || busy || reloadRequired) return Promise.resolve();
    if (readPromise) return readPromise;
    clear(poll); poll = null;
    const controller = new AbortController(), current = ++generation;
    activeRead = controller;
    const promise = Promise.resolve().then(() => options.client.retryActivity({signal: controller.signal})).then(value => {
      if (current === generation && !hidden()) apply(value);
    }).catch(error => {
      if (current !== generation || hidden() || controller.signal.aborted) return;
      unavailable = true;
      if ([403, 404].includes(error.status)) { state = null; root.hidden = true; }
      render();
    }).finally(() => {
      if (readPromise === promise) readPromise = null;
      if (current === generation) {
        activeRead = null;
        if (!hidden()) poll = later(() => { void refresh(); }, 15_000);
      }
    });
    readPromise = promise;
    return promise;
  };
  const mutate = async (method, token) => {
    if (hidden() || busy || unavailable || reloadRequired || !token) return;
    busy = true; generation++; activeRead?.abort(); activeRead = null; readPromise = null;
    clear(poll); poll = null; render();
    try {
      const value = await options.client[method](token);
      if (!stopped) apply(value);
    } catch (error) {
      if (!stopped) { unavailable = true; reloadRequired = error.code === "retry_reload_required"; }
    } finally {
      busy = false; render();
      // Unknown or stale writes only trigger a read of this conversation.
      if (!reloadRequired) void refresh();
    }
  };
  retryNow.addEventListener("click", () => { void mutate("retryNow", state?.activity?.retryNowToken); });
  stop.addEventListener("click", () => { void mutate("stopRetry", state?.activity?.stopToken); });
  dismiss.addEventListener("click", () => {
    if (state?.activity?.phase !== "terminal") return;
    const chainKey = state.activity.chainKey;
    dismissed.add(dismissalKey(chainKey));
    try { storage?.setItem(dismissalKey(chainKey), "1"); } catch (_) {}
    render();
  });
  const suspend = () => {
    suspended = true; generation++; activeRead?.abort(); activeRead = null; readPromise = null;
    clear(tick); clear(poll); tick = poll = null;
  };
  const resume = () => { if (stopped) return; suspended = false; render(); void refresh(); };
  const visibility = () => { if (page.hidden) suspend(); else resume(); };
  page.addEventListener?.("visibilitychange", visibility);
  browser.addEventListener?.("pagehide", suspend); browser.addEventListener?.("pageshow", resume); browser.addEventListener?.("focus", resume);
  void refresh();
  return {refresh, destroy() {
    stopped = true; suspend(); root.hidden = true; root.replaceChildren();
    page.removeEventListener?.("visibilitychange", visibility);
    browser.removeEventListener?.("pagehide", suspend); browser.removeEventListener?.("pageshow", resume); browser.removeEventListener?.("focus", resume);
  }};
}

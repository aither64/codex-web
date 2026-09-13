// Stream notifications are hints. A bounded snapshot read establishes freshness.
// All timers and browser event sources can be supplied by an embedding/test host.
export function createConversationSync(options) {
  if (typeof options?.read !== "function" || typeof options?.apply !== "function") {
    throw new TypeError("conversation read and apply callbacks are required");
  }
  const browser = options.window || globalThis;
  const page = options.document || globalThis.document;
  const EventSourceClass = options.EventSource === undefined ? globalThis.EventSource : options.EventSource;
  const now = options.now || Date.now;
  const later = options.setTimeout || globalThis.setTimeout;
  const clear = options.clearTimeout || globalThis.clearTimeout;
  const random = options.random || Math.random;
  const live = options.live !== false;
  const streaming = live && options.eventStream !== false && Boolean(EventSourceClass && options.eventsPath);
  const hidden = () => Boolean(page?.hidden);
  let destroyed = false, suspended = false, dirty = false;
  let source = null, active = null, refreshTimer = null, retryTimer = null, watchTimer = null;
  let retryAttempt = 0, lastWatch = now(), lastRecovery = -Infinity;
  let lastSuccessAt = null, lastTraffic = now(), heartbeatDeadline = null;
  let streamOpen = !streaming, streamFailed = false, needsSync = true, readError = null;
  let lastState = "";
  let warningTimer = null, recoverySince = null;

  const publish = () => {
    const status = readError || streamFailed ?
      (browser.navigator?.onLine === false ? "offline" : "reconnecting") :
      needsSync ? (lastSuccessAt === null ? "connecting" : "syncing") :
      !streamOpen ? "reconnecting" : "connected";
    const accessDenied = readError?.status === 401 || readError?.status === 403;
    if (status === "connected" || hidden() || suspended) {
      recoverySince = null;
      clear(warningTimer); warningTimer = null;
    } else if (recoverySince === null) {
      recoverySince = now();
    }
    const showWarning = accessDenied || (recoverySince !== null && now() - recoverySince >= 10_000);
    if (!showWarning && recoverySince !== null && warningTimer === null) {
      warningTimer = later(() => { warningTimer = null; publish(); }, Math.max(0, 10_000 - (now() - recoverySince)));
    }
    const state = {status, showWarning, lastSuccessAt, error: readError?.message || "", httpStatus: readError?.status || null};
    const signature = JSON.stringify(state);
    if (!destroyed && signature !== lastState) {
      lastState = signature;
      options.onStateChange?.(state);
    }
  };
  const closeStream = () => {
    const old = source;
    source = null;
    old?.close();
    streamOpen = !streaming;
    heartbeatDeadline = null;
  };
  const cancelRead = () => {
    const old = active;
    active = null;
    old?.abort.abort();
  };
  const clearRefresh = () => { clear(refreshTimer); refreshTimer = null; };
  const scheduleRetry = () => {
    if (destroyed || suspended || !live || retryTimer !== null) return;
    const delay = Math.min(30_000, 1000 * 2 ** Math.min(retryAttempt++, 5));
    retryTimer = later(() => {
      retryTimer = null;
      if (hidden()) return;
      connect();
      scheduleRefresh(0);
    }, Math.round(delay * (0.8 + random() * 0.2)));
  };

  function scheduleRefresh(delay = 200) {
    if (destroyed || suspended) return;
    dirty = true;
    if (readError && retryTimer !== null) return;
    if (hidden() || active) return;
    if (refreshTimer !== null) {
      if (delay !== 0) return;
      clearRefresh();
    }
    refreshTimer = later(() => { refreshTimer = null; void refresh(); }, delay);
  }

  function refresh() {
    if (destroyed || suspended || hidden()) return Promise.resolve(false);
    if (active) { dirty = true; return active.promise; }
    clearRefresh();
    dirty = false;
    const cycle = {abort: new AbortController(), promise: null};
    active = cycle;
    const signal = cycle.abort.signal;
    const isCurrent = () => !destroyed && active === cycle && !signal.aborted;
    let rejectAbort;
    const aborted = new Promise((_, reject) => { rejectAbort = reject; });
    const onAbort = () => rejectAbort(signal.reason);
    signal.addEventListener("abort", onAbort, {once: true});
    const deadline = later(() => {
      cycle.abort.abort(new Error("Conversation refresh timed out"));
    }, 35_000);
    cycle.promise = (async () => {
      try {
        const value = await Promise.race([Promise.resolve().then(() => options.read(signal)), aborted]);
        if (!isCurrent()) return false;
        await Promise.race([Promise.resolve().then(() => options.apply(value, {signal, isCurrent})), aborted]);
        if (!isCurrent()) return false;
        lastSuccessAt = now();
        readError = null;
        needsSync = false;
        if (streamOpen) {
          streamFailed = false;
          retryAttempt = 0;
          clear(retryTimer); retryTimer = null;
        } else scheduleRetry();
        publish();
        return true;
      } catch (error) {
        if (!destroyed && active === cycle) {
          readError = error instanceof Error ? error : new Error("Conversation refresh failed");
          needsSync = true;
          publish();
          scheduleRetry();
        }
        return false;
      } finally {
        clear(deadline);
        signal.removeEventListener("abort", onAbort);
        // Retire every sibling read, including a reconciliation POST, on failure.
        cycle.abort.abort();
        if (active === cycle) {
          active = null;
          // Retry failures with backoff even if a stream produced more hints.
          if (dirty && !readError) scheduleRefresh();
        }
      }
    })();
    return cycle.promise;
  }

  function connect() {
    if (!streaming || source || destroyed || suspended || hidden()) return;
    let events;
    try { events = new EventSourceClass(options.eventsPath); }
    catch (_) { streamFailed = true; publish(); scheduleRetry(); return; }
    source = events;
    lastTraffic = now();
    const current = () => !destroyed && source === events;
    events.onopen = () => {
      if (!current()) return;
      streamOpen = true;
      lastTraffic = now();
      needsSync = true;
      clear(retryTimer); retryTimer = null;
      // A read started before reconnection cannot establish freshness for it.
      cancelRead();
      publish();
      scheduleRefresh(0);
    };
    events.addEventListener("ready", event => {
      if (!current()) return;
      try {
        const interval = JSON.parse(event.data).heartbeatIntervalMs;
        if (Number.isSafeInteger(interval) && interval > 0) heartbeatDeadline = interval * 2 + 5000;
      } catch (_) {}
      lastTraffic = now();
    });
    events.addEventListener("heartbeat", () => { if (current()) lastTraffic = now(); });
    events.onmessage = () => {
      if (!current()) return;
      lastTraffic = now();
      scheduleRefresh();
    };
    events.onerror = () => {
      if (!current()) return;
      closeStream();
      streamFailed = true;
      needsSync = true;
      publish();
      // Own retries even when the native EventSource has permanently closed.
      scheduleRetry();
      scheduleRefresh(0);
    };
  }

  const recover = (force = true) => {
    if (destroyed || suspended || hidden() || now() - lastRecovery < 250) return;
    lastRecovery = now(); lastWatch = now();
    clear(retryTimer); retryTimer = null;
    const stale = now() - lastTraffic > (heartbeatDeadline ?? 45_000);
    if (force || !streamOpen || stale) {
      cancelRead();
      closeStream();
    }
    needsSync = true;
    publish();
    connect();
    scheduleRefresh(0);
  };
  const watch = () => {
    watchTimer = null;
    if (destroyed || suspended) return;
    const at = now();
    if (!hidden()) {
      if (at - lastWatch > 45_000) recover();
      else if (streaming && source && at - lastTraffic > (heartbeatDeadline ?? 45_000) && (heartbeatDeadline !== null || !streamOpen)) {
        closeStream(); streamFailed = true; needsSync = true;
        publish(); scheduleRetry(); scheduleRefresh(0);
      }
      if (lastSuccessAt === null || at - lastSuccessAt >= 60_000) {
        if (!active && retryTimer === null) scheduleRefresh(0);
      }
    }
    lastWatch = at;
    watchTimer = later(watch, 5000);
  };
  const visibility = () => { if (!hidden()) recover(false); else publish(); };
  const focus = () => recover(false);
  const online = () => recover(true);
  const offline = () => {
    if (destroyed || suspended) return;
    cancelRead(); closeStream(); streamFailed = true; needsSync = true;
    publish(); scheduleRetry();
  };
  const pagehide = () => {
    suspended = true;
    clear(warningTimer); warningTimer = null; recoverySince = null;
    cancelRead(); closeStream(); clearRefresh();
    clear(retryTimer); retryTimer = null;
    clear(watchTimer); watchTimer = null;
  };
  const pageshow = () => {
    if (destroyed) return;
    suspended = false;
    lastRecovery = -Infinity;
    recover();
    if (live && watchTimer === null) watchTimer = later(watch, 5000);
  };
  const listeners = [[page, "visibilitychange", visibility], [browser, "focus", focus],
    [browser, "online", online], [browser, "offline", offline],
    [browser, "pagehide", pagehide], [browser, "pageshow", pageshow]];
  for (const [target, name, callback] of listeners) target?.addEventListener?.(name, callback);
  publish(); connect(); scheduleRefresh(0);
  if (live) watchTimer = later(watch, 5000);
  return {
    refresh, scheduleRefresh, retry: recover,
    destroy() {
      destroyed = true;
      pagehide();
      for (const [target, name, callback] of listeners) target?.removeEventListener?.(name, callback);
    },
  };
}

export function connectionMessage(state) {
  if (state.showWarning === false || state.status === "connected") return "";
  if (state.status === "connecting") return "Connecting to conversation…";
  if (state.status === "syncing") return "Refreshing conversation…";
  if (state.httpStatus === 401 || state.httpStatus === 403) return "Conversation access denied. Check your access, then retry.";
  if (state.status === "offline") return "Network connection unavailable. Messages may be out of date. Retrying automatically.";
  if (state.error) return `${state.error}. Messages may be out of date. Retrying automatically.`;
  return "Live updates interrupted. Messages may be out of date. Retrying automatically.";
}

export function renderConnectionStatus(element, state, retry) {
  const message = connectionMessage(state);
  element.hidden = !message;
  element.setAttribute("role", "status");
  element.setAttribute("aria-live", "polite");
  element.replaceChildren();
  if (!message) return;
  const document = element.ownerDocument || globalThis.document;
  const text = document.createElement("span"); text.textContent = message;
  element.append(text);
  if (state.lastSuccessAt !== null) {
    const time = document.createElement("span");
    time.textContent = ` Last refreshed at ${new Date(state.lastSuccessAt).toLocaleTimeString()}.`;
    element.append(time);
  }
  const button = document.createElement("button");
  button.type = "button"; button.textContent = "Retry now";
  button.addEventListener("click", retry);
  element.append(button);
}

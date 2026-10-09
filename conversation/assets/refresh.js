// Resource-specific freshness budgets share presentation and retry defaults.
// Hosts may override individual values without replacing their domain controllers.
export const refreshPolicy = Object.freeze({
  initialLoadingMs: 750, manualLoadingMs: 250, warningMs: 30_000,
  retryInitialMs: 2000, retryMaxMs: 30_000, eventDebounceMs: 200,
  resumeDebounceMs: 250, watchMs: 5000, heartbeatGraceMs: 5000,
  stalledMs: 45_000, transcriptMs: 60_000, transcriptDeadlineMs: 35_000,
  historyRepairMs: 30_000, historyPageMs: 40,
  limitsMs: 60_000, limitsDeadlineMs: 10_000,
  pendingMs: 15_000, pendingDeadlineMs: 10_000,
  queueMs: 30_000, queueDeadlineMs: 12_000,
  activityActiveMs: 5000, activityIdleMs: 30_000,
  activityActiveFreshMs: 15_000, activityIdleFreshMs: 35_000,
  detailsMs: 15_000, detailsDeadlineMs: 15_000,
  indexMs: 15_000, indexRetryMs: 30_000, indexProgressMs: 1000,
  archivalMs: 30_000, archivalDeadlineMs: 60_000, readDeadlineMs: 10_000,
});

export function createRefreshNotice(options = {}) {
  const policy = {...refreshPolicy, ...options.policy};
  const page = options.document || globalThis.document;
  const browser = options.window || globalThis;
  const now = options.now || Date.now;
  const later = options.setTimeout || globalThis.setTimeout;
  const clear = options.clearTimeout || globalThis.clearTimeout;
  let timer = null, stopped = false, suspended = false, pending = false, manual = false;
  let started = null, failed = null, error = null, lastSuccessAt = null, urgent = false;
  const publish = () => {
    clear(timer); timer = null;
    if (stopped) return;
    const hidden = suspended || Boolean(page?.hidden);
    const accessDenied = error?.status === 401 || error?.status === 403;
    const warning = !hidden && Boolean(error) && (urgent || accessDenied ||
      failed !== null && now() - failed >= policy.warningMs);
    const loadingDelay = manual ? policy.manualLoadingMs : policy.initialLoadingMs;
    const loading = !hidden && pending && !error && (manual || lastSuccessAt === null) &&
      started !== null && now() - started >= loadingDelay;
    options.render?.({loading, warning, error, lastSuccessAt, hasValue: lastSuccessAt !== null});
    if (hidden) return;
    const remaining = [];
    if (error && !warning && failed !== null) remaining.push(policy.warningMs - (now() - failed));
    if (pending && !error && !loading && (manual || lastSuccessAt === null)) remaining.push(loadingDelay - (now() - started));
    if (remaining.length) timer = later(publish, Math.max(0, Math.min(...remaining)));
  };
  const visibility = () => {
    // Background time never counts toward a visible warning.
    if (error) failed = page?.hidden ? null : now();
    if (pending) started = now();
    publish();
  };
  page?.addEventListener?.("visibilitychange", visibility);
  const hide = () => { suspended = true; failed = null; publish(); };
  const show = () => { suspended = false; visibility(); };
  browser?.addEventListener?.("pagehide", hide);
  browser?.addEventListener?.("pageshow", show);
  return {
    begin({manual: requested = false} = {}) {
      pending = true; manual = requested; started = now(); publish();
    },
    success() { pending = false; error = null; failed = null; lastSuccessAt = now(); publish(); },
    failure(value, {immediate = manual} = {}) {
      urgent = immediate;
      pending = false; error = value instanceof Error ? value : new Error(String(value || "Update failed"));
      if (failed === null && !page?.hidden) failed = now();
      publish();
    },
    cancel() { pending = false; publish(); },
    destroy() {
      stopped = true; clear(timer);
      page?.removeEventListener?.("visibilitychange", visibility);
      browser?.removeEventListener?.("pagehide", hide);
      browser?.removeEventListener?.("pageshow", show);
    },
  };
}

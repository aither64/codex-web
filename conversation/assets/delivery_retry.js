// Retry only explicit, transient non-delivery. Unknown outcomes retain their
// original receipt and are resolved by history observation or a status check.
export function createDeliveryRetryScheduler(options) {
  const entries = new Map();
  const setTimer = options.setTimer || globalThis.setTimeout.bind(globalThis);
  const clearTimer = options.clearTimer || globalThis.clearTimeout.bind(globalThis);
  const current = options.isCurrent || (() => true);
  const available = options.canRetry || (() => true);
  const stop = id => {
    const entry = entries.get(id);
    if (entry?.timer !== null && entry?.timer !== undefined) clearTimer(entry.timer);
    entries.delete(id);
  };
  const arm = entry => {
    if (entry.timer !== null || entries.get(entry.id) !== entry) return;
    entry.timer = setTimer(async () => {
      entry.timer = null;
      if (entries.get(entry.id) !== entry) return;
      if (!current(entry.id)) { stop(entry.id); return; }
      if (!available(entry.id)) { arm(entry); return; }
      try { await options.retry(entry.id); }
      catch (error) { schedule(entry.id, error); }
    }, entry.delay);
    entry.delay = Math.min(5000, entry.delay * 2);
  };
  const schedule = (id, error) => {
    if (!id || error?.code !== "message_not_sent" || error?.notSent !== true || error?.retryable !== true) {
      stop(id); return false;
    }
    let entry = entries.get(id);
    if (!entry) { entry = {id, delay: 250, timer: null}; entries.set(id, entry); }
    arm(entry);
    return true;
  };
  return {schedule, stop, destroy() { for (const id of entries.keys()) stop(id); }};
}

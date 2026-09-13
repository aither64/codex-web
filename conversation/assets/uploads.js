const idPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export function attachmentIDs(value = []) {
  if (!Array.isArray(value) || value.length > 100 ||
      value.some((id) => typeof id !== "string" || !idPattern.test(id)) || new Set(value).size !== value.length) {
    throw new TypeError("Invalid attachment IDs");
  }
  return [...value];
}

export function sameAttachments(left, right) {
  return JSON.stringify(attachmentIDs(left)) === JSON.stringify(attachmentIDs(right));
}

export function fileSize(bytes) {
  const units = ["B", "KiB", "MiB", "GiB"];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) { value /= 1024; unit += 1; }
  return `${unit === 0 ? value : Number(value.toFixed(1))} ${units[unit]}`;
}

function localURL(value) {
  const parsed = new URL(value, globalThis.location?.href || "https://localhost/");
  const origin = globalThis.location?.origin || "https://localhost";
  if (parsed.origin !== origin || !value.startsWith("/") || value.startsWith("//") || parsed.username || parsed.password) {
    throw new TypeError("File endpoint must be on the current origin");
  }
  return parsed.pathname + parsed.search;
}

async function payload(response) {
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(data.error || `File operation failed (${response.status})`);
    error.status = response.status;
    throw error;
  }
  return data;
}

export function createUploadClient(basePath, options = {}) {
  const base = localURL(basePath);
  if (base.includes("?") || base.endsWith("/")) throw new TypeError("Invalid upload endpoint");
  const fetchRequest = options.fetch || globalThis.fetch.bind(globalThis);
  const request = (suffix, init = {}) => fetchRequest(base + suffix, {
    credentials: "same-origin", signal: options.signal, ...init,
  }).then(payload);
  const path = (id) => { if (!idPattern.test(id)) throw new TypeError("Invalid upload ID"); return `/${id}`; };
  return {
    list: () => request(""),
    create: (file, clientId) => request("", {method: "POST", headers: {"Content-Type": "application/json"},
      body: JSON.stringify({name: file.name, size: file.size, clientId})}),
    status: (id) => request(path(id)),
    complete: (id) => request(path(id), {method: "POST"}),
    remove: (id, confirmed = false) => request(path(id) + (confirmed ? "?confirmed=true" : ""), {method: "DELETE"}),
    append: (id, offset, checksum, chunk, onProgress, signal) => {
      const XHR = options.XMLHttpRequest || globalThis.XMLHttpRequest;
      return new Promise((resolve, reject) => {
        const xhr = new XHR();
        const abort = () => xhr.abort();
        const cleanup = () => signal?.removeEventListener("abort", abort);
        xhr.open("PATCH", base + path(id));
        xhr.withCredentials = true;
        xhr.timeout = 120000;
        xhr.setRequestHeader("Content-Type", "application/octet-stream");
        xhr.setRequestHeader("Upload-Offset", String(offset));
        xhr.setRequestHeader("Upload-Checksum", checksum);
        xhr.upload.onprogress = (event) => onProgress?.(event.loaded);
        xhr.onload = () => {
          cleanup();
          let data = {};
          try { data = JSON.parse(xhr.responseText); } catch (_error) {}
          if (xhr.status >= 200 && xhr.status < 300) resolve(data);
          else { const error = new Error(data.error || `Upload failed (${xhr.status})`); error.status = xhr.status; reject(error); }
        };
        xhr.onerror = xhr.ontimeout = () => { cleanup(); reject(new Error("Upload connection interrupted")); };
        xhr.onabort = () => { cleanup(); reject(new DOMException("Upload cancelled", "AbortError")); };
        signal?.addEventListener("abort", abort, {once: true});
        if (signal?.aborted) { cleanup(); reject(new DOMException("Upload cancelled", "AbortError")); return; }
        xhr.send(chunk);
      });
    },
  };
}

async function checksum(blob) {
  const digest = await crypto.subtle.digest("SHA-256", await blob.arrayBuffer());
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

// Transfer reads at most one chunk into memory. After a reload the user must
// reselect the file; every already-acknowledged chunk is verified before append.
export async function transferUpload(client, record, file, limits, onProgress, signal) {
  let current = await client.status(record.id);
  if (current.name !== file.name || current.size !== file.size) throw new Error("Choose the same file to resume this upload");
  if (current.state === "ready") return current;
  if (current.state !== "uploading") throw new Error("This file is no longer available");
  let verified = 0;
  for (const expected of current.checksums || []) {
    if (signal?.aborted) throw new DOMException("Upload cancelled", "AbortError");
    const end = Math.min(verified + limits.chunkBytes, file.size);
    if (await checksum(file.slice(verified, end)) !== expected) throw new Error("The selected file differs from the uploaded data");
    verified = end;
  }
  if (verified !== current.offset) throw new Error("Stored upload progress is inconsistent");
  while (current.offset < file.size) {
    if (signal?.aborted) throw new DOMException("Upload cancelled", "AbortError");
    const offset = current.offset;
    const chunk = file.slice(offset, Math.min(offset + limits.chunkBytes, file.size));
    const hash = await checksum(chunk);
    let failure;
    for (let retry = 0; retry < 3; retry += 1) {
      try {
        current = await client.append(current.id, offset, hash, chunk,
          (bytes) => onProgress?.(offset + bytes, "uploading"), signal);
        failure = null;
        break;
      } catch (error) {
        if (error.name === "AbortError" || (error.status >= 400 && error.status < 500 && error.status !== 409)) throw error;
        failure = error;
        current = await client.status(current.id);
        if (current.offset === offset + chunk.size && current.checksums?.at(-1) === hash) { failure = null; break; }
        if (current.offset !== offset) throw new Error("Upload progress changed; reselect the file to resume");
        await new Promise((resolve) => setTimeout(resolve, 250 * (retry + 1)));
      }
    }
    if (failure) throw failure;
    onProgress?.(current.offset, "uploading");
  }
  onProgress?.(file.size, "finishing");
  if (signal?.aborted) throw new DOMException("Upload cancelled", "AbortError");
  return client.complete(current.id);
}

export function renderAttachments(files, options = {}) {
  const list = document.createElement("div");
  list.className = "codex-attachments";
  for (const file of files || []) {
    const item = document.createElement("div");
    item.className = "codex-attachment";
    const label = document.createElement(file.downloadUrl && file.state === "ready" ? "a" : "span");
    label.textContent = file.name;
    if (label.tagName === "A") { label.href = localURL(file.downloadUrl); label.setAttribute("download", ""); }
    const size = document.createElement("span");
    size.className = "codex-attachment-detail";
    size.textContent = `${fileSize(file.size)}${file.state === "deleted" ? " · File removed" : ""}`;
    item.append(label, size);
    if (file.deleteUrl && file.state === "ready" && options.onRemove) {
      const remove = document.createElement("button");
      remove.type = "button"; remove.className = "quiet"; remove.textContent = "Delete file";
      remove.addEventListener("click", async () => {
        const refs = file.references?.length ? ` Referenced by: ${file.references.join(", ")}.` : "";
        if (!globalThis.confirm(`Delete ${file.name}? Codex will no longer be able to read this file. This does not erase content it has already read.${refs}`)) return;
        remove.disabled = true;
        try { await options.onRemove(file); } catch (error) { size.textContent = error.message; remove.disabled = false; }
      });
      item.append(remove);
    }
    list.append(item);
  }
  return list;
}

// mountUploads owns draft selection only. Successfully submitted IDs are cleared
// from the composer without deleting their server-side files. An optional
// controlsRoot places the menu beside host actions, leaving root for draft cards.
export function mountUploads(root, options) {
  const storage = options.storage || globalThis.localStorage;
  const key = options.storageKey;
  const client = options.client || createUploadClient(options.basePath);
  let entries = [];
  let limits;
  let locked = false;
  let stopped = false;
  let active = 0;
  let resumeEntry = null;
  const controllers = new Map();
  const controlsRoot = options.controlsRoot || root;
  const initiallyHidden = root.hidden;
  const controls = document.createElement("span");
  controls.className = "codex-upload-controls";
  const picker = document.createElement("input");
  picker.type = "file"; picker.multiple = true; picker.hidden = true;
  const button = document.createElement("button");
  button.type = "button"; button.className = "quiet codex-upload-toggle";
  button.disabled = true;
  button.setAttribute("aria-label", "Add attachments");
  button.setAttribute("aria-haspopup", "menu");
  button.setAttribute("aria-expanded", "false");
  const icon = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  icon.setAttribute("viewBox", "0 0 24 24"); icon.setAttribute("aria-hidden", "true");
  const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
  path.setAttribute("d", "M12 5v14M5 12h14"); icon.append(path); button.append(icon);
  const menu = document.createElement("div");
  menu.className = "codex-upload-menu"; menu.popover = "auto";
  menu.id = `codex-upload-menu-${crypto.randomUUID()}`;
  menu.setAttribute("role", "menu"); menu.setAttribute("aria-label", "Attachments");
  button.setAttribute("aria-controls", menu.id);
  button.popoverTargetElement = menu;
  const attach = document.createElement("button");
  attach.type = "button"; attach.textContent = "Attach files"; attach.setAttribute("role", "menuitem");
  menu.append(attach);
  const isOpen = () => menu.matches(":popover-open");
  const closeMenu = (restoreFocus = false) => {
    if (!isOpen()) return;
    menu.hidePopover(); button.setAttribute("aria-expanded", "false");
    if (restoreFocus) button.focus();
  };
  const positionMenu = () => {
    if (!isOpen()) return;
    const anchor = button.getBoundingClientRect();
    const size = menu.getBoundingClientRect();
    const gap = 6, margin = 8;
    const above = anchor.top - size.height - gap;
    const top = above >= margin ? above : anchor.bottom + gap;
    menu.style.left = `${Math.max(margin, Math.min(anchor.left, innerWidth - size.width - margin))}px`;
    menu.style.top = `${Math.max(margin, Math.min(top, innerHeight - size.height - margin))}px`;
  };
  const openMenu = () => {
    if (button.disabled || stopped || isOpen()) return;
    menu.showPopover(); button.setAttribute("aria-expanded", "true"); positionMenu(); attach.focus();
  };
  button.addEventListener("click", (event) => { event.preventDefault(); if (isOpen()) closeMenu(true); else openMenu(); });
  button.addEventListener("keydown", (event) => {
    if (event.key === "ArrowDown" || event.key === "ArrowUp") { event.preventDefault(); openMenu(); }
  });
  menu.addEventListener("keydown", (event) => {
    if (event.key === "Escape") { event.preventDefault(); event.stopPropagation(); closeMenu(true); }
    else if (event.key === "Tab") closeMenu(true);
    else if (["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) { event.preventDefault(); attach.focus(); }
  });
  menu.addEventListener("toggle", () => button.setAttribute("aria-expanded", String(isOpen())));
  globalThis.addEventListener("resize", positionMenu);
  globalThis.addEventListener("scroll", positionMenu, true);
  const notice = document.createElement("p");
  notice.className = "codex-upload-notice"; notice.setAttribute("role", "status");
  const list = document.createElement("div"); list.className = "codex-attachments";
  root.classList.add("codex-upload-composer");
  controls.append(button, picker, menu); controlsRoot.append(controls);
  notice.hidden = true; list.hidden = true;
  if (controlsRoot !== root) root.hidden = true;
  root.append(notice, list);
  const ready = () => Boolean(limits) && entries.every((entry) => entry.state === "ready" && !entry.removing);
  const save = () => {
    const value = JSON.stringify(entries.map(({file, error, progress, running, removing, task, ...entry}) => entry));
    storage.setItem(key, value);
    if (storage.getItem(key) !== value) throw new Error("Browser storage could not retain the attachment draft");
  };
  const render = () => {
    if (stopped) return;
    button.disabled = locked || !limits; attach.disabled = button.disabled;
    if (button.disabled) closeMenu();
    notice.hidden = !notice.textContent; list.hidden = entries.length === 0;
    if (controlsRoot !== root) root.hidden = notice.hidden && list.hidden;
    list.replaceChildren();
    for (const entry of entries) {
      const card = document.createElement("div"); card.className = "codex-attachment";
      const label = document.createElement("span"); label.textContent = entry.name;
      const detail = document.createElement("span"); detail.className = "codex-attachment-detail";
      detail.textContent = entry.error || `${fileSize(entry.size)} · ${entry.state === "ready" ? "Ready" : entry.running ? `${Math.floor(100 * (entry.progress || 0) / Math.max(1, entry.size))}%` : "Paused"}`;
      card.append(label, detail);
      if (entry.state !== "ready") {
        const progress = document.createElement("progress"); progress.max = Math.max(1, entry.size); progress.value = entry.progress || 0;
        progress.setAttribute("aria-label", `Uploading ${entry.name}`); card.append(progress);
        if (!entry.running) {
          const retry = document.createElement("button"); retry.type = "button"; retry.className = "quiet"; retry.textContent = entry.file ? "Retry" : "Choose file to resume";
          retry.disabled = locked;
          retry.addEventListener("click", () => { if (entry.file) { entry.error = null; pump(); } else { resumeEntry = entry; picker.multiple = false; picker.click(); } });
          card.append(retry);
        }
      }
      const remove = document.createElement("button"); remove.type = "button"; remove.className = "quiet"; remove.textContent = "Remove"; remove.disabled = locked || entry.removing;
      remove.setAttribute("aria-label", `Remove ${entry.name}`);
      remove.addEventListener("click", async () => {
        entry.removing = true;
        controllers.get(entry.clientId)?.abort();
        render();
        try {
          if (entry.task) await entry.task;
          // Repeating creation resolves a lost acknowledgement before deletion.
          if (!entry.id) Object.assign(entry, await client.create(entry, entry.clientId));
          await client.remove(entry.id);
          entries = entries.filter((candidate) => candidate !== entry); save(); render();
        } catch (error) { entry.removing = false; entry.error = error.message; render(); }
      });
      card.append(remove); list.append(card);
    }
    options.onChange?.({ready: ready() && !locked, count: entries.length});
  };
  const pump = () => {
    if (stopped || locked || !limits) return;
    for (const entry of entries) {
      if (active >= 2) break;
      if (entry.removing || entry.running || entry.error || !entry.file || entry.state === "ready") continue;
      active += 1; entry.running = true;
      const controller = new AbortController(); controllers.set(entry.clientId, controller);
      entry.task = (async () => {
        try {
          if (!entry.id) { Object.assign(entry, await client.create(entry.file, entry.clientId)); save(); }
          if (controller.signal.aborted) throw new DOMException("Upload cancelled", "AbortError");
          Object.assign(entry, await transferUpload(client, entry, entry.file, limits,
            (bytes) => { entry.progress = bytes; render(); }, controller.signal));
          save();
        } catch (error) { if (error.name !== "AbortError") entry.error = error.message; }
        finally { entry.running = false; active -= 1; controllers.delete(entry.clientId); render(); pump(); }
      })();
    }
    render();
  };
  const add = (files) => {
    if (locked || !limits) return;
    notice.textContent = "";
    let total = entries.reduce((sum, entry) => sum + entry.size, 0);
    for (const file of files) {
      if (file.size > limits.fileBytes || entries.length >= limits.files || total + file.size > limits.promptBytes) {
        notice.textContent = `${file.name} exceeds the file or prompt upload limit.`; continue;
      }
      entries.push({clientId: crypto.randomUUID(), name: file.name, size: file.size, state: "uploading", file});
      total += file.size;
    }
    try { save(); pump(); } catch (error) { notice.textContent = error.message; locked = true; render(); }
  };
  attach.addEventListener("click", () => { closeMenu(true); resumeEntry = null; picker.multiple = true; picker.click(); });
  picker.addEventListener("change", () => {
    if (resumeEntry && picker.files[0]) { resumeEntry.file = picker.files[0]; resumeEntry.error = null; resumeEntry = null; pump(); }
    else add(picker.files);
    picker.value = "";
  });
  const dropTarget = options.dropTarget || root;
  const drag = (event) => { if (event.dataTransfer?.types?.includes("Files")) { event.preventDefault(); dropTarget.classList.add("codex-upload-drag"); } };
  const leave = () => dropTarget.classList.remove("codex-upload-drag");
  const drop = (event) => { if (!event.dataTransfer?.types?.includes("Files")) return; event.preventDefault(); leave(); add(event.dataTransfer.files); };
  dropTarget.addEventListener("dragover", drag); dropTarget.addEventListener("dragleave", leave); dropTarget.addEventListener("drop", drop);
  const initialized = (async () => {
    try {
      const saved = JSON.parse(storage.getItem(key) || "[]");
      if (!Array.isArray(saved) || saved.length > 100 || saved.some((entry) => !idPattern.test(entry.clientId))) throw new Error("Stored attachment draft is invalid");
      entries = saved;
      const snapshot = await client.list(); limits = snapshot.limits;
      const available = new Map(snapshot.files.map((file) => [file.id, file]));
      for (const entry of entries) {
        if (entry.id && available.has(entry.id)) Object.assign(entry, available.get(entry.id));
        if (entry.id && (!available.has(entry.id) || entry.state === "deleted")) { entry.state = "missing"; entry.error = "File expired or was removed. Remove it and upload again."; }
      }
      save();
    } catch (error) { notice.textContent = error.message; locked = true; }
    render();
  })();
  return {
    initialized,
    ready,
    count: () => entries.length,
    ids: () => { if (!ready()) throw new Error("Wait for uploads to finish, or remove the unfinished files"); return attachmentIDs(entries.map((entry) => entry.id)); },
    clear: () => { entries = []; save(); render(); },
    lock: (value) => { locked = value; render(); if (!value) pump(); },
    destroy: () => {
      closeMenu(); stopped = true; controllers.forEach((controller) => controller.abort());
      globalThis.removeEventListener("resize", positionMenu); globalThis.removeEventListener("scroll", positionMenu, true);
      dropTarget.removeEventListener("dragover", drag); dropTarget.removeEventListener("dragleave", leave); dropTarget.removeEventListener("drop", drop);
      controls.remove(); root.replaceChildren(); root.hidden = initiallyHidden;
    },
  };
}

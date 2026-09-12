# codex-web

`codex-web` is a Go and browser integration for Codex App Server. It provides a
protocol client, capability-checked HTTP handlers and framework-free browser
assets for applications that already decide which Codex threads a user may
access.

The repository was extracted from a larger development-workspace application.
Workspace lifecycle and development-session policy remain in `dev-workspace`.

## Go packages

`github.com/aither64/codex-web/codex` connects to one Codex App Server over a
Unix socket. The client covers thread history, messages, queued work, settings,
questions, approvals, interruption and event subscriptions. Its durable
submission ledger prevents an uncertain browser retry from sending the same
message twice. Use `codex.NewWithOptions` when the application needs custom
App Server client identity, developer instructions or durable storage.
`ClientOptions.SubmissionLedgerPath` lets the application place the ledger in
writable persistent state independently of a connect-only or ephemeral socket
directory. Leave it empty to retain the compatibility path beside the socket;
all cooperating processes must select the same path.
Nonblocking user-input requests stay pending by default. Set
`ClientOptions.NonBlockingUserInput` only when the embedding application owns
an explicit automatic-response policy.

`Client.ReadAccountRateLimits(ctx)` reads current usage with
`account/rateLimits/read`. Its typed result contains the legacy `RateLimits`
snapshot and named `RateLimitsByLimitID` buckets, with nullable windows,
durations and reset times. Match `WindowDurationMins` to identify a window;
primary and secondary positions can represent different durations. `ResetsAt`
is Unix time in seconds. The result omits account identity, plan and credit
details. Applications authorize and expose this account-level read separately
from the conversation handler.

`github.com/aither64/codex-web/conversation` exposes those operations through
an `http.Handler`. The application supplies a resolver that maps its opaque
conversation ID to a trusted client, thread ID, canonical working directory and
explicit capabilities on every request. Transcript, pending-request and queue
reads have separate capabilities, so a passive view does not expose controls or
unresolved prompts. Browser payloads cannot select a socket, thread or working
directory. Opaque conversation and queue IDs must be valid Unicode between 1
and 256 UTF-8 bytes; `.`, `..`, `/` and NUL are not valid IDs. Mutating
requests require an exact allowed origin, an
application-owned lock shared with other mutations of the same conversation,
and configurable message and request-body limits. Use `NewMutationLock` so a
request waiting for that shared lock can honor its context. Non-streaming
operations have a configurable deadline. An optional shutdown channel closes
event streams promptly. After resolution, the handler asks the App Server to
verify that the
trusted thread is still bound to the trusted working directory before it
performs any operation.

Transcript entries can include `timestamp` (RFC3339) and
`timestampApproximate`. Item lifecycle times come from live events and the
bounded local rollout tail, preferring the item start over its completion or
record time. When an item has no available time, a known turn
time is marked approximate; otherwise the timestamp is omitted. This display
metadata does not change stored conversations or require a migration.

Search, agent tool calls and agent lifecycle entries also include typed
`activity` data. Their existing `summary` and `details` fields remain available
for other renderers and future protocol fields. `createTranscriptActivity(entry)`
returns the shared DOM body for these entries, or `null` for a generic fallback.
Search links allow HTTP and HTTPS without credentials. Agent identities appear
as text; an event does not authorize access to a child conversation.

### Recorded activity

`Client.ReadActivity(ctx, threadID)` reads all paginated turn boundaries and
returns aggregate timing in a fixed-size `ActivitySnapshot`. The latest turn's
`messages` and `toolCalls` count distinct root-thread items regardless of
transcript filters. Messages include assistant message and plan items. Tool
counts exclude outputs, agent lifecycle markers and child tool calls. Activity
reads load full items for the recent 20 turns and only metadata for older turns.
Terminal metadata is cached within the same rollout and invalidated when its
path changes, including after a revert. Each thread backfills independently;
cancelled backfills retain completed pages for the next read. Watches and
active or queued readers keep their history cache until they release it.
Completed unwatched histories are then released. Up to eight incomplete,
unwatched histories remain cached for retry; the oldest idle entry is evicted
when another is added. Ordinary `ReadThread` calls load only the recent 20
turns. Activity history reads accept up to 100,000 turns; this limit does not
remove stored summaries.

To record time without a browser, create a separate client with
`ClientOptions{ObserverOnly: true, ActivityRecorder: recorder}`. Construct the
recorder with `NewActivityRecorder(path)` using a private directory scoped
to one trusted App Server authority. Subscribe to each authorized root thread,
keep the subscription for the application's monitoring lifetime, and call
`ReadActivity` periodically and after notifications. Coalesce frequent events.
Observer clients never answer requests, reject unsupported requests, apply
developer instructions or change thread settings. The ordinary interactive
client retains its response policy.

Observer clients admit at most four connected RPCs at a time. Reads,
subscriptions, reconnect restoration and unsubscribe requests share that
limit. Reconnect uses four workers and replaces pending work when the
connection generation changes. Disconnecting or closing the client cancels
queued work. The observer's ten-second RPC timeout starts after admission;
the caller's context also bounds time spent waiting for a slot.

The recorder holds a lifetime lock on that directory. Each thread has an
independent writer, a bounded current checkpoint, and a compact summary file
for each observed turn. Directories use mode `0700`; files use mode `0600`.
Writes are atomic and synced. Slow or failed writes leave their pending time
unclassified without blocking event capture or another thread's writer.
Snapshots use only durable coverage. Recorder caches are released when their
last watch or read ends; summary files remain available for archived threads.

The recorder stores thread and turn identities, namespaced connection IDs,
outstanding request IDs and blocking categories, elapsed totals, and coverage
bounds. Resolved requests are removed. It does not store prompts, commands or
answers. At most 128 outstanding requests and 128 turns awaiting compaction
fit in one thread's current state; exceeding a bound makes the affected time
unclassified. Historical summaries have no authority-wide size limit.

The application owns the recorder: close observers first, then call
`recorder.Close()`. Closing a client does not close a recorder that other
clients might share. The activity directory is separate from submission
receipts; applications without activity support can ignore it.

Working time is observed root-turn elapsed time outside the union of blocking
questions and approvals, including permission requests. Nonblocking questions
do not pause work. Request resolution from any client, including the terminal,
ends its wait. `workingMs` includes coverage through `observedAtMs`.
`waitingMs` contains closed observed waits inside turns; `betweenTurnsMs`
contains completed gaps between consecutive turns. Add those two values for
total closed user-waiting time. `openWaitingMs` is separate: it describes the
current blocking wait or the time since the latest completed turn while idle.
There is no idle wait before the first own turn.

Use `currentState` (`working`, `waiting`, `idle` or `unclassified`),
`stateSinceMs` and `observedAtMs` for a live display. Browser projections are
display estimates and must not be written back as observation. Disconnects and
restarts end coverage at the last durable checkpoint. Unobserved historical
and offline turn intervals remain in `unclassifiedMs`; they are never assigned
to work. `coverageComplete` and `coverageReason` expose missing coverage, and
`timingApproximate` identifies observation precision. No historical approval
or question durations are reconstructed from rollout text. Summary coverage
must fit the authoritative turn bounds. Changed or ambiguous bounds remain
unclassified; second-precision completion timestamps discard the observed
partial second after completion.

Fork totals include only own turns proven by the rollout's logical fork cutoff
and visible physical lineage. Reverts preserve this distinction. `scope` is
`thread`, `sinceFork` or `unknown`; an ambiguous or unavailable fork lineage
reports unknown scope with no inherited counters or timers.

The HTTP activity endpoint is optional. Set `Target.Activity` to an
`ActivityProvider` to serve `GET /codex/conversations/{id}/activity` after the
usual trusted-thread check and `Read` capability check. The required `Client`
interface is unchanged. Without a provider the endpoint returns 404. The
browser client exposes `activity()` for applications that enable it. A client
without a recorder still aggregates historical boundaries and explicitly
unclassified turn time, without subscribing or resuming the thread.

The handler also serves `assets/conversation.js`. Import
`createConversationClient()` for the HTTP API, `createDurableSender()` for
response-loss-safe message delivery, or `mountConversation()` for the complete
framework-free interface. The mounted client keeps the same browser operation
ID across a retry for both immediate and queued messages. It fails closed when
durable browser storage is unavailable. Default storage keys retain the HTTP
base path and opaque conversation ID. A custom `conversationPath` instead
binds its retries to that effective endpoint, so pending work cannot silently
move to another server-side target. Aliases that intentionally identify the
same target can pass the same application-owned `durableNamespace`. A send
receipt is cleared only after the matching message is visible in the transcript
and acknowledged by the server. Pass an explicit `capabilities` object when
mounting a restricted interface.

`formatTranscriptTimestamp(entry)` supplies the mounted interface's local clock
label, full timestamp tooltip and local calendar date for custom renderers.
It returns `text`, `title`, `dateTime`, `dateKey`, `dateLabel` and `approximate`.
Use nonempty `dateKey` changes to insert date separators without changing entry
order. Missing times return `Time unavailable` and empty date fields.
An optional second argument accepts `locales` and `timeZone`; the default is
the browser's locale and time zone, with a 24-hour clock.

`transcriptEntryCopyText(entry)` returns message Markdown, command and output,
file paths and patches, or other activity summaries and details. It reads the
entry's source fields, including loaded content hidden by a custom renderer.
`createTranscriptCopyButton(entry)` returns a native button using that helper
and the Clipboard API. Its copy icon changes briefly to a check or error icon;
accessible labels and tooltips report `Copied` or `Copy failed`. It
has the `codex-entry-copy` class and `data-copy-state` (`idle`, `copied` or
`error`) for styling. Create it with the current entry whenever the transcript
updates. The mounted interface places this button and the timestamp in a
bottom-right footer on every entry.

`createCopyButton({text, getText, label})` supplies the same control for other
content. Pass a string or callback as `text`, or a `getText` callback evaluated
on click. `getText` takes precedence. Set `label` to describe what is copied;
it defaults to `Copy`. The shared control uses the `codex-copy-button` class.

The client returned by `createConversationClient()` carries its effective
endpoint identity into `createDurableSender()` and `mountConversation()`. A
caller supplying that client does not have to repeat its path options; explicit
sender options must resolve to the same durable identity. A custom client that
does not come from `createConversationClient()` must supply a `basePath`,
`conversationPath` or `durableNamespace` when durable sending is enabled.

Applications with a custom interface can use `createDurableAttemptStore()` as
the same verified persistence boundary. It supports a fixed compatibility key
or multiple attempts under an application-owned prefix, including application
context in the encoded attempt, and verifies every storage write and removal.
`createConversationClient()` also accepts an application-owned absolute
`conversationPath` when an existing endpoint must be retained during a
compatibility window.

Create one interactive `codex.Client` per App Server socket and share it within
an application process; use a separate observer for activity recording. Each
submission-ledger transaction takes an interprocess lock and
reloads the durable state before reading or updating it, so lifecycle tools can
cooperate with a long-lived server without losing attempts. Applications must
still serialize same-conversation App Server mutations through the shared
mutation lock supplied to the handler.

Applications can opt into prompt attachments with `Target.Attachments`. The
`AttachmentProvider` freezes a prompt from authorized file IDs before Send or
Queue, then decorates transcript and queue entries with `displayText` and
`attachments`. It must preserve the original `text` and submission digest.
Applications own storage, authorization, retention and safe deletion.

`NewUploadHandler` serves an application's `UploadStore` beneath an exact
HTTPS origin and base path. It supports file creation, offset and SHA-256 checked
chunks, completion, deletion and download. Each request resolves its scope
independently; eight chunk requests can transfer concurrently. Upload requests
have a two-minute deadline. Downloads use attachment disposition and stream
without loading the whole file. The store supplies limits and must enforce them
atomically across concurrent requests.

The shared `uploads.js` module exports `mountUploads`, `createUploadClient` and
`renderAttachments`. Serve `uploads.js` beside `conversation.js` and include
`uploads.css` when using a custom composer. `mountConversation` accepts an
optional `uploadBasePath`; custom clients pass attachment IDs to `message` as
its fourth argument, or `queueMessage` as its third. Durable sender `send` and
`queue` accept IDs as their second argument. A reload retains selection metadata;
resuming an incomplete upload requires reselecting and verifying the file.

`cmd/codex-web-example` is a standalone reference application. It accepts a
trusted Unix socket, thread ID and working directory on the command line and
serves the reusable browser client on loopback. It rejects non-loopback listen
addresses and browser origins and is not an authenticated remote deployment.
Applications exposing the handler beyond a trusted local browser must provide
their own authentication before granting conversation capabilities.

## Development

Run the package checks with:

```sh
nix flake check --print-build-logs
```

The App Server protocol is experimental. This repository validates its request
corpus against committed protocol fixtures. Applications that package a Codex
binary must also validate this source against schemas generated by the exact
Codex build they deploy.

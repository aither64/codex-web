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

The schema-3 ledger has a 16 MiB limit, including its trailing newline. Reads
and writes reject larger files before changing durable state. Transactions use
an interprocess lock, write a mode-0600 temporary file, sync it, and atomically
replace the ledger. An older client with a 1 MiB reader limit cannot open a
larger ledger even though the schema is unchanged. Stop older writers before
allowing the ledger to grow; recovery after growth needs this version or a
newer compatible reader. Do not truncate the ledger to roll back.

`ThreadSettings.Policy` supplies a persistent thread's role instructions and
`read-only` or `workspace-write` sandbox for explicit start, resume and fork
operations. The client appends role instructions to its common
`ClientOptions.DeveloperInstructions`. Applications with different thread roles
should set `PreserveThreadInstructionsOnResume`: unscoped reads, subscriptions
and reconnects then leave the App Server's persisted instructions intact.
Explicit member assignments can pass the same policy in
`TurnOptions.ThreadPolicy`; it is included in the durable send identity and
reapplied before either a new turn or a steer. `ReconcileThreadInstructions`
refreshes the common policy on an idle thread. Use
`ReconcileThreadInstructionsWithPolicy` to refresh the common policy together
with that thread's application-owned role instructions; it checks that the
thread is idle before resuming it and leaves active turns unchanged.
An application may also bind one host-side stdio MCP tool through
`ThreadPolicy.MCPServer: &ThreadMCPServer{Name, Command, Args, Tool}`. The command
must be a canonical absolute path, the server and tool names must be lowercase
safe identifiers, and arguments must be nonempty. The client emits
`config.mcp_servers` with only that tool enabled, marks the server required, and
sets the tool's approval mode to `approve`. Pass an MCP policy only when its
binding is valid for that destination thread. A tool
whose arguments include the destination thread ID must be omitted from start
and fork, then bound on an explicit resume after the new ID is known and on
each `SendWithOptions` assignment. App Server does not retain MCP configuration
across resume. The application remains responsible for the tool's
authorization and session isolation; this client only binds the tool to a
trusted thread request. Unscoped requests do not acquire a tool.
For queued member work, `StartQueueWithPolicy` rebinds that same policy before
starting the queued turn; ordinary `StartQueue` retains its empty-policy
behavior. Nonempty send options are stored with the durable attempt.
`OriginalSendOptions` retrieves the exact saved options for a matching message
identity, so an application can retry after its default model or effort
changes. The caller must still verify current authority for the thread and
must reject an old policy that is no longer authorized.
`ThreadSettings.ProjectID` selects an immutable project when starting a thread;
`ThreadListOptions.ProjectID` lists that project's threads and rejects results
outside it. A project ID cannot be selected during resume or fork.

`ThreadListOptions.UseStateDBOnly` asks `ListThreads` to read the Codex state
database without scanning JSONL rollouts to repair thread metadata. It sends
`useStateDbOnly: true` only when enabled; false omits the field and preserves
the server's scan-and-repair behavior. Database-only results can omit threads
or contain stale metadata, so callers that need complete discovery must keep
their full-scan recovery path. The selected Codex build must support this field.

`Client.SendWithOptions`, `PrepareSendWithOptions`,
`SendAttemptedWithOptions`, `ReconcileSendWithOptions`,
`DiscardPreparedSendWithOptions`, and `EnsureInitialMessageWithOptions` accept
`TurnOptions` for a caller-selected App Server model, reasoning effort and
application context. Empty model and effort fields are omitted. Model and
effort are emitted only for an idle `turn/start`; a send that steers an active
turn never changes them. App Server permits application context on both start
and steer, so a nonempty `AdditionalContext` map accompanies the message in
either case. Its entries are keyed by an opaque application source identifier,
must use `Kind: "application"`, and are bounded to 32 entries, 256-byte keys,
64 KiB values and 256 KiB total. Options are canonicalized into the durable
send-attempt identity, so retries and every prepare/reconcile/discard call must
use the original options. An omitted stored digest is the canonical zero
options identity; zero-option activity never writes `optionsDigest`, retaining
schema-3 reader and rollback compatibility. Existing methods remain exact
zero-option wrappers.

`Client.CompactAcceptedSendOptions(contextPrefix)` removes stored original
`TurnOptions` only from accepted send attempts whose application-owned action
context starts with that nonempty prefix. It keeps each message digest, action
context, options digest, accepted turn ID and steering flag, so a retry with the
same text, context and options returns the same receipt; changed values still
fail. Prepared and submitting attempts and other contexts retain their options.
Compaction is idempotent. Choose a prefix reserved for application sends, such
as `team:`, and call it before reserving more of those sends. After compaction,
`OriginalSendOptions` cannot recover nonzero options for those accepted sends;
the application must retain and supply the exact options for retries. Browser
send contexts should not be selected because browser recovery may need those
stored options.

`Client.ClearRetiredThreadAttempts(threadID)` removes queue, send and deletion
attempts for one thread after the application has proved that thread is retired.
It does not verify retirement itself and never removes directory operation
markers, including a root thread's retirement marker. Repeating the cleanup is
safe. Active threads must keep their attempts so uncertain submissions can be
recovered.

`EnsureInitialMessageWithOptions` does not itself durably bind options before
an initial `turn/start`. A recovering application must prepublish and retain
the exact binding before that call; the workspace team's Phase 2B creation
flow owns that durable precondition. It must never retry a materialized initial
thread with caller-supplied replacement options.

Nonblocking user-input requests stay pending by default. Set
`ClientOptions.NonBlockingUserInput` only when the embedding application owns
an explicit automatic-response policy.

Pending prompts expose an opaque `token` for each offer. Send that token with
answers, decisions and snoozes through `RespondPrompt`; an expired offer returns
`PromptResponseError`. Its `NotSent` field distinguishes a rejected response
from an uncertain transport outcome. Only retry definitely unsent answers after
restoring and checking the same thread, turn, item and question contents.
The legacy Go response methods remain available to direct callers. HTTP clients
backed by `PromptResponder` must supply tokens; older browser requests receive
`reload_required`. Other implementations retain their existing HTTP contract.

`Client.ReadAccountRateLimits(ctx)` reads current usage with
`account/rateLimits/read`. Its typed result contains the legacy `RateLimits`
snapshot and named `RateLimitsByLimitID` buckets, with nullable windows,
durations and reset times. Match `WindowDurationMins` to identify a window;
primary and secondary positions can represent different durations. `ResetsAt`
is Unix time in seconds. Optional `AccountID`, bucket `Credits` and account-level
`ResetCredits` preserve unreported values. `AvailableCount` is authoritative;
`Credits` may be null or contain only some reset details. Applications authorize
and expose this account-level read separately from the conversation handler.

`Client.ConsumeRateLimitResetCredit(ctx, idempotencyKey, creditID)` redeems one
banked reset through `account/rateLimitResetCredit/consume`. Pass a nonempty,
unique key for one confirmed user action and retain it across uncertain retries.
An empty credit ID asks Codex to choose the next available reset. The method
accepts the outcomes `reset`, `alreadyRedeemed`, `nothingToReset` and `noCredit`;
unknown outcomes fail. Account reads never call this method. Embedding
applications own confirmation, account binding, durable retry records and an
explicit mutation route. The conversation `Client` interface is unchanged.

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

Transcripts expose `latestTurnId` from the newest turn in App Server history,
even when that turn has no rendered items. Consumers offering a plan decision
must require a completed plan entry belonging to that turn; older entries are
history, and a missing turn identity does not establish a current proposal.

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

`Client.ReadThreadPage(ctx, threadID, cursor)` reads a newest page when `cursor`
is empty, or continues toward older history with the returned `olderCursor`.
Each call consumes at most 100 source items and turn failure rows. Hidden
reasoning items consume a slot, so a page can contain fewer than 100 visible
entries. Entries are chronological within each page. `hasOlder` reports whether
another page is available, including after an empty page caused by itemless
turns. Keep following `olderCursor` to reach older history. Page entries use the
same item normalization, client message identity and digest as `ReadThread`.
The current thread status, settings and `latestTurnId` accompany older pages.

Paging reads turn metadata and turn-filtered item lists. It does not request
full turns or synchronously decode the rollout tail. A page initially uses live
item times or approximate turn times while a bounded background scan fills the
timestamp and collaboration-mode cache. `metadataPending` asks callers to
refresh the page after enrichment. An unknown collaboration mode is omitted;
callers must wait for a known mode before offering mode-dependent plan actions.
The persisted timing cache is disposable, and older items outside the 64 MiB
rollout suffix can retain approximate times. Appending to a rollout parses new
complete records. Replacement, truncation, a same-size edit detected through
mtime, or a change in the previous 4 KiB tail immediately clears the cached
mode and exact times. An
incomplete final record is retried after later growth.

The rollout metadata cache expires 60 seconds after its last full suffix scan
began. Cache hits and incremental appends do not extend that age. On expiry, a
page withholds rollout-derived mode and exact times, sets `metadataPending`,
and queues one background rebuild from empty metadata, even when the file has
not changed. Validated settings notifications remain an independent mode
source. A failed or delayed rebuild leaves rollout mode unknown and times
approximate until a later retry succeeds. An earlier in-place edit followed by
an append can escape the small tail probe until expiry; this display cache is
not used as authority for thread mutations. The cache records full rebuild
count, bytes read and elapsed scan time for performance diagnostics. Each
rebuild may read up to the existing 64 MiB rollout suffix.

`GET /codex/conversations/{id}/thread/page` exposes this method when the
resolved client implements the optional `TranscriptPageReader`. An omitted
`cursor` requests the newest page. The handler verifies the trusted thread and
read capability on every request, then applies the ordinary transcript
transform and attachment observation. It rejects other query parameters,
duplicate cursors and malformed tokens with HTTP 400 and
`transcript_cursor_invalid`. Expired or evicted cursors return HTTP 409 and
`transcript_cursor_expired`; a changed thread, connection or rollout returns
HTTP 409 and `transcript_reset_required`. A continuation also resets when its
newest turn was active and then changes status, so a fresh page can include its
final error. Cursors are opaque, local to the
client instance, idle-expiring after 30 minutes and held in a bounded cache
of 256 continuations and 8 MiB. A cursor can be retried without consuming it.
After read authorization, clients without paging return HTTP 501 and
`transcript_paging_unavailable`. The existing `/thread` response remains the
recent 20 full turns. `createConversationClient().threadPage({cursor, signal})`
calls the page route. `mountConversation()` loads the newest page first and offers
**Load older** while a continuation exists. It keeps loaded entries when newer
pages arrive, repairs gaps after missed events, and keeps the visible scroll
position when older entries are added. A missing page route or an explicit
`transcript_paging_unavailable` response falls back to an authorized `/thread`
read; the interface then says that older history is unavailable. Other HTTP,
timeout, cursor and malformed-response errors do not trigger that fallback.

The composer shows the last confirmed model and reasoning effort with **Edit**.
The dialog saves both values together while the thread is idle. Closing it or
pressing Escape discards edits. Background reads preserve edits while the dialog
is open, and reads started before a successful save cannot replace the saved
pair. An uncertain save rereads the thread without sending a compensating write.

`refreshPolicy` in `conversation/assets/refresh.js` owns resource freshness,
deadlines, retry backoff and notice delays. `createRefreshNotice()` retains the
last successful value during background reads. Initial loading appears after
750 ms, manual loading after 250 ms, and refresh failures after 30 continuous
visible seconds. Hidden tabs and page restoration restart that warning grace.
Access failures and failed user actions appear immediately. Automatic history
repair retries with capped backoff; manual older-page failures wait for Retry.
Hosts can override individual policy values when mounting a conversation or
creating a notice or sync controller. Domain controllers retain cursor,
generation and mutation-recovery rules.

Custom interfaces can use `readTranscriptPage(client, {signal, cursor, legacy,
expectedThreadId})` for the same page validation and fallback policy, and
`createTranscriptHistory()` to merge newest, older and repair pages. The history
model exposes retained `rows` and `entries`, `olderCursor`, `hasOlder`, `gap`
and `repairCursor`; call `applyNewest`, `applyOlder` or `applyRepair` for each
validated page. `transcriptEntryKey(entry, index, entries)` supplies the stable
key used for item and turn-error rows. The model retains loaded entries in
server order and never treats absence from a newest page as proof of deletion.
After a 409 cursor error, discard traversal tokens while keeping loaded rows
visible. Build an authoritative range from the fresh newest page and its
continuations. Once that range reaches the oldest retained entry or the end of
history, replace the covered rows together; this also removes entries deleted
from the new lineage. The expected thread identity also applies to authorized
legacy `/thread` reads. Capture `repairVersion` with a historical cursor and
discard its response or error if that version changes before it settles.

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
Pending requests and queue reconciliation load separately from the transcript.
Their failures leave the last known controls visible with a retry action. A
pending send is acknowledged only after a matching message is observed in the
retained transcript, including an older page; a partial newest page cannot
prove that the message is absent.

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

Custom interfaces can use `createConversationSync({eventsPath, read, apply,
onStateChange})` for conversation recovery. `read(signal)` returns a snapshot;
`apply(snapshot, {signal, isCurrent})` renders it. Check `isCurrent()` after
asynchronous rendering steps before changing the interface. The controller
provides `refresh()`, coalesced `scheduleRefresh(delay)`, `retry()`, and
`destroy()`. Call `destroy()` when unmounting. `live: false` disables background
polling and streaming; `eventStream: false` selects snapshot polling only.

Refresh cycles expire after 35 seconds and cancel unfinished sibling work when
they settle. The controller refreshes on reconnection,
focus, visibility, pageshow and network restoration, and every 60 seconds while
visible. It retries failures with backoff and detects stalled advertised
heartbeats. The event endpoint emits `ready` with `heartbeatIntervalMs: 20000`
and named `heartbeat` events every 20 seconds. The browser allows two advertised
intervals plus five seconds before treating a stream as stale. Existing message notifications
keep their format. Servers without this advertisement use snapshot polling.

GET client methods accept an optional `{signal}` and have a 35-second request
deadline. `reconcileQueue({signal})` also accepts cancellation; other mutation
methods retain their existing behavior. Cancellation of a request does not
prove that a server-side mutation failed. Keep normal durable receipt recovery.
`onStateChange` receives `status`, `lastSuccessAt`, `error`, and `httpStatus`.
`renderConnectionStatus(element, state, retry)` displays a connection notice
separately from conversation activity and leaves message controls usable.

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

Attachment-aware queue deletion also requires `QueueDeletionCompleter`, which
`codex.Client` implements. Its completion callback updates the provider before
forgetting the durable deletion attempt. Editable browser surfaces use `POST /queue/reconcile` under the ordinary
mutation authority and lock before reading the queue. Recovery finishes only
recorded cancellations whose entries are already absent; it never deletes a
remaining entry. `GET /queue` does not perform cancellation recovery. Failed completion can therefore recover after a reload or restart.
`RequireSubmissionAttemptsResolved` reports pending deletions without changing
them. Retry a remaining deletion through its owning queue surface.

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
Failed selections remain removable after a reload. Never-started and rejected
creations are removed locally; an uncertain creation is retried with the same
client identity before deleting its server file. Stores must recover existing
client identities before applying stricter rules to new uploads, and reject
creation with HTTP 400 or 413 only when that request did not create an upload. Other
errors retain the uncertain outcome. When deletion returns 404, an authorized
listing must confirm the file is absent or deleted before the browser clears its
selection. Hosts may use 404 for a temporarily unavailable scope. Browser storage
must confirm the updated draft before the card disappears; an already deleted
file cannot become ready again while its card awaits local removal.

Upload names are untrusted display metadata. Render them as text, serialize
prompt references as JSON, and derive storage paths from application-generated
identities. Download responses use MIME filename encoding and attachment
disposition. Upload creation accepts at most 4096 bytes of valid UTF-8 JSON;
the store owns filename and file-size policy.
`mountUploads(root, options)` renders draft cards, a selection summary and errors
in `root`. The summary shows the selected file count and full total size in
binary units, plus the completed count while any file is unfinished. Completion
requires server acknowledgement. Paused, failed, missing and deleted selections
remain in the total until removed. A file awaiting removal stays selected; a
failed removal does not undo its completed upload. The summary is separate from
errors and does not change submission readiness. It disappears with an empty
selection. Submitted files are cleared from the composer without deleting them.
Pass `options.pasteTarget` to attach clipboard Files from a textarea's paste
event. File items take precedence over the clipboard file list, so browsers that
expose both attach each file once. Plain text and HTML retain native paste
behavior, including mixed text/file pastes. Clipboard text, URLs and paths do
not become files. Pasted files follow the same limits, upload retry and readiness
checks as selected or dropped files.
Pass `options.controlsRoot` to put its + attachment menu beside your form actions;
omit it to keep everything in `root`. A separate empty card root is hidden.
`destroy()` removes the component's controls and listeners while preserving
adjacent host actions. The menu uses the browser's Popover API.

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

## Ephemeral utility turns

`Client.RunEphemeralTurn(ctx, EphemeralTurnOptions)` returns
`EphemeralTurnResult{Text: ...}` for one completed final answer. It is an additive
Go API for trusted application callers; there is no browser route for selecting
its socket, paths, model or restriction settings.

The supported profile is Codex 0.160.0 with `Model: "gpt-5.5"` and
`Effort: "low"`. Other pairs fail with `ErrEphemeralSettings` before connecting.
The caller runs a dedicated utility App Server with the full original model
catalog selected at startup. This helper does not support CodeModeOnly models
or certify an arbitrary daemon. Model names and `model/list` alone do not
establish tool mode: an ordinary dynamic catalog can refresh that hidden field.

Options require that exact pair, explicit nonempty `Instructions`,
UTF-8 text `Input`, and a JSON-object `OutputSchema`. `Directory` is an empty,
canonical absolute directory owned by the application user, with mode 0700 and
a private parent, outside workspace/project trees. `InstructionFile` is a
separate canonical absolute regular mode-0600 file in private application
state outside that cwd. It must have one hard link and cannot be a symlink. Write
exactly `[]byte(codex.EphemeralInstructionFileContent)` to this unique per-call
file and keep its path and bytes unchanged through teardown. The helper verifies
the exact length and bytes before connecting, with a bounded read. Empty,
whitespace-only, altered, unreadable or unsafe files fail with the static
isolation error. A neutral filename such as `instructions.txt` is suitable;
the API imposes no filename convention.
The application provisions and removes its scratch state; the helper creates
no files. Keep project configuration, AGENTS and skills out of that state.
Empty-file callers of this unreleased API must update together with the provider
dependency; there is no legacy empty-file fallback.

`ModelCatalogFile` is a canonical absolute regular read-only file containing
the full unchanged original Codex 0.160.0 model catalog, at most 1 MiB. Its
SHA-256 is
`fd219bd9f061278275f528939f82f54d2eb97df4b25c23b022adbe48813d920b`.
Supply an immutable resource for the utility daemon's entire lifetime; resolve
package links before passing its path. This public metadata file does not use
the private instruction file's ownership, mode-0600 or hard-link rules. Missing,
writable, oversized or mismatched contents fail with the static isolation
error before connection.

Launch the pinned native utility App Server with
`-c 'model_catalog_json="<canonical-catalog-path>"'` before `app-server`,
and use `--strict-config`.
That startup setting selects the native static models manager. Do not set it
only in thread config or write it into user config after startup: those paths
cannot replace an existing dynamic manager. Before thread creation, the helper
requires `config/read` to return the exact catalog path and
`origins["model_catalog_json"].name.type == "sessionFlags"`.
`includeLayers` remains false; origins are still present. User/project origins,
missing metadata and managed overrides that obscure the startup origin fail
closed. No caller-supplied hash, config map or assertion bypasses this check.

The context must have a finite deadline. Input is limited to 64 KiB;
instructions and schema to 16 KiB each; final text to 16 KiB. Each JSON frame is
limited to 256 KiB, the call to 256 envelopes and 1 MiB of received JSON, and
provisional completion events to 32. JSON depth/node limits and duplicate-key
rejection apply to schemas and consumed envelopes. Model discovery permits at
most eight pages of 100 entries, with bounded nonrepeating cursors. Overflow,
missing settings and model substitution fail the call.

Each call copies only the trusted socket and client identity into a new private
connection. It uses the existing initialization and generation-bound transport,
then captures effective config and managed requirements, verifies the requested
model/effort, and starts one ephemeral thread/turn. It does not copy parent
instructions, roots, observers, activity recording, watchers, question timers
or submission state. It does not use normal Send/Resume/Subscribe/history
helpers, reconnect, repeat a turn or adopt an uncertain thread.

The fixed policy in `codex/ephemeral_policy.json` disables file/command/image,
web/app/MCP/skill/plugin, delegation, memory/goal and hook tools. Both starts
supply empty execution environments and runtime roots, read-only sandboxing and
never approval, plus explicit instructions and both private instruction-file
overrides. The pinned server loads both files before applying instruction
precedence and rejects empty contents. `EphemeralInstructionFileContent` is
`"Preserve the current utility task and its explicit instructions.\n"`. Explicit
base instructions remain authoritative. The fixed file text supplies the compact
prompt when the empty inline `compact_prompt` is trimmed away; compaction remains
available. Both private overrides replace inherited model and compact-file text.
The application owns the file and must keep it unchanged throughout the call.
Configured MCP names are captured once for the private cwd and disabled as
literal nested map keys, including names with dots. Unreadable inventories,
conflicting managed requirements and required hooks fail before inference.
The helper has no caller-controlled config/tool options.

Global user instructions remain trusted serving-instance policy. Operators
must pause/stop naming through existing service/package procedures before
changing MCP configuration; fresh calls then capture all newly configured
names. This boundary does not cover deliberate administrator reconfiguration
during a call. No shared config lease or Codex-home rewrite is performed.

The private sink rejects server requests before ordinary prompt admission,
activity, notices or timers. An actual question request receives JSON-RPC
`-32601` with the fixed message
`Questions are unavailable for ephemeral utility turns`; other requests receive
a fixed unsupported-request error. Either fails the utility. Metadata-driven
async questions are started/completed agent-message notifications with no
request ID: they immediately fail the utility without a fabricated reply or UI
admission. A later answer cannot restore success. The supported original Direct
profile and fixed restrictions expose no model tools, including clock, async
questions or JavaScript exec/wait. A hidden tool dispatch must still fail at
the native registry; an empty advertised list alone is not isolation proof.

Success requires the exact connection generation, thread and turn, successful
`turn/completed`, and exactly one completed agent message with explicit
`phase: final_answer`. Commentary and streamed partial text are excluded; an
unknown/missing phase is an error. Duplicate evidence for the same final item is
accepted only when its ID/text agree. Conflicting final items fail.

The helper reserves at most 250 ms within the original deadline for cleanup.
It interrupts only an owned exact turn on failure, unsubscribes only a known
exact thread, and always closes the private connection. After issuing its one
turn/start, a matching early turn or item notification may establish the turn
ID for interruption if the response is lost. That identity cannot authorize
success; foreign or contradictory identities prohibit interruption. Unsubscribe
alone does not stop an active turn. Cleanup never extends the caller's hard
deadline or archives/deletes persisted threads. A lost start response returns
an error. `ErrEphemeralIsolation`, `ErrEphemeralSettings`,
`ErrEphemeralProtocol` and `ErrEphemeralServer` carry static safe messages;
caller cancellation/deadline remains detectable with `errors.Is`. Unsubscribe
does not promise immediate native thread unload. CodeModeOnly models are
excluded because their yielded cells can survive completed turns and the pinned
public protocol has no scoped stop/join operation for them.

Native ephemeral mode omits resumable conversation storage. Codex's separate
SQLite diagnostic logger can retain plaintext utility input and thread/turn
identities in `logs_2.sqlite` under the configured SQLite home. The helper keeps
that existing local logger enabled. Codex 0.160.0 prunes rows older than ten days
at startup; this is not a guaranteed erasure deadline. Application operational
logs and normal conversation history have separate retention contracts.

Fake-transport tests verify client behavior, not model-action isolation.
`test/codex_protocol_contract.py` includes every literal utility call site and
consumed shape. Separately validate fixed restriction keys/types with
`test/codex_ephemeral_config_contract.py` against the exact candidate's
`codex-rs/core/config.schema.json`.

After mandatory independent review, the tagged
`TestEphemeralProtocolIntegration` exercises the exact candidate with a local
mock of the built-in OpenAI Responses provider. It retains the binary's model
catalog and built-in provider capabilities. The mock returns HTTP 426 for valid
WebSocket upgrades, so the native server selects its supported HTTP Responses
fallback. Plain GET remains 404; inference must reach the mock through real
POST requests. This covers native HTTP/SSE behavior. Successful WebSocket
framing, continuation caching, reconnects and hostile tool delivery over
WebSockets require separate coverage. Release verification also requires a
positive model call using the serving instance's unchanged transport settings.

The control conversation must prove sentinel/tool eligibility. The utility
inspects actual advertised schemas and hostile dispatch, including rejected
clock and async-question names. Separate labelled controls temporarily enable
actual-ID questions to verify rejection and cancellation. The fixture also
checks early events, cancellation/disconnect and an ordinary concurrent client.
It holds inference without tools open while dropping turn/start responses,
requiring exact interruption and actual provider cancellation. It inspects
pinned-source goal/memory stores and persistent synthetic state, using private
Linux user/network/mount/PID namespaces with nftables egress restricted to the
mock provider. The test process is namespace init. It verifies that private
ownership before each namespace-wide signal, waits for both native process
owners, then reaps adopted descendants until ECHILD proves the domain empty.
An ordinary completed turn can leave background writers, so the fixture proves
ordinary eligibility before this stop/join boundary, restarts the same profiles
and home, reads the persisted control thread, and captures one seeded baseline.
Persistence inspection permits rows only in the exact native diagnostic table
after validating its pinned schema. Markers in other tables or files still fail;
goal and memory records retain their full equality checks.
It cannot silently fall back to host networking or deterministic-only completion.

Explicit invocation requires absolute `CODEX_EPHEMERAL_TEST_BINARY`, its reviewed
`CODEX_EPHEMERAL_TEST_SHA256`, exact `CODEX_EPHEMERAL_TEST_SOURCE` (`codex-rs`),
and executable paths `CODEX_EPHEMERAL_TEST_IP`, `CODEX_EPHEMERAL_TEST_NFT` and
`CODEX_EPHEMERAL_TEST_SQLITE`. The host must permit unprivileged namespaces and
the private firewall/mount operations. Missing prerequisites or a different
binary fail the test. These fixture prerequisites are not production runtime
dependencies. Run the non-model ownership control first in the repository Nix
environment after review. It verifies detached and orphaned descendants in two
start/stop cycles. Proceed to the native protocol fixture only if it passes:

```sh
go test -mod=readonly -tags=codex_integration ./codex \
  -run '^TestEphemeralNamespaceOwnershipIntegration$' -count=1 -timeout=180s
go test -mod=readonly -tags=codex_integration ./codex \
  -run '^TestEphemeralProtocolIntegration$' -count=1 -timeout=180s
```

A passing schema or fake-transport check does not authorize live use. Record the
exact binary/profile/model identities and executed isolation evidence before
enabling a deployment that uses this helper.

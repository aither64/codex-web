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
`mountUploads(root, options)` renders draft cards and errors in `root`. Pass
`options.controlsRoot` to put its + attachment menu beside your form actions;
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

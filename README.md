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

Create one `codex.Client` per App Server socket and share it within an
application process. Each ledger transaction takes an interprocess lock and
reloads the durable state before reading or updating it, so lifecycle tools can
cooperate with a long-lived server without losing attempts. Applications must
still serialize same-conversation App Server mutations through the shared
mutation lock supplied to the handler.

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

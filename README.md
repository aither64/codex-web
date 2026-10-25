# codex-web

For a Codex release upgrade, follow the
[compatibility update skill](skills/update-codex-web/SKILL.md).

`codex-web` lets a Go application present and control trusted Codex App Server
conversations in a browser. It is for applications that already own user
authentication and conversation access decisions.

It includes:

- a Go client for Codex App Server over a Unix socket;
- a capability-checked HTTP handler for transcripts, messages, queued work,
  settings, questions, approvals, and interruption;
- browser modules for a complete conversation UI or a custom interface;
- durable message delivery, activity timing, and optional prompt attachments;
- exact failed-turn/context observations and idle-only continuation primitives.

The package does not decide who may access a conversation. Your application
maps its own opaque conversation ID to the trusted Codex thread, working
directory, App Server client, and allowed operations.

Applications can pass `ThreadSettings.ModelProvider` on lifecycle calls or set
`ClientOptions.LifecycleSettings` to resolve routing before every start, resume,
fork and reconnect. Applications with session-specific routing can use
`LifecycleRequestSettings` instead: it receives the method, source thread ID
and trusted destination directory, including for new threads and forks. It
takes precedence over the original callback. A reconnect can omit the directory;
resolve that case from the retained thread identity. The resolver supplies retained model and reasoning effort
when the caller omits them. Explicit caller model/effort settings take precedence;
the application resolver owns provider selection. A requested provider must match
the top-level effective provider in the native response. A mismatch is an error,
including when an already loaded thread ignores a provider override. Changing
existing routing requires the application's own idle transition and recovery
workflow. Passive observer clients do not resume threads or resolve routing.

Trusted application callers can set `ThreadPolicy.Sandbox` to `danger-full-access`
and `ThreadPolicy.ApprovalPolicy` to `never` for unrestricted command execution.
Apply the policy when creating, resuming or forking a thread and on later sends
and queue starts. These fields are application-owned; do not accept execution
policy from untrusted browser input. Omitted fields retain existing behavior.
Loaded threads receive unrestricted execution through `thread/settings/update`
before resume can release saved work; native resume overrides alone do not
change their retained permissions. Cold threads receive the policy in their
load request. Settings updates apply to subsequent turns and preserve an active
turn's policy.
Use `QueueWithPolicy` for application-owned queued work: adding to a loaded
native thread can execute immediately. It applies permissions before enqueueing
and keeps cold queues unloaded until explicit activation.

Set `conversation.Target.ModelCatalog` when a conversation's provider has a
different catalog from the shared App Server. The picker and settings validation
both use that callback. Without it they use `Client.ListModels`.
After an application closes and reopens native threads without disconnecting
the server, call `RefreshThreadSubscription` for affected threads. It rejoins
existing watches and retains the application's execution admission policy.

## Try the local example

The reference application serves one trusted conversation on a loopback
address. Start it with the App Server socket, thread ID, and canonical working
directory that your application already trusts:

```sh
go run ./cmd/codex-web-example \
  -socket /path/to/app-server.sock \
  -thread THREAD_ID \
  -cwd /canonical/project/path
```

Open `http://127.0.0.1:8080`. The example accepts only loopback listen
addresses and browser origins. It is useful for local evaluation, not as an
authenticated remote service.

## Add it to an application

Create one `codex.Client` for each App Server socket and share it within the
application process. Mount `conversation.NewHandler` at a path such as
`/codex/`, then resolve every request from your application's conversation ID:

```go
handler, err := conversation.NewHandler(conversation.Options{
    AllowedOrigins: []string{"https://app.example"},
    Resolver: conversation.ResolverFunc(func(_ context.Context, request conversation.ResolveRequest) (conversation.Target, error) {
        // Look up request.ID in application-owned, authorized state.
        return conversation.Target{
            Client: client, ThreadID: threadID, Directory: canonicalDirectory,
            MutationLock: mutationLock,
            Capabilities: conversation.Capabilities{Read: true, Send: true},
        }, nil
    }),
})
if err != nil {
    panic(err)
}
mux.Handle("/codex/", handler)
```

Use a shared `conversation.NewMutationLock()` for every mutation of the same
conversation. Choose capabilities separately for transcript reads, pending
requests, queue reads, and mutations. The handler verifies that the trusted
thread still belongs to the trusted working directory before each operation.

Never take a socket path, thread ID, or working directory from browser input.
Use an exact allowed origin for mutations and keep durable submission state in
writable private storage when the socket directory is temporary.

## Browser API

The handler serves `conversation.js`, `conversation.css`, and, when enabled,
the upload assets below its base path. Import one of these browser APIs:

- `mountConversation()` for the complete framework-free conversation UI;
- `mountRetryActivity()` for an optional retry-chain notice in a custom transcript;
- `createConversationClient()` for the HTTP API;
- `createDurableSender()` for retry-safe message submission;
- `createConversationSync()` for a custom synchronized interface.

The handler's optional activity provider exposes timing to `client.activity()`.
Snapshots include lifetime sent and received conversation-message counts and tool
calls. Distinct item IDs count once; fork totals exclude inherited turns. Older
turn pages are summarized and discarded after counting. `countsComplete` marks
incomplete history. Working, waiting and idle durations are separate. Explicit
sleep and collaboration waits expose `waitReason` and optional `waitUntilMs`;
blocking questions and approvals take precedence. Turn lifecycle determines
activity even when native thread status becomes idle during sleep. Unobserved
turn time remains unclassified. On reconnect, ongoing team waits are restored
from their native status. Sleep items have no lifecycle status in history; if
the observer missed the turn start, ambiguous time stays unclassified until
fresh native active status establishes current work. Recovered coverage starts
at that observation and never assigns earlier unknown intervals to work. The additive checkpoint fields can be ignored by
preceding versions; restoring older checkpoints does not invent wait coverage.
Attachment support uses `NewUploadHandler` and application-owned storage and
authorization.

## History reads

Applications with large conversations should share a `codex.NewHistoryPool(socket)`
through `ClientOptions.HistoryReader` on their interactive and observer clients.
The application closes the pool after those clients stop. The pool has two
passive connections and admits only history reads. It keeps history failures
separate from live subscriptions and approval offers. Omitting the option keeps
the existing client transport behavior.

With this reader, activity uses turn metadata and cursor-paged items. It retains
counts and small item projections rather than message bodies or tool output.
Completed turns contribute cached totals; cold history backfills in small batches
and reports `refreshing: true` with `countsComplete: false` until it finishes.
Fork verification runs in the background and then scans only appended records.
These caches are disposable and do not change activity checkpoint formats.

Item reads start at 32 entries. If a reply exceeds the 64 MiB message safeguard,
the reader halves the page size and retries its original cursor. A single item
that exceeds the safeguard returns an explicit error. Conversation length has
no total byte or turn limit. Transcript paging keeps full content available
without requesting whole turns. Hosts may retain their last good snapshot with
`stale: true`; its observation timestamp must remain unchanged, and it must not
produce fresh notifications.

## Reference and development

The [technical reference](docs/reference.md) contains the complete Go and
browser API contract, including durable delivery, prompt responses, activity,
uploads, compatibility, and protocol requirements.

The [submission storage guide](docs/submission-storage.md) explains indexed
receipt storage, delivery boundaries and migration from the JSON ledger.

Run the packaged checks with:

```sh
nix flake check --print-build-logs
```

The Codex App Server protocol is experimental. Validate an application against
the schemas generated by the exact Codex build it deploys.

## License

Licensed under the [Apache License 2.0](LICENSE).

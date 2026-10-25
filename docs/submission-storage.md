# Submission storage and delivery recovery

`ClientOptions.SubmissionDatabasePath` enables SQLite storage for send, queue,
deletion and retirement records. Cooperating clients must select the same
database and predecessor paths. Clients without this option continue using the
schema-3 JSON ledger at `SubmissionLedgerPath`.

SQLite uses WAL mode and full synchronization. Ordinary transactions load the
requested attempt or thread through an index. Network requests run outside
database transactions. Applications acknowledge or retire accepted receipts;
unresolved attempts are never removed by age. `ExportSubmissionAttempts` returns
a consistent schema-3 snapshot of one thread, independently of the backend.

## Delivery boundaries

A send reserves its exact text digest, action context and options before
dispatch. It claims the prepared attempt immediately before requesting a
WebSocket writer. Durable admission can consume the remaining deadline, so the
client checks cancellation again after that transaction.

Cancellation there, or failure to acquire the writer, proves that the operation
handed no message bytes to the writer. The client restores `prepared` before
reporting `MessageNotSentError`. SQLite admission claims prevent an older
operation from resetting a newer claim. Failed restoration leaves the outcome
uncertain.

Once writing begins, transport failure can leave delivery uncertain. The client
checks native history for the original identity and retains the attempt when
acceptance is not visible. Native client IDs provide correlation, not duplicate
suppression. Missing history cannot authorize another native submission.

`MessageNotSentError.Retryable` identifies transient connection or admission
failures. Protocol refusals remain unsent but need a corrected request. HTTP
responses expose `retryable` beside `code` and `notSent`. Browser retries reuse
the saved identity, back off from 250 ms to five seconds, and wait while the page
is hidden or unavailable. Uncertain attempts retain their receipt and status
check. Retries leave current drafts alone.

`DeliveryObserver` receives stage durations, identities and failure flags without
input text or turn options. Observers must return promptly and must not call the
client recursively. `TestCancellationDuringDurableAdmissionRemainsRetryable`
tests cancellation after the durable claim. `BenchmarkSubmissionStoreLookup`
compares the backends using synthetic retained receipts.

## Upgrade from schema-3 JSON

Exclude predecessor writers through the application's package transition before
starting new clients. Native conversation processes can continue running.

The first database access locks both configured JSON paths. It validates and
imports the primary ledger, or its legacy fallback when the primary is absent,
and preserves private, checksum-named backups beside the persistent database. Send states,
options, queue attempts, deletion intents and retirement markers survive import.

The import transaction records its phase with the imported data. Initialization
then publishes schema-4 SQLite fences at the JSON paths. Older clients refuse
those files. The database becomes ready after all fences exist. Retrying resumes
an interrupted publication, including after a volatile predecessor disappears
across reboot. A missing imported source requires its checksummed persistent
backup. Changed predecessor contents or database identities stop recovery
without replacing records. If a backup remains before the import journal commits
and all predecessors are absent, initialization refuses an empty import. Restore
the authoritative predecessor under writer exclusion before retrying; checksum
names alone cannot select an unreferenced snapshot. Once ready, the database is
authoritative. Under the predecessor locks, initialization validates present
fences and restores absent
ones without reimporting records. Unexpected present files still stop recovery.

The database schema is version 1. Keep the database, WAL, shared-memory files,
backups and fences in private application state. Preserve the database and WAL
together for an offline copy; use SQLite backup facilities for an online copy.

After migration, recover forward with a compatible package. Restoring an older
JSON backup discards newer receipts and can permit duplicate delivery.
Applications must enforce this boundary in their package transition policy.

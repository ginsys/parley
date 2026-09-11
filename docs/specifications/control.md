# Human control protocol, storage concurrency and ownership

Draft for [#25](https://github.com/ginsys/parley/issues/25), expanding the accepted
[control decision](../architecture.md#accepted-human-control-protocol),
[membership contract](membership.md) and [connection contract](connections.md).
The direction is accepted; the exact methods, limits and coordination rules here remain subject
to specification review. No server, client or migration is implemented by this document.

## Authority and endpoint configuration

The initial human endpoint is a pathname Unix stream socket. Server configuration supplies an
absolute `admin_socket`, trusted `server_uid`, and explicit `administrators` entries mapping an
immutable administrator UUID to one allowed connector UID. Duplicate IDs/UIDs are invalid. Retired
administrator IDs are never reused; OS account reuse requires a new administrator ID and review
of its privilege routes. Server and socket parents must be trusted, not agent-writable. Require
socket mode `0600`, or `0660` with an explicitly provisioned administrator-only group; kernel UID
allowlisting is required in either case. Reject abstract sockets. No TCP listener or URI fallback.

The client uses `--endpoint`, then `PARLEY_ENDPOINT`, then a trusted client configuration entry,
with the same precedence for `--server-uid`, `PARLEY_SERVER_UID` and configured server UID. No
implicit endpoint or guessed server UID is permitted. `PARLEY_DB` is server-only and cannot become
a client fallback. Relative endpoint paths and non-Unix schemes fail local validation. The client
checks the protected path and expected kernel server UID before sending any request. The server
checks `SO_PEERCRED` and constructs the administrator principal from configuration, never payload
fields or peer credentials. A peer credential presented on this endpoint cannot elevate its holder.

Each request rechecks the configured administrator capability; configuration is immutable for a
server lifetime, and changing the allowlist requires controlled restart and reauthentication. Any
process under the trusted admin UID has that authority: this does not detect human intent. The
production account separation and same-account development limitations remain in force. Do not
run the current protected CLI or construct actual grant/revoke/renew arguments from an agent.

Agent-facing authentication/discovery/send/ingestion use a separately authorized surface following
connections.md. The human socket never accepts those operations as shortcuts or executes arbitrary
agent-supplied method names. No endpoint can approve host tool calls, start processes or turn text
into commands. Server help and client help/argument validation work without DB/socket access.

## Wire profile and limits

Protocol name: `parley-control/1`. This deliberately restricts JSON-RPC 2.0 to one request object
per LF-terminated UTF-8 frame, with named-object parameters and string correlation IDs. Requests
require exactly `jsonrpc`, `id`, `method`, `params`; `jsonrpc` must be `"2.0"`. IDs are 1–64 printable
ASCII bytes, nonblank; method names are 1–64 ASCII letters/digits/dots/underscores, case-sensitive,
and cannot use the reserved `rpc.` prefix. Unknown fields, duplicate object keys, invalid UTF-8,
unpaired surrogate escapes, nonfinite numbers, top-level arrays and positional params are invalid.
No BOM or CRLF framing. A JSON string must escape its newline; it cannot split a frame.

Proposed limits: 1 MiB per complete frame including LF, nesting depth 32, 16 authenticated sockets
per administrator, 64 total, one executing request plus at most eight queued requests per socket.
A complete input frame has a five-second deadline from its first byte; incomplete/oversize frames
close the socket without parsing a truncated object. Idle subscription sockets need not send bytes.
All queued requests include their wait in the five-second server request deadline. Queue capacity
failure returns `capacity_exceeded` if bounded output space exists; otherwise close. No unbounded
allocation, goroutine per incoming frame or unbounded response queue. These limits are profile
constants initially; changing them requires an advertised protocol capability revision.

Mutations additionally carry `params.operation_id`, a canonical lowercase UUID distinct from the
JSON-RPC correlation ID. A retry may use a new correlation ID but must keep its operation ID and
identical logical payload. Correlation IDs cannot be reused while outstanding on the same socket.
Client notifications (missing ID) are not executed and receive no response; close with a bounded
operational diagnostic. Batches are rejected as a profile violation without executing elements.
This is an application profile, not unrestricted JSON-RPC conformance.

All domain 64-bit counters, versions and budgets are canonical nonnegative decimal **strings**:
`"0"` or a nonzero digit followed by digits, no sign/leading zeros/exponent/fraction, at most
9223372036854775807. Logical positivity rules still apply. Page limits are JSON integers 1–100;
UIDs are JSON integers in the platform UID range, rejecting reserved/unmapped identities.
Timestamps use UTC RFC3339 with optional fractional seconds up to nanoseconds and the representable
range required by connections.md. Missing optional values mean retain/default only where specified;
null is rejected unless a response field explicitly permits it. Never trim exact identifiers.

After kernel authentication, the first call within five seconds is `server.hello` with
`{"protocol":"parley-control/1"}`. Its response contains protocol, server/epoch/admin IDs, server
state (`running` or `recovery_only`), profile limits and supported method names. Unsupported
versions return `protocol_mismatch` then close; there is no silent downgrade. Every other request
before hello fails. Normal success has `jsonrpc`, echoed `id`, and `result`; failure has `jsonrpc`,
`id` (null only when unavailable) and `error`. Never include both result and error.

Server `state.changed` notifications omit ID and carry subscription ID and new view token fields
(epoch/revision), without bodies, credentials, private source paths or executable action text.
Output is serialized per socket: hello/subscription responses precede their notifications; no
interleaved JSON frames. Five-second write deadlines and bounded output queues disconnect a
stalled client; database work never waits for a network write.

## Method contracts

Every method below requires an administrator principal. Unsupported methods return method-not-found;
there is no generic action executor. Read operations omit operation IDs and are not command mutations.
`server.hello` is the only pre-negotiation method. No request carries an authoritative admin/peer role.

| Method | Named parameters | Result |
| --- | --- | --- |
| `server.hello` | `protocol` | Negotiated identity/capabilities/limits above |
| `state.snapshot` | Optional `cursor`, `limit` (20 default, 100 maximum) | `view`, `observed_at`, bounded typed `items`, nullable `next_cursor` |
| `state.subscribe` | `view` from snapshot | `subscription_id`, matching epoch/revision; begins ordered change hints |
| `state.unsubscribe` | `subscription_id` on this socket | `unsubscribed` boolean; repeat is a no-op for that socket's retired ID |
| `conversations.list` | Optional cursor/limit | Bounded conversation/member/grant metadata, view and next cursor |
| `conversation.get` | Exact `conversation` | Latest grant version/snapshot, nullable active grant, member online/readiness flags, or not-found |
| `pending.list` | Optional cursor/limit | Typed pending-human summaries, view and next cursor |
| `pending.get` | `kind`, immutable `item_id` | Current typed record, exact versions and permitted action names |
| `operation.get` | `operation_id` belonging to this admin | Recorded command receipt/terminal rejection, or `operation_not_found` |
| `audit.list` | Optional cursor/limit | Ordered safe audit records and next cursor; no live subscription implied |
| `membership.enroll` | Command fields below; expected latest version, full members/policy, positive maximum, optional future expiry | New grant receipt and bounded lifecycle summary |
| `membership.renew` | Command fields; expected active version, optional maximum/expiry, `cancel_pending_replies` (false default) | Successor grant receipt and lifecycle summary |
| `membership.replace` | Renew fields plus complete members/policy | Successor grant receipt; no implicit membership inference |
| `membership.revoke` | Command fields; expected active version | Revoked version receipt, queued-cancellation/in-flight counts |
| `admission.approve`, `admission.reject`, `admission.cancel` | Exact IDs, expected versions and membership/limits from connections.md, plus operation ID | Request/grant receipt under its atomic admission contract |
| `binding.register`, `binding.rotate`, `binding.reenroll`, `binding.revoke`, `binding.retire` | Exact connection-contract fields plus operation ID | Binding/credential metadata and incident references; never a secret |
| `connection.disconnect`, `hold.disposition`, `ingestion.resume`, `recovery.complete` | Exact versioned fields from connections.md plus operation ID | Audited target-specific receipt; no generic execution |
| `legacy.disposition`, `clock.reconcile` | Exact incident/version fields below plus operation ID | Audited quarantine/clock recovery without invented credentials or renewed deadlines |
| `provisioning.status` | Own committed `operation_id` | Recorded publication evidence/status; no secret or arbitrary file read |

Membership command fields are `operation_id`, exact `conversation` and
`expected_grant_version`. Enroll uses zero only when no history exists; all other methods require
a positive exact current version. Members/policy use membership.md's members-shaped schema;
there are no positional peer fields on the wire. Initial runtime supports exactly two `member`
roles and `open` or one-edge `directed`, rejecting room policies/topologies explicitly. Enroll
requires both bindings currently enabled and an explicit human-reviewed pair. It cannot consume
pending admission by naming its conversation: a waiting request must use `admission.approve`.
Re-enrolling an existing historical conversation uses expected latest version and does not bypass
binding/security/recovery checks. Maximum/expiry/renewal and reply-carry behavior follow membership.md.

### Connection-operation wire fields

These mappings fix JSON field names for connections.md's logical records. Every mutation includes
`operation_id`; every listed expected version is a positive decimal string. IDs use the connection
contract's UUID/exact-key rules; unknown or extraneous fields fail rather than being ignored.

| Method | Additional fields |
| --- | --- |
| `admission.approve` | `pending_id`, `join_id`, `expected_pending_version`, `expected_join_version`, `expected_bindings` (exactly two binding/credential-version tuples), `members`, `policy`, `max_exchanges`, optional `expires_at` |
| `admission.reject` | `join_id`, `expected_join_version` |
| `admission.cancel` | `target` tagged as `pending` or `join`, with `id` and `expected_version` |
| `binding.register` | `peer_id`, `host_kind` (`claude_code` or `codex_cli`), `host_namespace_id`, `host_session_id`, `connector_uid`, `expires_at`, `provisioning_target_ref` |
| `binding.rotate` | `binding_id`, `expected_binding_version`, `expected_credential_version`, `expires_at`, `provisioning_target_ref` |
| `binding.reenroll` | Rotate fields plus `host_evidence_ref` |
| `binding.revoke`, `binding.retire` | `binding_id`, `expected_binding_version`, `expected_credential_version` |
| `connection.disconnect` | `binding_id`, `expected_server_epoch`, `expected_connection_generation` |
| `hold.disposition` | `work` tagged as `pending`, `join` or `envelope` with `id`; `incident_id`, `expected_hold_version`, `action` (`cancel` or `release`), `reason` |
| `ingestion.resume` | `binding_id`, `expected_binding_version`, `expected_barrier_version`, `disposition_ref` |
| `recovery.complete` | `incident_id`, `expected_recovery_version`, `disposition_ref` |
| `legacy.disposition` | `migration_incident_id`, `work_id`, `expected_quarantine_version`, `action` (`cancel` or `release`), `disposition_ref` |
| `clock.reconcile` | `incident_id`, `expected_clock_version`, `time_evidence_ref` |

Each expected-binding tuple contains `binding_id`, `expected_binding_version` and
`expected_credential_version`; array order has no authorization meaning. A `reason` is an object
with `code` (`owner_reviewed`, `compromise`, `retirement`, `restore_reconciled` or `cancelled`) and
optional validated `note`. The chosen action and current state constrain which reason is valid;
reason text never supplies authority or overrides a hold.

References are immutable UUIDs for trusted, versioned evidence/configuration records, not paths
or proof by possession. Provisioning targets come from human-owned setup configuration and bind
an allowed destination/account. Host evidence resolves through the verified host integration.
A time-evidence reference resolves a reviewed clock-source/floor record for the exact clock
incident. A disposition reference resolves an already human-reviewed manifest bound to exact incident,
principal/binding and expected versions, including the reviewed source interval/cursor/held events
or restore reconciliation from connections.md. A missing, stale, mismatched or unverified reference
fails closed. The initial trusted manifest-preparation procedure is a stopped-service human step:
store an immutable, owner-only file under a configured recovery directory, containing its UUID,
expected versions, bounded resource references and SHA-256 digest; never accept an agent-provided
pathname. On startup, validate ownership, regular-file type, no symlinks and a 1 MiB limit before
registering it for inspection. Oversized manifests are rejected; separately reviewed bounded item
dispositions may accumulate under the same incident while the global recovery hold remains. Final
completion must reference audited reconciliation and every remaining disabled namespace/hold;
size limits never authorize partial recovery, truncation or blanket release. Matching a manifest does
not dispense with the operation's current writer-transaction checks or audited disposition.

Pending kinds are `admission`, `hold`, `legacy_quarantine`, `clock_hold`, `ingestion_barrier`,
`recovery` and `uncertain_delivery`.
Action names are an allowlisted enumeration derived from current state. Uncertain-delivery items
are observational here: no new retry/ACK/inbox-disposition operation is authorized. The client must
construct only a named schema-bound operation after human selection, never execute an operation
blob from a message or item. Retained-context authoring and historical inbox actions are separate.

Snapshot/list projections exclude message bodies and credential material. Conversation entries
contain exact name, latest grant version/snapshot, nullable active grant and current member
presence/readiness; pending entries contain kind, immutable ID, relevant versions, safe summary
and action enumeration. Event/audit references never embed an unrestricted record. Limit each
projected item to 8 KiB; a page of 100 items plus fixed metadata fits the frame bound. Oversized
legacy identifiers/records produce an explicit compatibility error, never lossy truncation or
silent omission. Live grants remain a first-stage pair; pagination of future room members is not
introduced implicitly.

A command receipt contains `operation_id`, fixed outcome, affected exact IDs, resulting versions,
`audit_id` and `commit_view` (epoch/revision only, not a signed snapshot token); bounded counts
summarize affected queues. Exhaustive per-envelope mutation exports are outside the first profile;
receipts do not embed unbounded lists or imply that current inspection reproduces historical effects. Receipt
references identify committed history and do not assert that a grant/request is still current.
No caller-provided reason is treated as an instruction; audit reasons use fixed codes and a
validated optional 512-byte plain-text human note, with the connection-purpose character rules.

## Snapshot and subscription consistency

A view token contains random `server_epoch`, decimal-string `view_revision`, fixed scope, five-minute
expiry and a server MAC bound to the administrator principal. The `proof` is unpadded base64url
HMAC-SHA-256 over the canonical typed token fields, scope and principal ID, using a fresh 32-byte
cryptographically random process key. It is not an authorization credential.
Snapshot cursors additionally bind collection and last `(kind,id)` key to that same view. Ordering
uses exact identifier bytes. Tokens cannot be transferred between principals, scopes or epochs.
One subscription per socket is supported initially; creating another conflicts until unsubscribe.

The server uses a short process coordinator gate for state publication, not network I/O. Every
durable state mutation, presence/readiness transition and effective deadline transition must
publish through it. The gate is acquired **before** a writer transaction; never acquire it while
already holding the writer. Bump the process view revision only for a committed state change or
published runtime transition. Include every field visible in the administrative projections.
Read queries and their diagnostics do not bump it or append command audit; otherwise observation
would invalidate itself. Counter overflow stops ordinary service instead of wrapping a token.

A snapshot page obtains a reader connection first, within its five-second total deadline, then:

1. Acquire the coordinator gate with the same deadline. Process due visibility/expiry transitions
   before selecting a revision. Housekeeping writes use separate writer transactions; the query
   itself remains read-only. A bounded overdue-work batch that cannot catch up returns
   `temporarily_unavailable`; never certify a revision that omits an effective deadline change.
2. Begin the deferred read transaction and perform a real read of the installation row to fix the
   SQLite snapshot while publication is excluded. Copy the bounded runtime projection needed for
   this page and capture epoch/revision/observation time. For a later page, require its cursor's
   still-current revision and scope before copying. Do not clone an unbounded runtime registry.
3. Release the coordinator gate. Materialize at most the requested page plus one lookahead row
   from that SQLite snapshot, using the captured runtime data/time. Close rows, transaction and
   reader before producing output. New writer commits may run during this read under WAL.
4. Sign the page token/cursor for the captured revision and original snapshot expiry. Later pages
   do not extend expiry. A subsequent change may already have made this page stale; the subscription
   check and current authorization checks must detect that, not relabel the page as newer.

The implementation must determine page keys and their bounded runtime projection during the fixed
snapshot/gate phase; only the remaining bounded SQL materialization occurs after release. This
avoids pairing old membership with a newly connected peer. All waits include pool/gate acquisition
in the read deadline; no gate acquisition may occur while waiting on client I/O. The peer-facing
query surface does not inherit administrative visibility from these projections.

A client assembles all required pages of one view before claiming a complete snapshot. Any page
whose cursor revision has changed returns `resnapshot_required`; discard the partial assembly.
Separate list calls carry their own view tokens and cannot be silently combined into one coherent
snapshot unless the tokens match. `audit.list` instead uses durable monotonic audit IDs: a bounded
historical page is not an administrative live snapshot.

Subscription registration acquires the coordinator gate, processes due transitions, rechecks admin
identity/token expiry/MAC and requires the snapshot's exact epoch/revision to remain current. It
registers a bounded output queue and places the success response ahead of future hints before
releasing the gate. Changes after registration enqueue in revision order. A change committed in
the snapshot/subscribe gap instead returns `resnapshot_required`, with no active subscription.
Registration can reserve bounded queue capacity, but cannot block on the socket writer.

During an active subscription the client can refresh snapshot pages. It tracks the greatest seen
revision, never replaces newer state with an older snapshot response, and keeps an explicit dirty
or unsynchronized indicator until a full current view is assembled. Hint revisions must increase;
regression, wrong epoch, missing expected continuity, malformed notification or disconnect means
resnapshot. The server sends one hint per published view revision, not a silently coalesced stream.
All client output shares FIFO ordering, with at most 256 queued hints and 4 MiB total queued bytes.
Overflow closes the connection; best-effort diagnostic is optional, never required for correctness.

Reconnect starts a new snapshot/subscription; hints are not replayed across connections. Ordinary
restart creates a new epoch and discards process view revisions/subscriptions, while preserving
application state, command results, audit and recovery holds. No notification journal is promised.
After three failed snapshot attempts within one interaction, show unsynchronized status and retry
with a one-second minimum delay; sustained churn may prevent convergence but cannot hide a gap.

## Command atomicity, idempotency and audit

Use the stable configured administrator UUID as the operation principal, not UID text, socket ID
or agent binding. The connection contract's lifetime replay retention and canonical request digest
apply. The digest includes operation kind and validated logical payload, excluding correlation ID
and bearer secrets. Schema defaults are resolved consistently before hashing. Preserve exact IDs,
retain absent/null distinctions where valid, and canonicalize only declared unordered fields.
The same operation ID with different content or method returns `operation_conflict`.

For a new valid command, acquire the coordinator gate and one immediate writer transaction.
Recheck current admin capability, recovery mode, all relevant membership/binding/credential/item
versions, expiry and every security, migration-quarantine and global recovery/clock hold. No read-pool snapshot authorizes a write. Write the mutation, permanent
operation receipt and audit row in that transaction, reserving its next view revision. On commit,
publish the corresponding revision/hint before releasing the gate; response I/O follows release.
A failed commit publishes no successful receipt or revision. A crash after commit but before
publication forces a new epoch on restart and resnapshot; the durable result remains authoritative.

Audit schema: immutable audit UUID plus positive monotonically allocated `audit_sequence`, trusted
admin UUID and observed connector UID, operation ID/type, canonical digest, server time, affected
IDs, prior/resulting versions, fixed outcome/reason, and commit epoch/revision. Unique operation
keys prevent duplicate mutation audit. A terminal authenticated domain rejection records its
receipt/audit without the prohibited state change; this audit insertion also publishes a revision.
Replayed commands return existing receipts and do not add audit rows or refresh budgets.
Malformed framing, missing/invalid operation IDs and unauthenticated calls cannot become attributed
commands; use bounded diagnostics, not fabricated audit principals. Read queries are not mutations.
Audit/result write failure rolls back the effect; storage exhaustion rejects new commands.

Same-ID retries first authenticate and authorize access to the old result, then return it before
reevaluating now-stale mutation preconditions. A rejection receipt is terminal for that ID too;
a corrected request needs a new operation ID and explicit fresh human intent. `operation.get`
returns `operation_not_found` only for that admin's absent record; absence during an in-flight
request is not proof of failure. Retry the same ID, never invent a second command after a lost
response. Unknown commit outcome is `outcome_unknown`; never state that the mutation did not run.

Credential-file publication cannot be atomic with SQLite. Registration/rotation commits its stable
credential-created receipt without secret material first, then the trusted publisher performs file
I/O outside the gate/transaction. Publication evidence is recorded separately and queried with
`provisioning.status`; a successful command receipt alone does not claim file publication success.
Ambiguous publication requires human rotation/revocation, never replay of the secret. Publish to a
new credential-ID-specific file without overwriting another version or updating a shared mutable
active-file pointer. The client displays that recovery requirement explicitly. These operations
retain the connection contract's current-version checks, holds and no-membership side effect.

Connection disconnect also has a runtime effect: record its exact epoch/generation target and
receipt/audit, then close that slot before releasing the coordinator gate. Other requests recheck
the retired token under the same gate. A crash between commit and close ends all sockets anyway;
replay cannot target a later generation. Reconnect still requires the connection contract's CAS.
The same publication discipline applies to all runtime state invalidated by durable administration.
Host delivery remains outside SQLite: independent exact-attempt settlement and uncertainty rules
are preserved, and an audit record never asserts exactly-once host execution.

## Error contract and traces

JSON-RPC envelope failures use `-32700` (parse), `-32600` (profile/envelope), `-32601` (method),
`-32602` (params), `-32603` (unexpected internal failure). Domain errors use `-32000` with a fixed
`error.data.code` and safe summary. A server cannot disclose credential IDs, paths, bodies or raw
transport errors in any error. A valid notification receives no error response, as specified above.
An unreadable/oversize/partial frame may be closed without a response or echoed ID.

| Domain code | Consequence |
| --- | --- |
| `protocol_mismatch`, `forbidden` | No operation; failed authentication closes before exposing capabilities |
| `resnapshot_required` | No subscription/consistent next page; refresh, do not reuse a stale view |
| `subscription_conflict`, `capacity_exceeded`, `temporarily_unavailable` | Bounded failure; no silent eviction or dropped replay records |
| `operation_conflict`, `operation_not_found`, `outcome_unknown` | Follow durable result rules; never retry under a new ID automatically |
| `stale_grant_version`, `version_conflict`, `request_expired`, `request_terminal` | No retargeted command effect; inspect current state before new human action |
| `invalid_membership`, `unsupported_membership`, `incompatible_identifier` | Explicit contract/compatibility rejection; no lossy translation |
| `security_hold`, `recovery_required`, `binding_unavailable`, `host_unverified` | Preserve holds and account/host boundaries; ordinary retry cannot authorize recovery |

Other domain codes are the explicit membership/connection error enumerations, not arbitrary strings.
Permission checks precede private lookup diagnostics. Expose expected/current versions only to a
principal allowed to inspect them. Cancellation before commit rolls back; cancellation after commit
may prevent its response but cannot delete its receipt/audit. Client timeouts never prove failure.

Synthetic read trace (these are separate LF-terminated frames, never a CLI grant invocation):

```json
{"jsonrpc":"2.0","id":"s1","method":"state.snapshot","params":{"limit":20}}
{"jsonrpc":"2.0","id":"s1","result":{"view":{"server_epoch":"fixture-epoch","view_revision":"41","scope":"admin","expires_at":"2026-09-11T16:05:00Z","proof":"synthetic-mac"},"observed_at":"2026-09-11T16:00:00Z","items":[],"next_cursor":null}}
{"jsonrpc":"2.0","id":"w1","method":"state.subscribe","params":{"view":{"server_epoch":"fixture-epoch","view_revision":"41","scope":"admin","expires_at":"2026-09-11T16:05:00Z","proof":"synthetic-mac"}}}
{"jsonrpc":"2.0","id":"w1","error":{"code":-32000,"message":"State changed; refresh the snapshot.","data":{"code":"resnapshot_required"}}}
```

The epoch/proof placeholders are deliberately non-authentic test notation. The trace starts after
successful hello; another change published revision 42 between the first response and subscribe.
For a duplicate mutation trace, the fixture drops the first response after commit, reconnects and
repeats the same operation ID/payload with a new correlation ID: it must receive the original
receipt/audit ID and observe one grant version/budget effect. Changing its expected version under
that operation ID conflicts rather than silently becoming a new command.

## Writer, readers and service ownership

Keep exactly one store-owned immediate writer connection and a distinct pool of at most four
read-only deferred readers. Do not raise the existing writer's pool ceiling to obtain read
parallelism. Mutation authorization, result/audit and budget claim reads stay in the writer
transaction. Expose separate write and pure-query interfaces so a reader cannot administer state.

Preserve `databaseDSN` as the writer constructor. Add an internal sibling `readerDSN`, sharing path
normalization and using `mode=ro`, `_txlock=deferred`, `_query_only=on`, `_busy_timeout=5000` and the
store-owned foreign-key settings. Keep both VFS read-only mode and connection query-only pragma.
Preserve the external option allowlist; callers cannot override locking through writer DSN aliases.
Readers must not set journal mode, create databases, migrate or run recovery. Missing DB fails.
Open readers only after writer initialization, ordered migrations and recovery-state establishment.
No online migration is supported. Library memory/URI test support is not server configuration.

Pure scans/snapshots include pool acquisition, coordinator waits, query and materialization in a
five-second deadline. Close rows/transactions/connections before network I/O, subscriptions or host
calls. Use keyset indexes and bounded projection; never hold a SQLite snapshot across pages or
subscriber waits. WAL allows bounded readers while the writer commits; a stalled client cannot
hold a WAL checkpoint hostage through an open read transaction. Busy timeout and migration retries
still handle legitimate SQLite contention within caller deadlines.

Server startup accepts an ordinary existing filesystem database path only. Reject `:memory:` and
URI connection strings before lock/open. Resolve the existing file and all symlink parents to a canonical absolute path under
trusted ownership; require a regular local file with one hard link, rejecting alternate hard-link
aliases. Paths or files replaceable by agents fail validation. The human controls setup/relocation;
the server never silently creates an empty replacement DB if its configured path is missing.
Initial database creation is a separate explicit human initialization action using the same
ownership lock and schema checks, before ordinary service startup, not an automatic fallback.

Acquire `LOCK_EX|LOCK_NB` flock on `<canonical-db-path>.lock` before `store.Open`, migrations or
listeners. Open the persistent regular lock file with close-on-exec and no symlink following;
validate its owner/permissions/link count and trusted parent. Never unlink it on release. Hold its
descriptor until workers and both pools stop. A contender returns `already_running` before either
SQLite busy retry path or any call to `store.Open`. Locking is cooperative; an untrusted process
with DB-directory write access violates the deployment boundary and is not defeated by flock.
Do not support shared network-filesystem or simultaneous restored-copy operation as local ownership.

After locking: open writer, validate/adopt/upgrade known schema atomically, establish recovery mode,
apply interrupted-dispatch recovery as permitted by that mode, then open readers and finally
publish listeners. Ordinary requests cannot run during initialization. With an external restore or clock
hold, only the explicitly limited human recovery inspection/disposition surface becomes available;
agent admission/dispatch/ingestion remain blocked. Define cleanup in reverse order on every failure.
Preserve frozen adoption SQL and choose ordered migration versions when implementation lands.

For stale socket paths after a crash, replace a socket only after obtaining DB ownership and
verifying the configured path is a trusted socket owned by the expected server account. A live
socket, regular file, symlink or unexpected owner fails startup; never unlink arbitrary content.
Only a bounded connection probe returning definite connection-refused permits stale-socket
replacement; timeout, permission failure or another error does not establish abandonment.
Shutdown stops admission, closes subscriptions/readiness and cancels workers, then drains/records
in-flight outcomes under existing independent settlement contexts, closes readers/writer, and only
then releases the ownership descriptor. A forced termination leaves interrupted claims uncertain;
restart never automatically retries them. Descriptor noninheritance and crash release need
controlled child-process tests, not assumptions about CLOEXEC preventing all explicit inheritance.

## Backup, relocation and restore

A human stops all writers and verifies ownership is released before backup/relocation. Preserve a
consistent database with any required WAL/SHM state and ownership metadata. A confirmed successful
checkpoint and clean close can yield a self-contained DB; copying just the main file while
committed data remains in WAL is invalid. Never delete WAL/SHM merely because they look temporary.
Keep the old deployment stopped while moving/copying state and updating explicit server/client
configuration; verify the intended installation ID and file inventory before reopening service.

Any restore, including a rollback during relocation, establishes the connection contract's
external recovery marker before startup. Reconcile grants/budgets/envelopes/attempts, bindings,
credential/revocation/provenance/holds, legacy quarantine, clock incidents/markers, source
cursors/deduplication, command results/tombstones and
audit history against surviving evidence. A new epoch invalidates views but cannot restore lost
external effects or authorize replay. Retain disabled namespaces/held or uncertain work when
evidence is incomplete; complete recovery only by the audited human procedure. A second restored
copy must never run concurrently as the same installation. No automated destructive recovery,
production account provisioning or permission change is part of this specification.

## Required controlled fixtures

These are implementation requirements, not tests claimed to exist. Use synthetic admin/peer
identities and credentials, temporary local file-backed WAL databases, injected clocks/sockets and
controlled subprocesses. No host CLI, real credentials or protected-controller invocation.

| Fixture | Required evidence |
| --- | --- |
| P01 Framing/profile | Exact/over-limit frame, partial EOF, slow frame, invalid UTF-8/surrogate, duplicate keys, depth, arrays, missing ID and ID collision produce bounded rejection with no mutation |
| P02 Authority | Wrong UID/server path, agent credential on admin socket, payload role forgery, unsupported method and retired admin identity cannot administer; same-admin-account limitation is demonstrated |
| P03 Command retry | Drop response after commit; reconnect/same operation yields same receipt/audit and one version/budget effect; conflicting payload and recorded rejection remain terminal |
| P04 Stale human action | Admission, renewal/revoke and binding/hold/recovery version races cannot retarget an action or partially mutate state |
| P05 Snapshot gap | Commit or runtime transition between snapshot and subscription returns resnapshot-required; change after registration appears after success response |
| P06 Mixed state/paging | Coordinated SQL snapshot and copied runtime state agree; stale/foreign/expired cursor rejects; later pages cannot renew a view or mix revisions |
| P07 Deadline visibility | Grant/request/credential expiry and liveness timeout before subscribe invalidate old views; overdue housekeeping cannot certify stale state |
| P08 Slow observer | Queue/hint/byte overflow and stalled socket close without blocking writer/dispatch or holding DB resources; client exposes unsynchronized state |
| P09 Reconnect/churn | New epoch, missing/regressing hint, repeated page conflicts and dropped connection require fresh view; no false convergence or notification replay promise |
| P10 Audit/storage failure | Inject before/after commit and audit/result failure; mutation atomicity and unknown-outcome behavior hold; read queries do not invalidate snapshots |
| P11 Runtime/publication effects | Credential receipt vs failed/ambiguous file publication is explicit; old publication cannot overwrite newer files; disconnect replay cannot close a later generation |
| P12 Reader isolation | Four deferred readers can read during writer activity; every mutation attempt on reader fails; caller cannot override DSN locks/query-only; missing DB is not created |
| P13 Read deadlines | Pool/gate acquisition cancellation, stalled query and interrupted materialization release every row/transaction; no resource retained during output |
| P14 Startup contention | First process paused before/during migration; second fails flock without calling store.Open; crash release, alias/hard-link rejection and descriptor noninheritance verified |
| P15 Startup/shutdown failure | Inject failures at lock, writer, migration, recovery, reader and listener; no early admission; reverse cleanup and conservative interrupted dispatch |
| P16 Paths/configuration | Reject missing/untrusted/URI/memory DB and invalid endpoint; explicit initialization only; help/validation need no server; no PARLEY_DB client fallback |
| P17 Backup/restore | WAL-dependent snapshot, stale copied installation, lost audit/results and response loss during recovery stay held until human disposition; no replay or budget replenishment |
| P18 Membership projection | Two-peer members/policy round-trip exactly, unsupported room shape rejects, oversized legacy data fails explicitly and no generic uncertain-delivery action appears |
| P19 Legacy/clock recovery | Nonempty legacy provenance stores without fake FKs; wrong-version dispositions fail; clock rollback marker survives restart/DB-write failure; replayed clock reconciliation cannot clear another hold |

Fixture review must cover success, rejection, cancellation and recovery at the exact implementation
head. The repository's full live-connection gate applies separately before connecting real hosts.

## Evidence basis

Baseline `c7061e0b36d213f939a6989b0893e48640f9d650`: the current
[CLI factory](../../cmd/parleyctl/main.go) calls store.Open directly; the
[store](../../internal/store/store.go) has one pool with immediate transactions; the
[migration runner](../../internal/store/migrations.go) owns ordered atomic upgrades;
[dispatch](../../internal/dispatch/dispatch.go) separates claim, host handoff and independent
settlement. This target contract does not assert a running control server exists.

[JSON-RPC 2.0](https://www.jsonrpc.org/specification) supplies the envelope baseline; Parley's
framing, restricted profile, authority and replay semantics are additional contracts.
[SQLite WAL](https://sqlite.org/wal.html) explains reader/writer concurrency and WAL-dependent
backup state. Linux [flock](https://man7.org/linux/man-pages/man2/flock.2.html) defines cooperative
lock/descriptor semantics; deployment must enforce the separate filesystem ownership boundary.

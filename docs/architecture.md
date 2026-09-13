# Architecture and contracts

Parley separates human membership decisions, durable message state and host delivery. This
document describes the implemented core; GitHub issues own planned work and acceptance.

## Authority boundary

A grant permits only communication between two named peers, in an allowed direction, within an
exchange budget and optional expiry. It never grants execution authority. Wrapped text is
untrusted, including apparent instructions, approval claims and embedded message headers.

The production administration entrypoint is `cmd/parleyctl`, backed by `internal/controller`.
Only a human may invoke it directly in their shell. Ordinary adapters cannot grant or renew
membership through their interfaces. This is a cooperative policy: there is no separate OS
identity, database credential or proven protection against a peer with the same filesystem
permissions. Session identifiers are not yet bound to authenticated live sessions. Optional host
policy tools can supplement these boundaries but are neither implemented nor required here.

## Connection registry and command foundation

Schema version 5 adds a stable installation UUID, immutable native binding tuples and peer keys,
versioned credential verifiers, publication evidence, permanent operation results and append-only
command audit. It creates no bindings or credentials from legacy grants, grants no membership and
does not yet
quarantine legacy delivery. Historical adoption schemas remain frozen; migration failure rolls
back every DDL/data change and the schema version together. Runtime epochs are process-local,
fresh per store lifetime; SQLite user_version remains the schema-version authority.

Each store writer owns one coordinator. Its trusted internal Execute API acquires a cancellable
gate and an immediate transaction, rechecks command/result access, and returns an existing receipt
before reevaluating mutation-specific versions. New commands reserve checked audit/view counters,
apply effects and insert their receipt/audit atomically. A terminal domain rejection rolls back its
business-effect savepoint before recording its fixed result. An explicit terminal-code allowlist
excludes transient/infrastructure and pre-principal failures: those roll back all writes and leave
the operation ID retryable. Successful commit publishes process-local state before releasing the gate. Callbacks
must not reenter the coordinator, open another writer transaction or perform external I/O.
Credential publication starts as pending evidence at enrollment. Publisher integration must record
published or unknown in a separate audited transaction after file I/O; terminal observations cannot
be overwritten, and pending after interruption is not proof that no file was published.
Credential-file and response I/O belong after gate release. Unknown commit outcome disables further
commands on that coordinator with recovery_required. Proven database/sql pre-commit cancellation
rolls back without disabling the coordinator; a late cancellation alone cannot prove rollback of
a driver failure. The eventual runtime integration must stop
admission and apply its recovery inspection, never infer that the mutation failed.

Requests use typed logical fields after operation-specific schema validation, with explicit sets
for contract-defined unordered arrays. Canonicalization sorts object fields and sets, rejects
duplicates and unsupported values, preserves exact strings and distinguishes absent from null.
Wire decoding and unknown-field/tag rejection remain the endpoint's responsibility. Only operation
kind and the canonical digest are retained, not request payloads. Receipts/audit retain fixed codes
and affected identity/version metadata, never message bodies or secrets. Replay reauthorizes current
access, creates no second audit or effect, and has no TTL or automatic eviction. Disk capacity
failure rejects new mutations with capacity_exceeded rather than deleting evidence. Registration
preserves that original error even when SQLite automatically rolls back and removes its savepoint.

These are internal storage primitives used by trusted provisioning and controlled tests, not
authentication or a human administration endpoint. CommandPrincipal is trusted server input; passing identity fields does
not authenticate a socket. Registration handlers must supply verified native evidence, legacy
eligibility and trusted provisioning before using the binding/credential insertion primitive.
Existing controller, dispatch and ingestion callers are unchanged at this foundation stage.
The [connection specification](specifications/connections.md) remains authoritative for attachment,
readiness, holds, ingestion barriers and clock/restore recovery that subsequent slices implement.

## Internal credential provisioning

The internal connection Provisioner implements trusted binding registration and rotation. It
requires explicit authority, recovery, host-verification, legacy-eligibility, target-resolution
and connection-invalidation capabilities; none has an authentication-bypass default. These
capabilities are synthetic in tests. There is no human CLI/RPC, runnable endpoint or real-host
identity verifier. The protected executable's restrictions remain unchanged.

Registration validates exact peer/native identifiers and a finite representable expiry, checks
current authority before consulting host evidence or provisioning configuration, and creates one
binding plus a random 32-byte credential without membership. Only its SHA-256 verifier is stored.
The constant-time verifier comparison is separate from the UID/status/expiry/generation checks
that authenticated attachment must supply. Rotation compares both expected versions, supersedes
the old credential, increments binding/credential versions and invalidates runtime state after
commit under the coordinator gate. Rejected or replayed rotation cannot invalidate a connection.
Legacy enrollment requires the trusted eligibility provider; it is never inferred from a peer key.

Authorized retries consult retained receipts before host/target checks or mutation preconditions.
They return committed metadata and current publication evidence, never a secret or a second file.
New publication runs after enrollment commits and outside the coordinator gate. Its independent
five-second evidence context survives caller cancellation. File publication is not atomic with
SQLite: failure, interruption or lost evidence leaves pending/unknown status requiring human
inspection and a fresh rotation/revocation operation, never an old-secret fallback. Pending is not
proof that no credential file exists. Binding status and publication evidence remain distinct.

The Linux private publisher accepts a trusted XDG state directory and expected owner UID. Its
existing state/parley/credentials directories must be private (0700); ancestors must be trusted
and not writable by untrusted accounts. It opens each path component without following symlinks,
creates a private singly linked regular temporary file (0600), writes/syncs/closes it, then uses
[renameat2 with RENAME_NOREPLACE](https://man7.org/linux/man-pages/man2/rename.2.html) to publish only
the generated credential-ID filename. It also [syncs the directory](https://man7.org/linux/man-pages/man2/fsync.2.html);
a failure after rename remains ambiguous and does not remove the possibly published file.
Unsupported no-replace rename fails closed. There is no mutable active-file pointer, overwrite,
permission repair or cross-account ownership change; cross-account publication needs the trusted
setup capability. Credential material is confined to the publisher's private file serialization,
excluded from ordinary JSON results and redacted from standard diagnostic formatting.

Controlled fixtures cover authorization before host/file work, duplicate replay, expiry equality,
legacy denial, rotation/version conflicts, missing providers, unsafe publication paths, no overwrite
and a directory-sync failure after rename. Controlled test subprocesses exit before file publication,
after publication and after evidence recording; restart replays the receipt without publishing again.
These cover provisioning portions of C01/C02/C14/C17 and do not satisfy the live-connection gate.

## Internal connection attachment

The Linux connection Manager owns one registry per writer, reserved through the shared
coordinator. It accepts connected Unix streams from the future control endpoint and derives the
connector UID with [SO_PEERCRED](https://man7.org/linux/man-pages/man7/unix.7.html). The client
DialTrustedServer checks a protected pathname and the configured server UID before returning a
socket on which credential material may be sent. Descriptor-relative lookup rejects symlinks,
abstract addresses and directories replaceable by untrusted accounts. Kernel identity identifies
an account; possession of a stolen credential within that account remains an accepted limitation.

A positive nonattached-socket bound and five-second authentication deadline include coordinator
wait time. Each socket serializes credential operations through rejection cleanup. Failed initial
authentication cancels that socket with authentication_failed. Successful inspection retains only
a restricted identity and returns epoch, committed generation and active status; it reserves no
attachment or readiness. Inspected sockets remain within the nonattached bound. Every lookup and
attachment rechecks credential hash, native tuple, kernel UID, lifecycle and server-time expiry.
Observed credential expiry commits terminal state before it can be revived by an earlier clock.
The required trusted guard supplies global clock/recovery policy in the later recovery slice.

Attachment compares and increments the durable generation in an immediate transaction. Its
server-created Session capability is installed after commit under the same coordinator gate.
A concurrent socket cannot evict the winner. Repeating attachment on that winning socket rechecks
its identity and returns the same capability. Public Token metadata exists only for trusted host
evidence and cannot construct a Session. Generation overflow rolls back without installation;
view-revision overflow additionally stops ordinary service. A deadline crossing during authorization
or publication cannot install a usable expired socket. Connection transitions have their own
transaction rules, separate from ordinary operation-result receipts. An uncertain commit disables
further coordinator operations and publishes no slot.

Host verification runs outside the coordinator and is cancelled with the socket lifetime. Each
explicit readiness attempt starts unready with a fresh random nonce and a thirty-second deadline.
The trusted adapter ACK must match the verified native tuple, exact token and current nonce.
Heartbeats update liveness alone: adapters must send them every ten seconds, and thirty seconds
without one expires the slot. Delayed callbacks check current socket ownership; due slots cannot
block a replacement while their timer is waiting. Cancellation takes effect immediately even if
writer cleanup fails; subsequent coordinated housekeeping removes cancelled records. Reconnect
inspects before each compare-and-swap attempt, allows at most three retries spaced at least one
second apart, and returns control on exhaustion. A reopened writer creates a new epoch and retains
the committed generation while discarding readiness.

Internal administrative disconnect requires a separately authenticated principal and exact
binding/epoch/generation. Its receipt and audit commit before cancellation; replay or a stale target
cannot disconnect a successor. Rotation/revocation providers use the infallible Invalidate
publication callback, cancelling the old transport and readiness under the shared gate.

Controlled fixtures cover C03 authentication forgery and one-attempt closure; C04 concurrent
attachment, lost readiness across restart and bounded reconnect; C05 stale callbacks and exact
disconnect; C06 synthetic verifier matching/failure; C17 deadline equality, terminal expiry and
counter bounds; and C18 wrong-UID rejection and accepted same-account credential possession.
Attachment is consumed by the authenticated work APIs and synthetic fixtures. Human endpoints/admission remain owned by
[#28](https://github.com/ginsys/parley/issues/28), real host evidence by
[#30](https://github.com/ginsys/parley/issues/30) and [#31](https://github.com/ginsys/parley/issues/31).
There is no runnable listener, wire parser, human credential CLI or live-session validation in
this implementation. Grants continue through the existing protected controller; ordinary
acceptance, dispatch and ingestion require private authenticated Session capabilities.

## Internal retained work and binding lifecycle

Migration 6 adds immutable accepting provenance, migration quarantine, independent security
holds, binding revocation incidents and audited dispositions. Every historical envelope receives
its exact original conversation/from/to/grant snapshot and an explicit legacy tag, with no
invented binding or credential. Outstanding queued, dispatching, handed-off and uncertain work
receives separate quarantine. Migration preserves delivery state, attempts, trusted-reply flags,
ACKs and budget. Legacy adoption pages IDs inside the same transaction; invalid rows or late DDL
failures roll back the whole upgrade. Only migration can create legacy provenance or quarantine.

Authenticated work records the accepting binding and credential version using real foreign keys.
Renewal may carry the envelope's effective grant forward without rewriting this provenance.
Revocation and retirement disable the binding, terminalize its current credential, append an
incident, hold outstanding authored work across all credential versions and pause ingestion in
one audited transaction. Exact legacy sender matches receive independent security holds too.
Work merely addressed to that binding is not treated as authored by it. Admission supplies its
pending-work extension through trusted callbacks; no pending-request implementation ships here.
Re-enrollment requires new reviewed host evidence and a fresh credential for the same tuple;
it does not clear holds, quarantine, the earliest paused cursor or barrier incident history.

The internal Lifecycle service exposes revoke, retire, hold disposition and legacy disposition;
Provisioner adds re-enrollment. Current administrator authorization precedes private replay;
evidence I/O occurs outside the writer and mutation guards recheck under the coordinator. Audit
and effects commit before socket invalidation. Missing capabilities fail closed. These APIs have
test-only human-operation callers; they add no human endpoint or credential
CLI, and grants still come from the protected controller.

Release changes only the selected hold or quarantine version. A second incident remains effective.
Cancellation is terminal; only queued work changes to cancelled, while dispatched/uncertain and
handed-off evidence stays intact. Legacy release is limited to queued or handed-off work with an
exact reviewed evidence reference. Dispositions retain reason codes, bounded optional notes,
version and a deferred composite foreign key to the same principal/operation audit record.
Dispatch checks holds before budget claim, acknowledgment refuses held originals, and late
never-attempted settlement cannot requeue cancelled work. Exact attempt settlement still refunds
only once and preserves original provenance and prior ACKs.

The same migration includes ingestion evidence/cursors/barriers, recovery incidents, retired
principal namespaces, versioned clock checkpoints and audited recovery-floor dispositions.
These reserve the complete storage shape for the separate ingestion/recovery implementation;
they do not yet implement ingestion.resume, restore-marker I/O or clock reconciliation. There
is no automatic detection claim for restores that bypass the trusted restore procedure.

Controlled tests cover C11 authored-work attribution, repeated incidents and re-enrollment;
C12 held claims/ACKs, late settlement and original provenance across renewal; and C15 migration
rollback/rerun, incompatible historical identifiers and committed release/cancel dispositions.
Pending admission extensions remain with #28. These tests do not replace the remaining controlled
fixture or actual-host evidence required before connecting a live session.

## Accepted runtime direction

The owner-approved roadmap changes the target deployment, not the current behavior above.
The decision provenance and accepted protocol ruling are recorded in
[control-plane decision #19](https://github.com/ginsys/parley/issues/19).

A standing server will own SQLite. `parleyctl` will become a deterministic client of authenticated
administration handlers, with no direct-database fallback. Those handlers may grant, renew and
revoke membership; agent-facing requests may not. Neither surface carries process spawning or
host execution approval. This explicitly replaces the original runtime prohibition on membership
administration once the specified implementation lands. Current protected CLI restrictions remain.

For production on this workstation the owner requires dedicated accounts: trusted server/admin
and non-sudo agent accounts, separate HOME/config/authentication/worktrees, and a trusted admin
login path. A human provisions and validates them. Development and CI use temporary synthetic data
under the current account; production isolation is not claimed from those tests. Separate socket
names alone cannot distinguish a human from an agent under the same account. The production
validation issue owns explicit privilege-route evidence; it does not block ordinary development.

The store now provides the explicit reader foundation described in [Runtime foundation](runtime.md).
The server will serialize writes through one immediate-transaction connection and use at most
four separate read-only deferred connections for pure queries. Authorization reads stay with the
mutation in a writer transaction. Bounded reads have a five-second deadline including connection
acquisition, materialize results and close rows/transactions before network I/O. Subscriptions
retain no connection during waits or client I/O. Readers open after writer initialization,
numbered migrations and recovery; online migrations are excluded.

Keep the store-owned writer `databaseDSN` and add an internal sibling `readerDSN` with shared path
normalization, `mode=ro`, `_txlock=deferred`, `_query_only=on`, `_busy_timeout=5000` and foreign-key
settings. Keep the caller DSN allowlist: do not expose a locking override through it. The VFS
read-only mode and connection query-only pragma protect different layers; both remain enabled.
Opening readers before a database exists fails. The pinned SQLite driver supports these options.

Server startup will accept ordinary filesystem paths only, rejecting memory databases and URI
connection strings before locking. Acquire nonblocking exclusive flock on a persistent lock file
adjacent to the canonical DB path before `store.Open`, migration/recovery and listeners. Hold its
close-on-exec descriptor until workers and both pools stop; do not unlink on normal release.
Contention rejects the second startup before either SQLite retry path, while legitimate database
busy retries remain. Concurrency tests must use temporary file-backed WAL databases.

`PARLEY_DB` will become server-only; the control spec defines client endpoint configuration. Help
and validation remain server-independent. Migration documentation must cover human-controlled
stopped-service consistent backup/relocation, WAL/SHM and ownership, and must not silently create a
replacement database at a new location.

The human control plane uses the accepted Unix-first JSON-RPC profile below. Session authentication
follows the [accepted identity decision](#accepted-peer-identity). Protocol v1 must use the accepted
membership model below even for a two-member-only implementation. Unsupported larger topologies
must fail explicitly. Future TCP
transport should preserve application semantics; its implementation is outside the first milestone.
The product sequence is two-peer runtime, durable inbox, then shared rooms. GitHub owns scopes,
acceptance and native dependencies; this section records architectural direction only.

## Accepted human control protocol

On 2026-09-11 the owner accepted [decision #19](https://github.com/ginsys/parley/issues/19): a
protected pathname Unix stream socket carrying a bounded, newline-delimited JSON-RPC 2.0
single-call profile. A deterministic CLI/TUI can issue commands and receive change notifications
on one connection. Batches and fire-and-forget mutations are excluded from the initial profile;
this is not unrestricted JSON-RPC conformance. TCP remains future work.

The server authenticates administration against configured trusted administrator OS accounts using
kernel socket credentials; the client verifies the trusted server UID and protected socket path.
A stable administrator principal is separate from every agent binding. Agent credentials and
endpoint names cannot confer administrator capability. This identifies the trusted account, not
human intent within it: any process with that account's authority could administer Parley.
Production therefore requires the already-decided separate non-sudo agent accounts and trusted
administrator login path. Same-account development does not prove that separation.

The control plane carries membership approval/renewal/revocation and reviewed identity/recovery
administration. It carries neither process spawning nor host execution approval. `parleyctl`
becomes a deterministic client with no direct-DB fallback, while offline help/validation remain
available. This is the accepted change to the target runtime's administration boundary; the
current protected-controller restrictions still apply until implementation lands.

The initial surface observes conversations/members, bounded state snapshots and pending-human
items; subscribes to change hints; and invokes the named membership, admission, identity and
recovery operations. There is no generic execute or arbitrary pending-item action. A snapshot
returns a server epoch/view revision. Subscription succeeds only while that view is current;
a change in the gap requires resnapshot. Reconnect, restart or notification-buffer overflow also
requires a fresh snapshot. Hints are not a historical event journal; durable state and audit own
history. A client must expose an unsynchronized view during sustained churn, not hide a gap.

Every mutation uses a durable operation identity, exact expected versions and current authority.
An ambiguous retry returns the recorded result, never a second grant or replenished budget.
Stale approvals fail without retargeting. Mutation/result/audit commit together; audit records
trusted principal, exact affected identities/versions, outcome and server time without secrets,
message bodies or raw transport errors. Restoration reconciles audit and replay state too.

Alternatives considered: HTTP/REST plus SSE/WebSocket suits a browser gateway but adds another
transport/event contract for the first local CLI; gRPC adds protobuf and code-generation tooling;
immediate TCP adds remote identity/confidentiality requirements. A second database-reading process
would not itself provide runtime commands or push consistency. A durable notification journal
could reduce resnapshot cost, but current-view observation and durable command audit meet the
initial need without its replay-retention contract. These alternatives remain possible later.

The [control specification](specifications/control.md) develops exact framing, operations,
snapshot coordination, concurrency, ownership and failure fixtures for review. Neither this
decision nor its specification claims an implemented server or proven production isolation.

## Accepted conversation admission

The owner approved the actual-pair admission direction in
[decision #21](https://github.com/ginsys/parley/issues/21) on 2026-09-11. This is a target workflow,
not an implemented API. The full identity decision is recorded below.

Two-peer support limits participants per conversation, not the number of conversations. Several
conversations may independently be waiting for a counterpart. Resuming a known host session does
not choose its conversation: authenticated host identity, conversation membership and online
presence are separate facts.

An agent may request a named conversation with a short purpose, or select an existing conversation
that it is permitted to discover. A single eligible result can be offered directly; multiple
results require an explicit choice. Discovery identifies the conversation, its participants and
actual availability; it does not expose private message bodies or credentials. An enrolled member
being offline does not make their place available to another agent.

A new conversation waits with its initiating participant and no active communication grant.
There is no placeholder second peer and no dispatchable traffic under the pending request. The
current controller requires two distinct peers when creating a grant, so waiting and join requests
need a separate specified lifecycle before implementation; do not weaken the active-grant contract.

For example, Alice requests `Review authentication` while Bob is waiting in `Debug deployment`.
Carol lists eligible conversations and selects Alice's. That selection creates a join request,
not membership. Authenticated human administration approves the actual Alice/Carol pair, direction,
budget and expiry before the conversation can carry messages. Each recipient's readiness remains
a delivery prerequisite. Agents cannot approve requests, mint grants or replenish budgets.

Approval must revalidate the current request and conversation atomically with grant creation.
Concurrent candidates cannot both occupy the second place; a stale approval fails and refreshes
the choices. Retrying an approval after a lost response must not create another grant or reset its
budget. Rejecting a candidate leaves the conversation waiting, without joining them elsewhere.
Cancellation or expiry of a pending request invalidates later approval. Disconnecting an admitted
member preserves membership; reconnect cannot silently replace them or select another conversation.

This keeps the authorization decision concrete: the human knows both peers before communication
is enabled. The cost is an admission approval when the counterpart arrives. Pre-authorizing any
later eligible agent was considered but not selected for this initial workflow: it would require a
separate bounded admission authorization with eligibility, expiry and limits. Discoverability or
possession of a connection credential must not accidentally provide that authority.

The accepted identity model below settles credentials, discovery and reconnect policy; detailed
operation schemas and pending-request deadlines belong to the connection specification. This ruling does not select wire methods,
change the protected-controller restrictions, approve host execution, or prove production account
isolation. Retained conversation context is a separate feature; context delivery cannot itself
authorize admission. Implementation must exercise multiple waiting conversations, unauthorized
discovery/join, competing candidates, stale approval, duplicate requests, restart and offline
members with synthetic identities and controlled transports.

## Accepted peer identity

On 2026-09-11 the owner accepted the full [identity decision](identity-proposal.md), after PR #43
landed and its review findings were resolved. A human registers an existing native host session;
one immutable binding and private per-binding bearer credential identify its peer across multiple
conversations. Local connector UID and server identity checks complement credential possession.
Same-account theft and compromised adapters remain explicit limitations.

Discovery uses explicit advertisement to registered peers or named invitations. A resumed native
session keeps its peer, while a new native session requires a new binding and admission. Exclusive
connection generations and authenticated generation lookup handle reconnect and lost responses;
neither reconnect nor rotation renews a grant. Revocation holds outstanding authored work across
credential versions until human disposition, including after re-enrollment or late settlement.
Restoration blocks ordinary operation until replay, delivery, authorization and accounting state
are reconciled; a new credential or process epoch alone is insufficient.

The decision records rationale, alternatives and required rejection/recovery cases. The
[connection specification](specifications/connections.md) proposes its data, operation, migration
and fixture details. These are target contracts; identity binding is not yet implemented.

## Accepted membership model

The owner approved [decision #17](https://github.com/ginsys/parley/issues/17) on 2026-09-10.
These are target contracts; the implemented core still uses the pair representation described below.
The [draft membership specification](specifications/membership.md) expands them into proposed API,
storage, lifecycle and verification contracts for owner review.

Grants have immutable versioned members (one exact peer ID and role per version) and a policy:

| Policy | Permitted communication between distinct enrolled members |
| --- | --- |
| `open` | Every member to every other member |
| `lead_only` | Exactly one lead; lead-to-member and member-to-lead, never member-to-member |
| `directed` | Explicit listed edges only; no implied reverse edge |

Legacy `bidirectional` maps to `open` with members A and B; `a_to_b` maps to the single directed
A-to-B edge; `b_to_a` maps to B-to-A. These preserve the exact existing permitted edges, with no
widening. A one-way grant must not become `lead_only`, which would also permit a reverse message.
Self-send remains forbidden. `lead_only` is named so administrator intent survives member changes
without re-enumerating edges: adding a developer establishes only lead/developer communication.
The `directed` policy never adds edges implicitly when a member joins.

**The first runtime and two-peer inbox retain the current positional grant storage.** A
members-shaped API translates the supported two-member policies to that representation and rejects
unsupported requests explicitly. Initially accept exactly two `member` roles with `open` or a
single-edge `directed` policy. Reject `lead_only`, empty/two-edge `directed` and larger groups:
pair storage cannot retain their role/policy intent, even if today's allowed edges coincide with
`bidirectional`. Never collapse them into `open` or add hidden policy storage. The
members/policies/edges tables and historical backfill appear
only in the later room migration, after the inbox. The membership specification defines both
stages; runtime ownership and administration do not require a members table. The room migration
uses numbered atomic `user_version` steps, preserves every historical conversation/version key and
envelope provenance, and retires positional storage without concurrent dual writers. Frozen
legacy-adoption SQL remains unchanged. Enforce unique members and schema-level self-send/self-edge
rejection where sender/recipient columns exist; controller validation alone is insufficient.

Member, role or policy changes create a successor version through human administration. Preserve
ordinary queued-message cancellation and default carry-forward of proven replies only while the
members, edge, provenance and intervening version history still permit it. Revocation and explicit
cancel-pending-replies boundaries remain barriers. Removed-member/edge replies are cancelled and
reported; acknowledged originals are not reset or replayed. Already-dispatching/handed-off messages
cannot be recalled; late never-attempted settlement must revalidate successor history.

Keep one shared exchange budget per active conversation grant. Successors retain today's renewal
semantics: zero used exchanges and an explicit or retained maximum. A chatty member can exhaust
that shared budget for every participant, including a lead-only room. This is an accepted
consequence; the model provides no per-member fairness, quota or reservation.

Fresh sends explicitly name conversation and one recipient, with sender derived from authenticated
binding. Replies derive conversation from the original envelope ID, retain explicit recipient and
original sender/recipient provenance checks, and require current authorization. An eligible new
reply may reference an older delivered original; it cannot revive a cancelled queued reply.
No implicit broadcast is introduced. Session authentication, control framing and inbox disposition
permissions remain separate contracts.

## Accepted identifier alphabet

On 2026-09-11 the owner chose printable ASCII for both conversation names and peer IDs:
bytes `0x20`–`0x7E`, with empty and space-only values rejected. Preserve permitted spaces and
punctuation exactly; message bodies retain their existing encoding rules. This simplifies identity
round-tripping by rejecting malformed UTF-8 and all non-ASCII names, including otherwise valid
Unicode names. The rejected alternatives were unrestricted Unicode plus compatibility machinery,
and encoded byte identifiers across public APIs.

Before upgrading an existing database, inventory stored identifiers read-only. If all comply,
no identifier migration or recovery feature is needed. Preserve incompatible historical bytes and
current exact-key human revocation; determine any necessary disposition from actual findings
before replacing the administration interface. No encoded-ID recovery API is approved.
The [membership specification](specifications/membership.md#accepted-ascii-identifier-rule) records
the contract. The shared byte predicate now enforces it at enrollment/renewal, acceptance,
queued claims, reply validation and wrapping/direct Codex delivery. Incompatible queued work
returns `incompatible_identifier` without changing historical state, spending budget or calling
the transport. Already claimed attempts retain their settlement rules. The
[read-only inventory](identifier-inventory.md) reports exact bytes from stopped database copies;
no operator database has been inventoried by these synthetic fixtures.

## Components

| Package | Responsibility |
| --- | --- |
| `internal/controller` | Validate enrollment/renewal, preserve grant history, cancel/reassign eligible queued rows |
| `internal/store` | SQLite transactions, versioned migrations, grants and envelope transitions |
| `internal/dispatch` | Authorize acceptance/claim, account for budget, call transport and settle outcomes |
| `internal/adapter/claude` | Readiness nonce/generation, cancellation on reconnect, bounded recipient polling |
| `internal/adapter/codex` | Queue-process outcome classification and atomic validated reply ingestion |
| `internal/replymarker` | Extract the permitted Markdown fence and validate reply provenance |
| `internal/bridgetext` | Untrusted-payload wrapper with unpredictable per-message boundaries |

There is no process wiring these components into a live bridge. Channels delivery, rollout
watching, identity binding and runtime lifecycle still need contracts and live evidence;
[issue #12](https://github.com/ginsys/parley/issues/12) owns that separate work. Durable inbox
semantics are separately tracked in [issue #6](https://github.com/ginsys/parley/issues/6). The
[follow-up proposal](specifications/follow-ups.md) describes explicit two-peer dispositions and
checkpoint summaries for owner review; it changes neither envelope states nor execution authority.

## Grants and renewal

A conversation has at most one active grant, enforced by a partial unique index. Versions
increase across renewals and re-enrollment after revoke; history is retained. Peers must be
nonempty and distinct, direction must be valid, the grant budget must be positive and an explicit
expiry must be in the future. Names and peer IDs are opaque exact keys: permitted leading/trailing
spaces are preserved. Both conversation and peer identifiers require printable ASCII bytes
with at least one non-space byte. Enrollment/renewal share the wrapper's byte validator and reject
incompatible keys before grant writes; CLI input rejects them before opening storage. Legacy IDs
are never rewritten, and a historical grant with unusable names or peers remains revocable.
Silently trimming existing keys could target a different conversation
or make historical grants inaccessible; administrator output quotes keys to expose whitespace.
Renewal rejects negative budget/TTL inputs; zero budget or omitted
expiry preserves the current setting, while `exchanges_used` starts at zero on the successor.

Acceptance validates the current peer pair, direction, status and expiry. Claiming delivery
revalidates all of these and requires the envelope's exact current `grant_version`. Exhaustion
leaves the message queued and returns `ErrBudgetExhausted`. Failed budget updates are explicitly
classified from the current grant, rather than assumed to mean exhaustion.

Renewal cancels old ordinary queued messages. Trusted replies carry forward by default because
ingestion already acknowledged their originals and no sender remains able to resubmit them.
Carry-forward requires trusted ingestion provenance, the acknowledged original in the same
conversation with reversed peers, and permission under the successor's direction.
`cancel_pending_replies` is persisted on the successor and prevents carry-forward across that
boundary, including late outcomes after further renewals. Revocation also blocks carry-forward.
An expired trusted reply waits queued for renewal; ordinary expired messages cancel at claim.
No carry-forward revives terminal rows.

## Delivery and accounting

Acceptance inserts a `queued` envelope without calling a host. Dispatch uses three steps:

1. In `BEGIN IMMEDIATE`, authorize, claim one budget slot and move `queued` to `dispatching`,
   incrementing `dispatch_attempt`; commit before invoking transport.
2. Call the host outside the database transaction.
3. In a fresh transaction independent of caller cancellation, conditionally settle the exact
   envelope ID, original grant version, attempt token and `dispatching` state. Only a winning
   settlement may refund the original grant, in that same transaction.

| Transport result | Stored state | Budget | Automatic retry |
| --- | --- | --- | --- |
| Host accepted | `handed_off` | Retained | No |
| Definitely not attempted, retryable | `queued`, or `cancelled` if authorization was superseded/revoked | Refunded | Only if queued and later authorized |
| Permanent rejection before host call | `failed` | Refunded | No |
| Ordinary reported host failure | `failed` | Retained | No |
| Ambiguous post-start timeout/signal/termination | `uncertain` | Retained | No |
| Process interrupted before outcome is durable | `uncertain` after recovery | Retained | No |

`handed_off` is host acceptance, not proof the recipient processed the message. Valid ingestion
atomically moves the original from `handed_off` to `acked` and queues its trusted reply. Competing
acknowledgements fail the expected-state transition. A missing envelope returns
`ErrEnvelopeNotFound`; an existing envelope in the wrong state returns `ErrStateConflict`. Settlement cannot overwrite a newer retry
or refund twice, even if the same grant remains active. If outcome storage fails, dispatch
returns an error with an empty/unknown state and no uncommitted diagnostic values; it preserves
whether this call attempted transport delivery. The row may still be `dispatching`.
`Outcome.Attempted` always describes this invocation, including stale settlement and no-claim
paths; it is not inferred from historical row state. Reading an already `handed_off` envelope
returns that durable state with `Attempted=false`, because this call did not invoke the host.

The Codex transport distinguishes failure to start from abnormal termination after startup.
It uses controlled subprocess tests; the meaning of ordinary nonzero CLI exits remains a host
compatibility assumption requiring live validation. Wrapping rejects empty/control-bearing
metadata before host invocation; the payload itself remains untrusted and is preserved verbatim.

Stored diagnostics are fixed `error_code`/`error_detail` values: `ambiguous`, `not_attempted`,
`rejected`, `failed`, and `interrupted` for crash recovery. Successful claims clear previous
attempt diagnostics. Raw errors, message bodies and transport output are never copied into these
fields. Returned operation errors may still describe database or parsing failures.

`RecoverUncertain` must run after prior dispatchers have stopped, before new dispatch begins.
It cannot distinguish a dead dispatcher from a currently active one. It never retries or refunds.
A controlled child-process test exits after a fake handoff and before outcome recording to prove
this recovery path. Human investigation is required for uncertain rows; no resolution UI exists.

## Readiness and queue ordering

Claude readiness requires acknowledgement of the current nonce. Reset/stop changes the generation
and cancels its context. Polling rechecks that generation before each dispatch and connects both
caller cancellation and generation cancellation to the transport context. Context cancellation
is cooperative; a host that already accepted a message cannot be recalled.

Each tick selects at most 100 queued IDs for the conversation and exact recipient, using the
`(conversation, state, to_peer, created_at_ns, id)` index. Bodies are loaded only when claiming an
individual envelope. Numeric nanoseconds order timestamps correctly despite RFC3339 fractional
precision; equal timestamps sort by envelope ID. `Tick` returns `dispatch.Outcome` values with
state, attempted flag and diagnostics, and stops on budget exhaustion with its explicit error.
An exhausted candidate is never reported as a host attempt. A process-local cursor advances
between serialized ticks and wraps at the tail so retryable old rows do not starve a later
backlog. Dispatch errors, including budget exhaustion, preserve the blocked position so renewal
resumes there; retryable transport outcomes still advance. Restarting the poller resets the cursor;
it does not change durable message state.

## Reply syntax

Only a complete, top-level fenced code block is eligible. Its opening line starts at column zero,
contains exactly three backticks followed by `BRIDGE-REPLY`, and permits only trailing ASCII
spaces/tabs. A closing backtick fence must be present. The body is strict JSON with
`in_reply_to`, `to` and `text`; unknown fields, duplicates/malformed markers and invalid metadata
are rejected. The supplied original must be a `handed_off` envelope in the same conversation, addressed
to the responding peer. Ingestion authorizes the reply against the current grant; the original
may have been delivered under an earlier version. The reply targets the enrolled opposite peer.

Goldmark v1.8.6, without extensions, determines Markdown block membership. Only direct document
children are considered: list items, quotes, HTML blocks, other fenced blocks and indented
openers cannot become replies. Syntax extraction grants no authority by itself; ingestion still
validates provenance and commits acknowledgement plus reply insertion atomically. Ordinary
`Send` cannot set trusted-reply provenance by supplying an `in_reply_to` value.

## Storage and migrations

SQLite connections use WAL, foreign keys, recursive triggers, a bounded busy timeout and `_txlock=immediate`.
Recursive triggers are required because SQLite replacement writes fire deletion guards only when
[recursive triggers are enabled](https://www.sqlite.org/lang_conflict.html); the setting is applied
through every store-owned connection DSN, including reopened connections.
`database/sql.Tx` owns cancellation and connection cleanup; no custom transaction wrapper exists.
All migration steps and `PRAGMA user_version` changes run within one immediate transaction:

| Version | Change |
| --- | --- |
| 1 | Bootstrap/adopt the two known unversioned schemas, normalizing trusted-reply provenance |
| 2 | Persist renewal cancellation policy on grants |
| 3 | Add dispatch attempt and fixed diagnostic fields |
| 4 | Backfill numeric envelope timestamps and add the recipient queue index |
| 5 | Add installation, bindings, credentials, publication evidence and permanent command results/audit |
| 6 | Add work provenance/quarantine/holds and complete ingestion/recovery retention schema |

`schema.sql` remains the frozen version-1 schema used for legacy adoption. Only version-zero
bootstrap inspects column layouts; subsequent steps follow version numbers. Unknown legacy
layouts, future versions and malformed or nanosecond-unrepresentable timestamps abort without
partial changes. Timestamp backfill reads bounded ID/timestamp pages, preserves original text,
and validates round-trip range before writing numeric values. Messages and metadata are plaintext;
filesystem isolation and SQLite-consistent backups remain the operator's responsibility.

## Evidence and limits

The normal verification gate is `mise run verify`; its contents are described in
[CONTRIBUTING.md](../CONTRIBUTING.md#documentation-checks). Regression tests cover controlled
subprocess outcomes, cancellation, independent connections, migration rollback/concurrency,
authorization/lifecycle, parser rejection, stale settlement/refunds and queue/CLI behavior.
Race/coverage, fuzzing, module verification, vulnerability and secret scans are supplemental
checks, not implicitly part of that gate.

Passing synthetic fixtures does not prove impersonation resistance or compatibility with live
Codex/Claude installations. Before live connection, the identity/approval-forgery and actual-host
fixtures in [AGENTS.md](../AGENTS.md#record-evidence-and-decisions) must be designed and passed.

## Authenticated work and retained ingestion

The internal `connection.Manager.Send` API accepts a private attached Session, operation ID,
conversation, recipient and text. The manager derives the sender and accepting credential version;
caller-supplied token fields cannot impersonate a Session. Enabled recipients may be offline at
acceptance. `dispatch.AuthenticatedBridge` claims only after both current bindings, immutable author
provenance, grant/version, hold state, exact recipient readiness and budget have been checked under
the shared coordinator. Transport receives the captured recipient Session after commit. A closed
claim cannot retarget its replacement, and an unattempted handoff uses the existing exact-token
settlement/refund transaction. The existing Claude poller now consumes this bridge. Raw sender-string
Send and Codex IngestTurn entrypoints have been removed. Ingestion consumers use the shared
connection.Ingestor with trusted source evidence and a private Session. Legacy regression fixtures
construct synthetic capabilities and explicit provenance; production code never imports those
test helpers. Fixed authenticated error codes replace detailed legacy acceptance errors.

`connection.Ingestor` requires trusted native-source verification and origin providers. It stores
source event identity, digest, revision and cursor edges independently of process epoch. Current
authentication precedes replay; an exact retained terminal event is returned without rereading a
changed host file, while changed content under that identity returns `event_conflict`. Source I/O
uses a bounded socket-linked context outside the writer. Invalid UTF-8 is rejected before hashing.
A pending predecessor keeps its cursor; terminal classification, original ACK, reply insertion and
accepting provenance commit together. Permanently cancelled originals become terminal held evidence
without an ACK. Human `ingestion.resume` requires an exact barrier/binding version and an immutable
reviewed contiguous source interval. Its events become held evidence, preventing later replay from
acknowledging old work. More than 1000 interval events returns `capacity_exceeded` and leaves the
barrier closed. Native event production and large-interval tooling remain separate host work.

## Durable recovery implementation

`internal/recovery.Service` supplies `runtime.Config.InspectRecovery` and installs store hooks before
service admission. Preparation and marker flush run outside the coordinator. Time sampling,
checkpoint comparison and advancement share a writer acquisition; business transactions receive one
validated authorization instant. A durable preceding checkpoint remains even when later business
work rejects. A rollback detected in the writer immediately holds ordinary operations and is flushed
after the gate is released. The old direct acceptance/claim/ingestion paths refuse a recovery-owned
store, while exact settlement remains available to retain an already attempted delivery's outcome.

The Linux marker repository uses a pre-existing private directory and descriptor-relative,
no-follow operations. Marker creation is non-replacing and syncs the file and parent directory.
Removal requires an exact matching canonical record and syncs the directory even when retrying an
already absent file. Malformed entries, unsafe paths and reused cleared incidents fail closed.
External publication failure invokes a mandatory nonblocking supervisor fail-stop callback; its
actual process/supervisor integration remains required before live use. No persistence guarantee is
claimed when every persistence path fails, or for a rollback never recorded before a crash and
subsequent clock correction. Once a marker or held database incident exists, corrected wall time and
restart cannot clear it. Restore detection requires the operator to establish a fresh external marker.

Internal human `clock.reconcile` verifies reviewed source evidence and nondecreasing samples at
least one monotonic second apart outside the writer, then rechecks the current floor/version. The
reconciled record, receipt and audit commit before exact marker removal and a final durable clear.
Recording an existing reconciled incident is idempotent because its global hold remains active;
the storage primitive rejects reuse of a cleared incident without changing its terminal evidence.
Retries reauthorize before looking up the receipt and may finish only that committed cleanup. Other
incidents keep the global gate closed. `recovery.complete` follows the same two-phase protocol.
Its implemented restore policy is conservative: every restored binding must be permanently retired,
and every outstanding envelope receives an independent restore hold. Surviving-history import is
not implemented. Grant budgets, ACKs, states and attempt tokens remain snapshot evidence; retired
identities cannot use them to authorize new work. A reviewed bad-floor disposition additionally
retires affected authority, matches the exact checkpoint version and all held clock incidents, and
records the floor change against the same audit. Ordinary reconciliation never lowers the floor.
Recovery does not clear individual security holds, ingestion barriers or persisted expiry.
Trusted writer authorization helpers enforce remembered exact-credential expiry denials even
when their caller has not installed an expiry observation collector.

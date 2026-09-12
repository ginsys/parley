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

## Accepted runtime direction

The owner-approved roadmap changes the target deployment, not the current behavior above.
The decision provenance and remaining protocol ruling are recorded in
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

Unix-first NDJSON JSON-RPC remains a proposal. Session authentication follows the
[accepted identity decision](#accepted-peer-identity). Protocol v1 must use the accepted membership
model below even for a two-member-only implementation. Unsupported larger topologies must fail explicitly. Future TCP
transport should preserve application semantics; its implementation is outside the first milestone.
The product sequence is two-peer runtime, durable inbox, then shared rooms. GitHub owns scopes,
acceptance and native dependencies; this section records architectural direction only.

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
this target contract; current runtime validation has not yet been changed.

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
semantics are separately tracked in [issue #6](https://github.com/ginsys/parley/issues/6).

## Grants and renewal

A conversation has at most one active grant, enforced by a partial unique index. Versions
increase across renewals and re-enrollment after revoke; history is retained. Peers must be
nonempty and distinct, direction must be valid, the grant budget must be positive and an explicit
expiry must be in the future. Names and peer IDs are opaque exact keys: permitted leading/trailing
whitespace is preserved, while whitespace-only values are rejected. Peer enrollment and renewal
share the wrapper's metadata check: control/format characters and U+2028/U+2029 are rejected before
grant writes; CLI enrollment rejects them before opening storage. Legacy IDs are never rewritten,
and a historical grant with unusable peer IDs remains revocable. There is no session identity
canonicalization policy yet. Silently trimming existing keys could target a different conversation
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

SQLite connections use WAL, foreign keys, a bounded busy timeout and `_txlock=immediate`.
`database/sql.Tx` owns cancellation and connection cleanup; no custom transaction wrapper exists.
All migration steps and `PRAGMA user_version` changes run within one immediate transaction:

| Version | Change |
| --- | --- |
| 1 | Bootstrap/adopt the two known unversioned schemas, normalizing trusted-reply provenance |
| 2 | Persist renewal cancellation policy on grants |
| 3 | Add dispatch attempt and fixed diagnostic fields |
| 4 | Backfill numeric envelope timestamps and add the recipient queue index |

`schema.sql` remains the frozen version-1 schema used for legacy adoption. Only version-zero
bootstrap inspects column layouts; subsequent steps follow version numbers. Unknown legacy
layouts, future versions and malformed or nanosecond-unrepresentable timestamps abort without
partial changes. Timestamp backfill reads bounded ID/timestamp pages, preserves original text,
and validates round-trip range before writing numeric values. Messages and metadata are plaintext;
filesystem isolation and SQLite-consistent backups remain the operator's responsibility.

## Host-probe matrix isolation

Host-probe matrix trials (`scripts/probe/host_trials.py`) are not ordinary tests: an owner
decision of 2026-09-11 has them run against an installed host CLI under the operator's real HOME
(`wake_probe.py --home inherit`), because a disposable HOME holds no host credentials and would
measure an unauthenticated session rather than a wake. The alternative considered — disposable at
the HOME level, matching every ordinary test — was rejected for exactly that reason: it cannot
authenticate against a real host, so it cannot measure what the matrix exists to measure.
Isolation is instead at the *session* level: each trial runs against a throwaway host session
tracked in a `SessionRegistry` that refuses to touch any id it did not itself mint or adopt,
torn down after the trial where a teardown mechanism is captured (Claude). Codex has no captured
create or teardown path: the caller creates the thread, `run_trial(existing_session=...)` adopts
it, `teardown()` raises `TeardownUnsupported` and keeps ownership so the id stays reportable, and
the caller disposes of the thread it created. Every matrix cell this produces therefore carries the developer's real
credentials and configuration and must be sanitized before publication — see
[host probes](host-probes.md#matrix-runner) for the driver contract and outcome detectors.

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

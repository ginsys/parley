# Connection principals and session lifecycle specification

Specification delivered for [#22](https://github.com/ginsys/parley/issues/22) in owner-enqueued
PR #44, grounded in the accepted [identity decision](../identity-proposal.md) and
[membership contract](membership.md). The landed artifact supplies the connection baseline;
GitHub records its acceptance. This document adds no runtime or schema implementation.
The [control specification](control.md) maps the logical operations to proposed wire contracts.

## Scope and boundaries

The first runtime supports many conversations with exactly two admitted members each. Connection
registration/authentication, conversation selection, membership approval and host readiness are
separate transitions. One peer can belong to several conversations. Disconnecting a member never
creates an available place, and resuming a native session never selects a conversation implicitly.

This specifies application semantics for the future server and deterministic adapters. Operation
names below are logical names, not approved RPC methods or executable CLI commands. Wire framing,
endpoint configuration, transport-wide frame/concurrency limits and authenticated human transport
belong to the control-plane specification. The protected controller remains human-only. An agent
credential never establishes an administrator principal, regardless of endpoint or request fields.

Host-specific evidence needs a verified integration before that host can become ready. Controlled
fixtures can implement the evidence interface; they do not prove a real host authenticator. This
specification does not provide remote transport, production account setup, history handover,
retained context, inbox dispositions, multi-party membership or execution authority.

## Logical records and validation

All records belong to the server-owned database except private credential files, live sockets and
the restore startup gate. The following are proposed logical storage contracts, not migration SQL.

| Record | Required fields and constraints |
| --- | --- |
| Installation | Immutable `server_id`; positive `schema_version`; random `server_epoch` created at every process start |
| Binding | Immutable `binding_id`, unique `peer_id`, unique `(host_kind, host_namespace_id, host_session_id)`, enrolled `connector_uid`; `status` = `enabled`, `revoked` or terminal `retired`; positive `binding_version`; nonnegative durable `connection_generation` |
| Credential | Immutable `(binding_id, credential_version)`, unique `credential_id`, hash verifier, finite `expires_at`, `status` = `current`, `superseded`, `expired` or `revoked`; at most one current credential per binding |
| Connection | Binding and credential version, epoch/generation, kernel connector UID, socket identity, liveness deadline, `authenticating`, `not_ready`, `ready` or `closed`; socket/readiness are process-local |
| Pending conversation | Unique `pending_id`, exact `conversation`, creator binding/credential provenance, purpose, visibility, immutable invitee set, finite deadline, positive `request_version`, `waiting`, `approved`, `cancelled` or `expired` |
| Join request | Unique `join_id`, pending ID, candidate binding/credential provenance, finite deadline, positive `request_version`, `pending`, `approved`, `rejected`, `cancelled` or `expired` |
| Accepted-work provenance | Unique work kind/ID, accepting binding and credential version; immutable across grant renewal, carry and credential rotation |
| Security hold | Work kind/ID and revocation incident ID, binding, creation time, positive `hold_version`, `held`, `released` or `cancelled`; append-only human disposition evidence; several incidents may hold one item |
| Operation result | Unique `(principal_id, operation_id)`, operation kind, canonical request digest, committed result or terminal error; no credentials or message bodies in result metadata |
| Ingestion evidence | Unique `(binding_id, native_event_id)`, source revision/digest and result; durable source cursor, unresolved event references and any recovery barrier |

Generated record IDs and operation IDs use canonical lowercase UUID strings; they confer no
permission. Server epochs use fresh random IDs. Counters are signed 64-bit integers, checked before
increment; overflow fails without mutation. `connection_generation` starts at zero; allocated
connections start at one. Peer IDs and conversation names obey the accepted exact printable-ASCII
rule; no trimming, normalization or silent historical rewrite. Operation names and tagged unions
reject unknown fields/tags. Omitted optional fields and explicit null have distinct schemas.

Proposed bounds: new peer/conversation keys at most 256 bytes; native session/namespace locators at
most 4096 UTF-8 bytes, additionally validated by the host integration; purpose at most 512 UTF-8
bytes with control/format and line-separator characters rejected; at most 100 distinct invitees.
Generated identifiers are never filesystem path components except the trusted credential ID.
Existing longer or incompatible identifiers require read-only inventory and explicit disposition;
the upgrade must not strand human revocation or silently omit historical records.

Persist timestamps as UTC instants with numeric nanosecond ordering and an explicit representable
range. Compare expiry using server time (`now >= deadline` is expired), never client time. Deadline addition overflow rejects the operation. Liveness timers use a monotonic clock; restart
resets them. Reliable server wall time is an operational precondition. A detected backward step
blocks new authorization pending human clock reconciliation; do not claim to detect every clock
rollback across a crash or database restore. Persist observed terminal request/credential expiry
in the writer so those records cannot revive when time changes. Clock recovery never extends
existing deadlines; broader restore recovery is specified below.

## Registration, credentials and host provenance

Human registration names the exact native binding, connector UID, peer and credential expiry. A
trusted integration validates the native locator/evidence; duplicate binding or peer registration
fails without issuing another credential. Registration grants no membership. Changing the native
session, namespace or UID requires a new peer/binding; never edit the historical tuple in place.
Human re-enrollment of the same revoked tuple uses its existing binding and increments its version.
Retired bindings and peer IDs cannot be reused. Legacy peer enrollment requires review of its
existing grants and unprovenanced work before enabling it.

Generate a separate 32-byte uniformly random credential for each version; store its SHA-256
verifier and compare fixed-length verifiers in constant time. Provision the secret only through
the trusted administration path into the adapter's private credential file, as required by the
[accepted protection contract](../identity-proposal.md#credential-provisioning-and-protection).
The file contains server ID, credential ID/version, binding ID and the secret; it is never ordinary
API output. Reject symlinks, unexpected ownership, non-regular files, broad file permissions or
unsafe parent directories. Cross-account publication is a trusted setup capability, never an
agent-specified arbitrary destination. Host prompts, arguments, environment values and diagnostics
must never contain the secret. A configured path can locate the file.

Commit enrollment/verifier before atomic file publication. A failed or lost publication response
is not proof that publication failed: report a fixed ambiguous-publication result, leave the
committed credential record, and require human rotation/revocation. Do not retry by revealing the
old secret. Rotation requires expected binding and credential versions, generates a new secret,
supersedes the old version and invalidates existing connections. Its result replay contains only
metadata, not the secret. A new human recovery rotation uses a new operation ID and current
expected versions. Re-enrollment cannot release security holds or renew membership.

For the first local integration, both peers check kernel credentials on the connected Unix stream
socket: the server checks the enrolled connector UID; the client checks the configured trusted
server UID and protected pathname before sending a secret. Do not accept abstract sockets under
this pathname-based protection contract. The operating-system evidence identifies an account,
not a language-model session. The accepted same-UID theft/adapter-compromise limitations remain.
A future remote implementation needs authenticated confidentiality and a reviewed replacement for
local checks; it cannot simply forward this bearer credential over plaintext TCP.

Human operations use a separately authenticated administrator principal and ordinary durable
operation IDs. No agent-facing request may call them through an alias.

| Operation | Required request fields | Result |
| --- | --- | --- |
| `binding.register` | Peer ID, host kind/namespace/session, connector UID, finite credential expiry, trusted provisioning target reference | Binding ID/version 1, credential metadata/version 1; no membership |
| `binding.rotate` | Binding ID, expected binding/credential versions, finite new expiry, trusted target reference | New credential version, incremented binding version, old connections invalidated |
| `binding.reenroll` | Revoked binding ID, expected versions, fresh matching host evidence, finite expiry, trusted target reference | Same tuple/peer enabled with new credential/version; holds and ingestion barrier retained |
| `binding.revoke` / `binding.retire` | Binding ID, expected binding/credential versions | Incremented binding version, revoked credential and durable incident/holds; retirement is terminal |
| `connection.disconnect` | Binding ID, expected epoch/generation | Closes only that slot; stale target conflicts, no grant change |
| `hold.disposition` | Work kind/ID, incident ID, expected hold version, cancel/release, reason | Incremented hold version and audited disposition |
| `ingestion.resume` | Binding ID, expected binding/barrier version, verified source interval and disposition, reviewed resume cursor/held event set | Audited barrier resolution; accepted events/ACKs are never invented |
| `recovery.complete` | External incident ID, expected durable recovery-record version, reviewed reconciliation/disposition references | Audited two-phase completion described below |

All mutable incident/barrier/recovery records have positive versions and increment them on each
transition. Repeating an operation ID returns its committed metadata; it never reprovisions a secret.
Changing expiry/credential requires rotation, not editing a credential record in place. Human
recovery input refers to trusted configured sources, never arbitrary agent-provided paths.

The trusted adapter's host verifier consumes the enrolled native tuple, its configured source and
the current connection token. It returns either verified matching host evidence, unavailable or
mismatch. Caller-provided transcript paths, PIDs and echoed session IDs alone are insufficient.
Unsupported evidence mechanisms return `host_unverified`; they cannot ready a connection. Each
host-specific implementation must record its pinned tool version, evidence source, failure cases
and passing compatibility fixtures before enabling live delivery.

## Connection operations and exclusive attachment

Unauthenticated sockets have a five-second authentication deadline. Limit each socket to one
credential authentication attempt; failure closes it with a generic `authentication_failed`.
The control layer must bound concurrent unauthenticated sockets. No lookup discloses whether a
credential ID, binding or peer exists before credential and UID verification succeed.

`connection.inspect` receives credential material and the enrolled tuple through the deterministic
adapter. After current credential, expiry, binding and UID validation, it returns only that binding's
`server_epoch`, committed generation and `active` flag. It grants no ordinary principal/readiness,
allocates no generation, reserves no slot and cannot evict a connection. Repeated lookups are reads.
Every lookup rechecks authorization; the secret is excluded from request recording/logging.

`connection.attach` receives the same authentication fields plus `expected_generation`. Under the
serialized writer, revalidate identity, confirm no active slot and compare/increment the durable
generation. Install the exact socket/epoch/generation slot before admitting ordinary operations or
returning success. A crash after commit but before installation leaves no reusable readiness;
restart or inspection can recover the committed generation. Reject another socket while a slot is
active. Repeating authentication on the successful socket returns its current attachment without
incrementing; it must still pass current credential/status checks.

Only the server constructs the operational principal: `(binding_id, peer_id, credential_version,
server_epoch, connection_generation, connector_uid)`. It is associated with the socket; presenting
those fields in a payload cannot create a principal. Expected versions are concurrency guards,
never proof of authority. An attached connection starts `not_ready` and may discover/request
admission. Sending and reply ingestion additionally require verified matching host evidence.

After host evidence succeeds, readiness uses a fresh unpredictable probe nonce scoped to the exact
connection token and intended host. An ACK from any other binding/generation/nonce is rejected.
Readiness has a 30-second attempt deadline with no automatic grant effect; a new explicit attempt
uses a fresh nonce on the same live generation. ACK handling cannot advance membership or budget.
Revocation, rotation, disconnect and restart cancel readiness and the old transport context.

Liveness requires an authenticated heartbeat at least every ten seconds; thirty seconds without
one closes the slot. Heartbeats affect liveness only, never readiness, credentials, pending
requests, grants or budgets. A close/timeout callback releases a slot only when its socket, epoch
and generation still match. Human disconnect can close that exact slot without changing identity.

Lost-response example: generation 5 attaches at 6; the response and socket are lost. A new socket
inspects 6, waits for the old slot to close or time out, then attaches with expected 6 and obtains 7.
A race loser returns `generation_conflict` or `already_connected`; the adapter may inspect and retry
at most three times per reconnect attempt with at least one second between retries. It never
evicts a winner. Exhaustion returns control to the host/operator. Reconnect resets readiness and
preserves memberships, credential provenance, operation results, ingestion evidence and budgets.

## Pending conversations, discovery and admission

All agent operations below derive the caller from its current socket principal. Bindings may be
registered and enabled while offline. Only human administration can approve a pair, and approval
requires both enrolled bindings currently enabled and unexpired, not necessarily connected.

| Operation | Inputs beyond operation ID | Authorized result |
| --- | --- | --- |
| `conversation.request_create` | Exact name, purpose, `visibility` = `hidden`, `advertised` or `invited`; invitee IDs only for invited; optional TTL | Creator-owned waiting record; no grant, active membership or traffic |
| `conversation.discover` | Optional cursor, limit | Permitted waiting records only; no mutation |
| `conversation.request_join` | Pending ID, expected request version, optional TTL | Caller-owned candidate request, not a reserved place |
| `conversation.withdraw` | Pending/join ID and expected version | Creator cancels its waiting record or candidate cancels its own join request |
| `conversation.inspect_request` | Own pending/join ID | Authorized current state; no other candidate identities or history |
| `admission.approve` (human) | Pending/join IDs and expected versions, expected binding/credential versions for both peers, exact supported membership/policy and communication limits | Atomically activates the actual pair, closes waiting state and records one approval result |
| `admission.reject` (human) | Join ID and expected version | Terminal rejection of that request; waiting conversation remains available |
| `admission.cancel` (human) | Pending/join ID and expected version | Terminal cancellation, without revoking an already-active grant |

Creation defaults to hidden. Hidden records are visible only to creator and human administration;
invited records additionally permit the named enabled peers; advertised records permit registered
enabled peers on this server. Invitations and visibility are immutable for a pending record: cancel
and explicitly create a new request to change disclosure. Invited/advertised does not authorize
joining a full conversation or accessing its context/history. The creator cannot join its own
waiting record; candidate bindings must be distinct.

Creation rejects a name already used by a waiting request or any stored conversation history.
Cancelled/expired pending names with no stored conversation can be reused with a new pending ID.
All later operations use the pending ID, so an old request cannot approve a newly reused name.
Keep terminal evidence. A peer may have one nonterminal join request per pending conversation;
a different operation ID cannot create a duplicate candidate. Reject a new request when its
creator/candidate has a security or recovery barrier that forbids the operation.

Pending creation defaults to one hour and join requests to fifteen minutes. Human-configured
maxima cannot exceed 24 hours; TTL must be positive, and a join deadline cannot exceed its pending
conversation deadline. Store absolute deadlines; reconnect, discovery and retries never extend
them. Expiry is enforced at every mutation even if background cleanup has not run. Creator
withdrawal/expiry cancels outstanding candidates; approval makes competing candidates unavailable.
No terminal state can be resurrected by operation replay.

Discovery returns pending ID, exact name, purpose, creator peer ID, creator connected flag,
`request_version`, deadline and `join_available`. It exposes no other candidates, native IDs,
paths, secrets, context or transcript. Connected status is informational, not a free membership
slot. Limit defaults to 20, maximum 100; order by creation instant then pending ID. Cursor is an
opaque server-authenticated position bound to the requesting peer and query, expires after five
minutes and confers no visibility. Every page rechecks current authorization/visibility. Results
are not a snapshot or reservation; selection is always revalidated. Active membership queries
are separate, scoped to members/humans by the control contract.

Approval checks both requests/deadlines/versions, both exact binding and current credential
versions, every hold, and the two distinct approved peers inside the same writer transaction as
initial conversation/grant creation and operation-result recording. Human-specified members must
match creator/candidate; supported policies/limits follow membership.md. No placeholder member is
inserted. Two approvals cannot create two grants or a third member. A lost response replays the
original approval result; it never renews its budget. Existing historical conversations use the
separate human membership-change path, not this initial-admission shortcut.

## Acceptance, ingestion and dispatch

An enabled binding has enabled enrollment and a current unexpired credential; it need not have a
live socket. Before message acceptance, check the authenticated sender's exact current token and
verified host binding, enabled recipient, membership edge/expiry and recovery barriers in the same
writer transaction that inserts the message, provenance and operation result. Sender IDs and
trusted-reply flags supplied by callers are rejected. Fresh sends name one conversation and one
recipient. Identity success alone never supplies membership or permits broadcast.

Reply ingestion validates native source evidence/event identity, current connection, original
provenance and membership, then commits original ACK, reply/provenance, cursor/result evidence
atomically. The reply carries the actual accepting credential version, even across later grant
carry. Different content for an already-recorded native event is `event_conflict`, not a second
reply. Ordinary no-marker and terminal malformed events can advance a cursor only with durable
classification and no ACK/reply; a valid event waiting for handoff, or a transiently unavailable
binding, cannot be skipped. Persist a pending event reference and retry after the relevant state
changes; do not hold the writer while waiting. Cursor advancement must not jump over unresolved
source events. Reconnect never invents new native event IDs for previously observed turns.

Check both required bindings, hold absence, exact grant/version/edge/expiry/budget and recipient
readiness immediately before a dispatch claim in the writer transaction. The original sender may
be offline after acceptance. An unavailable binding or held work defers without consuming budget
or changing delivery state; independent grant lifecycle still applies. Bind the claim to the exact
recipient connection token. Never redirect a claimed attempt to a new transport after reconnect.
Cancellation after host startup is cooperative; preserve exact attempt-token settlement and
conservative uncertainty. Identity revocation cannot erase evidence or enable automatic retries.

## Revocation and recovery holds

`binding.revoke` is human-only, requires expected binding/credential versions and commits disabled
enrollment, revoked current credential and holds on all outstanding authored work across all its
credential versions atomically. Increment binding version and cancel the active connection context.
Work includes pending creation/join requests and never-attempted envelopes/replies. An in-flight
attempt retains provenance and the incident so a later never-attempted requeue/carry is also held.
No subsequent rotation, renewal or re-enrollment clears these holds. Honest work addressed to the
revoked recipient waits for enabled recipient access; it is not attributed to the recipient.

Each human `hold.disposition` names one work item, incident and expected hold state/version, with
`cancel` or `release` and a bounded reason. Clearing one incident cannot clear another. Release
still requires current authorization at eventual use; it cannot revive terminal requests/messages,
refund budgets, reset an original ACK or replay an uncertain host attempt. Cancelling held work
preserves evidence; grant renewal carries eligible replies only with their holds/provenance intact.
Late never-attempted settlement refunds once under its exact attempt token and preserves the hold.

Revocation also creates an ingestion recovery barrier for that binding. Events not durably accepted
before revocation cannot be laundered through a fresh credential after re-enrollment. Until a human
reviews the source interval and establishes an audited resume cursor or explicitly held event set,
reply ingestion stays blocked, even if new sends become authorized. Preserve rejected/held interval
identities so later replay cannot bypass that disposition. No invented cursor advancement may ACK
an original. The host integration must fail closed when it cannot identify that interval reliably.

Routine expiry or rotation without revocation does not declare prior work compromised. It disables
old authentication/readiness, with no new security hold; existing holds/barriers still apply.
Retirement permanently disables a binding and applies revocation semantics. Fresh native sessions
require new bindings and human membership changes; no automatic credential or inbox handover.

## Operation replay and errors

All ordinary mutations require a client-generated operation ID. Scope deduplication to the stable
principal and ID, independently of credential/connection generations; include the operation kind
in the checked request digest. Canonicalization sorts object fields and contract-defined unordered
sets, preserves exact string bytes, rejects duplicate fields and distinguishes absent/null. It
never normalizes identifiers or hashes a bearer secret into retained request metadata. Record the
result atomically with the effect. Same ID/different operation or content returns `operation_conflict`.

Same-ID/same-request retries first reauthenticate/currently authorize access to the result, then
return the original result without rerunning the mutation. The result describes what committed,
not a promise the resource is still active. Revoked/inaccessible callers cannot retrieve private
results. Authentication/inspection/heartbeat/readiness use their connection-specific rules rather
than the ordinary operation-result mechanism; repeated probes cannot reset a deadline implicitly.

Keep operation IDs/digests/results and native-event deduplication for the installation's lifetime;
there is no automatic TTL eviction. Human retirement may remove sensitive payloads only while
preserving permanent replay tombstones and terminal result metadata. If storage capacity is
exhausted, reject new mutations with `capacity_exceeded`; do not discard replay protection to make
space. Queries are bounded and indexed. Backup/restore must preserve this state or invoke recovery.

| Error | Meaning and retry rule |
| --- | --- |
| `invalid_request` | Invalid shape, ID, bound, counter or deadline; correct before retrying |
| `authentication_failed` | Generic pre-principal credential/UID failure; close socket, no identity enumeration |
| `not_found` | Missing or invisible discovery/request resource; same outward response |
| `forbidden` | Authenticated caller lacks an operation capability; endpoint naming cannot elevate it |
| `identity_conflict` | Binding tuple or peer ID is already registered; use the existing binding lifecycle, never create an alias |
| `binding_unavailable`, `host_unverified`, `not_ready` | Required eligibility absent; no acceptance/claim; wait for explicit state change |
| `already_connected`, `generation_conflict`, `version_conflict` | Concurrent state changed; refresh permitted state, never force takeover |
| `request_expired`, `request_terminal` | No new admission effect; request must not be revived |
| `operation_conflict`, `event_conflict` | Reused identity with different content; no second effect |
| `security_hold`, `recovery_required` | Human disposition needed; rotation or ordinary retry cannot bypass it |
| `capacity_exceeded`, `temporarily_unavailable` | No new committed effect; bounded retry only; replay same ID if commit outcome is unknown |
| `outcome_unknown` | Response/commit evidence unavailable to the caller; inspect/replay the same ID, never infer failure or send a new mutation |

Errors carry fixed code, safe summary and optional authorized current version/retry delay. They
never contain raw transport errors, secrets, bodies, paths or hidden peer details. Membership and
budget errors retain their own semantics; identity errors cannot relabel uncertain delivery as
never attempted. Cancellation before commit rolls back all effects. After commit, response loss
leaves durable results; after host handoff, existing settlement rules govern external uncertainty.

## Migration, startup and database restoration

Use the next ordered `user_version` step at implementation time; do not reserve a version number
in this draft. Add binding/credential, pending-request, provenance, hold, replay and ingestion
records with unique/FK/CHECK constraints and indexes matching the lookups above. At most one current
credential per binding and one waiting request per exact conversation name must be schema-enforced.
Do not create room membership tables or change frozen historical-adoption SQL. Existing envelope
state values, grants, attempt tokens and numeric ordering remain authoritative.

Migrate under the accepted exclusive server lock and one immediate writer transaction before
readers/listeners. Rollback failed validation completely; rerunning a completed migration is a
no-op. Existing peer names do not establish bindings. Mark existing work as legacy/unprovenanced;
never infer credential provenance from the grant or current enrollment. Hold it pending human
review/retirement before enabling affected legacy access. Human-reviewed release retains explicit
legacy provenance and cannot manufacture trusted replies, ACKs or successful host attempts.

Ordinary restart creates a new epoch, drops all live slots/readiness, preserves durable generations,
operation/event evidence and holds, and applies existing interrupted-dispatch recovery. It does not
renew credentials, grants or pending deadlines. Service startup uses the accepted server locking,
writer/read-pool and backup ownership contracts in Architecture.

Before starting from any restored database, human restore procedure establishes a trusted startup
recovery marker outside the restored database. Its existence blocks ordinary agent admission,
request/approval operations, dispatch and ingestion. Only trusted human recovery inspection and
disposition are available. Repeated restart, new epoch or credential rotation cannot remove it.
There is no claim of automatic detection for a restore that bypasses this procedure.

Reconcile against surviving host and operational evidence: credential/revocation history, security
holds and ingestion barriers, operation results/tombstones, event identities/cursors, grant versions
and budgets, envelope states and attempt tokens. Unknown external outcomes stay uncertain/held;
never replay delivery, advance an ACK or replenish a budget from the old snapshot. Where missing
operation IDs/events cannot be enumerated reliably, retire the affected principal's mutation and
ingestion namespace permanently; do not reopen it with an empty deduplication history. Explicit
fresh native-session registration/admission may establish new authorized work without claiming old effects undone.

Recovery completion is human-only and two-phase: commit audited reconciled/retired state and all
remaining item/principal holds with a unique recovery incident ID, then remove the external marker
only after matching that durable record. A crash before removal remains globally held; a missing or
mismatched record refuses release. Unresolved namespaces stay disabled after the global gate opens.
Operators must re-establish the external marker before every subsequent restore, even of a snapshot
containing a prior completion record. No agent operation can complete recovery or delete tombstones.

## Required controlled fixtures

These are executable-test requirements for later implementation, not tests claimed to exist.
Use temporary file-backed databases, synthetic credentials, controlled sockets/host verifiers and
injectable time. Never use real credentials or invoke the protected executable from an agent.

| Fixture | Setup/action | Required result |
| --- | --- | --- |
| C01 Registration | Register a synthetic tuple twice; try same peer with new tuple | One binding, one credential, no grant; duplicate rejected |
| C02 Publication | Fail/crash before and after file publication, then replay/rotate | No secret output or old-secret fallback; recovery preserves version checks |
| C03 Authentication | Wrong/expired/revoked credential, wrong UID/server/tuple; arbitrary sender/admin fields | No substituted principal, secret disclosure, ACK, grant or host call |
| C04 Reconnect | Race two sockets; replay auth on winner; lose committed response and socket | One live slot; inspect then CAS recovery; no budget reset or evicted winner |
| C05 Stale callbacks | Old readiness ACK, heartbeat, close and liveness timeout after reconnect/restart | Cannot ready, prolong or clear the new slot or mutate work |
| C06 Host verification | Matching, unavailable, forged tuple and unsupported native evidence | Only verified match may ready/send/ingest; source paths cannot establish identity |
| C07 Discovery | Two waiting conversations, hidden/invited/advertised audiences, guessed ID and expired cursor | Explicit selection, bounded permitted metadata, no private content/candidate leak |
| C08 Admission race | Two candidates, approval vs withdrawal/expiry/revocation, response loss | Exactly one actual pair or no pair; no placeholder, partial grant or repeated budget |
| C09 Presence | Offline enrolled member; offline waiting creator; same native session in several conversations | No vacant full-conversation slot; eligibility separate from online/readiness |
| C10 Reply ingestion | Duplicate/conflicting event, malformed marker, event before handoff, interrupted commit | Atomic ACK/reply/cursor; no lost pending event or duplicate effect |
| C11 Revocation | Stolen credential queues while recipient offline; revoke, rotate, re-enroll and renew | All authored work across versions held; no restored dispatch/stale admission |
| C12 Late settlement | Revoke during claim/handoff; never-attempted result arrives after recovery | Exact refund once, no retarget, hold/provenance survives carry, original ACK preserved |
| C13 Revoked source interval | Unaccepted host event from compromised interval, then fresh credential ingestion | Barrier holds event until audited disposition; no credential laundering |
| C14 Replay retention | Duplicate/conflicting operations after reconnect/restart/credential rotation; payload purge | One effect; tombstones reject replay; current authorization required for results |
| C15 Migration | Known legacy DB, incompatible IDs, injected migration failure; rerun | Atomic schema upgrade, no synthesized provenance, reviewed legacy access, no room tables |
| C16 Restore | Snapshot before delivery/ACK/approval/revoke/budget use; rotate/restart and lose recovery response | External hold persists; no replay/replenishment; unknown namespaces retired/held; audited release only |
| C17 Limits and time | Boundary TTL/size/counter, expiry at equality, clock rollback, storage exhaustion | Explicit rejection; detected clock rollback blocks authorization; persisted terminal expiry and replay evidence survive restart |
| C18 Isolation limits | Same-UID stolen credential and compromised verifier; cross-UID secret possession | Demonstrate accepted same-account limit; wrong UID rejected without claiming production validation |

Acceptance of this specification requires review of these records and transitions against the
identity and membership decisions. Runtime implementation must supply passing applicable fixtures;
connecting live hosts additionally requires the repository's full live-connection evidence gate.

## Evidence basis

Baseline: `9cc3a5a795fdc48989ab08e6a527f5d4b6c01457`. Current
[dispatch](../../internal/dispatch/dispatch.go) accepts a caller-supplied sender and separately
claims/settles delivery; [ingestion](../../internal/adapter/codex/ingest.go) atomically validates,
ACKs and queues replies; [readiness](../../internal/adapter/claude/handshake.go) uses process-local
generations. The [store migrations](../../internal/store/migrations.go) own ordered upgrades.
This specification extends those boundaries; it does not describe them as already authenticated.

Linux [Unix socket documentation](https://man7.org/linux/man-pages/man7/unix.7.html) defines
`SO_PEERCRED` and pathname permissions; they identify an account, not a native host session.
Go [crypto/rand](https://pkg.go.dev/crypto/rand#Read) supplies cryptographic randomness.
The accepted identity decision records the bearer-credential rationale and protection limits.

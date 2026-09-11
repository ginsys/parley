# Two-peer follow-up specification

Proposal for [issue #34](https://github.com/ginsys/parley/issues/34), refining the inbox use case in
[issue #6](https://github.com/ginsys/parley/issues/6). This document needs owner review; it does not
establish accepted policy or implement an inbox. GitHub owns scope, acceptance and completion.

## A conversation with two people

Alice asks Bob to review a patch in conversation `auth-review`, marking that envelope actionable.
The accepted send creates one linked follow-up assigned to Bob. Transport acknowledgement means
Bob's host acknowledged the message; the follow-up remains open. A reply also leaves it open.

At his next work checkpoint Bob reads a compact inbox summary, retrieves the original by ID, and
explicitly resolves the item when he considers the review handled. This records Bob's disposition;
it does not prove that code ran, a patch was correct, or a human approved anything. Alice can
inspect that disposition while their current conversation permissions allow the return of status.

If Bob needs more time, he defers the item with a reason code and a finite deadline. It stays in the
outstanding list under `deferred`, returning to `open` when due. If Alice needs to take it over, Bob
offers assignment to Alice; Bob remains responsible until Alice explicitly accepts. Neither this
transfer nor resolving an item sends or executes the original instructions again.

Alice and Bob may also have another conversation, `release-review`. Inbox queries always name the
exact conversation; they do not select a room based on a resumed host session or merge its content
with `auth-review`. A new host session is a new binding and needs separate admission; it does not
inherit Bob's inbox by choosing Bob's display name. No third participant is needed or supported
here.

## Proposed decisions and alternatives

| Question | Proposed rule and reason | Alternative not selected in this draft |
| --- | --- | --- |
| What creates work? | Explicit `actionable: true` on an accepted ordinary send; default false. One follow-up per original prevents duplicate work from re-ingestion. | Inferring requests from prose or creating tasks for every message introduces guesses and noise. |
| Who finishes it? | The current assignee explicitly resolves; sender cannot silently finish someone else's work. | Treating acknowledgement or a reply as completion confuses receipt with disposition. |
| What does deferral mean? | Still outstanding, with reason and finite return time; it is not a terminal success. | Hiding deferred work permanently risks losing it again. |
| How does assignment work? | Offer and explicit acceptance between the original two peers, with both communication directions currently allowed. | Unilateral reassignment permits dumping work or treating an offline peer as consenting. |
| When are agents notified? | Pull a compact summary at a supported checkpoint/resume or explicit check; blockers rank first. No timer wakes or busy-turn interruption in this milestone. | Immediate blocker wake requires settled host compatibility and a separate owner-approved interruption policy. |
| Do notices contain instructions? | Only server-generated IDs, counts, state and age; fetch the original explicitly. | Replaying bodies as reminders can duplicate apparent instructions and spend delivery budget. |

These defaults are proposed together for review, particularly checkpoint-only blocker handling. They
do not authorize execution, create a grant, change host permissions or replenish exchanges. Retained
conversation context remains a separate [feature](https://github.com/ginsys/parley/issues/42);
receiving it cannot resolve these follow-ups.

## Logical data contracts

Use the server-owned SQLite database and ordered atomic schema upgrades. These are logical
contracts, not SQL or a chosen migration version. Keep the current two-peer grant representation; no
room membership migration is necessary. IDs are canonical lowercase UUIDs; exact conversation and
peer keys follow the [membership contract](membership.md#accepted-ascii-identifier-rule). Times use
server UTC instants and numeric nanosecond ordering with checked range arithmetic. Counters are
positive signed 64-bit integers, checked before increment. No implicit trimming.

| Record | Fields and constraints |
| --- | --- |
| Follow-up | `follow_up_id`; unique `envelope_id` FK; immutable `conversation`, `original_sender`, `original_recipient`, `accepted_grant_version`; `assignee` initially recipient, restricted to the original pair; `disposition` = `open`, `deferred`, `resolved`; positive `version`; `priority` = `routine`, `blocker`; `created_at`, `updated_at`; nullable `defer_reason`, `defer_until`, `resolution_code` |
| Assignment offer | Unique `offer_id`, follow-up FK, `from_peer`, `to_peer`, creation and finite expiry; `pending`, `accepted`, `declined`, `cancelled`, `expired`; positive version; at most one pending offer per item; two distinct original peers only |
| Disposition event | Unique `event_id`, follow-up FK, actor binding/credential provenance or tagged server timer, previous/new item version, operation kind, reason/result code, server time; append-only; no original message body |
| Mutation receipt | Unique `(binding_id, operation_id)`, method, canonical typed request digest and committed result; immutable; no credentials or original text; durable with its mutation |
| Checkpoint receipt | Unique `(binding_id, checkpoint_id)`, exact conversation, caller's last-seen revision, committed response metadata and response revision; immutable; no message bodies |
| Inbox revision | Monotonic revision per `(conversation, binding_id, view)`; advance only when that authorized projection changes, including access invalidation; no global revision exposed as a substitute and no promise of a durable event stream |

Store checks enforce disjoint disposition fields: only deferred rows have both defer fields; only
resolved rows have `resolution_code`. Offer source must equal the current assignee when created;
acceptance verifies it again transactionally. FKs and immutable original provenance survive grant
renewal even if the envelope's delivery grant version changes. Never use a display name as identity.

Follow-up creation and envelope acceptance commit together with authenticating provenance and the
send's idempotency record. Reject malformed metadata before creating either. Supported metadata is
`actionable` boolean and, only when true, `priority` (`routine` default or `blocker`). A client
cannot supply assignee, disposition, original peers or acceptance provenance. Trusted reply
ingestion does not infer actionability from reply prose or change the reply-marker grammar; an
ordinary explicit send is the only creation path in this milestone. Historic envelopes receive no
guessed follow-ups during migration. Schema failures roll back without modifying historical delivery
evidence.

Retain records, receipts and disposition events for the lifetime of their envelope history; no
automatic purge is specified. They contain no extra free-form task text. Database restore must
reconcile them along with delivery, grant, security-hold and replay state before ordinary operation.
A new process epoch or credential must not make an old operation ID executable again.

## Delivery and disposition are separate

The existing [envelope states](../../internal/store/types.go) remain unchanged. Present both axes
and eligibility in every item response; do not turn a transport error into `resolved`.

| Delivery evidence | Follow-up behavior |
| --- | --- |
| `queued` or `dispatching` | Record exists, reported as `awaiting_delivery`; no resolution, deferral or assignment yet. |
| `handed_off` or `acked` | Eligible for explicit disposition if authorization and holds allow; acknowledgement and replies never change disposition automatically. |
| `uncertain` | Report `delivery_uncertain`; no inferred receipt or automatic retry. Recipient can explicitly retrieve and disposition the item, recording that delivery was uncertain. This never acknowledges or retries its envelope. |
| `failed` or `cancelled` before handoff | Report `delivery_blocked`, preserve the open record; exclude from actionable reminders. No recipient disposition or implicit retry. Administrator inspection retains it as undelivered work. |

Eligibility is a projection, not another disposition: security/recovery holds override delivery, and
invalid current access denies the operation. Sender inspection can expose their own undelivered
items under current access. The recipient's actionable list contains eligible items only; an
explicit authorized detail read may show the blocked record and its diagnostic without granting
permission to process it. A denied read returns no diagnostic revealing a hidden item's existence.

## Operations and lifecycle

Names below are logical agent operations; control transport framing and host-specific tool wiring
are separate contracts. Every request rejects unknown fields and invalid tagged values. Derive actor
from the authenticated live connection; no caller-selected actor field. Mutation requests carry
`operation_id`, `follow_up_id` and exact `expected_version`; accept/decline/cancel operations
additionally carry exact `offer_id` and `expected_offer_version`. Creating an offer allocates its ID
and starts its version at one; every offer state change increments that version. No storage
transaction spans host I/O.

| Operation | Transition and required fields |
| --- | --- |
| `follow_up.resolve` | Current assignee changes eligible open/deferred to resolved; `resolution_code` = `handled`, `declined` or `no_longer_needed`. Cancel any pending offer atomically. |
| `follow_up.defer` | Current assignee changes eligible open/deferred to deferred; `reason` = `waiting_for_peer`, `waiting_for_human`, `waiting_for_external` or `scheduled`; server-valid `until` strictly in the future and at most 30 days away. Cancel any pending offer. |
| `follow_up.reopen` | Current assignee explicitly returns resolved/deferred to open, clearing disposition fields; cancel any pending offer. No delivery state or message changes. |
| `follow_up.offer_assignment` | Current assignee of eligible open item names the other original peer and `expires_at`, after now and at most 24 hours away. Responsibility stays put. Existing pending offer rejects. |
| `follow_up.accept_assignment` | Named target accepts a live pending offer on the exact open item/version; both directions must still be allowed. Change assignee, mark offer accepted, leave item open; no new envelope. |
| `follow_up.decline_assignment` | Named target marks live pending offer declined; assignee unchanged. |
| `follow_up.cancel_assignment` | Current assignee marks live pending offer cancelled; assignee unchanged. |

Every successful operation increments the item version and affected visible inbox revisions,
including offer-only changes, and appends an event in the same immediate transaction as its receipt.
Competing resolve, accept, defer and revoke therefore have one serialized outcome, never two
successful dispositions at the same expected version. At `now >= expires_at`, offer acceptance
fails; a writer expires the offer, increments versions and records a server event. At `now >=
defer_until`, the writer reopens the item and clears defer fields. Deadline processing never calls a
host or invents an agent actor. Clock/restore recovery barriers prevent ordinary timer mutation
until reconciled.

Process due deadlines before reading or mutating the affected item. A bounded checkpoint may advance
at most 100 due rows; if more remain it returns `refresh_required`, no complete-summary claim, and
the next check continues. Due indexes and writer batches must avoid a database-wide scan. Offer
expiry and deferral are observed durably; backward wall time cannot undo recorded transitions. All
return-time calculations use trusted server time, not client timestamps.

Authenticate and authorize before receipt lookup; a currently revoked peer cannot retrieve an old
success. A same-principal, same-operation, identical typed request returns its committed result
without repeating a mutation, even if its expected version is now old. A different method or payload
with the same ID fails `operation_conflict`. New IDs with stale versions fail `version_conflict`.
Validation/authorization failures before mutation need not reserve an operation ID. A lost response
after commit is recovered by repeating the same request; a rolled-back transaction changed nothing.
Replayed results report their original versions, never claim to be current snapshots.

## Permissions, direction and revocation

Access requires a current authenticated binding/credential/connection, no recovery barrier, an
active unexpired grant with the exact original pair, and no applicable held-work restriction.
Historical access also requires an uninterrupted authorized original sender-to-recipient edge across
all versions since acceptance, with no revoke/re-enrollment boundary. Current pair equality alone
does not restore access to old work. Normal renewal preserving that edge retains access; replacing
either peer does not transfer history to a new binding.

Projection revisions, cursors and checkpoint suppression must not reveal hidden activity: a private
Bob disposition under a one-way grant does not advance any Alice-visible revision or invalidate an
Alice cursor. Each affected projection is updated atomically with the underlying mutation.

The original recipient may read the original and their inbox under the original edge. Returning any
recipient disposition, offer response or inbox-derived information to the original sender requires
the reverse edge too. With a one-way Alice-to-Bob grant, Bob can privately resolve or defer; Alice
gets no follow-up status, version, timing, counts or receipt revealing Bob's actions. Alice's own
original send/delivery evidence remains governed by its separate existing contract. A later reverse
edge may enable status reads only if all other historical-access checks still pass.

Only the assignee changes disposition; only the named offer target accepts/declines. Assignment
requires both edges at offer and acceptance, and subsequent access is always rechecked. A sender
cannot change priority, withdraw or resolve the item after acceptance unless it has been explicitly
assigned to them. A `blocker` flag ranks work; it never elevates permissions or forces attention.
All metadata is untrusted even when authenticated. Free-form notes and proof of work are excluded;
additional discussion goes through ordinary authorized messages.

Budget exhaustion stops new host delivery but does not block authorized explicit inbox reads or
fixed-code dispositions: these operations neither enqueue messages nor invoke hosts. Direction,
expiry, revocation and security holds still apply to reads, receipt replay and mutations. No
follow-up mutation resets budget or holds, releases a quarantined legacy envelope, acknowledges a
message, grants execution authority or substitutes for the protected controller.

Revocation racing with disposition is serialized in the writer: a committed-before-revoke change
remains historical evidence; a later change fails without mutation. Revocation also suppresses
future peer reads and checkpoint receipts. Recheck authorization immediately before starting a
read/receipt response and discard it if access was lost; do not hold a writer transaction during
socket output. Revocation can still race after that final check, so already returned or concurrently
transmitting data cannot be recalled. Outstanding records survive; authenticated human
administration may inspect recovery evidence under its own capabilities but cannot manufacture an
agent's `handled` result. Binding-revocation authored-work holds cover follow-ups and offers as well
as originals, across rotations and late settlement.

## Bounded inbox reads and checkpoint notices

`inbox.list` requires exact `conversation`, `view` (`assigned`, `sent` or `offers`), optional
`disposition` filter and page size 1–100 (default 20). Derive the peer from authentication and
enforce the permissions above before pagination. Assigned lists default to all outstanding eligible
open and deferred items; offers list only live incoming offers; sent lists include original-sender
items only when status disclosure is authorized. Resolved items require an explicit filter. Never
leak items, counts or cursors from another conversation or peer.

Rows contain IDs, original sender/recipient, assignee, priority, disposition, item/offer versions,
server creation time, age, defer deadline/reason, offer expiry and delivery/hold eligibility. They
exclude body text. Sort blockers first, then numeric creation time, then follow-up ID; deferred
items are labelled and counted separately, never reported as resolved. List responses include server
time and the selected view's scoped revision. An opaque authenticated cursor binds principal,
conversation, view/filter, revision, last sort key and a five-minute deadline. Mutation or
authorization change invalidates further pages with `refresh_required`; never silently mix
revisions. Server restart invalidates cursors; authoritative records persist. Bound each read to
five seconds and close its read transaction before writing a response; use the server's separate
reader pool.

`follow_up.get` returns one authorized record; `include_original` defaults false. Setting it true
returns the original content once within the existing message size limit, clearly wrapped as
untrusted data. Retrieval is read-only: it is not receipt acknowledgement, task acceptance or a
request to execute anything. Recipient retrieval of the body requires existing `handed_off`, `acked`
or `uncertain` evidence from a charged host attempt; queued, dispatching, failed or cancelled
originals cannot be read this way to bypass delivery accounting/readiness. Original senders already
know their own content but gain no additional recipient metadata through that exception. Retrieval
never spawns a process or sends the original into another host.

`inbox.checkpoint` takes exact conversation, caller-generated UUID `checkpoint_id` and nullable
`last_seen_revision` (an opaque tuple of assigned and incoming-offers projection revisions). A host
adapter may call it only at a checkpoint or resume already supported by its host contract; the
server does not detect arbitrary work completion or compaction. Explicit checks work identically.
One call returns at most one notice, containing counts (open, deferred, incoming offers), up to 10
distinct item IDs from eligible open assignments and incoming offers, sorted blockers first then
creation time/ID, and `more`; no prose from messages, reason notes or sender-supplied titles. Query
at most 101 rows per category, report counts over 100 as `100+`, not an exact total. A response with
no outstanding work has no notice.

If the supplied revision equals the current scoped revision, return `unchanged` with no reminder. A
null revision requests an authoritative reminder after local state loss; it still returns one
bounded response and never queues a wake. Persist response metadata and checkpoint receipt with the
observed revision in one writer transaction after the bounded due pass; no host I/O in it. Reusing a
checkpoint ID with identical arguments returns that original response, marked replayed; changed
arguments conflict. Current authorization precedes any replay. A replay is a past summary; use a new
ID to refresh. An adapter records consumed checkpoint IDs durably and must not knowingly render the
same response twice. A crash between rendering and recording can repeat the metadata notice; it
cannot guarantee exactly-once display. Original instructions are never embedded.

Limit inbox requests to one in flight per binding, list/detail to 60 requests/minute and checkpoints
to six/minute. On startup give each binding zero tokens, refill uniformly to those capacities;
reconnect shares the binding's bucket and does not refill it. Return `rate_limited` with bounded
retry delay before database work. There is no periodic reminder timer, automatic host enqueue,
notification exchange charge, or claimed maximum blocker-response latency. A busy agent may not see
a blocker until its next checkpoint; that is the explicit proposed tradeoff. Budgeted ordinary
message delivery and host readiness keep their existing independent rules.

## Errors and implementation fixtures

Stable codes: `invalid_request`, `unauthenticated`, `not_found_or_forbidden`, `version_conflict`,
`operation_conflict`, `invalid_transition`, `delivery_not_eligible`, `held`, `offer_expired`,
`refresh_required`, `rate_limited`, `recovery_required`, `busy`, `counter_exhausted`. Check
visibility before item-specific codes; errors and logs contain neither message bodies nor secrets.
Transport mapping must preserve these meanings; the admin control protocol cannot turn an agent into
a human.

These are future implementation fixtures, not tests supplied by this documentation change. Use
synthetic SQLite files, controllable time, authenticated synthetic peers and fake host callbacks.
Every case asserts persisted rows/versions/events, visible projections and zero unexpected host
calls; run the repository's full `mise run verify` for each implementation increment.

| ID | Input or interruption | Required result |
| --- | --- | --- |
| F01 | Ordinary send, actionable send, malformed flags, duplicate accepted send | Zero/one follow-up as requested; malformed metadata creates neither record; duplicate creates one item and original only. |
| F02 | Original transitions queued → dispatching → handed_off → acked; normal/trusted reply arrives | Eligibility changes; disposition stays open; ingestion cannot create or finish a follow-up from prose. |
| F03 | Assignee resolves/defer/reopens; sender/third peer tries same; inspect one-way grant | Correct versioned transitions and fixed codes; unauthorized changes fail; sender learns no recipient state, revision change or cursor invalidation on a one-way edge. |
| F04 | Deferral at boundary, 101 due items, clock rollback/restart | Due items durably reopen in bounded passes; incomplete summary signals refresh; observed transitions never reverse. |
| F05 | Offer, decline, cancel, accept; simultaneous accept/resolve; expired offer | Responsibility stays until exact-version acceptance; one winner; no third peer or self-transfer; no replayed original. |
| F06 | Lost mutation response; repeated operation ID with identical/different arguments; crash before commit | Exactly one committed change or no change; conflict on changed request; original receipt marked historical. |
| F07 | Two conversations with same peers; another authenticated peer guesses IDs/cursors | Explicit scoping; no cross-conversation data/count leak; generic inaccessible response. |
| F08 | Renewal preserving original edge, removal, revoke/re-enroll, binding rotation/revocation | Preserved authorized history only; no history inheritance or hold bypass; revoked actor cannot replay receipts. |
| F09 | Revoke vs resolve/read/checkpoint, and expiry at the operation boundary | Serialized mutation outcome; responses after authorization loss suppressed when observed; already returned data cannot be recalled. |
| F10 | Exhausted budget, unready host, empty and nonempty inbox, blocker while busy | Explicit authorized reads/disposition still work; no host calls, timer wake or budget mutation; blockers wait for a check. |
| F11 | Burst of 250 items, repeated checkpoint, lost response and crash after rendering | One bounded metadata response, capped counts/10 IDs; repeated ID does not create another receipt; possible duplicate display documented, no instruction replay. |
| F12 | Paginated listing with concurrent disposition, authorization change, restart and cursor tampering | Stable page or explicit refresh/rejection; bounded read lifetime; no mixed or unauthorized pages. |
| F13 | Unknown fields, oversized/invalid IDs, counter/time overflow, invalid actor/offer FKs | Fail without partial rows, receipt or event; database constraints and transaction checks both exercised. |
| F14 | Failed/cancelled/uncertain original; migration and restore of old data | No invented completion, receipt or retry, and no pre-handoff body retrieval bypass; blocked work remains evidence; historic envelopes not auto-classified; restore reconciles receipts and holds. |
| F15 | Reconnect/restart and repeated checks over configured rates; missing/stale checkpoint revision | Shared binding limits and cold-start refill hold; unchanged revision suppresses reminders; null revision yields only one bounded notice. |

Owner review must assess the two-peer walkthrough, explicit actionability, transfer acceptance,
post-revoke visibility and checkpoint-only blocker policy. This does not solve live wake delivery,
transcript summarization, retained initialization, automatic task execution, work verification,
multi-agent rooms or historical data transfer to replacement sessions.
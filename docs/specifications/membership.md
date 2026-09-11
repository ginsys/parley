# Membership and authorization specification

Draft for [#20](https://github.com/ginsys/parley/issues/20), grounded in the owner's accepted
[membership decision](../architecture.md#accepted-membership-model). The model and migration
**timing** and the ASCII identifier rule below are accepted; other concrete contracts remain
proposed for specification review.
This document adds no runtime or schema implementation. GitHub owns acceptance and dependencies.

## Scope and implementation stages

| Stage | Public membership representation | Durable representation |
| --- | --- | --- |
| First runtime and two-peer inbox | Members and policy, using the supported subset below | Existing `grants.peer_a_id`, `peer_b_id`, `direction`; no members table |
| Later rooms | Same representation, expanded capabilities | Versioned members/policies/edges, migrated after the inbox |

The first stage must not create shadow membership state in a side table, file or memory-only
cache. The representation must round-trip across restart using the existing pair columns.
The room migration, not runtime ownership or control-client work, creates/backfills membership
tables and retires positional storage. No simultaneous pair and room writers are supported.

This specification covers membership, allowed communication, grant lifecycle, routing and
historical migration. It does not choose transport/framing, connection authentication, credential
enrollment, event subscriptions, request deduplication, inbox dispositions or broadcast behavior.
Those contracts consume this model. An authenticated principal is an input from the identity
binding layer, never a claimed sender field. Membership administration remains human-controlled;
message delivery never grants execution authority.

## Data contract

The logical object shape is transport-independent; JSON below illustrates names and structure,
not an approved JSON-RPC method. Integer encoding, frame limits and transport error envelopes
belong to the control specification. Grant versions and budgets are signed 64-bit integers with
positive maxima/versions; fractional, overflowing and negative values are invalid.

```json
{
  "conversation": "synthetic-room",
  "members": [
    {"peer_id": "fixture-a", "role": "member"},
    {"peer_id": "fixture-b", "role": "member"}
  ],
  "policy": {"kind": "directed", "edges": [{"from": "fixture-a", "to": "fixture-b"}]}
}
```

`conversation` and `peer_id` are opaque exact identifiers restricted to printable ASCII bytes
`0x20` through `0x7E`, inclusive. Empty and space-only values are invalid; permitted spaces remain
part of the key. Never trim, case-fold, normalize or rewrite IDs. See the
[accepted identifier rule](#accepted-ascii-identifier-rule) for existing data.
Each member has exactly one role (`member` or `lead`) per grant version. Duplicate IDs, duplicate edges and edges
with endpoints outside membership are invalid. At least two distinct members are required;
removing the penultimate member requires revocation instead of activating a one-member grant.

A policy is a tagged union, with unknown tags/fields rejected rather than ignored:

- `open`: no `edges`; all roles are `member`; every distinct pair is allowed.
- `lead_only`: no `edges`; exactly one `lead`, others `member`; only lead/member edges in both
  directions are allowed. Adding/removing a member recalculates the allowed pairs from the policy.
- `directed`: explicit `edges` array; all roles are `member`; only listed ordered pairs are
  allowed. An empty edge set permits no sends. Member addition never creates an edge implicitly.

Roles outside their policy's allowed shape are rejected, not silently discarded. Self-send is
always rejected, regardless of policy or lifecycle. Responses order members by exact identifier bytes,
and edges by `(from, to)` using the same order. This canonical output order does not change IDs or
rewrite legacy A/B positions. Request array ordering has no authorization meaning.

A grant snapshot adds server-owned `grant_version`, `status`, `max_exchanges`, `exchanges_used`,
`granted_at`, `expires_at`, `revoked_at` and `cancel_pending_replies`. Accounting/status may mutate;
membership, roles and policy are immutable within a version. Existing `revoked_at` also records
supersession time; preserve that historical meaning instead of treating every non-null value as
an actual revocation event. Status distinguishes revoked from superseded history.

## Accepted ASCII identifier rule

The owner approved printable ASCII for **both peer IDs and conversation names** on 2026-09-11.
Use a byte-range check (`0x20`–`0x7E`) and reject empty/space-only values. This excludes malformed
UTF-8 as well as valid non-ASCII names such as `José` and U+FFFD. Preserve permitted leading and
trailing spaces and punctuation exactly. Message bodies are unaffected by this identifier rule.

This simple restriction replaces the proposed Unicode identifier policy and encoded-ID recovery
API. The current validator can accept distinct malformed byte strings (`61FF`, `61FE`) that Go
JSON encodes identically. ASCII validation rejects both, including replacement characters produced
by a JSON decoder; no custom Unicode decoder or encoded public identity representation is needed.
Validate identifiers before storage, authorization, wrapping or public serialization.

For an existing database, first check stored conversation names and peer IDs as raw bytes against
this rule, across all historical versions and envelope endpoints. A read-only query/script with
escaped or hex diagnostics is sufficient; do not build a recovery service on speculation. No
inventory of an operator's database is claimed by this specification. If the check finds no
incompatible IDs, no identifier migration or recovery feature is needed.

Never rewrite or delete incompatible history automatically. Keep current exact-key human
revocation available; applying validation to enrollment/renewal must not strand that escape path.
Incompatible IDs must fail new authorization and text responses explicitly, without replacement
or silent omission. Existing attempts retain their settlement rules. If an inventory finds
incompatible history, record its disposition before moving that database behind a text-only
administration interface; this decision does not pre-authorize a new recovery API. Unaffected
grants remain usable. The later room backfill preserves historical bytes and still rejects the
independent membership/self-send/FK incompatibilities described below.

The implementation is tracked separately in #40. This document records an accepted target rule;
it does not claim that shipped code enforces ASCII today.

## First-runtime subset and exact translation

The first runtime accepts **exactly two `member` roles**, either `open` or `directed` with exactly
one edge. Other well-formed models return `unsupported_membership`; malformed models return
`invalid_membership`. Validation is completed before any durable mutation.

This deliberately rejects a two-member `lead_only` policy, a two-edge `directed` policy, and an
empty directed policy until rooms. They cannot round-trip through the existing pair columns
without losing policy/role intent. In particular, an explicit two-edge policy is not silently
converted to `open`, since adding a member would then mean something different. Accepting these
shapes earlier requires a reviewed specification change, not hidden supplemental storage.

| Existing Direction | External model | Exactly allowed sends |
| --- | --- | --- |
| `bidirectional` | `open` with A and B both members | A→B and B→A |
| `a_to_b` | `directed`, edge A→B | A→B only |
| `b_to_a` | `directed`, edge B→A | B→A only |

For newly created pair grants choose A/B in canonical member order, then derive `direction` from
the requested edge. For reads of existing grants, derive edges from their stored A/B positions
before sorting the output. Never infer the direction from the output order. Renewal without a
membership replacement copies the existing A/B fields exactly. Legacy CLI flags translate to
these objects; positional peer fields must not appear in the new public protocol.

Capabilities expose stage support (`max_members = 2` and the supported policy forms). Larger or
unsupported requests fail explicitly, without allocating a conversation/version, changing status,
cancelling messages or consuming budget. The capability/error transport representation is defined
by the control specification; this semantic contract does not select a wire protocol.

## Authorization and operation boundary

Define `AllowsEdge(members, policy, from, to)` independently of expiry, accounting and historical
status. It rejects self/nonmember edges, then applies the policy. Historical superseded grants
can therefore be inspected without falsely rejecting their edges because of lifecycle fields.

Ordinary acceptance and dispatch claim require the active grant, its unexpired expiry and an
allowed edge. A dispatch claim additionally requires the queued envelope's exact grant version
and available budget. Authorization and mutation run in the **same immediate writer transaction**;
no reader-pool snapshot may authorize a later write. A failed conditional budget update is
classified by re-reading current authorization/state, not assumed to mean exhaustion.

Logical administration operations (actual method names belong to the control specification):

| Operation | Required precondition | Atomic result |
| --- | --- | --- |
| Enroll/re-enroll | No active grant; expected latest version (0 if none) | New version `max(history)+1`, active, zero used budget |
| Renew | Exact expected active version | Same membership/policy; new version, budget/expiry as below |
| Replace membership/policy | Exact expected active version; complete valid replacement | New version with replacement; lifecycle rules below |
| Revoke | Exact expected active version | Mark revoked, cancel queued rows, report already dispatching/handed off |

A stale expected version causes `stale_grant_version` with no mutation. A concurrent create or
renewal cannot partially replace state. Version overflow fails without changes. New operations
preserve the existing one-active-grant invariant; mark the old grant superseded before inserting
its successor, within the same transaction. Message interfaces cannot invoke these operations.

Renewal/replacement retains the maximum and expiry when omitted (legacy budget zero translates
to omission), or accepts an explicit positive maximum and future expiry. It starts the successor
with zero exchanges used, as today. Retaining an already-expired expiry keeps delivery blocked;
explicitly supplying an expired timestamp is invalid. This contract adds no implicit clear-expiry
operation. Revocation and administrative errors do not refund attempted messages.

Request replay after an ambiguous network response is handled by the control protocol's durable
idempotency contract, before applying a new mutation. An identical body alone is not authority
to repeat a renewal and replenish a budget. This specification does not invent request IDs or
choose their persistence schema.

## Version changes, queued messages and budgets

Every member/role/policy change creates a new version. In that writer transaction:

1. Validate the full successor and expected active version; reject the whole operation if invalid.
2. Supersede the old active version and insert the new version/membership with zero used budget.
3. Carry eligible queued trusted replies to the successor before cancelling remaining old-version
   queued messages. Record cancellation/carry counts and already-dispatching/handed-off counts.
4. Commit all state together. Failure rolls back version, counters, membership and queue effects.

Ordinary old-version queued messages are cancelled, even if the same edge remains permitted.
Trusted replies carry by default because their originals are already acknowledged. A reply may
carry only with trusted ingestion provenance: the original is acknowledged, in the same
conversation and has exactly reversed sender/recipient. A caller-supplied `in_reply_to` is not
provenance. Member removal or loss of its edge cancels a reply durably and reports the cancellation;
the acknowledged original is not reset or replayed.

Crossing versions requires a contiguous readable history, no actual revocation, no successor
`cancel_pending_replies` flag, and the reply's edge permitted by **every crossed membership/policy
snapshot**, not just the final one. Thus remove-and-readd or deny-and-reallow cannot rescue an
in-flight old reply past an intervening denial. Ignore historical superseded status when evaluating
its edge; do not ignore an actual revoked boundary. Missing history fails closed. Expiry is not a
carry barrier: an otherwise eligible reply may wait queued under an expired successor, but claim
still requires a current unexpired grant. A reply cannot carry across a revocation.

Already-dispatching/handed-off messages cannot be recalled. Settlement must match its original
version, durable attempt token and `dispatching` state. A never-attempted result refunds only its
original grant once; if rescue to a successor is needed, re-evaluate the entire history above.
Ambiguous handoff remains uncertain and is never automatically replayed. A delayed result cannot
rewrite a newer attempt or reopen a cancelled reply.

There is one budget pool for the active conversation grant. Each successful dispatch claim,
including a reply claim, consumes one exchange; ACK alone does not. Budget exhaustion can leave
an accepted message queued. Preserve current refund/settlement rules. A chatty member can exhaust
the room budget for everyone, including the lead. There are no per-member quotas, reservations or
fairness guarantees. Only human administration may create a successor budget.

## Fresh sends and replies

Fresh send input is `(conversation, to, text, optional in_reply_to)`. Sender comes from the bound
principal. An ordinary send never gains trusted provenance from the optional reference.

Reply ingestion resolves the conversation from the globally unique original envelope ID; an
optional supplied conversation must match it, never override it. Require original recipient equal
to the authenticated respondent, explicit reply recipient equal to original sender, original state
`handed_off`, and current authorization of the reverse edge. Wrong recipient/replier/conversation,
unknown original or other final states reject without ACK or reply insertion. `dispatching`
returns retryable `delivery_pending`; durable ingestion must not advance its cursor past that event.
ACK and trusted reply insertion commit together, with the reply stamped under the current grant.
An older delivered original may anchor a newly authorized reply; this never revives a previously
cancelled queued reply. A duplicate original ACK fails its expected-state transition.

Fresh and reply messages have exactly one recipient; no implicit broadcast or topology fan-out.
Inboxes remain linked to envelope IDs, not an assumed opposite peer. Historical membership rows
remain available for provenance; whether a removed member may read or dispose of an inbox item
belongs to the inbox specification, not an automatic membership-to-inbox permission inference.

## Errors and rejection guarantees

Names below are logical error classes; transport codes/envelopes are defined elsewhere.

| Error | Examples | Mutation |
| --- | --- | --- |
| `invalid_membership` | Duplicate/self/nonmember edge, invalid role/tag/shape/identifier, fewer than two members | None |
| `incompatible_identifier` | Historical conversation/peer ID violates the ASCII rule | No new authorization or lossy response; current exact-key human revocation remains available |
| `unsupported_membership` | Valid model outside the first-runtime subset | None |
| `stale_grant_version` / `no_active_grant` / `already_active` | Failed operation precondition | None |
| `not_permitted` / `grant_expired` | Invalid acceptance edge or expired grant | No accepted message or budget claim |
| `budget_exhausted` | Authorized claim with no remaining exchanges | Envelope stays queued; no host attempt |
| `delivery_pending` | Reply original still dispatching | No ACK/reply; source event retained for retry |
| `wrong_recipient` / `wrong_replier` / `stale_reply` | Invalid reply provenance or state | No ACK/reply |
| `migration_incompatible` | Invalid history or constraints, unexpected dependent schema | Entire migration rolls back |

Claim-time authorization denial preserves current behavior: cancel an ordinary queued row;
a trusted reply whose only failure is expiry stays queued for renewal. Failed acceptance is not
claim-time cancellation. Cancellation and settlement use expected state/attempt checks.

## Room-stage storage and migration

These tables appear only after the two-peer inbox. Allocate the next numbered migration from the
schema actually shipped then; do not reserve version 5 now (the current core is version 4).

| Table | Key and required constraints |
| --- | --- |
| `grants` | Preserve `(conversation, grant_version)` PK with both columns explicitly NOT NULL, conversation FK, lifecycle/accounting fields and one-active partial unique index; replace positional fields with `policy_kind NOT NULL` and CHECK in the three allowed tags |
| `grant_members` | PK `(conversation, grant_version, peer_id)` with all three columns explicitly NOT NULL; FK to grants; role NOT NULL and CHECK in `member`, `lead` |
| `grant_edges` | PK `(conversation, grant_version, from_peer, to_peer)` with all four columns explicitly NOT NULL; two composite FKs to grant_members; `CHECK(from_peer <> to_peer)` with binary identifier comparison |
| `envelopes` | Preserve IDs as `id TEXT NOT NULL PRIMARY KEY`, all other existing fields and grant FK; add version-scoped sender/recipient FKs to grant_members as below, and `CHECK(from_peer <> to_peer)` |

All columns participating in these composite primary/foreign keys are explicitly NOT NULL,
including the retained envelope conversation/version/sender/recipient columns. Do not infer this
from PRIMARY KEY or CHECK: SQLite rowid tables allow null composite primary-key values, a null
child-key component skips the FK check, and a CHECK expression evaluating to NULL passes.
A single `TEXT PRIMARY KEY` also permits NULL in a rowid table: explicitly require non-null
envelope IDs for settlement and reply lookup. Reject legacy NULL IDs before copying, retaining
source evidence; never manufacture replacement IDs. Optional references such as `in_reply_to`
remain nullable.

Named policies have no stored edge rows. Cross-row rules (minimum membership, exactly one lead
for `lead_only`, policy-compatible roles/edges) are validated as a whole inside the insertion
transaction before activation, not claimed to be expressible by a row CHECK. Unique membership
prevents repeated-peer entries. Sender/recipient self-edge CHECKs belong on tables that actually
have those columns. Keep exact identifier validation at the application boundary as well.

Both envelope peers must reference membership in the envelope's own grant version:

```sql
FOREIGN KEY (conversation, grant_version, from_peer)
    REFERENCES grant_members(conversation, grant_version, peer_id),
FOREIGN KEY (conversation, grant_version, to_peer)
    REFERENCES grant_members(conversation, grant_version, peer_id)
```

These are immediate constraints with no cascading deletes or updates. Removing a member from a
successor does not invalidate envelopes under retained historical versions. At the current core
baseline, Send checks authorization before insertion; a rejected send leaves no envelope row.
Renewal preserves peers, and the specified carry predicate requires membership and the allowed
edge across every crossed snapshot. Insert the successor's members before carrying a reply's
`grant_version` forward, in the same transaction. The FKs enforce membership, not direction,
expiry or budget; transactional authorization remains necessary.

Legacy data still needs validation: [Send at pre-audit commit
6f75867](https://github.com/ginsys/parley/blob/6f75867f5e52427a8d4c2adc90a8564100060f94/internal/dispatch/dispatch.go#L94-L122)
could persist peers outside its stamped grant because it did not check authorization. That is
incompatible history, not a reason to omit the new FKs. An envelope peer missing from its own
version, a NULL envelope ID, a historical self-send or identical A/B members aborts the whole migration. Preserve the
source schema and evidence; do not invent membership, delete rows or rewrite IDs/versions to make
constraints pass. The operator must resolve incompatible history explicitly before retry.

### Preflight for an existing pair database

Before scheduling the stopped-service upgrade, open the existing database read-only, for example
with `sqlite3 -readonly /absolute/path/to/parley.db`, and run the query below. It reads all grant
versions and envelope states, not just the active grant or queued messages. Since `grant_members`
does not exist before the room migration, `expected_members` projects exactly the membership that
the backfill will create from each historical pair. It makes no persistent tables or changes.

```sql
WITH expected_members AS (
    SELECT conversation, grant_version, peer_a_id AS peer_id FROM grants
    UNION
    SELECT conversation, grant_version, peer_b_id AS peer_id FROM grants
), findings AS (
    SELECT 'sender_missing' AS diagnostic, e.conversation, e.grant_version,
           e.id AS envelope_id
    FROM envelopes AS e
    WHERE NOT EXISTS (
        SELECT 1 FROM expected_members AS m
        WHERE m.conversation = e.conversation AND m.grant_version = e.grant_version
          AND m.peer_id = e.from_peer
    )
    UNION ALL
    SELECT 'recipient_missing', e.conversation, e.grant_version, e.id
    FROM envelopes AS e
    WHERE NOT EXISTS (
        SELECT 1 FROM expected_members AS m
        WHERE m.conversation = e.conversation AND m.grant_version = e.grant_version
          AND m.peer_id = e.to_peer
    )
    UNION ALL
    SELECT 'self_send', conversation, grant_version, id
    FROM envelopes WHERE from_peer = to_peer
    UNION ALL
    SELECT 'identical_pair', conversation, grant_version, NULL
    FROM grants WHERE peer_a_id = peer_b_id
    UNION ALL
    SELECT 'null_envelope_id', conversation, grant_version, id
    FROM envelopes WHERE id IS NULL
)
SELECT diagnostic, hex(CAST(conversation AS BLOB)) AS conversation_hex, grant_version,
       CASE WHEN envelope_id IS NULL THEN 'NULL'
            ELSE hex(CAST(envelope_id AS BLOB)) END AS envelope_id_hex
FROM findings
ORDER BY conversation, grant_version, envelope_id, diagnostic;
```

Each result identifies incompatible history; one envelope can have multiple diagnostics. An
identical-pair result identifies the grant by conversation/version and displays the literal `NULL`
for its absent envelope ID. A `null_envelope_id` finding uses the same sentinel for a corrupt
envelope's missing key; its diagnostic distinguishes it from a grant-level finding. Both identifier
columns otherwise contain uppercase hex of the exact
stored bytes, including spaces, separators, newlines, NUL and malformed UTF-8. Hex keeps SQLite's
default pipe/newline output unambiguous; `quote()` alone leaves embedded newlines and truncates at
NUL. Decode hex only for exact-key inspection; it is not a new identity or a repair operation. A
missing grant produces missing-peer diagnostics too. No rows means these membership/self-send
checks passed for that read snapshot, not that all migration checks passed. A missing file, SQL
error or unsupported schema is a failed preflight, never a clean result. This query is for the
pre-room pair schema; after migration, membership checks use the real `grant_members` table.

Keep the reported identifiers and resolve their disposition explicitly before scheduling the
upgrade; this document supplies no automatic repair or deletion query. Re-run these checks inside
the exclusive migration transaction: a preflight against a running service can become stale.
The migration still checks the complete known catalog, foreign keys and integrity atomically.

### Atomic migration

Migration procedure, within the existing immediate transaction and `user_version` discipline:

1. Require exclusive server ownership before opening storage. Run numbered predecessors first;
   use the known schema version, not per-column sniffing. Reject future/unknown layouts. Preserve
   frozen legacy-adoption SQL unchanged. Admit no readers, workers or clients during migration.
2. Record the known dependent view/trigger definitions from the verified migration catalog.
   Drop those triggers and views (including transitive dependent views, dependents first) inside
   this transaction before replacing referenced tables. Otherwise an invalid view or trigger can
   make SQLite reject a later table rename. Unrecognized dependent objects fail the migration;
   rollback restores the original definitions. No application triggers may fire during copying.
3. Build new tables under temporary names, with their foreign keys pointing to the corresponding
   new parents. Backfill **all** active, superseded and revoked grant versions using the exact
   mapping above, preserving stored peer IDs, historical counters, dates and cancellation flags.
4. Rebuild the closed set of tables referencing replaced parents, including envelopes and any
   inbox/audit tables that have landed by then. Copy rows parent-first, preserving all IDs and
   references, before dropping any original table. Enumerate that graph from the shipped schema
   in the migration definition; an unrecognized dependency fails migration, not a best-effort copy.
5. Drop original dependent tables leaf-first, then original parents, so foreign keys stay enabled.
   Rename new parents and their dependent tables into their final names. Recreate the version's
   known indexes, then views in dependency order, then triggers after data copying and all renames.
   Verify their definitions and query the restored views. Do not rename an original parent to a
   backup name first, which rewrites references.
6. Verify full row/value equivalence except the explicit representation/constraint changes,
   exact allowed-edge equivalence for every legacy version, no orphan FKs, and valid catalog.
   `PRAGMA foreign_key_check` must return no rows. Check database integrity and required queue
   indexes. Only then advance `user_version` and commit. Every intermediate failure rolls back.

This is an application-specific rebuild of the complete dependent graph with foreign keys enabled,
not a single-table drop under active dependents. The eventual migration PR must instantiate the
then-current inbox/audit schemas and prove the graph ordering against the pinned SQLite driver;
it cannot silently disable foreign keys or use `writable_schema` if that ordering is insufficient.
SQLite's [schema-change documentation](https://www.sqlite.org/lang_altertable.html) explains the
create/copy/drop/rename pattern and rename-reference hazards; its
[foreign-key documentation](https://www.sqlite.org/foreignkeys.html) governs dependent drops.

No data migration runs twice, no migration statement is replayed after partial execution, and
no old/new writers coexist. Pre-room readers derive historical members on demand; post-room
readers use only migrated representation. Downgrade requires a stopped-service consistent backup
restore, not dual-write compatibility or an automatic destructive reverse migration.

## Required contract fixtures

These are specified assertions for implementation PRs, **not tests claimed to exist or pass today**.
Use temporary file-backed WAL databases for migration/concurrency claims.

| Fixture | Required result |
| --- | --- |
| Every legacy Direction, both member input orders and all candidate sender/recipient pairs | Exact old/new edge equivalence; reject outsiders and self-send |
| Pair object → storage → object across restart | Preserve kind, roles, edge and exact IDs; no caller-order meaning |
| `lead_only`, two-edge/empty directed, larger members on first runtime | Explicit unsupported error and byte-for-byte unchanged durable state |
| Invalid roles, duplicate IDs/edges, missing endpoints, control-bearing IDs, unknown fields | Invalid error; no version/budget/queue mutation |
| ASCII boundaries, punctuation, permitted spaces, empty/space-only strings, control bytes, DEL, non-ASCII Unicode and malformed UTF-8 | Accept only nonempty printable ASCII containing a non-space byte; preserve accepted keys exactly; no durable mutation on rejection |
| Historical incompatible conversation/peer IDs | Read-only inventory reports exact escaped/hex locations; rows unchanged; new authorization/renewal rejected, exact-key human revocation retained; unaffected grants still work |
| Room backfill containing otherwise compatible non-ASCII history | Preserve raw identifier bytes and relationships; do not silently replace, merge or drop history |
| Lead plus D1/D2, then add D3 under lead_only | Lead↔each developer allowed, developer↔developer forbidden |
| Same members under open / explicit directed | All distinct edges / only enumerated edges; directed does not expand on add |
| Enroll/re-enroll/renew/replace with stale concurrent expected versions | One winner; monotonic history and one active row; no partial loser mutation |
| Remove recipient or deny its edge during queued reply and during an in-flight never-attempted result | Cancel/report, do not reset original ACK; no rescue past removal |
| Remove then re-add / deny then re-allow before late settlement | Intervening denial still blocks carry |
| Ordinary renewal; opt-out; revoke; expired successor | Preserve ordinary cancellation, default eligible carry, sticky barriers and expiry waiting |
| Two senders race for final budget unit | Exactly one claim; shared budget exhausted; no per-member reserve |
| Duplicate settlement/refund and stale attempt result | Exactly one original-version refund; no successor/new-attempt overwrite |
| Reply unknown/wrong recipient/replier/conversation, malformed/duplicate/stale marker | Reject atomically; no ACK or trusted insertion |
| Reply original dispatching, then handed off; duplicate ingestion | Retry pending event, then one ACK/reply; duplicate cannot ACK again |
| All known version-zero schemas and every numbered predecessor, including revoked/superseded history | Complete backfill preserving keys, values, provenance and edge sets |
| Inject failure during copy/drop/rename/index/version update; terminate before commit | Restart sees intact old schema/version/data; no partial shadow catalog |
| Self-edge, repeated member, missing FK and invalid policy after migration | Database constraints or transactional graph validator reject as specified |
| NULL in each composite-key component or policy_kind, supplied explicitly or omitted | NOT NULL rejects the row; no unbound member/edge or policy-less grant |
| NULL or omitted envelope ID; legacy rows with NULL IDs | NOT NULL rejects new rows; preflight reports legacy rows and the migration rolls back without inventing IDs |
| Envelope sender or recipient exists only in another conversation/version | Its respective membership FK rejects insert/update; no partial mutation |
| Historical envelope after member removal; eligible reply carried to a successor | Historical FK remains valid; successor members precede the atomic reply version update |
| Attempt to delete membership still referenced by a historical envelope | FK rejects deletion; no cascading loss of evidence |
| Incompatible historical nonmember envelope, self-send/identical pair or unrecognized dependent table | Whole migration fails, leaving source schema and evidence unchanged; no synthetic membership repair |
| Preflight on valid history, peers present only in other versions/conversations, missing grant, self-send and identical pair | Exact diagnostic rows across all states/versions; valid historical removal is not flagged; read-only execution leaves data unchanged |
| Preflight identifiers containing pipes, spaces, newlines, NUL and malformed UTF-8 | Exact hex round-trip, one output row per finding, no separator ambiguity; absent envelope is the literal NULL |
| Inbox/audit references, queue ordering/indexes and uncertain rows after migration | References and state unchanged; uncertainty never automatically replayed |
| Known views over rebuilt tables, transitive views and cross-table triggers; failure after dropping them | Drop before originals, restore after final renames, retain view results and trigger behavior; failure restores the entire original catalog/data |

## Source basis and review boundary

At core baseline `a8beeffd1bbbfaf0033e086e68af74c5108cb56a`, compare
[Grant.PermitsDirection](../../internal/store/types.go),
[controller enrollment/renewal](../../internal/controller/controller.go),
[budget claims/refunds](../../internal/store/grants.go),
[carry-forward and attempt settlement](../../internal/store/envelopes.go),
[dispatch authorization](../../internal/dispatch/dispatch.go),
[atomic reply ingestion](../../internal/adapter/codex/ingest.go) and
[numbered migrations](../../internal/store/migrations.go).

Specification review must assess the first-runtime supported subset, error/operation contracts,
full-history carry predicate and foreign-key-preserving migration strategy, in addition to the
already accepted policy model/timing. Passing `mise run verify` validates repository checks and
links; it does not approve these proposed contracts or prove the future migration implementation.

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

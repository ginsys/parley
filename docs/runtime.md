# Runtime foundation

## Store readers

`store.Open` initializes the single immediate writer and ordered schema migrations. It retains
library memory and URI support and does not run startup recovery automatically. The runtime writer
entry point, `store.OpenExisting`, uses SQLite `mode=rw` so a missing file cannot become a silently
created replacement database.

After recovery, `DB.OpenReaders` explicitly initializes four file-backed connections and publishes
one pool only after all connections open. The internal `readerDSN` shares writer path normalization
and the caller option allowlist, but sets `mode=ro`, deferred transactions, query-only mode, foreign
keys and a five-second busy timeout. Readers do not set journal mode or run migrations. Each new
physical connection receives the same settings. Existing memory writers remain usable; they cannot
initialize this file-backed reader pool.

`DB.Queries` exposes materialized queue and envelope-outcome projections without SQL execution or
connection access. Queries before reader initialization fail with `readers_not_ready`; there is no
writer fallback. Each query has a maximum five-second context including acquisition, snapshot,
materialization and closure, or the caller's earlier deadline. Queue tail wrap shares the same
snapshot and deadline, and each batch contains at most 100 IDs for the exact recipient/conversation.
The outcome projection omits message bodies. Both are internal observations, not authorization for
agent disclosure or delivery.

The Claude poller selects candidates through readers; dispatch's observation of an existing outcome
also uses readers. Acceptance, authorization, budget claims, result settlement and controller
mutations retain immediate writer transactions. Readers release rows and snapshots before a host
call. A queued ID may become stale after selection; dispatch still rechecks current authority.

`DB.Close` closes readers before the writer. Lifecycle owners must stop workers and independent
settlement before closing the store. Runtime lifecycle and authenticated service integration are
separate from the query interface; reader initialization alone makes no live-session claim.

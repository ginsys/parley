# Runtime foundation

## Store readers

`store.Open` initializes the single immediate writer and ordered schema migrations. It retains
library memory and URI support and does not run startup recovery automatically. The runtime writer
entry point, `store.OpenExisting`, uses SQLite `mode=rw` so a missing file cannot become a silently
created replacement database. Ordinary relative writer paths are pinned at open time so delayed
reader initialization cannot follow a changed working directory.

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

## Exclusive Linux ownership

`runtime.Acquire` accepts an existing ordinary database path, resolves it to a canonical absolute
path and takes `LOCK_EX|LOCK_NB` on its persistent adjacent `.lock` file before any SQLite open.
Contenders return `already_running`; SQLite's legitimate busy and migration retries remain intact.
The descriptor is private, close-on-exec and never passed through `exec.Cmd.ExtraFiles`. Closing
ownership never unlinks the lock file. Crashes release the kernel lock.

The database, lock and existing SQLite WAL/SHM/rollback-journal sidecars must be regular, singly
linked, server-owned files with no group/other permissions. Opens do not follow symlinks; the DB
itself may be selected through a trusted symlink resolving to the same canonical identity.
Every alias in a symlink chain must have a trusted owner and parent, including aliases absent from
the final canonical path. Parent directories
must be owned by root or the effective server UID and must not be group/other writable. A root-owned
sticky ancestor such as `/tmp` is allowed above a protected directory, never as the immediate DB
parent. Named DB/lock identities are rechecked after acquisition. Runtime never adjusts permissions.

Supported filesystem types are ext2/3/4, XFS, Btrfs, tmpfs and overlayfs; unknown, FUSE and network
filesystem types reject. Non-Linux runtime ownership fails explicitly. Local filesystem type checks
do not establish the provenance of an overlay backing store; trusted deployment setup must keep
backing storage local. Dedicated production accounts and trusted mounts remain deployment gates.
The same-account development model does not stop another process under that account from bypassing
flock, changing permissions or replacing files. Trusted owners must not relocate or modify these
paths while service runs. Simultaneous restored copies are not made safe by separate file locks.

## Startup and shutdown

`runtime.Start` requires an explicit recovery inspector. After ownership it opens the existing
writer, performs ordered migrations, asks the inspector for `Normal` or `Held`, establishes permitted
recovery, opens readers, and starts registered services. Missing/unknown/failed inspection rejects
startup. Normal mode applies `RecoverUncertain`: interrupted `dispatching` rows become `uncertain`
without resetting attempts or refunding budget. Held mode makes no automatic envelope recovery
mutation and starts only registrations explicitly marked `RecoveryOnly`.

The inspector and service registrations are trusted application wiring, never agent-supplied
permissions. The inspector must check the installation's external and durable recovery evidence;
there is no built-in normal default. It must not initialize readers, publish services or clear holds.
Concrete durable clock/restore markers and authenticated human disposition are not implemented by
this slice. Tests inject controlled inspection results; a future executable must implement the
accepted recovery contract before exposing ordinary operations.

Each service receives initialized writer/query resources and a worker context. `Start` must honor
cancellation and return after initialization. `StopAdmission` promptly closes acceptance of new
work, while `Wait` joins workers and their independent settlement. Both cleanup methods must be safe
after partial `Start` failure; errors report a completed cleanup, not permission to abandon workers.
A failed service start is included in reverse cleanup.

`Runtime.Stop` stops admission in reverse registration order, cancels worker contexts, waits in
reverse order, closes readers then writer, and closes ownership last. Cleanup continues independently
of the caller's waiting context: a cancelled wait cannot release a still-active writer. Another
`Stop` can await the same result. Cancelling the parent lifetime context also initiates shutdown.
Services must cooperate with shutdown; a stuck service retains ownership rather than allowing a
second writer. Forced process termination leaves interrupted work for conservative restart recovery.

## Verification and remaining integration

Fixtures use synthetic local WAL databases and the Go test binary as a controlled child process,
never a host CLI. Linux ownership fixtures create explicitly private disposable directories under
`/tmp`; generic `testing.T.TempDir` permissions and an inherited writable temporary parent are not
assumed to meet runtime ownership policy. UID-remapping sandboxes may reject real ancestry; run
ownership checks in a normal Linux user namespace with the actual filesystem ownership visible.

Run `mise run verify` and focused race coverage with:

```sh
mise exec -- go test -race ./internal/store ./internal/runtime ./internal/dispatch ./internal/adapter/claude ./internal/adapter/codex
```

The tests cover reader/VFS isolation, replacement connections, pool/query/materialization
cancellation, concurrent WAL snapshots and writer commit, candidate authorization races, startup
failure cleanup, held-mode admission, shutdown ordering, cancelled shutdown waiting, process
contention, crash release and exec noninheritance. These establish the internal foundation, not a
running bridge or production isolation. There is no daemon, listener/socket implementation, human
initialization command, CLI conversion, binding migration or live host connection in this slice.
The executable and human initialization use this ownership lifecycle in subsequent administration
work; authenticated connection and durable recovery integration remain separate work.

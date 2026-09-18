# Operations

This document covers `cmd/parleyd` initialization, configuration and stopped-service backup. It
describes PR1's actual capability, not a target: there is no checkpoint, backup or restore
subcommand in this slice, and the wire administration surface exposes only `server.hello` and
`operation.get` (see [Architecture's accepted human control protocol](architecture.md#accepted-human-control-protocol)).
See [Runtime foundation](runtime.md) for the underlying ownership/startup/shutdown lifecycle this
document assumes.

## Initialization

Run `parleyd init -database PATH` exactly once per deployment, before the first `parleyd serve`.
`PATH` must be absolute and clean. `init`:

- refuses outright if anything already exists at `PATH` -- of any kind, including a leftover
  zero-length file from a previous failed attempt. Remove or relocate that entry yourself before
  retrying; `init` never overwrites, and there is no `-force`.
- creates the file privately (mode 0600) before anything else touches it, then joins the same
  exclusive ownership lock `serve` and `parleyctl`'s legacy commands use (`runtime.Acquire`),
  before running schema migrations.
- prints the minted `installation.server_id` on success. **Record this value.** It is the
  database's stable identity, distinct from the per-process `server_epoch` a running server mints
  fresh on every `serve` invocation; a restored or relocated copy must still report the same
  `server_id`.

`init`'s parent directory must be owned by root or the account that will run `parleyd`, and must
not be group- or other-writable (the same trust walk `runtime.Acquire` enforces for the database
itself). Help and invalid arguments touch neither a database nor a socket.

### If `init` fails partway through

`init` never deletes the target file to "recover" from a failure, at any phase, and never retries
by silently reopening or overwriting on a second attempt. Every failure message names the phase
that failed and what to do next; the file is always left in place for inspection:

- **Ownership already held by another process** -- another `parleyd`/`parleyctl` instance is
  running against this path. This attempt did not initialize the file; its current contents are
  unverified, since another owner may already be using or have replaced it. Stop the other
  process, or investigate a stale lock manually; `init` will not guess.
- **Ownership could not otherwise be established** -- do not delete the file blindly. Something
  else (permissions, filesystem state) is preventing exclusive access; diagnose that first.
- **Schema initialization failed** -- migration runs as a single transaction (one `BEGIN`, every
  step, one final `COMMIT`), so this normally means no schema was committed at all, not a partial
  one. Do not delete the file blindly regardless; inspect it manually before deciding how to
  proceed.
- **Schema committed but the installation identity could not be read back** -- `store.Open`
  (schema initialization) already reported success; only this later, separate read failed. A
  failed read does not by itself establish whether the database is corrupt or intact either way.
  Verify the database independently with `parleyctl hello` before deciding whether the file is
  usable.
- **Initialized but the success message or follow-up guidance could not be written to stdout** --
  the database itself was written and closed successfully before either write ran; only this
  status output failed. Do not reinitialize or delete the file -- verify it with `parleyctl hello`.

In every case, retrying `init` against the same path fails again with "already exists" (per the
non-overwrite rule above), so a failed attempt never silently becomes a fresh, empty database on
retry.

## Configuration and starting the server

```
parleyd serve \
  -database /path/to/parley.db \
  -admin-socket /path/to/admin.sock \
  -administrator 11111111-1111-4111-8111-111111111111=1000 \
  [-administrator ...] \
  [-server-uid 1000] \
  [-socket-mode 0600] \
  -recovery-markers-dir /path/to/recovery-markers \
  [-recovery-markers-capacity 64]
```

- `-database` must already be initialized (see above); `serve` never substitutes an empty database
  for a missing path -- it fails startup instead (`runtime.Start` uses `store.OpenExisting`).
- `-admin-socket` is the Unix socket administrators connect to. It must be absolute; its parent
  directory is validated the same way `-database`'s is. An existing entry at this path is replaced
  only after a bounded connect attempt proves it definitely abandoned (`ECONNREFUSED`); a live
  socket, a non-socket entry, a wrong owner, a timeout or a permission failure all refuse startup
  rather than risk hijacking a running instance's socket. Binding uses a TOCTOU-safe
  `/proc/self/fd/<fd>/<name>` address rather than the pathname itself, which can be a few bytes
  longer than `-admin-socket`'s own length; a resulting address that cannot fit a
  `struct sockaddr_un` (108 bytes including the terminator) fails startup with a concise diagnostic
  rather than falling back to an unprotected bind. In practice this means an `-admin-socket` path
  must leave a little headroom under the platform's 108-byte limit, not use it to the last byte.
- `-administrator ID=UID` maps one administrator's canonical UUID to the one OS account UID
  permitted to connect as that administrator (`SO_PEERCRED`-verified, never client-asserted).
  Repeat for multiple administrators. At least one is required. Two administrators cannot share a
  UID -- that would make them indistinguishable at authentication time.
- `-server-uid` defaults to the UID `parleyd` itself is running as. Administrators and
  `parleyctl hello` verify this value when connecting; it rarely needs to be set explicitly.
- `-socket-mode` defaults to `0600`. Use `0660` only with an explicitly provisioned
  administrator-only group -- provisioning that group is your responsibility, not `parleyd`'s.
- `-recovery-markers-dir` must be an existing, private (mode 0700), server-owned directory. This is
  where durable recovery incident markers are written; it is not a log directory and must not be
  shared with anything else.

`serve` blocks until `SIGINT`/`SIGTERM`, or until recovery evidence cannot be made durable
(fail-stop), then performs the same ordered shutdown `Runtime.Stop` always does: stop admission,
cancel workers, wait for them to finish, close readers then the writer, release ownership last. A
fail-stop exit is deliberate: it means recovery marker persistence failed, and the process refuses
to continue admitting work rather than risk losing incident evidence. Do not configure automatic
unattended restart on that exit code without investigating first.

## Stopped-service backup and relocation

There is no online backup or checkpoint command in this slice. To make a consistent, restorable
copy:

1. Stop `parleyd` (`SIGINT`/`SIGTERM`) and wait for it to exit. Confirm the process is gone and its
   `<database>.lock` file shows no active flock (e.g. `flock -n <lock> -c true` from a shell
   succeeds) before proceeding -- a copy taken while the writer is still live is not consistent.
2. Copy the main database file together with its `-wal` and `-shm` sidecars, if present, as one
   atomic unit (same instant, e.g. via a filesystem snapshot, or all three copied before anything
   else touches the directory). Never delete `-wal`/`-shm` as "temporary": SQLite's WAL mode keeps
   committed data there until the next checkpoint, and a copy missing them can silently omit
   recent commits.
3. Preserve file ownership and permissions on the copy; a relocated database is still subject to
   `runtime.Acquire`'s private-file and trusted-parent-directory requirements (see
   [Runtime foundation](runtime.md)).
4. Before trusting a restored or relocated copy, verify it via `parleyd init`'s printed
   `server_id` from when the database was created, or by running `parleyctl hello` against a
   `parleyd serve` started on the copy and comparing `server_id` in its output. A mismatched or
   missing `server_id` means you have the wrong file, not the deployment's actual database.

`parleyd` never creates a replacement database at a new location on its own; a missing or
unreadable `-database` path is always a startup failure, never a silent fresh start.

**Starting a restored or relocated copy is not automatically detected as a restore, and this slice
has no wire-level way to clear a recovery hold if one is triggered.** The runtime's clock-rollback
detection (see [Runtime foundation](runtime.md)) compares current wall time against the last wall
time this database observed while running; only an actual backward step of the *system* clock
relative to that recorded value trips it -- restoring an older backup onto hardware whose clock
keeps running forward normally does **not**, by itself, trigger a hold, since current time is then
naturally ahead of the restored copy's last recorded observation. A hold is still a real risk in
practice: restoring onto a machine with a lagging or misconfigured clock, restoring after the
original host's clock was corrected backward, or any other path that leaves current wall time
behind what this copy last recorded, will trip it. If it does, `parleyd serve` comes up in
`recovery_only` state (`parleyctl hello` reports `state: recovery_only`); `operation.get` continues
to work, but `recovery.complete` and the other recovery-disposition methods are PR5 scope and are
not wired yet -- there is no client-side path out of the hold in this slice, only a later release
that wires those methods, or direct database intervention outside `parleyctl`/`parleyd`. Verify the
restore host's clock before relying on a copy, and plan restore drills accordingly until PR5 lands.

## Diagnostics

`parleyctl hello [-endpoint PATH] [-server-uid UID]` is a pure client diagnostic: it dials the
administration socket, performs the required `server.hello` handshake, and prints the server's
protocol version, `server_id`, `server_epoch`, the calling administrator's ID, operational state
(`running` or `recovery_only`), the advertised profile limits and the exact set of implemented
methods. It opens no database. Endpoint and server UID resolve from `-endpoint`/`-server-uid`,
else `$PARLEY_ENDPOINT`/`$PARLEY_SERVER_UID`; `$PARLEY_DB` is refused outright as a client
configuration source -- it is a server-only variable, and its presence in a client's environment is
a misconfiguration, never a fallback.

A timed-out `hello` reports the outcome as unknown, not failed: the client cannot tell whether the
server never received the request or simply did not answer in time. Retry rather than assume the
server is down.

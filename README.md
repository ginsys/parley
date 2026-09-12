# Parley

Parley is an authorized conversation bridge between a Claude Code session and a Codex CLI
session. **Permission to communicate never grants permission to execute commands, approve
operations or push changes.** Every delivered message remains untrusted input.

## What works today

This repository contains the SQLite state machine, grant administration, message acceptance and
dispatch, a Claude readiness handshake/poller, strict reply-marker extraction, and a Codex
transport/ingestion adapter. These components have synthetic tests.

The only executable is `parleyctl`, a human-operated **grant administrator**. It does not start a
server or connect sessions. Running it without arguments displays help and exits successfully.
There is no runnable bridge, live Claude Channels connection, session identity binding or rollout
watcher yet. The `codex queue` adapter has not been validated against a live host. Runtime work is
tracked separately in [issue #12](https://github.com/ginsys/parley/issues/12); the durable inbox is
tracked in [issue #6](https://github.com/ginsys/parley/issues/6).

Read [Architecture](docs/architecture.md) for the complete current contracts and limitations.
No private design document is needed to understand or contribute to this repository.

## Build and verify

Install [mise](https://mise.jdx.dev/), then from a checkout:

```sh
mise trust
mise install
mise exec -- go build ./...
```

To produce the administrator binary:

```sh
mise exec -- go build -o parleyctl ./cmd/parleyctl
```

The pinned Go version is in [mise.toml](mise.toml) and [go.mod](go.mod). Full verification also
needs Git, Python 3.9+ with PyYAML 6, and network access for the pinned action-checker checkout:

```sh
git fetch origin main
mise run verify
```

That task builds all packages and runs vet, Go tests, formatting checks, documentation and
whitespace checks, shell/workflow lint, commit-lint fixtures and action-pin validation.
`mise run test` alone runs only the commit-lint shell fixtures; it is not the complete Go suite.
For the Go suite alone, use `mise exec -- go test ./...`.

Tests use temporary databases, fake transports and controlled helper processes. Ordinary tests
never invoke the installed Codex CLI. CLI routing tests inject a fake administrator; agents must
never invoke the protected `parleyctl` executable, even for help. Synthetic tests do not establish
live host compatibility.

## Grant administration

A human operates `parleyctl` directly in their own shell. Agents must neither execute it nor
construct or approve grant/revoke/renew arguments on the human's behalf. Built-in help describes
all flags; the human chooses conversation names, enrolled peer IDs, directions and budgets.

| Command | Effect |
| --- | --- |
| No arguments, `help`, `-h`, `--help` | Display usage; no database access |
| `grant` | Enroll distinct peers with a positive exchange budget; create a new historical version |
| `renew` | Create a successor version with a fresh budget counter; keep existing budget/expiry when their flags are zero |
| `revoke` | Revoke the active grant and cancel queued messages; report messages already in flight |

Grant direction is `bidirectional`, `a_to_b` or `b_to_a`. A positive `-expires-in` duration sets
expiry relative to now. Zero means no expiry for a new grant and preserves expiry for renewal;
negative budgets and durations are invalid. Required names and IDs must be nonempty and peers
must differ. Identifiers are preserved and compared exactly: `"x"` and `" x"` are different
conversation names, just as `"a"` and `"a "` are different peer IDs. Whitespace-only identifiers are
invalid. Both names and peer IDs accept only printable ASCII bytes (`0x20`–`0x7E`), matching
delivery-wrapper validation. Non-ASCII and malformed UTF-8 are rejected; message bodies are
unaffected. An existing grant with incompatible identifiers cannot authorize new work or be
renewed but remains revocable; its stored identifiers are not rewritten. Use the
[identifier inventory](docs/identifier-inventory.md) before a future administration-interface cutover.
Administrator output quotes identifiers to make whitespace visible; use the exact name
for later operations. Unknown commands, malformed flags and positional arguments fail before
storage opens.

Renewal cancels ordinary queued messages from the old version. It carries eligible trusted
replies forward by default, because their originals have already been acknowledged. The human
can choose `-cancel-pending-replies` to cancel those replies too; the choice also applies to late
unattempted outcomes crossing that renewal. Revoked or cancelled messages never revive.

Exit codes: `0` for help/success, `2` for invalid arguments, `1` for operational failures.

## Storage and recovery

`PARLEY_DB` selects the SQLite database; the default is `./parley.db`. Opening storage applies
atomic, numbered schema upgrades. The original two unversioned layouts are supported; unknown
layouts, future versions and malformed/out-of-range envelope timestamps fail without partial
migration. Back up existing data before upgrading, using a SQLite-consistent backup or stopping
all users first; an active WAL database cannot be backed up reliably by copying its main file alone.

Messages and peer identifiers are stored in plaintext. Restrict access to the database directory,
including WAL/SHM files. The cooperative human-only rule is not an operating-system isolation
boundary: code with the same user's filesystem access can bypass it.

Dispatch commits a budget claim before calling the host and records the outcome afterward.
An interrupted or ambiguous handoff becomes `uncertain`, retains its budget claim and is never
automatically replayed. Recovery is a library operation that must run only after previous
dispatchers have stopped; there is no recovery/admin UI yet. Revoke cannot recall messages already
accepted by a host or prohibit communication through other paths.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for issue scope, verification, draft PRs and review.
[AGENTS.md](AGENTS.md) records agent boundaries and permanent implementation decisions.
Host-specific policy tools, including nah, are optional local setup and are not dependencies of
this repository's core implementation.

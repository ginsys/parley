# Disposable host-wake probes

The investigation protocol is owned by [#18](https://github.com/ginsys/parley/issues/18).
The tooling here records evidence; it is not a production host adapter or session authenticator.
The controlled fixtures launch only Python children, never installed Claude, Codex or OpenCode.

## Reproduce controlled fixtures

With mise installed, run:

```sh
mise run python
mise run verify
```

`mise run python` and `mise run docs` first create the gitignored `.venv` using the pinned
Python and install `requirements-dev.txt` (`PyYAML==6.0.3`). CI uses the same setup task; no
system-interpreter PyYAML installation is required.

`mise run python` lints all Python under `scripts/` and runs the PTY, classification and aggregate
CI fixtures. The CI Python job installs both pinned tools explicitly. Python is a dependency of
both `verify` and CI's required `checks` aggregate. The gate fixtures execute the real aggregate
shell with success/failure/cancellation results for PR, main-push and merge-group events, and run
mise with the real verify dependency list, successful unrelated stubs and a failing Python task.
These tests establish failure propagation locally; an actual CI run remains separate evidence.

OpenCode server and driver fixtures live in `scripts/probe/test_opencode_driver.py`; shared
controlled helpers and the other host fixtures live in `scripts/probe/test_host_trials.py`.
Both modules run through the same unittest discovery command.

The PTY fixtures supply controlled observations for shell/pager/editor replacement, approval
prompts, unknown/busy state and stale generations after restart. They verify rejection before
writing. The no-ack fixture receives bytes but supplies no acknowledgement signal. These are
synthetic states, not a detector for a real host's prompt. A real observer must establish process,
session and prompt state independently; a caller claiming `idle` is not proof. Arbitrary terminal
output or an echoed marker cannot establish acceptance, a new turn or acknowledgement.

## Passive recording

[The recorder](../scripts/probe/wake_probe.py) uses Python's Unix
[pty.fork](https://docs.python.org/3/library/pty.html) and selectors. It owns a disposable child
process group, bounds captured input/output to 1 MiB, closes the PTY and kills/reaps its child on
exit. Terminal bytes are hex-encoded with monotonic timestamps rather than rendered as terminal
control sequences. By default (`--home disposable`) the CLI creates a fresh HOME and working
directory, passes only HOME/PATH/TERM/PWD, and never writes input. `--home inherit` opts into the
real HOME/environment instead, for driving an authenticated host session; the record's
`home_mode` field makes that choice visible rather than implicit, and no HOME cleanup runs for
it. `--cwd` overrides the child's working directory, and the record's `cwd` field carries the
resolved directory the child actually ran in — host behaviour varies with repository-level
configuration and instructions, so without it two captures of the same command in different
directories are indistinguishable and cannot satisfy the reproduction requirement below. In both
modes the child's `PWD` is set to that same resolved `cwd`. Inherit mode copies the parent's
whole environment, so without that override a `--cwd` differing from the parent's would start
the child believing it is somewhere it is not.
Before exec, the child closes all non-stdio descriptors, including handles
made inheritable by its launcher. This Linux harness requires `/proc/self/fd` to enumerate the
actual descriptor range; enumeration failure aborts startup instead of launching with unknown
handles. The parent's descriptors remain unchanged. SIGINT is blocked across fork until the parent
owns the child PID and PTY descriptor, then the original signal mask is restored in both processes.
A pending Ctrl-C can then trigger cleanup without losing ownership. Before exec, the child sets
the PTY to 80 columns by 24 rows; the record includes this configured `terminal_size` so trials
use reproducible geometry instead of a zero-sized terminal.

Complete JSON is flushed to a temporary file beside the destination and
published with an atomic no-replace hard link (then the temporary name is removed). The containing
directory is fsynced before success is reported, persisting the new link and removal as well as
the file contents. Directory-sync failures fail loudly; a visible file after such a failure does
not prove durable publication. This preserves
another capture even if it creates the destination during recording. Failed serialization leaves
no empty final file. A caught interruption preserves partial events and returns a nonzero status;
an uncatchable SIGKILL or power loss before publication cannot preserve an in-memory transcript.
The CLI installs one non-raising SIGINT handler before acquiring the child and retains it through
cleanup and evidence publication. Capture checks the remembered interruption between bounded reads
and while waiting after EOF; ownership assignment and entry into finalization have no unguarded
signal transition. The CLI returns 130 if Ctrl-C was requested. Repeated Ctrl-C during JSON serialization, flush/sync or
link/unlink cannot interrupt that finalization. The record keeps the actual capture outcome;
an interruption requested only during publication does not turn completed capture into failure.
The CLI restores the previous signal handler afterward, including on filesystem errors. This
uses Python's [main-thread signal handling](https://docs.python.org/3/library/signal.html#signals-and-threads);
direct library calls to `publish_record` do not install the CLI's signal guard. Disk/write errors
still fail loudly, and no guarantee is made that storage will complete within a fixed time.
If child teardown raises, publication still runs: `cleanup_error` records the exception class
separately from any earlier capture error. Such a record is failed/interrupted, and a missing exit
status remains unknown. Cleanup may be incomplete; the record does not assert the child was reaped.
PTY/selector descriptor cleanup runs even when process cleanup raises. The API retains the child
PID until a successful reap so an interrupted wait can be retried without abandoning that child.
Disposable-HOME acquisition is part of startup failure recording: if it fails while the output
directory remains writable, the CLI publishes a failed record without a child status. HOME cleanup
runs before publication under the same SIGINT handler, and its errors are recorded separately as
`home_cleanup_error`; they cannot produce a successful capture record. Ctrl-C during HOME cleanup
still returns 130 while preserving the earlier capture outcome.

For a synthetic recording, choose a new output filename:

```sh
mise exec -- python3 scripts/probe/wake_probe.py --seconds 2 --output probe-example.json -- python3 -u -c 'print("synthetic fixture")'
```

This command records output only; `delivery_claim` is null. Every record includes raw `wait_status`,
signed `exit_code` (negative means a signal), `cleanup_requested`, `stop_reason` and
`capture_status`. A nonzero child exit, including 127 from a failed exec, fails the capture and CLI;
127 can also be deliberately returned by a program, so it does not prove an exact startup stage.
Natural signal termination is a failure. A child still running at the deadline is stopped by the
harness and identified as such, not labelled a natural host failure. KeyboardInterrupt produces
an `interrupted` record with partial events and CLI exit 130; other capture/startup errors produce
failed records. A deadline cannot produce `complete`: an otherwise successful capture is
`stopped`, even when the direct child exited zero but a descendant kept the PTY open. Independent
capture/child failures still take precedence. `cleanup_requested` describes intervention on the direct child,
not whether every descendant exited naturally. `complete` requires observed PTY EOF and natural
direct-child exit zero, without a capture/cleanup error; it does not certify descendant outcomes.
If the child closes its terminal before exiting, EOF is provisional: the recorder continues
nonblocking reads while awaiting natural exit within the original capture deadline. A reopened
slave resumes capture, including output buffered when the child exits; an open but quiet slave
clears EOF too. Completion requires EOF after observing leader exit. EOF does not trigger immediate
termination or start a new window.
Deadline and interruption handling still apply while waiting after EOF. An exit first observable
after the deadline remains `stopped`, even if its eventual status is zero: the nonblocking status
query supplies no exit timestamp proving it happened within the window. Cleanup preserves the
actual exit status but does not retroactively certify capture completion.
CLI zero means the record was published successfully (including an intentionally stopped capture),
not that a trial completed or a host delivered anything. Failed/interrupted recordings cannot
supply negative wake findings. Even a
`complete` passive recording is not host-readiness evidence, and early EOF does not complete an
outcome observation window. Neither EOF nor a captured marker is
promoted to host acceptance. In disposable mode (the default) the command runs under a fresh HOME
with no copied host credentials; `--home inherit` trades that away deliberately, and its records
say so via `home_mode`. The Python `PtyProcess` API allows explicit writes only with a matching observer-supplied generation
and observed idle-agent state. It records partial writes as ambiguous and refuses other states.
It does not authenticate the observer, discover a current host identity or make PTY injection
safe by itself. Descendants which deliberately escape the child process group are outside this
harness's cleanup guarantee; fixtures must not do that.

## Trial protocol

Record host version, mechanism/configuration, synthetic input, reproduction command and UTC run
time. Observe each of acceptance, transcript visibility, new turn and acknowledgement separately.
Never equate a successful PTY write with host acceptance. Use three trials per mechanism/state:
idle, busy, approval-blocked, disconnected and restarted. Keep individual results and late events;
mixed trial results aggregate to inconclusive.

| Outcome | Deadline |
| --- | --- |
| Acceptance | 10 seconds from submission |
| Transcript visibility | 30 seconds from submission |
| New turn | 60 seconds from submission, or existing-turn completion when busy |
| Acknowledgement | 120 seconds from submission, or existing-turn completion when busy |

Keep `t_submit`, `t_turn_end` and each `t_outcome` on the same monotonic clock. For busy sessions,
wait at most 15 minutes for the existing turn to finish. If it does not finish within that cap,
dependent outcomes are inconclusive; if completion cannot be detected, they are unobservable.
Earlier independently observed outcomes count immediately. Keep later evidence separately.
`Trial.result` refuses to finalize a still-open observation window.
Its state values are `idle`, `busy`, `approval`, `disconnected` and `restarted`; unknown states
are rejected instead of silently receiving idle timing. Clock values must be finite, with
submission no later than now and every observed outcome/turn end inside that interval. Unknown
outcome names and invalid timestamps are errors, never a classified trial result.

Result codes: `observed`, `not_observed` (not observed within the window), `unobservable`,
`unsupported`, `inconclusive`. No negative classification proves nondelivery. Record which signal
established each positive result, not just a timestamp.

## Matrix runner

[`host_trials.py`](../scripts/probe/host_trials.py) drives a real host through one trial —
create a session, submit a synthetic marker message, observe the four outcomes — and feeds the
result into `wake_probe.py`'s own `Trial`/`aggregate` classification unmodified. It is tooling
only: filling the matrix (running it against installed hosts three times per mechanism/state)
is separate evidence, landed in a later change once trials actually run. Filling it is staged in
three changes: stage 1 was the first version of this tooling, built before any host had been
driven; the 2026-09-13 live captures (docs/host-probe-preflight.md) then replaced its guessed
shapes and it was trimmed to what those captures support; stage 2 runs the Claude and Codex rows
and lands `docs/host-wake-matrix.md`; stage 3 runs the OpenCode row and writes the synthesis #18
asks for. The stage numbers in docs/host-probe-preflight.md refer to that list.

### Ownership and isolation

Every session it creates is tracked in a `SessionRegistry` under a per-host namespace
(`claude:<id>`, `codex:<id>`, `opencode:<id>`); `submit`/`observe`/`teardown`/`attach`/`status`/
`stop`/`version` refuse an id the registry did not itself mint (`ForeignSessionError`). This matters concretely:
`claude agents --json --all` lists every background session on the workstation and
`codex queue --thread` reaches any thread, including ordinary human work, so a driver bug here
could otherwise stop or message someone else's session. Ownership comes only from the runner's
own creation output — stdout line 1 of `claude --bg`, the `thread.started` event of
`codex exec --json`, the first `sessionID` event of `opencode run` — never from a listing or a
rollout directory. A driver has no adoption path at all: a thread this run did not create cannot
be handed to it.

Ownership is also *per driver instance*, not merely per namespace. The registry stores each id
together with the driver instance that minted it, and `owned()` and the refusal above both read
that owner, so two instances of one host driver sharing a registry cannot reach each other's
sessions. Without that, sweeping one instance would close only its own PTY clients and servers and
then tear down the other instance's sessions while that instance's client or server was still
serving them — one `run_trial_with_cleanup()` deleting another live trial. Id and owner live in
one store rather than two, and are written in one statement, because the two facts must not be
separable: an interrupt landing between them would register a created session that no instance
owned, so the sweep would pass over a live authenticated session while the key naming it blocked
re-registration.

Trials run under the real HOME so the host session is actually authenticated, per the owner's
2026-09-11 decision — disposable at the *session* level, not the HOME level. The runner has no
disposable mode: its drivers call `subprocess.run` without an `env=` argument, so every host
command inherits the operator's environment, and its PTY clients inherit it plus
`TERM=xterm-256color` and `PWD=<probe cwd>`; `wake_probe.py --home inherit` is the PTY
recorder's equivalent and applies only to PTY captures. Every matrix cell this produces
therefore carries the developer's real credentials and config; it is not the clean-room
isolation `--home disposable` gives the PTY fixtures above. Each driver refuses at construction
any probe directory that is not owned by the current operator, permits group/other writes or
traversal, or is not empty. Blocking traversal also protects writable descendants of an allowed
fresh `.git` from access by other users after validation. Its resolved ancestor chain must belong
to the operator or root, and any shared
writable ancestor must have the sticky bit so other users cannot replace the owned child path.
This permits ordinary sticky temporary roots while rejecting writable non-sticky parents,
including unsafe ancestors above a private intermediate directory. It does not isolate against
the operator or root. The small controlled probe-directory fixtures use private directories
under Linux's sticky `/tmp` rather than inheriting potentially unsafe `TMPDIR` ancestry; build
caches and verification logs can remain in scratch. The directory keys a Codex trust
entry in `$CODEX_HOME/config.toml`, the slug of Claude's transcript directory, and whatever a real
repository's contents would feed the model. The one exception is a `.git` left by a bare
`git init` — what the captured Codex probe directory was, since `codex resume` gets no
`--skip-git-repo-check` — and it is checked, not trusted: a `.git` holding an index, a reflog,
any ref or any object is a repository with history, and a `.git` that is a file rather than a
directory points at a linked worktree or submodule store outside the probe directory. So does a
symlink, at `.git` itself or at any path beneath it: a link is followed by the very calls that
read the directory, so the freshness checks would describe a store the probe directory does not
hold, and `git init` leaves none for a legitimate one to be mistaken for. All three are
refused, because each would hand the authenticated hosts real history and configuration under
a directory whose emptiness is the whole precondition. Hooks must be regular `.sample` files;
active hooks are refused. Configuration must contain only the fresh Linux `[core]` baseline:
`repositoryformatversion = 0`, boolean `filemode`, `bare = false`, and `logallrefupdates = true`,
each once. Includes, extra sections or keys and modified values are refused. A fixture runs real
`git init` to check that this baseline still matches the installed Git. Validation also creates
a separate temporary baseline with installed Git, ignoring inherited Git environment settings
and global/system configuration. HEAD must name an initial branch accepted by Git; every other
path, file type and file's contents (apart from the separately checked config) must match that
baseline. Every regular metadata file must also be operator-owned and have exactly one hard link:
an external alias could otherwise change its inode without entering the private probe directory.
Group-write file modes from the operator's umask remain allowed behind the private directory's
traversal barrier; no second inode path can bypass it. Extra attributes, modified templates and
unknown metadata are refused. This requires
Git to be available; initialization failure or its ten-second timeout refuses the directory.
Drivers validate the same directory independently without initializing or changing it. The
temporary baseline is removed on exit. This keeps repeated driver construction compatible
without trusting repository settings that could execute commands.

Cleanup is part of the module, not left to a caller: `run_trial_with_cleanup()` is the entry
point real trials use. Its protected return runs `sweep()` in `finally`, including interruption
between a successful trial's return and the cleanup handoff. It tracks only exceptions raised
inside that protected operation; an unrelated outer exception handler cannot hide a cleanup
failure or receive its diagnostics. On every exit path it sweeps:
close open PTY clients, tear down every id in `owned()` (a copy `claude --bg --resume` started by
accident included), then close any server the driver runs. A failed trial propagates its own
exception with a note naming the sweep's failures and whatever is still owned; a completed trial
whose sweep failed raises `CleanupFailed` carrying the `TrialRun`, since the evidence is valid
even though a live, authenticated session remains for a human to remove. A teardown that cannot
confirm removal retains ownership rather than releasing it — releasing on a no-op would read as a
session having been cleaned up when it had not; likewise a server that survives SIGKILL stays
held and is reported. A client whose `close()` failed is kept on the driver too, and the sweep
then tears *nothing* down: that client may still be the process serving its session (a Codex
resume client is exactly that), and its handle is the only one there is, so deleting the session
anyway could remove a thread still in use and dropping the handle would leave the child
unrecoverable and unnamed. Every id stays owned and is reported alongside the client's failure. A
sweep covers exactly the driver instance it is given — its own minted ids, its own clients and
servers — and reaches nothing another instance created, so sweep every instance that did
work. One gap stays open by design: a Ctrl-C while
`claude --bg`, `codex exec` or `opencode run` is still running loses that command's output.
Claude makes one bounded (15-second command timeout), cwd-filtered listing snapshot and reports
all visible unowned candidates without adopting, stopping or deleting them. Failed discovery and
later arrivals remain uncertain, explicitly noted on the cancellation. Codex and OpenCode still
require manual discovery when interrupted output names no session. OpenCode adds the exact
generated title and probe cwd to the original interruption so the operator has a specific locator;
the session ID remains unknown and no automatic listing, adoption or deletion is attempted.
The human discovery routes
are `claude agents --json
--all --cwd <probe cwd>`; the newest rollout under `$CODEX_HOME/sessions` naming the probe cwd;
the matching generated title in the global `opencode --pure session list`, removed with
`opencode --pure session delete <id>`. The `opencode serve` child has the same shape in a
narrower instant: it is spawned and held in one statement inside the handler that closes it, but
an interrupt delivered between the OS creating it and its handle reaching Python leaves a process
nothing in-process can name. Its child runs in its own process group, so a terminal Ctrl-C does
not reach it either; `ss -lptn` on the probe run's port finds a stranded server.

PTY clients do **not** share that gap. `PtyProcess.__init__` blocks SIGINT across `pty.fork()`,
restores the prior mask in the child before `exec`, stores `pid`/`fd` in the parent before
unblocking, and closes the child if the pending interrupt then raises. The `serve` child gets no
equivalent because `subprocess.Popen` offers no way to restore the mask in the child before
`exec` — only `preexec_fn`, which is unsafe in a process that runs the PTY drain threads — so
masking there would hand the host child an uncaptured signal mask, which this runner will not do.
What remains for both is the last store of an already-owned handle: the interpreter can deliver an
interrupt between a constructor returning and the append or attribute assignment that puts the
handle where cleanup reads it. That is one bytecode and is not closable from inside the process.

### Drivers

Every host-facing shape below was captured live on 2026-09-13 at Claude Code 2.1.270, codex-cli
0.154.0 and OpenCode 1.18.30 (docs/host-probe-preflight.md, that section). A submission path no
capture covers, or an attempt that never reached the host, raises `SubmissionUncaptured` rather
than being guessed; the per-driver cases are listed below. Host behaviour the trials themselves
measure (what an approval-parked session lists as, mid-turn queue delivery) is not refused.

- **Claude** (`ClaudeDriver`). `create()` runs `claude --bg --model <m> '<prompt>'` from the
  probe directory, mints the short id from stdout line 1 (`backgrounded · <id>`) before checking
  the exit status or running the confirming `claude agents --json --all --cwd <probe cwd>`
  listing, and on a `subprocess.TimeoutExpired` mints from the partial output before re-raising.
  Every listing row must carry the exact validated probe cwd before session binding or further
  driving; the command's `--cwd` filter alone is not provenance evidence.
  If creation supplies no recognized ID, a bounded cwd-filtered listing reports candidates for
  manual investigation without adoption, excluding Claude sessions already owned by any driver
  in the shared registry. Another host's namespaced key does not suppress an unowned Claude
  candidate. This covers changed output on either exit status, timeout output lacking an ID,
  and interruption. Failed recovery preserves the original error.
  Cancellation preserves the original interrupt and its manual-investigation note even if a
  second interrupt aborts the bounded recovery listing.
  It then waits for that listing to report the creation turn finished (`state: "done"`) before
  returning: `claude --bg` returns while the turn is still working — the captured listing taken
  immediately afterwards reads `state: "working"` and the creation reply landed 12 s later — so a
  trial submitting straight away would stamp `submitted_at` before that reply arrived and count
  the creation turn's own assistant record as its `turn_start` and serving model. `state` is the
  boundary the host publishes; `status` read `idle` throughout the same turn. Every trial state
  settles there rather than in a settle callback, and a turn still running at the 180 s cap is an
  error, with the session left owned for the sweep. `status(id)`
  returns the owned entry from that listing (`pid`, `status`, `state`, `sessionId`) for settle
  callbacks. Every listing row must be a background-session object with nonempty string
  identity/state fields and an explicit `pid` that is null or a positive integer. Short IDs must
  have the captured eight-hex-digit shape and full IDs the lowercase hyphenated UUID shape.
  The short ID must equal the full UUID's first eight digits, as captured; once bound, the full
  UUID cannot change under the same owned short ID. Neither malformed nor changed bindings
  may redirect transcript reads. Assistant content parts must use the captured `text` or
  `thinking` types; missing, malformed or unknown types make the record unusable instead of
  silently hiding a marker and producing negative evidence. Malformed rows,
  duplicate IDs and missing PIDs fail the listing; they never prove that a session is absent or
  stopped and cannot authorize `rm` after a failed `stop`. A transcript candidate disappearing
  between discovery and stat makes that read unobservable; later polls retry discovery and a
  version read remains unknown. Identity-binding/listing failures remain fatal lifecycle errors.
  `observe()` reads the session's own transcript
  (`$HOME/.claude/projects/*/<sessionId>.jsonl`): `user` records carry a string `content`,
  `assistant` records a list of typed parts, both a UTC ISO `timestamp`. Every message record is
  bound to the owned full UUID and exact probe cwd by its captured `sessionId`
  and `cwd`. Missing or conflicting bindings invalidate the entire snapshot before outcomes,
  serving model or version can be extracted; a matching filename alone supplies no evidence.
  Content shapes are enforced per role — an assistant string or a user part list is a changed transcript and counts
  unusable, rather than letting a marker in the wrong shape establish an acknowledgement.
  Bookkeeping record types are skipped, a malformed message record makes the read unobservable.
  `submit()` reads
  the listing first; a listing that times out is `SubmissionUncaptured`, since nothing was sent.
  It has two mechanisms. `attach` opens `claude attach <id>` under a PTY, waits for the composer
  (a line holding only `❯` and a no-break space, the captured ready screen) to appear and the
  output to stay quiet for 3 s, the capture's own criterion, then types the message in short
  chunks with a separate Enter and **keeps the client attached**; it returns `None`, because a
  PTY write is never host acceptance. The detach is the sweep's job: `close_clients()` sends
  Ctrl-Z (captured to detach with the session still running) to every held client. For busy and
  approval trials, the settle callback first calls `attach(id)` while idle, then establishes the
  precondition through that client. Submission reuses that live, ready client for the exact owned
  session without waiting for idle again. Only a successful initial readiness check associates a
  client with its session; an unready or exited client is never reused. Without such a client,
  mid-turn attachment is refused because its readiness screen has not been captured. A write that
  fails for anything but an already-exited client is collected as a cleanup failure, like a
  failed close, rather than escaping and aborting the sweep before anything is closed or
  reported. The capture
  detached only after the reply was displayed — the user record landed at +0.27 s and the
  assistant reply at +2.6 s — so detaching as soon as the user record appeared would run every
  trial under an uncaptured mid-turn detach and make a missing `turn_start` or `ack`
  unattributable. A client that exits while typing is
  `SubmissionUncaptured`, whichever way the exit surfaces: the drain thread's EOF flag refuses the
  write, or the write reaches `os.write` first and fails there. Both leave the line partly typed
  with no Enter guaranteed, so both are the same uncaptured submission — the second only looks
  different because it arrives as an `OSError`. After typing succeeds, observation retains and
  rechecks that exact client. Exit, removal, replacement or changed session binding makes missing
  transcript outcomes unobservable even when the persisted transcript remains readable; positive
  transcript evidence still stands. `resume` runs `claude --bg --resume <sessionId> '<msg>'` with no other
  flags against a *stopped* session with `pid` null and `state: "done"` (the captured restarted
  path). A missing PID with a still-working or unknown state is refused before resuming, as is
  a running session (`pid` set), since with flags, or against a running session, the captured result
  is a copy under a new id — which, when stdout names one anyway, is minted whatever the exit
  status so the sweep removes it, and reported as a rejection. If the original ID is backgrounded
  before a nonzero exit, acceptance is ambiguous: it is unobservable and transcript polling
  continues, since that daemon may already have received the marker. Each resumed observation
  checks the cwd-filtered listing. The first post-submission observation retains the daemon PID;
  a missing, replaced or unreadable PID makes missing delivery outcomes unobservable for the
  remainder of that attempt, including if the daemon disappeared before the first poll. Positive
  transcript evidence still stands. This also covers ambiguous exits and timeouts, without adding
  listing latency to recorded command acceptance. A copy named in the partial output
  of a *timed-out* resume is uncaptured rather than a plain timeout: the message went to the copy,
  so polling the trial's own session would turn its absent marker into `not_observed` for a
  session that was never asked. `stop(id)` runs `claude stop` and
  requires the listing to show the entry gone or `pid` null, whatever the exit status said, but
  neither removes nor releases the session: it is the restarted cell's settle step before a
  `resume` submission. `teardown()` is `stop()` followed by `claude rm`; a surviving `pid` raises
  before `rm` runs and ownership is retained. `version()` is the `version` field the session's own message records carry,
  never `claude --version`: the binary drifted 2.1.267 → 2.1.270 over three days of captures
  and a daemon started before an upgrade keeps its code. No spend bound exists for Claude cells:
  `--max-budget-usd` needs `--print`, which conflicts with `--bg`, and `--model haiku` was
  the only captured model argument; other values are rejected before any host call. That argument was
  captured *not* being honoured (the assistant records name `claude-sonnet-5`); the matrix reads
  the serving model from those records' `message.model`, carried onto `TrialRun.model` (below).
  The session also runs under the operator's default
  permission mode, a wider authority surface than the Codex cells' `-s read-only -a never`.
- **Codex** (`CodexDriver`). `create()` runs
  `codex exec --json -s read-only --skip-git-repo-check -C <probe cwd> '<prompt>'` with stdin
  closed, mints a unique `thread.started.thread_id` before checking exit status, including partial
  timeout output, and returns once the turn ends. Multiple distinct creation IDs grant no
  ownership; every candidate is reported for investigation without deletion authority. Malformed
  JSON, non-object records and unusable `thread.started` records fail creation even if another
  event names a valid thread. Event types are limited to captured `thread.started`, `turn.started`,
  `item.completed` and `turn.completed`; missing, malformed and unknown discriminators fail too.
  A unique creation ID remains owned for cleanup. Codex creation IDs
  must have the captured lowercase hyphenated UUID shape before any ownership is granted. Both
  Codex and Claude transcript lookups reject non-UUID IDs and escape literal home/ID components
  before globbing, so metacharacters cannot select another session's transcript. The thread is
  then idle with no live process. A rollout that disappears between discovery and its metadata
  read makes that poll unobservable and its version unknown; later polls retry discovery.
  Rollout filenames only locate candidates: before outcomes or version are extracted, every
  `session_meta` must carry the owned UUID in both captured `payload.id` and `payload.session_id`
  fields, and the exact probe cwd in `payload.cwd`. Missing or conflicting metadata makes the
  entire snapshot unobservable, including positive evidence, and leaves the version unknown.
  `observe()` reads the rollout
  (`$CODEX_HOME/sessions/YYYY/MM/DD/rollout-*-<thread_id>.jsonl`): `response_item`/`message`
  records with roles `user`/`assistant` (`developer` carries fixed instructions and is skipped),
  and `event_msg` `task_started`/`task_complete`/`turn_aborted` records as `turn_start`/`turn_end`
  pseudo-role events, so on Codex `turn_start` is the host's own boundary. A user record wraps
  the prompt in the host's injected text, so the marker is searched for, never matched whole.
  `submit()` runs `codex queue --thread <id> --message <text>`; exit 0 is acceptance, nonzero a
  `SubmissionRejected` with the stderr. A queued item is delivered by whichever process next
  serves the thread (captured: a live idle TUI within ~7 s; a `resume` at its start), so with
  mechanism `queue` a settle callback must have opened `attach(thread_id)` beforehand — a
  `codex --no-alt-screen -s read-only -a never -C <probe cwd> resume <thread_id>` client
  under a PTY, held on the driver until the sweep closes it. With no *live* client open — none
  held, or every held one already exited — `submit()`
  raises `SubmissionUncaptured` before queueing anything, since the trial could only time out.
  With mechanism `queue-then-resume` (the restarted cell) `submit()` opens that client itself
  right after queueing and returns the time `codex queue` exited, so the resume client's startup
  is not counted against the 10 s acceptance window; a `codex queue` that times out there is
  `SubmissionUncaptured`, since nothing serves the thread yet. Ordinary queue submission requires
  a successfully readied live client for the exact owned thread: another owned thread's client
  or a held client whose readiness was never established cannot authorize queueing.
  The exact client is retained across queueing and observation: if it exits, is removed, or
  loses its thread association, missing delivery outcomes become unobservable. A replacement
  client cannot mask that loss. Exit-zero acceptance and positive rollout evidence remain valid.
  `attach()` answers the captured
  first-run trust dialog with Enter (which persists a trust entry for the probe directory in
  `$CODEX_HOME/config.toml`). Detection requires the captured question, warning and numbered
  Yes/No choices ending in `Press enter to continue` at the output tail. A question quoted in
  history, or an old dialog followed by a newer composer, cannot trigger Enter. It waits for
  the composer placeholder `› Ask Codex to do anything`
  drawn after that answer plus 3 s of quiet — the placeholder is drawn while a turn or the dialog
  is still up — and raises `PtyNotReady` with the stripped screen otherwise; inside `submit()`
  queue exit-zero acceptance is retained even when resume startup or readiness fails, including
  OS-level PTY/fork failures and runtime thread-start failures. Interruption still propagates and
  runs cleanup. The diagnostic records
  that failure, and only missing delivery outcomes become unobservable. A client that exits on the dialog, so that the Enter cannot
  be written at all, is that same not-ready case and is named as one, rather than escaping as the
  raw `OSError` `os.write` produced. The default `-a never` cannot produce an
  approval prompt. Other sandbox/approval arguments are rejected before launching a client;
  approval-state support needs a separate capture before the runner can accept new options.
  `teardown()` runs
  `codex delete --force <id>` and releases only on exit 0; what it does to a still-queued item is
  uncaptured. `version()` is the rollout's first `session_meta.payload.cli_version` — the
  creating `exec`'s, not necessarily the resume client's — and never `codex --version`, which
  has disagreed with a rollout on the same day. Explicit Codex model overrides are refused at
  driver construction: only the default-model command has been captured, so no `-m` path is emitted.
- **OpenCode** (`OpenCodeDriver`). Construction rejects every model except the captured
  `opencode/ling-3.0-flash-fin-free` before any host call. `create()` runs `opencode run --pure --format json --dir
  <probe cwd> --title <t> -m opencode/ling-3.0-flash-fin-free '<prompt>'` (the captured
  zero-cost model; the default one failed on a stale credential) and mints a unique `sessionID`
  from the events before checking for an `error` event or a nonzero exit — a failing turn
  still creates the session. Creation requires one consistent ID throughout the stream. Multiple
  distinct IDs, including in timeout output, grant no ownership: all candidates are reported by
  cleanup for manual investigation. Nonempty malformed JSON or non-object event lines fail
  validation, as do event types other than captured `step_start`, `text`, `step_finish` and `error`.
  Every event needs a nonempty string `sessionID`, and a present `part` must carry that same
  session identity. Success events (`step_start`, `text`, `step_finish`) require that part;
  the captured `error` event may omit it. Conflicting top-level
  and part IDs grant no creation ownership; attach reports the unexpected ID without adoption.
  Malformed events fail
  creation and make attach submission `SubmissionUncaptured`, regardless of exit status or
  timeout; a matching ID elsewhere cannot make an unreadable stream trustworthy. A unique
  creation ID remains available for cleanup after malformed output fails the command.
  `submit()` needs `serve()` open: `opencode serve --pure --port <p>`
  on a free loopback port chosen per call, refused if the port already answered (the session
  store is global, so a stranger's server would look identical). Readiness requests disable
  inherited HTTP proxy handling. Any HTTP response, including
  4xx/5xx, proves a listener exists and prevents spawning. Our child must first emit the captured
  complete stdout line `opencode server listening on http://127.0.0.1:<p>` for its own URL;
  only then can `GET /session` returning the captured 200 while it is still alive prove readiness.
  This prevents a listener racing the preflight check from standing in for a child that failed
  to bind. Other statuses fail startup. The owned stdout pipe is continuously drained with a
  bounded 16 KiB partial-line buffer, including after readiness; oversized lines are discarded.
  A failed drain or EOF without that line fails startup. Pipe/thread setup is inside the held
  child's cleanup boundary, and successful process-group cleanup also closes its reader.
  Read timeouts after spawning retry within the startup deadline. A preflight read timeout
  remains a refusal: it cannot prove that the port is free of another listener.
  The password variable is uncaptured, so the server is
  the captured unsecured loopback listener for the trial's duration. Session ownership protects
  what this driver may operate on; it does not authenticate local API clients. Other local
  principals can reach that listener and could read or alter evidence. Before new live OpenCode
  matrix trials, capture authenticated server, attach and readiness behavior together; do not
  infer support from the password warning or treat loopback binding as authenticated isolation.
  This captured-path rewrite adds no guessed authentication settings. The child is spawned and
  held in one statement inside the handler that closes it, so any failure or Ctrl-C during that
  wait closes it before re-raising — and, as in `close_servers()`, the handle is dropped only
  once the close succeeded, so a child that survived SIGKILL stays held for the sweep to retry
  and report. Closing means the whole process group, SIGTERM then SIGKILL, each waited out with
  signal 0 against the group rather than with the Popen handle: the child is spawned in a session
  of its own, so a descendant that outlives it stays in that group, keeps the port bound and
  keeps answering on the same URL. Judged by the handle alone that reads as a clean stop, the
  handle is released, and the next `serve()` refuses the run's own leftover as a stranger's
  server. Once the leader has been reaped, its numeric process-group ID may be reused. Cleanup
  then probes existence only: a remaining group is reported for manual investigation and the
  handle retained, with no further SIGTERM/SIGKILL. This can require manual cleanup of genuine
  descendants, but prevents signaling unrelated processes through a reused number. Server
  lifecycle operations are serialized, so an unreaped leader reserves the ID between the check
  and each signal. Submission is
  `opencode run --pure --format json --attach http://127.0.0.1:<p> --session <id> -m <model>
  '<msg>'`, judged on both the exit status and the event stream. A structured `error` event is
  `SubmissionUncaptured` on both zero and nonzero exits — only a nonzero exit without an error
  event and with a live serve child is a rejection. The exit-zero-with-an-error shape is the one `create()`
  already handles (captured: a stale credential), so trusting the exit status alone would record
  a provider, credential or model failure as accepted and later blame the host for the transcript
  outcomes that never arrive; on a nonzero exit the event's text joins the stderr in the
  rejection's diagnostic. If the `serve` child has exited — before the call, by the time a
  nonzero exit comes back, or by the time the call times out — it is `SubmissionUncaptured`
  instead, since a dead server says nothing about the host. The timeout case matters because the
  export is readable without the server: a bare `TimeoutExpired` would be polled as a submission
  that may have delivered and turn the absent marker into `not_observed`. An exit-zero stream
  must also name the session it was aimed at on every event carrying an ID, including later
  events in a partial timeout stream — the captured attach emitted a `step_start` event
  carrying its `sessionID` — so a stream naming a different id, or none at all, is
  `SubmissionUncaptured` too: polling the requested session would otherwise read the missing
  marker as `not_observed` for a host that was never asked. A different id is recorded before the
  refusal and reported by the sweep for manual investigation, never adopted or deleted: an attach
  event can name a pre-existing human session and proves no creation authority. A timeout reads those same two
  signals off whatever the stream had already printed before it fired: an error event, or an id
  that is not this session, makes it `SubmissionUncaptured` rather than a bare timeout, with any
  stray id recorded first. With a live child, empty/whitespace-only output or complete valid
  matching events re-raise the timeout: the message may have reached the session. A nonempty
  malformed final JSON line remains `SubmissionUncaptured`; no tail repair is inferred.
  After submission, observation retains the exact server used for
  the attempt. Its exit, removal or replacement makes missing delivery outcomes unobservable,
  even if the persistent export remains readable; positive export evidence still stands. `observe()` and
  `version()` read
  `opencode --pure export <id>`: `messages[].info.role`/`info.time.created` (ms epoch, used for
  user messages and the earliest assistant activity), assistant `info.time.completed`, and the
  `text` parts. Acknowledgement uses assistant completion as a conservative upper bound on text
  emission, separately from creation. Missing, malformed or backward completion times cannot date
  an acknowledgement; creation may still establish assistant activity. A reply completed after
  its window remains late even when its message was created inside the window.
  If multiple matching replies complete out of creation order, the earliest completion wins.
  Before observation or version extraction, the captured top-level `info.id`,
  together with `info.directory`, must bind the owned session and exact validated probe cwd;
  each message's `sessionID`, and each part's `sessionID` must name the owned session; each
  part's `messageID` must name its enclosing message, whose ID must be nonempty and unique.
  A missing or inconsistent binding makes the whole export unreadable, with no outcomes,
  model or version taken from it. User messages accept only captured `text` parts; assistant
  messages also accept `step-start`, `reasoning` and `step-finish`. Untyped, malformed,
  role-incompatible and unknown parts make their message unusable; an unparseable
  or malformed document is unobservable. Successful creation already records a user/assistant
  turn, so exports lacking parsed records of either role cannot establish negative delivery
  evidence. Positive events in such a partial snapshot remain usable. This checks the minimum
  captured shape, not completeness of the entire history. `teardown()` is
  `opencode --pure session delete <id>`. The server is closed by the sweep after teardown.

Readiness patterns and typing cadence live in `PtyClient`, which wraps `wake_probe.PtyProcess`
for fork/exec and teardown only. Its output is drained continuously by a background thread into
a bounded window: a client held open through a 900 s busy trial would otherwise either fill the
kernel pty buffer and stall the host (a Codex TUI blocked on stdout is the only process serving
its thread's queue) or trip `PtyProcess`'s hard transcript cap mid-trial. Input goes through
`os.write` directly, never `PtyProcess.send`, whose guard demands an observed idle state — a
trial's state is the settle callback's precondition, not something read off the screen, and an
attach to a busy or approval-parked session must still be able to type. Readiness is a property
of a *live* client: a child that drew its composer and then exited returns `False` from
`wait_for` even though the pattern matched, because it serves nothing — retaining such a client
would let a nonempty client list stand in as proof that a queued Codex message has a serving
process. A failed drain-thread `select` also invalidates the client: readiness, reuse and writes
all refuse it, while cleanup retains responsibility for the child. A client is launched and held
in one statement (`Driver.open_client()`), and `PtyClient`
closes the child itself if its own construction fails after the child started: the client object
is the only handle to an authenticated host process, so a gap between creating it and holding it
would leave that process serving a session the sweep was tearing down. The client is only ever
exercised by controlled Python children in tests. Windows for a PTY-delivered submission include
the client's own startup (captured: 3.2 s to Claude's composer), since `submitted_at` is stamped
when `submit()` is called. Codex's resume client (captured: 15 s to ready while it drained queued
items) is opened after acceptance is stamped, so only its transcript windows absorb that startup.

### One trial

The marker each trial submits is a fresh high-entropy token (`marker_token()`), so an
acknowledgement can never be satisfied by chance text or a terminal echoing the input back —
`detect_outcomes()` only counts an assistant event as `ack` when the marker itself appears in it,
distinct from `turn_start`, which any assistant activity satisfies. An event without a usable
timestamp is never placed in the window and makes the read unobservable: an undated entry cannot
be ordered against submission, and counting it would let the creation prompt's own reply stand in
as this trial's `turn_start`.

Each signal uses its earliest eligible timestamp, independent of record order. Later snapshots
can supply an earlier timestamp too; merging observations preserves that earlier evidence,
including the prior turn's completion that sets busy-trial windows.

`submit()`'s result drives acceptance. `True` is accepted, stamped when `submit` *returns* (a
submission that blocks for seconds is not backdated into its 10 s window). A number is the wall
time at which the driver itself saw acceptance, for a `submit()` that keeps working afterwards
(Codex `queue-then-resume`); any other value raises `TypeError`. `False` or
`SubmissionRejected` is a real, observed rejection: `accepted` stays observable, but nothing was
delivered, so no polling happens and the transcript outcomes are unobservable rather than polled
to a `not_observed` that would be negative evidence for a marker the host never received.
`None`, or a `subprocess.TimeoutExpired` from the call, means the message went through a channel
with no acceptance signal — a PTY write, a command that may have delivered before its timeout —
so `accepted` alone is unobservable and polling proceeds. `SubmissionUnsupported` says the *host*
lacks the mechanism and classifies every outcome `unsupported`; no driver raises it today, and a
known absence such as Claude's `--channels` (missing from the help text and the plugin cache) is
recorded without a live trial through `classify_trial(..., supported=...)`.
`SubmissionUncaptured` says *this runner* could not vouch for the attempt — an uncaptured path,
a PTY that never showed its composer, a submission nothing can serve — and classifies every
outcome `unobservable`, never `unsupported` and never `not_observed`. The diagnostic behind
any of these (exit status and stderr, the stripped screen, the attach client's own note of when
it typed and what was on screen then) lands in
`TrialRun.submission_diagnostic`.

`run_trial()` keeps observing until every transcript outcome is seen or the longest window
(120 s) has elapsed, merging each poll's evidence and keeping the first timestamp per outcome. A
single immediate snapshot would report `not_observed` for events that arrive comfortably inside
their window, which is precisely the delay the windows exist to measure. Observability is decided
by the *final* poll, not by whether any poll ever succeeded: a transcript is cumulative, so a
late successful read covers earlier gaps, but if the last read failed then the tail of the
window was never seen and a missing outcome is `unobservable` rather than negative. An outcome
already observed keeps its own evidence either way.

A Ctrl-C inside the polling loop, the first `observe()` call included, does not propagate: the
trial finalizes with whatever it had gathered, every still-missing outcome marked unobservable,
and `TrialRun.interrupted` set — a busy trial can spend minutes collecting evidence that a
propagated exception would discard. A caller looping over trials must stop on `interrupted` and
never pass such a run to `aggregate()`, since apart from that flag its fields are identical to
an uncaptured or unreadable run's. Anywhere else a Ctrl-C propagates raw, and so does any other
exception from creation, settle, submit or `observe()`; only a failed version read is recorded
as `None` instead. The cleanup sweep then runs from `run_trial_with_cleanup`'s `finally`; the
registry already names every live session, so no wrapper exception needs to carry the id.

A `busy` trial is the exception to that 120 s ceiling. `wake_probe.py` refuses to rule on
`turn_start` or `ack` for a busy host until the turn already running has ended or 900 s have
passed, so observing only to 120 s would leave those two cells unclassifiable by construction;
a busy trial therefore polls to the 900 s cap. When the transcript reports its own `turn_end`
the deadline collapses back to the last window measured from that boundary, which is what
lets the cell classify without waiting the cap out. If the window closes with no `turn_end`
seen, what happens next depends on whether the host demonstrated a turn-boundary stream at all
during the cap. Without one — Claude and OpenCode, whose transcripts carry no boundary record —
`turn_start` is unobservable: bare assistant activity cannot distinguish a prior turn's tail
from a new turn. An assistant message matching the fresh marker independently establishes
`ack` and is preserved; only a missing acknowledgement is unobservable without that boundary.
With a boundary stream — Codex, whose channel stayed readable through the
whole cap but never emitted the boundary — `Trial.result()`'s own `turn_end_observable` branch
classifies the cell `inconclusive` instead: the channel was readable the whole time and simply
never resolved, a different, positive fact from an unavailable channel.

It returns a named `TrialRun` carrying what `Trial`/`classify_trial` need plus the evidence a
matrix cell must cite alongside them — `session_id`, `marker`, `version`, `model`, `signals` (what
established each positive result: a user-role transcript match, an assistant-role match, the
host's own turn-boundary event, or the submit command's exit status), `submission_diagnostic`,
`interrupted` (the full field list is the dataclass in `host_trials.py`). `model` is the model
the host itself recorded as serving *this trial's* turn — Claude's `message.model`, OpenCode's
`<providerID>/<modelID>`, and `None` on Codex, whose rollout names none. It is read from the
first in-window assistant message and carried on the result because
`run_trial_with_cleanup()` deletes the session before a caller could go back for it. A busy trial
on a host with no turn-boundary stream records `None` instead: the reading came off the same
assistant record whose `turn_start` is unobservable there, and it may belong to the
turn that was already running. A cell whose
`model` is `None` says the model is unknown; it never repeats what `-m`/`--model` asked for,
since `--model haiku` was captured not being honoured. The requested `state`
is validated and carried into the result, but establishing a busy/approval/disconnected/restarted
precondition is the caller's `settle` callable — omitting `settle` for any non-`idle` state
raises `ValueError` immediately rather than silently exercising an idle host under that label.
A settle callback works only through the methods of the driver instance being swept (`attach`,
`status`, `stop`, `submit`), so every session it touches is owned and every client it opens is
held where the sweep finds it: a settle that ran `claude --bg --resume <flags>` itself would start
a copy nothing mints and nothing sweeps.

One documented deviation from `Trial`'s "same monotonic clock" contract: `wake_probe.py`'s own
PTY capture stays in one process and can use `time.monotonic()`, but a real host's transcript
carries only wall-clock timestamps from another process, and no monotonic-to-wall calibration
exists to convert them. Every *compared* value therefore comes from `time.time()`;
`Trial.result()` only requires one consistent clock across `submitted`/`outcomes`/`now`, not
monotonicity, so this is safe as long as every value on a given `Trial` uses the same clock.
The local polling deadline is the exception and uses `time.monotonic()`, because an elapsed
interval measured inside this process must not move when NTP steps the clock.

A wall-clock step mid-trial still distorts the recorded timestamps themselves, and no rebasing
can undo it: the host wrote those stamps from another process, on the clock as it then read. So
the runner detects the step instead of correcting it. Both the baseline and each poll bracket
the wall-clock read with monotonic reads. Those brackets bound the possible elapsed-time
difference, so descheduling between reads widens uncertainty instead of declaring a clock step.
The same bracketed check runs before returning a submission rejection, which skips polling;
a clock correction during the rejected command cannot turn elapsed wall time into a result.
Interrupted polling rechecks drift before retaining partial evidence too; a correction during
the interrupted sleep or observation invalidates timestamps and model attribution.
The trial ends only when that entire interval diverges past one second — far above the 500 ppm NTP
slew ceiling, which is 0.45 s across the whole 900 s busy cap, and far below the smallest 10 s
window: every outcome `unobservable`, every outcome timestamp and the serving model dropped
because their attribution used the shifted timeline, and the divergence recorded on `TrialRun` as
`clock_step` as the minimum proven divergence for the cell to cite in their place. A correction
inside a sampling bracket's uncertainty cannot be distinguished from scheduling delay. One case
stays outside that: a step backwards
large enough to put the caller's later `now` before submission, which `Trial.result()` refuses
outright as an invalid observation clock. That is a refusal, not a classification, so it fails
closed in the same direction.

## Real-host evidence still required

Probe Claude Channels and plain MCP separately, Codex queue and reachable IPC separately, and
OpenCode as the intended third host. Pin installed versions and check provider availability before
running disposable direct-host sessions. Upstream reports are hypotheses to reproduce, not results.
Missing prerequisites must be recorded and resolved or explicitly scoped out by the owner.
No herdr installation is assumed. Publish sanitized manifests and observations here or in durable
issue-linked artifacts; never copy real credentials, administrator sessions or ordinary work
messages into fixtures. The recorder does not provision host authentication.

Claude's `--channels` mechanism does not exist at the installed version (`claude --version`
`2.1.268`): absent from `claude --help`, and no channels plugin is present in the official
marketplace cache. The two upstream reports motivating this investigation describe a mechanism
this installation does not have; that is a legitimate `unsupported` classification for that
mechanism at this version, not evidence about Channels generally or about other versions.

Direct-host investigation does not connect live sessions through Parley. That connection still
requires the complete fixture gate in [AGENTS.md](../AGENTS.md#record-evidence-and-decisions),
production account evidence where applicable, and the separately authorized human smoke test.

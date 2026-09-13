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

Trials run under the real HOME so the host session is actually authenticated, per the owner's
2026-09-11 decision — disposable at the *session* level, not the HOME level. The runner has no
disposable mode: its drivers call `subprocess.run` without an `env=` argument, so every host
command inherits the operator's environment, and its PTY clients inherit it plus
`TERM=xterm-256color` and `PWD=<probe cwd>`; `wake_probe.py --home inherit` is the PTY
recorder's equivalent and applies only to PTY captures. Every matrix cell this produces
therefore carries the developer's real credentials and config; it is not the clean-room
isolation `--home disposable` gives the PTY fixtures above. Each driver refuses at construction
any probe directory that is not empty (a bare `.git` is allowed): the directory keys a Codex trust
entry in `$CODEX_HOME/config.toml`, the slug of Claude's transcript directory, and whatever a real
repository's contents would feed the model.

Cleanup is part of the module, not left to a caller: `run_trial_with_cleanup()` is the entry
point real trials use. It runs `run_trial()` and, on every exit path, `sweep()`s the driver:
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
unrecoverable and unnamed. Every id stays owned and is reported alongside the client's failure. A sweep covers the driver instance it is given: another instance over the
same registry sees the same ids but not the first one's held clients, servers or cached Claude
`sessionId`s, so sweep every instance that did work. One gap stays open by design: a Ctrl-C while
`claude --bg`, `codex exec` or `opencode run` is still running loses that command's output, and a
session the host created in that instant is findable only by a human (`claude agents --json
--all --cwd <probe cwd>`; the newest rollout under `$CODEX_HOME/sessions` naming the probe cwd;
the newest `parley-probe-*` row of the global `opencode --pure session list`, removed with
`opencode --pure session delete <id>`).

### Drivers

Every host-facing shape below was captured live on 2026-09-13 at Claude Code 2.1.270, codex-cli
0.154.0 and OpenCode 1.18.30 (docs/host-probe-preflight.md, that section). A submission path no
capture covers, or an attempt that never reached the host, raises `SubmissionUncaptured` rather
than being guessed; the per-driver cases are listed below. Host behaviour the trials themselves
measure (what an approval-parked session lists as, mid-turn queue delivery) is not refused.

- **Claude** (`ClaudeDriver`). `create()` runs `claude --bg --model <m> '<prompt>'` from the
  probe directory, mints the short id from stdout line 1 (`backgrounded · <id>`) before checking
  the exit status or running the confirming `claude agents --json --all --cwd <probe cwd>`
  listing, and on a `subprocess.TimeoutExpired` mints from the partial output before re-raising. `status(id)`
  returns the owned entry from that listing (`pid`, `status`, `state`, `sessionId`) for settle
  callbacks. `observe()` reads the session's own transcript
  (`$HOME/.claude/projects/*/<sessionId>.jsonl`): `user` records carry a string `content`,
  `assistant` records a list of typed parts, both a UTC ISO `timestamp`. Those shapes are
  enforced per role — an assistant string or a user part list is a changed transcript and counts
  unusable, rather than letting a marker in the wrong shape establish an acknowledgement.
  Bookkeeping record types are skipped, a malformed message record makes the read unobservable.
  `submit()` reads
  the listing first; a listing that times out is `SubmissionUncaptured`, since nothing was sent.
  It has two mechanisms. `attach` opens `claude attach <id>` under a PTY, waits for the composer
  (a line holding only `❯` and a no-break space, the captured ready screen) to appear and the
  output to stay quiet for 3 s, the capture's own criterion, then types the message in short
  chunks with a separate Enter and **keeps the client attached**; it returns `None`, because a
  PTY write is never host acceptance. The detach is the sweep's job: `close_clients()` sends
  Ctrl-Z (captured to detach with the session still running) to every held client. The capture
  detached only after the reply was displayed — the user record landed at +0.27 s and the
  assistant reply at +2.6 s — so detaching as soon as the user record appeared would run every
  trial under an uncaptured mid-turn detach and make a missing `turn_start` or `ack`
  unattributable. A client that exits while typing is
  `SubmissionUncaptured`. `resume` runs `claude --bg --resume <sessionId> '<msg>'` with no other
  flags against a *stopped* session (the captured restarted path); a running session (`pid` set)
  is refused as uncaptured, since with flags, or against a running session, the captured result
  is a copy under a new id — which, when stdout names one anyway, is minted whatever the exit
  status so the sweep removes it, and reported as a rejection. `stop(id)` runs `claude stop` and
  requires the listing to show the entry gone or `pid` null, whatever the exit status said, but
  neither removes nor releases the session: it is the restarted cell's settle step before a
  `resume` submission. `teardown()` is `stop()` followed by `claude rm`; a surviving `pid` raises
  before `rm` runs and ownership is retained. `version()` is the `version` field the session's own message records carry,
  never `claude --version`: the binary drifted 2.1.267 → 2.1.270 over three days of captures
  and a daemon started before an upgrade keeps its code. No spend bound exists for Claude cells:
  `--max-budget-usd` needs `--print`, which conflicts with `--bg`, and `--model haiku` was
  captured *not* being honoured (the assistant records name `claude-sonnet-5`); the matrix
  reads the serving model from those records. The session also runs under the operator's default
  permission mode, a wider authority surface than the Codex cells' `-s read-only -a never`.
- **Codex** (`CodexDriver`). `create()` runs
  `codex exec --json -s read-only --skip-git-repo-check -C <probe cwd> '<prompt>'` with stdin
  closed, mints `thread.started.thread_id` as soon as it is seen (before the exit status is
  checked, and from partial output on a timeout), and returns once the turn ends — the thread is
  then idle with no live process. `observe()` reads the rollout
  (`$CODEX_HOME/sessions/YYYY/MM/DD/rollout-*-<thread_id>.jsonl`): `response_item`/`message`
  records with roles `user`/`assistant` (`developer` carries fixed instructions and is skipped),
  and `event_msg` `task_started`/`task_complete`/`turn_aborted` records as `turn_start`/`turn_end`
  pseudo-role events, so on Codex `turn_start` is the host's own boundary. A user record wraps
  the prompt in the host's injected text, so the marker is searched for, never matched whole.
  `submit()` runs `codex queue --thread <id> --message <text>`; exit 0 is acceptance, nonzero a
  `SubmissionRejected` with the stderr. A queued item is delivered by whichever process next
  serves the thread (captured: a live idle TUI within ~7 s; a `resume` at its start), so with
  mechanism `queue` a settle callback must have opened `attach(thread_id)` beforehand — a
  `codex --no-alt-screen -s <sandbox> -a <approval> -C <probe cwd> resume <thread_id>` client
  under a PTY, held on the driver until the sweep closes it. With no *live* client open — none
  held, or every held one already exited — `submit()`
  raises `SubmissionUncaptured` before queueing anything, since the trial could only time out.
  With mechanism `queue-then-resume` (the restarted cell) `submit()` opens that client itself
  right after queueing and returns the time `codex queue` exited, so the resume client's startup
  is not counted against the 10 s acceptance window; a `codex queue` that times out there is
  `SubmissionUncaptured`, since nothing serves the thread yet. `attach()` answers the captured
  first-run trust dialog with Enter (which persists a trust entry for the probe directory in
  `$CODEX_HOME/config.toml`), waits for the composer placeholder `› Ask Codex to do anything`
  drawn after that answer plus 3 s of quiet — the placeholder is drawn while a turn or the dialog
  is still up — and raises `PtyNotReady` with the stripped screen otherwise; inside `submit()`
  that becomes `SubmissionUncaptured`. The default `-a never` cannot produce an
  approval prompt, so an approval settle has to ask for other flags. `teardown()` runs
  `codex delete --force <id>` and releases only on exit 0; what it does to a still-queued item is
  uncaptured. `version()` is the rollout's first `session_meta.payload.cli_version` — the
  creating `exec`'s, not necessarily the resume client's — and never `codex --version`, which
  has disagreed with a rollout on the same day.
- **OpenCode** (`OpenCodeDriver`). `create()` runs `opencode run --pure --format json --dir
  <probe cwd> --title <t> -m opencode/ling-3.0-flash-fin-free '<prompt>'` (the captured
  zero-cost model; the default one failed on a stale credential) and mints the first `sessionID`
  seen on any event before checking for an `error` event or a nonzero exit — a failing turn
  still creates the session. `submit()` needs `serve()` open: `opencode serve --pure --port <p>`
  on a free loopback port chosen per call, refused if the port already answered (the session
  store is global, so a stranger's server would look identical), ready once `GET /session`
  answers while the child is still alive; the password variable is uncaptured, so the server is
  the captured unsecured loopback listener for the trial's duration. Any failure or Ctrl-C during
  that wait closes the child before re-raising. Submission is
  `opencode run --pure --format json --attach http://127.0.0.1:<p> --session <id> -m <model>
  '<msg>'`, exit status as acceptance; if the `serve` child has exited, before the call or by the
  time a nonzero exit comes back, it is `SubmissionUncaptured` instead, since a dead server says
  nothing about the host. `observe()` and `version()` read
  `opencode --pure export <id>`: `messages[].info.role`/`info.time.created` (ms epoch, used for
  both roles so turn_start is the earliest assistant activity as on the other hosts) and the
  `text` parts; an unparseable or malformed document is unobservable. `teardown()` is
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
process. The client is only ever
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
`turn_start` and `ack` are marked `observable=False` unconditionally, even when one was
captured: with no independent boundary this runner cannot tell a still-running prior turn's
tail from a genuinely new one, so a bare capture is exactly that ambiguity rather than
trustworthy evidence either way. With one — Codex, whose channel stayed readable through the
whole cap but never emitted the boundary — `Trial.result()`'s own `turn_end_observable` branch
classifies the cell `inconclusive` instead: the channel was readable the whole time and simply
never resolved, a different, positive fact from an unavailable channel.

It returns a named `TrialRun` carrying what `Trial`/`classify_trial` need plus the evidence a
matrix cell must cite alongside them — `session_id`, `marker`, `version`, `signals` (what
established each positive result: a user-role transcript match, an assistant-role match, the
host's own turn-boundary event, or the submit command's exit status), `submission_diagnostic`,
`interrupted` (the full field list is the dataclass in `host_trials.py`). The requested `state`
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
interval measured inside this process must not move when NTP steps the clock. A wall-clock step
mid-trial still distorts the recorded timestamps themselves; that is a known limitation of
cross-process evidence, not something the runner can correct.

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

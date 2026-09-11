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
directory, passes only HOME/PATH/TERM, and never writes input. `--home inherit` opts into the
real HOME/environment instead, for driving an authenticated host session; the record's
`home_mode` field makes that choice visible rather than implicit, and no HOME cleanup runs for
it. `--cwd` overrides the child's working directory, and the record's `cwd` field carries the
resolved directory the child actually ran in — host behaviour varies with repository-level
configuration and instructions, so without it two captures of the same command in different
directories are indistinguishable and cannot satisfy the reproduction requirement below.
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
is separate evidence, landed in a later change once trials actually run.

Every session it creates is tracked in a `SessionRegistry`; `submit`/`observe`/`teardown` refuse
an id the registry did not itself mint (`ForeignSessionError`). This matters concretely: `claude
agents --json --all` lists every background session on the workstation, including ordinary human
work, so a driver bug here could otherwise stop or message someone else's session. Trials run
under the real HOME (`wake_probe.py --home inherit`, or equivalently a driver's own `run`
callable inheriting the environment) so the host session is actually authenticated, per the
owner's 2026-09-11 decision — disposable at the *session* level, not the HOME level. Every
matrix cell this produces therefore carries the developer's real credentials and config; it is
not the clean-room isolation `--home disposable` gives the PTY fixtures above.

Outcome detection is grounded in captured real output, not assumed formats:
- `claude agents --json [--all] [--cwd <dir>]` uses different field names by session `kind`: a
  `background` session (created by `claude --bg`, what this runner uses) reports `state`
  (e.g. `"done"`); an `interactive` session reports `status` instead. `background_sessions()`
  filters to the former; nothing here ever touches the latter.
- `claude logs <id>` fails once a background session's daemon has exited — observed as
  `connect ENOENT /tmp/cc-daemon-*/*/control.sock` against a session already `state: "done"`.
  Observation must happen *before* teardown; a failed read returns an `Observation` marked
  `observable=False`, which classifies `unobservable`. An empty outcome map is not enough: with
  observability defaulting to true, "nothing read" would become `not_observed`, i.e. negative
  evidence from a channel that was never available.
- `claude logs` output is **assumed** to carry no per-entry timestamp — a hypothesis to
  reproduce, since the one read attempted here failed against an already-exited daemon
  (docs/host-probe-preflight.md, 2026-09-11) — so a reachable Claude transcript is treated as
  `unobservable` today: an undated entry cannot be ordered against submission, and
  counting it would let the assistant turn produced by the session-creation prompt stand in as
  this trial's `turn_start` before the marker existed. The same fail-closed rule applies to a
  Codex rollout record whose `timestamp` is missing or malformed, and to a line that is not
  valid JSON at all (a truncated or corrupt rollout): opening a file successfully does not
  establish that its transcript was read successfully, so the read is reported unobservable
  rather than yielding an empty outcome map that would classify as `not_observed`. A record
  type this runner has no use for (`token_usage_record`, `world_state`, ...) is not a failed
  read and is skipped silently — `event_msg` is no longer in that set, see the turn-boundary
  rule below. Provenance is stricter still: `rollout_started_at` returns nothing at
  all on an unparseable line, since the line it could not read may be the earliest one.
- No captured mechanism at `2.1.268` submits a message to an *existing* background session:
  `claude --bg` takes its prompt at creation, `attach` is an interactive PTY, and
  `--remote-control` / `--print --input-format=stream-json` are unexercised. `ClaudeDriver.submit()`
  raises `SubmissionUncaptured`, rather than confirming the session is listed and reporting
  acceptance for a marker the host never received. Guessing an unconfirmed submission flag is the
  same evidence violation as guessing OpenCode's export shape; capturing a real path is stage 3
  work. The two refusals are distinct types and classify differently. `SubmissionUnsupported`
  says the *host* has no such mechanism — Claude's `--channels`, absent from both the help text
  and the plugin cache — and yields `supported=False`. `SubmissionUncaptured` says *this runner*
  has captured no path, and yields `observable=False`, so the cell classifies `unobservable`:
  "no trial was performed", never "the host cannot do this". The listing `submit` checks first
  is `claude agents --json --all`, the same call `background_sessions()` makes, and a nonzero
  exit from that listing is a failed read rather than evidence the session is gone.
- A Codex thread is adopted only when its rollout's earliest record timestamp is at or after the
  run's own start (`CodexDriver(started_at=...)`). Minting whatever id a caller passed defeated
  the registry guarantee, since `submit` then queues a message to it — a mistyped id could reach
  an ordinary human thread. This orders a thread against the run; it does not authenticate it.
  "Started at or after this run" is equally true of a human thread opened in the same minute, so
  adoption additionally requires that no *other* rollout under `sessions_root` shares that
  property: `register_existing` walks the tree and raises `ForeignSessionError` when any
  neighbour is an unruled-out rival, counting a neighbour it cannot read or cannot date as a
  rival rather than as an absence. A driver constructed without `sessions_root` cannot run that
  check and is refused outright instead of adopting on the weaker test.
  **Residual risk, not closed by the above:** recency and uniqueness order and isolate a thread,
  they do not prove *this run* created it. If a human opens the only other thread after
  `started_at` and its id is the one passed to `register_existing`, no rival exists and it is
  minted as owned before `submit` ever messages it. Binding adoption to the runner's own creation
  action is out of scope for this stage — `CodexDriver.create()` already refuses rather than
  guess (above) — so the caller passing `existing_session=` is trusted to have just created that
  exact thread itself.
- No captured mechanism *creates* a Codex thread non-interactively either: `codex queue` targets
  a thread that already exists, and no creation output shape has been captured to parse an id
  out of. `CodexDriver.create()` raises `SessionCreationUncaptured`, so a Codex cell is run as
  `run_trial(..., existing_session=<thread id>)` against a thread the caller made themselves.
- Codex's rollout JSONL (`$CODEX_HOME/sessions/YYYY/MM/DD/rollout-*.jsonl`) is one JSON object
  per line; a chat turn is `{"type": "response_item", "payload": {"type": "message", "role":
  "user"|"assistant"|"developer", "content": [{"type": "input_text"|"output_text", "text":
  ...}]}}` with a record-level ISO-8601 `timestamp`. Only `user`/`assistant` roles count as
  transcript turns; `developer` carries fixed instructions, not conversation. Turn boundaries
  are taken from the host rather than inferred: `{"type": "event_msg", "payload": {"type":
  "task_started"|"task_complete"|"turn_aborted"}}` records become `turn_start`/`turn_end`
  pseudo-role events, so on Codex `turn_start` means the host's own task start rather than
  whichever assistant message happened to be read first.
- OpenCode's `export <sessionID>` shape has **no captured sample yet** — no local session existed
  to export from at investigation time (`opencode --pure session list` printed nothing).
  `OpenCodeDriver` raises `NotImplementedError` rather than guess at an unconfirmed format; this
  is the scope gap the owner-recorded third-host requirement still has open.

The marker each trial submits is a fresh high-entropy token (`marker_token()`), so an
acknowledgement can never be satisfied by chance text or a terminal echoing the input back —
`detect_outcomes()` only counts an assistant event as `ack` when the marker itself appears in it,
distinct from `turn_start`, which any assistant activity satisfies.

`run_trial()` keeps observing until every transcript outcome is seen or the longest window
(120s) has elapsed, merging each poll's evidence and keeping the first timestamp per outcome. A
single immediate snapshot — what it took before — reported `not_observed` for events that
arrived comfortably inside their window, which is precisely the delay the windows exist to
measure. Observability is decided by the *final* poll, not by whether any poll ever succeeded:
a transcript is cumulative, so a late successful read covers earlier gaps, but if the last read
failed then the tail of the window was never seen and a missing outcome is `unobservable`
rather than negative. An outcome already observed keeps its own evidence either way.

A `busy` trial is the exception to that 120s ceiling. `wake_probe.py` refuses to rule on
`turn_start` or `ack` for a busy host until the turn already running has ended or 900s have
passed, so observing only to 120s would leave those two cells unclassifiable by construction;
a busy trial therefore polls to the 900s cap. When the transcript reports its own `turn_end`
the deadline collapses back to the last window measured from that boundary, which is what
lets the cell classify without waiting the cap out. If the window closes with no `turn_end`
seen, the outcomes that depend on it are marked `observable=False` rather than recorded as
negative evidence — but only those not positively observed, since the classifier does accept
an event that actually arrived during the preceding turn.

It returns a named `TrialRun` (`submitted_at`, `accepted_at`, `outcomes`, `state`,
`supported`, `observable`, `turn_end`, `turn_end_observable`) carrying exactly what
`Trial`/`classify_trial` need; acceptance is
stamped when `submit` *returns*, since a submission that blocks for seconds would otherwise be
backdated into its 10s window. The requested `state` is validated and carried into the result,
but establishing a busy/approval/disconnected/restarted precondition is the caller's `settle`
callable — passing `state='busy'` with a no-op `settle` still exercises an idle host, and no
code here can detect that for the caller.

One documented deviation from `Trial`'s "same monotonic clock" contract: `wake_probe.py`'s own
PTY capture stays in one process and can use `time.monotonic()`, but a real host's transcript
carries only wall-clock/ISO-8601 timestamps from another process, and no monotonic-to-wall
calibration exists to convert them. Every *compared* value therefore comes from `time.time()`;
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

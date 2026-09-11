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
control sequences. The CLI creates a fresh HOME and working directory, passes only HOME/PATH/TERM,
and never writes input. Complete JSON is flushed to a temporary file beside the destination and
published with an atomic no-replace hard link (then the temporary name is removed). This preserves
another capture even if it creates the destination during recording. Failed serialization leaves
no empty final file. A caught interruption preserves partial events and returns a nonzero status;
an uncatchable SIGKILL or power loss before publication cannot preserve an in-memory transcript.
Once capture ends, the CLI defers SIGINT through child cleanup and evidence publication, then
returns 130 if Ctrl-C was requested. Repeated Ctrl-C during JSON serialization, flush/sync or
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
CLI zero means the record was published successfully (including an intentionally stopped capture),
not that a trial completed or a host delivered anything. Failed/interrupted recordings cannot
supply negative wake findings. Even a
`complete` passive recording is not host-readiness evidence, and early EOF does not complete an
outcome observation window. Neither EOF nor a captured marker is
promoted to host acceptance. The command runs under a fresh HOME with no copied host credentials.
The Python `PtyProcess` API allows explicit writes only with a matching observer-supplied generation
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

## Real-host evidence still required

Probe Claude Channels and plain MCP separately, Codex queue and reachable IPC separately, and
OpenCode as the intended third host. Pin installed versions and check provider availability before
running disposable direct-host sessions. Upstream reports are hypotheses to reproduce, not results.
Missing prerequisites must be recorded and resolved or explicitly scoped out by the owner.
No herdr installation is assumed. Publish sanitized manifests and observations here or in durable
issue-linked artifacts; never copy real credentials, administrator sessions or ordinary work
messages into fixtures. The recorder does not provision host authentication.

Direct-host investigation does not connect live sessions through Parley. That connection still
requires the complete fixture gate in [AGENTS.md](../AGENTS.md#record-evidence-and-decisions),
production account evidence where applicable, and the separately authorized human smoke test.

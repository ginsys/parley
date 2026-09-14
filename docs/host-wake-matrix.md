# Host wake investigation

This records [investigation #18](https://github.com/ginsys/parley/issues/18) across Claude, Codex
and OpenCode, with recommendations for the subsequent wake-strategy decision.
It measures direct host invocations in disposable sessions under real HOME. No session was
connected through Parley and no production adapter or wake-strategy decision is implemented.

## Protocol and evidence

Each available mechanism/state row uses three completed trials. Acceptance has a 10-second
window, transcript visibility 30 seconds, a new turn 60 seconds and acknowledgement 120 seconds.
For busy trials, the last two windows start when the preexisting turn completes; independent
earlier positives still count. Completion has a 900-second cap. Undetectable completion makes
dependent outcomes unobservable; a detectable turn that does not finish makes them inconclusive.
Mixed results remain inconclusive. A missing observation never proves nondelivery.

The artifacts preserve individual classifications, recorded host versions, submission and
first-observation UTC/monotonic pairs, completion evidence, final observations and cleanup.
Host transcript timestamps are wall-clock observations, not exact local first-observation times.
The runner checks wall/monotonic divergence and rejects interrupted or invalid-clock attempts.
Polling is once per second plus read/command overhead. Every actual wait uses elapsed time;
classification does not substitute a future timestamp to close an unfinished window.

Positive acceptance means the host submission command exited successfully. A PTY write or MCP
server write is not that signal. Visibility requires the fresh marker in a user message;
acknowledgement requires it in an assistant message. This is a synthetic echo acknowledgement,
not validated Parley reply ingestion. Codex exposes native turn boundaries. Claude's new-turn
proxy is assistant activity after the relevant boundary; tool arguments and tool references
are never acknowledgement text. The still-blocked approval turn cannot count as a new one.

Synthetic creation/resume input: `Reply with exactly PONG. Do not call tools or change files.`
Each trial generates its own `PARLEY-PROBE-<32 hex digits>` marker and asks for that exact marker
back, through the shared [marker helper](../scripts/probe/host_trials.py). Busy input requests
the integers one through two hundred in English (five hundred for OpenCode), followed by
`HOLD COMPLETE`, with no tools or file changes. Approval input requests only an isolated print
command (Codex/OpenCode) or no-side-effect
hold tool (Claude); no approval is answered. See the [captured state shapes](host-probe-preflight.md#codex-state-captures-2026-09-14).

Evidence is intentionally sanitized: private paths and native identifiers are substituted;
injected instructions, credentials, thinking/signatures and unrelated host metadata are omitted.
JSON artifacts state exactly which repeated records or message portions they omit. Original
classifications and unsuccessful attempts are not silently replaced by later successful runs.
The [supplemental attempt ledger](evidence/host-wake/attempts-20260914.json) records those
preflight failures, parser-limited runs, manual cleanup dispositions and successful compatibility
captures separately from the matrix's three-trial rows.

## Codex 0.154.0

Both the CLI and each trial's rollout report `0.154.0`. The client configuration displayed
`gpt-6-astra` with high effort; these rollout records do not identify the model that served the
marker, so the trial's serving-model field remains unknown. The base TUI used read-only sandboxing
and approval policy `never`. Approval trials alone used the captured child-only
`approvals_reviewer="user"` / `on-request` configuration with the local review hook.

| State and mechanism | Accepted | Visible | New turn | Acknowledged | Trials / evidence |
| --- | --- | --- | --- | --- | --- |
| Idle, queue to an already serving TUI | observed | observed | observed | observed | [3/3](evidence/host-wake/codex-idle-20260914.json) |
| Busy text generation, queue to the same TUI | observed | observed | observed | observed | [3/3](evidence/host-wake/codex-busy-20260914.json) |
| Approval blocked, external queue | observed | not observed | not observed | not observed | [3/3](evidence/host-wake/codex-approval-20260914.json) |
| Disconnected, queue with no serving process | observed | not observed | not observed | not observed | [3/3](evidence/host-wake/codex-disconnected-20260914.json) |
| Restarted, queue then resume the same thread | observed | observed | observed | observed | [3/3](evidence/host-wake/codex-restarted-20260914.json) |

“Not observed” means within the applicable window above. Queue exit 0 is a useful acceptance
signal, but the approval/disconnected rows show why it cannot stand for wake or acknowledgement.
The busy row proves submission occurred inside the exact preexisting native turn's interval.
Restarted delivery required the explicitly opened resume process; it does not prove an absent
process wakes itself. Forced deletion was separately captured with a pending queue item: the
owned thread's item count changed from one to zero and its rollout disappeared.

Both advertised local sockets were absent before creation and while an owned TUI was ready:
`$CODEX_HOME/ipc/ipc.sock` and `$CODEX_HOME/app-server-control/app-server-control.sock`.
The read-only daemon version command also returned ENOENT. The conditional IPC path was therefore
not reachable in this configuration; no IPC session-state trials were run and no IPC capability
is classified as unsupported. No shared daemon was started and no human session was enumerated.

## Claude Code 2.1.270

Both the CLI and the session transcripts report `2.1.270`. Creation requested `--model haiku`,
while actual assistant records reported `claude-sonnet-5`. Negative marker trials have no serving
model attributable to the marker, so their model remains unknown. The restarted row names the
model of the explicit restart prompt's assistant activity, not a response to the lost notification.
Real-HOME settings/hooks remained active; the captured background sessions were in plan mode.

Plain MCP means this fixture's stdio `notifications/message` at log level `info`, logger
`parley-probe`, with the synthetic marker in `params.data`. The client offered protocol
`2025-11-25`; the fixture replied with `2025-06-18` and the client sent `notifications/initialized`.
That handshake alone is not a Parley readiness acknowledgement or evidence of notification wake.

| State and mechanism | Accepted | Visible | New turn | Acknowledged | Trials / evidence |
| --- | --- | --- | --- | --- | --- |
| Idle, connected plain-MCP logging | unobservable | not observed | not observed | not observed | [3/3](evidence/host-wake/claude-mcp-idle-20260914.json) |
| Busy text generation, connected plain-MCP logging | unobservable | not observed | not observed | not observed | [3/3](evidence/host-wake/claude-mcp-busy-20260914.json) |
| Approval blocked, connected plain-MCP logging | unobservable | not observed | not observed | not observed | [3/3](evidence/host-wake/claude-mcp-approval-20260914.json) |
| MCP transport disconnected, host remains idle | unobservable | not observed | not observed | not observed | [3/3](evidence/host-wake/claude-mcp-disconnected-20260914.json) |
| Host stopped, message attempted, same session resumed | unobservable | not observed | observed by explicit restart | not observed | [3/3](evidence/host-wake/claude-mcp-restarted-20260914.json) |

There is no host acceptance response for a logging notification. Connected trials prove the
fixture wrote the notification after initialization; they do not prove Claude accepted it.
The disconnected/restarted attempts were written to the private fixture control file while
the transport was absent. The fixture deliberately has no persistence/replay contract: it
ignores earlier controls on startup. No notification was written to the host in those rows.
These are measured loss/restart behaviors of this exact fixture, not a finding that every MCP
server loses messages or that persistent delivery could not work.

The restarted row explicitly invokes the no-flags background resume with the PONG prompt.
Its new assistant activity is caused by that restart input; the marker never became visible or
acknowledged. Restarting was not evidence that the earlier notification woke the session.

The ordinary local terminal launcher was unsuitable for `attach`: it prepended interactive
options and started a different conversation with the prompt `attach`. The investigation's
native attach invocation retains the same memory limits and verifies the created background
PID/session display before injection. Its command/configuration is part of every result.
The standalone driver's default PATH invocation is not a claim of compatibility with arbitrary
launchers. See the [launcher capture](host-probe-preflight.md#local-launcher-invalidates-the-apparent-attach).

### Channels availability

The [current version/help and official plugin-cache check](evidence/host-wake/surface-20260914.json)
found no `--channels` option or installed Channels plugin at `2.1.270`. Under the owner's accepted
scope decision, Channels is unsupported for all five states/four outcomes on this installation.
No runtime trial is invented for an absent mechanism; the three-trial rule applies to available
paths. This does not generalize to another Claude installation or to MCP logging itself.

## OpenCode 1.18.30

The CLI and every exported session report `1.18.30`. Creation and marker generation use
`opencode/ling-3.0-flash-fin-free`; assistant records confirm it when a marker turn exists.
Negative marker rows retain an unknown serving model. Each trial supplies a generated Basic
authentication password only to its own server/client children, under real HOME with `--pure`.
Approval trials additionally set child-only `OPENCODE_CONFIG_CONTENT` to
`{"permission":{"*":"ask"}}`. No permission menu is answered.

| State and mechanism | Accepted | Visible | New turn | Acknowledged | Trials / evidence |
| --- | --- | --- | --- | --- | --- |
| Idle, external run attached to the owned server/session | inconclusive | observed | observed | observed | [3 trials](evidence/host-wake/opencode-idle-20260914.json) |
| Busy text generation, same external attach | not observed | observed | observed | observed | [3/3](evidence/host-wake/opencode-busy-20260914.json) |
| Approval blocked, same external attach | unobservable | observed | not observed | not observed | [3/3](evidence/host-wake/opencode-approval-20260914.json) |
| Disconnected, attach attempted after the owned server stops | not observed | not observed | not observed | not observed | [3/3](evidence/host-wake/opencode-disconnected-20260914.json) |
| Restarted, same failed attach then server restarted on the same port | not observed | not observed | not observed | not observed | [3/3](evidence/host-wake/opencode-restarted-20260914.json) |

The idle trials' command exits were late once and within ten seconds twice, so acceptance
aggregates to inconclusive. Every busy command exited zero after ten seconds. These late
successful exits remain in the artifacts: “not observed” in that acceptance window is not an
observed rejection. The command can wait for generation after the marker is already visible.
The busy precondition binds the exact original request/assistant and verifies submission inside
its start/completion interval; only a subsequent assistant supplies the new-turn observation.
OpenCode's new-turn signal is assistant creation in the export, not a native turn-boundary event.

Approval submissions timed out at 60 seconds without an acceptance response. Independently,
the export showed the marker user message within 30 seconds; the original permission menu and
exact pending bash call remained blocked throughout 120 seconds. Its incomplete assistant
record is excluded from new-turn/ACK detection only while that bound precondition still holds.
The timeout supplies no negative acceptance evidence; the host's exit signal is unobservable
for those attempts. The request was made through external attach, never by typing into the menu.

Disconnected and restarted submissions returned exit 1 with `Session not found` and empty
stdout. Each trial still observed the independent 30/60/120-second windows. Restarted trials
started the owned server on its original port only after that rejection; they did not resubmit
the marker. No marker or new turn subsequently appeared. These rows measure an absent transport
and restart without replay in this exact command sequence, not a product-wide durability limit.

The [OpenCode compatibility ledger](evidence/host-wake/opencode-attempts-20260914.json) retains
eight preflights separately, including provider retries, the approval parser limitation and
authentication status checks. These captures establish command/state shapes, not additional
matrix trials. Idle/busy matrix runs used orchestration commit `879a0bc`; the remaining states
used `393c0f9`, which added liveness and intentional-rejection guards without changing those
positive paths or observation windows. Approval/disconnected/restarted windows overlapped in
separate owned sessions; the idle/busy runs were sequential. This is not an isolated load benchmark.

## Reproduction and cleanup

Three additional Codex approval trials repeated the acceptance-only result after the reproducer
was corrected to inspect the current terminal screen. The [repeat evidence](evidence/host-wake/codex-approval-current-screen-20260914.json)
retains rendered approval screens, raw-terminal hashes, rollout inventories and the original
120-second observations/classifications. The earlier approval captures remain available; their
cumulative-screen guard could not independently exclude a cleared historical menu. The repeated
trials supply that missing precondition check without changing the four reported outcomes.

Install the pinned test dependencies and run the controlled gate first:

```sh
mise run python
```

Then invoke one state at a time, using a new private evidence directory for each invocation:

```sh
.venv/bin/python scripts/probe/codex_matrix.py --state idle --output-directory <new-evidence-directory>
.venv/bin/python scripts/probe/claude_mcp_matrix.py --state idle --output-directory <new-evidence-directory>
.venv/bin/python scripts/probe/opencode_matrix.py --state idle --output-directory <new-evidence-directory>
```

All three commands accept `idle`, `busy`, `approval`, `disconnected`, and `restarted`; each invocation
defaults to three trials. They require the exact recorded host version, real-HOME authentication and
the captured configuration. Fresh tiny probe directories are created privately under the sticky
temporary-directory root; evidence and build caches belong on disk. Codex probe directories
contain only neutral fresh Git initialization. Claude's configuration points at the repository's
[MCP fixture](../scripts/probe/mcp_notification_fixture.py), whose control file and per-process
journals remain in the private evidence directory. Approval rendering uses pinned `pyte` and
`wcwidth`; ordinary tests drive only synthetic terminal bytes/children.

A failed setup stops an invocation and is retained. Claude also accepts explicit `--trials 1`
for a separately recorded additional attempt; it emits no three-trial aggregate. The final
approval row uses two valid trials from one invocation and one additional trial after its third
setup timed out. The new attempt allowed 180 seconds to establish approval instead of 45;
none of the observation windows changed. The failed setup is still in the attempt ledger.
To combine exactly three explicitly chosen completed trials, use:

```sh
.venv/bin/python scripts/probe/aggregate_matrix.py --output <new-aggregate.json> <trial-directory-1> <trial-directory-2> <trial-directory-3>
```

This checks original-clock classifications, completion/cleanup, common state/version and distinct
sessions/markers. It neither selects nor replaces trials automatically. The approval artifact
names the three selected capture/trial pairs.

Each driver creates its own sessions, retains the exact process/session binding, captures evidence
before deletion, closes clients and removes its owned sessions through the host CLI. Any failed
cleanup stops the run. The tiny Git/hook directories and sanitized-source captures are retained
until the investigation's evidence lands, then cleaned explicitly. Real-HOME side effects include
Claude transcript/hook files and Codex trust entries for these fresh directories; global Codex
configuration is not edited to erase them. No existing working session is adopted or removed.

## PTY fallback rejection and failure cases

The controlled harness fixtures ran in `mise run python`; they launch synthetic Python children,
not installed hosts. Their state labels are test inputs, not a real-host classifier.

| Case | Fixture behavior | Host acceptance | Visibility / new turn / acknowledgement |
| --- | --- | --- | --- |
| Pane replaced by shell, pager or editor | Refuses before any write | No host attempt | Unobservable from PTY alone |
| Approval prompt | Refuses before any write | No host attempt | Unobservable; injected text could answer permission |
| Unknown/busy pane state | Refuses before any write | No host attempt | Unobservable until host-specific state evidence exists |
| Old session generation after restart/pane reuse | Rejects the stale generation before writing | No host attempt | Unobservable for the stale target |
| Bytes written to an allowed synthetic idle pane, no ACK | Records bytes received only | Unobservable | All unobservable without separate host signals |

The relevant fixtures are `test_replaced_pane_and_approval_prompt_reject_before_writing`,
`test_restart_rejects_old_generation` and `test_written_bytes_are_not_an_acknowledgement` in
[the PTY tests](../scripts/probe/test_wake_probe.py). An idle composer or successful write alone
cannot bind a real pane to a native session. The launcher failure above reproduced that limitation.
The investigation's host-specific probes intentionally establish busy/approval states outside
this generic fallback; approval marker injection uses external queue, MCP or authenticated attach.

## Recommendations and decision enabled

Keep handoff, visibility, new-turn activity and acknowledgement separate. In the tested Codex
configuration, queue acceptance can coexist with no observed wake. In OpenCode's busy trials,
the fresh marker was visible before the successful command exit supplied the acceptance signal.
For Claude's logging fixture there is no host acceptance response at all. A single boolean
cannot describe these outcomes, and their observation order is not a universal delivery sequence.

The current [`Transport.Deliver`](../internal/dispatch/dispatch.go) reports acceptance by its
error result; successful settlement records `handed_off`. Authenticated
[`connection.Ingestor`](../internal/connection/ingestion_linux.go) separately validates reply
provenance and records `acked` transactionally with the reply. Preserve those guarantees.
The synthetic marker echoes here establish an observable host reply, not that Parley's ingestion
or readiness protocol worked against a live host.

Recommend that the follow-up host contract expose optional, independently timestamped
visibility and turn-start observations alongside acceptance, with native session/generation,
attempt and evidence provenance. These asynchronous observations need not be added as blocking
requirements to `Deliver`: waiting for a turn to finish would conflate handoff with generation,
and a host with no observable turn boundary must still be representable. Missing or late
observations must not trigger retries of an uncertain handoff or silently promote it to `acked`.
This investigation changes no runtime interface or envelope state.

Declare wake support separately from turn-start observability, scoped to host version,
mechanism, configuration and required session state. Distinguish unavailable, untested and
observed capabilities; these three-trial results are evidence, not reliability guarantees.
An absent IPC socket is an unreachable path in this installation, while the accepted Channels
scope decision is unsupported on the tested installation. Neither missing turn evidence nor
plain-MCP logging's negative result establishes that an entire host product cannot wake.

Deterministic external polling is viable for **observing persisted host evidence**: the runners
read bound transcripts/exports at a fixed cadence and recover marker observations independently
of command exit. A production observer would additionally need authenticated binding, durable
cursors, deduplication and recovery. Polling alone is not a demonstrated wake fallback: none of
these trials makes an idle agent poll Parley's inbox, makes a stopped process start itself, or
makes a logging notification become a user turn. Codex's successful restarted row required an
explicit resume action. A scheduler that launches/resumes a host would itself be an authorized
host action, with stale-session and approval-state checks, rather than proof of autonomous wake.

Prefer host-specific delivery and observation contracts where the measured signals exist.
A shared PTY fallback trades signal precision for reach and still requires host-specific
identity/state evidence; the rejection table above rules out treating a generic pane write as
safe delivery. The decision may choose bespoke adapters or combine them with a guarded fallback,
but every mode must declare its guarantees and refuse unsafe or unbound targets.
[Decision #23](https://github.com/ginsys/parley/issues/23) selects the strategy;
[specification #24](https://github.com/ginsys/parley/issues/24) defines those contracts.
These recommendations do not implement either follow-up or lift the live-connection fixture gate.

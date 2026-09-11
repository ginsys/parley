"""Matrix runner driving real hosts through wake_probe's Trial/aggregate classification.

Owned by ginsys/parley#18. This module supplies the pieces #18's harness (wake_probe.py) does
not: something that creates a session, submits one synthetic marker message, observes the four
outcomes (accepted/visible/turn_start/ack) and classifies each `Trial` — never a production host
adapter or session authenticator. Process creation is injectable everywhere a host CLI would run
(AGENTS.md's Test isolation section): fixtures supply a fake `run`, so no test launches an
installed Claude, Codex or OpenCode binary.

Two host-specific schemas are grounded in real, captured output rather than assumed:
- `claude agents --json [--all]` uses different field names by `kind`: a `background` session
  (one this module creates with `claude --bg`) reports `state` (e.g. "done"); an `interactive`
  session reports `status` instead. Only `kind == 'background'` entries are ever touched here.
- A completed background session's `claude logs <id>` fails once its daemon has exited — observed
  as `connect ENOENT /tmp/cc-daemon-*/*/control.sock` against a `state: done` session. Observation
  must happen before teardown, not after; `ClaudeDriver.observe()` documents this ordering
  requirement and does not retry past it. A failed read is an unavailable channel, reported as an
  unobservable `Observation`, never as a negative outcome.
- `claude logs` output carries no per-entry timestamp, and no captured mechanism at 2.1.268
  delivers a message to an *existing* background session (`--bg` takes its prompt at creation;
  `attach` is an interactive PTY, `--remote-control` and `--input-format=stream-json` are
  unexercised — docs/host-probe-preflight.md, 2026-09-11). `ClaudeDriver.submit()` therefore
  raises `SubmissionUnsupported` rather than report acceptance for a marker the host never
  received; the cell classifies `unsupported` until stage 3 captures a real submission path.
- Codex's rollout JSONL (`$CODEX_HOME/sessions/YYYY/MM/DD/rollout-*.jsonl`) is one JSON object per
  line; a chat turn is `{"type": "response_item", "payload": {"type": "message", "role": ...,
  "content": [{"type": "input_text"|"output_text", "text": ...}]}}` with a record-level ISO-8601
  `timestamp`. `role` is `developer`, `user` or `assistant`; only the latter two are transcript
  turns.

OpenCode's `export <sessionID>` shape has no captured sample yet (no local session existed to
export from at investigation time) and is deliberately left unimplemented rather than guessed;
`OpenCodeDriver` raises `NotImplementedError` until stage 3 supplies real evidence.
"""

import datetime
import json
import re
import subprocess
import time
import uuid
from dataclasses import dataclass, field

from wake_probe import WINDOWS

OUTCOME_NAMES = frozenset(WINDOWS)  # {'accepted', 'visible', 'turn_start', 'ack'}
TRANSCRIPT_OUTCOMES = ('visible', 'turn_start', 'ack')  # 'accepted' comes from submit, not a log
LAST_WINDOW = max(WINDOWS.values())  # 120s: the longest outcome window an observation must cover
# Mirrors the state values `wake_probe.Trial.result` accepts; validated at submission time so a
# typo fails before a host is driven rather than at classification.
TRIAL_STATES = frozenset({'idle', 'busy', 'approval', 'disconnected', 'restarted'})


class ForeignSessionError(ValueError):
    """Raised when an operation targets a session this run did not create."""


class SubmissionUnsupported(NotImplementedError):
    """Raised when a host has no captured mechanism for submitting to an existing session."""


@dataclass
class SessionRegistry:
    """Tracks session ids created by *this run*; refuses to touch anything else.

    `claude agents --json --all` lists every background session on the workstation, including
    ordinary human work. No driver may submit to, observe or tear down an id this registry did
    not itself mint via `mint()` — enforced here, not left to each driver to remember.
    """

    created: set = field(default_factory=set)

    def mint(self, session_id):
        if not session_id:
            raise ValueError('empty session id')
        if session_id in self.created:
            raise ValueError(f'session already registered: {session_id}')
        self.created.add(session_id)
        return session_id

    def require_owned(self, session_id):
        if session_id not in self.created:
            raise ForeignSessionError(f'refusing to operate on foreign session: {session_id}')

    def release(self, session_id):
        self.require_owned(session_id)
        self.created.discard(session_id)


def marker_token():
    """A fresh high-entropy marker per trial so an ack cannot be chance or terminal echo."""
    return f'PARLEY-PROBE-{uuid.uuid4().hex}'


@dataclass
class Event:
    """One transcript entry, normalized across hosts.

    `time` is a host-reported or record-derived epoch-seconds float, or None when the source
    carries no per-event timestamp. An undated event is never placed in a trial's window — see
    `detect_outcomes` for why that has to fail closed.
    """

    role: str  # 'user' or 'assistant'; 'developer'/system entries are filtered before this point
    text: str
    time: float | None = None


@dataclass
class Observation:
    """One read of a host's transcript: what was seen, and whether the channel could be read.

    `observable` False means this read establishes nothing either way — the log was unreachable,
    or it carried entries that cannot be placed relative to submission. Classification must map
    that to `unobservable`, never to `not_observed`: a dead or undatable channel is not negative
    evidence (docs/host-probes.md, Trial protocol).
    """

    outcomes: dict = field(default_factory=dict)
    observable: bool = True


def detect_outcomes(events, marker, *, submitted_at):
    """Classify normalized `events` into an `Observation` over visible/turn_start/ack.

    `submitted_at` is an epoch-seconds float on the same clock as each `Event.time`, marking
    when the marker message was sent; events strictly before it are ignored (host history from
    before this trial). Acceptance is not a transcript signal — the caller supplies it directly
    from the submit command's own exit status. The first assistant event of any content is
    `turn_start`; only one whose text contains the marker also counts as `ack`, so an unrelated
    assistant reply cannot be mistaken for acknowledging this trial's message.

    An event with `time=None` cannot be ordered against `submitted_at`, so it is skipped and the
    whole read is reported unobservable. Promoting undated entries into the current window
    manufactures outcomes out of pre-submission history: an old untimestamped rollout record, or
    the assistant turn a session-creation prompt produced before this trial's marker existed.
    """
    outcomes = {}
    undated = False
    for event in events:
        if event.time is None:
            undated = True
            continue
        if event.time < submitted_at:
            continue
        if event.role == 'user':
            if marker in event.text:
                outcomes.setdefault('visible', event.time)
            continue
        if event.role != 'assistant':
            continue
        outcomes.setdefault('turn_start', event.time)
        if marker in event.text:
            outcomes.setdefault('ack', event.time)
    return Observation(outcomes=outcomes, observable=not undated)


# --- Claude: `claude agents --json [--all] [--cwd ...]` and `claude logs <id>` -----------------

def background_sessions(raw):
    """Filter `claude agents --json` output to background sessions only.

    Interactive sessions report `status`, not `state`, and are never this module's concern:
    only sessions this runner itself starts with `claude --bg` are eligible for any operation.
    """
    return [entry for entry in json.loads(raw) if entry.get('kind') == 'background']


def parse_claude_transcript(raw, *, marker):
    """Parse `claude logs <id>` plain-text output into normalized Events.

    Only two roles are attributed: a line opening with `User:` or `Assistant:` starts a new
    event and following non-empty lines extend it, matching the multi-line message shape a
    transcript line-printer produces. `claude logs` carries no per-line timestamp, so every
    returned Event has `time=None`; `detect_outcomes` consequently reports an unobservable read
    rather than counting entries that cannot be ordered against submission. This format has not
    yet been captured against a running background session (observe() could not reach a `done`
    one's daemon) and must be reconciled with real output before being relied on for evidence.
    """
    events = []
    role = None
    for line in raw.splitlines():
        stripped = line.strip()
        match = re.match(r'^(User|Assistant):\s?(.*)$', stripped)
        if match:
            role = 'user' if match.group(1) == 'User' else 'assistant'
            events.append(Event(role=role, text=match.group(2)))
        elif role is not None and stripped:
            events[-1].text += '\n' + stripped
    if marker and not any(marker in event.text for event in events):
        pass  # absence is a legitimate not_observed outcome, not a parse error
    return events


class ClaudeDriver:
    """Drives Claude Code background sessions. `run` defaults to subprocess.run; tests inject a
    fake to avoid launching an installed `claude` binary (AGENTS.md, Test isolation)."""

    def __init__(self, registry, *, run=subprocess.run, cwd, model=None, max_budget_usd=None):
        self.registry = registry
        self.run = run
        self.cwd = cwd
        self.model = model
        self.max_budget_usd = max_budget_usd

    def create(self, prompt):
        argv = ['claude', '--bg', '--cwd', self.cwd, '--print']
        if self.model:
            argv += ['--model', self.model]
        if self.max_budget_usd is not None:
            argv += ['--max-budget-usd', str(self.max_budget_usd)]
        argv.append(prompt)
        result = self.run(argv, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            raise RuntimeError(f'claude --bg exited {result.returncode}: {result.stderr}')
        printed = result.stdout.strip()
        if not printed:
            # Exit 0 with no id still means a background session may exist, now untracked;
            # indexing an empty list here would surface that as a bare IndexError.
            raise RuntimeError('claude --bg exited 0 without printing a session id; '
                               'an untracked background session may be running')
        return self.registry.mint(printed.splitlines()[-1])

    def submit(self, session_id, message):
        """Refuse to report acceptance: no captured mechanism delivers `message` here.

        `claude --bg` takes its prompt at creation, and at 2.1.268 nothing captured submits a
        further message to an *existing* background session — `attach` is an interactive PTY,
        `--remote-control` and `--print --input-format=stream-json` are unexercised
        (docs/host-probe-preflight.md, 2026-09-11). This previously returned True after merely
        listing the session, which recorded an `accepted` outcome for a marker the host never
        received. Guessing an unconfirmed submission flag would violate AGENTS.md's evidence
        rule (the same reason `OpenCodeDriver` refuses), so this raises and `run_trial`
        classifies the cell `unsupported`. The listing check runs first, so an absent or
        foreign session still fails as such rather than as an unsupported mechanism.
        """
        self.registry.require_owned(session_id)
        result = self.run(['claude', 'agents', '--json', '--cwd', self.cwd], capture_output=True, text=True, timeout=15)
        owned = {entry['id'] for entry in background_sessions(result.stdout)}
        if session_id not in owned:
            raise ValueError(f'session not listed under {self.cwd}: {session_id}')
        raise SubmissionUnsupported(
            'claude has no captured message-submission path to an existing --bg session; '
            'creation carries the only delivered prompt')

    def observe(self, session_id, *, marker, submitted_at):
        """Read logs before teardown: a `done` session's daemon socket is already gone.

        A nonzero read is an unavailable channel, returned unobservable so classification
        cannot turn a dead daemon socket into `not_observed`.
        """
        self.registry.require_owned(session_id)
        result = self.run(['claude', 'logs', session_id], capture_output=True, text=True, timeout=15)
        if result.returncode != 0:
            return Observation(observable=False)
        return detect_outcomes(parse_claude_transcript(result.stdout, marker=marker), marker, submitted_at=submitted_at)

    def teardown(self, session_id):
        """Release ownership only after a confirmed removal.

        A failed `claude rm` leaves the background session alive; forgetting it here would make
        every later teardown attempt fail `require_owned`, so the session could never be
        reclaimed and would keep consuming the developer's real host environment.
        """
        self.registry.require_owned(session_id)
        result = self.run(['claude', 'rm', session_id], capture_output=True, text=True, timeout=15)
        if result.returncode != 0:
            raise RuntimeError(f'claude rm exited {result.returncode} for {session_id}: {result.stderr}')
        self.registry.release(session_id)


# --- Codex: `codex queue --thread <id> --message <text>` and the rollout JSONL -----------------

def record_time(record):
    """Epoch seconds from a rollout record's ISO-8601 `timestamp`, or None if unusable."""
    stamp = record.get('timestamp')
    if not isinstance(stamp, str):
        return None
    try:
        return datetime.datetime.fromisoformat(stamp.replace('Z', '+00:00')).timestamp()
    except ValueError:
        return None


def rollout_started_at(lines):
    """Earliest usable record timestamp in a rollout, or None when it carries none.

    Provenance, not transcript: every record type counts here (`session_meta` included), because
    the question is when the thread itself came into existence, not when it was spoken in.

    An unparseable line makes the whole answer None rather than being skipped: skipping it means
    the earliest record might be the one that failed to parse, so a thread that predates the run
    could pass the provenance check on the minimum of whatever happened to survive.
    """
    stamps = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except ValueError:
            return None
        when = record_time(record)
        if when is not None:
            stamps.append(when)
    return min(stamps) if stamps else None


def codex_rollout_events(lines):
    """Extract message Events from rollout JSONL lines; returns `(events, unusable)`.

    Skips `developer`-role entries (fixed instructions, not conversation turns) and any record
    missing the expected shape; a rollout file accumulates non-message record types this runner
    has no use for (event_msg, token_usage_record, world_state, turn_context, ...). Those are
    skipped silently: a record this runner has no use for is not evidence it failed to read.

    `unusable` counts content that *should* have been readable and was not — a line that is not
    valid JSON (a corrupt or half-written rollout), or a message record whose `timestamp` is
    missing or malformed. Neither can be ordered against submission, and opening a file
    successfully does not establish that its transcript was read successfully. The caller reports
    such a read unobservable rather than letting absent outcomes become negative evidence.
    """
    events = []
    unusable = 0
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except ValueError:
            unusable += 1
            continue
        payload = record.get('payload')
        if not isinstance(payload, dict) or payload.get('type') != 'message':
            continue
        role = payload.get('role')
        if role not in ('user', 'assistant'):
            continue
        when = record_time(record)
        if when is None:
            unusable += 1
            continue
        text = ''.join(part.get('text', '') for part in payload.get('content', []) if isinstance(part, dict))
        events.append(Event(role=role, text=text, time=when))
    return events, unusable


class CodexDriver:
    """Drives Codex CLI sessions via `codex queue`. Requires an existing thread id — creating one
    needs an interactive/exec session first; `create()` documents this as the caller's job."""

    def __init__(self, registry, *, run=subprocess.run, rollout_path_for, started_at):
        self.registry = registry
        self.run = run
        # Injectable lookup from thread id to its rollout file path, so tests never touch
        # $CODEX_HOME/sessions themselves.
        self.rollout_path_for = rollout_path_for
        # Epoch seconds this run began: the provenance boundary register_existing enforces.
        self.started_at = started_at

    def register_existing(self, thread_id):
        """Adopt a thread created during *this run*; refuse anything that predates it.

        Minting whatever id a caller passed defeated the registry's stated guarantee, since
        `submit` then queues a message to it: a mistyped or copy-pasted id could reach an
        ordinary human thread. Provenance comes from the rollout file's own record timestamps
        (confirmed shape, docs/host-probe-preflight.md 2026-09-11) — the earliest record must
        be at or after `started_at`, so a pre-existing thread cannot be adopted by accident.

        This orders a thread against the run; it does not authenticate it, and creating the
        thread remains the caller's job (class docstring). A thread whose rollout is missing or
        carries no usable timestamp is refused rather than adopted on trust.
        """
        path = self.rollout_path_for(thread_id)
        if path is None:
            raise ForeignSessionError(f'no rollout file to prove provenance for thread: {thread_id}')
        try:
            with open(path, encoding='utf-8') as handle:
                started = rollout_started_at(handle)
        except OSError as error:  # an unreadable rollout proves nothing; refuse, never adopt
            raise ForeignSessionError(f'cannot read rollout for thread {thread_id}: {error}') from error
        if started is None:
            raise ForeignSessionError(f'rollout carries no usable timestamp for thread: {thread_id}')
        if started < self.started_at:
            raise ForeignSessionError(f'thread predates this run and was not created by it: {thread_id}')
        return self.registry.mint(thread_id)

    def submit(self, thread_id, message):
        self.registry.require_owned(thread_id)
        result = self.run(['codex', 'queue', '--thread', thread_id, '--message', message],
                           capture_output=True, text=True, timeout=15)
        return result.returncode == 0

    def observe(self, thread_id, *, marker, submitted_at):
        """Read the thread's rollout; an unreadable or undatable rollout is unobservable."""
        self.registry.require_owned(thread_id)
        path = self.rollout_path_for(thread_id)
        if path is None:
            return Observation(observable=False)  # no rollout to read is a dead channel
        try:
            with open(path, encoding='utf-8') as handle:
                events, unusable = codex_rollout_events(handle)
        except OSError:
            # Same rule as a failed `claude logs`: an unreadable channel is unobservable, not
            # an absence of outcomes. A rollout is created lazily, so an early poll can precede it.
            return Observation(observable=False)
        observation = detect_outcomes(events, marker, submitted_at=submitted_at)
        if unusable:
            return Observation(outcomes=observation.outcomes, observable=False)
        return observation

    def teardown(self, thread_id):
        self.registry.require_owned(thread_id)
        self.registry.release(thread_id)  # codex has no `queue --stop`; nothing to tear down


class OpenCodeDriver:
    """Placeholder pending stage 3's real `export <sessionID>` sample.

    No local OpenCode session existed at investigation time (`opencode --pure session list`
    printed nothing), so its export JSON shape is unconfirmed. Guessing that shape would violate
    AGENTS.md's evidence rule for investigations; implement this once a real export is captured.
    """

    def __init__(self, *_args, **_kwargs):
        raise NotImplementedError('OpenCode export format not yet captured; see class docstring')


# --- Orchestration -------------------------------------------------------------------------

@dataclass
class TrialRun:
    """One trial's raw result, in the shape `Trial`/`classify_trial` need.

    A named result rather than a tuple: the fields are exactly what a caller must carry into
    `Trial(submitted=..., state=...)` and `classify_trial(..., supported=, observable=)`, and a
    tuple of five positional values invites the wrong unpacking at the one place where a
    mis-assigned timestamp silently corrupts a matrix cell.
    """

    session_id: str
    submitted_at: float
    accepted_at: float | None
    outcomes: dict
    state: str
    supported: dict
    observable: dict


def run_trial(driver, *, prompt, marker, state='idle', settle=lambda: None,
              poll_interval=5.0, clock=time.time, monotonic=time.monotonic, sleep=time.sleep):
    """Create, submit and observe one trial through its windows; returns a `TrialRun`.

    `settle` is what establishes the requested `state` (busy/approval/disconnected/restarted)
    before submission — this function cannot create those conditions itself, and the default
    no-op is only correct for `idle`. `state` is validated here and carried into the result so
    the cell cannot be recorded under a state the trial never exercised; a caller passing
    `state='busy'` with a no-op `settle` is still exercising an idle host, which no code can
    detect for it. Teardown stays the caller's responsibility so a failed trial's session
    remains inspectable.

    Observation polls until every transcript outcome is seen or the longest window (120s) has
    elapsed. A single immediate snapshot reported `not_observed` for events that arrived well
    inside their window, which is the whole failure mode the windows exist to measure. Polling
    deadlines use `monotonic` (injected, `time.monotonic` by default) because a local elapsed
    interval must not move when NTP steps the clock.

    Deliberate, bounded deviation from `Trial`'s "same monotonic clock" docstring: every
    timestamp that is *compared* — `submitted_at`, `accepted_at`, each `Event.time` — comes from
    `clock` (`time.time` by default), because a real host's transcript carries only
    wall-clock/ISO-8601 timestamps from another process and no monotonic-to-wall calibration
    exists to convert them. `Trial.result()` requires one consistent clock across
    `submitted`/`outcomes`/`now`, not monotonicity, so a caller must also pass `time.time()` for
    `now`. A wall-clock step during a trial therefore remains a known distortion of the
    recorded timestamps; only the local polling deadline is immune.

    `accepted_at` is taken *after* `submit` returns, not before: submission can block up to the
    subprocess timeout, and stamping acceptance at `submitted_at` backdated a slow
    acknowledgement into its 10s window. A host with no captured submission mechanism raises
    `SubmissionUnsupported`, which is recorded as `unsupported` for every outcome — nothing was
    delivered, so no transcript signal could belong to this trial.
    """
    if state not in TRIAL_STATES:
        raise ValueError(f'unknown trial state: {state}')
    session_id = driver.create(prompt)
    settle()
    submitted_at = clock()
    deadline = monotonic() + LAST_WINDOW
    try:
        accepted = driver.submit(session_id, marker)
    except SubmissionUnsupported:
        return TrialRun(session_id=session_id, submitted_at=submitted_at, accepted_at=None,
                        outcomes={}, state=state,
                        supported={name: False for name in OUTCOME_NAMES},
                        observable={name: True for name in OUTCOME_NAMES})
    accepted_at = clock() if accepted else None
    outcomes = {}
    channel_readable = False
    while True:
        observation = driver.observe(session_id, marker=marker, submitted_at=submitted_at)
        # The *final* read decides observability, not whether any read ever worked. A transcript
        # is cumulative, so one successful read late in the window covers the earlier gaps; but
        # if the last read failed — a finished Claude session's daemon socket disappearing is
        # exactly this — the tail of the window was never seen, and an outcome missing from a
        # transcript nobody could read at the end is not negative evidence.
        channel_readable = observation.observable
        for name, when in observation.outcomes.items():
            outcomes.setdefault(name, when)
        remaining = deadline - monotonic()
        if remaining <= 0 or all(name in outcomes for name in TRANSCRIPT_OUTCOMES):
            break
        sleep(min(poll_interval, remaining))
    if accepted_at is not None:
        outcomes['accepted'] = accepted_at
    # A positively observed outcome stands on its own evidence; only the ones still missing at
    # the deadline depend on whether the transcript could be read at all.
    observable = {'accepted': True}
    for name in TRANSCRIPT_OUTCOMES:
        observable[name] = True if name in outcomes else channel_readable
    return TrialRun(session_id=session_id, submitted_at=submitted_at, accepted_at=accepted_at,
                    outcomes=outcomes, state=state,
                    supported={name: True for name in OUTCOME_NAMES}, observable=observable)


def classify_trial(trial, now, *, supported=None, observable=None):
    """Classify every outcome for one Trial, using wake_probe's fixed windows unmodified.

    Propagates `Trial.result()`'s ValueError verbatim on a still-open observation window rather
    than defaulting it to any result value — a caller must wait, not guess. `supported`/
    `observable` are optional `{outcome: bool}` overrides, e.g. a pinned-version absence like
    Claude's missing `--channels` records `supported={'accepted': False, ...}` for every outcome
    of that mechanism without spending a live trial on something already known unsupported.
    """
    supported = supported or {}
    observable = observable or {}
    return {outcome: trial.result(outcome, now,
                                   supported=supported.get(outcome, True),
                                   observable=observable.get(outcome, True))
            for outcome in OUTCOME_NAMES}

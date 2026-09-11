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
  requirement and does not retry past it.
- Codex's rollout JSONL (`$CODEX_HOME/sessions/YYYY/MM/DD/rollout-*.jsonl`) is one JSON object per
  line; a chat turn is `{"type": "response_item", "payload": {"type": "message", "role": ...,
  "content": [{"type": "input_text"|"output_text", "text": ...}]}}` with a record-level ISO-8601
  `timestamp`. `role` is `developer`, `user` or `assistant`; only the latter two are transcript
  turns.

OpenCode's `export <sessionID>` shape has no captured sample yet (no local session existed to
export from at investigation time) and is deliberately left unimplemented rather than guessed;
`OpenCodeDriver` raises `NotImplementedError` until stage 3 supplies real evidence.
"""

import json
import re
import subprocess
import time
import uuid
from dataclasses import dataclass, field

from wake_probe import WINDOWS

OUTCOME_NAMES = frozenset(WINDOWS)  # {'accepted', 'visible', 'turn_start', 'ack'}


class ForeignSessionError(ValueError):
    """Raised when an operation targets a session this run did not create."""


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
    carries no per-event timestamp (the caller then treats every event as within-window).
    """

    role: str  # 'user' or 'assistant'; 'developer'/system entries are filtered before this point
    text: str
    time: float | None = None


def detect_outcomes(events, marker, *, submitted_at):
    """Classify normalized `events` into a subset of visible/turn_start/ack timestamps.

    `submitted_at` is an epoch-seconds float on the same clock as each `Event.time`, marking
    when the marker message was sent; events strictly before it are ignored (host history from
    before this trial). Acceptance is not a transcript signal — the caller supplies it directly
    from the submit command's own exit status. The first assistant event of any content is
    `turn_start`; only one whose text contains the marker also counts as `ack`, so an unrelated
    assistant reply cannot be mistaken for acknowledging this trial's message.
    """
    outcomes = {}
    for event in events:
        if event.time is not None and event.time < submitted_at:
            continue
        if event.role == 'user':
            if marker in event.text:
                outcomes.setdefault('visible', event.time if event.time is not None else submitted_at)
            continue
        if event.role != 'assistant':
            continue
        outcomes.setdefault('turn_start', event.time if event.time is not None else submitted_at)
        if marker in event.text:
            outcomes.setdefault('ack', event.time if event.time is not None else submitted_at)
    return outcomes


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
    returned Event has `time=None` and detect_outcomes treats it as within-window; ordering
    (not timing) is what distinguishes visible from turn_start here. This format has not yet
    been captured against a running background session (observe() could not reach a `done`
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
        session_id = result.stdout.strip().splitlines()[-1]
        return self.registry.mint(session_id)

    def submit(self, session_id, message):
        self.registry.require_owned(session_id)
        result = self.run(['claude', 'agents', '--json', '--cwd', self.cwd], capture_output=True, text=True, timeout=15)
        owned = {entry['id'] for entry in background_sessions(result.stdout)}
        if session_id not in owned:
            raise ValueError(f'session not listed under {self.cwd}: {session_id}')
        return True  # a background session's own turn is the create() prompt; see host-probes.md

    def observe(self, session_id, *, marker, submitted_at):
        """Read logs before teardown: a `done` session's daemon socket is already gone."""
        self.registry.require_owned(session_id)
        result = self.run(['claude', 'logs', session_id], capture_output=True, text=True, timeout=15)
        if result.returncode != 0:
            return {}
        return detect_outcomes(parse_claude_transcript(result.stdout, marker=marker), marker, submitted_at=submitted_at)

    def teardown(self, session_id):
        self.registry.require_owned(session_id)
        self.run(['claude', 'rm', session_id], capture_output=True, text=True, timeout=15)
        self.registry.release(session_id)


# --- Codex: `codex queue --thread <id> --message <text>` and the rollout JSONL -----------------

def codex_rollout_events(lines):
    """Extract normalized message Events from rollout JSONL lines (already-split text lines).

    Skips `developer`-role entries (fixed instructions, not conversation turns) and any record
    missing the expected shape; a rollout file accumulates non-message record types this runner
    has no use for (event_msg, token_usage_record, world_state, turn_context, ...).
    """
    events = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except ValueError:
            continue
        payload = record.get('payload')
        if not isinstance(payload, dict) or payload.get('type') != 'message':
            continue
        role = payload.get('role')
        if role not in ('user', 'assistant'):
            continue
        text = ''.join(part.get('text', '') for part in payload.get('content', []) if isinstance(part, dict))
        when = None
        stamp = record.get('timestamp')
        if isinstance(stamp, str):
            try:
                import datetime
                when = datetime.datetime.fromisoformat(stamp.replace('Z', '+00:00')).timestamp()
            except ValueError:
                when = None
        events.append(Event(role=role, text=text, time=when))
    return events


class CodexDriver:
    """Drives Codex CLI sessions via `codex queue`. Requires an existing thread id — creating one
    needs an interactive/exec session first; `create()` documents this as the caller's job."""

    def __init__(self, registry, *, run=subprocess.run, rollout_path_for):
        self.registry = registry
        self.run = run
        # Injectable lookup from thread id to its rollout file path, so tests never touch
        # $CODEX_HOME/sessions themselves.
        self.rollout_path_for = rollout_path_for

    def register_existing(self, thread_id):
        """Adopt an already-started thread (see class docstring) as this run's own."""
        return self.registry.mint(thread_id)

    def submit(self, thread_id, message):
        self.registry.require_owned(thread_id)
        result = self.run(['codex', 'queue', '--thread', thread_id, '--message', message],
                           capture_output=True, text=True, timeout=15)
        return result.returncode == 0

    def observe(self, thread_id, *, marker, submitted_at):
        self.registry.require_owned(thread_id)
        path = self.rollout_path_for(thread_id)
        if path is None:
            return {}
        with open(path, encoding='utf-8') as handle:
            events = codex_rollout_events(handle)
        return detect_outcomes(events, marker, submitted_at=submitted_at)

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

def run_trial(driver, *, prompt, marker, state='idle', settle=lambda: None):
    """Create, submit and observe one trial; returns (session_id, accepted, outcomes, submitted_at).

    `settle` lets a caller wait out a busy/approval/disconnected/restarted precondition before
    `submit`; the default is a no-op for the plain idle case. Teardown is the caller's
    responsibility so a failed trial's session remains inspectable.

    Deliberate deviation from `Trial`'s "same monotonic clock" docstring: `wake_probe.py`'s own
    PTY capture stays in one process and can use `time.monotonic()`, but a real host's transcript
    carries only wall-clock/ISO-8601 timestamps from another process, so this module standardizes
    on `time.time()` throughout (`submitted_at` here, `codex_rollout_events`' parsed `time`).
    `Trial.result()` only requires a single consistent clock across `submitted`/`outcomes`/`now`;
    it does not itself require monotonicity. A caller feeding these values into `Trial` must use
    `time.time()` for `now` too, not `time.monotonic()`.
    """
    session_id = driver.create(prompt)
    settle()
    submitted_at = time.time()
    accepted = driver.submit(session_id, marker)
    outcomes = driver.observe(session_id, marker=marker, submitted_at=submitted_at)
    if accepted:
        outcomes = {**outcomes, 'accepted': submitted_at}
    return session_id, outcomes


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

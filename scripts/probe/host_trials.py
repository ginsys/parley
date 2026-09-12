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
- `claude logs` output is assumed to carry no per-entry timestamp — a hypothesis, since the one
  read attempted here failed against an already-exited daemon — and no captured mechanism at
  2.1.268 delivers a message to an *existing* background session (`--bg` takes its prompt at
  creation; `attach` is an interactive PTY, `--remote-control` and `--input-format=stream-json`
  are unexercised — docs/host-probe-preflight.md, 2026-09-11). `ClaudeDriver.submit()` therefore
  raises `SubmissionUncaptured` rather than report acceptance for a marker the host never
  received; the cell classifies `unobservable` until stage 3 captures a real submission path.
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
import math
import os
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
# A busy trial's dependent windows start at the turn already running, so `wake_probe.py:57-67`
# refuses to classify `turn_start`/`ack` until either that turn ended or 900s passed. Observation
# must last that long or the cell is unclassifiable; the constant is wake_probe's, restated here
# because it is inline there.
BUSY_CAP = 900


class ForeignSessionError(ValueError):
    """Raised when an operation targets a session this run did not create."""


class SubmissionUnsupported(NotImplementedError):
    """Raised when the *host* lacks the submission mechanism — evidence about the host.

    Claude's absent `--channels` is the model case: missing from the help text and the plugin
    cache, so its absence is a property of the product. Classifies the cell `unsupported`.
    """


class SubmissionUncaptured(NotImplementedError):
    """Raised when *this runner* has captured no submission path — evidence about us.

    The host may well support submission by a mechanism nobody here has exercised yet, so the
    trial establishes nothing in either direction and classifies `unobservable`, never
    `unsupported`. Keeping the two apart stops a gap in our tooling being published as a
    host-capability result (docs/host-probes.md, Matrix runner).
    """


class SessionCreationUncaptured(NotImplementedError):
    """Raised when this runner has captured no session-creation path for a host.

    Like `SubmissionUncaptured`, a statement about this runner's evidence, not about the host.
    """


class AmbiguousSessionCreation(RuntimeError):
    """Raised when `create()` cannot verify which single session, if any, it just created.

    No candidate id is minted into the registry here: `require_owned`/`teardown` refusing any
    session this run did not verifiably create is the whole safety guarantee
    (`docs/host-probes.md`), and an ambiguous or unreadable post-create listing is, by
    definition, not verified -- minting one anyway previously let `teardown()` accept and
    `claude rm` an unrelated human session. A live background session may still exist under the
    operator's real HOME after this raises; `candidates` carries whatever ids or diagnostic
    detail were available so a human can investigate and clean it up out of band, deliberately
    outside this runner's own ownership authority.
    """

    def __init__(self, message, *, candidates=()):
        super().__init__(message)
        self.candidates = tuple(candidates)


class TeardownUnsupported(NotImplementedError):
    """Raised when this runner has captured no real teardown mechanism for a host session.

    Releasing the registry entry anyway would make the runner believe a live, authenticated
    host session had been cleaned up when it had not; ownership is retained instead, so the id
    stays inspectable and a caller cannot mistake this for a successful teardown.
    """


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


MARKER_PATTERN = re.compile(r'^PARLEY-PROBE-[0-9a-f]{32}$')


def marker_token():
    """A fresh high-entropy marker per trial so an ack cannot be chance or terminal echo."""
    return f'PARLEY-PROBE-{uuid.uuid4().hex}'


def marker_message(marker):
    """The literal text submitted to the host: an explicit instruction to echo `marker`.

    `detect_outcomes` treats the marker's appearance in an assistant message as acknowledgement,
    but a genuinely awake host asked only to receive an opaque token has no reason to quote it
    back verbatim -- a correct, non-quoting reply would misclassify as `not_observed`, confusing
    a probe artifact with real wake behavior. Asking explicitly removes that ambiguity without
    weakening the check itself, which still matches on the raw token appearing anywhere in the
    reply, not on this instruction's exact wording.
    """
    return f'Automated probe: reply with exactly this token to confirm receipt: {marker}'


@dataclass
class Event:
    """One transcript entry, normalized across hosts.

    `time` is a host-reported or record-derived epoch-seconds float, or None when the source
    carries no per-event timestamp. An undated event is never placed in a trial's window — see
    `detect_outcomes` for why that has to fail closed.
    """

    # 'user' or 'assistant' for a message; 'turn_start'/'turn_end' for a host's own turn-boundary
    # signal (Codex `event_msg` task_started/task_complete/turn_aborted), which carries no text.
    # 'developer'/system entries are filtered before this point.
    role: str
    text: str
    time: float | None = None


@dataclass
class Observation:
    """One read of a host's transcript: what was seen, and whether the channel could be read.

    `observable` False means this read establishes nothing either way — the log was unreachable,
    or it carried entries that cannot be placed relative to submission. Classification must map
    that to `unobservable`, never to `not_observed`: a dead or undatable channel is not negative
    evidence (docs/host-probes.md, Trial protocol).

    `turn_end` is the completion instant of the turn that was already running at submission,
    when the host emits such a signal, and None when it emits none or none arrived yet. A busy
    trial's dependent windows start there (docs/host-probes.md, Trial protocol).
    """

    outcomes: dict = field(default_factory=dict)
    # Which record/event established each entry in `outcomes`, keyed the same
    # (docs/host-probes.md, Trial protocol: "Record which signal established each positive
    # result, not just a timestamp"). Kept separate from `outcomes` itself rather than folded
    # into it, since `outcomes`' float values feed `wake_probe.Trial`'s classification
    # unmodified and must stay exactly that shape.
    signals: dict = field(default_factory=dict)
    observable: bool = True
    turn_end: float | None = None
    # True when this read came from a host that emits its own turn-boundary events
    # (`detect_outcomes`' `turn_stream`), so a captured turn_start/ack is independent evidence
    # of a *new* turn even before turn_end appears. False means the host offers no such signal
    # at all, and any assistant text after submission is indistinguishable from the tail of a
    # turn that was already running -- `run_trial` must not trust it either.
    turn_stream: bool = False


# Names for what established a positive outcome, carried in `Observation.signals`/
# `TrialRun.signals` alongside each outcome's timestamp (docs/host-probes.md, Trial protocol).
SIGNAL_USER_MESSAGE = 'user_message'
SIGNAL_ASSISTANT_MESSAGE = 'assistant_message'
SIGNAL_TURN_BOUNDARY_EVENT = 'turn_boundary_event'
SIGNAL_SUBMIT_EXIT_STATUS = 'submit_exit_status'


def detect_outcomes(events, marker, *, submitted_at, turn_stream=False):
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

    `turn_stream` says the host emits its own turn-boundary events (`turn_start`/`turn_end`
    pseudo-roles). Then the first such `turn_start` is the turn-start outcome rather than the
    first assistant message, and the first `turn_end` is reported separately as the completion
    of whatever turn was already running. Without that stream the caller cannot tell a host that
    stayed silent from one that was still finishing an earlier turn.

    The returned `Observation.signals` names what established each entry in `outcomes`, keyed
    the same -- a user-role match is `SIGNAL_USER_MESSAGE`, an assistant-role match is
    `SIGNAL_ASSISTANT_MESSAGE`, and a `turn_stream` host's own boundary event is
    `SIGNAL_TURN_BOUNDARY_EVENT` (docs/host-probes.md, Trial protocol).
    """
    outcomes = {}
    signals = {}
    undated = False
    turn_end = None
    started = None
    for event in events:
        if event.time is None:
            undated = True
            continue
        if event.time < submitted_at:
            continue
        if event.role == 'turn_start':
            if started is None:
                started = event.time
            continue
        if event.role == 'turn_end':
            if turn_end is None:
                turn_end = event.time
            continue
        if event.role == 'user':
            if marker in event.text:
                outcomes.setdefault('visible', event.time)
                signals.setdefault('visible', SIGNAL_USER_MESSAGE)
            continue
        if event.role != 'assistant':
            continue
        outcomes.setdefault('turn_start', event.time)
        signals.setdefault('turn_start', SIGNAL_ASSISTANT_MESSAGE)
        if marker in event.text:
            outcomes.setdefault('ack', event.time)
            signals.setdefault('ack', SIGNAL_ASSISTANT_MESSAGE)
    if turn_stream:
        outcomes.pop('turn_start', None)
        if started is not None:
            outcomes['turn_start'] = started
            signals['turn_start'] = SIGNAL_TURN_BOUNDARY_EVENT
        else:
            signals.pop('turn_start', None)
    return Observation(outcomes=outcomes, signals=signals, observable=not undated, turn_end=turn_end,
                       turn_stream=turn_stream)


# --- Claude: `claude agents --json [--all] [--cwd ...]` and `claude logs <id>` -----------------

def background_sessions(raw):
    """Filter `claude agents --json` output to background sessions only.

    Interactive sessions report `status`, not `state`, and are never this module's concern:
    only sessions this runner itself starts with `claude --bg` are eligible for any operation.

    A daemon-restarting or otherwise degraded CLI can print valid JSON that is not the expected
    list-of-objects shape -- an error object, `null`, or a bare scalar -- and `claude agents`
    still exits 0 when it does. Raising a clear RuntimeError here, rather than letting a
    non-list top level escape as an uncaught TypeError/AttributeError from the caller's own
    iteration, keeps this failure in the same reportable class as a nonzero exit instead of
    crashing the trial with an unrelated-looking exception.

    A malformed individual entry (non-dict, an unrecognized `kind`, or a `background` entry with
    no usable `id`) is likewise rejected rather than silently dropped: `create()` diffs two calls
    to this function to identify the session it just made, and a malformed entry that is simply
    missing from one snapshot's *filtered* output is indistinguishable from a session that never
    existed there. If that entry happened to be a real, unrelated background session becoming
    well-formed only in the later snapshot -- a listing race, not a probe session -- silently
    dropping it from the earlier snapshot would make the diff mint it as this trial's own.
    Rejecting the whole read instead keeps a listing race from ever reaching the diff at all.
    Only the known, intentionally-ignored `interactive` kind is skipped; a missing or
    schema-drifted kind is rejected the same way, since an entry that is merely malformed in one
    snapshot and a well-formed `background` entry in the other is exactly the same listing race.
    """
    try:
        entries = json.loads(raw)
    except ValueError as error:
        raise RuntimeError(f'claude agents --json produced unparseable output: {error}') from error
    if not isinstance(entries, list):
        raise RuntimeError(
            f'claude agents --json produced a non-list top level ({type(entries).__name__}): {raw!r}')
    sessions = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise RuntimeError(f'claude agents --json listed a non-object entry: {entry!r}')
        kind = entry.get('kind')
        if kind == 'interactive':
            continue
        if kind != 'background':
            raise RuntimeError(f'claude agents --json listed an entry with an unrecognized kind: {entry!r}')
        if not isinstance(entry.get('id'), str) or not entry['id']:
            raise RuntimeError(
                f'claude agents --json listed a background session with an invalid id: {entry!r}')
        sessions.append(entry)
    return sessions


def parse_claude_transcript(raw):
    """Parse `claude logs <id>` plain-text output into normalized Events.

    Only two roles are attributed: a line opening with `User:` or `Assistant:` starts a new
    event and following non-empty lines extend it, matching the multi-line message shape a
    transcript line-printer produces. `claude logs` carries no per-line timestamp, so every
    returned Event has `time=None`; `detect_outcomes` consequently reports an unobservable read
    rather than counting entries that cannot be ordered against submission. This format has not
    yet been captured against a running background session (observe() could not reach a `done`
    one's daemon) and must be reconciled with real output before being relied on for evidence.

    Marker matching is `detect_outcomes`' job, not this parser's: whether a marker appears in
    any event is a fact about the caller's outcomes, not about whether the transcript parsed.
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
    return events


class ClaudeDriver:
    """Drives Claude Code background sessions. `run` defaults to subprocess.run; tests inject a
    fake to avoid launching an installed `claude` binary (AGENTS.md, Test isolation).

    Registry ownership is namespaced by `NAMESPACE` (`_key()`): a bare session id in a
    `SessionRegistry` shared across host drivers would let one driver's minted id satisfy
    another's `require_owned()` check purely by string collision -- Claude's `claude agents`
    surface and Codex's `codex queue --thread` both accept caller-chosen ids/names
    (docs/host-probe-preflight.md), so a coincidentally identical id is not a hypothetical.
    Without the namespace, a Codex thread named the same as a Claude session Claude owns could
    be queued to via `CodexDriver.submit` without ever going through `register_existing`, or the
    reverse could let `teardown()` run `claude rm` against an unrelated Codex-named session.
    """

    NAMESPACE = 'claude'

    def __init__(self, registry, *, run=subprocess.run, cwd, model=None, max_budget_usd=None):
        self.registry = registry
        self.run = run
        self.cwd = cwd
        self.model = model
        self.max_budget_usd = max_budget_usd

    def _key(self, session_id):
        return f'{self.NAMESPACE}:{session_id}'

    def _background_session_ids(self):
        """`claude agents --json --all` background session ids under this driver's cwd.

        `--all` is required: without it the listing carries only `kind: "interactive"` entries
        (docs/host-probe-preflight.md, 2026-09-11).
        """
        result = self.run(['claude', 'agents', '--json', '--all', '--cwd', self.cwd],
                          capture_output=True, text=True, timeout=15)
        if result.returncode != 0:
            raise RuntimeError(f'claude agents exited {result.returncode}: {result.stderr}')
        return {entry['id'] for entry in background_sessions(result.stdout)}

    def _recoverable_candidates(self, before):
        """Best-effort new session ids after an uncertain `claude --bg` outcome.

        Called only when the run itself is already uncertain (a timeout) -- a further listing
        failure here is not this call's problem to raise, since the caller is already reporting
        the original uncertainty. An empty result means only "no candidates could be recovered",
        never "no session was created".
        """
        try:
            return self._background_session_ids() - before
        except (RuntimeError, subprocess.TimeoutExpired):
            return set()

    def create(self, prompt):
        """Start a background session and identify it from a listing diff, never from stdout.

        `claude --bg --print`'s own stdout shape has never been captured against a real
        session — `docs/host-probe-preflight.md` never got far enough to create one — so
        parsing an assumed last-line-is-the-id shape would mint whatever `claude --bg` happens
        to print last, including an informational or footer line, as the owned session; a wrong
        mint also leaves the real session unregistered and unable to be torn down. This instead
        diffs `claude agents --json --all` (the same verified listing `submit()` checks
        membership against) from before to after the command: the session created is whichever
        id appears afterward that did not before.

        An ambiguous diff (zero or more than one new id) still refuses to guess which one this
        trial created, and now refuses to mint any of them either: `claude --bg` already exited
        0, so an extra id it lists is a real, live background session under the operator's real
        HOME regardless of whether this call can identify it, but minting an unverified id gave
        it the same teardown authority as a session this runner actually created -- letting
        `teardown()` accept and `claude rm` a foreign, possibly human, session, contradicting the
        ownership guarantee itself. `AmbiguousSessionCreation.candidates` surfaces the id(s) for
        a human to investigate and clean up out of band instead. The post-create listing call can
        itself fail (timeout, nonzero exit, malformed JSON) after `claude --bg` already
        succeeded; that failure is caught the same way, since it leaves an equally real,
        equally-unidentified session behind and must not propagate as an unrelated exception.

        `claude --bg` itself can also exceed its own 30s timeout after it has already detached
        the background session -- the subprocess call raises before returning, but the session
        it forked is not thereby undone. That case attempts the same post-create listing on a
        best-effort basis (`_recoverable_candidates`) to surface whatever ids can be recovered;
        a further listing failure there is swallowed to an empty candidate set rather than
        raised, since the caller is already reporting the original timeout.
        """
        argv = ['claude', '--bg', '--cwd', self.cwd, '--print']
        if self.model:
            argv += ['--model', self.model]
        if self.max_budget_usd is not None:
            argv += ['--max-budget-usd', str(self.max_budget_usd)]
        argv.append(prompt)
        before = self._background_session_ids()
        try:
            result = self.run(argv, capture_output=True, text=True, timeout=30)
        except subprocess.TimeoutExpired as error:
            raise AmbiguousSessionCreation(
                f'claude --bg under {self.cwd} did not exit within its 30s timeout; it may have '
                'already detached a background session before hanging',
                candidates=sorted(self._recoverable_candidates(before))) from error
        if result.returncode != 0:
            raise RuntimeError(f'claude --bg exited {result.returncode}: {result.stderr}')
        try:
            after = self._background_session_ids()
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            raise AmbiguousSessionCreation(
                f'claude --bg exited 0 but the post-create listing under {self.cwd} could not '
                f'be read ({error!r}); a background session may now be running with an id this '
                'runner never learned', candidates=()) from error
        new = after - before
        if len(new) != 1:
            raise AmbiguousSessionCreation(
                f'claude --bg exited 0 but claude agents --json --all lists {len(new)} new '
                f'background session(s) under {self.cwd} (expected exactly one): '
                f'{sorted(new)!r} -- none minted as owned; this runner cannot verify which, if '
                'any, it created', candidates=sorted(new))
        session_id = new.pop()
        self.registry.mint(self._key(session_id))
        return session_id

    def submit(self, session_id, message):
        """Refuse to report acceptance: no captured mechanism delivers `message` here.

        `claude --bg` takes its prompt at creation, and at 2.1.268 nothing captured submits a
        further message to an *existing* background session — `attach` is an interactive PTY,
        `--remote-control` and `--print --input-format=stream-json` are unexercised
        (docs/host-probe-preflight.md, 2026-09-11). This previously returned True after merely
        listing the session, which recorded an `accepted` outcome for a marker the host never
        received. Guessing an unconfirmed submission flag would violate AGENTS.md's evidence
        rule (the same reason `OpenCodeDriver` refuses), so this raises `SubmissionUncaptured`
        — a statement about this runner, not about Claude — and `run_trial` classifies the cell
        `unobservable`. The listing check runs first, so an absent or foreign session still
        fails as such rather than as a missing mechanism.

        `--all` is required: without it the listing carries only `kind: "interactive"` entries
        (docs/host-probe-preflight.md, 2026-09-11), so `background_sessions()` would return
        nothing and every owned session would fail the membership check below. A nonzero
        listing is reported as such rather than reaching `json.loads` as a decode error.

        A `subprocess.TimeoutExpired` from that listing call is translated to
        `SubmissionUncaptured` rather than left to propagate: this listing is only a presence
        check, run entirely before the unconditional raise below, so its timing out means the
        marker was *definitely* never sent — unlike `run_trial`'s generic
        `subprocess.TimeoutExpired` handling for a driver whose submit call itself performs the
        delivery, where a timeout leaves genuine doubt about whether the host received it first.
        """
        self.registry.require_owned(self._key(session_id))
        try:
            listed = self._background_session_ids()
        except subprocess.TimeoutExpired as error:
            raise SubmissionUncaptured(
                'claude has no captured message-submission path to an existing --bg session; '
                'the presence check itself timed out, so no delivery could have been '
                'attempted either') from error
        if session_id not in listed:
            raise ValueError(f'session not listed under {self.cwd}: {session_id}')
        raise SubmissionUncaptured(
            'claude has no captured message-submission path to an existing --bg session; '
            'creation carries the only delivered prompt')

    def observe(self, session_id, *, marker, submitted_at):
        """Read logs before teardown: a `done` session's daemon socket is already gone.

        A nonzero read is an unavailable channel, returned unobservable so classification
        cannot turn a dead daemon socket into `not_observed`. Non-empty output that yields no
        recognized `User:`/`Assistant:` block is an unrecognized transcript shape, not "read
        cleanly, nothing there yet" — those two must not collapse into the same empty,
        `observable=True` result, or a format this runner cannot parse reads as a host that
        stayed silent. A stalled `claude logs` is the same unavailable channel too: `run_trial`'s
        polling loop does not catch exceptions from `observe`, so an uncaught timeout here would
        abort the whole trial and lose every poll's evidence gathered so far, not just this read.
        """
        self.registry.require_owned(self._key(session_id))
        try:
            result = self.run(['claude', 'logs', session_id], capture_output=True, text=True, timeout=15)
        except subprocess.TimeoutExpired:
            return Observation(observable=False)
        if result.returncode != 0:
            return Observation(observable=False)
        events = parse_claude_transcript(result.stdout)
        if not events and result.stdout.strip():
            return Observation(observable=False)
        return detect_outcomes(events, marker, submitted_at=submitted_at)

    def teardown(self, session_id):
        """Release ownership only after a confirmed removal.

        A failed `claude rm` leaves the background session alive; forgetting it here would make
        every later teardown attempt fail `require_owned`, so the session could never be
        reclaimed and would keep consuming the developer's real host environment.
        """
        self.registry.require_owned(self._key(session_id))
        result = self.run(['claude', 'rm', session_id], capture_output=True, text=True, timeout=15)
        if result.returncode != 0:
            raise RuntimeError(f'claude rm exited {result.returncode} for {session_id}: {result.stderr}')
        self.registry.release(self._key(session_id))


# --- Codex: `codex queue --thread <id> --message <text>` and the rollout JSONL -----------------

def record_time(record):
    """Epoch seconds from a rollout record's ISO-8601 `timestamp`, or None if unusable.

    A timezone-naive stamp is unusable, not merely awkward: `datetime.timestamp()` would read
    it as *local* time and return an epoch offset by the host's UTC offset, which then compares
    against `submitted_at` as a silently wrong instant. Every captured rollout record carries a
    trailing `Z` (docs/host-probe-preflight.md, 2026-09-11), so a naive one is an unknown
    producer and fails closed like any other undatable record.

    Valid JSON is not necessarily an object -- a damaged or schema-drifted line can decode to
    `null`, a number or a list -- and `.get` on any of those raises rather than reading as
    undated, so that shape is checked here rather than at every caller.
    """
    if not isinstance(record, dict):
        return None
    stamp = record.get('timestamp')
    if not isinstance(stamp, str):
        return None
    try:
        when = datetime.datetime.fromisoformat(stamp.replace('Z', '+00:00'))
    except ValueError:
        return None
    return None if when.tzinfo is None else when.timestamp()


def rollout_started_at(lines):
    """Earliest usable record timestamp in a rollout, or None when it carries none.

    Provenance, not transcript: every record type counts here (`session_meta` included), because
    the question is when the thread itself came into existence, not when it was spoken in.

    An unparseable line, or a record whose own timestamp is missing or unusable, makes the whole
    answer None rather than being skipped: skipping it means the earliest record might be the
    one that failed, so a thread that predates the run could pass the provenance check on the
    minimum of whatever happened to survive. Every captured rollout record carries a top-level
    `timestamp` (`record_time`'s docstring), so a record without one is exactly as untrustworthy
    as a line that failed to parse at all — not a legitimate timestamp-free record type.
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
        if when is None:
            return None
        stamps.append(when)
    return min(stamps) if stamps else None


# Codex's own turn-boundary signals, captured in a real rollout (docs/host-probe-preflight.md,
# 2026-09-11). They carry no text and are emitted by the host, not by a speaker, so they become
# pseudo-role Events: the only evidence that separates a *new* turn from the one already running.
TURN_BOUNDARY_ROLES = {'task_started': 'turn_start',
                       'task_complete': 'turn_end',
                       'turn_aborted': 'turn_end'}


# The two outer record kinds this runner reads at all (docs/host-probe-preflight.md,
# 2026-09-11): a rollout also accumulates session_meta/world_state/turn_context/
# token_usage_record records this runner has no use for, at either kind.
CODEX_MESSAGE_RECORD_TYPE = 'response_item'
CODEX_EVENT_RECORD_TYPE = 'event_msg'


# A message's content parts carry a role-appropriate `type` (docs/host-probe-preflight.md,
# 2026-09-11): `input_text` for what the user sent, `output_text` for what the assistant said.
# A part whose `type` doesn't match its record's role is a shape this runner has not captured,
# not a same-meaning synonym worth accepting on the `text` field alone.
ROLE_CONTENT_PART_TYPE = {'user': 'input_text', 'assistant': 'output_text'}


def codex_rollout_events(lines):
    """Extract message and turn-boundary Events from rollout JSONL; returns `(events, unusable)`.

    Skips `developer`-role entries (fixed instructions, not conversation turns) and any outer
    record type this runner has no use for (token_usage_record, world_state, turn_context,
    session_meta, ...). Those are skipped silently: a record this runner has no use for is not
    evidence it failed to read. The outer `type` is checked before the inner `payload.type` is
    ever trusted: an unrelated record whose payload happens to carry `type: "message"` or a
    boundary name must not be accepted as transcript evidence just because of that coincidence.

    `unusable` counts content that *should* have been readable and was not — a line that is not
    valid JSON (a corrupt or half-written rollout), a `response_item`/`event_msg` record whose
    `payload` is missing or not an object, a payload whose `type` is missing or not a string, a
    message record whose `timestamp` is missing or malformed, or a message record whose `content`
    is not the list of parts every captured shape carries (missing, explicit `null`, or a single
    object rather than a list — a structured/tool-call payload this runner has not captured a
    shape for). Neither can be ordered against submission, and opening a file successfully does
    not establish that its transcript was read successfully. The caller reports such a read
    unobservable rather than letting absent outcomes become negative evidence.
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
        if not isinstance(record, dict):
            # Valid JSON that isn't an object -- e.g. a bare `null` from a damaged or
            # schema-drifted rollout -- makes `record.get` raise instead of reading as an
            # unrecognized shape, aborting the whole read rather than just this record.
            unusable += 1
            continue
        outer_kind = record.get('type')
        if outer_kind not in (CODEX_MESSAGE_RECORD_TYPE, CODEX_EVENT_RECORD_TYPE):
            continue
        payload = record.get('payload')
        if not isinstance(payload, dict):
            # A relevant outer record with no readable payload is a corrupted or schema-drifted
            # record, not one of the record kinds this runner has no use for -- reporting it
            # unusable rather than skipping it silently keeps observe() from reading a broken
            # channel as a readable one with nothing worth reporting.
            unusable += 1
            continue
        kind = payload.get('type')
        if not isinstance(kind, str):
            # A missing or non-string type (e.g. a schema-drifted list, object, or omitted key)
            # crashes the membership test below with an unhashable-type TypeError for a non-string
            # value; a bare `None` previously fell through that check and was silently treated as
            # one of the record kinds this runner has no use for. Both a `response_item` and an
            # `event_msg` always carry a `payload.type` in every captured shape
            # (docs/host-probe-preflight.md), so either shape here is a corrupted or drifted
            # record, not evidence of a successful read.
            unusable += 1
            continue
        if outer_kind == CODEX_EVENT_RECORD_TYPE:
            if kind not in TURN_BOUNDARY_ROLES:
                continue
            when = record_time(record)
            if when is None:
                unusable += 1
                continue
            events.append(Event(role=TURN_BOUNDARY_ROLES[kind], text='', time=when))
            continue
        if kind != 'message':
            continue
        role = payload.get('role')
        if role == 'developer':
            continue
        if role not in ('user', 'assistant'):
            # A missing or schema-drifted role (e.g. a future "model") is not the same as the
            # known, intentionally-ignored `developer` role -- treating it the same way could
            # silently drop a current-turn assistant message from the observation while the
            # rollout still reads observable=True, producing a false not_observed instead of
            # reporting the uncaptured shape as unusable.
            unusable += 1
            continue
        when = record_time(record)
        if when is None:
            unusable += 1
            continue
        content = payload.get('content')
        if not isinstance(content, list):
            # `.get('content', [])` only substitutes the default when the key is absent; an
            # explicit `"content": null` or a single structured object both slip past it and
            # would otherwise raise iterating None or silently yield empty text for a shape
            # this runner has not captured.
            unusable += 1
            continue
        texts = []
        for part in content:
            part_text = part.get('text') if isinstance(part, dict) else None
            if not isinstance(part_text, str):
                # A non-dict part, a missing `text` key, or a non-string `text` value (e.g.
                # explicit `null`) is a shape this runner has not captured. `.get('text', '')`
                # let a part that omits `text` entirely default to an empty string and pass as
                # captured; silently dropping a part (dict or not) the same way let an unreadable
                # marker message join down to an ordinary-looking empty string -- negative
                # evidence rather than the unusable read it actually is, and could even produce a
                # false `turn_start` from an assistant record. Fail the whole record closed
                # instead of guessing at a partial join.
                texts = None
                break
            if part.get('type') != ROLE_CONTENT_PART_TYPE[role]:
                # A part's text is only trustworthy alongside the role-appropriate `type` --
                # accepting any string-valued `text` regardless of `type` would also accept an
                # `input_text` part inside an `assistant` record (or the reverse), a shape this
                # runner has never captured and has no evidence reads the same way.
                texts = None
                break
            texts.append(part_text)
        if texts is None:
            unusable += 1
            continue
        events.append(Event(role=role, text=''.join(texts), time=when))
    return events, unusable


class CodexDriver:
    """Drives Codex CLI sessions via `codex queue`. Requires an existing thread id — creating one
    needs an interactive/exec session first; `create()` documents this as the caller's job.

    Registry ownership is namespaced by `NAMESPACE` (`_key()`, see `ClaudeDriver`'s docstring for
    why a shared registry needs it): `codex queue --thread` accepts a caller-chosen name just as
    freely as `claude agents` does, so a name coincidentally shared with a Claude session must not
    satisfy this driver's ownership check.
    """

    NAMESPACE = 'codex'

    def __init__(self, registry, *, run=subprocess.run, rollout_path_for, started_at,
                 sessions_root=None):
        if not math.isfinite(started_at):
            # register_existing()'s `started < self.started_at` and _unruled_out_threads()'s
            # `started >= self.started_at` both evaluate False against a NaN boundary -- an
            # arbitrarily old rollout would then adopt as owned while every concurrent candidate
            # is silently ruled out as "not a rival", and a later submit() could queue into a
            # human's thread under the real HOME.
            raise ValueError(f'started_at must be a finite epoch-seconds timestamp: {started_at!r}')
        self.registry = registry
        self.run = run
        # Injectable lookup from thread id to its rollout file path, so tests never touch
        # $CODEX_HOME/sessions themselves.
        self.rollout_path_for = rollout_path_for
        # Epoch seconds this run began: the provenance boundary register_existing enforces.
        self.started_at = started_at
        # Directory holding every rollout this host writes ($CODEX_HOME/sessions). Adoption
        # needs it to see rival threads; without it there is nothing to compare against.
        self.sessions_root = sessions_root

    def _key(self, thread_id):
        return f'{self.NAMESPACE}:{thread_id}'

    def _owned_thread_ids(self):
        """This driver's own owned thread ids, unprefixed -- never the raw registry keys.

        A shared registry's `created` set can hold another driver's namespaced keys too;
        treating those as Codex thread ids would pass a foreign id straight into
        `rollout_path_for`, which is exactly the cross-namespace confusion the namespace exists
        to prevent.
        """
        prefix = self._key('')
        return {key[len(prefix):] for key in self.registry.created if key.startswith(prefix)}

    def create(self, prompt):
        """Refuse: no non-interactive codex thread-creation path has been captured.

        `codex exec` plausibly creates one, but nothing here has captured what it prints, and
        minting an id from a guessed output shape is the evidence failure `OpenCodeDriver`
        refuses for the same reason. Drive this host with `run_trial(existing_session=...)`,
        which adopts a caller-created thread through the provenance check below.
        """
        raise SessionCreationUncaptured(
            'no captured codex thread-creation path; pass existing_session= to run_trial')

    def _unruled_out_threads(self, adopted):
        """Rollouts under the sessions root that could equally be this run's thread.

        Fail closed in both directions: a neighbour that started after this run did, and a
        neighbour that cannot be read or dated at all, are both ambiguity rather than absence.
        This also covers a subdirectory that cannot even be listed: `glob.glob` swallows that
        `OSError` internally and just returns fewer matches, with no signal that anything was
        skipped, so an unreadable directory would read as "no rivals found" rather than "could
        not check" — the same fail-open shape a per-file read failure is guarded against below.
        `os.walk`'s `onerror` is the only way to observe that failure at all.

        A rollout this run already adopted (an earlier trial's thread) is excluded alongside
        `adopted` itself, not just `adopted`: it is provably a *different* thread this same run
        claimed, not an unresolved competitor for this one, and counting it as a rival made a
        second adoption in the same run always fail — the 3-trials-per-cell protocol
        (docs/host-probes.md, Trial protocol) could never be satisfied for a host with no
        creation path. Excluding it does not weaken the guarantee this method exists for: an
        outside human thread is still caught, since it was never registered here.

        A file's own last-modified time not having moved since before `started_at` proves every
        record inside it predates the run too — the run could not have written to a rollout it
        had not started yet — so such a file is skipped without opening or parsing it. On an
        operator's real, long-lived `$CODEX_HOME/sessions` this is the overwhelming majority of
        rollouts: only the handful touched since this run began need the actual read. Anything
        modified at or after `started_at` still gets the full read; the mtime check only ever
        turns "definitely too old to matter" into a skip, never a rival into a non-rival.
        """
        adopted = os.path.realpath(adopted)
        already_owned = {os.path.realpath(self.rollout_path_for(thread_id))
                         for thread_id in self._owned_thread_ids()}
        rivals = []

        def _cannot_list(error):
            rivals.append(f'<unreadable directory: {error}>')

        for root, _dirs, files in os.walk(self.sessions_root, onerror=_cannot_list):
            for name in files:
                if not name.endswith('.jsonl'):
                    continue
                other = os.path.join(root, name)
                other = os.path.realpath(other)
                if other == adopted or other in already_owned:
                    continue
                try:
                    if os.path.getmtime(other) < self.started_at:
                        continue
                    with open(other, encoding='utf-8') as handle:
                        started = rollout_started_at(handle)
                except (OSError, UnicodeDecodeError):
                    started = None
                if started is None or started >= self.started_at:
                    rivals.append(other)
        return rivals

    def register_existing(self, thread_id):
        """Adopt a thread created during *this run*; refuse anything that predates it.

        Minting whatever id a caller passed defeated the registry's stated guarantee, since
        `submit` then queues a message to it: a mistyped or copy-pasted id could reach an
        ordinary human thread. Provenance comes from the rollout file's own record timestamps
        (confirmed shape, docs/host-probe-preflight.md 2026-09-11) — the earliest record must
        be at or after `started_at`, so a pre-existing thread cannot be adopted by accident.

        "Started after this run did" is still not "created by this run": a human opening their
        own thread meanwhile satisfies it just as well. So adoption also requires that no other
        *unowned* rollout under `sessions_root` could be that thread — one candidate among the
        threads this run hasn't already claimed, or refuse (`_unruled_out_threads` excludes
        threads this run itself already adopted, so three trials against three distinct threads
        in the same run — the protocol docs/host-probes.md requires — can each adopt in turn
        instead of the second one always finding the first as an unresolved rival). This
        orders and isolates a thread; it does not authenticate it, and creating it remains the
        caller's job (class docstring). A thread whose rollout is missing or carries no usable
        timestamp is refused rather than adopted on trust.

        Residual risk this does not close: recency and uniqueness are not proof that *this run*
        created the thread. A human opening the only other thread after `started_at` leaves no
        rival, and this adopts their thread just as readily as one the caller actually made.
        `thread_id` is trusted to be a thread the caller just created — real creation-binding is
        out of scope for this stage (`create()` refuses to guess one, rather than provide a
        false sense of that binding here).
        """
        path = self.rollout_path_for(thread_id)
        if path is None:
            raise ForeignSessionError(f'no rollout file to prove provenance for thread: {thread_id}')
        try:
            with open(path, encoding='utf-8') as handle:
                started = rollout_started_at(handle)
        except (OSError, UnicodeDecodeError) as error:  # unreadable proves nothing; never adopt
            raise ForeignSessionError(f'cannot read rollout for thread {thread_id}: {error}') from error
        if started is None:
            raise ForeignSessionError(f'rollout carries no usable timestamp for thread: {thread_id}')
        if started < self.started_at:
            raise ForeignSessionError(f'thread predates this run and was not created by it: {thread_id}')
        if self.sessions_root is None:
            raise ForeignSessionError('sessions_root is required to rule out concurrent threads')
        rivals = self._unruled_out_threads(path)
        if rivals:
            raise ForeignSessionError(
                f'{len(rivals)} concurrent thread(s) under {self.sessions_root} cannot be told '
                f'apart from this run\'s; refusing to adopt {thread_id}')
        self.registry.mint(self._key(thread_id))
        return thread_id

    def submit(self, thread_id, message):
        self.registry.require_owned(self._key(thread_id))
        result = self.run(['codex', 'queue', '--thread', thread_id, '--message', message],
                           capture_output=True, text=True, timeout=15)
        return result.returncode == 0

    def observe(self, thread_id, *, marker, submitted_at):
        """Read the thread's rollout; an unreadable or undatable rollout is unobservable."""
        self.registry.require_owned(self._key(thread_id))
        path = self.rollout_path_for(thread_id)
        if path is None:
            return Observation(observable=False)  # no rollout to read is a dead channel
        try:
            with open(path, encoding='utf-8') as handle:
                events, unusable = codex_rollout_events(handle)
        except (OSError, UnicodeDecodeError):
            # Same rule as a failed `claude logs`: an unreadable channel is unobservable, not
            # an absence of outcomes. A rollout is created lazily, so an early poll can precede
            # it, and a partially written multi-byte character decodes no better than a missing
            # file — both are the channel being unreadable, not the host being silent.
            return Observation(observable=False)
        # turn_stream: this host reports its own turn boundaries, so a message emitted by the
        # turn that was already running cannot be miscounted as the start of a new one.
        observation = detect_outcomes(events, marker, submitted_at=submitted_at, turn_stream=True)
        if unusable:
            # `signals` must travel with `outcomes` here too: run_trial's polling loop merges
            # outcomes across polls with setdefault(), so an unusable poll that dropped `signals`
            # would let a later clean poll's outcome value in while permanently losing what
            # established it -- setdefault() never overwrites the None already recorded.
            return Observation(outcomes=observation.outcomes, signals=observation.signals,
                               observable=False, turn_end=observation.turn_end, turn_stream=True)
        return observation

    def teardown(self, thread_id):
        """Refuse: no real teardown mechanism has been captured for a Codex thread.

        `codex queue` has no `--stop`/`--delete`/equivalent. Releasing the registry entry
        anyway would make the runner believe a live, authenticated host session had been
        cleaned up when it had not, leaving it running under the operator's real HOME with no
        cleanup (AGENTS.md, Test isolation). Ownership is retained rather than released, so the
        thread stays inspectable and a caller cannot mistake this for a successful teardown.
        """
        self.registry.require_owned(self._key(thread_id))
        raise TeardownUnsupported(
            f'codex has no captured teardown mechanism; thread {thread_id} remains registered '
            'and its host session is still live')


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
    # What established each entry in `outcomes`, keyed the same (docs/host-probes.md, Trial
    # protocol) -- see `Observation.signals`. An outcome absent here was never positively
    # observed, regardless of what `observable` says about the channel.
    signals: dict = field(default_factory=dict)
    # The turn already running at submission, and whether its end could be observed at all.
    # `Trial` needs both to classify a busy cell; omitting them defaulted every busy trial to
    # "no turn was running", which is the one thing a busy trial is defined not to be.
    turn_end: float | None = None
    turn_end_observable: bool = True


def run_trial(driver, *, prompt, marker, state='idle', settle=lambda: None,
              existing_session=None,
              poll_interval=5.0, clock=time.time, monotonic=time.monotonic, sleep=time.sleep):
    """Create, submit and observe one trial through its windows; returns a `TrialRun`.

    `existing_session` adopts a session the caller already created (`driver.register_existing`)
    instead of calling `driver.create`. A host with no captured creation path — Codex today —
    can be driven no other way, and unconditional creation left it undriveable.

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

    A `busy` trial polls to `BUSY_CAP` instead, adjusted — shortened *or extended* — to the
    dependent windows once the running turn's end is observed, because `Trial.result` refuses to
    classify `turn_start`/`ack` before then and gives them a full `LAST_WINDOW` from that end
    even when it lands past `BUSY_CAP` itself. A turn ending at, say, 890s still owes its
    dependent outcomes the window out to 1010s; only a turn ending *after* `BUSY_CAP` gets no
    such extension, because `Trial.result` classifies that case `inconclusive` regardless of how
    much longer polling would wait. If the turn's end never appears at all, a host with no
    turn-boundary stream (`turn_stream` in `detect_outcomes`) has those two outcomes reported
    unobservable outright, captured value or not — it cannot tell "the host ignored us" from "the
    earlier turn was still going" even when an assistant message did show up, since either could
    have produced it. A host that *does* emit turn boundaries keeps `Trial.result`'s own
    distinction instead: an early turn_start/ack is trusted as independent evidence on its own,
    and a channel that stayed readable through the whole cap with no boundary at all reaches
    `inconclusive` via `turn_end_observable` rather than being forced `unobservable` here.

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
    acknowledgement into its 10s window. A host whose *product* lacks the mechanism raises
    `SubmissionUnsupported` and every outcome is `unsupported` — nothing was delivered, so no
    transcript signal could belong to this trial. A host where only *this runner* has captured
    no path raises `SubmissionUncaptured` and every outcome is `unobservable` instead: the same
    empty result, but recorded against us rather than published as a host capability. A clean
    failed submission — `submit` returning `False` rather than raising — skips the polling loop
    entirely instead of falling through to it: `accepted` itself stays observable (a failed
    submission is genuine, true negative evidence), but the transcript outcomes are marked
    unobservable rather than polled to an eventual `not_observed`, since no delivered message
    could ever have produced a signal for them. A `subprocess.TimeoutExpired` from `submit`
    itself is neither of those: the host process may already have received the marker before
    the hard-coded subprocess timeout fired, so this is treated as acceptance proceeding
    (polling continues, in case a delivered marker still produces transcript evidence) with
    `accepted` alone marked unobservable, rather than propagating the exception and losing the
    trial's evidence entirely.

    A `KeyboardInterrupt` during the polling loop — a real risk given a busy trial's up-to-900s
    wait — is caught and finalizes a `TrialRun` from whatever was accumulated so far, rather than
    propagating and losing it. An outcome already seen keeps standing on its own evidence; a
    still-missing transcript outcome is marked unobservable rather than the usual
    "readable channel, genuinely absent", since an interrupted poll never reached its deadline
    and a channel staying readable up to that point is not proof the outcome would never have
    appeared.
    """
    if state not in TRIAL_STATES:
        raise ValueError(f'unknown trial state: {state}')
    if not MARKER_PATTERN.fullmatch(marker):
        # An empty, guessable or hand-typed marker can appear in a transcript for reasons that
        # have nothing to do with this trial -- a short or low-entropy value risks colliding
        # with real conversation text, silently promoting an unrelated message to `ack`. Only
        # `marker_token()`'s own high-entropy shape is accepted; callers needing a marker call
        # it rather than construct one by hand.
        raise ValueError(f'marker does not look like a fresh marker_token() value: {marker!r}')
    if not math.isfinite(poll_interval) or poll_interval <= 0:
        # Caught here, before any session exists or the marker is sent: a negative or NaN
        # interval previously stayed unnoticed until the first `sleep()` call *after*
        # submission, by which point the real host may already have received the marker with no
        # returned evidence at all. Zero would pass that same later check (`sleep(0)` never
        # raises) and instead spin the polling loop CPU-bound for up to 900s.
        raise ValueError(f'poll_interval must be a positive, finite number of seconds: {poll_interval!r}')
    session_id = (driver.register_existing(existing_session) if existing_session is not None
                  else driver.create(prompt))
    settle()
    submitted_at = clock()
    deadline = monotonic() + (BUSY_CAP if state == 'busy' else LAST_WINDOW)
    accepted_unobservable = False
    try:
        accepted = driver.submit(session_id, marker_message(marker))
    except SubmissionUnsupported:
        return TrialRun(session_id=session_id, submitted_at=submitted_at, accepted_at=None,
                        outcomes={}, state=state,
                        supported={name: False for name in OUTCOME_NAMES},
                        observable={name: True for name in OUTCOME_NAMES})
    except SubmissionUncaptured:
        return TrialRun(session_id=session_id, submitted_at=submitted_at, accepted_at=None,
                        outcomes={}, state=state,
                        supported={name: True for name in OUTCOME_NAMES},
                        observable={name: False for name in OUTCOME_NAMES},
                        turn_end_observable=False)
    except subprocess.TimeoutExpired:
        # The subprocess may already have handed the marker to the host before the hard-coded
        # submission timeout fired; the runner just never learned whether it did. Propagating
        # this would lose the trial (and any transcript evidence a delivered marker produced)
        # entirely, so acceptance itself is recorded unobservable rather than assumed either
        # way, and observation continues exactly as if submission had returned True.
        accepted = True
        accepted_unobservable = True
    if not accepted:
        # A clean nonzero exit (not an exception) is a definitive, observed failure to accept --
        # 'not_observed' is the true classification for `accepted` itself -- but nothing was
        # delivered, so no transcript signal could ever belong to this trial. `classify_trial`
        # still won't report that 'not_observed' until `accepted`'s own 10s window has actually
        # elapsed, though: `observable=True` with nothing in `outcomes` means "genuinely still
        # pending" to `Trial.result`, the same as any other outcome, and it has no way to
        # distinguish that from "already known, just tell me now" -- there is no such input to
        # give it. A caller classifying immediately after this return still needs `now >=
        # submitted_at + WINDOWS['accepted']`, same as every other outcome (run_trial's
        # docstring already states callers must wait for windows, not guess). Polling anyway and
        # reporting the missing outcomes as `not_observed` would be negative evidence for a
        # marker the host never received, exactly the confusion `SubmissionUncaptured` exists to
        # prevent for the acceptance channel itself.
        observable = {'accepted': True}
        observable.update({name: False for name in TRANSCRIPT_OUTCOMES})
        return TrialRun(session_id=session_id, submitted_at=submitted_at, accepted_at=None,
                        outcomes={}, state=state,
                        supported={name: True for name in OUTCOME_NAMES}, observable=observable,
                        turn_end_observable=False)
    accepted_at = None if accepted_unobservable else clock()
    outcomes = {}
    signals = {}
    turn_end = None
    channel_readable = False
    turn_stream_capable = False
    interrupted = False
    try:
        while True:
            observation = driver.observe(session_id, marker=marker, submitted_at=submitted_at)
            # The *final* read decides observability, not whether any read ever worked. A transcript
            # is cumulative, so one successful read late in the window covers the earlier gaps; but
            # if the last read failed — a finished Claude session's daemon socket disappearing is
            # exactly this — the tail of the window was never seen, and an outcome missing from a
            # transcript nobody could read at the end is not negative evidence.
            channel_readable = observation.observable
            turn_stream_capable = turn_stream_capable or observation.turn_stream
            for name, when in observation.outcomes.items():
                outcomes.setdefault(name, when)
                signals.setdefault(name, observation.signals.get(name))
            if state == 'busy' and turn_end is None and observation.turn_end is not None:
                # Only a busy trial has a turn "already running at submission" for this field to
                # mean (Observation's docstring). For every other state, the first turn boundary
                # after submission is the completion of the turn *this trial's own marker* started,
                # not a pre-existing one — adopting it here would mislabel that turn as something
                # left running before the trial began.
                turn_end = observation.turn_end
                # The dependent windows run from the turn's end: adjust the deadline to match,
                # shortening it when they close early rather than sitting out the rest of the cap,
                # but also extending it when the end lands close to the cap — `min()` against the
                # cap-based deadline could only ever shorten, silently truncating a turn that ended
                # at e.g. 890s to the 900s cap instead of the 1010s its own window earns it. A turn
                # ending *past* the cap gets no such extension: `Trial.result` classifies that
                # `inconclusive` no matter how much longer polling would wait.
                if turn_end - submitted_at <= BUSY_CAP:
                    deadline = monotonic() + max(0.0, LAST_WINDOW - (clock() - turn_end))
            remaining = deadline - monotonic()
            if remaining <= 0 or all(name in outcomes for name in TRANSCRIPT_OUTCOMES):
                break
            sleep(min(poll_interval, remaining))
    except KeyboardInterrupt:
        # Losing the local outcomes/signals/turn_end accumulated so far to a propagated
        # interrupt would discard real evidence a busy trial's up-to-900s wait may have spent
        # minutes gathering. Finalize instead: an outcome already in `outcomes` keeps standing on
        # its own evidence exactly as a completed trial would, but a still-missing transcript
        # outcome is marked unobservable rather than the usual "readable channel, genuinely
        # absent" -- an interrupted poll never reached its deadline, so the channel staying
        # readable up to this point is not proof the outcome would never have appeared.
        interrupted = True
    if accepted_at is not None:
        outcomes['accepted'] = accepted_at
        signals['accepted'] = SIGNAL_SUBMIT_EXIT_STATUS
    # A positively observed outcome stands on its own evidence; only the ones still missing at
    # the deadline depend on whether the transcript could be read at all. `accepted` itself is
    # unobservable only when the submission call itself timed out without confirming either way.
    observable = {'accepted': not accepted_unobservable}
    for name in TRANSCRIPT_OUTCOMES:
        if name in outcomes:
            observable[name] = True
        elif interrupted:
            observable[name] = False
        else:
            observable[name] = channel_readable
    if state == 'busy' and turn_end is None and not turn_stream_capable:
        # No turn-boundary stream at all: this runner cannot tell a still-running prior turn's
        # tail from a genuinely new one (`detect_outcomes`' docstring), so a captured value is
        # exactly that ambiguity rather than evidence and must not be trusted either way — unlike
        # a turn-stream host, where an early turn_start/ack is independent evidence on its own
        # (`wake_probe.py:57-61`) and a channel that stayed readable with no boundary at all is
        # itself `Trial.result`'s own `inconclusive` case via `turn_end_observable`, not this one.
        for name in ('turn_start', 'ack'):
            observable[name] = False
    # Same reasoning as the transcript outcomes above: an interrupted poll never reached its
    # deadline, so a still-missing turn_end is not proof one would never have appeared, even
    # though the channel itself stayed readable up to the point of interruption.
    turn_end_observable = turn_end is not None or (channel_readable and not interrupted)
    return TrialRun(session_id=session_id, submitted_at=submitted_at, accepted_at=accepted_at,
                    outcomes=outcomes, state=state,
                    supported={name: True for name in OUTCOME_NAMES}, observable=observable,
                    signals=signals, turn_end=turn_end, turn_end_observable=turn_end_observable)


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

"""Matrix runner driving real hosts through wake_probe's Trial/aggregate classification.

Owned by ginsys/parley#18. This module supplies what #18's harness (wake_probe.py) does not:
something that creates a host session, submits one synthetic marker message, observes the four
outcomes (accepted/visible/turn_start/ack) and classifies each `Trial` -- never a production host
adapter or session authenticator. Process creation is injectable everywhere a host CLI would run
(AGENTS.md, Test isolation): fixtures supply a fake `run`/`popen`/`pty`, so no test launches an
installed Claude, Codex or OpenCode binary. The PTY client is exercised only against controlled
Python children.

Every host-facing shape here is the one captured live on 2026-09-13 at Claude Code 2.1.270,
codex-cli 0.154.0 and OpenCode 1.18.30 (docs/host-probe-preflight.md, that section's tables):

- Claude: `claude --bg --model <m> '<prompt>'` prints `backgrounded · <8-hex id>` on stdout line
  1; `claude agents --json --all --cwd <cwd>` lists that id with its full `sessionId`; the
  session's transcript is `$HOME/.claude/projects/*/<sessionId>.jsonl`, one JSON object per line,
  `user`/`assistant` records carrying an ISO `timestamp`; a live session takes a message through
  `claude attach <id>` under a PTY (the only captured live-delivery path); a stopped one through
  `claude stop <id>` then `claude --bg --resume <sessionId> '<msg>'` with no other flags (any flag
  starts a copy); teardown is `claude stop` then `claude rm`.
- Codex: `codex exec --json` prints `{"type":"thread.started","thread_id":...}`; the rollout is
  `$CODEX_HOME/sessions/YYYY/MM/DD/rollout-*-<thread_id>.jsonl`; `codex queue --thread <id>
  --message <text>` exits 0 and the item is delivered by whichever process next serves the thread
  (a live TUI drains it within seconds; `codex ... resume <id>` drains it at start); teardown is
  `codex delete --force <id>`.
- OpenCode: `opencode run --pure --format json` prints events carrying `sessionID`; a live
  `opencode serve --pure --port <p>` takes `opencode run --attach <url> --session <id>`;
  `opencode --pure export <id>` is the observation channel; `opencode --pure session delete <id>`
  the teardown.

A submission this runner cannot vouch for raises `SubmissionUncaptured` (every outcome
`unobservable`), never a host rejection: a `--bg --resume` against a *running* session,
`claude attach` to a stopped one, a PTY that never showed its composer or exited while typing,
a Codex `queue` with no process serving the thread, an OpenCode submit whose `serve` child is
gone, and a listing or queue call that timed out before anything was sent. What a
permission-parked Claude session lists as, how reliable `status: "busy"` is, and whether Codex
delivers a queued item mid-turn are not refused here: they are what the trials measure, and the
settle callback that establishes each state is responsible for checking its own precondition.

Cleanup contract: every driver derives `owned()` from the shared `SessionRegistry`, so any id it
minted -- including a copy `claude --bg --resume` started by accident -- is torn down by
`sweep()`. `run_trial_with_cleanup` is the entry point real trials use: a failure anywhere after
`create()` propagates raw, and its `finally` closes PTY clients, tears down every owned id, then
closes servers. `run_trial` itself no longer wraps exceptions to carry the session id.
"""

import datetime
import glob
import json
import math
import os
import re
import select
import signal
import socket
import stat
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, field

from wake_probe import WINDOWS, PtyProcess

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
# How far wall time may move against `monotonic` across one trial before its timestamps stop
# meaning anything. Every compared stamp is wall time -- host transcripts carry nothing else --
# so `wake_probe.Trial`'s monotonic contract is honoured by detection rather than by conversion:
# a stamp another process already wrote on a stepped clock cannot be rebased afterwards. One
# second is far above ordinary NTP slew (500 ppm, the kernel's ceiling, is 0.45s across the 900s
# busy cap) and far below the smallest window (10s), so it fires on a step and never on slew.
CLOCK_DRIFT_TOLERANCE = 1.0


class ForeignSessionError(ValueError):
    """Raised when an operation targets a session this run did not create."""


class SubmissionUnsupported(NotImplementedError):
    """Raised when the *host* lacks the submission mechanism -- evidence about the host.

    Claude's absent `--channels` is the model case: missing from the help text and the plugin
    cache, so its absence is a property of the product. Classifies the cell `unsupported`. No
    driver raises it today: a known absence like `--channels` is recorded without a live trial
    through `classify_trial(..., supported=...)`; `run_trial` handles it for a driver that does.
    """


class SubmissionUncaptured(NotImplementedError):
    """Raised when *this runner* cannot vouch for the submission -- evidence about us.

    Nothing captured covers the path (a resume against a running session, an attach to a stopped
    one), or the captured path did not reach the point where the host was asked anything (a PTY
    that never showed its composer). The host may well support the mechanism, so the trial
    establishes nothing in either direction and classifies `unobservable`, never `unsupported`
    and never `not_observed`. The message carries whatever diagnostic exists (the stripped PTY
    screen, the command's stdout) and lands in `TrialRun.submission_diagnostic`.
    """


class SubmissionRejected(RuntimeError):
    """Raised by `submit()` to report a definitive command-level rejection with its diagnostic.

    Reserved for a host command's own exit status (`codex queue`, `claude --bg --resume`,
    `opencode run --attach`): real, observed negative evidence for `accepted`, folded by
    `run_trial` into the not-accepted shape with the returncode/stderr retained via
    `submission_diagnostic`. A PTY-mechanism failure is never this -- see `SubmissionUncaptured`.
    """

    def __init__(self, returncode, stderr):
        super().__init__(f'submission rejected: exit {returncode}: {stderr}')
        self.returncode = returncode
        self.stderr = stderr


class PtyNotReady(RuntimeError):
    """Raised by a driver's `attach()` when the TUI never showed its captured ready pattern.

    `screen` is the stripped terminal text at the time of giving up, so a reader can see what
    the client was actually looking at (a permission dialog, a login prompt, nothing at all).
    """

    def __init__(self, message, *, screen):
        super().__init__(f'{message}: {screen[-600:]!r}')
        self.screen = screen


class CleanupFailed(RuntimeError):
    """Raised by `run_trial_with_cleanup` when the trial completed but the sweep did not.

    `run` is the completed `TrialRun` (still valid evidence), `failures` the per-id errors the
    sweep collected, `owned` the ids still registered afterwards -- each a live, authenticated
    host session a human must clean up out of band.
    """

    def __init__(self, run, failures, owned):
        super().__init__(f'cleanup incomplete; still owned: {owned}; failures: {failures!r}')
        self.run = run
        self.failures = failures
        self.owned = owned


@dataclass
class SessionRegistry:
    """Tracks session ids created by *this run*; refuses to touch anything else.

    `claude agents --json --all` and `codex queue --thread` reach every session on the
    workstation, including ordinary human work. No driver may submit to, observe or tear down an
    id this registry did not itself mint via `mint()` -- enforced here, not left to each driver
    to remember. Keys are namespaced `<host>:<id>` by the drivers, so a Codex thread that happens
    to share a name with a Claude session never satisfies the other driver's check.

    Each key is stored *with the driver instance that minted it*, in the one store, because
    registration and ownership must not be separable: a Ctrl-C landing between two writes would
    leave a created host session registered but owned by nobody, so the sweep would pass over a
    live real-HOME session while the key that names it blocked re-registration. `created[key] =
    owner` is a single store, so the id is registered and attributed together or not at all.
    """

    created: dict = field(default_factory=dict)

    def mint(self, session_id, owner):
        if not session_id:
            raise ValueError('empty session id')
        if session_id in self.created:
            raise ValueError(f'session already registered: {session_id}')
        self.created[session_id] = owner
        return session_id

    def owner(self, session_id):
        """The instance that minted `session_id`; `ForeignSessionError` if this run did not."""
        if session_id not in self.created:
            raise ForeignSessionError(f'refusing to operate on foreign session: {session_id}')
        return self.created[session_id]

    def require_owned(self, session_id):
        self.owner(session_id)

    def owned_by(self, owner):
        return {key for key, held in self.created.items() if held is owner}

    def release(self, session_id):
        self.require_owned(session_id)
        self.created.pop(session_id, None)


MARKER_PATTERN = re.compile(r'^PARLEY-PROBE-[0-9a-f]{32}$')


def marker_token():
    """A fresh high-entropy marker per trial so an ack cannot be chance or terminal echo."""
    return f'PARLEY-PROBE-{uuid.uuid4().hex}'


def marker_message(marker):
    """The literal text submitted to the host: an explicit instruction to echo `marker`.

    `detect_outcomes` treats the marker's appearance in an assistant message as acknowledgement,
    but a genuinely awake host asked only to receive an opaque token has no reason to quote it
    back verbatim -- a correct, non-quoting reply would misclassify as `not_observed`. Asking
    explicitly removes that ambiguity without weakening the check, which still matches on the raw
    token appearing anywhere in the reply, not on this wording.
    """
    return f'Automated probe: reply with exactly this token to confirm receipt: {marker}'


@dataclass
class Event:
    """One transcript entry, normalized across hosts.

    `time` is a host-reported epoch-seconds float, or None when the source carries no per-event
    timestamp. An undated event is never placed in a trial's window -- see `detect_outcomes`.
    """

    # 'user' or 'assistant' for a message; 'turn_start'/'turn_end' for a host's own turn-boundary
    # signal (Codex `event_msg` task_started/task_complete/turn_aborted), which carries no text.
    role: str
    text: str
    time: float | None = None
    # The model the host recorded as producing an assistant message, or None on any other event
    # and on a host whose transcript does not name one. Best-effort evidence, never an outcome:
    # a matrix cell that cannot cite a model must say so rather than repeat what was requested,
    # since the requested model is not necessarily the serving one (Claude `--model haiku` was
    # captured *not* being honoured).
    model: str | None = None


@dataclass
class Observation:
    """One read of a host's transcript: what was seen, and whether the channel could be read.

    `observable` False means this read establishes nothing either way -- the log was unreachable,
    or it carried entries that cannot be placed relative to submission. Classification must map
    that to `unobservable`, never to `not_observed`: a dead or undatable channel is not negative
    evidence (docs/host-probes.md, Trial protocol).

    `turn_end` is the completion instant of the turn that was already running at submission,
    when the host emits such a signal, and None when it emits none or none arrived yet. A busy
    trial's dependent windows start there.
    """

    outcomes: dict = field(default_factory=dict)
    # Which record/event established each entry in `outcomes`, keyed the same (docs/host-probes.md,
    # Trial protocol). Kept apart from `outcomes`, whose floats feed `wake_probe.Trial` unmodified.
    signals: dict = field(default_factory=dict)
    observable: bool = True
    turn_end: float | None = None
    # True when this read came from a host that emits its own turn-boundary events
    # (`detect_outcomes`' `turn_stream`), so a captured turn_start/ack is independent evidence
    # of a *new* turn even before turn_end appears. False means the host offers no such signal,
    # and bare assistant activity cannot establish a new turn. A matching fresh marker still
    # establishes acknowledgement independently of which turn produced it.
    turn_stream: bool = False
    # The model named by the first in-window assistant message that names one, or None. See
    # `Event.model`; `run_trial` carries the first non-None reading onto the `TrialRun`.
    model: str | None = None


SIGNAL_USER_MESSAGE = 'user_message'
SIGNAL_ASSISTANT_MESSAGE = 'assistant_message'
SIGNAL_TURN_BOUNDARY_EVENT = 'turn_boundary_event'
SIGNAL_SUBMIT_EXIT_STATUS = 'submit_exit_status'


def detect_outcomes(events, marker, *, submitted_at, turn_stream=False):
    """Classify normalized `events` into an `Observation` over visible/turn_start/ack.

    `submitted_at` is an epoch-seconds float on the same clock as each `Event.time`; events
    strictly before it are ignored (host history from before this trial). Acceptance is not a
    transcript signal -- the caller supplies it from the submit command's own result. The first
    assistant event of any content is `turn_start`; only one whose text contains the marker also
    counts as `ack`, so an unrelated assistant reply cannot be mistaken for acknowledging this
    trial's message. The marker is searched for, never matched whole: a Codex user record wraps
    the prompt in the host's own injected text (docs/host-probe-preflight.md, 2026-09-13).
    `Observation.model` is the first in-window assistant event's model, so a trial reports the
    model that served *it* rather than whichever one the session started under.

    An event with `time=None` cannot be ordered against `submitted_at`, so it is skipped and the
    whole read is reported unobservable. Promoting undated entries into the window would
    manufacture outcomes out of pre-submission history.

    `turn_stream` says the host emits its own turn-boundary events (`turn_start`/`turn_end`
    pseudo-roles). Then the first such `turn_start` is the turn-start outcome rather than the
    first assistant message, and the first `turn_end` is reported separately as the completion
    of whatever turn was already running. Without that stream the caller cannot tell a host that
    stayed silent from one that was still finishing an earlier turn.
    """
    outcomes = {}
    signals = {}
    undated = False
    turn_end = None
    started = None
    model = None
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
        if model is None:
            model = event.model
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
                       turn_stream=turn_stream, model=model)


def record_time(record):
    """Epoch seconds from a record's ISO-8601 `timestamp`, or None if unusable.

    A timezone-naive stamp is unusable, not merely awkward: `datetime.timestamp()` would read it
    as *local* time and return an epoch offset by the host's UTC offset. Every captured Claude and
    Codex record carries a trailing `Z`, so a naive one is an unknown producer and fails closed.
    Valid JSON is not necessarily an object, so that shape is checked here too.
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


def _partial_stdout(error):
    """Whatever a timed-out `subprocess.run` had captured on stdout, as text.

    `TimeoutExpired.output` is bytes even under `text=True` (the exception is raised from
    `communicate()` before decoding), or None when nothing was captured. A host can already have
    backgrounded a session or started a thread and printed its id before the timeout fired, so
    every `create()` scans this before re-raising, and mints what it finds.
    """
    output = error.output
    if output is None:
        return ''
    return output.decode('utf-8', 'replace') if isinstance(output, bytes) else output


def _utc_now():
    return datetime.datetime.now(datetime.UTC).isoformat(timespec='milliseconds')


# --- PTY client ---------------------------------------------------------------------------------

# CSI (parameters, intermediates such as the space in `ESC [ 0 SP q`, final byte), OSC up to
# BEL or ST, charset selection (`ESC ( B`), then any other two-character ESC sequence
# (`ESC M`, `ESC 7`, `ESC =`, ...). Alternation order matters: the longer forms go first.
ANSI_PATTERN = re.compile(rb'\x1b\[[0-?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)'
                          rb'|\x1b[()*+][0-~]|\x1b[0-~]')


def strip_ansi(data):
    """Terminal bytes to plain text: escape sequences removed, `\\r\\n` and a bare `\\r` as `\\n`.

    A PTY line discipline emits `\\r\\n` for every `\\n` (ONLCR), and a TUI's own cursor
    return is a bare `\\r`; both read as a line break so `(?m)^...$` patterns see lines.
    """
    text = ANSI_PATTERN.sub(b'', data).replace(b'\r\n', b'\n').replace(b'\r', b'\n')
    return text.decode('utf-8', 'replace')


class PtyClient:
    """A host TUI under a PTY: wait for a captured ready pattern, type a line, detach or kill.

    Wraps `wake_probe.PtyProcess` for fork/exec and teardown only. Output is drained
    continuously by a background thread into a bounded rolling window, bypassing `PtyProcess`'s
    hard transcript cap: a client held open through a 900s busy trial would otherwise either
    fill the kernel pty buffer and stall the host (a Codex TUI blocked on stdout is the only
    process serving its thread's queue) or trip that cap mid-trial. Input goes through
    `os.write` directly, never `PtyProcess.send`: that guard demands an *observed* idle state,
    and a trial's state is the settle callback's precondition, not something read off the
    screen -- an attach to a busy or approval-parked session must still be able to type.

    Tested only against controlled Python children (AGENTS.md, Test isolation), never an
    installed host CLI. `TERM` is `xterm-256color` and `PWD` the resolved cwd, matching the
    Claude attach and Codex TUI captures (docs/host-probe-preflight.md, 2026-09-13).
    """

    def __init__(self, argv, *, cwd, env=None, window=256 * 1024, generation='inherit'):
        env = dict(os.environ if env is None else env)
        env['TERM'] = 'xterm-256color'
        env['PWD'] = os.path.realpath(cwd)
        self.window = bytearray()
        self.limit = window
        self.total = 0  # bytes ever received; `mark()`/`since` offsets index this, not the window
        self.eof = False
        self.closing = False
        self.exit_code = None
        self.lock = threading.Lock()
        self.last_output = time.monotonic()
        self.process = PtyProcess(argv, cwd=cwd, env=env, generation=generation)
        try:
            self.thread = threading.Thread(target=self._drain, daemon=True)
            self.thread.start()
        except BaseException:
            # The child is already running and this object is about to be discarded, so nothing
            # would ever hold a handle to it: an authenticated host client would keep serving
            # its session while the sweep tore the session down around it.
            self.closing = True
            self.process.close()
            raise

    def _drain(self):
        fd = self.process.fd
        while not self.closing:
            try:
                readable, _, _ = select.select([fd], [], [], 0.2)
            except (OSError, ValueError):
                self.eof = True
                break
            if not readable:
                continue
            try:
                data = os.read(fd, 4096)
            except BlockingIOError:
                continue
            except OSError:  # EIO: the child side is gone
                data = b''
            if not data:
                self.eof = True
                break
            with self.lock:
                self.window += data
                self.total += len(data)
                del self.window[:-self.limit]
                self.last_output = time.monotonic()

    def mark(self):
        """An offset for `text_since`/`wait_for(since=...)`: only output after now counts."""
        with self.lock:
            return self.total

    def text_since(self, offset=0):
        """Stripped text produced after `offset` (a `mark()` value), bounded by the window."""
        with self.lock:
            dropped = self.total - len(self.window)
            data = bytes(self.window[max(0, offset - dropped):])
        return strip_ansi(data)

    def screen(self, limit=1500):
        """The most recent stripped text, for diagnostics."""
        return self.text_since(0)[-limit:]

    def wait_for(self, pattern, *, timeout, quiet=1.0, since=0):
        """True once `pattern` matches text after `since` and no output arrived for `quiet`s.

        The quiet requirement is what distinguishes a composer that is ready from one whose
        placeholder is drawn while a turn or a dialog is still in progress: both captured TUIs
        render their prompt text early and keep redrawing (a spinner, a trust dialog) until
        they are actually idle. Readiness is a property of a live client: a child that exited
        after drawing its composer serves nothing, so an exit returns False even when the pattern
        had matched. Returns False on timeout or exit.
        """
        regex = re.compile(pattern)
        deadline = time.monotonic() + timeout
        while True:
            if self.eof:
                return False
            matched = regex.search(self.text_since(since)) is not None
            if matched and time.monotonic() - self.last_output >= quiet:
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.1)

    def send_keys(self, data):
        """Write raw bytes to the child's terminal; a partial write is an error, never delivery."""
        if self.process.fd is None or self.eof:
            raise ValueError('closed or exited session')
        written = 0
        while written < len(data):
            written += os.write(self.process.fd, data[written:])
        return written

    def type_line(self, text, *, chunk=32, gap=0.05, pause=0.5):
        """Type `text` as a human would, then Enter after a pause.

        Both captured composers treat a large burst as a paste that stays in the composer
        (docs/host-probe-preflight.md, 2026-09-13: Codex row for the TUI, Claude row for attach),
        so the text goes in short chunks with a gap, and the CR is sent separately after `pause`.
        The captured Claude line was 45 characters typed in one burst; `marker_message()` is
        longer than any captured burst, hence the chunking.
        """
        payload = text.encode()
        for start in range(0, len(payload), chunk):
            self.send_keys(payload[start:start + chunk])
            time.sleep(gap)
        time.sleep(pause)
        self.send_keys(b'\r')

    def close(self):
        """Stop draining, then kill the client's own process group and reap it.

        The client is its own session leader (pty.fork), so the kill reaches it and anything it
        spawned, never a host daemon that predates it: for Claude the background daemon keeps
        running (captured: Ctrl-Z detached the attach client, exit 0, session still listed).
        Drivers send the detach key first where one is captured; the kill is the backstop.
        """
        self.closing = True
        self.thread.join(timeout=2.0)
        self.process.close()
        self.exit_code = self.process.exit_code


# --- Driver base --------------------------------------------------------------------------------

def initial_git_config(text):
    """Accept only the plain core settings written by a fresh Linux `git init`.

    This deliberately recognizes a narrow baseline, not the Git configuration language: no
    includes, subsections, continuations or executable settings. A real-git fixture checks the
    baseline rather than assuming the installed Git still produces it.
    """
    lines = text.strip().splitlines()
    if not lines or lines.pop(0).strip() != '[core]':
        return False
    allowed = {'repositoryformatversion': {'0'}, 'filemode': {'true', 'false'},
               'bare': {'false'}, 'logallrefupdates': {'true'}}
    seen = set()
    for line in lines:
        match = re.fullmatch(r'[ \t]*([a-zA-Z]+)[ \t]*=[ \t]*([a-z0-9]+)[ \t]*', line)
        if match is None:
            return False
        key, value = match.groups()
        key = key.lower()
        if key in seen or value not in allowed.get(key, set()):
            return False
        seen.add(key)
    return seen == allowed.keys()


def unfresh_git_reason(path):
    """Why `<path>/.git` is not the history-free repository `git init` leaves, or None.

    Checked rather than trusted: an existing repository whose worktree files were merely deleted,
    a `.git` *file* pointing at a linked worktree or a submodule's real store elsewhere, and a
    `.git` *symlink* to either of those all carry the name `.git` and would hand the authenticated
    hosts real history and configuration. A `git init` and nothing else leaves no index, no
    reflog, no refs, no objects, and no symlink anywhere beneath `.git`; anything beyond that is
    history, or a store this directory does not hold.
    """
    git = os.path.join(path, '.git')
    if os.path.islink(git):
        return '`.git` is a symlink: it names a store outside the probe directory'
    if not os.path.isdir(git):
        return '`.git` is not a directory: it points at a store outside the probe directory'
    for name in ('index', 'packed-refs', 'logs', 'shallow'):
        if os.path.lexists(os.path.join(git, name)):
            return f'`.git/{name}` exists'
    # `os.walk` does not descend into a symlinked directory, but every read below opens one by
    # name -- `os.walk('.git/refs')`, `os.listdir('.git/objects')` -- and those do follow. One
    # sweep for links first is what keeps the freshness checks inside the probe directory.
    for root, directories, files in os.walk(git):
        for entry in sorted(directories + files):
            candidate = os.path.join(root, entry)
            if os.path.islink(candidate):
                return (f'`{os.path.relpath(candidate, path)}` is a symlink: `git init` leaves '
                        f'none, and reading through it would leave the probe directory')
            if not (stat.S_ISDIR(os.lstat(candidate).st_mode) or
                    stat.S_ISREG(os.lstat(candidate).st_mode)):
                return f'`{os.path.relpath(candidate, path)}` is not a regular file or directory'
    for root, _directories, files in os.walk(os.path.join(git, 'refs')):
        if files:
            return f'`{os.path.join(root, sorted(files)[0])}` exists'
    hooks = os.path.join(git, 'hooks')
    if os.path.exists(hooks):
        if not os.path.isdir(hooks):
            return '`.git/hooks` is not a directory'
        for entry in sorted(os.listdir(hooks)):
            if not entry.endswith('.sample') or not os.path.isfile(os.path.join(hooks, entry)):
                return f'`.git/hooks/{entry}` is not a sample hook'
    config = os.path.join(git, 'config')
    if not os.path.isfile(config):
        return '`.git/config` is not a regular file'
    try:
        with open(config, encoding='ascii') as handle:
            if not initial_git_config(handle.read()):
                return '`.git/config` differs from the fresh core configuration'
    except (OSError, UnicodeError):
        return '`.git/config` could not be read as fresh core configuration'
    objects = os.path.join(git, 'objects')
    for entry in sorted(os.listdir(objects)) if os.path.isdir(objects) else ():
        if entry not in ('info', 'pack'):
            return f'`.git/objects/{entry}` exists'
        if os.listdir(os.path.join(objects, entry)):
            return f'`.git/objects/{entry}` is not empty'
    return git_baseline_difference(git)


def git_baseline_difference(git):
    """Compare the entire store to neutral installed Git output without initializing the cwd.

    Config has already passed the narrow semantic check above. HEAD supplies only the initial
    branch name; Git validates it while creating a separate disposable baseline. Everything
    else, including templates, must match that baseline exactly.
    """
    try:
        with open(os.path.join(git, 'HEAD'), encoding='ascii') as handle:
            head = handle.read(4096)
        match = re.fullmatch(r'ref: refs/heads/([^\n\r]+)\n', head)
        if match is None:
            return '`.git/HEAD` is not an initial branch reference'
        env = {key: value for key, value in os.environ.items() if not key.startswith('GIT_')}
        env.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_SYSTEM=os.devnull,
                   GIT_CONFIG_NOSYSTEM='1')
        with tempfile.TemporaryDirectory(prefix='parley-git-baseline-') as baseline:
            subprocess.run(['git', 'init', '--quiet', '--initial-branch=' + match[1], baseline],
                           env=env, check=True, capture_output=True, timeout=10)
            expected = os.path.join(baseline, '.git')

            def catalog(root):
                return {os.path.relpath(os.path.join(directory, name), root): kind
                        for directory, directories, files in os.walk(root)
                        for names, kind in ((directories, 'directory'), (files, 'file'))
                        for name in names}

            actual_catalog = catalog(git)
            if actual_catalog != catalog(expected):
                return '`.git` paths differ from a fresh initialization'
            for name, kind in actual_catalog.items():
                if kind != 'file' or name == 'config':
                    continue
                actual_file, expected_file = os.path.join(git, name), os.path.join(expected, name)
                if os.path.getsize(actual_file) != os.path.getsize(expected_file):
                    return f'`.git/{name}` differs from a fresh initialization'
                with open(actual_file, 'rb') as actual, open(expected_file, 'rb') as reference:
                    if actual.read() != reference.read():
                        return f'`.git/{name}` differs from a fresh initialization'
    except (OSError, UnicodeError, ValueError, subprocess.SubprocessError):
        return '`.git` could not be compared with a neutral fresh initialization'
    return None


def private_directory(cwd):
    """The resolved probe directory, refused unless it is empty or holds only a fresh `.git`.

    `cwd` keys three real-HOME side effects: Codex persists a trust entry for it in
    `$CODEX_HOME/config.toml`, Claude files the transcript under a slug of it, and a real
    repository's contents would feed the model. A fresh private directory per run is the
    precondition every capture was made under (docs/host-probe-preflight.md, 2026-09-13).

    The `.git` exception exists because that capture's Codex probe directory was `git init`ed
    (`codex resume` is given no `--skip-git-repo-check`), and it is only ever a *fresh* one --
    see `unfresh_git_reason`.
    """
    path = os.path.realpath(cwd)
    if not os.path.isdir(path):
        raise ValueError(f'probe cwd is not a directory: {cwd!r}')
    metadata = os.stat(path)
    if metadata.st_uid != os.geteuid():
        raise ValueError('probe cwd must be owned by the current operator')
    if metadata.st_mode & 0o022:
        raise ValueError('probe cwd must not be writable by the group or other users')
    ancestor = os.path.dirname(path)
    while True:
        parent_metadata = os.stat(ancestor)
        if parent_metadata.st_uid not in (0, os.geteuid()):
            raise ValueError(f'probe cwd ancestor has an untrusted owner: {ancestor!r}')
        if parent_metadata.st_mode & 0o022 and not parent_metadata.st_mode & stat.S_ISVTX:
            raise ValueError(f'probe cwd ancestor permits replacement by other users: {ancestor!r}')
        parent = os.path.dirname(ancestor)
        if parent == ancestor:
            break
        ancestor = parent
    entries = set(os.listdir(path))
    extra = entries - {'.git'}
    if extra:
        raise ValueError(f'probe cwd must be a fresh private directory; found {sorted(extra)} in {path}')
    if '.git' in entries:
        reason = unfresh_git_reason(path)
        if reason is not None:
            raise ValueError(f'probe cwd {path} holds a repository with history rather than a bare '
                             f'`git init`: {reason}')
    return path


class Driver:
    """What every host driver shares: namespaced ownership, transient handles, a private cwd.

    Ownership is *per instance*, not per registry: `mint()` records the id in the shared
    `SessionRegistry` (which keeps the run-wide "never touch a foreign session" guarantee)
    together with this instance as its owner, and `owned()`/`require_owned()` read that owner.
    Two instances of one host driver over a shared registry therefore cannot reach each other's
    sessions -- without that, sweeping instance A would close only A's clients and then tear down
    B's sessions while B's PTY client or server was still serving them, so one
    `run_trial_with_cleanup()` could delete another live trial. A copy minted inside `submit()`
    is attributed the same way and is swept with the session it copied. One store, not two: see
    `SessionRegistry` for why registration and attribution cannot be separable.
    `clients` holds open `PtyClient`s (closed by `sweep()` before any teardown, since a Codex
    resume client is what serves its thread); `close_servers()` is the hook for a driver that
    runs a server process (closed after teardown).
    """

    NAMESPACE = ''

    def __init__(self, registry, *, cwd):
        self.registry = registry
        self.cwd = private_directory(cwd)
        self.clients = []
        self.strays = set()
        # Free-text evidence about the last submit() call for a mechanism with no exit status of
        # its own (the attach path); `run_trial` copies it into `TrialRun.submission_diagnostic`.
        self.submission_note = None

    def _key(self, session_id):
        return f'{self.NAMESPACE}:{session_id}'

    def mint(self, session_id):
        self.registry.mint(self._key(session_id), self)
        return session_id

    def require_owned(self, session_id):
        if self.registry.owner(self._key(session_id)) is not self:
            raise ForeignSessionError(
                f'refusing to operate on {session_id}: created by another driver instance over '
                f'the same registry, which still holds its clients and servers')

    def release(self, session_id):
        self.require_owned(session_id)
        self.registry.release(self._key(session_id))

    def owned(self):
        cut = len(self.NAMESPACE) + 1
        return {key[cut:] for key in self.registry.owned_by(self)}

    def open_client(self, argv):
        """Launch a PTY client in the probe cwd and hold it, in one statement.

        The returned object is the only handle to an authenticated host child, so it lands on
        `clients` in the same statement that creates it: anything between the two -- a Ctrl-C, a
        failure -- would leave that child serving a real-HOME session with nothing for the sweep
        to close. `PtyClient` itself closes the process if its own construction fails after the
        child started. Drivers that open clients supply `pty`.
        """
        self.clients.append(self.pty(argv, cwd=self.cwd))
        return self.clients[-1]

    def live_client_for(self, session_id):
        self.require_owned(session_id)
        return next((client for client in self.clients
                     if getattr(client, 'serves', None) == session_id and not client.eof), None)

    def close_clients(self):
        """Close every held client; one whose close fails stays on `clients` and is reported.

        A failed close may leave the child alive, and it is the only handle to it: dropping it
        would make the child unrecoverable and let the sweep delete a thread it still serves.
        """
        failures = []
        for client in list(self.clients):
            try:
                client.close()
            except Exception as error:
                failures.append(('client', error))
            else:
                self.clients.remove(client)
        return failures

    def close_servers(self):
        return []


# --- Claude: `claude --bg`, `claude agents --json --all --cwd`, the JSONL transcript ------------

# stdout line 1 of `claude --bg ...` (captured: `backgrounded · 69aa52ed`); the short id is the
# first 8 hex digits of the listing's `sessionId`.
BACKGROUNDED_PATTERN = re.compile(r'^backgrounded\s+\S+\s+([0-9a-f]{8})\s*$')
# The idle composer line of `claude attach` (captured: `❯` followed by a non-breaking space, alone
# on its line, under a rule of `─`). An earlier `❯ <text>` line is the previous prompt echoed back
# and does not match; a permission dialog's layout is uncaptured.
CLAUDE_READY_PATTERN = r'(?m)^❯[ \xa0]*$'
CLAUDE_MECHANISMS = ('attach', 'resume')
# How long `create()` waits for the creation turn to finish. The captured turn took 12 s for a
# one-word reply; the ceiling is generous because a turn still running past it is an error, not a
# trial -- see `ClaudeDriver._settle_creation_turn`.
CLAUDE_CREATION_TURN_CAP = 180.0


def backgrounded_id(stdout):
    """The short session id from `claude --bg`'s first stdout line, or None."""
    lines = stdout.splitlines()
    match = BACKGROUNDED_PATTERN.match(lines[0]) if lines else None
    return match.group(1) if match else None


def claude_transcript_events(lines):
    """Extract user/assistant Events from a Claude session transcript; returns `(events, unusable)`.

    Captured shape (docs/host-probe-preflight.md, 2026-09-13): `{"type": "user", "timestamp":
    "<ISO Z>", "message": {"role": "user", "content": "<text>"}, ...}` and `{"type":
    "assistant", "timestamp": ..., "message": {"role": "assistant", "content": [{"type":
    "text", "text": ...}]}}` -- a string on user records, a list of typed parts on assistant
    records, some of which carry only a `thinking` part. Bookkeeping record types (`attachment`,
    `system`, `file-history-snapshot`, `last-prompt`, `mode`, ... many without any timestamp)
    are skipped silently: a record this runner has no use for is not a failed read.

    `unusable` counts what should have been readable and was not: a line that is not JSON, a
    record that is not an object or whose `type` is not a string, or a user/assistant record
    whose timestamp is missing/naive, whose `message.role` disagrees with its type, or whose
    content does not carry its own role's captured shape. The shapes are role-specific and
    checked as such: an assistant string or a user part list is a changed or malformed
    transcript, and promoting one would let a marker in the wrong shape establish an
    acknowledgement instead of making the read unobservable. The caller reports such a read
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
        if not isinstance(record, dict) or not isinstance(record.get('type'), str):
            unusable += 1
            continue
        kind = record['type']
        if kind not in ('user', 'assistant'):
            continue
        when = record_time(record)
        message = record.get('message')
        if when is None or not isinstance(message, dict) or message.get('role') != kind:
            unusable += 1
            continue
        content = message.get('content')
        if kind == 'user' and isinstance(content, str):
            text = content
        elif kind == 'assistant' and isinstance(content, list):
            texts = []
            for part in content:
                if not isinstance(part, dict) or part.get('type') not in ('text', 'thinking'):
                    texts = None
                    break
                if part.get('type') != 'text':
                    continue  # the captured thinking part carries no transcript text
                if not isinstance(part.get('text'), str):
                    texts = None
                    break
                texts.append(part['text'])
            if texts is None:
                unusable += 1
                continue
            text = ''.join(texts)
        else:
            unusable += 1
            continue
        # `message.model` (captured: `claude-sonnet-5` on the creation and attach turns, and
        # `claude-opus-5` on the no-flag resume turn, from a session created with
        # `--model haiku`). Best-effort: absent or non-string reads as None rather than making
        # the whole transcript unobservable, since no outcome depends on it.
        model = message.get('model') if kind == 'assistant' else None
        events.append(Event(role=kind, text=text, time=when,
                            model=model if isinstance(model, str) else None))
    return events, unusable


def claude_session_version(lines):
    """The single `version` the session's own user/assistant records carry, or None.

    The daemon that produced the transcript writes its version on every message record
    (captured: `"version": "2.1.270"`); the installed binary can differ -- it drifted
    2.1.267 → 268 → 269 → 270 across three days of captures, and a daemon started before an
    upgrade keeps the old code. Records that disagree, or none carrying a string, read as None.
    """
    versions = set()
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if not isinstance(record, dict) or record.get('type') not in ('user', 'assistant'):
            continue
        if isinstance(record.get('version'), str):
            versions.add(record['version'])
    return versions.pop() if len(versions) == 1 else None


def is_session_uuid(value):
    """The lowercase hyphenated UUID shape captured for Claude and Codex session IDs."""
    return isinstance(value, str) and re.fullmatch(
        r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', value) is not None


def default_claude_transcript_path(session_uuid):
    """Newest `$HOME/.claude/projects/*/<sessionId>.jsonl`; the directory slug is not relied on."""
    if not is_session_uuid(session_uuid):
        return None
    root = glob.escape(os.path.join(os.path.expanduser('~'), '.claude', 'projects'))
    matches = glob.glob(os.path.join(root, '*', f'{glob.escape(session_uuid)}.jsonl'))
    try:
        return max(matches, key=os.path.getmtime) if matches else None
    except OSError:
        # A candidate can disappear between discovery and stat; retry on a later poll.
        return None


class ClaudeDriver(Driver):
    """Drives Claude Code background sessions (captured shapes in the module docstring).

    `mechanism` selects how `submit()` delivers: `attach` types into a live session through
    `claude attach <id>` under a PTY (the only captured live-delivery path), `resume` continues a
    *stopped* session with `claude --bg --resume <sessionId> '<msg>'` and no other flags (the
    captured restarted path). No spend bound is established: `--max-budget-usd` is a `--print`
    option and `--print` conflicts with `--bg`, and the `--model haiku` passed at creation was
    not honoured (captured: the session's assistant records name `claude-sonnet-5`, and the
    no-flag resume reply `claude-opus-5`). A matrix cell must therefore cite `TrialRun.model`,
    read from the transcript's assistant records, never this argument. The session runs under
    the operator's default permission mode, since combining `--bg` with
    `--permission-mode`/`--disallowedTools` is uncaptured -- a wider authority surface than the
    Codex cells' `-s read-only -a never`.
    """

    NAMESPACE = 'claude'

    def __init__(self, registry, *, run=subprocess.run, cwd, model='haiku', mechanism='attach',
                 transcript_path_for=default_claude_transcript_path, pty=PtyClient, sleep=time.sleep,
                 monotonic=time.monotonic):
        if mechanism not in CLAUDE_MECHANISMS:
            raise ValueError(f'unknown submission mechanism: {mechanism!r}')
        super().__init__(registry, cwd=cwd)
        self.run = run
        self.model = model
        self.mechanism = mechanism
        self.transcript_path_for = transcript_path_for
        self.pty = pty
        self.sleep = sleep
        # The bounded local waits use this, injected with `sleep` so a test's fake sleep advances
        # the same clock the deadline reads (a real monotonic with a no-op sleep hot-spins).
        self.monotonic = monotonic
        self.sessions = {}  # short id -> full sessionId (None until the listing supplied it)

    def _listing(self):
        result = self.run(['claude', 'agents', '--json', '--all', '--cwd', self.cwd],
                          capture_output=True, text=True, timeout=15)
        if result.returncode != 0:
            raise RuntimeError(f'claude agents exited {result.returncode}: {result.stderr}')
        try:
            entries = json.loads(result.stdout)
        except ValueError as error:
            raise RuntimeError(f'claude agents --json produced unparseable output: {error}') from error
        if not isinstance(entries, list):
            raise RuntimeError(f'claude agents --json produced a non-list top level: {result.stdout!r}')
        seen = set()
        for entry in entries:
            if (not isinstance(entry, dict) or 'pid' not in entry or
                    any(not isinstance(entry.get(key), str) or not entry[key]
                        for key in ('id', 'kind', 'sessionId', 'state')) or
                    re.fullmatch(r'[0-9a-f]{8}', entry['id']) is None or
                    not is_session_uuid(entry['sessionId']) or
                    entry['sessionId'][:8] != entry['id'] or
                    entry['kind'] != 'background' or entry['id'] in seen or
                    (entry['pid'] is not None and
                     (type(entry['pid']) is not int or entry['pid'] <= 0))):
                raise RuntimeError('claude agents --json produced a malformed listing entry')
            seen.add(entry['id'])
        return entries

    def status(self, session_id):
        """The `claude agents --json --all --cwd <cwd>` entry for an owned id, or None if absent.

        Captured fields: `pid` (None once stopped), `status` (`idle`/`busy`, None once stopped),
        `state` (`working`/`done`), `sessionId`. Settle callbacks use it to establish or check a
        precondition; what an approval-parked session shows, and how reliable `busy` is, are
        uncaptured. Only this driver's own `--cwd`-filtered listing is ever read.
        """
        self.require_owned(session_id)
        for entry in self._listing():
            if entry.get('id') == session_id and entry.get('kind') == 'background':
                previous = self.sessions.get(session_id)
                if previous is not None and previous != entry['sessionId']:
                    raise RuntimeError('claude listing changed the full UUID for an owned short ID')
                self.sessions[session_id] = entry['sessionId']
                return entry
        return None

    def _mint(self, session_id):
        self.mint(session_id)
        self.sessions.setdefault(session_id, None)

    def _settle_creation_turn(self, session_id, timeout=CLAUDE_CREATION_TURN_CAP):
        """Wait until the listing reports the creation turn finished; return its entry.

        `claude --bg` returns while that turn is still working: the captured listing taken
        immediately afterwards reads `state: "working"`, and the creation prompt's assistant
        record landed 12 s later (docs/host-probe-preflight.md, 2026-09-13). Returning then would
        let the creation reply arrive *after* `run_trial` stamps `submitted_at`, and
        `detect_outcomes` would count it as this trial's `turn_start` and serving model without
        the marker ever having been sent. `state` is the boundary the host itself publishes
        (captured `working` → `done`, with `pid` still set and the session alive); `status` read
        `idle` throughout that same turn and is not it.

        Every trial state settles here rather than in a settle callback: a busy trial's long turn,
        a restarted trial's `stop` and an idle trial's submission all have to start from a session
        whose creation turn is over. A turn still running at the cap raises -- the session stays
        owned, so the sweep removes it.
        """
        deadline = self.monotonic() + timeout
        while True:
            entry = self.status(session_id)
            if entry is None:
                raise RuntimeError(f'claude --bg printed {session_id} but the listing under '
                                   f'{self.cwd} lacks it')
            if entry.get('state') == 'done':
                return entry
            if self.monotonic() >= deadline:
                raise RuntimeError(f'the creation turn of {session_id} was still '
                                   f'{entry.get("state")!r} after {timeout}s')
            self.sleep(1.0)

    def create(self, prompt):
        """`claude --bg --model <m> '<prompt>'`; the id comes from stdout line 1, then the listing.

        The short id is minted *before* the listing runs, so a listing failure afterwards still
        leaves the session in `owned()` for the sweep. A timeout after the daemon already printed
        its id mints from the partial output before re-raising. A Ctrl-C mid-command loses that
        output: one bounded cwd-filtered listing reports visible candidates without adopting
        them. Failed discovery and sessions appearing after that snapshot require manual work.

        Returns only once the creation turn has finished (`_settle_creation_turn`), so a caller
        that submits immediately is not observing that turn's reply.
        """
        argv = ['claude', '--bg', '--model', self.model, prompt]
        try:
            result = self.run(argv, capture_output=True, text=True, timeout=60, cwd=self.cwd,
                              stdin=subprocess.DEVNULL)
        except subprocess.TimeoutExpired as error:
            short = backgrounded_id(_partial_stdout(error))
            if short:
                self._mint(short)
            raise
        except KeyboardInterrupt as error:
            try:
                candidates = {entry['id'] for entry in self._listing()} - self.owned()
            except BaseException:
                error.add_note('Claude creation interrupted; bounded candidate discovery failed; '
                               'manual investigation required in the probe cwd')
            else:
                self.strays.update(candidates)
                error.add_note(f'Claude creation interrupted; candidate IDs {sorted(candidates)!r}; '
                               'no ownership granted, manual investigation required; '
                               'the bounded snapshot cannot exclude later arrivals')
            raise
        short = backgrounded_id(result.stdout)
        if short:
            self._mint(short)  # before the exit check: a nonzero exit after the line is still a session
        if result.returncode != 0:
            raise RuntimeError(f'claude --bg exited {result.returncode}: {result.stderr}')
        if short is None:
            raise RuntimeError(f'claude --bg printed no backgrounded line: {result.stdout!r}')
        self._settle_creation_turn(short)
        return short

    def _session_uuid(self, session_id):
        if self.sessions.get(session_id) is None:
            self.status(session_id)
        return self.sessions.get(session_id)

    def _transcript_path(self, session_id):
        session_uuid = self._session_uuid(session_id)
        return None if session_uuid is None else self.transcript_path_for(session_uuid)

    def _read_transcript(self, session_id, parse):
        path = self._transcript_path(session_id)
        if path is None:
            return None
        try:
            with open(path, encoding='utf-8') as handle:
                lines = handle.readlines()
        except (OSError, UnicodeDecodeError):
            return None
        found = False
        for line in lines:
            try:
                record = json.loads(line)
            except ValueError:
                continue  # The event parser still rejects malformed records.
            if not isinstance(record, dict) or record.get('type') not in ('user', 'assistant'):
                continue
            if record.get('sessionId') != self.sessions[session_id] or record.get('cwd') != self.cwd:
                return None
            found = True
        return parse(lines) if found else None

    def observe(self, session_id, *, marker, submitted_at):
        """Read the session's JSONL transcript; unreadable or undatable content is unobservable.

        `turn_stream` stays False: the captured transcript carries no turn-boundary record, so a
        busy trial's turn_start cannot be told apart from the running turn's tail. A fresh-marker
        acknowledgement remains independent evidence of receipt.
        """
        self.require_owned(session_id)
        parsed = self._read_transcript(session_id, claude_transcript_events)
        if parsed is None:
            return Observation(observable=False)
        events, unusable = parsed
        observation = detect_outcomes(events, marker, submitted_at=submitted_at)
        if unusable:
            observation.observable = False
        return observation

    def close_clients(self):
        """Send the captured detach key to every held attach client, then close as the base does.

        The captured attach detached with Ctrl-Z *after* the reply was on screen and the client
        exited 0 with the session still listed. Holding the client until cleanup reproduces that;
        an exited client is skipped (`close()` below reaps it) and the kill remains the backstop.

        A write that fails for any other reason (`send_keys` propagates `OSError` straight from
        `os.write`) is collected as a cleanup failure and the base close still runs. Letting it
        escape would abort `sweep()` before any client was closed or any teardown reported, mask
        whatever exception the trial was already unwinding, and leave an authenticated session
        live with nothing naming it. A collected failure also stops the sweep's teardowns, like a
        failed close: the detach is the captured way to leave the session usable, and one that
        did not happen is not evidence the session is safe to delete.
        """
        failures = []
        detached = False
        for client in self.clients:
            try:
                client.send_keys(b'\x1a')
            except ValueError:
                continue  # already exited; nothing to detach from
            except Exception as error:
                failures.append(('client', error))
                continue
            detached = True
        if detached:
            self.sleep(1.0)  # let the captured exit-0 detach complete before the kill backstop
        return failures + super().close_clients()

    def submit(self, session_id, message):
        self.require_owned(session_id)
        self.submission_note = None
        try:
            entry = self.status(session_id)
        except subprocess.TimeoutExpired as error:
            # Raised before anything was sent: letting it reach `run_trial` would read as a
            # submission that may have delivered and poll a marker the host never received.
            raise SubmissionUncaptured('claude agents timed out before submission; nothing sent') from error
        if entry is None:
            raise RuntimeError(f'session not listed under {self.cwd}: {session_id}')
        if self.mechanism == 'resume':
            return self._submit_resume(session_id, entry, message)
        return self._submit_attach(session_id, entry, message)

    def _submit_resume(self, session_id, entry, message):
        """`claude --bg --resume <sessionId> '<msg>'`, no other flags, against a stopped session.

        Captured: with no flags the same id continues and the original transcript grows; with any
        flag, or while the session is running with flags, a *copy* starts under a new id. A
        no-flag resume against a running session is uncaptured, so a listed `pid` refuses as
        `SubmissionUncaptured`. A copy is still a live session this runner started: it is minted
        (so the sweep removes it) and reported as a command-level rejection.

        A copy named on stdout settles the question even when the command then times out: the
        message went to the copy, so the trial's own session will never show it. Re-raising the
        timeout would send `run_trial` down its may-have-delivered path, polling the original and
        turning its absent marker into `not_observed` -- negative evidence about a host that was
        never asked. That case is `SubmissionUncaptured`, with the copy minted for the sweep.
        """
        if entry.get('pid') is not None:
            raise SubmissionUncaptured(
                f'--bg --resume against a running session is uncaptured (pid {entry["pid"]}); '
                'stop it first')
        session_uuid = self._session_uuid(session_id)
        if session_uuid is None:
            raise RuntimeError(f'listing carries no sessionId for {session_id}')
        argv = ['claude', '--bg', '--resume', session_uuid, message]
        try:
            result = self.run(argv, capture_output=True, text=True, timeout=60, cwd=self.cwd,
                              stdin=subprocess.DEVNULL)
        except subprocess.TimeoutExpired as error:
            started = backgrounded_id(_partial_stdout(error))
            if started and started != session_id:
                self._mint(started)
                raise SubmissionUncaptured(
                    f'--bg --resume timed out after starting a copy {started}: the message went '
                    f'to that copy, not to {session_id}') from error
            raise
        started = backgrounded_id(result.stdout)
        if started and started != session_id:
            self._mint(started)  # a copy named on stdout is live whatever the exit status says
        if result.returncode != 0:
            if started == session_id:
                self.submission_note = (f'--bg --resume backgrounded the original session but '
                                        f'exited {result.returncode}; acceptance is ambiguous, '
                                        'continuing transcript observation')
                return None
            raise SubmissionRejected(result.returncode, result.stderr)
        if started is None:
            raise SubmissionUncaptured(f'--bg --resume exited 0 without a backgrounded line: '
                                       f'{result.stdout!r} / {result.stderr!r}')
        if started != session_id:
            raise SubmissionRejected(0, f'started a copy {started} instead of continuing '
                                        f'{session_id}: {result.stderr}')
        return True

    def attach(self, session_id):
        """Hold a ready client before establishing a busy or approval trial's precondition.

        Only a successful initial idle-composer check makes a client reusable. A held unready
        or exited client still belongs to cleanup, but cannot establish submission readiness.
        """
        self.require_owned(session_id)
        entry = self.status(session_id)
        if entry is None or entry.get('pid') is None or entry.get('state') != 'done':
            raise SubmissionUncaptured('initial attach requires a live idle session; '
                                       'mid-turn attach readiness is uncaptured')
        client = self.open_client(['claude', 'attach', session_id])
        if not client.wait_for(CLAUDE_READY_PATTERN, quiet=3.0, timeout=30):
            raise SubmissionUncaptured(f'attach never showed the composer: {client.screen()!r}')
        client.serves = session_id
        return client

    def _submit_attach(self, session_id, entry, message):
        """Type into `claude attach <id>` under a PTY and keep the client attached afterwards.

        Returns None: a PTY write is never host acceptance (docs/host-probes.md, Trial protocol),
        so `accepted` is unobservable and polling proceeds. A composer that never appears is
        `SubmissionUncaptured` with the screen in the message -- evidence about this client, not
        a host rejection. Attaching to a stopped session is uncaptured and refused the same way.
        Windows for this mechanism include the client's startup (captured 3.2s to the prompt),
        since `submitted_at` is stamped when `submit()` is called.

        The client stays on `clients` through the whole observation and is detached by
        `close_clients()` during the sweep. The capture only ever detached after the reply was
        displayed (user record at +0.27 s, assistant reply at +2.6 s); detaching as soon as the
        user record lands would make every trial run under an uncaptured mid-turn detach, so a
        missing turn_start or ack could not be attributed to the host.
        """
        if entry.get('pid') is None:
            raise SubmissionUncaptured('attach to a stopped session is uncaptured; use mechanism=resume')
        client = self.live_client_for(session_id)
        if client is None:
            client = self.attach(session_id)
        screen_at_type = client.screen(400)
        typed_at = _utc_now()
        try:
            client.type_line(message)
        except (ValueError, OSError) as error:
            # The client exited between readiness and typing: `ValueError` if the drain thread had
            # already seen EOF, `OSError` straight from `os.write` if the child died first and it
            # had not. Both mean some, all or none of the line reached the composer with no Enter
            # guaranteed; only the first was classified, so a write that failed at the fd escaped
            # as a raw OSError instead of an uncaptured submission.
            raise SubmissionUncaptured(f'attach client could not be typed into ({error!r}): '
                                       f'{client.screen()!r}') from error
        self.submission_note = (f'attach: typed at {typed_at}; client held attached until cleanup; '
                                f'screen at type time: {screen_at_type!r}')
        return None

    def stop(self, session_id):
        """`claude stop <id>` for an owned id, confirmed through the listing; no `rm`, no release.

        The settle step of the restarted cell (captured: exit 0, `stopped <id>`, then the listing
        shows `pid: null, status: null, state: "done"`), and the first half of `teardown`. The
        listing, not the exit status, decides: a nonzero exit against an already-stopped session
        is tolerated, a surviving `pid` is an error whatever the exit status said. Returns the
        listing entry afterwards (None once removed).
        """
        self.require_owned(session_id)
        result = self.run(['claude', 'stop', session_id], capture_output=True, text=True, timeout=15)
        entry = self.status(session_id)
        if entry is not None and entry.get('pid') is not None:
            raise RuntimeError(f'claude stop exited {result.returncode} and left {session_id} running '
                               f'(pid {entry["pid"]}): {result.stderr}')
        return entry

    def teardown(self, session_id):
        """`stop()` then `claude rm <id>`; release only after a confirmed removal.

        Every captured `rm` followed a successful `stop`; `rm` against a running daemon is
        uncaptured, so `stop()`'s listing check must pass before `rm` runs -- otherwise
        ownership is retained and the live daemon reported.
        """
        self.stop(session_id)
        result = self.run(['claude', 'rm', session_id], capture_output=True, text=True, timeout=15)
        if result.returncode != 0:
            raise RuntimeError(f'claude rm exited {result.returncode} for {session_id}: {result.stderr}')
        self.release(session_id)
        self.sessions.pop(session_id, None)

    def version(self, session_id):
        """The session's own transcript-recorded version, or None (see `claude_session_version`).

        Called right after `create()`; the transcript appears within a fraction of a second of
        the listing's `startedAt` (captured), so a short bounded wait covers the race.
        """
        self.require_owned(session_id)
        deadline = self.monotonic() + 5.0
        while True:
            version = self._read_transcript(session_id, claude_session_version)
            if version is not None or self.monotonic() >= deadline:
                return version
            self.sleep(0.5)


# --- Codex: `codex exec --json`, `codex queue`, `codex resume` under a PTY, the rollout ---------

# Composer placeholder of the TUI (captured: `› Ask Codex to do anything`), drawn early and kept
# on screen while a turn or the first-run trust dialog is in progress -- hence the quiet gate.
# The TUI positions words with cursor moves rather than spaces in places, so once escapes are
# stripped the trust dialog reads `Doyoutrustthecontentsofthisdirectory?` (captured); both
# patterns therefore tolerate absent whitespace.
CODEX_READY_PATTERN = r'Ask\s*Codex\s*to\s*do\s*anything'
CODEX_TRUST_PATTERN = re.compile(r'Do\s*you\s*trust\s*the\s*contents\s*of\s*this\s*directory')
CODEX_MECHANISMS = ('queue', 'queue-then-resume')


def codex_thread_ids(stdout):
    """Distinct `thread.started.thread_id` values and unusable-record count."""
    thread_ids = {}
    unusable = 0
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except ValueError:
            unusable += 1
            continue
        if not isinstance(record, dict):
            unusable += 1
        elif record.get('type') == 'thread.started':
            if isinstance(record.get('thread_id'), str) and record['thread_id']:
                thread_ids[record['thread_id']] = None
            else:
                unusable += 1
    return thread_ids, unusable


def codex_session_version(lines):
    """The rollout's own recorded `session_meta.payload.cli_version`, or None.

    Best-effort evidence enrichment: an unparseable or malformed line is skipped. The first
    `session_meta` is the creating `codex exec`'s; a later `resume` appends its own records to
    the same file, so `TrialRun.version` names the creator, not necessarily the process that
    served the marker.
    """
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if not isinstance(record, dict) or record.get('type') != 'session_meta':
            continue
        payload = record.get('payload')
        version = payload.get('cli_version') if isinstance(payload, dict) else None
        return version if isinstance(version, str) else None
    return None


# Codex's own turn-boundary signals, captured in real rollouts. They carry no text and are emitted
# by the host, not a speaker, so they become pseudo-role Events: the only evidence that separates
# a *new* turn from the one already running.
TURN_BOUNDARY_ROLES = {'task_started': 'turn_start',
                       'task_complete': 'turn_end',
                       'turn_aborted': 'turn_end'}

# The two outer record kinds this runner reads at all; a rollout also accumulates session_meta/
# world_state/turn_context/token_usage_record records this runner has no use for.
CODEX_MESSAGE_RECORD_TYPE = 'response_item'
CODEX_EVENT_RECORD_TYPE = 'event_msg'

# A message's content parts carry a role-appropriate `type`: `input_text` for what the user sent,
# `output_text` for what the assistant said. A mismatch is an uncaptured shape, not a synonym.
ROLE_CONTENT_PART_TYPE = {'user': 'input_text', 'assistant': 'output_text'}


def codex_rollout_events(lines):
    """Extract message and turn-boundary Events from rollout JSONL; returns `(events, unusable)`.

    Skips `developer`-role entries (fixed instructions, not conversation turns) and any outer
    record type this runner has no use for; those are not failed reads. The outer `type` gates
    before the inner `payload.type` is trusted, so an unrelated record whose payload happens to
    carry `type: "message"` cannot masquerade as transcript evidence.

    `unusable` counts content that *should* have been readable and was not: a line that is not
    JSON, a non-object record, a missing/non-string outer or payload `type`, a relevant record
    with a non-object payload, an unknown message role, a missing/malformed timestamp, or content
    that is not the list of role-typed text parts every captured shape carries. None of those can
    be ordered against submission; the caller reports the read unobservable rather than letting
    absent outcomes become negative evidence.

    Every `Event.model` stays None: no captured rollout record names the model serving a turn, so
    a Codex cell cites no model rather than an assumed one.
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
            unusable += 1
            continue
        outer_kind = record.get('type')
        if not isinstance(outer_kind, str):
            unusable += 1
            continue
        if outer_kind not in (CODEX_MESSAGE_RECORD_TYPE, CODEX_EVENT_RECORD_TYPE):
            continue
        payload = record.get('payload')
        if not isinstance(payload, dict):
            unusable += 1
            continue
        kind = payload.get('type')
        if not isinstance(kind, str):
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
            unusable += 1
            continue
        when = record_time(record)
        if when is None:
            unusable += 1
            continue
        content = payload.get('content')
        if not isinstance(content, list):
            unusable += 1
            continue
        texts = []
        for part in content:
            part_text = part.get('text') if isinstance(part, dict) else None
            if not isinstance(part_text, str) or part.get('type') != ROLE_CONTENT_PART_TYPE[role]:
                texts = None
                break
            texts.append(part_text)
        if texts is None:
            unusable += 1
            continue
        events.append(Event(role=role, text=''.join(texts), time=when))
    return events, unusable


def default_codex_rollout_path(thread_id):
    """Newest `$CODEX_HOME/sessions/*/*/*/rollout-*-<thread_id>.jsonl` (`~/.codex` by default)."""
    if not is_session_uuid(thread_id):
        return None
    home = os.environ.get('CODEX_HOME') or os.path.join(os.path.expanduser('~'), '.codex')
    root = glob.escape(os.path.join(home, 'sessions'))
    matches = glob.glob(os.path.join(root, '*', '*', '*', f'rollout-*-{glob.escape(thread_id)}.jsonl'))
    try:
        return max(matches, key=os.path.getmtime) if matches else None
    except OSError:
        # Discovery and stat are separate reads. A vanished/replaced candidate makes this
        # poll unreadable; the next poll can discover again without aborting the whole trial.
        return None


class CodexDriver(Driver):
    """Drives Codex CLI threads (captured shapes in the module docstring).

    `mechanism` `queue` delivers with `codex queue` alone and relies on a process already serving
    the thread (a settle callback's `attach()`, opened before submission because a resume drains
    the queue at start). `queue-then-resume` queues first and then opens the resume client inside
    `submit()` -- the restarted cell, where nothing serves the thread until the resume does. The
    client stays open on `clients` and is closed by the sweep before `codex delete`.
    """

    NAMESPACE = 'codex'

    def __init__(self, registry, *, run=subprocess.run, cwd, model=None, mechanism='queue',
                 rollout_path_for=default_codex_rollout_path, pty=PtyClient, clock=time.time):
        if mechanism not in CODEX_MECHANISMS:
            raise ValueError(f'unknown submission mechanism: {mechanism!r}')
        if model is not None:
            raise ValueError('Codex model overrides are uncaptured; only the default is supported')
        super().__init__(registry, cwd=cwd)
        self.run = run
        self.mechanism = mechanism
        self.rollout_path_for = rollout_path_for
        self.pty = pty
        self.clock = clock
        self.queue_clients = {}

    def _created_thread(self, stdout):
        thread_ids, unusable = codex_thread_ids(stdout)
        if any(not is_session_uuid(thread_id) for thread_id in thread_ids):
            self.strays.update(thread_ids)
            raise RuntimeError('Codex creation named an invalid UUID; no ownership granted')
        if len(thread_ids) > 1:
            self.strays.update(thread_ids)
            raise RuntimeError(f'ambiguous Codex creation IDs {list(thread_ids)!r}; '
                               'manual investigation required, no ownership granted')
        thread_id = next(iter(thread_ids), None)
        if thread_id:
            self.mint(thread_id)
        if unusable:
            raise RuntimeError('codex creation stream contains malformed event records')
        return thread_id

    def create(self, prompt):
        """`codex exec --json -s read-only --skip-git-repo-check -C <cwd> '<prompt>'`.

        A unique `thread.started` id is minted before the exit status is checked, including
        partial timeout output. Multiple distinct IDs grant no ownership and every candidate
        is reported for investigation. `exec` returns after its turn: the thread is idle with no
        live process. Passing
        stdin as `/dev/null` is inferred from the captured stderr `Reading additional input from
        stdin...`. `-m` is uncaptured (the default model was used), so construction rejects
        any explicit model override before a host can be launched.
        """
        argv = ['codex', 'exec', '--json', '-s', 'read-only', '--skip-git-repo-check', '-C', self.cwd]
        argv.append(prompt)
        try:
            result = self.run(argv, capture_output=True, text=True, timeout=300, stdin=subprocess.DEVNULL)
        except subprocess.TimeoutExpired as error:
            self._created_thread(_partial_stdout(error))
            raise
        thread_id = self._created_thread(result.stdout)
        if result.returncode != 0:
            raise RuntimeError(f'codex exec exited {result.returncode}: {result.stderr}')
        if thread_id is None:
            raise RuntimeError(f'codex exec printed no thread.started event: {result.stdout!r}')
        return thread_id

    def attach(self, thread_id, *, sandbox='read-only', approval='never'):
        """Open `codex ... resume <thread_id>` under a PTY and wait for its composer.

        Answers only the captured first-run trust dialog (Enter, which persists a trust entry for
        `cwd` in `$CODEX_HOME/config.toml` -- a documented side effect); anything else that keeps
        the composer from settling raises `PtyNotReady` with the screen. The default flags cannot
        produce an approval prompt, so an approval settle must ask for other ones. Any items
        already queued are delivered at start (captured), before the composer is ready.
        """
        self.require_owned(thread_id)
        argv = ['codex', '--no-alt-screen', '-s', sandbox, '-a', approval, '-C', self.cwd,
                'resume', thread_id]
        client = self.open_client(argv)
        # quiet=3.0 is the capture's own readiness criterion (3 s of output silence).
        ready = client.wait_for(CODEX_READY_PATTERN, quiet=3.0, timeout=60)
        reason = 'never became ready'
        if ready and CODEX_TRUST_PATTERN.search(client.text_since(0)):
            since = client.mark()
            try:
                client.send_keys(b'\r')
            except (ValueError, OSError) as error:
                # The client exited on the dialog: `ValueError` if the drain thread had already
                # seen EOF, `OSError` straight from `os.write` if it had not. Either way nothing
                # answered the prompt, which is this client failing to become ready -- the caller
                # in `submit()` turns that into `SubmissionUncaptured`, where a raw OSError would
                # have escaped as an unclassified failure.
                ready = False
                reason = f'exited while its trust dialog was being answered ({error!r})'
            else:
                ready = client.wait_for(CODEX_READY_PATTERN, quiet=3.0, timeout=60, since=since)
        if not ready:
            screen = client.screen()
            client.close()
            self.clients.remove(client)
            raise PtyNotReady(f'codex resume {thread_id} {reason}', screen=screen)
        client.serves = thread_id
        return client

    def submit(self, thread_id, message):
        """`codex queue --thread <id> --message <text>`, then the resume client if this is the
        restarted cell.

        Under `queue` a resume client must already be open on `clients` (the settle callback's
        `attach()`): captured, a queued item is delivered only by a process serving the thread,
        so queueing with none open would produce a guaranteed `not_observed` that says nothing
        about the host -- refused as `SubmissionUncaptured` before anything is queued. Under
        `queue-then-resume` a `codex queue` that times out is also uncaptured: whether the item
        was queued is unknown and nothing serves the thread yet.
        """
        self.require_owned(thread_id)
        self.submission_note = None
        if self.mechanism == 'queue':
            client = self.live_client_for(thread_id)
            if client is None:
                raise SubmissionUncaptured('mechanism=queue needs a live resume client already serving the '
                                           'thread (the settle callback opens one with attach()); nothing queued')
            self.queue_clients[thread_id] = client
        try:
            result = self.run(['codex', 'queue', '--thread', thread_id, '--message', message],
                              capture_output=True, text=True, timeout=15)
        except subprocess.TimeoutExpired as error:
            if self.mechanism == 'queue-then-resume':
                raise SubmissionUncaptured('codex queue timed out before the resume client was opened; '
                                           'whether the item was queued is unknown and nothing serves '
                                           'the thread') from error
            raise
        finally:
            if self.mechanism == 'queue' and not self._queue_client_live(thread_id):
                self.submission_note = 'the exact serving client was lost during queue submission'
        if result.returncode != 0:
            raise SubmissionRejected(result.returncode, result.stderr)
        accepted_at = self.clock()
        if self.mechanism == 'queue-then-resume':
            self.queue_clients[thread_id] = None
            try:
                self.queue_clients[thread_id] = self.attach(thread_id)
            except PtyNotReady as error:
                self.submission_note = f'queued, but the resume client never became ready: {error}'
            # The host accepted when `codex queue` exited, not after the resume client's startup
            # (captured: 15s with items queued); `run_trial` takes a number as the acceptance time.
            return accepted_at
        return True

    def _queue_client_live(self, thread_id):
        client = self.queue_clients[thread_id]
        return (client is not None and any(held is client for held in self.clients) and not client.eof
                and getattr(client, 'serves', None) == thread_id)

    def observe(self, thread_id, *, marker, submitted_at):
        """Read the thread's rollout; an unreadable or undatable rollout is unobservable."""
        self.require_owned(thread_id)
        lines = self._read_rollout(thread_id)
        if lines is None:
            return Observation(observable=False)
        events, unusable = codex_rollout_events(lines)
        observation = detect_outcomes(events, marker, submitted_at=submitted_at, turn_stream=True)
        if unusable or (thread_id in self.queue_clients and not self._queue_client_live(thread_id)):
            observation.observable = False
        return observation

    def teardown(self, thread_id):
        """`codex delete --force <thread_id>`; release only on exit 0.

        Captured against a thread with no live process; the sweep closes any resume client
        first. What `delete` does to a still-queued item is uncaptured.
        """
        self.require_owned(thread_id)
        result = self.run(['codex', 'delete', '--force', thread_id], capture_output=True, text=True,
                          timeout=30)
        if result.returncode != 0:
            raise RuntimeError(f'codex delete exited {result.returncode} for {thread_id}: {result.stderr}')
        self.release(thread_id)

    def version(self, thread_id):
        """The thread's own rollout-recorded `cli_version`, or None; never `codex --version`.

        The installed binary and a rollout's own record disagreed on the same day once
        (docs/host-probe-preflight.md, 2026-09-11), so the client's version is not a fallback.
        """
        self.require_owned(thread_id)
        lines = self._read_rollout(thread_id)
        return codex_session_version(lines) if lines is not None else None

    def _read_rollout(self, thread_id):
        """Bind the complete snapshot to its owned thread and cwd before extracting evidence."""
        self.require_owned(thread_id)
        path = self.rollout_path_for(thread_id)
        if path is None:
            return None
        try:
            with open(path, encoding='utf-8') as handle:
                lines = handle.readlines()
        except (OSError, UnicodeDecodeError):
            return None
        found = False
        for line in lines:
            try:
                record = json.loads(line)
            except ValueError:
                continue  # The event parser marks malformed lines unobservable.
            if not isinstance(record, dict) or record.get('type') != 'session_meta':
                continue
            payload = record.get('payload')
            if (not isinstance(payload, dict) or payload.get('id') != thread_id
                    or payload.get('session_id') != thread_id or payload.get('cwd') != self.cwd):
                return None
            found = True
        return lines if found else None


# --- OpenCode: `opencode run --pure --format json`, `serve` + `run --attach`, `export` -----------

OPENCODE_FREE_MODEL = 'opencode/ling-3.0-flash-fin-free'  # captured: cost 0, no credential needed


def opencode_session_ids(stdout):
    """Every distinct session ID, the first error event, and malformed-line count."""
    session_ids = {}
    error = None
    unusable = 0
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except ValueError:
            unusable += 1
            continue
        if not isinstance(event, dict):
            unusable += 1
            continue
        if isinstance(event.get('sessionID'), str) and event['sessionID']:
            session_ids[event['sessionID']] = None
        if error is None and event.get('type') == 'error':
            error = json.dumps(event.get('error'))
    return session_ids, error, unusable


def opencode_export_events(raw):
    """Extract user/assistant Events from `opencode --pure export` JSON; returns `(events, unusable)`.

    Captured shape: `{"info": {...}, "messages": [{"info": {"role": ..., "time": {"created":
    <ms epoch>, ...}, ...}, "parts": [{"type": "text", "text": ...}, ...]}]}`. `time.created`
    is used for both roles (the assistant's `completed` also exists) so turn_start is the
    earliest assistant activity, as on the other hosts; only `text` parts contribute text.
    Unparseable output, a non-object top level, a non-list `messages`, a non-object message/info,
    a non-string role, a non-numeric creation time or a malformed part all count as unusable.
    An assistant message's `providerID`/`modelID` become `Event.model` (see
    `opencode_message_model`), best-effort and never a reason to fail the read.
    """
    try:
        document = json.loads(raw)
    except ValueError:
        return [], 1
    if not isinstance(document, dict) or not isinstance(document.get('messages'), list):
        return [], 1
    events = []
    unusable = 0
    for message in document['messages']:
        info = message.get('info') if isinstance(message, dict) else None
        if not isinstance(info, dict):
            unusable += 1
            continue
        role = info.get('role')
        created = info.get('time', {}).get('created') if isinstance(info.get('time'), dict) else None
        if role not in ('user', 'assistant') or not isinstance(created, (int, float)) \
                or isinstance(created, bool) or not math.isfinite(created):
            unusable += 1
            continue
        parts = message.get('parts')
        if not isinstance(parts, list):
            unusable += 1
            continue
        texts = []
        for part in parts:
            if (not isinstance(part, dict) or
                    part.get('type') not in ('text', 'step-start', 'reasoning', 'step-finish')):
                texts = None
                break
            if part.get('type') != 'text':
                continue
            if not isinstance(part.get('text'), str):
                texts = None
                break
            texts.append(part['text'])
        if texts is None:
            unusable += 1
            continue
        events.append(Event(role=role, text=''.join(texts), time=created / 1000.0,
                            model=opencode_message_model(info) if role == 'assistant' else None))
    return events, unusable


def opencode_message_model(info):
    """`<providerID>/<modelID>` from an assistant message's `info`, or None.

    Captured on every assistant message of an export; the provider qualifies the id the same way
    `-m` takes it. Best-effort like every `Event.model`: either field absent or non-string reads
    as None, and a `modelID` without a provider is reported bare rather than guessed at.
    """
    provider = info.get('providerID')
    model = info.get('modelID')
    if not isinstance(model, str):
        return None
    return f'{provider}/{model}' if isinstance(provider, str) else model


def opencode_export_version(raw):
    """`info.version` from an export document, or None."""
    try:
        document = json.loads(raw)
    except ValueError:
        return None
    info = document.get('info') if isinstance(document, dict) else None
    version = info.get('version') if isinstance(info, dict) else None
    return version if isinstance(version, str) else None


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def http_status(url):
    """HTTP status of a GET, raising `urllib.error.URLError` when nothing answers."""
    try:
        with urllib.request.urlopen(url, timeout=2) as response:
            return response.status
    except urllib.error.HTTPError as error:
        # HTTPError subclasses URLError, but a 401/500 is still a response from a listener.
        error.close()
        return error.code


class ServerOutput:
    """Drain the owned child's pipe, recognizing only its captured complete readiness line."""

    LIMIT = 16384

    def __init__(self, stream, url):
        self.stream = stream
        self.ready = False
        self.failed = False
        self.eof = False
        self.pending = b''
        self.discard = False
        self.expected = f'opencode server listening on {url}'.encode()
        self.closing = threading.Event()
        self.thread = threading.Thread(target=self._drain, daemon=True)

    def start(self):
        os.set_blocking(self.stream.fileno(), False)
        self.thread.start()

    def _drain(self):
        try:
            while not self.closing.is_set():
                readable, _, _ = select.select([self.stream], [], [], 0.1)
                if not readable:
                    continue
                try:
                    data = os.read(self.stream.fileno(), 4096)
                except BlockingIOError:
                    continue
                if not data:
                    self.eof = True
                    return
                parts = data.split(b'\n')
                for index, part in enumerate(parts):
                    if not self.discard:
                        self.pending += part
                        if len(self.pending) > self.LIMIT:
                            self.pending = b''
                            self.discard = True
                    if index < len(parts) - 1:
                        if not self.discard and self.pending == self.expected:
                            self.ready = True
                        self.pending = b''
                        self.discard = False
        except (OSError, ValueError):
            self.failed = True

    def close(self):
        self.closing.set()
        if self.thread.ident is not None:
            self.thread.join(timeout=2)
            if self.thread.is_alive():
                raise RuntimeError('opencode stdout drain did not stop')
        self.stream.close()


class Server:
    """A running `opencode serve` this driver started: its process group and base URL."""

    POLL = 0.1

    def __init__(self, process, url, *, sleep=time.sleep):
        self.process = process
        self.url = url
        self.sleep = sleep
        self.output = None

    def close_output(self):
        if self.output is not None:
            self.output.close()
        elif self.process.stdout is not None:
            self.process.stdout.close()

    def _group_alive(self, pgid):
        """Whether any process is still in the group, the leader reaped first if it has exited.

        Signal 0 is the existence test: it reaches the whole group, so it answers about the
        descendants `serve` may have left behind and not only about the handle we hold. An
        unreaped leader is still a group member, hence the `poll()` -- otherwise a zombie would
        read as a live server forever. Once this reaps the leader, the numeric group ID may
        be reused: close() must only report a surviving group, never signal it again.
        """
        self.process.poll()
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return False
        return True

    def _wait_for_group(self, pgid, grace):
        """Wait up to `grace` for the whole group to go. True once it has."""
        for _ in range(max(1, round(grace / self.POLL))):
            if not self._group_alive(pgid):
                return True
            self.sleep(self.POLL)
        return not self._group_alive(pgid)

    def close(self):
        """SIGTERM the server's own process group, then SIGKILL if it lingers.

        The *group*, not the handle: `serve` is spawned with `start_new_session=True`, so it
        leads a group of its own, and waiting on the Popen alone would call a server stopped
        while a child that outlived it still held the port and still answered on the same URL --
        exactly what the `serve()` precondition refusing an already-answering URL then reads as
        a foreign server. A process still alive after both signals is an error, so the sweep
        reports a server it could not stop instead of forgetting it.
        """
        pgid = self.process.pid
        for signum, grace in ((signal.SIGTERM, 5.0), (signal.SIGKILL, 5.0)):
            if not self._group_alive(pgid):
                self.close_output()
                return
            if self.process.returncode is not None:
                raise RuntimeError(f'opencode serve leader {pgid} was reaped; process-group '
                                   'ownership cannot be verified, manual investigation required')
            # Server lifecycle calls are serialized. No poll/wait occurs between this check
            # and killpg, so even if the leader exits, its unreaped PID reserves the group ID.
            try:
                os.killpg(pgid, signum)
            except ProcessLookupError:
                self.close_output()
                return
            if self._wait_for_group(pgid, grace):
                self.close_output()
                return
        raise RuntimeError(f'opencode serve (process group {pgid}) survived SIGTERM and SIGKILL')


class OpenCodeDriver(Driver):
    """Drives OpenCode sessions (captured shapes in the module docstring).

    Submission needs `serve()` open: `run --attach <url> --session <id>` is the only captured path
    into an existing session. The server listens on loopback without a password (captured
    warning; `OPENCODE_SERVER_PASSWORD` is uncaptured), on a free ephemeral port chosen per
    `serve()` so a leftover server cannot answer in its place.
    """

    NAMESPACE = 'opencode'

    def __init__(self, registry, *, run=subprocess.run, popen=subprocess.Popen, http_get=http_status,
                 cwd, model=OPENCODE_FREE_MODEL, port=None, sleep=time.sleep,
                 output_reader=ServerOutput):
        if model != OPENCODE_FREE_MODEL:
            raise ValueError('OpenCode model override is uncaptured; only the captured free model is supported')
        super().__init__(registry, cwd=cwd)
        self.run = run
        self.popen = popen
        self.http_get = http_get
        self.model = model
        self.port = port
        self.sleep = sleep
        self.output_reader = output_reader
        self.server = None
        self.submission_servers = {}

    def _created_session(self, stdout):
        session_ids, error, unusable = opencode_session_ids(stdout)
        if len(session_ids) > 1:
            self.strays.update(session_ids)
            raise RuntimeError(f'ambiguous OpenCode creation IDs {list(session_ids)!r}; '
                               'manual investigation required, no ownership granted')
        session_id = next(iter(session_ids), None)
        if session_id:
            self.mint(session_id)
        if unusable:
            raise RuntimeError('opencode creation stream contains malformed event lines')
        return session_id, error

    def create(self, prompt):
        """`opencode run --pure --format json --dir <cwd> --title <t> -m <model> '<prompt>'`.

        One consistent `sessionID` is minted before turn errors are checked: a failing turn
        (captured: an `error` event for a stale credential) still creates the session. Multiple
        IDs grant no ownership; malformed lines fail creation after retaining any unique ID
        for cleanup.
        Whether an error event or a nonzero exit takes precedence is uncaptured; both raise. A
        Ctrl-C mid-command loses the output: the session, if any, is then only findable by a
        human as the newest `parley-probe-*` row of the global `opencode --pure session list`,
        removed with `opencode --pure session delete <id>`.
        """
        title = f'parley-probe-{uuid.uuid4().hex[:12]}'
        argv = ['opencode', 'run', '--pure', '--format', 'json', '--dir', self.cwd, '--title', title,
                '-m', self.model, prompt]
        try:
            result = self.run(argv, capture_output=True, text=True, timeout=180, cwd=self.cwd,
                              stdin=subprocess.DEVNULL)
        except subprocess.TimeoutExpired as error:
            self._created_session(_partial_stdout(error))
            raise
        session_id, error = self._created_session(result.stdout)
        if error is not None:
            raise RuntimeError(f'opencode run reported an error event: {error}')
        if result.returncode != 0:
            raise RuntimeError(f'opencode run exited {result.returncode}: {result.stderr}')
        if session_id is None:
            raise RuntimeError(f'opencode run printed no sessionID: {result.stdout!r}')
        return session_id

    def serve(self, timeout=20.0):
        """Start `opencode serve --pure --port <p>` and wait until *our* child answers.

        Readiness requires the child's captured stdout listening line for this URL, followed by
        GET /session returning 200 while the child is still alive; a port that already
        answered before the child started is refused, since the global session store means a
        stranger's server would look identical. The child gets its own session so `close()` can
        signal the whole group -- which also means a terminal Ctrl-C never reaches it, and this
        handle is the only thing that can stop it.

        Ownership is therefore taken in the same statement as the spawn, inside the handler that
        closes it: from that point every exit from the readiness wait -- timeout, an early child
        exit, a Ctrl-C, a failing probe -- closes and disowns the child. `self.server` is set
        before the wait rather than after, so a `close_servers()` racing the wait finds the
        child instead of leaving it running, and it is cleared only once `close()` has actually
        succeeded, exactly as `close_servers()` does: a startup cleanup that could not stop the
        child must leave the handle behind for the sweep to retry and report.
        """
        if self.server is not None:
            if self.server.process.poll() is not None:
                raise RuntimeError(f'the held opencode serve exited {self.server.process.returncode}; '
                                   'close_servers() before serving again')
            if self.server.output is None or not self.server.output.ready or self.server.output.failed:
                raise RuntimeError('the held opencode serve has no verified readiness; close_servers() first')
            return self.server
        port = self.port or free_port()
        url = f'http://127.0.0.1:{port}'
        try:
            self.http_get(f'{url}/session')
        except urllib.error.URLError:
            pass
        else:
            raise RuntimeError(f'{url} already answers; refusing to adopt a server this run did not start')
        deadline = time.monotonic() + timeout
        try:
            self.server = Server(self.popen(['opencode', 'serve', '--pure', '--port', str(port)],
                                            cwd=self.cwd, stdin=subprocess.DEVNULL,
                                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                            start_new_session=True), url, sleep=self.sleep)
            self.server.output = self.output_reader(self.server.process.stdout, url)
            self.server.output.start()
            while True:
                if self.server.process.poll() is not None:
                    raise RuntimeError(f'opencode serve exited {self.server.process.returncode} '
                                       'before answering')
                if self.server.output.failed or (self.server.output.eof and not self.server.output.ready):
                    raise RuntimeError('opencode serve stdout ended or failed before verified readiness')
                if not self.server.output.ready:
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f'opencode serve readiness line absent within {timeout}s')
                    self.sleep(0.25)
                    continue
                try:
                    status = self.http_get(f'{url}/session')
                    if status != 200:
                        raise RuntimeError(f'opencode serve returned HTTP {status}; '
                                           'the captured readiness response is 200')
                    if self.server.process.poll() is not None:
                        raise RuntimeError('opencode serve exited during readiness verification')
                    break
                except (urllib.error.URLError, TimeoutError):
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f'opencode serve did not answer on {url} within {timeout}s') from None
                    self.sleep(0.25)
        except BaseException:
            if self.server is not None:
                self.server.close()  # a failure here keeps the handle for `close_servers()`
                self.server = None
            raise
        return self.server

    def close_servers(self):
        failures = []
        if self.server is not None:
            try:
                self.server.close()
            except Exception as error:
                failures.append(('server', error))
            else:
                self.server = None
        return failures

    def _attach_events(self, stdout, session_id):
        """Check every attach id without treating any as creation evidence.

        It may name a pre-existing human session. Only a human can investigate it; recording
        the id must never grant submit, observe or teardown authority.
        """
        session_ids, error, unusable = opencode_session_ids(stdout)
        unexpected = set(session_ids) - {session_id}
        self.strays.update(unexpected)
        if unexpected:
            raise SubmissionUncaptured(f'run --attach named unexpected sessions {sorted(unexpected)!r}; '
                                       'where the marker landed is uncaptured')
        if unusable:
            raise SubmissionUncaptured('run --attach contains malformed event lines; '
                                       'where the marker landed is uncaptured')
        return next(iter(session_ids), None), error

    def submit(self, session_id, message):
        """`opencode run --pure --format json --attach <url> --session <id>`; exit 0 is acceptance.

        Both the exit status and the event stream decide: a structured `error` event is
        `SubmissionUncaptured` whatever the exit status said, and a nonzero exit against a serve
        child that has died is this runner's failure rather than a host rejection.

        An exit-zero stream must also *name the session it was aimed at*. The captured attach
        emitted a `step_start` event carrying its `sessionID`, so a stream naming a different id
        -- or naming none at all -- is not evidence that this session received the marker: the
        trial would poll the requested session, find no marker and record `not_observed` for a
        host that was never asked. Either shape is `SubmissionUncaptured`, and a different id is
        recorded for manual investigation without granting ownership or deletion authority.

        A timeout is checked the same way, against both the child and the partial event stream.
        `run --attach` hanging until its own timeout is what a `serve` child dying under it looks
        like, and `run_trial` reads a bare `TimeoutExpired` as "may have delivered" -- it would
        poll the export, which is readable independently of the server, and record the absent
        marker as `not_observed` for a host whose submission path had disappeared or had already
        said it failed. A dead child, an error event already printed, or an id already named that
        is not this session all make the timeout `SubmissionUncaptured`, with any stray id recorded
        first. A partial stream naming *nothing* re-raises, since a truncated stream's silence is
        not evidence of misdirection and the message may well have reached the session.
        """
        self.require_owned(session_id)
        self.submission_note = None
        if self.server is None:
            raise SubmissionUncaptured('only `run --attach` into a live `serve` is captured; call serve() first')
        if self.server.process.poll() is not None:
            raise SubmissionUncaptured(f'the serve child exited {self.server.process.returncode} before '
                                       'submission; nothing sent')
        self.submission_servers[session_id] = self.server
        argv = ['opencode', 'run', '--pure', '--format', 'json', '--attach', self.server.url,
                '--session', session_id, '-m', self.model, message]
        try:
            result = self.run(argv, capture_output=True, text=True, timeout=60, cwd=self.cwd,
                              stdin=subprocess.DEVNULL)
        except subprocess.TimeoutExpired as error:
            partial_id, partial_error = self._attach_events(_partial_stdout(error), session_id)
            if self.server.process.poll() is not None:
                raise SubmissionUncaptured(
                    f'run --attach timed out against a serve child that had exited '
                    f'{self.server.process.returncode}') from error
            if partial_error is not None:
                raise SubmissionUncaptured(f'run --attach reported an error event before timing '
                                           f'out: {partial_error}') from error
            raise
        attached_id, event_error = self._attach_events(result.stdout, session_id)
        if event_error is not None:
            # Provider/credential/model errors do not establish whether submission reached
            # the session. Neither exit zero nor a nonzero status resolves that ambiguity.
            raise SubmissionUncaptured(f'run --attach exited {result.returncode} but reported an '
                                       f'error event: {event_error}; stderr: {result.stderr}')
        if result.returncode != 0:
            if self.server.process.poll() is not None:
                # A dead server is this runner's failure, not the host refusing the message.
                raise SubmissionUncaptured(f'run --attach exited {result.returncode} against a serve child '
                                           f'that had exited {self.server.process.returncode}: {result.stderr}')
            raise SubmissionRejected(result.returncode, result.stderr)
        if attached_id != session_id:
            raise SubmissionUncaptured(
                f'run --attach exited 0 but its events named {attached_id!r}, not {session_id}; '
                f'where the marker landed is uncaptured')
        return True

    def _export(self, session_id):
        try:
            result = self.run(['opencode', '--pure', 'export', session_id], capture_output=True,
                              text=True, timeout=30)
        except subprocess.TimeoutExpired:
            return None
        if result.returncode != 0:
            return None
        try:
            document = json.loads(result.stdout)
        except ValueError:
            return None
        if (not isinstance(document, dict) or not isinstance(document.get('info'), dict) or
                document['info'].get('id') != session_id or
                not isinstance(document.get('messages'), list)):
            return None
        seen = set()
        for message in document['messages']:
            info = message.get('info') if isinstance(message, dict) else None
            if (not isinstance(info, dict) or info.get('sessionID') != session_id or
                    not isinstance(info.get('id'), str) or not info['id'] or info['id'] in seen or
                    not isinstance(message.get('parts'), list)):
                return None
            seen.add(info['id'])
            for part in message['parts']:
                if (not isinstance(part, dict) or part.get('sessionID') != session_id or
                        part.get('messageID') != info['id']):
                    return None
        return result.stdout

    def observe(self, session_id, *, marker, submitted_at):
        """Read `opencode --pure export <id>`; a failed or malformed export is unobservable.

        `turn_stream` stays False: the export carries no turn-boundary record. Exporting while
        `serve` is up reads the same store (captured side by side; strict consistency between the
        two is an assumption).
        """
        self.require_owned(session_id)
        raw = self._export(session_id)
        if raw is None:
            return Observation(observable=False)
        events, unusable = opencode_export_events(raw)
        observation = detect_outcomes(events, marker, submitted_at=submitted_at)
        server = self.submission_servers.get(session_id)
        if unusable or (server is not None and (server is not self.server or server.process.poll() is not None)):
            observation.observable = False
        return observation

    def teardown(self, session_id):
        """`opencode --pure session delete <id>`; release only on exit 0."""
        self.require_owned(session_id)
        result = self.run(['opencode', '--pure', 'session', 'delete', session_id], capture_output=True,
                          text=True, timeout=30)
        if result.returncode != 0:
            raise RuntimeError(f'opencode session delete exited {result.returncode} for {session_id}: '
                               f'{result.stderr}')
        self.release(session_id)

    def version(self, session_id):
        """`info.version` from the session's export, or None."""
        self.require_owned(session_id)
        raw = self._export(session_id)
        return None if raw is None else opencode_export_version(raw)


# --- Orchestration ------------------------------------------------------------------------------

@dataclass
class TrialRun:
    """One trial's raw result, in the shape `Trial`/`classify_trial` need.

    A named result rather than a tuple: the fields are exactly what a caller must carry into
    `Trial(submitted=..., state=...)` and `classify_trial(..., supported=, observable=)`, plus the
    evidence a matrix cell must cite alongside them.
    """

    session_id: str
    submitted_at: float
    accepted_at: float | None
    outcomes: dict
    state: str
    supported: dict
    observable: dict
    # The exact token submitted (docs/host-probes.md, Trial protocol: record the synthetic input
    # alongside its evidence).
    marker: str
    # What established each entry in `outcomes`, keyed the same -- see `Observation.signals`.
    signals: dict = field(default_factory=dict)
    # The turn already running at submission, and whether its end could be observed at all.
    turn_end: float | None = None
    turn_end_observable: bool = True
    # The session's own version at trial time (transcript/rollout/export-recorded), or None.
    version: str | None = None
    # The model the host recorded as serving this trial's own turn, or None when the trial saw no
    # assistant message, the host names none (every Codex cell), or the reading could not be
    # attributed to this trial (a busy trial on a host with no turn-boundary stream, whose
    # turn_start is unobservable for the same reason). The requested model is not it:
    # `--model haiku` was captured not being honoured, so a cell whose model is None must say the
    # model is unknown rather than repeat what was asked for. `run_trial_with_cleanup` deletes the
    # session, so this is the caller's only chance to record it.
    model: str | None = None
    # Free-text evidence about the submission: a `SubmissionRejected`'s exit status and stderr, a
    # `SubmissionUncaptured`'s reason and screen, or a driver's own note about a mechanism with no
    # exit status of its own (the attach path: when the line was typed, what was on screen).
    submission_diagnostic: str | None = None
    # True when an operator Ctrl-C ended the polling loop early. Every other field is then
    # fail-closed exactly like an unreadable run; only this tells them apart.
    interrupted: bool = False
    # Set to the observed wall-vs-monotonic divergence, in seconds, when the wall clock was
    # corrected during the trial. Every outcome is then unobservable and every timestamp dropped
    # (`run_trial`); this is what the cell cites instead of them.
    clock_step: float | None = None


def run_trial(driver, *, prompt, marker=None, state='idle', settle=None,
              poll_interval=5.0, clock=time.time, monotonic=time.monotonic, sleep=time.sleep):
    """Create, submit and observe one trial through its windows; returns a `TrialRun`.

    Teardown is not done here: use `run_trial_with_cleanup`, or sweep `driver.owned()` yourself.
    Any failure after `create()` propagates raw -- the registry already names every live session.

    `marker` defaults to a fresh `marker_token()`, so several trials against one host can never
    reuse one by omission and count a delayed echo of an earlier marker as their own ack. A
    caller-supplied value is validated for shape only.

    `settle` establishes the requested `state` (busy/approval/disconnected/restarted) before
    submission, through the driver's own methods (`attach`, `status`, `stop`, `submit`) so every
    session it touches is owned -- a settle that runs `claude --bg --resume <flags>` itself would
    start a copy nothing mints. It receives `session_id`; omitting it for any non-`idle` state raises
    `ValueError` before any session exists.

    `submit()`'s result drives acceptance: `True` is accepted, stamped when `submit` *returns*
    (a slow submission is not backdated into its 10s window); a number is the `clock` reading at
    which the driver itself saw the host accept, for a `submit()` that keeps working after that
    point (Codex `queue-then-resume` opens its client only after `codex queue` exited 0); `False`
    or `SubmissionRejected` is
    a real, observed rejection -- `accepted` stays observable, but no polling happens and the
    transcript outcomes are unobservable, since nothing was delivered; `None`, or a
    `subprocess.TimeoutExpired` from the call, means the message went through a channel with no
    acceptance signal (a PTY write, a command that may have delivered before its timeout), so
    `accepted` alone is unobservable and polling proceeds; `SubmissionUnsupported` classifies
    every outcome `unsupported` (the host lacks the mechanism); `SubmissionUncaptured` classifies
    every outcome `unobservable` (this runner could not vouch for the attempt).

    Observation polls until every transcript outcome is seen or the longest window (120s) has
    elapsed, merging each poll's evidence and keeping the first timestamp per outcome; a single
    snapshot would report `not_observed` for events that arrive inside their window. The final
    poll decides observability: a transcript is cumulative, so a late successful read covers
    earlier gaps, but a failed last read leaves the tail of the window unseen. A `busy` trial
    polls to `BUSY_CAP`, adjusted to the dependent windows once the running turn's end is seen
    (extended past the cap when that end lands close to it); without a turn end, a host with no
    turn-boundary stream gets turn_start and missing ack marked unobservable, while a turn-stream
    host reaches `Trial.result`'s own `inconclusive` via `turn_end_observable`.

    A `KeyboardInterrupt` inside the polling loop finalizes what was gathered with `interrupted`
    set and every still-missing outcome unobservable, rather than discarding minutes of evidence;
    anywhere else it propagates, and the cleanup sweep runs from the caller's `finally`.

    `model` is the first serving model any poll read (`Observation.model`), carried onto the
    `TrialRun` because the session is deleted before a caller could go back for it.

    Compared timestamps (`submitted_at`, `accepted_at`, every `Event.time`) come from `clock`
    (wall time), because host transcripts carry only wall-clock stamps; the local polling
    deadline uses `monotonic`. `Trial.result()` needs one consistent clock across its inputs,
    not monotonicity, so a caller passes `time.time()` for `now`. Wall time is adjustable, and
    `Trial` documents a monotonic contract this cannot honour: a correction mid-trial moves host
    events relative to their windows, and nothing in a transcript another process already wrote
    can undo it. So every poll compares elapsed wall time against elapsed `monotonic` time, and
    a divergence past `CLOCK_DRIFT_TOLERANCE` ends the trial as `clock_step`: every outcome
    unobservable, every timestamp dropped rather than published on a broken timeline. What
    remains uncovered is a step large enough to put the caller's later `now` before
    `submitted_at`, which `Trial.result()` refuses outright -- fail-closed in the same direction.
    """
    if state not in TRIAL_STATES:
        raise ValueError(f'unknown trial state: {state}')
    if settle is None:
        if state != 'idle':
            raise ValueError(
                f'state={state!r} requires an explicit settle callback to establish it; the '
                f'default no-op only ever exercises idle')
        settle = lambda session_id: None  # noqa: E731 -- trivial, and named callers pass real ones
    if marker is None:
        marker = marker_token()
    elif not MARKER_PATTERN.fullmatch(marker):
        raise ValueError(f'marker does not look like a fresh marker_token() value: {marker!r}')
    if not math.isfinite(poll_interval) or poll_interval <= 0:
        raise ValueError(f'poll_interval must be a positive, finite number of seconds: {poll_interval!r}')
    session_id = driver.create(prompt)
    try:
        # Best-effort evidence: a driver whose read fails records None rather than losing the trial.
        version = driver.version(session_id)
    except Exception:
        version = None
    settle(session_id)
    submitted_mono_before = monotonic()
    submitted_at = clock()
    submitted_mono_after = monotonic()
    deadline = monotonic() + (BUSY_CAP if state == 'busy' else LAST_WINDOW)
    accepted_unobservable = False
    submission_diagnostic = None
    supported = {name: True for name in OUTCOME_NAMES}
    try:
        accepted = driver.submit(session_id, marker_message(marker))
    except SubmissionRejected as error:
        accepted = False
        submission_diagnostic = str(error)
    except SubmissionUnsupported as error:
        return TrialRun(session_id=session_id, submitted_at=submitted_at, accepted_at=None,
                        outcomes={}, state=state, marker=marker, version=version,
                        supported={name: False for name in OUTCOME_NAMES},
                        observable={name: True for name in OUTCOME_NAMES},
                        submission_diagnostic=str(error))
    except SubmissionUncaptured as error:
        return TrialRun(session_id=session_id, submitted_at=submitted_at, accepted_at=None,
                        outcomes={}, state=state, marker=marker, version=version, supported=supported,
                        observable={name: False for name in OUTCOME_NAMES},
                        turn_end_observable=False, submission_diagnostic=str(error))
    except subprocess.TimeoutExpired:
        accepted = None
    submission_diagnostic = submission_diagnostic or getattr(driver, 'submission_note', None)
    accepted_at = None
    if accepted is None:
        accepted_unobservable = True
    elif accepted is True:
        accepted_at = clock()
    elif isinstance(accepted, (int, float)) and not isinstance(accepted, bool) and math.isfinite(accepted):
        accepted_at = float(accepted)
    elif accepted is not False:
        raise TypeError(f'submit() returned {accepted!r}; expected True, False, None or an acceptance time')
    if accepted is False:
        # `accepted` itself is observable negative evidence, but `classify_trial` still waits
        # for its 10s window: `Trial.result` has no "already resolved" input. Nothing was
        # delivered, so the transcript outcomes are unobservable rather than polled to a
        # `not_observed` that would be negative evidence for a marker the host never received.
        observable = {'accepted': True}
        observable.update({name: False for name in TRANSCRIPT_OUTCOMES})
        return TrialRun(session_id=session_id, submitted_at=submitted_at, accepted_at=None,
                        outcomes={}, state=state, marker=marker, version=version, supported=supported,
                        observable=observable, turn_end_observable=False,
                        submission_diagnostic=submission_diagnostic)
    outcomes = {}
    signals = {}
    turn_end = None
    channel_readable = False
    turn_stream_capable = False
    interrupted = False
    model = None
    clock_step = None
    try:
        while True:
            observation = driver.observe(session_id, marker=marker, submitted_at=submitted_at)
            channel_readable = observation.observable
            turn_stream_capable = turn_stream_capable or observation.turn_stream
            model = model or observation.model
            for name, when in observation.outcomes.items():
                outcomes.setdefault(name, when)
                signals.setdefault(name, observation.signals.get(name))
            if state == 'busy' and turn_end is None and observation.turn_end is not None:
                # Only a busy trial has a turn "already running at submission"; for every other
                # state the first boundary after submission ends this trial's own marker turn.
                turn_end = observation.turn_end
                # A turn ending past the cap gets no extension: `Trial.result` classifies that
                # `inconclusive` regardless of how much longer polling would wait.
                if turn_end - submitted_at <= BUSY_CAP:
                    deadline = monotonic() + max(0.0, LAST_WINDOW - (clock() - turn_end))
            mono_before = monotonic()
            wall_delta = clock() - submitted_at
            mono_after = monotonic()
            # Each wall reading happened somewhere inside its monotonic bracket. Descheduling
            # widens that uncertainty; it is not evidence that either clock stepped. Declare a
            # correction only when every possible elapsed-time comparison exceeds tolerance.
            drift_low = wall_delta - (mono_after - submitted_mono_before)
            drift_high = wall_delta - (mono_before - submitted_mono_after)
            if drift_low > CLOCK_DRIFT_TOLERANCE or drift_high < -CLOCK_DRIFT_TOLERANCE:
                clock_step = drift_low if drift_low > CLOCK_DRIFT_TOLERANCE else drift_high
                break
            remaining = deadline - monotonic()
            if remaining <= 0 or all(name in outcomes for name in TRANSCRIPT_OUTCOMES):
                break
            sleep(min(poll_interval, remaining))
    except KeyboardInterrupt:
        interrupted = True
    if accepted_at is not None:
        outcomes['accepted'] = accepted_at
        signals['accepted'] = SIGNAL_SUBMIT_EXIT_STATUS
    # A positively observed outcome stands on its own evidence; only the ones still missing at
    # the deadline depend on whether the transcript could be read at all, and an interrupted poll
    # never reached its deadline.
    observable = {'accepted': not accepted_unobservable}
    for name in TRANSCRIPT_OUTCOMES:
        if name in outcomes:
            observable[name] = True
        elif interrupted:
            observable[name] = False
        else:
            observable[name] = channel_readable
    if state == 'busy' and turn_end is None and not turn_stream_capable:
        # Bare assistant activity may be the running turn's tail. A matching fresh marker,
        # however, independently acknowledges this submission even without a new turn.
        observable['turn_start'] = False
        if 'ack' not in outcomes:
            observable['ack'] = False
        # The model was read off that same unattributable assistant record -- on Claude, the
        # first one after submission, which here may belong to the turn that was already
        # running. A cell naming the prior turn's model is worse than one saying the model is
        # unknown, so it goes with the outcomes it came from.
        model = None
    turn_end_observable = turn_end is not None or (channel_readable and not interrupted)
    if clock_step is not None:
        # The wall clock moved against `monotonic` mid-trial, so every stamp compared against a
        # window -- this run's own and the host's -- is on a timeline that shifted under it. The
        # timestamps are dropped rather than reported: a stamp on a broken timeline invites
        # exactly the reasoning it cannot support, and `clock_step` is the evidence a cell cites.
        outcomes, signals, accepted_at, turn_end = {}, {}, None, None
        model = None  # Attribution used the same invalidated host-event timeline.
        observable = {name: False for name in OUTCOME_NAMES}
        turn_end_observable = False
    return TrialRun(session_id=session_id, submitted_at=submitted_at, accepted_at=accepted_at,
                    outcomes=outcomes, state=state, marker=marker, version=version, model=model,
                    supported=supported, observable=observable, signals=signals, turn_end=turn_end,
                    turn_end_observable=turn_end_observable, interrupted=interrupted,
                    clock_step=clock_step, submission_diagnostic=submission_diagnostic)


def sweep(driver):
    """Close every transient and tear down every owned session; returns the failures collected.

    Order matters and is fixed here: PTY clients first (a Codex resume client is the process
    serving its thread, and must be gone before `codex delete`), then each owned id in sorted
    order over a copy of `owned()`, then servers (an export during teardown reads the same store
    the server holds). A client that failed to close may still be serving a session, so no
    session is torn down in that case: every id stays owned and is reported with the client's
    failure. A failed teardown retains ownership and is reported, never re-raised mid-sweep, so
    one bad id cannot leave the rest alive. A second Ctrl-C during the sweep still aborts it.
    """
    failures = driver.close_clients()
    if not failures:
        for session_id in sorted(driver.owned()):
            try:
                driver.teardown(session_id)
            except Exception as error:
                failures.append((session_id, error))
    failures.extend(driver.close_servers())
    failures.extend((session_id, RuntimeError('unexpected or ambiguous session; manual investigation '
                                              'required, no cleanup authority granted'))
                    for session_id in sorted(driver.strays))
    return failures


def run_trial_with_cleanup(driver, **kwargs):
    """`run_trial`, then `sweep` -- on every exit path.

    On a failed trial the original exception propagates with a note naming the sweep's own
    failures and whatever is still owned; on a completed trial a failed sweep raises
    `CleanupFailed` carrying the `TrialRun`, since the evidence is valid even though a session
    remains for a human to remove.
    """
    error = None
    try:
        try:
            run = run_trial(driver, **kwargs)
            return run
        except BaseException as caught:
            error = caught
            raise
    finally:
        failures = sweep(driver)
        if error is not None and (failures or driver.owned()):
            error.add_note(f'cleanup after the failed trial: failures={failures!r}; '
                           f'still owned: {sorted(driver.owned())}')
        elif error is None and failures:
            raise CleanupFailed(run, failures, sorted(driver.owned()))


def classify_trial(trial, now, *, supported=None, observable=None):
    """Classify every outcome for one Trial, using wake_probe's fixed windows unmodified.

    Propagates `Trial.result()`'s ValueError verbatim on a still-open observation window rather
    than defaulting it to any result value -- a caller must wait, not guess. `supported`/
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

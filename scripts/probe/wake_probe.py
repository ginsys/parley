"""Disposable Linux PTY recorder and explicit wake-observation classification.

This is investigation tooling, not an authenticated host adapter. Observers supply
prompt state/session generation; the recorder cannot infer either from terminal text.
"""

import errno
import json
import math
import os
import pty
import selectors
import signal
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass, field

WINDOWS = {'accepted': 10, 'visible': 30, 'turn_start': 60, 'ack': 120}
RESULTS = {'observed', 'not_observed', 'unobservable', 'unsupported', 'inconclusive'}


@dataclass
class Trial:
    """All timestamps are seconds on the same monotonic clock, not wall time."""

    submitted: float
    state: str = 'idle'
    turn_end: float | None = None
    turn_end_observable: bool = True
    outcomes: dict = field(default_factory=dict)

    def result(self, outcome, now, *, supported=True, observable=True):
        window = WINDOWS[outcome]
        if self.state not in ('idle', 'busy', 'approval', 'disconnected', 'restarted'):
            raise ValueError('unknown trial state')
        if any(key not in WINDOWS for key in self.outcomes):
            raise ValueError('unknown observed outcome')
        timestamps = [self.submitted, now, *self.outcomes.values()]
        if self.turn_end is not None:
            timestamps.append(self.turn_end)
        if not all(math.isfinite(value) for value in timestamps) or now < self.submitted:
            raise ValueError('invalid observation clock')
        if self.turn_end is not None and not self.submitted <= self.turn_end <= now:
            raise ValueError('turn end outside trial')
        if any(not self.submitted <= value <= now for value in self.outcomes.values()):
            raise ValueError('observation outside trial')
        if not supported:
            return 'unsupported'
        if not observable:
            return 'unobservable'
        start = self.submitted
        observed = self.outcomes.get(outcome)
        if self.state == 'busy' and outcome in ('turn_start', 'ack'):
            # An early independently observed event is usable without a turn-end signal.
            if (observed is not None and observed <= self.submitted + 900
                    and (self.turn_end is None or observed <= self.turn_end)):
                return 'observed'
            if not self.turn_end_observable:
                return 'unobservable'
            if self.turn_end is None or self.turn_end > self.submitted + 900:
                if now < self.submitted + 900:
                    raise ValueError('current-turn observation window still open')
                return 'inconclusive'
            start = self.turn_end
        if observed is not None and observed <= start + window:
            return 'observed'
        if now < start + window:
            raise ValueError('outcome observation window still open')
        # Late timestamps remain in outcomes; do not erase them or call delivery failed.
        return 'not_observed'


def aggregate(results):
    if len(results) != 3 or any(x not in RESULTS for x in results):
        raise ValueError('exactly three classified trials required')
    return results[0] if len(set(results)) == 1 else 'inconclusive'


class PtyProcess:
    """Own one disposable child, bounded transcript and observer-supplied generation.

    argv/env/cwd must describe a disposable investigation process. No shell expansion.
    Do not use observer assertions as authentication or an approval-prompt detector.
    """

    def __init__(self, argv, *, cwd, env, generation, max_bytes=1024 * 1024):
        if not argv or max_bytes <= 0:
            raise ValueError('command and positive transcript limit required')
        self.generation = generation
        self.events = []
        self.limit = max_bytes
        self.size = 0
        self.fd = None
        self.pid = None
        self.eof = False
        self.wait_status = None
        self.exit_code = None
        self.cleanup_requested = False
        self.selector = selectors.DefaultSelector()
        try:
            pid, fd = pty.fork()
            if pid == 0:
                try:
                    os.chdir(cwd)
                    os.execvpe(argv[0], argv, env)
                except BaseException:
                    os._exit(127)
            self.pid, self.fd = pid, fd
            os.set_blocking(fd, False)
            os.set_inheritable(fd, False)
            self.selector.register(fd, selectors.EVENT_READ)
        except BaseException:
            self.close()
            raise

    def _record(self, kind, data):
        if self.size + len(data) > self.limit:
            raise ValueError('transcript limit exceeded; trial incomplete')
        self.size += len(data)
        self.events.append({'time': time.monotonic(), 'kind': kind, 'hex': data.hex()})

    def read(self, timeout):
        if self.eof or not self.selector.select(max(0, timeout)):
            return b''
        try:
            data = os.read(self.fd, 4096)
        except BlockingIOError:
            return b''
        except OSError as error:
            if error.errno != errno.EIO:
                raise
            data = b''
        if not data:
            self.eof = True
            self.selector.unregister(self.fd)
            return b''
        self._record('output', data)
        return data

    def send(self, payload, *, generation, state):
        if self.fd is None or self.eof or generation != self.generation:
            raise ValueError('closed or stale session')
        if state != 'idle':
            raise ValueError('PTY injection requires observed idle agent state')
        if len(payload) > 4096 or self.size + len(payload) > self.limit:
            raise ValueError('input exceeds bound')
        # A partial/failed write is ambiguous, never host acceptance or an ack.
        written = os.write(self.fd, payload)
        self._record('input', payload[:written])
        if written != len(payload):
            raise OSError('partial PTY write; delivery ambiguous')
        return written

    def wait_for_exit(self, deadline):
        """Observe the leader without reaping it or extending the capture deadline."""
        while time.monotonic() < deadline:
            if os.waitid(os.P_PID, self.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None:
                return True
            time.sleep(min(.01, max(0, deadline - time.monotonic())))
        return False

    def close(self):
        try:
            if self.pid is not None:
                # Inspect without reaping: keep the PID reserved while killing any
                # descendants in the disposable group. Preserve a natural exit status.
                ended = os.waitid(os.P_PID, self.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                self.cleanup_requested = self.cleanup_requested or ended is None
                try:
                    os.killpg(self.pid, signal.SIGKILL)
                except ProcessLookupError:
                    # forkpty session setup may not have finished in the child yet.
                    try:
                        os.kill(self.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                _, self.wait_status = os.waitpid(self.pid, 0)
                # Only a successful reap releases ownership. An interrupted/failed
                # wait must leave this child available for a later cleanup attempt.
                self.pid = None
                self.exit_code = os.waitstatus_to_exitcode(self.wait_status)
        finally:
            try:
                if self.fd is not None:
                    fd, self.fd = self.fd, None
                    os.close(fd)
            finally:
                self.selector.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def publish_record(destination, record):
    """Publish complete JSON atomically without replacing another capture.

    A hard link is the no-replace atomic publication operation available in the
    standard library on Linux; exists()+rename would race and overwrite evidence.
    The temporary file lives beside its destination, on the same filesystem.
    """
    directory = os.path.dirname(os.path.abspath(destination))
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=directory,
                                         prefix='.parley-capture-', suffix='.tmp', delete=False) as stream:
            temporary = stream.name
            json.dump(record, stream, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, destination)
    finally:
        if temporary is not None:
            os.unlink(temporary)


@contextmanager
def defer_sigint():
    """Let CLI finalization finish before honoring Ctrl-C, including repeated signals.

    Install only on the main thread. The caller returns 130 after publishing if
    interrupted; capture status still describes capture, not the publication phase.
    """
    interrupted = [False]

    def remember_interrupt(signum, frame):
        interrupted[0] = True

    previous = signal.signal(signal.SIGINT, remember_interrupt)
    try:
        yield interrupted
    finally:
        signal.signal(signal.SIGINT, previous)


def main():
    """Passively record a disposable command; injection requires an explicit observer."""
    import argparse
    import datetime

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds', type=float, default=10)
    parser.add_argument('--output', required=True, help='new JSON evidence file (never overwritten)')
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command or not 0 < args.seconds <= 1020:
        parser.error('command and duration in (0, 1020] required')
    if os.path.lexists(args.output):
        raise FileExistsError(args.output)
    record = {'utc': datetime.datetime.now(datetime.UTC).isoformat(),
              'command': command, 'duration': args.seconds, 'started': time.monotonic(),
              'kind': 'passive_capture', 'delivery_claim': None, 'events': [],
              'eof': False, 'error': None, 'cleanup_error': None, 'wait_status': None, 'exit_code': None,
              'cleanup_requested': False, 'capture_status': 'failed', 'stop_reason': 'startup'}
    result = 0
    child = None
    # No final file exists until a complete (possibly interrupted/failed) record
    # is ready. HOME/cwd never inherit host authentication.
    with tempfile.TemporaryDirectory(prefix='parley-probe-') as home:
        try:
            child = PtyProcess(command, cwd=home,
                               env={'HOME': home, 'PATH': os.environ.get('PATH', os.defpath), 'TERM': 'dumb'},
                               generation='disposable')
            deadline = record['started'] + args.seconds
            record['stop_reason'] = 'capture'
            while not child.eof and time.monotonic() < deadline:
                child.read(min(.1, max(0, deadline - time.monotonic())))
            # Terminal EOF can precede process exit. Keep the same deadline and
            # preserve a natural exit status before cleanup kills the owned group.
            exited = child.eof and child.wait_for_exit(deadline)
            record['stop_reason'] = 'eof' if exited else 'deadline'
        except BaseException as exc:
            record['error'] = type(exc).__name__
            result = 130 if isinstance(exc, KeyboardInterrupt) else 1
            record['capture_status'] = 'interrupted' if isinstance(exc, (KeyboardInterrupt, SystemExit)) else 'failed'
        finally:
            with defer_sigint() as interrupted:
                if child is not None:
                    try:
                        child.close()
                    except BaseException as exc:
                        # Preserve both failures if capture was already interrupted.
                        # Teardown failure must not discard the captured transcript or
                        # classify an unknown child status as a successful recording.
                        record['cleanup_error'] = type(exc).__name__
                        record['error'] = record['error'] or 'cleanup_failed'
                        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                            record['capture_status'] = 'interrupted'
                        if isinstance(exc, KeyboardInterrupt):
                            result = 130
                        elif result == 0:
                            result = 1
                    finally:
                        record.update(events=child.events, eof=child.eof, wait_status=child.wait_status,
                                      exit_code=child.exit_code, cleanup_requested=child.cleanup_requested)
                record['ended'] = time.monotonic()
                if record['error'] is None:
                    if child.exit_code > 0 or (child.exit_code < 0 and
                                               (not child.cleanup_requested or child.exit_code != -signal.SIGKILL)):
                        # 127 includes failed exec; a program can also deliberately return
                        # 127, so it is not proof of which startup stage failed. Neither is
                        # valid negative wake evidence. Record the exact status, fail closed.
                        record.update(capture_status='failed', error='child_exit_nonzero')
                        result = 1
                    else:
                        # A successful leader may leave descendants holding the PTY.
                        # Only observed EOF plus natural leader success proves capture
                        # completion; reaching the deadline always truncates capture.
                        complete = record['stop_reason'] == 'eof' and child.eof and not child.cleanup_requested
                        record['capture_status'] = 'complete' if complete else 'stopped'
                publish_record(args.output, record)
            if interrupted[0]:
                result = 130
    return result


if __name__ == '__main__':
    raise SystemExit(main())

"""Disposable Linux PTY recorder and explicit wake-observation classification.

This is investigation tooling, not an authenticated host adapter. Observers supply
prompt state/session generation; the recorder cannot infer either from terminal text.
"""

import errno
import os
import pty
import selectors
import signal
import time
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
        if not supported:
            return 'unsupported'
        if not observable:
            return 'unobservable'
        start = self.submitted
        observed = self.outcomes.get(outcome)
        if observed is not None and not self.submitted <= observed <= now:
            raise ValueError('observation outside trial')
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
            if not self.submitted <= self.turn_end <= now:
                raise ValueError('turn end outside trial')
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

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        self.selector.close()
        if self.pid is not None:
            # pty.fork makes the child a session/process-group leader. Terminate its
            # whole disposable group, including children which inherited the PTY.
            try:
                os.killpg(self.pid, signal.SIGKILL)
            except ProcessLookupError:
                # Child may not have completed forkpty session setup yet.
                try:
                    os.kill(self.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            os.waitpid(self.pid, 0)
            self.pid = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def main():
    """Passively record a disposable command; injection requires an explicit observer."""
    import argparse
    import datetime
    import json
    import tempfile

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds', type=float, default=10)
    parser.add_argument('--output', required=True, help='new JSON evidence file (never overwritten)')
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command or not 0 < args.seconds <= 1020:
        parser.error('command and duration in (0, 1020] required')
    # Opening with x prevents overwriting an earlier observation. HOME/cwd have no
    # host authentication. No credential copying or implicit live session enrollment.
    with open(args.output, 'x', encoding='utf-8') as output, tempfile.TemporaryDirectory(prefix='parley-probe-') as home:
        record = {'utc': datetime.datetime.now(datetime.UTC).isoformat(),
                  'command': command, 'duration': args.seconds,
                  'kind': 'passive_capture', 'delivery_claim': None}
        with PtyProcess(command, cwd=home, env={'HOME': home, 'PATH': os.environ.get('PATH', os.defpath),
                                              'TERM': 'dumb'}, generation='disposable') as child:
            record['started'] = time.monotonic()
            deadline = record['started'] + args.seconds
            error = None
            try:
                while not child.eof and time.monotonic() < deadline:
                    child.read(min(.1, max(0, deadline - time.monotonic())))
            except (OSError, ValueError) as exc:
                error = str(exc)
            record.update(events=child.events, eof=child.eof, error=error, ended=time.monotonic())
            json.dump(record, output, indent=2)
            output.write('\n')
        return 1 if error else 0


if __name__ == '__main__':
    raise SystemExit(main())

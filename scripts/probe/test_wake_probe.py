"""Controlled subprocesses only: no installed host CLI is launched by these tests."""

import fcntl
import inspect
import json
import os
import pty
import signal
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from wake_probe import PtyProcess, Trial, aggregate, main, publish_record

CHILD = '''
import sys, termios
config = termios.tcgetattr(0)
config[3] &= ~termios.ECHO
termios.tcsetattr(0, termios.TCSANOW, config)
print('READY', flush=True)
for line in sys.stdin:
    print('BYTES_RECEIVED', flush=True)
'''


class ObservationTests(unittest.TestCase):
    def test_distinct_deadlines_and_late_evidence(self):
        t = Trial(0, outcomes={'accepted': 9, 'visible': 31, 'ack': 121})
        self.assertEqual(t.result('accepted', 200), 'observed')
        self.assertEqual(t.result('visible', 200), 'not_observed')
        self.assertEqual(t.result('ack', 200), 'not_observed')
        self.assertEqual(t.outcomes['ack'], 121)
        with self.assertRaises(ValueError):
            Trial(0).result('ack', 119)

    def test_busy_ack_measured_after_turn_end(self):
        t = Trial(0, state='busy', turn_end=500, outcomes={'ack': 610})
        self.assertEqual(t.result('ack', 620), 'observed')
        self.assertEqual(t.result('turn_start', 620), 'not_observed')
        with self.assertRaises(ValueError):
            Trial(0, state='busy', turn_end=500).result('ack', 600)

    def test_busy_missing_completion_is_not_delivery_evidence(self):
        self.assertEqual(Trial(0, state='busy').result('ack', 900), 'inconclusive')
        self.assertEqual(Trial(0, state='busy', turn_end=901).result('ack', 1100), 'inconclusive')
        self.assertEqual(Trial(0, state='busy', turn_end_observable=False).result('ack', 200), 'unobservable')
        self.assertEqual(Trial(0, state='busy', outcomes={'ack': 50}).result('ack', 60), 'observed')
        self.assertEqual(Trial(0, state='busy', outcomes={'ack': 950}).result('ack', 960), 'inconclusive')

    def test_capability_and_trial_aggregation(self):
        self.assertEqual(Trial(0).result('ack', 200, observable=False), 'unobservable')
        self.assertEqual(Trial(0).result('ack', 200, supported=False), 'unsupported')
        self.assertEqual(aggregate(['observed'] * 3), 'observed')
        self.assertEqual(aggregate(['observed', 'observed', 'not_observed']), 'inconclusive')
        with self.assertRaises(ValueError):
            aggregate(['observed'])

    def test_invalid_observation_inputs_cannot_be_classified(self):
        cases = [(Trial(0, state='buisy'), 200), (Trial(10), 0),
                 (Trial(0, outcomes={'ACK': 1}), 200),
                 (Trial(0, outcomes={'accepted': 300}), 200),
                 (Trial(0, state='busy', turn_end=-1, outcomes={'ack': 1}), 200),
                 (Trial(0, state='busy', turn_end=300, outcomes={'ack': 1}), 200)]
        for invalid in (float('nan'), float('inf'), -float('inf')):
            cases.extend([(Trial(invalid), 200), (Trial(0), invalid),
                          (Trial(0, state='busy', turn_end=invalid), 200),
                          (Trial(0, outcomes={'ack': invalid}), 200)])
        for trial, now in cases:
            with self.subTest(trial=trial, now=now), self.assertRaises(ValueError):
                trial.result('ack', now)


class PtyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def spawn(self, code=CHILD, generation='session-1', **kwargs):
        child = PtyProcess([sys.executable, '-u', '-c', code], cwd=self.tmp.name,
                           env={'PATH': os.defpath, 'HOME': self.tmp.name},
                           generation=generation, **kwargs)
        self.addCleanup(child.close)
        return child

    def until(self, child, marker):
        end = time.monotonic() + 5
        data = b''
        while marker not in data and time.monotonic() < end and not child.eof:
            data += child.read(.1)
        self.assertIn(marker, data)
        return data

    def test_exec_closes_inherited_non_stdio_descriptors(self):
        read_fd, write_fd = os.pipe()
        self.addCleanup(os.close, read_fd)
        self.addCleanup(os.close, write_fd)
        # Allocate a free high descriptor so interpreter startup cannot reuse its
        # number and make the assertion accidentally inspect an unrelated file.
        inherited = fcntl.fcntl(read_fd, fcntl.F_DUPFD, 200)
        self.addCleanup(os.close, inherited)
        os.set_inheritable(inherited, True)
        code = f"""
import errno, os
try:
    os.fstat({inherited})
except OSError as error:
    assert error.errno == errno.EBADF
    print('DESCRIPTOR_CLOSED', flush=True)
else:
    print('DESCRIPTOR_LEAKED', flush=True)
"""
        child = self.spawn(code)
        self.until(child, b'DESCRIPTOR_CLOSED')
        # Child isolation must not close or otherwise consume the parent handle.
        os.write(write_fd, b'parent fixture')
        self.assertEqual(os.read(inherited, 100), b'parent fixture')

    def test_descriptor_enumeration_failure_aborts_exec(self):
        with patch('wake_probe.os.listdir', side_effect=OSError('proc unavailable')):
            child = self.spawn("print('must not execute', flush=True)")
        self.assertTrue(child.wait_for_exit(time.monotonic() + 5))
        child.close()
        self.assertEqual(child.exit_code, 127)
        self.assertFalse(child.cleanup_requested)

    def test_sigint_during_fork_records_ownership_before_unmasking(self):
        output = Path(self.tmp.name, 'fork-interrupt.json')
        original_fork = pty.fork
        handles = []
        original_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())

        def interrupted_fork():
            pid, fd = original_fork()
            if pid:
                handles.append((pid, fd))
                os.kill(os.getpid(), signal.SIGINT)
            return pid, fd

        args = ['wake_probe', '--output', str(output), '--seconds', '60', '--',
                sys.executable, '-c', 'import signal,time; signal.signal(signal.SIGHUP, signal.SIG_IGN); time.sleep(30)']
        try:
            with patch.object(sys, 'argv', args), patch('wake_probe.pty.fork', interrupted_fork):
                self.assertEqual(main(), 130)
            self.assertEqual(json.loads(output.read_text())['capture_status'], 'interrupted')
            self.assertEqual(signal.pthread_sigmask(signal.SIG_BLOCK, set()), original_mask)
            self.assertEqual(len(handles), 1)
            pid, fd = handles[0]
            with self.assertRaises(ChildProcessError):
                os.waitpid(pid, os.WNOHANG)
            with self.assertRaises(OSError):
                os.fstat(fd)
        finally:
            # The red test must also clean up the child/FD leaked by the old
            # constructor. Only kill a still-owned unreaped child, never a reused PID.
            for pid, fd in handles:
                try:
                    ended, _ = os.waitpid(pid, os.WNOHANG)
                    if not ended:
                        os.kill(pid, signal.SIGKILL)
                        os.waitpid(pid, 0)
                except ChildProcessError:
                    pass
                try:
                    os.close(fd)
                except OSError:
                    pass

    def test_exec_has_recorded_terminal_size_and_restored_signal_mask(self):
        output = Path(self.tmp.name, 'terminal.json')
        original_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
        code = f"""
import os, signal
size = os.get_terminal_size(0)
assert set(signal.pthread_sigmask(signal.SIG_BLOCK, set())) == {set(map(int, original_mask))!r}
print(f'SIZE {{size.columns}} {{size.lines}}', flush=True)
"""
        args = ['wake_probe', '--output', str(output), '--seconds', '60', '--',
                sys.executable, '-u', '-c', code]
        with patch.object(sys, 'argv', args):
            self.assertEqual(main(), 0)
        record = json.loads(output.read_text())
        transcript = b''.join(bytes.fromhex(e['hex']) for e in record['events'])
        self.assertIn(b'SIZE 80 24', transcript)
        self.assertEqual(record['terminal_size'], {'columns': 80, 'rows': 24})
        self.assertEqual(record['capture_status'], 'complete')

    def test_replaced_pane_and_approval_prompt_reject_before_writing(self):
        for state in ('shell', 'pager', 'editor', 'approval', 'busy', 'unknown'):
            with self.subTest(state=state):
                child = self.spawn()
                self.until(child, b'READY')
                with self.assertRaises(ValueError):
                    child.send(b'approve\n', generation='session-1', state=state)
                self.assertFalse(any(e['kind'] == 'input' for e in child.events))
                self.assertEqual(child.read(.02), b'')
                child.close()

    def test_restart_rejects_old_generation(self):
        old = self.spawn()
        self.until(old, b'READY')
        old.close()
        new = self.spawn(generation='session-2')
        self.until(new, b'READY')
        with self.assertRaises(ValueError):
            new.send(b'hello\n', generation='session-1', state='idle')
        self.assertFalse(any(e['kind'] == 'input' for e in new.events))

    def test_written_bytes_are_not_an_acknowledgement(self):
        child = self.spawn()
        self.until(child, b'READY')
        started = time.monotonic()
        self.assertEqual(child.send(b'hello\n', generation='session-1', state='idle'), 6)
        data = self.until(child, b'BYTES_RECEIVED')
        self.assertNotIn(b'ACK', data)
        self.assertEqual(Trial(started).result('ack', started + 120, observable=False), 'unobservable')
        self.assertTrue(all(e['time'] >= started for e in child.events if e['kind'] == 'input'))

    def test_passive_cli_records_synthetic_output_and_preserves_existing_evidence(self):
        output = Path(self.tmp.name, 'evidence.json')
        cmd = [sys.executable, str(Path(__file__).with_name('wake_probe.py')),
               '--output', str(output), '--seconds', '2', '--', sys.executable,
               '-u', '-c', "print('synthetic fixture', flush=True)"]
        ran = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        self.assertEqual(ran.returncode, 0, ran.stderr)
        record = json.loads(output.read_text())
        self.assertIsNone(record['delivery_claim'])
        self.assertTrue(record['eof'])
        self.assertEqual(record['exit_code'], 0)
        self.assertIsNone(record['error'])
        self.assertIn(b'synthetic fixture', b''.join(bytes.fromhex(e['hex']) for e in record['events']))
        original = output.read_bytes()
        ran = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        self.assertNotEqual(ran.returncode, 0)
        self.assertEqual(output.read_bytes(), original)

    def test_home_inherit_uses_real_environment_and_skips_temporary_home(self):
        output = Path(self.tmp.name, 'inherit.json')
        args = ['wake_probe', '--output', str(output), '--seconds', '2', '--home', 'inherit',
                '--', sys.executable, '-u', '-c', "import os; print(os.environ.get('HOME'), flush=True)"]
        with patch.object(sys, 'argv', args), \
                patch('wake_probe.tempfile.TemporaryDirectory',
                      side_effect=AssertionError('inherit mode must not create a throwaway HOME')):
            self.assertEqual(main(), 0)
        record = json.loads(output.read_text())
        self.assertEqual(record['home_mode'], 'inherit')
        # The generation names the session `send` checks against; an inherit-mode capture
        # labelled 'disposable' would let a token from the other mode pass as current.
        self.assertEqual(record['generation'], 'inherit')
        self.assertIsNone(record['home_cleanup_error'])
        printed = b''.join(bytes.fromhex(e['hex']) for e in record['events'])
        self.assertIn(os.environ['HOME'].encode(), printed)

    def test_home_missing_for_inherit_mode_is_a_usage_error(self):
        output = Path(self.tmp.name, 'no-home.json')
        args = ['wake_probe', '--output', str(output), '--home', 'inherit', '--', 'fixture']
        with patch.object(sys, 'argv', args), patch.dict(os.environ, {'HOME': ''}):
            with self.assertRaises(SystemExit):
                main()
        self.assertFalse(output.exists())

    def test_disposable_is_the_default_home_mode(self):
        output = Path(self.tmp.name, 'disposable.json')
        args = ['wake_probe', '--output', str(output), '--seconds', '2', '--',
                sys.executable, '-u', '-c', "print('default mode', flush=True)"]
        with patch.object(sys, 'argv', args):
            self.assertEqual(main(), 0)
        record = json.loads(output.read_text())
        self.assertEqual(record['home_mode'], 'disposable')
        self.assertEqual(record['generation'], 'disposable')
        self.assertIsNotNone(record['cwd'])  # the throwaway HOME, recorded not implied

    def test_explicit_cwd_is_recorded_as_the_resolved_effective_directory(self):
        # Host behaviour varies by directory-level configuration, so a capture that does not
        # name where the child ran cannot be reproduced or compared against another.
        output = Path(self.tmp.name, 'cwd.json')
        args = ['wake_probe', '--output', str(output), '--seconds', '2', '--cwd', self.tmp.name, '--',
                sys.executable, '-u', '-c', "import os; print(os.getcwd(), flush=True)"]
        with patch.object(sys, 'argv', args):
            self.assertEqual(main(), 0)
        record = json.loads(output.read_text())
        # realpath, not abspath: the record must name the directory the child actually reached.
        self.assertEqual(record['cwd'], os.path.realpath(self.tmp.name))
        printed = b''.join(bytes.fromhex(e['hex']) for e in record['events'])
        self.assertIn(os.path.realpath(self.tmp.name).encode(), printed)

    def test_missing_command_and_nonzero_exit_are_failed_captures(self):
        for command, expected in (([str(Path(self.tmp.name, 'missing-host'))], 127),
                                  ([sys.executable, '-c', 'raise SystemExit(23)'], 23)):
            with self.subTest(command=command):
                output = Path(self.tmp.name, 'result-' + str(len(command)) + '.json')
                args = ['wake_probe', '--output', str(output), '--seconds', '2', '--', *command]
                with patch.object(sys, 'argv', args):
                    result = main()
                self.assertEqual(result, 1)
                record = json.loads(output.read_text())
                self.assertEqual(record['capture_status'], 'failed')
                self.assertEqual(record['exit_code'], expected)
                self.assertEqual(record['error'], 'child_exit_nonzero')

    def test_interrupt_preserves_partial_events_and_reaps_child(self):
        output = Path(self.tmp.name, 'interrupted.json')
        original = PtyProcess.read
        seen = []

        def interrupted(child, timeout):
            data = original(child, timeout)
            if data:
                seen.append(child.pid)
                raise KeyboardInterrupt
            return data

        args = ['wake_probe', '--output', str(output), '--seconds', '30', '--',
                sys.executable, '-u', '-c', "import time; print('before interrupt', flush=True); time.sleep(30)"]
        with patch.object(sys, 'argv', args), patch.object(PtyProcess, 'read', interrupted):
            try:
                result = main()
            except KeyboardInterrupt:
                self.fail('interrupt escaped before partial evidence was saved')
        self.assertEqual(result, 130)
        record = json.loads(output.read_text())
        self.assertEqual(record['capture_status'], 'interrupted')
        self.assertTrue(record['events'])
        self.assertTrue(record['cleanup_requested'])
        self.assertFalse(Path(f'/proc/{seen[0]}').exists())

    def test_sigint_at_caller_ownership_and_finalization_handoffs(self):
        source, first_line = inspect.getsourcelines(main)
        finalization_line = first_line + next(i for i, line in enumerate(source)
                                             if line == '        finally:\n') + 1
        for stage in ('constructor_return', 'finalization_entry'):
            with self.subTest(stage=stage):
                output = Path(self.tmp.name, stage + '.json')
                children = []
                triggered = []
                previous_handler = signal.getsignal(signal.SIGINT)

                def construct(*args, **kwargs):
                    child = PtyProcess(*args, **kwargs)
                    children.append(child)
                    if stage == 'constructor_return':
                        signal.raise_signal(signal.SIGINT)
                        triggered.append(stage)
                    return child

                def trace(frame, event, arg):
                    if (stage == 'finalization_entry' and not triggered
                            and frame.f_code is main.__code__ and event == 'line'
                            and frame.f_lineno == finalization_line):
                        triggered.append(stage)
                        signal.raise_signal(signal.SIGINT)
                    return trace

                args = ['wake_probe', '--output', str(output), '--seconds', '60', '--',
                        sys.executable, '-u', '-c', "print('handoff fixture', flush=True)"]
                old_trace = sys.gettrace()
                try:
                    with patch.object(sys, 'argv', args), patch('wake_probe.PtyProcess', construct):
                        sys.settrace(trace)
                        result = main()
                    self.assertEqual(result, 130)
                    self.assertEqual(triggered, [stage])
                    record = json.loads(output.read_text())
                    self.assertEqual(record['capture_status'],
                                     'interrupted' if stage == 'constructor_return' else 'complete')
                    self.assertIsNotNone(record['wait_status'])
                    self.assertTrue(all(c.pid is None and c.fd is None for c in children))
                    self.assertEqual(signal.getsignal(signal.SIGINT), previous_handler)
                finally:
                    sys.settrace(old_trace)
                    for child in children:
                        child.close()

    def test_quiet_reopened_slave_clears_provisional_eof(self):
        child = self.spawn()
        self.until(child, b'READY')
        # Model the previously observed hangup; the real live slave is open and
        # quiet now, so the nonblocking read returns EAGAIN rather than EOF.
        child.selector.unregister(child.fd)
        child.eof = True
        self.assertEqual(child.read(0), b'')
        self.assertFalse(child.eof)
        self.assertIn(child.fd, child.selector.get_map())

    def test_reopened_slave_output_is_captured_before_completion(self):
        output = Path(self.tmp.name, 'reopened.json')
        release = Path(self.tmp.name, 'reopen.release')
        code = f"""
import os, time
name = os.ttyname(0)
print('before close', flush=True)
for fd in (0, 1, 2):
    os.close(fd)
while not os.path.exists({str(release)!r}):
    time.sleep(.01)
fd = os.open(name, os.O_RDWR)
os.write(fd, b'after reopen\\n' * 1000)
os.close(fd)
"""
        original = PtyProcess.wait_for_exit
        observed_eof = []

        def release_after_eof(child, deadline, **kwargs):
            self.assertTrue(child.eof)
            observed_eof.append(True)
            release.touch()
            return original(child, deadline, **kwargs)

        args = ['wake_probe', '--output', str(output), '--seconds', '60', '--',
                sys.executable, '-u', '-c', code]
        with patch.object(sys, 'argv', args), patch.object(PtyProcess, 'wait_for_exit', release_after_eof):
            self.assertEqual(main(), 0)
        record = json.loads(output.read_text())
        transcript = b''.join(bytes.fromhex(e['hex']) for e in record['events'])
        self.assertEqual(observed_eof, [True])
        self.assertEqual(transcript.count(b'after reopen'), 1000)
        self.assertEqual(record['capture_status'], 'complete')
        self.assertEqual(record['exit_code'], 0)

    def test_failed_dump_leaves_no_empty_destination_or_temporary_file(self):
        output = Path(self.tmp.name, 'failed.json')

        def fail_dump(record, stream, **kwargs):
            stream.write('{')
            raise OSError('injected write failure')

        args = ['wake_probe', '--output', str(output), '--seconds', '2', '--',
                sys.executable, '-c', 'pass']
        with patch.object(sys, 'argv', args), patch('json.dump', fail_dump):
            with self.assertRaises(OSError):
                main()
        self.assertEqual(list(Path(self.tmp.name).iterdir()), [])

    def test_teardown_failure_preserves_events_and_original_capture_error(self):
        original_read = PtyProcess.read
        original_close = PtyProcess.close
        for capture_interrupted in (False, True):
            for failure in (OSError, KeyboardInterrupt):
                with self.subTest(capture_interrupted=capture_interrupted, failure=failure):
                    output = Path(self.tmp.name, f'cleanup-{capture_interrupted}-{failure.__name__}.json')
                    children = []

                    def read(child, timeout):
                        data = original_read(child, timeout)
                        if data and capture_interrupted:
                            raise KeyboardInterrupt
                        return data

                    def fail_close(child):
                        children.append(child)
                        raise failure

                    args = ['wake_probe', '--output', str(output), '--seconds', '2', '--',
                            sys.executable, '-u', '-c', "print('before cleanup', flush=True)"]
                    try:
                        with patch.object(sys, 'argv', args), patch.object(PtyProcess, 'read', read), \
                                patch.object(PtyProcess, 'close', fail_close):
                            result = main()
                        record = json.loads(output.read_text())
                        self.assertIn(b'before cleanup', b''.join(bytes.fromhex(e['hex']) for e in record['events']))
                        self.assertEqual(record['cleanup_error'], failure.__name__)
                        self.assertEqual(record['error'], 'KeyboardInterrupt' if capture_interrupted else 'cleanup_failed')
                        interrupted = capture_interrupted or failure is KeyboardInterrupt
                        self.assertEqual(result, 130 if interrupted else 1)
                        self.assertEqual(record['capture_status'], 'interrupted' if interrupted else 'failed')
                        self.assertIsNone(record['exit_code'])
                        self.assertGreaterEqual(record['ended'], record['started'])
                    finally:
                        for child in children:
                            original_close(child)

    def test_publication_syncs_directory_and_reports_sync_failure(self):
        for fail_directory in (False, True):
            with self.subTest(fail_directory=fail_directory):
                output = Path(self.tmp.name, f'durable-{fail_directory}.json')
                original_sync = os.fsync
                synced = []
                directory_fds = []

                def sync(fd):
                    directory = stat.S_ISDIR(os.fstat(fd).st_mode)
                    synced.append('directory' if directory else 'file')
                    if directory:
                        directory_fds.append(fd)
                        self.assertEqual(json.loads(output.read_text()), {'fixture': True})
                        self.assertEqual(list(Path(self.tmp.name).glob('.parley-capture-*')), [])
                        if fail_directory:
                            raise OSError('directory sync failed')
                    return original_sync(fd)

                with patch('os.fsync', sync):
                    if fail_directory:
                        with self.assertRaisesRegex(OSError, 'directory sync failed'):
                            publish_record(output, {'fixture': True})
                    else:
                        publish_record(output, {'fixture': True})
                self.assertEqual(synced, ['file', 'directory'])
                for fd in directory_fds:
                    with self.assertRaises(OSError):
                        os.fstat(fd)

    def test_publication_race_preserves_winner(self):
        output = Path(self.tmp.name, 'winner.json')
        output.write_text('existing evidence')
        with self.assertRaises(FileExistsError):
            publish_record(output, {'competing': True})
        self.assertEqual(output.read_text(), 'existing evidence')
        self.assertEqual(list(Path(self.tmp.name).iterdir()), [output])

    def test_sigint_during_publication_preserves_record_and_restores_handler(self):
        for module, name in ((json, 'dump'), (os, 'fsync'), (os, 'link'), (os, 'unlink')):
            with self.subTest(operation=name):
                output = Path(self.tmp.name, f'publication-{name}.json')
                original = getattr(module, name)
                handler = signal.getsignal(signal.SIGINT)

                def interrupted(*args, **kwargs):
                    signal.raise_signal(signal.SIGINT)
                    signal.raise_signal(signal.SIGINT)
                    return original(*args, **kwargs)

                args = ['wake_probe', '--output', str(output), '--seconds', '5', '--',
                        sys.executable, '-u', '-c', "print('publication fixture', flush=True)"]
                with patch.object(sys, 'argv', args), patch.object(module, name, interrupted):
                    try:
                        result = main()
                    except KeyboardInterrupt:
                        self.fail('publication SIGINT escaped before evidence was preserved')
                self.assertEqual(result, 130)
                record = json.loads(output.read_text())
                self.assertEqual(record['capture_status'], 'complete')
                self.assertEqual(record['exit_code'], 0)
                self.assertTrue(record['eof'])
                self.assertIn(b'publication fixture', b''.join(bytes.fromhex(e['hex']) for e in record['events']))
                self.assertEqual(signal.getsignal(signal.SIGINT), handler)
                self.assertEqual(list(Path(self.tmp.name).glob('.parley-capture-*')), [])

    def test_signal_exit_is_distinct_from_deadline_cleanup(self):
        for code, expected, status in (
            ('import os, signal; os.kill(os.getpid(), signal.SIGTERM)', -signal.SIGTERM, 'failed'),
            ('import time; print("running", flush=True); time.sleep(30)', -signal.SIGKILL, 'stopped'),
        ):
            with self.subTest(status=status):
                output = Path(self.tmp.name, status + '.json')
                original_read, monotonic = PtyProcess.read, time.monotonic
                now = 0
                watchdog = monotonic() + 10
                observed = bytearray()

                def read(child, timeout):
                    nonlocal now
                    if monotonic() >= watchdog:
                        self.fail('fixture did not reach its synchronized outcome')
                    data = original_read(child, timeout)
                    observed.extend(data)
                    # The capture clock cannot expire during fork/exec. Only the
                    # observed running marker advances the deadline-cleanup case.
                    if status == 'stopped' and b'running' in observed:
                        now = 120
                    return data

                args = ['wake_probe', '--output', str(output), '--seconds', '60', '--',
                        sys.executable, '-u', '-c', code]
                with patch.object(sys, 'argv', args), patch.object(PtyProcess, 'read', read), \
                        patch('wake_probe.time.monotonic', side_effect=lambda: now):
                    result = main()
                record = json.loads(output.read_text())
                self.assertEqual(record['exit_code'], expected)
                self.assertEqual(record['capture_status'], status)
                self.assertEqual(result, 1 if status == 'failed' else 0)

    def test_eof_waits_for_exit_without_losing_deadline_or_interruption(self):
        for outcome, expected, status, result_code in (
            ('success', 0, 'complete', 0), ('failure', 23, 'failed', 1),
            ('signal', -signal.SIGTERM, 'failed', 1),
            ('deadline', -signal.SIGKILL, 'stopped', 0),
            ('interrupt', -signal.SIGKILL, 'interrupted', 130),
        ):
            with self.subTest(outcome=outcome):
                output = Path(self.tmp.name, outcome + '.json')
                release = Path(self.tmp.name, outcome + '.release')
                code = f"""
import os, signal, time
print('before eof', flush=True)
for fd in (0, 1, 2):
    os.close(fd)
while not os.path.exists({str(release)!r}):
    time.sleep(.01)
if {outcome!r} == 'signal':
    os.kill(os.getpid(), signal.SIGTERM)
os._exit({expected if expected >= 0 else 0})
"""
                test = self
                now = 0
                monotonic = time.monotonic
                watchdog = monotonic() + 10
                waited = []

                class SynchronizedProcess(PtyProcess):
                    def read(self, timeout):
                        if monotonic() >= watchdog:
                            test.fail('fixture did not close its terminal')
                        return super().read(timeout)

                    def wait_for_exit(self, deadline, **kwargs):
                        nonlocal now
                        test.assertTrue(self.eof)
                        test.assertIsNone(os.waitid(os.P_PID, self.pid,
                                                  os.WEXITED | os.WNOHANG | os.WNOWAIT))
                        waited.append(self.pid)
                        if outcome == 'interrupt':
                            signal.raise_signal(signal.SIGINT)
                        if outcome == 'deadline':
                            now = deadline
                        else:
                            release.touch()
                        # Keep the real wait bounded even if the fixture fails to
                        # exit. The deadline case tests the expired capture clock.
                        with patch('wake_probe.time.monotonic', monotonic):
                            return super().wait_for_exit(monotonic() if now else watchdog, **kwargs)

                args = ['wake_probe', '--output', str(output), '--seconds', '60', '--',
                        sys.executable, '-u', '-c', code]
                with patch.object(sys, 'argv', args), patch('wake_probe.PtyProcess', SynchronizedProcess), \
                        patch('wake_probe.time.monotonic', side_effect=lambda: now):
                    result = main()
                record = json.loads(output.read_text())
                self.assertTrue(waited, 'recorder killed the child without waiting after EOF')
                self.assertEqual(record['exit_code'], expected)
                self.assertEqual(record['capture_status'], status)
                self.assertEqual(result, result_code)
                self.assertTrue(record['eof'])
                self.assertEqual(record['stop_reason'], 'deadline' if outcome == 'deadline' else
                                 'capture' if outcome == 'interrupt' else 'eof')
                self.assertIn(b'before eof', b''.join(bytes.fromhex(e['hex']) for e in record['events']))
                self.assertFalse(Path(f'/proc/{waited[0]}').exists())

    def test_exit_first_observable_after_deadline_is_not_completion(self):
        child = object.__new__(PtyProcess)
        child.pid = 123  # virtual child: all process observation is mocked
        child.eof = True
        child.read = lambda _: b''
        now = 0
        exited = object()

        def sleep(_):
            nonlocal now
            now = 2  # scheduling resumes after the deadline; exit time is unknown

        with patch('wake_probe.time.monotonic', side_effect=lambda: now), \
                patch('wake_probe.time.sleep', sleep), patch('os.waitid', side_effect=[None, exited]):
            self.assertFalse(child.wait_for_exit(1))
            # An exit available now must not retroactively certify the window.
            self.assertIs(os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT), exited)

    def test_deadline_with_exited_leader_and_live_descendant_is_stopped(self):
        output = Path(self.tmp.name, 'descendant.json')
        code = '''
import os, signal, time
r, w = os.pipe()
if os.fork() == 0:
    os.close(r)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    os.write(w, b'R')
    os.close(w)
    print('descendant ready', flush=True)
    time.sleep(30)
else:
    os.close(w)
    os.read(r, 1)
    os.close(r)
    os._exit(0)
'''
        original_read, monotonic = PtyProcess.read, time.monotonic
        advance = 0

        def read(child, timeout):
            nonlocal advance
            data = original_read(child, timeout)
            if data:
                # Force the deadline only after proving leader exit, without a
                # timeout assumption about how quickly fork/exec must finish.
                end = monotonic() + 5
                while os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is None:
                    if monotonic() >= end:
                        self.fail('fixture leader did not exit')
                    original_read(child, .01)
                advance = 120
            return data

        args = ['wake_probe', '--output', str(output), '--seconds', '60', '--',
                sys.executable, '-u', '-c', code]
        with patch.object(sys, 'argv', args), patch.object(PtyProcess, 'read', read), \
                patch('wake_probe.time.monotonic', side_effect=lambda: monotonic() + advance):
            result = main()
        record = json.loads(output.read_text())
        self.assertEqual(record['stop_reason'], 'deadline')
        self.assertFalse(record['eof'])
        self.assertEqual(record['exit_code'], 0)
        self.assertFalse(record['cleanup_requested'])
        self.assertEqual(record['capture_status'], 'stopped')
        self.assertEqual(result, 0)

    def test_temporary_home_setup_failure_publishes_startup_evidence(self):
        output = Path(self.tmp.name, 'home-setup.json')
        args = ['wake_probe', '--output', str(output), '--', 'fixture']
        with patch.object(sys, 'argv', args), \
                patch('tempfile.TemporaryDirectory', side_effect=OSError('temporary root unavailable')):
            self.assertEqual(main(), 1)
        record = json.loads(output.read_text())
        self.assertEqual(record['capture_status'], 'failed')
        self.assertEqual(record['stop_reason'], 'startup')
        self.assertEqual(record['error'], 'OSError')
        self.assertIsNone(record['wait_status'])
        self.assertEqual(record['events'], [])

    def test_temporary_home_cleanup_preserves_interrupt_and_failure(self):
        cleanup = tempfile.TemporaryDirectory.cleanup
        for failure in ('interrupt', 'error'):
            with self.subTest(failure=failure):
                output = Path(self.tmp.name, f'home-cleanup-{failure}.json')
                original_handler = signal.getsignal(signal.SIGINT)

                def cleanup_then_fail(home):
                    cleanup(home)
                    if failure == 'interrupt':
                        signal.raise_signal(signal.SIGINT)
                    else:
                        raise OSError('home cleanup fixture')

                args = ['wake_probe', '--output', str(output), '--seconds', '60', '--',
                        sys.executable, '-u', '-c', "print('cleanup fixture', flush=True)"]
                with patch.object(sys, 'argv', args), \
                        patch.object(tempfile.TemporaryDirectory, 'cleanup', cleanup_then_fail):
                    self.assertEqual(main(), 130 if failure == 'interrupt' else 1)
                record = json.loads(output.read_text())
                self.assertEqual(record['capture_status'], 'complete' if failure == 'interrupt' else 'failed')
                self.assertEqual(record['exit_code'], 0)
                self.assertIn(b'cleanup fixture', b''.join(bytes.fromhex(e['hex']) for e in record['events']))
                self.assertEqual(signal.getsignal(signal.SIGINT), original_handler)

    def test_constructor_failure_produces_complete_failure_record(self):
        output = Path(self.tmp.name, 'startup.json')
        with patch.object(sys, 'argv', ['wake_probe', '--output', str(output), '--', 'fixture']), \
                patch('wake_probe.PtyProcess', side_effect=OSError('fixture startup failure')):
            result = main()
        self.assertEqual(result, 1)
        record = json.loads(output.read_text())
        self.assertEqual(record['capture_status'], 'failed')
        self.assertEqual(record['events'], [])
        self.assertIsNone(record['exit_code'])

    def test_limit_and_exit_are_bounded_and_child_is_reaped(self):
        child = self.spawn(code="print('x' * 100, flush=True)", max_bytes=16)
        with self.assertRaises(ValueError):
            self.until(child, b'xxx')
        pid = child.pid
        child.close()
        with self.assertRaises(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)
        self.assertFalse(Path(f'/proc/{pid}').exists())

    def test_reaping_errors_release_descriptors_and_keep_child_retryable(self):
        for operation in ('waitid', 'killpg', 'waitpid'):
            with self.subTest(operation=operation):
                child = self.spawn()
                self.until(child, b'READY')
                fd, pid = child.fd, child.pid
                with patch(f'os.{operation}', side_effect=OSError('injected cleanup failure')):
                    with self.assertRaises(OSError):
                        child.close()
                self.assertIsNone(child.fd)
                with self.assertRaises(OSError):
                    os.fstat(fd)
                self.assertIsNone(child.selector.get_map())
                self.assertEqual(child.pid, pid)
                child.close()
                self.assertIsNone(child.pid)
                with self.assertRaises(ChildProcessError):
                    os.waitpid(pid, os.WNOHANG)


if __name__ == '__main__':
    unittest.main()

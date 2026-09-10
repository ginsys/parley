"""Controlled subprocesses only: no installed host CLI is launched by these tests."""

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from wake_probe import PtyProcess, Trial, aggregate

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
        self.assertIn(b'synthetic fixture', b''.join(bytes.fromhex(e['hex']) for e in record['events']))
        original = output.read_bytes()
        ran = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        self.assertNotEqual(ran.returncode, 0)
        self.assertEqual(output.read_bytes(), original)

    def test_limit_and_exit_are_bounded_and_child_is_reaped(self):
        child = self.spawn(code="print('x' * 100, flush=True)", max_bytes=16)
        with self.assertRaises(ValueError):
            self.until(child, b'xxx')
        pid = child.pid
        child.close()
        with self.assertRaises(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)
        self.assertFalse(Path(f'/proc/{pid}').exists())


if __name__ == '__main__':
    unittest.main()

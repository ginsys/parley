"""Controlled checks of aggregation and failed-attempt retention; no host CLI launches."""

import json
import runpy
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from threading import Lock
from types import SimpleNamespace
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'scripts' / 'probe'))
import codex_matrix  # noqa: E402
import host_trials  # noqa: E402


class StateEvidenceTests(unittest.TestCase):
    def test_approval_guard_rejects_cleared_menu_with_unresolved_tool_call(self):
        menu = '\r\n'.join(('Would you like to run the following command?',
                            'PARLEY_APPROVAL_PREFLIGHT', '1. Yes, proceed (y)',
                            '2. No, and tell Codex what to do differently (esc)',
                            'Press enter to confirm or esc to cancel'))
        raw = menu.encode()
        client = SimpleNamespace(lock=Lock(), window=bytearray(raw), total=len(raw))
        call = dict(type='response_item', payload=dict(type='custom_tool_call', name='exec',
                    input='PARLEY_APPROVAL_PREFLIGHT', call_id='owned-call'))
        driver = SimpleNamespace(live_client_for=lambda _: client,
                                 _read_rollout=lambda _: [json.dumps(call)])
        codex_matrix.require_approval(driver, 'owned')
        client.window.extend(b'\x1b[2J\x1b[Hidle')
        client.total = len(client.window)
        with self.assertRaisesRegex(RuntimeError, 'no longer holds'):
            codex_matrix.require_approval(driver, 'owned')
        client.total += 1
        with self.assertRaisesRegex(RuntimeError, 'truncated'):
            codex_matrix.require_approval(driver, 'owned')

    def event(self, kind, stamp, turn='owned-turn'):
        return dict(type='event_msg', timestamp=stamp,
                    payload=dict(type=kind, turn_id=turn))

    def test_completed_turn_cannot_be_used_as_busy(self):
        start = self.event('task_started', '2026-09-14T06:00:00Z')
        end = self.event('task_complete', '2026-09-14T06:00:30Z')
        pending = codex_matrix.active_turn([start], 0)
        self.assertEqual(pending['turn_id'], 'owned-turn')
        self.assertIsNone(codex_matrix.active_turn([start, end], 0))
        stamp = host_trials.record_time(start)
        ended = host_trials.record_time(end)
        codex_matrix.verify_busy_interval([start, end], pending, stamp + 2, ended)
        for submitted, observed in ((ended + 1, ended), (stamp + 2, ended + 1)):
            with self.assertRaisesRegex(RuntimeError, 'exact preexisting turn'):
                codex_matrix.verify_busy_interval([start, end], pending, submitted, observed)

    def test_approval_requires_both_pending_call_and_complete_menu(self):
        screen = '\n'.join(('Would you like to run the following command?',
                            'PARLEY_APPROVAL_PREFLIGHT', '1. Yes, proceed (y)',
                            '2. No, and tell Codex what to do differently (esc)',
                            'Press enter to confirm or esc to cancel'))
        call = dict(type='response_item', payload=dict(type='custom_tool_call', name='exec',
                    input='PARLEY_APPROVAL_PREFLIGHT', call_id='owned-call'))
        done = dict(type='response_item', payload=dict(type='custom_tool_call_output',
                    call_id='owned-call'))
        self.assertEqual(codex_matrix.approval_pending([call], screen), dict(call_id='owned-call'))
        self.assertIsNone(codex_matrix.approval_pending([call, done], screen))
        self.assertIsNone(codex_matrix.approval_pending([call], screen.splitlines()[0]))


class OrchestrationTests(unittest.TestCase):
    def invoke(self, output, result):
        def controlled_run(argv, **kwargs):
            if argv == ['codex', '--version']:
                return subprocess.CompletedProcess(argv, 0, 'codex-cli 0.154.0\n', '')
            if argv[:3] == ['git', 'init', '--quiet']:
                return subprocess.CompletedProcess(argv, 0, '', '')
            raise AssertionError(f'host launch forbidden: {argv}')

        with tempfile.TemporaryDirectory(dir='/tmp') as cwd:
            argv = ['codex_matrix.py', '--repo', str(REPO), '--output-directory', str(output),
                    '--state', 'idle']
            with patch.object(sys, 'argv', argv), patch('subprocess.run', side_effect=controlled_run), \
                    patch('tempfile.mkdtemp', return_value=cwd), \
                    patch.object(host_trials, 'run_trial_with_cleanup', side_effect=result):
                runpy.run_path(str(Path(__file__).with_name('codex_matrix.py')), run_name='__main__')

    def test_three_trials_preserve_unobservable_instead_of_inventing_negative_evidence(self):
        calls = []

        def result(driver, **kwargs):
            calls.append(kwargs)
            now = time.time()
            return host_trials.TrialRun(
                session_id='synthetic', submitted_at=now - 20, accepted_at=now - 19,
                outcomes={'accepted': now - 19, 'visible': now - 18, 'turn_start': now - 17},
                state='idle', marker='PARLEY-PROBE-' + 'a' * 32,
                supported={name: True for name in host_trials.OUTCOME_NAMES},
                observable={name: name != 'ack' for name in host_trials.OUTCOME_NAMES})

        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            output = Path(directory) / 'result'
            self.invoke(output, result)
            aggregate = json.loads((output / 'aggregate.json').read_text())
            self.assertEqual(len(calls), 3)
            self.assertEqual(aggregate['aggregate'], dict(accepted='observed', visible='observed',
                                                        turn_start='observed', ack='unobservable'))
            self.assertEqual(len(aggregate['trials']), 3)

    def test_failed_attempt_is_retained_and_never_aggregated_or_replaced(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            output = Path(directory) / 'result'
            with self.assertRaisesRegex(RuntimeError, 'synthetic precondition failed'):
                self.invoke(output, RuntimeError('synthetic precondition failed'))
            self.assertFalse((output / 'aggregate.json').exists())
            self.assertFalse((output / 'trial-2').exists())
            records = [json.loads(line) for line in (output / 'trial-1' / 'journal.jsonl').read_text().splitlines()]
            self.assertEqual(records[-2]['kind'], 'attempt_failed')
            self.assertEqual(records[-1]['kind'], 'ownership_after_cleanup')
            self.assertEqual(records[-1]['owned'], [])


if __name__ == '__main__':
    unittest.main()

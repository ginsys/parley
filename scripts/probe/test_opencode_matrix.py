"""Controlled OpenCode state and observation fixtures; no installed host process."""

import copy
import json
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import opencode_matrix as matrix
from host_trials import TrialRun


def message(identity, role, created, text='', parent=None, completed=None):
    info = dict(id=identity, role=role, sessionID='owned', time=dict(created=created),
                providerID='opencode', modelID='ling-3.0-flash-fin-free')
    if parent:
        info['parentID'] = parent
    if completed is not None:
        info['time']['completed'] = completed
        info['finish'] = 'stop'
    return dict(info=info, parts=[dict(type='text', text=text)] if text else [])


class StateTests(unittest.TestCase):
    def test_published_approval_precondition_preserves_keys_and_exact_call_binding(self):
        artifact = Path(__file__).resolve().parents[2] / 'docs/evidence/host-wake/opencode-approval-20260914.json'
        for trial in json.loads(artifact.read_text())['trials']:
            evidence = next(row['evidence'] for row in trial['journal'] if row['kind'] == 'precondition')
            self.assertEqual(set(evidence), {'user_id', 'assistant_id', 'started_at', 'call_id'})
            for exported in trial['exports'].values():
                pending = next(m for m in exported['messages'] if m['info']['id'] == evidence['assistant_id'])
                self.assertEqual(pending['info']['parentID'], evidence['user_id'])
                self.assertEqual(pending['info']['time']['created'] / 1000, evidence['started_at'])
                calls = [part['callID'] for part in pending['parts'] if part['type'] == 'tool']
                self.assertEqual(calls, [evidence['call_id']])

    def approval(self):
        original = message('pending', 'assistant', 900000, parent='request')
        original['parts'] = [dict(type='tool', tool='bash', callID='call-owned',
                                 state=dict(status='running', input=dict(command=matrix.APPROVAL_COMMAND),
                                            time=dict(start=900001)))]
        document = dict(messages=[message('old-user', 'user', 800000, 'PONG'),
                                  message('old-answer', 'assistant', 800001, 'PONG', completed=800002),
                                  message('request', 'user', 899000, matrix.APPROVAL_PROMPT), original])
        screen = '\n'.join(('Permission required', 'Shell command', matrix.APPROVAL_COMMAND,
                            'Allow once', 'Allow always', 'Reject'))
        return document, screen

    def test_pending_approval_requires_exact_request_tool_and_current_menu(self):
        document, screen = self.approval()
        self.assertIsNotNone(matrix.approval_pending(document, screen))
        self.assertIsNone(matrix.approval_pending(document, 'idle'))
        for field, value in (('status', 'completed'), ('input', {'command': 'foreign'}),
                             ('output', 'PARLEY-PROBE-untrusted')):
            changed = copy.deepcopy(document)
            changed['messages'][-1]['parts'][0]['state'][field] = value
            self.assertIsNone(matrix.approval_pending(changed, screen))

    def test_blocked_old_tool_cannot_supply_new_turn_or_ack(self):
        document, screen = self.approval()
        precondition = matrix.approval_pending(document, screen)
        document['messages'].append(message('marker', 'user', 1001000, 'FRESH-MARKER'))
        result = matrix.observed_export(document, marker='FRESH-MARKER', submitted_at=1000,
                                        state='approval', precondition=precondition)
        self.assertTrue(result.observable)
        self.assertEqual(result.outcomes, {'visible': 1001})
        self.assertIsNone(result.model)

    def test_busy_completion_is_exact_and_old_assistant_is_not_new_turn(self):
        document = dict(messages=[message('request', 'user', 998000, matrix.BUSY_PROMPT),
                                  message('old', 'assistant', 999000, 'HOLD COMPLETE',
                                          parent='request', completed=1010000),
                                  message('marker', 'user', 1001000, 'FRESH-MARKER')])
        precondition = dict(user_id='request', assistant_id='old', started_at=999)
        result = matrix.observed_export(document, marker='FRESH-MARKER', submitted_at=1000,
                                        state='busy', precondition=precondition)
        self.assertEqual(result.turn_end, 1010)
        self.assertEqual(result.outcomes, {'visible': 1001})
        document['messages'].append(message('new', 'assistant', 1011000, 'FRESH-MARKER',
                                           parent='marker', completed=1012000))
        result = matrix.observed_export(document, marker='FRESH-MARKER', submitted_at=1000,
                                        state='busy', precondition=precondition)
        self.assertEqual(result.outcomes, {'visible': 1001, 'turn_start': 1011, 'ack': 1012})
        for submitted in (998, 1010, 1011):
            with self.assertRaisesRegex(RuntimeError, 'inside'):
                matrix.busy_end(document, precondition, submitted)
        document['messages'][1]['info']['parentID'] = 'foreign'
        with self.assertRaisesRegex(RuntimeError, 'identity'):
            matrix.busy_end(document, precondition, 1000)


class OrchestrationTests(unittest.TestCase):
    def test_acceptance_wait_rejects_forward_and_backward_clock_steps(self):
        for step in (5, -5):
            with self.subTest(step=step), tempfile.TemporaryDirectory(dir='/tmp') as directory:
                mono = [100.0]
                wall = [1000.0]

                def sleep(seconds):
                    mono[0] += seconds
                    wall[0] += seconds + step

                def rejected(driver, **kwargs):
                    return TrialRun(session_id='owned', submitted_at=1000, accepted_at=None,
                                    outcomes={}, state='idle', marker='synthetic', supported={},
                                    observable={'accepted': True, 'visible': False,
                                                'turn_start': False, 'ack': False})

                with patch.object(matrix.time, 'monotonic', side_effect=lambda: mono[0]), \
                        patch.object(matrix.time, 'time', side_effect=lambda: wall[0]), \
                        patch.object(matrix.time, 'sleep', side_effect=sleep):
                    with self.assertRaisesRegex(RuntimeError, 'clock stepped'):
                        self.invoke(Path(directory), rejected)
                self.assertFalse((Path(directory) / 'capture/aggregate.json').exists())
                rows = list(map(json.loads, (Path(directory) / 'capture/trial-1/journal.jsonl').read_text().splitlines()))
                self.assertFalse(any(row['kind'] == 'classified' for row in rows))
                self.assertEqual(rows[-2]['kind'], 'attempt_failed')
                self.assertEqual(mono[0], 110)

    def test_fast_live_rejection_waits_for_acceptance_without_inventing_visibility(self):
        now = [1000.0]

        def rejected(driver, **kwargs):
            return TrialRun(session_id='synthetic-owned', submitted_at=now[0], accepted_at=None,
                            outcomes={}, state='idle', marker='synthetic-marker',
                            supported={key: True for key in ('accepted', 'visible', 'turn_start', 'ack')},
                            observable={key: key == 'accepted' for key in ('accepted', 'visible', 'turn_start', 'ack')},
                            turn_end_observable=False)

        def elapsed(seconds):
            self.assertGreater(seconds, 0)
            now[0] += seconds

        with tempfile.TemporaryDirectory(dir='/tmp') as directory, \
                patch.object(matrix.time, 'time', side_effect=lambda: now[0]), \
                patch.object(matrix.time, 'monotonic', side_effect=lambda: now[0]), \
                patch.object(matrix.time, 'sleep', side_effect=elapsed):
            output = self.invoke(Path(directory), rejected)
            data = json.loads((output / 'aggregate.json').read_text())
            self.assertEqual(data['aggregate'], {'accepted': 'not_observed', 'visible': 'unobservable',
                                                  'turn_start': 'unobservable', 'ack': 'unobservable'})
            self.assertEqual(now[0], 1030)

    def test_disconnected_rejection_keeps_polling_and_records_acceptance_independently(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            root = Path(directory)
            cwd = root / 'cwd'
            cwd.mkdir(mode=0o700)
            output = root / 'capture'
            calls = []

            def run(argv, **kwargs):
                if argv == ['opencode', '--version']:
                    return subprocess.CompletedProcess(argv, 0, '1.18.30\n', '')
                if '--attach' in argv:
                    calls.append(argv)
                    return subprocess.CompletedProcess(argv, 1, '', 'Error: Session not found\n')
                raise AssertionError('ordinary test must not launch a host')

            def serve(driver):
                driver.server = SimpleNamespace(url='http://127.0.0.1:12345')

            def close(driver):
                driver.server = None
                return []

            def trial(driver, **kwargs):
                identity = f'owned-{len(calls)}'
                driver.mint(identity)
                kwargs['settle'](identity)
                # None keeps run_trial on its observation path; False would skip it.
                self.assertIsNone(driver.submit(identity, 'synthetic marker'))
                driver.release(identity)
                return TrialRun(session_id=identity, submitted_at=time.time() - 130,
                                accepted_at=None, outcomes={}, state='disconnected', marker='synthetic',
                                supported={key: True for key in ('accepted', 'visible', 'turn_start', 'ack')},
                                observable={key: key != 'accepted' for key in ('accepted', 'visible', 'turn_start', 'ack')})

            with patch.object(sys, 'argv', ['opencode_matrix.py', '--state', 'disconnected',
                                           '--output-directory', str(output)]), \
                    patch('subprocess.run', side_effect=run), patch('tempfile.mkdtemp', return_value=str(cwd)), \
                    patch.object(matrix.OpenCodeDriver, 'serve', serve), \
                    patch.object(matrix.OpenCodeDriver, 'close_servers', close), \
                    patch.object(matrix, 'run_trial_with_cleanup', side_effect=trial):
                matrix.main()
            data = json.loads((output / 'aggregate.json').read_text())
            self.assertEqual(len(calls), 3)
            self.assertEqual(set(data['aggregate'].values()), {'not_observed'})

    def invoke(self, directory, result):
        cwd = directory / 'cwd'
        cwd.mkdir(mode=0o700)
        output = directory / 'capture'

        def run(argv, **kwargs):
            if argv == ['opencode', '--version']:
                return subprocess.CompletedProcess(argv, 0, '1.18.30\n', '')
            raise AssertionError('ordinary test must not launch a host')

        with patch.object(sys, 'argv', ['opencode_matrix.py', '--state', 'idle',
                                       '--output-directory', str(output)]), \
                patch('subprocess.run', side_effect=run), patch('tempfile.mkdtemp', return_value=str(cwd)), \
                patch.object(matrix, 'run_trial_with_cleanup', side_effect=result):
            matrix.main()
        return output

    def test_three_trials_and_failed_attempt_retention(self):
        calls = []

        def result(driver, **kwargs):
            calls.append(driver)
            return TrialRun(session_id=f'synthetic-{len(calls)}', submitted_at=time.time() - 130,
                            accepted_at=None, outcomes={}, state='idle', marker='PARLEY-PROBE-' + 'a' * 32,
                            supported={key: True for key in ('accepted', 'visible', 'turn_start', 'ack')},
                            observable={key: False for key in ('accepted', 'visible', 'turn_start', 'ack')})

        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            output = self.invoke(Path(directory), result)
            data = json.loads((output / 'aggregate.json').read_text())
            self.assertEqual(len(calls), 3)
            self.assertEqual(set(data['aggregate'].values()), {'unobservable'})
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            with self.assertRaisesRegex(RuntimeError, 'synthetic failed state'):
                self.invoke(Path(directory), RuntimeError('synthetic failed state'))
            output = Path(directory) / 'capture'
            self.assertFalse((output / 'aggregate.json').exists())
            self.assertFalse((output / 'trial-2').exists())
            rows = list(map(json.loads, (output / 'trial-1/journal.jsonl').read_text().splitlines()))
            self.assertEqual(rows[-2]['kind'], 'attempt_failed')
            self.assertEqual(rows[-1]['kind'], 'ownership_after_cleanup')


if __name__ == '__main__':
    unittest.main()

"""Controlled OpenCode state and observation fixtures; no installed host process."""

import copy
import json
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
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

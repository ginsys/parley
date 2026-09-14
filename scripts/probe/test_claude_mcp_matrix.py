"""Controlled state evidence only; no installed host process is launched."""

import json
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from threading import Lock
from types import SimpleNamespace
from unittest.mock import patch

import claude_mcp_matrix
from claude_mcp_matrix import (
    HOLD_PROMPT,
    approval_pending,
    busy_completion,
    capture_transcript,
    terminal_screen,
    verify_binaries,
)
from host_trials import ClaudeDriver, ForeignSessionError, SessionRegistry, TrialRun


class StateEvidenceTests(unittest.TestCase):
    def test_resume_copy_is_bound_and_captured_without_overwriting_original(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            root = Path(directory)
            cwd = root / 'cwd'
            cwd.mkdir(mode=0o700)
            driver = ClaudeDriver(SessionRegistry(), cwd=str(cwd))
            ids = ('11111111', '22222222')
            for short_id in ids:
                driver._mint(short_id)
                (root / f'{short_id}-source').write_text(short_id)
            bound = []

            def status(short_id):
                driver.require_owned(short_id)
                bound.append(short_id)
                driver.sessions[short_id] = short_id + '-full-uuid'

            with patch.object(driver, 'status', side_effect=status), \
                    patch.object(driver, 'transcript_path_for',
                                 side_effect=lambda full: root / f'{full[:8]}-source'):
                targets = [capture_transcript(driver, short_id, root) for short_id in ids]
            self.assertEqual(bound, list(ids))
            self.assertNotEqual(*targets)
            self.assertEqual([target.read_text() for target in targets], list(ids))
            with self.assertRaises(ForeignSessionError):
                capture_transcript(driver, 'foreign', root)

    def test_native_attach_binary_is_independently_pinned(self):
        for native_result in (subprocess.CompletedProcess([], 0, '2.1.271 (Claude Code)', ''),
                              subprocess.CompletedProcess([], 1, '', 'missing')):
            calls = []

            def run(argv, **kwargs):
                calls.append(argv)
                return (subprocess.CompletedProcess(argv, 0, '2.1.270 (Claude Code)', '')
                        if argv[0] == 'claude' else native_result)

            with self.assertRaisesRegex(RuntimeError, 'version changed'):
                verify_binaries(run, Path('/synthetic/native/claude'))
            self.assertEqual(calls, [['claude', '--version'], ['/synthetic/native/claude', '--version']])

    def test_explicit_single_trial_never_publishes_three_trial_aggregate(self):
        def controlled_run(argv, **kwargs):
            if argv in (['claude', '--version'], [str(Path.home() / '.local/bin/claude'), '--version']):
                return subprocess.CompletedProcess(argv, 0, '2.1.270 (Claude Code)\n', '')
            raise AssertionError(f'host launch forbidden: {argv}')

        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            root = Path(directory)
            cwd = root / 'cwd'
            cwd.mkdir(mode=0o700)
            output = root / 'capture'
            trial = TrialRun(session_id='synthetic', submitted_at=time.time() - 130,
                             accepted_at=None, outcomes={}, state='idle', marker='PARLEY-PROBE-' + 'a' * 32,
                             supported={key: True for key in ('accepted', 'visible', 'turn_start', 'ack')},
                             observable={key: key != 'accepted' for key in ('accepted', 'visible', 'turn_start', 'ack')})
            argv = ['claude_mcp_matrix.py', '--output-directory', str(output), '--state', 'idle', '--trials', '1']
            with patch.object(sys, 'argv', argv), patch('subprocess.run', side_effect=controlled_run), \
                    patch('tempfile.mkdtemp', return_value=str(cwd)), \
                    patch.object(claude_mcp_matrix, 'run_trial_with_cleanup', return_value=trial):
                claude_mcp_matrix.main()
            self.assertFalse((output / 'aggregate.json').exists())
            self.assertEqual(len(json.loads((output / 'single-trial.json').read_text())['trials']), 1)
            self.assertFalse((output / 'trial-2').exists())

    def test_terminal_rendering_retains_cursor_reused_letters_and_clears_old_menu(self):
        raw = b'Esc to cancel\r\x1b[2C\x1b[3C\x1b[4C'
        client = SimpleNamespace(lock=Lock(), window=bytearray(raw), total=len(raw))
        self.assertIn('Esc to cancel', terminal_screen(client))
        client.window.extend(b'\x1b[2J\x1b[Hidle')
        client.total = len(client.window)
        self.assertNotIn('Esc to cancel', terminal_screen(client))
        client.total += 1
        with self.assertRaisesRegex(RuntimeError, 'truncated'):
            terminal_screen(client)

    def test_completion_requires_ancestry_and_exact_binding(self):
        unrelated = dict(type='system', subtype='turn_duration', parentUuid='other', uuid='other-end',
                         sessionId='owned', cwd='/synthetic', timestamp='2026-09-14T00:00:01Z')
        end = dict(unrelated, parentUuid='busy-user', uuid='end')
        self.assertIsNone(busy_completion([unrelated], 'busy-user', 'owned', '/synthetic'))
        self.assertIsNotNone(busy_completion([unrelated, end], 'busy-user', 'owned', '/synthetic'))
        for field in ('sessionId', 'cwd'):
            with self.subTest(field=field), self.assertRaisesRegex(RuntimeError, 'binding'):
                busy_completion([dict(end, **{field: 'foreign'})], 'busy-user', 'owned', '/synthetic')

    def test_approval_menu_alone_is_not_a_bound_request(self):
        menu = 'parleyprobe — Hold Tool: (MCP)\nDo you want to proceed?\n1. Yes\n3. No\nEsc to cancel'
        request = dict(type='user', uuid='request', message=dict(content=HOLD_PROMPT))
        self.assertEqual(approval_pending([request], menu), 'request')
        self.assertEqual(approval_pending([request], menu.replace('Esc', 'Ec')), 'request')
        self.assertIsNone(approval_pending([], menu))
        self.assertIsNone(approval_pending([request], 'parleyprobe'))


if __name__ == '__main__':
    unittest.main()

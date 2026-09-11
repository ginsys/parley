"""Controlled fixtures only: no test here launches an installed Claude, Codex or OpenCode CLI."""

import json
import os
import tempfile
import unittest
from dataclasses import dataclass

from host_trials import (
    ClaudeDriver,
    CodexDriver,
    Event,
    ForeignSessionError,
    OpenCodeDriver,
    SessionRegistry,
    background_sessions,
    classify_trial,
    codex_rollout_events,
    detect_outcomes,
    marker_token,
    parse_claude_transcript,
    run_trial,
)
from wake_probe import Trial, aggregate

MARKER = 'PARLEY-PROBE-deadbeefdeadbeefdeadbeefdeadbeef'


@dataclass
class FakeResult:
    returncode: int
    stdout: str = ''
    stderr: str = ''


class RegistryTests(unittest.TestCase):
    def test_mint_then_require_owned_then_release(self):
        registry = SessionRegistry()
        registry.mint('a')
        registry.require_owned('a')  # does not raise
        registry.release('a')
        with self.assertRaises(ForeignSessionError):
            registry.require_owned('a')

    def test_foreign_session_is_refused(self):
        registry = SessionRegistry()
        registry.mint('a')
        with self.assertRaises(ForeignSessionError):
            registry.require_owned('some-real-background-session-id')

    def test_duplicate_mint_and_empty_id_are_rejected(self):
        registry = SessionRegistry()
        registry.mint('a')
        with self.assertRaises(ValueError):
            registry.mint('a')
        with self.assertRaises(ValueError):
            registry.mint('')

    def test_release_of_foreign_session_is_refused(self):
        registry = SessionRegistry()
        with self.assertRaises(ForeignSessionError):
            registry.release('never-created')


class DetectOutcomesTests(unittest.TestCase):
    def test_marker_visible_then_acknowledged(self):
        events = [
            Event(role='user', text=f'do the thing {MARKER}', time=10.0),
            Event(role='assistant', text=f'ok, saw {MARKER}', time=11.0),
        ]
        outcomes = detect_outcomes(events, MARKER, submitted_at=9.0)
        self.assertEqual(outcomes, {'visible': 10.0, 'turn_start': 11.0, 'ack': 11.0})

    def test_turn_starts_without_acknowledging_the_marker(self):
        events = [
            Event(role='user', text=MARKER, time=10.0),
            Event(role='assistant', text='unrelated reply', time=11.0),
        ]
        outcomes = detect_outcomes(events, MARKER, submitted_at=9.0)
        self.assertEqual(outcomes, {'visible': 10.0, 'turn_start': 11.0})
        self.assertNotIn('ack', outcomes)

    def test_events_before_submission_are_ignored(self):
        events = [
            Event(role='user', text=MARKER, time=1.0),
            Event(role='assistant', text=MARKER, time=2.0),
        ]
        self.assertEqual(detect_outcomes(events, MARKER, submitted_at=5.0), {})

    def test_developer_role_is_never_a_signal(self):
        events = [Event(role='developer', text=MARKER, time=10.0)]
        self.assertEqual(detect_outcomes(events, MARKER, submitted_at=9.0), {})

    def test_turn_start_uses_first_assistant_event_ack_uses_first_marker_match(self):
        events = [
            Event(role='assistant', text='thinking...', time=11.0),
            Event(role='assistant', text=f'done, {MARKER}', time=12.0),
        ]
        outcomes = detect_outcomes(events, MARKER, submitted_at=9.0)
        self.assertEqual(outcomes, {'turn_start': 11.0, 'ack': 12.0})

    def test_untimed_events_are_treated_as_within_window(self):
        events = [Event(role='assistant', text=MARKER, time=None)]
        outcomes = detect_outcomes(events, MARKER, submitted_at=9.0)
        self.assertEqual(outcomes, {'turn_start': 9.0, 'ack': 9.0})


class ClaudeParsingTests(unittest.TestCase):
    # Field names captured from a real `claude agents --json --all` / `claude agents --json`
    # run: background sessions report `state`; interactive sessions report `status` instead.
    RAW = json.dumps([
        {'id': 'e9f3bf35', 'kind': 'background', 'state': 'done', 'cwd': '/x'},
        {'pid': 1, 'kind': 'interactive', 'status': 'idle', 'sessionId': 'abc', 'cwd': '/y'},
    ])

    def test_background_sessions_filters_out_interactive_schema(self):
        sessions = background_sessions(self.RAW)
        self.assertEqual([s['id'] for s in sessions], ['e9f3bf35'])

    def test_transcript_parses_role_prefixed_multiline_blocks(self):
        raw = f'User: hello {MARKER}\ncontinued\nAssistant: got it\nstill talking\n'
        events = parse_claude_transcript(raw, marker=MARKER)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].role, 'user')
        self.assertEqual(events[0].text, f'hello {MARKER}\ncontinued')
        self.assertEqual(events[1].role, 'assistant')
        self.assertEqual(events[1].text, 'got it\nstill talking')

    def test_transcript_without_marker_does_not_raise(self):
        events = parse_claude_transcript('User: hi\nAssistant: hello\n', marker=MARKER)
        self.assertEqual(len(events), 2)


class CodexParsingTests(unittest.TestCase):
    def test_rollout_extracts_user_and_assistant_skips_developer_and_other_types(self):
        lines = [
            json.dumps({'timestamp': '2026-09-11T00:00:00.000Z',
                        'payload': {'type': 'message', 'role': 'developer',
                                    'content': [{'type': 'input_text', 'text': 'system prompt'}]}}),
            json.dumps({'timestamp': '2026-09-11T00:00:01.000Z',
                        'payload': {'type': 'message', 'role': 'user',
                                    'content': [{'type': 'input_text', 'text': MARKER}]}}),
            json.dumps({'timestamp': '2026-09-11T00:00:02.000Z', 'payload': {'type': 'event_msg'}}),
            json.dumps({'timestamp': '2026-09-11T00:00:03.000Z',
                        'payload': {'type': 'message', 'role': 'assistant',
                                    'content': [{'type': 'output_text', 'text': f'ack {MARKER}'}]}}),
        ]
        events = codex_rollout_events(lines)
        self.assertEqual([(e.role, e.text) for e in events],
                          [('user', MARKER), ('assistant', f'ack {MARKER}')])
        self.assertLess(events[0].time, events[1].time)

    def test_malformed_and_non_message_lines_are_skipped_not_fatal(self):
        lines = ['not json', json.dumps({'payload': {'type': 'world_state'}}), '']
        self.assertEqual(codex_rollout_events(lines), [])


class ClaudeDriverTests(unittest.TestCase):
    def test_create_mints_the_printed_session_id(self):
        registry = SessionRegistry()
        calls = []

        def fake_run(argv, **kwargs):
            calls.append(argv)
            return FakeResult(0, stdout='abcd1234\n')

        driver = ClaudeDriver(registry, run=fake_run, cwd='/scratch')
        session_id = driver.create('probe prompt')
        self.assertEqual(session_id, 'abcd1234')
        self.assertIn('abcd1234', registry.created)
        self.assertEqual(calls[0][:3], ['claude', '--bg', '--cwd'])

    def test_create_raises_on_nonzero_exit(self):
        driver = ClaudeDriver(SessionRegistry(), run=lambda *a, **k: FakeResult(1, stderr='boom'), cwd='/scratch')
        with self.assertRaises(RuntimeError):
            driver.create('probe prompt')

    def test_submit_refuses_a_foreign_session(self):
        driver = ClaudeDriver(SessionRegistry(), run=lambda *a, **k: FakeResult(0, stdout='[]'), cwd='/scratch')
        with self.assertRaises(ForeignSessionError):
            driver.submit('not-mine', 'msg')

    def test_submit_confirms_listing_then_rejects_a_session_absent_from_it(self):
        registry = SessionRegistry()
        registry.mint('abcd1234')
        present = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(
            0, stdout=json.dumps([{'id': 'abcd1234', 'kind': 'background', 'state': 'idle'}])), cwd='/scratch')
        self.assertTrue(present.submit('abcd1234', 'msg'))

        absent = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(0, stdout='[]'), cwd='/scratch')
        with self.assertRaises(ValueError):
            absent.submit('abcd1234', 'msg')

    def test_observe_returns_empty_when_logs_unreachable(self):
        # Reproduces the real observation: `claude logs <id>` fails once a `done` session's
        # daemon socket is gone (ENOENT). A dead observation channel is not a negative result.
        registry = SessionRegistry()
        registry.mint('abcd1234')
        driver = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(1, stderr='connect ENOENT'), cwd='/scratch')
        self.assertEqual(driver.observe('abcd1234', marker=MARKER, submitted_at=0.0), {})

    def test_observe_parses_a_reachable_transcript(self):
        registry = SessionRegistry()
        registry.mint('abcd1234')
        raw = f'User: {MARKER}\nAssistant: ack {MARKER}\n'
        driver = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(0, stdout=raw), cwd='/scratch')
        outcomes = driver.observe('abcd1234', marker=MARKER, submitted_at=0.0)
        self.assertEqual(set(outcomes), {'visible', 'turn_start', 'ack'})

    def test_teardown_releases_the_registry_entry(self):
        registry = SessionRegistry()
        registry.mint('abcd1234')
        driver = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(0), cwd='/scratch')
        driver.teardown('abcd1234')
        with self.assertRaises(ForeignSessionError):
            registry.require_owned('abcd1234')


class CodexDriverTests(unittest.TestCase):
    def test_register_existing_mints_then_submit_uses_the_thread_id(self):
        registry = SessionRegistry()
        calls = []

        def fake_run(argv, **kwargs):
            calls.append(argv)
            return FakeResult(0)

        driver = CodexDriver(registry, run=fake_run, rollout_path_for=lambda _id: None)
        driver.register_existing('thread-1')
        self.assertTrue(driver.submit('thread-1', MARKER))
        self.assertEqual(calls[0], ['codex', 'queue', '--thread', 'thread-1', '--message', MARKER])

    def test_submit_refuses_a_foreign_thread(self):
        driver = CodexDriver(SessionRegistry(), run=lambda *a, **k: FakeResult(0), rollout_path_for=lambda _id: None)
        with self.assertRaises(ForeignSessionError):
            driver.submit('not-mine', MARKER)

    def test_observe_reads_the_rollout_file(self):
        registry = SessionRegistry()
        registry.mint('thread-1')
        line = json.dumps({'timestamp': '2026-09-11T00:00:01.000Z',
                            'payload': {'type': 'message', 'role': 'assistant',
                                        'content': [{'type': 'output_text', 'text': f'ack {MARKER}'}]}})
        with tempfile.NamedTemporaryFile('w', suffix='.jsonl', delete=False) as handle:
            handle.write(line + '\n')
            path = handle.name
        try:
            driver = CodexDriver(registry, run=lambda *a, **k: FakeResult(0), rollout_path_for=lambda _id: path)
            outcomes = driver.observe('thread-1', marker=MARKER, submitted_at=0.0)
            self.assertEqual(set(outcomes), {'turn_start', 'ack'})
        finally:
            os.unlink(path)

    def test_observe_returns_empty_without_a_rollout_path(self):
        registry = SessionRegistry()
        registry.mint('thread-1')
        driver = CodexDriver(registry, run=lambda *a, **k: FakeResult(0), rollout_path_for=lambda _id: None)
        self.assertEqual(driver.observe('thread-1', marker=MARKER, submitted_at=0.0), {})


class OpenCodeDriverTests(unittest.TestCase):
    def test_instantiation_refuses_until_export_format_is_captured(self):
        with self.assertRaises(NotImplementedError):
            OpenCodeDriver()


class RunTrialTests(unittest.TestCase):
    def test_accepted_submission_is_merged_into_observed_outcomes(self):
        class FakeDriver:
            def create(self, prompt):
                return 'sid'

            def submit(self, session_id, marker):
                assert session_id == 'sid'
                return True

            def observe(self, session_id, *, marker, submitted_at):
                return {'visible': submitted_at}

        session_id, outcomes = run_trial(FakeDriver(), prompt='hi', marker=MARKER)
        self.assertEqual(session_id, 'sid')
        self.assertEqual(set(outcomes), {'visible', 'accepted'})

    def test_settle_runs_between_create_and_submit(self):
        order = []

        class FakeDriver:
            def create(self, prompt):
                order.append('create')
                return 'sid'

            def submit(self, session_id, marker):
                order.append('submit')
                return False

            def observe(self, session_id, *, marker, submitted_at):
                return {}

        run_trial(FakeDriver(), prompt='hi', marker=MARKER, settle=lambda: order.append('settle'))
        self.assertEqual(order, ['create', 'settle', 'submit'])

    def test_rejected_submission_does_not_add_accepted(self):
        class FakeDriver:
            def create(self, prompt):
                return 'sid'

            def submit(self, session_id, marker):
                return False

            def observe(self, session_id, *, marker, submitted_at):
                return {}

        _, outcomes = run_trial(FakeDriver(), prompt='hi', marker=MARKER)
        self.assertNotIn('accepted', outcomes)


class ClassifyTrialTests(unittest.TestCase):
    def test_open_window_raises_rather_than_defaulting(self):
        trial = Trial(submitted=0.0, outcomes={})
        with self.assertRaises(ValueError):
            classify_trial(trial, now=5.0)  # 'ack' window (120s) still open at t=5

    def test_supported_and_observable_overrides_short_circuit_the_window_check(self):
        trial = Trial(submitted=0.0, outcomes={})
        result = classify_trial(trial, now=5.0,
                                 supported={o: False for o in ('accepted', 'visible', 'turn_start', 'ack')})
        self.assertTrue(all(v == 'unsupported' for v in result.values()))

    def test_three_classified_trials_with_mixed_results_aggregate_to_inconclusive(self):
        now = 1000.0
        observed = Trial(submitted=0.0, outcomes={'ack': 5.0})
        not_observed = Trial(submitted=0.0, outcomes={})
        results = [classify_trial(observed, now)['ack'],
                   classify_trial(observed, now)['ack'],
                   classify_trial(not_observed, now)['ack']]
        self.assertEqual(results, ['observed', 'observed', 'not_observed'])
        self.assertEqual(aggregate(results), 'inconclusive')


class MarkerTokenTests(unittest.TestCase):
    def test_tokens_are_unique_and_prefixed(self):
        first, second = marker_token(), marker_token()
        self.assertNotEqual(first, second)
        self.assertTrue(first.startswith('PARLEY-PROBE-'))


if __name__ == '__main__':
    unittest.main()

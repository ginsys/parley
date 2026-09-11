"""Controlled fixtures only: no test here launches an installed Claude, Codex or OpenCode CLI."""

import datetime
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
    Observation,
    OpenCodeDriver,
    SessionRegistry,
    SubmissionUnsupported,
    background_sessions,
    classify_trial,
    codex_rollout_events,
    detect_outcomes,
    marker_token,
    parse_claude_transcript,
    rollout_started_at,
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
        observation = detect_outcomes(events, MARKER, submitted_at=9.0)
        self.assertEqual(observation.outcomes, {'visible': 10.0, 'turn_start': 11.0, 'ack': 11.0})
        self.assertTrue(observation.observable)

    def test_turn_starts_without_acknowledging_the_marker(self):
        events = [
            Event(role='user', text=MARKER, time=10.0),
            Event(role='assistant', text='unrelated reply', time=11.0),
        ]
        outcomes = detect_outcomes(events, MARKER, submitted_at=9.0).outcomes
        self.assertEqual(outcomes, {'visible': 10.0, 'turn_start': 11.0})
        self.assertNotIn('ack', outcomes)

    def test_events_before_submission_are_ignored(self):
        events = [
            Event(role='user', text=MARKER, time=1.0),
            Event(role='assistant', text=MARKER, time=2.0),
        ]
        self.assertEqual(detect_outcomes(events, MARKER, submitted_at=5.0).outcomes, {})

    def test_developer_role_is_never_a_signal(self):
        events = [Event(role='developer', text=MARKER, time=10.0)]
        self.assertEqual(detect_outcomes(events, MARKER, submitted_at=9.0).outcomes, {})

    def test_turn_start_uses_first_assistant_event_ack_uses_first_marker_match(self):
        events = [
            Event(role='assistant', text='thinking...', time=11.0),
            Event(role='assistant', text=f'done, {MARKER}', time=12.0),
        ]
        self.assertEqual(detect_outcomes(events, MARKER, submitted_at=9.0).outcomes,
                          {'turn_start': 11.0, 'ack': 12.0})

    def test_untimed_events_are_never_promoted_into_the_window(self):
        # An undated entry cannot be ordered against submission: counting it would let a
        # pre-submission turn (a creation prompt's own reply, an old rollout record) fabricate
        # turn_start/ack for this trial. Fail closed, and say the read proved nothing.
        events = [Event(role='assistant', text=MARKER, time=None)]
        observation = detect_outcomes(events, MARKER, submitted_at=9.0)
        self.assertEqual(observation.outcomes, {})
        self.assertFalse(observation.observable)

    def test_a_dated_positive_survives_alongside_an_undated_entry(self):
        events = [Event(role='assistant', text='old', time=None),
                  Event(role='user', text=MARKER, time=10.0)]
        observation = detect_outcomes(events, MARKER, submitted_at=9.0)
        self.assertEqual(observation.outcomes, {'visible': 10.0})
        self.assertFalse(observation.observable)


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
        events, undated = codex_rollout_events(lines)
        self.assertEqual([(e.role, e.text) for e in events],
                          [('user', MARKER), ('assistant', f'ack {MARKER}')])
        self.assertLess(events[0].time, events[1].time)
        self.assertEqual(undated, 0)

    def test_malformed_and_non_message_lines_are_skipped_not_fatal(self):
        lines = ['not json', json.dumps({'payload': {'type': 'world_state'}}), '']
        self.assertEqual(codex_rollout_events(lines), ([], 0))

    def test_message_records_without_a_usable_timestamp_are_dropped_and_counted(self):
        lines = [
            json.dumps({'payload': {'type': 'message', 'role': 'assistant',
                                    'content': [{'type': 'output_text', 'text': 'undated'}]}}),
            json.dumps({'timestamp': 'not-a-date',
                        'payload': {'type': 'message', 'role': 'user',
                                    'content': [{'type': 'input_text', 'text': 'malformed'}]}}),
        ]
        self.assertEqual(codex_rollout_events(lines), ([], 2))

    def test_rollout_started_at_takes_the_earliest_record_of_any_type(self):
        lines = [
            json.dumps({'timestamp': '2026-09-11T00:00:05.000Z', 'payload': {'type': 'message'}}),
            json.dumps({'timestamp': '2026-09-11T00:00:01.000Z', 'type': 'session_meta'}),
            'not json',
        ]
        expected = datetime.datetime(2026, 9, 11, 0, 0, 1, tzinfo=datetime.UTC).timestamp()
        self.assertEqual(rollout_started_at(lines), expected)
        self.assertIsNone(rollout_started_at(['not json', '']))


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

    def test_create_reports_empty_output_instead_of_indexing_it(self):
        # Exit 0 with no printed id: a background session may be running untracked, which is
        # worth naming. Indexing splitlines()[-1] here raised a bare IndexError instead.
        driver = ClaudeDriver(SessionRegistry(), run=lambda *a, **k: FakeResult(0, stdout='  \n'), cwd='/scratch')
        with self.assertRaisesRegex(RuntimeError, 'without printing a session id'):
            driver.create('probe prompt')

    def test_submit_refuses_a_foreign_session(self):
        driver = ClaudeDriver(SessionRegistry(), run=lambda *a, **k: FakeResult(0, stdout='[]'), cwd='/scratch')
        with self.assertRaises(ForeignSessionError):
            driver.submit('not-mine', 'msg')

    def test_submit_confirms_listing_then_refuses_to_claim_an_unsupported_delivery(self):
        # A listed session no longer yields a bare True: nothing captured at this version
        # delivers a further message to a running --bg session, so reporting acceptance would
        # record an `accepted` outcome for a marker the host never received.
        registry = SessionRegistry()
        registry.mint('abcd1234')
        present = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(
            0, stdout=json.dumps([{'id': 'abcd1234', 'kind': 'background', 'state': 'idle'}])), cwd='/scratch')
        with self.assertRaises(SubmissionUnsupported):
            present.submit('abcd1234', 'msg')

    def test_submit_rejects_a_session_absent_from_the_listing_before_anything_else(self):
        registry = SessionRegistry()
        registry.mint('abcd1234')
        absent = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(0, stdout='[]'), cwd='/scratch')
        with self.assertRaises(ValueError) as caught:
            absent.submit('abcd1234', 'msg')
        self.assertNotIsInstance(caught.exception, SubmissionUnsupported)

    def test_observe_is_unobservable_when_logs_unreachable(self):
        # Reproduces the real observation: `claude logs <id>` fails once a `done` session's
        # daemon socket is gone (ENOENT). A dead observation channel is not a negative result,
        # so it must not reach classification as an empty (hence not_observed) outcome map.
        registry = SessionRegistry()
        registry.mint('abcd1234')
        driver = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(1, stderr='connect ENOENT'), cwd='/scratch')
        self.assertEqual(driver.observe('abcd1234', marker=MARKER, submitted_at=0.0),
                          Observation(outcomes={}, observable=False))

    def test_observe_of_an_untimestamped_transcript_is_unobservable(self):
        # `claude logs` prints no per-line timestamp, so a reachable transcript still cannot be
        # ordered against submission: the create() prompt's own turn would otherwise be read as
        # this trial's turn_start before the marker existed.
        registry = SessionRegistry()
        registry.mint('abcd1234')
        raw = f'User: {MARKER}\nAssistant: ack {MARKER}\n'
        driver = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(0, stdout=raw), cwd='/scratch')
        observation = driver.observe('abcd1234', marker=MARKER, submitted_at=0.0)
        self.assertEqual(observation.outcomes, {})
        self.assertFalse(observation.observable)

    def test_teardown_releases_the_registry_entry(self):
        registry = SessionRegistry()
        registry.mint('abcd1234')
        driver = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(0), cwd='/scratch')
        driver.teardown('abcd1234')
        with self.assertRaises(ForeignSessionError):
            registry.require_owned('abcd1234')

    def test_failed_teardown_keeps_ownership_so_it_can_be_retried(self):
        registry = SessionRegistry()
        registry.mint('abcd1234')
        driver = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(1, stderr='rm failed'), cwd='/scratch')
        with self.assertRaises(RuntimeError):
            driver.teardown('abcd1234')
        registry.require_owned('abcd1234')  # still ours: the live session can still be removed


class CodexDriverTests(unittest.TestCase):
    RUN_STARTED = datetime.datetime(2026, 9, 11, 12, 0, 0, tzinfo=datetime.UTC).timestamp()

    def rollout(self, *records):
        """A throwaway rollout file; returns its path and removes it when the test ends."""
        with tempfile.NamedTemporaryFile('w', suffix='.jsonl', delete=False) as handle:
            handle.writelines(json.dumps(record) + '\n' for record in records)
            path = handle.name
        self.addCleanup(os.unlink, path)
        return path

    @staticmethod
    def message(stamp, role, text):
        return {'timestamp': stamp, 'payload': {'type': 'message', 'role': role,
                                                 'content': [{'type': 'output_text', 'text': text}]}}

    def driver(self, registry, path, *, run=None):
        return CodexDriver(registry, run=run or (lambda *a, **k: FakeResult(0)),
                           rollout_path_for=lambda _id: path, started_at=self.RUN_STARTED)

    def test_register_existing_mints_a_thread_created_after_the_run_started_then_submits(self):
        registry = SessionRegistry()
        calls = []
        path = self.rollout({'timestamp': '2026-09-11T12:00:30.000Z', 'type': 'session_meta'})

        def fake_run(argv, **kwargs):
            calls.append(argv)
            return FakeResult(0)

        driver = self.driver(registry, path, run=fake_run)
        driver.register_existing('thread-1')
        self.assertTrue(driver.submit('thread-1', MARKER))
        self.assertEqual(calls[0], ['codex', 'queue', '--thread', 'thread-1', '--message', MARKER])

    def test_register_existing_refuses_a_thread_that_predates_this_run(self):
        # The registry's guarantee is that only sessions this run created are ever contacted;
        # a mistyped or copy-pasted id naming someone's ordinary thread must not be adoptable.
        registry = SessionRegistry()
        path = self.rollout({'timestamp': '2026-09-10T09:00:00.000Z', 'type': 'session_meta'})
        with self.assertRaises(ForeignSessionError):
            self.driver(registry, path).register_existing('someone-elses-thread')
        self.assertEqual(registry.created, set())

    def test_register_existing_refuses_without_a_rollout_or_a_usable_timestamp(self):
        registry = SessionRegistry()
        with self.assertRaises(ForeignSessionError):
            self.driver(registry, None).register_existing('thread-1')
        undated = self.rollout({'type': 'session_meta'})
        with self.assertRaises(ForeignSessionError):
            self.driver(registry, undated).register_existing('thread-1')
        self.assertEqual(registry.created, set())

    def test_register_existing_refuses_an_unreadable_rollout_rather_than_raising_oserror(self):
        registry = SessionRegistry()
        missing = os.path.join(self.tmp_missing(), 'never-written.jsonl')
        with self.assertRaises(ForeignSessionError):
            self.driver(registry, missing).register_existing('thread-1')

    def test_observe_is_unobservable_when_the_rollout_is_not_readable_yet(self):
        # A rollout is created lazily, so an early poll can precede the file: an unreadable
        # channel is unobservable, exactly as a failed `claude logs` read is.
        registry = SessionRegistry()
        registry.mint('thread-1')
        missing = os.path.join(self.tmp_missing(), 'never-written.jsonl')
        self.assertEqual(self.driver(registry, missing).observe('thread-1', marker=MARKER, submitted_at=0.0),
                          Observation(outcomes={}, observable=False))

    def tmp_missing(self):
        directory = tempfile.mkdtemp()
        self.addCleanup(os.rmdir, directory)
        return directory

    def test_submit_refuses_a_foreign_thread(self):
        with self.assertRaises(ForeignSessionError):
            self.driver(SessionRegistry(), None).submit('not-mine', MARKER)

    def test_observe_reads_the_rollout_file(self):
        registry = SessionRegistry()
        registry.mint('thread-1')
        path = self.rollout(self.message('2026-09-11T00:00:01.000Z', 'assistant', f'ack {MARKER}'))
        observation = self.driver(registry, path).observe('thread-1', marker=MARKER, submitted_at=0.0)
        self.assertEqual(set(observation.outcomes), {'turn_start', 'ack'})
        self.assertTrue(observation.observable)

    def test_observe_is_unobservable_when_the_rollout_holds_an_undated_message(self):
        registry = SessionRegistry()
        registry.mint('thread-1')
        path = self.rollout({'payload': {'type': 'message', 'role': 'assistant',
                                          'content': [{'type': 'output_text', 'text': f'ack {MARKER}'}]}})
        observation = self.driver(registry, path).observe('thread-1', marker=MARKER, submitted_at=0.0)
        self.assertEqual(observation.outcomes, {})
        self.assertFalse(observation.observable)

    def test_observe_is_unobservable_without_a_rollout_path(self):
        registry = SessionRegistry()
        registry.mint('thread-1')
        self.assertEqual(self.driver(registry, None).observe('thread-1', marker=MARKER, submitted_at=0.0),
                          Observation(outcomes={}, observable=False))


class OpenCodeDriverTests(unittest.TestCase):
    def test_instantiation_refuses_until_export_format_is_captured(self):
        with self.assertRaises(NotImplementedError):
            OpenCodeDriver()


class FakeClock:
    """Wall and monotonic readings a test advances itself, so no trial waits a real 120s."""

    def __init__(self, wall=1000.0):
        self.wall = wall
        self.elapsed = 0.0
        self.slept = []

    def time(self):
        return self.wall + self.elapsed

    def monotonic(self):
        return self.elapsed

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.elapsed += seconds


class FakeDriver:
    """Scripted host: `observations` is consumed one entry per observe() call, last repeating."""

    def __init__(self, *, observations=None, accepted=True, submit_error=None, clock=None):
        self.observations = list(observations or [Observation()])
        self.accepted = accepted
        self.submit_error = submit_error
        self.clock = clock
        self.order = []

    def create(self, prompt):
        self.order.append('create')
        return 'sid'

    def submit(self, session_id, marker):
        assert session_id == 'sid'
        self.order.append('submit')
        if self.submit_error is not None:
            raise self.submit_error
        if self.clock is not None:
            self.clock.elapsed += 12  # a slow host: submission returns well after it was sent
        return self.accepted

    def observe(self, session_id, *, marker, submitted_at):
        self.order.append('observe')
        return self.observations.pop(0) if len(self.observations) > 1 else self.observations[0]


class RunTrialTests(unittest.TestCase):
    def run_one(self, driver, clock, **kwargs):
        return run_trial(driver, prompt='hi', marker=MARKER, clock=clock.time,
                          monotonic=clock.monotonic, sleep=clock.sleep, **kwargs)

    def test_accepted_submission_is_stamped_when_submit_returns_not_before_it(self):
        # Submission can block for seconds; stamping acceptance at submitted_at backdated a
        # slow acknowledgement into its 10s window and reported it as observed on time.
        clock = FakeClock()
        driver = FakeDriver(observations=[Observation(outcomes={'visible': 1000.0,
                                                                 'turn_start': 1001.0,
                                                                 'ack': 1002.0})], clock=clock)
        run = self.run_one(driver, clock)
        self.assertEqual(run.session_id, 'sid')
        self.assertEqual(run.submitted_at, 1000.0)
        self.assertEqual(run.accepted_at, 1012.0)
        self.assertEqual(run.outcomes['accepted'], 1012.0)

    def test_settle_runs_between_create_and_submit(self):
        clock = FakeClock()
        driver = FakeDriver(clock=clock)
        self.run_one(driver, clock, settle=lambda: driver.order.append('settle'))
        self.assertEqual(driver.order[:3], ['create', 'settle', 'submit'])

    def test_rejected_submission_does_not_add_accepted(self):
        clock = FakeClock()
        run = self.run_one(FakeDriver(accepted=False, clock=clock), clock)
        self.assertNotIn('accepted', run.outcomes)
        self.assertIsNone(run.accepted_at)

    def test_observation_continues_through_the_windows_instead_of_one_snapshot(self):
        # The outcome lands on a later poll: a single immediate snapshot reported it
        # not_observed even though it arrived inside its own window.
        clock = FakeClock()
        late = Observation(outcomes={'visible': 1000.0, 'turn_start': 1030.0, 'ack': 1031.0})
        driver = FakeDriver(observations=[Observation(), Observation(), late], clock=clock)
        run = self.run_one(driver, clock, poll_interval=5.0)
        self.assertEqual(driver.order.count('observe'), 3)
        self.assertEqual(set(run.outcomes), {'accepted', 'visible', 'turn_start', 'ack'})
        self.assertTrue(all(run.observable.values()))

    def test_polling_stops_at_the_longest_window_and_reports_what_is_missing(self):
        clock = FakeClock()
        driver = FakeDriver(observations=[Observation(outcomes={'visible': 1000.0})], clock=clock)
        run = self.run_one(driver, clock, poll_interval=30.0)
        self.assertGreaterEqual(clock.elapsed, 120)  # LAST_WINDOW, on the injected monotonic clock
        self.assertEqual(set(run.outcomes), {'accepted', 'visible'})
        self.assertTrue(run.observable['visible'])

    def test_an_unreadable_channel_marks_only_the_unseen_outcomes_unobservable(self):
        clock = FakeClock()
        seen = Observation(outcomes={'visible': 1000.0}, observable=True)
        dead = Observation(observable=False)
        driver = FakeDriver(observations=[seen, dead], clock=clock)
        run = self.run_one(driver, clock, poll_interval=60.0)
        self.assertTrue(run.observable['visible'])  # a positive stands on its own evidence
        self.assertTrue(run.observable['ack'])  # one readable poll proves the channel existed

    def test_a_channel_that_never_reads_leaves_missing_outcomes_unobservable(self):
        clock = FakeClock()
        driver = FakeDriver(observations=[Observation(observable=False)], clock=clock)
        run = self.run_one(driver, clock, poll_interval=60.0)
        self.assertEqual(run.observable, {'accepted': True, 'visible': False,
                                           'turn_start': False, 'ack': False})
        trial = Trial(submitted=run.submitted_at, state=run.state, outcomes=run.outcomes)
        classified = classify_trial(trial, run.submitted_at + 1000, observable=run.observable)
        self.assertEqual(classified['ack'], 'unobservable')  # never not_observed

    def test_unsupported_submission_classifies_every_outcome_unsupported(self):
        clock = FakeClock()
        driver = FakeDriver(submit_error=SubmissionUnsupported('no captured path'), clock=clock)
        run = self.run_one(driver, clock)
        self.assertEqual(run.outcomes, {})
        self.assertFalse(any(run.supported.values()))
        self.assertNotIn('observe', driver.order)  # nothing was delivered, so nothing is ours
        trial = Trial(submitted=run.submitted_at, state=run.state, outcomes=run.outcomes)
        classified = classify_trial(trial, run.submitted_at + 1000, supported=run.supported)
        self.assertTrue(all(value == 'unsupported' for value in classified.values()))

    def test_requested_state_is_validated_and_carried_into_the_result(self):
        clock = FakeClock()
        run = self.run_one(FakeDriver(clock=clock), clock, state='busy')
        self.assertEqual(run.state, 'busy')
        with self.assertRaises(ValueError):
            self.run_one(FakeDriver(clock=clock), clock, state='asleep')


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

"""Controlled fixtures only: no test here launches an installed Claude, Codex or OpenCode CLI."""

import datetime
import json
import os
import subprocess
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
    SessionCreationUncaptured,
    SessionRegistry,
    SubmissionUncaptured,
    SubmissionUnsupported,
    TeardownUnsupported,
    background_sessions,
    classify_trial,
    codex_rollout_events,
    detect_outcomes,
    marker_message,
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

    def test_background_sessions_raises_on_a_daemon_error_object_not_a_list(self):
        # A degraded daemon can print a valid-JSON error object and still exit 0; iterating it
        # as the expected list-of-objects shape would raise an unrelated-looking AttributeError.
        with self.assertRaises(RuntimeError):
            background_sessions('{"error":"daemon restarting"}')

    def test_background_sessions_raises_on_a_null_top_level(self):
        with self.assertRaises(RuntimeError):
            background_sessions('null')

    def test_background_session_ids_raises_on_an_entry_with_no_id(self):
        driver = ClaudeDriver(SessionRegistry(),
                              run=lambda *a, **k: FakeResult(0, stdout='[{"kind":"background"}]'),
                              cwd='/scratch')
        with self.assertRaises(RuntimeError):
            driver._background_session_ids()

    def test_transcript_parses_role_prefixed_multiline_blocks(self):
        raw = f'User: hello {MARKER}\ncontinued\nAssistant: got it\nstill talking\n'
        events = parse_claude_transcript(raw)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].role, 'user')
        self.assertEqual(events[0].text, f'hello {MARKER}\ncontinued')
        self.assertEqual(events[1].role, 'assistant')
        self.assertEqual(events[1].text, 'got it\nstill talking')

    def test_transcript_without_marker_does_not_raise(self):
        events = parse_claude_transcript('User: hi\nAssistant: hello\n')
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

    def test_unparseable_lines_count_as_unusable_while_other_record_types_do_not(self):
        # A record this runner has no use for is not a failed read; a line that is not JSON is.
        lines = ['not json', json.dumps({'payload': {'type': 'world_state'}}), '']
        self.assertEqual(codex_rollout_events(lines), ([], 1))
        self.assertEqual(codex_rollout_events([json.dumps({'payload': {'type': 'world_state'}})]), ([], 0))

    def test_turn_boundary_records_become_pseudo_role_events(self):
        # Captured shapes (docs/host-probe-preflight.md): these are the only evidence that a
        # post-submission assistant message belongs to a *new* turn rather than the running one.
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:01.000Z',
                             'type': 'event_msg', 'payload': {'type': kind}})
                 for kind in ('task_started', 'task_complete', 'turn_aborted')]
        events, unusable = codex_rollout_events(lines)
        self.assertEqual([e.role for e in events], ['turn_start', 'turn_end', 'turn_end'])
        self.assertEqual(unusable, 0)

    def test_turn_stream_prefers_the_hosts_own_boundary_over_the_first_assistant_message(self):
        # The busy case: an assistant message from the turn already running must not be read
        # as this trial's turn starting.
        events = [Event(role='assistant', text='still finishing', time=5.0),
                  Event(role='turn_end', text='', time=7.0),
                  Event(role='turn_start', text='', time=8.0),
                  Event(role='assistant', text=f'ack {MARKER}', time=9.0)]
        observation = detect_outcomes(events, MARKER, submitted_at=0.0, turn_stream=True)
        self.assertEqual(observation.outcomes['turn_start'], 8.0)
        self.assertEqual(observation.turn_end, 7.0)
        self.assertEqual(observation.outcomes['ack'], 9.0)
        without = detect_outcomes(events, MARKER, submitted_at=0.0)
        self.assertEqual(without.outcomes['turn_start'], 5.0)  # the old behaviour, for contrast

    def test_message_records_without_a_usable_timestamp_are_dropped_and_counted(self):
        lines = [
            json.dumps({'payload': {'type': 'message', 'role': 'assistant',
                                    'content': [{'type': 'output_text', 'text': 'undated'}]}}),
            json.dumps({'timestamp': 'not-a-date',
                        'payload': {'type': 'message', 'role': 'user',
                                    'content': [{'type': 'input_text', 'text': 'malformed'}]}}),
        ]
        self.assertEqual(codex_rollout_events(lines), ([], 2))

    def test_null_content_is_unusable_rather_than_a_typeerror(self):
        # payload.get('content', []) only substitutes [] when the key is absent; an explicit
        # "content": null slips past that default and a bare iteration would raise TypeError.
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z',
                             'payload': {'type': 'message', 'role': 'user', 'content': None}})]
        self.assertEqual(codex_rollout_events(lines), ([], 1))

    def test_object_valued_content_is_unusable_not_silently_empty(self):
        # A structured/tool-call payload shape (content as a single object, not a list of parts)
        # must not parse as an empty-text event with observable=True -- that reads as "checked,
        # nothing there" instead of "this shape was never captured".
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z',
                             'payload': {'type': 'message', 'role': 'assistant',
                                         'content': {'type': 'tool_call', 'name': 'x'}}})]
        self.assertEqual(codex_rollout_events(lines), ([], 1))

    def test_a_non_string_part_text_is_unusable_rather_than_a_typeerror(self):
        # A list-shaped content whose part carries a non-string `text` (e.g. explicit null) made
        # ''.join(...) raise instead of reading as an unrecognized shape.
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z',
                             'payload': {'type': 'message', 'role': 'user',
                                         'content': [{'text': None}]}})]
        self.assertEqual(codex_rollout_events(lines), ([], 1))

    def test_a_non_dict_part_makes_the_whole_record_unusable_not_silently_shorter(self):
        # Silently skipping just the non-dict part let an unreadable marker message join down to
        # an empty, ordinary-looking string -- negative evidence rather than unusable.
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z',
                             'payload': {'type': 'message', 'role': 'user',
                                         'content': ['not-a-part', {'text': 'hi'}]}})]
        self.assertEqual(codex_rollout_events(lines), ([], 1))

    def test_a_non_object_record_is_unusable_rather_than_an_attributeerror(self):
        # Valid JSON is not necessarily an object -- a damaged or schema-drifted line can decode
        # to `null`, a number or a list -- and `record.get(...)` on any of those raises instead
        # of reading as an unrecognized shape, aborting the whole read rather than just the line.
        lines = [json.dumps(None), json.dumps([1, 2]), json.dumps(3),
                json.dumps({'timestamp': '2026-09-11T00:00:00.000Z',
                            'payload': {'type': 'message', 'role': 'user',
                                        'content': [{'text': 'hi'}]}})]
        events, unusable = codex_rollout_events(lines)
        self.assertEqual(unusable, 3)
        self.assertEqual(len(events), 1)

    def test_rollout_started_at_takes_the_earliest_record_of_any_type(self):
        lines = [
            json.dumps({'timestamp': '2026-09-11T00:00:05.000Z', 'payload': {'type': 'message'}}),
            json.dumps({'timestamp': '2026-09-11T00:00:01.000Z', 'type': 'session_meta'}),
        ]
        expected = datetime.datetime(2026, 9, 11, 0, 0, 1, tzinfo=datetime.UTC).timestamp()
        self.assertEqual(rollout_started_at(lines), expected)

    def test_rollout_started_at_fails_closed_on_an_undated_record_of_any_type(self):
        # A record whose own timestamp is missing or unusable must not be silently skipped from
        # the earliest-of computation: skipping it could hide that this exact record was the
        # earliest one in the file, letting a thread that predates this run pass provenance.
        lines = [
            json.dumps({'timestamp': '2026-09-11T00:00:05.000Z', 'payload': {'type': 'message'}}),
            json.dumps({'type': 'world_state'}),  # no timestamp key at all
        ]
        self.assertIsNone(rollout_started_at(lines))

    def test_rollout_started_at_fails_closed_on_an_unparseable_line(self):
        # The unreadable line could be the earliest record, so a minimum taken over whatever
        # survived would let a thread older than this run pass the provenance check.
        lines = ['not json', json.dumps({'timestamp': '2026-09-11T00:00:01.000Z', 'type': 'session_meta'})]
        self.assertIsNone(rollout_started_at(lines))
        self.assertIsNone(rollout_started_at(['']))

    def test_rollout_started_at_fails_closed_on_a_non_object_record(self):
        # Same shape as an unparseable line: valid JSON that isn't an object carries no
        # `.get`-able timestamp, and treating it as skippable could let the earliest real
        # record's own poisoning go unnoticed.
        lines = [json.dumps(None), json.dumps({'timestamp': '2026-09-11T00:00:01.000Z'})]
        self.assertIsNone(rollout_started_at(lines))


class ClaudeDriverTests(unittest.TestCase):
    def test_create_mints_the_new_session_id_from_a_listing_diff(self):
        # `claude --bg --print`'s own stdout shape has never been captured against a real
        # session, so a wrong guess parsed from it could mint a footer/informational line
        # instead of the real id. Identify the session from what `claude agents --json --all`
        # itself reports as new, never from stdout.
        registry = SessionRegistry()
        calls = []
        before = json.dumps([{'id': 'old1', 'kind': 'background'}])
        after = json.dumps([{'id': 'old1', 'kind': 'background'}, {'id': 'abcd1234', 'kind': 'background'}])
        responses = iter([FakeResult(0, stdout=before), FakeResult(0, stdout='irrelevant footer text\n'),
                          FakeResult(0, stdout=after)])

        def fake_run(argv, **kwargs):
            calls.append(argv)
            return next(responses)

        driver = ClaudeDriver(registry, run=fake_run, cwd='/scratch')
        session_id = driver.create('probe prompt')
        self.assertEqual(session_id, 'abcd1234')
        self.assertIn('abcd1234', registry.created)
        self.assertEqual(calls[1][:3], ['claude', '--bg', '--cwd'])

    def test_create_raises_on_nonzero_exit(self):
        def fake_run(argv, **kwargs):
            return FakeResult(1, stderr='boom') if '--bg' in argv else FakeResult(0, stdout='[]')

        driver = ClaudeDriver(SessionRegistry(), run=fake_run, cwd='/scratch')
        with self.assertRaises(RuntimeError):
            driver.create('probe prompt')

    def test_create_raises_when_no_new_background_session_appears(self):
        # claude --bg exiting 0 with no corresponding new listing entry must not be silently
        # accepted: a background session may exist now, untracked, or none was created at all.
        listing = json.dumps([{'id': 'old1', 'kind': 'background'}])

        def fake_run(argv, **kwargs):
            return FakeResult(0, stdout='  \n') if '--bg' in argv else FakeResult(0, stdout=listing)

        driver = ClaudeDriver(SessionRegistry(), run=fake_run, cwd='/scratch')
        with self.assertRaisesRegex(RuntimeError, '0 new'):
            driver.create('probe prompt')

    def test_create_raises_when_multiple_new_background_sessions_appear(self):
        # Ambiguity, not a guess: nothing here picks a winner among several new sessions.
        before = json.dumps([])
        after = json.dumps([{'id': 'a', 'kind': 'background'}, {'id': 'b', 'kind': 'background'}])
        responses = iter([FakeResult(0, stdout=before), FakeResult(0, stdout=''), FakeResult(0, stdout=after)])

        def fake_run(argv, **kwargs):
            return next(responses)

        driver = ClaudeDriver(SessionRegistry(), run=fake_run, cwd='/scratch')
        with self.assertRaisesRegex(RuntimeError, '2 new'):
            driver.create('probe prompt')

    def test_create_mints_ambiguous_sessions_before_raising_so_they_stay_reachable(self):
        # claude --bg already exited 0, so every id in an unexpected diff is a real, live
        # background session under the operator's real HOME regardless of whether create() can
        # tell which one this trial made -- leaving it out of the registry would make it
        # permanently untrackable, since teardown() requires ownership.
        registry = SessionRegistry()
        before = json.dumps([])
        after = json.dumps([{'id': 'a', 'kind': 'background'}, {'id': 'b', 'kind': 'background'}])
        responses = iter([FakeResult(0, stdout=before), FakeResult(0, stdout=''), FakeResult(0, stdout=after)])

        def fake_run(argv, **kwargs):
            return next(responses)

        driver = ClaudeDriver(registry, run=fake_run, cwd='/scratch')
        with self.assertRaisesRegex(RuntimeError, '2 new'):
            driver.create('probe prompt')
        self.assertEqual(registry.created, {'a', 'b'})

    def test_submit_refuses_a_foreign_session(self):
        driver = ClaudeDriver(SessionRegistry(), run=lambda *a, **k: FakeResult(0, stdout='[]'), cwd='/scratch')
        with self.assertRaises(ForeignSessionError):
            driver.submit('not-mine', 'msg')

    def test_submit_confirms_listing_then_refuses_to_claim_an_uncaptured_delivery(self):
        # A listed session no longer yields a bare True: nothing captured at this version
        # delivers a further message to a running --bg session, so reporting acceptance would
        # record an `accepted` outcome for a marker the host never received. The refusal is
        # `Uncaptured`, not `Unsupported` — we have not exercised a path, which is not the
        # same claim as Claude not having one.
        registry = SessionRegistry()
        registry.mint('abcd1234')
        present = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(
            0, stdout=json.dumps([{'id': 'abcd1234', 'kind': 'background', 'state': 'idle'}])), cwd='/scratch')
        with self.assertRaises(SubmissionUncaptured):
            present.submit('abcd1234', 'msg')
        self.assertNotIsInstance(SubmissionUncaptured(''), SubmissionUnsupported)

    def test_submit_lists_background_sessions_and_reports_a_failed_listing(self):
        # Without --all the listing carries interactive entries only, so every owned session
        # would fail the membership check; a nonzero exit is reported, not parsed as JSON.
        registry = SessionRegistry()
        registry.mint('abcd1234')
        seen = []

        def fake_run(argv, **kwargs):
            seen.append(argv)
            return FakeResult(1, stderr='daemon unreachable')

        driver = ClaudeDriver(registry, run=fake_run, cwd='/scratch')
        with self.assertRaises(RuntimeError):
            driver.submit('abcd1234', 'msg')
        self.assertIn('--all', seen[0])

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

    def test_observe_of_an_unrecognized_transcript_shape_is_unobservable(self):
        # Non-empty output that yields no recognized User:/Assistant: block is a format this
        # runner cannot parse, not "read cleanly, no conversation yet" -- those must not
        # collapse into the same empty, observable=True result.
        registry = SessionRegistry()
        registry.mint('abcd1234')
        driver = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(0, stdout='some other format\n'),
                              cwd='/scratch')
        observation = driver.observe('abcd1234', marker=MARKER, submitted_at=0.0)
        self.assertEqual(observation, Observation(outcomes={}, observable=False))

    def test_observe_of_genuinely_empty_output_is_still_observable(self):
        # A session with no conversation yet (nothing sent, or a poll racing session creation)
        # must not be conflated with an unrecognized shape: empty output is a legitimate read.
        registry = SessionRegistry()
        registry.mint('abcd1234')
        driver = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(0, stdout='  \n'), cwd='/scratch')
        observation = driver.observe('abcd1234', marker=MARKER, submitted_at=0.0)
        self.assertEqual(observation, Observation(outcomes={}, observable=True))

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

    def setUp(self):
        # One sessions root per test, so adoption sees exactly the rollouts the test wrote.
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.sessions_root = root.name

    def rollout(self, *records, lines=None):
        """A throwaway rollout under this test's sessions root; returns its path."""
        path = os.path.join(self.sessions_root, f'rollout-{len(os.listdir(self.sessions_root))}.jsonl')
        with open(path, 'w', encoding='utf-8') as handle:
            handle.writelines(lines or [json.dumps(record) + '\n' for record in records])
        return path

    @staticmethod
    def message(stamp, role, text):
        return {'timestamp': stamp, 'payload': {'type': 'message', 'role': role,
                                                 'content': [{'type': 'output_text', 'text': text}]}}

    def driver(self, registry, path, *, run=None):
        return CodexDriver(registry, run=run or (lambda *a, **k: FakeResult(0)),
                           rollout_path_for=lambda _id: path, started_at=self.RUN_STARTED,
                           sessions_root=self.sessions_root)

    def test_create_refuses_because_no_thread_creation_path_is_captured(self):
        # Guessing what `codex exec` prints would be the evidence failure OpenCodeDriver
        # refuses for; run_trial(existing_session=...) is the supported way in.
        with self.assertRaises(SessionCreationUncaptured):
            self.driver(SessionRegistry(), None).create('hi')

    def test_register_existing_refuses_without_a_sessions_root(self):
        # Without sessions_root there is no way to rule out a concurrent human thread at all --
        # this fixture's own driver() helper always supplies one, so the refusal path needs its
        # own direct construction to stay covered.
        registry = SessionRegistry()
        mine = self.rollout({'timestamp': '2026-09-11T12:00:30.000Z', 'type': 'session_meta'})
        driver = CodexDriver(registry, run=lambda *a, **k: FakeResult(0),
                             rollout_path_for=lambda _id: mine, started_at=self.RUN_STARTED,
                             sessions_root=None)
        with self.assertRaises(ForeignSessionError):
            driver.register_existing('thread-1')

    def test_adoption_ignores_an_older_neighbour_but_refuses_a_concurrent_one(self):
        # "Started after this run did" is equally true of a thread the human opened meanwhile,
        # so a second fresh rollout under the sessions root makes the adopted one ambiguous.
        registry = SessionRegistry()
        mine = self.rollout({'timestamp': '2026-09-11T12:00:30.000Z', 'type': 'session_meta'})
        self.rollout({'timestamp': '2026-09-10T09:00:00.000Z', 'type': 'session_meta'})
        self.driver(registry, mine).register_existing('thread-1')
        self.rollout({'timestamp': '2026-09-11T12:00:31.000Z', 'type': 'session_meta'})
        with self.assertRaises(ForeignSessionError):
            self.driver(SessionRegistry(), mine).register_existing('thread-1')

    def test_three_trials_can_each_adopt_a_distinct_thread_in_the_same_run(self):
        # The 3-trials-per-cell protocol (docs/host-probes.md) needs three adoptions in one run.
        # Counting an earlier trial's own already-owned thread as an unresolved rival made the
        # second adoption always refuse; it must be excluded once this run has claimed it.
        registry = SessionRegistry()
        paths = {}
        driver = CodexDriver(registry, run=lambda *a, **k: FakeResult(0),
                             rollout_path_for=paths.get, started_at=self.RUN_STARTED,
                             sessions_root=self.sessions_root)
        for tid in ('thread-1', 'thread-2', 'thread-3'):
            # One thread created and adopted before the next exists, matching how a real trial
            # sequence works: each rollout appears only once its own thread has been created.
            paths[tid] = self.rollout({'timestamp': '2026-09-11T12:00:30.000Z',
                                       'type': 'session_meta'})
            driver.register_existing(tid)
        self.assertEqual(registry.created, {'thread-1', 'thread-2', 'thread-3'})

    def test_a_neighbour_that_cannot_be_dated_is_ambiguity_not_absence(self):
        registry = SessionRegistry()
        mine = self.rollout({'timestamp': '2026-09-11T12:00:30.000Z', 'type': 'session_meta'})
        self.rollout(lines=['not json\n'])
        with self.assertRaises(ForeignSessionError):
            self.driver(registry, mine).register_existing('thread-1')
        self.assertEqual(registry.created, set())

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

    def test_an_unlistable_sibling_directory_is_treated_as_an_unruled_out_rival(self):
        # glob.glob swallows an OSError from an unreadable directory and just returns fewer
        # matches, with no signal anything was skipped -- a real rival hiding there would read
        # as "no rivals found" instead of "could not check".
        registry = SessionRegistry()
        mine = self.rollout({'timestamp': '2026-09-11T12:00:30.000Z', 'type': 'session_meta'})
        locked = os.path.join(self.sessions_root, 'locked')
        os.mkdir(locked)
        os.chmod(locked, 0o000)
        self.addCleanup(os.chmod, locked, 0o755)
        with self.assertRaises(ForeignSessionError):
            self.driver(registry, mine).register_existing('thread-1')

    def test_a_neighbour_untouched_since_before_the_run_is_skipped_without_being_opened(self):
        # A file whose mtime hasn't moved since before started_at cannot contain a record newer
        # than that mtime, so its earliest record is provably older too -- it never needs
        # opening. Garbage content that would otherwise read as an unruled-out rival proves the
        # skip happened before any read was attempted, not just that the answer came out right.
        registry = SessionRegistry()
        mine = self.rollout({'timestamp': '2026-09-11T12:00:30.000Z', 'type': 'session_meta'})
        old = self.rollout(lines=['not json\n'])
        old_time = self.RUN_STARTED - 3600
        os.utime(old, (old_time, old_time))
        self.driver(registry, mine).register_existing('thread-1')

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
        path = self.rollout(
            {'timestamp': '2026-09-11T00:00:00.500Z', 'type': 'event_msg',
             'payload': {'type': 'task_started'}},
            self.message('2026-09-11T00:00:01.000Z', 'assistant', f'ack {MARKER}'),
            {'timestamp': '2026-09-11T00:00:02.000Z', 'type': 'event_msg',
             'payload': {'type': 'task_complete'}})
        observation = self.driver(registry, path).observe('thread-1', marker=MARKER, submitted_at=0.0)
        # turn_start comes from the host's own boundary event, not from the message.
        self.assertEqual(set(observation.outcomes), {'turn_start', 'ack'})
        self.assertIsNotNone(observation.turn_end)
        self.assertTrue(observation.observable)

    def test_observe_is_unobservable_when_the_rollout_holds_unparseable_content(self):
        # Opening the file proved nothing about reading it: a truncated or corrupt rollout
        # would otherwise yield an empty, observable read and classify as not_observed.
        registry = SessionRegistry()
        registry.mint('thread-1')
        with tempfile.NamedTemporaryFile('w', suffix='.jsonl', delete=False) as handle:
            handle.write('{"timestamp": "2026-09-11T00:00:0\n')
            path = handle.name
        self.addCleanup(os.unlink, path)
        observation = self.driver(registry, path).observe('thread-1', marker=MARKER, submitted_at=0.0)
        self.assertEqual(observation.outcomes, {})
        self.assertFalse(observation.observable)

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

    def test_teardown_refuses_and_retains_ownership(self):
        # codex has no `queue --stop`; releasing the registry entry anyway would make the
        # runner believe a live, authenticated host session had been cleaned up when it had
        # not.
        registry = SessionRegistry()
        registry.mint('thread-1')
        with self.assertRaises(TeardownUnsupported):
            self.driver(registry, None).teardown('thread-1')
        registry.require_owned('thread-1')  # still ours: nothing was actually torn down

    def test_teardown_refuses_a_foreign_thread(self):
        with self.assertRaises(ForeignSessionError):
            self.driver(SessionRegistry(), None).teardown('not-mine')


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

    def register_existing(self, session_id):
        self.order.append('register_existing')
        return session_id

    def submit(self, session_id, message):
        assert session_id == 'sid'
        self.order.append('submit')
        self.submitted_message = message
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

    def test_submission_asks_the_host_to_echo_the_marker_rather_than_sending_it_bare(self):
        # A bare opaque token gives an awake host no reason to quote it back; detect_outcomes
        # only recognizes acknowledgement when the marker appears in the reply, so a correct,
        # non-quoting answer to a bare token would misclassify as not_observed.
        clock = FakeClock()
        driver = FakeDriver(clock=clock)
        self.run_one(driver, clock)
        self.assertIn(MARKER, driver.submitted_message)
        self.assertNotEqual(driver.submitted_message, MARKER)
        self.assertEqual(driver.submitted_message, marker_message(MARKER))

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

    def test_rejected_submission_skips_polling_and_marks_transcript_outcomes_unobservable(self):
        # A clean nonzero exit is a real, observed failure to accept -- 'not_observed' is the
        # true classification for `accepted` itself -- but nothing was delivered, so polling for
        # transcript outcomes and eventually reporting them `not_observed` would be negative
        # evidence for a marker the host never received.
        clock = FakeClock()
        driver = FakeDriver(accepted=False, clock=clock)
        run = self.run_one(driver, clock)
        self.assertNotIn('observe', driver.order)
        self.assertTrue(run.observable['accepted'])
        self.assertFalse(run.observable['visible'])
        self.assertFalse(run.observable['turn_start'])
        self.assertFalse(run.observable['ack'])
        trial = Trial(submitted=run.submitted_at, state=run.state, outcomes=run.outcomes)
        # Known immediately as a fact, but classify_trial has no "already resolved" input --
        # `accepted`'s own 10s window must still actually elapse before it reports that fact.
        with self.assertRaises(ValueError):
            classify_trial(trial, run.submitted_at, observable=run.observable)
        classified = classify_trial(trial, run.submitted_at + 1000, observable=run.observable)
        self.assertEqual(classified['accepted'], 'not_observed')
        self.assertEqual(classified['visible'], 'unobservable')

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
        # Merging the polls is only half of it: the merged timestamps must also classify as
        # arrived-in-window, which is the failure a single snapshot produced.
        trial = Trial(submitted=run.submitted_at, state=run.state, outcomes=run.outcomes)
        classified = classify_trial(trial, run.submitted_at + 200, observable=run.observable)
        self.assertEqual(classified['ack'], 'observed')
        self.assertEqual(classified['turn_start'], 'observed')

    def test_polling_stops_at_the_longest_window_and_reports_what_is_missing(self):
        clock = FakeClock()
        driver = FakeDriver(observations=[Observation(outcomes={'visible': 1000.0})], clock=clock)
        run = self.run_one(driver, clock, poll_interval=30.0)
        self.assertGreaterEqual(clock.elapsed, 120)  # LAST_WINDOW, on the injected monotonic clock
        self.assertEqual(set(run.outcomes), {'accepted', 'visible'})
        self.assertTrue(run.observable['visible'])

    def test_a_channel_that_dies_mid_window_leaves_the_unseen_outcomes_unobservable(self):
        # One early readable poll does not cover the rest of the window: the acknowledgement
        # could have arrived into a transcript nobody could read by the deadline.
        clock = FakeClock()
        seen = Observation(outcomes={'visible': 1000.0}, observable=True)
        dead = Observation(observable=False)
        driver = FakeDriver(observations=[seen, dead], clock=clock)
        run = self.run_one(driver, clock, poll_interval=60.0)
        self.assertTrue(run.observable['visible'])  # a positive stands on its own evidence
        self.assertFalse(run.observable['ack'])  # the tail of the window went unread

    def test_a_readable_final_poll_covers_an_earlier_failed_one(self):
        # A transcript is cumulative, so the last successful read sees everything the failed
        # earlier read would have; a transient failure is not a permanent loss of coverage.
        clock = FakeClock()
        driver = FakeDriver(observations=[Observation(observable=False),
                                           Observation(outcomes={'visible': 1000.0})], clock=clock)
        run = self.run_one(driver, clock, poll_interval=60.0)
        self.assertTrue(all(run.observable.values()))

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

    def test_uncaptured_submission_is_unobservable_not_a_claim_about_the_host(self):
        clock = FakeClock()
        driver = FakeDriver(submit_error=SubmissionUncaptured('nothing captured here'), clock=clock)
        run = self.run_one(driver, clock)
        self.assertTrue(all(run.supported.values()))  # no claim that the host lacks the path
        self.assertFalse(any(run.observable.values()))
        trial = Trial(submitted=run.submitted_at, state=run.state, outcomes=run.outcomes,
                      turn_end_observable=run.turn_end_observable)
        classified = classify_trial(trial, run.submitted_at + 1000,
                                    supported=run.supported, observable=run.observable)
        self.assertTrue(all(value == 'unobservable' for value in classified.values()))

    def test_a_submission_timeout_is_unobservable_acceptance_but_still_polls_for_evidence(self):
        # The subprocess may already have handed the marker to the host before its hard-coded
        # timeout fired; losing the trial here would also lose any transcript evidence that
        # delivery produced. Acceptance alone goes unobservable; observation still runs.
        clock = FakeClock()
        driver = FakeDriver(
            observations=[Observation(outcomes={'visible': 1000.0, 'turn_start': 1001.0,
                                                'ack': 1002.0})],
            submit_error=subprocess.TimeoutExpired(cmd=['codex', 'queue'], timeout=15),
            clock=clock)
        run = self.run_one(driver, clock)
        self.assertIsNone(run.accepted_at)
        self.assertNotIn('accepted', run.outcomes)
        self.assertFalse(run.observable['accepted'])
        self.assertTrue(run.observable['visible'])
        self.assertEqual(run.outcomes['visible'], 1000.0)
        self.assertEqual(run.outcomes['ack'], 1002.0)

    def test_an_existing_session_is_adopted_instead_of_created(self):
        clock = FakeClock()
        driver = FakeDriver(clock=clock)
        run = self.run_one(driver, clock, existing_session='sid')
        self.assertEqual(driver.order[:2], ['register_existing', 'submit'])
        self.assertNotIn('create', driver.order)
        self.assertEqual(run.session_id, 'sid')

    def test_an_idle_trial_never_adopts_a_turn_end(self):
        # Observation.turn_end means "completion of the turn already running at submission"
        # (its own docstring). An idle trial has no such turn: a turn boundary the host emits
        # after submission is the completion of *this trial's own* marker turn, not one left
        # running before it, and must not be mislabeled as the busy-only field.
        clock = FakeClock()
        seen = Observation(outcomes={'visible': 1000.0, 'turn_start': 1001.0, 'ack': 1002.0},
                           turn_end=1002.0)
        run = self.run_one(FakeDriver(observations=[seen], clock=clock), clock)
        self.assertIsNone(run.turn_end)

    def test_a_busy_trial_without_a_turn_end_leaves_its_dependent_outcomes_unobservable(self):
        # The host never said the running turn finished, so the turn_start/ack windows never
        # started: their absence measures nothing and must not read as host silence.
        clock = FakeClock()
        driver = FakeDriver(observations=[Observation(outcomes={'visible': 1000.0})], clock=clock)
        run = self.run_one(driver, clock, state='busy', poll_interval=300.0)
        self.assertGreaterEqual(clock.elapsed, 900)  # BUSY_CAP, not the 120s idle window
        self.assertIsNone(run.turn_end)
        self.assertTrue(run.observable['visible'])
        self.assertFalse(run.observable['ack'])
        trial = Trial(submitted=run.submitted_at, state=run.state, outcomes=run.outcomes,
                      turn_end=run.turn_end, turn_end_observable=run.turn_end_observable)
        classified = classify_trial(trial, run.submitted_at + 1000, observable=run.observable)
        self.assertEqual(classified['turn_start'], 'unobservable')

    def test_a_busy_trial_without_turn_stream_never_trusts_a_bare_assistant_message(self):
        # Without a turn-boundary stream, an assistant message after submission cannot be told
        # apart from the tail of the turn already running (`detect_outcomes`' docstring) -- a
        # captured value here is exactly that ambiguity, not evidence, even though the general
        # "positively seen" rule would otherwise let it stand on its own.
        clock = FakeClock()
        driver = FakeDriver(observations=[Observation(outcomes={'turn_start': 1005.0})], clock=clock)
        run = self.run_one(driver, clock, state='busy', poll_interval=300.0)
        self.assertIsNone(run.turn_end)
        self.assertIn('turn_start', run.outcomes)
        self.assertFalse(run.observable['turn_start'])
        trial = Trial(submitted=run.submitted_at, state=run.state, outcomes=run.outcomes,
                      turn_end=run.turn_end, turn_end_observable=run.turn_end_observable)
        classified = classify_trial(trial, run.submitted_at + 1000, observable=run.observable)
        self.assertEqual(classified['turn_start'], 'unobservable')

    def test_a_turn_stream_hosts_readable_busy_timeout_stays_inconclusive(self):
        # A channel that stayed readable for the whole cap but never emitted a boundary is
        # itself evidence the earlier turn never finished -- Trial.result's own `inconclusive`
        # case via `turn_end_observable` -- not an unreadable channel, and must not be forced to
        # `unobservable` here the way a non-turn-stream host's ambiguity is.
        clock = FakeClock()
        driver = FakeDriver(observations=[Observation(turn_stream=True)], clock=clock)
        run = self.run_one(driver, clock, state='busy', poll_interval=300.0)
        self.assertIsNone(run.turn_end)
        self.assertTrue(run.turn_end_observable)
        self.assertTrue(run.observable['turn_start'])
        self.assertTrue(run.observable['ack'])
        trial = Trial(submitted=run.submitted_at, state=run.state, outcomes=run.outcomes,
                      turn_end=run.turn_end, turn_end_observable=run.turn_end_observable)
        classified = classify_trial(trial, run.submitted_at + 1000, observable=run.observable)
        self.assertEqual(classified['turn_start'], 'inconclusive')
        self.assertEqual(classified['ack'], 'inconclusive')

    def test_an_observed_turn_end_shortens_the_busy_wait_and_reaches_classification(self):
        clock = FakeClock()
        seen = Observation(outcomes={'visible': 1000.0, 'turn_start': 1040.0, 'ack': 1041.0},
                           turn_end=1030.0)
        driver = FakeDriver(observations=[seen], clock=clock)
        run = self.run_one(driver, clock, state='busy', poll_interval=300.0)
        self.assertEqual(run.turn_end, 1030.0)
        self.assertLess(clock.elapsed, 900)  # the dependent windows closed before the cap
        trial = Trial(submitted=run.submitted_at, state=run.state, outcomes=run.outcomes,
                      turn_end=run.turn_end, turn_end_observable=run.turn_end_observable)
        classified = classify_trial(trial, run.submitted_at + 1000, observable=run.observable)
        self.assertEqual(classified['ack'], 'observed')

    def test_a_turn_ending_close_to_the_cap_extends_the_wait_past_it(self):
        # A turn ending at 850s (within BUSY_CAP=900) still owes turn_start/ack the full 120s
        # window from that end -- out to 970s -- even though that lands past the cap itself.
        # `min(deadline, ...)` could only shorten the cap-based deadline, never stretch it,
        # so the old code stopped polling at 900s and lost the ack sitting at 970s.
        clock = FakeClock()
        turn_end = 1850.0  # wall time; submitted_at is 1000.0, so this is 850s in
        early = Observation(outcomes={'visible': 1012.0})
        mid = Observation(outcomes={'visible': 1012.0, 'turn_start': 1840.0}, turn_end=turn_end)
        late = Observation(outcomes={'visible': 1012.0, 'turn_start': 1840.0, 'ack': 1900.0},
                            turn_end=turn_end)
        driver = FakeDriver(observations=[early, mid, late], clock=clock)
        run = self.run_one(driver, clock, state='busy', poll_interval=890.0)
        self.assertEqual(run.turn_end, turn_end)
        self.assertEqual(run.outcomes.get('ack'), 1900.0)
        self.assertGreater(clock.elapsed, 900)  # polled past BUSY_CAP to cover the real end

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

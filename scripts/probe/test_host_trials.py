"""Controlled fixtures only: no test here launches an installed Claude, Codex or OpenCode CLI.

Process creation is injected everywhere a host command would run (`run`, `popen`, `pty`), and the
PTY client is exercised against controlled Python children (the pattern `test_wake_probe.py`
uses). Every host-facing shape in the fixtures mirrors a capture in docs/host-probe-preflight.md.
"""

import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import unittest.mock
import urllib.error
from dataclasses import dataclass

import host_trials
from host_trials import (
    CLAUDE_READY_PATTERN,
    CLOCK_DRIFT_TOLERANCE,
    CODEX_READY_PATTERN,
    CODEX_TRUST_PATTERN,
    MARKER_PATTERN,
    OUTCOME_NAMES,
    SIGNAL_ASSISTANT_MESSAGE,
    SIGNAL_SUBMIT_EXIT_STATUS,
    SIGNAL_TURN_BOUNDARY_EVENT,
    SIGNAL_USER_MESSAGE,
    ClaudeDriver,
    CleanupFailed,
    CodexDriver,
    Driver,
    Event,
    ForeignSessionError,
    Observation,
    OpenCodeDriver,
    PtyClient,
    PtyNotReady,
    SessionRegistry,
    SubmissionRejected,
    SubmissionUncaptured,
    SubmissionUnsupported,
    backgrounded_id,
    classify_trial,
    claude_session_version,
    claude_transcript_events,
    codex_rollout_events,
    codex_session_version,
    codex_thread_ids,
    detect_outcomes,
    marker_message,
    marker_token,
    opencode_export_events,
    opencode_export_version,
    opencode_session_ids,
    run_trial,
    run_trial_with_cleanup,
    strip_ansi,
    sweep,
)
from wake_probe import Trial, aggregate

MARKER = 'PARLEY-PROBE-deadbeefdeadbeefdeadbeefdeadbeef'
SESSION_UUID = '69aa52ed-1111-4222-8333-444455556666'
THREAD_ID = '01a09a24-ff1d-7360-9385-722d230ef92b'


@dataclass
class FakeResult:
    returncode: int
    stdout: str = ''
    stderr: str = ''


class FakeRun:
    """Scripted `subprocess.run`: each entry keys on the command's leading words.

    Values are a `FakeResult`, an exception instance to raise, or a callable taking the argv
    and returning either. The first matching prefix wins; a command with no script is an
    error, never a silent success, so a test cannot pass by an unexpected host call.
    """

    def __init__(self, scripts):
        self.scripts = scripts
        self.calls = []

    def __call__(self, argv, **kwargs):
        self.calls.append((list(argv), kwargs))
        for prefix, value in self.scripts:
            if argv[:len(prefix)] == list(prefix):
                if callable(value):
                    value = value(argv)
                if isinstance(value, BaseException):
                    raise value
                return value
        raise AssertionError(f'unscripted host command: {argv}')

    def argv(self, *prefix):
        return [call for call, _ in self.calls if call[:len(prefix)] == list(prefix)]


class RegistryTests(unittest.TestCase):
    # The owner is any object; the registry only ever compares identity. These use plain
    # sentinels so the store's own contract is tested without a driver in the way.
    OWNER = object()
    OTHER = object()

    def test_mint_then_require_owned_then_release(self):
        registry = SessionRegistry()
        registry.mint('a', self.OWNER)
        registry.require_owned('a')  # does not raise
        registry.release('a')
        with self.assertRaises(ForeignSessionError):
            registry.require_owned('a')

    def test_foreign_session_is_refused(self):
        registry = SessionRegistry()
        registry.mint('a', self.OWNER)
        with self.assertRaises(ForeignSessionError):
            registry.require_owned('some-real-background-session-id')

    def test_duplicate_mint_and_empty_id_are_rejected(self):
        registry = SessionRegistry()
        registry.mint('a', self.OWNER)
        with self.assertRaises(ValueError):
            registry.mint('a', self.OWNER)
        with self.assertRaises(ValueError):
            registry.mint('', self.OWNER)

    def test_release_of_foreign_session_is_refused(self):
        registry = SessionRegistry()
        with self.assertRaises(ForeignSessionError):
            registry.release('never-created')

    def test_the_id_and_its_owner_are_stored_together(self):
        # One store, so an interrupt cannot leave a created session registered and unattributed:
        # the sweep would then pass over a live session while its key blocked re-registration.
        registry = SessionRegistry()
        registry.mint('a', self.OWNER)
        registry.mint('b', self.OTHER)
        self.assertIs(registry.owner('a'), self.OWNER)
        self.assertEqual(registry.owned_by(self.OWNER), {'a'})
        self.assertEqual(registry.owned_by(self.OTHER), {'b'})
        registry.release('a')
        self.assertEqual(registry.owned_by(self.OWNER), set())


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

    def test_signals_name_which_event_established_each_outcome(self):
        events = [
            Event(role='user', text=f'do the thing {MARKER}', time=10.0),
            Event(role='assistant', text=f'ok, saw {MARKER}', time=11.0),
        ]
        observation = detect_outcomes(events, MARKER, submitted_at=9.0)
        self.assertEqual(observation.signals,
                          {'visible': SIGNAL_USER_MESSAGE, 'turn_start': SIGNAL_ASSISTANT_MESSAGE,
                           'ack': SIGNAL_ASSISTANT_MESSAGE})

    def test_turn_stream_signal_credits_the_boundary_event_not_the_assistant_message(self):
        events = [Event(role='turn_start', text='', time=10.0),
                  Event(role='assistant', text=f'ok, saw {MARKER}', time=11.0)]
        observation = detect_outcomes(events, MARKER, submitted_at=9.0, turn_stream=True)
        self.assertEqual(observation.outcomes['turn_start'], 10.0)
        self.assertEqual(observation.signals['turn_start'], SIGNAL_TURN_BOUNDARY_EVENT)

    def test_turn_stream_with_no_boundary_event_carries_no_turn_start_signal(self):
        events = [Event(role='assistant', text='still finishing', time=11.0)]
        observation = detect_outcomes(events, MARKER, submitted_at=9.0, turn_stream=True)
        self.assertNotIn('turn_start', observation.outcomes)
        self.assertNotIn('turn_start', observation.signals)

    def test_the_model_comes_from_the_first_in_window_assistant_event(self):
        # A session can change model between turns (captured: creation under `claude-sonnet-5`,
        # the no-flag resume reply under `claude-opus-5`), so a cell must cite the model that
        # served this trial, not whichever one the session started under.
        events = [Event(role='assistant', text='before', time=1.0, model='claude-sonnet-5'),
                  Event(role='user', text=MARKER, time=10.0),
                  Event(role='assistant', text=f'ok {MARKER}', time=11.0, model='claude-opus-5'),
                  Event(role='assistant', text='more', time=12.0, model='claude-sonnet-5')]
        self.assertEqual(detect_outcomes(events, MARKER, submitted_at=9.0).model, 'claude-opus-5')

    def test_the_model_is_none_when_no_in_window_assistant_event_names_one(self):
        events = [Event(role='user', text=MARKER, time=10.0),
                  Event(role='assistant', text='reply', time=11.0)]
        self.assertIsNone(detect_outcomes(events, MARKER, submitted_at=9.0).model)


def claude_record(kind, content, *, stamp='2026-09-13T09:00:00.000Z', version='2.1.270',
                  model=None, **extra):
    message = {'role': kind, 'content': content}
    if model is not None:
        message['model'] = model
    record = {'type': kind, 'timestamp': stamp, 'message': message,
              'sessionId': SESSION_UUID, 'version': version, 'uuid': 'u', 'parentUuid': None}
    record.update(extra)
    return json.dumps(record)


class ClaudeParsingTests(unittest.TestCase):
    def test_backgrounded_line_yields_the_short_id_from_line_one_only(self):
        # Captured stdout: `backgrounded · 69aa52ed` then four hint lines.
        stdout = 'backgrounded · 69aa52ed\n  claude attach 69aa52ed\n  claude logs 69aa52ed\n'
        self.assertEqual(backgrounded_id(stdout), '69aa52ed')
        self.assertIsNone(backgrounded_id('hint\nbackgrounded · 69aa52ed\n'))
        self.assertIsNone(backgrounded_id(''))
        self.assertIsNone(backgrounded_id('backgrounded · notahexid\n'))

    def test_transcript_extracts_user_string_and_assistant_text_parts(self):
        # Captured shape: user content is a string; assistant content is a list of typed parts,
        # some records carrying only a `thinking` part.
        lines = [
            claude_record('user', MARKER, stamp='2026-09-13T09:00:01.000Z'),
            claude_record('assistant', [{'type': 'thinking', 'thinking': 'hmm'}],
                          stamp='2026-09-13T09:00:02.000Z'),
            claude_record('assistant', [{'type': 'text', 'text': f'ack {MARKER}'}],
                          stamp='2026-09-13T09:00:03.000Z'),
        ]
        events, unusable = claude_transcript_events(lines)
        self.assertEqual([(e.role, e.text) for e in events],
                          [('user', MARKER), ('assistant', ''), ('assistant', f'ack {MARKER}')])
        self.assertLess(events[0].time, events[2].time)
        self.assertEqual(unusable, 0)

    def test_bookkeeping_record_types_are_skipped_not_counted(self):
        # Captured: attachment/system carry timestamps; file-history-snapshot, last-prompt,
        # mode, permission-mode, ai-title, ... carry none. None of them is a failed read.
        lines = [json.dumps({'type': 'file-history-snapshot', 'snapshot': {}}),
                 json.dumps({'type': 'attachment', 'timestamp': '2026-09-13T09:00:00.000Z'}),
                 json.dumps({'type': 'last-prompt', 'lastPrompt': 'x'}),
                 json.dumps({'type': 'system', 'timestamp': '2026-09-13T09:00:00.000Z'})]
        self.assertEqual(claude_transcript_events(lines), ([], 0))

    def test_unparseable_non_object_or_untyped_lines_are_unusable(self):
        lines = ['not json', json.dumps(None), json.dumps([1]), json.dumps({'timestamp': 'x'}),
                 json.dumps({'type': 3})]
        self.assertEqual(claude_transcript_events(lines), ([], 5))

    def test_message_records_with_bad_timestamp_or_role_or_content_are_unusable(self):
        lines = [
            claude_record('user', MARKER, stamp='not-a-date'),
            claude_record('user', MARKER, stamp='2026-09-13T09:00:00'),  # naive
            json.dumps({'type': 'user', 'timestamp': '2026-09-13T09:00:00.000Z',
                        'message': {'role': 'assistant', 'content': MARKER}}),
            json.dumps({'type': 'user', 'timestamp': '2026-09-13T09:00:00.000Z', 'message': None}),
            claude_record('assistant', {'type': 'text', 'text': 'x'}),  # object, not list
            claude_record('assistant', ['not-a-part']),
            claude_record('assistant', [{'type': 'text', 'text': None}]),
            claude_record('user', 7),
        ]
        self.assertEqual(claude_transcript_events(lines), ([], 8))

    def test_cross_role_content_shapes_are_unusable_rather_than_promoted(self):
        # The captured contract is role-specific: a string only on user records, a typed-part
        # list only on assistant records. An assistant string carrying the marker must not become
        # an acknowledgement, and a user part list must not establish visibility -- a changed or
        # malformed transcript makes the read unobservable instead.
        lines = [claude_record('assistant', f'ack {MARKER}'),
                 claude_record('user', [{'type': 'text', 'text': MARKER}])]
        self.assertEqual(claude_transcript_events(lines), ([], 2))

    def test_assistant_parts_require_a_captured_type(self):
        parts = [{'text': MARKER}] + [dict(type=kind, text=MARKER)
                                     for kind in (None, 1, [], {}, '', ' ', 'future-part')]
        for part in parts:
            with self.subTest(part=part):
                self.assertEqual(claude_transcript_events([claude_record('assistant', [part])]),
                                 ([], 1))

    def test_the_assistant_records_model_is_carried_and_never_inferred(self):
        # Captured at 2.1.270: `message.model` names the serving model, and it is not the one
        # `--model` asked for. Nothing else on a record supplies it, so anything but a string on
        # an assistant record reads as None rather than as evidence.
        lines = [claude_record('user', MARKER, model='claude-haiku-ignored'),
                 claude_record('assistant', [{'type': 'text', 'text': 'a'}],
                               model='claude-sonnet-5', stamp='2026-09-13T09:00:02.000Z'),
                 claude_record('assistant', [{'type': 'text', 'text': 'b'}],
                               model=7, stamp='2026-09-13T09:00:03.000Z'),
                 claude_record('assistant', [{'type': 'text', 'text': 'c'}],
                               stamp='2026-09-13T09:00:04.000Z')]
        events, unusable = claude_transcript_events(lines)
        self.assertEqual([event.model for event in events], [None, 'claude-sonnet-5', None, None])
        self.assertEqual(unusable, 0)  # a missing model is not a failed read

    def test_session_version_reads_the_single_recorded_version(self):
        lines = [json.dumps({'type': 'mode', 'mode': 'x'}),
                 claude_record('user', 'hi', version='2.1.270'),
                 claude_record('assistant', [{'type': 'text', 'text': 'yo'}], version='2.1.270')]
        self.assertEqual(claude_session_version(lines), '2.1.270')

    def test_session_version_is_none_when_absent_or_disagreeing(self):
        self.assertIsNone(claude_session_version([json.dumps({'type': 'user', 'message': {}})]))
        self.assertIsNone(claude_session_version(['not json', '']))
        self.assertIsNone(claude_session_version([claude_record('user', 'a', version='2.1.269'),
                                                  claude_record('user', 'b', version='2.1.270')]))
        self.assertIsNone(claude_session_version([claude_record('user', 'a', version=3)]))


class CodexParsingTests(unittest.TestCase):
    def test_transcript_discovery_rejects_non_uuid_ids_before_globbing(self):
        for lookup in (host_trials.default_codex_rollout_path, host_trials.default_claude_transcript_path):
            for session_id in ('*', '../*', '--help', THREAD_ID + '[ab]'):
                with self.subTest(lookup=lookup.__name__, session_id=session_id):
                    with unittest.mock.patch.object(host_trials.glob, 'glob', return_value=['foreign-path']) as globber, \
                            unittest.mock.patch.object(os.path, 'getmtime', return_value=1):
                        self.assertIsNone(lookup(session_id))
                        globber.assert_not_called()

    def test_codex_home_metacharacters_remain_literal_during_discovery(self):
        with tempfile.TemporaryDirectory(prefix='parley[owned]-') as home:
            directory = os.path.join(home, 'sessions', '2026', '09', '13')
            os.makedirs(directory)
            path = os.path.join(directory, f'rollout-synthetic-{THREAD_ID}.jsonl')
            with open(path, 'w') as handle:
                handle.write('{}\n')
            with unittest.mock.patch.dict(os.environ, {'CODEX_HOME': home}):
                self.assertEqual(host_trials.default_codex_rollout_path(THREAD_ID), path)

    def test_thread_ids_come_from_thread_started_events(self):
        stdout = '\n'.join([json.dumps({'type': 'thread.started', 'thread_id': THREAD_ID}),
                            json.dumps({'type': 'turn.started'}), 'not json'])
        self.assertEqual(codex_thread_ids(stdout), ({THREAD_ID: None}, 1))
        self.assertEqual(codex_thread_ids(json.dumps({'type': 'turn.started'})), ({}, 0))
        self.assertEqual(codex_thread_ids(json.dumps({'type': 'thread.started', 'thread_id': ''})), ({}, 1))
        self.assertEqual(codex_thread_ids(json.dumps({'type': 'thread.started', 'thread_id': 5})), ({}, 1))
        self.assertEqual(codex_thread_ids(''), ({}, 0))

    def test_rollout_extracts_user_and_assistant_skips_developer_and_other_types(self):
        lines = [
            json.dumps({'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'response_item',
                        'payload': {'type': 'message', 'role': 'developer',
                                    'content': [{'type': 'input_text', 'text': 'system prompt'}]}}),
            json.dumps({'timestamp': '2026-09-11T00:00:01.000Z', 'type': 'response_item',
                        'payload': {'type': 'message', 'role': 'user',
                                    'content': [{'type': 'input_text', 'text': MARKER}]}}),
            # An unrelated outer record type whose payload.type happens to collide with a real
            # inner type must not be read as evidence -- the outer type gates first.
            json.dumps({'timestamp': '2026-09-11T00:00:02.000Z', 'type': 'turn_context',
                        'payload': {'type': 'event_msg'}}),
            json.dumps({'timestamp': '2026-09-11T00:00:03.000Z', 'type': 'response_item',
                        'payload': {'type': 'message', 'role': 'assistant',
                                    'content': [{'type': 'output_text', 'text': f'ack {MARKER}'}]}}),
        ]
        events, undated = codex_rollout_events(lines)
        self.assertEqual([(e.role, e.text) for e in events],
                          [('user', MARKER), ('assistant', f'ack {MARKER}')])
        self.assertLess(events[0].time, events[1].time)
        self.assertEqual(undated, 0)

    def test_an_unknown_message_role_is_unusable_not_silently_skipped(self):
        # A missing or schema-drifted role (e.g. a future "model") is not the same as the known,
        # intentionally-ignored `developer` role -- treating it the same way could silently drop
        # a current-turn assistant message while the rollout still reads observable=True.
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'response_item',
                             'payload': {'type': 'message', 'role': 'model',
                                         'content': [{'text': MARKER}]}})]
        self.assertEqual(codex_rollout_events(lines), ([], 1))

    def test_a_non_string_payload_type_is_unusable_rather_than_a_typeerror(self):
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'event_msg',
                             'payload': {'type': []}})]
        self.assertEqual(codex_rollout_events(lines), ([], 1))

    def test_a_relevant_record_with_a_non_object_payload_is_unusable(self):
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'response_item',
                             'payload': None})]
        self.assertEqual(codex_rollout_events(lines), ([], 1))

    def test_unparseable_lines_count_as_unusable_while_other_record_types_do_not(self):
        # A record this runner has no use for is not a failed read; a line that is not JSON is.
        lines = ['not json', json.dumps({'type': 'world_state', 'payload': {'type': 'world_state'}}), '']
        self.assertEqual(codex_rollout_events(lines), ([], 1))
        self.assertEqual(
            codex_rollout_events([json.dumps({'type': 'world_state', 'payload': {'type': 'world_state'}})]),
            ([], 0))

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
            json.dumps({'type': 'response_item',
                        'payload': {'type': 'message', 'role': 'assistant',
                                    'content': [{'type': 'output_text', 'text': 'undated'}]}}),
            json.dumps({'timestamp': 'not-a-date', 'type': 'response_item',
                        'payload': {'type': 'message', 'role': 'user',
                                    'content': [{'type': 'input_text', 'text': 'malformed'}]}}),
        ]
        self.assertEqual(codex_rollout_events(lines), ([], 2))

    def test_malformed_content_shapes_are_unusable_rather_than_empty_or_a_typeerror(self):
        # Explicit null, a single object instead of a list of parts, a non-string part text, a
        # non-dict part, a part omitting `text`, and a part type mismatched with the role --
        # each read as an uncaptured shape, never as an ordinary-looking empty message.
        base = {'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'response_item'}
        contents = [
            ('user', None),
            ('assistant', {'type': 'tool_call', 'name': 'x'}),
            ('user', [{'text': None}]),
            ('user', ['not-a-part', {'text': 'hi'}]),
            ('assistant', [{'type': 'text'}]),
            ('user', [{'type': 'output_text', 'text': MARKER}]),
        ]
        for role, content in contents:
            with self.subTest(role=role, content=content):
                line = json.dumps({**base, 'payload': {'type': 'message', 'role': role, 'content': content}})
                self.assertEqual(codex_rollout_events([line]), ([], 1))

    def test_a_payload_missing_its_type_key_is_unusable_not_silently_skipped(self):
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'response_item',
                             'payload': {'role': 'assistant',
                                         'content': [{'type': 'output_text', 'text': MARKER}]}}),
                json.dumps({'type': 'event_msg', 'payload': {'role': 'assistant'}})]
        self.assertEqual(codex_rollout_events(lines), ([], 2))

    def test_a_record_missing_its_outer_type_key_is_unusable_not_silently_skipped(self):
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z',
                             'payload': {'type': 'message', 'role': 'assistant',
                                         'content': [{'type': 'output_text', 'text': MARKER}]}}),
                json.dumps({'type': ['response_item'],
                            'payload': {'type': 'message', 'role': 'assistant',
                                        'content': [{'type': 'output_text', 'text': MARKER}]}})]
        self.assertEqual(codex_rollout_events(lines), ([], 2))

    def test_a_non_object_record_is_unusable_rather_than_an_attributeerror(self):
        lines = [json.dumps(None), json.dumps([1, 2]), json.dumps(3),
                json.dumps({'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'response_item',
                            'payload': {'type': 'message', 'role': 'user',
                                        'content': [{'type': 'input_text', 'text': 'hi'}]}})]
        events, unusable = codex_rollout_events(lines)
        self.assertEqual(unusable, 3)
        self.assertEqual(len(events), 1)

    def test_an_unrelated_outer_record_type_is_skipped_regardless_of_payload_shape(self):
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'turn_context',
                             'payload': {'type': 'message', 'role': 'user',
                                         'content': [{'text': MARKER}]}})]
        self.assertEqual(codex_rollout_events(lines), ([], 0))

    def test_codex_session_version_reads_the_confirmed_payload_shape(self):
        lines = [json.dumps({'timestamp': '2026-09-13T09:41:51.650Z', 'type': 'session_meta',
                             'payload': {'cli_version': '0.154.0'}})]
        self.assertEqual(codex_session_version(lines), '0.154.0')

    def test_codex_session_version_is_none_without_a_usable_session_meta_record(self):
        self.assertIsNone(codex_session_version([json.dumps({'type': 'response_item'})]))
        self.assertIsNone(codex_session_version([json.dumps({'type': 'session_meta', 'payload': {}})]))
        self.assertIsNone(codex_session_version([json.dumps({'type': 'session_meta',
                                                             'payload': {'cli_version': 7}})]))

    def test_codex_session_version_skips_unparseable_and_non_object_lines(self):
        lines = ['not json', 'null', '42', '[1, 2]',
                 json.dumps({'type': 'session_meta', 'payload': {'cli_version': '0.154.0'}})]
        self.assertEqual(codex_session_version(lines), '0.154.0')


def opencode_export(messages, *, version='1.18.30', session_id='ses_1'):
    return json.dumps({'info': {'id': session_id, 'version': version,
                                'time': {'created': 1, 'updated': 2}},
                       'messages': messages})


def opencode_message(role, parts, created_ms, **info):
    info = {'id': f'msg_{role}_{created_ms}', 'sessionID': 'ses_1', **info}
    if isinstance(parts, list):
        parts = [{'sessionID': info['sessionID'], 'messageID': info['id'], **part}
                 if isinstance(part, dict) else part for part in parts]
    return {'info': {'role': role, 'time': {'created': created_ms}, **info}, 'parts': parts}


class OpenCodeParsingTests(unittest.TestCase):
    def test_export_parts_require_a_captured_type(self):
        parts = [{'text': MARKER}] + [dict(type=kind, text=MARKER)
                                     for kind in (None, 1, [], {}, '', ' ', 'future-part')]
        for part in parts:
            for role in ('user', 'assistant'):
                with self.subTest(part=part, role=role):
                    raw = opencode_export([opencode_message(role, [part], 1_757_754_001_000)])
                    self.assertEqual(opencode_export_events(raw), ([], 1))

    def test_session_ids_are_collected_and_an_error_event_is_reported(self):
        # Captured: a failing turn (401) still emits its sessionID and lists the session.
        stdout = '\n'.join([json.dumps({'type': 'step_start', 'sessionID': 'ses_1'}),
                            json.dumps({'type': 'error', 'sessionID': 'ses_1',
                                        'error': {'name': 'ProviderAuthError'}}),
                            'not json', json.dumps([1])])
        session_ids, error, unusable = opencode_session_ids(stdout)
        self.assertEqual(list(session_ids), ['ses_1'])
        self.assertIn('ProviderAuthError', error)
        self.assertEqual(unusable, 2)
        self.assertEqual(opencode_session_ids(''), ({}, None, 0))
        self.assertEqual(opencode_session_ids(json.dumps({'type': 'text', 'sessionID': ''})), ({}, None, 0))

    def test_export_extracts_text_parts_with_millisecond_creation_times(self):
        raw = opencode_export([
            opencode_message('user', [{'type': 'text', 'text': MARKER}], 1_757_754_001_000),
            opencode_message('assistant', [{'type': 'step-start'},
                                           {'type': 'reasoning', 'text': 'hmm'},
                                           {'type': 'text', 'text': f'ack {MARKER}'}], 1_757_754_002_500),
        ])
        events, unusable = opencode_export_events(raw)
        self.assertEqual([(e.role, e.text, e.time) for e in events],
                          [('user', MARKER, 1_757_754_001.0), ('assistant', f'ack {MARKER}', 1_757_754_002.5)])
        self.assertEqual(unusable, 0)

    def test_malformed_export_documents_and_messages_are_unusable(self):
        for raw in ('not json', json.dumps([1]), json.dumps({'messages': 'x'}), json.dumps({})):
            with self.subTest(raw=raw):
                self.assertEqual(opencode_export_events(raw), ([], 1))
        messages = [
            'not-a-message',
            {'info': None, 'parts': []},
            {'info': {'role': 'system', 'time': {'created': 1}}, 'parts': []},
            {'info': {'role': 'user', 'time': {'created': 'x'}}, 'parts': []},
            {'info': {'role': 'user', 'time': {'created': True}}, 'parts': []},
            {'info': {'role': 'user', 'time': {'created': float('inf')}}, 'parts': []},
            {'info': {'role': 'user'}, 'parts': []},
            {'info': {'role': 'user', 'time': {'created': 1}}, 'parts': None},
            {'info': {'role': 'user', 'time': {'created': 1}}, 'parts': ['x']},
            {'info': {'role': 'user', 'time': {'created': 1}}, 'parts': [{'type': 'text', 'text': 2}]},
        ]
        self.assertEqual(opencode_export_events(opencode_export(messages)), ([], len(messages)))

    def test_export_carries_the_assistant_provider_and_model_only(self):
        raw = opencode_export([
            opencode_message('user', [{'type': 'text', 'text': MARKER}], 1_757_754_001_000,
                             providerID='opencode', modelID='ling-3.0-flash-fin-free'),
            opencode_message('assistant', [{'type': 'text', 'text': 'a'}], 1_757_754_002_000,
                             providerID='opencode', modelID='ling-3.0-flash-fin-free'),
            opencode_message('assistant', [{'type': 'text', 'text': 'b'}], 1_757_754_003_000,
                             modelID='ling-3.0-flash-fin-free'),  # provider absent: report it bare
            opencode_message('assistant', [{'type': 'text', 'text': 'c'}], 1_757_754_004_000,
                             providerID='opencode', modelID=7),
        ])
        events, unusable = opencode_export_events(raw)
        self.assertEqual([event.model for event in events],
                          [None, 'opencode/ling-3.0-flash-fin-free', 'ling-3.0-flash-fin-free', None])
        self.assertEqual(unusable, 0)  # the model is evidence, never a reason to fail the read

    def test_export_version_reads_info_version(self):
        self.assertEqual(opencode_export_version(opencode_export([])), '1.18.30')
        self.assertIsNone(opencode_export_version(opencode_export([], version=3)))
        self.assertIsNone(opencode_export_version('not json'))
        self.assertIsNone(opencode_export_version(json.dumps({'info': 'x'})))


class StripAnsiTests(unittest.TestCase):
    def test_csi_osc_and_two_byte_escapes_are_removed_and_cr_becomes_newline(self):
        raw = b'\x1b[2J\x1b[1;1H\x1b]0;title\x07hello\x1b[0 q \x1b=\x1b(B\r\nworld\rx\x1bM'
        self.assertEqual(strip_ansi(raw), 'hello \nworld\nx')

    def test_the_captured_claude_composer_line_matches_the_ready_pattern(self):
        # Captured attach screen: a rule, then `❯` + NBSP alone on its line, then a rule.
        screen = '────\r\n❯\xa0\r\n────\r\n[Sonnet 5] │ ⌂ claude\r\n'
        self.assertRegex(strip_ansi(screen.encode()), CLAUDE_READY_PATTERN)
        # An earlier prompt echoed back with text after the chevron is not the idle composer.
        self.assertNotRegex('❯ tell me a joke\n', CLAUDE_READY_PATTERN)

    def test_the_captured_codex_composer_and_trust_dialog_match_their_patterns(self):
        # Both as they read after strip_ansi on the real captured screens: the composer keeps its
        # spaces, the trust dialog loses them (the TUI places words with cursor moves).
        self.assertRegex('› Ask Codex to do anything   ? for shortcuts', CODEX_READY_PATTERN)
        collapsed = ('>You are in <probe-cwd>Doyoutrustthecontentsofthisdirectory?Workingwithuntrusted'
                     'contents...› 1. Yes, continue2.No,quitPress enter to continue')
        self.assertRegex(collapsed, CODEX_TRUST_PATTERN)
        self.assertRegex('Do you trust the contents of this directory?', CODEX_TRUST_PATTERN)


PTY_CHILD = '''
import sys, termios, time
config = termios.tcgetattr(0)
config[3] &= ~termios.ECHO
termios.tcsetattr(0, termios.TCSANOW, config)
print('BOOT', flush=True)
time.sleep(0.3)
print('\\x1b[1m> Ask me anything\\x1b[0m', flush=True)
for line in sys.stdin:
    line = line.rstrip('\\n')
    if line == 'quit':
        break
    print('GOT:' + line, flush=True)
'''


class PtyClientTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def spawn(self, code=PTY_CHILD, **kwargs):
        client = PtyClient([sys.executable, '-u', '-c', code], cwd=self.tmp.name,
                           env={'PATH': os.defpath, 'HOME': self.tmp.name}, **kwargs)
        self.addCleanup(client.close)
        return client

    def test_wait_for_needs_the_pattern_and_a_quiet_period(self):
        client = self.spawn()
        self.assertTrue(client.wait_for(r'> Ask me anything', quiet=0.3, timeout=5))
        # The pattern arrived after BOOT; the stripped screen holds both, ANSI removed.
        self.assertIn('BOOT\n> Ask me anything', client.screen())
        self.assertFalse(client.wait_for(r'never printed', quiet=0.1, timeout=0.5))

    def test_a_select_failure_invalidates_the_pty_client(self):
        with unittest.mock.patch.object(host_trials.select, 'select', side_effect=OSError('lost fd')):
            client = self.spawn()
            client.thread.join(timeout=2)
        self.assertFalse(client.thread.is_alive())
        self.assertTrue(client.eof)
        self.assertFalse(client.wait_for(r'.*', quiet=0, timeout=0))
        with self.assertRaises(ValueError):
            client.send_keys(b'unsafe\r')

    def test_a_client_that_cannot_start_its_drain_thread_closes_the_child_it_launched(self):
        # The child is already running and the half-built client is about to be discarded, so
        # nothing would ever hold a handle to it.
        launched = []
        real = host_trials.PtyProcess

        def recording(*args, **kwargs):
            launched.append(real(*args, **kwargs))
            return launched[-1]

        class Refusing(threading.Thread):
            def start(self):
                raise RuntimeError('cannot start thread')

        with unittest.mock.patch.object(host_trials, 'PtyProcess', recording), \
             unittest.mock.patch.object(host_trials.threading, 'Thread', Refusing):
            with self.assertRaises(RuntimeError):
                PtyClient([sys.executable, '-u', '-c', PTY_CHILD], cwd=self.tmp.name,
                          env={'PATH': os.defpath, 'HOME': self.tmp.name})
        self.assertEqual(len(launched), 1)
        self.assertIsNotNone(launched[0].exit_code)  # reaped, not left running

    def test_type_line_delivers_a_long_line_in_chunks_and_ends_with_enter(self):
        client = self.spawn()
        self.assertTrue(client.wait_for(r'> Ask me anything', quiet=0.3, timeout=5))
        since = client.mark()
        message = marker_message(MARKER)  # longer than one chunk
        client.type_line(message, chunk=16, gap=0.01, pause=0.05)
        self.assertTrue(client.wait_for(re.escape('GOT:' + message), quiet=0.1, timeout=5, since=since))
        self.assertIn(f'GOT:{message}', client.text_since(since))

    def test_the_drain_thread_keeps_reading_past_the_process_transcript_cap(self):
        # A held client must not stall the child or trip PtyProcess's 1 MiB `_record` cap: the
        # child emits 2 MiB and the client keeps only its bounded window, latest bytes last.
        # The trailing read keeps the child alive: `wait_for` refuses to call an exited client
        # ready, so a child that printed and exited could never satisfy it.
        code = '''
import sys
for i in range(2048):
    sys.stdout.write('L%05d ' % i + 'x' * 1017 + '\\n')
    sys.stdout.flush()
print('DONE', flush=True)
sys.stdin.readline()
'''
        client = self.spawn(code, window=64 * 1024)
        self.assertTrue(client.wait_for(r'DONE', quiet=0.2, timeout=20))
        self.assertGreater(client.total, 2 * 1024 * 1024)
        self.assertLessEqual(len(client.window), 64 * 1024)
        self.assertIn('L02047', client.screen())
        self.assertNotIn('L00000', client.text_since(0))

    def test_close_reaps_the_child_and_send_keys_refuses_afterwards(self):
        client = self.spawn()
        self.assertTrue(client.wait_for(r'> Ask me anything', quiet=0.3, timeout=5))
        client.close()
        self.assertIsNotNone(client.exit_code)
        with self.assertRaises(ValueError):
            client.send_keys(b'x')

    def test_child_exit_marks_eof_and_wait_for_returns_false_promptly(self):
        client = self.spawn()
        self.assertTrue(client.wait_for(r'> Ask me anything', quiet=0.3, timeout=5))
        client.type_line('quit', chunk=8, gap=0.0, pause=0.0)
        started = time.monotonic()
        self.assertFalse(client.wait_for(r'never', quiet=0.1, timeout=10))
        self.assertLess(time.monotonic() - started, 8)
        self.assertTrue(client.eof)

    def test_wait_for_refuses_a_composer_drawn_by_a_child_that_then_exited(self):
        # Readiness is a property of a live client. A child that drew the composer and exited
        # serves nothing: retaining it would let `CodexDriver.submit` read a nonempty client list
        # as proof that a queued message has a serving process.
        client = self.spawn("print('> Ask me anything', flush=True)")
        self.assertFalse(client.wait_for(r'> Ask me anything', quiet=0.5, timeout=5))
        self.assertTrue(client.eof)
        self.assertIn('> Ask me anything', client.screen())  # it did appear; the exit is what refuses

    def test_env_carries_xterm_term_and_the_resolved_cwd(self):
        code = ("import os, sys; print('TERM=' + os.environ['TERM'] + ' PWD=' + os.environ['PWD'], "
                "flush=True); sys.stdin.readline()")  # stays alive: an exited client is never ready
        client = self.spawn(code)
        self.assertTrue(client.wait_for(r'TERM=xterm-256color PWD=' + re.escape(os.path.realpath(self.tmp.name)),
                                        quiet=0.1, timeout=5))


class FakePtyClient:
    """Scripted TUI. `ready` False means the composer never appears after startup (or, with
    `trust_prompt`, after the trust dialog is answered). `trust_prompt` starts on the captured
    escape-stripped dialog, words run together, with the composer placeholder already drawn
    beneath it -- so a readiness match alone cannot tell the two screens apart. `eof` mirrors the
    real client's: a client whose child exited is never ready and refuses every write."""

    launched = []

    def __init__(self, argv, *, cwd, ready=True, trust_prompt=False, eof=False):
        self.argv = argv
        self.cwd = cwd
        self.ready = ready
        self.trust_prompt = trust_prompt
        self.eof = eof
        # Set by a test after launch to make the next write fail the way `os.write` can, without
        # the client having exited (the real `send_keys` propagates `OSError` straight through).
        self.write_error = None
        self.keys = []
        self.typed = []
        self.closed = False
        self.text = ('Doyoutrustthecontentsofthisdirectory?\n› Ask Codex to do anything\n'
                     if trust_prompt else '')
        FakePtyClient.launched.append(self)

    def mark(self):
        return len(self.text)

    def text_since(self, offset=0):
        return self.text[offset:]

    def screen(self, limit=1500):
        return self.text[-limit:]

    def wait_for(self, pattern, *, timeout, quiet=1.0, since=0):
        if self.eof:
            return False
        answered = b'\r' in self.keys
        if self.ready and (not self.trust_prompt or answered):
            self.text += '❯\xa0\n› Ask Codex to do anything\n'
        return bool(re.search(pattern, self.text[since:]))

    def send_keys(self, data):
        if self.closed or self.eof:
            raise ValueError('closed or exited session')
        if self.write_error is not None:
            raise self.write_error
        self.keys.append(data)
        return len(data)

    def type_line(self, text, **kwargs):
        self.typed.append(text)
        self.send_keys(text.encode())
        self.send_keys(b'\r')

    def close(self):
        self.closed = True


class DriverTestCase(unittest.TestCase):
    def setUp(self):
        # These small Linux probe fixtures need a trusted sticky ancestor; inherited TMPDIR
        # may have group-writable parents. Build caches and verification logs stay in scratch.
        self.tmp = tempfile.TemporaryDirectory(dir='/tmp')
        self.addCleanup(self.tmp.cleanup)
        self.cwd = self.tmp.name
        # Fixture files live in their own private directory: the probe cwd must stay empty, and
        # its parent is the shared temp root where fixed names would collide across runs.
        self.fixtures = tempfile.TemporaryDirectory(dir='/tmp')
        self.addCleanup(self.fixtures.cleanup)
        self.registry = SessionRegistry()
        FakePtyClient.launched = []
        self.transcripts = {}

    def git(self, *args):
        """Run one real `git` command, insulated from the operator's own configuration.

        `private_directory`'s `.git` exception is a claim about what `git init` leaves on disk,
        so the fixtures make a real one rather than a hand-built skeleton that could agree with
        a wrong predicate. `git` is not a host CLI and creates no session; the AGENTS.md rule it
        must not break is launching an installed Claude/Codex/OpenCode, which this does not.
        """
        env = {key: value for key, value in os.environ.items() if not key.startswith('GIT_')}
        env.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_SYSTEM=os.devnull,
                   GIT_CONFIG_NOSYSTEM='1', GIT_AUTHOR_NAME='probe', GIT_AUTHOR_EMAIL='probe@invalid',
                   GIT_COMMITTER_NAME='probe', GIT_COMMITTER_EMAIL='probe@invalid')
        # Installed Git is required evidence, so an unavailable/failed command must fail the
        # fixture rather than silently skipping the freshness check.
        subprocess.run(['git', *args], check=True, env=env, capture_output=True)

    def transcript_path_for(self, session_uuid):
        return self.transcripts.get(session_uuid)

    def write_lines(self, name, lines):
        path = os.path.join(self.fixtures.name, name)
        with open(path, 'w', encoding='utf-8') as handle:
            handle.write('\n'.join(lines) + '\n')
        return path


def listing(entries):
    return FakeResult(0, json.dumps(entries))


def claude_entry(short='69aa52ed', *, pid=4242, status='idle', state='done', cwd='/x'):
    return {'pid': pid, 'id': short, 'cwd': cwd, 'kind': 'background', 'startedAt': 1757754000000,
            'sessionId': short + SESSION_UUID[8:], 'name': None, 'status': status, 'state': state}


class ClaudeDriverTests(DriverTestCase):
    def test_listing_cannot_bind_a_short_id_to_an_unrelated_uuid(self):
        entry = claude_entry()
        entry['sessionId'] = 'deadbeef' + SESSION_UUID[8:]
        driver = self.driver(FakeRun([(['claude', 'agents'], listing([entry]))]))
        driver._mint('69aa52ed')
        with self.assertRaisesRegex(RuntimeError, 'malformed'):
            driver.status('69aa52ed')
        self.assertIsNone(driver.sessions['69aa52ed'])

    def test_listing_cannot_change_an_already_bound_full_uuid(self):
        entry = claude_entry()
        run = FakeRun([(['claude', 'agents'], lambda argv: listing([entry]))])
        driver = self.driver(run)
        driver._mint('69aa52ed')
        driver.status('69aa52ed')
        entry['sessionId'] = '69aa52ed-aaaa-4222-8333-444455556666'
        with self.assertRaisesRegex(RuntimeError, 'changed'):
            driver.status('69aa52ed')
        self.assertEqual(driver.sessions['69aa52ed'], SESSION_UUID)

    def test_interrupted_claude_creation_reports_candidates_without_adopting_them(self):
        candidates = [claude_entry(), claude_entry('deadbeef')]
        run = FakeRun([(['claude', '--bg'], KeyboardInterrupt()),
                       (['claude', 'agents'], listing(candidates))])
        driver = self.driver(run)
        with self.assertRaises(KeyboardInterrupt) as caught:
            run_trial_with_cleanup(driver, prompt='hello')
        self.assertEqual(driver.owned(), set())
        self.assertEqual(driver.strays, {'69aa52ed', 'deadbeef'})
        self.assertIn('manual investigation', ' '.join(caught.exception.__notes__))
        self.assertEqual(run.argv('claude', 'stop'), [])
        self.assertEqual(run.argv('claude', 'rm'), [])

    def test_interrupted_creation_preserves_cancellation_when_discovery_fails(self):
        for recovery_error in (subprocess.TimeoutExpired(['claude'], 15), KeyboardInterrupt()):
            with self.subTest(recovery_error=type(recovery_error).__name__):
                original = KeyboardInterrupt()
                run = FakeRun([(['claude', '--bg'], original), (['claude', 'agents'], recovery_error)])
                driver = self.driver(run)
                with self.assertRaises(KeyboardInterrupt) as caught:
                    run_trial_with_cleanup(driver, prompt='hello')
                self.assertIs(caught.exception, original)
                self.assertIn('discovery failed', ' '.join(caught.exception.__notes__))
                self.assertEqual(driver.owned(), set())

    def test_claude_transcript_removed_between_discovery_and_stat_is_unobservable(self):
        driver = self.driver(FakeRun([
            (['claude', '--bg'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
            (['claude', 'agents'], listing([claude_entry()]))]),
            transcript_path_for=host_trials.default_claude_transcript_path)
        driver.create('hello')
        with unittest.mock.patch.object(host_trials.glob, 'glob', return_value=['synthetic-transcript']), \
                unittest.mock.patch.object(os.path, 'getmtime', side_effect=FileNotFoundError):
            self.assertFalse(driver.observe('69aa52ed', marker=MARKER, submitted_at=0).observable)
            self.assertIsNone(driver.version('69aa52ed'))

    def driver(self, run, **kwargs):
        kwargs.setdefault('transcript_path_for', self.transcript_path_for)
        kwargs.setdefault('pty', FakePtyClient)
        # One fake clock drives both the sleeps and the deadlines they wait out, so a bounded
        # wait (the 5s version read, the 10s pre-detach check) ends without real time passing.
        self.clock = FakeClock()
        kwargs.setdefault('sleep', self.clock.sleep)
        kwargs.setdefault('monotonic', self.clock.monotonic)
        kwargs.setdefault('cwd', self.cwd)
        return ClaudeDriver(self.registry, run=run, **kwargs)

    def test_a_non_empty_probe_cwd_is_refused_at_construction(self):
        with open(os.path.join(self.cwd, 'notes.txt'), 'w') as handle:
            handle.write('x')
        with self.assertRaises(ValueError):
            self.driver(FakeRun([]))
        with self.assertRaises(ValueError):
            self.driver(FakeRun([]), cwd=os.path.join(self.cwd, 'missing'))

    def test_probe_cwd_must_be_operator_owned_and_not_writable_by_others(self):
        for mode in (0o770, 0o777):
            with self.subTest(mode=oct(mode)):
                os.chmod(self.cwd, mode)
                try:
                    with self.assertRaisesRegex(ValueError, 'writable'):
                        self.driver(FakeRun([]))
                finally:
                    os.chmod(self.cwd, 0o700)
        with unittest.mock.patch('os.geteuid', return_value=os.geteuid() + 1):
            with self.assertRaisesRegex(ValueError, 'owned'):
                self.driver(FakeRun([]))

    def test_writable_non_sticky_ancestors_are_refused(self):
        for mode in (0o770, 0o777):
            with self.subTest(mode=oct(mode)), tempfile.TemporaryDirectory(dir=self.fixtures.name) as parent:
                child = os.path.join(parent, 'nested', 'probe')
                os.makedirs(child, mode=0o700)
                os.chmod(parent, mode)
                with self.assertRaisesRegex(ValueError, 'ancestor'):
                    self.driver(FakeRun([]), cwd=child)

    def test_operator_owned_sticky_ancestor_preserves_child_ownership(self):
        with tempfile.TemporaryDirectory(dir=self.fixtures.name) as parent:
            child = os.path.join(parent, 'probe')
            os.mkdir(child, 0o700)
            os.chmod(parent, 0o1777)
            self.driver(FakeRun([]), cwd=child)

    def test_an_untrusted_ancestor_owner_is_refused_even_without_shared_write(self):
        original_stat = os.stat
        parent = os.path.dirname(self.cwd)

        def changed_owner(path, *args, **kwargs):
            result = original_stat(path, *args, **kwargs)
            if path == parent:
                fields = list(result)
                fields[4] = os.geteuid() + 1
                return os.stat_result(fields)
            return result

        with unittest.mock.patch('os.stat', side_effect=changed_owner):
            with self.assertRaisesRegex(ValueError, 'ancestor'):
                self.driver(FakeRun([]))

    def test_a_freshly_initialized_git_directory_is_allowed(self):
        # Real `git init`, not a hand-built skeleton: the predicate has to accept what the
        # captured Codex probe directory actually was.
        self.git('init', '--quiet', self.cwd)
        self.driver(FakeRun([]))  # does not raise
        self.driver(FakeRun([]))  # validation never changes the shared probe directory

    def test_git_fixtures_ignore_inherited_repository_and_template_settings(self):
        foreign = os.path.join(self.fixtures.name, 'foreign.git')
        with unittest.mock.patch.dict(os.environ, {'GIT_DIR': foreign,
                                                   'GIT_WORK_TREE': self.fixtures.name,
                                                   'GIT_OBJECT_DIRECTORY': foreign + '/objects',
                                                   'GIT_TEMPLATE_DIR': foreign + '/template'}):
            try:
                self.git('init', '--quiet', self.cwd)
            except unittest.SkipTest as error:
                self.fail(f'inherited Git configuration caused a skipped fixture: {error}')
        self.assertTrue(os.path.isdir(os.path.join(self.cwd, '.git')))
        self.assertFalse(os.path.exists(foreign))
        self.driver(FakeRun([]))

    def test_fresh_git_can_use_a_non_default_initial_branch(self):
        self.git('init', '--quiet', '--initial-branch=probe/initial', self.cwd)
        self.driver(FakeRun([]))

    def test_a_git_directory_carrying_history_is_refused(self):
        # An existing repository whose worktree files were deleted looks empty apart from `.git`,
        # and would feed the authenticated hosts real history and configuration.
        self.git('init', '--quiet', self.cwd)
        self.git('-C', self.cwd, 'commit', '--quiet', '--allow-empty', '-m', 'history')
        with self.assertRaises(ValueError):
            self.driver(FakeRun([]))

    def test_a_git_directory_with_non_initial_configuration_is_refused(self):
        self.git('init', '--quiet', self.cwd)
        config = os.path.join(self.cwd, '.git', 'config')
        with open(config) as handle:
            baseline = handle.read()
        for extra in ('\tfsmonitor = synthetic-command\n', '\thooksPath = elsewhere\n',
                      '\tpager = synthetic-command\n', '[include]\n\tpath = elsewhere\n',
                      '[alias]\n\tx = !synthetic-command\n',
                      '[filter "x"]\n\tclean = synthetic-command\n',
                      '\tbare = true\n', '\trepositoryformatversion = 1\n'):
            with self.subTest(extra=extra):
                with open(config, 'w') as handle:
                    handle.write(baseline + extra)
                with self.assertRaises(ValueError):
                    self.driver(FakeRun([]))

    def test_a_git_directory_with_an_active_hook_is_refused(self):
        self.git('init', '--quiet', self.cwd)
        with open(os.path.join(self.cwd, '.git', 'hooks', 'pre-commit'), 'w') as handle:
            handle.write('#!/bin/sh\nexit 0\n')
        with self.assertRaises(ValueError):
            self.driver(FakeRun([]))

    def test_other_git_metadata_must_match_a_fresh_initialization(self):
        for name, content in (('info/attributes', '* synthetic-attribute\n'),
                              ('HEAD', 'ref: refs/tags/other\n'),
                              ('HEAD', 'ref: refs/heads/../other\n'),
                              ('unexpected', 'uncontrolled metadata\n'),
                              ('description', 'uncontrolled instructions\n'),
                              ('info/exclude', 'uncontrolled-pattern\n'),
                              ('hooks/pre-commit.sample', 'uncontrolled sample\n')):
            with self.subTest(name=name):
                with tempfile.TemporaryDirectory(dir=self.fixtures.name) as cwd:
                    self.git('init', '--quiet', cwd)
                    with open(os.path.join(cwd, '.git', name), 'w') as handle:
                        handle.write(content)
                    with self.assertRaises(ValueError):
                        self.driver(FakeRun([]), cwd=cwd)

    def test_a_git_file_pointing_at_another_store_is_refused(self):
        # A linked worktree or a submodule: the store, and everything in it, lives elsewhere.
        with open(os.path.join(self.cwd, '.git'), 'w') as handle:
            handle.write(f'gitdir: {os.path.join(self.fixtures.name, "real-store")}\n')
        with self.assertRaises(ValueError):
            self.driver(FakeRun([]))

    def test_a_symlinked_git_store_is_refused(self):
        # `os.path.isdir` follows the link, so a `.git` symlink into a real repository read as a
        # directory; the freshness checks then described a store the probe directory never held.
        store = os.path.join(self.fixtures.name, 'elsewhere')
        self.git('init', '--quiet', store)
        os.symlink(os.path.join(store, '.git'), os.path.join(self.cwd, '.git'))
        with self.assertRaises(ValueError):
            self.driver(FakeRun([]))

    def test_a_symlink_beneath_a_fresh_git_directory_is_refused(self):
        # `os.walk('.git/refs')` and `os.listdir('.git/objects')` both follow a link given by
        # name, so a fresh-looking `.git` whose `refs` or `objects` points elsewhere would have
        # been judged on another repository's contents.
        store = os.path.join(self.fixtures.name, 'elsewhere')
        self.git('init', '--quiet', store)
        self.git('-C', store, 'commit', '--quiet', '--allow-empty', '-m', 'history')
        for name in ('refs', 'objects'):
            with self.subTest(name=name):
                fresh = tempfile.TemporaryDirectory(dir=self.fixtures.name)
                self.addCleanup(fresh.cleanup)
                self.git('init', '--quiet', fresh.name)
                target = os.path.join(fresh.name, '.git', name)
                shutil.rmtree(target)
                os.symlink(os.path.join(store, '.git', name), target)
                with self.assertRaises(ValueError):
                    self.driver(FakeRun([]), cwd=fresh.name)

    def test_create_mints_from_stdout_line_one_then_confirms_via_the_listing(self):
        run = FakeRun([(['claude', '--bg'], FakeResult(0, 'backgrounded · 69aa52ed\n  claude attach 69aa52ed\n')),
                       (['claude', 'agents'], listing([claude_entry()]))])
        driver = self.driver(run)
        self.assertEqual(driver.create('hello'), '69aa52ed')
        self.assertEqual(driver.owned(), {'69aa52ed'})
        self.assertEqual(run.argv('claude', '--bg')[0], ['claude', '--bg', '--model', 'haiku', 'hello'])
        self.assertEqual(run.argv('claude', 'agents')[0],
                          ['claude', 'agents', '--json', '--all', '--cwd', driver.cwd])
        self.assertEqual(driver.sessions['69aa52ed'], SESSION_UUID)

    def test_create_waits_for_the_creation_turn_to_leave_working(self):
        # `claude --bg` returns while the creation turn runs (captured `state: "working"`, reply
        # 12 s later). Returning then would put that reply inside the trial's own window.
        states = ['working', 'working', 'done']
        run = FakeRun([(['claude', '--bg'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', 'agents'],
                        lambda argv: listing([claude_entry(state=states.pop(0) if len(states) > 1
                                                           else states[0])]))])
        driver = self.driver(run)
        self.assertEqual(driver.create('hello'), '69aa52ed')
        self.assertEqual(states, ['done'])
        self.assertEqual(self.clock.monotonic(), 2.0)  # one 1s sleep per still-working listing

    def test_create_fails_when_the_creation_turn_never_finishes(self):
        run = FakeRun([(['claude', '--bg'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', 'agents'], listing([claude_entry(state='working')]))])
        driver = self.driver(run)
        with self.assertRaises(RuntimeError) as caught:
            driver.create('hello')
        self.assertIn("still 'working'", str(caught.exception))
        self.assertEqual(driver.owned(), {'69aa52ed'})  # the sweep still has to remove it

    def test_create_keeps_the_id_owned_when_the_listing_lacks_it(self):
        # The daemon printed its id: it exists. A listing that disagrees is an error the sweep
        # still has to act on.
        run = FakeRun([(['claude', '--bg'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', 'agents'], listing([]))])
        driver = self.driver(run)
        with self.assertRaises(RuntimeError):
            driver.create('hello')
        self.assertEqual(driver.owned(), {'69aa52ed'})

    def test_create_mints_from_partial_output_on_a_timeout(self):
        error = subprocess.TimeoutExpired(cmd=['claude'], timeout=60, output=b'backgrounded \xc2\xb7 69aa52ed\n')
        run = FakeRun([(['claude', '--bg'], error)])
        driver = self.driver(run)
        with self.assertRaises(subprocess.TimeoutExpired):
            driver.create('hello')
        self.assertEqual(driver.owned(), {'69aa52ed'})

    def test_create_with_no_backgrounded_line_or_nonzero_exit_owns_nothing(self):
        for result in (FakeResult(1, '', 'boom'), FakeResult(0, 'something else\n')):
            with self.subTest(result=result):
                driver = self.driver(FakeRun([(['claude', '--bg'], result)]))
                with self.assertRaises(RuntimeError):
                    driver.create('hello')
                self.assertEqual(driver.owned(), set())

    def test_create_mints_a_backgrounded_id_even_when_the_exit_status_is_nonzero(self):
        # The line names a session the daemon started; a later failure in the same command
        # (or a wrapper's exit status) does not unstart it.
        driver = self.driver(FakeRun([(['claude', '--bg'], FakeResult(1, 'backgrounded · 69aa52ed\n', 'boom'))]))
        with self.assertRaises(RuntimeError):
            driver.create('hello')
        self.assertEqual(driver.owned(), {'69aa52ed'})

    def test_status_and_teardown_refuse_a_foreign_id(self):
        driver = self.driver(FakeRun([(['claude', 'agents'], listing([claude_entry('deadbeef')]))]))
        for method in (driver.status, driver.stop, driver.teardown, driver.version):
            with self.assertRaises(ForeignSessionError):
                method('deadbeef')
        with self.assertRaises(ForeignSessionError):
            driver.submit('deadbeef', 'x')
        with self.assertRaises(ForeignSessionError):
            driver.observe('deadbeef', marker=MARKER, submitted_at=0.0)

    def test_observe_reads_the_owned_transcript_and_marks_unusable_reads_unobservable(self):
        run = FakeRun([(['claude', '--bg'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', 'agents'], listing([claude_entry()]))])
        driver = self.driver(run)
        driver.create('hello')
        self.transcripts[SESSION_UUID] = self.write_lines('t.jsonl', [
            claude_record('user', marker_message(MARKER), stamp='2026-09-13T09:00:01.000Z', cwd=driver.cwd),
            claude_record('assistant', [{'type': 'text', 'text': MARKER}],
                          stamp='2026-09-13T09:00:03.000Z', cwd=driver.cwd),
        ])
        submitted_at = 1_789_290_000.0  # 2026-09-13T09:00:00Z
        observation = driver.observe('69aa52ed', marker=MARKER, submitted_at=submitted_at)
        self.assertEqual(set(observation.outcomes), {'visible', 'turn_start', 'ack'})
        self.assertTrue(observation.observable)
        self.assertFalse(observation.turn_stream)
        self.transcripts[SESSION_UUID] = self.write_lines('u.jsonl', ['garbage'])
        self.assertFalse(driver.observe('69aa52ed', marker=MARKER, submitted_at=submitted_at).observable)
        self.transcripts.pop(SESSION_UUID)
        self.assertFalse(driver.observe('69aa52ed', marker=MARKER, submitted_at=submitted_at).observable)

    def test_version_comes_from_the_transcript_never_the_binary(self):
        run = FakeRun([(['claude', '--bg'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', 'agents'], listing([claude_entry()]))])
        driver = self.driver(run)
        driver.create('hello')
        self.transcripts[SESSION_UUID] = self.write_lines('v.jsonl', [claude_record('user', 'x', cwd=driver.cwd)])
        self.assertEqual(driver.version('69aa52ed'), '2.1.270')
        self.assertEqual(run.argv('claude', '--version'), [])
        self.transcripts.pop(SESSION_UUID)
        self.assertIsNone(driver.version('69aa52ed'))
        self.assertGreaterEqual(self.clock.elapsed, 5.0)  # waited out its bound on the fake clock

    def test_transcript_record_binding_gates_all_evidence(self):
        driver, _ = self.create_live()
        original = [claude_record('user', MARKER, cwd=driver.cwd),
                    claude_record('assistant', [{'type': 'text', 'text': MARKER}],
                                  cwd=driver.cwd, model='synthetic-model')]
        for index in (0, 1):
            for key in ('sessionId', 'cwd'):
                for value in (None, '', 'foreign'):
                    with self.subTest(index=index, key=key, value=value):
                        record = json.loads(original[index])
                        if value is None:
                            del record[key]
                        else:
                            record[key] = value
                        lines = list(original)
                        lines[index] = json.dumps(record)
                        self.transcripts[SESSION_UUID] = self.write_lines('foreign-transcript.jsonl', lines)
                        observation = driver.observe('69aa52ed', marker=MARKER, submitted_at=0)
                        self.assertFalse(observation.observable)
                        self.assertEqual(observation.outcomes, {})
                        self.assertIsNone(observation.model)
                        self.assertIsNone(driver.version('69aa52ed'))

    def create_live(self, extra_scripts=(), **kwargs):
        run = FakeRun([(['claude', '--bg', '--model'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', 'agents'], listing([claude_entry()])), *extra_scripts])
        driver = self.driver(run, **kwargs)
        driver.create('hello')
        return driver, run

    def test_attach_submit_types_the_message_and_holds_the_client_attached(self):
        # The capture only ever detached after the reply was displayed, so the client must stay
        # attached across the whole observation instead of leaving mid-turn.
        driver, run = self.create_live()
        self.assertIsNone(driver.submit('69aa52ed', marker_message(MARKER)))
        client, = FakePtyClient.launched
        self.assertEqual(client.argv, ['claude', 'attach', '69aa52ed'])
        self.assertEqual(client.typed, [marker_message(MARKER)])
        self.assertNotIn(b'\x1a', client.keys)  # no detach yet
        self.assertFalse(client.closed)
        self.assertEqual(driver.clients, [client])
        self.assertIn('client held attached until cleanup', driver.submission_note)

    def test_close_clients_detaches_the_held_attach_client_with_ctrl_z(self):
        driver, run = self.create_live()
        driver.submit('69aa52ed', marker_message(MARKER))
        client, = FakePtyClient.launched
        self.assertEqual(driver.close_clients(), [])
        self.assertEqual(client.keys[-1], b'\x1a')  # captured detach: exit 0, session keeps running
        self.assertTrue(client.closed)
        self.assertEqual(driver.clients, [])
        self.assertGreaterEqual(self.clock.elapsed, 1.0)  # the detach was given the captured moment

    def test_busy_submit_reuses_the_client_that_established_the_state(self):
        driver, run = self.create_live()
        client = driver.attach('69aa52ed')
        client.type_line('synthetic prior turn')
        run.scripts.insert(0, (['claude', 'agents'],
                               listing([claude_entry(status='busy', state='working')])))
        with unittest.mock.patch.object(client, 'wait_for', side_effect=AssertionError('idle wait')):
            self.assertIsNone(driver.submit('69aa52ed', marker_message(MARKER)))
        self.assertEqual(FakePtyClient.launched, [client])
        self.assertEqual(client.typed, ['synthetic prior turn', marker_message(MARKER)])

    def test_an_exited_or_unready_attach_is_not_reused(self):
        driver, run = self.create_live()
        client = driver.attach('69aa52ed')
        client.eof = True
        self.assertIsNone(driver.live_client_for('69aa52ed'))
        driver.pty = lambda argv, **kw: FakePtyClient(argv, ready=False, **kw)
        with self.assertRaises(SubmissionUncaptured):
            driver.attach('69aa52ed')
        self.assertIsNone(driver.live_client_for('69aa52ed'))

    def test_a_client_for_another_owned_session_is_not_reused(self):
        driver, run = self.create_live()
        driver.attach('69aa52ed')
        driver.mint('12345678')
        self.assertIsNone(driver.live_client_for('12345678'))

    def test_busy_submit_without_an_established_client_refuses_mid_turn_attach(self):
        driver, run = self.create_live()
        run.scripts.insert(0, (['claude', 'agents'],
                               listing([claude_entry(status='busy', state='working')])))
        with self.assertRaises(SubmissionUncaptured):
            driver.submit('69aa52ed', marker_message(MARKER))
        self.assertEqual(FakePtyClient.launched, [])

    def test_close_clients_skips_the_detach_for_a_client_that_already_exited(self):
        driver, run = self.create_live()
        driver.submit('69aa52ed', marker_message(MARKER))
        client, = FakePtyClient.launched
        client.eof = True  # the attach client died on its own; close() below still reaps it
        self.assertEqual(driver.close_clients(), [])
        self.assertNotIn(b'\x1a', client.keys)
        self.assertTrue(client.closed)
        self.assertEqual(driver.clients, [])

    def test_a_failed_detach_write_is_reported_and_does_not_escape_the_sweep(self):
        # `send_keys` propagates OSError from os.write. Letting it out of close_clients() would
        # abort sweep() before anything was closed or reported.
        driver, run = self.create_live()
        driver.submit('69aa52ed', marker_message(MARKER))
        client, = FakePtyClient.launched
        client.write_error = OSError('input/output error')
        failures = sweep(driver)
        self.assertEqual([kind for kind, _ in failures], ['client'])
        self.assertIsInstance(failures[0][1], OSError)
        self.assertTrue(client.closed)  # the base close still ran
        self.assertEqual(driver.owned(), {'69aa52ed'})  # and no teardown followed a failed detach
        self.assertEqual(run.argv('claude', 'rm'), [])

    def test_attach_client_exiting_while_typing_is_uncaptured(self):
        driver, run = self.create_live()

        class ExitingClient(FakePtyClient):
            def type_line(self, text, **kwargs):
                self.eof = True
                self.send_keys(text.encode())

        driver.pty = ExitingClient
        with self.assertRaises(SubmissionUncaptured):
            driver.submit('69aa52ed', marker_message(MARKER))
        # The client stays held: the sweep owns every close now, and a handle dropped here would
        # be the only one to a child that may still be alive.
        self.assertEqual(driver.clients, FakePtyClient.launched)

    def test_a_failed_pty_write_while_typing_is_uncaptured_too(self):
        # A child that dies with the drain thread not yet at EOF makes `send_keys` raise OSError
        # straight from `os.write` rather than the eof guard's ValueError. Only ValueError was
        # classified, so this escaped `submit()` unclassified -- and `run_trial` records an
        # unclassified failure as the trial failing, not as a submission that may have half landed.
        driver, run = self.create_live()

        class WriteFailingClient(FakePtyClient):
            def __init__(self, argv, **kwargs):
                super().__init__(argv, **kwargs)
                self.write_error = OSError(5, 'Input/output error')

        driver.pty = WriteFailingClient
        with self.assertRaises(SubmissionUncaptured) as caught:
            driver.submit('69aa52ed', marker_message(MARKER))
        self.assertIn('Input/output error', str(caught.exception))
        self.assertEqual(driver.clients, FakePtyClient.launched)

    def test_a_listing_timeout_before_submission_is_uncaptured_and_sends_nothing(self):
        driver, run = self.create_live()
        run.scripts.insert(0, (['claude', 'agents'], subprocess.TimeoutExpired(cmd=['claude'], timeout=15)))
        with self.assertRaises(SubmissionUncaptured):
            driver.submit('69aa52ed', marker_message(MARKER))
        self.assertEqual(FakePtyClient.launched, [])

    def test_attach_submit_with_no_composer_is_uncaptured_never_rejected(self):
        driver, run = self.create_live()
        driver.pty = lambda argv, *, cwd: FakePtyClient(argv, cwd=cwd, ready=False)
        with self.assertRaises(SubmissionUncaptured):
            driver.submit('69aa52ed', marker_message(MARKER))
        self.assertEqual(driver.clients, FakePtyClient.launched)  # the sweep closes it
        self.assertEqual(driver.close_clients(), [])
        self.assertTrue(FakePtyClient.launched[0].closed)

    def test_attach_to_a_stopped_session_is_uncaptured(self):
        run = FakeRun([(['claude', '--bg', '--model'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', 'agents'], listing([claude_entry(pid=None, status=None)]))])
        driver = self.driver(run)
        driver.create('hello')
        with self.assertRaises(SubmissionUncaptured):
            driver.submit('69aa52ed', 'x')
        self.assertEqual(FakePtyClient.launched, [])

    def test_resume_submit_continues_a_stopped_session_with_no_other_flags(self):
        run = FakeRun([(['claude', '--bg', '--model'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', '--bg', '--resume'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', 'agents'], listing([claude_entry(pid=None, status=None)]))])
        driver = self.driver(run, mechanism='resume')
        driver.create('hello')
        self.assertTrue(driver.submit('69aa52ed', 'msg'))
        self.assertEqual(run.argv('claude', '--bg', '--resume')[0],
                          ['claude', '--bg', '--resume', SESSION_UUID, 'msg'])

    def test_resume_submit_against_a_running_session_is_uncaptured(self):
        driver, run = self.create_live(mechanism='resume')
        with self.assertRaises(SubmissionUncaptured):
            driver.submit('69aa52ed', 'msg')
        self.assertEqual(run.argv('claude', '--bg', '--resume'), [])

    def test_resume_submit_that_starts_a_copy_mints_it_and_reports_a_rejection(self):
        run = FakeRun([(['claude', '--bg', '--model'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', '--bg', '--resume'], FakeResult(0, 'backgrounded · 0badc0de\n')),
                       (['claude', 'agents'], listing([claude_entry(pid=None, status=None)]))])
        driver = self.driver(run, mechanism='resume')
        driver.create('hello')
        with self.assertRaises(SubmissionRejected):
            driver.submit('69aa52ed', 'msg')
        self.assertEqual(driver.owned(), {'69aa52ed', '0badc0de'})

    def test_resume_submit_nonzero_exit_is_a_rejection_with_its_stderr(self):
        run = FakeRun([(['claude', '--bg', '--model'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', '--bg', '--resume'], FakeResult(1, '', 'No conversation found')),
                       (['claude', 'agents'], listing([claude_entry(pid=None, status=None)]))])
        driver = self.driver(run, mechanism='resume')
        driver.create('hello')
        with self.assertRaises(SubmissionRejected) as caught:
            driver.submit('69aa52ed', 'msg')
        self.assertEqual(caught.exception.returncode, 1)
        self.assertIn('No conversation found', caught.exception.stderr)

    def test_resume_nonzero_after_backgrounding_the_original_remains_pollable(self):
        run = FakeRun([(['claude', '--bg', '--model'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', '--bg', '--resume'],
                        FakeResult(1, 'backgrounded · 69aa52ed\n', 'then failed')),
                       (['claude', 'agents'], listing([claude_entry(pid=None, status=None)]))])
        driver = self.driver(run, mechanism='resume')
        driver.create('hello')
        self.assertIsNone(driver.submit('69aa52ed', 'msg'))
        self.assertIn('acceptance is ambiguous', driver.submission_note)
        self.assertEqual(driver.owned(), {'69aa52ed'})

    def test_resume_submit_nonzero_exit_still_mints_a_copy_named_on_stdout(self):
        run = FakeRun([(['claude', '--bg', '--model'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', '--bg', '--resume'], FakeResult(1, 'backgrounded · 0badc0de\n', 'then failed')),
                       (['claude', 'agents'], listing([claude_entry(pid=None, status=None)]))])
        driver = self.driver(run, mechanism='resume')
        driver.create('hello')
        with self.assertRaises(SubmissionRejected):
            driver.submit('69aa52ed', 'msg')
        self.assertEqual(driver.owned(), {'69aa52ed', '0badc0de'})

    def test_resume_submit_timeout_that_named_a_copy_is_uncaptured_not_a_timeout(self):
        # The copy proves where the message went. Re-raising the timeout would poll the original
        # and turn its absent marker into `not_observed`.
        error = subprocess.TimeoutExpired(cmd=['claude'], timeout=60, output=b'backgrounded \xc2\xb7 0badc0de\n')
        run = FakeRun([(['claude', '--bg', '--model'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', '--bg', '--resume'], error),
                       (['claude', 'agents'], listing([claude_entry(pid=None, status=None)]))])
        driver = self.driver(run, mechanism='resume')
        driver.create('hello')
        with self.assertRaises(SubmissionUncaptured) as caught:
            driver.submit('69aa52ed', 'msg')
        self.assertIn('0badc0de', str(caught.exception))
        self.assertEqual(driver.owned(), {'69aa52ed', '0badc0de'})

    def test_resume_submit_timeout_with_no_copy_named_stays_a_timeout(self):
        # Nothing says where the message went, so the trial polls its own session.
        error = subprocess.TimeoutExpired(cmd=['claude'], timeout=60, output=b'')
        run = FakeRun([(['claude', '--bg', '--model'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', '--bg', '--resume'], error),
                       (['claude', 'agents'], listing([claude_entry(pid=None, status=None)]))])
        driver = self.driver(run, mechanism='resume')
        driver.create('hello')
        with self.assertRaises(subprocess.TimeoutExpired):
            driver.submit('69aa52ed', 'msg')
        self.assertEqual(driver.owned(), {'69aa52ed'})

    def test_teardown_stops_confirms_the_pid_is_gone_then_removes(self):
        listings = iter([listing([claude_entry()]), listing([claude_entry(pid=None, status=None)])])
        run = FakeRun([(['claude', '--bg', '--model'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', 'agents'], lambda argv: next(listings)),
                       (['claude', 'stop'], FakeResult(0, 'stopped 69aa52ed\n')),
                       (['claude', 'rm'], FakeResult(0, 'removed 69aa52ed\n'))])
        driver = self.driver(run)
        driver.create('hello')
        driver.teardown('69aa52ed')
        self.assertEqual(driver.owned(), set())
        self.assertEqual([call[:2] for call, _ in run.calls[2:]],
                          [['claude', 'stop'], ['claude', 'agents'], ['claude', 'rm']])

    def test_stop_confirms_through_the_listing_and_keeps_the_session_owned(self):
        listings = iter([listing([claude_entry()]), listing([claude_entry(pid=None, status=None)])])
        run = FakeRun([(['claude', '--bg', '--model'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', 'agents'], lambda argv: next(listings)),
                       (['claude', 'stop'], FakeResult(0, 'stopped 69aa52ed\n'))])
        driver = self.driver(run)
        driver.create('hello')
        entry = driver.stop('69aa52ed')
        self.assertIsNone(entry['pid'])
        self.assertEqual(run.argv('claude', 'stop'), [['claude', 'stop', '69aa52ed']])
        self.assertEqual(run.argv('claude', 'rm'), [])
        self.assertEqual(driver.owned(), {'69aa52ed'})  # the restarted cell resumes it next

    def test_stop_is_an_error_when_the_pid_survives_whatever_the_exit_status(self):
        run = FakeRun([(['claude', '--bg', '--model'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', 'agents'], listing([claude_entry()])),
                       (['claude', 'stop'], FakeResult(0, 'stopped 69aa52ed\n'))])
        driver = self.driver(run)
        driver.create('hello')
        with self.assertRaises(RuntimeError):
            driver.stop('69aa52ed')
        self.assertEqual(driver.owned(), {'69aa52ed'})

    def test_the_restarted_cell_is_stop_then_a_flagless_resume(self):
        listings = iter([listing([claude_entry()]), listing([claude_entry(pid=None, status=None)])])
        run = FakeRun([(['claude', '--bg', '--model'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', '--bg', '--resume'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', 'agents'], lambda argv: next(listings, listing([claude_entry(pid=None, status=None)]))),
                       (['claude', 'stop'], FakeResult(0, 'stopped 69aa52ed\n'))])
        driver = self.driver(run, mechanism='resume')
        driver.create('hello')
        driver.stop('69aa52ed')  # the settle callback
        self.assertTrue(driver.submit('69aa52ed', 'msg'))
        self.assertEqual([call[:3] for call, _ in run.calls],
                          [['claude', '--bg', '--model'], ['claude', 'agents', '--json'],
                           ['claude', 'stop', '69aa52ed'], ['claude', 'agents', '--json'],
                           ['claude', 'agents', '--json'], ['claude', '--bg', '--resume']])

    def test_teardown_tolerates_a_nonzero_stop_against_an_already_stopped_session(self):
        # The restarted cell's session is stopped before the sweep reaches it; the listing, not
        # `stop`'s exit status, is what gates `rm`.
        run = FakeRun([(['claude', '--bg', '--model'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', 'agents'], listing([claude_entry(pid=None, status=None)])),
                       (['claude', 'stop'], FakeResult(1, '', 'not running')),
                       (['claude', 'rm'], FakeResult(0, 'removed 69aa52ed\n'))])
        driver = self.driver(run)
        driver.create('hello')
        driver.teardown('69aa52ed')
        self.assertEqual(run.argv('claude', 'rm'), [['claude', 'rm', '69aa52ed']])
        self.assertEqual(driver.owned(), set())

    def test_teardown_never_runs_rm_while_the_daemon_still_has_a_pid(self):
        run = FakeRun([(['claude', '--bg', '--model'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', 'agents'], listing([claude_entry()])),
                       (['claude', 'stop'], FakeResult(1, '', 'not stopped')),
                       (['claude', 'rm'], FakeResult(0))])
        driver = self.driver(run)
        driver.create('hello')
        with self.assertRaises(RuntimeError):
            driver.teardown('69aa52ed')
        self.assertEqual(run.argv('claude', 'rm'), [])
        self.assertEqual(driver.owned(), {'69aa52ed'})  # retained for a human to find

    def test_malformed_claude_listings_never_authorize_rm_after_a_failed_stop(self):
        missing_pid = claude_entry(pid=None, status=None)
        missing_pid.pop('pid')
        missing_id = claude_entry(pid=None, status=None)
        missing_id.pop('id')
        for entries in ([missing_pid], [None], [missing_id],
                        [claude_entry(pid=False)], [claude_entry(pid='unknown')],
                        [claude_entry(pid=None), claude_entry(pid=None)]):
            with self.subTest(entries=entries):
                self.registry = SessionRegistry()
                run = FakeRun([(['claude', 'agents'], listing(entries)),
                               (['claude', 'stop'], FakeResult(1, '', 'not stopped')),
                               (['claude', 'rm'], FakeResult(0))])
                driver = self.driver(run)
                driver._mint('69aa52ed')
                with self.assertRaisesRegex(RuntimeError, 'malformed'):
                    driver.teardown('69aa52ed')
                self.assertEqual(run.argv('claude', 'rm'), [])
                self.assertEqual(driver.owned(), {'69aa52ed'})

    def test_teardown_retains_ownership_when_rm_fails(self):
        run = FakeRun([(['claude', '--bg', '--model'], FakeResult(0, 'backgrounded · 69aa52ed\n')),
                       (['claude', 'agents'], listing([])),
                       (['claude', 'stop'], FakeResult(0)),
                       (['claude', 'rm'], FakeResult(1, '', 'busy'))])
        driver = self.driver(run)
        driver.mint('69aa52ed')
        with self.assertRaises(RuntimeError):
            driver.teardown('69aa52ed')
        self.assertEqual(driver.owned(), {'69aa52ed'})

    def test_an_unknown_mechanism_is_rejected(self):
        with self.assertRaises(ValueError):
            self.driver(FakeRun([]), mechanism='channels')


def rollout_lines(*, cli_version='0.154.0', cwd='/synthetic', messages=()):
    lines = [json.dumps({'timestamp': '2026-09-13T09:41:51.650Z', 'type': 'session_meta',
                         'payload': {'id': THREAD_ID, 'session_id': THREAD_ID, 'cwd': cwd,
                                     'cli_version': cli_version}})]
    for stamp, role, text in messages:
        part_type = 'input_text' if role == 'user' else 'output_text'
        lines.append(json.dumps({'timestamp': stamp, 'type': 'response_item',
                                 'payload': {'type': 'message', 'role': role,
                                             'content': [{'type': part_type, 'text': text}]}}))
    return lines


class CodexDriverTests(DriverTestCase):
    def test_codex_creation_never_mints_a_non_uuid_thread_id(self):
        for thread_id in ('*', '../*', '--help', 'not-a-uuid'):
            with self.subTest(thread_id=thread_id):
                self.registry = SessionRegistry()
                output = json.dumps({'type': 'thread.started', 'thread_id': thread_id})
                driver = self.driver(FakeRun([(['codex', 'exec'], FakeResult(0, output))]))
                with self.assertRaisesRegex(RuntimeError, 'UUID'):
                    driver.create('hello')
                self.assertEqual(driver.owned(), set())

    def driver(self, run, **kwargs):
        kwargs.setdefault('rollout_path_for', self.transcript_path_for)
        kwargs.setdefault('pty', FakePtyClient)
        return CodexDriver(self.registry, run=run, cwd=self.cwd, **kwargs)

    def exec_output(self, thread_id=THREAD_ID):
        return FakeResult(0, '\n'.join([json.dumps({'type': 'thread.started', 'thread_id': thread_id}),
                                        json.dumps({'type': 'turn.completed'})]) + '\n')

    def test_create_runs_exec_json_read_only_and_mints_the_thread(self):
        run = FakeRun([(['codex', 'exec'], self.exec_output())])
        driver = self.driver(run)
        self.assertEqual(driver.create('hello'), THREAD_ID)
        self.assertEqual(driver.owned(), {THREAD_ID})
        argv, kwargs = run.calls[0]
        self.assertEqual(argv, ['codex', 'exec', '--json', '-s', 'read-only', '--skip-git-repo-check',
                                '-C', driver.cwd, 'hello'])
        self.assertIs(kwargs['stdin'], subprocess.DEVNULL)

    def test_uncaptured_codex_model_override_is_refused_before_any_host_call(self):
        for model in ('synthetic-model', ''):
            with self.subTest(model=model):
                run = FakeRun([])
                with self.assertRaisesRegex(ValueError, 'uncaptured'):
                    self.driver(run, model=model)
                self.assertEqual(run.calls, [])

    def test_ambiguous_codex_creation_reports_all_candidates_without_ownership(self):
        other = '01a09a24-ff1d-7360-9385-722d230ef92c'
        output = self.exec_output().stdout + '\n' + json.dumps(
            {'type': 'thread.started', 'thread_id': other})
        for timeout in (False, True):
            with self.subTest(timeout=timeout):
                self.registry = SessionRegistry()
                result = (subprocess.TimeoutExpired(['codex'], 300, output=output)
                          if timeout else FakeResult(0, output))
                driver = self.driver(FakeRun([(['codex', 'exec'], result)]))
                with self.assertRaisesRegex(RuntimeError, 'ambiguous'):
                    driver.create('hello')
                self.assertEqual(driver.owned(), set())
                self.assertEqual({label for label, _ in sweep(driver)}, {THREAD_ID, other})
                for candidate in (THREAD_ID, other):
                    with self.assertRaises(ForeignSessionError):
                        driver.teardown(candidate)

    def test_malformed_codex_creation_cannot_hide_behind_a_valid_thread_event(self):
        for suffix in ('{"type":', '[1]', 'null',
                       json.dumps({'type': 'thread.started', 'thread_id': 5})):
            with self.subTest(suffix=suffix):
                self.registry = SessionRegistry()
                run = FakeRun([(['codex', 'exec'],
                                FakeResult(0, self.exec_output().stdout + '\n' + suffix))])
                driver = self.driver(run)
                with self.assertRaisesRegex(RuntimeError, 'malformed'):
                    driver.create('hello')
                self.assertEqual(driver.owned(), {THREAD_ID})

    def test_create_mints_before_checking_the_exit_status_and_from_partial_output(self):
        run = FakeRun([(['codex', 'exec'], FakeResult(2, self.exec_output().stdout, 'quota'))])
        driver = self.driver(run)
        with self.assertRaises(RuntimeError):
            driver.create('hello')
        self.assertEqual(driver.owned(), {THREAD_ID})
        partial = self.exec_output().stdout.encode()
        run = FakeRun([(['codex', 'exec'], subprocess.TimeoutExpired(cmd=['codex'], timeout=300, output=partial))])
        self.registry = SessionRegistry()  # a fresh run; the first driver's thread stays its own
        driver = self.driver(run)
        with self.assertRaises(subprocess.TimeoutExpired):
            driver.create('hello')
        self.assertEqual(driver.owned(), {THREAD_ID})

    def test_create_without_a_thread_started_event_owns_nothing(self):
        driver = self.driver(FakeRun([(['codex', 'exec'], FakeResult(0, json.dumps({'type': 'turn.completed'})))]))
        with self.assertRaises(RuntimeError):
            driver.create('hello')
        self.assertEqual(driver.owned(), set())

    def test_queue_submit_is_accepted_on_exit_zero_and_rejected_with_stderr_otherwise(self):
        run = FakeRun([(['codex', 'exec'], self.exec_output()),
                       (['codex', 'queue'], FakeResult(0, f'Queued message x for thread {THREAD_ID}.\n'))])
        driver = self.driver(run)
        driver.create('hello')
        client = driver.attach(THREAD_ID)  # what a live-cell settle does before submission
        self.assertTrue(driver.submit(THREAD_ID, 'msg'))
        self.assertEqual(run.argv('codex', 'queue')[0],
                          ['codex', 'queue', '--thread', THREAD_ID, '--message', 'msg'])
        self.assertEqual(FakePtyClient.launched, [client])  # submit opened no client of its own
        run.scripts.insert(0, (['codex', 'queue'], FakeResult(1, '', 'Error: No active session found')))
        with self.assertRaises(SubmissionRejected) as caught:
            driver.submit(THREAD_ID, 'msg')
        self.assertIn('No active session', caught.exception.stderr)

    def test_queue_submit_with_no_resume_client_open_is_uncaptured_and_queues_nothing(self):
        # Captured: a queued item is delivered only by a process serving the thread. With none
        # open the trial would time out for certain, which is evidence about the runner, not
        # the host.
        run = FakeRun([(['codex', 'exec'], self.exec_output()),
                       (['codex', 'queue'], FakeResult(0))])
        driver = self.driver(run)
        driver.create('hello')
        with self.assertRaises(SubmissionUncaptured):
            driver.submit(THREAD_ID, 'msg')
        self.assertEqual(run.argv('codex', 'queue'), [])

    def test_queue_submit_requires_a_ready_client_for_the_exact_owned_thread(self):
        run = FakeRun([(['codex', 'exec'], self.exec_output()), (['codex', 'queue'], FakeResult(0))])
        driver = self.driver(run)
        driver.create('hello')
        driver.attach(THREAD_ID)
        other = '01a09a24-ff1d-7360-9385-722d230ef92c'
        driver.mint(other)
        with self.assertRaises(SubmissionUncaptured):
            driver.submit(other, 'msg')
        self.assertEqual(run.argv('codex', 'queue'), [])

    def test_a_held_unready_codex_client_cannot_authorize_queue_submission(self):
        run = FakeRun([(['codex', 'exec'], self.exec_output()), (['codex', 'queue'], FakeResult(0))])
        driver = self.driver(run)
        driver.create('hello')
        driver.open_client(['codex', 'resume', THREAD_ID])
        with self.assertRaises(SubmissionUncaptured):
            driver.submit(THREAD_ID, 'msg')
        self.assertEqual(run.argv('codex', 'queue'), [])

    def test_queue_submit_with_only_an_exited_resume_client_is_uncaptured(self):
        # A resume client that exited serves nothing. Counting the retained handle as a serving
        # process would queue an item nobody delivers and blame the host for the silence.
        run = FakeRun([(['codex', 'exec'], self.exec_output()),
                       (['codex', 'resume'], None),
                       (['codex', 'queue'], FakeResult(0))])
        driver = self.driver(run)
        driver.create('hello')
        driver.attach(THREAD_ID)
        client, = FakePtyClient.launched
        client.eof = True
        with self.assertRaises(SubmissionUncaptured):
            driver.submit(THREAD_ID, 'msg')
        self.assertEqual(run.argv('codex', 'queue'), [])

    def test_queue_then_resume_treats_a_queue_timeout_as_uncaptured(self):
        # Nothing serves the thread yet, so a `codex queue` whose exit status is unknown cannot
        # be polled for: it is neither accepted nor a host rejection.
        run = FakeRun([(['codex', 'exec'], self.exec_output()),
                       (['codex', 'queue'], subprocess.TimeoutExpired(cmd=['codex', 'queue'], timeout=15))])
        driver = self.driver(run, mechanism='queue-then-resume')
        driver.create('hello')
        with self.assertRaises(SubmissionUncaptured):
            driver.submit(THREAD_ID, 'msg')
        self.assertEqual(FakePtyClient.launched, [])
        # Under `queue` a live client may still have drained it: the timeout propagates raw and
        # `run_trial` polls with `accepted` unobservable. Same instance, since only the instance
        # that created a thread may operate on it.
        driver.mechanism = 'queue'
        driver.attach(THREAD_ID)
        with self.assertRaises(subprocess.TimeoutExpired):
            driver.submit(THREAD_ID, 'msg')

    def test_losing_the_exact_queue_client_preserves_acceptance_but_not_negative_evidence(self):
        for loss in ('exit', 'remove', 'replace'):
            for timeout in (False, True):
                with self.subTest(loss=loss, timeout=timeout):
                    self.registry = SessionRegistry()

                    def queue(argv):
                        if loss == 'exit':
                            client.eof = True
                        else:
                            driver.clients.remove(client)
                            if loss == 'replace':
                                driver.attach(THREAD_ID)
                        if timeout:
                            raise subprocess.TimeoutExpired(argv, 15)
                        return FakeResult(0)

                    run = FakeRun([(['codex', 'exec'], self.exec_output()),
                                   (['codex', 'queue'], queue)])
                    driver = self.driver(run)
                    driver.create('hello')
                    client = driver.attach(THREAD_ID)
                    self.transcripts[THREAD_ID] = self.write_lines('lost-client.jsonl', rollout_lines(
                        cwd=driver.cwd,
                        messages=[('2026-09-13T09:42:01.000Z', 'user', MARKER)]))
                    if timeout:
                        with self.assertRaises(subprocess.TimeoutExpired):
                            driver.submit(THREAD_ID, 'msg')
                    else:
                        self.assertIs(driver.submit(THREAD_ID, 'msg'), True)
                    observation = driver.observe(THREAD_ID, marker=MARKER, submitted_at=0)
                    self.assertIn('visible', observation.outcomes)
                    self.assertFalse(observation.observable)

    def test_queue_client_loss_during_observation_invalidates_missing_outcomes(self):
        run = FakeRun([(['codex', 'exec'], self.exec_output()), (['codex', 'queue'], FakeResult(0))])
        driver = self.driver(run)
        driver.create('hello')
        client = driver.attach(THREAD_ID)
        self.transcripts[THREAD_ID] = self.write_lines('later-client-loss.jsonl', rollout_lines(cwd=driver.cwd))
        self.assertIs(driver.submit(THREAD_ID, 'msg'), True)
        self.assertTrue(driver.observe(THREAD_ID, marker=MARKER, submitted_at=0).observable)
        client.eof = True
        self.assertFalse(driver.observe(THREAD_ID, marker=MARKER, submitted_at=0).observable)

    def test_submit_and_observe_refuse_a_foreign_thread(self):
        driver = self.driver(FakeRun([]))
        for call in (lambda: driver.submit('other', 'x'),
                     lambda: driver.observe('other', marker=MARKER, submitted_at=0.0),
                     lambda: driver.teardown('other'), lambda: driver.version('other'),
                     lambda: driver.attach('other')):
            with self.assertRaises(ForeignSessionError):
                call()

    def test_queue_then_resume_opens_the_resume_client_after_queueing_and_keeps_it(self):
        def queue(argv):
            self.assertEqual(FakePtyClient.launched, [])  # queued first: a resume drains at start
            return FakeResult(0)

        run = FakeRun([(['codex', 'exec'], self.exec_output()),
                       (['codex', 'queue'], queue)])
        readings = iter([1234.5, 1250.0])  # queue exit, then whatever comes after the client
        driver = self.driver(run, mechanism='queue-then-resume', clock=lambda: next(readings))
        driver.create('hello')
        # Acceptance is the `codex queue` exit, stamped before the resume client's startup.
        self.assertEqual(driver.submit(THREAD_ID, 'msg'), 1234.5)
        client, = FakePtyClient.launched
        self.assertEqual(client.argv, ['codex', '--no-alt-screen', '-s', 'read-only', '-a', 'never',
                                       '-C', driver.cwd, 'resume', THREAD_ID])
        self.assertEqual(len(run.argv('codex', 'queue')), 1)
        self.assertFalse(client.closed)
        self.assertEqual(driver.clients, [client])

    def test_queue_then_resume_with_no_composer_preserves_acceptance_and_closes_the_client(self):
        run = FakeRun([(['codex', 'exec'], self.exec_output()),
                       (['codex', 'queue'], FakeResult(0))])
        driver = self.driver(run, mechanism='queue-then-resume', clock=lambda: 1234.5,
                             pty=lambda argv, *, cwd: FakePtyClient(argv, cwd=cwd, ready=False))
        driver.create('hello')
        self.assertEqual(driver.submit(THREAD_ID, 'msg'), 1234.5)
        self.assertIn('never became ready', driver.submission_note)
        self.transcripts[THREAD_ID] = self.write_lines('not-ready.jsonl', rollout_lines(cwd=driver.cwd))
        self.assertFalse(driver.observe(THREAD_ID, marker=MARKER, submitted_at=0).observable)
        self.assertTrue(FakePtyClient.launched[0].closed)
        self.assertEqual(driver.clients, [])

    def test_attach_answers_the_first_run_trust_prompt_with_enter(self):
        run = FakeRun([(['codex', 'exec'], self.exec_output())])
        driver = self.driver(run, pty=lambda argv, *, cwd: FakePtyClient(argv, cwd=cwd, trust_prompt=True))
        driver.create('hello')
        client = driver.attach(THREAD_ID)
        self.assertEqual(client.keys, [b'\r'])
        self.assertEqual(driver.clients, [client])

    def test_attach_is_not_ready_when_the_composer_never_follows_the_answered_trust_dialog(self):
        # The placeholder drawn beneath the dialog must not count: only output after the Enter does.
        run = FakeRun([(['codex', 'exec'], self.exec_output())])
        driver = self.driver(run, pty=lambda argv, *, cwd: FakePtyClient(argv, cwd=cwd, trust_prompt=True,
                                                                          ready=False))
        driver.create('hello')
        with self.assertRaises(PtyNotReady) as caught:
            driver.attach(THREAD_ID)
        client, = FakePtyClient.launched
        self.assertEqual(client.keys, [b'\r'])
        self.assertTrue(client.closed)
        self.assertEqual(driver.clients, [])
        self.assertIn('Doyoutrustthecontentsofthisdirectory', str(caught.exception))

    def test_a_failed_pty_write_answering_the_trust_dialog_is_not_ready(self):
        # `send_keys` propagates OSError from `os.write` when the child died before the drain
        # thread saw EOF. Unclassified it escaped `attach()` raw, and through `submit()` under
        # mechanism=queue-then-resume, where only PtyNotReady becomes `SubmissionUncaptured`.
        def failing(argv, *, cwd):
            client = FakePtyClient(argv, cwd=cwd, trust_prompt=True)
            client.write_error = OSError(5, 'Input/output error')
            return client

        run = FakeRun([(['codex', 'exec'], self.exec_output())])
        driver = self.driver(run, pty=failing)
        driver.create('hello')
        with self.assertRaises(PtyNotReady) as caught:
            driver.attach(THREAD_ID)
        self.assertIn('Input/output error', str(caught.exception))
        client, = FakePtyClient.launched
        self.assertTrue(client.closed)
        self.assertEqual(driver.clients, [])

    def test_attach_accepts_other_sandbox_and_approval_flags(self):
        run = FakeRun([(['codex', 'exec'], self.exec_output())])
        driver = self.driver(run)
        driver.create('hello')
        client = driver.attach(THREAD_ID, sandbox='workspace-write', approval='untrusted')
        self.assertEqual(client.argv[2:6], ['-s', 'workspace-write', '-a', 'untrusted'])

    def test_observe_reads_the_rollout_with_the_turn_stream_and_fails_closed(self):
        run = FakeRun([(['codex', 'exec'], self.exec_output())])
        driver = self.driver(run)
        driver.create('hello')
        self.transcripts[THREAD_ID] = self.write_lines('r.jsonl', rollout_lines(cwd=driver.cwd, messages=[
            ('2026-09-13T09:42:01.000Z', 'user', f'injected host text {MARKER}'),
            ('2026-09-13T09:42:03.000Z', 'assistant', MARKER)]))
        observation = driver.observe(THREAD_ID, marker=MARKER, submitted_at=1_789_292_520.0)  # 09:42:00Z
        self.assertEqual(set(observation.outcomes), {'visible', 'ack'})  # turn_start needs task_started
        self.assertTrue(observation.turn_stream)
        self.assertTrue(observation.observable)
        self.transcripts.pop(THREAD_ID)
        self.assertFalse(driver.observe(THREAD_ID, marker=MARKER, submitted_at=0.0).observable)

    def test_rollout_removed_between_discovery_and_stat_is_unobservable(self):
        driver = self.driver(FakeRun([(['codex', 'exec'], self.exec_output())]),
                             rollout_path_for=host_trials.default_codex_rollout_path)
        driver.create('hello')
        with unittest.mock.patch.object(host_trials.glob, 'glob', return_value=['synthetic-rollout']), \
                unittest.mock.patch.object(os.path, 'getmtime', side_effect=FileNotFoundError):
            self.assertFalse(driver.observe(THREAD_ID, marker=MARKER, submitted_at=0).observable)
            self.assertIsNone(driver.version(THREAD_ID))

    def test_version_reads_the_rollouts_cli_version_never_the_binary(self):
        run = FakeRun([(['codex', 'exec'], self.exec_output())])
        driver = self.driver(run)
        driver.create('hello')
        self.transcripts[THREAD_ID] = self.write_lines('r.jsonl', rollout_lines(cwd=driver.cwd, cli_version='0.154.0'))
        self.assertEqual(driver.version(THREAD_ID), '0.154.0')
        self.assertEqual(run.argv('codex', '--version'), [])
        self.transcripts.pop(THREAD_ID)
        self.assertIsNone(driver.version(THREAD_ID))

    def test_rollout_identity_and_cwd_gate_outcomes_and_version(self):
        driver = self.driver(FakeRun([(['codex', 'exec'], self.exec_output())]))
        driver.create('hello')
        original = rollout_lines(cwd=driver.cwd, messages=[
            ('2026-09-13T09:42:01.000Z', 'user', MARKER),
            ('2026-09-13T09:42:03.000Z', 'assistant', MARKER)])
        cases = [('missing metadata', original[1:])]
        for key in ('id', 'session_id', 'cwd'):
            for value in (None, '', 'foreign'):
                metadata = json.loads(original[0])
                if value is None:
                    del metadata['payload'][key]
                else:
                    metadata['payload'][key] = value
                bad = json.dumps(metadata)
                cases.append((f'{key}={value}', [bad, *original[1:]]))
                cases.append((f'conflicting later {key}={value}', [*original, bad]))
        for name, lines in cases:
            with self.subTest(name=name):
                self.transcripts[THREAD_ID] = self.write_lines('wrong-rollout.jsonl', lines)
                observation = driver.observe(THREAD_ID, marker=MARKER, submitted_at=0)
                self.assertFalse(observation.observable)
                self.assertEqual(observation.outcomes, {})
                self.assertIsNone(driver.version(THREAD_ID))

    def test_teardown_deletes_with_force_and_releases_only_on_exit_zero(self):
        run = FakeRun([(['codex', 'exec'], self.exec_output()),
                       (['codex', 'delete'], FakeResult(1, '', 'nope'))])
        driver = self.driver(run)
        driver.create('hello')
        with self.assertRaises(RuntimeError):
            driver.teardown(THREAD_ID)
        self.assertEqual(driver.owned(), {THREAD_ID})
        run.scripts.insert(0, (['codex', 'delete'], FakeResult(0, f'Deleted session {THREAD_ID}.\n')))
        driver.teardown(THREAD_ID)
        self.assertEqual(run.argv('codex', 'delete')[-1], ['codex', 'delete', '--force', THREAD_ID])
        self.assertEqual(driver.owned(), set())

    def test_an_unknown_mechanism_is_rejected(self):
        with self.assertRaises(ValueError):
            self.driver(FakeRun([]), mechanism='stdin')


class CrossDriverNamespaceTests(DriverTestCase):
    def test_owned_is_per_namespace_over_one_shared_registry(self):
        claude = ClaudeDriver(self.registry, run=FakeRun([]), cwd=self.cwd, pty=FakePtyClient,
                              transcript_path_for=self.transcript_path_for)
        codex = CodexDriver(self.registry, run=FakeRun([]), cwd=self.cwd, pty=FakePtyClient,
                            rollout_path_for=self.transcript_path_for)
        claude.mint('abc')
        codex.mint('abc')  # the same name in another namespace is another session
        self.assertEqual(claude.owned(), {'abc'})
        self.assertEqual(codex.owned(), {'abc'})
        self.assertEqual(self.registry.created, {'claude:abc': claude, 'codex:abc': codex})
        codex.release('abc')
        self.assertEqual(claude.owned(), {'abc'})
        self.assertEqual(codex.owned(), set())
        with self.assertRaises(ForeignSessionError):
            codex.observe('abc', marker=MARKER, submitted_at=0.0)

    def test_owned_is_per_instance_not_per_registry(self):
        # Sweeping one instance must not tear down a sibling's sessions: only that sibling holds
        # the PTY client or server still serving them, so the sweep would delete a live trial.
        first = ClaudeDriver(self.registry, run=FakeRun([]), cwd=self.cwd, pty=FakePtyClient,
                             transcript_path_for=self.transcript_path_for)
        second = ClaudeDriver(self.registry, run=FakeRun([]), cwd=self.cwd, pty=FakePtyClient,
                              transcript_path_for=self.transcript_path_for)
        first.mint('aaaaaaaa')
        second.mint('bbbbbbbb')
        self.assertEqual(first.owned(), {'aaaaaaaa'})
        self.assertEqual(second.owned(), {'bbbbbbbb'})
        for method in (lambda: second.stop('aaaaaaaa'), lambda: second.teardown('aaaaaaaa'),
                       lambda: second.status('aaaaaaaa')):
            with self.assertRaises(ForeignSessionError):
                method()


class ServerOutputTests(unittest.TestCase):
    URL = 'http://127.0.0.1:4096'

    def reader(self):
        read_fd, write_fd = os.pipe()
        reader = host_trials.ServerOutput(os.fdopen(read_fd, 'rb'), self.URL)
        self.addCleanup(reader.close)
        writer = os.fdopen(write_fd, 'wb', buffering=0)
        self.addCleanup(writer.close)
        return reader, writer

    def test_split_readiness_and_large_output_are_drained_with_bounded_storage(self):
        reader, writer = self.reader()
        reader.start()
        writer.write(b'Warning: OPENCODE_SERVER_PASSWORD is not set; server is unsecured.\n')
        writer.write(b'opencode server listen')
        writer.write(f'ing on {self.URL}\n'.encode())
        writer.write(b'x' * (reader.LIMIT * 20))
        writer.close()
        reader.thread.join(timeout=2)
        self.assertFalse(reader.thread.is_alive())
        self.assertTrue(reader.ready)
        self.assertTrue(reader.eof)
        self.assertFalse(reader.failed)
        self.assertLessEqual(len(reader.pending), reader.LIMIT)

    def test_wrong_url_unterminated_and_embedded_lines_do_not_prove_readiness(self):
        for data in (b'opencode server listening on http://127.0.0.1:4097\n',
                     f'opencode server listening on {self.URL}'.encode(),
                     f'prefix opencode server listening on {self.URL}\n'.encode(),
                     b'x' * host_trials.ServerOutput.LIMIT +
                     f'opencode server listening on {self.URL}\n'.encode()):
            with self.subTest(data_length=len(data)):
                reader, writer = self.reader()
                reader.start()
                writer.write(data)
                writer.close()
                reader.thread.join(timeout=2)
                self.assertTrue(reader.eof)
                self.assertFalse(reader.ready)

    def test_select_failure_marks_reader_failed(self):
        reader, _ = self.reader()
        with unittest.mock.patch('select.select', side_effect=OSError('synthetic')):
            reader.start()
            reader.thread.join(timeout=2)
        self.assertTrue(reader.failed)
        self.assertFalse(reader.ready)

    def test_close_after_thread_start_failure_closes_owned_pipe(self):
        reader, _ = self.reader()
        with unittest.mock.patch.object(reader.thread, 'start', side_effect=RuntimeError('synthetic')):
            with self.assertRaises(RuntimeError):
                reader.start()
        reader.close()
        self.assertTrue(reader.stream.closed)


class FakeServerOutput:
    def __init__(self, stream, url):
        self.stream = stream
        self.expected = f'opencode server listening on {url}\n'.encode()
        self.ready = False
        self.failed = False
        self.eof = False

    def start(self):
        self.ready = self.expected in self.stream.readlines()
        self.eof = True

    def close(self):
        self.stream.close()


class FakePopen:
    def __init__(self, argv, **kwargs):
        self.argv = argv
        self.kwargs = kwargs
        self.pid = 4321
        self.returncode = None
        self.signals = []
        port = argv[argv.index('--port') + 1]
        self.stdout = io.BytesIO(f'opencode server listening on http://127.0.0.1:{port}\n'.encode())

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        if self.returncode is None:
            raise subprocess.TimeoutExpired(cmd=self.argv, timeout=timeout)
        return self.returncode


class OpenCodeDriverTests(DriverTestCase):
    def test_foreign_or_missing_export_bindings_never_supply_outcomes_model_or_version(self):
        for target in ('top', 'message', 'part-session', 'part-message'):
            for missing in (False, True):
                with self.subTest(target=target, missing=missing):
                    self.registry = SessionRegistry()
                    document = json.loads(opencode_export([
                        opencode_message('assistant', [{'type': 'text', 'text': MARKER}],
                                         1_757_754_001_000, providerID='foreign', modelID='model')]))
                    message = document['messages'][0]
                    owner, field = {'top': (document['info'], 'id'),
                                    'message': (message['info'], 'sessionID'),
                                    'part-session': (message['parts'][0], 'sessionID'),
                                    'part-message': (message['parts'][0], 'messageID')}[target]
                    if missing:
                        del owner[field]
                    else:
                        owner[field] = 'foreign'
                    driver = self.driver(FakeRun([(['opencode', '--pure', 'export'],
                                                  FakeResult(0, json.dumps(document)))]))
                    driver.mint('ses_1')
                    observation = driver.observe('ses_1', marker=MARKER, submitted_at=1_757_754_000)
                    self.assertFalse(observation.observable)
                    self.assertEqual(observation.outcomes, {})
                    self.assertIsNone(observation.model)
                    self.assertIsNone(driver.version('ses_1'))
                    driver.release('ses_1')

    def test_foreign_http_after_spawn_cannot_replace_child_readiness(self):
        def popen(argv, **kwargs):
            process = self.popen(argv, **kwargs)
            process.stdout = io.BytesIO(b'bind failed\n')
            return process

        driver = self.driver(FakeRun([]), popen=popen, port=4096)
        with self.assertRaisesRegex(RuntimeError, 'readiness'):
            driver.serve(timeout=0)
        self.assertIsNone(driver.server)
        self.assertEqual(len(self.gets), 1)  # only the pre-spawn listener check
        self.assertTrue(self.servers[0].stdout.closed)

    def test_reader_start_failure_closes_child_and_pipe(self):
        driver = self.driver(FakeRun([]), port=4096)
        with unittest.mock.patch.object(FakeServerOutput, 'start', side_effect=RuntimeError('synthetic')):
            with self.assertRaisesRegex(RuntimeError, 'synthetic'):
                driver.serve()
        self.assertIsNone(driver.server)
        self.assertTrue(self.servers[0].stdout.closed)

    def test_malformed_creation_stream_is_never_successful(self):
        for suffix in ('{"type":', '[1]', 'null', '"text"'):
            with self.subTest(suffix=suffix):
                self.registry = SessionRegistry()
                run = FakeRun([(['opencode', 'run'],
                                FakeResult(0, self.run_output().stdout + '\n' + suffix))])
                driver = self.driver(run)
                with self.assertRaisesRegex(RuntimeError, 'malformed'):
                    driver.create('hello')
                # The one proven creation ID remains available for cleanup after failure.
                self.assertEqual(driver.owned(), {'ses_1'})

    def test_malformed_attach_stream_is_uncaptured_even_with_a_matching_id(self):
        for suffix in ('{"type":', '[1]', 'null', '"text"'):
            for returncode in (0, 1, 'timeout'):
                with self.subTest(suffix=suffix, returncode=returncode):
                    self.registry = SessionRegistry()
                    self.servers = []
                    self.answering = False
                    output = self.run_output().stdout + '\n' + suffix

                    def attach(argv):
                        if returncode == 'timeout':
                            raise subprocess.TimeoutExpired(argv, 60, output=output)
                        return FakeResult(returncode, output)

                    run = FakeRun([(['opencode', 'run', '--pure', '--format', 'json', '--dir'],
                                    self.run_output()),
                                   (['opencode', 'run', '--pure', '--format', 'json', '--attach'], attach)])
                    driver = self.driver(run, port=4096)
                    driver.create('hello')
                    driver.serve()
                    with self.assertRaisesRegex(SubmissionUncaptured, 'malformed'):
                        driver.submit('ses_1', 'msg')

    def driver(self, run, **kwargs):
        kwargs.setdefault('popen', self.popen)
        kwargs.setdefault('http_get', self.http_get)
        kwargs.setdefault('sleep', lambda seconds: None)
        kwargs.setdefault('output_reader', FakeServerOutput)
        return OpenCodeDriver(self.registry, run=run, cwd=self.cwd, **kwargs)

    def setUp(self):
        super().setUp()
        self.servers = []
        self.answering = False  # whether GET /session answers before our child starts
        self.gets = []
        # Even unexpected fixture failures must never signal a real PID.
        self.patch_killpg()

    def popen(self, argv, **kwargs):
        process = FakePopen(argv, **kwargs)
        self.servers.append(process)
        self.answering = True
        return process

    def http_get(self, url):
        self.gets.append(url)
        if not self.answering:
            raise urllib.error.URLError('refused')
        return 200

    def run_output(self, session_id='ses_1', error=None):
        events = [{'type': 'step_start', 'timestamp': 1, 'sessionID': session_id}]
        if error:
            events.append({'type': 'error', 'sessionID': session_id, 'error': error})
        return FakeResult(0, '\n'.join(json.dumps(e) for e in events) + '\n')

    def test_create_runs_pure_json_with_the_free_model_and_mints_the_session(self):
        run = FakeRun([(['opencode', 'run'], self.run_output())])
        driver = self.driver(run)
        self.assertEqual(driver.create('hello'), 'ses_1')
        self.assertEqual(driver.owned(), {'ses_1'})
        argv = run.calls[0][0]
        self.assertEqual(argv[:6], ['opencode', 'run', '--pure', '--format', 'json', '--dir'])
        self.assertIn('-m', argv)
        self.assertEqual(argv[argv.index('-m') + 1], 'opencode/ling-3.0-flash-fin-free')
        self.assertEqual(argv[-1], 'hello')

    def test_create_mints_the_session_even_when_an_error_event_follows(self):
        run = FakeRun([(['opencode', 'run'], self.run_output(error={'name': 'ProviderAuthError'}))])
        driver = self.driver(run)
        with self.assertRaises(RuntimeError):
            driver.create('hello')
        self.assertEqual(driver.owned(), {'ses_1'})

    def test_ambiguous_creation_reports_every_candidate_without_owning_any(self):
        output = self.run_output().stdout + self.run_output(session_id='ses_2').stdout
        for result in (FakeResult(0, output),
                       subprocess.TimeoutExpired(cmd=['opencode'], timeout=180, output=output.encode())):
            with self.subTest(result=result):
                driver = self.driver(FakeRun([(['opencode', 'run'], result)]))
                with self.assertRaisesRegex(RuntimeError, 'ambiguous'):
                    driver.create('hello')
                self.assertEqual(driver.owned(), set())
                self.assertEqual([label for label, _ in sweep(driver)], ['ses_1', 'ses_2'])
                for sid in ('ses_1', 'ses_2'):
                    with self.assertRaises(ForeignSessionError):
                        driver.teardown(sid)

    def test_create_mints_from_partial_output_on_a_timeout(self):
        partial = self.run_output().stdout.encode()
        run = FakeRun([(['opencode', 'run'], subprocess.TimeoutExpired(cmd=['opencode'], timeout=180, output=partial))])
        driver = self.driver(run)
        with self.assertRaises(subprocess.TimeoutExpired):
            driver.create('hello')
        self.assertEqual(driver.owned(), {'ses_1'})

    def test_submit_without_a_server_is_uncaptured(self):
        run = FakeRun([(['opencode', 'run'], self.run_output())])
        driver = self.driver(run)
        driver.create('hello')
        with self.assertRaises(SubmissionUncaptured):
            driver.submit('ses_1', 'msg')

    def test_serve_refuses_a_port_that_already_answers(self):
        self.answering = True
        driver = self.driver(FakeRun([]), port=4096)
        with self.assertRaises(RuntimeError):
            driver.serve()
        self.assertEqual(self.servers, [])

    def test_an_http_error_response_still_proves_a_foreign_listener_exists(self):
        self.patch_killpg()
        driver = self.driver(FakeRun([]), port=4096, http_get=host_trials.http_status)
        error = urllib.error.HTTPError('http://127.0.0.1:4096/session', 401, 'synthetic', {}, None)
        with unittest.mock.patch('urllib.request.urlopen', side_effect=error):
            with self.assertRaisesRegex(RuntimeError, 'already answers'):
                driver.serve(timeout=0)
        self.assertEqual(self.servers, [])

    def test_an_error_status_is_not_our_servers_captured_readiness_response(self):
        killed = self.patch_killpg()
        driver = self.driver(FakeRun([]), port=4096,
                             http_get=unittest.mock.Mock(side_effect=[urllib.error.URLError('refused'), 500]))
        with self.assertRaises(RuntimeError):
            driver.serve(timeout=0)
        self.assertEqual(killed, [(4321, 15)])
        self.assertIsNone(driver.server)

    def test_serve_starts_our_own_child_in_its_own_session_and_waits_for_it_to_answer(self):
        driver = self.driver(FakeRun([]), port=4096)
        server = driver.serve()
        process, = self.servers
        self.assertEqual(process.argv, ['opencode', 'serve', '--pure', '--port', '4096'])
        self.assertTrue(process.kwargs['start_new_session'])
        self.assertEqual(server.url, 'http://127.0.0.1:4096')
        self.assertIs(driver.serve(), server)  # idempotent
        self.assertEqual(len(self.servers), 1)

    def test_serve_fails_when_the_child_exits_before_answering(self):
        def popen(argv, **kwargs):
            process = FakePopen(argv, **kwargs)
            process.returncode = 1
            self.servers.append(process)
            return process
        driver = self.driver(FakeRun([]), popen=popen, port=4096)
        with self.assertRaises(RuntimeError):
            driver.serve()
        self.assertIsNone(driver.server)

    def silent_popen(self, argv, **kwargs):
        # Our child starts but never answers: `answering` stays False.
        process = FakePopen(argv, **kwargs)
        self.servers.append(process)
        return process

    def patch_killpg(self, *, survivors=0):
        """Model a process group, not just the Popen leader.

        `Server.close()` asks about the whole group with signal 0, which must not be recorded as
        a kill and must not stop anything; `survivors` keeps that many descendants alive through
        every real signal, the shape a `serve` child leaving something behind takes on the host.
        """
        killed = []
        remaining = {}  # pid -> group members left, once a signal has been through

        def killpg(pid, signum):
            spawned = any(process.pid == pid and (process.returncode is None or survivors)
                          for process in self.servers)
            if not spawned or remaining.get(pid, 1 + survivors) == 0:
                raise ProcessLookupError(pid)
            if signum == 0:
                return
            killed.append((pid, signum))
            remaining[pid] = survivors
            for process in self.servers:
                if process.pid == pid:
                    process.returncode = -signum

        patcher = unittest.mock.patch('os.killpg', killpg)
        patcher.start()
        self.addCleanup(patcher.stop)
        return killed

    def test_serve_timeout_closes_the_child_it_started(self):
        killed = self.patch_killpg()
        driver = self.driver(FakeRun([]), popen=self.silent_popen, port=4096)
        with self.assertRaises(RuntimeError):
            driver.serve(timeout=0.0)
        self.assertEqual(killed, [(4321, 15)])
        self.assertIsNone(driver.server)

    def test_serve_retries_a_read_timeout_only_after_starting_its_own_child(self):
        self.patch_killpg()
        getter = unittest.mock.Mock(side_effect=[urllib.error.URLError('refused'),
                                                 TimeoutError('slow response'), 200])
        driver = self.driver(FakeRun([]), port=4096, http_get=getter)
        server = driver.serve()
        self.assertIs(server.process, self.servers[0])
        self.assertEqual(getter.call_count, 3)

    def test_a_preflight_read_timeout_does_not_authorize_spawning(self):
        driver = self.driver(FakeRun([]), port=4096,
                             http_get=unittest.mock.Mock(side_effect=TimeoutError('ambiguous listener')))
        with self.assertRaises(TimeoutError):
            driver.serve()
        self.assertEqual(self.servers, [])

    def test_serve_read_timeouts_stop_at_the_startup_deadline(self):
        killed = self.patch_killpg()
        getter = unittest.mock.Mock(side_effect=[urllib.error.URLError('refused'), TimeoutError('slow')])
        driver = self.driver(FakeRun([]), port=4096, http_get=getter)
        with self.assertRaisesRegex(RuntimeError, 'within 0'):
            driver.serve(timeout=0)
        self.assertEqual(killed, [(4321, 15)])
        self.assertIsNone(driver.server)

    def test_serve_holds_the_child_before_it_waits_for_readiness(self):
        # Ownership is taken with the spawn, not after the wait: anything running while `serve()`
        # is still probing -- a sweep, a second exit path -- has to find the child, not None.
        seen = []

        def http_get(url):
            if self.servers:
                seen.append(driver.server)
                return 200
            raise urllib.error.URLError('refused')

        driver = self.driver(FakeRun([]), popen=self.silent_popen, http_get=http_get, port=4096)
        server = driver.serve()
        self.assertEqual(seen, [server])
        self.assertIs(server.process, self.servers[0])

    def test_serve_closes_the_child_when_the_readiness_wait_is_interrupted(self):
        # A Ctrl-C (or any failing probe) after the spawn used to leave the child running with no
        # handle to it: `close_servers()` had nothing to close.
        for error in (KeyboardInterrupt(), ValueError('invalid probe')):
            with self.subTest(error=type(error).__name__):
                self.servers = []
                killed = self.patch_killpg()

                def http_get(url, error=error):
                    if self.servers:
                        raise error
                    raise urllib.error.URLError('refused')

                driver = self.driver(FakeRun([]), popen=self.silent_popen, http_get=http_get, port=4096)
                with self.assertRaises(type(error)):
                    driver.serve()
                self.assertEqual(killed, [(4321, 15)])
                self.assertIsNone(driver.server)

    def test_serve_keeps_the_handle_when_its_own_cleanup_cannot_stop_the_child(self):
        # A child that survived SIGTERM and SIGKILL is still running: dropping the handle would
        # leave `close_servers()` and the sweep with nothing to retry or report.
        unstoppable = unittest.mock.patch('os.killpg', lambda pid, signum: None)
        unstoppable.start()
        self.addCleanup(unstoppable.stop)
        driver = self.driver(FakeRun([]), popen=self.silent_popen, port=4096)
        with self.assertRaises(RuntimeError) as caught:
            driver.serve(timeout=0.0)
        self.assertIn('survived SIGTERM and SIGKILL', str(caught.exception))
        self.assertIsNotNone(driver.server)
        self.assertIs(driver.server.process, self.servers[0])

    def test_serve_refuses_to_reuse_a_held_server_whose_child_has_exited(self):
        driver = self.driver(FakeRun([]), port=4096)
        server = driver.serve()
        server.process.returncode = 1
        with self.assertRaises(RuntimeError):
            driver.serve()
        self.assertEqual(len(self.servers), 1)  # no second child behind the caller's back

    def test_submit_is_uncaptured_rather_than_rejected_once_the_serve_child_is_gone(self):
        run = FakeRun([(['opencode', 'run', '--pure', '--format', 'json', '--dir'], self.run_output()),
                       (['opencode', 'run', '--pure', '--format', 'json', '--attach'],
                        FakeResult(1, '', 'connect ECONNREFUSED'))])
        driver = self.driver(run, port=4096)
        driver.create('hello')
        server = driver.serve()
        server.process.returncode = -9
        with self.assertRaises(SubmissionUncaptured):
            driver.submit('ses_1', 'msg')
        self.assertEqual(run.argv('opencode', 'run', '--pure', '--format', 'json', '--attach'), [])
        # The child dying during the attach call: a nonzero exit is then not host evidence either.
        server.process.returncode = None

        def dying_attach(argv):
            server.process.returncode = -9
            return FakeResult(1, '', 'connect ECONNREFUSED')

        run.scripts.insert(0, (['opencode', 'run', '--pure', '--format', 'json', '--attach'], dying_attach))
        with self.assertRaises(SubmissionUncaptured):
            driver.submit('ses_1', 'msg')

    def test_submit_timeout_against_a_dead_serve_child_is_uncaptured(self):
        # The export is readable without the server, so a bare timeout would poll it and read the
        # absent marker as `not_observed` for a submission path that had disappeared.
        server = None

        def dying_attach(argv):
            server.process.returncode = -9
            raise subprocess.TimeoutExpired(cmd=argv, timeout=60)

        run = FakeRun([(['opencode', 'run', '--pure', '--format', 'json', '--dir'], self.run_output()),
                       (['opencode', 'run', '--pure', '--format', 'json', '--attach'], dying_attach)])
        driver = self.driver(run, port=4096)
        driver.create('hello')
        server = driver.serve()
        with self.assertRaises(SubmissionUncaptured):
            driver.submit('ses_1', 'msg')

    def test_submit_timeout_with_the_serve_child_alive_stays_a_timeout(self):
        # Nothing says the message failed to arrive, so the trial polls the export as usual. A
        # partial stream that named the requested session, or nothing at all, says nothing either:
        # silence in a truncated stream is not evidence of misdirection.
        for output in (None, b'', self.run_output().stdout.encode()):
            with self.subTest(output=output):
                # Each iteration mints `ses_1` and starts a server on the same port again, so it
                # needs a registry of its own and a port that does not answer yet.
                self.registry = SessionRegistry()
                self.answering = False
                error = subprocess.TimeoutExpired(cmd=['opencode'], timeout=60, output=output)
                run = FakeRun([(['opencode', 'run', '--pure', '--format', 'json', '--dir'],
                                self.run_output()),
                               (['opencode', 'run', '--pure', '--format', 'json', '--attach'], error)])
                driver = self.driver(run, port=4096)
                driver.create('hello')
                driver.serve()
                with self.assertRaises(subprocess.TimeoutExpired):
                    driver.submit('ses_1', 'msg')
                self.assertEqual(driver.owned(), {'ses_1'})

    def test_a_partial_attach_stream_that_already_failed_is_uncaptured_not_a_timeout(self):
        # The live-server timeout path used to ignore `TimeoutExpired.output` entirely, so a
        # submission the event stream had already reported failed or redirected was polled as one
        # that may have delivered -- turning the absent marker into `not_observed`.
        partial = self.run_output(error={'name': 'ProviderAuthError'}).stdout.encode()
        error = subprocess.TimeoutExpired(cmd=['opencode'], timeout=60, output=partial)
        run = FakeRun([(['opencode', 'run', '--pure', '--format', 'json', '--dir'], self.run_output()),
                       (['opencode', 'run', '--pure', '--format', 'json', '--attach'], error)])
        driver = self.driver(run, port=4096)
        driver.create('hello')
        driver.serve()
        with self.assertRaises(SubmissionUncaptured) as caught:
            driver.submit('ses_1', 'msg')
        self.assertIn('ProviderAuthError', str(caught.exception))

    def test_a_partial_attach_stream_naming_another_session_never_adopts_it(self):
        partial = self.run_output(session_id='ses_2').stdout.encode()
        error = subprocess.TimeoutExpired(cmd=['opencode'], timeout=60, output=partial)
        run = FakeRun([(['opencode', 'run', '--pure', '--format', 'json', '--dir'], self.run_output()),
                       (['opencode', 'run', '--pure', '--format', 'json', '--attach'], error)])
        driver = self.driver(run, port=4096)
        driver.create('hello')
        driver.serve()
        with self.assertRaises(SubmissionUncaptured) as caught:
            driver.submit('ses_1', 'msg')
        self.assertIn('ses_2', str(caught.exception))
        self.assertEqual(driver.owned(), {'ses_1'})
        with self.assertRaises(ForeignSessionError):
            driver.teardown('ses_2')
        driver.teardown = lambda sid: driver.release(sid)
        failures = sweep(driver)
        self.assertEqual([label for label, _ in failures], ['ses_2'])
        self.assertIn('manual', str(failures[0][1]))

    def test_submit_attaches_to_our_server_and_reports_exit_status(self):
        run = FakeRun([(['opencode', 'run', '--pure', '--format', 'json', '--dir'], self.run_output()),
                       (['opencode', 'run', '--pure', '--format', 'json', '--attach'], self.run_output())])
        driver = self.driver(run, port=4096)
        driver.create('hello')
        driver.serve()
        self.assertTrue(driver.submit('ses_1', 'msg'))
        argv = run.argv('opencode', 'run', '--pure', '--format', 'json', '--attach')[0]
        self.assertEqual(argv[5:9], ['--attach', 'http://127.0.0.1:4096', '--session', 'ses_1'])
        run.scripts.insert(0, (['opencode', 'run', '--pure', '--format', 'json', '--attach'],
                               FakeResult(1, '', 'session not found')))
        with self.assertRaises(SubmissionRejected):
            driver.submit('ses_1', 'msg')

    def test_an_exit_zero_attach_naming_no_session_is_uncaptured(self):
        # Captured: the attach client's own stdout carried a `step_start` event with its
        # `sessionID`. Silence is not proof the marker reached the session polling will read.
        run = FakeRun([(['opencode', 'run', '--pure', '--format', 'json', '--dir'], self.run_output()),
                       (['opencode', 'run', '--pure', '--format', 'json', '--attach'], FakeResult(0))])
        driver = self.driver(run, port=4096)
        driver.create('hello')
        driver.serve()
        with self.assertRaises(SubmissionUncaptured) as caught:
            driver.submit('ses_1', 'msg')
        self.assertIn('None', str(caught.exception))
        self.assertEqual(driver.owned(), {'ses_1'})

    def test_an_exit_zero_attach_naming_another_session_never_adopts_it(self):
        run = FakeRun([(['opencode', 'run', '--pure', '--format', 'json', '--dir'], self.run_output()),
                       (['opencode', 'run', '--pure', '--format', 'json', '--attach'],
                        self.run_output(session_id='ses_2'))])
        driver = self.driver(run, port=4096)
        driver.create('hello')
        driver.serve()
        with self.assertRaises(SubmissionUncaptured) as caught:
            driver.submit('ses_1', 'msg')
        self.assertIn('ses_2', str(caught.exception))
        # An attach event can name a human's existing session; it proves no creation authority.
        self.assertEqual(driver.owned(), {'ses_1'})
        with self.assertRaises(ForeignSessionError):
            driver.teardown('ses_2')
        driver.teardown = lambda sid: driver.release(sid)
        failures = sweep(driver)
        self.assertEqual([label for label, _ in failures], ['ses_2'])
        self.assertIn('manual', str(failures[0][1]))

    def test_an_error_event_on_an_exit_zero_attach_is_uncaptured_never_accepted(self):
        # Captured on `create()`: a provider/credential/model failure is a structured `error`
        # event while the command still exits 0. Reading the exit status alone would record the
        # trial as accepted and blame the host for the transcript outcomes that never arrive.
        error_event = json.dumps({'type': 'error', 'sessionID': 'ses_1',
                                  'error': {'name': 'ProviderAuthError'}})
        run = FakeRun([(['opencode', 'run', '--pure', '--format', 'json', '--dir'], self.run_output()),
                       (['opencode', 'run', '--pure', '--format', 'json', '--attach'],
                        FakeResult(0, error_event))])
        driver = self.driver(run, port=4096)
        driver.create('hello')
        driver.serve()
        with self.assertRaises(SubmissionUncaptured) as caught:
            driver.submit('ses_1', 'msg')
        self.assertIn('ProviderAuthError', str(caught.exception))

    def test_later_attach_events_cannot_redirect_an_apparently_matching_stream(self):
        output = self.run_output().stdout + '\n' + self.run_output(session_id='ses_human').stdout
        for result in (FakeResult(0, output),
                       subprocess.TimeoutExpired(cmd=['opencode'], timeout=60, output=output.encode())):
            with self.subTest(result=result):
                self.registry = SessionRegistry()
                self.answering = False
                run = FakeRun([(['opencode', 'run', '--pure', '--format', 'json', '--dir'],
                                self.run_output()),
                               (['opencode', 'run', '--pure', '--format', 'json', '--attach'], result)])
                driver = self.driver(run, port=4096)
                driver.create('hello')
                driver.serve()
                with self.assertRaises(SubmissionUncaptured):
                    driver.submit('ses_1', 'msg')
                self.assertEqual(driver.owned(), {'ses_1'})
                self.assertEqual(driver.strays, {'ses_human'})
                with self.assertRaises(ForeignSessionError):
                    driver.teardown('ses_human')

    def test_a_nonzero_attach_error_event_is_uncaptured_with_its_diagnostics(self):
        error_event = json.dumps({'type': 'error', 'error': {'name': 'UnknownModel'}})
        run = FakeRun([(['opencode', 'run', '--pure', '--format', 'json', '--dir'], self.run_output()),
                       (['opencode', 'run', '--pure', '--format', 'json', '--attach'],
                        FakeResult(1, error_event, 'exited 1'))])
        driver = self.driver(run, port=4096)
        driver.create('hello')
        driver.serve()
        with self.assertRaises(SubmissionUncaptured) as caught:
            driver.submit('ses_1', 'msg')
        self.assertIn('UnknownModel', str(caught.exception))
        self.assertIn('exited 1', str(caught.exception))

    def test_observe_and_version_read_the_export_and_fail_closed(self):
        export = opencode_export([
            opencode_message('user', [{'type': 'text', 'text': MARKER}], 1_757_754_001_000),
            opencode_message('assistant', [{'type': 'text', 'text': MARKER}], 1_757_754_002_000)])
        run = FakeRun([(['opencode', 'run'], self.run_output()),
                       (['opencode', '--pure', 'export'], FakeResult(0, export))])
        driver = self.driver(run)
        driver.create('hello')
        observation = driver.observe('ses_1', marker=MARKER, submitted_at=1_757_754_000.0)
        self.assertEqual(set(observation.outcomes), {'visible', 'turn_start', 'ack'})
        self.assertFalse(observation.turn_stream)
        self.assertEqual(driver.version('ses_1'), '1.18.30')
        run.scripts.insert(0, (['opencode', '--pure', 'export'], FakeResult(1, '', 'nope')))
        self.assertFalse(driver.observe('ses_1', marker=MARKER, submitted_at=0.0).observable)
        self.assertIsNone(driver.version('ses_1'))
        run.scripts.insert(0, (['opencode', '--pure', 'export'], FakeResult(0, 'garbage')))
        self.assertFalse(driver.observe('ses_1', marker=MARKER, submitted_at=0.0).observable)

    def test_teardown_deletes_and_releases_only_on_exit_zero(self):
        run = FakeRun([(['opencode', 'run'], self.run_output()),
                       (['opencode', '--pure', 'session', 'delete'], FakeResult(1, '', 'nope'))])
        driver = self.driver(run)
        driver.create('hello')
        with self.assertRaises(RuntimeError):
            driver.teardown('ses_1')
        self.assertEqual(driver.owned(), {'ses_1'})
        run.scripts.insert(0, (['opencode', '--pure', 'session', 'delete'], FakeResult(0, '', 'Session ses_1 deleted')))
        driver.teardown('ses_1')
        self.assertEqual(driver.owned(), set())

    def test_close_servers_signals_the_group_and_forgets_the_server(self):
        killed = self.patch_killpg()
        driver = self.driver(FakeRun([]), port=4096)
        driver.serve()
        self.assertEqual(driver.close_servers(), [])
        self.assertEqual(killed, [(4321, 15)])
        self.assertIsNone(driver.server)

    def test_close_servers_reports_a_child_that_survives_sigkill_and_keeps_holding_it(self):
        signals = []

        def killpg(pid, signum):
            if signum:  # signal 0 is the group-liveness probe, not a kill
                signals.append(signum)

        patcher = unittest.mock.patch('os.killpg', killpg)
        patcher.start()
        self.addCleanup(patcher.stop)
        driver = self.driver(FakeRun([]), port=4096)
        driver.serve()
        failures = driver.close_servers()
        self.assertEqual([label for label, _ in failures], ['server'])
        self.assertIsInstance(failures[0][1], RuntimeError)
        self.assertEqual(signals, [15, 9])
        self.assertIsNotNone(driver.server)  # still ours to report, never silently forgotten

    def test_close_servers_reports_a_descendant_that_outlives_the_serve_child(self):
        # The leader exiting is not the server stopping: `serve` is its own session leader, so
        # anything it spawned stays in the group, keeps the port bound and keeps answering on the
        # same URL. Waiting on the Popen handle alone reported that as a clean stop, released the
        # handle, and left the next `serve()` to refuse its own leftover as a foreign server.
        killed = self.patch_killpg(survivors=1)
        driver = self.driver(FakeRun([]), port=4096)
        driver.serve()
        failures = driver.close_servers()
        self.assertEqual([label for label, _ in failures], ['server'])
        self.assertIn('ownership cannot be verified', str(failures[0][1]))
        self.assertEqual(killed, [(4321, 15)])
        self.assertIsNotNone(driver.server.process.returncode)  # the leader did exit
        self.assertIsNotNone(driver.server)  # and the handle is kept anyway

    def test_close_never_signals_a_group_after_reaping_its_original_leader(self):
        for already_reaped in (True, False):
            with self.subTest(already_reaped=already_reaped):
                self.answering = False
                driver = self.driver(FakeRun([]), port=4096)
                server = driver.serve()

                def reap():
                    server.process.returncode = 0
                    return 0

                if already_reaped:
                    reap()
                else:
                    server.process.poll = reap
                with unittest.mock.patch('os.killpg') as killpg:
                    failures = driver.close_servers()
                self.assertEqual([label for label, _ in failures], ['server'])
                self.assertIn('ownership cannot be verified', str(failures[0][1]))
                self.assertTrue(all(call.args[1] == 0 for call in killpg.call_args_list))
                self.assertIs(driver.server, server)

    def test_foreign_ids_are_refused_everywhere(self):
        driver = self.driver(FakeRun([]))
        for call in (lambda: driver.submit('other', 'x'),
                     lambda: driver.observe('other', marker=MARKER, submitted_at=0.0),
                     lambda: driver.teardown('other'), lambda: driver.version('other')):
            with self.assertRaises(ForeignSessionError):
                call()


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


class FakeDriver(Driver):
    """Scripted host: `observations` is consumed one entry per observe() call, last repeating.

    Ids minted here go through a real `SessionRegistry` under the `fake` namespace so the sweep
    tests exercise the same ownership path the real drivers use.
    """

    NAMESPACE = 'fake'

    def __init__(self, *, observations=None, accepted=True, submit_error=None,
                 observe_error=None, version_value=None, version_error=None, clock=None,
                 teardown_errors=None, submission_note=None, registry=None, on_observe=None):
        self.registry = registry or SessionRegistry()
        self.clients = []
        self.strays = set()
        self.observations = list(observations or [Observation()])
        self.accepted = accepted
        self.submit_error = submit_error
        self.observe_error = observe_error
        self.version_value = version_value
        self.version_error = version_error
        self.clock = clock
        self.teardown_errors = dict(teardown_errors or {})
        self.submission_note = submission_note
        self.on_observe = on_observe
        self.order = []
        self.torn_down = []
        self.server_closes = 0

    def create(self, prompt):
        self.order.append('create')
        self.mint('sid')
        return 'sid'

    def version(self, session_id):
        assert session_id == 'sid'
        self.order.append('version')
        if self.version_error is not None:
            raise self.version_error
        return self.version_value

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
        if self.on_observe is not None:
            self.on_observe()
        if self.observe_error is not None:
            raise self.observe_error
        return self.observations.pop(0) if len(self.observations) > 1 else self.observations[0]

    def teardown(self, session_id):
        self.order.append(f'teardown:{session_id}')
        self.torn_down.append(session_id)
        if session_id in self.teardown_errors:
            raise self.teardown_errors[session_id]
        self.release(session_id)

    def close_servers(self):
        self.order.append('close_servers')
        self.server_closes += 1
        return []


class RunTrialTests(unittest.TestCase):
    def run_one(self, driver, clock, **kwargs):
        return run_trial(driver, prompt='hi', marker=MARKER, clock=clock.time,
                          monotonic=clock.monotonic, sleep=clock.sleep, **kwargs)

    def test_submission_asks_the_host_to_echo_the_marker_rather_than_sending_it_bare(self):
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

    def test_a_numeric_submit_result_is_the_drivers_own_acceptance_time(self):
        # Codex queue-then-resume: `codex queue` exits 0, then the resume client takes 15s to
        # start. Stamping at return would push a real acceptance out of its 10s window.
        clock = FakeClock()
        driver = FakeDriver(accepted=1003.5, clock=clock)
        run = self.run_one(driver, clock)
        self.assertEqual(run.accepted_at, 1003.5)
        self.assertEqual(run.outcomes['accepted'], 1003.5)
        self.assertEqual(run.signals['accepted'], SIGNAL_SUBMIT_EXIT_STATUS)
        self.assertGreater(clock.time(), 1003.5)  # submit() returned later than it stamped
        for bad in ('yes', float('nan'), object()):
            with self.subTest(result=bad):
                with self.assertRaises(TypeError):
                    self.run_one(FakeDriver(accepted=bad, clock=FakeClock()), FakeClock())

    def test_the_result_carries_which_signal_established_each_outcome(self):
        clock = FakeClock()
        driver = FakeDriver(observations=[Observation(
            outcomes={'visible': 1000.0, 'turn_start': 1001.0, 'ack': 1002.0},
            signals={'visible': SIGNAL_USER_MESSAGE, 'turn_start': SIGNAL_ASSISTANT_MESSAGE,
                     'ack': SIGNAL_ASSISTANT_MESSAGE})], clock=clock)
        run = self.run_one(driver, clock)
        self.assertEqual(run.signals, {'visible': SIGNAL_USER_MESSAGE,
                                        'turn_start': SIGNAL_ASSISTANT_MESSAGE,
                                        'ack': SIGNAL_ASSISTANT_MESSAGE,
                                        'accepted': SIGNAL_SUBMIT_EXIT_STATUS})

    def test_a_keyboardinterrupt_during_polling_finalizes_a_partial_result(self):
        # A busy trial's poll loop can wait up to 900s; losing everything gathered so far to a
        # propagated interrupt would discard real evidence instead of finalizing it.
        clock = FakeClock()
        driver = FakeDriver(observations=[Observation(outcomes={'visible': 1000.0},
                                                       signals={'visible': SIGNAL_USER_MESSAGE})],
                            clock=clock)

        def interrupting_sleep(seconds):
            raise KeyboardInterrupt

        run = run_trial(driver, prompt='hi', marker=MARKER, clock=clock.time,
                        monotonic=clock.monotonic, sleep=interrupting_sleep)
        self.assertEqual(run.outcomes.get('visible'), 1000.0)
        self.assertNotIn('turn_start', run.outcomes)
        self.assertNotIn('ack', run.outcomes)
        self.assertTrue(run.observable['visible'])
        self.assertFalse(run.observable['turn_start'])
        self.assertFalse(run.observable['ack'])
        self.assertTrue(run.interrupted)

    def test_a_keyboardinterrupt_inside_the_first_observe_still_finalizes(self):
        clock = FakeClock()
        driver = FakeDriver(observe_error=KeyboardInterrupt(), clock=clock)
        run = self.run_one(driver, clock)
        self.assertTrue(run.interrupted)
        self.assertEqual(run.outcomes, {'accepted': 1012.0})
        self.assertFalse(any(run.observable[name] for name in ('visible', 'turn_start', 'ack')))
        self.assertFalse(run.turn_end_observable)

    def test_a_clock_correction_also_discards_the_attributed_model(self):
        for step in (60.0, -60.0):
            with self.subTest(step=step):
                clock = FakeClock()
                driver = FakeDriver(
                    observations=[Observation(outcomes={'turn_start': 1001.0}, model='prior-model')],
                    clock=clock, on_observe=lambda: setattr(clock, 'wall', clock.wall + step))
                run = self.run_one(driver, clock)
                self.assertIsNotNone(run.clock_step)
                self.assertIsNone(run.model)

    def test_a_wall_clock_correction_during_polling_makes_the_whole_trial_unobservable(self):
        # Every compared stamp is wall time, so a correction moves host events relative to their
        # windows -- a +60s step alone turns a reply 2s after submission into one 62s after it,
        # outside the 30s visibility window. Nothing can rebase a stamp another process wrote.
        for step in (60.0, -60.0):
            with self.subTest(step=step):
                clock = FakeClock()
                driver = FakeDriver(
                    observations=[Observation(outcomes={'visible': 1000.0, 'turn_start': 1001.0},
                                              signals={'visible': SIGNAL_USER_MESSAGE},
                                              turn_end=1005.0)],
                    clock=clock, on_observe=lambda clock=clock: setattr(clock, 'wall',
                                                                        clock.wall + step))
                run = self.run_one(driver, clock, state='busy', settle=lambda session_id: None)
                self.assertAlmostEqual(run.clock_step, step)
                self.assertEqual(run.outcomes, {})
                self.assertEqual(run.signals, {})
                self.assertIsNone(run.accepted_at)
                self.assertIsNone(run.turn_end)
                self.assertFalse(run.turn_end_observable)
                self.assertFalse(any(run.observable.values()))
                trial = Trial(submitted=run.submitted_at, state=run.state, turn_end=run.turn_end,
                              turn_end_observable=run.turn_end_observable, outcomes=run.outcomes)
                if step > 0:
                    self.assertEqual(classify_trial(trial, clock.time(), supported=run.supported,
                                                    observable=run.observable),
                                     {name: 'unobservable' for name in OUTCOME_NAMES})
                else:
                    # The documented residual: a step backwards past the trial's own duration
                    # puts the caller's `now` before submission, and `Trial.result` refuses the
                    # run outright rather than classifying it -- fail-closed the same way.
                    with self.assertRaises(ValueError):
                        classify_trial(trial, clock.time(), supported=run.supported,
                                       observable=run.observable)

    def test_slew_within_tolerance_leaves_an_ordinary_trial_alone(self):
        # NTP slew is bounded at 500 ppm, well under a second across even the 900s busy cap; a
        # tolerance that fired on it would make every long cell unobservable.
        clock = FakeClock()
        driver = FakeDriver(observations=[Observation(outcomes={'visible': 1000.0,
                                                                'turn_start': 1001.0,
                                                                'ack': 1002.0})],
                            clock=clock,
                            on_observe=lambda: setattr(clock, 'wall',
                                                       clock.wall + CLOCK_DRIFT_TOLERANCE / 2))
        run = self.run_one(driver, clock)
        self.assertIsNone(run.clock_step)
        self.assertEqual(run.outcomes['visible'], 1000.0)
        self.assertTrue(all(run.observable.values()))

    def test_descheduling_during_clock_sampling_is_not_a_clock_step(self):
        for sample in (1, 3):  # baseline and first poll (sample 2 records acceptance)
            for side in ('before', 'after'):
                with self.subTest(sample=sample, side=side):
                    clock = FakeClock()
                    calls = 0

                    def wall_time():
                        nonlocal calls
                        calls += 1
                        if calls == sample and side == 'before':
                            clock.elapsed += 3.0
                        value = clock.time()
                        if calls == sample and side == 'after':
                            clock.elapsed += 3.0
                        return value

                    driver = FakeDriver(observations=[Observation(outcomes={
                        'visible': 1015.0, 'turn_start': 1016.0, 'ack': 1017.0})], clock=clock)
                    run = run_trial(driver, prompt='hi', marker=MARKER, clock=wall_time,
                                    monotonic=clock.monotonic, sleep=clock.sleep)
                    self.assertIsNone(run.clock_step)
                    self.assertEqual(run.outcomes['ack'], 1017.0)

    def test_the_result_carries_the_driver_version_captured_at_trial_time(self):
        clock = FakeClock()
        driver = FakeDriver(version_value='2.1.270', clock=clock)
        run = self.run_one(driver, clock)
        self.assertEqual(run.version, '2.1.270')

    def test_the_result_carries_the_serving_model_the_first_poll_that_saw_one_read(self):
        # `run_trial_with_cleanup` deletes the session, so this is the caller's only chance to
        # record the model -- and the requested one is not it (`--model haiku` was captured not
        # being honoured). A poll that names none must not overwrite one already read.
        clock = FakeClock()
        observations = [Observation(),
                        Observation(outcomes={'visible': 1000.0}, model='claude-sonnet-5'),
                        Observation(outcomes={'turn_start': 1002.0, 'ack': 1002.0})]
        run = self.run_one(FakeDriver(observations=observations, clock=clock), clock)
        self.assertEqual(run.model, 'claude-sonnet-5')

    def test_the_result_model_is_none_when_no_poll_ever_named_one(self):
        clock = FakeClock()
        run = self.run_one(FakeDriver(clock=clock), clock)
        self.assertIsNone(run.model)

    def test_a_version_read_failure_records_none_rather_than_losing_the_trial(self):
        clock = FakeClock()
        driver = FakeDriver(version_error=RuntimeError('transcript unreadable'), clock=clock)
        run = self.run_one(driver, clock)
        self.assertIsNone(run.version)
        self.assertEqual(run.session_id, 'sid')  # the rest of the trial still completed normally

    def test_settle_runs_between_create_and_submit(self):
        clock = FakeClock()
        driver = FakeDriver(clock=clock)
        received = []

        def settle(session_id):
            received.append(session_id)
            driver.order.append('settle')

        self.run_one(driver, clock, settle=settle)
        self.assertEqual(driver.order[:4], ['create', 'version', 'settle', 'submit'])
        self.assertEqual(received, ['sid'])  # settle must be able to target the created session

    def test_failures_after_create_propagate_raw_and_leave_the_session_owned(self):
        # The registry, not a wrapper exception, is what names the live session now; the
        # caller's sweep (run_trial_with_cleanup) reads it. Nothing is swallowed or re-typed.
        cases = [
            ('settle', dict(), lambda session_id: (_ for _ in ()).throw(RuntimeError('no busy state'))),
            ('submit', dict(submit_error=RuntimeError('claude agents exited 1')), None),
            ('observe', dict(observe_error=OSError('transcript vanished')), None),
        ]
        for label, kwargs, settle in cases:
            with self.subTest(stage=label):
                clock = FakeClock()
                driver = FakeDriver(clock=clock, **kwargs)
                with self.assertRaises((RuntimeError, OSError)):
                    self.run_one(driver, clock, settle=settle)
                self.assertEqual(driver.owned(), {'sid'})

    def test_rejected_submission_does_not_add_accepted(self):
        clock = FakeClock()
        run = self.run_one(FakeDriver(accepted=False, clock=clock), clock)
        self.assertNotIn('accepted', run.outcomes)
        self.assertIsNone(run.accepted_at)
        self.assertEqual(run.marker, MARKER)  # the submitted token is still evidence, even rejected

    def test_rejected_submission_skips_polling_and_marks_transcript_outcomes_unobservable(self):
        clock = FakeClock()
        driver = FakeDriver(accepted=False, clock=clock)
        run = self.run_one(driver, clock)
        self.assertNotIn('observe', driver.order)
        self.assertTrue(run.observable['accepted'])
        self.assertFalse(run.observable['visible'])
        self.assertFalse(run.observable['turn_start'])
        self.assertFalse(run.observable['ack'])
        trial = Trial(submitted=run.submitted_at, state=run.state, outcomes=run.outcomes)
        with self.assertRaises(ValueError):
            classify_trial(trial, run.submitted_at, observable=run.observable)
        classified = classify_trial(trial, run.submitted_at + 1000, observable=run.observable)
        self.assertEqual(classified['accepted'], 'not_observed')
        self.assertEqual(classified['visible'], 'unobservable')

    def test_a_none_from_submit_leaves_accepted_unobservable_and_still_polls(self):
        # The attach path: a PTY write is never host acceptance, so `accepted` is unobservable
        # -- but the message may well have been delivered, so the transcript outcomes are polled
        # and stand on their own evidence.
        clock = FakeClock()
        driver = FakeDriver(accepted=None, submission_note='attach: typed at ...',
                            observations=[Observation(outcomes={'visible': 1000.0, 'turn_start': 1001.0,
                                                                'ack': 1002.0})], clock=clock)
        run = self.run_one(driver, clock)
        self.assertIsNone(run.accepted_at)
        self.assertNotIn('accepted', run.outcomes)
        self.assertNotIn('accepted', run.signals)
        self.assertFalse(run.observable['accepted'])
        self.assertIn('observe', driver.order)
        self.assertTrue(run.observable['visible'])
        self.assertEqual(run.outcomes['ack'], 1002.0)
        self.assertEqual(run.submission_diagnostic, 'attach: typed at ...')
        trial = Trial(submitted=run.submitted_at, state=run.state, outcomes=run.outcomes)
        classified = classify_trial(trial, run.submitted_at + 1000, observable=run.observable)
        self.assertEqual(classified['accepted'], 'unobservable')
        self.assertEqual(classified['ack'], 'observed')

    def test_submission_rejected_carries_its_diagnostic_rather_than_a_bare_false(self):
        clock = FakeClock()
        driver = FakeDriver(submit_error=SubmissionRejected(3, 'thread expired'), clock=clock)
        run = self.run_one(driver, clock)
        self.assertNotIn('accepted', run.outcomes)
        self.assertIsNone(run.accepted_at)
        self.assertIn('thread expired', run.submission_diagnostic)
        self.assertIn('3', run.submission_diagnostic)
        self.assertNotIn('observe', driver.order)

    def test_observation_continues_through_the_windows_instead_of_one_snapshot(self):
        clock = FakeClock()
        late = Observation(outcomes={'visible': 1000.0, 'turn_start': 1030.0, 'ack': 1031.0})
        driver = FakeDriver(observations=[Observation(), Observation(), late], clock=clock)
        run = self.run_one(driver, clock, poll_interval=5.0)
        self.assertEqual(driver.order.count('observe'), 3)
        self.assertEqual(set(run.outcomes), {'accepted', 'visible', 'turn_start', 'ack'})
        self.assertTrue(all(run.observable.values()))
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
        clock = FakeClock()
        seen = Observation(outcomes={'visible': 1000.0}, observable=True)
        dead = Observation(observable=False)
        driver = FakeDriver(observations=[seen, dead], clock=clock)
        run = self.run_one(driver, clock, poll_interval=60.0)
        self.assertTrue(run.observable['visible'])  # a positive stands on its own evidence
        self.assertFalse(run.observable['ack'])  # the tail of the window went unread

    def test_a_readable_final_poll_covers_an_earlier_failed_one(self):
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
        self.assertEqual(run.marker, MARKER)
        trial = Trial(submitted=run.submitted_at, state=run.state, outcomes=run.outcomes)
        classified = classify_trial(trial, run.submitted_at + 1000, supported=run.supported)
        self.assertTrue(all(value == 'unsupported' for value in classified.values()))

    def test_uncaptured_submission_is_unobservable_not_a_claim_about_the_host(self):
        clock = FakeClock()
        driver = FakeDriver(submit_error=SubmissionUncaptured('attach never showed the composer: ...'),
                            clock=clock)
        run = self.run_one(driver, clock)
        self.assertTrue(all(run.supported.values()))  # no claim that the host lacks the path
        self.assertFalse(any(run.observable.values()))
        self.assertFalse(run.interrupted)
        self.assertIn('never showed the composer', run.submission_diagnostic)
        trial = Trial(submitted=run.submitted_at, state=run.state, outcomes=run.outcomes,
                      turn_end_observable=run.turn_end_observable)
        classified = classify_trial(trial, run.submitted_at + 1000,
                                    supported=run.supported, observable=run.observable)
        self.assertTrue(all(value == 'unobservable' for value in classified.values()))

    def test_a_submission_timeout_is_unobservable_acceptance_but_still_polls_for_evidence(self):
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

    def test_an_idle_trial_never_adopts_a_turn_end(self):
        clock = FakeClock()
        seen = Observation(outcomes={'visible': 1000.0, 'turn_start': 1001.0, 'ack': 1002.0},
                           turn_end=1002.0)
        run = self.run_one(FakeDriver(observations=[seen], clock=clock), clock)
        self.assertIsNone(run.turn_end)

    def test_a_busy_trial_without_a_turn_end_leaves_its_dependent_outcomes_unobservable(self):
        clock = FakeClock()
        driver = FakeDriver(observations=[Observation(outcomes={'visible': 1000.0})], clock=clock)
        run = self.run_one(driver, clock, state='busy', poll_interval=300.0,
                           settle=lambda session_id: None)
        self.assertGreaterEqual(clock.elapsed, 900)  # BUSY_CAP, not the 120s idle window
        self.assertIsNone(run.turn_end)
        self.assertTrue(run.observable['visible'])
        self.assertFalse(run.observable['ack'])
        trial = Trial(submitted=run.submitted_at, state=run.state, outcomes=run.outcomes,
                      turn_end=run.turn_end, turn_end_observable=run.turn_end_observable)
        classified = classify_trial(trial, run.submitted_at + 1000, observable=run.observable)
        self.assertEqual(classified['turn_start'], 'unobservable')

    def test_a_busy_trial_without_turn_stream_never_trusts_a_bare_assistant_message(self):
        clock = FakeClock()
        driver = FakeDriver(observations=[Observation(outcomes={'turn_start': 1005.0})], clock=clock)
        run = self.run_one(driver, clock, state='busy', poll_interval=300.0,
                           settle=lambda session_id: None)
        self.assertIsNone(run.turn_end)
        self.assertIn('turn_start', run.outcomes)
        self.assertFalse(run.observable['turn_start'])
        trial = Trial(submitted=run.submitted_at, state=run.state, outcomes=run.outcomes,
                      turn_end=run.turn_end, turn_end_observable=run.turn_end_observable)
        classified = classify_trial(trial, run.submitted_at + 1000, observable=run.observable)
        self.assertEqual(classified['turn_start'], 'unobservable')

    def test_a_busy_trial_without_turn_stream_records_no_model(self):
        # The model comes off the same assistant record whose turn_start cannot be attributed;
        # it may be the turn that was already running.
        clock = FakeClock()
        driver = FakeDriver(observations=[Observation(outcomes={'turn_start': 1005.0},
                                                      model='claude-opus-5')], clock=clock)
        run = self.run_one(driver, clock, state='busy', poll_interval=300.0,
                           settle=lambda session_id: None)
        self.assertIsNone(run.model)

    def test_a_busy_trial_on_a_turn_stream_host_keeps_its_model(self):
        # The host's own boundary attributes the turn, so the reading stands.
        clock = FakeClock()
        driver = FakeDriver(observations=[Observation(outcomes={'turn_start': 1005.0},
                                                      turn_stream=True, model='gpt-5')],
                            clock=clock)
        run = self.run_one(driver, clock, state='busy', poll_interval=300.0,
                           settle=lambda session_id: None)
        self.assertEqual(run.model, 'gpt-5')

    def test_a_turn_stream_hosts_readable_busy_timeout_stays_inconclusive(self):
        clock = FakeClock()
        driver = FakeDriver(observations=[Observation(turn_stream=True)], clock=clock)
        run = self.run_one(driver, clock, state='busy', poll_interval=300.0,
                           settle=lambda session_id: None)
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
        run = self.run_one(driver, clock, state='busy', poll_interval=300.0,
                           settle=lambda session_id: None)
        self.assertEqual(run.turn_end, 1030.0)
        self.assertLess(clock.elapsed, 900)  # the dependent windows closed before the cap
        trial = Trial(submitted=run.submitted_at, state=run.state, outcomes=run.outcomes,
                      turn_end=run.turn_end, turn_end_observable=run.turn_end_observable)
        classified = classify_trial(trial, run.submitted_at + 1000, observable=run.observable)
        self.assertEqual(classified['ack'], 'observed')

    def test_an_early_turn_end_collapses_the_busy_deadline_to_its_own_window(self):
        # Submitted at wall 1000; the slow submit returns at +12s, when the first poll reports a
        # turn end at wall 1030. The deadline becomes LAST_WINDOW past that turn end: at the poll
        # that is 120 - (1012 - 1030) = 138s away, so polling stops at +150s.
        clock = FakeClock()
        seen = Observation(outcomes={'visible': 1012.0, 'turn_start': 1040.0}, turn_end=1030.0,
                           turn_stream=True)
        driver = FakeDriver(observations=[seen], clock=clock)
        run = self.run_one(driver, clock, state='busy', poll_interval=300.0,
                           settle=lambda session_id: None)
        self.assertEqual(run.turn_end, 1030.0)
        self.assertEqual(clock.elapsed, 150.0)  # not BUSY_CAP, and not cut short either

    def test_a_turn_ending_close_to_the_cap_extends_the_wait_past_it(self):
        clock = FakeClock()
        turn_end = 1850.0  # wall time; submitted_at is 1000.0, so this is 850s in
        early = Observation(outcomes={'visible': 1012.0})
        mid = Observation(outcomes={'visible': 1012.0, 'turn_start': 1840.0}, turn_end=turn_end)
        late = Observation(outcomes={'visible': 1012.0, 'turn_start': 1840.0, 'ack': 1900.0},
                            turn_end=turn_end)
        driver = FakeDriver(observations=[early, mid, late], clock=clock)
        run = self.run_one(driver, clock, state='busy', poll_interval=890.0,
                           settle=lambda session_id: None)
        self.assertEqual(run.turn_end, turn_end)
        self.assertEqual(run.outcomes.get('ack'), 1900.0)
        self.assertGreater(clock.elapsed, 900)  # polled past BUSY_CAP to cover the real end

    def test_requested_state_is_validated_and_carried_into_the_result(self):
        clock = FakeClock()
        run = self.run_one(FakeDriver(clock=clock), clock, state='busy',
                           settle=lambda session_id: None)
        self.assertEqual(run.state, 'busy')
        with self.assertRaises(ValueError):
            self.run_one(FakeDriver(clock=clock), clock, state='asleep')

    def test_a_non_idle_state_without_an_explicit_settle_is_rejected(self):
        clock = FakeClock()
        for state in ('busy', 'approval', 'disconnected', 'restarted'):
            with self.assertRaises(ValueError):
                self.run_one(FakeDriver(clock=clock), clock, state=state)

    def test_poll_interval_is_validated_before_any_session_exists_or_marker_is_sent(self):
        for bad in (-1.0, 0.0, float('nan')):
            clock = FakeClock()
            driver = FakeDriver(clock=clock)
            with self.assertRaises(ValueError):
                self.run_one(driver, clock, poll_interval=bad)
            self.assertEqual(driver.order, [])

    def test_marker_is_validated_before_any_session_exists_or_is_sent(self):
        for bad in ('', 'hello', MARKER.upper(), 'PARLEY-PROBE-tooshort', MARKER + '\n'):
            clock = FakeClock()
            driver = FakeDriver(clock=clock)
            with self.assertRaises(ValueError):
                run_trial(driver, prompt='hi', marker=bad, clock=clock.time,
                          monotonic=clock.monotonic, sleep=clock.sleep)
            self.assertEqual(driver.order, [])

    def test_omitting_the_marker_generates_a_fresh_one_each_call(self):
        clock = FakeClock()
        first_driver = FakeDriver(clock=clock)
        first_run = run_trial(first_driver, prompt='hi', clock=clock.time,
                              monotonic=clock.monotonic, sleep=clock.sleep)
        clock = FakeClock()
        second_driver = FakeDriver(clock=clock)
        second_run = run_trial(second_driver, prompt='hi', clock=clock.time,
                               monotonic=clock.monotonic, sleep=clock.sleep)
        self.assertNotEqual(first_run.marker, second_run.marker)
        self.assertIsNotNone(MARKER_PATTERN.fullmatch(first_run.marker))
        self.assertIsNotNone(MARKER_PATTERN.fullmatch(second_run.marker))
        prefix = 'Automated probe: reply with exactly this token to confirm receipt: '
        self.assertEqual(first_driver.submitted_message.removeprefix(prefix), first_run.marker)
        self.assertEqual(second_driver.submitted_message.removeprefix(prefix), second_run.marker)


class ClosingClient:
    def __init__(self, log, fail=False):
        self.log = log
        self.fail = fail

    def close(self):
        self.log.append('client')
        if self.fail:
            raise OSError('pty gone')


class SweepTests(unittest.TestCase):
    def test_sweep_closes_clients_then_tears_down_every_owned_id_then_closes_servers(self):
        driver = FakeDriver()
        driver.mint('b')
        driver.mint('a')
        other = object()
        driver.registry.mint('other:zzz', other)  # another driver's session on the shared registry
        driver.clients.append(ClosingClient(driver.order))
        self.assertEqual(sweep(driver), [])
        self.assertEqual(driver.order, ['client', 'teardown:a', 'teardown:b', 'close_servers'])
        self.assertEqual(driver.owned(), set())
        self.assertEqual(driver.registry.created, {'other:zzz': other})
        self.assertEqual(driver.clients, [])

    def test_sweep_continues_past_a_failed_teardown_and_reports_it(self):
        driver = FakeDriver(teardown_errors={'a': RuntimeError('stop left a running')})
        driver.mint('a')
        driver.mint('b')
        driver.clients.append(ClosingClient(driver.order))
        failures = sweep(driver)
        self.assertEqual([label for label, _ in failures], ['a'])
        self.assertEqual(driver.torn_down, ['a', 'b'])
        self.assertEqual(driver.owned(), {'a'})
        self.assertEqual(driver.server_closes, 1)

    def test_a_client_that_failed_to_close_blocks_every_teardown_and_stays_held(self):
        # A client whose close failed may still be the process serving its session, and it is the
        # only handle to it: tearing the session down anyway would delete a thread still in use,
        # and dropping the handle would leave the child unrecoverable and unnamed.
        driver = FakeDriver()
        driver.mint('a')
        driver.mint('b')
        client = ClosingClient(driver.order, fail=True)
        driver.clients.append(client)
        failures = sweep(driver)
        self.assertEqual([label for label, _ in failures], ['client'])
        self.assertEqual(driver.torn_down, [])
        self.assertEqual(driver.owned(), {'a', 'b'})
        self.assertEqual(driver.clients, [client])
        self.assertEqual(driver.server_closes, 1)  # the server is this run's, and still closes


class RunTrialWithCleanupTests(unittest.TestCase):
    def test_cleanup_failure_is_not_hidden_by_an_outer_exception_handler(self):
        clock = FakeClock()
        driver = FakeDriver(clock=clock, teardown_errors={'sid': RuntimeError('cleanup failed')})
        try:
            raise ValueError('unrelated outer exception')
        except ValueError as outer:
            with self.assertRaises(CleanupFailed) as caught:
                self.run_one(driver, clock)
            self.assertEqual(caught.exception.run.session_id, 'sid')
            self.assertFalse(hasattr(outer, '__notes__'))

    def test_interrupt_after_successful_trial_return_still_sweeps(self):
        clock = FakeClock()
        driver = FakeDriver(clock=clock)
        returned = False
        original_run = host_trials.run_trial

        def completed(*args, **kwargs):
            nonlocal returned
            result = original_run(*args, **kwargs)
            returned = True
            return result

        def interrupt_handoff(frame, event, arg):
            if returned and event == 'line' and frame.f_code is run_trial_with_cleanup.__code__:
                raise KeyboardInterrupt
            return interrupt_handoff

        previous_trace = sys.gettrace()
        try:
            with unittest.mock.patch.object(host_trials, 'run_trial', completed):
                sys.settrace(interrupt_handoff)
                with self.assertRaises(KeyboardInterrupt):
                    self.run_one(driver, clock)
        finally:
            sys.settrace(previous_trace)
        self.assertEqual(driver.torn_down, ['sid'])
        self.assertEqual(driver.owned(), set())

    def run_one(self, driver, clock, **kwargs):
        return run_trial_with_cleanup(driver, prompt='hi', marker=MARKER, clock=clock.time,
                                      monotonic=clock.monotonic, sleep=clock.sleep, **kwargs)

    def test_a_completed_trial_is_swept_and_returned(self):
        clock = FakeClock()
        driver = FakeDriver(clock=clock)
        run = self.run_one(driver, clock)
        self.assertEqual(run.session_id, 'sid')
        self.assertEqual(driver.owned(), set())
        self.assertEqual(driver.order[-2:], ['teardown:sid', 'close_servers'])

    def test_a_failure_in_any_stage_still_sweeps_every_owned_id(self):
        cases = [
            ('settle', dict(), lambda session_id: (_ for _ in ()).throw(RuntimeError('no busy state'))),
            ('submit', dict(submit_error=RuntimeError('listing exited 1')), None),
            ('observe', dict(observe_error=OSError('transcript vanished')), None),
        ]
        for label, kwargs, settle in cases:
            with self.subTest(stage=label):
                clock = FakeClock()
                driver = FakeDriver(clock=clock, **kwargs)
                with self.assertRaises((RuntimeError, OSError)):
                    self.run_one(driver, clock, settle=settle)
                self.assertEqual(driver.torn_down, ['sid'])
                self.assertEqual(driver.owned(), set())

    def test_a_keyboardinterrupt_in_settle_still_sweeps(self):
        clock = FakeClock()
        driver = FakeDriver(clock=clock)

        def interrupting_settle(session_id):
            raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            self.run_one(driver, clock, settle=interrupting_settle)
        self.assertEqual(driver.torn_down, ['sid'])

    def test_a_copy_minted_during_submit_is_swept_too(self):
        clock = FakeClock()
        driver = FakeDriver(clock=clock, submit_error=SubmissionRejected(0, 'started a copy'))
        original_submit = driver.submit

        def minting_submit(session_id, message):
            driver.mint('copy')
            return original_submit(session_id, message)

        driver.submit = minting_submit
        run = self.run_one(driver, clock)
        self.assertIsNone(run.accepted_at)
        self.assertEqual(sorted(driver.torn_down), ['copy', 'sid'])
        self.assertEqual(driver.owned(), set())

    def test_a_failed_sweep_after_a_completed_trial_raises_cleanupfailed_with_the_run(self):
        clock = FakeClock()
        driver = FakeDriver(clock=clock, teardown_errors={'sid': RuntimeError('rm exited 1')})
        with self.assertRaises(CleanupFailed) as caught:
            self.run_one(driver, clock)
        self.assertEqual(caught.exception.run.session_id, 'sid')
        self.assertEqual(caught.exception.owned, ['sid'])
        self.assertEqual([label for label, _ in caught.exception.failures], ['sid'])

    def test_a_failed_sweep_after_a_failed_trial_annotates_the_original_error(self):
        clock = FakeClock()
        driver = FakeDriver(clock=clock, submit_error=RuntimeError('listing exited 1'),
                            teardown_errors={'sid': RuntimeError('rm exited 1')})
        with self.assertRaises(RuntimeError) as caught:
            self.run_one(driver, clock)
        self.assertEqual(str(caught.exception), 'listing exited 1')
        self.assertTrue(any("still owned: ['sid']" in note for note in caught.exception.__notes__))


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

    def test_generated_tokens_satisfy_run_trials_own_validation(self):
        self.assertIsNotNone(MARKER_PATTERN.fullmatch(marker_token()))


if __name__ == '__main__':
    unittest.main()

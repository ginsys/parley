"""Controlled fixtures only: no test here launches an installed Claude, Codex or OpenCode CLI."""

import datetime
import json
import os
import subprocess
import tempfile
import unittest
from dataclasses import dataclass
from unittest.mock import patch

from host_trials import (
    MARKER_PATTERN,
    SIGNAL_ASSISTANT_MESSAGE,
    SIGNAL_SUBMIT_EXIT_STATUS,
    SIGNAL_TURN_BOUNDARY_EVENT,
    SIGNAL_USER_MESSAGE,
    AmbiguousSessionCreation,
    ClaudeDriver,
    CodexDriver,
    Event,
    ForeignSessionError,
    Observation,
    ObservationFailed,
    OpenCodeDriver,
    SessionCreationUncaptured,
    SessionRegistry,
    SettleFailed,
    SubmissionFailed,
    SubmissionRejected,
    SubmissionUncaptured,
    SubmissionUnsupported,
    TeardownUnsupported,
    VersionProbeInterrupted,
    background_sessions,
    classify_trial,
    codex_rollout_events,
    codex_session_version,
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

    def test_background_sessions_raises_on_a_non_dict_list_entry(self):
        # A malformed entry silently dropped by the filter, rather than rejected, lets a
        # snapshot read as "just fewer sessions" instead of "untrustworthy for diffing" -- a
        # race where a real background session is malformed in one snapshot and well-formed in
        # the next could then have create() mint it as this trial's own.
        with self.assertRaises(RuntimeError):
            background_sessions(json.dumps([{'id': 'a', 'kind': 'background'}, 'not-an-object']))

    def test_background_sessions_raises_on_an_empty_string_id(self):
        with self.assertRaises(RuntimeError):
            background_sessions(json.dumps([{'id': '', 'kind': 'background'}]))

    def test_background_sessions_raises_on_an_unrecognized_kind(self):
        # A missing or schema-drifted kind was previously dropped by the same `!= 'background'`
        # check used to skip the known `interactive` entries. If that entry is a real background
        # session that is merely malformed in this snapshot -- and well-formed in the other --
        # silently dropping it here would let create()'s diff mint it as this trial's own.
        with self.assertRaises(RuntimeError):
            background_sessions(json.dumps([{'id': 'a'}]))
        with self.assertRaises(RuntimeError):
            background_sessions(json.dumps([{'id': 'a', 'kind': 'zombie'}]))

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
        # A schema-drifted, unhashable payload.type (e.g. a list) crashed the
        # `kind in TURN_BOUNDARY_ROLES` membership test with a TypeError.
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'event_msg',
                             'payload': {'type': []}})]
        self.assertEqual(codex_rollout_events(lines), ([], 1))

    def test_a_relevant_record_with_a_non_object_payload_is_unusable(self):
        # A response_item/event_msg record is one of the two kinds this runner reads at all --
        # a missing or malformed payload there is a corrupted or schema-drifted record, not one
        # of the record kinds this runner has no use for.
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

    def test_null_content_is_unusable_rather_than_a_typeerror(self):
        # payload.get('content', []) only substitutes [] when the key is absent; an explicit
        # "content": null slips past that default and a bare iteration would raise TypeError.
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'response_item',
                             'payload': {'type': 'message', 'role': 'user', 'content': None}})]
        self.assertEqual(codex_rollout_events(lines), ([], 1))

    def test_object_valued_content_is_unusable_not_silently_empty(self):
        # A structured/tool-call payload shape (content as a single object, not a list of parts)
        # must not parse as an empty-text event with observable=True -- that reads as "checked,
        # nothing there" instead of "this shape was never captured".
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'response_item',
                             'payload': {'type': 'message', 'role': 'assistant',
                                         'content': {'type': 'tool_call', 'name': 'x'}}})]
        self.assertEqual(codex_rollout_events(lines), ([], 1))

    def test_a_non_string_part_text_is_unusable_rather_than_a_typeerror(self):
        # A list-shaped content whose part carries a non-string `text` (e.g. explicit null) made
        # ''.join(...) raise instead of reading as an unrecognized shape.
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'response_item',
                             'payload': {'type': 'message', 'role': 'user',
                                         'content': [{'text': None}]}})]
        self.assertEqual(codex_rollout_events(lines), ([], 1))

    def test_a_non_dict_part_makes_the_whole_record_unusable_not_silently_shorter(self):
        # Silently skipping just the non-dict part let an unreadable marker message join down to
        # an empty, ordinary-looking string -- negative evidence rather than unusable.
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'response_item',
                             'payload': {'type': 'message', 'role': 'user',
                                         'content': ['not-a-part', {'text': 'hi'}]}})]
        self.assertEqual(codex_rollout_events(lines), ([], 1))

    def test_a_part_omitting_text_entirely_makes_the_whole_record_unusable(self):
        # `.get('text', '')` let a part with no `text` key default to an empty string and pass
        # as captured -- an unreadable marker message would join down to an ordinary-looking
        # empty string, negative evidence rather than the unusable read it actually is.
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'response_item',
                             'payload': {'type': 'message', 'role': 'assistant',
                                         'content': [{'type': 'text'}]}})]
        self.assertEqual(codex_rollout_events(lines), ([], 1))

    def test_a_part_type_mismatched_with_its_records_role_is_unusable(self):
        # input_text belongs to a user record, output_text to an assistant one
        # (docs/host-probe-preflight.md) -- accepting any string-valued `text` regardless of
        # `type` would also accept an output_text part inside a user record (or the reverse), a
        # shape this runner has never captured.
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'response_item',
                             'payload': {'type': 'message', 'role': 'user',
                                         'content': [{'type': 'output_text', 'text': MARKER}]}})]
        self.assertEqual(codex_rollout_events(lines), ([], 1))

    def test_a_payload_missing_its_type_key_is_unusable_not_silently_skipped(self):
        # `payload.get('type')` returning `None` for a missing key must not fall through the
        # same path as a recognized-but-irrelevant string kind -- both `response_item` and
        # `event_msg` always carry `payload.type` in every captured shape, so its absence here
        # is a corrupted or drifted record, not evidence of a successful read.
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'response_item',
                             'payload': {'role': 'assistant',
                                         'content': [{'type': 'output_text', 'text': MARKER}]}}),
                json.dumps({'type': 'event_msg', 'payload': {'role': 'assistant'}})]
        self.assertEqual(codex_rollout_events(lines), ([], 2))

    def test_a_record_missing_its_outer_type_key_is_unusable_not_silently_skipped(self):
        # record.get('type') returning None (or a non-string) for a missing/malformed outer
        # discriminator must not fall through the same path as a recognized-but-irrelevant
        # string kind like token_usage_record/world_state/turn_context/session_meta -- those
        # are always a recognized string in every captured shape, so a missing or non-string
        # outer type here is a corrupted or drifted record, not evidence of a successful read.
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z',
                             'payload': {'type': 'message', 'role': 'assistant',
                                         'content': [{'type': 'output_text', 'text': MARKER}]}}),
                json.dumps({'type': ['response_item'],
                            'payload': {'type': 'message', 'role': 'assistant',
                                        'content': [{'type': 'output_text', 'text': MARKER}]}})]
        self.assertEqual(codex_rollout_events(lines), ([], 2))

    def test_a_non_object_record_is_unusable_rather_than_an_attributeerror(self):
        # Valid JSON is not necessarily an object -- a damaged or schema-drifted line can decode
        # to `null`, a number or a list -- and `record.get(...)` on any of those raises instead
        # of reading as an unrecognized shape, aborting the whole read rather than just the line.
        lines = [json.dumps(None), json.dumps([1, 2]), json.dumps(3),
                json.dumps({'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'response_item',
                            'payload': {'type': 'message', 'role': 'user',
                                        'content': [{'type': 'input_text', 'text': 'hi'}]}})]
        events, unusable = codex_rollout_events(lines)
        self.assertEqual(unusable, 3)
        self.assertEqual(len(events), 1)

    def test_an_unrelated_outer_record_type_is_skipped_regardless_of_payload_shape(self):
        # The outer type gates first: session_meta/world_state/turn_context/token_usage_record
        # records are skipped silently no matter what their payload happens to contain, so an
        # unrelated record cannot masquerade as transcript evidence via a colliding payload.type.
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'turn_context',
                             'payload': {'type': 'message', 'role': 'user',
                                         'content': [{'text': MARKER}]}})]
        self.assertEqual(codex_rollout_events(lines), ([], 0))

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

    def test_codex_session_version_reads_the_confirmed_payload_shape(self):
        # Shape confirmed from a real rollout on this workstation
        # (~/.codex/sessions/2026/09/08/rollout-2026-09-08T16-42-57-*.jsonl).
        lines = [json.dumps({'timestamp': '2026-09-08T14:44:05.650Z', 'type': 'session_meta',
                             'payload': {'cli_version': '0.153.4'}})]
        self.assertEqual(codex_session_version(lines), '0.153.4')

    def test_codex_session_version_is_none_without_a_session_meta_record(self):
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'response_item'})]
        self.assertIsNone(codex_session_version(lines))

    def test_codex_session_version_is_none_when_the_payload_carries_no_cli_version(self):
        lines = [json.dumps({'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'session_meta',
                             'payload': {}})]
        self.assertIsNone(codex_session_version(lines))

    def test_codex_session_version_skips_an_unparseable_line_rather_than_failing_closed(self):
        # Unlike rollout_started_at, this is best-effort evidence enrichment with a client
        # fallback available -- an unreadable line does not need to poison the whole read.
        lines = ['not json', json.dumps({'timestamp': '2026-09-11T00:00:00.000Z',
                                        'type': 'session_meta',
                                        'payload': {'cli_version': '0.153.4'}})]
        self.assertEqual(codex_session_version(lines), '0.153.4')

    def test_codex_session_version_skips_a_valid_but_non_object_record(self):
        # A bare JSON scalar, list or null is valid JSON -- json.loads() succeeds -- but calling
        # .get() on it raises AttributeError, losing a later genuine session_meta record's
        # version instead of skipping the record as this helper's docstring promises.
        lines = ['null', '42', '[1, 2]', '"a string"',
                 json.dumps({'timestamp': '2026-09-11T00:00:00.000Z', 'type': 'session_meta',
                            'payload': {'cli_version': '0.153.4'}})]
        self.assertEqual(codex_session_version(lines), '0.153.4')


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
            calls.append((argv, kwargs))
            return next(responses)

        driver = ClaudeDriver(registry, run=fake_run, cwd='/scratch')
        session_id = driver.create('probe prompt')
        self.assertEqual(session_id, 'abcd1234')
        self.assertIn('claude:abcd1234', registry.created)
        # The root command has no --cwd flag (docs/host-probe-preflight.md); the session's
        # directory is the subprocess cwd, the same directory the listings filter on.
        self.assertEqual(calls[1][0][:3], ['claude', '--bg', '--print'])
        self.assertNotIn('--cwd', calls[1][0])
        self.assertEqual(calls[1][1].get('cwd'), '/scratch')

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

    def test_create_never_mints_an_ambiguous_session_so_teardown_cannot_reach_a_foreign_one(self):
        # Minting an unverified id gave it the same teardown authority as a session this runner
        # actually created -- teardown() would then accept and `claude rm` a session that could
        # be an unrelated human's, contradicting the ownership guarantee. Candidates are surfaced
        # on the exception for a human to investigate out of band, never minted into the registry.
        registry = SessionRegistry()
        before = json.dumps([])
        after = json.dumps([{'id': 'a', 'kind': 'background'}, {'id': 'b', 'kind': 'background'}])
        responses = iter([FakeResult(0, stdout=before), FakeResult(0, stdout=''), FakeResult(0, stdout=after)])

        def fake_run(argv, **kwargs):
            return next(responses)

        driver = ClaudeDriver(registry, run=fake_run, cwd='/scratch')
        with self.assertRaises(AmbiguousSessionCreation) as ctx:
            driver.create('probe prompt')
        self.assertEqual(ctx.exception.candidates, ('a', 'b'))
        self.assertEqual(registry.created, set())

    def test_create_raises_when_the_post_create_listing_itself_fails(self):
        # claude --bg can succeed and still leave a real, live session with an unknown id if the
        # follow-up listing call times out, exits nonzero, or returns malformed JSON -- that
        # failure must not propagate as an unrelated exception from _background_session_ids().
        # The recovery attempt itself also fails here, so candidates still comes back empty --
        # a real recovery is covered by the next test.
        responses = iter([FakeResult(0, stdout=json.dumps([])), FakeResult(0, stdout=''),
                          FakeResult(1, stderr='daemon unavailable'),
                          FakeResult(1, stderr='daemon unavailable')])

        def fake_run(argv, **kwargs):
            return next(responses)

        driver = ClaudeDriver(SessionRegistry(), run=fake_run, cwd='/scratch')
        with self.assertRaises(AmbiguousSessionCreation) as ctx:
            driver.create('probe prompt')
        self.assertEqual(ctx.exception.candidates, ())

    def test_create_recovers_candidates_when_the_post_create_listing_itself_fails(self):
        # Unlike the case above, a fresh recovery listing here succeeds -- the transient failure
        # must not be treated as reason to give up with an empty candidate set when a retry can
        # still find the id.
        before = json.dumps([{'id': 'old1', 'kind': 'background'}])
        after = json.dumps([{'id': 'old1', 'kind': 'background'}, {'id': 'new1', 'kind': 'background'}])
        responses = iter([FakeResult(0, stdout=before), FakeResult(0, stdout=''),
                          FakeResult(1, stderr='daemon unavailable'), FakeResult(0, stdout=after)])

        def fake_run(argv, **kwargs):
            return next(responses)

        driver = ClaudeDriver(SessionRegistry(), run=fake_run, cwd='/scratch')
        with self.assertRaises(AmbiguousSessionCreation) as ctx:
            driver.create('probe prompt')
        self.assertEqual(ctx.exception.candidates, ('new1',))

    def test_create_recovers_candidates_when_claude_bg_itself_times_out(self):
        # claude --bg can exceed its own 30s timeout after already detaching the background
        # session -- the subprocess call raises before returning, but the session it forked is
        # not thereby undone. A best-effort post-timeout listing should still surface it.
        before = json.dumps([{'id': 'old1', 'kind': 'background'}])
        after = json.dumps([{'id': 'old1', 'kind': 'background'}, {'id': 'new1', 'kind': 'background'}])
        responses = iter([FakeResult(0, stdout=before), FakeResult(0, stdout=after)])

        def fake_run(argv, **kwargs):
            if '--bg' in argv:
                raise subprocess.TimeoutExpired(cmd=argv, timeout=kwargs.get('timeout', 30))
            return next(responses)

        driver = ClaudeDriver(SessionRegistry(), run=fake_run, cwd='/scratch')
        with self.assertRaises(AmbiguousSessionCreation) as ctx:
            driver.create('probe prompt')
        self.assertEqual(ctx.exception.candidates, ('new1',))

    def test_create_reports_no_candidates_when_bg_timeout_recovery_listing_also_fails(self):
        # The recovery attempt's own failure must not escalate to a second, unrelated exception
        # -- it swallows to an empty candidate set since the caller is already reporting the
        # original timeout.
        before = json.dumps([])

        def fake_run(argv, **kwargs):
            if '--bg' in argv:
                raise subprocess.TimeoutExpired(cmd=argv, timeout=kwargs.get('timeout', 30))
            if not hasattr(fake_run, 'called'):
                fake_run.called = True
                return FakeResult(0, stdout=before)
            return FakeResult(1, stderr='daemon unavailable')

        driver = ClaudeDriver(SessionRegistry(), run=fake_run, cwd='/scratch')
        with self.assertRaises(AmbiguousSessionCreation) as ctx:
            driver.create('probe prompt')
        self.assertEqual(ctx.exception.candidates, ())

    def test_create_reports_no_candidates_when_a_second_interrupt_hits_the_recovery_listing(self):
        # A second Ctrl-C while the best-effort recovery listing runs must not escalate into a
        # bare KeyboardInterrupt -- the caller is already mid-raise of AmbiguousSessionCreation
        # built from this method's return value, so letting it escape here would destroy that
        # signal and its candidate-cleanup path for nothing; the original ambiguity is real
        # either way.
        before = json.dumps([])

        def fake_run(argv, **kwargs):
            if '--bg' in argv:
                raise subprocess.TimeoutExpired(cmd=argv, timeout=kwargs.get('timeout', 30))
            if not hasattr(fake_run, 'called'):
                fake_run.called = True
                return FakeResult(0, stdout=before)
            raise KeyboardInterrupt()

        driver = ClaudeDriver(SessionRegistry(), run=fake_run, cwd='/scratch')
        with self.assertRaises(AmbiguousSessionCreation) as ctx:
            driver.create('probe prompt')
        self.assertEqual(ctx.exception.candidates, ())

    def test_create_recovers_candidates_when_interrupted_during_the_post_create_listing(self):
        # claude --bg can exit 0 -- a background session definitely exists -- and then a Ctrl-C
        # lands while the post-create listing itself is being read. That listing's own except
        # clause caught only RuntimeError/TimeoutExpired, so KeyboardInterrupt escaped without a
        # minted id or recovered candidates even though a fresh listing could still find one.
        before = json.dumps([{'id': 'old1', 'kind': 'background'}])
        after = json.dumps([{'id': 'old1', 'kind': 'background'}, {'id': 'new1', 'kind': 'background'}])
        calls = []

        def fake_run(argv, **kwargs):
            calls.append(argv)
            if '--bg' in argv:
                return FakeResult(0)
            if len(calls) == 3:
                raise KeyboardInterrupt()
            return FakeResult(0, stdout=before if len(calls) == 1 else after)

        driver = ClaudeDriver(SessionRegistry(), run=fake_run, cwd='/scratch')
        with self.assertRaises(AmbiguousSessionCreation) as ctx:
            driver.create('probe prompt')
        self.assertEqual(ctx.exception.candidates, ('new1',))
        self.assertIsInstance(ctx.exception.__cause__, KeyboardInterrupt)

    def test_create_recovers_candidates_when_interrupted_while_claude_bg_is_blocked(self):
        # A Ctrl-C while claude --bg is running shares the timeout case's ambiguity: the
        # background session may already have detached before the interrupt landed. This must
        # not propagate the bare KeyboardInterrupt and lose the candidate id.
        before = json.dumps([{'id': 'old1', 'kind': 'background'}])
        after = json.dumps([{'id': 'old1', 'kind': 'background'}, {'id': 'new1', 'kind': 'background'}])
        responses = iter([FakeResult(0, stdout=before), FakeResult(0, stdout=after)])

        def fake_run(argv, **kwargs):
            if '--bg' in argv:
                raise KeyboardInterrupt()
            return next(responses)

        driver = ClaudeDriver(SessionRegistry(), run=fake_run, cwd='/scratch')
        with self.assertRaises(AmbiguousSessionCreation) as ctx:
            driver.create('probe prompt')
        self.assertEqual(ctx.exception.candidates, ('new1',))
        self.assertIsInstance(ctx.exception.__cause__, KeyboardInterrupt)

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
        registry.mint('claude:abcd1234')
        present = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(
            0, stdout=json.dumps([{'id': 'abcd1234', 'kind': 'background', 'state': 'idle'}])), cwd='/scratch')
        with self.assertRaises(SubmissionUncaptured):
            present.submit('abcd1234', 'msg')
        self.assertNotIsInstance(SubmissionUncaptured(''), SubmissionUnsupported)

    def test_submit_lists_background_sessions_and_reports_a_failed_listing(self):
        # Without --all a completed background session drops out of the listing, so an owned
        # session could fail the membership check; a nonzero exit is reported, not parsed as JSON.
        registry = SessionRegistry()
        registry.mint('claude:abcd1234')
        seen = []

        def fake_run(argv, **kwargs):
            seen.append(argv)
            return FakeResult(1, stderr='daemon unreachable')

        driver = ClaudeDriver(registry, run=fake_run, cwd='/scratch')
        with self.assertRaises(RuntimeError):
            driver.submit('abcd1234', 'msg')
        self.assertIn('--all', seen[0])

    def test_submit_translates_a_listing_timeout_into_uncaptured_not_a_raw_timeout(self):
        # The listing is a presence check, run entirely before submit()'s unconditional
        # SubmissionUncaptured raise -- its timing out means the marker was definitely never
        # sent. Left as a raw TimeoutExpired, run_trial's generic handler would treat it as
        # "maybe delivered before the timeout fired", which is not true for a driver with no
        # delivery mechanism at all.
        registry = SessionRegistry()
        registry.mint('claude:abcd1234')

        def fake_run(argv, **kwargs):
            raise subprocess.TimeoutExpired(cmd=argv, timeout=kwargs.get('timeout', 15))

        driver = ClaudeDriver(registry, run=fake_run, cwd='/scratch')
        with self.assertRaises(SubmissionUncaptured):
            driver.submit('abcd1234', 'msg')

    def test_submit_rejects_a_session_absent_from_the_listing_before_anything_else(self):
        registry = SessionRegistry()
        registry.mint('claude:abcd1234')
        absent = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(0, stdout='[]'), cwd='/scratch')
        with self.assertRaises(ValueError) as caught:
            absent.submit('abcd1234', 'msg')
        self.assertNotIsInstance(caught.exception, SubmissionUnsupported)

    def test_observe_is_unobservable_when_logs_unreachable(self):
        # Reproduces the real observation: `claude logs <id>` fails once a `done` session's
        # daemon socket is gone (ENOENT). A dead observation channel is not a negative result,
        # so it must not reach classification as an empty (hence not_observed) outcome map.
        registry = SessionRegistry()
        registry.mint('claude:abcd1234')
        driver = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(1, stderr='connect ENOENT'), cwd='/scratch')
        self.assertEqual(driver.observe('abcd1234', marker=MARKER, submitted_at=0.0),
                          Observation(outcomes={}, observable=False))

    def test_observe_is_unobservable_when_claude_logs_times_out(self):
        # A stalled claude logs raised TimeoutExpired uncaught, aborting the whole trial instead
        # of returning the same unobservable result a nonzero exit already produces.
        registry = SessionRegistry()
        registry.mint('claude:abcd1234')

        def fake_run(argv, **kwargs):
            raise subprocess.TimeoutExpired(cmd=argv, timeout=kwargs.get('timeout', 15))

        driver = ClaudeDriver(registry, run=fake_run, cwd='/scratch')
        self.assertEqual(driver.observe('abcd1234', marker=MARKER, submitted_at=0.0),
                          Observation(outcomes={}, observable=False))

    def test_observe_of_an_untimestamped_transcript_is_unobservable(self):
        # `claude logs` prints no per-line timestamp, so a reachable transcript still cannot be
        # ordered against submission: the create() prompt's own turn would otherwise be read as
        # this trial's turn_start before the marker existed.
        registry = SessionRegistry()
        registry.mint('claude:abcd1234')
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
        registry.mint('claude:abcd1234')
        driver = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(0, stdout='some other format\n'),
                              cwd='/scratch')
        observation = driver.observe('abcd1234', marker=MARKER, submitted_at=0.0)
        self.assertEqual(observation, Observation(outcomes={}, observable=False))

    def test_observe_of_genuinely_empty_output_is_still_observable(self):
        # A session with no conversation yet (nothing sent, or a poll racing session creation)
        # must not be conflated with an unrecognized shape: empty output is a legitimate read.
        registry = SessionRegistry()
        registry.mint('claude:abcd1234')
        driver = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(0, stdout='  \n'), cwd='/scratch')
        observation = driver.observe('abcd1234', marker=MARKER, submitted_at=0.0)
        self.assertEqual(observation, Observation(outcomes={}, observable=True))

    def test_teardown_releases_the_registry_entry(self):
        registry = SessionRegistry()
        registry.mint('claude:abcd1234')
        driver = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(0), cwd='/scratch')
        driver.teardown('abcd1234')
        with self.assertRaises(ForeignSessionError):
            registry.require_owned('claude:abcd1234')

    def test_failed_teardown_keeps_ownership_so_it_can_be_retried(self):
        registry = SessionRegistry()
        registry.mint('claude:abcd1234')
        driver = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(1, stderr='rm failed'), cwd='/scratch')
        with self.assertRaises(RuntimeError):
            driver.teardown('abcd1234')
        registry.require_owned('claude:abcd1234')  # still ours: the live session can still be removed

    def test_version_reports_stripped_stdout(self):
        driver = ClaudeDriver(SessionRegistry(),
                               run=lambda *a, **k: FakeResult(0, stdout='2.1.268\n'), cwd='/scratch')
        self.assertEqual(driver.version('abcd1234'), '2.1.268')

    def test_version_is_none_when_the_command_fails(self):
        driver = ClaudeDriver(SessionRegistry(),
                               run=lambda *a, **k: FakeResult(1, stderr='not found'), cwd='/scratch')
        self.assertIsNone(driver.version('abcd1234'))

    def test_version_is_none_when_the_run_call_itself_raises(self):
        def raising_run(*_args, **_kwargs):
            raise subprocess.TimeoutExpired(cmd=['claude', '--version'], timeout=15)

        driver = ClaudeDriver(SessionRegistry(), run=raising_run, cwd='/scratch')
        self.assertIsNone(driver.version('abcd1234'))


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
        return {'timestamp': stamp, 'type': 'response_item',
                'payload': {'type': 'message', 'role': role,
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

    def test_a_non_finite_started_at_is_rejected_at_construction(self):
        # A NaN boundary makes both `started < self.started_at` (register_existing) and
        # `started >= self.started_at` (the rival scan) evaluate False, so an arbitrarily old
        # rollout could adopt as owned while every concurrent candidate is silently ruled out.
        for bad in (float('nan'), float('inf'), float('-inf')):
            with self.assertRaises(ValueError):
                CodexDriver(SessionRegistry(), rollout_path_for=lambda _id: None, started_at=bad,
                           sessions_root=self.sessions_root)

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

    def test_register_existing_refuses_a_rollout_outside_sessions_root(self):
        # _unruled_out_threads only ever walks sessions_root for rivals; an adopted rollout that
        # actually lives outside that tree is invisible to that scan, so an inconsistent
        # (sessions_root, rollout_path_for) pairing would otherwise silently disable the
        # concurrent-thread guard instead of failing closed.
        outside_root = tempfile.TemporaryDirectory()
        self.addCleanup(outside_root.cleanup)
        elsewhere = os.path.join(outside_root.name, 'rollout-elsewhere.jsonl')
        with open(elsewhere, 'w', encoding='utf-8') as handle:
            handle.write(json.dumps({'timestamp': '2026-09-11T12:00:30.000Z',
                                      'type': 'session_meta'}) + '\n')
        registry = SessionRegistry()
        driver = CodexDriver(registry, run=lambda *a, **k: FakeResult(0),
                             rollout_path_for=lambda _id: elsewhere, started_at=self.RUN_STARTED,
                             sessions_root=self.sessions_root)
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
        self.assertEqual(registry.created, {'codex:thread-1', 'codex:thread-2', 'codex:thread-3'})

    def test_an_owned_threads_rollout_path_going_missing_does_not_crash_adoption(self):
        # rollout_path_for is a live lookup, not a snapshot: if an owned thread's rollout later
        # becomes unresolvable (rotated away, cache evicted), the None it returns must not reach
        # os.path.realpath() -- the same fail-closed handling register_existing/observe give a
        # missing path elsewhere in this class.
        registry = SessionRegistry()
        paths = {}
        driver = CodexDriver(registry, run=lambda *a, **k: FakeResult(0),
                             rollout_path_for=paths.get, started_at=self.RUN_STARTED,
                             sessions_root=self.sessions_root)
        paths['thread-1'] = self.rollout({'timestamp': '2026-09-11T12:00:30.000Z',
                                          'type': 'session_meta'})
        driver.register_existing('thread-1')
        os.remove(paths['thread-1'])  # rotated away: rollout_path_for('thread-1') now finds nothing
        del paths['thread-1']
        paths['thread-2'] = self.rollout({'timestamp': '2026-09-11T12:00:31.000Z',
                                          'type': 'session_meta'})
        driver.register_existing('thread-2')
        self.assertEqual(registry.created, {'codex:thread-1', 'codex:thread-2'})

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
        # as "no rivals found" instead of "could not check". Mock the walk failure directly
        # rather than chmod(0o000): running as root (e.g. in CI containers) ignores directory
        # permission bits entirely, so os.walk would succeed and the test would pass for the
        # wrong reason -- or not raise at all.
        registry = SessionRegistry()
        mine = self.rollout({'timestamp': '2026-09-11T12:00:30.000Z', 'type': 'session_meta'})

        def fake_walk(root, onerror=None, **kwargs):
            onerror(OSError('permission denied (simulated)'))
            return iter(())

        with patch('host_trials.os.walk', side_effect=fake_walk):
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
        registry.mint('codex:thread-1')
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

    def test_submit_raises_rejection_with_the_exit_status_and_stderr_on_a_nonzero_exit(self):
        registry = SessionRegistry()
        registry.mint('codex:thread-1')
        driver = self.driver(registry, None,
                             run=lambda *a, **k: FakeResult(1, stderr='thread expired'))
        with self.assertRaises(SubmissionRejected) as ctx:
            driver.submit('thread-1', MARKER)
        self.assertEqual(ctx.exception.returncode, 1)
        self.assertEqual(ctx.exception.stderr, 'thread expired')

    def test_observe_reads_the_rollout_file(self):
        registry = SessionRegistry()
        registry.mint('codex:thread-1')
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
        registry.mint('codex:thread-1')
        with tempfile.NamedTemporaryFile('w', suffix='.jsonl', delete=False) as handle:
            handle.write('{"timestamp": "2026-09-11T00:00:0\n')
            path = handle.name
        self.addCleanup(os.unlink, path)
        observation = self.driver(registry, path).observe('thread-1', marker=MARKER, submitted_at=0.0)
        self.assertEqual(observation.outcomes, {})
        self.assertFalse(observation.observable)

    def test_observe_is_unobservable_when_the_rollout_holds_an_undated_message(self):
        registry = SessionRegistry()
        registry.mint('codex:thread-1')
        path = self.rollout({'type': 'response_item',
                             'payload': {'type': 'message', 'role': 'assistant',
                                          'content': [{'type': 'output_text', 'text': f'ack {MARKER}'}]}})
        observation = self.driver(registry, path).observe('thread-1', marker=MARKER, submitted_at=0.0)
        self.assertEqual(observation.outcomes, {})
        self.assertFalse(observation.observable)

    def test_observe_preserves_signals_alongside_outcomes_when_the_poll_is_unusable(self):
        # run_trial's polling loop merges outcomes across polls with setdefault(); an unusable
        # poll that dropped `signals` while keeping `outcomes` would let the outcome's timestamp
        # in but permanently lose what established it, since setdefault() never overwrites the
        # None already recorded by an earlier poll.
        registry = SessionRegistry()
        registry.mint('codex:thread-1')
        path = self.rollout(
            {'timestamp': '2026-09-11T00:00:01.000Z', 'type': 'response_item',
             'payload': {'type': 'message', 'role': 'assistant',
                         'content': [{'type': 'output_text', 'text': f'ack {MARKER}'}]}},
            {'type': 'response_item', 'payload': {'type': 'message', 'role': 'assistant',
                                                   'content': [{'type': 'output_text', 'text': 'undated'}]}},
        )
        observation = self.driver(registry, path).observe('thread-1', marker=MARKER, submitted_at=0.0)
        self.assertFalse(observation.observable)
        self.assertIn('ack', observation.outcomes)
        self.assertEqual(observation.signals.get('ack'), SIGNAL_ASSISTANT_MESSAGE)

    def test_observe_is_unobservable_without_a_rollout_path(self):
        registry = SessionRegistry()
        registry.mint('codex:thread-1')
        self.assertEqual(self.driver(registry, None).observe('thread-1', marker=MARKER, submitted_at=0.0),
                          Observation(outcomes={}, observable=False))

    def test_teardown_refuses_and_retains_ownership(self):
        # codex has no `queue --stop`; releasing the registry entry anyway would make the
        # runner believe a live, authenticated host session had been cleaned up when it had
        # not.
        registry = SessionRegistry()
        registry.mint('codex:thread-1')
        with self.assertRaises(TeardownUnsupported):
            self.driver(registry, None).teardown('thread-1')
        registry.require_owned('codex:thread-1')  # still ours: nothing was actually torn down

    def test_teardown_refuses_a_foreign_thread(self):
        with self.assertRaises(ForeignSessionError):
            self.driver(SessionRegistry(), None).teardown('not-mine')

    def test_version_prefers_the_adopted_rollouts_own_recorded_version(self):
        # A rollout's session_meta.cli_version can disagree with the currently installed
        # client's (docs/host-probe-preflight.md, 2026-09-11: 0.154.0 vs 0.153.4 same day); the
        # adopted thread's own record is the one that actually describes its transcript.
        path = self.rollout({'timestamp': '2026-09-11T12:00:30.000Z', 'type': 'session_meta',
                             'payload': {'cli_version': '0.154.0'}})
        driver = self.driver(SessionRegistry(), path,
                              run=lambda *a, **k: FakeResult(0, stdout='codex-cli 0.153.4\n'))
        self.assertEqual(driver.version('thread-1'), '0.154.0')

    def test_version_is_none_rather_than_the_client_when_the_rollout_has_no_cli_version(self):
        # The installed client's version can disagree with the adopted thread's own (see the
        # test above) -- falling back to it here would misattribute the matrix cell to a binary
        # that may not be the one that produced the transcript, so this reports unknown instead
        # of ever calling `codex --version` for this field.
        path = self.rollout({'timestamp': '2026-09-11T12:00:30.000Z', 'type': 'session_meta'})
        driver = self.driver(SessionRegistry(), path)
        self.assertIsNone(driver.version('thread-1'))

    def test_version_is_none_rather_than_the_client_when_there_is_no_rollout_path(self):
        driver = self.driver(SessionRegistry(), None)
        self.assertIsNone(driver.version('thread-1'))

    def test_version_is_none_when_the_rollout_cannot_be_read(self):
        path = self.rollout(lines=['not json\n'])
        os.chmod(path, 0)
        self.addCleanup(os.chmod, path, 0o644)
        driver = self.driver(SessionRegistry(), path)
        self.assertIsNone(driver.version('thread-1'))


class CrossDriverNamespaceTests(unittest.TestCase):
    """A bare session id minted by one driver must not satisfy another's ownership check.

    `claude agents` and `codex queue --thread` both accept caller-chosen names, so nothing stops
    the two hosts from coincidentally sharing one -- a run that adopts a Codex thread named
    `abcd1234` right after a Claude driver mints a session with the same id must not let either
    driver operate on the other's session.
    """

    RUN_STARTED = datetime.datetime(2026, 9, 11, 12, 0, 0, tzinfo=datetime.UTC).timestamp()

    def codex_driver(self, registry):
        return CodexDriver(registry, run=lambda *a, **k: FakeResult(0),
                           rollout_path_for=lambda _id: None, started_at=self.RUN_STARTED,
                           sessions_root=None)

    def test_a_claude_minted_id_does_not_satisfy_a_codex_driver_with_the_same_bare_id(self):
        registry = SessionRegistry()
        registry.mint('claude:collide')
        codex = self.codex_driver(registry)
        with self.assertRaises(ForeignSessionError):
            codex.submit('collide', 'hi')
        with self.assertRaises(ForeignSessionError):
            codex.observe('collide', marker=MARKER, submitted_at=0.0)
        with self.assertRaises(ForeignSessionError):
            codex.teardown('collide')

    def test_a_codex_minted_id_does_not_satisfy_a_claude_driver_with_the_same_bare_id(self):
        registry = SessionRegistry()
        registry.mint('codex:collide')
        claude = ClaudeDriver(registry, run=lambda *a, **k: FakeResult(0, stdout='[]'), cwd='/scratch')
        with self.assertRaises(ForeignSessionError):
            claude.submit('collide', 'hi')
        with self.assertRaises(ForeignSessionError):
            claude.observe('collide', marker=MARKER, submitted_at=0.0)
        with self.assertRaises(ForeignSessionError):
            claude.teardown('collide')


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

    def __init__(self, *, observations=None, accepted=True, submit_error=None,
                 observe_error=None, version_value=None, version_error=None, clock=None):
        self.observations = list(observations or [Observation()])
        self.accepted = accepted
        self.submit_error = submit_error
        self.observe_error = observe_error
        self.version_value = version_value
        self.version_error = version_error
        self.clock = clock
        self.order = []

    def create(self, prompt):
        self.order.append('create')
        return 'sid'

    def version(self, session_id):
        assert session_id == 'sid'
        self.order.append('version')
        if self.version_error is not None:
            raise self.version_error
        return self.version_value

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
        if self.observe_error is not None:
            raise self.observe_error
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

    def test_the_result_carries_which_signal_established_each_outcome(self):
        # docs/host-probes.md, Trial protocol: "Record which signal established each positive
        # result, not just a timestamp" -- a bare TrialRun.outcomes timestamp cannot show this.
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
        # propagated interrupt would discard real evidence instead of finalizing it. The channel
        # stayed readable (observable=True, the default) right up to the interrupt, but that must
        # not read as "definitively absent" for the outcomes the interrupted poll never reached.
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

    def test_the_result_carries_the_driver_version_captured_at_trial_time(self):
        # A matrix cell is version-scoped (docs/host-wake-matrix.md); without this, a cell built
        # against a drifted binary is indistinguishable from one built at the recorded preflight.
        clock = FakeClock()
        driver = FakeDriver(version_value='2.1.268', clock=clock)
        run = self.run_one(driver, clock)
        self.assertEqual(run.version, '2.1.268')

    def test_a_version_read_failure_records_none_rather_than_losing_the_trial(self):
        clock = FakeClock()
        driver = FakeDriver(version_error=RuntimeError('claude --version exited 1'), clock=clock)
        run = self.run_one(driver, clock)
        self.assertIsNone(run.version)
        self.assertEqual(run.session_id, 'sid')  # the rest of the trial still completed normally

    def test_a_version_read_interrupt_stops_the_trial_before_settle_or_submit_run(self):
        # Unlike an ordinary version() failure, a Ctrl-C here is honored as an explicit
        # cancellation: swallowing it and continuing would still let the trial proceed into
        # settle()/submit()/polling (up to 900s), spending real quota and host interaction
        # despite the interrupt. Raising still carries session_id so the caller can find and
        # tear down the already-live session.
        clock = FakeClock()
        driver = FakeDriver(version_error=KeyboardInterrupt(), clock=clock)
        with self.assertRaises(VersionProbeInterrupted) as ctx:
            self.run_one(driver, clock, settle=lambda session_id: driver.order.append('settle'))
        self.assertEqual(ctx.exception.session_id, 'sid')
        self.assertNotIn('settle', driver.order)
        self.assertNotIn('submit', driver.order)

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

    def test_a_settle_failure_carries_the_session_id_rather_than_discarding_it(self):
        # settle() runs after create() already produced a live, owned session; letting its
        # failure propagate bare would discard the only place that session_id is ever surfaced,
        # leaving an authenticated real-HOME session running with no way to find it.
        clock = FakeClock()
        driver = FakeDriver(clock=clock)

        def failing_settle(session_id):
            raise RuntimeError('could not confirm busy state')

        with self.assertRaises(SettleFailed) as caught:
            self.run_one(driver, clock, settle=failing_settle)
        self.assertEqual(caught.exception.session_id, 'sid')
        self.assertIsInstance(caught.exception.original, RuntimeError)
        self.assertNotIn('submit', driver.order)

    def test_a_settle_interrupt_also_carries_the_session_id(self):
        clock = FakeClock()
        driver = FakeDriver(clock=clock)

        def interrupting_settle(session_id):
            raise KeyboardInterrupt

        with self.assertRaises(SettleFailed) as caught:
            self.run_one(driver, clock, settle=interrupting_settle)
        self.assertEqual(caught.exception.session_id, 'sid')
        self.assertIsInstance(caught.exception.original, KeyboardInterrupt)

    def test_an_unenumerated_submission_failure_also_carries_the_session_id(self):
        # submit() runs after create()/settle() already produced a live, owned session; a
        # pre-delivery failure that is not one of the four documented submission signals (a
        # listing call's nonzero exit, here) previously propagated bare, discarding the only
        # place that session_id is ever surfaced.
        clock = FakeClock()
        driver = FakeDriver(submit_error=RuntimeError('claude agents exited 1'), clock=clock)
        with self.assertRaises(SubmissionFailed) as caught:
            self.run_one(driver, clock)
        self.assertEqual(caught.exception.session_id, 'sid')
        self.assertIsInstance(caught.exception.original, RuntimeError)

    def test_an_unenumerated_observation_failure_also_carries_the_session_id(self):
        # observe() runs in the polling loop after submission already succeeded; a mid-poll
        # failure that is not KeyboardInterrupt previously propagated bare, discarding the only
        # place that session_id is ever surfaced and any outcomes already gathered.
        clock = FakeClock()
        driver = FakeDriver(observe_error=RuntimeError('claude agents exited 1'), clock=clock)
        with self.assertRaises(ObservationFailed) as caught:
            self.run_one(driver, clock)
        self.assertEqual(caught.exception.session_id, 'sid')
        self.assertIsInstance(caught.exception.original, RuntimeError)

    def test_rejected_submission_does_not_add_accepted(self):
        clock = FakeClock()
        run = self.run_one(FakeDriver(accepted=False, clock=clock), clock)
        self.assertNotIn('accepted', run.outcomes)
        self.assertIsNone(run.accepted_at)
        self.assertEqual(run.marker, MARKER)  # the submitted token is still evidence, even rejected

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

    def test_submission_rejected_carries_its_diagnostic_rather_than_a_bare_false(self):
        # A `SubmissionRejected` (e.g. from `CodexDriver.submit`) folds into the identical
        # not-accepted shape a plain `False` return produces, except with the returncode/stderr
        # behind it retained via `submission_diagnostic` instead of discarded.
        clock = FakeClock()
        driver = FakeDriver(submit_error=SubmissionRejected(3, 'thread expired'), clock=clock)
        run = self.run_one(driver, clock)
        self.assertNotIn('accepted', run.outcomes)
        self.assertIsNone(run.accepted_at)
        self.assertIn('thread expired', run.submission_diagnostic)
        self.assertIn('3', run.submission_diagnostic)
        self.assertNotIn('observe', driver.order)

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
        self.assertEqual(run.marker, MARKER)  # the submitted token is still evidence, even unsupported
        trial = Trial(submitted=run.submitted_at, state=run.state, outcomes=run.outcomes)
        classified = classify_trial(trial, run.submitted_at + 1000, supported=run.supported)
        self.assertTrue(all(value == 'unsupported' for value in classified.values()))

    def test_uncaptured_submission_is_unobservable_not_a_claim_about_the_host(self):
        clock = FakeClock()
        driver = FakeDriver(submit_error=SubmissionUncaptured('nothing captured here'), clock=clock)
        run = self.run_one(driver, clock)
        self.assertTrue(all(run.supported.values()))  # no claim that the host lacks the path
        self.assertFalse(any(run.observable.values()))
        self.assertFalse(run.interrupted)  # fail-closed like an interrupt, but nobody cancelled
        self.assertEqual(run.marker, MARKER)  # the submitted token is still evidence, even unobservable
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

    def test_a_keyboardinterrupt_during_submit_stops_immediately_without_polling(self):
        # An operator's Ctrl-C while submit() is blocked leaves the same ambiguity as a
        # subprocess.TimeoutExpired from it (the host may already have received the marker), but
        # must honor the explicit cancellation rather than entering the up-to-900s polling loop
        # regardless -- that would need a second Ctrl-C to actually stop the trial. No observe()
        # call should happen at all.
        clock = FakeClock()
        driver = FakeDriver(
            observations=[Observation(outcomes={'visible': 1000.0, 'turn_start': 1001.0,
                                                'ack': 1002.0})],
            submit_error=KeyboardInterrupt(), clock=clock)
        run = self.run_one(driver, clock)
        self.assertIsNone(run.accepted_at)
        self.assertNotIn('accepted', run.outcomes)
        self.assertFalse(run.observable['accepted'])
        self.assertFalse(run.observable['visible'])
        self.assertFalse(run.observable['turn_start'])
        self.assertFalse(run.observable['ack'])
        self.assertEqual(run.outcomes, {})
        self.assertNotIn('observe', driver.order)
        self.assertTrue(run.interrupted)

    def test_an_existing_session_is_adopted_instead_of_created(self):
        clock = FakeClock()
        driver = FakeDriver(clock=clock)
        run = self.run_one(driver, clock, existing_session='sid')
        self.assertEqual(driver.order[:3], ['register_existing', 'version', 'submit'])
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
        # Without a turn-boundary stream, an assistant message after submission cannot be told
        # apart from the tail of the turn already running (`detect_outcomes`' docstring) -- a
        # captured value here is exactly that ambiguity, not evidence, even though the general
        # "positively seen" rule would otherwise let it stand on its own.
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

    def test_a_turn_stream_hosts_readable_busy_timeout_stays_inconclusive(self):
        # A channel that stayed readable for the whole cap but never emitted a boundary is
        # itself evidence the earlier turn never finished -- Trial.result's own `inconclusive`
        # case via `turn_end_observable` -- not an unreadable channel, and must not be forced to
        # `unobservable` here the way a non-turn-stream host's ambiguity is.
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
        # The default no-op settle only ever exercises an idle host; silently accepting it for
        # busy/approval/disconnected/restarted would publish a result for a condition the
        # experiment never established, with no code able to detect the gap afterwards.
        clock = FakeClock()
        for state in ('busy', 'approval', 'disconnected', 'restarted'):
            with self.assertRaises(ValueError):
                self.run_one(FakeDriver(clock=clock), clock, state=state)

    def test_poll_interval_is_validated_before_any_session_exists_or_marker_is_sent(self):
        # A negative or NaN interval previously stayed unnoticed until the first sleep() call
        # after submission -- by which point the real host may already have received the
        # marker with no returned evidence. Zero would pass that same later check (sleep(0)
        # never raises) and spin the polling loop CPU-bound for up to 900s instead.
        for bad in (-1.0, 0.0, float('nan')):
            clock = FakeClock()
            driver = FakeDriver(clock=clock)
            with self.assertRaises(ValueError):
                self.run_one(driver, clock, poll_interval=bad)
            self.assertEqual(driver.order, [])

    def test_marker_is_validated_before_any_session_exists_or_is_sent(self):
        # An empty or hand-typed marker can appear in a transcript for reasons unrelated to this
        # trial, silently promoting an unrelated message to `ack`; only marker_token()'s own
        # high-entropy shape is accepted.
        for bad in ('', 'hello', MARKER.upper(), 'PARLEY-PROBE-tooshort', MARKER + '\n'):
            # `re.match` treats `$` as matching immediately before a trailing newline, so a
            # marker with one appended still satisfied the old `.match()` check; `.fullmatch()`
            # requires the match to span the entire string.
            clock = FakeClock()
            driver = FakeDriver(clock=clock)
            with self.assertRaises(ValueError):
                run_trial(driver, prompt='hi', marker=bad, clock=clock.time,
                          monotonic=clock.monotonic, sleep=clock.sleep)
            self.assertEqual(driver.order, [])

    def test_omitting_the_marker_generates_a_fresh_one_each_call(self):
        # A caller-supplied marker was only ever checked for shape, so a fixed or reused literal
        # passed the check and let a later trial's delayed echo of an earlier trial's marker
        # count as its own acknowledgement. Omitting the argument is the path that actually
        # guarantees a fresh, unique token per trial.
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
        # assertRegex is re.search semantics: `$` matches before a trailing newline, so it would
        # not catch a generated token run_trial's own `.fullmatch()` check rejects (the same gap
        # test_marker_is_validated_before_any_session_exists_or_is_sent documents).
        self.assertIsNotNone(MARKER_PATTERN.fullmatch(marker_token()))


if __name__ == '__main__':
    unittest.main()

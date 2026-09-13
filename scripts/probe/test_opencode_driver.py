"""Controlled OpenCode server and driver fixtures; no installed host CLI is launched."""

import io
import json
import os
import subprocess
import threading
import unittest
import unittest.mock
import urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer

import host_trials
from host_trials import (
    ForeignSessionError,
    OpenCodeDriver,
    SessionRegistry,
    SubmissionRejected,
    SubmissionUncaptured,
    sweep,
)
from test_host_trials import (
    MARKER,
    DriverTestCase,
    FakeResult,
    FakeRun,
    opencode_export,
    opencode_message,
)


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
    def test_interrupted_creation_reports_exact_manual_discovery_locator(self):
        original = KeyboardInterrupt()
        run = FakeRun([(['opencode', 'run'], original)])
        driver = self.driver(run)
        with self.assertRaises(KeyboardInterrupt) as caught:
            driver.create('hello')
        self.assertIs(caught.exception, original)
        argv = run.calls[0][0]
        title = argv[argv.index('--title') + 1]
        notes = '\n'.join(getattr(original, '__notes__', []))
        self.assertIn(title, notes)
        self.assertIn(self.cwd, notes)
        self.assertIn('manual investigation', notes)
        self.assertEqual(driver.owned(), set())
        self.assertEqual(driver.strays, set())
        self.assertEqual(len(run.calls), 1)

    def test_export_directory_must_match_the_private_probe_cwd(self):
        for directory in (None, '', '/foreign'):
            with self.subTest(directory=directory):
                self.registry = SessionRegistry()
                document = json.loads(opencode_export([
                    opencode_message('user', [{'type': 'text', 'text': MARKER}], 1000),
                    opencode_message('assistant', [{'type': 'text', 'text': MARKER}], 2000,
                                     providerID='synthetic', modelID='model')], directory=self.cwd))
                if directory is None:
                    del document['info']['directory']
                else:
                    document['info']['directory'] = directory
                run = FakeRun([(['opencode', 'run'], self.run_output()),
                               (['opencode', '--pure', 'export'], FakeResult(0, json.dumps(document)))])
                driver = self.driver(run)
                driver.create('hello')
                observation = driver.observe('ses_1', marker=MARKER, submitted_at=0)
                self.assertFalse(observation.observable)
                self.assertEqual(observation.outcomes, {})
                self.assertIsNone(observation.model)
                self.assertIsNone(driver.version('ses_1'))
    def test_foreign_or_missing_export_bindings_never_supply_outcomes_model_or_version(self):
        for target in ('top', 'message', 'part-session', 'part-message'):
            for missing in (False, True):
                with self.subTest(target=target, missing=missing):
                    self.registry = SessionRegistry()
                    document = json.loads(opencode_export([
                        opencode_message('assistant', [{'type': 'text', 'text': MARKER}],
                                         1_757_754_001_000, providerID='foreign', modelID='model')],
                                                          directory=self.cwd))
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
        for suffix in ('{"type":', '[1]', 'null', '"text"', '{"type":"text"}',
                       '{"type":"text","sessionID":""}', '{"type":"text","sessionID":null}',
                       '{"type":"text","sessionID":7}'):
            with self.subTest(suffix=suffix):
                self.registry = SessionRegistry()
                run = FakeRun([(['opencode', 'run'],
                                FakeResult(0, self.run_output().stdout + '\n' + suffix))])
                driver = self.driver(run)
                with self.assertRaisesRegex(RuntimeError, 'malformed'):
                    driver.create('hello')
                # The one proven creation ID remains available for cleanup after failure.
                self.assertEqual(driver.owned(), {'ses_1'})

    def test_uncaptured_event_discriminators_cannot_accept_creation_or_attach(self):
        for kind in (None, 7, [], {}, '', ' ', 'future-event'):
            for operation in ('create', 'attach'):
                with self.subTest(kind=kind, operation=operation):
                    self.registry = SessionRegistry()
                    self.answering = False
                    event = {'sessionID': 'ses_1', 'error': {'name': 'synthetic-failure'}}
                    if kind is not None:
                        event['type'] = kind
                    result = FakeResult(0, json.dumps(event))
                    if operation == 'create':
                        driver = self.driver(FakeRun([(['opencode', 'run'], result)]))
                        with self.assertRaisesRegex(RuntimeError, 'malformed'):
                            driver.create('hello')
                    else:
                        run = FakeRun([(['opencode', 'run', '--pure', '--format', 'json', '--dir'],
                                        self.run_output()),
                                       (['opencode', 'run', '--pure', '--format', 'json', '--attach'], result)])
                        driver = self.driver(run, port=4096)
                        driver.create('hello')
                        driver.serve()
                        with self.assertRaisesRegex(SubmissionUncaptured, 'malformed'):
                            driver.submit('ses_1', 'msg')

    def test_success_events_require_parts_on_creation_and_attach(self):
        for kind in ('step_start', 'text', 'step_finish'):
            for returncode in (0, 1, 'timeout'):
                for operation in ('create', 'attach'):
                    with self.subTest(kind=kind, returncode=returncode, operation=operation):
                        self.registry = SessionRegistry()
                        self.answering = False
                        output = json.dumps({'type': kind, 'sessionID': 'ses_1'})
                        result = (subprocess.TimeoutExpired(['opencode'], 60, output=output)
                                  if returncode == 'timeout' else FakeResult(returncode, output))
                        if operation == 'create':
                            driver = self.driver(FakeRun([(['opencode', 'run'], result)]))
                            with self.assertRaisesRegex(RuntimeError, 'malformed'):
                                driver.create('hello')
                        else:
                            run = FakeRun([(['opencode', 'run', '--pure', '--format', 'json', '--dir'],
                                            self.run_output()),
                                           (['opencode', 'run', '--pure', '--format', 'json', '--attach'], result)])
                            driver = self.driver(run, port=4096)
                            driver.create('hello')
                            driver.serve()
                            with self.assertRaisesRegex(SubmissionUncaptured, 'malformed'):
                                driver.submit('ses_1', 'msg')
                        self.assertEqual(driver.owned(), {'ses_1'})

    def test_malformed_attach_stream_is_uncaptured_even_with_a_matching_id(self):
        for suffix in ('{"type":', '[1]', 'null', '"text"', '{"type":"text"}',
                       '{"type":"text","sessionID":""}', '{"type":"text","sessionID":null}',
                       '{"type":"text","sessionID":7}'):
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
        events = [{'type': 'step_start', 'timestamp': 1, 'sessionID': session_id,
                   'part': {'type': 'step-start', 'id': 'part-1', 'messageID': 'message-1',
                            'sessionID': session_id}}]
        if error:
            events.append({'type': 'error', 'sessionID': session_id, 'error': error})
        return FakeResult(0, '\n'.join(json.dumps(e) for e in events) + '\n')

    def test_conflicting_nested_creation_id_grants_no_ownership(self):
        event = json.loads(self.run_output().stdout)
        event['part']['sessionID'] = 'ses_foreign'
        output = json.dumps(event)
        for result in (FakeResult(0, output), FakeResult(1, output),
                       subprocess.TimeoutExpired(['opencode'], 180, output=output)):
            with self.subTest(result=type(result).__name__):
                self.registry = SessionRegistry()
                driver = self.driver(FakeRun([(['opencode', 'run'], result)]))
                with self.assertRaisesRegex(RuntimeError, 'ambiguous'):
                    driver.create('hello')
                self.assertEqual(driver.owned(), set())
                self.assertEqual(driver.strays, {'ses_1', 'ses_foreign'})

    def test_conflicting_nested_attach_id_is_never_adopted(self):
        event = json.loads(self.run_output().stdout)
        event['part']['sessionID'] = 'ses_foreign'
        output = json.dumps(event)
        for result in (FakeResult(0, output), FakeResult(1, output),
                       subprocess.TimeoutExpired(['opencode'], 60, output=output)):
            with self.subTest(result=type(result).__name__):
                self.registry = SessionRegistry()
                self.answering = False
                run = FakeRun([(['opencode', 'run', '--pure', '--format', 'json', '--dir'], self.run_output()),
                               (['opencode', 'run', '--pure', '--format', 'json', '--attach'], result)])
                driver = self.driver(run, port=4096)
                driver.create('hello')
                driver.serve()
                with self.assertRaisesRegex(SubmissionUncaptured, 'unexpected'):
                    driver.submit('ses_1', 'msg')
                self.assertEqual(driver.owned(), {'ses_1'})
                self.assertEqual(driver.strays, {'ses_foreign'})
                with self.assertRaises(ForeignSessionError):
                    driver.teardown('ses_foreign')

    def test_uncaptured_opencode_models_are_rejected_before_host_calls(self):
        for model in (None, '', 'synthetic-provider/uncaptured-model'):
            with self.subTest(model=model):
                run = FakeRun([])
                with self.assertRaisesRegex(ValueError, 'uncaptured'):
                    self.driver(run, model=model)
                self.assertEqual(run.calls, [])
                self.assertEqual(self.servers, [])

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
        with unittest.mock.patch('urllib.request.OpenerDirector.open', side_effect=error):
            with self.assertRaisesRegex(RuntimeError, 'already answers'):
                driver.serve(timeout=0)
        self.assertEqual(self.servers, [])

    def test_loopback_readiness_bypasses_inherited_http_proxies(self):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.server.paths.append(self.path)
                self.send_response(self.server.status)
                self.end_headers()

            def log_message(self, *args):
                pass

        servers = []
        threads = []
        try:
            for status in (200, 418):
                server = HTTPServer(('127.0.0.1', 0), Handler)
                servers.append(server)
                server.paths = []
                server.status = status
                thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.01},
                                          daemon=True)
                thread.start()
                threads.append(thread)
            target, proxy = servers
            proxy_url = f'http://127.0.0.1:{proxy.server_port}'
            with unittest.mock.patch.dict(os.environ, {'HTTP_PROXY': proxy_url, 'http_proxy': proxy_url,
                                                       'NO_PROXY': '', 'no_proxy': ''}), \
                    unittest.mock.patch('urllib.request._opener', None):
                self.assertEqual(host_trials.http_status(f'http://127.0.0.1:{target.server_port}/session'), 200)
            self.assertEqual(target.paths, ['/session'])
            self.assertEqual(proxy.paths, [])
        finally:
            for server in servers[:len(threads)]:
                server.shutdown()
            for thread in threads:
                thread.join(timeout=2)
            for server in servers:
                server.server_close()

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
        error_event = json.dumps({'type': 'error', 'sessionID': 'ses_1', 'error': {'name': 'UnknownModel'}})
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
            opencode_message('assistant', [{'type': 'text', 'text': MARKER}], 1_757_754_002_000)],
                                 directory=self.cwd)
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

    def test_losing_the_submission_server_invalidates_only_missing_outcomes(self):
        for loss in ('exit', 'remove', 'replace'):
            with self.subTest(loss=loss):
                self.registry = SessionRegistry()
                self.answering = False
                export = opencode_export([opencode_message('user', [{'type': 'text', 'text': MARKER}], 1000)],
                                         directory=self.cwd)
                run = FakeRun([(['opencode', 'run'], self.run_output()),
                               (['opencode', '--pure', 'export'], FakeResult(0, export))])
                driver = self.driver(run)
                driver.create('hello')
                server = driver.serve()
                self.assertIs(driver.submit('ses_1', 'msg'), True)
                self.assertTrue(driver.observe('ses_1', marker=MARKER, submitted_at=0).observable)
                if loss == 'exit':
                    server.process.returncode = 1
                else:
                    driver.server = None if loss == 'remove' else object()
                observation = driver.observe('ses_1', marker=MARKER, submitted_at=0)
                self.assertIn('visible', observation.outcomes)
                self.assertFalse(observation.observable)

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


if __name__ == "__main__":
    unittest.main()

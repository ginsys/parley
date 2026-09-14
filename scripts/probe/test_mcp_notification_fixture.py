"""Controlled protocol checks; no installed host CLI is launched."""

import json
import os
import select
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class FixtureTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.control = root / 'control.jsonl'
        self.control.touch()
        self.journal = root / 'journal'
        self.process = subprocess.Popen(
            [sys.executable, str(Path(__file__).with_name('mcp_notification_fixture.py')),
             '--journal', str(self.journal), '--control', str(self.control)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
        self.addCleanup(self.close)
        self.journal = Path(f'{self.journal}.{self.process.pid}.jsonl')

    def close(self):
        self.process.stdin.close()
        try:
            self.process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=2)
        self.process.stdout.close()
        self.process.stderr.close()

    def send(self, **message):
        self.process.stdin.write((json.dumps(dict(jsonrpc='2.0', **message)) + '\n').encode())

    def receive(self, timeout=2):
        line = b''
        while not line.endswith(b'\n'):
            ready, _, _ = select.select([self.process.stdout], [], [], timeout)
            self.assertTrue(ready, 'no protocol response')
            data = os.read(self.process.stdout.fileno(), 1)
            self.assertTrue(data, 'server exited before its response')
            line += data
        return json.loads(line)

    def initialize(self):
        self.send(id=1, method='initialize', params=dict(protocolVersion='2025-11-25',
                  capabilities={}, clientInfo=dict(name='controlled-test', version='1')))
        response = self.receive()
        self.assertEqual(response['result']['protocolVersion'], '2025-06-18')
        self.assertEqual(response['result']['capabilities'], dict(logging={}, tools={}))
        self.send(method='notifications/initialized')
        self.send(id=2, method='ping')
        self.assertEqual(self.receive(), dict(jsonrpc='2.0', id=2, result={}))

    def notify(self):
        with self.control.open('a') as handle:
            handle.write(json.dumps(dict(message='synthetic marker')) + '\n')

    def test_notification_is_delivered_during_a_pending_hold_and_eof_stops_server(self):
        self.initialize()
        self.send(id=3, method='tools/list')
        self.assertEqual(self.receive()['result']['tools'][0]['name'], 'hold')
        self.send(id=4, method='tools/call', params=dict(name='hold', arguments={}))
        self.send(id=5, method='ping')
        self.assertEqual(self.receive()['id'], 5)  # the hold does not block the transport
        self.notify()
        message = self.receive()
        self.assertEqual(message['method'], 'notifications/message')
        self.assertEqual(message['params'], dict(level='info', logger='parley-probe',
                                                 data='synthetic marker'))
        self.process.stdin.close()
        self.assertEqual(self.process.wait(timeout=2), 0)
        records = [json.loads(line) for line in self.journal.read_text().splitlines()]
        self.assertIn('hold_started', [entry['kind'] for entry in records])
        self.assertEqual(records[-1]['kind'], 'stdin_eof')

    def test_notification_honors_client_log_threshold(self):
        self.initialize()
        self.send(id=3, method='logging/setLevel', params=dict(level='warning'))
        self.assertEqual(self.receive()['result'], {})
        self.notify()
        ready, _, _ = select.select([self.process.stdout], [], [], 0.5)
        self.assertFalse(ready)
        self.send(id=4, method='logging/setLevel', params=dict(level='not-a-level'))
        self.assertEqual(self.receive()['error']['code'], -32602)
        records = [json.loads(line) for line in self.journal.read_text().splitlines()]
        self.assertIn('notification_refused', [entry['kind'] for entry in records])

    def test_disconnect_closes_transport_without_replaying_later_controls(self):
        self.initialize()
        with self.control.open('a') as handle:
            handle.write(json.dumps(dict(disconnect=True)) + '\n')
            handle.write(json.dumps(dict(message='must not be written')) + '\n')
        self.assertEqual(self.process.wait(timeout=2), 0)
        self.assertEqual(self.process.stdout.read(), b'')
        records = [json.loads(line) for line in self.journal.read_text().splitlines()]
        self.assertEqual(records[-1]['kind'], 'control_disconnect')


if __name__ == '__main__':
    unittest.main()

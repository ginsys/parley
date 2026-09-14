"""Synthetic stdio MCP 2025-06-18 fixture for the plain-MCP investigation.

Only protocol traffic goes to stdout. A private append-only control file supplies notifications;
the journal records actual negotiation and writes. No host acceptance is inferred from a write.
The hold tool sleeps in this event loop for 30 seconds without touching files or running commands.
Sources: modelcontextprotocol.io/specification/2025-06-18/{basic/lifecycle,basic/transports,
server/tools,server/utilities/logging}. This is a probe fixture, not a Parley adapter.
"""

import argparse
import json
import os
import select
import sys
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--journal', required=True)
    parser.add_argument('--control', required=True)
    args = parser.parse_args()
    levels = ['debug', 'info', 'notice', 'warning', 'error', 'critical', 'alert', 'emergency']
    threshold = 0
    initialized = False
    pending = b''
    controls = ''
    holds = []
    with open(f'{args.journal}.{os.getpid()}.jsonl', 'x') as journal, open(args.control) as control:
        # Plain notifications have no replay queue: a new transport ignores earlier controls.
        control.seek(0, os.SEEK_END)
        def record(kind, **data):
            journal.write(json.dumps(dict(kind=kind, utc=time.time(), monotonic=time.monotonic(),
                                          **data)) + '\n')
            journal.flush()

        def send(message):
            sys.stdout.write(json.dumps(dict(jsonrpc='2.0', **message)) + '\n')
            sys.stdout.flush()
            record('sent', message=message)

        record('started', pid=os.getpid(), protocol='2025-06-18')
        while True:
            readable, _, _ = select.select([sys.stdin], [], [], 0.1)
            if readable:
                data = os.read(sys.stdin.fileno(), 4096)
                if not data:
                    record('stdin_eof')
                    return
                pending += data
                if len(pending) > 65536:
                    raise ValueError('oversized protocol input')
                while b'\n' in pending:
                    line, pending = pending.split(b'\n', 1)
                    request = json.loads(line)
                    record('received', message=request)
                    method = request.get('method')
                    request_id = request.get('id')
                    params = request.get('params', {})
                    if method == 'initialize':
                        send(dict(id=request_id, result=dict(protocolVersion='2025-06-18',
                                  capabilities=dict(logging={}, tools={}),
                                  serverInfo=dict(name='parley-notification-probe', version='0.1.0'))))
                    elif method == 'notifications/initialized':
                        initialized = True
                        record('initialized')
                    elif method == 'ping':
                        send(dict(id=request_id, result={}))
                    elif not initialized:
                        if request_id is not None:
                            send(dict(id=request_id, error=dict(code=-32000, message='Not initialized')))
                    elif method == 'logging/setLevel':
                        level = params.get('level')
                        if level in levels:
                            threshold = levels.index(level)
                            send(dict(id=request_id, result={}))
                        else:
                            send(dict(id=request_id, error=dict(code=-32602, message='Invalid level')))
                    elif method == 'tools/list':
                        send(dict(id=request_id, result=dict(tools=[dict(
                            name='hold', description='Wait 30 seconds; no file, network or command access.',
                            inputSchema=dict(type='object', properties={}, additionalProperties=False))])))
                    elif method == 'tools/call' and params.get('name') == 'hold' and not params.get('arguments'):
                        holds.append((time.monotonic() + 30, request_id))
                        record('hold_started', request_id=request_id)
                    elif method == 'notifications/cancelled':
                        holds = [(due, key) for due, key in holds if key != params.get('requestId')]
                    elif request_id is not None:
                        send(dict(id=request_id, error=dict(code=-32601, message='Unsupported request')))
            controls += control.read()
            if len(controls) > 65536:
                raise ValueError('oversized control input')
            while '\n' in controls:
                line, controls = controls.split('\n', 1)
                command = json.loads(line)
                if command == {'disconnect': True}:
                    record('control_disconnect')
                    return
                message = command.get('message')
                if set(command) != {'message'} or not isinstance(message, str) or len(message) > 4096:
                    raise ValueError('invalid synthetic control message')
                if not initialized or threshold > levels.index('info'):
                    record('notification_refused', initialized=initialized, threshold=levels[threshold])
                else:
                    send(dict(method='notifications/message',
                              params=dict(level='info', logger='parley-probe', data=message)))
            for due, request_id in list(holds):
                if time.monotonic() >= due:
                    send(dict(id=request_id, result=dict(content=[dict(type='text', text='HOLD COMPLETE')],
                                                       isError=False)))
                    holds.remove((due, request_id))
                    record('hold_completed', request_id=request_id)


if __name__ == '__main__':
    main()

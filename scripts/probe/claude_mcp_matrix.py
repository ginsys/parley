"""Explicit live Claude 2.1.270 plain-MCP investigation; never an ordinary test.

Uses the native attach subcommand inside the installed launcher's memory limits. The local
interactive launcher prepends flags that turn attach into a different conversation.
"""

import argparse
import dataclasses
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from codex_matrix import BUSY_PROMPT
from host_trials import (
    ClaudeDriver,
    PtyClient,
    SessionRegistry,
    classify_trial,
    claude_transcript_events,
    detect_outcomes,
    record_time,
    run_trial_with_cleanup,
)
from terminal_capture import terminal_screen
from wake_probe import Trial, aggregate

HOLD_PROMPT = ('Call the parleyprobe hold tool exactly once with empty arguments. '
               'Do not call any other tool. Wait for it to finish, then reply HOLD COMPLETE.')


def verify_binaries(run, native):
    for executable in ('claude', str(native)):
        version = run([executable, '--version'], capture_output=True, text=True, timeout=15)
        if version.returncode or version.stdout.strip() != '2.1.270 (Claude Code)':
            raise RuntimeError(f'Claude version changed for {executable}; capture compatibility first')


def records_in(path):
    # A live writer may be between write and newline; the next poll includes that record.
    return [json.loads(line) for line in path.read_text().splitlines(keepends=True)
            if line.endswith('\n')]


def busy_completion(records, user_uuid, session_uuid, cwd):
    descendants = {user_uuid}
    for record in records:
        if record.get('parentUuid') not in descendants:
            continue
        if record.get('sessionId') != session_uuid or record.get('cwd') != cwd:
            raise RuntimeError('busy completion ancestry changed session binding')
        descendants.add(record.get('uuid'))
        if record.get('type') == 'system' and record.get('subtype') == 'turn_duration':
            stamp = record_time(record)
            if stamp is None:
                raise RuntimeError('busy completion has no usable time')
            return stamp
    return None


def approval_pending(records, screen):
    # Claude does not persist this tool_use before approval. Use the captured menu and the
    # exact preceding synthetic request; the fixture independently detects any execution.
    tail = ''.join(screen[-7000:].split())
    if not all(text in tail for text in ('Doyouwanttoproceed?', '1.Yes', '3.No',
                                         'parleyprobe—HoldTool:(MCP)')):
        return None
    requests = [record.get('uuid') for record in records if record.get('type') == 'user'
                and record.get('message', {}).get('content') == HOLD_PROMPT]
    return requests[0] if len(requests) == 1 else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output-directory', required=True)
    parser.add_argument('--state', choices=('idle', 'busy', 'approval', 'disconnected', 'restarted'), required=True)
    parser.add_argument('--trials', type=int, choices=(1, 3), default=3)
    args = parser.parse_args()
    output = Path(args.output_directory).resolve()
    output.mkdir(mode=0o700)
    results = []
    for index in range(1, args.trials + 1):
        trial_dir = output / f'trial-{index}'
        trial_dir.mkdir(mode=0o700)
        control = trial_dir / 'control.jsonl'
        control.touch(mode=0o600)
        config_path = trial_dir / 'mcp-config.json'
        config_path.write_text(json.dumps(dict(mcpServers=dict(parleyprobe=dict(
            type='stdio', command=sys.executable,
            args=[str(Path(__file__).with_name('mcp_notification_fixture.py')),
                  '--journal', str(trial_dir / 'server'), '--control', str(control)])))))
        with (trial_dir / 'journal.jsonl').open('x') as journal:
            clients = []
            precondition = {}

            def record(kind, **data):
                journal.write(json.dumps(dict(kind=kind, utc=time.time(),
                                              monotonic=time.monotonic(), **data)) + '\n')
                journal.flush()

            def run(argv, **kwargs):
                if argv[:2] == ['claude', '--bg'] and '--resume' not in argv:
                    argv = argv[:-1] + ['--mcp-config', str(config_path),
                                       '--strict-mcp-config', argv[-1]]
                    if args.state == 'approval':
                        # This is the captured configuration that displayed the approval menu.
                        argv = argv[:-1] + ['--allowedTools', 'mcp__parleyprobe__hold', '--', argv[-1]]
                record('command_started', argv=argv, cwd=kwargs.get('cwd'))
                response = subprocess.run(argv, **kwargs)
                record('command_finished', argv=argv, returncode=response.returncode,
                       stdout=response.stdout, stderr=response.stderr)
                return response

            def pty(argv, **kwargs):
                argv = ['systemd-run', '--user', '--quiet', '--collect', '--scope',
                        '--setenv=CLAUDE_MEM_SCOPE=1', '-p', 'MemoryHigh=6G',
                        '-p', 'MemoryMax=12G', '-p', 'MemorySwapMax=4G', '--',
                        str(native), *argv[1:]]
                record('pty_started', argv=argv, cwd=kwargs.get('cwd'))
                client = PtyClient(argv, **kwargs)
                clients.append(client)
                return client

            def server_records():
                return [entry for path in trial_dir.glob('server.*.jsonl')
                        for entry in records_in(path)]

            class RecordedDriver(ClaudeDriver):
                def transcript(self, session_id):
                    result = self._read_transcript(session_id,
                                                  lambda lines: [json.loads(line) for line in lines])
                    if result is None:
                        raise RuntimeError('no bound transcript for precondition')
                    return result

                def require_approval(self, session_id):
                    client = self.live_client_for(session_id)
                    if (client is None or approval_pending(self.transcript(session_id),
                                                          terminal_screen(client)) != precondition['request_uuid']
                            or any(entry['kind'] == 'hold_started' for entry in server_records())):
                        raise RuntimeError('approval precondition no longer holds')

                def submit(self, session_id, message):
                    self.require_owned(session_id)
                    if args.state == 'approval':
                        self.require_approval(session_id)
                    record('submission_started', session_id=session_id, message=message)
                    with control.open('a') as handle:
                        handle.write(json.dumps(dict(message=message)) + '\n')
                    if args.state == 'restarted':
                        response = self._submit_resume(session_id, self.status(session_id),
                                                       'Reply with exactly PONG. Do not call tools or change files.')
                        record('explicit_restart', result=response)
                        self._settle_creation_turn(session_id)
                        attach_bound(session_id)
                        if sum(entry['kind'] == 'initialized' for entry in server_records()) != 2:
                            raise RuntimeError('resumed fixture initialization not observed exactly once')
                    if args.state in ('disconnected', 'restarted'):
                        record('notification_unwritten', reason='control written while transport absent')
                        return None
                    deadline = time.monotonic() + 5
                    while True:
                        sent = [entry for entry in server_records() if entry['kind'] == 'sent'
                                and entry['message'].get('method') == 'notifications/message'
                                and entry['message'].get('params', {}).get('data') == message]
                        if len(sent) == 1:
                            record('notification_written', evidence=sent[0])
                            return None  # A server write is not host acceptance.
                        if len(sent) > 1 or time.monotonic() >= deadline:
                            raise RuntimeError('one actual notification write was not established')
                        time.sleep(0.1)

                def observe(self, session_id, **kwargs):
                    if args.state == 'approval':
                        self.require_approval(session_id)
                    observation = super().observe(session_id, **kwargs)
                    if args.state == 'approval':
                        # A pending tool call can be persisted after the menu appears. It is
                        # still the blocked original turn, not a new turn caused by this marker.
                        observation.outcomes.pop('turn_start', None)
                        observation.signals.pop('turn_start', None)
                        observation.model = None
                    if args.state in ('disconnected', 'restarted'):
                        expected = 1 if args.state == 'disconnected' else 2
                        entries = server_records()
                        if sum(entry['kind'] == 'initialized' for entry in entries) != expected:
                            raise RuntimeError('unexpected transport reconnection')
                        if any(entry['kind'] == 'sent' and entry['message'].get('method') == 'notifications/message'
                               for entry in entries):
                            raise RuntimeError('notification unexpectedly replayed after disconnect')
                    entry = self.status(session_id)
                    if entry is None or entry.get('pid') != precondition['pid']:
                        observation.observable = False
                    if args.state == 'busy':
                        records = self.transcript(session_id)
                        ended = busy_completion(records, precondition['user_uuid'],
                                                self.sessions[session_id], self.cwd)
                        observation.turn_end = ended
                        if ended is not None and ended <= kwargs['submitted_at']:
                            raise RuntimeError('busy turn completed before submission')
                        # The first assistant output can still belong to the existing turn.
                        observation.outcomes.pop('turn_start', None)
                        observation.signals.pop('turn_start', None)
                        observation.model = None
                        if ended is not None:
                            events, unusable = claude_transcript_events([json.dumps(row) for row in records])
                            following = detect_outcomes(events, kwargs['marker'], submitted_at=ended)
                            if 'turn_start' in following.outcomes:
                                observation.outcomes['turn_start'] = following.outcomes['turn_start']
                                observation.signals['turn_start'] = following.signals['turn_start']
                                observation.model = following.model
                            if unusable:
                                observation.observable = False
                    record('observation', observation=dataclasses.asdict(observation), state=entry)
                    return observation

                def teardown(self, session_id):
                    try:
                        path = self.transcript_path_for(self.sessions[session_id])
                        if path:
                            (trial_dir / 'transcript.jsonl').write_bytes(Path(path).read_bytes())
                        for number, client in enumerate(clients):
                            (trial_dir / f'pty-{number}.txt').write_text(client.text_since(0))
                            with client.lock:
                                (trial_dir / f'pty-{number}.raw').write_bytes(bytes(client.window))
                    finally:
                        super().teardown(session_id)

            native = Path.home() / '.local/bin/claude'
            verify_binaries(run, native)
            cwd = tempfile.mkdtemp(prefix='parley-probe-', dir='/tmp')
            record('probe_directory', cwd=cwd, retained_for_hook_evidence=True)
            driver = RecordedDriver(SessionRegistry(), cwd=cwd, run=run, pty=pty)

            def attach_bound(session_id):
                client = driver.attach(session_id)
                entry = driver.status(session_id)
                screen = ''.join(client.text_since(0).split())
                if (entry is None or f'pid:{entry["pid"]}' not in screen
                        or f'session:{entry["sessionId"][:6]}' not in screen):
                    raise RuntimeError('attach screen did not identify the created background process')
                precondition['pid'] = entry['pid']
                driver.submission_clients[session_id] = client
                return client

            def settle(session_id):
                client = attach_bound(session_id)
                if not any(entry['kind'] == 'initialized' for entry in server_records()):
                    raise RuntimeError('MCP initialization missing')
                if args.state == 'disconnected':
                    with control.open('a') as handle:
                        handle.write(json.dumps(dict(disconnect=True)) + '\n')
                    deadline = time.monotonic() + 5
                    while not any(entry['kind'] == 'control_disconnect' for entry in server_records()):
                        if time.monotonic() >= deadline:
                            raise RuntimeError('transport did not close')
                        time.sleep(0.1)
                elif args.state == 'restarted':
                    if driver.close_clients():
                        raise RuntimeError('initial client failed to close')
                    driver.stop(session_id)
                    record('stopped_before_notification', state=driver.status(session_id))
                elif args.state != 'idle':
                    before = time.time()
                    prompt = BUSY_PROMPT if args.state == 'busy' else HOLD_PROMPT
                    record('state_prompt', prompt=prompt)
                    client.type_line(prompt)
                    deadline = time.monotonic() + 180
                    while True:
                        records = driver.transcript(session_id)
                        if args.state == 'approval':
                            request_uuid = approval_pending(records, terminal_screen(client))
                            if request_uuid:
                                precondition['request_uuid'] = request_uuid
                                break
                        else:
                            users = [entry for entry in records if entry.get('type') == 'user'
                                     and entry.get('message', {}).get('content') == prompt
                                     and record_time(entry) is not None and record_time(entry) >= before]
                            state = driver.status(session_id)
                            if len(users) == 1 and state and state['state'] == 'working':
                                precondition['user_uuid'] = users[0]['uuid']
                                break
                        if client.eof or time.monotonic() >= deadline:
                            raise RuntimeError('requested state not established')
                        time.sleep(0.1)
                record('precondition', state=args.state, evidence=precondition, screen=client.text_since(0))
                if args.state == 'approval':
                    record('approval_screen', screen=terminal_screen(client))

            try:
                trial = run_trial_with_cleanup(driver, prompt='Reply with exactly PONG. Do not call tools or change files.',
                                               state=args.state, settle=settle, poll_interval=1.0)
                record('trial', trial=dataclasses.asdict(trial))
                if trial.interrupted or trial.clock_step is not None:
                    raise RuntimeError('invalid or interrupted attempt retained separately')
                result = classify_trial(Trial(submitted=trial.submitted_at, state=trial.state,
                                              outcomes=trial.outcomes, turn_end=trial.turn_end,
                                              turn_end_observable=trial.turn_end_observable),
                                        time.time(), supported=trial.supported, observable=trial.observable)
                record('classified', outcomes=result)
                results.append(result)
            except BaseException as error:
                record('attempt_failed', error_type=type(error).__name__, detail=str(error))
                raise
            finally:
                record('ownership_after_cleanup', owned=sorted(driver.owned()), clients=len(driver.clients))
            if driver.owned() or driver.clients:
                raise RuntimeError('cleanup incomplete')
    if len(results) == 3:
        combined = {name: aggregate([result[name] for result in results]) for name in results[0]}
        (output / 'aggregate.json').write_text(json.dumps(dict(state=args.state, trials=results,
                                                             aggregate=combined), indent=2) + '\n')
    else:
        (output / 'single-trial.json').write_text(json.dumps(dict(state=args.state, trials=results),
                                                            indent=2) + '\n')


if __name__ == '__main__':
    main()

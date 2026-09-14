"""Explicit OpenCode 1.18.30 investigation over captured authenticated run/attach paths."""

import argparse
import base64
import dataclasses
import json
import math
import os
import secrets
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

from host_trials import (
    CLOCK_DRIFT_TOLERANCE,
    OpenCodeDriver,
    PtyClient,
    SessionRegistry,
    classify_trial,
    detect_outcomes,
    http_status,
    opencode_export_events,
    run_trial_with_cleanup,
)
from terminal_capture import terminal_screen
from wake_probe import WINDOWS, Trial, aggregate

BUSY_PROMPT = ('Write the integers from one through five hundred in English words, '
               'one per line, followed by HOLD COMPLETE. Do not call tools or change files.')
APPROVAL_COMMAND = 'printf PARLEY_APPROVAL_PREFLIGHT'
APPROVAL_PROMPT = ('Use the bash tool to run exactly: printf PARLEY_APPROVAL_PREFLIGHT. '
                   'Request permission and wait. Do not use other tools or change files.')


def sample_clock():
    return time.monotonic(), time.time(), time.monotonic()


def checked_clock(anchor):
    current = sample_clock()
    wall_delta = current[1] - anchor[1]
    drift_low = wall_delta - (current[2] - anchor[0])
    drift_high = wall_delta - (current[0] - anchor[2])
    if drift_low > CLOCK_DRIFT_TOLERANCE or drift_high < -CLOCK_DRIFT_TOLERANCE:
        raise RuntimeError('wall clock stepped during OpenCode orchestration')
    return current


def acceptance_classification_time(submitted_at, anchor):
    current = checked_clock(anchor)
    deadline = current[2] + max(0, submitted_at + WINDOWS['accepted'] - current[1])
    while (remaining := deadline - time.monotonic()) > 0:
        time.sleep(remaining)
    return checked_clock(anchor)[1]


def seconds(value):
    if type(value) not in (int, float) or not math.isfinite(value):
        raise RuntimeError('unusable OpenCode timestamp')
    return value / 1000


def pending_turn(document, prompt):
    users = [m['info']['id'] for m in document['messages'] if m['info']['role'] == 'user'
             and ''.join(p.get('text', '') for p in m['parts'] if p.get('type') == 'text') == prompt]
    if len(users) != 1:
        return None
    pending = [m for m in document['messages'] if m['info']['role'] == 'assistant'
               and m['info'].get('parentID') == users[0]
               and 'completed' not in m['info']['time']]
    if len(pending) != 1:
        return None
    info = pending[0]['info']
    return dict(user_id=users[0], assistant_id=info['id'], started_at=seconds(info['time']['created']))


def approval_pending(document, screen):
    if not all(text in screen for text in ('Permission required', 'Shell command',
                                          APPROVAL_COMMAND, 'Allow once', 'Allow always', 'Reject')):
        return None
    turn = pending_turn(document, APPROVAL_PROMPT)
    if turn is None:
        return None
    message = next(m for m in document['messages'] if m['info']['id'] == turn['assistant_id'])
    tools = [p for p in message['parts'] if p.get('type') == 'tool']
    if len(tools) != 1 or any(p.get('type') not in ('step-start', 'reasoning', 'tool')
                              for p in message['parts']):
        return None
    tool = tools[0]
    state = tool.get('state', {})
    if (tool.get('tool') != 'bash' or not isinstance(tool.get('callID'), str)
            or state.get('status') != 'running'
            or state.get('input') != {'command': APPROVAL_COMMAND}
            or set(state) != {'status', 'input', 'time'}
            or set(state.get('time', {})) != {'start'}):
        return None
    seconds(state['time']['start'])
    return dict(turn, call_id=tool['callID'])


def busy_end(document, precondition, submitted_at):
    matches = [m['info'] for m in document['messages']
               if m['info']['id'] == precondition['assistant_id']]
    if len(matches) != 1 or matches[0].get('parentID') != precondition['user_id']:
        raise RuntimeError('busy turn identity changed')
    info = matches[0]
    if seconds(info['time']['created']) != precondition['started_at']:
        raise RuntimeError('busy turn start changed')
    if 'completed' not in info['time']:
        return None
    ended = seconds(info['time']['completed'])
    if not precondition['started_at'] < submitted_at < ended or info.get('finish') != 'stop':
        raise RuntimeError('submission was not inside the captured completed busy turn')
    return ended


def observed_export(document, *, marker, submitted_at, state, precondition):
    messages = document['messages']
    if state == 'approval':
        # require_approval has just revalidated this exact pending call and current menu.
        # It has no completed text and belongs to the preexisting blocked turn.
        messages = [m for m in messages if m['info']['id'] != precondition['assistant_id']]
    events, unusable = opencode_export_events(json.dumps(dict(document, messages=messages)))
    result = detect_outcomes(events, marker, submitted_at=submitted_at)
    if unusable or not {'user', 'assistant'} <= {e.role for e in events}:
        result.observable = False
    if state == 'busy':
        result.turn_end = busy_end(document, precondition, submitted_at)
        # The previous turn's completion must not supply the new turn or its model.
        later = [m for m in messages if m['info'].get('parentID') != precondition['user_id']]
        new_events, bad = opencode_export_events(json.dumps(dict(document, messages=later)))
        new = detect_outcomes(new_events, marker, submitted_at=submitted_at)
        result.outcomes.pop('turn_start', None)
        result.signals.pop('turn_start', None)
        if 'turn_start' in new.outcomes:
            result.outcomes['turn_start'] = new.outcomes['turn_start']
            result.signals['turn_start'] = new.signals['turn_start']
        result.model = new.model
        if bad:
            result.observable = False
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output-directory', required=True)
    parser.add_argument('--state', required=True,
                        choices=('idle', 'busy', 'approval', 'disconnected', 'restarted'))
    args = parser.parse_args()
    output = Path(args.output_directory).resolve()
    output.mkdir(mode=0o700)
    results = []
    for index in range(1, 4):
        directory = output / f'trial-{index}'
        directory.mkdir(mode=0o700)
        password = secrets.token_urlsafe(32)
        authorization = 'Basic ' + base64.b64encode(f'opencode:{password}'.encode()).decode()
        env = dict(os.environ, OPENCODE_SERVER_PASSWORD=password, OPENCODE_SERVER_USERNAME='opencode')
        if args.state == 'approval':
            env['OPENCODE_CONFIG_CONTENT'] = json.dumps({'permission': {'*': 'ask'}})
        with (directory / 'journal.jsonl').open('x') as journal:
            precondition = {}
            clients = []
            driver = None
            rejected = False

            def record(kind, **data):
                raw = json.dumps(dict(kind=kind, utc=time.time(), monotonic=time.monotonic(), **data))
                journal.write(raw.replace(password, '<generated-password>')
                              .replace(authorization, '<authorization>') + '\n')
                journal.flush()

            def run(argv, **kwargs):
                record('command_started', argv=argv, cwd=kwargs.get('cwd'))
                try:
                    result = subprocess.run(argv, env=env, **kwargs)
                except BaseException as error:
                    record('command_failed', error_type=type(error).__name__)
                    raise
                record('command_finished', argv=argv, returncode=result.returncode,
                       stdout=result.stdout, stderr=result.stderr)
                return result

            def popen(argv, **kwargs):
                record('server_started', argv=argv)
                return subprocess.Popen(argv, env=env, **kwargs)

            def pty(argv, **kwargs):
                record('pty_started', argv=argv)
                client = PtyClient(argv, env=env, **kwargs)
                clients.append(client)
                return client

            def readiness(url):
                if driver.server is None:
                    return http_status(url)  # Never send credentials to a preexisting listener.
                request = urllib.request.Request(url, headers={'Authorization': authorization})
                opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                try:
                    with opener.open(request, timeout=2) as response:
                        return response.status
                except urllib.error.HTTPError as error:
                    error.close()
                    return error.code

            class RecordedDriver(OpenCodeDriver):
                def document(self, session_id):
                    self.require_owned(session_id)
                    raw = self._export(session_id)
                    if raw is None:
                        raise RuntimeError('bound OpenCode export unavailable')
                    return json.loads(raw)

                def require_approval(self, session_id, document):
                    client = self.live_client_for(session_id)
                    if client is None or approval_pending(document, terminal_screen(client)) != precondition:
                        raise RuntimeError('approval precondition no longer holds')

                def submit(self, session_id, message):
                    nonlocal rejected
                    self.require_owned(session_id)
                    if args.state in ('idle', 'busy') and self.live_client_for(session_id) is None:
                        raise RuntimeError('the client establishing the submission state disappeared')
                    if args.state == 'approval':
                        self.require_approval(session_id, self.document(session_id))
                    record('submission_started', session_id=session_id, message=message)
                    if args.state not in ('disconnected', 'restarted'):
                        return super().submit(session_id, message)
                    if self.server is not None or self.clients:
                        raise RuntimeError('disconnected state has a live transport')
                    result = run(['opencode', 'run', '--pure', '--format', 'json',
                                  '--attach', precondition['url'], '--session', session_id,
                                  '-m', self.model, message], cwd=self.cwd, capture_output=True,
                                 text=True, stdin=subprocess.DEVNULL, timeout=15)
                    if (result.returncode != 1 or result.stdout.strip()
                            or 'Session not found' not in result.stderr):
                        raise RuntimeError('uncaptured disconnected submission response')
                    rejected = True
                    record('observed_rejection', returncode=1, reason='Session not found')
                    if args.state == 'restarted':
                        self.serve()
                        record('restarted', url=self.server.url)
                    # Keep observing all windows independently after the captured rejection.
                    return None

                def observe(self, session_id, **kwargs):
                    document = self.document(session_id)
                    if args.state == 'disconnected' and self.server is not None:
                        raise RuntimeError('disconnected server unexpectedly restarted')
                    if args.state != 'disconnected':
                        if self.server is None or self.server.process.poll() is not None:
                            raise RuntimeError('owned server unexpectedly disappeared')
                    if args.state == 'approval':
                        self.require_approval(session_id, document)
                    result = observed_export(document, state=args.state,
                                             precondition=precondition, **kwargs)
                    record('observation', observation=dataclasses.asdict(result))
                    (directory / 'export-last.json').write_text(json.dumps(document, indent=2) + '\n')
                    return result

                def teardown(self, session_id):
                    try:
                        raw = self._export(session_id)
                        if raw is not None:
                            (directory / f'export-{session_id}.json').write_text(raw)
                        for number, client in enumerate(clients):
                            with client.lock:
                                (directory / f'pty-{number}.raw').write_bytes(bytes(client.window))
                    finally:
                        super().teardown(session_id)

            version = run(['opencode', '--version'], capture_output=True, text=True, timeout=15)
            if version.returncode or version.stdout.strip() != '1.18.30':
                raise RuntimeError('OpenCode version changed; capture compatibility first')
            cwd = tempfile.mkdtemp(prefix='parley-probe-', dir='/tmp')
            record('probe_directory', cwd=cwd,
                   permission_override={'*': 'ask'} if args.state == 'approval' else None)
            driver = RecordedDriver(SessionRegistry(), cwd=cwd, run=run, popen=popen, http_get=readiness)
            driver.pty = pty

            def settle(session_id):
                driver.serve()
                if args.state in ('disconnected', 'restarted'):
                    precondition['url'] = driver.server.url
                    driver.port = int(driver.server.url.rsplit(':', 1)[1])
                    if driver.close_servers():
                        raise RuntimeError('owned server failed to stop')
                    record('precondition', state=args.state, evidence=precondition)
                    return
                client = driver.open_client(['opencode', 'attach', driver.server.url, '--pure',
                                             '--dir', cwd, '--session', session_id])
                client.serves = session_id
                deadline = time.monotonic() + 45
                while True:
                    if client.eof:
                        raise RuntimeError('owned terminal exited before idle readiness')
                    screen = terminal_screen(client)
                    if (all(text in screen for text in ('PONG', 'Ling 3.0 Flash Fin Free', cwd))
                            and 'esc interrupt' not in screen and 'Permission required' not in screen
                            and time.monotonic() - client.last_output >= 3):
                        break
                    if client.eof or time.monotonic() >= deadline:
                        raise RuntimeError('owned idle terminal not established')
                    time.sleep(0.1)
                if args.state in ('busy', 'approval'):
                    prompt = BUSY_PROMPT if args.state == 'busy' else APPROVAL_PROMPT
                    record('state_prompt', prompt=prompt)
                    client.type_line(prompt)
                    deadline = time.monotonic() + 90
                    while True:
                        document = driver.document(session_id)
                        screen = terminal_screen(client)
                        candidate = (approval_pending(document, screen) if args.state == 'approval'
                                     else pending_turn(document, prompt) if 'esc interrupt' in screen else None)
                        if candidate:
                            precondition.update(candidate)
                            (directory / 'export-precondition.json').write_text(json.dumps(document, indent=2) + '\n')
                            break
                        if client.eof or time.monotonic() >= deadline:
                            raise RuntimeError('requested state not established')
                        time.sleep(0.1)
                record('precondition', state=args.state, evidence=precondition, screen=terminal_screen(client))

            try:
                clock_anchor = sample_clock()
                trial = run_trial_with_cleanup(driver, prompt='Reply with exactly PONG. Do not call tools or change files.',
                                               state=args.state, settle=settle, poll_interval=1)
                if rejected:
                    trial.observable['accepted'] = True
                    trial.submission_diagnostic = 'exit 1: Session not found; independent windows still observed'
                record('trial', trial=dataclasses.asdict(trial))
                if trial.interrupted or trial.clock_step is not None:
                    raise RuntimeError('invalid or interrupted trial retained separately')
                # A captured live rejection returns before the acceptance window closes.
                # The outer clock bracket also covers setup/cleanup and this remaining wait.
                classification_utc = acceptance_classification_time(trial.submitted_at, clock_anchor)
                result = classify_trial(Trial(submitted=trial.submitted_at, state=trial.state,
                                              outcomes=trial.outcomes, turn_end=trial.turn_end,
                                              turn_end_observable=trial.turn_end_observable),
                                        classification_utc, supported=trial.supported, observable=trial.observable)
                record('classified', outcomes=result, classification_utc=classification_utc)
                results.append(result)
            except BaseException as error:
                record('attempt_failed', error_type=type(error).__name__, detail=str(error))
                raise
            finally:
                record('ownership_after_cleanup', owned=sorted(driver.owned()), clients=len(driver.clients),
                       server_held=driver.server is not None)
            if driver.owned() or driver.clients or driver.server is not None:
                raise RuntimeError('cleanup incomplete')
    combined = {name: aggregate([row[name] for row in results]) for name in results[0]}
    (output / 'aggregate.json').write_text(json.dumps(dict(state=args.state, trials=results,
                                                        aggregate=combined), indent=2) + '\n')


if __name__ == '__main__':
    main()

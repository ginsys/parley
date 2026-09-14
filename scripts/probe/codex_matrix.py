"""Live Codex 0.154.0 matrix: three owned-session trials per captured state.

All supported state preconditions and pending-item deletion have captured evidence in
docs/host-probe-preflight.md. New host shapes require captures before extension.
"""

import argparse
import dataclasses
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

BUSY_PROMPT = ('This is an isolated busy-state preflight. Write the integers from '
               'one through two hundred in English words, one per line, followed by '
               'HOLD COMPLETE. Do not use tools or change files.')
APPROVAL_PROMPT = (
    'This is an isolated approval prompt preflight. Use exec_command with '
    'sandbox_permissions=require_escalated for the exact command '
    'python3 -c \'print("PARLEY_APPROVAL_PREFLIGHT")\'. '
    'Ask for approval and wait. Do not use an unapproved fallback or another tool.')


def bound_records(driver, thread_id):
    lines = driver._read_rollout(thread_id)
    if lines is None:
        raise RuntimeError('state evidence has no bound rollout')
    return [json.loads(line) for line in lines]


def active_turn(records, after):
    from host_trials import record_time

    starts = {}
    ends = set()
    for record in records:
        payload = record.get('payload', {})
        if record.get('type') != 'event_msg' or not isinstance(payload, dict):
            continue
        turn_id = payload.get('turn_id')
        stamp = record_time(record)
        if payload.get('type') == 'task_started' and turn_id and stamp is not None and stamp >= after:
            starts[turn_id] = stamp
        elif payload.get('type') in ('task_complete', 'turn_aborted'):
            ends.add(turn_id)
    pending = [(turn_id, stamp) for turn_id, stamp in starts.items() if turn_id not in ends]
    if len(pending) == 1:
        turn_id, stamp = pending[0]
        return dict(turn_id=turn_id, started_at=stamp)
    return None


def verify_busy_interval(records, precondition, submitted_at, observed_end):
    from host_trials import record_time

    ends = [record_time(record) for record in records
            if record.get('type') == 'event_msg'
            and record.get('payload', {}).get('type') in ('task_complete', 'turn_aborted')
            and record['payload'].get('turn_id') == precondition['turn_id']]
    ends = [stamp for stamp in ends if stamp is not None]
    if (len(ends) != 1 or not precondition['started_at'] < submitted_at < ends[0]
            or observed_end != ends[0]):
        raise RuntimeError('submission was not proven inside the exact preexisting turn')


def approval_pending(records, screen):
    # Captured full menu tail plus an unresolved synthetic tool call, not historical text alone.
    tail = screen[-5000:]
    if not all(text in tail for text in (
            'Would you like to run the following command?',
            'PARLEY_APPROVAL_PREFLIGHT', '1. Yes, proceed (y)',
            '2. No, and tell Codex what to do differently (esc)',
            'Press enter to confirm or esc to cancel')):
        return None
    completed = {record['payload'].get('call_id') for record in records
                 if record.get('type') == 'response_item'
                 and record.get('payload', {}).get('type') == 'custom_tool_call_output'}
    calls = [record['payload'] for record in records
             if record.get('type') == 'response_item'
             and record.get('payload', {}).get('type') == 'custom_tool_call'
             and record['payload'].get('name') == 'exec'
             and 'PARLEY_APPROVAL_PREFLIGHT' in record['payload'].get('input', '')
             and record['payload'].get('call_id') not in completed]
    return dict(call_id=calls[0]['call_id']) if len(calls) == 1 else None


def require_approval(driver, thread_id):
    client = driver.live_client_for(thread_id)
    if client is None or not approval_pending(bound_records(driver, thread_id), client.text_since(0)):
        raise RuntimeError('approval precondition no longer holds')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', default=str(Path(__file__).resolve().parents[2]))
    parser.add_argument('--output-directory', required=True)
    parser.add_argument('--state', required=True, choices=('idle', 'busy', 'approval', 'disconnected', 'restarted'))
    args = parser.parse_args()
    sys.path.insert(0, str(Path(args.repo) / 'scripts' / 'probe'))
    from host_trials import (
        CodexDriver,
        PtyClient,
        SessionRegistry,
        SubmissionRejected,
        classify_trial,
        run_trial_with_cleanup,
    )
    from wake_probe import WINDOWS, Trial, aggregate

    output = Path(args.output_directory)
    output.mkdir(mode=0o700)
    results = []
    for index in range(1, 4):
        trial_dir = output / f'trial-{index}'
        trial_dir.mkdir(mode=0o700)
        with (trial_dir / 'journal.jsonl').open('x') as journal:
            clients = []
            precondition = {}

            def record(kind, **data):
                journal.write(json.dumps(dict(kind=kind, utc=time.time(),
                                              monotonic=time.monotonic(), **data)) + '\n')
                journal.flush()

            def run(argv, **kwargs):
                record('command_started', argv=argv, cwd=kwargs.get('cwd'),
                       stdin_devnull=kwargs.get('stdin') == subprocess.DEVNULL)
                try:
                    result = subprocess.run(argv, **kwargs)
                except BaseException as error:
                    record('command_failed', error_type=type(error).__name__)
                    raise
                record('command_finished', argv=argv, returncode=result.returncode,
                       stdout=result.stdout, stderr=result.stderr)
                return result

            def pty(argv, **kwargs):
                if args.state == 'approval':
                    argv = list(argv)
                    argv[argv.index('-a') + 1] = 'on-request'
                    argv = [argv[0], '-c', 'approvals_reviewer="user"', *argv[1:]]
                record('pty_started', argv=argv, cwd=kwargs.get('cwd'))
                client = PtyClient(argv, **kwargs)
                clients.append(client)
                return client

            class RecordedDriver(CodexDriver):
                def observe(self, thread_id, **kwargs):
                    if args.state == 'approval':
                        require_approval(self, thread_id)
                    observation = super().observe(thread_id, **kwargs)
                    record('observation', thread_id=thread_id,
                           observation=dataclasses.asdict(observation))
                    return observation

                def submit(self, thread_id, message):
                    record('submission_started', thread_id=thread_id, message=message)
                    if args.state == 'approval':
                        require_approval(self, thread_id)
                    if args.state == 'disconnected':
                        self.require_owned(thread_id)
                        if self.clients:
                            raise RuntimeError('disconnected submission has a client')
                        # Captured pending-item cleanup permits this deliberate no-client trial.
                        # The ordinary observer still checks the complete bound rollout and its
                        # creation roles. No queue_clients entry requires an intentionally absent TUI.
                        response = self.run(
                            ['codex', 'queue', '--thread', thread_id, '--message', message],
                            capture_output=True, text=True, timeout=15)
                        if response.returncode:
                            raise SubmissionRejected(response.returncode, response.stderr)
                        result = True
                    else:
                        result = super().submit(thread_id, message)
                    record('submission_returned', result=result)
                    return result

                def teardown(self, thread_id):
                    self.require_owned(thread_id)
                    try:
                        path = self.rollout_path_for(thread_id)
                        if path:
                            (trial_dir / 'rollout.jsonl').write_bytes(Path(path).read_bytes())
                        for number, client in enumerate(clients):
                            (trial_dir / f'pty-{number}.txt').write_text(client.text_since(0))
                    finally:
                        super().teardown(thread_id)
                    record('deleted', thread_id=thread_id, owned=sorted(self.owned()))

            version = run(['codex', '--version'], capture_output=True, text=True, timeout=15)
            if version.returncode or version.stdout.strip() != 'codex-cli 0.154.0':
                raise RuntimeError('version changed; capture new compatibility before trials')
            cwd = tempfile.mkdtemp(prefix='parley-probe-', dir='/tmp')
            record('probe_directory', cwd=cwd, retained_for_explicit_cleanup=True)
            git_env = {key: value for key, value in os.environ.items() if not key.startswith('GIT_')}
            git_env.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_SYSTEM=os.devnull,
                           GIT_CONFIG_NOSYSTEM='1')
            run(['git', 'init', '--quiet', cwd], capture_output=True, text=True,
                check=True, env=git_env, timeout=10)
            driver = RecordedDriver(SessionRegistry(), cwd=cwd, run=run, pty=pty,
                                    mechanism='queue-then-resume' if args.state == 'restarted' else 'queue')

            def settle(thread_id):
                if args.state in ('disconnected', 'restarted'):
                    if driver.clients:
                        raise RuntimeError('precondition requires no serving client')
                    record('precondition', state=args.state, live_clients=0,
                           sequence='completed exec; queue; ' +
                           ('resume in submit' if args.state == 'restarted' else 'stay disconnected'))
                    return
                client = driver.attach(thread_id)
                if args.state == 'idle':
                    record('precondition', state='idle', serves=client.serves,
                           client_eof=client.eof, screen=client.text_since(0))
                    return
                before = time.time()
                prompt = BUSY_PROMPT if args.state == 'busy' else APPROVAL_PROMPT
                record('state_prompt', state=args.state, prompt=prompt)
                client.type_line(prompt)
                deadline = time.monotonic() + 45
                while True:
                    if client.eof:
                        raise RuntimeError('client exited before state precondition')
                    if args.state == 'busy':
                        candidate = active_turn(bound_records(driver, thread_id), before)
                    else:
                        candidate = approval_pending(bound_records(driver, thread_id),
                                                     client.text_since(0))
                    if candidate:
                        precondition.update(candidate)
                        record('precondition', state=args.state, evidence=candidate,
                               screen=client.text_since(0))
                        return
                    if time.monotonic() >= deadline:
                        raise RuntimeError('requested state was not observed')
                    time.sleep(0.1)

            try:
                trial = run_trial_with_cleanup(
                    driver, prompt='Reply with exactly PONG. Do not call tools or change files.',
                    state=args.state, settle=settle, poll_interval=1.0)
                record('trial', trial=dataclasses.asdict(trial))
                if args.state == 'busy':
                    records = [json.loads(line) for line in
                               (trial_dir / 'rollout.jsonl').read_text().splitlines()]
                    verify_busy_interval(records, precondition, trial.submitted_at, trial.turn_end)

                if trial.interrupted or trial.clock_step is not None:
                    raise RuntimeError('interrupted or invalid-clock attempt is not a repeated trial')
                # An explicit rejection can return before its acceptance window closes.
                # Wait actual elapsed time; never substitute a future classification timestamp.
                remaining = trial.submitted_at + WINDOWS['accepted'] - time.time()
                if remaining > 0:
                    time.sleep(remaining)
                classified = classify_trial(
                    Trial(submitted=trial.submitted_at, state=trial.state, outcomes=trial.outcomes,
                          turn_end=trial.turn_end, turn_end_observable=trial.turn_end_observable),
                    time.time(), supported=trial.supported, observable=trial.observable)
                record('classified', outcomes=classified)
                results.append(classified)
            except BaseException as error:
                record('attempt_failed', error_type=type(error).__name__, detail=str(error))
                raise
            finally:
                record('ownership_after_cleanup', owned=sorted(driver.owned()),
                       clients=len(driver.clients))
            if driver.owned() or driver.clients:
                raise RuntimeError('cleanup incomplete; do not continue with another trial')
    combined = {name: aggregate([result[name] for result in results]) for name in WINDOWS}
    (output / 'aggregate.json').write_text(json.dumps(dict(state=args.state, trials=results,
                                                        aggregate=combined), indent=2) + '\n')


if __name__ == '__main__':
    main()

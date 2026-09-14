"""Explicitly aggregate exactly three completed captures, never select or retry attempts."""

import argparse
import json
from pathlib import Path

from host_trials import classify_trial
from wake_probe import Trial, aggregate


def combine(directories):
    if len(directories) != 3 or len({Path(path).resolve() for path in directories}) != 3:
        raise ValueError('exactly three distinct trial directories are required')
    trials = []
    for directory in directories:
        path = Path(directory)
        rows = [json.loads(line) for line in (path / 'journal.jsonl').read_text().splitlines()]
        runs = [row['trial'] for row in rows if row['kind'] == 'trial']
        results = [row for row in rows if row['kind'] == 'classified']
        cleanups = [row for row in rows if row['kind'] == 'ownership_after_cleanup']
        if (len(runs) != 1 or len(results) != 1 or len(cleanups) != 1
                or cleanups[0]['owned'] or cleanups[0]['clients']
                or any(row['kind'] == 'attempt_failed' for row in rows)):
            raise ValueError('trial is incomplete, failed or not cleaned up')
        run, result = runs[0], results[0]
        if run['interrupted'] or run['clock_step'] is not None or not run['version']:
            raise ValueError('interrupted, clock-invalid or unversioned trial')
        checked = classify_trial(
            Trial(submitted=run['submitted_at'], state=run['state'], outcomes=run['outcomes'],
                  turn_end=run['turn_end'], turn_end_observable=run['turn_end_observable']),
            result.get('classification_utc', result['utc']),
            supported=run['supported'], observable=run['observable'])
        if checked != result['outcomes']:
            raise ValueError('recorded classification does not match its original trial clock')
        trials.append(dict(source=f'{path.parent.name}/{path.name}', trial=run, outcomes=checked))
    if (len({entry['trial']['state'] for entry in trials}) != 1
            or len({entry['trial']['version'] for entry in trials}) != 1
            or len({entry['trial']['marker'] for entry in trials}) != 3
            or len({entry['trial']['session_id'] for entry in trials}) != 3):
        raise ValueError('trials must share state/version and have independent sessions/markers')
    return dict(state=trials[0]['trial']['state'], version=trials[0]['trial']['version'],
                trials=trials,
                aggregate={key: aggregate([entry['outcomes'][key] for entry in trials])
                           for key in trials[0]['outcomes']})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    parser.add_argument('trials', nargs=3)
    args = parser.parse_args()
    result = combine(args.trials)
    with Path(args.output).open('x') as handle:
        handle.write(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    main()

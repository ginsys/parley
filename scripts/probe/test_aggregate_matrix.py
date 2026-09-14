"""Offline aggregation must not relabel failed attempts as repeated live trials."""

import json
import tempfile
import unittest
from pathlib import Path

from aggregate_matrix import combine


class AggregateTests(unittest.TestCase):
    def test_exact_three_completed_independent_trials_and_failed_attempt_refusal(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for index in range(3):
                path = Path(directory) / f'trial-{index}'
                path.mkdir()
                run = dict(state='idle', version='synthetic', marker=f'marker-{index}',
                           session_id=f'session-{index}', submitted_at=10, outcomes={},
                           turn_end=None, turn_end_observable=True, supported={}, observable={},
                           interrupted=False, clock_step=None)
                rows = [dict(kind='trial', trial=run),
                        dict(kind='classified', utc=140, outcomes={key: 'not_observed'
                             for key in ('accepted', 'visible', 'turn_start', 'ack')}),
                        dict(kind='ownership_after_cleanup', owned=[], clients=0)]
                (path / 'journal.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in rows))
                paths.append(path)
            self.assertTrue(all(value == 'not_observed' for value in combine(paths)['aggregate'].values()))
            for path in paths:
                rows = list(map(json.loads, (path / 'journal.jsonl').read_text().splitlines()))
                rows[1]['classification_utc'] = rows[1]['utc']
                rows[1]['utc'] = 0  # A later journal clock reading is not the checked decision instant.
                (path / 'journal.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in rows))
            self.assertTrue(all(value == 'not_observed' for value in combine(paths)['aggregate'].values()))
            with self.assertRaisesRegex(ValueError, 'distinct'):
                combine([paths[0], paths[0], paths[2]])
            with (paths[-1] / 'journal.jsonl').open('a') as handle:
                handle.write(json.dumps(dict(kind='attempt_failed')) + '\n')
            with self.assertRaisesRegex(ValueError, 'failed'):
                combine(paths)


if __name__ == '__main__':
    unittest.main()

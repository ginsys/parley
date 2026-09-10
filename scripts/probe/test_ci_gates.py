"""Exercise the actual aggregate shell and mise dependency graph with Python failure."""

import json
import os
import subprocess
import tempfile
import tomllib
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


class GateTests(unittest.TestCase):
    def test_python_failure_reaches_ci_aggregate_for_all_events(self):
        ci = yaml.load((ROOT / '.github/workflows/ci.yml').read_text(), Loader=yaml.BaseLoader)
        jobs = ci['jobs']
        self.assertIn('python', jobs['checks']['needs'])
        self.assertNotIn('if', jobs['python'])
        install = [s['with']['install_args'] for s in jobs['python']['steps']
                   if 'with' in s and 'install_args' in s['with']]
        self.assertEqual(install, ['python ruff'])
        script = jobs['checks']['steps'][0]['run']
        for event in ('pull_request', 'push', 'merge_group'):
            self.assertIn(event, ci['on'])
            for result in ('success', 'failure', 'cancelled'):
                with self.subTest(event=event, result=result):
                    needs = {n: {'result': 'success'} for n in jobs['checks']['needs']}
                    needs['python']['result'] = result
                    if event != 'pull_request':
                        needs['commit-lint']['result'] = 'skipped'
                    env = dict(os.environ, EVENT=event, RESULTS=json.dumps(needs))
                    ran = subprocess.run(['bash', '-e', '-c', script], env=env,
                                         capture_output=True, text=True, timeout=10)
                    self.assertEqual(ran.returncode == 0, result == 'success', ran.stdout)

    def test_python_failure_reaches_mise_verify(self):
        config = tomllib.loads((ROOT / 'mise.toml').read_text())
        deps = config['tasks']['verify']['depends']
        self.assertIn('python', deps)
        # Keep the real aggregate dependency list; replace expensive unrelated tasks
        # with successful stubs and inject a known Python task failure.
        with tempfile.TemporaryDirectory() as tmp:
            text = '[tasks.verify]\ndepends = ' + json.dumps(deps) + '\n'
            for name in deps:
                text += f'\n[tasks.{name}]\nrun = "' + ('exit 41' if name == 'python' else 'true') + '"\n'
            Path(tmp, 'mise.toml').write_text(text)
            empty = Path(tmp, 'empty.toml')
            empty.write_text('')
            env = dict(os.environ, MISE_TRUSTED_CONFIG_PATHS=tmp,
                       MISE_GLOBAL_CONFIG_FILE=str(empty), MISE_SYSTEM_CONFIG_FILE=str(empty),
                       MISE_TASK_RUN_AUTO_INSTALL='false')
            ran = subprocess.run(['mise', 'run', 'verify'], cwd=tmp, env=env,
                                 capture_output=True, text=True, timeout=30)
            self.assertNotEqual(ran.returncode, 0)
            self.assertIn('exit 41', ran.stdout + ran.stderr)
            self.assertIn('failed', (ran.stdout + ran.stderr).lower())


if __name__ == '__main__':
    unittest.main()

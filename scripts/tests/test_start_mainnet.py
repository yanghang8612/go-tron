"""The mainnet entrypoint must not build after a broken verification."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).parents[1] / 'start.sh'
REMOTE = 'a' * 40


class StartMainnetTests(unittest.TestCase):
    def run_start(self, verify_status):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repo = root / 'go-tron'
            (repo / '.git').mkdir(parents=True)
            fake_bin = root / 'bin'
            fake_bin.mkdir()

            def fake(name, body):
                path = fake_bin / name
                path.write_text('#!/bin/sh\n' + body + '\n')
                path.chmod(0o755)

            fake('git', 'case "$1" in rev-parse) echo ' + REMOTE +
                 ';; symbolic-ref) echo master;; esac\nexit 0')
            fake('sudo', 'echo "$@" >> "$MOCK_CALLS"\n'
                 'case "$*" in *" verify --source "*) '
                 'if [ "$MOCK_VERIFY_STATUS" = 4 ]; then '
                 'if [ ! -f "$MOCK_SECOND_VERIFY" ]; then touch "$MOCK_SECOND_VERIFY"; exit 4; fi; '
                 'exit 0; fi; exit "$MOCK_VERIFY_STATUS";; esac\nexit 0')
            fake('flock', 'exit 0')
            fake('systemctl', 'exit 0')
            fake('curl', 'exit 0')
            fake('make', 'echo "make $*" >> "$MOCK_CALLS"\nexit 7')
            env = os.environ.copy()
            env.update({'APP_ROOT': str(root), 'REPO_DIR': str(repo),
                        'STATE_FILE': str(root / 'deployed.rev'),
                        'LOCK_FILE': str(root / 'start.lock'),
                        'LOG_FILE': str(root / 'start.log'),
                        'RELEASE_DIR': str(root / 'releases'),
                        'MOCK_CALLS': str(root / 'calls'),
                        'MOCK_SECOND_VERIFY': str(root / 'second-verify'),
                        'MOCK_VERIFY_STATUS': str(verify_status),
                        'PATH': str(fake_bin) + os.pathsep + env['PATH']})
            result = subprocess.run(['/bin/bash', str(SCRIPT)], cwd=str(root), env=env,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    timeout=15, check=False)
            state = (root / 'deployed.rev').read_text() if (root / 'deployed.rev').exists() else None
            calls = (root / 'calls').read_text() if (root / 'calls').exists() else ''
            return result.returncode, result.stdout.decode('utf-8', 'replace'), state, calls

    def test_verified_running_source_repairs_stale_record_without_build(self):
        status, output, state, calls = self.run_start(0)
        self.assertEqual(status, 0, output)
        self.assertEqual(state, REMOTE + '\n')
        self.assertIn('verify --source ' + REMOTE, calls)
        self.assertNotIn('make ', calls)
        self.assertNotIn(' restart ', calls)

    def test_broken_helper_or_guard_fails_closed(self):
        status, output, state, calls = self.run_start(1)
        self.assertNotEqual(status, 0, output)
        self.assertIsNone(state)
        self.assertNotIn('make ', calls)

    def test_only_distinct_source_enters_build(self):
        status, output, state, calls = self.run_start(3)
        self.assertNotEqual(status, 0, output)  # the fake build stops here
        self.assertIsNone(state)
        self.assertIn('make zksnark-deps', calls)

    def test_same_source_unhealthy_restarts_then_strictly_verifies(self):
        status, output, state, calls = self.run_start(4)
        self.assertEqual(status, 0, output)
        self.assertEqual(state, REMOTE + '\n')
        self.assertEqual(calls.count('verify --source ' + REMOTE), 2)
        self.assertIn('systemctl restart gtron.service', calls)
        self.assertNotIn('make ', calls)


if __name__ == '__main__':
    unittest.main()

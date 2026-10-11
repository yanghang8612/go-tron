"""The mainnet entrypoint must not build after a broken verification."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).parents[1] / 'start.sh'
REMOTE = 'a' * 40


class StartMainnetTests(unittest.TestCase):
    def run_start(self, verify_status, staging_guard_status=0, staged=False, build_success=False):
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
                 'case "$*" in *" deploy "*) printf "deploy-arg=<%s>\\n" "$@" >> "$MOCK_CALLS";; esac\n'
                 'case "$*" in *" check-deploy") exit "$MOCK_STAGING_GUARD_STATUS";; esac\n'
                 'case "$*" in *" staging-timeout "*) echo 43200; exit 0;; esac\n'
                 'case "$*" in *" verify --source "*) '
                 'if [ "$MOCK_VERIFY_STATUS" = 4 ]; then '
                 'if [ ! -f "$MOCK_SECOND_VERIFY" ]; then touch "$MOCK_SECOND_VERIFY"; exit 4; fi; '
                 'exit 0; fi; exit "$MOCK_VERIFY_STATUS";; esac\nexit 0')
            fake('flock', 'exit 0')
            fake('systemctl', 'exit 0')
            fake('curl', 'exit 0')
            fake('make', 'echo "make $*" >> "$MOCK_CALLS"\n'
                 '[ "$MOCK_BUILD_SUCCESS" = 1 ] || exit 7\n'
                 'for arg in "$@"; do case "$arg" in GOBIN=*) '
                 'out="${arg#GOBIN=}"; mkdir -p "$out"; '
                 'printf "#!/bin/sh\\nexit 0\\n" > "$out/gtron"; '
                 'chmod +x "$out/gtron";; esac; done\nexit 0')
            fake('sha256sum', 'echo "' + 'b' * 64 + '  $1"')
            env = os.environ.copy()
            required = root / 'main' / 'HISTORY_STAGING_READER_REQUIRED.json'
            if staged:
                required.parent.mkdir()
                required.write_text('{}')
            env.update({'APP_ROOT': str(root), 'REPO_DIR': str(repo),
                        'STATE_FILE': str(root / 'deployed.rev'),
                        'LOCK_FILE': str(root / 'start.lock'),
                        'LOG_FILE': str(root / 'start.log'),
                        'RELEASE_DIR': str(root / 'releases'),
                        'MOCK_CALLS': str(root / 'calls'),
                        'MOCK_SECOND_VERIFY': str(root / 'second-verify'),
                        'MOCK_VERIFY_STATUS': str(verify_status),
                        'MOCK_BUILD_SUCCESS': '1' if build_success else '0',
                        'MOCK_STAGING_GUARD_STATUS': str(staging_guard_status),
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

    def test_inactive_stop_intent_exits_successfully_before_fetch_or_restart(self):
        status, output, state, calls = self.run_start(3, staging_guard_status=3)
        self.assertEqual(status, 0, output)
        self.assertIsNone(state)
        self.assertIn('check-deploy', calls)
        self.assertNotIn('verify --source', calls)
        self.assertNotIn('systemctl restart', calls)
        self.assertNotIn('make ', calls)

    def test_migration_guard_failure_blocks_all_deployment_paths(self):
        status, output, state, calls = self.run_start(3, staging_guard_status=1)
        self.assertNotEqual(status, 0, output)
        self.assertIsNone(state)
        self.assertNotIn('verify --source', calls)
        self.assertNotIn('make ', calls)

    def test_staged_reader_uses_validated_extended_health_wait(self):
        status, output, state, calls = self.run_start(0, staged=True)
        self.assertEqual(status, 0, output)
        self.assertIn('staging-timeout --staging-health-timeout-sec 43200', calls)
        self.assertIn('staging startup health timeout: 43200s', output)

    def test_release_publication_optional_health_arguments_under_nounset(self):
        # /bin/bash is macOS Bash 3.2 here; empty arrays under nounset also
        # fail on older server Bash. Exercise the complete publication path,
        # preserving both absence and exact argument boundaries.
        for staged in (False, True):
            with self.subTest(staged=staged):
                status, output, state, calls = self.run_start(
                    3, staged=staged, build_success=True)
                self.assertEqual(status, 0, output)
                self.assertEqual(state, REMOTE + '\n')
                args = [line[len('deploy-arg=<'):-1] for line in calls.splitlines()
                        if line.startswith('deploy-arg=<')]
                self.assertEqual(args[:7], ['-n', '/usr/bin/python3',
                    '/usr/local/libexec/gtron-mainnet-release.py', 'deploy',
                    '--source', REMOTE, '--candidate'])
                self.assertTrue(args[7].endswith('/gtron'), args)
                self.assertEqual(args[8:10], ['--sha256', 'b' * 64])
                self.assertEqual(len(args), 12 if staged else 10)
                self.assertNotIn('', args)
                if staged:
                    self.assertEqual(args[-2:], ['--staging-health-timeout-sec', '43200'])
                else:
                    self.assertNotIn('--staging-health-timeout-sec', args)


if __name__ == '__main__':
    unittest.main()

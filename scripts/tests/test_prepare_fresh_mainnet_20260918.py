import ast
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / 'scripts/prepare_fresh_mainnet_20260918.py'
spec = importlib.util.spec_from_file_location('fresh_prepare', str(PATH))
s = importlib.util.module_from_spec(spec); spec.loader.exec_module(s)


class FreshPrepareTests(unittest.TestCase):
    def test_scope_requires_gc_reward_and_builder_but_not_later_ops_or_diagnostic(self):
        with mock.patch.object(s, 'REPO', ROOT):
            builder = s.load_builder()
        required = builder.REQUIRED_SOURCE_CHANGES
        for name in ('core/state/snapshots/retired_metadata_gc.go',
                     'core/reward/voter_reward.go', s.SCRIPT_PATH, s.TEST_PATH):
            self.assertIn(name, required)
        self.assertNotIn(s.ACCEPT_PATH, required)
        self.assertNotIn(s.CUTOVER_PATH, required)
        self.assertIn(s.ACCEPT_PATH, builder.ALLOWED_SOURCE_CHANGES)
        self.assertIn(s.CUTOVER_PATH, builder.ALLOWED_SOURCE_CHANGES)
        self.assertNotIn('cmd/reward-trace/historical_withdraw.go', required)
        self.assertIn('cmd/reward-trace/historical_withdraw.go', builder.ALLOWED_SOURCE_CHANGES)
        self.assertEqual(builder.BINARY, s.RELEASE / 'gtron')
        self.assertEqual(builder.build_environment()['GOAMD64'], 'v1')
        self.assertIn(('core/state/snapshots',
                       'TestManifestPublicationSeedsExactDecodedView'), builder.REQUIRED_TESTS)

    def test_reward_gate_is_full_package_with_exact_golden(self):
        self.assertEqual(s.REWARD_GOLDEN, 'TestOldRewardSum_JavaCompoundAssignmentGolden')
        source = PATH.read_text()
        body = source[source.index('def prepare('):]
        self.assertIn("'./core/reward'", body)
        self.assertIn("'-count=1'", body)
        self.assertNotIn('./cmd/reward-trace', body)

    def test_python36_and_no_service_or_database_operation(self):
        ast.parse(PATH.read_text(), feature_version=(3, 6))
        for forbidden in ('systemctl', '/data/gtron/main/datadir', 'shutil.rmtree'):
            self.assertNotIn(forbidden, PATH.read_text())

    def test_main_requires_exact_checkout_before_build(self):
        fake = types.SimpleNamespace(SIGNALS=(), interrupted=None)
        argv = ['prepare', '--source-revision', 'a' * 40, '--script-revision', 'b' * 40,
                '--rust-library-sha256', 'c' * 64, '--expected-checkout', 'd' * 40]
        with mock.patch.object(s, 'load_builder', return_value=fake), \
             mock.patch.object(s, 'prepare', return_value={'prepared': True}) as prepare, \
             mock.patch.object(s.os, 'geteuid', return_value=0), \
             mock.patch.object(s.sys, 'platform', 'linux'), \
             mock.patch.object(s.sys, 'argv', argv), mock.patch('builtins.print'):
            s.main()
        self.assertEqual(fake.CHECKOUT, 'd' * 40)
        prepare.assert_called_once()

    def test_failures_after_base_build_never_publish_final_prepared(self):
        class Helper:
            @staticmethod
            def read_regular(path):
                return Path(path).read_bytes()

            @staticmethod
            def verify_source(unused, manifest):
                return None

        class Builder:
            GO = '/fake/go'
            REPO = Path('/fake/repo')

            def __init__(self, release, fail_at):
                self.RELEASE, self.fail_at = release, fail_at

            @staticmethod
            def require(ok, message):
                if not ok:
                    raise RuntimeError(message)

            def prepare(self, args):
                self.write_json('prepared.json', {
                    'manifest': {}, 'checkout_before': {'head': args.source_revision},
                    'native_commands': [], 'go_binary_sha256': '1' * 64,
                    'archive_sha256': '2' * 64, 'script_sha256': '3' * 64,
                })
                return {'prepared': True}

            @staticmethod
            def build_environment():
                return {}

            def run_logged(self, argv, name, env, timeout=1800):
                if self.fail_at == 'reward' and name == 'native-reward-tests.jsonl':
                    raise RuntimeError('injected reward failure')
                if name == 'go-env.txt':
                    (self.RELEASE / name).write_text('linux\namd64\nv1\n1\ngo1.25.5\nlocal\n')
                return {'log': name}

            @staticmethod
            def pure_helpers():
                return Helper()

            @staticmethod
            def verify_test_log(helper, name, packages, required):
                return {'packages_passed': list(packages)}

            def resolve(self, revision):
                if self.fail_at == 'identity':
                    return '0' * 40
                return revision

            def write_json(self, name, value):
                path = self.RELEASE / name
                if path.exists():
                    raise RuntimeError('evidence already exists')
                path.write_text(json.dumps(value))

            @staticmethod
            def file_sha(path, limit=1 << 30):
                return hashlib.sha256(Path(path).read_bytes()).hexdigest()

        args = types.SimpleNamespace(source_revision='a' * 40, script_revision='b' * 40,
                                     rust_library_sha256='c' * 64)
        for failure in ('reward', 'identity'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                release = Path(directory)
                builder = Builder(release, failure)
                with self.assertRaisesRegex(RuntimeError, 'injected|identity'):
                    s.prepare(builder, args)
                self.assertFalse((release / 'prepared.json').exists())
                self.assertTrue((release / 'base-prepared.json').is_file())
                failure_record = json.loads((release / 'fresh-preparation-failure.json').read_text())
                self.assertFalse(failure_record['prepared'])
                self.assertEqual(failure_record['phase'], 'post-base-reward-gate')


if __name__ == '__main__':
    unittest.main()

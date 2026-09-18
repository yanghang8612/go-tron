import argparse
import ast
import hashlib
import importlib.util
from pathlib import Path
import subprocess
import types
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / 'scripts/prepare_reward_trace_20260918.py'
spec = importlib.util.spec_from_file_location('prepare_reward_trace_tested', str(PATH))
s = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s)


class PrepareRewardTraceTests(unittest.TestCase):
    def test_pinned_builder_scope_and_native_contract(self):
        with mock.patch.object(s, 'REPO', ROOT):
            builder = s.load_builder()
        blob = subprocess.check_output(
            ['git', 'show', s.BUILDER_COMMIT + ':' + s.BUILDER_PATH], cwd=str(ROOT))
        self.assertEqual(hashlib.sha256(blob).hexdigest(), s.BUILDER_SHA)
        self.assertEqual(builder.BASE, s.BASE)
        self.assertEqual(builder.RELEASE, s.RELEASE)
        self.assertEqual(builder.SOURCE, s.RELEASE / 'source')
        self.assertEqual(builder.BINARY, s.RELEASE / s.BINARY_NAME)
        self.assertFalse(hasattr(builder, 'CHECKOUT'))
        self.assertEqual(builder.REQUIRED_SOURCE_CHANGES, {
            'cmd/reward-trace/main.go',
            'cmd/reward-trace/historical_withdraw.go',
            'cmd/reward-trace/historical_withdraw_test.go',
            s.SCRIPT_PATH,
            s.TEST_PATH,
        })
        self.assertIn('core/reward/voter_reward.go', builder.ALLOWED_SOURCE_CHANGES)
        self.assertIn(('core/reward', s.REWARD_GOLDEN), set(builder.REQUIRED_TESTS))
        for name in s.EXPORTER_TESTS:
            self.assertIn(('cmd/reward-trace', name), set(builder.REQUIRED_TESTS))
        env = s.build_environment(builder)
        self.assertEqual(env['CGO_ENABLED'], '1')
        self.assertEqual(env['GOAMD64'], 'v1')
        self.assertEqual(env['GOTOOLCHAIN'], 'local')
        self.assertEqual(env['GOFLAGS'], '-mod=readonly')
        self.assertEqual(env['GOENV'], 'off')
        self.assertEqual(env['GOWORK'], 'off')

    def test_bad_builder_pin_is_rejected_before_execution(self):
        with mock.patch.object(s.subprocess, 'check_output',
                               return_value=b'raise AssertionError("executed")'):
            with self.assertRaisesRegex(RuntimeError, 'Git bytes differ'):
                s.load_builder()

    def test_commands_build_only_reward_trace_and_run_required_native_tests(self):
        builder = types.SimpleNamespace(GO='/data/go/bin/go', BINARY=s.RELEASE / s.BINARY_NAME)
        commands = s.native_commands(builder)
        golden = commands['reward-golden']
        self.assertEqual(golden[0:5], ['/data/go/bin/go', 'test', '-json', '-p', '2'])
        self.assertIn('./core/reward', golden)
        self.assertIn('^' + s.REWARD_GOLDEN + '$', golden)
        focused = commands['exporter-focused']
        self.assertIn('./cmd/reward-trace', focused)
        for name in s.EXPORTER_TESTS:
            self.assertIn(name, focused[focused.index('-run') + 1])
        self.assertEqual(commands['full-packages'][5:7],
                         ['./core/reward', './cmd/reward-trace'])
        build = commands['build']
        self.assertEqual(build[-1], './cmd/reward-trace')
        self.assertEqual(build[build.index('-o') + 1], str(s.RELEASE / s.BINARY_NAME))
        all_args = '\n'.join(' '.join(command) for command in commands.values())
        self.assertNotIn('cmd/gtron', all_args)
        self.assertNotIn('sapling', all_args.lower())
        self.assertNotIn('systemctl', all_args)
        self.assertEqual(commands['help'], [str(s.RELEASE / s.BINARY_NAME), '-h'])

    def test_native_identity_fails_closed_on_toolchain_or_amd64_mode(self):
        def require(value, message):
            if not value:
                raise RuntimeError(message)

        builder = types.SimpleNamespace(require=require)
        expected = ['linux', 'amd64', 'v1', '1', 'go1.25.5', 'local']
        s.verify_native_identity(builder, 'go version go1.25.5 linux/amd64', expected)
        with self.assertRaisesRegex(RuntimeError, 'version differs'):
            s.verify_native_identity(builder, 'go version go1.27.1 linux/amd64', expected)
        wrong_mode = list(expected)
        wrong_mode[2] = 'v3'
        with self.assertRaisesRegex(RuntimeError, 'environment differs'):
            s.verify_native_identity(builder, 'go version go1.25.5 linux/amd64', wrong_mode)

    def test_focused_logs_require_each_named_go_test_pass(self):
        calls = []

        def verify(helper, name, packages, required=()):
            calls.append((name, packages, required))
            return {'required_tests': required}

        builder = types.SimpleNamespace(verify_test_log=verify)
        reward, exporter = s.verify_focused_test_logs(builder, object())
        self.assertEqual(calls[0], (
            'native-reward-golden.jsonl', ('core/reward',),
            (('core/reward', s.REWARD_GOLDEN),),
        ))
        self.assertEqual(calls[1][0:2],
                         ('native-exporter-focused.jsonl', ('cmd/reward-trace',)))
        self.assertEqual(set(calls[1][2]),
                         set(('cmd/reward-trace', name) for name in s.EXPORTER_TESTS))
        self.assertEqual(reward['required_tests'], calls[0][2])
        self.assertEqual(exporter['required_tests'], calls[1][2])

    def test_help_contract_matches_flag_definition_lines_not_usage_substrings(self):
        def require(value, message):
            if not value:
                raise RuntimeError(message)

        builder = types.SimpleNamespace(require=require)
        valid = '\n'.join('  -' + name + ' value' for name in s.HELP_FLAG_NAMES)
        s.verify_help_contract(builder, valid)
        misleading = '\n'.join('description mentions --' + name for name in s.HELP_FLAG_NAMES)
        with self.assertRaisesRegex(RuntimeError, 'definitions missing'):
            s.verify_help_contract(builder, misleading)

    def test_admission_binds_exact_commits_manifest_and_scope(self):
        source, ops = 'a' * 40, 'b' * 40
        script = b'checked-in builder'
        changed = [
            'A\tcmd/reward-trace/main.go',
            'A\tcmd/reward-trace/historical_withdraw.go',
            'A\tcmd/reward-trace/historical_withdraw_test.go',
            'A\t' + s.SCRIPT_PATH,
            'A\t' + s.TEST_PATH,
        ]
        builder = types.SimpleNamespace(
            BASE=s.BASE,
            HELPER_SHA='helper-sha',
            PURE_FUNCTIONS=('one', 'two'),
            ALLOWED_SOURCE_CHANGES=set(row.split('\t')[1] for row in changed),
            REQUIRED_SOURCE_CHANGES=set(row.split('\t')[1] for row in changed),
            full_hex=lambda value, length: value,
            resolve=lambda ref: {'refs/remotes/origin/master': ops}.get(ref, ref),
            repository_state=lambda: {'head': 'c' * 40, 'tracked_status': ''},
        )

        def require(value, message):
            if not value:
                raise RuntimeError(message)

        def git(*args):
            if args[0] == 'merge-base':
                return (s.BASE + '\n').encode()
            if args[0] == 'show':
                return script
            if args[0] == 'diff':
                return ('\n'.join(changed) + '\n').encode()
            raise AssertionError(args)

        builder.require = require
        builder.git = git
        helper = types.SimpleNamespace(
            read_regular=lambda *args, **kwargs: script,
            source_tree_manifest=lambda revision: ({s.SCRIPT_PATH: 'sha'}, {}),
        )
        args = argparse.Namespace(source_revision=source, script_revision=ops)
        record = s.reward_admission(builder, args, helper)
        self.assertEqual(record['source_commit'], source)
        self.assertEqual(record['script_commit'], ops)
        self.assertEqual(record['manifest'], {s.SCRIPT_PATH: 'sha'})
        self.assertEqual(record['changed_files'], sorted(builder.REQUIRED_SOURCE_CHANGES))

        changed.append('M\tcmd/gtron/main.go')
        with self.assertRaisesRegex(RuntimeError, 'outside reviewed reward-trace scope'):
            s.reward_admission(builder, args, helper)

    def test_expected_checkout_is_explicit_before_builder_load(self):
        fake = mock.Mock(SIGNALS=(), interrupted=object())
        captured = {}
        good = 'c' * 40
        argv = ['prepare', '--source-revision', 'a' * 40,
                '--script-revision', 'b' * 40, '--expected-checkout', good]
        with mock.patch.object(s, 'load_builder', return_value=fake), \
             mock.patch.object(s, 'prepare',
                               side_effect=lambda builder, args: captured.update(
                                   checkout=builder.CHECKOUT, args=args) or {'prepared': True}), \
             mock.patch.object(s.os, 'geteuid', return_value=0), \
             mock.patch.object(s.sys, 'platform', 'linux'), \
             mock.patch.object(s.sys, 'argv', argv), \
             mock.patch('builtins.print'):
            self.assertEqual(s.main(), 0)
        self.assertEqual(captured['checkout'], good)
        self.assertEqual(captured['args'].expected_checkout, good)

        with mock.patch.object(s, 'load_builder') as load, \
             mock.patch.object(s.os, 'geteuid', return_value=0), \
             mock.patch.object(s.sys, 'platform', 'linux'), \
             mock.patch.object(s.sys, 'argv', argv[:-1] + ['HEAD']):
            with self.assertRaisesRegex(RuntimeError, 'checkout identity'):
                s.main()
            load.assert_not_called()

    def test_python36_syntax_and_no_production_operations(self):
        for path in (PATH, Path(__file__)):
            ast.parse(path.read_text(), feature_version=(3, 6))
        text = PATH.read_text()
        for forbidden in ('systemctl', 'service gtron', 'pebbledb.New', '/main/datadir'):
            self.assertNotIn(forbidden, text)


if __name__ == '__main__':
    unittest.main()

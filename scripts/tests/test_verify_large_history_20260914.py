"""Small hermetic checks for native acceptance evidence and command boundaries."""
import ast
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / 'scripts/verify_large_history_20260914.py'
spec = importlib.util.spec_from_file_location('large_history_native_acceptance', str(PATH))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


def fixture_rows(kind):
    if kind == 'state':
        names = [runner.STATE_BENCH + '/degree=' + degree + '/mixed=' + mixed + '/' + operation + '/' + mode
                 for degree, mixed in (('32', 'false'), ('10000', 'false'), ('100000', 'false'), ('100000', 'true'))
                 for operation in ('Membership', 'NewEntry', 'DecodeEntry') for mode in ('Legacy', 'Arena')]
    else:
        names = [runner.RAWDB_BENCH + '/' + scenario + '/' + operation + '/' + mode
                 for scenario in ('repeated_seed', 'repeated_existing', 'single_large_existing_no_v2', 'shared_disabled')
                 for operation in ('planner_only', 'codec_and_planner') for mode in ('legacy', 'candidate')]
    lines = ['goos: linux', 'goarch: amd64', 'cpu: fixture']
    for name in names:
        for index in range(5):
            # Unit order intentionally differs from usual Go output. Include
            # scientific notation and custom units to prevent column indexing.
            lines.append('{}-2 100 {} allocs/op {} B/op {:.2e} ns/op 7.5 MB/s 123 raw_B/op'.format(
                name, index, index * 10, (index + 1) * 100.0))
    lines += ['PASS', 'ok fixture 1.0s']
    return '\n'.join(lines)


class ParserTests(unittest.TestCase):
    def test_complete_metrics_order_and_printable_scope(self):
        state = runner.parse_benchmarks(fixture_rows('state'), 'state')
        rawdb = runner.parse_benchmarks(fixture_rows('rawdb'), 'rawdb')
        self.assertEqual((state['case_count'], state['sample_count']), (24, 120))
        self.assertEqual((rawdb['case_count'], rawdb['sample_count']), (16, 80))
        case = next(iter(state['cases'].values()))
        self.assertEqual(case['metrics']['ns/op'], {'median': 300.0, 'min': 100.0, 'max': 500.0})
        self.assertEqual(case['metrics']['B/op'], {'median': 20.0, 'min': 0.0, 'max': 40.0})
        self.assertEqual(case['metrics']['allocs/op'], {'median': 2.0, 'min': 0.0, 'max': 4.0})
        selected = runner.compact_benchmark_summary(state, rawdb)
        self.assertEqual(len(selected), 10)
        self.assertEqual(sum('/degree=100000/mixed=false/DecodeEntry/' in name for name in selected), 2)
        self.assertFalse(any('/mixed=true/' in name or '/repeated_seed/' in name for name in selected))

    def test_missing_extra_or_malformed_evidence_rejected(self):
        original = fixture_rows('state')
        lines = original.splitlines()
        bad_inputs = [
            '\n'.join(lines[:3] + lines[4:]),
            original + '\n' + lines[3],
            original.replace('/Membership/Legacy-2', '/Unexpected/Legacy-2', 1),
            original.replace('1.00e+02 ns/op', 'nan ns/op', 1),
            original.replace('1.00e+02 ns/op', '1.00e+02 unknown-unit', 1),
            original.replace('Legacy-2', 'Legacy-4', 1),
            original.replace(' 0 allocs/op', ' -1 allocs/op', 1),
        ]
        for bad in bad_inputs:
            self.assertNotEqual(bad, original)
            with self.subTest(prefix=bad[:150]), self.assertRaises((RuntimeError, ValueError)):
                runner.parse_benchmarks(bad, 'state')


class BoundaryTests(unittest.TestCase):
    def test_failed_tests_stop_before_benchmarks_and_remain_incomplete(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / 'out'
            calls = []

            def fail_tests(name, *args):
                calls.append(name)
                if name == 'go-version':
                    return 'go version go1.25.5 linux/amd64\n'
                raise RuntimeError('test command failed')

            with patch.object(runner, 'OUTPUT', output), patch.object(runner, 'validate_source', return_value={'source_revision': 'a' * 40}), \
                    patch.object(runner, 'run_command', side_effect=fail_tests), \
                    patch.object(runner.sys, 'argv', [str(PATH), '--source-revision', 'a' * 40]), patch('builtins.print'):
                with self.assertRaisesRegex(RuntimeError, 'test command failed'):
                    runner.main()
            state = json.loads((output / 'native-summary.json').read_text())
            self.assertFalse(state['complete'])
            self.assertIn('test command failed', state['error'])
            self.assertEqual(calls, ['go-version', 'state-actuator-tests'])
            self.assertNotIn('printable_benchmarks', state)
            for invalid_version in ('go version go1.25.5 darwin/arm64\n',
                                    'go version go1.25.5 linux/arm64\n'):
                with self.subTest(version=invalid_version), patch.object(runner, 'OUTPUT', output), \
                        patch.object(runner, 'validate_source', return_value={'source_revision': 'a' * 40}), \
                        patch.object(runner, 'run_command', return_value=invalid_version) as commands, \
                        patch.object(runner.sys, 'argv', [str(PATH), '--source-revision', 'a' * 40]), patch('builtins.print'):
                    with self.assertRaisesRegex(RuntimeError, 'exactly go1.25.5 linux/amd64'):
                        runner.main()
                    self.assertEqual(commands.call_count, 1)
                    self.assertEqual(commands.call_args[0][0], 'go-version')
                    rejected = json.loads((output / 'native-summary.json').read_text())
                    self.assertFalse(rejected['complete'])
                    self.assertNotIn('state_benchmark', rejected)

    def test_version_identity_and_command_order_without_executing_go(self):
        with tempfile.TemporaryDirectory() as root:
            release = Path(root) / 'release'
            source = release / 'source'
            source.mkdir(parents=True)
            (source / 'go.mod').write_text('module fixture\n')
            (release / 'source-commit').write_text('a' * 40 + '\n')
            calls = []

            def fake_command(name, command, env, directory, timeout, state):
                calls.append((name, command, dict(env), timeout))
                if name == 'go-version':
                    return 'go version go1.25.5 linux/amd64\n'
                if name == 'state-actuator-tests':
                    return 'ok fixture\n'
                return fixture_rows('state' if name == 'state-benchmark' else 'rawdb')

            output = Path(root) / 'out'
            with patch.object(runner, 'RELEASE', release), patch.object(runner, 'SOURCE', source), \
                    patch.object(runner, 'OUTPUT', output), patch.object(runner, 'run_command', side_effect=fake_command), \
                    patch.object(runner.sys, 'argv', [str(PATH), '--source-revision', 'a' * 40]), \
                    patch.dict(os.environ, {'GOROOT': '/wrong/toolchain'}), patch('builtins.print'):
                self.assertEqual(runner.main(), 0)
                record = json.loads((output / 'native-summary.json').read_text())
                self.assertTrue(record['complete'])
                with self.assertRaisesRegex(RuntimeError, 'source-commit'):
                    runner.validate_source('b' * 40)
            self.assertEqual([call[0] for call in calls],
                             ['go-version', 'state-actuator-tests', 'state-benchmark', 'rawdb-benchmark'])
            self.assertEqual(calls[1][1], [runner.GO, 'test', '-p', '2', '-tags', 'sapling',
                                           './core/state', './actuator', '-count=1', '-timeout=300s'])
            for name, command, env, timeout in calls:
                self.assertEqual(command[0], '/data/go/bin/go')
                self.assertFalse(any('systemctl' in item or '--datadir' in item for item in command))
                self.assertNotIn('GOROOT', env)
                self.assertEqual({key: env[key] for key in runner.ENV_OVERRIDES}, runner.ENV_OVERRIDES)
                if name.endswith('-benchmark'):
                    self.assertIn('-count=5', command)
                    self.assertIn('-benchtime=200ms', command)
                    self.assertIn('-timeout=180s', command)
                    self.assertIn('^$', command)
                    self.assertIn('-benchmem', command)

    def test_python36_syntax_and_runner_has_no_shell_service_calls(self):
        source = PATH.read_text()
        # Grammar compatibility, without invoking the native or Go paths.
        tree = ast.parse(source, feature_version=(3, 6))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                self.assertFalse(any(keyword.arg == 'shell' for keyword in node.keywords))
        self.assertNotIn('capture_output=', source)
        self.assertNotIn('text=True', source)
        self.assertNotIn('systemctl', source)


if __name__ == '__main__':
    unittest.main(verbosity=2)

"""Local-only checks: parse complete evidence and fake every Go subprocess."""
import ast
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / 'scripts/verify_commitment_prefetch_20260914.py'
spec = importlib.util.spec_from_file_location('prefetch_acceptance', str(PATH))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


def fixture_rows(kind='prefetch'):
    # Independent case names; never derive the expected fixture from runner configuration.
    if kind == 'prefetch':
        benchmark = 'BenchmarkCommitmentPrefetchOwnershipFullSession'
        paths = ['{}/{}/{}'.format(key, workload, mode)
                 for key in ('legacy', 'delta')
                 for workload in ('cold_same', 'cold_shrink', 'cold_grow', 'hot_same',
                                  'hot_shrink_grow', 'no_flush', 'direct_get')
                 for mode in ('legacy', 'candidate')]
    elif kind == 'event_frontier':
        benchmark = 'BenchmarkEventFrontierAllocation'
        paths = ['{}/{}/{}'.format(operation, catalog, variant)
                 for operation in ('Frontier', 'PlannerCovered', 'PlannerGap', 'StageWrite')
                 for catalog in ('Small', 'Mixed73113')
                 for variant in ('Frozen833', 'Candidate')]
    else:
        raise AssertionError('unexpected fixture kind')
    lines = ['goos: linux', 'goarch: amd64']
    for path in paths:
        for i in range(5):
            lines.append('{}-2 100 {} allocs/op 32 durable_reads/op {} B/op {:.2e} ns/op'.format(
                benchmark + '/' + path, i, i * 10, (i + 1) * 100.0))
    return '\n'.join(lines + ['PASS', 'ok fixture 1.0s'])


def pinned_helper():
    with patch.object(runner, 'REPO', ROOT):
        return runner.load_helper()


class ParserTests(unittest.TestCase):
    def test_28_cases_140_rows_and_named_units(self):
        result = runner.parse_benchmarks(fixture_rows(), 'prefetch')
        self.assertEqual((result['case_count'], result['sample_count'], result['repeats_per_case']), (28, 140, 5))
        case = next(iter(result['cases'].values()))
        self.assertEqual(case['metrics']['ns/op'], {'median': 300.0, 'min': 100.0, 'max': 500.0})
        self.assertEqual(case['metrics']['B/op'], {'median': 20.0, 'min': 0.0, 'max': 40.0})
        self.assertEqual(case['metrics']['allocs/op'], {'median': 2.0, 'min': 0.0, 'max': 4.0})
        self.assertEqual(case['metrics']['durable_reads/op']['median'], 32.0)
        event = runner.parse_benchmarks(fixture_rows('event_frontier'), 'event_frontier')
        self.assertEqual((event['case_count'], event['sample_count'], event['repeats_per_case']), (16, 80, 5))
        self.assertEqual(next(iter(event['cases'].values()))['metrics']['ns/op']['median'], 300.0)

    def test_missing_extra_and_malformed_rows_never_pass(self):
        original = fixture_rows()
        lines = original.splitlines()
        invalid = [
            '\n'.join(lines[:2] + lines[3:]), original + '\n' + lines[2],
            original.replace('/cold_same/', '/unknown/', 1),
            original.replace('/legacy-2', '/legacy-4', 1),
            original.replace('1.00e+02 ns/op', 'nan ns/op', 1),
            original.replace('1.00e+02 ns/op', '0 ns/op', 1),
            original.replace('1.00e+02 ns/op', '1.00e+02 unknown/op', 1),
            original.replace(' 0 allocs/op', ' -1 allocs/op', 1),
            original.replace(' 100 ', ' 0 ', 1),
            original.replace(' 32 durable_reads/op', ' 32 B/op', 1),
            original.replace('32 durable_reads/op', '32 other/op', 1),
            original.replace('32 durable_reads/op', '32', 1),
        ]
        for index, text in enumerate(invalid):
            with self.subTest(index=index), self.assertRaises((RuntimeError, ValueError)):
                runner.parse_benchmarks(text, 'prefetch')


class NativeBoundaryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.release = self.root / 'release'
        self.source = self.release / 'source'
        self.output = self.root / 'output'
        self.source.mkdir(parents=True)
        (self.source / 'go.mod').write_text('module fixture\n')
        (self.release / 'source-commit').write_text('a' * 40 + '\n')
        self.h = pinned_helper()
        self.h.RELEASE, self.h.SOURCE, self.h.OUTPUT = self.release, self.source, self.output
        self.calls = []
        self.version = 'go version go1.25.5 linux/amd64\n'
        self.bench = {'prefetch': fixture_rows(), 'event_frontier': fixture_rows('event_frontier')}
        self.fail_tests = False
        self.timeout_tests = False
        self.interrupt_tests = False
        self.change_source = False
        for patcher in (
                patch.object(runner, 'OUTPUT', self.output),
                patch.object(runner, 'load_helper', return_value=self.h),
                patch.object(runner.sys, 'argv', [str(PATH), '--source-revision', 'a' * 40]),
                patch.object(self.h.subprocess, 'Popen', side_effect=self.fake_process),
                patch('builtins.print')):
            patcher.start()
            self.addCleanup(patcher.stop)

    def fake_process(self, command, **kwargs):
        self.calls.append((command, kwargs))
        self.assertEqual(command[0], '/data/go/bin/go')
        self.assertEqual(kwargs['cwd'], str(self.source))
        self.assertIs(kwargs['start_new_session'], True)
        self.assertEqual(kwargs['stdin'], subprocess.DEVNULL)
        self.assertFalse(any('--datadir' in value or 'systemctl' in value for value in command))
        env = kwargs['env']
        self.assertEqual({k: env[k] for k in self.h.ENV_OVERRIDES}, self.h.ENV_OVERRIDES)
        self.assertNotIn('GOROOT', env)
        tests = './core/state' in command
        kind = 'prefetch' if './core/blockbuffer' in command else 'event_frontier'
        payload = self.version if command[1] == 'version' else ('ok fixture\n' if tests else self.bench[kind])
        kwargs['stdout'].write(payload.encode())
        kwargs['stderr'].write(b'fixture stderr\n')
        if self.change_source and not tests and command[1] != 'version':
            (self.release / 'source-commit').write_text('b' * 40 + '\n')
        owner = self
        class Process:
            pid = 123456
            returncode = None
            waits = 0
            def wait(self, timeout=None):
                self.waits += 1
                if tests and self.waits == 1:
                    if owner.timeout_tests:
                        raise subprocess.TimeoutExpired(command, timeout)
                    if owner.interrupt_tests:
                        signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
                self.returncode = -15 if tests and (owner.timeout_tests or owner.interrupt_tests) else (1 if tests and owner.fail_tests else 0)
                return self.returncode
        return Process()

    def summary(self):
        return json.loads((self.output / 'native-summary.json').read_text())

    def test_exact_source_order_env_and_complete_command_artifacts(self):
        with patch.dict(os.environ, {'GOROOT': '/wrong/toolchain'}):
            self.assertEqual(runner.main(), 0)
        record = self.summary()
        self.assertTrue(record['complete'])
        self.assertEqual(record['benchmarks']['prefetch']['sample_count'], 140)
        self.assertEqual(record['benchmarks']['event_frontier']['sample_count'], 80)
        self.assertEqual(record['helper_sha256'], runner.HELPER_SHA)
        self.assertEqual(self.calls[1][0], ['/data/go/bin/go', 'test', '-p', '2', '-tags', 'sapling',
            './core/blockbuffer', './core/state/domains', './core/state', './core/state/snapshots', './core/state/pruning', '-count=1', '-timeout=300s'])
        self.assertEqual(self.calls[2][0], ['/data/go/bin/go', 'test', '-p', '2', '-tags', 'sapling', './core/blockbuffer',
            '-run', '^$', '-bench', '^BenchmarkCommitmentPrefetchOwnershipFullSession$', '-benchmem', '-benchtime=200ms', '-count=5', '-timeout=180s'])
        self.assertEqual(self.calls[3][0], ['/data/go/bin/go', 'test', '-p', '2', '-tags', 'sapling', './core/state/snapshots',
            '-run', '^$', '-bench', '^BenchmarkEventFrontierAllocation$', '-benchmem', '-benchtime=200ms', '-count=5', '-timeout=180s'])
        for command in record['commands']:
            self.assertTrue(command['completed'])
            self.assertEqual(command['returncode'], 0)
            self.assertTrue(command['started_utc'] and command['finished_utc'])
            for name in ('stdout', 'stderr'):
                raw = Path(command[name]).read_bytes()
                self.assertEqual(command[name + '_bytes'], len(raw))
                self.assertEqual(command[name + '_sha256'], hashlib.sha256(raw).hexdigest())
        original_run = record['output_directory']
        self.assertEqual(runner.main(), 0)
        self.assertNotEqual(self.summary()['output_directory'], original_run)
        self.assertTrue((Path(original_run) / 'native-summary.json').is_file())

    def test_wrong_source_or_native_arch_and_changed_source_refuse(self):
        for invalid in ('a' * 40 + '\n', 'A' * 40, 'a' * 39):
            with self.subTest(source=invalid), patch.object(runner.sys, 'argv',
                    [str(PATH), '--source-revision', invalid]), patch.object(runner, 'load_helper') as helper:
                with self.assertRaisesRegex(RuntimeError, 'exact lowercase 40-hex'):
                    runner.main()
                helper.assert_not_called()
        (self.release / 'source-commit').write_text('b' * 40 + '\n')
        with self.assertRaisesRegex(RuntimeError, 'source-commit'):
            runner.main()
        self.assertEqual(self.calls, [])
        (self.release / 'source-commit').write_text('a' * 40 + '\n')
        for version in ('go version go1.25.5 darwin/arm64', 'go version go1.25.5 linux/arm64', 'go version go1.25.6 linux/amd64'):
            self.version = version
            self.calls.clear()
            with self.subTest(version=version), self.assertRaisesRegex(RuntimeError, 'exactly go1.25.5 linux/amd64'):
                runner.main()
            self.assertFalse(self.summary()['complete'])
            self.assertEqual(len(self.calls), 1)
        self.version = 'go version go1.25.5 linux/amd64'
        self.change_source = True
        with self.assertRaisesRegex(RuntimeError, 'source-commit'):
            runner.main()
        self.assertFalse(self.summary()['complete'])

    def test_failed_tests_stop_and_incomplete_benchmark_is_not_complete(self):
        self.fail_tests = True
        with self.assertRaisesRegex(RuntimeError, 'returned 1'):
            runner.main()
        self.assertEqual(len(self.calls), 2)
        self.assertFalse(self.summary()['complete'])
        self.fail_tests = False
        self.bench['prefetch'] = '\n'.join(self.bench['prefetch'].splitlines()[1:3] + self.bench['prefetch'].splitlines()[4:])
        with self.assertRaisesRegex(RuntimeError, 'expected 5 samples'):
            runner.main()
        record = self.summary()
        self.assertFalse(record['complete'])
        self.assertEqual([r['returncode'] for r in record['commands']], [0, 0, 0])
        self.bench['prefetch'] = fixture_rows()
        self.bench['event_frontier'] = self.bench['event_frontier'] + '\n' + self.bench['event_frontier'].splitlines()[2]
        with self.assertRaisesRegex(RuntimeError, 'expected 5 samples'):
            runner.main()
        record = self.summary()
        self.assertFalse(record['complete'])
        self.assertEqual([r['returncode'] for r in record['commands']], [0, 0, 0, 0])
        self.assertEqual(record['benchmarks']['prefetch']['sample_count'], 140)
        self.assertNotIn('event_frontier', record['benchmarks'])

    def test_lock_rejects_second_runner_before_any_go(self):
        self.output.mkdir()
        with open(str(self.output / 'runner.lock'), 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                runner.main()
        self.assertEqual(self.calls, [])
        self.assertEqual(list(self.output.iterdir()), [self.output / 'runner.lock'])

    def test_timeout_and_signal_clean_process_group_and_record_failure(self):
        handlers = {n: signal.getsignal(n) for n in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
        for interrupted in (False, True):
            self.timeout_tests, self.interrupt_tests = not interrupted, interrupted
            with self.subTest(interrupted=interrupted), patch.object(runner.os, 'killpg') as kill:
                with self.assertRaises((subprocess.TimeoutExpired, InterruptedError)):
                    runner.main()
                self.assertEqual([call[0] for call in kill.call_args_list],
                                 [(123456, signal.SIGTERM), (123456, signal.SIGKILL)])
            record = self.summary()
            self.assertFalse(record['complete'])
            self.assertEqual(len(record['commands']), 2)
            failed = record['commands'][-1]
            self.assertFalse(failed['completed'])
            self.assertEqual(failed['returncode'], -15)
            self.assertIn('stderr_sha256', failed)
            self.assertEqual({n: signal.getsignal(n) for n in handlers}, handlers)


class HelperTests(unittest.TestCase):
    def test_pinned_sha_precedes_exec_and_modules_are_isolated(self):
        with patch.object(runner.subprocess, 'check_output', return_value=b'raise AssertionError("executed")'):
            with self.assertRaisesRegex(RuntimeError, 'helper SHA changed'):
                runner.load_helper()
        first, second = pinned_helper(), pinned_helper()
        self.assertIsNot(first, second)
        first.SOURCE = Path('/changed')
        self.assertEqual(second.SOURCE, runner.SOURCE)
        self.assertEqual(second.RELEASE, runner.RELEASE)
        self.assertEqual(second.OUTPUT, runner.OUTPUT)
        self.assertIsNot(first.run_command.__globals__, second.run_command.__globals__)
        self.assertIs(second.stop_process_group, runner.stop_process_group)

    def test_python36_and_no_shell_or_service_commands(self):
        text = PATH.read_text()
        tree = ast.parse(text, feature_version=(3, 6))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                self.assertFalse(any(keyword.arg == 'shell' for keyword in node.keywords))
        self.assertNotIn('capture_output=', text)
        self.assertNotIn('text=True', text)
        self.assertNotIn('systemctl', text)


if __name__ == '__main__':
    unittest.main(verbosity=2)

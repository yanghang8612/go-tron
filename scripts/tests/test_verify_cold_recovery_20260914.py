"""Local-only native JSON acceptance checks; every Go subprocess is fake."""
import ast
import copy
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
PATH = ROOT / 'scripts/verify_cold_recovery_20260914.py'
spec = importlib.util.spec_from_file_location('cold_recovery_acceptance', str(PATH))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
REQUIRED = {
    './core/state/snapshots': ('TestSnapshotRecovery',),
    './core/state/pruning': ('TestPruningRecovery',),
    './core/maintenance': ('TestMaintenanceRecovery',),
    './cmd/gtron': ('TestHistoryProbe',),
}
FULL = ('./core/state/snapshots', './core/state/pruning', './core/maintenance')


def fixture_events(packages=FULL):
    rows = [{'Action': 'build-output', 'ImportPath': 'fixture/native/dependency',
             'Output': 'compiler diagnostic\n'}]
    for package in packages:
        name = 'github.com/tronprotocol/go-tron' + package[1:]
        def add(action, test=None):
            row = {'Time': '2026-09-14T10:00:00Z', 'Action': action, 'Package': name}
            if test is not None:
                row['Test'] = test
            rows.append(row)
        add('start')
        test = REQUIRED[package][0]
        add('run', test)
        add('pause', test)
        add('cont', test)
        add('run', test + '/boundary')
        add('output', test + '/boundary')
        add('pass', test + '/boundary')
        add('pass', test)
        # Full package Go tests also run fuzz seed corpora. Noncritical gated
        # tests may skip, but no skipped required test may satisfy acceptance.
        add('run', 'FuzzExisting')
        add('run', 'FuzzExisting/seed#0')
        add('pass', 'FuzzExisting/seed#0')
        add('pass', 'FuzzExisting')
        add('run', 'TestOptional')
        add('skip', 'TestOptional')
        add('output')
        add('pass')
    return rows


def encoded(rows):
    return '\n'.join(json.dumps(row) for row in rows) + '\n'


def pinned_helper():
    with patch.object(runner, 'REPO', ROOT):
        return runner.load_helper()


class ParserTests(unittest.TestCase):
    def setUp(self):
        binding = patch.object(runner, 'REQUIRED_TESTS', REQUIRED)
        binding.start()
        self.addCleanup(binding.stop)

    def test_complete_packages_required_tests_parallel_and_fuzz(self):
        report = runner.parse_test_json(encoded(fixture_events()), FULL)
        self.assertEqual(report['package_count'], 3)
        self.assertEqual(report['build_import_paths'], ['fixture/native/dependency'])
        self.assertEqual(report['build_output_events'], 1)
        for package, expected in REQUIRED.items():
            if package == './cmd/gtron':
                continue
            item = report['packages']['github.com/tronprotocol/go-tron' + package[1:]]
            self.assertEqual(item['required_passed'], list(expected))
            self.assertEqual(item['skipped'], ['TestOptional'])
            self.assertIn('FuzzExisting/seed#0', item['passed'])

    def test_missing_required_skipped_failures_and_malformed_stream_rejected(self):
        rows = fixture_events()
        variants = []
        variants.append([row for row in rows if row.get('Test') != 'TestSnapshotRecovery'])
        changed = copy.deepcopy(rows)
        next(row for row in changed if row.get('Test') == 'TestSnapshotRecovery' and row['Action'] == 'pass')['Action'] = 'skip'
        variants.append(changed)
        changed = copy.deepcopy(rows)
        next(row for row in changed if row.get('Test') == 'TestOptional' and row['Action'] == 'skip')['Action'] = 'fail'
        variants.append(changed)
        variants.extend([rows[:-1], rows + [rows[-1]], [rows[2]] + rows, rows + [{'Action': 'pass', 'Package': 'other'}],
                         rows + [{'Action': 'build-fail', 'ImportPath': 'fixture/native/dependency'}]])
        changed = copy.deepcopy(rows)
        changed.insert(3, dict(changed[2]))
        variants.append(changed)
        for variant in variants:
            with self.subTest(variant=variant[:2]), self.assertRaises(RuntimeError):
                runner.parse_test_json(encoded(variant), FULL)
        for value in ('', 'ok fixture\n', '[]\n', encoded(rows) + '{', encoded(rows) + json.dumps({'Action': 'bench'})):
            with self.subTest(value=value[-50:]), self.assertRaises(RuntimeError):
                runner.parse_test_json(value, FULL)

    def test_scope_pending_or_filter_mismatch_refuses_before_helper(self):
        with patch.object(runner.sys, 'argv', [str(PATH), '--source-revision', 'a' * 40]), \
                patch.object(runner, 'TESTS_FROZEN', False), patch.object(runner, 'load_helper') as load:
            with self.assertRaisesRegex(RuntimeError, 'test scope not frozen'):
                runner.main()
            load.assert_not_called()
        bad = dict(REQUIRED)
        bad['./cmd/gtron'] = ('TestUnrelatedProbe',)
        with patch.object(runner, 'TESTS_FROZEN', True), patch.object(runner, 'REQUIRED_TESTS', bad):
            with self.assertRaisesRegex(RuntimeError, 'filter misses'):
                runner.validate_test_scope()


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
        self.payloads = {'full': encoded(fixture_events()), 'cmd': encoded(fixture_events(('./cmd/gtron',)))}
        self.fail_tests = False
        self.timeout_tests = False
        self.interrupt_tests = False
        self.change_source = False
        for binding in (
                patch.object(runner, 'OUTPUT', self.output),
                patch.object(runner, 'load_helper', return_value=self.h),
                patch.object(runner, 'REQUIRED_TESTS', REQUIRED),
                patch.object(runner, 'TESTS_FROZEN', True),
                patch.object(runner.sys, 'argv', [str(PATH), '--source-revision', 'a' * 40]),
                patch.object(self.h.subprocess, 'Popen', side_effect=self.fake_process),
                patch('builtins.print')):
            binding.start()
            self.addCleanup(binding.stop)

    def fake_process(self, command, **kwargs):
        self.calls.append((command, kwargs))
        self.assertEqual(command[0], '/data/go/bin/go')
        self.assertEqual(kwargs['cwd'], str(self.source))
        self.assertIs(kwargs['start_new_session'], True)
        self.assertEqual(kwargs['stdin'], subprocess.DEVNULL)
        self.assertFalse(any('--datadir' in value or 'systemctl' in value or '-bench' in value for value in command))
        self.assertEqual({k: kwargs['env'][k] for k in self.h.ENV_OVERRIDES}, self.h.ENV_OVERRIDES)
        self.assertNotIn('GOROOT', kwargs['env'])
        full = './core/state/snapshots' in command
        cmd = './cmd/gtron' in command
        payload = self.version if command[1] == 'version' else self.payloads['full' if full else 'cmd']
        kwargs['stdout'].write(payload.encode())
        kwargs['stderr'].write(b'fixture stderr\n')
        if self.change_source and cmd:
            (self.release / 'source-commit').write_text('b' * 40 + '\n')
        owner = self
        class Process:
            pid = 123456
            returncode = None
            waits = 0
            def wait(self, timeout=None):
                self.waits += 1
                if full and self.waits == 1:
                    if owner.timeout_tests:
                        raise subprocess.TimeoutExpired(command, timeout)
                    if owner.interrupt_tests:
                        signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
                self.returncode = -15 if full and (owner.timeout_tests or owner.interrupt_tests) else (1 if full and owner.fail_tests else 0)
                return self.returncode
        return Process()

    def summary(self):
        return json.loads((self.output / 'native-summary.json').read_text())

    def test_exact_command_scope_env_and_complete_unique_evidence(self):
        with patch.dict(os.environ, {'GOROOT': '/wrong/toolchain'}):
            self.assertEqual(runner.main(), 0)
        record = self.summary()
        self.assertTrue(record['complete'])
        self.assertEqual(record['tests']['related-package-tests']['package_count'], 3)
        self.assertEqual(record['tests']['cmd-history-tests']['package_count'], 1)
        self.assertEqual(self.calls[1][0], ['/data/go/bin/go', 'test', '-json', '-p', '2', '-tags', 'sapling',
            './core/state/snapshots', './core/state/pruning', './core/maintenance', '-count=1', '-timeout=300s'])
        self.assertEqual(self.calls[2][0], ['/data/go/bin/go', 'test', '-json', '-p', '2', '-tags', 'sapling',
            './cmd/gtron', '-run', 'Test.*History', '-count=1', '-timeout=300s'])
        self.assertEqual(record['helper_sha256'], runner.HELPER_SHA)
        for command in record['commands']:
            self.assertTrue(command['completed'])
            self.assertEqual(command['returncode'], 0)
            self.assertTrue(command['started_utc'] and command['finished_utc'])
            for name in ('stdout', 'stderr'):
                raw = Path(command[name]).read_bytes()
                self.assertEqual(command[name + '_bytes'], len(raw))
                self.assertEqual(command[name + '_sha256'], hashlib.sha256(raw).hexdigest())
        original = record['output_directory']
        self.assertEqual(runner.main(), 0)
        self.assertNotEqual(self.summary()['output_directory'], original)
        self.assertTrue((Path(original) / 'native-summary.json').is_file())

    def test_wrong_source_or_native_arch_and_source_change_refuse(self):
        for invalid in ('a' * 40 + '\n', 'A' * 40, 'a' * 39):
            with self.subTest(source=invalid), patch.object(runner.sys, 'argv', [str(PATH), '--source-revision', invalid]), \
                    patch.object(runner, 'load_helper') as load:
                with self.assertRaisesRegex(RuntimeError, 'exact lowercase 40-hex'):
                    runner.main()
                load.assert_not_called()
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

    def test_failed_command_and_incomplete_json_never_complete(self):
        self.fail_tests = True
        with self.assertRaisesRegex(RuntimeError, 'returned 1'):
            runner.main()
        self.assertFalse(self.summary()['complete'])
        self.assertEqual(len(self.calls), 2)
        self.fail_tests = False
        self.payloads['full'] = encoded(fixture_events()[:-1])
        with self.assertRaisesRegex(RuntimeError, 'package pass missing'):
            runner.main()
        self.assertFalse(self.summary()['complete'])
        self.payloads['full'] = encoded(fixture_events())
        rows = fixture_events(('./cmd/gtron',))
        next(row for row in rows if row.get('Test') == 'TestHistoryProbe' and row['Action'] == 'pass')['Action'] = 'skip'
        self.payloads['cmd'] = encoded(rows)
        with self.assertRaisesRegex(RuntimeError, 'required tests did not execute'):
            runner.main()
        record = self.summary()
        self.assertFalse(record['complete'])
        self.assertEqual([r['returncode'] for r in record['commands']], [0, 0, 0])
        self.assertEqual(record['tests']['related-package-tests']['package_count'], 3)
        self.assertNotIn('cmd-history-tests', record['tests'])

    def test_lock_refuses_second_runner_before_go(self):
        self.output.mkdir()
        with open(str(self.output / 'runner.lock'), 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                runner.main()
        self.assertEqual(self.calls, [])

    def test_timeout_and_signal_cleanup_and_durable_failure(self):
        handlers = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
        for interrupted in (False, True):
            self.timeout_tests, self.interrupt_tests = not interrupted, interrupted
            with self.subTest(interrupted=interrupted), patch.object(runner.os, 'killpg') as kill:
                with self.assertRaises((subprocess.TimeoutExpired, InterruptedError)):
                    runner.main()
                self.assertEqual([call[0] for call in kill.call_args_list], [(123456, signal.SIGTERM), (123456, signal.SIGKILL)])
            record = self.summary()
            self.assertFalse(record['complete'])
            failed = record['commands'][-1]
            self.assertEqual(failed['returncode'], -15)
            self.assertFalse(failed['completed'])
            self.assertIn('stderr_sha256', failed)
            self.assertEqual({number: signal.getsignal(number) for number in handlers}, handlers)


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

    def test_python36_no_shell_and_declared_test_names_exist_when_frozen(self):
        text = PATH.read_text()
        tree = ast.parse(text, feature_version=(3, 6))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                self.assertFalse(any(keyword.arg == 'shell' for keyword in node.keywords))
        self.assertNotIn('capture_output=', text)
        self.assertNotIn('text=True', text)
        self.assertNotIn('systemctl', text)
        if runner.TESTS_FROZEN:
            runner.validate_test_scope()
            for package, tests in runner.REQUIRED_TESTS.items():
                source = '\n'.join(path.read_text() for path in (ROOT / package[2:]).glob('*_test.go'))
                for test in tests:
                    self.assertIn('func ' + test + '(t *testing.T)', source)
        else:
            self.assertEqual(runner.REQUIRED_TESTS, {})


if __name__ == '__main__':
    unittest.main(verbosity=2)

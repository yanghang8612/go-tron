import ast
import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import signal
import statistics
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / 'benchmark_history_cold_20260915.py'


def load_module():
    spec = importlib.util.spec_from_file_location('cold_benchmark_test_' + os.urandom(6).hex(), str(SCRIPT))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ColdBenchmarkWrapperTest(unittest.TestCase):
    def setUp(self):
        self.module = load_module()
        self.temp = tempfile.TemporaryDirectory(dir='/private/tmp')
        self.root = Path(self.temp.name).resolve()
        self.binary = self.root / 'diagnostic'
        self.binary.write_bytes(b'fixed diagnostic fixture, never executed\n')
        self.binary.chmod(0o700)
        self.input = self.root / 'input'
        self.input.mkdir(mode=0o700)
        (self.input / 'pebble').mkdir(mode=0o700)
        (self.input / 'pebble' / 'table.sst').write_bytes(b'private physical input')
        self.source = self.root / 'production' / 'gtron' / 'chaindata'
        self.source.mkdir(parents=True)
        self.export = {'complete': True, 'content_verified': False, 'stop_reason': 'complete',
                       'from_block': 2, 'to_block': 3, 'from_tx_num': 20, 'to_tx_num': 30,
                       'blocks': 2, 'physical_rows': 1, 'physical_bytes': 22,
                       'declared_decoded_bytes': 30, 'entries': [], 'block_details': [],
                       'codecs': {}, 'manifest_sha256': 'a' * 64}
        self.manifest = {'version': 1, 'source_chaindata': str(self.source),
                         'created_utc': '2026-09-15T03:00:00Z', 'finish_block': 3,
                         'covered_block': 1, 'export': self.export}
        (self.input / 'manifest.json').write_text(json.dumps(self.manifest))
        self.args = types.SimpleNamespace(binary=self.binary, binary_sha=self.module.file_sha(self.binary),
                                          input_dir=self.input, manifest_sha=self.module.file_sha(self.input / 'manifest.json'),
                                          output_dir=self.root / 'output')
        self.digest = {'rows': 3, 'payload_bytes': 45, 'prev_bytes': 30, 'max_prev_bytes': 10,
                       'large_prev_rows_ge_128kib': 0, 'large_prev_bytes_ge_128kib': 0,
                       'delegation_rows': 3, 'delegation_prev_bytes': 30,
                       'row_sha256': 'b' * 64, 'tx_ranges': 2, 'tx_range_sha256': 'c' * 64}
        self.old_umask = os.umask(0o077)
        os.umask(self.old_umask)

    def tearDown(self):
        os.umask(self.old_umask)
        self.temp.cleanup()

    def argv(self):
        return [str(SCRIPT), '--binary', str(self.args.binary), '--binary-sha', self.args.binary_sha,
                '--input-dir', str(self.args.input_dir), '--manifest-sha', self.args.manifest_sha,
                '--output-dir', str(self.args.output_dir)]

    def main(self):
        with mock.patch.object(sys, 'argv', self.argv()), contextlib.redirect_stdout(io.StringIO()):
            self.module.main()

    def report(self, output, mode='defensive', profile=False):
        rows = []
        refs = [{'dataset': 'state-domain-change', 'kind': kind, 'from_tx_num': 20, 'to_tx_num': 30,
                 'aggregation_steps': 1, 'path': 'history/state-domain-change-range.' + suffix,
                 'size': 10, 'checksum': 'sha256:' + 'd' * 64}
                for kind, suffix in [('history', 'seg'), ('accessor', 'kv'), ('inverted', 'ef')]]
        for i in range(1 if profile else 3):
            rows.append({'iteration': i + 1, 'build_wall_nanos': (i + 1) * 1000000000,
                         'build_allocated_bytes': 1000, 'build_allocations': 10,
                         'build_gc_cycles': 0, 'output_bytes': 30, 'refs': refs,
                         'verification_wall_nanos': 100, 'digest': dict(self.digest), 'equivalent': True})
        result = {'version': 1, 'complete': True, 'physical_verified': True,
                  'manifest_file_sha256': self.args.manifest_sha,
                  'options': {'input_dir': str(self.input), 'output_dir': str(output),
                              'iterations': len(rows), 'max_duration_ns': 300000000000,
                              'copy_mode': mode, 'cpu_profile': profile, 'compression_format': 'auto'},
                  'compression_format': 'auto', 'gomaxprocs': 2, 'go_version': 'go1.25.5',
                  'goos': 'linux', 'goarch': 'amd64', 'source_digest': dict(self.digest),
                  'export': self.export, 'iterations': rows}
        if profile:
            result['cpu_profile_sha256'] = hashlib.sha256(b'profile fixture').hexdigest()
            result['cpu_profile_scope'] = 'iteration 1 production trio build only; process-wide'
        return result

    def fake_popen(self, mutate=None, code=0, wait_error=None):
        def launch(command, **kwargs):
            self.last_command, self.last_kwargs = command, kwargs
            output = Path(command[command.index('--output-dir') + 1])
            mode = command[command.index('--copy-mode') + 1]
            profile = '--cpu-profile' in command
            output.mkdir(mode=0o700)
            data = self.report(output, mode, profile)
            if mutate:
                mutate(data)
            (output / 'report.json').write_text(json.dumps(data))
            if profile:
                (output / 'cpu.pprof').write_bytes(b'profile fixture')
            child = mock.Mock()
            child.pid = 987654321
            child.poll.return_value = code
            child.wait.side_effect = wait_error if wait_error is not None else None
            child.wait.return_value = code
            self.last_child = child
            return child
        return launch

    def run_one(self, mutate=None, profile=False, code=0, wait_error=None):
        self.args.output_dir.mkdir(mode=0o700)
        with mock.patch.object(self.module.subprocess, 'Popen', side_effect=self.fake_popen(mutate, code, wait_error)), \
                mock.patch.object(self.module.os, 'killpg', side_effect=ProcessLookupError):
            return self.module.run_one(self.args, self.args.output_dir, 0, 'defensive', profile)

    def test_command_is_complete_private_diagnostic_and_scopes_profile(self):
        result = self.run_one(profile=True)
        command, kwargs = self.last_command, self.last_kwargs
        self.assertEqual(command[:3], [str(self.binary), 'db', 'benchmark-history-cold'])
        self.assertEqual(command[command.index('--iterations') + 1], '1')
        self.assertIn('--cpu-profile', command)
        self.assertEqual(command[command.index('--compression-format') + 1], 'auto')
        self.assertNotIn('--datadir', command)
        self.assertEqual(kwargs['env']['GOMAXPROCS'], '2')
        self.assertEqual(kwargs['env']['GOMEMLIMIT'], '10GiB')
        self.assertNotIn('GTRON_HISTORY_COMPRESSION_FORMAT', kwargs['env'])
        self.assertTrue(kwargs['start_new_session'])
        self.assertEqual(kwargs['stdin'], subprocess.DEVNULL)
        self.assertEqual(result['source_digest'], self.digest)
        self.assertTrue(result['profile'])

    def test_mislabeled_incomplete_or_invalid_numeric_reports_fail(self):
        mutations = {
            'wrong-mode': lambda x: x['options'].__setitem__('copy_mode', 'owned'),
            'wrong-format': lambda x: x.__setitem__('compression_format', '2'),
            'wrong-manifest': lambda x: x.__setitem__('manifest_file_sha256', '0' * 64),
            'wrong-ordinal': lambda x: x['iterations'][1].__setitem__('iteration', 1),
            'wrong-digest': lambda x: x['iterations'][0]['digest'].__setitem__('rows', 4),
            'missing-equivalence': lambda x: x['iterations'][0].__setitem__('equivalent', False),
            'negative-wall': lambda x: x['iterations'][0].__setitem__('build_wall_nanos', -1),
            'bool-wall': lambda x: x['iterations'][0].__setitem__('build_wall_nanos', True),
            'negative-alloc': lambda x: x['iterations'][0].__setitem__('build_allocated_bytes', -1),
            'wrong-output-size': lambda x: x['iterations'][0].__setitem__('output_bytes', 31),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                self.args.output_dir = self.root / label
                with self.assertRaises((RuntimeError, ValueError)):
                    self.run_one(mutate)

    def test_profile_file_sha_is_required(self):
        with self.assertRaises((RuntimeError, ValueError)):
            self.run_one(lambda x: x.__setitem__('cpu_profile_sha256', '0' * 64), profile=True)

    def test_open_failure_restores_signal_mask(self):
        self.args.output_dir.mkdir(mode=0o700)
        mask_before = signal.pthread_sigmask(signal.SIG_BLOCK, [])
        try:
            with mock.patch('builtins.open', side_effect=OSError('fixture open failure')):
                with self.assertRaises(OSError):
                    self.module.run_one(self.args, self.args.output_dir, 0, 'defensive')
            self.assertEqual(signal.pthread_sigmask(signal.SIG_BLOCK, []), mask_before)
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, mask_before)

    def test_timeout_and_operator_interrupt_cleanup_the_child(self):
        for label, failure in [('timeout', subprocess.TimeoutExpired('fixture', 330)),
                               ('interrupt', InterruptedError('fixture signal'))]:
            with self.subTest(label=label):
                self.args.output_dir = self.root / label
                self.args.output_dir.mkdir(mode=0o700)
                with mock.patch.object(self.module.subprocess, 'Popen', side_effect=self.fake_popen(wait_error=failure)), \
                        mock.patch.object(self.module, 'terminate') as cleanup:
                    with self.assertRaises(type(failure)):
                        self.module.run_one(self.args, self.args.output_dir, 0, 'defensive')
                    cleanup.assert_called_once_with(self.last_child)

    def test_cleanup_covers_process_group_after_leader_exits(self):
        child = mock.Mock(pid=987654321)
        child.poll.return_value = 0
        calls = []

        def kill(group, sig):
            self.assertEqual(group, child.pid)
            calls.append(sig)
            if signal.SIGKILL in calls and sig == 0:
                raise ProcessLookupError()

        with mock.patch.object(self.module.os, 'killpg', side_effect=kill), \
                mock.patch.object(self.module.time, 'sleep', return_value=None):
            self.module.terminate(child)
        self.assertIn(signal.SIGKILL, calls, 'leader exit does not prove its process group is empty')

    def item(self, index, mode, profile):
        base = 100 if profile else index * 3
        rows = [{'build_wall_nanos': (base + i + 1) * 1000000000,
                 'build_allocated_bytes': 1000, 'build_allocations': 10,
                 'output_bytes': 30, 'equivalent': True} for i in range(1 if profile else 3)]
        return {'label': str(index), 'copy_mode': mode, 'profile': profile,
                'source_digest': dict(self.digest), 'blocks': 2, 'iterations': rows,
                'refs': self.report(self.root / 'unused')['iterations'][0]['refs'],
                'report_sha256': 'e' * 64, 'elapsed_seconds': 1}

    def test_abba_and_two_profiles_with_explicit_even_sample_statistic(self):
        calls = []

        def run(args, out, index, mode, profile=False):
            calls.append((mode, profile))
            return self.item(index, mode, profile)

        with mock.patch.object(self.module, 'run_one', side_effect=run):
            self.main()
        self.assertEqual(calls, [('defensive', False), ('owned', False), ('owned', False),
                                 ('defensive', False), ('defensive', True), ('owned', True)])
        summary = json.loads((self.args.output_dir / 'summary.json').read_text())
        self.assertTrue(summary['complete'])
        for mode in ('defensive', 'owned'):
            values = summary['results'][mode]['wall_seconds']
            self.assertEqual(len(values), 6)
            self.assertLess(max(values), 100, 'profile rows leaked into primary timing')
            result = summary['results'][mode]
            self.assertEqual(result['median_wall_seconds'], statistics.median(values))
            if 'median_blocks_per_second' in result:
                self.assertEqual(result['median_blocks_per_second'], statistics.median([2 / x for x in values]))
            else:
                self.assertEqual(result['blocks_per_second_at_median_wall'], 2 / statistics.median(values))

    def test_binary_manifest_and_root_preconditions(self):
        for kind in ('binary', 'manifest', 'root'):
            with self.subTest(kind=kind):
                self.args.output_dir = self.root / ('rejected-' + kind)
                with mock.patch.object(self.module, 'run_one') as run:
                    if kind == 'root':
                        with mock.patch.object(self.module.os, 'geteuid', return_value=0), self.assertRaises(RuntimeError):
                            self.main()
                    else:
                        field = 'binary_sha' if kind == 'binary' else 'manifest_sha'
                        old = getattr(self.args, field)
                        setattr(self.args, field, '0' * 64)
                        try:
                            with self.assertRaises(RuntimeError):
                                self.main()
                        finally:
                            setattr(self.args, field, old)
                    run.assert_not_called()

    def test_private_owner_and_output_exclusions(self):
        self.input.chmod(0o755)
        with mock.patch.object(self.module, 'run_one') as run, self.assertRaises(RuntimeError):
            self.main()
        run.assert_not_called()
        self.input.chmod(0o700)
        for output in (self.input, self.input / 'child', self.source.parent.parent / 'sibling'):
            with self.subTest(output=str(output)):
                self.args.output_dir = output
                with mock.patch.object(self.module, 'run_one') as run, self.assertRaises(RuntimeError):
                    self.main()
                run.assert_not_called()

    def test_spawn_failure_restores_signal_mask_and_leaves_no_child(self):
        self.args.output_dir.mkdir(mode=0o700)
        before = signal.pthread_sigmask(signal.SIG_BLOCK, [])
        try:
            with mock.patch.object(self.module.subprocess, 'Popen', side_effect=OSError('spawn failed')), \
                    mock.patch.object(self.module, 'terminate') as cleanup, self.assertRaises(OSError):
                self.module.run_one(self.args, self.args.output_dir, 0, 'defensive')
            cleanup.assert_called_once_with(None)
            self.assertEqual(signal.pthread_sigmask(signal.SIG_BLOCK, []), before)
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, before)

    def test_input_change_and_interruption_leave_incomplete_durable_summary(self):
        def run(args, out, index, mode, profile=False):
            (self.input / 'pebble' / 'table.sst').write_bytes(b'changed while running')
            if index == 1:
                raise InterruptedError('fixture interrupted')
            return self.item(index, mode, profile)

        with mock.patch.object(self.module, 'run_one', side_effect=run), self.assertRaises(InterruptedError):
            self.main()
        summary = json.loads((self.args.output_dir / 'summary.json').read_text())
        self.assertFalse(summary['complete'])
        self.assertIn('interrupted', summary['error'])
        # Failure reports must explicitly record the independent input check;
        # retaining only the interrupt cannot establish the read-only invariant.
        rendered = json.dumps(summary).lower()
        self.assertTrue(any(word in rendered for word in ('input_files_unchanged', 'input_unchanged', 'input_files_changed', 'input changed', 'input files changed')), rendered)

    def test_symlink_rejected_and_created_output_failure_has_summary(self):
        (self.input / 'pebble' / 'alias').symlink_to(self.binary)
        with mock.patch.object(self.module, 'run_one') as run, self.assertRaises(RuntimeError):
            self.main()
        run.assert_not_called()
        if self.args.output_dir.exists():
            self.assertTrue((self.args.output_dir / 'summary.json').is_file(), 'created output missing failure summary')

    def test_python36_syntax_and_no_live_actions(self):
        source = SCRIPT.read_text()
        ast.parse(source, filename=str(SCRIPT), feature_version=(3, 6))
        self.assertNotRegex(source, r'(?m)^\s*(?:from|import)\s+(?:requests|urllib|socket|paramiko)\b')
        self.assertNotIn('systemctl', source)
        self.assertNotIn('sudo', source)
        self.assertNotIn('curl', source)


if __name__ == '__main__':
    unittest.main()

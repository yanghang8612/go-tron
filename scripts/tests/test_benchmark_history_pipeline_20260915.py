import ast
import contextlib
import copy
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

SCRIPT = Path(__file__).resolve().parents[1] / 'benchmark_history_pipeline_20260915.py'


def load_module():
    spec = importlib.util.spec_from_file_location('parallel_test_' + os.urandom(6).hex(), str(SCRIPT))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PipelineBenchmarkTest(unittest.TestCase):
    def setUp(self):
        self.m = load_module()
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.binary = self.root / 'diagnostic'
        self.binary.write_bytes(b'fixture, never executed as Go')
        self.binary.chmod(0o700)
        self.input = self.root / 'capture'
        self.input.mkdir(mode=0o700)
        (self.input / 'pebble').mkdir(mode=0o700)
        (self.input / 'pebble' / 'fixture.sst').write_bytes(b'physical fixture')
        self.production = self.root / 'production' / 'gtron' / 'chaindata'
        self.production.mkdir(parents=True)
        self.source = {'version': 1, 'created_utc': '2026-09-15T00:00:00Z',
                       'source_chaindata': str(self.production), 'finish_block': 116, 'covered_block': 100,
                       'export': {'complete': True, 'content_verified': False, 'stop_reason': 'complete',
                                  'from_block': 101, 'to_block': 116, 'blocks': 16,
                                  'from_tx_num': 1001, 'to_tx_num': 1048, 'physical_rows': 1,
                                  'physical_bytes': 16, 'declared_decoded_bytes': 160,
                                  'manifest_sha256': 'a' * 64, 'entries': [{}], 'block_details': []}}
        for i in range(16):
            self.source['export']['block_details'].append({'block': 101 + i, 'canonical_hash': '0x' + 'b' * 64,
                'begin_tx_num': 1001 + i * 3, 'end_tx_num': 1003 + i * 3, 'declared_decoded_bytes': 10})
        (self.input / 'manifest.json').write_text(json.dumps(self.source))
        self.args = types.SimpleNamespace(binary=self.binary, binary_sha=self.m.file_sha(self.binary),
                    input_dir=self.input, manifest_sha=self.m.file_sha(self.input / 'manifest.json'),
                    output_dir=self.root / 'output')
        self.old_umask = os.umask(0o077)
        self.commands = []

    def tearDown(self):
        os.umask(self.old_umask)
        self.temp.cleanup()

    def digest(self, blocks=16):
        return {'rows': blocks * 2, 'payload_bytes': blocks * 10, 'prev_bytes': blocks * 8,
                'max_prev_bytes': 4, 'large_prev_rows_ge_128kib': 0, 'large_prev_bytes_ge_128kib': 0,
                'delegation_rows': blocks, 'delegation_prev_bytes': blocks * 4, 'row_sha256': 'c' * 64,
                'tx_ranges': blocks, 'tx_range_sha256': 'd' * 64}

    def argv(self):
        return [str(SCRIPT), '--binary', str(self.binary), '--binary-sha', self.args.binary_sha,
                '--input-dir', str(self.input), '--manifest-sha', self.args.manifest_sha,
                '--output-dir', str(self.args.output_dir)]

    def main(self):
        with mock.patch.object(sys, 'argv', self.argv()), mock.patch.object(sys, 'platform', 'linux'), \
                contextlib.redirect_stdout(io.StringIO()):
            self.m.main()

    def report(self, output, mode, profile=False):
        refs = [{'dataset': 'state-domain-change', 'kind': kind, 'fromTxNum': 1001, 'toTxNum': 1048,
                 'path': 'history/state-domain-change-range.' + extension, 'size': 100,
                 'checksum': 'sha256:' + 'e' * 64}
                for kind, extension in (('history', 'seg'), ('accessor', 'kv'), ('inverted', 'ef'))]
        result = {'version': 1, 'complete': True, 'physical_verified': True,
                  'manifest_file_sha256': self.args.manifest_sha, 'export': copy.deepcopy(self.source['export']),
                  'options': {'input_dir': str(self.input), 'output_dir': str(output), 'copy_mode': 'owned',
                              'compression_format': 'auto', 'shared_read_pipeline': mode == 'pipeline',
                              'iterations': 1 if profile else 3, 'cpu_profile': profile, 'max_duration_ns': 300000000000},
                  'compression_format': 'auto', 'gomaxprocs': 4, 'go_version': 'go1.25.5', 'goos': 'linux',
                  'goarch': 'amd64', 'source_digest': self.digest(), 'source_verification_wall_nanos': 100,
                  'iterations': []}
        for i in range(1 if profile else 3):
            result['iterations'].append({'iteration': i + 1, 'equivalent': True, 'build_wall_nanos': (i + 1) * 1000000000,
                'build_allocated_bytes': 1000, 'build_allocations': 10, 'build_gc_cycles': 0,
                'output_bytes': 300, 'refs': copy.deepcopy(refs), 'verification_wall_nanos': 200,
                'digest': self.digest()})
        if profile:
            result['cpu_profile_sha256'] = self.m.hashlib.sha256(b'profile fixture').hexdigest()
            result['cpu_profile_scope'] = 'iteration 1 complete production trio build only; process-wide'
        return result

    def launch(self, mutate=None, code=0, wait_error=None):
        def popen(command, **kwargs):
            self.commands.append((command, kwargs))
            output = Path(command[command.index('--output-dir') + 1])
            mode = 'pipeline' if '--shared-read-pipeline=true' in command else 'serial'
            profile = '--cpu-profile' in command
            output.mkdir(mode=0o700)
            report = self.report(output, mode, profile)
            for i, row in enumerate(report['iterations']):
                row['build_wall_nanos'] += len(self.commands) * 1000000 + (100000000000 if profile else 0)
            if mutate:
                mutate(report)
            (output / 'report.json').write_text(json.dumps(report))
            if profile:
                (output / 'cpu.pprof').write_bytes(b'profile fixture')
            child = mock.Mock(pid=987654321, returncode=code)
            child.wait.return_value = code
            if wait_error:
                child.wait.side_effect = wait_error
            self.child = child
            return child
        return popen

    def test_abba_two_profiles_and_exact_whole_trio_identity(self):
        with mock.patch.object(self.m.subprocess, 'Popen', side_effect=self.launch()), \
                mock.patch.object(self.m.os, 'killpg', side_effect=ProcessLookupError):
            self.main()
        summary = json.loads((self.args.output_dir / 'summary.json').read_text())
        self.assertTrue(summary['complete'] and summary['input_unchanged'] and summary['binary_unchanged'])
        self.assertEqual([(r['mode'], r['profile']) for r in summary['runs']],
            [('serial', False), ('pipeline', False), ('pipeline', False), ('serial', False), ('serial', True), ('pipeline', True)])
        self.assertEqual(sum(len(r['iterations']) for r in summary['runs']), 14)
        self.assertEqual(len(list(self.args.output_dir.glob('*.attempt.json'))), 6)
        for index, (command, kwargs) in enumerate(self.commands):
            enabled = index in (1, 2, 5)
            self.assertIn('--shared-read-pipeline=' + ('true' if enabled else 'false'), command)
            self.assertEqual(command[:3], [str(self.binary), 'db', 'benchmark-history-cold'])
            self.assertEqual(command[command.index('--copy-mode') + 1], 'owned')
            self.assertEqual(command[command.index('--compression-format') + 1], 'auto')
            self.assertEqual(command[command.index('--iterations') + 1], '1' if index >= 4 else '3')
            self.assertEqual(command[command.index('--max-duration') + 1], '5m')
            self.assertEqual(kwargs['env']['GOMAXPROCS'], '4')
            self.assertEqual(kwargs['env']['GOMEMLIMIT'], '10GiB')
            self.assertTrue(kwargs['start_new_session'])
            self.assertNotIn('--datadir', command)
        self.assertEqual(json.loads((self.args.output_dir / 'input-files-before.json').read_text()),
                         json.loads((self.args.output_dir / 'input-files-after.json').read_text()))
        for value in summary['results'].values():
            self.assertEqual(value['samples'], 6)
            self.assertEqual(len(value['whole_command_child_cpu_seconds']), 2)
            self.assertLess(max(value['wall_seconds']), 10, 'profile samples leaked into primary timings')
            self.assertEqual(value['blocks_per_second_at_median_wall'], 16 / statistics.median(value['wall_seconds']))
        self.assertIn('whole diagnostic command', summary['resource_scope'])
        self.assertIn('not an independent', summary['resource_scope'])

    def test_report_rejects_wrong_mode_runtime_proof_measurements_and_refs(self):
        mutations = {
            'pipeline': lambda r: r['options'].update(shared_read_pipeline=True),
            'pipeline-bool-type': lambda r: r['options'].update(shared_read_pipeline=0),
            'copy-mode': lambda r: r['options'].update(copy_mode='defensive'),
            'format': lambda r: r.update(compression_format='3'),
            'iterations-bool': lambda r: r['options'].update(iterations=True),
            'runtime': lambda r: r.update(go_version='go1.27.1'),
            'architecture': lambda r: r.update(goarch='arm64'),
            'manifest': lambda r: r.update(manifest_file_sha256='0' * 64),
            'physical': lambda r: r.update(physical_verified=False),
            'complete-bool': lambda r: r.update(complete=1),
            'export': lambda r: r['export'].update(from_block=102),
            'missing-digest': lambda r: r['source_digest'].pop('delegation_rows'),
            'cold-digest': lambda r: r['iterations'][0]['digest'].update(row_sha256='a' * 64),
            'failed': lambda r: r['iterations'][0].update(equivalent=False),
            'ordinal': lambda r: r['iterations'][1].update(iteration=1),
            'zero-wall': lambda r: r['iterations'][0].update(build_wall_nanos=0),
            'bool-alloc': lambda r: r['iterations'][0].update(build_allocations=True),
            'negative-verify': lambda r: r['iterations'][0].update(verification_wall_nanos=-1),
            'two-files': lambda r: r['iterations'][0]['refs'].pop(),
            'ref-dataset': lambda r: r['iterations'][0]['refs'][0].update(dataset='event-log'),
            'ref-range': lambda r: r['iterations'][0]['refs'][0].update(fromTxNum=1002),
            'ref-duplicate': lambda r: r['iterations'][0]['refs'][0].update(kind='accessor'),
            'ref-escape': lambda r: r['iterations'][0]['refs'][0].update(path='../outside'),
            'ref-size': lambda r: r['iterations'][0]['refs'][0].update(size=101),
            'ref-bytes': lambda r: r['iterations'][1]['refs'][0].update(checksum='sha256:' + 'f' * 64),
            'output-size': lambda r: r['iterations'][0].update(output_bytes=301),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                report = self.report(self.args.output_dir, 'serial')
                mutate(report)
                with self.assertRaises(RuntimeError):
                    self.m.check_report(report, self.args, self.args.output_dir, 'serial', False, self.source)

    def test_profile_file_sha_is_required(self):
        self.args.output_dir.mkdir(mode=0o700)
        report = self.report(self.args.output_dir, 'pipeline', profile=True)
        (self.args.output_dir / 'cpu.pprof').write_bytes(b'changed profile')
        with self.assertRaises(RuntimeError):
            self.m.check_report(report, self.args, self.args.output_dir, 'pipeline', True, self.source)

    def test_cross_mode_trio_change_fails_final_summary(self):
        def mutate(report):
            if report['options']['shared_read_pipeline']:
                for row in report['iterations']:
                    row['refs'][0]['checksum'] = 'sha256:' + 'f' * 64
        with mock.patch.object(self.m.subprocess, 'Popen', side_effect=self.launch(mutate=mutate)), \
                mock.patch.object(self.m.os, 'killpg', side_effect=ProcessLookupError), self.assertRaises(RuntimeError):
            self.main()
        summary = json.loads((self.args.output_dir / 'summary.json').read_text())
        self.assertFalse(summary['complete'])
        self.assertTrue(summary['input_unchanged'])
        self.assertIn('trio bytes differ', summary['error'])

    def test_timeout_interrupt_and_failed_exit_leave_attempt_evidence(self):
        failures = (subprocess.TimeoutExpired('fixture', 330), InterruptedError('signal'), None)
        for index, failure in enumerate(failures):
            self.args.output_dir = self.root / ('failure-' + str(index))
            self.args.output_dir.mkdir(mode=0o700)
            with mock.patch.object(self.m.subprocess, 'Popen', side_effect=self.launch(code=2, wait_error=failure)), \
                    mock.patch.object(self.m, 'terminate') as cleanup:
                with self.assertRaises(type(failure) if failure else RuntimeError):
                    self.m.run_one(self.args, 0, 'serial', self.source)
                cleanup.assert_called_once_with(self.child)
            attempt = json.loads((self.args.output_dir / '00-serial.attempt.json').read_text())
            self.assertFalse(attempt['complete'])
            for key in ('returncode', 'error', 'stdout_sha256', 'stderr_sha256', 'usage_after', 'whole_command_child_cpu_seconds'):
                self.assertIn(key, attempt)

    def test_spawn_failure_restores_signal_mask(self):
        self.args.output_dir.mkdir(mode=0o700)
        before = signal.pthread_sigmask(signal.SIG_BLOCK, [])
        with mock.patch.object(self.m.subprocess, 'Popen', side_effect=OSError('spawn failed')), \
                mock.patch.object(self.m, 'terminate') as cleanup, self.assertRaises(OSError):
            self.m.run_one(self.args, 0, 'serial', self.source)
        cleanup.assert_called_once_with(None)
        self.assertEqual(signal.pthread_sigmask(signal.SIG_BLOCK, []), before)

    def test_cleanup_kills_private_group_after_leader_exit(self):
        child = mock.Mock(pid=987654321, returncode=0)
        with mock.patch.object(self.m.os, 'killpg') as kill:
            self.m.terminate(child)
        self.assertEqual(kill.call_args_list, [mock.call(child.pid, signal.SIGTERM), mock.call(child.pid, signal.SIGKILL)])
        self.assertEqual(child.wait.call_args_list, [mock.call(timeout=5), mock.call(timeout=10)])

    def test_changed_input_on_interrupt_is_recorded(self):
        def failed(*args):
            (self.input / 'pebble' / 'fixture.sst').write_bytes(b'changed')
            raise InterruptedError('operator stopped')
        with mock.patch.object(self.m, 'run_one', side_effect=failed), self.assertRaises(InterruptedError):
            self.main()
        summary = json.loads((self.args.output_dir / 'summary.json').read_text())
        self.assertFalse(summary['complete'] or summary['input_unchanged'])
        self.assertTrue((self.args.output_dir / 'input-files-after.json').exists())

    def test_pins_root_privacy_and_output_overlap_reject_before_spawn(self):
        with mock.patch.object(self.m.os, 'geteuid', return_value=0), self.assertRaises(RuntimeError):
            self.main()
        for field in ('manifest_sha', 'binary_sha'):
            old = getattr(self.args, field)
            setattr(self.args, field, '0' * 64)
            with self.assertRaises(RuntimeError):
                self.main()
            setattr(self.args, field, old)
        self.input.chmod(0o755)
        with self.assertRaises(RuntimeError):
            self.main()
        self.input.chmod(0o700)
        for output in (self.input / 'child', self.production.parent.parent / 'sibling'):
            self.args.output_dir = output
            with self.assertRaises(RuntimeError):
                self.main()
        self.assertFalse(self.commands)

    def test_fixed_manifest_does_not_allow_partial_range(self):
        source = copy.deepcopy(self.source)
        source['export']['blocks'] = 15
        with self.assertRaises(RuntimeError):
            self.m.check_export(source)

    def test_python36_syntax(self):
        for path in (SCRIPT, Path(__file__)):
            ast.parse(path.read_text(), filename=str(path), feature_version=(3, 6))


if __name__ == '__main__':
    unittest.main()

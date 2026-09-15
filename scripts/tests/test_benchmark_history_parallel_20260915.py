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


SCRIPT = Path(__file__).resolve().parents[1] / 'benchmark_history_parallel_20260915.py'


def load_module():
    spec = importlib.util.spec_from_file_location('parallel_test_' + os.urandom(6).hex(), str(SCRIPT))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ParallelBenchmarkTest(unittest.TestCase):
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

    def report(self, output, workers, segments):
        ranges, built = [], []
        width = 16 // segments
        for i in range(segments):
            blocks = self.source['export']['block_details'][i * width:(i + 1) * width]
            plan = {'index': i, 'from_block': blocks[0]['block'], 'to_block': blocks[-1]['block'],
                    'from_tx_num': blocks[0]['begin_tx_num'], 'to_tx_num': blocks[-1]['end_tx_num'],
                    'declared_decoded_bytes': width * 10, 'source_digest': self.digest(width)}
            ranges.append(plan)
            refs = [{'dataset': 'state-domain-change', 'kind': kind, 'fromTxNum': plan['from_tx_num'],
                     'toTxNum': plan['to_tx_num'], 'path': 'history/state-domain-change-range.' + extension,
                     'size': 100, 'checksum': 'sha256:' + 'e' * 64}
                    for kind, extension in (('history', 'seg'), ('accessor', 'kv'), ('inverted', 'ef'))]
            built.append({'index': i, 'started': True, 'equivalent': True, 'build_wall_nanos': 900000000,
                          'refs': refs, 'output_bytes': 300, 'digest': self.digest(width)})
        budget = {'etl_aggregate_threshold_bytes': 64 << 20, 'etl_per_collector_bytes': (64 << 20) // (2 * workers),
                  'etl_collectors_per_worker': 2, 'max_workers': 4, 'max_input_physical_bytes': 1 << 30,
                  'max_input_physical_rows': 262144, 'max_input_declared_bytes': 4 << 30,
                  'shared_pack_max_bytes_per_worker': 128 << 20, 'v6_key_table_max_bytes_per_worker': 512 << 20,
                  'cdc_dictionary_payload_max_bytes_per_worker': 64 << 20,
                  'limit_scope': 'configured thresholds, not a hard heap/RSS bound'}
        return {'version': 1, 'complete': True, 'physical_verified': True, 'partition_coverage_verified': True,
                'manifest_file_sha256': self.args.manifest_sha, 'options': {'input_dir': str(self.input),
                'output_dir': str(output), 'workers': workers, 'segments': segments, 'iterations': 1,
                'max_duration_ns': 300000000000, 'cpu_profile': False}, 'copy_mode': 'owned',
                'compression_format': 'auto', 'gomaxprocs': 4, 'go_version': 'go1.25.5',
                'goos': 'linux', 'goarch': 'amd64', 'export': copy.deepcopy(self.source['export']),
                'source_digest': self.digest(), 'ranges': ranges, 'budget': budget, 'iterations': [{
                    'iteration': 1, 'equivalent': True, 'build_wall_nanos': 1000000000,
                    'build_allocated_bytes': 1000, 'build_allocations': 10, 'build_gc_cycles': 0,
                    'verification_wall_nanos': 100, 'output_bytes': 300 * segments, 'segments': built,
                    'statistics': self.m.statistics_only(self.digest())}]}

    def launch(self, mutate=None, code=0, wait_error=None):
        def popen(command, **kwargs):
            self.commands.append((command, kwargs))
            output = Path(command[command.index('--output-dir') + 1])
            workers, segments = [int(command[command.index('--' + key) + 1]) for key in ('workers', 'segments')]
            output.mkdir(mode=0o700)
            report = self.report(output, workers, segments)
            report['iterations'][0]['build_wall_nanos'] += len(self.commands) * 1000000
            if mutate:
                mutate(report)
            (output / 'report.json').write_text(json.dumps(report))
            child = mock.Mock(pid=987654321, returncode=code)
            child.wait.return_value = code
            if wait_error:
                child.wait.side_effect = wait_error
            self.child = child
            return child
        return popen

    def argv(self):
        return [str(SCRIPT), '--binary', str(self.binary), '--binary-sha', self.args.binary_sha,
                '--input-dir', str(self.input), '--manifest-sha', self.args.manifest_sha,
                '--output-dir', str(self.args.output_dir)]

    def main(self):
        with mock.patch.object(sys, 'argv', self.argv()), mock.patch.object(sys, 'platform', 'linux'), \
                contextlib.redirect_stdout(io.StringIO()):
            self.m.main()

    def test_full_forward_reverse_matrix_and_statistic_scope(self):
        with mock.patch.object(self.m.subprocess, 'Popen', side_effect=self.launch()), \
                mock.patch.object(self.m.os, 'killpg', side_effect=ProcessLookupError):
            self.main()
        summary = json.loads((self.args.output_dir / 'summary.json').read_text())
        expected = list(self.m.CONFIGURATIONS) + list(reversed(self.m.CONFIGURATIONS))
        self.assertEqual([(r['workers'], r['segments']) for r in summary['runs']], expected)
        self.assertTrue(summary['complete'] and summary['input_unchanged'] and summary['binary_unchanged'])
        self.assertEqual(json.loads((self.args.output_dir / 'input-files-before.json').read_text()),
                         json.loads((self.args.output_dir / 'input-files-after.json').read_text()))
        self.assertEqual(len(list(self.args.output_dir.glob('*.attempt.json'))), 12)
        for command, kwargs in self.commands:
            self.assertEqual(command[:3], [str(self.binary), 'db', 'benchmark-history-parallel'])
            self.assertEqual(command[command.index('--iterations') + 1], '1')
            self.assertEqual(command[command.index('--max-duration') + 1], '5m')
            self.assertNotIn('--datadir', command)
            self.assertNotIn('--cpu-profile', command)
            self.assertEqual(kwargs['env']['GOMAXPROCS'], '4')
            self.assertEqual(kwargs['env']['GOMEMLIMIT'], '10GiB')
            self.assertEqual(set(kwargs['env']), {'PATH', 'LANG', 'HOME', 'GOMAXPROCS', 'GOMEMLIMIT'})
            self.assertTrue(kwargs['start_new_session'])
            self.assertEqual(kwargs['stdin'], subprocess.DEVNULL)
        for result in summary['results'].values():
            self.assertEqual(result['samples'], 2)
            self.assertEqual(result['median_wall_seconds'], statistics.median(result['wall_seconds']))
            self.assertEqual(result['blocks_per_second_at_median_wall'], 16 / result['median_wall_seconds'])
        self.assertIn('not an independent peak', summary['resource_scope'])
        self.assertIn('Cross-segment', summary['comparison_scope'])
        self.assertNotIn('speedup', summary['results'])

    def test_report_rejects_missing_proof_bad_options_and_numeric_types(self):
        mutations = {
            'complete': lambda r: r.update(complete=1),
            'physical': lambda r: r.update(physical_verified=False),
            'partition': lambda r: r.update(partition_coverage_verified=False),
            'manifest': lambda r: r.update(manifest_file_sha256='0' * 64),
            'export': lambda r: r['export'].update(physical_bytes=17),
            'runtime': lambda r: r.update(goarch='arm64'),
            'options': lambda r: r['options'].update(workers=4),
            'bool-iteration-option': lambda r: r['options'].update(iterations=True),
            'bool-profile-option': lambda r: r['options'].update(cpu_profile=0),
            'bool-iteration': lambda r: r['iterations'][0].update(iteration=True),
            'limit': lambda r: r['budget'].update(etl_per_collector_bytes=64 << 20),
            'rss': lambda r: r['budget'].update(limit_scope='hard 64 MiB RSS bound'),
            'whole': lambda r: r['source_digest'].update(rows=900),
            'sha': lambda r: r['source_digest'].update(row_sha256=''),
            'missing-digest-field': lambda r: r['source_digest'].pop('delegation_rows'),
            'bool-number': lambda r: r['iterations'][0].update(build_allocations=True),
            'negative': lambda r: r['iterations'][0].update(build_wall_nanos=-1),
            'zero-wall': lambda r: r['iterations'][0].update(build_wall_nanos=0),
            'extra-iteration': lambda r: r['iterations'].append(copy.deepcopy(r['iterations'][0])),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                report = self.report(self.args.output_dir, 2, 2)
                mutate(report)
                with self.assertRaises(RuntimeError):
                    self.m.check_report(report, self.args, self.args.output_dir, 2, 2, self.source)

    def test_report_requires_exact_partition_full_digest_and_three_refs(self):
        mutations = {
            'gap': lambda r: r['ranges'][1].update(from_block=110),
            'overlap': lambda r: r['ranges'][1].update(from_tx_num=1024),
            'wrong-declared': lambda r: r['ranges'][0].update(declared_decoded_bytes=90),
            'range-digest': lambda r: r['ranges'][0]['source_digest'].update(tx_ranges=7),
            'range-hash': lambda r: r['ranges'][0]['source_digest'].update(row_sha256='a' * 64),
            'unstarted': lambda r: r['iterations'][0]['segments'][1].update(started=False),
            'unequal': lambda r: r['iterations'][0]['segments'][1].update(equivalent=False),
            'cold-hash': lambda r: r['iterations'][0]['segments'][1]['digest'].update(row_sha256='f' * 64),
            'two-files': lambda r: r['iterations'][0]['segments'][0]['refs'].pop(),
            'duplicate-kind': lambda r: r['iterations'][0]['segments'][0]['refs'][0].update(kind='accessor'),
            'ref-range': lambda r: r['iterations'][0]['segments'][0]['refs'][0].update(fromTxNum=0),
            'ref-path': lambda r: r['iterations'][0]['segments'][0]['refs'][0].update(path='../outside'),
            'ref-checksum': lambda r: r['iterations'][0]['segments'][0]['refs'][0].update(checksum='sha256:bad'),
            'ref-size': lambda r: r['iterations'][0]['segments'][0]['refs'][0].update(size=101),
            'total': lambda r: r['iterations'][0].update(output_bytes=601),
            'union': lambda r: r['iterations'][0]['statistics'].update(rows=31),
            'fake-union-hash': lambda r: r['iterations'][0]['statistics'].update(row_sha256='a' * 64),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                report = self.report(self.args.output_dir, 2, 2)
                mutate(report)
                with self.assertRaises(RuntimeError):
                    self.m.check_report(report, self.args, self.args.output_dir, 2, 2, self.source)

    def test_manifest_requires_complete_original_16_block_range(self):
        mutations = [lambda s: s['export'].update(blocks=15),
                     lambda s: s['export'].update(content_verified=True),
                     lambda s: s['export']['block_details'][1].update(begin_tx_num=1003),
                     lambda s: s['export']['block_details'][-1].update(end_tx_num=1049),
                     lambda s: s['export'].update(declared_decoded_bytes=159),
                     lambda s: s['export'].update(physical_rows=True),
                     lambda s: s.update(covered_block=101)]
        for mutate in mutations:
            source = copy.deepcopy(self.source)
            mutate(source)
            with self.assertRaises(RuntimeError):
                self.m.check_export(source)
        self.assertEqual(self.m.check_export(self.source)['blocks'], 16)

    def test_timeout_and_interrupt_join_then_save_durable_attempt(self):
        for index, failure in enumerate((subprocess.TimeoutExpired('fixture', 330), InterruptedError('signal'))):
            self.args.output_dir = self.root / ('attempt-' + str(index))
            self.args.output_dir.mkdir(mode=0o700)
            with mock.patch.object(self.m.subprocess, 'Popen', side_effect=self.launch(wait_error=failure)), \
                    mock.patch.object(self.m, 'terminate') as cleanup:
                with self.assertRaises(type(failure)):
                    self.m.run_one(self.args, 0, 1, 1, self.source)
                cleanup.assert_called_once_with(self.child)
            attempt = json.loads((self.args.output_dir / '00-w1-s1.attempt.json').read_text())
            self.assertFalse(attempt['complete'])
            self.assertIn('error', attempt)
            self.assertIn('stdout_sha256', attempt)
            self.assertIn('usage_after', attempt)
            self.assertIn('returncode', attempt)

    def test_spawn_failure_restores_mask(self):
        self.args.output_dir.mkdir(mode=0o700)
        before = signal.pthread_sigmask(signal.SIG_BLOCK, [])
        with mock.patch.object(self.m.subprocess, 'Popen', side_effect=OSError('cannot spawn')), \
                mock.patch.object(self.m, 'terminate') as cleanup:
            with self.assertRaises(OSError):
                self.m.run_one(self.args, 0, 1, 1, self.source)
            cleanup.assert_called_once_with(None)
        self.assertEqual(signal.pthread_sigmask(signal.SIG_BLOCK, []), before)
        self.assertFalse(json.loads((self.args.output_dir / '00-w1-s1.attempt.json').read_text())['complete'])

    def test_group_cleanup_still_kills_after_leader_exit(self):
        child = mock.Mock(pid=987654321, returncode=0)
        with mock.patch.object(self.m.os, 'killpg') as kill:
            self.m.terminate(child)
        self.assertEqual(kill.call_args_list, [mock.call(child.pid, signal.SIGTERM), mock.call(child.pid, signal.SIGKILL)])
        self.assertEqual(child.wait.call_args_list, [mock.call(timeout=5), mock.call(timeout=10)])

    def test_real_python_child_is_reaped(self):
        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'],
                                 stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL, start_new_session=True)
        self.m.terminate(child)
        self.assertIsNotNone(child.poll())

    def test_native_root_private_and_pin_preconditions(self):
        with mock.patch.object(self.m.os, 'geteuid', return_value=0), self.assertRaises(RuntimeError):
            self.main()
        for field in ('binary_sha', 'manifest_sha'):
            previous = getattr(self.args, field)
            setattr(self.args, field, '0' * 64)
            with self.assertRaises(RuntimeError):
                self.main()
            setattr(self.args, field, previous)
        self.input.chmod(0o755)
        with self.assertRaises(RuntimeError):
            self.main()
        self.input.chmod(0o700)
        for output in (self.input, self.input / 'inside', self.production.parent.parent / 'sibling'):
            self.args.output_dir = output
            with self.assertRaises(RuntimeError):
                self.main()
        self.assertFalse(self.commands)

    def test_symlink_and_special_files_are_rejected(self):
        link = self.input / 'symlink'
        link.symlink_to(self.binary)
        with self.assertRaises(RuntimeError):
            self.m.private_manifest(self.input)
        link.unlink()
        fifo = self.input / 'fifo'
        os.mkfifo(str(fifo))
        with self.assertRaises(RuntimeError):
            self.m.private_manifest(self.input)

    def test_input_after_check_survives_failed_run(self):
        def failed(*args):
            (self.input / 'pebble' / 'fixture.sst').write_bytes(b'changed')
            raise InterruptedError('fixture interrupt')
        with mock.patch.object(self.m, 'run_one', side_effect=failed), self.assertRaises(InterruptedError):
            self.main()
        summary = json.loads((self.args.output_dir / 'summary.json').read_text())
        self.assertFalse(summary['complete'])
        self.assertFalse(summary['input_unchanged'])
        self.assertTrue(summary['binary_unchanged'])
        self.assertTrue((self.args.output_dir / 'input-files-after.json').exists())

    def test_failed_exit_report_cannot_claim_success(self):
        self.args.output_dir.mkdir(mode=0o700)
        with mock.patch.object(self.m.subprocess, 'Popen', side_effect=self.launch(code=2)), \
                mock.patch.object(self.m.os, 'killpg', side_effect=ProcessLookupError):
            with self.assertRaises(RuntimeError):
                self.m.run_one(self.args, 0, 1, 1, self.source)
        attempt = json.loads((self.args.output_dir / '00-w1-s1.attempt.json').read_text())
        self.assertEqual(attempt['returncode'], 2)
        self.assertFalse(attempt['complete'])

    def test_python36_syntax(self):
        ast.parse(SCRIPT.read_text(), filename=str(SCRIPT), feature_version=(3, 6))


if __name__ == '__main__':
    unittest.main()

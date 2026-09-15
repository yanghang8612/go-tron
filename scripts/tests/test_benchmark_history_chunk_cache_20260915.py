import ast
import contextlib
import copy
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
import unittest
from unittest import mock

from scripts.tests import test_benchmark_history_pipeline_20260915 as fixtures

SCRIPT = Path(__file__).resolve().parents[1] / 'benchmark_history_chunk_cache_20260915.py'
ROOT = SCRIPT.parents[1]


def load_module():
    spec = importlib.util.spec_from_file_location('chunk_cache_test_' + os.urandom(6).hex(), str(SCRIPT))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ChunkCacheBenchmarkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        module = load_module()
        cls.helper_bytes = subprocess.check_output(['git', 'show', module.HELPER_REVISION + ':' + module.HELPER_PATH],
                                                  cwd=str(ROOT))

    def setUp(self):
        self.m = load_module()
        with mock.patch.object(self.m.subprocess, 'check_output', return_value=self.helper_bytes):
            self.h = self.m.load_helpers()
        self.fixture = fixtures.PipelineBenchmarkTest('runTest')
        self.fixture.setUp()
        self.args, self.source = self.fixture.args, self.fixture.source
        self.commands = []

    def tearDown(self):
        self.fixture.tearDown()

    def main(self):
        argv = self.fixture.argv()
        argv[0] = str(SCRIPT)
        with mock.patch.object(sys, 'argv', argv), mock.patch.object(sys, 'platform', 'linux'), \
                mock.patch.object(self.m, 'load_helpers', return_value=self.h), contextlib.redirect_stdout(io.StringIO()):
            self.m.main()

    def report(self, output, enabled, profile=False):
        report = self.fixture.report(output, 'pipeline', profile)
        report['options'].update(shared_read_workers=4, cdc_compression_workers=1, shared_chunk_cache=enabled)
        if enabled:
            for row in report['iterations']:
                before = {'hits': 5, 'misses': 2, 'inserts': 2, 'evictions': 1,
                          'payload_bytes': 4096, 'peak_payload_bytes': 8192, 'entries': 1, 'peak_entries': 2,
                          'payload_budget_bytes': 67108864, 'entry_limit': 4096, 'closed': False}
                row['shared_chunk_cache_before_close'] = before
                row['shared_chunk_cache_after_close'] = dict(before, closed=True, payload_bytes=0, entries=0)
        return report

    def launch(self, mutate=None, code=0, wait_error=None):
        def popen(command, **kwargs):
            self.commands.append((command, kwargs))
            output = Path(command[command.index('--output-dir') + 1])
            enabled = '--shared-chunk-cache=true' in command
            profile = '--cpu-profile' in command
            output.mkdir(mode=0o700)
            report = self.report(output, enabled, profile)
            for row in report['iterations']:
                row['build_wall_nanos'] += len(self.commands) * 1000000 + (100000000000 if profile else 0)
            if mutate:
                mutate(report)
            (output / 'report.json').write_text(json.dumps(report))
            if profile:
                (output / 'cpu.pprof').write_bytes(b'profile fixture')
            self.child = mock.Mock(pid=987654321, returncode=code)
            self.child.wait.return_value = code
            if wait_error:
                self.child.wait.side_effect = wait_error
            return self.child
        return popen

    def test_pinned_helper_load_is_isolated_and_does_not_execute_old_main(self):
        with mock.patch.object(self.m.subprocess, 'check_output', return_value=self.helper_bytes) as command, \
                mock.patch.object(self.m.subprocess, 'Popen') as spawn:
            one, two = self.m.load_helpers(), self.m.load_helpers()
        command.assert_called_with(['git', 'show', self.m.HELPER_REVISION + ':' + self.m.HELPER_PATH],
                                   cwd=str(self.m.REPO), stdin=subprocess.DEVNULL, timeout=60)
        self.assertEqual(hashlib.sha256(self.helper_bytes).hexdigest(), self.m.HELPER_SHA)
        self.assertIsNot(one.__dict__, two.__dict__)
        self.assertEqual(one.__file__, str(SCRIPT.resolve()))
        self.assertEqual(one.MODES, ('serial', 'pipeline', 'pipeline', 'serial'))
        spawn.assert_not_called()
        with mock.patch.object(self.m.subprocess, 'check_output', return_value=b'raise AssertionError("executed")'):
            with self.assertRaisesRegex(RuntimeError, 'Git bytes differ'):
                self.m.load_helpers()

    def test_abba_fixed_options_complete_identity_and_separate_profiles(self):
        with mock.patch.object(self.m.subprocess, 'Popen', side_effect=self.launch()), \
                mock.patch.object(self.h.os, 'killpg', side_effect=ProcessLookupError):
            self.main()
        summary = json.loads((self.args.output_dir / 'summary.json').read_text())
        self.assertTrue(summary['complete'] and summary['input_unchanged'] and summary['binary_unchanged'])
        self.assertEqual([(r['cache_enabled'], r['profile']) for r in summary['runs']], list(self.m.ORDER))
        self.assertEqual(len(list(self.args.output_dir.glob('*.attempt.json'))), 6)
        self.assertEqual(sum(len(r['iterations']) for r in summary['runs']), 14)
        self.assertEqual(summary['script_sha256'], self.h.file_sha(SCRIPT))
        self.assertEqual(json.loads((self.args.output_dir / 'input-files-before.json').read_text()),
                         json.loads((self.args.output_dir / 'input-files-after.json').read_text()))
        for (enabled, profile), (command, kwargs) in zip(self.m.ORDER, self.commands):
            self.assertEqual(command[:3], [str(self.args.binary), 'db', 'benchmark-history-cold'])
            self.assertIn('--shared-read-pipeline=true', command)
            self.assertIn('--shared-chunk-cache=' + ('true' if enabled else 'false'), command)
            for flag, value in (('--shared-read-workers', '4'), ('--cdc-compression-workers', '1'), ('--copy-mode', 'owned'),
                                ('--compression-format', 'auto'), ('--iterations', '1' if profile else '3'),
                                ('--max-duration', '5m')):
                self.assertEqual(command[command.index(flag) + 1], value)
            self.assertEqual('--cpu-profile' in command, profile)
            self.assertNotIn('--datadir', command)
            self.assertEqual(kwargs['env']['GOMAXPROCS'], '4')
            self.assertEqual(kwargs['env']['GOMEMLIMIT'], '10GiB')
            self.assertTrue(kwargs['start_new_session'])
        for result in summary['results'].values():
            self.assertEqual(result['samples'], 6)
            self.assertLess(max(result['wall_seconds']), 10, 'profiles polluted primary timings')
            self.assertEqual(result['blocks_per_second_at_median_wall'], 16 / statistics.median(result['wall_seconds']))
        on = summary['results']['on']
        self.assertTrue(on['cache_exercised'])
        self.assertEqual(on['cache_counters_total']['hits'], 30, 'profile counters included')
        self.assertEqual(on['cache_payload_bytes_after_close'], [0] * 6)
        self.assertEqual(on['cache_entries_after_close'], [0] * 6)
        self.assertEqual(on['cache_peak_payload_bytes'], [8192] * 6)
        self.assertFalse(summary['results']['off']['cache_exercised'])
        self.assertIn('do not bound process RSS', summary['comparison_scope'])
        self.assertIn('whole diagnostic command', summary['resource_scope'])
        self.assertIn('not an independent', summary['resource_scope'])

    def test_cache_stats_schema_budget_cleanup_and_counter_preservation(self):
        mutations = {
            'missing-before': lambda r: r.pop('shared_chunk_cache_before_close'),
            'missing-after': lambda r: r.pop('shared_chunk_cache_after_close'),
            'uint-bool': lambda r: r['shared_chunk_cache_before_close'].update(hits=True),
            'negative': lambda r: r['shared_chunk_cache_before_close'].update(misses=-1),
            'closed-type': lambda r: r['shared_chunk_cache_after_close'].update(closed=1),
            'before-closed': lambda r: r['shared_chunk_cache_before_close'].update(closed=True),
            'after-open': lambda r: r['shared_chunk_cache_after_close'].update(closed=False),
            'over-budget': lambda r: r['shared_chunk_cache_before_close'].update(peak_payload_bytes=67108865),
            'over-entries': lambda r: r['shared_chunk_cache_before_close'].update(peak_entries=4097),
            'wrong-budget': lambda r: r['shared_chunk_cache_before_close'].update(payload_budget_bytes=134217728),
            'wrong-entry-limit': lambda r: r['shared_chunk_cache_before_close'].update(entry_limit=8192),
            'live-beyond-peak': lambda r: r['shared_chunk_cache_before_close'].update(payload_bytes=16384),
            'entries-beyond-peak': lambda r: r['shared_chunk_cache_before_close'].update(entries=3),
            'retained-payload': lambda r: r['shared_chunk_cache_after_close'].update(payload_bytes=4096),
            'retained-entries': lambda r: r['shared_chunk_cache_after_close'].update(entries=1),
            'changed-counters': lambda r: r['shared_chunk_cache_after_close'].update(hits=6),
            'changed-peak': lambda r: r['shared_chunk_cache_after_close'].update(peak_payload_bytes=4096),
            'no-fresh-miss': lambda r: (r['shared_chunk_cache_before_close'].update(misses=0),
                                      r['shared_chunk_cache_after_close'].update(misses=0)),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                item = self.report(self.args.output_dir, True)['iterations'][0]
                mutate(item)
                with self.assertRaises(RuntimeError):
                    self.m.check_cache_stats(self.h, item, True)
        off = self.report(self.args.output_dir, False)['iterations'][0]
        off['shared_chunk_cache_before_close'] = None
        with self.assertRaises(RuntimeError):
            self.m.check_cache_stats(self.h, off, False)
        on = self.report(self.args.output_dir, True)['iterations'][0]
        for name in ('shared_chunk_cache_before_close', 'shared_chunk_cache_after_close'):
            on[name]['hits'] = 0
        self.m.check_cache_stats(self.h, on, True)  # Individual samples need not hit.

    def test_original_complete_authentication_cannot_be_replaced_by_good_cache_stats(self):
        mutations = {
            'missing-cache-option': lambda r: r['options'].pop('shared_chunk_cache'),
            'cache-bool': lambda r: r['options'].update(shared_chunk_cache=1),
            'workers': lambda r: r['options'].update(shared_read_workers=8),
            'cdc-workers-default': lambda r: r['options'].update(cdc_compression_workers=0),
            'cdc-workers-bool': lambda r: r['options'].update(cdc_compression_workers=True),
            'cdc-workers-missing': lambda r: r['options'].pop('cdc_compression_workers'),
            'workers-bool': lambda r: r['options'].update(shared_read_workers=True),
            'serial': lambda r: r['options'].update(shared_read_pipeline=False),
            'copy': lambda r: r['options'].update(copy_mode='defensive'),
            'format': lambda r: r['options'].update(compression_format='3'),
            'runtime': lambda r: r.update(go_version='go1.27.1'),
            'physical': lambda r: r.update(physical_verified=False),
            'export': lambda r: r['export'].update(from_block=102),
            'digest': lambda r: r['iterations'][0]['digest'].update(row_sha256='a' * 64),
            'ref': lambda r: r['iterations'][0]['refs'][0].update(fromTxNum=1002),
            'bytes': lambda r: r['iterations'][0].update(output_bytes=301),
            'alloc': lambda r: r['iterations'][0].update(build_allocated_bytes=True),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                report = self.report(self.args.output_dir, True)
                mutate(report)
                with self.assertRaises(RuntimeError):
                    self.m.check_report(self.h, report, self.args, self.args.output_dir, True, False, self.source)

    def test_cross_cache_bytes_or_source_change_rejects_and_keeps_final_evidence(self):
        for field in ('refs', 'source_digest'):
            self.commands = []
            self.args.output_dir = self.fixture.root / field
            def mutate(report):
                if report['options']['shared_chunk_cache']:
                    if field == 'refs':
                        for row in report['iterations']:
                            row['refs'][0]['checksum'] = 'sha256:' + 'f' * 64
                    else:
                        report['source_digest']['row_sha256'] = 'f' * 64
                        for row in report['iterations']:
                            row['digest']['row_sha256'] = 'f' * 64
            with mock.patch.object(self.m.subprocess, 'Popen', side_effect=self.launch(mutate)), \
                    mock.patch.object(self.h.os, 'killpg', side_effect=ProcessLookupError), self.assertRaises(RuntimeError):
                self.main()
            summary = json.loads((self.args.output_dir / 'summary.json').read_text())
            self.assertFalse(summary['complete'])
            self.assertTrue(summary['input_unchanged'] and summary['binary_unchanged'])
            self.assertEqual(len(self.commands), 2)

    def test_profile_hits_do_not_satisfy_primary_hit_exercise_gate(self):
        def mutate(report):
            if report['options']['shared_chunk_cache'] and not report['options']['cpu_profile']:
                for row in report['iterations']:
                    row['shared_chunk_cache_before_close']['hits'] = 0
                    row['shared_chunk_cache_after_close']['hits'] = 0
        with mock.patch.object(self.m.subprocess, 'Popen', side_effect=self.launch(mutate)), \
                mock.patch.object(self.h.os, 'killpg', side_effect=ProcessLookupError), \
                self.assertRaisesRegex(RuntimeError, 'did not exercise'):
            self.main()
        summary = json.loads((self.args.output_dir / 'summary.json').read_text())
        self.assertFalse(summary['complete'])
        self.assertEqual(len(summary['runs']), 6)

    def test_profile_content_sha_is_verified(self):
        self.args.output_dir.mkdir(mode=0o700)
        report = self.report(self.args.output_dir, True, True)
        (self.args.output_dir / 'cpu.pprof').write_bytes(b'changed profile')
        with self.assertRaises(RuntimeError):
            self.m.check_report(self.h, report, self.args, self.args.output_dir, True, True, self.source)

    def test_timeout_interruption_and_failure_preserve_attempt_and_after_checks(self):
        for index, failure in enumerate((subprocess.TimeoutExpired('fixture', 330), InterruptedError('operator'), None)):
            self.args.output_dir = self.fixture.root / ('failure-' + str(index))
            with mock.patch.object(self.m.subprocess, 'Popen', side_effect=self.launch(code=2, wait_error=failure)), \
                    mock.patch.object(self.h, 'terminate') as cleanup, self.assertRaises(type(failure) if failure else RuntimeError):
                self.main()
            cleanup.assert_called_once_with(self.child)
            attempt = json.loads((self.args.output_dir / '00-cache-off.attempt.json').read_text())
            summary = json.loads((self.args.output_dir / 'summary.json').read_text())
            self.assertFalse(attempt['complete'] or summary['complete'])
            self.assertTrue(summary['input_unchanged'] and summary['binary_unchanged'])
            for name in ('error', 'returncode', 'stdout_sha256', 'stderr_sha256', 'usage_before',
                         'usage_after', 'whole_command_child_cpu_seconds'):
                self.assertIn(name, attempt)

    def test_spawn_failure_restores_mask_and_group_cleanup_after_leader_exit(self):
        self.args.output_dir.mkdir(mode=0o700)
        before = signal.pthread_sigmask(signal.SIG_BLOCK, [])
        with mock.patch.object(self.m.subprocess, 'Popen', side_effect=OSError('spawn failed')), \
                mock.patch.object(self.h, 'terminate') as cleanup, self.assertRaises(OSError):
            self.m.run_one(self.h, self.args, 0, False, self.source)
        cleanup.assert_called_once_with(None)
        self.assertEqual(signal.pthread_sigmask(signal.SIG_BLOCK, []), before)
        child = mock.Mock(pid=987654321, returncode=0)
        with mock.patch.object(self.h.os, 'killpg') as kill:
            self.h.terminate(child)
        self.assertEqual(kill.call_args_list, [mock.call(child.pid, signal.SIGTERM), mock.call(child.pid, signal.SIGKILL)])
        self.assertEqual(child.wait.call_args_list, [mock.call(timeout=5), mock.call(timeout=10)])

    def test_input_and_binary_mutation_on_interrupt_cannot_complete(self):
        def fail(*unused):
            (self.args.input_dir / 'pebble' / 'fixture.sst').write_bytes(b'changed source')
            self.args.binary.write_bytes(b'changed binary')
            raise InterruptedError('operator')
        with mock.patch.object(self.m, 'run_one', side_effect=fail), self.assertRaises(InterruptedError):
            self.main()
        summary = json.loads((self.args.output_dir / 'summary.json').read_text())
        self.assertFalse(summary['complete'] or summary['input_unchanged'] or summary['binary_unchanged'])

    def test_root_bad_pins_nonprivate_input_and_protected_outputs_reject(self):
        with mock.patch.object(self.m.os, 'geteuid', return_value=0), self.assertRaises(RuntimeError):
            self.main()
        for field in ('manifest_sha', 'binary_sha'):
            value = getattr(self.args, field)
            setattr(self.args, field, '0' * 64)
            with self.assertRaises(RuntimeError):
                self.main()
            setattr(self.args, field, value)
        self.args.input_dir.chmod(0o755)
        with self.assertRaises(RuntimeError):
            self.main()
        self.args.input_dir.chmod(0o700)
        for output in (self.args.input_dir / 'child', self.fixture.production.parent.parent / 'sibling'):
            self.args.output_dir = output
            with self.assertRaises(RuntimeError):
                self.main()
        self.args.output_dir = self.fixture.root / 'existing'
        self.args.output_dir.mkdir(mode=0o700)
        with self.assertRaises(FileExistsError):
            self.main()
        self.assertFalse(self.commands)

    def test_python36_syntax(self):
        for path in (SCRIPT, Path(__file__)):
            ast.parse(path.read_text(), filename=str(path), feature_version=(3, 6))


if __name__ == '__main__':
    unittest.main()

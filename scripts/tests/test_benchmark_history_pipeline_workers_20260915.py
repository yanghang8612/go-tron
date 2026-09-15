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

SCRIPT = Path(__file__).resolve().parents[1] / 'benchmark_history_pipeline_workers_20260915.py'
ROOT = SCRIPT.parents[1]


def load_module():
    spec = importlib.util.spec_from_file_location('pipeline_workers_test_' + os.urandom(6).hex(), str(SCRIPT))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PipelineWorkersBenchmarkTest(unittest.TestCase):
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

    def report(self, output, workers=4):
        report = self.fixture.report(output, 'pipeline')
        report['options'].update(iterations=1, shared_read_workers=workers)
        report['iterations'] = report['iterations'][:1]
        return report

    def launch(self, mutate=None, code=0, wait_error=None):
        def popen(command, **kwargs):
            self.commands.append((command, kwargs))
            output = Path(command[command.index('--output-dir') + 1])
            workers = int(command[command.index('--shared-read-workers') + 1])
            output.mkdir(mode=0o700)
            report = self.report(output, workers)
            report['iterations'][0]['build_wall_nanos'] += len(self.commands) * 1000000
            if mutate:
                mutate(report)
            (output / 'report.json').write_text(json.dumps(report))
            self.child = mock.Mock(pid=987654321, returncode=code)
            self.child.wait.return_value = code
            if wait_error:
                self.child.wait.side_effect = wait_error
            return self.child
        return popen

    def test_pinned_helper_is_isolated_and_load_has_no_execution(self):
        with mock.patch.object(self.m.subprocess, 'check_output', return_value=self.helper_bytes) as command, \
                mock.patch.object(self.m.subprocess, 'Popen') as spawn:
            one, two = self.m.load_helpers(), self.m.load_helpers()
        command.assert_called_with(['git', 'show', self.m.HELPER_REVISION + ':' + self.m.HELPER_PATH],
                                   cwd=str(self.m.REPO), stdin=subprocess.DEVNULL, timeout=60)
        self.assertEqual(hashlib.sha256(self.helper_bytes).hexdigest(), self.m.HELPER_SHA)
        self.assertIsNot(one, two)
        self.assertIsNot(one.__dict__, two.__dict__)
        self.assertEqual(one.__file__, str(SCRIPT.resolve()))
        self.assertEqual(one.MODES, ('serial', 'pipeline', 'pipeline', 'serial'))
        spawn.assert_not_called()
        with mock.patch.object(self.m.subprocess, 'check_output', return_value=b'raise AssertionError("executed")'):
            with self.assertRaisesRegex(RuntimeError, 'Git bytes differ'):
                self.m.load_helpers()

    def test_forward_reverse_fixed_options_and_two_complete_samples(self):
        with mock.patch.object(self.m.subprocess, 'Popen', side_effect=self.launch()), \
                mock.patch.object(self.h.os, 'killpg', side_effect=ProcessLookupError):
            self.main()
        summary = json.loads((self.args.output_dir / 'summary.json').read_text())
        self.assertTrue(summary['complete'] and summary['input_unchanged'] and summary['binary_unchanged'])
        self.assertEqual([r['workers'] for r in summary['runs']], [2, 4, 8, 8, 4, 2])
        self.assertEqual(summary['helper_sha256'], self.m.HELPER_SHA)
        self.assertEqual(summary['script_sha256'], self.h.file_sha(SCRIPT))
        self.assertEqual(len(list(self.args.output_dir.glob('*.attempt.json'))), 6)
        self.assertEqual(json.loads((self.args.output_dir / 'input-files-before.json').read_text()),
                         json.loads((self.args.output_dir / 'input-files-after.json').read_text()))
        for count, (command, kwargs) in zip(self.m.ORDER, self.commands):
            self.assertEqual(command[:3], [str(self.args.binary), 'db', 'benchmark-history-cold'])
            self.assertIn('--shared-read-pipeline=true', command)
            for flag, value in (('--shared-read-workers', str(count)), ('--copy-mode', 'owned'),
                                ('--compression-format', 'auto'), ('--iterations', '1'), ('--max-duration', '5m')):
                self.assertEqual(command[command.index(flag) + 1], value)
            self.assertNotIn('--cpu-profile', command)
            self.assertNotIn('--datadir', command)
            self.assertEqual(kwargs['env']['GOMAXPROCS'], '4')
            self.assertEqual(kwargs['env']['GOMEMLIMIT'], '10GiB')
            self.assertTrue(kwargs['start_new_session'])
        for result in summary['results'].values():
            self.assertEqual(result['samples'], 2)
            self.assertEqual(len(result['wall_seconds']), 2)
            self.assertEqual(len(result['allocated_bytes']), 2)
            self.assertEqual(len(result['whole_command_child_cpu_seconds']), 2)
            self.assertEqual(result['blocks_per_second_at_median_wall'], 16 / statistics.median(result['wall_seconds']))
        self.assertIn('whole diagnostic command', summary['resource_scope'])
        self.assertIn('not an independent', summary['resource_scope'])
        self.assertIn('not statistical proof', summary['comparison_scope'])

    def test_report_rejects_worker_type_missing_or_mismatch_and_original_proof_errors(self):
        mutations = {
            'workers-missing': lambda r: r['options'].pop('shared_read_workers'),
            'workers-boolean': lambda r: r['options'].update(shared_read_workers=True),
            'workers-string': lambda r: r['options'].update(shared_read_workers='4'),
            'workers-wrong': lambda r: r['options'].update(shared_read_workers=2),
            'workers-unsupported': lambda r: r['options'].update(shared_read_workers=16),
            'serial': lambda r: r['options'].update(shared_read_pipeline=False),
            'profile': lambda r: r['options'].update(cpu_profile=True),
            'profile-sha': lambda r: r.update(cpu_profile_sha256='e' * 64),
            'iterations': lambda r: r['options'].update(iterations=3),
            'runtime': lambda r: r.update(go_version='go1.27.1'),
            'architecture': lambda r: r.update(goarch='arm64'),
            'gomaxprocs': lambda r: r.update(gomaxprocs=8),
            'copy': lambda r: r['options'].update(copy_mode='defensive'),
            'compression': lambda r: r['options'].update(compression_format='3'),
            'duration': lambda r: r['options'].update(max_duration_ns=600000000000),
            'physical': lambda r: r.update(physical_verified=False),
            'source-report': lambda r: r['export'].update(from_block=102),
            'manifest': lambda r: r.update(manifest_file_sha256='0' * 64),
            'complete': lambda r: r.update(complete=1),
            'digest': lambda r: r['source_digest'].pop('delegation_rows'),
            'cold-digest': lambda r: r['iterations'][0]['digest'].update(row_sha256='f' * 64),
            'ordinal': lambda r: r['iterations'][0].update(iteration=2),
            'second-row': lambda r: r['iterations'].append(copy.deepcopy(r['iterations'][0])),
            'wall': lambda r: r['iterations'][0].update(build_wall_nanos=0),
            'alloc-bool': lambda r: r['iterations'][0].update(build_allocated_bytes=True),
            'verification': lambda r: r['iterations'][0].update(verification_wall_nanos=-1),
            'ref-range': lambda r: r['iterations'][0]['refs'][0].update(fromTxNum=1002),
            'ref-kind': lambda r: r['iterations'][0]['refs'][0].update(kind='inverted'),
            'ref-path': lambda r: r['iterations'][0]['refs'][0].update(path='../outside'),
            'ref-size': lambda r: r['iterations'][0]['refs'][0].update(size=101),
            'output-bytes': lambda r: r['iterations'][0].update(output_bytes=301),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                report = self.report(self.args.output_dir)
                mutate(report)
                with self.assertRaises(RuntimeError):
                    self.m.check_report(self.h, report, self.args, self.args.output_dir, 4, self.source)
        for workers in (True, 1, 3, 16, '4'):
            with self.subTest(workers=workers), self.assertRaises(RuntimeError):
                self.m.check_report(self.h, self.report(self.args.output_dir), self.args, self.args.output_dir, workers, self.source)

    def test_cross_worker_byte_or_digest_change_fails(self):
        for field in ('refs', 'source_digest'):
            self.args.output_dir = self.fixture.root / field
            self.commands = []
            def mutate(report):
                if report['options']['shared_read_workers'] != 2:
                    if field == 'refs':
                        report['iterations'][0]['refs'][0]['checksum'] = 'sha256:' + 'f' * 64
                    else:
                        report['source_digest']['row_sha256'] = 'f' * 64
                        report['iterations'][0]['digest']['row_sha256'] = 'f' * 64
            with mock.patch.object(self.m.subprocess, 'Popen', side_effect=self.launch(mutate)), \
                    mock.patch.object(self.h.os, 'killpg', side_effect=ProcessLookupError), self.assertRaises(RuntimeError):
                self.main()
            summary = json.loads((self.args.output_dir / 'summary.json').read_text())
            self.assertFalse(summary['complete'])
            self.assertTrue(summary['input_unchanged'] and summary['binary_unchanged'])
            self.assertEqual(len(self.commands), 2)

    def test_failed_wait_signal_and_exit_preserve_attempt_and_final_evidence(self):
        for index, failure in enumerate((subprocess.TimeoutExpired('fixture', 330), InterruptedError('operator'), None)):
            self.args.output_dir = self.fixture.root / ('failure-' + str(index))
            with mock.patch.object(self.m.subprocess, 'Popen', side_effect=self.launch(code=2, wait_error=failure)), \
                    mock.patch.object(self.h, 'terminate') as cleanup, self.assertRaises(type(failure) if failure else RuntimeError):
                self.main()
            cleanup.assert_called_once_with(self.child)
            attempt = json.loads((self.args.output_dir / '00-workers-2.attempt.json').read_text())
            summary = json.loads((self.args.output_dir / 'summary.json').read_text())
            self.assertFalse(attempt['complete'] or summary['complete'])
            self.assertTrue(summary['input_unchanged'] and summary['binary_unchanged'])
            for name in ('error', 'returncode', 'stdout_sha256', 'stderr_sha256', 'usage_before',
                         'usage_after', 'whole_command_child_cpu_seconds'):
                self.assertIn(name, attempt)

    def test_spawn_failure_restores_signal_mask_and_cleanup_group_semantics(self):
        self.args.output_dir.mkdir(mode=0o700)
        before = signal.pthread_sigmask(signal.SIG_BLOCK, [])
        with mock.patch.object(self.m.subprocess, 'Popen', side_effect=OSError('spawn failed')), \
                mock.patch.object(self.h, 'terminate') as cleanup, self.assertRaises(OSError):
            self.m.run_one(self.h, self.args, 0, 2, self.source)
        cleanup.assert_called_once_with(None)
        self.assertEqual(signal.pthread_sigmask(signal.SIG_BLOCK, []), before)
        child = mock.Mock(pid=987654321, returncode=0)
        with mock.patch.object(self.h.os, 'killpg') as kill:
            self.h.terminate(child)
        self.assertEqual(kill.call_args_list, [mock.call(child.pid, signal.SIGTERM), mock.call(child.pid, signal.SIGKILL)])
        self.assertEqual(child.wait.call_args_list, [mock.call(timeout=5), mock.call(timeout=10)])

    def test_input_and_binary_changes_during_interrupt_are_not_marked_complete(self):
        def fail(*unused):
            (self.args.input_dir / 'pebble' / 'fixture.sst').write_bytes(b'changed physical input')
            self.args.binary.write_bytes(b'changed binary')
            raise InterruptedError('operator')
        with mock.patch.object(self.m, 'run_one', side_effect=fail), self.assertRaises(InterruptedError):
            self.main()
        summary = json.loads((self.args.output_dir / 'summary.json').read_text())
        self.assertFalse(summary['complete'] or summary['input_unchanged'] or summary['binary_unchanged'])
        self.assertTrue((self.args.output_dir / 'input-files-after.json').exists())

    def test_pins_root_input_privacy_output_scope_and_existing_output_are_required(self):
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

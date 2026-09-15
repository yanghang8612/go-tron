import ast
import copy
import hashlib
import importlib.util
from pathlib import Path
import subprocess
import types
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / 'scripts/benchmark_history_cdc_reuse_20260915.py'
spec = importlib.util.spec_from_file_location('cdc_reuse_benchmark_tested', str(PATH))
subject = importlib.util.module_from_spec(spec)
spec.loader.exec_module(subject)


class CDCReuseBenchmarkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = subprocess.check_output(['git', 'show', subject.BASE + ':' + subject.HELPER_PATH], cwd=str(ROOT))

    def test_only_pinned_helper_is_loaded_without_running_it(self):
        self.assertEqual(hashlib.sha256(self.source).hexdigest(), subject.HELPER_SHA)
        with mock.patch.object(subject.subprocess, 'check_output', return_value=self.source) as command:
            helper = subject.load_helper()
        command.assert_called_once_with(['git', 'show', subject.BASE + ':' + subject.HELPER_PATH],
                                        cwd=str(subject.REPO), stdin=subprocess.DEVNULL, timeout=60)
        self.assertEqual(helper.__file__, str(PATH.resolve()))
        self.assertTrue(callable(helper.run_one))

    def test_bad_helper_cannot_execute(self):
        with mock.patch.object(subject.subprocess, 'check_output', return_value=b'raise AssertionError()'):
            with self.assertRaisesRegex(RuntimeError, 'helper bytes'):
                subject.load_helper()

    def matrix(self, mutation=None, fail_at=None):
        args = types.SimpleNamespace(baseline_binary=Path('/baseline'), baseline_sha='a'*64,
                                     candidate_binary=Path('/candidate'), candidate_sha='b'*64,
                                     input_dir=Path('/input'), output_dir=Path('/output'), manifest_sha='c'*64)
        calls = []
        def require(ok, message):
            if not ok:
                raise RuntimeError(message)
        def run_one(options, index, mode, source, profile):
            calls.append((copy.copy(options), index, mode, source, profile))
            if index == fail_at:
                raise RuntimeError('child failed')
            row = {'build_wall_nanos': 2000000000 if options.binary == Path('/baseline') else 1000000000,
                   'output_bytes': 123, 'build_allocated_bytes': 456}
            item = {'complete': True, 'source_digest': {'hash':'full-authenticated'},
                    'refs': [{'kind':'history','checksum':'full-file'}], 'profile':profile,
                    'iterations':[dict(row) for _ in range(1 if profile else 3)]}
            if mutation:
                mutation(index, item)
            return item
        helper = types.SimpleNamespace(require=require, run_one=run_one)
        return args, helper, calls

    def test_matrix_changes_only_binary_and_excludes_profile_timings(self):
        args, helper, calls = self.matrix()
        summary = {'runs': []}
        with mock.patch('builtins.print'):
            results = subject.run_matrix(helper, args, {'source':'same'}, summary)
        self.assertEqual([str(c[0].binary) for c in calls], ['/baseline','/candidate','/candidate','/baseline','/baseline','/candidate'])
        self.assertEqual([c[4] for c in calls], [False]*4+[True]*2)
        self.assertTrue(all(c[2] == 'serial' and c[3] == {'source':'same'} for c in calls))
        self.assertTrue(all(c[0].input_dir == Path('/input') and c[0].manifest_sha == 'c'*64 for c in calls))
        self.assertEqual(results['baseline']['wall_seconds'], [2.0]*6)
        self.assertEqual(results['candidate']['wall_seconds'], [1.0]*6)
        self.assertEqual(results['candidate']['blocks_per_second_at_median_wall'], 16.0)
        self.assertEqual(summary['runs'][1]['binary_sha256'], 'b'*64)

    def test_any_primary_or_profile_content_difference_fails_closed(self):
        for index in (1, 4, 5):
            for field in ('source_digest', 'refs'):
                def mutation(i, item):
                    if i == index:
                        item[field] = {'wrong':True}
                args, helper, calls = self.matrix(mutation)
                summary = {'runs': []}
                with mock.patch('builtins.print'), self.assertRaisesRegex(RuntimeError, 'contents differ'):
                    subject.run_matrix(helper, args, {}, summary)
                self.assertEqual(len(summary['runs']), index)
                self.assertEqual(len(calls), index+1)

    def test_failed_run_stops_later_processes(self):
        args, helper, calls = self.matrix(fail_at=2)
        summary = {'runs': []}
        with mock.patch('builtins.print'), self.assertRaisesRegex(RuntimeError, 'child failed'):
            subject.run_matrix(helper, args, {}, summary)
        self.assertEqual(len(calls), 3)
        self.assertEqual(len(summary['runs']), 2)

    def test_root_is_rejected_before_helper_or_output(self):
        argv = ['benchmark','--baseline-binary','/a','--baseline-sha','a'*64,
                '--candidate-binary','/b','--candidate-sha','b'*64,
                '--input-dir','/i','--manifest-sha','c'*64,'--output-dir','/o']
        with mock.patch.object(subject.sys, 'argv', argv), mock.patch.object(subject.sys, 'platform', 'linux'), \
                mock.patch.object(subject.os, 'geteuid', return_value=0), mock.patch.object(subject, 'load_helper') as load:
            with self.assertRaisesRegex(RuntimeError, 'capture owner'):
                subject.main()
            load.assert_not_called()

    def test_python36_syntax(self):
        for path in (PATH, Path(__file__)):
            ast.parse(path.read_text(), feature_version=(3,6))


if __name__ == '__main__':
    unittest.main()

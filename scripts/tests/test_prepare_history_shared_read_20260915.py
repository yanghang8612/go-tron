import ast
import hashlib
import importlib.util
from pathlib import Path
import subprocess
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / 'scripts/prepare_history_shared_read_20260915.py'
spec = importlib.util.spec_from_file_location('pipeline_prepare_tested', str(PATH))
subject = importlib.util.module_from_spec(spec)
spec.loader.exec_module(subject)


class PreparePipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = subprocess.check_output(['git', 'show', subject.BASE + ':' + subject.BUILDER_PATH], cwd=str(ROOT))

    def load(self):
        with mock.patch.object(subject.subprocess, 'check_output', return_value=self.base) as command:
            builder = subject.load_builder()
        command.assert_called_once_with(['git', 'show', subject.BASE + ':' + subject.BUILDER_PATH],
                                        cwd=str(subject.REPO), stdin=subprocess.DEVNULL, timeout=60)
        return builder

    def test_loading_runs_only_the_pinned_git_read(self):
        self.assertEqual(hashlib.sha256(self.base).hexdigest(), subject.BUILDER_SHA)
        builder = self.load()
        self.assertEqual(builder.__file__, str(PATH.resolve()))
        self.assertEqual(builder.BASE, subject.BASE)
        self.assertEqual(builder.RELEASE, subject.RELEASE)
        self.assertEqual(builder.SOURCE, subject.RELEASE / 'source')
        self.assertEqual(builder.BINARY, subject.RELEASE / 'gtron-inspect')
        self.assertEqual(builder.SCRIPT_PATH, subject.SCRIPT_PATH)

    def test_bad_pin_is_rejected_before_execution(self):
        with mock.patch.object(subject.subprocess, 'check_output', return_value=b'raise AssertionError("executed")'):
            with self.assertRaisesRegex(RuntimeError, 'Git bytes differ'):
                subject.load_builder()

    def test_scope_is_explicit_and_default_service_is_absent(self):
        builder = self.load()
        self.assertTrue(builder.REQUIRED_SOURCE_CHANGES <= builder.ALLOWED_SOURCE_CHANGES)
        self.assertIn('core/rawdb/state_history_chunk_cache.go', builder.REQUIRED_SOURCE_CHANGES)
        self.assertNotIn('scripts/dev/history_distribution_20260915.go', builder.ALLOWED_SOURCE_CHANGES)
        self.assertIn('cmd/gtron/main.go', builder.ALLOWED_SOURCE_CHANGES)
        self.assertIn('core/state/snapshots/cold_builder.go', builder.ALLOWED_SOURCE_CHANGES)
        self.assertNotIn('systemctl', builder.__dict__)
        self.assertIn('--shared-read-pipeline', builder.HELP_CONTRACTS['benchmark-history-cold'])
        self.assertIn('HistoryChunkCache', builder.TEST_PATTERN)
        self.assertIn('--cdc-compression-workers', builder.HELP_CONTRACTS['benchmark-history-cold'])

    def test_existing_native_build_and_cancellation_contracts_are_inherited(self):
        builder = self.load()
        self.assertEqual(builder.GO, '/data/go/bin/go')
        self.assertEqual(builder.CHECKOUT, '19eda11f44424f673a40b06051edcbf629b50846')
        self.assertEqual(builder.HELPER_SHA, '7dcbe033f5425484f9a071f1eda1ab962f4a9280ecb41a2666e82854623d6e49')
        for name in ('prepare', 'pure_helpers', 'run_logged', 'stop_child', 'verify_test_log', 'repository_state'):
            self.assertTrue(callable(getattr(builder, name)))
        self.assertIn('TestDBHistoryColdBenchmarkCompleteModesAndReadOnly', [item[1] for item in builder.REQUIRED_TESTS])

    def test_nonroot_cannot_load_or_prepare(self):
        with mock.patch.object(subject.sys, 'argv', ['prepare', '--source-revision', 'a'*40,
                                                   '--script-revision', 'b'*40, '--rust-library-sha256', 'c'*64]), \
                mock.patch.object(subject.os, 'geteuid', return_value=1000), \
                mock.patch.object(subject, 'load_builder') as load:
            with self.assertRaisesRegex(RuntimeError, 'root'):
                subject.main()
            load.assert_not_called()

    def test_python36_syntax(self):
        for path in (PATH, Path(__file__)):
            ast.parse(path.read_text(), feature_version=(3, 6))


if __name__ == '__main__':
    unittest.main()

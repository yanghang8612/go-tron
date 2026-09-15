import argparse
import ast
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import types
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / 'scripts/prepare_history_gc_accounting_20260915.py'
spec = importlib.util.spec_from_file_location('prepare_gc_tested', str(PATH))
s = importlib.util.module_from_spec(spec); spec.loader.exec_module(s)


class PrepareGCTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parent = subprocess.check_output(['git', 'show', s.BASE + ':' + s.PARENT_PATH], cwd=str(ROOT))
        cls.raw = subprocess.check_output(['git', 'show', s.BASE + ':scripts/prepare_history_parallel_20260915.py'], cwd=str(ROOT))
        cls.old = {name: subprocess.check_output(['git', 'show', s.SOURCE_REVISION + ':' + name], cwd=str(ROOT)) for name in s.OLD_OPS}

    def load(self):
        with mock.patch.object(s.subprocess, 'check_output', side_effect=[self.parent, self.raw]) as run:
            b = s.load_builder()
        self.assertEqual(run.call_count, 2)
        self.assertTrue(all(call.args[0][2].startswith(s.BASE + ':') for call in run.call_args_list))
        return b

    def test_pinned_load_is_isolated_and_only_reads_git(self):
        self.assertEqual(hashlib.sha256(self.parent).hexdigest(), s.PARENT_SHA)
        a, b = self.load(), self.load()
        self.assertIsNot(a, b)
        a.REQUIRED_TESTS += (('fake', 'Fake'),)
        self.assertNotIn(('fake', 'Fake'), b.REQUIRED_TESTS)
        self.assertEqual(b.__file__, str(PATH))
        self.assertEqual(b.SCRIPT_PATH, s.SCRIPT_PATH)
        self.assertEqual(b.BASE, s.BASE)
        self.assertEqual(b.SOURCE, s.RELEASE / 'source')
        self.assertEqual(b.GO, '/data/go/bin/go')
        self.assertEqual(b.build_environment()['GOMAXPROCS'], '2')
        self.assertTrue(set(s.SNAPSHOT_TESTS) <= set(b.REQUIRED_TESTS))
        self.assertFalse(set(s.PRUNING_TESTS) & set(b.REQUIRED_TESTS))
        self.assertIn('--shared-chunk-cache', b.HELP_CONTRACTS['benchmark-history-cold'])

    def test_tampered_helper_never_executes(self):
        for blobs in ([b'raise AssertionError()'], [self.parent, b'raise AssertionError()']):
            with self.subTest(depth=len(blobs)), mock.patch.object(s.subprocess, 'check_output', side_effect=blobs):
                with self.assertRaisesRegex(RuntimeError, 'bytes differ'): s.load_builder()

    def test_source_scope_exact_and_old_ops_unchanged(self):
        paths = subprocess.check_output(['git', 'diff', '--name-only', s.BASE, s.SOURCE_REVISION], cwd=str(ROOT)).decode().splitlines()
        self.assertEqual({p for p in paths if not p.endswith('.md')}, s.ALLOWED_FILES)
        self.assertEqual(s.REQUIRED_FILES, s.ALLOWED_FILES)
        for path, checksum in s.OLD_OPS.items(): self.assertEqual(hashlib.sha256(self.old[path]).hexdigest(), checksum)

    def preparation(self, failure=None):
        b = self.load(); calls = []; records = {}
        with tempfile.TemporaryDirectory() as directory:
            release = Path(directory); b.RELEASE, b.SOURCE, b.BINARY = release, release / 'source', release / 'gtron-inspect'
            release.chmod(0o700)
            args = argparse.Namespace(source_revision=s.SOURCE_REVISION, script_revision='a'*40, rust_library_sha256='r'*64)
            helper = types.SimpleNamespace(read_regular=lambda path, *args, **kwargs: Path(path).read_bytes(), verify_source=mock.Mock())
            record = dict(build_environment=b.build_environment(), manifest={}, checkout_after={'head': 'unchanged'},
                          go_binary_sha256='g'*64, rust_library='/pinned/rust.a', rust_library_sha256='r'*64,
                          binary_sha256='b'*64, script_sha256='s'*64, go_version='go version go1.25.5 linux/amd64')
            (release / 'prepared.json').write_text(json.dumps(record))
            checksums = {str(b.GO): 'g'*64, '/pinned/rust.a': 'r'*64, str(b.BINARY): 'b'*64,
                         str(release / 'prepared.json'): 'p'*64, str(PATH): 's'*64,
                         str(release / 'gc-accounting-native.json'): 'n'*64}
            def run(argv, name, env, timeout):
                calls.append((argv, name, env, timeout))
                if failure == name: raise KeyboardInterrupt('injected native interruption')
                events = [dict(Action='pass', Package='github.com/tronprotocol/go-tron/core/state/pruning')]
                events += [dict(Action='pass', Package='github.com/tronprotocol/go-tron/' + package, Test=test)
                           for package, test in s.PRUNING_TESTS]
                if failure == 'missing': events.pop()
                if failure == 'failed': events.append(dict(Action='fail'))
                (release / name).write_text('\n'.join(json.dumps(row) for row in events))
                return dict(argv=argv, cwd=str(b.SOURCE), returncode=0, log=name, log_sha256='l'*64)
            def write(name, value): records[name] = value
            with mock.patch.object(s, 'RELEASE', release), mock.patch.object(b, 'prepare', return_value=dict(prepared=True, prepared_sha256='p'*64, binary_sha256='b'*64)), \
                 mock.patch.object(b, 'git', side_effect=lambda *a: self.old[a[1].split(':', 1)[1]]), \
                 mock.patch.object(b, 'pure_helpers', return_value=helper), mock.patch.object(b, 'run_logged', side_effect=run), \
                 mock.patch.object(b, 'write_json', side_effect=write), mock.patch.object(b, 'file_sha', side_effect=lambda path: checksums[str(path)]), \
                 mock.patch.object(b, 'repository_state', return_value={'head': 'unchanged'}):
                if failure:
                    with self.assertRaises((RuntimeError, KeyboardInterrupt)): s.prepare(b, args)
                else:
                    result = s.prepare(b, args)
                    self.assertTrue(result['gc_accounting_complete'])
                    self.assertEqual(result['native_sha256'], 'n'*64)
                    self.assertEqual(release.stat().st_mode & 0o777, 0o755)
            return calls, records

    def test_separate_pruning_focused_and_full_with_native_environment(self):
        calls, records = self.preparation()
        self.assertEqual(len(calls), 2)
        for argv, name, env, timeout in calls:
            self.assertEqual(argv[:8], ['/data/go/bin/go', 'test', '-json', '-p', '2', '-tags', 'sapling', './core/state/pruning'])
            self.assertEqual(argv[-2:], ['-count=1', '-timeout=300s'])
            self.assertEqual(env['CGO_ENABLED'], '1'); self.assertEqual(env['GOMAXPROCS'], '2')
            self.assertEqual(env['GOTOOLCHAIN'], 'local'); self.assertNotIn('GOROOT', env)
            self.assertEqual(timeout, 660)
        self.assertIn('-run', calls[0][0]); self.assertNotIn('-run', calls[1][0])
        proof = records['gc-accounting-native.json']
        self.assertTrue(proof['complete']); self.assertEqual(proof['prepared_sha256'], 'p'*64)
        self.assertEqual(proof['source_commit'], s.SOURCE_REVISION)
        self.assertEqual(proof['native_full_tests']['required_tests'], [list(p) for p in s.PRUNING_TESTS])

    def test_interruption_missing_pass_and_failure_cannot_attest_complete(self):
        for failure in ('native-gc-pruning-focused.jsonl', 'native-gc-pruning-full.jsonl', 'missing', 'failed'):
            with self.subTest(failure=failure):
                calls, records = self.preparation(failure)
                self.assertNotIn('gc-accounting-native.json', records)
                self.assertFalse(records['gc-accounting-native-failure.json']['complete'])

    def test_unfrozen_or_wrong_source_stops_before_build(self):
        b = self.load()
        for frozen, source in ((False, s.SOURCE_REVISION), (True, 'b'*40)):
            with mock.patch.object(s, 'SCOPE_FROZEN', frozen), mock.patch.object(b, 'prepare') as prepare:
                with self.assertRaisesRegex(RuntimeError, 'scope'): s.prepare(b, argparse.Namespace(source_revision=source))
                prepare.assert_not_called()

    def test_python36_syntax(self):
        for path in (PATH, Path(__file__)): ast.parse(path.read_text(), feature_version=(3, 6))


if __name__ == '__main__': unittest.main()

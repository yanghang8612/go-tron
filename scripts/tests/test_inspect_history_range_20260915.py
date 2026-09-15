"""Local pure/fake operational checks; no service, source DB or server access."""
import argparse
import base64
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('history_range_ops', ROOT / 'scripts/inspect_history_range_20260915.py')
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)


class FakeOps:
    def __init__(self, fail=None, incomplete=False):
        self.fail, self.incomplete, self.calls = fail or {}, incomplete, []

    def step(self, name):
        self.calls.append(name)
        if name in self.fail:
            raise self.fail[name]

    def before(self):
        self.step('before')
        return {'process': {'argv': [m.CURRENT_EXE]}}

    def save(self, report):
        pass

    def stop(self):
        self.step('stop')

    def assert_stopped(self):
        self.step('assert_stopped')

    def probe(self):
        self.step('probe')
        return {'ok': not self.incomplete}

    def cancel_probe(self):
        self.step('cancel_probe')

    def restore(self, before):
        self.step('restore')
        return {'healthy': True}


def saved(path, content):
    return {'path': str(path), 'sha256': m.sha(content), 'data_b64': base64.b64encode(content).decode(),
            'uid': 0, 'gid': 0, 'mode': 0o600, 'xattrs': {}}


def encoded(value):
    return (json.dumps(value, sort_keys=True) + '\n').encode()


class RangeOpsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Only four git-show reads/immutable module definitions, not entrypoints.
        with patch.object(m, 'REPO', ROOT):
            cls.engine, cls.helper = m.load_modules()

    def test_pinned_modules_are_direct_and_all_hashes_match(self):
        self.assertEqual(len(m.MODULES), 4)
        self.assertIsNotNone(self.engine.inspect_transaction)
        self.assertEqual(self.engine.BINARY, m.BINARY)
        self.assertEqual(self.helper.BINARY, Path(m.CURRENT_EXE))
        self.assertNotEqual(self.engine.BINARY, self.helper.BINARY)

    def test_complete_stop_probe_then_restore(self):
        ops = FakeOps()
        report = self.engine.inspect_transaction(ops)
        self.assertTrue(report['ok'])
        self.assertTrue(report['restored'])
        self.assertEqual(ops.calls, ['before', 'stop', 'assert_stopped', 'probe', 'cancel_probe', 'restore'])

    def test_stop_probe_and_signal_failures_still_restore_old_service(self):
        for phase, error in [('stop', subprocess.TimeoutExpired('systemctl stop', 660)),
                             ('assert_stopped', RuntimeError('not stopped')),
                             ('probe', subprocess.TimeoutExpired('probe', 120)),
                             ('probe', KeyboardInterrupt()), ('probe', RuntimeError('bad JSON'))]:
            with self.subTest(phase=phase, error=repr(error)):
                ops = FakeOps({phase: error})
                report = self.engine.inspect_transaction(ops)
                self.assertFalse(report['ok'])
                self.assertTrue(report['restored'])
                self.assertEqual(ops.calls[-2:], ['cancel_probe', 'restore'])
                if phase != 'probe':
                    self.assertNotIn('probe', ops.calls)

    def test_admission_failure_never_stops(self):
        ops = FakeOps({'before': RuntimeError('marker changed')})
        with self.assertRaisesRegex(RuntimeError, 'marker changed'):
            self.engine.inspect_transaction(ops)
        self.assertEqual(ops.calls, ['before'])

    def test_partial_capture_is_failure_but_restores(self):
        report = self.engine.inspect_transaction(FakeOps(incomplete=True))
        self.assertFalse(report['ok'])
        self.assertTrue(report['restored'])

    def test_unreaped_child_prevents_concurrent_restart(self):
        ops = FakeOps({'cancel_probe': RuntimeError('unreaped child')})
        report = self.engine.inspect_transaction(ops)
        self.assertFalse(report['restored'])
        self.assertNotIn('restore', ops.calls)

    def test_changed_guard_or_new_hold_blocks_restore_without_edit(self):
        ops = FakeOps({'restore': RuntimeError('guard changed')})
        report = self.engine.inspect_transaction(ops)
        self.assertFalse(report['restored'])
        self.assertEqual(report['phase'], 'recovery-required')

    def test_only_bounded_read_only_export_command(self):
        argv = m.probe_argv(argparse.Namespace(from_block=None))
        self.assertEqual(argv[:3], [str(m.BINARY), 'db', 'export-history-range'])
        self.assertEqual(argv[argv.index('--blocks') + 1], '16')
        self.assertEqual(argv[argv.index('--max-duration') + 1], '60s')
        self.assertEqual(argv[argv.index('--output-dir') + 1], str(m.COPY))
        self.assertNotIn('--from-block', argv)
        explicit = m.probe_argv(argparse.Namespace(from_block=30971351))
        self.assertEqual(explicit[-2:], ['--from-block', '30971351'])
        for start in (-1, (1 << 64) - 15, True):
            with self.assertRaises(RuntimeError):
                m.probe_argv(argparse.Namespace(from_block=start))

    def test_full_git_identity_and_tracked_dirty_rejection(self):
        source, ops = 'a' * 40, 'b' * 40
        required = ['cmd/gtron/db_history_range.go', 'core/rawdb/history_range_export.go']
        dirty = ['']
        def fake_git(*args):
            if args[0] == 'rev-parse':
                ref = args[-1].split('^')[0]
                return ({'HEAD': m.CHECKOUT, 'refs/remotes/origin/master': ops}.get(ref, ref) + '\n').encode()
            if args[0] == 'status':
                return dirty[0].encode()
            if args[0] == 'diff':
                return ('\n'.join('A\t' + path for path in required) + '\n').encode()
            if args[0] == 'show':
                return b'exact ops'
            if args[0] == 'ls-tree':
                return b'100644 blob ' + b'c' * 40 + b'\tcmd/gtron/db_history_range.go\0' + b'160000 commit ' + b'd' * 40 + b'\tthird_party/librustzcash\0'
            raise AssertionError(args)
        h = types.SimpleNamespace(run=lambda *args, **kwargs: ('', 0))
        with patch.object(m, 'git', fake_git), patch.object(m, 'read_regular', return_value=b'exact ops'), \
                patch.object(m, 'SOURCE_REVISION', source), patch.object(m, 'ALLOWED_FILES', tuple(required)):
            report = m.validate_revision(h, source, ops)
            self.assertEqual(report['source_commit'], source)
            dirty[0] = ' M cmd/gtron/main.go\n'
            with self.assertRaisesRegex(RuntimeError, 'tracked'):
                m.validate_revision(h, source, ops)
            dirty[0] = ' M third_party/librustzcash\n'
            self.assertEqual(m.validate_revision(h, source, ops)['script_commit'], ops)
            for value in ('master', 'a' * 39, 'A' * 40, source + ';echo BAD'):
                with self.assertRaises(RuntimeError):
                    m.validate_revision(h, value, ops)

    def test_source_manifest_checks_unchanged_blob_and_extra_file(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            target = source / 'file.go'
            target.write_bytes(b'fixed source')
            target.chmod(0o644)
            oid = hashlib.sha1(b'blob 12\0fixed source').hexdigest()
            manifest = {'file.go': {'git_blob': oid, 'mode': 0o644}}
            with patch.object(m, 'SOURCE', source):
                m.verify_source(None, manifest)
                self.assertEqual(manifest['file.go']['sha256'], m.sha(b'fixed source'))
                (source / 'unreviewed.go').write_text('extra')
                with self.assertRaisesRegex(RuntimeError, 'untracked'):
                    m.verify_source(None, manifest)
                (source / 'unreviewed.go').unlink()
                target.write_bytes(b'changed')
                with self.assertRaisesRegex(RuntimeError, 'Git blob'):
                    m.verify_source(None, manifest)

    def test_range_admission_binds_native_tests_and_builder_record(self):
        args = argparse.Namespace(source_revision='a' * 40, script_revision='b' * 40)
        record = {'range_cli_verified': True, 'source_commit': args.source_revision, 'script_commit': args.script_revision,
                  'range_builder_record_sha256': 'c' * 64, 'native_replay_tests_sha256': 'd' * 64}
        values = {'prepared.json': 'c' * 64, 'native-replay-tests.log': 'd' * 64}
        i = types.SimpleNamespace(verify_prepared=Mock())
        h = types.SimpleNamespace(file_sha=lambda path: values[Path(path).name])
        m.verify_range_prepared(i, h, record, args)
        for filename in values:
            old = values[filename]
            values[filename] = 'e' * 64
            with self.assertRaises(RuntimeError):
                m.verify_range_prepared(i, h, record, args)
            values[filename] = old

    def test_native_prepare_runs_full_replay_packages_before_admission(self):
        with tempfile.TemporaryDirectory() as directory:
            release = Path(directory)
            record = {'preflight': {}, 'manifest': {}}
            i = types.SimpleNamespace(GO='/native/go', prepare=lambda *args: {'prepared': True},
                                      verify_prepared=Mock(), snapshot=lambda h: {}, same_configuration=Mock())
            h = types.SimpleNamespace(CACHE_OBSERVER_ENV='CACHE', OBSERVER_ENV='OBSERVER', PRUNE_ENV='PRUNE',
                                      HISTORY_RANGE_ENV='RANGE', HISTORY_QUEUE_ENV='QUEUE',
                                      run=Mock(return_value=('--input-dir --output-dir --blocks --max-bytes --max-decoded-bytes --max-rows --max-duration --compression-format --copy-mode', 0)),
                                      load_json=lambda name: record, file_sha=lambda path: 'a' * 64, save_json=Mock())
            args = argparse.Namespace(source_revision='b' * 40, script_revision='c' * 40)
            with patch.object(m, 'RELEASE', release), patch.object(m, 'verify_source'):
                out = m.prepare_locked(i, h, args)
            self.assertTrue(out['prepared'])
            test_calls = [call for call in h.run.call_args_list if 'test' in call[0][0]]
            self.assertEqual(len(test_calls), 1)
            argv = test_calls[0][0][0]
            self.assertIn('./core/state/snapshots', argv)
            self.assertIn('./cmd/gtron', argv)
            self.assertNotIn('-run', argv)
            self.assertEqual(test_calls[0][1]['env']['GOTOOLCHAIN'], 'local')
            self.assertEqual(test_calls[0][1]['env']['GOMAXPROCS'], '2')
            self.assertTrue(record['range_cli_verified'])

    def test_attestation_chain_preserves_all_permanent_markers(self):
        with patch.object(m, 'REPO', ROOT):
            guard = m.pinned_module('guard')
        root_release = Path('/data/gtron/releases/20260913-history-sharing')
        old_source, old_sha = 'd656be3c43eb237fac4d46a8847e5fe551bc6a78', 'b' * 64
        old_identity = guard.reader_marker(root_release / 'gtron', old_sha, old_source)
        old_global = saved(m.MARKER, encoded(old_identity))
        old_permanent = saved(root_release / 'reader-required.json', encoded(old_identity))
        guard_file = saved(m.READER_GUARD, b'fixed guard')
        ancestor = {'prepared': True, 'shared_prepared': True, 'source_commit': old_source, 'binary_sha256': old_sha}
        top = {'prepared': True, 'upgrade_prepared': True, 'source_commit': m.CURRENT_SOURCE, 'binary_sha256': m.CURRENT_SHA,
               'old_marker': old_global, 'old_armed': old_permanent, 'reader_guard': guard_file,
               'builder_record_sha256': 'c' * 64, 'history_tests_sha256': 'd' * 64,
               'old_prepared_sha256': m.sha(encoded(ancestor))}
        current_identity = guard.reader_marker(m.CURRENT_EXE, m.CURRENT_SHA, m.CURRENT_SOURCE)
        current_global = m.changed_saved(old_global, encoded(current_identity))
        files = {m.MARKER: current_global, m.READER_GUARD: guard_file, old_permanent['path']: old_permanent,
                 str(m.CURRENT_RELEASE / 'reader-required.json'): m.changed_saved(current_global, encoded(current_identity), m.CURRENT_RELEASE / 'reader-required.json')}
        raw = {str(m.CURRENT_RELEASE / 'upgrade-prepared.json'): encoded(top), str(root_release / 'shared-prepared.json'): encoded(ancestor),
               str(m.CURRENT_RELEASE / 'source-commit'): (m.CURRENT_SOURCE + '\n').encode(),
               str(m.CURRENT_RELEASE / 'SHA256SUMS'): (m.CURRENT_SHA + '  gtron\n').encode()}
        h = types.SimpleNamespace(saved_file=lambda path: files[str(path)], file_sha=lambda path: 'c' * 64 if Path(path).name == 'prepared.json' else 'd' * 64)
        with patch.object(m, 'CURRENT_PREPARED_SHA', m.sha(encoded(top))), patch.object(m, 'read_regular', side_effect=lambda path, **kw: raw[str(path)]):
            self.assertEqual(m.production_record(h, guard), top)
            files[old_permanent['path']] = saved(old_permanent['path'], encoded(dict(old_identity, binary_sha256='e' * 64)))
            with self.assertRaisesRegex(RuntimeError, 'ancestor permanent'):
                m.production_record(h, guard)

    def test_configuration_derivation_keeps_environment_and_guard(self):
        old_exe, old_sha, old_source = '/data/gtron/releases/old/gtron', 'c' * 64, 'd' * 40
        config = {'process': {'exe': old_exe}, 'main': {'files': [saved('/etc/systemd/system/gtron.service', ('ExecStart=' + old_exe + ' --history.cross-block-dedup=true').encode()),
                   saved(m.READER_DROPIN, ('guard --binary ' + old_exe + ' --sha256 ' + old_sha + ' --source ' + old_source).encode())],
                   'properties': {'ExecStart': 'path=' + old_exe + '; argv[]=' + old_exe,
                                  'ExecStartPre': old_exe + ' ' + old_sha + ' ' + old_source, 'Environment': 'GTRON_HISTORY_RANGE_QUEUE=1'}}}
        record = {'current': config, 'old_marker': saved(m.MARKER, encoded({'binary': old_exe, 'binary_sha256': old_sha, 'source_commit': old_source}))}
        result = m.production_configuration(record)
        self.assertEqual(result['main']['properties']['Environment'], config['main']['properties']['Environment'])
        self.assertIn(m.CURRENT_EXE, result['main']['properties']['ExecStart'])
        self.assertIn(m.CURRENT_SHA, result['main']['properties']['ExecStartPre'])
        self.assertEqual(config['process']['exe'], old_exe)


if __name__ == '__main__':
    unittest.main()

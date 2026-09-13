"""Local-only synthetic evidence/failure tests; never access services or a DB."""
import argparse
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('boundary_inspection', ROOT / 'scripts/inspect_history_boundary_20260913.py')
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)


def synthetic_report(interval):
    return {'inspection': {'complete': True, 'stop_reason': 'complete', 'packs_complete': 64, 'missing_packs': 0,
                           'encoded_bytes_read': 64, 'encoded_bytes_accepted': 64, 'decoded_bytes_reserved': 128,
                           'rows': 64, 'options': {'from_block': interval['from_block'], 'to_block': interval['to_block'],
                                                  'samples': 64, 'seed': 20260913, 'max_rows': m.ROWS,
                                                  'max_encoded_bytes': m.ENCODED_BYTES, 'max_decoded_bytes': m.DECODED_BYTES,
                                                  'max_duration_ns': 90000000000},
                           'samples': [{'block': height, 'status': 'complete', 'codec': 'snappy1'}
                                       for height in interval['samples']]}}


class IntervalTests(unittest.TestCase):
    def test_exact_two_sides_choose_only_already_complete_blocks(self):
        for head in (1055, 2047, 2048, 2078, 2079, 30474046):
            interval = m.select_interval(head)
            boundary = interval['bucket_boundary']
            self.assertEqual(interval['samples'], list(range(boundary - 32, boundary + 32)))
            self.assertLessEqual(interval['to_block'], head)
            self.assertEqual(boundary % 1024, 0)
            self.assertEqual(sum(height < boundary for height in interval['samples']), 32)
        self.assertEqual(m.select_interval(2078)['bucket_boundary'], 1024)
        self.assertEqual(m.select_interval(2079)['bucket_boundary'], 2048)

    def test_invalid_head_is_rejected(self):
        for value in (-1, 1054, True, 2048.0, 1 << 64, None):
            with self.assertRaises(RuntimeError):
                m.select_interval(value)

    def test_command_uses_only_old_inspector_with_fixed_budgets(self):
        command = m.probe_argv(argparse.Namespace(from_block=992, to_block=1055, export_packs=True))
        self.assertEqual(command[:3], [str(m.BINARY), 'db', 'inspect-history-prev'])
        self.assertNotEqual(command[0], m.CURRENT_EXE)
        for key, value in (('--samples', '64'), ('--max-duration', '90s'), ('--max-encoded-bytes', str(256 << 20)),
                           ('--max-decoded-bytes', str(1 << 30)), ('--export-packs', str(m.PACKS))):
            self.assertEqual(command[command.index(key) + 1], value)
        self.assertEqual(m.PROBE_TIMEOUT, 120)
        self.assertNotIn(str(m.PACKS), (m.CURRENT_EXE, str(m.DIAGNOSTIC_RELEASE)))

    def test_wrong_range_or_missing_export_never_reaches_stop(self):
        for low, high, export in ((993, 1056, True), (992, 1054, True), (992, 1055, False), (992.0, 1055, True)):
            with self.assertRaises(RuntimeError):
                m.probe_argv(argparse.Namespace(from_block=low, to_block=high, export_packs=export))

    def test_full_report_preserves_exact_samples_and_integer_budgets(self):
        interval = m.select_interval(2079)
        report = synthetic_report(interval)
        self.assertEqual(m.validate_complete_samples(report, interval)['packs_complete'], 64)
        for field, value in (('complete', False), ('missing_packs', 1), ('packs_complete', 63),
                             ('encoded_bytes_read', m.ENCODED_BYTES + 1), ('rows', True)):
            bad = copy.deepcopy(report)
            bad['inspection'][field] = value
            with self.assertRaises(RuntimeError):
                m.validate_complete_samples(bad, interval)
        for change in ('height', 'codec', 'options'):
            bad = copy.deepcopy(report)
            if change == 'height':
                bad['inspection']['samples'][1]['block'] += 1
            elif change == 'codec':
                bad['inspection']['samples'][1]['codec'] = 'shared3'
            else:
                bad['inspection']['options']['max_decoded_bytes'] *= 2
            with self.assertRaises(RuntimeError):
                m.validate_complete_samples(bad, interval)


class ReuseEvidenceTests(unittest.TestCase):
    def fixture(self, directory, gate=False):
        release = Path(directory)
        source = m.CURRENT_SOURCE if gate else m.DIAGNOSTIC_SOURCE
        binary = release / ('gtron' if gate else 'gtron-inspect')
        binary.write_bytes(b'synthetic existing native binary')
        relative = 'core/rawdb/fixture.go'
        target = release / 'source' / relative
        target.parent.mkdir(parents=True)
        target.write_bytes(b'synthetic frozen source')
        record = {'prepared': True, 'source_commit': source, 'base_commit': m.SOURCE_BASE,
                  'helper_sha256': 'a0fcf33a7ccf09ad45dab5d8bc5f101955d36e8193d022854bf2f0568c6ff70b',
                  'binary_sha256': m.sha(binary.read_bytes()), 'script_commit': 'a' * 40,
                  'script_sha256': m.sha(b'synthetic ops'), 'manifest': {relative: m.sha(target.read_bytes())},
                  'changed_files': [relative]}
        if gate:
            (release / 'source-commit').write_text(source + '\n')
            (release / 'SHA256SUMS').write_text(record['binary_sha256'] + '  gtron\n')
            (release / 'prepared.json').write_text('synthetic native prepare')
            (release / 'native-history-tests.log').write_text('synthetic native test evidence')
            record.update(gate_prepared=True, builder_sha256=m.BUILDER_SHA,
                          builder_record_sha256=m.sha((release / 'prepared.json').read_bytes()),
                          history_tests_sha256=m.sha((release / 'native-history-tests.log').read_bytes()))
        path = release / ('gate-prepared.json' if gate else 'prepared.json')
        path.write_text(json.dumps(record))
        calls = []
        def run(command, **unused):
            calls.append(command)
            return 'M\t' + relative + '\n', 0
        h = types.SimpleNamespace(file_sha=lambda path: m.sha(Path(path).read_bytes()), run=run)
        def blob(revision, path):
            return b'synthetic ops' if path.startswith('scripts/') else b'synthetic frozen source'
        return release, binary, source, path, record, h, blob, calls

    def test_reuse_derives_binary_sha_from_checked_native_record_without_build(self):
        for gate in (False, True):
            with self.subTest(gate=gate), tempfile.TemporaryDirectory() as directory:
                release, binary, source, _, record, h, blob, calls = self.fixture(directory, gate)
                with patch.object(m, 'git_blob', side_effect=blob):
                    verified = m.verify_native_record(h, release, binary, source, gate)
                self.assertEqual(verified['binary_sha256'], record['binary_sha256'])
                self.assertTrue(all(command[:2] == ['git', 'diff'] for command in calls))

    def test_changed_binary_source_ops_and_prepare_fail_closed(self):
        for altered in ('binary', 'source', 'ops', 'prepared', 'manifest'):
            with self.subTest(altered=altered), tempfile.TemporaryDirectory() as directory:
                release, binary, source, path, record, h, blob, _ = self.fixture(directory)
                if altered == 'binary':
                    binary.write_bytes(b'changed')
                elif altered == 'source':
                    (release / 'source/core/rawdb/fixture.go').write_bytes(b'changed')
                elif altered == 'ops':
                    record['script_sha256'] = '0' * 64
                elif altered == 'prepared':
                    record['prepared'] = False
                else:
                    record['manifest']['unexpected.go'] = '0' * 64
                path.write_text(json.dumps(record))
                with patch.object(m, 'git_blob', side_effect=blob), self.assertRaises(RuntimeError):
                    m.verify_native_record(h, release, binary, source)

    def test_gate_native_test_or_checksum_marker_change_is_rejected(self):
        for filename in ('native-history-tests.log', 'prepared.json', 'SHA256SUMS', 'source-commit'):
            with tempfile.TemporaryDirectory() as directory:
                release, binary, source, _, _, h, blob, _ = self.fixture(directory, True)
                (release / filename).write_text('changed')
                with patch.object(m, 'git_blob', side_effect=blob), self.assertRaises(RuntimeError):
                    m.verify_native_record(h, release, binary, source, True)


class PinnedTransactionTests(unittest.TestCase):
    def modules(self):
        with patch.object(m, 'REPO', ROOT):
            return m.load_modules(ROOT / 'scripts/deploy_range_scheduling_20260913.py')

    def test_fixed_helpers_load_and_sha_mismatch_rejects(self):
        i, _ = self.modules()
        self.assertEqual(i.HELPER_SHA, 'a0fcf33a7ccf09ad45dab5d8bc5f101955d36e8193d022854bf2f0568c6ff70b')
        with patch.object(m, 'REPO', ROOT), patch.object(m, 'BUILDER_SHA', '0' * 64), self.assertRaises(RuntimeError):
            m.load_modules(ROOT / 'scripts/deploy_range_scheduling_20260913.py')

    def test_prepare_only_verifies_reuse_and_holds_prepare_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            release = Path(directory)
            calls = []
            i = types.SimpleNamespace(snapshot=lambda h: {'process': {'pid': m.CURRENT_PID}},
                                      same_configuration=lambda *args: None, HELPER_SHA='a' * 64)
            def run(command, **unused):
                calls.append(command)
                with open(str(release / '.prepare.lock'), 'a') as second:
                    with self.assertRaises(BlockingIOError):
                        m.fcntl.flock(second, m.fcntl.LOCK_EX | m.fcntl.LOCK_NB)
                return '--from-block --to-block --samples --export-packs --max-duration --max-encoded-bytes --max-decoded-bytes', 0
            h = types.SimpleNamespace(wallet_head=lambda: 2079, run=run,
                                      file_sha=lambda path: hashlib.sha256(Path(path).read_bytes()).hexdigest(),
                                      save_json=lambda name, value: (release / name).write_text(json.dumps(value)))
            with patch.object(m, 'RELEASE', release), patch.object(m, 'RESULT', release / 'result'), \
                    patch.object(m, 'validate_ops'), patch.object(m, 'verify_prepared'):
                result = m.prepare(i, h, argparse.Namespace(script_revision='a' * 40), {'synthetic': True})
            self.assertTrue(result['prepared'])
            self.assertFalse(result['build_performed'])
            self.assertEqual(calls, [[str(m.BINARY), 'db', 'inspect-history-prev', '--help']])

    def test_current_pins_queue_and_static_normalization(self):
        i, h = self.modules()
        m.configure_modules(i, h, {'production': {'binary_sha256': 'a' * 64}})
        self.assertEqual((i.CURRENT_PID, i.CURRENT_TICKS, i.CURRENT_SHA), (22687, 4503185044, 'a' * 64))
        self.assertEqual(i.BINARY, m.BINARY)
        self.assertEqual(h.expected_history_queue_environment(m.CURRENT_EXE), '1')
        self.assertEqual(h.expected_history_range_environment(m.CURRENT_EXE), '1')
        with self.assertRaises(RuntimeError):
            h.expected_history_queue_environment('/different/gtron')
        value = ('{ path=' + m.CURRENT_EXE + ' ; argv[]=' + m.CURRENT_EXE + ' --datadir /data/gtron/main/datadir ; '
                 'ignore_errors=no ; start_time=[x] ; stop_time=[n/a] ; pid=22687 ; code=(null) ; status=0/0 }')
        stopped = value.replace('pid=22687', 'pid=99999').replace('code=(null) ; status=0/0', 'code=killed ; status=11/SEGV')
        self.assertEqual(i.normalize_exec_start(value), i.normalize_exec_start(stopped))
        self.assertNotEqual(i.normalize_exec_start(value), i.normalize_exec_start(stopped.replace('datadir', 'other-dir')))

    def test_metric_types_and_same_process_identity(self):
        good = {name: {'count': 0} for name in m.SMALL_METRICS}
        self.assertEqual(len(m.small_metric_values(good)), 4)
        for bad in (True, -1, 0.0, None):
            changed = copy.deepcopy(good)
            changed[m.SMALL_METRICS[0]]['count'] = bad
            with self.assertRaises(RuntimeError):
                m.small_metric_values(changed)
        i, h = self.modules()
        h.verify_observer_runtime = lambda *unused: {'process_start_unix_nano': m.CURRENT_START_NANO}
        m.configure_modules(i, h, {'production': {'binary_sha256': 'a' * 64}})
        good['process/start/unix_nano'] = {'value': m.CURRENT_START_NANO + 1}
        with patch.object(m, 'read_local_metrics', return_value=good), self.assertRaisesRegex(RuntimeError, 'process changed'):
            h.verify_observer_runtime(m.CURRENT_PID, m.CURRENT_EXE)

    def test_incomplete_exception_and_interrupt_restore_through_pinned_transaction(self):
        i, _ = self.modules()
        for failure in (RuntimeError('missing pack'), InterruptedError('timeout'), KeyboardInterrupt()):
            calls = []
            def step(name):
                calls.append(name)
            def probe():
                step('probe')
                raise failure
            ops = types.SimpleNamespace(before=lambda: {}, save=lambda report: None,
                                        stop=lambda: step('stop'), assert_stopped=lambda: step('stopped'), probe=probe,
                                        cancel_probe=lambda: step('reap'), restore=lambda before: step('restore'))
            result = i.inspect_transaction(ops)
            self.assertFalse(result['ok'])
            self.assertTrue(result['restored'])
            self.assertEqual(calls, ['stop', 'stopped', 'probe', 'reap', 'restore'])

    def test_unreaped_child_blocks_restart_and_preflight_failure_never_stops(self):
        i, _ = self.modules()
        calls = []
        def fail():
            raise RuntimeError('unreaped')
        ops = types.SimpleNamespace(before=lambda: {}, save=lambda report: None, stop=lambda: calls.append('stop'),
                                    assert_stopped=lambda: None, probe=lambda: {'ok': True}, cancel_probe=fail,
                                    restore=lambda before: calls.append('restore'))
        result = i.inspect_transaction(ops)
        self.assertFalse(result['restored'])
        self.assertNotIn('restore', calls)
        calls.clear()
        ops.before = fail
        with self.assertRaises(RuntimeError):
            i.inspect_transaction(ops)
        self.assertEqual(calls, [])


class ExportTests(unittest.TestCase):
    def fixture(self, directory):
        packs = Path(directory) / 'packs'
        packs.mkdir(mode=0o700)
        interval = m.select_interval(2079)
        entries = []
        for height in interval['samples']:
            filename = '{0:020d}.pack'.format(height)
            data = ('synthetic pack ' + str(height)).encode()
            (packs / filename).write_bytes(data)
            os.chmod(str(packs / filename), 0o600)
            entries.append({'block': height, 'file': filename, 'codec': 'raw', 'sha256': hashlib.sha256(data).hexdigest(),
                            'encoded_bytes': len(data), 'decoded_bytes': len(data)})
        manifest = {'version': 1, 'inspection_complete': True, 'inspection_stop_reason': 'complete', 'entries': entries}
        (packs / 'manifest.json').write_text(json.dumps(manifest))
        os.chmod(str(packs / 'manifest.json'), 0o600)
        return packs, interval, manifest

    def test_all_64_hashes_are_verified_after_restore(self):
        with tempfile.TemporaryDirectory() as directory:
            packs, interval, _ = self.fixture(directory)
            with patch.object(m, 'PACKS', packs):
                self.assertEqual(m.verify_export_after_restore(interval)['files'], 64)

    def test_tamper_symlink_extra_file_and_incomplete_manifest_are_rejected(self):
        for change in ('tamper', 'symlink', 'extra', 'partial', 'budget'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                packs, interval, manifest = self.fixture(directory)
                first = packs / manifest['entries'][0]['file']
                if change == 'tamper':
                    first.write_bytes(b'tampered')
                elif change == 'symlink':
                    first.unlink()
                    first.symlink_to(packs / manifest['entries'][1]['file'])
                elif change == 'extra':
                    (packs / 'unexpected').write_text('extra')
                else:
                    if change == 'partial':
                        manifest['inspection_complete'] = False
                    else:
                        manifest['entries'][0]['encoded_bytes'] = m.ENCODED_BYTES + 1
                    (packs / 'manifest.json').write_text(json.dumps(manifest))
                with patch.object(m, 'PACKS', packs), self.assertRaises((RuntimeError, OSError)):
                    m.verify_export_after_restore(interval)


if __name__ == '__main__':
    unittest.main(verbosity=2)

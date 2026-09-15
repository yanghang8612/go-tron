import argparse
import ast
import base64
import copy
import importlib.util
import json
import os
from pathlib import Path
import signal
import stat
import sys
import tempfile
import unittest
from unittest import mock
import urllib.error

ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / 'scripts/activate_history_shared_read_20260915.py'
spec = importlib.util.spec_from_file_location('activation_tested', str(PATH))
s = importlib.util.module_from_spec(spec); spec.loader.exec_module(s)
GUARD_BYTES = (ROOT / 'scripts/history_shared_reader_guard.py').read_bytes()


def item(path, data, mode=0o644):
    return dict(path=str(path), data_b64=base64.b64encode(data).decode(), sha256=s.sha(data), uid=0, gid=0, mode=mode)


def fixture():
    g = s.module(GUARD_BYTES, s.GUARD_SHA, 'test_guard')
    argv = [s.OLD_BINARY, '--db.cache', '8192', '--history.cross-block-dedup=true', '--http.port', '8090']
    pre = '/usr/bin/python3 ' + s.GUARD + ' --binary ' + s.OLD_BINARY + ' --sha256 ' + s.OLD_SHA + ' --source ' + s.OLD_SOURCE + ' --marker ' + s.MARKER
    files = [item(s.EXEC, ('[Service]\nExecStart=\nExecStart=' + ' '.join(argv) + '\nEnvironment=KEEP=1\n').encode()),
             item(s.DROPIN, ('[Service]\nExecStartPre=' + pre + '\n').encode()),
             item(s.MARKER, json.dumps(g.reader_marker(s.OLD_BINARY, s.OLD_SHA, s.OLD_SOURCE)).encode()),
             item(s.GUARD, GUARD_BYTES, 0o755), item(s.SPACE_CONFIG, b'{"start_bytes":123}'),
             item(s.SPACE_GUARD, b'keep existing space protection', 0o755)]
    command = lambda path, argv: {'path': path, 'argv[]': argv, 'ignore_errors': 'no'}
    config = {k: '' for k in s.CONFIG_KEYS}
    config.update(ExecStart=[command(s.OLD_BINARY, ' '.join(argv))],
                  ExecStartPre=[command('/usr/bin/python3', '/usr/bin/python3 ' + s.SPACE_GUARD + ' check'), command('/usr/bin/python3', pre)],
                  ExecStartPost=[])
    record = dict(args={'binary_sha': 'b'*64, 'source_revision': 'c'*40},
                  files=files, plan=s.make_plan(files, g, 'b'*64, 'c'*40),
                  ancestors=[item('/data/gtron/releases/old%d/reader-required.json' % n, b'permanent') for n in range(9)],
                  parents={p: {} for p in (s.EXEC, s.DROPIN, s.MARKER, str(s.RELEASE / 'reader-required.json'))},
                  configuration=config, process=dict(pid=s.OLD_PID, ticks=s.OLD_TICKS, exe=s.OLD_BINARY,
                     argv=argv, environ=base64.b64encode(b'GOMEMLIMIT=10GiB\0KEEP=1\0').decode()),
                  control_group='/system.slice/gtron.service', head=100, process_start=1000)
    return g, record


class ActivationTests(unittest.TestCase):
    def test_plan_preserves_flags_environment_and_other_prestart(self):
        g, record = fixture(); plan = record['plan']
        original = s.content(record['files'][0]); changed = s.content(plan[0])
        self.assertEqual(changed.replace((str(s.BINARY) + ' ' + ' '.join(s.FLAGS)).encode(), s.OLD_BINARY.encode()), original)
        self.assertEqual(s.expected_config(record, True)['ExecStartPre'][0], record['configuration']['ExecStartPre'][0])
        self.assertEqual(json.loads(s.content(plan[2])), g.reader_marker(s.BINARY, 'b'*64, 'c'*40))
        self.assertEqual([x['mode'] for x in plan], [0o644]*3)
        for invalid in (original + s.OLD_BINARY.encode(), original + b' --history.shared-read-workers=2', original + b' --history.backlog-high=100'):
            files = copy.deepcopy(record['files']); files[0] = item(s.EXEC, invalid)
            with self.assertRaises(RuntimeError): s.make_plan(files, g, 'b'*64, 'c'*40)

    def test_saved_data_and_helper_tampering_rejected(self):
        bad = item('/x', b'original'); bad['data_b64'] = base64.b64encode(b'new').decode()
        with self.assertRaises(RuntimeError): s.content(bad)
        with self.assertRaises(RuntimeError): s.module(b'raise AssertionError()', s.GUARD_SHA, 'invalid')
        self.assertNotIn('ControlGroup', s.CONFIG_KEYS)

    def test_environment_only_systemd_invocation_fields_may_change(self):
        encode = lambda value: base64.b64encode(value).decode()
        self.assertEqual(s.environment(encode(b'GOMEMLIMIT=10GiB\0INVOCATION_ID=a\0JOURNAL_STREAM=1:2\0')),
                         s.environment(encode(b'INVOCATION_ID=b\0GOMEMLIMIT=10GiB\0JOURNAL_STREAM=1:3\0')))
        self.assertNotEqual(s.environment(encode(b'GOMEMLIMIT=10GiB\0')), s.environment(encode(b'GOMEMLIMIT=11GiB\0')))

    def transaction_case(self, failure=None, manual=False):
        g, record = fixture(); files = {f['path']: copy.deepcopy(f) for f in record['files'] + record['ancestors']}
        before = copy.deepcopy(files); events = []; pending = [failure]
        def maybe(name):
            events.append(name)
            if pending and pending[0] == name:
                pending.pop(); raise InterruptedError('injected ' + name)
        def write(value, parent_pin=None):
            maybe('write:' + value['path']); files[value['path']] = copy.deepcopy(value)
        def validate(unused): maybe('validate')
        def stop(unused): maybe('stop')
        def run(argv, timeout=60): maybe(argv[1]); return b''
        def save(name, value): maybe('save:' + name)
        def health(unused, new): maybe('health:new' if new else 'health:old'); return {'healthy': True}
        def config(unused, props):
            return s.expected_config(record, files[s.EXEC] == record['plan'][0])
        with mock.patch.object(s, 'validate_candidate', side_effect=validate), mock.patch.object(s, 'stop', side_effect=stop), \
             mock.patch.object(s, 'write_atomic', side_effect=write), mock.patch.object(s, 'saved', side_effect=lambda p: copy.deepcopy(files[str(p)])), \
             mock.patch.object(s, 'save', side_effect=save), mock.patch.object(s, 'healthy', side_effect=health), \
             mock.patch.object(s, 'run', side_effect=run), mock.patch.object(s, 'guard', return_value=g), \
             mock.patch.object(s, 'show', return_value={}), mock.patch.object(s, 'configuration', side_effect=config), \
             mock.patch.object(s, 'process', return_value=record['process']), \
             mock.patch.object(s, 'open_parent', side_effect=lambda p, expected=None: (os.open(os.devnull, os.O_RDONLY), {})), \
             mock.patch.object(s.os.path, 'lexists', side_effect=lambda p: str(p) in files), \
             mock.patch.object(s.signal, 'pthread_sigmask', return_value=set()):
            if manual:
                # Interrupted installation: one old, two new files. No candidate
                # attestation is available; manual restore must not load it.
                for value in record['plan'][1:]: files[value['path']] = copy.deepcopy(value)
                s.transact(record, False)
            elif failure:
                with self.assertRaises(InterruptedError): s.transact(record, True)
            else:
                s.transact(record, True)
        return record, before, files, events

    def test_success_keeps_nine_ancestors_and_adds_permanent_marker(self):
        record, before, files, events = self.transaction_case()
        self.assertEqual(files[s.EXEC], record['plan'][0])
        self.assertEqual(files[s.DROPIN], record['plan'][1])
        self.assertEqual(files[s.MARKER], record['plan'][2])
        self.assertIn(str(s.RELEASE / 'reader-required.json'), files)
        for saved in record['ancestors']: self.assertEqual(files[saved['path']], saved)
        self.assertEqual(files[s.SPACE_CONFIG], before[s.SPACE_CONFIG])
        self.assertNotIn('health:old', events)

    def test_each_partial_failure_and_signal_restores_original_bytes_modes(self):
        failures = ['stop'] + ['write:' + path for path in (s.EXEC, s.DROPIN, s.MARKER,
                    str(s.RELEASE / 'reader-required.json'))] + ['daemon-reload', 'start', 'health:new', 'save:activated.json']
        for failure in failures:
            with self.subTest(failure=failure):
                record, before, files, events = self.transaction_case(failure)
                for path, value in before.items(): self.assertEqual(files[path], value)
                self.assertIn('health:old', events)
                self.assertIn('save:rollback.json', events)
                self.assertEqual(events.count('validate'), 1)

    def test_manual_partial_recovery_never_requires_candidate_evidence(self):
        record, before, files, events = self.transaction_case(manual=True)
        self.assertNotIn('validate', events)
        for path, value in before.items(): self.assertEqual(files[path], value)
        self.assertIn('health:old', events)

    def test_failure_journal_write_cannot_skip_restore(self):
        g, record = fixture()
        with mock.patch.object(s, 'validate_candidate'), mock.patch.object(s, 'check_files'), \
             mock.patch.object(s, 'process', return_value=record['process']), mock.patch.object(s, 'show'), \
             mock.patch.object(s, 'guard'), mock.patch.object(s, 'configuration', return_value=record['configuration']), \
             mock.patch.object(s.os.path, 'lexists', return_value=False), \
             mock.patch.object(s, 'stop', side_effect=RuntimeError('stop failed')), \
             mock.patch.object(s, 'save', side_effect=lambda name, value: (_ for _ in ()).throw(OSError('disk full')) if name == 'failure.json' else None), \
             mock.patch.object(s, 'restore', return_value={'restored': True}) as restore, \
             mock.patch.object(s.signal, 'pthread_sigmask', return_value=set()):
            with self.assertRaises(OSError): s.transact(record, True)
            restore.assert_called_once_with(record)

    def test_unknown_drift_is_not_overwritten_and_hold_is_not_removed(self):
        g, record = fixture(); files = {f['path']: f for f in record['files'] + record['ancestors']}
        files[s.DROPIN] = item(s.DROPIN, b'operator changed config')
        with mock.patch.object(s, 'saved', side_effect=lambda p: files[p]), mock.patch.object(s, 'stop') as stop, \
             mock.patch.object(s, 'open_parent', side_effect=lambda p, expected=None: (os.open(os.devnull, os.O_RDONLY), {})):
            with self.assertRaisesRegex(RuntimeError, 'drift'): s.restore(record)
            stop.assert_not_called()
        with mock.patch.object(s, 'validate_candidate'), mock.patch.object(s.os.path, 'lexists', return_value=True), \
             mock.patch.object(s, 'stop') as stop:
            with self.assertRaisesRegex(RuntimeError, 'hold'): s.transact(record, True)
            stop.assert_not_called()

    def health_case(self, observations, wrong_config=False, new=True, final_process_changed=False):
        g, record = fixture()
        proc = copy.deepcopy(record['process'])
        if new:
            proc.update(pid=999, ticks=100, exe=str(s.BINARY), argv=[str(s.BINARY)] + s.FLAGS + proc['argv'][1:])
        props = {'ActiveState': 'active', 'MainPID': str(proc['pid']), 'ControlGroup': record['control_group']}
        config = s.expected_config(record, new)
        if wrong_config: config['MemoryLimit'] = 'unexpected'
        calls = []
        def current_process(pid):
            calls.append(pid)
            return dict(proc, ticks=proc['ticks'] + 1) if final_process_changed and len(calls) > 2 else proc
        with mock.patch.object(s, 'guard', return_value=g), mock.patch.object(s, 'show', return_value=props), \
             mock.patch.object(s, 'process', side_effect=current_process), mock.patch.object(s, 'configuration', return_value=config), \
             mock.patch.object(s, 'regular', return_value=b'pin'), mock.patch.object(s, 'sha', return_value='b'*64 if new else s.OLD_SHA), \
             mock.patch.object(s, 'observe', side_effect=observations) as observe, mock.patch.object(s, 'check_files'), \
             mock.patch.object(s.time, 'sleep'):
            result = s.healthy(record, new, timeout=30)
        return result, observe.call_count

    def metrics(self):
        prefix = 'state/snapshot/cold/history/shared_read/'
        result = {'process/start/unix_nano': {'value': 2000}}
        for key, field in [('attempts', 'count'), ('published', 'count')] + [('last/' + name, 'value') for name in ('active', 'workers', 'chunk_cache', 'codec_workers', 'fallback_reason')]:
            result[prefix + key] = {field: 0}
        return result

    def test_startup_connection_and_uninitialized_metrics_retry_but_mismatch_does_not(self):
        metrics = self.metrics()
        result, count = self.health_case([urllib.error.URLError('not listening'), (100, {}), (100, metrics), (101, metrics)])
        self.assertEqual(count, 4); self.assertEqual(result['head'], 101)
        with self.assertRaisesRegex(RuntimeError, 'effective unit'):
            self.health_case([], wrong_config=True)
        invalid = self.metrics(); invalid['state/snapshot/cold/history/shared_read/last/workers']['value'] = -1
        with self.assertRaisesRegex(RuntimeError, 'metric contract'): self.health_case([(100, invalid)])

    def test_final_health_catches_restart_during_http_and_startup_retry_is_bounded(self):
        metrics = self.metrics()
        with self.assertRaisesRegex(RuntimeError, 'final health'):
            self.health_case([(100, metrics), (101, metrics)], final_process_changed=True)
        with mock.patch.object(s.time, 'monotonic', side_effect=[0, 0, 31]):
            with self.assertRaisesRegex(RuntimeError, 'expired'):
                self.health_case([urllib.error.URLError('not listening')])

    def test_unpinned_candidate_rejected_before_service_access_and_stop_checks_argv(self):
        args = argparse.Namespace(source_revision='short', script_revision='a'*40, binary_sha='b'*64,
                                  prepared_sha='c'*64, abba_sha='d'*64)
        with mock.patch.object(s, 'run') as run:
            with self.assertRaisesRegex(RuntimeError, 'exact candidate'): s.validate_candidate(args)
            run.assert_not_called()
        g, record = fixture()
        proc = dict(record['process'], argv=[s.OLD_BINARY, '--unexpected'])
        with mock.patch.object(s, 'show', return_value={'MainPID': str(s.OLD_PID), 'ControlGroup': record['control_group']}), \
             mock.patch.object(s, 'process', return_value=proc), mock.patch.object(s, 'run') as run:
            with self.assertRaisesRegex(RuntimeError, 'altered process'): s.stop(record)
            run.assert_not_called()

    def test_real_local_command_signal_reaps_child_without_node_operations(self):
        with tempfile.TemporaryDirectory() as tmp:
            pidfile = Path(tmp) / 'child.pid'
            code = 'import os,time; open({!r}, "w").write(str(os.getpid())); time.sleep(30)'.format(str(pidfile))
            previous = signal.getsignal(signal.SIGALRM)
            def interrupt(unused_number, unused_frame):
                raise s.OperatorInterrupted('local test alarm')
            signal.signal(signal.SIGALRM, interrupt)
            try:
                signal.setitimer(signal.ITIMER_REAL, 0.5)
                with mock.patch.object(s, 'REPO', Path(tmp)):
                    with self.assertRaises(s.OperatorInterrupted): s.run([sys.executable, '-c', code])
            finally:
                signal.setitimer(signal.ITIMER_REAL, 0)
                signal.signal(signal.SIGALRM, previous)
            pid = int(pidfile.read_text())
            with self.assertRaises(ProcessLookupError): os.kill(pid, 0)

    def test_service_owned_marker_parent_is_pinned_and_atomic_write_uses_dirfd(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp).resolve(); directory.chmod(0o775)
            marker = directory / 'reader-marker.json'; marker.write_bytes(b'old')
            actual_info, original = directory.stat(), s.parent_info
            def service_owner(info):
                result = original(info)
                if (info.st_dev, info.st_ino) == (actual_info.st_dev, actual_info.st_ino):
                    result.update(uid=1003, gid=1003, mode=0o775)
                return result
            with mock.patch.object(s, 'MARKER', str(marker)), mock.patch.object(s, 'parent_info', side_effect=service_owner), \
                 mock.patch.object(s.os, 'fchown') as chown, mock.patch.object(s.os, 'replace', wraps=os.replace) as replace:
                fd, pin = s.open_parent(marker); os.close(fd)
                self.assertEqual((pin['uid'], pin['gid'], pin['mode']), (1003, 1003, 0o775))
                with self.assertRaisesRegex(RuntimeError, 'requires admitted'): s.write_atomic(item(marker, b'new'))
                s.write_atomic(item(marker, b'new'), parent_pin=pin)
                self.assertEqual(marker.read_bytes(), b'new')
                self.assertEqual(stat.S_IMODE(marker.stat().st_mode), 0o644)
                self.assertEqual(chown.call_args.args[1:], (0, 0))
                self.assertEqual(replace.call_args.kwargs['src_dir_fd'], replace.call_args.kwargs['dst_dir_fd'])
                self.assertEqual(replace.call_args.args[1], marker.name)
                with self.assertRaisesRegex(RuntimeError, 'identity changed'):
                    s.write_atomic(item(marker, b'bad'), parent_pin=dict(pin, inode=pin['inode'] + 1))
                with self.assertRaisesRegex(RuntimeError, 'unsafe write parent'): s.open_parent(directory / 'other-file')
            self.assertEqual(marker.read_bytes(), b'new')
            self.assertEqual(sorted(p.name for p in directory.iterdir()), [marker.name])

    def test_recovery_directory_name_is_fsynced_before_admission_can_be_saved(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp).resolve(); transaction = parent / 'activation'
            calls, real_fsync = [], os.fsync
            def sync(fd):
                self.assertTrue(transaction.is_dir())
                calls.append(os.fstat(fd).st_ino)
                return real_fsync(fd)
            with mock.patch.object(s, 'TRANSACTION', transaction), mock.patch.object(s.os, 'fsync', side_effect=sync):
                s.create_transaction()
            self.assertEqual(calls, [parent.stat().st_ino])
            self.assertEqual(stat.S_IMODE(transaction.stat().st_mode), 0o700)

    def test_failure_evidence_contains_reason_trace_and_recovery_error(self):
        try:
            raise RuntimeError('effective unit changed')
        except RuntimeError as error:
            details = s.failure_details(error)
        self.assertEqual(details['error'], 'effective unit changed')
        self.assertIn('RuntimeError: effective unit changed', details['traceback'])
        g, record = fixture(); saved = []
        with mock.patch.object(s, 'validate_candidate'), mock.patch.object(s, 'check_files'), \
             mock.patch.object(s, 'process', return_value=record['process']), mock.patch.object(s, 'show'), \
             mock.patch.object(s, 'guard'), mock.patch.object(s, 'configuration', return_value=record['configuration']), \
             mock.patch.object(s.os.path, 'lexists', return_value=False), mock.patch.object(s, 'stop', side_effect=RuntimeError('stop failed')), \
             mock.patch.object(s, 'save', side_effect=lambda name, data: saved.append((name, data))), \
             mock.patch.object(s, 'restore', side_effect=RuntimeError('old space guard refused')), \
             mock.patch.object(s.signal, 'pthread_sigmask', return_value=set()):
            with self.assertRaisesRegex(RuntimeError, 'old space guard'): s.transact(record, True)
        records = dict(saved)
        self.assertEqual(records['failure.json']['error'], 'stop failed')
        self.assertEqual(records['rollback-failed.json']['error'], 'old space guard refused')

    def test_no_node_kill_or_build_or_hold_removal_and_python36(self):
        source = PATH.read_text()
        for forbidden in ('killpg(', 'os.kill(', "'build'", "'fetch'", 'SPACE_HOLD).unlink'):
            self.assertNotIn(forbidden, source)
        for path in (PATH, Path(__file__)):
            ast.parse(path.read_text(), feature_version=(3, 6))


if __name__ == '__main__':
    unittest.main()

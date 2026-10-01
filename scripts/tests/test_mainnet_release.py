"""Focused release-contract and rollback tests without touching systemd."""

import importlib.util
import io
import json
from pathlib import Path
import subprocess
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location(
    'mainnet_release', Path(__file__).parents[1] / 'mainnet_release.py')
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)

SOURCE = 'a' * 40
OLD_SOURCE = 'b' * 40
SHA = '1' * 64
OLD_SHA = '2' * 64
OLD_BINARY = '/data/gtron/releases/old/gtron'
NEW_BINARY = '/data/gtron/releases/auto-' + SOURCE[:12] + '-' + SHA[:16] + '/gtron'


def old_state():
    guard = ['/usr/bin/python3', release.SHARED_GUARD, '--binary', OLD_BINARY,
             '--sha256', OLD_SHA, '--source', OLD_SOURCE, '--marker', str(release.MARKER)]
    return {'binary': OLD_BINARY, 'sha': OLD_SHA, 'source': OLD_SOURCE,
            'argv': [OLD_BINARY, '--history.shared-chunk-cache=true', '--p2p.port=18890'],
            'active': True, 'memory': (('ExecStart=' + OLD_BINARY + ' --p2p.port=18890\n').encode(), 0o644),
            'shared': (('ExecStartPre=' + ' '.join(guard) + '\n').encode(), 0o644),
            'marker': (release.json_bytes(release.shared_marker(OLD_BINARY, OLD_SHA, OLD_SOURCE)), 0o644),
            'guards': [(release.SHARED_GUARD, guard, {})], 'reference': None,
            'space_pre': release.SPACE_GUARD}


class MainnetReleaseTests(unittest.TestCase):
    def test_staging_wait_is_bounded_and_deploy_unit_covers_rollback(self):
        self.assertEqual(release.staging_health_timeout(), 43200)
        self.assertEqual(release.staging_health_timeout('86400'), 86400)
        for invalid in ('599', '86401', '-1', '1.5', ''):
            with self.assertRaises(RuntimeError):
                release.staging_health_timeout(invalid)
        self.assertEqual(release.systemd_duration_seconds('26h'), 26 * 3600)
        self.assertEqual(release.systemd_duration_seconds('1d 2h'), 26 * 3600)
        with mock.patch.object(release, 'command', return_value='TimeoutStartUSec=26h\n'):
            self.assertEqual(release.require_staging_deploy_timeout(43200), 26 * 3600)
        with mock.patch.object(release, 'command', return_value='TimeoutStartUSec=14h\n'):
            with self.assertRaisesRegex(RuntimeError, 'two staging health waits'):
                release.require_staging_deploy_timeout(43200)

    def test_health_wait_rejects_pid_change_before_timeout(self):
        values = iter(({'ActiveState': 'active', 'MainPID': '41'},
                       {'ActiveState': 'active', 'MainPID': '42'}))
        with mock.patch.object(release, 'show', side_effect=lambda *args: next(values)), \
                mock.patch.object(release, 'proc_identity', return_value=('/binary', ['/binary'])), \
                mock.patch.object(release, 'proc_exe_fingerprint', return_value=(1, 2, 3, 4)), \
                mock.patch.object(release, 'proc_sha', return_value='a' * 64) as sha, \
                mock.patch.object(release, 'wallet_head', side_effect=RuntimeError('API warming')), \
                mock.patch.object(release.time, 'sleep'):
            with self.assertRaisesRegex(RuntimeError, 'PID changed'):
                release.wait_healthy('/binary', 'a' * 64, ['/binary'], timeout=43200)
        sha.assert_called_once()

    def test_health_wait_hashes_once_during_warmup_and_again_on_acceptance(self):
        heads = iter((RuntimeError('API warming'), 123))
        def wallet():
            value = next(heads)
            if isinstance(value, Exception):
                raise value
            return value
        with mock.patch.object(release, 'show', return_value={'ActiveState': 'active', 'MainPID': '41'}), \
                mock.patch.object(release, 'proc_identity', return_value=('/binary', ['/binary'])), \
                mock.patch.object(release, 'proc_exe_fingerprint', return_value=(1, 2, 3, 4)), \
                mock.patch.object(release, 'proc_sha', return_value='a' * 64) as sha, \
                mock.patch.object(release, 'wallet_head', side_effect=wallet), \
                mock.patch.object(release.time, 'sleep'):
            self.assertEqual(release.wait_healthy('/binary', 'a' * 64, ['/binary'], timeout=60), 41)
        self.assertEqual(sha.call_count, 2)

    def test_prepared_or_migrating_source_blocks_ordinary_release(self):
        with mock.patch.object(release.os.path, 'lexists', side_effect=lambda path: path == release.STAGING_PREPARED):
            with self.assertRaisesRegex(RuntimeError, 'pinned to its legacy reader'):
                release.require_ordinary_deploy_allowed()
        with mock.patch.object(release.os.path, 'lexists', side_effect=lambda path: path == release.STAGING_LATCH):
            with self.assertRaisesRegex(RuntimeError, 'migration latch'):
                release.require_ordinary_deploy_allowed()

    def test_pending_activation_start_failure_does_not_restore_old_binary(self):
        candidate = str(release.RELEASES / 'staging-test' / 'gtron')
        latch = {'version': 1, 'state': 'VERIFIED_PENDING_ACTIVATION',
                 'candidate_sha256': SHA, 'source_commit': SOURCE,
                 'service_was_active': True, 'prune_mode': 'snap',
                 'history_window': 65536}
        prepared = {'version': 1, 'legacy_binary_sha256': OLD_SHA}
        old = old_state()
        old['active'] = False
        old['space_pre'] = release.SPACE_GUARD + ' ' + release.STAGING_GUARD
        buildinfo = ('\tvcs.revision=' + SOURCE + '\n\t-tags=sapling\n'
                     '\tCGO_ENABLED=1\n\tGOOS=linux\n\tGOARCH=amd64\n')
        actions = []
        files = {release.STAGING_LATCH: (release.json_bytes(latch), 0o644),
                 release.STAGING_PREPARED: (release.json_bytes(prepared), 0o644),
                 release.MEMORY: old['memory'], release.SHARED: old['shared'],
                 release.MARKER: old['marker']}

        def command(argv, timeout=60):
            actions.append(argv)
            if argv[:4] == ['/data/go/bin/go', 'version', '-m', candidate]:
                return buildinfo
            if argv[:2] == ['/bin/systemctl', 'start']:
                raise RuntimeError('new reader did not start')
            return ''

        effective = {'ExecStart': '{ path=' + candidate + ' ; argv[]=' + candidate +
                     ' --history.shared-chunk-cache=true --p2p.port=18890 ; ignore_errors=no }',
                     'ExecStartPre': release.STAGING_GUARD}
        def write(path, data, mode):
            actions.append(['write', str(path)])
            files[path] = (data, mode)

        with mock.patch.object(release, 'root_bytes', side_effect=lambda path, limit: files[path]), \
                mock.patch.object(release.os.path, 'lexists', side_effect=lambda path: path in files), \
                mock.patch.object(release, 'root_sha', return_value=SHA), \
                mock.patch.object(release, 'require_staging_capability'), \
                mock.patch.object(release, 'inspect', return_value=old), \
                mock.patch.object(release, 'atomic_root', side_effect=write), \
                mock.patch.object(release, 'show', return_value=effective), \
                mock.patch.object(release, 'command', side_effect=command), \
                mock.patch.object(release, 'restore') as restore:
            with self.assertRaisesRegex(RuntimeError, 'new reader did not start'):
                release.activate_staging(SOURCE, candidate, SHA)
        restore.assert_not_called()
        self.assertIn(['/bin/systemctl', 'stop', release.SERVICE], actions)

    def test_pending_activation_retries_after_partial_atomic_replacement(self):
        candidate = str(release.RELEASES / 'staging-test' / 'gtron')
        latch = {'version': 1, 'state': 'VERIFIED_PENDING_ACTIVATION',
                 'candidate_sha256': SHA, 'source_commit': SOURCE,
                 'service_was_active': True, 'prune_mode': 'snap',
                 'history_window': 65536}
        prepared = {'version': 1, 'legacy_binary_sha256': OLD_SHA}
        old = old_state()
        old['active'] = False
        old['space_pre'] = release.SPACE_GUARD + ' ' + release.STAGING_GUARD
        files = {release.STAGING_LATCH: (release.json_bytes(latch), 0o644),
                 release.STAGING_PREPARED: (release.json_bytes(prepared), 0o644),
                 release.MEMORY: old['memory'], release.SHARED: old['shared'],
                 release.MARKER: old['marker']}
        buildinfo = ('\tvcs.revision=' + SOURCE + '\n\t-tags=sapling\n'
                     '\tCGO_ENABLED=1\n\tGOOS=linux\n\tGOARCH=amd64\n')
        writes = []
        fail_after = [6]

        def write(path, data, mode):
            files[path] = (data, mode)
            writes.append(path)
            if fail_after[0] == len(writes):
                raise RuntimeError('simulated crash after durable replacement')

        def command(argv, timeout=60):
            if argv[:4] == ['/data/go/bin/go', 'version', '-m', candidate]:
                return buildinfo
            return ''

        effective = {'ExecStart': '{ path=' + candidate + ' ; argv[]=' + candidate +
                     ' --history.shared-chunk-cache=true --p2p.port=18890 ; ignore_errors=no }',
                     'ExecStartPre': release.STAGING_GUARD}
        with mock.patch.object(release, 'root_bytes', side_effect=lambda path, limit: files[path]), \
                mock.patch.object(release.os.path, 'lexists', side_effect=lambda path: path in files), \
                mock.patch.object(release, 'root_sha', return_value=SHA), \
                mock.patch.object(release, 'require_staging_capability'), \
                mock.patch.object(release, 'inspect', return_value=old) as inspect, \
                mock.patch.object(release, 'atomic_root', side_effect=write), \
                mock.patch.object(release, 'show', return_value=effective), \
                mock.patch.object(release, 'command', side_effect=command), \
                mock.patch.object(release, 'wait_healthy', return_value=42), \
                mock.patch.object(release, 'restore') as restore:
            with self.assertRaisesRegex(RuntimeError, 'simulated crash'):
                release.activate_staging(SOURCE, candidate, SHA)
            fail_after[0] = -1
            result = release.activate_staging(SOURCE, candidate, SHA)
        self.assertEqual(result['pid'], 42)
        self.assertEqual(inspect.call_count, 1)
        restore.assert_not_called()
        self.assertEqual(files[release.STAGING_REQUIRED][0],
                         release.json_bytes(release.staging_marker(
                             SHA, SOURCE, False, 'snap', 65536)))

    def test_wallet_probe_reads_small_node_info_and_rejects_missing_head(self):
        class Response(io.BytesIO):
            status = 200

        with mock.patch.object(release.urllib.request, 'urlopen',
                               return_value=Response(b'{"currentBlock":16566121}')) as request:
            self.assertEqual(release.wallet_head(), 16566121)
            self.assertEqual(request.call_args[0][0], release.HEALTH)
        for data in (b'{"currentBlock":0}', b'{"currentBlock":true}',
                     b'{"currentBlock":16566121}' + b' ' * (1 << 20)):
            with mock.patch.object(release.urllib.request, 'urlopen',
                                   return_value=Response(data)):
                with self.assertRaises(RuntimeError):
                    release.wallet_head()

    def test_old_systemd_exec_serialization_and_repeated_pre_lines(self):
        raw = ('ExecStart={ path=' + OLD_BINARY + ' ; argv[]=' + OLD_BINARY +
               ' --p2p.port=18890 ; ignore_errors=no ; start_time=[Mon 2026-09-21 02:42:11 UTC] ; '
               'stop_time=[n/a] ; pid=17902 ; code=(null) ; status=0/0 }')
        self.assertEqual(release.systemd_exec(raw[len('ExecStart='):]),
                         (OLD_BINARY, [OLD_BINARY, '--p2p.port=18890']))
        output = ('ExecStartPre={ path=/usr/bin/python3 ; argv[]=/usr/bin/python3 ' +
                  release.SPACE_GUARD + ' check ; ignore_errors=no }\n' +
                  'ExecStartPre={ path=/usr/bin/python3 ; argv[]=/usr/bin/python3 ' +
                  release.SHARED_GUARD + ' --binary ' + OLD_BINARY + ' ; ignore_errors=no }\n')
        with mock.patch.object(release, 'command', return_value=output):
            props = release.show('ExecStartPre')
        self.assertEqual(props['ExecStartPre'].count('ExecStartPre='), 0)
        self.assertEqual(props['ExecStartPre'].count(release.SPACE_GUARD), 1)
        self.assertEqual(props['ExecStartPre'].count(release.SHARED_GUARD), 1)

    def test_plan_keeps_other_service_flags_and_omits_missing_reference_guard(self):
        old = old_state()
        changed = release.plan(old, NEW_BINARY, SHA, SOURCE)
        self.assertEqual(changed['memory'],
                         ('ExecStart=' + NEW_BINARY + ' --p2p.port=18890\n').encode())
        self.assertEqual(changed['shared'].count(NEW_BINARY.encode()), 1)
        self.assertEqual(changed['shared'].count(SHA.encode()), 1)
        self.assertEqual(changed['shared'].count(SOURCE.encode()), 1)
        self.assertIsNone(changed['reference'])
        self.assertEqual(release.shared_marker(NEW_BINARY, SHA, SOURCE),
                         json.loads(changed['marker']))

    def test_verify_exit_classification_preserves_guard_failure(self):
        old = old_state()
        with mock.patch.object(release, 'inspect', return_value=old):
            with self.assertRaises(release.SourceDifferent):
                release.verify(SOURCE)
            with mock.patch.object(release, 'command', return_value=''):
                with mock.patch.object(release, 'wallet_head', side_effect=RuntimeError('API down')):
                    with self.assertRaises(release.SameSourceUnhealthy):
                        release.verify(OLD_SOURCE)
            with mock.patch.object(release, 'command', side_effect=RuntimeError('guard denied')):
                with self.assertRaisesRegex(RuntimeError, 'guard denied'):
                    release.verify(OLD_SOURCE)

    def test_published_guard_can_be_inspected_and_planned_again(self):
        first = release.plan(old_state(), NEW_BINARY, SHA, SOURCE)
        files = {release.MEMORY: (first['memory'], 0o644),
                 release.SHARED: (first['shared'], 0o644),
                 release.MARKER: (first['marker'], 0o644)}
        serialized = ('{ path=' + NEW_BINARY + ' ; argv[]=' + NEW_BINARY +
                      ' --history.shared-chunk-cache=true --p2p.port=18890 ; ignore_errors=no ; '
                      'start_time=[n/a] ; stop_time=[n/a] ; pid=100 ; code=(null) ; status=0/0 }')
        props = {'ActiveState': 'active', 'MainPID': '100', 'ExecStart': serialized,
                 'ExecStartPre': (release.SPACE_GUARD + '\n' + first['shared'].decode())}
        with mock.patch.object(release, 'root_bytes', side_effect=lambda path, _: files[path]), \
                mock.patch.object(release, 'root_sha', return_value=SHA), \
                mock.patch.object(release, 'show', return_value=props), \
                mock.patch.object(release, 'proc_identity', return_value=(NEW_BINARY,
                    [NEW_BINARY, '--history.shared-chunk-cache=true', '--p2p.port=18890'])), \
                mock.patch.object(release, 'proc_sha', return_value=SHA), \
                mock.patch.object(release, 'command', return_value=''), \
                mock.patch.object(release, 'wallet_head', return_value=16500000):
            inspected = release.inspect()
            self.assertEqual(release.verify(SOURCE)['binary_sha256'], SHA)
        self.assertEqual(inspected['source'], SOURCE)
        next_source, next_sha = 'c' * 40, '3' * 64
        next_binary = '/data/gtron/releases/auto-' + next_source[:12] + '-' + next_sha[:16] + '/gtron'
        second = release.plan(inspected, next_binary, next_sha, next_source)
        self.assertEqual(second['shared'].count(next_source.encode()), 1)
        self.assertEqual(second['shared'].count(next_binary.encode()), 1)
        self.assertEqual(json.loads(second['marker'])['source_commit'], next_source)

    def test_stop_failure_and_new_start_failure_both_restore(self):
        old = old_state()
        buildinfo = ('\tvcs.revision=' + SOURCE + '\n\t-tags=sapling\n\tCGO_ENABLED=1\n'
                     '\tGOOS=linux\n\tGOARCH=amd64\n')
        for failed_action in ('stop', 'start'):
            with self.subTest(failed_action=failed_action):
                attempts = []

                def command(argv, timeout=60):
                    attempts.append(argv)
                    if argv[0] == '/usr/bin/git':
                        return SOURCE + '\n'
                    if argv[0] == '/data/go/bin/go':
                        return buildinfo
                    if (argv[:2] == ['/bin/systemctl', failed_action] and
                            sum(row[:2] == argv[:2] for row in attempts) == 1):
                        raise subprocess.TimeoutExpired(argv, timeout)
                    return ''

                with mock.patch.object(release, 'command', side_effect=command), \
                        mock.patch.object(release, 'inspect', return_value=old), \
                        mock.patch.object(release, 'candidate_bytes', return_value=b'candidate'), \
                        mock.patch.object(release, 'install_release', return_value=NEW_BINARY), \
                        mock.patch.object(release, 'atomic_root'), \
                        mock.patch.object(release, 'restore') as restore, \
                        mock.patch.object(release, 'show', return_value={
                            'ExecStart': '{ path=' + NEW_BINARY + ' ; argv[]=' + NEW_BINARY +
                                         ' --history.shared-chunk-cache=true --p2p.port=18890 ; ignore_errors=no }',
                            'ExecStartPre': release.SPACE_GUARD + ' ' + NEW_BINARY + ' ' + SHA + ' ' + SOURCE}), \
                        mock.patch.object(release, 'guard_commands', return_value=[]), \
                        mock.patch.object(release, 'wait_healthy', return_value=100):
                    with self.assertRaises(subprocess.TimeoutExpired):
                        release.deploy(SOURCE, '/candidate', SHA)
                restore.assert_called_once_with(old, health_timeout=None)
                self.assertIn(['/usr/bin/git', '--git-dir=' + str(release.REPO / '.git'),
                               'rev-parse', 'HEAD'], attempts)

    def test_restore_reinstalls_original_bytes_before_start(self):
        old = old_state()
        events = []
        with mock.patch.object(release, 'atomic_root', side_effect=lambda *args: events.append(('write', args))), \
                mock.patch.object(release, 'command', side_effect=lambda argv, timeout=60: events.append(('command', argv))), \
                mock.patch.object(release, 'wait_healthy', side_effect=lambda *args, **kwargs: events.append(('healthy', args, kwargs))):
            release.restore(old)
        self.assertEqual([item[0] for item in events],
                         ['write', 'write', 'write', 'command', 'command', 'healthy'])
        self.assertEqual(events[0][1], (release.MEMORY,) + old['memory'])
        self.assertEqual(events[1][1], (release.SHARED,) + old['shared'])
        self.assertEqual(events[2][1], (release.MARKER,) + old['marker'])
        self.assertEqual(events[-1][1], (OLD_BINARY, OLD_SHA, old['argv']))


if __name__ == '__main__':
    unittest.main()

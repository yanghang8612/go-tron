"""Offline staging ownership handoff guards; no systemd or production paths."""

import fcntl
import importlib.util
import json
import os
from pathlib import Path
import pwd
import grp
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location(
    'mainnet_release', Path(__file__).parents[1] / 'mainnet_release.py')
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)


class ServiceOwnerQueryTests(unittest.TestCase):
    def query(self, props, active=False, returncode=0, uid=1001, gid=1002, user_gid=1002):
        def run(argv, **kwargs):
            # systemd 219 rejects unknown filtered properties, but its full
            # query succeeds and omits properties it does not implement.
            if '--property=DynamicUser' in argv:
                return subprocess.CompletedProcess(argv, 1, b'', b'Unknown property DynamicUser')
            self.assertEqual(argv, ['/bin/systemctl', 'show', release.SERVICE, '--no-pager', '--all'])
            output = '\n'.join('%s=%s' % item for item in props.items()).encode()
            return subprocess.CompletedProcess(argv, returncode, output, b'query failed')
        with mock.patch.object(release.subprocess, 'run', side_effect=run), \
                mock.patch.object(release.pwd, 'getpwnam', return_value=mock.Mock(pw_uid=uid, pw_gid=user_gid)), \
                mock.patch.object(release.grp, 'getgrnam', return_value=mock.Mock(gr_gid=gid)):
            return release._staging_service_owner(active=active)

    def props(self):
        return {'ActiveState': 'inactive', 'MainPID': '0', 'User': 'java-tron', 'Group': 'java-tron'}

    def test_systemd_219_full_query_accepts_absent_optional_property(self):
        self.assertEqual(self.query(self.props()), (1001, 1002))

    def test_modern_static_owner_and_active_branch(self):
        props = self.props()
        props.update(DynamicUser='no', ActiveState='active', MainPID='99')
        self.assertEqual(self.query(props, active=True), (1001, 1002))

    def test_dynamic_user_must_be_explicitly_false_if_present(self):
        for value in ('yes', '', 'unexpected'):
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                props = self.props()
                props['DynamicUser'] = value
                self.query(props)

    def test_each_required_property_missing_fails(self):
        for name in self.props():
            with self.subTest(name=name), self.assertRaises(RuntimeError):
                props = self.props()
                del props[name]
                self.query(props)

    def test_wrong_owner_state_and_pid_fail(self):
        for name, value in (('User', 'root'), ('Group', 'root'), ('ActiveState', 'active'), ('MainPID', '99')):
            with self.subTest(name=name), self.assertRaises(RuntimeError):
                props = self.props()
                props[name] = value
                self.query(props)
        for pid in ('0', '', 'invalid'):
            with self.subTest(pid=pid), self.assertRaises(RuntimeError):
                props = self.props()
                props.update(ActiveState='active', MainPID=pid)
                self.query(props, active=True)

    def test_real_query_failure_does_not_accept_stdout(self):
        with self.assertRaisesRegex(RuntimeError, 'failed \\(1\\)'):
            self.query(self.props(), returncode=1)

    def test_numeric_owner_validation_is_preserved(self):
        for ids in ({'uid': 0}, {'gid': 0}, {'user_gid': 9999}):
            with self.subTest(ids=ids), self.assertRaises(RuntimeError):
                self.query(self.props(), **ids)


class StorageHandoffTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(os.path.realpath(self.temp.name))
        self.source, self.target = root / 'source', root / 'target'
        self.cold, self.ancient = root / 'cold', root / 'ancient'
        for path in (self.source, self.target, self.cold, self.ancient):
            path.mkdir(mode=0o700)
        (self.cold / 'etl').mkdir(mode=0o700)
        (self.ancient / 'FLOCK').write_bytes(b'')
        for path in (self.source, self.target):
            for name in ('CURRENT', 'LOCK', 'MANIFEST-000001', 'OPTIONS-000001',
                         '000001.sst', '000002.log', release.STAGING_STORAGE_LOCK):
                item = path / name
                item.write_bytes(name.encode())
                item.chmod(0o600)
        (self.cold / release.STAGING_STORAGE_LOCK).write_bytes(b'')
        (self.cold / release.STAGING_STORAGE_LOCK).chmod(0o600)
        self.startlock = root / 'start.lock'
        self.startlock.write_bytes(b'')
        self.startlock.chmod(0o600)
        self.journal = root / 'handoff.json'
        self.uid, self.gid = os.getuid(), os.getgid()
        self.latch = {'state': 'VERIFIED_PENDING_ACTIVATION',
                      'service_was_active': True, 'job_id': '1' * 32,
                      'candidate_sha256': '2' * 64, 'source_commit': '3' * 40,
                      'source': str(self.source), 'target': str(self.target),
                      'cold': str(self.cold)}
        self.prepared = {key: self.latch[key] for key in ('source', 'target', 'cold')}
        self.intent = '4' * 64

        for name, path in (('STAGING_SOURCE', self.source), ('STAGING_TARGET', self.target),
                           ('STAGING_COLD', self.cold), ('STAGING_ANCIENT', self.ancient),
                           ('STAGING_START_LOCK', self.startlock),
                           ('STAGING_HANDOFF', self.journal)):
            patcher = mock.patch.object(release, name, path)
            patcher.start()
            self.addCleanup(patcher.stop)
        for name, value in (
                ('_staging_service_owner', (self.uid, self.gid)),
                ('_staging_parent_identity', 'start'),
                ('_staging_require_parent_locks', None),
                ('_staging_require_stopped', None),
                ('_staging_probe_service_access', None)):
            patcher = mock.patch.object(release, name, return_value=value)
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = mock.patch.object(release, 'show', return_value={'ActiveState': 'inactive',
                                                                  'MainPID': '0'})
        patcher.start()
        self.addCleanup(patcher.stop)
        patcher = mock.patch.object(release.os, 'geteuid', return_value=0)
        patcher.start()
        self.addCleanup(patcher.stop)
        # Local fixtures cannot be root-owned; only this ownership predicate
        # is substituted. Lock inode and POSIX exclusivity remain real.
        patcher = mock.patch.object(
            release, '_staging_root_lock_owned',
            side_effect=lambda info: info.st_uid == self.uid and info.st_gid == self.gid and
                                      info.st_nlink == 1 and not info.st_mode & 0o077)
        patcher.start()
        self.addCleanup(patcher.stop)
        patcher = mock.patch.object(
            release, 'atomic_root', side_effect=lambda path, data, mode: Path(path).write_bytes(data))
        patcher.start()
        self.addCleanup(patcher.stop)
        patcher = mock.patch.object(
            release, 'root_bytes', side_effect=lambda path, limit: (Path(path).read_bytes(), 0o600))
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_exact_inventory_and_idempotent_retry_preserve_contents(self):
        before = {(str(directory), item.name): (item.read_bytes(), item.stat().st_mode)
                  for directory in (self.source, self.target)
                  for item in directory.iterdir() if item.is_file()}
        self.assertFalse(release.handoff_staging_storage(self.latch, self.prepared, self.intent))
        self.assertEqual(json.loads(self.journal.read_text())['phase'], 'DONE')
        self.assertFalse(release.handoff_staging_storage(self.latch, self.prepared, self.intent))
        after = {(str(directory), item.name): (item.read_bytes(), item.stat().st_mode)
                 for directory in (self.source, self.target)
                 for item in directory.iterdir() if item.is_file()}
        self.assertEqual(before, after)

    def test_partial_journal_retries_and_wrong_identity_rejects(self):
        original = release._staging_handoff_file
        calls = [0]
        def interrupted(*args):
            calls[0] += 1
            if calls[0] == 2:
                raise RuntimeError('simulated handoff interruption')
            return original(*args)
        with mock.patch.object(release, '_staging_handoff_file', side_effect=interrupted):
            with self.assertRaisesRegex(RuntimeError, 'simulated handoff interruption'):
                release.handoff_staging_storage(self.latch, self.prepared, self.intent)
        self.assertEqual(json.loads(self.journal.read_text())['phase'], 'IN_PROGRESS')
        wrong = dict(self.latch, job_id='f' * 32)
        with self.assertRaisesRegex(RuntimeError, 'journal identity differs'):
            release.handoff_staging_storage(wrong, self.prepared, self.intent)
        self.assertFalse(release.handoff_staging_storage(self.latch, self.prepared, self.intent))
        self.assertEqual(json.loads(self.journal.read_text())['phase'], 'DONE')

    def test_foreign_entries_reject_before_any_file_handoff(self):
        cases = ('foreign-name', 'symlink', 'hardlink', 'nested', 'special-mode')
        for case in cases:
            with self.subTest(case=case):
                path = self.source / '000099.sst'
                nested = self.source / 'nested'
                if case == 'foreign-name':
                    path = self.source / 'foreign.txt'
                    path.write_bytes(b'x')
                elif case == 'symlink':
                    path.symlink_to(self.target / 'CURRENT')
                elif case == 'hardlink':
                    os.link(str(self.target / 'CURRENT'), str(path))
                elif case == 'nested':
                    nested.mkdir()
                else:
                    path.write_bytes(b'x')
                    path.chmod(0o4600)
                try:
                    with mock.patch.object(release, '_staging_handoff_file') as transfer:
                        with self.assertRaises(RuntimeError):
                            release.handoff_staging_storage(self.latch, self.prepared, self.intent)
                        transfer.assert_not_called()
                finally:
                    if nested.exists():
                        nested.rmdir()
                    elif path.is_symlink() or path.exists():
                        path.unlink()

    def test_wrong_service_or_inactive_job_fails_before_journal(self):
        with mock.patch.object(release, '_staging_service_owner', side_effect=RuntimeError('wrong User')):
            with self.assertRaisesRegex(RuntimeError, 'wrong User'):
                release.handoff_staging_storage(self.latch, self.prepared, self.intent)
        inactive = dict(self.latch, service_was_active=False)
        with self.assertRaisesRegex(RuntimeError, 'active-service'):
            release.handoff_staging_storage(inactive, self.prepared, self.intent)
        self.assertFalse(self.journal.exists())

    def test_failed_service_probe_keeps_journal_in_progress(self):
        with mock.patch.object(release, '_staging_probe_service_access',
                               side_effect=RuntimeError('service cannot write')):
            with self.assertRaisesRegex(RuntimeError, 'service cannot write'):
                release.handoff_staging_storage(self.latch, self.prepared, self.intent)
        self.assertEqual(json.loads(self.journal.read_text())['phase'], 'IN_PROGRESS')

    def test_postflight_rejects_replaced_pebble_file(self):
        def replace_file(_paths):
            path = self.source / '000001.sst'
            path.unlink()
            path.write_bytes(b'different inode')
        with mock.patch.object(release, '_staging_probe_service_access', side_effect=replace_file):
            with self.assertRaisesRegex(RuntimeError, 'changed after handoff'):
                release.handoff_staging_storage(self.latch, self.prepared, self.intent)
        self.assertEqual(json.loads(self.journal.read_text())['phase'], 'IN_PROGRESS')

    def test_active_retry_needs_done_matching_journal_and_does_not_take_pebble_lock(self):
        self.assertFalse(release.handoff_staging_storage(self.latch, self.prepared, self.intent))
        with mock.patch.object(release, 'show', return_value={'ActiveState': 'active', 'MainPID': '99'}), \
                mock.patch.object(release, '_staging_inventory_pebble',
                                  side_effect=AssertionError('must not acquire active Pebble LOCK')):
            self.assertTrue(release.handoff_staging_storage(self.latch, self.prepared, self.intent))
            row = json.loads(self.journal.read_text())
            row['phase'] = 'IN_PROGRESS'
            self.journal.write_text(json.dumps(row))
            with self.assertRaisesRegex(RuntimeError, 'journal identity differs'):
                release.handoff_staging_storage(self.latch, self.prepared, self.intent)

    def test_posix_pebble_lock_excludes_other_writer(self):
        locker = subprocess.Popen(['python3', '-c',
            'import fcntl,os,sys,time\n'
            'fd=os.open(sys.argv[1],os.O_RDWR)\n'
            'fcntl.lockf(fd,fcntl.LOCK_EX)\n'
            'sys.stdout.write("ready\\n");sys.stdout.flush();time.sleep(10)\n',
            str(self.source / 'LOCK')], stdout=subprocess.PIPE)
        try:
            self.assertEqual(locker.stdout.readline(), b'ready\n')
            with release.ExitStack() as stack:
                with self.assertRaisesRegex(RuntimeError, 'LOCK is held'):
                    release._staging_inventory_pebble(stack, self.source, self.uid, self.gid)
        finally:
            locker.terminate()
            locker.wait(timeout=5)
            locker.stdout.close()

    def test_parent_flock_parser_requires_exact_pid_and_four_inodes(self):
        files = [os.stat(str(path)) for path in
                 (self.startlock, self.source / release.STAGING_STORAGE_LOCK,
                  self.target / release.STAGING_STORAGE_LOCK,
                  self.cold / release.STAGING_STORAGE_LOCK)]
        lines = ['%d: FLOCK ADVISORY WRITE 41 %x:%x:%d 0 EOF' %
                 (n, os.major(row.st_dev), os.minor(row.st_dev), row.st_ino)
                 for n, row in enumerate(files, 1)]
        self.assertTrue(release._staging_parent_lock_matches('\n'.join(lines), 41, files))
        self.assertFalse(release._staging_parent_lock_matches('\n'.join(lines), 42, files))
        self.assertFalse(release._staging_parent_lock_matches('\n'.join(lines[:-1]), 41, files))


@unittest.skipUnless(sys.platform == 'linux' and os.geteuid() == 0,
                     'requires isolated Linux root fixture')
class RootOwnershipHandoffTests(unittest.TestCase):
    def test_real_root_to_service_partial_retry(self):
        try:
            user = pwd.getpwnam('java-tron')
            group = grp.getgrnam('java-tron')
        except KeyError:
            self.skipTest('java-tron service account is unavailable')
        self.assertGreater(user.pw_uid, 0)
        self.assertEqual(user.pw_gid, group.gr_gid)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(os.path.realpath(temporary))
            root.chmod(0o755)
            source, target, cold, ancient = [root / name for name in
                                             ('source', 'target', 'cold', 'ancient')]
            for directory in (source, target, cold, ancient):
                directory.mkdir(mode=0o700)
            os.chown(str(cold), user.pw_uid, group.gr_gid)
            os.chown(str(ancient), user.pw_uid, group.gr_gid)
            etl = cold / 'etl'
            etl.mkdir(mode=0o700)
            (ancient / 'FLOCK').write_bytes(b'')
            for directory in (source, target):
                for name in ('CURRENT', 'LOCK', 'MANIFEST-000001', 'OPTIONS-000001',
                             '000001.sst', '000002.log'):
                    file = directory / name
                    file.write_bytes((name + '-payload').encode())
                    file.chmod(0o600)
            storage_locks = [directory / release.STAGING_STORAGE_LOCK
                             for directory in (source, target, cold)]
            start_lock = root / 'start.lock'
            for file in [start_lock] + storage_locks:
                file.write_bytes(b'')
                file.chmod(0o600)
            latch = {'state': 'VERIFIED_PENDING_ACTIVATION', 'service_was_active': True,
                     'job_id': '1' * 32, 'candidate_sha256': '2' * 64,
                     'source_commit': '3' * 40, 'source': str(source),
                     'target': str(target), 'cold': str(cold)}
            prepared = {name: latch[name] for name in ('source', 'target', 'cold')}
            files_before = {(str(directory), file.name): (file.read_bytes(), file.stat().st_mode)
                            for directory in (source, target) for file in directory.iterdir()}
            journal = root / 'handoff.json'
            with mock.patch.multiple(release, STAGING_SOURCE=source, STAGING_TARGET=target,
                                     STAGING_COLD=cold, STAGING_ANCIENT=ancient,
                                     STAGING_START_LOCK=start_lock, STAGING_HANDOFF=journal), \
                    mock.patch.object(release, 'show', return_value={
                        'ActiveState': 'inactive', 'MainPID': '0', 'User': 'java-tron',
                        'Group': 'java-tron', 'DynamicUser': 'no'}), \
                    mock.patch.object(release, '_staging_require_stopped'):
                held = []
                try:
                    for file in [start_lock] + storage_locks:
                        fd = os.open(str(file), os.O_RDWR)
                        fcntl.flock(fd, fcntl.LOCK_EX)
                        held.append(fd)
                    for interrupt in (True, False):
                        if not interrupt:
                            # Preserve the exact locked inode while exercising the
                            # production deployment account's shared 0644 layout.
                            before = os.fstat(held[0])
                            os.fchown(held[0], user.pw_uid, group.gr_gid)
                            os.fchmod(held[0], 0o644)
                            after = os.stat(str(start_lock), follow_symlinks=False)
                            self.assertEqual((before.st_dev, before.st_ino),
                                             (after.st_dev, after.st_ino))
                        child = os.fork()
                        if child == 0:
                            try:
                                if interrupt:
                                    original = release._staging_handoff_file
                                    calls = [0]
                                    def fail_after_first(*args):
                                        calls[0] += 1
                                        if calls[0] == 2:
                                            raise RuntimeError('injected crash')
                                        return original(*args)
                                    release._staging_handoff_file = fail_after_first
                                release.handoff_staging_storage(latch, prepared, '4' * 64)
                                os._exit(0)
                            except Exception as error:
                                sys.stderr.write(str(error) + '\n')
                                os._exit(1)
                        _, status = os.waitpid(child, 0)
                        self.assertEqual(os.WEXITSTATUS(status), 1 if interrupt else 0)
                        self.assertEqual(json.loads(journal.read_text())['phase'],
                                         'IN_PROGRESS' if interrupt else 'DONE')
                finally:
                    for fd in held:
                        os.close(fd)
            files_after = {(str(directory), file.name): (file.read_bytes(), file.stat().st_mode)
                           for directory in (source, target) for file in directory.iterdir()}
            self.assertEqual(files_before, files_after)
            for directory in (source, target):
                self.assertEqual((directory.stat().st_uid, directory.stat().st_gid),
                                 (user.pw_uid, group.gr_gid))
                for file in directory.iterdir():
                    if file.name == release.STAGING_STORAGE_LOCK:
                        self.assertEqual((file.stat().st_uid, file.stat().st_gid), (0, 0))
                    else:
                        self.assertEqual((file.stat().st_uid, file.stat().st_gid),
                                         (user.pw_uid, group.gr_gid))


if __name__ == '__main__':
    unittest.main()

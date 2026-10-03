"""The deployment lock is shared only with the trusted java-tron account."""
import stat
from types import SimpleNamespace
import unittest
from unittest import mock
from scripts.tests.test_mainnet_release_staging_handoff import release

class SharedDeploymentLockTests(unittest.TestCase):
    def info(self, uid=0, gid=0, mode=0o600, nlink=1):
        return SimpleNamespace(st_uid=uid, st_gid=gid, st_mode=stat.S_IFREG | mode, st_nlink=nlink)

    def test_root_private_lock_does_not_need_java_account(self):
        with mock.patch.object(release.pwd, 'getpwnam', side_effect=KeyError), \
             mock.patch.object(release.grp, 'getgrnam', side_effect=KeyError):
            self.assertTrue(release._staging_shared_deployment_lock_owned(self.info()))
            self.assertFalse(release._staging_shared_deployment_lock_owned(self.info(mode=0o644)))

    def test_only_exact_trusted_account_and_mode_permit_shared_start_lock(self):
        account = SimpleNamespace(pw_uid=1003, pw_gid=1003)
        group = SimpleNamespace(gr_gid=1003)
        with mock.patch.object(release.pwd, 'getpwnam', return_value=account) as user, \
             mock.patch.object(release.grp, 'getgrnam', return_value=group) as lookup:
            shared = self.info(1003, 1003, 0o644)
            self.assertTrue(release._staging_shared_deployment_lock_owned(shared))
            self.assertFalse(release._staging_root_lock_owned(shared))
            user.assert_called_with('java-tron')
            lookup.assert_called_with('java-tron')
            for info in (self.info(1004,1003,0o644), self.info(1003,1004,0o644),
                         self.info(1003,1003,0o666), self.info(1003,1003,0o4644), self.info(0,0,0o4600), self.info(1003,1003,0o600),
                         self.info(1003,1003,0o644,2), self.info(0,0,0o644)):
                self.assertFalse(release._staging_shared_deployment_lock_owned(info))
            account.pw_gid = 1004
            self.assertFalse(release._staging_shared_deployment_lock_owned(shared))
            account.pw_gid = 1003
            account.pw_uid = 0
            self.assertFalse(release._staging_shared_deployment_lock_owned(self.info(0,1003,0o644)))
            account.pw_uid = 1003
            group.gr_gid = 0
            self.assertFalse(release._staging_shared_deployment_lock_owned(self.info(1003,0,0o644)))

    def test_non_regular_or_symlink_rejected(self):
        info = self.info()
        info.st_mode = stat.S_IFLNK | 0o600
        self.assertFalse(release._staging_shared_deployment_lock_owned(info))
        self.assertFalse(release._staging_root_lock_owned(info))

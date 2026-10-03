import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock
from scripts.tests.test_history_staging_guard import guard
from scripts.tests.test_history_staging_migrate import migrate

class UpgradeFenceTests(unittest.TestCase):
    def test_pending_marker_blocks_start_deploy_and_activation_before_any_inspection(self):
        with tempfile.TemporaryDirectory() as directory:
            pending = Path(directory) / 'pending'
            pending.touch()
            with mock.patch.object(guard, 'REPIN_INTENT', pending), \
                 mock.patch.object(guard, 'service_binary') as service:
                for action in (guard.check_service, guard.check_deploy):
                    with self.assertRaisesRegex(ValueError, 'pending'):
                        action()
                service.assert_not_called()
            with mock.patch.object(migrate.release, 'STAGING_REPIN_INTENT', pending), \
                 mock.patch.object(migrate.release, 'root_bytes') as read:
                with self.assertRaisesRegex(RuntimeError, 'pending'):
                    migrate.release.activate_staging('a' * 40, Path('/candidate'), 'b' * 64)
                read.assert_not_called()

    def test_completed_upgrade_guard_requires_exact_prepared_and_frozen_latch(self):
        latch = {'upgrade_binding': str(guard.UPGRADE_BINDING), 'state': 'VERIFIED_PENDING_ACTIVATION',
                 'candidate_sha256': 'f' * 64, 'plan_producer_sha256': 'c' * 64, 'job_id': 'a' * 32}
        prepared = {'candidate_sha256': 'f' * 64}
        journal = {'version': 1, 'state': 'DONE', 'new_prepared': prepared,
                   'new_latch': dict(latch, state='MIGRATION_IN_PROGRESS')}
        with mock.patch.object(guard, 'read_root_json', return_value=journal):
            guard.check_upgrade_fence(latch, prepared)
            with self.assertRaisesRegex(ValueError, 'latch differs'):
                guard.check_upgrade_fence(dict(latch, candidate_sha256='b' * 64), prepared)
            with self.assertRaisesRegex(ValueError, 'reader fences'):
                guard.check_upgrade_fence(latch, {'candidate_sha256': 'b' * 64})
            journal['state'] = 'AUTHORIZED'
            with self.assertRaisesRegex(ValueError, 'reader fences'):
                guard.check_upgrade_fence(latch, prepared)

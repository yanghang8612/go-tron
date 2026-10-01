"""History-staging startup fence tests without systemd or root privileges."""

import importlib.util
from pathlib import Path
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location(
    'history_staging_guard', Path(__file__).parents[1] / 'history_staging_guard.py')
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)

OLD = '1' * 64
NEW = '2' * 64
OTHER = '3' * 64


class HistoryStagingGuardTests(unittest.TestCase):
    def test_prepared_existing_source_allows_only_pinned_legacy_binary(self):
        prepared = {'version': 1, 'legacy_binary_sha256': OLD}
        guard.check_identity(prepared, None, None, OLD, False)
        for digest in (NEW, OTHER):
            with self.assertRaisesRegex(ValueError, 'pinned legacy'):
                guard.check_identity(prepared, None, None, digest, True)

    def test_migration_latch_blocks_even_the_old_binary(self):
        prepared = {'version': 1, 'legacy_binary_sha256': OLD}
        latch = {'version': 1, 'state': 'MIGRATION_IN_PROGRESS',
                 'candidate_sha256': NEW, 'service_was_active': True}
        for digest in (OLD, NEW):
            with self.assertRaisesRegex(ValueError, 'migration in progress'):
                guard.check_identity(prepared, latch, None, digest, digest == NEW)

    def test_pending_activation_requires_fixed_capable_candidate_and_active_intent(self):
        latch = {'version': 1, 'state': 'VERIFIED_PENDING_ACTIVATION',
                 'candidate_sha256': NEW, 'service_was_active': True}
        guard.check_identity(None, latch, None, NEW, True)
        for digest, capable in ((OLD, True), (NEW, False)):
            with self.assertRaisesRegex(ValueError, 'fixed capable candidate'):
                guard.check_identity(None, latch, None, digest, capable)
        latch['service_was_active'] = False
        with self.assertRaisesRegex(ValueError, 'originally inactive'):
            guard.check_identity(None, latch, None, NEW, True)

    def test_reader_marker_blocks_old_rollback_and_pins_candidate(self):
        required = {'version': 1, 'format_version': 1,
                    'binary_sha256': NEW, 'stop_intent': False,
                    'prune_mode': 'snap', 'history_window': 65536}
        guard.check_identity(None, None, required, NEW, True)
        for digest, capable in ((OLD, False), (NEW, False), (OTHER, True)):
            with self.assertRaisesRegex(ValueError, 'marker/binary capability mismatch'):
                guard.check_identity(None, None, required, digest, capable)
        required['stop_intent'] = True
        with self.assertRaisesRegex(ValueError, 'inactive intent'):
            guard.check_identity(None, None, required, NEW, True)

    def test_unknown_or_absent_state_fails_closed(self):
        with self.assertRaisesRegex(ValueError, 'not initialized'):
            guard.check_identity(None, None, None, OLD, False)
        with self.assertRaisesRegex(ValueError, 'unknown'):
                guard.check_identity(None, {'version': 1, 'state': 'UNKNOWN',
                                            'candidate_sha256': NEW}, None, NEW, True)

    def test_inactive_intent_is_a_clean_timer_skip(self):
        required = {'version': 1, 'format_version': 1,
                    'binary_sha256': NEW, 'stop_intent': True,
                    'prune_mode': 'snap', 'history_window': 65536}
        with mock.patch.object(guard, 'read_root_json', side_effect=[None, None, required]):
            with self.assertRaises(guard.StopIntent):
                guard.check_deploy()

    def test_service_check_uses_effective_binary_and_fails_bad_capability(self):
        required = {'version': 1, 'format_version': 1,
                    'binary_sha256': NEW, 'stop_intent': False,
                    'prune_mode': 'snap', 'history_window': 65536}
        with mock.patch.object(guard, 'read_root_json', side_effect=[None, None, required]), \
                mock.patch.object(guard, 'service_binary', return_value='/release/gtron'), \
                mock.patch.object(guard, 'binary_sha', return_value=NEW), \
                mock.patch.object(guard, 'capable', return_value=False):
            with self.assertRaisesRegex(ValueError, 'capability mismatch'):
                guard.check_service()


if __name__ == '__main__':
    unittest.main()

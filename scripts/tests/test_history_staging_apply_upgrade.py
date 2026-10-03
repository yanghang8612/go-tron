"""Durable executor replacement tests; no host services or Pebble writes."""
import json
from contextlib import ExitStack
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts.tests.test_history_staging_migrate import HistoryStagingMigrateTests, migrate

class ApplyUpgradeTests(unittest.TestCase):
    repin_fixture = HistoryStagingMigrateTests.repin_fixture
    repin_patches = HistoryStagingMigrateTests.repin_patches

    def fixture(self, root):
        latch, prepared, paths, sha, lp, pp, rp = self.repin_fixture(root)
        latch.update(plan_id='9' * 64, legacy_manifest_sha256=sha)
        lp.write_text(json.dumps(latch))
        return latch, prepared, paths, sha, lp, pp, rp

    def patches(self, root, fixture, cli):
        old, prepared, paths, sha, lp, pp, rp = fixture
        def write(value):
            (root / 'upgrade.json').write_text(json.dumps(value))
        return self.repin_patches(root, paths, lp, pp, rp, cli) + [
            mock.patch.object(migrate.release, 'root_sha', return_value=old['candidate_sha256']),
            mock.patch.object(migrate, 'UPGRADE_BINDING', root / 'upgrade.json'),
            mock.patch.object(migrate, 'verify_source_config'),
            mock.patch.object(migrate, 'verify_apply_upgrade_guards'),
            mock.patch.object(migrate, 'write_apply_upgrade', side_effect=write)]

    def run_upgrade(self, root, fixture, sha='f' * 64, source='e' * 40):
        return migrate.apply_repin(fixture[0]['job_id'], root / 'new', source, sha, fixture[3])

    def test_every_durable_boundary_exact_retry_and_conflict(self):
        for boundary in ('intent', 'precheck', 'authorized', 'prepared', 'latch', 'done', 'unlink'):
            with self.subTest(boundary=boundary), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                fixture = self.fixture(root)
                cli = mock.Mock(return_value={'phase': 'upgrade-check'})
                with ExitStack() as stack:
                    for patch in self.patches(root, fixture, cli):
                        stack.enter_context(patch)
                    atomic, journal_write, unlink = migrate.atomic_json, migrate.write_apply_upgrade, migrate.remove_durable_file
                    fired = []
                    def fault(path, value):
                        atomic(path, value)
                        where = 'intent' if path == migrate.REPIN_INTENT else 'prepared' if path == fixture[5] else 'latch'
                        if where == boundary and not fired:
                            fired.append(True)
                            raise RuntimeError('power interruption')
                    def journal_fault(value):
                        journal_write(value)
                        if value['state'].lower() == boundary and not fired:
                            fired.append(True)
                            raise RuntimeError('power interruption')
                    def unlink_fault(path):
                        unlink(path)
                        if boundary == 'unlink' and not fired:
                            fired.append(True)
                            raise RuntimeError('power interruption')
                    with mock.patch.object(migrate, 'atomic_json', side_effect=fault), \
                         mock.patch.object(migrate, 'write_apply_upgrade', side_effect=journal_fault), \
                         mock.patch.object(migrate, 'remove_durable_file', side_effect=unlink_fault):
                        with self.assertRaisesRegex(RuntimeError, 'power interruption'):
                            self.run_upgrade(root, fixture)
                    before = (fixture[4].read_bytes(), fixture[5].read_bytes())
                    with self.assertRaises(RuntimeError):
                        self.run_upgrade(root, fixture, sha='1' * 64)
                    self.assertEqual(before, (fixture[4].read_bytes(), fixture[5].read_bytes()))
                    result = self.run_upgrade(root, fixture)
                    self.assertEqual(result['plan_producer_sha256'], fixture[0]['candidate_sha256'])
                    latch = json.loads(fixture[4].read_text())
                    self.assertEqual(latch['plan_id'], fixture[0]['plan_id'])
                    self.assertEqual(latch['candidate_sha256'], 'f' * 64)
                    self.assertEqual(json.loads((root / 'upgrade.json').read_text())['state'], 'DONE')
                    self.assertFalse(migrate.REPIN_INTENT.exists())
                    self.assertEqual(self.run_upgrade(root, fixture), result)
                    if cli.call_count:
                        self.assertEqual(cli.call_args[0][1:], ('upgrade-check', '--plan-id', fixture[0]['plan_id']))

    def test_failed_preflight_stays_fenced_and_pending_resume_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = self.fixture(root)
            with ExitStack() as stack:
                for patch in self.patches(root, fixture, mock.Mock(side_effect=RuntimeError('corrupt receipt'))):
                    stack.enter_context(patch)
                with self.assertRaisesRegex(RuntimeError, 'corrupt receipt'):
                    self.run_upgrade(root, fixture)
                self.assertTrue(migrate.REPIN_INTENT.exists())
                self.assertEqual(json.loads(fixture[4].read_text()), fixture[0])
                with self.assertRaisesRegex(RuntimeError, 'repin is pending'):
                    migrate.resume(fixture[0]['job_id'])

    def test_pending_activation_and_path_or_old_identity_changes_refused(self):
        for change in ('activation', 'path', 'prepared', 'reader'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                fixture = self.fixture(root)
                with ExitStack() as stack:
                    cli = mock.Mock(return_value={'phase': 'upgrade-check'})
                    for patch in self.patches(root, fixture, cli):
                        stack.enter_context(patch)
                    old = dict(fixture[0])
                    if change == 'activation':
                        old['state'] = 'VERIFIED_PENDING_ACTIVATION'
                        old.update(prune_mode='snap', history_window=256)
                    elif change == 'path':
                        old['target'] = str(root / 'conflicting')
                    elif change == 'reader':
                        fixture[6].touch()
                    else:
                        prepared = dict(fixture[1], candidate_sha256='1' * 64)
                        fixture[5].write_text(json.dumps(prepared))
                    fixture[4].write_text(json.dumps(old))
                    with self.assertRaises(RuntimeError):
                        self.run_upgrade(root, fixture)
                    self.assertFalse(cli.called)
                    self.assertFalse(migrate.REPIN_INTENT.exists())

    def test_upgrade_check_final_cli_record_uses_actual_executor(self):
        latch = {'candidate': '/candidate', 'source': '/source', 'target': '/target',
                 'cold': '/cold', 'job_id': 'a' * 32, 'candidate_sha256': 'f' * 64,
                 'source_config_args': [], 'source_config_sha256': '',
                 'upgrade_binding': str(migrate.UPGRADE_BINDING), 'plan_id': '9' * 64}
        record = dict(version=1, phase='upgrade-check', job_id=latch['job_id'],
                      candidate_sha256=latch['candidate_sha256'], source=latch['source'], target=latch['target'])
        def stream(argv, output, job, action):
            self.assertIn('--upgrade-binding', argv)
            output.write((json.dumps(record) + '\n').encode())
            return 0, b''
        with mock.patch.object(migrate, 'verify_source_config'), \
             mock.patch.object(migrate, 'stream_cli_stderr', side_effect=stream):
            self.assertEqual(migrate.cli_result(latch, 'upgrade-check', '--plan-id', latch['plan_id']), record)
        record['candidate_sha256'] = 'c' * 64
        with mock.patch.object(migrate, 'verify_source_config'), \
             mock.patch.object(migrate, 'stream_cli_stderr', side_effect=stream):
            with self.assertRaises(RuntimeError):
                migrate.cli_result(latch, 'upgrade-check', '--plan-id', latch['plan_id'])

if __name__ == '__main__':
    unittest.main()

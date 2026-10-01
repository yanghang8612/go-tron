"""Fail-closed migration orchestration tests without touching the host."""

import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock


SCRIPTS = Path(__file__).parents[1]
sys.path.insert(0, str(SCRIPTS))
SPEC = importlib.util.spec_from_file_location(
    'history_staging_migrate', SCRIPTS / 'history_staging_migrate.py')
migrate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(migrate)


class HistoryStagingMigrateTests(unittest.TestCase):
    def test_cli_stderr_heartbeat_is_visible_before_process_completes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            gate = root / 'continue'
            script = root / 'fake-cli.py'
            script.write_text('import pathlib, sys, time\n'
                              'sys.stderr.write("phase=copy bucket=7\\n")\n'
                              'sys.stderr.flush()\n'
                              'while not pathlib.Path(sys.argv[1]).exists(): time.sleep(.01)\n'
                              'sys.stdout.write("done\\n")\n')
            latch = {'job_id': 'a' * 32}
            output = tempfile.TemporaryFile()
            observed = io.StringIO()
            result = []
            errors = []
            def run():
                try:
                    result.append(migrate.stream_cli_stderr(
                        [sys.executable, str(script), str(gate)], output, latch, 'apply'))
                except Exception as error:
                    errors.append(error)
            with mock.patch.object(migrate, 'CLI_LOG_DIR', root / 'logs'), \
                    mock.patch.object(migrate.sys, 'stderr', observed):
                worker = threading.Thread(target=run)
                worker.start()
                try:
                    deadline = time.monotonic() + 5
                    while 'phase=copy bucket=7' not in observed.getvalue() and time.monotonic() < deadline:
                        time.sleep(.01)
                    self.assertIn('phase=copy bucket=7', observed.getvalue(),
                                  'worker=%r errors=%r result=%r' % (worker.is_alive(), errors, result))
                    self.assertTrue(worker.is_alive(), 'heartbeat appeared only after CLI exit')
                finally:
                    gate.write_text('continue')
                    worker.join(timeout=5)
            self.assertFalse(worker.is_alive())
            self.assertFalse(errors)
            self.assertEqual(result[0][0], 0)
            output.seek(0)
            self.assertEqual(output.read(), b'done\n')
            output.close()
            self.assertIn(b'phase=copy bucket=7', (root / 'logs' / ('a' * 32 + '_apply.stderr.log')).read_bytes())

    def test_pre_latch_fence_failure_leaves_service_running_and_timer_stopped(self):
        old = {'binary': '/data/gtron/releases/legacy/gtron',
               'sha': 'a' * 64, 'source': 'b' * 40, 'active': True,
               'argv': ['/data/gtron/releases/legacy/gtron']}
        calls = []

        def systemctl(*args, **kwargs):
            calls.append(args)

        with mock.patch.object(migrate.os, 'geteuid', return_value=0), \
                mock.patch.object(migrate.os.path, 'lexists', return_value=False), \
                mock.patch.object(migrate, 'canonical_paths', return_value={
                    'source': '/source', 'target': '/target', 'cold': '/cold'}), \
                mock.patch.object(migrate, 'pinned_candidate', return_value=Path('/candidate')), \
                mock.patch.object(migrate.release, 'inspect', return_value=old), \
                mock.patch.object(migrate.release, 'root_sha', return_value=old['sha']), \
                mock.patch.object(migrate, 'unit_state', side_effect=[
                    {'active': True, 'enabled': True},
                    {'active': True, 'enabled': True}]), \
                mock.patch.object(migrate, 'systemctl', side_effect=systemctl), \
                mock.patch.object(migrate, 'verify_stopped'), \
                mock.patch.object(migrate, 'install_startup_fences',
                                  side_effect=RuntimeError('guard install failed')):
            with self.assertRaisesRegex(RuntimeError, 'guard install failed'):
                migrate.run_new('/input', 'b' * 40, 'c' * 64)
        self.assertEqual(calls[:2], [('stop', migrate.TIMER),
                                     ('stop', migrate.DEPLOY_SERVICE)])
        self.assertNotIn(('stop', migrate.SERVICE), calls)
        self.assertNotIn(('start', migrate.TIMER), calls)

    def test_done_journal_recovers_timer_after_latch_removed(self):
        job = 'a' * 32
        done = {'version': 1, 'state': 'DONE', 'job_id': job,
                'candidate_sha256': 'b' * 64, 'source_commit': 'c' * 40,
                'service_was_active': False, 'timer_was_active': True,
                'timer_was_enabled': True}
        current = {'sha': 'b' * 64, 'source': 'c' * 40,
                   'active': False, 'staging': (b'{}', 0o644)}
        with mock.patch.object(migrate.release, 'root_bytes',
                               return_value=(json.dumps(done).encode(), 0o644)), \
                mock.patch.object(migrate.os.path, 'lexists', return_value=False), \
                mock.patch.object(migrate.release, 'inspect', return_value=current), \
                mock.patch.object(migrate, 'verify_stopped'), \
                mock.patch.object(migrate, 'systemctl') as systemctl:
            self.assertEqual(migrate.finalize_done(job), done)
        systemctl.assert_any_call('enable', migrate.TIMER, timeout=30)
        systemctl.assert_any_call('start', migrate.TIMER, timeout=30)

    def test_done_journal_wrong_candidate_does_not_restart_timer(self):
        done = {'version': 1, 'state': 'DONE', 'job_id': 'a' * 32,
                'candidate_sha256': 'b' * 64, 'source_commit': 'c' * 40,
                'service_was_active': True, 'timer_was_active': True,
                'timer_was_enabled': True}
        with mock.patch.object(migrate.release, 'root_bytes',
                               return_value=(json.dumps(done).encode(), 0o644)), \
                mock.patch.object(migrate.os.path, 'lexists', return_value=False), \
                mock.patch.object(migrate.release, 'inspect', return_value={
                    'sha': 'd' * 64, 'source': 'c' * 40,
                    'active': True, 'staging': (b'{}', 0o644)}), \
                mock.patch.object(migrate, 'systemctl') as systemctl:
            with self.assertRaisesRegex(RuntimeError, 'identity'):
                migrate.finalize_done('a' * 32)
        systemctl.assert_not_called()

    def test_cli_jsonl_identity_must_match_latch(self):
        with tempfile.TemporaryDirectory() as directory:
            script = Path(directory) / 'candidate'
            script.write_text('#!/usr/bin/env python3\n'
                              'import json, sys\n'
                              'print(json.dumps({"version":1,"phase":sys.argv[3],'
                              '"job_id":"wrong","candidate_sha256":"b"*64,'
                              '"source":"/source","target":"/target"}))\n')
            script.chmod(0o755)
            latch = {'candidate': str(script), 'job_id': 'a' * 32,
                     'candidate_sha256': 'b' * 64,
                     'source': '/source', 'target': '/target', 'cold': '/cold',
                     'source_config_args': [], 'source_config_sha256': ''}
            with mock.patch.object(migrate, 'CLI_LOG_DIR', Path(directory) / 'logs'):
                with self.assertRaisesRegex(RuntimeError, 'invalid migration identity'):
                    migrate.cli_result(latch, 'inspect')


if __name__ == '__main__':
    unittest.main()

"""Fail-closed migration orchestration tests without touching the host."""

import importlib.util
from contextlib import ExitStack
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
    def test_checked_unpublished_job_temps_requires_bounded_exact_header(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan_dir = root / 'history-staging-plans'
            plan_dir.mkdir(mode=0o700)
            plan_dir.chmod(0o700)
            latch = {'job_id': 'a' * 32, 'candidate_sha256': 'b' * 64,
                     'source': '/source', 'target': '/target', 'cold': '/cold'}
            header = {'version': 1, 'job_id': latch['job_id'],
                      'candidate_sha256': latch['candidate_sha256'],
                      'manifest_sha256': 'c' * 64,
                      'paths': {key: latch[key] for key in ('source', 'target', 'cold')}}
            temporary = plan_dir / ('.' + latch['job_id'] + '.plan.123456789.tmp')
            temporary.write_text(json.dumps(header) + '\n' + 'partial row')
            temporary.chmod(0o600)
            self.assertEqual(len(migrate.checked_unpublished_job_temps(
                plan_dir, latch, 'c' * 64, owner=migrate.os.geteuid())), 1)
            with self.assertRaisesRegex(RuntimeError, 'header differs'):
                migrate.checked_unpublished_job_temps(plan_dir, latch, 'd' * 64,
                                                       owner=migrate.os.geteuid())
            bad = dict(header, candidate_sha256='d' * 64)
            temporary.write_text(json.dumps(bad) + '\n')
            temporary.chmod(0o600)
            with self.assertRaisesRegex(RuntimeError, 'header differs'):
                migrate.checked_unpublished_job_temps(plan_dir, latch, 'c' * 64,
                                                       owner=migrate.os.geteuid())
            temporary.write_text(json.dumps(header) + '\n')
            temporary.chmod(0o600)
            (plan_dir / (latch['job_id'] + '.jsonl')).write_text('published')
            with self.assertRaisesRegex(RuntimeError, 'published, foreign'):
                migrate.checked_unpublished_job_temps(plan_dir, latch, 'c' * 64,
                                                       owner=migrate.os.geteuid())
            (plan_dir / (latch['job_id'] + '.jsonl')).unlink()
            (plan_dir / ('.' + 'b' * 32 + '.plan.123456789.tmp')).write_text('foreign')
            with self.assertRaisesRegex(RuntimeError, 'published, foreign'):
                migrate.checked_unpublished_job_temps(plan_dir, latch, 'c' * 64,
                                                       owner=migrate.os.geteuid())

    def test_discard_preplan_tmp_only_after_storage_proof_then_strict_pristine(self):
        for storage_pristine in (False, True):
            with self.subTest(storage_pristine=storage_pristine), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                old, prepared, paths, legacy_sha, latch_path, prepared_path, required_path = \
                    self.repin_fixture(root)
                plan_dir = root / 'history-staging-plans'
                plan_dir.mkdir(mode=0o700)
                plan_dir.chmod(0o700)
                header = {'version': 1, 'job_id': old['job_id'],
                          'candidate_sha256': old['candidate_sha256'],
                          'manifest_sha256': legacy_sha, 'paths': paths}
                temporary = plan_dir / ('.' + old['job_id'] + '.plan.123456789.tmp')
                temporary.write_text(json.dumps(header) + '\npartial')
                temporary.chmod(0o600)
                actions = []
                def cli(probe, action, *options):
                    actions.append(options)
                    if options == ('--verify-pristine-storage',):
                        return {'phase': 'inspect', 'storage_pristine': storage_pristine}
                    self.assertFalse(temporary.exists())
                    return {'phase': 'inspect', 'pristine': True}
                patches = self.repin_patches(root, paths, latch_path, prepared_path,
                                             required_path, cli)
                real_checker = migrate.checked_unpublished_job_temps
                actual_owner = migrate.os.geteuid()
                with ExitStack() as stack:
                    for patch in patches:
                        stack.enter_context(patch)
                    stack.enter_context(mock.patch.object(migrate, 'PLAN_DIR', plan_dir))
                    stack.enter_context(mock.patch.object(
                        migrate, 'checked_unpublished_job_temps',
                        side_effect=lambda directory, latch, digest: real_checker(
                            directory, latch, digest, owner=actual_owner)))
                    if storage_pristine:
                        result = migrate.discard_preplan_tmp(old['job_id'], root / 'new',
                                                             'e' * 40, 'f' * 64, legacy_sha)
                        self.assertEqual(result['removed'], 1)
                        self.assertFalse(temporary.exists())
                        self.assertEqual(actions, [('--verify-pristine-storage',),
                                                   ('--verify-pristine',)])
                    else:
                        with self.assertRaisesRegex(RuntimeError, 'did not prove'):
                            migrate.discard_preplan_tmp(old['job_id'], root / 'new',
                                                       'e' * 40, 'f' * 64, legacy_sha)
                        self.assertTrue(temporary.exists())
                        self.assertEqual(actions, [('--verify-pristine-storage',)])
                    self.assertEqual(json.loads(latch_path.read_text()), old)
                    self.assertEqual(json.loads(prepared_path.read_text()), prepared)

    def test_discard_preplan_tmp_rechecks_file_before_unlink_and_retries_partial_unlink(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old, _, paths, legacy_sha, latch_path, prepared_path, required_path = \
                self.repin_fixture(root)
            plan_dir = root / 'history-staging-plans'
            plan_dir.mkdir(mode=0o700)
            plan_dir.chmod(0o700)
            header = {'version': 1, 'job_id': old['job_id'],
                      'candidate_sha256': old['candidate_sha256'],
                      'manifest_sha256': legacy_sha, 'paths': paths}
            temps = [plan_dir / ('.' + old['job_id'] + '.plan.%09d.tmp' % index)
                     for index in (1, 2)]
            for temporary in temps:
                temporary.write_text(json.dumps(header) + '\npartial')
                temporary.chmod(0o600)
            mutate = [True]
            def cli(probe, action, *options):
                if options == ('--verify-pristine-storage',):
                    if mutate[0]:
                        temps[0].write_text(json.dumps(header) + '\nchanged after inventory')
                        temps[0].chmod(0o600)
                    return {'phase': 'inspect', 'storage_pristine': True}
                return {'phase': 'inspect', 'pristine': True}
            patches = self.repin_patches(root, paths, latch_path, prepared_path,
                                         required_path, cli)
            checker = migrate.checked_unpublished_job_temps
            actual_owner = migrate.os.geteuid()
            with ExitStack() as stack:
                for patch in patches:
                    stack.enter_context(patch)
                stack.enter_context(mock.patch.object(migrate, 'PLAN_DIR', plan_dir))
                stack.enter_context(mock.patch.object(
                    migrate, 'checked_unpublished_job_temps',
                    side_effect=lambda directory, latch, digest: checker(
                        directory, latch, digest, owner=actual_owner)))
                with self.assertRaisesRegex(RuntimeError, 'changed during storage proof'):
                    migrate.discard_preplan_tmp(old['job_id'], root / 'new',
                                               'e' * 40, 'f' * 64, legacy_sha)
                self.assertTrue(all(path.exists() for path in temps))
                mutate[0] = False
                real_unlink = migrate.os.unlink
                removed = [0]
                def fail_after_one(path):
                    if removed[0] == 1:
                        raise RuntimeError('simulated unlink interruption')
                    removed[0] += 1
                    real_unlink(path)
                with mock.patch.object(migrate.os, 'unlink', side_effect=fail_after_one):
                    with self.assertRaisesRegex(RuntimeError, 'unlink interruption'):
                        migrate.discard_preplan_tmp(old['job_id'], root / 'new',
                                                   'e' * 40, 'f' * 64, legacy_sha)
                self.assertEqual(sum(path.exists() for path in temps), 1)
                result = migrate.discard_preplan_tmp(old['job_id'], root / 'new',
                                                     'e' * 40, 'f' * 64, legacy_sha)
                self.assertEqual(result['removed'], 1)
                self.assertFalse(any(path.exists() for path in temps))

    def test_discard_preplan_tmp_rejects_changed_latch_manifest_pin(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old, prepared, paths, legacy_sha, latch_path, prepared_path, required_path = \
                self.repin_fixture(root)
            old['legacy_manifest_sha256'] = '0' * 64
            latch_path.write_text(json.dumps(old))
            patches = self.repin_patches(root, paths, latch_path, prepared_path,
                                         required_path,
                                         lambda *args: self.fail('CLI must not run'))
            with ExitStack() as stack:
                for patch in patches:
                    stack.enter_context(patch)
                with self.assertRaisesRegex(RuntimeError, 'pin differs from durable latch'):
                    migrate.discard_preplan_tmp(old['job_id'], root / 'new',
                                               'e' * 40, 'f' * 64, legacy_sha)
                self.assertEqual(json.loads(latch_path.read_text()), old)
                self.assertEqual(json.loads(prepared_path.read_text()), prepared)

    def test_verify_stopped_timer_avoids_unsupported_mainpid_property(self):
        calls = []
        def old_systemd(argv, timeout):
            calls.append(argv)
            if '--property=MainPID' in argv:
                raise RuntimeError('Unknown property MainPID for timer unit')
            return 'ActiveState=inactive\n'
        with mock.patch.object(migrate, 'command', side_effect=old_systemd):
            migrate.verify_stopped(migrate.TIMER)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0], ['/bin/systemctl', 'show', migrate.TIMER,
                                    '--property=ActiveState', '--no-pager'])

    def test_verify_stopped_rejects_active_timer_and_service_pid(self):
        with mock.patch.object(migrate, 'command', return_value='ActiveState=active\n'):
            with self.assertRaisesRegex(RuntimeError, 'running process or timer'):
                migrate.verify_stopped(migrate.TIMER)
        with mock.patch.object(migrate, 'command',
                               return_value='ActiveState=inactive\nMainPID=123\n') as run:
            with self.assertRaisesRegex(RuntimeError, 'running process or timer'):
                migrate.verify_stopped(migrate.SERVICE)
        self.assertIn('--property=MainPID', run.call_args[0][0])
        with mock.patch.object(migrate, 'command', return_value='ActiveState=inactive\n'):
            with self.assertRaisesRegex(RuntimeError, 'running process or timer'):
                migrate.verify_stopped(migrate.SERVICE)

    def repin_fixture(self, root):
        source, target, cold = (root / name for name in ('source', 'target', 'cold'))
        for path in (source, target, cold):
            path.mkdir()
        (cold / 'manifest.json').write_bytes(b'unchanged legacy manifest')
        legacy_sha = migrate.sha_file(cold / 'manifest.json')
        paths = {'source': str(source), 'target': str(target), 'cold': str(cold)}
        latch = {'version': 1, 'state': 'MIGRATION_IN_PROGRESS',
                 'job_id': 'a' * 32, 'source_commit': 'b' * 40,
                 'candidate_sha256': 'c' * 64, 'candidate': str(root / 'old'),
                 'service_was_active': True, 'timer_was_active': True,
                 'timer_was_enabled': True, 'staging_health_timeout_sec': 43200,
                 'source_config_args': [], 'source_config_sha256': '', **paths}
        prepared = {'version': 1, 'legacy_binary_sha256': 'd' * 64,
                    'candidate_sha256': latch['candidate_sha256'],
                    'source_commit': latch['source_commit'],
                    'service_was_active': True, 'timer_was_active': True,
                    'timer_was_enabled': True, **paths}
        latch_path = root / 'HISTORY_STAGING_MIGRATION.json'
        prepared_path = root / 'HISTORY_STAGING_PREPARED.json'
        required_path = root / 'HISTORY_STAGING_REQUIRED.json'
        latch_path.write_text(json.dumps(latch))
        prepared_path.write_text(json.dumps(prepared))
        for name in ('guard', 'dropin', 'start.sh'):
            (root / name).touch()
        return latch, prepared, paths, legacy_sha, latch_path, prepared_path, required_path

    def repin_patches(self, root, paths, latch_path, prepared_path, required_path, cli):
        def root_bytes(path, limit):
            data = Path(path).read_bytes()
            self.assertLessEqual(len(data), limit)
            return data, 0o644
        def atomic(path, value):
            Path(path).write_text(json.dumps(value))
        return [mock.patch.object(migrate.os, 'geteuid', return_value=0),
                mock.patch.object(migrate, 'APP', root),
                mock.patch.object(migrate, 'GUARD_INSTALL', root / 'guard'),
                mock.patch.object(migrate, 'DROPIN', root / 'dropin'),
                mock.patch.object(migrate, 'LOCK', root / 'start.lock'),
                mock.patch.object(migrate, 'REPIN_INTENT', root / 'REPIN_INTENT.json'),
                mock.patch.object(migrate.release, 'STAGING_LATCH', latch_path),
                mock.patch.object(migrate.release, 'STAGING_PREPARED', prepared_path),
                mock.patch.object(migrate.release, 'STAGING_REQUIRED', required_path),
                mock.patch.object(migrate.release, 'root_bytes', side_effect=root_bytes),
                mock.patch.object(migrate, 'canonical_paths', return_value=paths),
                mock.patch.object(migrate, 'flock_file'),
                mock.patch.object(migrate, 'storage_locks'),
                mock.patch.object(migrate, 'verify_stopped'),
                mock.patch.object(migrate, 'pinned_candidate', return_value=root / 'new'),
                mock.patch.object(migrate, 'cli_result', side_effect=cli),
                mock.patch.object(migrate, 'atomic_json', side_effect=atomic)]

    def test_repin_preplan_preserves_original_intent_and_requires_pristine(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old, prepared, paths, legacy_sha, latch_path, prepared_path, required_path = \
                self.repin_fixture(root)
            calls = []
            def cli(probe, action, *options):
                calls.append((probe['source_commit'], action, options,
                              probe['legacy_manifest_sha256']))
                return {'phase': 'inspect', 'pristine': True}
            patches = self.repin_patches(root, paths, latch_path, prepared_path,
                                         required_path, cli)
            with ExitStack() as stack:
                for patch in patches:
                    stack.enter_context(patch)
                result = migrate.repin_preplan(old['job_id'], root / 'new', 'e' * 40,
                                               'f' * 64, legacy_sha)
                self.assertTrue(result['pristine'])
                self.assertEqual(calls, [('e' * 40, 'inspect', ('--verify-pristine',), legacy_sha)])
                updated = json.loads(latch_path.read_text())
                self.assertEqual(updated['job_id'], old['job_id'])
                self.assertEqual(updated['service_was_active'], old['service_was_active'])
                self.assertEqual(updated['timer_was_enabled'], old['timer_was_enabled'])
                self.assertEqual(updated['legacy_manifest_sha256'], legacy_sha)
                self.assertEqual(json.loads(prepared_path.read_text())['legacy_binary_sha256'],
                                 prepared['legacy_binary_sha256'])
                self.assertFalse((root / 'REPIN_INTENT.json').exists())

    def test_repin_preplan_crash_after_prepared_retries_same_intent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old, _, paths, legacy_sha, latch_path, prepared_path, required_path = \
                self.repin_fixture(root)
            def cli(probe, action, *options):
                return {'phase': 'inspect', 'pristine': True}
            patches = self.repin_patches(root, paths, latch_path, prepared_path,
                                         required_path, cli)
            with ExitStack() as stack:
                for patch in patches:
                    stack.enter_context(patch)
                with mock.patch.object(migrate, 'write_latch', side_effect=RuntimeError('power loss')):
                    with self.assertRaisesRegex(RuntimeError, 'power loss'):
                        migrate.repin_preplan(old['job_id'], root / 'new', 'e' * 40,
                                              'f' * 64, legacy_sha)
                self.assertTrue((root / 'REPIN_INTENT.json').exists())
                self.assertEqual(json.loads(latch_path.read_text())['candidate_sha256'], 'c' * 64)
                self.assertEqual(json.loads(prepared_path.read_text())['candidate_sha256'], 'f' * 64)
                with self.assertRaisesRegex(RuntimeError, 'different durable repin intent'):
                    migrate.repin_preplan(old['job_id'], root / 'new', '1' * 40,
                                          '2' * 64, legacy_sha)
                migrate.repin_preplan(old['job_id'], root / 'new', 'e' * 40,
                                      'f' * 64, legacy_sha)
                self.assertEqual(json.loads(latch_path.read_text())['candidate_sha256'], 'f' * 64)
                self.assertFalse((root / 'REPIN_INTENT.json').exists())

    def test_repin_preplan_each_durable_write_boundary_retries(self):
        for boundary in ('intent', 'prepared', 'latch'):
            with self.subTest(boundary=boundary), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                old, prepared, paths, legacy_sha, latch_path, prepared_path, required_path = \
                    self.repin_fixture(root)
                patches = self.repin_patches(root, paths, latch_path, prepared_path,
                                             required_path,
                                             lambda *args: {'phase': 'inspect', 'pristine': True})
                with ExitStack() as stack:
                    for patch in patches:
                        stack.enter_context(patch)
                    target = {'intent': root / 'REPIN_INTENT.json',
                              'prepared': prepared_path, 'latch': latch_path}[boundary]
                    tripped = [False]
                    def interrupt_after_write(path, value):
                        Path(path).write_text(json.dumps(value))
                        if Path(path) == target and not tripped[0]:
                            tripped[0] = True
                            raise RuntimeError('simulated power loss')
                    with mock.patch.object(migrate, 'atomic_json',
                                           side_effect=interrupt_after_write):
                        with self.assertRaisesRegex(RuntimeError, 'simulated power loss'):
                            migrate.repin_preplan(old['job_id'], root / 'new', 'e' * 40,
                                                  'f' * 64, legacy_sha)
                    self.assertTrue((root / 'REPIN_INTENT.json').exists())
                    migrate.repin_preplan(old['job_id'], root / 'new', 'e' * 40,
                                          'f' * 64, legacy_sha)
                    self.assertFalse((root / 'REPIN_INTENT.json').exists())
                    self.assertEqual(json.loads(latch_path.read_text())['candidate_sha256'],
                                     'f' * 64)
                    self.assertEqual(json.loads(prepared_path.read_text())['candidate_sha256'],
                                     'f' * 64)
                    self.assertEqual(json.loads(prepared_path.read_text())['legacy_binary_sha256'],
                                     prepared['legacy_binary_sha256'])

    def test_repin_preplan_rejects_unproven_or_started_work_without_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old, prepared, paths, legacy_sha, latch_path, prepared_path, required_path = \
                self.repin_fixture(root)
            patches = self.repin_patches(root, paths, latch_path, prepared_path,
                                         required_path,
                                         lambda *args: {'phase': 'inspect', 'pristine': False})
            with ExitStack() as stack:
                for patch in patches:
                    stack.enter_context(patch)
                with self.assertRaisesRegex(RuntimeError, 'did not prove'):
                    migrate.repin_preplan(old['job_id'], root / 'new', 'e' * 40,
                                          'f' * 64, legacy_sha)
                self.assertEqual(json.loads(latch_path.read_text()), old)
                self.assertEqual(json.loads(prepared_path.read_text()), prepared)
                self.assertFalse((root / 'REPIN_INTENT.json').exists())
                with self.assertRaisesRegex(RuntimeError, 'manifest SHA differs'):
                    migrate.repin_preplan(old['job_id'], root / 'new', 'e' * 40,
                                          'f' * 64, '0' * 64)
                planned = dict(old, plan_id='1' * 64)
                latch_path.write_text(json.dumps(planned))
                with self.assertRaisesRegex(RuntimeError, 'unfinished pre-plan'):
                    migrate.repin_preplan(old['job_id'], root / 'new', 'e' * 40,
                                          'f' * 64, legacy_sha)
                latch_path.write_text(json.dumps(old))
                with mock.patch.object(migrate, 'verify_stopped',
                                       side_effect=RuntimeError('gtron.service still active')):
                    with self.assertRaisesRegex(RuntimeError, 'still active'):
                        migrate.repin_preplan(old['job_id'], root / 'new', 'e' * 40,
                                              'f' * 64, legacy_sha)
                self.assertEqual(json.loads(latch_path.read_text()), old)
                self.assertEqual(json.loads(prepared_path.read_text()), prepared)

    def test_cli_result_passes_explicit_legacy_manifest_sha(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            latch = {'candidate': '/candidate', 'job_id': 'a' * 32,
                     'candidate_sha256': 'b' * 64, 'legacy_manifest_sha256': 'c' * 64,
                     'source': '/source', 'target': '/target', 'cold': '/cold',
                     'source_config_args': [], 'source_config_sha256': ''}
            commands = []
            def stream(commandline, output, identity, action):
                commands.append(commandline)
                output.write((json.dumps({'version': 1, 'phase': action,
                                          'job_id': identity['job_id'],
                                          'candidate_sha256': identity['candidate_sha256'],
                                          'source': identity['source'],
                                          'target': identity['target']}) + '\n').encode())
                return 0, b''
            with mock.patch.object(migrate, 'stream_cli_stderr', side_effect=stream):
                migrate.cli_result(latch, 'inspect', '--verify-pristine')
            self.assertIn('--legacy-manifest-sha256', commands[0])
            self.assertEqual(commands[0][commands[0].index('--legacy-manifest-sha256') + 1],
                             'c' * 64)
            self.assertIn('--verify-pristine', commands[0])

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

"""Local-only cold-recovery upgrade: recovery, ancestor retention and attestation."""
import copy
import importlib.util
import json
from pathlib import Path
import signal
import subprocess
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


q = load('cold_recovery_ops', 'scripts/deploy_cold_recovery_20260914.py')
fixture = load('cold_recovery_transaction_fixture',
               'scripts/tests/test_deploy_history_sharing_cpu_20260913.py')


def predecessor_adapter():
    with patch.object(q, 'REPO', ROOT):
        adapter = q.previous_adapter()
    adapter.REPO = ROOT
    return adapter


def predecessor_gc_adapter(adapter=None):
    if adapter is None:
        adapter = predecessor_adapter()
    reader_adapter = adapter.previous_adapter()
    reader_adapter.REPO = ROOT
    large_history_adapter = reader_adapter.previous_adapter()
    large_history_adapter.REPO = ROOT
    cold_metadata_adapter = large_history_adapter.previous_adapter()
    cold_metadata_adapter.REPO = ROOT
    companion_adapter = cold_metadata_adapter.previous_adapter()
    companion_adapter.REPO = ROOT
    gc_adapter = companion_adapter.previous_adapter()
    gc_adapter.REPO = ROOT
    return gc_adapter


def candidate_engine():
    # Execute the same pinned raw transaction engine as production, not a
    # configured predecessor subclass with the candidate's globals rebound.
    adapter = predecessor_adapter()
    engine = predecessor_gc_adapter(adapter).previous_module('/tmp/test-cold-recovery-engine.py')
    with patch.object(q, 'SOURCE_REVISION', 'c' * 40), \
            patch.object(q, 'ALLOWED_FILES', ('candidate.go',)), \
            patch.object(q, 'APPROVED', True):
        return q.configure(engine)


class TransactionTests(fixture.TransactionTests):
    def setUp(self):
        self.engine = candidate_engine()
        binding = patch.object(fixture, 'm', self.engine)
        binding.start()
        self.addCleanup(binding.stop)
        super().setUp()
        self.assertEqual(self.engine.CURRENT_SOURCE, q.CURRENT_SOURCE)
        self.assertEqual((self.engine.CURRENT_PID, self.engine.CURRENT_TICKS),
                         (q.CURRENT_PID, q.CURRENT_TICKS))
        self.ancestor_files = []
        for revision in ('D656', '8058', 'af989', '8a0681d3', '64ed654f', 'cda09766', '833e4a0b'):
            path = self.root / (revision + '-reader-required.json')
            value = json.dumps({'permanent': revision + ' v3 reader'}).encode() + b'\n'
            path.write_bytes(value)
            self.ancestor_files.append((path, value))
        self.b.ancestor_markers = tuple(fixture.item(path, value)
                                        for path, value in self.ancestor_files)
        self.b.queue_adapter = predecessor_gc_adapter()
        self.original.GC_METRICS = tuple('state/history/shared/gc/' + key for key in
            ('enabled', 'scanned_meta', 'candidates', 'retired', 'nonempty',
             'deferred_coverage', 'deferred_busy', 'errors'))
        self.queue_invalid = {}
        self.recovery_invalid = None
        self.metric_reads = 0
        self.metrics_process = 123456789
        self.health_process = {'pid': 20000, 'start_ticks': 12345}
        self.h.wait_healthy = lambda *args, **kwargs: {
            'process': copy.deepcopy(self.health_process),
            'observer': {'process_start_unix_nano': self.metrics_process}}
        self.h.preflight = lambda **kwargs: {'process': copy.deepcopy(self.health_process)}
        metric_patch = patch.object(self.b.g, 'read_local_metrics', side_effect=self.read_metrics)
        metric_patch.start()
        self.addCleanup(metric_patch.stop)
        self.bind_real_health()

    def bind_real_health(self):
        self.d.healthy_mode = types.MethodType(self.engine.Deployment.healthy_mode, self.d)

    def read_metrics(self):
        self.metric_reads += 1
        metrics = {'process/start/unix_nano': {'value': self.metrics_process}}
        for key in self.original.GC_METRICS:
            metrics[key] = {'value': int(key.endswith('/enabled'))}
        for key in fixture.g.SMALL_METRICS:
            metrics[key] = {'count': 0}
        for key in ('attempts', 'selected', 'new_chunks', 'reused_raw_bytes'):
            metrics['state/history/shared/' + key] = {'count': 0}
        for key in self.b.queue_adapter.QUEUE_METRICS:
            metrics['state/history/shared/gc/' + key] = {'value': 0}
        if self.d.mode() == 'candidate':
            for key, field in q.RECOVERY_METRICS:
                metrics['state/snapshot/cold/history/recovery_observation/' + key] = {field: 0}
            if self.recovery_invalid is not None:
                key, item = self.recovery_invalid
                path = 'state/snapshot/cold/history/recovery_observation/' + key
                if item is None:
                    del metrics[path]
                else:
                    metrics[path] = item
        bad = self.queue_invalid.get(self.d.mode())
        if bad:
            key, value = bad
            name = 'state/history/shared/gc/' + key
            if value is None:
                del metrics[name]
            else:
                metrics[name]['value'] = value
        return metrics

    def assert_permanent_markers(self):
        for path, value in self.ancestor_files:
            self.assertEqual(path.read_bytes(), value)
        self.assertEqual(self.old_armed.read_bytes(), self.old_armed_bytes)
        self.assertTrue((self.release / 'reader-required.json').exists())

    test_every_file_replace_and_reload_failure_restores_d656_on = None

    def test_every_file_replace_and_reload_failure_restores_c329bf12_on(self):
        fixture.TransactionTests.test_every_file_replace_and_reload_failure_restores_d656_on(self)
        self.assert_permanent_markers()
        self.assertEqual(json.loads(self.data_marker.read_bytes())['source_commit'], q.CURRENT_SOURCE)

    def test_upgrade_and_manual_on_off_rollback_preserve_all_eight_ancestors(self):
        self.assertTrue(fixture.g.activate_transaction(self.d)['active'])
        self.assert_permanent_markers()
        self.args.mode = 'rollback'
        for writer in ('off', 'on'):
            with self.subTest(writer=writer):
                self.args.rollback_writer = writer
                fixture.g.protected_rollback(self.d)
                self.assertEqual(self.d.mode(), 'old_' + writer)
                self.assertEqual(self.props['ActiveState'], 'active')
                self.assertEqual(self.running_exe, str(self.old_exe))
                self.assertEqual(json.loads(self.data_marker.read_bytes())['source_commit'], q.CURRENT_SOURCE)
                self.assert_permanent_markers()

    def test_activation_and_both_recovery_modes_preserve_resource_configuration(self):
        baseline = self.before['main']['properties']
        for mode in ('candidate', 'old_on', 'old_off'):
            with self.subTest(mode=mode):
                properties = self.d.configs[mode]['main']['properties']
                for name in ('Environment', 'MemoryLimit', 'WorkingDirectory', 'User'):
                    self.assertEqual(properties[name], baseline[name])
                # The original command's full flag suffix survives, except
                # for the explicitly requested shared-history writer mode.
                old = fixture.s.parse_commands(baseline['ExecStart'])[0]['argv[]'].split()
                new = fixture.s.parse_commands(properties['ExecStart'])[0]['argv[]'].split()
                self.assertEqual(new[2:], old[2:])
        self.assertTrue(fixture.g.activate_transaction(self.d)['active'])
        self.args.mode = 'rollback'
        for mode in ('off', 'on'):
            self.args.rollback_writer = mode
            fixture.g.protected_rollback(self.d)
            for name in ('Environment', 'MemoryLimit', 'WorkingDirectory', 'User'):
                self.assertEqual(self.props[name], baseline[name])
        self.assert_permanent_markers()

    def test_partial_transaction_reconstruction_can_restore_c329bf12(self):
        # Reconstruct the transaction from immutable evidence after each
        # replace/reload boundary; loading must not demand the old live state.
        for failure in (str(self.release / 'reader-required.json'), str(self.data_marker),
                        str(self.unit), str(self.dropin), 'reload'):
            with self.subTest(failure=failure):
                self.failure, self.failed = failure, False
                self.d.stop()
                with self.assertRaises(OSError):
                    self.d.install()
                self.d = self.engine.Deployment(self.b, self.args)
                self.bind_real_health()
                fixture.g.protected_rollback(self.d)
                self.assertEqual(self.d.mode(), 'old_on')
                self.assertEqual(self.running_exe, str(self.old_exe))
                self.assert_permanent_markers()

    def test_each_missing_candidate_queue_metric_restores_valid_c329bf12(self):
        for key in self.b.queue_adapter.QUEUE_METRICS:
            with self.subTest(key=key):
                self.queue_invalid = {'candidate': (key, None)}
                result = fixture.g.activate_transaction(self.d)
                self.assertEqual(result['phase'], 'rolled-back', result)
                self.assertIn('GC metric', result['error'])
                self.assertEqual(self.d.mode(), 'old_on')
                self.assertEqual(self.props['ActiveState'], 'active')
                self.assert_permanent_markers()

    def test_recovery_target_on_and_off_require_all_six_queue_metrics(self):
        self.assertTrue(fixture.g.activate_transaction(self.d)['active'])
        self.args.mode = 'rollback'
        for writer in ('on', 'off'):
            self.args.rollback_writer = writer
            for key in self.b.queue_adapter.QUEUE_METRICS:
                for bad in (None, True, 1.0, -1, '1'):
                    with self.subTest(writer=writer, key=key, bad=bad):
                        self.queue_invalid = {'old_' + writer: (key, bad)}
                        with self.assertRaisesRegex(RuntimeError, 'GC metric'):
                            fixture.g.protected_rollback(self.d)
                        self.assertEqual(self.d.mode(), 'old_' + writer)
                        self.assertEqual(self.running_exe, str(self.old_exe))
                        self.assert_permanent_markers()

    def test_invalid_queue_metrics_on_candidate_and_recovery_report_recovery_required(self):
        key = self.b.queue_adapter.QUEUE_METRICS[0]
        self.queue_invalid = {'candidate': (key, None), 'old_on': (key, None)}
        result = fixture.g.activate_transaction(self.d)
        self.assertEqual(result['phase'], 'recovery-required', result)
        self.assertIn('GC metric', result['rollback_error'])
        self.assertEqual(self.d.mode(), 'old_on')
        self.assert_permanent_markers()

    def test_candidate_recovery_metric_schema_and_zeroes_old_modes_exempt(self):
        self.assertTrue(fixture.g.activate_transaction(self.d)['active'])
        self.args.mode = 'rollback'
        for writer in ('off', 'on'):
            self.args.rollback_writer = writer
            fixture.g.protected_rollback(self.d)
            self.assertEqual(self.d.mode(), 'old_' + writer)
        self.args.mode = 'activate'
        for key, field in q.RECOVERY_METRICS:
            for item in (None, {}, {'wrong_field': 0}, {field: True}, {field: -1},
                         {field: float('nan')}, {field: float('inf')}, {field: '1'}):
                with self.subTest(key=key, item=item):
                    self.recovery_invalid = (key, item)
                    result = fixture.g.activate_transaction(self.d)
                    self.assertEqual(result['phase'], 'rolled-back', result)
                    self.assertIn('invalid recovery metric', result['error'])
                    self.assertEqual(self.d.mode(), 'old_on')
                    self.assert_permanent_markers()

    def test_queue_metrics_from_another_process_rejected(self):
        baseline = self.read_metrics
        def changed():
            metrics = baseline()
            if self.metric_reads == 2:
                metrics['process/start/unix_nano']['value'] += 1
            return metrics
        with patch.object(self.b.g, 'read_local_metrics', side_effect=changed):
            result = fixture.g.activate_transaction(self.d)
        self.assertEqual(result['phase'], 'rolled-back', result)
        self.assertIn('GC metric process changed', result['error'])
        self.assert_permanent_markers()

    def test_each_ancestor_marker_change_rejected_before_stop(self):
        for path, value in self.ancestor_files:
            with self.subTest(path=path):
                path.write_bytes(b'altered ancestor')
                with self.assertRaisesRegex(RuntimeError, 'ancestor permanent'):
                    self.d.stop()
                path.write_bytes(value)
        self.assertNotIn(('stop',), self.calls)

    def test_all_operator_signals_restore_handlers_and_c329bf12(self):
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            with self.subTest(signal=sig):
                self.failure, self.failed = str(self.data_marker), False
                self.failure_error = InterruptedError('operator signal ' + str(sig))
                handlers = {number: signal.getsignal(number) for number in
                            (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
                result = fixture.g.activate_transaction(self.d)
                self.assertEqual(result['phase'], 'rolled-back', result)
                self.assertEqual(self.d.mode(), 'old_on')
                self.assert_permanent_markers()
                self.assertEqual({number: signal.getsignal(number) for number in handlers}, handlers)


class ModuleTests(unittest.TestCase):
    def make_dependencies(self):
        old_h = types.SimpleNamespace(RELEASE=Path('/c329bf12'), BINARY=Path('/c329bf12/gtron'))
        old_i = types.SimpleNamespace(CURRENT_SOURCE='cda09766', RELEASE=Path('/c329bf12/source'))
        markerCda = {'path': '/cda09766/reader-required.json', 'sha256': '0' * 64}
        marker64ed = {'path': '/64ed654f/reader-required.json', 'sha256': '6' * 64}
        marker8a = {'path': '/8a0681d3/reader-required.json', 'sha256': 'c' * 64}
        markerAf989 = {'path': '/af989/reader-required.json', 'sha256': 'a' * 64}
        marker8058 = {'path': '/8058/reader-required.json', 'sha256': '8' * 64}
        markerD656 = {'path': '/d656/reader-required.json', 'sha256': 'd' * 64}
        marker833 = {'path': '/833e4a0b/reader-required.json', 'sha256': '3' * 64}
        record = {'binary_sha256': 'b' * 64, 'old_armed': marker833}
        self.checks = []
        predecessor = types.SimpleNamespace(record=record,
            check=lambda mode: self.checks.append(mode))
        original = types.SimpleNamespace(MARKER=Path('/datadir/reader-required.json'),
            DROPIN=Path('/etc/systemd/zz-reader.conf'), GUARD=Path('/guard'), GC_METRICS=('gc',))
        old_bundle = types.SimpleNamespace(original=original, i=old_i, h=old_h,
            guard=fixture.s, g=fixture.g, ancestor_markers=(markerCda, marker64ed, marker8a, markerAf989, marker8058, markerD656),
            queue_adapter=types.SimpleNamespace(validate_queue_metrics=lambda metrics: None))
        self.old_bundle = old_bundle
        self.previous_args = []
        def previous_load(args):
            self.previous_args.append(copy.deepcopy(vars(args)))
            return old_bundle
        previous = types.SimpleNamespace(BINARY=Path('/c329bf12/gtron'), RELEASE=Path('/c329bf12'),
            load=previous_load, Deployment=lambda *args: predecessor)
        self.previous_paths = []
        gc_adapter = types.SimpleNamespace(previous_module=lambda path: self.previous_paths.append(path))
        companion_adapter = types.SimpleNamespace(previous_adapter=lambda: gc_adapter)
        cold_metadata_adapter = types.SimpleNamespace(previous_adapter=lambda: companion_adapter)
        large_history_adapter = types.SimpleNamespace(previous_adapter=lambda: cold_metadata_adapter)
        reader_adapter = types.SimpleNamespace(previous_adapter=lambda: large_history_adapter)
        adapter = types.SimpleNamespace(configure=lambda engine: previous,
            previous_adapter=lambda: reader_adapter)
        self.old_state = (copy.deepcopy(vars(old_h)), copy.deepcopy(vars(old_i)), copy.deepcopy(record))
        self.old_h, self.old_i, self.old_record = old_h, old_i, record
        new_h = types.SimpleNamespace(RELEASE=Path('/original-helper'), BINARY=Path('/original-helper/gtron'))
        builder = types.SimpleNamespace(load_helper=lambda: new_h)
        wrapper = types.SimpleNamespace(install_show=lambda *args: None)
        return previous, adapter, builder, wrapper

    def test_load_immutable_predecessor_does_not_compare_live_partial_transaction(self):
        engine = candidate_engine()
        previous, adapter, builder, wrapper = self.make_dependencies()
        with patch.object(q, 'APPROVED', True), patch.object(q, 'ALLOWED_FILES', ('candidate.go',)), \
                patch.object(q, 'previous_adapter', return_value=adapter) as loader, \
                patch.object(engine, 'pinned_module', side_effect=[builder, wrapper]):
            bundle = engine.load(types.SimpleNamespace(old_prepared_sha=q.CURRENT_PREPARED_SHA))
        loader.assert_called_once_with()
        self.assertEqual(self.previous_paths, [q.PREVIOUS_NATIVE_SCRIPT])
        self.assertEqual(self.checks, [])
        self.assertEqual(self.previous_args, [{'mode': 'activate', 'source_revision': q.CURRENT_SOURCE,
            'script_revision': q.PREVIOUS_REVISION, 'old_prepared_sha': q.PREDECESSOR_PARENT_SHA,
            'prepared_sha': q.CURRENT_PREPARED_SHA, 'rollback_writer': 'on'}])
        self.assertEqual((vars(self.old_h), vars(self.old_i), self.old_record), self.old_state)
        self.assertIsNot(bundle.h, self.old_h)
        self.assertIsNot(bundle.i, self.old_i)
        self.assertEqual(builder.CURRENT_SOURCE, q.CURRENT_SOURCE)
        self.assertEqual(builder.CURRENT_EXE, str(previous.BINARY))
        self.assertEqual(builder.CURRENT_SHA, self.old_record['binary_sha256'])
        self.assertEqual((builder.CURRENT_PID, builder.CURRENT_TICKS), (q.CURRENT_PID, q.CURRENT_TICKS))
        self.assertEqual(builder.SCRIPT_PATH, q.SCRIPT_PATH)
        self.assertEqual(builder.ALLOWED_FILES, ('candidate.go',))
        self.assertEqual(builder.NATIVE_TEST_PATTERN,
            'Test.*(History|Shared|StateChange|StateDomainChangePrune|AsOf|Unwind|ResetMutableState|Snapshot.*Coverage|Manifest|Companion|Trusted|Delegation|Delegated|DrAccountIndex|Commitment|Prefetch|BaseReadCache|EventFrontier)')
        self.assertIs(bundle.ancestor_markers[0], self.old_record['old_armed'])
        self.assertEqual([item['path'] for item in bundle.ancestor_markers],
            ['/{}/reader-required.json'.format(name) for name in
             ('833e4a0b', 'cda09766', '64ed654f', '8a0681d3', 'af989', '8058', 'd656')])
        self.assertIs(bundle.queue_adapter, self.old_bundle.queue_adapter)
        self.assertEqual(bundle.original.ARMED, previous.RELEASE / 'reader-required.json')
        for exe in (str(previous.BINARY), str(engine.BINARY)):
            self.assertEqual(bundle.h.expected_history_queue_environment(exe), '1')
            self.assertEqual(bundle.h.expected_history_range_environment(exe), '1')
        for unapproved in ('/b130/gtron', '/d656/gtron', '/8058/gtron', '/af989/gtron', '/8a0681d3/gtron', '/64ed654f/gtron', '/cda09766/gtron', '/833e4a0b/gtron'):
            with self.assertRaisesRegex(RuntimeError, 'unapproved compatible reader'):
                bundle.h.expected_history_queue_environment(unapproved)
        bundle.old.compare('on')
        self.assertEqual(self.checks, ['candidate'])
        with self.assertRaises(RuntimeError):
            bundle.old.compare('off')

    def test_wrong_predecessor_prepared_sha_rejected_before_loading(self):
        engine = candidate_engine()
        with patch.object(q, 'previous_adapter') as loader:
            for bad in ('', q.PREDECESSOR_PARENT_SHA, '0' * 64):
                with self.subTest(sha=bad), self.assertRaisesRegex(RuntimeError, 'c329bf12 predecessor'):
                    engine.load(types.SimpleNamespace(old_prepared_sha=bad))
            loader.assert_not_called()

    def test_pending_review_refuses_before_dependency_execution(self):
        for approved, source, paths in ((False, 'c' * 40, ('candidate.go',)),
                                         (True, '', ('candidate.go',)),
                                         (True, 'c' * 40, ())):
            with self.subTest(approved=approved, source=source, paths=paths), \
                    patch.object(q, 'APPROVED', approved), patch.object(q, 'SOURCE_REVISION', source), \
                    patch.object(q, 'ALLOWED_FILES', paths):
                with self.assertRaisesRegex(RuntimeError, 'not frozen'):
                    q.configure(types.SimpleNamespace())
        with patch.object(q, 'APPROVED', False), patch.object(q, 'previous_adapter') as loader:
            with self.assertRaisesRegex(RuntimeError, 'not frozen'):
                q.main()
            loader.assert_not_called()

    def test_predecessor_blob_sha_checked_before_execution(self):
        with patch.object(q.subprocess, 'check_output', return_value=b'raise AssertionError("executed")'):
            with self.assertRaisesRegex(RuntimeError, 'predecessor adapter changed'):
                q.previous_adapter()

    def test_pinned_8f76ff88_adapter_and_raw_engines_are_independent(self):
        old_adapter = predecessor_adapter()
        candidate = candidate_engine()
        old_raw = predecessor_gc_adapter(old_adapter).previous_module(q.PREVIOUS_NATIVE_SCRIPT)
        old = old_adapter.configure(old_raw)
        self.assertIsNot(old, candidate)
        self.assertIsNot(old.Deployment, candidate.Deployment)
        self.assertEqual(old_adapter.__file__, q.PREVIOUS_NATIVE_SCRIPT)
        self.assertEqual(old_adapter.SOURCE_REVISION, q.CURRENT_SOURCE)
        self.assertEqual(old_adapter.CURRENT_PREPARED_SHA, q.PREDECESSOR_PARENT_SHA)
        self.assertEqual(old.SOURCE_REVISION, q.CURRENT_SOURCE)
        self.assertEqual(old.CURRENT_SOURCE, '833e4a0b02f3d490386b74a8114006658f035b80')
        self.assertEqual(old.RELEASE, Path('/data/gtron/releases/20260914-commitment-prefetch'))
        self.assertEqual(candidate.CURRENT_SOURCE, q.CURRENT_SOURCE)
        self.assertEqual(candidate.SOURCE_REVISION, 'c' * 40)
        self.assertEqual(candidate.Deployment.__mro__[1].__name__, 'Deployment')
        self.assertEqual(len(candidate.Deployment.__mro__), 3)

    def test_main_resolves_raw_engine_through_all_six_pinned_adapters(self):
        raw = types.SimpleNamespace()
        calls = []
        gc_adapter = types.SimpleNamespace(previous_module=lambda path: calls.append(path) or raw)
        companion = types.SimpleNamespace(previous_adapter=lambda: gc_adapter)
        cold_metadata = types.SimpleNamespace(previous_adapter=lambda: companion)
        large_history = types.SimpleNamespace(previous_adapter=lambda: cold_metadata)
        history_reader = types.SimpleNamespace(previous_adapter=lambda: large_history)
        commitment_prefetch = types.SimpleNamespace(previous_adapter=lambda: history_reader)
        configured = types.SimpleNamespace(main=lambda: 'dispatch reached')
        with patch.object(q, 'APPROVED', True), \
                patch.object(q, 'previous_adapter', return_value=commitment_prefetch) as load_adapter, \
                patch.object(q, 'configure', return_value=configured) as configure:
            self.assertEqual(q.main(), 'dispatch reached')
        load_adapter.assert_called_once_with()
        configure.assert_called_once_with(raw)
        self.assertEqual(calls, [q.__file__])

    def test_frozen_scope_matches_commit_when_available(self):
        if not q.APPROVED:
            self.assertEqual(q.SOURCE_REVISION, '')
            self.assertEqual(q.ALLOWED_FILES, ())
            return
        changed = subprocess.check_output(['git', 'diff', '--name-only', q.CURRENT_SOURCE,
                                           q.SOURCE_REVISION, '--'], cwd=ROOT).decode()
        actual = {path for path in changed.splitlines()
                  if not (path.startswith('docs/') and path.endswith('.md'))}
        self.assertEqual(actual, set(q.ALLOWED_FILES))
        self.assertEqual(len(q.ALLOWED_FILES), len(actual))
        self.assertNotIn(q.SCRIPT_PATH, actual)
        self.assertNotIn('scripts/tests/test_deploy_cold_recovery_20260914.py', actual)

    def test_candidate_source_and_prepared_sha_identity_reject(self):
        engine = candidate_engine()
        bundle = types.SimpleNamespace(i=types.SimpleNamespace(), h=types.SimpleNamespace())
        with self.assertRaisesRegex(RuntimeError, 'source revision'):
            engine.prepare(bundle, types.SimpleNamespace(source_revision='wrong'))
        record = {'upgrade_prepared': True, 'old_prepared_sha256': q.CURRENT_PREPARED_SHA,
                  'source_commit': 'c' * 40, 'script_commit': 'e' * 40}
        bundle.h.load_json = lambda name: record
        bundle.h.file_sha = lambda path: 'a' * 64
        args = types.SimpleNamespace(mode='activate', prepared_sha='b' * 64,
            source_revision='c' * 40, script_revision='e' * 40, old_prepared_sha=q.CURRENT_PREPARED_SHA)
        with self.assertRaisesRegex(RuntimeError, 'prepared record changed'):
            engine.verify_prepared(bundle, args)
        args.prepared_sha = 'a' * 64
        record['source_commit'] = 'd' * 40
        with self.assertRaisesRegex(RuntimeError, 'attestation identity differs'):
            engine.verify_prepared(bundle, args)


if __name__ == '__main__':
    unittest.main(verbosity=2)

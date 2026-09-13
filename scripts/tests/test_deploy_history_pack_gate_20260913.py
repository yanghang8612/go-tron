"""Local-only deployment/configuration/rollback audit; no service or DB access."""
import base64
import copy
import hashlib
import importlib.util
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('gate_ops', ROOT / 'scripts/deploy_history_pack_gate_20260913.py')
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)


class FakeDeployment:
    def __init__(self, failure=None):
        self.failure = failure or {}
        self.calls, self.records = [], []

    def step(self, name):
        self.calls.append(name)
        if name in self.failure:
            raise self.failure[name]

    def admit(self):
        self.step('admit')

    def save(self, record):
        self.records.append(copy.deepcopy(record))

    def stop(self):
        self.step('stop')

    def install(self):
        self.step('install')

    def start_candidate(self):
        self.step('start')

    def healthy(self, candidate):
        self.step('healthy')
        return {'candidate': candidate}

    def rollback(self):
        self.step('rollback')
        return {'original_healthy': True}


class TransactionTests(unittest.TestCase):
    def test_prepare_lock_covers_builder_and_extra_test_phase(self):
        i = types.SimpleNamespace(validate_revision=lambda *args: None)
        args = types.SimpleNamespace(revision='0' * 40, script_revision='1' * 40)
        with tempfile.TemporaryDirectory() as directory, patch.object(m, 'RELEASE', Path(directory)):
            def phases(*unused):
                with open(str(m.RELEASE / '.gate-prepare.lock'), 'a') as other:
                    with self.assertRaises(BlockingIOError):
                        m.fcntl.flock(other, m.fcntl.LOCK_EX | m.fcntl.LOCK_NB)
                return {'prepared': True}
            with patch.object(m, 'prepare_locked', side_effect=phases):
                self.assertTrue(m.prepare(i, None, args)['prepared'])
            with open(str(m.RELEASE / '.gate-prepare.lock'), 'a') as other:
                m.fcntl.flock(other, m.fcntl.LOCK_EX | m.fcntl.LOCK_NB)

    def test_success_stops_before_edit_and_start(self):
        ops = FakeDeployment()
        result = m.activate_transaction(ops)
        self.assertTrue(result['active'])
        self.assertEqual(ops.calls, ['admit', 'stop', 'install', 'start', 'healthy'])

    def test_admission_failure_never_stops(self):
        ops = FakeDeployment({'admit': RuntimeError('identity changed')})
        with self.assertRaises(RuntimeError):
            m.activate_transaction(ops)
        self.assertEqual(ops.calls, ['admit'])

    def test_interrupted_stop_still_restores(self):
        ops = FakeDeployment({'stop': InterruptedError('operator signal')})
        result = m.activate_transaction(ops)
        self.assertEqual(result['phase'], 'rolled-back')
        self.assertEqual(ops.calls, ['admit', 'stop', 'rollback'])

    def test_replace_fsync_failure_still_restores(self):
        ops = FakeDeployment({'install': OSError('replace succeeded, directory fsync failed')})
        result = m.activate_transaction(ops)
        self.assertEqual(result['phase'], 'rolled-back')
        self.assertNotIn('start', ops.calls)

    def test_start_failure_restores(self):
        ops = FakeDeployment({'start': RuntimeError('start failed')})
        self.assertEqual(m.activate_transaction(ops)['phase'], 'rolled-back')

    def test_health_failure_restores(self):
        ops = FakeDeployment({'healthy': RuntimeError('wrong metrics')})
        self.assertEqual(m.activate_transaction(ops)['phase'], 'rolled-back')

    def test_keyboard_interrupt_restores(self):
        ops = FakeDeployment({'healthy': KeyboardInterrupt()})
        self.assertEqual(m.activate_transaction(ops)['phase'], 'rolled-back')

    def test_real_latch_or_unrecovered_node_is_reported(self):
        ops = FakeDeployment({'healthy': RuntimeError('not healthy'),
                              'rollback': RuntimeError('real disk latch; original unit restored, start refused')})
        result = m.activate_transaction(ops)
        self.assertFalse(result['active'])
        self.assertEqual(result['phase'], 'recovery-required')
        self.assertIn('latch', result['rollback_error'])

    def test_explicit_rollback_ignores_signals_until_recovery_finishes(self):
        signals = (m.signal.SIGINT, m.signal.SIGTERM, m.signal.SIGHUP)
        old = {sig: m.signal.getsignal(sig) for sig in signals}
        ops = FakeDeployment()
        original = ops.rollback
        def rollback():
            self.assertTrue(all(m.signal.getsignal(sig) == m.signal.SIG_IGN for sig in signals))
            return original()
        ops.rollback = rollback
        self.assertTrue(m.protected_rollback(ops)['original_healthy'])
        self.assertEqual({sig: m.signal.getsignal(sig) for sig in signals}, old)


class ConfigurationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch.object(m, 'REPO', ROOT):
            cls.i, cls.h = m.load_modules(ROOT / 'scripts/deploy_range_scheduling_20260913.py')

    def setUp(self):
        self.command = m.CURRENT_EXE + ' --datadir /data/gtron/main/datadir'
        unit = ('[Service]\nExecStart=\nExecStart=' + self.command +
                '\nEnvironment=GTRON_HISTORY_RANGE_QUEUE=1\n').encode()
        self.item = {'path': '/etc/systemd/system/gtron.service.d/override.conf',
                     'data_b64': base64.b64encode(unit).decode(), 'sha256': hashlib.sha256(unit).hexdigest(),
                     'uid': 0, 'gid': 0, 'mode': 0o644, 'xattrs': {}}
        value = ('{ path=' + m.CURRENT_EXE + ' ; argv[]=' + self.command +
                 ' ; ignore_errors=no ; start_time=[Sun 2026-09-13 10:30:00 UTC] ; '
                 'stop_time=[n/a] ; pid=14862 ; code=(null) ; status=0/0 }')
        self.before = {'main': {'files': [self.item], 'properties': {
            'ExecStart': value, 'ExecStartPre': '', 'Environment': 'FIVE=1',
            'WorkingDirectory': '/data/gtron/main', 'User': 'java-tron', 'MemoryLimit': '42949672960'}},
            'others': {u: {'files': []} for u in self.h.PRESERVED_UNITS},
            'holds': {self.h.GLOBAL_HOLD: {'exists': True, 'file': {}}, self.h.MAIN_HOLD: {'exists': False}},
            'guard_config': {}, 'guard_script': {}}
        self.item, self.original, self.updated = m.effective_exec_edit(self.before)
        self.expected = m.candidate_configuration(self.i, self.before, self.item, self.updated)

    def compare(self, observed, expected=None, **options):
        h = self.h
        def unit(name):
            return observed['main'] if name == h.SERVICE else observed['others'][name]
        with patch.object(h, 'unit_snapshot', side_effect=unit), \
                patch.object(h, 'hold_snapshot', side_effect=lambda name: observed['holds'][name]), \
                patch.object(h, 'saved_file', return_value={}), patch.object(h, 'file_sha', return_value=m.CURRENT_SHA):
            return m.compare_configuration(self.i, h, expected or self.expected, **options)

    def test_both_old_and_candidate_require_all_existing_flags(self):
        for exe in (m.CURRENT_EXE, str(m.BINARY)):
            self.assertEqual(self.h.expected_history_queue_environment(exe), '1')
            self.assertEqual(self.h.expected_history_range_environment(exe), '1')
        with self.assertRaises(RuntimeError):
            self.h.expected_history_queue_environment('/unapproved/gtron')

    def test_replacement_changes_only_one_executable(self):
        self.assertEqual(self.updated.replace(str(m.BINARY).encode(), m.CURRENT_EXE.encode()), self.original)
        self.assertIn(b'Environment=GTRON_HISTORY_RANGE_QUEUE=1', self.updated)

    def test_effective_exec_must_be_unique(self):
        before = copy.deepcopy(self.before)
        before['main']['files'][0]['data_b64'] = base64.b64encode(
            self.original + ('ExecStart=' + self.command + '\n').encode()).decode()
        with self.assertRaises(RuntimeError):
            m.effective_exec_edit(before)

    def test_restart_runtime_state_is_ignored(self):
        observed = copy.deepcopy(self.expected)
        observed['main']['properties']['ExecStart'] = observed['main']['properties']['ExecStart'].replace(
            'pid=14862', 'pid=7777').replace('10:30:00 UTC', '11:00:00 UTC')
        self.compare(observed)

    def test_abnormal_exit_label_is_metadata_but_unknown_fields_fail(self):
        observed = copy.deepcopy(self.expected)
        value = observed['main']['properties']['ExecStart'].replace('code=(null)', 'code=killed').replace('status=0/0', 'status=11/SEGV')
        observed['main']['properties']['ExecStart'] = value
        self.compare(observed)
        observed['main']['properties']['ExecStart'] = value.replace(' ; status=', ' ; unknown=1 ; status=')
        with self.assertRaises(RuntimeError):
            self.compare(observed)

    def test_other_config_changes_are_rejected(self):
        for key in ('Environment', 'User', 'WorkingDirectory', 'MemoryLimit'):
            observed = copy.deepcopy(self.expected)
            observed['main']['properties'][key] = 'changed'
            with self.assertRaises(RuntimeError):
                self.compare(observed)

    def test_file_replaced_before_reload_can_be_safely_rolled_back(self):
        observed = copy.deepcopy(self.expected)
        observed['main']['properties']['ExecStart'] = self.before['main']['properties']['ExecStart']
        with self.assertRaises(RuntimeError):
            self.compare(observed)
        self.compare(observed, pending_exec_values=(self.before['main']['properties']['ExecStart'],
                                                    self.expected['main']['properties']['ExecStart']))

    def test_pending_reload_never_accepts_unapproved_argv(self):
        observed = copy.deepcopy(self.expected)
        observed['main']['properties']['ExecStart'] = observed['main']['properties']['ExecStart'].replace(
            '/data/gtron/main/datadir', '/data/other/datadir')
        with self.assertRaises(RuntimeError):
            self.compare(observed, pending_exec_values=(self.before['main']['properties']['ExecStart'],
                                                        self.expected['main']['properties']['ExecStart']))

    def test_new_main_latch_can_preserve_original_config_recovery(self):
        observed = copy.deepcopy(self.expected)
        observed['holds'][self.h.MAIN_HOLD] = {'exists': True, 'file': {'reason': 'disk protection'}}
        with self.assertRaises(RuntimeError):
            self.compare(observed)
        self.compare(observed, allow_new_latch=True)
        self.assertTrue(observed['holds'][self.h.MAIN_HOLD]['exists'])

    def test_global_hold_change_is_never_ignored(self):
        observed = copy.deepcopy(self.expected)
        observed['holds'][self.h.GLOBAL_HOLD] = {'exists': False}
        with self.assertRaises(RuntimeError):
            self.compare(observed, allow_new_latch=True)

    def test_candidate_metrics_allow_registered_zero_activity(self):
        metrics = {name: {'count': 0} for name in m.SMALL_METRICS}
        self.assertEqual(m.small_metric_values(metrics), {name: 0 for name in m.SMALL_METRICS})

    def test_candidate_metrics_reject_missing_negative_or_noninteger(self):
        for invalid in (None, -1, 1.5, True):
            metrics = {name: {'count': 0} for name in m.SMALL_METRICS}
            if invalid is None:
                del metrics[m.SMALL_METRICS[0]]
            else:
                metrics[m.SMALL_METRICS[0]]['count'] = invalid
            with self.assertRaises(RuntimeError):
                m.small_metric_values(metrics)


if __name__ == '__main__':
    unittest.main(verbosity=2)

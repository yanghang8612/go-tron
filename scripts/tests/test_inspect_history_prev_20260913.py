"""Offline state-machine audit. No systemctl, DB or server access."""
import argparse
import copy
import importlib.util
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('inspection_ops', ROOT / 'scripts/inspect_history_prev_20260913.py')
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)


class FakeOps:
    def __init__(self, fail=None, incomplete=False):
        self.fail = fail or {}
        self.incomplete = incomplete
        self.calls = []
        self.records = []

    def step(self, name):
        self.calls.append(name)
        if name in self.fail:
            raise self.fail[name]

    def before(self):
        self.step('before')
        return {'process': {'argv': [m.CURRENT_EXE]}}

    def save(self, record):
        self.records.append(copy.deepcopy(record))

    def stop(self):
        self.step('stop')

    def assert_stopped(self):
        self.step('assert_stopped')

    def probe(self):
        self.step('probe')
        return {'ok': not self.incomplete, 'complete': not self.incomplete}

    def cancel_probe(self):
        self.step('cancel_probe')

    def restore(self, before):
        self.step('restore')
        return {'process': {'pid': 9876, 'start_ticks': 6543}}


class StateMachineTests(unittest.TestCase):
    def test_success_restores_after_read(self):
        ops = FakeOps()
        out = m.inspect_transaction(ops)
        self.assertTrue(out['ok'])
        self.assertTrue(out['restored'])
        self.assertEqual(ops.calls, ['before', 'stop', 'assert_stopped', 'probe', 'cancel_probe', 'restore'])

    def test_preflight_failure_never_stops(self):
        ops = FakeOps({'before': RuntimeError('identity changed')})
        with self.assertRaisesRegex(RuntimeError, 'identity'):
            m.inspect_transaction(ops)
        self.assertEqual(ops.calls, ['before'])

    def test_failed_stop_may_have_queued_job_so_recovers(self):
        ops = FakeOps({'stop': subprocess.TimeoutExpired('systemctl stop', 660)})
        out = m.inspect_transaction(ops)
        self.assertFalse(out['ok'])
        self.assertTrue(out['restored'])
        self.assertNotIn('probe', ops.calls)
        self.assertEqual(ops.calls[-1], 'restore')

    def test_unsettled_stop_never_opens_database(self):
        ops = FakeOps({'assert_stopped': RuntimeError('deactivating')})
        out = m.inspect_transaction(ops)
        self.assertNotIn('probe', ops.calls)
        self.assertTrue(out['restored'])

    def test_incomplete_budget_keeps_failure_but_restores(self):
        ops = FakeOps(incomplete=True)
        out = m.inspect_transaction(ops)
        self.assertFalse(out['ok'])
        self.assertTrue(out['restored'])
        self.assertIn('incomplete', out['error'])

    def test_probe_timeout_always_cleans_before_restore(self):
        ops = FakeOps({'probe': subprocess.TimeoutExpired('read-only probe', 180)})
        out = m.inspect_transaction(ops)
        self.assertTrue(out['restored'])
        self.assertEqual(ops.calls[-2:], ['cancel_probe', 'restore'])

    def test_operator_interrupt_restores(self):
        for exception in (KeyboardInterrupt(), InterruptedError('SIGTERM')):
            ops = FakeOps({'probe': exception})
            out = m.inspect_transaction(ops)
            self.assertFalse(out['ok'])
            self.assertTrue(out['restored'])

    def test_new_disk_latch_refuses_restart_without_clearing(self):
        ops = FakeOps({'restore': RuntimeError('mainnet stop hold exists')})
        out = m.inspect_transaction(ops)
        self.assertFalse(out['ok'])
        self.assertFalse(out['restored'])
        self.assertEqual(out['phase'], 'recovery-required')
        self.assertIn('hold exists', out['recovery_error'])

    def test_unreaped_probe_never_starts_concurrent_node(self):
        ops = FakeOps({'cancel_probe': RuntimeError('child still alive')})
        out = m.inspect_transaction(ops)
        self.assertFalse(out['restored'])
        self.assertNotIn('restore', ops.calls)

    def test_journal_error_after_stop_still_restores(self):
        ops = FakeOps()
        original_save = ops.save
        def save(record):
            if record['phase'] == 'inspecting':
                raise OSError('journal disk error')
            original_save(record)
        ops.save = save
        out = m.inspect_transaction(ops)
        self.assertTrue(out['restored'])
        self.assertNotIn('probe', ops.calls)


class ContractTests(unittest.TestCase):
    def args(self, **values):
        out = {'from_block': 30031467, 'to_block': 30368000, 'export_packs': False}
        out.update(values)
        return argparse.Namespace(**out)

    def test_command_is_bounded_read_only_subcommand(self):
        command = m.probe_argv(self.args())
        self.assertEqual(command[:3], [str(m.BINARY), 'db', 'inspect-history-prev'])
        self.assertEqual(command[command.index('--samples') + 1], '128')
        self.assertEqual(command[command.index('--max-duration') + 1], '60s')
        self.assertNotEqual(str(m.BINARY), m.CURRENT_EXE)
        self.assertEqual(m.PROBE_TIMEOUT, 180)

    def test_invalid_range_rejected_before_stop(self):
        for low, high in ((-1, 2), (5, 4), (0, 1 << 64)):
            with self.assertRaises(RuntimeError):
                m.probe_argv(self.args(from_block=low, to_block=high))

    def test_export_is_explicit_and_fixed_outside_datadir(self):
        with patch.object(m, 'EXPORT_APPROVED', False):
            with self.assertRaisesRegex(RuntimeError, 'export'):
                m.probe_argv(self.args(export_packs=True))
        with patch.object(m, 'EXPORT_APPROVED', True):
            command = m.probe_argv(self.args(export_packs=True))
        self.assertEqual(command[-2:], ['--export-packs', str(m.PACKS)])
        self.assertFalse(str(m.PACKS).startswith(m.DATADIR))

    def test_unfrozen_safety_review_fails_closed(self):
        with patch.object(m, 'INSPECT_APPROVED', False):
            with self.assertRaises(RuntimeError):
                m.frozen()

    def test_zero_identity_fails_closed_even_after_scope_frozen(self):
        with patch.object(m, 'INSPECT_APPROVED', True), patch.object(m, 'ALLOWED_FILES', ('a.go',)), \
                patch.object(m, 'CURRENT_TICKS', 0):
            with self.assertRaisesRegex(RuntimeError, 'process'):
                m.frozen()

    def test_only_exact_sha_helper_can_be_loaded(self):
        path = ROOT / 'scripts/deploy_range_scheduling_20260913.py'
        with patch.object(m, 'HELPER', path):
            loaded = m.load_helper()
            self.assertEqual(str(loaded.BINARY), m.CURRENT_EXE)
        with patch.object(m, 'HELPER', path), patch.object(m, 'HELPER_SHA', '0' * 64):
            with self.assertRaisesRegex(RuntimeError, 'SHA'):
                m.load_helper()

    def test_all_current_new_inspection_test_names_match_native_regex(self):
        names = []
        for name in ('cmd/gtron/db_inspect_history_test.go', 'core/rawdb/inspect_state_history_test.go',
                     'cmd/gtron/db_history_codec_benchmark_test.go', 'core/rawdb/history_codec_benchmark_test.go'):
            names.extend(re.findall(r'^func (Test\w+)\(', (ROOT / name).read_text(), re.M))
        self.assertGreaterEqual(len(names), 10)
        self.assertEqual([name for name in names if not re.search(m.NATIVE_TEST_PATTERN, name)], [])

    def test_child_cleanup_only_targets_its_dedicated_group(self):
        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], start_new_session=True)
        ops = m.LiveOps(None, self.args())
        ops.child = child
        try:
            ops.cancel_probe()
            self.assertIsNotNone(child.poll())
            self.assertIsNone(ops.child)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)

    def test_natural_exit_racing_term_still_reaps(self):
        child = Mock(pid=9876)
        child.poll.return_value = None
        child.wait.return_value = 0
        ops = m.LiveOps(None, self.args())
        ops.child = child
        with patch.object(m.os, 'killpg', side_effect=ProcessLookupError):
            ops.cancel_probe()
        child.wait.assert_called_once_with(timeout=5)
        self.assertIsNone(ops.child)

    def test_natural_exit_racing_kill_still_reaps(self):
        child = Mock(pid=9876)
        child.poll.return_value = None
        child.wait.side_effect = [subprocess.TimeoutExpired('probe', 5), 0]
        ops = m.LiveOps(None, self.args())
        ops.child = child
        with patch.object(m.os, 'killpg', side_effect=[None, ProcessLookupError]) as kill:
            ops.cancel_probe()
        self.assertEqual(kill.call_count, 2)
        self.assertEqual(child.wait.call_args_list[-1][1], {'timeout': 10})
        self.assertIsNone(ops.child)


class ConfigurationTests(unittest.TestCase):
    def setUp(self):
        with patch.object(m, 'HELPER', ROOT / 'scripts/deploy_range_scheduling_20260913.py'):
            self.h = m.load_helper()
        self.running = ('{ path=' + m.CURRENT_EXE + ' ; argv[]=' + m.CURRENT_EXE +
                        ' --datadir /data/gtron/main/datadir ; ignore_errors=no ; '
                        'start_time=[Sun 2026-09-13 01:39:35 UTC] ; stop_time=[n/a] ; '
                        'pid=4345 ; code=(null) ; status=0/0 }')
        self.stopped = self.running.replace('stop_time=[n/a]',
                                           'stop_time=[Sun 2026-09-13 10:26:42 UTC]').replace(
                                               'code=(null) ; status=0/0', 'code=exited ; status=0')
        self.before = {'holds': {}, 'guard_config': {}, 'guard_script': {},
                       'main': {'files': [{'data_b64': 'unit bytes unchanged'}],
                                'properties': {'ExecStart': self.running, 'ExecStartPre': '', 'Environment': 'FIVE=1',
                                               'MemoryLimit': '42949672960'}},
                       'others': {name: {'files': []} for name in self.h.PRESERVED_UNITS}}

    def changed(self, value):
        after = copy.deepcopy(self.before)
        after['main']['properties']['ExecStart'] = value
        return after

    def test_observed_clean_stop_runtime_fields_are_not_configuration(self):
        m.same_configuration(self.h, self.before, self.changed(self.stopped))
        self.assertEqual(self.before['main']['properties']['ExecStart'], self.running)

    def test_restart_pid_and_times_change_but_static_command_is_equal(self):
        restarted = self.running.replace('pid=4345', 'pid=9999').replace(
            '01:39:35 UTC', '10:28:00 UTC')
        m.same_configuration(self.h, self.before, self.changed(restarted))

    def test_changed_argv_is_rejected(self):
        changed = self.stopped.replace('/data/gtron/main/datadir', '/data/other/datadir')
        with self.assertRaisesRegex(RuntimeError, 'ExecStart'):
            m.same_configuration(self.h, self.before, self.changed(changed))

    def test_changed_unit_bytes_environment_and_memory_are_still_rejected(self):
        for part in ('files', 'Environment', 'MemoryLimit'):
            after = self.changed(self.stopped)
            if part == 'files':
                after['main']['files'] = []
            else:
                after['main']['properties'][part] = 'changed'
            with self.assertRaises(RuntimeError):
                m.same_configuration(self.h, self.before, after)

    def test_unknown_missing_or_duplicate_fields_fail_closed(self):
        for value in (self.running + ' ' + self.running,
                      self.running.replace(' ; status=0/0', ''),
                      self.running.replace(' ; status=0/0', ' ; status=0/0 ; flags=unknown'),
                      self.running.replace(' ; status=0/0', ' ; status=0/0 ; pid=4345')):
            with self.assertRaises(RuntimeError):
                m.normalize_exec_start(value)

    def test_prestart_runtime_changes_are_not_configuration(self):
        before = copy.deepcopy(self.before)
        before['main']['properties']['ExecStartPre'] = self.running.replace(
            m.CURRENT_EXE, '/usr/bin/python3').replace(
                '--datadir /data/gtron/main/datadir', '/usr/local/libexec/gtron-mainnet-space-guard.py check')
        after = copy.deepcopy(before)
        after['main']['properties']['ExecStartPre'] = after['main']['properties']['ExecStartPre'].replace(
            'pid=4345', 'pid=9998').replace('01:39:35 UTC', '10:29:00 UTC').replace(
                'stop_time=[n/a]', 'stop_time=[Sun 2026-09-13 10:29:01 UTC]').replace(
                    'code=(null) ; status=0/0', 'code=exited ; status=0')
        m.same_configuration(self.h, before, after)

    def test_prestart_empty_is_allowed_but_adding_command_is_rejected(self):
        m.same_configuration(self.h, self.before, copy.deepcopy(self.before))
        after = copy.deepcopy(self.before)
        after['main']['properties']['ExecStartPre'] = self.running
        with self.assertRaisesRegex(RuntimeError, 'ExecStartPre'):
            m.same_configuration(self.h, self.before, after)

    def test_prestart_changed_argv_and_unknown_format_are_rejected(self):
        before = copy.deepcopy(self.before)
        before['main']['properties']['ExecStartPre'] = self.running
        for value in (self.running.replace('/data/gtron/main/datadir', '/data/other/datadir'),
                      self.running.replace(' ; status=0/0', ' ; status=0/0 ; flags=unknown')):
            after = copy.deepcopy(before)
            after['main']['properties']['ExecStartPre'] = value
            with self.assertRaises(RuntimeError):
                m.same_configuration(self.h, before, after)


if __name__ == '__main__':
    unittest.main(verbosity=2)

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
from unittest.mock import patch

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


if __name__ == '__main__':
    unittest.main(verbosity=2)

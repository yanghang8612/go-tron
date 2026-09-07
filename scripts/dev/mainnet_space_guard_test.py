import contextlib
import importlib.util
import io
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("guard", Path(__file__).with_name("mainnet_space_guard.py"))
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


class MainnetGuardTest(unittest.TestCase):
    def setUp(self):
        self.cfg = dict(data_device=7, stop_bytes=384 * guard.GIB,
                        start_bytes=512 * guard.GIB, min_free_inodes=100000)
        self.vfs = types.SimpleNamespace(f_frsize=4096, f_blocks=3 * 1024**4 // 4096,
                                         f_bavail=450 * guard.GIB // 4096, f_favail=500000,
                                         f_files=4000000)
        self.root_vfs = types.SimpleNamespace(f_frsize=4096, f_blocks=100 * guard.GIB // 4096,
                                              f_bavail=50 * guard.GIB // 4096,
                                              f_files=1000000, f_favail=900000)

    def inspect(self, starting=False, hold=False, device=7):
        with patch.object(guard, "read_config", return_value=self.cfg), \
             patch.object(guard.os, "stat", return_value=types.SimpleNamespace(st_mode=0o40755, st_dev=device)), \
             patch.object(guard.os, "statvfs", side_effect=lambda p: self.root_vfs if p == "/" else self.vfs), \
             patch.object(guard.os.path, "lexists", side_effect=lambda p: hold if p == guard.HOLD else False):
            return guard.inspect_space(starting)

    def test_hysteresis(self):
        self.assertTrue(self.inspect()["ok"])
        self.assertFalse(self.inspect(starting=True)["ok"])

    def test_low_bytes_and_inodes(self):
        self.vfs.f_bavail = 100 * guard.GIB // 4096
        self.vfs.f_favail = 10
        self.assertEqual(len(self.inspect()["reasons"]), 2)

    def test_hold_does_not_auto_clear(self):
        self.assertFalse(self.inspect(hold=True)["ok"])

    def test_exact_reserve_is_rejected(self):
        self.vfs.f_bavail = self.cfg["stop_bytes"] // 4096
        self.assertFalse(self.inspect()["ok"])
        self.vfs.f_bavail = self.cfg["start_bytes"] // 4096
        self.assertFalse(self.inspect(starting=True)["ok"])

    def test_data_inode_floor_is_at_least_five_percent(self):
        self.vfs.f_favail = self.vfs.f_files // 20
        self.assertFalse(self.inspect()["ok"])

    def test_root_bytes_and_inodes_guard_both_modes(self):
        self.vfs.f_bavail = 600 * guard.GIB // 4096
        self.root_vfs.f_bavail = 5 * guard.GIB // 4096
        self.root_vfs.f_favail = self.root_vfs.f_files // 20
        for starting in (True, False):
            result = self.inspect(starting=starting)
            self.assertFalse(result["ok"])
            self.assertEqual(len(result["reasons"]), 2)

    def test_wrong_device_rejected(self):
        with self.assertRaisesRegex(ValueError, "device mismatch"):
            self.inspect(device=8)

    def invoke(self, mode, result=None, failure=None, latch_error=None):
        with patch.object(guard.os, "geteuid", return_value=0), \
             patch.object(guard, "inspect_space", return_value=result, side_effect=failure), \
             patch.object(guard, "create_latch", side_effect=latch_error) as latch, \
             patch.object(guard.subprocess, "run", return_value=types.SimpleNamespace(returncode=0)) as stop, \
             contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            code = guard.run(mode)
            return code, latch, stop

    def test_check_never_writes_or_stops(self):
        code, latch, stop = self.invoke("check", failure=OSError("disk unavailable"))
        self.assertEqual(code, 1)
        latch.assert_not_called()
        stop.assert_not_called()

    def test_healthy_guard_no_mutation(self):
        code, latch, stop = self.invoke("guard", result={"ok": True})
        self.assertEqual(code, 0)
        latch.assert_not_called()
        stop.assert_not_called()

    def test_inspection_failure_latches_and_stops_only_mainnet(self):
        code, latch, stop = self.invoke("guard", failure=OSError("bad mount"))
        self.assertEqual(code, 1)
        latch.assert_called_once()
        self.assertEqual(stop.call_args[0][0], ["/bin/systemctl", "stop", "gtron.service"])
        self.assertEqual(stop.call_args[1]["timeout"], 650)

    def test_failed_latch_still_stops(self):
        _, _, stop = self.invoke("guard", result={"ok": False}, latch_error=OSError("full"))
        stop.assert_called_once()

    def test_latch_create_is_exclusive_and_persistent(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(guard, "HOLD", os.path.join(tmp, "hold")):
            guard.create_latch({"first": True})
            before = Path(guard.HOLD).read_bytes()
            guard.create_latch({"second": True})
            self.assertEqual(Path(guard.HOLD).read_bytes(), before)

    def test_latch_never_follows_symlink(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(guard, "HOLD", os.path.join(tmp, "hold")):
            target = Path(tmp, "target")
            target.write_text("untouched")
            os.symlink(str(target), guard.HOLD)
            guard.create_latch({"replace": True})
            self.assertEqual(target.read_text(), "untouched")

    def test_guard_requires_root_before_any_inspection(self):
        with patch.object(guard.os, "geteuid", return_value=1000), patch.object(guard, "inspect_space") as inspect:
            with self.assertRaises(PermissionError):
                guard.run("guard")
            inspect.assert_not_called()


if __name__ == "__main__":
    unittest.main()

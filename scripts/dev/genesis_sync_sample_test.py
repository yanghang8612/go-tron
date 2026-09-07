import importlib.util
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location("genesis_sync_sample", Path(__file__).with_name("genesis_sync_sample.py"))
sampler = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(sampler)


class GenesisSyncSampleTests(unittest.TestCase):
    def test_linux_stat_comm_and_disk_sector_units(self):
        fields = ["S"] + ["0"] * 49
        for offset, value in ((11, 120), (12, 60), (19, 900), (21, 8192)):
            fields[offset] = str(value)
        process = sampler.parse_stat("123 (name with ) spaces) " + " ".join(fields))
        self.assertEqual(process, {"state": "S", "user_ticks": 120, "system_ticks": 60,
                                  "start_ticks": 900, "rss_pages": 8192})
        disk = sampler.parse_diskstats("259 1 nvme1n1 10 2 7 12 20 3 11 30 4 50 60 0 0 0 0\n", "nvme1n1")
        self.assertEqual((disk["read_bytes"], disk["write_bytes"]), (7 * 512, 11 * 512))
        self.assertEqual((disk["io_ms"], disk["weighted_io_ms"]), (50, 60))
        with self.assertRaises(ValueError):
            sampler.parse_diskstats("", "nvme1n1")

    def test_metrics_missing_labels_nonfinite_and_integer_precision(self):
        metrics = sampler.parse_metrics('chain_head_block 12\nstate_x{kind="a b"} 0\n'
                                        'process_counter 9007199254740993\nstate_invalid NaN\n'
                                        '# comment\nnot_selected 7\n')
        self.assertEqual(metrics["process_counter"], 9007199254740993)
        self.assertEqual(metrics['state_x{kind="a b"}'], 0)
        self.assertNotIn("state_invalid", metrics)
        self.assertNotIn("chain_solidified_block", metrics)
        self.assertNotIn("not_selected", metrics)

    def test_rates_keep_missing_distinct_and_reject_pid_reuse(self):
        before = {"monotonic": 1, "process": {"start_ticks": 7, "user_ticks": 100, "system_ticks": 10},
                  "frontiers": {"head": 100, "hot_pruned": None},
                  "process_io": {"write_bytes": 20}}
        after = {"monotonic": 3, "process": {"start_ticks": 7, "user_ticks": 500, "system_ticks": 60},
                 "frontiers": {"head": 120, "hot_pruned": 80},
                 "process_io": {"write_bytes": 1020}}
        rates = sampler.derive(before, after, 100)
        self.assertEqual(rates["process.user_cpu_cores"], 2)
        self.assertEqual(rates["frontiers.head_per_second"], 10)
        self.assertEqual(rates["process_io.write_bytes_per_second"], 500)
        self.assertNotIn("frontiers.hot_pruned_per_second", rates)
        after["process"]["start_ticks"] += 1
        self.assertEqual(sampler.derive(before, after, 100), {})

    def test_read_cap_and_kilobytes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "oversize"
            with path.open("wb") as stream:
                stream.truncate(sampler.READ_CAP + 1)
            with self.assertRaises(ValueError):
                sampler.bounded_read(path)
        self.assertEqual(sampler.parse_kv("VmRSS: 2 kB\nThreads: 3\nState: S (sleeping)\n"),
                         {"VmRSS": 2048, "Threads": 3})


if __name__ == "__main__":
    unittest.main()

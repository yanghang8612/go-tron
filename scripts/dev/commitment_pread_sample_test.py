#!/usr/bin/env python3

import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("commitment_pread_sample.py")
SPEC = importlib.util.spec_from_file_location("commitment_pread_sample", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class CommitmentPreadSampleTest(unittest.TestCase):
    def test_normalize_metrics_accepts_live_and_legacy_shapes(self):
        live = MODULE.normalize_metrics({"metrics": {"a": {"count": 2}}})
        legacy = MODULE.normalize_metrics(
            {"metrics": [{"name": "a", "values": {"count": 2}}]}
        )
        self.assertEqual(live, {"a": {"count": 2}})
        self.assertEqual(legacy, live)

    def test_build_row_derives_interval_read_ratios(self):
        names = MODULE.ALL_METRICS
        previous_metrics = {name: 0 for name in names}
        current = {name: 0 for name in names}
        current.update(
            {
                "blockbuffer/commitment_parent/overlay/resolved": 10,
                "blockbuffer/commitment_parent/cache/resolved": 30,
                "blockbuffer/commitment_parent/durable/reads": 60,
                "blockbuffer/commitment_parent/depth_5_8/cache_resolved": 25,
                "blockbuffer/commitment_parent/depth_5_8/durable_reads": 75,
                "blockbuffer/commitment_parent/prefetch/planned": 50,
                "blockbuffer/commitment_parent/prefetch/durable_reads": 40,
                "blockbuffer/commitment_parent/prefetch/useful_hits": 20,
                "blockbuffer/commitment_parent/prefetch/depth_5/durable_reads": 20,
                "blockbuffer/commitment_parent/prefetch/depth_5/useful_hits": 15,
                "blockbuffer/commitment_parent/prefetch/depth_6_plus/durable_reads": 20,
                "blockbuffer/commitment_parent/prefetch/depth_6_plus/useful_hits": 5,
                "blockbuffer/commitment_parent/durable_publish_races": 5,
                "blockbuffer/commitment_parent/prefetch/unused_capacity_evicted": 4,
                "state/commitment/pipeline/prefetch_critical/planned": 20,
                "state/commitment/pipeline/prefetch_critical/wall_nanos": 2_000,
                "state/commitment/pipeline/prefetch_critical/wait_calls": 4,
                "state/commitment/pipeline/prefetch_critical/wait_nanos": 800,
                "cache/block/hit": 90,
                "cache/block/miss": 10,
                "cache/table/hit": 80,
                "cache/table/miss": 20,
                "filter/hit": 70,
                "filter/miss": 30,
            }
        )
        previous = {"unix": 100.0, "height": 1_000, "metrics": previous_metrics}
        row = MODULE.build_row(110.0, 1_020, current, previous)
        analysis = row["analysis"]
        self.assertEqual(row["intervalBlocks"], 20)
        self.assertEqual(row["blocksPerSecond"], 2.0)
        self.assertAlmostEqual(analysis["foregroundDurableRatio"], 0.6)
        self.assertAlmostEqual(analysis["foregroundDepthDurableRatio"]["depth_5_8"], 0.75)
        self.assertAlmostEqual(analysis["prefetchUsefulRatio"], 0.5)
        self.assertAlmostEqual(analysis["depth5UsefulRatio"], 0.75)
        self.assertAlmostEqual(analysis["depth6PlusUsefulRatio"], 0.25)
        self.assertAlmostEqual(analysis["durablePublishRaceRatio"], 0.05)
        self.assertEqual(analysis["criticalNanosPerPlannedRead"], 100.0)
        self.assertEqual(analysis["criticalWaitNanosPerLane"], 200.0)
        self.assertAlmostEqual(analysis["blockCacheHitRatio"], 0.9)

    def test_build_row_marks_counter_reset_without_negative_delta(self):
        current = {name: None for name in MODULE.ALL_METRICS}
        name = "blockbuffer/commitment_parent/durable/reads"
        current[name] = 3
        previous = {"unix": 100.0, "height": 1, "metrics": {name: 10}}
        row = MODULE.build_row(101.0, 2, current, previous)
        self.assertEqual(row["status"], "counter-reset")
        self.assertIsNone(row["delta"][name])
        self.assertIn(name, row["counterResets"])

    def test_with_prefix_replaces_existing_filter(self):
        got = MODULE.with_prefix("http://127.0.0.1:6062/debug/metrics?prefix=old/&x=1", "cache/")
        self.assertIn("prefix=cache%2F", got)
        self.assertIn("x=1", got)
        self.assertNotIn("old", got)


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3

import ast
import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("commitment_pread_sample.py")
SPEC = importlib.util.spec_from_file_location("commitment_pread_sample", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class CommitmentPreadSampleTest(unittest.TestCase):
    def test_source_parses_as_python_36(self):
        ast.parse(SCRIPT.read_text(encoding="utf-8"), filename=str(SCRIPT), feature_version=(3, 6))

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
                "blockbuffer/commitment_parent/singleflight/leaders": 90,
                "blockbuffer/commitment_parent/singleflight/waiters": 12,
                "blockbuffer/commitment_parent/singleflight/shared_results": 10,
                "blockbuffer/commitment_parent/singleflight/shared_results/foreground": 8,
                "blockbuffer/commitment_parent/singleflight/shared_results/prefetch": 2,
                "blockbuffer/commitment_parent/singleflight/shared_present": 8,
                "blockbuffer/commitment_parent/singleflight/shared_missing": 2,
                "blockbuffer/commitment_parent/singleflight/leader_errors": 1,
                "blockbuffer/commitment_parent/singleflight/wait_nanos": 1_200,
                "blockbuffer/commitment_parent/singleflight/waiters/foreground": 9,
                "blockbuffer/commitment_parent/singleflight/waiters/prefetch": 3,
                "blockbuffer/commitment_parent/prefetch/unused_capacity_evicted": 4,
                "state/commitment/pipeline/prefetch_critical/planned": 20,
                "state/commitment/pipeline/prefetch_critical/wall_nanos": 2_000,
                "state/commitment/pipeline/prefetch_critical/wait_calls": 4,
                "state/commitment/pipeline/prefetch_critical/wait_nanos": 800,
                "state/commitment/pipeline/prefetch_critical/queue_wait_calls": 4,
                "state/commitment/pipeline/prefetch_critical/queue_wait_nanos": 400,
                "state/commitment/pipeline/prefetch_critical/blocked_by_lookahead_calls": 2,
                "state/commitment/pipeline/prefetch_critical/blocked_by_lookahead_nanos": 300,
                "state/commitment/pipeline/prefetch_critical/queue_high_water": 3,
                "state/commitment/pipeline/prefetch_lookahead/finish_wait_calls": 2,
                "state/commitment/pipeline/prefetch_lookahead/finish_wait_nanos": 1_000,
                "state/commitment/pipeline/prefetch_lookahead/queue_high_water": 5,
                "chain/freezer/txindex/coverage": 100,
                "chain/freezer/txindex/debt/blocks": 16_384,
                "chain/freezer/txindex/prune/blocks": 40,
                "chain/freezer/txindex/prune/rows": 80,
                "chain/freezer/txindex/prune/duration": 2_000_000_000,
                "chain/freezer/txindex/maintenance/admitted": 3,
                "chain/freezer/txindex/maintenance/deferred": 4,
                "chain/freezer/txindex/deferred/sync": 2,
                "chain/freezer/txindex/deferred/catchup": 1,
                "chain/freezer/txindex/deferred/resource": 1,
                "state/snapshot/cold/history/forced_busy/passes": 4,
                "state/snapshot/cold/history/forced_busy/attempts": 3,
                "state/snapshot/cold/history/forced_busy/builds": 2,
                "state/snapshot/cold/history/admission/checks": 10,
                "state/snapshot/cold/history/admission/ready": 6,
                "state/snapshot/cold/history/admission/busy": 4,
                "state/snapshot/cold/lastpass/history/batch/blocks": 100,
                "state/snapshot/cold/lastpass/history/batch/txnums": 200,
                "state/snapshot/cold/history/forced_busy/last/batch/blocks": 20,
                "state/snapshot/cold/history/forced_busy/last/batch/txnums": 60,
                "state/snapshot/cold/history/forced_busy/last/recovery": 500_000_000,
                "state/snapshot/cold/history/forced_busy/last/duty_cycle_ppm": 250_000,
                "state/snapshot/cold/history/forced_busy/last/debt_blocks": 1_250,
                "state/snapshot/cold/history/forced_busy/last/debt_growth_blocks": -25,
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
        self.assertAlmostEqual(analysis["foregroundDurableRatio"], 60 / 108)
        self.assertAlmostEqual(analysis["foregroundDepthDurableRatio"]["depth_5_8"], 0.75)
        self.assertAlmostEqual(analysis["prefetchUsefulRatio"], 0.5)
        self.assertAlmostEqual(analysis["depth5UsefulRatio"], 0.75)
        self.assertAlmostEqual(analysis["depth6PlusUsefulRatio"], 0.25)
        self.assertAlmostEqual(analysis["durablePublishRaceRatio"], 0.05)
        self.assertAlmostEqual(analysis["singleflightSharedRatio"], 0.1)
        self.assertAlmostEqual(analysis["singleflightWaiterShareRatio"], 10 / 12)
        self.assertEqual(analysis["singleflightWaitNanosPerWaiter"], 100.0)
        self.assertAlmostEqual(analysis["singleflightSharedPresentRatio"], 0.8)
        self.assertAlmostEqual(analysis["singleflightForegroundWaiterRatio"], 0.75)
        self.assertAlmostEqual(analysis["singleflightLeaderErrorRatio"], 1 / 90)
        self.assertEqual(analysis["criticalNanosPerPlannedRead"], 100.0)
        self.assertEqual(analysis["criticalWaitNanosPerLane"], 200.0)
        self.assertEqual(analysis["criticalQueueWaitNanosPerCall"], 100.0)
        self.assertEqual(analysis["criticalQueueWaitNanosPerBlock"], 20.0)
        self.assertEqual(analysis["criticalBlockedByLookaheadNanosPerCall"], 150.0)
        self.assertEqual(analysis["criticalBlockedByLookaheadCallsPerBlock"], 0.1)
        self.assertEqual(analysis["criticalBlockedByLookaheadCallRatio"], 0.5)
        self.assertEqual(analysis["finishLookaheadWaitNanosPerCall"], 500.0)
        self.assertEqual(analysis["finishLookaheadWaitNanosPerBlock"], 50.0)
        self.assertEqual(analysis["criticalQueueHighWaterPerLane"], 3)
        self.assertEqual(analysis["lookaheadQueueHighWaterPerLane"], 5)
        self.assertEqual(analysis["txIndexDebtBlocks"], 16_384)
        self.assertEqual(analysis["txIndexCoverageBlocksPerBlock"], 5.0)
        self.assertEqual(analysis["txIndexPrunedBlocksPerBlock"], 2.0)
        self.assertEqual(analysis["txIndexPrunedRowsPerBlock"], 4.0)
        self.assertEqual(analysis["txIndexPruneNanosPerPrunedBlock"], 50_000_000.0)
        self.assertEqual(analysis["txIndexPruneDutyRatio"], 0.2)
        self.assertEqual(analysis["txIndexMaintenanceAdmittedPerBlock"], 0.15)
        self.assertEqual(analysis["txIndexMaintenanceDeferredPerBlock"], 0.2)
        self.assertEqual(analysis["txIndexMaintenanceAdmissionRatio"], 0.6)
        self.assertEqual(analysis["txIndexSyncDeferredPerBlock"], 0.1)
        self.assertEqual(analysis["coldSnapshotForcedBuildRatio"], 0.5)
        self.assertEqual(analysis["coldSnapshotForcedAttemptSuccessRatio"], 2 / 3)
        self.assertEqual(analysis["coldSnapshotForcedBuildsPerBlock"], 0.1)
        self.assertEqual(analysis["coldSnapshotAdmissionReadyRatio"], 0.6)
        self.assertEqual(analysis["coldSnapshotAdmissionBusyRatio"], 0.4)
        self.assertEqual(analysis["coldSnapshotLastBatchTxNumsPerBlock"], 2.0)
        self.assertEqual(analysis["coldSnapshotLastForcedBatchTxNumsPerBlock"], 3.0)
        self.assertEqual(analysis["coldSnapshotLastForcedRecoverySeconds"], 0.5)
        self.assertEqual(analysis["coldSnapshotLastForcedDutyRatio"], 0.25)
        self.assertEqual(analysis["coldSnapshotLastForcedDebtBlocks"], 1_250)
        self.assertEqual(analysis["coldSnapshotLastForcedDebtGrowthBlocks"], -25)
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

    def test_build_row_marks_monotonic_gauge_reset_without_negative_delta(self):
        current = {name: None for name in MODULE.ALL_METRICS}
        name = "chain/freezer/txindex/maintenance/admitted"
        current[name] = 3
        previous = {"unix": 100.0, "height": 1, "metrics": {name: 10}}
        row = MODULE.build_row(101.0, 2, current, previous)
        self.assertEqual(row["status"], "counter-reset")
        self.assertIsNone(row["delta"][name])
        self.assertIn(name, row["counterResets"])

    def test_process_identity_change_discards_all_process_scoped_deltas(self):
        current = {name: None for name in MODULE.ALL_METRICS}
        counter = "blockbuffer/commitment_parent/durable/reads"
        gauge = "chain/freezer/txindex/maintenance/admitted"
        current.update(
            {
                MODULE.PROCESS_IDENTITY_METRIC: 200,
                counter: 1_000,
                gauge: 100,
                "chain/freezer/txindex/debt/blocks": 12_345,
            }
        )
        previous = {
            "unix": 100.0,
            "height": 1,
            "metrics": {
                MODULE.PROCESS_IDENTITY_METRIC: 100,
                counter: 10,
                gauge: 2,
            },
        }
        row = MODULE.build_row(101.0, 2, current, previous)
        self.assertTrue(row["processRestart"])
        self.assertEqual(row["status"], "counter-reset")
        self.assertIn(MODULE.PROCESS_IDENTITY_METRIC, row["counterResets"])
        self.assertIsNone(row["delta"][counter])
        self.assertIsNone(row["delta"][gauge])
        self.assertEqual(row["analysis"]["txIndexDebtBlocks"], 12_345)

    def test_first_identity_sample_remains_bootstrap(self):
        current = {name: None for name in MODULE.ALL_METRICS}
        current[MODULE.PROCESS_IDENTITY_METRIC] = 200
        row = MODULE.build_row(101.0, 2, current, None)
        self.assertFalse(row["processRestart"])
        self.assertEqual(row["status"], "bootstrap")
        self.assertEqual(row["counterResets"], [])

    def test_process_identity_appearance_marks_upgrade_boundary(self):
        current = {name: None for name in MODULE.ALL_METRICS}
        counter = "blockbuffer/commitment_parent/durable/reads"
        current[MODULE.PROCESS_IDENTITY_METRIC] = 200
        current[counter] = 1_000
        previous = {"unix": 100.0, "height": 1, "metrics": {counter: 10}}
        row = MODULE.build_row(101.0, 2, current, previous)
        self.assertTrue(row["processRestart"])
        self.assertIsNone(row["delta"][counter])
        self.assertIn(MODULE.PROCESS_IDENTITY_METRIC, row["counterResets"])

    def test_process_identity_disappearance_marks_downgrade_boundary(self):
        current = {name: None for name in MODULE.ALL_METRICS}
        counter = "blockbuffer/commitment_parent/durable/reads"
        current[counter] = 1_000
        previous = {
            "unix": 100.0,
            "height": 1,
            "metrics": {MODULE.PROCESS_IDENTITY_METRIC: 200, counter: 10},
        }
        row = MODULE.build_row(101.0, 2, current, previous)
        self.assertTrue(row["processRestart"])
        self.assertIsNone(row["delta"][counter])
        self.assertIn(MODULE.PROCESS_IDENTITY_METRIC, row["counterResets"])

    def test_new_metrics_are_missing_safe_for_older_nodes(self):
        current = {name: None for name in MODULE.ALL_METRICS}
        previous = {"unix": 100.0, "height": 1, "metrics": {}}
        row = MODULE.build_row(101.0, 2, current, previous)
        self.assertEqual(row["status"], "ok")
        self.assertEqual(row["counterResets"], [])
        self.assertIsNone(row["analysis"]["criticalQueueWaitNanosPerCall"])
        self.assertIsNone(row["analysis"]["txIndexPruneDutyRatio"])
        self.assertIsNone(row["analysis"]["coldSnapshotLastForcedDutyRatio"])

    def test_new_prefixes_and_metric_types_match_exporters(self):
        self.assertIn("process/", MODULE.METRIC_PREFIXES)
        self.assertIn("chain/freezer/", MODULE.METRIC_PREFIXES)
        self.assertIn("state/snapshot/cold/", MODULE.METRIC_PREFIXES)
        self.assertIn(
            "state/commitment/pipeline/prefetch_critical/queue_wait_calls",
            MODULE.COUNTER_METRICS,
        )
        self.assertIn(
            "state/commitment/pipeline/prefetch_critical/queue_high_water",
            MODULE.POINT_GAUGE_METRICS,
        )
        freezer_admitted = "chain/freezer/txindex/maintenance/admitted"
        self.assertIn(freezer_admitted, MODULE.MONOTONIC_GAUGE_METRICS)
        self.assertNotIn(freezer_admitted, MODULE.COUNTER_METRICS)
        cold_duty = "state/snapshot/cold/history/forced_busy/last/duty_cycle_ppm"
        self.assertIn(cold_duty, MODULE.POINT_GAUGE_METRICS)
        self.assertNotIn(cold_duty, MODULE.MONOTONIC_GAUGE_METRICS)

    def test_with_prefix_replaces_existing_filter(self):
        got = MODULE.with_prefix("http://127.0.0.1:6062/debug/metrics?prefix=old/&x=1", "cache/")
        self.assertIn("prefix=cache%2F", got)
        self.assertIn("x=1", got)
        self.assertNotIn("old", got)


if __name__ == "__main__":
    unittest.main()

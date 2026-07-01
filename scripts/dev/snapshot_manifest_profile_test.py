#!/usr/bin/env python3
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "dev" / "snapshot_manifest_profile.py"


def write_manifest(path, segments, retired=None):
    manifest = {
        "version": 1,
        "generation": 7,
        "publishedUnix": 1,
        "visibleTxStart": 10,
        "visibleTxEnd": 20,
        "segments": segments,
    }
    if retired is not None:
        manifest["retired"] = retired
    (path / "manifest.json").write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")


def sample_segments():
    return [
        {"dataset": "account-latest", "kind": "latest", "fromTxNum": 10, "toTxNum": 20, "path": "latest/account.seg", "size": 1000},
        {"dataset": "account-latest", "kind": "accessor", "fromTxNum": 10, "toTxNum": 20, "path": "latest/account.idx", "size": 120},
        {"dataset": "account-latest", "kind": "btree", "fromTxNum": 10, "toTxNum": 20, "path": "latest/account.bt", "size": 80},
        {"dataset": "chain-freezer", "kind": "chain-freezer", "fromTxNum": 1, "toTxNum": 10, "path": "chain/freezer.seg", "size": 2000},
        {"dataset": "chain-freezer", "kind": "chain-index", "fromTxNum": 1, "toTxNum": 10, "path": "chain/index.idx", "size": 100},
        {"dataset": "chain-freezer", "kind": "chain-freezer-accessor", "fromTxNum": 1, "toTxNum": 10, "path": "chain/accessor.idx", "size": 300},
        {"dataset": "event-log", "kind": "event-log", "fromTxNum": 1, "toTxNum": 10, "path": "log/event.seg", "size": 500},
        {"dataset": "event-log", "kind": "event-log-index", "fromTxNum": 1, "toTxNum": 10, "path": "log/event.idx", "size": 250},
        {"dataset": "state-domain-change", "kind": "history", "fromTxNum": 10, "toTxNum": 20, "path": "history/account.seg", "size": 100},
        {"dataset": "state-domain-change", "kind": "inverted", "fromTxNum": 10, "toTxNum": 20, "path": "history/account.inv", "size": 50},
        {"dataset": "balance-trace", "kind": "balance-trace", "fromTxNum": 1, "toTxNum": 10, "path": "trace/balance.seg", "size": 70},
        {"dataset": "section-bloom", "kind": "section-bloom", "fromTxNum": 1, "toTxNum": 10, "path": "bloom/section.seg", "size": 30},
    ]


def write_segment_files(root, segments):
    for ref in segments:
        path = root / ref["path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x" * int(ref["size"]))


class SnapshotManifestProfileTest(unittest.TestCase):
    def test_profiles_active_segments_by_family_kind_and_dataset(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            write_manifest(tmpdir, sample_segments())

            proc = subprocess.run(
                [sys.executable, str(SCRIPT), str(tmpdir), "--json"],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            profile = json.loads(proc.stdout)
            self.assertEqual(profile["manifest"], str(tmpdir / "manifest.json"))
            self.assertEqual(profile["activeSegments"], 12)
            self.assertEqual(profile["totalBytes"], 4600)
            self.assertEqual(profile["payloadBytes"], 3700)
            self.assertEqual(profile["sidecarBytes"], 900)
            self.assertEqual(profile["sidecarShareMilli"], 196)
            self.assertEqual(profile["sidecarKinds"]["event-log-index"], 1)
            self.assertEqual(profile["byFamily"]["chain-freezer"]["sidecarBytes"], 400)
            self.assertEqual(profile["byFamily"]["event-log"]["sidecarShareMilli"], 334)
            self.assertEqual(profile["byKind"]["chain-index"]["sidecarBytes"], 100)
            self.assertEqual(profile["byDataset"]["account-latest"]["totalBytes"], 1200)
            candidates = profile["pointIndexCandidates"]
            self.assertEqual(candidates["txHashLookup"]["totalBytes"], 100)
            self.assertEqual(candidates["txHashLookup"]["snapshotShareMilli"], 22)
            self.assertEqual(candidates["eventLogIndex"]["totalBytes"], 250)
            self.assertEqual(candidates["eventLogIndex"]["snapshotShareMilli"], 55)
            self.assertEqual(candidates["stateHistoryAccessor"]["totalBytes"], 150)
            self.assertEqual(candidates["stateHistoryAccessor"]["payloadBytes"], 100)
            self.assertEqual(candidates["stateHistoryAccessor"]["sidecarBytes"], 50)
            self.assertEqual(candidates["stateHistoryAccessor"]["sidecarShareMilli"], 334)
            self.assertEqual(candidates["latestBTree"]["totalBytes"], 80)
            self.assertEqual(candidates["chainFreezerAccessor"]["totalBytes"], 300)
            self.assertEqual(candidates["chainFreezerAccessor"]["snapshotShareMilli"], 66)
            self.assertEqual(candidates["codeDomain"]["segments"], 0)

    def test_verify_files_accepts_matching_segment_sizes(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            segments = sample_segments()
            write_manifest(tmpdir, segments)
            write_segment_files(tmpdir, segments)

            proc = subprocess.run(
                [sys.executable, str(SCRIPT), str(tmpdir), "--json", "--verify-files"],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            profile = json.loads(proc.stdout)
            self.assertTrue(profile["verifyFiles"])
            self.assertEqual(profile["verifiedSegments"], len(segments))
            self.assertEqual(profile["totalBytes"], 4600)

    def test_verify_files_rejects_size_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            segments = [
                {"dataset": "event-log", "kind": "event-log", "fromTxNum": 1, "toTxNum": 1, "path": "log/event.seg", "size": 10},
            ]
            write_manifest(tmpdir, segments)
            (tmpdir / "log").mkdir(parents=True)
            (tmpdir / "log" / "event.seg").write_bytes(b"short")

            proc = subprocess.run(
                [sys.executable, str(SCRIPT), str(tmpdir), "--verify-files"],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
            self.assertIn("file size 5 does not match manifest size 10", proc.stderr)

    def test_verify_files_rejects_paths_outside_snapshot_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            write_manifest(
                tmpdir,
                [
                    {"dataset": "event-log", "kind": "event-log", "fromTxNum": 1, "toTxNum": 1, "path": "../event.seg", "size": 10},
                ],
            )

            proc = subprocess.run(
                [sys.executable, str(SCRIPT), str(tmpdir), "--verify-files"],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
            self.assertIn("escapes snapshot directory", proc.stderr)

    def test_human_output_accepts_manifest_file_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            write_manifest(tmpdir, sample_segments())

            proc = subprocess.run(
                [sys.executable, str(SCRIPT), str(tmpdir / "manifest.json")],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("snapshot manifest:", proc.stdout)
            self.assertIn("chain-freezer:", proc.stdout)
            self.assertIn("sidecarShareMilli=196", proc.stdout)
            self.assertIn("point-index candidates:", proc.stdout)
            self.assertIn("eventLogIndex:", proc.stdout)
            self.assertIn("snapshotShareMilli=55", proc.stdout)

    def test_threshold_gate_rejects_high_sidecar_share(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            write_manifest(
                tmpdir,
                [
                    {"dataset": "event-log", "kind": "event-log", "fromTxNum": 1, "toTxNum": 1, "path": "log/event.seg", "size": 400},
                    {"dataset": "event-log", "kind": "event-log-index", "fromTxNum": 1, "toTxNum": 1, "path": "log/event.idx", "size": 600},
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(tmpdir),
                    "--json",
                    "--max-sidecar-share-milli",
                    "500",
                    "--max-family-sidecar-share-milli",
                    "500",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
            profile = json.loads(proc.stdout)
            self.assertIn("overall sidecar share 600 milli exceeds max 500", profile["issues"])
            self.assertIn("event-log sidecar share 600 milli exceeds max 500", profile["issues"])
            self.assertIn("overall sidecar share", proc.stderr)

    def test_threshold_gate_rejects_heavy_point_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            write_manifest(
                tmpdir,
                [
                    {"dataset": "event-log", "kind": "event-log", "fromTxNum": 1, "toTxNum": 1, "path": "log/event.seg", "size": 400},
                    {"dataset": "event-log", "kind": "event-log-index", "fromTxNum": 1, "toTxNum": 1, "path": "log/event.idx", "size": 600},
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(tmpdir),
                    "--json",
                    "--max-point-sidecar-share-milli",
                    "500",
                    "--max-point-snapshot-share-milli",
                    "500",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
            profile = json.loads(proc.stdout)
            self.assertIn("eventLogIndex point sidecar share 1000 milli exceeds max 500", profile["issues"])
            self.assertIn("eventLogIndex point snapshot share 600 milli exceeds max 500", profile["issues"])
            self.assertIn("eventLogIndex point sidecar share", proc.stderr)
            self.assertIn("eventLogIndex point snapshot share", proc.stderr)

    def test_include_retired_adds_retired_segments_to_totals(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            write_manifest(
                tmpdir,
                [
                    {"dataset": "chain-freezer", "kind": "chain-freezer", "fromTxNum": 1, "toTxNum": 1, "path": "chain/freezer.seg", "size": 100},
                ],
                retired=[
                    {"dataset": "event-log", "kind": "event-log-index", "fromTxNum": 1, "toTxNum": 1, "path": "log/retired.idx", "size": 25},
                ],
            )

            base = subprocess.run(
                [sys.executable, str(SCRIPT), str(tmpdir), "--json"],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )
            included = subprocess.run(
                [sys.executable, str(SCRIPT), str(tmpdir), "--json", "--include-retired"],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(base.returncode, 0, base.stdout + base.stderr)
            self.assertEqual(included.returncode, 0, included.stdout + included.stderr)
            base_profile = json.loads(base.stdout)
            included_profile = json.loads(included.stdout)
            self.assertEqual(base_profile["totalBytes"], 100)
            self.assertEqual(base_profile["retiredSegments"], 0)
            self.assertEqual(included_profile["totalBytes"], 125)
            self.assertEqual(included_profile["retiredSegments"], 1)
            self.assertEqual(included_profile["sidecarBytes"], 25)


if __name__ == "__main__":
    unittest.main()

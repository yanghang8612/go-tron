#!/usr/bin/env python3
"""Read-only verifier for the frozen integration-progress evidence."""
import argparse
import base64
import collections
import gzip
import hashlib
import io
import json
import pathlib
import zipfile
import zlib

HERE = pathlib.Path(__file__).resolve().parent
OUTER = {
    "local-source-and-harness.zip": "a89e1eac58bd2265beea8ad46c04bed1beb2e458fea4dd7f8e0e8fd67cd333b7",
    "native-progress-and-acceptance.zip": "e4569b6f517f2c76c7eeb966528e636f0d741cbe552c9a5e10136c206cd1b5fc",
    "publish-control-and-live-progress.zip": "5cecb0ac5694af98d83bb52ee54e05f84e750f426916333ea6af191d5bf91815",
    "policy-census-and-adoption.zip": "5dafb2dc6401d088014db68e2ea777f5958c199b7163df48e348bd20ef527fe1",
    "mature-source-formats.zip": "70237d4fa1c49d847cc36696e7f6a12d427a696062604c9ccd582fddaad27c54",
}


def sha(data):
    return hashlib.sha256(data).hexdigest()


def checked_zip(data, manifest_name):
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        manifest = json.loads(z.read(manifest_name))
        assert len(z.namelist()) == len(set(z.namelist()))
        assert set(z.namelist()) == set(manifest) | {manifest_name}
        result = {}
        for name, expected in manifest.items():
            p = pathlib.PurePosixPath(name)
            assert not p.is_absolute() and ".." not in p.parts
            content = z.read(name)
            assert sha(content) == expected, name
            result[name] = content
        return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=pathlib.Path, default=HERE.parents[3])
    args = parser.parse_args()
    bundles = {}
    for name, expected in OUTER.items():
        data = (HERE / name).read_bytes()
        assert sha(data) == expected, name
        bundles[name] = checked_zip(data, "evidence-manifest.json")
    local = bundles["local-source-and-harness.zip"]
    native = bundles["native-progress-and-acceptance.zip"]
    payload = checked_zip(native["source-payload.zip"], "payload-manifest.json")
    checked_zip(native["capture-addon.zip"], "capture-addon-manifest.json")
    checked_zip(native["acceptance-addon.zip"], "acceptance-addon-manifest.json")
    assert payload["source-inventory.json"] == local["source-inventory.json"]
    manifest = json.loads(local["source-inventory.json"])
    assert len(manifest) == 2145
    for name, expected in manifest.items():
        assert sha((args.source_root / name).read_bytes()) == expected, name
    checks = json.loads(local["local-checks.json"])
    assert checks["source_inventory_sha256"] == sha(local["source-inventory.json"])
    receipt = json.loads(native["server-progress-01.json"])
    files = receipt["files"]
    assert files["source-verification.json"] == {
        "files": 2145, "mismatches": [], "tests": 1059
    }
    status = files["native-status.json"]
    assert status["complete"] and not status["production_mutations"]
    expected = {
        "native-record-tests", "native-snapshots-build", "native-http-build",
        "native-packages", "native-race", "native-vet", "native-sapling-build",
    }
    assert {c["label"] for c in status["checks"]} == expected
    assert all(c["returncode"] == 0 for c in status["checks"])
    capture = files["complete-input-status.json"]
    assert capture["files"] == 180 and capture["total"] == 4609
    assert capture["before_bytes"] - capture["after_bytes"] == 59609991
    assert not capture["complete"]
    assert not files["acceptance-status.json"]["complete"]
    publishing = bundles["publish-control-and-live-progress.zip"]
    uploaded = checked_zip(publishing["publish-addon.zip"], "publish-addon-manifest.json")
    for name, content in uploaded.items():
        assert publishing[name] == content, name
    live_bytes = publishing["server-progress-live-04.json"]
    assert sha(live_bytes) == "2dd5b5953288d9537a4a885d74debf623bccc99cbb6632aee50847a4a5048d68"
    live = json.loads(live_bytes)
    assert live["progress"]["files"] == 1090
    assert all(j["proc_exists"] and j["pid"] for j in live["jobs"].values())
    assert not live["progress"]["complete"]
    assert not live["acceptance"]["complete"] and not live["publish"]["complete"]
    assert live["progress"]["before_bytes"] - live["progress"]["after_bytes"] == 751604682
    policy = bundles["policy-census-and-adoption.zip"]
    for local_name, remote_name in [
        ("server-policy-census-01.json", "policy-census-01.json"),
        ("server-progress-live-07.json", "progress-live-07.json"),
    ]:
        data = policy[local_name]
        transport = json.loads(policy[local_name + ".transport.json"])
        assert transport["name"] == remote_name and transport["nonce"]
        assert transport["bytes"] == len(data) and transport["sha256"] == sha(data)
    census = json.loads(policy["server-policy-census-01.json"])
    assert census["complete"] and not census["production_mutations"]
    assert census["source_versions"] == {"2": {"files": 4609, "bytes": 48379318571}}
    assert census["source_manifest_sha256"] == "c0c642f3172bfb33013e868576f84e5d55d1947a562a954978b3686d1535eee2"
    completed = census["completed_by_source_version"]["2"]
    assert completed["files"] == completed["converted_files"] == census["complete_receipts_at_observation"] == 1631
    assert completed["kept_source_files"] == 0
    assert completed["source_bytes"] - completed["after_bytes"] == 1372209271
    log = policy["adoption-policy-stress.log"].decode()
    stress = json.loads(next(line[line.index("{"):] for line in log.splitlines() if '"trios"' in line))
    assert "--- PASS: TestRecordColumnFreshPolicyStress" in log
    assert stress["all_row_and_range_digests_identical"] and stress["offline_retained_cdc_source"]
    assert not stress["from_genesis_explicit_5_keeps_cdc_heuristic"]
    trios = {r["Format"]: r for r in stress["trios"]}
    assert {k: r["Files"] for k, r in trios.items()} == {
        "2": 8393548, "3": 2409637, "auto": 2409637, "5": 8393614,
    }
    assert trios["auto"]["PhysicalVersion"] == 3 and trios["5"]["PhysicalVersion"] == 5
    assert all(r["Logical"] == trios["2"]["Logical"] for r in trios.values())
    fallback = stress["offline_stats"]
    assert not fallback["Converted"] and fallback["AfterBytes"] == fallback["BeforeBytes"] < fallback["CandidateBytes"]
    negative = json.loads(policy["native-transfer-negative-01.json"])
    assert negative["rejected"] and not negative["local_payload_created"]
    latest = json.loads(policy["server-progress-live-07.json"])
    assert latest["progress"]["files"] == 2280 and latest["progress"]["total"] == 4609
    assert latest["progress"]["before_bytes"] - latest["progress"]["after_bytes"] == 2019200049
    assert all(j["proc_exists"] and j["pid"] for j in latest["jobs"].values())
    assert "391 28550" in latest["jobs"]["capture"]["ps"]
    assert not any(latest[k]["complete"] for k in ("capture", "progress", "acceptance", "publish"))
    assert not latest["production_mutations"]
    mature = bundles["mature-source-formats.zip"]
    for name in ("mature-source-formats-01", "mature-source-prefixes-01",
                 "mature-formats-archive-receipt", "mature-source-locations-01", "progress-live-08"):
        data = mature["server-" + name + ".json"]
        transport = json.loads(mature["server-" + name + ".json.transport.json"])
        assert transport["name"] == name + ".json" and transport["nonce"]
        assert transport["bytes"] == len(data) and transport["sha256"] == sha(data)
    mature_summary = json.loads(mature["server-mature-source-formats-01.json"])
    prefixes = json.loads(mature["server-mature-source-prefixes-01.json"])
    assert mature_summary["complete"] and mature_summary["header_only"]
    assert not mature_summary["physical_sha_recomputed"] and not prefixes["physical_sha_recomputed"]
    assert not mature_summary["production_mutations"] and not prefixes["production_mutations"]
    plan_raw = gzip.decompress((HERE.parent / "20261025-history-tx-runs/census/census-plan.json.gz").read_bytes())
    assert sha(plan_raw) == prefixes["source_plan_sha256"] == mature_summary["source_plan_sha256"] == "108728f335f23009784a9af2931e70ba9176f28bd22bcc92b2130d8bead66b6c"
    plan = json.loads(plan_raw)
    assert len(plan) == prefixes["files"] == mature_summary["source_files"] == 8651
    assert prefixes["header_prefix_bytes"] == 16
    decoder = zlib.decompressobj()
    raw_headers = decoder.decompress(base64.b64decode(prefixes["ordered_prefixes_zlib_base64"], validate=True), len(plan) * 16 + 1)
    assert decoder.eof and not decoder.unused_data and not decoder.unconsumed_tail
    assert len(raw_headers) == len(plan) * 16 and sha(raw_headers) == prefixes["ordered_prefixes_sha256"]
    assert prefixes["full_rows_sha256"] == mature_summary["rows_sha256"] == "459847497ca77e5799a369cf6a4a9976385e59bc8efc9b10f634b31d199ccd04"
    groups = collections.defaultdict(lambda: dict(files=0, physical_bytes=0, records=0, logical_bytes=0))
    windows = collections.defaultdict(lambda: collections.defaultdict(lambda: dict(files=0, physical_bytes=0)))
    for i, row in enumerate(plan):
        header = raw_headers[i*16:(i+1)*16]
        assert header[:8] == b"gtcblk01"
        version = str(int.from_bytes(header[8:12], "big"))
        g = groups[version]
        g["files"] += 1
        g["physical_bytes"] += row["history_ref"]["size"]
        g["records"] += row["records"]
        g["logical_bytes"] += row["logical_bytes"]
        window = ("25M-30M" if row["first_block"] >= 25000000 and row["last_block"] < 30000000 else
                  "30M-35M" if row["first_block"] >= 30000000 and row["last_block"] <= 35000000 else "boundary")
        windows[window][version]["files"] += 1
        windows[window][version]["physical_bytes"] += row["history_ref"]["size"]
    assert dict(groups) == mature_summary["groups"]
    assert {k: dict(v) for k, v in windows.items()} == mature_summary["windows"]
    assert groups["2"]["files"] == 8651 and len(groups) == 1
    assert groups["2"]["physical_bytes"] == mature_summary["source_physical_bytes"] == 99504679277
    full_archive = json.loads(mature["server-mature-formats-archive-receipt.json"])
    assert full_archive["bytes"] == 442721 and full_archive["sha256"] == "96ea01fc84e8ff8354b1137035f48d2937c611fcd2f9a87b9c6a3b5f34be4433"
    # This verifies the server receipt, not an unavailable local copy of that ZIP.
    latest = json.loads(mature["server-progress-live-08.json"])
    assert latest["progress"]["files"] == 2330
    assert latest["progress"]["before_bytes"] - latest["progress"]["after_bytes"] == 2075235778
    assert all(j["proc_exists"] and j["pid"] for j in latest["jobs"].values())
    assert "391 28550" in latest["jobs"]["capture"]["ps"]
    assert not any(latest[k]["complete"] for k in ("capture", "progress", "acceptance", "publish"))
    print(json.dumps({
        "source_files_verified": len(manifest), "native_checks_passed": len(expected),
        "initial_capture_files": capture["files"],
        "latest_verified_live_capture_files": latest["progress"]["files"],
        "verified_live_at": latest["utc"], "all_source_headers_verified": census["all_source_files"],
        "synthetic_cdc_adoption_counterexample_verified": True,
        "mature_format_prefixes_verified": len(plan),
        "mature_file_bodies_rehashed_by_this_census": False,
        "full_acceptance_complete": False,
        "scope": "Frozen progress receipt; not full-copy or rollout acceptance",
    }, indent=2))


if __name__ == "__main__":
    main()

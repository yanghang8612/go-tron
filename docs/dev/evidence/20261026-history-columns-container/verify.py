#!/usr/bin/env python3
"""Read-only verifier for the frozen integration-progress evidence."""
import argparse
import hashlib
import io
import json
import pathlib
import zipfile

HERE = pathlib.Path(__file__).resolve().parent
OUTER = {
    "local-source-and-harness.zip": "a89e1eac58bd2265beea8ad46c04bed1beb2e458fea4dd7f8e0e8fd67cd333b7",
    "native-progress-and-acceptance.zip": "e4569b6f517f2c76c7eeb966528e636f0d741cbe552c9a5e10136c206cd1b5fc",
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
    print(json.dumps({
        "source_files_verified": len(manifest), "native_checks_passed": len(expected),
        "partial_capture_files": capture["files"], "full_acceptance_complete": False,
        "scope": "Frozen progress receipt; not full-copy or rollout acceptance",
    }, indent=2))


if __name__ == "__main__":
    main()

"""Recompute the small HTTP performance capsule; no database rescan."""
import argparse
import collections
import hashlib
import json
import math
from pathlib import Path
import statistics
import zipfile

parser = argparse.ArgumentParser()
parser.add_argument("archive", type=Path)
parser.add_argument("--sha256", required=True)
parser.add_argument("--out", type=Path, required=True)
args = parser.parse_args()
archive_sha = hashlib.sha256(args.archive.read_bytes()).hexdigest()
assert archive_sha == args.sha256
with zipfile.ZipFile(args.archive) as z:
    names = z.namelist()
    assert len(names) == len(set(names))
    assert all(not n.startswith("/") and ".." not in Path(n).parts for n in names)
    files = {n: z.read(n) for n in names}
manifest = json.loads(files["files-sha256.json"])
assert set(files) == set(manifest) | {"files-sha256.json"}
for name, meta in manifest.items():
    assert len(files[name]) == meta["bytes"]
    assert hashlib.sha256(files[name]).hexdigest() == meta["sha256"]

def read(name):
    return json.loads(files[name])

prefix = "http-performance-priority/"
status = read(prefix + "status.json")
assert status["complete"] and status["all_results_correct"]
assert status["production_before"] == status["production_after"]
assert status["production_identity_unchanged"]
assert status["fixed_cut"]["head"] == 45836827
assert status["fixed_cut"]["head_tx"] == 4240407293
assert read("source-muhash/status.json")["complete"]
scope = read("performance-scope.json")
assert scope["full_source_muhash_verified"]
assert not scope["full_snapshot_restore_verified"]
requests = [json.loads(x) for x in files[prefix + "requests.jsonl"].splitlines()]
assert len(requests) == 96
assert {q["id"] for q in requests} == set(range(1, 97))
server = {}
for name, content in files.items():
    if name.startswith(prefix) and name.endswith("/server-requests.jsonl"):
        for line in content.splitlines():
            q = json.loads(line)
            if q["method"] == "eth_call":
                assert q["id"] not in server
                server[q["id"]] = q
assert set(server) == set(range(1, 97))
known = {3144020: 6, 3144021: 7, 5000000: 63, 9000000: 207}
groups = collections.defaultdict(list)
def timeval(t):
    return t["Sec"] + t["Usec"] / 1e6
def io(text):
    return {k: int(v) for k, v in (x.split(":", 1) for x in text.splitlines())}
for q in requests:
    assert q["correct"] and "error" not in q["response"]
    assert q["response"]["result"] == q["expected"]
    if q["height"] in known:
        assert int(q["expected"], 16) == known[q["height"]]
    raw = server[q["id"]]
    before, after = io(raw["io_before"]), io(raw["io_after"])
    metrics = {
        "seconds_complete_loopback_http": q["seconds_complete_loopback_http"],
        "server_seconds": raw["elapsed_ns"] / 1e9,
        "cpu_seconds": sum(timeval(raw["usage_after"][k]) - timeval(raw["usage_before"][k]) for k in ["Utime", "Stime"]),
        "allocated_bytes": raw["alloc_bytes"],
        "gc_cycles": raw["num_gc"],
        "read_bytes": after["read_bytes"] - before["read_bytes"],
        "rchar": after["rchar"] - before["rchar"],
        "read_syscalls": after["syscr"] - before["syscr"],
    }
    groups[q["mode"], q["cache"], q["tag"]].append(metrics)
summary = read(prefix + "summary.json")
assert len(summary) == len(groups) == 32
for row in summary:
    values = groups[row["mode"], row["cache"], row["tag"]]
    assert len(values) == row["n"] == (4 if row["cache"] == "process-warm" else 2)
    for key, saved in row["medians"].items():
        assert math.isclose(statistics.median(v[key] for v in values), saved, rel_tol=1e-12, abs_tol=1e-9)
for name in files:
    if name.startswith(prefix) and name.endswith("/ready.json"):
        ready = read(name)
        assert all(ready[k] == v for k, v in status["fixed_cut"].items())
    if name.startswith(prefix) and name.endswith("/storage0-oracle.json"):
        assert read(name)["equals_getter"]
assert files["source-baseline/core/offline_full_acceptance_http_test.go"] == files["source-candidate/core/offline_full_acceptance_http_test.go"]
cold = read("http-filepage-cold-9m/status.json")
assert not cold["complete"] and cold["deferred"]
profile = read("http-performance-profile/status.json")
assert profile["complete"] and profile["all_returns_207"]
profile_calls = read("http-performance-profile/profile-calls.json")
assert len(profile_calls) == profile["calls"]
assert all(int(q["response"]["result"], 16) == 207 for q in profile_calls)
args.out.mkdir(parents=True, exist_ok=True)
for name in [prefix + "summary.json", prefix + "runtime-limits.json", prefix + "status.json", "http-performance-profile/status.json", "http-filepage-cold-9m/status.json", "performance-scope.json"]:
    target = args.out / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(files[name])
for name in ["cpu-top.txt", "cpu-eth-call-tag-top.txt", "heap-top.txt"]:
    (args.out / name).write_bytes(files["http-performance-profile/" + name])
result = {"complete": True, "archive_sha256": archive_sha, "members": len(files), "requests": 96, "groups_recomputed": 32, "known_answer_heights": sorted(known), "all_96_version_results_equal": True, "full_source_muhash_previously_passed": True, "full_snapshot_restore_verified": False, "filepage_cold_deferred": True, "profile_calls": len(profile_calls), "scope": "Archive and raw HTTP/resource receipt recomputation; no repeated database scan."}
(args.out / "local-performance-verification.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result, indent=2))

#!/usr/bin/env python3
"""Verify the sealed observer packet and the bounded loopback reprobe."""
from pathlib import Path
import base64
import hashlib
import json
import zlib

p = Path(__file__).resolve().parent
for line in (p / "SHA256SUMS").read_text().splitlines():
    digest, name = line.split("  ", 1)
    assert Path(name).name == name
    assert hashlib.sha256((p / name).read_bytes()).hexdigest() == digest, name

seal = (p / "seal.txt").read_text().split()
encoded = (p / "packet.txt").read_text().strip()
assert encoded.startswith("GTRROL:")
packet = zlib.decompress(base64.b64decode(encoded[7:]))
assert hashlib.sha256(packet).hexdigest() == seal[1]
assert len(packet) == int(seal[3])
for name, content in json.loads(packet).items():
    assert Path(name).name == name
    assert (p / name).read_text() == content

status = json.loads((p / "status.json").read_text())
rows = [json.loads(line) for line in (p / "runtime-samples.jsonl").read_text().splitlines()]
summary = json.loads((p / "summary.json").read_text())
assert status["expected_identity"] and status["old_inventory_unchanged"]
assert status["pid"] == 10692 and len(rows) == 45
assert status["binary_sha256"] == "859f0b6b44dcad8b74905a7d889d93ab1317d91a2365aa8aab1c30c455313505"
assert all(not row.get("new_error_lines") for row in rows)
periodic = [row for row in rows if "disk_live_sample" in row]
timeouts = [row for row in rows if row.get("errors")]
assert len(periodic) == 8 and len(timeouts) == 4
assert all(row["errors"] == ["timeout('timed out',)"] for row in timeouts)
assert [row["utc"] for row in timeouts] == summary["timeout_batch_utc"]
assert len(summary["tvm_stage_timeout_batches"]) == 3
assert len(summary["genesis_stage_timeout_batches"]) == 1
complete = [row for row in periodic if len(row.get("tvm_counter_history_probes", [])) == 2]
assert len(complete) == summary["complete_history_probe_batches_correct"] == 4
for row in periodic:
    assert row.get("genesis_balance_correct") is not False
    assert row.get("first_transaction_lookup_correct") is not False
    assert all(probe["correct"] for probe in row.get("transfer_history_probes", []) + row.get("tvm_counter_history_probes", []))
assert summary["cold_published_block"] == summary["cold_pruned_block"] == 8748336
assert summary["eligible_backlog_blocks"] == 261307
assert summary["cold_errors"] == 0

text = (p / "timeout-reprobe-terminal.txt").read_text().strip()
assert text.startswith("REPRO_JSON ")
reprobe = json.loads(text[len("REPRO_JSON "):])
assert reprobe == json.loads((p / "timeout-reprobe.json").read_text())
assert reprobe["timeout_seconds"] == 25 and len(reprobe["probes"]) == 4
assert all("error" not in probe for probe in reprobe["probes"])
assert all(probe["correct"] for probe in reprobe["probes"] if probe["name"] != "latest")
assert hashlib.sha256((p / "observer-source.py").read_bytes()).hexdigest() == "4e4029013b0313424bfb3a87d0e05dd144f93df46e5f0be0519081edf417ebd3"
assert "urlopen(r,timeout=8)" in (p / "observer-source.py").read_text()
print("PASS: four incomplete timeout batches; completed historical values and bounded reprobe correct; original identity retained")

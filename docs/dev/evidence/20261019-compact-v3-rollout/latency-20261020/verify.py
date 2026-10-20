#!/usr/bin/env python3
"""Verify sealed server audits and the read-only latency evidence."""
from pathlib import Path
import base64
import hashlib
import json
import re
import zlib

p = Path(__file__).resolve().parent
for line in (p / "SHA256SUMS").read_text().splitlines():
    digest, name = line.split("  ", 1)
    path = (p / name).resolve()
    assert path.is_relative_to(p) and path.is_file(), name
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest, name

def sealed(packet_name, seal_name, prefix):
    text = (p / packet_name).read_text().strip()
    assert text.startswith(prefix)
    data = zlib.decompress(base64.b64decode(text[len(prefix):]))
    seal = (p / seal_name).read_text().split()
    assert len(data) == int(seal[3])
    assert hashlib.sha256(data).hexdigest() == seal[1]
    return json.loads(data)

packet = sealed("packet.txt", "seal.txt", "GTRAUD:")
for name, value in packet.items():
    assert Path(name).name == name
    assert (p / name).read_text() == value
supplement = sealed("supplement-packet.txt", "supplement-seal.txt", "GTRSUP:")
assert supplement == json.loads((p / "supplement.json").read_text())
assert supplement["journal"]["rc"] == 0
assert len(supplement["journal"]["error_lines"]) == 5
assert all('err="context canceled"' in line for line in supplement["journal"]["error_lines"])

audit = json.loads((p / "audit.json").read_text())
final_text = (p / "final-audit-terminal.txt").read_text().strip()
assert final_text.startswith("FINAL_AUDIT ")
final = json.loads(final_text[len("FINAL_AUDIT "):])
assert final == json.loads((p / "final-audit.json").read_text())
assert final["journal_rc"] == 0 and not final["journal_muhash_or_severe_errors"]
assert final["samples"] == 910 and final["periodic_batches"] == 180
assert final["complete_tvm_batches"] == 175
assert len(final["sample_errors"]) == 5 and final["returned_false"] == 0
assert all(row["errors"] == ["timeout('timed out',)"] and not row["log_errors"] for row in final["sample_errors"])
assert audit["identity"]["sha256"] == final["last_identity"]["sha256"] == "859f0b6b44dcad8b74905a7d889d93ab1317d91a2365aa8aab1c30c455313505"
assert final["pid_counts"] == {"10692": 910}
assert audit["timezone_env"] == ["TZ=Asia/Shanghai"]
assert audit["logs"][0]["unparsed_lines_all"] == 0
assert audit["logs"][0]["error_count"] == 0
assert audit["manifest"]["history_segments"] == 3388
assert audit["manifest"]["overlap_after_target_tx"] == 3002
assert all(q["correct"] for q in audit["probes"] if q["name"] != "latest")

source = json.loads((p / "source-verification.json").read_text())
assert all(row["matches_deployed_commit"] and row["matches_local"] for row in source["files"].values())
assert all(row["same_before_compact_v3"] for row in source["functions"])
balance = json.loads((p / "balance-stack-summary.json").read_text())
assert len(balance) == 8 and all(row["tx_range_for_genesis"] for row in balance)
stacks = json.loads((p / "live-20261020T005730/active-query-stacks.json").read_text())
assert len(stacks) == 13 and all("[runnable]" in row["stack"].splitlines()[0] for row in stacks)
cpu = json.loads((p / "cpu-20261020T005832/attribution.json").read_text())
assert cpu["query_cpu_ns"] == 12930000000
assert cpu["cumulative_functions"]["(*Manager).IterateStateDomainChangesByKey"]["cpu_ns"] == 9910000000
assert cpu["cumulative_functions"]["IterateStateDomainChangesByBlockRangeContext"]["cpu_ns"] == 3010000000
assert cpu["exclusive_high_level_phases_ns"]["metadata"] == 9200000000
print("PASS: night audit has five HTTP timeouts and no observed MuHash error; cold lookup, genesis TxRange scan and source identity evidence consistent")

#!/usr/bin/env python3
"""Verify retained evidence and regenerate analysis.json without a live node."""
import collections
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def read(name):
    return json.loads((ROOT / name).read_text())


def percent(saved, before):
    return saved * 100 / before


def compare(before, after):
    return {"before": before, "after": after, "saved": before - after,
            "saved_percent": percent(before - after, before)}


if (ROOT / "sha256.json").exists():
    for name, expected in read("sha256.json").items():
        assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == expected, name

hot = read("distribution.json")
prior = json.loads((ROOT.parent / "20261018-generation-census/census.json").read_text())
assert hot["complete"] and hot["matches_authenticated_baseline"] and prior["muhash_verified"]
assert hot["source_rows"] == prior["source_rows"]
assert hot["source_sha256"] == prior["source_sha256"]
assert hot["Block"] == prior["block_num"] and hot["TxNum"] == prior["tx_num"]
assert hot["BlockHash"] == bytes(prior["block_hash"]).hex()
assert hot["Root"] == bytes(prior["muhash_root"]).hex()
for new, old in [("rows", "rows"), ("key_bytes", "physical_key_bytes"),
                 ("wrapped_bytes", "wrapped_value_bytes"), ("payload_bytes", "payload_bytes")]:
    assert sum(v[new] for v in hot["Domains"].values()) == prior["total_kv"][old]
    for cls in ["current", "obsolete"]:
        assert sum(v[new] for k, v in hot["Domains"].items() if k.endswith("/" + cls)) == prior["classes"][cls][old]
for v in hot["Numbers"].values():
    assert sum(v["minimal_be_widths"].values()) == v["rows"]
    assert sum(v["logical_key_widths"].values()) == v["rows"]
    assert v["payload_bytes"] == 8 * v["rows"]
    assert v["zero_rows"] == v["minimal_be_widths"].get("0", 0)
a = hot["Assets"]
assert a["Legacy"] == a["Equal"] + a["Different"] + a["MissingV2"]
assert a["V2"] == a["Equal"] + a["Different"] + a["V2Only"]
assert not a["MissingV2"] and not a["MissingNameMapping"] and not a["MappingCollision"]
accounts = hot["Accounts"]
assert accounts["rows"] == hot["source_rows"][0]
assert accounts["wrapped_bytes"] == accounts["CoreBytes"] + accounts["EnvelopeBytes"]
assert accounts["rows"] == sum(accounts[k] for k in ["AddressMatchesOwner", "AddressAbsent", "AddressDiffers"])
assert hot["SameABIRows"] + hot["DifferentABIRows"] + hot["MissingSeparateABIRows"] == hot["MetadataABIRows"]
sst = hot["sst_experiments"]
for v in sst.values():
    assert v["checksums_verified"]
for prefix, count in [("int64", sum(v["rows"] for v in hot["Numbers"].values())),
                      ("account", accounts["rows"]), ("native", sum(v["rows"] for v in hot["Native"].values()))]:
    assert all(v["rows"] == count for k, v in sst.items() if k.startswith(prefix + "-"))
sst_comparisons = {}
for name, base, candidate in [("int64", "int64-post-v2", "int64-trim"),
                              ("account", "account-baseline", "account-address-elided"),
                              ("native", "native-baseline", "native-zstd")]:
    for codec in ["snappy", "none"]:
        b, c = sst[base + "-" + codec], sst[candidate + "-" + codec]
        assert b["key_bytes"] == c["key_bytes"] and b["rows"] == c["rows"]
        sst_comparisons[name + "-" + codec] = compare(b["sst_bytes"], c["sst_bytes"])

code = read("code-distribution.json")
assert code["Complete"] and code["Block"] == hot["Block"] and len(code["SourceSHA"]) == 64
assert code["Rows"] == 1809 and code["CodeBytes"] == 11499696
for v in code["Experiments"].values():
    assert v["rows"] == code["Rows"] and v["key_bytes"] == code["KeyBytes"] and v["checksums_verified"]
for codec in ["snappy", "none"]:
    b, c = code["Experiments"]["code-baseline-" + codec], code["Experiments"]["code-zstd-" + codec]
    assert b["value_bytes"] == code["CodeBytes"]
    sst_comparisons["code-" + codec] = compare(b["sst_bytes"], c["sst_bytes"])

inventory, headers, selection = read("inventory.json"), read("headers.json"), read("cold-selection.json")
assert not inventory["missing"] and not inventory["size_mismatches"]
assert inventory["manifest_sha256"] == headers["state_manifest_sha256"] == selection["original_manifest_sha256"]
assert selection["population_segments"] == 4053
assert selection["selected_indices"] == [i * (4053 - 1) // 15 for i in range(16)]
files = {f["path"]: f for f in selection["files"]}
assert len(files) == 48
cold, votes = read("cold-distribution.json"), read("cold-distribution-votes.json")
assert cold["Complete"] and votes["Complete"]
assert cold["ManifestSHA256"] == votes["ManifestSHA256"]
for r in [cold, votes]:
    assert len(r["Samples"]) == 16
    sums = collections.defaultdict(collections.Counter)
    for s in r["Samples"]:
        assert s["Path"] in files and s["SHA256"] == files[s["Path"]]["manifest_checksum"]
        assert s["Bytes"] == files[s["Path"]]["size"]
        assert s["Rows"] == sum(v["Rows"] for v in s["Domains"].values())
        for name, v in s["Domains"].items():
            assert v["Present"] <= v["Rows"]
            assert sum(v["Widths"].values()) <= v["Present"]
            for k in ["Rows", "Present", "Payload", "KeyBytes", "Zero", "AddressElisionBytes"]:
                sums[name][k] += v[k]
    for name, counts in sums.items():
        assert all(r["Total"][name][k] == v for k, v in counts.items())
for k in ["Rows", "Present", "Payload", "KeyBytes"]:
    assert votes["Total"]["kv-latest/WitnessVoteState/VotesIndex"][k] + votes["Total"]["kv-latest/WitnessVoteState/voter-record"][k] == cold["Total"]["kv-latest/WitnessVoteState"][k]
payload = sum(v["Payload"] for v in votes["Total"].values())
assert payload == 553761185
assert sum(v["Rows"] for v in votes["Total"].values()) == 11229671
for k in ["event_log_v4_sections", "history_accessor_v7_sections", "transaction_index_sections"]:
    s = headers[k]
    meta = {"total", "files", "rows", "records", "keys"}
    assert sum(v for k, v in s.items() if k not in meta) == s["total"]
assert headers["event_log_v4_sections"]["total"] == inventory["active"]["event-log/event-log"]["bytes"]
assert headers["history_accessor_v7_sections"]["total"] == inventory["active"]["state-domain-change/accessor"]["bytes"]
for v in headers["ancient_v2_tables"].values():
    assert v["total"] == sum(v[k] for k in ["header", "dictionary", "frame_table", "compressed"])

cdc, pages = read("cdc-comparison.json"), read("sidecar-comparison.json")
assert cdc["Complete"] and pages["Complete"] and len(cdc["Samples"]) == 16
assert cdc["Before"] == sum(f["size"] for f in files.values())
for k, field in [("Before", "activeBytesBefore"), ("After", "activeBytesAfter")]:
    assert cdc[k] == sum(s[field] for s in cdc["Samples"])
assert all(s["migratedTrios"] == 1 and s["completedTrios"] == 1 and not s["remainingTrios"] and s["activeBytesAfter"] > s["activeBytesBefore"] for s in cdc["Samples"])
page_comparisons = {}
for name, suffix in [("accessor", ".kv"), ("inverted", ".idx")]:
    raw = sum(f["size"] for p, f in files.items() if p.endswith(suffix))
    for v in pages["Models"][name]:
        assert v["Files"] == 16 and v["Raw"] == raw
        assert v["FrameDirectory"] == v["Pages"] * 24
        assert v["RawFallbackPages"] <= v["Pages"]
        assert v["Pages"] == sum((f["size"] + v["PageBytes"] - 1) // v["PageBytes"] for p, f in files.items() if p.endswith(suffix))
        page_comparisons[name + "-" + str(v["PageBytes"])] = compare(v["Raw"], v["Stored"])

tx = read("txindex-width.json")
assert tx["Complete"] and len(tx["Runs"]) == tx["Files"] == 1307
for k in ["Rows", "Bytes", "Packed", "Plain", "CandidateBytes"]:
    assert sum(v[k] for v in tx["Runs"]) == tx[k]
widths = collections.Counter()
previous_end = None
for v in tx["Runs"]:
    assert len(v["SHA256"]) == 64 and v["Packed"] == v["Rows"] and not v["Plain"]
    assert v["RelativeWidth"] == max(1, (v["MaxRelative"].bit_length() + 7) // 8)
    assert v["CandidateBytes"] == v["Bytes"] - (8 - v["RelativeWidth"]) * v["Rows"]
    assert v["End"] > v["Start"] and (previous_end is None or v["Start"] == previous_end)
    previous_end = v["End"]
    widths[str(v["RelativeWidth"])] += v["Rows"]
assert dict(widths) == tx["Widths"] == {"4": 491120809}
proof = read("consistency.json")
assert proof["complete"] and proof["source_files_verified_before"] == proof["source_files_verified_after"] == 599
assert proof["service_before"] == proof["service_after"] and not proof["production_modified"]
final = read("final-proof.json")
assert not final["production_modified"] and final["production_service"]["ActiveState"] == "active"

analysis = {
    "verified": True,
    "hot_cut_block": hot["Block"],
    "cold_cut_hot_prune_block": inventory["manifest_header"]["progress"]["hotPruneBlockNum"],
    "cold_active_bytes": sum(v["bytes"] for v in inventory["active"].values()),
    "hot_sst_comparisons": sst_comparisons,
    "cold_sample_rows": sum(v["Rows"] for v in votes["Total"].values()),
    "cold_sample_prev_payload_bytes": payload,
    "cold_sample_prev_payload_percent": {k: percent(v["Payload"], payload) for k, v in votes["Total"].items()},
    "votes_index_share_of_vote_prev_payload_percent": percent(votes["Total"]["kv-latest/WitnessVoteState/VotesIndex"]["Payload"], cold["Total"]["kv-latest/WitnessVoteState"]["Payload"]),
    "cdc_actual_trios": compare(cdc["Before"], cdc["After"]),
    "cold_page_models": page_comparisons,
    "txindex_rows": tx["Rows"],
    "txindex_exclusive_end_block": previous_end,
    "txindex_layout_model": compare(tx["Bytes"], tx["CandidateBytes"]),
    "final_service_differs_from_census_service": final["production_service"] != proof["service_after"],
    "limits": ["hot full authenticated 5.715M cut; not current live hot state",
               "cold 16 ordinal samples: exact sample bytes, not a full-domain census",
               "SSTs are isolated identical-setting models, not production LSM attribution",
               "page and tx-index layouts are models; no production latency benchmark",
               "ancient/event/accessor section inventory is header-only; tx-index census verifies all CRCs/rows",
               "later production version changed externally; collection did not deploy/restart/prune"]
}
(ROOT / "analysis.json").write_text(json.dumps(analysis, indent=2) + "\n")
print(json.dumps(analysis, indent=2))

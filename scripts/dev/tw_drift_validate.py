#!/usr/bin/env python3
"""Localize the Nile-8,825,873 total_energy_weight drift (gtron ends 3,091 below
canonical). SCRATCH diagnostic — not committed.

Inputs (produced by a gtron re-sync with both env vars set):
  GTRON_TW_EVENTS -> events.csv : block,resource,owner,receiver,signed_amount
  GTRON_TW_TRACE  -> tw.csv     : block,total_energy_weight,total_net_weight

It independently re-derives the canonical weight using java-tron v4.0.1's floor
method, tracking per-account (and per owner->receiver delegated) frozen balances
from the FREEZE events, and for each UNFREEZE compares the amount gtron actually
unfroze (signed_amount) against the amount canonical *should* have unfrozen
(the tracked frozen total). The first mismatch is the bug; it also reports the
first block where the re-derived running weight diverges from gtron's logged
weight.

Usage: tw_drift_validate.py events.csv tw.csv
"""
import csv
import sys
from collections import defaultdict

TRX = 1_000_000


def floordiv(x):  # Go/Java integer division truncates toward zero
    return int(x / TRX) if x >= 0 else -int((-x) / TRX)


def main(events_path, tw_path):
    # canonical per-resource running weight, re-derived with v4.0.1 floor rules
    weight = {"BANDWIDTH": 0, "ENERGY": 0, "TRON_POWER": 0}
    # tracked stored-frozen so we can recompute the canonical unfreeze amount
    frozen = defaultdict(int)          # (owner, resource) -> stored frozen (non-delegated)
    deleg = defaultdict(int)           # (owner, receiver, resource) -> stored delegated frozen

    # gtron's logged per-block weight, for the running cross-check
    gtron_tw = {}
    if tw_path:
        with open(tw_path) as f:
            for row in csv.DictReader(f):
                gtron_tw[int(row["block"])] = int(row["total_energy_weight"])

    amount_mismatches = 0
    # re-derived canonical ENERGY weight as of the end of each event-bearing block
    rederived_energy_at_block = {}

    with open(events_path) as f:
        for row in csv.DictReader(f):
            block = int(row["block"])
            res = row["resource"]
            owner = row["owner"]
            recv = row["receiver"]
            amt = int(row["signed_amount"])
            delegated = recv != ""

            if amt >= 0:  # FREEZE
                if delegated:
                    deleg[(owner, recv, res)] += amt
                else:
                    frozen[(owner, res)] += amt
                weight[res] += floordiv(amt)
            else:  # UNFREEZE: gtron unfroze |amt|; canonical should unfreeze the tracked total
                gtron_amt = -amt
                if delegated:
                    key = (owner, recv, res)
                    expected = deleg.get(key, 0)
                    deleg[key] = 0
                else:
                    key = (owner, res)
                    expected = frozen.get(key, 0)
                    frozen[key] = 0
                if expected != gtron_amt and amount_mismatches < 50:
                    print(f"AMOUNT MISMATCH @block {block} {res} owner={owner} "
                          f"recv={recv or '-'}: gtron unfroze {gtron_amt} but canonical "
                          f"tracked {expected} (delta {gtron_amt - expected})")
                    amount_mismatches += 1
                # re-derive canonical weight using the EXPECTED (canonical) amount
                weight[res] -= floordiv(expected)
            rederived_energy_at_block[block] = weight["ENERGY"]

    # Universal signal: first block where canonical re-derived ENERGY weight
    # disagrees with gtron's logged value. Walk blocks in order, carrying the
    # last re-derived value forward across no-event blocks.
    first_weight_divergence = None
    carry = 0
    for b in sorted(set(list(rederived_energy_at_block.keys()) + list(gtron_tw.keys()))):
        if b in rederived_energy_at_block:
            carry = rederived_energy_at_block[b]
        if b in gtron_tw and gtron_tw[b] != carry:
            first_weight_divergence = (b, carry, gtron_tw[b])
            break

    print("\n--- summary ---")
    print(f"re-derived canonical ENERGY weight (final): {weight['ENERGY']}")
    print(f"re-derived canonical NET    weight (final): {weight['BANDWIDTH']}")
    if gtron_tw:
        tail = max(gtron_tw)
        print(f"gtron logged ENERGY weight @block {tail}: {gtron_tw[tail]}")
        print(f"final delta (canonical - gtron): {weight['ENERGY'] - gtron_tw.get(tail, 0)}")
    print(f"unfreeze amount mismatches: {amount_mismatches}")
    if first_weight_divergence:
        b, canon, g = first_weight_divergence
        print(f"FIRST per-block ENERGY weight divergence @block {b}: canonical {canon} vs gtron {g}")
    else:
        print("no per-block ENERGY weight divergence detected (check amount mismatches above, "
              "or the bug is outside V1 freeze/unfreeze events)")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)

#!/usr/bin/env python3
"""Fail closed on go-tron speculative execution rollout metrics."""

import argparse
import json
import sys
import urllib.parse
import urllib.request


COUNTER = "count"
GAUGE = "value"


def metric_value(metrics, name):
    entry = metrics.get(name)
    if not isinstance(entry, dict):
        raise ValueError(f"missing metric {name!r}")
    field = COUNTER if COUNTER in entry else GAUGE if GAUGE in entry else None
    if field is None or not isinstance(entry[field], (int, float)):
        raise ValueError(f"metric {name!r} has no numeric count/value")
    return entry[field]


def audit_metrics(
    metrics,
    require_transfer_publications=False,
    require_vm_publications=False,
    min_transfer_publications=0,
    min_vm_publications=0,
):
    issues = []

    def read(name):
        try:
            return metric_value(metrics, name)
        except ValueError as exc:
            issues.append(str(exc))
            return None

    must_be_zero = [
        "core/mainnet_state_repair/create_transfer_failure",
        "core/mainnet_state_repair/parallel_vm_missed_payment",
        "core/mainnet_state_repair/cost_missed_reward",
        "core/mainnet_state_repair/wink_missing_runtime",
        "core/speculative_execution/safety_fallbacks",
        "core/speculative_execution/safety_disabled",
        "core/speculative_execution/safety_persisted",
        "core/speculative_execution/safety_persist_errors",
        "core/parallel_transfer/errors",
        "core/parallel_transfer/sender_retry/errors",
        "core/parallel_transfer/balance_oracle/mismatches",
        "core/parallel_transfer/balance_oracle/errors",
        "core/parallel_transfer/serial_verify/info_mismatches",
        "core/parallel_transfer/serial_verify/write_set_mismatches",
        "core/parallel_transfer/serial_verify/balance_trace_mismatches",
        "core/parallel_transfer/serial_verify/restore_mismatches",
        "core/parallel_transfer/serial_verify/errors",
        "core/parallel_transfer/write_seal/mismatches",
        "core/parallel_transfer/publish_audit/mismatches",
        "core/parallel_transfer/publish_audit/errors",
        "core/parallel_vm/serial_verify/info_mismatches",
        "core/parallel_vm/serial_verify/write_set_mismatches",
        "core/parallel_vm/serial_verify/balance_trace_mismatches",
        "core/parallel_vm/serial_verify/restore_mismatches",
        "core/parallel_vm/serial_verify/errors",
        "core/parallel_vm/dual_oracle/info_mismatches",
        "core/parallel_vm/dual_oracle/write_set_mismatches",
        "core/parallel_vm/dual_oracle/balance_trace_mismatches",
        "core/parallel_vm/dual_oracle/errors",
        "core/parallel_vm/write_seal/mismatches",
        "core/parallel_vm/publish_audit/mismatches",
        "core/parallel_vm/publish_audit/errors",
        "core/parallel_vm/errors",
        "core/parallel_vm/retry/async_publish/errors",
    ]
    for name in must_be_zero:
        value = read(name)
        if value is not None and value != 0:
            issues.append(f"{name}={value}, want 0")

    # Worker and canonical serial execution can legitimately materialize
    # different cache/system-account reads. These are diagnostic only because
    # the canonical read set and result are the sole publication payload.
    read("core/parallel_transfer/serial_verify/read_set_differences")
    read("core/parallel_vm/serial_verify/read_set_differences")
    read("core/parallel_vm/dual_oracle/read_set_differences")

    balance_candidates = read("core/parallel_transfer/balance_oracle/candidates")
    balance_matches = read("core/parallel_transfer/balance_oracle/matches")
    balance_fallbacks = read("core/parallel_transfer/balance_oracle/fallbacks")
    balance_mismatches = read("core/parallel_transfer/balance_oracle/mismatches")
    balance_errors = read("core/parallel_transfer/balance_oracle/errors")
    if None not in (balance_candidates, balance_matches, balance_fallbacks, balance_mismatches, balance_errors):
        outcomes = balance_matches + balance_fallbacks + balance_mismatches + balance_errors
        if balance_candidates != outcomes:
            issues.append(
                "transfer balance-oracle accounting does not close: "
                f"candidates={balance_candidates} outcomes={outcomes}"
            )

    for family in ("parallel_transfer", "parallel_vm"):
        for audit in ("serial_verify", "write_seal", "publish_audit"):
            candidates = read(f"core/{family}/{audit}/candidates")
            matches = read(f"core/{family}/{audit}/matches")
            if candidates is not None and matches is not None and candidates != matches:
                issues.append(
                    f"core/{family}/{audit} candidates/matches={candidates}/{matches}, want equality"
                )

    vm_dual_candidates = read("core/parallel_vm/dual_oracle/candidates")
    vm_dual_matches = read("core/parallel_vm/dual_oracle/matches")
    if None not in (vm_dual_candidates, vm_dual_matches) and vm_dual_candidates != vm_dual_matches:
        issues.append(
            "core/parallel_vm/dual_oracle candidates/matches="
            f"{vm_dual_candidates}/{vm_dual_matches}, want equality"
        )

    transfer_enabled = read("core/parallel_transfer/enabled")
    vm_enabled = read("core/parallel_vm/enabled")
    transfer_published = read("core/parallel_transfer/published")
    vm_published = read("core/parallel_vm/published")
    transfer_publish_audits = read("core/parallel_transfer/publish_audit/candidates")
    vm_publish_audits = read("core/parallel_vm/publish_audit/candidates")
    transfer_serial_matches = read("core/parallel_transfer/serial_verify/matches")
    vm_serial_matches = read("core/parallel_vm/serial_verify/matches")
    vm_dual_matches = read("core/parallel_vm/dual_oracle/matches")
    transfer_seal_matches = read("core/parallel_transfer/write_seal/matches")
    vm_seal_matches = read("core/parallel_vm/write_seal/matches")
    vm_block_energy_published = read("core/parallel_vm/block_energy/published")
    if None not in (transfer_published, balance_matches) and transfer_published != balance_matches:
        issues.append(
            "parallel Transfer publications are not one-for-one balance-verified: "
            f"published={transfer_published} balance_matches={balance_matches}"
        )
    if None not in (transfer_published, transfer_serial_matches) and transfer_published != transfer_serial_matches:
        issues.append(
            "parallel Transfer publications are not one-for-one serial-verified: "
            f"published={transfer_published} serial_matches={transfer_serial_matches}"
        )
    if None not in (vm_published, vm_serial_matches) and vm_published != vm_serial_matches:
        issues.append(
            "parallel VM publications are not one-for-one serial-verified: "
            f"published={vm_published} serial_matches={vm_serial_matches}"
        )
    if None not in (vm_published, vm_dual_matches) and vm_published != vm_dual_matches:
        issues.append(
            "parallel VM publications are not one-for-one dual-oracle verified: "
            f"published={vm_published} dual_oracle_matches={vm_dual_matches}"
        )
    if None not in (transfer_published, transfer_seal_matches) and transfer_published != transfer_seal_matches:
        issues.append(
            "parallel Transfer publications are not one-for-one WriteSet-sealed: "
            f"published={transfer_published} seal_matches={transfer_seal_matches}"
        )
    if None not in (vm_published, vm_seal_matches) and vm_published != vm_seal_matches:
        issues.append(
            "parallel VM publications are not one-for-one WriteSet-sealed: "
            f"published={vm_published} seal_matches={vm_seal_matches}"
        )
    if None not in (transfer_published, transfer_publish_audits) and transfer_published != transfer_publish_audits:
        issues.append(
            "parallel Transfer publications are not one-for-one audited: "
            f"published={transfer_published} audits={transfer_publish_audits}"
        )
    if None not in (vm_published, vm_publish_audits) and vm_published != vm_publish_audits:
        issues.append(
            "parallel VM publications are not one-for-one audited: "
            f"published={vm_published} audits={vm_publish_audits}"
        )
    if None not in (vm_published, vm_block_energy_published) and vm_published != vm_block_energy_published:
        issues.append(
            "parallel VM publications are not one-for-one block-energy settled: "
            f"published={vm_published} block_energy={vm_block_energy_published}"
        )

    transfer_minimum = max(min_transfer_publications, int(require_transfer_publications))
    vm_minimum = max(min_vm_publications, int(require_vm_publications))
    if transfer_minimum:
        if transfer_enabled is not None and transfer_enabled != 1:
            issues.append(f"parallel Transfer gate requested an enabled publisher but enabled={transfer_enabled}")
        if transfer_published is not None and transfer_published < transfer_minimum:
            issues.append(
                "parallel Transfer gate has insufficient activity: "
                f"published={transfer_published}, want >= {transfer_minimum}"
            )
    if vm_minimum:
        if vm_enabled is not None and vm_enabled != 1:
            issues.append(f"parallel VM gate requested an enabled publisher but enabled={vm_enabled}")
        if vm_published is not None and vm_published < vm_minimum:
            issues.append(
                "parallel VM gate has insufficient activity: "
                f"published={vm_published}, want >= {vm_minimum}"
            )
    return issues


def fetch_metrics(url, timeout):
    separator = "&" if "?" in url else "?"
    request_url = url + separator + urllib.parse.urlencode({"prefix": "core/"})
    with urllib.request.urlopen(request_url, timeout=timeout) as response:
        if response.status != 200:
            raise RuntimeError(f"metrics HTTP status {response.status}")
        payload = json.load(response)
    metrics = payload.get("metrics")
    if not isinstance(metrics, dict):
        raise RuntimeError("metrics response has no object field 'metrics'")
    return metrics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--metrics-url",
        default="http://127.0.0.1:6062/debug/pprof/metrics",
        help="gtron debug JSON metrics endpoint",
    )
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--require-transfer-publications", action="store_true")
    parser.add_argument("--require-vm-publications", action="store_true")
    parser.add_argument("--min-transfer-publications", type=int, default=0)
    parser.add_argument("--min-vm-publications", type=int, default=0)
    args = parser.parse_args()
    if args.min_transfer_publications < 0 or args.min_vm_publications < 0:
        parser.error("minimum publication counts must be non-negative")
    try:
        metrics = fetch_metrics(args.metrics_url, args.timeout)
        issues = audit_metrics(
            metrics,
            require_transfer_publications=args.require_transfer_publications,
            require_vm_publications=args.require_vm_publications,
            min_transfer_publications=args.min_transfer_publications,
            min_vm_publications=args.min_vm_publications,
        )
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        print(f"parallel execution release gate: ERROR: {exc}", file=sys.stderr)
        return 2
    if issues:
        print("parallel execution release gate: FAIL", file=sys.stderr)
        for issue in issues:
            print(f"- {issue}", file=sys.stderr)
        return 1
    print("parallel execution release gate: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Validate state_prefetch_benchmark.sh raw Go benchmark output."""

import argparse
import re
import statistics
import sys
from pathlib import Path


OFF_VARIANT = "prefetch=off"
DEFAULT_REQUIRED_CASES = (
    "LightTRX_HeavyState",
    "LightTRX_ColdState",
    "HeavyTRX_HeavyState",
    "HeavyTRX_ColdState",
)
DEFAULT_LIGHT_CASES = (
    "LightTRX_HeavyState",
    "LightTRX_ColdState",
)
DEFAULT_HEAVY_CASES = ("HeavyTRX_ColdState",)

BENCH_RE = re.compile(
    r"^BenchmarkProcessBlock_"
    r"(?P<case>[A-Za-z0-9_]+)/"
    r"(?P<variant>[^ \t]+)-\d+\s+"
    r"(?P<iters>\d+)\s+"
    r"(?P<ns>\d+(?:\.\d+)?)\s+ns/op"
    r"(?:\s+(?P<bytes>\d+)\s+B/op)?"
    r"(?:\s+(?P<allocs>\d+)\s+allocs/op)?"
    r"\s*$"
)


def split_csv(values):
    out = []
    for value in values:
        for item in value.split(","):
            item = item.strip()
            if item:
                out.append(item)
    return out


def load_benchmarks(path):
    samples = {}
    issues = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        return {}, [f"read {path}: {exc}"]

    for line_no, line in enumerate(lines, 1):
        if not line.startswith("BenchmarkProcessBlock_"):
            continue
        match = BENCH_RE.match(line)
        if not match:
            issues.append(f"{path}:{line_no}: cannot parse benchmark line: {line}")
            continue
        case = match.group("case")
        variant = match.group("variant")
        iters = int(match.group("iters"))
        if iters <= 0:
            issues.append(f"{path}:{line_no}: benchmark iterations={iters}, want positive integer")
            continue
        ns = float(match.group("ns"))
        if ns <= 0:
            issues.append(f"{path}:{line_no}: ns/op={ns:g}, want positive value")
            continue
        sample = samples.setdefault(case, {}).setdefault(
            variant,
            {"ns": [], "bytes": [], "allocs": []},
        )
        sample["ns"].append(ns)
        if match.group("bytes") is not None:
            sample["bytes"].append(int(match.group("bytes")))
        if match.group("allocs") is not None:
            sample["allocs"].append(int(match.group("allocs")))

    if not samples and not issues:
        issues.append(f"{path}: no ProcessBlock prefetch benchmark rows found")
    return samples, issues


def median_metric(samples, case, variant, metric):
    values = samples.get(case, {}).get(variant, {}).get(metric)
    if not values:
        return None
    return statistics.median(values)


def sample_count(samples, case, variant, metric):
    return len(samples.get(case, {}).get(variant, {}).get(metric) or [])


def ratio(got, base):
    if base is None or base <= 0 or got is None:
        return None
    return (got - base) / base


def overhead_ratio(got, base):
    if base is None or got is None:
        return None
    if base == 0:
        return 0.0 if got == 0 else float("inf")
    return (got - base) / base


def percent(value):
    if value is None:
        return "n/a"
    return f"{value * 100:.2f}%"


def candidate_variants(samples, explicit):
    if explicit:
        return explicit
    variants = set()
    for by_variant in samples.values():
        variants.update(by_variant)
    return sorted(variant for variant in variants if is_prefetch_on_variant(variant))


def is_prefetch_on_variant(variant):
    return variant.startswith("prefetch=on")


def check_required(samples, required_cases, min_samples):
    issues = []
    fatal = False
    for case in required_cases:
        variants = samples.get(case)
        if not variants:
            issues.append(f"missing benchmark case {case}")
            fatal = True
            continue
        if OFF_VARIANT not in variants:
            issues.append(f"missing {OFF_VARIANT} baseline for {case}")
            fatal = True
            continue
        count = sample_count(samples, case, OFF_VARIANT, "ns")
        if count < min_samples:
            issues.append(
                f"{OFF_VARIANT} {case} ns/op samples={count}, want >= {min_samples}"
            )
    return issues, fatal


def evaluate_variant(
    samples,
    variant,
    required_cases,
    heavy_cases,
    light_cases,
    min_heavy,
    max_light,
    max_heavy_hot,
    max_bytes_overhead,
    max_allocs_overhead,
    min_samples,
):
    issues = []
    heavy_improvements = []
    light_overheads = []
    heavy_hot_overhead = None
    bytes_overheads = []
    allocs_overheads = []

    for case in required_cases:
        if median_metric(samples, case, variant, "ns") is None:
            issues.append(f"{variant} missing benchmark case {case}")
            continue
        count = sample_count(samples, case, variant, "ns")
        if count < min_samples:
            issues.append(f"{variant} {case} ns/op samples={count}, want >= {min_samples}")

    for case in heavy_cases:
        base = median_metric(samples, case, OFF_VARIANT, "ns")
        got = median_metric(samples, case, variant, "ns")
        if got is None:
            issues.append(f"{variant} missing benchmark case {case}")
            continue
        improvement = -ratio(got, base)
        heavy_improvements.append(improvement)
        if improvement is None or improvement < min_heavy:
            issues.append(
                f"{variant} {case} improvement={percent(improvement)}, "
                f"want >= {percent(min_heavy)}"
            )

    for case in light_cases:
        base = median_metric(samples, case, OFF_VARIANT, "ns")
        got = median_metric(samples, case, variant, "ns")
        if got is None:
            issues.append(f"{variant} missing benchmark case {case}")
            continue
        overhead = ratio(got, base)
        light_overheads.append(overhead)
        if overhead is None or overhead > max_light:
            issues.append(
                f"{variant} {case} overhead={percent(overhead)}, "
                f"want <= {percent(max_light)}"
            )

    if max_heavy_hot is not None:
        base = median_metric(samples, "HeavyTRX_HeavyState", OFF_VARIANT, "ns")
        got = median_metric(samples, "HeavyTRX_HeavyState", variant, "ns")
        if got is None:
            issues.append(f"{variant} missing benchmark case HeavyTRX_HeavyState")
        else:
            heavy_hot_overhead = ratio(got, base)
            if heavy_hot_overhead is None or heavy_hot_overhead > max_heavy_hot:
                issues.append(
                    f"{variant} HeavyTRX_HeavyState overhead={percent(heavy_hot_overhead)}, "
                    f"want <= {percent(max_heavy_hot)}"
                )

    for metric, label, max_allowed, out in (
        ("bytes", "B/op", max_bytes_overhead, bytes_overheads),
        ("allocs", "allocs/op", max_allocs_overhead, allocs_overheads),
    ):
        if max_allowed is None:
            continue
        for case in required_cases:
            base = median_metric(samples, case, OFF_VARIANT, metric)
            got = median_metric(samples, case, variant, metric)
            if base is None:
                issues.append(f"missing {label} baseline for {case}")
                continue
            base_count = sample_count(samples, case, OFF_VARIANT, metric)
            if base_count < min_samples:
                issues.append(
                    f"{OFF_VARIANT} {case} {label} samples={base_count}, "
                    f"want >= {min_samples}"
                )
            if got is None:
                issues.append(f"{variant} missing {label} sample for {case}")
                continue
            got_count = sample_count(samples, case, variant, metric)
            if got_count < min_samples:
                issues.append(
                    f"{variant} {case} {label} samples={got_count}, want >= {min_samples}"
                )
            overhead = overhead_ratio(got, base)
            out.append(overhead)
            if overhead is None or overhead > max_allowed:
                issues.append(
                    f"{variant} {case} {label} overhead={percent(overhead)}, "
                    f"want <= {percent(max_allowed)}"
                )

    score_values = [value for value in heavy_improvements if value is not None]
    score = statistics.mean(score_values) if score_values else float("-inf")
    return {
        "variant": variant,
        "issues": issues,
        "score": score,
        "heavyMinImprovement": min((v for v in heavy_improvements if v is not None), default=None),
        "lightMaxOverhead": max((v for v in light_overheads if v is not None), default=None),
        "heavyHotOverhead": heavy_hot_overhead,
        "bytesMaxOverhead": max((v for v in bytes_overheads if v is not None), default=None),
        "allocsMaxOverhead": max((v for v in allocs_overheads if v is not None), default=None),
    }


def check_benchmarks(samples, args):
    required_cases = list(DEFAULT_REQUIRED_CASES)
    for case in args.heavy_case + args.light_case + args.require_case:
        if case not in required_cases:
            required_cases.append(case)
    issues, fatal = check_required(samples, required_cases, args.min_samples)
    if fatal:
        return None, issues, []

    variants = candidate_variants(samples, args.variant)
    invalid_variants = [variant for variant in variants if not is_prefetch_on_variant(variant)]
    if invalid_variants:
        return None, [
            f"{variant} is not a prefetch=on benchmark variant"
            for variant in invalid_variants
        ], []
    if not variants:
        return None, ["no prefetch=on benchmark variants found"], []

    results = [
        evaluate_variant(
            samples,
            variant,
            required_cases,
            args.heavy_case,
            args.light_case,
            args.min_heavy_improvement,
            args.max_light_overhead,
            args.max_heavy_hot_overhead,
            args.max_bytes_overhead,
            args.max_allocs_overhead,
            args.min_samples,
        )
        for variant in variants
    ]
    passing = [result for result in results if not result["issues"]]
    if passing and not issues:
        return max(passing, key=lambda result: result["score"]), [], results

    for result in results:
        issues.extend(result["issues"])
    return None, issues, results


def build_parser():
    parser = argparse.ArgumentParser(
        description="Validate state prefetch benchmark output against rollout gates.",
    )
    parser.add_argument("benchmark", type=Path, help="raw benchmark.txt from state_prefetch_benchmark.sh")
    parser.add_argument(
        "--variant",
        action="append",
        default=[],
        help="specific prefetch variant to validate, e.g. prefetch=on_workers=4_lookahead=8; repeatable",
    )
    parser.add_argument(
        "--heavy-case",
        action="append",
        default=[],
        help="benchmark case that must improve; default: HeavyTRX_ColdState",
    )
    parser.add_argument(
        "--light-case",
        action="append",
        default=[],
        help="benchmark case that must stay within light overhead; default: both LightTRX cases",
    )
    parser.add_argument(
        "--require-case",
        action="append",
        default=[],
        help="additional benchmark case that must be present with prefetch=off baseline; repeatable",
    )
    parser.add_argument(
        "--min-heavy-improvement",
        type=float,
        default=0.10,
        help="minimum required heavy-case speedup as a ratio (default: 0.10)",
    )
    parser.add_argument(
        "--max-light-overhead",
        type=float,
        default=0.01,
        help="maximum allowed light-case slowdown as a ratio (default: 0.01)",
    )
    parser.add_argument(
        "--max-heavy-hot-overhead",
        type=float,
        default=None,
        help="optional maximum HeavyTRX_HeavyState slowdown ratio",
    )
    parser.add_argument(
        "--max-bytes-overhead",
        type=float,
        default=None,
        help="optional maximum B/op overhead ratio across all required cases",
    )
    parser.add_argument(
        "--max-allocs-overhead",
        type=float,
        default=None,
        help="optional maximum allocs/op overhead ratio across all required cases",
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        default=5,
        help=(
            "minimum repeated benchmark samples required for each baseline and "
            "candidate case (default: 5, matching state_prefetch_benchmark.sh)"
        ),
    )
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.min_samples < 1:
        parser.error("--min-samples must be >= 1")
    if not args.heavy_case:
        args.heavy_case = list(DEFAULT_HEAVY_CASES)
    else:
        args.heavy_case = split_csv(args.heavy_case)
    if not args.light_case:
        args.light_case = list(DEFAULT_LIGHT_CASES)
    else:
        args.light_case = split_csv(args.light_case)
    args.require_case = split_csv(args.require_case)

    samples, issues = load_benchmarks(args.benchmark)
    if not issues:
        selected, issues, _ = check_benchmarks(samples, args)
    else:
        selected = None

    if issues:
        print("state prefetch benchmark acceptance: failed", file=sys.stderr)
        for issue in issues:
            print(f"- {issue}", file=sys.stderr)
        return 1

    print(
        "state prefetch benchmark acceptance: ok "
        f"variant={selected['variant']} "
        f"heavyMinImprovement={percent(selected['heavyMinImprovement'])} "
        f"lightMaxOverhead={percent(selected['lightMaxOverhead'])}"
    )
    if selected["heavyHotOverhead"] is not None:
        print(f"heavyHotOverhead={percent(selected['heavyHotOverhead'])}")
    if selected["bytesMaxOverhead"] is not None:
        print(f"bytesMaxOverhead={percent(selected['bytesMaxOverhead'])}")
    if selected["allocsMaxOverhead"] is not None:
        print(f"allocsMaxOverhead={percent(selected['allocsMaxOverhead'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

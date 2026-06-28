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
    r"(?P<ns>[0-9.]+)\s+ns/op"
    r"(?:\s+(?P<bytes>[0-9.]+)\s+B/op)?"
    r"(?:\s+(?P<allocs>[0-9.]+)\s+allocs/op)?"
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
        ns = float(match.group("ns"))
        samples.setdefault(case, {}).setdefault(variant, []).append(ns)

    if not samples and not issues:
        issues.append(f"{path}: no ProcessBlock prefetch benchmark rows found")
    return samples, issues


def median_ns(samples, case, variant):
    values = samples.get(case, {}).get(variant)
    if not values:
        return None
    return statistics.median(values)


def ratio(got, base):
    if base is None or base <= 0 or got is None:
        return None
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
    variants.discard(OFF_VARIANT)
    return sorted(variants)


def check_required(samples, required_cases):
    issues = []
    for case in required_cases:
        variants = samples.get(case)
        if not variants:
            issues.append(f"missing benchmark case {case}")
            continue
        if OFF_VARIANT not in variants:
            issues.append(f"missing {OFF_VARIANT} baseline for {case}")
    return issues


def evaluate_variant(samples, variant, required_cases, heavy_cases, light_cases, min_heavy, max_light, max_heavy_hot):
    issues = []
    heavy_improvements = []
    light_overheads = []
    heavy_hot_overhead = None

    for case in required_cases:
        if median_ns(samples, case, variant) is None:
            issues.append(f"{variant} missing benchmark case {case}")

    for case in heavy_cases:
        base = median_ns(samples, case, OFF_VARIANT)
        got = median_ns(samples, case, variant)
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
        base = median_ns(samples, case, OFF_VARIANT)
        got = median_ns(samples, case, variant)
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
        base = median_ns(samples, "HeavyTRX_HeavyState", OFF_VARIANT)
        got = median_ns(samples, "HeavyTRX_HeavyState", variant)
        if got is None:
            issues.append(f"{variant} missing benchmark case HeavyTRX_HeavyState")
        else:
            heavy_hot_overhead = ratio(got, base)
            if heavy_hot_overhead is None or heavy_hot_overhead > max_heavy_hot:
                issues.append(
                    f"{variant} HeavyTRX_HeavyState overhead={percent(heavy_hot_overhead)}, "
                    f"want <= {percent(max_heavy_hot)}"
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
    }


def check_benchmarks(samples, args):
    required_cases = list(DEFAULT_REQUIRED_CASES)
    for case in args.heavy_case + args.light_case + args.require_case:
        if case not in required_cases:
            required_cases.append(case)
    issues = check_required(samples, required_cases)
    if issues:
        return None, issues, []

    variants = candidate_variants(samples, args.variant)
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
        )
        for variant in variants
    ]
    passing = [result for result in results if not result["issues"]]
    if passing:
        return max(passing, key=lambda result: result["score"]), [], results

    issues = []
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
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

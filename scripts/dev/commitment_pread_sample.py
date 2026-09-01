#!/usr/bin/env python3
"""Sample commitment random-read metrics into interval-oriented JSONL.

Run this on the node so the default loopback debug and wallet URLs do not need
authentication or proxy support. A single invocation appends one row; --samples
and --interval can keep the sampler alive for a bounded or continuous capture.
"""

import argparse
import datetime as dt
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path


METRIC_PREFIXES = (
    "blockbuffer/commitment_parent/",
    "state/commitment/",
    "cache/",
    "filter/",
    "iter/",
)

COUNTER_METRICS = (
    "blockbuffer/commitment_parent/overlay/resolved",
    "blockbuffer/commitment_parent/cache/resolved",
    "blockbuffer/commitment_parent/durable/reads",
    "blockbuffer/commitment_parent/durable/hits",
    "blockbuffer/commitment_parent/trunk/cache_resolved",
    "blockbuffer/commitment_parent/trunk/durable_reads",
    "blockbuffer/commitment_parent/window/cache_resolved",
    "blockbuffer/commitment_parent/depth_5_8/cache_resolved",
    "blockbuffer/commitment_parent/depth_5_8/durable_reads",
    "blockbuffer/commitment_parent/depth_9_16/cache_resolved",
    "blockbuffer/commitment_parent/depth_9_16/durable_reads",
    "blockbuffer/commitment_parent/depth_17_32/cache_resolved",
    "blockbuffer/commitment_parent/depth_17_32/durable_reads",
    "blockbuffer/commitment_parent/depth_33_plus/cache_resolved",
    "blockbuffer/commitment_parent/depth_33_plus/durable_reads",
    "blockbuffer/commitment_parent/prefetch/planned",
    "blockbuffer/commitment_parent/prefetch/overlay_resolved",
    "blockbuffer/commitment_parent/prefetch/cache_resolved",
    "blockbuffer/commitment_parent/prefetch/durable_reads",
    "blockbuffer/commitment_parent/prefetch/durable_hits",
    "blockbuffer/commitment_parent/prefetch/useful_hits",
    "blockbuffer/commitment_parent/prefetch/depth_5/planned",
    "blockbuffer/commitment_parent/prefetch/depth_5/cache_resolved",
    "blockbuffer/commitment_parent/prefetch/depth_5/durable_reads",
    "blockbuffer/commitment_parent/prefetch/depth_5/useful_hits",
    "blockbuffer/commitment_parent/prefetch/depth_6_plus/planned",
    "blockbuffer/commitment_parent/prefetch/depth_6_plus/cache_resolved",
    "blockbuffer/commitment_parent/prefetch/depth_6_plus/durable_reads",
    "blockbuffer/commitment_parent/prefetch/depth_6_plus/useful_hits",
    "blockbuffer/commitment_parent/durable_publish_races",
    "blockbuffer/commitment_parent/durable_publish_races/prefetch",
    "blockbuffer/commitment_parent/durable_publish_races/foreground",
    "blockbuffer/commitment_parent/prefetch/unused_capacity_evicted",
    "blockbuffer/commitment_parent/prefetch/unused_capacity_evicted_bytes",
    "state/commitment/fold/wall_nanos",
    "state/commitment/pipeline/jobs",
    "state/commitment/pipeline/prefetch_errors",
    "state/commitment/pipeline/prefetch_critical/planned",
    "state/commitment/pipeline/prefetch_critical/wall_nanos",
    "state/commitment/pipeline/prefetch_critical/wait_calls",
    "state/commitment/pipeline/prefetch_critical/wait_nanos",
    "state/commitment/pipeline/prefetch_lookahead/planned",
    "state/commitment/pipeline/prefetch_lookahead/capped_lanes",
    "state/commitment/pipeline/prefetch_lookahead/wall_nanos",
)

GAUGE_METRICS = (
    "state/commitment/pipeline/prefetch_critical/depth",
    "state/commitment/pipeline/prefetch_lookahead/depth",
    "state/commitment/pipeline/prefetch_lookahead/limit_per_lane",
    "cache/block/hit",
    "cache/block/miss",
    "cache/table/hit",
    "cache/table/miss",
    "filter/hit",
    "filter/miss",
    "iter/count",
)

ALL_METRICS = COUNTER_METRICS + GAUGE_METRICS


def request_json(url, timeout):
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status} from {url}")
        return json.load(response)


def with_prefix(url, prefix):
    parts = urllib.parse.urlsplit(url)
    query = [(key, value) for key, value in urllib.parse.parse_qsl(parts.query) if key != "prefix"]
    query.append(("prefix", prefix))
    return urllib.parse.urlunsplit(parts._replace(query=urllib.parse.urlencode(query)))


def normalize_metrics(payload):
    if not isinstance(payload, dict):
        raise RuntimeError("metrics response is not a JSON object")
    raw = payload.get("metrics")
    if isinstance(raw, dict):
        return {str(name): values for name, values in raw.items() if isinstance(values, dict)}
    # Accept the older operator-script fixture shape as well as the live map.
    if isinstance(raw, list):
        result = {}
        for metric in raw:
            if not isinstance(metric, dict):
                continue
            name = metric.get("name")
            values = metric.get("values")
            if name and isinstance(values, dict):
                result[str(name)] = values
        return result
    raise RuntimeError("metrics response has no object or list field 'metrics'")


def fetch_metrics(url, timeout):
    merged = {}
    for prefix in METRIC_PREFIXES:
        merged.update(normalize_metrics(request_json(with_prefix(url, prefix), timeout)))
    return merged


def metric_scalar(values):
    if not values:
        return None
    for field in ("count", "value"):
        value = values.get(field)
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            return value
    return None


def select_metrics(metrics):
    return {name: metric_scalar(metrics.get(name)) for name in ALL_METRICS}


def wallet_height(wallet_url, timeout):
    payload = request_json(wallet_url.rstrip("/") + "/wallet/getnowblock", timeout)
    if not isinstance(payload, dict):
        return None
    header = payload.get("block_header")
    if not isinstance(header, dict):
        return None
    raw = header.get("raw_data")
    if not isinstance(raw, dict):
        return None
    number = raw.get("number")
    return int(number) if isinstance(number, (int, float)) and not isinstance(number, bool) else None


def load_previous(path):
    if not path:
        return None
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return None
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            return row
    return None


def safe_ratio(numerator, denominator):
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return float(numerator) / float(denominator)


def positive_sum(*values):
    if any(value is None for value in values):
        return None
    return sum(value for value in values if value is not None)


def build_row(now, height, current, previous):
    previous_metrics = previous.get("metrics", {}) if isinstance(previous, dict) else {}
    if not isinstance(previous_metrics, dict):
        previous_metrics = {}
    deltas = {}
    resets = []
    for name in COUNTER_METRICS:
        value = current.get(name)
        old = previous_metrics.get(name)
        if value is None or not isinstance(old, (int, float)) or isinstance(old, bool):
            deltas[name] = None
        elif value < old:
            deltas[name] = None
            resets.append(name)
        else:
            deltas[name] = value - old
    # Pebble exports cumulative cache/filter totals as gauges, so treat only
    # those gauges as monotonic interval sources. Configuration gauges remain
    # point-in-time values.
    for name in GAUGE_METRICS:
        if not name.startswith(("cache/", "filter/")):
            continue
        value = current.get(name)
        old = previous_metrics.get(name)
        if value is None or not isinstance(old, (int, float)) or isinstance(old, bool) or value < old:
            deltas[name] = None
            if value is not None and isinstance(old, (int, float)) and not isinstance(old, bool) and value < old:
                resets.append(name)
        else:
            deltas[name] = value - old

    previous_unix = previous.get("unix") if isinstance(previous, dict) else None
    interval = now - float(previous_unix) if isinstance(previous_unix, (int, float)) and now >= previous_unix else None
    previous_height = previous.get("height") if isinstance(previous, dict) else None
    interval_blocks = None
    if height is not None and isinstance(previous_height, int) and height >= previous_height:
        interval_blocks = height - previous_height

    foreground_total = positive_sum(
        deltas.get("blockbuffer/commitment_parent/overlay/resolved"),
        deltas.get("blockbuffer/commitment_parent/cache/resolved"),
        deltas.get("blockbuffer/commitment_parent/durable/reads"),
    )
    total_durable = positive_sum(
        deltas.get("blockbuffer/commitment_parent/durable/reads"),
        deltas.get("blockbuffer/commitment_parent/prefetch/durable_reads"),
    )
    block_cache_total = positive_sum(deltas.get("cache/block/hit"), deltas.get("cache/block/miss"))
    table_cache_total = positive_sum(deltas.get("cache/table/hit"), deltas.get("cache/table/miss"))
    filter_total = positive_sum(deltas.get("filter/hit"), deltas.get("filter/miss"))

    foreground_depth_ratios = {}
    for bucket in ("depth_5_8", "depth_9_16", "depth_17_32", "depth_33_plus"):
        cached = deltas.get(f"blockbuffer/commitment_parent/{bucket}/cache_resolved")
        durable = deltas.get(f"blockbuffer/commitment_parent/{bucket}/durable_reads")
        foreground_depth_ratios[bucket] = safe_ratio(durable, positive_sum(cached, durable))

    analysis = {
        "foregroundDurableRatio": safe_ratio(
            deltas.get("blockbuffer/commitment_parent/durable/reads"), foreground_total
        ),
        "prefetchDurableRatio": safe_ratio(
            deltas.get("blockbuffer/commitment_parent/prefetch/durable_reads"),
            deltas.get("blockbuffer/commitment_parent/prefetch/planned"),
        ),
        "prefetchUsefulRatio": safe_ratio(
            deltas.get("blockbuffer/commitment_parent/prefetch/useful_hits"),
            deltas.get("blockbuffer/commitment_parent/prefetch/durable_reads"),
        ),
        "depth5UsefulRatio": safe_ratio(
            deltas.get("blockbuffer/commitment_parent/prefetch/depth_5/useful_hits"),
            deltas.get("blockbuffer/commitment_parent/prefetch/depth_5/durable_reads"),
        ),
        "depth6PlusUsefulRatio": safe_ratio(
            deltas.get("blockbuffer/commitment_parent/prefetch/depth_6_plus/useful_hits"),
            deltas.get("blockbuffer/commitment_parent/prefetch/depth_6_plus/durable_reads"),
        ),
        # Eviction may lag admission across interval boundaries, so this is a
        # rate per durable prefetch rather than a bounded cohort hit ratio.
        "unusedPrefetchCapacityEvictionsPerDurableRead": safe_ratio(
            deltas.get("blockbuffer/commitment_parent/prefetch/unused_capacity_evicted"),
            deltas.get("blockbuffer/commitment_parent/prefetch/durable_reads"),
        ),
        "durablePublishRaceRatio": safe_ratio(
            deltas.get("blockbuffer/commitment_parent/durable_publish_races"), total_durable
        ),
        "criticalNanosPerPlannedRead": safe_ratio(
            deltas.get("state/commitment/pipeline/prefetch_critical/wall_nanos"),
            deltas.get("state/commitment/pipeline/prefetch_critical/planned"),
        ),
        "lookaheadNanosPerPlannedRead": safe_ratio(
            deltas.get("state/commitment/pipeline/prefetch_lookahead/wall_nanos"),
            deltas.get("state/commitment/pipeline/prefetch_lookahead/planned"),
        ),
        "criticalWaitNanosPerLane": safe_ratio(
            deltas.get("state/commitment/pipeline/prefetch_critical/wait_nanos"),
            deltas.get("state/commitment/pipeline/prefetch_critical/wait_calls"),
        ),
        "durableReadsPerBlock": safe_ratio(total_durable, interval_blocks),
        "blockCacheHitRatio": safe_ratio(deltas.get("cache/block/hit"), block_cache_total),
        "tableCacheHitRatio": safe_ratio(deltas.get("cache/table/hit"), table_cache_total),
        "filterHitRatio": safe_ratio(deltas.get("filter/hit"), filter_total),
        "foregroundDepthDurableRatio": foreground_depth_ratios,
    }
    return {
        "timestamp": dt.datetime.fromtimestamp(now, dt.timezone.utc).isoformat(),
        "unix": now,
        "height": height,
        "intervalSeconds": interval,
        "intervalBlocks": interval_blocks,
        "blocksPerSecond": safe_ratio(interval_blocks, interval),
        "status": "counter-reset" if resets else ("bootstrap" if previous is None else "ok"),
        "counterResets": sorted(set(resets)),
        "metrics": current,
        "delta": deltas,
        "analysis": analysis,
    }


def append_row(path, row):
    encoded = json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    print(encoded, flush=True)
    if path:
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("a", encoding="utf-8") as stream:
            stream.write(encoded + "\n")


def sample_once(args, previous):
    now = time.time()
    metrics = select_metrics(fetch_metrics(args.metrics_url, args.timeout))
    height = None
    if args.wallet_url:
        try:
            height = wallet_height(args.wallet_url, args.timeout)
        except (OSError, RuntimeError, ValueError, json.JSONDecodeError):
            height = None
    return build_row(now, height, metrics, previous)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--metrics-url",
        default="http://127.0.0.1:6062/debug/pprof/metrics",
        help="gtron debug JSON metrics endpoint",
    )
    parser.add_argument(
        "--wallet-url",
        default="http://127.0.0.1:8090",
        help="wallet API base URL; pass an empty string to omit height",
    )
    parser.add_argument("--output", default="", help="append compact JSONL rows to this file")
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--interval", type=float, default=0.0, help="seconds between samples")
    parser.add_argument("--samples", type=int, default=1, help="sample count; 0 runs until interrupted")
    args = parser.parse_args()
    if args.timeout <= 0 or args.interval < 0 or args.samples < 0:
        parser.error("timeout must be positive; interval and samples must be non-negative")
    if args.samples != 1 and args.interval <= 0:
        parser.error("--interval must be positive when --samples is not 1")

    previous = load_previous(args.output)
    remaining = args.samples
    try:
        while True:
            row = sample_once(args, previous)
            append_row(args.output, row)
            previous = row
            if remaining > 0:
                remaining -= 1
                if remaining == 0:
                    break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 130
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        print(f"commitment pread sample: ERROR: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

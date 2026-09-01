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
    "process/",
    "compact/",
    "level/",
    "disk/physical/read/sst/",
    "blockbuffer/commitment_parent/",
    "state/commitment/",
    "state/snapshot/commitment_branch/",
    "chain/freezer/",
    "state/snapshot/cold/",
    "state/code_cache/",
    "cache/",
    "filter/",
    "iter/",
)

PROCESS_IDENTITY_METRIC = "process/start/unix_nano"

PEBBLE_COMPACTION_INPUT_METRIC = "compact/input"
PEBBLE_COMPACTION_LIVE_COUNT_METRIC = "compact/live/count"
# Pebble's production layout exposes the standard seven levels, L0 through L6.
PEBBLE_LEVEL_COMPACTION_READ_METRICS = {
    level: "level/{}/compact/read".format(level) for level in range(7)
}

# Optional physical-I/O contracts. Keep the names and output prefixes in one
# place: older/mixed-version nodes must report missing data, never invented
# zeroes. The commitment group is attributable to the immutable branch segment;
# the SST group is process-wide physical I/O and must not be presented as if it
# were commitment-only.
PHYSICAL_READ_METRIC_GROUPS = (
    {
        "output_prefix": "commitmentSegmentPhysicalRead",
        "metrics": {
            "calls": "state/snapshot/commitment_branch/point_read/physical/calls",
            "bytes": "state/snapshot/commitment_branch/point_read/physical/bytes",
            "nanos": "state/snapshot/commitment_branch/point_read/physical/nanos",
            "errors": "state/snapshot/commitment_branch/point_read/physical/errors",
            "short_reads": "state/snapshot/commitment_branch/point_read/physical/short_reads",
            "locality_samples": "state/snapshot/commitment_branch/point_read/locality/samples",
            "offset_jump_bytes": "state/snapshot/commitment_branch/point_read/locality/offset_jump_bytes",
            "same_block": "state/snapshot/commitment_branch/point_read/locality/same_block",
            "adjacent_block": "state/snapshot/commitment_branch/point_read/locality/adjacent_block",
        },
        "locality_ratios": (
            ("same_block", "SameBlockRatio"),
            ("adjacent_block", "AdjacentBlockRatio"),
        ),
        "locality_total": ("same_block", "adjacent_block"),
    },
    {
        "output_prefix": "sstPhysicalRead",
        "metrics": {
            "calls": "disk/physical/read/sst/calls",
            "bytes": "disk/physical/read/sst/bytes",
            "nanos": "disk/physical/read/sst/nanos",
            "errors": "disk/physical/read/sst/errors",
            "short_reads": "disk/physical/read/sst/short_reads",
            "locality_samples": "disk/physical/read/sst/locality/samples",
            "offset_jump_bytes": "disk/physical/read/sst/locality/offset_jump_bytes",
            "same_offset": "disk/physical/read/sst/locality/same_offset",
        },
        "locality_ratios": (
            ("same_offset", "SameOffsetRatio"),
        ),
        "locality_total": ("same_offset",),
    },
)
PHYSICAL_READ_COUNTER_METRICS = tuple(
    name
    for group in PHYSICAL_READ_METRIC_GROUPS
    for name in group["metrics"].values()
)

PEBBLE_COMMITMENT_CURSOR_METRICS = {
    "cursors": "blockbuffer/commitment_parent/pebble/cursors",
    "seek_calls": "blockbuffer/commitment_parent/pebble/seek_calls",
    "internal_seek_calls": "blockbuffer/commitment_parent/pebble/internal_seek_calls",
    "block_bytes": "blockbuffer/commitment_parent/pebble/block_bytes",
    "block_bytes_cached": "blockbuffer/commitment_parent/pebble/block_bytes_cached",
    "block_read_nanos": "blockbuffer/commitment_parent/pebble/block_read_nanos",
    "point_count": "blockbuffer/commitment_parent/pebble/point_count",
    "read_amp_sum": "blockbuffer/commitment_parent/pebble/read_amp_sum",
}
PEBBLE_COMMITMENT_CURSOR_COUNTER_METRICS = tuple(
    PEBBLE_COMMITMENT_CURSOR_METRICS.values()
)

# These counters attribute ReadAt work by the access advice carried by the
# successful SST Open. Sequential includes both compaction and speculative
# threshold reopens; it does not prove that the kernel accepted fadvise.
SST_FD_ACCESS_METRICS = {
    access: {
        unit: "disk/physical/read/sst/fd/{}/{}".format(access, unit)
        for unit in ("calls", "bytes", "nanos")
    }
    for access in ("random", "sequential", "other")
}
SST_FD_ACCESS_COUNTER_METRICS = tuple(
    name
    for access_metrics in SST_FD_ACCESS_METRICS.values()
    for name in access_metrics.values()
)
SST_PREFETCH_METRICS = {
    "calls": "disk/physical/read/sst/prefetch/calls",
    "requested_bytes": "disk/physical/read/sst/prefetch/requested_bytes",
    "errors": "disk/physical/read/sst/prefetch/errors",
}
SST_PREFETCH_COUNTER_METRICS = tuple(SST_PREFETCH_METRICS.values())

FREEZER_ANCIENT_READ_METRIC = "chain/freezer/ancient/read"
FREEZER_V2_MONOTONIC_METRICS = (
    "chain/freezer/v2/coverage",
    "chain/freezer/v2/blocks",
    "chain/freezer/v2/batch/budget_exhausted",
    "chain/freezer/v2/deferred/catchup",
    "chain/freezer/v2/deferred/resource",
    "chain/freezer/v2/deferred/error_backoff",
    "chain/freezer/v2/deferred/source_pruned",
    "chain/freezer/v2/errors",
)
FREEZER_V2_POINT_METRICS = (
    "chain/freezer/v2/backlog/blocks",
    "chain/freezer/v2/backlog/segments",
    "chain/freezer/v2/batch/segments",
    "chain/freezer/v2/batch/duration",
)
STATE_CODE_CACHE_COUNTER_METRICS = (
    "state/code_cache/hits",
    "state/code_cache/misses",
    "state/code_cache/admissions",
    "state/code_cache/evictions",
    "state/code_cache/hash_rejections",
)
STATE_CODE_CACHE_BYTES_METRIC = "state/code_cache/bytes"
COLD_COMPACTION_ACTIVE_METRIC = "state/snapshot/cold/compaction/current/active"


def physical_read_group(output_prefix):
    for group in PHYSICAL_READ_METRIC_GROUPS:
        if group["output_prefix"] == output_prefix:
            return group
    raise KeyError(output_prefix)


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
    "blockbuffer/commitment_parent/singleflight/leaders",
    "blockbuffer/commitment_parent/singleflight/waiters",
    "blockbuffer/commitment_parent/singleflight/shared_results",
    "blockbuffer/commitment_parent/singleflight/shared_results/foreground",
    "blockbuffer/commitment_parent/singleflight/shared_results/prefetch",
    "blockbuffer/commitment_parent/singleflight/shared_present",
    "blockbuffer/commitment_parent/singleflight/shared_missing",
    "blockbuffer/commitment_parent/singleflight/leader_errors",
    "blockbuffer/commitment_parent/singleflight/wait_nanos",
    "blockbuffer/commitment_parent/singleflight/waiters/foreground",
    "blockbuffer/commitment_parent/singleflight/waiters/prefetch",
    "blockbuffer/commitment_parent/prefetch/unused_capacity_evicted",
    "blockbuffer/commitment_parent/prefetch/unused_capacity_evicted_bytes",
    "state/commitment/fold/wall_nanos",
    "state/commitment/pipeline/jobs",
    "state/commitment/pipeline/prefetch_errors",
    "state/commitment/pipeline/prefetch_critical/planned",
    "state/commitment/pipeline/prefetch_critical/wall_nanos",
    "state/commitment/pipeline/prefetch_critical/wait_calls",
    "state/commitment/pipeline/prefetch_critical/wait_nanos",
    "state/commitment/pipeline/prefetch_critical/queue_wait_calls",
    "state/commitment/pipeline/prefetch_critical/queue_wait_nanos",
    "state/commitment/pipeline/prefetch_critical/blocked_by_lookahead_calls",
    "state/commitment/pipeline/prefetch_critical/blocked_by_lookahead_nanos",
    "state/commitment/pipeline/prefetch_lookahead/planned",
    "state/commitment/pipeline/prefetch_lookahead/capped_lanes",
    "state/commitment/pipeline/prefetch_lookahead/wall_nanos",
    "state/commitment/pipeline/prefetch_lookahead/finish_wait_calls",
    "state/commitment/pipeline/prefetch_lookahead/finish_wait_nanos",
) + (PEBBLE_COMPACTION_INPUT_METRIC,) + PHYSICAL_READ_COUNTER_METRICS + (
    PEBBLE_COMMITMENT_CURSOR_COUNTER_METRICS
    + SST_FD_ACCESS_COUNTER_METRICS
    + SST_PREFETCH_COUNTER_METRICS
    + (FREEZER_ANCIENT_READ_METRIC,)
    + STATE_CODE_CACHE_COUNTER_METRICS
)

# These are exported as gauges by their owners, but hold process-lifetime or
# durable monotonic totals. Delta them like counters while retaining their
# actual metric type here; a decrease is a restart/reset, never negative work.
MONOTONIC_GAUGE_METRICS = (
    "chain/freezer/txindex/coverage",
    "chain/freezer/txindex/pruned",
    "chain/freezer/txindex/rows/archived",
    "chain/freezer/txindex/rows/pruned",
    "chain/freezer/txindex/prune/blocks",
    "chain/freezer/txindex/prune/rows",
    "chain/freezer/txindex/prune/duration",
    "chain/freezer/txindex/maintenance/admitted",
    "chain/freezer/txindex/maintenance/deferred",
    "chain/freezer/txindex/deferred/sync",
    "chain/freezer/txindex/deferred/catchup",
    "chain/freezer/txindex/deferred/resource",
    "chain/freezer/txindex/deferred/error_backoff",
    "chain/freezer/txindex/errors",
    "state/snapshot/cold/history/deferred/sync",
    "state/snapshot/cold/history/deferred/rate_limit",
    "state/snapshot/cold/history/accelerated/builds",
    "state/snapshot/cold/history/forced_busy/passes",
    "state/snapshot/cold/history/forced_busy/attempts",
    "state/snapshot/cold/history/forced_busy/builds",
    "state/snapshot/cold/history/admission/checks",
    "state/snapshot/cold/history/admission/ready",
    "state/snapshot/cold/history/admission/busy",
    "state/snapshot/cold/history/deferred/resource",
    "cache/block/hit",
    "cache/block/miss",
    "cache/table/hit",
    "cache/table/miss",
    "filter/hit",
    "filter/miss",
) + tuple(
    PEBBLE_LEVEL_COMPACTION_READ_METRICS.values()
) + FREEZER_V2_MONOTONIC_METRICS

POINT_GAUGE_METRICS = (
    PROCESS_IDENTITY_METRIC,
    PEBBLE_COMPACTION_LIVE_COUNT_METRIC,
    "state/commitment/pipeline/prefetch_critical/depth",
    "state/commitment/pipeline/prefetch_critical/queue_high_water",
    "state/commitment/pipeline/prefetch_lookahead/depth",
    "state/commitment/pipeline/prefetch_lookahead/limit_per_lane",
    "state/commitment/pipeline/prefetch_lookahead/queue_high_water",
    "chain/freezer/txindex/debt/blocks",
    "state/snapshot/cold/lastpass/history/batch/blocks",
    "state/snapshot/cold/lastpass/history/batch/txnums",
    "state/snapshot/cold/history/forced_busy/last/batch/blocks",
    "state/snapshot/cold/history/forced_busy/last/batch/txnums",
    "state/snapshot/cold/history/forced_busy/last/recovery",
    "state/snapshot/cold/history/forced_busy/last/duty_cycle_ppm",
    "state/snapshot/cold/history/forced_busy/last/debt_blocks",
    "state/snapshot/cold/history/forced_busy/last/debt_growth_blocks",
    *FREEZER_V2_POINT_METRICS,
    STATE_CODE_CACHE_BYTES_METRIC,
    COLD_COMPACTION_ACTIVE_METRIC,
    "iter/count",
)

GAUGE_METRICS = MONOTONIC_GAUGE_METRICS + POINT_GAUGE_METRICS
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


def nonnegative_difference(total, part):
    if total is None or part is None or part > total:
        return None
    return total - part


def build_physical_read_analysis(group, current, deltas, interval_blocks):
    metrics = group["metrics"]
    output_prefix = group["output_prefix"]
    present = sum(1 for name in metrics.values() if current.get(name) is not None)
    calls = deltas.get(metrics["calls"])
    locality_samples = deltas.get(metrics["locality_samples"])
    result = {
        output_prefix + "MetricsAvailable": present > 0,
        output_prefix + "MetricCoverageRatio": safe_ratio(present, len(metrics)),
        output_prefix + "CallsPerBlock": safe_ratio(calls, interval_blocks),
        output_prefix + "BytesPerBlock": safe_ratio(
            deltas.get(metrics["bytes"]), interval_blocks
        ),
        output_prefix + "NanosPerBlock": safe_ratio(
            deltas.get(metrics["nanos"]), interval_blocks
        ),
        output_prefix + "BytesPerCall": safe_ratio(deltas.get(metrics["bytes"]), calls),
        output_prefix + "NanosPerCall": safe_ratio(deltas.get(metrics["nanos"]), calls),
        output_prefix + "ErrorRatio": safe_ratio(deltas.get(metrics["errors"]), calls),
        output_prefix + "ShortReadRatio": safe_ratio(
            deltas.get(metrics["short_reads"]), calls
        ),
        output_prefix + "OffsetJumpBytesPerSample": safe_ratio(
            deltas.get(metrics["offset_jump_bytes"]), locality_samples
        ),
    }
    for metric_key, output_suffix in group["locality_ratios"]:
        value = deltas.get(metrics[metric_key])
        result[output_prefix + output_suffix] = safe_ratio(value, locality_samples)
    local_hits = [deltas.get(metrics[key]) for key in group["locality_total"]]
    result[output_prefix + "LocalityRatio"] = safe_ratio(
        positive_sum(*local_hits), locality_samples
    )
    return result


def build_pebble_commitment_cursor_analysis(current, deltas, interval_blocks):
    metrics = PEBBLE_COMMITMENT_CURSOR_METRICS
    present = sum(1 for name in metrics.values() if current.get(name) is not None)
    values = {key: deltas.get(name) for key, name in metrics.items()}
    uncached = nonnegative_difference(values["block_bytes"], values["block_bytes_cached"])
    return {
        "pebbleCursorMetricsAvailable": present > 0,
        "pebbleCursorMetricCoverageRatio": safe_ratio(present, len(metrics)),
        "pebbleCursorUncachedBlockBytes": uncached,
        "pebbleCursorUncachedBlockRatio": safe_ratio(uncached, values["block_bytes"]),
        "pebbleCursorCursorsPerBlock": safe_ratio(values["cursors"], interval_blocks),
        "pebbleCursorSeekCallsPerBlock": safe_ratio(values["seek_calls"], interval_blocks),
        "pebbleCursorInternalSeekCallsPerBlock": safe_ratio(
            values["internal_seek_calls"], interval_blocks
        ),
        "pebbleCursorBlockBytesPerBlock": safe_ratio(values["block_bytes"], interval_blocks),
        "pebbleCursorBlockBytesCachedPerBlock": safe_ratio(
            values["block_bytes_cached"], interval_blocks
        ),
        "pebbleCursorUncachedBlockBytesPerBlock": safe_ratio(uncached, interval_blocks),
        "pebbleCursorBlockReadNanosPerBlock": safe_ratio(
            values["block_read_nanos"], interval_blocks
        ),
        "pebbleCursorPointCountPerBlock": safe_ratio(values["point_count"], interval_blocks),
        "pebbleCursorReadAmpSumPerBlock": safe_ratio(values["read_amp_sum"], interval_blocks),
        "pebbleCursorBlockReadNanosPerSeek": safe_ratio(
            values["block_read_nanos"], values["seek_calls"]
        ),
        "pebbleCursorReadAmpPerCursor": safe_ratio(values["read_amp_sum"], values["cursors"]),
    }


def build_pebble_compaction_analysis(current, deltas, interval_blocks, interval):
    level_values = {
        str(level): deltas.get(name)
        for level, name in PEBBLE_LEVEL_COMPACTION_READ_METRICS.items()
    }
    level_present = sum(
        1 for name in PEBBLE_LEVEL_COMPACTION_READ_METRICS.values()
        if current.get(name) is not None
    )
    level_total = positive_sum(*level_values.values())
    compaction_input = deltas.get(PEBBLE_COMPACTION_INPUT_METRIC)
    return {
        "pebbleCompactionInputBytes": compaction_input,
        "pebbleCompactionInputBytesPerBlock": safe_ratio(compaction_input, interval_blocks),
        "pebbleCompactionInputBytesPerSecond": safe_ratio(compaction_input, interval),
        "pebbleCompactionLiveCount": current.get(PEBBLE_COMPACTION_LIVE_COUNT_METRIC),
        "pebbleLevelCompactionReadMetricCoverageRatio": safe_ratio(
            level_present, len(PEBBLE_LEVEL_COMPACTION_READ_METRICS)
        ),
        "pebbleLevelCompactionReadBytes": level_values,
        "pebbleLevelCompactionReadBytesPerBlock": {
            level: safe_ratio(value, interval_blocks)
            for level, value in level_values.items()
        },
        "pebbleLevelCompactionReadBytesTotal": level_total,
        "pebbleLevelCompactionReadBytesPerBlockTotal": safe_ratio(
            level_total, interval_blocks
        ),
    }


def build_sst_fd_access_analysis(current, deltas, interval_blocks):
    present = sum(
        1
        for access_metrics in SST_FD_ACCESS_METRICS.values()
        for name in access_metrics.values()
        if current.get(name) is not None
    )
    totals = {
        unit: positive_sum(
            *(deltas.get(metrics[unit]) for metrics in SST_FD_ACCESS_METRICS.values())
        )
        for unit in ("calls", "bytes", "nanos")
    }
    sst_metrics = physical_read_group("sstPhysicalRead")["metrics"]
    result = {
        "sstFdAccessMetricsAvailable": present > 0,
        "sstFdAccessMetricCoverageRatio": safe_ratio(
            present, len(SST_FD_ACCESS_COUNTER_METRICS)
        ),
        "sstFdClassifiedCallsPerBlock": safe_ratio(totals["calls"], interval_blocks),
        "sstFdClassifiedBytesPerBlock": safe_ratio(totals["bytes"], interval_blocks),
        "sstFdClassifiedNanosPerBlock": safe_ratio(totals["nanos"], interval_blocks),
        "sstFdClassifiedCallRatio": safe_ratio(
            totals["calls"], deltas.get(sst_metrics["calls"])
        ),
        "sstFdClassifiedByteRatio": safe_ratio(
            totals["bytes"], deltas.get(sst_metrics["bytes"])
        ),
        "sstFdClassifiedNanosRatio": safe_ratio(
            totals["nanos"], deltas.get(sst_metrics["nanos"])
        ),
    }
    for access, metrics in SST_FD_ACCESS_METRICS.items():
        output = access[0].upper() + access[1:]
        calls = deltas.get(metrics["calls"])
        result["sstFd" + output + "CallsPerBlock"] = safe_ratio(calls, interval_blocks)
        result["sstFd" + output + "BytesPerBlock"] = safe_ratio(
            deltas.get(metrics["bytes"]), interval_blocks
        )
        result["sstFd" + output + "NanosPerBlock"] = safe_ratio(
            deltas.get(metrics["nanos"]), interval_blocks
        )
        result["sstFd" + output + "BytesPerCall"] = safe_ratio(
            deltas.get(metrics["bytes"]), calls
        )
        result["sstFd" + output + "NanosPerCall"] = safe_ratio(
            deltas.get(metrics["nanos"]), calls
        )
        result["sstFd" + output + "CallRatio"] = safe_ratio(calls, totals["calls"])
        result["sstFd" + output + "ByteRatio"] = safe_ratio(
            deltas.get(metrics["bytes"]), totals["bytes"]
        )
        result["sstFd" + output + "NanosRatio"] = safe_ratio(
            deltas.get(metrics["nanos"]), totals["nanos"]
        )
    return result


def build_sst_prefetch_analysis(current, deltas, interval_blocks):
    present = sum(1 for name in SST_PREFETCH_METRICS.values() if current.get(name) is not None)
    calls = deltas.get(SST_PREFETCH_METRICS["calls"])
    requested_bytes = deltas.get(SST_PREFETCH_METRICS["requested_bytes"])
    return {
        "sstPrefetchMetricsAvailable": present > 0,
        "sstPrefetchMetricCoverageRatio": safe_ratio(present, len(SST_PREFETCH_METRICS)),
        "sstPrefetchCallsPerBlock": safe_ratio(calls, interval_blocks),
        # Requested bytes are overlapping kernel hints, not bytes proven read.
        "sstPrefetchRequestedBytesPerBlock": safe_ratio(requested_bytes, interval_blocks),
        "sstPrefetchRequestedBytesPerCall": safe_ratio(requested_bytes, calls),
        "sstPrefetchErrorRatio": safe_ratio(
            deltas.get(SST_PREFETCH_METRICS["errors"]), calls
        ),
    }


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
    for name in MONOTONIC_GAUGE_METRICS:
        value = current.get(name)
        old = previous_metrics.get(name)
        if value is None or not isinstance(old, (int, float)) or isinstance(old, bool) or value < old:
            deltas[name] = None
            if value is not None and isinstance(old, (int, float)) and not isinstance(old, bool) and value < old:
                resets.append(name)
        else:
            deltas[name] = value - old

    previous_process = previous_metrics.get(PROCESS_IDENTITY_METRIC)
    current_process = current.get(PROCESS_IDENTITY_METRIC)
    previous_process_valid = isinstance(previous_process, (int, float)) and not isinstance(previous_process, bool)
    current_process_valid = isinstance(current_process, (int, float)) and not isinstance(current_process, bool)
    process_restart = isinstance(previous, dict) and (
        previous_process_valid != current_process_valid
        or (previous_process_valid and current_process_valid and current_process != previous_process)
    )
    if process_restart:
        # A per-metric decrease is insufficient: a hot counter may catch up to
        # its pre-restart value before the next sample. Invalidate the complete
        # process-scoped interval while retaining current durable/point gauges.
        for name in COUNTER_METRICS + MONOTONIC_GAUGE_METRICS:
            deltas[name] = None
        resets.append(PROCESS_IDENTITY_METRIC)

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
        deltas.get("blockbuffer/commitment_parent/singleflight/shared_results/foreground"),
    )
    total_durable = positive_sum(
        deltas.get("blockbuffer/commitment_parent/durable/reads"),
        deltas.get("blockbuffer/commitment_parent/prefetch/durable_reads"),
    )
    block_cache_total = positive_sum(deltas.get("cache/block/hit"), deltas.get("cache/block/miss"))
    table_cache_total = positive_sum(deltas.get("cache/table/hit"), deltas.get("cache/table/miss"))
    filter_total = positive_sum(deltas.get("filter/hit"), deltas.get("filter/miss"))
    state_code_cache_total = positive_sum(
        deltas.get("state/code_cache/hits"), deltas.get("state/code_cache/misses")
    )
    flight_logical_reads = positive_sum(
        deltas.get("blockbuffer/commitment_parent/singleflight/leaders"),
        deltas.get("blockbuffer/commitment_parent/singleflight/shared_results"),
    )
    tx_index_maintenance_attempts = positive_sum(
        deltas.get("chain/freezer/txindex/maintenance/admitted"),
        deltas.get("chain/freezer/txindex/deferred/catchup"),
        deltas.get("chain/freezer/txindex/deferred/resource"),
        deltas.get("chain/freezer/txindex/deferred/error_backoff"),
    )
    interval_nanos = interval * 1_000_000_000 if interval is not None else None

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
        "singleflightSharedRatio": safe_ratio(
            deltas.get("blockbuffer/commitment_parent/singleflight/shared_results"),
            flight_logical_reads,
        ),
        "singleflightWaiterShareRatio": safe_ratio(
            deltas.get("blockbuffer/commitment_parent/singleflight/shared_results"),
            deltas.get("blockbuffer/commitment_parent/singleflight/waiters"),
        ),
        "singleflightWaitNanosPerWaiter": safe_ratio(
            deltas.get("blockbuffer/commitment_parent/singleflight/wait_nanos"),
            deltas.get("blockbuffer/commitment_parent/singleflight/waiters"),
        ),
        "singleflightSharedPresentRatio": safe_ratio(
            deltas.get("blockbuffer/commitment_parent/singleflight/shared_present"),
            deltas.get("blockbuffer/commitment_parent/singleflight/shared_results"),
        ),
        "singleflightForegroundWaiterRatio": safe_ratio(
            deltas.get("blockbuffer/commitment_parent/singleflight/waiters/foreground"),
            deltas.get("blockbuffer/commitment_parent/singleflight/waiters"),
        ),
        "singleflightLeaderErrorRatio": safe_ratio(
            deltas.get("blockbuffer/commitment_parent/singleflight/leader_errors"),
            deltas.get("blockbuffer/commitment_parent/singleflight/leaders"),
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
        "criticalQueueWaitNanosPerCall": safe_ratio(
            deltas.get("state/commitment/pipeline/prefetch_critical/queue_wait_nanos"),
            deltas.get("state/commitment/pipeline/prefetch_critical/queue_wait_calls"),
        ),
        "criticalQueueWaitNanosPerBlock": safe_ratio(
            deltas.get("state/commitment/pipeline/prefetch_critical/queue_wait_nanos"),
            interval_blocks,
        ),
        "criticalBlockedByLookaheadNanosPerCall": safe_ratio(
            deltas.get("state/commitment/pipeline/prefetch_critical/blocked_by_lookahead_nanos"),
            deltas.get("state/commitment/pipeline/prefetch_critical/blocked_by_lookahead_calls"),
        ),
        "criticalBlockedByLookaheadCallsPerBlock": safe_ratio(
            deltas.get("state/commitment/pipeline/prefetch_critical/blocked_by_lookahead_calls"),
            interval_blocks,
        ),
        "criticalBlockedByLookaheadCallRatio": safe_ratio(
            deltas.get("state/commitment/pipeline/prefetch_critical/blocked_by_lookahead_calls"),
            deltas.get("state/commitment/pipeline/prefetch_critical/queue_wait_calls"),
        ),
        "finishLookaheadWaitNanosPerCall": safe_ratio(
            deltas.get("state/commitment/pipeline/prefetch_lookahead/finish_wait_nanos"),
            deltas.get("state/commitment/pipeline/prefetch_lookahead/finish_wait_calls"),
        ),
        "finishLookaheadWaitNanosPerBlock": safe_ratio(
            deltas.get("state/commitment/pipeline/prefetch_lookahead/finish_wait_nanos"),
            interval_blocks,
        ),
        # Both high-water gauges are maxima of queued items in one lane, not
        # totals across every commitment lane.
        "criticalQueueHighWaterPerLane": current.get(
            "state/commitment/pipeline/prefetch_critical/queue_high_water"
        ),
        "lookaheadQueueHighWaterPerLane": current.get(
            "state/commitment/pipeline/prefetch_lookahead/queue_high_water"
        ),
        "durableReadsPerBlock": safe_ratio(total_durable, interval_blocks),
        "freezerAncientReadBytesPerBlock": safe_ratio(
            deltas.get(FREEZER_ANCIENT_READ_METRIC), interval_blocks
        ),
        "freezerV2Coverage": current.get("chain/freezer/v2/coverage"),
        "freezerV2CoverageBlocksPerBlock": safe_ratio(
            deltas.get("chain/freezer/v2/coverage"), interval_blocks
        ),
        "freezerV2CompactedBlocksPerBlock": safe_ratio(
            deltas.get("chain/freezer/v2/blocks"), interval_blocks
        ),
        "freezerV2BacklogBlocks": current.get("chain/freezer/v2/backlog/blocks"),
        "freezerV2BacklogSegments": current.get("chain/freezer/v2/backlog/segments"),
        "freezerV2LastBatchSegments": current.get("chain/freezer/v2/batch/segments"),
        "freezerV2LastBatchSeconds": safe_ratio(
            current.get("chain/freezer/v2/batch/duration"), 1_000_000_000
        ),
        "freezerV2BudgetExhaustedPerBlock": safe_ratio(
            deltas.get("chain/freezer/v2/batch/budget_exhausted"), interval_blocks
        ),
        "coldSnapshotCompactionActive": current.get(COLD_COMPACTION_ACTIVE_METRIC),
        "stateCodeCacheHitRatio": safe_ratio(
            deltas.get("state/code_cache/hits"), state_code_cache_total
        ),
        "stateCodeCacheMissesPerBlock": safe_ratio(
            deltas.get("state/code_cache/misses"), interval_blocks
        ),
        "stateCodeCacheBytes": current.get(STATE_CODE_CACHE_BYTES_METRIC),
        "txIndexDebtBlocks": current.get("chain/freezer/txindex/debt/blocks"),
        "txIndexCoverageBlocksPerBlock": safe_ratio(
            deltas.get("chain/freezer/txindex/coverage"), interval_blocks
        ),
        "txIndexPrunedBlocksPerBlock": safe_ratio(
            deltas.get("chain/freezer/txindex/prune/blocks"), interval_blocks
        ),
        "txIndexPrunedRowsPerBlock": safe_ratio(
            deltas.get("chain/freezer/txindex/prune/rows"), interval_blocks
        ),
        "txIndexPruneNanosPerPrunedBlock": safe_ratio(
            deltas.get("chain/freezer/txindex/prune/duration"),
            deltas.get("chain/freezer/txindex/prune/blocks"),
        ),
        "txIndexPruneDutyRatio": safe_ratio(
            deltas.get("chain/freezer/txindex/prune/duration"), interval_nanos
        ),
        "txIndexMaintenanceAdmittedPerBlock": safe_ratio(
            deltas.get("chain/freezer/txindex/maintenance/admitted"), interval_blocks
        ),
        "txIndexMaintenanceDeferredPerBlock": safe_ratio(
            deltas.get("chain/freezer/txindex/maintenance/deferred"), interval_blocks
        ),
        # Use reason-specific deferrals so the denominator remains explainable
        # even if future skip-only observability is kept separate from admission.
        "txIndexMaintenanceAdmissionRatio": safe_ratio(
            deltas.get("chain/freezer/txindex/maintenance/admitted"),
            tx_index_maintenance_attempts,
        ),
        "txIndexSyncDeferredPerBlock": safe_ratio(
            deltas.get("chain/freezer/txindex/deferred/sync"), interval_blocks
        ),
        "coldSnapshotForcedBuildRatio": safe_ratio(
            deltas.get("state/snapshot/cold/history/forced_busy/builds"),
            deltas.get("state/snapshot/cold/history/forced_busy/passes"),
        ),
        "coldSnapshotForcedAttemptSuccessRatio": safe_ratio(
            deltas.get("state/snapshot/cold/history/forced_busy/builds"),
            deltas.get("state/snapshot/cold/history/forced_busy/attempts"),
        ),
        "coldSnapshotForcedBuildsPerBlock": safe_ratio(
            deltas.get("state/snapshot/cold/history/forced_busy/builds"), interval_blocks
        ),
        "coldSnapshotAdmissionReadyRatio": safe_ratio(
            deltas.get("state/snapshot/cold/history/admission/ready"),
            deltas.get("state/snapshot/cold/history/admission/checks"),
        ),
        "coldSnapshotAdmissionBusyRatio": safe_ratio(
            deltas.get("state/snapshot/cold/history/admission/busy"),
            deltas.get("state/snapshot/cold/history/admission/checks"),
        ),
        "coldSnapshotLastBatchTxNumsPerBlock": safe_ratio(
            current.get("state/snapshot/cold/lastpass/history/batch/txnums"),
            current.get("state/snapshot/cold/lastpass/history/batch/blocks"),
        ),
        "coldSnapshotLastForcedBatchTxNumsPerBlock": safe_ratio(
            current.get("state/snapshot/cold/history/forced_busy/last/batch/txnums"),
            current.get("state/snapshot/cold/history/forced_busy/last/batch/blocks"),
        ),
        "coldSnapshotLastForcedRecoverySeconds": safe_ratio(
            current.get("state/snapshot/cold/history/forced_busy/last/recovery"),
            1_000_000_000,
        ),
        "coldSnapshotLastForcedDutyRatio": safe_ratio(
            current.get("state/snapshot/cold/history/forced_busy/last/duty_cycle_ppm"),
            1_000_000,
        ),
        "coldSnapshotLastForcedDebtBlocks": current.get(
            "state/snapshot/cold/history/forced_busy/last/debt_blocks"
        ),
        "coldSnapshotLastForcedDebtGrowthBlocks": current.get(
            "state/snapshot/cold/history/forced_busy/last/debt_growth_blocks"
        ),
        "blockCacheHitRatio": safe_ratio(deltas.get("cache/block/hit"), block_cache_total),
        "tableCacheHitRatio": safe_ratio(deltas.get("cache/table/hit"), table_cache_total),
        "filterHitRatio": safe_ratio(deltas.get("filter/hit"), filter_total),
        "foregroundDepthDurableRatio": foreground_depth_ratios,
    }
    for group in PHYSICAL_READ_METRIC_GROUPS:
        analysis.update(build_physical_read_analysis(group, current, deltas, interval_blocks))
    analysis.update(build_pebble_commitment_cursor_analysis(current, deltas, interval_blocks))
    analysis.update(build_pebble_compaction_analysis(current, deltas, interval_blocks, interval))
    analysis.update(build_sst_fd_access_analysis(current, deltas, interval_blocks))
    analysis.update(build_sst_prefetch_analysis(current, deltas, interval_blocks))
    return {
        "timestamp": dt.datetime.fromtimestamp(now, dt.timezone.utc).isoformat(),
        "unix": now,
        "height": height,
        "intervalSeconds": interval,
        "intervalBlocks": interval_blocks,
        "blocksPerSecond": safe_ratio(interval_blocks, interval),
        "status": "counter-reset" if resets else ("bootstrap" if previous is None else "ok"),
        "counterResets": sorted(set(resets)),
        "processRestart": process_restart,
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

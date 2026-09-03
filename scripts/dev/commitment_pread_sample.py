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
    "sync/import/window/",
    "compact/",
    "level/",
    "disk/physical/read/sst/",
    "blockbuffer/commitment_parent/",
    "blockbuffer/base_cache/",
    "state/commitment/",
    "state/prune/",
    "state/snapshot/commitment_branch/",
    "ancient/",
    "chain/freezer/",
    "state/snapshot/cold/",
    "state/code_cache/",
    "cache/",
    "filter/",
    "iter/",
)

PROCESS_IDENTITY_METRIC = "process/start/unix_nano"
SYNC_IMPORT_WINDOW_FRESHNESS_SECONDS = 60.0

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

# The raw freezer is opened with an empty metrics namespace. Its byte meter is
# therefore rooted at ancient/, separately from the runner's chain/freezer/.
FREEZER_ANCIENT_READ_METRIC = "ancient/read"
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

PRUNE_MONOTONIC_GAUGE_METRICS = tuple(
    "state/prune/{}".format(name)
    for name in (
        "passes",
        "errors",
        "skipped/catchup",
        "verification/canceled/catchup",
        "verification/memory_hits",
        "verification/persisted_hits",
        "verification/full",
        "verification/trusted",
        "verification/checksum/started",
        "verification/checksum/completed",
        "verification/checksum/failed",
        "verification/checksum/canceled",
        "verification/checksum/bytes/started",
        "verification/checksum/bytes/completed",
        "retired/verification/memory_hits",
        "retired/verification/persisted_hits",
        "retired/verification/full",
        "retired/verification/canceled/catchup",
        "deleted/tx_ranges",
        "deleted/domain_change_blocks",
        "deleted/commitment_checkpoints",
        "deleted/state_code_rows",
        "state_code/deferred/catchup",
    )
)
PRUNE_POINT_GAUGE_METRICS = tuple(
    "state/prune/{}".format(name)
    for name in (
        "verification/cache_entries",
        "verification/active_segments",
        "verification/active_bytes",
        "verification/checksum/inflight",
        "verification/checksum/bytes/inflight",
        "last/solidified_block",
        "last/domain_change/start_block",
        "last/domain_change/pruned_through_block",
        "last/domain_change/pruned_through_tx",
        "lastpass/duration",
    )
)

EXACT_DEPTH_METRICS = {
    "depth_{}".format(depth): {
        "cache": "blockbuffer/commitment_parent/depth_{}/cache_resolved".format(depth),
        "durable": "blockbuffer/commitment_parent/depth_{}/durable_reads".format(depth),
    }
    for depth in range(5, 9)
}
EXACT_DEPTH_COUNTER_METRICS = tuple(
    name for metrics in EXACT_DEPTH_METRICS.values() for name in metrics.values()
)
BASE_CACHE_WINDOW_COUNTER_METRICS = {
    "promoted": "blockbuffer/base_cache/window/promoted",
    "evicted": "blockbuffer/base_cache/window/evicted",
    "admission_bypassed": "blockbuffer/base_cache/window/admission_bypassed",
    "admission_throttled": "blockbuffer/base_cache/window/admission_throttled",
    "admission_relaxed": "blockbuffer/base_cache/window/admission_relaxed",
}
BASE_CACHE_WINDOW_ADMITTED_METRIC = "blockbuffer/base_cache/window/admitted"
BASE_CACHE_OCCUPANCY_METRICS = {
    tier: {
        unit: "blockbuffer/base_cache/{}/{}".format(tier, unit)
        for unit in ("entries", "bytes")
    }
    for tier in ("trunk", "window", "tail", "other")
}
BASE_CACHE_OCCUPANCY_POINT_METRICS = tuple(
    name
    for metrics in BASE_CACHE_OCCUPANCY_METRICS.values()
    for name in metrics.values()
)
BASE_CACHE_CAPACITY_METRIC = "blockbuffer/base_cache/capacity/bytes"
BASE_CACHE_BUDGET_METRICS = {
    tier: "blockbuffer/base_cache/{}/budget/bytes".format(tier)
    for tier in ("trunk", "window", "other")
}
BASE_CACHE_CAPACITY_POINT_METRICS = (BASE_CACHE_CAPACITY_METRIC,) + tuple(
    BASE_CACHE_BUDGET_METRICS.values()
)

# These gauges are already normalized by the sync reporter over its latest
# progress window. Capture them as point-in-time workload and phase context;
# never delta them against the sampler's independent interval.
SYNC_IMPORT_WINDOW_POINT_METRICS = tuple(
    "sync/import/window/{}".format(name)
    for name in (
        "updated_unix",
        "elapsed_seconds",
        "blocks_per_second",
        "transactions_per_second",
        "transactions_per_block",
        "energy_per_second",
        "energy_per_block",
        "energy_per_transaction",
        "vm_transactions_per_second",
        "native_transactions_per_second",
        "vm_transactions_per_block",
        "native_transactions_per_block",
        "vm_transaction_share",
        "raw_energy_per_second",
        "raw_energy_per_vm_transaction",
        "billed_to_raw_energy_ratio",
        "vm_execution_milliseconds_per_vm_transaction",
        "vm_execution_nanoseconds_per_raw_energy",
        "apply_sample_blocks",
        "apply_sample_transactions",
        "apply_sample_coverage_ratio",
        "apply_milliseconds_per_block",
        "outside_transaction_milliseconds_per_block",
        "execute_fixed_milliseconds_per_block",
        "transaction_milliseconds_per_transaction",
        "state_commit_milliseconds_per_block",
        "state_commit_accounts_per_block",
        "state_commit_kv_accounts_per_block",
        "state_commit_kv_items_per_block",
        "state_commit_storage_writes_per_block",
        "state_commit_kv_writes_per_block",
        "state_commit_commitment_updates_per_block",
        "state_commit_nanoseconds_per_commitment_update",
        "persist_milliseconds_per_block",
        "persist_metadata_bytes_per_block",
        "persist_metadata_bytes_per_transaction",
        "persist_metadata_records_per_block",
        "persist_transaction_lookup_rows_per_block",
        "persist_trace_accounts_per_block",
    )
)


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
    "state/commitment/fold/calls",
    "state/commitment/fold/input_updates",
    "state/commitment/fold/resolved_ops",
    "state/commitment/fold/wall_nanos",
    "state/commitment/fold/errors",
    "state/commitment/fold/changed",
    "state/commitment/fold/unchanged",
    "state/commitment/fold/parallel/calls",
    "state/commitment/fold/parallel/active_splits",
    "state/commitment/fold/parallel/workers",
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
) + EXACT_DEPTH_COUNTER_METRICS + tuple(BASE_CACHE_WINDOW_COUNTER_METRICS.values()) + (
    PEBBLE_COMPACTION_INPUT_METRIC,
) + PHYSICAL_READ_COUNTER_METRICS + (
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
) + FREEZER_V2_MONOTONIC_METRICS + PRUNE_MONOTONIC_GAUGE_METRICS + (
    BASE_CACHE_WINDOW_ADMITTED_METRIC,
)

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
    *PRUNE_POINT_GAUGE_METRICS,
    STATE_CODE_CACHE_BYTES_METRIC,
    COLD_COMPACTION_ACTIVE_METRIC,
    "iter/count",
) + (
    BASE_CACHE_OCCUPANCY_POINT_METRICS
    + BASE_CACHE_CAPACITY_POINT_METRICS
    + SYNC_IMPORT_WINDOW_POINT_METRICS
)

GAUGE_METRICS = MONOTONIC_GAUGE_METRICS + POINT_GAUGE_METRICS
ALL_METRICS = COUNTER_METRICS + GAUGE_METRICS


def request_json(url, timeout):
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status} from {url}")
        return json.load(response)


def with_prefixes(url, prefixes):
    parts = urllib.parse.urlsplit(url)
    query = [(key, value) for key, value in urllib.parse.parse_qsl(parts.query) if key != "prefix"]
    query.extend(("prefix", prefix) for prefix in prefixes)
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
    # Ask the debug endpoint for every required family in one registry snapshot
    # so the selected counters share one scrape window. Keep the local filter as
    # a defensive boundary for unexpected endpoint responses.
    payload = request_json(with_prefixes(url, METRIC_PREFIXES), timeout)
    metrics = normalize_metrics(payload)
    if payload.get("prefixes") != list(METRIC_PREFIXES):
        raise RuntimeError(
            "metrics endpoint did not confirm the required multi-prefix snapshot"
        )
    return {
        name: values
        for name, values in metrics.items()
        if name.startswith(METRIC_PREFIXES)
    }


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


def build_sync_import_window_analysis(now, current):
    updated = current.get("sync/import/window/updated_unix")
    updated_valid = (
        isinstance(updated, (int, float))
        and not isinstance(updated, bool)
        and 0 < updated <= now
    )
    age = now - float(updated) if updated_valid else None
    return {
        "syncImportWindowUpdatedUnix": updated,
        "syncImportWindowAgeSeconds": age,
        "syncImportWindowFreshnessLimitSeconds": SYNC_IMPORT_WINDOW_FRESHNESS_SECONDS,
        "syncImportWindowFresh": (
            age is not None and age <= SYNC_IMPORT_WINDOW_FRESHNESS_SECONDS
        ),
    }


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


def build_exact_depth_analysis(current, deltas, interval_blocks):
    current_present = sum(
        1
        for metrics in EXACT_DEPTH_METRICS.values()
        for name in metrics.values()
        if current.get(name) is not None
    )
    delta_present = sum(
        1
        for metrics in EXACT_DEPTH_METRICS.values()
        for name in metrics.values()
        if deltas.get(name) is not None
    )
    cached = {}
    durable = {}
    resolved = {}
    durable_ratios = {}
    for depth, metrics in EXACT_DEPTH_METRICS.items():
        cached[depth] = deltas.get(metrics["cache"])
        durable[depth] = deltas.get(metrics["durable"])
        resolved[depth] = positive_sum(cached[depth], durable[depth])
        durable_ratios[depth] = safe_ratio(durable[depth], resolved[depth])

    exact_cache_total = positive_sum(*(cached[depth] for depth in EXACT_DEPTH_METRICS))
    exact_durable_total = positive_sum(*(durable[depth] for depth in EXACT_DEPTH_METRICS))
    durable_shares = {
        depth: safe_ratio(durable[depth], exact_durable_total)
        for depth in EXACT_DEPTH_METRICS
    }
    aggregate_cache = deltas.get(
        "blockbuffer/commitment_parent/depth_5_8/cache_resolved"
    )
    aggregate_durable = deltas.get(
        "blockbuffer/commitment_parent/depth_5_8/durable_reads"
    )
    return {
        "foregroundExactDepthMetricCoverageRatio": safe_ratio(
            current_present, len(EXACT_DEPTH_COUNTER_METRICS)
        ),
        "foregroundExactDepthIntervalCoverageRatio": safe_ratio(
            delta_present, len(EXACT_DEPTH_COUNTER_METRICS)
        ),
        "foregroundExactDepthCacheResolvedPerBlock": {
            depth: safe_ratio(value, interval_blocks) for depth, value in cached.items()
        },
        "foregroundExactDepthDurableReadsPerBlock": {
            depth: safe_ratio(value, interval_blocks) for depth, value in durable.items()
        },
        "foregroundExactDepthResolvedPerBlock": {
            depth: safe_ratio(value, interval_blocks) for depth, value in resolved.items()
        },
        "foregroundExactDepthDurableRatio": durable_ratios,
        "foregroundExactDepthDurableReadShare": durable_shares,
        "foregroundExactDepthCacheResolvedPerBlockTotal": safe_ratio(
            exact_cache_total, interval_blocks
        ),
        "foregroundExactDepthDurableReadsPerBlockTotal": safe_ratio(
            exact_durable_total, interval_blocks
        ),
        # Exact metrics repeat the old depth_5_8 parent bucket. Ratios near one
        # verify exporter/scrape consistency; neither side belongs in totals twice.
        "foregroundDepth5To8CacheReconciliationRatio": safe_ratio(
            exact_cache_total, aggregate_cache
        ),
        "foregroundDepth5To8DurableReconciliationRatio": safe_ratio(
            exact_durable_total, aggregate_durable
        ),
    }


def build_base_cache_analysis(current, previous_metrics, deltas, interval_blocks, same_process):
    window = BASE_CACHE_WINDOW_COUNTER_METRICS
    promoted = deltas.get(window["promoted"])
    evicted = deltas.get(window["evicted"])
    throttled = deltas.get(window["admission_throttled"])
    relaxed = deltas.get(window["admission_relaxed"])
    result = {
        "baseCacheWindowAdmittedPerBlock": safe_ratio(
            deltas.get(BASE_CACHE_WINDOW_ADMITTED_METRIC), interval_blocks
        ),
        "baseCacheWindowPromotedPerBlock": safe_ratio(promoted, interval_blocks),
        "baseCacheWindowEvictedPerBlock": safe_ratio(evicted, interval_blocks),
        "baseCacheWindowAdmissionBypassedPerBlock": safe_ratio(
            deltas.get(window["admission_bypassed"]), interval_blocks
        ),
        "baseCacheWindowThrottleAdjustmentsPerBlock": safe_ratio(
            throttled, interval_blocks
        ),
        "baseCacheWindowRelaxAdjustmentsPerBlock": safe_ratio(relaxed, interval_blocks),
        "baseCacheWindowOutcomePromotionRatio": safe_ratio(
            promoted, positive_sum(promoted, evicted)
        ),
        "baseCacheWindowThrottleAdjustmentShare": safe_ratio(
            throttled, positive_sum(throttled, relaxed)
        ),
        "baseCacheWindowCacheResolvedPerBlock": safe_ratio(
            deltas.get("blockbuffer/commitment_parent/window/cache_resolved"),
            interval_blocks,
        ),
        "baseCacheWindowCacheResolvedShare": safe_ratio(
            deltas.get("blockbuffer/commitment_parent/window/cache_resolved"),
            deltas.get("blockbuffer/commitment_parent/cache/resolved"),
        ),
    }

    occupancy_end = {}
    occupancy_start = {}
    occupancy_change = {}
    for tier, metrics in BASE_CACHE_OCCUPANCY_METRICS.items():
        occupancy_end[tier] = {}
        occupancy_start[tier] = {}
        occupancy_change[tier] = {}
        for unit, name in metrics.items():
            value = current.get(name)
            old = previous_metrics.get(name)
            occupancy_end[tier][unit] = value
            if (
                same_process
                and isinstance(value, (int, float))
                and not isinstance(value, bool)
                and isinstance(old, (int, float))
                and not isinstance(old, bool)
            ):
                occupancy_start[tier][unit] = old
                occupancy_change[tier][unit] = value - old
            else:
                occupancy_start[tier][unit] = None
                occupancy_change[tier][unit] = None

    totals_end = {}
    totals_start = {}
    totals_change = {}
    for unit in ("entries", "bytes"):
        totals_end[unit] = positive_sum(
            *(occupancy_end[tier][unit] for tier in BASE_CACHE_OCCUPANCY_METRICS)
        )
        totals_start[unit] = positive_sum(
            *(occupancy_start[tier][unit] for tier in BASE_CACHE_OCCUPANCY_METRICS)
        )
        totals_change[unit] = positive_sum(
            *(occupancy_change[tier][unit] for tier in BASE_CACHE_OCCUPANCY_METRICS)
        )

    result.update(
        {
            "baseCacheOccupancyMetricCoverageRatio": safe_ratio(
                sum(
                    1
                    for metrics in BASE_CACHE_OCCUPANCY_METRICS.values()
                    for name in metrics.values()
                    if current.get(name) is not None
                ),
                len(BASE_CACHE_OCCUPANCY_POINT_METRICS),
            ),
            "baseCacheOccupancyEnd": occupancy_end,
            "baseCacheOccupancyStart": occupancy_start,
            "baseCacheOccupancyChange": occupancy_change,
            "baseCacheOccupancyTotalEnd": totals_end,
            "baseCacheOccupancyTotalStart": totals_start,
            "baseCacheOccupancyTotalChange": totals_change,
            "baseCacheWindowResidentByteShare": safe_ratio(
                occupancy_end["window"]["bytes"], totals_end["bytes"]
            ),
            "baseCacheRetainedChargeBytesPerEntry": safe_ratio(
                totals_end["bytes"], totals_end["entries"]
            ),
            "baseCacheCapacityBytes": current.get(BASE_CACHE_CAPACITY_METRIC),
            "baseCacheOccupancyRatio": safe_ratio(
                totals_end["bytes"], current.get(BASE_CACHE_CAPACITY_METRIC)
            ),
            "baseCacheTierBudgetBytes": {
                tier: current.get(name)
                for tier, name in BASE_CACHE_BUDGET_METRICS.items()
            },
            # trunk/window are hard reservations. Other is a soft target that
            # may be borrowed, so its ratio may exceed one without violating
            # the total cache capacity.
            "baseCacheTierBudgetOccupancyRatio": {
                tier: safe_ratio(
                    occupancy_end[tier]["bytes"], current.get(name)
                )
                for tier, name in BASE_CACHE_BUDGET_METRICS.items()
            },
        }
    )
    return result


def build_row(now, height, current, previous, sample_window=None):
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
    same_process = (
        isinstance(previous, dict)
        and previous_process_valid
        and current_process_valid
        and current_process == previous_process
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
    interval_blocks_estimate = None
    interval_height_uncertainty = None
    interval_height_tolerance = None
    interval_strict = None
    if sample_window is not None:
        previous_height_after = previous.get("heightAfter") if isinstance(previous, dict) else None
        current_height_after = sample_window["heightAfter"]
        if (
            isinstance(previous_height_after, int)
            and not isinstance(previous_height_after, bool)
            and isinstance(current_height_after, int)
            and not isinstance(current_height_after, bool)
            and current_height_after >= previous_height_after
        ):
            interval_blocks_estimate = current_height_after - previous_height_after
        previous_height_span = previous.get("heightSpanBlocks") if isinstance(previous, dict) else None
        current_height_span = sample_window["heightSpanBlocks"]
        if (
            isinstance(previous_height_span, int)
            and not isinstance(previous_height_span, bool)
            and previous_height_span >= 0
            and isinstance(current_height_span, int)
            and not isinstance(current_height_span, bool)
            and current_height_span >= 0
        ):
            # This is deliberately conservative. The metric snapshot lies
            # somewhere inside each height bracket, so the sum of both spans
            # is an upper bound on the interval denominator's uncertainty.
            interval_height_uncertainty = previous_height_span + current_height_span
        if interval_blocks_estimate is not None:
            interval_height_tolerance = max(1.0, interval_blocks_estimate * 0.01)
        interval_strict = (
            isinstance(previous, dict)
            and sample_window["heightBracketValid"]
            and previous.get("heightBracketValid") is True
            and same_process
            and not resets
            and interval is not None
            and interval_blocks_estimate is not None
            and interval_height_uncertainty is not None
            and interval_height_uncertainty <= interval_height_tolerance
        )
    interval_blocks = None
    if sample_window is not None:
        if interval_strict:
            # Explicit denominator semantics: compare the height observed by
            # the wallet call after each metrics scrape.
            interval_blocks = interval_blocks_estimate
    elif height is not None and isinstance(previous_height, int) and height >= previous_height:
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
    fold_input_updates = deltas.get("state/commitment/fold/input_updates")

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
        "commitmentFoldCallsPerBlock": safe_ratio(
            deltas.get("state/commitment/fold/calls"), interval_blocks
        ),
        "commitmentFoldInputUpdatesPerBlock": safe_ratio(
            fold_input_updates, interval_blocks
        ),
        "commitmentFoldResolvedOpsPerBlock": safe_ratio(
            deltas.get("state/commitment/fold/resolved_ops"), interval_blocks
        ),
        "commitmentFoldNanosPerInputUpdate": safe_ratio(
            deltas.get("state/commitment/fold/wall_nanos"), fold_input_updates
        ),
        "commitmentFoldErrorRatio": safe_ratio(
            deltas.get("state/commitment/fold/errors"),
            deltas.get("state/commitment/fold/calls"),
        ),
        "commitmentDurableReadsPerInputUpdate": safe_ratio(
            total_durable, fold_input_updates
        ),
        "commitmentSegmentPhysicalReadCallsPerInputUpdate": safe_ratio(
            deltas.get(
                physical_read_group("commitmentSegmentPhysicalRead")["metrics"]["calls"]
            ),
            fold_input_updates,
        ),
        "sstRandomReadCallsPerInputUpdate": safe_ratio(
            deltas.get(SST_FD_ACCESS_METRICS["random"]["calls"]), fold_input_updates
        ),
        "sstRandomReadNanosPerInputUpdate": safe_ratio(
            deltas.get(SST_FD_ACCESS_METRICS["random"]["nanos"]), fold_input_updates
        ),
        "pebbleCursorSeekCallsPerInputUpdate": safe_ratio(
            deltas.get(PEBBLE_COMMITMENT_CURSOR_METRICS["seek_calls"]),
            fold_input_updates,
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
        "prunePassesPerBlock": safe_ratio(
            deltas.get("state/prune/passes"), interval_blocks
        ),
        "pruneVerificationFullPerBlock": safe_ratio(
            deltas.get("state/prune/verification/full"), interval_blocks
        ),
        "pruneVerificationPersistentHitsPerBlock": safe_ratio(
            deltas.get("state/prune/verification/persisted_hits"), interval_blocks
        ),
        "pruneVerificationMemoryHitsPerBlock": safe_ratio(
            deltas.get("state/prune/verification/memory_hits"), interval_blocks
        ),
        "pruneChecksumStartedPerBlock": safe_ratio(
            deltas.get("state/prune/verification/checksum/started"), interval_blocks
        ),
        "pruneChecksumCompletedPerBlock": safe_ratio(
            deltas.get("state/prune/verification/checksum/completed"), interval_blocks
        ),
        "pruneChecksumFailedPerBlock": safe_ratio(
            deltas.get("state/prune/verification/checksum/failed"), interval_blocks
        ),
        "pruneChecksumCanceledPerBlock": safe_ratio(
            deltas.get("state/prune/verification/checksum/canceled"), interval_blocks
        ),
        "pruneChecksumBytesCompletedPerBlock": safe_ratio(
            deltas.get("state/prune/verification/checksum/bytes/completed"),
            interval_blocks,
        ),
        "pruneChecksumInFlight": current.get(
            "state/prune/verification/checksum/inflight"
        ),
        "pruneChecksumBytesInFlight": current.get(
            "state/prune/verification/checksum/bytes/inflight"
        ),
        "pruneVerificationCacheEntries": current.get(
            "state/prune/verification/cache_entries"
        ),
        "pruneVerificationActiveSegments": current.get(
            "state/prune/verification/active_segments"
        ),
        "pruneVerificationActiveBytes": current.get(
            "state/prune/verification/active_bytes"
        ),
        "pruneLastPassSeconds": safe_ratio(
            current.get("state/prune/lastpass/duration"), 1_000_000_000
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
    analysis.update(build_exact_depth_analysis(current, deltas, interval_blocks))
    analysis.update(build_sync_import_window_analysis(now, current))
    analysis.update(
        build_base_cache_analysis(
            current, previous_metrics, deltas, interval_blocks, same_process
        )
    )
    row = {
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
    if sample_window is not None:
        row.update(sample_window)
        row["intervalStrict"] = interval_strict
        row["intervalBlocksEstimate"] = interval_blocks_estimate
        row["intervalHeightBasis"] = "heightAfter"
        row["intervalHeightUncertaintyBlocks"] = interval_height_uncertainty
        row["intervalHeightToleranceBlocks"] = interval_height_tolerance
    return row


def append_row(path, row):
    encoded = json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    print(encoded, flush=True)
    if path:
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("a", encoding="utf-8") as stream:
            stream.write(encoded + "\n")


def sample_once(args, previous):
    scrape_start = time.time()
    height_before = None
    if args.wallet_url:
        try:
            height_before = wallet_height(args.wallet_url, args.timeout)
        except (OSError, RuntimeError, ValueError, json.JSONDecodeError):
            height_before = None
    metrics = select_metrics(fetch_metrics(args.metrics_url, args.timeout))
    height_after = None
    if args.wallet_url:
        try:
            height_after = wallet_height(args.wallet_url, args.timeout)
        except (OSError, RuntimeError, ValueError, json.JSONDecodeError):
            height_after = None
    scrape_end = time.time()
    height_span = None
    if isinstance(height_before, int) and isinstance(height_after, int):
        height_span = height_after - height_before
    height_bracket_valid = (
        isinstance(height_span, int) and height_span >= 0 and scrape_end >= scrape_start
    )
    # Keep a tight per-scrape signal for operators, but do not require it for a
    # long interval: intervalStrict applies the relative two-bracket budget.
    scrape_strict = height_bracket_valid and height_span <= 1
    sample_window = {
        "scrapeStartUnix": scrape_start,
        "scrapeEndUnix": scrape_end,
        "scrapeSeconds": scrape_end - scrape_start if scrape_end >= scrape_start else None,
        "heightBefore": height_before,
        "heightAfter": height_after,
        "heightSpanBlocks": height_span,
        "heightBracketValid": height_bracket_valid,
        "scrapeStrict": scrape_strict,
    }
    # Retain the latest observed height for operator visibility. build_row only
    # uses heightAfter as a denominator when the two bracket spans fit the
    # interval-relative uncertainty budget.
    height = height_after if height_after is not None else height_before
    return build_row(scrape_end, height, metrics, previous, sample_window)


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

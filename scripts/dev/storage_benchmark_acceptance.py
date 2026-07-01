#!/usr/bin/env python3
"""Validate storage benchmark JSONL samples against release/soak gates."""

import argparse
import json
import re
import sys
from pathlib import Path


ALERT_STATUS_FIELDS = (
    "storageAlertStatus",
    "freezerAlertStatus",
    "stageVerifyStatus",
    "modeAlertStatus",
    "snapshotAlertStatus",
)

PROMETHEUS_REQUIRED_SNIPPETS = (
    ("gtron_storage_alert_status{", "gtron_storage_alert_status"),
    ("# TYPE gtron_storage_alert_issue gauge", "gtron_storage_alert_issue"),
)

PROMETHEUS_PRUNE_BOUNDARY_FIELDS = (
    "coldFreezerToBlock",
    "chainLookupPruneToBlock",
    "tailPrunedThroughBlock",
    "balanceTracePruneToBlock",
    "sectionBloomPruneToSection",
)

PROMETHEUS_STATUS_VALUES = {
    "ok": 0,
    "warning": 1,
    "critical": 2,
}

BENCHMARK_STATUS_VALUES = {
    "ok": 0,
    "warning": 1,
    "storage-alerts-critical": 2,
    "unknown": 3,
}

BENCHMARK_PROMETHEUS_STATUS_METRIC = "gtron_storage_benchmark_status"

BENCHMARK_PROMETHEUS_SUM_FIELDS = (
    ("gtron_storage_benchmark_height", ("height",)),
    ("gtron_storage_benchmark_elapsed_seconds", ("elapsedSeconds",)),
    ("gtron_storage_benchmark_datadir_bytes", ("datadirBytes",)),
    ("gtron_storage_benchmark_chaindata_bytes", ("chaindataBytes",)),
    ("gtron_storage_benchmark_ancient_bytes", ("ancientBytes",)),
    ("gtron_storage_benchmark_snapshot_bytes", ("snapshotBytes",)),
    ("gtron_storage_benchmark_cold_archive_bytes", ("ancientBytes", "snapshotBytes")),
    ("gtron_storage_benchmark_derived_index_bytes", ("derivedIndexBytes",)),
    ("gtron_storage_benchmark_snapshot_sidecar_share_milli", ("snapshotSidecarShareMilli",)),
    ("gtron_storage_benchmark_archive_api_checks", ("archiveApiChecks",)),
    ("gtron_storage_benchmark_archive_api_block", ("archiveApiBlock",)),
    ("gtron_storage_benchmark_archive_api_failures", ("archiveApiFailures",)),
)

BENCHMARK_PROMETHEUS_PER_BLOCK_FIELDS = (
    ("gtron_storage_benchmark_datadir_bytes_per_block", ("datadirBytes",)),
    ("gtron_storage_benchmark_hot_bytes_per_block", ("chaindataBytes",)),
    ("gtron_storage_benchmark_cold_archive_bytes_per_block", ("ancientBytes", "snapshotBytes")),
    ("gtron_storage_benchmark_derived_index_bytes_per_block", ("derivedIndexBytes",)),
)

BENCHMARK_PROMETHEUS_SNAPSHOT_POINT_FIELDS = tuple(
    (
        f"gtron_storage_benchmark_snapshot_point_{metric_prefix}_{metric_suffix}",
        f"{field_prefix}{field_suffix}",
    )
    for metric_prefix, field_prefix in (
        ("tx_hash_lookup", "snapshotPointTxHashLookup"),
        ("event_log_index", "snapshotPointEventLogIndex"),
        ("state_history_accessor", "snapshotPointStateHistoryAccessor"),
        ("latest_btree", "snapshotPointLatestBTree"),
        ("chain_freezer_accessor", "snapshotPointChainFreezerAccessor"),
        ("code_domain", "snapshotPointCodeDomain"),
        ("commitment_snapshot", "snapshotPointCommitmentSnapshot"),
    )
    for metric_suffix, field_suffix in (
        ("segments", "Segments"),
        ("bytes", "Bytes"),
        ("payload_bytes", "PayloadBytes"),
        ("sidecar_bytes", "SidecarBytes"),
        ("sidecar_share_milli", "SidecarShareMilli"),
        ("snapshot_share_milli", "SnapshotShareMilli"),
    )
)

BENCHMARK_PROMETHEUS_DIRECT_FIELDS = (
    ("gtron_storage_benchmark_archive_api_depth_blocks", "archiveApiDepthBlocks"),
    *BENCHMARK_PROMETHEUS_SNAPSHOT_POINT_FIELDS,
    ("gtron_storage_benchmark_cold_freezer_to_block", "coldFreezerToBlock"),
    ("gtron_storage_benchmark_derived_index_to_block", "derivedIndexToBlock"),
    ("gtron_storage_benchmark_chain_lookup_prune_to_block", "chainLookupPruneToBlock"),
    ("gtron_storage_benchmark_tail_pruned_through_block", "tailPrunedThroughBlock"),
    ("gtron_storage_benchmark_balance_trace_prune_to_block", "balanceTracePruneToBlock"),
    ("gtron_storage_benchmark_section_bloom_prune_to_section", "sectionBloomPruneToSection"),
    ("gtron_storage_benchmark_signed_cold_prune", "signedColdPrune"),
    ("gtron_storage_benchmark_tail_pruned_files", "tailPrunedFiles"),
    ("gtron_storage_benchmark_history_window", "historyWindow"),
    ("gtron_storage_benchmark_event_log_index_segments", "eventLogIndexSegments"),
    ("gtron_storage_benchmark_event_log_index_from_block", "eventLogIndexFromBlock"),
    ("gtron_storage_benchmark_event_log_index_to_block", "eventLogIndexToBlock"),
    ("gtron_storage_benchmark_event_log_index_address_keys", "eventLogIndexAddressKeys"),
    ("gtron_storage_benchmark_event_log_index_address_postings", "eventLogIndexAddressPostings"),
    (
        "gtron_storage_benchmark_event_log_index_address_avg_postings_milli",
        "eventLogIndexAddressAvgPostingsMilli",
    ),
    ("gtron_storage_benchmark_event_log_index_address_max_postings", "eventLogIndexAddressMaxPostings"),
    (
        "gtron_storage_benchmark_event_log_index_address_singleton_keys",
        "eventLogIndexAddressSingletonKeys",
    ),
    (
        "gtron_storage_benchmark_event_log_index_address_multi_posting_keys",
        "eventLogIndexAddressMultiPostingKeys",
    ),
    ("gtron_storage_benchmark_event_log_index_topic_keys", "eventLogIndexTopicKeys"),
    ("gtron_storage_benchmark_event_log_index_topic_postings", "eventLogIndexTopicPostings"),
    (
        "gtron_storage_benchmark_event_log_index_topic_avg_postings_milli",
        "eventLogIndexTopicAvgPostingsMilli",
    ),
    ("gtron_storage_benchmark_event_log_index_topic_max_postings", "eventLogIndexTopicMaxPostings"),
    (
        "gtron_storage_benchmark_event_log_index_topic_singleton_keys",
        "eventLogIndexTopicSingletonKeys",
    ),
    (
        "gtron_storage_benchmark_event_log_index_topic_multi_posting_keys",
        "eventLogIndexTopicMultiPostingKeys",
    ),
)

BENCHMARK_PROMETHEUS_SIGNED_INTEGER_DIRECT_FIELDS = {
    "coldFreezerToBlock",
    "derivedIndexToBlock",
    "chainLookupPruneToBlock",
    "tailPrunedThroughBlock",
    "balanceTracePruneToBlock",
    "sectionBloomPruneToSection",
    "eventLogIndexFromBlock",
    "eventLogIndexToBlock",
}

BENCHMARK_PROMETHEUS_NON_NEGATIVE_INTEGER_DIRECT_FIELDS = {
    "archiveApiDepthBlocks",
    "snapshotPointTxHashLookupSegments",
    "snapshotPointEventLogIndexSegments",
    "snapshotPointStateHistoryAccessorSegments",
    "snapshotPointLatestBTreeSegments",
    "snapshotPointChainFreezerAccessorSegments",
    "snapshotPointCodeDomainSegments",
    "snapshotPointCommitmentSnapshotSegments",
    "signedColdPrune",
    "tailPrunedFiles",
    "historyWindow",
    "eventLogIndexSegments",
    "eventLogIndexAddressKeys",
    "eventLogIndexAddressPostings",
    "eventLogIndexAddressAvgPostingsMilli",
    "eventLogIndexAddressMaxPostings",
    "eventLogIndexAddressSingletonKeys",
    "eventLogIndexAddressMultiPostingKeys",
    "eventLogIndexTopicKeys",
    "eventLogIndexTopicPostings",
    "eventLogIndexTopicAvgPostingsMilli",
    "eventLogIndexTopicMaxPostings",
    "eventLogIndexTopicSingletonKeys",
    "eventLogIndexTopicMultiPostingKeys",
}

DEFAULT_ARCHIVE_API_METHODS = (
    "eth_getBlockByNumber",
    "eth_getBlockByHash",
    "eth_getBlockTransactionCountByNumber",
    "eth_getBlockTransactionCountByHash",
    "eth_getUncleCountByBlockNumber",
    "eth_getUncleCountByBlockHash",
    "eth_getUncleByBlockNumberAndIndex",
    "eth_getUncleByBlockHashAndIndex",
    "eth_getBlockReceipts",
    "eth_getBalance",
    "eth_getCode",
    "eth_getStorageAt",
    "eth_getLogs",
)

ARCHIVE_API_CALL_METHODS = (
    "eth_call",
    "debug_traceCall",
    "eth_estimateGas",
)

ARCHIVE_API_TX_METHODS = (
    "eth_getTransactionByHash",
    "eth_getTransactionReceipt",
    "eth_getTransactionByBlockNumberAndIndex",
    "eth_getTransactionByBlockHashAndIndex",
)

ARCHIVE_API_TRACE_TX_METHOD = "debug_traceTransaction"
ARCHIVE_API_TRACE_BLOCK_METHODS = (
    "debug_traceBlockByNumber",
    "debug_traceBlockByHash",
)

ARCHIVE_API_METHOD_SUCCESS_METRIC = "gtron_storage_benchmark_archive_api_method_success"
ARCHIVE_API_TX_METHOD_SUCCESS_METRIC = "gtron_storage_benchmark_archive_api_tx_method_success"


def row_sort_key(row):
    unix = row.get("unix")
    try:
        unix_value = int(unix)
    except (TypeError, ValueError):
        unix_value = 0
    return (unix_value, row["_line"])


def latest_rows(rows):
    latest = {}
    for row in rows:
        key = (str(row.get("mode", "")), str(row.get("role", "")))
        if key not in latest or row_sort_key(row) > row_sort_key(latest[key]):
            latest[key] = row
    return latest


def load_rows(path):
    rows = []
    issues = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        return [], [f"read {path}: {exc}"]
    for line_no, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            issues.append(f"{path}:{line_no}: invalid JSON: {exc}")
            continue
        if not isinstance(row, dict):
            issues.append(f"{path}:{line_no}: expected JSON object row")
            continue
        row["_line"] = line_no
        rows.append(row)
    if not rows and not issues:
        issues.append(f"{path}: no JSONL rows found")
    return rows, issues


def split_modes(values):
    modes = []
    for value in values:
        for mode in value.split(","):
            mode = mode.strip()
            if mode:
                modes.append(mode)
    return modes


def split_csv_values(values):
    out = []
    for value in values:
        for item in value.split(","):
            item = item.strip()
            if item:
                out.append(item)
    return out


def line_label(row):
    mode = row.get("mode", "")
    role = row.get("role", "")
    return f"line {row['_line']} {mode}/{role}"


def check_statuses(rows, allow_warning):
    issues = []
    allowed_overall = {"ok"}
    if allow_warning:
        allowed_overall.add("warning")
    allowed_component = {"ok"}
    if allow_warning:
        allowed_component.add("warning")
    for row in rows:
        status = str(row.get("status", "")).lower()
        if status not in allowed_overall:
            issues.append(f"{line_label(row)} status={row.get('status')!r} is not accepted")
        for field in ALERT_STATUS_FIELDS:
            if field not in row:
                continue
            value = str(row.get(field, "")).lower()
            if value and value not in allowed_component:
                issues.append(f"{line_label(row)} {field}={row.get(field)!r} is not accepted")
    return issues


def latest_for(rows, mode=None, role=None):
    matches = []
    for row in rows:
        if mode is not None and str(row.get("mode", "")) != mode:
            continue
        if role is not None and str(row.get("role", "")) != role:
            continue
        matches.append(row)
    if not matches:
        return None
    return max(matches, key=row_sort_key)


def as_number(row, field):
    value = row.get(field)
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_int(row, field):
    value = row.get(field)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return None
    if isinstance(value, str):
        try:
            return int(value, 10)
        except ValueError:
            return None
    return None


def as_bool(row, field):
    value = row.get(field)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "ok"}
    if isinstance(value, (int, float)):
        return value != 0
    return False


def approx_equal(got, want, tolerance=1e-9):
    if got is None or want is None:
        return False
    return abs(got - want) <= tolerance


def parse_threshold(raw):
    if "=" not in raw:
        raise ValueError(f"{raw!r} must use FIELD=VALUE")
    field_ref, raw_value = raw.split("=", 1)
    field_ref = field_ref.strip()
    if not field_ref:
        raise ValueError(f"{raw!r} has an empty field reference")
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{raw!r} has a non-numeric threshold") from exc
    parts = field_ref.split(".")
    if len(parts) == 1:
        return None, None, parts[0], value
    if len(parts) == 2:
        return parts[0], None, parts[1], value
    if len(parts) == 3:
        return parts[0], parts[1], parts[2], value
    raise ValueError(f"{raw!r} must be FIELD, MODE.FIELD, or MODE.ROLE.FIELD")


def rows_for_threshold(rows, mode, role):
    if mode is None:
        return list(latest_rows(rows).values())
    row = latest_for(rows, mode=mode, role=role)
    return [] if row is None else [row]


def check_thresholds(rows, raws, op_name, predicate):
    issues = []
    for raw in raws:
        try:
            mode, role, field, want = parse_threshold(raw)
        except ValueError as exc:
            issues.append(str(exc))
            continue
        matches = rows_for_threshold(rows, mode, role)
        if not matches:
            scope = "/".join(part for part in (mode, role) if part)
            issues.append(f"{raw}: no latest row matched scope {scope!r}")
            continue
        for row in matches:
            got = as_number(row, field)
            if got is None:
                issues.append(f"{raw}: {line_label(row)} field {field!r} is missing or non-numeric")
                continue
            if not predicate(got, want):
                issues.append(
                    f"{raw}: {line_label(row)} field {field}={got:g} failed {op_name} {want:g}"
                )
    return issues


def parse_size_reduction(raw):
    if "=" not in raw:
        raise ValueError(f"{raw!r} must use MODE:BASE_MODE:FIELD=RATIO")
    scope, raw_ratio = raw.split("=", 1)
    parts = [part.strip() for part in scope.split(":")]
    if len(parts) != 3 or any(not part for part in parts):
        raise ValueError(f"{raw!r} must use MODE:BASE_MODE:FIELD=RATIO")
    try:
        ratio = float(raw_ratio)
    except ValueError as exc:
        raise ValueError(f"{raw!r} has a non-numeric ratio") from exc
    if ratio < 0 or ratio > 1:
        raise ValueError(f"{raw!r} ratio must be between 0 and 1")
    return parts[0], parts[1], parts[2], ratio


def check_size_reductions(rows, raws, role):
    issues = []
    role_suffix = "" if role is None else f" for role {role!r}"
    for raw in raws:
        try:
            mode, base_mode, field, want_ratio = parse_size_reduction(raw)
        except ValueError as exc:
            issues.append(str(exc))
            continue

        row = latest_for(rows, mode=mode, role=role)
        if row is None:
            issues.append(f"{raw}: no latest row for candidate mode {mode!r}{role_suffix}")
            continue
        base = latest_for(rows, mode=base_mode, role=role)
        if base is None:
            issues.append(f"{raw}: no latest row for baseline mode {base_mode!r}{role_suffix}")
            continue

        got = as_number(row, field)
        if got is None:
            issues.append(f"{raw}: {line_label(row)} {field} is missing or non-numeric")
            continue
        baseline = as_number(base, field)
        if baseline is None:
            issues.append(f"{raw}: {line_label(base)} {field} is missing or non-numeric")
            continue
        if baseline <= 0:
            issues.append(f"{raw}: {line_label(base)} {field}={baseline:g} must be > 0")
            continue

        reduction = (baseline - got) / baseline
        if reduction + 1e-12 < want_ratio:
            issues.append(
                f"{raw}: {mode} {field} reduction={reduction:.2%}, "
                f"want >= {want_ratio:.2%} versus {base_mode} "
                f"(candidate {line_label(row)}={got:g}, baseline {line_label(base)}={baseline:g})"
            )
    return issues


def check_max_bytes_per_block(rows, maximum, label, fields, metric_name):
    if maximum is None:
        return []
    issues = []
    for row in latest_rows(rows).values():
        height = as_number(row, "height")
        if height is None or height <= 0:
            issues.append(
                f"{line_label(row)} height={row.get('height')!r}, want > 0 "
                f"for {label} bytes-per-block evidence"
            )
            continue
        total = 0.0
        missing = []
        for field in fields:
            value = as_number(row, field)
            if value is None or value < 0:
                missing.append(field)
                continue
            total += value
        if missing:
            issues.append(
                f"{line_label(row)} {label} bytes-per-block evidence missing fields: "
                + ",".join(missing)
            )
            continue
        per_block = total / height
        if per_block > maximum:
            issues.append(
                f"{line_label(row)} {metric_name}={per_block:g} failed <= "
                f"max {label} bytes per block {maximum:g} "
                f"(bytes={total:g} height={height:g})"
            )
    return issues


def resolve_artifact(result_path, raw_path):
    path = Path(str(raw_path))
    if path.is_absolute():
        return path
    return result_path.parent / path


def check_prometheus_text(label, path, text, row):
    issues = []
    for needle, name in PROMETHEUS_REQUIRED_SNIPPETS:
        if needle not in text:
            issues.append(f"{label} prometheus artifact {path} missing {name}")
    if "gtron_storage_alert_status{" in text:
        issues.extend(
            check_prometheus_metric_present(label, path, text, "gtron_storage_alert_status", row)
        )
        issues.extend(check_prometheus_alert_status_value(label, path, text, row))
    return issues


def parse_prometheus_labels(raw):
    labels = {}
    for match in re.finditer(r'([A-Za-z_][A-Za-z0-9_]*)="((?:\\.|[^"\\])*)"', raw):
        value = match.group(2)
        value = value.replace(r"\\", "\\").replace(r"\"", '"').replace(r"\n", "\n")
        labels[match.group(1)] = value
    return labels


def prometheus_label_matches(row, labels):
    want_datadir = row.get("datadir")
    if want_datadir:
        return labels.get("datadir") == str(want_datadir)
    return True


def prometheus_issue_keys(text, row):
    keys = set()
    for line in text.splitlines():
        match = re.match(r"^gtron_storage_alert_issue\{([^}]*)\}\s+", line.strip())
        if not match:
            continue
        labels = parse_prometheus_labels(match.group(1))
        if not prometheus_label_matches(row, labels):
            continue
        component = labels.get("component")
        kind = labels.get("kind")
        severity = labels.get("severity")
        if component and kind and severity:
            keys.add((component, kind, severity))
    return keys


def prometheus_metric_samples(text, metric):
    samples = []
    pattern = re.compile(rf"^{re.escape(metric)}(?:\{{([^}}]*)\}})?\s+([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)$")
    for line in text.splitlines():
        match = pattern.match(line.strip())
        if not match:
            continue
        try:
            value = float(match.group(2))
        except ValueError:
            continue
        samples.append((parse_prometheus_labels(match.group(1) or ""), value))
    return samples


def prometheus_metric_value(text, metric, row=None):
    samples = prometheus_metric_samples(text, metric)
    if row is not None:
        samples = [(labels, value) for labels, value in samples if prometheus_label_matches(row, labels)]
    if not samples:
        return None
    return samples[-1][1]


def check_prometheus_metric_value(label, path, text, metric, want, row):
    got = prometheus_metric_value(text, metric, row)
    if got is None:
        return [f"{label} prometheus artifact {path} missing {metric}"]
    if got != float(want):
        return [f"{label} prometheus artifact {path} {metric}={got:g}, want {want:g}"]
    return []


def check_prometheus_metric_present(label, path, text, metric, row):
    if prometheus_metric_value(text, metric, row) is None:
        return [f"{label} prometheus artifact {path} missing {metric}"]
    return []


def prometheus_prune_boundary_value(text, field, row):
    matched = [
        value
        for labels, value in prometheus_metric_samples(
            text, "gtron_storage_prune_boundary_block"
        )
        if prometheus_label_matches(row, labels) and labels.get("field") == field
    ]
    if not matched:
        return None
    return matched[-1]


def check_prometheus_prune_boundaries(label, path, text, row):
    issues = []
    if "signedColdPrune" in row:
        want = 1 if as_bool(row, "signedColdPrune") else 0
        issues.extend(
            check_prometheus_metric_value(
                label,
                path,
                text,
                "gtron_storage_signed_cold_prune",
                want,
                row,
            )
        )
    for field in PROMETHEUS_PRUNE_BOUNDARY_FIELDS:
        want = as_int(row, field)
        if want is None:
            if field_present(row, field):
                issues.append(
                    f"{label} {field}={row.get(field)!r}, want integer "
                    "for prometheus prune boundary evidence"
                )
            continue
        got = prometheus_prune_boundary_value(text, field, row)
        if got is None:
            issues.append(
                f"{label} prometheus artifact {path} missing "
                f"gtron_storage_prune_boundary_block field={field!r}"
            )
        elif not got.is_integer():
            issues.append(
                f"{label} prometheus artifact {path} "
                f"gtron_storage_prune_boundary_block field={field!r}={got:g}, "
                "want integer"
            )
        elif int(got) != want:
            issues.append(
                f"{label} prometheus artifact {path} "
                f"gtron_storage_prune_boundary_block field={field!r}={int(got):g}, "
                f"want {want:g}"
            )
    return issues


def check_prometheus_alert_status_value(label, path, text, row):
    status = str(row.get("storageAlertStatus", "")).lower()
    if status not in PROMETHEUS_STATUS_VALUES:
        return []
    return check_prometheus_metric_value(
        label,
        path,
        text,
        "gtron_storage_alert_status",
        PROMETHEUS_STATUS_VALUES[status],
        row,
    )


def row_alert_issue_keys(row):
    keys = set()
    fields = {
        "freezer": "freezerAlertDetails",
        "stage": "stageVerifyDetails",
        "mode": "modeAlertDetails",
        "snapshot": "snapshotAlertDetails",
    }
    for component, field in fields.items():
        details = row.get(field)
        if not isinstance(details, list):
            continue
        for detail in details:
            if not isinstance(detail, dict):
                continue
            kind = detail.get("kind")
            severity = detail.get("severity")
            if kind and severity:
                keys.add((component, str(kind), str(severity)))
    return keys


def check_prometheus_issue_kinds(label, path, text, row):
    issues = []
    actual = prometheus_issue_keys(text, row)
    for component, kind, severity in sorted(row_alert_issue_keys(row)):
        if (component, kind, severity) not in actual:
            issues.append(
                f"{label} prometheus artifact {path} missing "
                f"gtron_storage_alert_issue component={component!r} kind={kind!r} severity={severity!r}"
            )
    return issues


def check_prometheus_stage_pipeline(label, path, text, row):
    if "stageAlertPipelinePending" not in row and "stageAlertPipelineIssues" not in row:
        return []
    issues = []
    required = (
        ("gtron_storage_stage_pipeline_pending{", "gtron_storage_stage_pipeline_pending"),
        ("gtron_storage_stage_pipeline_issues{", "gtron_storage_stage_pipeline_issues"),
    )
    if "stageAlertPipelineComplete" in row:
        required = (
            ("gtron_storage_stage_pipeline_complete{", "gtron_storage_stage_pipeline_complete"),
            *required,
        )
    if row.get("stageAlertPipelineNext"):
        required = (
            *required,
            (
                "gtron_storage_stage_pipeline_next_target_block{",
                "gtron_storage_stage_pipeline_next_target_block",
            ),
            (
                "gtron_storage_stage_pipeline_next_current_block{",
                "gtron_storage_stage_pipeline_next_current_block",
            ),
        )
    for needle, name in required:
        if needle not in text:
            issues.append(f"{label} prometheus artifact {path} missing {name}")
    if "stageAlertPipelineComplete" in row:
        issues.extend(
            check_prometheus_metric_value(
                label,
                path,
                text,
                "gtron_storage_stage_pipeline_complete",
                1 if as_bool(row, "stageAlertPipelineComplete") else 0,
                row,
            )
        )
    if "stageAlertPipelinePending" in row:
        pending = as_non_negative_int(row, "stageAlertPipelinePending")
        if pending is not None:
            issues.extend(
                check_prometheus_metric_value(
                    label, path, text, "gtron_storage_stage_pipeline_pending", pending, row
                )
            )
        else:
            issues.append(
                f"{label} stageAlertPipelinePending="
                f"{row.get('stageAlertPipelinePending')!r}, want non-negative integer"
            )
    if "stageAlertPipelineIssues" in row:
        count = as_non_negative_int(row, "stageAlertPipelineIssues")
        if count is not None:
            issues.extend(
                check_prometheus_metric_value(
                    label, path, text, "gtron_storage_stage_pipeline_issues", count, row
                )
            )
        else:
            issues.append(
                f"{label} stageAlertPipelineIssues="
                f"{row.get('stageAlertPipelineIssues')!r}, want non-negative integer"
            )
    next_stage = row.get("stageAlertPipelineNext")
    if next_stage:
        want_target = as_non_negative_int(row, "stageAlertPipelineNextTarget")
        if want_target is None:
            issues.append(
                f"{label} stageAlertPipelineNextTarget="
                f"{row.get('stageAlertPipelineNextTarget')!r}, want non-negative integer"
            )
        want_status = str(row.get("stageAlertPipelineNextStatus", ""))
        want_upstream = str(row.get("stageAlertPipelineNextUpstream", ""))

        def labels_match(labels):
            return (
                prometheus_label_matches(row, labels) and labels.get("stage") == str(next_stage)
                and (not want_status or labels.get("status") == want_status)
                and (not want_upstream or labels.get("upstream") == want_upstream)
            )

        candidates = prometheus_metric_samples(text, "gtron_storage_stage_pipeline_next_target_block")
        matched = [
            value
            for labels, value in candidates
            if labels_match(labels)
        ]
        if not matched:
            issues.append(
                f"{label} prometheus artifact {path} missing next pipeline target "
                f"stage={next_stage!r} status={want_status!r} upstream={want_upstream!r}"
            )
        elif want_target is not None and matched[-1] != want_target:
            issues.append(
                f"{label} prometheus artifact {path} next pipeline target "
                f"stage={next_stage!r} status={want_status!r} upstream={want_upstream!r} "
                f"value={matched[-1]:g}, want {want_target:g}"
            )

        want_current = as_non_negative_int(row, "stageAlertPipelineNextCurrent")
        if want_current is None:
            issues.append(
                f"{label} stageAlertPipelineNextCurrent="
                f"{row.get('stageAlertPipelineNextCurrent')!r}, want non-negative integer"
            )
        candidates = prometheus_metric_samples(text, "gtron_storage_stage_pipeline_next_current_block")
        matched = [
            value
            for labels, value in candidates
            if labels_match(labels)
        ]
        if not matched:
            issues.append(
                f"{label} prometheus artifact {path} missing next pipeline current "
                f"stage={next_stage!r} status={want_status!r} upstream={want_upstream!r}"
            )
        elif want_current is not None and matched[-1] != want_current:
            issues.append(
                f"{label} prometheus artifact {path} next pipeline current "
                f"stage={next_stage!r} status={want_status!r} upstream={want_upstream!r} "
                f"value={matched[-1]:g}, want {want_current:g}"
            )
    return issues


def check_prometheus_artifacts(result_path, rows):
    issues = []
    for row in latest_rows(rows).values():
        raw_path = row.get("storageAlertPrometheus")
        if not raw_path:
            issues.append(f"{line_label(row)} missing storageAlertPrometheus")
            continue
        path = resolve_artifact(result_path, raw_path)
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            issues.append(f"{line_label(row)} prometheus artifact {path}: {exc}")
            continue
        issues.extend(check_prometheus_text(line_label(row), path, text, row))
        issues.extend(check_prometheus_issue_kinds(line_label(row), path, text, row))
        issues.extend(check_prometheus_stage_pipeline(line_label(row), path, text, row))
        issues.extend(
            check_prometheus_prune_boundaries(line_label(row), path, text, row)
        )
    return issues


def benchmark_prometheus_expected(row, fields, divisor_field=None):
    total = 0.0
    missing = []
    for field in fields:
        value = as_number(row, field)
        if value is None:
            missing.append(field)
            continue
        total += value
    if missing:
        return None, missing
    if divisor_field:
        divisor = as_number(row, divisor_field)
        if divisor is None or divisor <= 0:
            return None, [divisor_field]
        total /= divisor
    return total, []


def benchmark_prometheus_metric_value(text, metric, row, extra=None):
    samples = prometheus_metric_samples(text, metric)
    if extra is None:
        extra = {}
    matches = []
    for labels, value in samples:
        if not benchmark_prometheus_label_matches(row, labels, extra):
            continue
        matches.append((labels, value))
    if not matches:
        return None
    return matches[-1][1]


def benchmark_prometheus_label_matches(row, labels, extra=None):
    if not prometheus_label_matches(row, labels):
        return False
    for field in ("profile", "mode", "role", "status"):
        if field in row:
            value = row.get(field)
            if value is not None and str(value) != "" and labels.get(field) != str(value):
                return False
    if extra:
        for key, want in extra.items():
            if labels.get(key) != str(want):
                return False
    return True


def benchmark_status_value(row):
    status = str(row.get("status", "unknown")).lower()
    return BENCHMARK_STATUS_VALUES.get(status, BENCHMARK_STATUS_VALUES["unknown"])


def expected_archive_api_method_metrics(row, successful_methods):
    expected = set(successful_methods)
    expected.update(DEFAULT_ARCHIVE_API_METHODS)
    if as_bool(row, "archiveApiCallProbe"):
        expected.update(ARCHIVE_API_CALL_METHODS)
    if as_bool(row, "archiveApiTxProbe"):
        expected.update(ARCHIVE_API_TX_METHODS)
    if as_bool(row, "archiveApiTraceTransactionProbe"):
        expected.add(ARCHIVE_API_TRACE_TX_METHOD)
    if as_bool(row, "archiveApiTraceBlockProbe"):
        expected.update(ARCHIVE_API_TRACE_BLOCK_METHODS)
    return sorted(expected)


def check_benchmark_prometheus_method_metric(label, path, text, row, metric, method, want):
    got = benchmark_prometheus_metric_value(text, metric, row, {"method": method})
    if got is None:
        return [f"{label} benchmark prometheus artifact {path} missing {metric}{{method={method!r}}}"]
    if got != want:
        return [
            f"{label} benchmark prometheus artifact {path} "
            f"{metric}{{method={method!r}}}={got:g}, want {want:g}"
        ]
    return []


def check_benchmark_prometheus_archive_api_methods(label, path, text, row):
    issues = []
    methods = archive_api_methods(row)
    if methods is None:
        return issues
    checks = as_number(row, "archiveApiChecks")
    if (
        not methods
        and not as_bool(row, "archiveApiCallProbe")
        and not as_bool(row, "archiveApiTxProbe")
        and not as_bool(row, "archiveApiTraceTransactionProbe")
        and (checks is None or checks <= 0)
    ):
        return issues
    for method in expected_archive_api_method_metrics(row, methods):
        want = 1.0 if method in methods else 0.0
        issues.extend(
            check_benchmark_prometheus_method_metric(
                label,
                path,
                text,
                row,
                ARCHIVE_API_METHOD_SUCCESS_METRIC,
                method,
                want,
            )
        )

    tx_methods = string_set_field(row, "archiveApiTxMethods")
    if tx_methods is None and not as_bool(row, "archiveApiTxProbe"):
        return issues
    if tx_methods is None:
        tx_methods = set()
    expected_tx_methods = set(tx_methods)
    if as_bool(row, "archiveApiTxProbe"):
        expected_tx_methods.update(ARCHIVE_API_TX_METHODS)
    if as_bool(row, "archiveApiTraceTransactionProbe"):
        expected_tx_methods.add(ARCHIVE_API_TRACE_TX_METHOD)
    for method in sorted(expected_tx_methods):
        want = 1.0 if method in tx_methods else 0.0
        issues.extend(
            check_benchmark_prometheus_method_metric(
                label,
                path,
                text,
                row,
                ARCHIVE_API_TX_METHOD_SUCCESS_METRIC,
                method,
                want,
            )
        )
    return issues


def check_benchmark_prometheus_artifacts(result_path, rows):
    issues = []
    for row in latest_rows(rows).values():
        raw_path = row.get("storageBenchmarkPrometheus")
        if not raw_path:
            issues.append(f"{line_label(row)} missing storageBenchmarkPrometheus")
            continue
        path = resolve_artifact(result_path, raw_path)
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            issues.append(f"{line_label(row)} benchmark prometheus artifact {path}: {exc}")
            continue
        got_status = benchmark_prometheus_metric_value(
            text, BENCHMARK_PROMETHEUS_STATUS_METRIC, row
        )
        want_status = benchmark_status_value(row)
        if got_status is None:
            issues.append(
                f"{line_label(row)} benchmark prometheus artifact {path} "
                f"missing {BENCHMARK_PROMETHEUS_STATUS_METRIC}"
            )
        elif got_status != want_status:
            issues.append(
                f"{line_label(row)} benchmark prometheus artifact {path} "
                f"{BENCHMARK_PROMETHEUS_STATUS_METRIC}={got_status:g}, "
                f"want {want_status:g}"
            )
        for metric, fields in BENCHMARK_PROMETHEUS_SUM_FIELDS:
            want, missing = benchmark_prometheus_expected(row, fields)
            if missing:
                issues.append(
                    f"{line_label(row)} benchmark prometheus evidence missing JSONL fields "
                    f"for {metric}: " + ",".join(missing)
                )
                continue
            got = benchmark_prometheus_metric_value(text, metric, row)
            if got is None:
                issues.append(f"{line_label(row)} benchmark prometheus artifact {path} missing {metric}")
            elif got != want:
                issues.append(
                    f"{line_label(row)} benchmark prometheus artifact {path} "
                    f"{metric}={got:g}, want {want:g}"
                )
        for metric, fields in BENCHMARK_PROMETHEUS_PER_BLOCK_FIELDS:
            want, missing = benchmark_prometheus_expected(row, fields, divisor_field="height")
            if missing:
                issues.append(
                    f"{line_label(row)} benchmark prometheus evidence missing JSONL fields "
                    f"for {metric}: " + ",".join(missing)
                )
                continue
            got = benchmark_prometheus_metric_value(text, metric, row)
            if got is None:
                issues.append(f"{line_label(row)} benchmark prometheus artifact {path} missing {metric}")
            elif got != want:
                issues.append(
                    f"{line_label(row)} benchmark prometheus artifact {path} "
                    f"{metric}={got:g}, want {want:g}"
                )
        for metric, field in BENCHMARK_PROMETHEUS_DIRECT_FIELDS:
            if field not in row:
                continue
            integer_field = (
                field in BENCHMARK_PROMETHEUS_SIGNED_INTEGER_DIRECT_FIELDS
                or field in BENCHMARK_PROMETHEUS_NON_NEGATIVE_INTEGER_DIRECT_FIELDS
            )
            if field in BENCHMARK_PROMETHEUS_SIGNED_INTEGER_DIRECT_FIELDS:
                want = as_int(row, field)
                want_text = "integer"
            elif field in BENCHMARK_PROMETHEUS_NON_NEGATIVE_INTEGER_DIRECT_FIELDS:
                want = as_non_negative_int(row, field)
                want_text = "non-negative integer"
            else:
                want = as_number(row, field)
                want_text = "numeric"
            if want is None:
                issues.append(
                    f"{line_label(row)} benchmark prometheus evidence field {field!r}="
                    f"{row.get(field)!r}, want {want_text} for {metric}"
                )
                continue
            got = benchmark_prometheus_metric_value(text, metric, row)
            if got is None:
                issues.append(f"{line_label(row)} benchmark prometheus artifact {path} missing {metric}")
            elif integer_field and (
                not got.is_integer()
                or (field in BENCHMARK_PROMETHEUS_NON_NEGATIVE_INTEGER_DIRECT_FIELDS and got < 0)
            ):
                issues.append(
                    f"{line_label(row)} benchmark prometheus artifact {path} "
                    f"{metric}={got:g}, want {want_text}"
                )
            elif integer_field and int(got) != want:
                issues.append(
                    f"{line_label(row)} benchmark prometheus artifact {path} "
                    f"{metric}={int(got):g}, want {want:g}"
                )
            elif got != want:
                issues.append(
                    f"{line_label(row)} benchmark prometheus artifact {path} "
                    f"{metric}={got:g}, want {want:g}"
                )
        issues.extend(
            check_benchmark_prometheus_archive_api_methods(line_label(row), path, text, row)
        )
    return issues


def check_required_modes(rows, modes):
    issues = []
    present = {str(row.get("mode", "")) for row in rows}
    for mode in modes:
        if mode not in present:
            issues.append(f"required mode {mode!r} has no selected benchmark row")
    return issues


def check_minimal_tail_prune(rows, role):
    row = latest_for(rows, mode="minimal", role=role)
    if row is None:
        scope = "minimal" if role is None else f"minimal/{role}"
        return [f"required minimal tail-prune evidence has no selected {scope} row"]

    issues = []
    issues.extend(
        check_integer_fields(row, ("chainLookupPruneToBlock", "tailPrunedThroughBlock"))
    )
    if as_number(row, "signedColdPrune") != 1.0:
        issues.append(f"{line_label(row)} signedColdPrune must be true")
    chain_lookup = as_number(row, "chainLookupPruneToBlock")
    if chain_lookup is None or chain_lookup < 0:
        issues.append(f"{line_label(row)} chainLookupPruneToBlock must be >= 0")
    tail_pruned = as_number(row, "tailPrunedThroughBlock")
    if tail_pruned is None or tail_pruned < 0:
        issues.append(f"{line_label(row)} tailPrunedThroughBlock must be >= 0")
    if chain_lookup is not None and tail_pruned is not None and tail_pruned > chain_lookup:
        issues.append(
            f"{line_label(row)} tailPrunedThroughBlock={tail_pruned:g} exceeds "
            f"chainLookupPruneToBlock={chain_lookup:g}"
        )
    return issues


def check_minimal_physical_tail_prune(rows, role):
    row = latest_for(rows, mode="minimal", role=role)
    if row is None:
        scope = "minimal" if role is None else f"minimal/{role}"
        return [f"required minimal physical tail-prune evidence has no selected {scope} row"]

    issues = []
    issues.extend(check_non_negative_integer_fields(row, ("tailPrunedFiles",)))
    tail_pruned_files = as_number(row, "tailPrunedFiles")
    if tail_pruned_files is None or tail_pruned_files <= 0:
        issues.append(f"{line_label(row)} tailPrunedFiles={tail_pruned_files}, want > 0")
    return issues


def field_present(row, field):
    return field in row and row.get(field) not in {None, ""}


def check_non_negative_forbidden(row, field, reason):
    if not field_present(row, field):
        return []
    value = as_number(row, field)
    if value is not None and value >= 0:
        return [f"{line_label(row)} {field}={value:g} is not allowed for {reason}"]
    return []


def check_positive_forbidden(row, field, reason):
    if not field_present(row, field):
        return []
    value = as_number(row, field)
    if value is not None and value > 0:
        return [f"{line_label(row)} {field}={value:g} is not allowed for {reason}"]
    return []


PRUNE_BOUNDARY_INTEGER_FIELDS = (
    "coldFreezerToBlock",
    "derivedIndexToBlock",
    "chainLookupPruneToBlock",
    "tailPrunedThroughBlock",
    "balanceTracePruneToBlock",
    "sectionBloomPruneToSection",
)

PRUNE_COUNT_INTEGER_FIELDS = ("tailPrunedFiles",)


def check_integer_fields(row, fields):
    issues = []
    for field in fields:
        if field_present(row, field) and as_int(row, field) is None:
            issues.append(f"{line_label(row)} {field}={row.get(field)!r}, want integer")
    return issues


def check_non_negative_integer_fields(row, fields):
    issues = []
    for field in fields:
        if field_present(row, field) and as_non_negative_int(row, field) is None:
            issues.append(
                f"{line_label(row)} {field}={row.get(field)!r}, want non-negative integer"
            )
    return issues


def check_prune_mode_semantics(rows):
    issues = []
    for row in latest_rows(rows).values():
        issues.extend(check_integer_fields(row, PRUNE_BOUNDARY_INTEGER_FIELDS))
        issues.extend(check_non_negative_integer_fields(row, PRUNE_COUNT_INTEGER_FIELDS))

        mode = str(row.get("mode", "")).lower()
        if not mode:
            issues.append(f"{line_label(row)} mode is missing")
            continue

        persisted_mode = str(row.get("pruneMode", "")).lower()
        if not persisted_mode or persisted_mode == "unknown":
            issues.append(f"{line_label(row)} pruneMode is missing or unknown")
        elif persisted_mode != mode:
            issues.append(
                f"{line_label(row)} pruneMode={row.get('pruneMode')!r} does not match mode={mode!r}"
            )

        if not as_bool(row, "pruneModePersisted"):
            issues.append(f"{line_label(row)} pruneModePersisted must be true")

        if mode == "archive":
            if as_number(row, "signedColdPrune") == 1.0:
                issues.append(f"{line_label(row)} signedColdPrune must be false for archive")
            for field in (
                "chainLookupPruneToBlock",
                "tailPrunedThroughBlock",
                "balanceTracePruneToBlock",
                "sectionBloomPruneToSection",
            ):
                issues.extend(check_non_negative_forbidden(row, field, "archive mode"))

        if mode not in {"archive", "minimal"}:
            issues.extend(
                check_non_negative_forbidden(row, "tailPrunedThroughBlock", f"{mode} mode")
            )
        if mode != "minimal":
            issues.extend(check_positive_forbidden(row, "tailPrunedFiles", f"{mode} mode"))

        signed_cold_prune = as_number(row, "signedColdPrune") == 1.0
        chain_lookup = as_number(row, "chainLookupPruneToBlock")
        cold_freezer = as_number(row, "coldFreezerToBlock")
        tail_pruned_files = as_number(row, "tailPrunedFiles")
        if mode != "archive" and signed_cold_prune:
            if chain_lookup is None or chain_lookup < 0:
                issues.append(
                    f"{line_label(row)} chainLookupPruneToBlock must be >= 0 "
                    f"when signedColdPrune is true for {mode} mode"
                )
            elif cold_freezer is None or cold_freezer < chain_lookup:
                issues.append(
                    f"{line_label(row)} coldFreezerToBlock={cold_freezer} must cover "
                    f"chainLookupPruneToBlock={chain_lookup:g}"
                )

        tail_pruned = as_number(row, "tailPrunedThroughBlock")
        if mode == "minimal" and tail_pruned_files is not None and tail_pruned_files > 0:
            if tail_pruned is None or tail_pruned < 0:
                issues.append(
                    f"{line_label(row)} tailPrunedThroughBlock must be >= 0 "
                    "when tailPrunedFiles is positive for minimal mode"
                )
        if mode == "minimal" and tail_pruned is not None and tail_pruned >= 0:
            if chain_lookup is None or chain_lookup < 0:
                issues.append(
                    f"{line_label(row)} chainLookupPruneToBlock must be >= 0 "
                    "when tailPrunedThroughBlock is set for minimal mode"
                )
            elif tail_pruned > chain_lookup:
                issues.append(
                    f"{line_label(row)} tailPrunedThroughBlock={tail_pruned:g} exceeds "
                    f"chainLookupPruneToBlock={chain_lookup:g}"
                )
            if cold_freezer is None or cold_freezer < tail_pruned:
                issues.append(
                    f"{line_label(row)} coldFreezerToBlock={cold_freezer} must cover "
                    f"tailPrunedThroughBlock={tail_pruned:g}"
                )
            derived_index = as_number(row, "derivedIndexToBlock")
            if tail_pruned > 0 and (derived_index is None or derived_index < tail_pruned):
                issues.append(
                    f"{line_label(row)} derivedIndexToBlock={derived_index} must cover "
                    f"tailPrunedThroughBlock={tail_pruned:g}"
                )

    return issues


def string_set_field(row, field):
    raw = row.get(field)
    if raw is None:
        return None
    if not isinstance(raw, list):
        return set()
    return {str(value) for value in raw}


def archive_api_methods(row):
    return string_set_field(row, "archiveApiMethods")


def archive_api_method_count(row):
    raw = row.get("archiveApiMethods")
    if not isinstance(raw, list):
        return None
    return len(raw)


ARCHIVE_API_EVIDENCE_FIELDS = (
    "archiveApiStatus",
    "archiveApiChecks",
    "archiveApiMethods",
    "archiveApiBlock",
)


def has_archive_api_evidence(row):
    return any(field in row for field in ARCHIVE_API_EVIDENCE_FIELDS)


def check_archive_api_evidence(rows, required_methods, required_modes=(), min_depth_blocks=None, require_trace_block=False):
    issues = []
    latest = list(latest_rows(rows).values())
    evidence_rows = [
        row
        for row in latest
        if has_archive_api_evidence(row)
    ]
    if not evidence_rows and not required_modes:
        return ["required archive API evidence has no selected latest row"]

    for mode in required_modes:
        row = latest_for(rows, mode=mode)
        if row is None:
            issues.append(f"required archive API evidence has no selected latest row for mode {mode!r}")
            continue
        if not has_archive_api_evidence(row):
            issues.append(f"{line_label(row)} missing archive API evidence for required mode {mode!r}")

    for row in evidence_rows:
        status = str(row.get("archiveApiStatus", "")).lower()
        if status != "ok":
            issues.append(f"{line_label(row)} archiveApiStatus={row.get('archiveApiStatus')!r}, want 'ok'")

        checks = as_non_negative_int(row, "archiveApiChecks")
        if checks is None or checks <= 0:
            issues.append(
                f"{line_label(row)} archiveApiChecks={row.get('archiveApiChecks')!r}, "
                "want positive integer"
            )

        failures = as_non_negative_int(row, "archiveApiFailures")
        if failures is None:
            issues.append(
                f"{line_label(row)} archiveApiFailures={row.get('archiveApiFailures')!r}, "
                "want non-negative integer"
            )
        elif failures != 0:
            issues.append(f"{line_label(row)} archiveApiFailures={failures:g}, want 0")

        if require_trace_block and not as_bool(row, "archiveApiTraceBlockProbe"):
            issues.append(
                f"{line_label(row)} archiveApiTraceBlockProbe is not true; "
                "run storage_benchmark.sh with --archive-api-trace-block"
            )

        chain_lookup = None
        if field_present(row, "chainLookupPruneToBlock"):
            chain_lookup = as_int(row, "chainLookupPruneToBlock")
            if chain_lookup is None:
                issues.append(
                    f"{line_label(row)} chainLookupPruneToBlock="
                    f"{row.get('chainLookupPruneToBlock')!r}, want integer"
                )
        tail_pruned = None
        if field_present(row, "tailPrunedThroughBlock"):
            tail_pruned = as_int(row, "tailPrunedThroughBlock")
            if tail_pruned is None:
                issues.append(
                    f"{line_label(row)} tailPrunedThroughBlock="
                    f"{row.get('tailPrunedThroughBlock')!r}, want integer"
                )

        block = as_non_negative_int(row, "archiveApiBlock")
        if block is None:
            issues.append(
                f"{line_label(row)} archiveApiBlock={row.get('archiveApiBlock')!r}, "
                "want non-negative integer historical block"
            )
        else:
            height = None
            if field_present(row, "height"):
                height = as_non_negative_int(row, "height")
                if height is None:
                    issues.append(
                        f"{line_label(row)} height={row.get('height')!r}, "
                        "want non-negative integer for archive API depth evidence"
                    )
            depth = None
            if height is not None and block >= height:
                issues.append(
                    f"{line_label(row)} archiveApiBlock={block:g} must be below height={height:g}"
                )
            if height is not None:
                depth = height - block
                if field_present(row, "archiveApiDepthBlocks"):
                    reported_depth = as_non_negative_int(row, "archiveApiDepthBlocks")
                    if reported_depth is None:
                        issues.append(
                            f"{line_label(row)} archiveApiDepthBlocks="
                            f"{row.get('archiveApiDepthBlocks')!r}, want non-negative integer"
                        )
                    elif not approx_equal(reported_depth, depth):
                        issues.append(
                            f"{line_label(row)} archiveApiDepthBlocks={reported_depth:g}, "
                            f"want height - archiveApiBlock = {depth:g}"
                        )
            if min_depth_blocks is not None:
                if height is None:
                    issues.append(f"{line_label(row)} archive API depth evidence requires numeric height")
                elif depth is not None and depth < min_depth_blocks:
                    issues.append(
                        f"{line_label(row)} archiveApiBlock depth={depth:g} failed >= "
                        f"min archive API depth {min_depth_blocks:g} blocks"
                    )
            if chain_lookup is not None and chain_lookup >= 0 and block > chain_lookup:
                issues.append(
                    f"{line_label(row)} archiveApiBlock={block:g} must be <= "
                    f"chainLookupPruneToBlock={chain_lookup:g} "
                    "to prove post-chain-lookup-prune archive reads"
                )
            if tail_pruned is not None and tail_pruned >= 0 and block > tail_pruned:
                issues.append(
                    f"{line_label(row)} archiveApiBlock={block:g} must be <= "
                    f"tailPrunedThroughBlock={tail_pruned:g} to prove post-prune archive reads"
                )

        methods = archive_api_methods(row)
        if methods is None:
            issues.append(f"{line_label(row)} archiveApiMethods is missing")
        elif not methods:
            issues.append(f"{line_label(row)} archiveApiMethods must be a non-empty list")
        else:
            method_count = archive_api_method_count(row)
            if (
                failures == 0
                and checks is not None
                and checks > 0
                and method_count is not None
                and checks != method_count
            ):
                issues.append(
                    f"{line_label(row)} archiveApiChecks={checks:g} must equal "
                    f"successful archiveApiMethods={method_count} when archiveApiFailures=0"
                )
            missing = sorted(set(required_methods) - methods)
            if missing:
                issues.append(
                    f"{line_label(row)} archiveApiMethods missing required methods: "
                    + ",".join(missing)
                )

    return issues


ARCHIVE_API_TX_EVIDENCE_FIELDS = (
    "archiveApiTxProbe",
    "archiveApiTxHash",
    "archiveApiTxMethods",
)


def has_archive_tx_evidence(row):
    return any(field in row for field in ARCHIVE_API_TX_EVIDENCE_FIELDS)


def archive_api_tx_required_methods(require_trace_transaction=False):
    methods = list(ARCHIVE_API_TX_METHODS)
    if require_trace_transaction and ARCHIVE_API_TRACE_TX_METHOD not in methods:
        methods.append(ARCHIVE_API_TRACE_TX_METHOD)
    return methods


def check_archive_tx_evidence(rows, required_modes=(), require_trace_transaction=False):
    issues = []
    required_methods = archive_api_tx_required_methods(require_trace_transaction)
    latest = list(latest_rows(rows).values())
    evidence_rows = [
        row
        for row in latest
        if has_archive_tx_evidence(row)
    ]
    if not evidence_rows and not required_modes:
        return ["required archive tx evidence has no selected latest row"]

    for mode in required_modes:
        row = latest_for(rows, mode=mode)
        if row is None:
            issues.append(f"required archive tx evidence has no selected latest row for mode {mode!r}")
            continue
        if not has_archive_tx_evidence(row):
            issues.append(f"{line_label(row)} missing archive tx evidence for required mode {mode!r}")
            continue
        if row not in evidence_rows:
            evidence_rows.append(row)

    for row in evidence_rows:
        if not has_archive_api_evidence(row):
            issues.append(f"{line_label(row)} archive API evidence is missing for archive tx evidence")
        if not as_bool(row, "archiveApiTxProbe"):
            issues.append(
                f"{line_label(row)} archiveApiTxProbe is not true; "
                "choose an archive-api-block with at least one transaction"
            )
        if require_trace_transaction and not as_bool(row, "archiveApiTraceTransactionProbe"):
            issues.append(
                f"{line_label(row)} archiveApiTraceTransactionProbe is not true; "
                "run storage_benchmark.sh with --archive-api-trace-transaction"
            )
        tx_hash = row.get("archiveApiTxHash")
        if not isinstance(tx_hash, str) or not tx_hash:
            issues.append(f"{line_label(row)} archiveApiTxHash is missing")
        elif re.fullmatch(r"0x[0-9a-fA-F]{64}", tx_hash) is None:
            issues.append(f"{line_label(row)} archiveApiTxHash must be a 0x-prefixed 32-byte hash")

        methods = archive_api_methods(row)
        if methods is not None:
            missing = sorted(set(required_methods) - methods)
            if missing:
                issues.append(
                    f"{line_label(row)} archiveApiMethods missing required tx methods: "
                    + ",".join(missing)
                )

        tx_methods = string_set_field(row, "archiveApiTxMethods")
        if tx_methods is None:
            issues.append(f"{line_label(row)} archiveApiTxMethods is missing")
        elif not tx_methods:
            issues.append(f"{line_label(row)} archiveApiTxMethods must be a non-empty list")
        else:
            missing = sorted(set(required_methods) - tx_methods)
            if missing:
                issues.append(
                    f"{line_label(row)} archiveApiTxMethods missing required methods: "
                    + ",".join(missing)
                )

    return issues


def as_non_negative_int(row, field):
    parsed = as_int(row, field)
    if parsed is None or parsed < 0:
        return None
    return parsed


RETIRED_PRUNE_RAN_FIELD = "retiredPruneRan"

RETIRED_PRUNE_FIELDS = (
    "retiredPruneSegments",
    "retiredPruneDeleted",
    "retiredPruneMissing",
    "retiredPruneSkippedActive",
    "retiredPruneBytesDeleted",
)

SNAPSHOT_RETIRED_ZERO_FIELDS = (
    "snapshotRetiredSegments",
    "snapshotRetiredFiles",
    "snapshotRetiredMissing",
    "snapshotRetiredSkippedActive",
    "snapshotRetiredBytes",
)


def retired_prune_evidence_row(row):
    return RETIRED_PRUNE_RAN_FIELD in row or any(
        field in row for field in RETIRED_PRUNE_FIELDS + SNAPSHOT_RETIRED_ZERO_FIELDS
    )


def check_retired_prune_row(row):
    issues = []
    if not as_bool(row, RETIRED_PRUNE_RAN_FIELD):
        issues.append(f"{line_label(row)} {RETIRED_PRUNE_RAN_FIELD}={row.get(RETIRED_PRUNE_RAN_FIELD)!r}, want true")
    for field in RETIRED_PRUNE_FIELDS:
        value = as_non_negative_int(row, field)
        if value is None:
            issues.append(f"{line_label(row)} {field}={row.get(field)!r}, want non-negative integer")

    for field in ("retiredPruneMissing", "retiredPruneSkippedActive"):
        value = as_non_negative_int(row, field)
        if value is not None and value != 0:
            issues.append(f"{line_label(row)} {field}={value}, want 0")

    for field in SNAPSHOT_RETIRED_ZERO_FIELDS:
        value = as_non_negative_int(row, field)
        if value is None:
            issues.append(f"{line_label(row)} {field}={row.get(field)!r}, want non-negative integer")
        elif value != 0:
            issues.append(f"{line_label(row)} {field}={value}, want 0 after prune-retired")
    return issues


def check_retired_prune_evidence(rows, required_modes=()):
    issues = []
    rows_to_check = []
    if required_modes:
        for mode in required_modes:
            row = latest_for(rows, mode=mode)
            if row is None:
                issues.append(f"required retired-prune evidence has no selected latest row for mode {mode!r}")
                continue
            if not retired_prune_evidence_row(row):
                issues.append(f"{line_label(row)} missing retired-prune evidence for required mode {mode!r}")
                continue
            rows_to_check.append(row)
    else:
        rows_to_check = [
            row for row in latest_rows(rows).values() if retired_prune_evidence_row(row)
        ]
        if not rows_to_check:
            return ["required retired-prune evidence has no selected latest row"]

    for row in rows_to_check:
        issues.extend(check_retired_prune_row(row))
    return issues


def event_log_index_evidence_row(row):
    derived_to = as_number(row, "derivedIndexToBlock")
    segments = as_number(row, "eventLogIndexSegments")
    return (
        (derived_to is not None and derived_to >= 0)
        or (segments is not None and segments > 0)
    )


def rounded_milli(postings, keys):
    if keys == 0:
        return 0
    return (postings * 1000 + keys // 2) // keys


def check_event_log_index_lookup_stats(row, label, prefix):
    issues = []
    fields = {
        "keys": f"eventLogIndex{prefix}Keys",
        "postings": f"eventLogIndex{prefix}Postings",
        "avg": f"eventLogIndex{prefix}AvgPostingsMilli",
        "max": f"eventLogIndex{prefix}MaxPostings",
        "singleton": f"eventLogIndex{prefix}SingletonKeys",
        "multi": f"eventLogIndex{prefix}MultiPostingKeys",
    }
    values = {}
    for name, field in fields.items():
        value = as_non_negative_int(row, field)
        if value is None:
            issues.append(
                f"{line_label(row)} {field}={row.get(field)!r}, want non-negative integer"
            )
        values[name] = value
    if issues:
        return issues

    keys = values["keys"]
    postings = values["postings"]
    avg = values["avg"]
    max_postings = values["max"]
    singleton = values["singleton"]
    multi = values["multi"]
    if singleton + multi != keys:
        issues.append(
            f"{line_label(row)} {label} singleton+multi={singleton + multi} "
            f"must equal keys={keys}"
        )
    if keys == 0:
        if postings != 0:
            issues.append(f"{line_label(row)} {label} postings={postings} must be 0 when keys=0")
        if avg != 0:
            issues.append(f"{line_label(row)} {label} avgPostingsMilli={avg} must be 0 when keys=0")
        if max_postings != 0:
            issues.append(
                f"{line_label(row)} {label} maxPostings={max_postings} must be 0 when keys=0"
            )
        return issues
    if postings < keys:
        issues.append(f"{line_label(row)} {label} postings={postings} must be >= keys={keys}")
    if max_postings == 0:
        issues.append(f"{line_label(row)} {label} maxPostings must be > 0 when keys={keys}")
    if max_postings > postings:
        issues.append(
            f"{line_label(row)} {label} maxPostings={max_postings} must be <= postings={postings}"
        )
    want_avg = rounded_milli(postings, keys)
    if avg != want_avg:
        issues.append(
            f"{line_label(row)} {label} avgPostingsMilli={avg}, want {want_avg} "
            f"for postings={postings} keys={keys}"
        )
    return issues


def check_event_log_index_evidence(rows, required_modes=(), require_non_empty=False):
    issues = []
    evidence_rows = [
        row for row in latest_rows(rows).values() if event_log_index_evidence_row(row)
    ]
    if not evidence_rows and not required_modes:
        return ["required event-log index evidence has no selected latest derived-index row"]

    for mode in required_modes:
        row = latest_for(rows, mode=mode)
        if row is None:
            issues.append(f"required event-log index evidence has no selected latest row for mode {mode!r}")
            continue
        if not event_log_index_evidence_row(row):
            issues.append(f"{line_label(row)} missing event-log index evidence for required mode {mode!r}")

    for row in evidence_rows:
        derived_to = as_non_negative_int(row, "derivedIndexToBlock")
        if derived_to is None:
            issues.append(
                f"{line_label(row)} derivedIndexToBlock={row.get('derivedIndexToBlock')!r}, "
                "want non-negative integer"
            )
        segments = as_non_negative_int(row, "eventLogIndexSegments")
        if segments is None:
            issues.append(
                f"{line_label(row)} eventLogIndexSegments={row.get('eventLogIndexSegments')!r}, "
                "want positive integer"
            )
        elif segments <= 0:
            issues.append(
                f"{line_label(row)} eventLogIndexSegments={row.get('eventLogIndexSegments')!r}, want > 0"
            )
        from_block = as_non_negative_int(row, "eventLogIndexFromBlock")
        to_block = as_non_negative_int(row, "eventLogIndexToBlock")
        if from_block is None:
            issues.append(
                f"{line_label(row)} eventLogIndexFromBlock={row.get('eventLogIndexFromBlock')!r}, "
                "want non-negative integer"
            )
        if to_block is None:
            issues.append(
                f"{line_label(row)} eventLogIndexToBlock={row.get('eventLogIndexToBlock')!r}, "
                "want non-negative integer"
            )
        if from_block is not None and to_block is not None:
            if from_block > to_block:
                issues.append(
                    f"{line_label(row)} event-log index range [{from_block},{to_block}] is inverted"
                )
            elif derived_to is not None and derived_to != to_block:
                issues.append(
                    f"{line_label(row)} eventLogIndexToBlock={to_block} must match "
                    f"derivedIndexToBlock={derived_to}"
                )
            tail_pruned = as_number(row, "tailPrunedThroughBlock")
            if tail_pruned is not None and tail_pruned >= 0:
                if from_block > tail_pruned or to_block < tail_pruned:
                    issues.append(
                        f"{line_label(row)} event-log index range [{from_block},{to_block}] "
                        f"must cover tailPrunedThroughBlock={tail_pruned:g}"
                    )
        issues.extend(check_event_log_index_lookup_stats(row, "address", "Address"))
        issues.extend(check_event_log_index_lookup_stats(row, "topic", "Topic"))
        if require_non_empty:
            address_postings = as_non_negative_int(row, "eventLogIndexAddressPostings")
            if address_postings is None or address_postings <= 0:
                issues.append(
                    f"{line_label(row)} eventLogIndexAddressPostings="
                    f"{row.get('eventLogIndexAddressPostings')!r}, want > 0"
                )
    return issues


SNAPSHOT_PROFILE_EVIDENCE_FIELDS = (
    "snapshotManifestProfileStatus",
    "snapshotProfileSegments",
    "snapshotProfileVerifyFiles",
    "snapshotProfileVerifiedSegments",
    "snapshotProfileTotalBytes",
    "snapshotPayloadBytes",
    "snapshotSidecarBytes",
    "snapshotSidecarShareMilli",
)

SNAPSHOT_PROFILE_FAMILY_FIELDS = (
    "snapshotLatestSidecar",
    "snapshotStateHistorySidecar",
    "snapshotChainFreezerSidecar",
    "snapshotEventLogSidecar",
    "snapshotBalanceTraceSidecar",
    "snapshotSectionBloomSidecar",
)

SNAPSHOT_PROFILE_POINT_FIELDS = (
    "snapshotPointTxHashLookup",
    "snapshotPointEventLogIndex",
    "snapshotPointStateHistoryAccessor",
    "snapshotPointLatestBTree",
    "snapshotPointChainFreezerAccessor",
    "snapshotPointCodeDomain",
    "snapshotPointCommitmentSnapshot",
)


def snapshot_profile_evidence_row(row):
    return (
        any(field in row for field in SNAPSHOT_PROFILE_EVIDENCE_FIELDS)
        or any(
            f"{prefix}Bytes" in row or f"{prefix}SnapshotShareMilli" in row
            for prefix in SNAPSHOT_PROFILE_POINT_FIELDS
        )
    )


def sidecar_share_milli(sidecar_bytes, total_bytes):
    if sidecar_bytes <= 0 or total_bytes <= 0:
        return 0
    return (sidecar_bytes * 1000 + total_bytes - 1) // total_bytes


def check_snapshot_profile_row(row):
    issues = []
    status = str(row.get("snapshotManifestProfileStatus", "")).lower()
    if status != "ok":
        issues.append(
            f"{line_label(row)} snapshotManifestProfileStatus={row.get('snapshotManifestProfileStatus')!r}, want 'ok'"
        )
    segments = as_non_negative_int(row, "snapshotProfileSegments")
    if segments is None or segments <= 0:
        issues.append(
            f"{line_label(row)} snapshotProfileSegments={row.get('snapshotProfileSegments')!r}, want > 0"
        )
    if not as_bool(row, "snapshotProfileVerifyFiles"):
        issues.append(f"{line_label(row)} snapshotProfileVerifyFiles must be true")
    verified_segments = as_non_negative_int(row, "snapshotProfileVerifiedSegments")
    if verified_segments is None:
        issues.append(
            f"{line_label(row)} snapshotProfileVerifiedSegments={row.get('snapshotProfileVerifiedSegments')!r}, want non-negative integer"
        )
    elif segments is not None and verified_segments != segments:
        issues.append(
            f"{line_label(row)} snapshotProfileVerifiedSegments={verified_segments}, "
            f"want snapshotProfileSegments={segments}"
        )
    total_bytes = as_non_negative_int(row, "snapshotProfileTotalBytes")
    payload_bytes = as_non_negative_int(row, "snapshotPayloadBytes")
    sidecar_bytes = as_non_negative_int(row, "snapshotSidecarBytes")
    share = as_non_negative_int(row, "snapshotSidecarShareMilli")
    if total_bytes is None or total_bytes <= 0:
        issues.append(
            f"{line_label(row)} snapshotProfileTotalBytes={row.get('snapshotProfileTotalBytes')!r}, want > 0"
        )
    if payload_bytes is None:
        issues.append(f"{line_label(row)} snapshotPayloadBytes={row.get('snapshotPayloadBytes')!r}, want non-negative integer")
    if sidecar_bytes is None:
        issues.append(f"{line_label(row)} snapshotSidecarBytes={row.get('snapshotSidecarBytes')!r}, want non-negative integer")
    if share is None:
        issues.append(f"{line_label(row)} snapshotSidecarShareMilli={row.get('snapshotSidecarShareMilli')!r}, want non-negative integer")
    if (
        total_bytes is not None
        and payload_bytes is not None
        and sidecar_bytes is not None
        and total_bytes != payload_bytes + sidecar_bytes
    ):
        issues.append(
            f"{line_label(row)} snapshot payload+sidecar={payload_bytes + sidecar_bytes} "
            f"must equal total={total_bytes}"
        )
    if total_bytes is not None and sidecar_bytes is not None and share is not None:
        want_share = sidecar_share_milli(sidecar_bytes, total_bytes)
        if share != want_share:
            issues.append(
                f"{line_label(row)} snapshotSidecarShareMilli={share}, want {want_share} "
                f"for sidecarBytes={sidecar_bytes} totalBytes={total_bytes}"
            )
        if share > 1000:
            issues.append(f"{line_label(row)} snapshotSidecarShareMilli={share}, want <= 1000")

    for prefix in SNAPSHOT_PROFILE_FAMILY_FIELDS:
        bytes_field = f"{prefix}Bytes"
        share_field = f"{prefix}ShareMilli"
        family_bytes = as_non_negative_int(row, bytes_field)
        family_share = as_number(row, share_field)
        if family_bytes is None:
            issues.append(f"{line_label(row)} {bytes_field}={row.get(bytes_field)!r}, want non-negative integer")
        if family_share is None:
            issues.append(f"{line_label(row)} {share_field}={row.get(share_field)!r}, want numeric value")
        elif family_share < -1 or family_share > 1000:
            issues.append(f"{line_label(row)} {share_field}={family_share:g}, want -1..1000")
        elif family_bytes is not None and family_bytes > 0 and family_share < 0:
            issues.append(
                f"{line_label(row)} {share_field}={family_share:g}, want >= 0 "
                f"when {bytes_field}={family_bytes}"
            )
    for prefix in SNAPSHOT_PROFILE_POINT_FIELDS:
        segments_field = f"{prefix}Segments"
        bytes_field = f"{prefix}Bytes"
        payload_field = f"{prefix}PayloadBytes"
        sidecar_field = f"{prefix}SidecarBytes"
        sidecar_share_field = f"{prefix}SidecarShareMilli"
        share_field = f"{prefix}SnapshotShareMilli"
        point_segments = as_non_negative_int(row, segments_field)
        point_bytes = as_non_negative_int(row, bytes_field)
        point_payload = as_non_negative_int(row, payload_field)
        point_sidecar = as_non_negative_int(row, sidecar_field)
        point_sidecar_share = as_number(row, sidecar_share_field)
        point_share = as_number(row, share_field)
        if point_segments is None:
            issues.append(f"{line_label(row)} {segments_field}={row.get(segments_field)!r}, want non-negative integer")
        if point_bytes is None:
            issues.append(f"{line_label(row)} {bytes_field}={row.get(bytes_field)!r}, want non-negative integer")
        if point_payload is None:
            issues.append(f"{line_label(row)} {payload_field}={row.get(payload_field)!r}, want non-negative integer")
        if point_sidecar is None:
            issues.append(f"{line_label(row)} {sidecar_field}={row.get(sidecar_field)!r}, want non-negative integer")
        if (
            point_bytes is not None
            and point_payload is not None
            and point_sidecar is not None
            and point_bytes != point_payload + point_sidecar
        ):
            issues.append(
                f"{line_label(row)} {prefix} payload+sidecar={point_payload + point_sidecar} "
                f"must equal bytes={point_bytes}"
            )
        if point_sidecar_share is None:
            issues.append(f"{line_label(row)} {sidecar_share_field}={row.get(sidecar_share_field)!r}, want numeric value")
        elif point_sidecar_share < -1 or point_sidecar_share > 1000:
            issues.append(f"{line_label(row)} {sidecar_share_field}={point_sidecar_share:g}, want -1..1000")
        elif point_sidecar is not None and point_sidecar > 0 and point_sidecar_share < 0:
            issues.append(
                f"{line_label(row)} {sidecar_share_field}={point_sidecar_share:g}, want >= 0 "
                f"when {sidecar_field}={point_sidecar}"
            )
        elif point_bytes is not None and point_sidecar is not None and point_sidecar_share >= 0:
            want_sidecar_share = sidecar_share_milli(point_sidecar, point_bytes)
            if point_sidecar_share != want_sidecar_share:
                issues.append(
                    f"{line_label(row)} {sidecar_share_field}={point_sidecar_share:g}, "
                    f"want {want_sidecar_share} for {sidecar_field}={point_sidecar} "
                    f"{bytes_field}={point_bytes}"
                )
        if point_share is None:
            issues.append(f"{line_label(row)} {share_field}={row.get(share_field)!r}, want numeric value")
        elif point_share < -1 or point_share > 1000:
            issues.append(f"{line_label(row)} {share_field}={point_share:g}, want -1..1000")
        elif point_bytes is not None and point_bytes > 0 and point_share < 0:
            issues.append(
                f"{line_label(row)} {share_field}={point_share:g}, want >= 0 "
                f"when {bytes_field}={point_bytes}"
            )
        elif total_bytes is not None and point_bytes is not None and point_share >= 0:
            want_share = sidecar_share_milli(point_bytes, total_bytes)
            if point_share != want_share:
                issues.append(
                    f"{line_label(row)} {share_field}={point_share:g}, want {want_share} "
                    f"for {bytes_field}={point_bytes} totalBytes={total_bytes}"
                )
    return issues


def check_snapshot_profile_evidence(rows, required_modes=()):
    issues = []
    evidence_rows = [
        row for row in latest_rows(rows).values() if snapshot_profile_evidence_row(row)
    ]
    if not evidence_rows and not required_modes:
        return ["required snapshot manifest profile evidence has no selected latest row"]

    for mode in required_modes:
        row = latest_for(rows, mode=mode)
        if row is None:
            issues.append(f"required snapshot manifest profile evidence has no selected latest row for mode {mode!r}")
            continue
        if not snapshot_profile_evidence_row(row):
            issues.append(
                f"{line_label(row)} missing snapshot manifest profile evidence for required mode {mode!r}"
            )
            continue
        if row not in evidence_rows:
            evidence_rows.append(row)

    for row in evidence_rows:
        issues.extend(check_snapshot_profile_row(row))
    return issues


def check_snapshot_point_thresholds(rows, max_sidecar_share_milli, max_snapshot_share_milli):
    if max_sidecar_share_milli is None and max_snapshot_share_milli is None:
        return []
    issues = []
    for row in latest_rows(rows).values():
        if not snapshot_profile_evidence_row(row):
            issues.append(f"{line_label(row)} snapshot point threshold requires snapshot manifest profile evidence")
            continue
        for prefix in SNAPSHOT_PROFILE_POINT_FIELDS:
            segments_field = f"{prefix}Segments"
            segments = as_non_negative_int(row, segments_field)
            if segments is None:
                issues.append(
                    f"{line_label(row)} {segments_field}={row.get(segments_field)!r}, "
                    "want non-negative integer"
                )
                continue
            if segments <= 0:
                continue
            if max_sidecar_share_milli is not None:
                field = f"{prefix}SidecarShareMilli"
                share = as_number(row, field)
                if share is None:
                    issues.append(f"{line_label(row)} {field}={row.get(field)!r}, want numeric value")
                elif share > max_sidecar_share_milli:
                    issues.append(f"{line_label(row)} {field}={share:g} exceeds max {max_sidecar_share_milli:g}")
            if max_snapshot_share_milli is not None:
                field = f"{prefix}SnapshotShareMilli"
                share = as_number(row, field)
                if share is None:
                    issues.append(f"{line_label(row)} {field}={row.get(field)!r}, want numeric value")
                elif share > max_snapshot_share_milli:
                    issues.append(f"{line_label(row)} {field}={share:g} exceeds max {max_snapshot_share_milli:g}")
    return issues


def build_parser():
    parser = argparse.ArgumentParser(
        description="Validate storage_benchmark.sh JSONL output against soak acceptance gates.",
    )
    parser.add_argument("result", type=Path, help="storage_benchmark.sh JSONL output")
    parser.add_argument(
        "--require-mode",
        action="append",
        default=[],
        help="mode that must appear in selected rows; repeatable",
    )
    parser.add_argument(
        "--require-modes",
        action="append",
        default=[],
        help="comma-separated modes that must appear in selected rows",
    )
    parser.add_argument("--role", help="only validate rows with this role")
    parser.add_argument(
        "--allow-warning",
        action="store_true",
        help="allow warning component statuses; critical statuses still fail",
    )
    parser.add_argument(
        "--require-prometheus-artifacts",
        action="store_true",
        help="require each latest selected row to reference a readable storage alert metrics file",
    )
    parser.add_argument(
        "--require-benchmark-prometheus-artifacts",
        action="store_true",
        help="require each latest selected row to reference a storage benchmark metrics file whose gauges match JSONL bytes",
    )
    parser.add_argument(
        "--require-minimal-tail-prune",
        action="store_true",
        help="require latest minimal row to prove signed cold lookup prune plus tail prune",
    )
    parser.add_argument(
        "--require-minimal-physical-tail-prune",
        action="store_true",
        help="require latest minimal row to report physical freezer tail-file deletion",
    )
    parser.add_argument(
        "--require-prune-mode-semantics",
        action="store_true",
        help="require latest rows to preserve archive/blocks/minimal prune-mode semantics",
    )
    parser.add_argument(
        "--require-archive-api-evidence",
        action="store_true",
        help="require latest rows to include successful historical archive API evidence",
    )
    parser.add_argument(
        "--require-archive-api-mode",
        action="append",
        default=[],
        help="mode whose latest selected row must include successful archive API evidence; repeatable",
    )
    parser.add_argument(
        "--require-archive-api-modes",
        action="append",
        default=[],
        help="comma-separated modes whose latest selected rows must include successful archive API evidence",
    )
    parser.add_argument(
        "--require-archive-tx-evidence",
        action="store_true",
        help=(
            "require latest rows to prove historical archive tx and receipt lookups "
            "for a block with at least one transaction"
        ),
    )
    parser.add_argument(
        "--require-archive-trace-transaction",
        action="store_true",
        help=(
            "require archive tx evidence to include a successful "
            "debug_traceTransaction probe for the selected transaction"
        ),
    )
    parser.add_argument(
        "--require-archive-trace-block",
        action="store_true",
        help=(
            "require archive API evidence to include successful "
            "debug_traceBlockByNumber and debug_traceBlockByHash probes"
        ),
    )
    parser.add_argument(
        "--require-archive-tx-mode",
        action="append",
        default=[],
        help="mode whose latest selected row must include archive tx and receipt evidence; repeatable",
    )
    parser.add_argument(
        "--require-archive-tx-modes",
        action="append",
        default=[],
        help="comma-separated modes whose latest selected rows must include archive tx and receipt evidence",
    )
    parser.add_argument(
        "--require-event-log-index-evidence",
        action="store_true",
        help="require latest derived-index rows to include event-log-index fanout/selectivity counters",
    )
    parser.add_argument(
        "--require-event-log-index-mode",
        action="append",
        default=[],
        help="mode whose latest selected row must include event-log-index fanout/selectivity counters; repeatable",
    )
    parser.add_argument(
        "--require-event-log-index-modes",
        action="append",
        default=[],
        help="comma-separated modes whose latest selected rows must include event-log-index fanout/selectivity counters",
    )
    parser.add_argument(
        "--require-event-log-index-non-empty",
        action="store_true",
        help=(
            "when event-log-index evidence is required, require address postings "
            "to be non-empty for samples expected to include logs"
        ),
    )
    parser.add_argument(
        "--require-retired-prune-evidence",
        action="store_true",
        help="require latest rows to include clean retired snapshot prune evidence",
    )
    parser.add_argument(
        "--require-retired-prune-mode",
        action="append",
        default=[],
        help="mode whose latest selected row must include clean retired snapshot prune evidence; repeatable",
    )
    parser.add_argument(
        "--require-retired-prune-modes",
        action="append",
        default=[],
        help="comma-separated modes whose latest selected rows must include clean retired snapshot prune evidence",
    )
    parser.add_argument(
        "--require-snapshot-profile-evidence",
        action="store_true",
        help="require latest rows to include valid snapshot manifest payload/sidecar profile counters",
    )
    parser.add_argument(
        "--require-snapshot-profile-mode",
        action="append",
        default=[],
        help="mode whose latest selected row must include valid snapshot manifest profile counters; repeatable",
    )
    parser.add_argument(
        "--require-snapshot-profile-modes",
        action="append",
        default=[],
        help="comma-separated modes whose latest selected rows must include valid snapshot manifest profile counters",
    )
    parser.add_argument(
        "--max-snapshot-point-sidecar-share-milli",
        type=float,
        metavar="N",
        help="fail if any present snapshot point candidate sidecar share exceeds N/1000 of candidate bytes",
    )
    parser.add_argument(
        "--max-snapshot-point-snapshot-share-milli",
        type=float,
        metavar="N",
        help="fail if any present snapshot point candidate bytes exceed N/1000 of total snapshot bytes",
    )
    parser.add_argument(
        "--archive-api-method",
        action="append",
        default=[],
        help="archive API method that must appear in archiveApiMethods; repeatable",
    )
    parser.add_argument(
        "--archive-api-methods",
        action="append",
        default=[],
        help="comma-separated archive API methods that must appear in archiveApiMethods",
    )
    parser.add_argument(
        "--min-archive-api-depth-blocks",
        type=float,
        metavar="BLOCKS",
        help=(
            "when archive API evidence is required, require archiveApiBlock "
            "to be at least this many blocks below height"
        ),
    )
    parser.add_argument(
        "--min",
        dest="minimums",
        action="append",
        default=[],
        metavar="FIELD=VALUE",
        help="numeric lower bound for FIELD, MODE.FIELD, or MODE.ROLE.FIELD",
    )
    parser.add_argument(
        "--max",
        dest="maximums",
        action="append",
        default=[],
        metavar="FIELD=VALUE",
        help="numeric upper bound for FIELD, MODE.FIELD, or MODE.ROLE.FIELD",
    )
    parser.add_argument(
        "--max-datadir-bytes-per-block",
        type=float,
        metavar="BYTES",
        help="require each latest selected row datadirBytes/height to be no greater than BYTES",
    )
    parser.add_argument(
        "--max-hot-bytes-per-block",
        type=float,
        metavar="BYTES",
        help="require each latest selected row chaindataBytes/height to be no greater than BYTES",
    )
    parser.add_argument(
        "--max-cold-archive-bytes-per-block",
        type=float,
        metavar="BYTES",
        help=(
            "require each latest selected row (ancientBytes+snapshotBytes)/height "
            "to be no greater than BYTES"
        ),
    )
    parser.add_argument(
        "--max-derived-index-bytes-per-block",
        type=float,
        metavar="BYTES",
        help="require each latest selected row derivedIndexBytes/height to be no greater than BYTES",
    )
    parser.add_argument(
        "--require-size-reduction",
        dest="size_reductions",
        action="append",
        default=[],
        metavar="MODE:BASE_MODE:FIELD=RATIO",
        help="require latest MODE row FIELD to be smaller than BASE_MODE by at least RATIO; repeatable",
    )
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    rows, issues = load_rows(args.result)
    if args.role:
        rows = [row for row in rows if str(row.get("role", "")) == args.role]
        if not rows:
            issues.append(f"no rows matched role {args.role!r}")

    required_modes = split_modes(args.require_mode + args.require_modes)
    issues.extend(check_required_modes(rows, required_modes))
    issues.extend(check_statuses(rows, args.allow_warning))
    if args.require_prometheus_artifacts:
        issues.extend(check_prometheus_artifacts(args.result, rows))
    if args.require_benchmark_prometheus_artifacts:
        issues.extend(check_benchmark_prometheus_artifacts(args.result, rows))
    if args.require_minimal_tail_prune or args.require_minimal_physical_tail_prune:
        issues.extend(check_minimal_tail_prune(rows, args.role))
    if args.require_minimal_physical_tail_prune:
        issues.extend(check_minimal_physical_tail_prune(rows, args.role))
    if args.require_prune_mode_semantics:
        issues.extend(check_prune_mode_semantics(rows))
    archive_api_methods_required = list(DEFAULT_ARCHIVE_API_METHODS)
    for method in split_csv_values(args.archive_api_method + args.archive_api_methods):
        if method not in archive_api_methods_required:
            archive_api_methods_required.append(method)
    if args.require_archive_trace_transaction and ARCHIVE_API_TRACE_TX_METHOD not in archive_api_methods_required:
        archive_api_methods_required.append(ARCHIVE_API_TRACE_TX_METHOD)
    if args.require_archive_trace_block:
        for method in ARCHIVE_API_TRACE_BLOCK_METHODS:
            if method not in archive_api_methods_required:
                archive_api_methods_required.append(method)
    required_archive_api_modes = split_modes(
        args.require_archive_api_mode + args.require_archive_api_modes
    )
    required_archive_tx_modes = split_modes(
        args.require_archive_tx_mode + args.require_archive_tx_modes
    )
    archive_api_required_modes = list(required_archive_api_modes)
    for mode in required_archive_tx_modes:
        if mode not in archive_api_required_modes:
            archive_api_required_modes.append(mode)
    if (
        args.require_archive_api_evidence
        or archive_api_required_modes
        or args.require_archive_tx_evidence
        or args.require_archive_trace_transaction
        or args.require_archive_trace_block
        or required_archive_tx_modes
        or args.min_archive_api_depth_blocks is not None
    ):
        issues.extend(
            check_archive_api_evidence(
                rows,
                archive_api_methods_required,
                archive_api_required_modes,
                min_depth_blocks=args.min_archive_api_depth_blocks,
                require_trace_block=args.require_archive_trace_block,
            )
        )
    if args.require_archive_tx_evidence or args.require_archive_trace_transaction or required_archive_tx_modes:
        issues.extend(
            check_archive_tx_evidence(
                rows,
                required_archive_tx_modes,
                require_trace_transaction=args.require_archive_trace_transaction,
            )
        )
    required_event_log_index_modes = split_modes(
        args.require_event_log_index_mode + args.require_event_log_index_modes
    )
    if (
        args.require_event_log_index_evidence
        or required_event_log_index_modes
        or args.require_event_log_index_non_empty
    ):
        issues.extend(
            check_event_log_index_evidence(
                rows,
                required_event_log_index_modes,
                require_non_empty=args.require_event_log_index_non_empty,
            )
        )
    required_retired_prune_modes = split_modes(
        args.require_retired_prune_mode + args.require_retired_prune_modes
    )
    if args.require_retired_prune_evidence or required_retired_prune_modes:
        issues.extend(check_retired_prune_evidence(rows, required_retired_prune_modes))
    required_snapshot_profile_modes = split_modes(
        args.require_snapshot_profile_mode + args.require_snapshot_profile_modes
    )
    if args.require_snapshot_profile_evidence or required_snapshot_profile_modes:
        issues.extend(check_snapshot_profile_evidence(rows, required_snapshot_profile_modes))
    issues.extend(
        check_snapshot_point_thresholds(
            rows,
            args.max_snapshot_point_sidecar_share_milli,
            args.max_snapshot_point_snapshot_share_milli,
        )
    )
    issues.extend(check_thresholds(rows, args.minimums, ">=", lambda got, want: got >= want))
    issues.extend(check_thresholds(rows, args.maximums, "<=", lambda got, want: got <= want))
    issues.extend(
        check_max_bytes_per_block(
            rows,
            args.max_datadir_bytes_per_block,
            "datadir",
            ("datadirBytes",),
            "datadirBytesPerBlock",
        )
    )
    issues.extend(
        check_max_bytes_per_block(
            rows,
            args.max_hot_bytes_per_block,
            "hot",
            ("chaindataBytes",),
            "hotBytesPerBlock",
        )
    )
    issues.extend(
        check_max_bytes_per_block(
            rows,
            args.max_cold_archive_bytes_per_block,
            "cold archive",
            ("ancientBytes", "snapshotBytes"),
            "coldArchiveBytesPerBlock",
        )
    )
    issues.extend(
        check_max_bytes_per_block(
            rows,
            args.max_derived_index_bytes_per_block,
            "derived index",
            ("derivedIndexBytes",),
            "derivedIndexBytesPerBlock",
        )
    )
    issues.extend(check_size_reductions(rows, args.size_reductions, args.role))

    if issues:
        print("storage benchmark acceptance: failed", file=sys.stderr)
        for issue in issues:
            print(f"- {issue}", file=sys.stderr)
        return 1

    latest = list(latest_rows(rows).values())
    modes = ",".join(sorted({str(row.get("mode", "")) for row in latest if row.get("mode")}))
    checks = (
        1
        + len(required_modes)
        + len(args.minimums)
        + len(args.maximums)
        + len(args.size_reductions)
    )
    for value in (
        args.max_datadir_bytes_per_block,
        args.max_hot_bytes_per_block,
        args.max_cold_archive_bytes_per_block,
        args.max_derived_index_bytes_per_block,
    ):
        if value is not None:
            checks += len(latest)
    if args.require_prometheus_artifacts:
        checks += len(latest)
    if args.require_benchmark_prometheus_artifacts:
        checks += len(latest)
    if args.require_minimal_tail_prune or args.require_minimal_physical_tail_prune:
        checks += 1
    if args.require_minimal_physical_tail_prune:
        checks += 1
    if args.require_prune_mode_semantics:
        checks += len(latest)
    if args.require_archive_api_evidence or args.require_archive_tx_evidence:
        checks += 1
    checks += len(archive_api_required_modes)
    if args.require_archive_tx_evidence:
        checks += 1
    checks += len(required_archive_tx_modes)
    if args.require_event_log_index_evidence:
        checks += 1
    checks += len(required_event_log_index_modes)
    if args.require_retired_prune_evidence:
        checks += 1
    checks += len(required_retired_prune_modes)
    if args.require_snapshot_profile_evidence:
        checks += 1
    checks += len(required_snapshot_profile_modes)
    print(
        f"storage benchmark acceptance: ok rows={len(rows)} latest={len(latest)} "
        f"modes={modes or '-'} checks={checks}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

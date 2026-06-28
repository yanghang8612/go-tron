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

PROMETHEUS_STATUS_VALUES = {
    "ok": 0,
    "warning": 1,
    "critical": 2,
}

DEFAULT_ARCHIVE_API_METHODS = (
    "eth_getBlockByNumber",
    "eth_getBalance",
    "eth_getCode",
    "eth_getStorageAt",
    "eth_getLogs",
)


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


def as_bool(row, field):
    value = row.get(field)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "ok"}
    if isinstance(value, (int, float)):
        return value != 0
    return False


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
        pending = as_number(row, "stageAlertPipelinePending")
        if pending is not None:
            issues.extend(
                check_prometheus_metric_value(
                    label, path, text, "gtron_storage_stage_pipeline_pending", pending, row
                )
            )
    if "stageAlertPipelineIssues" in row:
        count = as_number(row, "stageAlertPipelineIssues")
        if count is not None:
            issues.extend(
                check_prometheus_metric_value(
                    label, path, text, "gtron_storage_stage_pipeline_issues", count, row
                )
            )
    next_stage = row.get("stageAlertPipelineNext")
    if next_stage:
        want_target = as_number(row, "stageAlertPipelineNextTarget")
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

        want_current = as_number(row, "stageAlertPipelineNextCurrent")
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


def check_prune_mode_semantics(rows):
    issues = []
    for row in latest_rows(rows).values():
        mode = str(row.get("mode", "")).lower()
        if not mode:
            continue

        persisted_mode = str(row.get("pruneMode", "")).lower()
        if persisted_mode and persisted_mode != "unknown" and persisted_mode != mode:
            issues.append(
                f"{line_label(row)} pruneMode={row.get('pruneMode')!r} does not match mode={mode!r}"
            )

        if field_present(row, "pruneModePersisted") and not as_bool(row, "pruneModePersisted"):
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


def archive_api_methods(row):
    raw = row.get("archiveApiMethods")
    if raw is None:
        return None
    if not isinstance(raw, list):
        return set()
    return {str(method) for method in raw}


def check_archive_api_evidence(rows, required_methods):
    issues = []
    latest = list(latest_rows(rows).values())
    evidence_rows = [
        row
        for row in latest
        if any(
            field in row
            for field in (
                "archiveApiStatus",
                "archiveApiChecks",
                "archiveApiMethods",
                "archiveApiBlock",
            )
        )
    ]
    if not evidence_rows:
        return ["required archive API evidence has no selected latest row"]

    for row in evidence_rows:
        status = str(row.get("archiveApiStatus", "")).lower()
        if status != "ok":
            issues.append(f"{line_label(row)} archiveApiStatus={row.get('archiveApiStatus')!r}, want 'ok'")

        checks = as_number(row, "archiveApiChecks")
        if checks is None or checks <= 0:
            issues.append(f"{line_label(row)} archiveApiChecks={checks}, want > 0")

        failures = as_number(row, "archiveApiFailures")
        if failures is None:
            issues.append(f"{line_label(row)} archiveApiFailures={failures}, want 0")
        elif failures != 0:
            issues.append(f"{line_label(row)} archiveApiFailures={failures:g}, want 0")

        block = as_number(row, "archiveApiBlock")
        if block is None or block < 0:
            issues.append(f"{line_label(row)} archiveApiBlock={block}, want >= 0 historical block")
        else:
            height = as_number(row, "height")
            if height is not None and block >= height:
                issues.append(
                    f"{line_label(row)} archiveApiBlock={block:g} must be below height={height:g}"
                )
            tail_pruned = as_number(row, "tailPrunedThroughBlock")
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
            missing = sorted(set(required_methods) - methods)
            if missing:
                issues.append(
                    f"{line_label(row)} archiveApiMethods missing required methods: "
                    + ",".join(missing)
                )

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
    if args.require_archive_api_evidence:
        issues.extend(check_archive_api_evidence(rows, archive_api_methods_required))
    issues.extend(check_thresholds(rows, args.minimums, ">=", lambda got, want: got >= want))
    issues.extend(check_thresholds(rows, args.maximums, "<=", lambda got, want: got <= want))

    if issues:
        print("storage benchmark acceptance: failed", file=sys.stderr)
        for issue in issues:
            print(f"- {issue}", file=sys.stderr)
        return 1

    latest = list(latest_rows(rows).values())
    modes = ",".join(sorted({str(row.get("mode", "")) for row in latest if row.get("mode")}))
    checks = 1 + len(required_modes) + len(args.minimums) + len(args.maximums)
    if args.require_prometheus_artifacts:
        checks += len(latest)
    if args.require_minimal_tail_prune or args.require_minimal_physical_tail_prune:
        checks += 1
    if args.require_minimal_physical_tail_prune:
        checks += 1
    if args.require_prune_mode_semantics:
        checks += len(latest)
    if args.require_archive_api_evidence:
        checks += 1
    print(
        f"storage benchmark acceptance: ok rows={len(rows)} latest={len(latest)} "
        f"modes={modes or '-'} checks={checks}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

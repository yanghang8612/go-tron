#!/usr/bin/env python3
"""Validate nile_sync_sample.sh JSONL output for staged-sync acceptance."""

import argparse
import json
import re
import sys
from pathlib import Path


ZERO_ISSUE_FIELDS = (
    "heightRegressionBlocks",
    "stageProgressRegressionCount",
    "stageMismatchRows",
    "stageMissingCanonicalRows",
    "stageStagedBodyIssueRows",
    "stageIssueRows",
    "stageOrderIssueRows",
    "stageSyncPipelineViolationCount",
)

PROMETHEUS_REQUIRED_SNIPPETS = (
    ("gtron_storage_alert_status{", "gtron_storage_alert_status"),
    ("# TYPE gtron_storage_alert_issue gauge", "gtron_storage_alert_issue"),
)


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


def row_sort_key(row):
    try:
        unix = int(row.get("unix", 0))
    except (TypeError, ValueError):
        unix = 0
    return unix, row.get("_line", 0)


def row_label(row):
    parts = [f"line {row.get('_line', '?')}"]
    for key in ("network", "mode", "label"):
        value = row.get(key)
        if value:
            parts.append(f"{key}={value}")
    return " ".join(parts)


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


def filter_rows(rows, args):
    out = []
    for row in rows:
        if args.network and str(row.get("network", "")) != args.network:
            continue
        if args.mode and str(row.get("mode", "")) != args.mode:
            continue
        if args.label and str(row.get("label", "")) != args.label:
            continue
        out.append(row)
    return out


def parse_threshold(raw):
    if "=" not in raw:
        raise ValueError(f"{raw!r} must use FIELD=VALUE")
    field, value = raw.split("=", 1)
    field = field.strip()
    if not field:
        raise ValueError(f"{raw!r} has an empty field")
    try:
        want = float(value)
    except ValueError as exc:
        raise ValueError(f"{raw!r} has a non-numeric threshold") from exc
    return field, want


def check_thresholds(row, raws, op_name, predicate):
    issues = []
    for raw in raws:
        try:
            field, want = parse_threshold(raw)
        except ValueError as exc:
            issues.append(str(exc))
            continue
        got = as_number(row, field)
        if got is None:
            issues.append(f"{raw}: {field!r} is missing or non-numeric")
            continue
        if not predicate(got, want):
            issues.append(f"{raw}: {field}={got:g} failed {op_name} {want:g}")
    return issues


def resolve_artifact(result_path, raw_path):
    path = Path(str(raw_path))
    if path.is_absolute():
        return path
    return result_path.parent / path


def check_prometheus_artifact(result_path, row):
    raw_path = row.get("offlineDbCheckPrometheus")
    if not raw_path:
        return ["offlineDbCheckPrometheus is missing while status is ok"]
    path = resolve_artifact(result_path, raw_path)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        return [f"offlineDbCheckPrometheus artifact {path}: {exc}"]
    issues = []
    for needle, name in PROMETHEUS_REQUIRED_SNIPPETS:
        if needle not in text:
            issues.append(f"offlineDbCheckPrometheus artifact {path} missing {name}")
    issues.extend(check_prometheus_issue_kinds(path, text, row))
    return issues


def parse_prometheus_labels(raw):
    labels = {}
    for match in re.finditer(r'([A-Za-z_][A-Za-z0-9_]*)="((?:\\.|[^"\\])*)"', raw):
        value = match.group(2)
        value = value.replace(r"\\", "\\").replace(r"\"", '"').replace(r"\n", "\n")
        labels[match.group(1)] = value
    return labels


def prometheus_issue_keys(text):
    keys = set()
    for line in text.splitlines():
        match = re.match(r"^gtron_storage_alert_issue\{([^}]*)\}\s+", line.strip())
        if not match:
            continue
        labels = parse_prometheus_labels(match.group(1))
        component = labels.get("component")
        kind = labels.get("kind")
        severity = labels.get("severity")
        if component and kind and severity:
            keys.add((component, kind, severity))
    return keys


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


def check_prometheus_issue_kinds(path, text, row):
    issues = []
    actual = prometheus_issue_keys(text)
    for component, kind, severity in sorted(row_alert_issue_keys(row)):
        if (component, kind, severity) not in actual:
            issues.append(
                f"offlineDbCheckPrometheus artifact {path} missing "
                f"gtron_storage_alert_issue component={component!r} kind={kind!r} severity={severity!r}"
            )
    return issues


def check_row(row, args):
    issues = []
    if row.get("sampleStatus") != "ok":
        issues.append(f"sampleStatus={row.get('sampleStatus')!r}, want 'ok'")

    health = str(row.get("soakHealthStatus", "unknown"))
    if args.allow_warning_health:
        if health not in {"ok", "warning"}:
            issues.append(f"soakHealthStatus={health!r}, want ok/warning")
    elif health != "ok":
        issues.append(f"soakHealthStatus={health!r}, want 'ok'")

    if args.require_stage_status and row.get("stageStatusFileStatus") != "ok":
        issues.append(f"stageStatusFileStatus={row.get('stageStatusFileStatus')!r}, want 'ok'")

    status = str(row.get("fullStagedSyncStatus", "unknown"))
    if args.require_caught_up:
        if status != "caught-up" or not as_bool(row, "fullStagedSyncCompleteAtHead"):
            issues.append(
                "full staged sync is not caught up: "
                f"status={status!r} completeAtHead={row.get('fullStagedSyncCompleteAtHead')!r}"
            )
    elif status not in {"catching-up", "caught-up"} or not as_bool(row, "fullStagedSyncReady"):
        issues.append(
            "full staged sync is not ready: "
            f"status={status!r} ready={row.get('fullStagedSyncReady')!r}"
        )

    if row.get("stageSyncPipelineMonotonic") is not None and not as_bool(row, "stageSyncPipelineMonotonic"):
        issues.append("stageSyncPipelineMonotonic=false")

    for field in ZERO_ISSUE_FIELDS:
        value = as_number(row, field)
        if value is not None and value != 0:
            issues.append(f"{field}={value:g}, want 0")

    if args.require_offline_db_check:
        if not as_bool(row, "offlineDbCheck"):
            issues.append("offlineDbCheck is not true")
        if row.get("offlineDbCheckStatus") != "ok":
            issues.append(f"offlineDbCheckStatus={row.get('offlineDbCheckStatus')!r}, want 'ok'")
        if row.get("offlineDbCheckPrometheusStatus") not in {None, "", "ok", "skipped"}:
            issues.append(
                "offlineDbCheckPrometheusStatus="
                f"{row.get('offlineDbCheckPrometheusStatus')!r}, want ok/skipped"
            )
        if row.get("offlineDbCheckPrometheusStatus") == "ok":
            issues.extend(check_prometheus_artifact(args.result, row))

    if args.min_height is not None:
        height = as_number(row, "height")
        if height is None or height < args.min_height:
            issues.append(f"height={height}, want >= {args.min_height}")
    if args.max_lag_blocks is not None:
        lag = as_number(row, "fullStagedSyncHeadLagBlocks")
        if lag is None or lag > args.max_lag_blocks:
            issues.append(f"fullStagedSyncHeadLagBlocks={lag}, want <= {args.max_lag_blocks}")

    issues.extend(check_thresholds(row, args.minimums, ">=", lambda got, want: got >= want))
    issues.extend(check_thresholds(row, args.maximums, "<=", lambda got, want: got <= want))
    return issues


def build_parser():
    parser = argparse.ArgumentParser(
        description="Validate nile_sync_sample.sh JSONL output for full staged-sync acceptance.",
    )
    parser.add_argument("result", type=Path, help="nile_sync_sample.sh JSONL output")
    parser.add_argument("--network", help="only consider rows with this network value")
    parser.add_argument("--mode", help="only consider rows with this mode value")
    parser.add_argument("--label", help="only consider rows with this label value")
    parser.add_argument(
        "--all",
        action="store_true",
        help="validate every selected row instead of only the latest selected row",
    )
    parser.add_argument(
        "--allow-warning-health",
        action="store_true",
        help="allow soakHealthStatus=warning; critical still fails",
    )
    parser.add_argument(
        "--no-require-stage-status",
        dest="require_stage_status",
        action="store_false",
        default=True,
        help="do not require stageStatusFileStatus=ok",
    )
    parser.add_argument(
        "--require-caught-up",
        action="store_true",
        help="require fullStagedSyncStatus=caught-up and completeAtHead=true",
    )
    parser.add_argument(
        "--require-offline-db-check",
        action="store_true",
        help="require offline db check fields to report ok",
    )
    parser.add_argument("--min-height", type=float, help="require latest height to be at least this value")
    parser.add_argument(
        "--max-lag-blocks",
        type=float,
        help="require fullStagedSyncHeadLagBlocks to be no greater than this value",
    )
    parser.add_argument(
        "--min",
        dest="minimums",
        action="append",
        default=[],
        metavar="FIELD=VALUE",
        help="numeric lower bound for a top-level JSONL field; repeatable",
    )
    parser.add_argument(
        "--max",
        dest="maximums",
        action="append",
        default=[],
        metavar="FIELD=VALUE",
        help="numeric upper bound for a top-level JSONL field; repeatable",
    )
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    rows, issues = load_rows(args.result)
    selected = filter_rows(rows, args)
    if not selected:
        issues.append("no rows matched selection filters")

    rows_to_check = selected if args.all else ([max(selected, key=row_sort_key)] if selected else [])
    for row in rows_to_check:
        for issue in check_row(row, args):
            issues.append(f"{row_label(row)}: {issue}")

    if issues:
        print("nile sync acceptance: failed", file=sys.stderr)
        for issue in issues:
            print(f"- {issue}", file=sys.stderr)
        return 1

    latest = max(selected, key=row_sort_key)
    print(
        "nile sync acceptance: ok "
        f"rows={len(rows_to_check)} latestLine={latest.get('_line')} "
        f"height={latest.get('height', '-')} "
        f"status={latest.get('fullStagedSyncStatus', '-')} "
        f"lag={latest.get('fullStagedSyncHeadLagBlocks', '-')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

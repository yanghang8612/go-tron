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

PROMETHEUS_STATUS_VALUES = {
    "ok": 0,
    "warning": 1,
    "critical": 2,
}

FULL_STAGED_SYNC_REQUIRED_STAGES = (
    "SyncBodies",
    "SyncBodiesReady",
    "SyncImport",
    "SyncExecution",
    "SyncCommitment",
    "SyncFinish",
)

DEFAULT_ARCHIVE_API_METHODS = (
    "eth_getBalance",
    "eth_getCode",
    "eth_getStorageAt",
    "eth_getLogs",
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


def split_csv_values(values):
    out = []
    for value in values:
        for item in value.split(","):
            item = item.strip()
            if item:
                out.append(item)
    return out


def approx_equal(got, want, tolerance=1e-9):
    if got is None or want is None:
        return False
    return abs(got - want) <= tolerance


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
    if "gtron_storage_alert_status{" in text:
        issues.extend(
            check_prometheus_metric_present(path, text, "gtron_storage_alert_status", row)
        )
        issues.extend(check_prometheus_alert_status_value(path, text, row))
    issues.extend(check_prometheus_issue_kinds(path, text, row))
    issues.extend(check_prometheus_stage_pipeline(path, text, row))
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


def check_prometheus_metric_value(path, text, metric, want, row):
    got = prometheus_metric_value(text, metric, row)
    if got is None:
        return [f"offlineDbCheckPrometheus artifact {path} missing {metric}"]
    if got != float(want):
        return [f"offlineDbCheckPrometheus artifact {path} {metric}={got:g}, want {want:g}"]
    return []


def check_prometheus_metric_present(path, text, metric, row):
    if prometheus_metric_value(text, metric, row) is None:
        return [f"offlineDbCheckPrometheus artifact {path} missing {metric}"]
    return []


def check_prometheus_alert_status_value(path, text, row):
    status = str(row.get("storageAlertStatus", "")).lower()
    if status not in PROMETHEUS_STATUS_VALUES:
        return []
    return check_prometheus_metric_value(
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


def check_prometheus_issue_kinds(path, text, row):
    issues = []
    actual = prometheus_issue_keys(text, row)
    for component, kind, severity in sorted(row_alert_issue_keys(row)):
        if (component, kind, severity) not in actual:
            issues.append(
                f"offlineDbCheckPrometheus artifact {path} missing "
                f"gtron_storage_alert_issue component={component!r} kind={kind!r} severity={severity!r}"
            )
    return issues


def check_prometheus_stage_pipeline(path, text, row):
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
            issues.append(f"offlineDbCheckPrometheus artifact {path} missing {name}")
    if "stageAlertPipelineComplete" in row:
        issues.extend(
            check_prometheus_metric_value(
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
                    path, text, "gtron_storage_stage_pipeline_pending", pending, row
                )
            )
    if "stageAlertPipelineIssues" in row:
        count = as_number(row, "stageAlertPipelineIssues")
        if count is not None:
            issues.extend(
                check_prometheus_metric_value(
                    path, text, "gtron_storage_stage_pipeline_issues", count, row
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
                f"offlineDbCheckPrometheus artifact {path} missing next pipeline target "
                f"stage={next_stage!r} status={want_status!r} upstream={want_upstream!r}"
            )
        elif want_target is not None and matched[-1] != want_target:
            issues.append(
                f"offlineDbCheckPrometheus artifact {path} next pipeline target "
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
                f"offlineDbCheckPrometheus artifact {path} missing next pipeline current "
                f"stage={next_stage!r} status={want_status!r} upstream={want_upstream!r}"
            )
        elif want_current is not None and matched[-1] != want_current:
            issues.append(
                f"offlineDbCheckPrometheus artifact {path} next pipeline current "
                f"stage={next_stage!r} status={want_status!r} upstream={want_upstream!r} "
                f"value={matched[-1]:g}, want {want_current:g}"
            )
    return issues


def check_full_staged_sync_evidence(row):
    issues = []
    required = row.get("fullStagedSyncRequiredStages")
    expected = list(FULL_STAGED_SYNC_REQUIRED_STAGES)
    if required != expected:
        issues.append(f"fullStagedSyncRequiredStages={required!r}, want {expected!r}")

    stage_count = as_number(row, "fullStagedSyncStageCount")
    present_count = as_number(row, "fullStagedSyncPresentStageCount")
    verified_count = as_number(row, "fullStagedSyncVerifiedStageCount")
    expected_count = float(len(expected))
    if stage_count != expected_count:
        issues.append(f"fullStagedSyncStageCount={stage_count}, want {len(expected)}")
    if present_count != expected_count:
        issues.append(f"fullStagedSyncPresentStageCount={present_count}, want {len(expected)}")
    if verified_count != expected_count:
        issues.append(f"fullStagedSyncVerifiedStageCount={verified_count}, want {len(expected)}")

    for field in (
        "fullStagedSyncMissingStages",
        "fullStagedSyncHashIssues",
        "fullStagedSyncUnverifiedStages",
    ):
        value = row.get(field)
        if value != []:
            issues.append(f"{field}={value!r}, want []")

    for field in ("fullStagedSyncStageCoverageRatio", "fullStagedSyncVerificationRatio"):
        value = as_number(row, field)
        if value != 1.0:
            issues.append(f"{field}={value}, want 1")

    complete = as_number(row, "fullStagedSyncCompleteBlock")
    head = as_number(row, "fullStagedSyncHeadBlock")
    lag = as_number(row, "fullStagedSyncHeadLagBlocks")
    for field, value in (
        ("fullStagedSyncCompleteBlock", complete),
        ("fullStagedSyncHeadBlock", head),
        ("fullStagedSyncHeadLagBlocks", lag),
    ):
        if value is None or value < 0:
            issues.append(f"{field}={value}, want >= 0")
    if complete is not None and head is not None and lag is not None:
        if head < complete:
            issues.append(f"fullStagedSyncHeadBlock={head:g} is below complete block {complete:g}")
        elif lag != head - complete:
            issues.append(
                f"fullStagedSyncHeadLagBlocks={lag:g}, want {head - complete:g} "
                "from fullStagedSyncHeadBlock-fullStagedSyncCompleteBlock"
            )
    completion_ratio = as_number(row, "fullStagedSyncCompletionRatio")
    if complete is not None and head is not None and head > 0:
        want = complete / head
        if not approx_equal(completion_ratio, want):
            issues.append(
                f"fullStagedSyncCompletionRatio={completion_ratio}, want {want:g} "
                "from fullStagedSyncCompleteBlock/fullStagedSyncHeadBlock"
            )

    pipeline_lag = as_number(row, "fullStagedSyncPipelineLagBlocks")
    if pipeline_lag is None or pipeline_lag < 0:
        issues.append(f"fullStagedSyncPipelineLagBlocks={pipeline_lag}, want >= 0")
    elif lag is not None and lag >= 0 and pipeline_lag < lag:
        issues.append(
            f"fullStagedSyncPipelineLagBlocks={pipeline_lag:g} is below "
            f"fullStagedSyncHeadLagBlocks={lag:g}"
        )
    stage_pipeline_lag = as_number(row, "stageSyncPipelineLagBlocks")
    if (
        pipeline_lag is not None
        and stage_pipeline_lag is not None
        and not approx_equal(pipeline_lag, stage_pipeline_lag)
    ):
        issues.append(
            f"fullStagedSyncPipelineLagBlocks={pipeline_lag:g}, "
            f"want stageSyncPipelineLagBlocks={stage_pipeline_lag:g}"
        )

    bottleneck = str(row.get("fullStagedSyncBottleneck", ""))
    bottleneck_lag = as_number(row, "fullStagedSyncBottleneckLagBlocks")
    if bottleneck_lag is None or bottleneck_lag < 0:
        issues.append(f"fullStagedSyncBottleneckLagBlocks={bottleneck_lag}, want >= 0")
    elif pipeline_lag is not None and pipeline_lag >= 0:
        if bottleneck_lag > pipeline_lag:
            issues.append(
                f"fullStagedSyncBottleneckLagBlocks={bottleneck_lag:g} exceeds "
                f"fullStagedSyncPipelineLagBlocks={pipeline_lag:g}"
            )
        if pipeline_lag == 0 and bottleneck != "none":
            issues.append(f"fullStagedSyncBottleneck={bottleneck!r}, want 'none' when pipeline lag is 0")
        if pipeline_lag > 0 and bottleneck in {"", "none", "unknown"}:
            issues.append(
                f"fullStagedSyncBottleneck={bottleneck!r}, want a concrete bottleneck "
                "when pipeline lag is positive"
            )
    stage_bottleneck = str(row.get("stageSyncBottleneck", ""))
    if bottleneck and stage_bottleneck and bottleneck != stage_bottleneck:
        issues.append(
            f"fullStagedSyncBottleneck={bottleneck!r}, want stageSyncBottleneck={stage_bottleneck!r}"
        )
    stage_bottleneck_lag = as_number(row, "stageSyncBottleneckLagBlocks")
    if (
        bottleneck_lag is not None
        and stage_bottleneck_lag is not None
        and not approx_equal(bottleneck_lag, stage_bottleneck_lag)
    ):
        issues.append(
            f"fullStagedSyncBottleneckLagBlocks={bottleneck_lag:g}, "
            f"want stageSyncBottleneckLagBlocks={stage_bottleneck_lag:g}"
        )
    bottleneck_share = as_number(row, "fullStagedSyncBottleneckLagShare")
    if pipeline_lag is not None and bottleneck_lag is not None:
        want_share = (bottleneck_lag / pipeline_lag) if pipeline_lag > 0 else -1.0
        if not approx_equal(bottleneck_share, want_share):
            issues.append(
                f"fullStagedSyncBottleneckLagShare={bottleneck_share}, want {want_share:g} "
                "from fullStagedSyncBottleneckLagBlocks/fullStagedSyncPipelineLagBlocks"
            )

    height = as_number(row, "height")
    if height is not None and head is not None and head != height:
        issues.append(f"fullStagedSyncHeadBlock={head:g}, want height {height:g}")

    status = str(row.get("fullStagedSyncStatus", "unknown"))
    complete_at_head = as_bool(row, "fullStagedSyncCompleteAtHead")
    ready = as_bool(row, "fullStagedSyncReady")
    want_ready = status in {"catching-up", "caught-up"}
    if ready != want_ready:
        issues.append(
            f"fullStagedSyncReady={row.get('fullStagedSyncReady')!r}, "
            f"want {want_ready!r} for status {status!r}"
        )
    if lag is not None:
        want_complete_at_head = ready and lag == 0
        if complete_at_head != want_complete_at_head:
            issues.append(
                f"fullStagedSyncCompleteAtHead={row.get('fullStagedSyncCompleteAtHead')!r}, "
                f"want {want_complete_at_head!r} from ready={ready!r} "
                f"and fullStagedSyncHeadLagBlocks={lag:g}"
            )
    if status == "caught-up" and (lag != 0 or not complete_at_head):
        issues.append(
            "full staged sync caught-up row is inconsistent: "
            f"lag={lag} completeAtHead={row.get('fullStagedSyncCompleteAtHead')!r}"
        )
    if status == "catching-up" and ((lag is not None and lag <= 0) or complete_at_head):
        issues.append(
            "full staged sync catching-up row is inconsistent: "
            f"lag={lag} completeAtHead={row.get('fullStagedSyncCompleteAtHead')!r}"
        )
    return issues


def check_stage_stall_evidence(row):
    fields = (
        "stageStalled",
        "stageStalledCount",
        "stageStalledStage",
        "stageStalledSeconds",
        "stageStalledLagBlocks",
        "stageStalls",
    )
    if not any(field in row for field in fields):
        return []

    issues = []
    stalled = as_bool(row, "stageStalled")
    stalls = row.get("stageStalls")
    if stalls is None:
        stalls = []
    if not isinstance(stalls, list):
        issues.append(f"stageStalls={stalls!r}, want list")
        stalls = []

    count = as_number(row, "stageStalledCount")
    if count is not None and count != len(stalls):
        issues.append(f"stageStalledCount={count:g}, want len(stageStalls)={len(stalls)}")

    health_issues = row.get("soakHealthIssues")
    has_stage_stalled_issue = isinstance(health_issues, list) and "stage-stalled" in health_issues
    if stalled and not has_stage_stalled_issue:
        issues.append("stageStalled=true but soakHealthIssues lacks 'stage-stalled'")
    if not stalled and has_stage_stalled_issue:
        issues.append("soakHealthIssues contains 'stage-stalled' but stageStalled is false")

    stage = str(row.get("stageStalledStage", ""))
    seconds = as_number(row, "stageStalledSeconds")
    lag = as_number(row, "stageStalledLagBlocks")
    if not stalled:
        if stalls:
            issues.append(f"stageStalls has {len(stalls)} entries while stageStalled is false")
        if stage:
            issues.append(f"stageStalledStage={stage!r}, want '' when stageStalled is false")
        if seconds is not None and seconds != 0:
            issues.append(f"stageStalledSeconds={seconds:g}, want 0 when stageStalled is false")
        if lag is not None and lag > 0:
            issues.append(f"stageStalledLagBlocks={lag:g}, want <= 0 when stageStalled is false")
        return issues

    if not stalls:
        issues.append("stageStalled=true but stageStalls is empty")
    if not stage:
        issues.append("stageStalledStage is empty while stageStalled is true")
    if seconds is None or seconds <= 0:
        issues.append(f"stageStalledSeconds={seconds}, want > 0 when stageStalled is true")
    if lag is None or lag <= 0:
        issues.append(f"stageStalledLagBlocks={lag}, want > 0 when stageStalled is true")

    primary = None
    for stall in stalls:
        if not isinstance(stall, dict):
            issues.append(f"stageStalls entry {stall!r} is not an object")
            continue
        if primary is None:
            primary = stall
            continue
        current_key = (
            as_number(stall, "stalledSeconds") or 0,
            as_number(stall, "lagBlocks") or 0,
        )
        primary_key = (
            as_number(primary, "stalledSeconds") or 0,
            as_number(primary, "lagBlocks") or 0,
        )
        if current_key > primary_key:
            primary = stall
    if primary is not None:
        primary_stage = str(primary.get("stage", ""))
        primary_seconds = as_number(primary, "stalledSeconds")
        primary_lag = as_number(primary, "lagBlocks")
        if stage and primary_stage and stage != primary_stage:
            issues.append(f"stageStalledStage={stage!r}, want primary stalled stage {primary_stage!r}")
        if seconds is not None and primary_seconds is not None and seconds != primary_seconds:
            issues.append(
                f"stageStalledSeconds={seconds:g}, want primary stalled seconds {primary_seconds:g}"
            )
        if lag is not None and primary_lag is not None and lag != primary_lag:
            issues.append(f"stageStalledLagBlocks={lag:g}, want primary stalled lag {primary_lag:g}")

    return issues


def archive_api_methods(row):
    raw = row.get("archiveApiMethods")
    if raw is None:
        return None
    if not isinstance(raw, list):
        return set()
    return {str(method) for method in raw}


def check_archive_api_evidence(row, required_methods):
    issues = []
    status = str(row.get("archiveApiStatus", "")).lower()
    if status != "ok":
        issues.append(f"archiveApiStatus={row.get('archiveApiStatus')!r}, want 'ok'")

    checks = as_number(row, "archiveApiChecks")
    if checks is None or checks <= 0:
        issues.append(f"archiveApiChecks={checks}, want > 0")

    failures = as_number(row, "archiveApiFailures")
    if failures is None:
        issues.append(f"archiveApiFailures={failures}, want 0")
    elif failures != 0:
        issues.append(f"archiveApiFailures={failures:g}, want 0")

    block = as_number(row, "archiveApiBlock")
    if block is None or block < 0:
        issues.append(f"archiveApiBlock={block}, want >= 0 historical block")
    else:
        height = as_number(row, "height")
        if height is not None and block >= height:
            issues.append(f"archiveApiBlock={block:g} must be below height={height:g}")
        tail_pruned = as_number(row, "tailPrunedThroughBlock")
        if tail_pruned is not None and tail_pruned >= 0 and block > tail_pruned:
            issues.append(
                f"archiveApiBlock={block:g} must be <= tailPrunedThroughBlock={tail_pruned:g} "
                "to prove post-prune archive reads"
            )

    methods = archive_api_methods(row)
    if methods is None:
        issues.append("archiveApiMethods is missing")
    elif not methods:
        issues.append("archiveApiMethods must be a non-empty list")
    else:
        missing = sorted(set(required_methods) - methods)
        if missing:
            issues.append("archiveApiMethods missing required methods: " + ",".join(missing))

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
    if args.require_stage_status:
        issues.extend(check_full_staged_sync_evidence(row))

    status = str(row.get("fullStagedSyncStatus", "unknown"))
    if args.require_caught_up:
        if (
            status != "caught-up"
            or not as_bool(row, "fullStagedSyncReady")
            or not as_bool(row, "fullStagedSyncCompleteAtHead")
        ):
            issues.append(
                "full staged sync is not caught up: "
                f"status={status!r} ready={row.get('fullStagedSyncReady')!r} "
                f"completeAtHead={row.get('fullStagedSyncCompleteAtHead')!r}"
            )
    elif status not in {"catching-up", "caught-up"} or not as_bool(row, "fullStagedSyncReady"):
        issues.append(
            "full staged sync is not ready: "
            f"status={status!r} ready={row.get('fullStagedSyncReady')!r}"
        )

    if row.get("stageSyncPipelineMonotonic") is not None and not as_bool(row, "stageSyncPipelineMonotonic"):
        issues.append("stageSyncPipelineMonotonic=false")
    issues.extend(check_stage_stall_evidence(row))
    if args.require_archive_api_evidence:
        issues.extend(check_archive_api_evidence(row, args.archive_api_methods_required))

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
    parser.add_argument(
        "--require-archive-api-evidence",
        action="store_true",
        help="require selected rows to include successful historical archive API evidence",
    )
    parser.add_argument(
        "--archive-api-method",
        action="append",
        default=[],
        help="additional archive API method that must appear in archiveApiMethods; repeatable",
    )
    parser.add_argument(
        "--archive-api-methods",
        action="append",
        default=[],
        help="comma-separated additional archive API methods that must appear in archiveApiMethods",
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
    args.archive_api_methods_required = list(DEFAULT_ARCHIVE_API_METHODS)
    for method in split_csv_values(args.archive_api_method + args.archive_api_methods):
        if method not in args.archive_api_methods_required:
            args.archive_api_methods_required.append(method)
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

#!/usr/bin/env python3
"""Export Prometheus artifacts referenced by JSONL soak/benchmark rows."""

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path


DEFAULT_FIELDS = (
    "samplePrometheus",
    "offlineDbCheckPrometheus",
    "storageAlertPrometheus",
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
            issues.append(f"{path}:{line_no}: expected JSON object")
            continue
        row["_line"] = line_no
        row["_jsonl"] = str(path)
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


def selected_rows(paths, all_rows):
    selected = []
    issues = []
    for path in paths:
        rows, row_issues = load_rows(path)
        issues.extend(row_issues)
        if not rows:
            continue
        if all_rows:
            selected.extend(rows)
        else:
            selected.append(max(rows, key=row_sort_key))
    return selected, issues


def resolve_artifact(row, field):
    raw = row.get(field)
    if not raw:
        return None
    path = Path(str(raw))
    if path.is_absolute():
        return path
    return Path(row["_jsonl"]).parent / path


def read_artifacts(rows, fields, required_fields):
    chunks = []
    issues = []
    seen = set()
    found_fields = set()
    for row in rows:
        label = f"{row['_jsonl']}:line {row.get('_line', '?')}"
        for field in fields:
            path = resolve_artifact(row, field)
            if path is None:
                continue
            found_fields.add(field)
            key = (field, str(path))
            if key in seen:
                continue
            seen.add(key)
            try:
                text = path.read_text(encoding="utf-8")
            except OSError as exc:
                issues.append(f"{label}: read {field} artifact {path}: {exc}")
                continue
            if text.strip():
                chunks.append((field, path, text.rstrip() + "\n"))

    for field in required_fields:
        if field not in found_fields:
            issues.append(f"no selected row provided required field {field}")
    if not chunks and not issues:
        issues.append("no prometheus artifacts found in selected rows")
    return chunks, issues


def atomic_write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def render(chunks):
    lines = [
        "# Exported by scripts/dev/prometheus_artifact_export.py",
        "# Source artifacts are referenced by accepted JSONL sample rows.",
    ]
    for field, path, text in chunks:
        lines.append(f"# BEGIN {field} {path}")
        lines.append(text.rstrip())
        lines.append(f"# END {field} {path}")
    return "\n".join(lines) + "\n"


def build_parser():
    parser = argparse.ArgumentParser(
        description=(
            "Combine Prometheus text artifacts referenced by Nile sync/storage "
            "benchmark JSONL rows into one scrapeable output file."
        )
    )
    parser.add_argument("jsonl", nargs="+", type=Path, help="input JSONL result files")
    parser.add_argument("--output", required=True, type=Path, help="combined Prometheus text output")
    parser.add_argument(
        "--field",
        action="append",
        default=[],
        help="artifact field to export; repeatable (default: known sampler/benchmark fields)",
    )
    parser.add_argument(
        "--require-field",
        action="append",
        default=[],
        help="artifact field that must be present and readable in selected rows; repeatable",
    )
    parser.add_argument(
        "--all-rows",
        action="store_true",
        help="export artifacts from every row instead of only the latest row per JSONL file",
    )
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    fields = tuple(args.field) if args.field else DEFAULT_FIELDS
    required_fields = set(args.require_field)
    unknown_required = sorted(required_fields - set(fields))
    if unknown_required:
        parser.error("--require-field must also be included by --field/default fields: " + ",".join(unknown_required))

    rows, issues = selected_rows(args.jsonl, args.all_rows)
    chunks, artifact_issues = read_artifacts(rows, fields, required_fields)
    issues.extend(artifact_issues)
    if issues:
        print("prometheus artifact export: failed", file=sys.stderr)
        for issue in issues:
            print(f"- {issue}", file=sys.stderr)
        return 1

    try:
        atomic_write(args.output, render(chunks))
    except OSError as exc:
        print(f"prometheus artifact export: write {args.output}: {exc}", file=sys.stderr)
        return 1

    print(
        "prometheus artifact export: ok "
        f"rows={len(rows)} artifacts={len(chunks)} output={args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

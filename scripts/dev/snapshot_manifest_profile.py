#!/usr/bin/env python3
import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path


SIDECAR_KINDS = {
    "accessor",
    "btree",
    "chain-index",
    "chain-freezer-accessor",
    "event-log-index",
    "inverted",
}

LATEST_DATASETS = {
    "account-latest",
    "kv-latest",
    "kv-generation",
    "code",
    "commitment-root",
    "commitment-checkpoint",
}

POINT_INDEX_CANDIDATES = (
    "txHashLookup",
    "eventLogIndex",
    "stateHistoryAccessor",
    "latestBTree",
    "chainFreezerAccessor",
    "codeDomain",
    "commitmentSnapshot",
)


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Profile active snapshot manifest payload and sidecar sizes."
    )
    parser.add_argument(
        "path",
        help="Snapshot directory containing manifest.json, or a manifest JSON path.",
    )
    parser.add_argument(
        "--include-retired",
        action="store_true",
        help="Include manifest retired segments in size totals.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print a machine-readable JSON profile.",
    )
    parser.add_argument(
        "--max-sidecar-share-milli",
        type=int,
        metavar="N",
        help="Fail if overall sidecar bytes exceed N/1000 of total bytes.",
    )
    parser.add_argument(
        "--max-family-sidecar-share-milli",
        type=int,
        metavar="N",
        help="Fail if any family sidecar bytes exceed N/1000 of that family's total bytes.",
    )
    return parser.parse_args(argv)


def manifest_path(path):
    candidate = Path(path)
    if candidate.is_dir():
        return candidate / "manifest.json"
    return candidate


def load_manifest(path):
    resolved = manifest_path(path)
    with resolved.open("r", encoding="utf-8") as fh:
        return resolved, json.load(fh)


def ratio_milli(part, total):
    if part <= 0 or total <= 0:
        return 0
    return (part * 1000 + total - 1) // total


def segment_size(ref, source):
    value = ref.get("size", 0)
    try:
        out = int(value)
    except (TypeError, ValueError) as exc:
        path = ref.get("path", "<unknown>")
        raise ValueError(f"{source} segment {path} has non-integer size {value!r}") from exc
    if out < 0:
        path = ref.get("path", "<unknown>")
        raise ValueError(f"{source} segment {path} has negative size {out}")
    return out


def is_sidecar(kind):
    return kind in SIDECAR_KINDS


def segment_family(dataset, kind):
    if kind in {"chain-freezer", "chain-index", "chain-freezer-accessor"} or dataset == "chain-freezer":
        return "chain-freezer"
    if kind in {"event-log", "event-log-index"} or dataset == "event-log":
        return "event-log"
    if kind == "balance-trace" or dataset == "balance-trace":
        return "balance-trace"
    if kind == "section-bloom" or dataset == "section-bloom":
        return "section-bloom"
    if kind in {"latest", "accessor", "btree"} or dataset in LATEST_DATASETS:
        return "latest"
    if kind in {"history", "inverted"} or dataset == "state-domain-change":
        return "state-history"
    return "other"


def empty_stats():
    return {
        "segments": 0,
        "totalBytes": 0,
        "payloadBytes": 0,
        "sidecarBytes": 0,
    }


def add_stats(stats, size, sidecar):
    stats["segments"] += 1
    stats["totalBytes"] += size
    if sidecar:
        stats["sidecarBytes"] += size
    else:
        stats["payloadBytes"] += size


def finalize_stats(stats):
    stats["sidecarShareMilli"] = ratio_milli(stats["sidecarBytes"], stats["totalBytes"])
    return stats


def finalize_candidate_stats(stats, snapshot_total):
    stats = finalize_stats(stats)
    stats["snapshotShareMilli"] = ratio_milli(stats["totalBytes"], snapshot_total)
    return stats


def sorted_stats(stats_by_key):
    return {
        key: finalize_stats(dict(value))
        for key, value in sorted(stats_by_key.items(), key=lambda item: item[0])
    }


def point_index_candidate_names(dataset, kind):
    names = []
    if kind == "chain-index":
        names.append("txHashLookup")
    if kind == "event-log-index":
        names.append("eventLogIndex")
    if dataset == "state-domain-change":
        names.append("stateHistoryAccessor")
    if kind == "btree":
        names.append("latestBTree")
    if kind == "chain-freezer-accessor":
        names.append("chainFreezerAccessor")
    if dataset == "code":
        names.append("codeDomain")
    if dataset in {"commitment-root", "commitment-checkpoint"}:
        names.append("commitmentSnapshot")
    return names


def profile_manifest(path, include_retired=False):
    resolved, manifest = load_manifest(path)
    active = list(manifest.get("segments") or [])
    retired = list(manifest.get("retired") or []) if include_retired else []

    overall = empty_stats()
    by_family = defaultdict(empty_stats)
    by_kind = defaultdict(empty_stats)
    by_dataset = defaultdict(empty_stats)
    sidecar_kinds = defaultdict(int)
    point_candidates = {name: empty_stats() for name in POINT_INDEX_CANDIDATES}

    for source, refs in (("active", active), ("retired", retired)):
        for ref in refs:
            kind = str(ref.get("kind", "")).strip()
            dataset = str(ref.get("dataset", "")).strip()
            size = segment_size(ref, source)
            sidecar = is_sidecar(kind)
            family = segment_family(dataset, kind)
            add_stats(overall, size, sidecar)
            add_stats(by_family[family], size, sidecar)
            add_stats(by_kind[kind or "<missing>"], size, sidecar)
            add_stats(by_dataset[dataset or "<missing>"], size, sidecar)
            if sidecar:
                sidecar_kinds[kind] += 1
            for name in point_index_candidate_names(dataset, kind):
                add_stats(point_candidates[name], size, sidecar)

    profile = finalize_stats(overall)
    profile.update(
        {
            "manifest": str(resolved),
            "version": manifest.get("version"),
            "generation": manifest.get("generation"),
            "visibleTxStart": manifest.get("visibleTxStart"),
            "visibleTxEnd": manifest.get("visibleTxEnd"),
            "includeRetired": include_retired,
            "activeSegments": len(active),
            "retiredSegments": len(retired),
            "byFamily": sorted_stats(by_family),
            "byKind": sorted_stats(by_kind),
            "byDataset": sorted_stats(by_dataset),
            "sidecarKinds": dict(sorted(sidecar_kinds.items())),
            "pointIndexCandidates": {
                name: finalize_candidate_stats(dict(point_candidates[name]), profile["totalBytes"])
                for name in POINT_INDEX_CANDIDATES
            },
            "issues": [],
        }
    )
    return profile


def apply_thresholds(profile, max_sidecar_share_milli, max_family_sidecar_share_milli):
    issues = []
    if (
        max_sidecar_share_milli is not None
        and profile["sidecarShareMilli"] > max_sidecar_share_milli
    ):
        issues.append(
            "overall sidecar share "
            f"{profile['sidecarShareMilli']} milli exceeds max {max_sidecar_share_milli}"
        )
    if max_family_sidecar_share_milli is not None:
        for family, stats in profile["byFamily"].items():
            if stats["sidecarShareMilli"] > max_family_sidecar_share_milli:
                issues.append(
                    f"{family} sidecar share {stats['sidecarShareMilli']} milli "
                    f"exceeds max {max_family_sidecar_share_milli}"
                )
    profile["issues"] = issues
    return issues


def print_human(profile):
    suffix = ""
    if profile["includeRetired"]:
        suffix = f" + {profile['retiredSegments']} retired"
    print(f"snapshot manifest: {profile['manifest']}")
    print(
        f"segments={profile['activeSegments']} active{suffix} "
        f"totalBytes={profile['totalBytes']} "
        f"payloadBytes={profile['payloadBytes']} "
        f"sidecarBytes={profile['sidecarBytes']} "
        f"sidecarShareMilli={profile['sidecarShareMilli']}"
    )
    for family, stats in sorted(
        profile["byFamily"].items(),
        key=lambda item: (-item[1]["totalBytes"], item[0]),
    ):
        print(
            f"{family}: segments={stats['segments']} "
            f"totalBytes={stats['totalBytes']} "
            f"payloadBytes={stats['payloadBytes']} "
            f"sidecarBytes={stats['sidecarBytes']} "
            f"sidecarShareMilli={stats['sidecarShareMilli']}"
        )
    candidates = {
        name: stats
        for name, stats in profile["pointIndexCandidates"].items()
        if stats["segments"] > 0
    }
    if candidates:
        print("point-index candidates:")
        for name, stats in sorted(
            candidates.items(),
            key=lambda item: (-item[1]["totalBytes"], item[0]),
        ):
            print(
                f"{name}: segments={stats['segments']} "
                f"totalBytes={stats['totalBytes']} "
                f"payloadBytes={stats['payloadBytes']} "
                f"sidecarBytes={stats['sidecarBytes']} "
                f"sidecarShareMilli={stats['sidecarShareMilli']} "
                f"snapshotShareMilli={stats['snapshotShareMilli']}"
            )
    if profile["issues"]:
        print("issues:")
        for issue in profile["issues"]:
            print(f"- {issue}")


def main(argv=None):
    args = parse_args(argv or sys.argv[1:])
    try:
        profile = profile_manifest(args.path, include_retired=args.include_retired)
        issues = apply_thresholds(
            profile,
            args.max_sidecar_share_milli,
            args.max_family_sidecar_share_milli,
        )
    except Exception as exc:
        print(f"snapshot manifest profile: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(profile, sort_keys=True))
    else:
        print_human(profile)
    if issues:
        for issue in issues:
            print(f"snapshot manifest profile: {issue}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Bounded, read-only Linux sampler. JSONL to stdout; never controls the node.

No database open, recursive directory scan, remote access, or service mutation.
Run with permission to read the selected process's /proc/PID/io. Counters are
raw unless explicitly under rates; missing/error observations are not zeroes.
"""

import argparse
import datetime
import http.client
import json
import math
import os
from pathlib import Path
import sys
import time


READ_CAP = 4 * 1024 * 1024
OUTPUT_CAP = 128 * 1024 * 1024
PREFIXES = (
    "chain_", "sync_", "p2p_", "state_", "process_", "system_", "go_",
    "compact_", "disk_", "level_", "stall_", "table_", "tables_", "cache_",
    "filter_", "iter_", "memory_", "ancient_", "blockbuffer_",
)
FRONTIERS = {
    "head": "chain_head_block",
    "solidified": "chain_solidified_block",
    "cold_eligible": "state_snapshot_cold_last_eligible_cutoff_block",
    "cold_published": "state_snapshot_cold_last_published_block",
    "hot_pruned": "state_prune_last_domain_change_pruned_through_block",
}


def bounded_read(path):
    with open(path, "rb") as stream:
        raw = stream.read(READ_CAP + 1)
    if len(raw) > READ_CAP:
        raise ValueError("read cap exceeded: " + str(path))
    return raw.decode("utf-8", errors="strict")


def parse_stat(text):
    # comm may contain spaces and parentheses; fields after its last ')' start
    # at field 3 (state). Only the counters required here are interpreted.
    fields = text[text.rindex(")") + 2:].split()
    return {"state": fields[0], "user_ticks": int(fields[11]),
            "system_ticks": int(fields[12]), "start_ticks": int(fields[19]),
            "rss_pages": int(fields[21])}


def parse_kv(text):
    out = {}
    for line in text.splitlines():
        key, sep, value = line.partition(":")
        fields = value.split()
        if sep and fields and fields[0].isdigit():
            out[key] = int(fields[0]) * (1024 if fields[1:] == ["kB"] else 1)
    return out


def parse_diskstats(text, device):
    for line in text.splitlines():
        fields = line.split()
        if len(fields) >= 14 and fields[2] == device:
            n = [int(x) for x in fields[3:]]
            return {"reads_completed": n[0], "read_bytes": n[2] * 512,
                    "read_ms": n[3], "writes_completed": n[4],
                    "write_bytes": n[6] * 512, "write_ms": n[7],
                    "inflight": n[8], "io_ms": n[9], "weighted_io_ms": n[10]}
    raise ValueError("device absent from /proc/diskstats: " + device)


def parse_metrics(text):
    out = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.rsplit(None, 1)
        if len(parts) != 2 or not parts[0].startswith(PREFIXES):
            continue
        try:
            value = int(parts[1])
        except ValueError:
            try:
                value = float(parts[1])
            except ValueError:
                continue
        if math.isfinite(value):
            # Preserve labels and unavailable versus zero. This exporter emits
            # no optional sample timestamps. Integer counters remain integers.
            out[parts[0]] = value
            if len(out) > 16384:
                raise ValueError("selected metric count cap exceeded")
    return out


def read_metrics(port):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
    try:
        connection.request("GET", "/metrics")
        response = connection.getresponse()
        if response.status != 200:
            raise ValueError("metrics HTTP status " + str(response.status))
        raw = response.read(READ_CAP + 1)
        if len(raw) > READ_CAP:
            raise ValueError("metrics response cap exceeded")
        return parse_metrics(raw.decode("utf-8"))
    finally:
        connection.close()


def filesystem(path):
    stat = os.statvfs(path)
    return {"total_bytes": stat.f_blocks * stat.f_frsize,
            "available_bytes": stat.f_bavail * stat.f_frsize,
            "free_bytes_including_reserved": stat.f_bfree * stat.f_frsize,
            "inodes": stat.f_files, "available_inodes": stat.f_favail}


def derive(previous, current, hz):
    result = {}
    elapsed = current["monotonic"] - previous["monotonic"]
    if elapsed <= 0 or current["process"]["start_ticks"] != previous["process"]["start_ticks"]:
        return result
    for family, keys in (("process_io", ("read_bytes", "write_bytes")),
                         ("disk", ("read_bytes", "write_bytes", "reads_completed", "writes_completed")),
                         ("frontiers", tuple(FRONTIERS))):
        for key in keys:
            before, after = previous.get(family, {}).get(key), current.get(family, {}).get(key)
            if before is not None and after is not None:
                result[family + "." + key + "_per_second"] = (after - before) / elapsed
    for key in ("user_ticks", "system_ticks"):
        result["process." + key.replace("_ticks", "_cpu_cores")] = (
            current["process"][key] - previous["process"][key]) / (hz * elapsed)
    if "data_fs" in previous and "data_fs" in current:
        result["data_fs.net_consumed_bytes_per_second"] = (
            previous["data_fs"]["available_bytes"] - current["data_fs"]["available_bytes"]) / elapsed
    if "disk" in previous and "disk" in current:
        result["disk.average_queue_depth"] = (
            current["disk"]["weighted_io_ms"] - previous["disk"]["weighted_io_ms"]) / (elapsed * 1000)
        result["disk.busy_fraction"] = (
            current["disk"]["io_ms"] - previous["disk"]["io_ms"]) / (elapsed * 1000)
    return result


def sample(args, identity):
    started = time.monotonic()
    row = {"utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
           "monotonic": started, "pid": args.pid, "errors": {}, "warnings": []}
    proc = Path("/proc") / str(args.pid)
    row["process"] = parse_stat(bounded_read(proc / "stat"))
    if row["process"]["start_ticks"] != identity:
        raise RuntimeError("PID identity changed; start a new capture for the new process")
    reads = {
        "process_io": lambda: parse_kv(bounded_read(proc / "io")),
        "process_status": lambda: parse_kv(bounded_read(proc / "status")),
        "memory": lambda: parse_kv(bounded_read("/proc/meminfo")),
        "host_cpu": lambda: [int(x) for x in bounded_read("/proc/stat").splitlines()[0].split()[1:]],
        "disk": lambda: parse_diskstats(bounded_read("/proc/diskstats"), args.device),
        "data_fs": lambda: filesystem(args.data_path),
        "root_fs": lambda: filesystem("/"),
        "metrics": lambda: read_metrics(args.metrics_port),
    }
    for name, read in reads.items():
        try:
            row[name] = read()
        except (OSError, ValueError, http.client.HTTPException) as exc:
            row["errors"][name] = str(exc)
    # Detect exit/restart during the scrape. Never blend identities into rates.
    if parse_stat(bounded_read(proc / "stat"))["start_ticks"] != identity:
        raise RuntimeError("process changed during sample")
    row["frontiers"] = {key: row.get("metrics", {}).get(metric) for key, metric in FRONTIERS.items()}
    row["missing_frontiers"] = [key for key, value in row["frontiers"].items() if value is None]
    if row["missing_frontiers"]:
        row["warnings"].append("frontier observations incomplete")
    fs = row.get("data_fs")
    if fs and fs["total_bytes"]:
        free_pct = 100 * fs["available_bytes"] / fs["total_bytes"]
        row["data_available_percent"] = free_pct
        if free_pct <= args.stop_free_percent:
            row["warnings"].append("operator stop recommended: experiment reserve reached; NO signal sent")
        elif free_pct <= args.warn_free_percent:
            row["warnings"].append("operator capacity review recommended")
    row["collection_seconds"] = time.monotonic() - started
    return row


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", required=True, type=int)
    parser.add_argument("--device", required=True, help="verified whole device name, e.g. nvme1n1")
    parser.add_argument("--data-path", default="/data")
    parser.add_argument("--metrics-port", type=int, default=6071)
    parser.add_argument("--samples", type=int, default=121)
    parser.add_argument("--interval", type=float, default=15)
    parser.add_argument("--warn-free-percent", type=float, default=10)
    parser.add_argument("--stop-free-percent", type=float, default=5)
    args = parser.parse_args(argv)
    if (args.pid <= 0 or not 1 <= args.samples <= 1441 or not 5 <= args.interval <= 60
            or args.samples * args.interval > 21660 or not 1 <= args.metrics_port <= 65535
            or not 0 < args.stop_free_percent < args.warn_free_percent < 100):
        parser.error("invalid bounds (maximum capture approximately 6 hours, 1441 samples)")
    hz = os.sysconf("SC_CLK_TCK")
    identity = parse_stat(bounded_read(Path("/proc") / str(args.pid) / "stat"))["start_ticks"]
    previous, output_bytes = None, 0
    metadata = {"kind": "capture", "arguments": vars(args), "clock_ticks_per_second": hz,
                "page_bytes": os.sysconf("SC_PAGE_SIZE"), "boot_id": bounded_read("/proc/sys/kernel/random/boot_id").strip(),
                "process_start_ticks": identity, "notes": [
                    "Read-only observer; warnings never stop the service.",
                    "Disk/free-space counters include MySQL and every process on the shared volume.",
                    "Frontier gauges update at different instants; not an atomic database proof.",
                    "Bounded socket inactivity timeout; not a strict response wall-clock deadline."]}
    print(json.dumps(metadata, separators=(",", ":")), flush=True)
    deadline = time.monotonic()
    for index in range(args.samples):
        if index:
            time.sleep(max(0, deadline - time.monotonic()))
        row = sample(args, identity)
        row["sample_index"] = index
        if previous:
            row["rates"] = derive(previous, row, hz)
        encoded = json.dumps(row, separators=(",", ":"), allow_nan=False)
        output_bytes += len(encoded.encode("utf-8")) + 1
        if output_bytes > OUTPUT_CAP:
            raise RuntimeError("128 MiB output cap reached; capture ended, node untouched")
        print(encoded, flush=True)
        previous = row
        deadline = max(deadline + args.interval, time.monotonic())


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, http.client.HTTPException) as error:
        print(json.dumps({"kind": "capture_error", "error": str(error), "node_action": "none"}), file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("capture interrupted; node untouched", file=sys.stderr)
        sys.exit(130)

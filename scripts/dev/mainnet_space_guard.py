#!/usr/bin/env python3
"""Mainnet-only, latching disk stop guard. Never starts a service or opens a DB.

Install a root-owned copy outside the checkout; see the deployment review.
`check` is read-only and suitable for an unprivileged ExecStartPre. `guard`
requires root and can only create its fixed latch and stop gtron.service.
"""

import json
import os
import stat
import subprocess
import sys
import time

CONFIG = "/etc/gtron/mainnet-space-guard.json"
HOLD = "/var/lib/gtron-mainnet-disk-stop.hold"
DATA = "/data"
DATADIR = "/data/gtron/main/datadir"
SYSTEMCTL = "/bin/systemctl"
GIB = 1024 ** 3
ROOT_RESERVE = 5 * GIB


def filesystem_space(path):
    vfs = os.statvfs(path)
    if (vfs.f_frsize <= 0 or vfs.f_blocks <= 0 or
            not 0 <= vfs.f_bavail <= vfs.f_blocks or vfs.f_files <= 0 or
            not 0 <= vfs.f_favail <= vfs.f_files):
        raise ValueError("invalid filesystem accounting: " + path)
    return (vfs.f_blocks * vfs.f_frsize, vfs.f_bavail * vfs.f_frsize,
            vfs.f_favail, (vfs.f_files + 19) // 20)


def read_config():
    fd = os.open(CONFIG, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
            raise ValueError("config must be a root-owned, non-writable regular file")
        raw = stream.read(16385)
    if len(raw) > 16384:
        raise ValueError("config exceeds 16 KiB")
    cfg = json.loads(raw.decode("utf-8"))
    expected = {"data_device", "stop_bytes", "start_bytes", "min_free_inodes"}
    if not isinstance(cfg, dict) or set(cfg) != expected:
        raise ValueError("unexpected config keys")
    if any(type(cfg[k]) is not int or cfg[k] <= 0 for k in expected):
        raise ValueError("config values must be positive integers")
    if not 32 * GIB <= cfg["stop_bytes"] < cfg["start_bytes"]:
        raise ValueError("require 32 GiB <= stop_bytes < start_bytes")
    return cfg


def inspect_space(starting):
    cfg = read_config()
    paths = [DATA, DATADIR]
    child = os.path.join(DATADIR, "gtron")
    if os.path.lexists(child):
        paths.append(child)
    for path in paths:
        info = os.stat(path)
        if not stat.S_ISDIR(info.st_mode) or info.st_dev != cfg["data_device"]:
            raise ValueError("data directory/device mismatch: " + path)
    total, free, inodes, data_inode_floor = filesystem_space(DATADIR)
    _, root_free, root_inodes, root_inode_floor = filesystem_space("/")
    if cfg["start_bytes"] >= total:
        raise ValueError("start reserve must be smaller than filesystem")
    result = {"time_unix": int(time.time()), "mode": "check" if starting else "guard",
              "free_bytes": free, "free_inodes": inodes,
              "min_free_inodes": max(cfg["min_free_inodes"], data_inode_floor),
              "root_free_bytes": root_free, "root_free_inodes": root_inodes,
              "root_min_free_inodes": root_inode_floor,
              "threshold_bytes": cfg["start_bytes"] if starting else cfg["stop_bytes"]}
    # lexists also treats a broken symlink as a stop latch; never follow it.
    reasons = []
    if os.path.lexists(HOLD):
        reasons.append("mainnet stop latch exists")
    if result["free_bytes"] <= result["threshold_bytes"]:
        reasons.append("free byte reserve reached")
    if result["free_inodes"] <= result["min_free_inodes"]:
        reasons.append("free inode reserve reached")
    if root_free <= ROOT_RESERVE:
        reasons.append("root filesystem 5 GiB reserve reached")
    if root_inodes <= root_inode_floor:
        reasons.append("root filesystem 5 percent inode reserve reached")
    result["reasons"] = reasons
    result["ok"] = not reasons
    return result


def create_latch(result):
    try:
        fd = os.open(HOLD, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
    except FileExistsError:
        return
    with os.fdopen(fd, "w") as stream:
        stream.write(json.dumps(result, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    parent_fd = os.open(os.path.dirname(HOLD), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)


def run(mode):
    if mode not in ("check", "guard"):
        raise ValueError("mode must be check or guard")
    if mode == "guard" and os.geteuid() != 0:
        raise PermissionError("guard mode requires root")
    try:
        result = inspect_space(mode == "check")
    except Exception as exc:
        result = {"ok": False, "mode": mode, "time_unix": int(time.time()),
                  "reasons": ["space inspection failed: " + str(exc)[:1024]]}
    if result["ok"]:
        if mode == "check":
            print(json.dumps(result, sort_keys=True))
        return 0
    if mode == "guard":
        try:
            create_latch(result)
        except Exception as exc:
            # A full root filesystem must not prevent the stop attempt.
            result["latch_error"] = str(exc)[:1024]
        try:
            stopped = subprocess.run([SYSTEMCTL, "stop", "gtron.service"],
                                     stdin=subprocess.DEVNULL, timeout=650, check=False)
            result["stop_returncode"] = stopped.returncode
        except Exception as exc:
            result["stop_error"] = str(exc)[:1024]
    print(json.dumps(result, sort_keys=True), file=sys.stderr)
    return 1


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ("check", "guard"):
        sys.exit("usage: mainnet_space_guard.py check|guard")
    sys.exit(run(sys.argv[1]))

#!/usr/bin/env python3
"""Read-only cold-file census. Usage: cold_census.py SNAPSHOT_DIR OUTPUT.json

No database open, decompression, payload checksum, publication, or deletion.
Reads at most 128 header + 48 footer bytes per referenced regular file.
The manifest is bounded to 128 MiB. Directory inventory only stats entries.
Output must be outside the snapshot directory and must not already exist.
Header claims are not authenticated; concurrent observations are not a snapshot.
"""
import argparse
import collections
import datetime
import hashlib
import json
import os
import stat
import struct
import sys
import zlib

MANIFEST_LIMIT = 128 << 20
BLOCK_KINDS = {"chain-freezer", "chain-index", "chain-freezer-accessor",
               "balance-trace", "section-bloom", "event-log", "event-log-index"}


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def identity(s):
    return {"dev": s.st_dev, "ino": s.st_ino, "size": s.st_size,
            "mtime_ns": s.st_mtime_ns, "ctime_ns": s.st_ctime_ns,
            "allocated_bytes": s.st_blocks * 512, "nlink": s.st_nlink}


def open_under(rootfd, path, directory=False):
    # Reject traversal and every symlink component, including replacement races.
    parts = path.split("/")
    if not path or any(p in ("", ".", "..") for p in parts):
        raise ValueError("unsafe relative path")
    fd = os.dup(rootfd)
    try:
        for i, part in enumerate(parts):
            flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
            if i < len(parts) - 1 or directory:
                flags |= os.O_DIRECTORY
            child = os.open(part, flags, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def manifest_read(rootfd):
    fd = open_under(rootfd, "manifest.json")
    try:
        a = os.fstat(fd)
        if not stat.S_ISREG(a.st_mode) or a.st_size > MANIFEST_LIMIT:
            raise ValueError("manifest is not regular or exceeds 128 MiB")
        with os.fdopen(os.dup(fd), "rb") as f:
            raw = f.read(MANIFEST_LIMIT + 1)
        b = os.fstat(fd)
        if len(raw) > MANIFEST_LIMIT or identity(a) != identity(b):
            raise ValueError("manifest changed during read or exceeds limit")
        return raw, identity(b)
    finally:
        os.close(fd)


def header_info(fd, size):
    b = os.pread(fd, 128, 0)
    out = {"format": "unknown", "container_version": None,
           "inner_version": None, "logical_bytes": None,
           "rows": None, "row_unit": None, "read_bytes": len(b)}

    def u32(buf, off):
        return struct.unpack_from(">I", buf, off)[0]

    def u64(buf, off):
        return struct.unpack_from(">Q", buf, off)[0]

    def need(n):
        if len(b) < n:
            raise ValueError("truncated header")

    magic = b[:8]
    out["magic_hex"] = magic.hex()
    if magic == b"gtcblk01":
        need(48)
        v = u32(b, 8)
        out.update(format="compressed-block", container_version=v)
        if v == 1:
            out.update(rows=u64(b, 16), row_unit="records",
                       chunks=u64(b, 24), logical_bytes=u64(b, 32))
        elif v in (2, 3):
            if size < 96 or any(b[16:48]):
                raise ValueError("invalid footer container header")
            f = os.pread(fd, 48, size - 48)
            out["read_bytes"] += len(f)
            if len(f) != 48 or f[:8] != (b"gtcend02" if v == 2 else b"gtcend03"):
                raise ValueError("invalid container footer")
            if v == 2:
                count, blocks, logical, off, length = struct.unpack_from(">5Q", f, 8)
                if off < 48 or length != blocks * 28 or off + length != size - 48:
                    raise ValueError("invalid V2 table bounds")
                out.update(rows=count, row_unit="records", chunks=blocks,
                           logical_bytes=logical)
            else:
                if u32(f, 40) != zlib.crc32(f[:40]) or u32(f, 44) != 0:
                    raise ValueError("invalid V3 footer CRC")
                count, logical, off, length = struct.unpack_from(">4Q", f, 8)
                pages = (count + 1023) // 1024
                if off < 48 or length != count * 28 + pages * 4 or off + length + pages * 8 + 4 + 48 != size:
                    raise ValueError("invalid V3 table bounds")
                # CDC count is chunks, NOT history records.
                out.update(chunks=count, logical_bytes=logical)
    elif magic == b"GTHREF01":
        need(128)
        v = u32(b, 8)
        out.update(format="history-reference", container_version=v)
        if v == 1:
            logical, physical, cd, chunks, sd, spans = struct.unpack_from(">6Q", b, 16)
            if any(b[12:16]) or any(b[96:128]) or physical != size or cd < 128 or cd + chunks * 64 != sd or sd + spans * 32 != size:
                raise ValueError("invalid R1 header bounds")
            out.update(logical_bytes=logical, chunks=chunks, spans=spans,
                       directory_bytes=chunks * 64 + spans * 32,
                       stored_chunk_region_bytes=cd - 128)
    elif magic in (b"gtsdcseg", b"gtsdcidx", b"gtsdckv1"):
        need(36)
        v = u32(b, 8)
        out.update(format=magic.decode(), inner_version=v)
        if v in range(1, 8):
            out.update(rows=u64(b, 28), row_unit="records")
            if magic == b"gtsdcseg":
                out["logical_bytes"] = size
            if magic == b"gtsdckv1" and v in (6, 7):
                need(112)
                out.update(key_instances=u64(b, 40))
                if v == 7:
                    need(120)
                    out["posting_bytes"] = u64(b, 104)
    elif magic in (b"gtlatseg", b"gtlatidx", b"gtlatbt1"):
        need(48)
        v = u32(b, 8)
        out.update(format=magic.decode(), inner_version=v)
        if v == 1:
            out.update(rows=u64(b, 40), row_unit="entries",
                       flags=struct.unpack_from(">H", b, 22)[0])
            # Latest values can be individually compressed: size is NOT logical.
    elif magic in (b"gtfrzr1\n", b"gtcfax1\n", b"gtcidx1\n",
                   b"gtbtrc1\n", b"gtsblm1\n"):
        need(32)
        units = {b"gtfrzr1\n": "blocks", b"gtcfax1\n": "entries",
                 b"gtcidx1\n": "blocks", b"gtbtrc1\n": "blocks",
                 b"gtsblm1\n": "bloom_rows"}
        out.update(format=magic.decode().strip(), inner_version=1,
                   rows=u64(b, 24), row_unit=units.get(magic))
    elif magic in (b"gtevlg1\n", b"gtevlg2\n", b"gtevlg3\n", b"gtevlg4\n"):
        out.update(format="event-log", inner_version=int(chr(magic[6])))
        # The count prefix is identical in V1..V4; no frame directories read.
        need(32)
        out.update(rows=u64(b, 24), row_unit="logs")
    elif magic == b"gtevlx2\n":
        out.update(format="event-log-index", inner_version=2)
    return out


def census(root):
    started = now()
    rootfd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        raw, mi = manifest_read(rootfd)
        m = json.loads(raw)
        refs = {}
        duplicates = []
        for membership, field in (("active", "segments"), ("retired", "retired")):
            for ref in m.get(field, []) or []:
                p = ref["path"]
                if p in refs:
                    duplicates.append({"path": p, "membership": membership, "ref": ref})
                else:
                    refs[p] = (membership, ref)
        files = []
        for p, (membership, ref) in sorted(refs.items()):
            item = {"path": p, "membership": membership, "dataset": ref.get("dataset") or ("kv-latest" if ref.get("kind") == "latest" else "unknown"),
                    "domain": ref.get("domain", 0), "kind": ref.get("kind", "unknown"),
                    "range_unit": "blocks" if ref.get("kind") in BLOCK_KINDS else "transactions",
                    "from": ref.get("fromTxNum"), "to": ref.get("toTxNum"),
                    "declared_bytes": ref.get("size"), "status": "unread", "header": None}
            fd = None
            try:
                fd = open_under(rootfd, p)
                a = os.fstat(fd)
                if not stat.S_ISREG(a.st_mode):
                    raise ValueError("not a regular file")
                item["identity_before"] = identity(a)
                try:
                    item["header"] = header_info(fd, a.st_size)
                    item["status"] = "observed"
                except (ValueError, struct.error) as e:
                    item.update(status="header_error", error=str(e))
                item["identity_after"] = identity(os.fstat(fd))
                # Reopen the path to detect replacement/unlink while old fd lives.
                check = open_under(rootfd, p)
                try:
                    item["path_identity_after"] = identity(os.fstat(check))
                finally:
                    os.close(check)
                if item["identity_before"] != item["identity_after"] or item["identity_after"] != item["path_identity_after"]:
                    item["status"] = "changed"
                item["declared_size_matches"] = ref.get("size") == a.st_size
            except (OSError, ValueError) as e:
                item.update(status="missing_or_changed" if isinstance(e, FileNotFoundError) else "read_error", error=str(e))
            finally:
                if fd is not None:
                    os.close(fd)
            files.append(item)

        # Walk via directory fds; never follow directory/file symlinks.
        unknown = []
        walk_errors = []
        def walk(dfd, prefix=""):
            try:
                names = sorted(os.listdir(dfd))
            except OSError as e:
                walk_errors.append({"path": prefix, "error": str(e)})
                return
            for name in names:
                p = prefix + name
                try:
                    s = os.stat(name, dir_fd=dfd, follow_symlinks=False)
                    if stat.S_ISDIR(s.st_mode):
                        child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=dfd)
                        try:
                            walk(child, p + "/")
                        finally:
                            os.close(child)
                    elif p not in refs:
                        unknown.append({"path": p, "type": "regular" if stat.S_ISREG(s.st_mode) else "symlink_or_other", "identity": identity(s)})
                except OSError as e:
                    walk_errors.append({"path": p, "error": str(e)})
        walk(rootfd)

        groups = {}
        for item in files:
            h = item["header"] or {}
            key = (item["membership"], item["dataset"], str(item["domain"]), item["kind"], h.get("format", "unknown"), h.get("container_version"), h.get("inner_version"), item["status"])
            if key not in groups:
                groups[key] = dict(zip(("membership", "dataset", "domain", "kind", "format", "container_version", "inner_version", "status"), key))
                groups[key].update(files=0, declared_bytes=0, actual_bytes=0, allocated_bytes=0, logical_bytes_known_sum=0, logical_unknown_files=0, rows_known_sum=0, rows_unknown_files=0, row_units=[], physical_unknown_files=0)
            g = groups[key]
            g["files"] += 1
            g["declared_bytes"] += item["declared_bytes"] or 0
            s = item.get("identity_before")
            if s:
                g["actual_bytes"] += s["size"]
                g["allocated_bytes"] += s["allocated_bytes"]
            else:
                g["physical_unknown_files"] += 1
            for source, total, missing in (("logical_bytes", "logical_bytes_known_sum", "logical_unknown_files"), ("rows", "rows_known_sum", "rows_unknown_files")):
                if h.get(source) is None or item["status"] != "observed":
                    g[missing] += 1
                else:
                    g[total] += h[source]
            if h.get("row_unit") and h["row_unit"] not in g["row_units"]:
                g["row_units"].append(h["row_unit"])
        end = {}
        try:
            eraw, ei = manifest_read(rootfd)
            end = {"sha256": hashlib.sha256(eraw).hexdigest(), "identity": ei, "same_bytes": eraw == raw}
        except (OSError, ValueError) as e:
            end = {"same_bytes": False, "error": str(e)}
        return {"schema": "cold-physical-census-v1", "started_utc": started, "finished_utc": now(), "directory": root,
                "manifest": {"sha256": hashlib.sha256(raw).hexdigest(), "generation": m.get("generation"), "identity": mi}, "manifest_end": end,
                "limits": {"header_read_bytes_per_file_max": 176, "manifest_bytes_max": MANIFEST_LIMIT, "payload_decompression": False, "payload_authentication": False},
                "notes": ["Header claims only; not a manifest/file integrity verifier or atomic snapshot.", "Path totals may double-count hardlinks; identity includes device/inode/nlink for deduplication.", "Rows across kinds are not additive; accessor/index rows can describe the same records.", "Unknown/unreferenced files are not deletion candidates. Logical unknown is not zero.", "Changed/missing files may reflect concurrent publication or retirement."],
                "groups": list(groups.values()), "status_counts": dict(collections.Counter(f["status"] for f in files)), "files": files, "duplicate_refs": duplicates,
                "unreferenced": unknown, "unreferenced_actual_bytes": sum(f["identity"]["size"] for f in unknown if f["type"] == "regular"),
                "unreferenced_allocated_bytes": sum(f["identity"]["allocated_bytes"] for f in unknown if f["type"] == "regular"), "inventory_errors": walk_errors}
    finally:
        os.close(rootfd)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("directory")
    p.add_argument("output")
    args = p.parse_args()
    root = os.path.realpath(args.directory)
    output = os.path.join(os.path.realpath(os.path.dirname(os.path.abspath(args.output))), os.path.basename(args.output))
    if os.path.commonpath((root, output)) == root:
        p.error("output must be outside the source snapshot directory")
    if os.path.lexists(output):
        p.error("output already exists")
    result = census(root)
    # Exclusive output creation: never replace an existing evidence file.
    fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(result, f, indent=2, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    print(json.dumps({"output": output, "manifest": result["manifest"]["sha256"], "manifest_unchanged": result["manifest_end"]["same_bytes"], "status_counts": result["status_counts"]}))


if __name__ == "__main__":
    main()

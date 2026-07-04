#!/usr/bin/env python3
"""Capture historical archive state fixtures from a JSON-RPC endpoint."""

import argparse
import json
import shlex
import sys
import urllib.error
import urllib.request


def parse_block(value):
    try:
        if value.lower().startswith("0x"):
            block = int(value, 16)
        else:
            block = int(value, 10)
    except (AttributeError, ValueError):
        raise argparse.ArgumentTypeError("block must be a non-negative integer or 0x quantity")
    if block < 0:
        raise argparse.ArgumentTypeError("block must be non-negative")
    return block


def is_hex_string(value, *, allow_empty=True):
    if not isinstance(value, str) or not value.startswith("0x"):
        return False
    if not allow_empty and value == "0x":
        return False
    return all(ch in "0123456789abcdefABCDEF" for ch in value[2:])


def jsonrpc_call(endpoint, method, params, request_id, timeout):
    payload = json.dumps(
        {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
        separators=(",", ":"),
    ).encode("utf-8")
    request = urllib.request.Request(
        endpoint,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
    except urllib.error.URLError as exc:
        raise RuntimeError(f"{method} request failed: {exc}") from exc
    try:
        data = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"{method} returned invalid JSON") from exc
    if data.get("error") is not None:
        raise RuntimeError(f"{method} returned JSON-RPC error: {data.get('error')!r}")
    if "result" not in data:
        raise RuntimeError(f"{method} response missing result")
    return data["result"]


def capture_fixtures(endpoint, block, address, storage_slot, timeout):
    block_tag = hex(block)
    calls = (
        ("archiveApiExpectedBalance", "eth_getBalance", [address, block_tag], False),
        ("archiveApiExpectedCode", "eth_getCode", [address, block_tag], True),
        ("archiveApiExpectedStorage", "eth_getStorageAt", [address, storage_slot, block_tag], False),
    )
    values = {}
    for request_id, (field, method, params, allow_empty) in enumerate(calls, start=1):
        result = jsonrpc_call(endpoint, method, params, request_id, timeout)
        if not is_hex_string(result, allow_empty=allow_empty):
            kind = "0x hex string" if allow_empty else "non-empty 0x hex quantity"
            raise RuntimeError(f"{method} result {result!r} is not a {kind}")
        values[field] = result
    return {
        "archiveApiBlock": block,
        "archiveApiBlockTag": block_tag,
        "archiveApiAddress": address,
        "archiveApiStorageSlot": storage_slot,
        **values,
    }


def render_args(row):
    pairs = (
        ("--archive-api-block", str(row["archiveApiBlock"])),
        ("--archive-api-address", row["archiveApiAddress"]),
        ("--archive-api-storage-slot", row["archiveApiStorageSlot"]),
        ("--archive-api-expected-balance", row["archiveApiExpectedBalance"]),
        ("--archive-api-expected-code", row["archiveApiExpectedCode"]),
        ("--archive-api-expected-storage", row["archiveApiExpectedStorage"]),
    )
    return " ".join(
        " ".join((shlex.quote(flag), shlex.quote(value)))
        for flag, value in pairs
    )


def build_parser():
    parser = argparse.ArgumentParser(
        description=(
            "Capture eth_getBalance/eth_getCode/eth_getStorageAt fixtures for "
            "post-prune archive API validation."
        ),
    )
    parser.add_argument("--jsonrpc", required=True, help="JSON-RPC endpoint to query")
    parser.add_argument("--block", required=True, type=parse_block, help="historical block number")
    parser.add_argument("--address", required=True, help="archive API account/contract address")
    parser.add_argument("--storage-slot", default="0x0", help="storage slot for eth_getStorageAt")
    parser.add_argument("--timeout", type=float, default=5.0, help="per-request timeout in seconds")
    parser.add_argument(
        "--format",
        choices=("json", "args"),
        default="json",
        help="output JSON or shell-safe sampler/benchmark CLI arguments",
    )
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if not is_hex_string(args.address, allow_empty=False):
        parser.error("--address must be a non-empty 0x hex string")
    if not is_hex_string(args.storage_slot, allow_empty=False):
        parser.error("--storage-slot must be a non-empty 0x hex string")
    try:
        row = capture_fixtures(
            args.jsonrpc,
            args.block,
            args.address,
            args.storage_slot,
            args.timeout,
        )
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if args.format == "args":
        print(render_args(row))
    else:
        print(json.dumps(row, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

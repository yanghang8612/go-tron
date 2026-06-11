#!/usr/bin/env python3
"""Audit the Nile 19,716,962 sync-stop incident against a gtron node.

The script is intentionally narrow and forensic: it verifies the exact block and
receipt facts that explain the 840-SUN shortfall seen on 2026-06-11. Use it after
rebuilding a Nile node from a clean DB or trusted pre-divergence snapshot.

Defaults match the incident environment:

  SOCKS5_PROXY=127.0.0.1:1088 scripts/dev/nile_19716962_audit.py

Override endpoints with --gtron-url and --nile-url when needed.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from typing import Any


FAIL_BLOCK = 19_716_962
FAIL_TX = "e78c527291df957205a90512a1e6b336c9cfadbe9f1698af2d7c013e65bac4c1"
FAIL_BLOCK_ID = "00000000012cdb62fa2b5148fcb783d15d4b18e1f68572261ce0ce50053cdd35"

HISTORICAL_TXS = [
    (
        "first owner overcharge",
        "07674adbbaacf92e95e64d6959e7333abf740c9ac2a9bf87a4ec8f41f3a9e0e6",
        {
            "blockNumber": 19_555_385,
            "fee": 15_982_920,
            "receipt.energy_fee": 14_962_920,
            "receipt.origin_energy_usage": 140_632,
            "receipt.energy_usage_total": 176_258,
        },
    ),
    (
        "second owner overcharge",
        "26238015137427dc240841d024a0ada8a2f53d286d6ab4fb3c275e52985ae45c",
        {
            "blockNumber": 19_713_603,
            "fee": 73_778_460,
            "receipt.energy_fee": 72_854_460,
            "receipt.origin_energy_usage": 13_201,
            "receipt.energy_usage_total": 186_664,
        },
    ),
]


@dataclass
class RpcResult:
    status: int
    data: Any | None
    body: str


def post_json(base_url: str, method: str, payload: dict[str, Any], socks5: str, timeout: int) -> RpcResult:
    url = f"{base_url.rstrip('/')}/{method}"
    cmd = ["curl", "-sS", "--max-time", str(timeout), "-w", "\n%{http_code}"]
    if socks5:
        cmd.extend(["--socks5-hostname", socks5])
    cmd.extend(["-X", "POST", url, "-H", "Content-Type: application/json", "-d", json.dumps(payload)])

    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"curl failed for {url}: {proc.stderr.strip()}")
    body, _, status_text = proc.stdout.rpartition("\n")
    try:
        status = int(status_text)
    except ValueError as exc:
        raise RuntimeError(f"curl did not return an HTTP status for {url}: {proc.stdout!r}") from exc

    data = None
    if body:
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            data = None
    return RpcResult(status=status, data=data, body=body)


def get_path(data: Any, path: str) -> Any:
    cur = data
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


class Audit:
    def __init__(self) -> None:
        self.failures: list[str] = []

    def check(self, condition: bool, ok: str, fail: str) -> None:
        if condition:
            print(f"PASS {ok}")
        else:
            print(f"FAIL {fail}")
            self.failures.append(fail)

    def compare_field(self, label: str, field: str, got: Any, want: Any) -> None:
        self.check(
            got == want,
            f"{label} {field}={want}",
            f"{label} {field}: gtron={got!r}, want={want!r}",
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gtron-url", default=os.environ.get("GTRON_URL", "http://3.12.206.71:6060/gn"))
    parser.add_argument("--nile-url", default=os.environ.get("NILE_URL", "https://nile.trongrid.io"))
    parser.add_argument("--socks5", default=os.environ.get("SOCKS5_PROXY", "127.0.0.1:1088"))
    parser.add_argument("--nile-socks5", default=os.environ.get("NILE_SOCKS5_PROXY", ""))
    parser.add_argument("--timeout", type=int, default=int(os.environ.get("AUDIT_TIMEOUT", "20")))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    audit = Audit()

    print(f"gtron: {args.gtron_url} via SOCKS5 {args.socks5 or '<none>'}")
    print(f"nile:  {args.nile_url} via SOCKS5 {args.nile_socks5 or '<none>'}")

    node = post_json(args.gtron_url, "wallet/getnodeinfo", {}, args.socks5, args.timeout)
    current = get_path(node.data, "currentBlock")
    audit.check(
        isinstance(current, int) and current >= FAIL_BLOCK,
        f"gtron currentBlock {current} >= {FAIL_BLOCK}",
        f"gtron currentBlock {current!r} is before {FAIL_BLOCK}; node is still stopped before the failing block",
    )

    public_block = post_json(args.nile_url, "wallet/getblockbynum", {"num": FAIL_BLOCK}, args.nile_socks5, args.timeout)
    gtron_block = post_json(args.gtron_url, "wallet/getblockbynum", {"num": FAIL_BLOCK}, args.socks5, args.timeout)
    audit.compare_field("public Nile block", "blockID", get_path(public_block.data, "blockID"), FAIL_BLOCK_ID)
    audit.compare_field("gtron block", "blockID", get_path(gtron_block.data, "blockID"), FAIL_BLOCK_ID)

    public_fail = post_json(args.nile_url, "wallet/gettransactioninfobyid", {"value": FAIL_TX}, args.nile_socks5, args.timeout)
    gtron_fail = post_json(args.gtron_url, "wallet/gettransactioninfobyid", {"value": FAIL_TX}, args.socks5, args.timeout)
    for field, want in {
        "blockNumber": FAIL_BLOCK,
        "receipt.net_usage": 269,
    }.items():
        audit.compare_field("public failing tx", field, get_path(public_fail.data, field), want)
        audit.compare_field("gtron failing tx", field, get_path(gtron_fail.data, field), want)
    audit.check(
        get_path(gtron_fail.data, "fee") in (None, 0),
        "gtron failing tx has no fee",
        f"gtron failing tx fee={get_path(gtron_fail.data, 'fee')!r}, want absent/0",
    )

    for label, txid, expected in HISTORICAL_TXS:
        public = post_json(args.nile_url, "wallet/gettransactioninfobyid", {"value": txid}, args.nile_socks5, args.timeout)
        gtron = post_json(args.gtron_url, "wallet/gettransactioninfobyid", {"value": txid}, args.socks5, args.timeout)
        for field, want in expected.items():
            public_value = get_path(public.data, field)
            audit.compare_field(f"public {label}", field, public_value, want)
            audit.compare_field(f"gtron {label}", field, get_path(gtron.data, field), public_value)

    if audit.failures:
        print()
        print(f"FAILED {len(audit.failures)} check(s). Rebuild from a clean DB or trusted pre-divergence snapshot, then rerun.")
        return 1
    print()
    print("OK: gtron matches public Nile for the 19,716,962 incident checks.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

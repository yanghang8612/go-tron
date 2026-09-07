#!/usr/bin/env python3
"""Read-only mainnet balance history canary; run on the node after startup.

Only whitelisted JSON-RPC reads and loopback metrics. No DB/service access.
discover writes a baseline to stdout; verify never updates that baseline.
"""

import argparse
import datetime
import http.client
import json
from pathlib import Path
import re
import sys
import time


GENESIS = "0x00000000000000001ebf88508a03865c71d452e25f4d51194196a1d22b6653dc"
ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}\Z")
HASH = re.compile(r"0x[0-9a-fA-F]{64}\Z")
SUN_SCALE = 10**12
MAX_BYTES = 16 * 1024 * 1024
ALLOWED = {"eth_blockNumber", "eth_getBlockByNumber", "eth_getBalance"}


def quantity(value):
    if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]+", value):
        raise ValueError("invalid nonnegative RPC quantity")
    return int(value, 16)


class Client:
    def __init__(self, rpc_port, metrics_port):
        self.rpc_port, self.metrics_port = rpc_port, metrics_port
        self.calls, self.bytes = 0, 0
        self.deadline = time.monotonic() + 150

    def request(self, port, path, body=None):
        self.calls += 1
        remaining = self.deadline - time.monotonic()
        if self.calls > 128 or remaining <= 0:
            raise ValueError("request/time budget reached; no acceptance result")
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=min(5, remaining))
        try:
            connection.request("POST" if body is not None else "GET", path, body=body,
                               headers={"Content-Type": "application/json"} if body else {})
            response = connection.getresponse()
            if response.status != 200:
                raise ValueError("HTTP status " + str(response.status))
            cap = min(4 * 1024 * 1024, MAX_BYTES - self.bytes)
            raw = response.read(cap + 1)
            if len(raw) > cap:
                raise ValueError("response/total byte cap reached")
            self.bytes += len(raw)
            return raw.decode("utf-8")
        finally:
            connection.close()

    def rpc(self, method, params):
        if method not in ALLOWED:
            raise ValueError("method is not a whitelisted read")
        request_id = self.calls + 1
        body = json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
        result = json.loads(self.request(self.rpc_port, "/", body))
        if result.get("id") != request_id or result.get("error") is not None or "result" not in result:
            raise ValueError("RPC failure: " + str(result))
        return result["result"]

    def frontiers(self):
        values = {}
        for line in self.request(self.metrics_port, "/metrics").splitlines():
            fields = line.split()
            if len(fields) == 2:
                values[fields[0]] = fields[1]
        names = {"solidified": "chain_solidified_block",
                 "published": "state_snapshot_cold_last_published_block",
                 "pruned": "state_prune_last_domain_change_pruned_through_block"}
        result = {key: int(values[name]) for key, name in names.items()}
        result["head"] = quantity(self.rpc("eth_blockNumber", []))
        return result

    def block(self, number, full=False):
        block = self.rpc("eth_getBlockByNumber", [hex(number), full])
        if (not isinstance(block, dict) or quantity(block.get("number")) != number
                or not HASH.fullmatch(str(block.get("hash")))):
            raise ValueError("block missing or malformed: " + str(number))
        return block


def balance_series(client, address, b, c):
    numbers = sorted({b - 1, b, c - 1, c})
    hashes = {str(n): client.block(n)["hash"].lower() for n in numbers}
    balances = {}
    for n in numbers:
        raw = client.rpc("eth_getBalance", [address, hex(n)])
        amount = quantity(raw)
        if amount % SUN_SCALE:
            raise ValueError("balance does not obey gtron SUN x 1e12 API scale")
        balances[str(n)] = {"rpc_quantity": hex(amount), "sun": amount // SUN_SCALE}
    # Numeric tags are supported, EIP-1898 hash objects are not assumed. Pin
    # canonical hashes before and after the scalar reads, at solidified blocks.
    for n in numbers:
        if client.block(n)["hash"].lower() != hashes[str(n)]:
            raise ValueError("canonical hash changed during capture")
    return {"hashes": hashes, "balances": balances}


def useful(series, b, c):
    amounts = {int(n): row["sun"] for n, row in series["balances"].items()}
    return (amounts[b] > 0 and amounts[b - 1] != amounts[b]
            and amounts[c - 1] != amounts[c] and amounts[b] != amounts[c])


def discover(client, start, count, stride):
    frontier = client.frontiers()
    sightings, examined = {}, []
    for n in range(start, start + count * stride, stride):
        if n >= frontier["head"] or n > frontier["solidified"]:
            break
        block = client.block(n, True)
        examined.append(n)
        transactions = block.get("transactions", [])
        if len(transactions) > 10000:
            raise ValueError("block transaction count cap reached")
        for tx in transactions:
            # A discovery hint, not proof of a successful TransferContract.
            # The subsequent four historical balances prove actual changes.
            if tx.get("input") != "0x" or quantity(tx.get("value", "0x0")) <= 0:
                continue
            for side in ("from", "to"):
                address = tx.get(side, "")
                if not ADDRESS.fullmatch(address) or int(address[2:], 16) == 0:
                    continue
                address = address.lower()
                if address not in sightings and len(sightings) >= 2048:
                    raise ValueError("candidate identity cap reached")
                occurrences = sightings.setdefault(address, [])
                if not occurrences or occurrences[-1]["block"] != n:
                    occurrences.append({"block": n, "tx_hash": tx.get("hash"), "side": side})
    attempts = 0
    for address, occurrences in sightings.items():
        if len(occurrences) < 2:
            continue
        b, c = occurrences[0]["block"], occurrences[1]["block"]
        if frontier["pruned"] >= b - 1:
            continue
        attempts += 1
        series = balance_series(client, address, b, c)
        if useful(series, b, c):
            after = client.frontiers()
            if after["pruned"] >= b - 1:
                raise ValueError("selected range was pruned during baseline capture")
            return {"schema": 1, "kind": "pre_prune_baseline", "genesis_hash": GENESIS,
                    "address": address, "b": b, "c": c, "series": series,
                    "before": frontier, "after": after, "examined_blocks": examined,
                    "candidate_attempts": attempts, "discovery_hints": occurrences[:2]}
        if attempts >= 4:
            break
    raise ValueError("no proven two-change candidate in bounded sample; examined=" + str(examined)
                     + "; attempts=" + str(attempts) + "; no automatic wider scan")


def verify(client, baseline):
    if baseline.get("schema") != 1 or baseline.get("kind") != "pre_prune_baseline" or baseline.get("genesis_hash") != GENESIS:
        raise ValueError("not a recognized mainnet pre-prune baseline")
    address, b, c = baseline["address"], baseline["b"], baseline["c"]
    if not ADDRESS.fullmatch(address) or not 1 < b < c <= 10_000_000:
        raise ValueError("invalid baseline identity/range")
    if not useful(baseline["series"], b, c):
        raise ValueError("baseline does not prove two balance changes")
    for point in (baseline["before"], baseline["after"]):
        if point["pruned"] >= b - 1 or point["solidified"] < c or point["head"] <= c:
            raise ValueError("baseline is not a pre-prune solidified observation")
    before = client.frontiers()
    if before["head"] <= c or min(before["solidified"], before["published"], before["pruned"]) < c:
        raise ValueError("not ready: C must be below head and at/below solidified, cold published, hot pruned")
    actual = balance_series(client, address, b, c)
    if actual != baseline["series"]:
        raise ValueError("fixed-hash historical balance mismatch; preserve baseline and investigate")
    return {"schema": 1, "kind": "post_prune_balance_canary_pass", "address": address,
            "b": b, "c": c, "series": actual, "frontiers": before,
            "scope": "one account scalar balance; not all history domains or whole-database validation"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("discover", "verify"))
    parser.add_argument("--rpc-port", type=int, default=8545)
    parser.add_argument("--metrics-port", type=int, default=6071)
    parser.add_argument("--start", type=int, default=2)
    parser.add_argument("--count", type=int, default=32)
    parser.add_argument("--stride", type=int, default=32)
    parser.add_argument("--baseline")
    args = parser.parse_args()
    if (not 1 <= args.rpc_port <= 65535 or not 1 <= args.metrics_port <= 65535
            or not 2 <= args.start <= 100000 or not 1 <= args.count <= 32 or not 1 <= args.stride <= 256):
        parser.error("invalid endpoint or bounded sample parameters")
    client = Client(args.rpc_port, args.metrics_port)
    if client.block(0)["hash"].lower() != GENESIS:
        raise ValueError("unexpected mainnet genesis hash")
    if args.mode == "discover":
        result = discover(client, args.start, args.count, args.stride)
    else:
        if not args.baseline:
            parser.error("verify requires --baseline")
        with Path(args.baseline).open("rb") as stream:
            raw = stream.read(65537)
        if len(raw) > 65536:
            raise ValueError("baseline file cap reached")
        result = verify(client, json.loads(raw))
    result.update({"utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                   "requests": client.calls, "response_bytes": client.bytes})
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError, http.client.HTTPException) as error:
        print(json.dumps({"status": "not_accepted", "error": str(error), "node_action": "none"}), file=sys.stderr)
        sys.exit(1)

# Custom-genesis loader for gtron — design

**Date:** 2026-05-02
**Goal:** Let `gtron` join an arbitrary external TRON chain (e.g. a local
java-tron private chain) by loading its genesis from a file on disk.

## Why now

Wire-level P2P interop with java-tron is verified
(`docs/dev/p2p-interop-status.md`): handshake, discovery, Hello,
SyncBlockChain, BLOCK_CHAIN_INVENTORY, and FETCH_INV_DATA → BLOCK all
work cross-implementation. The remaining gap to full cross-impl sync is
that `gtron init` only knows mainnet and Nile genesis; it cannot match a
private-chain peer's genesis hash, so any block past #0 is rejected as a
fork.

This is the same gap that blocks `scripts/system_test_cross.sh`
(`TODO.md` §6).

## Source of truth

java-tron's `config.conf` defines the genesis under a `genesis.block { ... }`
HOCON object. The local node we're targeting has:

```hocon
genesis.block = {
  assets = [
    { accountName = "Zion",      accountType = "AssetIssue",
      address = "TMVQGm1qAQYVdetCeGRRkTWYYrLXuHK2HC", balance = "99000000000000000" },
    { accountName = "Blackhole", accountType = "AssetIssue",
      address = "TLsV52sRDL79HXGGm9yzwKibb6BeruhUzy", balance = "-9223372036854775808" },
  ]
  witnesses = [
    { address: TMVQGm1qAQYVdetCeGRRkTWYYrLXuHK2HC, url = "http://test.io", voteCount = 100 },
  ]
  timestamp = "0"
  parentHash = "0xe58f33f9baf9305dc6f82b9f1934ea8f0ade2defb951258d50167028c780351f"
}
```

Plus `p2p.version = 0` (used as `NetworkID` in the libp2p Hello).

## Approach

A two-layer split keeps HOCON parsing out of the critical path:

1. **gtron-side:** add a stable JSON schema and `--genesis <file>` CLI flag.
2. **Operator-side:** a tiny converter (sub-command) reads the bounded
   `genesis.block { ... }` block out of a HOCON `config.conf` and writes
   the JSON. Hand-rolled, no third-party HOCON dep.

### Prerequisite: genesis-block structural parity

A live probe (`p2p.TestProbeJavaTronGenesis`, 2026-05-02) confirms that
java-tron's genesis block diverges from gtron's `core.GenesisToBlock`
in fields that are part of the SHA-256 input:

| `BlockHeaderRaw` field | gtron today | java-tron |
|---|---|---|
| `accountStateRoot` | StateDB.Commit() root | **empty** |
| `txTrieRoot` | empty | MerkleRoot of genesis txs |
| genesis transactions | 0 | one `TransferContract` per asset entry |
| `witness_address` | empty | famous-quote bytes (`"A new system..."`) |

Java-tron's algorithm (`BlockUtil.newGenesisBlockCapsule`,
`BlockCapsule.setMerkleRoot`, `MerkleTree.createTree`,
`TransactionUtil.newGenesisTransaction`):

- Each `assets[i]` becomes a `Transaction { rawData.contract: [{ type:
  TransferContract, parameter: TransferContract{ amount:
  assets[i].balance, ownerAddress: "0x000000000000000000000".getBytes(),
  toAddress: assets[i].address } }] }`. No signature, no `ret`. The
  `ownerAddress` literal is the **ASCII bytes of "0x000000000000000000000"**
  (21 chars), not a zeroed 21-byte address — this is a java-tron quirk.
- Leaf hash for each tx: `SHA256(Transaction.toByteArray())` (full proto
  bytes, including the empty `signature` and `ret` lists).
- Merkle pairing: pairs `(i, i+1)`; if odd count, the lone leaf
  propagates upward unchanged (no doubling). Parent =
  `SHA256(left.bytes || right.bytes)` else parent = left if right is nil.
- `witness_address = "A new system must allow existing systems to be
  linked together without requiring any central control or coordination"
  .getBytes()` (98 ASCII bytes). Set via `BlockCapsule.setWitness(String)`.
- `accountStateRoot` and `witness_signature` left empty/unset.

Until `GenesisToBlock` matches this exactly, **no `--genesis` work
unblocks anything** because every cross-impl genesis hash check fails.
Fix it first, then add the file loader.

### JSON schema

```json
{
  "chain_id": 9999,
  "p2p_version": 0,
  "timestamp_ms": 0,
  "parent_hash": "0xe58f33f9baf9305dc6f82b9f1934ea8f0ade2defb951258d50167028c780351f",
  "accounts": [
    { "address": "TMVQGm1qAQYVdetCeGRRkTWYYrLXuHK2HC", "balance": "99000000000000000", "name": "Zion" },
    { "address": "TLsV52sRDL79HXGGm9yzwKibb6BeruhUzy", "balance": "-9223372036854775808", "name": "Blackhole" }
  ],
  "witnesses": [
    { "address": "TMVQGm1qAQYVdetCeGRRkTWYYrLXuHK2HC", "vote_count": 100, "url": "http://test.io" }
  ],
  "dynamic_properties": { "maintenance_time_interval": 21600000 }
}
```

- `address` accepts either Base58Check (`T…`) or hex (`41…`) — decided by
  prefix. `crypto.Base58ToAddress` already exists.
- `balance` is a string to fit `int64.Min` (`-9223372036854775808`)
  cleanly across JSON/JS clients without precision risk.
- `dynamic_properties` is optional; absent → falls back to a curated
  default set (same as `makeDevGenesis(fullFeatures=true)`'s baseline)
  so private chains run feature-complete.

### CLI surface

- New flag: `--genesis <file>` (string). Mutually exclusive with
  `--testnet` and `--dev`.
- `gtron init --genesis <file>` initialises the on-disk DB from the file.
- `gtron --genesis <file>` runs a node against that genesis.
- `--genesis` overrides the default `NetworkID`/`P2PVersion` from
  `chain_id` / `p2p_version` so the libp2p Hello matches the peer.
- `--seednode` continues to work alongside; for our test we'll point at
  `127.0.0.1:18888`.

### Genesis-hash compatibility (critical risk)

`core.GenesisToBlock` derives the genesis block hash from genesis
content. For `gtron` to sync from java-tron we need
**byte-identical genesis block hash** to the one java-tron computed.

Slice 1 therefore includes a **hash-parity sanity check** as the first
test: build the java-tron private-chain genesis via `--genesis
java-tron.json`, compute its hash, and compare against the
`getblockbyid` result for block #0 from the live java-tron HTTP API.
If this diverges, the rest of the slice is moot — the bug is in
`GenesisToBlock` field ordering / encoding, and that gets a separate
follow-up plan.

### Out of scope (this slice)

- HOCON parser. The hand-rolled extractor is its own follow-up
  (`gtron genesis from-hocon`) — the JSON file is enough to start a
  cross-impl sync today.
- Mid-chain reorg recovery if java-tron rewinds while gtron is following.
- Multi-version DP defaults — we ship one curated DP map; operators
  override per chain in the JSON.

## Verification

End-to-end success criterion: with java-tron running on
`127.0.0.1:18888` (block ≥ #69k), starting gtron with
`--genesis java-tron.json --seednode 127.0.0.1:18888` results in
gtron's `eth_blockNumber` advancing past 0 within 60s, and gtron's
block-#1 hash matching java-tron's `getblockbynum 1`.

Stretch: gtron catches up to within ~10 blocks of java-tron's head
within 5 minutes (≈ 13k blocks/s sync target is unrealistic; just
require monotonic progress).

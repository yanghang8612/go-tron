# Ancient transaction index

**Status:** Implemented for offline bootstrap and bounded online maintenance
**Date:** 2026-07-27
**Related:** [Chain freezer](./2026-05-19-chain-freezer-design.md),
[TransactionInfo deduplication](./2026-07-26-transaction-info-dedup-design.md),
[Ancient V2 compression](./2026-07-27-ancient-v2-compression-design.md)

## Evidence and problem

The mainnet inspection measured 745,824,140 live `tx-*` rows at height
13,761,515. Every row contains a three-byte key prefix, a uniformly random
32-byte transaction ID, and an eight-byte block locator (new cold runs also
retain a packed block/ordinal locator). The
logical lower bound is therefore 43 bytes per transaction (29.87 GiB at that
height), before Pebble block indexes, filters, and LSM write amplification.

The value is already compact. The remaining growth comes from retaining every
historical random hash in the mutable Pebble LSM. Those hashes do not compress
and historical locators are immutable once their blocks are solidified.

## Goals

- Keep the recent/reorg window in Pebble with the existing `tx-*` schema.
- Move locators for V2-covered historical blocks to an immutable flat index.
- Preserve exact Wallet and JSON-RPC lookup semantics, including misses.
- Bound lookup I/O without probing every 65,536-block V2 segment.
- Make migration resumable and delete Pebble rows only after durable publish.

## Layout direction

A block-segment-local index is not sufficient: a transaction hash does not
identify the block segment that contains it. Looking through every V2 segment
would make a negative lookup O(number of segments).

The cold index is instead routed by the high bits of the transaction hash. A
single directory maps each hash prefix to a contiguous bucket in an immutable
file. The candidate production layout uses a 20-bit directory (1,048,577
eight-byte offsets, about 8 MiB) and stores two parallel arrays per bucket:

```text
directory[prefix] -> bucket byte range
bucket:
  16-byte row-count/checksum header
  sorted 64-bit fingerprints[]
  packed 64-bit locations[]
```

The fingerprint is the next 64 transaction-hash bits after the directory
prefix. At one billion entries the combined 84-bit routed identifier has an
expected collision-pair count far below one. Collisions are nevertheless
handled exactly: every equal fingerprint is a candidate, and the reader checks
the transaction at the candidate block/ordinal against the complete requested
32-byte hash. A negative query is returned only after all equal candidates fail
verification.

The reader fetches one bounded bucket, verifies independent CRC32C checksums for
its header, fingerprints, and locations, then binary-searches the fingerprint
array. The projected immutable payload is 16 bytes per transaction plus the
fixed directory, file header, and 16 bytes per non-empty bucket, about 63%
below the current 43-byte logical row. A 96-bit fingerprint variant (20 bytes
per transaction) remains a benchmark candidate.

## Read routing

`ReadTransactionIndex(hash)` performs:

1. Point-read the current `tx-*` row from Pebble.
2. On a miss, route the hash to one cold bucket using its high prefix bits.
3. Binary-search the fingerprint array and obtain candidate packed locations.
4. Read the candidate block transaction at the encoded ordinal and compare its
   full SHA-256 transaction ID with `hash`.
5. Return the block number only after full-hash verification.

`ReadTransactionInfo(hash)` uses the same verified location, then reads the
canonical `tx_infos` row. This prevents a truncated-fingerprint collision from
returning another transaction's receipt.

## Publication and migration

The first migration is offline. `gtron db migrate-tx-index` scans `tx-*`
rows in hash order and streams entries below V2 coverage into completed hash
buckets into a temporary immutable index, verifies and fsyncs it, and atomically
publishes a small manifest. Only then does a validation scan build one atomic
Pebble batch that range-deletes `tx-*` and reinserts the uncovered hot tail;
`--compact` optionally performs immediate physical reclamation. This avoids a
point tombstone and WAL record for every historical transaction.

If execution stops after the run rename but before manifest publication, the
next invocation verifies and publishes that complete unreferenced run. If it
stops during Pebble deletion, the manifest already makes every covered lookup
safe and the next invocation repeats the atomic replacement. Stored replay and
later historical resync both check cold coverage and do not repopulate migrated
`tx-*` rows.

Incremental online publication does not rewrite the entire historical index on
every 65,536-block V2 promotion. One segment's complete hashes and packed
ordinals are derived from canonical bodies, sorted in bounded memory, and
published as a new immutable run. Covered `tx-*` keys are point-deleted in
bounded batches only after the live ancient reader has installed that run. A
durable `FreezerTxIndexPrune` cursor advances after deletion, so a crash repeats
only the final segment.

Equal-block-span tail runs are merged geometrically, like carries in a binary
counter. The merge combines already-routed fingerprint/location buckets and
does not require the discarded full hashes; public lookup correctness remains
anchored by full-hash verification against the canonical body. Consequently N
online segments produce O(log N) open runs and lookup probes rather than N.

V2 promotion has a gate independent from synchronous Pebble compaction. During
bulk sync the latter remains disabled, while V2 may publish at most one segment
per freezer pass with a full freezer-interval cooldown after success. Each pass
also performs at most one transaction-index action (publish, prune, or merge),
which bounds CPU, memory, WAL, and I/O bursts while preventing unbounded V1 and
`tx-*` growth.

The manifest is the commit point. Unreferenced temporary files are ignored on
startup. A crash before Pebble deletion leaves duplicate readable indexes; a
crash after durable publication but during deletion still has the cold copy.

## Benchmark gate

Before fixing the on-disk version, `gtron db benchmark-tx-index` samples real
`tx-*` rows across the hash space and reports:

- current logical bytes and compact 64/96-bit projections;
- directory and average bucket sizes for selectable prefix widths;
- observed and expected truncated-identifier collisions;
- successful and unsuccessful in-memory lookup throughput.

The default candidate is accepted only if mainnet data confirms bounded bucket
sizes and materially lower lookup CPU than the ancient body/receipt read that
follows it.

## Compatibility and safety

- No consensus, protobuf, P2P, or public API format changes.
- Legacy and recent `tx-*` rows remain readable without migration.
- A compact-index hit is never trusted without checking the full transaction
  hash in the canonical body.
- Rewind continues deleting hot `tx-*` rows; cold runs cover only blocks below
  the solidified/freezer boundary and are never part of a normal reorg.
- Historical replay and the Erigon-aligned recoverable `TxLookup` stage skip
  blocks covered by the immutable index, so a rebuild cannot silently recreate
  the deleted historical `tx-*` keyspace. New blocks above coverage continue
  using the ordinary hot-tail layout.
- A legacy cold run with no prune cursor is adopted automatically only when a
  scan proves it has no covered hot rows. A large partially completed legacy
  run requires the atomic offline command instead of online point deletion.
- Older binaries are not supported after historical `tx-*` rows are deleted.

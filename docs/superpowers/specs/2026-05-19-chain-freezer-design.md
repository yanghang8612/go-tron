# Chain freezer (ancient store) — design

**Status:** Proposed
**Author:** yanghang8612
**Date:** 2026-05-19
**Inspiration:** [go-ethereum/core/rawdb/chain_freezer.go](../../../../ethereum/go-ethereum/core/rawdb/chain_freezer.go), [freezer_table.go](../../../../ethereum/go-ethereum/core/rawdb/freezer_table.go)
**Related plan:** [2026-05-19-chain-freezer.md](../plans/2026-05-19-chain-freezer.md)

## Background

gtron currently stores **every block ever applied** in Pebble:

- `b-<num>` → block proto
- `bh-<hash>` → block number
- `ti-<txid>` → tx info
- `tib-<num>` → tx infos by block
- `tx-<hash>` → tx index
- `bsr-<hash>` → block state root

For Nile (~10M blocks at session date) the chain DB is already ~80 GB;
mainnet (70M+ blocks) is multiple hundreds of GB. Pebble's LSM compaction
slows down super-linearly as the DB grows, and most of that data is **never
accessed after solidification** — only wallet APIs, block explorers, and the
occasional reorg need to read it.

go-ethereum solves this with a chain freezer:

> [`core/rawdb/chain_freezer.go`](../../../../ethereum/go-ethereum/core/rawdb/chain_freezer.go) +
> [`freezer_table.go`](../../../../ethereum/go-ethereum/core/rawdb/freezer_table.go)

A background goroutine batches finalized data out of the hot KV store into
append-only flat files (one table per "kind": headers, bodies, receipts,
hashes). Once frozen, the corresponding KV rows are deleted. Reads
transparently fall through `accessors_chain.go` to the freezer for blocks
older than the watermark. Mainnet datadir shrinks ~50%; compaction load
drops by ~10×.

This spec ports that pattern to gtron.

## Goals

- Move solidified block bodies + tx infos + block-by-number + block-by-hash
  out of Pebble into append-only flat files
- Keep `accessors_chain.go` / `accessors_indexes.go` callers' API unchanged
  (transparent fall-through)
- Reduce mainnet/Nile datadir by 40-60%
- Reduce Pebble compaction CPU by 5-10× (target the steady-state, not
  initial sync)
- Reads of frozen blocks remain correct, with bounded latency (sequential
  file Read at offset; ~ms range on modern SSDs)
- Operationally safe: freezing is incremental + idempotent + interruptible

## Non-goals

- Do NOT freeze state history (that's the separate
  [`state-history-index`](./2026-05-19-state-history-index-design.md) work).
- Do NOT freeze unsolidified data. The freeze watermark is strictly below
  `LatestSolidifiedBlockNum` to avoid mid-reorg races.
- Do NOT support online/dynamic re-thaw of frozen data. Truncating the
  freezer back to a block N is allowed (for testing / disaster recovery)
  but is not exposed as a runtime operation.
- Do NOT change KV key prefixes for non-frozen rows.
- Do NOT support era1 export in this slice (separate work; see "future
  follow-ups" below).

## Scope of "what gets frozen"

In scope (per block):

| Kind | Source KV key | Frozen as |
|---|---|---|
| header | (part of `b-<num>` block proto) | `headers.cdat` table, one row per block num |
| body | `b-<num>` (the tx list portion) | `bodies.cdat` |
| tx_info_list | `tib-<num>` | `tx_infos.cdat` |
| hash → num | `bh-<hash>` | `hashes.cdat` (keyed by num; reverse-lookup via Pebble for the immutable hash key kept hot) |
| state_root | `bsr-<hash>` | `state_roots.cdat` |

Out of scope for slice 1 (stays in Pebble):

- `bh-<hash>` reverse index (small; ~32 bytes × 70M = 2 GB acceptable)
- `tx-<hash>` tx-by-hash index (same reasoning — small entries, hot wallet
  lookup path)
- Per-tx `ti-<txid>` tx info index (eventually deserves its own table, but
  the slice 1 win is mostly in `b-` and `tib-`)
- State / accounts / contract storage / shielded entries
- Witness records, votes, proposals, exchanges

Slice 1 targets the biggest 90% of disk; tx-by-hash etc. are slice 2 candidates.

## File layout

```
<datadir>/ancient/
  chain/
    headers.cdat        # compressed data file (Snappy)
    headers.cidx        # 6-byte index entries: file_num(2) + offset(4)
    bodies.cdat
    bodies.cidx
    tx_infos.cdat
    tx_infos.cidx
    state_roots.cdat
    state_roots.cidx
  FLOCK                  # exclusive lock
  meta.json              # {schema_ver, frozen_max, frozen_min}
```

One `.cdat` is sharded across multiple files when it exceeds 2 GB
(geth's `freezerTableSize`). Each `.cidx` entry says `(file_num, offset)`;
length is the next entry's offset minus this one. Snappy compression per
row by default.

This is **exactly** geth's freezer table layout. We can lift
[`core/rawdb/freezer_table.go`](../../../../ethereum/go-ethereum/core/rawdb/freezer_table.go)
nearly verbatim (apache-2.0 / LGPL; gtron's go-ethereum dep already covers
the licence).

## Read fall-through

`core/rawdb/accessors_chain.go` is the chokepoint. The current shape:

```go
func ReadBlock(db ethdb.KeyValueReader, num uint64) *types.Block {
    data, err := db.Get(blockKey(num))
    if err != nil || len(data) == 0 {
        return nil
    }
    // decode
}
```

The new shape:

```go
func ReadBlock(db AncientReader, num uint64) *types.Block {
    if num < db.AncientCutoff() {
        return db.Ancient("blocks", num)
    }
    data, _ := db.Get(blockKey(num))
    // ...
}
```

Where `AncientReader` is a composite interface implemented by a new wrapper
`freezerdb.DB` that holds the Pebble store + the ancient store. The wrapper
is constructed once in `NewBlockChain`; all accessors take the wrapper
instead of bare `ethdb.KeyValueReader`.

Migrating call sites is mechanical: ~200 callers of `rawdb.ReadBlock /
ReadTransactionInfo* / ReadHeader`, all pass `bc.db`. We change
`bc.db`'s static type from `ethdb.KeyValueStore` to a new
`ChainDB` interface that embeds both KV + Ancient.

## Write path

`accessors_chain.go::WriteBlock` continues writing to Pebble. The freezer
goroutine is the only thing that moves data; the rest of the code never
writes ancient.

### Freezing goroutine

```
loop every freezeInterval (default 30 s):
  current  = bc.CurrentBlock().Number()
  solid    = LatestSolidifiedBlockNum()
  freezeTo = solid - freezeMarginBlocks   # default 128
  freezeFrom = ancient.LastFrozen() + 1

  if freezeTo <= freezeFrom:
    continue

  cap freezeTo at freezeFrom + freezeBatch  # default 30_000

  ancient.AppendRange(freezeFrom, freezeTo):
    for num in [freezeFrom, freezeTo]:
      header   = rawdb.ReadHeader(db, num)
      body     = rawdb.ReadBody(db, num)
      txinfos  = rawdb.ReadTxInfosByBlock(db, num)
      stateRt  = rawdb.ReadBlockStateRoot(db, hashFromNum(num))
      ancient.Append(headers, header)
      ancient.Append(bodies, body)
      ancient.Append(tx_infos, txinfos)
      ancient.Append(state_roots, stateRt)

  ancient.Sync()       # fsync all .cdat / .cidx files
  rawdb.DeleteBlockRange(db, freezeFrom, freezeTo)   # batch delete from Pebble
  bc.db.Compact(blockPrefix||freezeFrom, blockPrefix||freezeTo)
```

**Crash safety**: every batch first appends to ancient (with fsync), then
deletes from Pebble. If we crash mid-batch:
- ancient has rows we already wrote (idempotent on next pass — check
  `ancient.LastFrozen()`)
- Pebble may still have some of those rows (next freeze pass re-deletes,
  no-op)
- No data loss; worst case is small duplicate work

**Watermark choice**: `freezeMargin = 128 blocks (~6 min)` ensures we never
freeze past the solidified-block - reorg-window line. Configurable.

**Batch size**: 30K blocks per pass keeps the freezer goroutine from
holding `chainmu` (it doesn't — freezing uses snapshot reads) but caps
single-pass IO so we never starve other Pebble traffic.

## Pebble pressure modelling

Reads on the hot path go to Pebble; freezing removes rows; Pebble
compaction frees the freed range. Steady-state: Pebble keeps `freezeMargin`
+ `freezeBatch` worth of recent blocks (~30K + 128 = ~30K blocks
≈ a few hundred MB). Compaction operates on a small, hot working set.

The freezer's own IO is sequential append-only writes, which is the
cheapest possible filesystem workload. Modern NVMe sustains 2+ GB/s of
this; we'll never get close.

## Reorg / fork-rewind safety

The freezer never touches data above `solidified - freezeMargin`. Since
gtron's reorg horizon is bounded by KhaosDB's 1024-block window and PBFT
solidification finalizes well below that, the freezer can never need to
"unfreeze." If `switchFork` were ever to rewind past the freeze line
(catastrophic), the freezer must be truncated; this is a one-shot
disaster-recovery action via a CLI subcommand, not an automatic flow.

## Configuration

`gtron.toml`:

```toml
[freezer]
enabled         = true       # default true; set false on tiny dev chains
interval        = "30s"      # freezing pass cadence
margin_blocks   = 128        # don't freeze closer than this to solidified
batch_blocks    = 30000      # max per pass
table_size      = 2147483648 # 2 GiB per shard file
```

CLI override: `--freezer.disable`, `--freezer.interval=5m`, etc.

## API surface

Internal:

- `core/rawdb.AncientReader` / `AncientWriter` interfaces
- `core/rawdb.NewFreezer(path) → *Freezer`
- `core/rawdb.NewChainDB(kv, freezer) → ChainDB`
- `(db *ChainDB).AncientCutoff() uint64` — first block num still hot
- `(db *ChainDB).Ancient(kind, num) []byte`
- All `accessors_chain.go` functions migrated to take `ChainDB`
  (with adapter functions for unit tests that still use bare memdb)

External:

- New JSON-RPC endpoint `gtron_freezerStatus` → `{ frozen_min, frozen_max,
  table_sizes_bytes, last_pass_at, last_pass_duration }`
- Existing block-read endpoints unchanged in semantics (just transparently
  read from ancient when applicable)

## Migration

Existing datadirs have everything in Pebble. On first launch with freezer
enabled:

- Freezer initializes empty (`LastFrozen() = -1`)
- The freezing goroutine starts catching up. For a chain with 10M solidified
  blocks at ~30K/pass and ~30s interval, this is ~10M / 30K / (1 pass /
  30s) ≈ ~50 minutes of "catch-up" runtime
- During catch-up the node is fully functional — reads still hit Pebble
  for everything, the freezer just walks forward in the background

There is no destructive migration; the freezer is purely additive at the
read layer, and the destructive Pebble deletes only happen after a
successful ancient append.

## Acceptance criteria

- Nile datadir shrinks ≥ 40% after a 24h freeze catch-up
- p99 latency on `getBlockByNumber(old)` stays < 20 ms on SSD-backed
  freezer
- `accessors_chain` tests pass with both memdb (ancient absent) and
  freezerdb (ancient present)
- Freezer interruptions (kill -9 mid-pass) leave the DB consistent on
  restart — verified by a chaos test
- `make test` stays green; new tests cover slice 1 freezer mechanics
- Long-running Nile soak: Pebble compaction CPU goes from ~30% (today) to
  < 5% steady state

## Future follow-ups (not in this spec)

- **Era1 export format** — geth's `core/rawdb/eradb`. Standalone chain
  archive distribution. Defer until there's a use case (light-sync,
  block-explorer bulk data).
- **Freezing `ti-<txid>` and `tx-<hash>`** — adds another ~10-15% saving
  but complicates the wallet hot path. Slice 2 if needed.
- **Receipt/log filter integration** — when `filtermaps`-style log indexing
  arrives, the index will need to read frozen receipts. Both designs are
  compatible.

# State-change posting index: fresh-sync format

## Status and compatibility boundary

This is the only hot state-history index written by a new database. Deployment
starts from genesis. There is no v2 reader, migration marker, dual-write mode,
old-database upgrade command, or old-binary rollback promise. Operators must
resync from genesis (or restore a verified snapshot produced by this format)
when switching an existing datadir.

`state-changeset-v2-*` and `state-tx-range-v1-*` remain authoritative temporal
history; the `v2` in the changeset name is its independent row encoding, not a
legacy index. Posting and directory rows are rebuildable accessors. Cold
history segment formats are unchanged.

## Physical encoding

Exact lookup and logical-prefix discovery use separate keyspaces:

```
state-change-posting-v3- || SHA256(latestKey) || firstBlockBE64
    -> 0x01 || countUvarint || delta(block[1]-block[0])Uvarint || ...

state-change-keys-v3- || latestKey -> empty
```

`firstBlock` is both the first posting and immutable frame identifier, so the
value does not repeat it. Blocks are strictly increasing and count is in
`[1,256]`. Multiple changes to the same latest key in one block collapse to
one candidate. Sorting by `SHA256(latestKey) || blockNum` deliberately unions
two original keys with the same digest instead of overwriting either one.

The frame limit is a format constant, not a runtime option.

## Writers and stage watermark

There are two producers of the same format:

1. Ordinary canonical insertion writes a one-block immutable posting plus its
   directory key in the block's atomic buffer and advances
   `StateHistoryIndex` with that block.
2. Bulk genesis sync writes only authoritative changesets during execution.
   The derived stage scans hash-bound canonical ranges, ETL-sorts candidates,
   collapses hash/block duplicates, and emits frames of at most 256 blocks.

The stage initializes at genesis block 0. A rebuild writes directory and
posting rows before advancing the hash-bound watermark. An interruption can
therefore leave idempotent rows beyond the watermark. Bounded and unbounded
history readers cap index use at the watermark and scan authoritative
changesets for the unindexed tx/block tail, so partial publication cannot alter
results. Rerunning the same stage range overwrites deterministic frame keys.

The stage targets the solidified boundary during sync. This keeps emitted
multi-block frames immutable. Ordinary insertion after the deferred gap does
not jump the watermark; the stage first fills the gap, after which normal live
blocks resume atomic single-frame publication.

## Exactness and seek semantics

SHA-256 is only a candidate selector. For every posting candidate, the reader
opens the referenced changeset block, reconstructs the original latest key
from `FlatDomain`, `Owner`, `Generation`, `Domain`, and logical `Key`, and
requires byte-for-byte equality. Missing/pruned changesets, different keys,
and real hash collisions are skipped. Correctness never depends on collision
probability.

Range lookup scans the segment list under one 32-byte digest and filters values
to inclusive `[fromBlock,toBlock]`. It cannot seek only to `fromBlock`, because
a sparse frame whose first block is earlier may overlap that boundary. Segment
keys and values preserve ascending block order. Earliest-change readers use the
same verified iterator, preserving `GetAsOf`, `eth_call`, `debug_traceCall`,
Wallet historical state, and tx-window semantics.

## Logical-prefix queries

Digest order cannot answer logical-prefix history. Prefix readers enumerate
`state-change-keys-v3- || latestPrefix`, resolve each key through its exact
posting list, perform the same mandatory changeset key check, deduplicate block
candidates, and apply tx/block bounds. This preserves system-store listing,
account-KV prefix rollback, and snapshot-backed archive paths without repeating
the variable-length logical key once per changed block.

## Rewind, pruning, snapshots, and maintenance

- A non-final canonical rewind deletes only the one-block live frame whose
  first block equals the unwound block. It leaves the directory row as a safe
  stale hint. Packed frames cover finalized staged history and are never
  rewritten by rewind.
- Hot pruning deletes authoritative changesets. A mixed posting frame may then
  contain stale candidates; exact verification skips them.
- `gtron db compact-state-history` removes a frame only when none of its blocks
  still contains a changeset with the frame digest, removes directory keys with
  no remaining posting, and compacts changeset/posting/directory keyspaces.
- Snapshot build and cold compaction consume authoritative changesets.
  Verified hot-history restore writes changesets, tx ranges, directory keys,
  and valid one-block posting frames through sorted ETL. No format marker is
  invalidated.
- `ResetMutableState` deletes changesets, stage progress, postings, and the key
  directory. Genesis replay reconstructs them coherently.
- `db inspect` reports postings and the logical-key directory as separate
  physical keyspaces.

## Full-scale benchmark decision record

The complete scan (`complete=true`) contained 2,552,895,165 rows,
266,482,665 unique latest keys, 9.57997 blocks/key on average, and a maximum of
21,873,175 blocks for one key.

| Candidate | Estimated bytes | Savings vs current rows |
| --- | ---: | ---: |
| current rows | 30,435,403,003 | baseline |
| hash256 row-only | 30,843,715,605 | **-1.3416%** |
| posting-128 | 15,092,156,858 | 50.4125% |
| posting-256 | 14,999,336,550 | 50.7175% |
| posting-512 | 14,969,697,622 | 50.8149% |
| posting-1024 | 14,950,447,871 | 50.8781% |

Posting-256 produced 274,213,039 physical rows. Moving from 256 to 1024 saved
only another 48,888,679 bytes (~46.6 MiB for the whole index) while
quadrupling worst-case decode and incremental build/GC granularity. Production
therefore fixes the frame at 256.

| Blocks per key | Keys |
| --- | ---: |
| 1 | 203,386,824 |
| 2-3 | 52,717,436 |
| 4-15 | 6,990,497 |
| 16-63 | 2,147,011 |
| 64-255 | 893,154 |
| 256-1023 | 223,823 |
| 1024+ | 123,920 |

These buckets sum to 266,482,665. Hash-row-only is rejected: it consumed more
space, cannot serve logical-prefix history alone, and still needs exact
changeset collision verification.

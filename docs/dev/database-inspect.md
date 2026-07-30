# Database storage inspection

`gtron db inspect` explains an offline node's storage use without modifying
chain data. It reports:

- every live Pebble row in `chaindata`, grouped by the rawdb schema prefix;
- row count, key bytes, value bytes, logical bytes, and logical share;
- the total physical size of the Pebble directory;
- the physical bytes and row count of each ancient table (`bodies`,
  `tx_infos`, and `state_roots`).

## Run it

Pebble and the ancient freezer take database locks. Stop the node that owns the
datadir before inspecting it:

```bash
sudo systemctl stop gtron

/data/gtron/go-tron/build/bin/gtron db inspect \
  --datadir /data/gtron/main/datadir \
  --top 30

sudo systemctl start gtron
```

The scan reads every live Pebble key once. On a hundreds-of-gigabytes database
it can take a while and will populate the operating system page cache. Progress
is written every five seconds. Set `--progress 0s` to disable it.

For machine-readable output:

```bash
/data/gtron/go-tron/build/bin/gtron db inspect \
  --datadir /data/gtron/main/datadir \
  --progress 30s \
  --json > /tmp/gtron-db-inspect.json
```

`--top` affects text output only. JSON always contains every observed
keyspace.

## Interpret the numbers

The `chaindata` table shows **logical live bytes**: the uncompressed sum of key
and value lengths returned by Pebble. It is the right number for finding which
business keyspace owns the data, but it will not equal `du` because SST
compression, indexes, bloom filters, WAL files, obsolete files, and LSM
amplification are physical storage concerns shared by many prefixes.

The ancient table sizes are actual file sizes, so they are directly comparable
with `du`.

The most useful rows when diagnosing mainnet growth are:

| Output name | Key prefix | Meaning |
| --- | --- | --- |
| `transaction-info` | `ti-*` | Per-transaction `TransactionInfo` history in Pebble |
| `transaction-info-by-block` | `tib-*` | Per-block `TransactionRet` history in Pebble |
| `transaction-index` | `tx-*` | Transaction hash lookup index |
| `block-body` | `b-*` | Hot block bodies |
| `state-changeset` | `state-changeset-v2-*` | Temporal state pre-values |
| ancient `tx_infos` | freezer table | Frozen per-block transaction history |
| ancient `bodies` | freezer table | Frozen block bodies |

If `transaction-info` is large while ancient `tx_infos` also owns most of the
freezer, the report demonstrates hot/cold history duplication. New binaries no
longer write `ti-*`; individual lookups use `tx-*` to reach the canonical
block-level receipt instead.

## Prune legacy `ti-*` rows

Deploy the new binary before pruning. The node must be stopped, and an older
binary must not be used against the pruned datadir because it only understands
the legacy direct lookup.

The safe first step writes one durable range tombstone and lets normal Pebble
compaction reclaim physical bytes over time:

```bash
sudo systemctl stop gtron

/data/gtron/go-tron/build/bin/gtron db prune-tx-info \
  --datadir /data/gtron/main/datadir \
  --yes

sudo systemctl start gtron
```

Immediate offline reclamation is available with `--compact`, but on a large
mainnet database it may run for hours, generate heavy disk I/O, and need
substantial temporary free space:

```bash
/data/gtron/go-tron/build/bin/gtron db prune-tx-info \
  --datadir /data/gtron/main/datadir \
  --yes \
  --compact
```

The range is exactly `[ti-, ti.)`; it cannot delete adjacent `tib-*` or `tx-*`
rows. The command verifies that no live `ti-*` row remains before returning.

## Migrate ancient bodies and receipts to V2

Ancient V2 groups 64 consecutive protobuf rows into independently seekable
Zstd frames and atomically publishes 65,536-block segments. The node must be
stopped. On installations with an automatic deployment timer, pause that timer
before stopping the node so its health recovery does not restart gtron during
migration.

Use a retained-source canary first:

```bash
gtron db migrate-ancient-v2 \
  --datadir /data/gtron/main/datadir \
  --max-segments 1 \
  --keep-v1 \
  --yes
```

Restart the node and verify historical block and transaction-info APIs. The new
binary reads the published V2 segment first but V1 remains available as a
fallback and as an older-binary rollback source.

After validation, stop the node again and migrate every complete segment:

```bash
gtron db migrate-ancient-v2 \
  --datadir /data/gtron/main/datadir \
  --yes
```

The command resumes from the last manifest, verifies every compressed frame
before advancing the V1 tail, and leaves the final incomplete 65,536-block
range in V1 so the running freezer can keep appending. Once V1 files have been
reclaimed, do not roll back to a pre-V2 binary.

After an initial V2 prefix exists, the normal freezer automatically promotes
each new complete 65,536-block V1 range. Online promotion publishes and installs
the new V2 read view before reclaiming its V1 source, so historical APIs remain
available throughout. Promotion is deferred while the node is bulk-syncing and
can be disabled independently when diagnosing storage or CPU pressure:

```bash
gtron --freezer.v2.disable ...
```

The production defaults remain 64 blocks per frame and 65,536 per segment.
Datadirs migrated with custom values must pass the matching
`--freezer.v2.frame-blocks` and `--freezer.v2.segment-blocks` settings.

## Compact duplicate transaction IDs in existing V2 segments

Deploy a binary containing the compact-row reader first. Then stop the node and
the automatic deployment timer. Existing V2 `tx_infos` rows contain a copy of
the 32-byte transaction hash for every transaction even though the hash is
already the `tx-*` index key and is derivable from the matching body.

Rewrite one segment as a canary:

```bash
gtron db compact-ancient-tx-info-v2 \
  --datadir /data/gtron/main/datadir \
  --max-segments 1 \
  --yes
```

The command upgrades the hot `tx-*` values from a block-only locator to a
block-plus-ordinal locator without changing their 8-byte size. It syncs those
indexes before atomically publishing the ID-less replacement segment. All
other TransactionInfo and unknown protobuf fields are preserved. Restart and
verify both historical transaction-by-ID and block receipt queries, then stop
the node and finish the remaining segments:

```bash
gtron db compact-ancient-tx-info-v2 \
  --datadir /data/gtron/main/datadir \
  --yes \
  --json > /data/gtron/main/ancient-tx-info-v2-compact.json
```

## Benchmark the historical transaction index

Every live `tx-*` row currently stores the three-byte schema prefix, complete
32-byte transaction hash, and eight-byte block/ordinal locator in Pebble. To
measure the next cold-index optimization against a stopped node:

```bash
gtron db benchmark-tx-index \
  --datadir /data/gtron/main/datadir \
  --sample-transactions 1000000 \
  --windows 256 \
  --prefix-bits 20 \
  --lookups 1000000 \
  --progress 30s \
  --json > /data/gtron/main/tx-index-benchmark.json
```

The command is read-only and bounded by `--sample-transactions`. It seeks into
evenly distributed transaction-hash windows rather than scanning every index
row. Projections compare the current 43-byte logical row with an exact sharded
hash and 64/96-bit routed fingerprints. Fingerprint candidates are always
verified against the complete transaction hash in the canonical block body;
the collision estimates therefore describe extra candidate reads, not an API
correctness risk.

## Migrate historical transaction indexes

After benchmarking and deploying a binary containing the cold-index reader,
stop both gtron and its automatic deployment timer. The command indexes exactly
the block prefix already protected by Ancient V2:

```bash
gtron db migrate-tx-index \
  --datadir /data/gtron/main/datadir \
  --prefix-bits 20 \
  --progress 30s \
  --yes \
  --json > /data/gtron/main/tx-index-migration.json
```

Building the run scans the live `tx-*` keyspace in hash order. The run is
checksummed, fsynced, fully verified, and atomically added to the manifest
before any hot row is deleted. A second validation scan plans one atomic Pebble
batch: delete the complete `tx-*` namespace and reinsert only rows above V2
coverage. This avoids billions of point tombstones while preserving the hot
tail. A process interruption is safe: rerunning either publishes a previously
completed run or repeats the atomic replacement using the already published
cold copy. Recent rows above V2 coverage remain in Pebble.

By default, range-deleted SST bytes are left for normal background compaction.
Use `--compact` only when immediate reclamation justifies several hours of
heavy offline I/O and the host has sufficient temporary disk space. The
configured `--progress` interval also emits compact heartbeats. Once hot history
has been deleted, do not roll back to a binary without cold-index support.

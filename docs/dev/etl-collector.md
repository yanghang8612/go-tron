# ETL Collector Runbook

Status: initial production integration. Snapshot latest-domain restore now
loads through the collector, state-domain history restore does the same, and
chain-freezer hot lookup index restore uses the collector for its `bh-`, `tx-`,
and `ti-` rows. Chain-index sidecar builds now use the collector for external
hash-order sorting. `rawdb.DerivedIndexCollector` provides the typed bulk-load
entry point for transaction lookup/info, account trace, balance trace, and
section bloom backfills. Transaction lookup/info and section-bloom rebuilds
from retained chain data now use that collector and are exposed through `gtron
db`. Account trace repair from retained block-balance trace rows also uses the
collector and is exposed through `gtron db rebuild-account-traces`. Historical
block-balance-trace repair is exposed through `gtron db backfill-balance-traces`
with an isolated replay DB and collector-backed trace writes. History-enabled
canonical replay now populates `BlockBalanceTrace` and final `AccountTrace`
rows during execution, so Wallet HTTP/gRPC account and block balance trace read
paths have a live data source for newly imported history-enabled blocks.
Snapshot restore and snapshot build commands expose `--snapshot.etl.*` flags so
operators can place large sorted-run scratch files on fast temporary storage.
Bulk sync exposes the same controls for its recoverable `TxLookup` stage through
`--sync.etl.tempdir`, `--sync.etl.buffer`, and `--sync.etl.batch`.

## Purpose

Erigon uses sorted ETL ingestion to turn large unordered changes into
key-ordered database writes. go-tron's equivalent starts in
`core/rawdb/etl`:

- collect `Put` and `Delete` operations in any order
- spill sorted runs to a private temp directory when the memory buffer fills
- merge runs by key
- collapse duplicate keys so the latest collected operation wins
- load the final stream through `ethdb.KeyValueWriter`, using `ethdb.Batcher`
  when the target DB supports batches

This reduces write amplification for large snapshot restore, history backfill,
and derived index construction when those paths switch from direct unordered
writes to collector-backed loads.

## Current Callers

- `snapshots.Manager.RestoreLatest` restores account-latest, account-KV latest,
  generation, code, commitment root/checkpoint, and commitment branch latest
  segments through one collector, then loads the final rawdb key stream in
  physical key order. Use `RestoreLatestWithOptions` to control ETL scratch
  space.
- `snapshots.Manager.RestoreStateDomainHistory` restores StateDomainChange hot
  rows, inverse indexes, and StateTxRange rows through one collector, then
  loads the final rawdb key stream in physical key order. Use
  `RestoreStateDomainHistoryWithOptions` to control ETL scratch space.
- `snapshots.RestoreChainFreezerIndexes` restores block-hash, transaction-hash,
  and per-transaction-info hot lookup rows through one collector, then loads
  the final rawdb key stream in physical key order. Use
  `RestoreChainFreezerIndexesWithOptions`, `RestoreChainFreezerOptions.ETL`, or
  `RestoreVerifiedSnapshotOptions.ETL` to control ETL scratch space from higher
  restore entry points.
- `snapshots.BuildChainIndexSegmentFromChainFreezerSegment` builds the
  chain-freezer `chain-index` sidecar through one collector, then streams
  sorted block-hash and transaction-hash entries into the existing sidecar file
  format. Use `BuildChainIndexSegmentFromChainFreezerSegmentWithOptions` or
  `Aggregator.BuildChainFreezerWithOptions` to control ETL scratch space.
- `rawdb.DerivedIndexCollector` is the typed bulk-load entry point for
  replay-derived RPC indexes: transaction lookup/info rows, account trace,
  balance trace, and section bloom rows. Backfill tools can add rows in block
  execution order, then `Load` writes the final rawdb key stream in physical
  key order.
- `rawdb.RebuildTransactionDerivedIndexesFromBlocks` rebuilds transaction
  reverse lookup and transaction-info rows from retained blocks plus hot or
  ancient per-block `TransactionRet` rows through `DerivedIndexCollector`.
- `gtron db rebuild-tx-indexes` is the operator entry point for that rebuild.
  It opens the datadir hot store plus read-only ancient freezer rows and writes
  the rebuilt hot `tx-`, `tib-`, and `ti-` rows through sorted ETL.
- `BlockChain.AdvanceTransactionLookupStage` rebuilds only the deferred `tx-`
  reverse lookup rows after bulk sync. Runtime `--sync.etl.*` controls its
  collector scratch directory, buffer, and batch size without changing block
  execution or canonical stage semantics.
- `rawdb.RebuildSectionBloomsFromTransactionInfos` rebuilds java-tron-compatible
  section-bloom rows from retained canonical blocks plus hot or ancient
  per-block `TransactionRet` log payloads. Partial-range rebuilds read existing
  section rows first and OR in new block offsets so they do not clear other
  blocks in the same section. Snapshot `section-bloom` sidecars are stricter:
  they are published only for complete 2048-block bloom sections because cold
  manifests and prune stages reason at section granularity.
- `gtron db rebuild-section-blooms` exposes that section-bloom rebuild. It uses
  the same datadir, ancient freezer, range, and ETL scratch-space flags as
  `rebuild-tx-indexes`.
- `snapshots.BuildEventLogSegmentFromChain` and `Aggregator.BuildEventLogs`
  now build registered cold `event-log` sidecars from retained blocks plus hot
  or ancient per-block `TransactionRet` payloads. The current segment is a
  verified immutable log stream with segment-local address and positional-topic
  postings, plus manager-side range filtering across registered segments.
  `gtron snapshot build-event-logs` exposes the builder, and
  `gtron snapshot build-derived-indexes` includes event-log sidecars alongside
  balance-trace and section-bloom segments. The production snap-mode history
  builder can also publish the matching event-log and event-log-index sidecars
  in the same manifest generation as the state-history segment before hot prune
  runs, using the same `RestoreETLOptions` scratch controls as manual snapshot
  builds when the lifecycle config supplies them. Event-log builder paths now
  advance `SnapshotEventLogBuild` only after writing matching event-log-index
  coverage as a continuous prefix from block 1, and verification rebuilds
  address/topic postings from the active event-log segments before trusting the
  sidecar, so stage-status automation can compare indexed cold log coverage
  with the verified chain `Finish` boundary. Filtered cold reads can compose
  adjacent `event-log-index` sidecars across a query range, so unrelated segment
  files can be skipped even when no single index sidecar spans the full request.
  `TronBackend.GetLogs` uses it when checker-verified manifest coverage fully
  spans the query range; broader global/recsplit-style address/topic point
  indexes beyond segment-start fanout remain follow-up work.
- `TronBackend.GetLogs` consumes those section-bloom rows as an optional
  prefilter for address/topic-constrained log queries. Missing or malformed
  bloom rows are treated as unknown and fall back to the pre-existing
  block-by-block scan, so older datadirs remain correct until the operator runs
  the rebuild. Snap-mode history passes can now publish full-section cold
  `section-bloom` sidecars once the state-history cutoff has crossed the full
  bloom section through the lifecycle ETL settings; the hot prune hook still
  verifies the cold row byte-for-byte before deleting hot `sb-` rows.
- `TronBackend.GetAccountBalanceTrace` and `GetBlockBalanceTrace` expose
  retained account/balance trace rows through Wallet HTTP/gRPC APIs.
  History-enabled canonical replay now populates those rows during block
  execution. Snap-mode history passes now also build matching cold
  `balance-trace` sidecars when every canonical block in the pass range has a
  hash-matching hot `BlockBalanceTrace` and every account touched by those
  operations has an exact-height hot `AccountTrace`; these lifecycle builds use
  the configured ETL scratch path for the account index. Incomplete legacy
  ranges are skipped instead of publishing false cold coverage, while
  mismatched block traces fail the pass.
- `rawdb.RebuildAccountTracesFromBlockBalanceTraces` rebuilds account-trace
  rows from retained `BlockBalanceTrace` operation diffs through
  `DerivedIndexCollector`.
- `gtron db rebuild-account-traces` exposes that account-trace repair command
  to operators. It opens the same hot Pebble plus read-only ancient freezer
  stack as the other `db` rebuild commands and supports the shared block-range
  and ETL scratch-space flags.
- `gtron db backfill-balance-traces` fills missing `BlockBalanceTrace` and
  `AccountTrace` rows by replaying retained canonical blocks in an isolated
  database, optionally seeded from a verified signed snapshot. The generated
  rows are copied back through `DerivedIndexCollector`, and
  `--db.replay.dir` makes the replay database resumable.

## Benchmarking

Run the path-specific restore benchmark after changing the collector or any
snapshot restore path:

```bash
go test ./core/state/snapshots -run '^$' \
  -bench 'BenchmarkSnapshotRestoreETL' \
  -benchtime=5x -count=3 -benchmem
```

The benchmark compares the former direct unordered write shape with the sorted
collector load for latest-domain restore, state-domain history restore, and
chain-freezer hot lookup index restore. It also compares in-memory chain-index
sidecar sorting with the collector-backed sidecar build. The `out_of_order/put`
metric should be greater than zero for the direct restore baselines and exactly
zero for every restore `sorted_etl` variant.

Recorded samples:

- [2026-06-10 smoke sample](etl-collector-benchmark-results-2026-06-10.md)
- [2026-06-10 derived-index smoke sample](etl-derived-index-benchmark-results-2026-06-10.md)

## Usage

```go
collector, err := etl.NewCollector(etl.Options{
    TempDir:     "/path/to/tmp",
    BufferLimit: 64 << 20,
    BatchSize:   ethdb.IdealBatchSize,
})
if err != nil {
    return err
}
defer collector.Close()

if err := collector.Put(key, value); err != nil {
    return err
}
if err := collector.Delete(oldKey); err != nil {
    return err
}
stats, err := collector.Load(db)
```

`Load` is single-use. A caller should create a new collector for each logical
restore/backfill stage, then record the returned `Stats` next to the stage
metrics.

Snapshot restore entry points accept `snapshots.RestoreETLOptions`:

```go
opts := snapshots.RestoreVerifiedSnapshotOptions{
    ETL: snapshots.RestoreETLOptions{
        TempDir:     "/path/to/fast-scratch",
        BufferLimit: 256 << 20,
        BatchSize:   ethdb.IdealBatchSize,
    },
}
```

Zero values preserve collector defaults. For production bootstrap, point
`TempDir` at a filesystem with enough free space for temporary sorted runs.

Transaction index rebuild accepts the same scratch-space shape through CLI
flags:

```bash
gtron db rebuild-tx-indexes \
  --datadir /path/to/datadir \
  --db.from-block 1 \
  --db.to-block 1000000 \
  --db.etl.tempdir /path/to/fast-scratch \
  --db.etl.buffer 256
```

Omit `--db.to-block` to rebuild through the current head. The command fails on
missing blocks rather than silently publishing partial transaction indexes.

Bulk sync can move TxLookup's temporary sorted runs to a fast local disk while
keeping canonical data in the node datadir:

```bash
gtron --datadir /path/to/datadir \
  --sync.etl.tempdir /path/to/fast-scratch \
  --sync.etl.buffer 256 \
  --sync.etl.batch 16
```

`0` keeps the collector defaults; `GTRON_SYNC_ETL_TEMPDIR`,
`GTRON_SYNC_ETL_BUFFER`, and `GTRON_SYNC_ETL_BATCH` provide the same runtime
settings for managed deployments.

Section bloom rebuild uses the same flags and rebuilds the java-tron
`section-bloom` rows from stored `TransactionInfo.log` payloads:

```bash
gtron db rebuild-section-blooms \
  --datadir /path/to/datadir \
  --db.from-block 1 \
  --db.to-block 1000000 \
  --db.etl.tempdir /path/to/fast-scratch \
  --db.etl.buffer 256
```

Omit `--db.to-block` to rebuild through the current head. The command fails on
missing blocks and preserves existing block bits outside the requested range
when a section row already exists. `eth_getLogs` and filter queries can then use
the rebuilt rows to skip blocks that cannot match the requested address/topic
bloom.

Snapshot restore and snapshot sidecar builds use `--snapshot.etl.*` for the
same scratch-space controls:

```bash
gtron snapshot restore \
  --datadir /path/to/datadir \
  --snapshot.etl.tempdir /path/to/fast-scratch \
  --snapshot.etl.buffer 512

gtron snapshot build-derived-indexes \
  --datadir /path/to/datadir \
  --snapshot.from-block 1 \
  --snapshot.to-block 1000000 \
  --snapshot.etl.tempdir /path/to/fast-scratch \
  --snapshot.etl.buffer 512
```

The same flags are accepted by `snapshot bootstrap`, `snapshot build-freezer`,
`snapshot build-balance-traces`, `snapshot build-section-blooms`, and
`snapshot build-event-logs`. When passed to the root `gtron` process, the same
flags also configure the snap-mode background lifecycle's state-history,
event-log, balance-trace, and section-bloom builds; set them before starting a
long-running node so those periodic passes do not spill sorted runs into the
default temporary directory.

## Migration Targets

- global/recsplit-style event-log address/topic point accessors beyond
  segment-start fanout and segment-local postings
- any future RPC index build where input order follows block execution rather
  than target DB key order

Do not use the collector inside per-block consensus execution. It is a bulk
ingestion tool, not a replacement for serial state mutation.

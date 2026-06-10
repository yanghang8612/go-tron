# ETL Collector Runbook

Status: initial production integration. Snapshot latest-domain restore now
loads through the collector, state-domain history restore does the same, and
chain-freezer hot lookup index restore uses the collector for its `bh-`, `tx-`,
and `ti-` rows. Backfill and derived-index builders still need deliberate
migration and path-specific benchmarks.

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
chain-freezer hot lookup index restore. The `out_of_order/put` metric should be
greater than zero for the direct baselines and exactly zero for every
`sorted_etl` variant.

Recorded samples:

- [2026-06-10 smoke sample](etl-collector-benchmark-results-2026-06-10.md)

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

## Migration Targets

- chain-index and future transaction-info/event-log sidecar builders
- derived account/balance trace index backfills
- any future RPC index build where input order follows block execution rather
  than target DB key order

Do not use the collector inside per-block consensus execution. It is a bulk
ingestion tool, not a replacement for serial state mutation.

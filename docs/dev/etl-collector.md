# ETL Collector Runbook

Status: initial production integration. Snapshot latest-domain restore now
loads through the collector; history restore, backfill, and derived-index
builders still need deliberate migration and path-specific benchmarks.

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
  physical key order.

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

## Migration Targets

- snapshot history restore paths that rebuild many hot rows
- chain-index and future transaction-info/event-log sidecar builders
- derived account/balance trace index backfills
- any future RPC index build where input order follows block execution rather
  than target DB key order

Do not use the collector inside per-block consensus execution. It is a bulk
ingestion tool, not a replacement for serial state mutation.

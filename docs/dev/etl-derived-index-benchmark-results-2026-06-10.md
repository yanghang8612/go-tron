# Derived Index ETL Benchmark Results - 2026-06-10

This file records a short `1x` smoke sample for the rawdb derived-index bulk
loader. It proves the benchmark harness and write-order metric work; use longer
runs before tuning collector defaults.

Environment:

- Host: Apple M1 Max
- GOOS/GOARCH: `darwin/arm64`
- Package: `github.com/tronprotocol/go-tron/core/rawdb`

Command:

```bash
go test ./core/rawdb -run '^$' \
  -bench 'BenchmarkDerivedIndexCollector' \
  -benchtime=1x -count=1
```

Results:

| Benchmark | ns/op | out_of_order/put | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| DerivedIndexCollector/direct_unordered | 1,017,459 | 0.5000 | 746,944 | 8,571 |
| DerivedIndexCollector/sorted_etl | 4,660,417 | 0 | 7,154,272 | 15,708 |
| RebuildTransactionDerivedIndexes/direct_unordered | 770,709 | 0.6125 | 552,600 | 6,821 |
| RebuildTransactionDerivedIndexes/sorted_etl | 3,804,709 | 0 | 5,007,984 | 10,384 |

Notes:

- The direct baseline writes transaction lookup/info, account trace, balance
  trace, and section bloom rows in block-execution order.
- The sorted ETL variant uses `rawdb.DerivedIndexCollector` and eliminates
  out-of-order rawdb writes in the final load stream.
- The rebuild benchmark uses retained blocks plus per-block `TransactionRet`
  rows and exercises `rawdb.RebuildTransactionDerivedIndexesFromBlocks`.
- This in-memory smoke run measures collector overhead, not Pebble compaction
  savings. Use larger Pebble-backed backfill samples before changing defaults.

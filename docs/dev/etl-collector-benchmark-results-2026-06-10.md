# ETL Collector Benchmark Results — 2026-06-10

This file records the first smoke sample from
`BenchmarkSnapshotRestoreETL`. It is intentionally a short `1x` run to prove
the benchmark harness and write-order metric work; use the longer runbook
command before making tuning decisions.

Environment:

- Host: Apple M1 Max
- GOOS/GOARCH: `darwin/arm64`
- Package: `github.com/tronprotocol/go-tron/core/state/snapshots`

Command:

```bash
go test ./core/state/snapshots -run '^$' \
  -bench 'BenchmarkSnapshotRestoreETL' \
  -benchtime=1x -count=1
```

Results:

| Benchmark | ns/op | out_of_order/put | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| latest/direct_unordered | 700,125 | 0.4990 | 99,312 | 2,324 |
| latest/sorted_etl | 2,632,167 | 0 | 6,790,840 | 8,619 |
| state_history/direct_unordered | 533,542 | 0.3333 | 376,384 | 5,564 |
| state_history/sorted_etl | 2,554,291 | 0 | 6,985,432 | 9,653 |
| chain_freezer_indexes/direct_unordered | 732,125 | 0.6654 | 433,008 | 6,162 |
| chain_freezer_indexes/sorted_etl | 2,142,459 | 0 | 4,898,648 | 10,876 |

Notes:

- The direct baselines intentionally model the previous restore write shape;
  all three produce out-of-order rawdb writes.
- The sorted ETL variants add collector overhead in this in-memory smoke run,
  but eliminate out-of-order writes completely. The real target is lower Pebble
  write amplification during large snapshot restore and backfill workloads.

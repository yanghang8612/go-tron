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
| latest/direct_unordered | 451,167 | 0.4990 | 99,328 | 2,324 |
| latest/sorted_etl | 5,454,000 | 0 | 6,786,872 | 8,618 |
| state_history/direct_unordered | 556,917 | 0.3333 | 376,272 | 5,563 |
| state_history/sorted_etl | 2,521,958 | 0 | 6,985,448 | 9,653 |
| chain_freezer_indexes/direct_unordered | 792,708 | 0.6654 | 433,344 | 6,164 |
| chain_freezer_indexes/sorted_etl | 2,021,792 | 0 | 4,898,312 | 10,874 |
| chain_index_build/direct_in_memory | 6,065,667 | n/a | 83,256 | 833 |
| chain_index_build/sorted_etl | 7,305,375 | n/a | 4,514,504 | 4,009 |

Notes:

- The direct baselines intentionally model the previous restore write shape;
  all three produce out-of-order rawdb writes.
- The sorted ETL variants add collector overhead in this in-memory smoke run,
  but eliminate out-of-order writes completely. The real target is lower Pebble
  write amplification during large snapshot restore and backfill workloads.
- The chain-index build rows compare the previous in-memory sidecar sort with
  the collector-backed external sort. They do not report `out_of_order/put`
  because the output target is a sidecar file, not a rawdb writer.

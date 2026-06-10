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
| direct_unordered | 995,792 | 0.5000 | 746,672 | 8,569 |
| sorted_etl | 4,737,208 | 0 | 7,154,496 | 15,710 |

Notes:

- The direct baseline writes transaction lookup/info, account trace, balance
  trace, and section bloom rows in block-execution order.
- The sorted ETL variant uses `rawdb.DerivedIndexCollector` and eliminates
  out-of-order rawdb writes in the final load stream.
- This in-memory smoke run measures collector overhead, not Pebble compaction
  savings. Use larger Pebble-backed backfill samples before changing defaults.

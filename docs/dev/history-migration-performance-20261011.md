# History migration performance, 2026-10-11

This is a local implementation and same-input comparison. Nothing was deployed
or migrated on the server. The supplied live evidence describes normal sync and
archive catch-up with no offline migration running. Complete host/process/disk
observations and window limits are recorded in
[live-sampling-summary.json](../../build/benchmarks/20261011-migration-performance/live-sampling-summary.json).

## Evidence and changes

The supplied 30.10-second live CPU profile contains 99.78 CPU seconds. The
history index stage accounts for 6.43 seconds, shared-pack decoding for 5.33,
and its complete decoded-pack SHA for 4.05. A serial chunk cache was already
active. The change retains both chunk authentication and the complete pack SHA;
it reuses one private, size-bounded block buffer across the borrowed range scan.
Owning readers retain their allocation and byte ownership. This reduces
allocation and zeroing in the sampled path without predicting an overall node
throughput gain from a component benchmark.

A second supplied 30-second profile has no index-stage sample. It attributes
4.37/93.09 CPU seconds to `Runner.OnePass`, 1.32 to manifest validation, 1.25
to manifest publication and 1.05 to integration (these cumulative costs overlap).
The live batches contain only 57–71 blocks and about 7.6 MB; publication takes
1.5–1.6 seconds versus 0.8–1.0 seconds for history construction. The reported
2.65–2.8-second elapsed interval is the complete deferred operation, not another
interval to add to it. Read/write competition and many small publications remain
the current overall archive bottlenecks. These changes do not establish that
archive throughput now keeps pace with import.

The later observed 14:48:43.402–14:54:03.768 window advances publication only
575 blocks in 320.366 seconds (1.79 blocks/second); backlog increases 6,343
(19.80/second), with many 20% duty intervals. The earlier 321.55-second window
averages 6.67 blocks/second and includes higher duty intervals, so the rates
are not a before/after code comparison. Six disk samples average about 107 MiB/s
read, 95 MiB/s write, queue 4.89, await 0.792 ms and 83.93% utilization. The
observed process is syncing/archiving, not an identified offline migration job.

Adjacent progress-only publication remains a possible separate optimization:
`UpdateHotPruneProgress` loads and validates the full catalog and publishes it
again. A completion cursor must never advance ahead of its durable work merely
to save a publication. This increment does not combine lifecycle phases or
change scheduler/hardware admission thresholds. The additional metadata change
below instead removes repeated work within one existing publication.

The offline hot-to-history mover now records one exact range deletion per
fully examined page. All rows, including discarded legacy owners, are covered.
The destination still commits and syncs before the source deletion commits and
syncs. Each page releases its iterator, so the mover does not pin an entire
family's old SST files. Partial runs resume from surviving source keys.

Cold R1-to-CDC migration hashes companions while copying and syncing them.
Fresh verified copies no longer require a second full read before installation;
an existing destination name still requires full size/checksum verification.
Installed names are synced once per parent directory before journal publication.
Retired sources retain their exact identity check before unlink, and deletion
directories are synced once before clearing the journal. For an ordinary trio
sharing one directory, installation/deletion directory syncs fall from 7 to 2.
File-content syncs, atomic manifest publication, and journal recovery remain.

## Controlled local comparison

Apple M1 Max, Darwin arm64, Go 1.27.2, `GOMAXPROCS=4`, identical input and benchmark source.
The before implementation is saved under `build/benchmarks/20261011-migration-performance/`
and selected using `before-overlay.json`; no checkout files were swapped.
Results below are medians of three runs. MB/s is decimal logical input throughput;
B/op measures allocations, not RSS. These are component measurements, not replay
of production heights, chunk boundaries, storage latency, or hit rates.

The index scan uses 16 authenticated shared packs, each with 512 KiB of previous
value data. Every invocation creates and closes its serial chunk cache, as the
index stage does. The byte patterns deliberately control reuse and compression.

| Fresh-cache scan | Before ms | After ms | Before → after MB/s | Before → after allocated MB |
| --- | ---: | ---: | ---: | ---: |
| Unique raw | 10.340 | 9.904 | 811 → 847 | 35.062 → 27.072 |
| High reuse raw | 6.277 | 5.801 | 1336 → 1446 | 16.612 → 8.621 |
| High reuse Snappy | 6.049 | 5.512 | 1387 → 1522 | 11.575 → 3.585 |

Final fresh-cache timing ranges are 10.310–10.567 → 9.862–9.931 ms,
6.261–6.339 → 5.782–5.811 ms, and 6.047–6.127 → 5.479–5.524 ms.
The allocation reductions are approximately 23%, 48%, and 69%. Earlier warm-cache
experiments show larger percentage reductions because cache population is absent;
they are supplemental and are not the index-stage total.

The offline mover uses real Pebble stores, 262,144 identical 128-byte values,
and 1 MiB pages. Setup, initial compaction, and complete destination value/source
absence checks are outside timing. Median elapsed time is 693.610 → 659.030 ms
(61.60 → 64.84 MB/s). One before sample overlapped an independent test and took
949 ms; the other before samples took 690–694 ms, versus 652–674 ms after.
Point deletes fall from 262,144 to zero, replaced by 41 range deletes. Delete-key
API bytes fall from 9,175,040 to 2,911; this is not a physical disk-write counter.
Durability syncs remain 86. Allocated bytes remain approximately 171 MB/op, and
allocation count increases from 2,257 to 4,633; no allocation win is claimed.

The cold benchmark uses the same production-built R1 trio with 20,000 changes
in both implementations and includes streaming encoding, authentication,
installation, journal/manifest publication, and old-file deletion. Median elapsed
time is 158.075 → 142.554 ms (35.05 → 38.86 MB/s); ranges are 156.649–162.813 →
140.497–144.290 ms. Allocation is effectively unchanged (19.887 → 19.821 MB/op).
The exact logical stream is 5,540,132 bytes. Both implementations produce the same
5,210,497-byte CDC history file and 5,460,964-byte complete trio, from a
5,457,945-byte R1 source trio. This fixture grows by 3,019 bytes, so the benchmark
explicitly enables `AllowGrowth`; it demonstrates migration performance, not
compression savings. Default migration still skips insufficient savings.

The initial cold CPU sample is dominated by system calls (59%), followed by
Zstd encoding (15%). That sample also contains fixture setup, so it supports
investigating file operations but is not a precise migration-only CPU attribution.
The measured throughput improvement and removed read/sync operations are reported
separately from the online profile.

## Additional manifest publication optimization

The archive integration now owns preallocated active/retired slices, deduplicates
its private retired slice in place, and leaves their only required sort to
publication. It avoids the extra constructor copy and duplicate sorts. Empty
active inputs still produce a nil slice and the exact existing `"segments":null`
JSON. Neither successful nor failed integration modifies caller-owned inputs.

Publication validates every reference and companion after sorting. Because that
sort groups the exact existing dataset/domain/kind overlap family, overlap
validation checks adjacent ranges without allocating and sorting another set
of per-family ranges. A containing interval still overlaps an adjacent interval.
Public `Manifest.Validate` retains arbitrary-order validation. Parent-directory
recognition scans path components without allocating a `strings.Split` slice;
its semantics are compared directly with the original helper, including platform
separators and non-ASCII paths. No file-authentication cache or lifecycle phase
was added, and no completed prune cursor advances earlier.

The complete publication benchmark contains 70,198 active and 92,667 retired
references (162,865 total). It times durable publication, byte-identity cache
seeding and the next production load, with a 256 MiB cache budget; the integration
variant also constructs and retires replacement refs. Both versions produce
exactly 35,916,455-byte / 35,917,559-byte JSON files respectively.

The final comparison uses the repository's Go 1.25 toolchain generation:
the already installed Go 1.25.5, matching explicit `GOROOT`, Darwin arm64,
`GOMAXPROCS=4`, five operations per run and three runs, before then after.
Baseline publication/integration sources are selected by
`publication-before-overlay.json`. No live manifest or segment was read.

| Complete operation, Go 1.25.5 | Before ms | After ms | Before → after allocated MB | Before → after allocations |
| --- | ---: | ---: | ---: | ---: |
| Publish + production load | 140.579 | 130.780 | 226.486 → 185.614 | 820439 → 657459 |
| Integrate + publish + production load | 201.466 | 186.407 | 310.968 → 244.094 | 820768 → 657752 |

Medians show approximately 7.0% and 7.5% less elapsed time. Timing ranges are
138.868–141.080 → 128.793–130.859 ms and 197.264–204.526 → 181.506–187.343 ms.
Integration allocation bytes decrease about 21.5%, and allocation counts
decrease about 19.9% in both paths. JSON encoder buffer-pool recovery causes
allocation-byte steps: pure publication's after range is 185.614–239.301 MB,
including one sample above the baseline's 226.486 MB. Thus the table's byte
medians are not a guaranteed per-publication bound or RSS reduction.

Supplementary Go 1.27.2 runs, retained in `manifest-publication-final-*.log`,
show a smaller publish/load timing gain (146.162 → 142.522 ms) and integration
gain (194.796 → 178.612 ms), with larger JSON buffer-pool variation. They are
not substituted for the Go 1.25.5 results. Neither comparison demonstrates that
metadata optimization overcomes the observed 20% archive duty intervals.

New publication regressions compare ordered/public validation on shuffled,
invalid and nested-range catalogs; compare path scanning against the original
helper on explicit boundary paths and 1,000 generated paths; verify successful
and failed integration ownership; verify completed prune progress; verify exact
frozen JSON bytes; and reload empty/nonempty publication after clearing the cache.
Go 1.27.2 manifest/aggregator tests passed in 6.243 seconds, additional regressions
in 0.773 seconds, and the targeted race set in 2.491 seconds. Astra identified
the empty-slice boundary during audit; it was fixed and the final audit passed.
Go 1.25.5 validation and exact final results are in `manifest-publication-go125-tests.log`.

Logs are `manifest-publication-go125-{before,after}.log` plus the test/race logs
under the same benchmark directory. To reproduce, select the desired Go 1.25
installation and matching GOROOT, then run:

```sh
GOMAXPROCS=4 go test ./core/state/snapshots -run '^$' -bench '^BenchmarkManifestArchivePublication$' -benchtime=5x -count=3 -benchmem
```

Insert `-overlay build/benchmarks/20261011-migration-performance/publication-before-overlay.json`
for the saved baseline. Its new-regression test file is omitted because the
old implementation does not have the new private validation helper; the measured
benchmark source is identical.

## Verification and reproduction

New regressions cover exact page bounds and singleton neighbors across resume;
borrowed scans over alternating large/small blocks with copied output equality;
whole-pack digest corruption rejecting the block before callbacks; companion copy
digest mismatch; identical installed names; and conflicting installed identities.
Existing CDC/R1 tests cover prepublication and postpublication resume, including
partial old-file removal, leases, corruption, and space-admission behavior.

Targeted rawdb tests passed in 2.659 seconds; CDC/R1 migration tests passed in
19.041 seconds. Targeted race tests passed in rawdb (2.407 seconds) and snapshots
(2.463 seconds). Independent Astra audit found no actionable issue and independently
passed related rawdb/snapshots tests. No full-repository test result is claimed.

Final combined validation with explicit Go 1.25.5 and matching GOROOT passed:
`go test ./core/... ./actuator/... ./cmd/gtron -count=1 -timeout 900s`.
This includes core (320.535 s), rawdb (70.514 s), snapshots (204.697 s),
state (3.722 s), and CLI (111.028 s). The Go 1.25.5 Sapling-enabled
`make gtron` build also passed. Final logs are `final-go125-tests.log` and
`final-go125-build.log`. An initial mismatched inherited GOROOT caused compiler
version errors before tests ran; setting the matching GOROOT resolved them.
The old failure logs are retained separately as `toolchain-mismatch-*.log`.

Logs and supplied profiles are under
`build/benchmarks/20261011-migration-performance/`: `scan-final-{before,after}.log`,
`hot-controlled-{before,after}.log`, `cold-controlled-{before,after}.log`,
`cold-size-{before,after}.log`, `rawdb-tests.log`, `cold-tests.log`, and
`migration-race.log`. Earlier exploratory logs retain concurrency/noise and are
not substituted for the final fresh-cache comparison.

```sh
GOMAXPROCS=4 go test ./core/rawdb -run '^$' -bench '^BenchmarkStateHistoryMigrationScan$' -benchtime=500ms -count=3 -benchmem
GOMAXPROCS=4 go test ./core/rawdb -run '^$' -bench '^BenchmarkDirectHistoryMove$' -benchtime=3x -count=3 -benchmem
GOMAXPROCS=4 go test ./core/state/snapshots -run '^$' -bench '^BenchmarkHistoryCDCMigrate$' -benchtime=5x -count=3 -benchmem
```

Insert `-overlay build/benchmarks/20261011-migration-performance/before-overlay.json`
after `go test` for the saved baseline. The overlay artifacts are local build
outputs and require this checkout's absolute paths.

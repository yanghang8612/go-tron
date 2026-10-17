# Rejected Pebble CLOCK eviction prototypes, 2026-10-17

## Evidence and scope

The frozen 2026-10-17 profile is
`build/benchmarks/20261017-sync-sampling/cpu-current-60s.pb.gz`.
Of 200.78 CPU seconds, `cache.(*shard).runHandCold` accounts for 26.87
seconds flat; its `e.ptype == etCold` source line receives 26.07 seconds.
This supports investigating pointer chasing and scan overhead. It does not
prove a CLOCK algorithm bug, a memory stall count, or a disk throughput limit.
The online profile intersects the commitment point-read path for 25.82 CPU
seconds. The frozen report is not changed by this work.

This investigation is restricted to Pebble's internal cache. Capacity, shard count,
CLOCK replacement decisions, namespace/file identity, references, and payload
ownership must remain the same. No additional cache, metadata index, or
background worker is introduced.

## Candidate A: entry layout

Before this change, a 64-bit entry has `blockLink.next` at offset 32 and
`ptype` at offset 72. A non-cold cold-hand visit reads both fields. Reorder
fields so `ptype`, `referenced`, and `blockLink.next` occupy the first 16
bytes. A 64-bit normal-build entry remains 96 bytes. This reduces the span
of metadata needed for an ordinary ring visit, without changing the ring
or allocating more memory. It is a locality hypothesis; whole cache traces
must establish whether the resulting placement helps on the target machine.

## Candidate B: batch no-op visits

While `evict` holds the shard mutex, target size cannot change. When
`sizeHot < target - coldTarget`, the hot-balancing loop in `runHandCold`
will not execute. For a non-cold entry, that function therefore only advances
`handCold`. Perform consecutive such advances in a tight loop in `evict`,
then call the unchanged `runHandCold` for the next cold entry.

A cold entry must exist when this fast path is entered: eviction requires
`sizeHot + sizeCold >= target`, while the fast-path condition implies
`sizeHot < target`, because `coldTarget >= 0`. Hence `sizeCold > 0`.
No mutations, callbacks, or lock releases happen while skipping these visits.
The first cold entry, all hand positions at the next state-changing action,
and all observable replacement decisions remain identical. Recursive calls
from the hot and test hands still use the original implementation.

This batches scan work; it does not reduce the number of ring nodes examined.
Neither candidate justifies claiming fewer disk reads or a lower miss rate.

## Validation

`build/benchmarks/20261017-cache-eviction/clockpro_workload_test.go.txt`
contains the frozen prototype tests and deterministic complete cache workloads:
hot reads, hot plus scan, scan only, mixed, tiny capacity, oversize values,
and a 256 MiB hot plus scan case with about 24 MiB of entry metadata.
The source loader reconstructs immutable bytes on every cache miss, so
`reads/op` measures actual simulated block-source loads, including allocation
and insertion. The workload uses a fixed RNG seed and fixes the package's
randomized hash seed in test binaries. All variants receive the same payload
budget, request stream, source bytes, process settings, and warmup length.
Reported Go heap allocations exclude manual C allocations; the entry byte
metric separately checks metadata size.

An oracle fixture captured from the original implementation hashes every
returned value/hit decision and each CLOCK state transition (resident ring,
entry types/reference bits, three hands, byte accounting and cold target).
Six 6,000-step traces include file eviction, deletion and reservation.
Independent invariants recalculate all ring counts/byte totals, validate ring
links and table membership, and require test entries to have no resident value.
Additional tests retain handles across resident resize, file eviction,
oversize bypass, reservation and cache destruction; concurrent readers,
setters, deletions, file evictions and reservations are checked under race.

The vendor directory initially contains no cache test files. Prototype validation here
provides cache coverage rather than claiming to have run absent upstream tests.
The test source is kept in the ignored evidence artifact and is applied to a
temporary module (or through a Go source overlay); it is not added to the
production vendor tree because no cache prototype was selected.
Run normal, race, `CGO_ENABLED=0`, and `-tags invariants,tracing` cache tests.
These package tests do not need Sapling. Application native build/testing and
online sampling remain integration gates owned by the deployment task.

## Reproducible A/B evidence

`build/benchmarks/20261017-cache-eviction/` contains frozen original and
candidate sources plus a standalone module archive and `native-bench.sh`.
The script compiles three binaries (baseline, layout, layout plus batching),
runs correctness tests, then executes five alternating-order serial trials
with `CGO_ENABLED=1`, `GOMAXPROCS=1`, and one million operations per case.
Each trial reports misses/source reads and allocation cost. The maximum
payload budget is 256 MiB, with no concurrent benchmark processes. The script
works in a temporary module and does not edit a deployment checkout; its trap
removes temporary source and binaries. Source SHA and environment are saved.

Initial local non-fixed-hash pilot timings are not used as evidence of speedup.
Target Linux/amd64 measurements are required before selecting either candidate.
If complete workloads fail to establish an improvement, retain the negative
result and do not deploy a speculative optimization.

## Rejected layout combinations: target-machine results

The target machine used Go 1.25.5, linux/amd64, CGO enabled, GOMAXPROCS=1,
and an Intel Xeon Platinum 8175M at 2.50 GHz. All 15 benchmark trials and
three correctness runs completed; the wrapper took 269.688 seconds and its
maximum child RSS was 454436 KiB. Sources and the module archive matched the
frozen hashes. Raw results and derived summaries are under
`native-results/results/` in the evidence directory.

Relative to the same-round baseline, median layout timings were 5.11% slower
for scan, 6.74% slower for mixed, and 5.05% slower for large hot plus scan.
Layout plus batching was also slower by 1.51%, 2.02%, and 1.61%, respectively.
Hot-only medians were faster but their ranges were broad. All variants had
matching reported source-read ratios, 96-byte entries and Go heap/alloc costs.
These results do not establish a production benefit. The layout change was
rejected and `entry.go` restored to the original layout.

The local controlled five-round run used Go 1.27.2 on darwin/arm64 (Apple M1
Max), not the target compiler or architecture. It also failed to establish a
benefit; its negative data is preserved in `local-results/`. Do not describe
those measurements as Go 1.25.5 or extrapolate them to Linux.

At that point, one isolated question remained: batching improved the layout combination, but
the three-arm experiment did not measure batching with the original layout.
`native-batch-only.sh` performed the final allowed candidate comparison: original
layout plus original CLOCK versus original layout plus batching, with five new
alternating-order paired rounds and the same complete seven workloads. Older
baseline trials were not reused in this comparison, and no tuning sweep was
performed. Batching remained provisional until this direct experiment
completed. The final result below led to restoring `evict` and retaining the
rejection evidence.

## Final isolated batching result and decision

The final original-layout baseline versus original-layout-plus-batching run
completed on the same native Go 1.25.5/amd64 machine. It took 177.253 seconds
(user 165.533, system 7.958), with maximum child RSS 450396 KiB. Its two
correctness runs and all ten benchmark trials passed. The downloaded archive
SHA256 is `744dcb5937dedaed23d85dd796d678ced90df75fb54c976017e79e3ad256aab5`.
Raw trials and derived summaries are in
`native-batch-only-results/batch-only-results/` under the evidence directory.

| Workload | New baseline ns/op | Batch only ns/op | Median change |
|---|---:|---:|---:|
| hot | 275.1 | 295.2 | +7.31% |
| hot plus scan | 786.8 | 779.0 | -0.99% |
| scan | 1844.0 | 1886.0 | +2.28% |
| mixed | 1279.0 | 1318.0 | +3.05% |
| small | 633.2 | 606.8 | -4.17% |
| oversize | 2739.0 | 2703.0 | -1.31% |
| large hot plus scan | 894.4 | 869.3 | -2.81% |

Negative changes mean faster. These are descriptive medians, not confidence
intervals. Source-read ratios, entry bytes, Go heap bytes and allocation counts
matched in each paired trial. The large hot-plus-scan improvement does not
establish a general benefit in view of mixed and scan regressions. Batching
was therefore rejected too; the original `evict` code was restored without
replacing other parts of `clockpro.go`.

The final vendor tree contains neither prototype nor this investigation's
extra test file. The complete frozen test source remains in the ignored
artifact so the oracle, lifetime, race and invariants checks can be reproduced
with the standalone scripts or a source overlay. This investigation makes no
cache production change and supports no claim of improved online throughput.
No further cache candidates were tuned or benchmarked after this isolated
fourth-variant comparison.

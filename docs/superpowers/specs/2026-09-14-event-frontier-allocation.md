# Event frontier temporary metadata allocation

2026-09-14. Status: implemented; dedicated correctness tests and local
fixed-input benchmarks passed. Combined regression, targeted race and vet passed; native acceptance
remains pending deployment.

## Evidence and scope

The complete profiles recorded for the `833e4a0b` reader release attribute
0.67 CPU seconds before and 0.31 seconds after to
`eventLogBuildBlockFromManifest` (0.59% and 0.25% of the respective profiles).
This is a narrow allocation opportunity, not the main import bottleneck.
The after window still advanced cold pruning at 6.696 blocks/s against
13.599 imported blocks/s. Neither this profile nor component benchmarks can
establish that the throughput gap will close.

The frontier function currently copies full event and event-index references,
sorts each by the manifest ordering, then sorts each again by block bounds.
Only the bounds are used to compute the jointly indexed continuous frontier.
Replace these temporary full-reference arrays with private two-uint64 ranges,
counted from active references and allocated exactly once, then sort each
family once by its bounds. No state is retained after the call.

## Invariants

- Select exactly the same Kind and normalized Dataset; ignore retired rows.
- Preserve coverage through gaps, overlap, duplicate and malformed ranges,
  including the predecessor's zero and MaxUint64 behavior. Equal bounds have
  the same effect regardless of the path tie-break used by the old sort.
- Never sort or mutate the caller's manifest or reference arrays. Keep the
  public full-reference helpers and their sorting side effects unchanged.
- Keep all canonical hash reads, fallback reads, write ordering and errors in
  event stage publication. Do not cache or bypass those proofs.
- Do not change formats, loaders, checksum or fresh Stat checks, publication,
  guards, lifetimes, scheduling, retention, or memory/concurrency budgets.
- Scratch ranges are bounded by the active catalog: 16 bytes per selected
  reference before allocator rounding. Allocation bytes are not process RSS.

## Validation

Freeze the old frontier, reference collection/sorts, coverage algorithms,
complete catchup planner and complete stage-write wrapper from business
revision `833e4a0b02f3d490386b74a8114006658f035b80`, changing only private helper
names. Compare outputs, errors, input immutability and stage database traces.
Exercise nil/empty, wrong dataset/kind, retired rows, gaps, overlaps, duplicate
paths, arbitrary ordering, uint64 boundaries and deterministic randomized
catalogs. Check the unchanged canonical hash error and fallback paths.

Measure complete frontier, covered and gapped catchup planning, and stage
write on identical inputs, with tiny controls and a synthetic catalog matching
the observed 73,113 active / 96,630 retired rows: 32,229 event and 32,229 index
references plus 2,885 of each state-history companion kind. JSON parsing is
outside the timed region. The gapped planner still performs its existing
full-reference overlap search; this optimization does not remove that cost.
An optional SHA-pinned real-manifest replay reads metadata only and must not
depend on live immutable segment files remaining present.

Use Go 1.25.5, GOMAXPROCS=2 and serial benchmark windows. Keep full commands,
raw output and comparison ranges under
`build/benchmarks/20260914-commitment-read/frontier/`. Report timing and
allocation separately; do not interpret metadata savings as disk reclamation.
Deployment and end-to-end results remain pending the parent task's combined
regression, native acceptance and complete online windows.

## Local result

Five isolated 200ms rounds on Go 1.25.5 / Apple M1 Max measured the large
frontier at median 7.083ms before and 0.952ms after. Temporary allocation fell
from 32,996,768 to 1,032,192 B/op (60 to 1 allocations). The 64,458 ranges
contain 1,031,328 element bytes; measured allocator rounding adds 864 bytes.
The full stage-write benchmark includes an in-memory canonical body read/hash
and stage batch write, with median 7.262ms to 0.955ms. It does not model
Pebble latency or fsync.

The complete gap planner improves from median 16.360ms to 9.273ms and still
allocates 41,147,440 B/op because its full-reference repair scan is unchanged.
All 16 operation/catalog/variant combinations completed five rounds; all 13
frozen oracle functions match the source exactly after mechanical identifier
renaming. Raw commands, output and min/median/max are retained in the directory
above. The default native benchmark is `BenchmarkEventFrontierAllocation`;
the optional real JSON replay uses `BenchmarkEventFrontierAllocationRealManifest`
and requires both `GTRON_FRONTIER_MANIFEST` and its exact
`GTRON_FRONTIER_MANIFEST_SHA256`.

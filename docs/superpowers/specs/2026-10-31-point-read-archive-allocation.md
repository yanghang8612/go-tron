# Bounded prefetch table reuse and event companion validation

Base: `26921ac19fddd6259ec67629e53b43f5f7a371d1`. VM and transaction
execution remain serial. Storage keys, values, roots, snapshot formats,
verification, publication and archive eligibility are unchanged.

## Prefetch iterator retention

Unbounded state-prefetch cursors retain at most eight point/range-deletion
iterator pairs. Bounded commitment cursors keep four; canonical storage keeps
sixteen. The eager unbounded factory and recovery after a poisoned inner cursor
share the same private options helper. Their L6 filter policy is unchanged.

The first four slots remain inline. Pebble allocates the existing fixed-size
overflow backing only when a fifth compatible table is visited. Whole-Get
leases still last through borrowed-value release; logical iterator Close does
not release a lease. Error joining, poisoned-inner discard, snapshot sequence,
callback ownership, lazy bounded construction and final cleanup retain their
existing behavior. Handles are bounded; pinned block bytes are variable.

No prefetch worker, queue, application cache, memory budget, traversal order or
physical read plan changes. Higher retention can change filesystem reads and
where an injected physical error is encountered; it does not promise identical
physical IO to four slots.

## Event index companions

`validateEventLogIndexCompanions` filters and sorts local row numbers into the
input manifest rather than copying two slices of large `SegmentRef` values.
It uses the complete existing segment comparator and coverage progression.
Indices are local to the call, never cached or exposed. The input is not sorted
or mutated. Earlier validation stages, complete error text, overflow behavior,
repeated index coverage, file authentication and snapshot ownership are kept.

This reduces temporary validation allocations. It does not bypass verified
history or receipt coverage, promote incomplete archive segments, increase
compression workers, change replay budgets or alter event handoff scheduling.

## Evidence and limits

The ignored evidence root is
`build/benchmarks/20261031-point-read-archive/`.

* `cursor/` retains the fixed 117-record inner-cursor screen, including its
  parser failure and recovery of the original first record without rerunning
  it. The initial no-extra-VFS-traffic rule held that screen. Its unused-inner
  model does not represent the production eager outer factory.
* `pipeline/` retains the stopped baseline calibration whose WAL-backed
  "memory" fixture actually replayed into SSTs. `pipeline-v2/` retains 148
  completed Sync-WAL records and the explicit manual abort. Its direct test DB
  construction omitted the production NoSync default. Neither set is a
  production read-lifecycle acceptance result.
* `pipeline-v3/` explicitly uses and asserts NoSync. The timed complete READ
  lifecycle includes Buffer/plan construction, overlays and cache seeding,
  actual sorted `PrefetchBatch`, eager snapshot/cursor acquisition, decoding,
  callback/release/Close, positive and negative cache consumption, canonical
  cached reads, Discard, and final DB Close. Durable commit/flush/restore is a
  separate fixed four-life correctness test. A setup-only LogData Sync settles
  fixture initialization without changing NoSync; process totals include it.
  Logical/physical WAL bytes, completed flushes and every physical level layout
  remain unchanged during the timed read loop. Memory controls require zero
  SSTs and zero SST ReadAt. Four simultaneous existing warmup workers are
  measured; VM execution is never parallelized.

The v3 method fixes two baseline-only 4x calibration processes per shape, then
uses the registered 3-second-per-op formula rounded to a multiple of four.
All eight shapes run ten balanced AB/BA rounds with equal explicit Nx per pair.
The 160 records satisfy all 66 frozen local numeric criteria. Representative
limited-cache wall/CPU medians decrease 22.694%/21.754%; fit-cache warm medians
decrease 19.940%/19.220%. The 17-table pressure control decreases
12.065%/11.286%; the memory control increases 1.799%/1.502% within its 5% limit.
All records, medians, MADs and absolute allocation/RSS checks are retained.
SST VFS ReadAt counts and returned bytes are distinct from device IO.

The validator has independent literal/random error-and-input equivalence
coverage, plus full Validate and publish/load lifecycle comparisons. Local
large-manifest full-Validate allocation falls about 61.9% (90.33 to 34.38 MiB).
Publish timing is noisy and does not establish an archive-throughput gain.
Replay-budget and owned-replay prototypes failed their benefit gates and are
not included. Optional canonical L6 filtering remains a separate experiment.

These are local component results. Linux exact-source native validation,
independent release audit and stable-process production samples must establish
release readiness and actual sync/backlog effects. Archive rounded segment
backlog and real coverage/head gap must be reported separately.

# Unbounded snapshot point reads for state prefetch

Baseline: `b2fb5cc892cd9b4c80749dd2cc846f960f8a3855`. With
`GTRON_PEBBLE_BOUNDED_POINT_READ=get`, `NewCursor(nil)` now uses the existing
four-entry Pebble GetCursor. Nonempty-prefix commitment cursors keep their
existing behavior. The iterator mode remains selectable. No cache budget,
retention-slot count, storage encoding, compaction option, consensus rule or
snapshot sequence changes.

## Read strategy and ownership

PrefetchBatch sorts exact, potentially sparse keys after resolving its overlay
and application cache. A merging iterator positions every overlapping level;
Get searches levels lazily and may stop at an upper visible value. This change
uses the existing MVCC/merge/range-deletion resolver rather than implementing a
second lookup algorithm. It retains the original snapshot and lifecycle lease.
Borrowed values remain read-only and callback-scoped; ReleaseValue runs after
the callback, including errors and panic. Panic propagates after release.

An unbounded wrapper constructs its first GetCursor eagerly, as the previous
wrapper constructed its Iterator eagerly. Bounded wrappers remain lazy. In this
fork Snapshot.NewIterWithContext returns newIter and a fixed nil error for a
valid snapshot; it does not read SSTs during construction. NewGetCursor similarly
performs no SST reads at construction. Actual read failures remain per-View,
where PrefetchBatch can deliver them to the corresponding callback. Invalid
closed-snapshot lifecycle misuse is not a supported retry mechanism.

Only exact ErrNotFound is absence. Joined missing/read/cleanup errors propagate
and cannot populate negative application-cache entries. On an unbounded engine
Get or ReleaseValue error, the wrapper closes and detaches that inner cursor in
the same operation. It joins fresh cleanup errors with the operation error. The
next View clears the last engine error and constructs a new inner cursor from
the **same** snapshot; it neither takes a new snapshot nor retries within the
failed call. Mutations, flush and compaction between calls cannot change its
visible sequence. A callback business error alone does not discard the cursor.
If callback and release/cleanup both fail, View returns both; the saved engine
error excludes the callback error.

The wrapper saves the latest unbounded engine error for Close, preserving the
previous Iterator's fail-then-Close behavior. Any subsequent View, including a
miss, clears that saved error as a new Iterator positioning operation did.
Close joins the remaining saved error with fresh final cleanup errors and then
releases its inner readState. The snapshot owner retains its existing lifecycle
lease until Snapshot.Close. A second Close returns nil. Bounded commitment
poisoning and Close error behavior remain unchanged. Merge-value closer errors
can now surface immediately after each callback rather than at the next iterator
positioning/Close; the value remains valid throughout its callback.

## Metrics and current adapter premise

Unbounded Get wrappers publish logical cursors/calls/hits/errors/nanos under
`blockbuffer/state_prefetch/pebble/get/`, once at wrapper Close. Lookup nanos
exclude callback time. Discarded and final inner cursors each publish their SST
request/open/reuse/fallback/eviction counters exactly once under
`blockbuffer/state_prefetch/pebble/get_cursor/`. `inner_cursors` counts those
inners, and `inner_retained_peak_sum` sums their individual peak handle counts;
it is neither a wrapper simultaneous peak nor a pinned-byte measurement. No
synthetic zero Iterator samples enter the old iterator metrics. Commitment Get
counters retain their existing namespace and meaning. CPU profiles of viewGet
now include both PrefetchBatch and commitment callers and need caller splitting.

Iterator read sampling can trigger compaction while Get does not sample reads.
The production adapter already sets ReadSamplingMultiplier=-1. The acceptance
fixture sets the same value. There is no claimed extra throttling/compaction
benefit; changing that premise requires evaluating this behavioral difference.

## Evidence and limits

Raw probes, overlays, commands and outputs live in the ignored module
`build/benchmarks/20261016-read-write-amplification/point-read/`. The final
strict-01..04 sequence is ABBA, five iterations per case per process, with no
other local Go workload. It overlays frozen baseline/candidate runtime sources
and copies one identical sealed physical image for every operation. Earlier
logical-image/default-sampling trials are diagnostic, not this acceptance set.

The image contains 32768 530-byte values, six L0 and five L6 SSTs, exceeds both
1 and 4 MiB caches, and includes deep-only values, upper Set/delete/range
tombstones, cross-level merges and exact misses. Independent expected bytes and
presence validate four blocks of real overlay -> sorted PrefetchBatch ->
foreground cached/unpredicted reads -> CommitBlock -> canonical FlushUpTo.
Warm cases use the same complete ordinary-Get warmup, larger than cache, and do
not imply every block is resident. Both arms use the same compaction-disabled
image fixture; it isolates lookup work, not production compaction throughput.

| Cache / warmup | VFS ReadAt/op old -> new | VFS bytes/op old -> new | time change |
| --- | ---: | ---: | ---: |
| 1 MiB / cold | 3242.5 -> 1895.5 | 36284330 -> 31326679 | -7.2% |
| 1 MiB / warm | 3321 -> 1896.5 | 36693394 -> 31412230 | +1.9% |
| 4 MiB / cold | 2593 -> 1534 | 9901428 -> 5841766 | -8.0% |
| 4 MiB / warm | 2668 -> 1505 | 10254711 -> 5786083 | -2.4% |

Short/dense controls are retained in control-*.txt and control-summary.json.
They expose a real tradeoff: a single-level dense 256-key trace performs nearly
identical I/O and Get is slower (32-byte values +329%, 530-byte values +94% in
these short ten-iteration controls). Eight-L0 dense traces reduce reads about
58% but take +58%/+21%; sparse 530-byte traces reduce reads 40–72% and time
15–47%. Sparse 32-byte/single-level time also regresses with no I/O gain. These
micro timings are short and noisy, but must not be presented as universal CPU
improvement. The complete mixed sparse pipeline is the primary local gate.

VFS calls/bytes are filesystem API traffic, not physical device I/O. Local
memory/image reads are microsecond scale, unlike the measured server read
latency; neither local wall time nor VFS reductions establish mainnet TPS/BPS
improvement. Native post-deploy samples must independently check real workload,
read latency, memory and throughput. Retaining four SST iterator pairs bounds
handles, not bytes; it can pin blocks/readState outside cache accounting as the
existing bounded GetCursor does.

## Regression coverage

Independent MVCC expected values cover both bounded/unbounded cursors, empty
values, point/range deletion, merges, misses, old snapshots, newer mutable
writes, flush and compaction. Fault tests exercise read failure then success,
miss or Close; engine ReleaseValue failure; actual retained SST sequential
read-handle Close failure; combined callback/release/fresh-cleanup errors;
callback panic; retry at an old snapshot across flush/compaction; borrowed-value
lifetime; and once-only resource/metric publication. Real PrefetchBatch tests
cover callback-ignored middle/final failures, continued later rows, final Close
error and successful foreground lookup without negative-cache pollution.

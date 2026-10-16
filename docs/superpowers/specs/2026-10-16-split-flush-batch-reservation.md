# Hot/history flush batch reservation

2026-10-16, based on 9c1f7ecc. The production allocation profile attributes
nearly all Batch.grow bytes to DirectHistoryStore's hot batch reached from
blockbuffer's coalesced sorted flush. Blockbuffer already reserves its final
encoded size, but NewBatchWithSize currently assigns the entire hint to history
and lets hot grow geometrically.

Add an optional NewSplitBatchWithSize(hotSize, historySize) factory to
DirectHistoryStore. After selecting the final coalesced operations, blockbuffer
classifies their existing physical keys with rawdb.IsDirectHistoryKey and sums
exact Pebble point-record sizes. Each nonempty engine receives its own header
and existing temporary-varint slack; an empty side uses NewBatch without a large
reservation. A single-layer group excludes already durable writes exactly as
its writer does. Ordinary backends keep the existing single size hint. Groups
with range tombstones keep the existing generic hint because routing can split
one tombstone at multiple schema boundaries. This is a reservation optimization
only: writing, history-before-hot synchronization, Reset/Close, failures, reorg,
key ordering, persisted bytes, roots, and all read/cache semantics are unchanged.

No cache or pool capacity changes, arena pooling, or mutable borrowing are
introduced. Immutable branch arena bytes remain owned by the layer. Final maps
remain pinned by the existing flush protocol; single-layer shard inspection uses
existing read locks. Allocation sizing does not publish or mutate any operation.

Validation uses paired fixed-input complete 64-partition ordered pipeline,
root oracle, eight layer commits and dual-Pebble durable flush. The existing
1536-key clustered fixture activates only one of the configured 64 owners and
its branch writes are all hot; it does not represent 64-owner production
throughput or mixed history synchronization. Add dual-Pebble
flush cases for hot/history dominance, single-side writes, overwrites, and point
deletes, along with exact sizing and range/single-backend fallback checks.
Existing direct-history failure order, retry/reorg/snapshot and concurrent flush
regressions remain required. Save raw measurements under
build/benchmarks/20261016-branch-alloc and report B/op, allocs/op and full-pipeline
time, including regressions; local small inputs are not a mainnet throughput
forecast. Native Sapling, independent audit and production acceptance are handled
by the deployment workflow.

The audit also reproduced an existing range-tombstone correctness bug: the
initial grouping window stopped at ranges, but its overwrite-driven extension
loop could cross one after admitting a point layer. Add the same range guard
to that loop without changing limits. The regression fills the normal 32 MiB
source window with overwritten points, then verifies an unseen durable key is
deleted by the following range while the post-range point survives. Both the
generic and split factories fail this regression before the guard.

## Fixed-input measurements

Final same-binary paired runs used Go 1.25.5 on darwin/arm64, 15 operations per
sample and five samples per arm. Mixed and hot/cold_scan/scan cases ran in the
exclusive CPU window; the final delayed_read samples may overlap the root
agent's later full-repository test and are qualified accordingly.
The baseline wrapper hides only the optional split factory; all physical
routing, durable writes, caches and update inputs are otherwise identical.
Raw output, commands, method and min/median/max values are in
`build/benchmarks/20261016-branch-alloc/{METHOD.txt,summary.json,summary.txt}`.

| Complete operation | Median B/op change | Median time change |
| --- | ---: | ---: |
| Four-layer dual-Pebble hot-only flush | -31.36% | -0.15% |
| Four-layer dual-Pebble history-only flush | +0.85% | +2.03% |
| Four-layer dual-Pebble hot-dominant mixed flush | -31.78% | +2.89% |
| Four-layer dual-Pebble history-dominant mixed flush | -3.57% | +2.86% |
| Four-layer dual-Pebble balanced mixed flush | -11.41% | -5.69% |
| Eight-block root-oracle hot pipeline | -4.20% | +14.73% |
| Eight-block root-oracle cold_scan pipeline | -4.40% | +3.56% |
| Eight-block root-oracle scan pipeline | -19.52% | +4.36% |
| Eight-block root-oracle delayed_read pipeline | -14.85% | -5.61% |

Every pipeline block matched its independent root oracle. Allocations counts
changed only slightly; the benefit is fewer large backing-array allocation
bytes. Mixed cases execute actual history Write -> SyncKeyValue -> hot Write.
These samples do not establish an overall throughput improvement. In particular,
the final hot pipeline time rose from median 15.744 ms (14.527–17.447) to
18.064 ms (16.589–19.142); an earlier exploratory run showed +3.1% instead.
The final arms ran five baseline samples followed by five split samples;
order/drift needs a reverse or interleaved control before attributing the
entire timing difference to the extra map scan. History-only has no hot grow
to remove and pays classification overhead. Preserve these controls and report
regressions alongside allocation savings during production acceptance.

After the full-repository test finished, eight additional independent hot-only
invocations ran 50 operations each in ABBA/BAAB order, with A hiding the factory
and B exposing it. Time medians were 18.968 -> 19.104 ms (+0.72%), with ranges
18.530–19.441 and 18.453–21.028 ms. The four adjacent direction-controlled pairs
were -0.42%, -1.20%, +11.77%, -0.64%; the slow third pair is retained. B/op medians
were 2,245,544 -> 2,095,988.5 (-6.66%) and each pair saved 3.93–9.33%; allocation
counts changed -0.14%. These controls did not consistently reproduce the earlier
+14.73% timing increase, while allocation savings persisted. They support an
allocation optimization, not a CPU speed claim or a promise of no slow samples.
All eight raw outputs, timestamps/order and exact summary are saved as
`focused-hot-*.txt`, `focused-hot-order.json`, and `focused-hot-summary.json` in
the evidence directory. Production acceptance must still inspect throughput,
GC, heap/RSS, errors and write stalls on comparable chain windows.

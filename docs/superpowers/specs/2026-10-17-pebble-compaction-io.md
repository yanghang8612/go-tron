# Pebble transient-level compression investigation, 2026-10-17

## Problem and boundary

The current wrapper explicitly writes L0–L5 without block compression, keeping
Snappy only at L6. The original policy reduced codec CPU while initial sync was
CPU-bound. The recent production observation instead has substantial device
read/write traffic and CPU headroom. This justifies testing lossless Snappy on
transient levels; it does not establish that a change is beneficial.

Compression can reduce physical table bytes read and written. It does not
itself remove logical updates, change snapshot stripes, or guarantee fewer
compactions. The existing iterator already elides superseded versions within a
snapshot stripe. It cannot indiscriminately discard equal SET values across
snapshot boundaries, and the published snapshot-pinned byte/key counters are
cumulative work tallies rather than currently reclaimable disk bytes.

No durability, WAL sync policy, cache/memtable budget, file-size target, base
size, compaction threshold or concurrency is changed in this investigation.
The previous 2 GiB base-level experiment accumulated debt; it is not repeated.
The rejected CLOCK cache prototypes are unrelated and remain rejected.
No production DB is compacted, scanned or rewritten by this experiment.

## Controlled experiment

The ignored evidence directory is
`build/benchmarks/20261017-compaction-io-followup/`. It contains a frozen
real-Pebble test, the exact local vendored sources, scripts, compiler settings,
and raw output. Both arms use identical public Pebble operations, values, keys,
WAL durability, cache/memtable budget and compaction limits. Only L0–L5
compression differs: current `NoCompression` versus `SnappyCompression`.
L6 remains Snappy in both. Inputs and cleanup operations are hashed and
byte-counted independently of engine counters.

The main data format follows `BranchData.Encode` in
`core/state/domains/commitment_tree.go`: a big-endian uint16 presence mask and
kind 0 hash children or kind 2 path leaves. Values contain 2, 4, 8 or 16
children, full-entropy SHA256 hashes and paths, and one changing child per
round. No zero-padded hashes or repetitive padding manufacture compression.
A same-length high-entropy value workload supplies a compression counterexample.
The generator is an encoding-shaped fixture, not a replay of real node state or
a proof that its frequency/working-set distribution matches production.

The test performs 20 rounds of 16,384 keys, including identical-value writes,
point deletions, range deletions, explicit flushes and retained historical
snapshots. Every value and absence is checked against an independent expected
map through both DB Get and historical snapshot Get. Snapshot validation occurs
before and after a complete manual compaction. After snapshots close, both arms
write exactly the final key set again, including deletion markers, and compact
it through L6. This cleanup cost and its logical input bytes are reported.

That final rewrite is required: public `Compact` stops before compacting an
already-bottommost table. Merely closing snapshots and invoking `Compact` again
can leave historical versions in L6 while estimated debt is zero. The first
pilot exposed this trap; its output is retained separately and must not be
used as a fully reclaimed comparison.

Reports distinguish natural background quiescence, manual compaction with
snapshots, compaction after release/cleanup, and random reads after reopening.
The final state must have zero estimated debt, no active compaction/snapshot,
and all expected bytes must survive reopen. Each phase includes per-level live
size, actual `.sst` file sizes, compaction input/output, flush/WAL bytes,
read amplification, write stalls, physical VFS ReadAt traffic, process user/sys
CPU, wall time and Go allocation totals. Final random reads have separate VFS
counter deltas. Cumulative VFS traffic in earlier phases includes oracle reads;
engine compaction input is Pebble's file-accounting counter, not a direct
measurement of device reads. Go allocation metrics exclude native allocations.

Fix Go 1.25.5, CGO enabled and GOMAXPROCS=2. Execute three serial alternating
AB/BA/AB rounds locally for screening, then the identical frozen fixture on
Linux/amd64. Results on Darwin/arm64 do not establish target-machine performance.
Report counterexample CPU cost, trial variation and final debt/file state, not
just the most favorable compressed-byte ratio. Long-retained snapshots are an
MVCC stress case and may overstate cross-version compression opportunities for
short-lived production snapshots.

## Local result and decision

Three serial alternating rounds per workload ran on Darwin/arm64 with Go
1.25.5, CGO enabled and GOMAXPROCS=2. The frozen parent module copies the
application's complete `go.mod`/`go.sum` and redirects only its local Pebble
replacement. In particular it selects production Snappy v1.0.0, rather than
the older v0.0.4 selected when the vendor directory is treated as a standalone
main module. Earlier older-graph pilots are retained under `old-dependency-*`
and are superseded, not used for this decision.

The primary workload closes each snapshot after two rounds, checking its
complete historical contents first. Median total flush-plus-compaction bytes
were 207,224,870 versus 203,629,902 (-1.74%). Paired changes were -0.92%,
+1.39% and -6.07%, an inconsistent small-fixture result. Median lifecycle CPU
increased 1.74%, while wall decreased 4.10% with a slower first pair. Every
operation/cleanup hash and WAL byte count matched. Both arms finished with
15,989 internal entries equal to the independent live-key count, zero debt,
zero active snapshots/compactions, read amplification 1 and no stalls. Actual
final SST sizes were 5,305,892 in baseline and 5,305,814–5,305,892 in Snappy;
the equality guarantee concerns returned data, not byte-identical table layout.

The natural intermediate-level probe executes the identical 32,768-query trace
under the 1 MiB cache. Median VFS read bytes decreased 26.57%, with essentially
equal wall (+0.04%), but baseline read amplification varied 4/6/3 against
6/6/6 in Snappy and substantial natural debt remained in several trials.
Each arm's flush bytes stayed about 113 MiB with less than 0.03% difference,
consistent with the branch values being almost incompressible. Different
natural compaction shapes prevent attributing the read-byte difference to
compression. Final L6 reads validate persistence but do not establish
transient-level read cost.

The long-pinned safety workload retained two historical snapshots for most of
the 20-round run. Median SST writes decreased only 2.49%, CPU decreased 5.09%,
and wall increased 2.83%. The same-length high-entropy counterexample increased
SST writes 1.70%, CPU 7.15% and wall 4.47%. These are descriptive ratios of
three medians, not confidence intervals or target Linux measurements. The real
application decoder and encoder round-tripped all 1,280 generated branch
fixtures through a source overlay.

Reject the default change: the representative short-snapshot workload does not
establish useful total I/O reduction and the entropy counterexample regresses.
Keep the existing compression policy. No Linux result, online throughput
improvement, production rewrite or deployment is claimed. Frozen sources permit
reproduction, but this rejected prototype is not sent for a native trial.

## Gate for reconsideration

No production default changes in this investigation. Require useful target-machine physical
I/O reduction with bounded CPU/wall/space cost and equivalent values, durability,
final debt and historical snapshot behavior. A candidate that only postpones
work is rejected. Production configuration changes require the owning task's
review and integration checks; this harness does not deploy anything.

An online before/after window must retain the same process/resource identity,
track block/transaction mix, mature sync and cold progress, compaction/flush/WAL
bytes, per-level occupancy/debt, SST ReadAt calls/bytes, stalls, cache occupancy,
CPU/iowait and cgroup memory. Existing files are not rewritten at activation, so
compression policy transitions gradually as ordinary flushes/compactions occur.
Operational observations cannot isolate causality as tightly as the fixed
workload experiment.

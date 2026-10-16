# History-only source snapshots for the history index stage

The normal direct-history cold builder already pins only its history engine.
The separate history-index ETL stage still called `NewReadSnapshotThrough`,
which pins both engines for the complete pass. That ETL consumes changesets,
shared chunks, tx ranges, postings and directory rows. It obtains canonical
hashes separately through `bc.readCanonicalHashStrict` and revalidates the
watermark before publication. Its unused hot snapshot can therefore retain an
old hot MVCC stripe while state and commitment branches change.

Add the explicit `Buffer.NewStateHistoryReadSnapshotThrough` acquisition path.
It captures the same committed layer prefix and durable base under the existing
`flushMu` and `mu` order. It preserves immutable layer pointers through a
concurrent flush, excludes inflight and newer committed layers, and uses the
existing `AcquireStateHistoryBuildReadView` partition-aware base factory. A
history-only `ReadSnapshot` rejects hot point keys and ambiguous, mixed or hot
prefix ranges before looking in its overlays. This also prevents a hot row
present only in an overlay from escaping the history-only contract. The index
stage selects it only on its normal, unstaged path.

General snapshots and the staging route keep their existing semantics. Single
engine sources retain a generic MVCC snapshot. Ordinary stores that cannot pin
MVCC return `ErrReadSnapshotUnsupported`, preserving the index stage's existing
fully locked fallback. An error from an explicit history partition factory,
including unsupported, nil and unpinned results, remains a real acquisition
failure; it cannot be reclassified as an ordinary backend's missing capability.
Cleanup failures also cannot trigger a moving-source fallback. The stage must
close its source before publishing progress, as before.

The strict acquisition failure guarantee above applies to explicit history
partition factories. Generic stores inherit `AcquireStateHistoryReadView`'s
legacy unsupported-error behavior, including errors joined with unsupported;
this change does not add a new fail-closed guarantee for that generic path.
Custom partition factories must report storage errors rather than returning
Buffer's caller-private fallback sentinel themselves.

No write grouping, sync setting, history retention, database format, branch
encoding, root, pruning authority, canonical boundary, cache budget or worker
count changes. This removes one unnecessary hot MVCC pin. It does not remove
historical snapshots, promise a history-engine compaction reduction or establish
an online throughput percentage.

Validation covers the full direct-history index caller's success, interruption,
factory error, unsupported and joined errors, Close failure and progress rules;
failed partition acquisition must not open a moving-source iterator. Buffer
checks cover overlay cutoff, continued foreground publication and flush, old
history values through compaction, all read/seek scope guards, unchanged general
snapshots and the ordinary non-MVCC fallback.

The ignored same-image experiment runs production ordered BranchData folds,
real DPP1 changesets and the actual serial history-index ETL. It fixes the
logical interleaving through the ETL cancellation callback in both arms so its
source remains alive through the same foreground blocks and physical drain
points. Every foreground root is checked against an independently generated
serial staged-store oracle; old source history, final hot/history digests and
reopened values/roots must agree. A post-close full current-hot-image rewrite
then flush/compact forces retained bottom-level old stripes to meet newer values;
SST properties must contain exactly one entry per surviving logical hot row and
no deletions. WAL, flush, compaction and actual VFS counters are separate. The
128-entry DPP1 pages are below the shared-chunk eligibility threshold, and the
chunk cache records zero activity; this experiment does not establish a shared
chunk decoding or cache benefit. The
small cache/memtable and fixed manual compaction schedule measure this mechanism,
not production natural-settle service rate or device traffic. Preserve all
initial screens, latency/allocation negatives and physical endpoint checks.

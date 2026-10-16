# History-only snapshots for normal cold-source builds

Normal direct-history cold-source acquisition previously took a general KV
snapshot of both Pebble engines. The cold history view uses only the history
partition, but its unused hot snapshot can keep one old MVCC stripe alive while
foreground state is rewritten. Subsequent compactions may repeatedly emit that
old stripe. It does not retain every intermediate version. Removing the unused
hot pin targets hot rewrite work; it is not a reduction in history-engine
compaction or proof of higher cold service throughput.

Add an explicit `StateHistoryReadSnapshotter` capability and
`AcquireStateHistoryBuildReadView`. `DirectHistoryStore` acquires only its history
snapshot under the existing mutation mutex. History Write → Sync → hot Write
ordering and its lock remain unchanged. Factory errors, unsupported errors, nil
results and unpinned results cannot fall back to a moving view. Unpinned results
are closed and cleanup errors are joined. The known `stateHistorySnapshotDatabase`
facade forwards acquisition to its underlying store; it owns no overlay or
independent read semantics. Ordinary single-store sources continue through the
existing generic snapshot acquisition.

The dedicated direct-history view fails closed on non-history `Get`/`Has` keys.
Prefix iteration and seek require an entire prefix range provably within a
history family. Empty, mixed, hot and ambiguous singleton-stage prefixes return
`ErrStateHistorySnapshotScope`, rather than silently omitting hot rows. Point
reads of the history-index progress singleton remain permitted. Owned-value and
concurrent-read capabilities are forwarded only from the history snapshot.
Workers must join before Close, as before; Close releases only the acquired
history snapshot and returns its error.

Only the normal, unstaged branch of `BlockChain.AcquireStateHistorySourceView`
selects this API. The staging-route branch and general `NewKeyValueSnapshot` /
`AcquireStateHistoryReadView` remain unchanged. General exports and diagnostics
can need canonical block bytes in hot and retain their dual-engine contract.
No format, migration, generation, checkpoint, reorg, pruning authority, cache
budget, compaction option, worker count or history retention policy changes.

## Source-call audit

The returned cold-source `historyDB` is used for these operations:

- Cutoff, lower-bound search, recovery reconciliation and bounded tx-range table
  reads use `state-tx-range` keys. Stage progress and canonical boundary hashes
  use the separate original `db` argument.
- Dictionary/record passes use changesets, shared chunks and bucket metadata;
  posting/directory reads, when selected, also belong to the history partition.
- Chunk-cache/pipelined wrappers borrow this same pinned history sequence and
  complete before source release. They do not acquire a newer underlying view.
- Parallel event construction captures the separately acquired `chainDB` and
  takes its own hot/ancient/cold-sidecar view; it does not consume `historyDB`.
- Manifest pinning follows KV acquisition under the existing chain/publication
  coordination. Publication writes files before hot-history pruning, so old
  history KV plus newer manifest coverage preserves the existing ordering.
- The pruner adapter consumes this source for historical coverage/tx-range
  checks, not current-state or canonical-block reads.

## Validation

Use independent old/current expected values across writes, flush and compaction,
all history families and forbidden hot/mixed-prefix reads, generic dual snapshots,
facade forwarding, nil/unpinned/unsupported/factory/Close failures, and acquisition
blocked behind a half-finished mixed batch. Preserve existing durability and
history-reader regressions. Exercise cancellation and DB lifecycle through the
real cold-source caller. A complete same-seed probe must include source acquire,
real cold build, concurrent foreground rewrites/history append/prune, source close,
and equal final state/history/file oracles. Separate fixed manual-compaction
experiments from natural-settle measurements. Include a post-close write/cleanup
phase: a bare bottom-level Compact may do nothing, and is not evidence of a
physically drained endpoint. Report hot/history metrics separately and preserve
negative latency/allocation/memory controls. VFS bytes are not device IO.

The normal Manager wrapper retains its pre-existing capability surface. This
patch does not add owned/concurrent capability forwarding or enable parallel
history reads. The measured baseline explicitly requests workers=0, chunk cache=1
and reports fallback reason 0; its serial authenticated cache does not need the
parallel capability. Regression tests use a real published/pinned Manager and
verify the cold-file lease is released even when snapshot Close reports an error.

The Runner's outer history-source release occurs when the complete function
returns. A nested borrowed view's release is a no-op. Preserve that existing
ordering: an outer Close error can be returned after a manifest has already been
published. This change does not promise that every snapshot Close error prevents
publication, nor does it move publication or source-release boundaries.

## Rejected history compression candidate

A separate experiment compared existing L0–L5 `NoCompression` with Snappy for
history only; both arms retained L6 Snappy. No compression option is changed by
this patch. Four deterministic workloads used the production CDC block-pack,
shared-chunk, tx-range and indexed range-pruning accessors: schema-addressed DPP1
pages with 128 nonzero high-entropy addresses, 32-byte storage slots/previous
values, valid constructed ABI protobufs, and a stated mixture. The ABI had 4096
entries and one changed entry per block; it is a constructed stress case, not a
mainnet trace. Each pair opened copies of the same 64-block SST image, appended
32 blocks, pruned the 64 indexed seed packs, checked an old snapshot, released it,
and completed the same flush/manual-compaction endpoint. Reopening compared
every surviving row and previous value against the independent input. WAL bytes
and complete logical history digests were identical in each pair.

| Input | Flush bytes change | Compaction read bytes change | Compaction write bytes change | SST VFS read bytes change | SST VFS write bytes change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Delegation pages | -0.046% | -0.001% | -0.496% | -0.002% | -0.047% |
| Storage | -0.254% | -0.085% | 0.000% | -0.085% | -0.128% |
| Constructed ABI/shared chunks | -0.090% | -0.087% | -0.174% | -0.087% | -0.153% |
| Mixed | -0.040% | -0.013% | -0.087% | -0.011% | -0.068% |

Mixed logical previous-value bytes were 61.8% pages, 5.6% storage and 32.6% ABI;
ABI was inserted once per eight blocks. All 196 ABI-only shared-chunk values and
47 of 48 mixed shared-chunk values already used the production inner Snappy
codec, which selects compression only when smaller. Additional SST compression
therefore gave negligible net I/O savings in this screen. The mixed arm used
270.4 versus 266.2 ms process CPU, 548.3 versus 549.1 ms wall time, and its commit
p95 rose 2.8%; these short fixed-order samples do not establish CPU/latency gains.
The compression candidate is rejected rather than adding an option for this
small measured effect.

This was a bounded causal screen with scaled 4 MiB memtables, 8 MiB caches,
128 KiB initial table targets, one compactor and automatic compaction disabled.
It measures actual file calls and engine byte counters, not device traffic or
production natural-settle throughput. Trivial moves can retain a table's
existing codec in deeper levels. Heap samples were not RSS/native-memory peaks.
The raw command, source, binary, immutable-image and result hashes are preserved
under `build/benchmarks/20261016-sst-history/history-compaction/` in
`compression-summary.json`; the snapshot candidate remains independent of this
rejected compression experiment.

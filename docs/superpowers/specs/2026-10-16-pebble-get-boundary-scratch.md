# Pooled Get boundary scratch

Baseline: `10f5937a572cb9764e107d9ba07d0d05c2c199a4`, Go 1.25.5.
This patch changes allocation only. It does not change cache budgets, the
four retained SST iterator pairs, keys, persisted formats or consensus.

## Lifetime and correctness

Pebble's getIter searches one memtable or LSM level at a time. Its single
embedded levelIter previously borrowed a newly allocated boundary context for
every visited level. Embed one context in the existing pooled getIter instead,
and clear it at both L0 and non-L0 initialization sites. The preceding
levelIter is closed before another level opens. No retained SST point or range
iterator borrows the context. A cross-level tombstone Span keeps its original
range-iterator lifetime; clearing context slice headers does not mutate that
Span or the file metadata they describe.

Iterator.Close still owns cleanup, readState references and value closers. It
clears the whole getIterAlloc before pool return. Neither GetCursor nor this
patch accesses the returned allocation afterwards. Ordinary DB.Get,
Snapshot.Get, indexed Batch.Get and GetCursor share the same resolver and this
scratch allocation change. MVCC visibility, lazy level traversal, merge/range
delete precedence, misses, iterator bounds and error precedence are unchanged.

## Evidence and limits

The October 16 steady CPU sample separates commitment viewGet (21.63 of
284.95 CPU seconds) from unbounded state-prefetch iteration (7.91 seconds) and
ordinary DB/Snapshot getInternal (9.72 seconds). Their work must not be added
to overlapping callers. The two boundary-context allocation sites account for
267 MiB of the 31.43 GiB delta allocation sample. This is a bounded allocation
target, not a claim of equivalent node throughput improvement.

Ignored same-input source overlays under
`build/benchmarks/20261016-random-state-branch/point-read/` retain the original
and candidate source, SHA manifest, commands and raw results. A fixed-key
32-byte-value fixture uses real DB.Get in its reference arm and GetCursor in
its other arm. At eight overlapping L0 tables and 256 reads, context reuse
removes per-level allocations: DB.Get changes from approximately 103 kB and
1,601 allocations per session to zero; GetCursor changes from approximately
105 kB and 1,608 allocations to 1.5 kB and seven. Table open/reuse/fallback
counts and VFS reads remain equal. Snapshot.Get and GetCursor share getIter,
so their differential tests alone are not an independent resolver oracle.

The existing eight-block seedSST commitment fixture checks literal expected
roots through fold, publication and flush in hot, scan and delayed-read modes.
All arms retain 36 VFS ReadAt calls per pipeline operation. Allocation counts
drop by roughly 500–660 per operation, while pooled byte allocations and
scheduling introduce substantial variation. These short local timing samples
are not isolated and establish neither mainnet TPS gains nor reduced device IO.

## Validation

Run the root pebbledb tests and nested Pebble tests explicitly; root `go test
./...` does not discover nested tests. The independent literal-value fixture
alternates range misses, cross-level merges, mutable writes newer than old
snapshots, indexed-batch changes and ordinary/cursor/merged-iterator reads,
then repeats after compaction with old cursors still pinned. Existing cursor
tests cover concurrent readers, borrowed-value callbacks, errors, exactly-once
cleanup and live leases. Run the focused tests with the race detector as well.
Keep UPSTREAM_FILES.json as original upstream provenance; document the patched
get_iter.go in GO_TRON_FORK.md rather than replacing its original SHA.

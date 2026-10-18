# Sync stage point-read termination

## Scope and evidence

The deployed baseline is 7ce7412c8a02a3c7340e96206e7f3e8b3a8c85a8.
The earlier saved production CPU profile at roughly275k blocks attributes
about76.36% cumulative CPU
to P2P staging readiness/progress lookup through DirectHistoryStore.Has,
Pebble.Get and Iterator.nextUserKey. This is CPU attribution, not physical SSD
read attribution. MuHash, source encoding, storage budgets, consensus and wire
behavior are unchanged. No database wipe or global stage cache is needed.
A later 3.4-million-block baseline shows stage-row reads at 1.55% cumulative
CPU and nextUserKey at 1.79%; the earlier 76% hotspot is not a claim about
current CPU share or promised end-to-end throughput improvement.

getIter is a private fixed-user-key iterator for DB.Get, Snapshot.Get,
IndexedBatch.Get and GetCursor. It returns visible point tombstones to the
public-iterator adapter, whose delete handling advances nextUserKey. That
advancement currently walks all older versions of the same key and older
levels even though the visible tombstone already determines absence.

## Minimal change

In findNextEntry's visible Delete, SingleDelete and DeleteSized case, return
exhausted immediately only when getIterAlloc is nonnil. This marks the two
private point-lookup constructors (DB getInternal and GetCursor), not general
iteration. Keep the iterator/key intact for existing Close and cleanup error
handling; no terminal flag, new state or changed getIter.Next behavior is needed.
Snapshot filtering and range-deletion coverage happen before a key is returned
by getIter. Ordinary forward/reverse Iterator and mergeNext are unchanged.
A merge still consumes a deletion as its normal base barrier.

ReadStageProgressRow will use the existing presence-coupled readPresentValue
helper. Production readers implementing GetWithPresence perform one lookup;
plain readers retain Has-first fallback/error semantics. Missing remains a
missing row, present empty remains malformed stage data, and storage errors
remain errors with errors.Is identity. Database.GetWithPresence and the
existing keyValueSnapshot.Has must classify only exact pebble.ErrNotFound as
absence; joined/wrapped storage failures containing that sentinel remain errors.
No global missing-error predicate changes. Stage hash/height validation, body-ready
frontiers, prune/reorg and restart recovery logic are unchanged.

## Validation and release boundary

First retain baseline real-Pebble benchmarks with repeated same-key SET
versions followed by a point deletion at fixed sizes 1,128,4096,65536. Keep
setup outside timed lookup work and account for allocations. Repeat the same
benchmark on the candidate. Add deterministic internal-step assertions to
prove no historical Next calls occur after the visible tombstone; benchmark
speed alone is not the complexity oracle.

Correctness covers all three point deletion kinds, snapshots before/after the
delete (including invisible newer tombstones), mutable/immutable and persisted
levels, indexed batches, merge/delete and range boundaries, cursor miss-to-hit
reuse, and cleanup failure/ownership. Ordinary iteration must retain identical
forward/reverse results. Accessor tests cover coupled versus plain read counts,
missing/empty/corrupt/error, and existing downloader hash-bound recovery.
Run focused normal/race and relevant vendored tests, then required repository
and native validation appropriate to the exact reviewed candidate. Keep prior
prototype freezes and original27 plus the additional document byte-identical.
Astra review and Root release/timer/lock coordination precede commit/push and
service switching. Production sampling is a separate fixed approved window.

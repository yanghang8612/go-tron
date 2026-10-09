# Online staging history-index GC

2026-10-09. This supplements the history-staging store and runtime scheduler
designs. It changes only optional reclamation of derived posting frames and
orphan directory rows. It does not change the history format, retention window,
canonical commit protocol, current state, payload/chunks, stage progress, or
cold manifest. No deployment or production throughput result is claimed.

## Problem and authority

With history staging installed, the legacy pruning worker deliberately skips
history payload pruning. Its successful-hot-prune permission and manifest
watermark consequently stop advancing. The existing full index pruner and
posting worker cannot supply continuous staging-aware cleanup. The offline
cleanup already removed the old backlog; the online worker primarily limits
new accumulation, with no promised rate or physical reclamation amount.

Use an independent staging index worker. It owns a private, process-local
authenticated prefix and independent posting/directory cursors. No caller-
supplied height, hot changeset miss, maximum observed COLD bucket, old
HotPrune progress, or offline Bloom filter grants permission.

Only start authorization after successful runtime readiness. Starting at bucket
1, examine at most 256 consecutive route records per pass, also stopping at the
cooperative work deadline. A bucket extends the prefix only if its route is
current-epoch COLD, its complete binding matches the route's binding epoch,
its semantic/transaction commitments are nonzero, and it has no current-epoch
claim. Missing routes, SOURCE/TARGET owners, incomplete bindings and claims
stop at the hole; the same bucket is revisited on later passes. Bucket zero is
never certified by this worker.

Runtime readiness authenticated existing durable bindings. New COLD ownership
requires semantic equivalence and durable certification in the existing manager
protocol. Within one epoch COLD ownership cannot return to SOURCE/TARGET;
rebinding preserves semantic equivalence. Therefore a private prefix can grow
incrementally without rescanning old certified routes on every delete chunk.
The private authority is bound to the constructor manager pointer and store
identity; a missing/replaced manager requires a fresh worker. Epoch/reset
changes or a changed canonical prefix anchor invalidate authority
and both cursors. No prefix or cursor is persisted.

## Guard and resource admission

Every bounded delete chunk obtains index then chain writer guards and rechecks
runtime readiness, epoch/reset state, its private prefix and canonical anchor,
strict hash-bound Finish and StateHistoryIndex, solid height, pending commit/async flush
errors, and HistoryPrefixSettled through the prefix. It verifies the current
endpoint route/binding remains COLD and complete. A changed authorization
clears traversal; temporary unavailability leaves cursors unchanged.

Resource admission occurs after writer locks are acquired: use cached engine
and device pressure, then a nonblocking shared heavy-work lease. Unknown/stale
pressure, hard engine pressure, a busy lease, and unsupported required read
capabilities defer work. No wait for a heavy lease or proc/device probing occurs
inside the writer guards. The worker releases its lease and snapshots before
unlocking and waits for its next cadence.

After each pass, wait max(configured interval, nine times the complete pass
duration), cancellably. Slow mandatory proofs therefore reduce frequency and
keep sustained guarded duty at about ten percent or less; one individual IO
operation still has no hard latency bound. Defaults use a 100 ms minimum wait
and at most 10 ms of cooperative work per
guarded chunk. Limits cover scanned rows, scanned encoded key/value bytes,
logical delete bytes and elapsed work. Checks run between complete rows.
Each pass admits at least one metadata candidate. If required proof or snapshot
capture exhausts the cooperative budget, raw scanning is limited to one complete
row with the scanner's first-row overrun policy. Slow mandatory proofs therefore
cannot permanently starve actual traversal. Individual canonical lookup, row seek,
snapshot acquisition or batch write may
exceed the target; there is no hard 10 ms latency guarantee. Snapshot capture
uses maintenance-only TryNewReadSnapshot: flushMu.TryLock failure returns a
typed busy deferral before directory scanning or cursor changes. Success follows
the original flushMu -> buffer.mu -> base snapshot capture. Only flushMu
acquisition is nonblocking; buffer.mu and base snapshot creation can still wait.
This preserves one
atomic view rather than constructing separate raw and overlay snapshots.

The first posting traversal waits until the metadata prefix scan reaches its
first hole or currently eligible end. Each full posting-plus-directory sweep
then fixes its cutoff/anchor even if the certified prefix grows meanwhile.
After completing both phases, another sweep starts only when the prefix has
advanced. This avoids rescanning the entire hot index for every 256-bucket
metadata page or repeatedly sweeping an unchanged cutoff.

## Posting deletion

Scan only schema-owned raw posting keys with an exclusive in-memory cursor.
Strictly validate every frame's encoding and ordered block numbers. Delete
only whole frames whose first block is at least 1024 and whose last block is
at most the private certified prefix. Retain bucket-zero frames, frames
crossing either boundary and all later frames. Malformed rows abort the chunk.
One bounded batch is written after releasing its iterator. Advance the cursor
and committed deletion counters only after a successful batch write. Errors,
cancellation and ambiguous writes retain the input cursor for idempotent retry.

## Directory deletion and concurrent writers

Use a separate bounded raw directory iterator for candidates. Do not create
an iterator over the entire directory prefix on a buffer snapshot: that can
materialize the full overlay. For each candidate, derive its exact posting
hash with schema-owned helpers and seek only its first visible posting through
one pinned raw-plus-buffer read view captured under the guards. The view must
include all committed and inflight layers; NewReadSnapshotThrough is unsafe
here because a newer posting protects the same directory key. Production must
use pointread.PrefixSeeker, not a fallback iterator which can materialize all
posting records for a hash. Any visible posting retains the directory.

The index guard excludes ETL publication, including its directory-first load;
an interrupted ETL has no pending asynchronous continuation. Canonical writes
are excluded by the chain guard. blockchain.go's foreground FlushFinal publishes
history/index before async enqueue. DomainChangeStage -> WriteHotHistoryIndex
-> writeStateChangePostingIndex always writes directory then posting in the same
canonical layer. Async Fold/finishCommitState subsequently writes commitment,
stage and metadata families, not posting/directory rows. Thus their contents
are stable while chainmu is held even if an inflight layer is promoted.
buffer.flushMu makes base capture atomic with whole-layer flush handoff.
Capturing topology alone is not a general freeze of mutable inflight contents;
this maintenance use relies specifically on the audited posting/directory
writer ordering. Raw-only absence, a pinned view excluding inflight, or an
unlocked two-pass live set is insufficient.

After exact current-view absence, delete only the raw candidate directory row,
using one bounded batch while retaining both writer guards and the read view.
Future canonical writes republish the directory with their posting. No directory
delete touches an overlay or historical payload. Snapshot/seek/iterator errors
abort without advancing the cursor; unsupported coherent views defer.

## Integration and observation

Wire the independent worker only for staging-enabled nodes and disable the
legacy full-index and posting-worker paths there. Preserve legacy behavior for
single-store nodes. The node lifecycle stops the GC worker before blockchain
and stores close. No new persistent markers or public deletion authorization
API are introduced.

Expose prefix/epoch, successful chunks/sweeps, posting and directory scanned
rows/bytes and deleted rows/logical bytes, errors, invalidations, and deferrals
for readiness, holes, writer contention, unsettled history, resources and
unsupported/busy snapshots. Logical deletion counters do not measure reclaimed
SST space. Global hash ordering still requires a full posting/directory keyspace
scan per sweep; bounded chunks do not bound the total index or guarantee cleanup
throughput. Production must measure scans, deletes, durations and contention.
Startup rereads metadata; partial traversals restart safely.

GC deletes only derived hot keys through MVCC batches. Existing routed readers
pin their hot base snapshot plus all overlay layers, staging sequence and cold
file/manifest leases. They retain their old posting/directory visibility after
GC; new views resolve historical payload through COLD routing. No physical
unlink or additional reader-drain permission is introduced.

## Verification

Use real Pebble fixtures for bucket-zero, crossing and cold-covered frames,
exact orphan removal and current posting retention, small-page progress,
invalid encodings, iterator/seek/write failure, cancellation and retry. Prove
directory protection for raw, committed-buffer and inflight posting layers,
flush races and future directory/posting republication. Test private prefix
stopping/revisiting holes, claims, wrong/incomplete bindings, epoch/reset and
canonical-anchor invalidation, readiness/Finish/Index/solid/settled rejection,
writer contention, heavy-work contention and unknown/stale pressure. Test that
canonical/current-state/payload/chunks/stages/manifest remain unchanged. Run
focused race checks for worker/flush/overlay interactions. Native Linux Sapling
release validation remains separate from local implementation evidence.

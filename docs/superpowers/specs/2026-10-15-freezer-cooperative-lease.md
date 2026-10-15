# Direct V2 freezer cooperative shared lease

The c1e77d5a baseline held one shared HeavyWorkGate lease while converting a
complete 65,536-block segment. Cold history's `HistoryGateDeferred` counted
failed gate acquisitions, independently of memory readiness. The captured log
reported completed freezing and transaction-index maintenance before cold
publication resumed; it did not report a precise job start or lease duration.

## Contract

Only the direct hot-source caller opts into cooperative checkpoints. V1
migration, bytes/schema, dictionary policy, full-segment manifests, fsync,
installation and publish-before-hot-prune ordering remain unchanged. A segment
has no partially visible coverage. An in-process checkpoint is not a durable
resume record: crash/shutdown still abandon unpublished files under existing
cleanup/recovery. Final publication, installation, fsync and hot pruning remain
uninterrupted and separately observed.

Preparation uses frame-aligned windows of up to 2,048 records (or one frame
when frames exceed that size). The last prepared Read joins its producer and
transform workers. At a window boundary the frame pipeline drains *all* queued
compression results before invoking a checkpoint; encoders are idle and retain
workspaces. The next producer starts only after resume. Dictionary sample reads
and verification have serial checkpoints with no borrowed input outstanding.
Compression, dictionary training, a single record/frame, fsync and cleanup may
be uninterruptible. The approximately ten-second work quantum is a soft time
bound plus the current window/operation, not a deadline or RSS guarantee.

Only fresh history contention (at most ten seconds old) triggers yielding.
The freezer releases its current lease, honors the existing recovery interval,
and gives ordinary history at least its three-second retry opportunity. That
opportunity starts only while CanTryAcquire reports no active owner,
reservation or recovery; another owner's lease/recovery resets it. Either an
observed history lease acquisition or an expired opportunity ends this wait.
History acquisition proves a lease handoff, not publication/progress. Resume
still requires a real acquisition, current promotion permission and all gate
admission checks. Pressure may prolong that context-aware wait indefinitely;
shutdown cancellation remains interruptible. No global queue or guard bypass
is added. Same-Runner transaction-index work remains serial until the segment
job finishes; this change makes no mid-segment index fairness claim.

## Ownership and locks

Exactly one Runner owns each underlying Freezer. OnePass, CompactV2Once and
MaintainTransactionIndexOnce first TryLock workMu, returning a no-op on a busy
job, before any shared gate acquisition. Internal routines do not reenter it.
Normal ordering is workMu -> shared gate -> v2Migrate -> tailMutation ->
writeLock. During a checkpoint, only v2Migrate retains private build ownership;
writeLock and tailMutation are dropped *before* releasing the shared gate so
cold V1Tail/TruncateTail can proceed. They are reacquired in the original order
and direct layout plus mutationGeneration are checked before further work.
Actual mutation attempts increment the generation before modifying state,
including subsequently rolled-back writes, to reject ABA. No-op truncation
leaves it unchanged. Source rows are canonical solid hot rows which cold state
history pruning does not remove. Other migrations/index writers/Close cannot
enter v2Migrate; all online gate-owning callers are the serialized Runner.
Offline tools cannot open the same store's instance lock. Close takes no shared
gate and follows Runner cancellation/join. This is not a supported arbitrary
multiple-Runner/shared-store topology.

A rotating lease owner always releases the latest callback. Sync activation
which cancels a fused job during a yielded wait must reacquire under the new
non-canceled context before its unfused retry. Mutation mismatch, cancellation
or source/writer error cleans unpublished outputs and leaves hot data intact.

## Observation and verification

Bounded owner IDs: 0 unspecified, 1 V2, 2 history, 3 transaction index. Gate
metrics include owner/acquired-at, cumulative/max hold and rejection counts for
busy-or-reserved (including coordination lock contention), cooldown and
admission. Job active/start count/start/finish/duration are emitted before the
first yield. Fixed phases writing/verification/index/commit/prune/compact expose
actual held time, excluding yielded waits; job wall time includes those waits.
Yield counts distinguish actual history handoffs from opportunity timeouts.

Tests cover frame sizes 1/64/>2048, incomplete last preparation windows,
workers 1/8, all row identity, no reread from discarded lookahead, cancellation
and subsequent retry, real layout/rolled-back mutation rejection, multi-segment
raw API, actual cold gate/V1Tail/TruncateTail access, concurrent public Runner
calls, third-owner recovery, stale/no-longer-active contenders, and fused cancel
followed by a newly admitted unfused build. Focused race and whole related
packages are required. Fixed-input complete migrations compare the original
path, noncontended windowing and no-op yield overhead; they do not predict live
throughput or device I/O gains. Natural post-deployment observation must include
an actual full direct job and history handoff with retained-workspace memory.

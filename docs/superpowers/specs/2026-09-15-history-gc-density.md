# Independent shared-GC work and history batch density

The September 15 native restart showed 88 successful persistent verification
operations on 88 unique old history paths, with no repeats and no further
completions after 06:02:15 UTC in the observed log. This was finite startup
coverage warmup, not a manifest-generation cache invalidation bug. During it,
complete prune passes took about 7–8 seconds while new history builds took about
1.4–1.9 seconds. Shared GC checks up to four independently selected old buckets
per pass; its proof cost does not scale with the newly built history range.
Including it in the batch density sample caused unrelated warmup to shrink the
history batch. These observations do not establish stable post-warmup throughput.

Measure the entire existing `pruneHistorySharedChunks` call in Worker, including
metadata scan, old cold coverage checks, writer-guard wait and retirement. Its
interval is disjoint from current hot-history pruning and existing metadata
intervals. Forward it only after successful PrunePass completion into
`PassResult.BeforeMergeHistoryGCDuration`. Invalid or failed/unmeasured callback
bookkeeping retains the conservative whole-work estimate.

Subtract this measured interval only from adaptive history row-density work.
Validate nonnegative durations and metadata + GC <= the containing before-merge
phase with subtraction-based bounds; subtract the two disjoint intervals before
saturating sums. Preserve the existing metadata gauge and add `density_gc_work`
for the actual accepted exclusion (zero for invalid measurements). Zero-valued
new fields preserve legacy behavior. A timer-resolution-sized residual stays
positive so it cannot reset the initial batch. Output-byte limits, bounded 25%
growth, unknown/hard pressure behavior and failure shrink remain unchanged.

The complete lifecycle wall time, recovery cost, already established deadline,
GC safety checks, 64 metadata-row/four-bucket bounds, queued guard admission,
frequency, cursor, retirement and error policy remain unchanged. Independent GC
is not free, and its checksum work is not skipped. This change neither promises
that cached proofs survive restart nor increases any duty target.

Acceptance uses deterministic same-row-work replay with zero/8s/120s independent
GC, disjoint/overlapping/negative/overflow boundaries, a real Worker whose scan,
proof and guard each have an observed delay, real lifecycle forwarding, and
complete deferred-maintenance recovery/duplicate-completion invariants. Native
full-window throughput and disk behavior remain necessary after deployment.

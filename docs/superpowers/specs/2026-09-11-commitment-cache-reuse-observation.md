# Commitment flush-admission cohort observation

## Motivation

Depth 7 still spends most foreground durable work on nonresident branches. The
2026-09-11 baseline contains 781,444 depth-7 foreground durable reads in about
360 seconds, while 95.66% of depth-7 window promotions record only flush source.
This is not sufficient evidence to remove flush admission: fixed-input cache
experiments improve scan pressure but increase reads by 41.97% for a valid
read/flush/delayed-read workload. Existing cache policy and all byte budgets stay
unchanged while gathering actual cohort evidence.

## Scope and semantics

An optional cache-owner observer, enabled once at construction by
`GTRON_BASE_CACHE_REUSE_OBSERVER=1`, samples two separate depth-6/7 cohorts:
flush-only window promotion, and probation-triggered absent flush admission.
The latter uses the existing probabilistic fingerprint admission decision; it
does not prove that a previous read used the exact same physical key.

Each of 16 cache shards owns 128 fixed slots, with complete inline physical keys
up to 48 bytes. The observer retains no cache-entry, value, arena or key-string
pointers. Metadata is explicitly additional diagnostic memory, bounded to
512 KiB; the 512 MiB cache retained-charge budget and 80-byte entry remain intact.
A deterministic 1/64 key gate and an independent slot-index bit range avoid
gate/index correlation. Complete key equality distinguishes collisions and
physical generations. Repeated episodes of a hot key are not independent random
samples.

Hooks execute under existing shard writer locks on enrollment, actual capacity
retirement, deletion, oversize invalidation and clear. Ordinary hits receive no
new hook. At retirement, the observer records foreground/prefetch reference
source bits accumulated after enrollment. Reference calls include more than
actual cache hits and cannot be equated with avoided Pebble reads. This first
implementation does not observe post-eviction durable reads, first-hit time,
reuse distance, or which admission displaced another useful entry.

Collision, episode replacement, delete, oversize and clear are explicit censor
reasons. Unretired records remain live. For each owner/depth/cohort:
`enrolled = completed_capacity + censored + live`. No live or censored record is
classified as completed without subsequent reference. Low-frequency snapshots
may inspect bounded live samples under existing shard locks; metrics include
enabled state and an owner ID unique within the process. Deltas require both
the exact process-start identity and unchanged observer owner ID. Completion
ratios must be presented with censor/live counts because censoring is selective.

## Correctness and validation

Return bytes, missing/error semantics, cache versions, per-key invalidation,
overlay priority, queues, reference credits, adaptive sampling and budgets must
match observer-off execution. Preserve the global snapshot-version fill guard:
an epoch captured after a snapshot's key was changed cannot justify filling the
old snapshot value into the current cache.

Validate forced slot collisions and key generations, both cohorts and sources,
all censor paths, conservation, concurrent publication, clear/rebuild, fixed
metadata and unchanged entry size. Compare observer off/on on identical complete
operation traces, including retained charge and queue state. Benchmark hit and
churn paths with off/on; do not present diagnostic cost as cache-policy gain.
Run the blockbuffer/domain and relevant race tests, then native Sapling checks.

## Deployment

Use an isolated Git-pinned release, preserving the prior executable for rollback.
The only intended service-file changes are the executable path and an explicit
observer environment assignment. Verify effective and running environment,
observer enabled/owner metrics, process identity, head advancement, resource
limits, guards and holds. Rollback restores the exact original service bytes.
Collect raw metrics and report completed, censored and live cohorts separately;
do not change admission policy based on a source-bit percentage alone.

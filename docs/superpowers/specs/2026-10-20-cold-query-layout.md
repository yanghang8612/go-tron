# Cold history lookup layout

The user permits incompatible storage formats and a fresh genesis sync. Local
and fixed-copy work is authorized; production deployment, restart or deletion
remains outside this experiment.

The preceding fixed-copy experiment measured 563 MB of compressed header input
rehashing per deep historical call, with zero decoded-prefix cache misses.
4,609 history segments (4,440 original leaves) also impose thousands of paired
opens and negative dictionary searches. Enlarging the cache does not remove
either structural cost.

First isolate two causes before adding another persistent index:

1. Separate the first 128 logical history bytes from the 128 KiB body chunks.
   Split the original first chunk; leave later body boundaries aligned and
   their compressed bytes unchanged. This contains the canonical header,
   dictionary commitment, and backpatched
   transaction-range count. Keep virtual offsets and all logical bytes intact.
   Use existing V1/V2 independent checksummed frames. CDC boundaries also affect
   value deduplication and remain a separate measured policy. Do not weaken
   dictionary or frame validation.
   Right-size the footer table scratch buffer to its actual bounded table
   length. Transfer the descriptor already inspected by magic dispatch into
   the compressed reader; use explicit-offset reads, and close it on every
   failure. This also removes a path replacement race between the size gate
   and a second open. Keep per-open physical-byte authentication unchanged.
2. Compact existing small segments on a new derived copy using the existing
   canonical merge, verification and manifest publication pipeline. Compare
   query cost against both unchanged and prefix-only layouts. Record build
   resources and amplification; do not increase production compaction budgets
   solely from a successful offline query benchmark.

Keep original fixed input, previous evidence, and production immutable. Private
derived directories own their metadata and any rewritten output. Shared input
inodes are immutable and never opened for writing or cache eviction. Before any
OS-cold run prove that every advised inode is independent of the original.

Acceptance includes exact byte-stream round trips, independently checksummed
prefix and body corruption rejection, short/empty/cross-boundary streams,
backpatch/reset/cancel lifecycles, mixed full-owner/generation/TxNum/Seq history,
MuHash and executable snapshot restore regressions. Compare full HTTP eth_call
on the same head and oracle, separating warm and first/OS-cold measurements.
No assumption that compression format work alone solves cross-segment routing.

If compacting still leaves substantial cross-run negative lookup cost, the next
candidate is a key-oriented global routing index bound to exact covered
segment/accessor identities. A routing absence may only skip completely covered
immutable pairs; new or stale ranges must fall back. This is a design candidate,
not an implemented or authenticated absence proof in this change.

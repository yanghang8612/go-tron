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

The bulk canonical rewrite experiment was stopped after 34 committed passes
because of its construction cost. No complete coarsened-corpus query result is
claimed. Keep those partial outputs and receipts; prioritize exact routing.

## Exact routing for singleton system keys

The measured mixed batch includes dynamic properties and software fork votes.
Missing keys scan every future segment. Add a bounded exact routing sidecar for
the system account's SystemDynamicProperty and SystemForkVote namespaces,
ContractMetadata for all owners, and the system/zero account envelope and
generation keys. These are the measured preflight bottlenecks. Other keys retain
their existing readers. This is a first-class manifest segment; snapshots carry
and verify it with the other active objects. This is not a universal index for
arbitrary absent accounts or contract storage keys.

Each column binds the full immutable history/accessor pair identity (including
checksums, paths, ranges and aggregation steps), and each full encoded key maps
to a presence bitmap or a smaller sorted sparse column list. Only a matching covered identity may
use a negative bit. New segments, replaced inputs, absent indexes and keys
outside these namespaces fall back to the original authenticated reader.
Queries read and SHA256-authenticate current sidecar bytes every time; no
stat-only negative cache. Retain at most one bounded decoded index after current
physical SHA verification. Bound sources, keys and file bytes before allocation.
Build from physically authenticated accessors, cross-check the canonical history
dictionary commitment, and walk unique dictionary keys instead of canonical
posting rows. Reuse prior coverage only after authenticating its current bytes.
Strict snapshot checking independently reconstructs covered key memberships.

Inspect at most 64 newly uncovered pairs per optional maintenance pass, under
the existing heavy-work lease. Publish the new sidecar atomically, without
changing canonical history, cold/prune frontiers, MuHash state, or the current
small streaming compaction budgets. Cancellation must not publish partial
absence coverage. Large/unsupported dictionaries defer routing and preserve the
normal path. Cap accessor input at 128 MiB per pair, retained new key data at
32 MiB per pass (16 MiB per pair), and the output at 64 MiB. Publish a busy batch
only after 64 pairs are waiting; an idle pass can finish the remaining tail of
an existing index. All production actions remain outside this experiment.

# History accessor singleton inlining and bounded sparse directories

User scope: offline implementation and fixed-input validation only. No production
restart, deployment, pruning, or state reset. Keep canonical state, MuHash,
`.seg`/`.idx`, transaction ordering, dictionary key IDs and dictionary digest.

Accessor-only V8 uses the V7 120-byte header. The final reserved uint32 stores
sparse-directory length; V8 header CRC covers both bytes 0..111 and 116..119.
The ordinary block-offset array and CRC-protected entries precede the optional
sparse directory, all included in blockDirLen. The sparse section is at most
64 KiB including its count and CRC, records every 32nd block's exact first key,
entry offset and contiguous group length. Each query checks the sparse CRC,
ordering, full coverage and bounds, then reads and validates one bounded group.
Small ordinary directories can be fetched and checked in one bounded read.
Oversized sparse metadata/groups use the existing authoritative directory path.
There is no cross-file or process-wide authenticated cache.

V8 key records retain prefix/suffix coding. Token zero means an inline singleton
with TxNum-from, logical offset and ordinal varints. Tokens >=2 mean posting
count followed by posting offset. Token one is invalid. The enclosing key-page
CRC protects the complete singleton. Multi-posting payload/frame bytes retain
V7 semantics. Readers validate inline bounds, counts and ordering; cursors,
strict verification, prefix/batch queries, routing, debug and restore support it.
Fresh builders write V8; V6/V7 remain readable. No V8 history/index format is
introduced. V8 cannot be read by the old binary, so it stays offline.

Acceptance: independent singleton/multi/frame-boundary fixtures, corruption and
cancellation, streaming build/compaction/restore, dictionary identity, no changes
to history meaning, full MuHash fold and executable restore regression. Reuse
the 154.037 GB immutable accessor cohort for exact written-space accounting;
it is not a restorable node. Complete loopback HTTP eth_call A/B uses the same
11,818,192 canonical capture for both arms with derived candidate accessors,
seven heights, warm/new-process/private-inode OS-cold controls and independent
before-image oracle. Rebind optional routing to unchanged dictionary identities
without accepting stale source coverage. Separate code/index effects and do
not extrapolate microbench ratios to whole HTTP or production throughput.

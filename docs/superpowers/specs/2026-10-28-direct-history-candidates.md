# Direct cold candidates and hot-tail indexing ablation

Keep master, production service, production parameters and canonical data
unchanged. Reuse H45,836,827 and the existing complete HTTP host. Prioritize
latency and identical return values; do not repeat full-state scans or restores.

Compile exact immutable history/accessor pair identities once per owned
manifest, capped at 262,144 sources. Use inverted key memberships to generate
possible sources directly, preserving manifest order. Unknown/replaced sources
and unsupported key families remain candidates. Authenticate auxiliary bytes
per request as before. Do not retain cross-request decoded file proofs. An
unreadable speculative metadata page stays possible, so the demand loop still
reports its error only if needed. Preserve authoritative row, range, owner,
generation and Seq checks, plus bounded read-ahead and cancellation.

Measure incomplete and complete cold routing separately. Rebuild only derived
postings in an independent copy of the fixed hot-history database, appending only
the unpublished range, then advance its auxiliary frontier after success. Source
state HEAD and canonical cold refs remain fixed. This ablation quantifies the
unindexed-tail penalty; it does not claim a production atomic indexing policy or
unchanged import throughput. Bound the tail to 4096 blocks and mutable copy bytes
to 1 GiB. Do not clear shared/production file caches; reuse private cold-history
inodes with zero-residency and actual-read coverage for cold comparisons.

Batch indexed KV reads by raw hash candidate block, then validate full keys in
canonical repaired rows once per group. Never accept the hash match as a value;
retry stale/colliding candidates and retain the independent unindexed tail.
The preliminary block-aligned index ablation is not admitted: 256 is a count of
key occurrences per frame, not a block-alignment rule. Final data appends from
the exact previous frontier plus one, with no overlapping rebuild.

The hot-only prefetch and bounded canonical-block request cache were measured
as separate trials. Neither removed the indexed near-HEAD regression; both are
reverted. Keep the direct candidate cursors and raw candidate grouping only.
The selected matched measurements use product source 9a9de233 on the corrected
auxiliary data. External production rollouts occurred between phases, with stable identity inside
each phase. Do not merge those backgrounds; exclude the ineffective cache trial
from the selected performance conclusion.


## Follow-up: plan compilation, request images and bounded frame decoding

Reuse the exact corrected fixed input without rebuilding any auxiliary index.
Compile window coverage and column-to-manifest translations once for each
already authenticated route within a mixed batch; key cursors remain separate.

One PersistentHistoryReader may retain at most 64 MiB / 8 exact route images.
A fresh constructor or SetContext starts a fresh scope. Each image is owned and
checksum-authenticated before admission; all later decoders validate structure
and full query identity as before. Independent/unscoped requests still read and
hash current physical bytes. No errors or partial reads are memoized. A scoped
image represents one pinned request, not a cross-request file-integrity cache.

Reuse a stack-bounded 256-item posting decode buffer in read-only candidate
walks. Preserve overlapping frames, varint validation, candidate order and
canonical row validation; only stop before frames starting beyond toBlock.
Do not assume a 256-block width or a non-overlapping predecessor-frame layout.
Verify matched HTTP latency, return values, CPU and allocated bytes on the same
input before considering the previous near-HEAD regression resolved.


A tiny-window cost path bypasses posting prefixes only for a pinned request
with an owner-tail cache, present stage metadata and <=16 requested blocks.
It reuses the exact existing canonical first-tail scanner and all key/Tx/repair
predicates. Larger/unpinned/unstaged reads retain ordinary indexed behavior.
For request route images, let the dominant large authenticated object replace
smaller images when byte pressure would otherwise prevent its admission;
small images never evict it, and the same 64 MiB cap still applies.

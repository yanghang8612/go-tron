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

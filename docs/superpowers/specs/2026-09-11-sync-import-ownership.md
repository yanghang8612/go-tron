# Sync import batch ownership

The September 11 production sample found 63,332–64,107 raw blocks in the download buffer while canonical import continued at about 43 blocks/s. The raw buffer rose by 775 blocks in ten minutes and remained fetch-backpressured. This is independent of history archival debt, which remained stable overnight. The sample does not expose buffered heights and therefore does not prove that every retained block has the same cause.

`commitDecodedBufferedBatch` removes a decoded prefix from raw-buffer/hash/path tracking before the prefix finishes execution. Scheduling, inventory and fetched-body acceptance may consequently forget that exact block hashes are still owned by the importer. A repeated body can arrive before canonical head advances, re-enter the raw buffer, and be left behind its drain cursor.

The importer must retain exact-hash ownership after the off-lock decode has been validated against the locked raw buffer. Ownership continues through the insertion session's final asynchronous commit barrier. It is separate from raw body retention and must not retain protobufs or raw bytes. Fetch scheduling, inventory deduplication and receipt acceptance consult the same ownership state.

Ownership belongs to the serialized drain, not a peer connection or a network-session reset. Failover/reset can occur during off-lock execution. Such a reset must not expose still-importing hashes to another fetch. Completion must not remove a replacement block with a different hash at the same height. Staged recovery that restores an exact owned hash must not leave an unreachable duplicate in raw memory after the drain finishes. Durable staged data continues to follow the existing canonical settlement/recovery rules.

Preserve malformed-prefix handling: decoded good prefix, malformed boundary and untouched suffix have different lifetimes. No ownership transfer occurs before locked validation and required malformed-row persistence operations succeed. Failure, stop, reset, partial decode and asynchronous completion all need explicit regression coverage.

Do not reject a block solely because its height is at or below `syncedTipNum`, which is a drain cursor and can lead canonical head. Preserve different-hash fork/recovery behavior and existing peer accounting, retry and staged-body semantics. No wire, consensus, database-key or on-disk format change is required.

Validation must first demonstrate a regression on the old production code and then pass on the fix. Cover scheduler and receipt entry points, delayed canonical visibility, ordinary success, failure, decode-boundary atomicity, peer disconnect/reset and same-height different-hash preservation. Run focused race tests, affected package tests, the full repository suite and the existing two-node system test before deployment. Pin Go 1.25.5 to match production.

Any accompanying branch-codec optimization is independently benchmarked on fixed inputs and checked for identical encoding/roots. Microbenchmark improvements are reported separately from production block throughput. Cache budgets, concurrency, signature checks and archival admission remain governed by their existing policies.

Deployment follows the already authorized master workflow: reviewed source commit, normal GitHub push, isolated native Linux/Sapling build from the fixed revision, preserved rollback executable and guarded service switch, followed by online progress/cache/error sampling. Keep prior evidence and distinguish restart-cleared cache from proof of reduced ongoing retention.

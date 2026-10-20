# Cold history point reads

User authorization: implement and verify locally and on independent copies;
no production restart, push, deployment, format migration, or source DB writes.
Reuse compact-v3 bytes and retain all MuHash, as-of and snapshot semantics.

1. First hot change: an empty indexed interval must still search the unindexed
   tail. Resolve cold first and bound hot work by the cold match's inclusive
   transaction and block; compare sequence numbers on overlaps.
2. Storage already restored the target account envelope. Resolve contract
   metadata in that envelope's generation rather than replaying generation
   history again. Cover deletion/recreation and metadata changes explicitly.
3. Genesis has cut zero; honor an explicit hot StateTxRange(0), otherwise avoid
   the cold all-segment negative search. Propagate request cancellation into
   hot first seeks, cold per-segment traversal and underlying bounded reads.
4. Preserve segment/header/dictionary and accessor CRC checks on every lookup.
   Memoize at most 128 decoded header bytes by BLAKE2b-256 of the complete
   compressed block AND decoded size. Each new reader rereads/hashes current
   physical bytes; within-reader reuse follows its existing decoded-block cache.
   Thus same-size/mtime corruption cannot hit an old entry. Use an entry-bounded
   cache (8,192 entries), copy private output, no file handles, manifest
   pointers or contexts in cache. Keep existing file leases and range/dictionary validation. This
   defers accessor-first shortcuts until a stronger authenticated pair proof
   is available.

Validation: reproduce omitted-tail bug; cold/hot overlap including same-tx seq;
canonical genesis and explicit range; recreated-contract metadata; prompt cancel
with absent keys; bounded cache eviction, concurrency, input/output ownership,
changed physical bytes, truncation and corruption. Run state/rawdb/snapshots and
core MuHash/snapshot restore regressions with race/vet where appropriate.
Benchmark baseline and candidate on identical immutable data, including cold
and warm process caches, missing/existing keys, repeated and varied addresses.
Keep source-copy identities and test limits with results; don't equate a
component speedup with whole-RPC latency or change production query timeouts.

## Follow-up: filtered logs and the unindexed tail

Read RPC event addresses by 20-byte identity, querying both existing 00-padded
TVM and 41-prefixed dictionary keys. Preserve persisted bytes, payload width,
canonical validation, topic intersections and deduplicated row order.

For hot point/batch fallbacks, use the existing validating borrowed block decoder
and copy only the requested owner. Apply positive sequence repairs before using
matches, including replacement by another owner. Legacy standalone rows remain
readable; corrupt/shared-pack failures and cancellation propagate.

Wrap only an explicitly pinned RPC history view in a query-owned tail cache.
Cache complete owner/block-range scans, including empty results, and reuse a
covering scan for narrower bounds. Keep all transaction, generation, domain,
key and sequence filtering in existing accessors. Bound retained data to 8 MiB,
16,384 rows and 16 owners; continue the same filtered scan on budget exhaustion
and avoid future cache attempts for that owner. Positive queries stop immediately
and incomplete scans never publish coverage. The byte limit accounts retained
cache payload and estimated row overhead, not all RPC RSS or per-block decode scratch.
Never publish partial, canceled or failed scans; no global negative cache,
new persistence, MuHash changes or online scheduler parameter changes.

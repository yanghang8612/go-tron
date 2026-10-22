# Prefix predecessor candidates v2 (2026-10-22)

Independent new design round, frozen before v2 production edits and timing.
Parent remains actual `9030ce7a31016491ece014ac76e5656ec149417e`.
B v1 is sealed NO-GO under `docs/dev/evidence/serial-state-reads-oct22-b/`.
Its gate and raw samples remain unchanged.

## Authorized scope and algorithm

Root extends B's allowed production scope to the existing
`layerPrefixBucketIndex` fields/helpers/addToBucket in buffer.go and the one
range-reset site in layer_view.go. A owns ReadSnapshot/cacheVersion and is
explicitly coordinated not to overlap these sections. predecessor.go remains B.

Add only a 256-bit per-bucket sorted-state bitmap (32 bytes per existing index).
Retain and lazily sort the existing bucket string slice in place under the
existing shard lock. No additional keys, persistent cache object or independent
cache lifetime. New-key append invalidates only its bucket's sorted bit;
replacements/point deletes preserve key membership and valid ordering. Packed
all-bucket construction and range reset clear the sorted state. Read queries
use a ready generic full index first, otherwise exact typed account bucket
bounds and descending best-candidate merge. Missing bucket or sort publication
uses TryLock, with the existing RLock map fallback while flush retains RLock.

Only complete prefixes accepted by rawdb's fixed-header decoder use the new
path; partial/generic/unknown domain prefixes keep the original implementation.
Use borrowed strings only for synchronous caller-owned arguments. Any schema
string escaping into the persistent index must be cloned. Compute prefixEnd
only when needed; nil vs nonnil empty upper remain distinct. HasPrefix precedes
other key comparisons on unsorted fallback. Newest masks older, own points
precede own ranges, global best only increases, highest deleted base candidates
cause repeated fallback. View capture and close behavior remain unchanged.

## Correctness and mutation gate

Zero differences against the independent full materialized map oracle already
used in v1, copied unchanged to both arms. Add candidate-only focused tests for
bucket sort reuse and invalidation on first new key, existing replacement,
write->delete->write, new delete, structured/owned batch mutations, first/second
bucket packed expansion, range reset, generic-index coexistence, caller byte
reuse after index construction, and contended RLock fallback. Validate pinned
and fresh snapshots plus flush/discard and base read/release errors. Run normal
and focused race tests and full blockbuffer tests. Astra audits exact frozen
production/test/harness hashes, particularly every index mutation lifecycle.

## Performance gate (unchanged)

Primary cold/churn: >=20% median speedup, >=50% fewer generic all-key sorts,
(max-min)/median <0.30 for each arm. Controls (hotstable, smallprefix,
tombstones, generic) regress <=10%. No resampling, no discarded observations.
Actual-parent and B-only candidate exports share byte-identical common tests
and harness; only the three authorized production files differ.

New v2 harness uses fixed 5 samples per revision in ABBAABBAAB order, exclusive
historical lock `/Users/asuka/Projects/asuka/go/go-tron/build/benchmarks/20261018-tvm-event-replay/local-benchmark.lock`.
Four layers, canonical one shielded root in base, 256-byte unrelated values,
and fixed 8/128 MiB payload remain. Each timed operation issues exactly one
root predecessor. Double-arm fixed iterations by shape: cold=32, churn=256,
hotstable=32768, smallprefix=32768, tombstones=128, generic=32768. Churn adds
512 new unrelated keys (timed), then the harness outside timing removes those
same keys, resets only active-layer indexes, and warms predecessor to restore
the same initial keyset and ready-index state. A complete active-layer
commutative checksum (write/delete counts plus two 64-bit key/value digests)
must match the initial input after every restoration. Clearing the entire
index holder also clears every built/sorted state. Thus input size never drifts
with b.N, both arms perform the same fixture restoration, and timed base-call
counts exclude restoration. The original v1 fixed 16x samples are not replaced.
Measure ns/op, allocs/op, B/op, base Get/iterator/reverse-seek calls and actual
generic sorted-key constructions. Separate immutable v2 evidence/manifest.

This benchmark covers only the local overlay and its 512 unrelated-write
maintenance; it does not claim Pebble or SaveCurrentAsBest complete-stack
performance. Each per-shape command uses a fixed 30-minute timeout for both
arms, frozen before timing. Cold and churn are intentionally distinct shapes.

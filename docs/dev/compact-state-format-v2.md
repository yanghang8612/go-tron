# Compact latest state and sparse Shielded roots

Implementation date: 2026-10-19. This is an incompatible local format, authorized
for a fresh genesis sync. No production data reset, service stop, push or release
is performed by this change. TRON wire messages and Java storage identity remain
unchanged.

## ContractStorage and exact generic values

The latest-KV physical prefix changes from `state-kv-latest-v2-` to `kv3-`.
A row is `kv3- || owner20 || generation8 || domain2 || logicalKey`. Storage keeps
the complete Java-derived 32-byte row key; its physical key is 66 bytes instead
of 81. Owner, generation, domain, metadata/version semantics and Java's slot
collision behavior are preserved. The earlier 44-byte experimental candidate
relied on one slot prefix per incarnation; that sample observation is not a
permanent schema invariant, so this implementation does not discard those bytes.

Values use one canonical, self-describing envelope:

- Tag 1 followed by the exact arbitrary byte string.
- Tag 2 followed by the minimal big-endian suffix of an exactly 32-byte word
  with at least one leading zero. The suffix has 0–31 bytes; a nonempty suffix
  starts with a nonzero byte. A zero word has an empty suffix.

Readers restore all 32 logical bytes before state, history or TVM use. Present
empty bytes (tag 1 alone), present zero word (tag 2 alone), and absence remain
three distinct physical states. Uncompressed words with leading zeros and
nonminimal/malformed compact words are rejected. Wide words, unknown protobuf
bytes and ordinary variable-length values remain exact. The format applies to
all generic domains so source capture, caches and snapshot restore use one codec.

Dirty staging owns both logical and encoded bytes; compact logical values are
not treated as a suffix view of their encoding. Before-images and per-slot
history retain exact logical values; applying an inverse delta re-encodes them
canonically. No shared value dictionary or aggregate contract-blob history is
introduced.

## Cold latest segments

Latest binary segment version 2 reserves value-length bit 30 for a minimal
32-byte word, and retains bit 31 for Snappy. The modes are mutually exclusive;
the decoded value is still the original 32 bytes. This is needed because the
previous Snappy threshold was 64 bytes, leaving all 32-byte storage values raw.
An empty raw payload and a compact zero word have different frame flags.
Bounds, accessors, B-tree lookups, verification, segment materialization and
restore all consume the same frame decoder. Existing history segment formats
remain unchanged and continue returning full logical before-images.

## SystemShielded change points

Production StateDB stores `mrc- || height8` in SystemShielded only at the first
indexed height and when the 32-byte root changes. Equal-root block finalization
performs no index write, no per-block run-end update and no root-index history
mutation. LAST_TREE, CURRENT_TREE, root-keyed trees, notes, nullifiers and proofs
retain their semantics. The raw-DB MerkleContainer adapter remains a legacy
test utility; production construction uses StateDB.

Lookup selects the greatest boundary at or before the requested height, bounded
by the current execution/canonical HEAD or the historical view's explicit cut.
Nonpositive heights, heights before the first boundary and future heights are
absent. Writes older than the current execution cut are rejected. Canonical execution
is contiguous; arbitrary imports with holes are not
supported by this index. A/B/A roots produce three separate boundaries.

Reverse seeking is native in Pebble and its MVCC snapshots. Direct-history
routing, blockbuffer layers, pinned read snapshots, the bound async LayerView,
commit-scope pending writes, dirty puts and tombstones preserve the same view as
point reads. Historical views reconstruct the logical change-point prefix as of
their requested cut before selecting its predecessor. This archive fallback
can materialize the change-point prefix; normal current-state queries do not
scan from genesis.

## MuHash and incompatibility boundary

- Scheme: `state-flat-latest-muhash3072-compact-v2`.
- HEAD version and element codec version: 2; element and delta domains change.
- Snapshot manifest version: 3; latest binary segment version: 2.

MuHash covers the exact new physical source rows, including inactive generations.
A shorter namespace can overlap a raw 32-byte trie hash. Such keys cannot be
latest-KV rows (minimum physical length is 34 bytes), and are excluded consistently
from source enumeration, initialization, export, rebuild and row census. Other
malformed source keys still fail authentication. Old HEADs/manifests are rejected;
even a residual old latest-KV namespace without a HEAD prevents empty-state
initialization or restore. This never silently opens an old database as genesis.

## Space accounting at the authenticated analysis cut

The prior full census at block 5,715,086 contains 26,940,778 Storage rows,
862,104,896 logical value bytes and 175,256,452 minimal value bytes. Applying the
implemented encoding to that exact distribution gives:

| Visible representation | Before (bytes) | After (bytes) | Reduction |
| --- | ---: | ---: | ---: |
| Hot Storage keys plus canonical values | 3,071,248,692 | 1,980,288,578 | 1,090,960,114 (35.52%) |
| Cold latest Storage record bodies, keys and 8-byte framing | 2,694,077,800 | 2,007,229,356 | 686,848,444 (25.49%) |
| Shielded dense index rows | 5,715,086 | 1 boundary | 5,715,085 rows |

These are exact representation calculations for the previously authenticated
input distribution, not measured live LSM allocation or full cold directory
savings. SST prefix compression, indexes, retained histories and compaction
change physical results. The earlier 24.81% Snappy SST experiment used a different
44-byte key and unwrapped value; it is not an implementation benchmark here.

## Correctness verification

The regression fixture performs normal transaction finalization and canonical
commit, then compares the complete normalized MuHash state and digest against a
full physical-source fold after every block. It includes small, wide, deleted and physically present zero/empty
storage values; distinct complete Java slots sharing a suffix; constant
roots and A/B/A root changes; snapshot/revert; hot and exclusively cold historical
queries; future-height rejection; inverse unwind and byte-identical replay;
verified latest snapshot import and continued identical execution after restore.
A colliding raw trie-hash prefix is present throughout export/authentication.

Separate tests cover all leading-zero lengths, canonical rejection, owned slices,
10,000 unchanged blocks retaining one boundary, same-height overwrite collapse,
commit-scope pending/dirty tombstones, native predecessor bounds, range tombstones,
async layer isolation and pinned snapshots across flush. Existing restore-marker,
atomic publication and MuHash failure-path regressions remain enabled.

The local native predecessor benchmark on Apple M1 Max (warm query, excluding
setup and database close) is 1.040 µs with one row and 1.542 µs with 100,000
change-point rows, both at 14 allocations per query. This verifies the native
reverse-seek path rather than an O(number-of-heights) scan; it is not an online
end-to-end latency or sync-speed measurement.

Validation evidence is retained under
[evidence/20261019-compact-state-format](evidence/20261019-compact-state-format/validation.json).
The full Go 1.25.5 run passed 61 packages; targeted race, vet, Sapling build and
native Shielded lifecycle checks also passed. An initial Go 1.27 run had one
async-retry timing assertion fail; its unchanged test passed 10 repetitions and
the full Go 1.25.5 run passed. No async scheduling assertion was weakened.

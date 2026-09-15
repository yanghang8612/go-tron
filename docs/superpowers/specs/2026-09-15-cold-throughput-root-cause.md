# Cold history throughput: fixed real input and bounded capture

The objective is to remove measured redundant cold-build work while retaining
the existing historical contents, canonical binding, complete SHA checks,
publication ordering and prune proofs. Import admission alone does not increase
cold throughput. Measurements must distinguish logical history lag from Pebble
LSM compaction debt and avoid attributing a change to a single deployment when
the block inputs differ.

## Measurement boundary

The ordinary cold builder reads one pinned history view twice (dictionary and
records), resolves shared chunks into original logical histories, writes the
key-oriented record stream and its companions, and performs its existing final
self checks. Key dictionaries do not eliminate repeated Prev payloads. The CDC
container subsequently splits and compares those payloads again. Profiles must
separate source reads/copies, decoding/authentication, record serialization,
CDC/compression, companion indexes and self checks.

The full bounded builder benchmark includes the production segment trio. It
does not by itself measure global manifest publication, lifecycle scheduling,
pruning or retired-file GC. Whole-manifest metadata work can grow independently
of a captured range and needs separate evidence.

## Real physical range capture

After stopping the service if required by the exclusive Pebble lock, open the
source read-only and acquire one pinned snapshot. Capture an explicitly selected
inclusive range into a new private Pebble directory, then close the source and
resume service before expensive decoding or benchmarking. The CLI owns source
and destination path separation and cleanup. Never open the production database
as a writer for diagnostics.

`rawdb.ExportStateHistoryRange` retains exact existing schema keys and values:
canonical hot block protobufs, each block's tx range, every changeset sequence
(including positive repair rows), referenced shared chunks in their original
1024-block buckets, and present bucket metadata. It validates canonical height,
tx-range hash binding and range/parent continuity. A legitimate block can lack a
sequence-zero pack and have no changeset rows. Missing bucket metadata is
reported without fabrication; it is not needed for ordinary shared reads.

Hard limits are 256 blocks, 1 GiB copied key/value bytes, 262144 physical rows and
4 GiB declared decoded history bytes. Logical bytes count each pack/repair once,
including repeated shared references; unique chunk sizes are reported separately
by codec. Envelopes and declared lengths are preflighted without materializing
histories or decompressing chunks. Limits apply before destination writes;
individual reads/iterator steps/protobuf decode/writes are not interruptible.

Reports identify every acknowledged physical write by exact key, value length
and SHA256, plus a deterministic sorted manifest digest. The manifest hashes
each entry's big-endian u64 key length, raw key, big-endian u64 value length and
raw value SHA256. These fingerprints identify exact input bytes; they are not
proof that decoded histories are valid. `ContentVerified` remains false even
when physical `Complete` is true. Any failure produces an explicit incomplete
report, and its destination must be discarded; a failing writer can itself have
partially applied a write.

## Offline replay and acceptance

After production resumes, verify the private database against the physical
manifest, perform complete normal decoding (including all chunk and pack SHAs),
and run the ordinary bounded cold builder using a `DomainCfg.HistoryPath` `.seg`
path. Compare every archival row in production order from the hot copy and
generated cold files, including tx number, block binding, identity, presence and
exact Prev bytes. Physical sequence keys remain exact in the export; the existing
cold format replaces that sequence with its stable ordinal. Transient Next is
not an archival payload. The current borrowed builder rejects legacy repair
rows, so such a replay must fail explicitly instead of choosing another reader.
Measure wall time,
CPU profiles and allocated bytes on identical inputs before and after a change.
Synthetic cases help isolate mechanisms but cannot replace a real range replay.

The private range is not a chain backup or a complete shared bucket. It omits
unrelated history, chain state, global manifests and references outside the
selected range. Never use it as evidence to prune source chunks or run source
GC. A missing hot canonical block requires a deliberate ancient-aware capture
extension; it must not silently weaken canonical checks.

## First measured candidate: redundant owned-value copy

The private Pebble snapshot's `Get` already copies the engine value before
releasing its closer. The ordinary presence accessor then defensively copied
that owned value a second time. Add an explicit `GetReturnsOwnedBytes` capability
only to that snapshot. Its guarantee covers independent results, subsequent
reads and reader close; it conveys no presence, pinning or concurrency promise.
Unmarked readers and false capabilities retain the defensive copy. The original
Has-then-Get order, errors, nil representation of empty values and the separate
presence-coupled path remain unchanged. This removes no decode or SHA check.

Acceptance covers ownership mutation tests, read/error sequence equivalence,
complete cold-trio bytes/references and full decoded output. Synthetic full
builder measurements provide an allocation hypothesis; their timings cannot
establish production throughput or prove the existing backlog will disappear.
Only the real physical range replay can establish the benefit for current data.

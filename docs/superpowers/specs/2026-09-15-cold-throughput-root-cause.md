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

## Offline bounded parallel experiment

`db benchmark-history-parallel` accepts only a complete original 16-block private
export. It reuses the serial diagnostic's manifest/path/source exclusion checks,
physical re-export verification and full logical digest routines. One real
read-only private Pebble snapshot is owned until every worker joins; no arbitrary
pinned reader is assumed concurrency-safe. Owned Get and auto compression are
fixed. Workers and segments are independently 1, 2 or 4, workers <= segments.
Partitioning is by equal complete-block counts on the exact captured heights;
canonical and transaction continuity remain independently verified. No rows are
re-numbered to manufacture locality.

The entire command has a cooperative deadline at most five minutes and at most
three iterations. Failure cancels the group, stops new admissions and joins all
started work before snapshot release. Each worker creates its complete original
trio in a separate private directory. Timed wall covers all group builds plus
join; source authentication and exhaustive cold re-read/companion verification
are outside timing. There is no production manifest publication, stage update,
pruning, GC or scheduler change. Therefore this experiment cannot claim to have
measured batched publication or complete online maintenance throughput.

Across concurrent key/posting collectors the configured ETL spill thresholds
sum to 64 MiB: each receives 64 MiB / (2 * workers). This is not a strict heap/RSS
bound. Entry/order arrays, append overshoot, arena allocation/pools, codecs,
per-worker key tables (up to 512 MiB), CDC dictionary payload (up to 64 MiB),
pack output (up to 128 MiB), frame scratch and compression pipelines are extra.
Existing export maxima remain aggregate input bounds (1 GiB physical, 262144
rows, 4 GiB declared logical bytes), not aggregate resident memory claims.
Normal diagnostic/default production builder thresholds do not change.

Every source subrange and resulting trio must match the complete logical-row
and tx-range digests exactly. The partition must exhaust the original block/tx
range and summed statistics must match the full source, with a maximum rather
than sum for MaxPrevBytes. SHA strings are not algebraically combined. Reports
preserve all per-range hashes and the full-source hash, label process-wide
allocations/profile scope and warmed caches, and mark any incomplete group as
failure. Acceptance includes exact partition/overflow tests, duplicate/empty
and missing-pack input, all six supported worker/segment combinations, actual
private snapshot reads, source unchanged, cancellation/join and shared serial
diagnostic regressions. Native fixed-input measurements remain a separate gate.

## Opt-in single-trio authentication pipeline

The offline cold diagnostic may explicitly enable `--shared-read-pipeline` with owned
copy mode. Normal readers, Runner scheduling and the parallel-segment diagnostic
remain unchanged. A new explicit capability is implemented only by the private
Pebble snapshot: pinned sequence, caller-owned Get values and concurrent Has/Get
with independently owned iterators. Pinning/ownership alone are insufficient.
Presence-coupled adapters (`GetWithPresence`) are explicitly unsupported by this
experiment; it must not silently replace that operation with Has/Get.

Within each existing dictionary and record pass, one ordered producer retains
the original block/tx-range matching and schema handling. At most two shared
packs are materialized concurrently through the unchanged complete reference
preflight, Has then Get per chunk, chunk decode/SHA and final full pack SHA.
The consumer parses RLP and invokes borrowed callbacks in original order. The
post-materialization parser is shared with the unchanged serial path; already
authenticated bytes never re-enter shared-envelope recognition. Non-shared and
invalid-envelope cases drain predecessors and use the original serial decoder.
Both full passes, their dictionary boundary, CDC and output trio remain intact.

Global speculative reads of later blocks are explicitly permitted; the global
serial I/O trace is not promised. Each block's reference read/authentication
order is preserved. A future block failure does not cancel a predecessor or
overtake its callback/parse failure. Known future failure stops further work;
the consumer chooses the first original-order failure. Callback early stop
ignores future speculative failures. Parent cancellation stops admissions,
checks between reads and joins every worker before iterator/snapshot release.
No worker, callback slice or authentication result survives the read operation.

The two slots include completed outputs waiting for consumption. Their declared
decoded total is at most 256 MiB; existing shared packs remain limited to
128 MiB and a maximum-sized pack runs alone. No oversized pack is newly legal.
The reused row scratch is cleared before releasing its decoded-output charge,
so no prior pack remains reachable through borrowed Key/Prev fields.
Pending iterator bytes are borrowed until admission; only admitted valid shared
envelopes are copied. Encoded envelopes, chunk Get values, allocator overhead,
serial legacy decoding, ETL and compression are additional memory, so 256 MiB
is not a process heap/RSS limit. No cache, changed format, weakened checksum,
new production admission, publication or prune authority is introduced.
In particular, a fully authenticated shared payload can contain a compatibility
Snappy storage envelope that the existing serial decoder accepts. Its inner
decoded buffer (up to the unchanged 128 MiB limit) is additional to the outer
shared-materialization charge. Tests preserve that old behavior; the 256 MiB
claim does not include this serial compatibility buffer.

Acceptance compares the frozen serial iterator and decoder for row bytes,
filtering, order, legacy handling, malformed/nested payloads, per-block read
errors and multiple competing failures. Tests exercise bounded admission,
callback ownership, future error/earlier callback precedence, canceled blocked
reads and complete joining. Full diagnostic output refs/bytes/digests must equal
serial on the same private input; only a real complete-trio replay can establish
throughput benefit. The options JSON explicitly reports `shared_read_pipeline: false` by default;
source identity, refs, full digests and other report fields retain their contract.

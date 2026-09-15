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

The subsequent offline experiment adds explicit `--shared-read-workers=2|4|8`.
Existing no-argument pipeline APIs and the CLI default retain two slots. A
disabled pipeline accepts only the default two-worker setting, and every report
records `shared_read_workers`; default production scheduling remains serial,
with a separately gated opt-in described below.
All selected slots (running or awaiting ordered consumption) share the same
256 MiB declared shared-output budget. A maximum 128 MiB pack remains exclusive.
Admission distinguishes an actual exclusive pack from the cumulative size of
several small packs: four/eight smaller packs may use the full shared budget.
Encoded envelopes, active chunk Get buffers and runtime/goroutine overhead may
increase with workers; the budget remains distinct from total heap/RSS, and the
same inner-Snappy compatibility allowance applies. Queue ordering, immutable
snapshot, complete authentication, callback-first errors and cancel/join remain
unchanged. Deterministic 2/4/8 tests exercise slot occupancy, ordered errors and
join-before-Close; complete same-input trios must remain byte-identical.

## Explicit build-scoped shared chunk authentication reuse

The offline diagnostic can opt into `--shared-chunk-cache=true` (default false)
on an audited immutable pinned owned view, with no presence-coupled operation.
The normal public reader and the default production configuration do not create
this adapter; the separately gated production option below may create it.
One private cache is created per complete trio, inside the build timer, and the
same wrapper is borrowed by both original passes and any 2/4/8 pipeline jobs.
No snapshot is reopened or unwrapped. All jobs and iterators finish before the
cache is cleared; only then may its owned snapshot be released. Borrowed source
snapshots remain owned by their caller. Cache Close is idempotent, requires
joined operations, and does not promise concurrent Close/engine-read safety.

A fixed 4096-entry direct-mapped table stores complete `(bucket, SHA, expected
decoded length)` identities. Physical reads use the existing schema accessor.
On a miss, the original Has/Get, codec/length checks and chunk SHA write into
the existing private pack output. Only a successful chunk can be admitted.
Under the write lock, the cache rechecks competing inserts and evicts before
cloning; clock eviction scans no more than the fixed table length. Hits copy
under the read lock into the current pack output and never expose cached slices.
There is no negative cache, singleflight or extra speculative miss clone.

Cache payload slice capacities are capped at 64 MiB with at most 4096 entries;
the table (about 0.3 MiB), allocator rounding, pack outputs and other builder
buffers are additional. No evicted slice remains referenced by an uncharged
local cache entry. Completed independent misses can use their own verified
output even if another worker has already installed that key. Collisions or
eviction affect performance only; later misses repeat the complete read/auth.

Every shared pack still checks its complete SHA, and both build passes remain.
A valid chunk may enter before its enclosing pack subsequently fails pack SHA;
the chunk proof is independent, the pack failure is never hidden, and a failed
build closes its entire cache. Cache hits intentionally omit later Get/chunkSHA
and therefore do not preserve transient per-reference I/O failures on hits.
Miss errors, malformed lengths, bucket isolation, ownership and ordered
pipeline errors remain checked. Job cancellation is forwarded even on hits.
The cache also retains its creation scope context: both that context and each
operation/job context are checked, so a Background borrower cannot bypass scope
cancellation. Snapshot release errors are joined with build/cancellation errors.

Reports always include `options.shared_chunk_cache`. Enabled iterations record
`shared_chunk_cache_before_close` and `shared_chunk_cache_after_close`; disabled
ones omit both objects. Each reports hits/misses/inserts/evictions, current/peak
payload and entry counts, fixed budgets and closed state. Construction, both
passes, statistics and close are timed. After close, payload/entries must be
zero while cumulative/peak counters stay available. Real same-input cache-off/
on ABBA is required before claiming benefit; production defaults stay disabled.

## CDC digest reuse after complete byte equality

The CDC writer already owns an immutable copy of each active deduplication
anchor. Its SHA256 dictionary remains the authority for anchor selection, LRU
order and eviction. Add a secondary `(seeded fast fingerprint, length)` index
with at most one candidate per key and no collision chain. A hit must still name
the current primary dictionary element and pass a full `bytes.Equal` comparison
against the producer chunk. Only that equality proof permits reuse of the
existing SHA256. Unequal bytes, fingerprint collisions, absent/retired candidates
and misses use the original complete SHA256 calculation and primary lookup.
The original primary lookup also performs its existing equality check.

The secondary index borrows only primary dictionary identities, never byte
slices from callers, iterators or worker outputs. It is maintained on the same
single writer as the dictionary; entry removal deletes the secondary candidate
only when it still names that exact element. It adds at most one map entry per
active primary entry (maximum 8192), plus a fingerprint in each primary entry,
and no additional retained payload. The existing 64 MiB payload bound and
pending compression budget remain unchanged; neither is a total heap/RSS bound.
Abort, Finish and Reset discard the secondary index. A randomized fingerprint
seed can affect only cache misses and computation, never serialized output.

This is not an input authentication cache. Every original shared-chunk and full
pack authentication, both history scans, CDC split boundary, primary dictionary
choice, compressed bytes, literal/reference distance and complete file checksum
remain unchanged. Full equality establishes that the existing digest is exactly
the SHA256 of the current bytes. Primary dictionary hash-collision behavior also
remains unchanged, because candidates must still be the live primary identity.
No fingerprint is persisted or accepted as an integrity proof.

Acceptance compares complete files and their full decompressed contents against
writer and writer-owned pipeline methods frozen from commit
`e913c72d274d3f76f07b61f1d44ae1df986cbee9`. Tests cover worker counts 1/2/4/8,
encoder configurations, variable Write partitioning and retained-prefix edits,
forced fingerprint collisions and differing lengths, caller ownership, both
primary eviction limits, reentry, page-spanning references and the uint32 anchor
sentinel. The complete writer benchmark includes file finalization/checksum and
uses deterministic native-codec legacy delegation aggregates with small list
insertions, alongside low-reuse data. It reports avoided SHA bytes and unchanged
output size; these synthetic timings are not production throughput. A separate
native fixed-input full-trio comparison and online complete maintenance rate
remain necessary before claiming this change clears the backlog.


## Opt-in production reader topology and context ownership

The production CLI accepts `--history.shared-read-workers=0|2|4|8` (default 0)
and `--history.shared-chunk-cache=false|true` (default false). These flags have no
environment-variable aliases and do not change the deployed service. A zero/
false configuration retains serial reads, automatic codec concurrency and the
existing independently admitted history/event pair. The registered bounded
history hook now carries the real Runner context even with default options.

Select the actual read plan only after the existing HeavyWorkGate lease is held;
no new parallel admission, retry, forced-busy or recovery path is introduced.
The original fresh engine/device pressure check must permit CPU work. The CPU
capacity observation is the same mutex-protected sampler used by existing
history/event admission, with the same completion timestamps, affinity/scope
pair checks, finite-quota rejection and slow-read rejection. Missing, stale,
future or unknown observations fall back to the original serial reader. Forced-
busy describes importer scheduling, not resource pressure: an admitted forced-
busy batch may use at most four reader workers when these same fresh resource
checks pass. Otherwise it still progresses serially. Its existing finite batch,
complete-maintenance recovery and cooldown are unchanged.

Select the highest 2/4/8 worker count no greater than the requested maximum that
has at least `W+1` effective idle cores and available GOMAXPROCS slots. Cache-only
mode needs one effective idle core. In addition to the existing 2 GiB available
memory headroom, reserve the 256 MiB declared pipeline output budget when read
workers are selected and the 64 MiB retained cache payload when enabled. This
is a conservative admission target, not a total heap/RSS guarantee; encoded
buffers, active reads, table metadata, allocator rounding and runtime memory
remain additional. Both ETL collectors retain the current effective 64 MiB
threshold separately. No global GOMAXPROCS, compression policy or ETL default is
changed. An actually enhanced read/cache plan uses one local codec worker;
fallback uses the original automatic codec setting. If either option is
configured, history and event files are generated sequentially, including
fallback batches, to avoid nesting a second builder under the reader workers.

One complete production trio acquires one pinned view, or one explicit cache
session that owns its acquired view. Both original passes borrow that same view.
The real context is checked before work, in range/change callbacks, by pipeline
jobs and throughout the builder's existing context-aware finalization. All
reader and codec jobs join before cache clearing or snapshot release. Close
errors join build/cancellation errors and prevent manifest/stage publication.
The normal post-build cancellation check, manifest integration, stage writes,
merge and prune flow remain in order. Existing event/derived builders retain
their bounded completion behavior on cancellation. Cancellation does not delete
immutable output files that may already exist, but observed cancellation or a
history close error cannot publish that attempted range or prune its hot input.

Metrics under `state/snapshot/cold/history/shared_read/` expose the last admitted
attempt rather than requested configuration: `last/workers`, `last/chunk_cache`,
`last/codec_workers` (0 means original automatic setting), `last/from_block`,
`last/to_block`, `last/active`, and `last/last_published`. `attempts` and `published`
are cumulative counters. These describe the selected actual execution plan and
manifest/stage completion, not total lifecycle completion or measured active OS
threads; deferred passes do not erase the last attempt. `last/fallback_reason`
is bounded: 0 enabled, 1 disabled, 2 reserved (not emitted), 3 missing lease, 4 storage
pressure, 5 unknown resources, 6 stale resources, 7 insufficient memory, 8
insufficient CPU. Complete maintenance throughput and eligible-lag slope remain
the acceptance measures. A successful isolated trio or configured flag alone
cannot establish that sustained backlog has been fixed.

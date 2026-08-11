# Event-log snapshot space audit and format experiment

Status: benchmark tooling implemented; no V3 format selected.

## Scope and production motivation

The 2026-08-11 mainnet snapshot inventory reports 299.22 GiB in the active
event-log family:

| Kind | Files | Physical GiB | Family share |
| --- | ---: | ---: | ---: |
| `event-log` | 7,399 | 286.626669 | 95.79% |
| `event-log-index` | 6,678 | 12.595282 | 4.21% |

The optimization target is therefore the main event-log segment. Compressing
only the external segment-start sidecar cannot materially solve this storage
problem.

This phase adds a read-only inspector and format simulator. It deliberately
does not add a V3 writer or change production readers. A V3 decision requires
representative mainnet measurements from immutable production files.

## Current formats

### Main event-log segment

`EventLogSegmentVersion` is 2. Readers dispatch by the eight-byte magic and
accept both V1 and V2.

V1 is:

```text
48-byte header
rowCount * 125-byte fixed rows
raw TransactionInfo_Log protobuf payloads through EOF
```

V2 is:

```text
88-byte header
rowCount * 125-byte fixed rows
raw TransactionInfo_Log protobuf payloads
embedded address lookup
embedded positional-topic lookup
```

Each fixed row stores big-endian `blockNum`, `txIndex`, and block-global
`logIndex` (8 bytes each), `txHash` (32), `blockHash` (32), TRON address (21),
absolute payload offset (8), and payload length (8). The protobuf repeats the
address. Payloads are uncompressed and have no length prefix outside the fixed
row.

Embedded lookup layout is an eight-byte key count, a sorted fixed directory,
and fixed-width `uint64` row postings. Address directory entries are 37 bytes
per key (`21 + offset8 + count8`); positional-topic entries are 56 bytes per key
(`position8 + hash32 + offset8 + count8`). Every posting is eight bytes.

### External event-log-index

The V1 sidecar has a 56-byte header followed by the same two lookup layouts.
Its postings are candidate main segment start blocks, not row ordinals. It is a
performance hint, not authoritative log data.

## Construction and lifecycle audit

Canonical construction reads each strict block and its strict
`TransactionInfo` rows, validates transaction coverage, then emits rows in
`(block, tx, block-global log)` order. Cold re-materialization requires complete
event-log coverage and sorts reader rows through ETL before writing.

The writer creates a temporary file in the target leaf directory, reserves the
header and fixed row region, streams payloads, fills fixed rows with `WriteAt`,
appends both embedded lookups, backfills the header, fsyncs the file, computes
size and SHA-256, renames to a content-addressed path, and only later integrates
the new main/sidecar pair into an atomically renamed manifest generation. Old
overlapping refs become retired; a separate pruner removes retired physical
files only after active and retained published-manifest lease checks.

The signed fetch path verifies catalog signature, immutable manifest checksum,
every segment size/checksum, and semantic companion correctness before
publishing the local catalog view. Fetch is resumable and fsyncs downloaded leaf
directories. Restore materializes latest/history state into Pebble but keeps
event-log files as immutable cold readers. Snapshot installation derives the
event-log build stage only from continuous main plus external-index coverage.
Freezer-tail pruning refuses to advance without that indexed coverage.

One crash-safety gap exists in local main/sidecar writers: after renaming a file
inside `log/`, they do not fsync that leaf directory before the root manifest is
published. Root-directory fsync does not make a nested directory entry durable
on POSIX. Any V3 publication/migration must fsync the leaf after each content-
addressed rename; the same hardening should be applied to legacy writers.

## Query and API semantics

The production chain is:

```text
eth_getLogs / eth_getFilterLogs
  -> TronBackend.GetLogs
  -> ChainDB.IterateCoveredEventLogs
  -> snapshots.Manager.IterateCoveredEventLogs
  -> external candidate segment lookup
  -> embedded candidate row lookup
  -> protobuf decode and semantic validation
```

Address values within one filter are OR. Topic values within one position are
OR, different positions are AND, and an empty position is a wildcard. The
topic position is part of the lookup key. Readers must return complete
`rawdb.EventLog` metadata and strict `(blockNum, txIndex, logIndex)` order with
no duplicates.

Cold coverage is currently all-or-nothing for the requested range. If cold
event-log cannot cover the whole range, `GetLogs` scans the hot/freezer block
and receipt path for the whole range rather than joining a cold prefix with a
hot suffix. This avoids a normal cold/hot deduplication step today. A future
partial-range join must explicitly sort and deduplicate by block/tx/log while
also checking block and transaction hashes.

External and embedded lookups may not cause false negatives. Corrupt or absent
lookups can fall back to the authoritative fixed rows and protobuf payloads
when continuous main coverage is intact. Corrupt authoritative metadata or
payload is a hard error.

`eth_getTransactionReceipt` does not read event-log segments. It reads
`TransactionRet` from chain-freezer/ancient data first and hot rows second, then
computes block-global log indexes. Event-log V3 must not accidentally create a
new receipt dependency or permit pruning the freezer receipts that receipt
queries still require. Real-time filter changes and websocket log subscriptions also
consume new blocks rather than cold event-log snapshots.

### Verification amplification found by this audit

Current filtered lookup verifies the external sidecar, checks candidate main
segments, and full-scans non-candidate main segments to prove the sidecar did
not omit a match. Without a trusted verification cache, the sidecar therefore
does not provide the expected end-to-end bound on segment opens or bytes read.
Strict manifest verification also performs repeated full passes while checking
registered main files and then proving sidecar equivalence.

Performance benchmarking must keep three measurements separate:

1. raw lookup structure cost;
2. current end-to-end query cost including semantic re-verification;
3. a pinned, already-verified manifest/segment cache path.

The space inspector intentionally does not reuse strict manifest verification,
because doing so would add multiple passes over roughly 299 GiB.

## Read-only space benchmark

The command is:

```bash
gtron snapshot event-log-space-benchmark \
  --snapshot.dir /path/to/state-snapshots \
  --snapshot.event-log.sample-segments 16
```

Set the sample count to zero to scan all active main segments. Positive samples
are deterministic and evenly spaced, and each selected segment is scanned in
full. Output is JSON.

The command:

- loads `manifest.json` exactly once through `LoadProductionManifest`;
- retains that generation and its sorted active refs without following later
  manifest updates;
- never constructs a normal auto-refreshing Manager;
- never opens ChainDB, Pebble, ancient data, or chaindata;
- performs only `os.Open`, `Stat`, and `ReadAt` on immutable active refs;
- rejects a selected file whose physical size differs from the pinned
  manifest ref;
- reports the pinned generation and file path on failures rather than silently
  following a new manifest.

The command-level regression test holds the target Pebble database lock while
the tool runs and compares all manifest/segment bytes before and after.

For selected V1/V2 main files it reports exact physical bytes for header,
fixed rows, protobuf payload, embedded address postings, and embedded topic
postings. External sidecars are included only when their entire covered main
range belongs to the selected set; a shared sidecar is not charged to a partial
sample. Manifest-wide main-plus-sidecar bytes remain exact from active ref
sizes even for a sample run.

It reports exact payload-size and topic-count quantiles using value histograms,
plus row count, repeated block/transaction hashes in canonical row order, zero
hash counts, and exact distinct/repeated addresses for the selected rows.

## Candidate simulation

The current simulator merges all selected main segments into one modeled
segment. With a non-contiguous sample this is a comparative compression model,
not a deployable merge range. It reports assumptions and does not extrapolate a
sample to the full manifest.

The modeled row stream uses 256-row frames with a 24-byte frame directory
entry. Block, transaction, block-global log, prior payload length, payload
length, transaction dictionary ID, and address dictionary ID use unsigned
varints with frame checkpoints. Transaction hashes are stored once in a
32-byte segment dictionary. The embedded address lookup's sorted 21-byte keys
also serve as the address dictionary, so row IDs do not require a second key
copy. Addresses are removed from cloned protobuf payloads while topics, data,
and protobuf unknown fields are retained.

Block hashes are compared as two explicit alternatives:

- `chain-freezer`: no block hash bytes in the event segment, but one additional
  freezer point lookup is required and its byte/latency cost is deliberately
  marked outside the event-log estimate;
- `segment-dictionary`: sorted logged block deltas plus one 32-byte hash per
  logged block, preserving event-log self-containment when canonical data is
  unavailable.

Address and positional-topic postings are row-delta varints split into at most
4 KiB frames. The model includes fixed key directories, frame directories, and
physical posting bytes. A modeled external sidecar collapses every key to the
single merged segment start.

Protobuf-without-address payloads are packed on row boundaries into independent
seekable Zstd frames at 16, 32, and 64 KiB targets. An oversized row receives a
single oversized frame. For each size, output includes compressed physical
bytes, frame-directory bytes, payload frame count, maximum compressed bytes read
for one row lookup, and maximum raw bytes decompressed. The point-read estimate
also includes one row frame, direct dictionary values, and frame-directory
records. A separate upper bound covers one address/topic lookup key; an
arbitrary number of OR keys is necessarily unbounded by the on-disk format.

Merge factors 1, 2, 4, 8, 16, and 32 report projected main and external file
counts and posting collapse using exact selected-segment row boundaries. Sample
posting projections are labeled representative; file-count arithmetic uses the
full active counts.

All candidate results contain measured comparison bytes, estimated candidate
physical bytes, signed savings in milli-units, and explicit limitations. They
are not a V3 commitment.

### Synthetic microbenchmark result

`BenchmarkEventLogSpaceSyntheticFixture`, run once on an Apple M1 Max, scanned
2,560 logs from 256 blocks. The fixture has one transaction per block,
deterministic pseudo-random data, cyclic addresses, and zero to four topics. It
is useful for formula and random-read-bound checks but is not mainnet-
representative.

Measured current physical bytes were 1,981,460: header 88, fixed rows 320,000,
protobuf payload 964,862, embedded address lookup 29,775, embedded topic lookup
327,688, and external sidecar 339,047. Payload p50/p95/p99 were 376/586/651
bytes. The fixture's external sidecar share is roughly 17%, versus 4.21% in the
production inventory, which is itself evidence against extrapolating this
fixture.

The chain-freezer block-hash candidate measured 1,751,926 / 1,749,737 /
1,748,675 bytes for 16/32/64 KiB payload frames (11.5%/11.6%/11.7% modeled
savings). The segment block dictionary added 8,448 bytes and produced
1,760,374 / 1,758,185 / 1,757,123 bytes (11.1%/11.2%/11.3% savings). Maximum
modeled payload decompression was 16,373 / 32,763 / 65,534 bytes. The tiny
64-KiB advantage on this synthetic input is not enough to choose a production
frame size.

## V1/V2/V3 compatibility and migration boundary

A future V3 must use a new magic and a separate header/parser/checker branch.
It must not reinterpret V2 magic. Manifest dataset/kind can remain stable, so
one manifest may contain adjacent V1, V2, and V3 segments. Legacy 125-byte row
decoding and V1 full-scan behavior remain available until every retained and
published manifest lease no longer references them.

The V3 checker must reconstruct the same semantic row stream and verify:

- frame directories, monotonic bounds, and non-overlap;
- row-frame delta reset and strict global ordering;
- dictionary IDs, uniqueness/canonical ordering, and hash/address recovery;
- payload frame checksums, decompression bounds, protobuf decoding, address and
  topic semantics;
- embedded lookup key order, posting order/range, and complete equivalence to
  the reconstructed authoritative rows;
- external sidecar equivalence to the active main segment set.

If block hashes come only from freezer data, freezer coverage and checksum
identity become part of event-log coverage, manifest verification, restore,
and prune gates. A segment block dictionary avoids that new correctness
dependency and should be the default comparison until production measurements
show the extra bytes are unacceptable.

Migration must be crash-safe and idempotent:

1. pin and fully verify source V1/V2 refs from one manifest generation;
2. build V3 main and companion files under same-leaf temporary names;
3. fsync each file, calculate complete size/checksum/self-check metadata, rename
   to content-addressed names, and fsync each leaf directory;
4. reopen and exhaustively verify new V3 semantics against source rows;
5. atomically publish one new manifest generation containing both new refs and
   retiring the covered old refs, then fsync the root directory;
6. advance the hash-bound event-log build stage only after manifest durability;
7. leave old files recoverable through retired and published-generation leases;
8. permit restart/retry to reuse checksum-identical completed objects or remove
   orphan temporary files without changing the active view.

A source-generation change, missing pinned source file, checksum mismatch, or
semantic difference aborts before publication. Re-running the same migration
must produce byte-identical content-addressed outputs and either integrate them
once or recognize that the target generation already covers the range.

## Required evidence before V3

The repository fixture can verify formulas, compatibility, Zstd framing, and
query equivalence, but it is not representative of mainnet. Existing benchmark
fixtures use zero or synthetic hashes, one transaction per block, fixed topic
counts, repetitive data, and a small cyclic address set. The smoke chain fixture
does not carry representative `TransactionInfo_Log` artifacts.

Before selecting V3, run deterministic samples and then a full scan on the
production immutable snapshot. Record per-component physical bytes,
distributions, candidate compression, point-read/decompression bounds, elapsed
time, peak RSS, and the pinned manifest generation. Separately benchmark current
end-to-end filtered queries and a trusted verification-cache design. Only then
choose block-hash ownership, payload frame size, merge target, and rollout plan.

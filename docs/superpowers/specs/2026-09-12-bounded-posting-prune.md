# Bounded cold-covered posting reclamation

The August 10 posting format remains unchanged. The existing full posting and
directory sweep waits for catch-up to finish; a September 12 production sample
estimated 37.14 GiB in the index namespace and no recorded completed full sweep.

An opt-in `GTRON_POSTING_PRUNE=1` worker reclaims only immutable posting frames
whose final block is at or below a fixed cold-covered hot-prune boundary H.
Directories and mixed frames remain intact. The old full sweep is disabled when
this worker is enabled. No consensus, reader, cold-file, schema or format changes.

The normal snap-mode hot pruner publishes an in-memory permission only after a
successful verified cold-coverage prune pass and durable manifest publication.
It includes H and the canonical prune-head hash captured before that pass. The
worker never polls the approximately 35 MB manifest on its one-second cadence.
Startup waits for a new successful hot prune; an existing manifest alone does
not authorize a sweep. Every chunk checks the permission's proof hash, durable
Finish and StateHistoryIndex hashes/watermarks, current head/solidification and
the sweep's fixed canonical H hash. Locks in order stateHistoryIndexMu -> chainmu
serialize chunks with indexing and rewinds. The index mutex is tried without
waiting; the chain mutex registers a waiter, so the importer can hand it over
at a block boundary. Instantaneous TryLock on both mutexes produced zero chunks
in the initial online observations and only one successful chunk in the full
ten-minute window despite repeated gate admission. Both remain
held through batch submission. Chain-lock waiting is not cancellable mid-wait;
context is checked immediately after acquisition, so a canceled worker does no
scan or write. Stop joins that wait. Slow holders can exceed a block interval,
and the total callback metric includes waiting; it is not exact lock-held time.
Offline restore/reset still requires
stopping the node. Ordinary writers append above the verified prefix.

Each chunk owns and releases its iterator before submitting one delete batch.
Initial budgets: 4,096 rows, 1 MiB encoded scan bytes, 256 KiB encoded deleted
bytes and 10 ms scan time, followed by a one-second interval. Byte/time budgets
are cooperative between complete frames; a single row or storage call may
overshoot. Malformed large frames fail before decoding. There is no forced
compaction. Shared maintenance admission and fresh engine/device pressure
checks defer work on stalls, high L0/debt, high device latency or unavailable
telemetry; a long lease also inherits the shared recovery cooldown.

The exclusive cursor advances only after successful batch.Write. Errors retry
from the old cursor; process restart begins again, idempotently. No persistent
cursor and no StateChangeIndexPruneBlockNum update: that legacy watermark means
the full posting plus directory sweep. A finished pass waits for 262,144 more
eligible blocks. Regressed/invalid proofs discard the current cursor and wait
for a fresh permission. Metrics distinguish scanned/deleted logical bytes,
completed sweeps, deferred chunks, errors and total chunk duration. Logical
deletions do not imply immediate filesystem reclamation; Pebble compaction and
the known obsolete-table accounting defect constrain space interpretation.

Validation covers exact/prefix/as-of reads before/after actual Pebble compaction,
mixed frames, bounded progress, cancellation and ambiguous writes, chain proof
failures and concurrency, queued handoff and canceled waiters, worker
lifecycle/admission and restart. Deployment
pins source and binary, preserves observer flags and rollback, and samples
canonical agreement, sync/backlogs, stalls, compaction and logical cleanup.

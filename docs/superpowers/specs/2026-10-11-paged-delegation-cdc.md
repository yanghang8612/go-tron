# Paged delegation and CDC-only cold history output

The October 11 census found that `drax-` delegation lists account for 86.05%
of historical previous-value logical bytes, despite only 0.0478% of records.
Decoded-list caching does not solve this: each mutation still serializes and
versions the entire list. Existing R1 containers also retain approximately
72.8 GiB of uncompressed span directories. These are separate sources of work.

## Delegation layout

Use the existing generic SystemDelegation KV implementation for a small DLP1
descriptor at the logical `drax-` anchor and independent DPP1 ordered pages.
Each page contains at most 128 list entries. Schema-owned `drap-` keys include
the length-prefixed account, direction and page number. The descriptor holds
the account, timestamp, unknown protobuf fields and page counts. Ordinary
append/remove operations version only affected pages and a changed descriptor;
they never serialize the full aggregate into historical changesets.

API reconstruction preserves Java list order, duplicate handling, arbitrary
entry bytes and unknown fields. Removal compacts only affected pages. Generic
KV snapshots, undo, generation changes and archive AsOf reads continue to own
version semantics. Decoded caches must be invalidated by any anchor/page write,
and cache hits must re-emit the dependencies needed by optimistic execution.

There is no aggregate-value runtime fallback and no latest-only conversion.
Latest-only conversion would make earlier archive states unreadable in the new
representation. This change requires a fresh database and genesis sync. Normal
startup and init reject unmarked existing chains; a fixed-key format marker
admits new chains and subsequent restarts without scans or history validation.
Offline cold-container maintenance remains usable independently of node startup.

Snapshot catalog identity does not certify this storage layout. Runtime snapshot
bootstrap is therefore unavailable for this fresh-genesis-only release. Offline
state imports invalidate any target layout marker before writing state; they
cannot inherit startup admission from an initialized fresh target.

## R1 retirement

Reuse the existing CDC V3 container, existing companions and the offline
per-trio publication mechanism. Extend the converter to read R1 as well as V2;
do not introduce another reference format. Stream logical bytes into CDC while
hashing, then read the completed output once to compare its logical digest.
Keep cancellation and overflow-safe work-space admission. Replaced immutable
files are deleted only after the new manifest is durable. A small existing
journal resumes interrupted publication and retirement.

Do not assume CDC is smaller merely because R1 has directory overhead. Compare
the complete same-input encoded result and skip outputs below the requested
savings threshold. Ordinary compaction with reference output disabled must
emit streaming output even when its inputs include R1. R1 output remains an
explicit opt-in, rather than being inherited from old source files.

## Validation

Test Java-equivalent ordering and duplicates, page boundaries, self-delegation,
metadata, transaction revert, generation/cache invalidation, malformed data and
historical reads. Measure per-transaction history bytes at different list sizes.
For cold rewriting, compare exact decoded history, indexed queries and complete
physical file sizes; exercise no-gain, cancellation, failure/resume, and disabled
reference compaction. An independent Astra audit follows implementation.

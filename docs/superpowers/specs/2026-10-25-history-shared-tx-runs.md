# Shared transaction runs for history postings (offline V11)

User authorization: implement shared posting transaction ranges; census every
previous value in blocks 25M–35M; compare net storage and full HTTP eth_call on
the SAME 11,818,192 fixed capture. Preserve values, ordering, MuHash, history,
verified import and actual restore. No push, deployment, service restart, reset,
production budget edits or production database mutations. Reuse the existing
worktree and P3 immutable V10 outputs; no subagents.

V11 removes per-row TxNum from multi-postings, retaining delta ordinals, framing,
and first-Tx seek metadata. Inline singletons keep their complete tuple. A
self-contained table appended to each accessor maps monotonic TxNum runs to
contiguous ordinal ranges; V9's shared ordinal->offset pages remain. Canonical
keys, IDs, dictionary digest, .seg/.idx bytes, before-images and MuHash element
encoding remain unchanged. V10 owners remain optional, with full V9-style keys
when owner dictionaries are not beneficial. All existing versions stay explicit.

Runs are grouped into 256-run pages, encoded with positive transaction deltas
(first delta zero) and positive row counts. A 32-byte directory entry binds
first TxNum, first ordinal, covered rows, 48-bit relative offset, stored length,
run count, reserved bytes, page-index/entry/payload CRC. A 64-byte footer binds
section lengths, run counts, page/entry widths, owner-presence flag, directory
CRC and footer CRC. The authenticated complete directory is capped at 1 MiB;
the builder can choose V10 BEFORE consuming posting ETL if this cap is exceeded
or all rows are singleton keys. Empty/oversized output never drops history.
Pages are <=256*15 bytes; per-open-reader decoded page cache is <=2 MiB, with
synchronized ownership and no cross-source/global cache. Directory coverage,
run and ordinal monotonicity, transaction bounds, page CRC and all arithmetic
must be checked. Whole-table verification checks exact row/run coverage.

Range queries convert TxNum bounds into ordinal bounds once per segment/request,
filter ordinal frames before resolving selected TxNums and offsets, and read
only selected values. Never scan .seg to rediscover row offsets. Generic cursors
and strict verification reconstruct complete tuples and compare against the
authoritative .seg; a forged run table with recomputed CRC/SHA must fail joint
semantic validation. No unconditional per-file savings claim: measure all table,
key-metadata and framing costs and preserve any growing files in the accounting.

Production streaming builds capture runs in chronological CollectPosting order with
bounded scratch files. Offline transformations derive an independently checked chronological vector
from the authenticated V9/V10 posting ordinals, reject duplicate/missing ordinals,
and compare every old/new transaction/ordinal and the byte-identical locator. Tests cover repeated
TxNum, gaps, zero/MaxUint64 bounds, 255/256/257-run pages, overflow/corruption,
optional owners, old readers, bounded cache/read behavior, cancellation,
streaming/compaction and strict restore/import.

Census must pin immutable .seg/.kv pairs without stopping the service, prove real block
coverage for [25,000,000,35,000,000], handle boundary segments by exact ranges,
separate per-domain logical old-value bytes from compressed .seg file bytes,
and never extrapolate the 33.248M accessor cohort to complete 35M. Missing files
are errors requiring recovery from retained immutable sources, never skipped.
Run census/conversion with explicit bounded resources; end heavy jobs before
HTTP timing. Keep every timing sample, regression and original evidence.

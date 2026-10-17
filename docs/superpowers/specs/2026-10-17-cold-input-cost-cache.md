# Busy cold-history admission cost reuse

The busy streaming selector repeatedly scans original leaves whose physical size
fits 128 MiB but whose record/logical cost cannot fit its merge budget. Reopening
their history footer and accessor header on every pass adds avoidable metadata IO.
Physical size alone is not a representative rejection filter.

A private Runner-owned volatile cache reuses successful admission costs only for
BusyLeafOnly state-domain-change streaming compaction. Ordinary/reference paths
retain their existing cost reader. The initial manifest is borrowed read-only;
the Integrator still loads its own writable manifest.

Each key owns copies of every string in the complete history/accessor/inverted
SegmentRef trio. No manifest string slab or companion slice is retained. Directory
path, directory identity and staging-reset epoch bind a cache instance. Each
lookup freshly Stats all three files and compares identity, size, mode and mtime.
Missing/changed/unknown identities never suppress errors. Misses reuse that fresh
before observation, read the original metadata, then validate an after observation.
Errors and cancellation are never stored.

Before any proof claim or payload/output work, every selected input takes the
original metadata reader again, with before/after file observations. All source,
physical, logical and record limits are checked. Different admission costs defer
the whole group without claims or publication, retaining the fresh cost so the
next pass can select again. Normal checksum and codec authentication remain in
force, including mutations invisible to inode/size/mtime observations.

The cache retains at most 32768 entries and 64 MiB of conservative charged
metadata. It charges 2048 fixed bytes plus twice the owned reference strings and
complete fresh Stat path lengths, with overflow checks. The bound directory path
is owned separately once. This is not a process RSS limit. At capacity it rejects
new keys and keeps the historical prefix, avoiding a sequential-scan FIFO flush.
Removed catalog keys may retain bounded slots until an epoch/directory reset;
the uncached suffix continues through the original reader. Therefore the
O(leaves + selected) header-read claim only applies to a working set that fits.

Validation uses real compressed V6 history trios: 256 old record-over-budget
leaves and four eligible leaves over five passes. Identical selection is required;
full metadata-reader calls, including selected fresh reads, are 1300 versus 280.
The cache's extra Stat calls are measured through a private per-instance seam;
original-reader Stat calls are a separately labelled code-path model. Same-size
replacement, corruption, missing files, cancellation, epoch/directory replacement,
companion changes, backing-string ownership, capacity degradation and selected
cost changes/authentication are negative controls. Sequential warm-FS wall times
are not evidence of production throughput improvement.

A capacity model over the local 2026-10-14 manifest's 19217 complete original
trios, using the production directory path, charges 71,743,036 bytes. In fixed
catalog order, a fresh 64 MiB cache admits about 17981 trios (93.57%), leaving an
uncached suffix. This is a local retained-charge model, not an observed production
hit ratio or read reduction: real selection may skip merged inputs or finish at
an eligible group before the end of the catalog. The small 260-leaf all-fitting
test therefore does not establish whole-production O(leaves + selected) behavior.

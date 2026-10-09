# Offline hot history-index cleanup

## Scope and invariants

This is an offline, separate operation from physical Pebble compaction. It may
delete only complete state-change posting frames and orphaned state-change key
directory rows. It never deletes history payloads, shared chunks, canonical
chain or current-state rows, history staging routes, receipts, commitment
generations, or bucket zero. It never advances the global hot-prune progress.
The stopped mainnet service, deployment timer, and other automatic starters
must remain fenced throughout the operation. The staging Pebble store keeps
its exclusive read-only handle for the entire operation. Switching the hot
store from read-only to read-write releases and reacquires Pebble's own lock;
the external start lock and deployment fence must cover that gap, followed by
complete guard and cold-file revalidation before any write. The cold manifest
is pinned by exact file SHA-256.
Dry-run is the default and performs no logical write or billions-row posting
scan. `--yes` is a separately reviewed operation.

## Authorization of the deletion boundary

The operator selects an explicit first height of 1024 and an inclusive last
height H at a complete bucket boundary. Buckets 1 through H/1024 must be a
contiguous prefix of current-epoch COLD routes. Missing routes, any current
epoch claim, SOURCE or TARGET owners, reset intent, missing route barrier,
or an inconsistent cold binding stop authorization. Any reset intent is a
conservative stop for this offline tool, even if normal startup could finish
or recover that intent. H cannot exceed
the strict canonical solid height, durable Finish height, or durable state
history index height. The source and target staging identities must match.
These checks use schema-owned accessors, not guessed key prefixes.

Each COLD binding must be the durable semantic receipt for its route. A COLD
bucket need not have a separate TARGET migration receipt; requiring one would
reject legitimately certified cold-only buckets. A
pinned snapshots manager verifies every binding receipt against its manifest
trio and the actual companion file checksums. A task-local receipt audit records
the strong file fingerprints for every authenticated trio; it rejects a file
change during the long audit or at any later recheck. The audit rechecks all
recorded file identities before switching from read-only to read-write and
again after writing. It does not treat a global checksum-cache hit or a bare
ContentID as proof if the physical file changed. The manifest bytes and SHA,
epoch, reset state, barrier, route/binding digest, stage identity, and chain
boundary must also remain identical across that transition. A mismatch aborts
before writing. A stopped process can retry from scratch; no marker may
declare a partial scan complete.

## Posting and directory decision

The posting iterator scans the entire schema-owned posting range. It strictly
decodes every frame, including its ordered block numbers. A frame is removable
only when its minimum block is at least 1024 and maximum block is at most H.
Every other frame is retained, including frames crossing H, bucket zero, and
all later or malformed-looking coverage; malformed encoding aborts the job.
The retained frame's 32-byte key hash is added to a fixed 512 MiB,
seven-probe Bloom filter. Bloom saturation or collision can only keep extra
directory rows: there must be no false negative for a retained frame.

The directory pass starts only after the full posting iterator reports no
error and the final posting delete batch has successfully written and synced.
For each directory row, the schema accessor computes the same hash from its
latest-key bytes. A Bloom-negative row is orphaned and may be deleted. A
Bloom-positive row stays, even if it is actually orphaned. The Bloom filter
is rebuilt on every retry. The operation never uses the hot-changeset fallback
or existing watermark-only posting pruner: staging may own history no longer
visible in the hot changeset table.

## Bounded writes and failures

The logical-delete writer is an ordinary Pebble handle with automatic
compaction enabled, one compaction worker, 64 MiB memtable, and 256 MiB cache.
This is separate from the physical maintenance handle that disables automatic
compaction and limits its own SST allocation. Delete batches are at most
16 MiB, the total logical key-plus-value delete budget is 256 GiB, and free space
must be at least 128 GiB before each batch and sync. These are admission and
work bounds, not a hard filesystem-usage ceiling. Only a successful Write
**and** Sync increments committed counters. A Sync error leaves the tail
batch's durability uncertain and returns the original error; retry scans the
remaining live rows. Iterator, decode, budget, free-space, write, sync, or
authentication errors stop the operation. In every case after opening a writer,
close and reopen the hot database while holding the offline-operation lock,
then compare strict canonical, commitment, staging identity, and selected
current-state value canaries to the pre-write snapshot. Never report an error
as successful cleanup merely because a partial batch was committed.

The final physical reclaim, if desired, uses the separately reviewed
`hot-maintenance compact` command and its own SST budget. Logical deletion
counts and Pebble overlapping-SST estimates are not physical reclaimed bytes.

## Evidence and tests

The read-only report records fixed manifest SHA, epoch, route/binding digest,
first/last eligible bucket, H, strict chain/index boundary, authentication
count, and only schema-owned SST estimates. The writing report additionally
records scanned/retained/deleted posting and directory counts, committed
logical key-plus-value bytes, durability-uncertain tail, last completed phase and error.
It emits progress at least every 30 seconds during full scans.

Tests use real Pebble fixtures for bucket-zero and crossing frames, missing
or claimed COLD routes, wrong receipts or changed trio bytes, conservative
Bloom collisions/saturation, iterator failure, write and sync failure,
free-space and total-budget refusal, restart after partial posting deletes,
and unchanged protected state after success and failure. A dry-run must not
mutate hot, staging, or cold files and must not scan the full posting range.

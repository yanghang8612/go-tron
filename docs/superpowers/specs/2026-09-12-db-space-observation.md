# Database space observation

## Motivation

The September 12 production inspection found chaindata at 156.51 GiB versus
89.99 GiB in the previous day's allocated-byte directory baseline. State and
body archival continue, but existing metrics cannot distinguish major hot key
families, obsolete tables, WAL recycling, and snapshot retention. This change
adds observation before changing any retention or compaction policy.

## Contract

Reuse each existing three-second Pebble Metrics snapshot to publish engine
space components, open snapshots, cumulative snapshot-pinned output and
estimated tombstones. Preserve existing names: table/zombie counts memtables,
not SSTables. Snapshot pinned keys/bytes are cumulative output retained during
flush/compaction, not current retained or reclaimable disk bytes. Components
calculated from the same Metrics object are published with an observation time;
registry scraping still is not atomic.

An optional worker, enabled by GTRON_DB_SPACE_OBSERVER=1 for writable rawdb
construction, estimates one of twelve schema-owned ranges at a time. Empty/0
disables it and invalid values fail before opening the database. Read-only
offline opens do not run the worker. Engine counters remain inexpensive and
available without this switch.

The fixed ranges cover account latest, account KV latest, KV generation, code,
commitment, changesets, change indexes, tx ranges, staged bodies, block bodies,
transaction indexes and receipts. They do not cover the whole keyspace. KV
latest includes several rooted domains and must not be labelled contract
storage alone. Bounds and names are copied and validated before DB open;
there are at most sixteen ranges and no unconstrained/full-database range.

The worker waits thirty seconds after each completed estimate before the next
range. It has one in-flight call, no retry bursts and no value iteration. The
Pebble estimator reads SST metadata/index blocks and may share boundary blocks
between otherwise disjoint key ranges. Pebble's end bound is inclusive. The
estimates exclude WAL, include retained versions in current SSTs, are sampled
at different times, and are neither additive accounting nor reclaimable bytes.
There is no exact other = disk minus estimates metric.

Each range reports known, last successful time, bytes, last attempt time and
duration, attempts and errors. Failed attempts retain previous successful bytes
and their timestamp. A process-local owner identifies each observer instance.
The first open database owns new space metrics within its namespace; auxiliary
opens with the same namespace publish neither engine space gauges nor range
estimates. Closing a secondary DB cannot reset the owner. A new open can acquire
the namespace after the owner's workers and database close; existing secondary
opens do not automatically take over.
HTTP exports registered gauges only; requests never trigger estimates. The
worker is independent of the existing Pebble meter and import/maintenance locks.
DB Close signals and joins it before closing Pebble. A native estimate cannot
be interrupted: shutdown waits for an in-flight call, and no detached timeout
goroutine is created. Fixed concurrency/ranges do not impose hard per-call IO
or time bounds.

## Validation and rollout

Check range coverage, disjoint logical bounds, defensive copies and the default
off switch. Exercise error/stale-value semantics, serial rotation, stop during
an in-flight estimate, and live Pebble snapshot retention/release. Use relevant
race tests, repository tests and native Sapling checks. Preserve wire/storage
formats, policies, current service settings and the existing cache observer.

Publish on master through GitHub and build a pinned isolated server release.
Activation changes only the executable and new observer assignment, with the
current unit and executable retained for rollback. Validate engine metrics,
observer enabled/owner, a complete twelve-range rotation, canonical progress,
backlogs, compaction/stall/errors and resource usage. Cross-process counters
must not be subtracted. New values will refine the existing growth report;
they do not by themselves authorize or require deleting files.

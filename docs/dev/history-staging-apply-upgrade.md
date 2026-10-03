# Controlled executor replacement for a published history-staging plan

The original plan remains immutable and content addressed. Its
`candidate_sha256` names the **plan producer**. Progress JSONL, executable
verification, activation, and the final route barrier name the **actual running
executor**. These identities may differ only through the root-owned upgrade
journal; there is no arbitrary `--plan-candidate-sha256` override. Claim IDs
continue to derive from the original plan digest and bucket number.

The operator first installs the reviewed startup guard and release helper,
stops every writer and deployment unit, and leaves the migration latch in
`MIGRATION_IN_PROGRESS`. Then run the reviewed migration helper's
`apply-repin --job-id JOB --candidate BINARY --source COMMIT --sha256 SHA
--legacy-manifest-sha256 SHA`. It verifies root ownership, the stopped service,
timer and deployment units, exact storage paths, the frozen configuration and
manifest, the old prepared/latch identity, the new binary build identity and
capability. It holds the start lock and all three storage migration locks.
`apply-repin` performs the handoff only; the existing `resume --job-id JOB`
subsequently continues the same plan.

A pending `HISTORY_STAGING_REPIN_INTENT.json` marker fences ordinary resume,
CLI operations, service startup, deployment, rollback and activation. The
root-only `0600` authorization journal lives at the fixed path
`/var/lib/gtron-history-staging/apply-upgrade.json`, inside a root-owned `0700`
directory with trusted ancestors. It contains exact old/new executor identities
and complete old/new latch and prepared snapshots, original producer SHA and
plan ID. The durable sequence is pending intent → `PRECHECK` → read-only
`upgrade-check` → `AUTHORIZED` → prepared switch → latch switch → `DONE` →
remove pending intent. Every boundary can be retried with precisely the same
arguments; a different candidate or contradictory snapshot fails closed. An
intent written before its journal is reconstructed only from the exact original
snapshots and matching request. A failed preflight keeps the pending fence.

The CLI checks the root parent's identity/start time, all four exact exclusive
BSD locks and matching parent file descriptors. Its own read-only Pebble opens
retain the separate storage `LOCK` protocol. It fully hashes and parses the
original plan, validates its frozen chain/configuration/retention/work-limit
inputs, manager identity and epoch, and bounded per-bucket claim, route, receipt
and cold-binding metadata against that plan. It accepts the legal interruption
between durable target receipt and source `TargetReady` publication. It does not
scan all historical payload or cold trio records during executor preflight;
normal apply/resume and final verification still perform their data proofs.
Orphan private target payload is not promoted to a readable owner by this check.
Pending reset, a completed route barrier, an active reader marker, pending
activation, conflicting paths, changed frozen inputs or conflicting claims are
rejected. The old producer SHA never appears as a substitute for the actual
executable SHA.

The new latch carries `plan_producer_sha256` and the fixed `upgrade_binding`.
Only a `DONE` journal consistent with the current reader fences authorizes
ordinary apply/resume/inspection. Activation may add its ordinary lifecycle
fields to the latch, but cannot change the frozen identity. The existing release
ownership handoff remains mandatory before starting the service reader.

This version authorizes **one executor replacement per migration job**. Repeating
the exact completed request is idempotent. A second different candidate is
rejected without changing the completed journal or reader fences; additional
generations require a separately reviewed controlled protocol.

Tests cover every durable switch boundary, pending startup/deploy/activation and
resume rejection, forged producer/executor identities, path and prepared
conflicts, exact claim/receipt metadata, and a real dual-Pebble reopen after the
receipt-before-ready interruption. The native Linux root integration runner
`scripts/dev/history_staging_upgrade_root_test.py` uses an isolated mount
namespace with temporary fixture databases and fences. It checks the actual
executable SHA and root parent `/proc` locks, wrong-lock and no-binding rejection,
frozen-input mismatch, original plan immutability, and partial claim resume.
Its original producer is a sealed fixture identity; it does not execute a
production old binary or modify production migration files.

The production start lock has a different ownership contract from the storage
locks. `/data/gtron/start.lock` is shared with deployment: `start.sh` opens it
as `java-tron` before invoking the root helper. Both executor preflight and
release ownership handoff accept exactly a regular, single-link, non-symlink
start lock with either root:root mode `0600`, or the UID and GID resolved from
the trusted `java-tron` system account/group with mode `0644`. The service UID
and GID must be nonzero and the user's primary GID must match that group. No
owner discovered from the lock file and no CLI override can authorize a new
account. A missing service account does not disable the root-private branch.
All three storage locks remain root:root mode `0600`. Every branch still
requires the root parent's start time, exact exclusive FLOCKs, matching parent
file descriptors and unchanged lock path inodes. The helper does not chown,
chmod or replace the production start lock, preserving later deployment access.
Native tests change only their private namespace fixture inode to exercise the
service-owned `0644` branch and reject unknown owners, writable modes, symlinks,
replaced inodes, lost locks and service-owned storage locks.

Local proof measurements used an M1 Max, Go 1.27.1 on Darwin/arm64 and default
GOMAXPROCS=10. The dense V5 fixture processes 16 buckets at 512 records per
block: 8,388,608 real records per run. The new proof path measured 5.512 seconds
with one worker, 1.716 seconds with four, and 0.959 seconds with eight, retaining
one full trio authentication per run. The 5.75× ratio compares eight workers
with the new serial path; it is not a comparison against the production old
binary. On the smaller V6 fixture (32 buckets, 262,144 records), the old serial
path measured about 600 ms, the new bulk serial path 398 ms, and eight workers
about 577 ms. Parallel overhead therefore outweighs the bulk serial improvement
on that smaller input. These local fixtures establish proof-path behavior and
sample-dependent benefit, not production throughput, migration duration, host
memory, device I/O or final activation success. Native Linux root integration
and the original production job's progress remain deployment acceptance checks.

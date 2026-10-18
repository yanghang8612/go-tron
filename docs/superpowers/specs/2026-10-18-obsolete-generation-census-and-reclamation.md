# Obsolete generation census and reclamation

Status: census tooling implemented; production reclamation is NOT implemented or
enabled. The user authorizes an exact census, a reclamation design, and one brief
same-binary stop/capture/restart. Production database deletion, genesis resync and
a release are outside this task.

## Classification and measurement

The frozen chaindata HEAD defines the block/hash/transaction/source cut. Join
every physical KV owner to the generation table in this same immutable view.
An absent generation row means generation zero. Classify stored generations
strictly less than current as obsolete, equal as current, greater as anomalous
future rows. An absent account alone does not grant deletion permission: system
owners and historical account incarnations require separate treatment. Never
delete the generation counter, since doing so could make an old incarnation
addressable again.

Execution reads the generation from the account envelope. Exhaustively verify
every live envelope agrees with the generation table/default as a companion
pass. A mismatch makes classification unsuitable for reclamation even if the
MuHash root is internally consistent. The companion must refer to the exact
same HEAD/source image; it does not substitute for the full-source MuHash pass.

The census scans every account, generation and KV row, validates schema and
presence envelopes, hashes length-framed physical key/value streams, and rebuilds
the full normalized MuHash state. Both state and digest must equal the complete
HEAD. Report all row counts, physical-key bytes, wrapped-value bytes, payload
bytes, domain and size distributions, inactive owner/generation/domain groups
and bounded largest-owner results. Iterator/read/close errors, cancellation,
malformed records, pending restore/unwind and root mismatches produce an
incomplete report. The CLI takes an exclusive read-only Pebble lock and writes
only new reports outside the source database.

These are exact visible logical KV bytes. They exclude old LSM versions,
tombstones, compression, indexes, WAL and compaction scratch. An SST byte cannot
be assigned exactly to one obsolete row in a mixed compressed block. Physical
reclaim, if material, needs identically compacted disposable control/pruned
copies; never use logical bytes as a claimed disk saving.

## Why deleting obsolete rows is not an ordinary background prune

MuHash authenticates the complete physical source set, including obsolete
generations. Deleting a source row alone invalidates HEAD. Deleting the row and
leaving it in the accumulator makes full-source verification and snapshot
restore fail. Filtering it out only during snapshot export has the same failure.
Changing HEAD/root outside the canonical commit also conflicts with the bound
block identity, delta ID, retry semantics and historical block-root anchors.

Ordinary history queries are another constraint. Once an old generation row is
deleted, an as-of lookup before deletion still needs its last value. Existing
history stores before-images, not a free complete copy of every post-image. A
new deletion therefore needs the same temporal before-image/absence semantics as
other physical mutations, even if the generation is unreachable in current
execution. Such a record and its accessor have a real storage cost.

## Preferred protocol if the census justifies reclamation

Treat each reclamation as an explicit block-owned physical source transition,
using the existing element codec and accumulator. Do not replace the current
database or rebuild it from genesis.

1. Select only stored<current keys from a canonical, generation-bump-solidified
   view. Revalidate the current generation and exact captured physical bytes
   inside the same guarded parent view used by the committing block. Future
   rows, incomplete proofs and source errors are retained and reported.
2. Capture owned before-images for the actual stored generation; the normal
   StateDB convenience API resolves the current generation and cannot be used
   blindly to delete a different physical generation. Coalesce with that block's
   execution deltas, retaining first-before/last-after and no-op behavior.
3. Publish ordinary domain-history deletion before-images at the existing
   block-final transaction cut through the normal history lifecycle. Preserve
   generation/owner/domain/logical-key identity. Budget the history records,
   posting and eventual cold accessor as well as delete tombstones.
4. Remove each exact element from MuHash, then publish physical deletions,
   normalized successor HEAD, internal root, block root and canonical identity
   through the existing canonical layer/flush protocol. A separate callback
   must never rewrite the current HEAD in place. Backend failure poisons the
   process according to the existing durable-cut recovery rules.
5. Make selection and progress reproducible for stored-block replay. Prefer an
   explicit activation boundary, fixed data-based batch limits and deterministic
   ordering. Wall-clock deadlines or pressure-dependent skip decisions cannot
   silently change which keys a historical block deleted. If decisions require
   durable plans, their format, lifetime and replay authentication must be
   designed before enabling the worker.
6. Persist any cursor, activation and plan state in the same source/HEAD cut and
   transport it in the execution checkpoint. Restore must retain identical next
   batch behavior. Version required execution metadata explicitly; preserve
   existing snapshots through an explicit supported conversion/default policy,
   never by pretending omitted required metadata is present.
7. Reorg uses persisted before-images to restore deleted physical rows and the
   accumulator together. Recompute the historical target digest before
   publication. Cross-maintenance/missing-history replay must reproduce the GC
   decisions as well as ordinary execution.
8. Latest snapshots include every remaining physical row, including any
   uncollected obsolete rows. SourceRows and complete HEAD must match the pinned
   cut. Restore recomputes the entire source set and executes subsequent normal
   and reclamation blocks without a root or allocator mismatch.

The concrete batch limits, activation, cursor/plan representation and checkpoint
compatibility policy remain implementation gates, not assumptions already
covered by today's census. No worker should be enabled from this document alone.

## Acceptance and prioritization

- Zero or negligible obsolete bytes: do not add a complex GC protocol merely
  because the synthetic latest-snapshot regression exists. Keep the census
  reusable and prioritize measured larger storage families.
- Material obsolete bytes: first run a disposable, complete source/history/cold
  experiment including all added history/index bytes, WAL/tombstones and equal
  compaction. Require positive total physical savings and bounded foreground
  costs, not just a smaller latest table.
- Independent tests must cover absent/default generation, future rows,
  destroyed/recreated owners, empty physical values, repeated same-key writes,
  async depths, backend failures, restart, same-cycle and maintenance-crossing
  reorg, replay determinism, and snapshot restore followed by another GC batch.
- Full-set oracles must agree before and after each transition. Logical source,
  history queries, canonical identity and restored execution must be preserved;
  no TRON wire or consensus field changes.

Production activation requires a separately reviewed implementation and release
authorization. A failed test or unexpected marker is a stop for that operation,
not permission to clear the database or start from genesis.

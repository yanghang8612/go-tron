# Compact Storage and Shielded index: fresh-state design

Status: the distribution/SST analysis is complete. The follow-up implementation
is documented in [compact-state-format-v2.md](../../dev/compact-state-format-v2.md).
It keeps all 32 logical slot bytes, shortens the physical table namespace and
compacts values in both hot latest and cold latest segments. The experimental
44-byte key representation below is not the implemented format. Production has
not been reset or deployed by this implementation task.

The design changes local representation, not TRON wire messages, TVM storage
semantics, shielded anchor validation or consensus outputs.

## Measure before choosing the representation

Use the same immutable source cut as the generation census. Match each source
family's complete row count and length-framed SHA-256 to that authenticated
baseline. Report all Storage owner/incarnation rows, byte/value distributions,
slot-prefix uniformity and metadata agreement. Compute exact value frequencies
by bounded-memory external sorting, rather than extrapolating a sample.

Classify every SystemShielded logical key by existing rawdb accessors. Measure
block-root height coverage, gaps, distinct roots and contiguous equal-root runs.
Do not confuse a root-tree record with the per-block root index, or assume that
a repeated value means a consensus record can be discarded.

Construct baseline and candidate SSTs using the full matching input, equal
block/restart/filter/rotation settings and both Snappy and no compression. Add
required prefix metadata to compact Storage totals. This separates ordinary
prefix encoding and compression from actual encoding gains. It is not a
measurement of the running mixed LSM or the full history/cold storage stack.

## Shielded: record root change points

The existing block finalizer calls SaveCurrentAsBest after every block. Even
when the tree is unchanged, it writes a fresh block-number-to-root row. Keep
the existing LAST_TREE/root-to-tree/nullifier/note/proof semantics and replace
only this dense index with change points in a dedicated logical namespace.

At the first indexed height, persist (height, root). Thereafter persist a new
boundary only when the root differs. Read a historical height using the greatest
boundary at or before the query. Bound the query by the first indexed height
and the canonical execution cut; a query before the first indexed block or
after the cut must remain absent. If imported/noncanonical holes are supported,
represent gaps explicitly; predecessor lookup alone cannot invent coverage.

An explicit start/end/root run is useful for the offline size experiment. It
is not the preferred mutable protocol: extending its end every block would
continue producing writes and history. Change points plus an already required
canonical-cut bound avoid that per-block endpoint update.

All readers of the old direct key must move to the accessor, including the
previous-block-root fast path, raw DB adapter, StateDB/overlay readers, compare
tools, and any snapshot/history readers. A predecessor seek must honor pending
block-buffer layers, tombstones and as-of views; a raw Pebble predecessor that
bypasses them would be incorrect. Avoid a scan from height one for every lookup.

Root-keyed trees remain available for spend anchors. Dense historical lookup
behavior must remain unchanged even though the local index is sparse. Reorg
undoes the relevant boundary writes and canonical upper bound together; restore
must retain the boundary before the cut and all subsequent boundaries up to it.
Do not filter existing MuHash source rows at snapshot time as a shortcut.

## ContractStorage: retain Java's key identity, reduce physical redundancy

The current Java-compatible row key is prefix16 || tail16. The prefix derives
from owner and nonzero contract transaction hash; version 1 hashes the VM slot
before taking its tail. The outer latest key also carries owner and generation.
Keeping only tail16 in a dedicated owner20/generation8 table is reversible when
one prefix applies to an incarnation; retain/derive that prefix once and verify
the invariant. Never replace this identity with the original 32-byte VM slot:
that would change Java's lookup and collision semantics.

Store nonzero 256-bit values in minimal big-endian form, reconstructing the
32-byte value before TVM use. An empty SST value can represent zero-but-present;
absence is the lookup presence bit, not an empty value. Removing the wrapper
requires every cache, before-image, error path and history accessor to preserve
presence independently. Do not silently turn malformed or oversized encodings
into zero. Keep all-ones and wide values exact.

The diagnostic measures compact keys with the existing wrapped value separately
from compact keys with trimmed values, so key and value effects can be assessed.
Common key bytes are already prefix-encoded inside SST blocks, so a large
logical reduction from key shortening may yield a small physical reduction.

A shared value dictionary or special-value tags are further candidates only
when exact frequencies justify their complexity. They add lookups/cache state,
allocation/versioning and history/checkpoint requirements. Do not count repeated
zero-padding twice as both trimming and dictionary savings.

## Fresh-state commitment and execution boundary

Choose a new explicit schema/element-codec identifier for the local physical
representation. New source-family enumeration, MuHash insertion/removal and
snapshot row counts must refer to exactly that representation. Source rows,
complete successor HEAD, internal root, block identity and required execution
checkpoint must publish through the normal canonical commit protocol.

No old-format migration or compatibility mode is required by the user. A future
deployment may select a fresh verified mainnet data target and sync genesis,
but this does not authorize clearing any existing directory during analysis.
Keep Java/Nile, keys, parameters and configuration outside that operation.

Old-format data and snapshots must fail explicitly under the new scheme rather
than being partially interpreted. New snapshots must reproduce source counts,
MuHash normalized state/digest and executable canonical metadata, then continue
normal sync across an index boundary and a Storage write.

## Acceptance before any runtime implementation is released

- Exhaustive dense-versus-sparse root lookup equality over the measured cut,
  including first/last, absent/future heights, gaps and repeated A/B/A roots.
- Overlay/as-of/reorg correctness across root boundaries and maintenance;
  crash/restart consistency at uncertain flush outcomes.
- Storage codec round trips for zero presence, 1, all-ones, random wide values,
  malformed values, all leading-zero lengths, and versioned slot layouts.
- MuHash full-source oracles after normal commits, rollback and restore;
  snapshot import followed by subsequent execution.
- Controlled total physical size and CPU/I/O/read-latency measurements, followed
  by fresh genesis replay. Do not claim an end-to-end sync speedup from SST sizes.

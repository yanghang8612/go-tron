# Serial account demand reads and frozen query reuse

Execution remains serial. This change follows the fixed 2026-10-31 mainnet
profile at source `52aea62ef56799b7e6c1365293123dd029286935`: account hydration,
frozen bandwidth queries and pending-prefix materialization dominate relevant
state CPU. The live comparison window was 2,698 blocks / 1,193,525 transactions
in 300 seconds (8.993 blocks/s); this is context, not a replay fixture.

Delegate validation reads receiver type at the same validation position through
the compact typed getter. Delegation usage helpers read only their consumed
resource fields. Transfer/Available require COMPLETE V1 bandwidth rows, the
requested V2 point and the resource row, preserving relevant V1 -> V2 ->
resource error precedence. Fold/Merge need the resource row and scalar usage
fields. Arithmetic, mutation methods, journal and commit remain unchanged.
Narrowing intentionally avoids unrelated corrupt auxiliary domains; neither
physical IO nor the old unrelated-domain read/error set is claimed identical.
The requested V2 amount is validated before the resource row and then obtained
again by the caller from the existing typed point cache (a duplicate logical
query, without another durable read).

The optional frozen V1 point cache covers only physical indexes 0 and 1,
including absent results. It owns scalar values plus immutable unknown-field
bytes and reconstructs fresh protobufs, independent of mutable Account.Frozen.
It never asserts prefix completeness. Existing invalidation clears both the
point cache and materialization; installing equivalent materialization leaves
the independent point results intact. Copies own separate cache containers.
Generation is checked, errors are not cached and active transaction-versioned
readers bypass point caching. Reset, generic Set/Delete, journal/revert,
recreation and object reuse retain invalidation boundaries.

Materialization provenance is explicit: canonical-only, compatibility-fast or
complete. A canonical missing index 0 does not prove an empty prefix. Fast-row
fallback comes directly from the actual index-1 probe, never another cache's
bits. A later full account/projection upgrades canonical/fast materialization
through a complete prefix scan. Complete cached reads replay the unsupported
range marker; point/materializer hits replay existence, generation and exact
point dependencies. This deliberately repairs prior conservative-dependency
and incomplete-provenance gaps, including canonical-miss -> sparse full reads.
The general 0/1 compatibility guard remains; no new assumption about index>=2
is introduced into the existing fast point-query semantics.

Pending-prefix iteration still eagerly reads all durable rows before any user
callback, merges overlay puts/tombstones, sorts keys and preserves stop/errors.
The one-shot merge already owns each independent value; callback ownership is
transferred instead of cloning every value a second time. No durable result
cache, schema, root format, consensus rule or memory-budget change is added.
Current account and subrow prefetch plans already deduplicate physical keys;
there is no demonstrated remaining duplicate plan requiring a hint/fanout
change. Missing hints remain useful negative cache entries and are retained.

Correctness has two contracts. Narrowing preserves valid-state outputs,
arithmetic, durable account/history bytes and commitment roots versus exact
upstream while requiring consumed dependencies and relevant error ordering.
Point caching/prefix ownership preserve values, errors and required readsets;
explicit conservative provenance repairs are enumerated above. Exact legacy
usage helpers are retained mechanically renamed in a test-only oracle.

Adversarial tests cover unknown fields, mutable protobuf aliases, misses and
insertion, generic writes/deletion, copy isolation, snapshot/revert, reset and
recreation, IO/decode errors, sparse indexes 0/1/7, canonical/complete upgrades,
public materializer dependency replay and transaction-versioned values,
tombstones and local-overlay precedence. Timed fixtures include first use and
account churn, writes, repeated reads, mutation, snapshot/revert, copy costs,
commit/reopen/reset/recreate and database close; delegation additionally compares
all durable bytes including temporal history and exercises both TVM float orders
and MergeUsageToInheritor. Fixtures are deterministic synthetic state, not a
mainnet replay or an SSD-throughput claim.

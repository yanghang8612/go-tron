# Business state V4 and independent event archive frontier

## Scope and authorization

Continue the three measured optimization paths from the authenticated 2026-11-02
chaindata census, on the pinned 0096c0af production source. Development and fixed
copy validation only. Do not deploy, restart production, reset/delete a database,
or mutate the authenticated source copy. User permits a future fresh genesis.
Java protobuf/wire/block hashes/consensus arithmetic remain unchanged.

## State representation and commitment

Use scheme `state-flat-latest-muhash3072-compact-v4`, HEAD/element codec version 4,
and a distinct element/delta domain. Snapshot manifest version 5 requires that
scheme and element version. Keep the existing HEAD physical key: an old HEAD must
be found and rejected, never silently initialized. Genesis/startup/import must
reject old versions before any source mutation. No automatic conversion or reset.
MuHash continues to fold the complete actual physical source key/value set,
including presence and retained generations. Commit, undo, full-source rebuild,
export and restore must use the identical representation. Do not filter unwanted
source rows during export to obtain a smaller root.

## Permissions: owner-aware native encoding

Keep every original permission physical key and its presence. Use a versioned
permission-row codec with exact Owner/Witness/Active template modes. Omit SELF
addresses only with the enclosing owner supplied at decode. Active permissions
store exact operation bytes or an immutable exact-match constant ID; never derive
old operations from the current governance state. Custom shapes, unknown fields,
duplicate keys/order, non-SELF addresses and all numeric bit patterns survive.
Unprofitable/custom rows may retain their old native representation inside a V4
store; this is not permission to open a V3 database. Decode templates directly to
Permission objects rather than expand/reparse a complete old native value.
Latest point reads, account hydration and historical hydration pass the same owner.
No absent/default conflation and no owner bundling or Bloom membership reduction.

## Reward snapshots: Votes and explicit presence

Retain all existing cycle-account-vote logical/physical keys. Native Account
containers hold only the Votes field, preserving vote order, addresses, counts and
Vote-level unknown bytes. Present-empty remains a real row, distinct from absent.
Expose typed `(votes, present, error)` reads and typed writes. Production reward
settlement/query and TVM must not test protobuf byte-slice nilness for existence or
hydrate a full current Account to write a vote snapshot. Reject complete old
Account values rather than silently discarding their non-Votes fields.
Diagnostics explicitly request/label a Votes projection. Full old Account values,
allowance diagnostics and old MuHash are not preserved by this new model.
History stores normal opaque V4 before-images through existing journal/commit
paths. No cycle-key GC or single-slot conversion in this implementation. Old/new
reward arithmetic, within-transaction snapshot/revert, historical reward reads,
unwind/replay and restore followed by execution must agree on Votes/presence.

## Independent event frontier

Only direct Ancient V2 with external receipt logs and the explicit next-segment
request callback enables this path. Balance/section sidecars retain their old
SnapshotBuild bound. Event target is bounded by the next complete ancient segment,
latest solid block and hash-verified Finish, independently of SnapshotBuild and its
HistoryWindow. A missing/unhashed/mismatched Finish is not replaced with head.
Pin the target canonical hash before reading and verify it plus solid/Finish again
before publication. Missing bodies/receipts or a reorg cannot publish coverage.
Keep existing gate, budget, adaptive shrinking, retry backoff and freezer handoff.
Existing manifest block-numbered coverage needs no broad validation relaxation.
An event-only initial manifest does not imply executable latest-state coverage.
Manifest-first/stage-second recovery verifies canonical coverage. Snapshot install
caps event stage at the authenticated restored HEAD; ahead files cannot become
trusted executed coverage until their canonical identities can be verified.
Never delete hot rows before ancient publication and required event coverage.

## Acceptance and limits

- Schema incompatibility rejected without source writes/deletes/reset.
- Permission exact native/protobuf round trips and point/history/revert semantics.
- Reward Votes/presence arithmetic against the existing Java-derived reference,
  absent/empty, current-cycle, previous-cycle, unknown fields, malformed data.
- Small coherent full-source MuHash state/digest after commit, unwind/replay,
  full and incremental snapshot restore followed by further mutation/execution.
- Event source/identity gaps, resource deferral, manifest/stage failure recovery,
  latest-state restore behind event coverage and canonical mismatch.
- Fixed-cut matched physical files and production-policy point reads; complete
  HTTP and cold publication checks only on a complete disposable copy. Avoid
  repeated whole-production MuHash recomputation; do not call microbenchmarks HTTP.
- Preserve frozen research sources and raw evidence; publish source transfer only
  on the research implementation branch, never master during this phase.

# Cold history point reads: implementation and offline acceptance

Scope and invariants: [specification](../specs/2026-10-20-cold-history-query.md).

- [x] Reproduce and repair first-seek posting-miss/unindexed-tail omission.
- [x] Restore metadata using the target account envelope generation; exercise
      changes, deletion/recreation, storage key layouts and independent MuHash.
- [x] Bound hot work by the inclusive cold first-match tx/block and preserve
      same-transaction sequence ordering and overlap semantics.
- [x] Skip the canonical genesis cold negative search, retaining explicit hot
      ranges; propagate cancellation through request point-read IO.
- [x] Compare bounded header memoization variants on fixed real cold files.
      Retain current-byte-verified BLAKE2b prefix cache; discard larger small-read
      caches and accessor paging because the measured benefit was insufficient.
- [x] Verify 162 identical cold results and 15 real historical balances; rebuild
      full 227222 checkpoint MuHash; verify local/server Sapling builds, restore
      and continued execution, related full packages/race/vet, source hashes.
- [x] Preserve raw results, profiles, transport seal, source and corpus identities
      and first-fill/positive-query performance limits.

No push, production restart, deployment, format reset or database mutation.
Authenticated accessor-first filtering and mixed-key directory/tail batching
remain separate follow-up work; no stat-only negative shortcut was introduced.

Acceptance and exact commands/results:
[offline report](../../dev/cold-history-query-optimization-20261020.md).

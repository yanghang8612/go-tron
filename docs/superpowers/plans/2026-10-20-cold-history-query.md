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

## Follow-up: log identity and pinned query tail reuse

- [x] Reproduce the public 20-byte address filter miss on hot, legacy cold and
      V4 cold paths; fix payload predicates and both segment/global lookups.
- [x] Differential-test the borrowed owner scan against the owning logical
      oracle, including shared packs, standalone zero rows and owner-changing
      positive-sequence repairs.
- [x] Bound pinned query reuse by bytes/rows/owners; cover transaction cuts,
      recreation, subset ranges, input/output ownership, concurrency,
      cancellation, corruption and cross-snapshot negative isolation.
- [x] Preserve immediate early hits; on budget exhaustion continue the same
      scan rather than restarting it. Benchmark both negative and early hits.
- [x] Real immutable event-file baseline/candidate comparison and canonical
      balances, full source-fold MuHash, Sapling restore and continued execution.
- [x] Diagnose index wake cadence (4096-block session barrier, minimum 256,
      maximum 4096 per pass) from same-process samples. Keep scheduling changes
      out of this reader patch pending a throughput/write-amplification study.

This follow-up remains local/offline. Deployment authorization for 9030ce7a
already completed does not cause another automatic push or production restart.

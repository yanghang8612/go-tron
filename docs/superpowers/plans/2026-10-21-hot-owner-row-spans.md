# Hot owner row spans plan

- [x] Add bounded, source-bound owner/row-span sidecar codec and strict builder.
- [x] Integrate pinned selected-owner reads without changing canonical decoders.
- [x] Publish in atomic block writes and cover history routing/prune lifecycle.
- [x] Test oracle equivalence, failures, budgets, repair/MVCC/restore semantics.
- [x] Freeze source; complete local and native checks.
- [x] Build indexes on a derived copy; prove unchanged canonical data and watermarks.
- [x] Compare full seven-height HTTP cold/warm and actual index/storage overhead.
- [x] Recompute MuHash, restore windows, diagnose residual work, transfer evidence.

Do not push, deploy or restart production.

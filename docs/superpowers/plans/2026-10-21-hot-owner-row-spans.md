# Hot owner row spans plan

- [ ] Add bounded, source-bound owner/row-span sidecar codec and strict builder.
- [ ] Integrate pinned selected-owner reads without changing canonical decoders.
- [ ] Publish in atomic block writes and cover history routing/prune lifecycle.
- [ ] Test oracle equivalence, failures, budgets, repair/MVCC/restore semantics.
- [ ] Freeze source; complete local and native checks.
- [ ] Build indexes on a derived copy; prove unchanged canonical data and watermarks.
- [ ] Compare full seven-height HTTP cold/warm and actual index/storage overhead.
- [ ] Recompute MuHash, restore windows, diagnose residual work, transfer evidence.

Do not push, deploy or restart production.

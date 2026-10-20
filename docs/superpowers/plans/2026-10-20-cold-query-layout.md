# Cold query layout implementation

- [x] Read prior history query, fresh compact-format and compaction specifications.
- [x] Implement separate retained prefix and body boundaries in stream writers.
- [x] Verify prefix/body CRC, logical offsets, reset/backpatch and migration
  idempotence, then run state/snapshot/MuHash/restore regressions.
- [ ] Build prefix-only derived fixed corpus preserving compressed body frames.
- [ ] Compare identical HTTP host/binary on original and prefix-only copies.
- [ ] Canonically merge bounded groups on another derived corpus; verify logical
  rows, oracle, MuHash and restore, then compare complete HTTP calls.
- [ ] Record query benefit and construction/sync costs before selecting a runtime
  compaction or global routing strategy. Do not deploy or restart production.

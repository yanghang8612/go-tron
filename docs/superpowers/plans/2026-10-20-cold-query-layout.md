# Cold query layout implementation

- [x] Read prior history query, fresh compact-format and compaction specifications.
- [x] Implement separate retained prefix and body boundaries in stream writers.
- [x] Verify prefix/body CRC, logical offsets, reset/backpatch and migration
  idempotence, then run state/snapshot/MuHash/restore regressions.
- [x] Build prefix-only derived fixed corpus preserving compressed body frames.
- [x] Compare identical HTTP host/binary on original and prefix-only copies.
- [x] Measure bounded canonical merge construction costs on another derived
  corpus. Stop after 33 committed passes; no full coarsened query claim.
- [x] Implement exact bounded routing for measured preflight namespaces, with
  sparse/dense membership, pair identity binding and ordinary-reader fallback.
- [ ] Validate snapshot semantic checking, maintenance admission, corruption,
  replacements and cross-cut queries; run race/native state and MuHash checks.
- [ ] Build routing on the prefix-only fixed copy and compare full HTTP calls.
- [ ] Record query benefit and construction/sync costs before selecting a runtime
  compaction or global routing strategy. Do not deploy or restart production.

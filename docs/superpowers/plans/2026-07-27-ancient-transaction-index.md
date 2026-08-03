# Ancient transaction index — implementation plan

**Spec:** [2026-07-27-ancient-transaction-index-design.md](../specs/2026-07-27-ancient-transaction-index-design.md)

- [x] Establish a separate implementation worktree and branch.
- [x] Add a bounded, deterministic real-database transaction-index benchmark.
- [x] Compare exact hashes, routed 64-bit fingerprints, and routed 96-bit
  fingerprints at selectable directory widths.
- [x] Run the benchmark against the deployed mainnet database and select the
  production directory/fingerprint parameters.
- [x] Implement the checksummed immutable run writer/reader and manifest.
- [x] Add hot-Pebble-first, cold-index-second lookup with full-body verification.
- [x] Add an offline resumable migration and safe historical `tx-*` deletion.
- [x] Add geometrically merged incremental runs for newly V2-covered history.
- [ ] Verify historical Wallet/JSON-RPC transactions and receipts before and
  after physical Pebble compaction.

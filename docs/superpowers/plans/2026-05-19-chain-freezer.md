# Chain freezer — plan

**Spec:** [2026-05-19-chain-freezer-design.md](../specs/2026-05-19-chain-freezer-design.md)

## Slice 1 — Freezer mechanism + read fall-through

- [ ] Vendor / port geth's freezer-table machinery into `core/rawdb/freezer/`:
  - [ ] `freezer.go` — top-level: open/close, per-kind tables, sync, lock
  - [ ] `freezer_table.go` — single .cdat/.cidx pair, append + read at index
  - [ ] `freezer_batch.go` — append-many in one fsync window
  - [ ] `freezer_meta.go` — meta.json + crash-recovery on open
  - [ ] `freezer_utils.go` — Snappy compression helpers
- [ ] Tests ported from geth, adapted to gtron's deps (`make test ./core/rawdb/freezer/`)
- [ ] Tables: `headers`, `bodies`, `tx_infos`, `state_roots`. (No
      transactions index in slice 1.)
- [ ] `AncientReader` / `AncientWriter` interfaces in
      `core/rawdb/accessors_ancient.go`
- [ ] `ChainDB` wrapper composing Pebble + freezer; constructor
      `NewChainDB(kv, freezer)`

## Slice 2 — Migrate accessors_chain.go callers

- [ ] Audit every accessor: `ReadBlock`, `ReadHeader`, `ReadBlockHeader`,
      `ReadBody`, `ReadTransactionInfosByBlock`, `ReadBlockStateRoot`,
      `ReadCanonicalHash`. List in audit doc.
- [ ] Add `ChainDB` parameter (replacing `ethdb.KeyValueReader`) to each;
      route reads to ancient when `num < cutoff`
- [ ] Adapter for tests using `ethrawdb.NewMemoryDatabase()`: a `ChainDB`
      that wraps just memdb with a "no-op" ancient (always returns cutoff=0
      so all reads go to memdb)
- [ ] Migrate every caller: `core/blockchain.go`, `core/state_processor.go`,
      `core/producer/*`, `internal/jsonrpc/*`, `internal/grpcapi/*`,
      `net/handler.go`, etc.
- [ ] No test regressions; one new unit test per accessor that exercises
      ancient + KV fall-through

## Slice 3 — Background freezing goroutine

- [ ] `core/freezer/runner.go` — runs as a `node.Lifecycle` service
- [ ] Per-pass logic:
  - Read solidified, compute `freezeTo = solidified - margin`
  - Snapshot reads from Pebble (`bc.db.Snapshot()`)
  - `freezer.AppendRange(freezeFrom..freezeTo)` writes ancient
  - `bc.db.DeleteRange(prefix..)` deletes Pebble rows
  - `bc.db.Compact(...)` to reclaim space
- [ ] Config: `[freezer]` TOML + CLI flags
- [ ] Metrics: `frozen_min`, `frozen_max`, `pass_duration_seconds`,
      `pebble_size_after_pass_bytes`
- [ ] Crash-safety test: kill -9 mid-pass, restart, assert no data loss
      and freezer resumes
- [ ] Long-running Nile soak: 24h, assert linear ancient growth and
      bounded Pebble size

## Slice 4 — RPC + observability

- [ ] `gtron_freezerStatus` JSON-RPC endpoint
- [ ] CLI `gtron freezer status` (reads from on-disk meta directly,
      doesn't need a running node)
- [ ] CLI `gtron freezer truncate --to=<num>` (disaster recovery only,
      requires `--force` confirmation)
- [ ] Document operator workflow in `docs/dev/freezer-operator.md`:
      first-launch catch-up, monitoring, when to disable, how to recover

## Slice 5 — Test sweep + rollout

- [ ] Compatibility test: pre-freezer datadir snapshot, launch with
      freezer enabled, verify all reads still work during catch-up
- [ ] Reorg test: extreme case where margin needs adjustment
      (we never freeze past the rewind point — fuzz test confirms)
- [ ] Performance test: p99 read latency on frozen block, with and
      without OS page cache, on tmpfs / nvme / sata
- [ ] Acceptance: 24h Nile archive node soak shows
      `datadir(today) ≥ 1.7 × datadir(after-freeze)`

## Acceptance criteria (overall)

- [ ] Pebble datadir reduces ≥ 40% on Nile after catch-up
- [ ] Pebble compaction CPU < 5% steady-state on Nile
- [ ] No regression in any existing accessors test
- [ ] Crash chaos test (kill -9 mid-pass × 10) leaves DB consistent
- [ ] Frozen block read p99 < 20ms on commodity SSD
- [ ] Long-running ops doc + truncation tool both shipped

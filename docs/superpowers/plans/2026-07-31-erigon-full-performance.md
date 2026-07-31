# Erigon Full-Performance Alignment Plan

**Spec:** [2026-07-31-erigon-full-performance.md](../specs/2026-07-31-erigon-full-performance.md)

## P1: Shared read-cache substrate

- [x] Keep latest-account, latest-KV, code, and commitment reads on the bounded
  blockbuffer base cache below rewindable overlays.
- [x] Reuse confirmed missing-key classification instead of issuing a second
  Pebble `Has`.
- [x] Let `Buffer.Has` consume authoritative positive/negative base-cache rows.
- [x] Preserve snapshot-version, flush-refresh, discard, and same-key
  invalidation semantics for concurrent fills.
- [x] Remove the rejected per-block compatibility-prefetch rollout and its
  configuration surface from the latest-only fresh-sync path.
- [ ] Collect before/after mainnet CPU, Pebble, and sync-throughput samples.

## P2: Session-scoped read ahead

- [x] Move worker ownership to `canonicalRangeExecutor` for
  sync sessions.
- [x] Enqueue future-block hints immediately after staged bodies decode.
- [x] Extract schema-owned account, contract-metadata, and code keys without
  mutating or validating canonical state.
- [x] Route direct admissions into the same bounded cache canonical reads use,
  retaining overlay precedence and same-key late-fill rejection.
- [x] Replace lifetime-wide deduplication with per-block deduplication plus a
  bounded block/byte queue.
- [x] Tie reset, abort, and close to a session epoch and cover queued/in-flight
  old-fork rejection plus late-fill/flush races with race-clean tests.
- [x] Add queue bytes, drops, present/missing rows, read errors, stale blocks,
  and useful canonical-prefetch-hit metrics.
- [ ] Validate roots/results and collect fixed-window before/after production
  throughput, CPU, Pebble reads, and useful-prefetch ratios.

## P3: Ordered parallel preprocessing

- [ ] Profile protobuf decode, signature, envelope, receipt/log, and derived
  index costs on a transaction-dense fixed replay.
- [ ] Parallelize independent static validation with serial authoritative
  consumption.
- [ ] Move deterministic key extraction and eligible cold reads ahead of the
  serial executor.
- [ ] Parallelize derived receipt/log/trace encoding behind recoverable stages.

## P4: Conflict-aware execution

- [ ] Define the physical/logical transaction read/write-set schema.
- [ ] Add serial shadow capture and deterministic journal comparison.
- [ ] Implement a narrow disjoint-transfer speculative executor.
- [ ] Replay conflicts serially and publish only in original transaction order.
- [ ] Expand eligibility by actuator family using java-tron fixtures and fixed
  mainnet replay windows.

## P5: Snapshot and cold steady state

- [ ] Productionize signed snapshot catalog hosting and resumable bootstrap.
- [ ] Verify recent-tail execution and restart after restore.
- [ ] Tune freezer/history build-merge-prune throughput above sustained import.
- [ ] Run the 30-minute resource gate and 24-hour mainnet soak gate.

# Erigon Full-Performance Alignment Plan

**Spec:** [2026-07-31-erigon-full-performance.md](../specs/2026-07-31-erigon-full-performance.md)

## P1: Shared read-cache hot path

- [x] Preserve blockbuffer generic and typed no-copy cache capabilities through
  `vmKVStore`.
- [x] Route prefetch reads into the same cache used by canonical state reads.
- [x] Reuse confirmed missing-key classification instead of issuing a second
  Pebble `Has`.
- [x] Let `Buffer.Has` consume authoritative positive/negative base-cache rows.
- [x] Remove defensive copies from prefetch-only latest-state reads.
- [x] Add focused forwarding, negative-cache, and error-semantics tests.
- [x] Reject the current default-on candidate after its hot-state benchmark
  exceeded the light/hot overhead gates; keep all network defaults off.
- [ ] Collect before/after mainnet CPU, Pebble, and sync-throughput samples.

## P2: Session-scoped read ahead

- [ ] Move worker ownership from `processBlock` to `canonicalRangeExecutor` for
  sync sessions.
- [ ] Enqueue future-block hints from decoded staged bodies.
- [ ] Replace lifetime-wide deduplication with a bounded/version-aware policy.
- [ ] Add cache invalidation and reorg/restart concurrency tests.
- [ ] Add queue-byte, useful-hit, stale/reload, and wait-avoided metrics.

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

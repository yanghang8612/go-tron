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
- [x] Expand the bounded signature pool into a shared immutable-block
  preprocessing pool without adding workers or configuration.
- [x] Precompute transaction Merkle roots, typed contract decode, protobuf wire
  sizes, txid/signers, and witness recovery for sync batches.
- [x] Keep mutable standalone transaction sizing uncached and retain serial
  authoritative validation/error ordering.
- [x] Replace ordered owner reflection and read-ahead owner extraction with the
  shared typed-contract memo.
- [ ] Parallelize independent static validation with serial authoritative
  consumption beyond the implemented immutable facts.
- [x] Move deterministic latest-state key extraction and eligible reads ahead
  of the serial executor.
- [ ] Parallelize derived receipt/log/trace encoding behind recoverable stages.

## P4: Conflict-aware execution

- [x] Define the logical transaction write-cell schema over the authoritative
  StateDB journal, with conservative unknown-entry handling.
- [x] Add an observe-only Transfer wave planner using actual serial writes,
  dynamic-property barriers, and address dependency detection.
- [x] Capture generic transaction read cells across StateDB account, storage,
  code, metadata, witness, account-KV, and per-key DynamicProperties paths.
- [x] Validate captured reads against a block-local version map and report
  first-pass validity/conflict families by VM, Transfer, and other contracts.
- [x] Label only audited commutative fee/cumulative settlement operations and
  measure raw versus ordered-delta-normalized first-pass validity without
  changing canonical execution.
- [x] Split hot TRON Account access into Erigon-style typed field paths with
  full-account hierarchy barriers, exact cached-row dependencies, and an
  observe-only typed first-pass validator.
- [x] Model Erigon's explicit previous-sender execution dependency on top of
  typed versions and measure the remaining cross-sender retry set and sender
  critical-chain depth before starting workers.
- [x] Build an observe-only dependency-ready DAG from actual typed read/write
  versions, with unknown/range reads as serial barriers, and report attainable
  wave width before allocating speculative workers.
- [ ] Extract normalized settlement writes into explicit per-transaction deltas
  and apply them at ordered publication after serial-equivalence fixtures pass.
- [ ] Execute eligible waves in discard-only shadow workers and compare full
  journals plus TransactionInfo with the serial reference.
- [ ] Implement a narrow disjoint-transfer speculative executor.
- [ ] Replay conflicts serially and publish only in original transaction order.
- [ ] Expand eligibility by actuator family using java-tron fixtures and fixed
  mainnet replay windows.

## P5: Snapshot and cold steady state

- [ ] Productionize signed snapshot catalog hosting and resumable bootstrap.
- [ ] Verify recent-tail execution and restart after restore.
- [ ] Tune freezer/history build-merge-prune throughput above sustained import.
- [ ] Run the 30-minute resource gate and 24-hour mainnet soak gate.

# State history index — plan

**Spec:** [2026-05-19-state-history-index-design.md](../specs/2026-05-19-state-history-index-design.md)

## Slice 1 — Schema + read API shape

- [ ] Audit every `state.StateDB` setter (`SetBalance`, `SubBalance`,
      `AddTRC10Balance`, `SetResource*`, `SetCode`, `VoteWitness`, etc.)
      to enumerate the full per-account mutable field set
- [ ] Define `AccountDelta` proto in `proto/core/historystate.proto` with
      every field surfaced in the audit
- [ ] Add `sh-m-`, `sh-a-`, `sh-s-`, `sh-i-a-`, `sh-i-s-`, `sh-cfg-`
      prefixes to [core/rawdb/schema.go](../../../core/rawdb/schema.go)
- [ ] Add rawdb accessors in `core/rawdb/accessors_history_state.go`:
      `WriteAccountDelta`, `ReadAccountDelta`, `WriteSlotDelta`,
      `ReadSlotDelta`, `WriteHistoryMeta`, `ReadHistoryMeta`,
      `IterateAddrInverse`, `IterateSlotInverse`
- [ ] Tests for accessors: round-trip, range scan over inverse index,
      delete-by-prefix
- [ ] Stub `state.HistoryReader` interface + a no-op implementation that
      returns live state for any block (so the API compiles before slice 2)

## Slice 2 — Capture deltas during applyBlock

- [ ] Add `stateObject.preBlock` snapshot field; populate on first dirty
      mutation per block
- [ ] `state.StateDB.AccumulateHistory(buffer)` — called by
      `core/blockchain.go::applyBlock` between `ProcessBlock` success
      and `bc.buffer.CommitBlock()`
- [ ] TVM storage path: in `state.StateDB.SetState`, record `preValue`
      into a per-block storage-history accumulator
- [ ] Write inverse-index rows (`sh-i-a-`, `sh-i-s-`) alongside the
      forward-delta rows
- [ ] All writes through `bc.buffer` (verify with a fork-rewind test)
- [ ] Gate behind `config.HistoryEnabled` so non-archive operators pay
      zero overhead

## Slice 3 — Read API

- [ ] Implement `HistoryReader.AccountAt(addr, N)`:
      seek inverse index → walk forward deltas from HEAD to first
      modification ≤ N → reconstruct
- [ ] `HistoryReader.StorageAt(addr, slot, N)` — same pattern
- [ ] `HistoryReader.CodeAt(addr, N)` — code is immutable per (addr,
      codeHash) so this is just `AccountAt(addr, N).CodeHash` + `ReadCode`
- [ ] Per-request cache: `historyReader` caches reconstructed accounts /
      slots within a single RPC call so multi-key reads at the same block
      number share work
- [ ] Tests: synth a chain of N blocks where each block tweaks a known
      account, query at each block, assert byte-exact values

## Slice 4 — Fork rewind integration

- [ ] Add a fork-rewind test: insert blocks B0..B5, then trigger a
      switchFork that reorgs them out, verify no `sh-*` rows remain for
      B0..B5
- [ ] Stress test: reorg depth = 27 (one DPoS round), verify history
      reconstruction at each post-rewind block matches reapplied state
- [ ] Concurrent read during rewind: spawn a goroutine doing `AccountAt`
      while reorg runs, ensure no race (`bc.buffer` already handles this
      via its RW lock)

## Slice 5 — Pruning + config

- [ ] `--gcmode=full|archive` flag in `cmd/gtron/main.go`
- [ ] `[history]` TOML section: `mode`, `prune_window`
- [ ] Background pruner goroutine in `core/blockchain.go`:
      `prune_window` blocks old, range-delete `sh-m-` / `sh-a-` /
      `sh-s-` blocks below cutoff
- [ ] Inverse-index pruner: scan `sh-i-a-` / `sh-i-s-` rows whose
      embedded blockNum < cutoff; delete in batches
- [ ] Metrics: history size, prune progress, p99 query latency
- [ ] Acceptance: 24h soak in `full` mode shows no unbounded growth;
      24h soak in `archive` mode shows linear growth

## Slice 6 — Operator recovery

- [ ] Detect "history-config absent but archive mode requested" at
      startup; refuse to launch with a clear message
- [ ] CLI tool `cmd/gtron-history-backfill`: walks blocks N..M, replays
      via the existing `ProcessBlock` path with an instrumented StateDB
      that emits history deltas. Single-threaded, idempotent.
- [ ] Doc: `docs/dev/state-history-recovery.md` — three recovery paths
      (resync from genesis, restore from snapshot + replay, use the
      backfill tool)
- [ ] Smoke test: prune to a checkpoint, run backfill, assert byte-for-byte
      identical history rows compared to a from-genesis archive sync

## Slice 7 — RPC + cross-impl parity

- [ ] `eth_getBalance(addr, blockNum)` / `eth_getStorageAt` / `eth_getCode`
      wired into [internal/jsonrpc](../../../internal/jsonrpc) with `latest`
      / `earliest` / `pending` aliases
- [ ] TRON-style: `getAccountAt`, `getResourceAt`, `triggerConstantContractAt`
- [ ] `debug_traceTransaction(txhash)` re-opens StateDB at the parent
      block via the history reader and runs the existing tracer
- [ ] Cross-impl test: pick 100 random Nile blocks, query `getAccountAt`
      on every account that block touched against both gtron-archive and
      java-tron-archive, assert byte-exact match

## Acceptance criteria (overall)

- [ ] Every value the spec lists in `AccountDelta` round-trips exactly
- [ ] `getAccountAt(known_voter, mainnet_block_50M)` matches java-tron
- [ ] `debug_traceTransaction` works on any block in archive mode
- [ ] Fork-rewind drops orphan-branch history entries
- [ ] `--gcmode=full` retains only `prune_window` blocks of history
- [ ] No regression in `make test`; new tests cover slice 1-4

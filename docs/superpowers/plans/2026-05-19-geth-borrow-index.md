# go-ethereum borrowings — index + execution order

**Date:** 2026-05-19
**Source analysis:** session of 2026-05-19, comparing
`/Users/asuka/Projects/ethereum/go-ethereum` (commit 3d1e6aa6c, ~May 2026)
against `/Users/asuka/Projects/asuka/go/go-tron`.

This is the umbrella index for the seven design specs / plans landed
today. Each is independently reviewable + shippable. Suggested order
reflects ROI (impact ÷ effort) and dependency chain.

## What gtron already aligns with (no work needed)

| Concept | geth file | gtron file |
|---|---|---|
| Service lifecycle | `node/node.go` | [node/lifecycle.go](../../../node/lifecycle.go) + [node/node.go](../../../node/node.go) |
| Diff-layer buffer | `triedb/pathdb/buffer.go` | [core/blockbuffer/buffer.go](../../../core/blockbuffer/buffer.go) |
| Block hook surface | reorg/insert events | [core/blockchain.go::AddBlockHook](../../../core/blockchain.go) |
| rawdb prefix schema | `core/rawdb/schema.go` | [core/rawdb/schema.go](../../../core/rawdb/schema.go) |
| Per-phase apply timing | `core/blockchain_insert.go::insertStats` | [core/blockchain.go::ApplyStats](../../../core/blockchain.go) |

## Recommended order

### 1. State history index — archive node enabler

- [Spec](../specs/2026-05-19-state-history-index-design.md)
- [Plan](2026-05-19-state-history-index.md)

**Why first**: unlocks `debug_traceTransaction(oldtx)`,
`eth_getBalance(addr, oldblock)`, cross-impl divergence triage. Today
gtron archive is fictional — has no per-block delta index, can't answer
historical queries. Highest user-facing impact among the seven.

**Dependencies**: none (additive to existing flat-state).

### 2. Chain freezer (ancient store)

- [Spec](../specs/2026-05-19-chain-freezer-design.md)
- [Plan](2026-05-19-chain-freezer.md)

**Why second**: storage pressure compounds — earlier we run the freezer,
less migration pain. Nile already ~80GB; mainnet 300GB+. Cuts datadir
50%, Pebble compaction 5-10×.

**Dependencies**: none. Independent of state history.

### 3. SyncService split

- [Spec](../specs/2026-05-19-sync-split-design.md)
- [Plan](2026-05-19-sync-split.md)

**Why third**: every sync bug we hit lives in a 1323-line file. Cost
compounds with every bug fix. Slice 1-2 only relocates code; slice 4-5
makes future bug fixes localizable.

**Dependencies**: none. Touches `net/sync.go` only.

### 4. State prefetcher

- [Spec](../specs/2026-05-19-state-prefetcher-design.md)
- [Plan](2026-05-19-state-prefetcher.md)

**Why fourth**: ~10% block-import throughput uplift on heavy blocks.
Modest but free — small surface change, gated behind a flag.

**Dependencies**: none.

### 5. TxPool subpool architecture

- [Spec](../specs/2026-05-19-txpool-subpool-design.md)
- [Plan](2026-05-19-txpool-subpool.md)

**Why fifth**: fixes the "governance txs evicted under TVM spam" class
of bugs (Nile h=860k stuck proposal). Lower urgency than archive +
freezer because we don't hit it daily, but the architecture is the right
foundation for any future per-type policy.

**Dependencies**: none.

### 6. VM tracing hooks

- [Spec](../specs/2026-05-19-vm-tracing-hooks-design.md)
- [Plan](2026-05-19-vm-tracing-hooks.md)

**Why sixth**: enables `debug_traceTransaction` / prestate / callTracer.
Real but specialized — devops audience. Best combined with archive (1)
since historical trace needs both.

**Dependencies**: [state-history-index](2026-05-19-state-history-index.md)
for historical trace; works on HEAD-only without it.

### 7. JSON-RPC reflection framework

- [Spec](../specs/2026-05-19-jsonrpc-reflection-design.md)
- [Plan](2026-05-19-jsonrpc-reflection.md)

**Why seventh**: not user-visible. Cleans up the hand-rolled dispatcher;
every future RPC method becomes one function. Highest "cleanup" / lowest
direct-feature value.

**Dependencies**: none (vendoring is independent of every other item).

## Optional / future (not in any spec)

| Pattern | go-ethereum file | gtron status | Notes |
|---|---|---|---|
| Era1 export format | `core/rawdb/eradb/` | absent | Long-term chain-archive distribution; defer until use case appears |
| FilterMaps log index (EIP-7745) | `core/filtermaps/` | absent | gtron's `eth_getLogs` is bespoke; ok for now; revisit when archive logs queries get heavy |
| Prometheus metric registry | `metrics/` | partial | Long-term consolidation; currently ad-hoc in `core/blockchain.go::applyStatsHooks` |
| JS tracer surface | `eth/tracers/js/` | absent | Custom JS tracers (user-supplied scripts); defer indefinitely |

## Notes on adoption strategy

- **Independent slices**: each of the seven can ship without the others.
  Slot them into release cycles as bandwidth allows.
- **Default-off → default-on**: every behavior change ships behind a
  flag, defaulted off for one release cycle (Nile soak), then flipped
  to default-on after observation.
- **Wire compat**: nothing here changes the TRON wire protocol. Every
  proposal preserves byte-for-byte protocol identity with java-tron.
- **Test discipline**: every spec lists explicit cross-impl tests
  against a live java-tron archive node. The `system-test-cross-flows`
  harness should pick these up.

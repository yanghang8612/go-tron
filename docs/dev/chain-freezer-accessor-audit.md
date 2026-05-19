# Chain freezer accessor audit (slice 2)

**Spec:** [2026-05-19-chain-freezer-design.md](../superpowers/specs/2026-05-19-chain-freezer-design.md)
**Plan:** [2026-05-19-chain-freezer.md](../superpowers/plans/2026-05-19-chain-freezer.md)
**Date:** 2026-05-19

## Spec-vs-reality reconciliation

The plan checklist lists `ReadBlock`, `ReadHeader`, `ReadBlockHeader`,
`ReadBody`, `ReadTransactionInfosByBlock`, `ReadBlockStateRoot`,
`ReadCanonicalHash`. **gtron has neither separate header/body accessors
nor a canonical-hash table** — the block proto is monolithic
(`b-<num>` → full `corepb.Block`) and the canonical hash is recovered
from the proto's BlockHeader directly. The audit therefore narrows to
the six chain accessors that actually exist:

| Accessor                              | KV key prefix | Spec "in scope" (slice 1)? | Route to ancient?         |
|---------------------------------------|---------------|----------------------------|---------------------------|
| `ReadBlock(num)`                      | `b-`          | yes (`bodies`)             | yes, num-keyed            |
| `ReadBlockNumber(hash)`               | `bh-`         | no (stays hot)             | no, KV-only               |
| `ReadTransactionInfosByBlock(num)`    | `tib-`        | yes (`tx_infos`)           | yes, num-keyed            |
| `ReadTransactionInfo(txID)`           | `ti-`         | no (stays hot)             | no, KV-only               |
| `ReadTransactionIndex(txHash)`        | `tx-`         | no (stays hot)             | no, KV-only               |
| `ReadBlockStateRoot(hash)`            | `bsr-`        | yes (`state_roots`)        | conditional (hash→num via KV) |

Slice 1's design table (`chain-freezer-design.md`, lines 70-76) explicitly
keeps the small `bh-<hash>` reverse index, the per-tx `ti-<txid>` blob,
and the `tx-<hash>` lookup hot. They get a `*ChainDB` parameter purely
for **uniformity** — every chain accessor takes the same type so callers
don't keep one foot in each camp — but their bodies stay KV-only.

`ReadBlockStateRoot` is the special case: its key is hash-indexed, but
the freezer table is num-indexed (geth-style). The accessor first tries
KV (`bsr-<hash>`); on miss it falls through to ancient via a two-step
lookup `bh-<hash>` → num → `state_roots[num]`. Both halves of that
two-step are zero-allocation on the KV-hit path.

### Why not split `header` and `body` tables?

The geth spec uses two tables because Ethereum stores headers and bodies
separately. gtron's `corepb.Block` is one protobuf with everything
inside, so a single `bodies` table holding the marshalled block is
sufficient. Slice 3 writes one row per block num. This is a deliberate
divergence from the spec's table list and is captured here.

## Migrated accessor signatures

```go
// before
func ReadBlock(db ethdb.KeyValueReader, num uint64) *types.Block
func ReadBlockNumber(db ethdb.KeyValueReader, hash common.Hash) *uint64
func ReadTransactionInfo(db ethdb.KeyValueReader, txID []byte) *corepb.TransactionInfo
func ReadTransactionInfosByBlock(db ethdb.KeyValueReader, blockNum uint64) []*corepb.TransactionInfo
func ReadTransactionIndex(db ethdb.KeyValueReader, txHash []byte) *uint64
func ReadBlockStateRoot(db ethdb.KeyValueReader, blockHash common.Hash) common.Hash

// after
func ReadBlock(db *ChainDB, num uint64) *types.Block
func ReadBlockNumber(db *ChainDB, hash common.Hash) *uint64
func ReadTransactionInfo(db *ChainDB, txID []byte) *corepb.TransactionInfo
func ReadTransactionInfosByBlock(db *ChainDB, blockNum uint64) []*corepb.TransactionInfo
func ReadTransactionIndex(db *ChainDB, txHash []byte) *uint64
func ReadBlockStateRoot(db *ChainDB, blockHash common.Hash) common.Hash
```

`*ChainDB` already embeds `ethdb.KeyValueStore` and `AncientReader`, so
KV-only accessors still call `db.Get(...)` exactly as before. Going
through `*ChainDB` instead of `ethdb.KeyValueReader` means the call
sites only need a single composite handle.

Writers (`WriteBlock`, `WriteBlockStateRoot`, `WriteTransactionInfo*`,
`WriteTransactionIndex`) stay on `ethdb.KeyValueWriter`. Per the spec
the freezing goroutine (slice 3) is the only writer to ancient; all
hot-path writes continue to land in Pebble unchanged.

## Caller migration list

`bc.chaindb *rawdb.ChainDB` is added in `core/blockchain.go` alongside
the existing `bc.db ethdb.KeyValueStore`. Constructed as
`rawdb.NewChainDB(db, rawdb.NoopAncient{})` in slice 2; slice 3 swaps
the ancient reader for a real `*freezer.Freezer`.

| File | Lines | Before | After |
|---|---|---|---|
| `core/blockchain.go` | 182 | `rawdb.ReadBlock(db, 0)` | `rawdb.ReadBlock(chaindb, 0)` (genesis load — `chaindb` constructed locally) |
| `core/blockchain.go` | 234, 238, 255 | `rawdb.ReadBlock(db, …)` / `rawdb.ReadBlockNumber(db, …)` (in `loadStoredHeadBlock` / `recoverHeadToAppliedState`) | helpers take `*ChainDB` |
| `core/blockchain.go` | 284, 289, 296, 882, 973, 975 | `rawdb.ReadBlock(bc.db, …)` / `rawdb.ReadBlockNumber(bc.db, …)` | `rawdb.ReadBlock(bc.chaindb, …)` |
| `core/blockchain.go` | 453, 1022, 1039 | `rawdb.ReadBlockStateRoot(bc.db, …)` | `rawdb.ReadBlockStateRoot(bc.chaindb, …)` |
| `core/block_builder.go` | 35 | `rawdb.ReadBlockStateRoot(bc.db, …)` | `rawdb.ReadBlockStateRoot(bc.chaindb, …)` (helper takes `*ChainDB`) |
| `core/genesis.go` | 54 | `rawdb.ReadBlock(db, 0)` | helper accepts `*ChainDB`; call sites that have raw KV wrap with `NoopAncient` |
| `core/tron_backend.go` | 232, 236, 249, 257, 1143, 1160, 1239 | `rawdb.Read*(b.chain.db, …)` | `rawdb.Read*(b.chain.chaindb, …)` |
| `cmd/balance-trace/main.go` | 60, 72, 94 | `rawdb.Read*(db, …)` (raw Pebble store) | `rawdb.Read*(rawdb.NewChainDB(db, rawdb.NoopAncient{}), …)` |
| `vm/instructions.go` | 476 | `rawdb.ReadBlock(interpreter.tvm.DB, index)` | wraps `tvm.DB` as a transient `*ChainDB` with `NoopAncient`; safe because `opBlockHash`'s 256-block window sits above the 128-block freezer margin |

### Test-side migration

Test code constructs `*ChainDB` via the new helper
`rawdb.NewMemoryChainDB()` (memdb + `NoopAncient{}`) so existing
fixtures stay byte-identical. Affected tests:

| File | Lines |
|---|---|
| `core/rawdb/accessors_test.go` | 12, 21, 24, 38, 41 (TestWriteReadBlock, TestWriteReadBlockByHash) |
| `core/blockchain_test.go` | 134, 675 |
| `core/blockchain_insert_test.go` | 300, 1334 |
| `core/genesis_test.go` | 279 |

`bc.DB()` keeps returning `ethdb.KeyValueStore` for tests that exercise
the KV-only side (writers, dynamic-property reads). Tests that read
blocks/tx-infos/state-roots through `rawdb.Read*` are switched to
`bc.ChainDB()` (a new accessor).

## What slice 3 needs to know

1. **Single `bodies` table.** No `headers` split. Slice 3 marshals the
   whole `corepb.Block` once per block num.

2. **`state_roots` is num-keyed but the public accessor is hash-keyed.**
   Slice 3's freezing pass must resolve `hashFromNum(num)` *before*
   deleting `b-<num>` from Pebble. The two valid orders:
   - Read header + hash first, freeze `state_roots[num]` keyed on that
     hash internally, then delete.
   - Or persist a separate num→hash freezer table. Slice 2 leaves this
     decision to slice 3 because the read-side doesn't care: it does
     `bh-<hash>` → num via KV (the spec keeps `bh-` hot) and then
     `state_roots[num]` via ancient.

3. **`NoopAncient` is the default.** Slice 2 ships with the freezer
   disabled effectively: every accessor falls through to KV because
   `AncientCount → 0`. Behavior is byte-identical to pre-slice-1.
   Slice 3 only needs to inject a `NewFreezerReader(*freezer.Freezer)`
   into `NewChainDB` when the operator turns the freezer on.

4. **`cmd/balance-trace` opens raw Pebble.** When slice 3 starts
   deleting frozen rows from Pebble, this tool will silently miss
   frozen blocks. Either it must open the freezer files alongside, or
   it must explicitly refuse to run on a post-freezer datadir until
   adapted. Tracked as a slice-3 follow-up.

5. **VM `opBlockHash` is intentionally KV-only.** The 256-block lookback
   window in `vm/instructions.go` sits entirely above the 128-block
   freezer margin, so wrapping `tvm.DB` with `NoopAncient` at the
   `ReadBlock` call site is safe forever — the freezer can never hold
   a row this opcode would request. If the margin is ever reduced below
   256, this site needs revisiting.

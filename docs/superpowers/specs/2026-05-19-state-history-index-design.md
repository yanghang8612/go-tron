# State history index for archive queries — design

**Status:** Proposed
**Author:** yanghang8612
**Date:** 2026-05-19
**Inspiration:** [go-ethereum/triedb/pathdb/history.go](../../../../ethereum/go-ethereum/triedb/pathdb/history.go)
**Related plan:** [2026-05-19-state-history-index.md](../plans/2026-05-19-state-history-index.md)

## Background

gtron currently has **no archive-grade historical state query**. The chain DB
stores the current state (via flat-state KV) plus per-block proto bodies, but
nothing that lets us answer "what was account X's balance at block N" without
either:

1. Replaying every block from genesis (impossible at mainnet scale: 70M+ blocks
   × ~3s replay each), or
2. Keeping an independent snapshot per block (storage-impossible).

This is the user-visible gap behind every "archive mode" feature:

- `debug_traceTransaction(txhash)` — needs the state at the tx's parent block
- `eth_call(req, blockNum)` / TRON `triggerConstantContract(_, blockNum)` —
  needs state at any historical block
- Block-level `getAccountAt(addr, N)` / `getStorageAt(addr, key, N)` — same
- Receipt-replay tooling (cross-impl divergence triage) — same

java-tron handles this by simply never pruning: archive mode = full state +
every block's complete contract storage diff is persisted. Storage cost is
high but acceptable for archive operators. gtron has the bones of the same
model (flat-state, no MPT pruning) but no **reverse index from `(block, key)`
→ pre-value** that lets us reconstruct historical state.

go-ethereum (since Feb 2025) ships
[`triedb/pathdb/history.go`](../../../../ethereum/go-ethereum/triedb/pathdb/history.go)
+ `history_index*.go` that solve exactly this: for each block N, record
"which (account, storage) slots changed and what their pre-values were."
Reconstruction at block M < HEAD = current state ⊖ Σ deltas[M+1 .. HEAD].

This spec ports that pattern to gtron's flat-state model, gated behind
`--gcmode={full,archive}`.

## Goals

- archive node serves `getAccountAt(addr, N)` / `getStorageAt(addr, key, N)` /
  `getCodeAt(addr, N)` for any N ≤ HEAD without re-execution
- archive mode + `debug_traceTransactionAt` works on any past tx
- full mode prunes state history beyond the last `N` blocks (default
  `2*params.MaxBlockNumberDiff` ≈ 27*MAINT_SLOTS — covers reorg horizon plus
  a generous wallet-tx grace window)
- reorgs and `switchFork` rewind history entries together with the
  forward state changes (re-uses the `bc.buffer` machinery)
- API parity with java-tron's archive node: same query surface, same
  byte-for-byte values

## Non-goals

- Do NOT introduce an MPT. gtron's flat-state model is the right one for
  TRON; this design is purely additive.
- Do NOT change the block proto / wire format.
- Do NOT support snap-sync from history (separate work — see freezer spec).
- Do NOT serve historical TVM contract storage via re-execution. The history
  must store the pre-image directly so the cost of a historical query is
  O(deltas-between-block-and-head), not O(re-execute-from-block).
- Do NOT block the default `full` build: history-index writes must be skippable
  by config so non-archive operators don't pay the storage tax.

## Mental model

```
HEAD state                       ← lives in Pebble flat-state today
  ⊖ Δ_HEAD                       ← "what HEAD changed vs HEAD-1"
  ⊖ Δ_(HEAD-1)
  ⊖ ...
  ⊖ Δ_(N+1)
  = state at block N
```

A "Δ_N" entry records, for every (account, slot) key written by block N, the
**pre-value** (the value the key had at block N-1). Reads at block N walk
forward Δ entries newest-first, applying the pre-value to undo each delta,
stopping once every key is satisfied.

## Storage layout

New rawdb prefixes (slot in [core/rawdb/schema.go](../../../core/rawdb/schema.go)):

| Prefix | Key | Value | Notes |
|---|---|---|---|
| `sh-m-` | `sh-m-` ‖ big-endian uint64 blockNum | proto-encoded `StateHistoryMeta` | Per-block manifest: how many entries, schema version, prev-block link |
| `sh-a-` | `sh-a-` ‖ big-endian uint64 blockNum ‖ 21-byte addr | proto-encoded `AccountDelta` | Account-level pre-state (balance/nonce/code-hash/asset map) |
| `sh-s-` | `sh-s-` ‖ big-endian uint64 blockNum ‖ 21-byte addr ‖ 32-byte slotkey | 32-byte slot pre-value | TVM contract storage pre-value (one row per (block, addr, slot)) |
| `sh-i-a-` | `sh-i-a-` ‖ 21-byte addr ‖ big-endian uint64 blockNum | empty (key-only) | **Inverse index**: for each addr, sorted-by-block list of "blocks where this addr was modified". Speeds up "find latest change ≤ N" |
| `sh-i-s-` | `sh-i-s-` ‖ 21-byte addr ‖ 32-byte slotkey ‖ big-endian uint64 blockNum | empty | Same inverse index for (addr, slot) |
| `sh-cfg-` | sentinel | proto-encoded `HistoryConfig` | `{firstBlock, lastBlock, mode: archive|full, prune_window}` |

### `StateHistoryMeta` proto sketch

```protobuf
message StateHistoryMeta {
  uint64 block_num   = 1;
  bytes  block_hash  = 2;
  uint32 num_addrs   = 3;   // count of sh-a- rows for this block
  uint32 num_slots   = 4;   // count of sh-s- rows
  uint32 schema_ver  = 5;   // bump on layout change
}
```

### `AccountDelta` proto sketch

Records the **pre-block** value of every field that gtron's `state.Account`
exposes. New fields are appended; old field absence ⇒ "field was zero pre-block".

```protobuf
message AccountDelta {
  bytes  addr            = 1;
  int64  balance_pre     = 2;
  int64  nonce_pre       = 3;   // currently always 0 on TRON, here for future-proofing
  bytes  code_hash_pre   = 4;   // 32-byte, empty if no code
  // TRC10 balances are sparse — only changed assets land here.
  repeated TRC10Delta trc10_pre = 5;
  // Resources: bandwidth, energy
  ResourceDelta bandwidth_pre = 6;
  ResourceDelta energy_pre    = 7;
  // Vote / freeze deltas
  repeated VoteDelta votes_pre = 8;
  // Permission deltas (rare)
  PermissionDelta perm_pre = 9;
  // Sentinel: if absent in this block, account was non-existent pre-block.
  bool  existed_pre = 10;
}
```

Exact field list TBD during slice 1 audit — see [plan](../plans/2026-05-19-state-history-index.md) Slice 1.

## Write path: capture deltas during `applyBlock`

[`core/state/statedb.go`](../../../core/state/statedb.go) currently tracks
dirty objects via `journal` for in-flight rollback. We extend it to:

1. **Per-tx**: every Write to a state object records the **pre-tx** value
   (already done for journal). Persist this into a per-block `historyAccum`
   when the tx commits.
2. **Per-block**: at the end of `applyBlock`, just before
   `bc.buffer.CommitBlock()`, serialize `historyAccum` into `sh-a-` /
   `sh-s-` rows. Writes go through `bc.buffer` so `switchFork` discards
   orphan-branch history on rewind.
3. **Inverse index**: append `sh-i-a-` / `sh-i-s-` rows for the same
   (addr) / (addr, slot) at this blockNum. These are key-only rows
   (zero-value writes); the inverse index lets us answer "latest block
   ≤ N that touched key K" in one Pebble seek.

### Avoiding duplicates / write amplification

Inside one block, a slot may be written multiple times. We only record the
**very first pre-tx value**. Subsequent writes don't add history rows.
Implementation: `historyAccum` is a `map[key]value` where the first write
sticks (`if _, ok := m[k]; !ok { m[k] = preValue }`).

### Cost estimate (per block)

- Typical Nile block: ~50 txs, ~200 account writes, ~500 storage slot writes
- ~200 × (~80 byte AccountDelta) + ~500 × (32-byte slot + ~40 byte key) ≈ 35 KB/block
- 70M blocks × 35 KB ≈ 2.4 TB for full mainnet archive
- Acceptable for archive operators (java-tron archive is similar or larger).
  Default `full` mode prunes to last ~10K blocks → ~350 MB; trivial.

## Read path: reconstruct state at block N

### Account reads

```
ReadAccountAt(db, addr, N):
  # Fast path: query the inverse index for the latest change to addr at any block ≤ N.
  # If no rows found, the account hasn't changed since N → return live state.
  latestChange = SeekLE(db, "sh-i-a-"||addr||MAX_UINT64, "sh-i-a-"||addr||0)
  if latestChange == nil:
    return ReadAccountLive(db, addr)

  # If the latest change ≤ N is at some block M, the state we want IS the live state
  # rolled back through deltas at blocks M+1, M+2, ..., HEAD.
  # But we only need the pre-state at block M (because nothing changed between M and N).
  # That pre-state is recoverable by applying delta_M.

  # Walk from HEAD backward, applying each AccountDelta as a rollback.
  acc = ReadAccountLive(db, addr)
  for blockNum := HEAD; blockNum > N; blockNum-- :
    delta = ReadAccountDelta(db, blockNum, addr)
    if delta != nil:
      acc = applyRollback(acc, delta)  // undo this block's mutation
  return acc
```

Worst case: addr was modified every block between HEAD and N. Best case: addr
hasn't been touched in a long time — `inverse index` short-circuits to the
live read.

Practical case (TRON wallet account, queried mid-day for yesterday's balance):
~28K blocks back. ~10 hits on the inverse index. Each hit = one Pebble Get.
Total: ~10 ms for a "balance yesterday" query. Acceptable.

### Storage-slot reads

Same pattern, keyed by (addr, slot).

### Block-level state reads (rare)

Full `state.StateDB` reconstruction at block N (used by `debug_traceBlockByNumber`)
walks all `sh-a-` rows between N+1 and HEAD, applies them as rollbacks to the
live state. Cost is O(touched_entries_between_N_and_HEAD) — for old blocks
this can be large, so we cache the reconstructed StateDB by block for the
duration of a trace request.

## Pruning (full mode)

Background goroutine (mirrors geth's history-pruner):

```
every 1 minute:
  cutoff = max(0, solidifiedBlock - prune_window)
  range delete sh-* with blockNum < cutoff:
    sh-m- prefix in [..., cutoff)    # cheap, single prefix
    sh-a- prefix in [..., cutoff)
    sh-s- prefix in [..., cutoff)
    sh-i-a- entries with blockNum < cutoff (more expensive — per-addr scan)
    sh-i-s- entries with blockNum < cutoff (same)
```

`sh-i-a-` / `sh-i-s-` pruning is the only non-trivial bit (key-prefix doesn't
embed blockNum first). Use Pebble's `RangeDelete` for the `sh-m-/sh-a-/sh-s-`
prefixes and a periodic scan for inverse-index cleanup.

## Fork-rewind safety

All `sh-*` writes route through `bc.buffer`. `switchFork`'s `DiscardBlock`
already drops the orphan branch's buffer layers; we get history rollback for
free.

The buffer's `flushBufferUpToSolidified` semantics mean history entries are
only persisted once a block is solidified, so reads "at block N" where N >
solidified must consult the buffer layered above disk. Reader API takes the
same buffer-aware reader the rest of `core/state` does.

## Backwards compatibility

- Datadirs synced without history support: no `sh-*` rows. `ReadAccountAt`
  returns the live state for any N (degraded mode) and logs a warning at
  startup. To populate, operator re-syncs in archive mode from genesis (or
  from a snapshot taken before history-index was active).
- Future scheme bumps: `sh-cfg-` records `schema_ver`; mismatches at startup
  refuse to launch with a clear "rebuild your archive" error.

## API surface

New `core/state.HistoryReader` interface:

```go
type HistoryReader interface {
    AccountAt(addr Address, blockNum uint64) (*Account, error)
    StorageAt(addr Address, slot Hash, blockNum uint64) (Hash, error)
    CodeAt(addr Address, blockNum uint64) ([]byte, error)
}
```

JSON-RPC additions (mirror geth):

- `eth_getBalance(addr, blockNum)`
- `eth_getStorageAt(addr, slot, blockNum)`
- `eth_getCode(addr, blockNum)`
- `debug_traceBlockByNumber(blockNum, traceConfig)` — reuse the
  history reader to open a `StateDB` view at the parent block

TRON-specific (mirror java-tron solidity API):

- `getAccountAt(addr, blockNum)`
- `getResourceAt(addr, blockNum)`
- `triggerConstantContractAt(req, blockNum)`

## Configuration

`gtron.toml`:

```toml
[history]
mode = "archive"             # or "full"
prune_window = 27000         # blocks; only used in full mode
```

Default: `mode = "full"`, `prune_window` = `27 * maintSlots ≈ 27 * 1024`.
Operator flips to `archive` and restarts — backfill from latest snapshot or
re-sync depending on their situation. The audit doc captures both paths.

## Acceptance criteria

- `state.HistoryReader` returns byte-exact values for any past block in
  archive mode, compared against java-tron's archive node on the same chain
- `eth_getBalance(addr, oldblock)` p99 latency < 50 ms on Nile archive
- `debug_traceTransaction(oldtx)` succeeds on any archive-mode block
- Full-mode operators see <5% extra storage growth (only ~5K blocks of
  history retained)
- Switch-fork rewind: a reorg that drops blocks B0..B5 also drops their
  history entries — verified by a test that triggers a reorg, then asserts
  no `sh-*` rows for B0..B5 remain
- `make test` stays green
- Long-running Nile soak (24h archive mode) shows linear (not super-linear)
  storage growth

## Open questions for slice planning

1. **TVM contract storage** — `state.StateDB.SetState(addr, slot, val)` is
   the hot path. Need to confirm the journal records every write (it does),
   then add the pre-value capture hook. Estimated diff: one new field on
   `stateObject` + a hook in `setState`.

2. **TRC10 balances** — sparse map per account. Pre-image is the entire
   asset_id → balance map at block start, or just the delta entries? Going
   with delta entries (smaller, requires `applyRollback` to merge).

3. **Resource sliding-window fields** — `latest_consume_time*`, public-net
   times. These mutate every transparent tx. Decide whether to history
   these or recompute on read. Spec defaults to "history them" for
   simplicity; can swap to "recompute" later if storage proves problematic.

4. **Vote tally**: `WitnessVoteCount` mutates per vote tx. History-as-delta.
   Active-witness rotation at maintenance is a derived value; recompute on
   read from the historied `WitnessVoteCount`.

5. **Backfill tool**: do we ship a "scan blocks N..M and synthesize history
   entries" CLI? Slice 6 (operator recovery) covers this.

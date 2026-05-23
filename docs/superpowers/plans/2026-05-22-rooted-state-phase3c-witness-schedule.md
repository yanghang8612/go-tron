# Rooted State Phase 3c — Witness Schedule Implementation Plan

> **For agentic workers:** tightly-coupled single change (NOT independent tasks). Execute inline, bundle as ONE change (genesis seed + read-path + write-path must land together, like 3b). Steps use checkbox (`- [ ]`) syntax.

**Goal:** Root the global witness schedule — the active witness list and the witness index — into the system account's `SystemWitnessSchedule` (0x0002) KV, so they rewind with the full state root for "restart sync from historical height."

**Architecture:** Mirror 3b's rooted-dynprops shape, but simpler — no new hot cache. The active list keeps its existing `bc.activeWitnesses` atomic; only its *persistence* moves from `bc.buffer` to the rooted system-KV. The witness index needs no cache: all 6 consensus readers already hold a `*StateDB`, and the 2 RPC readers + startup open a head-root sysKV (`bc.sysKVAt`). Same-block actuator→maintenance visibility is preserved because the actuator's `ctx.State` is the *same `*StateDB` instance* maintenance reads.

**Tech stack:** Go 1.25, go-tron `core/state` (per-account KV trie), `kvdomains.SystemWitnessSchedule`.

**Encoding:** reuse the existing `4-byte BE count || N×21B addresses` wire format as the system-KV value. No new encoding lineage.

**Scope (design doc lines 200-202, 371-394, 581-594 + advisor):**
- **IN:** active witness list (`ActiveWitnesses` key), witness index (`WitnessIndex` key) → `SystemWitnessSchedule`.
- **OUT — shuffled witnesses:** design doc names them as the target location, but `WriteShuffledWitnesses` has **zero production callers** (only PBFT reads, which are therefore inert). Nothing to root yet. Document as reserved.
- **OUT — witness capsules (`w-`), `wlb-`:** Phase 4 (witness-owned domains).
- **OUT — `GenesisWitnesses` immutable list:** never mutates → no rewind benefit; stays flat.
- **OUT — reward `dl-`/`rvi-`:** SystemDelegation / later.

---

## File Structure

- **Create** `core/state/witness_schedule.go` — `*StateDB` methods + private encode/decode + KV key consts. Centralizes all witness-schedule system-KV access (callers never touch the domain/key/encoding directly).
- **Create** `core/state/witness_schedule_test.go` — round-trip + anchor/rewind at the state layer.
- **Modify** `core/genesis.go` — seed index + initial active set into the genesis root inside `genesisBlockAndStateRoot`; drop the flat `AppendWitnessIndex` in the post-root witness loop.
- **Modify** `actuator/witness.go` — `ctx.State.AppendWitnessIndex(addr)` instead of `rawdb.AppendWitnessIndex(ctx.DB, addr)`.
- **Modify** `core/blockchain.go` — `SetActiveWitnesses(statedb, …)`, `reloadActiveWitnesses(lcaRoot)`, startup read-from-sysKV (delete derive-and-write fallback), `gatherWitnessVotes`/`loadWitnessesIntoState` index reads, `SetActiveWitnessesForTest`.
- **Modify** `core/reward.go` — 2 index reads → `statedb.ReadWitnessIndex()`.
- **Modify** `core/block_builder.go` — index read → `statedb.ReadWitnessIndex()`.
- **Modify** `core/blockchain_history_backfill.go` — index read (verify statedb in scope; else sysKV at the parent root).
- **Modify** `core/tron_backend.go` — 2 RPC index reads → head-root sysKV (or reuse the in-scope at-root statedb).
- **Modify** `core/rawdb/accessors_chain.go` + `schema.go` — DELETE the now-dead flat accessors `WriteActiveWitnesses`/`ReadActiveWitnesses`/`WriteWitnessIndex`/`ReadWitnessIndex`/`AppendWitnessIndex` + `witnessIndexReadWriter` + the `activeWitnessesKey`/`witnessIndexKey` flat keys.
- **Modify** `core/rawdb/accessors_chain_test.go` — drop `TestActiveWitnesses`/`TestWitnessIndex` (flat path gone).
- **Modify** `core/blockchain_test.go` (+ genesis determinism test) — dual-mechanism rewind test, index anchor test.

---

## Task 1: State-layer witness-schedule API

**Files:** Create `core/state/witness_schedule.go`, `core/state/witness_schedule_test.go`

- [ ] **Encode/decode + keys** (private to `state`): `witnessScheduleActiveKey = []byte("ActiveWitnesses")`, `witnessScheduleIndexKey = []byte("WitnessIndex")`; `encodeAddressList([]tcommon.Address) []byte` and `decodeAddressList([]byte) []tcommon.Address` using `4 + N*common.AddressLength` (BE count, 21-byte addrs) — copied byte-for-byte from the old `accessors_chain.go` pack/unpack so the format is identical.
- [ ] **`*StateDB` methods:**
  - `ReadActiveWitnesses() []tcommon.Address` — `SystemKVGet(SystemWitnessSchedule, activeKey)`; nil/!ok → nil; decode.
  - `WriteActiveWitnesses(addrs []tcommon.Address)` — `SystemKVPut(…, encode(addrs))`.
  - `ReadWitnessIndex() []tcommon.Address` — symmetric.
  - `WriteWitnessIndex(addrs []tcommon.Address)` — symmetric.
  - `AppendWitnessIndex(addr tcommon.Address)` — read-modify-write with dedup (mirror old `rawdb.AppendWitnessIndex` semantics).
- [ ] **Test** (`witness_schedule_test.go`): write index+active via a fresh test StateDB → Commit → reopen at the root → read back equal (anchor); write a second root with a different set → assert roots differ and each reopen recovers its own set (rewind); `AppendWitnessIndex` dedups.
- [ ] Run: `go test ./core/state/ -run WitnessSchedule -v` → PASS.

## Task 2: Genesis seeds index + active set into the root

**Files:** `core/genesis.go`

- [ ] In `genesisBlockAndStateRoot`, after the rooted-dynprops build and before `statedb.Commit()`: build `idx := []addr` from `genesis.Witnesses` (in config order) and `statedb.WriteWitnessIndex(idx)`. Compute the initial active set **in memory** — feed `(gw.Address, gw.VoteCount)` straight into `dpos.SelectActiveWitnessesWithOptimization(votes, dp.ConsensusLogicOptimization())` (NO capsule read-back) — then `statedb.WriteActiveWitnesses(active)`.
- [ ] In `SetupGenesisBlockWithAncient`'s witness loop: **delete** `rawdb.AppendWitnessIndex(db, gw.Address)`. Keep `rawdb.WriteWitness` (capsule, Phase 4) and `rawdb.WriteGenesisWitnesses`.
- [ ] Verify the genesis block hash is unchanged (state root not in header) and `genesisBlockAndStateRoot` still deterministic.
- [ ] Run: `go test ./core/ -run Genesis -v` → PASS (incl. `TestSetupGenesisBlock_RootedDynPropsDeterministic` — now also covers index/active).

## Task 3: Actuator write path

**Files:** `actuator/witness.go`

- [ ] Replace `rawdb.AppendWitnessIndex(ctx.DB, ownerAddr)` (line 72) with `ctx.State.AppendWitnessIndex(ownerAddr)`. Keep `rawdb.WriteWitness(ctx.DB, …)` (capsule). Same-block visibility holds (ctx.State == maintenance statedb); journaled so a tx revert rolls the append back.
- [ ] Run: `go test ./actuator/ -run Witness -v` → PASS.

## Task 4: Consensus read sweep (statedb in hand)

**Files:** `core/blockchain.go`, `core/reward.go`, `core/block_builder.go`, `core/blockchain_history_backfill.go`

- [ ] `gatherWitnessVotes` (~1815) and `loadWitnessesIntoState` (~1559): `rawdb.ReadWitnessIndex(bc.buffer)` → `statedb.ReadWitnessIndex()`.
- [ ] `reward.go` `buildStandbyWitnessPaySet` (~108) and `maintenanceWitnessVotes` (~214): `rawdb.ReadWitnessIndex(db)` → `statedb.ReadWitnessIndex()`.
- [ ] `block_builder.go` (~61): `rawdb.ReadWitnessIndex(bc.buffer)` → `statedb.ReadWitnessIndex()` (statedb at parentRoot — more faithful than the buffer merge).
- [ ] `blockchain_history_backfill.go` (~300): verify a `*StateDB` at the parent root is in scope; if so `statedb.ReadWitnessIndex()`, else open `bc.sysKVAt(parentRoot)`.
- [ ] Run: `go test ./core/ -run 'Reward|Maintenance|Backfill|BlockBuilder' -count=1` → PASS.

## Task 5: Active-witness write + reload + startup

**Files:** `core/blockchain.go`

- [ ] `SetActiveWitnesses(statedb *state.StateDB, witnesses []tcommon.Address)`: `bc.activeWitnesses.Store(witnesses)` + `statedb.WriteActiveWitnesses(witnesses)`. **Delete** `rawdb.WriteActiveWitnesses(bc.buffer, witnesses)` (no double-write). Call site ~846 → `bc.SetActiveWitnesses(statedb, newActive)`.
- [ ] `reloadActiveWitnesses(lcaRoot tcommon.Hash)`: `if sysKV := bc.sysKVAt(lcaRoot); sysKV != nil { if aw := sysKV.ReadActiveWitnesses(); aw != nil { bc.activeWitnesses.Store(aw) } }`. Update switchFork to call it right after `reloadDynPropsCache(lcaRoot)` (reuse the same `lcaRoot`); update the applyBlock error-defer to pass the current head root.
- [ ] Startup (~312-330): **delete** the `ReadActiveWitnesses(db)` + derive-from-index + `WriteActiveWitnesses(db)` fallback. Replace with: `if sysKV := bc.sysKVAt(bc.HeadStateRoot()); sysKV != nil { if aw := sysKV.ReadActiveWitnesses(); len(aw) > 0 { bc.activeWitnesses.Store(aw) } }`. (Genesis now always seeds active into the root → fallback unreachable.)
- [ ] Add `SetActiveWitnessesForTest(witnesses []tcommon.Address)` (atomic Store only) for cross-package tests that seed without a block. Update any non-applyBlock callers of `SetActiveWitnesses` (grep first).
- [ ] Run: `go test ./core/ -run 'SwitchFork|ActiveWitness|Reorg' -count=1` → PASS.

## Task 6: RPC read sweep

**Files:** `core/tron_backend.go`

- [ ] Index reads at ~441 and ~1557: route through a `*StateDB` — reuse the in-scope at-root statedb if present (~1565 builds a Context with one), else `b.chain.sysKVAt(b.chain.HeadStateRoot())`. Handle nil (return empty).
- [ ] Run: `go test ./core/ -run 'TronBackend|ListWitness|GetWitness' -count=1` → PASS.

## Task 7: Delete dead flat accessors

**Files:** `core/rawdb/accessors_chain.go`, `schema.go`, `accessors_chain_test.go`

- [ ] Delete `WriteActiveWitnesses`, `ReadActiveWitnesses`, `WriteWitnessIndex`, `ReadWitnessIndex`, `AppendWitnessIndex`, `witnessIndexReadWriter`. Delete unused `activeWitnessesKey`, `witnessIndexKey` from `schema.go`.
- [ ] Delete `TestActiveWitnesses`, `TestWitnessIndex` from `accessors_chain_test.go` (flat path no longer exists; round-trip now covered by Task 1's state test).
- [ ] Add a one-line comment at the `SystemWitnessSchedule` const (kvdomains) or in this plan: shuffled witnesses are reserved for this domain once a writer exists; currently inert.
- [ ] Run: `go build ./...` → no unused-symbol / missing-ref errors.

## Task 8: Acceptance tests + full verification

**Files:** `core/blockchain_test.go`

- [ ] **Dual-mechanism rewind test** (the 3c-specific gate): seed a chain; create a witness in a pre-maintenance block (index grows); insert a maintenance block that changes the active set AND flips `is_jobs`; capture `rootAfter`. switchFork rewind across the maintenance boundary to the pre-maintenance head. Assert **all three** reflect pre-maintenance state: `bc.ActiveWitnesses()` (rooted reload), per-witness `is_jobs` (capsule via `bc.buffer` rewind), and the witness index (rooted). This is the dual-rewind interaction (root vs buffer) the 3b suite never exercised.
- [ ] **Index anchor test:** register a witness → assert head root moves and `sysKV(newRoot).ReadWitnessIndex()` contains it; reopen the pre-register root → index does NOT contain it.
- [ ] **Genesis determinism:** confirm `TestSetupGenesisBlock_RootedDynPropsDeterministic` still green (now covers index+active in the root).
- [ ] Run `go test ./... -count=1`, `go vet ./...`.
- [ ] Run `go test ./core/ -run TestBlockChainInsertBlock_MaintenanceFiresOncePerBoundary -count=20` — confirm 3c didn't deepen the known flake (not fixing it).
- [ ] Update memory `project_rooted_state_refactor.md` (3c DONE section) + `MEMORY.md` index.

---

## Self-Review checklist
- Spec coverage: active list + index rooted; shuffled/capsules/genesis-witnesses/reward correctly OUT with rationale.
- Removal list (advisor): all 6 in-place removals enumerated in Tasks 2/3/5/7 — no zombie `WriteActiveWitnesses(bc.buffer)`.
- switchFork ordering: `reloadActiveWitnesses(lcaRoot)` takes explicit LCA root (3b trap).
- Bundle-as-one: genesis seed (T2) + read sweep (T4/T6) + write path (T3/T5) land together.

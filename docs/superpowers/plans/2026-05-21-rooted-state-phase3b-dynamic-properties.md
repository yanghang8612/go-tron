# Rooted State — Phase 3b: Dynamic Properties → System-Account KV — Implementation Plan

> **For agentic workers:** tightly-coupled slice — execute INLINE (no green intermediate state), TDD where the package can compile. Checkbox (`- [ ]`) steps.
>
> **Source spec:** `/Users/asuka/Projects/asuka/go/go-tron/docs/superpowers/specs/2026-05-21-rooted-generic-state-kv-design.md`. Builds on Phase 1 (`43edf91`), Phase 2 (`8efe2bc`), Phase 3a (system account + `SystemKVGet/Put/Delete`, on branch), the P0 fix (`32f0437`), and the cherry-picked marshal-determinism fix.
>
> **Commits:** GPG-signed; squash to 1 feat + 1 test before merge. Don't commit plan docs.

**Goal:** Make the ~129 consensus dynamic properties part of the rooted state — persisted in the system account's `SystemDynamicProperty` KV (committed by `statedb.Commit`, thus in the internal full-state root) — so that reopening a historical root restores them. Keep the 4 "head-pointer" keys derived/unrooted (USER DECISION 2026-05-21).

**Architecture:** The in-memory `dynPropsCache` (canonical head snapshot, `blockchain.go:113`) is THE source of truth for live reads; `applyBlock` mutates a `Copy()` of it and stores it back. Disk Load happens at exactly 5 sites (startup init, cache-miss fallback, fork reload, witness bootstrap, producer build). Persistence **splits**: rooted keys ⇄ system-account KV (via `StateDB.SystemKVPut` + a batched `SystemKVGetBatch`); the 4 derived keys ⇄ flat `dp-` rawdb (unchanged). Rooted keys are all dirtied pre-`Commit`; a new `FlushRooted(*StateDB)` stages them into system-KV right before `Commit`. The 4 derived keys are set post-`Commit` and keep flushing to `dp-`.

**THE BUNDLE (advisor-mandated):** 3b-1 (mechanism) + 3b-2 (genesis seed) + 3b-3 (pipeline) land **together** in one atomic change. Reason: once Load reads rooted keys from system-KV but genesis hasn't seeded them, a fresh-DB startup pulls *defaults* for all 129 rooted keys (`maintenance_time_interval`, witness pay, every governance/economic param) and block #1 runs with wrong consensus params. The rewind acceptance test also needs seeded rooted values to be meaningful. No legacy shim, no staged commits — intermediate states are not green and must not be shipped.

## Decisions

- **Derived (unrooted) key set — exactly 4:** `latest_block_header_number`, `latest_block_header_timestamp`, `latest_solidified_block_num` (int64, in `props`), and `latest_block_header_hash` (the `latestBlockHeaderHash`/`hashDirty` field, NOT in `defaultStringProps`). Everything else roots: all 123 remaining `defaultProps` int64 keys + all 6 `defaultStringProps` keys (`energy_price_history`, `bandwidth_price_history`, `memo_fee_history`, `block_filled_slots`, `available_contract_type`, `active_default_operations`).
- **System-KV value encoding = identical to `dp-`:** int64 → 8-byte BE; string → raw bytes. Key = the property name bytes under domain `kvdomains.SystemDynamicProperty`. Shared decode logic.
- **Load merge order:** derived keys from `dp-` FIRST, then overlay rooted keys from system-KV (sysKV-wins). This makes the merge robust even if a rooted key were ever stray-written to `dp-`.
- **Read source for live queries = the in-memory cache,** not disk. `DynProps()`, `NextMaintenanceTime()` route through `bc.cachedDynProps()` (a `Copy()`), removing the per-call 129-read load (critical: `ValidateTransaction` calls `DynProps()` per admitted tx).
- **One trie open per Load:** `SystemKVGet` opens the account KV trie on every call. A new `SystemKVGetBatch(domain, keys)` opens it once and resolves all ~129 keys — used by `LoadDynamicProperties`.
- **No dual-write.** `dp-` holds only the 4 derived keys after this phase (fresh-DB-only; no migration).

---

## Slice 3b (bundled): rooted/derived split + genesis seed + pipeline wiring

**Files:** `core/state/dynamic_properties.go`, `core/state/account_kv.go`, `core/state/system_store.go`, `core/genesis.go`, `core/blockchain.go`, `core/block_builder.go`; tests `core/state/dynamic_properties_rooted_test.go`, `core/blockchain_rooted_dynprops_test.go`.

### A. Mechanism (`core/state/`)

- [ ] **A1. Batched system-KV read** — in `account_kv.go`, add a one-trie-open batch resolver; wrap it in `system_store.go`:

```go
// account_kv.go
// GetAccountKVBatch opens owner's KV trie ONCE and resolves every key in one
// domain, returning name->value for present keys (presence prefix stripped).
// The dirty overlay (obj.kvDirty) is consulted first per key, matching
// GetAccountKV; a freshly-opened StateDB (the Load case) has an empty overlay.
func (s *StateDB) GetAccountKVBatch(owner tcommon.Address, domain kvdomains.KVDomain, keys [][]byte) (map[string][]byte, error) {
	if !kvdomains.IsRegistered(domain) {
		return nil, fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	out := make(map[string][]byte, len(keys))
	obj := s.getStateObject(owner)
	if obj == nil {
		return out, nil
	}
	tr, err := s.db.OpenTrie(ethcommon.Hash(obj.accountKVRoot))
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		comp := kvCompositeKey(domain, key)
		if e, ok := obj.kvDirty[string(comp)]; ok {
			if !e.deleted {
				out[string(key)] = append([]byte{}, e.val...)
			}
			continue
		}
		raw, err := tr.Get(kvTrieKey(comp))
		if err != nil {
			return nil, err
		}
		if len(raw) == 0 {
			continue
		}
		out[string(key)] = append([]byte{}, raw[1:]...) // strip presence prefix
	}
	return out, nil
}
```
```go
// system_store.go
func (s *StateDB) SystemKVGetBatch(domain kvdomains.KVDomain, keys [][]byte) (map[string][]byte, error) {
	return s.GetAccountKVBatch(tcommon.SystemAccountAddress, domain, keys)
}
```

- [ ] **A2. derived-key set + FlushRooted + 2-arg Load** in `dynamic_properties.go` (import `kvdomains`):

```go
var derivedDPKeys = map[string]struct{}{
	"latest_block_header_number":    {},
	"latest_block_header_timestamp": {},
	"latest_block_header_hash":      {}, // routed via the hash field, not props
	"latest_solidified_block_num":   {},
}

func isDerivedDPKey(k string) bool { _, ok := derivedDPKeys[k]; return ok }

// FlushRooted stages every dirty NON-derived dynamic property into the system
// account's SystemDynamicProperty KV (committed by statedb.Commit → rooted).
// Call BEFORE statedb.Commit(); derived keys are left for Flush(db) post-Commit.
func (dp *DynamicProperties) FlushRooted(s *StateDB) error {
	buf := make([]byte, 8)
	for k := range dp.dirty {
		if isDerivedDPKey(k) {
			continue
		}
		binary.BigEndian.PutUint64(buf, uint64(dp.props[k]))
		if err := s.SystemKVPut(kvdomains.SystemDynamicProperty, []byte(k), append([]byte(nil), buf...)); err != nil {
			return err
		}
		delete(dp.dirty, k)
	}
	for k := range dp.stringDirty {
		if isDerivedDPKey(k) {
			continue
		}
		if err := s.SystemKVPut(kvdomains.SystemDynamicProperty, []byte(k), []byte(dp.stringProps[k])); err != nil {
			return err
		}
		delete(dp.stringDirty, k)
	}
	return nil
}

// loadDerivedFromDB reads ONLY the 4 derived keys from flat dp- rawdb.
func (dp *DynamicProperties) loadDerivedFromDB(db ethdb.KeyValueReader) {
	if iter, ok := db.(ethdb.Iteratee); ok {
		rawdb.IterateDynamicProperties(iter, func(name string, value []byte) {
			if isDerivedDPKey(name) {
				applyLoadedDPValue(dp, name, value)
			}
		})
		return
	}
	for k := range derivedDPKeys {
		applyLoadedDPValue(dp, k, rawdb.ReadDynamicProperty(db, k)) // no-op on empty
	}
}

// LoadDynamicProperties builds DynamicProperties from persisted state: derived
// keys from flat dp- rawdb (db); rooted keys from the system account's KV
// (sysKV, a StateDB opened at the target root). sysKV may be nil (pre-genesis)
// — rooted keys then keep their defaults.
func LoadDynamicProperties(db ethdb.KeyValueReader, sysKV *StateDB) *DynamicProperties {
	dp := NewDynamicProperties()
	dp.loadDerivedFromDB(db)
	if sysKV == nil {
		return dp
	}
	keys := make([][]byte, 0, len(defaultProps)+len(defaultStringProps))
	for k := range defaultProps {
		if !isDerivedDPKey(k) {
			keys = append(keys, []byte(k))
		}
	}
	for k := range defaultStringProps {
		if !isDerivedDPKey(k) {
			keys = append(keys, []byte(k))
		}
	}
	vals, err := sysKV.SystemKVGetBatch(kvdomains.SystemDynamicProperty, keys)
	if err != nil {
		return dp // defaults for rooted on error; derived already loaded
	}
	for k := range defaultProps {
		if v, ok := vals[k]; ok && len(v) == 8 {
			dp.props[k] = int64(binary.BigEndian.Uint64(v))
		}
	}
	for k := range defaultStringProps {
		if v, ok := vals[k]; ok {
			dp.stringProps[k] = string(v)
		}
	}
	return dp
}
```
> `Flush(db)` is UNCHANGED — after `FlushRooted`, only derived keys remain dirty, so `Flush` writes only those (incl. the `hashDirty` path) to `dp-`. Drop the old all-keys scan in `LoadDynamicProperties` (now `loadDerivedFromDB`).

### B. Genesis seed (`core/genesis.go`) — 3b-2

- [ ] **B1.** In `genesisBlockAndStateRoot`, the system account is already created (3a). Build the `DynamicProperties`, then BEFORE the genesis `statedb.Commit()`: `dp.FlushRooted(statedb)` (rooted → system-KV, committed into the genesis root). After Commit, the existing `dp.Flush(db)` path writes only the 4 derived keys to `dp-`. Sequence: create system account → build dp → `dp.FlushRooted(statedb)` → `statedb.Commit()` → `dp.Flush(db)`. Audit the current dynprops construction location (~`SetupGenesisBlockWithAncient` line ~144) and move/adapt so `FlushRooted` runs on the same `statedb` that Commit roots.

### C. Pipeline wiring (`core/blockchain.go`, `core/block_builder.go`) — 3b-3

- [ ] **C1. applyBlock flush** — before `statedb.Commit()` (~line 890), after the maintenance + state-flag block, add:
```go
if err := dynProps.FlushRooted(statedb); err != nil {
	return fmt.Errorf("flush rooted dynamic properties: %w", err)
}
```
Placement note: must run before Commit (890) so writes enter the root. Running before or after `AccumulateHistory` (~882) is equivalent for SHI (SHI doesn't capture `kvChange` — see Risks); put it directly above Commit for clarity. The existing post-Commit `dynProps.Flush(bc.buffer)` (~918) now flushes only the 4 derived keys. (Verified: the only post-Commit dynprop writes are the 4 derived keys at 903-905 + `updateSolidifiedBlock`.)

- [ ] **C2. block_builder flush** — before its throwaway `statedb.Commit()` (~line 167), add the same `dynProps.FlushRooted(statedb)`. This is producer-vs-replay consistency: without it, the node's internal root for its own freshly-built block won't match what its applier recomputes on the same block. (Doesn't surface on the wire — root is out-of-band — but matters for any local consumer of the post-build root. Same necessity as C1.) No derived `dp-` flush here (throwaway build).

- [ ] **C3. Load sites — wire each to the correct root:**
  - **block_builder.go:49** — `statedb` is already opened at `parentRoot` (line ~44). Pass it: `state.LoadDynamicProperties(bc.buffer, statedb)`.
  - **blockchain.go:292** (startup cache init) — currently runs BEFORE `head` is known. **MOVE** the `bc.storeDynPropsCache(...)` line to AFTER `bc.currentBlock.Store(head)` (~line 304), then: `sysKV, _ := state.New(bc.HeadStateRoot(), bc.stateDB); bc.storeDynPropsCache(state.LoadDynamicProperties(buffer, sysKV))`. (Tolerate a nil/err sysKV → defaults; head root exists post-genesis.)
  - **blockchain.go:324** (witness bootstrap, reads `ConsensusLogicOptimization` — rooted) — runs after head stored: `sysKV, _ := state.New(bc.HeadStateRoot(), bc.stateDB); dynProps := state.LoadDynamicProperties(db, sysKV)`.
  - **blockchain.go:364** (`recoverHeadToAppliedState`, reads `LatestBlockHeaderNumber` — derived only) — `state.LoadDynamicProperties(db, nil)`.
  - **blockchain.go:1157** (`Close`, reads `LatestSolidifiedBlockNum` — derived only) — `state.LoadDynamicProperties(bc.buffer, nil)`.
  - **blockchain.go:1409** (`cachedDynProps` cache-miss fallback — advisor flagged: easy to miss) — `sysKV, _ := state.New(bc.HeadStateRoot(), bc.stateDB); return state.LoadDynamicProperties(bc.buffer, sysKV)`.
  - **blockchain.go:1419** (`reloadDynPropsCache`) — see C4.
  - **blockchain.go:1529** (`NextMaintenanceTime`, reads rooted) — route through the cache: `return bc.cachedDynProps().NextMaintenanceTime()`.
  - **blockchain.go:1578** (`DynProps`, full snapshot for RPC/`ValidateTransaction`) — route through the cache: `return bc.cachedDynProps()`.

- [ ] **C4. reloadDynPropsCache(lcaRoot) + switchFork (CONSENSUS-CRITICAL).** `reloadDynPropsCache` (1418) currently runs at switchFork:1233 BEFORE `currentBlock` is rewound to LCA (1247), so `HeadStateRoot()` would return the OLD head. Change the signature to take the LCA root explicitly:
```go
func (bc *BlockChain) reloadDynPropsCache(rootAt tcommon.Hash) {
	var sysKV *state.StateDB
	if rootAt != (tcommon.Hash{}) {
		sysKV, _ = state.New(rootAt, bc.stateDB)
	}
	bc.storeDynPropsCache(state.LoadDynamicProperties(bc.buffer, sysKV))
}
```
At the switchFork call site (1233), compute the LCA root with a genesis fallback mirroring `HeadStateRoot`:
```go
lcaRoot := rawdb.ReadBlockStateRoot(bc.chaindb, lcaHash)
if lcaRoot == (tcommon.Hash{}) {
	if n := rawdb.ReadBlockNumber(bc.chaindb, lcaHash); n != nil && *n == 0 {
		lcaRoot = rawdb.ReadGenesisStateRoot(bc.db)
	}
}
bc.reloadDynPropsCache(lcaRoot)
```
(`lcaHash` is in scope at 1207-1214.) Rationale: after `DiscardBlock` pops orphan layers, derived keys are rewound in the buffer; the rooted keys must be re-read from the LCA trie root. The re-apply loop (1254) then reads this LCA-correct cache via `cachedDynProps()`.

### D. Tests

- [ ] **D1.** `core/state/dynamic_properties_rooted_test.go` (package `state`, can use unexported helpers): rooted key round-trips through system-KV across `FlushRooted`+`Commit`+reopen; derived key is NOT in system-KV; `LoadDynamicProperties(db, sysKV)` merges rooted (from sysKV) + derived (from dp-). Use `newTestStateDB(t)` (existing helper) + write a derived key via `rawdb.WriteDynamicProperty(sdb.db.DiskDB(), "latest_block_header_number", be8(5))`.
- [ ] **D2. Genesis:** after genesis, open `state.New(genesisRoot, db)` and assert seeded rooted values readable (`maintenance_time_interval`, `next_maintenance_time`); genesis internal root identical across two fresh runs (determinism); java `accountStateRoot` unaffected (`account_state_root.go` untouched).
- [ ] **D3. Anchor:** a rooted dynprop change moves the internal full-state root (Phase 1/2 lacked this for dynprops).
- [ ] **D4. REWIND (ACCEPTANCE GATE):** build state at root R1 with a rooted key = X (e.g. `next_maintenance_time`), advance to R2 with = Y, reopen `state.New(R1, db)` and `LoadDynamicProperties(buffer, that)` → returns X. This is the whole point of the phase.
- [ ] **D5. Determinism + flake:** replay → identical root; the known `TestBlockChainInsertBlock_MaintenanceFiresOncePerBoundary` flake still passes (rerun if it flakes — pre-existing ~7%, not a regression).

### E. Verify + commit

- [ ] **E1.** `go build ./...`; `go test ./core/state/ ./core/ ./actuator/ -count=1`; `make lint`. The whole package must be green (no intermediate-state commits).
- [ ] **E2.** Squash to `feat(state): root dynamic properties into the system-account KV` + `test(state): rooted dynamic-properties round-trip, rewind, determinism`. GPG-signed. Don't `git add` plan docs.

---

## Test Plan (whole phase)

- rooted dynprop round-trips through system-KV across commit+reopen; derived keys stay in `dp-`.
- genesis seeds rooted dynprops into the genesis root; genesis root deterministic; java `accountStateRoot` unchanged.
- a rooted dynprop mutation changes the internal full-state root (anchor).
- **rewind**: an old root yields the old rooted dynprops (D4 acceptance gate).
- `next_maintenance_time` / `state_flag` drive maintenance correctly through system-KV (maintenance flake test still passes).
- block/tx wire encoding unchanged; full `go test ./...` green.

## Risks / notes

- **SHI gap is OUT OF SCOPE (verified safe).** `AccumulateHistory` (`history_capture.go`) does not capture `kvChange`/`kvResetChange` journal entries, so the system account's KV deltas are not in the State History Index. This is NOT a correctness problem: rooted state rewinds via the **root pointer** (`switchFork` → `state.New(parentRoot)` in the re-apply loop), and the SHI is a forward read-index that `DiscardBlock` itself pops — it is not a rewind engine. The SHI never captured dynprops (they lived in `dp-`, also uncaptured). Teaching `AccumulateHistory` about `kvChange` is at most an archive-read-completeness item for a later phase.
- **Performance:** the per-block hot path uses the in-memory cache, not Load. Disk Load (now ~129 batched point-reads over ONE trie open) happens only at startup/cache-miss/fork-reload/witness-bootstrap/producer-build. Live RPC/tx-admission reads route through the cache. The old `dp-` prefix-scan fast path is gone for rooted keys, but those reads moved off the hot path.
- **Producer/consumer root consistency:** block_builder (C2) and applyBlock (C1) must flush rooted dynprops identically. D2/D4 + the existing build/apply tests guard this.
- **switchFork ordering (C4):** the LCA-root reload is the single most consensus-critical wiring step. The explicit-root signature avoids coupling to the `currentBlock.Store` ordering.

## Out of scope

- **3c:** witness-schedule / maintenance-cycle state → `SystemWitnessSchedule` KV.
- SHI `kvChange` capture (archive-read completeness) → later phase.
- Validation guard (P2#1) + domain/owner policy (P2#2) → Phase 5 store audit.

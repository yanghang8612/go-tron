# Rooted State — Phase 2: Generic Account KV — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.
>
> **Source spec:** `/Users/asuka/Projects/asuka/go/go-tron/docs/superpowers/specs/2026-05-21-rooted-generic-state-kv-design.md` (main repo; not in this worktree). Builds on Phase 1 (merged to master at `43edf91`): `StateAccountV2` envelope, `common.AccountID`, `kvdomains` registry, account trie keyed by `Keccak256(AccountID20)`.
>
> **Commits:** GPG-signed (key configured; never `--no-gpg-sign`). Per-task commits are intermediate; squash to a feat+test pair before merge. Do not `git add` this plan file.

**Goal:** Give every account one rooted generic key-value space — a per-account MPT whose root (`AccountKVRoot`) is committed into the `StateAccountV2` envelope and therefore into the internal full-state root — with `StateDB` get/set/delete/reset, journal/snapshot/revert coverage, and KV-generation reset.

**Architecture:** In the existing **hash-mode** triedb, a per-account KV trie is just `trie.New(TrieID(accountKVRoot), trieDB)` — same content-addressed node store as the account trie, no owner-scoping. Writes accumulate in an in-memory dirty overlay on the `stateObject`; on `Commit`, each account with pending KV writes commits its KV trie, updates `obj.accountKVRoot`, and that root flows into the envelope. KV trie key = `Keccak256(domain_u16_be || logical_key)`.

**Tech Stack:** Go 1.25; go-ethereum `trie`/`triedb`/`trienode`/`crypto`; `core/state/kvdomains`.

## Scoping decisions (decided, not gated — all reversible/additive-later)

- **Deferred to Phase 7 (history/pruning): the flat physical latest-state index** (`state-kv-latest-v2`, `owner||gen||domain||key`). The committed per-account MPT alone gives correctness and block-level rooted rewind (the Phase 2 goal). The design lists this index as an open decision; we defer it.
- **Deferred to Phase 3: `IterateAccountKV` and `DeletePrefix`.** No Phase 2 consumer needs them. **Important consequence of the hashed key:** `Keccak256(domain||key)` scatters keys, so the MPT *cannot* do efficient domain-prefix enumeration — an efficient domain iterator would require the deferred flat index. When Phase 3 (dynamic-property loading) needs enumeration, the cheap correct paths are: (a) enumerate known property names and point-`Get`, or (b) `trie.NodeIterator` over the whole account KV trie with caller-side domain filtering (fine for ~100 dynprops at startup). Do **not** add a domain-prefix iterator on the hashed MPT — it would be quadratic/incorrect.
- **KV generation:** `ResetAccountKV` sets `AccountKVRoot=EmptyKVRoot` and increments `AccountKVGeneration`. Without the flat index the generation is not yet load-bearing for storage, but we bump it now (forward-compat for when the flat index lands) and it documents intent.
- **No value caching on reads** in Phase 2 (reads miss → open the KV trie). A read cache is a later optimization; no Phase 2 workload stresses it.

## Key facts (established; do not re-derive)

- `core/state/database.go`: `triedb.NewDatabase(diskdb, nil)` (hash mode); `OpenTrie(root) = trie.New(trie.TrieID(root), db.trieDB)`. Reusable for KV tries.
- `core/state/statedb.go` `Commit()`: per-dirty-account loop builds the `StateAccountV2` envelope (`AccountKVRoot: obj.accountKVRoot`) then `s.trie.Update(trieKey(addr), data)`; after the loop, `s.trie.Commit(false)` + `TrieDB().Update/Commit`. The KV-trie commit hook goes **inside the loop, before the envelope is built**.
- `core/state/state_object.go`: `stateObject` already has `accountKVRoot tcommon.Hash` and `accountKVGeneration uint64` (Phase 1), defaulting to `EmptyKVRoot`/0; `newStateObject`/`newEmptyStateObject` init them; `Copy()`'s deep-copy literal copies them.
- `core/state/state_account.go`: `EmptyKVRoot` (= empty-trie root), `StateAccountV2`.
- `core/state/journal.go`: `journalChange` interface = `revert(stateObjects map[tcommon.Address]*stateObject, witnesses map[tcommon.Address]*types.Witness)`. `storageChange` is the closest pattern to mirror.
- `GetOrCreateAccount(addr) *stateObject` and `getStateObject(addr) *stateObject` (nil if absent) exist.
- **go-ethereum trie treats `Update(key, emptyValue)` as delete** — an empty value cannot be stored raw. We therefore wrap persisted KV values with a 1-byte presence prefix `0x01` (so empty-but-present round-trips and stays distinct from absent). This wrapper is internal to the generic KV trie; it never reaches java-tron-visible data.

---

### Task 1: In-memory generic account-KV layer (no persistence yet)

**Files:**
- Create: `core/state/account_kv.go`
- Modify: `core/state/state_object.go` (add `kvDirty` field; init in both constructors; copy in `Copy()` deep-copy)
- Modify: `core/state/journal.go` (add `kvChange`)
- Test: `core/state/account_kv_test.go`

- [ ] **Step 1: Write the failing test** — create `core/state/account_kv_test.go`:

```go
package state

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestAccountKVSetGet(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x11)
	if err := sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, ok, err := sdb.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k1"))
	if err != nil || !ok || !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("get = (%q,%v,%v), want (v1,true,nil)", got, ok, err)
	}
}

func TestAccountKVDomainIsolation(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x11)
	_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k"), []byte("a"))
	_ = sdb.SetAccountKV(addr, kvdomains.ContractStorage, []byte("k"), []byte("b"))
	g1, _, _ := sdb.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k"))
	g2, _, _ := sdb.GetAccountKV(addr, kvdomains.ContractStorage, []byte("k"))
	if !bytes.Equal(g1, []byte("a")) || !bytes.Equal(g2, []byte("b")) {
		t.Fatalf("domain isolation broken: %q %q", g1, g2)
	}
}

func TestAccountKVDelete(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x11)
	_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k"), []byte("v"))
	if err := sdb.DeleteAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok, _ := sdb.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k")); ok {
		t.Fatal("key should be absent after delete")
	}
}

func TestAccountKVUnregisteredDomain(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x11)
	if err := sdb.SetAccountKV(addr, kvdomains.KVDomain(0x0099), []byte("k"), []byte("v")); err == nil {
		t.Fatal("set with unregistered domain must error")
	}
}

func TestAccountKVSnapshotRevert(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x11)
	_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k"), []byte("v1"))
	snap := sdb.Snapshot()
	_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k"), []byte("v2"))
	_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k2"), []byte("x"))
	sdb.RevertToSnapshot(snap)
	if g, ok, _ := sdb.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k")); !ok || !bytes.Equal(g, []byte("v1")) {
		t.Fatalf("k after revert = %q, want v1", g)
	}
	if _, ok, _ := sdb.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k2")); ok {
		t.Fatal("k2 should be gone after revert")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./core/state/ -run TestAccountKV -v`
Expected: FAIL — `SetAccountKV`/`GetAccountKV`/`DeleteAccountKV` undefined.

- [ ] **Step 3a: Add the `kvDirty` field to `stateObject`** (`core/state/state_object.go`)

After the Phase-1 KV fields, add `kvDirty`:

```go
	// Rooted generic-KV fields (Phase 1: always EmptyKVRoot / 0; no KV trie yet).
	accountKVRoot       tcommon.Hash
	accountKVGeneration uint64

	// kvDirty holds pending generic-KV writes keyed by string(domainBE2||key).
	kvDirty map[string]kvEntry
```

Init in `newStateObject` (add `kvDirty: make(map[string]kvEntry),`) and in `newEmptyStateObject` (same). In `Copy()`'s deep-copy literal (`core/state/statedb.go`), add a deep copy:

```go
		kvDirtyCopy := make(map[string]kvEntry, len(obj.kvDirty))
		for k, v := range obj.kvDirty {
			ec := kvEntry{deleted: v.deleted}
			if v.val != nil {
				ec.val = append([]byte{}, v.val...)
			}
			kvDirtyCopy[k] = ec
		}
```
and add `kvDirty: kvDirtyCopy,` to the `newObj := &stateObject{...}` literal.

- [ ] **Step 3b: Add `kvChange` to the journal** (`core/state/journal.go`)

```go
// kvChange records a single generic-KV overlay change for revert.
type kvChange struct {
	address   tcommon.Address
	mapKey    string
	hadEntry  bool
	prevEntry kvEntry
}

func (e kvChange) revert(stateObjects map[tcommon.Address]*stateObject, _ map[tcommon.Address]*types.Witness) {
	obj := stateObjects[e.address]
	if obj == nil {
		return
	}
	if e.hadEntry {
		obj.kvDirty[e.mapKey] = e.prevEntry
	} else {
		delete(obj.kvDirty, e.mapKey)
	}
}
```

- [ ] **Step 3c: Create `core/state/account_kv.go`** (helpers + in-memory Get/Set/Delete; persisted-trie read with presence-byte strip — the persisted trie is empty until Task 2, so the strip is dormant here)

```go
package state

import (
	"encoding/binary"
	"fmt"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// kvPresencePrefix wraps persisted KV values so an empty-but-present value
// stays distinct from an absent key (go-ethereum tries treat an empty value as
// a delete). Internal to the generic KV trie; never java-tron-visible.
const kvPresencePrefix = 0x01

// kvEntry is one pending account-KV write in the dirty overlay. deleted=true is
// a tombstone; deleted=false means val is present (val may be empty but != nil).
type kvEntry struct {
	val     []byte
	deleted bool
}

// kvCompositeKey is the pre-hash logical key: domain (big-endian u16) || key.
func kvCompositeKey(domain kvdomains.KVDomain, key []byte) []byte {
	out := make([]byte, 2+len(key))
	binary.BigEndian.PutUint16(out, uint16(domain))
	copy(out[2:], key)
	return out
}

// kvTrieKey is the per-account KV trie key: Keccak256(domain || key).
func kvTrieKey(composite []byte) []byte {
	return crypto.Keccak256(composite)
}

// GetAccountKV reads a generic-KV value for owner. Returns (value, exists, err).
func (s *StateDB) GetAccountKV(owner tcommon.Address, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	if !kvdomains.IsRegistered(domain) {
		return nil, false, fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	obj := s.getStateObject(owner)
	if obj == nil {
		return nil, false, nil
	}
	comp := kvCompositeKey(domain, key)
	if e, ok := obj.kvDirty[string(comp)]; ok {
		if e.deleted {
			return nil, false, nil
		}
		return append([]byte{}, e.val...), true, nil
	}
	tr, err := s.db.OpenTrie(ethcommon.Hash(obj.accountKVRoot))
	if err != nil {
		return nil, false, err
	}
	raw, err := tr.Get(kvTrieKey(comp))
	if err != nil {
		return nil, false, err
	}
	if len(raw) == 0 {
		return nil, false, nil
	}
	return append([]byte{}, raw[1:]...), true, nil // strip presence prefix
}

// SetAccountKV stages a generic-KV write for owner (creating the account if absent).
func (s *StateDB) SetAccountKV(owner tcommon.Address, domain kvdomains.KVDomain, key, value []byte) error {
	if !kvdomains.IsRegistered(domain) {
		return fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	obj := s.GetOrCreateAccount(owner)
	mk := string(kvCompositeKey(domain, key))
	prev, had := obj.kvDirty[mk]
	s.journal.append(kvChange{address: owner, mapKey: mk, hadEntry: had, prevEntry: prev})
	obj.kvDirty[mk] = kvEntry{val: append([]byte{}, value...), deleted: false}
	obj.markDirty()
	return nil
}

// DeleteAccountKV stages a tombstone for owner's (domain,key).
func (s *StateDB) DeleteAccountKV(owner tcommon.Address, domain kvdomains.KVDomain, key []byte) error {
	if !kvdomains.IsRegistered(domain) {
		return fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	obj := s.getStateObject(owner)
	if obj == nil {
		return nil
	}
	mk := string(kvCompositeKey(domain, key))
	prev, had := obj.kvDirty[mk]
	s.journal.append(kvChange{address: owner, mapKey: mk, hadEntry: had, prevEntry: prev})
	obj.kvDirty[mk] = kvEntry{val: nil, deleted: true}
	obj.markDirty()
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./core/state/ -run TestAccountKV -v`
Expected: PASS (all five).

- [ ] **Step 5: Commit**

```bash
git add core/state/account_kv.go core/state/state_object.go core/state/journal.go core/state/account_kv_test.go
git commit -m "feat(state): add in-memory generic account-KV layer"
```

---

### Task 2: Commit the account-KV trie into the full state root

**Files:**
- Modify: `core/state/account_kv.go` (add `commitAccountKV`)
- Modify: `core/state/statedb.go` (call it in the `Commit()` loop, before the envelope is built)
- Test: append to `core/state/account_kv_test.go`

- [ ] **Step 1: Write the failing test (the anchor)** — append:

```go
func TestAccountKVRootMovesAndPersists(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x11)
	sdb.CreateAccount(addr, corepbAccountNormal())
	root0, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit0: %v", err)
	}
	if err := sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}
	root1, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit1: %v", err)
	}
	if root1 == root0 {
		t.Fatal("KV write did not move the full state root")
	}
	reopened, err := New(root1, sdb.db)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if g, ok, _ := reopened.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k")); !ok || string(g) != "v" {
		t.Fatalf("persisted get = %q,%v, want v,true", g, ok)
	}
}

func TestAccountKVDeterministicRoot(t *testing.T) {
	build := func() tcommon.Hash {
		sdb := newTestStateDB(t)
		addr := testAddr(0x22)
		sdb.CreateAccount(addr, corepbAccountNormal())
		_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("a"), []byte("1"))
		_ = sdb.SetAccountKV(addr, kvdomains.ContractStorage, []byte("b"), []byte("2"))
		_ = sdb.SetAccountKV(addr, kvdomains.SystemProposal, []byte("c"), []byte("3"))
		r, err := sdb.Commit()
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
		return r
	}
	if build() != build() {
		t.Fatal("KV commit is non-deterministic")
	}
}

func TestAccountKVEmptyValueDistinctFromDeleted(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x33)
	sdb.CreateAccount(addr, corepbAccountNormal())
	_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("empty"), []byte{})
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	reopened, _ := New(root, sdb.db)
	v, ok, _ := reopened.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("empty"))
	if !ok || len(v) != 0 {
		t.Fatalf("empty-but-present value lost: v=%q ok=%v", v, ok)
	}
}

func TestBalanceOnlyAccountKeepsEmptyKVRoot(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x44)
	sdb.CreateAccount(addr, corepbAccountNormal())
	sdb.AddBalance(addr, 5)
	if _, err := sdb.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	obj := sdb.getStateObject(addr)
	if obj.accountKVRoot != EmptyKVRoot {
		t.Fatalf("balance-only account got non-empty KV root %x", obj.accountKVRoot)
	}
}
```

> Add a tiny helper near the top of the test file if not already present:
> ```go
> func corepbAccountNormal() corepb.AccountType { return corepb.AccountType_Normal }
> ```
> and import `corepb "github.com/tronprotocol/go-tron/proto/core"`. (Or inline `corepb.AccountType_Normal` at each `CreateAccount` call — match the existing test style in `core/state`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./core/state/ -run "TestAccountKVRootMoves|TestAccountKVDeterministic|TestAccountKVEmptyValue|TestBalanceOnlyAccount" -v`
Expected: FAIL — `root1 == root0` (KV writes not yet committed to a trie / not in the envelope).

- [ ] **Step 3a: Add `commitAccountKV`** (`core/state/account_kv.go`)

```go
// commitAccountKV applies obj's dirty KV overlay to its KV trie, persists the
// trie nodes, and returns the new AccountKVRoot. Call only when len(obj.kvDirty) > 0.
func (s *StateDB) commitAccountKV(obj *stateObject) (tcommon.Hash, error) {
	base := ethcommon.Hash(obj.accountKVRoot)
	tr, err := s.db.OpenTrie(base)
	if err != nil {
		return tcommon.Hash{}, err
	}
	for mk, e := range obj.kvDirty {
		tk := kvTrieKey([]byte(mk))
		if e.deleted {
			if err := tr.Delete(tk); err != nil {
				return tcommon.Hash{}, err
			}
			continue
		}
		wrapped := make([]byte, 1+len(e.val))
		wrapped[0] = kvPresencePrefix
		copy(wrapped[1:], e.val)
		if err := tr.Update(tk, wrapped); err != nil {
			return tcommon.Hash{}, err
		}
	}
	root, nodes := tr.Commit(false)
	if nodes != nil {
		if err := s.db.TrieDB().Update(root, base, 0, trienode.NewWithNodeSet(nodes), nil); err != nil {
			return tcommon.Hash{}, err
		}
		if err := s.db.TrieDB().Commit(root, false); err != nil {
			return tcommon.Hash{}, err
		}
	}
	return tcommon.Hash(root), nil
}
```

Add `"github.com/ethereum/go-ethereum/trie/trienode"` to `account_kv.go` imports.

- [ ] **Step 3b: Hook it into `Commit()`** (`core/state/statedb.go`)

In the per-dirty-account loop, immediately **before** `accBytes, err := obj.account.Marshal()` (the envelope build), insert:

```go
		if len(obj.kvDirty) > 0 {
			kvRoot, err := s.commitAccountKV(obj)
			if err != nil {
				return tcommon.Hash{}, err
			}
			obj.accountKVRoot = kvRoot
			obj.kvDirty = make(map[string]kvEntry)
		}
```

(The envelope already reads `AccountKVRoot: obj.accountKVRoot`, so it now picks up the freshly committed root. Balance-only accounts have empty `kvDirty` and skip this entirely.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./core/state/ -run "TestAccountKV|TestBalanceOnlyAccount" -v && go test ./core/state/ -count=1`
Expected: PASS. Full `core/state` package green (no root-literal tests exist there).

- [ ] **Step 5: Commit**

```bash
git add core/state/account_kv.go core/state/statedb.go core/state/account_kv_test.go
git commit -m "feat(state): commit account-KV trie into the full state root"
```

---

### Task 3: `ResetAccountKV` + generation, with revert that restores the overlay

**Files:**
- Modify: `core/state/account_kv.go` (add `ResetAccountKV`)
- Modify: `core/state/journal.go` (add `kvResetChange`)
- Test: append to `core/state/account_kv_test.go`

- [ ] **Step 1: Write the failing test** — append:

```go
func TestResetAccountKVBumpsGenerationAndEmptiesRoot(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x55)
	sdb.CreateAccount(addr, corepbAccountNormal())
	_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k"), []byte("v"))
	if _, err := sdb.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := sdb.ResetAccountKV(addr); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if _, err := sdb.Commit(); err != nil {
		t.Fatalf("commit2: %v", err)
	}
	obj := sdb.getStateObject(addr)
	if obj.accountKVRoot != EmptyKVRoot {
		t.Fatalf("KV root after reset = %x, want EmptyKVRoot", obj.accountKVRoot)
	}
	if obj.accountKVGeneration != 1 {
		t.Fatalf("generation after reset = %d, want 1", obj.accountKVGeneration)
	}
	if _, ok, _ := sdb.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k")); ok {
		t.Fatal("key should be unreachable after reset+commit")
	}
}

func TestResetAccountKVRevertRestoresOverlay(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x66)
	sdb.CreateAccount(addr, corepbAccountNormal())
	_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k"), []byte("orig"))
	snap := sdb.Snapshot()
	_ = sdb.ResetAccountKV(addr)
	_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k2"), []byte("new"))
	sdb.RevertToSnapshot(snap)
	if g, ok, _ := sdb.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k")); !ok || string(g) != "orig" {
		t.Fatalf("k after revert-past-reset = %q,%v, want orig,true", g, ok)
	}
	if obj := sdb.getStateObject(addr); obj.accountKVGeneration != 0 {
		t.Fatalf("generation after revert = %d, want 0", obj.accountKVGeneration)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./core/state/ -run TestResetAccountKV -v`
Expected: FAIL — `ResetAccountKV` undefined.

- [ ] **Step 3a: Add `kvResetChange`** (`core/state/journal.go`)

```go
// kvResetChange records a generic-KV reset (generation bump) for revert. It
// snapshots the prior root, generation, AND the dirty overlay, because the
// reset clears the overlay and the post-reset overlay belongs to a new generation.
type kvResetChange struct {
	address        tcommon.Address
	prevRoot       tcommon.Hash
	prevGeneration uint64
	prevDirty      map[string]kvEntry
}

func (e kvResetChange) revert(stateObjects map[tcommon.Address]*stateObject, _ map[tcommon.Address]*types.Witness) {
	obj := stateObjects[e.address]
	if obj == nil {
		return
	}
	obj.accountKVRoot = e.prevRoot
	obj.accountKVGeneration = e.prevGeneration
	obj.kvDirty = e.prevDirty
}
```

- [ ] **Step 3b: Add `ResetAccountKV`** (`core/state/account_kv.go`)

```go
// ResetAccountKV discards owner's entire generic-KV namespace: the KV root is
// reset to empty and the generation is bumped. Old keys become unreachable from
// the new generation without an O(N) prefix delete (Erigon-incarnation style).
func (s *StateDB) ResetAccountKV(owner tcommon.Address) error {
	obj := s.getStateObject(owner)
	if obj == nil {
		return nil
	}
	prevDirty := make(map[string]kvEntry, len(obj.kvDirty))
	for k, v := range obj.kvDirty {
		prevDirty[k] = v
	}
	s.journal.append(kvResetChange{
		address:        owner,
		prevRoot:       obj.accountKVRoot,
		prevGeneration: obj.accountKVGeneration,
		prevDirty:      prevDirty,
	})
	obj.kvDirty = make(map[string]kvEntry)
	obj.accountKVRoot = EmptyKVRoot
	obj.accountKVGeneration++
	obj.markDirty()
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./core/state/ -run TestResetAccountKV -v && go test ./core/state/ -count=1`
Expected: PASS; full `core/state` green.

- [ ] **Step 5: Commit**

```bash
git add core/state/account_kv.go core/state/journal.go core/state/account_kv_test.go
git commit -m "feat(state): add ResetAccountKV with generation bump"
```

---

### Task 4: Full verification

- [ ] **Step 1: Build + full suite**

Run:
```bash
go build ./...
go test ./... -count=1 -timeout 300s
```
Expected: build clean; suite green. **Known pre-existing flake:** `TestBlockChainInsertBlock_MaintenanceFiresOncePerBoundary` (~7%, unrelated to this work). If it red-fails, rerun that package; it is NOT a Phase 2 regression. Any OTHER failure is real — investigate.

- [ ] **Step 2: Confirm Phase 1 + java root still intact**

Run:
```bash
go test ./core/state/ ./common/ ./core/ ./core/types/ -count=1
go test ./core/ ./core/types/ -run "AccountStateRoot" -count=1 -v
```
Expected: PASS; java `accountStateRoot` tests unchanged (Phase 2 never touches `account_state_root.go`).

- [ ] **Step 3: vet + fmt (golangci-lint substitute; lint not installed)**

Run:
```bash
go vet ./core/state/ ./common/
gofmt -l core/state/account_kv.go core/state/account_kv_test.go core/state/state_object.go core/state/journal.go core/state/statedb.go
```
Expected: vet clean; `gofmt -l` prints nothing.

- [ ] **Step 4: Commit any fixups** (only if Steps 1-3 required edits; otherwise no commit)

```bash
git add -u && git commit -m "test(state): verify Phase 2 generic account-KV"
```

---

## Self-Review (completed during planning)

- **Spec coverage (Phase 2 bullets):** per-account KV tries → Tasks 1-2; StateDB get/set/delete/reset → Tasks 1,3; iterate → **deferred to Phase 3** (documented, with the hashed-key constraint); snapshot/journal coverage of KV writes → Task 1 (`kvChange`) + Task 3 (`kvResetChange`); KV-root commit before account-trie commit → Task 2 (hook precedes envelope build, which precedes `s.trie.Commit`); generation increment/reset → Task 3.
- **Advisor subtleties:** hashed-key/iteration constraint (scoping section + deferred note); Reset journals root+gen+overlay (Task 3 `kvResetChange` + revert test); conditional KV commit (Task 2 `if len(obj.kvDirty) > 0`); anchor/determinism tests (Task 2 `TestAccountKVRootMovesAndPersists` + `TestAccountKVDeterministicRoot`).
- **Empty-vs-deleted:** presence-byte wrapper (Task 2) + `TestAccountKVEmptyValueDistinctFromDeleted`.
- **Type consistency:** `kvEntry{val, deleted}`, `kvDirty map[string]kvEntry`, `kvCompositeKey`/`kvTrieKey`, `kvChange`/`kvResetChange`, `commitAccountKV`, `GetAccountKV`/`SetAccountKV`/`DeleteAccountKV`/`ResetAccountKV` consistent across tasks.
- **Placeholder scan:** every code step has real code; the only judgment call is the `corepbAccountNormal()` test helper vs inlining `corepb.AccountType_Normal` — instruction says match existing `core/state` test style.

## Out of scope (later phases)

- **Phase 3:** reserved `SystemAccountID`; move dynamic properties + witness schedule behind system-account KV; the dynprop-load iteration path (NodeIterator+filter or enumerate-known-names — NOT a hashed-MPT domain-prefix iterator).
- **Phase 4:** contract storage/code/metadata + witness capsules into KV/code domains. Note: `setCode` will need to also update the source `CodeHash` once the envelope `CodeHash` becomes authoritative.
- **Phase 7:** flat physical latest-state index (`state-kv-latest-v2`), pruning of old generations, history/change-sets, snapshot files.

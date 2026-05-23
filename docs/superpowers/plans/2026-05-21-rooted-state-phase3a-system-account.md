# Rooted State — Phase 3a: Reserved System Account Foundation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.
>
> **Source spec:** `/Users/asuka/Projects/asuka/go/go-tron/docs/superpowers/specs/2026-05-21-rooted-generic-state-kv-design.md` (main repo). Builds on Phase 1 (`43edf91`) + Phase 2 (`8efe2bc`, generic account-KV merged to master).
>
> **Commits:** GPG-signed (never `--no-gpg-sign`). Squash to feat+test before merge. Don't `git add` plan files.

**Goal:** Stand up the reserved system account that will own chain-global rooted state in Phases 3b/3c — a fixed 20-byte `SystemAccountID`, created at genesis, with a thin `StateDB` system-KV facade. No chain-global data moves yet (that's 3b dynprops, 3c witness schedule).

**Architecture:** `SystemAccountID = 0xff..fe` (20 bytes) lives in `common`, with a canonical internal `SystemAccountAddress` (`0x41`-prefixed; the prefix is cosmetic since the account trie keys by `AccountID`). Genesis creates the account so it exists to own KV. A `StateDB` facade (`SystemKVGet/Put/Delete`) wraps the Phase-2 `GetAccountKV/SetAccountKV/DeleteAccountKV` with `SystemAccountAddress` as owner.

**Tech Stack:** Go 1.25; `common`, `core/state`, `core/state/kvdomains`, `core/genesis.go`.

## Decisions / scope

- `SystemAccountID` = 20 bytes, all `0xff` except the last byte `0xfe` (per design: `0xff..fe`).
- The system account's address prefix is **cosmetic** — the rooted state keys by `AccountID` (Phase 1). Use one canonical `SystemAccountAddress` (`0x41`-prefixed) everywhere internally to avoid the address-aliasing trap (two `Address`es with the same `AccountID` but different prefix would alias to one trie key but two `stateObjects` cache entries).
- **Validation guard deferred (noted):** the design says reject user mutation of the system account. In practice no transaction actuator can write the system account's KV (only internal code calls `SetAccountKV`), and a balance transfer to it is harmless (nothing reads its balance for consensus). So the actuator-layer guard is deferred to Phase 5 (store audit) / when load-bearing, rather than speculatively touching every actuator now.
- Genesis creating the system account **changes the internal genesis state root** (a new account leaf). Per Phase 1/2 findings, no test asserts the internal genesis root. It does **not** affect `BlockHeader.raw.accountStateRoot` (genesis has no java root on its header; the system account has no balance/allowance so it never enters a `JavaAccountStateRoot` touched-set).

---

### Task 1: `common` system account identity

**Files:**
- Create: `common/system_account.go`
- Test: `common/system_account_test.go`

- [ ] **Step 1: Write the failing test** — `common/system_account_test.go`:

```go
package common

import (
	"bytes"
	"testing"
)

func TestSystemAccountID(t *testing.T) {
	id := SystemAccountID
	for i := 0; i < AccountIDLength-1; i++ {
		if id[i] != 0xff {
			t.Fatalf("byte %d = %#x, want 0xff", i, id[i])
		}
	}
	if id[AccountIDLength-1] != 0xfe {
		t.Fatalf("last byte = %#x, want 0xfe", id[AccountIDLength-1])
	}
}

func TestSystemAccountAddress(t *testing.T) {
	addr := SystemAccountAddress
	if addr[0] != AddressPrefixMainnet {
		t.Fatalf("prefix = %#x, want 0x41", addr[0])
	}
	if !bytes.Equal(addr.AccountID().Bytes(), SystemAccountID.Bytes()) {
		t.Fatal("SystemAccountAddress.AccountID() must equal SystemAccountID")
	}
}

func TestIsSystemAccount(t *testing.T) {
	if !IsSystemAccount(SystemAccountAddress) {
		t.Fatal("SystemAccountAddress must be a system account")
	}
	// Same AccountID under the testnet prefix is still the system account (keying ignores prefix).
	if !IsSystemAccount(SystemAccountID.Address(AddressPrefixTestnet)) {
		t.Fatal("system account must be recognized regardless of network prefix")
	}
	var other Address
	other[0] = AddressPrefixMainnet
	other[1] = 0x11
	if IsSystemAccount(other) {
		t.Fatal("ordinary address must not be a system account")
	}
}
```

- [ ] **Step 2: Run, expect FAIL** — `go test ./common/ -run "System|IsSystem" -v` (undefined identifiers).

- [ ] **Step 3: Implement** — `common/system_account.go`:

```go
package common

// SystemAccountID is the reserved 20-byte owner of chain-global rooted state
// (dynamic properties, witness schedule, and other consensus-global records in
// later phases). It is a synthetic internal identity, never a user address.
var SystemAccountID = func() AccountID {
	var id AccountID
	for i := range id {
		id[i] = 0xff
	}
	id[AccountIDLength-1] = 0xfe
	return id
}()

// SystemAccountAddress is the canonical internal Address for the system account.
// The network prefix is cosmetic — rooted state keys by AccountID — but a single
// canonical Address must be used everywhere to avoid stateObject cache aliasing.
var SystemAccountAddress = SystemAccountID.Address(AddressPrefixMainnet)

// IsSystemAccount reports whether addr is the reserved system account, by
// AccountID (ignoring the network prefix).
func IsSystemAccount(addr Address) bool {
	return addr.AccountID() == SystemAccountID
}
```

- [ ] **Step 4: Run, expect PASS** — `go test ./common/ -run "System|IsSystem" -v && go test ./common/ -count=1`.

- [ ] **Step 5: Commit**

```bash
git add common/system_account.go common/system_account_test.go
git commit -m "feat(common): add reserved SystemAccountID identity"
```

---

### Task 2: System-KV facade + genesis creation

**Files:**
- Create: `core/state/system_store.go`
- Modify: `core/genesis.go` (`genesisBlockAndStateRoot`, before `statedb.Commit()`)
- Test: `core/state/system_store_test.go`; append to `core/genesis_test.go`

- [ ] **Step 1: Write the failing tests.**

`core/state/system_store_test.go`:
```go
package state

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestSystemKVRoundTrip(t *testing.T) {
	sdb := newTestStateDB(t)
	if err := sdb.SystemKVPut(kvdomains.SystemDynamicProperty, []byte("p"), []byte("42")); err != nil {
		t.Fatalf("put: %v", err)
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	v, ok, err := reopened.SystemKVGet(kvdomains.SystemDynamicProperty, []byte("p"))
	if err != nil || !ok || string(v) != "42" {
		t.Fatalf("get = %q,%v,%v want 42,true,nil", v, ok, err)
	}
	// The value lives under the system account specifically.
	if !sdb.AccountExists(tcommon.SystemAccountAddress) {
		t.Fatal("system account should exist after a system KV write")
	}
}
```

Append to `core/genesis_test.go` (it already imports what genesis tests need; add `state` and `tcommon` imports if absent):
```go
func TestGenesisCreatesSystemAccount(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	genesis := defaultTestGenesis() // reuse whatever helper existing genesis tests use; otherwise build a minimal params.Genesis like other tests in this file
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	sdb := state.NewDatabase(diskdb)
	st, err := state.New(rawdb.ReadGenesisStateRoot(diskdb), sdb)
	if err != nil {
		t.Fatal(err)
	}
	if !st.AccountExists(tcommon.SystemAccountAddress) {
		t.Fatal("genesis must create the reserved system account")
	}
}
```
> If `defaultTestGenesis`/`SetupGenesisBlock` differ from existing genesis-test usage, match the exact pattern already in `core/genesis_test.go` (read it first). The assertion is the point: the system account exists after genesis.

- [ ] **Step 2: Run, expect FAIL** — `go test ./core/state/ -run TestSystemKV -v` (SystemKVPut undefined); `go test ./core/ -run TestGenesisCreatesSystemAccount -v` (system account absent).

- [ ] **Step 3a: Implement the facade** — `core/state/system_store.go`:

```go
package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// SystemKVGet reads a chain-global value owned by the reserved system account.
func (s *StateDB) SystemKVGet(domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	return s.GetAccountKV(tcommon.SystemAccountAddress, domain, key)
}

// SystemKVPut writes a chain-global value owned by the reserved system account.
func (s *StateDB) SystemKVPut(domain kvdomains.KVDomain, key, value []byte) error {
	return s.SetAccountKV(tcommon.SystemAccountAddress, domain, key, value)
}

// SystemKVDelete removes a chain-global value owned by the reserved system account.
func (s *StateDB) SystemKVDelete(domain kvdomains.KVDomain, key []byte) error {
	return s.DeleteAccountKV(tcommon.SystemAccountAddress, domain, key)
}
```

- [ ] **Step 3b: Create the system account at genesis** — in `core/genesis.go` `genesisBlockAndStateRoot`, immediately before `stateRoot, err := statedb.Commit()` (line ~235), add:

```go
	// Reserved system account: owner of chain-global rooted state (dynamic
	// properties, witness schedule, etc. in later phases). Created here so it
	// exists to own KV; it carries no balance and is never a user address.
	statedb.GetOrCreateAccount(tcommon.SystemAccountAddress)
```
(Confirm `tcommon` is the import alias in genesis.go; it is — `genesis.go` already uses `tcommon.Hash`.)

- [ ] **Step 4: Run, expect PASS** — `go test ./core/state/ -run TestSystemKV -v && go test ./core/ -run TestGenesisCreatesSystemAccount -v`. Then `go test ./core/state/ ./core/ -count=1` (mind the known maintenance flake — rerun if it alone fails).

- [ ] **Step 5: Commit**

```bash
git add core/state/system_store.go core/genesis.go core/state/system_store_test.go core/genesis_test.go
git commit -m "feat(state): create reserved system account at genesis with KV facade"
```

---

### Task 3: Full verification

- [ ] **Step 1: build + suite**
```bash
go build ./...
go test ./... -count=1 -timeout 300s
```
Expected: green. Known flake `TestBlockChainInsertBlock_MaintenanceFiresOncePerBoundary` (~7%) — rerun its package if it alone fails; not a regression.

- [ ] **Step 2: java root unaffected**
```bash
go test ./core/ ./core/types/ -run "AccountStateRoot" -count=1 -v
```
Expected: PASS, no fixture edits (the system account has no balance/allowance, so it never enters a `JavaAccountStateRoot` touched-set; `account_state_root.go` untouched).

- [ ] **Step 3: vet + fmt**
```bash
go vet ./common/ ./core/state/ ./core/
gofmt -l common/system_account.go common/system_account_test.go core/state/system_store.go core/state/system_store_test.go core/genesis.go core/genesis_test.go
```
Expected: vet clean; `gofmt -l` empty.

## Self-Review

- Spec coverage: reserved system account created at genesis (Task 2); identity (Task 1); facade for later typed stores (Task 2). Validation rule explicitly deferred with rationale.
- No placeholders except the genesis-test helper, which says "match existing pattern in `core/genesis_test.go`" with the assertion pinned.
- Types consistent: `SystemAccountID`, `SystemAccountAddress`, `IsSystemAccount`, `SystemKVGet/Put/Delete`.

## Out of scope

- **Phase 3b:** move the ~129 consensus dynprops into `SystemDynamicProperty` KV (flushed pre-Commit); keep the 4 head-pointer keys derived/unrooted. Genesis dynprops flush splits. Determinism + genesis-reproducibility tests required.
- **Phase 3c:** witness-schedule / maintenance state into `SystemWitnessSchedule` KV.
- Validation guard (reject user mutation of the system account) → Phase 5 / when load-bearing.

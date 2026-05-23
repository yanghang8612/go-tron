# Rooted State — Phase 1: Root Envelope, AccountID, Domain Registry — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **Source spec:** `/Users/asuka/Projects/asuka/go/go-tron/docs/superpowers/specs/2026-05-21-rooted-generic-state-kv-design.md` (read it before starting; it is NOT in this worktree).
>
> **Commits:** GPG signing is mandatory (key already configured in git). Never pass `--no-gpg-sign`. Per-task commits below are intermediate; the human squashes to 1–2 commits with `git reset --soft <base>` before opening the PR. Do not `git add` this plan file.

**Goal:** Switch the account trie to store a versioned internal `StateAccountV2` RLP envelope (carrying `AccountKVRoot` + `AccountKVGeneration` + `CodeHash` alongside the account proto), key the trie by the normalized 20-byte `AccountID`, and stand up the central `KVDomain` registry — laying the foundation for a single rooted full-state model on a fresh database.

**Architecture:** The account trie value changes from raw `proto.Marshal(corepb.Account)` to `rlp(StateAccountV2{version, accountProto, accountKVRoot, accountKVGeneration, codeHash})`. Trie keys change from `Keccak256(addr21)` to `Keccak256(accountID20)`. Phase 1 adds the fields/plumbing only — `AccountKVRoot` is always the empty root and `AccountKVGeneration` is always 0 (no per-account KV trie exists until Phase 2). The java-tron lightweight account-state root (`account_state_root.go`) is a **separate** trie and is left completely untouched.

**Tech Stack:** Go 1.25; go-ethereum `rlp` + `crypto` + `trie`/`triedb` primitives; `corepb.Account` (java-tron proto, unchanged); Pebble via existing `state.Database`.

**Fresh-database only.** No migration, no legacy-value fallback. Decoding a non-V2 trie value is an error path.

---

## Scope guardrails (read once)

- **Do NOT touch** `core/state/account_state_root.go` (`JavaAccountStateRoot`). It builds its own trie keyed by `RLP(addr.Bytes())` with a minimal `corepb.Account{Address,Balance,Allowance}` value. It is the java-tron header field and must stay byte-identical. Its root is independent of the internal full-state root we are changing.
- **Do NOT add** new `core/rawdb/schema.go` prefixes in Phase 1. Account-trie nodes are stored by go-ethereum `triedb`, not under a named prefix. The `state-kv-latest-v2` / `state-code-v2` prefixes belong to Phases 2 and 4.
- **Do NOT migrate** contract code / storage / metadata physical location. Code still loads from rawdb `c-<addr>`; storage from `s-<addr>`. Phase 1 writes `CodeHash` as the **zero hash** in the envelope (it is neither populated nor consumed yet); it becomes authoritative in Phase 4 with the content-addressed code domain. Note: go-tron never writes `corepb.Account.code_hash` (verified: no setter in non-test code), so `GetCodeHash()` would yield zero anyway — we zero it explicitly to signal intent rather than ship a "looks populated" value Phase 4 can't trust.
- `corepb.Account` proto is **not** modified.
- **Block hashes MUST NOT change.** The internal full-state root lives out-of-band in `bsr-<blockhash>` (and `genesis-state-root`); it is not in the block header. `BlockHeader.raw.accountStateRoot` is the *java* root from the untouched `JavaAccountStateRoot`. So: a stable block hash is **not** evidence Phase 1 did nothing, and a **moved block hash IS a regression** — stop and investigate if any block-hash fixture changes.

## Facts established during planning (do not re-derive)

- Test helpers already exist in `core/state/statedb_test.go`: `newTestStateDB(t) *StateDB` (line 16; backed by `ethrawdb.NewMemoryDatabase()`) and `testAddr(b byte) tcommon.Address` (line 27; sets `addr[0]=0x41`, `addr[20]=b`). Reopen at a committed root in-package via `New(root, sdb.db)`.
- Java account-state-root test coverage is exactly three files: `core/genesis_test.go`, `core/block_builder_test.go`, `core/types/block_test.go`. This is the load-bearing parity coverage — none of these `JavaAccountStateRoot` assertions may be edited in Task 8.

- Account trie **write**: `core/state/statedb.go:1656-1662` (`obj.account.Marshal()` → `s.trie.Update(trieKey(addr), data)`), inside `Commit()`.
- Account trie **read** (only decode site): `core/state/statedb.go:1932-1942` (`getStateObject`: `s.trie.Get(trieKey(addr))` → `types.UnmarshalAccount(data)`).
- `trieKey`: `core/state/statedb.go:1973-1976` — `crypto.Keccak256(addr.Bytes())`. Single chokepoint; 4 references all in statedb.go.
- `CopyAccount` deep-copy path: `core/state/statedb.go:1603-1626` re-marshals `obj.account` directly (NOT the trie value) — unaffected by the envelope change, but must also copy the two new fields.
- History capture is **unaffected**: `AccountDelta.AccountProtoPre` is sourced from journal `accountChange.prev` = `obj.account.Marshal()` (proto bytes), see `core/state/statedb.go:1946-1958` and `core/state/history.go:394`. It serializes the account proto, never the trie value.
- `common.Address` = `[21]byte`; `AddressPrefixMainnet=0x41`, `AddressPrefixTestnet=0xa0` (`common/address.go`).
- `corepb.Account` has `GetCodeHash() []byte` (`proto/core/Tron.pb.go:2127`).
- `tcommon.Hash` = `[32]byte` (`common/hash.go:12`), rlp-encodable as a 32-byte string.
- `ethtypes.EmptyRootHash` is the empty-trie root constant (already used at `core/genesis.go:193`).
- Genesis state root is produced by `statedb.Commit()` in `genesisBlockAndStateRoot` (`core/genesis.go:235`); persisted via `rawdb.WriteGenesisStateRoot` (`core/genesis.go:98`). Phase 1 changes this root's **value** (not its meaning). Tests asserting it must be updated in lockstep (Task 8).

---

### Task 1: `KVDomain` registry package

**Files:**
- Create: `core/state/kvdomains/domains.go`
- Test: `core/state/kvdomains/domains_test.go`

- [ ] **Step 1: Write the failing test**

```go
// core/state/kvdomains/domains_test.go
package kvdomains

import "testing"

func TestRegisteredDomains(t *testing.T) {
	registered := []KVDomain{
		SystemDynamicProperty, SystemWitnessSchedule, SystemProposal,
		SystemForkVote, SystemAsset, SystemExchange, SystemDelegation,
		SystemAccountIndex, ContractStorage, ContractMetadata, ContractABI,
		ContractRuntimeState, AccountLocalIndex, AccountPermissionAux,
		WitnessCapsule, WitnessVoteState,
	}
	for _, d := range registered {
		if !IsRegistered(d) {
			t.Fatalf("domain %#04x should be registered", uint16(d))
		}
		if Name(d) == "" {
			t.Fatalf("domain %#04x should have a name", uint16(d))
		}
	}
}

func TestUnregisteredDomain(t *testing.T) {
	if IsRegistered(KVDomain(0x0099)) {
		t.Fatal("0x0099 must not be registered")
	}
	if IsRegistered(KVDomain(0xABCD)) {
		t.Fatal("0xABCD must not be registered")
	}
}

func TestNoDuplicateIDsOrNames(t *testing.T) {
	seenName := map[string]bool{}
	for d, name := range registry {
		if seenName[name] {
			t.Fatalf("duplicate domain name %q", name)
		}
		seenName[name] = true
		_ = d
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./core/state/kvdomains/ -run TestRegisteredDomains -v`
Expected: FAIL — package/identifiers undefined.

- [ ] **Step 3: Write minimal implementation**

```go
// core/state/kvdomains/domains.go

// Package kvdomains is the central registry of generic account-KV domain IDs.
// Every consensus-relevant mutable record in the rooted state model is addressed
// by (owner AccountID, domain KVDomain, logical key). Domain IDs MUST be
// registered here; no raw domain constants may be scattered through actuators.
package kvdomains

// KVDomain identifies a logical namespace within an account's generic KV space.
type KVDomain uint16

// Domain groups (see design spec "Generic Account KV"):
//
//	0x0001-0x00ff system/global   0x0100-0x01ff contract
//	0x0200-0x02ff account-local   0x0300-0x03ff witness
//	0x0400-0x04ff governance      0x8000-0xffff test/private/reserved
const (
	SystemDynamicProperty KVDomain = 0x0001
	SystemWitnessSchedule KVDomain = 0x0002
	SystemProposal        KVDomain = 0x0003
	SystemForkVote        KVDomain = 0x0004
	SystemAsset           KVDomain = 0x0005
	SystemExchange        KVDomain = 0x0006
	SystemDelegation      KVDomain = 0x0007
	SystemAccountIndex    KVDomain = 0x0008

	ContractStorage      KVDomain = 0x0100
	ContractMetadata     KVDomain = 0x0101
	ContractABI          KVDomain = 0x0102
	ContractRuntimeState KVDomain = 0x0103

	AccountLocalIndex    KVDomain = 0x0200
	AccountPermissionAux KVDomain = 0x0201

	WitnessCapsule   KVDomain = 0x0300
	WitnessVoteState KVDomain = 0x0301
)

var registry = map[KVDomain]string{
	SystemDynamicProperty: "SystemDynamicProperty",
	SystemWitnessSchedule: "SystemWitnessSchedule",
	SystemProposal:        "SystemProposal",
	SystemForkVote:        "SystemForkVote",
	SystemAsset:           "SystemAsset",
	SystemExchange:        "SystemExchange",
	SystemDelegation:      "SystemDelegation",
	SystemAccountIndex:    "SystemAccountIndex",
	ContractStorage:       "ContractStorage",
	ContractMetadata:      "ContractMetadata",
	ContractABI:           "ContractABI",
	ContractRuntimeState:  "ContractRuntimeState",
	AccountLocalIndex:     "AccountLocalIndex",
	AccountPermissionAux:  "AccountPermissionAux",
	WitnessCapsule:        "WitnessCapsule",
	WitnessVoteState:      "WitnessVoteState",
}

// IsRegistered reports whether d is a known domain.
func IsRegistered(d KVDomain) bool {
	_, ok := registry[d]
	return ok
}

// Name returns the registered name for d, or "" if unregistered.
func Name(d KVDomain) string {
	return registry[d]
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./core/state/kvdomains/ -v`
Expected: PASS (all three tests).

- [ ] **Step 5: Commit**

```bash
git add core/state/kvdomains/domains.go core/state/kvdomains/domains_test.go
git commit -m "feat(state): add central KVDomain registry"
```

---

### Task 2: `AccountID` type + 21→20 byte normalization

**Files:**
- Modify: `common/address.go`
- Test: `common/address_test.go` (append; create if absent)

- [ ] **Step 1: Write the failing test**

```go
// common/address_test.go  (add these tests)
package common

import (
	"bytes"
	"testing"
)

func TestAddressAccountID(t *testing.T) {
	var addr Address
	addr[0] = AddressPrefixMainnet
	for i := 1; i < AddressLength; i++ {
		addr[i] = byte(i)
	}
	id := addr.AccountID()
	if len(id.Bytes()) != AccountIDLength {
		t.Fatalf("AccountID len = %d, want %d", len(id.Bytes()), AccountIDLength)
	}
	if !bytes.Equal(id.Bytes(), addr[1:]) {
		t.Fatalf("AccountID = %x, want %x", id.Bytes(), addr[1:])
	}
}

func TestAccountIDRoundTrip(t *testing.T) {
	var addr Address
	addr[0] = AddressPrefixMainnet
	for i := 1; i < AddressLength; i++ {
		addr[i] = byte(0xF0 + i)
	}
	got := addr.AccountID().Address(AddressPrefixMainnet)
	if got != addr {
		t.Fatalf("round-trip = %x, want %x", got.Bytes(), addr.Bytes())
	}
}

func TestAccountIDIgnoresPrefix(t *testing.T) {
	// Same 20-byte identity, different network prefix -> identical AccountID.
	var a, b Address
	a[0], b[0] = AddressPrefixMainnet, AddressPrefixTestnet
	for i := 1; i < AddressLength; i++ {
		a[i], b[i] = byte(i), byte(i)
	}
	if a.AccountID() != b.AccountID() {
		t.Fatal("AccountID must ignore the network prefix byte")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./common/ -run TestAccountID -v`
Expected: FAIL — `AccountID`, `AccountIDLength`, `addr.AccountID`, `id.Address` undefined.

- [ ] **Step 3: Write minimal implementation**

Append to `common/address.go`:

```go
// AccountIDLength is the rooted-state account identity size: a TRON address
// with its 1-byte network prefix (0x41 / 0xa0) stripped.
const AccountIDLength = AddressLength - 1

// AccountID is the 20-byte rooted-state owner identity. It matches the
// Solidity/TVM ABI address-word shape and is used for all internal rooted-state
// keying. Protocol boundaries keep the 21-byte Address; only the state layer
// normalizes to AccountID.
type AccountID [AccountIDLength]byte

// AccountID strips the network prefix byte. The caller is responsible for
// prefix validation at protocol boundaries; the state layer keys by identity.
func (a Address) AccountID() AccountID {
	var id AccountID
	copy(id[:], a[1:])
	return id
}

// ValidPrefix reports whether the address carries a known TRON network prefix.
func (a Address) ValidPrefix() bool {
	return a[0] == AddressPrefixMainnet || a[0] == AddressPrefixTestnet
}

func (id AccountID) Bytes() []byte { return id[:] }

// Address re-attaches a network prefix to produce a 21-byte TRON address.
func (id AccountID) Address(prefix byte) Address {
	var a Address
	a[0] = prefix
	copy(a[1:], id[:])
	return a
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./common/ -run TestAccountID -v && go test ./common/ -run TestAddressAccountID -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add common/address.go common/address_test.go
git commit -m "feat(common): add 20-byte AccountID identity derived from Address"
```

---

### Task 3: `StateAccountV2` RLP envelope

**Files:**
- Create: `core/state/state_account.go`
- Test: `core/state/state_account_test.go`

- [ ] **Step 1: Write the failing test**

```go
// core/state/state_account_test.go
package state

import (
	"bytes"
	"testing"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
)

func TestStateAccountV2RoundTrip(t *testing.T) {
	in := &StateAccountV2{
		Version:             StateAccountVersion,
		AccountProto:        []byte{0x0a, 0x15, 0x41, 0x01, 0x02},
		AccountKVRoot:       tcommon.BytesToHash([]byte{0xde, 0xad, 0xbe, 0xef}),
		AccountKVGeneration: 7,
		CodeHash:            tcommon.BytesToHash([]byte{0xca, 0xfe}),
	}
	enc, err := in.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := DecodeStateAccountV2(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Version != in.Version ||
		!bytes.Equal(out.AccountProto, in.AccountProto) ||
		out.AccountKVRoot != in.AccountKVRoot ||
		out.AccountKVGeneration != in.AccountKVGeneration ||
		out.CodeHash != in.CodeHash {
		t.Fatalf("round-trip mismatch:\n in=%+v\nout=%+v", in, out)
	}
}

func TestStateAccountV2Deterministic(t *testing.T) {
	v := &StateAccountV2{Version: StateAccountVersion, AccountProto: []byte{1, 2, 3}}
	a, _ := v.Encode()
	b, _ := v.Encode()
	if !bytes.Equal(a, b) {
		t.Fatal("encoding must be deterministic")
	}
}

func TestStateAccountV2RejectsWrongVersion(t *testing.T) {
	v := &StateAccountV2{Version: 99, AccountProto: []byte{1}}
	enc, _ := v.Encode()
	if _, err := DecodeStateAccountV2(enc); err == nil {
		t.Fatal("decode must reject unknown version")
	}
}

func TestEmptyKVRootIsEmptyTrieRoot(t *testing.T) {
	if EmptyKVRoot != tcommon.Hash(ethtypes.EmptyRootHash) {
		t.Fatal("EmptyKVRoot must equal the empty trie root")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./core/state/ -run TestStateAccountV2 -v`
Expected: FAIL — `StateAccountV2`, `StateAccountVersion`, `Encode`, `DecodeStateAccountV2`, `EmptyKVRoot` undefined.

- [ ] **Step 3: Write minimal implementation**

```go
// core/state/state_account.go
package state

import (
	"fmt"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	tcommon "github.com/tronprotocol/go-tron/common"
)

// StateAccountVersion is the only account-trie envelope version this build
// reads or writes. Fresh databases only; legacy raw-proto values are not
// supported.
const StateAccountVersion uint64 = 2

// EmptyKVRoot is the AccountKVRoot value for an account with no generic-KV
// entries (the empty trie root). Phase 1 always uses this.
var EmptyKVRoot = tcommon.Hash(ethtypes.EmptyRootHash)

// StateAccountV2 is the internal, versioned, RLP-encoded value stored in the
// account trie. It is deterministic and independent of java-tron protobuf
// definitions; it never leaks onto the wire, into blocks/transactions, or into
// RPC responses. The java-tron account serialization is unchanged and lives in
// AccountProto.
type StateAccountV2 struct {
	Version             uint64
	AccountProto        []byte
	AccountKVRoot       tcommon.Hash
	AccountKVGeneration uint64
	CodeHash            tcommon.Hash
}

// Encode serializes the envelope with RLP (deterministic, list-framed).
func (v *StateAccountV2) Encode() ([]byte, error) {
	return rlp.EncodeToBytes(v)
}

// DecodeStateAccountV2 parses an account-trie value and enforces the version.
func DecodeStateAccountV2(data []byte) (*StateAccountV2, error) {
	v := new(StateAccountV2)
	if err := rlp.DecodeBytes(data, v); err != nil {
		return nil, fmt.Errorf("decode StateAccountV2: %w", err)
	}
	if v.Version != StateAccountVersion {
		return nil, fmt.Errorf("unsupported StateAccountV2 version %d (want %d)", v.Version, StateAccountVersion)
	}
	return v, nil
}
```

> RLP encodes a struct as a list of its fields in declaration order; `tcommon.Hash` (`[32]byte`) encodes as a fixed 32-byte string — including the all-zero hash (`EmptyKVRoot` is the keccak of empty-trie, not zero; `CodeHash` *is* all-zero in Phase 1, which round-trips correctly). `uint64` encodes as a minimal big-endian integer. The round-trip + determinism tests above lock this in.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./core/state/ -run "TestStateAccountV2|TestEmptyKVRoot" -v`
Expected: PASS (all four tests).

- [ ] **Step 5: Commit**

```bash
git add core/state/state_account.go core/state/state_account_test.go
git commit -m "feat(state): add versioned StateAccountV2 RLP envelope"
```

---

### Task 4: Add KV fields to `stateObject` and thread through copy/construct

**Files:**
- Modify: `core/state/state_object.go:11-47`
- Modify: `core/state/statedb.go:1603-1626` (CopyAccount deep-copy)
- Test: `core/state/state_object_kvfields_test.go` (create)

- [ ] **Step 1: Write the failing test**

```go
// core/state/state_object_kvfields_test.go
package state

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestNewStateObjectDefaultsKVFields(t *testing.T) {
	var addr tcommon.Address
	addr[0] = tcommon.AddressPrefixMainnet
	obj := newStateObject(addr, types.NewAccount(addr, corepb.AccountType_Normal))
	if obj.accountKVRoot != EmptyKVRoot {
		t.Fatalf("accountKVRoot = %x, want EmptyKVRoot", obj.accountKVRoot)
	}
	if obj.accountKVGeneration != 0 {
		t.Fatalf("accountKVGeneration = %d, want 0", obj.accountKVGeneration)
	}
}

func TestNewEmptyStateObjectDefaultsKVFields(t *testing.T) {
	var addr tcommon.Address
	addr[0] = tcommon.AddressPrefixMainnet
	obj := newEmptyStateObject(addr)
	if obj.accountKVRoot != EmptyKVRoot {
		t.Fatalf("accountKVRoot = %x, want EmptyKVRoot", obj.accountKVRoot)
	}
	if obj.accountKVGeneration != 0 {
		t.Fatalf("accountKVGeneration = %d, want 0", obj.accountKVGeneration)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./core/state/ -run "KVFields" -v`
Expected: FAIL — `obj.accountKVRoot` / `obj.accountKVGeneration` undefined.

- [ ] **Step 3: Write minimal implementation**

In `core/state/state_object.go`, add two fields to the struct (after `selfDestructed`):

```go
	selfDestructed    bool

	// Rooted generic-KV fields (Phase 1: always EmptyKVRoot / 0; no KV trie yet).
	accountKVRoot       tcommon.Hash
	accountKVGeneration uint64
```

In `newStateObject`, initialize the root:

```go
func newStateObject(addr tcommon.Address, acc *types.Account) *stateObject {
	return &stateObject{
		address:       addr,
		account:       acc,
		storage:       make(map[tcommon.Hash]tcommon.Hash),
		storageExists: make(map[tcommon.Hash]bool),
		accountKVRoot: EmptyKVRoot,
	}
}
```

In `newEmptyStateObject`, initialize the root:

```go
func newEmptyStateObject(addr tcommon.Address) *stateObject {
	return &stateObject{
		address:       addr,
		account:       types.NewAccount(addr, corepb.AccountType_Normal),
		dirty:         true,
		created:       true,
		storage:       make(map[tcommon.Hash]tcommon.Hash),
		storageExists: make(map[tcommon.Hash]bool),
		accountKVRoot: EmptyKVRoot,
	}
}
```

In `core/state/statedb.go` `CopyAccount` deep-copy literal (the `newObj := &stateObject{...}` at ~1603), add the two fields:

```go
		newObj := &stateObject{
			address:             addr,
			dirty:               obj.dirty,
			deleted:             obj.deleted,
			created:             obj.created,
			code:                append([]byte{}, obj.code...),
			codeHash:            obj.codeHash,
			codeDirty:           obj.codeDirty,
			contractMeta:        metaCopy,
			contractMetaDirty:   obj.contractMetaDirty,
			storage:             make(map[tcommon.Hash]tcommon.Hash),
			storageExists:       make(map[tcommon.Hash]bool),
			selfDestructed:      obj.selfDestructed,
			accountKVRoot:       obj.accountKVRoot,
			accountKVGeneration: obj.accountKVGeneration,
		}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./core/state/ -run "KVFields" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add core/state/state_object.go core/state/statedb.go core/state/state_object_kvfields_test.go
git commit -m "feat(state): carry AccountKVRoot/Generation on stateObject"
```

---

### Task 5: Write the account trie value as `StateAccountV2`

**Files:**
- Modify: `core/state/statedb.go:1656-1662` (the non-deleted write branch in `Commit`)

- [ ] **Step 1: Write the failing test**

This is verified end-to-end in Task 6 (write+read round-trip through the real trie). For Task 5 in isolation, add a temporary assertion that the trie value is no longer raw proto:

```go
// core/state/state_account_write_test.go
package state

import (
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestCommitWritesV2Envelope(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x11)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 1234)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	raw, err := reopened.trie.Get(trieKey(addr))
	if err != nil || raw == nil {
		t.Fatalf("trie.Get: data=%v err=%v", raw, err)
	}
	v, err := DecodeStateAccountV2(raw)
	if err != nil {
		t.Fatalf("trie value is not a StateAccountV2 envelope: %v", err)
	}
	if v.Version != StateAccountVersion {
		t.Fatalf("version = %d", v.Version)
	}
}
```

> Uses the existing `newTestStateDB(t)` and `testAddr(b)` helpers from `core/state/statedb_test.go`. Reopen at the committed root in-package via `New(root, sdb.db)` (`sdb.db` is the unexported `*Database`, accessible within package `state`).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./core/state/ -run TestCommitWritesV2Envelope -v`
Expected: FAIL — `DecodeStateAccountV2` errors because the value is still raw `corepb.Account` proto bytes.

- [ ] **Step 3: Write minimal implementation**

Replace `core/state/statedb.go:1656-1662` (currently `data := obj.account.Marshal()` → `s.trie.Update(trieKey(addr), data)`):

```go
		accBytes, err := obj.account.Marshal()
		if err != nil {
			return tcommon.Hash{}, err
		}
		envelope := &StateAccountV2{
			Version:             StateAccountVersion,
			AccountProto:        accBytes,
			AccountKVRoot:       obj.accountKVRoot,
			AccountKVGeneration: obj.accountKVGeneration,
			// CodeHash is zero in Phase 1; populated in Phase 4 alongside the
			// content-addressed code domain. Code still loads from rawdb c-<addr>,
			// and the verbatim java code_hash remains inside AccountProto.
			CodeHash: tcommon.Hash{},
		}
		data, err := envelope.Encode()
		if err != nil {
			return tcommon.Hash{}, err
		}
		if err := s.trie.Update(trieKey(addr), data); err != nil {
			return tcommon.Hash{}, err
		}
```

> The delete branch (statedb.go:1637-1654) is unchanged — it still calls `s.trie.Delete(trieKey(addr))`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./core/state/ -run TestCommitWritesV2Envelope -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add core/state/statedb.go core/state/state_account_write_test.go
git commit -m "feat(state): commit account trie value as StateAccountV2 envelope"
```

---

### Task 6: Read the account trie value as `StateAccountV2`

**Files:**
- Modify: `core/state/statedb.go:1932-1942` (`getStateObject`)
- Test: `core/state/state_account_roundtrip_test.go` (create)

- [ ] **Step 1: Write the failing test**

```go
// core/state/state_account_roundtrip_test.go
package state

import (
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestAccountSurvivesCommitReopen(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x22)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 9999)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	obj := reopened.getStateObject(addr)
	if obj == nil {
		t.Fatal("account not found after reopen")
	}
	if got := obj.account.Balance(); got != 9999 {
		t.Fatalf("balance = %d, want 9999", got)
	}
	if obj.accountKVRoot != EmptyKVRoot {
		t.Fatalf("accountKVRoot = %x, want EmptyKVRoot", obj.accountKVRoot)
	}
	if obj.accountKVGeneration != 0 {
		t.Fatalf("accountKVGeneration = %d, want 0", obj.accountKVGeneration)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./core/state/ -run TestAccountSurvivesCommitReopen -v`
Expected: FAIL — `getStateObject` still does `types.UnmarshalAccount(data)` on the V2 envelope bytes, which fails to parse as `corepb.Account`, so the account is not found (obj == nil).

- [ ] **Step 3: Write minimal implementation**

Replace the decode block in `getStateObject` (`core/state/statedb.go:1932-1942`):

```go
	data, err := s.trie.Get(trieKey(addr))
	if err != nil || data == nil {
		return nil
	}
	envelope, err := DecodeStateAccountV2(data)
	if err != nil {
		return nil
	}
	acc, err := types.UnmarshalAccount(envelope.AccountProto)
	if err != nil {
		return nil
	}
	obj := newStateObject(addr, acc)
	obj.accountKVRoot = envelope.AccountKVRoot
	obj.accountKVGeneration = envelope.AccountKVGeneration
	s.stateObjects[addr] = obj
	return obj
```

> Phase 1 deliberately does **not** consume `envelope.CodeHash` on read — contract code still loads from rawdb `c-<addr>` lazily (`GetCode`), and `corepb.Account` carries its own `code_hash`. The envelope `CodeHash` becomes authoritative in Phase 4.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./core/state/ -run TestAccountSurvivesCommitReopen -v`
Expected: PASS.

- [ ] **Step 5: Run the whole state package + verify java root untouched**

Run: `go test ./core/state/ -count=1`
Expected: PASS. If `account_state_root.go` tests exist, they must pass unchanged (we never touched that file).

- [ ] **Step 6: Commit**

```bash
git add core/state/statedb.go core/state/state_account_roundtrip_test.go
git commit -m "feat(state): decode account trie value as StateAccountV2 envelope"
```

---

### Task 7: Normalize the account trie key to 20-byte `AccountID`

**Files:**
- Modify: `core/state/statedb.go:1973-1976` (`trieKey`)
- Test: `core/state/trie_key_test.go` (create)

- [ ] **Step 1: Write the failing test**

```go
// core/state/trie_key_test.go
package state

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
	tcommon "github.com/tronprotocol/go-tron/common"
)

func TestTrieKeyUsesAccountID(t *testing.T) {
	var addr tcommon.Address
	addr[0] = tcommon.AddressPrefixMainnet
	for i := 1; i < tcommon.AddressLength; i++ {
		addr[i] = byte(i)
	}
	got := trieKey(addr)
	want := crypto.Keccak256(addr.AccountID().Bytes())
	if !bytes.Equal(got, want) {
		t.Fatalf("trieKey = %x, want Keccak256(AccountID) = %x", got, want)
	}
	// Must differ from the old 21-byte keying.
	old := crypto.Keccak256(addr.Bytes())
	if bytes.Equal(got, old) {
		t.Fatal("trieKey must no longer hash the 21-byte address")
	}
}

func TestTrieKeyIgnoresPrefix(t *testing.T) {
	var a, b tcommon.Address
	a[0], b[0] = tcommon.AddressPrefixMainnet, tcommon.AddressPrefixTestnet
	for i := 1; i < tcommon.AddressLength; i++ {
		a[i], b[i] = byte(i), byte(i)
	}
	if !bytes.Equal(trieKey(a), trieKey(b)) {
		t.Fatal("trieKey must depend only on the 20-byte AccountID")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./core/state/ -run TestTrieKey -v`
Expected: FAIL — `trieKey` still hashes `addr.Bytes()` (21 bytes).

- [ ] **Step 3: Write minimal implementation**

Replace `core/state/statedb.go:1973-1976`:

```go
// trieKey returns the account-trie MPT key for a TRON address: the
// Keccak256 of its normalized 20-byte AccountID (network prefix stripped).
func trieKey(addr tcommon.Address) []byte {
	return crypto.Keccak256(addr.AccountID().Bytes())
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./core/state/ -run TestTrieKey -v && go test ./core/state/ -count=1`
Expected: PASS. The whole `core/state` package stays green (round-trip tests from Tasks 5–6 still pass because read and write share `trieKey`).

- [ ] **Step 5: Commit**

```bash
git add core/state/statedb.go core/state/trie_key_test.go
git commit -m "refactor(state): key account trie by 20-byte AccountID"
```

---

### Task 8: Update root-asserting fixtures and prove the full suite green

**Files (expected; confirm by running):**
- `core/genesis_test.go`
- `core/block_builder_test.go`
- `core/blockchain_insert_test.go`
- `core/blockchain_test.go`
- `core/tron_backend_archive_test.go`
- `core/blockchain_history_backfill_test.go`
- any other test that hard-codes an internal state root / genesis root

- [ ] **Step 1: Find every test that asserts an internal root**

Run:
```bash
grep -rn "GenesisStateRoot\|stateRoot\|StateRoot\|0x[0-9a-fA-F]\{40,\}" core/ --include="*_test.go"
```
List each hard-coded root literal. These changed because the trie value (envelope) and key (AccountID) both changed.

- [ ] **Step 2: Run the core suite to surface the new expected values**

Run: `go test ./core/... -count=1`
Expected: FAILs at each root assertion, printing `got <newRoot> want <oldRoot>`. Record the `got` value for each.

- [ ] **Step 3: Update each fixture to the new root**

For every failing assertion, replace the old root literal with the observed `got` value. Do NOT change assertions in cross-impl / java-tron interop tests or anything asserting `BlockHeader.raw.accountStateRoot` — that root comes from the untouched `JavaAccountStateRoot` (separate trie) and MUST remain identical. If such an assertion fails, STOP: it means something other than `account_state_root.go` moved it — investigate before editing.

- [ ] **Step 4: Confirm the java account-state root is unchanged**

The java root comes from the untouched `JavaAccountStateRoot` (a separate trie, keyed `RLP(addr.Bytes())`). Its test coverage is exactly three files: `core/genesis_test.go`, `core/block_builder_test.go`, `core/types/block_test.go`. Run:
```bash
go test ./core/ ./core/types/ -run "AccountStateRoot" -count=1 -v
```
Expected: PASS with **NO** fixture edits to any `accountStateRoot`/`JavaAccountStateRoot` assertion. If one of these moved, STOP — Phase 1 must not perturb the java root; investigate before editing anything.

- [ ] **Step 5: Full build, test, lint**

Run:
```bash
make gtron
make test
make lint
```
Expected: build succeeds; `go test ./... -count=1 -timeout 300s` passes; `golangci-lint run ./...` clean.

- [ ] **Step 6: Commit**

```bash
git add -u
git commit -m "test(state): update internal state-root fixtures for V2 envelope"
```

- [ ] **Step 7: Cross-impl stress gate (before Phase 2 starts)**

The hermetic suite proves internal consistency but not java-tron interop. Before Phase 2, run the cross-impl stress harness (`scripts/system_test_stress.sh`, see `docs/dev/`) against a private java-tron chain to confirm go-tron blocks remain acceptable to java-tron and golden actuator results still match. A clean stress run is the real Phase 1 acceptance signal; record it before building generic KV on top.

---

## Self-Review (completed during planning)

- **Spec coverage (Phase 1 section of the design):**
  - "Add a `core/state/kvdomains` package or equivalent central registry" → Task 1.
  - "Add internal `StateAccountV2` encoding/decoding" → Task 3.
  - "Update account trie value handling to support only V2 for fresh databases" → Tasks 5 (write) + 6 (read); version-enforced decode rejects non-V2 (Task 3).
  - "Keep java-tron account serialization unchanged for RPC and wire-facing code" → `account_state_root.go` untouched (guardrails + Task 8 Step 4); `corepb.Account` unchanged; `AccountProto` carries the verbatim java serialization.
  - "Add empty account KV root and account KV generation fields to state objects" → Task 4 (defaults `EmptyKVRoot` / 0).
  - AccountID 21→20 normalization (Address Identity Model + user decision to bundle) → Tasks 2 + 7.
- **Placeholder scan:** every code step contains real code; the only deferred item is `newTestStateDB` in Task 5, with explicit fallback construction instructions and a grep to find the existing helper. No "TBD"/"handle errors appropriately".
- **Type consistency:** `StateAccountV2` field names (`Version`, `AccountProto`, `AccountKVRoot`, `AccountKVGeneration`, `CodeHash`) are identical across Tasks 3, 5, 6. `accountKVRoot`/`accountKVGeneration` (lowercase struct fields) identical across Tasks 4, 5, 6. `EmptyKVRoot`, `StateAccountVersion`, `DecodeStateAccountV2`, `Encode`, `AccountID`, `AccountIDLength`, `addr.AccountID()` used consistently.

## Out of scope (later phases — do not start here)

- Generic account-KV tries, `StateDB.GetAccountKV/SetAccountKV/...`, KV journal entries, generation increment/reset → **Phase 2**.
- Reserved `SystemAccountID` genesis creation; moving dynamic properties / witness schedule behind system-account KV → **Phase 3**.
- Moving contract storage / code (content-addressed) / metadata / witness capsules into KV/code domains → **Phase 4**.
- Audit + relocation of remaining consensus stores → **Phase 5**.
- Historical rewind command/API → **Phase 6**.
- Change sets / pruning / snapshot files → **Phase 7**.

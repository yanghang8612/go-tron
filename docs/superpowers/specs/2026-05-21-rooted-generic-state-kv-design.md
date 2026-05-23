# Rooted Generic Account KV State Design

Date: 2026-05-21

Status: design draft

Scope: fresh database only. This design intentionally does not include migration from
the current flat state layout.

## Problem

go-tron currently persists many consensus-relevant objects as independent flat KV
records outside a single rooted state model. Accounts are covered by the internal
account-state root side store, but dynamic properties, witness data, contract code,
contract metadata, contract storage, proposals, assets, exchanges, delegation data,
and other chain-global records are mostly rootless physical records.

That makes recent fork handling possible through the block buffer, but it prevents a
general "restart sync from historical height" feature because selecting an old block
head does not also select the full state that existed at that height.

The target design is a rooted state model where every consensus-relevant mutable
record is reachable from one full internal state root, while java-tron wire
compatibility remains unchanged.

## Non-goals

- No migration or compatibility path for existing go-tron databases.
- No change to protobuf definitions imported from java-tron.
- No change to P2P messages, block encoding, transaction encoding, actuator dispatch,
  or java-tron consensus behavior.
- No attempt to replace TRON's `BlockHeader.raw.accountStateRoot` semantics. That
  header field remains the java-tron lightweight account-state root, not the new
  internal full-state root.
- No per-transaction Erigon-style historical query system in the first version.
  Block-level rooted rewind is the immediate goal.

## Current State Observations

The current `corepb.Account` message has `codeHash`, but it does not have a
`storage_root` field. Because proto files are imported from java-tron, go-tron should
not add `storage_root` to the protobuf account type.

The current `StateDB` commits the serialized `corepb.Account` directly into the
internal account trie. Contract code, contract metadata, and contract storage are
stored through flat rawdb prefixes. Dynamic properties and many chain-global stores
are also flat records.

The current persisted roots are therefore split:

- canonical chain head pointer: physical head selection
- internal account state root side store: rooted account proto state only
- java-tron `accountStateRoot` header field: lightweight java-compatible root
- many rootless consensus stores: dynamic properties, witness data, contract storage,
  contract metadata, indexes, and other state capsules

The design below collapses consensus-relevant mutable state into one internal full
state root without changing java-tron-visible data.

## Erigon Research

The local Erigon tree studied for this design is:

- `/Users/asuka/Projects/erigontech/erigon/README.md`
- `/Users/asuka/Projects/erigontech/erigon/docs/programmers_guide/guide.md`
- `/Users/asuka/Projects/erigontech/erigon/db/state/domain.go`
- `/Users/asuka/Projects/erigontech/erigon/db/state/history.go`
- `/Users/asuka/Projects/erigontech/erigon/db/state/aggregator.go`
- `/Users/asuka/Projects/erigontech/erigon/db/state/execctx/domain_shared.go`
- `/Users/asuka/Projects/erigontech/erigon/db/state/statecfg/state_schema.go`
- `/Users/asuka/Projects/erigontech/erigon/execution/state/rw_v3.go`
- `/Users/asuka/Projects/erigontech/erigon/db/state/changeset/state_changeset.go`

Useful Erigon concepts:

- State is split into logical domains: accounts, storage, code, commitment, receipts,
  and other optional domains.
- Latest state, historical values, inverted indexes, and commitment data are separate
  structures with explicit dependencies.
- Reads use a layered view: dirty overlay first, parent/mem state next, cache next,
  persisted latest state last.
- Writes record previous values so unwind/history can be generated.
- Storage keys are composed from account identity plus storage key. Erigon also uses
  account "incarnation" to avoid deleting huge storage ranges when an account is
  destroyed and later recreated.
- Snapshots and history are immutable segments, while the hot database stays smaller.

Borrow for go-tron:

- Logical domain separation.
- A generic domain/KV abstraction instead of one-off rawdb prefixes.
- Layered read/write overlay inside block execution.
- Recording previous values for rollback and future history support.
- Account KV generation, inspired by Erigon incarnation, to avoid O(N) prefix deletes.
- Clear separation between canonical rooted state and derived indexes.

Do not borrow in the first version:

- MDBX-specific data layout.
- Erigon's snapshot file formats.
- Per-transaction `txNum` history.
- Ethereum-specific selfdestruct semantics. go-tron should generalize the idea as an
  account KV generation.

## Design Overview

The new internal full state root is keyed by a 20-byte account identity, not by the
21-byte TRON wire address:

```text
FullStateRoot = Root(AccountTrie)

AccountTrie[account_id20] = StateAccountV2

StateAccountV2 = {
  AccountProtoBytes,
  AccountKVRoot,
  AccountKVGeneration,
  CodeHash
}

AccountKVRoot = Root(AccountKVTrie)
AccountKVTrie[hash(domain_id || logical_key)] = encoded_value
```

Every account has one generic rooted KV space. That includes:

- EOA accounts
- smart-contract accounts
- witness accounts
- reserved system accounts

System/global state is represented as KV entries under a reserved system account
instead of a separate global flat KV store.

Contract storage, witness capsules, contract metadata, and future account-local data
are all modeled as namespaced entries in the owning account's generic KV.

Large immutable code bytes are stored in a separate content-addressed code domain,
with `CodeHash` committed in `StateAccountV2`. This follows Erigon's separation of
code from account/storage while keeping code reachable from the full root.

## Address Identity Model

go-tron must keep java-tron-visible addresses as 21-byte TRON addresses:

```text
tron_address21 = network_prefix1 || account_id20
```

The network prefix is `0x41` on mainnet-style addresses and `0xa0` on testnet-style
addresses in the current codebase. Protobuf fields, transaction contracts, block
headers, witness addresses, RPC-facing values, signatures, java-tron account-state
root calculation, and TVM boundary behavior must continue to use the java-tron
21-byte representation where java-tron does.

The rooted state implementation should normalize addresses to:

```text
AccountID = tron_address21[1:]  // 20 bytes, after prefix validation
```

Use `AccountID` for:

- account trie keys
- generic account KV owner keys
- physical latest-state owner prefixes
- account KV history/change-set owner keys
- system account identity

Do not remove the prefix at protocol boundaries. Address derivation and validation
logic must still match java-tron, then the state layer converts the resulting
21-byte address into `AccountID` for storage lookup.

This gives the storage model the same 20-byte identity shape as Solidity/TVM ABI
address words, while preserving TRON's 21-byte external address semantics.

## Reserved System Account

Use one reserved 20-byte account ID as the owner of chain-global state:

```text
SystemAccountID = 0xfffffffffffffffffffffffffffffffffffffffe
```

When exposed through a TRON-address boundary, the rendered address is:

```text
mainnet-style: 0x41fffffffffffffffffffffffffffffffffffffffe
testnet-style: 0xa0fffffffffffffffffffffffffffffffffffffffe
```

The state layer must reject user-created or user-mutated account operations against
this account identity unless the write comes from an internal system store.

The system account's generic KV owns consensus-global records such as:

- dynamic properties
- latest solid block metadata
- maintenance-cycle state
- active witness list and shuffled witness schedule
- witness indexes
- proposals and proposal indexes
- fork-controller vote state
- assets, exchanges, markets, and chain-global indexes
- delegation, reward, brokerage, and vote accounting records when not naturally owned
  by a user account
- account-name and account-id indexes
- shielded/global feature state

Each former flat global store should be moved behind typed accessors that read/write
`AccountKV(SystemAccountID, domain, key)`.

## Generic Account KV

The account KV design should be domain-first and type-neutral.

```go
type AccountID [20]byte
type KVDomain uint16

type AccountKV interface {
    Get(owner AccountID, domain KVDomain, key []byte) ([]byte, bool, error)
    Put(owner AccountID, domain KVDomain, key []byte, value []byte) error
    Delete(owner AccountID, domain KVDomain, key []byte) error
    DeletePrefix(owner AccountID, domain KVDomain, prefix []byte) error
    Iterator(owner AccountID, domain KVDomain, prefix []byte) (Iterator, error)
}
```

Properties:

- Values are opaque bytes at this layer.
- Domain-specific packages own protobuf, integer, address, and composite-key encoding.
- Typed stores may accept `common.Address` at their public boundary, but they must
  validate and normalize to `AccountID` before calling the generic KV layer.
- Empty value and deleted value must be distinct. A nil value in journals/change sets
  means delete.
- Iteration is scoped by owner, generation, domain, and prefix.
- Domain IDs are centrally registered. No raw domain constants should be scattered
  through actuators.

Suggested domain groups:

```text
0x0001-0x00ff: system/global domains
0x0100-0x01ff: contract domains
0x0200-0x02ff: account-local domains
0x0300-0x03ff: witness domains
0x0400-0x04ff: governance domains
0x8000-0xffff: test/private/reserved domains
```

Initial domains:

```text
0x0001 SystemDynamicProperty
0x0002 SystemWitnessSchedule
0x0003 SystemProposal
0x0004 SystemForkVote
0x0005 SystemAsset
0x0006 SystemExchange
0x0007 SystemDelegation
0x0008 SystemAccountIndex

0x0100 ContractStorage
0x0101 ContractMetadata
0x0102 ContractABI
0x0103 ContractRuntimeState

0x0200 AccountLocalIndex
0x0201 AccountPermissionAux

0x0300 WitnessCapsule
0x0301 WitnessVoteState
```

The exact registry can be adjusted during implementation, but the invariant is that
all consensus-relevant mutable state has an owner account, domain, and logical key.

## Account KV Generation

Each `StateAccountV2` has an `AccountKVGeneration` field.

Physical latest-state keys use:

```text
physical_key = owner_account_id20 || generation_u64 || domain_id_u16 || logical_key
```

The committed per-account trie key uses:

```text
trie_key = Hash(domain_id_u16 || logical_key)
```

When an account's KV namespace must be discarded, the state layer increments
`AccountKVGeneration` and resets `AccountKVRoot` to the empty root. Old physical keys
become unreachable from the account's latest generation and remain available only to
old roots/history until pruning removes them.

This avoids expensive prefix deletion for contracts or accounts with many KV entries.
It also generalizes Erigon's incarnation idea beyond Ethereum selfdestruct.

The generation must change only through state-layer operations, never through typed
store packages.

## StateAccountV2 Encoding

`corepb.Account` remains unchanged.

The value stored in the internal account trie changes from serialized
`corepb.Account` to an internal envelope:

```text
StateAccountV2 {
  version: 2
  account_proto: bytes
  account_kv_root: bytes32
  account_kv_generation: uint64
  code_hash: bytes32
}
```

Encoding requirements:

- Deterministic.
- Versioned.
- Independent from java-tron protobuf definitions.
- Does not leak into network messages, blocks, transactions, or RPC responses unless
  an explicit debug/internal endpoint is added.

Implementation can use a small internal protobuf, RLP, or another deterministic
encoding already used in the codebase. The important rule is that java-tron imported
proto messages are not modified.

## Code Domain

Smart-contract code is large, immutable, and naturally content-addressed. It should be
stored outside the per-account KV trie in a code domain:

```text
CodeDomain[code_hash] = code_bytes
```

`StateAccountV2.CodeHash` commits the selected code for an account. Because the
account trie commits the code hash, code remains part of the internal full state root
without duplicating large bytecode in per-account KV tries.

Contract metadata and runtime mutable contract state should use the owning account's
generic KV domains. Only immutable code bytes need this separate code domain.

## Contract Storage

TVM storage becomes a normal account KV domain:

```text
owner  = contract_address
domain = ContractStorage
key    = java_tron_compatible_storage_key
value  = storage_value_bytes
```

The storage key adapter must preserve current TRON storage semantics. Existing code
that currently calls rawdb storage accessors should be moved behind `StateDB` or a
contract storage facade backed by generic AccountKV.

The contract account's `AccountKVRoot` replaces the missing protobuf `storage_root`.
No `storage_root` field is added to `corepb.Account`.

## Witness State

Witness records should be owned by the witness account where possible:

```text
owner  = witness_address
domain = WitnessCapsule
key    = canonical witness record key
value  = serialized witness capsule
```

Global witness scheduling data remains under `SystemAccountID`, because the schedule
is global and selected by maintenance-cycle logic:

```text
owner  = SystemAccountID
domain = SystemWitnessSchedule
key    = schedule key
value  = encoded schedule value
```

This split keeps witness-owned data account-local while preserving global scheduling
as system state.

## EOA State

EOA accounts can also own arbitrary future KV domains. This is intentional. The
storage model should not assume that only contracts have storage.

Initial EOA KV use cases include:

- account-local auxiliary indexes
- permission auxiliary data if it grows beyond the account proto
- future protocol features that attach many records to a user account

Typed accessors must continue to enforce consensus rules about which transaction
types may mutate which domains.

## Root and Commit Flow

Block execution uses a layered state view:

```text
dirty overlay
    -> state snapshot/journal
        -> account KV tries for current root
            -> persisted rooted state
```

Commit order:

1. Execute transactions against `StateDB`.
2. Track dirty accounts and dirty account-KV domains.
3. For each dirty account, commit its account KV trie and update
   `StateAccountV2.AccountKVRoot`.
4. Commit content-addressed code writes.
5. Commit the account trie to produce the new internal full state root.
6. Store `block_hash -> full_state_root` in the existing root side store or a renamed
   successor.
7. Compute java-tron `BlockHeader.raw.accountStateRoot` exactly as today.
8. Persist blocks, receipts, tx indexes, TAPOS cache, and other derived records.

The internal full state root and the java-tron header root must remain separate.

## Journaling and Revert

`StateDB.Snapshot` and `StateDB.RevertToSnapshot` must cover:

- account proto mutations
- account creation/deletion
- account KV put/delete operations
- account KV generation changes
- code hash changes
- code-domain writes made during the snapshot

Each dirty KV write records the previous value in the journal:

```text
KVChange {
  owner
  generation
  domain
  logical_key
  previous_value
  had_previous_value
}
```

This mirrors the useful part of Erigon's shared-domain write model while staying
block-level in the first implementation.

## Historical Rewind

With complete full-state roots, restarting sync from a historical block becomes:

1. Resolve target block by height.
2. Load `target_block_hash -> full_state_root`.
3. Set canonical head pointers to the target block.
4. Open `StateDB` at the target full-state root.
5. Clear txpool and network sync in-flight state.
6. Delete or guard derived transaction indexes above the target height.
7. Rebuild TAPOS/reference-block helpers from canonical block data around the target.
8. Resume sync from target height + 1.

Rollback correctness depends on every consensus-relevant mutable store being reachable
from the loaded full-state root. Transaction indexes, receipts, logs, and query
accelerators are derived data and may be deleted or rebuilt as needed.

## Derived Data Policy

Stores that do not affect block execution can remain outside the full state root.

Examples:

- block bodies and headers
- transaction lookup indexes
- receipt lookup indexes
- API query indexes
- peer/sync metadata
- txpool contents
- caches

Rules:

- Consensus validation must not read unrooted derived data as source of truth.
- Rewind must either delete derived records above the target height or include height
  guards in readers.
- If a record can change the result of actuator validation or execution, it belongs in
  rooted state.

## RawDB Layout Direction

New low-level prefixes should still be defined in `core/rawdb/schema.go`; callers
should not hand-roll keys.

Suggested physical groups:

```text
state-account-v2:         account trie nodes / account trie values
state-account-kv-v2:      account KV trie nodes / values
state-code-v2:            code_hash -> code bytes
state-root-v2:            block_hash -> full_state_root
state-kv-latest-v2:       owner || generation || domain || key -> value
state-kv-history-v2:      reserved for future history/change sets
```

The first implementation can keep latest state and trie data in Pebble. The API
should leave room for a future Erigon-like split between hot latest state, immutable
history, and snapshot/freezer files.

## API Surface

Introduce a single state-facing API for rooted KV:

```go
func (s *StateDB) GetAccountKV(owner common.Address, domain KVDomain, key []byte) ([]byte, bool, error)
func (s *StateDB) SetAccountKV(owner common.Address, domain KVDomain, key []byte, value []byte) error
func (s *StateDB) DeleteAccountKV(owner common.Address, domain KVDomain, key []byte) error
func (s *StateDB) IterateAccountKV(owner common.Address, domain KVDomain, prefix []byte) (Iterator, error)
func (s *StateDB) ResetAccountKV(owner common.Address) error
```

Typed stores wrap this API:

- `DynamicPropertiesStore`
- `WitnessStore`
- `ContractStore`
- `ContractStorage`
- `ProposalStore`
- `AssetStore`
- `DelegationStore`
- other capsule stores

Actuators and consensus code should depend on typed stores, not on raw generic KV
calls unless the operation is inherently generic.

## Validation Rules

The state layer must enforce:

- reserved system account cannot be mutated by normal transactions
- only authorized internal store code may write system domains
- domain IDs must be registered
- account KV generation cannot be set directly by callers
- contract storage writes require the owner to be a contract account where TRON rules
  require that
- witness domains require the owner to be a witness account where TRON rules require
  that

Typed stores should enforce higher-level consensus rules. The generic KV layer
enforces structural invariants.

## Implementation Plan

### Phase 1: Root Envelope and Domain Registry

- Add a `core/state/kvdomains` package or equivalent central registry.
- Add internal `StateAccountV2` encoding/decoding.
- Update account trie value handling to support only V2 for fresh databases.
- Keep java-tron account serialization unchanged for RPC and wire-facing code.
- Add empty account KV root and account KV generation fields to state objects.

### Phase 2: Generic Account KV

- Implement per-account generic KV tries.
- Add StateDB APIs for get/set/delete/iterate/reset.
- Extend StateDB snapshots and journal entries to cover KV writes.
- Add account KV root commit before account trie commit.
- Add generation increment/reset support.

### Phase 3: System Account Stores

- Create the reserved system account during genesis initialization.
- Move dynamic properties behind system account KV.
- Move maintenance-cycle and witness-schedule state behind system account KV.
- Keep typed store interfaces stable where possible.

### Phase 4: Contract and Witness Stores

- Move contract storage to `ContractStorage` account KV domain.
- Move contract metadata/ABI/runtime mutable state to account KV domains.
- Move immutable code bytes to the content-addressed code domain.
- Move witness capsules to witness-owned KV domains.
- Keep global witness schedule under the system account.

### Phase 5: Remaining Consensus Stores

- Audit all rawdb/state stores used by actuators, forks, DPoS, and VM.
- Move every consensus-relevant mutable store into account KV or code domain.
- Mark all remaining flat records as derived or physical chain data.

### Phase 6: Rewind Support

- Add a rewind command/API that selects a historical canonical block.
- Load the full-state root for that block.
- Reset head pointers.
- Clear txpool/sync state.
- Delete or guard derived indexes above the rewind height.
- Rebuild TAPOS helpers.

### Phase 7: Optional History/Snapshot Extension

- Add block-level change sets from recorded previous values.
- Add pruning policy for unreachable account KV generations.
- Consider immutable snapshot/freezer files for old state/history.
- Keep this phase separate from the initial rooted-state correctness work.

## Test Plan

Fresh database only:

- genesis creates `SystemAccountID`
- genesis full-state root is deterministic
- `StateAccountV2` encoding round-trips deterministically
- account KV get/set/delete/iterate works for EOA, contract, witness, and system
  accounts
- account KV generation reset makes old keys unreachable without prefix deletion
- StateDB snapshot/revert restores account KV and generation changes
- contract storage reads/writes through generic KV
- contract code hash commits code selection into the full state root
- witness capsules and witness schedules read through their new stores
- dynamic properties read/write through system account KV
- 21-byte TRON addresses normalize to stable 20-byte AccountIDs for rooted state keys
- block execution produces stable internal full-state roots
- java-tron `accountStateRoot` header calculation remains unchanged
- block and transaction wire encoding remains unchanged
- fork/blockbuffer rewind tests still pass
- new historical rewind test selects an older block and resumes execution from the
  exact rooted state

Interop tests:

- existing P2P compatibility tests remain unchanged
- blocks produced by go-tron remain acceptable to java-tron for unchanged consensus
  scenarios
- golden actuator tests continue to match java-tron results

## Open Decisions

- Exact deterministic encoding format for `StateAccountV2`.
- Whether the physical latest-state index is required in the first version or can be
  introduced together with history/pruning.
- Exact domain registry split for legacy stores after the rawdb audit.
- Pruning policy for old account KV generations.
- Whether debug/internal RPCs should expose full-state roots and account KV roots.

## Key Invariants

- Imported java-tron protobuf messages are not modified.
- External/protobuf/RPC/TVM boundary addresses remain java-tron-compatible 21-byte
  TRON addresses.
- Rooted account state and generic account KV use 20-byte `AccountID` owner keys
  without the network prefix.
- All consensus-relevant mutable state is reachable from the internal full-state root.
- `BlockHeader.raw.accountStateRoot` stays java-tron compatible and separate.
- Every account has one generic KV namespace, regardless of account type.
- System/global state is owned by the reserved `SystemAccountID`.
- Contract storage is a domain in generic account KV, not a special flat rawdb store.
- Large immutable code is content-addressed and committed by code hash.
- Transaction-related indexes are derived data and can be deleted or rebuilt on rewind.

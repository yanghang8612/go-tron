package state

import (
	"errors"
	"fmt"
	"strconv"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

var (
	ErrInsufficientBalance = errors.New("insufficient balance")
)

// StateDB manages in-memory account state with MPT-backed commits.
type StateDB struct {
	db   *Database
	trie *trie.Trie

	stateObjects map[tcommon.Address]*stateObject
	witnesses    map[tcommon.Address]*types.Witness

	journal   *journal
	snapshots []int // journal length at each snapshot

	dynProps *DynamicProperties

	// originRoot is the trie root at the time of the last successful Commit (or
	// the root passed to New). It is used as the parent root when updating the
	// triedb so that the hashdb reference graph is correct across multiple blocks.
	originRoot ethcommon.Hash
}

// New creates a StateDB from the given state root.
func New(root tcommon.Hash, db *Database) (*StateDB, error) {
	tr, err := db.OpenTrie(ethcommon.Hash(root))
	if err != nil {
		return nil, err
	}
	return &StateDB{
		db:           db,
		trie:         tr,
		stateObjects: make(map[tcommon.Address]*stateObject),
		witnesses:    make(map[tcommon.Address]*types.Witness),
		journal:      newJournal(),
		dynProps:     NewDynamicProperties(),
		originRoot:   ethcommon.Hash(root),
	}, nil
}

// GetAccount returns the account at addr, or nil if not found.
func (s *StateDB) GetAccount(addr tcommon.Address) *types.Account {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return nil
	}
	return obj.account
}

// GetOrCreateAccount returns the state object at addr, creating it if it doesn't exist.
// When a new account is created, a nil-prev journal entry is recorded so that
// snapshot revert can delete it.
func (s *StateDB) GetOrCreateAccount(addr tcommon.Address) *stateObject {
	obj := s.getStateObject(addr)
	if obj != nil && !obj.deleted {
		return obj
	}
	// Journal a nil-prev entry so revert can delete this new account.
	s.journal.append(accountChange{
		address: addr,
		prev:    nil,
	})
	obj = newEmptyStateObject(addr)
	s.stateObjects[addr] = obj
	return obj
}

// GetBalance returns the TRX balance of the account.
func (s *StateDB) GetBalance(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.Balance()
}

// AddBalance adds amount to the account's balance.
func (s *StateDB) AddBalance(addr tcommon.Address, amount int64) {
	obj := s.GetOrCreateAccount(addr)
	s.journalAccount(addr, obj)
	obj.account.SetBalance(obj.account.Balance() + amount)
	obj.markDirty()
}

// SubBalance subtracts amount from the account's balance.
func (s *StateDB) SubBalance(addr tcommon.Address, amount int64) error {
	obj := s.getStateObject(addr)
	if obj == nil {
		return ErrInsufficientBalance
	}
	if obj.account.Balance() < amount {
		return ErrInsufficientBalance
	}
	s.journalAccount(addr, obj)
	obj.account.SetBalance(obj.account.Balance() - amount)
	obj.markDirty()
	return nil
}

// GetTRC10Balance returns the TRC10 token balance of addr for the given tokenID.
// Balances are stored in the account proto's AssetV2 map (persisted through state commits).
func (s *StateDB) GetTRC10Balance(addr tcommon.Address, tokenID int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.Proto().GetAssetV2()[strconv.FormatInt(tokenID, 10)]
}

// GetTRC10BalanceByName returns the legacy pre-AllowSameTokenName TRC10
// balance stored in Account.asset keyed by token name.
func (s *StateDB) GetTRC10BalanceByName(addr tcommon.Address, name []byte) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.Proto().GetAsset()[string(name)]
}

// SetTRC10Balance sets the TRC10 token balance in the account proto's AssetV2 map.
func (s *StateDB) SetTRC10Balance(addr tcommon.Address, tokenID int64, amount int64) {
	obj := s.GetOrCreateAccount(addr)
	s.journalAccount(addr, obj)
	pb := obj.account.Proto()
	if pb.AssetV2 == nil {
		pb.AssetV2 = make(map[string]int64)
	}
	pb.AssetV2[strconv.FormatInt(tokenID, 10)] = amount
	obj.markDirty()
}

// SetTRC10BalanceByName sets the legacy Account.asset balance keyed by token name.
func (s *StateDB) SetTRC10BalanceByName(addr tcommon.Address, name []byte, amount int64) {
	obj := s.GetOrCreateAccount(addr)
	s.journalAccount(addr, obj)
	pb := obj.account.Proto()
	if pb.Asset == nil {
		pb.Asset = make(map[string]int64)
	}
	pb.Asset[string(name)] = amount
	obj.markDirty()
}

// SetTRC10BalanceLegacyAndV2 mirrors java-tron AccountCapsule.addAssetAmountV2
// before AllowSameTokenName: the legacy Account.asset value is authoritative,
// and Account.assetV2 is kept in lockstep under the token ID.
func (s *StateDB) SetTRC10BalanceLegacyAndV2(addr tcommon.Address, name []byte, tokenID int64, amount int64) {
	obj := s.GetOrCreateAccount(addr)
	s.journalAccount(addr, obj)
	pb := obj.account.Proto()
	if pb.Asset == nil {
		pb.Asset = make(map[string]int64)
	}
	if pb.AssetV2 == nil {
		pb.AssetV2 = make(map[string]int64)
	}
	pb.Asset[string(name)] = amount
	pb.AssetV2[strconv.FormatInt(tokenID, 10)] = amount
	obj.markDirty()
}

func (s *StateDB) GetTRC10BalanceFinal(addr tcommon.Address, name []byte, tokenID int64, allowSameTokenName bool) int64 {
	if allowSameTokenName {
		return s.GetTRC10Balance(addr, tokenID)
	}
	return s.GetTRC10BalanceByName(addr, name)
}

func (s *StateDB) AddTRC10BalanceFinal(addr tcommon.Address, name []byte, tokenID int64, amount int64, allowSameTokenName bool) {
	if allowSameTokenName {
		s.AddTRC10Balance(addr, tokenID, amount)
		return
	}
	current := s.GetTRC10BalanceByName(addr, name)
	s.SetTRC10BalanceLegacyAndV2(addr, name, tokenID, current+amount)
}

func (s *StateDB) SubTRC10BalanceFinal(addr tcommon.Address, name []byte, tokenID int64, amount int64, allowSameTokenName bool) error {
	if allowSameTokenName {
		return s.SubTRC10Balance(addr, tokenID, amount)
	}
	current := s.GetTRC10BalanceByName(addr, name)
	if current < amount {
		return ErrInsufficientBalance
	}
	s.SetTRC10BalanceLegacyAndV2(addr, name, tokenID, current-amount)
	return nil
}

// SetAssetIssued records the issued TRC10 token's name and ID on the issuer
// account, mirroring java-tron's AssetIssueActuator (accountCapsule
// .setAssetIssuedName / .setAssetIssuedID). These fields are part of the
// persisted account proto, so they must live in state — not be derived at
// read time — or the conformance digest diverges at the issuance block.
func (s *StateDB) SetAssetIssued(addr tcommon.Address, name []byte, id string) {
	obj := s.GetOrCreateAccount(addr)
	s.journalAccount(addr, obj)
	pb := obj.account.Proto()
	pb.AssetIssuedName = name
	pb.AssetIssued_ID = []byte(id)
	obj.markDirty()
}

// AddFrozenSupply appends frozen-supply entries to the account proto's
// frozen_supply field. java-tron's AssetIssueActuator writes these onto the
// issuer account when a TRC10 token is issued with a FrozenSupply list.
func (s *StateDB) AddFrozenSupply(addr tcommon.Address, frozen []*corepb.Account_Frozen) {
	if len(frozen) == 0 {
		return
	}
	obj := s.GetOrCreateAccount(addr)
	s.journalAccount(addr, obj)
	pb := obj.account.Proto()
	pb.FrozenSupply = append(pb.FrozenSupply, frozen...)
	obj.markDirty()
}

func (s *StateDB) RemoveExpiredFrozenSupply(addr tcommon.Address, now int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	pb := obj.account.Proto()
	if len(pb.FrozenSupply) == 0 {
		return 0
	}
	s.journalAccount(addr, obj)
	var remaining []*corepb.Account_Frozen
	var amount int64
	for _, frozen := range pb.FrozenSupply {
		if frozen.ExpireTime <= now {
			amount += frozen.FrozenBalance
			continue
		}
		remaining = append(remaining, frozen)
	}
	pb.FrozenSupply = remaining
	obj.markDirty()
	return amount
}

// AddTRC10Balance credits amount TRC10 tokens to addr.
func (s *StateDB) AddTRC10Balance(addr tcommon.Address, tokenID int64, amount int64) {
	s.SetTRC10Balance(addr, tokenID, s.GetTRC10Balance(addr, tokenID)+amount)
}

// SubTRC10Balance debits amount TRC10 tokens from addr.
// Returns ErrInsufficientBalance if addr has fewer than amount tokens.
func (s *StateDB) SubTRC10Balance(addr tcommon.Address, tokenID int64, amount int64) error {
	current := s.GetTRC10Balance(addr, tokenID)
	if current < amount {
		return ErrInsufficientBalance
	}
	s.SetTRC10Balance(addr, tokenID, current-amount)
	return nil
}

// TransferAllTRC10Balance moves every AssetV2 token balance from one account
// to another, leaving explicit zero entries on the source account. This
// mirrors java-tron MUtil.transferAllToken, used by SELFDESTRUCT.
func (s *StateDB) TransferAllTRC10Balance(from, to tcommon.Address) {
	fromObj := s.getStateObject(from)
	if fromObj == nil || fromObj.account == nil {
		return
	}
	fromPB := fromObj.account.Proto()
	if len(fromPB.AssetV2) == 0 {
		return
	}
	toObj := s.GetOrCreateAccount(to)
	s.journalAccount(from, fromObj)
	s.journalAccount(to, toObj)
	toPB := toObj.account.Proto()
	if toPB.AssetV2 == nil {
		toPB.AssetV2 = make(map[string]int64)
	}
	for tokenID, amount := range fromPB.AssetV2 {
		toPB.AssetV2[tokenID] += amount
		fromPB.AssetV2[tokenID] = 0
	}
	fromObj.markDirty()
	toObj.markDirty()
}

// IsFrozenClaimed returns whether frozen_supply entry at index has been claimed.
func (s *StateDB) IsFrozenClaimed(addr tcommon.Address, tokenID int64, index uint32) bool {
	v := s.GetState(addr, trc10FrozenClaimedSlot(tokenID, index))
	return v[31] != 0
}

// SetFrozenClaimed marks frozen_supply entry at index as claimed.
func (s *StateDB) SetFrozenClaimed(addr tcommon.Address, tokenID int64, index uint32) {
	var v tcommon.Hash
	v[31] = 0x01
	s.SetState(addr, trc10FrozenClaimedSlot(tokenID, index), v)
}

// AddFreezeV2 adds a freeze entry for the given resource type.
func (s *StateDB) AddFreezeV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.AddFreezeV2(resourceType, amount)
	obj.markDirty()
}

// --- V1 Stake (Stake 1.0) StateDB methods ---

func (s *StateDB) FreezeV1Bandwidth(addr tcommon.Address, amount, expireTimeMs int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetFrozenBandwidth(obj.account.TotalFrozenBandwidth()+amount, expireTimeMs)
	obj.markDirty()
}

func (s *StateDB) UnfreezeV1Bandwidth(addr tcommon.Address, blockTimeMs int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	s.journalAccount(addr, obj)
	refunded := obj.account.RemoveExpiredFrozenBandwidth(blockTimeMs)
	obj.markDirty()
	return refunded
}

func (s *StateDB) FreezeV1Energy(addr tcommon.Address, amount, expireTimeMs int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.AddFrozenEnergy(amount, expireTimeMs)
	obj.markDirty()
}

func (s *StateDB) FreezeV1TronPower(addr tcommon.Address, amount, expireTimeMs int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.AddV1TronPower(amount, expireTimeMs)
	obj.markDirty()
}

func (s *StateDB) UnfreezeV1TronPower(addr tcommon.Address, blockTimeMs int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	if obj.account.V1TronPowerExpireTime() > blockTimeMs {
		return 0
	}
	amount := obj.account.V1TronPowerFrozen()
	if amount == 0 {
		return 0
	}
	s.journalAccount(addr, obj)
	obj.account.ClearV1TronPower()
	obj.markDirty()
	return amount
}

func (s *StateDB) UnfreezeV1Energy(addr tcommon.Address, blockTimeMs int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	if obj.account.FrozenEnergyExpireTime() > blockTimeMs {
		return 0
	}
	amount := obj.account.FrozenEnergyAmount()
	if amount == 0 {
		return 0
	}
	s.journalAccount(addr, obj)
	obj.account.ClearFrozenEnergy()
	obj.markDirty()
	return amount
}

func (s *StateDB) GetDelegatedFrozenV1Bandwidth(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.DelegatedFrozenBandwidth()
}

func (s *StateDB) GetDelegatedFrozenV1Energy(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.DelegatedFrozenEnergy()
}

func (s *StateDB) FreezeV1DelegatedBandwidth(owner, receiver tcommon.Address, amount int64) {
	ownerObj := s.getStateObject(owner)
	if ownerObj == nil {
		return
	}
	s.journalAccount(owner, ownerObj)
	ownerObj.account.SetDelegatedFrozenBandwidth(ownerObj.account.DelegatedFrozenBandwidth() + amount)
	ownerObj.markDirty()

	recvObj := s.getStateObject(receiver)
	if recvObj == nil {
		return
	}
	s.journalAccount(receiver, recvObj)
	recvObj.account.SetAcquiredDelegatedFrozenBandwidth(recvObj.account.AcquiredDelegatedFrozenBandwidth() + amount)
	recvObj.markDirty()
}

func (s *StateDB) UnfreezeV1DelegatedBandwidth(owner, receiver tcommon.Address, amount int64) {
	ownerObj := s.getStateObject(owner)
	if ownerObj == nil {
		return
	}
	s.journalAccount(owner, ownerObj)
	ownerObj.account.SetDelegatedFrozenBandwidth(ownerObj.account.DelegatedFrozenBandwidth() - amount)
	ownerObj.markDirty()

	recvObj := s.getStateObject(receiver)
	if recvObj == nil {
		return
	}
	s.journalAccount(receiver, recvObj)
	v := recvObj.account.AcquiredDelegatedFrozenBandwidth() - amount
	if v < 0 {
		v = 0
	}
	recvObj.account.SetAcquiredDelegatedFrozenBandwidth(v)
	recvObj.markDirty()
}

func (s *StateDB) FreezeV1DelegatedEnergy(owner, receiver tcommon.Address, amount int64) {
	ownerObj := s.getStateObject(owner)
	if ownerObj == nil {
		return
	}
	s.journalAccount(owner, ownerObj)
	ownerObj.account.SetDelegatedFrozenEnergy(ownerObj.account.DelegatedFrozenEnergy() + amount)
	ownerObj.markDirty()

	recvObj := s.getStateObject(receiver)
	if recvObj == nil {
		return
	}
	s.journalAccount(receiver, recvObj)
	recvObj.account.SetAcquiredDelegatedFrozenEnergy(recvObj.account.AcquiredDelegatedFrozenEnergy() + amount)
	recvObj.markDirty()
}

func (s *StateDB) UnfreezeV1DelegatedEnergy(owner, receiver tcommon.Address, amount int64) {
	ownerObj := s.getStateObject(owner)
	if ownerObj == nil {
		return
	}
	s.journalAccount(owner, ownerObj)
	ownerObj.account.SetDelegatedFrozenEnergy(ownerObj.account.DelegatedFrozenEnergy() - amount)
	ownerObj.markDirty()

	recvObj := s.getStateObject(receiver)
	if recvObj == nil {
		return
	}
	s.journalAccount(receiver, recvObj)
	v := recvObj.account.AcquiredDelegatedFrozenEnergy() - amount
	if v < 0 {
		v = 0
	}
	recvObj.account.SetAcquiredDelegatedFrozenEnergy(v)
	recvObj.markDirty()
}

// GetStateObject returns the account for addr (nil if not found). Used by tests and later tasks.
func (s *StateDB) GetStateObject(addr tcommon.Address) *types.Account {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return nil
	}
	return obj.account
}

// GetWitness returns the witness at addr.
func (s *StateDB) GetWitness(addr tcommon.Address) *types.Witness {
	return s.witnesses[addr]
}

// PutWitness stores a witness, journaling the previous state for revert.
// The new record carries only the URL; counters reset to zero. Use
// SetWitnessURL when updating an existing witness so that VoteCount /
// production counters survive the URL change (java-tron parity).
func (s *StateDB) PutWitness(addr tcommon.Address, url string) {
	s.journalWitness(addr)
	s.witnesses[addr] = types.NewWitness(addr, url)
}

// SetWitnessURL updates the URL on the existing in-memory witness without
// resetting VoteCount / production counters. Mirrors java-tron's
// WitnessCapsule.setUrl semantics where only the URL field is mutated.
func (s *StateDB) SetWitnessURL(addr tcommon.Address, url string) {
	existing := s.witnesses[addr]
	if existing == nil {
		// No in-memory record — promote a fresh one. Caller is responsible
		// for ensuring counters are loaded separately if needed.
		s.journalWitness(addr)
		s.witnesses[addr] = types.NewWitness(addr, url)
		return
	}
	s.journalWitness(addr)
	existing.Proto().Url = url
}

// witnessFlushKV is the narrow capability FlushWitnesses needs. The block
// buffer (read+write layered store) and a plain ethdb.KeyValueStore both
// satisfy this.
type witnessFlushKV interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}

// FlushWitnesses persists the in-memory witness deltas (VoteCount, URL) onto
// rawdb-stored witness records via db. Called by applyBlock between
// ProcessBlock and ApplyBlockStatistics so VoteWitness / Unfreeze /
// WitnessUpdate effects on VoteCount and URL survive across blocks —
// StateDB.Commit only flushes accounts, never the witness map.
//
// Mirrors java-tron's pattern where VoteWitnessActuator writes to
// VotesStore and MaintenanceManager.countVote drains it into WitnessStore;
// the per-block merge here keeps the in-memory cache aligned with rawdb so
// the next block's pre-load sees the updated VoteCount.
func (s *StateDB) FlushWitnesses(db witnessFlushKV) {
	for addr, w := range s.witnesses {
		if w == nil {
			continue
		}
		stored := rawdb.ReadWitness(db, addr)
		if stored == nil {
			// Witness not yet persisted (e.g. WitnessCreateActuator
			// already wrote it via ctx.DB earlier in this block, OR a
			// new witness materialised purely in memory). Write the
			// in-memory record so its VoteCount/URL land — counters
			// default to 0, which ApplyBlockStatistics will populate
			// when the witness produces or misses.
			rawdb.WriteWitness(db, addr, w.Copy())
			continue
		}
		// Merge: only override fields the in-memory record owns.
		// TotalProduced / TotalMissed / LatestBlockNum / LatestSlotNum
		// are written by ApplyBlockStatistics on the same buffer and
		// must not be clobbered.
		stored.SetVoteCount(w.VoteCount())
		stored.Proto().Url = w.URL()
		rawdb.WriteWitness(db, addr, stored)
	}
}

// DynamicProperties returns the dynamic properties.
func (s *StateDB) DynamicProperties() *DynamicProperties {
	return s.dynProps
}

// SetDynamicProperties sets the dynamic properties (used during genesis setup).
func (s *StateDB) SetDynamicProperties(dp *DynamicProperties) {
	s.dynProps = dp
}

// Snapshot returns a snapshot ID for later revert.
func (s *StateDB) Snapshot() int {
	id := len(s.snapshots)
	s.snapshots = append(s.snapshots, s.journal.length())
	return id
}

// RevertToSnapshot reverts state changes to the given snapshot.
func (s *StateDB) RevertToSnapshot(id int) {
	if id >= len(s.snapshots) {
		return
	}
	journalLen := s.snapshots[id]
	s.journal.revert(s.stateObjects, s.witnesses, journalLen)
	s.snapshots = s.snapshots[:id]
}

// AccountExists returns whether an account exists (non-nil and not deleted).
func (s *StateDB) AccountExists(addr tcommon.Address) bool {
	obj := s.getStateObject(addr)
	return obj != nil && !obj.deleted
}

// CreateAccount creates a new account at addr with the given type.
// If the account already exists, it returns the existing account.
//
// NOTE: This entry point leaves Account.create_time at 0. New on-chain
// account-creation paths must use CreateAccountWithTime so the field mirrors
// java-tron's `dynamicStore.getLatestBlockHeaderTimestamp()`. This 2-arg form
// is retained for VM-internal call sites (slice 2c) and tests/genesis paths
// where create_time is irrelevant.
func (s *StateDB) CreateAccount(addr tcommon.Address, accountType corepb.AccountType) *types.Account {
	obj := s.GetOrCreateAccount(addr)
	s.journalAccount(addr, obj)
	obj.account.SetAccountType(accountType)
	obj.markDirty()
	return obj.account
}

// CreateAccountWithTime creates a new account at addr with the given type and
// stamps Account.create_time = createTime. Mirrors java-tron's AccountCapsule
// 5-arg constructor (AccountCapsule.java:158-180), which sets create_time on
// both the with-default-permission and without-default-permission branches —
// i.e. createTime is unconditional, independent of AllowMultiSign.
//
// Callers should pass `dp.LatestBlockHeaderTimestamp()` so the value matches
// java-tron's `dynamicStore.getLatestBlockHeaderTimestamp()`.
//
// This is the entry point for actuators creating new on-chain accounts
// (Transfer / TransferAsset / CreateAccount / ShieldedTransfer). Like
// CreateAccount, it overwrites type/create_time on an existing account, so
// callers must first gate on !AccountExists(addr) to preserve real stored
// values — every actuator call site already does this.
func (s *StateDB) CreateAccountWithTime(addr tcommon.Address, accountType corepb.AccountType, createTime int64) *types.Account {
	obj := s.GetOrCreateAccount(addr)
	s.journalAccount(addr, obj)
	obj.account.SetAccountType(accountType)
	obj.account.SetCreateTime(createTime)
	obj.markDirty()
	return obj.account
}

// ClearAcquiredDelegatedResource clears incoming delegated-resource fields.
// java-tron's CREATE2 collision path uses this when an existing account is
// upgraded to a contract account.
func (s *StateDB) ClearAcquiredDelegatedResource(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	pb := obj.account.Proto()
	pb.AcquiredDelegatedFrozenBalanceForBandwidth = 0
	pb.AcquiredDelegatedFrozenV2BalanceForBandwidth = 0
	if pb.AccountResource != nil {
		pb.AccountResource.AcquiredDelegatedFrozenBalanceForEnergy = 0
		pb.AccountResource.AcquiredDelegatedFrozenV2BalanceForEnergy = 0
	}
	obj.markDirty()
}

// IsWitness returns whether the account is marked as a witness.
func (s *StateDB) IsWitness(addr tcommon.Address) bool {
	obj := s.getStateObject(addr)
	if obj == nil {
		return false
	}
	return obj.account.IsWitness()
}

// SetIsWitness sets the witness flag on an account.
func (s *StateDB) SetIsWitness(addr tcommon.Address, isWitness bool) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetIsWitness(isWitness)
	obj.markDirty()
}

// GetFrozenV2Amount returns the frozen amount for a specific resource type.
func (s *StateDB) GetFrozenV2Amount(addr tcommon.Address, resourceType corepb.ResourceCode) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.GetFrozenV2Amount(resourceType)
}

// ReduceFreezeV2 reduces the frozen amount for a resource type.
func (s *StateDB) ReduceFreezeV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.ReduceFreezeV2(resourceType, amount)
	obj.markDirty()
}

// AddUnfreezeV2 adds a pending unfreeze entry with expiration time.
func (s *StateDB) AddUnfreezeV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount, expireTime int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.AddUnfreezeV2(resourceType, amount, expireTime)
	obj.markDirty()
}

// GetFreezeV1ExpireTime returns the expire time (ms) of the V1 frozen balance
// for the given resource type (0=BANDWIDTH, 1=ENERGY).
func (s *StateDB) GetFreezeV1ExpireTime(addr tcommon.Address, resourceType int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	switch resourceType {
	case 0: // BANDWIDTH: max expire time across Frozen list
		var maxExpire int64
		for _, f := range obj.account.FrozenBandwidthList() {
			if f.ExpireTime > maxExpire {
				maxExpire = f.ExpireTime
			}
		}
		return maxExpire
	case 1: // ENERGY
		return obj.account.FrozenEnergyExpireTime()
	}
	return 0
}

// CancelAllUnfreezeV2 moves all pending V2 unfreeze entries back to frozen
// and returns the total amount cancelled.
func (s *StateDB) CancelAllUnfreezeV2(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	entries := obj.account.UnfrozenV2()
	if len(entries) == 0 {
		return 0
	}
	s.journalAccount(addr, obj)
	var total int64
	for _, u := range entries {
		total += u.UnfreezeAmount
		obj.account.AddFreezeV2(u.Type, u.UnfreezeAmount)
	}
	obj.account.ClearUnfrozenV2()
	obj.markDirty()
	return total
}

// UnfreezeV2Count returns the number of pending unfreeze entries.
func (s *StateDB) UnfreezeV2Count(addr tcommon.Address) int {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return len(obj.account.UnfrozenV2())
}

// RemoveExpiredUnfreezeV2 removes expired entries and returns the total withdrawn.
func (s *StateDB) RemoveExpiredUnfreezeV2(addr tcommon.Address, now int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	// Check if any entries would expire before journaling.
	hasExpired := false
	for _, u := range obj.account.UnfrozenV2() {
		if u.UnfreezeExpireTime <= now {
			hasExpired = true
			break
		}
	}
	if !hasExpired {
		return 0
	}
	s.journalAccount(addr, obj)
	amount := obj.account.RemoveExpiredUnfreezeV2(now)
	obj.markDirty()
	return amount
}

// TotalFrozenV2 returns the total frozen balance across all resource types.
func (s *StateDB) TotalFrozenV2(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.TotalFrozenV2()
}

// GetLegacyTronPower returns the pre-AllowNewResourceModel voting power in drops.
func (s *StateDB) GetLegacyTronPower(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.LegacyTronPower()
}

// GetAllTronPower returns the AllowNewResourceModel voting power in drops.
func (s *StateDB) GetAllTronPower(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.AllTronPower()
}

// InitializeOldTronPowerIfNeeded snapshots LegacyTronPower into old_tron_power
// when the field is still uninitialized (== 0). No-op otherwise.
func (s *StateDB) InitializeOldTronPowerIfNeeded(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil || !obj.account.OldTronPowerIsNotInitialized() {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.InitializeOldTronPower()
	obj.markDirty()
}

// InvalidateOldTronPower sets old_tron_power to -1 (invalid), consuming the
// legacy snapshot. No-op if already invalid.
func (s *StateDB) InvalidateOldTronPower(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil || obj.account.OldTronPowerIsInvalid() {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.InvalidateOldTronPower()
	obj.markDirty()
}

// GetVotes returns the votes for an account.
func (s *StateDB) GetVotes(addr tcommon.Address) []*corepb.Vote {
	obj := s.getStateObject(addr)
	if obj == nil {
		return nil
	}
	return obj.account.Votes()
}

// SetVotes sets the vote list on an account.
func (s *StateDB) SetVotes(addr tcommon.Address, votes []*corepb.Vote) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetVotes(votes)
	obj.markDirty()
}

// ClearVotes clears all votes on an account.
func (s *StateDB) ClearVotes(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.ClearVotes()
	obj.markDirty()
}

// AddWitnessVoteCount adds delta to a witness's vote count.
func (s *StateDB) AddWitnessVoteCount(addr tcommon.Address, delta int64) {
	w := s.witnesses[addr]
	if w == nil {
		return
	}
	s.journalWitness(addr)
	w.SetVoteCount(w.VoteCount() + delta)
}

// GetAllowance returns the witness reward allowance.
func (s *StateDB) GetAllowance(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.Allowance()
}

// SetAllowance sets the witness reward allowance.
func (s *StateDB) SetAllowance(addr tcommon.Address, allowance int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetAllowance(allowance)
	obj.markDirty()
}

// AddAllowance adds amount to the witness reward allowance.
func (s *StateDB) AddAllowance(addr tcommon.Address, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetAllowance(obj.account.Allowance() + amount)
	obj.markDirty()
}

// GetLatestWithdrawTime returns the latest withdraw timestamp.
func (s *StateDB) GetLatestWithdrawTime(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.LatestWithdrawTime()
}

// SetLatestWithdrawTime sets the latest withdraw timestamp.
func (s *StateDB) SetLatestWithdrawTime(addr tcommon.Address, t int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetLatestWithdrawTime(t)
	obj.markDirty()
}

// GetNetUsage returns the net (bandwidth) usage for an account.
func (s *StateDB) GetNetUsage(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.NetUsage()
}

// SetNetUsage sets the net (bandwidth) usage for an account.
func (s *StateDB) SetNetUsage(addr tcommon.Address, usage int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetNetUsage(usage)
	obj.markDirty()
}

// GetLatestOperationTime returns the latest account operation timestamp.
func (s *StateDB) GetLatestOperationTime(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.LatestOperationTime()
}

// SetLatestOperationTime sets the latest account operation timestamp.
func (s *StateDB) SetLatestOperationTime(addr tcommon.Address, t int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetLatestOperationTime(t)
	obj.markDirty()
}

// GetLatestConsumeTime returns the latest consume time for an account.
func (s *StateDB) GetLatestConsumeTime(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.LatestConsumeTime()
}

// SetLatestConsumeTime sets the latest consume time for an account.
func (s *StateDB) SetLatestConsumeTime(addr tcommon.Address, t int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetLatestConsumeTime(t)
	obj.markDirty()
}

// GetFreeNetUsage returns the free net (bandwidth) usage for an account.
func (s *StateDB) GetFreeNetUsage(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.FreeNetUsage()
}

// SetFreeNetUsage sets the free net (bandwidth) usage for an account.
func (s *StateDB) SetFreeNetUsage(addr tcommon.Address, usage int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetFreeNetUsage(usage)
	obj.markDirty()
}

// GetLatestConsumeFreeTime returns the latest consume free time for an account.
func (s *StateDB) GetLatestConsumeFreeTime(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.LatestConsumeFreeTime()
}

// SetLatestConsumeFreeTime sets the latest consume free time for an account.
func (s *StateDB) SetLatestConsumeFreeTime(addr tcommon.Address, t int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetLatestConsumeFreeTime(t)
	obj.markDirty()
}

func (s *StateDB) GetFreeAssetNetUsage(addr tcommon.Address, key string) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.FreeAssetNetUsage(key)
}

func (s *StateDB) SetFreeAssetNetUsage(addr tcommon.Address, key string, usage int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetFreeAssetNetUsage(key, usage)
	obj.markDirty()
}

func (s *StateDB) GetFreeAssetNetUsageV2(addr tcommon.Address, key string) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.FreeAssetNetUsageV2(key)
}

func (s *StateDB) SetFreeAssetNetUsageV2(addr tcommon.Address, key string, usage int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetFreeAssetNetUsageV2(key, usage)
	obj.markDirty()
}

func (s *StateDB) GetLatestAssetOperationTime(addr tcommon.Address, key string) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.LatestAssetOperationTime(key)
}

func (s *StateDB) SetLatestAssetOperationTime(addr tcommon.Address, key string, t int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetLatestAssetOperationTime(key, t)
	obj.markDirty()
}

func (s *StateDB) GetLatestAssetOperationTimeV2(addr tcommon.Address, key string) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.LatestAssetOperationTimeV2(key)
}

func (s *StateDB) SetLatestAssetOperationTimeV2(addr tcommon.Address, key string, t int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetLatestAssetOperationTimeV2(key, t)
	obj.markDirty()
}

// GetEnergyUsage returns the energy usage for an account.
func (s *StateDB) GetEnergyUsage(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.EnergyUsage()
}

// SetEnergyUsage sets the energy usage for an account.
func (s *StateDB) SetEnergyUsage(addr tcommon.Address, usage int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetEnergyUsage(usage)
	obj.markDirty()
}

// GetLatestConsumeTimeForEnergy returns the latest energy consume time for an account.
func (s *StateDB) GetLatestConsumeTimeForEnergy(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.LatestConsumeTimeForEnergy()
}

// SetLatestConsumeTimeForEnergy sets the latest energy consume time for an account.
func (s *StateDB) SetLatestConsumeTimeForEnergy(addr tcommon.Address, t int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetLatestConsumeTimeForEnergy(t)
	obj.markDirty()
}

// --- Contract support ---

// GetCode returns the contract bytecode at addr.
func (s *StateDB) GetCode(addr tcommon.Address) []byte {
	obj := s.getStateObject(addr)
	if obj == nil {
		return nil
	}
	if len(obj.code) == 0 {
		code := rawdb.ReadCode(s.db.DiskDB(), addr)
		if len(code) > 0 {
			obj.code = code
			obj.codeHash = tcommon.Sha256(code)
		}
	}
	return obj.code
}

// SetCode sets the contract bytecode at addr. Creates the account if needed.
func (s *StateDB) SetCode(addr tcommon.Address, code []byte) {
	obj := s.GetOrCreateAccount(addr)
	s.journal.append(codeChange{
		address:  addr,
		prevCode: obj.code,
		prevHash: obj.codeHash,
	})
	obj.setCode(code)
}

// GetCodeSize returns the length of the contract bytecode.
func (s *StateDB) GetCodeSize(addr tcommon.Address) int {
	return len(s.GetCode(addr))
}

// GetCodeHash returns the SHA256 hash of the contract bytecode.
func (s *StateDB) GetCodeHash(addr tcommon.Address) tcommon.Hash {
	obj := s.getStateObject(addr)
	if obj == nil {
		return tcommon.Hash{}
	}
	return obj.codeHash
}

// GetState returns a storage value from a contract.
func (s *StateDB) GetState(addr tcommon.Address, key tcommon.Hash) tcommon.Hash {
	v, _ := s.GetStateWithExist(addr, key)
	return v
}

// GetStateWithExist returns a storage value and whether the java-tron
// StorageRow exists. A present zero row can exist inside the same transaction
// before commit; SSTORE energy accounting distinguishes that from a missing
// row even though both read as zero.
func (s *StateDB) GetStateWithExist(addr tcommon.Address, key tcommon.Hash) (tcommon.Hash, bool) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return tcommon.Hash{}, false
	}
	if v, ok := obj.storage[key]; ok {
		return v, true
	}
	if obj.created {
		return tcommon.Hash{}, false
	}
	// Load from persistent storage on cache miss.
	raw := rawdb.ReadStorage(s.db.DiskDB(), addr, key)
	if len(raw) == 0 {
		return tcommon.Hash{}, false
	}
	var h tcommon.Hash
	copy(h[len(h)-len(raw):], raw)
	if h == (tcommon.Hash{}) {
		return tcommon.Hash{}, false
	}
	obj.storage[key] = h
	return h, true
}

// SetState sets a storage value on a contract.
func (s *StateDB) SetState(addr tcommon.Address, key, value tcommon.Hash) {
	obj := s.GetOrCreateAccount(addr)
	prev := obj.getStorage(key)
	s.journal.append(storageChange{
		address: addr,
		key:     key,
		prev:    prev,
	})
	obj.setStorage(key, value)
}

// GetContract returns the contract metadata at addr.
func (s *StateDB) GetContract(addr tcommon.Address) *contractpb.SmartContract {
	obj := s.getStateObject(addr)
	if obj == nil {
		return nil
	}
	if obj.contractMeta == nil {
		data := rawdb.ReadContract(s.db.DiskDB(), addr)
		if len(data) > 0 {
			var sc contractpb.SmartContract
			if err := proto.Unmarshal(data, &sc); err == nil {
				obj.contractMeta = &sc
			}
		}
	}
	return obj.contractMeta
}

// SetContract stores contract metadata at addr.
func (s *StateDB) SetContract(addr tcommon.Address, contract *contractpb.SmartContract) {
	obj := s.GetOrCreateAccount(addr)
	// Clone prevMeta so the journal holds a snapshot of the pre-mutation state.
	// Callers often mutate the pointer returned by GetContract in-place and then
	// call SetContract with the same pointer; without cloning, prevMeta would
	// already reflect the mutation and RevertToSnapshot would be a no-op.
	var prevMeta *contractpb.SmartContract
	if obj.contractMeta != nil {
		prevMeta = proto.Clone(obj.contractMeta).(*contractpb.SmartContract)
	}
	s.journal.append(contractMetaChange{
		address:  addr,
		prevMeta: prevMeta,
	})
	obj.contractMeta = contract
	obj.contractMetaDirty = true
	obj.markDirty()
}

// IsContract returns whether the address has contract code or metadata.
func (s *StateDB) IsContract(addr tcommon.Address) bool {
	return s.GetContract(addr) != nil || len(s.GetCode(addr)) > 0
}

// Exist returns whether an account exists (non-nil and not deleted).
func (s *StateDB) Exist(addr tcommon.Address) bool {
	return s.AccountExists(addr)
}

// Empty returns whether an account is empty (no balance, no code).
func (s *StateDB) Empty(addr tcommon.Address) bool {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return true
	}
	return obj.account.Balance() == 0 && len(s.GetCode(addr)) == 0
}

// SelfDestruct marks an account as self-destructed.
func (s *StateDB) SelfDestruct(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journal.append(selfDestructChange{
		address: addr,
		prev:    obj.selfDestructed,
	})
	obj.markSelfDestructed()
}

// DeleteAccount removes an account from the account trie on commit.
func (s *StateDB) DeleteAccount(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.deleted = true
	obj.markDirty()
}

// HasSelfDestructed returns whether the account has been self-destructed.
func (s *StateDB) HasSelfDestructed(addr tcommon.Address) bool {
	obj := s.getStateObject(addr)
	if obj == nil {
		return false
	}
	return obj.selfDestructed
}

// Copy creates a deep copy of the StateDB for read-only execution.
func (s *StateDB) Copy() (*StateDB, error) {
	tr, err := s.db.OpenTrie(s.originRoot)
	if err != nil {
		return nil, err
	}
	cp := &StateDB{
		db:           s.db,
		trie:         tr,
		stateObjects: make(map[tcommon.Address]*stateObject),
		witnesses:    make(map[tcommon.Address]*types.Witness),
		journal:      newJournal(),
		dynProps:     s.dynProps,
		originRoot:   s.originRoot,
	}
	for addr, obj := range s.stateObjects {
		var metaCopy *contractpb.SmartContract
		if obj.contractMeta != nil {
			metaCopy = proto.Clone(obj.contractMeta).(*contractpb.SmartContract)
		}
		newObj := &stateObject{
			address:           addr,
			dirty:             obj.dirty,
			deleted:           obj.deleted,
			created:           obj.created,
			code:              append([]byte{}, obj.code...),
			codeHash:          obj.codeHash,
			codeDirty:         obj.codeDirty,
			contractMeta:      metaCopy,
			contractMetaDirty: obj.contractMetaDirty,
			storage:           make(map[tcommon.Hash]tcommon.Hash),
			selfDestructed:    obj.selfDestructed,
		}
		if obj.account != nil {
			data, _ := obj.account.Marshal()
			acc, _ := types.UnmarshalAccount(data)
			newObj.account = acc
		}
		for k, v := range obj.storage {
			newObj.storage[k] = v
		}
		cp.stateObjects[addr] = newObj
	}
	return cp, nil
}

// Commit writes all dirty accounts to the MPT and returns the new root hash.
func (s *StateDB) Commit() (tcommon.Hash, error) {
	for addr, obj := range s.stateObjects {
		if !obj.dirty {
			continue
		}
		if obj.deleted {
			if err := s.trie.Delete(trieKey(addr)); err != nil {
				return tcommon.Hash{}, err
			}
			obj.dirty = false
			continue
		}
		data, err := obj.account.Marshal()
		if err != nil {
			return tcommon.Hash{}, err
		}
		if err := s.trie.Update(trieKey(addr), data); err != nil {
			return tcommon.Hash{}, err
		}
		if obj.codeDirty {
			rawdb.WriteCode(s.db.DiskDB(), addr, obj.code)
			obj.codeDirty = false
		}
		if obj.contractMetaDirty && obj.contractMeta != nil {
			metaBytes, err := proto.Marshal(obj.contractMeta)
			if err != nil {
				return tcommon.Hash{}, fmt.Errorf("marshal contractMeta for %s: %w", addr.Hex(), err)
			}
			rawdb.WriteContract(s.db.DiskDB(), addr, metaBytes)
			obj.contractMetaDirty = false
		}
		for k, v := range obj.storage {
			if v == (tcommon.Hash{}) {
				rawdb.DeleteStorage(s.db.DiskDB(), addr, k)
			} else {
				rawdb.WriteStorage(s.db.DiskDB(), addr, k, v.Bytes())
			}
		}
		obj.created = false
		obj.dirty = false
	}

	root, nodes := s.trie.Commit(false)
	if nodes != nil {
		if err := s.db.TrieDB().Update(root, s.originRoot, 0, trienode.NewWithNodeSet(nodes), nil); err != nil {
			return tcommon.Hash{}, err
		}
		if err := s.db.TrieDB().Commit(root, false); err != nil {
			return tcommon.Hash{}, err
		}
	}

	newTrie, err := s.db.OpenTrie(root)
	if err != nil {
		return tcommon.Hash{}, err
	}
	s.trie = newTrie
	s.originRoot = root
	s.journal = newJournal()
	s.snapshots = s.snapshots[:0]

	return tcommon.Hash(root), nil
}

// SetAccountName sets the account name.
func (s *StateDB) SetAccountName(addr tcommon.Address, name string) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetAccountName(name)
	obj.markDirty()
}

// GetAccountName returns the account name.
func (s *StateDB) GetAccountName(addr tcommon.Address) string {
	obj := s.getStateObject(addr)
	if obj == nil {
		return ""
	}
	return obj.account.AccountName()
}

// SetAccountId sets the account ID.
func (s *StateDB) SetAccountId(addr tcommon.Address, id string) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetAccountId(id)
	obj.markDirty()
}

// GetAccountId returns the account ID.
func (s *StateDB) GetAccountId(addr tcommon.Address) string {
	obj := s.getStateObject(addr)
	if obj == nil {
		return ""
	}
	return obj.account.AccountId()
}

// SetPermissions sets all permissions on the account.
func (s *StateDB) SetPermissions(addr tcommon.Address, owner, witness *corepb.Permission, actives []*corepb.Permission) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetOwnerPermission(owner)
	obj.account.SetWitnessPermission(witness)
	obj.account.SetActivePermission(actives)
	obj.markDirty()
}

// ApplyDefaultAccountPermissions installs the default Owner permission and a
// default Active[0] permission whose operations bitmap is loaded from
// dp.ActiveDefaultOperations(). Mirrors java-tron AccountCapsule's
// `withDefaultPermission=true` constructor branch (createDefaultOwnerPermission
// + createDefaultActivePermission). The caller is responsible for the
// AllowMultiSign gate. No-op if the account does not exist.
//
// Note: this OVERWRITES any existing Owner / Active permissions; intended use
// is immediately after StateDB.CreateAccount on a freshly-minted account.
func (s *StateDB) ApplyDefaultAccountPermissions(addr tcommon.Address, dp *DynamicProperties) {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return
	}
	s.journalAccount(addr, obj)
	owner := types.MakeDefaultOwnerPermission(addr)
	active := types.MakeDefaultActivePermission(addr, dp.ActiveDefaultOperations())
	obj.account.SetOwnerPermission(owner)
	obj.account.SetActivePermission([]*corepb.Permission{active})
	obj.markDirty()
}

// ApplyWitnessPermissions installs the witness permission on addr and
// back-fills default Owner / Active[0] only if they are missing. Mirrors
// java-tron AccountCapsule.setDefaultWitnessPermission. The caller is
// responsible for the AllowMultiSign gate. No-op if the account does not
// exist.
//
// Conditional semantics (java-tron parity):
//   - Witness permission is ALWAYS set/overwritten (default shape).
//   - Owner permission is only set if account.OwnerPermission() == nil.
//   - Active[0] is only appended if len(account.ActivePermission()) == 0.
//
// This preserves any custom Owner/Active permissions the account installed
// via AccountPermissionUpdate before being upgraded to a witness.
func (s *StateDB) ApplyWitnessPermissions(addr tcommon.Address, dp *DynamicProperties) {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return
	}
	s.journalAccount(addr, obj)
	// Witness: unconditional (overwrite if any).
	obj.account.SetWitnessPermission(types.MakeDefaultWitnessPermission(addr))
	// Owner: only fill if missing.
	if obj.account.OwnerPermission() == nil {
		obj.account.SetOwnerPermission(types.MakeDefaultOwnerPermission(addr))
	}
	// Active: only fill if list is empty.
	if len(obj.account.ActivePermission()) == 0 {
		active := types.MakeDefaultActivePermission(addr, dp.ActiveDefaultOperations())
		obj.account.SetActivePermission([]*corepb.Permission{active})
	}
	obj.markDirty()
}

// GetDelegatedFrozenV2 returns the delegated (outgoing) frozen balance for a resource type.
func (s *StateDB) GetDelegatedFrozenV2(addr tcommon.Address, resourceType corepb.ResourceCode) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	if resourceType == corepb.ResourceCode_BANDWIDTH {
		return obj.account.DelegatedFrozenV2BalanceForBandwidth()
	}
	return obj.account.DelegatedFrozenV2BalanceForEnergy()
}

// AddDelegatedFrozenV2 adds to the delegated (outgoing) frozen balance for a resource.
func (s *StateDB) AddDelegatedFrozenV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	if resourceType == corepb.ResourceCode_BANDWIDTH {
		obj.account.SetDelegatedFrozenV2BalanceForBandwidth(obj.account.DelegatedFrozenV2BalanceForBandwidth() + amount)
	} else {
		obj.account.SetDelegatedFrozenV2BalanceForEnergy(obj.account.DelegatedFrozenV2BalanceForEnergy() + amount)
	}
	obj.markDirty()
}

// SubDelegatedFrozenV2 subtracts from the delegated (outgoing) frozen balance for a resource.
func (s *StateDB) SubDelegatedFrozenV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	if resourceType == corepb.ResourceCode_BANDWIDTH {
		v := obj.account.DelegatedFrozenV2BalanceForBandwidth() - amount
		if v < 0 {
			v = 0
		}
		obj.account.SetDelegatedFrozenV2BalanceForBandwidth(v)
	} else {
		v := obj.account.DelegatedFrozenV2BalanceForEnergy() - amount
		if v < 0 {
			v = 0
		}
		obj.account.SetDelegatedFrozenV2BalanceForEnergy(v)
	}
	obj.markDirty()
}

// AddAcquiredDelegatedFrozenV2 adds to the acquired (incoming) delegated frozen balance.
func (s *StateDB) AddAcquiredDelegatedFrozenV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	if resourceType == corepb.ResourceCode_BANDWIDTH {
		obj.account.SetAcquiredDelegatedFrozenV2BalanceForBandwidth(obj.account.AcquiredDelegatedFrozenV2BalanceForBandwidth() + amount)
	} else {
		obj.account.SetAcquiredDelegatedFrozenV2BalanceForEnergy(obj.account.AcquiredDelegatedFrozenV2BalanceForEnergy() + amount)
	}
	obj.markDirty()
}

// SubAcquiredDelegatedFrozenV2 subtracts from the acquired (incoming) delegated frozen balance.
func (s *StateDB) SubAcquiredDelegatedFrozenV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	if resourceType == corepb.ResourceCode_BANDWIDTH {
		v := obj.account.AcquiredDelegatedFrozenV2BalanceForBandwidth() - amount
		if v < 0 {
			v = 0
		}
		obj.account.SetAcquiredDelegatedFrozenV2BalanceForBandwidth(v)
	} else {
		v := obj.account.AcquiredDelegatedFrozenV2BalanceForEnergy() - amount
		if v < 0 {
			v = 0
		}
		obj.account.SetAcquiredDelegatedFrozenV2BalanceForEnergy(v)
	}
	obj.markDirty()
}

// ClearUnfrozenV2 removes all pending unfreeze entries.
func (s *StateDB) ClearUnfrozenV2(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.ClearUnfrozenV2()
	obj.markDirty()
}

// getStateObject returns the state object for addr, loading from trie if needed.
func (s *StateDB) getStateObject(addr tcommon.Address) *stateObject {
	if obj, ok := s.stateObjects[addr]; ok {
		return obj
	}
	data, err := s.trie.Get(trieKey(addr))
	if err != nil || data == nil {
		return nil
	}
	acc, err := types.UnmarshalAccount(data)
	if err != nil {
		return nil
	}
	obj := newStateObject(addr, acc)
	s.stateObjects[addr] = obj
	return obj
}

// journalAccount records the current state of an account for revert.
func (s *StateDB) journalAccount(addr tcommon.Address, obj *stateObject) {
	var prev []byte
	if obj != nil && obj.account != nil {
		prev, _ = obj.account.Marshal()
	}
	s.journal.append(accountChange{
		address: addr,
		prev:    prev,
	})
}

// journalWitness records the current witness state for revert.
func (s *StateDB) journalWitness(addr tcommon.Address) {
	existing := s.witnesses[addr]
	var prev *types.Witness
	if existing != nil {
		prev = existing.Copy()
	}
	s.journal.append(witnessChange{
		address: addr,
		prev:    prev,
	})
}

// trieKey returns the MPT key for a TRON address: Keccak256(address).
func trieKey(addr tcommon.Address) []byte {
	return crypto.Keccak256(addr.Bytes())
}

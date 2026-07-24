package types

import (
	"sync/atomic"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

type Account struct {
	pb *corepb.Account
	// marshalSizeHint is the previous deterministic encoding length. Account
	// state changes frequently but its encoded size usually does not; using the
	// last length lets Marshal call the generated append fast-path directly and
	// skip protobuf's full Size pre-pass without repeated buffer growth.
	marshalSizeHint atomic.Int64
}

func NewAccountFromPB(pb *corepb.Account) *Account {
	return &Account{pb: pb}
}

func NewAccount(addr common.Address, accType corepb.AccountType) *Account {
	return &Account{
		pb: &corepb.Account{
			Address: addr.Bytes(),
			Type:    accType,
		},
	}
}

func (a *Account) Proto() *corepb.Account              { return a.pb }
func (a *Account) Address() common.Address             { return common.BytesToAddress(a.pb.Address) }
func (a *Account) Balance() int64                      { return a.pb.Balance }
func (a *Account) SetBalance(b int64)                  { a.pb.Balance = b }
func (a *Account) Type() corepb.AccountType            { return a.pb.Type }
func (a *Account) SetAccountType(t corepb.AccountType) { a.pb.Type = t }
func (a *Account) IsWitness() bool                     { return a.pb.IsWitness }
func (a *Account) SetIsWitness(v bool)                 { a.pb.IsWitness = v }
func (a *Account) CreateTime() int64                   { return a.pb.CreateTime }
func (a *Account) SetCreateTime(t int64)               { a.pb.CreateTime = t }

func (a *Account) Copy() *Account {
	if a == nil {
		return nil
	}
	copy := &Account{pb: proto.Clone(a.pb).(*corepb.Account)}
	copy.marshalSizeHint.Store(a.marshalSizeHint.Load())
	return copy
}

// AccountName accessors.
func (a *Account) AccountName() string        { return string(a.pb.AccountName) }
func (a *Account) SetAccountName(name string) { a.pb.AccountName = []byte(name) }

// FrozenV2 returns all FreezeV2 entries.
func (a *Account) FrozenV2() []*corepb.Account_FreezeV2 {
	return a.pb.FrozenV2
}

// AddFreezeV2 adds amount to the existing entry for resourceType, or appends a new entry.
func (a *Account) AddFreezeV2(resourceType corepb.ResourceCode, amount int64) {
	for _, f := range a.pb.FrozenV2 {
		if f.Type == resourceType {
			f.Amount += amount
			return
		}
	}
	a.pb.FrozenV2 = append(a.pb.FrozenV2, &corepb.Account_FreezeV2{
		Type:   resourceType,
		Amount: amount,
	})
}

// ReduceFreezeV2 reduces the frozen amount for resourceType, floored at 0.
func (a *Account) ReduceFreezeV2(resourceType corepb.ResourceCode, amount int64) {
	for _, f := range a.pb.FrozenV2 {
		if f.Type == resourceType {
			f.Amount -= amount
			if f.Amount < 0 {
				f.Amount = 0
			}
			return
		}
	}
}

// GetFrozenV2Amount returns the frozen amount for the given resource type.
func (a *Account) GetFrozenV2Amount(resourceType corepb.ResourceCode) int64 {
	for _, f := range a.pb.FrozenV2 {
		if f.Type == resourceType {
			return f.Amount
		}
	}
	return 0
}

// TotalFrozenV2 returns the sum of all frozen amounts.
func (a *Account) TotalFrozenV2() int64 {
	var total int64
	for _, f := range a.pb.FrozenV2 {
		total += f.Amount
	}
	return total
}

// OldTronPower returns the old_tron_power field: 0=uninitialized, -1=invalid, >0=snapshot.
func (a *Account) OldTronPower() int64 { return a.pb.OldTronPower }

// SetOldTronPower sets the old_tron_power field directly.
func (a *Account) SetOldTronPower(v int64) { a.pb.OldTronPower = v }

// OldTronPowerIsNotInitialized reports whether the field has not been set yet (== 0).
func (a *Account) OldTronPowerIsNotInitialized() bool { return a.pb.OldTronPower == 0 }

// OldTronPowerIsInvalid reports whether the field is marked invalid (== -1).
func (a *Account) OldTronPowerIsInvalid() bool { return a.pb.OldTronPower == -1 }

// V1TronPowerFrozen returns the V1 explicit tron_power-typed frozen balance (proto field 47).
func (a *Account) V1TronPowerFrozen() int64 {
	if a.pb.TronPower == nil {
		return 0
	}
	return a.pb.TronPower.FrozenBalance
}

func (a *Account) V1TronPowerExpireTime() int64 {
	if a.pb.TronPower == nil {
		return 0
	}
	return a.pb.TronPower.ExpireTime
}

func (a *Account) AddV1TronPower(amount, expireTimeMs int64) {
	if a.pb.TronPower == nil {
		a.pb.TronPower = &corepb.Account_Frozen{
			FrozenBalance: amount,
			ExpireTime:    expireTimeMs,
		}
		return
	}
	a.pb.TronPower.FrozenBalance += amount
	if expireTimeMs > a.pb.TronPower.ExpireTime {
		a.pb.TronPower.ExpireTime = expireTimeMs
	}
}

func (a *Account) ClearV1TronPower() int64 {
	amount := a.V1TronPowerFrozen()
	a.pb.TronPower = nil
	return amount
}

// V2TronPowerFrozen returns the V2 TRON_POWER-resource-typed frozen balance.
func (a *Account) V2TronPowerFrozen() int64 {
	for _, f := range a.pb.FrozenV2 {
		if f.Type == corepb.ResourceCode_TRON_POWER {
			return f.Amount
		}
	}
	return 0
}

// LegacyTronPower returns voting power using the pre-AllowNewResourceModel model:
// V1 frozen (bandwidth + energy + delegated) + non-TRON_POWER V2 frozen + V2 delegated.
// Mirrors java-tron AccountCapsule.getTronPower().
func (a *Account) LegacyTronPower() int64 {
	var tp int64
	tp += a.TotalFrozenBandwidth()
	tp += a.FrozenEnergyAmount()
	tp += a.DelegatedFrozenBandwidth()
	tp += a.DelegatedFrozenEnergy()
	for _, f := range a.pb.FrozenV2 {
		if f.Type != corepb.ResourceCode_TRON_POWER {
			tp += f.Amount
		}
	}
	tp += a.DelegatedFrozenV2BalanceForBandwidth()
	tp += a.DelegatedFrozenV2BalanceForEnergy()
	return tp
}

// AllTronPower returns voting power using the AllowNewResourceModel model.
// The old_tron_power field controls which legacy amount to credit:
//   - 0 (uninitialized): use LegacyTronPower() live
//   - -1 (invalid): legacy contribution is zero
//   - >0 (snapshot): use the snapshotted value
//
// Either way, explicit V1 and V2 TRON_POWER-typed frozen are always added.
// Mirrors java-tron AccountCapsule.getAllTronPower().
func (a *Account) AllTronPower() int64 {
	v1tp := a.V1TronPowerFrozen()
	v2tp := a.V2TronPowerFrozen()
	switch {
	case a.pb.OldTronPower == -1:
		return v1tp + v2tp
	case a.pb.OldTronPower == 0:
		return a.LegacyTronPower() + v1tp + v2tp
	default:
		return a.pb.OldTronPower + v1tp + v2tp
	}
}

// TronPowerUsage returns the Tron Power the account has spent voting (the sum
// of its vote counts), mirroring java-tron AccountCapsule.getTronPowerUsage().
func (a *Account) TronPowerUsage() int64 {
	var total int64
	for _, v := range a.pb.Votes {
		total += v.GetVoteCount()
	}
	return total
}

// StorageLimit returns the account's exchanged-storage byte limit.
func (a *Account) StorageLimit() int64 { return a.pb.GetAccountResource().GetStorageLimit() }

// StorageUsage returns the account's consumed exchanged storage in bytes.
func (a *Account) StorageUsage() int64 { return a.pb.GetAccountResource().GetStorageUsage() }

// InitializeOldTronPower snapshots the current LegacyTronPower into old_tron_power,
// or sets -1 if the legacy power is zero. Mirrors AccountCapsule.initializeOldTronPower().
func (a *Account) InitializeOldTronPower() {
	value := a.LegacyTronPower()
	if value == 0 {
		value = -1
	}
	a.pb.OldTronPower = value
}

// InvalidateOldTronPower marks old_tron_power as -1 (invalid).
// Called after any V2 unfreeze to consume the legacy snapshot.
func (a *Account) InvalidateOldTronPower() {
	a.pb.OldTronPower = -1
}

// UnfrozenV2 returns all UnFreezeV2 entries.
func (a *Account) UnfrozenV2() []*corepb.Account_UnFreezeV2 {
	return a.pb.UnfrozenV2
}

// AddUnfreezeV2 appends a new unfreeze entry.
func (a *Account) AddUnfreezeV2(resourceType corepb.ResourceCode, amount int64, expireTime int64) {
	a.pb.UnfrozenV2 = append(a.pb.UnfrozenV2, &corepb.Account_UnFreezeV2{
		Type:               resourceType,
		UnfreezeAmount:     amount,
		UnfreezeExpireTime: expireTime,
	})
}

// RemoveExpiredUnfreezeV2 removes entries with expireTime <= now and returns the total withdrawn.
func (a *Account) RemoveExpiredUnfreezeV2(now int64) int64 {
	var withdrawn int64
	remaining := a.pb.UnfrozenV2[:0]
	for _, u := range a.pb.UnfrozenV2 {
		if u.UnfreezeExpireTime <= now {
			withdrawn += u.UnfreezeAmount
		} else {
			remaining = append(remaining, u)
		}
	}
	a.pb.UnfrozenV2 = remaining
	return withdrawn
}

// Votes accessors.
func (a *Account) Votes() []*corepb.Vote         { return a.pb.Votes }
func (a *Account) SetVotes(votes []*corepb.Vote) { a.pb.Votes = votes }
func (a *Account) ClearVotes()                   { a.pb.Votes = nil }

// Bandwidth resource tracking.
func (a *Account) NetUsage() int64                  { return a.pb.NetUsage }
func (a *Account) SetNetUsage(v int64)              { a.pb.NetUsage = v }
func (a *Account) LatestOperationTime() int64       { return a.pb.LatestOprationTime }
func (a *Account) SetLatestOperationTime(t int64)   { a.pb.LatestOprationTime = t }
func (a *Account) LatestConsumeTime() int64         { return a.pb.LatestConsumeTime }
func (a *Account) SetLatestConsumeTime(t int64)     { a.pb.LatestConsumeTime = t }
func (a *Account) FreeNetUsage() int64              { return a.pb.FreeNetUsage }
func (a *Account) SetFreeNetUsage(v int64)          { a.pb.FreeNetUsage = v }
func (a *Account) LatestConsumeFreeTime() int64     { return a.pb.LatestConsumeFreeTime }
func (a *Account) SetLatestConsumeFreeTime(t int64) { a.pb.LatestConsumeFreeTime = t }

func (a *Account) FreeAssetNetUsage(key string) int64 {
	return a.pb.GetFreeAssetNetUsage()[key]
}
func (a *Account) SetFreeAssetNetUsage(key string, v int64) {
	if a.pb.FreeAssetNetUsage == nil {
		a.pb.FreeAssetNetUsage = make(map[string]int64)
	}
	a.pb.FreeAssetNetUsage[key] = v
}
func (a *Account) FreeAssetNetUsageV2(key string) int64 {
	return a.pb.GetFreeAssetNetUsageV2()[key]
}
func (a *Account) SetFreeAssetNetUsageV2(key string, v int64) {
	if a.pb.FreeAssetNetUsageV2 == nil {
		a.pb.FreeAssetNetUsageV2 = make(map[string]int64)
	}
	a.pb.FreeAssetNetUsageV2[key] = v
}
func (a *Account) LatestAssetOperationTime(key string) int64 {
	return a.pb.GetLatestAssetOperationTime()[key]
}
func (a *Account) SetLatestAssetOperationTime(key string, t int64) {
	if a.pb.LatestAssetOperationTime == nil {
		a.pb.LatestAssetOperationTime = make(map[string]int64)
	}
	a.pb.LatestAssetOperationTime[key] = t
}
func (a *Account) LatestAssetOperationTimeV2(key string) int64 {
	return a.pb.GetLatestAssetOperationTimeV2()[key]
}
func (a *Account) SetLatestAssetOperationTimeV2(key string, t int64) {
	if a.pb.LatestAssetOperationTimeV2 == nil {
		a.pb.LatestAssetOperationTimeV2 = make(map[string]int64)
	}
	a.pb.LatestAssetOperationTimeV2[key] = t
}

// ensureAccountResource creates the AccountResource sub-message if it is nil.
func (a *Account) ensureAccountResource() {
	if a.pb.AccountResource == nil {
		a.pb.AccountResource = &corepb.Account_AccountResource{}
	}
}

// Energy resource tracking.
func (a *Account) EnergyUsage() int64 {
	if a.pb.AccountResource == nil {
		return 0
	}
	return a.pb.AccountResource.EnergyUsage
}

func (a *Account) SetEnergyUsage(v int64) {
	a.ensureAccountResource()
	a.pb.AccountResource.EnergyUsage = v
}

func (a *Account) LatestConsumeTimeForEnergy() int64 {
	if a.pb.AccountResource == nil {
		return 0
	}
	return a.pb.AccountResource.LatestConsumeTimeForEnergy
}

func (a *Account) SetLatestConsumeTimeForEnergy(t int64) {
	a.ensureAccountResource()
	a.pb.AccountResource.LatestConsumeTimeForEnergy = t
}

// Per-account energy recovery window. Mirrors java-tron AccountCapsule's
// getWindowSize / getWindowSizeV2 / setNewWindowSize / setNewWindowSizeV2 for
// the ENERGY resource. The raw field is energy_window_size; when
// energy_window_optimized is set the raw value is stored in V2 units
// (slots * WindowSizePrecision).

// RawEnergyWindowSize returns the stored field verbatim (0 when never written).
func (a *Account) RawEnergyWindowSize() int64 {
	return a.pb.GetAccountResource().GetEnergyWindowSize()
}

// EnergyWindowOptimized reports whether the window is stored in V2 units.
func (a *Account) EnergyWindowOptimized() bool {
	return a.pb.GetAccountResource().GetEnergyWindowOptimized()
}

// EnergyWindowSize returns the window in slots (java getWindowSize(ENERGY)).
func (a *Account) EnergyWindowSize() int64 {
	raw := a.pb.GetAccountResource().GetEnergyWindowSize()
	if raw == 0 {
		return params.WindowSizeSlots
	}
	if a.pb.GetAccountResource().GetEnergyWindowOptimized() {
		if raw < params.WindowSizePrecision {
			return params.WindowSizeSlots
		}
		return raw / params.WindowSizePrecision
	}
	return raw
}

// EnergyWindowSizeV2 returns the window in V2 units (java getWindowSizeV2(ENERGY)).
func (a *Account) EnergyWindowSizeV2() int64 {
	raw := a.pb.GetAccountResource().GetEnergyWindowSize()
	if raw == 0 {
		return params.WindowSizeSlots * params.WindowSizePrecision
	}
	if a.pb.GetAccountResource().GetEnergyWindowOptimized() {
		return raw
	}
	return raw * params.WindowSizePrecision
}

// SetNewEnergyWindowSize sets the raw window (V1 form; leaves the optimized
// flag untouched). Mirrors setNewWindowSize(ENERGY, v).
func (a *Account) SetNewEnergyWindowSize(v int64) {
	a.ensureAccountResource()
	a.pb.AccountResource.EnergyWindowSize = v
}

// SetNewEnergyWindowSizeV2 sets the raw window and marks it optimized (V2).
// Mirrors setNewWindowSizeV2(ENERGY, v).
func (a *Account) SetNewEnergyWindowSizeV2(v int64) {
	a.ensureAccountResource()
	a.pb.AccountResource.EnergyWindowSize = v
	a.pb.AccountResource.EnergyWindowOptimized = true
}

// SetEnergyWindow sets the raw window and optimized flag together (used by the
// StateDB setter to persist a computed window).
func (a *Account) SetEnergyWindow(raw int64, optimized bool) {
	a.ensureAccountResource()
	a.pb.AccountResource.EnergyWindowSize = raw
	a.pb.AccountResource.EnergyWindowOptimized = optimized
}

// Per-account bandwidth (NET) recovery window. Mirrors java-tron AccountCapsule's
// getWindowSize / getWindowSizeV2 / setNewWindowSize / setNewWindowSizeV2 for the
// BANDWIDTH resource. The raw field is net_window_size (a top-level Account field,
// unlike energy_window_size which lives under AccountResource); when
// net_window_optimized is set the raw value is stored in V2 units
// (slots * WindowSizePrecision).

// RawNetWindowSize returns the stored field verbatim (0 when never written).
func (a *Account) RawNetWindowSize() int64 {
	return a.pb.GetNetWindowSize()
}

// NetWindowOptimized reports whether the window is stored in V2 units.
func (a *Account) NetWindowOptimized() bool {
	return a.pb.GetNetWindowOptimized()
}

// NetWindowSize returns the window in slots (java getWindowSize(BANDWIDTH)).
func (a *Account) NetWindowSize() int64 {
	raw := a.pb.GetNetWindowSize()
	if raw == 0 {
		return params.WindowSizeSlots
	}
	if a.pb.GetNetWindowOptimized() {
		if raw < params.WindowSizePrecision {
			return params.WindowSizeSlots
		}
		return raw / params.WindowSizePrecision
	}
	return raw
}

// NetWindowSizeV2 returns the window in V2 units (java getWindowSizeV2(BANDWIDTH)).
func (a *Account) NetWindowSizeV2() int64 {
	raw := a.pb.GetNetWindowSize()
	if raw == 0 {
		return params.WindowSizeSlots * params.WindowSizePrecision
	}
	if a.pb.GetNetWindowOptimized() {
		return raw
	}
	return raw * params.WindowSizePrecision
}

// SetNewNetWindowSize sets the raw window (V1 form; leaves the optimized flag
// untouched). Mirrors setNewWindowSize(BANDWIDTH, v).
func (a *Account) SetNewNetWindowSize(v int64) {
	a.pb.NetWindowSize = v
}

// SetNewNetWindowSizeV2 sets the raw window and marks it optimized (V2).
// Mirrors setNewWindowSizeV2(BANDWIDTH, v).
func (a *Account) SetNewNetWindowSizeV2(v int64) {
	a.pb.NetWindowSize = v
	a.pb.NetWindowOptimized = true
}

// SetNetWindow sets the raw window and optimized flag together (used by the
// StateDB setter to persist a computed window).
func (a *Account) SetNetWindow(raw int64, optimized bool) {
	a.pb.NetWindowSize = raw
	a.pb.NetWindowOptimized = optimized
}

// Allowance (witness rewards) accessors.
func (a *Account) Allowance() int64              { return a.pb.Allowance }
func (a *Account) SetAllowance(v int64)          { a.pb.Allowance = v }
func (a *Account) LatestWithdrawTime() int64     { return a.pb.LatestWithdrawTime }
func (a *Account) SetLatestWithdrawTime(t int64) { a.pb.LatestWithdrawTime = t }

// AccountId accessors.
func (a *Account) AccountId() string      { return string(a.pb.AccountId) }
func (a *Account) SetAccountId(id string) { a.pb.AccountId = []byte(id) }

// Permission accessors.
func (a *Account) OwnerPermission() *corepb.Permission            { return a.pb.OwnerPermission }
func (a *Account) WitnessPermission() *corepb.Permission          { return a.pb.WitnessPermission }
func (a *Account) ActivePermission() []*corepb.Permission         { return a.pb.ActivePermission }
func (a *Account) SetOwnerPermission(p *corepb.Permission)        { a.pb.OwnerPermission = p }
func (a *Account) SetWitnessPermission(p *corepb.Permission)      { a.pb.WitnessPermission = p }
func (a *Account) SetActivePermission(perms []*corepb.Permission) { a.pb.ActivePermission = perms }

// WitnessPermissionAddress returns the address authorized to sign blocks on
// this account's behalf: the first key of the witness permission, or the
// account's own address when no witness permission is set. Mirrors java-tron
// AccountCapsule.getWitnessPermissionAddress, which block signature validation
// consults when AllowMultiSign is active so a witness may delegate block
// signing to a separate key.
func (a *Account) WitnessPermissionAddress() common.Address {
	wp := a.pb.WitnessPermission
	if wp == nil || len(wp.Keys) == 0 {
		return a.Address()
	}
	return common.BytesToAddress(wp.Keys[0].Address)
}

// MakeDefaultOwnerPermission builds the default Owner permission for addr:
// type=Owner, id=0, name="owner", threshold=1, parent_id=0, single key
// (addr, weight=1), no operations bitmap. Mirrors java-tron
// AccountCapsule.createDefaultOwnerPermission.
func MakeDefaultOwnerPermission(addr common.Address) *corepb.Permission {
	return &corepb.Permission{
		Type:           corepb.Permission_Owner,
		Id:             0,
		PermissionName: "owner",
		Threshold:      1,
		ParentId:       0,
		Keys: []*corepb.Key{
			{Address: addr.Bytes(), Weight: 1},
		},
	}
}

// MakeDefaultActivePermission builds the default Active permission for addr,
// loading the operations bitmap from activeDefaultOps. Mirrors java-tron
// AccountCapsule.createDefaultActivePermission. The returned permission has
// type=Active, id=2, name="active", threshold=1, parent_id=0, a single key
// (addr, weight=1), and a defensive copy of activeDefaultOps as its
// operations bitmap.
func MakeDefaultActivePermission(addr common.Address, activeDefaultOps []byte) *corepb.Permission {
	ops := make([]byte, len(activeDefaultOps))
	copy(ops, activeDefaultOps)
	return &corepb.Permission{
		Type:           corepb.Permission_Active,
		Id:             2,
		PermissionName: "active",
		Threshold:      1,
		ParentId:       0,
		Operations:     ops,
		Keys: []*corepb.Key{
			{Address: addr.Bytes(), Weight: 1},
		},
	}
}

// MakeDefaultWitnessPermission builds the default Witness permission for addr:
// type=Witness, id=1, name="witness", threshold=1, parent_id=0, single key
// (addr, weight=1), no operations bitmap. Mirrors java-tron
// AccountCapsule.createDefaultWitnessPermission.
func MakeDefaultWitnessPermission(addr common.Address) *corepb.Permission {
	return &corepb.Permission{
		Type:           corepb.Permission_Witness,
		Id:             1,
		PermissionName: "witness",
		Threshold:      1,
		ParentId:       0,
		Keys: []*corepb.Key{
			{Address: addr.Bytes(), Weight: 1},
		},
	}
}

// Delegated frozen V2 balance accessors (resources delegated TO this account).
func (a *Account) DelegatedFrozenV2BalanceForBandwidth() int64 {
	return a.pb.DelegatedFrozenV2BalanceForBandwidth
}
func (a *Account) SetDelegatedFrozenV2BalanceForBandwidth(v int64) {
	a.pb.DelegatedFrozenV2BalanceForBandwidth = v
}
func (a *Account) AcquiredDelegatedFrozenV2BalanceForBandwidth() int64 {
	return a.pb.AcquiredDelegatedFrozenV2BalanceForBandwidth
}
func (a *Account) SetAcquiredDelegatedFrozenV2BalanceForBandwidth(v int64) {
	a.pb.AcquiredDelegatedFrozenV2BalanceForBandwidth = v
}

func (a *Account) DelegatedFrozenV2BalanceForEnergy() int64 {
	if a.pb.AccountResource == nil {
		return 0
	}
	return a.pb.AccountResource.DelegatedFrozenV2BalanceForEnergy
}
func (a *Account) SetDelegatedFrozenV2BalanceForEnergy(v int64) {
	a.ensureAccountResource()
	a.pb.AccountResource.DelegatedFrozenV2BalanceForEnergy = v
}
func (a *Account) AcquiredDelegatedFrozenV2BalanceForEnergy() int64 {
	if a.pb.AccountResource == nil {
		return 0
	}
	return a.pb.AccountResource.AcquiredDelegatedFrozenV2BalanceForEnergy
}
func (a *Account) SetAcquiredDelegatedFrozenV2BalanceForEnergy(v int64) {
	a.ensureAccountResource()
	a.pb.AccountResource.AcquiredDelegatedFrozenV2BalanceForEnergy = v
}

// --- V1 Stake (Stake 1.0) frozen balance accessors ---

func (a *Account) FrozenBandwidthList() []*corepb.Account_Frozen {
	return a.pb.Frozen
}

func (a *Account) AddFrozenBandwidth(amount, expireTimeMs int64) {
	a.pb.Frozen = append(a.pb.Frozen, &corepb.Account_Frozen{
		FrozenBalance: amount,
		ExpireTime:    expireTimeMs,
	})
}

func (a *Account) SetFrozenBandwidth(amount, expireTimeMs int64) {
	a.pb.Frozen = []*corepb.Account_Frozen{{
		FrozenBalance: amount,
		ExpireTime:    expireTimeMs,
	}}
}

func (a *Account) TotalFrozenBandwidth() int64 {
	var total int64
	for _, f := range a.pb.Frozen {
		total += f.FrozenBalance
	}
	return total
}

func (a *Account) RemoveExpiredFrozenBandwidth(blockTimeMs int64) int64 {
	var refunded int64
	remaining := a.pb.Frozen[:0]
	for _, f := range a.pb.Frozen {
		if f.ExpireTime <= blockTimeMs {
			refunded += f.FrozenBalance
		} else {
			remaining = append(remaining, f)
		}
	}
	a.pb.Frozen = remaining
	return refunded
}

func (a *Account) FrozenEnergyAmount() int64 {
	if a.pb.AccountResource == nil || a.pb.AccountResource.FrozenBalanceForEnergy == nil {
		return 0
	}
	return a.pb.AccountResource.FrozenBalanceForEnergy.FrozenBalance
}

func (a *Account) FrozenEnergyExpireTime() int64 {
	if a.pb.AccountResource == nil || a.pb.AccountResource.FrozenBalanceForEnergy == nil {
		return 0
	}
	return a.pb.AccountResource.FrozenBalanceForEnergy.ExpireTime
}

func (a *Account) AddFrozenEnergy(amount, expireTimeMs int64) {
	a.ensureAccountResource()
	if a.pb.AccountResource.FrozenBalanceForEnergy == nil {
		a.pb.AccountResource.FrozenBalanceForEnergy = &corepb.Account_Frozen{
			FrozenBalance: amount,
			ExpireTime:    expireTimeMs,
		}
	} else {
		a.pb.AccountResource.FrozenBalanceForEnergy.FrozenBalance += amount
		if expireTimeMs > a.pb.AccountResource.FrozenBalanceForEnergy.ExpireTime {
			a.pb.AccountResource.FrozenBalanceForEnergy.ExpireTime = expireTimeMs
		}
	}
}

func (a *Account) ClearFrozenEnergy() {
	if a.pb.AccountResource != nil {
		a.pb.AccountResource.FrozenBalanceForEnergy = nil
	}
}

// SetFrozenEnergy overwrites the V1 frozen-for-energy slot with the given
// amount/expiry, mirroring java-tron AccountCapsule.setFrozenForEnergy. Unlike
// ClearFrozenEnergy, this always leaves FrozenBalanceForEnergy present (a zero
// amount serialises as a present-but-empty sub-message), matching java's proto
// encoding for clearOwnerFreeze.
func (a *Account) SetFrozenEnergy(amount, expireTimeMs int64) {
	a.ensureAccountResource()
	a.pb.AccountResource.FrozenBalanceForEnergy = &corepb.Account_Frozen{
		FrozenBalance: amount,
		ExpireTime:    expireTimeMs,
	}
}

// V1 delegation: bandwidth
func (a *Account) DelegatedFrozenBandwidth() int64 { return a.pb.DelegatedFrozenBalanceForBandwidth }
func (a *Account) SetDelegatedFrozenBandwidth(v int64) {
	a.pb.DelegatedFrozenBalanceForBandwidth = v
}
func (a *Account) AcquiredDelegatedFrozenBandwidth() int64 {
	return a.pb.AcquiredDelegatedFrozenBalanceForBandwidth
}
func (a *Account) SetAcquiredDelegatedFrozenBandwidth(v int64) {
	a.pb.AcquiredDelegatedFrozenBalanceForBandwidth = v
}

// V1 delegation: energy
func (a *Account) DelegatedFrozenEnergy() int64 {
	if a.pb.AccountResource == nil {
		return 0
	}
	return a.pb.AccountResource.DelegatedFrozenBalanceForEnergy
}
func (a *Account) SetDelegatedFrozenEnergy(v int64) {
	a.ensureAccountResource()
	a.pb.AccountResource.DelegatedFrozenBalanceForEnergy = v
}
func (a *Account) AcquiredDelegatedFrozenEnergy() int64 {
	if a.pb.AccountResource == nil {
		return 0
	}
	return a.pb.AccountResource.AcquiredDelegatedFrozenBalanceForEnergy
}
func (a *Account) SetAcquiredDelegatedFrozenEnergy(v int64) {
	a.ensureAccountResource()
	a.pb.AccountResource.AcquiredDelegatedFrozenBalanceForEnergy = v
}

// ClearUnfrozenV2 removes all pending unfreeze entries.
func (a *Account) ClearUnfrozenV2() {
	a.pb.UnfrozenV2 = nil
}

// ClearFrozenV2 removes all FreezeV2 entries, mirroring java-tron
// AccountCapsule.clearFrozenV2 used by the SELFDESTRUCT path under
// allow_tvm_freeze_v2 (clearOwnerFreezeV2).
func (a *Account) ClearFrozenV2() {
	a.pb.FrozenV2 = nil
}

// Marshal serializes the complete Account protobuf deterministically. The six
// string→int64 maps use a direct sorted encoder to avoid protobuf reflection;
// all other fields retain generated-protobuf encoding. StateAccount v3 storage
// uses MarshalStorageCore instead, while full account/API consumers keep the
// complete byte-compatible representation returned here.
func (a *Account) Marshal() ([]byte, error) {
	hint := int(a.marshalSizeHint.Load())
	if data, err, handled := marshalAccountDirectMaps(a.pb, hint); handled {
		if err != nil {
			return nil, err
		}
		a.marshalSizeHint.Store(int64(len(data)))
		return data, nil
	}
	data, err := marshalMessageDeterministic(a.pb.ProtoReflect(), hint)
	if err != nil {
		return nil, err
	}
	a.marshalSizeHint.Store(int64(len(data)))
	return data, nil
}

// MarshalStorageCore serializes the v3 account-latest core without the split
// TRC10 maps, Owner/Witness/Active permissions, votes, Stake V1/V2 fields,
// frozen supply, and AccountResource persisted in account-local KV domains.
func (a *Account) MarshalStorageCore() ([]byte, error) {
	data, err := marshalAccountStorageCore(a.pb, int(a.marshalSizeHint.Load()))
	if err != nil {
		return nil, err
	}
	return data, nil
}

func UnmarshalAccount(data []byte) (*Account, error) {
	pb, err, handled := unmarshalAccountDirectMaps(data)
	if !handled {
		pb = &corepb.Account{}
		err = proto.Unmarshal(data, pb)
	}
	if err != nil {
		return nil, err
	}
	account := &Account{pb: pb}
	account.marshalSizeHint.Store(int64(len(data)))
	return account, nil
}

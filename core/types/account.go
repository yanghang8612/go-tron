package types

import (
	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

type Account struct {
	pb *corepb.Account
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

func (a *Account) Proto() *corepb.Account  { return a.pb }
func (a *Account) Address() common.Address  { return common.BytesToAddress(a.pb.Address) }
func (a *Account) Balance() int64           { return a.pb.Balance }
func (a *Account) SetBalance(b int64)       { a.pb.Balance = b }
func (a *Account) Type() corepb.AccountType        { return a.pb.Type }
func (a *Account) SetAccountType(t corepb.AccountType) { a.pb.Type = t }
func (a *Account) IsWitness() bool                  { return a.pb.IsWitness }
func (a *Account) SetIsWitness(v bool)      { a.pb.IsWitness = v }
func (a *Account) CreateTime() int64        { return a.pb.CreateTime }
func (a *Account) SetCreateTime(t int64)    { a.pb.CreateTime = t }

// AccountName accessors.
func (a *Account) AccountName() string         { return string(a.pb.AccountName) }
func (a *Account) SetAccountName(name string)  { a.pb.AccountName = []byte(name) }

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
func (a *Account) Votes() []*corepb.Vote      { return a.pb.Votes }
func (a *Account) SetVotes(votes []*corepb.Vote) { a.pb.Votes = votes }
func (a *Account) ClearVotes()                { a.pb.Votes = nil }

// Bandwidth resource tracking.
func (a *Account) NetUsage() int64                { return a.pb.NetUsage }
func (a *Account) SetNetUsage(v int64)            { a.pb.NetUsage = v }
func (a *Account) LatestConsumeTime() int64       { return a.pb.LatestConsumeTime }
func (a *Account) SetLatestConsumeTime(t int64)   { a.pb.LatestConsumeTime = t }
func (a *Account) FreeNetUsage() int64            { return a.pb.FreeNetUsage }
func (a *Account) SetFreeNetUsage(v int64)        { a.pb.FreeNetUsage = v }
func (a *Account) LatestConsumeFreeTime() int64   { return a.pb.LatestConsumeFreeTime }
func (a *Account) SetLatestConsumeFreeTime(t int64) { a.pb.LatestConsumeFreeTime = t }

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

// Allowance (witness rewards) accessors.
func (a *Account) Allowance() int64              { return a.pb.Allowance }
func (a *Account) SetAllowance(v int64)          { a.pb.Allowance = v }
func (a *Account) LatestWithdrawTime() int64     { return a.pb.LatestWithdrawTime }
func (a *Account) SetLatestWithdrawTime(t int64) { a.pb.LatestWithdrawTime = t }

// AccountId accessors.
func (a *Account) AccountId() string       { return string(a.pb.AccountId) }
func (a *Account) SetAccountId(id string)  { a.pb.AccountId = []byte(id) }

// Permission accessors.
func (a *Account) OwnerPermission() *corepb.Permission            { return a.pb.OwnerPermission }
func (a *Account) WitnessPermission() *corepb.Permission          { return a.pb.WitnessPermission }
func (a *Account) ActivePermission() []*corepb.Permission         { return a.pb.ActivePermission }
func (a *Account) SetOwnerPermission(p *corepb.Permission)        { a.pb.OwnerPermission = p }
func (a *Account) SetWitnessPermission(p *corepb.Permission)      { a.pb.WitnessPermission = p }
func (a *Account) SetActivePermission(perms []*corepb.Permission) { a.pb.ActivePermission = perms }

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

func (a *Account) Marshal() ([]byte, error) {
	return proto.Marshal(a.pb)
}

func UnmarshalAccount(data []byte) (*Account, error) {
	pb := &corepb.Account{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return nil, err
	}
	return NewAccountFromPB(pb), nil
}

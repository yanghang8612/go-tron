package core

import (
	"math/big"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// availableAccountEnergy returns this account's share of the global energy
// pool, mirroring java-tron's EnergyProcessor.calculateGlobalEnergyLimit.
// Uses total_energy_current_limit (which is dynamically adjusted when
// adaptive energy is active) rather than the static total_energy_limit.
func availableAccountEnergy(acct *types.Account, dp *state.DynamicProperties) int64 {
	if acct == nil {
		return 0
	}
	frozen := frozenForEnergy(acct)

	totalWeight := dp.TotalEnergyWeight()
	if totalWeight <= 0 {
		return 0
	}
	totalLimit := dp.TotalEnergyCurrentLimit()
	harden := dp.AllowHardenResourceCalculation()

	if dp.UnfreezeDelayDays() > 0 {
		return calculateGlobalResourceLimitV2(frozen, totalLimit, totalWeight, harden)
	}
	if frozen < trxPrecision {
		return 0
	}
	return calculateGlobalResourceLimitV1(frozen, totalLimit, totalWeight, harden)
}

// ownerResourceSnapshot holds the pre-execution balance and bandwidth state of
// a transaction's fee-payer (owner). Captured per-tx for cross-impl diagnostics
// and surfaced in TransactionInfo.ResourceReceipt — non-consensus, never hashed.
// The "left" values are post-recovery available bandwidth; the timestamps and
// frozen sums are the recovery/limit inputs, so a stalled re-sync can be diffed
// against java-tron without re-running gtron with extra logging.
type ownerResourceSnapshot struct {
	Balance                int64
	FreeNetLeft            int64
	FrozenNetLeft          int64
	NetLastConsumeTime     int64
	FreeNetLastConsumeTime int64
	FrozenForNet           int64
	FrozenForEnergy        int64
}

// captureOwnerResourceSnapshot reads the owner's balance and bandwidth state at
// execution start. It mirrors consumeBandwidth's recovery math (so the "left"
// values match what the bandwidth charge will see) but mutates nothing. A
// missing account yields the zero snapshot.
func captureOwnerResourceSnapshot(statedb *state.StateDB, dp *state.DynamicProperties, owner tcommon.Address, resourceTime int64) ownerResourceSnapshot {
	if !statedb.AccountExists(owner) {
		return ownerResourceSnapshot{}
	}
	acct := statedb.GetAccount(owner)
	snap := ownerResourceSnapshot{
		Balance:                statedb.GetBalance(owner),
		NetLastConsumeTime:     statedb.GetLatestConsumeTime(owner),
		FreeNetLastConsumeTime: statedb.GetLatestConsumeFreeTime(owner),
		FrozenForNet:           frozenForNet(acct),
		FrozenForEnergy:        frozenForEnergy(acct),
	}

	frozenLimit := availableAccountNet(acct, dp)
	frozenUsed := recoverUsageForDP(statedb.GetNetUsage(owner), snap.NetLastConsumeTime, resourceTime, dp)
	snap.FrozenNetLeft = max(0, frozenLimit-frozenUsed)

	freeLimit := dp.FreeNetLimit()
	freeUsed := recoverUsageForDP(statedb.GetFreeNetUsage(owner), snap.FreeNetLastConsumeTime, resourceTime, dp)
	snap.FreeNetLeft = max(0, freeLimit-freeUsed)

	return snap
}

// frozenForNet sums the frozen balances that contribute to an account's net
// (bandwidth) limit, matching availableAccountNet's java
// AccountCapsule.getAllFrozenBalanceForBandwidth sources.
func frozenForNet(acct *types.Account) int64 {
	if acct == nil {
		return 0
	}
	return acct.TotalFrozenBandwidth() +
		acct.AcquiredDelegatedFrozenBandwidth() +
		acct.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH) +
		acct.AcquiredDelegatedFrozenV2BalanceForBandwidth()
}

// frozenForEnergy sums the frozen balances that contribute to an account's
// energy limit, matching availableAccountEnergy's frozen sources.
func frozenForEnergy(acct *types.Account) int64 {
	if acct == nil {
		return 0
	}
	return acct.FrozenEnergyAmount() +
		acct.AcquiredDelegatedFrozenEnergy() +
		acct.GetFrozenV2Amount(corepb.ResourceCode_ENERGY) +
		acct.AcquiredDelegatedFrozenV2BalanceForEnergy()
}

// ResourceProcessor handles bandwidth and energy consumption/recovery.
type ResourceProcessor struct {
	statedb *state.StateDB
}

// NewResourceProcessor creates a new ResourceProcessor.
func NewResourceProcessor(statedb *state.StateDB) *ResourceProcessor {
	return &ResourceProcessor{statedb: statedb}
}

// RecoverBandwidth applies sliding window recovery to frozen bandwidth usage.
func (r *ResourceProcessor) RecoverBandwidth(addr tcommon.Address, now int64) {
	oldUsage := r.statedb.GetNetUsage(addr)
	lastTime := r.statedb.GetLatestConsumeTime(addr)
	newUsage := recoverUsage(oldUsage, lastTime, now)
	if newUsage != oldUsage {
		r.statedb.SetNetUsage(addr, newUsage)
	}
}

// RecoverFreeBandwidth applies sliding window recovery to free bandwidth usage.
func (r *ResourceProcessor) RecoverFreeBandwidth(addr tcommon.Address, now int64) {
	oldUsage := r.statedb.GetFreeNetUsage(addr)
	lastTime := r.statedb.GetLatestConsumeFreeTime(addr)
	newUsage := recoverUsage(oldUsage, lastTime, now)
	if newUsage != oldUsage {
		r.statedb.SetFreeNetUsage(addr, newUsage)
	}
}

// RecoverEnergy applies sliding window recovery to energy usage.
func (r *ResourceProcessor) RecoverEnergy(addr tcommon.Address, now int64) {
	oldUsage := r.statedb.GetEnergyUsage(addr)
	lastTime := r.statedb.GetLatestConsumeTimeForEnergy(addr)
	newUsage := recoverUsage(oldUsage, lastTime, now)
	if newUsage != oldUsage {
		r.statedb.SetEnergyUsage(addr, newUsage)
	}
}

// recoverUsage computes new usage after sliding window recovery.
func recoverUsage(oldUsage int64, lastTime int64, now int64) int64 {
	return recoverUsageWithHarden(oldUsage, lastTime, now, false)
}

func recoverUsageForDP(oldUsage, lastTime, now int64, dp *state.DynamicProperties) int64 {
	return recoverUsageWithHarden(oldUsage, lastTime, now, dp != nil && dp.AllowHardenResourceCalculation())
}

func recoverUsageWithHarden(oldUsage, lastTime, now int64, harden bool) int64 {
	if oldUsage <= 0 {
		return 0
	}
	windowSize := int64(params.WindowSizeSlots)
	elapsed := now - lastTime
	if elapsed >= windowSize {
		return 0
	}
	if elapsed <= 0 {
		return oldUsage
	}
	// java ResourceProcessor.increase(oldUsage, 0, lastTime, now, windowSize):
	// precision-averaging recovery over the global 28800-slot window, NOT a
	// plain truncate. The truncate drifted ~1 unit per recovered block,
	// compounding to a free-vs-burn bandwidth fork (NET twin of energy 6cfc163).
	if harden {
		return increaseHardened(oldUsage, 0, lastTime, now, windowSize)
	}
	return increase(oldUsage, 0, lastTime, now, windowSize)
}

func calculateGlobalResourceLimitV1(frozen, totalLimit, totalWeight int64, harden bool) int64 {
	weight := frozen / trxPrecision
	if !harden {
		return int64(float64(weight) * (float64(totalLimit) / float64(totalWeight)))
	}
	return bigMulDivInt64(weight, totalLimit, totalWeight)
}

func calculateGlobalResourceLimitV2(frozen, totalLimit, totalWeight int64, harden bool) int64 {
	if !harden {
		weight := float64(frozen) / float64(trxPrecision)
		return int64(weight * (float64(totalLimit) / float64(totalWeight)))
	}
	denominator := new(big.Int).Mul(big.NewInt(trxPrecision), big.NewInt(totalWeight))
	return bigMulDivBigInt64(big.NewInt(frozen), big.NewInt(totalLimit), denominator)
}

func divideCeilBig(numerator, denominator *big.Int) int64 {
	q, r := new(big.Int).QuoRem(numerator, denominator, new(big.Int))
	if r.Sign() > 0 {
		q.Add(q, big.NewInt(1))
	}
	return tcommon.BigInt64Exact(q, "resource divideCeil")
}

func bigMulDivInt64(a, b, c int64) int64 {
	return bigMulDivBigInt64(big.NewInt(a), big.NewInt(b), big.NewInt(c))
}

func bigMulDivBigInt64(a, b, c *big.Int) int64 {
	n := new(big.Int).Mul(a, b)
	n.Quo(n, c)
	return tcommon.BigInt64Exact(n, "resource multiply/divide")
}

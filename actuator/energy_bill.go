package actuator

import (
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// PayEnergyBill settles the energy fee for a smart-contract transaction.
//
// Mirrors java-tron `ReceiptCapsule.payEnergyBill`
// (chainbase/src/main/java/org/tron/core/capsule/ReceiptCapsule.java::260).
// Routing precedence and the OUT_OF_TIME exception match java line-for-line:
//
//  1. Subtract `accountEnergyLeft` (stake-funded energy after sliding-window
//     recovery) from `EnergyUsageTotal`. Stake portion is debited from the
//     caller's energy_usage counter (EnergyProcessor.useEnergy) — no SUN
//     leaves the caller's balance.
//  2. The overage is multiplied by the per-energy SUN price (DP key
//     `energy_fee`, default 100) to produce the balance bill.
//  3. The bill is debited from the caller's TRX balance, then routed:
//       - `support_transaction_fee_pool` && result != OUT_OF_TIME
//             -> dynamic property `transaction_fee_pool` += bill
//       - else `allow_blackhole_optimization`
//             -> dynamic property `burn_trx_amount` += bill
//       - else
//             -> credit the genesis "Blackhole" account
//
// On the live cross-impl chain (config.conf private chain), the active
// branch is `allow_blackhole_optimization` (see
// docs/dev/cross-impl-divergences-2026-05-02.md D-1 root cause).
//
// FOLLOW-UP: this implementation does NOT yet model the
// `consume_user_resource_percent` split (origin contract owner absorbs a
// fraction of the energy via stake). java-tron handles that case in
// payEnergyBill's three-arg overload at line 201-239 of ReceiptCapsule.java.
// All historical TVM txs on the cross-impl chain are CreateSmartContract
// where caller == origin, so the split path is unexercised; deferring is
// safe for D-1 parity but must land before mainnet replay covers TRC-20
// triggers.
func PayEnergyBill(ctx *Context, result *Result) error {
	if result.EnergyUsageTotal <= 0 {
		return nil
	}

	owner := extractOwnerAddress(ctx)
	if owner == (common.Address{}) {
		return fmt.Errorf("payEnergyBill: cannot determine caller for tx %x", ctx.Tx.Hash())
	}

	totalEnergy := result.EnergyUsageTotal

	// Step 1: drain stake-funded energy first.
	//
	// availableAccountEnergyForBill returns the caller's stake-energy
	// allowance after sliding-window recovery applied to its prior
	// energy_usage. java-tron computes this in
	// EnergyProcessor.getAccountLeftEnergyFromFreeze; we inline it here so
	// the actuator package doesn't import the parent core package.
	stakeLeft := availableAccountEnergyForBill(ctx.State, ctx.DynProps, owner, ctx.BlockTime)

	stakeUsed := stakeLeft
	if stakeUsed > totalEnergy {
		stakeUsed = totalEnergy
	}
	balanceUsed := totalEnergy - stakeUsed

	// Mark the stake-funded portion against the caller's energy_usage.
	// Mirrors EnergyProcessor.useEnergy: recovered_usage + stakeUsed,
	// timestamp updated to `now`.
	if stakeUsed > 0 {
		recovered := recoverEnergyUsage(
			ctx.State.GetEnergyUsage(owner),
			ctx.State.GetLatestConsumeTimeForEnergy(owner),
			ctx.BlockTime,
		)
		ctx.State.SetEnergyUsage(owner, recovered+stakeUsed)
		ctx.State.SetLatestConsumeTimeForEnergy(owner, ctx.BlockTime)
	}

	// proto field 1 (energy_usage) carries the stake-paid amount.
	result.EnergyUsed = stakeUsed

	if balanceUsed == 0 {
		// Pure-stake path. No SUN leaves the caller's balance, no fee
		// routing happens. This matches java's early-return at line 277
		// of ReceiptCapsule.java when `accountEnergyLeft >= usage`.
		return nil
	}

	// Step 2: balance-paid portion.
	sunPerEnergy := ctx.DynProps.EnergyFee()
	if sunPerEnergy <= 0 {
		// Mirrors java's Constant.SUN_PER_ENERGY fallback. The DP value is
		// initialized to 100 on mainnet/Nile; private chains may set 0
		// only when energy_fee was never proposed.
		sunPerEnergy = 100
	}
	bill := balanceUsed * sunPerEnergy

	if err := ctx.State.SubBalance(owner, bill); err != nil {
		return fmt.Errorf("payEnergyBill: insufficient balance to pay %d sun: %w", bill, err)
	}

	result.EnergyFee = bill
	result.Fee += bill

	// Step 3: route the bill.
	//
	// The OUT_OF_TIME exception is critical: java skips the
	// transaction_fee_pool path when the contract result is OUT_OF_TIME so
	// that the SR-time-budget overrun fee gets burned rather than rebated
	// to the SR via the witness pay-out. Falling through to
	// blackhole/burnTrx mirrors that escape.
	contractRet := corepb.Transaction_ResultContractResult(result.ContractRet)
	outOfTime := contractRet == corepb.Transaction_Result_OUT_OF_TIME

	if ctx.DynProps.AllowTransactionFeePool() && !outOfTime {
		// FOLLOW-UP: the per-block drain that returns this pool back to
		// the witness's allowance (java Manager.payReward lines 1934-1944)
		// is NOT yet implemented in core/reward.go. On any chain that
		// activates `support_transaction_fee_pool`, the pool will grow
		// without ever paying out — flagged in D-1 follow-up gaps.
		ctx.DynProps.AddTransactionFeePool(bill)
		return nil
	}
	if ctx.DynProps.AllowBlackHoleOptimization() {
		ctx.DynProps.AddBurnTrx(bill)
		return nil
	}
	ctx.State.AddBalance(params.BlackholeAddress, bill)
	return nil
}

// extractOwnerAddress mirrors core.extractSender but stays inside the
// actuator package to avoid the actuator -> core import cycle. The TVM
// tx types only ever carry one contract whose owner is the caller; the
// generic path (any contract with GetOwnerAddress) keeps the helper
// usable for non-VM contract types if needed later.
func extractOwnerAddress(ctx *Context) common.Address {
	c := ctx.Tx.Contract()
	if c == nil || c.Parameter == nil {
		return common.Address{}
	}
	msg, err := c.Parameter.UnmarshalNew()
	if err != nil {
		return common.Address{}
	}
	type ownerAddressGetter interface{ GetOwnerAddress() []byte }
	if oag, ok := msg.(ownerAddressGetter); ok {
		return common.BytesToAddress(oag.GetOwnerAddress())
	}
	return common.Address{}
}

// availableAccountEnergyForBill returns the caller's stake-energy
// allowance net of recovered prior usage — the java-tron
// EnergyProcessor.getAccountLeftEnergyFromFreeze quantity.
//
// Returns 0 if the caller has no frozen-for-energy stake or if recovered
// usage already meets the entitled limit.
func availableAccountEnergyForBill(s *state.StateDB, dp *state.DynamicProperties, addr common.Address, now int64) int64 {
	acct := s.GetAccount(addr)
	if acct == nil {
		return 0
	}
	limit := calcAccountEnergyLimit(acct, dp)
	if limit <= 0 {
		return 0
	}
	recovered := recoverEnergyUsage(s.GetEnergyUsage(addr), s.GetLatestConsumeTimeForEnergy(addr), now)
	if recovered >= limit {
		return 0
	}
	return limit - recovered
}

// calcAccountEnergyLimit mirrors java-tron's
// EnergyProcessor.calculateGlobalEnergyLimit. We can't reuse
// core.availableAccountEnergy because actuator can't import core; the
// duplication is intentional and the formulas must match
// core/resource.go::availableAccountEnergy line-for-line.
func calcAccountEnergyLimit(acct *types.Account, dp *state.DynamicProperties) int64 {
	frozen := acct.FrozenEnergyAmount()
	frozen += acct.AcquiredDelegatedFrozenEnergy()
	frozen += acct.GetFrozenV2Amount(corepb.ResourceCode_ENERGY)
	frozen += acct.AcquiredDelegatedFrozenV2BalanceForEnergy()

	totalWeight := dp.TotalEnergyWeight()
	if totalWeight <= 0 {
		return 0
	}
	totalLimit := dp.TotalEnergyCurrentLimit()

	if dp.UnfreezeDelayDays() > 0 {
		netWeight := float64(frozen) / float64(params.TRXPrecision)
		return int64(netWeight * (float64(totalLimit) / float64(totalWeight)))
	}
	if frozen < params.TRXPrecision {
		return 0
	}
	netWeight := frozen / params.TRXPrecision
	return int64(float64(netWeight) * (float64(totalLimit) / float64(totalWeight)))
}

// recoverEnergyUsage applies the sliding-window recovery to a stored
// energy_usage value. Identical math to core.recoverUsage (window =
// 86_400_000ms = 1 day). Inlined to keep actuator -> core import-free.
func recoverEnergyUsage(oldUsage, lastTime, now int64) int64 {
	if oldUsage <= 0 {
		return 0
	}
	elapsed := now - lastTime
	if elapsed >= int64(params.WindowSizeMs) {
		return 0
	}
	if elapsed <= 0 {
		return oldUsage
	}
	remaining := int64(params.WindowSizeMs) - elapsed
	return oldUsage * remaining / int64(params.WindowSizeMs)
}

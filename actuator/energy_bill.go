package actuator

import (
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// PayEnergyBill settles the energy fee for a smart-contract transaction.
//
// Mirrors java-tron `ReceiptCapsule.payEnergyBill`
// (chainbase/src/main/java/org/tron/core/capsule/ReceiptCapsule.java).
// java has two overloads:
//
//  - 1-arg `payEnergyBill(account, usage, ...)` (line 260): drain
//    account's stake-funded energy first; spill to balance-billed
//    energy_fee at the per-SUN rate; route the fee to
//    `transaction_fee_pool` / `burn_trx_amount` / blackhole based on DP
//    flags. The OUT_OF_TIME exception skips the fee-pool path so the
//    SR-time-budget overrun gets burned rather than rebated to the SR.
//  - 3-arg `payEnergyBill(origin, caller, percent, originEnergyLimit, …)`
//    (line 201): when caller != origin and the contract has
//    `consume_user_resource_percent > 0`, split the bill — origin
//    absorbs `percent%` of EnergyUsageTotal (capped by its stake-energy
//    AND `origin_energy_limit`), the remainder bills the caller via
//    the 1-arg path. Origin NEVER pays TRX from balance; if its stake
//    can't cover its share, the shortfall flows back to caller.
//
// On the live cross-impl chain (`allow_blackhole_optimization` active),
// the spill goes to `burn_trx_amount`. See
// docs/dev/cross-impl-divergences-2026-05-02.md.
//
// gtron uses the "modern" `getOriginUsage` formula
// (allowTvmFreeze/supportUnfreezeDelay path), which caps origin usage by
// min(stake-left, origin_energy_limit). All chains since 4.0 take this
// branch; pre-4.0 historical replay (no origin_energy_limit cap) is not
// modeled here — would need a fork gate if M0″ Phase 2 covers blocks
// from that era.
func PayEnergyBill(ctx *Context, result *Result) error {
	if result.EnergyUsageTotal <= 0 {
		return nil
	}

	caller := extractOwnerAddress(ctx)
	if caller == (common.Address{}) {
		return fmt.Errorf("payEnergyBill: cannot determine caller for tx %x", ctx.Tx.Hash())
	}

	totalEnergy := result.EnergyUsageTotal

	// 3-arg path: TriggerSmartContract with caller != origin and a
	// non-zero ConsumeUserResourcePercent. Mirrors java's split.
	origin, originUsage, callerUsage := splitOriginCallerUsage(ctx, caller, totalEnergy)
	if originUsage > 0 {
		// Bill origin against its stake-energy only. No balance debit.
		// Mirrors `energyProcessor.useEnergy(origin, originUsage, now)` at
		// ReceiptCapsule.java:235 — we already pre-capped originUsage by
		// origin's available stake in splitOriginCallerUsage, so this
		// never over-bills.
		recovered := recoverEnergyUsage(
			ctx.State.GetEnergyUsage(origin),
			ctx.State.GetLatestConsumeTimeForEnergy(origin),
			ctx.PrevBlockTime,
		)
		ctx.State.SetEnergyUsage(origin, recovered+originUsage)
		ctx.State.SetLatestConsumeTimeForEnergy(origin, ctx.PrevBlockTime)
		// Receipt's origin_energy_usage carries the split share so SDKs
		// see the same TransactionInfo as java-tron.
		result.OriginEnergyUsage = originUsage
	}

	return billCallerSide(ctx, result, caller, callerUsage)
}

// billCallerSide implements java-tron's 1-arg payEnergyBill: drain
// caller's stake-funded energy, spill to balance, route the fee.
func billCallerSide(ctx *Context, result *Result, caller common.Address, usage int64) error {
	if usage <= 0 {
		// All energy was absorbed by origin's stake (or the tx was a no-op).
		// Nothing to bill on the caller side.
		return nil
	}

	stakeLeft := availableAccountEnergyForBill(ctx.State, ctx.DynProps, caller, ctx.PrevBlockTime)

	stakeUsed := stakeLeft
	if stakeUsed > usage {
		stakeUsed = usage
	}
	balanceUsed := usage - stakeUsed

	// Mark the stake-funded portion against the caller's energy_usage.
	// Mirrors EnergyProcessor.useEnergy: recovered_usage + stakeUsed,
	// timestamp updated to `now`.
	if stakeUsed > 0 {
		recovered := recoverEnergyUsage(
			ctx.State.GetEnergyUsage(caller),
			ctx.State.GetLatestConsumeTimeForEnergy(caller),
			ctx.PrevBlockTime,
		)
		ctx.State.SetEnergyUsage(caller, recovered+stakeUsed)
		ctx.State.SetLatestConsumeTimeForEnergy(caller, ctx.PrevBlockTime)
	}

	// proto field 1 (energy_usage) carries the stake-paid amount.
	result.EnergyUsed = stakeUsed

	if balanceUsed == 0 {
		// Pure-stake path. No SUN leaves the caller's balance, no fee
		// routing happens. This matches java's early-return at line 277
		// of ReceiptCapsule.java when `accountEnergyLeft >= usage`.
		return nil
	}

	// Balance-paid portion.
	sunPerEnergy := ctx.DynProps.EnergyFee()
	if sunPerEnergy <= 0 {
		// Mirrors java's Constant.SUN_PER_ENERGY fallback. The DP value is
		// initialized to 100 on mainnet/Nile; private chains may set 0
		// only when energy_fee was never proposed.
		sunPerEnergy = 100
	}
	bill := balanceUsed * sunPerEnergy

	if err := ctx.State.SubBalance(caller, bill); err != nil {
		return fmt.Errorf("payEnergyBill: insufficient balance to pay %d sun: %w", bill, err)
	}

	result.EnergyFee = bill
	result.Fee += bill

	// Route the bill. The OUT_OF_TIME exception is critical: java skips
	// the transaction_fee_pool path when the contract result is
	// OUT_OF_TIME so that the SR-time-budget overrun fee gets burned
	// rather than rebated to the SR via the witness pay-out.
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

// splitOriginCallerUsage decides the (origin, originUsage, callerUsage)
// split for the current tx. Returns (zeroAddr, 0, totalEnergy) for the
// no-split path: not a TriggerSmartContract, caller == origin,
// ConsumeUserResourcePercent == 0, or the contract metadata is missing.
//
// For TriggerSmartContract with a non-zero percent and caller != origin,
// applies java-tron's modern `getOriginUsage` formula:
//
//	originUsage = min(totalEnergy * percent / 100,
//	                  min(originStakeLeft, originEnergyLimit))
//	callerUsage = totalEnergy - originUsage
func splitOriginCallerUsage(ctx *Context, caller common.Address, totalEnergy int64) (origin common.Address, originShare, callerShare int64) {
	if ctx.Tx.ContractType() != corepb.Transaction_Contract_TriggerSmartContract {
		return common.Address{}, 0, totalEnergy
	}
	c := ctx.Tx.Contract()
	if c == nil || c.Parameter == nil {
		return common.Address{}, 0, totalEnergy
	}
	tsc := &contractpb.TriggerSmartContract{}
	if err := c.Parameter.UnmarshalTo(tsc); err != nil {
		return common.Address{}, 0, totalEnergy
	}
	contractAddr := common.BytesToAddress(tsc.ContractAddress)
	contract := ctx.State.GetContract(contractAddr)
	if contract == nil {
		// Metadata went missing. Fall back to caller-only billing.
		return common.Address{}, 0, totalEnergy
	}
	originAddr := common.BytesToAddress(contract.OriginAddress)
	if originAddr == (common.Address{}) || originAddr == caller {
		return common.Address{}, 0, totalEnergy
	}

	percent := contract.ConsumeUserResourcePercent
	if percent <= 0 {
		return common.Address{}, 0, totalEnergy
	}
	if percent > 100 {
		percent = 100
	}

	want := totalEnergy * percent / 100

	originLimit := contract.OriginEnergyLimit
	originStakeLeft := availableAccountEnergyForBill(ctx.State, ctx.DynProps, originAddr, ctx.PrevBlockTime)

	cap := originStakeLeft
	if originLimit > 0 && originLimit < cap {
		cap = originLimit
	}
	if want > cap {
		want = cap
	}
	if want < 0 {
		want = 0
	}
	return originAddr, want, totalEnergy - want
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

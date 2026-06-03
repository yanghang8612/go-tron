package actuator

import (
	"fmt"
	"math/big"

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
//   - 1-arg `payEnergyBill(account, usage, ...)` (line 260): drain
//     account's stake-funded energy first; spill to balance-billed
//     energy_fee at the per-SUN rate; route the fee to
//     `transaction_fee_pool` / `burn_trx_amount` / blackhole based on DP
//     flags. The OUT_OF_TIME exception skips the fee-pool path so the
//     SR-time-budget overrun gets burned rather than rebated to the SR.
//   - 3-arg `payEnergyBill(origin, caller, percent, originEnergyLimit, …)`
//     (line 201): when caller != origin and the contract has
//     `consume_user_resource_percent > 0`, split the bill — origin
//     absorbs `percent%` of EnergyUsageTotal (capped by its stake-energy
//     AND `origin_energy_limit`), the remainder bills the caller via
//     the 1-arg path. Origin NEVER pays TRX from balance; if its stake
//     can't cover its share, the shortfall flows back to caller.
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
	origin, originUsage, callerUsage := splitOriginCallerUsage(ctx, result, caller, totalEnergy)
	if origin != (common.Address{}) {
		// Bill origin against its stake-energy only. No balance debit.
		// Mirrors `energyProcessor.useEnergy(origin, originUsage, now)` at
		// ReceiptCapsule.java:235 — we already pre-capped originUsage by
		// origin's available stake in splitOriginCallerUsage, so this
		// never over-bills.
		useEnergyForBill(ctx, origin, originUsage, contractSucceeded(result))
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
		// All energy was absorbed by origin's stake. java-tron still routes
		// through EnergyProcessor.useEnergy(caller, 0), refreshing the caller's
		// recovered usage window and latest operation timestamp.
		useEnergyForBill(ctx, caller, 0, contractSucceeded(result))
		return nil
	}

	resourceTime := ctx.ResourceTime()
	stakeLeft := availableAccountEnergyForBill(ctx.State, ctx.DynProps, caller, resourceTime)
	if vmReceiptEnergyLeftMode(ctx) && result.HasCallerEnergyLeft {
		stakeLeft = result.CallerEnergyLeft
	}
	if legacyVMReceiptEnergyLeftMode(ctx) {
		stakeLeft = 0
	}

	stakeUsed := stakeLeft
	if stakeUsed > usage {
		stakeUsed = usage
	}
	balanceUsed := usage - stakeUsed

	// Mark the stake-funded portion against the caller's energy_usage.
	// Mirrors EnergyProcessor.useEnergy: recovered_usage + stakeUsed,
	// timestamp updated to `now`.
	useEnergyForBill(ctx, caller, stakeUsed, contractSucceeded(result))

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

// useEnergyForBill settles `usage` energy against addr's stake-funded
// energy_usage and (in the V2 regime) the per-account recovery window.
//
// `success` selects java's settle shape. On a SUCCESSFUL contract result java
// commits the VMActuator pre-charge and TransactionTrace.resetAccountUsage
// restores the pre-merge (R, W_R), so EnergyProcessor.useEnergy settles in two
// steps: recover (decay + window shrink), then add `usage` at lastTime==now. On
// REVERT/exception/OOE the pre-charge is discarded (VMActuator.java:234-250
// never commits rootRepository) and resetAccountUsage is skipped, so useEnergy
// settles single-step over the ORIGINAL usage/time. The two shapes differ by up
// to one unit of energy_usage and a few of windowSize (java-verified), so the
// gate is consensus-relevant.
// contractSucceeded reports whether the VM result is a clean success (no
// exception, no revert). Mirrors java TransactionTrace's
// `getException() == null && !isRevert()` gate — only then does java commit the
// pre-charge and run resetAccountUsage (the two-step settle).
func contractSucceeded(result *Result) bool {
	return result != nil && result.ContractRet == int32(corepb.Transaction_Result_SUCCESS)
}

func useEnergyForBill(ctx *Context, addr common.Address, usage int64, success bool) {
	now := ctx.ResourceTime()
	dp := ctx.DynProps
	oldUsage := ctx.State.GetEnergyUsage(addr)
	oldTime := ctx.State.GetLatestConsumeTimeForEnergy(addr)

	if dp == nil || !dp.SupportUnfreezeDelay() {
		// Pre-Stake-2.0: global-window recovery + add, no per-account window.
		// Mirrors java EnergyProcessor.useEnergy's two increase() calls — recover
		// usage to `now`, then add `usage` at lastTime==now — both over the global
		// 28800-slot window (precision-averaging, NOT truncate). The success/
		// failure settle shapes collapse here: every increase() already
		// recovers-then-adds, so a committed pre-charge and a discarded one yield
		// the same result.
		harden := dp != nil && dp.AllowHardenResourceCalculation()
		recovered := computeEnergyIncreaseGlobal(oldUsage, 0, oldTime, now, harden)
		final := computeEnergyIncreaseGlobal(recovered, usage, now, now, harden)
		ctx.State.SetEnergyUsage(addr, final)
		ctx.State.SetLatestConsumeTimeForEnergy(addr, now)
		ctx.State.SetLatestOperationTime(addr, ctx.PrevBlockTime)
		return
	}

	harden := dp.AllowHardenResourceCalculation()
	cancelAllV2 := dp.SupportCancelAllUnfreezeV2()

	var rawWindow int64
	var optimized bool
	if acct := ctx.State.GetAccount(addr); acct != nil {
		rawWindow, optimized = acct.RawEnergyWindowSize(), acct.EnergyWindowOptimized()
	}

	var finalUsage int64
	if success {
		r, rw, opt := computeEnergyIncrease(rawWindow, optimized, oldUsage, 0, oldTime, now, harden, cancelAllV2)
		finalUsage, rawWindow, optimized = computeEnergyIncrease(rw, opt, r, usage, now, now, harden, cancelAllV2)
	} else {
		finalUsage, rawWindow, optimized = computeEnergyIncrease(rawWindow, optimized, oldUsage, usage, oldTime, now, harden, cancelAllV2)
	}

	ctx.State.SetEnergyUsage(addr, finalUsage)
	ctx.State.SetEnergyWindow(addr, rawWindow, optimized)
	ctx.State.SetLatestConsumeTimeForEnergy(addr, now)
	ctx.State.SetLatestOperationTime(addr, ctx.PrevBlockTime)
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
func splitOriginCallerUsage(ctx *Context, result *Result, caller common.Address, totalEnergy int64) (origin common.Address, originShare, callerShare int64) {
	if ctx.Tx.ContractType() != corepb.Transaction_Contract_TriggerSmartContract {
		return common.Address{}, 0, totalEnergy
	}
	if legacyVMReceiptEnergyLeftMode(ctx) {
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
	if !ctx.State.AccountExists(originAddr) && ctx.DynProps.AllowTvmConstantinople() {
		return common.Address{}, 0, totalEnergy
	}

	userPercent := clampPercent(contract.ConsumeUserResourcePercent)
	originPercent := 100 - userPercent
	if originPercent <= 0 {
		return common.Address{}, 0, totalEnergy
	}

	want := totalEnergy * originPercent / 100

	originLimit := contractOriginEnergyLimit(contract)
	originStakeLeft := availableAccountEnergyForBill(ctx.State, ctx.DynProps, originAddr, ctx.ResourceTime())
	if vmReceiptEnergyLeftMode(ctx) && result != nil && result.HasOriginEnergyLeft {
		originStakeLeft = result.OriginEnergyLeft
	}

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

func vmReceiptEnergyLeftMode(ctx *Context) bool {
	if ctx == nil || ctx.DynProps == nil {
		return false
	}
	return ctx.DynProps.AllowTvmFreeze() || ctx.DynProps.SupportUnfreezeDelay()
}

func legacyVMReceiptEnergyLeftMode(ctx *Context) bool {
	if ctx == nil || ctx.DynProps == nil {
		return false
	}
	// java-tron pre-ENERGY_LIMIT fork uses VMActuator's float-ratio energy
	// limit path. That path does not populate ReceiptCapsule.callerEnergyLeft
	// / originEnergyLeft, while ReceiptCapsule.payEnergyBill reads those
	// fields whenever allowTvmFreeze or supportUnfreezeDelay is active. The
	// effective stake-paid energy is therefore zero until the hard fork flips
	// VMActuator to the fixed-ratio path.
	return !energyLimitHardForkActive(ctx) && vmReceiptEnergyLeftMode(ctx)
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
	recovered := recoveredEnergyUsage(s, dp, acct, addr, now)
	if recovered >= limit {
		return 0
	}
	return limit - recovered
}

// recoveredEnergyUsage returns the caller's energy_usage decayed to `now`.
// In the V2 regime it mirrors java EnergyProcessor.getAccountLeftEnergyFromFreeze
// -> recovery(), which is the scaled increase over the PER-ACCOUNT window;
// computeEnergyIncrease with usage==0 reproduces it exactly (the recomputed
// window is discarded — recovery does not persist it). This keeps limit-time
// recovery consistent with the settle path. Pre-Stake-2.0 keeps the prior
// global-window behavior.
func recoveredEnergyUsage(s *state.StateDB, dp *state.DynamicProperties, acct *types.Account, addr common.Address, now int64) int64 {
	oldUsage := s.GetEnergyUsage(addr)
	oldTime := s.GetLatestConsumeTimeForEnergy(addr)
	if dp == nil || !dp.SupportUnfreezeDelay() {
		// Pre-Stake-2.0: java getAccountLeftEnergyFromFreeze -> recovery() ->
		// increase(usage,0,…) over the global 28800-slot window. Must match the
		// settle's recovery exactly, or limit-time available energy diverges.
		harden := dp != nil && dp.AllowHardenResourceCalculation()
		return computeEnergyIncreaseGlobal(oldUsage, 0, oldTime, now, harden)
	}
	recovered, _, _ := computeEnergyIncrease(
		acct.RawEnergyWindowSize(), acct.EnergyWindowOptimized(),
		oldUsage, 0, oldTime, now,
		dp.AllowHardenResourceCalculation(), dp.SupportCancelAllUnfreezeV2())
	return recovered
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

	// The IN-VM energy-limit duplicate in java-tron is
	// RepositoryImpl.calculateGlobalEnergyLimit (actuator module), which is
	// UNCONDITIONALLY V1 integer-truncated plain-float — NO supportUnfreezeDelay
	// (V2 fractional) branch and NO harden (BigInteger) branch. (The chainbase
	// EnergyProcessor V2 path mirrored by core/resource.go::availableAccountEnergy
	// is the separate RPC path and stays V2.) Keeping the sub-TRX fraction here
	// over-grants the in-VM energy limit for non-whole-TRX stakes and can flip
	// OUT_OF_ENERGY -> SUCCESS (same class as the 8,825,873 stall).
	if frozen < params.TRXPrecision {
		return 0
	}
	totalWeight := dp.TotalEnergyWeight()
	if totalWeight <= 0 {
		return 0
	}
	totalLimit := dp.TotalEnergyCurrentLimit()
	weight := frozen / params.TRXPrecision
	return int64(float64(weight) * (float64(totalLimit) / float64(totalWeight)))
}

const resourcePrecisionForEnergy = int64(1_000_000)

func divideCeilBigInt(numerator, denominator *big.Int) int64 {
	q, r := new(big.Int).QuoRem(numerator, denominator, new(big.Int))
	if r.Sign() > 0 {
		q.Add(q, big.NewInt(1))
	}
	return q.Int64()
}

func bigMulDivInt64(a, b, c int64) int64 {
	return bigMulDivBigInt64(big.NewInt(a), big.NewInt(b), big.NewInt(c))
}

func bigMulDivBigInt64(a, b, c *big.Int) int64 {
	n := new(big.Int).Mul(a, b)
	n.Quo(n, c)
	return n.Int64()
}

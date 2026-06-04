package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

// newEnergyBillCtx builds a Context whose tx is a TriggerSmartContract
// owned by `owner`. The TriggerSmartContract carrier is incidental — what
// PayEnergyBill needs is that ctx.Tx.Contract().GetOwnerAddress() returns
// the caller. The actual VM execution path is bypassed by callers, who
// hand-build a Result with a fixed EnergyUsageTotal.
func newEnergyBillCtx(t *testing.T, owner tcommon.Address) *Context {
	t.Helper()

	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}

	tsc := &contractpb.TriggerSmartContract{
		OwnerAddress:    owner[:],
		ContractAddress: tcommon.Address{0x41, 0x02}.Bytes(),
	}
	anyParam, err := anypb.New(tsc)
	if err != nil {
		t.Fatal(err)
	}

	tx := types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			FeeLimit: 1_000_000_000,
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_TriggerSmartContract,
					Parameter: anyParam,
				},
			},
		},
	})

	dp := state.NewDynamicProperties()
	dp.Set("energy_fee", 100)
	dp.SetLatestBlockHeaderNumber(blockNumForEnergyLimit)

	return &Context{
		State:         sdb,
		DynProps:      dp,
		Tx:            tx,
		BlockTime:     1_777_700_000_000,
		PrevBlockTime: 1_777_700_000_000,
		HeadSlot:      42,
		HasHeadSlot:   true,
		BlockNumber:   80_000,
	}
}

func TestPayEnergyBill_PreEnergyLimitForkIgnoresStakeWhenFreezeV2Enabled(t *testing.T) {
	owner := tcommon.Address{0x41, 0x99, 0x09}
	ctx := newEnergyBillCtx(t, owner)
	ctx.DynProps.SetLatestBlockHeaderNumber(blockNumForEnergyLimit - 1)
	ctx.DynProps.SetAllowTvmFreeze(true)
	ctx.DynProps.SetAllowBlackHoleOptimization(true)

	const initialBalance = int64(100_000_000)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, initialBalance)

	const frozen = int64(10_000) * params.TRXPrecision
	acct := ctx.State.GetAccount(owner)
	acct.AddFrozenEnergy(frozen, ctx.BlockTime+10_000_000)
	ctx.DynProps.SetTotalEnergyWeight(10_000)
	ctx.DynProps.SetTotalEnergyCurrentLimit(50_000)

	if got := availableAccountEnergyForBill(ctx.State, ctx.DynProps, owner, ctx.ResourceTime()); got < 50_000 {
		t.Fatalf("availableAccountEnergyForBill = %d, want stake available before legacy receipt gate", got)
	}

	const energyUsed = int64(32_121)
	expectedFee := energyUsed * 100
	result := &Result{EnergyUsageTotal: energyUsed, ContractRet: 1}

	if err := PayEnergyBill(ctx, result); err != nil {
		t.Fatalf("PayEnergyBill: %v", err)
	}
	if result.EnergyUsed != 0 {
		t.Errorf("EnergyUsed = %d, want 0 before ENERGY_LIMIT fork", result.EnergyUsed)
	}
	if result.EnergyFee != expectedFee {
		t.Errorf("EnergyFee = %d, want %d", result.EnergyFee, expectedFee)
	}
	if got := ctx.State.GetBalance(owner); got != initialBalance-expectedFee {
		t.Errorf("balance = %d, want %d", got, initialBalance-expectedFee)
	}
	if got := ctx.State.GetEnergyUsage(owner); got != 0 {
		t.Errorf("energy_usage = %d, want 0", got)
	}
}

// TestPayEnergyBill_BurnTrx mirrors the live cross-impl chain
// (config.conf private chain) where AllowOptimizeBlackHole=1 is the
// active routing branch. Reproduces the D-1 root cause: 16,915,000 sun
// of historical energy fees that gtron failed to debit. Asserts the
// caller's TRX balance drops by exactly energyUsage * energy_fee, and
// that DP.burn_trx_amount accumulates the same amount.
func TestPayEnergyBill_BurnTrx(t *testing.T) {
	owner := tcommon.Address{0x41, 0x99, 0x01}
	ctx := newEnergyBillCtx(t, owner)
	ctx.DynProps.SetAllowBlackHoleOptimization(true)
	ctx.DynProps.SetAllowTransactionFeePool(false)

	const initialBalance = int64(100_000_000)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, initialBalance)

	const energyUsed = int64(32_121) // matches block-13229 receipt on live chain
	result := &Result{EnergyUsageTotal: energyUsed, ContractRet: 1}

	if err := PayEnergyBill(ctx, result); err != nil {
		t.Fatalf("PayEnergyBill: %v", err)
	}

	expectedFee := energyUsed * 100
	if result.EnergyFee != expectedFee {
		t.Errorf("EnergyFee = %d, want %d", result.EnergyFee, expectedFee)
	}
	if result.EnergyUsed != 0 {
		t.Errorf("EnergyUsed = %d, want 0 (no stake)", result.EnergyUsed)
	}
	if got := ctx.State.GetBalance(owner); got != initialBalance-expectedFee {
		t.Errorf("balance = %d, want %d (debit %d sun)",
			got, initialBalance-expectedFee, expectedFee)
	}
	if got := ctx.DynProps.BurnTrxAmount(); got != expectedFee {
		t.Errorf("burn_trx_amount = %d, want %d", got, expectedFee)
	}
	// Pool branch must NOT be touched in this routing.
	if got := ctx.DynProps.TransactionFeePool(); got != 0 {
		t.Errorf("transaction_fee_pool = %d, want 0", got)
	}
	// Blackhole-account fallback must NOT be touched either.
	if got := ctx.State.GetBalance(params.BlackholeAddress); got != 0 {
		t.Errorf("blackhole balance = %d, want 0", got)
	}
	if got := ctx.State.GetLatestOperationTime(owner); got != ctx.PrevBlockTime {
		t.Errorf("latest_opration_time = %d, want %d", got, ctx.PrevBlockTime)
	}
	if got := ctx.State.GetLatestConsumeTimeForEnergy(owner); got != ctx.HeadSlot {
		t.Errorf("latest_consume_time_for_energy = %d, want %d", got, ctx.HeadSlot)
	}
}

// TestPayEnergyBill_TransactionFeePool covers the
// support_transaction_fee_pool branch. The pool is per-block-witness
// rebated downstream (java Manager.payReward), so the bill must land
// here NOT in burn_trx_amount even when blackhole-opt is also enabled.
func TestPayEnergyBill_TransactionFeePool(t *testing.T) {
	owner := tcommon.Address{0x41, 0x99, 0x02}
	ctx := newEnergyBillCtx(t, owner)
	ctx.DynProps.SetAllowTransactionFeePool(true)
	ctx.DynProps.SetAllowBlackHoleOptimization(true) // pool wins precedence

	const initialBalance = int64(50_000_000)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, initialBalance)

	const energyUsed = int64(9) // matches block-2343 receipt on live chain
	result := &Result{EnergyUsageTotal: energyUsed, ContractRet: 1}

	if err := PayEnergyBill(ctx, result); err != nil {
		t.Fatalf("PayEnergyBill: %v", err)
	}
	expectedFee := energyUsed * 100
	if got := ctx.DynProps.TransactionFeePool(); got != expectedFee {
		t.Errorf("transaction_fee_pool = %d, want %d", got, expectedFee)
	}
	if got := ctx.DynProps.BurnTrxAmount(); got != 0 {
		t.Errorf("burn_trx_amount = %d, want 0 (pool wins precedence)", got)
	}
	if got := ctx.State.GetBalance(owner); got != initialBalance-expectedFee {
		t.Errorf("balance = %d, want %d", got, initialBalance-expectedFee)
	}
}

// TestPayEnergyBill_TransactionFeePool_OutOfTime asserts the
// OUT_OF_TIME exception at line 304-305 of ReceiptCapsule.java: even
// when the pool is enabled, a tx that ran out of time falls through to
// blackhole / burnTrx so that SR-time-budget overrun fees aren't
// rebated to the SR.
func TestPayEnergyBill_TransactionFeePool_OutOfTime(t *testing.T) {
	owner := tcommon.Address{0x41, 0x99, 0x03}
	ctx := newEnergyBillCtx(t, owner)
	ctx.DynProps.SetAllowTransactionFeePool(true)
	ctx.DynProps.SetAllowBlackHoleOptimization(true)

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, 50_000_000)

	const energyUsed = int64(1_000)
	result := &Result{
		EnergyUsageTotal: energyUsed,
		ContractRet:      int32(corepb.Transaction_Result_OUT_OF_TIME),
	}

	if err := PayEnergyBill(ctx, result); err != nil {
		t.Fatalf("PayEnergyBill: %v", err)
	}
	expectedFee := energyUsed * 100
	if got := ctx.DynProps.TransactionFeePool(); got != 0 {
		t.Errorf("transaction_fee_pool = %d, want 0 (OUT_OF_TIME bypasses pool)", got)
	}
	if got := ctx.DynProps.BurnTrxAmount(); got != expectedFee {
		t.Errorf("burn_trx_amount = %d, want %d", got, expectedFee)
	}
}

// TestPayEnergyBill_BlackholeAddress covers the legacy branch where
// neither the fee pool nor blackhole optimization is active. The bill
// is credited to the genesis Blackhole account address. This path was
// the only available routing pre-proposal-49.
func TestPayEnergyBill_BlackholeAddress(t *testing.T) {
	owner := tcommon.Address{0x41, 0x99, 0x04}
	ctx := newEnergyBillCtx(t, owner)
	ctx.DynProps.SetAllowTransactionFeePool(false)
	ctx.DynProps.SetAllowBlackHoleOptimization(false)

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, 50_000_000)
	// Pre-create the Blackhole account so AddBalance has a target.
	ctx.State.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)

	const energyUsed = int64(500)
	result := &Result{EnergyUsageTotal: energyUsed, ContractRet: 1}

	if err := PayEnergyBill(ctx, result); err != nil {
		t.Fatalf("PayEnergyBill: %v", err)
	}
	expectedFee := energyUsed * 100
	if got := ctx.State.GetBalance(params.BlackholeAddress); got != expectedFee {
		t.Errorf("blackhole balance = %d, want %d", got, expectedFee)
	}
	if got := ctx.DynProps.BurnTrxAmount(); got != 0 {
		t.Errorf("burn_trx_amount = %d, want 0 (legacy routing)", got)
	}
	if got := ctx.DynProps.TransactionFeePool(); got != 0 {
		t.Errorf("transaction_fee_pool = %d, want 0 (legacy routing)", got)
	}
}

// TestPayEnergyBill_PureStakeNoBalanceDebit asserts that a caller with
// enough frozen-for-energy stake to cover the full VM bill does NOT
// pay any TRX. Mirrors java's early-return at ReceiptCapsule.java:277
// (`accountEnergyLeft >= usage`).
func TestPayEnergyBill_PureStakeNoBalanceDebit(t *testing.T) {
	owner := tcommon.Address{0x41, 0x99, 0x05}
	ctx := newEnergyBillCtx(t, owner)
	ctx.DynProps.SetAllowBlackHoleOptimization(true)

	const initialBalance = int64(100_000_000)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, initialBalance)

	// Set up the account with a frozen-for-energy stake big enough to
	// cover ~10k energy. Numbers are illustrative — we just need
	// availableAccountEnergyForBill to return >= EnergyUsageTotal.
	const frozen = int64(10_000) * params.TRXPrecision
	acct := ctx.State.GetAccount(owner)
	acct.AddFrozenEnergy(frozen, ctx.BlockTime+10_000_000)

	// Total energy weight = our stake / 1e6 (sun-per-TRX): 10k weight.
	// Total current limit chosen so that share = 50_000 energy.
	ctx.DynProps.SetTotalEnergyWeight(10_000)
	ctx.DynProps.SetTotalEnergyCurrentLimit(50_000)

	// Sanity: helper should report >= 50_000 entitled.
	got := availableAccountEnergyForBill(ctx.State, ctx.DynProps, owner, ctx.ResourceTime())
	if got < 50_000 {
		t.Fatalf("availableAccountEnergyForBill = %d, want >= 50000", got)
	}

	const energyUsed = int64(32_121)
	result := &Result{EnergyUsageTotal: energyUsed, ContractRet: 1}

	if err := PayEnergyBill(ctx, result); err != nil {
		t.Fatalf("PayEnergyBill: %v", err)
	}
	if result.EnergyFee != 0 {
		t.Errorf("EnergyFee = %d, want 0 (pure-stake path)", result.EnergyFee)
	}
	if result.EnergyUsed != energyUsed {
		t.Errorf("EnergyUsed = %d, want %d (full stake-paid)", result.EnergyUsed, energyUsed)
	}
	if got := ctx.State.GetBalance(owner); got != initialBalance {
		t.Errorf("balance = %d, want %d (no debit)", got, initialBalance)
	}
	if got := ctx.DynProps.BurnTrxAmount(); got != 0 {
		t.Errorf("burn_trx_amount = %d, want 0 (no SUN routed)", got)
	}
	// energy_usage counter on the account should reflect the stake spend.
	if got := ctx.State.GetEnergyUsage(owner); got != energyUsed {
		t.Errorf("energy_usage = %d, want %d", got, energyUsed)
	}
	if got := ctx.State.GetLatestOperationTime(owner); got != ctx.PrevBlockTime {
		t.Errorf("latest_opration_time = %d, want %d", got, ctx.PrevBlockTime)
	}
}

// TestPayEnergyBill_PartialStakeOverage asserts the mixed path: stake
// covers some energy, the remainder is billed in TRX. Tests both fields
// of the Result split (EnergyUsed = stake portion, EnergyFee = SUN bill
// for the overage).
func TestPayEnergyBill_PartialStakeOverage(t *testing.T) {
	owner := tcommon.Address{0x41, 0x99, 0x06}
	ctx := newEnergyBillCtx(t, owner)
	ctx.DynProps.SetAllowBlackHoleOptimization(true)

	const initialBalance = int64(100_000_000)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, initialBalance)

	// Stake covers exactly 5_000 energy.
	const frozen = int64(1_000) * params.TRXPrecision
	acct := ctx.State.GetAccount(owner)
	acct.AddFrozenEnergy(frozen, ctx.BlockTime+10_000_000)
	ctx.DynProps.SetTotalEnergyWeight(1_000)
	ctx.DynProps.SetTotalEnergyCurrentLimit(5_000)

	got := availableAccountEnergyForBill(ctx.State, ctx.DynProps, owner, ctx.ResourceTime())
	if got != 5_000 {
		t.Fatalf("availableAccountEnergyForBill = %d, want 5000", got)
	}

	const energyUsed = int64(8_000) // 5000 from stake, 3000 from balance
	const expectedStake = int64(5_000)
	const expectedOverage = int64(3_000)
	expectedFee := expectedOverage * 100

	result := &Result{EnergyUsageTotal: energyUsed, ContractRet: 1}
	if err := PayEnergyBill(ctx, result); err != nil {
		t.Fatalf("PayEnergyBill: %v", err)
	}
	if result.EnergyUsed != expectedStake {
		t.Errorf("EnergyUsed = %d, want %d", result.EnergyUsed, expectedStake)
	}
	if result.EnergyFee != expectedFee {
		t.Errorf("EnergyFee = %d, want %d", result.EnergyFee, expectedFee)
	}
	if got := ctx.State.GetBalance(owner); got != initialBalance-expectedFee {
		t.Errorf("balance = %d, want %d", got, initialBalance-expectedFee)
	}
	if got := ctx.DynProps.BurnTrxAmount(); got != expectedFee {
		t.Errorf("burn_trx_amount = %d, want %d", got, expectedFee)
	}
	// Stake counter takes the stake portion only.
	if got := ctx.State.GetEnergyUsage(owner); got != expectedStake {
		t.Errorf("energy_usage = %d, want %d", got, expectedStake)
	}
}

// TestPayEnergyBill_NoEnergyNoOp asserts the early-return at the top of
// ReceiptCapsule.payEnergyBill: when EnergyUsageTotal is zero (e.g.,
// non-VM tx routed through ApplyTransaction), no balance side-effects.
func TestPayEnergyBill_NoEnergyNoOp(t *testing.T) {
	owner := tcommon.Address{0x41, 0x99, 0x07}
	ctx := newEnergyBillCtx(t, owner)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, 50_000_000)

	result := &Result{EnergyUsageTotal: 0, ContractRet: 1, OriginEnergyUsage: 12345}
	if err := PayEnergyBill(ctx, result); err != nil {
		t.Fatalf("PayEnergyBill: %v", err)
	}
	if got := ctx.State.GetBalance(owner); got != 50_000_000 {
		t.Errorf("balance = %d, want untouched", got)
	}
	if result.EnergyUsed != 0 || result.EnergyFee != 0 || result.OriginEnergyUsage != 0 {
		t.Errorf("expected zero energy fields, got EnergyUsed=%d EnergyFee=%d OriginEnergyUsage=%d",
			result.EnergyUsed, result.EnergyFee, result.OriginEnergyUsage)
	}
}

// TestPayEnergyBill_InsufficientBalance asserts that the BalanceInsufficient
// path returns an error so the caller can revert the snapshot. Mirrors
// java's BalanceInsufficientException at line 299 of ReceiptCapsule.java.
func TestPayEnergyBill_InsufficientBalance(t *testing.T) {
	owner := tcommon.Address{0x41, 0x99, 0x08}
	ctx := newEnergyBillCtx(t, owner)
	ctx.DynProps.SetAllowBlackHoleOptimization(true)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, 100) // way too little for the bill

	result := &Result{EnergyUsageTotal: 100_000, ContractRet: 1}
	err := PayEnergyBill(ctx, result)
	if err == nil {
		t.Fatal("expected insufficient-balance error, got nil")
	}
}

// installOriginContract writes a SmartContract record to state at
// `contractAddr` with the given `origin` deployer, user percent, and limit.
// java-tron bills the origin for 100 - consume_user_resource_percent.
func installOriginContract(t *testing.T, ctx *Context, contractAddr, origin tcommon.Address, percent, originLimit int64) {
	t.Helper()
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		ContractAddress:            contractAddr.Bytes(),
		OriginAddress:              origin.Bytes(),
		ConsumeUserResourcePercent: percent,
		OriginEnergyLimit:          originLimit,
	})
}

// stakeForEnergyBill seeds an account with enough frozen-V2 energy stake
// to make `availableAccountEnergyForBill` return exactly `targetEnergy`
// (modulo a 1-unit rounding tolerance from float math). Uses the modern
// V2 path (UnfreezeDelayDays > 0) which is the live cross-impl chain
// configuration.
//
// Math: calcAccountEnergyLimit returns
// (frozen / TRXPrecision) * (totalLimit / totalWeight).
// We pin totalLimit == totalWeight so the ratio is 1.0 — frozen TRX
// units map 1:1 to energy units. Then frozen = targetEnergy * TRXPrecision.
func stakeForEnergyBill(t *testing.T, ctx *Context, addr tcommon.Address, targetEnergy int64) {
	t.Helper()
	if targetEnergy <= 0 {
		return
	}
	// 1 TRX (1_000_000 sun) of frozen weight ↔ 1 energy unit. Pin both
	// limit and weight unconditionally — DynamicProperties' default
	// `total_energy_current_limit` is 5×10^10, so a conditional override
	// would silently miss and skew the ratio.
	ctx.DynProps.SetTotalEnergyWeight(1_000_000_000_000)              // 10^12
	ctx.DynProps.Set("total_energy_current_limit", 1_000_000_000_000) // 10^12
	ctx.DynProps.Set("unfreeze_delay_days", 14)

	if !ctx.State.AccountExists(addr) {
		ctx.State.CreateAccount(addr, corepb.AccountType_Normal)
	}
	// Round up by 1 TRX so the float math gives at least targetEnergy.
	frozenSun := (targetEnergy + 1) * params.TRXPrecision
	ctx.State.AddFreezeV2(addr, corepb.ResourceCode_ENERGY, frozenSun)
}

// TestPayEnergyBill_OriginSplit_HappyPath: TriggerSmartContract with
// caller != origin, percent=50, both have stake; origin absorbs 50%
// against its stake (no balance debit), caller absorbs 50% against its
// stake. No SUN leaves either balance.
func TestPayEnergyBill_OriginSplit_HappyPath(t *testing.T) {
	caller := tcommon.Address{0x41, 0xAA, 0x01}
	origin := tcommon.Address{0x41, 0xBB, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}

	ctx := newEnergyBillCtx(t, caller)
	ctx.DynProps.SetAllowBlackHoleOptimization(true)

	installOriginContract(t, ctx, contractAddr, origin, 50, 1_000_000)
	stakeForEnergyBill(t, ctx, caller, 1_000_000)
	stakeForEnergyBill(t, ctx, origin, 1_000_000)

	const initialBalance = int64(100_000_000)
	ctx.State.AddBalance(caller, initialBalance)
	ctx.State.AddBalance(origin, initialBalance)

	const totalEnergy = int64(20_000)
	result := &Result{EnergyUsageTotal: totalEnergy, ContractRet: 1}
	if err := PayEnergyBill(ctx, result); err != nil {
		t.Fatalf("PayEnergyBill: %v", err)
	}

	// origin absorbs exactly 50% (=10_000); caller absorbs the rest.
	if result.OriginEnergyUsage != totalEnergy/2 {
		t.Errorf("OriginEnergyUsage = %d, want %d", result.OriginEnergyUsage, totalEnergy/2)
	}
	if result.EnergyUsed != totalEnergy/2 {
		t.Errorf("EnergyUsed (caller stake) = %d, want %d", result.EnergyUsed, totalEnergy/2)
	}
	// No balance leaves either account: both shares fit within stake.
	if got := ctx.State.GetBalance(caller); got != initialBalance {
		t.Errorf("caller balance = %d, want %d (no balance debit)", got, initialBalance)
	}
	if got := ctx.State.GetBalance(origin); got != initialBalance {
		t.Errorf("origin balance = %d, want %d (origin must NEVER pay TRX)", got, initialBalance)
	}
	// Both energy_usage counters bumped.
	if got := ctx.State.GetEnergyUsage(caller); got != totalEnergy/2 {
		t.Errorf("caller energy_usage = %d, want %d", got, totalEnergy/2)
	}
	if got := ctx.State.GetEnergyUsage(origin); got != totalEnergy/2 {
		t.Errorf("origin energy_usage = %d, want %d", got, totalEnergy/2)
	}
	if got := ctx.State.GetLatestOperationTime(caller); got != ctx.PrevBlockTime {
		t.Errorf("caller latest_opration_time = %d, want %d", got, ctx.PrevBlockTime)
	}
	if got := ctx.State.GetLatestOperationTime(origin); got != ctx.PrevBlockTime {
		t.Errorf("origin latest_opration_time = %d, want %d", got, ctx.PrevBlockTime)
	}
	// burn_trx_amount untouched since no balance debit happened.
	if got := ctx.DynProps.BurnTrxAmount(); got != 0 {
		t.Errorf("burn_trx_amount = %d, want 0 (no balance bill on this path)", got)
	}
}

// TestPayEnergyBill_OriginSplit_OriginCappedByLimit: user percent=80 leaves
// 20% for origin, and origin_energy_limit caps that share; the
// uncapped portion spills back to the caller's bill.
func TestPayEnergyBill_OriginSplit_OriginCappedByLimit(t *testing.T) {
	caller := tcommon.Address{0x41, 0xAA, 0x02}
	origin := tcommon.Address{0x41, 0xBB, 0x02}
	contractAddr := tcommon.Address{0x41, 0x02}

	ctx := newEnergyBillCtx(t, caller)
	ctx.DynProps.SetAllowBlackHoleOptimization(true)

	const totalEnergy = int64(10_000)
	const originLimit = int64(2_000) // forces origin to absorb at most 2_000

	installOriginContract(t, ctx, contractAddr, origin, 80, originLimit)
	// Plenty of stake on both, but the limit is the tighter constraint.
	stakeForEnergyBill(t, ctx, caller, totalEnergy)
	stakeForEnergyBill(t, ctx, origin, totalEnergy)

	ctx.State.AddBalance(caller, 100_000_000)

	result := &Result{EnergyUsageTotal: totalEnergy, ContractRet: 1}
	if err := PayEnergyBill(ctx, result); err != nil {
		t.Fatalf("PayEnergyBill: %v", err)
	}

	if result.OriginEnergyUsage != originLimit {
		t.Errorf("OriginEnergyUsage = %d, want %d (capped by origin_energy_limit)", result.OriginEnergyUsage, originLimit)
	}
	expectedCaller := totalEnergy - originLimit
	if result.EnergyUsed != expectedCaller {
		t.Errorf("caller EnergyUsed = %d, want %d", result.EnergyUsed, expectedCaller)
	}
}

// TestPayEnergyBill_OriginSplit_OriginCappedByStake: origin has less
// stake-energy than its 50% share would demand; the uncovered portion
// flows back to caller (who pays it via its own stake or balance).
func TestPayEnergyBill_OriginSplit_OriginCappedByStake(t *testing.T) {
	caller := tcommon.Address{0x41, 0xAA, 0x03}
	origin := tcommon.Address{0x41, 0xBB, 0x03}
	contractAddr := tcommon.Address{0x41, 0x02}

	ctx := newEnergyBillCtx(t, caller)
	ctx.DynProps.SetAllowBlackHoleOptimization(true)

	const totalEnergy = int64(10_000)
	const originStake = int64(1_500) // less than the 5_000 percent share

	installOriginContract(t, ctx, contractAddr, origin, 50, totalEnergy)
	stakeForEnergyBill(t, ctx, caller, totalEnergy)
	stakeForEnergyBill(t, ctx, origin, originStake)

	const initialBalance = int64(100_000_000)
	ctx.State.AddBalance(caller, initialBalance)

	result := &Result{EnergyUsageTotal: totalEnergy, ContractRet: 1}
	if err := PayEnergyBill(ctx, result); err != nil {
		t.Fatalf("PayEnergyBill: %v", err)
	}

	// availableAccountEnergyForBill rounds via float math, so origin's
	// usable share is somewhere in [originStake, originStake+1]. The
	// invariant we lock down is that origin's share ≤ originStake+1
	// (cap by stake) and caller covers the rest.
	if result.OriginEnergyUsage > originStake+1 || result.OriginEnergyUsage < originStake-1 {
		t.Errorf("OriginEnergyUsage = %d, want ~%d (capped by stake)", result.OriginEnergyUsage, originStake)
	}
	expectedCaller := totalEnergy - result.OriginEnergyUsage
	if result.EnergyUsed != expectedCaller {
		t.Errorf("caller EnergyUsed = %d, want %d (rest after origin's share)",
			result.EnergyUsed, expectedCaller)
	}
}

// TestPayEnergyBill_OriginSplit_PercentZero: when the contract's
// ConsumeUserResourcePercent is 0, java bills the origin for up to
// origin_energy_limit/frozen-energy and spills the rest to the caller.
func TestPayEnergyBill_OriginSplit_PercentZeroBillsOrigin(t *testing.T) {
	caller := tcommon.Address{0x41, 0xAA, 0x04}
	origin := tcommon.Address{0x41, 0xBB, 0x04}
	contractAddr := tcommon.Address{0x41, 0x02}

	ctx := newEnergyBillCtx(t, caller)
	ctx.DynProps.SetAllowBlackHoleOptimization(true)

	const totalEnergy = int64(5_000)
	const originLimit = int64(2_000)
	installOriginContract(t, ctx, contractAddr, origin, 0, originLimit)
	stakeForEnergyBill(t, ctx, origin, totalEnergy*10)

	ctx.State.CreateAccount(caller, corepb.AccountType_Normal)
	ctx.State.AddBalance(caller, 100_000_000)

	result := &Result{EnergyUsageTotal: totalEnergy, ContractRet: 1}
	if err := PayEnergyBill(ctx, result); err != nil {
		t.Fatalf("PayEnergyBill: %v", err)
	}

	if result.OriginEnergyUsage != originLimit {
		t.Errorf("OriginEnergyUsage = %d, want %d", result.OriginEnergyUsage, originLimit)
	}
	if got := ctx.State.GetEnergyUsage(origin); got != originLimit {
		t.Errorf("origin energy_usage = %d, want %d", got, originLimit)
	}
	expectedCallerFee := (totalEnergy - originLimit) * 100
	if result.EnergyFee != expectedCallerFee {
		t.Errorf("EnergyFee = %d, want %d", result.EnergyFee, expectedCallerFee)
	}
}

func TestPayEnergyBill_OriginSplit_PercentHundredBillsCaller(t *testing.T) {
	caller := tcommon.Address{0x41, 0xAA, 0x06}
	origin := tcommon.Address{0x41, 0xBB, 0x06}
	contractAddr := tcommon.Address{0x41, 0x02}

	ctx := newEnergyBillCtx(t, caller)
	ctx.DynProps.SetAllowBlackHoleOptimization(true)

	const totalEnergy = int64(5_000)
	installOriginContract(t, ctx, contractAddr, origin, 100, 100_000)
	stakeForEnergyBill(t, ctx, origin, totalEnergy*10)

	ctx.State.CreateAccount(caller, corepb.AccountType_Normal)
	ctx.State.AddBalance(caller, 100_000_000)

	result := &Result{EnergyUsageTotal: totalEnergy, ContractRet: 1}
	if err := PayEnergyBill(ctx, result); err != nil {
		t.Fatalf("PayEnergyBill: %v", err)
	}

	if result.OriginEnergyUsage != 0 {
		t.Errorf("OriginEnergyUsage = %d, want 0", result.OriginEnergyUsage)
	}
	if got := ctx.State.GetEnergyUsage(origin); got != 0 {
		t.Errorf("origin energy_usage = %d, want 0", got)
	}
}

// TestPayEnergyBill_OriginSplit_CallerEqualsOrigin: when caller IS the
// contract's deployer (common case for owner-only admin functions),
// java skips the split and bills caller directly.
func TestPayEnergyBill_OriginSplit_CallerEqualsOrigin(t *testing.T) {
	caller := tcommon.Address{0x41, 0xAA, 0x05}
	contractAddr := tcommon.Address{0x41, 0x02}

	ctx := newEnergyBillCtx(t, caller)
	ctx.DynProps.SetAllowBlackHoleOptimization(true)

	installOriginContract(t, ctx, contractAddr, caller, 50, 1_000_000)
	ctx.State.CreateAccount(caller, corepb.AccountType_Normal)
	ctx.State.AddBalance(caller, 100_000_000)

	const totalEnergy = int64(5_000)
	result := &Result{EnergyUsageTotal: totalEnergy, ContractRet: 1}
	if err := PayEnergyBill(ctx, result); err != nil {
		t.Fatalf("PayEnergyBill: %v", err)
	}

	if result.OriginEnergyUsage != 0 {
		t.Errorf("OriginEnergyUsage = %d, want 0 (caller==origin path)", result.OriginEnergyUsage)
	}
	// Caller paid the whole thing via balance (no caller stake set up).
	expectedFee := totalEnergy * 100
	if result.EnergyFee != expectedFee {
		t.Errorf("EnergyFee = %d, want %d", result.EnergyFee, expectedFee)
	}
}

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

	return &Context{
		State:       sdb,
		DynProps:    dp,
		Tx:          tx,
		BlockTime:   1_777_700_000_000,
		BlockNumber: 80_000,
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
	got := availableAccountEnergyForBill(ctx.State, ctx.DynProps, owner, ctx.BlockTime)
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

	got := availableAccountEnergyForBill(ctx.State, ctx.DynProps, owner, ctx.BlockTime)
	if got != 5_000 {
		t.Fatalf("availableAccountEnergyForBill = %d, want 5000", got)
	}

	const energyUsed = int64(8_000)        // 5000 from stake, 3000 from balance
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

	result := &Result{EnergyUsageTotal: 0, ContractRet: 1}
	if err := PayEnergyBill(ctx, result); err != nil {
		t.Fatalf("PayEnergyBill: %v", err)
	}
	if got := ctx.State.GetBalance(owner); got != 50_000_000 {
		t.Errorf("balance = %d, want untouched", got)
	}
	if result.EnergyUsed != 0 || result.EnergyFee != 0 {
		t.Errorf("expected zero energy fields, got EnergyUsed=%d EnergyFee=%d",
			result.EnergyUsed, result.EnergyFee)
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

package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

// makeTransferTxWithSigs returns a TransferContract tx with nSigs signatures.
// The signature bytes are placeholder values (not cryptographically valid).
func makeTransferTxWithSigs(from, to byte, amount int64, nSigs int) *types.Transaction {
	fromAddr := makeTestAddr(from)
	toAddr := makeTestAddr(to)
	transfer := &contractpb.TransferContract{
		OwnerAddress: fromAddr.Bytes(),
		ToAddress:    toAddr.Bytes(),
		Amount:       amount,
	}
	anyParam, _ := anypb.New(transfer)
	sigs := make([][]byte, nSigs)
	for i := range sigs {
		sigs[i] = make([]byte, 65)
		sigs[i][0] = byte(i + 1)
	}
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_TransferContract,
					Parameter: anyParam,
				},
			},
		},
		Signature: sigs,
	}
	return types.NewTransactionFromPB(pb)
}

// makeTransferTxWithMemo returns a TransferContract tx with the given memo data.
func makeTransferTxWithMemo(from, to byte, amount int64, memo []byte) *types.Transaction {
	fromAddr := makeTestAddr(from)
	toAddr := makeTestAddr(to)
	transfer := &contractpb.TransferContract{
		OwnerAddress: fromAddr.Bytes(),
		ToAddress:    toAddr.Bytes(),
		Amount:       amount,
	}
	anyParam, _ := anypb.New(transfer)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_TransferContract,
					Parameter: anyParam,
				},
			},
			Data: memo,
		},
	}
	return types.NewTransactionFromPB(pb)
}

// ── ConsumeMultiSignFee ──────────────────────────────────────────────────────

func TestConsumeMultiSignFee_charged(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 10_000_000)

	const multiSignFee = 1_000_000
	tx := makeTransferTxWithSigs(1, 2, 1_000, 2) // 2 signatures → multi-sig

	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowMultiSign(true)
	ctx.DynProps.SetMultiSignFee(multiSignFee)

	if err := ConsumeMultiSignFee(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := statedb.GetBalance(owner)
	want := int64(10_000_000 - multiSignFee)
	if got != want {
		t.Errorf("balance after multi-sign fee: got %d, want %d", got, want)
	}
}

func TestConsumeMultiSignFee_skipped_single_sig(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 10_000_000)

	tx := makeTransferTxWithSigs(1, 2, 1_000, 1) // 1 signature — not multi-sig

	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowMultiSign(true)
	ctx.DynProps.SetMultiSignFee(1_000_000)

	if err := ConsumeMultiSignFee(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := statedb.GetBalance(owner)
	if got != 10_000_000 {
		t.Errorf("single-sig should not be charged: got balance %d", got)
	}
}

func TestConsumeMultiSignFee_skipped_flag_off(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 10_000_000)

	tx := makeTransferTxWithSigs(1, 2, 1_000, 3) // 3 sigs but multi-sign not enabled

	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowMultiSign(false) // gate off
	ctx.DynProps.SetMultiSignFee(1_000_000)

	if err := ConsumeMultiSignFee(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := statedb.GetBalance(owner)
	if got != 10_000_000 {
		t.Errorf("flag-off should not charge: got balance %d", got)
	}
}

func TestConsumeMultiSignFee_zero_fee(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 10_000_000)

	tx := makeTransferTxWithSigs(1, 2, 1_000, 2)

	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowMultiSign(true)
	ctx.DynProps.SetMultiSignFee(0) // fee is 0 — no charge

	if err := ConsumeMultiSignFee(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := statedb.GetBalance(owner)
	if got != 10_000_000 {
		t.Errorf("zero fee should not charge: got balance %d", got)
	}
}

// ── ConsumeMemoFee ───────────────────────────────────────────────────────────

func TestConsumeMemoFee_charged(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 10_000_000)

	const memoFee = 500_000
	tx := makeTransferTxWithMemo(1, 2, 1_000, []byte("hello world"))

	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetMemoFee(memoFee)

	if err := ConsumeMemoFee(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := statedb.GetBalance(owner)
	want := int64(10_000_000 - memoFee)
	if got != want {
		t.Errorf("balance after memo fee: got %d, want %d", got, want)
	}
}

func TestConsumeMemoFee_skipped_empty_memo(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 10_000_000)

	tx := makeTransferTxWithMemo(1, 2, 1_000, nil) // no memo

	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetMemoFee(500_000)

	if err := ConsumeMemoFee(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := statedb.GetBalance(owner); got != 10_000_000 {
		t.Errorf("empty memo should not charge: got balance %d", got)
	}
}

func TestConsumeMemoFee_skipped_zero_fee(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 10_000_000)

	tx := makeTransferTxWithMemo(1, 2, 1_000, []byte("hello"))

	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetMemoFee(0) // governance default — 0

	if err := ConsumeMemoFee(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := statedb.GetBalance(owner); got != 10_000_000 {
		t.Errorf("zero memo fee should not charge: got balance %d", got)
	}
}

// ── burn_trx_amount accumulation ─────────────────────────────────────────────

func TestBurnTrxAmount_accumulates_on_blackhole_optimization(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 10_000_000)

	tx := makeTransferTxWithSigs(1, 2, 1_000, 2)
	txMemo := makeTransferTxWithMemo(1, 2, 1_000, []byte("memo"))

	// Build a context with AllowBlackholeOptimization active (blockNum high enough
	// that forks.IsActive returns true — set the DP flag directly).
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowMultiSign(true)
	ctx.DynProps.SetMultiSignFee(1_000_000)
	ctx.DynProps.SetMemoFee(500_000)
	ctx.DynProps.Set("allow_blackhole_optimization", 1)

	// Charge multi-sign fee on tx1 (1_000_000 burned)
	if err := ConsumeMultiSignFee(ctx); err != nil {
		t.Fatalf("multi-sign fee: %v", err)
	}

	// Charge memo fee on tx2 — reuse owner, update tx in ctx
	ctx.Tx = txMemo
	if err := ConsumeMemoFee(ctx); err != nil {
		t.Fatalf("memo fee: %v", err)
	}

	got := ctx.DynProps.BurnTrxAmount()
	want := int64(1_000_000 + 500_000)
	if got != want {
		t.Errorf("BurnTrxAmount: got %d, want %d", got, want)
	}
}

func TestBurnTrxAmount_zero_before_blackhole_optimization(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	// Need enough balance for both fees
	seedAccount(statedb, owner, 10_000_000)

	// Seed the blackhole address so AddBalance succeeds on the pre-fork path.
	statedb.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)

	tx := makeTransferTxWithSigs(1, 2, 1_000, 2)

	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowMultiSign(true)
	ctx.DynProps.SetMultiSignFee(1_000_000)
	// allow_blackhole_optimization stays 0 (default) → pre-fork path

	if err := ConsumeMultiSignFee(ctx); err != nil {
		t.Fatalf("multi-sign fee: %v", err)
	}

	if got := ctx.DynProps.BurnTrxAmount(); got != 0 {
		t.Errorf("BurnTrxAmount should be 0 before blackhole optimization: got %d", got)
	}
}

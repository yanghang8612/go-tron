package actuator

import (
	"encoding/hex"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

// mustDecodeHex decodes a hex string into bytes, failing the test on error.
func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("decode hex %q: %v", s, err)
	}
	return b
}

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

// makeShieldedTxWithSigs returns a ShieldedTransferContract tx whose sender is
// carried in transparent_from_address (NOT owner_address), with nSigs signatures.
// java-tron's TransactionCapsule.getOwner has an explicit ShieldedTransferContract
// case returning transparent_from_address.
func makeShieldedTxWithSigs(transparentFrom []byte, nSigs int) *types.Transaction {
	shielded := &contractpb.ShieldedTransferContract{
		TransparentFromAddress: transparentFrom,
	}
	anyParam, _ := anypb.New(shielded)
	sigs := make([][]byte, nSigs)
	for i := range sigs {
		sigs[i] = make([]byte, 65)
		sigs[i][0] = byte(i + 1)
	}
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_ShieldedTransferContract,
					Parameter: anyParam,
				},
			},
		},
		Signature: sigs,
	}
	return types.NewTransactionFromPB(pb)
}

// makeShieldedTxWithMemo returns a ShieldedTransferContract tx with the given
// transparent sender and memo data.
func makeShieldedTxWithMemo(transparentFrom []byte, memo []byte) *types.Transaction {
	shielded := &contractpb.ShieldedTransferContract{
		TransparentFromAddress: transparentFrom,
	}
	anyParam, _ := anypb.New(shielded)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_ShieldedTransferContract,
					Parameter: anyParam,
				},
			},
			Data: memo,
		},
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

	charged, err := ConsumeMultiSignFee(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if charged != multiSignFee {
		t.Fatalf("charged multi-sign fee: got %d, want %d", charged, multiSignFee)
	}

	got := statedb.GetBalance(owner)
	want := int64(10_000_000 - multiSignFee)
	if got != want {
		t.Errorf("balance after multi-sign fee: got %d, want %d", got, want)
	}
}

// TestConsumeMultiSignFee_shielded_transparent_from mirrors Nile block 1,818,883:
// a 2-signature ShieldedTransferContract whose sender lives in
// transparent_from_address (416c0214c9995c6f3a61ab23f0eb84b0cde7fd9c7c). java-tron
// charges 1_000_000 sun multi_sign_fee for it via TransactionCapsule.getOwner's
// ShieldedTransferContract case; go-tron previously returned the zero address and
// charged 0.
func TestConsumeMultiSignFee_shielded_transparent_from(t *testing.T) {
	statedb := setupStateDB(t)
	transparentFrom := mustDecodeHex(t, "416c0214c9995c6f3a61ab23f0eb84b0cde7fd9c7c")
	owner := common.BytesToAddress(transparentFrom)
	seedAccount(statedb, owner, 10_000_000)

	const multiSignFee = 1_000_000
	tx := makeShieldedTxWithSigs(transparentFrom, 2) // 2 signatures → multi-sig

	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowMultiSign(true)
	ctx.DynProps.SetMultiSignFee(multiSignFee)

	charged, err := ConsumeMultiSignFee(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if charged != multiSignFee {
		t.Fatalf("charged multi-sign fee for shielded transparent_from: got %d, want %d", charged, multiSignFee)
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

	if _, err := ConsumeMultiSignFee(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := statedb.GetBalance(owner)
	if got != 10_000_000 {
		t.Errorf("single-sig should not be charged: got balance %d", got)
	}
}

func TestConsumeMultiSignFeeCountsPQSignatures(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 10_000_000)
	ctx := setupContext(t, statedb, makeTransferTxWithSigs(1, 2, 1_000, 0))
	ctx.Tx.Proto().Signature = nil
	ctx.Tx.Proto().PqAuthSig = []*corepb.PQAuthSig{
		{Scheme: corepb.PQScheme_FN_DSA_512},
		{Scheme: corepb.PQScheme_FN_DSA_512},
	}
	ctx.DynProps.SetMultiSignFee(1_000_000)
	charged, err := ConsumeMultiSignFee(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if charged != 1_000_000 || ctx.State.GetBalance(owner) != 9_000_000 {
		t.Fatalf("PQ multi-sign charge=%d balance=%d", charged, ctx.State.GetBalance(owner))
	}
}

// TestConsumeMultiSignFee_charged_regardless_of_allow_multisign locks java
// parity: Manager.consumeMultiSignFee charges on signatureCount > 1 with NO
// AllowMultiSign guard (MULTI_SIGN_FEE is 1_000_000 from genesis in both impls).
// The guard is latent in practice — a >1-sig tx cannot validate before
// AllowMultiSign activates — but go-tron must not gate on it.
func TestConsumeMultiSignFee_charged_regardless_of_allow_multisign(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 10_000_000)

	const multiSignFee = 1_000_000
	tx := makeTransferTxWithSigs(1, 2, 1_000, 3) // 3 sigs → multi-sig

	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowMultiSign(false) // gate off — java still charges
	ctx.DynProps.SetMultiSignFee(multiSignFee)

	charged, err := ConsumeMultiSignFee(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if charged != multiSignFee {
		t.Fatalf("multi-sign fee must be charged regardless of allow_multi_sign: got %d, want %d", charged, multiSignFee)
	}

	got := statedb.GetBalance(owner)
	want := int64(10_000_000 - multiSignFee)
	if got != want {
		t.Errorf("balance after multi-sign fee: got %d, want %d", got, want)
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

	if _, err := ConsumeMultiSignFee(ctx); err != nil {
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

	charged, err := ConsumeMemoFee(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if charged != memoFee {
		t.Fatalf("charged memo fee: got %d, want %d", charged, memoFee)
	}

	got := statedb.GetBalance(owner)
	want := int64(10_000_000 - memoFee)
	if got != want {
		t.Errorf("balance after memo fee: got %d, want %d", got, want)
	}
}

// TestConsumeMemoFee_shielded_transparent_from is the memo-fee twin of the
// multi-sign golden case: a ShieldedTransferContract carrying a raw_data.data memo
// and a transparent sender must be charged the memo fee (java getOwner returns
// transparent_from_address; go-tron previously returned the zero address → 0 fee).
func TestConsumeMemoFee_shielded_transparent_from(t *testing.T) {
	statedb := setupStateDB(t)
	transparentFrom := mustDecodeHex(t, "416c0214c9995c6f3a61ab23f0eb84b0cde7fd9c7c")
	owner := common.BytesToAddress(transparentFrom)
	seedAccount(statedb, owner, 10_000_000)

	const memoFee = 500_000
	tx := makeShieldedTxWithMemo(transparentFrom, []byte("shielded memo"))

	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetMemoFee(memoFee)

	charged, err := ConsumeMemoFee(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if charged != memoFee {
		t.Fatalf("charged memo fee for shielded transparent_from: got %d, want %d", charged, memoFee)
	}

	got := statedb.GetBalance(owner)
	want := int64(10_000_000 - memoFee)
	if got != want {
		t.Errorf("balance after memo fee: got %d, want %d", got, want)
	}
}

// TestConsumeFee_shielded_empty_transparent_from locks the parity boundary: a
// fully-shielded transfer (empty transparent_from_address) has no owner — java's
// getOwner returns new byte[0], so neither multi-sign nor memo fee is charged.
func TestConsumeFee_shielded_empty_transparent_from(t *testing.T) {
	statedb := setupStateDB(t)

	multiSignTx := makeShieldedTxWithSigs(nil, 2)         // empty transparent_from, 2 sigs
	memoTx := makeShieldedTxWithMemo(nil, []byte("memo")) // empty transparent_from, memo

	ctx := setupContext(t, statedb, multiSignTx)
	ctx.DynProps.SetAllowMultiSign(true)
	ctx.DynProps.SetMultiSignFee(1_000_000)
	ctx.DynProps.SetMemoFee(500_000)

	charged, err := ConsumeMultiSignFee(ctx)
	if err != nil {
		t.Fatalf("multi-sign fee: unexpected error: %v", err)
	}
	if charged != 0 {
		t.Errorf("empty transparent_from must not be charged multi-sign fee: got %d", charged)
	}

	ctx.Tx = memoTx
	charged, err = ConsumeMemoFee(ctx)
	if err != nil {
		t.Fatalf("memo fee: unexpected error: %v", err)
	}
	if charged != 0 {
		t.Errorf("empty transparent_from must not be charged memo fee: got %d", charged)
	}
}

func TestConsumeMemoFee_skipped_empty_memo(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 10_000_000)

	tx := makeTransferTxWithMemo(1, 2, 1_000, nil) // no memo

	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetMemoFee(500_000)

	if _, err := ConsumeMemoFee(ctx); err != nil {
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

	if _, err := ConsumeMemoFee(ctx); err != nil {
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
	if _, err := ConsumeMultiSignFee(ctx); err != nil {
		t.Fatalf("multi-sign fee: %v", err)
	}

	// Charge memo fee on tx2 — reuse owner, update tx in ctx
	ctx.Tx = txMemo
	if _, err := ConsumeMemoFee(ctx); err != nil {
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

	if _, err := ConsumeMultiSignFee(ctx); err != nil {
		t.Fatalf("multi-sign fee: %v", err)
	}

	if got := ctx.DynProps.BurnTrxAmount(); got != 0 {
		t.Errorf("BurnTrxAmount should be 0 before blackhole optimization: got %d", got)
	}
}

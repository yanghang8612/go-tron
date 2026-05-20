package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

const zenTokenID = int64(1_000_016)

func shieldedReceive(commitment []byte) *contractpb.ReceiveDescription {
	cm := make([]byte, zcElementSize)
	copy(cm, commitment)
	return &contractpb.ReceiveDescription{
		ValueCommitment: make([]byte, zcElementSize),
		NoteCommitment:  cm,
		Epk:             make([]byte, zcElementSize),
		CEnc:            make([]byte, zcEncCiphertextSize),
		COut:            make([]byte, zcOutCiphertextSize),
		Zkproof:         make([]byte, zcProofSize),
	}
}

func seedShieldedAnchor(t *testing.T, ctx *Context, anchor []byte) {
	t.Helper()
	if err := rawdb.WriteIncrMerkleTree(ctx.DB, anchor, &contractpb.IncrementalMerkleTree{}); err != nil {
		t.Fatalf("WriteIncrMerkleTree: %v", err)
	}
}

func setupShieldedCtx(t *testing.T, c *contractpb.ShieldedTransferContract) *Context {
	t.Helper()
	ctx := newTestContext(t, corepb.Transaction_Contract_ShieldedTransferContract, c, 0)
	ctx.DB = ethrawdb.NewMemoryDatabase()
	ctx.DynProps.SetAllowSameTokenName(true)
	ctx.DynProps.SetAllowShieldedTransaction(true)
	return ctx
}

// TestShieldedTransferDisabled verifies that validate fails when the feature is not enabled.
func TestShieldedTransferDisabled(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ShieldedTransferContract{
		TransparentFromAddress: owner[:],
		FromAmount:             1_000_000,
		SpendDescription:       []*contractpb.SpendDescription{{Nullifier: []byte("nullifier1")}},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ShieldedTransferContract, c, 0)
	ctx.DB = ethrawdb.NewMemoryDatabase()
	// AllowShieldedTransaction defaults to false

	act := &ShieldedTransferActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error when shielded transactions are disabled")
	}
}

// TestShieldedTransferValidateTransparentFrom validates a transparent-in transaction.
func TestShieldedTransferValidateTransparentFrom(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ShieldedTransferContract{
		TransparentFromAddress: owner[:],
		FromAmount:             500_000,
		ReceiveDescription:     []*contractpb.ReceiveDescription{shieldedReceive([]byte("cm1"))},
		BindingSignature:       make([]byte, zcSignatureSize),
	}
	ctx := setupShieldedCtx(t, c)

	act := &ShieldedTransferActuator{}

	// No account yet
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing sender account")
	}

	// Create account with insufficient balance
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetTRC10Balance(owner, zenTokenID, 100_000) // only 100k, needs 500k + fee
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for insufficient ZEN balance")
	}

	// Fund account properly: fromAmount(500k) + fee(100k) = 600k
	ctx.State.SetTRC10Balance(owner, zenTokenID, 600_000)
	if err := rawdb.WriteZKProofResult(ctx.DB, ctx.Tx.Hash().Bytes(), true); err != nil {
		t.Fatal(err)
	}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

// TestShieldedTransferDoubleSpend checks that reusing a nullifier is rejected.
func TestShieldedTransferDoubleSpend(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	nullifier := []byte("testnullifier32bytes____________")
	anchor := []byte("anchor_for_double_spend________")
	c := &contractpb.ShieldedTransferContract{
		SpendDescription:   []*contractpb.SpendDescription{{Nullifier: nullifier, Anchor: anchor}},
		ReceiveDescription: []*contractpb.ReceiveDescription{shieldedReceive([]byte("cm1"))},
	}
	ctx := setupShieldedCtx(t, c)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetTRC10Balance(owner, zenTokenID, 1_000_000)
	seedShieldedAnchor(t, ctx, anchor)

	// Pre-record the nullifier to simulate double spend
	if err := rawdb.WriteNullifier(ctx.DB, nullifier); err != nil {
		t.Fatal(err)
	}

	act := &ShieldedTransferActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for double spend")
	}
}

func TestShieldedTransferValidateRejectsUnknownAnchor(t *testing.T) {
	nullifier := []byte("testnullifier32bytes_anchor____")
	c := &contractpb.ShieldedTransferContract{
		SpendDescription:   []*contractpb.SpendDescription{{Nullifier: nullifier, Anchor: []byte("missing_anchor")}},
		ReceiveDescription: []*contractpb.ReceiveDescription{shieldedReceive([]byte("cm1"))},
	}
	ctx := setupShieldedCtx(t, c)

	err := (&ShieldedTransferActuator{}).Validate(ctx)
	if err == nil {
		t.Fatal("expected invalid anchor")
	}
	if err.Error() != "Rt is invalid." {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestShieldedTransferValidateRejectsCiphertextSize(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ShieldedTransferContract{
		TransparentFromAddress: owner[:],
		FromAmount:             500_000,
		ReceiveDescription:     []*contractpb.ReceiveDescription{{NoteCommitment: []byte("cm1"), CEnc: make([]byte, zcEncCiphertextSize-1), COut: make([]byte, zcOutCiphertextSize)}},
	}
	ctx := setupShieldedCtx(t, c)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetTRC10Balance(owner, zenTokenID, 1_000_000)

	err := (&ShieldedTransferActuator{}).Validate(ctx)
	if err == nil {
		t.Fatal("expected ciphertext size rejection")
	}
	if err.Error() != "Cout or CEnc size error" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestShieldedTransferValidateRejectsOutputProofShape(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ShieldedTransferContract{
		TransparentFromAddress: owner[:],
		FromAmount:             500_000,
		ReceiveDescription: []*contractpb.ReceiveDescription{{
			ValueCommitment: make([]byte, zcElementSize),
			NoteCommitment:  make([]byte, zcElementSize),
			Epk:             nil,
			CEnc:            make([]byte, zcEncCiphertextSize),
			COut:            make([]byte, zcOutCiphertextSize),
			Zkproof:         make([]byte, zcProofSize),
		}},
		BindingSignature: make([]byte, zcSignatureSize),
	}
	ctx := setupShieldedCtx(t, c)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetTRC10Balance(owner, zenTokenID, 1_000_000)

	err := (&ShieldedTransferActuator{}).Validate(ctx)
	if err == nil {
		t.Fatal("expected output proof shape rejection")
	}
	if err.Error() != "librustzcashSaplingCheckOutput error" {
		t.Fatalf("unexpected error: %v", err)
	}
	txID := ctx.Tx.Hash()
	if cached, ok := rawdb.ReadZKProofResult(ctx.DB, txID.Bytes()); !ok || cached {
		t.Fatalf("failed proof cache: got (%v,%v), want (false,true)", cached, ok)
	}

	err = (&ShieldedTransferActuator{}).Validate(ctx)
	if err == nil {
		t.Fatal("expected cached proof failure")
	}
	if err.Error() != "record is fail, skip proof" {
		t.Fatalf("unexpected cached error: %v", err)
	}
}

func TestShieldedTransferValidateRejectsUnverifiedProof(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ShieldedTransferContract{
		TransparentFromAddress: owner[:],
		FromAmount:             500_000,
		ReceiveDescription:     []*contractpb.ReceiveDescription{shieldedReceive([]byte("cm1"))},
		BindingSignature:       make([]byte, zcSignatureSize),
	}
	ctx := setupShieldedCtx(t, c)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetTRC10Balance(owner, zenTokenID, 1_000_000)

	if err := (&ShieldedTransferActuator{}).Validate(ctx); err == nil {
		t.Fatal("expected proof verification rejection")
	}
	txID := ctx.Tx.Hash()
	if cached, ok := rawdb.ReadZKProofResult(ctx.DB, txID.Bytes()); !ok || cached {
		t.Fatalf("failed proof cache: got (%v,%v), want (false,true)", cached, ok)
	}
}

func TestShieldedTransferValidateCachedProofSuccess(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ShieldedTransferContract{
		TransparentFromAddress: owner[:],
		FromAmount:             500_000,
		ReceiveDescription:     []*contractpb.ReceiveDescription{shieldedReceive([]byte("cm1"))},
		BindingSignature:       make([]byte, zcSignatureSize),
	}
	ctx := setupShieldedCtx(t, c)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetTRC10Balance(owner, zenTokenID, 1_000_000)

	if err := rawdb.WriteZKProofResult(ctx.DB, ctx.Tx.Hash().Bytes(), true); err != nil {
		t.Fatal(err)
	}
	if err := (&ShieldedTransferActuator{}).Validate(ctx); err != nil {
		t.Fatalf("cached validate should pass: %v", err)
	}
}

// TestShieldedTransferExecuteTransparentIn tests shielding ZEN into the pool.
func TestShieldedTransferExecuteTransparentIn(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	nullifier := []byte("nullifier_for_spend_desc_______1")
	commitment := []byte("notecommitment_for_receive______")
	c := &contractpb.ShieldedTransferContract{
		TransparentFromAddress: owner[:],
		FromAmount:             500_000,
		SpendDescription:       []*contractpb.SpendDescription{{Nullifier: nullifier}},
		ReceiveDescription:     []*contractpb.ReceiveDescription{shieldedReceive(commitment)},
	}
	ctx := setupShieldedCtx(t, c)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	// Fund: 500_000 (from) + 100_000 (fee) = 600_000
	ctx.State.SetTRC10Balance(owner, zenTokenID, 600_000)

	act := &ShieldedTransferActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}
	if result.Fee != 0 || result.ShieldedTransactionFee != 100_000 {
		t.Fatalf("receipt fees: want Fee=0 shielded=100000, got Fee=%d shielded=%d", result.Fee, result.ShieldedTransactionFee)
	}

	// Sender pays fromAmount; the fee is credited to Blackhole and removed from the pool.
	if got := ctx.State.GetTRC10Balance(owner, zenTokenID); got != 100_000 {
		t.Fatalf("sender ZEN balance: want 100000, got %d", got)
	}
	if got := ctx.State.GetTRC10Balance(params.BlackholeAddress, zenTokenID); got != 100_000 {
		t.Fatalf("blackhole ZEN balance: want 100000, got %d", got)
	}
	// Nullifier should be recorded
	if !rawdb.HasNullifier(ctx.DB, nullifier) {
		t.Fatal("nullifier should be recorded after execute")
	}
	// Note commitment should be recorded
	if got := rawdb.NoteCommitmentCount(ctx.DB); got != 1 {
		t.Fatalf("note commitment count: want 1, got %d", got)
	}
	// Pool value: fromAmount - toAmount(0) - fee = 500k - 0 - 100k = 400k
	if got := ctx.DynProps.TotalShieldedPoolValue(); got != 400_000 {
		t.Fatalf("shielded pool value: want 400000, got %d", got)
	}
}

// TestShieldedTransferExecuteTransparentOut tests unshielding ZEN from the pool.
func TestShieldedTransferExecuteTransparentOut(t *testing.T) {
	to := tcommon.Address{0x41, 0x02}
	nullifier := []byte("nullifier_for_spend_desc_______2")
	c := &contractpb.ShieldedTransferContract{
		SpendDescription:     []*contractpb.SpendDescription{{Nullifier: nullifier}},
		TransparentToAddress: to[:],
		ToAmount:             300_000,
	}
	ctx := setupShieldedCtx(t, c)
	// Pre-create recipient so regular fee (100k) applies, not create-account fee.
	ctx.State.CreateAccount(to, corepb.AccountType_Normal)
	// Pre-seed pool value so the deduction makes sense
	ctx.DynProps.AdjustTotalShieldedPoolValue(1_000_000)

	act := &ShieldedTransferActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}

	// Recipient should be created with toAmount
	if !ctx.State.AccountExists(to) {
		t.Fatal("recipient account should have been created")
	}
	if got := ctx.State.GetTRC10Balance(to, zenTokenID); got != 300_000 {
		t.Fatalf("recipient ZEN balance: want 300000, got %d", got)
	}
	// Nullifier recorded
	if !rawdb.HasNullifier(ctx.DB, nullifier) {
		t.Fatal("nullifier should be recorded")
	}
	// Pool: 1_000_000 + 0 - 300_000 - 100_000 (fee) = 600_000
	if got := ctx.DynProps.TotalShieldedPoolValue(); got != 600_000 {
		t.Fatalf("pool value: want 600000, got %d", got)
	}
}

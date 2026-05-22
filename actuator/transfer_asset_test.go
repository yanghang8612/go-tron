package actuator

import (
	"strconv"
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeTransferAssetTx(ownerByte, toByte byte, tokenID int64, amount int64) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	to := makeTestAddr(toByte)
	c := &contractpb.TransferAssetContract{
		OwnerAddress: owner.Bytes(),
		ToAddress:    to.Bytes(),
		AssetName:    []byte(strconv.FormatInt(tokenID, 10)),
		Amount:       amount,
	}
	anyParam, _ := anypb.New(c)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_TransferAssetContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

// TestTransferAssetExecute_AssetNameAsLiteralName locks down the pre-fork
// path (asset_name carrying the literal token NAME, not its numeric id).
// Mainnet sync stalled at block 5584 — a TransferAssetContract whose
// asset_name was "Bitcoin" — until resolveAssetNameOrID was added; before
// that, ParseInt("Bitcoin") failed silently → tokenID=0 → "insufficient
// balance" → sync wedged.
func TestTransferAssetExecute_AssetNameAsLiteralName(t *testing.T) {
	const tokenID = int64(1_000_004)
	const tokenName = "Bitcoin"
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	to := makeTestAddr(2)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(to, corepb.AccountType_Normal)
	statedb.SetTRC10BalanceLegacyAndV2(owner, []byte(tokenName), tokenID, 21_000_000)

	asset := &contractpb.AssetIssueContract{Name: []byte(tokenName), Id: strconv.FormatInt(tokenID, 10)}
	if err := statedb.WriteAssetIssueByName([]byte(tokenName), asset); err != nil {
		t.Fatal(err)
	}
	if err := statedb.WriteAssetIssue(tokenID, asset); err != nil {
		t.Fatal(err)
	}
	if err := statedb.WriteAssetNameIndex([]byte(tokenName), tokenID); err != nil {
		t.Fatal(err)
	}

	c := &contractpb.TransferAssetContract{
		OwnerAddress: owner.Bytes(),
		ToAddress:    to.Bytes(),
		AssetName:    []byte(tokenName), // literal name, not numeric id
		Amount:       100,
	}
	anyParam, _ := anypb.New(c)
	tx := types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: corepb.Transaction_Contract_TransferAssetContract, Parameter: anyParam},
			},
		},
	})
	ctx := setupContext(t, statedb, tx)

	act := &TransferAssetActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate must accept literal name: %v", err)
	}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute must succeed with literal name: %v", err)
	}
	if got := statedb.GetTRC10Balance(owner, tokenID); got != 21_000_000-100 {
		t.Errorf("owner balance after transfer: got %d want %d", got, 21_000_000-100)
	}
	if got := statedb.GetTRC10Balance(to, tokenID); got != 100 {
		t.Errorf("recipient balance: got %d want 100", got)
	}
}

func TestTransferAssetExecute_PreSameTokenNameNumericNameUsesNameIndex(t *testing.T) {
	const tokenID = int64(1_000_004)
	const numericName = "123"
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	to := makeTestAddr(2)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(to, corepb.AccountType_Normal)
	statedb.SetTRC10BalanceLegacyAndV2(owner, []byte(numericName), tokenID, 21_000_000)

	asset := &contractpb.AssetIssueContract{Name: []byte(numericName), Id: strconv.FormatInt(tokenID, 10)}
	if err := statedb.WriteAssetIssueByName([]byte(numericName), asset); err != nil {
		t.Fatal(err)
	}
	if err := statedb.WriteAssetIssue(tokenID, asset); err != nil {
		t.Fatal(err)
	}
	if err := statedb.WriteAssetIssue(123, &contractpb.AssetIssueContract{Name: []byte("OTHER")}); err != nil {
		t.Fatal(err)
	}
	if err := statedb.WriteAssetNameIndex([]byte(numericName), tokenID); err != nil {
		t.Fatal(err)
	}

	c := &contractpb.TransferAssetContract{
		OwnerAddress: owner.Bytes(),
		ToAddress:    to.Bytes(),
		AssetName:    []byte(numericName),
		Amount:       100,
	}
	anyParam, _ := anypb.New(c)
	tx := types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: corepb.Transaction_Contract_TransferAssetContract, Parameter: anyParam},
			},
		},
	})
	ctx := setupContext(t, statedb, tx)

	act := &TransferAssetActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate must resolve numeric-looking pre-fork name through name index: %v", err)
	}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute must use name-index token ID: %v", err)
	}
	if got := statedb.GetTRC10Balance(owner, tokenID); got != 21_000_000-100 {
		t.Errorf("owner balance after transfer: got %d want %d", got, 21_000_000-100)
	}
	if got := statedb.GetTRC10Balance(to, tokenID); got != 100 {
		t.Errorf("recipient balance for name-index token: got %d want 100", got)
	}
	if got := statedb.GetTRC10Balance(to, 123); got != 0 {
		t.Errorf("recipient balance for parsed token ID must stay zero, got %d", got)
	}
}

func TestTransferAssetValidate_Success(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	to := makeTestAddr(2)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(to, corepb.AccountType_Normal)
	statedb.SetTRC10Balance(owner, tokenID, 500_000)

	if err := statedb.WriteAssetIssue(tokenID, &contractpb.AssetIssueContract{Name: []byte("T")}); err != nil {
		t.Fatal(err)
	}

	tx := makeTransferAssetTx(1, 2, tokenID, 100_000)
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowSameTokenName(true)

	act := &TransferAssetActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

func TestTransferAssetValidate_UnknownToken(t *testing.T) {
	statedb := setupStateDB(t)
	statedb.CreateAccount(makeTestAddr(1), corepb.AccountType_Normal)

	tx := makeTransferAssetTx(1, 2, 9_999_999, 100)
	ctx := setupContext(t, statedb, tx)
	// empty rooted state — no token
	ctx.DynProps.SetAllowSameTokenName(true)

	act := &TransferAssetActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for unknown token")
	}
}

func TestTransferAssetValidate_InsufficientBalance(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.SetTRC10Balance(owner, tokenID, 50) // only 50 tokens

	if err := statedb.WriteAssetIssue(tokenID, &contractpb.AssetIssueContract{Name: []byte("T")}); err != nil {
		t.Fatal(err)
	}

	tx := makeTransferAssetTx(1, 2, tokenID, 100) // wants 100
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowSameTokenName(true)

	act := &TransferAssetActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for insufficient balance")
	}
}

func TestTransferAssetExecute_Success(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	to := makeTestAddr(2)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(to, corepb.AccountType_Normal)
	statedb.SetTRC10Balance(owner, tokenID, 1_000_000)

	if err := statedb.WriteAssetIssue(tokenID, &contractpb.AssetIssueContract{Name: []byte("T")}); err != nil {
		t.Fatal(err)
	}

	tx := makeTransferAssetTx(1, 2, tokenID, 300_000)
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowSameTokenName(true)

	act := &TransferAssetActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}
	if got := statedb.GetTRC10Balance(owner, tokenID); got != 700_000 {
		t.Fatalf("sender: want 700000, got %d", got)
	}
	if got := statedb.GetTRC10Balance(to, tokenID); got != 300_000 {
		t.Fatalf("recipient: want 300000, got %d", got)
	}
}

func TestTransferAssetExecute_CreateAccountFeeBurnedNotCounted(t *testing.T) {
	// Same rationale as the TransferActuator test: actuator-level fee from
	// proposal #12 is burned but does not update total_create_account_cost.
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 1_000_000)
	statedb.SetTRC10Balance(owner, tokenID, 1_000_000)

	if err := statedb.WriteAssetIssue(tokenID, &contractpb.AssetIssueContract{Name: []byte("T")}); err != nil {
		t.Fatal(err)
	}

	tx := makeTransferAssetTx(1, 0xBB, tokenID, 500_000)
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowSameTokenName(true)
	ctx.DynProps.Set("create_new_account_fee_in_system_contract", int64(100_000))
	balanceBefore := statedb.GetBalance(owner)

	act := &TransferAssetActuator{}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got := ctx.DynProps.TotalCreateAccountCost(); got != 0 {
		t.Fatalf("TotalCreateAccountCost should remain 0 (bandwidth-path counter): got %d", got)
	}
	if got := statedb.GetBalance(owner); got != balanceBefore-100_000 {
		t.Fatalf("owner TRX balance after burn: want %d, got %d", balanceBefore-100_000, got)
	}
}

func TestTransferAssetExecute_CreatesRecipient(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	to := makeTestAddr(9)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 1_000_000) // TRX for create_account_fee
	statedb.SetTRC10Balance(owner, tokenID, 1_000_000)

	if err := statedb.WriteAssetIssue(tokenID, &contractpb.AssetIssueContract{Name: []byte("T")}); err != nil {
		t.Fatal(err)
	}

	tx := makeTransferAssetTx(1, 9, tokenID, 500_000)
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowSameTokenName(true)

	act := &TransferAssetActuator{}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if !statedb.AccountExists(to) {
		t.Fatal("recipient account should have been created")
	}
	if got := statedb.GetTRC10Balance(to, tokenID); got != 500_000 {
		t.Fatalf("recipient TRC10 balance: want 500000, got %d", got)
	}
}

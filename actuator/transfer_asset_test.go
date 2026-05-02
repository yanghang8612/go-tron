package actuator

import (
	"strconv"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
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

func TestTransferAssetValidate_Success(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	to := makeTestAddr(2)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(to, corepb.AccountType_Normal)
	statedb.SetTRC10Balance(owner, tokenID, 500_000)

	db := ethrawdb.NewMemoryDatabase()
	if err := rawdb.WriteAssetIssue(db, tokenID, &contractpb.AssetIssueContract{Name: []byte("T")}); err != nil {
		t.Fatal(err)
	}

	tx := makeTransferAssetTx(1, 2, tokenID, 100_000)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

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
	ctx.DB = ethrawdb.NewMemoryDatabase() // empty DB — no token

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

	db := ethrawdb.NewMemoryDatabase()
	if err := rawdb.WriteAssetIssue(db, tokenID, &contractpb.AssetIssueContract{Name: []byte("T")}); err != nil {
		t.Fatal(err)
	}

	tx := makeTransferAssetTx(1, 2, tokenID, 100) // wants 100
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

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

	db := ethrawdb.NewMemoryDatabase()
	if err := rawdb.WriteAssetIssue(db, tokenID, &contractpb.AssetIssueContract{Name: []byte("T")}); err != nil {
		t.Fatal(err)
	}

	tx := makeTransferAssetTx(1, 2, tokenID, 300_000)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

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

	db := ethrawdb.NewMemoryDatabase()
	if err := rawdb.WriteAssetIssue(db, tokenID, &contractpb.AssetIssueContract{Name: []byte("T")}); err != nil {
		t.Fatal(err)
	}

	tx := makeTransferAssetTx(1, 0xBB, tokenID, 500_000)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db
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

	db := ethrawdb.NewMemoryDatabase()
	if err := rawdb.WriteAssetIssue(db, tokenID, &contractpb.AssetIssueContract{Name: []byte("T")}); err != nil {
		t.Fatal(err)
	}

	tx := makeTransferAssetTx(1, 9, tokenID, 500_000)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

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

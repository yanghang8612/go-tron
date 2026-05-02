package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeTransferTx(from, to byte, amount int64) *types.Transaction {
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
		},
	}
	return types.NewTransactionFromPB(pb)
}

func TestTransferValidate_Success(t *testing.T) {
	statedb := setupStateDB(t)
	from := makeTestAddr(1)
	to := makeTestAddr(2)
	seedAccount(statedb, from, 10_000_000)
	seedAccount(statedb, to, 0)

	tx := makeTransferTx(1, 2, 5_000_000)
	ctx := setupContext(t, statedb, tx)
	act := &TransferActuator{}

	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

func TestTransferValidate_InsufficientBalance(t *testing.T) {
	statedb := setupStateDB(t)
	seedAccount(statedb, makeTestAddr(1), 100)
	seedAccount(statedb, makeTestAddr(2), 0)

	tx := makeTransferTx(1, 2, 5_000_000)
	ctx := setupContext(t, statedb, tx)
	act := &TransferActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("validate should fail for insufficient balance")
	}
}

func TestTransferValidate_NegativeAmount(t *testing.T) {
	statedb := setupStateDB(t)
	seedAccount(statedb, makeTestAddr(1), 10_000_000)

	tx := makeTransferTx(1, 2, -1)
	ctx := setupContext(t, statedb, tx)
	act := &TransferActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("validate should reject negative amount")
	}
}

func TestTransferValidate_SelfTransfer(t *testing.T) {
	statedb := setupStateDB(t)
	seedAccount(statedb, makeTestAddr(1), 10_000_000)

	tx := makeTransferTx(1, 1, 100)
	ctx := setupContext(t, statedb, tx)
	act := &TransferActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("validate should reject self-transfer")
	}
}

func TestTransferExecute_Success(t *testing.T) {
	statedb := setupStateDB(t)
	from := makeTestAddr(1)
	to := makeTestAddr(2)
	seedAccount(statedb, from, 10_000_000)
	seedAccount(statedb, to, 5_000_000)

	tx := makeTransferTx(1, 2, 3_000_000)
	ctx := setupContext(t, statedb, tx)
	act := &TransferActuator{}

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result == nil {
		t.Fatal("result should not be nil")
	}

	if statedb.GetBalance(from) != 7_000_000 {
		t.Fatalf("from balance: expected 7000000, got %d", statedb.GetBalance(from))
	}
	if statedb.GetBalance(to) != 8_000_000 {
		t.Fatalf("to balance: expected 8000000, got %d", statedb.GetBalance(to))
	}
}

func TestTransferExecute_CreatesRecipient(t *testing.T) {
	statedb := setupStateDB(t)
	from := makeTestAddr(1)
	to := makeTestAddr(3)
	seedAccount(statedb, from, 10_000_000)

	tx := makeTransferTx(1, 3, 1_000_000)
	ctx := setupContext(t, statedb, tx)
	act := &TransferActuator{}

	_, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if !statedb.AccountExists(to) {
		t.Fatal("recipient account should have been created")
	}
	if statedb.GetBalance(to) != 1_000_000 {
		t.Fatalf("to balance: expected 1000000, got %d", statedb.GetBalance(to))
	}
}

func TestTransferExecute_CreateAccountFeeBurnedNotCounted(t *testing.T) {
	// java-tron's TransferActuator deducts CreateNewAccountFeeInSystemContract
	// (proposal #12 default 0; here forced to 100k) from owner and routes it
	// to blackhole, but does NOT update total_create_account_cost — that
	// counter is bumped only by BandwidthProcessor's create_account_fee path
	// (mirrored in core.consumeBandwidthForCreateNewAccount).
	statedb := setupStateDB(t)
	from := makeTestAddr(1)
	seedAccount(statedb, from, 10_000_000)
	balanceBefore := statedb.GetBalance(from)

	tx := makeTransferTx(1, 0xAA, 1_000_000)
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.Set("create_new_account_fee_in_system_contract", int64(100_000))
	act := &TransferActuator{}

	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got := ctx.DynProps.TotalCreateAccountCost(); got != 0 {
		t.Fatalf("TotalCreateAccountCost should remain 0 (counter belongs to bandwidth path): got %d", got)
	}
	wantBalance := balanceBefore - 1_000_000 - 100_000
	if got := statedb.GetBalance(from); got != wantBalance {
		t.Fatalf("owner balance after fee burn: want %d, got %d", wantBalance, got)
	}
}

func TestTransferExecute_NoCounterWhenRecipientExists(t *testing.T) {
	statedb := setupStateDB(t)
	from := makeTestAddr(1)
	to := makeTestAddr(2)
	seedAccount(statedb, from, 10_000_000)
	seedAccount(statedb, to, 0)

	tx := makeTransferTx(1, 2, 1_000_000)
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.Set("create_new_account_fee_in_system_contract", int64(100_000))
	act := &TransferActuator{}

	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got := ctx.DynProps.TotalCreateAccountCost(); got != 0 {
		t.Fatalf("TotalCreateAccountCost should remain 0: got %d", got)
	}
}

// Suppress unused import warning for tcommon.
var _ tcommon.Address

package actuator

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeTransferTx(from, to common.Address, amount int64) *types.Transaction {
	transfer := &contractpb.TransferContract{
		OwnerAddress: from.Bytes(),
		ToAddress:    to.Bytes(),
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

func setupDB(accounts map[common.Address]int64) ethdb.KeyValueStore {
	db := rawdb.NewMemoryDatabase()
	for addr, balance := range accounts {
		acc := types.NewAccount(addr, corepb.AccountType_Normal)
		acc.SetBalance(balance)
		rawdb.WriteAccount(db, addr, acc)
	}
	return db
}

func TestTransferValidate_Success(t *testing.T) {
	from := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	to := common.BytesToAddress([]byte{0x41, 2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{from: 10_000_000, to: 0})

	tx := makeTransferTx(from, to, 5_000_000)
	ctx := &Context{DB: db, Tx: tx}
	act := &TransferActuator{}

	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

func TestTransferValidate_InsufficientBalance(t *testing.T) {
	from := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	to := common.BytesToAddress([]byte{0x41, 2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{from: 100, to: 0})

	tx := makeTransferTx(from, to, 5_000_000)
	ctx := &Context{DB: db, Tx: tx}
	act := &TransferActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("validate should fail for insufficient balance")
	}
}

func TestTransferValidate_NegativeAmount(t *testing.T) {
	from := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	to := common.BytesToAddress([]byte{0x41, 2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{from: 10_000_000})

	tx := makeTransferTx(from, to, -1)
	ctx := &Context{DB: db, Tx: tx}
	act := &TransferActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("validate should reject negative amount")
	}
}

func TestTransferValidate_SelfTransfer(t *testing.T) {
	from := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{from: 10_000_000})

	tx := makeTransferTx(from, from, 100)
	ctx := &Context{DB: db, Tx: tx}
	act := &TransferActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("validate should reject self-transfer")
	}
}

func TestTransferExecute_Success(t *testing.T) {
	from := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	to := common.BytesToAddress([]byte{0x41, 2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{from: 10_000_000, to: 5_000_000})

	tx := makeTransferTx(from, to, 3_000_000)
	ctx := &Context{DB: db, Tx: tx}
	act := &TransferActuator{}

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result == nil {
		t.Fatal("result should not be nil")
	}

	fromAcc := rawdb.ReadAccount(db, from)
	toAcc := rawdb.ReadAccount(db, to)
	if fromAcc.Balance() != 7_000_000 {
		t.Fatalf("from balance: expected 7000000, got %d", fromAcc.Balance())
	}
	if toAcc.Balance() != 8_000_000 {
		t.Fatalf("to balance: expected 8000000, got %d", toAcc.Balance())
	}
}

func TestTransferExecute_CreatesRecipient(t *testing.T) {
	from := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	to := common.BytesToAddress([]byte{0x41, 3, 3, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{from: 10_000_000})

	tx := makeTransferTx(from, to, 1_000_000)
	ctx := &Context{DB: db, Tx: tx}
	act := &TransferActuator{}

	_, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	toAcc := rawdb.ReadAccount(db, to)
	if toAcc == nil {
		t.Fatal("recipient account should have been created")
	}
	if toAcc.Balance() != 1_000_000 {
		t.Fatalf("to balance: expected 1000000, got %d", toAcc.Balance())
	}
}

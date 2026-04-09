package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeCreateAccountTx(owner, newAddr common.Address) *types.Transaction {
	contract := &contractpb.AccountCreateContract{
		OwnerAddress:   owner.Bytes(),
		AccountAddress: newAddr.Bytes(),
		Type:           corepb.AccountType_Normal,
	}
	anyParam, _ := anypb.New(contract)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_AccountCreateContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

func TestCreateAccountValidate_Success(t *testing.T) {
	owner := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	newAddr := common.BytesToAddress([]byte{0x41, 5, 5, 5, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{owner: 10_000_000})

	tx := makeCreateAccountTx(owner, newAddr)
	ctx := &Context{DB: db, Tx: tx}
	act := &CreateAccountActuator{}

	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

func TestCreateAccountValidate_AlreadyExists(t *testing.T) {
	owner := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	existing := common.BytesToAddress([]byte{0x41, 5, 5, 5, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{owner: 10_000_000, existing: 0})

	tx := makeCreateAccountTx(owner, existing)
	ctx := &Context{DB: db, Tx: tx}
	act := &CreateAccountActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("validate should reject already existing account")
	}
}

func TestCreateAccountExecute(t *testing.T) {
	owner := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	newAddr := common.BytesToAddress([]byte{0x41, 7, 7, 7, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{owner: 10_000_000})

	tx := makeCreateAccountTx(owner, newAddr)
	ctx := &Context{DB: db, Tx: tx}
	act := &CreateAccountActuator{}

	_, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	newAcc := rawdb.ReadAccount(db, newAddr)
	if newAcc == nil {
		t.Fatal("new account should exist")
	}
}

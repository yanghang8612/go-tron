package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeCreateAccountTx(ownerByte, newByte byte) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	newAddr := makeTestAddr(newByte)
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
	statedb := setupStateDB(t)
	seedAccount(statedb, makeTestAddr(1), 10_000_000)

	tx := makeCreateAccountTx(1, 5)
	ctx := setupContext(t, statedb, tx)
	act := &CreateAccountActuator{}

	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

func TestCreateAccountValidate_AlreadyExists(t *testing.T) {
	statedb := setupStateDB(t)
	seedAccount(statedb, makeTestAddr(1), 10_000_000)
	seedAccount(statedb, makeTestAddr(5), 0)

	tx := makeCreateAccountTx(1, 5)
	ctx := setupContext(t, statedb, tx)
	act := &CreateAccountActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("validate should reject already existing account")
	}
}

func TestCreateAccountExecute(t *testing.T) {
	statedb := setupStateDB(t)
	seedAccount(statedb, makeTestAddr(1), 10_000_000)

	newAddr := makeTestAddr(7)
	tx := makeCreateAccountTx(1, 7)
	ctx := setupContext(t, statedb, tx)
	act := &CreateAccountActuator{}

	_, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if !statedb.AccountExists(newAddr) {
		t.Fatal("new account should exist")
	}
}

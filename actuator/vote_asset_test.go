package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeVoteAssetTx(ownerByte byte) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	c := &contractpb.VoteAssetContract{
		OwnerAddress: owner.Bytes(),
		Count:        1,
	}
	anyParam, _ := anypb.New(c)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_VoteAssetContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

func TestVoteAssetValidate_Success(t *testing.T) {
	owner := makeTestAddr(1)
	tx := makeVoteAssetTx(1)
	statedb := setupStateDB(t)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = ethrawdb.NewMemoryDatabase()

	act := &VoteAssetActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

func TestVoteAssetValidate_OwnerNotExist(t *testing.T) {
	tx := makeVoteAssetTx(1)
	statedb := setupStateDB(t)
	// No account created
	ctx := setupContext(t, statedb, tx)
	ctx.DB = ethrawdb.NewMemoryDatabase()

	act := &VoteAssetActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: owner does not exist")
	}
}

func TestVoteAssetExecute_NoStateChange(t *testing.T) {
	owner := makeTestAddr(1)
	tx := makeVoteAssetTx(1)
	statedb := setupStateDB(t)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 1_000_000)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = ethrawdb.NewMemoryDatabase()
	beforeBalance := statedb.GetBalance(owner)

	act := &VoteAssetActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}
	if statedb.GetBalance(owner) != beforeBalance {
		t.Fatal("VoteAsset should not change any balance")
	}
}

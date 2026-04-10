package core

import (
	"testing"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func newTestState(t *testing.T) *state.StateDB {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	statedb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), sdb)
	if err != nil {
		t.Fatal(err)
	}
	return statedb
}

func testProcessorAddr(b byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func makeTestTransferTx(from, to byte, amount int64) *types.Transaction {
	tc := &contractpb.TransferContract{
		OwnerAddress: testProcessorAddr(from).Bytes(),
		ToAddress:    testProcessorAddr(to).Bytes(),
		Amount:       amount,
	}
	param, _ := anypb.New(tc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TransferContract,
				Parameter: param,
			}},
		},
	})
}

func TestApplyTransaction_Transfer(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 1_000_000)

	tx := makeTestTransferTx(1, 2, 300_000)
	fee, err := ApplyTransaction(statedb, dynProps, tx, 3000, 1)
	if err != nil {
		t.Fatal(err)
	}
	if fee != 0 {
		t.Fatalf("fee: got %d, want 0", fee)
	}
	if got := statedb.GetBalance(testProcessorAddr(1)); got != 700_000 {
		t.Fatalf("sender: got %d, want 700000", got)
	}
	if got := statedb.GetBalance(testProcessorAddr(2)); got != 300_000 {
		t.Fatalf("recipient: got %d, want 300000", got)
	}
}

func TestApplyTransaction_ValidationFails(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	// No account seeded — validation should fail
	tx := makeTestTransferTx(1, 2, 100)
	_, err := ApplyTransaction(statedb, dynProps, tx, 3000, 1)
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestProcessBlock_WithTransactions(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 10_000_000)

	// Commit the initial state so we have a clean base
	_, err := statedb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	tx1 := makeTestTransferTx(1, 2, 1_000_000)
	tx2 := makeTestTransferTx(1, 3, 2_000_000)

	witnessAddr := testProcessorAddr(0xFF)
	// Witnesses always have an account in practice (created before becoming witness)
	statedb.CreateAccount(witnessAddr, corepb.AccountType_Normal)

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         1,
				Timestamp:      3000,
				WitnessAddress: witnessAddr.Bytes(),
			},
		},
		Transactions: []*corepb.Transaction{tx1.Proto(), tx2.Proto()},
	})

	root, err := ProcessBlock(statedb, dynProps, block)
	if err != nil {
		t.Fatal(err)
	}
	if root == (tcommon.Hash{}) {
		t.Fatal("expected non-empty state root")
	}

	// Verify: sender lost 3M, recipients got 1M and 2M
	if got := statedb.GetBalance(testProcessorAddr(1)); got != 7_000_000 {
		t.Fatalf("sender: got %d, want 7000000", got)
	}
	if got := statedb.GetBalance(testProcessorAddr(2)); got != 1_000_000 {
		t.Fatalf("recipient 2: got %d, want 1000000", got)
	}
	if got := statedb.GetBalance(testProcessorAddr(3)); got != 2_000_000 {
		t.Fatalf("recipient 3: got %d, want 2000000", got)
	}

	// Verify witness reward
	reward := dynProps.WitnessPayPerBlock()
	if got := statedb.GetAllowance(witnessAddr); got != reward {
		t.Fatalf("witness reward: got %d, want %d", got, reward)
	}
}

func TestProcessBlock_FailingTxRevertsState(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 100)

	// tx tries to transfer 200 — should fail validation
	tx := makeTestTransferTx(1, 2, 200)

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    1,
				Timestamp: 3000,
			},
		},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})

	_, err := ProcessBlock(statedb, dynProps, block)
	if err == nil {
		t.Fatal("expected error for invalid transaction")
	}

	// Balance should be unchanged
	if got := statedb.GetBalance(testProcessorAddr(1)); got != 100 {
		t.Fatalf("balance should be unchanged: got %d, want 100", got)
	}
}

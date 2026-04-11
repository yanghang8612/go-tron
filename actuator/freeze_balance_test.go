package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeFreezeBalanceTx(ownerByte byte, amount, duration int64, resource corepb.ResourceCode, receiverByte *byte) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	fc := &contractpb.FreezeBalanceContract{
		OwnerAddress:   owner.Bytes(),
		FrozenBalance:  amount,
		FrozenDuration: duration,
		Resource:       resource,
	}
	if receiverByte != nil {
		recv := makeTestAddr(*receiverByte)
		fc.ReceiverAddress = recv.Bytes()
	}
	any, _ := anypb.New(fc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: corepb.Transaction_Contract_FreezeBalanceContract, Parameter: any},
			},
		},
	})
}

func TestFreezeBalanceValidate_Success(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 10_000_000)

	tx := makeFreezeBalanceTx(1, 1_000_000, 3, corepb.ResourceCode_BANDWIDTH, nil)
	act := &FreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFreezeBalanceValidate_InsufficientBalance(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(2)
	seedAccount(statedb, owner, 500_000)

	tx := makeFreezeBalanceTx(2, 1_000_000, 3, corepb.ResourceCode_BANDWIDTH, nil)
	act := &FreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for insufficient balance")
	}
}

func TestFreezeBalanceValidate_DurationTooShort(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(3)
	seedAccount(statedb, owner, 10_000_000)

	tx := makeFreezeBalanceTx(3, 1_000_000, 2, corepb.ResourceCode_BANDWIDTH, nil)
	act := &FreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for duration too short")
	}
}

func TestFreezeBalanceExecute_Bandwidth(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(4)
	seedAccount(statedb, owner, 10_000_000)

	tx := makeFreezeBalanceTx(4, 1_000_000, 3, corepb.ResourceCode_BANDWIDTH, nil)
	act := &FreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}
	if got := statedb.GetBalance(owner); got != 9_000_000 {
		t.Fatalf("balance: want 9000000, got %d", got)
	}
	obj := statedb.GetStateObject(owner)
	if obj == nil {
		t.Fatal("state object not found")
	}
	if got := obj.TotalFrozenBandwidth(); got != 1_000_000 {
		t.Fatalf("frozen bandwidth: want 1000000, got %d", got)
	}
}

func TestFreezeBalanceExecute_Energy(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(5)
	seedAccount(statedb, owner, 10_000_000)

	tx := makeFreezeBalanceTx(5, 2_000_000, 3, corepb.ResourceCode_ENERGY, nil)
	act := &FreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}
	if got := statedb.GetBalance(owner); got != 8_000_000 {
		t.Fatalf("balance: want 8000000, got %d", got)
	}
	obj := statedb.GetStateObject(owner)
	if obj == nil {
		t.Fatal("state object not found")
	}
	if got := obj.FrozenEnergyAmount(); got != 2_000_000 {
		t.Fatalf("frozen energy: want 2000000, got %d", got)
	}
}

func TestFreezeBalanceExecute_Delegated(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(6)
	receiverByte := byte(7)
	receiver := makeTestAddr(receiverByte)
	seedAccount(statedb, owner, 10_000_000)
	seedAccount(statedb, receiver, 0)

	tx := makeFreezeBalanceTx(6, 3_000_000, 3, corepb.ResourceCode_BANDWIDTH, &receiverByte)
	act := &FreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}
	if got := statedb.GetBalance(owner); got != 7_000_000 {
		t.Fatalf("balance: want 7000000, got %d", got)
	}
	ownerObj := statedb.GetStateObject(owner)
	if ownerObj == nil {
		t.Fatal("owner state object not found")
	}
	if got := ownerObj.DelegatedFrozenBandwidth(); got != 3_000_000 {
		t.Fatalf("delegated frozen bandwidth: want 3000000, got %d", got)
	}
	recvObj := statedb.GetStateObject(receiver)
	if recvObj == nil {
		t.Fatal("receiver state object not found")
	}
	if got := recvObj.AcquiredDelegatedFrozenBandwidth(); got != 3_000_000 {
		t.Fatalf("acquired delegated frozen bandwidth: want 3000000, got %d", got)
	}
}

package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeUnfreezeBalanceTx(ownerByte byte, resource corepb.ResourceCode, receiverByte *byte) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	uc := &contractpb.UnfreezeBalanceContract{
		OwnerAddress: owner.Bytes(),
		Resource:     resource,
	}
	if receiverByte != nil {
		recv := makeTestAddr(*receiverByte)
		uc.ReceiverAddress = recv.Bytes()
	}
	any, _ := anypb.New(uc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: corepb.Transaction_Contract_UnfreezeBalanceContract, Parameter: any},
			},
		},
	})
}

// TestUnfreezeBalanceValidate_NotExpired tests that validation fails when the frozen bandwidth has
// not yet expired (expiry time > BlockTime 1_000_000).
func TestUnfreezeBalanceValidate_NotExpired(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 10_000_000)
	// Freeze with expiry 2_000_000 which is > BlockTime 1_000_000 (not expired)
	statedb.FreezeV1Bandwidth(owner, 1_000_000, 2_000_000)

	tx := makeUnfreezeBalanceTx(1, corepb.ResourceCode_BANDWIDTH, nil)
	act := &UnfreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-expired frozen bandwidth")
	}
}

// TestUnfreezeBalanceValidate_NoFrozen tests that validation fails when there is no frozen balance.
func TestUnfreezeBalanceValidate_NoFrozen(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(2)
	seedAccount(statedb, owner, 10_000_000)
	// No frozen balance set

	tx := makeUnfreezeBalanceTx(2, corepb.ResourceCode_BANDWIDTH, nil)
	act := &UnfreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for no frozen balance")
	}
}

// TestUnfreezeBalanceExecute_Bandwidth tests that unfreezing expired bandwidth restores balance.
func TestUnfreezeBalanceExecute_Bandwidth(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(3)
	seedAccount(statedb, owner, 9_000_000)
	// Freeze with expiry 500_000 which is < BlockTime 1_000_000 (already expired)
	statedb.FreezeV1Bandwidth(owner, 1_000_000, 500_000)

	tx := makeUnfreezeBalanceTx(3, corepb.ResourceCode_BANDWIDTH, nil)
	act := &UnfreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}
	// Balance should be restored: 9_000_000 + 1_000_000 = 10_000_000
	if got := statedb.GetBalance(owner); got != 10_000_000 {
		t.Fatalf("balance: want 10000000, got %d", got)
	}
	obj := statedb.GetStateObject(owner)
	if obj == nil {
		t.Fatal("state object not found")
	}
	// Frozen bandwidth should be cleared
	if got := obj.TotalFrozenBandwidth(); got != 0 {
		t.Fatalf("frozen bandwidth: want 0, got %d", got)
	}
}

// TestUnfreezeBalanceExecute_Energy tests that unfreezing expired energy restores balance.
func TestUnfreezeBalanceExecute_Energy(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(4)
	seedAccount(statedb, owner, 8_000_000)
	// Freeze energy with expiry 500_000 which is < BlockTime 1_000_000 (already expired)
	statedb.FreezeV1Energy(owner, 2_000_000, 500_000)

	tx := makeUnfreezeBalanceTx(4, corepb.ResourceCode_ENERGY, nil)
	act := &UnfreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}
	// Balance should be restored: 8_000_000 + 2_000_000 = 10_000_000
	if got := statedb.GetBalance(owner); got != 10_000_000 {
		t.Fatalf("balance: want 10000000, got %d", got)
	}
	obj := statedb.GetStateObject(owner)
	if obj == nil {
		t.Fatal("state object not found")
	}
	// Frozen energy should be cleared
	if got := obj.FrozenEnergyAmount(); got != 0 {
		t.Fatalf("frozen energy: want 0, got %d", got)
	}
}

// TestUnfreezeBalanceExecute_Delegated tests that unfreezing a delegation restores balance
// and clears the delegation.
func TestUnfreezeBalanceExecute_Delegated(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(5)
	receiverByte := byte(6)
	receiver := makeTestAddr(receiverByte)
	seedAccount(statedb, owner, 7_000_000)
	seedAccount(statedb, receiver, 0)
	// Set up delegated frozen bandwidth
	statedb.FreezeV1DelegatedBandwidth(owner, receiver, 3_000_000)

	tx := makeUnfreezeBalanceTx(5, corepb.ResourceCode_BANDWIDTH, &receiverByte)
	act := &UnfreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}
	// Balance should be restored: 7_000_000 + 3_000_000 = 10_000_000
	if got := statedb.GetBalance(owner); got != 10_000_000 {
		t.Fatalf("balance: want 10000000, got %d", got)
	}
	ownerObj := statedb.GetStateObject(owner)
	if ownerObj == nil {
		t.Fatal("owner state object not found")
	}
	// Delegation should be cleared
	if got := ownerObj.DelegatedFrozenBandwidth(); got != 0 {
		t.Fatalf("delegated frozen bandwidth: want 0, got %d", got)
	}
	recvObj := statedb.GetStateObject(receiver)
	if recvObj == nil {
		t.Fatal("receiver state object not found")
	}
	if got := recvObj.AcquiredDelegatedFrozenBandwidth(); got != 0 {
		t.Fatalf("acquired delegated frozen bandwidth: want 0, got %d", got)
	}
}

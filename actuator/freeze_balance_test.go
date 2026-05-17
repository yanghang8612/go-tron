package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
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

func TestFreezeBalanceValidate_DurationTooLong(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(30)
	seedAccount(statedb, owner, 10_000_000)

	tx := makeFreezeBalanceTx(30, 1_000_000, 4, corepb.ResourceCode_BANDWIDTH, nil)
	act := &FreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for duration above max_frozen_time")
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

func TestFreezeBalanceExecute_AllowNewRewardUsesWeightDelta(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(41)
	seedAccount(statedb, owner, 10_000_000)
	statedb.FreezeV1Bandwidth(owner, 1_900_000, 500_000)

	tx := makeFreezeBalanceTx(41, 1_900_000, 3, corepb.ResourceCode_BANDWIDTH, nil)
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowNewReward(true)

	if _, err := (&FreezeBalanceActuator{}).Execute(ctx); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if got := ctx.DynProps.TotalNetWeight(); got != 2 {
		t.Fatalf("total net weight delta: want 2, got %d", got)
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

func TestFreezeBalanceExecute_TronPower(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(8)
	seedAccount(statedb, owner, 10_000_000)
	statedb.FreezeV1Bandwidth(owner, 2_000_000, 500_000)

	tx := makeFreezeBalanceTx(8, 3_000_000, 3, corepb.ResourceCode_TRON_POWER, nil)
	act := &FreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowNewResourceModel(true)

	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute: %v", err)
	}
	acc := statedb.GetAccount(owner)
	if got := acc.OldTronPower(); got != 2_000_000 {
		t.Fatalf("old tron power snapshot: want 2000000, got %d", got)
	}
	if got := acc.V1TronPowerFrozen(); got != 3_000_000 {
		t.Fatalf("V1 tron power frozen: want 3000000, got %d", got)
	}
	if got := ctx.DynProps.TotalTronPowerWeight(); got != 3 {
		t.Fatalf("total tron power weight: want 3, got %d", got)
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
	ctx.DynProps.SetAllowDelegateResource(true)

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
	legacy := rawdb.ReadDrAccountIndexLegacy(ctx.DB, owner.Bytes())
	if legacy == nil || len(legacy.ToAccounts) != 1 || string(legacy.ToAccounts[0]) != string(receiver.Bytes()) {
		t.Fatalf("legacy owner index wrong: %+v", legacy)
	}
	if directional := rawdb.ReadDrAccountIndexEntry(ctx.DB, rawdb.DrAccIdxV1From, owner.Bytes(), receiver.Bytes()); directional != nil {
		t.Fatalf("pre-optimization must not write directional index, got %+v", directional)
	}
}

func TestFreezeBalanceExecute_DelegatedAllowNewRewardUsesReceiverWeightDelta(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(42)
	receiverByte := byte(43)
	receiver := makeTestAddr(receiverByte)
	seedAccount(statedb, owner, 10_000_000)
	seedAccount(statedb, receiver, 0)
	statedb.FreezeV1DelegatedBandwidth(owner, receiver, 1_900_000)

	tx := makeFreezeBalanceTx(42, 1_900_000, 3, corepb.ResourceCode_BANDWIDTH, &receiverByte)
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.DynProps.SetAllowNewReward(true)

	if _, err := (&FreezeBalanceActuator{}).Execute(ctx); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if got := ctx.DynProps.TotalNetWeight(); got != 2 {
		t.Fatalf("delegated total net weight delta: want 2, got %d", got)
	}
}

func TestFreezeBalanceExecute_DelegatedOptimizationWritesDirectionalIndex(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(44)
	receiverByte := byte(45)
	receiver := makeTestAddr(receiverByte)
	seedAccount(statedb, owner, 10_000_000)
	seedAccount(statedb, receiver, 0)

	tx := makeFreezeBalanceTx(44, 3_000_000, 3, corepb.ResourceCode_BANDWIDTH, &receiverByte)
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.DynProps.SetAllowDelegateOptimization(true)

	if _, err := (&FreezeBalanceActuator{}).Execute(ctx); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if legacy := rawdb.ReadDrAccountIndexLegacy(ctx.DB, owner.Bytes()); legacy != nil {
		t.Fatalf("post-optimization legacy index should be absent, got %+v", legacy)
	}
	directional := rawdb.ReadDrAccountIndexEntry(ctx.DB, rawdb.DrAccIdxV1From, owner.Bytes(), receiver.Bytes())
	if directional == nil || string(directional.Account) != string(receiver.Bytes()) || directional.Timestamp != ctx.PrevBlockTime {
		t.Fatalf("directional owner index wrong: %+v", directional)
	}
}

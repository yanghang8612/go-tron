package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
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
	if result.UnfreezeAmount != 1_000_000 {
		t.Fatalf("unfreeze amount: want 1000000, got %d", result.UnfreezeAmount)
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

func TestUnfreezeBalanceExecute_ClearsVotes(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(31)
	witness := makeTestAddr(32)
	seedAccount(statedb, owner, 9_000_000)
	statedb.CreateAccount(witness, corepb.AccountType_Normal)
	statedb.SetVotes(owner, []*corepb.Vote{{VoteAddress: witness[:], VoteCount: 1}})
	statedb.FreezeV1Bandwidth(owner, 1_000_000, 500_000)

	tx := makeUnfreezeBalanceTx(31, corepb.ResourceCode_BANDWIDTH, nil)
	act := &UnfreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if votes := statedb.GetVotes(owner); len(votes) != 0 {
		t.Fatalf("votes should be cleared after V1 unfreeze, got %v", votes)
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

func TestUnfreezeBalanceExecute_TronPower(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(7)
	seedAccount(statedb, owner, 7_000_000)
	statedb.FreezeV1TronPower(owner, 3_000_000, 500_000)

	tx := makeUnfreezeBalanceTx(7, corepb.ResourceCode_TRON_POWER, nil)
	act := &UnfreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowNewResourceModel(true)
	ctx.DynProps.SetTotalTronPowerWeight(3)

	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate: %v", err)
	}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}
	if got := statedb.GetBalance(owner); got != 10_000_000 {
		t.Fatalf("balance: want 10000000, got %d", got)
	}
	if got := statedb.GetAccount(owner).V1TronPowerFrozen(); got != 0 {
		t.Fatalf("V1 tron power frozen: want 0, got %d", got)
	}
	if got := ctx.DynProps.TotalTronPowerWeight(); got != 0 {
		t.Fatalf("total tron power weight: want 0, got %d", got)
	}
}

// TestUnfreezeBalanceExecute_DelegatedMissingContractReceiver covers the V1
// delegated-unfreeze global-weight release when the receiver account no longer
// exists — a contract receiver that self-destructed before the owner unfreezes.
// Under allow_tvm_constantinople java-tron's UnfreezeBalanceActuator takes the
// floor branch (decrease = -unfreezeBalance / TRX_PRECISION) whenever the
// receiver is null OR a contract, so total_energy_weight drops by the unfrozen
// weight. Routing a missing receiver through DecrementReceiverAcquired (which
// returns 0 for a non-existent account) leaks total_energy_weight HIGH. This
// completes a7fda66f (existing-Contract case) for the null receiver; the test
// sets allow_new_reward (the only era where the weight delta isn't floor-
// overridden, so where the receiver branch is actually live).
func TestUnfreezeBalanceExecute_DelegatedMissingContractReceiver(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(8)
	receiverByte := byte(9)
	receiver := makeTestAddr(receiverByte)
	seedAccount(statedb, owner, 7_000_000)
	// Receiver is intentionally NOT created (it self-destructed). The owner's
	// delegated frozen energy is still recorded; the receiver leg is skipped.
	statedb.FreezeV1DelegatedEnergy(owner, receiver, 3_000_000)

	tx := makeUnfreezeBalanceTx(8, corepb.ResourceCode_ENERGY, &receiverByte)
	act := &UnfreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.DynProps.SetAllowTvmConstantinople(true)
	ctx.DynProps.SetAllowNewReward(true)
	ctx.DynProps.SetTotalEnergyWeight(100)
	if err := statedb.WriteDelegatedResourceLegacy(owner, receiver, &rawdb.DelegatedResource{
		From:                   owner,
		To:                     receiver,
		FrozenBalanceForEnergy: 3_000_000,
		ExpireTimeForEnergy:    500_000,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// java subtracts unfreezeBalance / TRX_PRECISION = 3_000_000 / 1_000_000 = 3.
	if got := ctx.DynProps.TotalEnergyWeight(); got != 97 {
		t.Fatalf("total_energy_weight: want 97 (100 - 3), got %d", got)
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
	ctx.DynProps.SetAllowDelegateResource(true)
	if err := statedb.WriteDelegatedResourceLegacy(owner, receiver, &rawdb.DelegatedResource{
		From:                      owner,
		To:                        receiver,
		FrozenBalanceForBandwidth: 3_000_000,
		ExpireTimeForBandwidth:    500_000,
	}); err != nil {
		t.Fatal(err)
	}

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

// TestUnfreezeBalanceValidate_DelegatedEnergyExpiryGatedByMultiSign locks in
// java-tron's historical DelegatedResourceCapsule behavior: before the
// ALLOW_MULTI_SIGN proposal, delegated ENERGY unfreeze validation reads the
// BANDWIDTH expiry; after activation it reads the ENERGY expiry.
func TestUnfreezeBalanceValidate_DelegatedEnergyExpiryGatedByMultiSign(t *testing.T) {
	tests := []struct {
		name            string
		allowMultiSign  bool
		bandwidthExpiry int64
		energyExpiry    int64
		wantErr         bool
	}{
		{
			name:            "before proposal uses expired bandwidth timestamp",
			bandwidthExpiry: 500_000,
			energyExpiry:    2_000_000,
		},
		{
			name:            "before proposal rejects future bandwidth timestamp",
			bandwidthExpiry: 2_000_000,
			energyExpiry:    500_000,
			wantErr:         true,
		},
		{
			name:            "after proposal rejects future energy timestamp",
			allowMultiSign:  true,
			bandwidthExpiry: 500_000,
			energyExpiry:    2_000_000,
			wantErr:         true,
		},
		{
			name:            "after proposal uses expired energy timestamp",
			allowMultiSign:  true,
			bandwidthExpiry: 2_000_000,
			energyExpiry:    500_000,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ownerByte := byte(40 + i*2)
			receiverByte := ownerByte + 1
			owner := makeTestAddr(ownerByte)
			receiver := makeTestAddr(receiverByte)
			statedb := setupStateDB(t)
			seedAccount(statedb, owner, 10_000_000)
			seedAccount(statedb, receiver, 0)

			tx := makeUnfreezeBalanceTx(ownerByte, corepb.ResourceCode_ENERGY, &receiverByte)
			ctx := setupContext(t, statedb, tx)
			ctx.DynProps.SetAllowDelegateResource(true)
			ctx.DynProps.SetAllowMultiSign(tt.allowMultiSign)
			if err := statedb.WriteDelegatedResourceLegacy(owner, receiver, &rawdb.DelegatedResource{
				From:                   owner,
				To:                     receiver,
				FrozenBalanceForEnergy: 1_000_000,
				ExpireTimeForBandwidth: tt.bandwidthExpiry,
				ExpireTimeForEnergy:    tt.energyExpiry,
			}); err != nil {
				t.Fatal(err)
			}

			err := (&UnfreezeBalanceActuator{}).Validate(ctx)
			if tt.wantErr {
				if err == nil || err.Error() != "It's not time to unfreeze." {
					t.Fatalf("Validate error = %v, want expiry error", err)
				}
			} else if err != nil {
				t.Fatalf("Validate: %v", err)
			}
		})
	}
}

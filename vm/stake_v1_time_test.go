package vm

import (
	"testing"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestVMStakeV1FreezeUsesLatestBlockHeaderTimestamp(t *testing.T) {
	const (
		prevBlockTime = int64(1_000_000)
		currentTime   = int64(1_003_000)
		amount        = int64(tvmTRXPrecision)
	)

	tvm, statedb, _ := newTestTVMForCreate(t, TVMConfig{}, func(dp *state.DynamicProperties) {
		dp.SetLatestBlockHeaderTimestamp(prevBlockTime)
	})
	tvm.Timestamp = currentTime

	caller := tcommon.Address{0x41, 0x01}
	statedb.CreateAccount(caller, corepb.AccountType_Normal)
	statedb.AddBalance(caller, amount)

	receiver := addressToUint256(caller)
	stack := newStack()
	stack.push(&receiver)
	stack.push(uint256.NewInt(uint64(amount)))
	stack.push(uint256.NewInt(1))
	contract := NewContract(caller, caller, 0, 100000)

	if _, err := opFreeze(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("FREEZE opcode error: %v", err)
	}
	result := stack.pop()
	if got := result.Uint64(); got != 1 {
		t.Fatalf("FREEZE result: got %d, want 1", got)
	}
	wantExpire := prevBlockTime + 3*86_400_000
	if got := statedb.GetFreezeV1ExpireTime(caller, 1); got != wantExpire {
		t.Fatalf("energy expire time: got %d, want %d", got, wantExpire)
	}
}

func TestVMStakeV1UnfreezeUsesLatestBlockHeaderTimestamp(t *testing.T) {
	const (
		prevBlockTime = int64(1_000_000)
		currentTime   = int64(1_003_000)
		amount        = int64(tvmTRXPrecision)
	)

	tvm, statedb, _ := newTestTVMForCreate(t, TVMConfig{}, func(dp *state.DynamicProperties) {
		dp.SetLatestBlockHeaderTimestamp(prevBlockTime)
	})
	tvm.Timestamp = currentTime

	caller := tcommon.Address{0x41, 0x02}
	statedb.CreateAccount(caller, corepb.AccountType_Normal)
	statedb.FreezeV1Energy(caller, amount, prevBlockTime+1)

	receiver := addressToUint256(caller)
	stack := newStack()
	stack.push(&receiver)
	stack.push(uint256.NewInt(1))
	contract := NewContract(caller, caller, 0, 100000)

	if _, err := opUnfreeze(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("UNFREEZE opcode error: %v", err)
	}
	result := stack.pop()
	if got := result.Uint64(); got != 0 {
		t.Fatalf("UNFREEZE result before latest header expiry: got %d, want 0", got)
	}
	account := statedb.GetAccount(caller)
	if account == nil {
		t.Fatal("caller account missing")
	}
	if got := account.FrozenEnergyAmount(); got != amount {
		t.Fatalf("frozen energy after early UNFREEZE: got %d, want %d", got, amount)
	}
}

func TestVMStakeV1DelegatedUnfreezePreservesZeroResourceRecord(t *testing.T) {
	const amount = int64(tvmTRXPrecision)
	tvm, statedb, dp := newFreezeV1TVM(t)
	owner := tcommon.Address{0x41, 0x03}
	receiver := tcommon.Address{0x41, 0x04}
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(receiver, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 10*amount)

	if got := callFreeze(t, tvm, owner, receiver, uint256.NewInt(uint64(amount)), 0); got != 1 {
		t.Fatalf("delegated freeze: got %d, want 1", got)
	}
	dr := statedb.ReadDelegatedResourceLegacy(owner, receiver)
	if dr == nil {
		t.Fatal("delegated resource missing after freeze")
	}
	dr.ExpireTimeForBandwidth = dp.LatestBlockHeaderTimestamp()
	if err := statedb.WriteDelegatedResourceLegacy(owner, receiver, dr); err != nil {
		t.Fatalf("set expiry: %v", err)
	}

	stack := newStack()
	receiverWord := addressToUint256(receiver)
	stack.push(&receiverWord)
	stack.push(uint256.NewInt(0)) // BANDWIDTH
	contract := NewContract(owner, owner, 0, 100000)
	if _, err := opUnfreeze(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("delegated unfreeze: %v", err)
	}
	result := stack.pop()
	if got := result.Uint64(); got != 1 {
		t.Fatalf("delegated unfreeze result: got %d, want 1", got)
	}
	got := statedb.ReadDelegatedResourceLegacy(owner, receiver)
	if got == nil {
		t.Fatal("java-compatible all-zero delegated resource was deleted")
	}
	want := &rawdb.DelegatedResource{From: owner, To: receiver}
	if got.FrozenBalanceForBandwidth != want.FrozenBalanceForBandwidth ||
		got.ExpireTimeForBandwidth != want.ExpireTimeForBandwidth ||
		got.FrozenBalanceForEnergy != want.FrozenBalanceForEnergy ||
		got.ExpireTimeForEnergy != want.ExpireTimeForEnergy {
		t.Fatalf("delegated resource after full unfreeze = %+v, want zero balances/expiries", got)
	}
}

func TestVMFreezeExpireTimeRequiresNonzeroBalanceAndUsesFirstBandwidthRow(t *testing.T) {
	tvm, statedb, _ := newFreezeV1TVM(t)
	owner := tcommon.Address{0x41, 0x05}
	receiver := tcommon.Address{0x41, 0x06}
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(receiver, corepb.AccountType_Normal)
	statedb.FreezeV1Bandwidth(owner, 0, 9_000)
	account := statedb.GetAccount(owner)
	account.Proto().Frozen = append([]*corepb.Account_Frozen(nil), account.Proto().Frozen...)
	account.Proto().Frozen = append(account.Proto().Frozen, &corepb.Account_Frozen{
		FrozenBalance: tvmTRXPrecision,
		ExpireTime:    12_000,
	})
	if err := statedb.WriteDelegatedResourceLegacy(owner, receiver, &rawdb.DelegatedResource{
		From:                      owner,
		To:                        receiver,
		ExpireTimeForBandwidth:    15_000,
		FrozenBalanceForBandwidth: 0,
	}); err != nil {
		t.Fatalf("write zero delegated record: %v", err)
	}

	query := func(target tcommon.Address) uint64 {
		stack := newStack()
		targetWord := addressToUint256(target)
		stack.push(&targetWord)
		stack.push(uint256.NewInt(0))
		contract := NewContract(owner, owner, 0, 100000)
		if _, err := opFreezeExpireTime(nil, tvm.interpreter, contract, nil, stack); err != nil {
			t.Fatalf("FREEZEEXPIRETIME: %v", err)
		}
		result := stack.pop()
		return result.Uint64()
	}
	if got := query(owner); got != 0 {
		t.Fatalf("self zero first balance returned expiry %d, want 0", got)
	}
	if got := query(receiver); got != 0 {
		t.Fatalf("delegated zero balance returned expiry %d, want 0", got)
	}
}

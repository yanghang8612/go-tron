package vm

import (
	"testing"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
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

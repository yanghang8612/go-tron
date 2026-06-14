package vm

import (
	"testing"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// selfFreeze runs the FREEZE opcode as a contract freezing its own balance
// (receiver == caller) for the given resource (0 = bandwidth, 1 = energy).
func selfFreeze(t *testing.T, tvm *TVM, caller tcommon.Address, resourceType, amount int64) {
	t.Helper()
	stack := newStack()
	receiver := addressToUint256(caller)
	stack.push(&receiver)
	stack.push(uint256.NewInt(uint64(amount)))
	stack.push(uint256.NewInt(uint64(resourceType)))
	contract := NewContract(caller, caller, 0, 100_000)
	if _, err := opFreeze(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("FREEZE (resource %d) error: %v", resourceType, err)
	}
	result := stack.pop()
	if got := result.Uint64(); got != 1 {
		t.Fatalf("FREEZE (resource %d) result: got %d, want 1", resourceType, got)
	}
}

// TestSelfDestructReleasesFrozenV1WeightToObtainer mirrors java-tron
// Program.suicide -> transferDelegatedResourceToInheritor for the pre-Stake-2.0
// Nile era (allow_tvm_freeze on, allow_tvm_selfdestruct_restriction off). A
// contract self-freezes energy and bandwidth via FREEZE, then SELFDESTRUCTs to
// a distinct obtainer; the global total_energy_weight/total_net_weight must
// return to their pre-freeze values and the frozen balance is credited to the
// obtainer. This is the consensus path that diverged at Nile 19,716,962.
func TestSelfDestructReleasesFrozenV1WeightToObtainer(t *testing.T) {
	const (
		energyAmount    = int64(5 * tvmTRXPrecision)
		bandwidthAmount = int64(2 * tvmTRXPrecision)
	)
	tvm, statedb, dp := newTestTVMForCreate(t, TVMConfig{Freeze: true, TransferTrc10: true},
		func(dp *state.DynamicProperties) {
			dp.SetLatestBlockHeaderTimestamp(1_000_000)
		})

	contractAddr := tcommon.Address{0x41, 0x11}
	obtainer := tcommon.Address{0x41, 0x22}
	statedb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	statedb.AddBalance(contractAddr, energyAmount+bandwidthAmount)

	baseEnergyWeight := dp.TotalEnergyWeight()
	baseNetWeight := dp.TotalNetWeight()

	selfFreeze(t, tvm, contractAddr, 1, energyAmount)
	selfFreeze(t, tvm, contractAddr, 0, bandwidthAmount)

	if got, want := dp.TotalEnergyWeight(), baseEnergyWeight+energyAmount/tvmTRXPrecision; got != want {
		t.Fatalf("energy weight after freeze: got %d, want %d", got, want)
	}
	if got, want := dp.TotalNetWeight(), baseNetWeight+bandwidthAmount/tvmTRXPrecision; got != want {
		t.Fatalf("net weight after freeze: got %d, want %d", got, want)
	}
	if got := statedb.GetBalance(contractAddr); got != 0 {
		t.Fatalf("contract liquid balance after freezing all: got %d, want 0", got)
	}

	stack := newStack()
	word := addressToUint256(obtainer)
	stack.push(&word)
	contract := NewContract(tcommon.Address{0x41, 0x02}, contractAddr, 0, 100_000)
	if _, err := opSelfDestruct(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opSelfDestruct: %v", err)
	}

	if got := dp.TotalEnergyWeight(); got != baseEnergyWeight {
		t.Fatalf("energy weight after selfdestruct: got %d, want %d (pre-freeze)", got, baseEnergyWeight)
	}
	if got := dp.TotalNetWeight(); got != baseNetWeight {
		t.Fatalf("net weight after selfdestruct: got %d, want %d (pre-freeze)", got, baseNetWeight)
	}
	if got, want := statedb.GetBalance(obtainer), energyAmount+bandwidthAmount; got != want {
		t.Fatalf("obtainer balance: got %d, want %d (frozen energy+bandwidth)", got, want)
	}
	if !statedb.HasSelfDestructed(contractAddr) {
		t.Fatal("contract should be marked self-destructed")
	}
}

// TestSelfDestructToSelfRoutesFrozenBalanceToBlackhole covers the
// owner == obtainer branch of java suicide(): the released frozen balance goes
// to the chain's blackhole address rather than the (self) obtainer.
func TestSelfDestructToSelfRoutesFrozenBalanceToBlackhole(t *testing.T) {
	const energyAmount = int64(3 * tvmTRXPrecision)
	tvm, statedb, dp := newTestTVMForCreate(t, TVMConfig{Freeze: true, TransferTrc10: true},
		func(dp *state.DynamicProperties) {
			dp.SetLatestBlockHeaderTimestamp(1_000_000)
		})
	blackhole := tcommon.Address{0x41, 0xbb}
	tvm.SetBlackholeAddress(blackhole)

	contractAddr := tcommon.Address{0x41, 0x11}
	statedb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	statedb.AddBalance(contractAddr, energyAmount)

	baseEnergyWeight := dp.TotalEnergyWeight()
	selfFreeze(t, tvm, contractAddr, 1, energyAmount)

	stack := newStack()
	word := addressToUint256(contractAddr) // beneficiary == owner
	stack.push(&word)
	contract := NewContract(tcommon.Address{0x41, 0x02}, contractAddr, 0, 100_000)
	if _, err := opSelfDestruct(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opSelfDestruct: %v", err)
	}

	if got := dp.TotalEnergyWeight(); got != baseEnergyWeight {
		t.Fatalf("energy weight after selfdestruct: got %d, want %d", got, baseEnergyWeight)
	}
	if got := statedb.GetBalance(blackhole); got != energyAmount {
		t.Fatalf("blackhole balance: got %d, want %d", got, energyAmount)
	}
}

// TestSelfDestructWithoutFrozenDoesNotMaterializeObtainer guards the scope of
// the freeze-transfer port: a contract with no frozen resources self-destructs
// to a fresh obtainer with allow_tvm_freeze on. java's addBalance(inheritor, 0)
// is a no-op on the already-existing inheritor, so go-tron must not GetOrCreate
// a bare obtainer account here (which would diverge from pre-19.5M behavior).
func TestSelfDestructWithoutFrozenDoesNotMaterializeObtainer(t *testing.T) {
	tvm, statedb, dp := newTestTVMForCreate(t, TVMConfig{Freeze: true, TransferTrc10: true},
		func(dp *state.DynamicProperties) {
			dp.SetLatestBlockHeaderTimestamp(1_000_000)
		})

	contractAddr := tcommon.Address{0x41, 0x11}
	obtainer := tcommon.Address{0x41, 0x33}
	statedb.CreateAccount(contractAddr, corepb.AccountType_Contract) // no balance, no frozen, no tokens

	baseEnergyWeight := dp.TotalEnergyWeight()
	baseNetWeight := dp.TotalNetWeight()

	stack := newStack()
	word := addressToUint256(obtainer)
	stack.push(&word)
	contract := NewContract(tcommon.Address{0x41, 0x02}, contractAddr, 0, 100_000)
	if _, err := opSelfDestruct(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opSelfDestruct: %v", err)
	}

	if dp.TotalEnergyWeight() != baseEnergyWeight {
		t.Fatalf("energy weight changed with nothing frozen: got %d, want %d", dp.TotalEnergyWeight(), baseEnergyWeight)
	}
	if dp.TotalNetWeight() != baseNetWeight {
		t.Fatalf("net weight changed with nothing frozen: got %d, want %d", dp.TotalNetWeight(), baseNetWeight)
	}
	if statedb.AccountExists(obtainer) {
		t.Fatal("zero-credit inheritor must not be materialized by the freeze transfer")
	}
}

// TestSelfDestructRestrictionClearsOwnerFreezeInPlace covers the gated
// clearOwnerFreeze branch: when allow_tvm_selfdestruct_restriction is active a
// non-new contract is not deleted (suicide2), so its frozen slots must be
// zeroed in place while the weight is still released and the obtainer credited.
// java leaves FrozenBalanceForEnergy present-but-zero (setFrozenForEnergy(0,0)).
func TestSelfDestructRestrictionClearsOwnerFreezeInPlace(t *testing.T) {
	const energyAmount = int64(4 * tvmTRXPrecision)
	tvm, statedb, dp := newTestTVMForCreate(t, TVMConfig{Freeze: true, TransferTrc10: true, SelfdestructRestrict: true},
		func(dp *state.DynamicProperties) {
			dp.SetLatestBlockHeaderTimestamp(1_000_000)
		})

	contractAddr := tcommon.Address{0x41, 0x11}
	obtainer := tcommon.Address{0x41, 0x22}
	statedb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	statedb.AddBalance(contractAddr, energyAmount)

	baseEnergyWeight := dp.TotalEnergyWeight()
	selfFreeze(t, tvm, contractAddr, 1, energyAmount)

	stack := newStack()
	word := addressToUint256(obtainer)
	stack.push(&word)
	contract := NewContract(tcommon.Address{0x41, 0x02}, contractAddr, 0, 1_000_000)
	if _, err := opSelfDestruct(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opSelfDestruct: %v", err)
	}

	if got := dp.TotalEnergyWeight(); got != baseEnergyWeight {
		t.Fatalf("energy weight after selfdestruct: got %d, want %d", got, baseEnergyWeight)
	}
	if got := statedb.GetBalance(obtainer); got != energyAmount {
		t.Fatalf("obtainer balance: got %d, want %d", got, energyAmount)
	}
	// Under restriction the owner is not deleted, just zeroed.
	if statedb.HasSelfDestructed(contractAddr) {
		t.Fatal("non-new contract must not be deleted under selfdestruct restriction")
	}
	owner := statedb.GetAccount(contractAddr)
	if owner == nil {
		t.Fatal("owner account should still exist under restriction")
	}
	if got := owner.FrozenEnergyAmount(); got != 0 {
		t.Fatalf("owner frozen energy after clear: got %d, want 0", got)
	}
	if res := owner.Proto().GetAccountResource(); res == nil || res.GetFrozenBalanceForEnergy() == nil {
		t.Fatal("FrozenBalanceForEnergy must remain present-but-zero to match java encoding")
	}
}

package vm

import (
	"testing"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/params"
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

	// java OperationActions.suicideAction2 → canSuicide2()/freezeV1Check reverts a
	// SELFDESTRUCT while the owner holds UNEXPIRED V1 frozen, so the clearOwnerFreeze
	// branch is reachable only once the freeze has expired. Advance past the 3-day
	// expiry (opFreeze set expire = 1_000_000 + 259_200_000) so the suicide is allowed.
	dp.SetLatestBlockHeaderTimestamp(261_000_000)

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

// TestSelfDestructRevertsOnUnexpiredFrozen locks the canSuicide2 guard (java
// OperationActions.suicideAction2 → program.canSuicide2()/freezeV1Check): a
// non-new contract under restriction holding UNEXPIRED V1 frozen resources must
// REVERT the SELFDESTRUCT, not destroy + clear. Without the guard go-tron drifted
// from java (ran the freeze-inheritance where java reverts).
func TestSelfDestructRevertsOnUnexpiredFrozen(t *testing.T) {
	const energyAmount = int64(4 * tvmTRXPrecision)
	tvm, statedb, _ := newTestTVMForCreate(t, TVMConfig{Freeze: true, TransferTrc10: true, SelfdestructRestrict: true},
		func(dp *state.DynamicProperties) {
			dp.SetLatestBlockHeaderTimestamp(1_000_000)
		})
	contractAddr := tcommon.Address{0x41, 0x11}
	obtainer := tcommon.Address{0x41, 0x22}
	statedb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	statedb.AddBalance(contractAddr, energyAmount)
	selfFreeze(t, tvm, contractAddr, 1, energyAmount) // unexpired (now + 3 days)

	stack := newStack()
	word := addressToUint256(obtainer)
	stack.push(&word)
	contract := NewContract(tcommon.Address{0x41, 0x02}, contractAddr, 0, 1_000_000)
	if _, err := opSelfDestruct(nil, tvm.interpreter, contract, nil, stack); err != ErrExecutionReverted {
		t.Fatalf("expected ErrExecutionReverted on unexpired frozen, got %v", err)
	}
	if statedb.HasSelfDestructed(contractAddr) {
		t.Fatal("contract must not be destroyed when canSuicide2 reverts")
	}
}

// selfFreezeV2 runs the FREEZEBALANCEV2 opcode as a contract freezing its own
// balance into the Stake-2.0 (FrozenV2) pool for the given resource.
func selfFreezeV2(t *testing.T, tvm *TVM, caller tcommon.Address, resource corepb.ResourceCode, amount int64) {
	t.Helper()
	stack := newStack()
	stack.push(uint256.NewInt(uint64(resource)))
	stack.push(uint256.NewInt(uint64(amount)))
	contract := NewContract(caller, caller, 0, 100_000)
	if _, err := opFreezeBalanceV2(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("FREEZEBALANCEV2 (resource %d) error: %v", resource, err)
	}
	result := stack.pop()
	if got := result.Uint64(); got != 1 {
		t.Fatalf("FREEZEBALANCEV2 (resource %d) result: got %d, want 1", resource, got)
	}
}

// TestSelfDestructMovesFrozenV2ToObtainerConservingWeight mirrors java-tron
// Program.suicide -> transferFrozenV2BalanceToInheritor for the Stake-2.0 era
// (allow_tvm_freeze_v2 on). A contract self-freezes V2 energy and bandwidth,
// then SELFDESTRUCTs to a distinct obtainer. Unlike the V1 release, the V2
// frozen balance MOVES (still frozen) to the obtainer and the global
// total_energy_weight/total_net_weight are CONSERVED, not reduced.
func TestSelfDestructMovesFrozenV2ToObtainerConservingWeight(t *testing.T) {
	const (
		energyAmount    = int64(5 * tvmTRXPrecision)
		bandwidthAmount = int64(3 * tvmTRXPrecision)
	)
	tvm, statedb, dp := newTestTVMForCreate(t, TVMConfig{Freeze: true, StakingV2: true, TransferTrc10: true},
		func(dp *state.DynamicProperties) {
			dp.SetLatestBlockHeaderTimestamp(1_000_000)
		})

	contractAddr := tcommon.Address{0x41, 0x11}
	obtainer := tcommon.Address{0x41, 0x22}
	statedb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	statedb.CreateAccount(obtainer, corepb.AccountType_Normal)
	statedb.AddBalance(contractAddr, energyAmount+bandwidthAmount)

	selfFreezeV2(t, tvm, contractAddr, corepb.ResourceCode_ENERGY, energyAmount)
	selfFreezeV2(t, tvm, contractAddr, corepb.ResourceCode_BANDWIDTH, bandwidthAmount)

	wEnergy := dp.TotalEnergyWeight()
	wNet := dp.TotalNetWeight()
	if want := energyAmount / tvmTRXPrecision; wEnergy != want {
		t.Fatalf("energy weight after freezeV2: got %d, want %d", wEnergy, want)
	}
	if want := bandwidthAmount / tvmTRXPrecision; wNet != want {
		t.Fatalf("net weight after freezeV2: got %d, want %d", wNet, want)
	}

	stack := newStack()
	word := addressToUint256(obtainer)
	stack.push(&word)
	contract := NewContract(tcommon.Address{0x41, 0x02}, contractAddr, 0, 1_000_000)
	if _, err := opSelfDestruct(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opSelfDestruct: %v", err)
	}

	if got := dp.TotalEnergyWeight(); got != wEnergy {
		t.Fatalf("energy weight after selfdestruct: got %d, want %d (conserved)", got, wEnergy)
	}
	if got := dp.TotalNetWeight(); got != wNet {
		t.Fatalf("net weight after selfdestruct: got %d, want %d (conserved)", got, wNet)
	}
	if got := statedb.GetFrozenV2Amount(obtainer, corepb.ResourceCode_ENERGY); got != energyAmount {
		t.Fatalf("obtainer frozen V2 energy: got %d, want %d", got, energyAmount)
	}
	if got := statedb.GetFrozenV2Amount(obtainer, corepb.ResourceCode_BANDWIDTH); got != bandwidthAmount {
		t.Fatalf("obtainer frozen V2 bandwidth: got %d, want %d", got, bandwidthAmount)
	}
	// V2 stays frozen on the obtainer; nothing is credited as liquid balance.
	if got := statedb.GetBalance(obtainer); got != 0 {
		t.Fatalf("obtainer liquid balance: got %d, want 0 (V2 stays frozen)", got)
	}
	if !statedb.HasSelfDestructed(contractAddr) {
		t.Fatal("contract should be marked self-destructed")
	}
}

// TestSelfDestructWithdrawsExpiredUnfrozenV2ToObtainer covers java
// transferFrozenV2BalanceToInheritor's expire-unfreeze withdrawal: an expired
// pending V2 unfreeze is paid out to the inheritor's liquid balance, a
// "withdrawExpireUnfreezeWhileSuiciding" internal tx is recorded, and the value
// is added to the suicide internal tx value.
func TestSelfDestructWithdrawsExpiredUnfrozenV2ToObtainer(t *testing.T) {
	const (
		liquid  = int64(7 * tvmTRXPrecision)
		expired = int64(4 * tvmTRXPrecision)
	)
	tvm, statedb, _ := newTestTVMForCreate(t, TVMConfig{Freeze: true, StakingV2: true, TransferTrc10: true},
		func(dp *state.DynamicProperties) {
			dp.SetLatestBlockHeaderTimestamp(2_000_000)
		})

	contractAddr := tcommon.Address{0x41, 0x11}
	obtainer := tcommon.Address{0x41, 0x22}
	statedb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	statedb.CreateAccount(obtainer, corepb.AccountType_Normal)
	statedb.AddBalance(contractAddr, liquid)
	// An expired pending unfreeze (expire <= head timestamp).
	statedb.AddUnfreezeV2(contractAddr, corepb.ResourceCode_ENERGY, expired, 1_500_000)

	stack := newStack()
	word := addressToUint256(obtainer)
	stack.push(&word)
	contract := NewContract(tcommon.Address{0x41, 0x02}, contractAddr, 0, 1_000_000)
	if _, err := opSelfDestruct(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opSelfDestruct: %v", err)
	}

	if got := statedb.GetBalance(obtainer); got != liquid+expired {
		t.Fatalf("obtainer balance: got %d, want %d (liquid+expired)", got, liquid+expired)
	}
	if len(tvm.InternalTransactions) != 2 {
		t.Fatalf("internal transactions: got %d, want 2", len(tvm.InternalTransactions))
	}
	suicideIT := tvm.InternalTransactions[0]
	if string(suicideIT.Note) != "suicide" {
		t.Fatalf("first internal tx note: got %q, want suicide", suicideIT.Note)
	}
	if got := suicideIT.CallValueInfo[0].CallValue; got != liquid+expired {
		t.Fatalf("suicide internal tx value: got %d, want %d (bumped by expired)", got, liquid+expired)
	}
	withdrawIT := tvm.InternalTransactions[1]
	if string(withdrawIT.Note) != "withdrawExpireUnfreezeWhileSuiciding" {
		t.Fatalf("second internal tx note: got %q", withdrawIT.Note)
	}
	if got := withdrawIT.CallValueInfo[0].CallValue; got != expired {
		t.Fatalf("withdraw internal tx value: got %d, want %d", got, expired)
	}
}

// TestSelfDestructRestrictionClearsOwnerFreezeV2InPlace covers the suicide2
// (kept-alive) path under allow_tvm_selfdestruct_restriction: the owner's V2
// frozen balance, resource usage, recovery window and pending unfreezes are all
// zeroed in place (clearOwnerFreezeV2) while the obtainer inherits the frozen
// balance, the folded usage and the expired unfreeze.
func TestSelfDestructRestrictionClearsOwnerFreezeV2InPlace(t *testing.T) {
	const (
		energyAmount = int64(6 * tvmTRXPrecision)
		expired      = int64(2 * tvmTRXPrecision)
	)
	usage := int64(params.WindowSizeSlots) // chosen so window recovery is the identity
	tvm, statedb, _ := newTestTVMForCreate(t, TVMConfig{Freeze: true, StakingV2: true, TransferTrc10: true, SelfdestructRestrict: true},
		func(dp *state.DynamicProperties) {
			dp.SetLatestBlockHeaderTimestamp(3_000_000)
		})
	tvm.Timestamp = 5000 // ResourceTime() == 5000 (no head slot)

	contractAddr := tcommon.Address{0x41, 0x11}
	obtainer := tcommon.Address{0x41, 0x22}
	statedb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	statedb.CreateAccount(obtainer, corepb.AccountType_Normal)
	statedb.AddBalance(contractAddr, energyAmount)
	selfFreezeV2(t, tvm, contractAddr, corepb.ResourceCode_ENERGY, energyAmount)
	// Owner energy usage with consume time == now so recovery folds the identity.
	statedb.SetEnergyUsage(contractAddr, usage)
	statedb.SetLatestConsumeTimeForEnergy(contractAddr, tvm.ResourceTime())
	statedb.AddUnfreezeV2(contractAddr, corepb.ResourceCode_ENERGY, expired, 2_000_000)

	stack := newStack()
	word := addressToUint256(obtainer)
	stack.push(&word)
	contract := NewContract(tcommon.Address{0x41, 0x02}, contractAddr, 0, 1_000_000)
	if _, err := opSelfDestruct(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opSelfDestruct: %v", err)
	}

	// suicide2: the owner survives, V2 state zeroed in place.
	if statedb.HasSelfDestructed(contractAddr) {
		t.Fatal("non-new contract must not be deleted under selfdestruct restriction")
	}
	owner := statedb.GetAccount(contractAddr)
	if owner == nil {
		t.Fatal("owner should still exist under restriction")
	}
	if got := statedb.GetFrozenV2Amount(contractAddr, corepb.ResourceCode_ENERGY); got != 0 {
		t.Fatalf("owner frozen V2 energy after clear: got %d, want 0", got)
	}
	if got := statedb.GetEnergyUsage(contractAddr); got != 0 {
		t.Fatalf("owner energy usage after clear: got %d, want 0", got)
	}
	if got := owner.RawEnergyWindowSize(); got != 0 {
		t.Fatalf("owner energy window after clear: got %d, want 0", got)
	}
	if n := len(owner.UnfrozenV2()); n != 0 {
		t.Fatalf("owner unfrozenV2 after clear: got %d entries, want 0", n)
	}
	// Obtainer inherits the frozen V2, the folded usage and the expired balance.
	if got := statedb.GetFrozenV2Amount(obtainer, corepb.ResourceCode_ENERGY); got != energyAmount {
		t.Fatalf("obtainer frozen V2 energy: got %d, want %d", got, energyAmount)
	}
	if got := statedb.GetEnergyUsage(obtainer); got != usage {
		t.Fatalf("obtainer folded energy usage: got %d, want %d", got, usage)
	}
	if got := statedb.GetBalance(obtainer); got != expired {
		t.Fatalf("obtainer balance: got %d, want %d (expired unfreeze)", got, expired)
	}
}

// TestSelfDestructFoldsOwnerBandwidthUsageIntoObtainer locks the bandwidth
// usage-fold (java unDelegateIncrease) on the old-suicide path: with the owner's
// consume time equal to now (no decay) the recovered usage equals the stored
// usage and folds whole into the fresh obtainer's window.
func TestSelfDestructFoldsOwnerBandwidthUsageIntoObtainer(t *testing.T) {
	usage := int64(params.WindowSizeSlots)
	tvm, statedb, _ := newTestTVMForCreate(t, TVMConfig{Freeze: true, StakingV2: true, TransferTrc10: true},
		func(dp *state.DynamicProperties) {
			dp.SetLatestBlockHeaderTimestamp(1_000_000)
		})
	tvm.Timestamp = 7000

	contractAddr := tcommon.Address{0x41, 0x11}
	obtainer := tcommon.Address{0x41, 0x22}
	statedb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	statedb.CreateAccount(obtainer, corepb.AccountType_Normal)
	statedb.SetNetUsage(contractAddr, usage)
	statedb.SetLatestConsumeTime(contractAddr, tvm.ResourceTime())

	stack := newStack()
	word := addressToUint256(obtainer)
	stack.push(&word)
	contract := NewContract(tcommon.Address{0x41, 0x02}, contractAddr, 0, 1_000_000)
	if _, err := opSelfDestruct(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opSelfDestruct: %v", err)
	}
	if got := statedb.GetNetUsage(obtainer); got != usage {
		t.Fatalf("obtainer folded net usage: got %d, want %d", got, usage)
	}
	if !statedb.HasSelfDestructed(contractAddr) {
		t.Fatal("contract should be self-destructed")
	}
}

// TestSelfDestructWithoutStakingV2SkipsV2Transfer verifies the Stake-2.0 gate:
// with allow_tvm_freeze_v2 off the V2 transfer must not run.
func TestSelfDestructWithoutStakingV2SkipsV2Transfer(t *testing.T) {
	const energyAmount = int64(5 * tvmTRXPrecision)
	tvm, statedb, _ := newTestTVMForCreate(t, TVMConfig{Freeze: true, TransferTrc10: true}, // StakingV2 off
		func(dp *state.DynamicProperties) {
			dp.SetLatestBlockHeaderTimestamp(1_000_000)
		})

	contractAddr := tcommon.Address{0x41, 0x11}
	obtainer := tcommon.Address{0x41, 0x22}
	statedb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	statedb.CreateAccount(obtainer, corepb.AccountType_Normal)
	statedb.AddFreezeV2(contractAddr, corepb.ResourceCode_ENERGY, energyAmount)

	stack := newStack()
	word := addressToUint256(obtainer)
	stack.push(&word)
	contract := NewContract(tcommon.Address{0x41, 0x02}, contractAddr, 0, 1_000_000)
	if _, err := opSelfDestruct(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opSelfDestruct: %v", err)
	}
	if got := statedb.GetFrozenV2Amount(obtainer, corepb.ResourceCode_ENERGY); got != 0 {
		t.Fatalf("obtainer frozen V2 with StakingV2 off: got %d, want 0", got)
	}
}

// TestSelfDestructToSelfUnderRestrictionKeepsFrozenV2 covers java suicide2's
// early return when owner == obtainer: no transfer, no clear, no deletion.
func TestSelfDestructToSelfUnderRestrictionKeepsFrozenV2(t *testing.T) {
	const energyAmount = int64(5 * tvmTRXPrecision)
	tvm, statedb, _ := newTestTVMForCreate(t, TVMConfig{Freeze: true, StakingV2: true, TransferTrc10: true, SelfdestructRestrict: true},
		func(dp *state.DynamicProperties) {
			dp.SetLatestBlockHeaderTimestamp(1_000_000)
		})

	contractAddr := tcommon.Address{0x41, 0x11}
	statedb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	statedb.AddBalance(contractAddr, energyAmount)
	selfFreezeV2(t, tvm, contractAddr, corepb.ResourceCode_ENERGY, energyAmount)

	stack := newStack()
	word := addressToUint256(contractAddr) // beneficiary == owner
	stack.push(&word)
	contract := NewContract(tcommon.Address{0x41, 0x02}, contractAddr, 0, 1_000_000)
	if _, err := opSelfDestruct(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opSelfDestruct: %v", err)
	}
	if statedb.HasSelfDestructed(contractAddr) {
		t.Fatal("self-obtainer under restriction must not be deleted")
	}
	if got := statedb.GetFrozenV2Amount(contractAddr, corepb.ResourceCode_ENERGY); got != energyAmount {
		t.Fatalf("owner frozen V2 energy must be untouched: got %d, want %d", got, energyAmount)
	}
}

// TestSelfDestructToSelfMovesFrozenV2ToBlackhole covers the old-suicide
// owner == obtainer branch for V2: java routes the inheritor to the blackhole
// (FastByteComparisons.isEqual), so the contract's FrozenV2 balance moves there
// before the account is deleted.
func TestSelfDestructToSelfMovesFrozenV2ToBlackhole(t *testing.T) {
	const energyAmount = int64(4 * tvmTRXPrecision)
	tvm, statedb, _ := newTestTVMForCreate(t, TVMConfig{Freeze: true, StakingV2: true, TransferTrc10: true},
		func(dp *state.DynamicProperties) {
			dp.SetLatestBlockHeaderTimestamp(1_000_000)
		})
	blackhole := tcommon.Address{0x41, 0xbb}
	tvm.SetBlackholeAddress(blackhole)
	statedb.CreateAccount(blackhole, corepb.AccountType_Normal)

	contractAddr := tcommon.Address{0x41, 0x11}
	statedb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	statedb.AddBalance(contractAddr, energyAmount)
	selfFreezeV2(t, tvm, contractAddr, corepb.ResourceCode_ENERGY, energyAmount)

	stack := newStack()
	word := addressToUint256(contractAddr) // beneficiary == owner
	stack.push(&word)
	contract := NewContract(tcommon.Address{0x41, 0x02}, contractAddr, 0, 1_000_000)
	if _, err := opSelfDestruct(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opSelfDestruct: %v", err)
	}
	if got := statedb.GetFrozenV2Amount(blackhole, corepb.ResourceCode_ENERGY); got != energyAmount {
		t.Fatalf("blackhole frozen V2 energy: got %d, want %d", got, energyAmount)
	}
	if !statedb.HasSelfDestructed(contractAddr) {
		t.Fatal("contract should be self-destructed")
	}
}

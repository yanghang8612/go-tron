package vm

import (
	"errors"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// newStakeParityTVM builds a TVM wired for the Stake-2.0 VM opcodes
// (DELEGATERESOURCE / UNDELEGATERESOURCE / CANCELALLUNFREEZEV2). DynProps is
// seeded so SupportUnfreezeDelay()/SupportCancelAllUnfreezeV2() return true,
// matching the java VM native-contract gate (proposal #70/#71).
func newStakeParityTVM(t *testing.T) (*TVM, *state.StateDB, *state.DynamicProperties) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	statedb, err := state.New(tcommon.Hash{}, sdb)
	if err != nil {
		t.Fatal(err)
	}
	dp := state.NewDynamicProperties()
	dp.SetUnfreezeDelayDays(14)
	dp.SetAllowCancelAllUnfreezeV2(true)
	statedb.SetDynamicProperties(dp)
	tvm := NewTVM(statedb, dp, tcommon.Address{}, 1, 1_000_000, tcommon.Address{}, 1,
		TVMConfig{StakingV2: true})
	tvm.SetDB(diskdb)
	return tvm, statedb, dp
}

func stakeAddr(last byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = last
	return addr
}

func addressWord(addr tcommon.Address) uint256.Int {
	var w uint256.Int
	w.SetBytes(addr[1:]) // drop the 0x41 prefix; uint256ToAddress re-adds it
	return w
}

// callDelegateResource invokes opDelegateResource with the java stack layout:
// pop order is resourceType, amount, receiver, so push receiver, amount,
// resource (resource on top).
func callDelegateResource(t *testing.T, tvm *TVM, owner, receiver tcommon.Address, resource corepb.ResourceCode, amount int64) uint64 {
	t.Helper()
	stack := newStack()
	recv := addressWord(receiver)
	stack.push(&recv)
	stack.push(uint256.NewInt(uint64(amount)))
	stack.push(uint256.NewInt(uint64(resource)))
	contract := NewContract(owner, owner, 0, 1_000_000)
	if _, err := opDelegateResource(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opDelegateResource error: %v", err)
	}
	ret := stack.pop()
	return ret.Uint64()
}

func callUnDelegateResource(t *testing.T, tvm *TVM, owner, receiver tcommon.Address, resource corepb.ResourceCode, amount int64) uint64 {
	t.Helper()
	stack := newStack()
	recv := addressWord(receiver)
	stack.push(&recv)
	stack.push(uint256.NewInt(uint64(amount)))
	stack.push(uint256.NewInt(uint64(resource)))
	contract := NewContract(owner, owner, 0, 1_000_000)
	if _, err := opUnDelegateResource(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opUnDelegateResource error: %v", err)
	}
	ret := stack.pop()
	return ret.Uint64()
}

// TestDelegateOpcodeWritesPerPairRecordAndIndex locks the F-1 fix: the VM
// DELEGATERESOURCE opcode must persist the per-pair DelegatedResourceV2 record
// and the owner's delegation index, identical to java-tron
// DelegateResourceProcessor.delegateResource (writes createDbKeyV2(owner,
// receiver, false) + the FROM/TO index) and to go-tron's actuator
// DelegateResourceActuator.Execute. Before the fix opDelegateResource only
// adjusted the aggregate delegated balance and left the per-pair record absent.
func TestDelegateOpcodeWritesPerPairRecordAndIndex(t *testing.T) {
	tvm, statedb, _ := newStakeParityTVM(t)
	owner := stakeAddr(0x01)
	receiver := stakeAddr(0x02)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(receiver, corepb.AccountType_Normal)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_ENERGY, 100*tvmTRXPrecision)

	if ret := callDelegateResource(t, tvm, owner, receiver, corepb.ResourceCode_ENERGY, 40*tvmTRXPrecision); ret != 1 {
		t.Fatalf("delegate result: got %d, want 1", ret)
	}

	// Aggregate balances move (already worked before the fix).
	if got := statedb.GetFrozenV2Amount(owner, corepb.ResourceCode_ENERGY); got != 60*tvmTRXPrecision {
		t.Fatalf("owner remaining frozen: got %d, want %d", got, 60*tvmTRXPrecision)
	}
	if got := statedb.GetDelegatedFrozenV2(owner, corepb.ResourceCode_ENERGY); got != 40*tvmTRXPrecision {
		t.Fatalf("owner delegated frozen: got %d, want %d", got, 40*tvmTRXPrecision)
	}
	if got := statedb.GetAccount(receiver).AcquiredDelegatedFrozenV2BalanceForEnergy(); got != 40*tvmTRXPrecision {
		t.Fatalf("receiver acquired: got %d, want %d", got, 40*tvmTRXPrecision)
	}

	// The F-1 fix: per-pair record + index must exist (java golden:
	// DelegateResourceProcessor.delegateResource writes the unlocked record).
	dr := statedb.ReadDelegatedResourceV2(owner, receiver, false)
	if dr == nil {
		t.Fatal("per-pair DelegatedResourceV2 record missing after DELEGATERESOURCE opcode")
	}
	if dr.FrozenBalanceForEnergy != 40*tvmTRXPrecision {
		t.Fatalf("per-pair record energy: got %d, want %d", dr.FrozenBalanceForEnergy, 40*tvmTRXPrecision)
	}
	idx := statedb.ReadDelegationIndex(owner)
	if len(idx) != 1 || idx[0] != receiver {
		t.Fatalf("delegation index: got %v, want [%x]", idx, receiver)
	}
}

// TestDelegateResourceRejectsInvalidReceiver pins java DelegateResourceProcessor.validate:
// DELEGATERESOURCE must push 0 (→ contract revert) when the receiver is the owner,
// does not exist, or is a contract. go lacked these checks — Nile 34,212,851 delegated
// to a non-existent account and returned SUCCESS where java REVERTs.
func TestDelegateResourceRejectsInvalidReceiver(t *testing.T) {
	owner := stakeAddr(0x21)
	setup := func(t *testing.T) (*TVM, *state.StateDB) {
		tvm, statedb, _ := newStakeParityTVM(t)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.AddFreezeV2(owner, corepb.ResourceCode_ENERGY, 100*tvmTRXPrecision)
		return tvm, statedb
	}

	t.Run("valid_existing_eoa_succeeds", func(t *testing.T) {
		tvm, statedb := setup(t)
		recv := stakeAddr(0x22)
		statedb.CreateAccount(recv, corepb.AccountType_Normal)
		if ret := callDelegateResource(t, tvm, owner, recv, corepb.ResourceCode_ENERGY, 40*tvmTRXPrecision); ret != 1 {
			t.Fatalf("valid existing receiver: got %d, want 1", ret)
		}
	})

	t.Run("nonexistent_receiver_rejected", func(t *testing.T) {
		tvm, statedb := setup(t)
		recv := stakeAddr(0x23) // never created
		if ret := callDelegateResource(t, tvm, owner, recv, corepb.ResourceCode_ENERGY, 40*tvmTRXPrecision); ret != 0 {
			t.Fatalf("non-existent receiver: got %d, want 0", ret)
		}
		if got := statedb.GetFrozenV2Amount(owner, corepb.ResourceCode_ENERGY); got != 100*tvmTRXPrecision {
			t.Fatalf("frozen mutated on rejected delegate: got %d, want %d", got, 100*tvmTRXPrecision)
		}
		if statedb.ReadDelegatedResourceV2(owner, recv, false) != nil {
			t.Fatal("per-pair record written on rejected delegate")
		}
	})

	t.Run("self_receiver_rejected", func(t *testing.T) {
		tvm, _ := setup(t)
		if ret := callDelegateResource(t, tvm, owner, owner, corepb.ResourceCode_ENERGY, 40*tvmTRXPrecision); ret != 0 {
			t.Fatalf("self receiver: got %d, want 0", ret)
		}
	})

	t.Run("contract_receiver_rejected", func(t *testing.T) {
		tvm, statedb := setup(t)
		recv := stakeAddr(0x24)
		statedb.CreateAccount(recv, corepb.AccountType_Contract)
		if ret := callDelegateResource(t, tvm, owner, recv, corepb.ResourceCode_ENERGY, 40*tvmTRXPrecision); ret != 0 {
			t.Fatalf("contract receiver: got %d, want 0", ret)
		}
	})
}

// TestDelegateResourceUsesUsageAdjustedAvailable pins java DelegateResourceProcessor.validate:
// delegate gates on the USAGE-ADJUSTED available (frozenV2 − v2Usage, == the
// getDelegatableResource precompile), NOT the raw frozen. go previously compared
// against raw frozen, so a contract that had consumed energy could over-delegate.
func TestDelegateResourceUsesUsageAdjustedAvailable(t *testing.T) {
	owner := stakeAddr(0x31)
	recv := stakeAddr(0x32)
	const frozen = int64(100) * tvmTRXPrecision
	setup := func(t *testing.T) (*TVM, int64) {
		tvm, statedb, dp := newStakeParityTVM(t)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.CreateAccount(recv, corepb.AccountType_Normal)
		statedb.AddFreezeV2(owner, corepb.ResourceCode_ENERGY, frozen)
		// Fresh (unrecovered) energy usage reduces the delegatable below raw frozen:
		// weight==limit and usage 60 → balance 60 TRX → delegatable 40 TRX.
		statedb.SetEnergyUsage(owner, 60)
		statedb.SetLatestConsumeTimeForEnergy(owner, 0)
		dp.SetTotalEnergyWeight(1000)
		dp.SetTotalEnergyCurrentLimit(1000)
		return tvm, delegatableFrozenV2(tvm, owner, corepb.ResourceCode_ENERGY)
	}

	tvm, avail := setup(t)
	if avail <= tvmTRXPrecision || avail >= frozen {
		t.Fatalf("setup: delegatable=%d, want a value in (1 TRX, %d) so usage reduced it", avail, frozen)
	}
	// amount == available → success.
	if ret := callDelegateResource(t, tvm, owner, recv, corepb.ResourceCode_ENERGY, avail); ret != 1 {
		t.Fatalf("delegate exactly-available %d: got %d, want 1", avail, ret)
	}
	// amount between available and raw frozen → java REVERTs; pre-fix go allowed it.
	tvm2, avail2 := setup(t)
	over := avail2 + tvmTRXPrecision
	if ret := callDelegateResource(t, tvm2, owner, recv, corepb.ResourceCode_ENERGY, over); ret != 0 {
		t.Fatalf("delegate over-available %d (frozen=%d avail=%d): got %d, want 0", over, frozen, avail2, ret)
	}
}

// TestUnDelegateOpcodeReadsPerPairRecord locks the second half of F-1: the VM
// UNDELEGATERESOURCE opcode must validate against and decrement the per-pair
// record (not the aggregate), and remove the record + index when it hits zero.
// Mirrors java UnDelegateResourceProcessor.execute and go actuator
// UnDelegateResourceActuator.Execute.
func TestUnDelegateOpcodeReadsPerPairRecord(t *testing.T) {
	tvm, statedb, _ := newStakeParityTVM(t)
	owner := stakeAddr(0x11)
	receiver := stakeAddr(0x12)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(receiver, corepb.AccountType_Normal)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_ENERGY, 100*tvmTRXPrecision)
	callDelegateResource(t, tvm, owner, receiver, corepb.ResourceCode_ENERGY, 40*tvmTRXPrecision)

	// Partial undelegate: record decremented but still present.
	if ret := callUnDelegateResource(t, tvm, owner, receiver, corepb.ResourceCode_ENERGY, 15*tvmTRXPrecision); ret != 1 {
		t.Fatalf("partial undelegate result: got %d, want 1", ret)
	}
	dr := statedb.ReadDelegatedResourceV2(owner, receiver, false)
	if dr == nil || dr.FrozenBalanceForEnergy != 25*tvmTRXPrecision {
		t.Fatalf("per-pair record after partial undelegate: %+v, want energy=%d", dr, 25*tvmTRXPrecision)
	}
	if got := statedb.GetFrozenV2Amount(owner, corepb.ResourceCode_ENERGY); got != 75*tvmTRXPrecision {
		t.Fatalf("owner frozen after partial undelegate: got %d, want %d", got, 75*tvmTRXPrecision)
	}

	// Remaining undelegate empties the record -> record + index deleted.
	if ret := callUnDelegateResource(t, tvm, owner, receiver, corepb.ResourceCode_ENERGY, 25*tvmTRXPrecision); ret != 1 {
		t.Fatalf("final undelegate result: got %d, want 1", ret)
	}
	if dr := statedb.ReadDelegatedResourceV2(owner, receiver, false); dr != nil {
		t.Fatalf("per-pair record should be deleted at zero, got %+v", dr)
	}
	if idx := statedb.ReadDelegationIndex(owner); len(idx) != 0 {
		t.Fatalf("delegation index should be empty, got %v", idx)
	}
	if got := statedb.GetFrozenV2Amount(owner, corepb.ResourceCode_ENERGY); got != 100*tvmTRXPrecision {
		t.Fatalf("owner frozen fully restored: got %d, want %d", got, 100*tvmTRXPrecision)
	}
}

// TestUnDelegateOpcodeCrossReceiverIsolation locks java/actuator parity for the
// per-pair check: owner delegated only to receiver2, so undelegating from
// receiver1 must fail (push 0) and mutate nothing. The pre-fix code validated
// against the aggregate GetDelegatedFrozenV2(owner) which is non-zero, so it
// would have wrongly succeeded against receiver1.
func TestUnDelegateOpcodeCrossReceiverIsolation(t *testing.T) {
	tvm, statedb, _ := newStakeParityTVM(t)
	owner := stakeAddr(0x21)
	receiver1 := stakeAddr(0x22)
	receiver2 := stakeAddr(0x23)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(receiver1, corepb.AccountType_Normal)
	statedb.CreateAccount(receiver2, corepb.AccountType_Normal)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_ENERGY, 100*tvmTRXPrecision)
	callDelegateResource(t, tvm, owner, receiver2, corepb.ResourceCode_ENERGY, 40*tvmTRXPrecision)

	ownerFrozenBefore := statedb.GetFrozenV2Amount(owner, corepb.ResourceCode_ENERGY)
	ownerDelegatedBefore := statedb.GetDelegatedFrozenV2(owner, corepb.ResourceCode_ENERGY)

	// Undelegate from receiver1 (no record) must fail.
	if ret := callUnDelegateResource(t, tvm, owner, receiver1, corepb.ResourceCode_ENERGY, 10*tvmTRXPrecision); ret != 0 {
		t.Fatalf("cross-receiver undelegate should fail: got %d, want 0", ret)
	}
	if got := statedb.GetFrozenV2Amount(owner, corepb.ResourceCode_ENERGY); got != ownerFrozenBefore {
		t.Fatalf("owner frozen mutated by failed undelegate: got %d, want %d", got, ownerFrozenBefore)
	}
	if got := statedb.GetDelegatedFrozenV2(owner, corepb.ResourceCode_ENERGY); got != ownerDelegatedBefore {
		t.Fatalf("owner delegated mutated by failed undelegate: got %d, want %d", got, ownerDelegatedBefore)
	}
	// The valid receiver2 record is untouched.
	if dr := statedb.ReadDelegatedResourceV2(owner, receiver2, false); dr == nil || dr.FrozenBalanceForEnergy != 40*tvmTRXPrecision {
		t.Fatalf("receiver2 record disturbed: %+v", dr)
	}
}

// TestStakingPrecompileRejectsMalformedResourceType pins the resource-type operand
// decode to java DataWord.longValueSafe: a type word with any high byte set must
// map to neither BANDWIDTH(0) nor ENERGY(1)/POWER(2) and the precompile must return
// ZERO (java returns longTo32Bytes(0)). go previously read only the low 8 bytes
// (parseInt64FromWord), so a high-byte word with low byte 0 wrongly decoded as
// BANDWIDTH and returned the real frozenV2 balance — inflating any reader.
func TestStakingPrecompileRejectsMalformedResourceType(t *testing.T) {
	tvm, statedb, _ := newStakeParityTVM(t)
	owner := stakeAddr(0x71)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 100*tvmTRXPrecision)

	mkInput := func(typeWord func([]byte)) []byte {
		in := make([]byte, 64)
		copy(in[12:32], owner[1:]) // address in the low 20 bytes of word 0
		typeWord(in[32:64])
		return in
	}
	// Clean type 0 (BANDWIDTH) → returns the real frozenV2 bandwidth balance.
	cleanOut, _, err := (&unfreezableBalanceV2{}).Run(tvm, tcommon.Address{}, mkInput(func(w []byte) {}), 50)
	if err != nil {
		t.Fatalf("clean type: %v", err)
	}
	if got := int64FromWord(cleanOut); got != 100*tvmTRXPrecision {
		t.Fatalf("clean type 0: got %d, want %d", got, 100*tvmTRXPrecision)
	}
	// Malformed: high byte set, low byte 0. java longValueSafe → maxInt64 → ZERO.
	badOut, _, err := (&unfreezableBalanceV2{}).Run(tvm, tcommon.Address{}, mkInput(func(w []byte) { w[0] = 0xff }), 50)
	if err != nil {
		t.Fatalf("malformed type: %v", err)
	}
	if got := int64FromWord(badOut); got != 0 {
		t.Fatalf("malformed high-byte type: got %d, want 0 (java longValueSafe rejects)", got)
	}
}

// newProductionWiredStakeTVM mirrors the production block-execution wiring for
// the staking opcodes: the live DynamicProperties is passed to NewTVM (-> the dp
// every VM staking site must use) and StateDB.SetDynamicProperties is NOT called,
// so the StateDB's own dp is a DISTINCT empty genesis default. Used to prove the
// CANCELALLUNFREEZEV2 weight mutation lands on the live dp, not the empty one.
func newProductionWiredStakeTVM(t *testing.T) (*TVM, *state.StateDB, *state.DynamicProperties) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	statedb, err := state.New(tcommon.Hash{}, sdb)
	if err != nil {
		t.Fatal(err)
	}
	// Intentionally NOT calling SetDynamicProperties — mirrors production.
	dp := state.NewDynamicProperties()
	dp.SetUnfreezeDelayDays(14)
	dp.SetAllowCancelAllUnfreezeV2(true)
	tvm := NewTVM(statedb, dp, tcommon.Address{}, 1, 1_000_000, tcommon.Address{}, 1,
		TVMConfig{StakingV2: true})
	tvm.SetDB(diskdb)
	if statedb.DynamicProperties() == dp {
		t.Fatal("precondition: production StateDB dp must DIFFER from the wired tvm.DynProps")
	}
	return tvm, statedb, dp
}

// TestCancelAllUnfreezeV2OpcodeWeightHitsLiveDpAndRevertsOnSnapshot locks the L5
// fix: the refreeze weight delta from CANCELALLUNFREEZEV2 must (a) land on the
// LIVE dp (tvm.DynProps) — pre-fix it mutated the StateDB's own dp, which is the
// empty genesis default in production, so the live total_energy_weight never
// moved — and (b) be journaled so a frame revert rolls it back (same class as the
// 27,405,576 FREEZE-then-revert tw leak).
func TestCancelAllUnfreezeV2OpcodeWeightHitsLiveDpAndRevertsOnSnapshot(t *testing.T) {
	tvm, statedb, dp := newProductionWiredStakeTVM(t)
	owner := stakeAddr(0x61)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)

	const headerNow = int64(1_000_000)
	dp.SetLatestBlockHeaderTimestamp(headerNow)
	// One UNEXPIRED 200-TRX ENERGY entry -> refrozen -> weight delta +200.
	statedb.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, 200*tvmTRXPrecision, headerNow+1)

	liveBefore := dp.TotalEnergyWeight()
	stateDpBefore := statedb.DynamicProperties().TotalEnergyWeight()

	snap := statedb.Snapshot()
	stack := newStack()
	contract := NewContract(owner, owner, 0, 1_000_000)
	if _, err := opCancelAllUnfreezeV2(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opCancelAllUnfreezeV2 error: %v", err)
	}

	// (a) dp-source: the live dp moved by +200, the empty StateDB dp did NOT.
	if got := dp.TotalEnergyWeight() - liveBefore; got != 200 {
		t.Fatalf("live total_energy_weight delta: got %d, want 200", got)
	}
	if got := statedb.DynamicProperties().TotalEnergyWeight(); got != stateDpBefore {
		t.Fatalf("empty StateDB dp was mutated: %d -> %d (weight must hit the live dp)", stateDpBefore, got)
	}

	// (b) journaling: a frame revert rolls the weight delta back.
	statedb.RevertToSnapshot(snap)
	if got := dp.TotalEnergyWeight(); got != liveBefore {
		t.Fatalf("total_energy_weight not rolled back on revert: got %d, want %d", got, liveBefore)
	}
}

// TestUnDelegateOpcodeOwnerUntouchedWhenNoTransfer locks the C1 fix at the
// opcode level: java gates the owner-side unDelegateIncrease on transferUsage > 0,
// so when the receiver transferred no usage (here it never spent the delegated
// energy) the owner's energy_usage / window / consume-time are left exactly as
// they were. Pre-fix opUnDelegateResource folded unconditionally, decaying the
// owner's usage and stamping latest_consume_time_for_energy = now.
func TestUnDelegateOpcodeOwnerUntouchedWhenNoTransfer(t *testing.T) {
	tvm, statedb, _ := newStakeParityTVM(t)
	owner := stakeAddr(0x51)
	receiver := stakeAddr(0x52)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(receiver, corepb.AccountType_Normal)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_ENERGY, 100*tvmTRXPrecision)
	callDelegateResource(t, tvm, owner, receiver, corepb.ResourceCode_ENERGY, 40*tvmTRXPrecision)

	// Owner carries stale usage stamped at slot 0; the receiver never spent the
	// delegated energy, so the proportional transfer is 0.
	statedb.SetEnergyUsage(owner, 400)
	statedb.SetLatestConsumeTimeForEnergy(owner, 0)
	ownerWindowBefore := statedb.GetAccount(owner).RawEnergyWindowSize()

	// Undelegate half a recovery window later, so an (incorrect) owner fold would
	// visibly decay 400 -> 200 and move the consume time.
	tvm.HeadSlot = int64(params.WindowSizeSlots / 2)
	tvm.HasHeadSlot = true

	if ret := callUnDelegateResource(t, tvm, owner, receiver, corepb.ResourceCode_ENERGY, 15*tvmTRXPrecision); ret != 1 {
		t.Fatalf("undelegate result: got %d, want 1", ret)
	}

	if got := statedb.GetEnergyUsage(owner); got != 400 {
		t.Fatalf("owner energy usage changed on zero-transfer undelegate: got %d, want 400", got)
	}
	if got := statedb.GetLatestConsumeTimeForEnergy(owner); got != 0 {
		t.Fatalf("owner energy consume time changed on zero-transfer undelegate: got %d, want 0", got)
	}
	if got := statedb.GetAccount(owner).RawEnergyWindowSize(); got != ownerWindowBefore {
		t.Fatalf("owner energy window changed on zero-transfer undelegate: got %d, want %d", got, ownerWindowBefore)
	}
}

// TestCancelAllUnfreezeV2OpcodeWithdrawsExpiredAndUsesHeaderTimestamp locks the
// F-2 fix at the opcode level: CANCELALLUNFREEZEV2 must (1) add the EXPIRED
// total to the contract's balance, (2) refreeze the unexpired entry and bump
// the global weight, (3) push 1 on success (java OperationActions pushes
// ONE/ZERO, not the amount), and (4) split entries on
// DynProps.LatestBlockHeaderTimestamp() — the java getLatestBlockHeaderTimestamp
// source — NOT tvm.Timestamp.
//
// To prove the `now` source, LatestBlockHeaderTimestamp and tvm.Timestamp are
// set to DIFFERENT values straddling the unexpired entry: under the correct
// header timestamp the 200-TRX entry is unexpired (refrozen); under the wrong
// tvm.Timestamp source it would look expired (withdrawn). The assertions only
// pass with the header-timestamp source.
func TestCancelAllUnfreezeV2OpcodeWithdrawsExpiredAndUsesHeaderTimestamp(t *testing.T) {
	tvm, statedb, dp := newStakeParityTVM(t)
	owner := stakeAddr(0x31)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 5*tvmTRXPrecision)

	const headerNow = int64(1_000_000)
	dp.SetLatestBlockHeaderTimestamp(headerNow)
	// tvm.Timestamp deliberately LATER than the unexpired entry's expiry, so a
	// buggy now=tvm.Timestamp would wrongly treat the 200-TRX entry as expired.
	tvm.Timestamp = headerNow + 100_000

	// U1 = 100 TRX ENERGY expired (<= headerNow): withdrawn to balance.
	statedb.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, 100*tvmTRXPrecision, headerNow-1)
	// U2 = 200 TRX ENERGY unexpired (> headerNow but < tvm.Timestamp): refrozen.
	statedb.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, 200*tvmTRXPrecision, headerNow+1)

	energyWeightBefore := dp.TotalEnergyWeight()

	stack := newStack()
	contract := NewContract(owner, owner, 0, 1_000_000)
	if _, err := opCancelAllUnfreezeV2(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opCancelAllUnfreezeV2 error: %v", err)
	}

	ret := stack.pop()
	if ret.Uint64() != 1 {
		t.Fatalf("opcode return: got %d, want 1 (java pushes ONE on success)", ret.Uint64())
	}
	// balance += 100 TRX (only the expired entry).
	if got := statedb.GetBalance(owner); got != 105*tvmTRXPrecision {
		t.Fatalf("balance: got %d, want %d (5 start + 100 expired)", got, 105*tvmTRXPrecision)
	}
	// 200 TRX refrozen into ENERGY (proves header-timestamp source: a buggy
	// tvm.Timestamp source would have withdrawn this too, leaving 0 frozen).
	if got := statedb.GetFrozenV2Amount(owner, corepb.ResourceCode_ENERGY); got != 200*tvmTRXPrecision {
		t.Fatalf("refrozen energy: got %d, want %d", got, 200*tvmTRXPrecision)
	}
	if got := dp.TotalEnergyWeight() - energyWeightBefore; got != 200 {
		t.Fatalf("total_energy_weight delta: got %d, want 200", got)
	}
	if got := statedb.UnfreezeV2Count(owner); got != 0 {
		t.Fatalf("unfreeze queue not cleared: got %d", got)
	}
}

// pow2 returns 2^n as a uint256.Int.
func pow2(n uint) *uint256.Int {
	return new(uint256.Int).Lsh(uint256.NewInt(1), n)
}

// TestVoteWitnessEnergyCostV2WrapsLikeDataWord locks the D-1 fix: the #81
// (EnergyAdjustment, currently-active) VOTEWITNESS energy path must mirror java
// EnergyCost.getVoteWitnessCost2, which builds the array size with
// DataWord.mul(32) then DataWord.add(32) — BOTH wrapping mod 2^256. With
// witnessCount = 2^251, java computes 2^251 * 32 = 2^256 ≡ 0, then +32 = 32, so
// memNeeded = offset + 32 (tiny) and NO OutOfMemory is thrown. The pre-fix go
// code used non-wrapping bits.Mul64 and rejected (false -> OOM), diverging.
func TestVoteWitnessEnergyCostV2WrapsLikeDataWord(t *testing.T) {
	tvm, _, _ := newStakeParityTVM(t)
	in := tvm.interpreter
	in.tvmConfig = TVMConfig{Vote: true, EnergyAdjustment: true} // v2: #81 active, not Osaka

	count := pow2(251) // 2^251 * 32 == 2^256 == 0 (mod 2^256)
	wOff := uint256.NewInt(0)
	aOff := uint256.NewInt(0)
	mem := newMemory()

	cost, needed, err := voteWitnessMemoryEnergyCost(in, mem, wOff, count, aOff, count)
	if err != nil {
		t.Fatalf("v2 wrapping should NOT OOM (2^251*32 wraps to 0, +32=32): got err %v", err)
	}
	// wrapped size = 32, memNeeded = offset(0) + 32 = 32 -> one 32-byte word.
	if needed != 32 {
		t.Fatalf("v2 needed: got %d, want 32 (wrapped size)", needed)
	}
	if want := memoryEnergyCost(32); cost != want {
		t.Fatalf("v2 cost: got %d, want %d", cost, want)
	}
}

// TestVoteWitnessEnergyCostV3DoesNotWrap locks the Osaka (#96) path against
// java EnergyCost.getVoteWitnessCost3, which uses pure BigInteger arithmetic
// (no wrapping). With witnessCount = 2^251 the array size is astronomically
// larger than the 3MB MEM_LIMIT, so it MUST throw OutOfMemory.
func TestVoteWitnessEnergyCostV3DoesNotWrap(t *testing.T) {
	tvm, _, _ := newStakeParityTVM(t)
	in := tvm.interpreter
	in.tvmConfig = TVMConfig{Vote: true, EnergyAdjustment: true, Osaka: true} // v3: Osaka active

	count := pow2(251)
	wOff := uint256.NewInt(0)
	aOff := uint256.NewInt(0)
	mem := newMemory()

	_, _, err := voteWitnessMemoryEnergyCost(in, mem, wOff, count, aOff, count)
	if !errors.Is(err, ErrOutOfMemory) {
		t.Fatalf("v3 (Osaka) must NOT wrap: 2^251*32 > 3MB, want ErrOutOfMemory, got %v", err)
	}
}

// TestVoteWitnessEnergyCostNormalUnchanged: for a normal small count the three
// fork states must produce the same memNeeded/cost they did before the fix
// (wrapping only matters for pathological inputs). count=2 at offset 0:
//   - base (neither flag): size = 2*32 = 64, memNeeded = 64.
//   - v2 / v3:             size = 2*32 + 32 = 96, memNeeded = 96.
func TestVoteWitnessEnergyCostNormalUnchanged(t *testing.T) {
	tvm, _, _ := newStakeParityTVM(t)
	in := tvm.interpreter
	count := uint256.NewInt(2)
	wOff := uint256.NewInt(0)
	aOff := uint256.NewInt(0)

	cases := []struct {
		name       string
		cfg        TVMConfig
		wantNeeded uint64
	}{
		{"base", TVMConfig{Vote: true}, 64},
		{"v2", TVMConfig{Vote: true, EnergyAdjustment: true}, 96},
		{"v3", TVMConfig{Vote: true, EnergyAdjustment: true, Osaka: true}, 96},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in.tvmConfig = tc.cfg
			cost, needed, err := voteWitnessMemoryEnergyCost(in, newMemory(), wOff, count, aOff, count)
			if err != nil {
				t.Fatalf("%s: unexpected err %v", tc.name, err)
			}
			if needed != tc.wantNeeded {
				t.Fatalf("%s needed: got %d, want %d", tc.name, needed, tc.wantNeeded)
			}
			if want := memoryEnergyCost(tc.wantNeeded); cost != want {
				t.Fatalf("%s cost: got %d, want %d", tc.name, cost, want)
			}
		})
	}
}

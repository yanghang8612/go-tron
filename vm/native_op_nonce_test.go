package vm

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/reward"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// SV-3: every TRON-native stake/vote opcode must increment tvm.Nonce exactly as
// java-tron's Program does (each native op calls increaseNonce() up front, and a
// few call it a second time when execute() returns an expired-withdrawal > 0).
// The nonce feeds TransactionUtil.generateContractAddress(rootTxId, nonce) for a
// subsequent CREATE, so a missing increment makes the CREATE address diverge from
// java within the same transaction. go previously incremented the nonce for EVM
// CALL/CREATE/SELFDESTRUCT but for NONE of the native ops — this locks the gap.
//
// Each sub-test drives one opcode and asserts the nonce delta against the java
// increaseNonce count for that op's executed branch (java Program.java line cited
// per op). It also asserts that createAddress shifts by the same delta, i.e. the
// CREATE-address parity that the nonce guards.

func newNonceTVM(t *testing.T, cfg TVMConfig) (*TVM, *state.StateDB, *state.DynamicProperties) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	statedb, err := state.New(tcommon.Hash{}, sdb)
	if err != nil {
		t.Fatal(err)
	}
	dp := state.NewDynamicProperties()
	dp.SetUnfreezeDelayDays(14)
	dp.SetLatestBlockHeaderTimestamp(1_000_000)
	dp.SetCurrentCycleNumber(10)
	dp.SetNewRewardAlgorithmEffectiveCycle(0)
	dp.SetAllowCancelAllUnfreezeV2(true)
	statedb.SetDynamicProperties(dp)
	tvm := NewTVM(statedb, dp, tcommon.Address{}, 1, 1_000_000, tcommon.Address{}, 1, cfg)
	tvm.SetDB(diskdb)
	return tvm, statedb, dp
}

func nonceAddr(last byte) tcommon.Address {
	var a tcommon.Address
	a[0] = 0x41
	a[20] = last
	return a
}

// assertNonceDelta drives fn and checks tvm.Nonce advanced by want, and that the
// CREATE address derived from the post-op nonce matches createAddress(start+want).
func assertNonceDelta(t *testing.T, tvm *TVM, want uint64, fn func()) {
	t.Helper()
	start := tvm.Nonce
	wantAddr := tvm.createAddress(start + want)
	fn()
	got := tvm.Nonce - start
	if got != want {
		t.Fatalf("nonce delta: got %d, want %d (java increaseNonce count)", got, want)
	}
	if gotAddr := tvm.createAddress(tvm.Nonce); gotAddr != wantAddr {
		t.Fatalf("post-op CREATE address mismatch: got %x, want %x", gotAddr, wantAddr)
	}
}

func TestNativeOpNonceFreeze(t *testing.T) {
	// java OperationActions.freezeAction: under allowTvmFreezeV2 it pushes 0
	// WITHOUT calling freeze() (no nonce); otherwise freeze() increaseNonce()
	// once (Program.java:1920). So pre-V2 FREEZE = +1, and the V2-gated path = +0.
	t.Run("v1_increments", func(t *testing.T) {
		tvm, statedb, _ := newNonceTVM(t, TVMConfig{Freeze: true})
		owner := nonceAddr(0x01)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.AddBalance(owner, 100*tvmTRXPrecision)
		assertNonceDelta(t, tvm, 1, func() {
			callFreeze(t, tvm, owner, owner, uint256.NewInt(uint64(10*tvmTRXPrecision)), 1)
		})
	})
	t.Run("v2_gated_no_increment", func(t *testing.T) {
		// Freeze + StakingV2: freezeAction pushes 0 without calling freeze().
		tvm, statedb, _ := newNonceTVM(t, TVMConfig{Freeze: true, StakingV2: true})
		owner := nonceAddr(0x02)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.AddBalance(owner, 100*tvmTRXPrecision)
		assertNonceDelta(t, tvm, 0, func() {
			callFreeze(t, tvm, owner, owner, uint256.NewInt(uint64(10*tvmTRXPrecision)), 1)
		})
	})
}

func TestNativeOpNonceUnfreeze(t *testing.T) {
	// java unfreeze() increaseNonce() once (Program.java:1956), unconditionally.
	tvm, statedb, _ := newNonceTVM(t, TVMConfig{Freeze: true})
	owner := nonceAddr(0x03)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	assertNonceDelta(t, tvm, 1, func() {
		stack := newStack()
		recv := addressWord(owner)
		stack.push(&recv)
		stack.push(uint256.NewInt(1)) // resourceType ENERGY
		contract := NewContract(owner, owner, 0, 1_000_000)
		if _, err := opUnfreeze(nil, tvm.interpreter, contract, nil, stack); err != nil {
			t.Fatalf("opUnfreeze: %v", err)
		}
	})
}

func TestNativeOpNonceFreezeBalanceV2(t *testing.T) {
	// java freezeBalanceV2() increaseNonce() once (Program.java:2020).
	tvm, statedb, _ := newNonceTVM(t, TVMConfig{StakingV2: true})
	owner := nonceAddr(0x04)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 100*tvmTRXPrecision)
	assertNonceDelta(t, tvm, 1, func() {
		callFreezeV2(t, tvm, owner, uint256.NewInt(uint64(10*tvmTRXPrecision)), corepb.ResourceCode_ENERGY)
	})
}

func TestNativeOpNonceUnfreezeBalanceV2(t *testing.T) {
	owner := nonceAddr(0x05)
	// java unfreezeBalanceV2(): increaseNonce() once up front (Program.java:2051),
	// PLUS a second time IFF execute() returns expireBalance > 0 (the
	// withdrawExpireUnfreezeWhileUnfreezing internalTx, Program.java:2067).
	t.Run("no_expired_plus_one", func(t *testing.T) {
		tvm, statedb, _ := newNonceTVM(t, TVMConfig{StakingV2: true})
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.AddFreezeV2(owner, corepb.ResourceCode_ENERGY, 50*tvmTRXPrecision)
		assertNonceDelta(t, tvm, 1, func() {
			callUnfreezeV2(t, tvm, owner, uint256.NewInt(uint64(10*tvmTRXPrecision)), corepb.ResourceCode_ENERGY)
		})
	})
	t.Run("expired_withdrawal_plus_two", func(t *testing.T) {
		tvm, statedb, dp := newNonceTVM(t, TVMConfig{StakingV2: true})
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.AddFreezeV2(owner, corepb.ResourceCode_ENERGY, 50*tvmTRXPrecision)
		// A previously-queued unfreeze that is already expired (<= header now): it
		// gets withdrawn during this unfreeze, so execute() returns > 0 -> +2.
		statedb.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, 7*tvmTRXPrecision, dp.LatestBlockHeaderTimestamp()-1)
		assertNonceDelta(t, tvm, 2, func() {
			callUnfreezeV2(t, tvm, owner, uint256.NewInt(uint64(10*tvmTRXPrecision)), corepb.ResourceCode_ENERGY)
		})
	})
}

func TestNativeOpNonceWithdrawExpireUnfreeze(t *testing.T) {
	// java withdrawExpireUnfreeze() increaseNonce() once (Program.java:2087).
	tvm, statedb, dp := newNonceTVM(t, TVMConfig{StakingV2: true})
	owner := nonceAddr(0x06)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, 5*tvmTRXPrecision, dp.LatestBlockHeaderTimestamp()-1)
	assertNonceDelta(t, tvm, 1, func() {
		stack := newStack()
		contract := NewContract(owner, owner, 0, 1_000_000)
		if _, err := opWithdrawExpireUnfreeze(nil, tvm.interpreter, contract, nil, stack); err != nil {
			t.Fatalf("opWithdrawExpireUnfreeze: %v", err)
		}
	})
}

func TestNativeOpNonceCancelAllUnfreezeV2(t *testing.T) {
	owner := nonceAddr(0x07)
	// java cancelAllUnfreezeV2Action(): increaseNonce() once up front
	// (Program.java:2118), PLUS a second time IFF the WITHDRAW_EXPIRE_BALANCE
	// result > 0 (Program.java:2132).
	t.Run("no_expired_plus_one", func(t *testing.T) {
		tvm, statedb, dp := newNonceTVM(t, TVMConfig{StakingV2: true})
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		// Only an UNEXPIRED entry -> refrozen, expire-withdrawal == 0 -> +1.
		statedb.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, 5*tvmTRXPrecision, dp.LatestBlockHeaderTimestamp()+10_000)
		assertNonceDelta(t, tvm, 1, func() {
			stack := newStack()
			contract := NewContract(owner, owner, 0, 1_000_000)
			if _, err := opCancelAllUnfreezeV2(nil, tvm.interpreter, contract, nil, stack); err != nil {
				t.Fatalf("opCancelAllUnfreezeV2: %v", err)
			}
		})
	})
	t.Run("expired_withdrawal_plus_two", func(t *testing.T) {
		tvm, statedb, dp := newNonceTVM(t, TVMConfig{StakingV2: true})
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		// An EXPIRED entry -> withdrawn (> 0) -> +2.
		statedb.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, 5*tvmTRXPrecision, dp.LatestBlockHeaderTimestamp()-1)
		assertNonceDelta(t, tvm, 2, func() {
			stack := newStack()
			contract := NewContract(owner, owner, 0, 1_000_000)
			if _, err := opCancelAllUnfreezeV2(nil, tvm.interpreter, contract, nil, stack); err != nil {
				t.Fatalf("opCancelAllUnfreezeV2: %v", err)
			}
		})
	})
}

func TestNativeOpNonceDelegateResource(t *testing.T) {
	// java delegateResource() increaseNonce() once (Program.java:2162).
	tvm, statedb, _ := newNonceTVM(t, TVMConfig{StakingV2: true})
	owner := nonceAddr(0x08)
	receiver := nonceAddr(0x09)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(receiver, corepb.AccountType_Normal)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_ENERGY, 100*tvmTRXPrecision)
	assertNonceDelta(t, tvm, 1, func() {
		callDelegateResource(t, tvm, owner, receiver, corepb.ResourceCode_ENERGY, 40*tvmTRXPrecision)
	})
}

func TestNativeOpNonceUnDelegateResource(t *testing.T) {
	// java unDelegateResource() increaseNonce() once (Program.java:2196).
	tvm, statedb, _ := newNonceTVM(t, TVMConfig{StakingV2: true})
	owner := nonceAddr(0x0A)
	receiver := nonceAddr(0x0B)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(receiver, corepb.AccountType_Normal)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_ENERGY, 100*tvmTRXPrecision)
	callDelegateResource(t, tvm, owner, receiver, corepb.ResourceCode_ENERGY, 40*tvmTRXPrecision)
	assertNonceDelta(t, tvm, 1, func() {
		callUnDelegateResource(t, tvm, owner, receiver, corepb.ResourceCode_ENERGY, 15*tvmTRXPrecision)
	})
}

func TestNativeOpNonceVoteWitness(t *testing.T) {
	// java voteWitness() increaseNonce() once (Program.java:2272).
	tvm, statedb, _ := newNonceTVM(t, TVMConfig{Vote: true, EnergyAdjustment: true})
	caller := nonceAddr(0x0C)
	w := nonceAddr(0x0D)
	statedb.CreateAccount(caller, corepb.AccountType_Normal)
	statedb.FreezeV1Bandwidth(caller, 100*tvmTRXPrecision, tvm.Timestamp+1)
	statedb.PutWitness(w, "w")
	mem := newMemory()
	mem.set32(0, uint256.NewInt(1))
	witnessWordAt(mem, 32, w)
	mem.set32(64, uint256.NewInt(1))
	mem.set32(96, uint256.NewInt(4))
	assertNonceDelta(t, tvm, 1, func() {
		callVoteWitness(t, tvm, caller, mem,
			uint256.NewInt(0), uint256.NewInt(1),
			uint256.NewInt(64), uint256.NewInt(1), 1_000_000)
	})
}

func TestNativeOpNonceWithdrawReward(t *testing.T) {
	// java withdrawReward() increaseNonce() once (Program.java:2332).
	tvm, statedb, _ := newNonceTVM(t, TVMConfig{Vote: true})
	caller := nonceAddr(0x0E)
	witness := nonceAddr(0x0F)
	statedb.CreateAccount(caller, corepb.AccountType_Normal)
	statedb.SetAllowance(caller, 50)
	statedb.SetVotes(caller, []*corepb.Vote{{VoteAddress: witness.Bytes(), VoteCount: 100}})
	_ = statedb.WriteBeginCycle(caller.Bytes(), 1)
	_ = statedb.WriteWitnessVI(0, witness.Bytes(), reward.DecimalOfViReward)
	_ = statedb.WriteWitnessVI(9, witness.Bytes(), reward.DecimalOfViReward)
	statedb.AddBalance(caller, 1000)
	assertNonceDelta(t, tvm, 1, func() {
		stack := newStack()
		contract := NewContract(caller, caller, 0, 100_000)
		if _, err := opWithdrawReward(nil, tvm.interpreter, contract, nil, stack); err != nil {
			t.Fatalf("opWithdrawReward: %v", err)
		}
	})
}

// TestNativeOpNonceVoteWitnessUnconditionalOnFailure locks that the up-front
// increaseNonce fires even when the op FAILS (java places increaseNonce before
// the try/validate, so a push-0 failure path still advanced the nonce). Here the
// vote fails (tronPower 0 < requested votes) yet the nonce must still be +1.
func TestNativeOpNonceVoteWitnessUnconditionalOnFailure(t *testing.T) {
	tvm, statedb, _ := newNonceTVM(t, TVMConfig{Vote: true, EnergyAdjustment: true})
	caller := nonceAddr(0x1A)
	w := nonceAddr(0x1B)
	statedb.CreateAccount(caller, corepb.AccountType_Normal)
	statedb.PutWitness(w, "w")
	// No tron power -> the sum>tronPower check fails -> push 0. Nonce still +1.
	mem := newMemory()
	mem.set32(0, uint256.NewInt(1))
	witnessWordAt(mem, 32, w)
	mem.set32(64, uint256.NewInt(1))
	mem.set32(96, uint256.NewInt(4))
	assertNonceDelta(t, tvm, 1, func() {
		got, err := callVoteWitness(t, tvm, caller, mem,
			uint256.NewInt(0), uint256.NewInt(1),
			uint256.NewInt(64), uint256.NewInt(1), 1_000_000)
		if err != nil {
			t.Fatalf("voteWitness err: %v", err)
		}
		if got != 0 {
			t.Fatalf("expected failed vote (push 0), got %d", got)
		}
	})
}

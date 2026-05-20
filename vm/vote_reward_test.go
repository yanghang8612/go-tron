package vm

import (
	"encoding/binary"
	"errors"
	"math/big"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/reward"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func newVoteRewardTVM(t *testing.T) (*TVM, *state.StateDB, *state.DynamicProperties) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	statedb, err := state.New(tcommon.Hash{}, sdb)
	if err != nil {
		t.Fatal(err)
	}
	dp := state.NewDynamicProperties()
	dp.SetCurrentCycleNumber(10)
	statedb.SetDynamicProperties(dp)
	tvm := NewTVM(statedb, dp, tcommon.Address{}, 1, 123456, tcommon.Address{}, 1, TVMConfig{Vote: true})
	tvm.SetDB(diskdb)
	return tvm, statedb, dp
}

func voteRewardAddr(last byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = last
	return addr
}

func seedPendingTVMReward(statedb *state.StateDB, tvm *TVM, voter, witness tcommon.Address, votes, allowance int64) {
	statedb.CreateAccount(voter, corepb.AccountType_Normal)
	statedb.SetAllowance(voter, allowance)
	statedb.SetVotes(voter, []*corepb.Vote{{VoteAddress: witness.Bytes(), VoteCount: votes}})
	rawdb.WriteBeginCycle(tvm.DB, voter.Bytes(), 1)
	rawdb.WriteWitnessVI(tvm.DB, 0, witness.Bytes(), new(big.Int))
	rawdb.WriteWitnessVI(tvm.DB, 9, witness.Bytes(), reward.DecimalOfViReward)
}

func int64FromWord(out []byte) int64 {
	if len(out) != 32 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(out[24:]))
}

func TestRewardBalancePrecompileQueriesCallerReward(t *testing.T) {
	tvm, statedb, _ := newVoteRewardTVM(t)
	caller := voteRewardAddr(0x01)
	witness := voteRewardAddr(0x02)
	other := voteRewardAddr(0x03)
	seedPendingTVMReward(statedb, tvm, caller, witness, 100, 50)
	statedb.CreateAccount(other, corepb.AccountType_Normal)

	out, cost, err := (&rewardBalance{}).Run(tvm, caller, []byte("ignored"), 500)
	if err != nil {
		t.Fatalf("rewardBalance error: %v", err)
	}
	if cost != 500 {
		t.Fatalf("cost: got %d, want 500", cost)
	}
	if got := int64FromWord(out); got != 150 {
		t.Fatalf("reward balance: got %d, want 150", got)
	}
	if got := statedb.GetAllowance(caller); got != 50 {
		t.Fatalf("query mutated allowance: got %d, want 50", got)
	}
	if got := rawdb.ReadBeginCycle(tvm.DB, caller.Bytes()); got != 1 {
		t.Fatalf("query mutated begin cycle: got %d, want 1", got)
	}
}

func TestWithdrawRewardOpcodeSettlesPendingReward(t *testing.T) {
	tvm, statedb, _ := newVoteRewardTVM(t)
	caller := voteRewardAddr(0x11)
	witness := voteRewardAddr(0x12)
	seedPendingTVMReward(statedb, tvm, caller, witness, 100, 50)
	statedb.AddBalance(caller, 1000)

	stack := newStack()
	contract := NewContract(caller, caller, 0, 100000)
	if _, err := opWithdrawReward(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("withdrawReward opcode error: %v", err)
	}
	gotWord := stack.pop()
	if got := gotWord.Uint64(); got != 150 {
		t.Fatalf("withdraw amount: got %d, want 150", got)
	}
	if got := statedb.GetBalance(caller); got != 1150 {
		t.Fatalf("balance: got %d, want 1150", got)
	}
	if got := statedb.GetAllowance(caller); got != 0 {
		t.Fatalf("allowance: got %d, want 0", got)
	}
	if got := statedb.GetLatestWithdrawTime(caller); got != tvm.Timestamp {
		t.Fatalf("latest withdraw: got %d, want %d", got, tvm.Timestamp)
	}
	if got := rawdb.ReadBeginCycle(tvm.DB, caller.Bytes()); got != 10 {
		t.Fatalf("begin cycle: got %d, want 10", got)
	}
}

func TestVoteWitnessOpcodeUsesJavaArrayLayoutAndSettlesReward(t *testing.T) {
	tvm, statedb, _ := newVoteRewardTVM(t)
	caller := voteRewardAddr(0x21)
	oldWitness := voteRewardAddr(0x22)
	newWitness := voteRewardAddr(0x23)
	seedPendingTVMReward(statedb, tvm, caller, oldWitness, 5, 0)
	statedb.FreezeV1Bandwidth(caller, 10*tvmTRXPrecision, tvm.Timestamp+1)
	statedb.PutWitness(newWitness, "new")

	mem := newMemory()
	mem.set32(0, uint256.NewInt(1))
	witnessWord := make([]byte, 32)
	copy(witnessWord[12:], newWitness[1:])
	mem.set(32, 32, witnessWord)
	mem.set32(64, uint256.NewInt(1))
	mem.set32(96, uint256.NewInt(7))

	stack := newStack()
	stack.push(uint256.NewInt(0))
	stack.push(uint256.NewInt(1))
	stack.push(uint256.NewInt(64))
	stack.push(uint256.NewInt(1))
	contract := NewContract(caller, caller, 0, 100000)

	if _, err := opVoteWitness(nil, tvm.interpreter, contract, mem, stack); err != nil {
		t.Fatalf("voteWitness opcode error: %v", err)
	}
	gotWord := stack.pop()
	if got := gotWord.Uint64(); got != 1 {
		t.Fatalf("voteWitness result: got %d, want 1", got)
	}
	if got := statedb.GetAllowance(caller); got != 5 {
		t.Fatalf("settled allowance: got %d, want 5", got)
	}
	votes := statedb.GetVotes(caller)
	if len(votes) != 1 || tcommon.BytesToAddress(votes[0].VoteAddress) != newWitness || votes[0].VoteCount != 7 {
		t.Fatalf("account votes: got %+v, want one vote for new witness count 7", votes)
	}
	pending := rawdb.ReadVotes(tvm.DB, caller)
	if pending == nil || len(pending.OldVotes) != 1 || len(pending.NewVotes) != 1 {
		t.Fatalf("pending votes not written correctly: %+v", pending)
	}
	if tcommon.BytesToAddress(pending.OldVotes[0].VoteAddress) != oldWitness || pending.OldVotes[0].VoteCount != 5 {
		t.Fatalf("old pending votes: got %+v", pending.OldVotes)
	}
	if tcommon.BytesToAddress(pending.NewVotes[0].VoteAddress) != newWitness || pending.NewVotes[0].VoteCount != 7 {
		t.Fatalf("new pending votes: got %+v", pending.NewVotes)
	}
}

func TestVoteWitnessOpcodeAllowsEmptyVoteListToClearVotes(t *testing.T) {
	tvm, statedb, _ := newVoteRewardTVM(t)
	caller := voteRewardAddr(0x31)
	oldWitness := voteRewardAddr(0x32)
	seedPendingTVMReward(statedb, tvm, caller, oldWitness, 5, 0)

	mem := newMemory()
	mem.set32(0, uint256.NewInt(0))
	mem.set32(32, uint256.NewInt(0))

	stack := newStack()
	stack.push(uint256.NewInt(0))
	stack.push(uint256.NewInt(0))
	stack.push(uint256.NewInt(32))
	stack.push(uint256.NewInt(0))
	contract := NewContract(caller, caller, 0, 100000)

	if _, err := opVoteWitness(nil, tvm.interpreter, contract, mem, stack); err != nil {
		t.Fatalf("voteWitness opcode error: %v", err)
	}
	gotWord := stack.pop()
	if got := gotWord.Uint64(); got != 1 {
		t.Fatalf("voteWitness result: got %d, want 1", got)
	}
	if votes := statedb.GetVotes(caller); len(votes) != 0 {
		t.Fatalf("votes should be cleared, got %+v", votes)
	}
	pending := rawdb.ReadVotes(tvm.DB, caller)
	if pending == nil || len(pending.OldVotes) != 1 || len(pending.NewVotes) != 0 {
		t.Fatalf("pending empty vote update not written correctly: %+v", pending)
	}
}

func TestVoteWitnessOpcodeMemoryEnergyCostFollowsJavaForks(t *testing.T) {
	tvm, _, _ := newVoteRewardTVM(t)
	caller := voteRewardAddr(0x41)

	run := func(cfg TVMConfig, energy uint64) error {
		tvm.interpreter.tvmConfig = cfg
		mem := newMemory()
		stack := newStack()
		stack.push(uint256.NewInt(0))
		stack.push(uint256.NewInt(0))
		stack.push(uint256.NewInt(32))
		stack.push(uint256.NewInt(0))
		contract := NewContract(caller, caller, 0, energy)
		_, err := opVoteWitness(nil, tvm.interpreter, contract, mem, stack)
		return err
	}

	if err := run(TVMConfig{Vote: true}, 0); err != errVoteWitnessMemoryLength {
		t.Fatalf("legacy zero-length arrays should not charge dynamic-array length word, got %v", err)
	}
	if err := run(TVMConfig{Vote: true, EnergyAdjustment: true}, 5); err != ErrOutOfEnergy {
		t.Fatalf("energy-adjusted zero-length arrays should charge 64 bytes of memory, got %v", err)
	}
	if err := run(TVMConfig{Vote: true, Osaka: true}, 5); err != ErrOutOfEnergy {
		t.Fatalf("Osaka zero-length arrays should charge 64 bytes of memory, got %v", err)
	}
}

func TestVoteWitnessOpcodeMemoryLimit(t *testing.T) {
	tvm, _, _ := newVoteRewardTVM(t)
	caller := voteRewardAddr(0x42)
	tvm.interpreter.tvmConfig = TVMConfig{Vote: true, EnergyAdjustment: true}

	mem := newMemory()
	stack := newStack()
	stack.push(uint256.NewInt(3*1024*1024 - 31))
	stack.push(uint256.NewInt(0))
	stack.push(uint256.NewInt(0))
	stack.push(uint256.NewInt(0))
	contract := NewContract(caller, caller, 0, 100_000_000)

	_, err := opVoteWitness(nil, tvm.interpreter, contract, mem, stack)
	if !errors.Is(err, ErrOutOfMemory) {
		t.Fatalf("voteWitness memory limit error: got %v, want ErrOutOfMemory", err)
	}
	want := "Out of Memory when 'VOTEWITNESS' operation executing"
	if got := err.Error(); got != want {
		t.Fatalf("error message: got %q, want %q", got, want)
	}
}

func TestVoteWitnessOpcodeExpandsMemoryAtLimit(t *testing.T) {
	tvm, _, _ := newVoteRewardTVM(t)
	caller := voteRewardAddr(0x43)
	tvm.interpreter.tvmConfig = TVMConfig{Vote: true, EnergyAdjustment: true}

	mem := newMemory()
	stack := newStack()
	stack.push(uint256.NewInt(tvmMemoryLimit - 32))
	stack.push(uint256.NewInt(0))
	stack.push(uint256.NewInt(0))
	stack.push(uint256.NewInt(0))
	contract := NewContract(caller, caller, 0, 100_000_000)

	_, err := opVoteWitness(nil, tvm.interpreter, contract, mem, stack)
	if err != nil {
		t.Fatalf("voteWitness at memory limit: %v", err)
	}
	result := stack.pop()
	if got := result.Uint64(); got != 1 {
		t.Fatalf("voteWitness result: got %d, want 1", got)
	}
	if got := mem.len(); got != int(tvmMemoryLimit) {
		t.Fatalf("memory size: got %d, want %d", got, tvmMemoryLimit)
	}
	wantEnergy := uint64(100_000_000) - memoryEnergyCost(tvmMemoryLimit)
	if got := contract.Energy; got != wantEnergy {
		t.Fatalf("remaining energy: got %d, want %d", got, wantEnergy)
	}
}

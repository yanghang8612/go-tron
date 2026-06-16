package vm

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// newVoteParityTVM builds a Vote-enabled TVM (allow_tvm_vote, #59) with the #81
// energy-adjustment memory-cost variant active (the production path), matching
// java VMConfig.allowEnergyAdjustment()/allowTvmVote().
func newVoteParityTVM(t *testing.T) (*TVM, *state.StateDB, *state.DynamicProperties) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	statedb, err := state.New(tcommon.Hash{}, sdb)
	if err != nil {
		t.Fatal(err)
	}
	dp := state.NewDynamicProperties()
	dp.SetCurrentCycleNumber(10)
	dp.SetNewRewardAlgorithmEffectiveCycle(0)
	statedb.SetDynamicProperties(dp)
	tvm := NewTVM(statedb, dp, tcommon.Address{}, 1, 123456, tcommon.Address{}, 1,
		TVMConfig{Vote: true, EnergyAdjustment: true})
	tvm.SetDB(diskdb)
	return tvm, statedb, dp
}

func voteParityAddr(last byte) tcommon.Address {
	var a tcommon.Address
	a[0] = 0x41
	a[20] = last
	return a
}

// witnessWordAt writes a 20-byte witness address into the low bytes of a 32-byte
// word at the given memory offset (java DataWord layout: address in bytes 12..31).
func witnessWordAt(mem *Memory, off uint64, addr tcommon.Address) {
	w := make([]byte, 32)
	copy(w[12:], addr[1:])
	mem.set(off, 32, w)
}

// callVoteWitness drives opVoteWitness with the java stack layout. Stack pop
// order is amountCount, amountOffset, witnessCount, witnessOffset, so we push
// witnessOffset, witnessCount, amountOffset, amountCount (amountCount on top).
func callVoteWitness(t *testing.T, tvm *TVM, caller tcommon.Address, mem *Memory,
	witnessOffset, witnessCount, amountOffset, amountCount *uint256.Int, energy uint64) (uint64, error) {
	t.Helper()
	stack := newStack()
	stack.push(witnessOffset)
	stack.push(witnessCount)
	stack.push(amountOffset)
	stack.push(amountCount)
	contract := NewContract(caller, caller, 0, energy)
	_, err := opVoteWitness(nil, tvm.interpreter, contract, mem, stack)
	if err != nil {
		return 0, err
	}
	ret := stack.pop()
	return ret.Uint64(), nil
}

// TestVoteWitnessMergeOrderMatchesJavaHashMap is the SV-2 opcode-level gate. A
// contract votes for several DISTINCT witnesses; java VoteWitnessProcessor merges
// them through a HashMap<ByteString,Long> and appends to the account's `votes` in
// entrySet() order (VoteWitnessProcessor.java:54,105-108). The account's votes
// must therefore come out in javaHashMapOrder, NOT first-seen insertion order.
//
// The witness set is chosen so the HashMap order differs from insertion order
// (verified against the SV-2 golden corpus): insertion 0x01..0x05, HashMap order
// is a permutation. Before the fix opVoteWitness wrote insertion order.
func TestVoteWitnessMergeOrderMatchesJavaHashMap(t *testing.T) {
	tvm, statedb, _ := newVoteParityTVM(t)
	caller := voteParityAddr(0xA0)
	statedb.CreateAccount(caller, corepb.AccountType_Normal)
	// Plenty of tron power so the sum check passes.
	statedb.FreezeV1Bandwidth(caller, 1_000_000*tvmTRXPrecision, tvm.Timestamp+1)

	// Five distinct registered witnesses, last bytes 0x01..0x05.
	insertion := []tcommon.Address{
		voteParityAddr(0x01), voteParityAddr(0x02), voteParityAddr(0x03),
		voteParityAddr(0x04), voteParityAddr(0x05),
	}
	for _, w := range insertion {
		statedb.PutWitness(w, "w")
	}

	n := len(insertion)
	mem := newMemory()
	// witness array at offset 0: [len][w0][w1]...; amount array right after.
	mem.set32(0, uint256.NewInt(uint64(n)))
	for i, w := range insertion {
		witnessWordAt(mem, uint64(32+i*32), w)
	}
	aBase := uint64(32 + n*32)
	mem.set32(aBase, uint256.NewInt(uint64(n)))
	for i := 0; i < n; i++ {
		mem.set32(aBase+uint64(32+i*32), uint256.NewInt(uint64(i+1))) // counts 1..5
	}

	got, err := callVoteWitness(t, tvm, caller, mem,
		uint256.NewInt(0), uint256.NewInt(uint64(n)),
		uint256.NewInt(aBase), uint256.NewInt(uint64(n)), 1_000_000)
	if err != nil {
		t.Fatalf("voteWitness error: %v", err)
	}
	if got != 1 {
		t.Fatalf("voteWitness result: got %d, want 1", got)
	}

	want := javaHashMapOrder(insertion)
	// Sanity: this case must actually reorder, else the test has no teeth.
	if addrsEqual(insertion, want) {
		t.Fatal("test setup: chosen witnesses do not reorder under HashMap; pick others")
	}

	votes := statedb.GetVotes(caller)
	if len(votes) != n {
		t.Fatalf("vote count: got %d, want %d", len(votes), n)
	}
	for i := range want {
		gotAddr := tcommon.BytesToAddress(votes[i].VoteAddress)
		if gotAddr != want[i] {
			gotOrder := make([]tcommon.Address, len(votes))
			for j, v := range votes {
				gotOrder[j] = tcommon.BytesToAddress(v.VoteAddress)
			}
			t.Fatalf("vote order mismatch at %d:\n got =%s\n want=%s (java HashMap entrySet order)",
				i, addrsHex(gotOrder), addrsHex(want))
		}
	}
}

// TestVoteWitnessHugeCountRevertsLikeJava is the A (truncation) gate for the vote
// count word. witnessCount = 2^251 + 1 has low-64-bits == 1, so the old
// int64(word.Uint64()) truncated it to 1 — matching the memory length-word (1) —
// and would have VOTED successfully for one witness (replacing the seeded vote).
// java reads the count via DataWord.intValueSafe(), which CLAMPS any >4-byte word
// to Integer.MAX_VALUE (DataWord.java:222-228); the length-word (1) then differs
// from MAX_VALUE and java THROWS BytecodeExecutionException — a revert
// (Program.java:2276). The count is chosen so its DataWord memory-size,
// (2^251+1)*32+32 ≡ 64 (mod 2^256), stays under the 3 MB MEM_LIMIT (java
// getVoteWitnessCost2 wraps), so the energy path does NOT OOM and execution
// reaches the length-word check — isolating the truncation-vs-clamp behavior.
func TestVoteWitnessHugeCountRevertsLikeJava(t *testing.T) {
	huge := new(uint256.Int).Add(pow2(251), uint256.NewInt(1))

	// setup seeds a caller with one existing vote and a two-array memory whose
	// length words and single element are valid for a 1-vote call.
	setup := func(t *testing.T) (*TVM, *state.StateDB, tcommon.Address, *Memory) {
		tvm, statedb, _ := newVoteParityTVM(t)
		caller := voteParityAddr(0xB0)
		statedb.CreateAccount(caller, corepb.AccountType_Normal)
		statedb.FreezeV1Bandwidth(caller, 100*tvmTRXPrecision, tvm.Timestamp+1)
		existing := voteParityAddr(0x0E)
		statedb.PutWitness(existing, "e")
		statedb.SetVotes(caller, []*corepb.Vote{{VoteAddress: existing.Bytes(), VoteCount: 3}})
		mem := newMemory()
		mem.set32(0, uint256.NewInt(1)) // witness length word = 1
		witnessWordAt(mem, 32, existing)
		mem.set32(64, uint256.NewInt(1)) // amount length word = 1
		mem.set32(96, uint256.NewInt(9)) // amount[0] = 9
		return tvm, statedb, caller, mem
	}
	assertReverted := func(t *testing.T, statedb *state.StateDB, caller tcommon.Address, err error) {
		t.Helper()
		if err != errVoteWitnessMemoryLength {
			t.Fatalf("huge count must clamp to MAX_VALUE and revert on length mismatch: got err %v, want errVoteWitnessMemoryLength", err)
		}
		votes := statedb.GetVotes(caller)
		if len(votes) != 1 || votes[0].VoteCount != 3 {
			t.Fatalf("votes mutated by reverted huge-count vote: %+v", votes)
		}
	}

	// Huge witnessCount, small (matching) amountCount: isolates the witness-count
	// clamp (witness length-word 1 != MAX_VALUE -> revert). With the old
	// truncation witnessCount -> 1 == length-word 1, the witness check would pass
	// and the call would VOTE (push 1), not revert.
	t.Run("witness_count", func(t *testing.T) {
		tvm, statedb, caller, mem := setup(t)
		_, err := callVoteWitness(t, tvm, caller, mem,
			uint256.NewInt(0), huge,
			uint256.NewInt(64), uint256.NewInt(1), 100_000_000)
		assertReverted(t, statedb, caller, err)
	})

	// Huge amountCount, small (matching) witnessCount: isolates the amount-count
	// clamp (amount length-word 1 != MAX_VALUE -> revert).
	t.Run("amount_count", func(t *testing.T) {
		tvm, statedb, caller, mem := setup(t)
		_, err := callVoteWitness(t, tvm, caller, mem,
			uint256.NewInt(0), uint256.NewInt(1),
			uint256.NewInt(64), huge, 100_000_000)
		assertReverted(t, statedb, caller, err)
	})
}

// TestVoteWitnessLengthMismatchCheckedBeforeCountEquality is the SV-4 gate. java
// checks the memory length-words FIRST (revert on mismatch), and only THEN the
// witnessArrayLength == amountArrayLength equality (push 0). Construct a case
// where BOTH differ: the witness length-word in memory (2) != the witnessCount
// arg (1). java must REVERT (length check) rather than push 0 (count-equality),
// even though the counts also differ. The pre-fix go code checked count-equality
// first and would push 0.
func TestVoteWitnessLengthMismatchCheckedBeforeCountEquality(t *testing.T) {
	tvm, statedb, _ := newVoteParityTVM(t)
	caller := voteParityAddr(0xC0)
	statedb.CreateAccount(caller, corepb.AccountType_Normal)
	statedb.FreezeV1Bandwidth(caller, 100*tvmTRXPrecision, tvm.Timestamp+1)
	w := voteParityAddr(0x07)
	statedb.PutWitness(w, "w")

	mem := newMemory()
	// witness length-word in memory = 2, but witnessCount arg = 1 (mismatch) AND
	// amountCount arg = 2 (so counts also differ: 1 != 2).
	mem.set32(0, uint256.NewInt(2))
	witnessWordAt(mem, 32, w)
	witnessWordAt(mem, 64, w)
	aBase := uint64(96)
	mem.set32(aBase, uint256.NewInt(2))
	mem.set32(aBase+32, uint256.NewInt(1))
	mem.set32(aBase+64, uint256.NewInt(1))

	_, err := callVoteWitness(t, tvm, caller, mem,
		uint256.NewInt(0), uint256.NewInt(1), // witnessCount=1 (!= len-word 2)
		uint256.NewInt(aBase), uint256.NewInt(2), // amountCount=2
		100_000_000)
	if err != errVoteWitnessMemoryLength {
		t.Fatalf("length-word mismatch must revert BEFORE count-equality check: got err %v, want errVoteWitnessMemoryLength", err)
	}
}

// TestVoteWitnessNormalSmallCountUnchanged guards that the truncation/order fixes
// do not perturb the ordinary single-witness path.
func TestVoteWitnessNormalSmallCountUnchanged(t *testing.T) {
	tvm, statedb, _ := newVoteParityTVM(t)
	caller := voteParityAddr(0xD0)
	statedb.CreateAccount(caller, corepb.AccountType_Normal)
	statedb.FreezeV1Bandwidth(caller, 100*tvmTRXPrecision, tvm.Timestamp+1)
	w := voteParityAddr(0x09)
	statedb.PutWitness(w, "w")

	mem := newMemory()
	mem.set32(0, uint256.NewInt(1))
	witnessWordAt(mem, 32, w)
	mem.set32(64, uint256.NewInt(1))
	mem.set32(96, uint256.NewInt(4))

	got, err := callVoteWitness(t, tvm, caller, mem,
		uint256.NewInt(0), uint256.NewInt(1),
		uint256.NewInt(64), uint256.NewInt(1), 1_000_000)
	if err != nil {
		t.Fatalf("normal vote error: %v", err)
	}
	if got != 1 {
		t.Fatalf("normal vote result: got %d, want 1", got)
	}
	votes := statedb.GetVotes(caller)
	if len(votes) != 1 || tcommon.BytesToAddress(votes[0].VoteAddress) != w || votes[0].VoteCount != 4 {
		t.Fatalf("normal vote state: got %+v, want one vote for %x count 4", votes, w)
	}
}

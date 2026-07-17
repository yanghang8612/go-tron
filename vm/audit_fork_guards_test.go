package vm

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// TestCreate2DepthCpuTimeGuardOutOfTime locks the VERSION_4_8_1_1 guard ported
// from java-tron Program.createContract2 → MUtil.checkCPUTimeForCreate2: with the
// guard active and compatibleEvm OFF (the live mainnet/Nile state), a CREATE2 at
// MAX_DEPTH must abort the whole tx with OUT_OF_TIME (ErrAlreadyTimeOut, spend-all)
// — not silently fail-and-recurse, which left CREATE2 as the only uncapped
// recursion vector on mainnet.
func TestCreate2DepthCpuTimeGuardOutOfTime(t *testing.T) {
	caller := tcommon.Address{0x41, 0x01}
	context := tcommon.Address{0x41, 0x02}

	push4 := func(s *Stack) {
		s.push(uint256.NewInt(0)) // salt (bottom)
		s.push(uint256.NewInt(0)) // size
		s.push(uint256.NewInt(0)) // offset
		s.push(uint256.NewInt(0)) // value (top)
	}

	tvm, _, _ := newTestTVMForCreate(t, TVMConfig{Constantinople: true, CpuTimeGuard: true}, nil)
	tvm.Depth = maxCallDepth + 1
	st := newStack()
	push4(st)
	if _, err := opCreate2(nil, tvm.interpreter, NewContract(caller, context, 0, 1_000_000), newMemory(), st); err != ErrAlreadyTimeOut {
		t.Fatalf("CREATE2 at MAX_DEPTH under CpuTimeGuard: got %v, want ErrAlreadyTimeOut", err)
	}

	// Without the guard (pre-VERSION_4_8_1_1) the same call must NOT OUT_OF_TIME.
	tvm2, _, _ := newTestTVMForCreate(t, TVMConfig{Constantinople: true}, nil)
	tvm2.Depth = maxCallDepth + 1
	st2 := newStack()
	push4(st2)
	if _, err := opCreate2(nil, tvm2.interpreter, NewContract(caller, context, 0, 1_000_000), newMemory(), st2); err == ErrAlreadyTimeOut {
		t.Fatal("pre-VERSION_4_8_1_1 CREATE2 at depth must not OUT_OF_TIME")
	}
}

func TestCreate2LegacyJVMStackOverflowReplayGate(t *testing.T) {
	caller := tcommon.Address{0x41, 0x01}
	context := tcommon.Address{0x41, 0x02}
	push4 := func(s *Stack) {
		s.push(uint256.NewInt(0)) // salt
		s.push(uint256.NewInt(0)) // size
		s.push(uint256.NewInt(0)) // offset
		s.push(uint256.NewInt(0)) // value
	}

	newReplay := func(cfg TVMConfig, expected corepb.Transaction_ResultContractResult) (*TVM, *Stack) {
		tvm, _, _ := newTestTVMForCreate(t, cfg, nil)
		tvm.Depth = maxCallDepth + 1
		tvm.TrustTransactionRet = true
		tvm.ExpectedContractRet = expected
		stack := newStack()
		push4(stack)
		return tvm, stack
	}

	legacy, stack := newReplay(TVMConfig{Constantinople: true}, corepb.Transaction_Result_JVM_STACK_OVER_FLOW)
	if _, err := opCreate2(nil, legacy.interpreter, NewContract(caller, context, 0, 1_000_000), newMemory(), stack); err != ErrJVMStackOverflow {
		t.Fatalf("legacy canonical replay: got %v, want ErrJVMStackOverflow", err)
	}

	// The receipt is only an oracle while replaying signed blocks. A pending
	// execution and a canonical SUCCESS receipt retain the old local behavior.
	pending, pendingStack := newReplay(TVMConfig{Constantinople: true}, corepb.Transaction_Result_JVM_STACK_OVER_FLOW)
	pending.TrustTransactionRet = false
	if _, err := opCreate2(nil, pending.interpreter, NewContract(caller, context, 0, 1_000_000), newMemory(), pendingStack); err == ErrJVMStackOverflow {
		t.Fatal("pending execution must not trust contractRet")
	}
	success, successStack := newReplay(TVMConfig{Constantinople: true}, corepb.Transaction_Result_SUCCESS)
	if _, err := opCreate2(nil, success.interpreter, NewContract(caller, context, 0, 1_000_000), newMemory(), successStack); err == ErrJVMStackOverflow {
		t.Fatal("SUCCESS receipt must not be rewritten")
	}

	guarded, guardedStack := newReplay(TVMConfig{Constantinople: true, CpuTimeGuard: true}, corepb.Transaction_Result_JVM_STACK_OVER_FLOW)
	if _, err := opCreate2(nil, guarded.interpreter, NewContract(caller, context, 0, 1_000_000), newMemory(), guardedStack); err != ErrAlreadyTimeOut {
		t.Fatalf("VERSION_4_8_1_1 precedence: got %v, want ErrAlreadyTimeOut", err)
	}
	osaka, osakaStack := newReplay(TVMConfig{Constantinople: true, CpuTimeGuard: true, Osaka: true}, corepb.Transaction_Result_JVM_STACK_OVER_FLOW)
	if _, err := opCreate2(nil, osaka.interpreter, NewContract(caller, context, 0, 1_000_000), newMemory(), osakaStack); err != nil {
		t.Fatalf("Osaka precedence: got %v, want graceful CREATE2 failure", err)
	}
}

func TestCreate2OsakaGracefulAtMaxDepth(t *testing.T) {
	caller := tcommon.Address{0x41, 0x01}
	context := tcommon.Address{0x41, 0x02}
	tvm, _, _ := newTestTVMForCreate(t, TVMConfig{Constantinople: true, CpuTimeGuard: true, Osaka: true}, nil)
	tvm.Depth = maxCallDepth + 1
	stack := newStack()
	stack.push(uint256.NewInt(0)) // salt
	stack.push(uint256.NewInt(0)) // size
	stack.push(uint256.NewInt(0)) // offset
	stack.push(uint256.NewInt(0)) // value
	if _, err := opCreate2(nil, tvm.interpreter, NewContract(caller, context, 0, 1_000_000), newMemory(), stack); err != nil {
		t.Fatalf("Osaka CREATE2 at MAX_DEPTH must push zero, got %v", err)
	}
	if got := stack.pop(); !got.IsZero() {
		t.Fatalf("Osaka CREATE2 at MAX_DEPTH pushed %x, want zero", got.Bytes32())
	}
}

// TestModExpCpuTimeGuardOutOfTime locks the VERSION_4_8_1_1 guard ported from
// java-tron PrecompiledContracts.ModExp → MUtil.checkCPUTimeForModExp: the
// degenerate input baseLen==0 && modLen==0 && expLen>1024 aborts with OUT_OF_TIME
// when the guard is active and Osaka is not (the Osaka upper-bound reject would
// otherwise short-circuit it).
func TestModExpCpuTimeGuardOutOfTime(t *testing.T) {
	input := make([]byte, 96)
	new(big.Int).SetUint64(2048).FillBytes(input[32:64]) // expLen = 2048; baseLen=modLen=0

	if _, _, _, err := (&bigModExp{cpuTimeGuard: true}).RunWithStatus(nil, tcommon.Address{}, input, 1_000_000); err != ErrAlreadyTimeOut {
		t.Fatalf("ModExp degenerate under CpuTimeGuard: got %v, want ErrAlreadyTimeOut", err)
	}
	if _, _, ok, err := (&bigModExp{}).RunWithStatus(nil, tcommon.Address{}, input, 1_000_000); err != nil || !ok {
		t.Fatalf("ModExp degenerate without guard: got ok=%v err=%v, want cheap success", ok, err)
	}
	if _, _, ok, err := (&bigModExp{osaka: true, cpuTimeGuard: true}).RunWithStatus(nil, tcommon.Address{}, input, 1_000_000); err != nil || ok {
		t.Fatalf("ModExp degenerate under Osaka: got ok=%v err=%v, want reject (ok=false, nil)", ok, err)
	}
}

// TestWriteCallReturnPrecompileTruncationGate locks the selfdestruct-restriction
// gate on precompile return-data writeback (java Program.callToPrecompiledAddress):
// pre-restriction a precompile's return — successful output or the zero-word
// failure payload — was written at FULL length (extending MSIZE past out-size);
// post-restriction it is truncated. Regular (non-precompile) returns are always
// truncated.
func TestWriteCallReturnPrecompileTruncationGate(t *testing.T) {
	ret := []byte{1, 2, 3, 4, 5, 6, 7, 8} // 8 bytes; caller requests retSz=4

	// precompile + pre-restriction → full untruncated write.
	in := &Interpreter{tvmConfig: TVMConfig{}}
	mem := newMemory()
	in.writeCallReturn(mem, true, nil, 0, 4, ret)
	if got := mem.getCopy(0, 8); !bytes.Equal(got, ret) {
		t.Fatalf("pre-restriction precompile: got %x, want full %x", got, ret)
	}

	// precompile + restriction → truncated to retSz.
	in2 := &Interpreter{tvmConfig: TVMConfig{SelfdestructRestrict: true}}
	mem2 := newMemory()
	resizeMemory(mem2, 0, 8)
	in2.writeCallReturn(mem2, true, nil, 0, 4, ret)
	if got := mem2.getCopy(0, 8); !bytes.Equal(got, []byte{1, 2, 3, 4, 0, 0, 0, 0}) {
		t.Fatalf("post-restriction precompile: got %x, want truncated to 4", got)
	}

	// non-precompile → always truncated (matches java memorySaveLimited).
	in3 := &Interpreter{tvmConfig: TVMConfig{}}
	mem3 := newMemory()
	resizeMemory(mem3, 0, 8)
	in3.writeCallReturn(mem3, false, nil, 0, 4, ret)
	if got := mem3.getCopy(0, 8); !bytes.Equal(got, []byte{1, 2, 3, 4, 0, 0, 0, 0}) {
		t.Fatalf("non-precompile: got %x, want truncated to 4", got)
	}

	// precompile FAILURE payload (java Pair.of(false, zero-word), memorySaved
	// after the failure branch): pre-restriction full-length like success…
	in4 := &Interpreter{tvmConfig: TVMConfig{}}
	mem4 := newMemory()
	resizeMemory(mem4, 0, 8)
	mem4.set(0, 8, ret)
	in4.writeCallReturn(mem4, true, errPrecompileFailure, 0, 4, make([]byte, 32))
	if got := mem4.getCopy(0, 32); !bytes.Equal(got, make([]byte, 32)) {
		t.Fatalf("pre-restriction precompile failure: got %x, want 32 zero bytes", got)
	}

	// …and post-restriction truncated to retSz (stale tail bytes survive).
	in5 := &Interpreter{tvmConfig: TVMConfig{SelfdestructRestrict: true}}
	mem5 := newMemory()
	resizeMemory(mem5, 0, 8)
	mem5.set(0, 8, ret)
	in5.writeCallReturn(mem5, true, errPrecompileFailure, 0, 4, make([]byte, 32))
	if got := mem5.getCopy(0, 8); !bytes.Equal(got, []byte{0, 0, 0, 0, 5, 6, 7, 8}) {
		t.Fatalf("post-restriction precompile failure: got %x, want zeroed head only", got)
	}
}

// TestSelfDestruct2SelfObtainerSkipsVoteCancel locks the A5 scope fix: java
// suicide2 returns on owner==obtainer BEFORE its allow_tvm_vote block, so a
// non-new contract self-destructing to ITSELF under restriction must NOT cancel
// its votes (the vote-cancel must run only for the old path or a distinct obtainer).
func TestSelfDestruct2SelfObtainerSkipsVoteCancel(t *testing.T) {
	tvm, statedb, _ := newTestTVMForCreate(t, TVMConfig{Vote: true, SelfdestructRestrict: true},
		func(dp *state.DynamicProperties) { dp.SetCurrentCycleNumber(10) })
	contractAddr := tcommon.Address{0x41, 0x11}
	witness := tcommon.Address{0x41, 0x12}
	statedb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	statedb.SetVotes(contractAddr, []*corepb.Vote{{VoteAddress: witness.Bytes(), VoteCount: 5}})
	_ = statedb.WriteBeginCycle(contractAddr.Bytes(), 11) // beginCycle>currentCycle: skip reward math

	stack := newStack()
	self := addressToUint256(contractAddr) // obtainer == owner
	stack.push(&self)
	contract := NewContract(tcommon.Address{0x41, 0x02}, contractAddr, 0, 1_000_000)
	if _, err := opSelfDestruct(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opSelfDestruct: %v", err)
	}
	if vs := statedb.GetVotes(contractAddr); len(vs) != 1 || vs[0].VoteCount != 5 {
		t.Fatalf("suicide2 self-obtainer must NOT cancel votes, got %v", vs)
	}
}

// TestTLoadTStoreAddressNamespacedAndReverts locks the EIP-1153 (Cancun) fix:
// TLOAD/TSTORE transient storage is namespaced by the executing contract's
// address (not the slot alone) and is rolled back by RevertToSnapshot with the
// frame and discarded at the transaction boundary — matching java-tron
// RepositoryImpl.transientStorage (HashBasedTable<address,key> on the per-frame
// child repository, committed on success / discarded on revert). Pre-fix
// go-tron keyed a single shared interpreter map by slot only and never reverted
// it (cross-contract collision + revert-spanning leak).
func TestTLoadTStoreAddressNamespacedAndReverts(t *testing.T) {
	tvm, statedb, _ := newTestTVMForCreate(t, TVMConfig{Cancun: true}, nil)
	in := tvm.interpreter
	addrA := tcommon.Address{0x41, 0xAA}
	addrB := tcommon.Address{0x41, 0xBB}
	cA := NewContract(tcommon.Address{0x41, 0x01}, addrA, 0, 1_000_000)
	cB := NewContract(tcommon.Address{0x41, 0x02}, addrB, 0, 1_000_000)

	tstore := func(c *Contract, key, val uint64) {
		st := newStack()
		st.push(uint256.NewInt(val)) // value (bottom)
		st.push(uint256.NewInt(key)) // slot (top, popped first)
		if _, err := opTStore(nil, in, c, nil, st); err != nil {
			t.Fatalf("opTStore: %v", err)
		}
	}
	tload := func(c *Contract, key uint64) uint64 {
		st := newStack()
		st.push(uint256.NewInt(key))
		if _, err := opTLoad(nil, in, c, nil, st); err != nil {
			t.Fatalf("opTLoad: %v", err)
		}
		v := st.pop()
		return v.Uint64()
	}

	tstore(cA, 7, 42)
	if got := tload(cA, 7); got != 42 {
		t.Fatalf("TLOAD A/7 = %d, want 42", got)
	}
	if got := tload(cB, 7); got != 0 {
		t.Fatalf("TLOAD B/7 = %d, want 0 (address-namespaced, no slot collision)", got)
	}

	// A nested frame's TSTORE is undone when that frame reverts.
	snap := statedb.Snapshot()
	tstore(cA, 7, 99)
	statedb.RevertToSnapshot(snap)
	if got := tload(cA, 7); got != 42 {
		t.Fatalf("TLOAD A/7 after revert = %d, want pre-frame 42", got)
	}

	// All transient storage is discarded at the transaction boundary.
	statedb.FinalizeTransaction()
	if got := tload(cA, 7); got != 0 {
		t.Fatalf("TLOAD A/7 after FinalizeTransaction = %d, want 0 (EIP-1153 discard)", got)
	}
}

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
// pre-restriction a successful precompile's return was written at FULL length
// (extending MSIZE past out-size); post-restriction it is truncated. Regular
// (non-precompile) returns are always truncated.
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

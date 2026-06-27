package vm

import (
	"errors"
	"testing"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
)

func callOOMAddr(last byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = last
	return addr
}

// TestCallFamilyMemoryLimitPrecedesOutOfEnergy pins java-tron's ordering for the
// CALL family: EnergyCost.getCalculateCallCost builds the full cost as
// base + calcMemEnergy(oldMemSize, in.max(out)), and calcMemEnergy runs
// checkMemorySize (the 3 MB OUT_OF_MEMORY guard) BEFORE the single
// `energyCost > energyLimitLeft` OUT_OF_ENERGY comparison. So a memory region
// larger than 3 MB must surface as OUT_OF_MEMORY even when the base call cost
// alone would already exhaust the remaining energy.
//
// Regression: Nile/mainnet block 47,893,227 tx 74d6fda7… — java returned
// OUT_OF_MEMORY, gtron returned OUT_OF_ENERGY because each CALL-family handler
// charged the base cost (which can fail with OOE) before validating the memory
// region against the 3 MB limit.
func TestCallFamilyMemoryLimitPrecedesOutOfEnergy(t *testing.T) {
	tvm, _, _ := newTestTVMForCreate(t, TVMConfig{}, nil)
	caller := callOOMAddr(0x41)

	// Memory region whose end (offset 0 + size) exceeds the 3 MB cap.
	oversize := tvmMemoryLimit + 1
	// Energy below EnergyCall (40) so the base charge alone would OOE first.
	const tinyEnergy = 10

	cases := []struct {
		name    string
		execute executionFunc
		// stack pushed bottom→top; the handler pops top first.
		stack []uint64
	}{
		{
			// pops: gas, addr, value, inOffset, inSize, retOffset, retSize
			name:    "CALL",
			execute: opCall,
			stack:   []uint64{0 /*retSize*/, 0 /*retOffset*/, oversize /*inSize*/, 0 /*inOffset*/, 0 /*value*/, 0 /*addr*/, 0 /*gas*/},
		},
		{
			name:    "CALLCODE",
			execute: opCallCode,
			stack:   []uint64{0, 0, oversize, 0, 0, 0, 0},
		},
		{
			// pops: gas, addr, inOffset, inSize, retOffset, retSize
			name:    "DELEGATECALL",
			execute: opDelegateCall,
			stack:   []uint64{0 /*retSize*/, 0 /*retOffset*/, oversize /*inSize*/, 0 /*inOffset*/, 0 /*addr*/, 0 /*gas*/},
		},
		{
			name:    "STATICCALL",
			execute: opStaticCall,
			stack:   []uint64{0, 0, oversize, 0, 0, 0},
		},
		{
			// pops: gas, addr, tokenValue, tokenId, inOffset, inSize, retOffset, retSize
			name:    "CALLTOKEN",
			execute: opCallToken,
			stack:   []uint64{0 /*retSize*/, 0 /*retOffset*/, oversize /*inSize*/, 0 /*inOffset*/, 0 /*tokenId*/, 0 /*tokenValue*/, 0 /*addr*/, 0 /*gas*/},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mem := newMemory()
			stack := newStack()
			for _, v := range tc.stack {
				stack.push(uint256.NewInt(v))
			}
			contract := NewContract(caller, caller, 0, tinyEnergy)
			_, err := tc.execute(nil, tvm.interpreter, contract, mem, stack)
			if !errors.Is(err, ErrOutOfMemory) {
				t.Fatalf("%s: got %v, want ErrOutOfMemory (3 MB guard must precede OUT_OF_ENERGY)", tc.name, err)
			}
		})
	}
}

// runOpsErr runs a bytecode program through the interpreter loop and returns the
// terminating error (the loop charges an opcode's static energyCost BEFORE the
// handler runs, so CREATE/CREATE2 — which carry a 32000 base in the jump table —
// can only exhibit the OOM-vs-OOE ordering when driven through the loop, not by
// a direct handler call).
func runOpsErr(t *testing.T, code []byte, cfg TVMConfig, energyLimit uint64) error {
	t.Helper()
	tvm, _, _ := newTestTVMForCreate(t, cfg, nil)
	contract := NewContract(callOOMAddr(0x01), callOOMAddr(0x02), 0, energyLimit)
	contract.SetCode(callOOMAddr(0x02), code)
	_, err := tvm.interpreter.Run(contract)
	return err
}

// TestStaticBaseOpsMemoryLimitPrecedesOutOfEnergy covers the ops whose base cost
// lives as a static jump-table energyCost the interpreter loop charges BEFORE the
// handler runs — CREATE/CREATE2 (32000) and VOTEWITNESS (30000). java folds that
// base into getEnergyCost alongside calcMemEnergy, whose checkMemorySize
// (OUT_OF_MEMORY) precedes the spend, so a >3 MB region with insufficient energy
// for the base alone must still surface as OUT_OF_MEMORY, not OUT_OF_ENERGY. These
// must be driven through the interpreter loop (not a direct handler call) to
// exercise the loop's static-cost charge.
func TestStaticBaseOpsMemoryLimitPrecedesOutOfEnergy(t *testing.T) {
	// size operand = 3 MB + 1 (0x300001), offset/value/salt = 0.
	const sz0, sz1, sz2 = 0x30, 0x00, 0x01

	t.Run("CREATE", func(t *testing.T) {
		// PUSH3 size; PUSH1 offset; PUSH1 value; CREATE  (pops value,offset,size)
		code := []byte{
			byte(PUSH3), sz0, sz1, sz2,
			byte(PUSH1), 0x00,
			byte(PUSH1), 0x00,
			byte(CREATE),
		}
		// Enough for the three pushes (9) but well under the 32000 CREATE base.
		if err := runOpsErr(t, code, TVMConfig{}, 100); !errors.Is(err, ErrOutOfMemory) {
			t.Fatalf("CREATE: got %v, want ErrOutOfMemory", err)
		}
	})

	t.Run("VOTEWITNESS", func(t *testing.T) {
		// VOTEWITNESS carries the 30000 VOTE_WITNESS base as a static jump-table
		// energyCost (java getVoteWitnessCost = VOTE_WITNESS + calcMemEnergy, whose
		// checkMemorySize is OOM-first). A witness array of 100000 elements needs
		// 100000*32+32 ≈ 3.2 MB > the 3 MB cap, so with <30000 energy left it must
		// be OUT_OF_MEMORY, not OUT_OF_ENERGY.
		// pops: amountCount, amountOffset, witnessCount, witnessOffset
		code := []byte{
			byte(PUSH1), 0x00, // witnessOffset (bottom)
			byte(PUSH3), 0x01, 0x86, 0xA0, // witnessCount = 100000
			byte(PUSH1), 0x00, // amountOffset
			byte(PUSH1), 0x00, // amountCount (top)
			byte(VOTEWITNESS),
		}
		cfg := TVMConfig{Vote: true, EnergyAdjustment: true}
		if err := runOpsErr(t, code, cfg, 100); !errors.Is(err, ErrOutOfMemory) {
			t.Fatalf("VOTEWITNESS: got %v, want ErrOutOfMemory", err)
		}
	})

	t.Run("CREATE2", func(t *testing.T) {
		// PUSH1 salt; PUSH3 size; PUSH1 offset; PUSH1 value; CREATE2
		code := []byte{
			byte(PUSH1), 0x00,
			byte(PUSH3), sz0, sz1, sz2,
			byte(PUSH1), 0x00,
			byte(PUSH1), 0x00,
			byte(CREATE2),
		}
		if err := runOpsErr(t, code, TVMConfig{Constantinople: true}, 100); !errors.Is(err, ErrOutOfMemory) {
			t.Fatalf("CREATE2: got %v, want ErrOutOfMemory", err)
		}
	})
}

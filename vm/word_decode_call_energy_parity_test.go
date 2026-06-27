package vm

import (
	"testing"

	"github.com/holiman/uint256"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Word-decode parity for the CALL-family forwarded-energy operand. java reads the
// requested-energy word as a FULL 256-bit DataWord and caps it with
// Program.getCallEnergy = `requested.compareTo(available) > 0 ? available : requested`
// (EnergyCost.java passes the raw callEnergyWord). gtron truncated it with
// `energyVal.Uint64()` BEFORE the min, so a word > 2^64 whose low-64 bits are small
// (e.g. 2^64+5 -> 5) forwarded only 5 energy to the child where java forwards the
// available cap. The child then OOEs in gtron but SUCCEEDs in java, flipping the
// caller's CALL result word and all downstream state.
func TestCallEnergyHighByteWordForwardsAvailable(t *testing.T) {
	evm := newTestEVM(t)
	// Child costs > 5 energy: two PUSH1 (6) then SSTORE — starved by a 5-energy
	// forward, comfortably afforded by the available cap.
	child := deployTestCode(t, evm, 0xC2, []byte{
		byte(PUSH1), 0x01, byte(PUSH1), 0x00, byte(SSTORE), byte(STOP),
	})
	owner := wordDecodeAddr(0x20)
	evm.StateDB.CreateAccount(owner, corepb.AccountType_Contract)

	run := func(gasWord *uint256.Int) uint64 {
		mem := newMemory()
		stack := newStack()
		// opCall pop order: energy, addr, value, inOffset, inSize, retOffset, retSize
		// push bottom->top reversed:
		stack.push(uint256.NewInt(0)) // retSize
		stack.push(uint256.NewInt(0)) // retOffset
		stack.push(uint256.NewInt(0)) // inSize
		stack.push(uint256.NewInt(0)) // inOffset
		stack.push(uint256.NewInt(0)) // value
		caddr := addressWord(child)
		stack.push(&caddr)  // addr
		stack.push(gasWord) // energy (top)
		contract := NewContract(owner, owner, 0, 5_000_000)
		if _, err := opCall(nil, evm.interpreter, contract, mem, stack); err != nil {
			t.Fatalf("opCall: %v", err)
		}
		ret := stack.pop()
		return ret.Uint64()
	}

	if got := run(highByteOffset(5)); got != 1 {
		t.Fatalf("CALL gas word 2^64+5: child success = %d, want 1 (java getCallEnergy forwards available, not the truncated 5)", got)
	}
	// Sanity: a genuinely tiny gas of 5 starves the child -> 0 (proves the test has teeth).
	if got := run(uint256.NewInt(5)); got != 0 {
		t.Fatalf("CALL gas 5: child success = %d, want 0 (5 energy can't pay the child)", got)
	}
	// Sanity: a normal large fit-uint64 gas also succeeds (unchanged hot path).
	if got := run(uint256.NewInt(1_000_000)); got != 1 {
		t.Fatalf("CALL gas 1e6: child success = %d, want 1", got)
	}
	_ = corepb.AccountType_Contract
}

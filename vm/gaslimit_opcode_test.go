package vm

import (
	"testing"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
)

// java-tron OperationActions.gasLimitAction always pushes DataWord.ZERO().
// gtron previously pushed the chain's TotalEnergyCurrentLimit, a non-zero
// value, which diverged any contract that folds GASLIMIT into derived
// randomness (Nile block 23,269,068 tx 2: gtron SUCCESS vs java REVERT, the
// branch flipped by a GASLIMIT-seeded keccak). Pin a non-zero energy limit so
// the pre-fix behaviour would be caught.
func TestGasLimitOpcodeReturnsZero(t *testing.T) {
	evm := newTestEVM(t)
	evm.StateDB.DynamicProperties().SetTotalEnergyCurrentLimit(50_000_000_000)

	code := []byte{
		byte(GASLIMIT),
		byte(PUSH1), 0x00,
		byte(MSTORE),
		byte(PUSH1), 0x20,
		byte(PUSH1), 0x00,
		byte(RETURN),
	}
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	ret, err := evm.interpreter.Run(contract)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ret) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(ret))
	}
	var v uint256.Int
	v.SetBytes(ret)
	if !v.IsZero() {
		t.Fatalf("GASLIMIT must be 0 to match java-tron, got %s", v.String())
	}
}

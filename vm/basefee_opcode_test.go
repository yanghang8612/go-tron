package vm

import (
	"testing"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
)

// java-tron OperationActions.baseFeeAction pushes getEnergyFee() (sun per
// energy) unconditionally once the London opcodes are enabled. gtron used to
// push 0, which diverges any contract that reads block.basefee. Found while
// auditing the block-env opcode set after the GASLIMIT divergence (Nile
// 23,269,068); fixed proactively to avoid the next same-class re-sync stall.
func TestBaseFeeOpcodeReturnsEnergyFee(t *testing.T) {
	evm := newTestEVMWithConfig(t, TVMConfig{London: true})
	evm.StateDB.DynamicProperties().Set("energy_fee", 420)

	code := []byte{
		byte(BASEFEE),
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
	if v.Uint64() != 420 {
		t.Fatalf("BASEFEE must equal the chain energy fee (420), got %s", v.String())
	}
}

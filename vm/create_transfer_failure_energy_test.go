package vm

import (
	"testing"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Mainnet block 12,227,725 tx 7c2105f8... creates a contract whose
// constructor raises TransferException from an out-of-range CALL endowment.
// Program.createContractImpl catches that constructor exception and pushes 0,
// but returns before refundEnergyAfterVM: the parent therefore receives none
// of the constructor's remaining energy. Its next DUP1 exhausts the transaction
// with OUT_OF_ENERGY instead of reaching the later REVERT.
func TestCreateConstructorTransferFailureDoesNotRefundParentEnergy(t *testing.T) {
	evm := newTestEVMWithConfig(t, TVMConfig{Constantinople: true})
	owner := tcommon.Address{0x41, 0x01}
	evm.StateDB.CreateAccount(owner, corepb.AccountType_Contract)

	initCode := endowmentOverflowCallee()
	memory := newMemory()
	memory.set(0, uint64(len(initCode)), initCode)
	stack := newStack()
	stack.push(uint256.NewInt(uint64(len(initCode)))) // size (bottom)
	stack.push(uint256.NewInt(0))                     // offset
	stack.push(uint256.NewInt(0))                     // value (top)

	const energyLimit = uint64(5_000_000)
	contract := NewContract(owner, owner, 0, energyLimit)
	if _, err := opCreate(nil, evm.interpreter, contract, memory, stack); err != nil {
		t.Fatalf("CREATE must catch the constructor transfer failure, got %v", err)
	}
	if result := stack.pop(); !result.IsZero() {
		t.Fatalf("CREATE result = %x, want 0", result.Bytes32())
	}
	if contract.Energy != 0 {
		t.Fatalf("remaining parent energy = %d, want 0 (constructor exception must not refund)", contract.Energy)
	}
}

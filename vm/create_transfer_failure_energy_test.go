package vm

import (
	"errors"
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

// Mainnet block 12,897,681 tx 740da79d... is the top-level counterpart to
// TestCreateConstructorTransferFailureDoesNotRefundParentEnergy. Its external
// CreateSmartContract constructor surfaced TRANSFER_FAILED after spending
// 79,537 of a 500,000-energy limit. With no parent CREATE opcode to swallow the
// exception, RuntimeImpl bills only the energy actually executed.
func TestTopLevelCreateConstructorTransferFailureKeepsRemainingEnergy(t *testing.T) {
	evm := newTestEVMWithConfig(t, TVMConfig{Constantinople: true})
	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}
	evm.StateDB.CreateAccount(owner, corepb.AccountType_Normal)
	evm.StateDB.AddBalance(owner, 100)

	// The constructor transfers one sun to its own address. The top-level
	// endowment gives the new contract enough balance to reach java-tron's
	// transfer-to-self validation rather than the insufficient-balance guard.
	code := []byte{
		byte(PUSH1), 0x00, // out size
		byte(PUSH1), 0x00, // out offset
		byte(PUSH1), 0x00, // in size
		byte(PUSH1), 0x00, // in offset
		byte(PUSH1), 0x01, // value
		byte(PUSH20),
	}
	code = append(code, contractAddr[1:]...)
	code = append(code,
		byte(PUSH2), 0x08, 0xfc, // message-call energy: 2300
		byte(CALL),
		byte(STOP),
	)

	const energyLimit = uint64(100_000)
	_, _, left, err := evm.CreateAt(owner, contractAddr, code, energyLimit, 1)
	if !errors.Is(err, ErrTransferFailed) {
		t.Fatalf("top-level create error = %v, want TRANSFER_FAILED", err)
	}
	if left == 0 {
		t.Fatal("top-level create transfer failure consumed all energy")
	}
	if used := energyLimit - left; used >= 20_000 {
		t.Fatalf("top-level create transfer failure energy used = %d, want actual execution cost only", used)
	}
	if got := evm.StateDB.GetBalance(owner); got != 100 {
		t.Fatalf("owner balance after failed create = %d, want 100", got)
	}
	if evm.StateDB.AccountExists(contractAddr) {
		t.Fatal("failed top-level create left the contract account behind")
	}
}

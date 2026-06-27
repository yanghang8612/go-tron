package vm

import (
	"errors"
	"testing"

	"github.com/holiman/uint256"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// java VM.play re-throws JVMStackOverFlowException | OutOfTimeException out of a
// constructor (VM.java outer catch), and createContractImpl's VM.play is NOT in a
// try/catch, so a fatal CPU-timeout raised inside a CREATE/CREATE2 constructor
// aborts the ENTIRE tx with OUT_OF_TIME + spendAllEnergy. The CALL family already
// propagates these via shouldPropagateCallError; gtron's opCreate/opCreate2 instead
// swallowed EVERY error (push 0, return nil) and let the parent frame keep running
// with its remaining energy — divergent contractRet AND energy state.
//
// Trigger: a constructor that CALLs the ModExp precompile (0x05) with the degenerate
// baseLen==0, modLen==0, expLen=2048 input, which under VERSION_4_8_1_1
// (CpuTimeGuard) returns ErrAlreadyTimeOut.

func modexpTimeoutConstructor() []byte {
	return []byte{
		byte(PUSH2), 0x08, 0x00, // expLen = 2048 (> UPPER_BOUND 1024)
		byte(PUSH1), 0x20, // word1 offset (baseLen@0, modLen@64 stay zero)
		byte(MSTORE),
		byte(PUSH1), 0x00, // retSize
		byte(PUSH1), 0x00, // retOffset
		byte(PUSH1), 0x60, // inSize = 96 (the 3 length words)
		byte(PUSH1), 0x00, // inOffset
		byte(PUSH1), 0x00, // value
		byte(PUSH1), 0x05, // addr = modexp precompile
		byte(GAS),
		byte(CALL),
		byte(STOP),
	}
}

func TestCreateConstructorTimeoutPropagates(t *testing.T) {
	evm := newTestEVMWithConfig(t, TVMConfig{CpuTimeGuard: true})
	owner := wordDecodeAddr(0x30)
	evm.StateDB.CreateAccount(owner, corepb.AccountType_Contract)

	code := modexpTimeoutConstructor()
	mem := newMemory()
	mem.set(0, uint64(len(code)), code)
	stack := newStack()
	stack.push(uint256.NewInt(uint64(len(code)))) // size (bottom)
	stack.push(uint256.NewInt(0))                 // offset
	stack.push(uint256.NewInt(0))                 // value (top)
	contract := NewContract(owner, owner, 0, 5_000_000)

	_, err := opCreate(nil, evm.interpreter, contract, mem, stack)
	if !errors.Is(err, ErrAlreadyTimeOut) {
		pushed := stack.pop()
		t.Fatalf("CREATE constructor OutOfTime must propagate (java re-throws -> tx OUT_OF_TIME+spendAll): got err=%v, pushed=%d (buggy swallow pushes 0 and continues the parent)", err, pushed.Uint64())
	}
}

func TestCreate2ConstructorTimeoutPropagates(t *testing.T) {
	evm := newTestEVMWithConfig(t, TVMConfig{CpuTimeGuard: true})
	owner := wordDecodeAddr(0x31)
	evm.StateDB.CreateAccount(owner, corepb.AccountType_Contract)

	code := modexpTimeoutConstructor()
	mem := newMemory()
	mem.set(0, uint64(len(code)), code)
	stack := newStack()
	// opCreate2 pop order: value, offset, size, salt
	stack.push(uint256.NewInt(0))                 // salt (bottom)
	stack.push(uint256.NewInt(uint64(len(code)))) // size
	stack.push(uint256.NewInt(0))                 // offset
	stack.push(uint256.NewInt(0))                 // value (top)
	contract := NewContract(owner, owner, 0, 5_000_000)

	_, err := opCreate2(nil, evm.interpreter, contract, mem, stack)
	if !errors.Is(err, ErrAlreadyTimeOut) {
		pushed := stack.pop()
		t.Fatalf("CREATE2 constructor OutOfTime must propagate: got err=%v, pushed=%d", err, pushed.Uint64())
	}
}

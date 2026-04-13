package vm

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
)

func newTestEVM(t *testing.T) *TVM {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	return NewTVM(sdb, tcommon.Address{}, 1, 1000, tcommon.Address{}, 1, TVMConfig{})
}

func TestInterpreterAddition(t *testing.T) {
	evm := newTestEVM(t)

	// PUSH1 3 PUSH1 4 ADD PUSH1 0 MSTORE PUSH1 32 PUSH1 0 RETURN
	code := []byte{
		byte(PUSH1), 0x03,
		byte(PUSH1), 0x04,
		byte(ADD),
		byte(PUSH1), 0x00,
		byte(MSTORE),
		byte(PUSH1), 0x20,
		byte(PUSH1), 0x00,
		byte(RETURN),
	}

	caller := tcommon.Address{0x41, 0x01}
	contract := NewContract(caller, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	result, err := evm.interpreter.Run(contract)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(result))
	}
	if result[31] != 7 {
		t.Fatalf("expected 7, got %d", result[31])
	}
}

func TestInterpreterOutOfEnergy(t *testing.T) {
	evm := newTestEVM(t)

	code := []byte{byte(PUSH1), 0x01}
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 2)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	_, err := evm.interpreter.Run(contract)
	if err != ErrOutOfEnergy {
		t.Fatalf("expected ErrOutOfEnergy, got %v", err)
	}
}

func TestInterpreterInvalidJump(t *testing.T) {
	evm := newTestEVM(t)

	code := []byte{byte(PUSH1), 0x10, byte(JUMP)}
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	_, err := evm.interpreter.Run(contract)
	if err != ErrInvalidJump {
		t.Fatalf("expected ErrInvalidJump, got %v", err)
	}
}

func TestInterpreterRevert(t *testing.T) {
	evm := newTestEVM(t)

	code := []byte{byte(PUSH1), 0x00, byte(PUSH1), 0x00, byte(REVERT)}
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	_, err := evm.interpreter.Run(contract)
	if err != ErrExecutionReverted {
		t.Fatalf("expected ErrExecutionReverted, got %v", err)
	}
}

func TestInterpreterWriteProtection(t *testing.T) {
	evm := newTestEVM(t)

	// PUSH1 0 PUSH1 0 SSTORE — should fail in static mode
	code := []byte{byte(PUSH1), 0x00, byte(PUSH1), 0x00, byte(SSTORE)}
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	evm.interpreter.readOnly = true
	_, err := evm.interpreter.Run(contract)
	if err != ErrWriteProtection {
		t.Fatalf("expected ErrWriteProtection, got %v", err)
	}
	evm.interpreter.readOnly = false
}

func TestInterpreterChainIDRequiresIstanbul(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	// Istanbul NOT enabled (TVMConfig{} has all false)
	evm := NewTVM(sdb, tcommon.Address{}, 1, 1000, tcommon.Address{}, 1, TVMConfig{})

	// CHAINID PUSH1 0 MSTORE PUSH1 32 PUSH1 0 RETURN
	code := []byte{byte(CHAINID), byte(PUSH1), 0x00, byte(MSTORE), byte(PUSH1), 0x20, byte(PUSH1), 0x00, byte(RETURN)}
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	_, err = evm.interpreter.Run(contract)
	if err != ErrInvalidOpCode {
		t.Fatalf("expected ErrInvalidOpCode, got %v", err)
	}
}

func TestInterpreterChainIDWorksWithIstanbul(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	evm := NewTVM(sdb, tcommon.Address{}, 1, 1000, tcommon.Address{}, 1, TVMConfig{Istanbul: true})

	// CHAINID PUSH1 0 MSTORE PUSH1 32 PUSH1 0 RETURN
	code := []byte{byte(CHAINID), byte(PUSH1), 0x00, byte(MSTORE), byte(PUSH1), 0x20, byte(PUSH1), 0x00, byte(RETURN)}
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	_, err = evm.interpreter.Run(contract)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

package vm

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
)

func TestMakeLogCapturesEvents(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	statedb, _ := state.New(tcommon.Hash{}, db)

	contractAddr := tcommon.Address{0x41, 0x01}
	origin := tcommon.Address{0x41, 0x02}

	evm := NewTVM(statedb, origin, 1, 1000, tcommon.Address{}, 1, TVMConfig{})

	// LOG1 with topic and data "hello"
	// Store "hello" at memory[27..31] (right-aligned in 32-byte MSTORE)
	topic := make([]byte, 32)
	topic[0] = 0xAB
	topic[1] = 0xCD

	code := []byte{
		0x64,                     // PUSH5
		'h', 'e', 'l', 'l', 'o', // "hello"
		0x60, 0x00, // PUSH1 0
		0x52, // MSTORE
	}
	code = append(code, 0x7F) // PUSH32
	code = append(code, topic...)
	code = append(code,
		0x60, 0x05, // PUSH1 5  (size)
		0x60, 0x1B, // PUSH1 27 (offset = 32-5)
		0xA1, // LOG1
		0x00, // STOP
	)

	contract := NewContract(origin, contractAddr, 0, 1_000_000)
	contract.SetCode(contractAddr, code)

	_, err := evm.interpreter.Run(contract)
	if err != nil {
		t.Fatalf("execution error: %v", err)
	}

	if len(evm.Logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(evm.Logs))
	}

	log := evm.Logs[0]
	if log.Address != contractAddr {
		t.Fatalf("log address: got %x, want %x", log.Address, contractAddr)
	}
	if len(log.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(log.Topics))
	}
	if log.Topics[0][0] != 0xAB || log.Topics[0][1] != 0xCD {
		t.Fatalf("topic mismatch: got %x", log.Topics[0])
	}
	if string(log.Data) != "hello" {
		t.Fatalf("data: got %q, want %q", log.Data, "hello")
	}
}

func TestLogRevertOnSubCallFailure(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	statedb, _ := state.New(tcommon.Hash{}, db)

	caller := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}

	statedb.GetOrCreateAccount(caller)
	statedb.AddBalance(caller, 10_000_000)

	// Contract code: LOG0(0,0) then REVERT(0,0)
	code := []byte{
		0x60, 0x00, // PUSH1 0 (size)
		0x60, 0x00, // PUSH1 0 (offset)
		0xA0,       // LOG0
		0x60, 0x00, // PUSH1 0 (size)
		0x60, 0x00, // PUSH1 0 (offset)
		0xFD, // REVERT
	}
	statedb.SetCode(contractAddr, code)

	evm := NewTVM(statedb, caller, 1, 1000, tcommon.Address{}, 1, TVMConfig{})

	_, _, err := evm.Call(caller, contractAddr, nil, 1_000_000, 0)
	if err != ErrExecutionReverted {
		t.Fatalf("expected ErrExecutionReverted, got %v", err)
	}

	if len(evm.Logs) != 0 {
		t.Fatalf("expected 0 logs after revert, got %d", len(evm.Logs))
	}
}

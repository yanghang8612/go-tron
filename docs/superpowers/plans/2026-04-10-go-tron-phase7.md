# Phase 7: Transaction Lifecycle & Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make transaction execution results observable — log collection, TransactionInfo receipts, persistent storage, 13 new API endpoints for transaction building/querying/resources.

**Architecture:** EVM log events flow from `makeLog()` → `evm.Logs` → `actuator.Result` → `buildTransactionInfo()` → rawdb storage. Transaction building APIs use a shared `buildTransaction()` helper. Query APIs read from rawdb. All new endpoints go through the existing `Backend` interface + `TronBackend` implementation pattern.

**Tech Stack:** Go, protobuf (TransactionInfo/TransactionRet/ResourceReceipt), go-ethereum ethdb, SHA-256/Keccak-256

---

## File Structure

| File | Type | Responsibility |
|------|------|----------------|
| `vm/log.go` | **New** | `Log` struct |
| `vm/log_test.go` | **New** | Tests for log snapshot/revert |
| `vm/evm.go` | Modify | Add `Logs []Log`, `LogSnapshot()`, `RevertLogs()`, wire snapshot/revert into `create()`, `Call()` |
| `vm/instructions.go` | Modify | `makeLog()` captures events into `evm.Logs` |
| `actuator/actuator.go` | Modify | Extend `Result` struct with energy/net/logs/contractResult/contractAddress fields |
| `actuator/vm_actuator.go` | Modify | Populate extended `Result` fields, add `contractRetFromError()` |
| `actuator/vm_actuator_test.go` | Modify | Test extended Result fields |
| `core/bandwidth.go` | Modify | `consumeBandwidth` returns `*BandwidthResult` |
| `core/state_processor.go` | Modify | `ApplyTransaction` returns `*actuator.Result`, `ProcessBlock` returns `[]*corepb.TransactionInfo`, add `buildTransactionInfo()` |
| `core/state_processor_test.go` | Modify | Update tests for new return types, add TransactionInfo verification |
| `core/blockchain.go` | Modify | `InsertBlock` persists TransactionInfos and tx indexes |
| `core/block_builder.go` | Modify | Update `BuildBlock` for new `ApplyTransaction` return type |
| `core/rawdb/schema.go` | Modify | Add `txInfoBlockPrefix`, `txInfoKey()`, `txInfoBlockKey()` |
| `core/rawdb/accessors_txinfo.go` | **New** | Write/Read TransactionInfo, TransactionInfosByBlock, TransactionIndex |
| `core/rawdb/accessors_txinfo_test.go` | **New** | Unit tests for all rawdb txinfo accessors |
| `core/state/dynamic_properties.go` | Modify | Add `All() map[string]int64` |
| `internal/tronapi/backend.go` | Modify | Extend `Backend` interface, add `AccountResource`, `ChainParameter`, `WitnessInfo` types |
| `internal/tronapi/txbuilder.go` | **New** | `buildTransaction()` common tx builder |
| `internal/tronapi/txbuilder_test.go` | **New** | Tests for ref_block_bytes, ref_block_hash, expiration |
| `internal/tronapi/api.go` | Modify | Register 13 new route handlers + handler implementations |
| `internal/tronapi/api_test.go` | Modify | Add mock methods, test new handlers |
| `core/tron_backend.go` | Modify | Implement all new Backend methods |

---

### Task 1: EVM Log Type and Snapshot/Revert

**Files:**
- Create: `vm/log.go`
- Modify: `vm/evm.go`
- Create: `vm/log_test.go`

- [ ] **Step 1: Write the Log type test**

```go
// vm/log_test.go
package vm

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
)

func TestLogSnapshotRevert(t *testing.T) {
	evm := &EVM{}

	// Add a log
	evm.Logs = append(evm.Logs, Log{
		Address: tcommon.Address{0x41, 0x01},
		Topics:  [][]byte{{0x01}},
		Data:    []byte{0xAA},
	})
	if len(evm.Logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(evm.Logs))
	}

	// Snapshot
	snap := evm.LogSnapshot()
	if snap != 1 {
		t.Fatalf("expected snapshot 1, got %d", snap)
	}

	// Add another log
	evm.Logs = append(evm.Logs, Log{
		Address: tcommon.Address{0x41, 0x02},
		Topics:  [][]byte{{0x02}},
		Data:    []byte{0xBB},
	})
	if len(evm.Logs) != 2 {
		t.Fatalf("expected 2 logs, got %d", len(evm.Logs))
	}

	// Revert
	evm.RevertLogs(snap)
	if len(evm.Logs) != 1 {
		t.Fatalf("expected 1 log after revert, got %d", len(evm.Logs))
	}
	if evm.Logs[0].Data[0] != 0xAA {
		t.Fatal("wrong log after revert")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./vm/ -run TestLogSnapshotRevert -v`
Expected: FAIL — `Log` type undefined, `LogSnapshot`/`RevertLogs` undefined

- [ ] **Step 3: Create vm/log.go and add Logs field + methods to evm.go**

```go
// vm/log.go
package vm

import tcommon "github.com/tronprotocol/go-tron/common"

// Log represents a contract log event emitted by LOG0-LOG4.
type Log struct {
	Address tcommon.Address
	Topics  [][]byte
	Data    []byte
}
```

In `vm/evm.go`, add `Logs []Log` field to the `EVM` struct (after `Depth int`):

```go
type EVM struct {
	// ... existing fields ...
	Depth       int
	Logs        []Log // accumulated log events from this execution

	interpreter *Interpreter
}
```

Add methods:

```go
// LogSnapshot returns the current log count for later revert.
func (evm *EVM) LogSnapshot() int {
	return len(evm.Logs)
}

// RevertLogs discards logs added after the snapshot.
func (evm *EVM) RevertLogs(snapshot int) {
	evm.Logs = evm.Logs[:snapshot]
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./vm/ -run TestLogSnapshotRevert -v`
Expected: PASS

- [ ] **Step 5: Wire log snapshot/revert into EVM call methods**

In `vm/evm.go`, modify `create()` to save/revert log snapshots on error:

```go
func (evm *EVM) create(caller tcommon.Address, contractAddr tcommon.Address, code []byte, energy uint64, value int64) ([]byte, tcommon.Address, uint64, error) {
	snap := evm.StateDB.Snapshot()
	logSnap := evm.LogSnapshot()

	// ... existing GetOrCreateAccount, value transfer ...

	contract := NewContract(caller, contractAddr, value, energy)
	contract.SetCode(contractAddr, code)

	evm.Depth++
	ret, err := evm.interpreter.Run(contract)
	evm.Depth--

	if err != nil {
		evm.StateDB.RevertToSnapshot(snap)
		evm.RevertLogs(logSnap)
		if err == ErrExecutionReverted {
			return ret, tcommon.Address{}, contract.Energy, err
		}
		return nil, tcommon.Address{}, 0, err
	}

	if len(ret) > maxCodeSize {
		evm.StateDB.RevertToSnapshot(snap)
		evm.RevertLogs(logSnap)
		return nil, tcommon.Address{}, 0, ErrContractCodeTooLarge
	}

	depositCost := uint64(len(ret)) * EnergyCodeDeposit
	if !contract.UseEnergy(depositCost) {
		evm.StateDB.RevertToSnapshot(snap)
		evm.RevertLogs(logSnap)
		return nil, tcommon.Address{}, 0, ErrOutOfEnergy
	}

	evm.StateDB.SetCode(contractAddr, ret)
	return ret, contractAddr, contract.Energy, nil
}
```

Modify `Call()` similarly — add `logSnap := evm.LogSnapshot()` after `snap := evm.StateDB.Snapshot()`, and add `evm.RevertLogs(logSnap)` alongside each `evm.StateDB.RevertToSnapshot(snap)`:

```go
func (evm *EVM) Call(caller, addr tcommon.Address, input []byte, energy uint64, value int64) ([]byte, uint64, error) {
	if evm.Depth >= maxCallDepth {
		return nil, energy, ErrDepthExceeded
	}

	snap := evm.StateDB.Snapshot()
	logSnap := evm.LogSnapshot()

	// ... existing value transfer, precompile check ...

	// (For precompile errors, revert both:)
	if p, ok := precompiles[addr]; ok {
		ret, energyUsed, err := p.Run(input, energy)
		remaining := energy - energyUsed
		if err != nil {
			evm.StateDB.RevertToSnapshot(snap)
			evm.RevertLogs(logSnap)
			return nil, 0, err
		}
		return ret, remaining, nil
	}

	// ... existing code check, contract creation, interpreter.Run ...

	if err != nil {
		evm.StateDB.RevertToSnapshot(snap)
		evm.RevertLogs(logSnap)
		if err == ErrExecutionReverted {
			return ret, contract.Energy, err
		}
		return nil, 0, err
	}
	return ret, contract.Energy, nil
}
```

No changes needed for `StaticCall` and `DelegateCall` — StaticCall doesn't have state snapshots to revert (no value transfer), and DelegateCall doesn't have state snapshots either. Logs in StaticCall are collected (matching Ethereum behavior). Actually, `StaticCall` doesn't create a state snapshot at all, so no log revert is needed there. `DelegateCall` also has no snapshot. Leave them as-is.

- [ ] **Step 6: Run all VM tests**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./vm/ -v`
Expected: All PASS

- [ ] **Step 7: Commit**

```bash
git add vm/log.go vm/log_test.go vm/evm.go
git commit -m "vm: add Log type and snapshot/revert for EVM log events

Log struct captures address, topics, and data from LOG0-LOG4.
EVM.Logs accumulates events; LogSnapshot/RevertLogs handle rollback
on sub-call failure. create() and Call() wire log revert alongside
state revert."
```

---

### Task 2: Wire makeLog() to Capture Events

**Files:**
- Modify: `vm/instructions.go`
- Create: `vm/log_integration_test.go`

- [ ] **Step 1: Write the integration test**

```go
// vm/log_integration_test.go
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

	evm := NewEVM(statedb, origin, 1, 1000, tcommon.Address{}, 1)

	// LOG1 with topic=0xABCD... and data="hello"
	// PUSH5 "hello" -> PUSH1 0 -> MSTORE
	// PUSH32 topic -> PUSH1 5 -> PUSH1 27 -> LOG1
	// (store "hello" at memory[27..31], then LOG1 offset=27 size=5 topic=0xABCD...)
	topic := make([]byte, 32)
	topic[0] = 0xAB
	topic[1] = 0xCD

	code := []byte{
		// Store "hello" starting at memory offset 0
		0x64,                                     // PUSH5
		'h', 'e', 'l', 'l', 'o',                 // "hello"
		0x60, 0x00, // PUSH1 0
		0x52,       // MSTORE (stores 32 bytes at offset 0, right-aligned)
		// LOG1: offset=27, size=5, topic
	}
	// PUSH32 topic
	code = append(code, 0x7F) // PUSH32
	code = append(code, topic...)
	code = append(code,
		0x60, 0x05, // PUSH1 5  (size)
		0x60, 0x1B, // PUSH1 27 (offset, 32-5=27 for right-aligned "hello")
		0xA1,       // LOG1
		0x00,       // STOP
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

	// Contract code: LOG0(offset=0, size=0) then REVERT(0,0)
	code := []byte{
		0x60, 0x00, // PUSH1 0 (size)
		0x60, 0x00, // PUSH1 0 (offset)
		0xA0,       // LOG0
		0x60, 0x00, // PUSH1 0 (size)
		0x60, 0x00, // PUSH1 0 (offset)
		0xFD,       // REVERT
	}
	statedb.SetCode(contractAddr, code)

	evm := NewEVM(statedb, caller, 1, 1000, tcommon.Address{}, 1)

	_, _, err := evm.Call(caller, contractAddr, nil, 1_000_000, 0)
	if err != ErrExecutionReverted {
		t.Fatalf("expected ErrExecutionReverted, got %v", err)
	}

	// Logs should be reverted because the call reverted
	if len(evm.Logs) != 0 {
		t.Fatalf("expected 0 logs after revert, got %d", len(evm.Logs))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./vm/ -run TestMakeLogCapturesEvents -v`
Expected: FAIL — `makeLog()` currently discards topics/data, `evm.Logs` will be empty

- [ ] **Step 3: Update makeLog() in instructions.go**

Replace the `makeLog` function in `vm/instructions.go`:

```go
func makeLog(topicCount int) executionFunc {
	return func(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
		offset, size := stack.pop(), stack.pop()
		sz := size.Uint64()

		cost := EnergyLog + EnergyLogTopic*uint64(topicCount) + EnergyLogData*sz
		if mcost := memoryExpansionCost(memory, offset.Uint64(), sz); mcost > 0 {
			cost += mcost
		}
		if !contract.UseEnergy(cost) {
			return nil, ErrOutOfEnergy
		}
		memory.resize(offset.Uint64() + sz)

		topics := make([][]byte, topicCount)
		for i := 0; i < topicCount; i++ {
			t := stack.pop()
			b := t.Bytes32()
			topics[i] = make([]byte, 32)
			copy(topics[i], b[:])
		}

		data := memory.getCopy(int64(offset.Uint64()), int64(sz))

		interpreter.evm.Logs = append(interpreter.evm.Logs, Log{
			Address: contract.Address,
			Topics:  topics,
			Data:    data,
		})

		return nil, nil
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./vm/ -run "TestMakeLog|TestLogRevert" -v`
Expected: Both PASS

- [ ] **Step 5: Run all VM tests**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./vm/ -v`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add vm/instructions.go vm/log_integration_test.go
git commit -m "vm: wire makeLog to capture LOG0-LOG4 events into evm.Logs

Previously makeLog discarded topic and data. Now it appends a Log entry
with contract address, topics, and data to the EVM's log accumulator.
Includes integration tests for log capture and revert-on-failure."
```

---

### Task 3: Extend actuator.Result and VMActuator

**Files:**
- Modify: `actuator/actuator.go`
- Modify: `actuator/vm_actuator.go`
- Modify: `actuator/vm_actuator_test.go`

- [ ] **Step 1: Write test for extended Result fields**

Add to `actuator/vm_actuator_test.go`:

```go
func TestVMActuatorCreateExecute_ExtendedResult(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}

	// Simple runtime: PUSH1 0x42 PUSH1 0 MSTORE PUSH1 32 PUSH1 0 RETURN
	runtime := []byte{
		0x60, 0x42, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3,
	}

	runtimeLen := byte(len(runtime))
	initCode := []byte{
		0x60, runtimeLen, 0x80, 0x60, 0x00, 0x60, 0x00, 0x39,
		0x60, runtimeLen, 0x60, 0x00, 0xf3,
	}
	initCode[4] = byte(len(initCode))
	bytecode := append(initCode, runtime...)

	csc := &contractpb.CreateSmartContract{
		OwnerAddress: owner[:],
		NewContract: &contractpb.SmartContract{
			OriginAddress: owner[:],
			Bytecode:      bytecode,
			Name:          "TestContract",
		},
	}

	ctx := newTestContext(t, corepb.Transaction_Contract_CreateSmartContract, csc, 10_000_000)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, 100_000_000)

	act := &VMActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.EnergyUsed <= 0 {
		t.Fatal("expected non-zero EnergyUsed")
	}
	if result.EnergyFee <= 0 {
		t.Fatal("expected non-zero EnergyFee")
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1 (SUCCESS), got %d", result.ContractRet)
	}
	if len(result.ContractAddress) == 0 {
		t.Fatal("expected non-empty ContractAddress")
	}
	t.Logf("EnergyUsed=%d, EnergyFee=%d, ContractRet=%d, ContractAddr=%x",
		result.EnergyUsed, result.EnergyFee, result.ContractRet, result.ContractAddress)
}

func TestVMActuatorTriggerExecute_ExtendedResult(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}

	// Code: LOG0(0,0) then PUSH1 0x42 PUSH1 0 MSTORE PUSH1 32 PUSH1 0 RETURN
	code := []byte{
		0x60, 0x00, 0x60, 0x00, 0xA0, // LOG0(0,0)
		0x60, 0x42, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3, // return 0x42
	}

	tsc := &contractpb.TriggerSmartContract{
		OwnerAddress:    owner[:],
		ContractAddress: contractAddr[:],
	}

	ctx := newTestContext(t, corepb.Transaction_Contract_TriggerSmartContract, tsc, 10_000_000)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, 100_000_000)
	ctx.State.SetCode(contractAddr, code)

	act := &VMActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.EnergyUsed <= 0 {
		t.Fatal("expected non-zero EnergyUsed")
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1 (SUCCESS), got %d", result.ContractRet)
	}
	if len(result.Logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(result.Logs))
	}
	if len(result.ContractResult) != 32 {
		t.Fatalf("expected 32 bytes contract result, got %d", len(result.ContractResult))
	}
	t.Logf("EnergyUsed=%d, Logs=%d, ContractResult=%x",
		result.EnergyUsed, len(result.Logs), result.ContractResult)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./actuator/ -run "TestVMActuatorCreateExecute_Extended|TestVMActuatorTriggerExecute_Extended" -v`
Expected: FAIL — `Result` struct doesn't have `EnergyUsed`, `ContractRet`, etc.

- [ ] **Step 3: Extend Result struct in actuator.go**

In `actuator/actuator.go`, add the import for `vm` and extend `Result`:

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/vm"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Result holds the outcome of an actuator execution.
type Result struct {
	Fee               int64
	EnergyUsed        int64
	EnergyFee         int64
	OriginEnergyUsage int64
	NetUsage          int64
	NetFee            int64
	ContractResult    []byte
	ContractAddress   []byte
	Logs              []vm.Log
	ContractRet       int32
}
```

- [ ] **Step 4: Update VMActuator to populate extended Result**

In `actuator/vm_actuator.go`, add `contractRetFromError()` and update `executeCreate` and `executeTrigger`:

```go
func contractRetFromError(err error) int32 {
	switch err {
	case vm.ErrExecutionReverted:
		return 2 // REVERT
	case vm.ErrInvalidJump:
		return 3 // BAD_JUMP_DESTINATION
	case vm.ErrOutOfEnergy:
		return 10 // OUT_OF_ENERGY
	case vm.ErrStackUnderflow:
		return 6 // STACK_TOO_SMALL
	case vm.ErrStackOverflow:
		return 7 // STACK_TOO_LARGE
	case vm.ErrWriteProtection:
		return 8 // ILLEGAL_OPERATION
	case vm.ErrDepthExceeded:
		return 9 // STACK_OVERFLOW (depth exceeded maps to this)
	case vm.ErrContractCodeTooLarge:
		return 15 // INVALID_CODE
	default:
		return 13 // UNKNOWN
	}
}

func (a *VMActuator) executeCreate(ctx *Context) (*Result, error) {
	csc, err := a.getCreateContract(ctx)
	if err != nil {
		return nil, err
	}

	owner := common.BytesToAddress(csc.OwnerAddress)
	callValue := csc.NewContract.CallValue
	bytecode := csc.NewContract.Bytecode

	energyFee := ctx.DynProps.EnergyFee()
	if energyFee <= 0 {
		energyFee = 100
	}
	energyLimit := uint64(ctx.Tx.FeeLimit()) / uint64(energyFee)

	evm := vm.NewEVM(ctx.State, owner, ctx.BlockNumber, ctx.BlockTime, common.Address{}, 1)

	ret, contractAddr, energyLeft, vmErr := evm.Create(owner, bytecode, energyLimit, callValue)

	energyUsed := energyLimit - energyLeft
	fee := int64(energyUsed) * energyFee

	result := &Result{
		Fee:            fee,
		EnergyUsed:     int64(energyUsed),
		EnergyFee:      fee,
		ContractResult: ret,
		Logs:           evm.Logs,
	}

	if vmErr != nil {
		result.ContractRet = contractRetFromError(vmErr)
		return result, nil
	}

	result.ContractRet = 1 // SUCCESS
	result.ContractAddress = contractAddr[:]

	sc := csc.NewContract
	sc.ContractAddress = contractAddr[:]
	ctx.State.SetContract(contractAddr, sc)

	return result, nil
}

func (a *VMActuator) executeTrigger(ctx *Context) (*Result, error) {
	tsc, err := a.getTriggerContract(ctx)
	if err != nil {
		return nil, err
	}

	owner := common.BytesToAddress(tsc.OwnerAddress)
	contractAddr := common.BytesToAddress(tsc.ContractAddress)
	callValue := tsc.CallValue
	data := tsc.Data

	energyFee := ctx.DynProps.EnergyFee()
	if energyFee <= 0 {
		energyFee = 100
	}
	energyLimit := uint64(ctx.Tx.FeeLimit()) / uint64(energyFee)

	evm := vm.NewEVM(ctx.State, owner, ctx.BlockNumber, ctx.BlockTime, common.Address{}, 1)

	ret, energyLeft, vmErr := evm.Call(owner, contractAddr, data, energyLimit, callValue)

	energyUsed := energyLimit - energyLeft
	fee := int64(energyUsed) * energyFee

	result := &Result{
		Fee:            fee,
		EnergyUsed:     int64(energyUsed),
		EnergyFee:      fee,
		ContractResult: ret,
		Logs:           evm.Logs,
	}

	if vmErr != nil {
		result.ContractRet = contractRetFromError(vmErr)
		return result, nil
	}

	result.ContractRet = 1 // SUCCESS
	return result, nil
}
```

- [ ] **Step 5: Update non-VM actuators to set ContractRet=1 on success**

In each non-VM actuator's `Execute` method, change `return &Result{Fee: 0}, nil` to `return &Result{Fee: 0, ContractRet: 1}, nil`. This affects:
- `actuator/transfer_actuator.go`
- `actuator/create_account_actuator.go`
- `actuator/witness_create_actuator.go`
- `actuator/freeze_actuator.go`
- `actuator/unfreeze_actuator.go`
- `actuator/vote_witness_actuator.go`
- `actuator/withdraw_balance_actuator.go`
- `actuator/withdraw_expire_unfreeze_actuator.go`

For each, find `return &Result{Fee: 0}, nil` at the end of `Execute` and change to `return &Result{Fee: 0, ContractRet: 1}, nil`. Some may use `return &Result{Fee: fee}, nil` — keep the fee value, just add `ContractRet: 1`.

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./actuator/ -v`
Expected: All PASS including the new extended result tests

- [ ] **Step 7: Commit**

```bash
git add actuator/actuator.go actuator/vm_actuator.go actuator/vm_actuator_test.go actuator/transfer_actuator.go actuator/create_account_actuator.go actuator/witness_create_actuator.go actuator/freeze_actuator.go actuator/unfreeze_actuator.go actuator/vote_witness_actuator.go actuator/withdraw_balance_actuator.go actuator/withdraw_expire_unfreeze_actuator.go
git commit -m "actuator: extend Result with energy, logs, contract fields

Result now carries EnergyUsed, EnergyFee, ContractResult, ContractAddress,
Logs, and ContractRet. VMActuator populates all fields after EVM execution.
contractRetFromError maps VM errors to Transaction.Result.contractResult
enum values. Non-VM actuators set ContractRet=1 (SUCCESS) on success."
```

---

### Task 4: RawDB TransactionInfo Accessors

**Files:**
- Modify: `core/rawdb/schema.go`
- Create: `core/rawdb/accessors_txinfo.go`
- Create: `core/rawdb/accessors_txinfo_test.go`

- [ ] **Step 1: Write the tests**

```go
// core/rawdb/accessors_txinfo_test.go
package rawdb

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestWriteReadTransactionInfo(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()

	txID := bytes.Repeat([]byte{0xAB}, 32)
	info := &corepb.TransactionInfo{
		Id:             txID,
		Fee:            12345,
		BlockNumber:    100,
		BlockTimeStamp: 300000,
		Receipt: &corepb.ResourceReceipt{
			EnergyUsage:      500,
			EnergyFee:        50000,
			EnergyUsageTotal: 500,
		},
	}

	WriteTransactionInfo(db, txID, info)

	got := ReadTransactionInfo(db, txID)
	if got == nil {
		t.Fatal("expected non-nil TransactionInfo")
	}
	if got.Fee != 12345 {
		t.Fatalf("fee: got %d, want 12345", got.Fee)
	}
	if got.BlockNumber != 100 {
		t.Fatalf("blockNumber: got %d, want 100", got.BlockNumber)
	}
	if got.Receipt.EnergyUsage != 500 {
		t.Fatalf("energyUsage: got %d, want 500", got.Receipt.EnergyUsage)
	}
}

func TestReadTransactionInfo_NotFound(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	got := ReadTransactionInfo(db, bytes.Repeat([]byte{0x00}, 32))
	if got != nil {
		t.Fatal("expected nil for missing key")
	}
}

func TestWriteReadTransactionInfosByBlock(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()

	infos := []*corepb.TransactionInfo{
		{Id: bytes.Repeat([]byte{0x01}, 32), Fee: 100, BlockNumber: 5, BlockTimeStamp: 15000},
		{Id: bytes.Repeat([]byte{0x02}, 32), Fee: 200, BlockNumber: 5, BlockTimeStamp: 15000},
	}

	WriteTransactionInfosByBlock(db, 5, infos)

	got := ReadTransactionInfosByBlock(db, 5)
	if len(got) != 2 {
		t.Fatalf("expected 2 infos, got %d", len(got))
	}
	if got[0].Fee != 100 {
		t.Fatalf("info[0] fee: got %d, want 100", got[0].Fee)
	}
	if got[1].Fee != 200 {
		t.Fatalf("info[1] fee: got %d, want 200", got[1].Fee)
	}
}

func TestReadTransactionInfosByBlock_NotFound(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	got := ReadTransactionInfosByBlock(db, 999)
	if len(got) != 0 {
		t.Fatalf("expected 0 infos, got %d", len(got))
	}
}

func TestWriteReadTransactionIndex(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()

	txHash := bytes.Repeat([]byte{0xCC}, 32)
	WriteTransactionIndex(db, txHash, 42)

	got := ReadTransactionIndex(db, txHash)
	if got == nil {
		t.Fatal("expected non-nil block number")
	}
	if *got != 42 {
		t.Fatalf("block number: got %d, want 42", *got)
	}
}

func TestReadTransactionIndex_NotFound(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	got := ReadTransactionIndex(db, bytes.Repeat([]byte{0x00}, 32))
	if got != nil {
		t.Fatal("expected nil for missing tx index")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/rawdb/ -run "TestWriteReadTransaction|TestReadTransaction" -v`
Expected: FAIL — functions not defined

- [ ] **Step 3: Add schema helpers**

In `core/rawdb/schema.go`, add:

```go
var (
	// ... existing prefixes ...
	txInfoBlockPrefix = []byte("tib-")
)

func txInfoKey(hash []byte) []byte {
	return append(append([]byte{}, txInfoPrefix...), hash...)
}

func txInfoBlockKey(number uint64) []byte {
	k := make([]byte, len(txInfoBlockPrefix)+8)
	copy(k, txInfoBlockPrefix)
	binary.BigEndian.PutUint64(k[len(txInfoBlockPrefix):], number)
	return k
}
```

- [ ] **Step 4: Create accessors_txinfo.go**

```go
// core/rawdb/accessors_txinfo.go
package rawdb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// WriteTransactionInfo stores a single TransactionInfo indexed by txID.
func WriteTransactionInfo(db ethdb.KeyValueWriter, txID []byte, info *corepb.TransactionInfo) {
	data, err := proto.Marshal(info)
	if err != nil {
		return
	}
	db.Put(txInfoKey(txID), data)
}

// ReadTransactionInfo retrieves a TransactionInfo by txID.
func ReadTransactionInfo(db ethdb.KeyValueReader, txID []byte) *corepb.TransactionInfo {
	data, err := db.Get(txInfoKey(txID))
	if err != nil {
		return nil
	}
	info := &corepb.TransactionInfo{}
	if err := proto.Unmarshal(data, info); err != nil {
		return nil
	}
	return info
}

// WriteTransactionInfosByBlock stores all TransactionInfos for a block.
func WriteTransactionInfosByBlock(db ethdb.KeyValueWriter, blockNum uint64, infos []*corepb.TransactionInfo) {
	ret := &corepb.TransactionRet{
		BlockNumber:     int64(blockNum),
		Transactioninfo: infos,
	}
	if len(infos) > 0 {
		ret.BlockTimeStamp = infos[0].BlockTimeStamp
	}
	data, err := proto.Marshal(ret)
	if err != nil {
		return
	}
	db.Put(txInfoBlockKey(blockNum), data)
}

// ReadTransactionInfosByBlock retrieves all TransactionInfos for a block number.
func ReadTransactionInfosByBlock(db ethdb.KeyValueReader, blockNum uint64) []*corepb.TransactionInfo {
	data, err := db.Get(txInfoBlockKey(blockNum))
	if err != nil {
		return nil
	}
	ret := &corepb.TransactionRet{}
	if err := proto.Unmarshal(data, ret); err != nil {
		return nil
	}
	return ret.Transactioninfo
}

// WriteTransactionIndex stores a tx-hash -> block-number mapping.
func WriteTransactionIndex(db ethdb.KeyValueWriter, txHash []byte, blockNum uint64) {
	num := make([]byte, 8)
	binary.BigEndian.PutUint64(num, blockNum)
	db.Put(txKey(txHash), num)
}

// ReadTransactionIndex retrieves the block number for a tx hash.
func ReadTransactionIndex(db ethdb.KeyValueReader, txHash []byte) *uint64 {
	data, err := db.Get(txKey(txHash))
	if err != nil || len(data) != 8 {
		return nil
	}
	num := binary.BigEndian.Uint64(data)
	return &num
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/rawdb/ -v`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add core/rawdb/schema.go core/rawdb/accessors_txinfo.go core/rawdb/accessors_txinfo_test.go
git commit -m "rawdb: add TransactionInfo and TransactionIndex accessors

Write/Read TransactionInfo by txID (ti-<hash>),
Write/Read TransactionInfosByBlock (tib-<blockNum>),
Write/Read TransactionIndex (tx-<hash> -> blockNum).
Uses protobuf TransactionInfo and TransactionRet messages."
```

---

### Task 5: State Processor — BandwidthResult, ApplyTransaction, ProcessBlock, buildTransactionInfo

**Files:**
- Modify: `core/bandwidth.go`
- Modify: `core/state_processor.go`
- Modify: `core/state_processor_test.go`

- [ ] **Step 1: Write test for updated return types**

Add/modify in `core/state_processor_test.go`:

```go
func TestApplyTransaction_ReturnsResult(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 1_000_000)

	tx := makeTestTransferTx(1, 2, 300_000)
	result, err := ApplyTransaction(statedb, dynProps, tx, 3000, 1)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1, got %d", result.ContractRet)
	}
}

func TestProcessBlock_ReturnsTransactionInfos(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 10_000_000)

	_, err := statedb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	tx1 := makeTestTransferTx(1, 2, 1_000_000)
	tx2 := makeTestTransferTx(1, 3, 2_000_000)

	witnessAddr := testProcessorAddr(0xFF)
	statedb.CreateAccount(witnessAddr, corepb.AccountType_Normal)

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         1,
				Timestamp:      3000,
				WitnessAddress: witnessAddr.Bytes(),
			},
		},
		Transactions: []*corepb.Transaction{tx1.Proto(), tx2.Proto()},
	})

	txInfos, err := ProcessBlock(statedb, dynProps, block)
	if err != nil {
		t.Fatal(err)
	}
	if len(txInfos) != 2 {
		t.Fatalf("expected 2 txInfos, got %d", len(txInfos))
	}
	for i, info := range txInfos {
		if info.BlockNumber != 1 {
			t.Fatalf("txInfo[%d] blockNumber: got %d, want 1", i, info.BlockNumber)
		}
		if info.BlockTimeStamp != 3000 {
			t.Fatalf("txInfo[%d] blockTimeStamp: got %d, want 3000", i, info.BlockTimeStamp)
		}
		if len(info.Id) == 0 {
			t.Fatalf("txInfo[%d] has empty ID", i)
		}
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/ -run "TestApplyTransaction_ReturnsResult|TestProcessBlock_ReturnsTransactionInfos" -v`
Expected: FAIL — `ApplyTransaction` returns `(int64, error)` not `(*actuator.Result, error)`

- [ ] **Step 3: Update consumeBandwidth to return BandwidthResult**

In `core/bandwidth.go`:

```go
package core

import (
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// BandwidthResult captures bandwidth consumption details.
type BandwidthResult struct {
	NetUsage int64
	NetFee   int64
}

func consumeBandwidth(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, blockTime int64) (*BandwidthResult, error) {
	sender := extractSender(tx)
	if sender == (tcommon.Address{}) {
		return nil, fmt.Errorf("cannot determine sender")
	}

	txSize := int64(tx.Size())

	// Try frozen bandwidth first
	frozenBW := statedb.GetFrozenV2Amount(sender, corepb.ResourceCode_BANDWIDTH)
	if frozenBW > 0 {
		recoveredUsage := recoverUsage(statedb.GetNetUsage(sender), statedb.GetLatestConsumeTime(sender), blockTime)
		if recoveredUsage+txSize <= frozenBW {
			statedb.SetNetUsage(sender, recoveredUsage+txSize)
			statedb.SetLatestConsumeTime(sender, blockTime)
			return &BandwidthResult{NetUsage: txSize}, nil
		}
	}

	// Try free bandwidth
	freeLimit := dynProps.FreeNetLimit()
	recoveredFreeUsage := recoverUsage(statedb.GetFreeNetUsage(sender), statedb.GetLatestConsumeFreeTime(sender), blockTime)
	if recoveredFreeUsage+txSize <= freeLimit {
		statedb.SetFreeNetUsage(sender, recoveredFreeUsage+txSize)
		statedb.SetLatestConsumeFreeTime(sender, blockTime)
		return &BandwidthResult{NetUsage: txSize}, nil
	}

	// Burn TRX
	cost := txSize * dynProps.TransactionFee()
	if err := statedb.SubBalance(sender, cost); err != nil {
		return nil, fmt.Errorf("insufficient balance to pay bandwidth: need %d sun", cost)
	}
	return &BandwidthResult{NetFee: cost}, nil
}

// extractSender extracts the owner address from the first contract of a transaction.
func extractSender(tx *types.Transaction) tcommon.Address {
	contract := tx.Contract()
	if contract == nil {
		return tcommon.Address{}
	}
	msg, err := contract.Parameter.UnmarshalNew()
	if err != nil {
		return tcommon.Address{}
	}
	type ownerAddressGetter interface {
		GetOwnerAddress() []byte
	}
	if oag, ok := msg.(ownerAddressGetter); ok {
		return tcommon.BytesToAddress(oag.GetOwnerAddress())
	}
	return tcommon.Address{}
}
```

- [ ] **Step 4: Update ApplyTransaction and ProcessBlock**

In `core/state_processor.go`:

```go
package core

import (
	"fmt"

	"github.com/tronprotocol/go-tron/actuator"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// ApplyTransaction executes a single transaction against the given state.
// Returns the full execution result for TransactionInfo construction.
func ApplyTransaction(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, blockTime int64, blockNum uint64) (*actuator.Result, error) {
	act, err := actuator.CreateActuator(tx)
	if err != nil {
		return nil, fmt.Errorf("create actuator: %w", err)
	}

	ctx := &actuator.Context{
		State:       statedb,
		DynProps:    dynProps,
		Tx:          tx,
		BlockTime:   blockTime,
		BlockNumber: blockNum,
	}

	if err := act.Validate(ctx); err != nil {
		return nil, fmt.Errorf("validate: %w", err)
	}

	bwResult, err := consumeBandwidth(statedb, dynProps, tx, blockTime)
	if err != nil {
		return nil, fmt.Errorf("bandwidth: %w", err)
	}

	snap := statedb.Snapshot()
	result, err := act.Execute(ctx)
	if err != nil {
		statedb.RevertToSnapshot(snap)
		return nil, fmt.Errorf("execute: %w", err)
	}

	result.NetUsage = bwResult.NetUsage
	result.NetFee = bwResult.NetFee

	return result, nil
}

// buildTransactionInfo constructs a TransactionInfo protobuf from execution results.
func buildTransactionInfo(tx *types.Transaction, result *actuator.Result, blockNum uint64, blockTime int64) *corepb.TransactionInfo {
	txID := tx.Hash()

	info := &corepb.TransactionInfo{
		Id:             txID[:],
		Fee:            result.Fee + result.NetFee,
		BlockNumber:    int64(blockNum),
		BlockTimeStamp: blockTime,
		Receipt: &corepb.ResourceReceipt{
			EnergyUsage:      result.EnergyUsed,
			EnergyFee:        result.EnergyFee,
			OriginEnergyUsage: result.OriginEnergyUsage,
			EnergyUsageTotal: result.EnergyUsed + result.OriginEnergyUsage,
			NetUsage:         result.NetUsage,
			NetFee:           result.NetFee,
			Result:           corepb.Transaction_Result_contractResult(result.ContractRet),
		},
	}

	if len(result.ContractResult) > 0 {
		info.ContractResult = [][]byte{result.ContractResult}
	}

	if len(result.ContractAddress) > 0 {
		info.ContractAddress = result.ContractAddress
	}

	for _, l := range result.Logs {
		pbLog := &corepb.TransactionInfo_Log{
			Address: l.Address[:],
			Data:    l.Data,
		}
		for _, topic := range l.Topics {
			pbLog.Topics = append(pbLog.Topics, topic)
		}
		info.Log = append(info.Log, pbLog)
	}

	if result.ContractRet > 1 {
		info.Result = corepb.TransactionInfo_FAILED
		if result.ContractRet == 2 && len(result.ContractResult) > 0 {
			info.ResMessage = result.ContractResult
		}
	}

	return info
}

// ProcessBlock executes all transactions in a block, pays the block reward,
// and returns the TransactionInfo list for persistence.
func ProcessBlock(statedb *state.StateDB, dynProps *state.DynamicProperties, block *types.Block) ([]*corepb.TransactionInfo, error) {
	var txInfos []*corepb.TransactionInfo

	for i, tx := range block.Transactions() {
		result, err := ApplyTransaction(statedb, dynProps, tx, block.Timestamp(), block.Number())
		if err != nil {
			return nil, fmt.Errorf("tx %d: %w", i, err)
		}
		info := buildTransactionInfo(tx, result, block.Number(), block.Timestamp())
		txInfos = append(txInfos, info)
	}

	witnessAddr := block.WitnessAddress()
	if witnessAddr != (tcommon.Address{}) {
		reward := dynProps.WitnessPayPerBlock()
		if reward > 0 {
			statedb.AddAllowance(witnessAddr, reward)
		}
	}

	return txInfos, nil
}
```

- [ ] **Step 5: Update existing tests for new return types**

In `core/state_processor_test.go`, update existing tests that use the old `(int64, error)` return:

Change `TestApplyTransaction_Transfer`:
```go
func TestApplyTransaction_Transfer(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 1_000_000)

	tx := makeTestTransferTx(1, 2, 300_000)
	result, err := ApplyTransaction(statedb, dynProps, tx, 3000, 1)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: got %d, want 0", result.Fee)
	}
	// ... rest of balance checks unchanged ...
}
```

Change `TestApplyTransaction_ValidationFails`:
```go
func TestApplyTransaction_ValidationFails(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()
	tx := makeTestTransferTx(1, 2, 100)
	_, err := ApplyTransaction(statedb, dynProps, tx, 3000, 1)
	if err == nil {
		t.Fatal("expected validation error")
	}
}
```

Change `TestProcessBlock_WithTransactions`:
```go
func TestProcessBlock_WithTransactions(t *testing.T) {
	// ... setup unchanged ...

	txInfos, err := ProcessBlock(statedb, dynProps, block)
	if err != nil {
		t.Fatal(err)
	}
	_ = txInfos // verified in TestProcessBlock_ReturnsTransactionInfos

	// ... balance and reward checks unchanged ...
}
```

Change `TestProcessBlock_FailingTxRevertsState`:
```go
func TestProcessBlock_FailingTxRevertsState(t *testing.T) {
	// ... setup unchanged ...

	_, err := ProcessBlock(statedb, dynProps, block)
	if err == nil {
		t.Fatal("expected error for invalid transaction")
	}

	// ... balance check unchanged ...
}
```

- [ ] **Step 6: Run tests**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/ -v`
Expected: All PASS

- [ ] **Step 7: Commit**

```bash
git add core/bandwidth.go core/state_processor.go core/state_processor_test.go
git commit -m "core: ApplyTransaction returns *actuator.Result, ProcessBlock returns TransactionInfos

consumeBandwidth now returns *BandwidthResult with NetUsage/NetFee.
ApplyTransaction returns the full *actuator.Result for TransactionInfo
construction. ProcessBlock collects TransactionInfos via
buildTransactionInfo and returns them for persistence by the caller."
```

---

### Task 6: BlockChain and BlockBuilder Integration

**Files:**
- Modify: `core/blockchain.go`
- Modify: `core/block_builder.go`

- [ ] **Step 1: Update InsertBlock to persist TransactionInfos**

In `core/blockchain.go`, update the `InsertBlock` method. The `ProcessBlock` call changes from `error` to `([]*corepb.TransactionInfo, error)`:

```go
// Replace:
if err := ProcessBlock(statedb, dynProps, block); err != nil {
    return fmt.Errorf("process block: %w", err)
}

// With:
txInfos, err := ProcessBlock(statedb, dynProps, block)
if err != nil {
    return fmt.Errorf("process block: %w", err)
}
```

After the existing `rawdb.WriteBlock` and `rawdb.WriteHeadBlockHash` calls, add persistence:

```go
// Persist transaction infos and indexes
for _, info := range txInfos {
    rawdb.WriteTransactionInfo(bc.db, info.Id, info)
}
rawdb.WriteTransactionInfosByBlock(bc.db, block.Number(), txInfos)
for _, tx := range block.Transactions() {
    h := tx.Hash()
    rawdb.WriteTransactionIndex(bc.db, h[:], block.Number())
}
```

- [ ] **Step 2: Update BuildBlock for new ApplyTransaction return**

In `core/block_builder.go`, the `ApplyTransaction` call currently does:
```go
_, err := ApplyTransaction(statedb, dynProps, tx, timestamp, blockNum)
```

Change to:
```go
result, err := ApplyTransaction(statedb, dynProps, tx, timestamp, blockNum)
if err != nil {
    failedTxIDs = append(failedTxIDs, tx.Hash())
    continue
}
_ = result
```

- [ ] **Step 3: Run all tests to verify nothing is broken**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/... -v`
Expected: All PASS

- [ ] **Step 4: Commit**

```bash
git add core/blockchain.go core/block_builder.go
git commit -m "core: persist TransactionInfos and tx indexes in InsertBlock

InsertBlock now writes per-tx TransactionInfo, per-block TransactionInfosByBlock,
and tx-hash-to-block-number indexes after processing a block.
BuildBlock updated for new ApplyTransaction return type."
```

---

### Task 7: DynamicProperties.All() Method

**Files:**
- Modify: `core/state/dynamic_properties.go`

- [ ] **Step 1: Write the test**

Add a test in `core/state/dynamic_properties_test.go` (may need to create):

```go
// core/state/dynamic_properties_test.go
package state

import "testing"

func TestDynamicProperties_All(t *testing.T) {
	dp := NewDynamicProperties()
	dp.Set("energy_fee", 420)

	all := dp.All()
	if all["energy_fee"] != 420 {
		t.Fatalf("energy_fee: got %d, want 420", all["energy_fee"])
	}
	// Verify it's a copy — mutating shouldn't affect the original
	all["energy_fee"] = 999
	if dp.EnergyFee() != 420 {
		t.Fatal("All() should return a copy, not a reference")
	}
	// Should have all default props
	if _, ok := all["maintenance_time_interval"]; !ok {
		t.Fatal("missing maintenance_time_interval in All()")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/state/ -run TestDynamicProperties_All -v`
Expected: FAIL — `All()` method not defined

- [ ] **Step 3: Implement All()**

In `core/state/dynamic_properties.go`, add:

```go
// All returns a read-only copy of all dynamic properties.
func (dp *DynamicProperties) All() map[string]int64 {
	result := make(map[string]int64, len(dp.props))
	for k, v := range dp.props {
		result[k] = v
	}
	return result
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/state/ -run TestDynamicProperties_All -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add core/state/dynamic_properties.go core/state/dynamic_properties_test.go
git commit -m "state: add DynamicProperties.All() for chain parameters API

Returns a read-only copy of all key-value properties."
```

---

### Task 8: Transaction Builder

**Files:**
- Create: `internal/tronapi/txbuilder.go`
- Create: `internal/tronapi/txbuilder_test.go`

- [ ] **Step 1: Write tests**

```go
// internal/tronapi/txbuilder_test.go
package tronapi

import (
	"encoding/binary"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestBuildTransaction(t *testing.T) {
	headBlockNum := uint64(12345)
	headBlockHash := make([]byte, 32)
	for i := range headBlockHash {
		headBlockHash[i] = byte(i)
	}
	headBlockTimestamp := int64(1700000000000)

	tc := &contractpb.TransferContract{
		OwnerAddress: []byte{0x41, 0x01},
		ToAddress:    []byte{0x41, 0x02},
		Amount:       1000000,
	}

	tx, err := buildTransaction(headBlockNum, headBlockHash, headBlockTimestamp,
		corepb.Transaction_Contract_TransferContract, tc, 0)
	if err != nil {
		t.Fatalf("buildTransaction failed: %v", err)
	}

	raw := tx.RawData
	if raw == nil {
		t.Fatal("RawData is nil")
	}

	// ref_block_bytes: bytes 6..7 of block number (big-endian)
	numBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(numBytes, headBlockNum)
	if raw.RefBlockBytes[0] != numBytes[6] || raw.RefBlockBytes[1] != numBytes[7] {
		t.Fatalf("ref_block_bytes: got %x, want %x", raw.RefBlockBytes, numBytes[6:8])
	}

	// ref_block_hash: bytes 8..15 of block hash
	for i := 0; i < 8; i++ {
		if raw.RefBlockHash[i] != headBlockHash[8+i] {
			t.Fatalf("ref_block_hash mismatch at byte %d", i)
		}
	}

	// expiration
	expectedExp := headBlockTimestamp + txExpirationSeconds*1000
	if raw.Expiration != expectedExp {
		t.Fatalf("expiration: got %d, want %d", raw.Expiration, expectedExp)
	}

	// timestamp should be recent (within a few seconds)
	if raw.Timestamp == 0 {
		t.Fatal("timestamp is zero")
	}

	// contract
	if len(raw.Contract) != 1 {
		t.Fatalf("expected 1 contract, got %d", len(raw.Contract))
	}
	if raw.Contract[0].Type != corepb.Transaction_Contract_TransferContract {
		t.Fatalf("wrong contract type: %v", raw.Contract[0].Type)
	}

	// fee_limit should be 0 (not set)
	if raw.FeeLimit != 0 {
		t.Fatalf("fee_limit should be 0, got %d", raw.FeeLimit)
	}
}

func TestBuildTransaction_WithFeeLimit(t *testing.T) {
	tc := &contractpb.TransferContract{
		OwnerAddress: []byte{0x41, 0x01},
		ToAddress:    []byte{0x41, 0x02},
		Amount:       100,
	}

	tx, err := buildTransaction(100, make([]byte, 32), 1000,
		corepb.Transaction_Contract_TransferContract, tc, 50_000_000)
	if err != nil {
		t.Fatalf("buildTransaction failed: %v", err)
	}
	if tx.RawData.FeeLimit != 50_000_000 {
		t.Fatalf("fee_limit: got %d, want 50000000", tx.RawData.FeeLimit)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./internal/tronapi/ -run TestBuildTransaction -v`
Expected: FAIL — `buildTransaction` not defined

- [ ] **Step 3: Create txbuilder.go**

```go
// internal/tronapi/txbuilder.go
package tronapi

import (
	"encoding/binary"
	"time"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const txExpirationSeconds = 60

// buildTransaction creates an unsigned Transaction wrapping the given contract.
func buildTransaction(
	headBlockNum uint64,
	headBlockHash []byte,
	headBlockTimestamp int64,
	contractType corepb.Transaction_Contract_ContractType,
	contractMsg proto.Message,
	feeLimit int64,
) (*corepb.Transaction, error) {
	paramAny, err := anypb.New(contractMsg)
	if err != nil {
		return nil, err
	}

	numBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(numBytes, headBlockNum)
	refBlockBytes := numBytes[6:8]

	var refBlockHash []byte
	if len(headBlockHash) >= 16 {
		refBlockHash = headBlockHash[8:16]
	}

	now := time.Now().UnixMilli()
	expiration := headBlockTimestamp + txExpirationSeconds*1000

	rawData := &corepb.TransactionRaw{
		RefBlockBytes: refBlockBytes,
		RefBlockHash:  refBlockHash,
		Expiration:    expiration,
		Timestamp:     now,
		Contract: []*corepb.Transaction_Contract{{
			Type:      contractType,
			Parameter: paramAny,
		}},
	}

	if feeLimit > 0 {
		rawData.FeeLimit = feeLimit
	}

	return &corepb.Transaction{
		RawData: rawData,
	}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./internal/tronapi/ -run TestBuildTransaction -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add internal/tronapi/txbuilder.go internal/tronapi/txbuilder_test.go
git commit -m "tronapi: add buildTransaction helper for tx building APIs

Constructs unsigned Transaction proto with ref_block_bytes, ref_block_hash,
expiration, and timestamp derived from head block. Used by createtransaction,
deploycontract, and triggersmartcontract endpoints."
```

---

### Task 9: Extend Backend Interface and Types

**Files:**
- Modify: `internal/tronapi/backend.go`
- Modify: `internal/tronapi/api_test.go` (update mock)

- [ ] **Step 1: Extend backend.go**

```go
package tronapi

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type NodeInfo struct {
	Version      string `json:"version"`
	CurrentBlock uint64 `json:"currentBlock"`
}

type TriggerResult struct {
	Result     []byte `json:"result"`
	EnergyUsed int64  `json:"energy_used"`
}

type AccountResource struct {
	FreeNetUsed       int64 `json:"freeNetUsed"`
	FreeNetLimit      int64 `json:"freeNetLimit"`
	NetUsed           int64 `json:"NetUsed"`
	NetLimit          int64 `json:"NetLimit"`
	TotalNetLimit     int64 `json:"TotalNetLimit"`
	TotalNetWeight    int64 `json:"TotalNetWeight"`
	EnergyUsed        int64 `json:"EnergyUsed"`
	EnergyLimit       int64 `json:"EnergyLimit"`
	TotalEnergyLimit  int64 `json:"TotalEnergyLimit"`
	TotalEnergyWeight int64 `json:"TotalEnergyWeight"`
}

type ChainParameter struct {
	Key   string `json:"key"`
	Value int64  `json:"value"`
}

type WitnessInfo struct {
	Address   string `json:"address"`
	VoteCount int64  `json:"voteCount"`
	URL       string `json:"url"`
	IsJobs    bool   `json:"isJobs"`
}

type Backend interface {
	// Existing
	CurrentBlock() *types.Block
	GetBlockByNumber(number uint64) (*types.Block, error)
	GetAccount(addr common.Address) (*types.Account, error)
	BroadcastTransaction(tx *types.Transaction) error
	GetNodeInfo() *NodeInfo
	PendingTransactionCount() int
	GetContract(addr common.Address) (*contractpb.SmartContract, error)
	TriggerConstantContract(owner, contract common.Address, data []byte, energyLimit int64) (*TriggerResult, error)

	// Transaction queries
	GetTransactionByID(txHash common.Hash) (*corepb.Transaction, error)
	GetTransactionInfoByID(txHash common.Hash) (*corepb.TransactionInfo, error)
	GetTransactionInfoByBlockNum(blockNum uint64) ([]*corepb.TransactionInfo, error)

	// Block queries
	GetBlockByHash(hash common.Hash) (*types.Block, error)
	GetBlocksByRange(start, end uint64) ([]*types.Block, error)

	// Transaction building
	BuildTransferTransaction(owner, to common.Address, amount int64) (*corepb.Transaction, error)
	BuildDeployContractTransaction(owner common.Address, abi string, bytecode []byte,
		feeLimit int64, callValue int64, name string, consumePercent int64) (*corepb.Transaction, error)
	BuildTriggerContractTransaction(owner, contract common.Address, data []byte,
		feeLimit int64, callValue int64) (*corepb.Transaction, *TriggerResult, error)
	EstimateEnergy(owner, contract common.Address, data []byte) (int64, error)

	// Resource & chain queries
	GetAccountResource(addr common.Address) (*AccountResource, error)
	GetChainParameters() []ChainParameter
	ListWitnesses() ([]*WitnessInfo, error)
	NextMaintenanceTime() int64
}
```

- [ ] **Step 2: Update mockBackend in api_test.go**

Add stub implementations to `mockBackend` in `internal/tronapi/api_test.go`:

```go
func (m *mockBackend) GetTransactionByID(txHash common.Hash) (*corepb.Transaction, error) {
	return nil, fmt.Errorf("not found")
}

func (m *mockBackend) GetTransactionInfoByID(txHash common.Hash) (*corepb.TransactionInfo, error) {
	return nil, fmt.Errorf("not found")
}

func (m *mockBackend) GetTransactionInfoByBlockNum(blockNum uint64) ([]*corepb.TransactionInfo, error) {
	return nil, nil
}

func (m *mockBackend) GetBlockByHash(hash common.Hash) (*types.Block, error) {
	return nil, fmt.Errorf("not found")
}

func (m *mockBackend) GetBlocksByRange(start, end uint64) ([]*types.Block, error) {
	var blocks []*types.Block
	for i := start; i < end; i++ {
		b, _ := m.GetBlockByNumber(i)
		blocks = append(blocks, b)
	}
	return blocks, nil
}

func (m *mockBackend) BuildTransferTransaction(owner, to common.Address, amount int64) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}

func (m *mockBackend) BuildDeployContractTransaction(owner common.Address, abi string, bytecode []byte,
	feeLimit int64, callValue int64, name string, consumePercent int64) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}

func (m *mockBackend) BuildTriggerContractTransaction(owner, contract common.Address, data []byte,
	feeLimit int64, callValue int64) (*corepb.Transaction, *TriggerResult, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, &TriggerResult{Result: []byte{0x42}, EnergyUsed: 100}, nil
}

func (m *mockBackend) EstimateEnergy(owner, contract common.Address, data []byte) (int64, error) {
	return 21000, nil
}

func (m *mockBackend) GetAccountResource(addr common.Address) (*AccountResource, error) {
	return &AccountResource{
		FreeNetLimit:     1500,
		TotalNetLimit:    43200000000,
		TotalEnergyLimit: 50000000000,
	}, nil
}

func (m *mockBackend) GetChainParameters() []ChainParameter {
	return []ChainParameter{
		{Key: "energy_fee", Value: 420},
	}
}

func (m *mockBackend) ListWitnesses() ([]*WitnessInfo, error) {
	return []*WitnessInfo{
		{Address: "4100000000000000000000000000000000000001", VoteCount: 100, IsJobs: true},
	}, nil
}

func (m *mockBackend) NextMaintenanceTime() int64 {
	return 1700000000000
}
```

- [ ] **Step 3: Run tests to verify they compile and pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./internal/tronapi/ -v`
Expected: All PASS

- [ ] **Step 4: Commit**

```bash
git add internal/tronapi/backend.go internal/tronapi/api_test.go
git commit -m "tronapi: extend Backend interface with 13 new methods

Adds transaction query, block query, tx building, and resource/chain
query methods. Adds AccountResource, ChainParameter, WitnessInfo types.
Updates mockBackend with stub implementations."
```

---

### Task 10: Implement TronBackend Methods

**Files:**
- Modify: `core/tron_backend.go`

- [ ] **Step 1: Implement all new Backend methods**

In `core/tron_backend.go`, add the new methods. Add needed imports (`encoding/hex`):

```go
package core

import (
	"encoding/hex"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/tronprotocol/go-tron/vm"
)

// ... existing struct and methods unchanged ...

func (b *TronBackend) GetTransactionByID(txHash tcommon.Hash) (*corepb.Transaction, error) {
	blockNum := rawdb.ReadTransactionIndex(b.chain.db, txHash[:])
	if blockNum == nil {
		return nil, fmt.Errorf("transaction not found")
	}
	block := rawdb.ReadBlock(b.chain.db, *blockNum)
	if block == nil {
		return nil, fmt.Errorf("block %d not found", *blockNum)
	}
	for _, tx := range block.Transactions() {
		if tx.Hash() == txHash {
			return tx.Proto(), nil
		}
	}
	return nil, fmt.Errorf("transaction not found in block %d", *blockNum)
}

func (b *TronBackend) GetTransactionInfoByID(txHash tcommon.Hash) (*corepb.TransactionInfo, error) {
	info := rawdb.ReadTransactionInfo(b.chain.db, txHash[:])
	if info == nil {
		return nil, fmt.Errorf("transaction info not found")
	}
	return info, nil
}

func (b *TronBackend) GetTransactionInfoByBlockNum(blockNum uint64) ([]*corepb.TransactionInfo, error) {
	infos := rawdb.ReadTransactionInfosByBlock(b.chain.db, blockNum)
	return infos, nil
}

func (b *TronBackend) GetBlockByHash(hash tcommon.Hash) (*types.Block, error) {
	block := b.chain.GetBlockByHash(hash)
	if block == nil {
		return nil, fmt.Errorf("block not found")
	}
	return block, nil
}

func (b *TronBackend) GetBlocksByRange(start, end uint64) ([]*types.Block, error) {
	if end <= start {
		return nil, fmt.Errorf("invalid range")
	}
	if end-start > 100 {
		end = start + 100
	}
	var blocks []*types.Block
	for i := start; i < end; i++ {
		block := b.chain.GetBlockByNumber(i)
		if block == nil {
			break
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

func (b *TronBackend) BuildTransferTransaction(owner, to tcommon.Address, amount int64) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	tc := &contractpb.TransferContract{
		OwnerAddress: owner[:],
		ToAddress:    to[:],
		Amount:       amount,
	}
	return buildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_TransferContract, tc, 0)
}

func (b *TronBackend) BuildDeployContractTransaction(owner tcommon.Address, abi string, bytecode []byte,
	feeLimit int64, callValue int64, name string, consumePercent int64) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	csc := &contractpb.CreateSmartContract{
		OwnerAddress: owner[:],
		NewContract: &contractpb.SmartContract{
			OriginAddress:              owner[:],
			Abi:                        &contractpb.SmartContract_ABI{},
			Bytecode:                   bytecode,
			CallValue:                  callValue,
			Name:                       name,
			ConsumeUserResourcePercent: consumePercent,
		},
	}
	return buildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_CreateSmartContract, csc, feeLimit)
}

func (b *TronBackend) BuildTriggerContractTransaction(owner, contract tcommon.Address, data []byte,
	feeLimit int64, callValue int64) (*corepb.Transaction, *tronapi.TriggerResult, error) {
	current := b.chain.CurrentBlock()
	tsc := &contractpb.TriggerSmartContract{
		OwnerAddress:    owner[:],
		ContractAddress: contract[:],
		Data:            data,
		CallValue:       callValue,
	}
	tx, err := buildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_TriggerSmartContract, tsc, feeLimit)
	if err != nil {
		return nil, nil, err
	}

	triggerResult, _ := b.TriggerConstantContract(owner, contract, data, 30_000_000)
	return tx, triggerResult, nil
}

func (b *TronBackend) EstimateEnergy(owner, contract tcommon.Address, data []byte) (int64, error) {
	result, err := b.TriggerConstantContract(owner, contract, data, 30_000_000)
	if err != nil {
		return 0, err
	}
	return result.EnergyUsed, nil
}

func (b *TronBackend) GetAccountResource(addr tcommon.Address) (*tronapi.AccountResource, error) {
	current := b.chain.CurrentBlock()
	root := current.AccountStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}

	dynProps := state.LoadDynamicProperties(b.chain.db)

	return &tronapi.AccountResource{
		FreeNetUsed:       statedb.GetFreeNetUsage(addr),
		FreeNetLimit:      dynProps.FreeNetLimit(),
		NetUsed:           statedb.GetNetUsage(addr),
		TotalNetLimit:     dynProps.TotalNetLimit(),
		EnergyUsed:        statedb.GetEnergyUsage(addr),
		TotalEnergyLimit:  dynProps.TotalEnergyCurrentLimit(),
	}, nil
}

func (b *TronBackend) GetChainParameters() []tronapi.ChainParameter {
	dynProps := state.LoadDynamicProperties(b.chain.db)
	all := dynProps.All()
	params := make([]tronapi.ChainParameter, 0, len(all))
	for k, v := range all {
		params = append(params, tronapi.ChainParameter{Key: k, Value: v})
	}
	return params
}

func (b *TronBackend) ListWitnesses() ([]*tronapi.WitnessInfo, error) {
	witnessAddrs := rawdb.ReadWitnessIndex(b.chain.db)
	activeSet := b.chain.ActiveWitnesses()
	activeMap := make(map[tcommon.Address]bool, len(activeSet))
	for _, a := range activeSet {
		activeMap[a] = true
	}

	var result []*tronapi.WitnessInfo
	for _, addr := range witnessAddrs {
		w := rawdb.ReadWitness(b.chain.db, addr)
		if w == nil {
			continue
		}
		result = append(result, &tronapi.WitnessInfo{
			Address:   hex.EncodeToString(addr[:]),
			VoteCount: w.VoteCount(),
			URL:       w.URL(),
			IsJobs:    activeMap[addr],
		})
	}
	return result, nil
}

func (b *TronBackend) NextMaintenanceTime() int64 {
	return b.chain.NextMaintenanceTime()
}
```

Note: the `buildTransaction` import comes from `txbuilder.go` — both are in `package tronapi`. But wait — `TronBackend` is in `package core`, not `tronapi`. It needs to call the package-private `buildTransaction`. This requires moving the call into a helper or making `buildTransaction` exported.

Actually, looking at the code, `buildTransaction` is in `internal/tronapi/txbuilder.go` (package `tronapi`), but `TronBackend` is in `core/tron_backend.go` (package `core`). So `TronBackend` can't call `buildTransaction` directly. The solution: use `tronapi.BuildTransaction` (export it).

In `internal/tronapi/txbuilder.go`, rename `buildTransaction` to `BuildTransaction` (exported).

In `internal/tronapi/txbuilder_test.go`, update calls from `buildTransaction` to `BuildTransaction`.

In `core/tron_backend.go`, call `tronapi.BuildTransaction(...)`.

- [ ] **Step 2: Check for GetEnergyUsage method**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && grep -rn "GetEnergyUsage\|EnergyUsage" core/state/statedb.go`

If it doesn't exist, it returns 0 for now (we haven't implemented energy tracking per account yet). Use a stub that returns 0.

- [ ] **Step 3: Run compilation check**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go build ./...`
Expected: Compiles successfully

- [ ] **Step 4: Run tests**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/ -v && go test ./internal/tronapi/ -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add core/tron_backend.go internal/tronapi/txbuilder.go internal/tronapi/txbuilder_test.go
git commit -m "core: implement all new TronBackend methods for Phase 7

Transaction queries use rawdb accessors. Block queries delegate to
BlockChain. Transaction builders use tronapi.BuildTransaction.
Resource queries read from StateDB and DynamicProperties.
Export BuildTransaction for cross-package access."
```

---

### Task 11: API Handlers — Transaction Building Endpoints

**Files:**
- Modify: `internal/tronapi/api.go`

- [ ] **Step 1: Add route registrations and handler implementations**

In `api.go`, add to `RegisterRoutes`:

```go
// Transaction building
mux.HandleFunc("/wallet/createtransaction", api.createTransaction)
mux.HandleFunc("/wallet/deploycontract", api.deployContract)
mux.HandleFunc("/wallet/triggersmartcontract", api.triggerSmartContract)
mux.HandleFunc("/wallet/estimateenergy", api.estimateEnergy)
```

Add handler methods:

```go
func (api *API) createTransaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress string `json:"owner_address"`
		ToAddress    string `json:"to_address"`
		Amount       int64  `json:"amount"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	owner := common.BytesToAddress(common.FromHex(body.OwnerAddress))
	to := common.BytesToAddress(common.FromHex(body.ToAddress))

	tx, err := api.backend.BuildTransferTransaction(owner, to, body.Amount)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) deployContract(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress               string `json:"owner_address"`
		ABI                        string `json:"abi"`
		Bytecode                   string `json:"bytecode"`
		FeeLimit                   int64  `json:"fee_limit"`
		CallValue                  int64  `json:"call_value"`
		Name                       string `json:"name"`
		ConsumeUserResourcePercent int64  `json:"consume_user_resource_percent"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	owner := common.BytesToAddress(common.FromHex(body.OwnerAddress))
	bytecode := common.FromHex(body.Bytecode)

	tx, err := api.backend.BuildDeployContractTransaction(owner, body.ABI, bytecode,
		body.FeeLimit, body.CallValue, body.Name, body.ConsumeUserResourcePercent)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) triggerSmartContract(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress     string `json:"owner_address"`
		ContractAddress  string `json:"contract_address"`
		FunctionSelector string `json:"function_selector"`
		Parameter        string `json:"parameter"`
		Data             string `json:"data"`
		FeeLimit         int64  `json:"fee_limit"`
		CallValue        int64  `json:"call_value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	owner := common.BytesToAddress(common.FromHex(body.OwnerAddress))
	contract := common.BytesToAddress(common.FromHex(body.ContractAddress))

	var data []byte
	if body.Data != "" {
		data = common.FromHex(body.Data)
	} else if body.FunctionSelector != "" {
		selectorHash := common.Keccak256([]byte(body.FunctionSelector))
		data = selectorHash[:4]
		if body.Parameter != "" {
			data = append(data, common.FromHex(body.Parameter)...)
		}
	}

	tx, triggerResult, err := api.backend.BuildTriggerContractTransaction(owner, contract, data, body.FeeLimit, body.CallValue)

	resp := map[string]interface{}{
		"result": map[string]interface{}{
			"result": err == nil,
		},
	}
	if tx != nil {
		txJSON, _ := marshalTransactionMap(tx)
		resp["transaction"] = txJSON
	}
	if triggerResult != nil {
		resp["energy_used"] = triggerResult.EnergyUsed
		if len(triggerResult.Result) > 0 {
			resp["constant_result"] = []string{hex.EncodeToString(triggerResult.Result)}
		}
	}
	if err != nil {
		resp["result"].(map[string]interface{})["message"] = hex.EncodeToString([]byte(err.Error()))
	}

	jsonData, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)
}

func (api *API) estimateEnergy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		OwnerAddress     string `json:"owner_address"`
		ContractAddress  string `json:"contract_address"`
		FunctionSelector string `json:"function_selector"`
		Parameter        string `json:"parameter"`
		Data             string `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	owner := common.BytesToAddress(common.FromHex(body.OwnerAddress))
	contract := common.BytesToAddress(common.FromHex(body.ContractAddress))

	var data []byte
	if body.Data != "" {
		data = common.FromHex(body.Data)
	} else if body.FunctionSelector != "" {
		selectorHash := common.Keccak256([]byte(body.FunctionSelector))
		data = selectorHash[:4]
		if body.Parameter != "" {
			data = append(data, common.FromHex(body.Parameter)...)
		}
	}

	energy, err := api.backend.EstimateEnergy(owner, contract, data)

	resp := map[string]interface{}{
		"result": map[string]interface{}{
			"result": err == nil,
		},
	}
	if err == nil {
		resp["energy_required"] = energy
	} else {
		resp["result"].(map[string]interface{})["message"] = hex.EncodeToString([]byte(err.Error()))
	}

	jsonData, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)
}
```

Add helper functions:

```go
func writeTransactionJSON(w http.ResponseWriter, tx *corepb.Transaction) {
	txMap, err := marshalTransactionMap(tx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, _ := json.Marshal(txMap)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func marshalTransactionMap(tx *corepb.Transaction) (map[string]interface{}, error) {
	if tx == nil {
		return nil, fmt.Errorf("nil transaction")
	}
	result := marshalMessage(tx.ProtoReflect())
	addTxComputedFields(result, tx.ProtoReflect())
	return result, nil
}
```

Note: `marshalMessage` and `addTxComputedFields` are already in `tronjson.go` (same package). Add `"fmt"` to imports if not already present.

- [ ] **Step 2: Run tests**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./internal/tronapi/ -v`
Expected: All PASS

- [ ] **Step 3: Commit**

```bash
git add internal/tronapi/api.go
git commit -m "tronapi: add transaction building API handlers

createtransaction, deploycontract, triggersmartcontract, estimateenergy
endpoints. All build unsigned transactions server-side for client signing."
```

---

### Task 12: API Handlers — Query Endpoints

**Files:**
- Modify: `internal/tronapi/api.go`

- [ ] **Step 1: Add route registrations and handler implementations**

In `RegisterRoutes`, add:

```go
// Transaction queries
mux.HandleFunc("/wallet/gettransactionbyid", api.getTransactionByID)
mux.HandleFunc("/wallet/gettransactioninfobyid", api.getTransactionInfoByID)
mux.HandleFunc("/wallet/gettransactioninfobyblocknum", api.getTransactionInfoByBlockNum)

// Block queries
mux.HandleFunc("/wallet/getblockbyid", api.getBlockByID)
mux.HandleFunc("/wallet/getblockbylimitnext", api.getBlockByLimitNext)

// Resource & chain queries
mux.HandleFunc("/wallet/getaccountresource", api.getAccountResource)
mux.HandleFunc("/wallet/getchainparameters", api.getChainParameters)
mux.HandleFunc("/wallet/listwitnesses", api.listWitnesses)
mux.HandleFunc("/wallet/getnextmaintenancetime", api.getNextMaintenanceTime)
```

Add handlers:

```go
func (api *API) getTransactionByID(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Value string `json:"value"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if body.Value == "" {
		body.Value = r.URL.Query().Get("value")
	}
	if body.Value == "" {
		http.Error(w, "value required", http.StatusBadRequest)
		return
	}

	hashBytes := common.FromHex(body.Value)
	var hash common.Hash
	copy(hash[:], hashBytes)

	tx, err := api.backend.GetTransactionByID(hash)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) getTransactionInfoByID(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Value string `json:"value"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if body.Value == "" {
		body.Value = r.URL.Query().Get("value")
	}
	if body.Value == "" {
		http.Error(w, "value required", http.StatusBadRequest)
		return
	}

	hashBytes := common.FromHex(body.Value)
	var hash common.Hash
	copy(hash[:], hashBytes)

	info, err := api.backend.GetTransactionInfoByID(hash)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	writeTronJSON(w, info)
}

func (api *API) getTransactionInfoByBlockNum(w http.ResponseWriter, r *http.Request) {
	numStr := r.URL.Query().Get("num")
	if numStr == "" {
		var body struct {
			Num int64 `json:"num"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			numStr = strconv.FormatInt(body.Num, 10)
		}
	}
	num, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid block number", http.StatusBadRequest)
		return
	}

	infos, err := api.backend.GetTransactionInfoByBlockNum(num)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Marshal each info to tron JSON and return as array
	var result []map[string]interface{}
	for _, info := range infos {
		m := marshalMessage(info.ProtoReflect())
		result = append(result, m)
	}
	if result == nil {
		result = []map[string]interface{}{}
	}

	data, _ := json.Marshal(result)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getBlockByID(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Value string `json:"value"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if body.Value == "" {
		body.Value = r.URL.Query().Get("value")
	}
	if body.Value == "" {
		http.Error(w, "value required", http.StatusBadRequest)
		return
	}

	hashBytes := common.FromHex(body.Value)
	var hash common.Hash
	copy(hash[:], hashBytes)

	block, err := api.backend.GetBlockByHash(hash)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	writeBlockJSON(w, block.Proto())
}

func (api *API) getBlockByLimitNext(w http.ResponseWriter, r *http.Request) {
	var body struct {
		StartNum int64 `json:"startNum"`
		EndNum   int64 `json:"endNum"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	blocks, err := api.backend.GetBlocksByRange(uint64(body.StartNum), uint64(body.EndNum))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var blockList []map[string]interface{}
	for _, b := range blocks {
		data, err := MarshalBlock(b.Proto())
		if err != nil {
			continue
		}
		var m map[string]interface{}
		json.Unmarshal(data, &m)
		blockList = append(blockList, m)
	}
	if blockList == nil {
		blockList = []map[string]interface{}{}
	}

	resp := map[string]interface{}{
		"block": blockList,
	}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getAccountResource(w http.ResponseWriter, r *http.Request) {
	addrHex := r.URL.Query().Get("address")
	if addrHex == "" {
		var body struct {
			Address string `json:"address"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			addrHex = body.Address
		}
	}
	if addrHex == "" {
		http.Error(w, "address required", http.StatusBadRequest)
		return
	}

	addr := common.BytesToAddress(common.FromHex(addrHex))
	res, err := api.backend.GetAccountResource(addr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, _ := json.Marshal(res)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getChainParameters(w http.ResponseWriter, r *http.Request) {
	params := api.backend.GetChainParameters()
	resp := map[string]interface{}{
		"chainParameter": params,
	}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) listWitnesses(w http.ResponseWriter, r *http.Request) {
	witnesses, err := api.backend.ListWitnesses()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := map[string]interface{}{
		"witnesses": witnesses,
	}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getNextMaintenanceTime(w http.ResponseWriter, r *http.Request) {
	t := api.backend.NextMaintenanceTime()
	resp := map[string]int64{"num": t}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}
```

- [ ] **Step 2: Add API handler tests**

In `internal/tronapi/api_test.go`, add:

```go
func TestEstimateEnergy(t *testing.T) {
	api := NewAPI(&mockBackend{})
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	body := `{"owner_address":"4101","contract_address":"4102","data":"00"}`
	req := httptest.NewRequest("POST", "/wallet/estimateenergy", strings.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result map[string]interface{}
	json.NewDecoder(w.Body).Decode(&result)
	if _, ok := result["energy_required"]; !ok {
		t.Fatal("expected energy_required in response")
	}
}

func TestGetChainParameters(t *testing.T) {
	api := NewAPI(&mockBackend{})
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	req := httptest.NewRequest("POST", "/wallet/getchainparameters", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result map[string]interface{}
	json.NewDecoder(w.Body).Decode(&result)
	if _, ok := result["chainParameter"]; !ok {
		t.Fatal("expected chainParameter in response")
	}
}

func TestListWitnesses(t *testing.T) {
	api := NewAPI(&mockBackend{})
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	req := httptest.NewRequest("POST", "/wallet/listwitnesses", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result map[string]interface{}
	json.NewDecoder(w.Body).Decode(&result)
	if _, ok := result["witnesses"]; !ok {
		t.Fatal("expected witnesses in response")
	}
}

func TestGetNextMaintenanceTime(t *testing.T) {
	api := NewAPI(&mockBackend{})
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	req := httptest.NewRequest("POST", "/wallet/getnextmaintenancetime", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result map[string]int64
	json.NewDecoder(w.Body).Decode(&result)
	if result["num"] != 1700000000000 {
		t.Fatalf("expected 1700000000000, got %d", result["num"])
	}
}
```

Add `"strings"` to imports in `api_test.go`.

- [ ] **Step 3: Run tests**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./internal/tronapi/ -v`
Expected: All PASS

- [ ] **Step 4: Commit**

```bash
git add internal/tronapi/api.go internal/tronapi/api_test.go
git commit -m "tronapi: add query and resource API handlers

9 new endpoints: gettransactionbyid, gettransactioninfobyid,
gettransactioninfobyblocknum, getblockbyid, getblockbylimitnext,
getaccountresource, getchainparameters, listwitnesses,
getnextmaintenancetime. All 13 Phase 7 endpoints now registered."
```

---

### Task 13: Full Build and Integration Verification

**Files:**
- No new files

- [ ] **Step 1: Run full build**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go build ./...`
Expected: Compiles successfully

- [ ] **Step 2: Run all tests**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./... 2>&1 | tail -30`
Expected: All packages PASS

- [ ] **Step 3: Run system test**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && bash scripts/system_test.sh`
Expected: All tests PASS, both nodes start successfully

- [ ] **Step 4: Commit any fixes if needed**

If any tests or the system test reveals issues, fix them and commit with a descriptive message.

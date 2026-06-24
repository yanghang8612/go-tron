package tracers_test

import (
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/tronprotocol/go-tron/vm"
	"github.com/tronprotocol/go-tron/vm/tracers"
)

// runWithTracer deploys code at a contract address and runs a top-level Call
// with the given tracer installed, returning the tracer's result.
func runWithTracer(t *testing.T, code []byte, tracer tracers.Tracer) interface{} {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb, err := state.New(tcommon.Hash{}, state.NewDatabase(diskdb))
	if err != nil {
		t.Fatal(err)
	}
	owner := tcommon.Address{0x41, 0x01}
	addr := tcommon.Address{0x41, 0x02}
	sdb.CreateAccount(owner, corepb.AccountType_Normal)
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.SetContract(addr, &contractpb.SmartContract{ContractAddress: addr.Bytes()})
	sdb.SetCode(addr, code)

	cfg := vm.TVMConfig{Tracer: tracer}
	evm := vm.NewTVM(sdb, nil, owner, 1, 1000, tcommon.Address{}, 1, cfg)
	if _, _, err := evm.Call(owner, addr, nil, 1_000_000, 0); err != nil && err != vm.ErrExecutionReverted {
		t.Fatalf("call: %v", err)
	}
	res, err := tracer.GetResult()
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	return res
}

// revertingStorageCode: SSTORE 9 at slot 0, SLOAD slot 0, then REVERT.
//
//	PUSH1 09 PUSH1 00 SSTORE
//	PUSH1 00 SLOAD POP
//	PUSH1 00 PUSH1 00 REVERT
var revertingStorageCode = []byte{
	byte(vm.PUSH1), 0x09,
	byte(vm.PUSH1), 0x00,
	byte(vm.SSTORE),
	byte(vm.PUSH1), 0x00,
	byte(vm.SLOAD),
	byte(vm.POP),
	byte(vm.PUSH1), 0x00,
	byte(vm.PUSH1), 0x00,
	byte(vm.REVERT),
}

func TestStructLoggerEndsAtRevertWithOperandsAndStorage(t *testing.T) {
	logger := tracers.NewStructLogger(tracers.LogConfig{})
	res := runWithTracer(t, revertingStorageCode, logger)

	exec, ok := res.(*tracers.ExecutionResult)
	if !ok {
		t.Fatalf("result type: got %T, want *tracers.ExecutionResult", res)
	}
	if !exec.Failed {
		t.Fatal("reverting call must report failed=true")
	}
	if len(exec.StructLogs) == 0 {
		t.Fatal("no struct logs")
	}
	last := exec.StructLogs[len(exec.StructLogs)-1]
	if last.Op != "REVERT" {
		t.Fatalf("trace must end at REVERT, got %q", last.Op)
	}

	// The SSTORE step must expose its operands on the pre-execute stack: the
	// value 9 being stored is visible (operands are the whole point of tracing).
	var sstore *tracers.StructLogRes
	var sload *tracers.StructLogRes
	for i := range exec.StructLogs {
		switch exec.StructLogs[i].Op {
		case "SSTORE":
			sstore = &exec.StructLogs[i]
		case "SLOAD":
			sload = &exec.StructLogs[i]
		}
	}
	if sstore == nil || sstore.Stack == nil {
		t.Fatal("SSTORE step missing or has no stack")
	}
	foundValue := false
	for _, s := range *sstore.Stack {
		if s == "0x9" {
			foundValue = true
		}
	}
	if !foundValue {
		t.Fatalf("SSTORE operands not visible on stack: %v", *sstore.Stack)
	}

	// Storage capture: after SLOAD the zero slot maps to 0x09.
	if sload == nil || sload.Storage == nil {
		t.Fatal("SLOAD step missing or has no storage")
	}
	zeroSlot := strings.Repeat("0", 64)
	val, ok := (*sload.Storage)[zeroSlot]
	if !ok {
		t.Fatalf("SLOAD storage missing zero slot: %v", *sload.Storage)
	}
	if !strings.HasSuffix(val, "09") {
		t.Fatalf("zero slot value: got %q, want suffix 09", val)
	}
}

func TestStructLoggerHonorsToggles(t *testing.T) {
	logger := tracers.NewStructLogger(tracers.LogConfig{
		DisableStack:   true,
		DisableStorage: true,
		DisableMemory:  true,
	})
	res := runWithTracer(t, revertingStorageCode, logger)
	exec := res.(*tracers.ExecutionResult)
	for _, l := range exec.StructLogs {
		if l.Stack != nil {
			t.Fatalf("disableStack ignored: %v", *l.Stack)
		}
		if l.Storage != nil {
			t.Fatalf("disableStorage ignored: %v", *l.Storage)
		}
		if l.Memory != nil {
			t.Fatalf("disableMemory ignored: %v", *l.Memory)
		}
	}
}

func TestStructLoggerHonorsLimit(t *testing.T) {
	logger := tracers.NewStructLogger(tracers.LogConfig{Limit: 3})
	res := runWithTracer(t, revertingStorageCode, logger)
	exec := res.(*tracers.ExecutionResult)
	if len(exec.StructLogs) != 3 {
		t.Fatalf("limit not honored: got %d logs, want 3", len(exec.StructLogs))
	}
}

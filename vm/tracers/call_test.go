package tracers_test

import (
	"encoding/hex"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/tronprotocol/go-tron/vm"
	"github.com/tronprotocol/go-tron/vm/tracers"
)

func TestNewTracerSelection(t *testing.T) {
	def, err := tracers.New(nil)
	if err != nil {
		t.Fatalf("default tracer: %v", err)
	}
	if _, ok := def.(*tracers.StructLogger); !ok {
		t.Fatalf("default tracer: got %T, want *StructLogger", def)
	}

	bad := "noSuchTracer"
	if _, err := tracers.New(&tracers.TraceConfig{Tracer: &bad}); err == nil {
		t.Fatal("unknown tracer name must return an error")
	}
}

func TestCallTracerBuildsNestedTree(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb, err := state.New(tcommon.Hash{}, state.NewDatabase(diskdb))
	if err != nil {
		t.Fatal(err)
	}
	owner := tcommon.Address{0x41, 0x01}
	parent := tcommon.Address{0x41, 0x21}
	child := tcommon.Address{0x41, 0x22}
	for _, a := range []tcommon.Address{owner, parent, child} {
		sdb.CreateAccount(a, corepb.AccountType_Contract)
		sdb.SetContract(a, &contractpb.SmartContract{ContractAddress: a.Bytes()})
	}
	sdb.SetCode(child, []byte{byte(vm.STOP)})

	// Parent CALLs child (zero value), then STOPs.
	parentCode := []byte{
		byte(vm.PUSH1), 0x00, // retSize
		byte(vm.PUSH1), 0x00, // retOffset
		byte(vm.PUSH1), 0x00, // argsSize
		byte(vm.PUSH1), 0x00, // argsOffset
		byte(vm.PUSH1), 0x00, // value
		byte(vm.PUSH20),
	}
	parentCode = append(parentCode, child[1:]...)
	parentCode = append(parentCode,
		byte(vm.PUSH2), 0x27, 0x10, // energy 10000
		byte(vm.CALL),
		byte(vm.STOP),
	)
	sdb.SetCode(parent, parentCode)

	name := "callTracer"
	tracer, err := tracers.New(&tracers.TraceConfig{Tracer: &name})
	if err != nil {
		t.Fatalf("new callTracer: %v", err)
	}
	evm := vm.NewTVM(sdb, nil, owner, 1, 1000, tcommon.Address{}, 1, vm.TVMConfig{Tracer: tracer})
	if _, _, err := evm.Call(owner, parent, []byte{0xab, 0xcd}, 1_000_000, 0); err != nil {
		t.Fatalf("call: %v", err)
	}

	res, err := tracer.GetResult()
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	root, ok := res.(*tracers.CallFrame)
	if !ok {
		t.Fatalf("result type: got %T, want *tracers.CallFrame", res)
	}
	if root.Type != "CALL" {
		t.Fatalf("root type: got %q, want CALL", root.Type)
	}
	if root.To != "0x"+hex.EncodeToString(parent[:]) {
		t.Fatalf("root to: got %q, want parent", root.To)
	}
	if root.Input != "0xabcd" {
		t.Fatalf("root input: got %q, want 0xabcd", root.Input)
	}
	if len(root.Calls) != 1 {
		t.Fatalf("root calls: got %d, want 1 nested CALL", len(root.Calls))
	}
	nested := root.Calls[0]
	if nested.Type != "CALL" {
		t.Fatalf("nested type: got %q, want CALL", nested.Type)
	}
	if nested.From != "0x"+hex.EncodeToString(parent[:]) {
		t.Fatalf("nested from: got %q, want parent", nested.From)
	}
	if nested.To != "0x"+hex.EncodeToString(child[:]) {
		t.Fatalf("nested to: got %q, want child", nested.To)
	}
}

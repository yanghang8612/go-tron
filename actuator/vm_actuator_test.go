package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func newTestContext(t *testing.T, contractType corepb.Transaction_Contract_ContractType, param proto.Message, feeLimit int64) *Context {
	t.Helper()

	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}

	anyParam, err := anypb.New(param)
	if err != nil {
		t.Fatal(err)
	}

	tx := types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			FeeLimit: feeLimit,
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      contractType,
					Parameter: anyParam,
				},
			},
		},
	})

	dynProps := state.NewDynamicProperties()
	dynProps.Set("energy_fee", 100)

	return &Context{
		State:       sdb,
		DynProps:    dynProps,
		Tx:          tx,
		BlockTime:   1000,
		BlockNumber: 1,
		DB:          diskdb,
	}
}

func TestVMActuatorCreateValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	csc := &contractpb.CreateSmartContract{
		OwnerAddress: owner[:],
		NewContract: &contractpb.SmartContract{
			Bytecode: []byte{0x60, 0x00, 0x60, 0x00, 0xf3}, // PUSH1 0 PUSH1 0 RETURN
		},
	}

	ctx := newTestContext(t, corepb.Transaction_Contract_CreateSmartContract, csc, 1_000_000)

	// Owner doesn't exist yet
	act := &VMActuator{}
	err := act.Validate(ctx)
	if err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	// Create owner
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, 10_000_000)

	err = act.Validate(ctx)
	if err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestVMActuatorCreateExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}

	// Simple runtime: PUSH1 0x42 PUSH1 0 MSTORE PUSH1 32 PUSH1 0 RETURN
	runtime := []byte{
		0x60, 0x42, // PUSH1 0x42
		0x60, 0x00, // PUSH1 0
		0x52,       // MSTORE
		0x60, 0x20, // PUSH1 32
		0x60, 0x00, // PUSH1 0
		0xf3,       // RETURN
	}

	// Init code: CODECOPY + RETURN
	runtimeLen := byte(len(runtime))
	initCode := []byte{
		0x60, runtimeLen, // PUSH1 runtimeLen
		0x80,       // DUP1
		0x60, 0x00, // placeholder codeOffset
		0x60, 0x00, // PUSH1 0 (memOffset)
		0x39,       // CODECOPY
		0x60, runtimeLen, // PUSH1 runtimeLen
		0x60, 0x00, // PUSH1 0
		0xf3,       // RETURN
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
	// VMActuator.Execute populates EnergyUsageTotal; the SUN-side Fee/
	// EnergyFee split is filled in by PayEnergyBill in state_processor.
	if result.EnergyUsageTotal <= 0 {
		t.Fatal("expected non-zero EnergyUsageTotal")
	}
	t.Logf("Create energy total: %d", result.EnergyUsageTotal)
}

func TestVMActuatorTriggerValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}

	tsc := &contractpb.TriggerSmartContract{
		OwnerAddress:    owner[:],
		ContractAddress: contractAddr[:],
		Data:            []byte{0x00},
	}

	ctx := newTestContext(t, corepb.Transaction_Contract_TriggerSmartContract, tsc, 1_000_000)

	act := &VMActuator{}

	// Owner doesn't exist
	err := act.Validate(ctx)
	if err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	// Create owner but no contract
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	err = act.Validate(ctx)
	if err == nil {
		t.Fatal("expected error for non-existent contract")
	}

	// Set code on contract address to make it a contract
	ctx.State.SetCode(contractAddr, []byte{0x00}) // STOP
	err = act.Validate(ctx)
	if err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestVMActuatorTriggerExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}

	// Code: PUSH1 0x42 PUSH1 0 MSTORE PUSH1 32 PUSH1 0 RETURN
	code := []byte{
		0x60, 0x42,
		0x60, 0x00,
		0x52,
		0x60, 0x20,
		0x60, 0x00,
		0xf3,
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
	if result.EnergyUsageTotal <= 0 {
		t.Fatal("expected non-zero EnergyUsageTotal")
	}
	t.Logf("Trigger energy total: %d", result.EnergyUsageTotal)
}

func TestVMActuatorCreateExecute_ExtendedResult(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}

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
	// EnergyUsed/EnergyFee are zero until PayEnergyBill runs;
	// EnergyUsageTotal carries the full VM energy consumption out of
	// Execute. See actuator.Result godoc.
	if result.EnergyUsageTotal <= 0 {
		t.Fatal("expected non-zero EnergyUsageTotal")
	}
	if result.EnergyUsed != 0 {
		t.Fatalf("expected EnergyUsed=0 pre-PayEnergyBill, got %d", result.EnergyUsed)
	}
	if result.EnergyFee != 0 {
		t.Fatalf("expected EnergyFee=0 pre-PayEnergyBill, got %d", result.EnergyFee)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1 (SUCCESS), got %d", result.ContractRet)
	}
	if len(result.ContractAddress) == 0 {
		t.Fatal("expected non-empty ContractAddress")
	}
	t.Logf("EnergyUsageTotal=%d, ContractRet=%d, ContractAddr=%x",
		result.EnergyUsageTotal, result.ContractRet, result.ContractAddress)
}

func TestVMActuatorTriggerExecute_ExtendedResult(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}

	// Code: LOG0(0,0) then PUSH1 0x42 PUSH1 0 MSTORE PUSH1 32 PUSH1 0 RETURN
	code := []byte{
		0x60, 0x00, 0x60, 0x00, 0xA0, // LOG0(0,0)
		0x60, 0x42, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3,
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
	if result.EnergyUsageTotal <= 0 {
		t.Fatal("expected non-zero EnergyUsageTotal")
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
	t.Logf("EnergyUsageTotal=%d, Logs=%d, ContractResult=%x",
		result.EnergyUsageTotal, len(result.Logs), result.ContractResult)
}

func TestCreateActuatorVMTypes(t *testing.T) {
	// Verify that CreateActuator returns VMActuator for types 30 and 31
	csc := &contractpb.CreateSmartContract{}
	anyParam, _ := anypb.New(csc)

	tx30 := types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: 30, Parameter: anyParam},
			},
		},
	})
	act30, err := CreateActuator(tx30)
	if err != nil {
		t.Fatalf("CreateActuator type 30: %v", err)
	}
	if _, ok := act30.(*VMActuator); !ok {
		t.Fatalf("expected VMActuator for type 30, got %T", act30)
	}

	tsc := &contractpb.TriggerSmartContract{}
	anyParam2, _ := anypb.New(tsc)
	tx31 := types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: 31, Parameter: anyParam2},
			},
		},
	})
	act31, err := CreateActuator(tx31)
	if err != nil {
		t.Fatalf("CreateActuator type 31: %v", err)
	}
	if _, ok := act31.(*VMActuator); !ok {
		t.Fatalf("expected VMActuator for type 31, got %T", act31)
	}
}

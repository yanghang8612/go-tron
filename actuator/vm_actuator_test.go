package actuator

import (
	"bytes"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	tronrawdb "github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/tronprotocol/go-tron/vm"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestResultContractAddressUsesEmbeddedStorage(t *testing.T) {
	addr := tcommon.Address{0x41, 0x01, 0x02}
	result := new(Result)
	result.setContractAddress(addr)

	if !bytes.Equal(result.ContractAddress, addr[:]) {
		t.Fatalf("contract address = %x, want %x", result.ContractAddress, addr)
	}
	if &result.ContractAddress[0] != &result.contractAddress[0] {
		t.Fatal("contract address does not use Result's embedded backing storage")
	}
	addr[1] = 0xff
	if result.ContractAddress[1] != 0x01 {
		t.Fatal("contract address aliases the caller's temporary address")
	}
	if allocs := testing.AllocsPerRun(1_000, func() {
		result.setContractAddress(addr)
	}); allocs != 0 {
		t.Fatalf("setContractAddress allocations = %v, want 0", allocs)
	}
}

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
		State:         sdb,
		DynProps:      dynProps,
		Tx:            tx,
		BlockTime:     1000,
		PrevBlockTime: 1000,
		HeadSlot:      1,
		HasHeadSlot:   true,
		BlockNumber:   1,
		DB:            diskdb,
	}
}

func enableVM(ctx *Context) {
	ctx.DynProps.SetAllowCreationOfContracts(true)
}

func TestVMActuatorCreateValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	csc := &contractpb.CreateSmartContract{
		OwnerAddress: owner[:],
		NewContract: &contractpb.SmartContract{
			OriginAddress: owner[:],
			Bytecode:      []byte{0x60, 0x00, 0x60, 0x00, 0xf3}, // PUSH1 0 PUSH1 0 RETURN
		},
	}

	ctx := newTestContext(t, corepb.Transaction_Contract_CreateSmartContract, csc, 1_000_000)
	enableVM(ctx)

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
		0xf3, // RETURN
	}

	// Init code: CODECOPY + RETURN
	runtimeLen := byte(len(runtime))
	initCode := []byte{
		0x60, runtimeLen, // PUSH1 runtimeLen
		0x80,       // DUP1
		0x60, 0x00, // placeholder codeOffset
		0x60, 0x00, // PUSH1 0 (memOffset)
		0x39,             // CODECOPY
		0x60, runtimeLen, // PUSH1 runtimeLen
		0x60, 0x00, // PUSH1 0
		0xf3, // RETURN
	}
	initCode[4] = byte(len(initCode))
	bytecode := append(initCode, runtime...)

	csc := &contractpb.CreateSmartContract{
		OwnerAddress: owner[:],
		NewContract: &contractpb.SmartContract{
			OriginAddress: owner[:],
			Bytecode:      bytecode,
			Name:          "TestContract",
			Abi: &contractpb.SmartContract_ABI{Entrys: []*contractpb.SmartContract_ABI_Entry{{
				Name: "ping", Type: contractpb.SmartContract_ABI_Entry_Function,
			}}},
		},
	}

	ctx := newTestContext(t, corepb.Transaction_Contract_CreateSmartContract, csc, 10_000_000)
	enableVM(ctx)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, 100_000_000)
	ctx.ResultSink = &Result{
		ShieldedTransactionFee: 99,
		CancelUnfreezeV2Amount: map[string]int64{"ENERGY": 88},
	}

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
	if result.VMRawEnergyUsage != result.EnergyUsageTotal {
		t.Fatalf("pre-dynamic raw energy = %d, want billed %d", result.VMRawEnergyUsage, result.EnergyUsageTotal)
	}
	if result.VMExecutionDuration <= 0 {
		t.Fatalf("VM execution duration = %s, want positive", result.VMExecutionDuration)
	}
	if result != ctx.ResultSink {
		t.Fatal("create did not return the block-local result sink")
	}
	if result.ShieldedTransactionFee != 0 || result.CancelUnfreezeV2Amount != nil {
		t.Fatalf("create result sink retained stale fields: %#v", result)
	}
	contractAddr := generateContractAddress(ctx.Tx, owner)
	if got := ctx.State.ReadContractABI(contractAddr); !proto.Equal(got, csc.NewContract.Abi) {
		t.Fatalf("dedicated ABI = %v, want %v", got, csc.NewContract.Abi)
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
	enableVM(ctx)

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

	// Set contract metadata to make it a smart contract.
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:   owner[:],
		ContractAddress: contractAddr[:],
	})
	ctx.State.SetCode(contractAddr, []byte{0x00}) // STOP
	err = act.Validate(ctx)
	if err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestVMActuatorTriggerRejectsMissingRuntimeCode(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}
	code := []byte{0x60, 0x01, 0x00}
	codeHash := tcommon.Keccak256(code)
	tsc := &contractpb.TriggerSmartContract{
		OwnerAddress:    owner[:],
		ContractAddress: contractAddr[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_TriggerSmartContract, tsc, 1_000_000)
	enableVM(ctx)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:   owner[:],
		ContractAddress: contractAddr[:],
	})
	ctx.State.SetCode(contractAddr, code)
	if _, err := ctx.State.Commit(); err != nil {
		t.Fatalf("commit contract: %v", err)
	}
	if err := tronrawdb.DeleteStateCode(ctx.DB, codeHash); err != nil {
		t.Fatalf("delete runtime code fixture: %v", err)
	}
	disk, ok := ctx.DB.(ethdb.Database)
	if !ok {
		t.Fatalf("test DB %T does not implement ethdb.Database", ctx.DB)
	}
	reloaded, err := state.New(tcommon.Hash{}, state.NewDatabase(disk))
	if err != nil {
		t.Fatalf("reload state: %v", err)
	}
	ctx.State = reloaded

	err = (&VMActuator{}).Validate(ctx)
	if err == nil || !strings.Contains(err.Error(), "contract runtime code unavailable") || !strings.Contains(err.Error(), codeHash.Hex()) {
		t.Fatalf("Validate error = %v, want missing runtime code with hash", err)
	}
	if _, err := (&VMActuator{}).Execute(ctx); err == nil || !strings.Contains(err.Error(), "contract runtime code unavailable") {
		t.Fatalf("Execute error = %v, want missing runtime code", err)
	}
}

func TestVMActuatorTriggerAllowsRecordedEmptyRuntime(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}
	tsc := &contractpb.TriggerSmartContract{
		OwnerAddress:    owner[:],
		ContractAddress: contractAddr[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_TriggerSmartContract, tsc, 1_000_000)
	enableVM(ctx)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:   owner[:],
		ContractAddress: contractAddr[:],
	})
	ctx.State.SetCode(contractAddr, nil)
	if err := (&VMActuator{}).Validate(ctx); err != nil {
		t.Fatalf("Validate empty runtime: %v", err)
	}
}

func TestVMActuatorTriggerValidate_CustomEnergyLimitForkBlock(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}

	tsc := &contractpb.TriggerSmartContract{
		OwnerAddress:    owner[:],
		ContractAddress: contractAddr[:],
		CallTokenValue:  -1,
	}

	ctx := newTestContext(t, corepb.Transaction_Contract_TriggerSmartContract, tsc, 1_000_000)
	enableVM(ctx)
	ctx.DynProps.SetAllowTvmTransferTrc10(true)
	ctx.DynProps.SetLatestBlockHeaderNumber(0)
	ctx.EnergyLimitForkBlockNum = 0
	ctx.HasEnergyLimitForkBlockNum = true
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, 100_000_000)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:   owner[:],
		ContractAddress: contractAddr[:],
	})
	ctx.State.SetCode(contractAddr, []byte{0x00})

	err := (&VMActuator{}).Validate(ctx)
	if err == nil || err.Error() != "tokenValue must be >= 0" {
		t.Fatalf("Validate error: got %v, want tokenValue must be >= 0", err)
	}
}

func TestVMActuatorTriggerEnergyLimit_FixRatioCapsByBalanceAndOrigin(t *testing.T) {
	caller := tcommon.Address{0x41, 0x10}
	origin := tcommon.Address{0x41, 0x20}
	contractAddr := tcommon.Address{0x41, 0x30}

	tsc := &contractpb.TriggerSmartContract{
		OwnerAddress:    caller[:],
		ContractAddress: contractAddr[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_TriggerSmartContract, tsc, 1_000_000_000)
	enableVM(ctx)
	ctx.DynProps.SetLatestBlockHeaderNumber(blockNumForEnergyLimit)
	ctx.DynProps.SetAllowTvmFreeze(true)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.DynProps.SetTotalEnergyWeight(1_000_000_000_000)
	ctx.DynProps.Set("total_energy_current_limit", 1_000_000_000_000)

	ctx.State.CreateAccount(caller, corepb.AccountType_Normal)
	ctx.State.AddBalance(caller, 100_000_000)
	ctx.State.AddFreezeV2(caller, corepb.ResourceCode_ENERGY, 3_253_937_000_000)

	ctx.State.CreateAccount(origin, corepb.AccountType_Normal)
	ctx.State.AddFreezeV2(origin, corepb.ResourceCode_ENERGY, 20_000_000_000)

	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:              origin[:],
		ContractAddress:            contractAddr[:],
		ConsumeUserResourcePercent: 0,
		OriginEnergyLimit:          10_000,
	})

	result := &Result{}
	got, err := triggerEnergyLimit(ctx, caller, contractAddr, ctx.Tx.FeeLimit(), 0, result)
	if err != nil {
		t.Fatalf("triggerEnergyLimit returned error: %v", err)
	}
	const want = int64(4_263_937)
	if got != want {
		t.Fatalf("triggerEnergyLimit = %d, want %d", got, want)
	}
	if !result.HasCallerEnergyLeft || result.CallerEnergyLeft != 3_253_937 {
		t.Fatalf("caller energy left = (%v,%d), want (true,3253937)", result.HasCallerEnergyLeft, result.CallerEnergyLeft)
	}
	if !result.HasOriginEnergyLeft || result.OriginEnergyLeft != 20_000 {
		t.Fatalf("origin energy left = (%v,%d), want (true,20000)", result.HasOriginEnergyLeft, result.OriginEnergyLeft)
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
	enableVM(ctx)
	ctx.DynProps.SetAllowDynamicEnergy(true)
	ctx.DynProps.SetCurrentCycleNumber(10)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, 100_000_000)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:   owner[:],
		ContractAddress: contractAddr[:],
	})
	ctx.State.SetCode(contractAddr, code)
	contractState := types.NewContractState(10)
	contractState.SetEnergyFactor(5000) // effective multiplier 1.5x
	if err := ctx.State.WriteContractState(contractAddr, contractState); err != nil {
		t.Fatal(err)
	}
	ctx.ResultSink = &Result{
		ShieldedTransactionFee: 99,
		CancelUnfreezeV2Amount: map[string]int64{"ENERGY": 88},
	}

	act := &VMActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.EnergyUsageTotal <= 0 {
		t.Fatal("expected non-zero EnergyUsageTotal")
	}
	if result.VMRawEnergyUsage <= 0 || result.VMRawEnergyUsage >= result.EnergyUsageTotal {
		t.Fatalf("dynamic raw/billed energy = %d/%d, want 0 < raw < billed", result.VMRawEnergyUsage, result.EnergyUsageTotal)
	}
	if result.VMExecutionDuration <= 0 {
		t.Fatalf("VM execution duration = %s, want positive", result.VMExecutionDuration)
	}
	if result != ctx.ResultSink {
		t.Fatal("trigger did not return the block-local result sink")
	}
	if result.ShieldedTransactionFee != 0 || result.CancelUnfreezeV2Amount != nil {
		t.Fatalf("trigger result sink retained stale fields: %#v", result)
	}
	t.Logf("Trigger energy total: %d", result.EnergyUsageTotal)
}

func TestVMActuatorTriggerExecuteCarriesDynamicEnergyPenalty(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}
	code := []byte{
		byte(vm.PUSH1), 0x42,
		byte(vm.PUSH1), 0x00,
		byte(vm.MSTORE),
		byte(vm.STOP),
	}
	tsc := &contractpb.TriggerSmartContract{
		OwnerAddress:    owner[:],
		ContractAddress: contractAddr[:],
	}

	ctx := newTestContext(t, corepb.Transaction_Contract_TriggerSmartContract, tsc, 10_000_000)
	enableVM(ctx)
	ctx.DynProps.SetAllowDynamicEnergy(true)
	ctx.DynProps.SetCurrentCycleNumber(10)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, 100_000_000)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:   owner[:],
		ContractAddress: contractAddr[:],
	})
	ctx.State.SetCode(contractAddr, code)
	contractState := types.NewContractState(10)
	contractState.SetEnergyFactor(5_000) // effective multiplier 1.5x
	if err := ctx.State.WriteContractState(contractAddr, contractState); err != nil {
		t.Fatal(err)
	}

	result, err := new(VMActuator).Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.EnergyPenaltyTotal <= 0 {
		t.Fatalf("dynamic energy penalty total = %d, want positive", result.EnergyPenaltyTotal)
	}
	if result.EnergyPenaltyTotal >= result.EnergyUsageTotal {
		t.Fatalf("penalty/total = %d/%d, want penalty smaller than total", result.EnergyPenaltyTotal, result.EnergyUsageTotal)
	}
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
	enableVM(ctx)
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
	expectedAddress := generateContractAddress(ctx.Tx, owner)
	if string(result.ContractAddress) != string(expectedAddress[:]) {
		t.Fatalf("contract address: got %x, want %x", result.ContractAddress, expectedAddress[:])
	}
	t.Logf("EnergyUsageTotal=%d, ContractRet=%d, ContractAddr=%x",
		result.EnergyUsageTotal, result.ContractRet, result.ContractAddress)
}

func TestVMActuatorCreateInvalidCodeKeepsJavaTronReceiptFields(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	bytecode := []byte{
		byte(vm.PUSH1), 1,
		byte(vm.PUSH1), 12,
		byte(vm.PUSH1), 0,
		byte(vm.CODECOPY),
		byte(vm.PUSH1), 1,
		byte(vm.PUSH1), 0,
		byte(vm.RETURN),
		0xEF,
	}
	csc := &contractpb.CreateSmartContract{
		OwnerAddress: owner[:],
		NewContract: &contractpb.SmartContract{
			OriginAddress: owner[:],
			Bytecode:      bytecode,
			Name:          "BadCode",
		},
	}

	ctx := newTestContext(t, corepb.Transaction_Contract_CreateSmartContract, csc, 10_000_000)
	enableVM(ctx)
	ctx.DynProps.SetAllowTvmConstantinople(true)
	ctx.DynProps.SetAllowTvmLondon(true)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, 100_000_000)

	result, err := (&VMActuator{}).Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != int32(corepb.Transaction_Result_INVALID_CODE) {
		t.Fatalf("contract ret: got %d, want INVALID_CODE", result.ContractRet)
	}
	if string(result.ContractResult) != string([]byte{0xEF}) {
		t.Fatalf("contract result: got %x, want ef", result.ContractResult)
	}
	wantAddress := generateContractAddress(ctx.Tx, owner)
	if string(result.ContractAddress) != string(wantAddress[:]) {
		t.Fatalf("contract address: got %x, want %x", result.ContractAddress, wantAddress[:])
	}
	if got := string(result.ResMessage); got != vm.ErrInvalidCode.Error() {
		t.Fatalf("resMessage: got %q, want %q", got, vm.ErrInvalidCode.Error())
	}
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
	enableVM(ctx)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, 100_000_000)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:   owner[:],
		ContractAddress: contractAddr[:],
	})
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
	if string(result.ContractAddress) != string(contractAddr[:]) {
		t.Fatalf("contract address: got %x, want %x", result.ContractAddress, contractAddr[:])
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

func TestVMActuatorTriggerInvalidOpcodeReturnsIllegalOperation(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}

	tsc := &contractpb.TriggerSmartContract{
		OwnerAddress:    owner[:],
		ContractAddress: contractAddr[:],
	}

	ctx := newTestContext(t, corepb.Transaction_Contract_TriggerSmartContract, tsc, 10_000_000)
	enableVM(ctx)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, 100_000_000)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:   owner[:],
		ContractAddress: contractAddr[:],
	})
	ctx.State.SetCode(contractAddr, []byte{0xfe})

	result, err := (&VMActuator{}).Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != int32(corepb.Transaction_Result_ILLEGAL_OPERATION) {
		t.Fatalf("contract ret: got %d, want ILLEGAL_OPERATION", result.ContractRet)
	}
	if got := string(result.ResMessage); got != "Invalid operation code: opCode[fe];" {
		t.Fatalf("resMessage: got %q", got)
	}
}

func TestVMActuatorTriggerReplayOutOfTime(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}

	code := []byte{
		0x60, 0x42, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3,
	}

	tsc := &contractpb.TriggerSmartContract{
		OwnerAddress:    owner[:],
		ContractAddress: contractAddr[:],
	}

	ctx := newTestContext(t, corepb.Transaction_Contract_TriggerSmartContract, tsc, 10_000_000)
	ctx.Tx.Proto().Ret = []*corepb.Transaction_Result{{
		ContractRet: corepb.Transaction_Result_OUT_OF_TIME,
	}}
	ctx.TrustTransactionRet = true
	enableVM(ctx)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, 100_000_000)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:   owner[:],
		ContractAddress: contractAddr[:],
	})
	ctx.State.SetCode(contractAddr, code)

	result, err := (&VMActuator{}).Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != int32(corepb.Transaction_Result_OUT_OF_TIME) {
		t.Fatalf("ContractRet: got %d, want OUT_OF_TIME", result.ContractRet)
	}
	if result.EnergyUsageTotal != 100_000 {
		t.Fatalf("EnergyUsageTotal: got %d, want full energy limit 100000", result.EnergyUsageTotal)
	}
	if !result.ContractResultPresent || len(result.ContractResult) != 0 {
		t.Fatalf("contract result: present=%v len=%d, want present empty", result.ContractResultPresent, len(result.ContractResult))
	}
	if got := string(result.ResMessage); got != "Already Time Out" {
		t.Fatalf("resMessage: got %q", got)
	}
	if len(result.Logs) != 0 {
		t.Fatalf("replay OUT_OF_TIME must not execute contract code, got %d logs", len(result.Logs))
	}
	if result.VMExecutionDuration != 0 || result.VMRawEnergyUsage != 0 {
		t.Fatalf("replay OUT_OF_TIME execution telemetry = %s/%d, want zero", result.VMExecutionDuration, result.VMRawEnergyUsage)
	}
}

func TestContractRetFromTransferFailed(t *testing.T) {
	if got := contractRetFromError(vm.ErrAlreadyTimeOut); got != int32(corepb.Transaction_Result_OUT_OF_TIME) {
		t.Fatalf("already timeout ret: got %d", got)
	}
	if got := contractRetFromError(vm.ErrOutOfMemory); got != int32(corepb.Transaction_Result_OUT_OF_MEMORY) {
		t.Fatalf("out of memory ret: got %d", got)
	}
	if got := contractRetFromError(vm.ErrPrecompiledContract); got != int32(corepb.Transaction_Result_PRECOMPILED_CONTRACT) {
		t.Fatalf("precompiled contract ret: got %d", got)
	}
	if got := contractRetFromError(vm.ErrJVMStackOverflow); got != int32(corepb.Transaction_Result_JVM_STACK_OVER_FLOW) {
		t.Fatalf("JVM stack overflow ret: got %d", got)
	}
	if got := contractRetFromError(vm.ErrWriteProtection); got != int32(corepb.Transaction_Result_UNKNOWN) {
		t.Fatalf("static call modification ret: got %d", got)
	}
	if got := contractRetFromError(vm.ErrValidateForSmartContract); got != int32(corepb.Transaction_Result_UNKNOWN) {
		t.Fatalf("legacy smart-contract transfer validation ret: got %d, want UNKNOWN(13)", got)
	}
	if got := contractRetFromError(vm.ErrLegacyCreateEmptyCode); got != int32(corepb.Transaction_Result_UNKNOWN) {
		t.Fatalf("legacy empty-code CREATE ret: got %d, want UNKNOWN(13)", got)
	}
	if got := contractRetFromError(vm.ErrSelfDestructTransferFailure); got != int32(corepb.Transaction_Result_UNKNOWN) {
		t.Fatalf("legacy SELFDESTRUCT transfer ret: got %d, want UNKNOWN(13)", got)
	}
	if got := contractRetFromError(vm.ErrTransferFailed); got != int32(corepb.Transaction_Result_TRANSFER_FAILED) {
		t.Fatalf("TRX transfer failed ret: got %d", got)
	}
	if got := contractRetFromError(vm.ErrTokenTransferFailed); got != int32(corepb.Transaction_Result_TRANSFER_FAILED) {
		t.Fatalf("TRC10 transfer failed ret: got %d", got)
	}
	if got := contractRetFromError(vm.ErrEndowmentOutOfRange); got != int32(corepb.Transaction_Result_TRANSFER_FAILED) {
		t.Fatalf("endowment overflow ret: got %d", got)
	}
	if got := contractRetFromError(vm.ErrLegacyEndowmentOutOfRange); got != int32(corepb.Transaction_Result_UNKNOWN) {
		t.Fatalf("legacy endowment overflow ret: got %d, want UNKNOWN(13)", got)
	}
	if got := contractRetFromError(vm.ErrInvalidTokenIDTransfer); got != int32(corepb.Transaction_Result_TRANSFER_FAILED) {
		t.Fatalf("transfer invalid token id ret: got %d", got)
	}
	if got := contractRetFromError(vm.ErrInvalidTokenID); got != int32(corepb.Transaction_Result_UNKNOWN) {
		t.Fatalf("plain invalid token id ret: got %d", got)
	}
	// ValidateMultiSign short/malformed input → java raises an uncaught
	// RuntimeException → RuntimeImpl.setResultCode falls through to UNKNOWN(13).
	// Pin the mapping so a future reorder of the switch can't silently demote it.
	if got := contractRetFromError(vm.ErrPrecompileUnknown); got != int32(corepb.Transaction_Result_UNKNOWN) {
		t.Fatalf("precompile uncaught-exception ret: got %d, want UNKNOWN(13)", got)
	}
	if got := string(runtimeMessageFromError(vm.ErrTokenTransferFailed)); got != "transfer trc10 failed: Cannot transfer asset to yourself." {
		t.Fatalf("TRC10 transfer failed message: got %q", got)
	}
	if got := string(runtimeMessageFromError(vm.ErrEndowmentOutOfRange)); got != "endowment out of long range" {
		t.Fatalf("endowment overflow message: got %q", got)
	}
	if got := string(runtimeMessageFromError(vm.ErrLegacyEndowmentOutOfRange)); got != "BigInteger out of long range" {
		t.Fatalf("legacy endowment overflow message: got %q", got)
	}
	if got := string(runtimeMessageFromError(vm.ErrValidateForSmartContract)); got != "validateForSmartContract failure" {
		t.Fatalf("legacy smart-contract transfer validation message: got %q", got)
	}
	if got := string(runtimeMessageFromError(vm.ErrLegacyCreateEmptyCode)); got != "Unknown Exception" {
		t.Fatalf("legacy empty-code CREATE message: got %q", got)
	}
	if got := string(runtimeMessageFromError(vm.ErrSelfDestructTransferFailure)); got != "transfer failure" {
		t.Fatalf("legacy SELFDESTRUCT transfer message: got %q", got)
	}
}

var benchmarkRuntimeMessage []byte

func BenchmarkRuntimeMessageFromRevert(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchmarkRuntimeMessage = runtimeMessageFromError(vm.ErrExecutionReverted)
	}
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

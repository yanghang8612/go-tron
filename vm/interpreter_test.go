package vm

import (
	"bytes"
	"errors"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	tronrawdb "github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func newTestEVM(t *testing.T) *TVM {
	return newTestEVMWithConfig(t, TVMConfig{})
}

func newTestEVMWithConfig(t *testing.T, cfg TVMConfig) *TVM {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	return NewTVM(sdb, nil, tcommon.Address{}, 1, 1000, tcommon.Address{}, 1, cfg)
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
	if !errors.Is(err, ErrOutOfEnergy) {
		t.Fatalf("expected ErrOutOfEnergy, got %v", err)
	}
	want := "Not enough energy for 'PUSH1' operation executing: curInvokeEnergyLimit[2], curOpEnergy[3], usedEnergy[0]"
	if err.Error() != want {
		t.Fatalf("out of energy message: got %q, want %q", err.Error(), want)
	}
}

func TestInterpreterInvalidJump(t *testing.T) {
	evm := newTestEVM(t)

	code := []byte{byte(PUSH1), 0x10, byte(JUMP)}
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	_, err := evm.interpreter.Run(contract)
	if !errors.Is(err, ErrInvalidJump) {
		t.Fatalf("expected ErrInvalidJump, got %v", err)
	}
	if got := err.Error(); got != "Operation with pc isn't 'JUMPDEST': PC[16];" {
		t.Fatalf("invalid jump message: got %q", got)
	}
}

func TestInterpreterInvalidOpcodeMessageMatchesJava(t *testing.T) {
	evm := newTestEVM(t)

	code := []byte{0xfe}
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	_, err := evm.interpreter.Run(contract)
	if !errors.Is(err, ErrInvalidOpCode) {
		t.Fatalf("expected ErrInvalidOpCode, got %v", err)
	}
	if got := err.Error(); got != "Invalid operation code: opCode[fe];" {
		t.Fatalf("invalid opcode message: got %q", got)
	}
}

func TestInterpreterStackUnderflowMessageMatchesJava(t *testing.T) {
	evm := newTestEVM(t)

	code := []byte{byte(POP)}
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	_, err := evm.interpreter.Run(contract)
	if !errors.Is(err, ErrStackUnderflow) {
		t.Fatalf("expected ErrStackUnderflow, got %v", err)
	}
	if got := err.Error(); got != "Expected stack size 1 but actual 0;" {
		t.Fatalf("stack underflow message: got %q", got)
	}
}

func TestInterpreterStackOverflowPrecedesExecution(t *testing.T) {
	tests := []struct {
		name string
		op   OpCode
	}{
		{name: "push", op: PUSH1},
		{name: "dup", op: DUP1},
		{name: "address", op: ADDRESS},
		{name: "calldatasize", op: CALLDATASIZE},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evm := newTestEVM(t)
			code := make([]byte, 0, stackLimit*2+3)
			for i := 0; i < stackLimit; i++ {
				code = append(code, byte(PUSH1), 0x00)
			}
			if tt.op == PUSH1 {
				code = append(code, byte(PUSH1), 0x10, byte(JUMP))
			} else {
				code = append(code, byte(tt.op))
			}

			const energyLimit = uint64(stackLimit)*EnergyVeryLow + 1000
			contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, energyLimit)
			contract.SetCode(tcommon.Address{0x41, 0x02}, code)

			_, err := evm.interpreter.Run(contract)
			if !errors.Is(err, ErrStackOverflow) {
				t.Fatalf("expected ErrStackOverflow, got %v", err)
			}
			if got := err.Error(); got != "Expected: overflow 1024 elements stack limit" {
				t.Fatalf("stack overflow message: got %q", got)
			}
			wantEnergy := energyLimit - uint64(stackLimit)*EnergyVeryLow
			if contract.Energy != wantEnergy {
				t.Fatalf("overflow opcode charged energy: got remaining %d, want %d", contract.Energy, wantEnergy)
			}
		})
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

func TestReturnDataCopyRejectsOffsetSizeOverflow(t *testing.T) {
	evm := newTestEVM(t)
	evm.interpreter.returnData = []byte{0x01, 0x02, 0x03, 0x04}
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100_000)
	stack := newStack()
	stack.push(uint256.NewInt(2)) // length
	var dataOffset uint256.Int
	dataOffset.SetUint64(^uint64(0))
	stack.push(&dataOffset)
	stack.push(uint256.NewInt(0)) // memory offset

	_, err := opReturnDataCopy(nil, evm.interpreter, contract, newMemory(), stack)
	if !errors.Is(err, ErrReturnDataOutOfBounds) {
		t.Fatalf("RETURNDATACOPY error: got %v, want %v", err, ErrReturnDataOutOfBounds)
	}
}

func TestReturnDataCopyRejectsNonUint64OffsetEvenWithZeroLength(t *testing.T) {
	evm := newTestEVM(t)
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100_000)
	stack := newStack()
	stack.push(uint256.NewInt(0)) // length
	var dataOffset uint256.Int
	dataOffset.SetBytes([]byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	stack.push(&dataOffset)
	stack.push(uint256.NewInt(0)) // memory offset

	_, err := opReturnDataCopy(nil, evm.interpreter, contract, newMemory(), stack)
	if !errors.Is(err, ErrReturnDataOutOfBounds) {
		t.Fatalf("RETURNDATACOPY error: got %v, want %v", err, ErrReturnDataOutOfBounds)
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
	if got := err.Error(); got != "Attempt to call a state modifying opcode inside STATICCALL" {
		t.Fatalf("write protection message: got %q", got)
	}
	evm.interpreter.readOnly = false
}

func TestInterpreterStaticCallAllowsZeroValueCall(t *testing.T) {
	evm := newTestEVM(t)

	code := []byte{
		byte(PUSH1), 0x00, // out size
		byte(PUSH1), 0x00, // out offset
		byte(PUSH1), 0x00, // in size
		byte(PUSH1), 0x00, // in offset
		byte(PUSH1), 0x00, // value
		byte(PUSH1), 0x99, // address
		byte(PUSH2), 0x03, 0xe8, // energy
		byte(CALL),
		byte(STOP),
	}
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	evm.interpreter.readOnly = true
	_, err := evm.interpreter.Run(contract)
	if err != nil {
		t.Fatalf("static CALL with zero value should be allowed, got %v", err)
	}
	evm.interpreter.readOnly = false
}

func TestInterpreterStaticCallRejectsNonZeroValueCall(t *testing.T) {
	evm := newTestEVM(t)

	code := []byte{
		byte(PUSH1), 0x00, // out size
		byte(PUSH1), 0x00, // out offset
		byte(PUSH1), 0x00, // in size
		byte(PUSH1), 0x00, // in offset
		byte(PUSH1), 0x01, // value
		byte(PUSH1), 0x99, // address
		byte(PUSH2), 0x03, 0xe8, // energy
		byte(CALL),
		byte(STOP),
	}
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	evm.interpreter.readOnly = true
	_, err := evm.interpreter.Run(contract)
	if err != ErrWriteProtection {
		t.Fatalf("expected ErrWriteProtection, got %v", err)
	}
	evm.interpreter.readOnly = false
}

func TestSstoreCostUsesStorageRowExistence(t *testing.T) {
	evm := newTestEVM(t)

	code := []byte{
		byte(PUSH1), 0x00,
		byte(PUSH1), 0x00,
		byte(SSTORE),
		byte(PUSH1), 0x09,
		byte(PUSH1), 0x00,
		byte(SSTORE),
		byte(STOP),
	}
	const energyLimit = 100_000
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, energyLimit)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	if _, err := evm.interpreter.Run(contract); err != nil {
		t.Fatalf("run error: %v", err)
	}
	got := energyLimit - contract.Energy
	want := uint64(4*EnergyVeryLow + 2*EnergySstoreReset)
	if got != want {
		t.Fatalf("SSTORE energy: got %d, want %d", got, want)
	}
}

func TestSstoreCostAfterTransactionBoundaryTreatsZeroRowAsMissing(t *testing.T) {
	evm := newTestEVM(t)
	addr := tcommon.Address{0x41, 0x02}

	zeroCode := []byte{
		byte(PUSH1), 0x00,
		byte(PUSH1), 0x00,
		byte(SSTORE),
		byte(STOP),
	}
	contract := NewContract(tcommon.Address{0x41, 0x01}, addr, 0, 100000)
	contract.SetCode(addr, zeroCode)
	if _, err := evm.interpreter.Run(contract); err != nil {
		t.Fatalf("zero SSTORE run error: %v", err)
	}

	evm.StateDB.FinalizeTransaction()

	setCode := []byte{
		byte(PUSH1), 0x09,
		byte(PUSH1), 0x00,
		byte(SSTORE),
		byte(STOP),
	}
	contract = NewContract(tcommon.Address{0x41, 0x01}, addr, 0, 100000)
	contract.SetCode(addr, setCode)
	if _, err := evm.interpreter.Run(contract); err != nil {
		t.Fatalf("set SSTORE run error: %v", err)
	}
	got := uint64(100000 - contract.Energy)
	want := uint64(2*EnergyVeryLow + EnergySstoreSet)
	if got != want {
		t.Fatalf("SSTORE after tx boundary energy: got %d, want %d", got, want)
	}
}

func TestInterpreterChainIDRequiresIstanbul(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	// Istanbul NOT enabled (TVMConfig{} has all false)
	evm := NewTVM(sdb, nil, tcommon.Address{}, 1, 1000, tcommon.Address{}, 1, TVMConfig{})

	// CHAINID PUSH1 0 MSTORE PUSH1 32 PUSH1 0 RETURN
	code := []byte{byte(CHAINID), byte(PUSH1), 0x00, byte(MSTORE), byte(PUSH1), 0x20, byte(PUSH1), 0x00, byte(RETURN)}
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	_, err = evm.interpreter.Run(contract)
	if !errors.Is(err, ErrInvalidOpCode) {
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
	evm := NewTVM(sdb, nil, tcommon.Address{}, 1, 1000, tcommon.Address{}, 1, TVMConfig{Istanbul: true})

	// CHAINID PUSH1 0 MSTORE PUSH1 32 PUSH1 0 RETURN
	code := []byte{byte(CHAINID), byte(PUSH1), 0x00, byte(MSTORE), byte(PUSH1), 0x20, byte(PUSH1), 0x00, byte(RETURN)}
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	_, err = evm.interpreter.Run(contract)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFreezeStaticCallWriteProtectionStartsWithVoteProposal(t *testing.T) {
	run := func(cfg TVMConfig) error {
		tvm, _, _ := newTestTVMForCreate(t, cfg, nil)
		tvm.interpreter.readOnly = true
		stack := newStack()
		stack.push(uint256.NewInt(0)) // receiver
		stack.push(uint256.NewInt(0)) // amount: invalid, so pre-vote path returns 0 before mutating state
		stack.push(uint256.NewInt(0)) // resource
		contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
		_, err := opFreeze(nil, tvm.interpreter, contract, newMemory(), stack)
		return err
	}

	if err := run(TVMConfig{Freeze: true}); err != nil {
		t.Fatalf("pre-vote static FREEZE: got %v, want nil", err)
	}
	if err := run(TVMConfig{Freeze: true, Vote: true}); err != ErrWriteProtection {
		t.Fatalf("post-vote static FREEZE: got %v, want %v", err, ErrWriteProtection)
	}
}

func TestChainIDReturnsFullGenesisBlockIDBeforeOptimizedProposal(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	genesis := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     0,
				Timestamp:  123,
				ParentHash: []byte("parent-hash-for-chainid-test"),
			},
		},
	})
	if err := tronrawdb.WriteBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	evm := NewTVM(sdb, nil, tcommon.Address{}, 1, 1000, tcommon.Address{}, 0x01020304, TVMConfig{Istanbul: true})
	evm.SetDB(diskdb)

	code := []byte{byte(CHAINID), byte(PUSH1), 0x00, byte(MSTORE), byte(PUSH1), 0x20, byte(PUSH1), 0x00, byte(RETURN)}
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	ret, err := evm.interpreter.Run(contract)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got, want := ret, genesis.Hash().Bytes(); !bytes.Equal(got, want) {
		t.Fatalf("chainid full block id:\n got  %x\n want %x", got, want)
	}
}

func TestChainIDOptimizedProposalReturnsLowFourBytes(t *testing.T) {
	evm := newTestEVMWithConfig(t, TVMConfig{Istanbul: true, OptimizedReturnValueOfChainId: true})
	evm.ChainID = 0x01020304

	code := []byte{byte(CHAINID), byte(PUSH1), 0x00, byte(MSTORE), byte(PUSH1), 0x20, byte(PUSH1), 0x00, byte(RETURN)}
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	ret, err := evm.interpreter.Run(contract)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	got := new(uint256.Int).SetBytes(ret).Uint64()
	if got != 0x01020304 {
		t.Fatalf("chainid optimized: got %#x want %#x", got, uint64(0x01020304))
	}
}

func TestTimestampOpcodeReturnsSeconds(t *testing.T) {
	evm := newTestEVM(t)
	evm.Timestamp = 1_779_252_429_000

	code := []byte{byte(TIMESTAMP), byte(PUSH1), 0x00, byte(MSTORE), byte(PUSH1), 0x20, byte(PUSH1), 0x00, byte(RETURN)}
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	ret, err := evm.interpreter.Run(contract)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	got := new(uint256.Int).SetBytes(ret).Uint64()
	if got != 1_779_252_429 {
		t.Fatalf("timestamp: got %d, want seconds", got)
	}
}

func TestGasPriceOpcodeReturnsEnergyFeeForVersionOneCompatibilityContract(t *testing.T) {
	evm, sdb, dp := newTestTVMForCreate(t, TVMConfig{Compatibility: true}, nil)
	dp.Set("energy_fee", 420)
	addr := tcommon.Address{0x41, 0x02}
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.SetContract(addr, &contractpb.SmartContract{
		ContractAddress:            addr.Bytes(),
		ConsumeUserResourcePercent: 100,
		Version:                    1,
	})

	code := []byte{byte(GASPRICE), byte(PUSH1), 0x00, byte(MSTORE), byte(PUSH1), 0x20, byte(PUSH1), 0x00, byte(RETURN)}
	contract := NewContract(tcommon.Address{0x41, 0x01}, addr, 0, 100000)
	contract.Version = 1
	contract.SetCode(addr, code)

	ret, err := evm.interpreter.Run(contract)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	got := new(uint256.Int).SetBytes(ret).Uint64()
	if got != 420 {
		t.Fatalf("gasprice: got %d, want energy fee", got)
	}
}

func TestLegacyContractCallForwardsAllRemainingEnergy(t *testing.T) {
	evm, sdb, _ := newTestTVMForCreate(t, TVMConfig{Compatibility: true}, nil)
	parent := tcommon.Address{0x41, 0x21}
	child := tcommon.Address{0x41, 0x22}
	sdb.GetOrCreateAccount(parent)
	sdb.GetOrCreateAccount(child)
	sdb.SetContract(parent, &contractpb.SmartContract{ContractAddress: parent.Bytes(), Version: 0})
	sdb.SetContract(child, &contractpb.SmartContract{ContractAddress: child.Bytes(), Version: 0})
	sdb.SetCode(child, []byte{0xfe})

	code := []byte{
		byte(PUSH1), 0x00, // out size
		byte(PUSH1), 0x00, // out offset
		byte(PUSH1), 0x00, // in size
		byte(PUSH1), 0x00, // in offset
		byte(PUSH1), 0x00, // value
		byte(PUSH20),
	}
	code = append(code, child[1:]...)
	code = append(code,
		byte(PUSH2), 0xff, 0xff, // requested energy exceeds remaining
		byte(CALL),
		byte(ISZERO),
		byte(STOP),
	)
	contract := NewContract(tcommon.Address{0x41, 0x01}, parent, 0, 1_000)
	contract.Version = 0
	contract.SetCode(parent, code)

	_, err := evm.interpreter.Run(contract)
	if !errors.Is(err, ErrOutOfEnergy) {
		t.Fatalf("legacy contract should spend all remaining energy in CALL, got %v", err)
	}
}

func TestVersionOneContractCallKeepsSixtyFourthEnergy(t *testing.T) {
	evm, sdb, _ := newTestTVMForCreate(t, TVMConfig{Compatibility: true}, nil)
	parent := tcommon.Address{0x41, 0x21}
	child := tcommon.Address{0x41, 0x22}
	sdb.GetOrCreateAccount(parent)
	sdb.GetOrCreateAccount(child)
	sdb.SetContract(parent, &contractpb.SmartContract{ContractAddress: parent.Bytes(), Version: 1})
	sdb.SetContract(child, &contractpb.SmartContract{ContractAddress: child.Bytes(), Version: 1})
	sdb.SetCode(child, []byte{0xfe})

	code := []byte{
		byte(PUSH1), 0x00, // out size
		byte(PUSH1), 0x00, // out offset
		byte(PUSH1), 0x00, // in size
		byte(PUSH1), 0x00, // in offset
		byte(PUSH1), 0x00, // value
		byte(PUSH20),
	}
	code = append(code, child[1:]...)
	code = append(code,
		byte(PUSH2), 0xff, 0xff, // requested energy exceeds remaining
		byte(CALL),
		byte(ISZERO),
		byte(STOP),
	)
	contract := NewContract(tcommon.Address{0x41, 0x01}, parent, 0, 1_000)
	contract.Version = 1
	contract.SetCode(parent, code)

	if _, err := evm.interpreter.Run(contract); err != nil {
		t.Fatalf("version-1 contract should retain 1/64 energy after CALL, got %v", err)
	}
}

func TestInterpreterPush0RequiresShanghai(t *testing.T) {
	evm := newTestEVM(t)
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, []byte{byte(PUSH0), byte(STOP)})

	_, err := evm.interpreter.Run(contract)
	if !errors.Is(err, ErrInvalidOpCode) {
		t.Fatalf("expected ErrInvalidOpCode, got %v", err)
	}
}

func TestInterpreterPush0WorksWithShanghai(t *testing.T) {
	evm := newTestEVMWithConfig(t, TVMConfig{Shanghai: true})
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, []byte{
		byte(PUSH0),
		byte(PUSH1), 0x00,
		byte(MSTORE),
		byte(PUSH1), 0x20,
		byte(PUSH1), 0x00,
		byte(RETURN),
	})

	result, err := evm.interpreter.Run(contract)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 32 {
		t.Fatalf("result length: got %d, want 32", len(result))
	}
	for _, b := range result {
		if b != 0 {
			t.Fatalf("PUSH0 result should be zero word, got %x", result)
		}
	}
}

func TestInterpreterCLZRequiresOsaka(t *testing.T) {
	evm := newTestEVM(t)
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, []byte{byte(PUSH1), 0x01, byte(CLZ), byte(STOP)})

	_, err := evm.interpreter.Run(contract)
	if !errors.Is(err, ErrInvalidOpCode) {
		t.Fatalf("expected ErrInvalidOpCode, got %v", err)
	}
}

func TestInterpreterCLZWorksWithOsaka(t *testing.T) {
	evm := newTestEVMWithConfig(t, TVMConfig{Osaka: true})
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, []byte{
		byte(PUSH1), 0x00,
		byte(CLZ),
		byte(PUSH1), 0x00,
		byte(MSTORE),
		byte(PUSH1), 0x20,
		byte(PUSH1), 0x00,
		byte(RETURN),
	})

	result, err := evm.interpreter.Run(contract)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 32 || result[30] != 0x01 || result[31] != 0x00 {
		t.Fatalf("CLZ(0) should return 256, got %x", result)
	}
}

func TestInterpreterCancunAndBlobOpcodesRequireForks(t *testing.T) {
	for _, tc := range []struct {
		name string
		code []byte
	}{
		{name: "mcopy", code: []byte{byte(PUSH1), 0x00, byte(PUSH1), 0x00, byte(PUSH1), 0x00, byte(MCOPY), byte(STOP)}},
		{name: "blobhash", code: []byte{byte(PUSH1), 0x00, byte(BLOBHASH), byte(STOP)}},
		{name: "blobbasefee", code: []byte{byte(BLOBBASEFEE), byte(STOP)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evm := newTestEVM(t)
			contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
			contract.SetCode(tcommon.Address{0x41, 0x02}, tc.code)
			_, err := evm.interpreter.Run(contract)
			if !errors.Is(err, ErrInvalidOpCode) {
				t.Fatalf("expected ErrInvalidOpCode, got %v", err)
			}
		})
	}
}

func TestInterpreterMCopyWorksWithCancun(t *testing.T) {
	evm := newTestEVMWithConfig(t, TVMConfig{Cancun: true})
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, []byte{
		byte(PUSH1), 0x2a,
		byte(PUSH1), 0x00,
		byte(MSTORE),
		byte(PUSH1), 0x20,
		byte(PUSH1), 0x00,
		byte(PUSH1), 0x20,
		byte(MCOPY),
		byte(PUSH1), 0x20,
		byte(PUSH1), 0x20,
		byte(RETURN),
	})

	result, err := evm.interpreter.Run(contract)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 32 || result[31] != 0x2a {
		t.Fatalf("MCOPY return mismatch: %x", result)
	}
}

func TestInterpreterMCopyZeroLengthChargesVeryLow(t *testing.T) {
	evm := newTestEVMWithConfig(t, TVMConfig{Cancun: true})
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, []byte{
		byte(PUSH1), 0x00,
		byte(PUSH1), 0x00,
		byte(PUSH1), 0x00,
		byte(MCOPY),
		byte(STOP),
	})

	if _, err := evm.interpreter.Run(contract); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := uint64(100000) - contract.Energy; got != 3*EnergyVeryLow+EnergyVeryLow {
		t.Fatalf("MCOPY zero-length energy: got %d, want PUSHes plus MCOPY very low", got)
	}
}

func TestInterpreterBlobOpcodesReturnZeroWithBlobFork(t *testing.T) {
	for _, tc := range []struct {
		name string
		code []byte
	}{
		{name: "blobhash", code: []byte{byte(PUSH1), 0x00, byte(BLOBHASH), byte(PUSH1), 0x00, byte(MSTORE), byte(PUSH1), 0x20, byte(PUSH1), 0x00, byte(RETURN)}},
		{name: "blobbasefee", code: []byte{byte(BLOBBASEFEE), byte(PUSH1), 0x00, byte(MSTORE), byte(PUSH1), 0x20, byte(PUSH1), 0x00, byte(RETURN)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evm := newTestEVMWithConfig(t, TVMConfig{Blob: true})
			contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
			contract.SetCode(tcommon.Address{0x41, 0x02}, tc.code)
			result, err := evm.interpreter.Run(contract)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(result) != 32 {
				t.Fatalf("result length: got %d, want 32", len(result))
			}
			for _, b := range result {
				if b != 0 {
					t.Fatalf("%s should return zero word, got %x", tc.name, result)
				}
			}
		})
	}
}

func TestSelfDestructEnergyCostFollowsJavaForks(t *testing.T) {
	beneficiary := tcommon.Address{0x41, 0x99}
	for _, tc := range []struct {
		name    string
		cfg     TVMConfig
		energy  uint64
		wantErr error
	}{
		{name: "legacy-zero-cost", cfg: TVMConfig{}, energy: 0},
		{name: "energy-adjustment-new-account", cfg: TVMConfig{EnergyAdjustment: true}, energy: EnergyCallNewAcct - 1, wantErr: ErrOutOfEnergy},
		{name: "selfdestruct-restriction-new-account", cfg: TVMConfig{SelfdestructRestrict: true}, energy: EnergySelfDestruct + EnergyCallNewAcct - 1, wantErr: ErrOutOfEnergy},
		{name: "selfdestruct-restriction-enough-energy", cfg: TVMConfig{SelfdestructRestrict: true}, energy: EnergySelfDestruct + EnergyCallNewAcct},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evm := newTestEVMWithConfig(t, tc.cfg)
			contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, tc.energy)
			stack := newStack()
			var word uint256.Int
			word.SetBytes(beneficiary[1:])
			stack.push(&word)

			_, err := opSelfDestruct(nil, evm.interpreter, contract, nil, stack)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("opSelfDestruct error: got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestSelfDestructRestrictionKeepsExistingContract(t *testing.T) {
	evm := newTestEVMWithConfig(t, TVMConfig{SelfdestructRestrict: true})
	contractAddr := tcommon.Address{0x41, 0x44}
	beneficiary := tcommon.Address{0x41, 0x55}
	evm.StateDB.CreateAccount(contractAddr, corepb.AccountType_Contract)
	evm.StateDB.AddBalance(contractAddr, 123)

	contract := NewContract(tcommon.Address{0x41, 0x01}, contractAddr, 0, EnergySelfDestruct+EnergyCallNewAcct)
	stack := newStack()
	var word uint256.Int
	word.SetBytes(beneficiary[1:])
	stack.push(&word)

	if _, err := opSelfDestruct(nil, evm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opSelfDestruct: %v", err)
	}
	if evm.StateDB.HasSelfDestructed(contractAddr) {
		t.Fatal("existing contract must not be deleted after allow_tvm_selfdestruct_restriction")
	}
	if got := evm.StateDB.GetBalance(beneficiary); got != 123 {
		t.Fatalf("beneficiary balance: got %d, want 123", got)
	}
}

func TestSelfDestructRestrictionDeletesNewContract(t *testing.T) {
	evm := newTestEVMWithConfig(t, TVMConfig{SelfdestructRestrict: true})
	contractAddr := tcommon.Address{0x41, 0x66}
	beneficiary := tcommon.Address{0x41, 0x77}
	evm.StateDB.CreateAccount(contractAddr, corepb.AccountType_Contract)
	evm.StateDB.AddBalance(contractAddr, 123)
	evm.newContracts[contractAddr] = true

	contract := NewContract(tcommon.Address{0x41, 0x01}, contractAddr, 0, EnergySelfDestruct+EnergyCallNewAcct)
	stack := newStack()
	var word uint256.Int
	word.SetBytes(beneficiary[1:])
	stack.push(&word)

	if _, err := opSelfDestruct(nil, evm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opSelfDestruct: %v", err)
	}
	if !evm.StateDB.HasSelfDestructed(contractAddr) {
		t.Fatal("new contract must still be deleted after allow_tvm_selfdestruct_restriction")
	}
	if got := evm.StateDB.GetBalance(beneficiary); got != 123 {
		t.Fatalf("beneficiary balance: got %d, want 123", got)
	}
}

func TestSelfDestructSelfTransfersToBlackholeBeforeRestriction(t *testing.T) {
	evm := newTestEVMWithConfig(t, TVMConfig{TransferTrc10: true})
	blackhole := tcommon.Address{0x41, 0xaa}
	evm.SetBlackholeAddress(blackhole)
	contractAddr := tcommon.Address{0x41, 0x01}
	evm.StateDB.CreateAccount(contractAddr, corepb.AccountType_Contract)
	evm.StateDB.AddBalance(contractAddr, 1_000)
	evm.StateDB.SetTRC10Balance(contractAddr, 1_000_017, 7)
	evm.StateDB.SetTRC10Balance(contractAddr, 1_000_018, 0)

	contract := NewContract(tcommon.Address{0x41, 0x02}, contractAddr, 0, 100_000)
	stack := newStack()
	word := addressToUint256(contractAddr)
	stack.push(&word)

	if _, err := opSelfDestruct(nil, evm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opSelfDestruct: %v", err)
	}
	if got := evm.StateDB.GetBalance(contractAddr); got != 0 {
		t.Fatalf("contract balance: got %d, want 0", got)
	}
	if got := evm.StateDB.GetTRC10Balance(contractAddr, 1_000_017); got != 0 {
		t.Fatalf("contract token: got %d, want 0", got)
	}
	if got := evm.StateDB.GetBalance(blackhole); got != 1_000 {
		t.Fatalf("blackhole balance: got %d, want 1000", got)
	}
	if got := evm.StateDB.GetTRC10Balance(blackhole, 1_000_017); got != 7 {
		t.Fatalf("blackhole token: got %d, want 7", got)
	}
	if len(evm.InternalTransactions) != 1 {
		t.Fatalf("internal transactions: got %d, want 1", len(evm.InternalTransactions))
	}
	values := evm.InternalTransactions[0].CallValueInfo
	if len(values) != 2 {
		t.Fatalf("callValueInfo length: got %d, want 2", len(values))
	}
	if got := values[0].CallValue; got != 1_000 {
		t.Fatalf("trx callValueInfo: got %d, want 1000", got)
	}
	if values[1].TokenId != "1000017" || values[1].CallValue != 7 {
		t.Fatalf("token callValueInfo: got token=%q value=%d, want 1000017/7", values[1].TokenId, values[1].CallValue)
	}
	if !evm.StateDB.AccountExists(contractAddr) {
		t.Fatal("contract account should remain visible until transaction commit")
	}
	if !evm.StateDB.HasSelfDestructed(contractAddr) {
		t.Fatal("contract account should be marked for deletion before selfdestruct restriction")
	}
	if _, err := evm.StateDB.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if evm.StateDB.AccountExists(contractAddr) {
		t.Fatal("contract account should be deleted at commit")
	}
}

func TestSelfDestructAddressComparisonFollowsEnergyAdjustment(t *testing.T) {
	contractAddr := mustAddressFromHex(t, "410102030405060708090a0b0c0d0e0f1011121314")
	beneficiary := mustAddressFromHex(t, "410102030405060708090a0b0c0d0e0f10111213ff")

	run := func(t *testing.T, cfg TVMConfig) (beneficiaryBalance int64, blackholeBalance int64) {
		t.Helper()
		evm := newTestEVMWithConfig(t, cfg)
		evm.StateDB.CreateAccount(contractAddr, corepb.AccountType_Contract)
		evm.StateDB.AddBalance(contractAddr, 100)

		contract := NewContract(tcommon.Address{0x41, 0x01}, contractAddr, 0, EnergyCallNewAcct)
		stack := newStack()
		word := addressToUint256(beneficiary)
		stack.push(&word)
		if _, err := opSelfDestruct(nil, evm.interpreter, contract, nil, stack); err != nil {
			t.Fatalf("opSelfDestruct: %v", err)
		}
		return evm.StateDB.GetBalance(beneficiary), evm.StateDB.GetBalance(evm.blackholeAddress())
	}

	t.Run("legacy-compares-first-twenty-address-bytes", func(t *testing.T) {
		beneficiaryBalance, blackholeBalance := run(t, TVMConfig{TransferTrc10: true})
		if beneficiaryBalance != 0 {
			t.Fatalf("beneficiary balance: got %d want 0", beneficiaryBalance)
		}
		if blackholeBalance != 100 {
			t.Fatalf("blackhole balance: got %d want 100", blackholeBalance)
		}
	})

	t.Run("energy-adjustment-compares-full-address", func(t *testing.T) {
		beneficiaryBalance, blackholeBalance := run(t, TVMConfig{TransferTrc10: true, EnergyAdjustment: true})
		if beneficiaryBalance != 100 {
			t.Fatalf("beneficiary balance: got %d want 100", beneficiaryBalance)
		}
		if blackholeBalance != 0 {
			t.Fatalf("blackhole balance: got %d want 0", blackholeBalance)
		}
	})
}

func TestSelfDestructKeepsCodeVisibleUntilCommit(t *testing.T) {
	evm := newTestEVM(t)
	contractAddr := tcommon.Address{0x41, 0x88}
	beneficiary := tcommon.Address{0x41, 0x89}
	code := []byte{byte(PUSH1), 0x2a, byte(PUSH1), 0x00, byte(MSTORE), byte(STOP)}
	evm.StateDB.CreateAccount(contractAddr, corepb.AccountType_Contract)
	evm.StateDB.SetCode(contractAddr, code)

	contract := NewContract(tcommon.Address{0x41, 0x02}, contractAddr, 0, 100_000)
	stack := newStack()
	word := addressToUint256(beneficiary)
	stack.push(&word)

	if _, err := opSelfDestruct(nil, evm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opSelfDestruct: %v", err)
	}
	if got := evm.StateDB.GetCodeSize(contractAddr); got != len(code) {
		t.Fatalf("code should remain visible until commit: got size %d want %d", got, len(code))
	}
	if !evm.StateDB.IsContract(contractAddr) {
		t.Fatal("selfdestructed contract should remain callable until commit")
	}
	if _, err := evm.StateDB.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := evm.StateDB.GetCodeSize(contractAddr); got != 0 {
		t.Fatalf("code should be deleted at commit: got size %d", got)
	}
	if evm.StateDB.AccountExists(contractAddr) {
		t.Fatal("account should be deleted at commit")
	}
}

func TestTokenBalanceStackOrderMatchesJava(t *testing.T) {
	evm := newTestEVM(t)
	addr := tcommon.Address{0x41, 0x01}
	tokenID := int64(1_000_020)
	evm.StateDB.SetTRC10Balance(addr, tokenID, 17)

	stack := newStack()
	addrWord := addressToUint256(addr)
	tokenWord := uint256.NewInt(uint64(tokenID))
	stack.push(&addrWord)
	stack.push(tokenWord)

	if _, err := opTokenBalance(nil, evm.interpreter, nil, nil, stack); err != nil {
		t.Fatalf("opTokenBalance: %v", err)
	}
	gotWord := stack.pop()
	got := gotWord.Uint64()
	if got != 17 {
		t.Fatalf("token balance: got %d, want 17", got)
	}
}

func TestTokenBalanceInvalidTokenIDMatchesJavaExceptionClass(t *testing.T) {
	t.Run("low-token-id-stays-unknown-even-after-constantinople", func(t *testing.T) {
		evm := newTestEVMWithConfig(t, TVMConfig{MultiSign: true, Constantinople: true})

		stack := newStack()
		addrWord := addressToUint256(tcommon.Address{0x41, 0x01})
		tokenWord := uint256.NewInt(1_000_000)
		stack.push(&addrWord)
		stack.push(tokenWord)

		_, err := opTokenBalance(nil, evm.interpreter, nil, nil, stack)
		if !errors.Is(err, ErrInvalidTokenID) {
			t.Fatalf("opTokenBalance error: got %v want %v", err, ErrInvalidTokenID)
		}
		if errors.Is(err, ErrInvalidTokenIDTransfer) {
			t.Fatalf("low token id must not be transfer failure: %v", err)
		}
	})

	t.Run("overflow-token-id-becomes-transfer-failure-after-constantinople", func(t *testing.T) {
		evm := newTestEVMWithConfig(t, TVMConfig{MultiSign: true, Constantinople: true})

		stack := newStack()
		addrWord := addressToUint256(tcommon.Address{0x41, 0x01})
		var tokenWord uint256.Int
		tokenWord.SetUint64(1 << 63)
		stack.push(&addrWord)
		stack.push(&tokenWord)

		_, err := opTokenBalance(nil, evm.interpreter, nil, nil, stack)
		if !errors.Is(err, ErrInvalidTokenIDTransfer) {
			t.Fatalf("opTokenBalance error: got %v want %v", err, ErrInvalidTokenIDTransfer)
		}
	})

	t.Run("negative-exact-token-id-stays-plain-invalid", func(t *testing.T) {
		evm := newTestEVMWithConfig(t, TVMConfig{MultiSign: true, Constantinople: true})

		stack := newStack()
		addrWord := addressToUint256(tcommon.Address{0x41, 0x01})
		var tokenWord uint256.Int
		tokenWord.SetAllOne()
		stack.push(&addrWord)
		stack.push(&tokenWord)

		_, err := opTokenBalance(nil, evm.interpreter, nil, nil, stack)
		if !errors.Is(err, ErrInvalidTokenID) {
			t.Fatalf("opTokenBalance error: got %v want %v", err, ErrInvalidTokenID)
		}
		if errors.Is(err, ErrInvalidTokenIDTransfer) {
			t.Fatalf("negative exact token id must not be transfer failure: %v", err)
		}
	})
}

func TestCallValueInsufficientBalanceRefundsMessageEnergy(t *testing.T) {
	evm := newTestEVMWithConfig(t, TVMConfig{Constantinople: true})
	caller := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}
	target := tcommon.Address{0x41, 0x03}
	evm.StateDB.CreateAccount(contractAddr, corepb.AccountType_Contract)
	evm.StateDB.CreateAccount(target, corepb.AccountType_Normal)
	evm.StateDB.AddBalance(contractAddr, 3_000_000)

	code := []byte{
		byte(PUSH1), 0x00, // out size
		byte(PUSH1), 0x00, // out offset
		byte(PUSH1), 0x00, // in size
		byte(PUSH1), 0x00, // in offset
		byte(PUSH4), 0x05, 0xf5, 0xe1, 0x00, // value: 100_000_000
		byte(PUSH20),
	}
	code = append(code, target[1:]...)
	code = append(code,
		byte(PUSH2), 0x08, 0xfc, // message-call energy: 2300
		byte(CALL),
		byte(STOP),
	)

	contract := NewContract(caller, contractAddr, 0, 100_000)
	contract.SetCode(contractAddr, code)
	_, err := evm.runContract(contract)
	if err != nil {
		t.Fatalf("CALL with insufficient balance should push 0, not halt: %v", err)
	}
	if contract.Energy == 0 {
		t.Fatal("CALL failure consumed all energy")
	}
	if got, wantMin := contract.Energy, uint64(90_000); got < wantMin {
		t.Fatalf("CALL failure did not refund message energy: remaining %d, want >= %d", got, wantMin)
	}
	if got := evm.StateDB.GetBalance(contractAddr); got != 3_000_000 {
		t.Fatalf("contract balance changed: got %d", got)
	}
	if got := evm.StateDB.GetBalance(target); got != 0 {
		t.Fatalf("target balance changed: got %d", got)
	}
}

func TestTopLevelTransferFailedKeepsRemainingEnergy(t *testing.T) {
	evm := newTestEVMWithConfig(t, TVMConfig{Constantinople: true})
	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}
	evm.StateDB.CreateAccount(owner, corepb.AccountType_Normal)
	evm.StateDB.CreateAccount(contractAddr, corepb.AccountType_Contract)
	evm.StateDB.AddBalance(owner, 1_000_000)
	evm.StateDB.AddBalance(contractAddr, 10)
	evm.StateDB.SetContract(contractAddr, &contractpb.SmartContract{ContractAddress: contractAddr.Bytes()})

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
	evm.StateDB.SetCode(contractAddr, code)

	const energyLimit = uint64(100_000)
	_, left, err := evm.Call(owner, contractAddr, nil, energyLimit, 0)
	if !errors.Is(err, ErrTransferFailed) {
		t.Fatalf("expected transfer failure, got %v", err)
	}
	if left == 0 {
		t.Fatal("transfer failure consumed all energy")
	}
	if used := energyLimit - left; used >= 20_000 {
		t.Fatalf("transfer failure energy used = %d, want actual execution cost only", used)
	}
}

// TestTopLevelTransferFailedViaDelegateCallKeepsRemainingEnergy is the
// DelegateCall twin of TestTopLevelTransferFailedKeepsRemainingEnergy. A
// DELEGATECALL runs the delegate's code in the parent (non-readOnly) context,
// so the delegated code can issue a CALL with value to the context address
// itself; that transfer-to-self surfaces ErrTransferFailed (caller == addr)
// and propagates up through DelegateCall's top-level error handler. java-tron
// refunds the message energy on a transfer failure (Program.callToAddress →
// refundEnergy), billing only the energy actually executed — exactly as for
// the Call/CallToken paths. Regression guard for the DelegateCall branch that
// previously billed the full energy limit (consume-all) on a transfer failure.
func TestTopLevelTransferFailedViaDelegateCallKeepsRemainingEnergy(t *testing.T) {
	evm := newTestEVMWithConfig(t, TVMConfig{Constantinople: true})
	owner := tcommon.Address{0x41, 0x01}
	contextAddr := tcommon.Address{0x41, 0x02}
	codeHolder := tcommon.Address{0x41, 0x03}
	evm.StateDB.CreateAccount(owner, corepb.AccountType_Normal)
	evm.StateDB.CreateAccount(contextAddr, corepb.AccountType_Contract)
	evm.StateDB.CreateAccount(codeHolder, corepb.AccountType_Contract)
	evm.StateDB.AddBalance(owner, 1_000_000)
	evm.StateDB.AddBalance(contextAddr, 10)
	evm.StateDB.SetContract(contextAddr, &contractpb.SmartContract{ContractAddress: contextAddr.Bytes()})
	evm.StateDB.SetContract(codeHolder, &contractpb.SmartContract{ContractAddress: codeHolder.Bytes()})

	// Delegate code lives at codeHolder but runs in contextAddr's frame, so a
	// CALL to contextAddr is a transfer-to-self (caller == addr) and fails with
	// ErrTransferFailed, which propagates up through DelegateCall.
	code := []byte{
		byte(PUSH1), 0x00, // out size
		byte(PUSH1), 0x00, // out offset
		byte(PUSH1), 0x00, // in size
		byte(PUSH1), 0x00, // in offset
		byte(PUSH1), 0x01, // value
		byte(PUSH20),
	}
	code = append(code, contextAddr[1:]...)
	code = append(code,
		byte(PUSH2), 0x08, 0xfc, // message-call energy: 2300
		byte(CALL),
		byte(STOP),
	)
	evm.StateDB.SetCode(codeHolder, code)

	const energyLimit = uint64(100_000)
	// caller, context, addr: run codeHolder's code in contextAddr's context.
	_, left, err := evm.DelegateCall(owner, contextAddr, codeHolder, nil, energyLimit, 0, 0)
	if !errors.Is(err, ErrTransferFailed) {
		t.Fatalf("expected transfer failure, got %v", err)
	}
	if left == 0 {
		t.Fatal("transfer failure consumed all energy")
	}
	if used := energyLimit - left; used >= 20_000 {
		t.Fatalf("transfer failure energy used = %d, want actual execution cost only", used)
	}
}

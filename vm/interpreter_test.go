package vm

import (
	"errors"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
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
	if err != ErrInvalidJump {
		t.Fatalf("expected ErrInvalidJump, got %v", err)
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
			if err != ErrStackOverflow {
				t.Fatalf("expected ErrStackOverflow, got %v", err)
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

func TestInterpreterPush0RequiresShanghai(t *testing.T) {
	evm := newTestEVM(t)
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, []byte{byte(PUSH0), byte(STOP)})

	_, err := evm.interpreter.Run(contract)
	if err != ErrInvalidOpCode {
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
	if err != ErrInvalidOpCode {
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
			if err != ErrInvalidOpCode {
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

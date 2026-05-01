package vm

import (
	"crypto/sha256"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
)

const maxCallDepth = 1024

// KVReadWriter is the narrow ethdb capability the TVM needs from the
// rawdb store: per-contract ReadContractState lookups plus
// WriteContractState updates for dynamic-energy accounting. Both
// `ethdb.KeyValueStore` and `core/blockbuffer.Buffer` satisfy it, letting
// callers (`actuator.VMActuator`) route writes either directly to disk
// (BuildBlock path) or through the fork-rewind buffer (applyBlock path).
//
// Slice 3 of the fork-rewind fix widened this from `ethdb.KeyValueStore`
// so that contract-state writes during `act.Execute(ctx)` are rewound on
// switchFork.
type KVReadWriter interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}

// TVM is the top-level TVM execution context.
type TVM struct {
	StateDB     *state.StateDB
	DB          KVReadWriter    // rawdb access (e.g., ContractState for dynamic energy)
	Origin      tcommon.Address // tx.origin
	BlockNumber uint64
	Timestamp   int64
	Coinbase    tcommon.Address // block producer
	ChainID     int64
	Depth       int   // call depth
	Logs        []Log // accumulated log events from this execution

	cfg         TVMConfig
	interpreter *Interpreter
}

func (tvm *TVM) LogSnapshot() int {
	return len(tvm.Logs)
}

func (tvm *TVM) RevertLogs(snapshot int) {
	tvm.Logs = tvm.Logs[:snapshot]
}

// NewTVM creates a new TVM instance.
func NewTVM(stateDB *state.StateDB, origin tcommon.Address, blockNum uint64, timestamp int64, coinbase tcommon.Address, chainID int64, cfg TVMConfig) *TVM {
	tvm := &TVM{
		StateDB:     stateDB,
		Origin:      origin,
		BlockNumber: blockNum,
		Timestamp:   timestamp,
		Coinbase:    coinbase,
		ChainID:     chainID,
		cfg:         cfg,
	}
	tvm.interpreter = NewInterpreter(tvm, cfg)
	return tvm
}

// SetDB sets the rawdb store used for access to per-contract state
// (ContractState for dynamic energy factor tracking, etc.).
func (tvm *TVM) SetDB(db KVReadWriter) {
	tvm.DB = db
}

// Create deploys a new contract.
func (tvm *TVM) Create(caller tcommon.Address, code []byte, energy uint64, value int64) ([]byte, tcommon.Address, uint64, error) {
	if tvm.Depth >= maxCallDepth {
		return nil, tcommon.Address{}, energy, ErrDepthExceeded
	}

	// Generate contract address: SHA256(caller + block_number + depth)
	nonce := make([]byte, 0, 21+8+4)
	nonce = append(nonce, caller[:]...)
	nonce = append(nonce, byte(tvm.BlockNumber>>56), byte(tvm.BlockNumber>>48),
		byte(tvm.BlockNumber>>40), byte(tvm.BlockNumber>>32),
		byte(tvm.BlockNumber>>24), byte(tvm.BlockNumber>>16),
		byte(tvm.BlockNumber>>8), byte(tvm.BlockNumber))
	nonce = append(nonce, byte(tvm.Depth>>24), byte(tvm.Depth>>16),
		byte(tvm.Depth>>8), byte(tvm.Depth))
	hash := sha256.Sum256(nonce)

	var contractAddr tcommon.Address
	contractAddr[0] = 0x41
	copy(contractAddr[1:], hash[12:32])

	return tvm.create(caller, contractAddr, code, energy, value)
}

// Create2 deploys a new contract with a deterministic address.
func (tvm *TVM) Create2(caller tcommon.Address, code []byte, energy uint64, value int64, salt [32]byte) ([]byte, tcommon.Address, uint64, error) {
	if tvm.Depth >= maxCallDepth {
		return nil, tcommon.Address{}, energy, ErrDepthExceeded
	}

	codeHash := sha256.Sum256(code)
	var buf []byte
	buf = append(buf, 0xFF)
	buf = append(buf, caller[:]...)
	buf = append(buf, salt[:]...)
	buf = append(buf, codeHash[:]...)
	hash := sha256.Sum256(buf)

	var contractAddr tcommon.Address
	contractAddr[0] = 0x41
	copy(contractAddr[1:], hash[12:32])

	return tvm.create(caller, contractAddr, code, energy, value)
}

func (tvm *TVM) create(caller tcommon.Address, contractAddr tcommon.Address, code []byte, energy uint64, value int64) ([]byte, tcommon.Address, uint64, error) {
	snap := tvm.StateDB.Snapshot()
	logSnap := tvm.LogSnapshot()

	tvm.StateDB.GetOrCreateAccount(contractAddr)

	if value > 0 {
		if err := tvm.StateDB.SubBalance(caller, value); err != nil {
			tvm.RevertLogs(logSnap)
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, tcommon.Address{}, energy, ErrInsufficientBalance
		}
		tvm.StateDB.AddBalance(contractAddr, value)
	}

	contract := NewContract(caller, contractAddr, value, energy)
	contract.SetCode(contractAddr, code)

	tvm.Depth++
	ret, err := tvm.interpreter.Run(contract)
	tvm.Depth--

	if err != nil {
		tvm.RevertLogs(logSnap)
		tvm.StateDB.RevertToSnapshot(snap)
		if err == ErrExecutionReverted {
			return ret, tcommon.Address{}, contract.Energy, err
		}
		return nil, tcommon.Address{}, 0, err
	}

	if len(ret) > maxCodeSize {
		tvm.RevertLogs(logSnap)
		tvm.StateDB.RevertToSnapshot(snap)
		return nil, tcommon.Address{}, 0, ErrContractCodeTooLarge
	}

	depositCost := uint64(len(ret)) * EnergyCodeDeposit
	if !contract.UseEnergy(depositCost) {
		tvm.RevertLogs(logSnap)
		tvm.StateDB.RevertToSnapshot(snap)
		return nil, tcommon.Address{}, 0, ErrOutOfEnergy
	}

	tvm.StateDB.SetCode(contractAddr, ret)
	return ret, contractAddr, contract.Energy, nil
}

// Call executes a contract call.
func (tvm *TVM) Call(caller, addr tcommon.Address, input []byte, energy uint64, value int64) ([]byte, uint64, error) {
	if tvm.Depth >= maxCallDepth {
		return nil, energy, ErrDepthExceeded
	}

	snap := tvm.StateDB.Snapshot()
	logSnap := tvm.LogSnapshot()

	if value > 0 {
		if err := tvm.StateDB.SubBalance(caller, value); err != nil {
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, energy, ErrInsufficientBalance
		}
		tvm.StateDB.AddBalance(addr, value)
	}

	// Check for precompiled contract
	if p := getPrecompile(addr, tvm.cfg); p != nil {
		ret, energyUsed, err := p.Run(tvm, caller, input, energy)
		remaining := energy - energyUsed
		if err != nil {
			tvm.RevertLogs(logSnap)
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, 0, err
		}
		return ret, remaining, nil
	}

	code := tvm.StateDB.GetCode(addr)
	if len(code) == 0 {
		return nil, energy, nil
	}

	contract := NewContract(caller, addr, value, energy)
	contract.SetCode(addr, code)
	contract.SetInput(input)

	tvm.Depth++
	ret, err := tvm.interpreter.Run(contract)
	tvm.Depth--

	tvm.interpreter.returnData = ret

	if err != nil {
		tvm.RevertLogs(logSnap)
		tvm.StateDB.RevertToSnapshot(snap)
		if err == ErrExecutionReverted {
			return ret, contract.Energy, err
		}
		return nil, 0, err
	}
	return ret, contract.Energy, nil
}

// CallToken executes a contract call with a TRC-10 token transfer.
func (tvm *TVM) CallToken(caller, addr tcommon.Address, input []byte, energy uint64, value int64, tokenID int64, tokenValue int64) ([]byte, uint64, error) {
	if tvm.Depth >= maxCallDepth {
		return nil, energy, ErrDepthExceeded
	}

	snap := tvm.StateDB.Snapshot()
	logSnap := tvm.LogSnapshot()

	if value > 0 {
		if err := tvm.StateDB.SubBalance(caller, value); err != nil {
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, energy, ErrInsufficientBalance
		}
		tvm.StateDB.AddBalance(addr, value)
	}
	if tokenValue > 0 && tokenID > 0 {
		if err := tvm.StateDB.SubTRC10Balance(caller, tokenID, tokenValue); err != nil {
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, energy, ErrInsufficientBalance
		}
		tvm.StateDB.AddTRC10Balance(addr, tokenID, tokenValue)
	}

	// Check for precompiled contract
	if p := getPrecompile(addr, tvm.cfg); p != nil {
		ret, energyUsed, err := p.Run(tvm, caller, input, energy)
		remaining := energy - energyUsed
		if err != nil {
			tvm.RevertLogs(logSnap)
			tvm.StateDB.RevertToSnapshot(snap)
			return nil, 0, err
		}
		return ret, remaining, nil
	}

	code := tvm.StateDB.GetCode(addr)
	if len(code) == 0 {
		return nil, energy, nil
	}

	contract := NewContract(caller, addr, value, energy)
	contract.SetCode(addr, code)
	contract.SetInput(input)
	contract.TokenID = tokenID
	contract.TokenValue = tokenValue

	tvm.Depth++
	ret, err := tvm.interpreter.Run(contract)
	tvm.Depth--

	tvm.interpreter.returnData = ret

	if err != nil {
		tvm.RevertLogs(logSnap)
		tvm.StateDB.RevertToSnapshot(snap)
		if err == ErrExecutionReverted {
			return ret, contract.Energy, err
		}
		return nil, 0, err
	}
	return ret, contract.Energy, nil
}

// StaticCall executes a call without state modifications.
func (tvm *TVM) StaticCall(caller, addr tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	if tvm.Depth >= maxCallDepth {
		return nil, energy, ErrDepthExceeded
	}

	if p := getPrecompile(addr, tvm.cfg); p != nil {
		ret, energyUsed, err := p.Run(tvm, caller, input, energy)
		remaining := energy - energyUsed
		if err != nil {
			return nil, 0, err
		}
		return ret, remaining, nil
	}

	code := tvm.StateDB.GetCode(addr)
	if len(code) == 0 {
		return nil, energy, nil
	}

	contract := NewContract(caller, addr, 0, energy)
	contract.SetCode(addr, code)
	contract.SetInput(input)

	prevReadOnly := tvm.interpreter.readOnly
	tvm.interpreter.readOnly = true

	tvm.Depth++
	ret, err := tvm.interpreter.Run(contract)
	tvm.Depth--

	tvm.interpreter.readOnly = prevReadOnly
	tvm.interpreter.returnData = ret

	if err != nil && err != ErrExecutionReverted {
		return nil, 0, err
	}
	return ret, contract.Energy, err
}

// DelegateCall executes with the caller's context.
func (tvm *TVM) DelegateCall(caller, addr tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	if tvm.Depth >= maxCallDepth {
		return nil, energy, ErrDepthExceeded
	}

	code := tvm.StateDB.GetCode(addr)
	if len(code) == 0 {
		return nil, energy, nil
	}

	contract := NewContract(caller, caller, 0, energy)
	contract.SetCode(addr, code)
	contract.SetInput(input)

	tvm.Depth++
	ret, err := tvm.interpreter.Run(contract)
	tvm.Depth--

	tvm.interpreter.returnData = ret

	if err != nil && err != ErrExecutionReverted {
		return nil, 0, err
	}
	return ret, contract.Energy, err
}

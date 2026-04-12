package vm

import (
	"crypto/sha256"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
)

const maxCallDepth = 1024

// EVM is the top-level execution context.
type EVM struct {
	StateDB     *state.StateDB
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

func (evm *EVM) LogSnapshot() int {
	return len(evm.Logs)
}

func (evm *EVM) RevertLogs(snapshot int) {
	evm.Logs = evm.Logs[:snapshot]
}

// NewEVM creates a new EVM instance.
func NewEVM(stateDB *state.StateDB, origin tcommon.Address, blockNum uint64, timestamp int64, coinbase tcommon.Address, chainID int64, cfg TVMConfig) *EVM {
	evm := &EVM{
		StateDB:     stateDB,
		Origin:      origin,
		BlockNumber: blockNum,
		Timestamp:   timestamp,
		Coinbase:    coinbase,
		ChainID:     chainID,
		cfg:         cfg,
	}
	evm.interpreter = NewInterpreter(evm, cfg)
	return evm
}

// Create deploys a new contract.
func (evm *EVM) Create(caller tcommon.Address, code []byte, energy uint64, value int64) ([]byte, tcommon.Address, uint64, error) {
	if evm.Depth >= maxCallDepth {
		return nil, tcommon.Address{}, energy, ErrDepthExceeded
	}

	// Generate contract address: SHA256(caller + block_number + depth)
	nonce := make([]byte, 0, 21+8+4)
	nonce = append(nonce, caller[:]...)
	nonce = append(nonce, byte(evm.BlockNumber>>56), byte(evm.BlockNumber>>48),
		byte(evm.BlockNumber>>40), byte(evm.BlockNumber>>32),
		byte(evm.BlockNumber>>24), byte(evm.BlockNumber>>16),
		byte(evm.BlockNumber>>8), byte(evm.BlockNumber))
	nonce = append(nonce, byte(evm.Depth>>24), byte(evm.Depth>>16),
		byte(evm.Depth>>8), byte(evm.Depth))
	hash := sha256.Sum256(nonce)

	var contractAddr tcommon.Address
	contractAddr[0] = 0x41
	copy(contractAddr[1:], hash[12:32])

	return evm.create(caller, contractAddr, code, energy, value)
}

// Create2 deploys a new contract with a deterministic address.
func (evm *EVM) Create2(caller tcommon.Address, code []byte, energy uint64, value int64, salt [32]byte) ([]byte, tcommon.Address, uint64, error) {
	if evm.Depth >= maxCallDepth {
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

	return evm.create(caller, contractAddr, code, energy, value)
}

func (evm *EVM) create(caller tcommon.Address, contractAddr tcommon.Address, code []byte, energy uint64, value int64) ([]byte, tcommon.Address, uint64, error) {
	snap := evm.StateDB.Snapshot()
	logSnap := evm.LogSnapshot()

	evm.StateDB.GetOrCreateAccount(contractAddr)

	if value > 0 {
		if err := evm.StateDB.SubBalance(caller, value); err != nil {
			evm.RevertLogs(logSnap)
			evm.StateDB.RevertToSnapshot(snap)
			return nil, tcommon.Address{}, energy, ErrInsufficientBalance
		}
		evm.StateDB.AddBalance(contractAddr, value)
	}

	contract := NewContract(caller, contractAddr, value, energy)
	contract.SetCode(contractAddr, code)

	evm.Depth++
	ret, err := evm.interpreter.Run(contract)
	evm.Depth--

	if err != nil {
		evm.RevertLogs(logSnap)
		evm.StateDB.RevertToSnapshot(snap)
		if err == ErrExecutionReverted {
			return ret, tcommon.Address{}, contract.Energy, err
		}
		return nil, tcommon.Address{}, 0, err
	}

	if len(ret) > maxCodeSize {
		evm.RevertLogs(logSnap)
		evm.StateDB.RevertToSnapshot(snap)
		return nil, tcommon.Address{}, 0, ErrContractCodeTooLarge
	}

	depositCost := uint64(len(ret)) * EnergyCodeDeposit
	if !contract.UseEnergy(depositCost) {
		evm.RevertLogs(logSnap)
		evm.StateDB.RevertToSnapshot(snap)
		return nil, tcommon.Address{}, 0, ErrOutOfEnergy
	}

	evm.StateDB.SetCode(contractAddr, ret)
	return ret, contractAddr, contract.Energy, nil
}

// Call executes a contract call.
func (evm *EVM) Call(caller, addr tcommon.Address, input []byte, energy uint64, value int64) ([]byte, uint64, error) {
	if evm.Depth >= maxCallDepth {
		return nil, energy, ErrDepthExceeded
	}

	snap := evm.StateDB.Snapshot()
	logSnap := evm.LogSnapshot()

	if value > 0 {
		if err := evm.StateDB.SubBalance(caller, value); err != nil {
			evm.StateDB.RevertToSnapshot(snap)
			return nil, energy, ErrInsufficientBalance
		}
		evm.StateDB.AddBalance(addr, value)
	}

	// Check for precompiled contract
	if p := getPrecompile(addr, evm.cfg); p != nil {
		ret, energyUsed, err := p.Run(evm, caller, input, energy)
		remaining := energy - energyUsed
		if err != nil {
			evm.RevertLogs(logSnap)
			evm.StateDB.RevertToSnapshot(snap)
			return nil, 0, err
		}
		return ret, remaining, nil
	}

	code := evm.StateDB.GetCode(addr)
	if len(code) == 0 {
		return nil, energy, nil
	}

	contract := NewContract(caller, addr, value, energy)
	contract.SetCode(addr, code)
	contract.SetInput(input)

	evm.Depth++
	ret, err := evm.interpreter.Run(contract)
	evm.Depth--

	evm.interpreter.returnData = ret

	if err != nil {
		evm.RevertLogs(logSnap)
		evm.StateDB.RevertToSnapshot(snap)
		if err == ErrExecutionReverted {
			return ret, contract.Energy, err
		}
		return nil, 0, err
	}
	return ret, contract.Energy, nil
}

// StaticCall executes a call without state modifications.
func (evm *EVM) StaticCall(caller, addr tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	if evm.Depth >= maxCallDepth {
		return nil, energy, ErrDepthExceeded
	}

	if p := getPrecompile(addr, evm.cfg); p != nil {
		ret, energyUsed, err := p.Run(evm, caller, input, energy)
		remaining := energy - energyUsed
		if err != nil {
			return nil, 0, err
		}
		return ret, remaining, nil
	}

	code := evm.StateDB.GetCode(addr)
	if len(code) == 0 {
		return nil, energy, nil
	}

	contract := NewContract(caller, addr, 0, energy)
	contract.SetCode(addr, code)
	contract.SetInput(input)

	prevReadOnly := evm.interpreter.readOnly
	evm.interpreter.readOnly = true

	evm.Depth++
	ret, err := evm.interpreter.Run(contract)
	evm.Depth--

	evm.interpreter.readOnly = prevReadOnly
	evm.interpreter.returnData = ret

	if err != nil && err != ErrExecutionReverted {
		return nil, 0, err
	}
	return ret, contract.Energy, err
}

// DelegateCall executes with the caller's context.
func (evm *EVM) DelegateCall(caller, addr tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	if evm.Depth >= maxCallDepth {
		return nil, energy, ErrDepthExceeded
	}

	code := evm.StateDB.GetCode(addr)
	if len(code) == 0 {
		return nil, energy, nil
	}

	contract := NewContract(caller, caller, 0, energy)
	contract.SetCode(addr, code)
	contract.SetInput(input)

	evm.Depth++
	ret, err := evm.interpreter.Run(contract)
	evm.Depth--

	evm.interpreter.returnData = ret

	if err != nil && err != ErrExecutionReverted {
		return nil, 0, err
	}
	return ret, contract.Energy, err
}

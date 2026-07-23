package vm

import (
	"sync"

	tcommon "github.com/tronprotocol/go-tron/common"
)

// Contract represents a single call frame's execution context.
type Contract struct {
	Caller   tcommon.Address // msg.sender
	Address  tcommon.Address // address of this contract
	Value    int64           // msg.value (TRX in sun)
	Code     []byte          // bytecode to execute
	CodeAddr tcommon.Address // code source address (differs for DELEGATECALL)
	CodeHash tcommon.Hash    // Keccak256(Code) when known; keys the jumpdest cache
	Input    []byte          // calldata

	InternalTxHash tcommon.Hash // java-tron parent hash for nested internal txs

	// TRC-10 token transfer (for CALLTOKEN opcode)
	TokenID    int64 // TRC-10 token ID (0 = none)
	TokenValue int64 // TRC-10 token amount

	Energy     uint64 // remaining energy
	EnergyUsed uint64 // energy consumed so far
	Version    int32  // java-tron SmartContract.version for this code frame

	jumpdests bitvec // valid JUMPDEST positions, 1 bit per code offset
}

// executionContractPool reuses the short-lived Contract object attached to
// each VM call frame. Nested calls borrow distinct objects and return them as
// their Go call frames unwind. Debug tracing deliberately bypasses this pool
// because a third-party tracer may retain the live ScopeContext.Contract
// pointer after CaptureState returns.
var executionContractPool = sync.Pool{
	New: func() any { return new(Contract) },
}

func acquireExecutionContract(caller, addr tcommon.Address, value int64, energy uint64) *Contract {
	c := executionContractPool.Get().(*Contract)
	*c = Contract{
		Caller:  caller,
		Address: addr,
		Value:   value,
		Energy:  energy,
	}
	return c
}

func releaseExecutionContract(c *Contract) {
	if c == nil {
		return
	}
	// Clear Code/Input/jumpdests before pooling so the process does not retain
	// state bytecode, calldata or an uncached init-code analysis through an
	// otherwise idle frame object.
	*c = Contract{}
	executionContractPool.Put(c)
}

// NewContract creates a new contract execution context.
func NewContract(caller, addr tcommon.Address, value int64, energy uint64) *Contract {
	return &Contract{
		Caller:  caller,
		Address: addr,
		Value:   value,
		Energy:  energy,
	}
}

// SetCode sets the contract's bytecode and builds the jumpdest analysis. When the
// caller has populated CodeHash (the StateDB code identity), the analysis is
// served from / stored in the process-wide cache so identical code is scanned
// once; otherwise (initcode, zero hash) it is analyzed directly.
func (c *Contract) SetCode(addr tcommon.Address, code []byte) {
	c.Code = code
	c.CodeAddr = addr
	c.jumpdests = globalJumpdestCache.analyze(c.CodeHash, code)
}

// SetInput sets the contract's calldata.
func (c *Contract) SetInput(input []byte) {
	c.Input = input
}

// UseEnergy deducts amount from remaining energy. Returns false if insufficient.
func (c *Contract) UseEnergy(amount uint64) bool {
	if c.Energy < amount {
		return false
	}
	c.Energy -= amount
	c.EnergyUsed += amount
	return true
}

// IsValidJumpdest checks if pos is a valid JUMPDEST in the code.
func (c *Contract) IsValidJumpdest(pos uint64) bool {
	if pos >= uint64(len(c.Code)) {
		return false
	}
	return c.jumpdests.isSet(pos)
}

// GetOp returns the opcode at position pos in the code.
func (c *Contract) GetOp(pos uint64) OpCode {
	if pos >= uint64(len(c.Code)) {
		return STOP
	}
	return OpCode(c.Code[pos])
}

// bitvec is a compact 1-bit-per-code-offset set of valid JUMPDEST positions. It
// replaces the former map[uint64]bool: identical valid-dest set, but one flat
// []byte instead of a per-dest map node, so each contract load allocates
// ceil(len(code)/8) bytes instead of O(#jumpdests) map entries.
type bitvec []byte

func (bv bitvec) set(pos uint64)        { bv[pos/8] |= 1 << (pos % 8) }
func (bv bitvec) isSet(pos uint64) bool { return bv[pos/8]&(1<<(pos%8)) != 0 }

// analyzeJumpdests finds all valid JUMPDEST positions, skipping PUSH data. The
// scan is identical to the reference map implementation pinned in
// contract_jumpdest_test.go; only the storage representation is a bitvec.
func analyzeJumpdests(code []byte) bitvec {
	bv := make(bitvec, len(code)/8+1)
	for i := 0; i < len(code); i++ {
		op := OpCode(code[i])
		if op == JUMPDEST {
			bv.set(uint64(i))
		} else if op >= PUSH1 && op <= PUSH32 {
			i += int(op - PUSH1 + 1)
		}
	}
	return bv
}

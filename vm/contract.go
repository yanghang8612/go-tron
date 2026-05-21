package vm

import tcommon "github.com/tronprotocol/go-tron/common"

// Contract represents a single call frame's execution context.
type Contract struct {
	Caller   tcommon.Address // msg.sender
	Address  tcommon.Address // address of this contract
	Value    int64           // msg.value (TRX in sun)
	Code     []byte          // bytecode to execute
	CodeAddr tcommon.Address // code source address (differs for DELEGATECALL)
	Input    []byte          // calldata

	InternalTxHash tcommon.Hash // java-tron parent hash for nested internal txs

	// TRC-10 token transfer (for CALLTOKEN opcode)
	TokenID    int64 // TRC-10 token ID (0 = none)
	TokenValue int64 // TRC-10 token amount

	Energy     uint64 // remaining energy
	EnergyUsed uint64 // energy consumed so far
	Version    int32  // java-tron SmartContract.version for this code frame

	jumpdests map[uint64]bool // cached valid JUMPDEST positions
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

// SetCode sets the contract's bytecode and builds the jumpdest analysis.
func (c *Contract) SetCode(addr tcommon.Address, code []byte) {
	c.Code = code
	c.CodeAddr = addr
	c.jumpdests = analyzeJumpdests(code)
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
	return c.jumpdests[pos]
}

// GetOp returns the opcode at position pos in the code.
func (c *Contract) GetOp(pos uint64) OpCode {
	if pos >= uint64(len(c.Code)) {
		return STOP
	}
	return OpCode(c.Code[pos])
}

// analyzeJumpdests finds all valid JUMPDEST positions, skipping PUSH data.
func analyzeJumpdests(code []byte) map[uint64]bool {
	dests := make(map[uint64]bool)
	for i := 0; i < len(code); i++ {
		op := OpCode(code[i])
		if op == JUMPDEST {
			dests[uint64(i)] = true
		} else if op >= PUSH1 && op <= PUSH32 {
			i += int(op - PUSH1 + 1)
		}
	}
	return dests
}

# Phase 6: TVM (Smart Contract Execution) — Design Spec

## Goal

Enable smart contract deployment and execution in go-tron. Users can deploy Solidity contracts via `CreateSmartContract` and invoke them via `TriggerSmartContract`, with EVM-compatible bytecode execution and energy metering.

## Architecture

```
vm/                              # TVM execution engine
  opcodes.go                     # Opcode constants (0x00-0xFF)
  jump_table.go                  # Operation dispatch table
  stack.go                       # 256-bit word stack (max 1024)
  memory.go                      # Byte-addressable expandable memory
  interpreter.go                 # Fetch-decode-execute loop
  instructions.go                # Opcode implementations (arithmetic, control, etc.)
  instructions_call.go           # CALL/CREATE family implementations
  contract.go                    # Contract execution context
  energy.go                      # Energy cost definitions and calculation
  errors.go                      # VM error types
  precompiles.go                 # Precompiled contracts (ECRecover, SHA256, etc.)

actuator/
  vm_actuator.go                 # CreateSmartContract + TriggerSmartContract actuator

core/state/
  statedb.go                     # (extend) Add code/storage/contract state methods
  state_object.go                # (extend) Add code, storage, contract fields
```

The TVM is a stack-based virtual machine executing EVM bytecode. It shares opcode semantics with Ethereum's EVM but uses TRON's energy model instead of gas, and operates on TRON's protobuf-based account state rather than Ethereum's MPT.

**Design principle**: Build a self-contained VM that integrates with go-tron's existing state layer. Do NOT import go-ethereum's `core/vm` package — TRON's state model (protobuf accounts, energy vs gas, TRX sun units) differs enough to warrant a native implementation. We do reuse `github.com/holiman/uint256` for 256-bit arithmetic (already a go-ethereum dependency).

## State Layer Extensions

The current `StateDB` needs contract support. Add these methods:

```go
// Code storage
GetCode(addr Address) []byte
SetCode(addr Address, code []byte)
GetCodeSize(addr Address) int
GetCodeHash(addr Address) Hash

// Contract key-value storage (256-bit key → 256-bit value)
GetState(addr Address, key Hash) Hash
SetState(addr Address, key Hash, value Hash)

// Contract metadata
GetContract(addr Address) *SmartContract       // proto SmartContract
SetContract(addr Address, contract *SmartContract)
IsContract(addr Address) bool

// Existence
Exist(addr Address) bool
Empty(addr Address) bool

// Self-destruct
SelfDestruct(addr Address)
HasSelfDestructed(addr Address) bool
```

The `stateObject` gains:
- `code []byte` — contract bytecode (loaded lazily)
- `storage map[Hash]Hash` — dirty contract storage
- `contractMeta *SmartContract` — contract metadata (ABI, origin, settings)
- `selfDestructed bool`

Code is stored in the underlying database keyed by `CodePrefix + address`. Storage is keyed by `StoragePrefix + address + key`.

## VM Core

### Stack

1024-element stack of `uint256.Int` values. Operations:

```go
type Stack struct {
    data []uint256.Int
}
func (s *Stack) Push(v *uint256.Int)
func (s *Stack) Pop() uint256.Int
func (s *Stack) Peek() *uint256.Int       // top without pop
func (s *Stack) Back(n int) *uint256.Int   // nth from top
func (s *Stack) Swap(n int)                // swap top with nth
func (s *Stack) Dup(n int)                 // duplicate nth to top
func (s *Stack) Len() int
```

### Memory

Byte-addressable, word-aligned expandable memory:

```go
type Memory struct {
    store []byte
}
func (m *Memory) Set(offset, size uint64, value []byte)
func (m *Memory) Set32(offset uint64, val *uint256.Int)
func (m *Memory) GetCopy(offset, size int64) []byte
func (m *Memory) GetPtr(offset, size int64) []byte
func (m *Memory) Len() int
func (m *Memory) Resize(size uint64)
```

Memory expansion costs energy: `words * 3 + words^2 / 512`.

### Contract

Execution context for a single call frame:

```go
type Contract struct {
    Caller    Address       // msg.sender
    Address   Address       // this contract's address
    Value     int64         // msg.value (TRX in sun)
    Code      []byte        // bytecode
    CodeAddr  Address       // code source (differs for DELEGATECALL)
    Input     []byte        // calldata
    Energy    uint64        // remaining energy
    EnergyUsed uint64
}
```

### Interpreter

The main execution loop:

```go
type Interpreter struct {
    evm    *EVM
    table  [256]Operation
}

func (i *Interpreter) Run(contract *Contract) ([]byte, error)
```

`Run` loops: fetch opcode → lookup in jump table → validate stack → charge energy → execute → advance PC. Stops on STOP, RETURN, REVERT, SELFDESTRUCT, or error.

### EVM (top-level context)

```go
type EVM struct {
    StateDB     *state.StateDB
    Origin      Address         // tx.origin
    BlockNumber uint64
    Timestamp   int64
    Coinbase    Address         // block producer
    ChainID     int64
    Depth       int             // call depth (max 1024)
    interpreter *Interpreter
}

func NewEVM(stateDB *state.StateDB, origin Address, blockNum uint64, timestamp int64, coinbase Address, chainID int64) *EVM

// Create deploys a new contract
func (evm *EVM) Create(caller Address, code []byte, energy uint64, value int64) ([]byte, Address, uint64, error)

// Call executes a contract
func (evm *EVM) Call(caller, addr Address, input []byte, energy uint64, value int64) ([]byte, uint64, error)

// StaticCall executes without state modifications
func (evm *EVM) StaticCall(caller, addr Address, input []byte, energy uint64) ([]byte, uint64, error)

// DelegateCall executes with caller's context
func (evm *EVM) DelegateCall(caller, addr Address, input []byte, energy uint64) ([]byte, uint64, error)
```

## Opcode Support

Phase 6 implements the standard EVM opcode set (Constantinople + Istanbul + London + Shanghai). TRON-specific opcodes (0xD0-0xDF) are deferred to a later phase.

### Arithmetic (0x01-0x0B)
ADD, MUL, SUB, DIV, SDIV, MOD, SMOD, ADDMOD, MULMOD, EXP, SIGNEXTEND

### Comparison & Bitwise (0x10-0x1D)
LT, GT, SLT, SGT, EQ, ISZERO, AND, OR, XOR, NOT, BYTE, SHL, SHR, SAR

### Cryptographic (0x20)
SHA3 (Keccak-256)

### Environmental (0x30-0x3F)
ADDRESS, BALANCE, ORIGIN, CALLER, CALLVALUE, CALLDATALOAD, CALLDATASIZE, CALLDATACOPY, CODESIZE, CODECOPY, EXTCODESIZE, EXTCODECOPY, RETURNDATASIZE, RETURNDATACOPY, EXTCODEHASH

### Block Information (0x40-0x48)
BLOCKHASH, COINBASE, TIMESTAMP, NUMBER, DIFFICULTY (returns 0), GASLIMIT (returns max energy), CHAINID, SELFBALANCE, BASEFEE (returns 0)

### Stack/Memory/Storage (0x50-0x5B)
POP, MLOAD, MSTORE, MSTORE8, SLOAD, SSTORE, JUMP, JUMPI, PC, MSIZE, GAS, JUMPDEST

### Push (0x5F-0x7F)
PUSH0, PUSH1-PUSH32

### Dup (0x80-0x8F)
DUP1-DUP16

### Swap (0x90-0x9F)
SWAP1-SWAP16

### Logging (0xA0-0xA4)
LOG0-LOG4

### System (0xF0-0xFF)
CREATE, CALL, CALLCODE, RETURN, DELEGATECALL, CREATE2, STATICCALL, REVERT, SELFDESTRUCT

### Energy Mapping (TRON names, EVM semantics)

TRON uses "energy" where Ethereum uses "gas". The cost table matches Ethereum's Constantinople schedule:

| Category | Cost |
|----------|------|
| Zero | 0 (STOP, RETURN, REVERT) |
| Base | 2 (ADDRESS, ORIGIN, CALLER, etc.) |
| VeryLow | 3 (ADD, SUB, LT, GT, etc.) |
| Low | 5 (MUL, DIV, etc.) |
| Mid | 8 (ADDMOD, MULMOD) |
| High | 10 (JUMPI) |
| SHA3 | 30 + 6 per word |
| SLOAD | 200 |
| SSTORE | 5000 (set) / 20000 (create) / 15000 refund (clear) |
| CALL | 700 base + value transfer + new account costs |
| CREATE | 32000 |
| LOG | 375 + 375*topics + 8*data_bytes |
| EXP | 10 + 50*byte_len |
| COPY | 3 + 3*words (CALLDATACOPY, CODECOPY, etc.) |
| Memory | 3*words + words²/512 |
| CodeDeposit | 200 per byte (contract creation return data) |

## Precompiled Contracts

Phase 6 implements the 4 essential precompiled contracts at addresses 0x01-0x04:

| Address | Name | Purpose | Energy |
|---------|------|---------|--------|
| 0x01 | ECRecover | ECDSA public key recovery | 3000 |
| 0x02 | SHA256 | SHA-256 hash | 60 + 12/word |
| 0x03 | RIPEMD160 | RIPEMD-160 hash | 600 + 120/word |
| 0x04 | Identity | Data copy (memcpy) | 15 + 3/word |

Additional precompiles (ModExp, BN128, Blake2) are deferred.

## Contract Address Generation

For `CREATE`: `address = hash(sender, nonce)` — TRON uses SHA256 of `(sender_address + nonce_bytes)`, take last 20 bytes, set first byte to 0x41.

For `CREATE2`: `address = hash(0xFF, sender, salt, codeHash)` — same trailing 20 bytes approach.

## VMActuator

The `VMActuator` integrates with the existing actuator pattern but handles both `CreateSmartContract` (type 30) and `TriggerSmartContract` (type 31):

```go
type VMActuator struct{}

func (a *VMActuator) Validate(ctx *Context) error
func (a *VMActuator) Execute(ctx *Context) (*Result, error)
```

### CreateSmartContract Flow

1. **Validate**: Check owner exists, contract name ≤ 32 bytes, consume_user_resource_percent ∈ [0,100], fee limit valid
2. **Generate address**: SHA256(owner + tx_nonce), first byte = 0x41
3. **Calculate energy limit**: `min(frozen_energy + balance/energy_price, fee_limit/energy_price)`
4. **Create EVM**: set origin, block context
5. **Execute**: `evm.Create(owner, bytecode, energyLimit, callValue)`
6. **On success**: Store contract metadata (ABI, origin, settings) via `StateDB.SetContract()`
7. **Charge energy**: consumed energy * energy_price deducted from balance
8. **On failure**: Revert state, charge all energy

### TriggerSmartContract Flow

1. **Validate**: Check owner exists, contract exists, has code
2. **Calculate energy limit**: Same formula, with creator/caller split via `consume_user_resource_percent`
3. **Create EVM**: set origin, block context
4. **Execute**: `evm.Call(owner, contractAddr, data, energyLimit, callValue)`
5. **On success**: Commit state changes
6. **Charge energy**: consumed energy * energy_price
7. **On failure**: Revert, charge all energy

### Energy Price

The energy price is a dynamic property: `DynamicProperties.EnergyFee` (default: 420 sun per energy unit). This converts energy consumption to TRX cost.

## API Additions

Add to `internal/tronapi/`:

```
POST /wallet/deploycontract        — Deploy a smart contract
POST /wallet/triggersmartcontract   — Call a contract (state-changing)
POST /wallet/triggerconstantcontract — Call a contract (read-only, no broadcast)
POST /wallet/getcontract           — Get contract metadata by address
```

### deploycontract

Request: `CreateSmartContract` JSON (owner_address, abi, bytecode, name, call_value, consume_user_resource_percent, origin_energy_limit)

Response: Unsigned transaction JSON (caller signs and broadcasts separately)

### triggersmartcontract

Request: `TriggerSmartContract` JSON (owner_address, contract_address, function_selector, parameter, call_value, fee_limit)

Response: Unsigned transaction + `constant_result` (execution result for estimation)

### triggerconstantcontract

Same as triggersmartcontract but executes in a snapshot (read-only). No transaction is created. Returns only the execution result.

### getcontract

Request: `{"value": "contract_address_hex"}`

Response: SmartContract proto JSON (ABI, bytecode, origin, settings)

## State Snapshot for Read-Only Calls

`triggerconstantcontract` needs to execute without modifying state. The `StateDB` already uses snapshot-based journaling. For constant calls:

1. Copy the current StateDB
2. Execute the call in the copy
3. Return results, discard the copy

This is also used for gas estimation in `triggersmartcontract`.

## Integration with Existing Code

### State Processor

`state_processor.go` already calls `actuator.CreateActuator(tx)` for each transaction. Add the VM actuator to the switch:

```go
case corepb.Transaction_Contract_CreateSmartContract:
    return &VMActuator{}, nil
case corepb.Transaction_Contract_TriggerSmartContract:
    return &VMActuator{}, nil
```

### Backend

Add to `tron_backend.go`:

```go
func (b *TronBackend) GetContract(addr Address) *SmartContract
func (b *TronBackend) TriggerConstantContract(owner, contract Address, data []byte, value int64) ([]byte, int64, error)
```

### DynamicProperties

Add energy-related properties to `DynamicProperties`:

```go
EnergyFee int64    // sun per energy unit (default 420)
```

## Testing Strategy

1. **Unit tests**: Stack, memory, individual opcodes (arithmetic, comparison, bitwise)
2. **Integration tests**: Full contract deploy + call cycle with simple Solidity contracts
3. **Precompile tests**: ECRecover, SHA256, RIPEMD160 with known test vectors
4. **Energy tests**: Verify energy consumption matches expected costs
5. **Error tests**: Out of energy, stack overflow/underflow, invalid jump, revert
6. **API tests**: Deploy via HTTP, call via HTTP, constant call

### Test Contracts

Use pre-compiled Solidity bytecode for testing (no Solidity compiler dependency):

- **Counter**: `increment()`, `get()` — basic storage read/write
- **Calculator**: `add(a,b)`, `mul(a,b)` — pure computation
- **SimpleStore**: `set(uint)`, `get()` — storage with events

## Out of Scope

- TRON-specific opcodes (CALLTOKEN, FREEZE, VOTEWITNESS, etc.)
- TRC-10 token support in VM (call_token_value, token_id)
- Advanced precompiles (ModExp, BN128, Blake2, ZK proofs)
- Dynamic energy factor / energy penalty
- Event/log subscription and filtering
- Internal transaction tracking
- CREATE2 with nonce management
- Access list (EIP-2929/2930)
- Transient storage (EIP-1153)
- JSON-RPC interface (eth_call, eth_estimateGas)

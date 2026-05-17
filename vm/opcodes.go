package vm

// OpCode is a single byte TVM opcode.
type OpCode byte

func (op OpCode) String() string {
	if name, ok := opCodeNames[op]; ok {
		return name
	}
	return "INVALID"
}

// 0x0 range - arithmetic ops
const (
	STOP       OpCode = 0x00
	ADD        OpCode = 0x01
	MUL        OpCode = 0x02
	SUB        OpCode = 0x03
	DIV        OpCode = 0x04
	SDIV       OpCode = 0x05
	MOD        OpCode = 0x06
	SMOD       OpCode = 0x07
	ADDMOD     OpCode = 0x08
	MULMOD     OpCode = 0x09
	EXP        OpCode = 0x0A
	SIGNEXTEND OpCode = 0x0B
)

// 0x10 range - comparison ops
const (
	LT     OpCode = 0x10
	GT     OpCode = 0x11
	SLT    OpCode = 0x12
	SGT    OpCode = 0x13
	EQ     OpCode = 0x14
	ISZERO OpCode = 0x15
	AND    OpCode = 0x16
	OR     OpCode = 0x17
	XOR    OpCode = 0x18
	NOT    OpCode = 0x19
	BYTE   OpCode = 0x1A
	SHL    OpCode = 0x1B
	SHR    OpCode = 0x1C
	SAR    OpCode = 0x1D
	CLZ    OpCode = 0x1E
)

// 0x20 range - crypto
const (
	SHA3 OpCode = 0x20
)

// 0x30 range - environment
const (
	ADDRESS        OpCode = 0x30
	BALANCE        OpCode = 0x31
	ORIGIN         OpCode = 0x32
	CALLER         OpCode = 0x33
	CALLVALUE      OpCode = 0x34
	CALLDATALOAD   OpCode = 0x35
	CALLDATASIZE   OpCode = 0x36
	CALLDATACOPY   OpCode = 0x37
	CODESIZE       OpCode = 0x38
	CODECOPY       OpCode = 0x39
	GASPRICE       OpCode = 0x3A
	EXTCODESIZE    OpCode = 0x3B
	EXTCODECOPY    OpCode = 0x3C
	RETURNDATASIZE OpCode = 0x3D
	RETURNDATACOPY OpCode = 0x3E
	EXTCODEHASH    OpCode = 0x3F
)

// 0x40 range - block operations
const (
	BLOCKHASH   OpCode = 0x40
	COINBASE    OpCode = 0x41
	TIMESTAMP   OpCode = 0x42
	NUMBER      OpCode = 0x43
	DIFFICULTY  OpCode = 0x44
	GASLIMIT    OpCode = 0x45
	CHAINID     OpCode = 0x46
	SELFBALANCE OpCode = 0x47
	BASEFEE     OpCode = 0x48
	BLOBHASH    OpCode = 0x49
	BLOBBASEFEE OpCode = 0x4A
)

// 0x50 range - stack/memory/storage
const (
	POP      OpCode = 0x50
	MLOAD    OpCode = 0x51
	MSTORE   OpCode = 0x52
	MSTORE8  OpCode = 0x53
	SLOAD    OpCode = 0x54
	SSTORE   OpCode = 0x55
	JUMP     OpCode = 0x56
	JUMPI    OpCode = 0x57
	PC       OpCode = 0x58
	MSIZE    OpCode = 0x59
	GAS      OpCode = 0x5A
	JUMPDEST OpCode = 0x5B
	TLOAD    OpCode = 0x5C // EIP-1153 transient storage load  (allow_tvm_cancun)
	TSTORE   OpCode = 0x5D // EIP-1153 transient storage store (allow_tvm_cancun)
	MCOPY    OpCode = 0x5E // EIP-5656 memory copy             (allow_tvm_cancun)
)

// 0x5f range - push
const (
	PUSH0  OpCode = 0x5F
	PUSH1  OpCode = 0x60
	PUSH2  OpCode = 0x61
	PUSH3  OpCode = 0x62
	PUSH4  OpCode = 0x63
	PUSH5  OpCode = 0x64
	PUSH6  OpCode = 0x65
	PUSH7  OpCode = 0x66
	PUSH8  OpCode = 0x67
	PUSH9  OpCode = 0x68
	PUSH10 OpCode = 0x69
	PUSH11 OpCode = 0x6A
	PUSH12 OpCode = 0x6B
	PUSH13 OpCode = 0x6C
	PUSH14 OpCode = 0x6D
	PUSH15 OpCode = 0x6E
	PUSH16 OpCode = 0x6F
	PUSH17 OpCode = 0x70
	PUSH18 OpCode = 0x71
	PUSH19 OpCode = 0x72
	PUSH20 OpCode = 0x73
	PUSH21 OpCode = 0x74
	PUSH22 OpCode = 0x75
	PUSH23 OpCode = 0x76
	PUSH24 OpCode = 0x77
	PUSH25 OpCode = 0x78
	PUSH26 OpCode = 0x79
	PUSH27 OpCode = 0x7A
	PUSH28 OpCode = 0x7B
	PUSH29 OpCode = 0x7C
	PUSH30 OpCode = 0x7D
	PUSH31 OpCode = 0x7E
	PUSH32 OpCode = 0x7F
)

// 0x80 range - dup
const (
	DUP1  OpCode = 0x80
	DUP2  OpCode = 0x81
	DUP3  OpCode = 0x82
	DUP4  OpCode = 0x83
	DUP5  OpCode = 0x84
	DUP6  OpCode = 0x85
	DUP7  OpCode = 0x86
	DUP8  OpCode = 0x87
	DUP9  OpCode = 0x88
	DUP10 OpCode = 0x89
	DUP11 OpCode = 0x8A
	DUP12 OpCode = 0x8B
	DUP13 OpCode = 0x8C
	DUP14 OpCode = 0x8D
	DUP15 OpCode = 0x8E
	DUP16 OpCode = 0x8F
)

// 0x90 range - swap
const (
	SWAP1  OpCode = 0x90
	SWAP2  OpCode = 0x91
	SWAP3  OpCode = 0x92
	SWAP4  OpCode = 0x93
	SWAP5  OpCode = 0x94
	SWAP6  OpCode = 0x95
	SWAP7  OpCode = 0x96
	SWAP8  OpCode = 0x97
	SWAP9  OpCode = 0x98
	SWAP10 OpCode = 0x99
	SWAP11 OpCode = 0x9A
	SWAP12 OpCode = 0x9B
	SWAP13 OpCode = 0x9C
	SWAP14 OpCode = 0x9D
	SWAP15 OpCode = 0x9E
	SWAP16 OpCode = 0x9F
)

// 0xa0 range - logging
const (
	LOG0 OpCode = 0xA0
	LOG1 OpCode = 0xA1
	LOG2 OpCode = 0xA2
	LOG3 OpCode = 0xA3
	LOG4 OpCode = 0xA4
)

// 0xd0 range - TRON extensions (Op.java)
const (
	CALLTOKEN              OpCode = 0xD0 // TRC-10 token call       (allow_tvm_transfer_trc10)
	TOKENBALANCE           OpCode = 0xD1 // TRC-10 balance query    (allow_tvm_transfer_trc10)
	CALLTOKENVALUE         OpCode = 0xD2 // incoming token value    (allow_tvm_transfer_trc10)
	CALLTOKENID            OpCode = 0xD3 // incoming token ID       (allow_tvm_transfer_trc10)
	ISCONTRACT             OpCode = 0xD4 // is address a contract   (allow_tvm_solidity059)
	FREEZE                 OpCode = 0xD5 // freeze TRX (V1)         (allow_tvm_freeze)
	UNFREEZE               OpCode = 0xD6 // unfreeze TRX (V1)       (allow_tvm_freeze)
	FREEZEEXPIRETIME       OpCode = 0xD7 // V1 freeze expire time   (allow_tvm_freeze)
	VOTEWITNESS            OpCode = 0xD8 // vote for SR             (allow_tvm_vote)
	WITHDRAWREWARD         OpCode = 0xD9 // withdraw voting reward  (allow_tvm_vote)
	FREEZEBALANCEV2        OpCode = 0xDA // stake V2                (allow_staking_v2)
	UNFREEZEBALANCEV2      OpCode = 0xDB // unstake V2              (allow_staking_v2)
	CANCELALLUNFREEZEV2    OpCode = 0xDC // cancel all V2 unstakes  (allow_staking_v2)
	WITHDRAWEXPIREUNFREEZE OpCode = 0xDD // withdraw expired V2     (allow_staking_v2)
	DELEGATERESOURCE       OpCode = 0xDE // delegate resource       (allow_staking_v2)
	UNDELEGATERESOURCE     OpCode = 0xDF // undelegate resource     (allow_staking_v2)
)

// 0xf0 range - system
const (
	CREATE       OpCode = 0xF0
	CALL         OpCode = 0xF1
	CALLCODE     OpCode = 0xF2
	RETURN       OpCode = 0xF3
	DELEGATECALL OpCode = 0xF4
	CREATE2      OpCode = 0xF5
	STATICCALL   OpCode = 0xFA
	REVERT       OpCode = 0xFD
	SELFDESTRUCT OpCode = 0xFF
)

var opCodeNames = map[OpCode]string{
	STOP: "STOP", ADD: "ADD", MUL: "MUL", SUB: "SUB",
	DIV: "DIV", SDIV: "SDIV", MOD: "MOD", SMOD: "SMOD",
	ADDMOD: "ADDMOD", MULMOD: "MULMOD", EXP: "EXP", SIGNEXTEND: "SIGNEXTEND",
	LT: "LT", GT: "GT", SLT: "SLT", SGT: "SGT", EQ: "EQ", ISZERO: "ISZERO",
	AND: "AND", OR: "OR", XOR: "XOR", NOT: "NOT", BYTE: "BYTE",
	SHL: "SHL", SHR: "SHR", SAR: "SAR", CLZ: "CLZ", SHA3: "SHA3",
	ADDRESS: "ADDRESS", BALANCE: "BALANCE", ORIGIN: "ORIGIN",
	CALLER: "CALLER", CALLVALUE: "CALLVALUE",
	CALLDATALOAD: "CALLDATALOAD", CALLDATASIZE: "CALLDATASIZE", CALLDATACOPY: "CALLDATACOPY",
	CODESIZE: "CODESIZE", CODECOPY: "CODECOPY", GASPRICE: "GASPRICE",
	EXTCODESIZE: "EXTCODESIZE", EXTCODECOPY: "EXTCODECOPY",
	RETURNDATASIZE: "RETURNDATASIZE", RETURNDATACOPY: "RETURNDATACOPY",
	EXTCODEHASH: "EXTCODEHASH",
	BLOCKHASH:   "BLOCKHASH", COINBASE: "COINBASE", TIMESTAMP: "TIMESTAMP",
	NUMBER: "NUMBER", DIFFICULTY: "DIFFICULTY", GASLIMIT: "GASLIMIT",
	CHAINID: "CHAINID", SELFBALANCE: "SELFBALANCE", BASEFEE: "BASEFEE",
	BLOBHASH: "BLOBHASH", BLOBBASEFEE: "BLOBBASEFEE",
	POP: "POP", MLOAD: "MLOAD", MSTORE: "MSTORE", MSTORE8: "MSTORE8",
	SLOAD: "SLOAD", SSTORE: "SSTORE", JUMP: "JUMP", JUMPI: "JUMPI",
	PC: "PC", MSIZE: "MSIZE", GAS: "GAS", JUMPDEST: "JUMPDEST",
	TLOAD: "TLOAD", TSTORE: "TSTORE", MCOPY: "MCOPY",
	PUSH0: "PUSH0",
	PUSH1: "PUSH1", PUSH2: "PUSH2", PUSH3: "PUSH3", PUSH4: "PUSH4",
	PUSH5: "PUSH5", PUSH6: "PUSH6", PUSH7: "PUSH7", PUSH8: "PUSH8",
	PUSH9: "PUSH9", PUSH10: "PUSH10", PUSH11: "PUSH11", PUSH12: "PUSH12",
	PUSH13: "PUSH13", PUSH14: "PUSH14", PUSH15: "PUSH15", PUSH16: "PUSH16",
	PUSH17: "PUSH17", PUSH18: "PUSH18", PUSH19: "PUSH19", PUSH20: "PUSH20",
	PUSH21: "PUSH21", PUSH22: "PUSH22", PUSH23: "PUSH23", PUSH24: "PUSH24",
	PUSH25: "PUSH25", PUSH26: "PUSH26", PUSH27: "PUSH27", PUSH28: "PUSH28",
	PUSH29: "PUSH29", PUSH30: "PUSH30", PUSH31: "PUSH31", PUSH32: "PUSH32",
	DUP1: "DUP1", DUP2: "DUP2", DUP3: "DUP3", DUP4: "DUP4",
	DUP5: "DUP5", DUP6: "DUP6", DUP7: "DUP7", DUP8: "DUP8",
	DUP9: "DUP9", DUP10: "DUP10", DUP11: "DUP11", DUP12: "DUP12",
	DUP13: "DUP13", DUP14: "DUP14", DUP15: "DUP15", DUP16: "DUP16",
	SWAP1: "SWAP1", SWAP2: "SWAP2", SWAP3: "SWAP3", SWAP4: "SWAP4",
	SWAP5: "SWAP5", SWAP6: "SWAP6", SWAP7: "SWAP7", SWAP8: "SWAP8",
	SWAP9: "SWAP9", SWAP10: "SWAP10", SWAP11: "SWAP11", SWAP12: "SWAP12",
	SWAP13: "SWAP13", SWAP14: "SWAP14", SWAP15: "SWAP15", SWAP16: "SWAP16",
	LOG0: "LOG0", LOG1: "LOG1", LOG2: "LOG2", LOG3: "LOG3", LOG4: "LOG4",
	CREATE: "CREATE", CALL: "CALL", CALLCODE: "CALLCODE",
	RETURN: "RETURN", DELEGATECALL: "DELEGATECALL", CREATE2: "CREATE2",
	STATICCALL: "STATICCALL", REVERT: "REVERT", SELFDESTRUCT: "SELFDESTRUCT",
	// TRON extensions
	CALLTOKEN: "CALLTOKEN", TOKENBALANCE: "TOKENBALANCE",
	CALLTOKENVALUE: "CALLTOKENVALUE", CALLTOKENID: "CALLTOKENID",
	ISCONTRACT: "ISCONTRACT",
	FREEZE:     "FREEZE", UNFREEZE: "UNFREEZE", FREEZEEXPIRETIME: "FREEZEEXPIRETIME",
	VOTEWITNESS: "VOTEWITNESS", WITHDRAWREWARD: "WITHDRAWREWARD",
	FREEZEBALANCEV2: "FREEZEBALANCEV2", UNFREEZEBALANCEV2: "UNFREEZEBALANCEV2",
	CANCELALLUNFREEZEV2: "CANCELALLUNFREEZEV2", WITHDRAWEXPIREUNFREEZE: "WITHDRAWEXPIREUNFREEZE",
	DELEGATERESOURCE: "DELEGATERESOURCE", UNDELEGATERESOURCE: "UNDELEGATERESOURCE",
}

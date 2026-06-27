package vm

// executionFunc is the signature for opcode implementations.
type executionFunc func(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error)

// operation represents a single opcode's metadata.
type operation struct {
	execute    executionFunc
	energyCost uint64               // static energy cost (0 means dynamic)
	minStack   int                  // minimum stack items required
	maxStack   int                  // maximum stack items after execution
	writes     bool                 // true if this opcode modifies state
	enabledFn  func(TVMConfig) bool // nil means always enabled
}

// JumpTable is the dispatch table mapping opcodes to operations.
type JumpTable [256]*operation

// newJumpTable creates the standard jump table with all supported opcodes.
func newJumpTable() JumpTable {
	var tbl JumpTable

	// Arithmetic
	tbl[ADD] = &operation{execute: opAdd, energyCost: EnergyVeryLow, minStack: 2, maxStack: 1023}
	tbl[MUL] = &operation{execute: opMul, energyCost: EnergyLow, minStack: 2, maxStack: 1023}
	tbl[SUB] = &operation{execute: opSub, energyCost: EnergyVeryLow, minStack: 2, maxStack: 1023}
	tbl[DIV] = &operation{execute: opDiv, energyCost: EnergyLow, minStack: 2, maxStack: 1023}
	tbl[SDIV] = &operation{execute: opSdiv, energyCost: EnergyLow, minStack: 2, maxStack: 1023}
	tbl[MOD] = &operation{execute: opMod, energyCost: EnergyLow, minStack: 2, maxStack: 1023}
	tbl[SMOD] = &operation{execute: opSmod, energyCost: EnergyLow, minStack: 2, maxStack: 1023}
	tbl[ADDMOD] = &operation{execute: opAddmod, energyCost: EnergyMid, minStack: 3, maxStack: 1022}
	tbl[MULMOD] = &operation{execute: opMulmod, energyCost: EnergyMid, minStack: 3, maxStack: 1022}
	tbl[EXP] = &operation{execute: opExp, minStack: 2, maxStack: 1023}
	tbl[SIGNEXTEND] = &operation{execute: opSignExtend, energyCost: EnergyLow, minStack: 2, maxStack: 1023}

	// Comparison & Bitwise
	tbl[LT] = &operation{execute: opLt, energyCost: EnergyVeryLow, minStack: 2, maxStack: 1023}
	tbl[GT] = &operation{execute: opGt, energyCost: EnergyVeryLow, minStack: 2, maxStack: 1023}
	tbl[SLT] = &operation{execute: opSlt, energyCost: EnergyVeryLow, minStack: 2, maxStack: 1023}
	tbl[SGT] = &operation{execute: opSgt, energyCost: EnergyVeryLow, minStack: 2, maxStack: 1023}
	tbl[EQ] = &operation{execute: opEq, energyCost: EnergyVeryLow, minStack: 2, maxStack: 1023}
	tbl[ISZERO] = &operation{execute: opIszero, energyCost: EnergyVeryLow, minStack: 1, maxStack: 1024}
	tbl[AND] = &operation{execute: opAnd, energyCost: EnergyVeryLow, minStack: 2, maxStack: 1023}
	tbl[OR] = &operation{execute: opOr, energyCost: EnergyVeryLow, minStack: 2, maxStack: 1023}
	tbl[XOR] = &operation{execute: opXor, energyCost: EnergyVeryLow, minStack: 2, maxStack: 1023}
	tbl[NOT] = &operation{execute: opNot, energyCost: EnergyVeryLow, minStack: 1, maxStack: 1024}
	tbl[BYTE] = &operation{execute: opByte, energyCost: EnergyVeryLow, minStack: 2, maxStack: 1023}
	tbl[SHL] = &operation{execute: opSHL, energyCost: EnergyVeryLow, minStack: 2, maxStack: 1023}
	tbl[SHR] = &operation{execute: opSHR, energyCost: EnergyVeryLow, minStack: 2, maxStack: 1023}
	tbl[SAR] = &operation{execute: opSAR, energyCost: EnergyVeryLow, minStack: 2, maxStack: 1023}
	tbl[CLZ] = &operation{execute: opClz, energyCost: EnergyLow, minStack: 1, maxStack: 1024}

	// SHA3
	tbl[SHA3] = &operation{execute: opSHA3, minStack: 2, maxStack: 1023}

	// Environment
	tbl[ADDRESS] = &operation{execute: opAddress, energyCost: EnergyBase, minStack: 0, maxStack: 1024}
	tbl[BALANCE] = &operation{execute: opBalance, energyCost: EnergyBalance, minStack: 1, maxStack: 1024}
	tbl[ORIGIN] = &operation{execute: opOrigin, energyCost: EnergyBase, minStack: 0, maxStack: 1024}
	tbl[CALLER] = &operation{execute: opCaller, energyCost: EnergyBase, minStack: 0, maxStack: 1024}
	tbl[CALLVALUE] = &operation{execute: opCallValue, energyCost: EnergyBase, minStack: 0, maxStack: 1024}
	tbl[CALLDATALOAD] = &operation{execute: opCallDataLoad, energyCost: EnergyVeryLow, minStack: 1, maxStack: 1024}
	tbl[CALLDATASIZE] = &operation{execute: opCallDataSize, energyCost: EnergyBase, minStack: 0, maxStack: 1024}
	tbl[CALLDATACOPY] = &operation{execute: opCallDataCopy, minStack: 3, maxStack: 1021}
	tbl[CODESIZE] = &operation{execute: opCodeSize, energyCost: EnergyBase, minStack: 0, maxStack: 1024}
	tbl[CODECOPY] = &operation{execute: opCodeCopy, minStack: 3, maxStack: 1021}
	tbl[EXTCODESIZE] = &operation{execute: opExtCodeSize, energyCost: EnergyExtCodeSize, minStack: 1, maxStack: 1024}
	tbl[EXTCODECOPY] = &operation{execute: opExtCodeCopy, minStack: 4, maxStack: 1020}
	tbl[RETURNDATASIZE] = &operation{execute: opReturnDataSize, energyCost: EnergyBase, minStack: 0, maxStack: 1024}
	tbl[RETURNDATACOPY] = &operation{execute: opReturnDataCopy, minStack: 3, maxStack: 1021}
	tbl[EXTCODEHASH] = &operation{execute: opExtCodeHash, energyCost: EnergyExtCodeHash, minStack: 1, maxStack: 1024}
	tbl[GASPRICE] = &operation{execute: opGasPrice, energyCost: EnergyBase, minStack: 0, maxStack: 1024}

	// Block information
	tbl[BLOCKHASH] = &operation{execute: opBlockHash, energyCost: EnergyBlockHash, minStack: 1, maxStack: 1024}
	tbl[COINBASE] = &operation{execute: opCoinbase, energyCost: EnergyBase, minStack: 0, maxStack: 1024}
	tbl[TIMESTAMP] = &operation{execute: opTimestamp, energyCost: EnergyBase, minStack: 0, maxStack: 1024}
	tbl[NUMBER] = &operation{execute: opNumber, energyCost: EnergyBase, minStack: 0, maxStack: 1024}
	tbl[DIFFICULTY] = &operation{execute: opDifficulty, energyCost: EnergyBase, minStack: 0, maxStack: 1024}
	tbl[GASLIMIT] = &operation{execute: opGasLimit, energyCost: EnergyBase, minStack: 0, maxStack: 1024}
	tbl[CHAINID] = &operation{execute: opChainID, energyCost: EnergyBase, minStack: 0, maxStack: 1024}
	tbl[SELFBALANCE] = &operation{execute: opSelfBalance, energyCost: EnergySelfBalance, minStack: 0, maxStack: 1024}
	tbl[BASEFEE] = &operation{execute: opBaseFee, energyCost: EnergyBase, minStack: 0, maxStack: 1024}
	tbl[BLOBHASH] = &operation{execute: opBlobHash, energyCost: EnergyVeryLow, minStack: 1, maxStack: 1024}
	tbl[BLOBBASEFEE] = &operation{execute: opBlobBaseFee, energyCost: EnergyBase, minStack: 0, maxStack: 1024}

	// Stack/Memory/Storage
	tbl[POP] = &operation{execute: opPop, energyCost: EnergyBase, minStack: 1, maxStack: 1024}
	tbl[MLOAD] = &operation{execute: opMload, minStack: 1, maxStack: 1024}
	tbl[MSTORE] = &operation{execute: opMstore, minStack: 2, maxStack: 1024}
	tbl[MSTORE8] = &operation{execute: opMstore8, minStack: 2, maxStack: 1024}
	tbl[SLOAD] = &operation{execute: opSload, energyCost: EnergySload, minStack: 1, maxStack: 1024}
	tbl[SSTORE] = &operation{execute: opSstore, minStack: 2, maxStack: 1024, writes: true}
	tbl[JUMP] = &operation{execute: opJump, energyCost: EnergyMid, minStack: 1, maxStack: 1024}
	tbl[JUMPI] = &operation{execute: opJumpi, energyCost: EnergyHigh, minStack: 2, maxStack: 1024}
	tbl[PC] = &operation{execute: opPc, energyCost: EnergyBase, minStack: 0, maxStack: 1024}
	tbl[MSIZE] = &operation{execute: opMsize, energyCost: EnergyBase, minStack: 0, maxStack: 1024}
	tbl[GAS] = &operation{execute: opGas, energyCost: EnergyBase, minStack: 0, maxStack: 1024}
	tbl[JUMPDEST] = &operation{execute: opJumpdest, energyCost: EnergyJumpDest, minStack: 0, maxStack: 1024}

	// Push
	tbl[PUSH0] = &operation{execute: opPush0, energyCost: EnergyBase, minStack: 0, maxStack: 1024}
	for i := 1; i <= 32; i++ {
		n := i
		tbl[PUSH1+OpCode(i-1)] = &operation{
			execute:    makePush(n),
			energyCost: EnergyVeryLow,
			minStack:   0,
			maxStack:   1024,
		}
	}

	// Dup
	for i := 1; i <= 16; i++ {
		n := i
		tbl[DUP1+OpCode(i-1)] = &operation{
			execute:    makeDup(n),
			energyCost: EnergyVeryLow,
			minStack:   n,
			maxStack:   1025 - n,
		}
	}

	// Swap
	for i := 1; i <= 16; i++ {
		n := i
		tbl[SWAP1+OpCode(i-1)] = &operation{
			execute:    makeSwap(n),
			energyCost: EnergyVeryLow,
			minStack:   n + 1,
			maxStack:   1024,
		}
	}

	// Log
	for i := 0; i <= 4; i++ {
		n := i
		tbl[LOG0+OpCode(i)] = &operation{
			execute:  makeLog(n),
			minStack: 2 + n,
			maxStack: 1024,
			writes:   true,
		}
	}

	// System
	tbl[STOP] = &operation{execute: opStop, energyCost: EnergyZero, minStack: 0, maxStack: 1024}
	tbl[RETURN] = &operation{execute: opReturn, energyCost: EnergyZero, minStack: 2, maxStack: 1024}
	tbl[REVERT] = &operation{execute: opRevert, energyCost: EnergyZero, minStack: 2, maxStack: 1024}
	tbl[SELFDESTRUCT] = &operation{execute: opSelfDestruct, minStack: 1, maxStack: 1024, writes: true}
	// CREATE/CREATE2 charge the 32000 base INSIDE the handler, after the 3 MB
	// memory OOM guard (java getCreateCost = CREATE + calcMemEnergy, OOM-first).
	// Charging it here as a static cost would let the interpreter loop spend it
	// ahead of the memory check, flipping OUT_OF_MEMORY to OUT_OF_ENERGY.
	tbl[CREATE] = &operation{execute: opCreate, minStack: 3, maxStack: 1022, writes: true}
	tbl[CREATE2] = &operation{execute: opCreate2, minStack: 4, maxStack: 1021, writes: true}
	tbl[CALL] = &operation{execute: opCall, minStack: 7, maxStack: 1018}
	tbl[CALLCODE] = &operation{execute: opCallCode, minStack: 7, maxStack: 1018}
	tbl[DELEGATECALL] = &operation{execute: opDelegateCall, minStack: 6, maxStack: 1019}
	tbl[STATICCALL] = &operation{execute: opStaticCall, minStack: 6, maxStack: 1019}

	// Constantinople opcodes — require AllowTvmConstantinople
	constantinople := func(c TVMConfig) bool { return c.Constantinople }
	tbl[SHL].enabledFn = constantinople
	tbl[SHR].enabledFn = constantinople
	tbl[SAR].enabledFn = constantinople
	tbl[EXTCODEHASH].enabledFn = constantinople
	tbl[CREATE2].enabledFn = constantinople

	// Istanbul opcodes — require AllowTvmIstanbul
	istanbul := func(c TVMConfig) bool { return c.Istanbul }
	tbl[CHAINID].enabledFn = istanbul
	tbl[SELFBALANCE].enabledFn = istanbul

	// London opcodes — require AllowTvmLondon
	tbl[BASEFEE].enabledFn = func(c TVMConfig) bool { return c.London }

	// Shanghai opcodes — require AllowTvmShanghai
	tbl[PUSH0].enabledFn = func(c TVMConfig) bool { return c.Shanghai }

	// Osaka opcodes — require AllowTvmOsaka
	tbl[CLZ].enabledFn = func(c TVMConfig) bool { return c.Osaka }

	// Cancun opcodes — require AllowTvmCancun (EIP-1153 transient storage)
	tbl[TLOAD] = &operation{execute: opTLoad, energyCost: EnergyTLoad, minStack: 1, maxStack: 1024,
		enabledFn: func(c TVMConfig) bool { return c.Cancun }}
	tbl[TSTORE] = &operation{execute: opTStore, energyCost: EnergyTStore, minStack: 2, maxStack: 1022,
		writes: true, enabledFn: func(c TVMConfig) bool { return c.Cancun }}
	tbl[MCOPY] = &operation{execute: opMCopy, minStack: 3, maxStack: 1021,
		enabledFn: func(c TVMConfig) bool { return c.Cancun }}
	tbl[BLOBHASH].enabledFn = func(c TVMConfig) bool { return c.Blob }
	tbl[BLOBBASEFEE].enabledFn = func(c TVMConfig) bool { return c.Blob }

	// TRON extensions — TRC-10 token transfer (AllowTvmTransferTrc10)
	transferTrc10 := func(c TVMConfig) bool { return c.TransferTrc10 }
	tbl[CALLTOKEN] = &operation{execute: opCallToken, minStack: 8, maxStack: 1017,
		enabledFn: transferTrc10}
	tbl[TOKENBALANCE] = &operation{execute: opTokenBalance, energyCost: EnergyTokenBalance,
		minStack: 2, maxStack: 1023, enabledFn: transferTrc10}
	tbl[CALLTOKENVALUE] = &operation{execute: opCallTokenValue, energyCost: EnergyBase,
		minStack: 0, maxStack: 1024, enabledFn: transferTrc10}
	tbl[CALLTOKENID] = &operation{execute: opCallTokenId, energyCost: EnergyBase,
		minStack: 0, maxStack: 1024, enabledFn: transferTrc10}

	// TRON extensions — ISCONTRACT (AllowTvmSolidity059)
	tbl[ISCONTRACT] = &operation{execute: opIsContract, energyCost: EnergyIsContract,
		minStack: 1, maxStack: 1024,
		enabledFn: func(c TVMConfig) bool { return c.Solidity059 }}

	// TRON extensions — V1 freeze/unfreeze (AllowTvmFreeze)
	freeze := func(c TVMConfig) bool { return c.Freeze }
	tbl[FREEZE] = &operation{execute: opFreeze, energyCost: EnergyFreeze,
		minStack: 3, maxStack: 1022, enabledFn: freeze}
	tbl[UNFREEZE] = &operation{execute: opUnfreeze, energyCost: EnergyUnfreeze,
		minStack: 2, maxStack: 1023, enabledFn: freeze}
	tbl[FREEZEEXPIRETIME] = &operation{execute: opFreezeExpireTime, energyCost: EnergyFreezeExpireTime,
		minStack: 2, maxStack: 1023, enabledFn: freeze}

	// TRON extensions — voting / rewards (AllowTvmVote)
	vote := func(c TVMConfig) bool { return c.Vote }
	// VOTEWITNESS charges the 30000 VOTE_WITNESS base INSIDE the handler, after the
	// 3 MB memory OOM guard (java getVoteWitnessCost = VOTE_WITNESS + calcMemEnergy,
	// OOM-first). A static cost here would let the interpreter loop spend it ahead
	// of the memory check, flipping OUT_OF_MEMORY to OUT_OF_ENERGY.
	tbl[VOTEWITNESS] = &operation{execute: opVoteWitness,
		minStack: 4, maxStack: 1021, writes: true, enabledFn: vote}
	tbl[WITHDRAWREWARD] = &operation{execute: opWithdrawReward, energyCost: EnergyWithdrawReward,
		minStack: 0, maxStack: 1024, writes: true, enabledFn: vote}

	// TRON extensions — Stake 2.0 (AllowStakingV2)
	stakingV2 := func(c TVMConfig) bool { return c.StakingV2 }
	tbl[FREEZEBALANCEV2] = &operation{execute: opFreezeBalanceV2, energyCost: EnergyFreezeV2,
		minStack: 2, maxStack: 1023, writes: true, enabledFn: stakingV2}
	tbl[UNFREEZEBALANCEV2] = &operation{execute: opUnfreezeBalanceV2, energyCost: EnergyUnfreezeV2,
		minStack: 2, maxStack: 1023, writes: true, enabledFn: stakingV2}
	tbl[CANCELALLUNFREEZEV2] = &operation{execute: opCancelAllUnfreezeV2, energyCost: EnergyCancelAllUnfreezeV2,
		minStack: 0, maxStack: 1024, writes: true, enabledFn: stakingV2}
	tbl[WITHDRAWEXPIREUNFREEZE] = &operation{execute: opWithdrawExpireUnfreeze, energyCost: EnergyWithdrawExpireUnfreeze,
		minStack: 0, maxStack: 1024, writes: true, enabledFn: stakingV2}
	tbl[DELEGATERESOURCE] = &operation{execute: opDelegateResource, energyCost: EnergyDelegateResource,
		minStack: 3, maxStack: 1022, writes: true, enabledFn: stakingV2}
	tbl[UNDELEGATERESOURCE] = &operation{execute: opUnDelegateResource, energyCost: EnergyUnDelegateResource,
		minStack: 3, maxStack: 1022, writes: true, enabledFn: stakingV2}

	return tbl
}

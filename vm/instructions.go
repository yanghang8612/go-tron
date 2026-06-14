package vm

import (
	"math/bits"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"golang.org/x/crypto/sha3"
)

// --- Arithmetic ---

func opAdd(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x, y := stack.pop(), stack.peek()
	y.Add(&x, y)
	return nil, nil
}

func opMul(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x, y := stack.pop(), stack.peek()
	y.Mul(&x, y)
	return nil, nil
}

func opSub(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x, y := stack.pop(), stack.peek()
	y.Sub(&x, y)
	return nil, nil
}

func opDiv(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x, y := stack.pop(), stack.peek()
	if y.IsZero() {
		y.Clear()
	} else {
		y.Div(&x, y)
	}
	return nil, nil
}

func opSdiv(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x, y := stack.pop(), stack.peek()
	if y.IsZero() {
		y.Clear()
	} else {
		y.SDiv(&x, y)
	}
	return nil, nil
}

func opMod(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x, y := stack.pop(), stack.peek()
	if y.IsZero() {
		y.Clear()
	} else {
		y.Mod(&x, y)
	}
	return nil, nil
}

func opSmod(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x, y := stack.pop(), stack.peek()
	if y.IsZero() {
		y.Clear()
	} else {
		y.SMod(&x, y)
	}
	return nil, nil
}

func opAddmod(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x, y, z := stack.pop(), stack.pop(), stack.peek()
	if z.IsZero() {
		z.Clear()
	} else {
		z.AddMod(&x, &y, z)
	}
	return nil, nil
}

func opMulmod(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x, y, z := stack.pop(), stack.pop(), stack.peek()
	if z.IsZero() {
		z.Clear()
	} else {
		z.MulMod(&x, &y, z)
	}
	return nil, nil
}

func opExp(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	base, exponent := stack.pop(), stack.peek()
	byteLen := uint64(exponent.ByteLen())
	cost := EnergyExp + EnergyExpByte*byteLen
	if !interpreter.useEnergy(contract, cost) {
		return nil, ErrOutOfEnergy
	}
	exponent.Exp(&base, exponent)
	return nil, nil
}

func opSignExtend(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	back, num := stack.pop(), stack.peek()
	num.ExtendSign(num, &back)
	return nil, nil
}

// --- Comparison ---

func opLt(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x, y := stack.pop(), stack.peek()
	if x.Lt(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	return nil, nil
}

func opGt(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x, y := stack.pop(), stack.peek()
	if x.Gt(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	return nil, nil
}

func opSlt(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x, y := stack.pop(), stack.peek()
	if x.Slt(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	return nil, nil
}

func opSgt(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x, y := stack.pop(), stack.peek()
	if x.Sgt(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	return nil, nil
}

func opEq(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x, y := stack.pop(), stack.peek()
	if x.Eq(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	return nil, nil
}

func opIszero(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x := stack.peek()
	if x.IsZero() {
		x.SetOne()
	} else {
		x.Clear()
	}
	return nil, nil
}

// --- Bitwise ---

func opAnd(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x, y := stack.pop(), stack.peek()
	y.And(&x, y)
	return nil, nil
}

func opOr(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x, y := stack.pop(), stack.peek()
	y.Or(&x, y)
	return nil, nil
}

func opXor(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x, y := stack.pop(), stack.peek()
	y.Xor(&x, y)
	return nil, nil
}

func opNot(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x := stack.peek()
	x.Not(x)
	return nil, nil
}

func opByte(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	th, val := stack.pop(), stack.peek()
	if th.LtUint64(32) {
		b := val.Bytes32()
		val.Clear()
		val.SetUint64(uint64(b[th.Uint64()]))
	} else {
		val.Clear()
	}
	return nil, nil
}

func opSHL(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	shift, value := stack.pop(), stack.peek()
	if shift.LtUint64(256) {
		value.Lsh(value, uint(shift.Uint64()))
	} else {
		value.Clear()
	}
	return nil, nil
}

func opSHR(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	shift, value := stack.pop(), stack.peek()
	if shift.LtUint64(256) {
		value.Rsh(value, uint(shift.Uint64()))
	} else {
		value.Clear()
	}
	return nil, nil
}

func opSAR(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	shift, value := stack.pop(), stack.peek()
	if shift.GtUint64(255) {
		if value.Sign() >= 0 {
			value.Clear()
		} else {
			value.SetAllOne()
		}
	} else {
		value.SRsh(value, uint(shift.Uint64()))
	}
	return nil, nil
}

func opClz(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	word := stack.pop()
	data := word.Bytes32()
	count := 0
	for _, b := range data {
		if b == 0 {
			count += 8
			continue
		}
		count += bits.LeadingZeros8(b)
		break
	}
	stack.push(uint256.NewInt(uint64(count)))
	return nil, nil
}

// --- SHA3 ---

func opSHA3(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	offset, size := stack.pop(), stack.peek()
	off, sz, memCost, err := checkedMemoryExpansionCostWords(memory, &offset, size, SHA3)
	if err != nil {
		return nil, err
	}
	if memCost > 0 {
		if !interpreter.useEnergy(contract, memCost) {
			return nil, ErrOutOfEnergy
		}
	}
	resizeMemory(memory, off, sz)
	words := toWordSize(sz)
	cost := EnergySHA3 + EnergySHA3Word*words
	if !interpreter.useEnergy(contract, cost) {
		return nil, ErrOutOfEnergy
	}
	data := memory.getCopy(int64(off), int64(sz))
	h := sha3.NewLegacyKeccak256()
	h.Write(data)
	var hash tcommon.Hash
	h.Sum(hash[:0])
	size.SetBytes(hash.Bytes())
	return nil, nil
}

// --- Environment ---

func opAddress(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	v := addressToUint256WithMode(contract.Address, interpreter.tvmConfig.MultiSign)
	stack.push(&v)
	return nil, nil
}

func opBalance(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	addr := stack.peek()
	address := uint256ToAddress(addr)
	balance := interpreter.tvm.StateDB.GetBalance(address)
	addr.SetUint64(uint64(balance))
	return nil, nil
}

func opOrigin(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	v := addressToUint256WithMode(interpreter.tvm.Origin, interpreter.tvmConfig.MultiSign)
	stack.push(&v)
	return nil, nil
}

func opCaller(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	v := addressToUint256(contract.Caller)
	stack.push(&v)
	return nil, nil
}

func opCallValue(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	v := uint256.NewInt(uint64(contract.Value))
	stack.push(v)
	return nil, nil
}

func opCallDataLoad(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	x := stack.peek()
	offset := x.Uint64()
	var data [32]byte
	input := contract.Input
	if offset < uint64(len(input)) {
		copy(data[:], input[offset:])
	}
	x.SetBytes(data[:])
	return nil, nil
}

func opCallDataSize(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	v := uint256.NewInt(uint64(len(contract.Input)))
	stack.push(v)
	return nil, nil
}

func opCallDataCopy(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	memOffset, dataOffset, length := stack.pop(), stack.pop(), stack.pop()
	off, size, memCost, err := checkedMemoryExpansionCostWords(memory, &memOffset, &length, CALLDATACOPY)
	if err != nil {
		return nil, err
	}
	words := toWordSize(size)
	// java-tron `EnergyCost.getCallDataCopyCost` charges only memDelta + copy
	// energy — there is no per-op base tier. Mirror that exactly.
	cost := EnergyCopy*words + memCost
	if !interpreter.useEnergy(contract, cost) {
		return nil, ErrOutOfEnergy
	}
	resizeMemory(memory, off, size)
	data := getDataSlice(contract.Input, dataOffset.Uint64(), size)
	memory.set(off, size, data)
	return nil, nil
}

func opCodeSize(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	v := uint256.NewInt(uint64(len(contract.Code)))
	stack.push(v)
	return nil, nil
}

func opCodeCopy(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	memOffset, codeOffset, length := stack.pop(), stack.pop(), stack.pop()
	off, size, memCost, err := checkedMemoryExpansionCostWords(memory, &memOffset, &length, CODECOPY)
	if err != nil {
		return nil, err
	}
	words := toWordSize(size)
	// java-tron `EnergyCost.getCodeCopyCost` charges only memDelta + copy
	// energy — no per-op base tier. Mirror that exactly.
	cost := EnergyCopy*words + memCost
	if !interpreter.useEnergy(contract, cost) {
		return nil, ErrOutOfEnergy
	}
	resizeMemory(memory, off, size)
	data := getDataSlice(contract.Code, codeOffset.Uint64(), size)
	memory.set(off, size, data)
	return nil, nil
}

func opExtCodeSize(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	addr := stack.peek()
	address := uint256ToAddress(addr)
	size := interpreter.tvm.StateDB.GetCodeSize(address)
	addr.SetUint64(uint64(size))
	return nil, nil
}

func opExtCodeCopy(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	a, memOffset, codeOffset, length := stack.pop(), stack.pop(), stack.pop(), stack.pop()
	address := uint256ToAddress(&a)
	off, size, memCost, err := checkedMemoryExpansionCostWords(memory, &memOffset, &length, EXTCODECOPY)
	if err != nil {
		return nil, err
	}
	words := toWordSize(size)
	cost := EnergyExtCodeCopy + EnergyCopy*words + memCost
	if !interpreter.useEnergy(contract, cost) {
		return nil, ErrOutOfEnergy
	}
	resizeMemory(memory, off, size)
	code := interpreter.tvm.StateDB.GetCode(address)
	data := getDataSlice(code, codeOffset.Uint64(), size)
	memory.set(off, size, data)
	return nil, nil
}

func opReturnDataSize(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	v := uint256.NewInt(uint64(len(interpreter.returnData)))
	stack.push(v)
	return nil, nil
}

func opReturnDataCopy(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	memOffset, dataOffset, length := stack.pop(), stack.pop(), stack.pop()
	off, size, memCost, err := checkedMemoryExpansionCostWords(memory, &memOffset, &length, RETURNDATACOPY)
	if err != nil {
		return nil, err
	}
	if !dataOffset.IsUint64() {
		return nil, ErrReturnDataOutOfBounds
	}
	dataStart := dataOffset.Uint64()
	returnDataSize := uint64(len(interpreter.returnData))
	if dataStart > returnDataSize || size > returnDataSize-dataStart {
		return nil, ErrReturnDataOutOfBounds
	}
	end := dataStart + size
	words := toWordSize(size)
	// java-tron `EnergyCost.getReturnDataCopyCost` charges only memDelta +
	// copy energy — no per-op base tier. Mirror that exactly.
	cost := EnergyCopy*words + memCost
	if !interpreter.useEnergy(contract, cost) {
		return nil, ErrOutOfEnergy
	}
	resizeMemory(memory, off, size)
	memory.set(off, size, interpreter.returnData[dataStart:end])
	return nil, nil
}

func opExtCodeHash(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	addr := stack.peek()
	address := uint256ToAddress(addr)
	if !interpreter.tvm.StateDB.Exist(address) {
		addr.Clear()
	} else {
		hash := interpreter.tvm.StateDB.GetCodeHash(address)
		addr.SetBytes(hash.Bytes())
	}
	return nil, nil
}

func opGasPrice(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	price := uint64(0)
	if interpreter.tvmConfig.Compatibility && interpreter.tvm.DynProps != nil && contract.Version == 1 {
		price = uint64(interpreter.tvm.DynProps.EnergyFee())
	}
	stack.push(uint256.NewInt(price))
	return nil, nil
}

// --- Block Information ---

const (
	blockHashHistoryWindow = uint64(256)
	javaMaxInt             = uint64(1<<31 - 1)
)

func opBlockHash(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	num := stack.peek()
	index := javaMaxInt
	if num.IsUint64() && !num.GtUint64(javaMaxInt) {
		index = num.Uint64()
	}

	current := interpreter.tvm.BlockNumber
	lower := uint64(0)
	if current > blockHashHistoryWindow {
		lower = current - blockHashHistoryWindow
	}
	if interpreter.tvm.DB == nil || index >= current || index < lower {
		num.Clear()
		return nil, nil
	}
	// The 256-block lookback window reaches PAST the freezer line: with the
	// default 128-block margin the slice-3 freezer deletes hot b-<num> rows
	// older than (solidified - 128), so a bare KV read goes blind for the
	// older part of the window. Nile stalled at block 16,745,722 exactly
	// here — JustLink VRF mixes blockhash(head-211) into its seed, the row
	// was already pruned, BLOCKHASH returned 0 and the proof check
	// reverted while java-tron (whose RecentBlockStore always covers 256
	// blocks) succeeded. Production paths hand the VM a store implementing
	// rawdb.BlockHashReader whose lookup falls through to ancient; the raw
	// KV read below remains as the fallback for tests with bare memdbs.
	if bhr, ok := interpreter.tvm.DB.(rawdb.BlockHashReader); ok {
		if h, found := bhr.BlockHashByNumber(index); found {
			num.SetBytes(h.Bytes())
		} else {
			num.Clear()
		}
		return nil, nil
	}
	block := rawdb.ReadBlockKV(interpreter.tvm.DB, index)
	if block == nil {
		num.Clear()
		return nil, nil
	}
	num.SetBytes(block.Hash().Bytes())
	return nil, nil
}

func opCoinbase(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	var v uint256.Int
	v.SetBytes(interpreter.tvm.Coinbase[:])
	stack.push(&v)
	return nil, nil
}

func opTimestamp(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	v := uint256.NewInt(uint64(interpreter.tvm.Timestamp / 1000))
	stack.push(v)
	return nil, nil
}

func opNumber(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	v := uint256.NewInt(interpreter.tvm.BlockNumber)
	stack.push(v)
	return nil, nil
}

func opDifficulty(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	stack.push(uint256.NewInt(0))
	return nil, nil
}

func opGasLimit(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	stack.push(uint256.NewInt(uint64(interpreter.tvm.StateDB.DynamicProperties().TotalEnergyCurrentLimit())))
	return nil, nil
}

func opChainID(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	if !(interpreter.tvmConfig.Compatibility || interpreter.tvmConfig.OptimizedReturnValueOfChainId) && interpreter.tvm.DB != nil {
		// Same freezer hazard as BLOCKHASH: once block 0 is frozen its hot
		// row is pruned and a bare KV read would silently fall through to
		// the (wrong) numeric ChainID. Resolve via BlockHashReader first.
		if bhr, ok := interpreter.tvm.DB.(rawdb.BlockHashReader); ok {
			if h, found := bhr.BlockHashByNumber(0); found {
				var v uint256.Int
				v.SetBytes(h.Bytes())
				stack.push(&v)
				return nil, nil
			}
		} else if genesis := rawdb.ReadBlockKV(interpreter.tvm.DB, 0); genesis != nil {
			var v uint256.Int
			v.SetBytes(genesis.Hash().Bytes())
			stack.push(&v)
			return nil, nil
		}
	}
	v := uint256.NewInt(uint64(interpreter.tvm.ChainID))
	stack.push(v)
	return nil, nil
}

func opSelfBalance(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	balance := interpreter.tvm.StateDB.GetBalance(contract.Address)
	stack.push(uint256.NewInt(uint64(balance)))
	return nil, nil
}

func opBaseFee(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	stack.push(uint256.NewInt(0))
	return nil, nil
}

func opBlobHash(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	stack.pop()
	stack.push(uint256.NewInt(0))
	return nil, nil
}

func opBlobBaseFee(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	stack.push(uint256.NewInt(0))
	return nil, nil
}

// --- Stack/Memory/Storage/Control ---

func opPop(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	stack.pop()
	return nil, nil
}

func opMload(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	offset := stack.peek()
	off, memCost, err := checkedMemoryExpansionCostFixed(memory, offset, 32, MLOAD)
	if err != nil {
		return nil, err
	}
	// java-tron `EnergyCost.getMloadCost` charges only memDelta by default.
	// When proposal #65 (`allow_higher_limit_for_max_cpu_time_of_one_tx`) is
	// active, `OperationRegistry.adjustMemOperations` rebases MLOAD to
	// `SPECIAL_TIER (1) + memDelta` (see EnergyCost.java:170-172).
	var cost uint64
	if interpreter.tvmConfig.HigherLimitForMaxCpuTimeOfOneTx {
		cost = EnergySpecial
	}
	cost += memCost
	if !interpreter.useEnergy(contract, cost) {
		return nil, ErrOutOfEnergy
	}
	memory.resize(off + 32)
	var v uint256.Int
	v.SetBytes(memory.getPtr(int64(off), 32))
	offset.Set(&v)
	return nil, nil
}

func opMstore(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	offset, val := stack.pop(), stack.pop()
	off, memCost, err := checkedMemoryExpansionCostFixed(memory, &offset, 32, MSTORE)
	if err != nil {
		return nil, err
	}
	// See opMload for the proposal-#65 base-tier note.
	var cost uint64
	if interpreter.tvmConfig.HigherLimitForMaxCpuTimeOfOneTx {
		cost = EnergySpecial
	}
	cost += memCost
	if !interpreter.useEnergy(contract, cost) {
		return nil, ErrOutOfEnergy
	}
	memory.resize(off + 32)
	memory.set32(off, &val)
	return nil, nil
}

func opMstore8(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	offset, val := stack.pop(), stack.pop()
	off, memCost, err := checkedMemoryExpansionCostFixed(memory, &offset, 1, MSTORE8)
	if err != nil {
		return nil, err
	}
	// See opMload for the proposal-#65 base-tier note.
	var cost uint64
	if interpreter.tvmConfig.HigherLimitForMaxCpuTimeOfOneTx {
		cost = EnergySpecial
	}
	cost += memCost
	if !interpreter.useEnergy(contract, cost) {
		return nil, ErrOutOfEnergy
	}
	memory.resize(off + 1)
	memory.store[off] = byte(val.Uint64())
	return nil, nil
}

func opMCopy(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	dst, src, length := stack.pop(), stack.pop(), stack.pop()
	if length.IsZero() {
		if !interpreter.useEnergy(contract, EnergyVeryLow) {
			return nil, ErrOutOfEnergy
		}
		return nil, nil
	}
	if !dst.IsUint64() || !src.IsUint64() || !length.IsUint64() {
		return nil, newOutOfMemoryError(MCOPY)
	}
	size := length.Uint64()
	dstOff := dst.Uint64()
	srcOff := src.Uint64()
	maxOff := dstOff
	if srcOff > maxOff {
		maxOff = srcOff
	}
	memCost, err := checkedMemoryExpansionCost(memory, maxOff, size, MCOPY)
	if err != nil {
		return nil, err
	}

	cost := EnergyVeryLow + EnergyCopy*toWordSize(size)
	cost += memCost
	if !interpreter.useEnergy(contract, cost) {
		return nil, ErrOutOfEnergy
	}
	end := maxOff + size
	memory.resize(end)
	copy(memory.store[dstOff:dstOff+size], memory.store[srcOff:srcOff+size])
	return nil, nil
}

func opSload(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	key := stack.peek()
	var k tcommon.Hash
	b := key.Bytes32()
	copy(k[:], b[:])
	val := interpreter.tvm.StateDB.GetState(contract.Address, k)
	key.SetBytes(val.Bytes())
	return nil, nil
}

func opSstore(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	key, val := stack.pop(), stack.pop()
	var k, v tcommon.Hash
	kb := key.Bytes32()
	vb := val.Bytes32()
	copy(k[:], kb[:])
	copy(v[:], vb[:])

	_, exists := interpreter.tvm.StateDB.GetStateWithExist(contract.Address, k)
	var cost uint64
	if !exists && v != (tcommon.Hash{}) {
		cost = EnergySstoreSet
	} else {
		cost = EnergySstoreReset
	}
	if !interpreter.useEnergy(contract, cost) {
		return nil, ErrOutOfEnergy
	}

	interpreter.tvm.StateDB.SetState(contract.Address, k, v)
	return nil, nil
}

func opJump(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	pos := stack.pop()
	dest := pos.Uint64()
	if !contract.IsValidJumpdest(dest) {
		return nil, newInvalidJumpError(dest)
	}
	*pc = dest - 1
	return nil, nil
}

func opJumpi(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	pos, cond := stack.pop(), stack.pop()
	if !cond.IsZero() {
		dest := pos.Uint64()
		if !contract.IsValidJumpdest(dest) {
			return nil, newInvalidJumpError(dest)
		}
		*pc = dest - 1
	}
	return nil, nil
}

func opPc(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	stack.push(uint256.NewInt(*pc))
	return nil, nil
}

func opMsize(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	stack.push(uint256.NewInt(uint64(memory.len())))
	return nil, nil
}

func opGas(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	stack.push(uint256.NewInt(contract.Energy))
	return nil, nil
}

func opJumpdest(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	return nil, nil
}

func opPush0(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	stack.push(uint256.NewInt(0))
	return nil, nil
}

// --- Logging ---

func makeLog(topicCount int) executionFunc {
	return func(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
		offset, size := stack.pop(), stack.pop()

		// java getLogCost charges the data-energy (size*8) against remaining
		// energy and throws OUT_OF_ENERGY BEFORE the 3MB memory-overflow check.
		// Mirror that order so a huge size operand yields OUT_OF_ENERGY (contractRet
		// 10), not OUT_OF_MEMORY (4). Operate in uint256 so size*8 cannot wrap.
		if !size.IsUint64() {
			return nil, ErrOutOfEnergy
		}
		var dataCost uint256.Int
		dataCost.Mul(&size, uint256.NewInt(EnergyLogData))
		if !dataCost.IsUint64() || dataCost.Uint64() > contract.Energy {
			return nil, ErrOutOfEnergy
		}

		off, sz, memCost, err := checkedMemoryExpansionCostWords(memory, &offset, &size, LOG0+OpCode(topicCount))
		if err != nil {
			return nil, err
		}

		cost := EnergyLog + EnergyLogTopic*uint64(topicCount) + EnergyLogData*sz
		cost += memCost
		if !interpreter.useEnergy(contract, cost) {
			return nil, ErrOutOfEnergy
		}
		resizeMemory(memory, off, sz)

		topics := make([][]byte, topicCount)
		for i := 0; i < topicCount; i++ {
			t := stack.pop()
			b := t.Bytes32()
			topics[i] = make([]byte, 32)
			copy(topics[i], b[:])
		}

		data := memory.getCopy(int64(off), int64(sz))

		interpreter.tvm.Logs = append(interpreter.tvm.Logs, Log{
			Address: contract.Address,
			Topics:  topics,
			Data:    data,
		})

		return nil, nil
	}
}

// --- System ---

func opStop(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	return nil, nil
}

func opReturn(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	offset, size := stack.pop(), stack.pop()
	off, sz, memCost, err := checkedMemoryExpansionCostWords(memory, &offset, &size, RETURN)
	if err != nil {
		return nil, err
	}
	if memCost > 0 {
		if !interpreter.useEnergy(contract, memCost) {
			return nil, ErrOutOfEnergy
		}
	}
	resizeMemory(memory, off, sz)
	return memory.getCopy(int64(off), int64(sz)), nil
}

func opRevert(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	offset, size := stack.pop(), stack.pop()
	off, sz, memCost, err := checkedMemoryExpansionCostWords(memory, &offset, &size, REVERT)
	if err != nil {
		return nil, err
	}
	if memCost > 0 {
		if !interpreter.useEnergy(contract, memCost) {
			return nil, ErrOutOfEnergy
		}
	}
	resizeMemory(memory, off, sz)
	return memory.getCopy(int64(off), int64(sz)), nil
}

func opSelfDestruct(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	beneficiary := stack.pop()
	address := uint256ToAddress(&beneficiary)

	var cost uint64
	if interpreter.tvmConfig.SelfdestructRestrict {
		cost = EnergySelfDestruct
	}
	if (interpreter.tvmConfig.EnergyAdjustment || interpreter.tvmConfig.SelfdestructRestrict) && !interpreter.tvm.StateDB.Exist(address) {
		cost += EnergyCallNewAcct
	}
	if !interpreter.useEnergy(contract, cost) {
		return nil, ErrOutOfEnergy
	}

	interpreter.tvm.Nonce++
	balance := interpreter.tvm.StateDB.GetBalance(contract.Address)
	var tokenInfo map[string]int64
	if account := interpreter.tvm.StateDB.GetAccount(contract.Address); account != nil {
		if assets := account.Proto().GetAssetV2(); len(assets) > 0 {
			tokenInfo = make(map[string]int64, len(assets))
			for tokenID, amount := range assets {
				if amount != 0 {
					tokenInfo[tokenID] = amount
				}
			}
		}
	}
	suicideIT := interpreter.tvm.addInternalTransactionWithTokenInfo(contract.Address, address, balance, nil, "suicide", tokenInfo)
	oldSuicide := !interpreter.tvmConfig.SelfdestructRestrict || interpreter.tvm.isNewContract(contract.Address)
	sameOldSuicideAddress := sameSelfDestructAddress(contract.Address, address, interpreter.tvmConfig.EnergyAdjustment)
	if oldSuicide && sameOldSuicideAddress {
		blackhole := interpreter.tvm.blackholeAddress()
		if balance > 0 {
			if err := interpreter.tvm.StateDB.SubBalance(contract.Address, balance); err != nil {
				return nil, err
			}
			if interpreter.tvmConfig.TransferTrc10 {
				interpreter.tvm.StateDB.AddBalance(blackhole, balance)
			}
		}
		if interpreter.tvmConfig.TransferTrc10 {
			interpreter.tvm.StateDB.TransferAllTRC10Balance(contract.Address, blackhole)
		}
	} else if address != contract.Address {
		if balance > 0 {
			// java-tron Program.suicide (Program.java:483) and suicide2 (555)
			// call createAccountIfNotExist before transferring to a non-existent
			// obtainer; the call is gated by allowTvmSolidity059 inside the
			// helper. Mirror that here so SUICIDE-with-balance auto-create
			// stamps create_time and (when AllowMultiSign is on) default
			// permissions, matching RepositoryImpl.createNormalAccount.
			interpreter.tvm.maybeCreateNormalAccountForValueTransfer(address)
			interpreter.tvm.StateDB.AddBalance(address, balance)
			if err := interpreter.tvm.StateDB.SubBalance(contract.Address, balance); err != nil {
				return nil, err
			}
		}
		if interpreter.tvmConfig.TransferTrc10 {
			interpreter.tvm.StateDB.TransferAllTRC10Balance(contract.Address, address)
		}
	}
	if interpreter.tvmConfig.Freeze {
		// java-tron Program.suicide (Program.java:497-504) / suicide2 (570-572):
		// when allow_tvm_freeze is active, release the destroyed contract's V1
		// frozen bandwidth/energy weight and credit the frozen balance to the
		// inheritor. suicide2 returns before this when owner == obtainer, so the
		// new-suicide path only runs for distinct addresses; old suicide routes a
		// self-obtainer to the blackhole. The owner == obtainer test uses java's
		// full-address isEqual (line 499), independent of the energy-adjustment
		// compare used for the balance transfer above.
		if oldSuicide {
			inheritor := address
			if address == contract.Address {
				inheritor = interpreter.tvm.blackholeAddress()
			}
			interpreter.tvm.transferDelegatedResourceToInheritor(contract.Address, inheritor)
		} else if address != contract.Address {
			interpreter.tvm.transferDelegatedResourceToInheritor(contract.Address, address)
		}
	}
	if interpreter.tvmConfig.StakingV2 {
		// java-tron Program.suicide (Program.java:505-514) / suicide2 (574-581):
		// when allow_tvm_freeze_v2 (Stake 2.0) is active, move the destroyed
		// contract's FrozenV2 balances + usage to the inheritor and bump the
		// suicide internal-tx value by any withdrawn expired-unfreeze balance. The
		// inheritor selection matches the V1 block above (blackhole for a
		// self-obtainer on old suicide; the obtainer otherwise). suicide2 returns
		// before this when owner == obtainer, so the distinct-address guard holds.
		var expireUnfrozenBalance int64
		if oldSuicide {
			inheritor := address
			if address == contract.Address {
				inheritor = interpreter.tvm.blackholeAddress()
			}
			expireUnfrozenBalance = interpreter.tvm.transferFrozenV2BalanceToInheritor(contract.Address, inheritor)
		} else if address != contract.Address {
			expireUnfrozenBalance = interpreter.tvm.transferFrozenV2BalanceToInheritor(contract.Address, address)
		}
		if expireUnfrozenBalance > 0 && suicideIT != nil && len(suicideIT.CallValueInfo) > 0 {
			suicideIT.CallValueInfo[0].CallValue += expireUnfrozenBalance
		}
	}
	if oldSuicide {
		interpreter.tvm.StateDB.SelfDestruct(contract.Address)
	}
	return nil, nil
}

// --- Helpers ---

func sameSelfDestructAddress(owner, beneficiary tcommon.Address, energyAdjustment bool) bool {
	if energyAdjustment {
		return owner == beneficiary
	}
	for i := 0; i < 20; i++ {
		if owner[i] != beneficiary[i] {
			return false
		}
	}
	return true
}

func uint256ToAddress(v *uint256.Int) tcommon.Address {
	b := v.Bytes32()
	var addr tcommon.Address
	copy(addr[1:], b[32-20:])
	addr[0] = 0x41
	return addr
}

func uint256ToInt64Exact(v *uint256.Int) (int64, bool) {
	const maxInt64 = uint64(1<<63 - 1)
	if v.CmpUint64(maxInt64) > 0 {
		return 0, false
	}
	return int64(v.Uint64()), true
}

func uint256ToJavaLongExact(v *uint256.Int) (int64, bool) {
	b := v.Bytes32()
	if b[0]&0x80 == 0 {
		return uint256ToInt64Exact(v)
	}
	for i := 0; i < 24; i++ {
		if b[i] != 0xff {
			return 0, false
		}
	}
	if b[24] < 0x80 {
		return 0, false
	}
	return int64(v.Uint64()), true
}

func addressToUint256(addr tcommon.Address) uint256.Int {
	var v uint256.Int
	v.SetBytes(addr[1:])
	return v
}

func addressToUint256WithPrefix(addr tcommon.Address) uint256.Int {
	var v uint256.Int
	v.SetBytes(addr[:])
	return v
}

func addressToUint256WithMode(addr tcommon.Address, multiSign bool) uint256.Int {
	if multiSign {
		return addressToUint256(addr)
	}
	return addressToUint256WithPrefix(addr)
}

func getDataSlice(data []byte, offset, size uint64) []byte {
	if size == 0 {
		return nil
	}
	result := make([]byte, size)
	if offset < uint64(len(data)) {
		copy(result, data[offset:])
	}
	return result
}

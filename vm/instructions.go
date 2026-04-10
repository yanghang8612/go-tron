package vm

import (
	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
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
	if !contract.UseEnergy(cost) {
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

// --- SHA3 ---

func opSHA3(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	offset, size := stack.pop(), stack.peek()
	sz := size.Uint64()
	off := offset.Uint64()
	if cost := memoryExpansionCost(memory, off, sz); cost > 0 {
		if !contract.UseEnergy(cost) {
			return nil, ErrOutOfEnergy
		}
	}
	memory.resize(off + sz)
	words := toWordSize(sz)
	cost := EnergySHA3 + EnergySHA3Word*words
	if !contract.UseEnergy(cost) {
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
	var v uint256.Int
	v.SetBytes(contract.Address[:])
	stack.push(&v)
	return nil, nil
}

func opBalance(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	addr := stack.peek()
	address := uint256ToAddress(addr)
	balance := interpreter.evm.StateDB.GetBalance(address)
	addr.SetUint64(uint64(balance))
	return nil, nil
}

func opOrigin(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	var v uint256.Int
	v.SetBytes(interpreter.evm.Origin[:])
	stack.push(&v)
	return nil, nil
}

func opCaller(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	var v uint256.Int
	v.SetBytes(contract.Caller[:])
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
	size := length.Uint64()
	words := toWordSize(size)
	cost := EnergyVeryLow + EnergyCopy*words
	if mcost := memoryExpansionCost(memory, memOffset.Uint64(), size); mcost > 0 {
		cost += mcost
	}
	if !contract.UseEnergy(cost) {
		return nil, ErrOutOfEnergy
	}
	memory.resize(memOffset.Uint64() + size)
	data := getDataSlice(contract.Input, dataOffset.Uint64(), size)
	memory.set(memOffset.Uint64(), size, data)
	return nil, nil
}

func opCodeSize(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	v := uint256.NewInt(uint64(len(contract.Code)))
	stack.push(v)
	return nil, nil
}

func opCodeCopy(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	memOffset, codeOffset, length := stack.pop(), stack.pop(), stack.pop()
	size := length.Uint64()
	words := toWordSize(size)
	cost := EnergyVeryLow + EnergyCopy*words
	if mcost := memoryExpansionCost(memory, memOffset.Uint64(), size); mcost > 0 {
		cost += mcost
	}
	if !contract.UseEnergy(cost) {
		return nil, ErrOutOfEnergy
	}
	memory.resize(memOffset.Uint64() + size)
	data := getDataSlice(contract.Code, codeOffset.Uint64(), size)
	memory.set(memOffset.Uint64(), size, data)
	return nil, nil
}

func opExtCodeSize(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	addr := stack.peek()
	address := uint256ToAddress(addr)
	size := interpreter.evm.StateDB.GetCodeSize(address)
	addr.SetUint64(uint64(size))
	return nil, nil
}

func opExtCodeCopy(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	a, memOffset, codeOffset, length := stack.pop(), stack.pop(), stack.pop(), stack.pop()
	address := uint256ToAddress(&a)
	size := length.Uint64()
	words := toWordSize(size)
	cost := EnergyCopy * words
	if mcost := memoryExpansionCost(memory, memOffset.Uint64(), size); mcost > 0 {
		cost += mcost
	}
	if !contract.UseEnergy(cost) {
		return nil, ErrOutOfEnergy
	}
	memory.resize(memOffset.Uint64() + size)
	code := interpreter.evm.StateDB.GetCode(address)
	data := getDataSlice(code, codeOffset.Uint64(), size)
	memory.set(memOffset.Uint64(), size, data)
	return nil, nil
}

func opReturnDataSize(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	v := uint256.NewInt(uint64(len(interpreter.returnData)))
	stack.push(v)
	return nil, nil
}

func opReturnDataCopy(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	memOffset, dataOffset, length := stack.pop(), stack.pop(), stack.pop()
	size := length.Uint64()
	end := dataOffset.Uint64() + size
	if end > uint64(len(interpreter.returnData)) {
		return nil, ErrReturnDataOutOfBounds
	}
	words := toWordSize(size)
	cost := EnergyVeryLow + EnergyCopy*words
	if mcost := memoryExpansionCost(memory, memOffset.Uint64(), size); mcost > 0 {
		cost += mcost
	}
	if !contract.UseEnergy(cost) {
		return nil, ErrOutOfEnergy
	}
	memory.resize(memOffset.Uint64() + size)
	memory.set(memOffset.Uint64(), size, interpreter.returnData[dataOffset.Uint64():end])
	return nil, nil
}

func opExtCodeHash(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	addr := stack.peek()
	address := uint256ToAddress(addr)
	if !interpreter.evm.StateDB.Exist(address) {
		addr.Clear()
	} else {
		hash := interpreter.evm.StateDB.GetCodeHash(address)
		addr.SetBytes(hash.Bytes())
	}
	return nil, nil
}

func opGasPrice(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	stack.push(uint256.NewInt(0))
	return nil, nil
}

// --- Block Information ---

func opBlockHash(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	num := stack.peek()
	num.Clear() // simplified — no block hash lookup in StateDB
	return nil, nil
}

func opCoinbase(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	var v uint256.Int
	v.SetBytes(interpreter.evm.Coinbase[:])
	stack.push(&v)
	return nil, nil
}

func opTimestamp(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	v := uint256.NewInt(uint64(interpreter.evm.Timestamp))
	stack.push(v)
	return nil, nil
}

func opNumber(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	v := uint256.NewInt(interpreter.evm.BlockNumber)
	stack.push(v)
	return nil, nil
}

func opDifficulty(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	stack.push(uint256.NewInt(0))
	return nil, nil
}

func opGasLimit(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	stack.push(uint256.NewInt(uint64(interpreter.evm.StateDB.DynamicProperties().TotalEnergyCurrentLimit())))
	return nil, nil
}

func opChainID(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	v := uint256.NewInt(uint64(interpreter.evm.ChainID))
	stack.push(v)
	return nil, nil
}

func opSelfBalance(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	balance := interpreter.evm.StateDB.GetBalance(contract.Address)
	stack.push(uint256.NewInt(uint64(balance)))
	return nil, nil
}

func opBaseFee(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
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
	off := offset.Uint64()
	cost := EnergyVeryLow
	if mcost := memoryExpansionCost(memory, off, 32); mcost > 0 {
		cost += mcost
	}
	if !contract.UseEnergy(cost) {
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
	off := offset.Uint64()
	cost := EnergyVeryLow
	if mcost := memoryExpansionCost(memory, off, 32); mcost > 0 {
		cost += mcost
	}
	if !contract.UseEnergy(cost) {
		return nil, ErrOutOfEnergy
	}
	memory.resize(off + 32)
	memory.set32(off, &val)
	return nil, nil
}

func opMstore8(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	offset, val := stack.pop(), stack.pop()
	off := offset.Uint64()
	cost := EnergyVeryLow
	if mcost := memoryExpansionCost(memory, off, 1); mcost > 0 {
		cost += mcost
	}
	if !contract.UseEnergy(cost) {
		return nil, ErrOutOfEnergy
	}
	memory.resize(off + 1)
	memory.store[off] = byte(val.Uint64())
	return nil, nil
}

func opSload(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	key := stack.peek()
	var k tcommon.Hash
	b := key.Bytes32()
	copy(k[:], b[:])
	val := interpreter.evm.StateDB.GetState(contract.Address, k)
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

	current := interpreter.evm.StateDB.GetState(contract.Address, k)
	var cost uint64
	if current == (tcommon.Hash{}) && v != (tcommon.Hash{}) {
		cost = EnergySstoreSet
	} else {
		cost = EnergySstoreReset
	}
	if !contract.UseEnergy(cost) {
		return nil, ErrOutOfEnergy
	}

	interpreter.evm.StateDB.SetState(contract.Address, k, v)
	return nil, nil
}

func opJump(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	pos := stack.pop()
	dest := pos.Uint64()
	if !contract.IsValidJumpdest(dest) {
		return nil, ErrInvalidJump
	}
	*pc = dest - 1
	return nil, nil
}

func opJumpi(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	pos, cond := stack.pop(), stack.pop()
	if !cond.IsZero() {
		dest := pos.Uint64()
		if !contract.IsValidJumpdest(dest) {
			return nil, ErrInvalidJump
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
		sz := size.Uint64()

		cost := EnergyLog + EnergyLogTopic*uint64(topicCount) + EnergyLogData*sz
		if mcost := memoryExpansionCost(memory, offset.Uint64(), sz); mcost > 0 {
			cost += mcost
		}
		if !contract.UseEnergy(cost) {
			return nil, ErrOutOfEnergy
		}
		memory.resize(offset.Uint64() + sz)

		topics := make([][]byte, topicCount)
		for i := 0; i < topicCount; i++ {
			t := stack.pop()
			b := t.Bytes32()
			topics[i] = make([]byte, 32)
			copy(topics[i], b[:])
		}

		data := memory.getCopy(int64(offset.Uint64()), int64(sz))

		interpreter.evm.Logs = append(interpreter.evm.Logs, Log{
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
	sz := size.Uint64()
	if mcost := memoryExpansionCost(memory, offset.Uint64(), sz); mcost > 0 {
		if !contract.UseEnergy(mcost) {
			return nil, ErrOutOfEnergy
		}
	}
	memory.resize(offset.Uint64() + sz)
	return memory.getCopy(int64(offset.Uint64()), int64(sz)), nil
}

func opRevert(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	offset, size := stack.pop(), stack.pop()
	sz := size.Uint64()
	if mcost := memoryExpansionCost(memory, offset.Uint64(), sz); mcost > 0 {
		if !contract.UseEnergy(mcost) {
			return nil, ErrOutOfEnergy
		}
	}
	memory.resize(offset.Uint64() + sz)
	return memory.getCopy(int64(offset.Uint64()), int64(sz)), nil
}

func opSelfDestruct(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	beneficiary := stack.pop()
	address := uint256ToAddress(&beneficiary)

	cost := uint64(EnergySelfDestruct)
	if !interpreter.evm.StateDB.Exist(address) {
		cost += EnergyCallNewAcct
	}
	if !contract.UseEnergy(cost) {
		return nil, ErrOutOfEnergy
	}

	balance := interpreter.evm.StateDB.GetBalance(contract.Address)
	if balance > 0 {
		interpreter.evm.StateDB.AddBalance(address, balance)
		interpreter.evm.StateDB.SubBalance(contract.Address, balance)
	}
	interpreter.evm.StateDB.SelfDestruct(contract.Address)
	return nil, nil
}

// --- Helpers ---

func uint256ToAddress(v *uint256.Int) tcommon.Address {
	b := v.Bytes32()
	var addr tcommon.Address
	copy(addr[1:], b[32-20:])
	addr[0] = 0x41
	return addr
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

package vm

import (
	"github.com/holiman/uint256"
)

// opCreate deploys a new contract.
// Stack: [value, offset, size] → [addr]
func opCreate(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	value, offset, size := stack.pop(), stack.pop(), stack.pop()
	sz := size.Uint64()

	if mcost := memoryExpansionCost(memory, offset.Uint64(), sz); mcost > 0 {
		if !contract.UseEnergy(mcost) {
			return nil, ErrOutOfEnergy
		}
	}
	memory.resize(offset.Uint64() + sz)

	code := memory.getCopy(int64(offset.Uint64()), int64(sz))
	val := int64(value.Uint64())

	energyForCall := contract.Energy - contract.Energy/64
	contract.UseEnergy(energyForCall)

	ret, addr, remainingEnergy, err := interpreter.tvm.Create(
		contract.Address, code, energyForCall, val,
	)
	contract.Energy += remainingEnergy

	var result uint256.Int
	if err != nil {
		result.Clear()
	} else {
		result.SetBytes(addr[:])
	}
	stack.push(&result)
	interpreter.returnData = ret
	return nil, nil
}

// opCreate2 deploys a new contract with a deterministic address.
// Stack: [value, offset, size, salt] → [addr]
func opCreate2(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	value, offset, size, saltVal := stack.pop(), stack.pop(), stack.pop(), stack.pop()
	sz := size.Uint64()

	if mcost := memoryExpansionCost(memory, offset.Uint64(), sz); mcost > 0 {
		if !contract.UseEnergy(mcost) {
			return nil, ErrOutOfEnergy
		}
	}
	memory.resize(offset.Uint64() + sz)

	words := toWordSize(sz)
	hashCost := EnergySHA3Word * words
	if !contract.UseEnergy(hashCost) {
		return nil, ErrOutOfEnergy
	}

	code := memory.getCopy(int64(offset.Uint64()), int64(sz))
	val := int64(value.Uint64())
	salt := saltVal.Bytes32()

	energyForCall := contract.Energy - contract.Energy/64
	contract.UseEnergy(energyForCall)

	ret, addr, remainingEnergy, err := interpreter.tvm.Create2(
		contract.Address, code, energyForCall, val, salt,
	)
	contract.Energy += remainingEnergy

	var result uint256.Int
	if err != nil {
		result.Clear()
	} else {
		result.SetBytes(addr[:])
	}
	stack.push(&result)
	interpreter.returnData = ret
	return nil, nil
}

// opCall executes a contract call.
// Stack: [energy, addr, value, inOffset, inSize, outOffset, outSize] → [success]
func opCall(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	energyVal, addrVal, value, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()

	addr := uint256ToAddress(&addrVal)
	val := int64(value.Uint64())
	gas := energyVal.Uint64()

	cost := uint64(EnergyCall)
	if val > 0 {
		cost += EnergyCallValueTx
		if !interpreter.tvm.StateDB.Exist(addr) {
			cost += EnergyCallNewAcct
		}
	}
	if !contract.UseEnergy(cost) {
		return nil, ErrOutOfEnergy
	}

	inSz := inSize.Uint64()
	retSz := retSize.Uint64()
	if mcost := memoryExpansionCost(memory, inOffset.Uint64(), inSz); mcost > 0 {
		if !contract.UseEnergy(mcost) {
			return nil, ErrOutOfEnergy
		}
	}
	if mcost := memoryExpansionCost(memory, retOffset.Uint64(), retSz); mcost > 0 {
		if !contract.UseEnergy(mcost) {
			return nil, ErrOutOfEnergy
		}
	}
	memory.resize(inOffset.Uint64() + inSz)
	memory.resize(retOffset.Uint64() + retSz)

	available := contract.Energy - contract.Energy/64
	if gas > available {
		gas = available
	}
	contract.UseEnergy(gas)

	if val > 0 {
		gas += EnergyCallStipend
	}

	input := memory.getCopy(int64(inOffset.Uint64()), int64(inSz))
	ret, remainingEnergy, err := interpreter.tvm.Call(contract.Address, addr, input, gas, val)
	contract.Energy += remainingEnergy

	var success uint256.Int
	if err == nil {
		success.SetOne()
	}
	stack.push(&success)

	if retSz > 0 && len(ret) > 0 {
		copyLen := retSz
		if uint64(len(ret)) < copyLen {
			copyLen = uint64(len(ret))
		}
		memory.set(retOffset.Uint64(), copyLen, ret[:copyLen])
	}
	interpreter.returnData = ret
	return nil, nil
}

// opCallCode: like CALL but executes code in caller's storage context.
func opCallCode(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	energyVal, addrVal, _, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()

	addr := uint256ToAddress(&addrVal)
	gas := energyVal.Uint64()

	if !contract.UseEnergy(EnergyCall) {
		return nil, ErrOutOfEnergy
	}

	inSz := inSize.Uint64()
	retSz := retSize.Uint64()
	if mcost := memoryExpansionCost(memory, inOffset.Uint64(), inSz); mcost > 0 {
		if !contract.UseEnergy(mcost) {
			return nil, ErrOutOfEnergy
		}
	}
	if mcost := memoryExpansionCost(memory, retOffset.Uint64(), retSz); mcost > 0 {
		if !contract.UseEnergy(mcost) {
			return nil, ErrOutOfEnergy
		}
	}
	memory.resize(inOffset.Uint64() + inSz)
	memory.resize(retOffset.Uint64() + retSz)

	available := contract.Energy - contract.Energy/64
	if gas > available {
		gas = available
	}
	contract.UseEnergy(gas)

	input := memory.getCopy(int64(inOffset.Uint64()), int64(inSz))
	ret, remainingEnergy, err := interpreter.tvm.DelegateCall(contract.Address, addr, input, gas)
	contract.Energy += remainingEnergy

	var success uint256.Int
	if err == nil {
		success.SetOne()
	}
	stack.push(&success)
	if retSz > 0 && len(ret) > 0 {
		copyLen := retSz
		if uint64(len(ret)) < copyLen {
			copyLen = uint64(len(ret))
		}
		memory.set(retOffset.Uint64(), copyLen, ret[:copyLen])
	}
	interpreter.returnData = ret
	return nil, nil
}

// opDelegateCall: DELEGATECALL.
// Stack: [energy, addr, inOffset, inSize, outOffset, outSize] → [success]
func opDelegateCall(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	energyVal, addrVal, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()

	addr := uint256ToAddress(&addrVal)
	gas := energyVal.Uint64()

	if !contract.UseEnergy(EnergyCall) {
		return nil, ErrOutOfEnergy
	}

	inSz := inSize.Uint64()
	retSz := retSize.Uint64()
	if mcost := memoryExpansionCost(memory, inOffset.Uint64(), inSz); mcost > 0 {
		if !contract.UseEnergy(mcost) {
			return nil, ErrOutOfEnergy
		}
	}
	if mcost := memoryExpansionCost(memory, retOffset.Uint64(), retSz); mcost > 0 {
		if !contract.UseEnergy(mcost) {
			return nil, ErrOutOfEnergy
		}
	}
	memory.resize(inOffset.Uint64() + inSz)
	memory.resize(retOffset.Uint64() + retSz)

	available := contract.Energy - contract.Energy/64
	if gas > available {
		gas = available
	}
	contract.UseEnergy(gas)

	input := memory.getCopy(int64(inOffset.Uint64()), int64(inSz))
	ret, remainingEnergy, err := interpreter.tvm.DelegateCall(contract.Caller, addr, input, gas)
	contract.Energy += remainingEnergy

	var success uint256.Int
	if err == nil {
		success.SetOne()
	}
	stack.push(&success)
	if retSz > 0 && len(ret) > 0 {
		copyLen := retSz
		if uint64(len(ret)) < copyLen {
			copyLen = uint64(len(ret))
		}
		memory.set(retOffset.Uint64(), copyLen, ret[:copyLen])
	}
	interpreter.returnData = ret
	return nil, nil
}

// opStaticCall: STATICCALL (read-only call).
// Stack: [energy, addr, inOffset, inSize, outOffset, outSize] → [success]
func opStaticCall(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	energyVal, addrVal, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()

	addr := uint256ToAddress(&addrVal)
	gas := energyVal.Uint64()

	if !contract.UseEnergy(EnergyCall) {
		return nil, ErrOutOfEnergy
	}

	inSz := inSize.Uint64()
	retSz := retSize.Uint64()
	if mcost := memoryExpansionCost(memory, inOffset.Uint64(), inSz); mcost > 0 {
		if !contract.UseEnergy(mcost) {
			return nil, ErrOutOfEnergy
		}
	}
	if mcost := memoryExpansionCost(memory, retOffset.Uint64(), retSz); mcost > 0 {
		if !contract.UseEnergy(mcost) {
			return nil, ErrOutOfEnergy
		}
	}
	memory.resize(inOffset.Uint64() + inSz)
	memory.resize(retOffset.Uint64() + retSz)

	available := contract.Energy - contract.Energy/64
	if gas > available {
		gas = available
	}
	contract.UseEnergy(gas)

	input := memory.getCopy(int64(inOffset.Uint64()), int64(inSz))
	ret, remainingEnergy, err := interpreter.tvm.StaticCall(contract.Address, addr, input, gas)
	contract.Energy += remainingEnergy

	var success uint256.Int
	if err == nil {
		success.SetOne()
	}
	stack.push(&success)
	if retSz > 0 && len(ret) > 0 {
		copyLen := retSz
		if uint64(len(ret)) < copyLen {
			copyLen = uint64(len(ret))
		}
		memory.set(retOffset.Uint64(), copyLen, ret[:copyLen])
	}
	interpreter.returnData = ret
	return nil, nil
}

package vm

import (
	"github.com/holiman/uint256"
)

// opCreate deploys a new contract.
// Stack: [value, offset, size] → [addr]
func opCreate(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	value, offset, size := stack.pop(), stack.pop(), stack.pop()
	off, sz, memCost, err := checkedMemoryExpansionCostWords(memory, &offset, &size, CREATE)
	if err != nil {
		return nil, err
	}

	if memCost > 0 {
		if !interpreter.useEnergy(contract, memCost) {
			return nil, ErrOutOfEnergy
		}
	}
	resizeMemory(memory, off, sz)

	code := memory.getCopy(int64(off), int64(sz))
	val, valueOK := uint256ToInt64Exact(&value)
	if !valueOK {
		return nil, ErrEndowmentOutOfRange
	}

	energyForCall := interpreter.tvm.adjustedCreateEnergy(contract)
	contract.UseEnergy(energyForCall)

	ret, addr, remainingEnergy, err := interpreter.tvm.createWithVersion(
		contract.Address, code, energyForCall, val, contract.Version,
	)
	contract.Energy += remainingEnergy

	var result uint256.Int
	if err != nil {
		result.Clear()
	} else {
		result = addressToUint256(addr)
	}
	stack.push(&result)
	interpreter.returnData = ret
	return nil, nil
}

// opCreate2 deploys a new contract with a deterministic address.
// Stack: [value, offset, size, salt] → [addr]
func opCreate2(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	value, offset, size, saltVal := stack.pop(), stack.pop(), stack.pop(), stack.pop()
	off, sz, memCost, err := checkedMemoryExpansionCostWords(memory, &offset, &size, CREATE2)
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
	hashCost := EnergySHA3Word * words
	if !interpreter.useEnergy(contract, hashCost) {
		return nil, ErrOutOfEnergy
	}

	code := memory.getCopy(int64(off), int64(sz))
	val, valueOK := uint256ToInt64Exact(&value)
	if !valueOK {
		return nil, ErrEndowmentOutOfRange
	}
	salt := saltVal.Bytes32()

	energyForCall := interpreter.tvm.adjustedCreateEnergy(contract)
	contract.UseEnergy(energyForCall)

	addressSeed := contract.Address
	if !interpreter.tvm.cfg.Istanbul {
		addressSeed = contract.Caller
	}
	ret, addr, remainingEnergy, err := interpreter.tvm.create2WithVersion(
		contract.Address, addressSeed, code, energyForCall, val, salt, contract.Version,
	)
	contract.Energy += remainingEnergy

	var result uint256.Int
	if err != nil {
		result.Clear()
	} else {
		result = addressToUint256(addr)
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
	valueNonZero := !value.IsZero()
	val, valueOK := uint256ToInt64Exact(&value)
	gas := energyVal.Uint64()

	if interpreter.readOnly && valueNonZero {
		return nil, ErrWriteProtection
	}

	cost := uint64(EnergyCall)
	if valueNonZero {
		cost += EnergyCallValueTx
		if !interpreter.tvm.StateDB.Exist(addr) {
			cost += EnergyCallNewAcct
		}
	}
	if !interpreter.useEnergy(contract, cost) {
		return nil, ErrOutOfEnergy
	}

	inOff, inSz, inMemCost, err := checkedMemoryExpansionCostWords(memory, &inOffset, &inSize, CALL)
	if err != nil {
		return nil, err
	}
	retOff, retSz, retMemCost, err := checkedMemoryExpansionCostWords(memory, &retOffset, &retSize, CALL)
	if err != nil {
		return nil, err
	}
	if inMemCost > 0 {
		if !interpreter.useEnergy(contract, inMemCost) {
			return nil, ErrOutOfEnergy
		}
	}
	if retMemCost > 0 {
		if !interpreter.useEnergy(contract, retMemCost) {
			return nil, ErrOutOfEnergy
		}
	}
	resizeMemory(memory, inOff, inSz)
	resizeMemory(memory, retOff, retSz)

	gas = interpreter.tvm.adjustedCallEnergy(contract, gas)
	contract.UseEnergy(gas)

	if valueNonZero {
		gas += EnergyCallStipend
	}
	if !valueOK {
		contract.Energy += gas
		return nil, ErrEndowmentOutOfRange
	}

	input := memory.getCopy(int64(inOff), int64(inSz))
	ret, remainingEnergy, err := interpreter.tvm.Call(contract.Address, addr, input, gas, val)
	contract.Energy += remainingEnergy
	if shouldPropagateCallError(err) {
		return nil, err
	}

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
		memory.set(retOff, copyLen, ret[:copyLen])
	}
	if err == errPrecompileFailure {
		interpreter.returnData = nil
	} else {
		interpreter.returnData = ret
	}
	return nil, nil
}

// opCallCode: like CALL but executes code in caller's storage context.
func opCallCode(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	energyVal, addrVal, value, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()

	addr := uint256ToAddress(&addrVal)
	valueNonZero := !value.IsZero()
	val, valueOK := uint256ToInt64Exact(&value)
	gas := energyVal.Uint64()

	cost := uint64(EnergyCall)
	if valueNonZero {
		cost += EnergyCallValueTx
	}
	if !interpreter.useEnergy(contract, cost) {
		return nil, ErrOutOfEnergy
	}

	inOff, inSz, inMemCost, err := checkedMemoryExpansionCostWords(memory, &inOffset, &inSize, CALLCODE)
	if err != nil {
		return nil, err
	}
	retOff, retSz, retMemCost, err := checkedMemoryExpansionCostWords(memory, &retOffset, &retSize, CALLCODE)
	if err != nil {
		return nil, err
	}
	if inMemCost > 0 {
		if !interpreter.useEnergy(contract, inMemCost) {
			return nil, ErrOutOfEnergy
		}
	}
	if retMemCost > 0 {
		if !interpreter.useEnergy(contract, retMemCost) {
			return nil, ErrOutOfEnergy
		}
	}
	resizeMemory(memory, inOff, inSz)
	resizeMemory(memory, retOff, retSz)

	gas = interpreter.tvm.adjustedCallEnergy(contract, gas)
	contract.UseEnergy(gas)
	if valueNonZero {
		gas += EnergyCallStipend
	}
	if !valueOK {
		contract.Energy += gas
		return nil, ErrEndowmentOutOfRange
	}

	input := memory.getCopy(int64(inOff), int64(inSz))
	ret, remainingEnergy, err := interpreter.tvm.DelegateCall(contract.Address, contract.Address, addr, input, gas, val, val)
	contract.Energy += remainingEnergy
	if shouldPropagateCallError(err) {
		return nil, err
	}

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
		memory.set(retOff, copyLen, ret[:copyLen])
	}
	if err == errPrecompileFailure {
		interpreter.returnData = nil
	} else {
		interpreter.returnData = ret
	}
	return nil, nil
}

// opDelegateCall: DELEGATECALL.
// Stack: [energy, addr, inOffset, inSize, outOffset, outSize] → [success]
func opDelegateCall(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	energyVal, addrVal, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()

	addr := uint256ToAddress(&addrVal)
	gas := energyVal.Uint64()

	if !interpreter.useEnergy(contract, EnergyCall) {
		return nil, ErrOutOfEnergy
	}

	inOff, inSz, inMemCost, err := checkedMemoryExpansionCostWords(memory, &inOffset, &inSize, DELEGATECALL)
	if err != nil {
		return nil, err
	}
	retOff, retSz, retMemCost, err := checkedMemoryExpansionCostWords(memory, &retOffset, &retSize, DELEGATECALL)
	if err != nil {
		return nil, err
	}
	if inMemCost > 0 {
		if !interpreter.useEnergy(contract, inMemCost) {
			return nil, ErrOutOfEnergy
		}
	}
	if retMemCost > 0 {
		if !interpreter.useEnergy(contract, retMemCost) {
			return nil, ErrOutOfEnergy
		}
	}
	resizeMemory(memory, inOff, inSz)
	resizeMemory(memory, retOff, retSz)

	gas = interpreter.tvm.adjustedCallEnergy(contract, gas)
	contract.UseEnergy(gas)

	input := memory.getCopy(int64(inOff), int64(inSz))
	ret, remainingEnergy, err := interpreter.tvm.DelegateCall(contract.Caller, contract.Address, addr, input, gas, contract.Value, 0)
	contract.Energy += remainingEnergy
	if shouldPropagateCallError(err) {
		return nil, err
	}

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
		memory.set(retOff, copyLen, ret[:copyLen])
	}
	if err == errPrecompileFailure {
		interpreter.returnData = nil
	} else {
		interpreter.returnData = ret
	}
	return nil, nil
}

// opStaticCall: STATICCALL (read-only call).
// Stack: [energy, addr, inOffset, inSize, outOffset, outSize] → [success]
func opStaticCall(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	energyVal, addrVal, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()

	addr := uint256ToAddress(&addrVal)
	gas := energyVal.Uint64()

	if !interpreter.useEnergy(contract, EnergyCall) {
		return nil, ErrOutOfEnergy
	}

	inOff, inSz, inMemCost, err := checkedMemoryExpansionCostWords(memory, &inOffset, &inSize, STATICCALL)
	if err != nil {
		return nil, err
	}
	retOff, retSz, retMemCost, err := checkedMemoryExpansionCostWords(memory, &retOffset, &retSize, STATICCALL)
	if err != nil {
		return nil, err
	}
	if inMemCost > 0 {
		if !interpreter.useEnergy(contract, inMemCost) {
			return nil, ErrOutOfEnergy
		}
	}
	if retMemCost > 0 {
		if !interpreter.useEnergy(contract, retMemCost) {
			return nil, ErrOutOfEnergy
		}
	}
	resizeMemory(memory, inOff, inSz)
	resizeMemory(memory, retOff, retSz)

	gas = interpreter.tvm.adjustedCallEnergy(contract, gas)
	contract.UseEnergy(gas)

	input := memory.getCopy(int64(inOff), int64(inSz))
	ret, remainingEnergy, err := interpreter.tvm.StaticCall(contract.Address, addr, input, gas)
	contract.Energy += remainingEnergy
	if shouldPropagateCallError(err) {
		return nil, err
	}

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
		memory.set(retOff, copyLen, ret[:copyLen])
	}
	if err == errPrecompileFailure {
		interpreter.returnData = nil
	} else {
		interpreter.returnData = ret
	}
	return nil, nil
}

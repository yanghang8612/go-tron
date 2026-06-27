package vm

import (
	"github.com/holiman/uint256"
)

// javaCallEnergyRequest mirrors java Program.getCallEnergy's FULL 256-bit compare
// (`requested.compareTo(available) > 0 ? available : requested`): a requested-energy
// word that exceeds uint64 is necessarily greater than the caller's available
// energy, so it must forward the available cap — never its truncated low-64 bits.
// adjustedCallEnergy applies the min against available, so a saturated uint64 here
// clamps to available, matching java. Words that fit uint64 already carry their full
// value (the > available case was already handled by adjustedCallEnergy's min).
func javaCallEnergyRequest(v *uint256.Int) uint64 {
	if v.IsUint64() {
		return v.Uint64()
	}
	return ^uint64(0)
}

// opCreate deploys a new contract.
// Stack: [value, offset, size] → [addr]
// writeCallReturn copies a sub-call's return data into the caller's memory. java
// truncates regular-call returns to the requested out-size always (callToAddress →
// memorySaveLimited), but a SUCCESSFUL precompile's return was written at FULL length
// (extending MSIZE past out-size) until allow_tvm_selfdestruct_restriction switched it
// to truncated (Program.callToPrecompiledAddress). Replicate that fork-gated precompile
// behavior so pre-restriction blocks replay identically; everything else truncates.
func (in *Interpreter) writeCallReturn(memory *Memory, toPrecompile bool, callErr error, retOff, retSz uint64, ret []byte) {
	if len(ret) == 0 {
		return
	}
	if toPrecompile && callErr == nil && !in.tvmConfig.SelfdestructRestrict {
		resizeMemory(memory, retOff, uint64(len(ret)))
		memory.set(retOff, uint64(len(ret)), ret)
		return
	}
	if retSz == 0 {
		return
	}
	copyLen := retSz
	if uint64(len(ret)) < copyLen {
		copyLen = uint64(len(ret))
	}
	memory.set(retOff, copyLen, ret[:copyLen])
}

func opCreate(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	value, offset, size := stack.pop(), stack.pop(), stack.pop()
	off, sz, memCost, err := checkedMemoryExpansionCostWords(memory, &offset, &size, CREATE)
	if err != nil {
		return nil, err
	}

	// java getCreateCost = CREATE + calcMemEnergy(...); calcMemEnergy runs
	// checkMemorySize (the 3 MB OOM guard, above) before this base is spent. So
	// charge the 32000 CREATE base here, after the OOM check, instead of as a
	// static jump-table energyCost that the interpreter loop would spend ahead of
	// the memory check (which would wrongly surface OUT_OF_ENERGY for a >3 MB
	// init-code region when <32000 energy remains).
	if !interpreter.useEnergy(contract, EnergyCreate) {
		return nil, ErrOutOfEnergy
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
	// A fatal VM error (OutOfTime / JVM-stack-overflow) raised inside the
	// constructor must abort the whole tx, not push 0 and continue: java VM.play
	// re-throws these (VM.java outer catch) and createContractImpl's VM.play is not
	// in a try/catch, so they escape to VMActuator -> OUT_OF_TIME + spendAll. Mirror
	// the CALL family's shouldPropagateCallError gate. Non-fatal create failures
	// (revert, OOE, bytecode/transfer exceptions) still push 0 like java's
	// stackPushZero.
	if isFatalVMError(err) {
		return nil, err
	}

	var result uint256.Int
	if err != nil {
		result.Clear()
	} else {
		result = addressToUint256(addr)
	}
	stack.push(&result)
	// CREATE resets the return buffer UNCONDITIONALLY before the call
	// (java-tron Program.createContract:797), so a successful or
	// exceptionally-failed CREATE leaves the buffer empty; only a REVERTing
	// child exposes its output through RETURNDATA* (Program.java:965). The
	// success return value here is the deployed runtime code, which java
	// never exposes. NOTE: CREATE2 differs — its reset is Osaka-gated; see
	// opCreate2.
	if err == ErrExecutionReverted {
		interpreter.returnData = ret
	} else {
		interpreter.returnData = nil
	}
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

	// java getCreate2Cost = CREATE + calcMemEnergy(...) + sizeInWords*SHA3_WORD;
	// the 3 MB OOM guard inside calcMemEnergy precedes spending. Charge the CREATE
	// base here (after the OOM check) rather than via the static jump-table cost —
	// see opCreate.
	if !interpreter.useEnergy(contract, EnergyCreate) {
		return nil, ErrOutOfEnergy
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
	// java-tron Program.createContract2: the compatibleEvm-gated stackPushZero (the
	// Compatibility branch inside create2WithVersion) is the dead mainnet path; the
	// live guard is an UNCONDITIONAL checkCPUTimeForCreate2() at MAX_DEPTH that throws
	// OutOfTimeException once VERSION_4_8_1_1 passed. Without it CREATE2 is the only
	// recursion vector with no effective depth cap on mainnet — unbounded recursion
	// (state fork vs java's OUT_OF_TIME, and potential node stack overflow). Abort the
	// tx with OUT_OF_TIME (ErrAlreadyTimeOut → spend-all) at depth, except on the
	// compatibleEvm path (create2WithVersion's graceful ErrDepthExceeded → push 0).
	if interpreter.tvmConfig.CpuTimeGuard && !interpreter.tvmConfig.Compatibility && interpreter.tvm.Depth > maxCallDepth {
		return nil, ErrAlreadyTimeOut
	}
	ret, addr, remainingEnergy, err := interpreter.tvm.create2WithVersion(
		contract.Address, addressSeed, code, energyForCall, val, salt, contract.Version,
	)
	contract.Energy += remainingEnergy
	// Propagate a fatal constructor error (OutOfTime / JVM-stack-overflow) instead
	// of swallowing it — see opCreate. The depth self-guard above only catches a
	// timeout raised BEFORE entering the constructor; one raised INSIDE it (e.g. a
	// precompile CPU-time abort) surfaces here as err.
	if isFatalVMError(err) {
		return nil, err
	}

	var result uint256.Int
	if err != nil {
		result.Clear()
	} else {
		result = addressToUint256(addr)
	}
	stack.push(&result)
	// CREATE2 resets the return buffer only under Osaka (java-tron
	// Program.createContract2:1619 gates the reset on allowTvmOsaka). On
	// every TRON network before Osaka — including all of Nile history and
	// the current head — a successful or exceptionally-failed CREATE2
	// therefore leaves the CALLER's prior return data INTACT; only a
	// REVERTing child overwrites it (Program.java:965). The per-frame Run()
	// reset already isolated the constructor and the defer restored the
	// caller's pre-CREATE2 value, so we overwrite only on REVERT, or clear
	// to empty once Osaka is active (matching CREATE). This asymmetry with
	// CREATE is a java-tron historical quirk, not an oversight.
	if err == ErrExecutionReverted {
		interpreter.returnData = ret
	} else if interpreter.tvm.cfg.Osaka {
		interpreter.returnData = nil
	}
	return nil, nil
}

// opCall executes a contract call.
// Stack: [energy, addr, value, inOffset, inSize, outOffset, outSize] → [success]
func opCall(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	energyVal, addrVal, value, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()

	addr := uint256ToAddress(&addrVal)
	valueNonZero := !value.IsZero()
	val, valueOK := uint256ToInt64Exact(&value)
	gas := javaCallEnergyRequest(&energyVal)

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

	// java EnergyCost.getCalculateCallCost adds calcMemEnergy(oldMemSize,
	// in.max(out)) to the base cost, and calcMemEnergy runs checkMemorySize
	// (the 3 MB OUT_OF_MEMORY guard) BEFORE the single energyCost >
	// energyLimitLeft OUT_OF_ENERGY comparison. So validate both memory regions
	// (OOM) before charging the base cost, else a >3 MB region wrongly surfaces
	// as OUT_OF_ENERGY when the base cost alone exhausts remaining energy.
	inOff, inSz, _, err := checkedMemoryExpansionCostWords(memory, &inOffset, &inSize, CALL)
	if err != nil {
		return nil, err
	}
	retOff, retSz, _, err := checkedMemoryExpansionCostWords(memory, &retOffset, &retSize, CALL)
	if err != nil {
		return nil, err
	}
	if !interpreter.useEnergy(contract, cost) {
		return nil, ErrOutOfEnergy
	}
	// Single combined expansion to max(inEnd, retEnd) — java EnergyCost
	// calcMemEnergy(oldMemSize, in.max(out)). Charging in and ret separately
	// double-counts the overlapping region.
	if memCost := combinedMemoryExpansionCost(memory, inOff, inSz, retOff, retSz); memCost > 0 {
		if !interpreter.useEnergy(contract, memCost) {
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

	interpreter.writeCallReturn(memory, getPrecompile(addr, interpreter.tvm.cfg) != nil, err, retOff, retSz, ret)
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
	gas := javaCallEnergyRequest(&energyVal)

	cost := uint64(EnergyCall)
	if valueNonZero {
		cost += EnergyCallValueTx
	}

	// OUT_OF_MEMORY (3 MB guard) precedes OUT_OF_ENERGY — see opCall.
	inOff, inSz, _, err := checkedMemoryExpansionCostWords(memory, &inOffset, &inSize, CALLCODE)
	if err != nil {
		return nil, err
	}
	retOff, retSz, _, err := checkedMemoryExpansionCostWords(memory, &retOffset, &retSize, CALLCODE)
	if err != nil {
		return nil, err
	}
	if !interpreter.useEnergy(contract, cost) {
		return nil, ErrOutOfEnergy
	}
	if memCost := combinedMemoryExpansionCost(memory, inOff, inSz, retOff, retSz); memCost > 0 {
		if !interpreter.useEnergy(contract, memCost) {
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
	interpreter.writeCallReturn(memory, getPrecompile(addr, interpreter.tvm.cfg) != nil, err, retOff, retSz, ret)
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
	gas := javaCallEnergyRequest(&energyVal)

	// OUT_OF_MEMORY (3 MB guard) precedes OUT_OF_ENERGY — see opCall.
	inOff, inSz, _, err := checkedMemoryExpansionCostWords(memory, &inOffset, &inSize, DELEGATECALL)
	if err != nil {
		return nil, err
	}
	retOff, retSz, _, err := checkedMemoryExpansionCostWords(memory, &retOffset, &retSize, DELEGATECALL)
	if err != nil {
		return nil, err
	}
	if !interpreter.useEnergy(contract, EnergyCall) {
		return nil, ErrOutOfEnergy
	}
	if memCost := combinedMemoryExpansionCost(memory, inOff, inSz, retOff, retSz); memCost > 0 {
		if !interpreter.useEnergy(contract, memCost) {
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
	interpreter.writeCallReturn(memory, getPrecompile(addr, interpreter.tvm.cfg) != nil, err, retOff, retSz, ret)
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
	gas := javaCallEnergyRequest(&energyVal)

	// OUT_OF_MEMORY (3 MB guard) precedes OUT_OF_ENERGY — see opCall.
	inOff, inSz, _, err := checkedMemoryExpansionCostWords(memory, &inOffset, &inSize, STATICCALL)
	if err != nil {
		return nil, err
	}
	retOff, retSz, _, err := checkedMemoryExpansionCostWords(memory, &retOffset, &retSize, STATICCALL)
	if err != nil {
		return nil, err
	}
	if !interpreter.useEnergy(contract, EnergyCall) {
		return nil, ErrOutOfEnergy
	}
	if memCost := combinedMemoryExpansionCost(memory, inOff, inSz, retOff, retSz); memCost > 0 {
		if !interpreter.useEnergy(contract, memCost) {
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
	interpreter.writeCallReturn(memory, getPrecompile(addr, interpreter.tvm.cfg) != nil, err, retOff, retSz, ret)
	if err == errPrecompileFailure {
		interpreter.returnData = nil
	} else {
		interpreter.returnData = ret
	}
	return nil, nil
}

package vm

// TRON-specific opcode implementations (0xd0–0xdf and 0x5c/0x5d).
// Stack conventions follow java-tron OperationActions.java.
// All state-modifying opcodes have writes:true set in jump_table.go.

import (
	"encoding/binary"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// ── 0x5C TLOAD ────────────────────────────────────────────────────────────────

func opTLoad(_ *uint64, in *Interpreter, _ *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	key := stack.pop()
	result := in.transient[key]
	stack.push(&result)
	return nil, nil
}

// ── 0x5D TSTORE ───────────────────────────────────────────────────────────────

func opTStore(_ *uint64, in *Interpreter, _ *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	key := stack.pop()
	val := stack.pop()
	in.transient[key] = val
	return nil, nil
}

// ── 0xD0 CALLTOKEN ────────────────────────────────────────────────────────────
// Stack (top → bottom): gas, addr, tokenValue, tokenId, inOffset, inSize, retOffset, retSize
// Pushes 1 on success, 0 on failure.

func opCallToken(_ *uint64, in *Interpreter, contract *Contract, mem *Memory, stack *Stack) ([]byte, error) {
	gas := stack.pop()
	addrWord := stack.pop()
	tokenValueWord := stack.pop()
	tokenIdWord := stack.pop()
	inOffsetWord := stack.pop()
	inSizeWord := stack.pop()
	retOffsetWord := stack.pop()
	retSizeWord := stack.pop()

	addr := uint256ToAddress(&addrWord)
	tokenValue := int64(tokenValueWord.Uint64())
	tokenID := int64(tokenIdWord.Uint64())

	callEnergy := gas.Uint64()
	if callEnergy > contract.Energy {
		callEnergy = contract.Energy
	}
	contract.UseEnergy(callEnergy)

	inputData := mem.getCopy(int64(inOffsetWord.Uint64()), int64(inSizeWord.Uint64()))
	ret, remaining, err := in.tvm.CallToken(
		contract.Address, addr, inputData, callEnergy,
		0 /*TRX value*/, tokenID, tokenValue,
	)
	contract.Energy += remaining

	retOffset := int64(retOffsetWord.Uint64())
	retSize := int64(retSizeWord.Uint64())
	if err == nil && len(ret) > 0 && retSize > 0 {
		if int64(len(ret)) > retSize {
			ret = ret[:retSize]
		}
		mem.set(uint64(retOffset), uint64(len(ret)), ret)
	}
	in.returnData = ret

	result := uint256.NewInt(1)
	if err != nil {
		result.Clear()
	}
	stack.push(result)
	return nil, nil
}

// ── 0xD1 TOKENBALANCE ─────────────────────────────────────────────────────────
// Stack: addr, tokenId → balance

func opTokenBalance(_ *uint64, in *Interpreter, _ *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	addrWord := stack.pop()
	tokenIdWord := stack.pop()
	addr := uint256ToAddress(&addrWord)
	tokenID := int64(tokenIdWord.Uint64())

	bal := in.tvm.StateDB.GetTRC10Balance(addr, tokenID)
	result := uint256.NewInt(0)
	result.SetUint64(uint64(bal))
	stack.push(result)
	return nil, nil
}

// ── 0xD2 CALLTOKENVALUE ───────────────────────────────────────────────────────
// Stack: → tokenValue (current message's incoming TRC-10 amount)

func opCallTokenValue(_ *uint64, _ *Interpreter, contract *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	result := uint256.NewInt(0)
	if contract.TokenValue > 0 {
		result.SetUint64(uint64(contract.TokenValue))
	}
	stack.push(result)
	return nil, nil
}

// ── 0xD3 CALLTOKENID ──────────────────────────────────────────────────────────
// Stack: → tokenId (current message's incoming TRC-10 token ID)

func opCallTokenId(_ *uint64, _ *Interpreter, contract *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	result := uint256.NewInt(0)
	if contract.TokenID > 0 {
		result.SetUint64(uint64(contract.TokenID))
	}
	stack.push(result)
	return nil, nil
}

// ── 0xD4 ISCONTRACT ───────────────────────────────────────────────────────────
// Stack: addr → bool (1 if contract, 0 if EOA)

func opIsContract(_ *uint64, in *Interpreter, _ *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	addrWord := stack.pop()
	addr := uint256ToAddress(&addrWord)
	code := in.tvm.StateDB.GetCode(addr)
	result := uint256.NewInt(0)
	if len(code) > 0 {
		result.SetOne()
	}
	stack.push(result)
	return nil, nil
}

// ── 0xD5 FREEZE ───────────────────────────────────────────────────────────────
// Stack: amount, duration (days), resourceType → success
// resourceType: 0=BANDWIDTH, 1=ENERGY

func opFreeze(_ *uint64, in *Interpreter, contract *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	amountWord := stack.pop()
	durationWord := stack.pop()
	resourceWord := stack.pop()

	amount := int64(amountWord.Uint64())
	durationDays := int64(durationWord.Uint64())
	resourceType := int64(resourceWord.Uint64())
	caller := contract.Address

	if err := in.tvm.StateDB.SubBalance(caller, amount); err != nil {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}

	// Freeze expire = current block time + duration * 86400s (in ms)
	expireMs := in.tvm.Timestamp + durationDays*86400_000
	switch resourceType {
	case 0:
		in.tvm.StateDB.FreezeV1Bandwidth(caller, amount, expireMs)
	case 1:
		in.tvm.StateDB.FreezeV1Energy(caller, amount, expireMs)
	default:
		in.tvm.StateDB.AddBalance(caller, amount) // refund unknown type
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	stack.push(uint256.NewInt(1))
	return nil, nil
}

// ── 0xD6 UNFREEZE ─────────────────────────────────────────────────────────────
// Stack: resourceType, receiverAddr → unfrozenAmount

func opUnfreeze(_ *uint64, in *Interpreter, contract *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	resourceWord := stack.pop()
	_ = stack.pop() // receiver address (V1 unfreeze returns to owner)

	resourceType := int64(resourceWord.Uint64())
	caller := contract.Address

	var unfrozen int64
	switch resourceType {
	case 0:
		unfrozen = in.tvm.StateDB.UnfreezeV1Bandwidth(caller, in.tvm.Timestamp)
	case 1:
		unfrozen = in.tvm.StateDB.UnfreezeV1Energy(caller, in.tvm.Timestamp)
	}
	if unfrozen > 0 {
		in.tvm.StateDB.AddBalance(caller, unfrozen)
	}
	result := uint256.NewInt(0)
	result.SetUint64(uint64(unfrozen))
	stack.push(result)
	return nil, nil
}

// ── 0xD7 FREEZEEXPIRETIME ────────────────────────────────────────────────────
// Stack: addr, resourceType → expireTimeMs

func opFreezeExpireTime(_ *uint64, in *Interpreter, _ *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	addrWord := stack.pop()
	resourceWord := stack.pop()
	addr := uint256ToAddress(&addrWord)
	resourceType := int64(resourceWord.Uint64())

	expireMs := in.tvm.StateDB.GetFreezeV1ExpireTime(addr, resourceType)
	result := uint256.NewInt(0)
	result.SetUint64(uint64(expireMs))
	stack.push(result)
	return nil, nil
}

// ── 0xD8 VOTEWITNESS ──────────────────────────────────────────────────────────
// Stack: witnessOffset, witnessCount, amountOffset, amountCount → success
// Memory: witnessOffset points to array of 32-byte TRON addresses (right-aligned)
//         amountOffset points to array of 32-byte vote amounts

func opVoteWitness(_ *uint64, in *Interpreter, contract *Contract, mem *Memory, stack *Stack) ([]byte, error) {
	witnessOffsetWord := stack.pop()
	witnessCountWord := stack.pop()
	amountOffsetWord := stack.pop()
	amountCountWord := stack.pop()

	n := int64(witnessCountWord.Uint64())
	if an := int64(amountCountWord.Uint64()); an < n {
		n = an
	}
	if n <= 0 || n > 30 {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}

	wBase := int64(witnessOffsetWord.Uint64())
	aBase := int64(amountOffsetWord.Uint64())
	caller := contract.Address

	votes := make([]*corepb.Vote, 0, n)
	for i := int64(0); i < n; i++ {
		wBytes := mem.getCopy(wBase+i*32, 32)
		aBytes := mem.getCopy(aBase+i*32, 32)

		var witnessAddr tcommon.Address
		if len(wBytes) == 32 {
			copy(witnessAddr[1:], wBytes[12:32])
			witnessAddr[0] = 0x41
		}
		var amount int64
		if len(aBytes) >= 8 {
			amount = int64(binary.BigEndian.Uint64(aBytes[len(aBytes)-8:]))
		}
		if amount <= 0 {
			continue
		}
		votes = append(votes, &corepb.Vote{
			VoteAddress: witnessAddr[:],
			VoteCount:   amount,
		})
	}

	// Subtract old vote tallies from the SR vote counts.
	for _, v := range in.tvm.StateDB.GetVotes(caller) {
		var wAddr tcommon.Address
		copy(wAddr[:], v.VoteAddress)
		in.tvm.StateDB.AddWitnessVoteCount(wAddr, -v.VoteCount)
	}
	for _, v := range votes {
		var wAddr tcommon.Address
		copy(wAddr[:], v.VoteAddress)
		in.tvm.StateDB.AddWitnessVoteCount(wAddr, v.VoteCount)
	}
	in.tvm.StateDB.SetVotes(caller, votes)

	stack.push(uint256.NewInt(1))
	return nil, nil
}

// ── 0xD9 WITHDRAWREWARD ───────────────────────────────────────────────────────
// Stack: → withdrawnAmount

func opWithdrawReward(_ *uint64, in *Interpreter, contract *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	caller := contract.Address
	allowance := in.tvm.StateDB.GetAllowance(caller)
	if allowance > 0 {
		in.tvm.StateDB.AddBalance(caller, allowance)
		in.tvm.StateDB.SetAllowance(caller, 0)
		in.tvm.StateDB.SetLatestWithdrawTime(caller, in.tvm.Timestamp)
	}
	result := uint256.NewInt(0)
	result.SetUint64(uint64(allowance))
	stack.push(result)
	return nil, nil
}

// ── 0xDA FREEZEBALANCEV2 ──────────────────────────────────────────────────────
// Stack: amount, resourceType → success

func opFreezeBalanceV2(_ *uint64, in *Interpreter, contract *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	amountWord := stack.pop()
	resourceWord := stack.pop()
	amount := int64(amountWord.Uint64())
	resource := corepb.ResourceCode(int32(resourceWord.Uint64()))
	caller := contract.Address

	if err := in.tvm.StateDB.SubBalance(caller, amount); err != nil {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	in.tvm.StateDB.AddFreezeV2(caller, resource, amount)
	stack.push(uint256.NewInt(1))
	return nil, nil
}

// ── 0xDB UNFREEZEBALANCEV2 ────────────────────────────────────────────────────
// Stack: amount, resourceType → success

func opUnfreezeBalanceV2(_ *uint64, in *Interpreter, contract *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	amountWord := stack.pop()
	resourceWord := stack.pop()
	amount := int64(amountWord.Uint64())
	resource := corepb.ResourceCode(int32(resourceWord.Uint64()))
	caller := contract.Address

	frozen := in.tvm.StateDB.GetFrozenV2Amount(caller, resource)
	if amount > frozen {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	in.tvm.StateDB.ReduceFreezeV2(caller, resource, amount)
	expireMs := in.tvm.Timestamp + 14*86400_000 // 14-day unstaking delay
	in.tvm.StateDB.AddUnfreezeV2(caller, resource, amount, expireMs)
	stack.push(uint256.NewInt(1))
	return nil, nil
}

// ── 0xDC CANCELALLUNFREEZEV2 ──────────────────────────────────────────────────
// Stack: → cancelledAmount

func opCancelAllUnfreezeV2(_ *uint64, in *Interpreter, contract *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	cancelled := in.tvm.StateDB.CancelAllUnfreezeV2(contract.Address)
	result := uint256.NewInt(0)
	result.SetUint64(uint64(cancelled))
	stack.push(result)
	return nil, nil
}

// ── 0xDD WITHDRAWEXPIREUNFREEZE ───────────────────────────────────────────────
// Stack: → withdrawnAmount

func opWithdrawExpireUnfreeze(_ *uint64, in *Interpreter, contract *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	caller := contract.Address
	released := in.tvm.StateDB.RemoveExpiredUnfreezeV2(caller, in.tvm.Timestamp)
	if released > 0 {
		in.tvm.StateDB.AddBalance(caller, released)
	}
	result := uint256.NewInt(0)
	result.SetUint64(uint64(released))
	stack.push(result)
	return nil, nil
}

// ── 0xDE DELEGATERESOURCE ────────────────────────────────────────────────────
// Stack: amount, resourceType, receiverAddr → success

func opDelegateResource(_ *uint64, in *Interpreter, contract *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	amountWord := stack.pop()
	resourceWord := stack.pop()
	receiverWord := stack.pop()
	amount := int64(amountWord.Uint64())
	resource := corepb.ResourceCode(int32(resourceWord.Uint64()))
	receiver := uint256ToAddress(&receiverWord)
	caller := contract.Address

	frozen := in.tvm.StateDB.GetFrozenV2Amount(caller, resource)
	if amount > frozen {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	in.tvm.StateDB.ReduceFreezeV2(caller, resource, amount)
	in.tvm.StateDB.AddDelegatedFrozenV2(caller, resource, amount)
	in.tvm.StateDB.AddAcquiredDelegatedFrozenV2(receiver, resource, amount)
	stack.push(uint256.NewInt(1))
	return nil, nil
}

// ── 0xDF UNDELEGATERESOURCE ───────────────────────────────────────────────────
// Stack: amount, resourceType, receiverAddr → success

func opUnDelegateResource(_ *uint64, in *Interpreter, contract *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	amountWord := stack.pop()
	resourceWord := stack.pop()
	receiverWord := stack.pop()
	amount := int64(amountWord.Uint64())
	resource := corepb.ResourceCode(int32(resourceWord.Uint64()))
	receiver := uint256ToAddress(&receiverWord)
	caller := contract.Address

	delegated := in.tvm.StateDB.GetDelegatedFrozenV2(caller, resource)
	if amount > delegated {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	in.tvm.StateDB.SubDelegatedFrozenV2(caller, resource, amount)
	in.tvm.StateDB.SubAcquiredDelegatedFrozenV2(receiver, resource, amount)
	in.tvm.StateDB.AddFreezeV2(caller, resource, amount)
	stack.push(uint256.NewInt(1))
	return nil, nil
}

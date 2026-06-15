package vm

// TRON-specific opcode implementations (0xd0–0xdf and 0x5c/0x5d).
// Stack conventions follow java-tron OperationActions.java.
// All state-modifying opcodes have writes:true set in jump_table.go.

import (
	"encoding/binary"
	"errors"
	"math/bits"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/delegation"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/reward"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const (
	tvmTRXPrecision = int64(1_000_000)
	tvmMinTokenID   = int64(1_000_000)
	tvmMemoryLimit  = uint64(3 * 1024 * 1024)
)

var errVoteWitnessMemoryLength = errors.New("TVM VoteWitness: memory array length do not match length parameter")

func tvmLatestBlockHeaderTimestamp(tvm *TVM) int64 {
	if tvm != nil && tvm.DynProps != nil {
		return tvm.DynProps.LatestBlockHeaderTimestamp()
	}
	if tvm != nil {
		return tvm.Timestamp
	}
	return 0
}

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
	tokenValueNonZero := !tokenValueWord.IsZero()
	tokenValue, tokenValueOK := uint256ToInt64Exact(&tokenValueWord)
	tokenID := int64(tokenIdWord.Uint64())
	isTokenTransfer := tokenID != 0
	if in.tvm.cfg.MultiSign {
		var tokenIDOK bool
		tokenID, tokenIDOK = uint256ToJavaLongExact(&tokenIdWord)
		if !tokenIDOK {
			tokenID = 0
		}
		isTokenTransfer = true
	}

	if in.readOnly && tokenValueNonZero {
		return nil, ErrWriteProtection
	}

	cost := uint64(EnergyCall)
	if tokenValueNonZero {
		cost += EnergyCallValueTx
		if !in.tvm.StateDB.Exist(addr) {
			cost += EnergyCallNewAcct
		}
	}
	if !in.useEnergy(contract, cost) {
		return nil, ErrOutOfEnergy
	}

	inOff, inSz, _, err := checkedMemoryExpansionCostWords(mem, &inOffsetWord, &inSizeWord, CALLTOKEN)
	if err != nil {
		return nil, err
	}
	retOff, retSz, _, err := checkedMemoryExpansionCostWords(mem, &retOffsetWord, &retSizeWord, CALLTOKEN)
	if err != nil {
		return nil, err
	}
	// Single combined expansion to max(inEnd, retEnd) — java EnergyCost
	// calcMemEnergy(oldMemSize, in.max(out)); separate in/ret charges
	// double-count the overlap.
	if memCost := combinedMemoryExpansionCost(mem, inOff, inSz, retOff, retSz); memCost > 0 {
		if !in.useEnergy(contract, memCost) {
			return nil, ErrOutOfEnergy
		}
	}
	resizeMemory(mem, inOff, inSz)
	resizeMemory(mem, retOff, retSz)

	callEnergy := gas.Uint64()
	callEnergy = in.tvm.adjustedCallEnergy(contract, callEnergy)
	contract.UseEnergy(callEnergy)
	if tokenValueNonZero {
		callEnergy += EnergyCallStipend
	}
	if in.tvm.cfg.MultiSign && (tokenID <= tvmMinTokenID) {
		if in.tvm.cfg.Constantinople {
			contract.Energy += callEnergy
			return nil, ErrInvalidTokenIDTransfer
		}
		return nil, ErrInvalidTokenID
	}
	if !tokenValueOK {
		contract.Energy += callEnergy
		return nil, ErrEndowmentOutOfRange
	}

	inputData := mem.getCopy(int64(inOff), int64(inSz))
	var ret []byte
	var remaining uint64
	if isTokenTransfer {
		ret, remaining, err = in.tvm.CallToken(
			contract.Address, addr, inputData, callEnergy,
			0 /*TRX value*/, tokenID, tokenValue,
		)
	} else {
		ret, remaining, err = in.tvm.Call(contract.Address, addr, inputData, callEnergy, tokenValue)
	}
	contract.Energy += remaining
	if shouldPropagateCallError(err) {
		return nil, err
	}

	retOffset := int64(retOff)
	retSize := int64(retSz)
	if err == nil && len(ret) > 0 && retSize > 0 {
		if int64(len(ret)) > retSize {
			ret = ret[:retSize]
		}
		mem.set(uint64(retOffset), uint64(len(ret)), ret)
	}
	if err == errPrecompileFailure {
		in.returnData = nil
	} else {
		in.returnData = ret
	}

	result := uint256.NewInt(1)
	if err != nil {
		result.Clear()
	}
	stack.push(result)
	return nil, nil
}

// ── 0xD1 TOKENBALANCE ─────────────────────────────────────────────────────────
// Stack top first: tokenId, addr → balance

func opTokenBalance(_ *uint64, in *Interpreter, _ *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	tokenIdWord := stack.pop()
	addrWord := stack.pop()
	addr := uint256ToAddress(&addrWord)
	tokenID := int64(tokenIdWord.Uint64())
	if in.tvm.cfg.MultiSign {
		var ok bool
		tokenID, ok = uint256ToJavaLongExact(&tokenIdWord)
		if !ok {
			if in.tvm.cfg.Constantinople {
				return nil, ErrInvalidTokenIDTransfer
			}
			return nil, ErrInvalidTokenID
		}
		if tokenID <= tvmMinTokenID {
			return nil, ErrInvalidTokenID
		}
	}

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
	if contract.TokenValue != 0 {
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
	result := uint256.NewInt(0)
	if in.tvm.StateDB.GetContract(addr) != nil {
		result.SetOne()
	}
	stack.push(result)
	return nil, nil
}

// ── 0xD5 FREEZE ───────────────────────────────────────────────────────────────
// Stack: receiverAddr, amount, resourceType → success
// resourceType: 0=BANDWIDTH, 1=ENERGY

func opFreeze(_ *uint64, in *Interpreter, contract *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	if in.tvm.cfg.Vote && in.readOnly {
		return nil, ErrWriteProtection
	}
	resourceWord := stack.pop()
	amountWord := stack.pop()
	receiverWord := stack.pop()

	amount := int64(amountWord.Uint64())
	resourceType := int64(resourceWord.Uint64())
	caller := contract.Address
	receiver := uint256ToAddress(&receiverWord)

	if in.tvm.cfg.StakingV2 || amount < tvmTRXPrecision || (resourceType != 0 && resourceType != 1) {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	if err := in.tvm.StateDB.SubBalance(caller, amount); err != nil {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}

	durationDays := int64(3)
	if in.tvm.DynProps != nil && in.tvm.DynProps.MinFrozenTime() > 0 {
		durationDays = in.tvm.DynProps.MinFrozenTime()
	}
	nowMs := tvmLatestBlockHeaderTimestamp(in.tvm)
	expireMs := nowMs + durationDays*86400_000
	delegated := receiver != caller
	if delegated && !in.tvm.StateDB.AccountExists(receiver) {
		in.tvm.StateDB.CreateAccountWithTime(receiver, corepb.AccountType_Normal, nowMs)
		if in.tvm.DynProps != nil && in.tvm.DynProps.AllowMultiSign() {
			in.tvm.StateDB.ApplyDefaultAccountPermissions(receiver, in.tvm.DynProps)
		}
	}
	if delegated {
		recvAccount := in.tvm.StateDB.GetAccount(receiver)
		if recvAccount != nil && recvAccount.Type() == corepb.AccountType_Contract {
			in.tvm.StateDB.AddBalance(caller, amount)
			stack.push(uint256.NewInt(0))
			return nil, nil
		}
	}
	switch resourceType {
	case 0:
		if delegated {
			in.tvm.StateDB.FreezeV1DelegatedBandwidth(caller, receiver, amount)
			dr := in.tvm.StateDB.ReadDelegatedResourceLegacy(caller, receiver)
			if dr == nil {
				dr = &rawdb.DelegatedResource{From: caller, To: receiver}
			}
			dr.FrozenBalanceForBandwidth += amount
			dr.ExpireTimeForBandwidth = expireMs
			_ = in.tvm.StateDB.WriteDelegatedResourceLegacy(caller, receiver, dr)
		} else {
			in.tvm.StateDB.FreezeV1Bandwidth(caller, amount, expireMs)
		}
	case 1:
		if delegated {
			in.tvm.StateDB.FreezeV1DelegatedEnergy(caller, receiver, amount)
			dr := in.tvm.StateDB.ReadDelegatedResourceLegacy(caller, receiver)
			if dr == nil {
				dr = &rawdb.DelegatedResource{From: caller, To: receiver}
			}
			dr.FrozenBalanceForEnergy += amount
			dr.ExpireTimeForEnergy = expireMs
			_ = in.tvm.StateDB.WriteDelegatedResourceLegacy(caller, receiver, dr)
		} else {
			in.tvm.StateDB.FreezeV1Energy(caller, amount, expireMs)
		}
	}
	if resourceType == 0 {
		tvmAddResourceWeight(in.tvm, corepb.ResourceCode_BANDWIDTH, amount/tvmTRXPrecision)
	} else {
		tvmAddResourceWeight(in.tvm, corepb.ResourceCode_ENERGY, amount/tvmTRXPrecision)
	}
	stack.push(uint256.NewInt(1))
	return nil, nil
}

// ── 0xD6 UNFREEZE ─────────────────────────────────────────────────────────────
// Stack: receiverAddr, resourceType → success

func opUnfreeze(_ *uint64, in *Interpreter, contract *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	if in.tvm.cfg.Vote && in.readOnly {
		return nil, ErrWriteProtection
	}
	resourceWord := stack.pop()
	receiverWord := stack.pop()

	resourceType := int64(resourceWord.Uint64())
	caller := contract.Address
	receiver := uint256ToAddress(&receiverWord)
	nowMs := tvmLatestBlockHeaderTimestamp(in.tvm)

	var unfrozen int64
	delegated := receiver != caller
	if delegated {
		dr := in.tvm.StateDB.ReadDelegatedResourceLegacy(caller, receiver)
		if dr == nil {
			stack.push(uint256.NewInt(0))
			return nil, nil
		}
		switch resourceType {
		case 0:
			if dr.FrozenBalanceForBandwidth <= 0 || dr.ExpireTimeForBandwidth > nowMs {
				stack.push(uint256.NewInt(0))
				return nil, nil
			}
			unfrozen = dr.FrozenBalanceForBandwidth
			dr.FrozenBalanceForBandwidth = 0
			dr.ExpireTimeForBandwidth = 0
			in.tvm.StateDB.UnfreezeV1DelegatedBandwidth(caller, receiver, unfrozen)
		case 1:
			if dr.FrozenBalanceForEnergy <= 0 || dr.ExpireTimeForEnergy > nowMs {
				stack.push(uint256.NewInt(0))
				return nil, nil
			}
			unfrozen = dr.FrozenBalanceForEnergy
			dr.FrozenBalanceForEnergy = 0
			dr.ExpireTimeForEnergy = 0
			in.tvm.StateDB.UnfreezeV1DelegatedEnergy(caller, receiver, unfrozen)
		default:
			stack.push(uint256.NewInt(0))
			return nil, nil
		}
		if dr.FrozenBalanceForBandwidth == 0 && dr.FrozenBalanceForEnergy == 0 {
			_ = in.tvm.StateDB.DeleteDelegatedResourceLegacy(caller, receiver)
		} else {
			_ = in.tvm.StateDB.WriteDelegatedResourceLegacy(caller, receiver, dr)
		}
	} else {
		switch resourceType {
		case 0:
			unfrozen = in.tvm.StateDB.UnfreezeV1Bandwidth(caller, nowMs)
		case 1:
			unfrozen = in.tvm.StateDB.UnfreezeV1Energy(caller, nowMs)
		default:
			stack.push(uint256.NewInt(0))
			return nil, nil
		}
	}
	if unfrozen > 0 {
		in.tvm.StateDB.AddBalance(caller, unfrozen)
		if resourceType == 0 {
			tvmAddResourceWeight(in.tvm, corepb.ResourceCode_BANDWIDTH, -unfrozen/tvmTRXPrecision)
		} else {
			tvmAddResourceWeight(in.tvm, corepb.ResourceCode_ENERGY, -unfrozen/tvmTRXPrecision)
		}
		_ = updateTVMVotesAfterUnfreezeV1(in.tvm, caller)
	}
	result := uint256.NewInt(0)
	if unfrozen > 0 {
		result.SetOne()
	}
	stack.push(result)
	return nil, nil
}

// ── 0xD7 FREEZEEXPIRETIME ────────────────────────────────────────────────────
// Stack: addr, resourceType → expireTimeSeconds

func opFreezeExpireTime(_ *uint64, in *Interpreter, contract *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	resourceWord := stack.pop()
	addrWord := stack.pop()
	addr := uint256ToAddress(&addrWord)
	resourceType := int64(resourceWord.Uint64())

	var expireMs int64
	if addr != contract.Address {
		if dr := in.tvm.StateDB.ReadDelegatedResourceLegacy(contract.Address, addr); dr != nil {
			switch resourceType {
			case 0:
				expireMs = dr.ExpireTimeForBandwidth
			case 1:
				expireMs = dr.ExpireTimeForEnergy
			}
		}
	} else {
		expireMs = in.tvm.StateDB.GetFreezeV1ExpireTime(addr, resourceType)
	}
	result := uint256.NewInt(0)
	result.SetUint64(uint64(expireMs / 1000))
	stack.push(result)
	return nil, nil
}

// ── 0xD8 VOTEWITNESS ──────────────────────────────────────────────────────────
// Stack: witnessOffset, witnessCount, amountOffset, amountCount -> success
// Memory: witnessOffset/amountOffset point to ABI arrays:
//         [length word][32-byte element...]

func opVoteWitness(_ *uint64, in *Interpreter, contract *Contract, mem *Memory, stack *Stack) ([]byte, error) {
	amountCountWord := stack.pop()
	amountOffsetWord := stack.pop()
	witnessCountWord := stack.pop()
	witnessOffsetWord := stack.pop()

	cost, needed, err := voteWitnessMemoryEnergyCost(in, mem, &witnessOffsetWord, &witnessCountWord, &amountOffsetWord, &amountCountWord)
	if err != nil {
		return nil, err
	}
	if cost > 0 {
		if !in.useEnergy(contract, cost) {
			return nil, ErrOutOfEnergy
		}
	}
	if needed > 0 && mem != nil && uint64(mem.len()) < needed {
		mem.resize(needed)
	}

	n := int64(witnessCountWord.Uint64())
	amountN := int64(amountCountWord.Uint64())
	if n != amountN {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	wBase := int64(witnessOffsetWord.Uint64())
	aBase := int64(amountOffsetWord.Uint64())
	if got, ok := memoryArrayLength(mem, wBase); !ok || got != n {
		return nil, errVoteWitnessMemoryLength
	}
	if got, ok := memoryArrayLength(mem, aBase); !ok || got != n {
		return nil, errVoteWitnessMemoryLength
	}
	if n < 0 || n > 30 {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}

	caller := contract.Address

	const maxInt64 = int64(^uint64(0) >> 1)

	voteSums := make(map[tcommon.Address]int64, n)
	voteOrder := make([]tcommon.Address, 0, n)
	var totalVotes int64
	for i := int64(0); i < n; i++ {
		wBytes := mem.getCopy(wBase+32+i*32, 32)
		aBytes := mem.getCopy(aBase+32+i*32, 32)

		var witnessAddr tcommon.Address
		if len(wBytes) == 32 {
			copy(witnessAddr[1:], wBytes[12:32])
			witnessAddr[0] = 0x41
		}
		if in.tvm.StateDB.GetWitness(witnessAddr) == nil {
			stack.push(uint256.NewInt(0))
			return nil, nil
		}
		amount, ok := int64ExactFromWord(aBytes)
		if !ok || amount < 0 {
			stack.push(uint256.NewInt(0))
			return nil, nil
		}
		if amount == 0 {
			continue
		}
		if totalVotes > maxInt64-amount {
			stack.push(uint256.NewInt(0))
			return nil, nil
		}
		totalVotes += amount
		if _, ok := voteSums[witnessAddr]; !ok {
			voteOrder = append(voteOrder, witnessAddr)
		}
		if voteSums[witnessAddr] > maxInt64-amount {
			stack.push(uint256.NewInt(0))
			return nil, nil
		}
		voteSums[witnessAddr] += amount
	}
	if totalVotes > 0 && totalVotes > maxInt64/tvmTRXPrecision {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	var tronPower int64
	if in.tvm.DynProps != nil && in.tvm.DynProps.SupportUnfreezeDelay() && in.tvm.DynProps.AllowNewResourceModel() {
		tronPower = in.tvm.StateDB.GetAllTronPower(caller)
	} else {
		tronPower = in.tvm.StateDB.GetLegacyTronPower(caller)
	}
	if totalVotes*tvmTRXPrecision > tronPower {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}

	votes := make([]*corepb.Vote, 0, len(voteOrder))
	for _, witnessAddr := range voteOrder {
		votes = append(votes, &corepb.Vote{
			VoteAddress: witnessAddr.Bytes(),
			VoteCount:   voteSums[witnessAddr],
		})
	}

	tvmWithdrawReward(in.tvm, caller)
	oldVotes := in.tvm.StateDB.GetVotes(caller)
	if err := recordTVMPendingVotes(in.tvm, caller, oldVotes, votes); err != nil {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	if len(votes) == 0 {
		in.tvm.StateDB.ClearVotes(caller)
	} else {
		in.tvm.StateDB.SetVotes(caller, votes)
	}

	stack.push(uint256.NewInt(1))
	return nil, nil
}

func voteWitnessMemoryEnergyCost(in *Interpreter, mem *Memory, witnessOffset, witnessCount, amountOffset, amountCount *uint256.Int) (uint64, uint64, error) {
	includeLengthWord := in.tvmConfig.EnergyAdjustment || in.tvmConfig.Osaka
	wEnd, ok := voteWitnessArrayEnd(witnessOffset, witnessCount, includeLengthWord)
	if !ok {
		return 0, 0, newOutOfMemoryError(VOTEWITNESS)
	}
	aEnd, ok := voteWitnessArrayEnd(amountOffset, amountCount, includeLengthWord)
	if !ok {
		return 0, 0, newOutOfMemoryError(VOTEWITNESS)
	}
	needed := wEnd
	if aEnd > needed {
		needed = aEnd
	}
	if needed > tvmMemoryLimit {
		return 0, 0, newOutOfMemoryError(VOTEWITNESS)
	}
	var oldSize uint64
	if mem != nil {
		oldSize = uint64(mem.len())
	}
	if oldSize >= needed {
		return 0, needed, nil
	}
	return memoryEnergyCost(needed) - memoryEnergyCost(oldSize), needed, nil
}

func voteWitnessArrayEnd(offset, count *uint256.Int, includeLengthWord bool) (uint64, bool) {
	if !offset.IsUint64() || !count.IsUint64() {
		return 0, false
	}
	hi, size := bits.Mul64(count.Uint64(), 32)
	if hi != 0 {
		return 0, false
	}
	if size == 0 && !includeLengthWord {
		return 0, true
	}
	if includeLengthWord {
		if size > ^uint64(0)-32 {
			return 0, false
		}
		size += 32
	}
	off := offset.Uint64()
	if off > ^uint64(0)-size {
		return 0, false
	}
	return off + size, true
}

func memoryArrayLength(mem *Memory, offset int64) (int64, bool) {
	if mem == nil || offset < 0 || offset+32 > int64(mem.len()) {
		return 0, false
	}
	word := mem.getCopy(offset, 32)
	for _, b := range word[:24] {
		if b != 0 {
			return 0, false
		}
	}
	u := binary.BigEndian.Uint64(word[24:])
	const maxInt64 = uint64(^uint64(0) >> 1)
	if u > maxInt64 {
		return 0, false
	}
	return int64(u), true
}

func int64ExactFromWord(word []byte) (int64, bool) {
	if len(word) != 32 {
		return 0, false
	}
	negative := word[24]&0x80 != 0
	fill := byte(0)
	if negative {
		fill = 0xff
	}
	for _, b := range word[:24] {
		if b != fill {
			return 0, false
		}
	}
	return int64(binary.BigEndian.Uint64(word[24:])), true
}

func cloneVMVotes(votes []*corepb.Vote) []*corepb.Vote {
	if len(votes) == 0 {
		return nil
	}
	out := make([]*corepb.Vote, 0, len(votes))
	for _, vote := range votes {
		if vote == nil {
			continue
		}
		out = append(out, &corepb.Vote{
			VoteAddress: append([]byte(nil), vote.VoteAddress...),
			VoteCount:   vote.VoteCount,
		})
	}
	return out
}

// recordTVMPendingVotes stages a voter's epoch delta into the rooted VotesStore
// (WitnessVoteState KV) on the TVM's statedb — the same *StateDB the enclosing
// actuator and the maintenance drain hold — so a contract-issued vote is
// visible to the same-block drain and rewinds with the full state root.
func recordTVMPendingVotes(tvm *TVM, owner tcommon.Address, oldVotes, newVotes []*corepb.Vote) error {
	if tvm.StateDB == nil {
		return nil
	}
	pending := tvm.StateDB.ReadVotes(owner)
	if pending == nil {
		pending = &corepb.Votes{
			Address:  owner.Bytes(),
			OldVotes: cloneVMVotes(oldVotes),
		}
	}
	pending.NewVotes = cloneVMVotes(newVotes)
	return tvm.StateDB.WriteVotes(owner, pending)
}

func validTVMStakeV2Resource(tvm *TVM, resource corepb.ResourceCode) bool {
	switch resource {
	case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_ENERGY:
		return true
	case corepb.ResourceCode_TRON_POWER:
		return tvm.DynProps != nil && tvm.DynProps.AllowNewResourceModel()
	default:
		return false
	}
}

func tvmFrozenV2WithDelegatedWeight(s interface {
	GetFrozenV2Amount(tcommon.Address, corepb.ResourceCode) int64
	GetDelegatedFrozenV2(tcommon.Address, corepb.ResourceCode) int64
}, addr tcommon.Address, resource corepb.ResourceCode) int64 {
	balance := s.GetFrozenV2Amount(addr, resource)
	if resource != corepb.ResourceCode_TRON_POWER {
		balance += s.GetDelegatedFrozenV2(addr, resource)
	}
	return balance / tvmTRXPrecision
}

func tvmAddResourceWeight(tvm *TVM, resource corepb.ResourceCode, delta int64) {
	if tvm.DynProps == nil || delta == 0 {
		return
	}
	switch resource {
	case corepb.ResourceCode_BANDWIDTH:
		tvm.DynProps.AddTotalNetWeight(delta)
	case corepb.ResourceCode_ENERGY:
		tvm.DynProps.AddTotalEnergyWeight(delta)
	case corepb.ResourceCode_TRON_POWER:
		tvm.DynProps.AddTotalTronPowerWeight(delta)
	}
}

func tvmUnfreezingV2Count(account interface {
	UnfrozenV2() []*corepb.Account_UnFreezeV2
}, now int64) int {
	if account == nil {
		return 0
	}
	var count int
	for _, u := range account.UnfrozenV2() {
		if u.UnfreezeExpireTime > now {
			count++
		}
	}
	return count
}

func updateTVMVotesAfterUnfreezeV2(tvm *TVM, owner tcommon.Address, resource corepb.ResourceCode) error {
	if tvm == nil || !tvm.cfg.Vote {
		return nil
	}
	votes := tvm.StateDB.GetVotes(owner)
	if len(votes) == 0 {
		return nil
	}
	if tvm.DynProps != nil && tvm.DynProps.AllowNewResourceModel() {
		account := tvm.StateDB.GetAccount(owner)
		if account != nil && account.OldTronPowerIsInvalid() &&
			(resource == corepb.ResourceCode_BANDWIDTH || resource == corepb.ResourceCode_ENERGY) {
			return nil
		}
		if err := recordTVMPendingVotes(tvm, owner, votes, nil); err != nil {
			return err
		}
		tvm.StateDB.ClearVotes(owner)
		return nil
	}

	var totalVotes int64
	for _, vote := range votes {
		totalVotes += vote.VoteCount
	}
	if totalVotes == 0 {
		return nil
	}
	ownedTronPower := tvm.StateDB.GetLegacyTronPower(owner)
	if tvm.DynProps != nil && tvm.DynProps.AllowNewResourceModel() {
		ownedTronPower = tvm.StateDB.GetAllTronPower(owner)
	}
	if totalVotes <= ownedTronPower/tvmTRXPrecision {
		return nil
	}
	newVotes := make([]*corepb.Vote, 0, len(votes))
	for _, vote := range votes {
		newVoteCount := int64(float64(vote.VoteCount) / float64(totalVotes) * float64(ownedTronPower) / float64(tvmTRXPrecision))
		if newVoteCount > 0 {
			newVotes = append(newVotes, &corepb.Vote{
				VoteAddress: append([]byte(nil), vote.VoteAddress...),
				VoteCount:   newVoteCount,
			})
		}
	}
	if err := recordTVMPendingVotes(tvm, owner, votes, newVotes); err != nil {
		return err
	}
	if len(newVotes) == 0 {
		tvm.StateDB.ClearVotes(owner)
		return nil
	}
	tvm.StateDB.SetVotes(owner, newVotes)
	return nil
}

func updateTVMVotesAfterUnfreezeV1(tvm *TVM, owner tcommon.Address) error {
	if tvm == nil || !tvm.cfg.Vote {
		return nil
	}
	votes := tvm.StateDB.GetVotes(owner)
	if len(votes) == 0 {
		return nil
	}
	var usedTronPower int64
	for _, vote := range votes {
		usedTronPower += vote.VoteCount
	}
	if tvm.StateDB.GetLegacyTronPower(owner) >= usedTronPower*tvmTRXPrecision {
		return nil
	}
	tvmWithdrawReward(tvm, owner)
	if err := recordTVMPendingVotes(tvm, owner, votes, nil); err != nil {
		return err
	}
	tvm.StateDB.ClearVotes(owner)
	return nil
}

// ── 0xD9 WITHDRAWREWARD ───────────────────────────────────────────────────────
// Stack: → withdrawnAmount

func opWithdrawReward(_ *uint64, in *Interpreter, contract *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	caller := contract.Address
	if isTVMGenesisWitness(in.tvm, caller) {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	withdrawable := tvmQueryReward(in.tvm, caller)
	const maxInt64 = int64(^uint64(0) >> 1)
	if withdrawable <= 0 || in.tvm.StateDB.GetBalance(caller) > maxInt64-withdrawable {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	tvmWithdrawReward(in.tvm, caller)
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

// tvmWithdrawRewardAndCancelVote mirrors java-tron Program.withdrawRewardAndCancelVote,
// which both suicide (Program.java:457) and suicide2 (547) invoke at the top when
// allow_tvm_vote is active. It (1) settles the destroyed contract's pending voter
// reward into allowance, (2) CANCELS its votes — recording a removal (new=nil) on
// the pending VotesStore record so the next maintenance fold subtracts those votes
// from the witnesses it voted for, then clearing the persistent votes — and (3)
// rolls allowance into balance so the inheritor receives it. Without step 2 the
// destroyed account's votes vanish from the account store (voter sum) but stay in
// the witness vote tally (it was only ever added by the fold, never removed),
// drifting tally above voter-sum and corrupting the standby-reward voteSum.
func tvmWithdrawRewardAndCancelVote(tvm *TVM, owner tcommon.Address) {
	tvmWithdrawReward(tvm, owner)
	if votes := tvm.StateDB.GetVotes(owner); len(votes) > 0 {
		// recordTVMPendingVotes preserves an existing record's epoch-start OldVotes
		// and sets NewVotes=nil (java: new VotesCapsule(old=votes) / clearNewVotes),
		// so the fold delta is (nil - old) = full removal.
		_ = recordTVMPendingVotes(tvm, owner, votes, nil)
		tvm.StateDB.ClearVotes(owner)
		tvm.StateDB.SetOldTronPower(owner, 0)
	}
	allowance := tvm.StateDB.GetAllowance(owner)
	if allowance != 0 {
		tvm.StateDB.AddBalance(owner, allowance)
	}
	tvm.StateDB.SetAllowance(owner, 0)
	tvm.StateDB.SetLatestWithdrawTime(owner, tvm.Timestamp)
}

func isTVMGenesisWitness(tvm *TVM, addr tcommon.Address) bool {
	if tvm == nil || tvm.DB == nil {
		return false
	}
	for _, witness := range rawdb.ReadGenesisWitnesses(tvm.DB) {
		if witness.Address == addr {
			return true
		}
	}
	return false
}

func tvmWithdrawReward(tvm *TVM, addr tcommon.Address) {
	if tvm == nil || !tvm.cfg.Vote || tvm.StateDB == nil || tvm.DynProps == nil {
		return
	}
	currentCycle := tvm.DynProps.CurrentCycleNumber()
	beginCycle := tvm.StateDB.ReadBeginCycle(addr.Bytes())
	endCycle := tvm.StateDB.ReadEndCycle(addr.Bytes())
	acct := tvm.StateDB.GetAccount(addr)
	if acct == nil || beginCycle > currentCycle {
		return
	}
	if beginCycle == currentCycle {
		if snap := tvm.StateDB.ReadCycleAccountVote(beginCycle, addr.Bytes()); snap != nil {
			return
		}
	}
	if beginCycle+1 == endCycle && beginCycle < currentCycle {
		if votes := tvmReadSnapshotVotes(tvm.StateDB, beginCycle, addr); len(votes) > 0 {
			tvmAdjustAllowance(tvm, addr, reward.ComputeVoterReward(tvm.StateDB, tvm.DynProps, votes, beginCycle, endCycle))
		}
		beginCycle++
	}
	endCycle = currentCycle

	currentVotes := tvmVoteEntriesFromAccount(acct)
	if len(currentVotes) == 0 {
		_ = tvm.StateDB.WriteBeginCycle(addr.Bytes(), endCycle+1)
		return
	}
	if beginCycle < endCycle {
		tvmAdjustAllowance(tvm, addr, reward.ComputeVoterReward(tvm.StateDB, tvm.DynProps, currentVotes, beginCycle, endCycle))
	}
	_ = tvm.StateDB.WriteBeginCycle(addr.Bytes(), endCycle)
	_ = tvm.StateDB.WriteEndCycle(addr.Bytes(), endCycle+1)
	if snap := tvmMarshalAccountVote(acct); snap != nil {
		_ = tvm.StateDB.WriteCycleAccountVote(endCycle, addr.Bytes(), snap)
	}
}

func tvmQueryReward(tvm *TVM, addr tcommon.Address) int64 {
	if tvm == nil || !tvm.cfg.Vote || tvm.StateDB == nil || tvm.DynProps == nil {
		return 0
	}
	acct := tvm.StateDB.GetAccount(addr)
	if acct == nil {
		return 0
	}
	allowance := tvm.StateDB.GetAllowance(addr)
	currentCycle := tvm.DynProps.CurrentCycleNumber()
	beginCycle := tvm.StateDB.ReadBeginCycle(addr.Bytes())
	endCycle := tvm.StateDB.ReadEndCycle(addr.Bytes())
	if beginCycle > currentCycle {
		return allowance
	}

	var pending int64
	if beginCycle+1 == endCycle && beginCycle < currentCycle {
		if votes := tvmReadSnapshotVotes(tvm.StateDB, beginCycle, addr); len(votes) > 0 {
			pending += reward.ComputeVoterReward(tvm.StateDB, tvm.DynProps, votes, beginCycle, endCycle)
		}
		beginCycle++
	}
	endCycle = currentCycle
	currentVotes := tvmVoteEntriesFromAccount(acct)
	if len(currentVotes) == 0 {
		return pending + allowance
	}
	if beginCycle < endCycle {
		pending += reward.ComputeVoterReward(tvm.StateDB, tvm.DynProps, currentVotes, beginCycle, endCycle)
	}
	return pending + allowance
}

type tvmVoteAccount interface {
	Votes() []*corepb.Vote
	Proto() *corepb.Account
}

func tvmVoteEntriesFromAccount(acct tvmVoteAccount) []reward.VoteEntry {
	if acct == nil {
		return nil
	}
	votes := acct.Votes()
	out := make([]reward.VoteEntry, 0, len(votes))
	for _, vote := range votes {
		out = append(out, reward.VoteEntry{
			Witness: tcommon.BytesToAddress(vote.VoteAddress),
			Count:   vote.VoteCount,
		})
	}
	return out
}

func tvmReadSnapshotVotes(store interface {
	ReadCycleAccountVote(cycle int64, addr []byte) []byte
}, cycle int64, addr tcommon.Address) []reward.VoteEntry {
	raw := store.ReadCycleAccountVote(cycle, addr.Bytes())
	if len(raw) == 0 {
		return nil
	}
	snap := &corepb.Account{}
	if err := proto.Unmarshal(raw, snap); err != nil {
		return nil
	}
	out := make([]reward.VoteEntry, 0, len(snap.Votes))
	for _, vote := range snap.Votes {
		out = append(out, reward.VoteEntry{
			Witness: tcommon.BytesToAddress(vote.VoteAddress),
			Count:   vote.VoteCount,
		})
	}
	return out
}

func tvmMarshalAccountVote(acct tvmVoteAccount) []byte {
	if acct == nil {
		return nil
	}
	raw, err := proto.Marshal(acct.Proto())
	if err != nil {
		return nil
	}
	return raw
}

func tvmAdjustAllowance(tvm *TVM, addr tcommon.Address, amount int64) {
	if amount <= 0 {
		return
	}
	tvm.StateDB.AddAllowance(addr, amount)
}

// ── 0xDA FREEZEBALANCEV2 ──────────────────────────────────────────────────────
// Stack: amount, resourceType → success

func opFreezeBalanceV2(_ *uint64, in *Interpreter, contract *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	amountWord := stack.pop()
	resourceWord := stack.pop()
	amount := int64(amountWord.Uint64())
	resource := corepb.ResourceCode(int32(resourceWord.Uint64()))
	caller := contract.Address

	if amount < tvmTRXPrecision || !validTVMStakeV2Resource(in.tvm, resource) {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	if in.tvm.DynProps != nil && in.tvm.DynProps.AllowNewResourceModel() {
		in.tvm.StateDB.InitializeOldTronPowerIfNeeded(caller)
	}
	oldWeight := tvmFrozenV2WithDelegatedWeight(in.tvm.StateDB, caller, resource)
	if err := in.tvm.StateDB.SubBalance(caller, amount); err != nil {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	in.tvm.StateDB.AddFreezeV2(caller, resource, amount)
	newWeight := tvmFrozenV2WithDelegatedWeight(in.tvm.StateDB, caller, resource)
	tvmAddResourceWeight(in.tvm, resource, newWeight-oldWeight)
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

	if amount <= 0 || !validTVMStakeV2Resource(in.tvm, resource) || tvmUnfreezingV2Count(in.tvm.StateDB.GetAccount(caller), in.tvm.Timestamp) >= 32 {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	frozen := in.tvm.StateDB.GetFrozenV2Amount(caller, resource)
	if amount > frozen {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	tvmWithdrawReward(in.tvm, caller)
	withdrawnExpired := in.tvm.StateDB.RemoveExpiredUnfreezeV2(caller, in.tvm.Timestamp)
	if withdrawnExpired > 0 {
		in.tvm.StateDB.AddBalance(caller, withdrawnExpired)
	}
	if in.tvm.DynProps != nil && in.tvm.DynProps.AllowNewResourceModel() {
		in.tvm.StateDB.InitializeOldTronPowerIfNeeded(caller)
	}
	oldWeight := tvmFrozenV2WithDelegatedWeight(in.tvm.StateDB, caller, resource)
	in.tvm.StateDB.ReduceFreezeV2(caller, resource, amount)
	newWeight := tvmFrozenV2WithDelegatedWeight(in.tvm.StateDB, caller, resource)
	tvmAddResourceWeight(in.tvm, resource, newWeight-oldWeight)
	delayDays := int64(14)
	if in.tvm.DynProps != nil && in.tvm.DynProps.UnfreezeDelayDays() > 0 {
		delayDays = in.tvm.DynProps.UnfreezeDelayDays()
	}
	expireMs := in.tvm.Timestamp + delayDays*86400_000
	in.tvm.StateDB.AddUnfreezeV2(caller, resource, amount, expireMs)
	if err := updateTVMVotesAfterUnfreezeV2(in.tvm, caller, resource); err != nil {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	if in.tvm.DynProps != nil && in.tvm.DynProps.AllowNewResourceModel() {
		in.tvm.StateDB.InvalidateOldTronPower(caller)
	}
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
	amount, ok := uint256ToInt64Exact(&amountWord)
	if !ok || amount < tvmTRXPrecision {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	resource := corepb.ResourceCode(int32(resourceWord.Uint64()))
	receiver := uint256ToAddress(&receiverWord)
	caller := contract.Address

	frozen := in.tvm.StateDB.GetFrozenV2Amount(caller, resource)
	if amount > frozen {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	// java's DelegateResourceProcessor (VM native contract) does NOT recover or
	// persist the owner's usage on delegate: validate() mutates a getAccount()
	// byte-snapshot copy that is never put back, and execute() only adjusts the
	// frozen/delegated balances. Recovering+writing the owner usage here diverged
	// from java (a Stake-2.0 state difference); leave the owner's usage untouched.
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
	amount, ok := uint256ToInt64Exact(&amountWord)
	if !ok || amount <= 0 {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	resource := corepb.ResourceCode(int32(resourceWord.Uint64()))
	receiver := uint256ToAddress(&receiverWord)
	caller := contract.Address

	delegated := in.tvm.StateDB.GetDelegatedFrozenV2(caller, resource)
	if amount > delegated {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	// Before balances shift, transfer the receiver's proportional usage
	// back to the owner. Mirrors java-tron UnDelegateResourceActuator.execute.
	dp := in.tvm.StateDB.DynamicProperties()
	resourceTime := in.tvm.ResourceTime()
	transfer, recvRawWindow, recvOptimized := delegation.TransferUsageFromReceiver(in.tvm.StateDB, dp, receiver, resource, amount, resourceTime)
	in.tvm.StateDB.SubDelegatedFrozenV2(caller, resource, amount)
	in.tvm.StateDB.SubAcquiredDelegatedFrozenV2(receiver, resource, amount)
	in.tvm.StateDB.AddFreezeV2(caller, resource, amount)
	// java unDelegateIncrease runs UNCONDITIONALLY and blends the receiver window.
	delegation.FoldUsageIntoOwner(in.tvm.StateDB, dp, caller, resource, transfer, recvRawWindow, recvOptimized, resourceTime)
	stack.push(uint256.NewInt(1))
	return nil, nil
}

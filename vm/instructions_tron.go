package vm

// TRON-specific opcode implementations (0xd0–0xdf and 0x5c/0x5d).
// Stack conventions follow java-tron OperationActions.java.
// All state-modifying opcodes have writes:true set in jump_table.go.

import (
	"encoding/binary"
	"errors"
	"math/big"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/delegation"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/reward"
	"github.com/tronprotocol/go-tron/core/state"
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

func opTLoad(_ *uint64, in *Interpreter, contract *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	loc := stack.pop()
	val := in.tvm.StateDB.GetTransientState(contract.Address, tcommon.Hash(loc.Bytes32()))
	result := new(uint256.Int).SetBytes(val[:])
	stack.push(result)
	return nil, nil
}

// ── 0x5D TSTORE ───────────────────────────────────────────────────────────────

func opTStore(_ *uint64, in *Interpreter, contract *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	loc := stack.pop()
	val := stack.pop()
	in.tvm.StateDB.SetTransientState(contract.Address, tcommon.Hash(loc.Bytes32()), tcommon.Hash(val.Bytes32()))
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

	in.writeCallReturn(mem, getPrecompile(addr, in.tvm.cfg) != nil, err, retOff, retSz, ret)
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

	// SV-3: java OperationActions.freezeAction calls program.freeze() only when
	// allowTvmFreezeV2 is NOT active (else it pushes 0 without calling freeze);
	// freeze() then increaseNonce() once up front (Program.java:1920), before its
	// own validate/execute, so the nonce advances even on a validate failure. The
	// nonce feeds a subsequent CREATE address, so mirror the +1 here (gated on the
	// same !StakingV2 condition) before any push-0 early return.
	if !in.tvm.cfg.StakingV2 {
		in.tvm.Nonce++
	}

	// A (truncation): java Program.freeze parses the amount via
	// frozenBalance.sValue().longValueExact() (Program.java:1935), which throws
	// ArithmeticException for an out-of-int64 word -> the freeze is rejected
	// (push 0) with zero state change. uint256ToInt64Exact carries the same
	// reject-when-out-of-range semantics (a negative sValue is < TRX_PRECISION and
	// would be rejected by validate anyway), replacing the old low-64-bit
	// int64(amountWord.Uint64()) truncation that let a huge word freeze its
	// low bits.
	amount, amountOK := uint256ToInt64Exact(&amountWord)
	resourceType := int64(resourceWord.Uint64())
	caller := contract.Address
	receiver := uint256ToAddress(&receiverWord)

	// java EnergyCost.getFreezeCost adds NEW_ACCT_CALL when the receiver word
	// (stack[size-3]) is a dead account. The base FREEZE(20000) is already
	// billed via the jump-table energyCost in the interpreter loop; this adds
	// the dead-receiver surcharge through the same useEnergy penalty path.
	// java's cost function runs for ALL FREEZE invocations under allowTvmFreeze
	// and reads the receiver unconditionally (it does not gate on
	// receiver==caller — for a self-freeze the caller always exists, so it is
	// moot), so charge it here against pre-execution state, before the
	// amount/resourceType/StakingV2 short-circuits and any CreateAccountWithTime.
	if !in.tvm.StateDB.AccountExists(receiver) {
		if !in.useEnergy(contract, EnergyCallNewAcct) {
			return nil, ErrOutOfEnergy
		}
	}

	// !amountOK is evaluated before the `< tvmTRXPrecision` comparison so a
	// rejected (out-of-range) amount never uses a bogus parsed value.
	if in.tvm.cfg.StakingV2 || !amountOK || amount < tvmTRXPrecision || (resourceType != 0 && resourceType != 1) {
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

	// SV-3: java unfreeze() increaseNonce() once up front (Program.java:1956),
	// unconditionally (before validate/execute), feeding a later CREATE address.
	in.tvm.Nonce++

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

	// SV-3: java voteWitness() increaseNonce() once up front (Program.java:2272),
	// after the energy cost is charged but BEFORE the memory length-word check and
	// validate, so the nonce advances even when the vote reverts or pushes 0.
	in.tvm.Nonce++

	// java OperationActions.voteWitnessAction reads the four stack words via
	// DataWord.intValueSafe() (clamps a >4-byte or sign-negative word to
	// Integer.MAX_VALUE), NOT a low-64-bit truncation. A huge witnessCount whose
	// low bits happen to match a small length word must therefore clamp to
	// MAX_VALUE and fail the length-word check below, not slip through as a small
	// count (the A truncation fix).
	n := int64(wordToIntValueSafe(&witnessCountWord))
	amountN := int64(wordToIntValueSafe(&amountCountWord))
	wBase := int64(wordToIntValueSafe(&witnessOffsetWord))
	aBase := int64(wordToIntValueSafe(&amountOffsetWord))

	// SV-4 / java Program.voteWitness order: the memory length-word check runs
	// FIRST (Program.java:2276 — a BytecodeExecutionException / revert on
	// mismatch), BEFORE the witnessArrayLength == amountArrayLength equality
	// (Program.java:2283 — a plain `return false` / push 0). Both the stored
	// length word and the count argument are compared through intValueSafe, so a
	// >4-byte length word also clamps to MAX_VALUE.
	if memoryArrayLengthSafe(mem, wBase) != n || memoryArrayLengthSafe(mem, aBase) != amountN {
		return nil, errVoteWitnessMemoryLength
	}
	if n != amountN {
		stack.push(uint256.NewInt(0))
		return nil, nil
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

	// SV-2: java VoteWitnessProcessor.execute merges votes into a
	// HashMap<ByteString,Long> (VoteWitnessProcessor.java:54) and appends them to
	// the account's `votes` in entrySet() order (:105-108). That order — not the
	// first-seen insertion order we accumulated in voteOrder — is what gets
	// protobuf-serialized into state, so reorder to the java HashMap iteration
	// order before writing or the state root diverges.
	hashOrder := javaHashMapOrder(voteOrder)
	votes := make([]*corepb.Vote, 0, len(hashOrder))
	for _, witnessAddr := range hashOrder {
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

// voteWitnessWordSize = DataWord.WORD_SIZE (32), the per-element stride.
var voteWitnessWordSize = uint256.NewInt(32)

// voteWitnessMemoryLimitBig is java EnergyCost.MEM_LIMIT (3 MB) as a big.Int;
// calcMemEnergy throws OutOfMemory when memNeeded exceeds it.
var voteWitnessMemoryLimitBig = new(big.Int).SetUint64(tvmMemoryLimit)

// voteWitnessMemoryEnergyCost computes the VOTEWITNESS memory-expansion energy,
// faithfully reproducing java EnergyCost's THREE fork variants
// (OperationRegistry gates which one is installed):
//
//   - base getVoteWitnessCost (neither flag): size = count.mul(32) — a single
//     DataWord.mul, wrapping mod 2^256, NO +32. memNeeded short-circuits to 0
//     when the wrapped size is 0.
//   - getVoteWitnessCost2 (#81 allowEnergyAdjustment, CURRENTLY ACTIVE):
//     size = count.mul(32).add(32) — both DataWord ops wrap mod 2^256. This is
//     the divergence fixed here: 2^251*32 ≡ 0, +32 = 32, so a huge witnessCount
//     no longer (wrongly) overflows to OutOfMemory.
//   - getVoteWitnessCost3 (#96 allowTvmOsaka): pure BigInteger, NO wrapping —
//     size = count*32 + 32.
//
// In every variant memNeeded(offset, size) = size==0 ? 0 : offset+size is a
// BigInteger sum (NO wrap), then calcMemEnergy throws OutOfMemory when memNeeded
// > 3 MB. Returns (energyDelta, neededBytes, err); neededBytes <= 3 MB on
// success so it always fits uint64.
func voteWitnessMemoryEnergyCost(in *Interpreter, mem *Memory, witnessOffset, witnessCount, amountOffset, amountCount *uint256.Int) (uint64, uint64, error) {
	wrap := !in.tvmConfig.Osaka                                 // base + #81 use DataWord (wrapping) math
	includeLengthWord := !wrap || in.tvmConfig.EnergyAdjustment // +32 for #81 and #96, not base

	wNeeded := voteWitnessArrayMemNeeded(witnessOffset, witnessCount, wrap, includeLengthWord)
	aNeeded := voteWitnessArrayMemNeeded(amountOffset, amountCount, wrap, includeLengthWord)
	needed := wNeeded
	if aNeeded.Cmp(needed) > 0 {
		needed = aNeeded
	}
	if needed.Cmp(voteWitnessMemoryLimitBig) > 0 {
		return 0, 0, newOutOfMemoryError(VOTEWITNESS)
	}
	neededU64 := needed.Uint64() // safe: needed <= 3 MB
	var oldSize uint64
	if mem != nil {
		oldSize = uint64(mem.len())
	}
	if oldSize >= neededU64 {
		return 0, neededU64, nil
	}
	return memoryEnergyCost(neededU64) - memoryEnergyCost(oldSize), neededU64, nil
}

// voteWitnessArrayMemNeeded returns java memNeeded(offset, size) for one array.
// When wrap is true the size word is built with DataWord (uint256, mod 2^256)
// arithmetic; otherwise with non-wrapping big.Int (the Osaka cost3 path).
// includeLengthWord adds the 32-byte dynamic-array length word (#81/#96).
func voteWitnessArrayMemNeeded(offset, count *uint256.Int, wrap, includeLengthWord bool) *big.Int {
	var size *big.Int
	if wrap {
		// DataWord.mul(32) [+ DataWord.add(32)], each mod 2^256.
		w := new(uint256.Int).Mul(count, voteWitnessWordSize)
		if includeLengthWord {
			w.Add(w, voteWitnessWordSize)
		}
		size = w.ToBig()
	} else {
		// Pure BigInteger: count*32 + 32 (Osaka cost3).
		size = new(big.Int).Mul(count.ToBig(), big.NewInt(32))
		if includeLengthWord {
			size.Add(size, big.NewInt(32))
		}
	}
	// memNeeded: size==0 -> 0, else offset + size (BigInteger, no wrap).
	if size.Sign() == 0 {
		return new(big.Int)
	}
	return new(big.Int).Add(offset.ToBig(), size)
}

// wordToIntValueSafe mirrors java DataWord.intValueSafe() (DataWord.java:222-228):
// if the word occupies more than 4 bytes, or its low-32-bit signed int is
// negative (high bit of byte[28] set), it clamps to Integer.MAX_VALUE
// (0x7FFFFFFF); otherwise it returns the low 32 bits as a non-negative int. This
// is how OperationActions.voteWitnessAction reads the four VOTEWITNESS stack
// words, so a >4-byte count/offset is bounded, never truncated to its low bits.
func wordToIntValueSafe(v *uint256.Int) int32 {
	b := v.Bytes32()
	// bytesOccupied: a non-zero byte anywhere in bytes[0..27] means the value
	// needs more than 4 bytes. Also matches the java check that the low 32-bit
	// int is negative (byte[28] high bit set), since that flips intValue() < 0.
	for i := 0; i < 28; i++ {
		if b[i] != 0 {
			return 0x7FFFFFFF
		}
	}
	v32 := binary.BigEndian.Uint32(b[28:])
	if v32 > 0x7FFFFFFF {
		return 0x7FFFFFFF
	}
	return int32(v32)
}

// memoryArrayLengthSafe reads the 32-byte dynamic-array length word at `offset`
// and applies java DataWord.intValueSafe() to it (java Program.voteWitness:2276
// reads `memoryLoad(offset).intValueSafe()`). It returns -1 — a sentinel that can
// never equal a non-negative intValueSafe count — when the word lies outside the
// (already energy-charged, pre-resized) memory, preserving go-tron's existing
// requirement that the length word be in allocated memory (locked by
// TestVoteWitnessOpcodeMemoryEnergyCostFollowsJavaForks).
func memoryArrayLengthSafe(mem *Memory, offset int64) int64 {
	if mem == nil || offset < 0 || offset+32 > int64(mem.len()) {
		return -1
	}
	var w uint256.Int
	w.SetBytes(mem.getCopy(offset, 32))
	return int64(wordToIntValueSafe(&w))
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

// tvmAddResourceWeight applies a staking-opcode weight delta through the
// journaled StateDB path so a later frame revert rolls it back — java's freeze
// opcode mutates a discardable Repository, so its total_*_weight delta is
// dropped on revert; gtron must journal the delta or it leaks (over-counting
// total_energy_weight on a freeze-opcode-then-revert).
func tvmAddResourceWeight(tvm *TVM, resource corepb.ResourceCode, delta int64) {
	tvm.StateDB.AddResourceWeightJournaled(tvm.DynProps, resource, delta)
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
	// SV-3: java withdrawReward() increaseNonce() once up front (Program.java:2332),
	// unconditionally (before validate/execute), so the nonce advances even on the
	// genesis-witness / no-reward push-0 paths.
	in.tvm.Nonce++
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
	resourceWord := stack.pop() // java pops resourceType before the amount (OperationActions.*V2Action / *ResourceAction)
	amountWord := stack.pop()
	// A (truncation): java Program.freezeBalanceV2 parses the amount via
	// frozenBalance.sValue().longValueExact() (Program.java:2029) -> an
	// out-of-int64 word throws ArithmeticException and the freeze is rejected
	// (push 0, no state change). uint256ToInt64Exact carries the same semantics,
	// replacing the old low-64-bit int64(amountWord.Uint64()) truncation.
	amount, amountOK := uint256ToInt64Exact(&amountWord)
	resource := corepb.ResourceCode(int32(resourceWord.Uint64()))
	caller := contract.Address

	// SV-3: java freezeBalanceV2() increaseNonce() once up front
	// (Program.java:2020), unconditionally, feeding a later CREATE address.
	in.tvm.Nonce++

	if !amountOK || amount < tvmTRXPrecision || !validTVMStakeV2Resource(in.tvm, resource) {
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
	resourceWord := stack.pop() // java pops resourceType before the amount (OperationActions.*V2Action / *ResourceAction)
	amountWord := stack.pop()
	// A (truncation): java Program.unfreezeBalanceV2 parses the amount via
	// unfreezeBalance.sValue().longValueExact() (Program.java:2059) -> an
	// out-of-int64 word throws ArithmeticException and the unfreeze is rejected
	// (push 0, no state change). uint256ToInt64Exact carries the same semantics,
	// replacing the old low-64-bit int64(amountWord.Uint64()) truncation.
	amount, amountOK := uint256ToInt64Exact(&amountWord)
	resource := corepb.ResourceCode(int32(resourceWord.Uint64()))
	caller := contract.Address
	// java UnfreezeBalanceV2Processor uses getLatestBlockHeaderTimestamp() for the
	// unfreezing-count gate, the expired-withdrawal sweep, and the new entry's
	// expire time — the chain head's timestamp, NOT tvm.Timestamp (current block).
	now := tvmLatestBlockHeaderTimestamp(in.tvm)

	// SV-3: java unfreezeBalanceV2() increaseNonce() once up front
	// (Program.java:2051), unconditionally (before validate/execute).
	in.tvm.Nonce++

	if !amountOK || amount <= 0 || !validTVMStakeV2Resource(in.tvm, resource) || tvmUnfreezingV2Count(in.tvm.StateDB.GetAccount(caller), now) >= 32 {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	frozen := in.tvm.StateDB.GetFrozenV2Amount(caller, resource)
	if amount > frozen {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	tvmWithdrawReward(in.tvm, caller)
	withdrawnExpired := in.tvm.StateDB.RemoveExpiredUnfreezeV2(caller, now)
	if withdrawnExpired > 0 {
		in.tvm.StateDB.AddBalance(caller, withdrawnExpired)
		// SV-3: java increaseNonce() a SECOND time when execute() returns an
		// expired-withdrawal balance > 0 (the withdrawExpireUnfreezeWhileUnfreezing
		// internalTx, Program.java:2066-2069).
		in.tvm.Nonce++
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
	expireMs := now + delayDays*86400_000
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
	// java CancelAllUnfreezeV2Processor.execute uses now =
	// getLatestBlockHeaderTimestamp() (the chain head's timestamp, the block
	// BEFORE the one being applied) to split expired vs unexpired entries — the
	// same source as the actuator's ctx.PrevBlockTime and the rest of the VM
	// stake code (tvm.go suicide path). NOT tvm.Timestamp, which is the current
	// block's TIMESTAMP-opcode value.
	var now int64
	if in.tvm.DynProps != nil {
		now = in.tvm.DynProps.LatestBlockHeaderTimestamp()
	}
	// SV-3: java cancelAllUnfreezeV2Action() increaseNonce() once up front
	// (Program.java:2118), unconditionally (before validate/execute).
	in.tvm.Nonce++
	// CancelAllUnfreezeV2 refreezes unexpired entries (updating global weight)
	// and returns the expired total, which java/actuator add to the balance.
	expired := in.tvm.StateDB.CancelAllUnfreezeV2(contract.Address, now)
	if expired > 0 {
		in.tvm.StateDB.AddBalance(contract.Address, expired)
		// SV-3: java increaseNonce() a SECOND time when the WITHDRAW_EXPIRE_BALANCE
		// result is > 0 (the withdrawExpireUnfreezeWhileCanceling internalTx,
		// Program.java:2131-2134).
		in.tvm.Nonce++
	}
	// java OperationActions.cancelAllUnfreezeV2Action pushes ONE/ZERO for
	// success/failure (Program.cancelAllUnfreezeV2Action returns boolean), NOT
	// the cancelled amount. The processor succeeds whenever the account exists,
	// which is guaranteed here.
	stack.push(uint256.NewInt(1))
	return nil, nil
}

// ── 0xDD WITHDRAWEXPIREUNFREEZE ───────────────────────────────────────────────
// Stack: → withdrawnAmount

func opWithdrawExpireUnfreeze(_ *uint64, in *Interpreter, contract *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	caller := contract.Address
	// SV-3: java withdrawExpireUnfreeze() increaseNonce() once up front
	// (Program.java:2087), unconditionally (before validate/execute).
	in.tvm.Nonce++
	released := in.tvm.StateDB.RemoveExpiredUnfreezeV2(caller, tvmLatestBlockHeaderTimestamp(in.tvm))
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
	resourceWord := stack.pop() // java pops resourceType before the amount (OperationActions.*V2Action / *ResourceAction)
	amountWord := stack.pop()
	receiverWord := stack.pop()
	// SV-3: java delegateResource() increaseNonce() once up front
	// (Program.java:2162), unconditionally (before validate/execute), so the nonce
	// advances even when the amount fails validation below.
	in.tvm.Nonce++
	amount, ok := uint256ToInt64Exact(&amountWord)
	if !ok || amount < tvmTRXPrecision {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	resource := corepb.ResourceCode(int32(resourceWord.Uint64()))
	receiver := uint256ToAddress(&receiverWord)
	caller := contract.Address

	// java DelegateResourceProcessor.validate checks the USAGE-ADJUSTED available
	// balance (frozenV2ForResource − v2Usage), not the raw frozen amount — the
	// same value the getDelegatableResource precompile returns. go previously
	// compared against raw frozen, so a contract that had consumed resource could
	// delegate more than java permits. delegatableFrozenV2 mirrors java
	// FreezeV2Util.queryDelegatableResource.
	if amount > delegatableFrozenV2(in.tvm, caller, resource) {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	// java DelegateResourceProcessor.validate rejects (→ push 0) when the
	// receiver is the owner itself, does not exist, or is a contract account
	// ("Do not allow delegate resources to contract addresses"). go skipped
	// these, so a contract delegating to a non-existent/self/contract receiver
	// succeeded where java reverts — Nile 34,212,851 delegated to a non-existent
	// account (expected REVERT, got SUCCESS). Use the account type, mirroring
	// java receiverCapsule.getType() == Contract.
	recvAccount := in.tvm.StateDB.GetAccount(receiver)
	if receiver == caller || recvAccount == nil || recvAccount.Type() == corepb.AccountType_Contract {
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

	// Persist the per-pair DelegatedResourceV2 record + owner delegation index,
	// identical to actuator DelegateResourceActuator.Execute and java
	// DelegateResourceProcessor.delegateResource. The VM native contract always
	// writes the UNLOCKED record (java createDbKeyV2(owner, receiver, false) —
	// the opcode has no lock parameter), so without this the per-pair state that
	// UNDELEGATERESOURCE / getDelegatedResource read back would be absent.
	if err := tvmWriteDelegateRecord(in.tvm.StateDB, caller, receiver, resource, amount); err != nil {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	stack.push(uint256.NewInt(1))
	return nil, nil
}

// tvmWriteDelegateRecord mirrors actuator DelegateResourceActuator.Execute's
// per-pair record + index bookkeeping (java DelegateResourceProcessor.
// delegateResource): add `amount` to the owner→receiver UNLOCKED
// DelegatedResourceV2 record and register the receiver in the owner's
// delegation index. Aggregate balances are updated by the caller.
func tvmWriteDelegateRecord(sdb *state.StateDB, owner, receiver tcommon.Address, resource corepb.ResourceCode, amount int64) error {
	dr := sdb.ReadDelegatedResourceV2(owner, receiver, false)
	if dr == nil {
		dr = &rawdb.DelegatedResource{From: owner, To: receiver}
	}
	if resource == corepb.ResourceCode_BANDWIDTH {
		dr.FrozenBalanceForBandwidth += amount
	} else {
		dr.FrozenBalanceForEnergy += amount
	}
	if err := sdb.WriteDelegatedResourceV2(owner, receiver, false, dr); err != nil {
		return err
	}
	receivers := sdb.ReadDelegationIndex(owner)
	for _, r := range receivers {
		if r == receiver {
			return nil
		}
	}
	return sdb.WriteDelegationIndex(owner, append(receivers, receiver))
}

// ── 0xDF UNDELEGATERESOURCE ───────────────────────────────────────────────────
// Stack: amount, resourceType, receiverAddr → success

func opUnDelegateResource(_ *uint64, in *Interpreter, contract *Contract, _ *Memory, stack *Stack) ([]byte, error) {
	resourceWord := stack.pop() // java pops resourceType before the amount (OperationActions.*V2Action / *ResourceAction)
	amountWord := stack.pop()
	receiverWord := stack.pop()
	// SV-3: java unDelegateResource() increaseNonce() once up front
	// (Program.java:2196), unconditionally (before validate/execute).
	in.tvm.Nonce++
	amount, ok := uint256ToInt64Exact(&amountWord)
	if !ok || amount <= 0 {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	resource := corepb.ResourceCode(int32(resourceWord.Uint64()))
	receiver := uint256ToAddress(&receiverWord)
	caller := contract.Address

	// Validate against the per-pair UNLOCKED DelegatedResourceV2 record, NOT the
	// aggregate delegated balance. Mirrors java UnDelegateResourceProcessor.
	// validate (reads createDbKeyV2(owner, receiver, false), rejects when the
	// record is absent or its frozen balance < amount) and actuator
	// UnDelegateResourceActuator. The old aggregate check let an owner undelegate
	// from a receiver it never delegated to (cross-receiver leak) as long as it
	// had delegated the amount to some other receiver.
	dr := in.tvm.StateDB.ReadDelegatedResourceV2(caller, receiver, false)
	if dr == nil {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	var recorded int64
	if resource == corepb.ResourceCode_BANDWIDTH {
		recorded = dr.FrozenBalanceForBandwidth
	} else {
		recorded = dr.FrozenBalanceForEnergy
	}
	if amount > recorded {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	// Before balances shift, transfer the receiver's proportional usage
	// back to the owner. Mirrors java-tron UnDelegateResourceActuator.execute.
	// Use the wired dp (tvm.DynProps); the production StateDB dp is the empty
	// genesis default (same class as the dynamic-energy / getChainParameter fix).
	dp := stakingDynamicProperties(in.tvm)
	resourceTime := in.tvm.ResourceTime()
	transfer, recvRawWindow, recvOptimized := delegation.TransferUsageFromReceiver(in.tvm.StateDB, dp, receiver, resource, amount, resourceTime)
	in.tvm.StateDB.SubDelegatedFrozenV2(caller, resource, amount)
	in.tvm.StateDB.SubAcquiredDelegatedFrozenV2(receiver, resource, amount)
	in.tvm.StateDB.AddFreezeV2(caller, resource, amount)
	// java unDelegateIncrease runs UNCONDITIONALLY and blends the receiver window.
	delegation.FoldUsageIntoOwner(in.tvm.StateDB, dp, caller, resource, transfer, recvRawWindow, recvOptimized, resourceTime)

	// Decrement the per-pair record; delete record + index when fully drained.
	// Mirrors actuator UnDelegateResourceActuator.Execute / java
	// UnDelegateResourceProcessor.execute.
	if err := tvmReduceDelegateRecord(in.tvm.StateDB, caller, receiver, resource, amount); err != nil {
		stack.push(uint256.NewInt(0))
		return nil, nil
	}
	stack.push(uint256.NewInt(1))
	return nil, nil
}

// tvmReduceDelegateRecord subtracts `amount` from the owner→receiver UNLOCKED
// DelegatedResourceV2 record. When both resource legs reach zero the record and
// the owner's delegation-index entry are removed. Mirrors actuator
// UnDelegateResourceActuator.Execute and java UnDelegateResourceProcessor.
func tvmReduceDelegateRecord(sdb *state.StateDB, owner, receiver tcommon.Address, resource corepb.ResourceCode, amount int64) error {
	dr := sdb.ReadDelegatedResourceV2(owner, receiver, false)
	if dr == nil {
		return nil
	}
	if resource == corepb.ResourceCode_BANDWIDTH {
		dr.FrozenBalanceForBandwidth -= amount
	} else {
		dr.FrozenBalanceForEnergy -= amount
	}
	if dr.FrozenBalanceForBandwidth <= 0 && dr.FrozenBalanceForEnergy <= 0 {
		if err := sdb.DeleteDelegatedResourceV2(owner, receiver, false); err != nil {
			return err
		}
	} else if err := sdb.WriteDelegatedResourceV2(owner, receiver, false, dr); err != nil {
		return err
	}
	// Remove the index entry only when no record remains for this pair.
	if sdb.ReadDelegatedResourceV2(owner, receiver, false) == nil &&
		sdb.ReadDelegatedResourceV2(owner, receiver, true) == nil {
		receivers := sdb.ReadDelegationIndex(owner)
		out := receivers[:0]
		for _, r := range receivers {
			if r != receiver {
				out = append(out, r)
			}
		}
		return sdb.WriteDelegationIndex(owner, out)
	}
	return nil
}

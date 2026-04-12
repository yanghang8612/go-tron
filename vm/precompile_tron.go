package vm

import (
	"crypto/sha256"
	"errors"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

const (
	tronPrecompileWordSize = 32
	maxBatchSignSize       = 16
	trxPrecision           = 1_000_000 // SUN per TRX
)

// ── Signature helpers ─────────────────────────────────────────────────────────

// recoverTronAddr recovers the TRON address from a 65-byte secp256k1 signature.
// sig must be [r(32) | s(32) | v(1)] where v is 0/1 (or 27/28).
// Returns the zero address on failure.
func recoverTronAddr(sig, hash []byte) tcommon.Address {
	if len(sig) < 65 {
		return tcommon.Address{}
	}
	s := make([]byte, 65)
	copy(s, sig[:65])
	v := s[64]
	if v >= 27 {
		v -= 27
	}
	if v > 1 {
		return tcommon.Address{}
	}
	s[64] = v

	pubBytes, err := ethcrypto.Ecrecover(hash, s)
	if err != nil || len(pubBytes) != 65 {
		return tcommon.Address{}
	}
	pubHash := ethcrypto.Keccak256(pubBytes[1:])
	var addr tcommon.Address
	addr[0] = 0x41
	copy(addr[1:], pubHash[12:]) // last 20 bytes of keccak
	return addr
}

// ── ABI decoding helpers ──────────────────────────────────────────────────────

// parseBytesArray decodes a `bytes[]` ABI-encoded at byteOffset within data.
// The encoding follows standard Solidity ABI: length word, then per-element
// relative offsets, then element (length, data) pairs.
func parseBytesArray(data []byte, byteOffset int) [][]byte {
	if byteOffset+32 > len(data) {
		return nil
	}
	count := int(parseUint64FromWord(data, byteOffset))
	if count <= 0 || count > 256 {
		return nil
	}
	result := make([][]byte, count)
	for i := 0; i < count; i++ {
		// relative offset (in bytes) to element i, from position byteOffset+32
		relOff := int(parseUint64FromWord(data, byteOffset+32*(1+i)))
		// absolute position of element's length word
		lenPos := byteOffset + 32 + relOff
		if lenPos+32 > len(data) {
			result[i] = nil
			continue
		}
		elemLen := int(parseUint64FromWord(data, lenPos))
		dataPos := lenPos + 32
		elem := make([]byte, elemLen)
		if dataPos < len(data) {
			copy(elem, data[dataPos:])
		}
		result[i] = elem
	}
	return result
}

// parseAddressArray decodes an `address[]` ABI-encoded at byteOffset within data.
// Each address is a 32-byte word with the last 20 bytes containing the address body.
func parseAddressArray(data []byte, byteOffset int) []tcommon.Address {
	if byteOffset+32 > len(data) {
		return nil
	}
	count := int(parseUint64FromWord(data, byteOffset))
	if count <= 0 || count > 256 {
		return nil
	}
	result := make([]tcommon.Address, count)
	for i := 0; i < count; i++ {
		word := parseWord32(data, byteOffset+32*(1+i))
		result[i] = tronAddrFromWord(word)
	}
	return result
}

// ── 0x09 BatchValidateSign ────────────────────────────────────────────────────

type batchValidateSign struct{}

func (c *batchValidateSign) Run(_ *EVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	// Energy: 1500 per signature (ceil of full count from input length).
	// Formula: (words-5)/6 signatures, each costs 1500.
	words := len(input) / 32
	sigCount := 0
	if words > 5 {
		sigCount = (words - 5) / 6
	}
	cost := uint64(1500 * sigCount)
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	ret := c.execute(input)
	return ret, cost, nil
}

func (c *batchValidateSign) execute(input []byte) []byte {
	if len(input) < 96 {
		return make([]byte, 32)
	}

	// word[0]: hash
	hash := parseWord32(input, 0)

	// word[1]: byte offset to sigs array
	sigsOffset := int(parseUint64FromWord(input, 32))
	// word[2]: byte offset to addrs array
	addrsOffset := int(parseUint64FromWord(input, 64))

	sigs := parseBytesArray(input, sigsOffset)
	addrs := parseAddressArray(input, addrsOffset)

	if len(sigs) == 0 || len(sigs) > maxBatchSignSize || len(sigs) != len(addrs) {
		return make([]byte, 32)
	}

	result := make([]byte, 32)
	for i, sig := range sigs {
		recovered := recoverTronAddr(sig, hash)
		if recovered == addrs[i] {
			result[i] = 1
		}
	}
	return result
}

// ── 0x0a ValidateMultiSign ───────────────────────────────────────────────────

type validateMultiSign struct{}

func (c *validateMultiSign) Run(evm *EVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	words := len(input) / 32
	sigCount := 0
	if words > 5 {
		sigCount = (words - 5) / 5
	}
	cost := uint64(1500 * sigCount)
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}

	ret := c.execute(evm, input)
	return ret, cost, nil
}

func (c *validateMultiSign) execute(evm *EVM, input []byte) []byte {
	falseResult := make([]byte, 32)
	trueResult := make([]byte, 32)
	trueResult[31] = 1

	if len(input) < 128 {
		return falseResult
	}

	// word[0]: owner address
	ownerAddr := tronAddrFromWord(parseWord32(input, 0))
	// word[1]: permission ID
	permID := int(parseInt64FromWord(input, 32))
	// word[2]: 32-byte message data (to be included in the signed hash)
	msgData := parseWord32(input, 64)
	// word[3]: byte offset to sigs array
	sigsOffset := int(parseUint64FromWord(input, 96))

	// Compute what was signed: SHA256(ownerAddr(21) || permID(4 BE) || msgData(32))
	var combine [21 + 4 + 32]byte
	copy(combine[0:21], ownerAddr[:])
	combine[21] = byte(permID >> 24)
	combine[22] = byte(permID >> 16)
	combine[23] = byte(permID >> 8)
	combine[24] = byte(permID)
	copy(combine[25:57], msgData)
	hash := sha256.Sum256(combine[:])

	sigs := parseBytesArray(input, sigsOffset)
	if len(sigs) == 0 || len(sigs) > maxBatchSignSize {
		return falseResult
	}

	acc := evm.StateDB.GetAccount(ownerAddr)
	if acc == nil {
		return falseResult
	}

	perm := permissionByID(acc, permID)
	if perm == nil {
		return falseResult
	}

	var totalWeight int64
	seen := make(map[tcommon.Address]bool)
	for _, sig := range sigs {
		recovered := recoverTronAddr(sig, hash[:])
		if recovered == (tcommon.Address{}) {
			return falseResult
		}
		if seen[recovered] {
			continue
		}
		weight := permissionWeight(perm, recovered)
		if weight == 0 {
			return falseResult // wrong signer
		}
		totalWeight += weight
		seen[recovered] = true
	}

	if totalWeight >= perm.GetThreshold() {
		return trueResult
	}
	return falseResult
}

// permissionByID returns the permission with the given ID from the account.
func permissionByID(acc interface{ OwnerPermission() *corepb.Permission; WitnessPermission() *corepb.Permission; ActivePermission() []*corepb.Permission }, id int) *corepb.Permission {
	switch id {
	case 0:
		return acc.OwnerPermission()
	case 1:
		return acc.WitnessPermission()
	default:
		for _, p := range acc.ActivePermission() {
			if int(p.GetId()) == id {
				return p
			}
		}
	}
	return nil
}

// permissionWeight returns the weight of addr in the permission's key list, or 0.
func permissionWeight(perm *corepb.Permission, addr tcommon.Address) int64 {
	for _, key := range perm.GetKeys() {
		if tcommon.BytesToAddress(key.GetAddress()) == addr {
			return key.GetWeight()
		}
	}
	return 0
}

// ── 0x01000001–0x01000004 Shielded token stubs ───────────────────────────────

var errShieldedNotImplemented = errors.New("shielded token precompiles not implemented")

type shieldedStub struct{}

func (c *shieldedStub) Run(_ *EVM, _ tcommon.Address, _ []byte, energy uint64) ([]byte, uint64, error) {
	// Energy cost: most expensive shielded op is VerifyTransferProof (200000).
	// Use 200000 as a safe upper bound for any shielded stub.
	const cost = 200000
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	return nil, cost, errShieldedNotImplemented
}

// ── 0x01000005 RewardBalance ──────────────────────────────────────────────────

type rewardBalance struct{}

func (c *rewardBalance) Run(evm *EVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 20
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 32 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input)
	bal := evm.StateDB.GetAllowance(addr)
	return int64ToBytes32(bal), cost, nil
}

// ── 0x01000006 IsSrCandidate ──────────────────────────────────────────────────

type isSrCandidate struct{}

func (c *isSrCandidate) Run(evm *EVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 20
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 32 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input)
	result := make([]byte, 32)
	if evm.StateDB.GetWitness(addr) != nil {
		result[31] = 1
	}
	return result, cost, nil
}

// ── 0x01000007 VoteCount ──────────────────────────────────────────────────────

type voteCount struct{}

func (c *voteCount) Run(evm *EVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 500
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 64 {
		return make([]byte, 32), cost, nil
	}
	ownerAddr := tronAddrFromWord(input[0:32])
	srAddr := tronAddrFromWord(input[32:64])

	var count int64
	for _, vote := range evm.StateDB.GetVotes(ownerAddr) {
		if tcommon.BytesToAddress(vote.GetVoteAddress()) == srAddr {
			count += vote.GetVoteCount()
		}
	}
	return int64ToBytes32(count), cost, nil
}

// ── 0x01000008 UsedVoteCount ──────────────────────────────────────────────────

type usedVoteCount struct{}

func (c *usedVoteCount) Run(evm *EVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 20
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 32 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input)
	var total int64
	for _, vote := range evm.StateDB.GetVotes(addr) {
		total += vote.GetVoteCount()
	}
	return int64ToBytes32(total), cost, nil
}

// ── 0x01000009 ReceivedVoteCount ─────────────────────────────────────────────

type receivedVoteCount struct{}

func (c *receivedVoteCount) Run(evm *EVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 20
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 32 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input)
	var count int64
	if w := evm.StateDB.GetWitness(addr); w != nil {
		count = w.VoteCount()
	}
	return int64ToBytes32(count), cost, nil
}

// ── 0x0100000a TotalVoteCount ─────────────────────────────────────────────────

type totalVoteCount struct{}

func (c *totalVoteCount) Run(evm *EVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 20
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 32 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input)
	// Tron Power = total frozen V2 / TRX_PRECISION (1 TP per 1 TRX frozen)
	tp := evm.StateDB.TotalFrozenV2(addr) / trxPrecision
	return int64ToBytes32(tp), cost, nil
}

// ── 0x0100000b GetChainParameter ─────────────────────────────────────────────

type getChainParameter struct{}

func (c *getChainParameter) Run(evm *EVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 32 {
		return make([]byte, 32), cost, nil
	}
	code := parseInt64FromWord(input, 0)
	dp := evm.StateDB.DynamicProperties()
	var val int64
	switch code {
	case 1: // TOTAL_NET_LIMIT
		val = dp.TotalNetLimit()
	case 3: // TOTAL_ENERGY_CURRENT_LIMIT
		val = dp.TotalEnergyCurrentLimit()
	case 5: // UNFREEZE_DELAY_DAYS
		val = dp.UnfreezeDelayDays()
	default:
		val = 0
	}
	return int64ToBytes32(val), cost, nil
}

// ── 0x0100000c AvailableUnfreezeV2Size ────────────────────────────────────────

const maxUnfreezeV2 = 32

type availableUnfreezeV2Size struct{}

func (c *availableUnfreezeV2Size) Run(evm *EVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 32 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input)
	used := evm.StateDB.UnfreezeV2Count(addr)
	available := int64(maxUnfreezeV2 - used)
	if available < 0 {
		available = 0
	}
	return int64ToBytes32(available), cost, nil
}

// ── 0x0100000d UnfreezableBalanceV2 ──────────────────────────────────────────

type unfreezableBalanceV2 struct{}

func (c *unfreezableBalanceV2) Run(evm *EVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 64 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input[0:32])
	resType := resourceCodeFromInt(parseInt64FromWord(input, 32))
	bal := evm.StateDB.GetFrozenV2Amount(addr, resType)
	return int64ToBytes32(bal), cost, nil
}

// ── 0x0100000e ExpireUnfreezeBalanceV2 ────────────────────────────────────────

type expireUnfreezeBalanceV2 struct{}

func (c *expireUnfreezeBalanceV2) Run(evm *EVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 64 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input[0:32])
	// Input time is in seconds; java-tron converts to milliseconds (* 1000).
	timeSec := parseInt64FromWord(input, 32)
	if timeSec < 0 {
		return make([]byte, 32), cost, nil
	}
	var timeMs int64
	if timeSec >= (1<<62)/1000 {
		timeMs = 1<<63 - 1 // effectively max
	} else {
		timeMs = timeSec * 1000
	}

	acc := evm.StateDB.GetAccount(addr)
	if acc == nil {
		return make([]byte, 32), cost, nil
	}

	var total int64
	for _, u := range acc.UnfrozenV2() {
		if u.GetUnfreezeExpireTime() <= timeMs {
			total += u.GetUnfreezeAmount()
		}
	}
	return int64ToBytes32(total), cost, nil
}

// ── 0x0100000f DelegatableResource ───────────────────────────────────────────

type delegatableResource struct{}

func (c *delegatableResource) Run(evm *EVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 64 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input[0:32])
	resType := resourceCodeFromInt(parseInt64FromWord(input, 32))

	frozen := evm.StateDB.GetFrozenV2Amount(addr, resType)
	delegatedOut := delegatedFrozenV2Out(evm, addr, resType)
	result := frozen - delegatedOut
	if result < 0 {
		result = 0
	}
	return int64ToBytes32(result), cost, nil
}

// ── 0x01000010 ResourceV2 ─────────────────────────────────────────────────────

type resourceV2 struct{}

func (c *resourceV2) Run(evm *EVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 96 {
		return make([]byte, 32), cost, nil
	}
	target := tronAddrFromWord(input[0:32])
	from := tronAddrFromWord(input[32:64])
	resType := resourceCodeFromInt(parseInt64FromWord(input, 64))

	var balance int64
	if from == target {
		// Same account: return unfrozen balance for this type
		balance = evm.StateDB.GetFrozenV2Amount(from, resType)
	} else {
		// Cross-account delegation: we don't track per-pair delegation records.
		// Return 0 — callers that need this data should use on-chain stores.
		balance = 0
	}
	return int64ToBytes32(balance), cost, nil
}

// ── 0x01000011 CheckUnDelegateResource ───────────────────────────────────────

type checkUnDelegateResource struct{}

func (c *checkUnDelegateResource) Run(_ *EVM, _ tcommon.Address, _ []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	// Returns (locked, limit, penalty) — stub returns (0, 0, 0).
	return make([]byte, 96), cost, nil
}

// ── 0x01000012 ResourceUsage ──────────────────────────────────────────────────

type resourceUsage struct{}

func (c *resourceUsage) Run(_ *EVM, _ tcommon.Address, _ []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	// Returns (usage, limit) — stub returns (0, 0).
	return make([]byte, 64), cost, nil
}

// ── 0x01000013 TotalResource ──────────────────────────────────────────────────

type totalResource struct{}

func (c *totalResource) Run(evm *EVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 64 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input[0:32])
	resType := resourceCodeFromInt(parseInt64FromWord(input, 32))

	// Total = own frozen + acquired delegations
	frozen := evm.StateDB.GetFrozenV2Amount(addr, resType)
	acquired := acquiredDelegatedV2(evm, addr, resType)
	return int64ToBytes32(frozen + acquired), cost, nil
}

// ── 0x01000014 TotalDelegatedResource ────────────────────────────────────────

type totalDelegatedResource struct{}

func (c *totalDelegatedResource) Run(evm *EVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 64 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input[0:32])
	resType := resourceCodeFromInt(parseInt64FromWord(input, 32))
	return int64ToBytes32(delegatedFrozenV2Out(evm, addr, resType)), cost, nil
}

// ── 0x01000015 TotalAcquiredResource ─────────────────────────────────────────

type totalAcquiredResource struct{}

func (c *totalAcquiredResource) Run(evm *EVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 64 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input[0:32])
	resType := resourceCodeFromInt(parseInt64FromWord(input, 32))
	return int64ToBytes32(acquiredDelegatedV2(evm, addr, resType)), cost, nil
}

// ── Resource helpers ──────────────────────────────────────────────────────────

func resourceCodeFromInt(v int64) corepb.ResourceCode {
	switch v {
	case 0:
		return corepb.ResourceCode_BANDWIDTH
	case 1:
		return corepb.ResourceCode_ENERGY
	default:
		return corepb.ResourceCode_BANDWIDTH
	}
}

// delegatedFrozenV2Out returns how much of resType the account has delegated out.
func delegatedFrozenV2Out(evm *EVM, addr tcommon.Address, resType corepb.ResourceCode) int64 {
	acc := evm.StateDB.GetAccount(addr)
	if acc == nil {
		return 0
	}
	switch resType {
	case corepb.ResourceCode_BANDWIDTH:
		return acc.DelegatedFrozenV2BalanceForBandwidth()
	case corepb.ResourceCode_ENERGY:
		return acc.DelegatedFrozenV2BalanceForEnergy()
	}
	return 0
}

// acquiredDelegatedV2 returns how much of resType others have delegated to addr.
func acquiredDelegatedV2(evm *EVM, addr tcommon.Address, resType corepb.ResourceCode) int64 {
	acc := evm.StateDB.GetAccount(addr)
	if acc == nil {
		return 0
	}
	switch resType {
	case corepb.ResourceCode_BANDWIDTH:
		return acc.AcquiredDelegatedFrozenV2BalanceForBandwidth()
	case corepb.ResourceCode_ENERGY:
		return acc.AcquiredDelegatedFrozenV2BalanceForEnergy()
	}
	return 0
}

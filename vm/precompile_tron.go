package vm

import (
	"crypto/sha256"
	"fmt"
	"math"
	"math/big"
	"os"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/core/zksnark"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

const (
	tronPrecompileWordSize = 32
	maxBatchSignSize       = 16
	maxMultiSignSize       = 5
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

func parseFixed65SigArray(data []byte, byteOffset int) [][]byte {
	if byteOffset+32 > len(data) {
		return nil
	}
	count := int(parseUint64FromWord(data, byteOffset))
	if count <= 0 || count > 256 {
		return nil
	}
	result := make([][]byte, count)
	for i := 0; i < count; i++ {
		relOff := int(parseUint64FromWord(data, byteOffset+32*(1+i)))
		dataPos := byteOffset + relOff + 64
		sig := make([]byte, 65)
		if dataPos < len(data) {
			copy(sig, data[dataPos:])
		}
		result[i] = sig
	}
	return result
}

func validAbiEncoding(data []byte, headerWords, itemWords int) bool {
	if len(data) == 0 || len(data)%tronPrecompileWordSize != 0 {
		return false
	}
	tail := len(data) - headerWords*tronPrecompileWordSize
	return tail > 0 && tail%(itemWords*tronPrecompileWordSize) == 0
}

// ── 0x09 BatchValidateSign ────────────────────────────────────────────────────

type batchValidateSign struct{}

func (c *batchValidateSign) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	ret, used, _, err := c.RunWithStatus(tvm, tcommon.Address{}, input, energy)
	return ret, used, err
}

func (c *batchValidateSign) RunWithStatus(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, bool, error) {
	// Energy: 1500 per signature (ceil of full count from input length).
	// Formula: (words-5)/6 signatures, each costs 1500.
	words := len(input) / 32
	sigCount := 0
	if words > 5 {
		sigCount = (words - 5) / 6
	}
	cost := uint64(1500 * sigCount)
	if energy < cost {
		return nil, energy, false, ErrOutOfEnergy
	}
	ret, success := c.executeWithStatus(tvm, input)
	return ret, cost, success, nil
}

func (c *batchValidateSign) execute(tvm *TVM, input []byte) []byte {
	ret, _ := c.executeWithStatus(tvm, input)
	return ret
}

func (c *batchValidateSign) executeWithStatus(tvm *TVM, input []byte) ([]byte, bool) {
	if len(input) < 96 {
		return make([]byte, 32), true
	}
	if tvm != nil && tvm.cfg.Osaka && !validAbiEncoding(input, 5, 6) {
		return nil, false
	}

	// word[0]: hash
	hash := parseWord32(input, 0)

	// word[1]: byte offset to sigs array
	sigsOffset := int(parseUint64FromWord(input, 32))
	// word[2]: byte offset to addrs array
	addrsOffset := int(parseUint64FromWord(input, 64))

	var sigs [][]byte
	if tvm != nil && tvm.cfg.SelfdestructRestrict {
		if int(parseUint64FromWord(input, sigsOffset)) > maxBatchSignSize ||
			int(parseUint64FromWord(input, addrsOffset)) > maxBatchSignSize {
			return make([]byte, 32), true
		}
		sigs = parseFixed65SigArray(input, sigsOffset)
	} else {
		sigs = parseBytesArray(input, sigsOffset)
	}
	addrs := parseAddressArray(input, addrsOffset)

	if len(sigs) == 0 || len(sigs) > maxBatchSignSize || len(sigs) != len(addrs) {
		return make([]byte, 32), true
	}

	result := make([]byte, 32)
	for i, sig := range sigs {
		recovered := recoverTronAddr(sig, hash)
		if recovered == addrs[i] {
			result[i] = 1
		}
	}
	return result, true
}

// ── 0x0a ValidateMultiSign ───────────────────────────────────────────────────

type validateMultiSign struct{}

func (c *validateMultiSign) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	ret, used, _, err := c.RunWithStatus(tvm, tcommon.Address{}, input, energy)
	return ret, used, err
}

func (c *validateMultiSign) RunWithStatus(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, bool, error) {
	words := len(input) / 32
	sigCount := 0
	if words > 5 {
		sigCount = (words - 5) / 5
	}
	cost := uint64(1500 * sigCount)
	if energy < cost {
		return nil, energy, false, ErrOutOfEnergy
	}

	ret, success, err := c.executeWithStatus(tvm, input)
	if err != nil {
		return nil, cost, false, err
	}
	return ret, cost, success, nil
}

func (c *validateMultiSign) execute(tvm *TVM, input []byte) []byte {
	ret, _, _ := c.executeWithStatus(tvm, input)
	return ret
}

func (c *validateMultiSign) executeWithStatus(tvm *TVM, input []byte) ([]byte, bool, error) {
	falseResult := make([]byte, 32)
	trueResult := make([]byte, 32)
	trueResult[31] = 1

	if len(input) < 128 {
		return falseResult, true, nil
	}
	if tvm != nil && tvm.cfg.Osaka && !validAbiEncoding(input, 5, 5) {
		return nil, false, nil
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

	var sigs [][]byte
	if tvm != nil && tvm.cfg.SelfdestructRestrict {
		if int(parseUint64FromWord(input, sigsOffset)) > maxMultiSignSize {
			return falseResult, true, nil
		}
		sigs = parseFixed65SigArray(input, sigsOffset)
	} else {
		sigs = parseBytesArray(input, sigsOffset)
	}
	if len(sigs) == 0 || len(sigs) > maxMultiSignSize {
		return falseResult, true, nil
	}

	acc := tvm.StateDB.GetAccount(ownerAddr)
	if acc == nil {
		return falseResult, true, nil
	}

	perm := permissionByID(acc, permID)
	if perm == nil {
		return falseResult, true, nil
	}

	// Mirrors java-tron PrecompiledContracts.ValidateMultiSign + MUtil.checkCPUTime:
	//
	//   for each sig:
	//     if executedSignList contains recoveredAddr:
	//       if executedSignList contains (addr || sig): continue          // exact dup
	//       MUtil.checkCPUTime()                                          // same addr, diff sig
	//       // pre-4_7_1: no-op, fall through and ACCUMULATE again
	//       // post-4_7_1: throws OutOfTimeException → precompile fails
	//     accumulate weight
	//
	// Java's outer try/catch RETHROWS OutOfTimeException specifically
	// (PrecompiledContracts.java:1106-1108), so it propagates as a VM
	// failure rather than a precompile-returned-false result.
	multiSigCheckV2 := tvm != nil && tvm.cfg.MultiSigCheckV2

	var totalWeight int64
	seenAddr := make(map[tcommon.Address]bool)
	seenSig := make(map[string]bool)
	for _, sig := range sigs {
		recovered := recoverTronAddr(sig, hash[:])
		if recovered == (tcommon.Address{}) {
			return falseResult, true, nil
		}
		merged := append(recovered.Bytes(), sig...)
		mergedKey := string(merged)
		if seenAddr[recovered] {
			if seenSig[mergedKey] {
				// Exact (addr, sig) duplicate — java's `continue`,
				// independent of the fork gate.
				continue
			}
			if multiSigCheckV2 {
				// post-4_7_1: java throws OutOfTimeException.
				return nil, false, ErrAlreadyTimeOut
			}
			// pre-4_7_1: fall through and re-accumulate weight, matching
			// java's `MUtil.checkCPUTime()` no-op return.
		}
		weight := permissionWeight(perm, recovered)
		if weight == 0 {
			return falseResult, true, nil // wrong signer
		}
		totalWeight += weight
		seenSig[mergedKey] = true
		seenAddr[recovered] = true
	}

	if totalWeight >= perm.GetThreshold() {
		return trueResult, true, nil
	}
	return falseResult, true, nil
}

// permissionByID returns the permission with the given ID from the account.
func permissionByID(acc interface {
	OwnerPermission() *corepb.Permission
	WitnessPermission() *corepb.Permission
	ActivePermission() []*corepb.Permission
}, id int) *corepb.Permission {
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

// ── 0x01000001–0x01000004 Shielded token precompiles ─────────────────────────

const (
	shieldedTreeWidth                = uint64(1) << 32
	shieldedTRC20NileActivationBlock = 6_360_101
)

type verifyMintProof struct{}

func (c *verifyMintProof) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 150000
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 1504 {
		return shieldedFailurePayload(), cost, nil
	}
	cm := input[0:32]
	cv := input[32:64]
	epk := input[64:96]
	proof := input[96:288]
	bindingSig := input[288:352]
	value := parseInt64FromWord(input, 352)
	signHash := input[384:416]
	frontier, ok := shieldedParseFrontier(input, 416)
	if !ok {
		return shieldedFailurePayload(), cost, nil
	}
	leafCount := parseUint64FromWord(input, 1472)
	if leafCount >= shieldedTreeWidth {
		return shieldedFailurePayload(), cost, nil
	}
	if err := zksnark.VerifyShieldedTRC20Mint(cm, cv, epk, proof, bindingSig, signHash, value); err != nil && !trustedShieldedTRC20Replay(tvm) {
		return shieldedFailurePayload(), cost, nil
	}
	var leaf zksnark.PedersenHash
	copy(leaf[:], cm)
	return shieldedInsertLeaves(frontier, leafCount, []zksnark.PedersenHash{leaf}), cost, nil
}

type verifyTransferProof struct{}

func (c *verifyTransferProof) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 200000
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	switch len(input) {
	case 2080, 2368, 2464, 2752:
	default:
		return shieldedFailurePayload(), cost, nil
	}

	spendOffset, ok := shieldedParseOffset(input, 0)
	if !ok {
		return shieldedFailurePayload(), cost, nil
	}
	spendAuthSigOffset, ok := shieldedParseOffset(input, 32)
	if !ok {
		return shieldedFailurePayload(), cost, nil
	}
	receiveOffset, ok := shieldedParseOffset(input, 64)
	if !ok {
		return shieldedFailurePayload(), cost, nil
	}
	bindingSig := input[96:160]
	signHash := input[160:192]
	value := parseInt64FromWord(input, 192)
	frontier, ok := shieldedParseFrontier(input, 224)
	if !ok {
		return shieldedFailurePayload(), cost, nil
	}
	leafCount := parseUint64FromWord(input, 1280)
	if leafCount >= shieldedTreeWidth-1 {
		return shieldedFailurePayload(), cost, nil
	}

	spendCount, ok := shieldedParseCount(input, spendOffset)
	if !ok {
		return shieldedFailurePayload(), cost, nil
	}
	spendAuthSigCount, ok := shieldedParseCount(input, spendAuthSigOffset)
	if !ok {
		return shieldedFailurePayload(), cost, nil
	}
	receiveCount, ok := shieldedParseCount(input, receiveOffset)
	if !ok {
		return shieldedFailurePayload(), cost, nil
	}
	if spendCount != spendAuthSigCount || spendCount < 1 || spendCount > 2 || receiveCount < 1 || receiveCount > 2 {
		return shieldedFailurePayload(), cost, nil
	}

	spends := make([]zksnark.ShieldedTRC20Spend, spendCount)
	seenNullifiers := make(map[string]struct{}, spendCount)
	spendOffset += 32
	for i := 0; i < spendCount; i++ {
		base := spendOffset + 320*i
		spendData, ok := shieldedSlice(input, base, 320)
		if !ok {
			return shieldedFailurePayload(), cost, nil
		}
		nullifier := spendData[0:32]
		if _, exists := seenNullifiers[string(nullifier)]; exists {
			return shieldedFailurePayload(), cost, nil
		}
		seenNullifiers[string(nullifier)] = struct{}{}
		spends[i] = zksnark.ShieldedTRC20Spend{
			Nullifier:               nullifier,
			Anchor:                  spendData[32:64],
			ValueCommitment:         spendData[64:96],
			Rk:                      spendData[96:128],
			Proof:                   spendData[128:320],
			SpendAuthoritySignature: nil,
		}
	}
	spendAuthSigOffset += 32
	for i := 0; i < spendCount; i++ {
		base := spendAuthSigOffset + 64*i
		sig, ok := shieldedSlice(input, base, 64)
		if !ok {
			return shieldedFailurePayload(), cost, nil
		}
		spends[i].SpendAuthoritySignature = sig
	}

	receives := make([]zksnark.ShieldedTRC20Receive, receiveCount)
	leaves := make([]zksnark.PedersenHash, receiveCount)
	seenCommitments := make(map[string]struct{}, receiveCount)
	receiveOffset += 32
	for i := 0; i < receiveCount; i++ {
		base := receiveOffset + 288*i
		receiveData, ok := shieldedSlice(input, base, 288)
		if !ok {
			return shieldedFailurePayload(), cost, nil
		}
		cm := receiveData[0:32]
		if _, exists := seenCommitments[string(cm)]; exists {
			return shieldedFailurePayload(), cost, nil
		}
		seenCommitments[string(cm)] = struct{}{}
		receives[i] = zksnark.ShieldedTRC20Receive{
			NoteCommitment:  cm,
			ValueCommitment: receiveData[32:64],
			Epk:             receiveData[64:96],
			Proof:           receiveData[96:288],
		}
		copy(leaves[i][:], cm)
	}

	// TEMP DEBUG (Nile 6,498,505 stall): trace which branch of the shielded
	// transfer precompile is taken during replay. Revert once captured.
	verr := zksnark.VerifyShieldedTRC20Transfer(spends, receives, bindingSig, signHash, value)
	trusted := trustedShieldedTRC20Replay(tvm)
	var gh tcommon.Hash
	var bn uint64
	var trustRet bool
	var expRet corepb.Transaction_ResultContractResult
	if tvm != nil {
		gh, bn, trustRet, expRet = tvm.GenesisHash, tvm.BlockNumber, tvm.TrustTransactionRet, tvm.ExpectedContractRet
	}
	fmt.Fprintf(os.Stderr, "[SHIELD-DBG] transfer blk=%d avail=%v verr=%v trusted=%v trustRet=%v expRet=%d genMatch=%v spends=%d recv=%d leaf=%d inLen=%d\n",
		bn, zksnark.Available(), verr, trusted, trustRet, expRet, gh == params.NileGenesisHash, spendCount, receiveCount, leafCount, len(input))
	if verr != nil && !trusted {
		fmt.Fprintln(os.Stderr, "[SHIELD-DBG] transfer -> FAILURE payload (verify failed, not trusted)")
		return shieldedFailurePayload(), cost, nil
	}
	out := shieldedInsertLeaves(frontier, leafCount, leaves)
	fw := byte(255)
	if len(out) > 31 {
		fw = out[31]
	}
	fmt.Fprintf(os.Stderr, "[SHIELD-DBG] transfer -> insertLeaves firstWord=%d outLen=%d\n", fw, len(out))
	return out, cost, nil
}

type verifyBurnProof struct{}

func (c *verifyBurnProof) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 150000
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 512 {
		return shieldedFailurePayload(), cost, nil
	}
	spend := zksnark.ShieldedTRC20Spend{
		Nullifier:               input[0:32],
		Anchor:                  input[32:64],
		ValueCommitment:         input[64:96],
		Rk:                      input[96:128],
		Proof:                   input[128:320],
		SpendAuthoritySignature: input[320:384],
	}
	value := parseInt64FromWord(input, 384)
	bindingSig := input[416:480]
	signHash := input[480:512]
	if err := zksnark.VerifyShieldedTRC20Burn(spend, bindingSig, signHash, value); err != nil && !trustedShieldedTRC20Replay(tvm) {
		return shieldedFailurePayload(), cost, nil
	}
	return shieldedSuccessPayload(), cost, nil
}

type shieldedMerkleHash struct{}

func (c *shieldedMerkleHash) Run(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	ret, used, _, err := c.RunWithStatus(nil, tcommon.Address{}, input, energy)
	return ret, used, err
}

func (c *shieldedMerkleHash) RunWithStatus(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, bool, error) {
	const cost = 500
	if energy < cost {
		return nil, energy, false, ErrOutOfEnergy
	}
	if len(input) < 96 {
		return nil, cost, false, nil
	}
	level, ok := shieldedParseIntWord(input, 0)
	if !ok || level < 0 || level > 62 {
		return nil, cost, false, nil
	}
	var left, right zksnark.PedersenHash
	copy(left[:], input[32:64])
	copy(right[:], input[64:96])
	hash, err := zksnark.Combine(level, left, right)
	if err != nil {
		return nil, cost, false, nil
	}
	out := make([]byte, 32)
	copy(out, hash[:])
	return out, cost, true, nil
}

func shieldedFailurePayload() []byte {
	return make([]byte, 32)
}

func shieldedSuccessPayload() []byte {
	out := make([]byte, 32)
	out[31] = 1
	return out
}

func trustedShieldedTRC20Replay(tvm *TVM) bool {
	return tvm != nil &&
		tvm.TrustTransactionRet &&
		tvm.ExpectedContractRet == corepb.Transaction_Result_SUCCESS &&
		tvm.GenesisHash == params.NileGenesisHash &&
		tvm.BlockNumber >= shieldedTRC20NileActivationBlock
}

func shieldedParseOffset(input []byte, offset int) (int, bool) {
	value, ok := shieldedParseIntWord(input, offset)
	if !ok || value < 0 || value > len(input) {
		return 0, false
	}
	return value, true
}

func shieldedParseCount(input []byte, offset int) (int, bool) {
	value, ok := shieldedParseIntWord(input, offset)
	if !ok || value < 0 {
		return 0, false
	}
	return value, true
}

func shieldedParseIntWord(input []byte, offset int) (int, bool) {
	if _, ok := shieldedSlice(input, offset, 32); !ok {
		return 0, false
	}
	value := parseUint64FromWord(input, offset)
	if value > uint64(int(^uint(0)>>1)) {
		return 0, false
	}
	return int(value), true
}

func shieldedSlice(input []byte, offset, size int) ([]byte, bool) {
	if offset < 0 || size < 0 || offset > len(input) || size > len(input)-offset {
		return nil, false
	}
	return input[offset : offset+size], true
}

func shieldedParseFrontier(input []byte, offset int) ([33]zksnark.PedersenHash, bool) {
	var frontier [33]zksnark.PedersenHash
	for i := range frontier {
		word, ok := shieldedSlice(input, offset+i*32, 32)
		if !ok {
			return frontier, false
		}
		copy(frontier[i][:], word)
	}
	return frontier, true
}

func shieldedFrontierSlot(leafIndex uint64) int {
	if leafIndex%2 == 0 {
		return 0
	}
	exp := 1
	pow1 := uint64(2)
	pow2 := pow1 << 1
	for {
		if (leafIndex+1-pow1)%pow2 == 0 {
			return exp
		}
		pow1 = pow2
		pow2 <<= 1
		exp++
	}
}

func shieldedInsertLeaves(frontier [33]zksnark.PedersenHash, leafCount uint64, leaves []zksnark.PedersenHash) []byte {
	if len(leaves) == 0 {
		return shieldedFailurePayload()
	}
	empties, err := zksnark.EmptyRoots()
	if err != nil {
		return shieldedFailurePayload()
	}

	slots := make([]int, len(leaves))
	resultWords := 1 // final root
	for i := range leaves {
		slots[i] = shieldedFrontierSlot(leafCount + uint64(i))
		resultWords += slots[i] + 1
	}
	result := make([]byte, resultWords*32)

	offset := 0
	var nodeIndex uint64
	var nodeValue zksnark.PedersenHash
	for i, leaf := range leaves {
		copy(result[offset:offset+32], int64ToBytes32(int64(slots[i])))
		offset += 32
		nodeIndex = leafCount + uint64(i) + shieldedTreeWidth - 1
		nodeValue = leaf
		if slots[i] == 0 {
			frontier[0] = nodeValue
			continue
		}
		for level := 1; level <= slots[i]; level++ {
			var left, right zksnark.PedersenHash
			if nodeIndex%2 == 0 {
				left = frontier[level-1]
				right = nodeValue
				nodeIndex = (nodeIndex - 1) / 2
			} else {
				left = nodeValue
				right = empties[level-1]
				nodeIndex /= 2
			}
			hash, err := zksnark.Combine(level-1, left, right)
			if err != nil {
				return shieldedFailurePayload()
			}
			nodeValue = hash
			copy(result[offset:offset+32], hash[:])
			offset += 32
		}
		frontier[slots[i]] = nodeValue
	}

	for level := slots[len(slots)-1] + 1; level <= zksnark.Depth; level++ {
		var left, right zksnark.PedersenHash
		if nodeIndex%2 == 0 {
			left = frontier[level-1]
			right = nodeValue
			nodeIndex = (nodeIndex - 1) / 2
		} else {
			left = nodeValue
			right = empties[level-1]
			nodeIndex /= 2
		}
		hash, err := zksnark.Combine(level-1, left, right)
		if err != nil {
			return shieldedFailurePayload()
		}
		nodeValue = hash
	}
	copy(result[offset:offset+32], nodeValue[:])

	out := make([]byte, 32+len(result))
	out[31] = 1
	copy(out[32:], result)
	return out
}

// ── 0x01000005 RewardBalance ──────────────────────────────────────────────────

type rewardBalance struct{}

func (c *rewardBalance) Run(tvm *TVM, caller tcommon.Address, _ []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 500
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	return int64ToBytes32(tvmQueryReward(tvm, caller)), cost, nil
}

// ── 0x01000006 IsSrCandidate ──────────────────────────────────────────────────

type isSrCandidate struct{}

func (c *isSrCandidate) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 20
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 32 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input)
	result := make([]byte, 32)
	if tvm.StateDB.GetWitness(addr) != nil {
		result[31] = 1
	}
	return result, cost, nil
}

// ── 0x01000007 VoteCount ──────────────────────────────────────────────────────

type voteCount struct{}

func (c *voteCount) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
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
	for _, vote := range tvm.StateDB.GetVotes(ownerAddr) {
		if tcommon.BytesToAddress(vote.GetVoteAddress()) == srAddr {
			count += vote.GetVoteCount()
		}
	}
	return int64ToBytes32(count), cost, nil
}

// ── 0x01000008 UsedVoteCount ──────────────────────────────────────────────────

type usedVoteCount struct{}

func (c *usedVoteCount) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 20
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 32 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input)
	var total int64
	for _, vote := range tvm.StateDB.GetVotes(addr) {
		total += vote.GetVoteCount()
	}
	return int64ToBytes32(total), cost, nil
}

// ── 0x01000009 ReceivedVoteCount ─────────────────────────────────────────────

type receivedVoteCount struct{}

func (c *receivedVoteCount) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 20
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 32 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input)
	var count int64
	if w := tvm.StateDB.GetWitness(addr); w != nil {
		count = w.VoteCount()
	}
	return int64ToBytes32(count), cost, nil
}

// ── 0x0100000a TotalVoteCount ─────────────────────────────────────────────────

type totalVoteCount struct{}

func (c *totalVoteCount) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 20
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 32 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input)
	var tp int64
	if tvm.cfg.NewResourceModelPower {
		tp = tvm.StateDB.GetAllTronPower(addr)
	} else {
		tp = tvm.StateDB.GetLegacyTronPower(addr)
	}
	return int64ToBytes32(tp / trxPrecision), cost, nil
}

// ── 0x0100000b GetChainParameter ─────────────────────────────────────────────

type getChainParameter struct{}

func (c *getChainParameter) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 32 {
		return make([]byte, 32), cost, nil
	}
	code := parseInt64FromWord(input, 0)
	dp := tvm.StateDB.DynamicProperties()
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

func (c *availableUnfreezeV2Size) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 32 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input)
	acc := tvm.StateDB.GetAccount(addr)
	if acc == nil {
		return make([]byte, 32), cost, nil
	}
	used := tvmUnfreezingV2Count(acc, stakingNowMs(tvm))
	available := int64(maxUnfreezeV2 - used)
	if available < 0 {
		available = 0
	}
	return int64ToBytes32(available), cost, nil
}

// ── 0x0100000d UnfreezableBalanceV2 ──────────────────────────────────────────

type unfreezableBalanceV2 struct{}

func (c *unfreezableBalanceV2) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 64 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input[0:32])
	resType, ok := freezeV2ResourceFromInt(parseInt64FromWord(input, 32))
	if !ok {
		return make([]byte, 32), cost, nil
	}
	bal := tvm.StateDB.GetFrozenV2Amount(addr, resType)
	return int64ToBytes32(bal), cost, nil
}

// ── 0x0100000e ExpireUnfreezeBalanceV2 ────────────────────────────────────────

type expireUnfreezeBalanceV2 struct{}

func (c *expireUnfreezeBalanceV2) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
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

	acc := tvm.StateDB.GetAccount(addr)
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

func (c *delegatableResource) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 64 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input[0:32])
	resType, ok := stakingResourceFromInt(parseInt64FromWord(input, 32))
	if !ok {
		return make([]byte, 32), cost, nil
	}

	return int64ToBytes32(delegatableFrozenV2(tvm, addr, resType)), cost, nil
}

// ── 0x01000010 ResourceV2 ─────────────────────────────────────────────────────

type resourceV2 struct{}

func (c *resourceV2) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 96 {
		return make([]byte, 32), cost, nil
	}
	target := tronAddrFromWord(input[0:32])
	from := tronAddrFromWord(input[32:64])
	typeCode := parseInt64FromWord(input, 64)

	var balance int64
	if from == target {
		resType, ok := freezeV2ResourceFromInt(typeCode)
		if !ok {
			return make([]byte, 32), cost, nil
		}
		balance = tvm.StateDB.GetFrozenV2Amount(from, resType)
	} else {
		resType, ok := stakingResourceFromInt(typeCode)
		if !ok {
			return make([]byte, 32), cost, nil
		}
		balance = delegatedPairV2(tvm, from, target, resType)
	}
	return int64ToBytes32(balance), cost, nil
}

// ── 0x01000011 CheckUnDelegateResource ───────────────────────────────────────

type checkUnDelegateResource struct{}

func (c *checkUnDelegateResource) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 96 {
		return make([]byte, 96), cost, nil
	}
	addr := tronAddrFromWord(input[0:32])
	amount := parseInt64FromWord(input, 32)
	resType, ok := stakingResourceFromInt(parseInt64FromWord(input, 64))
	if !ok || amount <= 0 {
		return make([]byte, 96), cost, nil
	}
	acc := tvm.StateDB.GetAccount(addr)
	if acc == nil {
		return make([]byte, 96), cost, nil
	}

	usage, restoreSeconds := resourceUsageBalanceAndRestoreSeconds(tvm, addr, resType)
	resourceLimit := totalResourceBalance(acc, resType)
	if amount > resourceLimit {
		amount = resourceLimit
	}
	if resourceLimit <= usage {
		return encodeInt64Words(0, amount, restoreSeconds), cost, nil
	}

	clean := int64(float64(amount) * (float64(resourceLimit-usage) / float64(resourceLimit)))
	return encodeInt64Words(clean, amount-clean, restoreSeconds), cost, nil
}

// ── 0x01000012 ResourceUsage ──────────────────────────────────────────────────

type resourceUsage struct{}

func (c *resourceUsage) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 64 {
		return make([]byte, 64), cost, nil
	}
	addr := tronAddrFromWord(input[0:32])
	resType, ok := stakingResourceFromInt(parseInt64FromWord(input, 32))
	if !ok || tvm.StateDB.GetAccount(addr) == nil {
		return make([]byte, 64), cost, nil
	}
	usage, restoreSeconds := resourceUsageBalanceAndRestoreSeconds(tvm, addr, resType)
	return encodeInt64Words(usage, restoreSeconds), cost, nil
}

// ── 0x01000013 TotalResource ──────────────────────────────────────────────────

type totalResource struct{}

func (c *totalResource) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 64 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input[0:32])
	resType, ok := stakingResourceFromInt(parseInt64FromWord(input, 32))
	if !ok {
		return make([]byte, 32), cost, nil
	}
	acc := tvm.StateDB.GetAccount(addr)
	if acc == nil {
		return make([]byte, 32), cost, nil
	}
	return int64ToBytes32(totalResourceBalance(acc, resType)), cost, nil
}

// ── 0x01000014 TotalDelegatedResource ────────────────────────────────────────

type totalDelegatedResource struct{}

func (c *totalDelegatedResource) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 64 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input[0:32])
	resType, ok := stakingResourceFromInt(parseInt64FromWord(input, 32))
	if !ok {
		return make([]byte, 32), cost, nil
	}
	acc := tvm.StateDB.GetAccount(addr)
	if acc == nil {
		return make([]byte, 32), cost, nil
	}
	return int64ToBytes32(totalDelegatedResourceBalance(acc, resType)), cost, nil
}

// ── 0x01000015 TotalAcquiredResource ─────────────────────────────────────────

type totalAcquiredResource struct{}

func (c *totalAcquiredResource) Run(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 50
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	if len(input) != 64 {
		return make([]byte, 32), cost, nil
	}
	addr := tronAddrFromWord(input[0:32])
	resType, ok := stakingResourceFromInt(parseInt64FromWord(input, 32))
	if !ok {
		return make([]byte, 32), cost, nil
	}
	acc := tvm.StateDB.GetAccount(addr)
	if acc == nil {
		return make([]byte, 32), cost, nil
	}
	return int64ToBytes32(totalAcquiredResourceBalance(acc, resType)), cost, nil
}

// ── Resource helpers ──────────────────────────────────────────────────────────

func stakingResourceFromInt(v int64) (corepb.ResourceCode, bool) {
	switch v {
	case 0:
		return corepb.ResourceCode_BANDWIDTH, true
	case 1:
		return corepb.ResourceCode_ENERGY, true
	default:
		return corepb.ResourceCode_BANDWIDTH, false
	}
}

func freezeV2ResourceFromInt(v int64) (corepb.ResourceCode, bool) {
	if v == 2 {
		return corepb.ResourceCode_TRON_POWER, true
	}
	return stakingResourceFromInt(v)
}

func delegatedPairV2(tvm *TVM, from, to tcommon.Address, resType corepb.ResourceCode) int64 {
	if tvm == nil || tvm.StateDB == nil {
		return 0
	}
	var total int64
	for _, locked := range []bool{false, true} {
		dr := tvm.StateDB.ReadDelegatedResourceV2(from, to, locked)
		if dr == nil {
			continue
		}
		switch resType {
		case corepb.ResourceCode_BANDWIDTH:
			total += dr.FrozenBalanceForBandwidth
		case corepb.ResourceCode_ENERGY:
			total += dr.FrozenBalanceForEnergy
		}
	}
	return total
}

func delegatableFrozenV2(tvm *TVM, addr tcommon.Address, resType corepb.ResourceCode) int64 {
	acc := tvm.StateDB.GetAccount(addr)
	if acc == nil {
		return 0
	}
	frozenV2 := acc.GetFrozenV2Amount(resType)
	if frozenV2 <= 0 {
		return 0
	}
	usage, _ := resourceUsageBalanceAndRestoreSeconds(tvm, addr, resType)
	if usage <= 0 {
		return frozenV2
	}

	var v2Usage int64
	switch resType {
	case corepb.ResourceCode_BANDWIDTH:
		v2Usage = usage - acc.TotalFrozenBandwidth() -
			acc.AcquiredDelegatedFrozenBandwidth() -
			acc.AcquiredDelegatedFrozenV2BalanceForBandwidth()
	case corepb.ResourceCode_ENERGY:
		v2Usage = usage - acc.FrozenEnergyAmount() -
			acc.AcquiredDelegatedFrozenEnergy() -
			acc.AcquiredDelegatedFrozenV2BalanceForEnergy()
	}
	if v2Usage < 0 {
		v2Usage = 0
	}
	available := frozenV2 - v2Usage
	if available < 0 {
		return 0
	}
	return available
}

func resourceUsageBalanceAndRestoreSeconds(tvm *TVM, addr tcommon.Address, resType corepb.ResourceCode) (int64, int64) {
	acc := tvm.StateDB.GetAccount(addr)
	dp := stakingDynamicProperties(tvm)
	if acc == nil || dp == nil {
		return 0, 0
	}

	var usage, lastTime, totalLimit, totalWeight int64
	switch resType {
	case corepb.ResourceCode_BANDWIDTH:
		usage = tvm.StateDB.GetNetUsage(addr)
		lastTime = tvm.StateDB.GetLatestConsumeTime(addr)
		totalLimit = dp.TotalNetLimit()
		totalWeight = dp.TotalNetWeight()
	case corepb.ResourceCode_ENERGY:
		usage = tvm.StateDB.GetEnergyUsage(addr)
		lastTime = tvm.StateDB.GetLatestConsumeTimeForEnergy(addr)
		totalLimit = dp.TotalEnergyCurrentLimit()
		totalWeight = dp.TotalEnergyWeight()
	default:
		return 0, 0
	}

	now := stakingNowSlot(tvm)
	window := stakingWindowSizeSlots(acc, resType)
	if now >= lastTime+window {
		return 0, 0
	}
	restoreSeconds := (lastTime + window - now) * params.BlockProducedInterval / 1000
	recovered := recoverStakingUsage(usage, lastTime, now, window, dp.AllowHardenResourceCalculation())
	balance := stakingUsageToBalance(recovered, totalWeight, totalLimit, dp.AllowHardenResourceCalculation())
	return balance, restoreSeconds
}

func totalResourceBalance(acc *types.Account, resType corepb.ResourceCode) int64 {
	switch resType {
	case corepb.ResourceCode_BANDWIDTH:
		return acc.TotalFrozenBandwidth() +
			acc.AcquiredDelegatedFrozenBandwidth() +
			acc.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH) +
			acc.AcquiredDelegatedFrozenV2BalanceForBandwidth()
	case corepb.ResourceCode_ENERGY:
		return acc.FrozenEnergyAmount() +
			acc.AcquiredDelegatedFrozenEnergy() +
			acc.GetFrozenV2Amount(corepb.ResourceCode_ENERGY) +
			acc.AcquiredDelegatedFrozenV2BalanceForEnergy()
	}
	return 0
}

func totalDelegatedResourceBalance(acc *types.Account, resType corepb.ResourceCode) int64 {
	switch resType {
	case corepb.ResourceCode_BANDWIDTH:
		return acc.DelegatedFrozenBandwidth() + acc.DelegatedFrozenV2BalanceForBandwidth()
	case corepb.ResourceCode_ENERGY:
		return acc.DelegatedFrozenEnergy() + acc.DelegatedFrozenV2BalanceForEnergy()
	}
	return 0
}

func totalAcquiredResourceBalance(acc *types.Account, resType corepb.ResourceCode) int64 {
	switch resType {
	case corepb.ResourceCode_BANDWIDTH:
		return acc.AcquiredDelegatedFrozenBandwidth() + acc.AcquiredDelegatedFrozenV2BalanceForBandwidth()
	case corepb.ResourceCode_ENERGY:
		return acc.AcquiredDelegatedFrozenEnergy() + acc.AcquiredDelegatedFrozenV2BalanceForEnergy()
	}
	return 0
}

func stakingDynamicProperties(tvm *TVM) *state.DynamicProperties {
	if tvm.DynProps != nil {
		return tvm.DynProps
	}
	if tvm.StateDB != nil {
		return tvm.StateDB.DynamicProperties()
	}
	return nil
}

func stakingNowSlot(tvm *TVM) int64 {
	if tvm != nil && tvm.HasHeadSlot {
		return tvm.HeadSlot
	}
	if dp := stakingDynamicProperties(tvm); dp != nil {
		return dp.LatestBlockHeaderTimestamp() / params.BlockProducedInterval
	}
	if tvm == nil {
		return 0
	}
	return tvm.Timestamp / params.BlockProducedInterval
}

func stakingNowMs(tvm *TVM) int64 {
	if dp := stakingDynamicProperties(tvm); dp != nil {
		return dp.LatestBlockHeaderTimestamp()
	}
	if tvm == nil {
		return 0
	}
	return tvm.Timestamp
}

func stakingWindowSizeSlots(acc *types.Account, resType corepb.ResourceCode) int64 {
	const windowSizePrecision = int64(1000)
	pb := acc.Proto()
	var windowSize int64
	var optimized bool
	if resType == corepb.ResourceCode_BANDWIDTH {
		windowSize = pb.GetNetWindowSize()
		optimized = pb.GetNetWindowOptimized()
	} else if pb.GetAccountResource() != nil {
		windowSize = pb.GetAccountResource().GetEnergyWindowSize()
		optimized = pb.GetAccountResource().GetEnergyWindowOptimized()
	}
	if windowSize == 0 {
		return int64(params.WindowSizeSlots)
	}
	if optimized {
		if windowSize < windowSizePrecision {
			return int64(params.WindowSizeSlots)
		}
		return windowSize / windowSizePrecision
	}
	return windowSize
}

func recoverStakingUsage(oldUsage, lastTime, now, windowSize int64, harden bool) int64 {
	if oldUsage <= 0 {
		return 0
	}
	elapsed := now - lastTime
	if elapsed >= windowSize {
		return 0
	}
	if elapsed <= 0 {
		return oldUsage
	}
	remaining := windowSize - elapsed
	if harden {
		averageLastUsage := divideCeilBigInt(
			new(big.Int).Mul(big.NewInt(oldUsage), big.NewInt(resourcePrecisionForStaking)),
			big.NewInt(windowSize),
		)
		decay := float64(remaining) / float64(windowSize)
		averageLastUsage = int64(math.Round(float64(averageLastUsage) * decay))
		return bigMulDivStaking(averageLastUsage, windowSize, resourcePrecisionForStaking)
	}
	return oldUsage * remaining / windowSize
}

func stakingUsageToBalance(usage, totalWeight, totalLimit int64, harden bool) int64 {
	if usage <= 0 || totalWeight <= 0 || totalLimit <= 0 {
		return 0
	}
	if harden {
		n := new(big.Int).Mul(big.NewInt(usage), big.NewInt(totalWeight))
		n.Mul(n, big.NewInt(trxPrecision))
		n.Quo(n, big.NewInt(totalLimit))
		return n.Int64()
	}
	return int64(float64(usage) * float64(totalWeight) / float64(totalLimit) * float64(trxPrecision))
}

const resourcePrecisionForStaking = int64(1_000_000)

func divideCeilBigInt(numerator, denominator *big.Int) int64 {
	q, r := new(big.Int).QuoRem(numerator, denominator, new(big.Int))
	if r.Sign() > 0 {
		q.Add(q, big.NewInt(1))
	}
	return q.Int64()
}

func bigMulDivStaking(a, b, c int64) int64 {
	n := new(big.Int).Mul(big.NewInt(a), big.NewInt(b))
	n.Quo(n, big.NewInt(c))
	return n.Int64()
}

func encodeInt64Words(values ...int64) []byte {
	out := make([]byte, 32*len(values))
	for i, v := range values {
		copy(out[i*32:(i+1)*32], int64ToBytes32(v))
	}
	return out
}

package vm

import (
	"crypto/sha256"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/crypto/pq"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

const (
	pqMessageSize          = 32
	pqFalconSignatureSlot  = pq.FnDsa512SignatureMaxSize - 1
	pqBatchMaxSize         = 16
	pqBatchEnergyPerSign   = 2400
	pqSingleVerifyEnergy   = 3000
	pqMultiECDSAEnergy     = 1500
	pqMultiMaxSize         = 5
	pqMultiWorstSignEnergy = pqBatchEnergyPerSign
)

func pqBoolWord(v bool) []byte {
	out := make([]byte, tronPrecompileWordSize)
	if v {
		out[len(out)-1] = 1
	}
	return out
}

// falconSlotSignature converts PQ1's fixed 666-byte EIP-8052 slot into the
// variable-length, headered Falcon signature used on the protobuf wire. The
// slot contains salt || compressed_s2 without 0x39 and is padded with zeroes.
func falconSlotSignature(slot []byte) ([]byte, bool) {
	if len(slot) != pqFalconSignatureSlot {
		return nil, false
	}
	logical := len(slot)
	for logical > 0 && slot[logical-1] == 0 {
		logical--
	}
	if logical < pq.FnDsa512SignatureMinSize-1 || logical > pq.FnDsa512SignatureMaxSize-1 {
		return nil, false
	}
	sig := make([]byte, logical+1)
	sig[0] = 0x39
	copy(sig[1:], slot[:logical])
	return sig, true
}

type verifyFnDsa512 struct{}

func (c *verifyFnDsa512) Run(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	if energy < pqSingleVerifyEnergy {
		return nil, energy, ErrOutOfEnergy
	}
	const inputSize = pqMessageSize + pqFalconSignatureSlot + pq.FnDsa512PublicKeySize
	if len(input) != inputSize {
		return pqBoolWord(false), pqSingleVerifyEnergy, nil
	}
	sig, ok := falconSlotSignature(input[pqMessageSize : pqMessageSize+pqFalconSignatureSlot])
	if !ok {
		return pqBoolWord(false), pqSingleVerifyEnergy, nil
	}
	pk := input[pqMessageSize+pqFalconSignatureSlot:]
	return pqBoolWord(pq.Verify(corepb.PQScheme_FN_DSA_512, pk, input[:pqMessageSize], sig)), pqSingleVerifyEnergy, nil
}

type verifyMlDsa44 struct{}

func (c *verifyMlDsa44) Run(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	if energy < pqSingleVerifyEnergy {
		return nil, energy, ErrOutOfEnergy
	}
	const inputSize = pqMessageSize + pq.MlDsa44SignatureSize + pq.MlDsa44PublicKeySize
	if len(input) != inputSize {
		return pqBoolWord(false), pqSingleVerifyEnergy, nil
	}
	sigEnd := pqMessageSize + pq.MlDsa44SignatureSize
	return pqBoolWord(pq.Verify(corepb.PQScheme_ML_DSA_44, input[sigEnd:], input[:pqMessageSize], input[pqMessageSize:sigEnd])), pqSingleVerifyEnergy, nil
}

// pqValidABIHead and pqArrayWord reproduce PQPrecompiledContracts' stricter
// ABI validation. Unlike legacy precompiles, PQ bytes[] entries are variable
// length, so only word alignment and explicit bounds are accepted.
func pqValidABIHead(input []byte, headWords int) bool {
	return len(input)%tronPrecompileWordSize == 0 && len(input) >= headWords*tronPrecompileWordSize
}

func pqArrayWord(input []byte, offsetWord, headWords int) (int, bool) {
	offset, ok := wordIntValueSafe(input, offsetWord)
	if !ok || offset < headWords*tronPrecompileWordSize || offset%tronPrecompileWordSize != 0 {
		return 0, false
	}
	idx := offset / tronPrecompileWordSize
	return idx, idx < wordCount(input)
}

// pqEnergyArrayWord mirrors getEnergyForData's deliberately looser decoding:
// java divides intValueSafe by 32 without checking alignment or ABI-head
// placement. Execution performs the strict validation separately.
func pqEnergyArrayWord(input []byte, offsetWord int) (int, bool) {
	offset, ok := wordIntValueSafe(input, offsetWord)
	if !ok {
		return 0, false
	}
	idx := offset / tronPrecompileWordSize
	return idx, idx >= 0 && idx < wordCount(input)
}

func pqExtractBytesArray(input []byte, offset int) ([][]byte, bool) {
	if offset < 0 || offset >= wordCount(input) {
		return nil, false
	}
	count, _ := wordIntValueSafe(input, offset)
	if int64(offset)+int64(count)+1 > int64(wordCount(input)) {
		return nil, false
	}
	out := make([][]byte, count)
	for i := 0; i < count; i++ {
		relBytes, ok := wordIntValueSafe(input, offset+i+1)
		if !ok || relBytes%tronPrecompileWordSize != 0 {
			return nil, false
		}
		relWords := relBytes / tronPrecompileWordSize
		lengthIdx := offset + relWords + 1
		length, ok := wordIntValueSafe(input, lengthIdx)
		if !ok {
			return nil, false
		}
		from := int64(offset+relWords+2) * tronPrecompileWordSize
		to := from + int64(length)
		if from < 0 || from > int64(len(input)) || to < from || to > int64(len(input)) {
			return nil, false
		}
		out[i] = append([]byte(nil), input[int(from):int(to)]...)
	}
	return out, true
}

type batchValidatePQ struct{ scheme corepb.PQScheme }

func (c *batchValidatePQ) energy(input []byte) uint64 {
	idx, ok := pqEnergyArrayWord(input, 1)
	if !ok {
		return pqBatchMaxSize * pqBatchEnergyPerSign
	}
	count, ok := wordIntValueSafe(input, idx)
	if !ok {
		return pqBatchMaxSize * pqBatchEnergyPerSign
	}
	if count > pqBatchMaxSize {
		count = pqBatchMaxSize
	}
	return uint64(count * pqBatchEnergyPerSign)
}

func (c *batchValidatePQ) Run(tvm *TVM, caller tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	ret, used, _, err := c.RunWithStatus(tvm, caller, input, energy)
	return ret, used, err
}

func (c *batchValidatePQ) RunWithStatus(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, bool, error) {
	cost := c.energy(input)
	if energy < cost {
		return nil, energy, false, ErrOutOfEnergy
	}
	if !pqValidABIHead(input, 4) {
		return nil, cost, false, nil
	}
	sigWord, ok1 := pqArrayWord(input, 1, 4)
	pkWord, ok2 := pqArrayWord(input, 2, 4)
	addrWord, ok3 := pqArrayWord(input, 3, 4)
	if !ok1 || !ok2 || !ok3 {
		return nil, cost, false, nil
	}
	sigCount, _ := wordIntValueSafe(input, sigWord)
	pkCount, _ := wordIntValueSafe(input, pkWord)
	addrCount, _ := wordIntValueSafe(input, addrWord)
	if sigCount > pqBatchMaxSize || pkCount > pqBatchMaxSize || addrCount > pqBatchMaxSize || sigCount != pkCount || sigCount != addrCount {
		return nil, cost, false, nil
	}
	sigs, ok := pqExtractBytesArray(input, sigWord)
	if !ok {
		return nil, cost, false, nil
	}
	pks, ok := pqExtractBytesArray(input, pkWord)
	if !ok {
		return nil, cost, false, nil
	}
	addrs, oob := extractBytes32ArrayWordIndex(input, addrWord)
	if oob || len(sigs) == 0 || len(pks) != len(sigs) || len(addrs) != len(sigs) {
		return nil, cost, false, nil
	}
	hash := input[:pqMessageSize]
	result := make([]byte, tronPrecompileWordSize)
	for i := range sigs {
		sig := sigs[i]
		if c.scheme == corepb.PQScheme_FN_DSA_512 {
			var valid bool
			sig, valid = falconSlotSignature(sig)
			if !valid {
				continue
			}
		} else if len(sig) != pq.MlDsa44SignatureSize {
			continue
		}
		addr, err := pq.Address(c.scheme, pks[i])
		if err == nil && equalAddressLast20(addrs[i], addr.Bytes()) && pq.Verify(c.scheme, pks[i], hash, sig) {
			result[i] = 1
		}
	}
	return result, cost, true, nil
}

type validateMultiPQSig struct{}

func (c *validateMultiPQSig) energy(input []byte) uint64 {
	ecdsaWord, ok1 := pqEnergyArrayWord(input, 3)
	schemeWord, ok2 := pqEnergyArrayWord(input, 4)
	if !ok1 || !ok2 {
		return pqMultiMaxSize * pqMultiWorstSignEnergy
	}
	ecdsaCount, ok1 := wordIntValueSafe(input, ecdsaWord)
	pqCount, ok2 := wordIntValueSafe(input, schemeWord)
	if !ok1 || !ok2 || int64(ecdsaCount)+int64(pqCount) > pqMultiMaxSize {
		return pqMultiMaxSize * pqMultiWorstSignEnergy
	}
	return uint64(ecdsaCount*pqMultiECDSAEnergy + pqCount*pqBatchEnergyPerSign)
}

func (c *validateMultiPQSig) Run(tvm *TVM, caller tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	ret, used, _, err := c.RunWithStatus(tvm, caller, input, energy)
	return ret, used, err
}

func (c *validateMultiPQSig) RunWithStatus(tvm *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, bool, error) {
	cost := c.energy(input)
	if energy < cost {
		return nil, energy, false, ErrOutOfEnergy
	}
	if !pqValidABIHead(input, 7) {
		return nil, cost, false, nil
	}
	arrayWords := [4]int{}
	for i := range arrayWords {
		var ok bool
		arrayWords[i], ok = pqArrayWord(input, i+3, 7)
		if !ok {
			return nil, cost, false, nil
		}
	}
	permID, _ := wordIntValueSafe(input, 1)
	if permID > 9 {
		return nil, cost, false, nil
	}
	ecdsaCount, _ := wordIntValueSafe(input, arrayWords[0])
	schemeCount, _ := wordIntValueSafe(input, arrayWords[1])
	pqSigCount, _ := wordIntValueSafe(input, arrayWords[2])
	pqPKCount, _ := wordIntValueSafe(input, arrayWords[3])
	if ecdsaCount > pqMultiMaxSize || schemeCount > pqMultiMaxSize || schemeCount != pqSigCount || schemeCount != pqPKCount || int64(ecdsaCount)+int64(schemeCount) == 0 || int64(ecdsaCount)+int64(schemeCount) > pqMultiMaxSize {
		return nil, cost, false, nil
	}
	ecdsaSigs, oob := extractSigArrayWordIndex(input, arrayWords[0])
	if oob {
		return nil, cost, false, nil
	}
	pqSigs, ok := pqExtractBytesArray(input, arrayWords[2])
	if !ok {
		return nil, cost, false, nil
	}
	pqPKs, ok := pqExtractBytesArray(input, arrayWords[3])
	if !ok || len(pqSigs) != schemeCount || len(pqPKs) != schemeCount {
		return nil, cost, false, nil
	}
	schemes := make([]corepb.PQScheme, schemeCount)
	for i := range schemes {
		tag, ok := wordIntValueSafe(input, arrayWords[1]+1+i)
		if !ok {
			return nil, cost, false, nil
		}
		schemes[i] = corepb.PQScheme(tag)
	}

	owner := tronAddrFromWord(input[:32])
	msg := input[64:96]
	combined := make([]byte, 0, 57)
	combined = append(combined, owner.Bytes()...)
	combined = append(combined, byte(permID>>24), byte(permID>>16), byte(permID>>8), byte(permID))
	combined = append(combined, msg...)
	hash := sha256.Sum256(combined)

	acc := tvm.StateDB.GetAccount(owner)
	if acc == nil {
		return pqBoolWord(false), cost, true, nil
	}
	perm := permissionByID(acc, permID)
	if perm == nil {
		return pqBoolWord(false), cost, true, nil
	}
	var totalWeight int64
	seen := make(map[tcommon.Address]struct{}, len(ecdsaSigs)+len(pqSigs))
	for _, sig := range ecdsaSigs {
		addr := recoverTronAddr(sig, hash[:])
		if addr == (tcommon.Address{}) {
			return pqBoolWord(false), cost, true, nil
		}
		if _, duplicate := seen[addr]; duplicate {
			continue
		}
		weight := permissionWeight(perm, addr)
		if weight == 0 {
			return pqBoolWord(false), cost, true, nil
		}
		totalWeight += weight
		seen[addr] = struct{}{}
	}
	for i, scheme := range schemes {
		if scheme != corepb.PQScheme_FN_DSA_512 && scheme != corepb.PQScheme_ML_DSA_44 {
			return nil, cost, false, nil
		}
		if (scheme == corepb.PQScheme_FN_DSA_512 && !tvm.cfg.FnDsa512) || (scheme == corepb.PQScheme_ML_DSA_44 && !tvm.cfg.MlDsa44) {
			return pqBoolWord(false), cost, true, nil
		}
		sig := pqSigs[i]
		if scheme == corepb.PQScheme_FN_DSA_512 {
			var valid bool
			sig, valid = falconSlotSignature(sig)
			if !valid {
				return nil, cost, false, nil
			}
		} else if len(sig) != pq.MlDsa44SignatureSize {
			return nil, cost, false, nil
		}
		addr, err := pq.Address(scheme, pqPKs[i])
		if err != nil {
			return nil, cost, false, nil
		}
		if _, duplicate := seen[addr]; duplicate {
			continue
		}
		weight := permissionWeight(perm, addr)
		if weight == 0 || !pq.Verify(scheme, pqPKs[i], hash[:], sig) {
			return pqBoolWord(false), cost, true, nil
		}
		totalWeight += weight
		seen[addr] = struct{}{}
	}
	return pqBoolWord(totalWeight >= perm.GetThreshold()), cost, true, nil
}

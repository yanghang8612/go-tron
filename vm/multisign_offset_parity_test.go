package vm

import (
	"crypto/sha256"
	"errors"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// D-2 parity: under allow_tvm_selfdestruct_restriction (#94) java reads the
// signature/address array *size* and each element *offset* by WORD INDEX
// (words[byteOffset/32]), not by exact byte offset. java-tron golden standard:
//
//   PrecompiledContracts.java:1067  (0x0a size)   words[words[3].intValueSafe()/WORD_SIZE]
//   PrecompiledContracts.java:1073  (0x0a array)  extractSigArray(words, words[3].intValueSafe()/WORD_SIZE, rawData)
//   PrecompiledContracts.java:1166-1167 (0x09 size) words[words[1|2].intValueSafe()/WORD_SIZE]
//   PrecompiledContracts.java:1173-1177 (0x09 array)
//   PrecompiledContracts.java:414-426  extractSigArray   (in-loop words[offset+i+1].intValueSafe()/32, element read word-aligned)
//   PrecompiledContracts.java:399-412  extractBytesArray  (restriction-OFF path)
//   PrecompiledContracts.java:390-397  extractBytes32Array (0x09 address array)
//   DataWord.java:133-141 parseArray (words.length = data.length/32, truncating; words[idx] AIOOBE)
//   DataWord.java:222-229 intValueSafe (bytesOccupied>4 || intValue<0 -> Integer.MAX_VALUE)
//
// Crucial asymmetry on out-of-bounds word index:
//   0x0a: the size/array reads happen OUTSIDE ValidateMultiSign.execute's inner
//         try (lines 1066-1074 precede the try at 1082) -> AIOOBE escapes the
//         precompile -> VM spendAllEnergy + contractResult.UNKNOWN(13).
//   0x09: doExecute runs entirely inside execute()'s try/catch(Throwable)
//         (1146-1153) -> any AIOOBE -> Pair.of(true, new byte[32]) (zero success).

const javaIntMax = int64(2147483647) // Integer.MAX_VALUE

// javaIntValueSafe models DataWord.intValueSafe over a 32-byte word.
func javaIntValueSafe(word []byte) int64 {
	w := make([]byte, 32)
	copy(w, word) // zero-padded if short, like parseArray's fixed 32-byte words
	firstNonZero := -1
	for i := 0; i < 32; i++ {
		if w[i] != 0 {
			firstNonZero = i
			break
		}
	}
	bytesOccupied := 0
	if firstNonZero != -1 {
		bytesOccupied = 32 - firstNonZero
	}
	// intValue(): low 32 bits of the big-endian word.
	intValue := int32(uint32(w[28])<<24 | uint32(w[29])<<16 | uint32(w[30])<<8 | uint32(w[31]))
	if bytesOccupied > 4 || intValue < 0 {
		return javaIntMax
	}
	return int64(intValue)
}

func wordAt(data []byte, wordIdx int) ([]byte, bool) {
	start := wordIdx * 32
	if start < 0 || start+32 > len(data) {
		return nil, false
	}
	return data[start : start+32], true
}

// javaExtractSigArray models PrecompiledContracts.extractSigArray and signals an
// out-of-bounds word access (AIOOBE) via oob=true. Element reads use 65-byte
// copyOfRange; an out-of-range range also reports oob (java Arrays.copyOfRange
// throws ArrayIndexOutOfBoundsException when from < 0 or from > data.length, but
// PADS with zeroes when to > data.length).
func javaExtractSigArray(data []byte, offset int) (sigs [][]byte, oob bool) {
	words := len(data) / 32
	if offset > words-1 {
		return nil, false // java guard -> empty array, NOT oob
	}
	w, _ := wordAt(data, offset)
	length := int(javaIntValueSafe(w))
	out := make([][]byte, 0, length)
	for i := 0; i < length; i++ {
		ew, ok := wordAt(data, offset+i+1)
		if !ok {
			return nil, true // words[offset+i+1] AIOOBE
		}
		bytesOffset := int(javaIntValueSafe(ew) / 32)
		start := (bytesOffset + offset + 2) * 32
		// Arrays.copyOfRange(data, start, start+65): from<0 or from>len -> AIOOBE.
		if start < 0 || start > len(data) {
			return nil, true
		}
		sig := make([]byte, 65)
		if start < len(data) {
			copy(sig, data[start:]) // pads with zero when start+65 > len(data)
		}
		out = append(out, sig)
	}
	return out, false
}

// ── 0x0a ValidateMultiSign word-index parity ─────────────────────────────────

// buildValidateMultiSignWordAligned constructs a fully word-aligned 0x0a input
// where the sigs-array size word lives at an unaligned byte offset whose WORD
// INDEX (sigsByteOffset/32) selects a valid count word. Byte-exact decoding
// (the old gtron behaviour) reads a different size; word-index decoding (java)
// reads `n`. With sigsByteOffset chosen non-32-aligned, the two disagree.
func buildValidateMultiSignWordAligned(owner tcommon.Address, permID int64, msgData []byte, sigs [][]byte, sigsByteOffset int) []byte {
	n := len(sigs)
	wordIdx := sigsByteOffset / 32
	// Layout: header words 0..3, then padding up to the count word at wordIdx,
	// then the per-element offset table, then the 65-byte sig payloads.
	countWordPos := wordIdx * 32
	tablePos := countWordPos + 32
	payloadPos := tablePos + 32*n
	totalLen := payloadPos + 65*n
	if rem := totalLen % 32; rem != 0 {
		totalLen += 32 - rem
	}
	if totalLen < 4*32 {
		totalLen = 4 * 32
	}
	input := make([]byte, totalLen)
	copy(input[0:32], stakingAddrWord(owner))
	copy(input[32:64], int64ToBytes32(permID))
	copy(input[64:96], msgData)
	copy(input[96:128], int64ToBytes32(int64(sigsByteOffset)))
	copy(input[countWordPos:countWordPos+32], int64ToBytes32(int64(n)))
	for i := range sigs {
		// element i payload at payloadPos + i*65 = (bytesOffset+offset+2)*32
		// => bytesOffset = (payloadPos + i*65)/32 - wordIdx - 2, then java
		// re-multiplies bytesOffset*32, so the relative word offset is what
		// matters. Encode bytesOffset as a *byte* value = wordIdx_elem*32.
		start := payloadPos + i*65
		bytesOffsetWords := start/32 - wordIdx - 2
		copy(input[tablePos+32*i:tablePos+32*(i+1)], int64ToBytes32(int64(bytesOffsetWords*32)))
		copy(input[start:start+65], sigs[i])
	}
	return input
}

func TestValidateMultiSign_SizeReadUsesWordIndex_D2(t *testing.T) {
	tvm, _, _ := newTestTVMWithDB(t)
	tvm.cfg.SelfdestructRestrict = true

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	owner := crypto.PubkeyToAddress(&key.PublicKey)
	tvm.StateDB.CreateAccount(owner, corepb.AccountType_Normal)
	tvm.StateDB.SetPermissions(owner, coretypes.MakeDefaultOwnerPermission(owner), nil, nil)

	msgData := make([]byte, 32)
	msgData[31] = 0x7b
	hash := hashForMultiSign(owner, 0, msgData)
	sig, err := crypto.Sign(hash[:], key)
	if err != nil {
		t.Fatal(err)
	}

	// sigsByteOffset = 129 (non-32-aligned). Word index = 129/32 = 4, so the
	// count word lives at byte 128. Byte-exact decode reads input[129:161]
	// (low8 spans into the per-element table -> a large bogus size -> DATA_FALSE);
	// word-index decode reads input[128:160] = the real count = 1.
	input := buildValidateMultiSignWordAligned(owner, 0, msgData, [][]byte{sig}, 129)

	// Sanity: the byte-exact size the OLD code would read must differ from 1 and
	// exceed MAX_SIZE(5), so the red path is genuinely DATA_FALSE (not a fluke
	// agreement). This mirrors the reproduction (443 > 5).
	if got := int(parseUint64FromWord(input, 129)); got <= maxMultiSignSize {
		t.Fatalf("fixture invalid: byte-exact size %d must exceed MAX_SIZE so red != green", got)
	}

	// Cross-check the java golden model: word-index path verifies exactly 1 sig.
	javaSigs, oob := javaExtractSigArray(input, 129/32)
	if oob {
		t.Fatalf("java model: unexpected oob for aligned fixture")
	}
	if len(javaSigs) != 1 {
		t.Fatalf("java model: extractSigArray len = %d, want 1", len(javaSigs))
	}

	out, _, success, err := (&validateMultiSign{}).RunWithStatus(tvm, zeroCaller, input, 1500)
	if err != nil {
		t.Fatalf("unexpected vm error: %v", err)
	}
	if !success {
		t.Fatalf("want success=true")
	}
	// Threshold-1 owner permission signed by `key` -> validates -> dataOne.
	if len(out) != 32 || out[31] != 1 {
		t.Fatalf("word-index size read should validate the single sig, got %x", out)
	}
}

func TestValidateMultiSign_OutOfBoundsWordIndex_SpendAll_D2(t *testing.T) {
	tvm, _, _ := newTestTVMWithDB(t)
	tvm.cfg.SelfdestructRestrict = true

	// 4-word input (128 bytes) clears the words[0..3] header read, but
	// word[3] = sigsOffset points so its word index (sigsOffset/32) is >=
	// words.length (=4). java: words[idx] AIOOBE OUTSIDE the try -> spendAll +
	// UNKNOWN. gtron mirror = ErrPrecompileUnknown.
	input := make([]byte, 4*32)
	copy(input[0:32], make([]byte, 32))
	copy(input[32:64], int64ToBytes32(0))
	copy(input[64:96], make([]byte, 32))
	copy(input[96:128], int64ToBytes32(4*32)) // word index 4 >= words.length 4 -> oob

	_, _, success, err := (&validateMultiSign{}).RunWithStatus(tvm, zeroCaller, input, 1500)
	if !errors.Is(err, ErrPrecompileUnknown) {
		t.Fatalf("oob word index: err = %v, want ErrPrecompileUnknown (java spendAll/UNKNOWN)", err)
	}
	if success {
		t.Fatalf("oob word index must not succeed")
	}
	if !shouldPropagateCallError(ErrPrecompileUnknown) {
		t.Fatal("ErrPrecompileUnknown must propagate from sub-calls")
	}
}

func TestValidateMultiSign_InLoopOutOfBoundsElementOffset_SpendAll_D2(t *testing.T) {
	tvm, _, _ := newTestTVMWithDB(t)
	tvm.cfg.SelfdestructRestrict = true

	// Size word in-bounds and <= MAX_SIZE, but the per-element offset word
	// (words[offset+i+1]) lies past the truncated word array -> extractSigArray
	// AIOOBE, which in java is OUTSIDE ValidateMultiSign's try -> spendAll/UNKNOWN.
	// Build: header (4 words) + count word at the sigsOffset word index, with
	// count = 1, but NO per-element table word present (input ends right after
	// the count word).
	const sigsOffset = 128               // word index 4
	input := make([]byte, sigsOffset+32) // ends exactly after the count word at idx 4
	copy(input[96:128], int64ToBytes32(int64(sigsOffset)))
	copy(input[sigsOffset:sigsOffset+32], int64ToBytes32(1)) // count = 1

	// java model: extractSigArray reads words[offset+1] = words[5], oob.
	if _, oob := javaExtractSigArray(input, sigsOffset/32); !oob {
		t.Fatalf("java model: expected oob for missing element-offset word")
	}

	_, _, success, err := (&validateMultiSign{}).RunWithStatus(tvm, zeroCaller, input, 1500)
	if !errors.Is(err, ErrPrecompileUnknown) {
		t.Fatalf("in-loop oob: err = %v, want ErrPrecompileUnknown", err)
	}
	if success {
		t.Fatalf("in-loop oob must not succeed")
	}
}

// The sigs-offset word goes through DataWord.intValueSafe BEFORE /WORD_SIZE.
// A word whose HIGH bytes are non-zero but whose low-8 bytes are a small,
// in-bounds value must saturate to Integer.MAX_VALUE → a huge OOB word index →
// AIOOBE → spendAll/UNKNOWN. A naive low-8-byte read would instead pick a small
// in-bounds index and silently decode a (java-incompatible) result.
func TestValidateMultiSign_OffsetWordSaturates_SpendAll_D2(t *testing.T) {
	tvm, _, _ := newTestTVMWithDB(t)
	tvm.cfg.SelfdestructRestrict = true

	// 6-word input so a low-8 offset of 128 (word index 4) would be in bounds.
	input := make([]byte, 6*32)
	copy(input[96:128], int64ToBytes32(128)) // low-8 bytes = 128 -> idx 4 (in bounds)
	input[96] = 0x01                         // set a HIGH byte -> intValueSafe saturates
	// place a benign small count at word 4 so a low-8 (non-saturating) read would
	// proceed and (wrongly) succeed; the saturating read must AIOOBE instead.
	copy(input[128:160], int64ToBytes32(1))

	// java model: intValueSafe of word[3] saturates -> idx = MAX/32 -> oob.
	if got := javaIntValueSafe(input[96:128]); got != javaIntMax {
		t.Fatalf("java model: offset word should saturate, got %d", got)
	}

	_, _, success, err := (&validateMultiSign{}).RunWithStatus(tvm, zeroCaller, input, 1500)
	if !errors.Is(err, ErrPrecompileUnknown) {
		t.Fatalf("saturating offset word: err = %v, want ErrPrecompileUnknown", err)
	}
	if success {
		t.Fatalf("saturating offset word must not succeed")
	}
}

// ── 0x09 BatchValidateSign word-index parity ─────────────────────────────────

// buildBatchValidateSignWordAligned builds a word-aligned 0x09 input where the
// sigs-array and addrs-array sizes are pointed at by NON-32-aligned byte offsets
// whose word index (offset/32) selects the real count word. Layout (word units):
//
//	w0: hash
//	w1: sigsByteOffset (= sigsWordIdx*32 + 1, deliberately unaligned)
//	w2: addrsByteOffset (= addrsWordIdx*32 + 1, deliberately unaligned)
//	[sigs region]  count word @ sigsWordIdx, offset table (n words), 65-byte payloads
//	[addrs region] count word @ addrsWordIdx, n inlined 32-byte address words
//
// The two regions are placed back to back after the header so nothing overlaps.
func buildBatchValidateSignWordAligned(hash []byte, sigs [][]byte, addrs []tcommon.Address) []byte {
	n := len(sigs)
	// sigs region starts at word 3.
	sigsWordIdx := 3
	sigCountPos := sigsWordIdx * 32
	sigTablePos := sigCountPos + 32
	sigPayloadPos := sigTablePos + 32*n
	sigEnd := sigPayloadPos + 65*n
	if rem := sigEnd % 32; rem != 0 {
		sigEnd += 32 - rem
	}
	// addrs region starts right after the sigs region.
	addrsWordIdx := sigEnd / 32
	addrCountPos := addrsWordIdx * 32
	addrTablePos := addrCountPos + 32 // bytes32[] inlines elements after the count
	totalLen := addrTablePos + 32*len(addrs)
	if rem := totalLen % 32; rem != 0 {
		totalLen += 32 - rem
	}
	input := make([]byte, totalLen)
	copy(input[0:32], hash)
	copy(input[32:64], int64ToBytes32(int64(sigsWordIdx*32+1)))  // unaligned
	copy(input[64:96], int64ToBytes32(int64(addrsWordIdx*32+1))) // unaligned
	copy(input[sigCountPos:sigCountPos+32], int64ToBytes32(int64(n)))
	for i := range sigs {
		start := sigPayloadPos + i*65
		bytesOffsetWords := start/32 - sigsWordIdx - 2
		copy(input[sigTablePos+32*i:sigTablePos+32*(i+1)], int64ToBytes32(int64(bytesOffsetWords*32)))
		copy(input[start:start+65], sigs[i])
	}
	copy(input[addrCountPos:addrCountPos+32], int64ToBytes32(int64(len(addrs))))
	for i, a := range addrs {
		copy(input[addrTablePos+32*i:addrTablePos+32*(i+1)], stakingAddrWord(a))
	}
	return input
}

func TestBatchValidateSign_SizeReadUsesWordIndex_D2(t *testing.T) {
	tvm, _, _ := newTestTVMWithDB(t)
	tvm.cfg.SelfdestructRestrict = true

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	addr := crypto.PubkeyToAddress(&key.PublicKey)

	hash := make([]byte, 32)
	hash[31] = 0x99
	sig, err := crypto.Sign(hash, key)
	if err != nil {
		t.Fatal(err)
	}

	input := buildBatchValidateSignWordAligned(hash, [][]byte{sig}, []tcommon.Address{addr})
	sigsByteOffset := int(parseUint64FromWord(input, 32))

	// Red guard: the byte-exact size the OLD code reads (at the unaligned byte
	// offset) must differ from the word-index size, so red != green. For an
	// unaligned offset the byte read straddles the count word and the next word.
	byteExactSize := int(parseUint64FromWord(input, sigsByteOffset))
	wordIdxSize, _ := wordIntValueSafe(input, sigsByteOffset/32)
	if byteExactSize == wordIdxSize {
		t.Fatalf("fixture invalid: byte-exact (%d) and word-index (%d) sizes must differ", byteExactSize, wordIdxSize)
	}
	if wordIdxSize != 1 {
		t.Fatalf("fixture invalid: word-index size = %d, want 1", wordIdxSize)
	}

	out, _, success, err := (&batchValidateSign{}).RunWithStatus(tvm, zeroCaller, input, 1500)
	if err != nil {
		t.Fatalf("unexpected vm error: %v", err)
	}
	if !success {
		t.Fatalf("want success=true")
	}
	// The single (sig, addr) pair matches -> res[0] = 1.
	if len(out) != 32 || out[0] != 1 {
		t.Fatalf("word-index size read should recover the matching signer, got %x", out)
	}
}

func TestBatchValidateSign_OutOfBoundsWordIndex_ZeroSuccess_D2(t *testing.T) {
	tvm, _, _ := newTestTVMWithDB(t)
	tvm.cfg.SelfdestructRestrict = true

	// sigsOffset word index >= words.length -> java words[idx] AIOOBE, but
	// 0x09's whole doExecute is wrapped in execute()'s try/catch(Throwable) ->
	// Pair.of(true, new byte[32]). gtron mirror = (32-zero, success=true), NO error.
	input := make([]byte, 4*32) // words.length = 4
	hash := make([]byte, 32)
	copy(input[0:32], hash)
	copy(input[32:64], int64ToBytes32(4*32)) // sigs size word index 4 -> oob
	copy(input[64:96], int64ToBytes32(64))   // addrs size offset word index 2 (in bounds)

	out, _, success, err := (&batchValidateSign{}).RunWithStatus(tvm, zeroCaller, input, 0)
	if err != nil {
		t.Fatalf("0x09 oob must be caught, got err = %v", err)
	}
	if !success {
		t.Fatalf("0x09 oob must report success=true (java outer catch)")
	}
	if len(out) != 32 {
		t.Fatalf("0x09 oob payload must be a 32-byte word, got len %d", len(out))
	}
	for i, b := range out {
		if b != 0 {
			t.Fatalf("0x09 oob payload must be all-zero, byte %d = %d", i, b)
		}
	}
}

// ── No-regression: aligned offsets unchanged across the fork gate ─────────────

func TestSignaturePrecompiles_AlignedOffsets_NoRegression_D2(t *testing.T) {
	tvm, _, _ := newTestTVMWithDB(t)
	tvm.cfg.SelfdestructRestrict = true

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	owner := crypto.PubkeyToAddress(&key.PublicKey)
	tvm.StateDB.CreateAccount(owner, corepb.AccountType_Normal)
	tvm.StateDB.SetPermissions(owner, coretypes.MakeDefaultOwnerPermission(owner), nil, nil)

	msgData := make([]byte, 32)
	msgData[31] = 0x55
	hash := hashForMultiSign(owner, 0, msgData)
	sig, err := crypto.Sign(hash[:], key)
	if err != nil {
		t.Fatal(err)
	}

	// Standard aligned sigsOffset = 160 (word index 5, exactly the byte offset)
	// — the existing validateMultiSignInputN layout. Must keep passing.
	input := validateMultiSignInputN(owner, 0, msgData, [][]byte{sig})
	out, _, success, err := (&validateMultiSign{}).RunWithStatus(tvm, zeroCaller, input, 1500)
	if err != nil {
		t.Fatalf("aligned 0x0a vm error: %v", err)
	}
	if !success || len(out) != 32 || out[31] != 1 {
		t.Fatalf("aligned 0x0a must still validate, success=%v out=%x", success, out)
	}

	// Sanity: for an aligned offset the byte-exact and word-index size reads
	// coincide (sigsOffset=160 is a multiple of 32), so both old and new agree.
	if int(parseUint64FromWord(input, 160)) != 1 {
		t.Fatalf("aligned size word must read 1")
	}
}

func TestValidateMultiSign_RestrictionOff_ByteOffsetUnchanged_D2(t *testing.T) {
	tvm, _, _ := newTestTVMWithDB(t)
	tvm.cfg.SelfdestructRestrict = false

	// restriction OFF must keep the legacy extractBytesArray semantics. Reuse
	// the aligned fixed-65 layout which the legacy bytes[] parser rejects
	// (DATA_FALSE), matching java extractBytesArray on this shape.
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	owner := crypto.PubkeyToAddress(&key.PublicKey)
	tvm.StateDB.CreateAccount(owner, corepb.AccountType_Normal)
	tvm.StateDB.SetPermissions(owner, coretypes.MakeDefaultOwnerPermission(owner), nil, nil)

	msgData := make([]byte, 32)
	msgData[31] = 0x55
	hash := sha256.Sum256(append(append(owner[:], 0, 0, 0, 0), msgData...))
	sig, err := crypto.Sign(hash[:], key)
	if err != nil {
		t.Fatal(err)
	}
	input := validateMultiSignInputN(owner, 0, msgData, [][]byte{sig})

	out, _, success, err := (&validateMultiSign{}).RunWithStatus(tvm, zeroCaller, input, 1500)
	if err != nil {
		t.Fatalf("restriction-off 0x0a vm error: %v", err)
	}
	if !success || len(out) != 32 || out[31] != 0 {
		t.Fatalf("restriction-off legacy parser should DATA_FALSE on fixed65 layout, success=%v out=%x", success, out)
	}
}

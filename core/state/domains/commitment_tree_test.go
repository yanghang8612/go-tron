package domains

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"golang.org/x/crypto/sha3"
)

func referenceLegacyKeccak(parts ...[]byte) common.Hash {
	h := sha3.NewLegacyKeccak256()
	for _, part := range parts {
		_, _ = h.Write(part)
	}
	var out common.Hash
	copy(out[:], h.Sum(nil))
	return out
}

func TestCommitmentHashFastPathsMatchReference(t *testing.T) {
	key := []byte("commitment-key")
	value := []byte("commitment-value")
	var keyLen, valueLen [8]byte
	binary.BigEndian.PutUint64(keyLen[:], uint64(len(key)))
	binary.BigEndian.PutUint64(valueLen[:], uint64(len(value)))

	pathDigest := referenceLegacyKeccak(keyLen[:], key)
	if got := keyPath(key); got != pathDigest {
		t.Fatalf("keyPath = %x, want %x", got, pathDigest)
	}
	for depth := 0; depth < pathLen; depth++ {
		want := pathDigest[depth>>1] >> 4
		if depth&1 != 0 {
			want = pathDigest[depth>>1] & 0x0f
		}
		if got := pathNibble(pathDigest, depth); got != want {
			t.Fatalf("pathNibble(%d) = %x, want %x", depth, got, want)
		}
	}

	wantLeaf := referenceLegacyKeccak([]byte{0x00}, keyLen[:], key, valueLen[:], value)
	if got := leafValueHash(key, value); got != wantLeaf {
		t.Fatalf("leafValueHash = %x, want %x", got, wantLeaf)
	}

	hashChild := common.Hash{0x11, 0x22}
	leafChild := common.Hash{0x33, 0x44}
	var branch BranchData
	branch.SetHashChild(2, hashChild)
	branch.SetLeafChild(9, []byte("ignored-by-node-hash"), leafChild)
	wantNode := referenceLegacyKeccak(
		[]byte{0x01},
		[]byte{0x02}, hashChild[:],
		[]byte{0x09}, leafChild[:],
	)
	if got := branch.nodeHash(); got != wantNode {
		t.Fatalf("nodeHash = %x, want %x", got, wantNode)
	}

	// Exercise the maximum-size 529-byte node preimage as well as the sparse
	// case above. Alternate child kinds; leaf keys are deliberately excluded
	// from the node hash by the commitment format.
	parts := make([][]byte, 1, 1+16*2)
	parts[0] = []byte{0x01}
	var full BranchData
	for nibble := uint8(0); nibble < 16; nibble++ {
		childHash := common.Hash{nibble, nibble + 1, nibble + 2}
		if nibble%2 == 0 {
			full.SetHashChild(nibble, childHash)
		} else {
			full.SetLeafChild(nibble, []byte{0xff, nibble}, childHash)
		}
		parts = append(parts, []byte{nibble}, childHash[:])
	}
	wantFull := referenceLegacyKeccak(parts...)
	if got := full.nodeHash(); got != wantFull {
		t.Fatalf("full nodeHash = %x, want %x", got, wantFull)
	}
}

func TestBranchDataRoundTrip(t *testing.T) {
	var b BranchData

	h := common.Hash{0xAB, 0xCD}
	b.SetHashChild(0x3, h)

	key := []byte("somekey")
	valHash := common.Hash{0x12, 0x34}
	b.SetLeafChild(0xf, key, valHash)

	enc := b.Encode()
	got, err := DecodeBranchData(enc)
	if err != nil {
		t.Fatalf("DecodeBranchData: %v", err)
	}
	if !b.Equal(got) {
		t.Fatalf("decoded branch not Equal to original")
	}
}

func TestBranchDataPathLeafRoundTrip(t *testing.T) {
	path := keyPath([]byte("state-kv-latest-v1-production-shaped-key"))
	valHash := common.Hash{0x12, 0x34}
	var branch BranchData
	branch.setLeafChildPath(0xa, path[:], valHash)

	encoded := branch.Encode()
	if got, want := len(encoded), 2+1+common.HashLength+common.HashLength; got != want {
		t.Fatalf("path-leaf encoded bytes = %d, want %d", got, want)
	}
	if encoded[2] != kindLeafPath {
		t.Fatalf("path-leaf kind = %d, want %d", encoded[2], kindLeafPath)
	}
	decoded, err := DecodeBranchData(encoded)
	if err != nil {
		t.Fatalf("DecodeBranchData: %v", err)
	}
	identity, pathOnly, gotHash := decoded.leafChildIdentityAt(0xa)
	if !pathOnly || !bytes.Equal(identity, path[:]) || gotHash != valHash {
		t.Fatalf("decoded path leaf = (pathOnly=%v identity=%x hash=%x)", pathOnly, identity, gotHash)
	}
	if !branch.Equal(decoded) {
		t.Fatal("decoded path branch not Equal to original")
	}
}

func TestBranchDataPathLeafRejectsTruncation(t *testing.T) {
	path := keyPath([]byte("long commitment key"))
	var branch BranchData
	branch.setLeafChildPath(1, path[:], common.Hash{0x77})
	encoded := branch.Encode()
	for _, cut := range []int{3, len(encoded) - 1} {
		if _, err := DecodeBranchData(encoded[:cut]); err == nil {
			t.Fatalf("DecodeBranchData accepted path leaf truncated to %d bytes", cut)
		}
	}
}

func TestBranchDataKindMaskTransitions(t *testing.T) {
	var branch BranchData
	branch.SetLeafChild(3, []byte("leaf"), common.Hash{0x11})
	if got, want := branch.leafMask(), uint16(1<<3); got != want {
		t.Fatalf("leaf mask after SetLeafChild = %#x, want %#x", got, want)
	}

	branch.SetHashChild(3, common.Hash{0x22})
	if got := branch.leafMask(); got != 0 {
		t.Fatalf("leaf mask after leaf-to-hash replacement = %#x, want 0", got)
	}
	encodedHash := branch.Encode()
	decodedHash, err := DecodeBranchData(encodedHash)
	if err != nil {
		t.Fatal(err)
	}
	if got := decodedHash.leafMask(); got != 0 {
		t.Fatalf("decoded hash leaf mask = %#x, want 0", got)
	}

	branch.SetLeafChild(3, []byte("leaf-again"), common.Hash{0x33})
	if got, want := branch.leafMask(), uint16(1<<3); got != want {
		t.Fatalf("leaf mask after hash-to-leaf replacement = %#x, want %#x", got, want)
	}
	branch.clearChild(3)
	if got := branch.leafMask(); got != 0 {
		t.Fatalf("leaf mask after clear = %#x, want 0", got)
	}
	if got := branch.presentMask(); got != 0 {
		t.Fatalf("presence mask after clear = %#x, want 0", got)
	}
}

func TestBranchDataEncodeAllocatesExactCapacity(t *testing.T) {
	var b BranchData
	for nibble := uint8(0); nibble < 16; nibble++ {
		keyLen := 1 << (nibble % 9)
		b.SetLeafChild(nibble, bytes.Repeat([]byte{nibble}, keyLen), common.Hash{nibble})
	}

	encoded := b.Encode()
	if cap(encoded) != len(encoded) {
		t.Fatalf("encoded capacity = %d, want exact length %d", cap(encoded), len(encoded))
	}
	if _, err := DecodeBranchData(encoded); err != nil {
		t.Fatalf("DecodeBranchData: %v", err)
	}
}

func TestBranchDataDecodeLeafKeyOwnership(t *testing.T) {
	var source BranchData
	source.SetLeafChild(4, []byte("borrow-me"), common.Hash{0xaa})
	source.SetLeafChild(9, []byte("pack-with-me"), common.Hash{0xbb})
	encoded := source.Encode()

	var copied, borrowed BranchData
	if err := DecodeBranchDataInto(encoded, &copied); err != nil {
		t.Fatal(err)
	}
	if err := decodeBranchDataIntoNoCopy(encoded, &borrowed); err != nil {
		t.Fatal(err)
	}
	keyOffset := bytes.Index(encoded, []byte("borrow-me"))
	if keyOffset < 0 {
		t.Fatal("encoded leaf key not found")
	}
	encoded[keyOffset] = 'B'

	copiedKey, _ := copied.leafChildAt(4)
	if string(copiedKey) != "borrow-me" {
		t.Fatalf("public decoder retained input: %q", copiedKey)
	}
	borrowedKey, _ := borrowed.leafChildAt(4)
	if string(borrowedKey) != "Borrow-me" {
		t.Fatalf("no-copy decoder did not alias input: %q", borrowedKey)
	}
	secondOffset := bytes.Index(encoded, []byte("pack-with-me"))
	if secondOffset < 0 {
		t.Fatal("second encoded leaf key not found")
	}
	encoded[secondOffset] = 'P'
	secondCopiedKey, _ := copied.leafChildAt(9)
	if string(secondCopiedKey) != "pack-with-me" {
		t.Fatalf("packed public decoder retained input: %q", secondCopiedKey)
	}
	secondBorrowedKey, _ := borrowed.leafChildAt(9)
	if string(secondBorrowedKey) != "Pack-with-me" {
		t.Fatalf("packed no-copy decoder did not alias input: %q", secondBorrowedKey)
	}
}

func TestBranchDataSetLeafChildOwnsKey(t *testing.T) {
	key := []byte("owned-leaf")
	var branch BranchData
	branch.SetLeafChild(3, key, common.Hash{0x33})
	key[0] = 'X'

	stored, _ := branch.leafChildAt(3)
	if string(stored) != "owned-leaf" {
		t.Fatalf("SetLeafChild retained caller key: %q", stored)
	}
}

func TestDecodeBranchDataIntoArenaReusesStorageAndPreservesValueCopies(t *testing.T) {
	var firstSource BranchData
	firstSource.SetLeafChild(2, []byte("first-leaf"), common.Hash{0x22})
	firstEncoded := firstSource.Encode()

	var arena []byte
	var decoded BranchData
	if err := decodeBranchDataIntoArena(firstEncoded, &decoded, &arena); err != nil {
		t.Fatal(err)
	}
	firstCopy := decoded
	firstKey, _ := firstCopy.leafChildAt(2)
	if cap(firstKey) != len(firstKey) {
		t.Fatalf("leaf key capacity = %d, want length %d", cap(firstKey), len(firstKey))
	}

	// Force arena growth without resetting it. Earlier BranchData value copies
	// must keep their old immutable backing even if slices.Grow moves the arena.
	var secondSource BranchData
	secondSource.SetLeafChild(7, bytes.Repeat([]byte{0x77}, cap(arena)+1), common.Hash{0x77})
	if err := decodeBranchDataIntoArena(secondSource.Encode(), &decoded, &arena); err != nil {
		t.Fatal(err)
	}
	clear(firstEncoded)
	if got := string(firstKey); got != "first-leaf" {
		t.Fatalf("earlier value copy changed after arena growth: %q", got)
	}

	// Reset is legal only after all prior fold outputs are dead. Once warmed,
	// repeatedly decoding into the same destination and arena allocates nothing.
	secondEncoded := secondSource.Encode()
	arena = arena[:0]
	if err := decodeBranchDataIntoArena(secondEncoded, &decoded, &arena); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		arena = arena[:0]
		if err := decodeBranchDataIntoArena(secondEncoded, &decoded, &arena); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("warmed fold arena allocated %.2f objects, want 0", allocs)
	}
}

func TestDecodeBranchDataIntoNoCopyReuseClearsStaleLeafKeys(t *testing.T) {
	var dst BranchData
	dst.SetLeafChild(2, []byte("overwritten-leaf"), common.Hash{0x22})
	dst.SetLeafChild(7, []byte("absent-old-leaf"), common.Hash{0x77})

	var source BranchData
	source.SetHashChild(2, common.Hash{0xaa})
	encoded := source.Encode()
	if err := decodeBranchDataIntoNoCopy(encoded, &dst); err != nil {
		t.Fatal(err)
	}
	if got, want := dst.presentMask(), uint16(1<<2); got != want {
		t.Fatalf("presence mask = %#x, want %#x", got, want)
	}
	if dst.children[2].leafKey != "" {
		t.Fatalf("hash replacement retained old leaf key: %q", dst.children[2].leafKey)
	}
	if dst.children[7].leafKey != "" {
		t.Fatalf("absent slot retained old leaf key: %q", dst.children[7].leafKey)
	}
}

func TestDecodeBranchDataIntoNoCopyErrorClearsPartialState(t *testing.T) {
	var source BranchData
	source.SetLeafChild(1, []byte("first"), common.Hash{0x11})
	source.SetLeafChild(2, []byte("second"), common.Hash{0x22})
	encoded := source.Encode()

	var dst BranchData
	dst.SetLeafChild(9, []byte("old"), common.Hash{0x99})
	if err := decodeBranchDataIntoNoCopy(encoded[:len(encoded)-1], &dst); err == nil {
		t.Fatal("truncated no-copy decode unexpectedly succeeded")
	}
	if dst.presentMask() != 0 {
		t.Fatalf("partial decode presence mask = %#x, want 0", dst.presentMask())
	}
	for i := range dst.children {
		child := &dst.children[i]
		if child.valueHash != (common.Hash{}) || child.leafKey != "" {
			t.Fatalf("partial decode retained child %d: %+v", i, child)
		}
	}
}

func TestBranchDataDeterministicAndProperty(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < 50; i++ {
		// Build a reference branch with random children.
		var ref BranchData
		for nibble := uint8(0); nibble < 16; nibble++ {
			if rng.Intn(2) == 0 {
				continue
			}
			if rng.Intn(2) == 0 {
				var h common.Hash
				rng.Read(h[:])
				ref.SetHashChild(nibble, h)
			} else {
				keyLen := rng.Intn(32) + 1
				key := make([]byte, keyLen)
				rng.Read(key)
				var vh common.Hash
				rng.Read(vh[:])
				ref.SetLeafChild(nibble, key, vh)
			}
		}

		// Encode → decode → must be Equal.
		enc := ref.Encode()
		got, err := DecodeBranchData(enc)
		if err != nil {
			t.Fatalf("iter %d: DecodeBranchData: %v", i, err)
		}
		if !ref.Equal(got) {
			t.Fatalf("iter %d: decoded branch not Equal", i)
		}

		// Insert same children in a different (reverse) order into a second branch;
		// Encode must be byte-identical.
		var ref2 BranchData
		for nibble := int(15); nibble >= 0; nibble-- {
			c := ref.children[nibble]
			if !ref.childPresent(uint8(nibble)) {
				continue
			}
			if ref.childKindAt(uint8(nibble)) == kindHash {
				ref2.SetHashChild(uint8(nibble), c.valueHash)
			} else {
				ref2.SetLeafChild(uint8(nibble), leafKeyBytes(c.leafKey), c.valueHash)
			}
		}
		enc2 := ref2.Encode()
		if !bytes.Equal(enc, enc2) {
			t.Fatalf("iter %d: encoding not deterministic: enc=%x enc2=%x", i, enc, enc2)
		}
	}
}

func TestReturnOpsBufClearsBorrowedKeys(t *testing.T) {
	buf := make([]op, 1, 4)
	buf[0] = op{
		path:    common.Hash{1},
		key:     []byte("borrowed-update-key"),
		valHash: common.Hash{2},
		delete:  true,
	}
	backing := buf
	returnOpsBuf(&buf)

	if len(buf) != 0 {
		t.Fatalf("returned buffer len = %d, want 0", len(buf))
	}
	if backing[0].key != nil {
		t.Fatalf("returned buffer retained borrowed key: %+v", backing[0])
	}
}

func TestReturnOpsBufDropsOversizedBuffer(t *testing.T) {
	buf := make([]op, 1, maxPooledOps+1)
	buf[0].key = []byte("borrowed-update-key")
	backing := buf
	returnOpsBuf(&buf)

	if buf != nil {
		t.Fatalf("oversized returned buffer cap = %d, want nil", cap(buf))
	}
	if backing[0].path != (common.Hash{}) || backing[0].key != nil ||
		backing[0].valHash != (common.Hash{}) || backing[0].delete {
		t.Fatalf("oversized buffer retained op references: %+v", backing[0])
	}
}

func TestBranchDataDecodeSafety(t *testing.T) {
	var b BranchData
	b.SetHashChild(0x1, common.Hash{0x11})
	b.SetLeafChild(0x5, []byte("key"), common.Hash{0x55})
	valid := b.Encode()

	// Truncate at every possible length — must not panic.
	for i := 0; i < len(valid); i++ {
		_, err := DecodeBranchData(valid[:i])
		if err == nil {
			// Only the full-length decode should succeed.
			t.Fatalf("truncated at %d bytes unexpectedly succeeded", i)
		}
	}

	// Garbage bytes.
	if _, err := DecodeBranchData([]byte{0xFF, 0xFF, 0xFF, 0x00}); err == nil {
		t.Fatal("garbage decode should fail")
	}

	// Trailing bytes after valid data.
	trailing := append(append([]byte{}, valid...), 0x00)
	if _, err := DecodeBranchData(trailing); err == nil {
		t.Fatal("trailing bytes should fail")
	}

	// Invalid kind byte.
	bad := append([]byte{}, valid...)
	// After the 2-byte childMask, the first child entry starts at byte 2.
	// Set kind to 0xFF.
	bad[2] = 0xFF
	if _, err := DecodeBranchData(bad); err == nil {
		t.Fatal("invalid kind byte should fail")
	}
}

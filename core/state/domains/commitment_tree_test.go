package domains

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

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
			if !c.present {
				continue
			}
			if c.kind == 0 {
				ref2.SetHashChild(uint8(nibble), c.hashVal)
			} else {
				ref2.SetLeafChild(uint8(nibble), c.leafKey, c.leafValHash)
			}
		}
		enc2 := ref2.Encode()
		if !bytes.Equal(enc, enc2) {
			t.Fatalf("iter %d: encoding not deterministic: enc=%x enc2=%x", i, enc, enc2)
		}
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

package rawdb

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
)

func TestSectionBloom_RoundTrip(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	bloom := []byte{0xde, 0xad, 0xbe, 0xef}

	if ReadSectionBloom(db, 3, 42) != nil {
		t.Fatal("absent: read returned non-nil")
	}
	if err := WriteSectionBloom(db, 3, 42, bloom); err != nil {
		t.Fatal(err)
	}
	got := ReadSectionBloom(db, 3, 42)
	if !bytes.Equal(got, bloom) {
		t.Fatalf("roundtrip: got %x, want %x", got, bloom)
	}
	if err := DeleteSectionBloom(db, 3, 42); err != nil {
		t.Fatal(err)
	}
	if ReadSectionBloom(db, 3, 42) != nil {
		t.Fatal("after delete: still present")
	}
}

func TestSectionBloom_CompositeKey(t *testing.T) {
	// Java encodes (section, bitIndex) as Long.toHexString(section*1e6 + bitIndex), ASCII.
	// Pin the wire layout so a future capture/replay diff isn't masked.
	k := sectionBloomKey(3, 42)
	want := []byte("sb-2dc6ea")
	if !bytes.Equal(k, want) {
		t.Fatalf("key: got %q, want %q", k, want)
	}
}

func TestSectionBloomBitSetCodec(t *testing.T) {
	bitset := setSectionBloomBit(nil, 0)
	bitset = setSectionBloomBit(bitset, 2047)
	encoded, err := EncodeSectionBloomBitSet(bitset)
	if err != nil {
		t.Fatalf("EncodeSectionBloomBitSet: %v", err)
	}
	decoded, err := DecodeSectionBloomBitSet(encoded)
	if err != nil {
		t.Fatalf("DecodeSectionBloomBitSet: %v", err)
	}
	if !bytes.Equal(decoded, bitset) {
		t.Fatalf("decoded bitset = %x, want %x", decoded, bitset)
	}
}

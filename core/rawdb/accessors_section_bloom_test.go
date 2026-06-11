package rawdb

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
)

func TestSectionBloom_RoundTrip(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
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

func TestSectionBloom_ColdFallback(t *testing.T) {
	coldBitset := setSectionBloomBit(nil, 7)
	coldEncoded, err := EncodeSectionBloomBitSet(coldBitset)
	if err != nil {
		t.Fatalf("EncodeSectionBloomBitSet cold: %v", err)
	}
	hotBitset := setSectionBloomBit(nil, 9)
	hotEncoded, err := EncodeSectionBloomBitSet(hotBitset)
	if err != nil {
		t.Fatalf("EncodeSectionBloomBitSet hot: %v", err)
	}

	db := NewMemoryChainDB()
	db.SetSectionBloomReader(fakeSectionBloomReader{
		rows: map[[2]uint64][]byte{
			{3, 42}: coldEncoded,
		},
	})
	bitset, ok, err := ReadSectionBloomBitSet(db, 3, 42)
	if err != nil || !ok || !SectionBloomBitSetHas(bitset, 7) {
		t.Fatalf("cold ReadSectionBloomBitSet = %x/%v/%v, want bit 7", bitset, ok, err)
	}

	if err := WriteSectionBloom(db, 3, 42, hotEncoded); err != nil {
		t.Fatalf("WriteSectionBloom hot: %v", err)
	}
	bitset, ok, err = ReadSectionBloomBitSet(db, 3, 42)
	if err != nil || !ok || !SectionBloomBitSetHas(bitset, 9) || SectionBloomBitSetHas(bitset, 7) {
		t.Fatalf("hot ReadSectionBloomBitSet = %x/%v/%v, want hot bit 9 only", bitset, ok, err)
	}
}

func TestIterateSectionBloomRows(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if err := WriteSectionBloom(db, 3, 42, []byte{0xaa}); err != nil {
		t.Fatalf("WriteSectionBloom 3/42: %v", err)
	}
	if err := WriteSectionBloom(db, 4, 7, []byte{0xbb}); err != nil {
		t.Fatalf("WriteSectionBloom 4/7: %v", err)
	}
	var rows []struct {
		section  uint64
		bitIndex uint64
		value    []byte
	}
	if err := IterateSectionBloomRows(db, func(section, bitIndex uint64, value []byte) (bool, error) {
		rows = append(rows, struct {
			section  uint64
			bitIndex uint64
			value    []byte
		}{section: section, bitIndex: bitIndex, value: value})
		return true, nil
	}); err != nil {
		t.Fatalf("IterateSectionBloomRows: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows len = %d, want 2", len(rows))
	}
	want := map[[2]uint64][]byte{
		{3, 42}: {0xaa},
		{4, 7}:  {0xbb},
	}
	for _, row := range rows {
		if !bytes.Equal(row.value, want[[2]uint64{row.section, row.bitIndex}]) {
			t.Fatalf("row %d/%d value = %x", row.section, row.bitIndex, row.value)
		}
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

type fakeSectionBloomReader struct {
	rows map[[2]uint64][]byte
}

func (r fakeSectionBloomReader) SectionBloom(section, bitIndex uint64) ([]byte, bool, error) {
	value, ok := r.rows[[2]uint64{section, bitIndex}]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

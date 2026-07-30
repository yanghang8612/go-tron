package rawdb

import (
	"bytes"
	"compress/zlib"
	"errors"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
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

func TestChainDBSectionBloomReaderInterfaceUsesComposedView(t *testing.T) {
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
	var rawReader SectionBloomReader = db
	var bitsetReader SectionBloomBitSetReader = db

	raw, ok, err := rawReader.SectionBloom(3, 42)
	if err != nil || !ok {
		t.Fatalf("cold interface SectionBloom = %x/%v/%v, want cold row", raw, ok, err)
	}
	bitset, err := DecodeSectionBloomBitSet(raw)
	if err != nil || !SectionBloomBitSetHas(bitset, 7) {
		t.Fatalf("cold interface SectionBloom bitset = %x/%v, want bit 7", bitset, err)
	}
	bitset, ok, err = bitsetReader.SectionBloomBitSet(3, 42)
	if err != nil || !ok || !SectionBloomBitSetHas(bitset, 7) {
		t.Fatalf("cold interface SectionBloomBitSet = %x/%v/%v, want bit 7", bitset, ok, err)
	}

	if err := WriteSectionBloom(db, 3, 42, hotEncoded); err != nil {
		t.Fatalf("WriteSectionBloom hot: %v", err)
	}
	raw, ok, err = rawReader.SectionBloom(3, 42)
	if err != nil || !ok || !bytes.Equal(raw, hotEncoded) {
		t.Fatalf("hot interface SectionBloom = %x/%v/%v, want hot row", raw, ok, err)
	}
	bitset, ok, err = bitsetReader.SectionBloomBitSet(3, 42)
	if err != nil || !ok || !SectionBloomBitSetHas(bitset, 9) || SectionBloomBitSetHas(bitset, 7) {
		t.Fatalf("hot interface SectionBloomBitSet = %x/%v/%v, want hot bit 9 only", bitset, ok, err)
	}
}

func TestChainDBSectionBloomReaderInterfaceSurfacesColdErrors(t *testing.T) {
	db := NewMemoryChainDB()
	db.SetSectionBloomReader(fakeSectionBloomReader{err: errors.New("cold section bloom unavailable")})
	var rawReader SectionBloomReader = db
	var bitsetReader SectionBloomBitSetReader = db

	if raw, ok, err := rawReader.SectionBloom(3, 42); err == nil || ok || raw != nil || !strings.Contains(err.Error(), "cold section bloom unavailable") {
		t.Fatalf("interface SectionBloom cold error = %x/%v/%v, want cold error", raw, ok, err)
	}
	if bitset, ok, err := bitsetReader.SectionBloomBitSet(3, 42); err == nil || ok || bitset != nil || !strings.Contains(err.Error(), "cold section bloom unavailable") {
		t.Fatalf("interface SectionBloomBitSet cold error = %x/%v/%v, want cold error", bitset, ok, err)
	}
}

func TestReadHotSectionBloomStrictIgnoresColdFallback(t *testing.T) {
	coldEncoded, err := EncodeSectionBloomBitSet(setSectionBloomBit(nil, 7))
	if err != nil {
		t.Fatalf("EncodeSectionBloomBitSet cold: %v", err)
	}
	hotEncoded, err := EncodeSectionBloomBitSet(setSectionBloomBit(nil, 9))
	if err != nil {
		t.Fatalf("EncodeSectionBloomBitSet hot: %v", err)
	}

	db := NewMemoryChainDB()
	db.SetSectionBloomReader(fakeSectionBloomReader{
		rows: map[[2]uint64][]byte{
			{3, 42}: coldEncoded,
		},
	})
	if got, ok, err := ReadHotSectionBloomStrict(db, 3, 42); err != nil || ok || got != nil {
		t.Fatalf("cold-only ReadHotSectionBloomStrict = %x/%v/%v, want missing hot row", got, ok, err)
	}

	if err := WriteSectionBloom(db, 3, 42, hotEncoded); err != nil {
		t.Fatalf("WriteSectionBloom hot: %v", err)
	}
	got, ok, err := ReadHotSectionBloomStrict(db, 3, 42)
	if err != nil || !ok || !bytes.Equal(got, hotEncoded) {
		t.Fatalf("hot ReadHotSectionBloomStrict = %x/%v/%v, want hot row", got, ok, err)
	}
	got[0] ^= 0xff
	again, ok, err := ReadHotSectionBloomStrict(db, 3, 42)
	if err != nil || !ok || !bytes.Equal(again, hotEncoded) {
		t.Fatalf("mutated ReadHotSectionBloomStrict result changed stored row = %x/%v/%v", again, ok, err)
	}
}

func TestSectionBloomStrictRejectsInvalidBitIndexWithoutHotAlias(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	encoded, err := EncodeSectionBloomBitSet(setSectionBloomBit(nil, 7))
	if err != nil {
		t.Fatalf("EncodeSectionBloomBitSet: %v", err)
	}
	if err := WriteSectionBloom(db, 4, 42, encoded); err != nil {
		t.Fatalf("WriteSectionBloom alias target: %v", err)
	}

	invalidBitIndex := uint64(1_000_042)
	if got, ok, err := ReadSectionBloomStrict(db, 3, invalidBitIndex); err == nil || ok || got != nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("ReadSectionBloomStrict invalid bit index = %x/%v/%v, want validation error", got, ok, err)
	}
	if got := ReadSectionBloom(db, 3, invalidBitIndex); got != nil {
		t.Fatalf("ReadSectionBloom invalid bit index = %x, want compatibility miss", got)
	}
	if bitset, ok, err := ReadSectionBloomBitSetStrict(db, 3, invalidBitIndex); err == nil || ok || bitset != nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("ReadSectionBloomBitSetStrict invalid bit index = %x/%v/%v, want validation error", bitset, ok, err)
	}
}

func TestSectionBloomStrictRejectsInvalidBitIndexBeforeColdFallback(t *testing.T) {
	db := NewMemoryChainDB()
	db.SetSectionBloomReader(fakeSectionBloomReader{err: errors.New("cold reader should not be reached")})

	invalidBitIndex := uint64(SectionBloomBitSize)
	if got, ok, err := ReadSectionBloomStrict(db, 3, invalidBitIndex); err == nil || ok || got != nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("ReadSectionBloomStrict invalid cold bit index = %x/%v/%v, want validation error", got, ok, err)
	}
	if bitset, ok, err := ReadSectionBloomBitSetStrict(db, 3, invalidBitIndex); err == nil || ok || bitset != nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("ReadSectionBloomBitSetStrict invalid cold bit index = %x/%v/%v, want validation error", bitset, ok, err)
	}
}

func TestSectionBloomBitSetStrictUsesDecodedColdReader(t *testing.T) {
	coldBitset := setSectionBloomBit(nil, 7)
	hotBitset := setSectionBloomBit(nil, 9)
	hotEncoded, err := EncodeSectionBloomBitSet(hotBitset)
	if err != nil {
		t.Fatalf("EncodeSectionBloomBitSet hot: %v", err)
	}

	db := NewMemoryChainDB()
	db.SetSectionBloomReader(fakeSectionBloomBitSetReader{
		bitsets: map[[2]uint64][]byte{
			{3, 42}: coldBitset,
		},
		rawErr: errors.New("raw section bloom should not be read"),
	})
	bitset, ok, err := ReadSectionBloomBitSetStrict(db, 3, 42)
	if err != nil || !ok || !SectionBloomBitSetHas(bitset, 7) {
		t.Fatalf("strict decoded cold ReadSectionBloomBitSet = %x/%v/%v, want bit 7", bitset, ok, err)
	}
	if got := ReadSectionBloom(db, 3, 42); got != nil {
		t.Fatalf("ReadSectionBloom decoded-only reader = %x, want nil compatibility miss", got)
	}

	if err := WriteSectionBloom(db, 3, 42, hotEncoded); err != nil {
		t.Fatalf("WriteSectionBloom hot: %v", err)
	}
	bitset, ok, err = ReadSectionBloomBitSetStrict(db, 3, 42)
	if err != nil || !ok || !SectionBloomBitSetHas(bitset, 9) || SectionBloomBitSetHas(bitset, 7) {
		t.Fatalf("strict hot ReadSectionBloomBitSet = %x/%v/%v, want hot bit 9 only", bitset, ok, err)
	}
}

func TestSectionBloomStrictSurfacesColdReaderError(t *testing.T) {
	db := NewMemoryChainDB()
	db.SetSectionBloomReader(fakeSectionBloomReader{err: errors.New("cold section bloom corrupt")})

	if got := ReadSectionBloom(db, 3, 42); got != nil {
		t.Fatalf("ReadSectionBloom cold error = %x, want nil compatibility miss", got)
	}
	if bitset, ok, err := ReadSectionBloomBitSet(db, 3, 42); err != nil || ok || bitset != nil {
		t.Fatalf("ReadSectionBloomBitSet cold error = %x/%v/%v, want compatibility miss", bitset, ok, err)
	}
	if bitset, ok, err := ReadSectionBloomBitSetStrict(db, 3, 42); err == nil || ok || bitset != nil || !strings.Contains(err.Error(), "cold section bloom corrupt") {
		t.Fatalf("ReadSectionBloomBitSetStrict cold error = %x/%v/%v, want cold error", bitset, ok, err)
	}
}

func TestSectionBloomStrictSurfacesHotReadError(t *testing.T) {
	wantErr := errors.New("hot section bloom read corrupt")
	db := failingGetStore{
		KeyValueStore: ethrawdb.NewMemoryDatabase(),
		key:           sectionBloomKey(3, 42),
		err:           wantErr,
	}

	if got := ReadSectionBloom(db, 3, 42); got != nil {
		t.Fatalf("ReadSectionBloom hot read error = %x, want nil compatibility miss", got)
	}
	if bitset, ok, err := ReadSectionBloomBitSetStrict(db, 3, 42); !errors.Is(err, wantErr) || ok || bitset != nil {
		t.Fatalf("ReadSectionBloomBitSetStrict hot read error = %x/%v/%v, want hot read error", bitset, ok, err)
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

func TestSectionBloomBitSetCodecRejectsOversizedInput(t *testing.T) {
	bitset := setSectionBloomBit(nil, SectionBloomBitSize)
	if _, err := EncodeSectionBloomBitSet(bitset); err == nil || !strings.Contains(err.Error(), "bitset has") {
		t.Fatalf("EncodeSectionBloomBitSet oversized error = %v, want bitset-size error", err)
	}
}

func TestSectionBloomRejectsInvalidBitIndex(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if err := WriteSectionBloom(db, 0, SectionBloomBitSize, []byte{0x01}); err == nil {
		t.Fatal("WriteSectionBloom accepted out-of-range bit index")
	}
	if err := DeleteSectionBloom(db, 0, SectionBloomBitSize); err == nil {
		t.Fatal("DeleteSectionBloom accepted out-of-range bit index")
	}
}

func TestSectionBloomRejectsCompositeKeyOverflow(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	overflowSection := ^uint64(0)/1_000_000 + 1
	if err := WriteSectionBloom(db, overflowSection, 0, []byte{0x01}); err == nil || !strings.Contains(err.Error(), "overflows") {
		t.Fatalf("WriteSectionBloom overflow = %v, want composite-key overflow", err)
	}
	if err := DeleteSectionBloom(db, overflowSection, 0); err == nil || !strings.Contains(err.Error(), "overflows") {
		t.Fatalf("DeleteSectionBloom overflow = %v, want composite-key overflow", err)
	}
	if raw, ok, err := ReadSectionBloomStrict(db, overflowSection, 0); err == nil || raw != nil || ok || !strings.Contains(err.Error(), "overflows") {
		t.Fatalf("ReadSectionBloomStrict overflow = %x/%v/%v, want composite-key overflow", raw, ok, err)
	}
	if raw, ok, err := ReadHotSectionBloomStrict(db, overflowSection, 0); err == nil || raw != nil || ok || !strings.Contains(err.Error(), "overflows") {
		t.Fatalf("ReadHotSectionBloomStrict overflow = %x/%v/%v, want composite-key overflow", raw, ok, err)
	}
	if bitset, ok, err := ReadSectionBloomBitSetStrict(db, overflowSection, 0); err == nil || bitset != nil || ok || !strings.Contains(err.Error(), "overflows") {
		t.Fatalf("ReadSectionBloomBitSetStrict overflow = %x/%v/%v, want composite-key overflow", bitset, ok, err)
	}
	if raw := ReadSectionBloom(db, overflowSection, 0); raw != nil {
		t.Fatalf("ReadSectionBloom overflow = %x, want compatibility miss", raw)
	}
}

func TestSectionBloomBitSetCodecRejectsOversizedDecodedRow(t *testing.T) {
	bitset := setSectionBloomBit(nil, SectionBloomBitSize)
	encoded := encodeRawSectionBloomPayload(t, bitset)
	if _, err := DecodeSectionBloomBitSet(encoded); err == nil || !strings.Contains(err.Error(), "decoded bitset has") {
		t.Fatalf("DecodeSectionBloomBitSet oversized error = %v, want decoded-size error", err)
	}

	db := ethrawdb.NewMemoryDatabase()
	if err := WriteSectionBloom(db, 3, 42, encoded); err != nil {
		t.Fatalf("WriteSectionBloom oversized raw row: %v", err)
	}
	if _, ok, err := ReadSectionBloomBitSet(db, 3, 42); !ok || err == nil || !strings.Contains(err.Error(), "decoded bitset has") {
		t.Fatalf("ReadSectionBloomBitSet oversized = ok %v err %v, want decoded-size error", ok, err)
	}
}

func encodeRawSectionBloomPayload(t *testing.T, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	if _, err := zw.Write(payload); err != nil {
		t.Fatalf("zlib write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zlib close: %v", err)
	}
	return buf.Bytes()
}

type fakeSectionBloomReader struct {
	rows map[[2]uint64][]byte
	err  error
}

func (r fakeSectionBloomReader) SectionBloom(section, bitIndex uint64) ([]byte, bool, error) {
	if r.err != nil {
		return nil, false, r.err
	}
	value, ok := r.rows[[2]uint64{section, bitIndex}]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

type fakeSectionBloomBitSetReader struct {
	bitsets map[[2]uint64][]byte
	err     error
	rawErr  error
}

func (r fakeSectionBloomBitSetReader) SectionBloom(section, bitIndex uint64) ([]byte, bool, error) {
	if r.rawErr != nil {
		return nil, false, r.rawErr
	}
	bitset, ok := r.bitsets[[2]uint64{section, bitIndex}]
	if !ok {
		return nil, false, nil
	}
	encoded, err := EncodeSectionBloomBitSet(bitset)
	if err != nil {
		return nil, false, err
	}
	return encoded, true, nil
}

func (r fakeSectionBloomBitSetReader) SectionBloomBitSet(section, bitIndex uint64) ([]byte, bool, error) {
	if r.err != nil {
		return nil, false, r.err
	}
	bitset, ok := r.bitsets[[2]uint64{section, bitIndex}]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), bitset...), true, nil
}

type failingGetStore struct {
	ethdb.KeyValueStore
	key []byte
	err error
}

func (db failingGetStore) Has(key []byte) (bool, error) {
	if bytes.Equal(key, db.key) {
		return true, nil
	}
	return db.KeyValueStore.Has(key)
}

func (db failingGetStore) Get(key []byte) ([]byte, error) {
	if bytes.Equal(key, db.key) {
		return nil, db.err
	}
	return db.KeyValueStore.Get(key)
}

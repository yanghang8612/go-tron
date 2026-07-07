package rawdb

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"strconv"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

const (
	SectionBloomBlockPerSection = 2048
	SectionBloomBitSize         = 2048
	SectionBloomByteSize        = SectionBloomBitSize / 8
)

// WriteSectionBloom stores the raw section-bloom value for (section, bitIndex).
// java-tron stores a zlib-compressed BitSet.toByteArray payload; callers that
// build compatible rows should use EncodeSectionBloomBitSet first.
func WriteSectionBloom(db ethdb.KeyValueWriter, section, bitIndex uint64, value []byte) error {
	if err := validateSectionBloomBitIndex(bitIndex, "write section bloom"); err != nil {
		return err
	}
	return db.Put(sectionBloomKey(section, bitIndex), value)
}

// ReadSectionBloom returns the raw stored section-bloom value or nil if absent.
func ReadSectionBloom(db ethdb.KeyValueReader, section, bitIndex uint64) []byte {
	value, ok, err := ReadSectionBloomStrict(db, section, bitIndex)
	if err != nil || !ok {
		return nil
	}
	return value
}

// ReadSectionBloomStrict returns the raw stored section-bloom value plus an
// existence flag and surfaces cold sidecar lookup errors. The non-strict
// reader intentionally treats cold errors as misses because section blooms are
// optional log-query prefilters; rebuild and verification paths should use this
// strict variant so they do not republish incomplete bitsets.
func ReadSectionBloomStrict(db ethdb.KeyValueReader, section, bitIndex uint64) ([]byte, bool, error) {
	if db == nil {
		return nil, false, fmt.Errorf("rawdb: nil database during read section bloom")
	}
	if err := validateSectionBloomBitIndex(bitIndex, "read section bloom"); err != nil {
		return nil, false, err
	}
	key := sectionBloomKey(section, bitIndex)
	exists, err := db.Has(key)
	if err != nil {
		return nil, false, err
	}
	if exists {
		data, err := db.Get(key)
		if err != nil {
			return nil, false, err
		}
		if len(data) != 0 {
			out := make([]byte, len(data))
			copy(out, data)
			return out, true, nil
		}
	}
	if cdb, ok := db.(*ChainDB); ok && cdb.sectionBloom != nil {
		cold, ok, err := cdb.sectionBloom.SectionBloom(section, bitIndex)
		if err != nil || !ok || len(cold) == 0 {
			return nil, ok, err
		}
		out := make([]byte, len(cold))
		copy(out, cold)
		return out, true, nil
	}
	return nil, false, nil
}

// ReadHotSectionBloomStrict returns only the raw hot KV row for (section,
// bitIndex). It intentionally does not consult cold snapshot sidecars, so
// snapshot prune code can distinguish rows that still occupy Pebble space from
// rows already served only by cold storage.
func ReadHotSectionBloomStrict(db ethdb.KeyValueReader, section, bitIndex uint64) ([]byte, bool, error) {
	if db == nil {
		return nil, false, fmt.Errorf("rawdb: nil database during read hot section bloom")
	}
	if err := validateSectionBloomBitIndex(bitIndex, "read hot section bloom"); err != nil {
		return nil, false, err
	}
	key := sectionBloomKey(section, bitIndex)
	exists, err := db.Has(key)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		return nil, false, nil
	}
	data, err := db.Get(key)
	if err != nil {
		return nil, true, err
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out, true, nil
}

// DeleteSectionBloom removes the (section, bitIndex) entry.
func DeleteSectionBloom(db ethdb.KeyValueWriter, section, bitIndex uint64) error {
	if err := validateSectionBloomBitIndex(bitIndex, "delete section bloom"); err != nil {
		return err
	}
	return db.Delete(sectionBloomKey(section, bitIndex))
}

// EncodeSectionBloomBitSet mirrors java-tron's ByteUtil.compress(BitSet.toByteArray()).
// The bitset bytes use Java BitSet's little-endian layout: bit N is stored at
// bytes[N/8]&(1<<(N%8)). Trailing zero bytes are trimmed before compression.
func EncodeSectionBloomBitSet(bitset []byte) ([]byte, error) {
	trimmed := trimTrailingZeroes(bitset)
	if len(trimmed) > SectionBloomByteSize {
		return nil, fmt.Errorf("bitset has %d bytes, want <= %d", len(trimmed), SectionBloomByteSize)
	}
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	if _, err := zw.Write(trimmed); err != nil {
		_ = zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeSectionBloomBitSet decodes the java-tron section-bloom value into Java
// BitSet little-endian bytes.
func DecodeSectionBloomBitSet(value []byte) ([]byte, error) {
	zr, err := zlib.NewReader(bytes.NewReader(value))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	data, err := io.ReadAll(io.LimitReader(zr, SectionBloomByteSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > SectionBloomByteSize {
		return nil, fmt.Errorf("decoded bitset has %d bytes, want <= %d", len(data), SectionBloomByteSize)
	}
	return data, nil
}

// ReadSectionBloomBitSet reads and decodes a section-bloom row. ok is false
// when the row is absent.
func ReadSectionBloomBitSet(db ethdb.KeyValueReader, section, bitIndex uint64) ([]byte, bool, error) {
	value := ReadSectionBloom(db, section, bitIndex)
	if value == nil {
		return nil, false, nil
	}
	bitset, err := DecodeSectionBloomBitSet(value)
	if err != nil {
		return nil, true, fmt.Errorf("section bloom %d/%d: decode: %w", section, bitIndex, err)
	}
	return bitset, true, nil
}

// ReadSectionBloomBitSetStrict reads and decodes a section-bloom row while
// preserving cold sidecar errors for callers that require complete indexes.
func ReadSectionBloomBitSetStrict(db ethdb.KeyValueReader, section, bitIndex uint64) ([]byte, bool, error) {
	if db == nil {
		return nil, false, fmt.Errorf("rawdb: nil database during read section bloom")
	}
	if err := validateSectionBloomBitIndex(bitIndex, "read section bloom bitset"); err != nil {
		return nil, false, err
	}
	key := sectionBloomKey(section, bitIndex)
	exists, err := db.Has(key)
	if err != nil {
		return nil, false, err
	}
	if exists {
		value, err := db.Get(key)
		if err != nil {
			return nil, false, err
		}
		if len(value) != 0 {
			bitset, err := DecodeSectionBloomBitSet(value)
			if err != nil {
				return nil, true, fmt.Errorf("section bloom %d/%d: decode: %w", section, bitIndex, err)
			}
			return bitset, true, nil
		}
	}
	if cdb, ok := db.(*ChainDB); ok && cdb.sectionBloom != nil {
		if bitsetReader, ok := cdb.sectionBloom.(SectionBloomBitSetReader); ok {
			bitset, ok, err := bitsetReader.SectionBloomBitSet(section, bitIndex)
			if err != nil || !ok {
				return nil, ok, err
			}
			out := make([]byte, len(bitset))
			copy(out, bitset)
			return out, true, nil
		}
		value, ok, err := cdb.sectionBloom.SectionBloom(section, bitIndex)
		if err != nil || !ok || len(value) == 0 {
			return nil, ok, err
		}
		bitset, err := DecodeSectionBloomBitSet(value)
		if err != nil {
			return nil, true, fmt.Errorf("section bloom %d/%d: decode: %w", section, bitIndex, err)
		}
		return bitset, true, nil
	}
	return nil, false, nil
}

func IterateSectionBloomRows(db ethdb.Iteratee, fn func(section, bitIndex uint64, value []byte) (bool, error)) error {
	if db == nil || fn == nil {
		return nil
	}
	it := db.NewIterator(sectionBloomPrefix, nil)
	defer it.Release()
	for it.Next() {
		section, bitIndex, ok := parseSectionBloomKey(it.Key())
		if !ok {
			continue
		}
		cont, err := fn(section, bitIndex, append([]byte(nil), it.Value()...))
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return it.Error()
}

// SectionBloomBitIndexes returns the three section-bloom bit indexes set by
// java-tron's Bloom.create(Hash.sha3(data)).
func SectionBloomBitIndexes(data []byte) [3]uint64 {
	hash := common.Keccak256(data)
	return [3]uint64{
		sectionBloomBitIndex((uint64(hash[0]&0x07) << 8) | uint64(hash[1])),
		sectionBloomBitIndex((uint64(hash[2]&0x07) << 8) | uint64(hash[3])),
		sectionBloomBitIndex((uint64(hash[4]&0x07) << 8) | uint64(hash[5])),
	}
}

func SectionBloomBitSetHas(bitset []byte, bit uint64) bool {
	byteIndex := bit / 8
	if byteIndex >= uint64(len(bitset)) {
		return false
	}
	return bitset[byteIndex]&(1<<(bit%8)) != 0
}

func sectionBloomBitIndex(movement uint64) uint64 {
	byteIndex := SectionBloomByteSize - 1 - movement/8
	return byteIndex*8 + movement%8
}

func validateSectionBloomBitIndex(bitIndex uint64, op string) error {
	if bitIndex >= SectionBloomBitSize {
		return fmt.Errorf("%s: bit index %d exceeds %d", op, bitIndex, SectionBloomBitSize-1)
	}
	return nil
}

func validateSectionBloomValue(value []byte, op string) error {
	if _, err := DecodeSectionBloomBitSet(value); err != nil {
		return fmt.Errorf("%s: decode: %w", op, err)
	}
	return nil
}

func trimTrailingZeroes(data []byte) []byte {
	end := len(data)
	for end > 0 && data[end-1] == 0 {
		end--
	}
	return data[:end]
}

func parseSectionBloomKey(key []byte) (uint64, uint64, bool) {
	if !bytes.HasPrefix(key, sectionBloomPrefix) {
		return 0, 0, false
	}
	compositeHex := string(key[len(sectionBloomPrefix):])
	if compositeHex == "" {
		return 0, 0, false
	}
	composite, err := strconv.ParseUint(compositeHex, 16, 64)
	if err != nil {
		return 0, 0, false
	}
	section := composite / 1_000_000
	bitIndex := composite % 1_000_000
	if bitIndex >= SectionBloomBitSize {
		return 0, 0, false
	}
	return section, bitIndex, true
}

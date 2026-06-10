package rawdb

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"

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
	return db.Put(sectionBloomKey(section, bitIndex), value)
}

// ReadSectionBloom returns the raw stored section-bloom value or nil if absent.
func ReadSectionBloom(db ethdb.KeyValueReader, section, bitIndex uint64) []byte {
	data, err := db.Get(sectionBloomKey(section, bitIndex))
	if err != nil || len(data) == 0 {
		return nil
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out
}

// DeleteSectionBloom removes the (section, bitIndex) entry.
func DeleteSectionBloom(db ethdb.KeyValueWriter, section, bitIndex uint64) error {
	return db.Delete(sectionBloomKey(section, bitIndex))
}

// EncodeSectionBloomBitSet mirrors java-tron's ByteUtil.compress(BitSet.toByteArray()).
// The bitset bytes use Java BitSet's little-endian layout: bit N is stored at
// bytes[N/8]&(1<<(N%8)). Trailing zero bytes are trimmed before compression.
func EncodeSectionBloomBitSet(bitset []byte) ([]byte, error) {
	trimmed := trimTrailingZeroes(bitset)
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
	data, err := io.ReadAll(zr)
	if err != nil {
		return nil, err
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

func trimTrailingZeroes(data []byte) []byte {
	end := len(data)
	for end > 0 && data[end-1] == 0 {
		end--
	}
	return data[:end]
}

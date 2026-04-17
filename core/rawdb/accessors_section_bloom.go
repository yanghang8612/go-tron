package rawdb

import "github.com/ethereum/go-ethereum/ethdb"

// WriteSectionBloom stores the bloom-filter bytes for (section, bitIndex).
// Mirrors java-tron SectionBloomStore. The `bloom` payload is treated as
// opaque — java-tron wraps a zlib-compressed BitSet; callers porting the
// eth-compat filter path are responsible for matching the encoding.
func WriteSectionBloom(db ethdb.KeyValueWriter, section, bitIndex uint64, bloom []byte) error {
	return db.Put(sectionBloomKey(section, bitIndex), bloom)
}

// ReadSectionBloom returns the bloom bytes or nil if absent.
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

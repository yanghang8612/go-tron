package rawdb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

// taposRefBytes converts a block number to the 2-byte refBlockBytes form
// java-tron uses for RecentBlockStore lookups: the low 16 bits of the
// block number, big-endian. Mirrors
// TransactionCapsule.getRefBlockBytes -> num[6], num[7].
func taposRefBytes(blockNum uint64) [2]byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(blockNum&0xFFFF))
	return b
}

// TaposRefStorageKey returns the recent-block ring key selected by blockNum.
// It is used by layered writers that retain the TAPOS row for overlay/reorg
// reads after a direct metadata batch has already persisted the same value.
func TaposRefStorageKey(blockNum uint64) []byte {
	ref := taposRefBytes(blockNum)
	return taposKey(ref[:])
}

// WriteTaposRef records a block's hash tail (bytes 8..16 of its
// 32-byte hash) under the 2-byte ref slot derived from its block number.
// Each new block at the same lower-half number overwrites the previous
// occupant — a 65536-entry ring whose bound is enforced by the keyspace.
// Mirrors java-tron Manager.updateRecentBlock at the head of every
// pushBlockInner.
func WriteTaposRef(db ethdb.KeyValueWriter, blockNum uint64, blockHash common.Hash) error {
	ref := taposRefBytes(blockNum)
	return db.Put(taposKey(ref[:]), blockHash[8:16])
}

// ReadTaposRef returns the stored 8-byte hash tail for the given
// refBlockBytes, or nil if no block has yet been written at that slot.
// java-tron throws ItemNotFoundException on miss; we surface that as a nil
// return and let the caller (ValidateTAPOS) treat it as TAPOS failure.
func ReadTaposRef(db ethdb.KeyValueReader, refBlockBytes []byte) []byte {
	if len(refBlockBytes) != 2 {
		return nil
	}
	v, err := db.Get(taposKey(refBlockBytes))
	if err != nil || len(v) != 8 {
		return nil
	}
	return v
}

// ReadTaposRefNoCopy is the validation hot-path variant. Layered readers can
// resolve the fixed prefix and slot without allocating a physical key and may
// return their immutable cached value directly.
func ReadTaposRefNoCopy(db ethdb.KeyValueReader, refBlockBytes []byte) []byte {
	if len(refBlockBytes) != 2 {
		return nil
	}
	var (
		v   []byte
		err error
	)
	if reader, ok := db.(cachedNoCopyKeyPartsReader); ok {
		v, err = reader.GetNoCopyCachedKeyParts(taposPrefix, refBlockBytes)
	} else {
		v, err = db.Get(taposKey(refBlockBytes))
	}
	if err != nil || len(v) != 8 {
		return nil
	}
	return v
}

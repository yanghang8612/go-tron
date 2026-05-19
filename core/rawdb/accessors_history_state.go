package rawdb

import (
	"encoding/binary"
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	historypb "github.com/tronprotocol/go-tron/proto/core/historystate"
	"google.golang.org/protobuf/proto"
)

// ErrHistoryConfigAbsent is returned by ReadHistoryConfig when no config
// sentinel has been written yet. Callers (e.g. node startup) treat this as
// "history index is fresh / disabled".
var ErrHistoryConfigAbsent = errors.New("rawdb: history config absent")

// Every accessor in this file takes the narrow ethdb.KeyValueWriter /
// ethdb.KeyValueReader / ethdb.Iteratee interfaces — never a concrete
// Pebble store — so the same code path works against both the on-disk
// database and core/blockbuffer.Buffer. That buffer-routability is what
// keeps history rows fork-rewindable when switchFork discards layers in
// slice 2 and beyond.

// ---- Account delta (sh-a-) ------------------------------------------------

// WriteAccountDelta persists the pre-block account state for (blockNum, addr).
// The caller has already populated delta.ExistedPre and (when ExistedPre is
// true) the AccountProtoPre / CodePre / ContractMetaPre fields. AddrPre is
// set defensively here to the addr argument so the row is self-describing
// during pruning even if the caller forgot.
//
// Returns an error only on the proto marshal path; the underlying KV
// writer's Put error is bubbled up unchanged.
func WriteAccountDelta(db ethdb.KeyValueWriter, blockNum uint64, addr tcommon.Address, delta *historypb.AccountDelta) error {
	if delta == nil {
		return errors.New("rawdb: nil AccountDelta")
	}
	// Force the addr field to match the key. Callers should set it but
	// the redundancy keeps rows self-describing and catches programmer
	// error if the addr argument and delta.Addr drift.
	delta.Addr = append(delta.Addr[:0], addr.Bytes()...)
	data, err := proto.Marshal(delta)
	if err != nil {
		return err
	}
	return db.Put(historyAccountKey(blockNum, addr.Bytes()), data)
}

// ReadAccountDelta returns the pre-block AccountDelta for (blockNum, addr),
// or nil if no such row exists. Returns nil on unmarshal failure so callers
// can treat both "absent" and "corrupt" identically — Slice 3 of the
// reader will surface the latter via metrics.
func ReadAccountDelta(db ethdb.KeyValueReader, blockNum uint64, addr tcommon.Address) *historypb.AccountDelta {
	data, err := db.Get(historyAccountKey(blockNum, addr.Bytes()))
	if err != nil || len(data) == 0 {
		return nil
	}
	var delta historypb.AccountDelta
	if err := proto.Unmarshal(data, &delta); err != nil {
		return nil
	}
	return &delta
}

// HasAccountDelta reports whether an AccountDelta row exists at (blockNum, addr).
func HasAccountDelta(db ethdb.KeyValueReader, blockNum uint64, addr tcommon.Address) bool {
	ok, _ := db.Has(historyAccountKey(blockNum, addr.Bytes()))
	return ok
}

// DeleteAccountDelta removes the AccountDelta row at (blockNum, addr).
// Used by Slice 5's pruner (and tests).
func DeleteAccountDelta(db ethdb.KeyValueWriter, blockNum uint64, addr tcommon.Address) error {
	return db.Delete(historyAccountKey(blockNum, addr.Bytes()))
}

// ---- Slot delta (sh-s-) ---------------------------------------------------

// slotSentinelZero is a 1-byte marker we write when the pre-slot value is
// the all-zero Hash{}. An empty/absent value would be ambiguous because
// some KV backends (e.g. memorydb) return a nil byte slice for both
// "tombstoned" and "absent" — distinguishing the two matters for slot
// reads where an explicit "this slot was 0 before the block" is materially
// different from "this slot wasn't touched". Readers normalise back to
// Hash{} when they see this sentinel.
var slotSentinelZero = []byte{0x00}

// WriteSlotDelta persists the 32-byte pre-block storage value for
// (blockNum, addr, slot). All-zero pre-values are stored as a 1-byte
// sentinel; readers handle the round-trip.
func WriteSlotDelta(db ethdb.KeyValueWriter, blockNum uint64, addr tcommon.Address, slot, preValue tcommon.Hash) error {
	key := historySlotKey(blockNum, addr.Bytes(), slot.Bytes())
	if preValue == (tcommon.Hash{}) {
		return db.Put(key, slotSentinelZero)
	}
	return db.Put(key, preValue.Bytes())
}

// ReadSlotDelta returns the pre-block slot value at (blockNum, addr, slot)
// together with a presence flag. found=false means no row exists at that
// (block, addr, slot); found=true with the zero hash means the pre-block
// slot was empty.
func ReadSlotDelta(db ethdb.KeyValueReader, blockNum uint64, addr tcommon.Address, slot tcommon.Hash) (tcommon.Hash, bool) {
	data, err := db.Get(historySlotKey(blockNum, addr.Bytes(), slot.Bytes()))
	if err != nil || len(data) == 0 {
		return tcommon.Hash{}, false
	}
	// Sentinel: explicit "pre-value was zero".
	if len(data) == 1 && data[0] == 0x00 {
		return tcommon.Hash{}, true
	}
	return tcommon.BytesToHash(data), true
}

// HasSlotDelta reports whether a SlotDelta row exists at (blockNum, addr, slot).
func HasSlotDelta(db ethdb.KeyValueReader, blockNum uint64, addr tcommon.Address, slot tcommon.Hash) bool {
	ok, _ := db.Has(historySlotKey(blockNum, addr.Bytes(), slot.Bytes()))
	return ok
}

// DeleteSlotDelta removes the SlotDelta row at (blockNum, addr, slot).
func DeleteSlotDelta(db ethdb.KeyValueWriter, blockNum uint64, addr tcommon.Address, slot tcommon.Hash) error {
	return db.Delete(historySlotKey(blockNum, addr.Bytes(), slot.Bytes()))
}

// ---- Per-block meta (sh-m-) ----------------------------------------------

// WriteHistoryMeta persists the per-block StateHistoryMeta record.
func WriteHistoryMeta(db ethdb.KeyValueWriter, blockNum uint64, meta *historypb.StateHistoryMeta) error {
	if meta == nil {
		return errors.New("rawdb: nil StateHistoryMeta")
	}
	// Force key/value blockNum agreement.
	meta.BlockNum = blockNum
	data, err := proto.Marshal(meta)
	if err != nil {
		return err
	}
	return db.Put(historyMetaKey(blockNum), data)
}

// ReadHistoryMeta returns the per-block StateHistoryMeta record, or nil if
// absent / unparseable.
func ReadHistoryMeta(db ethdb.KeyValueReader, blockNum uint64) *historypb.StateHistoryMeta {
	data, err := db.Get(historyMetaKey(blockNum))
	if err != nil || len(data) == 0 {
		return nil
	}
	var meta historypb.StateHistoryMeta
	if err := proto.Unmarshal(data, &meta); err != nil {
		return nil
	}
	return &meta
}

// HasHistoryMeta reports whether a per-block StateHistoryMeta row exists.
func HasHistoryMeta(db ethdb.KeyValueReader, blockNum uint64) bool {
	ok, _ := db.Has(historyMetaKey(blockNum))
	return ok
}

// DeleteHistoryMeta removes the per-block StateHistoryMeta record. Used by
// the pruner and by switchFork's rollback path.
func DeleteHistoryMeta(db ethdb.KeyValueWriter, blockNum uint64) error {
	return db.Delete(historyMetaKey(blockNum))
}

// ---- Inverse index (sh-i-a-, sh-i-s-) -------------------------------------

// WriteAddrInverse writes the key-only inverse-index row that marks "addr
// was modified at blockNum". The value is empty.
func WriteAddrInverse(db ethdb.KeyValueWriter, addr tcommon.Address, blockNum uint64) error {
	return db.Put(historyAddrInverseKey(addr.Bytes(), blockNum), nil)
}

// HasAddrInverse reports whether an inverse-index row exists for (addr, blockNum).
func HasAddrInverse(db ethdb.KeyValueReader, addr tcommon.Address, blockNum uint64) bool {
	ok, _ := db.Has(historyAddrInverseKey(addr.Bytes(), blockNum))
	return ok
}

// DeleteAddrInverse removes the inverse-index row for (addr, blockNum).
func DeleteAddrInverse(db ethdb.KeyValueWriter, addr tcommon.Address, blockNum uint64) error {
	return db.Delete(historyAddrInverseKey(addr.Bytes(), blockNum))
}

// IterateAddrInverse returns an iterator over every inverse-index row for
// the given addr. Keys are sorted by ascending blockNum (big-endian uint64
// in the key tail). Callers MUST Release() the iterator. Typical use:
//
//	it := IterateAddrInverse(db, addr)
//	defer it.Release()
//	for it.Next() {
//	    blockNum := AddrInverseBlockNum(it.Key())
//	    ...
//	}
func IterateAddrInverse(db ethdb.Iteratee, addr tcommon.Address) ethdb.Iterator {
	return db.NewIterator(historyAddrInverseAddrPrefix(addr.Bytes()), nil)
}

// AddrInverseBlockNum extracts the trailing big-endian uint64 blockNum
// from a sh-i-a- key. Returns 0 and false if the key shape is wrong (too
// short, wrong prefix). Slice 3 of the reader uses this on iterator
// output to seek the corresponding sh-a- row.
func AddrInverseBlockNum(key []byte) (uint64, bool) {
	if len(key) < len(shAddrInversePrefix)+tcommon.AddressLength+8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[len(key)-8:]), true
}

// WriteSlotInverse writes the key-only inverse-index row that marks "slot
// of addr was modified at blockNum". The value is empty.
func WriteSlotInverse(db ethdb.KeyValueWriter, addr tcommon.Address, slot tcommon.Hash, blockNum uint64) error {
	return db.Put(historySlotInverseKey(addr.Bytes(), slot.Bytes(), blockNum), nil)
}

// HasSlotInverse reports whether an inverse-index row exists for
// (addr, slot, blockNum).
func HasSlotInverse(db ethdb.KeyValueReader, addr tcommon.Address, slot tcommon.Hash, blockNum uint64) bool {
	ok, _ := db.Has(historySlotInverseKey(addr.Bytes(), slot.Bytes(), blockNum))
	return ok
}

// DeleteSlotInverse removes the inverse-index row for (addr, slot, blockNum).
func DeleteSlotInverse(db ethdb.KeyValueWriter, addr tcommon.Address, slot tcommon.Hash, blockNum uint64) error {
	return db.Delete(historySlotInverseKey(addr.Bytes(), slot.Bytes(), blockNum))
}

// IterateSlotInverse returns an iterator over every inverse-index row for
// (addr, slot). Keys are sorted by ascending blockNum. Same Release()
// discipline as IterateAddrInverse.
func IterateSlotInverse(db ethdb.Iteratee, addr tcommon.Address, slot tcommon.Hash) ethdb.Iterator {
	return db.NewIterator(historySlotInverseSlotPrefix(addr.Bytes(), slot.Bytes()), nil)
}

// SlotInverseBlockNum extracts the trailing big-endian uint64 blockNum
// from a sh-i-s- key.
func SlotInverseBlockNum(key []byte) (uint64, bool) {
	if len(key) < len(shSlotInversePrefix)+tcommon.AddressLength+tcommon.HashLength+8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[len(key)-8:]), true
}

// ---- History config (sh-cfg-) --------------------------------------------

// WriteHistoryConfig persists the singleton HistoryConfig sentinel.
func WriteHistoryConfig(db ethdb.KeyValueWriter, cfg *historypb.HistoryConfig) error {
	if cfg == nil {
		return errors.New("rawdb: nil HistoryConfig")
	}
	data, err := proto.Marshal(cfg)
	if err != nil {
		return err
	}
	return db.Put(historyConfigKey(), data)
}

// ReadHistoryConfig returns the singleton HistoryConfig, or
// (nil, ErrHistoryConfigAbsent) if no sentinel has been written yet.
func ReadHistoryConfig(db ethdb.KeyValueReader) (*historypb.HistoryConfig, error) {
	data, err := db.Get(historyConfigKey())
	if err != nil || len(data) == 0 {
		return nil, ErrHistoryConfigAbsent
	}
	var cfg historypb.HistoryConfig
	if err := proto.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// HasHistoryConfig reports whether the singleton HistoryConfig sentinel
// has been written.
func HasHistoryConfig(db ethdb.KeyValueReader) bool {
	ok, _ := db.Has(historyConfigKey())
	return ok
}

// DeleteHistoryConfig removes the HistoryConfig sentinel. Used by the
// "switch from archive to full" operator path that wipes config so the
// next startup re-bootstraps from defaults.
func DeleteHistoryConfig(db ethdb.KeyValueWriter) error {
	return db.Delete(historyConfigKey())
}

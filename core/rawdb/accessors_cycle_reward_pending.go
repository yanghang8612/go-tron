package rawdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

// CycleRewardPendingEntry is the compact handoff form of one current-cycle
// reward. Async commit snapshots use a slice of entries instead of cloning the
// whole map and its buckets.
type CycleRewardPendingEntry struct {
	Address common.Address
	Amount  int64
}

// ReadCycleRewardPending reads the flat current-cycle reward accumulator.
func ReadCycleRewardPending(db ethdb.KeyValueReader) (int64, map[common.Address]int64, bool, error) {
	data, err := db.Get(cycleRewardPendingKey)
	if err != nil || len(data) == 0 {
		return 0, nil, false, nil
	}
	if len(data) < 12 {
		return 0, nil, false, errors.New("cycle reward pending: short value")
	}
	cycle := int64(binary.BigEndian.Uint64(data[:8]))
	count := int(binary.BigEndian.Uint32(data[8:12]))
	off := 12
	wantLen := off + count*(common.AddressLength+8)
	if count < 0 || len(data) != wantLen {
		return 0, nil, false, errors.New("cycle reward pending: malformed value")
	}
	rewards := make(map[common.Address]int64, count)
	for i := 0; i < count; i++ {
		var addr common.Address
		copy(addr[:], data[off:off+common.AddressLength])
		off += common.AddressLength
		amount := int64(binary.BigEndian.Uint64(data[off : off+8]))
		off += 8
		if amount != 0 {
			rewards[addr] = amount
		}
	}
	return cycle, rewards, true, nil
}

// WriteCycleRewardPending overwrites the flat current-cycle reward accumulator.
func WriteCycleRewardPending(db ethdb.KeyValueWriter, cycle int64, rewards map[common.Address]int64) error {
	entries := make([]CycleRewardPendingEntry, 0, len(rewards))
	for addr, amount := range rewards {
		if amount != 0 {
			entries = append(entries, CycleRewardPendingEntry{Address: addr, Amount: amount})
		}
	}
	return WriteCycleRewardPendingEntries(db, cycle, entries)
}

// WriteCycleRewardPendingEntries sorts and consumes a freshly captured entry
// slice. The caller transfers the slice and must not read or mutate it after
// this call. Layered writers can likewise retain the freshly encoded value
// without a defensive copy.
func WriteCycleRewardPendingEntries(db ethdb.KeyValueWriter, cycle int64, entries []CycleRewardPendingEntry) error {
	kept := entries[:0]
	for _, entry := range entries {
		if entry.Amount != 0 {
			kept = append(kept, entry)
		}
	}
	entries = kept
	if len(entries) == 0 {
		return DeleteCycleRewardPending(db)
	}
	slices.SortFunc(entries, func(a, b CycleRewardPendingEntry) int {
		return bytes.Compare(a.Address[:], b.Address[:])
	})
	buf := make([]byte, 12+len(entries)*(common.AddressLength+8))
	binary.BigEndian.PutUint64(buf[:8], uint64(cycle))
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(entries)))
	off := 12
	for _, entry := range entries {
		copy(buf[off:off+common.AddressLength], entry.Address[:])
		off += common.AddressLength
		binary.BigEndian.PutUint64(buf[off:off+8], uint64(entry.Amount))
		off += 8
	}
	if writer, ok := db.(stringOwnedValueWriter); ok {
		return writer.PutStringOwnedValue(cycleRewardPendingKeyString, buf)
	}
	return db.Put(cycleRewardPendingKey, buf)
}

func DeleteCycleRewardPending(db ethdb.KeyValueWriter) error {
	return db.Delete(cycleRewardPendingKey)
}

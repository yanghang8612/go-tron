package rawdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

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
	addrs := make([]common.Address, 0, len(rewards))
	for addr, amount := range rewards {
		if amount != 0 {
			addrs = append(addrs, addr)
		}
	}
	if len(addrs) == 0 {
		return DeleteCycleRewardPending(db)
	}
	sort.Slice(addrs, func(i, j int) bool {
		return bytes.Compare(addrs[i][:], addrs[j][:]) < 0
	})
	buf := make([]byte, 12+len(addrs)*(common.AddressLength+8))
	binary.BigEndian.PutUint64(buf[:8], uint64(cycle))
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(addrs)))
	off := 12
	for _, addr := range addrs {
		copy(buf[off:off+common.AddressLength], addr[:])
		off += common.AddressLength
		binary.BigEndian.PutUint64(buf[off:off+8], uint64(rewards[addr]))
		off += 8
	}
	return db.Put(cycleRewardPendingKey, buf)
}

func DeleteCycleRewardPending(db ethdb.KeyValueWriter) error {
	return db.Delete(cycleRewardPendingKey)
}

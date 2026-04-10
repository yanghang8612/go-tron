package rawdb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

const defaultBrokerage int64 = 20

func WriteWitnessBrokerage(db ethdb.KeyValueWriter, addr common.Address, brokerage int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(brokerage))
	return db.Put(brokerageKey(addr[:]), buf)
}

func ReadWitnessBrokerage(db ethdb.KeyValueReader, addr common.Address) int64 {
	data, err := db.Get(brokerageKey(addr[:]))
	if err != nil || len(data) != 8 {
		return defaultBrokerage
	}
	return int64(binary.BigEndian.Uint64(data))
}

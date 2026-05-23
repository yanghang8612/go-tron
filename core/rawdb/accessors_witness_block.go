package rawdb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
)

// ReadWitnessLatestBlock returns the latest block number produced by the given
// witness address, or 0 if no record exists. Mirrors java-tron's per-witness
// latestBlockNum tracking used by Manager.updateSolidifiedBlock.
func ReadWitnessLatestBlock(db ethdb.KeyValueReader, addr tcommon.Address) int64 {
	data, err := db.Get(witnessLatestBlockKey(addr[:]))
	if err != nil || len(data) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(data))
}

// WriteWitnessLatestBlock persists the latest block number produced by the
// given witness address. Called on every block insert to keep the per-witness
// cursor current for solidified-block computation.
func WriteWitnessLatestBlock(db ethdb.KeyValueWriter, addr tcommon.Address, num int64) {
	key := witnessLatestBlockKey(addr[:])
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(num))
	db.Put(key, buf[:])
}

// WitnessLatestBlockStateKey exposes the legacy latest-block key bytes for the
// native typed StateDB witness store. The key shape stays centralized here.
func WitnessLatestBlockStateKey(addr tcommon.Address) []byte {
	return witnessLatestBlockKey(addr[:])
}

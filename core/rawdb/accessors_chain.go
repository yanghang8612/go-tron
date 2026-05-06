package rawdb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

func WriteHeadBlockHash(db ethdb.KeyValueWriter, hash common.Hash) {
	db.Put(headBlockKey, hash.Bytes())
}

func ReadHeadBlockHash(db ethdb.KeyValueReader) common.Hash {
	data, err := db.Get(headBlockKey)
	if err != nil {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

func WriteHeadSolidBlockHash(db ethdb.KeyValueWriter, hash common.Hash) {
	db.Put(headSolidBlockKey, hash.Bytes())
}

func ReadHeadSolidBlockHash(db ethdb.KeyValueReader) common.Hash {
	data, err := db.Get(headSolidBlockKey)
	if err != nil {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

func WriteDynamicProperty(db ethdb.KeyValueWriter, name string, value []byte) {
	db.Put(dynPropKey(name), value)
}

func ReadDynamicProperty(db ethdb.KeyValueReader, name string) []byte {
	data, err := db.Get(dynPropKey(name))
	if err != nil {
		return nil
	}
	return data
}

// WriteActiveWitnesses stores the active witness list as length-prefixed addresses.
func WriteActiveWitnesses(db ethdb.KeyValueWriter, witnesses []common.Address) {
	buf := make([]byte, 4+len(witnesses)*common.AddressLength)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(witnesses)))
	for i, w := range witnesses {
		copy(buf[4+i*common.AddressLength:], w.Bytes())
	}
	db.Put(activeWitnessesKey, buf)
}

func ReadActiveWitnesses(db ethdb.KeyValueReader) []common.Address {
	data, err := db.Get(activeWitnessesKey)
	if err != nil || len(data) < 4 {
		return nil
	}
	count := int(binary.BigEndian.Uint32(data[:4]))
	if len(data) < 4+count*common.AddressLength {
		return nil
	}
	witnesses := make([]common.Address, count)
	for i := 0; i < count; i++ {
		witnesses[i] = common.BytesToAddress(data[4+i*common.AddressLength : 4+(i+1)*common.AddressLength])
	}
	return witnesses
}

func WriteWitnessIndex(db ethdb.KeyValueWriter, witnesses []common.Address) {
	buf := make([]byte, 4+len(witnesses)*common.AddressLength)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(witnesses)))
	for i, w := range witnesses {
		copy(buf[4+i*common.AddressLength:], w.Bytes())
	}
	db.Put(witnessIndexKey, buf)
}

func ReadWitnessIndex(db ethdb.KeyValueReader) []common.Address {
	data, err := db.Get(witnessIndexKey)
	if err != nil || len(data) < 4 {
		return nil
	}
	count := int(binary.BigEndian.Uint32(data[:4]))
	if len(data) < 4+count*common.AddressLength {
		return nil
	}
	witnesses := make([]common.Address, count)
	for i := 0; i < count; i++ {
		witnesses[i] = common.BytesToAddress(data[4+i*common.AddressLength : 4+(i+1)*common.AddressLength])
	}
	return witnesses
}

// witnessIndexReadWriter composes the narrow capabilities AppendWitnessIndex
// needs so callers can pass either an ethdb.KeyValueStore (genesis path) or
// the buffered KV view from actuator.Context.DB (WitnessCreateActuator).
type witnessIndexReadWriter interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}

func AppendWitnessIndex(db witnessIndexReadWriter, addr common.Address) {
	existing := ReadWitnessIndex(db)
	for _, w := range existing {
		if w == addr {
			return
		}
	}
	existing = append(existing, addr)
	WriteWitnessIndex(db, existing)
}

// ReadTotalTransactionCount returns the cumulative number of transactions ever
// processed by this node. Returns 0 if the counter has not been written yet.
// Non-consensus metric: not part of any state root or block hash.
func ReadTotalTransactionCount(db ethdb.KeyValueReader) int64 {
	data, err := db.Get(totalTransactionCountKey)
	if err != nil || len(data) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(data))
}

// WriteTotalTransactionCount persists the cumulative transaction count.
func WriteTotalTransactionCount(db ethdb.KeyValueWriter, count int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(count))
	db.Put(totalTransactionCountKey, buf[:])
}

// WriteGenesisStateRoot persists the post-genesis StateDB root, used to
// bootstrap block #1's parent state. The genesis block itself omits
// `account_state_root` to match java-tron's wire format.
func WriteGenesisStateRoot(db ethdb.KeyValueWriter, root common.Hash) {
	db.Put(genesisStateRootKey, root.Bytes())
}

// ReadGenesisStateRoot returns the post-genesis StateDB root, or the zero
// hash if it has not been written yet.
func ReadGenesisStateRoot(db ethdb.KeyValueReader) common.Hash {
	data, err := db.Get(genesisStateRootKey)
	if err != nil {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

// WriteBlockStateRoot persists the post-apply state root for the given
// block hash. Stored out-of-band so that the block proto's
// `account_state_root` field can stay empty for wire-format parity with
// java-tron blocks that arrived without it.
func WriteBlockStateRoot(db ethdb.KeyValueWriter, blockHash, root common.Hash) {
	db.Put(blockStateRootKey(blockHash.Bytes()), root.Bytes())
}

// ReadBlockStateRoot returns the post-apply state root for the given
// block hash, or the zero hash if not stored.
func ReadBlockStateRoot(db ethdb.KeyValueReader, blockHash common.Hash) common.Hash {
	data, err := db.Get(blockStateRootKey(blockHash.Bytes()))
	if err != nil {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

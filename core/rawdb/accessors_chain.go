package rawdb

import (
	"bytes"
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

// IterateDynamicProperties invokes fn for every persisted DynamicProperties
// key-value pair the underlying iterator can see. Callers that need many DP
// keys in a single pass (state.LoadDynamicProperties) use this in place of
// N point Gets — a profile on Nile at h≈890k showed 133 Gets per applyBlock
// dominating CPU at 46% of total samples.
//
// The visible name (the part after the "dp-" key prefix) is passed
// unprefixed so callers can route by name without re-parsing. fn must not
// retain the slice arguments past the call; the iterator owns them. Stops
// silently on iterator error (no return) — the caller cannot distinguish
// "no DP rows" from "iteration broke", which mirrors the pre-existing
// per-key path that silently dropped failing Gets.
func IterateDynamicProperties(db ethdb.Iteratee, fn func(name string, value []byte)) {
	it := db.NewIterator(dynPropPrefix, nil)
	defer it.Release()
	for it.Next() {
		name := string(bytes.TrimPrefix(it.Key(), dynPropPrefix))
		fn(name, it.Value())
	}
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

// GenesisWitness is the immutable {address, initial vote count} pair captured
// at genesis. java-tron's tryRemoveThePowerOfTheGr subtracts the *initial*
// vote count (not the current count after vote accumulation), so this list
// is recorded once at genesis setup and never mutated.
type GenesisWitness struct {
	Address   common.Address
	VoteCount int64
}

const genesisWitnessRecordLen = common.AddressLength + 8

func WriteGenesisWitnesses(db ethdb.KeyValueWriter, witnesses []GenesisWitness) {
	buf := make([]byte, 4+len(witnesses)*genesisWitnessRecordLen)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(witnesses)))
	for i, w := range witnesses {
		off := 4 + i*genesisWitnessRecordLen
		copy(buf[off:], w.Address.Bytes())
		binary.BigEndian.PutUint64(buf[off+common.AddressLength:], uint64(w.VoteCount))
	}
	db.Put(genesisWitnessesKey, buf)
}

func ReadGenesisWitnesses(db ethdb.KeyValueReader) []GenesisWitness {
	data, err := db.Get(genesisWitnessesKey)
	if err != nil || len(data) < 4 {
		return nil
	}
	count := int(binary.BigEndian.Uint32(data[:4]))
	if len(data) < 4+count*genesisWitnessRecordLen {
		return nil
	}
	out := make([]GenesisWitness, count)
	for i := 0; i < count; i++ {
		off := 4 + i*genesisWitnessRecordLen
		out[i].Address = common.BytesToAddress(data[off : off+common.AddressLength])
		out[i].VoteCount = int64(binary.BigEndian.Uint64(data[off+common.AddressLength : off+genesisWitnessRecordLen]))
	}
	return out
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

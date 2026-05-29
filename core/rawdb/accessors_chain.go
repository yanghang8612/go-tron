package rawdb

import (
	"bytes"
	"encoding/binary"
	"fmt"

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

func WriteCleanShutdownHeadHash(db ethdb.KeyValueWriter, hash common.Hash) {
	_ = db.Put(cleanShutdownHeadKey, hash.Bytes())
}

func ReadCleanShutdownHeadHash(db ethdb.KeyValueReader) common.Hash {
	data, err := db.Get(cleanShutdownHeadKey)
	if err != nil {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

func DeleteCleanShutdownHeadHash(db ethdb.KeyValueWriter) {
	_ = db.Delete(cleanShutdownHeadKey)
}

func WriteStartupRecoveryTarget(db ethdb.KeyValueWriter, number uint64, hash common.Hash) error {
	var data [8 + common.HashLength]byte
	binary.BigEndian.PutUint64(data[:8], number)
	copy(data[8:], hash.Bytes())
	return db.Put(startupRecoveryTargetKey, data[:])
}

func ReadStartupRecoveryTarget(db ethdb.KeyValueReader) (uint64, common.Hash, bool, error) {
	data, err := db.Get(startupRecoveryTargetKey)
	if err != nil {
		return 0, common.Hash{}, false, nil
	}
	if len(data) != 8+common.HashLength {
		return 0, common.Hash{}, false, fmt.Errorf("startup recovery target: bad length %d", len(data))
	}
	return binary.BigEndian.Uint64(data[:8]), common.BytesToHash(data[8:]), true, nil
}

func DeleteStartupRecoveryTarget(db ethdb.KeyValueWriter) error {
	return db.Delete(startupRecoveryTargetKey)
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
func WriteBlockStateRoot(db ethdb.KeyValueWriter, blockHash, root common.Hash) error {
	return db.Put(blockStateRootKey(blockHash.Bytes()), root.Bytes())
}

// DeleteBlockStateRoot removes the hash-keyed post-apply state root for the
// given block. After this, ReadBlockStateRoot falls through to the freezer
// (frozen blocks) or returns the zero hash, and TronBackend.GetAccountAt
// reconstructs the account from flat temporal history instead of opening a
// StateDB at the (now-absent) root. State-history-aware pruning and tests that
// simulate a pruned state root use this.
func DeleteBlockStateRoot(db ethdb.KeyValueWriter, blockHash common.Hash) {
	db.Delete(blockStateRootKey(blockHash.Bytes()))
}

// ancientStateRoots names the freezer table holding the 32-byte
// post-apply state root for each frozen block, keyed by block number.
// The KV side of the index remains hash-keyed (`bsr-<hash>`); the
// num-keyed ancient is reached via the two-step
// `bh-<hash>` → num → `state_roots[num]` fall-through encoded in
// `ReadBlockStateRoot`.
const ancientStateRoots = "state_roots"

// ReadBlockStateRoot returns the post-apply state root for the given
// block hash, or the zero hash if not stored.
//
// The KV side is the source of truth for live (still-hot) blocks; the
// freezer holds the same value keyed by num for frozen blocks. On a KV
// miss we resolve `hash → num` (`bh-<hash>` is intentionally kept hot by
// the slice-1 freezer spec) and probe the `state_roots` ancient table.
// Paying the extra `Get(bh-<hash>)` only on the miss path keeps the
// hot-block read path single-Get.
func ReadBlockStateRoot(db *ChainDB, blockHash common.Hash) common.Hash {
	if data, err := db.Get(blockStateRootKey(blockHash.Bytes())); err == nil {
		return common.BytesToHash(data)
	}
	// KV miss: try the freezer via the still-hot bh-<hash> reverse index.
	numBytes, err := db.Get(blockHashKey(blockHash.Bytes()))
	if err != nil || len(numBytes) != 8 {
		return common.Hash{}
	}
	num := binary.BigEndian.Uint64(numBytes)
	if data, ok := readAncient(db, ancientStateRoots, num); ok {
		return common.BytesToHash(data)
	}
	return common.Hash{}
}

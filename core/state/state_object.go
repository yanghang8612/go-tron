package state

import (
	"sync"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// kvDirtyMapPool reuses the per-account dirty-KV hash table across commit
// intervals. Values and composite keys still have commit/journal ownership of
// their own backing storage; only the cleared map buckets are pooled.
//
// A transaction can grow a map and then journal-revert most of its entries, so
// len at finalization is not a safe retained-size bound. stateObject tracks the
// high-water mark and drops unusually large maps instead of letting sync.Pool
// keep their bucket arrays alive. A 1024-entry ceiling covers normal hot
// contracts while bounding one pooled map to a modest working-set object.
const maxPooledKVDirtyEntries = 1024

var kvDirtyMapPool sync.Pool

// dirtyStorageMapPool reuses the per-account first-write origin table across
// commit intervals. Keys and values contain no pointers, and commit planning
// consumes every origin before finalize releases the cleared buckets.
//
// Track the high-water mark because a reverted transaction may shrink the map
// before commit. Dropping unusually large maps prevents a single storage-heavy
// contract from becoming the pool's retained steady-state footprint.
const maxPooledDirtyStorageEntries = 1024

var dirtyStorageMapPool sync.Pool

// storageMapPool reuses the private per-stateObject SLOAD cache after the
// object is evicted, deleted, or its oversized cache is discarded. Account
// references exposed outside StateDB do not expose this map, so detaching it
// from the stateObject before clearing makes reuse ownership-safe.
//
// Storage-heavy contracts can touch thousands of unique mapping slots. Retain
// only modest cache maps so the pool reduces ordinary account churn without
// pinning large hash tables after an exceptional block.
const maxPooledStorageEntries = 512

var storageMapPool sync.Pool

// stateObjectPool reuses the private account wrapper after StateDB evicts it
// from the previous block's working set. The wrapper is comparatively large
// (it carries all lazy-load and dirty-tracking state), while historical sync
// creates hundreds of thousands of short-lived wrappers per sampling window.
//
// Only wrappers that have never escaped through GetOrCreateAccount enter the
// pool. AccountReference/GetAccount expose *types.Account rather than the
// wrapper, so clearing the wrapper does not mutate those externally retained
// account values.
var stateObjectPool = sync.Pool{
	New: func() any { return new(stateObject) },
}

// storageOrigin is the durable value observed before a slot's first write in
// the current commit interval. SetState already has to load that value for
// SSTORE semantics, so retaining it lets commit planning avoid reading the
// same flat-latest row again. loaded distinguishes a known absent row from the
// fallback entries produced by direct stateObject tests/helpers.
type storageOrigin struct {
	value  tcommon.Hash
	exists bool
	loaded bool
}

// storageSlot keeps the cached StorageRow value and java-tron row-existence
// bit under one hash-table entry. Keeping these in separate maps duplicated
// every 32-byte storage key and paid two map lookups on each hit/write. Storage
// is the largest live StateDB heap consumer during range sync, so the combined
// entry materially shrinks both the retained working set and GC scan surface.
type storageSlot struct {
	value  tcommon.Hash
	exists bool
}

// stateObject represents an in-memory account with dirty tracking.
type stateObject struct {
	address tcommon.Address
	account *types.Account
	// wrapperEscaped is set when GetOrCreateAccount returns this private wrapper
	// to a caller. Preserve such a stale wrapper after cache eviction rather than
	// reusing its identity for an unrelated address that the caller could then
	// observe through its exported Account method.
	wrapperEscaped bool
	// cacheTouched records membership in StateDB.touchedStateObjects for the
	// current block. StateDB keeps only the previous block's account working set
	// across a successful commit; the bit makes repeated hot-path lookups a
	// predictable branch instead of a map insertion on every access.
	cacheTouched bool
	// accountProto caches the deterministic protobuf bytes for the current
	// account value. It is populated when history/commit serialization already
	// pays that cost and invalidated before every account mutation. Keeping it
	// on the object lets the next transaction touching a hot account reuse the
	// previous transaction's post-image as its pre-image without sorting all
	// protobuf map fields again.
	accountProto []byte
	// accountProtoLoaded marks bytes borrowed from the flat account envelope at
	// first load. They are useful as a same-block journal pre-image, but an
	// untouched read-only account must not retain a second full representation
	// for the rest of a long range import. StateDB clears still-marked entries at
	// the successful commit boundary; any mutation invalidates the marker.
	accountProtoLoaded bool
	dirty              bool
	// accountDirty tracks protobuf-account envelope changes separately from
	// rooted KV/storage/code changes so net-zero KV overlays can skip account
	// trie updates at commit.
	accountDirty bool
	// Split account fields are materialized only for full Account responses or
	// the typed mutators that need them.
	accountMapsLoaded            bool
	accountPermissionsLoaded     bool
	accountVotesLoaded           bool
	accountStakeV2Loaded         bool
	accountFrozenSupplyLoaded    bool
	accountResourceLoaded        bool
	accountFrozenBandwidthLoaded bool
	accountTronPowerLoaded       bool
	// accountFrozenBandwidthCanonicalPooled marks Account.Frozen as backed by a
	// one-element array borrowed from the canonical V1 bandwidth cache pool.
	accountFrozenBandwidthCanonicalPooled bool
	// FrozenV2 uses three point-addressable resource keys. Resource accounting
	// reads BANDWIDTH more than once per transaction, so retain typed point-read
	// results without materializing the unrelated V2 unfreeze queue.
	accountFrozenV2PointLoaded  uint8
	accountFrozenV2PointExists  uint8
	accountFrozenV2PointAmounts [3]int64
	deleted                     bool
	created                     bool

	// Contract fields
	code              []byte                    // contract bytecode
	codeHash          tcommon.Hash              // Keccak-256 hash of the code
	codeDirty         bool                      // true if code was modified
	contractMeta      *contractpb.SmartContract // contract metadata
	contractMetaDirty bool                      // true if contractMeta was modified
	// contractRuntime caches the scalar metadata and storage-key layout decoded
	// directly from the committed wire value. Ordinary VM execution does not
	// need to materialize the embedded ABI graph in SmartContract.
	contractRuntime       ContractRuntimeMetadata
	contractRuntimeLoaded bool
	contractRuntimeExists bool
	// storageKeyPrefix is the java StorageRow address-derived prefix. Every
	// slot of one contract shares it, so cache the Keccak result instead of
	// hashing address||creationTxHash for every first SLOAD/SSTORE.
	storageKeyPrefix       [storageKeyPrefixBytes]byte
	storageKeyLayoutCached bool
	storageKeyHashSlot     bool
	storage                map[tcommon.Hash]storageSlot // cached current contract storage and StorageRow existence
	storageHighWater       int
	dirtyStorage           map[tcommon.Hash]storageOrigin // slots written this block and their pre-write values
	dirtyStorageHighWater  int
	selfDestructed         bool

	// Generic-KV generation is the Erigon-style incarnation number. AccountKVRoot
	// is retained in the envelope as EmptyKVRoot while the flat latest rows carry
	// actual generic-KV content.
	accountKVRoot            tcommon.Hash
	accountKVGeneration      uint64
	accountKVGenerationDirty bool

	// kvDirty holds pending generic-KV writes keyed by string(domainBE2||key).
	kvDirty          map[string]kvEntry
	kvDirtyHighWater int

	// dirtySet is a back-pointer to the owning StateDB's dirtyObjects set. It is
	// set when the object enters the cache (getStateObject / GetOrCreateAccount /
	// Copy) so that markDirty can record this object's address without the
	// StateDB needing to scan. nil for detached objects (none ever mutated).
	dirtySet map[tcommon.Address]struct{}
}

func (s *stateObject) deterministicAccountProto() ([]byte, error) {
	if s == nil || s.account == nil {
		return nil, nil
	}
	if s.accountProto != nil {
		return s.accountProto, nil
	}
	data, err := s.account.MarshalStorageCore()
	if err != nil {
		return nil, err
	}
	s.accountProto = data
	s.accountProtoLoaded = false
	return data, nil
}

func (s *stateObject) invalidateAccountProto() {
	if s != nil {
		s.accountProto = nil
		s.accountProtoLoaded = false
	}
}

func newStateObject(addr tcommon.Address, acc *types.Account) *stateObject {
	obj := acquireStateObject()
	*obj = stateObject{
		address:       addr,
		account:       acc,
		accountKVRoot: EmptyKVRoot,
	}
	return obj
}

func newEmptyStateObject(addr tcommon.Address) *stateObject {
	obj := acquireStateObject()
	*obj = stateObject{
		address:       addr,
		account:       types.NewAccount(addr, corepb.AccountType_Normal),
		dirty:         true,
		accountDirty:  true,
		created:       true,
		accountKVRoot: EmptyKVRoot,
	}
	return obj
}

func acquireStateObject() *stateObject {
	// New entries start zeroed and releaseStateObject clears every reused entry
	// before Put. Constructors overwrite the full struct immediately afterward.
	return stateObjectPool.Get().(*stateObject)
}

// releaseStateObject detaches private reusable maps and, when the wrapper has
// not escaped, returns the cleared wrapper to the pool. Callers must first
// remove obj from stateObjects and clear every StateDB pointer to it.
func releaseStateObject(obj *stateObject) {
	if obj == nil {
		return
	}
	obj.releaseStorage()
	obj.releaseDirtyStorage()
	obj.releaseKVDirty()
	if obj.wrapperEscaped {
		return
	}
	*obj = stateObject{}
	stateObjectPool.Put(obj)
}

func (s *stateObject) markDirty() {
	s.dirty = true
	if s.dirtySet != nil {
		s.dirtySet[s.address] = struct{}{}
	}
}

func (s *stateObject) ensureStorage() {
	if s.storage != nil {
		return
	}
	if pooled := storageMapPool.Get(); pooled != nil {
		s.storage = pooled.(map[tcommon.Hash]storageSlot)
	} else {
		s.storage = make(map[tcommon.Hash]storageSlot)
	}
}

func (s *stateObject) cacheStorageSlot(key tcommon.Hash, slot storageSlot) {
	s.ensureStorage()
	s.storage[key] = slot
	if len(s.storage) > s.storageHighWater {
		s.storageHighWater = len(s.storage)
	}
}

func (s *stateObject) releaseStorage() {
	storage := s.storage
	highWater := s.storageHighWater
	s.storage = nil
	s.storageHighWater = 0
	if storage == nil {
		return
	}
	if len(storage) > highWater {
		highWater = len(storage)
	}
	clear(storage)
	if highWater <= maxPooledStorageEntries {
		storageMapPool.Put(storage)
	}
}

func (s *stateObject) ensureDirtyStorage() {
	if s.dirtyStorage != nil {
		return
	}
	if pooled := dirtyStorageMapPool.Get(); pooled != nil {
		s.dirtyStorage = pooled.(map[tcommon.Hash]storageOrigin)
	} else {
		s.dirtyStorage = make(map[tcommon.Hash]storageOrigin)
	}
}

func (s *stateObject) recordDirtyStorageOrigin(key tcommon.Hash, origin storageOrigin) {
	s.ensureDirtyStorage()
	s.dirtyStorage[key] = origin
	if len(s.dirtyStorage) > s.dirtyStorageHighWater {
		s.dirtyStorageHighWater = len(s.dirtyStorage)
	}
}

// releaseDirtyStorage detaches the origin table after commit planning has
// consumed it. The stateObject remains observably clean (nil map), while a
// later contract/account can reuse the cleared bucket array.
func (s *stateObject) releaseDirtyStorage() {
	dirty := s.dirtyStorage
	highWater := s.dirtyStorageHighWater
	s.dirtyStorage = nil
	s.dirtyStorageHighWater = 0
	if dirty == nil {
		return
	}
	if len(dirty) > highWater {
		highWater = len(dirty)
	}
	clear(dirty)
	if highWater <= maxPooledDirtyStorageEntries {
		dirtyStorageMapPool.Put(dirty)
	}
}

func (s *stateObject) ensureKVDirty() {
	if s.kvDirty == nil {
		if pooled := kvDirtyMapPool.Get(); pooled != nil {
			s.kvDirty = pooled.(map[string]kvEntry)
		} else {
			s.kvDirty = make(map[string]kvEntry)
		}
	}
}

func (s *stateObject) setKVDirty(mapKey string, entry kvEntry) {
	s.ensureKVDirty()
	s.kvDirty[mapKey] = entry
	if len(s.kvDirty) > s.kvDirtyHighWater {
		s.kvDirtyHighWater = len(s.kvDirty)
	}
}

// releaseKVDirty detaches and clears the dirty map before making its buckets
// available to another account. Callers must first finish every operation that
// borrows map keys or entries (commit plans do so through finalization).
func (s *stateObject) releaseKVDirty() {
	dirty := s.kvDirty
	highWater := s.kvDirtyHighWater
	s.kvDirty = nil
	s.kvDirtyHighWater = 0
	if dirty == nil {
		return
	}
	// Directly constructed test/genesis objects may install a map without
	// going through setKVDirty. Current length is still a valid lower bound;
	// the tracked value preserves a larger pre-revert high-water mark.
	if len(dirty) > highWater {
		highWater = len(dirty)
	}
	clear(dirty)
	if highWater <= maxPooledKVDirtyEntries {
		kvDirtyMapPool.Put(dirty)
	}
}

// Account returns the underlying account for direct mutation during genesis setup.
func (s *stateObject) Account() *types.Account { return s.account }

func (s *stateObject) setCode(code []byte) {
	if len(code) == 0 {
		s.code = nil
		// java-tron RepositoryImpl.saveCode always records Hash.sha3(code)
		// once Constantinople is active, including for an empty runtime.  A
		// zero hash means "not recorded"; it is not the hash of empty code.
		s.codeHash = tcommon.Keccak256(nil)
	} else {
		s.code = make([]byte, len(code))
		copy(s.code, code)
		s.codeHash = tcommon.Keccak256(code)
	}
	s.codeDirty = true
	s.markDirty()
}

func (s *stateObject) getStorage(key tcommon.Hash) tcommon.Hash {
	return s.storage[key].value
}

func (s *stateObject) getStorageWithExist(key tcommon.Hash) (tcommon.Hash, bool, bool) {
	slot, cached := s.storage[key]
	if !cached {
		return tcommon.Hash{}, false, false
	}
	return slot.value, slot.exists, true
}

func (s *stateObject) setStorage(key, value tcommon.Hash, exists bool) {
	if _, dirty := s.dirtyStorage[key]; !dirty {
		// Production writes install a loaded origin in SetState before calling
		// here. Keep direct helper calls correct by leaving an explicit fallback
		// entry that makes commit planning use the durable reader.
		s.recordDirtyStorageOrigin(key, storageOrigin{})
	}
	s.setStorageValue(key, value, exists)
}

// setStorageValue updates a slot after StateDB.SetState has already installed
// its durable origin. Keeping that production fast path separate avoids a
// second dirtyStorage lookup on every SSTORE; setStorage retains the defensive
// origin setup used by direct stateObject helpers and tests.
func (s *stateObject) setStorageValue(key, value tcommon.Hash, exists bool) {
	s.cacheStorageSlot(key, storageSlot{value: value, exists: exists})
	s.markDirty()
}

func (s *stateObject) stageKV(domain kvdomains.KVDomain, key, value []byte) {
	s.setKVDirty(kvCompositeKeyString(domain, key), newKVEntry(value, false))
	s.markDirty()
}

func (s *stateObject) stageDeleteKV(domain kvdomains.KVDomain, key []byte) {
	s.setKVDirty(kvCompositeKeyString(domain, key), newKVEntry(nil, true))
	s.markDirty()
}

func (s *stateObject) markSelfDestructed() {
	s.selfDestructed = true
	s.markDirty()
}

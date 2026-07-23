package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

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
	deleted      bool
	created      bool

	// Contract fields
	code              []byte                         // contract bytecode
	codeHash          tcommon.Hash                   // Keccak-256 hash of the code
	codeDirty         bool                           // true if code was modified
	contractMeta      *contractpb.SmartContract      // contract metadata
	contractMetaDirty bool                           // true if contractMeta was modified
	storage           map[tcommon.Hash]storageSlot   // cached current contract storage and StorageRow existence
	dirtyStorage      map[tcommon.Hash]storageOrigin // slots written this block and their pre-write values
	selfDestructed    bool

	// Generic-KV generation is the Erigon-style incarnation number. AccountKVRoot
	// is retained in the envelope as EmptyKVRoot while the flat latest rows carry
	// actual generic-KV content.
	accountKVRoot            tcommon.Hash
	accountKVGeneration      uint64
	accountKVGenerationDirty bool

	// kvDirty holds pending generic-KV writes keyed by string(domainBE2||key).
	kvDirty map[string]kvEntry

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
	data, err := s.account.Marshal()
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
	return &stateObject{
		address:       addr,
		account:       acc,
		storage:       make(map[tcommon.Hash]storageSlot),
		accountKVRoot: EmptyKVRoot,
		kvDirty:       make(map[string]kvEntry),
	}
}

func newEmptyStateObject(addr tcommon.Address) *stateObject {
	return &stateObject{
		address:       addr,
		account:       types.NewAccount(addr, corepb.AccountType_Normal),
		dirty:         true,
		accountDirty:  true,
		created:       true,
		storage:       make(map[tcommon.Hash]storageSlot),
		accountKVRoot: EmptyKVRoot,
		kvDirty:       make(map[string]kvEntry),
	}
}

func (s *stateObject) markDirty() {
	s.dirty = true
	if s.dirtySet != nil {
		s.dirtySet[s.address] = struct{}{}
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
	if s.dirtyStorage == nil {
		s.dirtyStorage = make(map[tcommon.Hash]storageOrigin)
	}
	if _, dirty := s.dirtyStorage[key]; !dirty {
		// Production writes install a loaded origin in SetState before calling
		// here. Keep direct helper calls correct by leaving an explicit fallback
		// entry that makes commit planning use the durable reader.
		s.dirtyStorage[key] = storageOrigin{}
	}
	s.storage[key] = storageSlot{value: value, exists: exists}
	s.markDirty()
}

func (s *stateObject) stageKV(domain kvdomains.KVDomain, key, value []byte) {
	comp := kvCompositeKey(domain, key)
	s.kvDirty[string(comp)] = newKVEntry(comp, value, false)
	s.markDirty()
}

func (s *stateObject) stageDeleteKV(domain kvdomains.KVDomain, key []byte) {
	comp := kvCompositeKey(domain, key)
	s.kvDirty[string(comp)] = newKVEntry(comp, nil, true)
	s.markDirty()
}

func (s *stateObject) markSelfDestructed() {
	s.selfDestructed = true
	s.markDirty()
}

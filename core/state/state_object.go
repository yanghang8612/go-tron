package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// stateObject represents an in-memory account with dirty tracking.
type stateObject struct {
	address tcommon.Address
	account *types.Account
	dirty   bool
	deleted bool
	created bool

	// Contract fields
	code              []byte                        // contract bytecode
	codeHash          tcommon.Hash                  // Keccak-256 hash of the code
	codeDirty         bool                          // true if code was modified
	contractMeta      *contractpb.SmartContract     // contract metadata
	contractMetaDirty bool                          // true if contractMeta was modified
	storage           map[tcommon.Hash]tcommon.Hash // dirty contract storage
	storageExists     map[tcommon.Hash]bool         // java-tron StorageRow existence for cached slots
	selfDestructed    bool

	// Rooted generic-KV fields: root of this account's per-account KV trie
	// (committed into the StateAccountV2 envelope) and its reset generation.
	accountKVRoot            tcommon.Hash
	accountKVGeneration      uint64
	accountKVGenerationDirty bool

	// kvDirty holds pending generic-KV writes keyed by string(domainBE2||key).
	kvDirty map[string]kvEntry
}

func newStateObject(addr tcommon.Address, acc *types.Account) *stateObject {
	return &stateObject{
		address:       addr,
		account:       acc,
		storage:       make(map[tcommon.Hash]tcommon.Hash),
		storageExists: make(map[tcommon.Hash]bool),
		accountKVRoot: EmptyKVRoot,
		kvDirty:       make(map[string]kvEntry),
	}
}

func newEmptyStateObject(addr tcommon.Address) *stateObject {
	return &stateObject{
		address:       addr,
		account:       types.NewAccount(addr, corepb.AccountType_Normal),
		dirty:         true,
		created:       true,
		storage:       make(map[tcommon.Hash]tcommon.Hash),
		storageExists: make(map[tcommon.Hash]bool),
		accountKVRoot: EmptyKVRoot,
		kvDirty:       make(map[string]kvEntry),
	}
}

func (s *stateObject) markDirty() {
	s.dirty = true
}

// Account returns the underlying account for direct mutation during genesis setup.
func (s *stateObject) Account() *types.Account { return s.account }

func (s *stateObject) setCode(code []byte) {
	if len(code) == 0 {
		s.code = nil
		s.codeHash = tcommon.Hash{}
	} else {
		s.code = make([]byte, len(code))
		copy(s.code, code)
		s.codeHash = tcommon.Keccak256(code)
	}
	s.codeDirty = true
	s.markDirty()
}

func (s *stateObject) getStorage(key tcommon.Hash) tcommon.Hash {
	return s.storage[key]
}

func (s *stateObject) getStorageWithExist(key tcommon.Hash) (tcommon.Hash, bool, bool) {
	value, cached := s.storage[key]
	if !cached {
		return tcommon.Hash{}, false, false
	}
	return value, s.storageExists[key], true
}

func (s *stateObject) setStorage(key, value tcommon.Hash, exists bool) {
	s.storage[key] = value
	s.storageExists[key] = exists
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

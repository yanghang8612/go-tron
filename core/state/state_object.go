package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
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

	// Contract fields
	code              []byte                        // contract bytecode
	codeHash          tcommon.Hash                  // SHA256 hash of the code
	codeDirty         bool                          // true if code was modified
	contractMeta      *contractpb.SmartContract     // contract metadata
	contractMetaDirty bool                          // true if contractMeta was modified
	storage           map[tcommon.Hash]tcommon.Hash // dirty contract storage
	selfDestructed    bool
}

func newStateObject(addr tcommon.Address, acc *types.Account) *stateObject {
	return &stateObject{
		address: addr,
		account: acc,
		storage: make(map[tcommon.Hash]tcommon.Hash),
	}
}

func newEmptyStateObject(addr tcommon.Address) *stateObject {
	return &stateObject{
		address: addr,
		account: types.NewAccount(addr, corepb.AccountType_Normal),
		dirty:   true,
		storage: make(map[tcommon.Hash]tcommon.Hash),
	}
}

func (s *stateObject) markDirty() {
	s.dirty = true
}

// Account returns the underlying account for direct mutation during genesis setup.
func (s *stateObject) Account() *types.Account { return s.account }

func (s *stateObject) setCode(code []byte) {
	s.code = make([]byte, len(code))
	copy(s.code, code)
	s.codeHash = tcommon.Sha256(code)
	s.codeDirty = true
	s.markDirty()
}

func (s *stateObject) getStorage(key tcommon.Hash) tcommon.Hash {
	return s.storage[key]
}

func (s *stateObject) setStorage(key, value tcommon.Hash) {
	s.storage[key] = value
	s.markDirty()
}

func (s *stateObject) markSelfDestructed() {
	s.selfDestructed = true
	s.markDirty()
}

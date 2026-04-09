package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// stateObject represents an in-memory account with dirty tracking.
type stateObject struct {
	address tcommon.Address
	account *types.Account
	dirty   bool
	deleted bool
}

func newStateObject(addr tcommon.Address, acc *types.Account) *stateObject {
	return &stateObject{
		address: addr,
		account: acc,
	}
}

func newEmptyStateObject(addr tcommon.Address) *stateObject {
	return &stateObject{
		address: addr,
		account: types.NewAccount(addr, corepb.AccountType_Normal),
		dirty:   true,
	}
}

func (s *stateObject) markDirty() {
	s.dirty = true
}

// Account returns the underlying account for direct mutation during genesis setup.
func (s *stateObject) Account() *types.Account { return s.account }

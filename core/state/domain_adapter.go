package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

var (
	_ statedomains.LatestReader = (*DomainState)(nil)
	_ statedomains.Writer       = (*DomainState)(nil)
)

// DomainState adapts the current StateDB account-KV trie to the Phase-1 domain
// engine interfaces. The underlying MPT/account-KV root remains authoritative.
type DomainState struct {
	state *StateDB
}

func NewDomainState(s *StateDB) *DomainState {
	return &DomainState{state: s}
}

func (s *StateDB) Domains() *DomainState {
	return NewDomainState(s)
}

func (d *DomainState) GetLatest(owner tcommon.Address, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	if d == nil || d.state == nil {
		return nil, false, nil
	}
	return d.state.GetAccountKV(owner, domain, key)
}

func (d *DomainState) DomainPut(owner tcommon.Address, domain kvdomains.KVDomain, key, value []byte) error {
	if d == nil || d.state == nil {
		return nil
	}
	return d.state.SetAccountKV(owner, domain, key, value)
}

func (d *DomainState) DomainDel(owner tcommon.Address, domain kvdomains.KVDomain, key []byte) error {
	if d == nil || d.state == nil {
		return nil
	}
	return d.state.DeleteAccountKV(owner, domain, key)
}

func (d *DomainState) DomainDelPrefix(owner tcommon.Address, domain kvdomains.KVDomain, prefix []byte) error {
	if d == nil || d.state == nil {
		return nil
	}
	return d.state.DeleteAccountKVPrefix(owner, domain, prefix)
}

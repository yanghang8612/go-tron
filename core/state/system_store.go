package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// SystemKVGet reads a chain-global value owned by the reserved system account.
func (s *StateDB) SystemKVGet(domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	return s.GetAccountKV(tcommon.SystemAccountAddress, domain, key)
}

// SystemKVPut writes a chain-global value owned by the reserved system account.
func (s *StateDB) SystemKVPut(domain kvdomains.KVDomain, key, value []byte) error {
	return s.SetAccountKV(tcommon.SystemAccountAddress, domain, key, value)
}

// SystemKVPutFinal writes a block-final chain-global value without transaction
// snapshot journaling. It is for post-transaction block bookkeeping only.
func (s *StateDB) SystemKVPutFinal(domain kvdomains.KVDomain, key, value []byte) error {
	return s.SetAccountKVFinal(tcommon.SystemAccountAddress, domain, key, value)
}

// SystemKVDelete removes a chain-global value owned by the reserved system account.
func (s *StateDB) SystemKVDelete(domain kvdomains.KVDomain, key []byte) error {
	return s.DeleteAccountKV(tcommon.SystemAccountAddress, domain, key)
}

// SystemKVGetBatch resolves many keys in one domain with a single KV-trie open.
func (s *StateDB) SystemKVGetBatch(domain kvdomains.KVDomain, keys [][]byte) (map[string][]byte, error) {
	return s.GetAccountKVBatch(tcommon.SystemAccountAddress, domain, keys)
}

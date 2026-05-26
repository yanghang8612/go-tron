package domains

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// rawDBLatestKVStore is the compatibility adapter between FlatStore's typed
// latest-KV boundary and the current rawdb latest keyspace.
type rawDBLatestKVStore struct {
	db latestStateDB
}

func newRawDBLatestKVStore(db latestStateDB) latestKVStore {
	if db == nil {
		return nil
	}
	return rawDBLatestKVStore{db: db}
}

func (s rawDBLatestKVStore) GetLatest(owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	return rawdb.ReadStateKVLatest(s.db, owner, generation, domain, key)
}

func (s rawDBLatestKVStore) PutLatest(owner common.Address, generation uint64, domain kvdomains.KVDomain, key, value []byte) error {
	return rawdb.WriteStateKVLatest(s.db, owner, generation, domain, key, value)
}

func (s rawDBLatestKVStore) DeleteLatest(owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) error {
	return rawdb.DeleteStateKVLatest(s.db, owner, generation, domain, key)
}

func (s rawDBLatestKVStore) DeleteLatestPrefix(owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte) error {
	return rawdb.DeleteStateKVLatestPrefix(s.db, owner, generation, domain, prefix)
}

func (s rawDBLatestKVStore) IterateLatest(owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn IterateFunc) error {
	return rawdb.IterateStateKVLatest(s.db, owner, generation, domain, prefix, fn)
}

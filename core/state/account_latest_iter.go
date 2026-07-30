package state

import (
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// IterateAccountLatestRows exposes physical latest-account enumeration through
// the state boundary for offline diagnostics and snapshot builders.
func IterateAccountLatestRows(db ethdb.Iteratee, ownerPrefix []byte, fn func(owner tcommon.Address, value []byte) (bool, error)) error {
	return rawdb.IterateStateAccountLatest(db, ownerPrefix, func(row rawdb.StateAccountLatestRow) (bool, error) {
		return fn(row.Owner, row.Value)
	})
}

// IterateAccountLatest walks latest account rows through the state package
// boundary. Callers must not retain value after fn returns.
func (db *Database) IterateAccountLatest(ownerPrefix []byte, fn func(owner tcommon.Address, value []byte) (bool, error)) error {
	if db == nil || db.disk == nil || fn == nil {
		return nil
	}
	return IterateAccountLatestRows(db.disk, ownerPrefix, fn)
}

// IterateKVLatestDomainRows and ReadKVGenerationLatest are narrow physical
// latest-state boundaries used by the java-tron database comparison tool.
func IterateKVLatestDomainRows(db ethdb.Iteratee, domain kvdomains.KVDomain, fn func(rawdb.StateKVLatestRow) (bool, error)) error {
	return rawdb.IterateStateKVLatestDomainRows(db, domain, fn)
}

func ReadKVGenerationLatest(db ethdb.KeyValueReader, owner tcommon.Address) (uint64, bool, error) {
	return rawdb.ReadStateKVGeneration(db, owner)
}

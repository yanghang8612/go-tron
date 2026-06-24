package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// IterateAccountLatest walks latest account rows through the state package
// boundary. Callers must not retain value after fn returns.
func (db *Database) IterateAccountLatest(ownerPrefix []byte, fn func(owner tcommon.Address, value []byte) (bool, error)) error {
	if db == nil || db.disk == nil || fn == nil {
		return nil
	}
	return rawdb.IterateStateAccountLatest(db.disk, ownerPrefix, func(row rawdb.StateAccountLatestRow) (bool, error) {
		return fn(row.Owner, row.Value)
	})
}

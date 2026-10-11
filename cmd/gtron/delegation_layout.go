package main

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// Only fresh chains may adopt paged delegation. Rewriting latest rows alone
// would leave archive queries with an incompatible history representation.
// Startup performs fixed-key reads; it does not scan or certify the database.
func ensurePagedDelegationLayout(db ethdb.KeyValueStore) error {
	marked, err := rawdb.ReadPagedDelegationLayout(db)
	if err != nil || marked {
		return err
	}
	_, hasHead, err := rawdb.ReadHeadBlockHashStrict(db)
	if err != nil {
		return err
	}
	if hasHead {
		return errors.New("paged delegation requires a fresh datadir and sync from genesis; existing aggregate-list history is unsupported")
	}
	return rawdb.MarkPagedDelegationLayout(db)
}

package rawdb

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
)

// HistoryStagingMetadataPresent is a read-only, schema-owned preplan check.
// Any staging key, including a partial epoch or orphan claim without an
// identity, disqualifies a source from being repinned as pristine.
func HistoryStagingMetadataPresent(db ethdb.Iteratee) (bool, error) {
	if db == nil {
		return false, errors.New("rawdb: missing history staging source iterator")
	}
	it := db.NewIterator(historyStagingMetadataPrefix, nil)
	defer it.Release()
	present := it.Next()
	if err := it.Error(); err != nil {
		return false, err
	}
	return present, nil
}

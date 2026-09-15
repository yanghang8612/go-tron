package rawdb

import (
	"fmt"
	"github.com/ethereum/go-ethereum/ethdb"
)

// Frozen verbatim from 80024781 accessors_read.go; only function name changed.
func readPresentValueBeforeOwnedOracle(db ethdb.KeyValueReader, key []byte, context string) ([]byte, bool, error) {
	// Buffered readers can keep an overlay key visible only until a concurrent
	// reorg replaces its layer. Ask them for one presence-coupled view instead
	// of allowing a Has/Get pair to straddle that replacement. Plain databases
	// retain the explicit Has-first error semantics below.
	if reader, ok := db.(interface {
		GetWithPresence([]byte) ([]byte, bool, error)
	}); ok {
		value, exists, err := reader.GetWithPresence(key)
		if err != nil {
			return nil, false, fmt.Errorf("rawdb: read %s: %w", context, err)
		}
		return value, exists, nil
	}
	exists, err := readKeyPresence(db, key, context)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		return nil, false, nil
	}
	value, err := db.Get(key)
	if err != nil {
		return nil, false, fmt.Errorf("rawdb: read %s: %w", context, err)
	}
	return append([]byte(nil), value...), true, nil
}

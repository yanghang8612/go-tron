package rawdb

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
)

// StateChangeIndexFamily selects one physical latest-domain family embedded in
// state-change-index keys.
type StateChangeIndexFamily string

const (
	StateChangeIndexAll           StateChangeIndexFamily = "all"
	StateChangeIndexAccountLatest StateChangeIndexFamily = "account-latest"
	StateChangeIndexKVGeneration  StateChangeIndexFamily = "kv-generation"
	StateChangeIndexKVLatest      StateChangeIndexFamily = "kv-latest"
)

// StateChangeIndexScanOptions bounds a read-only sequential index scan. A zero
// MaxRows scans the selected family completely.
type StateChangeIndexScanOptions struct {
	Family  StateChangeIndexFamily
	MaxRows uint64
}

// StateChangeIndexRow borrows its key slices from the underlying iterator.
// Callers must consume them before the callback returns.
type StateChangeIndexRow struct {
	PhysicalKey []byte
	LatestKey   []byte
	BlockNum    uint64
	ValueBytes  int
}

type StateChangeIndexScanResult struct {
	Rows     uint64
	Complete bool
}

// IterateStateChangeIndexRows walks one ordered physical index family without
// decoding its referenced changesets. The callback observes borrowed key bytes
// and may stop the scan by returning false.
func IterateStateChangeIndexRows(db ethdb.Iteratee, opts StateChangeIndexScanOptions, fn func(StateChangeIndexRow) (bool, error)) (StateChangeIndexScanResult, error) {
	if db == nil {
		return StateChangeIndexScanResult{}, fmt.Errorf("rawdb: nil state change index database")
	}
	if fn == nil {
		return StateChangeIndexScanResult{}, fmt.Errorf("rawdb: nil state change index callback")
	}
	familyPrefix, err := stateChangeIndexFamilyPrefix(opts.Family)
	if err != nil {
		return StateChangeIndexScanResult{}, err
	}
	physicalPrefix := make([]byte, 0, len(stateChangeInversePrefix)+len(familyPrefix))
	physicalPrefix = append(physicalPrefix, stateChangeInversePrefix...)
	physicalPrefix = append(physicalPrefix, familyPrefix...)
	it := db.NewIterator(physicalPrefix, nil)
	defer it.Release()
	result := StateChangeIndexScanResult{Complete: true}
	for it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, physicalPrefix) {
			break
		}
		if len(key) < len(stateChangeInversePrefix)+8 {
			return StateChangeIndexScanResult{}, fmt.Errorf("rawdb: malformed state change index key length %d", len(key))
		}
		latestEnd := len(key) - 8
		row := StateChangeIndexRow{
			PhysicalKey: key,
			LatestKey:   key[len(stateChangeInversePrefix):latestEnd],
			BlockNum:    binary.BigEndian.Uint64(key[latestEnd:]),
			ValueBytes:  len(it.Value()),
		}
		cont, err := fn(row)
		if err != nil {
			return StateChangeIndexScanResult{}, err
		}
		result.Rows++
		if !cont {
			result.Complete = false
			return result, nil
		}
		if opts.MaxRows != 0 && result.Rows >= opts.MaxRows {
			result.Complete = false
			return result, nil
		}
	}
	if err := it.Error(); err != nil {
		return StateChangeIndexScanResult{}, err
	}
	return result, nil
}

func stateChangeIndexFamilyPrefix(family StateChangeIndexFamily) ([]byte, error) {
	switch family {
	case "", StateChangeIndexAll:
		return nil, nil
	case StateChangeIndexAccountLatest:
		return stateAccountLatestPrefix, nil
	case StateChangeIndexKVGeneration:
		return stateKVGenerationPrefix, nil
	case StateChangeIndexKVLatest:
		return stateKVLatestPrefix, nil
	default:
		return nil, fmt.Errorf("rawdb: unknown state change index family %q", family)
	}
}

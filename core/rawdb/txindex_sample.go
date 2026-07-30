package rawdb

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
)

const maxTransactionIndexSampleWindows = uint64(1 << 16)

// TransactionIndexSample is one immutable transaction reverse-index row. Hash
// excludes the tx- schema prefix; Location is the encoded eight-byte value so
// candidate layouts can preserve both legacy and packed locators byte-for-byte.
type TransactionIndexSample struct {
	Hash     [32]byte
	Location uint64
}

// TransactionIndexSampleProgress describes a bounded hash-space sample.
type TransactionIndexSampleProgress struct {
	Rows    uint64
	Windows uint64
	Elapsed time.Duration
}

// TransactionIndexSampleOptions controls SampleTransactionIndexes.
type TransactionIndexSampleOptions struct {
	Rows             uint64
	Windows          uint64
	ProgressInterval time.Duration
	Progress         func(TransactionIndexSampleProgress)
}

// VisitTransactionIndexesByBlockRange scans tx-* in hash order and invokes
// visit for locations in the half-open block range. The stable hash ordering
// is suitable for streaming directly into an immutable transaction-index run.
// It returns the total live tx-* rows scanned and the rows selected.
func VisitTransactionIndexesByBlockRange(db ethdb.Iteratee, startBlock, endBlock uint64, visit func(TransactionIndexSample) error) (scanned, selected uint64, err error) {
	if db == nil {
		return 0, 0, fmt.Errorf("visit transaction indexes: nil database")
	}
	if endBlock < startBlock {
		return 0, 0, fmt.Errorf("visit transaction indexes: invalid block range [%d,%d)", startBlock, endBlock)
	}
	if visit == nil {
		return 0, 0, fmt.Errorf("visit transaction indexes: nil visitor")
	}
	scanned, err = VisitTransactionIndexes(db, func(sample TransactionIndexSample) error {
		location := sample.Location
		block := transactionIndexLocationBlock(location)
		if block < startBlock || block >= endBlock {
			return nil
		}
		if err := visit(sample); err != nil {
			return err
		}
		selected++
		return nil
	})
	if err != nil {
		return scanned, selected, err
	}
	return scanned, selected, nil
}

// VisitTransactionIndexes scans every live tx-* row once in full-hash order.
// It validates the physical key/value shape before exposing each immutable
// sample. Offline migration uses this to replace the covered prefix while
// preserving the small unarchived hot tail in one atomic Pebble batch.
func VisitTransactionIndexes(db ethdb.Iteratee, visit func(TransactionIndexSample) error) (scanned uint64, err error) {
	if db == nil {
		return 0, fmt.Errorf("visit transaction indexes: nil database")
	}
	if visit == nil {
		return 0, fmt.Errorf("visit transaction indexes: nil visitor")
	}
	it := db.NewIterator(txPrefix, nil)
	defer it.Release()
	for it.Next() {
		key, value := it.Key(), it.Value()
		scanned++
		if len(key) != len(txPrefix)+len(TransactionIndexSample{}.Hash) {
			return scanned, fmt.Errorf("visit transaction indexes: malformed tx key length %d", len(key))
		}
		if len(value) != 8 {
			return scanned, fmt.Errorf("visit transaction indexes: malformed tx value length %d for %x", len(value), key[len(txPrefix):])
		}
		var sample TransactionIndexSample
		copy(sample.Hash[:], key[len(txPrefix):])
		sample.Location = binary.BigEndian.Uint64(value)
		if err := visit(sample); err != nil {
			return scanned, err
		}
	}
	if err := it.Error(); err != nil {
		return scanned, fmt.Errorf("visit transaction indexes: %w", err)
	}
	return scanned, nil
}

// SampleTransactionIndexes reads up to Rows tx-* records from evenly spaced,
// disjoint ranges of the first 16 hash bits. Transaction IDs are SHA-256 values
// and therefore uniformly distributed; seeking to every window makes the work
// bounded without scanning the complete index or biasing the sample toward one
// contiguous part of the hash space.
func SampleTransactionIndexes(db ethdb.Iteratee, opts TransactionIndexSampleOptions) ([]TransactionIndexSample, error) {
	if db == nil {
		return nil, fmt.Errorf("sample transaction indexes: nil database")
	}
	if opts.Rows == 0 {
		return nil, fmt.Errorf("sample transaction indexes: row count must be positive")
	}
	if opts.Rows > uint64(^uint32(0)) {
		return nil, fmt.Errorf("sample transaction indexes: row count %d exceeds %d", opts.Rows, uint64(^uint32(0)))
	}
	if opts.Windows == 0 || opts.Windows > maxTransactionIndexSampleWindows {
		return nil, fmt.Errorf("sample transaction indexes: windows must be in [1,%d]", maxTransactionIndexSampleWindows)
	}
	if opts.Windows > opts.Rows {
		opts.Windows = opts.Rows
	}
	capacity := opts.Rows
	// Avoid an architecture-dependent uint64 -> int overflow in make.
	if uint64(int(capacity)) != capacity {
		return nil, fmt.Errorf("sample transaction indexes: row count %d exceeds addressable memory", capacity)
	}
	samples := make([]TransactionIndexSample, 0, int(capacity))
	started := time.Now()
	lastProgress := started
	for window := uint64(0); window < opts.Windows; window++ {
		wanted := opts.Rows / opts.Windows
		if window < opts.Rows%opts.Windows {
			wanted++
		}
		start16 := window * (1 << 16) / opts.Windows
		end16 := (window + 1) * (1 << 16) / opts.Windows
		var start [32]byte
		binary.BigEndian.PutUint16(start[:2], uint16(start16))
		it := db.NewIterator(txPrefix, start[:])
		var taken uint64
		for taken < wanted && it.Next() {
			key, value := it.Key(), it.Value()
			if len(key) != len(txPrefix)+len(TransactionIndexSample{}.Hash) {
				it.Release()
				return nil, fmt.Errorf("sample transaction indexes: malformed tx key length %d", len(key))
			}
			hash := key[len(txPrefix):]
			prefix16 := uint64(binary.BigEndian.Uint16(hash[:2]))
			if prefix16 >= end16 {
				break
			}
			if len(value) != 8 {
				it.Release()
				return nil, fmt.Errorf("sample transaction indexes: malformed tx value length %d for %x", len(value), hash)
			}
			var sample TransactionIndexSample
			copy(sample.Hash[:], hash)
			sample.Location = binary.BigEndian.Uint64(value)
			samples = append(samples, sample)
			taken++
			if opts.ProgressInterval > 0 && opts.Progress != nil && time.Since(lastProgress) >= opts.ProgressInterval {
				opts.Progress(TransactionIndexSampleProgress{
					Rows:    uint64(len(samples)),
					Windows: window + 1,
					Elapsed: time.Since(started),
				})
				lastProgress = time.Now()
			}
		}
		err := it.Error()
		it.Release()
		if err != nil {
			return nil, fmt.Errorf("sample transaction indexes window %d: %w", window, err)
		}
	}
	return samples, nil
}

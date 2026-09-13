package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
)

// StateHistoryChunkGCScan is a bounded page of active bucket metadata. Next is
// an opaque exclusive cursor, not a completed reclamation watermark. Callers
// must restart at nil after Complete so busy or nonempty buckets are revisited.
type StateHistoryChunkGCScan struct {
	Buckets  []uint64
	Scanned  uint64
	Next     []byte
	Complete bool
}

// StateHistoryChunkBucketBounds returns inclusive block bounds for a fixed v3
// bucket. Invalid metadata must never wrap around into another bucket.
func StateHistoryChunkBucketBounds(bucket uint64) (uint64, uint64, error) {
	if bucket > ^uint64(0)/StateHistoryChunkBucketBlocks {
		return 0, 0, errors.New("state history chunks: bucket overflows block range")
	}
	first := bucket * StateHistoryChunkBucketBlocks
	return first, first + StateHistoryChunkBucketBlocks - 1, nil
}

// ScanStateHistoryChunkGCBuckets reads only bucket metadata, never chunks or
// history values. Limits are mandatory and bounded independently of DB size.
// Retired metadata consumes the scan budget too. A new/retained tail completes
// this sweep; it will be considered again after the caller restarts at nil.
func ScanStateHistoryChunkGCBuckets(ctx context.Context, db ethdb.Iteratee, after []byte, through uint64, maxRows, maxBuckets uint64) (StateHistoryChunkGCScan, error) {
	var result StateHistoryChunkGCScan
	if ctx == nil || db == nil || maxRows == 0 || maxRows > 256 || maxBuckets == 0 || maxBuckets > 4 {
		return result, errors.New("state history chunks: invalid GC scan arguments")
	}
	if len(after) != 0 && (len(after) != 9 || after[8] != 0) {
		return result, errors.New("state history chunks: invalid GC cursor")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	prefix := stateHistoryChunkBucketMetaPrefix()
	it := db.NewIterator(prefix, after)
	defer it.Release()
	for result.Scanned < maxRows && uint64(len(result.Buckets)) < maxBuckets {
		if err := ctx.Err(); err != nil {
			return StateHistoryChunkGCScan{}, err
		}
		if !it.Next() {
			if err := it.Error(); err != nil {
				return StateHistoryChunkGCScan{}, err
			}
			result.Complete, result.Next = true, nil
			return result, nil
		}
		key, value := it.Key(), it.Value()
		if len(key) != len(prefix)+8 || !bytes.HasPrefix(key, prefix) || len(value) != 2 || value[0] != 1 || value[1] > 1 {
			return StateHistoryChunkGCScan{}, errors.New("state history chunks: malformed bucket metadata")
		}
		bucket := binary.BigEndian.Uint64(key[len(prefix):])
		_, last, err := StateHistoryChunkBucketBounds(bucket)
		if err != nil {
			return StateHistoryChunkGCScan{}, err
		}
		if last > through {
			result.Complete, result.Next = true, nil
			return result, nil
		}
		result.Scanned++
		result.Next = append(append([]byte(nil), key[len(prefix):]...), 0)
		if value[1] == 0 {
			result.Buckets = append(result.Buckets, bucket)
		}
	}
	if err := it.Error(); err != nil {
		return StateHistoryChunkGCScan{}, err
	}
	return result, nil
}

// StateHistoryChunkRetirement counts metadata outcomes, not reclaimed bytes.
type StateHistoryChunkRetirement struct {
	Retired        bool
	AlreadyRetired bool
	NonEmpty       bool
}

// RetireStateHistoryChunkBucket requires the caller to hold the canonical
// chain/index writer guard and settled-buffer fence through this call, and to
// have verified cold coverage for the entire bucket. All earlier pack deletes
// must already be committed. db must be the fresh committed store, not an old
// snapshot or an auto-flushing prune wrapper. No fallback weakens these rules.
//
// A single fresh iterator rejects ANY physical history key in the bucket,
// including positive repair rows and malformed extensions. Chunk DeleteRange
// and the permanent retired marker then commit atomically in one DB batch.
// Old MVCC readers retain their chunk versions; future writers must respect the
// retired marker and emit self-contained packs. The marker is never deleted.
func RetireStateHistoryChunkBucket(ctx context.Context, db StateKVLatestStore, bucket uint64) (StateHistoryChunkRetirement, error) {
	var result StateHistoryChunkRetirement
	if ctx == nil || db == nil {
		return result, errors.New("state history chunks: missing GC context or database")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if view, ok := db.(pointread.PinnedKeyValueView); ok && view.IsPinnedKeyValueView() {
		return result, errors.New("state history chunks: retirement requires a fresh committed store")
	}
	first, last, err := StateHistoryChunkBucketBounds(bucket)
	if err != nil {
		return result, err
	}
	batcher, ok := db.(ethdb.Batcher)
	if !ok {
		return result, errors.New("state history chunks: retirement requires an atomic DB batch")
	}
	meta, present, err := readPresentValue(db, stateHistoryChunkBucketKey(bucket), "history chunk bucket")
	if err != nil {
		return result, err
	}
	if !present || len(meta) != 2 || meta[0] != 1 || meta[1] > 1 {
		return result, errors.New("state history chunks: missing or invalid retirement metadata")
	}
	if meta[1] == 1 {
		result.AlreadyRetired = true
		return result, nil
	}
	lower := stateChangeSetBlockPrefix(first)
	upper := prefixUpperBound(stateChangeSetPrefix)
	if last != ^uint64(0) {
		upper = stateChangeSetBlockPrefix(last + 1)
	}
	it := db.NewIterator(stateChangeSetPrefix, lower[len(stateChangeSetPrefix):])
	if it.Next() && bytes.Compare(it.Key(), upper) < 0 {
		result.NonEmpty = true
	}
	err = it.Error()
	it.Release()
	if err != nil {
		return StateHistoryChunkRetirement{}, err
	}
	if result.NonEmpty {
		return result, nil
	}
	if err := ctx.Err(); err != nil {
		return StateHistoryChunkRetirement{}, err
	}
	batch := batcher.NewBatch()
	defer func() {
		batch.Reset()
		if closer, ok := batch.(interface{ Close() }); ok {
			closer.Close()
		}
	}()
	prefix := stateHistoryChunkBucketPrefix(bucket)
	if err := batch.DeleteRange(prefix, prefixUpperBound(prefix)); err != nil {
		return result, fmt.Errorf("state history chunks: queue bucket delete: %w", err)
	}
	if err := batch.Put(stateHistoryChunkBucketKey(bucket), []byte{1, 1}); err != nil {
		return result, fmt.Errorf("state history chunks: queue retired marker: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := batch.Write(); err != nil {
		return result, fmt.Errorf("state history chunks: commit retirement: %w", err)
	}
	result.Retired = true
	return result, nil
}

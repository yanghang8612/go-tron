package rawdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
)

// StagingIndexReadView must include the durable base and every live overlay,
// including inflight canonical layers. The core caller owns its capture and
// writer guards through the directory delete batch. No iterator fallback is
// permitted: one exact hash seek must not materialize the durable posting set.
type StagingIndexReadView interface {
	pointread.PrefixSeeker
	pointread.PinnedKeyValueView
}

// PruneStagingStateChangePostingChunk is a schema primitive, not a deletion
// permission. The caller must authenticate a contiguous current-epoch COLD
// prefix and retain index/chain guards through its batch. Bucket zero and
// crossing frames are retained. It never consults hot changeset absence.
// Row/time/byte checks are cooperative between complete rows. Scan/delete
// budgets can exceed their limit by one validated posting frame and key.
func PruneStagingStateChangePostingChunk(ctx context.Context, db ethdb.KeyValueStore, through uint64, after []byte, limits StateChangePostingPruneLimits) (StateChangePostingPruneChunkResult, error) {
	if through < StateHistoryChunkBucketBlocks || through%StateHistoryChunkBucketBlocks != StateHistoryChunkBucketBlocks-1 {
		return StateChangePostingPruneChunkResult{NextCursor: bytes.Clone(after)}, errors.New("rawdb: staging index boundary is not a complete non-genesis bucket")
	}
	return stagingIndexGCChunk(ctx, db, nil, false, through, after, limits)
}

// PruneStagingStateChangeDirectoryChunk removes only raw directory rows with
// no posting in the caller's coherent full read view. It must run under the
// same authenticated COLD permission and writer guards as the posting phase.
// A candidate key may be at most 16 MiB. Scan bytes include a returned posting
// key/value, so a budget can be exceeded by one candidate plus one bounded
// posting result; delete bytes can exceed by one candidate. Cursor/result
// commit and error behavior otherwise matches StateChangePostingPruneChunkResult.
func PruneStagingStateChangeDirectoryChunk(ctx context.Context, db ethdb.KeyValueStore, view StagingIndexReadView, after []byte, limits StateChangePostingPruneLimits) (StateChangePostingPruneChunkResult, error) {
	if view == nil || !view.IsPinnedKeyValueView() {
		return StateChangePostingPruneChunkResult{NextCursor: bytes.Clone(after)}, errors.New("rawdb: staging directory GC requires a pinned complete prefix seeker")
	}
	return stagingIndexGCChunk(ctx, db, view, true, 0, after, limits)
}

func stagingIndexGCChunk(ctx context.Context, db ethdb.KeyValueStore, view StagingIndexReadView, directory bool, through uint64, after []byte, limits StateChangePostingPruneLimits) (result StateChangePostingPruneChunkResult, err error) {
	result.NextCursor = bytes.Clone(after)
	if ctx == nil || db == nil || limits.MaxScannedRows == 0 || limits.MaxScannedBytes == 0 || limits.MaxDeleteBytes == 0 || limits.MaxDuration <= 0 {
		return result, errors.New("rawdb: invalid staging index GC context/store/limits")
	}
	prefix := stateChangePostingPrefix
	if directory {
		prefix = stateChangeKeyDirectoryPrefix
	}
	validKey := func(key []byte) bool {
		if !bytes.HasPrefix(key, prefix) || len(key) <= len(prefix) || len(key) > 16<<20 {
			return false
		}
		return directory || len(key) == len(prefix)+sha256.Size+8
	}
	if len(after) != 0 && !validKey(after) {
		return result, errors.New("rawdb: invalid staging index GC cursor")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	started := time.Now()
	batch := db.NewBatch()
	defer batch.Close()
	next := bytes.Clone(after)
	var deleted, deletedBytes uint64
	var complete, bounded bool
	err = func() error {
		var start []byte
		if len(after) != 0 {
			start = after[len(prefix):]
		}
		it := db.NewIterator(prefix, start)
		defer it.Release()
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			if result.RowsScanned > 0 && (result.RowsScanned >= limits.MaxScannedRows || result.BytesScanned >= limits.MaxScannedBytes || deletedBytes >= limits.MaxDeleteBytes || time.Since(started) >= limits.MaxDuration) {
				bounded = true
				break
			}
			if !it.Next() {
				complete = true
				break
			}
			key, value := it.Key(), it.Value()
			if bytes.Equal(key, after) {
				continue
			}
			result.RowsScanned++
			cost := uint64(len(key)) + uint64(len(value))
			result.BytesScanned += cost
			if !validKey(key) || bytes.Compare(key, after) <= 0 {
				return errors.New("rawdb: malformed staging index GC key")
			}
			remove := false
			if directory {
				if len(value) != 0 {
					return errors.New("rawdb: malformed staging directory value")
				}
				hash := stateChangePostingHash(key[len(prefix):])
				postingPrefix := stateChangePostingHashPrefix(hash)
				postingKey, postingValue, present, seekErr := view.SeekPrefix(postingPrefix, nil)
				if seekErr != nil {
					return seekErr
				}
				if present {
					result.BytesScanned += uint64(len(postingKey)) + uint64(len(postingValue))
					if len(postingKey) != len(postingPrefix)+8 || !bytes.HasPrefix(postingKey, postingPrefix) || len(postingValue) > maxStateChangePostingEncodedBytes {
						return errors.New("rawdb: malformed protected staging posting")
					}
					if _, _, err := stateChangePostingLastBlock(binary.BigEndian.Uint64(postingKey[len(postingPrefix):]), postingValue); err != nil {
						return err
					}
				}
				remove = !present
			} else {
				if len(value) > maxStateChangePostingEncodedBytes {
					return errors.New("rawdb: oversized staging posting")
				}
				first := binary.BigEndian.Uint64(key[len(key)-8:])
				last, _, err := stateChangePostingLastBlock(first, value)
				if err != nil {
					return err
				}
				remove = first >= StateHistoryChunkBucketBlocks && last <= through
			}
			if remove {
				if err := batch.Delete(key); err != nil {
					return err
				}
				deleted++
				deletedBytes += cost
			}
			next = append(next[:0], key...)
		}
		return it.Error()
	}()
	result.ScanDuration = time.Since(started)
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	writeStarted := time.Now()
	if deleted != 0 {
		if err := batch.Write(); err != nil {
			result.WriteDuration = time.Since(writeStarted)
			return result, err
		}
	}
	result.WriteDuration = time.Since(writeStarted)
	result.NextCursor, result.Complete, result.StoppedByBudget = next, complete, bounded
	result.RowsDeleted, result.BytesDeleted = deleted, deletedBytes
	return result, nil
}

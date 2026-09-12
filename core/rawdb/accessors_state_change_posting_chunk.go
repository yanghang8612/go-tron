package rawdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
)

// StateChangePostingPruneLimits bounds one posting-only sweep chunk. All
// limits must be positive. Rows is a hard limit. Bytes count full encoded
// key+value sizes, not tombstone bytes or reclaimed disk space. Byte and time
// limits are checked between complete rows, so a chunk can exceed each byte
// budget by at most one validated frame and its fixed-width key. A first row
// is allowed even if MaxDuration has elapsed; context cancellation is not.
// Duration is cooperative: it cannot interrupt iterator IO or batch.Write.
type StateChangePostingPruneLimits struct {
	MaxScannedRows  uint64
	MaxScannedBytes uint64
	MaxDeleteBytes  uint64
	MaxDuration     time.Duration
}

// StateChangePostingPruneChunkResult describes one committed chunk. NextCursor
// is an owned copy of the last fully processed posting key, exclusive on the
// next call. Complete means this chunk reached its iterator's end. Reaching a
// budget does not probe one extra row merely to establish end-of-iteration.
// RowsDeleted/BytesDeleted describe successfully submitted deletions, not
// physical reclamation. On error they are zero, NextCursor stays at the input
// cursor, and neither Complete nor StoppedByBudget is asserted; scan counters
// may still describe work attempted before the failure.
type StateChangePostingPruneChunkResult struct {
	NextCursor      []byte
	Complete        bool
	StoppedByBudget bool
	RowsScanned     uint64
	BytesScanned    uint64
	RowsDeleted     uint64
	BytesDeleted    uint64
	// Durations include attempted work on errors. Scan includes iterator
	// creation/release and batch construction; Write measures batch.Write only.
	// Neither is a latency bound or a count of device I/O.
	ScanDuration  time.Duration
	WriteDuration time.Duration
}

// Even noncanonical-but-decodable uvarints use at most ten bytes each: one
// count and at most 255 deltas. Reject a huge malformed value before decoding.
const maxStateChangePostingEncodedBytes = 1 + binary.MaxVarintLen64*int(StateChangePostingFrameRows)

// PruneStaleStateChangePostingChunkContext removes only complete immutable
// posting frames whose final block is <= prunedThrough. The caller must supply
// one fixed, durable inclusive authoritative-hot-prune watermark throughout a
// cursor traversal, and serialize this sweep with incompatible restoration,
// rebuild and reset writes. This primitive provides no writer mutex.
//
// resumeKeyExclusive is nil to begin, otherwise an opaque full posting key
// returned by a successful prior call. Each call creates exactly one bounded
// posting iterator and releases it before writing one batch; it performs no
// point reads, directory sweep, live-hash set, compaction or checkpoint write.
// Progress is in memory only and returned after batch.Write succeeds. A restart
// may repeat completed deletions safely. An ambiguous batch error also returns
// the old cursor, so retrying cannot skip unconfirmed work.
func PruneStaleStateChangePostingChunkContext(ctx context.Context, db ethdb.KeyValueStore, prunedThrough uint64, resumeKeyExclusive []byte, limits StateChangePostingPruneLimits) (result StateChangePostingPruneChunkResult, err error) {
	result.NextCursor = bytes.Clone(resumeKeyExclusive)
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if db == nil {
		return result, errors.New("rawdb: nil database for posting prune chunk")
	}
	if limits.MaxScannedRows == 0 || limits.MaxScannedBytes == 0 || limits.MaxDeleteBytes == 0 || limits.MaxDuration <= 0 {
		return result, errors.New("rawdb: posting prune chunk limits must all be positive")
	}
	keyBytes := len(stateChangePostingPrefix) + sha256.Size + 8
	if len(resumeKeyExclusive) != 0 && (len(resumeKeyExclusive) != keyBytes || !bytes.HasPrefix(resumeKeyExclusive, stateChangePostingPrefix)) {
		return result, errors.New("rawdb: invalid posting prune exclusive cursor")
	}
	started := time.Now()
	scanning := true
	defer func() {
		if scanning {
			result.ScanDuration = time.Since(started)
		}
	}()
	nextCursor := bytes.Clone(resumeKeyExclusive)
	var deletedRows, deletedBytes uint64
	var complete, stoppedByBudget bool
	batch := db.NewBatch()
	defer batch.Close()
	err = func() error {
		var start []byte
		if len(resumeKeyExclusive) > 0 {
			start = resumeKeyExclusive[len(stateChangePostingPrefix):]
		}
		it := db.NewIterator(stateChangePostingPrefix, start)
		defer it.Release()
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			if result.RowsScanned > 0 && (result.RowsScanned >= limits.MaxScannedRows || result.BytesScanned >= limits.MaxScannedBytes || deletedBytes >= limits.MaxDeleteBytes || time.Since(started) >= limits.MaxDuration) {
				stoppedByBudget = true
				break
			}
			if !it.Next() {
				complete = true
				break
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			key := it.Key()
			// NewIterator seeks inclusively. A surviving mixed/live cursor row
			// must not be counted again, while a deleted cursor resumes at the
			// next greater row without assuming the old row still exists.
			if bytes.Equal(key, resumeKeyExclusive) {
				continue
			}
			value := it.Value()
			rowBytes := uint64(len(key)) + uint64(len(value))
			result.RowsScanned++
			result.BytesScanned += rowBytes
			if len(key) != keyBytes || !bytes.HasPrefix(key, stateChangePostingPrefix) || bytes.Compare(key, resumeKeyExclusive) <= 0 {
				return fmt.Errorf("rawdb: malformed or unordered posting prune key length %d", len(key))
			}
			if len(value) > maxStateChangePostingEncodedBytes {
				return fmt.Errorf("rawdb: oversized state change posting value %d", len(value))
			}
			first := binary.BigEndian.Uint64(key[len(key)-8:])
			blocks, err := decodeStateChangePosting(first, value)
			if err != nil {
				return err
			}
			if blocks[len(blocks)-1] <= prunedThrough {
				if err := batch.Delete(key); err != nil {
					return err
				}
				deletedRows++
				deletedBytes += rowBytes
			}
			nextCursor = append(nextCursor[:0], key...)
		}
		return it.Error()
	}()
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	result.ScanDuration = time.Since(started)
	scanning = false
	writeStarted := time.Now()
	writeErr := batch.Write()
	result.WriteDuration = time.Since(writeStarted)
	if err := writeErr; err != nil {
		return result, err
	}
	result.NextCursor = nextCursor
	result.Complete = complete
	result.StoppedByBudget = stoppedByBudget
	result.RowsDeleted = deletedRows
	result.BytesDeleted = deletedBytes
	return result, nil
}

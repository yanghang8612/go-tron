package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

const (
	cleanupBloomBytes      = 512 << 20
	cleanupBatchBytes      = 16 << 20
	cleanupDeleteByteLimit = 256 << 30
	cleanupMinFreeBytes    = 128 << 30
	cleanupPageRows        = 50000
)

// Hash inputs are the exact 32-byte posting-key hash. Double hashing uses an
// odd stride over a power-of-two bit domain. Bloom positives only retain.
type cleanupBloom struct {
	bits []byte
	k    uint64
}

func newCleanupBloom(bytes uint64, probes uint64) (*cleanupBloom, error) {
	if bytes == 0 || bytes > cleanupBloomBytes || bytes&(bytes-1) != 0 || probes == 0 || probes > 16 {
		return nil, errors.New("cleanup: invalid bounded Bloom size or probe count")
	}
	return &cleanupBloom{bits: make([]byte, bytes), k: probes}, nil
}

func (b *cleanupBloom) indexes(hash [32]byte, visit func(uint64)) {
	mask := uint64(len(b.bits))*8 - 1
	first := binary.LittleEndian.Uint64(hash[:8])
	step := binary.LittleEndian.Uint64(hash[8:16]) | 1
	for i := uint64(0); i < b.k; i++ {
		visit((first + i*step) & mask)
	}
}

func (b *cleanupBloom) Add(hash [32]byte) {
	b.indexes(hash, func(bit uint64) { b.bits[bit>>3] |= 1 << (bit & 7) })
}

func (b *cleanupBloom) MayContain(hash [32]byte) bool {
	present := true
	b.indexes(hash, func(bit uint64) { present = present && b.bits[bit>>3]&(1<<(bit&7)) != 0 })
	return present
}

type cleanupDeleteStats struct {
	PostingScanned          uint64 `json:"posting_scanned"`
	PostingRetained         uint64 `json:"posting_retained"`
	PostingDeleted          uint64 `json:"posting_deleted"`
	DirectoryScanned        uint64 `json:"directory_scanned"`
	DirectoryRetained       uint64 `json:"directory_retained"`
	DirectoryDeleted        uint64 `json:"directory_deleted"`
	CommittedLogicalBytes   uint64 `json:"committed_logical_delete_bytes"`
	DurabilityUncertainTail bool   `json:"durability_uncertain_tail"`
	Phase                   string `json:"phase"`
}

type cleanupBatchWriter struct {
	db               ethdb.KeyValueStore
	syncer           interface{ SyncKeyValue() error }
	batch            ethdb.Batch
	hotPath          string
	stats            *cleanupDeleteStats
	pending          uint64
	rows             uint64
	pendingPosting   uint64
	pendingDirectory uint64
	maxBytes         uint64
	minFree          uint64
	hold             *cleanupHoldState
	ctx              context.Context
	lock             *os.File
	lockPath         string
}

func newCleanupBatchWriter(db ethdb.KeyValueStore, path string, stats *cleanupDeleteStats) (*cleanupBatchWriter, error) {
	syncer, ok := db.(interface{ SyncKeyValue() error })
	if !ok || db == nil || stats == nil {
		return nil, errors.New("cleanup: writable store lacks explicit durable sync")
	}
	return &cleanupBatchWriter{db: db, syncer: syncer, batch: db.NewBatch(), hotPath: path,
		stats: stats, maxBytes: cleanupDeleteByteLimit, minFree: cleanupMinFreeBytes}, nil
}

func cleanupFreeBytes(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	if uint64(stat.Bsize) != 0 && stat.Bavail > math.MaxUint64/uint64(stat.Bsize) {
		return math.MaxUint64, nil
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}

func (w *cleanupBatchWriter) checkAdmission() error {
	if w.hold != nil {
		if err := w.hold.Recheck(); err != nil {
			return err
		}
	}
	if w.lock != nil {
		if err := checkCleanupOperationLock(w.lock, w.lockPath); err != nil {
			return err
		}
	}
	if w.stats.CommittedLogicalBytes > w.maxBytes || w.pending > w.maxBytes-w.stats.CommittedLogicalBytes {
		return errors.New("cleanup: logical delete-byte budget exceeded")
	}
	free, err := cleanupFreeBytes(w.hotPath)
	if err != nil {
		return err
	}
	if free < w.minFree {
		return fmt.Errorf("cleanup: free space %d below required %d", free, w.minFree)
	}
	return nil
}

func (w *cleanupBatchWriter) Delete(key []byte, logicalBytes uint64, directory bool) error {
	if logicalBytes < uint64(len(key)) || logicalBytes > w.maxBytes ||
		uint64(len(key))+16 > cleanupBatchBytes ||
		w.stats.CommittedLogicalBytes > w.maxBytes-logicalBytes || w.pending > w.maxBytes-w.stats.CommittedLogicalBytes-logicalBytes {
		return errors.New("cleanup: logical delete-byte budget exceeded")
	}
	if w.rows > 0 && uint64(w.batch.ValueSize())+uint64(len(key))+16 > cleanupBatchBytes {
		if err := w.Flush(); err != nil {
			return err
		}
	}
	if err := w.batch.Delete(key); err != nil {
		return err
	}
	w.pending += logicalBytes
	w.rows++
	if directory {
		w.pendingDirectory++
	} else {
		w.pendingPosting++
	}
	return nil
}

func (w *cleanupBatchWriter) Flush() error {
	if w.rows == 0 {
		return nil
	}
	if w.ctx != nil {
		if err := w.ctx.Err(); err != nil {
			return err
		}
	}
	if err := w.checkAdmission(); err != nil {
		return err
	}
	if err := w.batch.Write(); err != nil {
		w.stats.DurabilityUncertainTail = true
		return err
	}
	if err := w.checkAdmission(); err != nil {
		w.stats.DurabilityUncertainTail = true
		return err
	}
	if err := w.syncer.SyncKeyValue(); err != nil {
		w.stats.DurabilityUncertainTail = true
		return err
	}
	w.stats.CommittedLogicalBytes += w.pending
	w.stats.PostingDeleted += w.pendingPosting
	w.stats.DirectoryDeleted += w.pendingDirectory
	w.pending, w.rows, w.pendingPosting, w.pendingDirectory = 0, 0, 0, 0
	w.batch.Reset()
	return nil
}

func runCleanupDeletes(ctx context.Context, db ethdb.KeyValueStore, path string, through uint64, hold *cleanupHoldState, lock *os.File, lockPath string, stats *cleanupDeleteStats) error {
	return runCleanupDeletesWithOptions(ctx, db, path, through, hold, lock, lockPath, stats, cleanupBloomBytes, cleanupPageRows, cleanupMinFreeBytes)
}

// The production entry point fixes these limits. The private options let
// small real-Pebble fixtures exercise the same scanner, Bloom and batch order.
func runCleanupDeletesWithOptions(ctx context.Context, db ethdb.KeyValueStore, path string, through uint64, hold *cleanupHoldState, lock *os.File, lockPath string, stats *cleanupDeleteStats, bloomBytes, pageRows, minFree uint64) error {
	if ctx == nil || db == nil || stats == nil || through < cleanupFirstBlock {
		return errors.New("cleanup: missing deletion state")
	}
	filter, err := newCleanupBloom(bloomBytes, 7)
	if err != nil {
		return err
	}
	writer, err := newCleanupBatchWriter(db, path, stats)
	if err != nil {
		return err
	}
	defer writer.batch.Close()
	writer.minFree = minFree
	writer.hold = hold
	writer.ctx = ctx
	writer.lock, writer.lockPath = lock, lockPath
	stats.Phase = "posting"
	lastProgress := time.Now()
	var after []byte
	for {
		next, complete, err := rawdb.ScanStateChangePostingFramePage(ctx, db, after, pageRows, func(frame rawdb.StateChangePostingFrameCursor) error {
			stats.PostingScanned++
			if frame.FirstBlock >= cleanupFirstBlock && frame.LastBlock <= through {
				if err := writer.Delete(frame.Key, uint64(len(frame.Key))+frame.ValueBytes, false); err != nil {
					return err
				}
			} else {
				filter.Add(frame.Hash)
				stats.PostingRetained++
			}
			cleanupProgress("posting", stats.PostingScanned, 0, &lastProgress)
			return nil
		})
		if err != nil {
			return err
		}
		// Usually the iterator is closed before this flush. A very large page
		// can flush a 16 MiB batch inside the callback, but pins at most one
		// bounded page, never the entire posting keyspace.
		if err := writer.Flush(); err != nil {
			return err
		}
		if complete {
			break
		}
		after = next
	}
	// The directory pass may start only after the entire posting iterator has
	// returned successfully and the final delete batch was durably synced.
	stats.Phase = "directory"
	lastProgress = time.Now()
	after = nil
	for {
		next, complete, err := rawdb.ScanStateChangeKeyDirectoryPage(ctx, db, after, pageRows, func(row rawdb.StateChangeDirectoryCursor) error {
			stats.DirectoryScanned++
			if filter.MayContain(row.Hash) {
				stats.DirectoryRetained++
			} else {
				if err := writer.Delete(row.Key, uint64(len(row.Key)), true); err != nil {
					return err
				}
			}
			cleanupProgress("directory", stats.DirectoryScanned, 0, &lastProgress)
			return nil
		})
		if err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}
		if complete {
			break
		}
		after = next
	}
	stats.Phase = "complete"
	return nil
}

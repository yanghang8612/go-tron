package rawdb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

// StateChangePostingFrameRows is fixed by the complete mainnet benchmark:
// 2,552,895,165 source rows and 266,482,665 unique keys. Posting-256 reduced
// the estimated data blocks by 50.7175%; moving to 1024 saved only another
// 48,888,679 bytes (~46.6 MiB) while quadrupling the decode bound.
const StateChangePostingFrameRows uint16 = 256

const stateChangePostingValueVersion byte = 1

func stateChangePostingHash(latestKey []byte) [sha256.Size]byte { return sha256.Sum256(latestKey) }

func stateChangePostingKey(hash [sha256.Size]byte, firstBlock uint64) []byte {
	key := make([]byte, 0, len(stateChangePostingPrefix)+sha256.Size+8)
	return appendStateChangePostingKey(key, hash, firstBlock)
}

func appendStateChangePostingKey(dst []byte, hash [sha256.Size]byte, firstBlock uint64) []byte {
	dst = append(dst, stateChangePostingPrefix...)
	dst = append(dst, hash[:]...)
	var block [8]byte
	binary.BigEndian.PutUint64(block[:], firstBlock)
	return append(dst, block[:]...)
}

func stateChangePostingHashPrefix(hash [sha256.Size]byte) []byte {
	key := make([]byte, len(stateChangePostingPrefix)+sha256.Size)
	copy(key, stateChangePostingPrefix)
	copy(key[len(stateChangePostingPrefix):], hash[:])
	return key
}

func stateChangeKeyDirectoryKey(latestKey []byte) []byte {
	key := make([]byte, 0, len(stateChangeKeyDirectoryPrefix)+len(latestKey))
	key = append(key, stateChangeKeyDirectoryPrefix...)
	return append(key, latestKey...)
}

func encodeStateChangePosting(blocks []uint64) ([]byte, error) {
	if len(blocks) == 0 || len(blocks) > int(StateChangePostingFrameRows) {
		return nil, fmt.Errorf("rawdb: invalid state change posting count %d", len(blocks))
	}
	value := make([]byte, 1+binary.MaxVarintLen64*(len(blocks)+1))
	value[0] = stateChangePostingValueVersion
	off := 1
	off += binary.PutUvarint(value[off:], uint64(len(blocks)))
	previous := blocks[0]
	for i := 1; i < len(blocks); i++ {
		if blocks[i] <= previous {
			return nil, fmt.Errorf("rawdb: non-increasing state change posting block %d after %d", blocks[i], previous)
		}
		off += binary.PutUvarint(value[off:], blocks[i]-previous)
		previous = blocks[i]
	}
	return value[:off], nil
}

func decodeStateChangePosting(firstBlock uint64, value []byte) ([]uint64, error) {
	if len(value) < 2 || value[0] != stateChangePostingValueVersion {
		return nil, fmt.Errorf("rawdb: malformed state change posting version/length %d", len(value))
	}
	count, n := binary.Uvarint(value[1:])
	if n <= 0 || count == 0 || count > uint64(StateChangePostingFrameRows) {
		return nil, errors.New("rawdb: malformed state change posting count")
	}
	blocks := make([]uint64, count)
	blocks[0] = firstBlock
	off := 1 + n
	for i := 1; i < len(blocks); i++ {
		delta, consumed := binary.Uvarint(value[off:])
		if consumed <= 0 || delta == 0 || blocks[i-1] > math.MaxUint64-delta {
			return nil, fmt.Errorf("rawdb: malformed state change posting delta at %d", i)
		}
		blocks[i] = blocks[i-1] + delta
		off += consumed
	}
	if off != len(value) {
		return nil, fmt.Errorf("rawdb: trailing state change posting bytes %d", len(value)-off)
	}
	return blocks, nil
}

// writeStateChangePostingIndex publishes the live-block form. Bulk sync uses
// stateChangePostingCollector below to combine up to 256 blocks per frame.
func writeStateChangePostingIndex(db ethdb.KeyValueWriter, latestKey []byte, blockNum uint64) error {
	if err := db.Put(stateChangeKeyDirectoryKey(latestKey), nil); err != nil {
		return err
	}
	value, err := encodeStateChangePosting([]uint64{blockNum})
	if err != nil {
		return err
	}
	return db.Put(stateChangePostingKey(stateChangePostingHash(latestKey), blockNum), value)
}

// deleteLiveStateChangePostingByHash is the pruning fast path. The caller can
// reuse keyScratch across a block, and singleton live postings are validated
// without materializing the general []uint64 representation. Packed frames
// are finalized history and remain immutable; pruning merely removes their
// authoritative changesets, after which readers reject stale candidates.
func deleteLiveStateChangePostingByHash(db StateKVLatestStore, hash [sha256.Size]byte, blockNum uint64, keyScratch []byte) ([]byte, error) {
	key := appendStateChangePostingKey(keyScratch[:0], hash, blockNum)
	value, exists, err := readPresentValue(db, key, "state change posting")
	if err != nil || !exists {
		if err != nil {
			return key, fmt.Errorf("rawdb: read state change posting at block %d: %w", blockNum, err)
		}
		return key, nil
	}
	singleton, err := isSingletonStateChangePosting(blockNum, value)
	if err != nil {
		return key, err
	}
	if singleton {
		return key, db.Delete(key)
	}
	return key, nil
}

func isSingletonStateChangePosting(firstBlock uint64, value []byte) (bool, error) {
	if len(value) < 2 || value[0] != stateChangePostingValueVersion {
		return false, fmt.Errorf("rawdb: malformed state change posting version/length %d", len(value))
	}
	count, n := binary.Uvarint(value[1:])
	if n <= 0 || count == 0 || count > uint64(StateChangePostingFrameRows) {
		return false, errors.New("rawdb: malformed state change posting count")
	}
	if count == 1 {
		if off := 1 + n; off != len(value) {
			return false, fmt.Errorf("rawdb: trailing state change posting bytes %d", len(value)-off)
		}
		return true, nil
	}
	// Packed immutable frames are uncommon on this live-delete path. Retain
	// full validation for them, including delta overflow and trailing bytes.
	if _, err := decodeStateChangePosting(firstBlock, value); err != nil {
		return false, err
	}
	return false, nil
}

// iterateStateChangePostingCandidates walks hash candidates in block order.
// Every candidate must subsequently be checked against the original latest key
// reconstructed from its authoritative changeset.
func iterateStateChangePostingCandidates(db ethdb.Iteratee, latestKey []byte, fromBlock, toBlock uint64, fn func(uint64) (bool, error)) error {
	hash := stateChangePostingHash(latestKey)
	prefix := stateChangePostingHashPrefix(hash)
	it := db.NewIterator(prefix, nil)
	defer it.Release()
	for it.Next() {
		key := it.Key()
		if len(key) != len(prefix)+8 || !bytes.HasPrefix(key, prefix) {
			continue
		}
		first := binary.BigEndian.Uint64(key[len(prefix):])
		blocks, err := decodeStateChangePosting(first, it.Value())
		if err != nil {
			return err
		}
		for _, blockNum := range blocks {
			if blockNum < fromBlock {
				continue
			}
			if blockNum > toBlock {
				break
			}
			cont, err := fn(blockNum)
			if err != nil || !cont {
				return err
			}
		}
	}
	return it.Error()
}

type StateChangePostingBuildResult struct {
	FromBlock     uint64
	ToBlock       uint64
	SourceRows    uint64
	DirectoryRows uint64
	PostingRows   uint64
	ETL           etl.Stats
}

type stateChangePostingCollector struct {
	postings  *etl.Collector
	directory *etl.Collector
	result    StateChangePostingBuildResult
}

func newStateChangePostingCollector(fromBlock, toBlock uint64, opts etl.Options) (*stateChangePostingCollector, error) {
	postings, err := etl.NewCollector(opts)
	if err != nil {
		return nil, err
	}
	directory, err := etl.NewCollector(opts)
	if err != nil {
		_ = postings.Close()
		return nil, err
	}
	return &stateChangePostingCollector{postings: postings, directory: directory, result: StateChangePostingBuildResult{FromBlock: fromBlock, ToBlock: toBlock}}, nil
}

func (c *stateChangePostingCollector) Close() {
	_ = c.postings.Close()
	_ = c.directory.Close()
}

func (c *stateChangePostingCollector) Collect(change *StateDomainChange) error {
	latestKey, err := stateDomainChangeLatestKey(change)
	if err != nil {
		return err
	}
	hash := stateChangePostingHash(latestKey)
	sortKey := make([]byte, sha256.Size+8)
	copy(sortKey, hash[:])
	binary.BigEndian.PutUint64(sortKey[sha256.Size:], change.BlockNum)
	if err := c.postings.PutOwned(sortKey, nil); err != nil {
		return err
	}
	if err := c.directory.Put(stateChangeKeyDirectoryKey(latestKey), nil); err != nil {
		return err
	}
	c.result.SourceRows++
	return nil
}

func (c *stateChangePostingCollector) Load(writer ethdb.KeyValueWriter, interrupted func() bool) (*StateChangePostingBuildResult, error) {
	directoryStats, err := c.directory.LoadInterruptible(writer, interrupted)
	if errors.Is(err, etl.ErrLoadInterrupted) {
		return nil, ErrStateHistoryIndexRebuildInterrupted
	}
	if err != nil {
		return nil, err
	}
	c.result.DirectoryRows = directoryStats.AppliedPuts
	postingWriter := newStateChangePostingETLWriter(writer, int(StateChangePostingFrameRows))
	defer postingWriter.Close()
	stats, err := c.postings.LoadInterruptible(postingWriter, interrupted)
	if errors.Is(err, etl.ErrLoadInterrupted) {
		return nil, ErrStateHistoryIndexRebuildInterrupted
	}
	if err == nil {
		err = postingWriter.Finish()
	}
	if err != nil {
		return nil, err
	}
	c.result.PostingRows = postingWriter.rows
	stats.BatchWrites += postingWriter.batchWrites
	c.result.ETL = stats
	result := c.result
	return &result, nil
}

type stateChangePostingETLWriter struct {
	target      ethdb.KeyValueWriter
	batch       ethdb.Batch
	frameRows   int
	hash        [sha256.Size]byte
	blocks      []uint64
	haveHash    bool
	rows        uint64
	batchWrites uint64
}

func newStateChangePostingETLWriter(target ethdb.KeyValueWriter, frameRows int) *stateChangePostingETLWriter {
	w := &stateChangePostingETLWriter{target: target, frameRows: frameRows}
	if batcher, ok := target.(ethdb.Batcher); ok {
		w.batch = batcher.NewBatch()
	}
	return w
}

func (w *stateChangePostingETLWriter) Put(key, _ []byte) error {
	if len(key) != sha256.Size+8 {
		return fmt.Errorf("rawdb: state change posting ETL key length %d", len(key))
	}
	var hash [sha256.Size]byte
	copy(hash[:], key[:sha256.Size])
	if w.haveHash && hash != w.hash {
		if err := w.flushAll(); err != nil {
			return err
		}
	}
	if !w.haveHash || hash != w.hash {
		w.hash, w.haveHash = hash, true
	}
	blockNum := binary.BigEndian.Uint64(key[sha256.Size:])
	if len(w.blocks) != 0 && w.blocks[len(w.blocks)-1] >= blockNum {
		return fmt.Errorf("rawdb: unordered state change posting ETL block %d", blockNum)
	}
	w.blocks = append(w.blocks, blockNum)
	if len(w.blocks) == w.frameRows {
		return w.flushFrame()
	}
	return nil
}

func (w *stateChangePostingETLWriter) Delete([]byte) error {
	return errors.New("rawdb: unexpected state change posting ETL delete")
}

func (w *stateChangePostingETLWriter) Finish() error {
	if err := w.flushAll(); err != nil {
		return err
	}
	return w.flushBatch()
}

func (w *stateChangePostingETLWriter) Close() {
	if w.batch != nil {
		w.batch.Close()
	}
}

func (w *stateChangePostingETLWriter) flushBatch() error {
	if w.batch == nil || w.batch.ValueSize() == 0 {
		return nil
	}
	if err := w.batch.Write(); err != nil {
		return err
	}
	w.batch.Reset()
	w.batchWrites++
	return nil
}

func (w *stateChangePostingETLWriter) flushAll() error {
	if err := w.flushFrame(); err != nil {
		return err
	}
	w.haveHash = false
	return nil
}

func (w *stateChangePostingETLWriter) flushFrame() error {
	if len(w.blocks) == 0 {
		return nil
	}
	value, err := encodeStateChangePosting(w.blocks)
	if err != nil {
		return err
	}
	target := w.target
	if w.batch != nil {
		target = w.batch
	}
	if err := target.Put(stateChangePostingKey(w.hash, w.blocks[0]), value); err != nil {
		return err
	}
	w.rows++
	w.blocks = w.blocks[:0]
	if w.batch != nil && w.batch.ValueSize() >= ethdb.IdealBatchSize {
		return w.flushBatch()
	}
	return nil
}

type StateChangePostingPruneResult struct {
	PostingRowsScanned   uint64
	PostingRowsDeleted   uint64
	DirectoryRowsScanned uint64
	DirectoryRowsDeleted uint64
}

// PruneStaleStateChangePostingIndex reclaims only wholly stale immutable
// frames. Mixed live/stale frames remain unchanged and are filtered exactly by
// the mandatory changeset collision check.
func PruneStaleStateChangePostingIndex(db ethdb.KeyValueStore) (StateChangePostingPruneResult, error) {
	var result StateChangePostingPruneResult
	postingIt := db.NewIterator(stateChangePostingPrefix, nil)
	batch := db.NewBatch()
	defer batch.Close()
	for postingIt.Next() {
		key := postingIt.Key()
		if len(key) != len(stateChangePostingPrefix)+sha256.Size+8 {
			postingIt.Release()
			return result, fmt.Errorf("rawdb: malformed state change posting key length %d", len(key))
		}
		result.PostingRowsScanned++
		first := binary.BigEndian.Uint64(key[len(key)-8:])
		blocks, err := decodeStateChangePosting(first, postingIt.Value())
		if err != nil {
			postingIt.Release()
			return result, err
		}
		wantHash := key[len(stateChangePostingPrefix) : len(stateChangePostingPrefix)+sha256.Size]
		live := false
		for _, blockNum := range blocks {
			err := IterateStateDomainChanges(db, blockNum, func(change *StateDomainChange) (bool, error) {
				latestKey, err := stateDomainChangeLatestKey(change)
				if err != nil {
					return false, err
				}
				hash := stateChangePostingHash(latestKey)
				if bytes.Equal(hash[:], wantHash) {
					live = true
					return false, nil
				}
				return true, nil
			})
			if err != nil {
				postingIt.Release()
				return result, err
			}
			if live {
				break
			}
		}
		if !live {
			if err := batch.Delete(key); err != nil {
				postingIt.Release()
				return result, err
			}
			result.PostingRowsDeleted++
		}
		if batch.ValueSize() >= ethdb.IdealBatchSize {
			if err := batch.Write(); err != nil {
				postingIt.Release()
				return result, err
			}
			batch.Reset()
		}
	}
	postingErr := postingIt.Error()
	postingIt.Release()
	if postingErr != nil {
		return result, postingErr
	}
	if err := batch.Write(); err != nil {
		return result, err
	}
	batch.Reset()

	directoryIt := db.NewIterator(stateChangeKeyDirectoryPrefix, nil)
	for directoryIt.Next() {
		key := directoryIt.Key()
		if !bytes.HasPrefix(key, stateChangeKeyDirectoryPrefix) || len(key) == len(stateChangeKeyDirectoryPrefix) {
			directoryIt.Release()
			return result, errors.New("rawdb: malformed state change key directory row")
		}
		result.DirectoryRowsScanned++
		hash := stateChangePostingHash(key[len(stateChangeKeyDirectoryPrefix):])
		it := db.NewIterator(stateChangePostingHashPrefix(hash), nil)
		hasPosting := it.Next()
		err := it.Error()
		it.Release()
		if err != nil {
			directoryIt.Release()
			return result, err
		}
		if !hasPosting {
			if err := batch.Delete(key); err != nil {
				directoryIt.Release()
				return result, err
			}
			result.DirectoryRowsDeleted++
		}
		if batch.ValueSize() >= ethdb.IdealBatchSize {
			if err := batch.Write(); err != nil {
				directoryIt.Release()
				return result, err
			}
			batch.Reset()
		}
	}
	directoryErr := directoryIt.Error()
	directoryIt.Release()
	if directoryErr != nil {
		return result, directoryErr
	}
	return result, batch.Write()
}

package rawdb

import (
	"bytes"
	"context"
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

const stateChangePostingDedupeSlots = 64
const stateChangePostingDedupeWays = 4

// stateChangePostingDeduper is a bounded four-way exact cache for repeated
// latest keys inside one block. Set overflow replaces an entry and may only
// reduce the hit rate; equality is still checked, so required work is never
// suppressed and lookup remains bounded on high-cardinality blocks.
type stateChangePostingDeduper struct {
	hashes [stateChangePostingDedupeSlots][sha256.Size]byte
	stamps [stateChangePostingDedupeSlots]uint32
	epoch  uint32
}

func (d *stateChangePostingDeduper) Reset() {
	d.epoch++
	if d.epoch == 0 {
		d.stamps = [stateChangePostingDedupeSlots]uint32{}
		d.epoch = 1
	}
}

func (d *stateChangePostingDeduper) Seen(hash [sha256.Size]byte) bool {
	if d.epoch == 0 {
		d.Reset()
	}
	set := int(binary.BigEndian.Uint16(hash[:2])) & (stateChangePostingDedupeSlots/stateChangePostingDedupeWays - 1)
	base := set * stateChangePostingDedupeWays
	empty := -1
	for way := 0; way < stateChangePostingDedupeWays; way++ {
		slot := base + way
		if d.stamps[slot] != d.epoch {
			if empty < 0 {
				empty = slot
			}
			continue
		}
		if d.hashes[slot] == hash {
			return true
		}
	}
	slot := empty
	if slot < 0 {
		slot = base + int(hash[2])&(stateChangePostingDedupeWays-1)
	}
	d.hashes[slot] = hash
	d.stamps[slot] = d.epoch
	return false
}

func stateChangePostingKey(hash [sha256.Size]byte, firstBlock uint64) []byte {
	key := make([]byte, 0, len(stateChangePostingPrefix)+sha256.Size+8)
	return appendStateChangePostingKey(key, hash, firstBlock)
}

func appendStateChangePostingKey(dst []byte, hash [sha256.Size]byte, firstBlock uint64) []byte {
	dst = append(dst, stateChangePostingPrefix...)
	dst = append(dst, hash[:]...)
	return binary.BigEndian.AppendUint64(dst, firstBlock)
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

// deleteLiveStateChangePosting deletes only a single-block live frame. Packed
// frames are finalized history and remain immutable; pruning merely removes
// their authoritative changesets, after which readers reject stale candidates.
func deleteLiveStateChangePosting(db StateKVLatestStore, latestKey []byte, blockNum uint64) error {
	_, err := deleteLiveStateChangePostingWithScratch(db, latestKey, blockNum, nil)
	return err
}

func deleteLiveStateChangePostingWithScratch(db StateKVLatestStore, latestKey []byte, blockNum uint64, keyScratch []byte) ([]byte, error) {
	return deleteLiveStateChangePostingByHash(db, stateChangePostingHash(latestKey), blockNum, keyScratch)
}

// deleteLiveStateChangePostingByHash lets a block-level prune plan deduplicate
// repeated latest keys before point reads and reuse one physical-key buffer.
func deleteLiveStateChangePostingByHash(db StateKVLatestStore, hash [sha256.Size]byte, blockNum uint64, keyScratch []byte) ([]byte, error) {
	key := appendStateChangePostingKey(keyScratch[:0], hash, blockNum)
	value, exists, err := readLiveStateChangePosting(db, key, blockNum)
	if err != nil || !exists {
		return key, err
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

// readLiveStateChangePosting keeps success and miss paths allocation-free apart
// from the posting key itself. Prune scans invoke this for every historical
// change, while the formatted block context is only useful on an actual read
// error.
func readLiveStateChangePosting(db StateKVLatestStore, key []byte, blockNum uint64) ([]byte, bool, error) {
	if db == nil {
		return nil, false, fmt.Errorf("rawdb: nil database while reading state change posting at block %d", blockNum)
	}
	if reader, ok := db.(interface {
		GetWithPresence([]byte) ([]byte, bool, error)
	}); ok {
		value, exists, err := reader.GetWithPresence(key)
		if err != nil {
			return nil, false, fmt.Errorf("rawdb: read state change posting at block %d: %w", blockNum, err)
		}
		return value, exists, nil
	}
	exists, err := db.Has(key)
	if err != nil {
		return nil, false, fmt.Errorf("rawdb: read state change posting at block %d presence: %w", blockNum, err)
	}
	if !exists {
		return nil, false, nil
	}
	value, err := db.Get(key)
	if err != nil {
		return nil, false, fmt.Errorf("rawdb: read state change posting at block %d: %w", blockNum, err)
	}
	return append([]byte(nil), value...), true, nil
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
	postings         *etl.Collector
	directory        *etl.Collector
	latestKeyScratch []byte
	result           StateChangePostingBuildResult
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
	latestKey, err := appendStateDomainChangeLatestKey(c.latestKeyScratch[:0], change)
	if err != nil {
		return err
	}
	c.latestKeyScratch = latestKey
	hash := stateChangePostingHash(latestKey)
	if err := c.postings.PutEncoded(sha256.Size+8, 0, func(sortKey, _ []byte) {
		copy(sortKey, hash[:])
		binary.BigEndian.PutUint64(sortKey[sha256.Size:], change.BlockNum)
	}); err != nil {
		return err
	}
	if err := c.directory.PutEncoded(len(stateChangeKeyDirectoryPrefix)+len(latestKey), 0, func(key, _ []byte) {
		copy(key, stateChangeKeyDirectoryPrefix)
		copy(key[len(stateChangeKeyDirectoryPrefix):], latestKey)
	}); err != nil {
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

// StateChangePostingPruneProgress reports bounded checkpoints while pruning
// the derived state-change index. Callers must treat Result as a snapshot; the
// final authoritative counters are returned by the prune function.
type StateChangePostingPruneProgress struct {
	Phase  string
	Result StateChangePostingPruneResult
}

type StateChangePostingPruneProgressFn func(StateChangePostingPruneProgress)

const stateChangePostingPruneProgressRows uint64 = 65_536

// PruneStaleStateChangePostingIndex reclaims only wholly stale immutable
// frames. Mixed live/stale frames remain unchanged and are filtered exactly by
// the mandatory changeset collision check.
func PruneStaleStateChangePostingIndex(db ethdb.KeyValueStore) (StateChangePostingPruneResult, error) {
	return pruneStaleStateChangePostingIndex(context.Background(), db, 0, false, nil)
}

// PruneStaleStateChangePostingIndexContext is the cancellable form used by
// online maintenance when no authoritative prune watermark is available.
func PruneStaleStateChangePostingIndexContext(ctx context.Context, db ethdb.KeyValueStore) (StateChangePostingPruneResult, error) {
	return pruneStaleStateChangePostingIndex(ctx, db, 0, false, nil)
}

// PruneStaleStateChangePostingIndexThroughContext reclaims immutable posting
// frames after hot StateDomainChanges have durably been pruned through the
// inclusive block watermark. Frames ending at or below the watermark can be
// deleted by one sequential scan without random changeset reads. A frame that
// crosses the watermark is retained if any newer authoritative changeset is
// still live.
func PruneStaleStateChangePostingIndexThroughContext(ctx context.Context, db ethdb.KeyValueStore, prunedThrough uint64) (StateChangePostingPruneResult, error) {
	return pruneStaleStateChangePostingIndex(ctx, db, prunedThrough, true, nil)
}

// PruneStaleStateChangePostingIndexThroughContextWithProgress is the offline
// maintenance form. The durable hot-prune watermark makes the posting sweep
// sequential, and progress checkpoints keep long scans observable.
func PruneStaleStateChangePostingIndexThroughContextWithProgress(ctx context.Context, db ethdb.KeyValueStore, prunedThrough uint64, progress StateChangePostingPruneProgressFn) (StateChangePostingPruneResult, error) {
	return pruneStaleStateChangePostingIndex(ctx, db, prunedThrough, true, progress)
}

func pruneStaleStateChangePostingIndex(ctx context.Context, db ethdb.KeyValueStore, prunedThrough uint64, hasPruneWatermark bool, progress StateChangePostingPruneProgressFn) (StateChangePostingPruneResult, error) {
	var result StateChangePostingPruneResult
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	// With a durable watermark, retain the exact set of hashes which still
	// have a live posting. The following directory sweep can then perform two
	// ordered scans instead of opening one Pebble iterator per logical key.
	// The set is bounded by the unpruned hot suffix rather than all history.
	var livePostingHashes map[[sha256.Size]byte]struct{}
	if hasPruneWatermark {
		livePostingHashes = make(map[[sha256.Size]byte]struct{})
	}
	report := func(phase string) {
		if progress != nil {
			progress(StateChangePostingPruneProgress{Phase: phase, Result: result})
		}
	}
	postingIt := db.NewIterator(stateChangePostingPrefix, nil)
	batch := db.NewBatch()
	defer batch.Close()
	for postingIt.Next() {
		if err := ctx.Err(); err != nil {
			postingIt.Release()
			return result, err
		}
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
		// Hot-prune progress advances only after all authoritative changesets in
		// the covered prefix have been deleted. Trust that durable boundary and
		// reserve point/range reads for the at most one crossing frame per hash.
		if !hasPruneWatermark || blocks[len(blocks)-1] > prunedThrough {
			for _, blockNum := range blocks {
				if hasPruneWatermark && blockNum <= prunedThrough {
					continue
				}
				if err := ctx.Err(); err != nil {
					postingIt.Release()
					return result, err
				}
				err := IterateStateDomainChanges(db, blockNum, func(change *StateDomainChange) (bool, error) {
					if err := ctx.Err(); err != nil {
						return false, err
					}
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
		}
		if !live {
			if err := batch.Delete(key); err != nil {
				postingIt.Release()
				return result, err
			}
			result.PostingRowsDeleted++
		} else if hasPruneWatermark {
			var hash [sha256.Size]byte
			copy(hash[:], wantHash)
			livePostingHashes[hash] = struct{}{}
		}
		if result.PostingRowsScanned%stateChangePostingPruneProgressRows == 0 {
			report("postings")
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
	report("postings-complete")

	directoryIt := db.NewIterator(stateChangeKeyDirectoryPrefix, nil)
	for directoryIt.Next() {
		if err := ctx.Err(); err != nil {
			directoryIt.Release()
			return result, err
		}
		key := directoryIt.Key()
		if !bytes.HasPrefix(key, stateChangeKeyDirectoryPrefix) || len(key) == len(stateChangeKeyDirectoryPrefix) {
			directoryIt.Release()
			return result, errors.New("rawdb: malformed state change key directory row")
		}
		result.DirectoryRowsScanned++
		hash := stateChangePostingHash(key[len(stateChangeKeyDirectoryPrefix):])
		hasPosting := false
		if hasPruneWatermark {
			_, hasPosting = livePostingHashes[hash]
		} else {
			it := db.NewIterator(stateChangePostingHashPrefix(hash), nil)
			hasPosting = it.Next()
			err := it.Error()
			it.Release()
			if err != nil {
				directoryIt.Release()
				return result, err
			}
		}
		if !hasPosting {
			if err := batch.Delete(key); err != nil {
				directoryIt.Release()
				return result, err
			}
			result.DirectoryRowsDeleted++
		}
		if result.DirectoryRowsScanned%stateChangePostingPruneProgressRows == 0 {
			report("directory")
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
	if err := batch.Write(); err != nil {
		return result, err
	}
	report("directory-complete")
	return result, nil
}

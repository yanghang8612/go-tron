package snapshots

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

// stateDomainChangeHistoryBuildResult keeps build-only accessor details out of
// the public snapshot API while letting tests prove bounded ETL spill behavior.
type stateDomainChangeHistoryBuildResult struct {
	recordETL   etl.Stats
	refs        []SegmentRef
	accessorETL etl.Stats
}

var errStateDomainChangeHistoryRecordsNotOrdered = errors.New("snapshots: state-domain-change history records are not ordered")

type stateDomainChangeHistoryBlockRange struct {
	from uint64
	to   uint64
}

// buildStateDomainChangeHistoryBinarySegmentsFromDB writes the production cold
// history trio without materializing a batch of StateDomainChange rows. Fresh
// block packs are already in tx/sequence order and stream directly into the
// history and txNum index; only key-ordered accessors require bounded ETL.
func buildStateDomainChangeHistoryBinarySegmentsFromDB(db ethdb.Iteratee, dir string, ref SegmentRef, cfg DomainCfg, opts etl.Options) (result stateDomainChangeHistoryBuildResult, err error) {
	return buildStateDomainChangeHistoryBinarySegmentsFromDBRange(db, dir, ref, cfg, opts, nil)
}

func buildStateDomainChangeHistoryBinarySegmentsFromDBRange(db ethdb.Iteratee, dir string, ref SegmentRef, cfg DomainCfg, opts etl.Options, blockRange *stateDomainChangeHistoryBlockRange) (result stateDomainChangeHistoryBuildResult, err error) {
	if ref.Kind == "" {
		ref.Kind = SegmentHistory
	}
	if ref.Dataset == "" {
		ref.Dataset = SegmentDatasetStateDomainChange
	}
	if ref.AggregationSteps == 0 {
		ref.AggregationSteps = 1
	}
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
		return result, fmt.Errorf("snapshots: state-domain-change binary segment %q is %s/%s, want state-domain-change/history", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return result, err
	}
	if cfg.IterateHotHistoryTxRangeChanges == nil || cfg.IterateHotHistoryTxRanges == nil {
		return result, errors.New("snapshots: missing state-domain history iterators")
	}
	if blockRange != nil && ((cfg.IterateHotHistoryBlockTxChanges == nil && cfg.IterateHotHistoryBlockTxBorrowed == nil) ||
		(cfg.IterateHotHistoryTxRangeBlocks == nil && cfg.IterateHotHistoryTxRangeBorrowed == nil)) {
		return result, errors.New("snapshots: missing bounded state-domain history iterators")
	}

	if opts.TempDir == "" {
		opts.TempDir = filepath.Join(dir, "etl")
	}
	segmentTmp, err := createStateDomainChangeHistoryTemp(dir, ref.Path, CompressHistorySegments)
	if err != nil {
		return result, err
	}
	defer segmentTmp.Close()
	if err := writeStateDomainChangeBinaryHeaderTo(segmentTmp, stateDomainChangeBinarySegmentMagic, ref.FromTxNum, ref.ToTxNum, 0); err != nil {
		return result, err
	}
	txRangeCount, err := writeStateDomainChangeBinaryTxRangeTableFromDB(segmentTmp, db, cfg, ref.FromTxNum, ref.ToTxNum, blockRange)
	if err != nil {
		return result, err
	}
	recordOffset := uint64(stateDomainChangeBinaryHeaderSize) + 8 + txRangeCount*stateDomainChangeBinaryTxRangeSize

	indexPath := stateDomainChangeBinaryIndexPath(ref.Path)
	indexAbs := filepath.Join(dir, indexPath)
	// Segment temp creation above already prepared this shared parent; the index
	// path only replaces the extension, so avoid a second MkdirAll/stat per pack.
	indexTmp, indexTmpName, err := createStateDomainChangeBinaryTempFileInDir(filepath.Dir(indexAbs), filepath.Base(indexAbs))
	if err != nil {
		return result, err
	}
	defer func() {
		_ = indexTmp.Close()
		_ = os.Remove(indexTmpName)
	}()
	if err := writeStateDomainChangeBinaryHeaderTo(indexTmp, stateDomainChangeBinaryIndexMagic, ref.FromTxNum, ref.ToTxNum, 0); err != nil {
		return result, err
	}
	accessorCollectors, err := newStateDomainChangeBinaryAccessorV4Collectors(opts)
	if err != nil {
		return result, err
	}
	defer func() {
		if accessorCollectors != nil {
			accessorCollectors.Close()
		}
	}()

	recordWriter := newStateDomainChangeHistoryRecordWriter(segmentTmp, indexTmp, accessorCollectors, ref, math.MaxUint64, recordOffset)
	defer func() { recordWriter.Release() }()
	borrowedStream := blockRange != nil && cfg.IterateHotHistoryBlockTxBorrowed != nil
	streamErr := iterateStateDomainChangeHistoryChanges(db, cfg, ref.FromTxNum, ref.ToTxNum, blockRange, func(change *rawdb.StateDomainChange) (bool, error) {
		if change == nil {
			return false, errors.New("snapshots: nil state-domain-change history record")
		}
		var err error
		if borrowedStream {
			err = recordWriter.WriteBorrowedChange(change)
		} else {
			err = recordWriter.WriteChange(change)
		}
		if err != nil {
			return false, err
		}
		return true, nil
	})
	if streamErr != nil && !errors.Is(streamErr, errStateDomainChangeHistoryRecordsNotOrdered) {
		return result, fmt.Errorf("snapshots: stream state-domain-change history records: %w", streamErr)
	}
	recordCount := recordWriter.count
	if errors.Is(streamErr, errStateDomainChangeHistoryRecordsNotOrdered) {
		// Positive-sequence legacy and repair rows predate the block-pack order
		// invariant. Discard the incomplete direct attempt and pay the bounded
		// record ETL cost only for those old inputs.
		recordWriter.Release()
		accessorCollectors.Close()
		accessorCollectors = nil
		if err := segmentTmp.Reset(); err != nil {
			return result, fmt.Errorf("snapshots: reset unordered state-domain-change history segment: %w", err)
		}
		if err := resetStateDomainChangeHistoryTempFile(indexTmp); err != nil {
			return result, fmt.Errorf("snapshots: reset unordered state-domain-change history index: %w", err)
		}

		recordCollector, err := etl.NewCollector(opts)
		if err != nil {
			return result, fmt.Errorf("snapshots: create state-domain-change history fallback ETL collector: %w", err)
		}
		defer recordCollector.Close()
		recordCount, err = collectStateDomainChangeHistoryRecords(db, cfg, ref.FromTxNum, ref.ToTxNum, blockRange, recordCollector)
		if err != nil {
			return result, err
		}
		if err := writeStateDomainChangeBinaryHeaderTo(segmentTmp, stateDomainChangeBinarySegmentMagic, ref.FromTxNum, ref.ToTxNum, recordCount); err != nil {
			return result, err
		}
		txRangeCount, err = writeStateDomainChangeBinaryTxRangeTableFromDB(segmentTmp, db, cfg, ref.FromTxNum, ref.ToTxNum, blockRange)
		if err != nil {
			return result, err
		}
		recordOffset = uint64(stateDomainChangeBinaryHeaderSize) + 8 + txRangeCount*stateDomainChangeBinaryTxRangeSize
		if err := writeStateDomainChangeBinaryHeaderTo(indexTmp, stateDomainChangeBinaryIndexMagic, ref.FromTxNum, ref.ToTxNum, 0); err != nil {
			return result, err
		}
		accessorCollectors, err = newStateDomainChangeBinaryAccessorV4Collectors(opts)
		if err != nil {
			return result, err
		}
		recordWriter = newStateDomainChangeHistoryRecordWriter(segmentTmp, indexTmp, accessorCollectors, ref, recordCount, recordOffset)
		result.recordETL, err = recordCollector.Load(recordWriter)
		if err != nil {
			return result, fmt.Errorf("snapshots: sort fallback state-domain-change history records: %w", err)
		}
	} else {
		recordWriter.expected = recordCount
	}
	if err := recordWriter.Finish(); err != nil {
		return result, err
	}
	if err := writeStateDomainChangeBinaryHeaderCount(segmentTmp, recordCount); err != nil {
		return result, err
	}
	if err := writeStateDomainChangeBinaryHeaderCount(indexTmp, recordWriter.indexWritten); err != nil {
		return result, err
	}

	var segmentRef, indexRef, accessorRef SegmentRef
	published := false
	defer func() {
		if published {
			return
		}
		for _, output := range []SegmentRef{segmentRef, indexRef, accessorRef} {
			if output.Path != "" {
				_ = os.Remove(filepath.Join(dir, output.Path))
			}
		}
	}()

	segmentRef, err = segmentTmp.Finalize(ref, true)
	if err != nil {
		return result, err
	}
	indexRef, accessorRef, result.accessorETL, err = finalizeStateDomainChangeBinaryCompanions(dir, segmentRef, indexTmp, indexTmpName, accessorCollectors, recordCount)
	if err != nil {
		return result, err
	}

	// Hot pruning trusts manifest coverage. Reopen the just-built history through
	// its production reader and bind its complete compressed layout plus sampled
	// payload blocks to the writer's exact logical end/counts. The writer already
	// checked every tx-range and record while emitting; imported/offline files
	// still receive the exhaustive replay. Derived writers similarly validate
	// every ordered row, so their trusted check only reopens fixed layouts.
	if err := validateBuiltStateDomainChangeBinaryFiles(dir, segmentRef, indexRef, accessorRef, recordWriter.segmentOff, recordWriter.indexWritten, recordCount, txRangeCount); err != nil {
		return result, err
	}
	published = true
	result.refs = []SegmentRef{segmentRef, accessorRef, indexRef}
	return result, nil
}

func validateBuiltStateDomainChangeBinaryFiles(dir string, segmentRef, indexRef, accessorRef SegmentRef, logicalEnd, indexCount, recordCount, txRangeCount uint64) error {
	if err := validateTrustedBuiltHistorySegment(dir, segmentRef, logicalEnd, recordCount, txRangeCount); err != nil {
		return fmt.Errorf("snapshots: state-domain-change history segment self-check: %w", err)
	}
	index, indexHeader, err := openStateDomainChangeBinaryIndexReader(dir, indexRef)
	if err != nil {
		return fmt.Errorf("snapshots: state-domain-change history index self-check: %w", err)
	}
	if err := index.Close(); err != nil {
		return fmt.Errorf("snapshots: close state-domain-change history index self-check: %w", err)
	}
	if indexHeader.count != indexCount {
		return fmt.Errorf("snapshots: state-domain-change history index count %d, want %d", indexHeader.count, indexCount)
	}
	accessor, accessorHeader, accessorSize, err := openStateDomainChangeBinaryAccessorReader(dir, accessorRef)
	if err != nil {
		return fmt.Errorf("snapshots: state-domain-change history accessor self-check: %w", err)
	}
	if accessorHeader.count != recordCount {
		_ = accessor.Close()
		return fmt.Errorf("snapshots: state-domain-change history accessor count %d, want %d", accessorHeader.count, recordCount)
	}
	if accessorHeader.version != stateDomainChangeBinaryVersionV4 {
		_ = accessor.Close()
		return fmt.Errorf("snapshots: state-domain-change history accessor version %d, want %d", accessorHeader.version, stateDomainChangeBinaryVersionV4)
	}
	layout, err := stateDomainChangeBinaryAccessorV4LayoutAt(accessor, accessorSize, accessorHeader)
	if err == nil {
		if layout.groupCount == 0 {
			if accessorSize != layout.groupPayloadStart {
				err = fmt.Errorf("snapshots: state-domain-change history accessor has %d trailing bytes", accessorSize-layout.groupPayloadStart)
			}
		} else {
			_, err = readStateDomainChangeBinaryAccessorV4GroupMetaAt(accessor, layout, layout.groupCount-1, accessorSize)
		}
	}
	if err != nil {
		_ = accessor.Close()
		return fmt.Errorf("snapshots: state-domain-change history accessor layout self-check: %w", err)
	}
	if err := accessor.Close(); err != nil {
		return fmt.Errorf("snapshots: close state-domain-change history accessor self-check: %w", err)
	}
	return nil
}

func resetStateDomainChangeHistoryTempFile(file *os.File) error {
	if file == nil {
		return errors.New("snapshots: nil state-domain-change history temp file")
	}
	if err := file.Truncate(0); err != nil {
		return err
	}
	_, err := file.Seek(0, io.SeekStart)
	return err
}

type stateDomainChangeBinaryWriteAtWriter interface {
	io.Writer
	io.WriterAt
}

func collectStateDomainChangeHistoryRecords(db ethdb.Iteratee, cfg DomainCfg, fromTxNum, toTxNum uint64, blockRange *stateDomainChangeHistoryBlockRange, collector *etl.Collector) (uint64, error) {
	if collector == nil {
		return 0, errors.New("snapshots: nil state-domain-change history record ETL collector")
	}
	var count uint64
	err := iterateStateDomainChangeHistoryChanges(db, cfg, fromTxNum, toTxNum, blockRange, func(change *rawdb.StateDomainChange) (bool, error) {
		if change == nil {
			return false, errors.New("snapshots: nil state-domain-change history record")
		}
		if change.TxNum < fromTxNum || change.TxNum > toTxNum {
			return false, fmt.Errorf("snapshots: state-domain-change tx %d outside segment range [%d,%d]", change.TxNum, fromTxNum, toTxNum)
		}
		if count == math.MaxUint64 {
			return false, errors.New("snapshots: state-domain-change history record count overflows")
		}
		value, err := encodeStateDomainChangeRecord(change)
		if err != nil {
			return false, err
		}
		if err := collector.PutOwned(stateDomainChangeHistoryRecordETLSortKey(change, count), value); err != nil {
			return false, err
		}
		count++
		return true, nil
	})
	return count, err
}

func writeStateDomainChangeBinaryTxRangeTableFromDB(file stateDomainChangeBinaryWriteAtWriter, db ethdb.Iteratee, cfg DomainCfg, fromTxNum, toTxNum uint64, blockRange *stateDomainChangeHistoryBlockRange) (uint64, error) {
	if file == nil {
		return 0, errors.New("snapshots: nil state-domain-change history file")
	}
	writer := acquireStateDomainChangeHistoryWriter(file)
	defer releaseStateDomainChangeHistoryWriter(&writer)
	if err := writeStateDomainChangeBinaryTxRangeCount(writer, 0); err != nil {
		return 0, err
	}
	const maxCount = (math.MaxUint64 - uint64(stateDomainChangeBinaryHeaderSize) - 8) / stateDomainChangeBinaryTxRangeSize
	var (
		written uint64
		raw     [stateDomainChangeBinaryTxRangeSize]byte
	)
	if err := iterateStateDomainChangeHistoryTxRanges(db, cfg, fromTxNum, toTxNum, blockRange, func(row *rawdb.StateTxRange) error {
		if written >= maxCount {
			return fmt.Errorf("snapshots: state-domain-change tx range count exceeds maximum %d", maxCount)
		}
		if err := putStateDomainChangeBinaryTxRangeEntry(&raw, row); err != nil {
			return err
		}
		if _, err := writer.Write(raw[:]); err != nil {
			return err
		}
		written++
		return nil
	}); err != nil {
		return 0, err
	}
	if err := writer.Flush(); err != nil {
		return 0, err
	}
	var countRaw [8]byte
	binary.BigEndian.PutUint64(countRaw[:], written)
	if _, err := file.WriteAt(countRaw[:], stateDomainChangeBinaryHeaderSize); err != nil {
		return 0, err
	}
	return written, nil
}

func iterateStateDomainChangeHistoryTxRanges(db ethdb.Iteratee, cfg DomainCfg, fromTxNum, toTxNum uint64, blockRange *stateDomainChangeHistoryBlockRange, fn func(*rawdb.StateTxRange) error) error {
	var (
		previousBlock uint64
		havePrevious  bool
	)
	return iterateStateDomainChangeHistorySourceTxRanges(db, cfg, blockRange, func(row *rawdb.StateTxRange) (bool, error) {
		if row == nil {
			return false, errors.New("snapshots: nil state-domain-change tx range")
		}
		if row.EndTxNum < row.BeginTxNum {
			return false, fmt.Errorf("snapshots: state tx range for block %d is inverted", row.BlockNum)
		}
		if row.EndTxNum < fromTxNum || row.BeginTxNum > toTxNum {
			return true, nil
		}
		if havePrevious && row.BlockNum <= previousBlock {
			return false, fmt.Errorf("snapshots: state-domain-change tx ranges are not ordered at block %d", row.BlockNum)
		}
		if err := fn(row); err != nil {
			return false, err
		}
		previousBlock = row.BlockNum
		havePrevious = true
		return true, nil
	})
}

func iterateStateDomainChangeHistoryChanges(db ethdb.Iteratee, cfg DomainCfg, fromTxNum, toTxNum uint64, blockRange *stateDomainChangeHistoryBlockRange, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	if blockRange == nil {
		return cfg.IterateHotHistoryTxRangeChanges(db, fromTxNum, toTxNum, fn)
	}
	if cfg.IterateHotHistoryBlockTxChanges == nil && cfg.IterateHotHistoryBlockTxBorrowed == nil {
		return errors.New("snapshots: missing bounded state-domain history change iterator")
	}
	if cfg.IterateHotHistoryBlockTxBorrowed != nil {
		return cfg.IterateHotHistoryBlockTxBorrowed(db, blockRange.from, blockRange.to, fromTxNum, toTxNum, fn)
	}
	return cfg.IterateHotHistoryBlockTxChanges(db, blockRange.from, blockRange.to, fromTxNum, toTxNum, fn)
}

func iterateStateDomainChangeHistorySourceTxRanges(db ethdb.Iteratee, cfg DomainCfg, blockRange *stateDomainChangeHistoryBlockRange, fn func(*rawdb.StateTxRange) (bool, error)) error {
	if blockRange == nil {
		return cfg.IterateHotHistoryTxRanges(db, fn)
	}
	if cfg.IterateHotHistoryTxRangeBlocks == nil && cfg.IterateHotHistoryTxRangeBorrowed == nil {
		return errors.New("snapshots: missing bounded state-domain history tx-range iterator")
	}
	if cfg.IterateHotHistoryTxRangeBorrowed != nil {
		return cfg.IterateHotHistoryTxRangeBorrowed(db, blockRange.from, blockRange.to, fn)
	}
	return cfg.IterateHotHistoryTxRangeBlocks(db, blockRange.from, blockRange.to, fn)
}

func appendStateDomainChangeBinaryRecordFrame(dst []byte, change *rawdb.StateDomainChange) ([]byte, error) {
	payloadSize, err := stateDomainChangeRecordV5Size(change)
	if err != nil {
		return nil, err
	}
	frameSize := 4 + payloadSize
	start := len(dst)
	if cap(dst)-start < frameSize {
		capacity := cap(dst) * 2
		if capacity < start+frameSize {
			capacity = start + frameSize
		}
		grown := make([]byte, start, capacity)
		copy(grown, dst)
		dst = grown
	}
	dst = dst[:start+frameSize]
	binary.BigEndian.PutUint32(dst[start:start+4], uint32(payloadSize))
	putStateDomainChangeRecordV5(dst[start+4:], change)
	return dst, nil
}

func encodeStateDomainChangeBinaryAccessorEntryFrame(entry stateDomainChangeBinaryAccessorEntry) ([]byte, error) {
	var buf bytes.Buffer
	if err := writeStateDomainChangeBinaryAccessorEntryTo(&buf, entry); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// stateDomainChangeHistoryRecordETLSortKey mirrors
// compareStateDomainChangeForBinary. Variable-width byte fields are escaped and
// terminated so byte ordering, including prefix ordering, survives external
// sorting. ordinal preserves duplicate comparator-equivalent records because
// Collector intentionally collapses identical keys.
func stateDomainChangeHistoryRecordETLSortKey(change *rawdb.StateDomainChange, ordinal uint64) []byte {
	out := make([]byte, 0, 128+len(change.Key)+len(change.Prev)+len(change.Next))
	var fixed [8]byte
	appendUint64 := func(value uint64) {
		binary.BigEndian.PutUint64(fixed[:], value)
		out = append(out, fixed[:]...)
	}
	appendUint16 := func(value uint16) {
		var raw [2]byte
		binary.BigEndian.PutUint16(raw[:], value)
		out = append(out, raw[:]...)
	}
	appendBytes := func(value []byte) {
		for _, b := range value {
			if b == 0 {
				out = append(out, 0, 0xff)
				continue
			}
			out = append(out, b)
		}
		out = append(out, 0, 0)
	}
	appendUint64(change.TxNum)
	appendUint64(change.Seq)
	appendUint64(change.BlockNum)
	out = append(out, change.BlockHash[:]...)
	out = append(out, byte(change.FlatDomain))
	out = append(out, change.Owner[:]...)
	appendUint64(change.Generation)
	appendUint16(uint16(change.Domain))
	appendBytes(change.Key)
	if change.PrevExists {
		out = append(out, 1)
	} else {
		out = append(out, 0)
	}
	appendBytes(change.Prev)
	if change.NextExists {
		out = append(out, 1)
	} else {
		out = append(out, 0)
	}
	appendBytes(change.Next)
	appendUint64(ordinal)
	return out
}

type stateDomainChangeHistoryRecordWriter struct {
	segment       *bufio.Writer
	index         *bufio.Writer
	accessors     *stateDomainChangeBinaryAccessorV4Collectors
	ref           SegmentRef
	expected      uint64
	count         uint64
	segmentOff    uint64
	indexWritten  uint64
	currentIndex  stateDomainChangeBinaryTxOffset
	indexScratch  [stateDomainChangeBinaryIndexEntrySize]byte
	haveIndex     bool
	previous      *rawdb.StateDomainChange
	recordScratch []byte
}

const stateDomainChangeHistoryWriteBufferSize = 256 << 10

func newStateDomainChangeHistoryRecordWriter(segment io.Writer, index *os.File, accessors *stateDomainChangeBinaryAccessorV4Collectors, ref SegmentRef, expected, segmentOff uint64) *stateDomainChangeHistoryRecordWriter {
	return &stateDomainChangeHistoryRecordWriter{
		segment:    acquireStateDomainChangeHistoryWriter(segment),
		index:      acquireStateDomainChangeHistoryWriter(index),
		accessors:  accessors,
		ref:        ref,
		expected:   expected,
		segmentOff: segmentOff,
	}
}

// Put implements ethdb.KeyValueWriter for the bounded legacy/repair fallback.
// Fresh block packs bypass it and call WriteChange with decoded rows directly.
func (w *stateDomainChangeHistoryRecordWriter) Put(_ []byte, value []byte) error {
	if w == nil || w.segment == nil || w.index == nil {
		return errors.New("snapshots: nil state-domain-change history record writer")
	}
	change, err := decodeStateDomainChangeRecord(value)
	if err != nil {
		return err
	}
	return w.WriteChange(change)
}

// WriteChange emits one already-decoded history row while maintaining the
// companion txNum index. Cold builds and compaction both call it directly from
// their ordered source streams, avoiding a record ETL encode/decode round trip.
func (w *stateDomainChangeHistoryRecordWriter) WriteChange(change *rawdb.StateDomainChange) error {
	return w.writeChange(change, false)
}

// WriteBorrowedChange consumes a callback-scoped row from the canonical block
// pack iterator. That iterator validates block sequence and txNum order, so the
// writer must not retain the borrowed pointer for an adjacent-row comparison.
func (w *stateDomainChangeHistoryRecordWriter) WriteBorrowedChange(change *rawdb.StateDomainChange) error {
	return w.writeChange(change, true)
}

func (w *stateDomainChangeHistoryRecordWriter) writeChange(change *rawdb.StateDomainChange, trustedOrder bool) error {
	if w == nil || w.segment == nil || w.index == nil {
		return errors.New("snapshots: nil state-domain-change history record writer")
	}
	if change == nil {
		return errors.New("snapshots: nil state-domain-change history record")
	}
	if w.count >= w.expected {
		return fmt.Errorf("snapshots: state-domain-change history emitted more than %d records", w.expected)
	}
	if change.TxNum < w.ref.FromTxNum || change.TxNum > w.ref.ToTxNum {
		return fmt.Errorf("snapshots: state-domain-change tx %d outside segment range [%d,%d]", change.TxNum, w.ref.FromTxNum, w.ref.ToTxNum)
	}
	if !trustedOrder && w.previous != nil && compareStateDomainChangeForBinary(w.previous, change) > 0 {
		return errStateDomainChangeHistoryRecordsNotOrdered
	}
	if w.accessors != nil {
		if err := w.accessors.Collect(change, w.segmentOff, w.count); err != nil {
			return err
		}
	}
	if !w.haveIndex {
		w.currentIndex = stateDomainChangeBinaryTxOffset{txNum: change.TxNum, offset: w.segmentOff, recordIndex: w.count, count: 1}
		w.haveIndex = true
	} else if w.currentIndex.txNum == change.TxNum {
		if w.currentIndex.count == math.MaxUint64 {
			return errors.New("snapshots: state-domain-change tx index count overflows")
		}
		w.currentIndex.count++
	} else {
		if err := w.flushIndex(); err != nil {
			return err
		}
		w.currentIndex = stateDomainChangeBinaryTxOffset{txNum: change.TxNum, offset: w.segmentOff, recordIndex: w.count, count: 1}
		w.haveIndex = true
	}

	frame, err := appendStateDomainChangeBinaryRecordFrame(w.recordScratch[:0], change)
	if err != nil {
		return err
	}
	w.recordScratch = frame
	if _, err := w.segment.Write(frame); err != nil {
		return err
	}
	if uint64(len(frame)) > math.MaxUint64-w.segmentOff {
		return errors.New("snapshots: state-domain-change segment offset overflows")
	}
	w.segmentOff += uint64(len(frame))
	w.count++
	// Owning producers keep the decoded row valid until the next WriteChange
	// call, so retain it for the adjacent-order check. Borrowed iteration already
	// validated order and invalidates the row as soon as this call returns.
	if trustedOrder {
		w.previous = nil
	} else {
		w.previous = change
	}
	return nil
}

func (*stateDomainChangeHistoryRecordWriter) Delete([]byte) error {
	return errors.New("snapshots: state-domain-change history record writer does not support deletes")
}

func (w *stateDomainChangeHistoryRecordWriter) Finish() error {
	if w == nil {
		return errors.New("snapshots: nil state-domain-change history record writer")
	}
	defer w.Release()
	if err := w.flushIndex(); err != nil {
		return err
	}
	if w.count != w.expected {
		return fmt.Errorf("snapshots: state-domain-change history emitted %d records, want %d", w.count, w.expected)
	}
	if err := w.segment.Flush(); err != nil {
		return err
	}
	if err := w.index.Flush(); err != nil {
		return err
	}
	return nil
}

func (w *stateDomainChangeHistoryRecordWriter) Release() {
	if w == nil {
		return
	}
	releaseStateDomainChangeHistoryWriter(&w.segment)
	releaseStateDomainChangeHistoryWriter(&w.index)
}

func (w *stateDomainChangeHistoryRecordWriter) flushIndex() error {
	if !w.haveIndex {
		return nil
	}
	putStateDomainChangeBinaryIndexEntry(&w.indexScratch, w.currentIndex)
	if _, err := w.index.Write(w.indexScratch[:]); err != nil {
		return err
	}
	w.indexWritten++
	w.haveIndex = false
	return nil
}

func writeStateDomainChangeBinaryHeaderCount(file io.WriterAt, count uint64) error {
	if file == nil {
		return errors.New("snapshots: nil state-domain-change binary file")
	}
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], count)
	_, err := file.WriteAt(raw[:], 28)
	return err
}

type stateDomainChangeBinaryAccessorV3ExactETLWriter struct {
	file     *bufio.Writer
	expected uint64
	count    uint64
	previous stateDomainChangeBinaryAccessorV3ExactEntry
	havePrev bool
}

func (w *stateDomainChangeBinaryAccessorV3ExactETLWriter) Put(_ []byte, value []byte) error {
	if w == nil || w.file == nil {
		return errors.New("snapshots: nil state-domain-change accessor v3 exact ETL writer")
	}
	if len(value) != stateDomainChangeBinaryAccessorV3ExactEntrySize {
		return fmt.Errorf("snapshots: state-domain-change accessor v3 exact value size %d, want %d", len(value), stateDomainChangeBinaryAccessorV3ExactEntrySize)
	}
	if w.count >= w.expected {
		return fmt.Errorf("snapshots: state-domain-change accessor v3 exact emitted more than %d entries", w.expected)
	}
	var entry stateDomainChangeBinaryAccessorV3ExactEntry
	copy(entry.hash[:], value[:stateDomainChangeBinaryAccessorV3HashSize])
	entry.offset = binary.BigEndian.Uint64(value[stateDomainChangeBinaryAccessorV3HashSize : stateDomainChangeBinaryAccessorV3HashSize+8])
	entry.recordIndex = binary.BigEndian.Uint32(value[stateDomainChangeBinaryAccessorV3HashSize+8:])
	if entry.offset < stateDomainChangeBinaryHeaderSize {
		return fmt.Errorf("snapshots: state-domain-change accessor v3 exact record offset %d is invalid", entry.offset)
	}
	if w.havePrev {
		if cmp := bytes.Compare(w.previous.hash[:], entry.hash[:]); cmp > 0 ||
			(cmp == 0 && (w.previous.offset > entry.offset || (w.previous.offset == entry.offset && w.previous.recordIndex >= entry.recordIndex))) {
			return errors.New("snapshots: state-domain-change accessor v3 exact ETL rows are not strictly ordered")
		}
	}
	if _, err := w.file.Write(value); err != nil {
		return err
	}
	w.previous = entry
	w.havePrev = true
	w.count++
	return nil
}

func (*stateDomainChangeBinaryAccessorV3ExactETLWriter) Delete([]byte) error {
	return errors.New("snapshots: state-domain-change accessor v3 exact ETL writer does not support deletes")
}

func (w *stateDomainChangeBinaryAccessorV3ExactETLWriter) Finish() error {
	if w == nil || w.file == nil {
		return errors.New("snapshots: nil state-domain-change accessor v3 exact ETL writer")
	}
	defer w.Release()
	return w.file.Flush()
}

func (w *stateDomainChangeBinaryAccessorV3ExactETLWriter) Release() {
	if w == nil {
		return
	}
	releaseStateDomainChangeHistoryWriter(&w.file)
}

type stateDomainChangeBinaryAccessorV4GroupETLWriter struct {
	payload       *bufio.Writer
	offsets       *[]uint64
	payloadOffset uint64
	groups        uint64
	records       uint64
	haveGroup     bool
	currentKey    [stateDomainChangeBinaryAccessorV3GroupKeySize]byte
	currentCount  uint64
}

func (w *stateDomainChangeBinaryAccessorV4GroupETLWriter) Put(_ []byte, value []byte) error {
	if w == nil || w.payload == nil || w.offsets == nil {
		return errors.New("snapshots: nil state-domain-change accessor v3 group ETL writer")
	}
	if len(value) != stateDomainChangeBinaryAccessorV3GroupKeySize+stateDomainChangeBinaryAccessorV4GroupEntrySize {
		return fmt.Errorf("snapshots: state-domain-change accessor v4 group value size %d, want %d", len(value), stateDomainChangeBinaryAccessorV3GroupKeySize+stateDomainChangeBinaryAccessorV4GroupEntrySize)
	}
	key := value[:stateDomainChangeBinaryAccessorV3GroupKeySize]
	offset := binary.BigEndian.Uint64(value[stateDomainChangeBinaryAccessorV3GroupKeySize+4 : stateDomainChangeBinaryAccessorV3GroupKeySize+12])
	if offset < stateDomainChangeBinaryHeaderSize {
		return fmt.Errorf("snapshots: state-domain-change accessor v3 group record offset %d is invalid", offset)
	}
	if !w.haveGroup || !bytes.Equal(w.currentKey[:], key) {
		if err := w.finishGroup(); err != nil {
			return err
		}
		*w.offsets = append(*w.offsets, w.payloadOffset)
		if _, err := w.payload.Write(key); err != nil {
			return err
		}
		if err := writeZeroes(w.payload, 8); err != nil {
			return err
		}
		w.payloadOffset += stateDomainChangeBinaryAccessorV3GroupKeySize + 8
		copy(w.currentKey[:], key)
		w.currentCount = 0
		w.haveGroup = true
		w.groups++
	}
	if _, err := w.payload.Write(value[stateDomainChangeBinaryAccessorV3GroupKeySize:]); err != nil {
		return err
	}
	w.payloadOffset += stateDomainChangeBinaryAccessorV4GroupEntrySize
	w.currentCount++
	w.records++
	return nil
}

func (w *stateDomainChangeBinaryAccessorV4GroupETLWriter) finishGroup() error {
	if w == nil || !w.haveGroup {
		return nil
	}
	if w.currentCount == 0 {
		return errors.New("snapshots: state-domain-change accessor v4 group has no records")
	}
	w.haveGroup = false
	return nil
}

func (*stateDomainChangeBinaryAccessorV4GroupETLWriter) Delete([]byte) error {
	return errors.New("snapshots: state-domain-change accessor v4 group ETL writer does not support deletes")
}

func (w *stateDomainChangeBinaryAccessorV4GroupETLWriter) Finish() error {
	if w == nil || w.payload == nil || w.offsets == nil {
		return errors.New("snapshots: nil state-domain-change accessor v4 group ETL writer")
	}
	defer w.Release()
	if err := w.finishGroup(); err != nil {
		return err
	}
	if err := w.payload.Flush(); err != nil {
		return err
	}
	return nil
}

func (w *stateDomainChangeBinaryAccessorV4GroupETLWriter) Release() {
	if w == nil {
		return
	}
	releaseStateDomainChangeHistoryWriter(&w.payload)
}

// copyStateDomainChangeBinaryAccessorV4GroupPayload turns the buffered count
// placeholders into their fixed-width values while the private group file is
// copied into the final accessor. The temp file and final output therefore
// remain sequential; no group-sized memory buffer or random pwrite is needed.
func copyStateDomainChangeBinaryAccessorV4GroupPayload(dst io.Writer, src io.Reader, offsets []uint64, payloadSize uint64) (int64, error) {
	if payloadSize > math.MaxInt64 {
		return 0, fmt.Errorf("snapshots: state-domain-change accessor v4 group payload size %d exceeds int64", payloadSize)
	}
	if len(offsets) == 0 {
		if payloadSize != 0 {
			return 0, fmt.Errorf("snapshots: state-domain-change accessor v4 group payload has size %d without groups", payloadSize)
		}
	} else if offsets[0] != 0 {
		return 0, fmt.Errorf("snapshots: state-domain-change accessor v4 first group offset %d, want 0", offsets[0])
	}
	for i, start := range offsets {
		next := payloadSize
		if i+1 < len(offsets) {
			next = offsets[i+1]
		}
		entriesStart := start + stateDomainChangeBinaryAccessorV3GroupKeySize + 8
		if entriesStart < start || next < entriesStart {
			return 0, fmt.Errorf("snapshots: state-domain-change accessor v4 group %d has invalid payload range [%d,%d)", i, start, next)
		}
		entriesSize := next - entriesStart
		if entriesSize == 0 || entriesSize%stateDomainChangeBinaryAccessorV4GroupEntrySize != 0 {
			return 0, fmt.Errorf("snapshots: state-domain-change accessor v4 group %d payload size %d is invalid", i, entriesSize)
		}
	}

	buffer := stateDomainChangeHistoryCopyBufferPool.Get().(*stateDomainChangeHistoryCopyBuffer)
	defer stateDomainChangeHistoryCopyBufferPool.Put(buffer)
	var (
		copied     uint64
		groupIndex int
		rawCount   [8]byte
	)
	for {
		read, readErr := src.Read(buffer[:])
		if read > 0 {
			if copied > payloadSize || uint64(read) > payloadSize-copied {
				return int64(copied), fmt.Errorf("snapshots: state-domain-change accessor v4 group payload exceeds size %d", payloadSize)
			}
			chunkEnd := copied + uint64(read)
			for groupIndex < len(offsets) {
				countStart := offsets[groupIndex] + stateDomainChangeBinaryAccessorV3GroupKeySize
				countEnd := countStart + 8
				if countStart >= chunkEnd {
					break
				}
				if countEnd <= copied {
					groupIndex++
					continue
				}
				next := payloadSize
				if groupIndex+1 < len(offsets) {
					next = offsets[groupIndex+1]
				}
				entriesStart := countEnd
				binary.BigEndian.PutUint64(rawCount[:], (next-entriesStart)/stateDomainChangeBinaryAccessorV4GroupEntrySize)
				patchStart := max(copied, countStart)
				patchEnd := min(chunkEnd, countEnd)
				copy(buffer[patchStart-copied:patchEnd-copied], rawCount[patchStart-countStart:patchEnd-countStart])
				if patchEnd < countEnd {
					break
				}
				groupIndex++
			}
			wrote, writeErr := dst.Write(buffer[:read])
			if wrote < 0 || wrote > read {
				wrote = 0
				if writeErr == nil {
					writeErr = io.ErrShortWrite
				}
			}
			copied += uint64(wrote)
			if writeErr != nil {
				return int64(copied), writeErr
			}
			if wrote != read {
				return int64(copied), io.ErrShortWrite
			}
		}
		if readErr != nil {
			if readErr != io.EOF {
				return int64(copied), readErr
			}
			if copied != payloadSize {
				return int64(copied), fmt.Errorf("snapshots: state-domain-change accessor v4 group payload size %d, want %d", copied, payloadSize)
			}
			if groupIndex != len(offsets) {
				return int64(copied), fmt.Errorf("snapshots: state-domain-change accessor v4 patched groups %d, want %d", groupIndex, len(offsets))
			}
			return int64(copied), nil
		}
	}
}

type stateDomainChangeBinaryAccessorV4Collectors struct {
	exact      *etl.Collector
	group      *etl.Collector
	keyScratch []byte
}

func newStateDomainChangeBinaryAccessorV4Collectors(opts etl.Options) (*stateDomainChangeBinaryAccessorV4Collectors, error) {
	exact, err := etl.NewCollector(opts)
	if err != nil {
		return nil, fmt.Errorf("snapshots: create state-domain-change accessor v4 exact ETL collector: %w", err)
	}
	group, err := etl.NewCollector(opts)
	if err != nil {
		_ = exact.Close()
		return nil, fmt.Errorf("snapshots: create state-domain-change accessor v4 group ETL collector: %w", err)
	}
	return &stateDomainChangeBinaryAccessorV4Collectors{exact: exact, group: group}, nil
}

func (c *stateDomainChangeBinaryAccessorV4Collectors) Close() {
	if c == nil {
		return
	}
	_ = c.exact.Close()
	_ = c.group.Close()
}

func (c *stateDomainChangeBinaryAccessorV4Collectors) Collect(change *rawdb.StateDomainChange, offset, recordIndex uint64) error {
	if c == nil || c.exact == nil || c.group == nil {
		return errors.New("snapshots: nil state-domain-change accessor v4 collectors")
	}
	if change == nil {
		return errors.New("snapshots: nil state-domain-change accessor v4 change")
	}
	if recordIndex > math.MaxUint32 {
		return fmt.Errorf("snapshots: state-domain-change accessor v4 record index %d exceeds uint32", recordIndex)
	}
	c.keyScratch = appendStateDomainChangeBinaryAccessorLookupKey(c.keyScratch[:0], change.FlatDomain, change.Owner, change.Generation, change.Domain, change.Key)
	key := c.keyScratch
	hash := stateDomainChangeBinaryAccessorV3Hash(key)
	if err := c.exact.PutEncodedKeyAsValue(stateDomainChangeBinaryAccessorV3ExactEntrySize, func(exactValue []byte) {
		copy(exactValue[:stateDomainChangeBinaryAccessorV3HashSize], hash[:])
		binary.BigEndian.PutUint64(exactValue[stateDomainChangeBinaryAccessorV3HashSize:], offset)
		binary.BigEndian.PutUint32(exactValue[stateDomainChangeBinaryAccessorV3HashSize+8:], uint32(recordIndex))
	}); err != nil {
		return err
	}
	if groupKey, ok := stateDomainChangeBinaryAccessorV3GroupKey(change); ok {
		entry := stateDomainChangeBinaryAccessorEntry{key: key, txNum: change.TxNum, seq: change.Seq, offset: offset, recordIndex: recordIndex}
		sortKeySize := stateDomainChangeBinaryAccessorETLSortKeySize(entry)
		groupValueSize := stateDomainChangeBinaryAccessorV3GroupKeySize + stateDomainChangeBinaryAccessorV4GroupEntrySize
		if err := c.group.PutEncoded(sortKeySize, groupValueSize, func(sortKey, groupValue []byte) {
			putStateDomainChangeBinaryAccessorETLSortKey(sortKey, entry)
			copy(groupValue[:stateDomainChangeBinaryAccessorV3GroupKeySize], groupKey[:])
			binary.BigEndian.PutUint32(groupValue[stateDomainChangeBinaryAccessorV3GroupKeySize:stateDomainChangeBinaryAccessorV3GroupKeySize+4], stateDomainChangeBinaryAccessorV4LogicalPrefix(change.Key))
			binary.BigEndian.PutUint64(groupValue[stateDomainChangeBinaryAccessorV3GroupKeySize+4:stateDomainChangeBinaryAccessorV3GroupKeySize+12], offset)
			binary.BigEndian.PutUint32(groupValue[stateDomainChangeBinaryAccessorV3GroupKeySize+12:], uint32(recordIndex))
		}); err != nil {
			return err
		}
	}
	return nil
}

func buildStateDomainChangeBinaryAccessorV4FromHistorySegment(dir string, segmentRef, accessorRef SegmentRef, opts etl.Options) (SegmentRef, etl.Stats, error) {
	if opts.TempDir == "" {
		opts.TempDir = filepath.Join(dir, "etl")
	}
	collectors, err := newStateDomainChangeBinaryAccessorV4Collectors(opts)
	if err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	defer collectors.Close()

	segment, segmentSize, header, err := openHistorySegmentForSequentialRead(dir, segmentRef)
	if err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	defer segment.Close()
	if header.count > math.MaxUint32 {
		return SegmentRef{}, etl.Stats{}, fmt.Errorf("snapshots: state-domain-change accessor v4 count %d exceeds uint32 record index", header.count)
	}
	_, offset, err := stateDomainChangeBinaryTxRangeTableBoundsAt(segment, segmentSize, segmentRef, header)
	if err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	for recordIndex := uint64(0); recordIndex < header.count; recordIndex++ {
		change, next, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segment, offset, segmentSize, recordIndex)
		if err != nil {
			return SegmentRef{}, etl.Stats{}, err
		}
		if err := collectors.Collect(change, offset, recordIndex); err != nil {
			return SegmentRef{}, etl.Stats{}, err
		}
		offset = next
	}
	if offset != segmentSize {
		return SegmentRef{}, etl.Stats{}, fmt.Errorf("snapshots: state-domain-change accessor v4 source segment %q has %d trailing bytes", segmentRef.Path, segmentSize-offset)
	}
	return collectors.Build(dir, accessorRef, header.count)
}

func (c *stateDomainChangeBinaryAccessorV4Collectors) Build(dir string, accessorRef SegmentRef, recordCount uint64) (SegmentRef, etl.Stats, error) {
	if c == nil || c.exact == nil || c.group == nil {
		return SegmentRef{}, etl.Stats{}, errors.New("snapshots: nil state-domain-change accessor v4 collectors")
	}
	if recordCount > math.MaxUint32 {
		return SegmentRef{}, etl.Stats{}, fmt.Errorf("snapshots: state-domain-change accessor v4 count %d exceeds uint32 record index", recordCount)
	}
	accessorAbs := filepath.Join(dir, accessorRef.Path)
	accessorDir := filepath.Dir(accessorAbs)
	if err := os.MkdirAll(accessorDir, 0o755); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	accessorBase := filepath.Base(accessorAbs)
	groupPayloadTmp, groupPayloadName, err := createStateDomainChangeBinaryTempFileInDir(accessorDir, accessorBase+".groups")
	if err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	defer func() { _ = groupPayloadTmp.Close(); _ = os.Remove(groupPayloadName) }()
	groupOffsets := acquireStateDomainChangeAccessorGroupOffsets()
	defer releaseStateDomainChangeAccessorGroupOffsets(&groupOffsets)
	groupWriter := stateDomainChangeBinaryAccessorV4GroupETLWriter{
		payload: acquireStateDomainChangeHistoryWriter(groupPayloadTmp),
		offsets: groupOffsets,
	}
	defer groupWriter.Release()
	if _, err := c.group.Load(&groupWriter); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	if err := groupWriter.Finish(); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	if groupWriter.groups > recordCount || groupWriter.records > recordCount {
		return SegmentRef{}, etl.Stats{}, fmt.Errorf("snapshots: state-domain-change accessor v4 groups %d records %d exceed segment count %d", groupWriter.groups, groupWriter.records, recordCount)
	}
	if uint64(len(*groupOffsets)) != groupWriter.groups {
		return SegmentRef{}, etl.Stats{}, fmt.Errorf("snapshots: state-domain-change accessor v4 group offsets %d, want %d", len(*groupOffsets), groupWriter.groups)
	}

	// exact/group files are private assembly inputs and are never published.
	// Their buffered writers are flushed above; only the assembled accessor
	// needs fsync before its atomic rename, matching Erigon's temp-file policy.
	accessorTmp, accessorTmpName, err := createStateDomainChangeBinaryTempFileInDir(accessorDir, accessorBase)
	if err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	defer func() { _ = accessorTmp.Close(); _ = os.Remove(accessorTmpName) }()
	metadataWriter := newSnapshotMetadataWriter(accessorTmp)
	if err := writeStateDomainChangeBinaryHeaderToVersion(metadataWriter, stateDomainChangeBinaryAccessorMagic, accessorRef.FromTxNum, accessorRef.ToTxNum, recordCount, stateDomainChangeBinaryVersionV4); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	if err := writeStateDomainChangeBinaryTxRangeCount(metadataWriter, groupWriter.groups); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	exactWriter := stateDomainChangeBinaryAccessorV3ExactETLWriter{
		file:     acquireStateDomainChangeHistoryWriter(metadataWriter),
		expected: recordCount,
	}
	defer exactWriter.Release()
	exactStats, err := c.exact.Load(&exactWriter)
	if err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	if exactWriter.count != recordCount {
		return SegmentRef{}, etl.Stats{}, fmt.Errorf("snapshots: state-domain-change accessor v4 exact entries %d, want %d", exactWriter.count, recordCount)
	}
	if err := exactWriter.Finish(); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	tailWriter := acquireStateDomainChangeHistoryWriter(metadataWriter)
	defer releaseStateDomainChangeHistoryWriter(&tailWriter)
	payloadStart := uint64(stateDomainChangeBinaryHeaderSize+stateDomainChangeBinaryAccessorV3HeaderExtra) + recordCount*stateDomainChangeBinaryAccessorV3ExactEntrySize + groupWriter.groups*8
	var raw [8]byte
	for _, relative := range *groupOffsets {
		if relative > math.MaxUint64-payloadStart {
			return SegmentRef{}, etl.Stats{}, errors.New("snapshots: state-domain-change accessor v3 group payload offset overflows")
		}
		binary.BigEndian.PutUint64(raw[:], payloadStart+relative)
		if _, err := tailWriter.Write(raw[:]); err != nil {
			return SegmentRef{}, etl.Stats{}, err
		}
	}
	if _, err := groupPayloadTmp.Seek(0, io.SeekStart); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	if _, err := copyStateDomainChangeBinaryAccessorV4GroupPayload(tailWriter, groupPayloadTmp, *groupOffsets, groupWriter.payloadOffset); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	if err := tailWriter.Flush(); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	releaseStateDomainChangeHistoryWriter(&tailWriter)
	resultRef, err := finalizeStateDomainChangeHistoryFileWithMetadata(dir, accessorRef, accessorTmp, accessorTmpName, metadataWriter.Metadata(), false)
	if err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	return resultRef, exactStats, nil
}

// finalizeStateDomainChangeBinaryCompanions publishes the independently built
// txNum index and v4 accessor in parallel after the canonical history payload
// is durable. Both outputs remain derived from the same ordered record stream;
// callers remove either published sidecar if its sibling fails.
func finalizeStateDomainChangeBinaryCompanions(dir string, historyRef SegmentRef, indexTmp *os.File, indexTmpName string, collectors *stateDomainChangeBinaryAccessorV4Collectors, recordCount uint64) (indexRef, accessorRef SegmentRef, accessorStats etl.Stats, err error) {
	indexRef = SegmentRef{
		Dataset:          SegmentDatasetStateDomainChange,
		Kind:             SegmentInverted,
		FromTxNum:        historyRef.FromTxNum,
		ToTxNum:          historyRef.ToTxNum,
		AggregationSteps: historyRef.AggregationSteps,
		Path:             stateDomainChangeBinaryIndexPath(historyRef.Path),
	}
	accessorRef = SegmentRef{
		Dataset:          SegmentDatasetStateDomainChange,
		Kind:             SegmentAccessor,
		FromTxNum:        historyRef.FromTxNum,
		ToTxNum:          historyRef.ToTxNum,
		AggregationSteps: historyRef.AggregationSteps,
		Path:             stateDomainChangeBinaryAccessorPath(historyRef.Path),
	}

	var indexErr, accessorErr error
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		indexRef, indexErr = finalizeStateDomainChangeHistoryFile(dir, indexRef, indexTmp, indexTmpName, false)
	}()
	go func() {
		defer workers.Done()
		accessorRef, accessorStats, accessorErr = collectors.Build(dir, accessorRef, recordCount)
	}()
	workers.Wait()
	if indexErr != nil {
		return indexRef, accessorRef, accessorStats, indexErr
	}
	if accessorErr != nil {
		return indexRef, accessorRef, accessorStats, accessorErr
	}
	return indexRef, accessorRef, accessorStats, nil
}

// stateDomainChangeBinaryAccessorETLSortKey preserves the binary accessor's
// comparison order while appending a unique fixed-width suffix. NUL escaping
// keeps prefix keys ordered before their extensions, so Collector never merges
// distinct rows with the same logical accessor key.
func stateDomainChangeBinaryAccessorETLSortKeySize(entry stateDomainChangeBinaryAccessorEntry) int {
	escapedZeroes := 0
	for _, b := range entry.key {
		if b == 0 {
			escapedZeroes++
		}
	}
	return len(entry.key) + escapedZeroes + 34
}

func putStateDomainChangeBinaryAccessorETLSortKey(out []byte, entry stateDomainChangeBinaryAccessorEntry) {
	offset := 0
	for _, b := range entry.key {
		if b == 0 {
			out[offset], out[offset+1] = 0, 0xff
			offset += 2
			continue
		}
		out[offset] = b
		offset++
	}
	out[offset], out[offset+1] = 0, 0
	offset += 2
	binary.BigEndian.PutUint64(out[offset:offset+8], entry.txNum)
	offset += 8
	binary.BigEndian.PutUint64(out[offset:offset+8], entry.seq)
	offset += 8
	binary.BigEndian.PutUint64(out[offset:offset+8], entry.offset)
	offset += 8
	binary.BigEndian.PutUint64(out[offset:offset+8], entry.recordIndex)
}

type stateDomainChangeBinaryAccessorETLWriter struct {
	file          *os.File
	ref           SegmentRef
	expected      uint64
	count         uint64
	payloadOffset uint64
	previous      stateDomainChangeBinaryAccessorEntry
	havePrevious  bool
}

func (w *stateDomainChangeBinaryAccessorETLWriter) Put(_ []byte, value []byte) error {
	if w == nil || w.file == nil {
		return errors.New("snapshots: nil state-domain-change accessor ETL writer")
	}
	if w.count >= w.expected {
		return fmt.Errorf("snapshots: state-domain-change accessor emitted more than %d entries", w.expected)
	}
	entry, next, err := decodeStateDomainChangeBinaryAccessorEntryFrame(value, 0)
	if err != nil {
		return err
	}
	if next != uint64(len(value)) {
		return errors.New("snapshots: state-domain-change accessor ETL value has trailing bytes")
	}
	if err := validateStateDomainChangeBinaryAccessorEntry(w.ref, entry, w.count); err != nil {
		return err
	}
	if w.havePrevious && compareStateDomainChangeBinaryAccessorEntry(w.previous, entry) >= 0 {
		return errors.New("snapshots: state-domain-change accessor ETL rows are not strictly ordered")
	}
	if err := writeStateDomainChangeBinaryAccessorOffsetAt(w.file, w.count, w.payloadOffset); err != nil {
		return err
	}
	if _, err := w.file.Write(value); err != nil {
		return err
	}
	if uint64(len(value)) > math.MaxUint64-w.payloadOffset {
		return errors.New("snapshots: state-domain-change accessor payload offset overflows")
	}
	w.payloadOffset += uint64(len(value))
	w.previous = entry
	w.havePrevious = true
	w.count++
	return nil
}

func (*stateDomainChangeBinaryAccessorETLWriter) Delete([]byte) error {
	return errors.New("snapshots: state-domain-change accessor ETL writer does not support deletes")
}

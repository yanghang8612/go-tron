package snapshots

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
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
	keyETL      etl.Stats
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
	v6Build, err := newStateDomainChangeV6Build(opts, dir, ref.Path)
	if err != nil {
		return result, err
	}
	defer v6Build.Close()
	if err := iterateStateDomainChangeHistoryChanges(db, cfg, ref.FromTxNum, ref.ToTxNum, blockRange, func(change *rawdb.StateDomainChange) (bool, error) {
		if change == nil {
			return false, errors.New("snapshots: nil state-domain-change history key")
		}
		return true, v6Build.CollectKey(change)
	}); err != nil {
		return result, fmt.Errorf("snapshots: collect V6 state-domain history keys: %w", err)
	}
	if err := v6Build.FinishDictionary(); err != nil {
		return result, err
	}
	result.keyETL = v6Build.keyStats
	segmentTmp, err := createStateDomainChangeHistoryTemp(dir, ref.Path, CompressHistorySegments)
	if err != nil {
		return result, err
	}
	defer segmentTmp.Close()
	if err := writeStateDomainChangeBinaryHeaderToVersion(segmentTmp, stateDomainChangeBinarySegmentMagic, ref.FromTxNum, ref.ToTxNum, 0, stateDomainChangeBinaryVersionV6); err != nil {
		return result, err
	}
	if err := writeStateDomainChangeBinaryV6DictionaryCommitment(segmentTmp, v6Build.dictionaryDigest); err != nil {
		return result, err
	}
	txRangeCount, err := writeStateDomainChangeBinaryTxRangeTableFromDB(segmentTmp, db, cfg, ref.FromTxNum, ref.ToTxNum, blockRange)
	if err != nil {
		return result, err
	}
	recordOffset := stateDomainChangeBinaryTxRangeTableStart(stateDomainChangeBinaryVersionV6) + 8 + txRangeCount*stateDomainChangeBinaryTxRangeSize

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
	recordWriter := newStateDomainChangeHistoryRecordWriterV6(segmentTmp, indexTmp, v6Build, ref, math.MaxUint64, recordOffset)
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
		if err := v6Build.ResetPostings(); err != nil {
			return result, err
		}
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
		if err := writeStateDomainChangeBinaryHeaderToVersion(segmentTmp, stateDomainChangeBinarySegmentMagic, ref.FromTxNum, ref.ToTxNum, recordCount, stateDomainChangeBinaryVersionV6); err != nil {
			return result, err
		}
		if err := writeStateDomainChangeBinaryV6DictionaryCommitment(segmentTmp, v6Build.dictionaryDigest); err != nil {
			return result, err
		}
		txRangeCount, err = writeStateDomainChangeBinaryTxRangeTableFromDB(segmentTmp, db, cfg, ref.FromTxNum, ref.ToTxNum, blockRange)
		if err != nil {
			return result, err
		}
		recordOffset = stateDomainChangeBinaryTxRangeTableStart(stateDomainChangeBinaryVersionV6) + 8 + txRangeCount*stateDomainChangeBinaryTxRangeSize
		if err := writeStateDomainChangeBinaryHeaderTo(indexTmp, stateDomainChangeBinaryIndexMagic, ref.FromTxNum, ref.ToTxNum, 0); err != nil {
			return result, err
		}
		recordWriter = newStateDomainChangeHistoryRecordWriterV6(segmentTmp, indexTmp, v6Build, ref, recordCount, recordOffset)
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
	indexTmp, indexTmpName, err = rewriteStateDomainChangeBinaryIndexV7(indexTmp, indexTmpName)
	if err != nil {
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
	indexRef, accessorRef, result.accessorETL, err = finalizeStateDomainChangeBinaryCompanionsV6(dir, segmentRef, indexTmp, indexTmpName, v6Build, recordCount)
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
	if accessorHeader.version == stateDomainChangeBinaryVersionV6 || accessorHeader.version == stateDomainChangeBinaryVersionV7 {
		var v6Header stateDomainChangeBinaryAccessorV6Header
		v6Header, err = decodeStateDomainChangeBinaryAccessorKeyHeader(accessor, accessorSize)
		if err == nil {
			var history historySegmentReader
			history, _, _, err = openHistorySegmentForRead(dir, segmentRef)
			if err == nil {
				var segmentDigest [stateDomainChangeBinaryV6DictionaryCommitmentSize]byte
				segmentDigest, err = readStateDomainChangeBinaryV6DictionaryCommitment(history)
				_ = history.Close()
				if err == nil && segmentDigest != v6Header.dictionaryDigest {
					err = errors.New("snapshots: V6 history/accessor dictionary commitment mismatch")
				}
			}
		}
		if err == nil {
			if accessorHeader.version == stateDomainChangeBinaryVersionV7 {
				err = checkStateDomainChangeBinaryAccessorV7(accessor, accessorSize)
			} else {
				err = checkStateDomainChangeBinaryAccessorV6(accessor, accessorSize)
			}
		}
	} else if accessorHeader.version == stateDomainChangeBinaryVersionV5 {
		var layout stateDomainChangeBinaryAccessorV3Layout
		layout, err = stateDomainChangeBinaryAccessorV5LayoutAt(accessor, accessorSize, accessorHeader)
		if err == nil {
			if layout.groupCount == 0 {
				if accessorSize != layout.groupPayloadStart {
					err = fmt.Errorf("snapshots: state-domain-change history accessor has %d trailing bytes", accessorSize-layout.groupPayloadStart)
				}
			} else {
				_, err = readStateDomainChangeBinaryAccessorV5GroupMetaAt(accessor, layout, layout.groupCount-1, accessorSize)
			}
		}
	} else {
		err = fmt.Errorf("snapshots: unsupported built state-domain-change accessor version %d", accessorHeader.version)
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
	if _, err := file.WriteAt(countRaw[:], int64(stateDomainChangeBinaryTxRangeTableStart(stateDomainChangeBinaryVersionV6))); err != nil {
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

func appendStateDomainChangeBinaryRecordFrameV6(dst []byte, change *rawdb.StateDomainChange, keyID uint32) ([]byte, error) {
	payloadSize, err := stateDomainChangeRecordV6PayloadSize(change)
	if err != nil {
		return nil, err
	}
	frameSize := 4 + payloadSize
	start := len(dst)
	if cap(dst)-start < frameSize {
		capacity := max(cap(dst)*2, start+frameSize)
		grown := make([]byte, start, capacity)
		copy(grown, dst)
		dst = grown
	}
	dst = dst[:start+frameSize]
	binary.BigEndian.PutUint32(dst[start:start+4], uint32(payloadSize))
	putStateDomainChangeRecordV6(dst[start+4:start+frameSize], change, keyID)
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
	segment        *bufio.Writer
	index          *bufio.Writer
	accessors      *stateDomainChangeBinaryAccessorV4Collectors
	v6             *stateDomainChangeV6Build
	ref            SegmentRef
	expected       uint64
	count          uint64
	segmentOff     uint64
	indexWritten   uint64
	currentIndex   stateDomainChangeBinaryTxOffset
	indexScratch   [stateDomainChangeBinaryIndexEntrySize]byte
	haveIndex      bool
	previous       *rawdb.StateDomainChange
	previousV5Tx   uint64
	previousV5Seq  uint64
	havePreviousV5 bool
	recordScratch  []byte
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

func newStateDomainChangeHistoryRecordWriterV6(segment io.Writer, index *os.File, build *stateDomainChangeV6Build, ref SegmentRef, expected, segmentOff uint64) *stateDomainChangeHistoryRecordWriter {
	w := newStateDomainChangeHistoryRecordWriter(segment, index, nil, ref, expected, segmentOff)
	w.v6 = build
	return w
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

// WriteBorrowedV5Change consumes a decoded v5 row whose variable-width fields
// borrow a sequential decompressor cache. V5 derives a unique Seq for every
// record, so the leading (TxNum, Seq) pair is a complete adjacent-order check;
// retaining Key/Prev views past this call is unnecessary.
func (w *stateDomainChangeHistoryRecordWriter) WriteBorrowedV5Change(change *rawdb.StateDomainChange) error {
	if w == nil {
		return errors.New("snapshots: nil borrowed v5 state-domain-change history writer")
	}
	if change == nil {
		return errors.New("snapshots: nil borrowed v5 state-domain-change history record")
	}
	if w.previous != nil {
		if compareStateDomainChangeForBinary(w.previous, change) > 0 {
			return errStateDomainChangeHistoryRecordsNotOrdered
		}
	} else if w.havePreviousV5 && (change.TxNum < w.previousV5Tx || change.TxNum == w.previousV5Tx && change.Seq <= w.previousV5Seq) {
		return errStateDomainChangeHistoryRecordsNotOrdered
	}
	if err := w.writeChange(change, true); err != nil {
		return err
	}
	w.previousV5Tx = change.TxNum
	w.previousV5Seq = change.Seq
	w.havePreviousV5 = true
	return nil
}

// WriteBorrowedV6Change emits a decoded V6 row with an already-remapped target
// dictionary ID. The source dictionary is immutable and sorted, so compaction
// can build this mapping once per source and avoid resolving/decompressing its
// logical key for every record in transaction order.
func (w *stateDomainChangeHistoryRecordWriter) WriteBorrowedV6Change(change *rawdb.StateDomainChange, targetKeyID uint32) error {
	if w == nil {
		return errors.New("snapshots: nil borrowed v6 state-domain-change history writer")
	}
	if change == nil {
		return errors.New("snapshots: nil borrowed v6 state-domain-change history record")
	}
	if w.previous != nil {
		if compareStateDomainChangeForBinary(w.previous, change) > 0 {
			return errStateDomainChangeHistoryRecordsNotOrdered
		}
	} else if w.havePreviousV5 && (change.TxNum < w.previousV5Tx || change.TxNum == w.previousV5Tx && change.Seq <= w.previousV5Seq) {
		return errStateDomainChangeHistoryRecordsNotOrdered
	}
	if err := w.writeChangeWithV6KeyID(change, true, &targetKeyID); err != nil {
		return err
	}
	w.previousV5Tx = change.TxNum
	w.previousV5Seq = change.Seq
	w.havePreviousV5 = true
	return nil
}

func (w *stateDomainChangeHistoryRecordWriter) writeChange(change *rawdb.StateDomainChange, trustedOrder bool) error {
	return w.writeChangeWithV6KeyID(change, trustedOrder, nil)
}

func (w *stateDomainChangeHistoryRecordWriter) writeChangeWithV6KeyID(change *rawdb.StateDomainChange, trustedOrder bool, mappedKeyID *uint32) error {
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
	if !trustedOrder {
		if w.previous != nil && compareStateDomainChangeForBinary(w.previous, change) > 0 {
			return errStateDomainChangeHistoryRecordsNotOrdered
		}
		if w.previous == nil && w.havePreviousV5 && (change.TxNum < w.previousV5Tx || change.TxNum == w.previousV5Tx && change.Seq <= w.previousV5Seq) {
			return errStateDomainChangeHistoryRecordsNotOrdered
		}
	}
	if w.accessors != nil {
		if err := w.accessors.Collect(change, w.segmentOff, w.count); err != nil {
			return err
		}
	}
	var v6KeyID uint32
	if w.v6 != nil {
		if mappedKeyID != nil {
			if *mappedKeyID >= w.v6.keyCount {
				return errors.New("snapshots: remapped V6 key id outside target dictionary")
			}
			v6KeyID = *mappedKeyID
		} else {
			w.v6.keyScratch = appendStateDomainChangeBinaryAccessorLookupKey(w.v6.keyScratch[:0], change.FlatDomain, change.Owner, change.Generation, change.Domain, change.Key)
			keyID, err := w.v6.KeyID(w.v6.keyScratch)
			if err != nil {
				return err
			}
			v6KeyID = keyID
		}
		if err := w.v6.CollectPosting(v6KeyID, change.TxNum, w.segmentOff, w.count); err != nil {
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

	var frame []byte
	var err error
	if w.v6 != nil {
		frame, err = appendStateDomainChangeBinaryRecordFrameV6(w.recordScratch[:0], change, v6KeyID)
	} else {
		frame, err = appendStateDomainChangeBinaryRecordFrame(w.recordScratch[:0], change)
	}
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
		w.havePreviousV5 = false
	} else {
		w.previous = change
		w.havePreviousV5 = false
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

type stateDomainChangeBinaryAccessorV5ExactETLWriter struct {
	file     *bufio.Writer
	expected uint64
	count    uint64
	previous stateDomainChangeBinaryAccessorV5ExactEntry
	havePrev bool
}

func (w *stateDomainChangeBinaryAccessorV5ExactETLWriter) Put(_ []byte, value []byte) error {
	if w == nil || w.file == nil {
		return errors.New("snapshots: nil state-domain-change accessor v5 exact ETL writer")
	}
	if len(value) != stateDomainChangeBinaryAccessorV5ExactEntrySize {
		return fmt.Errorf("snapshots: state-domain-change accessor v5 exact value size %d, want %d", len(value), stateDomainChangeBinaryAccessorV5ExactEntrySize)
	}
	if w.count >= w.expected {
		return fmt.Errorf("snapshots: state-domain-change accessor v5 exact emitted more than %d entries", w.expected)
	}
	var entry stateDomainChangeBinaryAccessorV5ExactEntry
	copy(entry.fingerprint[:], value[:stateDomainChangeBinaryAccessorV5FingerprintSize])
	offset, err := stateDomainChangeBinaryAccessorV5Offset(value[stateDomainChangeBinaryAccessorV5FingerprintSize:])
	if err != nil {
		return err
	}
	entry.offset = offset
	entry.recordIndex = binary.BigEndian.Uint32(value[stateDomainChangeBinaryAccessorV5FingerprintSize+stateDomainChangeBinaryAccessorV5OffsetSize:])
	if entry.offset < stateDomainChangeBinaryHeaderSize {
		return fmt.Errorf("snapshots: state-domain-change accessor v5 exact record offset %d is invalid", entry.offset)
	}
	if w.havePrev {
		if cmp := bytes.Compare(w.previous.fingerprint[:], entry.fingerprint[:]); cmp > 0 ||
			(cmp == 0 && (w.previous.offset > entry.offset || (w.previous.offset == entry.offset && w.previous.recordIndex >= entry.recordIndex))) {
			return errors.New("snapshots: state-domain-change accessor v5 exact ETL rows are not strictly ordered")
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

func (*stateDomainChangeBinaryAccessorV5ExactETLWriter) Delete([]byte) error {
	return errors.New("snapshots: state-domain-change accessor v5 exact ETL writer does not support deletes")
}

func (w *stateDomainChangeBinaryAccessorV5ExactETLWriter) Finish() error {
	if w == nil || w.file == nil {
		return errors.New("snapshots: nil state-domain-change accessor v5 exact ETL writer")
	}
	defer w.Release()
	return w.file.Flush()
}

func (w *stateDomainChangeBinaryAccessorV5ExactETLWriter) Release() {
	if w == nil {
		return
	}
	releaseStateDomainChangeHistoryWriter(&w.file)
}

type stateDomainChangeBinaryAccessorV4GroupETLWriter struct {
	payload       *bufio.Writer
	offsets       *[]uint64
	offsetWriter  *bufio.Writer
	payloadOffset uint64
	groups        uint64
	records       uint64
	haveGroup     bool
	currentKey    [stateDomainChangeBinaryAccessorV3GroupKeySize]byte
	currentCount  uint64
}

func (w *stateDomainChangeBinaryAccessorV4GroupETLWriter) Put(_ []byte, value []byte) error {
	if w == nil || w.payload == nil || (w.offsets == nil && w.offsetWriter == nil) {
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
		if w.offsets != nil {
			*w.offsets = append(*w.offsets, w.payloadOffset)
		}
		if w.offsetWriter != nil {
			var raw [8]byte
			binary.BigEndian.PutUint64(raw[:], w.payloadOffset)
			if _, err := w.offsetWriter.Write(raw[:]); err != nil {
				return err
			}
		}
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
	if w == nil || w.payload == nil || (w.offsets == nil && w.offsetWriter == nil) {
		return errors.New("snapshots: nil state-domain-change accessor v4 group ETL writer")
	}
	defer w.Release()
	if err := w.finishGroup(); err != nil {
		return err
	}
	if err := w.payload.Flush(); err != nil {
		return err
	}
	if w.offsetWriter != nil {
		if err := w.offsetWriter.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (w *stateDomainChangeBinaryAccessorV4GroupETLWriter) Release() {
	if w == nil {
		return
	}
	releaseStateDomainChangeHistoryWriter(&w.payload)
	releaseStateDomainChangeHistoryWriter(&w.offsetWriter)
}

type stateDomainChangeBinaryAccessorV5GroupETLWriter struct {
	payload       *bufio.Writer
	offsets       *[]uint64
	offsetWriter  *bufio.Writer
	payloadOffset uint64
	groups        uint64
	records       uint64
	haveGroup     bool
	currentKey    [stateDomainChangeBinaryAccessorV3GroupKeySize]byte
	currentCount  uint64
}

func (w *stateDomainChangeBinaryAccessorV5GroupETLWriter) Put(_ []byte, value []byte) error {
	if w == nil || w.payload == nil || (w.offsets == nil && w.offsetWriter == nil) {
		return errors.New("snapshots: nil state-domain-change accessor v5 group ETL writer")
	}
	if len(value) != stateDomainChangeBinaryAccessorV3GroupKeySize+stateDomainChangeBinaryAccessorV5GroupEntrySize {
		return fmt.Errorf("snapshots: state-domain-change accessor v5 group value size %d, want %d", len(value), stateDomainChangeBinaryAccessorV3GroupKeySize+stateDomainChangeBinaryAccessorV5GroupEntrySize)
	}
	key := value[:stateDomainChangeBinaryAccessorV3GroupKeySize]
	offset, err := stateDomainChangeBinaryAccessorV5Offset(value[stateDomainChangeBinaryAccessorV3GroupKeySize+4:])
	if err != nil {
		return err
	}
	if offset < stateDomainChangeBinaryHeaderSize {
		return fmt.Errorf("snapshots: state-domain-change accessor v5 group record offset %d is invalid", offset)
	}
	if !w.haveGroup || !bytes.Equal(w.currentKey[:], key) {
		if err := w.finishGroup(); err != nil {
			return err
		}
		if w.offsets != nil {
			*w.offsets = append(*w.offsets, w.payloadOffset)
		}
		if w.offsetWriter != nil {
			var raw [8]byte
			binary.BigEndian.PutUint64(raw[:], w.payloadOffset)
			if _, err := w.offsetWriter.Write(raw[:]); err != nil {
				return err
			}
		}
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
	w.payloadOffset += stateDomainChangeBinaryAccessorV5GroupEntrySize
	w.currentCount++
	w.records++
	return nil
}

func (w *stateDomainChangeBinaryAccessorV5GroupETLWriter) finishGroup() error {
	if w == nil || !w.haveGroup {
		return nil
	}
	if w.currentCount == 0 {
		return errors.New("snapshots: state-domain-change accessor v5 group has no records")
	}
	w.haveGroup = false
	return nil
}

func (*stateDomainChangeBinaryAccessorV5GroupETLWriter) Delete([]byte) error {
	return errors.New("snapshots: state-domain-change accessor v5 group ETL writer does not support deletes")
}

func (w *stateDomainChangeBinaryAccessorV5GroupETLWriter) Finish() error {
	if w == nil || w.payload == nil || (w.offsets == nil && w.offsetWriter == nil) {
		return errors.New("snapshots: nil state-domain-change accessor v5 group ETL writer")
	}
	defer w.Release()
	if err := w.finishGroup(); err != nil {
		return err
	}
	if err := w.payload.Flush(); err != nil {
		return err
	}
	if w.offsetWriter != nil {
		if err := w.offsetWriter.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (w *stateDomainChangeBinaryAccessorV5GroupETLWriter) Release() {
	if w == nil {
		return
	}
	releaseStateDomainChangeHistoryWriter(&w.payload)
	releaseStateDomainChangeHistoryWriter(&w.offsetWriter)
}

func writeStateDomainChangeBinaryAccessorV4GroupDirectory(ctx context.Context, dst io.Writer, offsets io.Reader, groupCount, payloadStart, payloadSize uint64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var raw [8]byte
	var previous uint64
	for i := uint64(0); i < groupCount; i++ {
		if i&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if _, err := io.ReadFull(offsets, raw[:]); err != nil {
			return fmt.Errorf("snapshots: read state-domain-change accessor v4 group directory offset %d: %w", i, err)
		}
		relative := binary.BigEndian.Uint64(raw[:])
		if (i == 0 && relative != 0) || (i > 0 && relative <= previous) || relative >= payloadSize {
			return fmt.Errorf("snapshots: state-domain-change accessor v4 group directory offset %d is invalid: %d", i, relative)
		}
		if relative > math.MaxUint64-payloadStart {
			return errors.New("snapshots: state-domain-change accessor v4 group payload offset overflows")
		}
		previous = relative
		binary.BigEndian.PutUint64(raw[:], payloadStart+relative)
		if _, err := dst.Write(raw[:]); err != nil {
			return err
		}
	}
	if groupCount == 0 && payloadSize != 0 {
		return fmt.Errorf("snapshots: state-domain-change accessor v4 group payload has size %d without groups", payloadSize)
	}
	return nil
}

// copyStateDomainChangeBinaryAccessorV4GroupPayload turns the buffered count
// placeholders into their fixed-width values while the private group file is
// copied into the final accessor. The temp file and final output therefore
// remain sequential; no group-sized memory buffer or random pwrite is needed.
func copyStateDomainChangeBinaryAccessorV4GroupPayload(dst io.Writer, src io.Reader, offsets []uint64, payloadSize uint64) (int64, error) {
	var encoded bytes.Buffer
	var raw [8]byte
	for _, offset := range offsets {
		binary.BigEndian.PutUint64(raw[:], offset)
		_, _ = encoded.Write(raw[:])
	}
	return copyStateDomainChangeBinaryAccessorV4GroupPayloadFromOffsets(dst, src, bytes.NewReader(encoded.Bytes()), uint64(len(offsets)), payloadSize)
}

// copyStateDomainChangeBinaryAccessorV4GroupPayloadFromOffsets patches group
// counts from a sequential offset stream. Keeping offsets on disk prevents a
// worst-case one-offset-per-record history from creating another linear heap
// allocation during build or semantic verification.
func copyStateDomainChangeBinaryAccessorV4GroupPayloadFromOffsets(dst io.Writer, src, offsets io.Reader, groupCount, payloadSize uint64) (int64, error) {
	return copyStateDomainChangeBinaryAccessorGroupPayloadFromOffsets(dst, src, offsets, groupCount, payloadSize, stateDomainChangeBinaryAccessorV4GroupEntrySize, "v4")
}

func copyStateDomainChangeBinaryAccessorV5GroupPayloadFromOffsets(dst io.Writer, src, offsets io.Reader, groupCount, payloadSize uint64) (int64, error) {
	return copyStateDomainChangeBinaryAccessorGroupPayloadFromOffsets(dst, src, offsets, groupCount, payloadSize, stateDomainChangeBinaryAccessorV5GroupEntrySize, "v5")
}

func copyStateDomainChangeBinaryAccessorGroupPayloadFromOffsets(dst io.Writer, src, offsets io.Reader, groupCount, payloadSize, groupEntrySize uint64, version string) (int64, error) {
	if payloadSize > math.MaxInt64 {
		return 0, fmt.Errorf("snapshots: state-domain-change accessor %s group payload size %d exceeds int64", version, payloadSize)
	}
	if groupCount == 0 {
		if payloadSize != 0 {
			return 0, fmt.Errorf("snapshots: state-domain-change accessor %s group payload has size %d without groups", version, payloadSize)
		}
		return 0, nil
	}
	readOffset := func() (uint64, error) {
		var raw [8]byte
		if _, err := io.ReadFull(offsets, raw[:]); err != nil {
			return 0, err
		}
		return binary.BigEndian.Uint64(raw[:]), nil
	}
	current, err := readOffset()
	if err != nil {
		return 0, fmt.Errorf("snapshots: read state-domain-change accessor %s first group offset: %w", version, err)
	}
	if current != 0 {
		return 0, fmt.Errorf("snapshots: state-domain-change accessor %s first group offset %d, want 0", version, current)
	}
	var next uint64
	if groupCount > 1 {
		next, err = readOffset()
		if err != nil {
			return 0, fmt.Errorf("snapshots: read state-domain-change accessor %s group offset 1: %w", version, err)
		}
	} else {
		next = payloadSize
	}
	validateRange := func(index, start, end uint64) error {
		entriesStart := start + stateDomainChangeBinaryAccessorV3GroupKeySize + 8
		if entriesStart < start || end < entriesStart || end > payloadSize {
			return fmt.Errorf("snapshots: state-domain-change accessor %s group %d has invalid payload range [%d,%d)", version, index, start, end)
		}
		entriesSize := end - entriesStart
		if entriesSize == 0 || entriesSize%groupEntrySize != 0 {
			return fmt.Errorf("snapshots: state-domain-change accessor %s group %d payload size %d is invalid", version, index, entriesSize)
		}
		return nil
	}
	if err := validateRange(0, current, next); err != nil {
		return 0, err
	}
	var advanceGroup func() error

	buffer := stateDomainChangeHistoryCopyBufferPool.Get().(*stateDomainChangeHistoryCopyBuffer)
	defer stateDomainChangeHistoryCopyBufferPool.Put(buffer)
	var (
		copied     uint64
		groupIndex uint64
		rawCount   [8]byte
	)
	advanceGroup = func() error {
		groupIndex++
		if groupIndex >= groupCount {
			return nil
		}
		current = next
		if groupIndex+1 < groupCount {
			var offsetErr error
			next, offsetErr = readOffset()
			if offsetErr != nil {
				return fmt.Errorf("snapshots: read state-domain-change accessor %s group offset %d: %w", version, groupIndex+1, offsetErr)
			}
		} else {
			next = payloadSize
		}
		return validateRange(groupIndex, current, next)
	}
	for {
		read, readErr := src.Read(buffer[:])
		if read > 0 {
			if copied > payloadSize || uint64(read) > payloadSize-copied {
				return int64(copied), fmt.Errorf("snapshots: state-domain-change accessor %s group payload exceeds size %d", version, payloadSize)
			}
			chunkEnd := copied + uint64(read)
			for groupIndex < groupCount {
				countStart := current + stateDomainChangeBinaryAccessorV3GroupKeySize
				countEnd := countStart + 8
				if countStart >= chunkEnd {
					break
				}
				if countEnd <= copied {
					if err := advanceGroup(); err != nil {
						return int64(copied), err
					}
					continue
				}
				entriesStart := countEnd
				binary.BigEndian.PutUint64(rawCount[:], (next-entriesStart)/groupEntrySize)
				patchStart := max(copied, countStart)
				patchEnd := min(chunkEnd, countEnd)
				copy(buffer[patchStart-copied:patchEnd-copied], rawCount[patchStart-countStart:patchEnd-countStart])
				if patchEnd < countEnd {
					break
				}
				if err := advanceGroup(); err != nil {
					return int64(copied), err
				}
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
				return int64(copied), fmt.Errorf("snapshots: state-domain-change accessor %s group payload size %d, want %d", version, copied, payloadSize)
			}
			if groupIndex != groupCount {
				return int64(copied), fmt.Errorf("snapshots: state-domain-change accessor %s patched groups %d, want %d", version, groupIndex, groupCount)
			}
			return int64(copied), nil
		}
	}
}

type stateDomainChangeBinaryAccessorV4Collectors struct {
	exact      *etl.Collector
	group      *etl.Collector
	keyScratch []byte
	version    uint32
}

func newStateDomainChangeBinaryAccessorV4Collectors(opts etl.Options) (*stateDomainChangeBinaryAccessorV4Collectors, error) {
	return newStateDomainChangeBinaryAccessorCollectors(opts, stateDomainChangeBinaryVersionV4)
}

func newStateDomainChangeBinaryAccessorV5Collectors(opts etl.Options) (*stateDomainChangeBinaryAccessorV4Collectors, error) {
	return newStateDomainChangeBinaryAccessorCollectors(opts, stateDomainChangeBinaryVersionV5)
}

func newStateDomainChangeBinaryAccessorCollectors(opts etl.Options, version uint32) (*stateDomainChangeBinaryAccessorV4Collectors, error) {
	if version != stateDomainChangeBinaryVersionV4 && version != stateDomainChangeBinaryVersionV5 {
		return nil, fmt.Errorf("snapshots: unsupported state-domain-change accessor collector version %d", version)
	}
	exact, err := etl.NewCollector(opts)
	if err != nil {
		return nil, fmt.Errorf("snapshots: create state-domain-change accessor v%d exact ETL collector: %w", version, err)
	}
	group, err := etl.NewCollector(opts)
	if err != nil {
		_ = exact.Close()
		return nil, fmt.Errorf("snapshots: create state-domain-change accessor v%d group ETL collector: %w", version, err)
	}
	return &stateDomainChangeBinaryAccessorV4Collectors{exact: exact, group: group, version: version}, nil
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
	if c.version == stateDomainChangeBinaryVersionV5 {
		if offset > stateDomainChangeBinaryAccessorV5MaxOffset {
			return fmt.Errorf("snapshots: state-domain-change accessor v5 offset %d exceeds 48 bits", offset)
		}
		fingerprint := stateDomainChangeBinaryAccessorV5Fingerprint(key)
		if err := c.exact.PutEncodedKeyAsValue(stateDomainChangeBinaryAccessorV5ExactEntrySize, func(exactValue []byte) {
			copy(exactValue[:stateDomainChangeBinaryAccessorV5FingerprintSize], fingerprint[:])
			_ = putStateDomainChangeBinaryAccessorV5Offset(exactValue[stateDomainChangeBinaryAccessorV5FingerprintSize:], offset)
			binary.BigEndian.PutUint32(exactValue[stateDomainChangeBinaryAccessorV5FingerprintSize+stateDomainChangeBinaryAccessorV5OffsetSize:], uint32(recordIndex))
		}); err != nil {
			return err
		}
	} else {
		hash := stateDomainChangeBinaryAccessorV3Hash(key)
		if err := c.exact.PutEncodedKeyAsValue(stateDomainChangeBinaryAccessorV3ExactEntrySize, func(exactValue []byte) {
			copy(exactValue[:stateDomainChangeBinaryAccessorV3HashSize], hash[:])
			binary.BigEndian.PutUint64(exactValue[stateDomainChangeBinaryAccessorV3HashSize:], offset)
			binary.BigEndian.PutUint32(exactValue[stateDomainChangeBinaryAccessorV3HashSize+8:], uint32(recordIndex))
		}); err != nil {
			return err
		}
	}
	if groupKey, ok := stateDomainChangeBinaryAccessorV3GroupKey(change); ok {
		entry := stateDomainChangeBinaryAccessorEntry{key: key, txNum: change.TxNum, seq: change.Seq, offset: offset, recordIndex: recordIndex}
		sortKeySize := stateDomainChangeBinaryAccessorETLSortKeySize(entry)
		groupEntrySize := stateDomainChangeBinaryAccessorV4GroupEntrySize
		if c.version == stateDomainChangeBinaryVersionV5 {
			groupEntrySize = stateDomainChangeBinaryAccessorV5GroupEntrySize
		}
		groupValueSize := stateDomainChangeBinaryAccessorV3GroupKeySize + groupEntrySize
		if err := c.group.PutEncoded(sortKeySize, groupValueSize, func(sortKey, groupValue []byte) {
			putStateDomainChangeBinaryAccessorETLSortKey(sortKey, entry)
			copy(groupValue[:stateDomainChangeBinaryAccessorV3GroupKeySize], groupKey[:])
			binary.BigEndian.PutUint32(groupValue[stateDomainChangeBinaryAccessorV3GroupKeySize:stateDomainChangeBinaryAccessorV3GroupKeySize+4], stateDomainChangeBinaryAccessorV4LogicalPrefix(change.Key))
			if c.version == stateDomainChangeBinaryVersionV5 {
				_ = putStateDomainChangeBinaryAccessorV5Offset(groupValue[stateDomainChangeBinaryAccessorV3GroupKeySize+4:], offset)
				binary.BigEndian.PutUint32(groupValue[stateDomainChangeBinaryAccessorV3GroupKeySize+4+stateDomainChangeBinaryAccessorV5OffsetSize:], uint32(recordIndex))
			} else {
				binary.BigEndian.PutUint64(groupValue[stateDomainChangeBinaryAccessorV3GroupKeySize+4:stateDomainChangeBinaryAccessorV3GroupKeySize+12], offset)
				binary.BigEndian.PutUint32(groupValue[stateDomainChangeBinaryAccessorV3GroupKeySize+12:], uint32(recordIndex))
			}
		}); err != nil {
			return err
		}
	}
	return nil
}

func buildStateDomainChangeBinaryAccessorFromHistorySegment(dir string, segmentRef, accessorRef SegmentRef, opts etl.Options) (SegmentRef, etl.Stats, error) {
	if opts.TempDir == "" {
		opts.TempDir = filepath.Join(dir, "etl")
	}
	collectors, err := newStateDomainChangeBinaryAccessorV5Collectors(opts)
	if err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	defer collectors.Close()

	segment, segmentSize, header, err := openHistorySegmentForSequentialRead(dir, segmentRef)
	if err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	defer segment.Close()
	if header.version == stateDomainChangeBinaryVersionV6 {
		_ = segment.Close()
		return buildStateDomainChangeBinaryAccessorV6FromHistorySegment(dir, segmentRef, accessorRef, opts, header.count)
	}
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

func buildStateDomainChangeBinaryAccessorV6FromHistorySegment(dir string, segmentRef, accessorRef SegmentRef, opts etl.Options, recordCount uint64) (SegmentRef, etl.Stats, error) {
	build, err := newStateDomainChangeV6Build(opts, dir, accessorRef.Path+".rebuild")
	if err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	defer build.Close()
	collect := func(keys bool) error {
		segment, size, header, err := openHistorySegmentForSequentialRead(dir, segmentRef)
		if err != nil {
			return err
		}
		defer segment.Close()
		_, offset, err := stateDomainChangeBinaryTxRangeTableBoundsAt(segment, size, segmentRef, header)
		if err != nil {
			return err
		}
		for i := uint64(0); i < header.count; i++ {
			change, next, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segment, offset, size, i)
			if err != nil {
				return err
			}
			if keys {
				err = build.CollectKey(change)
			} else {
				build.keyScratch = appendStateDomainChangeBinaryAccessorLookupKey(build.keyScratch[:0], change.FlatDomain, change.Owner, change.Generation, change.Domain, change.Key)
				var id uint32
				id, err = build.KeyID(build.keyScratch)
				if err == nil {
					err = build.CollectPosting(id, change.TxNum, offset, i)
				}
			}
			if err != nil {
				return err
			}
			offset = next
		}
		if offset != size {
			return fmt.Errorf("snapshots: V6 accessor rebuild source has %d trailing bytes", size-offset)
		}
		return nil
	}
	if err := collect(true); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	if err := build.FinishDictionary(); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	if err := collect(false); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	return build.BuildAccessor(dir, accessorRef, recordCount)
}

func (c *stateDomainChangeBinaryAccessorV4Collectors) Build(dir string, accessorRef SegmentRef, recordCount uint64) (SegmentRef, etl.Stats, error) {
	ref, _, stats, err := c.build(context.Background(), dir, accessorRef, recordCount, true)
	return ref, stats, err
}

// ExpectedMetadataContext deterministically rebuilds the accessor byte stream
// into a hashing sink. ETL spill files and the group payload stay bounded on
// disk, while the multi-gigabyte final accessor is never materialized a second
// time merely to prove semantic equality with an immutable object.
func (c *stateDomainChangeBinaryAccessorV4Collectors) ExpectedMetadataContext(ctx context.Context, dir string, accessorRef SegmentRef, recordCount uint64) (snapshotFileMetadata, etl.Stats, error) {
	_, metadata, stats, err := c.build(ctx, dir, accessorRef, recordCount, false)
	return metadata, stats, err
}

func (c *stateDomainChangeBinaryAccessorV4Collectors) build(ctx context.Context, dir string, accessorRef SegmentRef, recordCount uint64, publish bool) (SegmentRef, snapshotFileMetadata, etl.Stats, error) {
	if c == nil || c.exact == nil || c.group == nil {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, errors.New("snapshots: nil state-domain-change accessor v4 collectors")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, err
	}
	if recordCount > math.MaxUint32 {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, fmt.Errorf("snapshots: state-domain-change accessor v4 count %d exceeds uint32 record index", recordCount)
	}
	accessorAbs := filepath.Join(dir, accessorRef.Path)
	accessorDir := filepath.Dir(accessorAbs)
	if err := os.MkdirAll(accessorDir, 0o755); err != nil {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, err
	}
	accessorBase := filepath.Base(accessorAbs)
	groupPayloadTmp, groupPayloadName, err := createStateDomainChangeBinaryTempFileInDir(accessorDir, accessorBase+".groups")
	if err != nil {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, err
	}
	defer func() { _ = groupPayloadTmp.Close(); _ = os.Remove(groupPayloadName) }()
	groupOffsetsTmp, groupOffsetsName, err := createStateDomainChangeBinaryTempFileInDir(accessorDir, accessorBase+".group-offsets")
	if err != nil {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, err
	}
	defer func() { _ = groupOffsetsTmp.Close(); _ = os.Remove(groupOffsetsName) }()
	var (
		groupSink          ethdb.KeyValueWriter
		finishGroup        func() error
		releaseGroup       func()
		groupCount         *uint64
		groupRecords       *uint64
		groupPayloadOffset *uint64
	)
	if c.version == stateDomainChangeBinaryVersionV5 {
		writer := &stateDomainChangeBinaryAccessorV5GroupETLWriter{
			payload:      acquireStateDomainChangeHistoryWriter(groupPayloadTmp),
			offsetWriter: acquireStateDomainChangeHistoryWriter(groupOffsetsTmp),
		}
		groupSink, finishGroup, releaseGroup = writer, writer.Finish, writer.Release
		groupCount, groupRecords, groupPayloadOffset = &writer.groups, &writer.records, &writer.payloadOffset
	} else {
		writer := &stateDomainChangeBinaryAccessorV4GroupETLWriter{
			payload:      acquireStateDomainChangeHistoryWriter(groupPayloadTmp),
			offsetWriter: acquireStateDomainChangeHistoryWriter(groupOffsetsTmp),
		}
		groupSink, finishGroup, releaseGroup = writer, writer.Finish, writer.Release
		groupCount, groupRecords, groupPayloadOffset = &writer.groups, &writer.records, &writer.payloadOffset
	}
	defer releaseGroup()
	if _, err := c.group.LoadInterruptible(groupSink, func() bool { return ctx.Err() != nil }); err != nil {
		if errors.Is(err, etl.ErrLoadInterrupted) && ctx.Err() != nil {
			err = ctx.Err()
		}
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, err
	}
	if err := finishGroup(); err != nil {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, err
	}
	if *groupCount > recordCount || *groupRecords > recordCount {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, fmt.Errorf("snapshots: state-domain-change accessor v%d groups %d records %d exceed segment count %d", c.version, *groupCount, *groupRecords, recordCount)
	}
	if info, err := groupOffsetsTmp.Stat(); err != nil {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, err
	} else if uint64(info.Size()) != *groupCount*8 {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, fmt.Errorf("snapshots: state-domain-change accessor v%d group offsets size %d, want %d", c.version, info.Size(), *groupCount*8)
	}

	var (
		accessorTmp     *os.File
		accessorTmpName string
		destination     io.Writer = io.Discard
	)
	if publish {
		// exact/group files are private assembly inputs and are never published.
		// Their buffered writers are flushed above; only the assembled accessor
		// needs fsync before its atomic rename, matching Erigon's temp-file policy.
		accessorTmp, accessorTmpName, err = createStateDomainChangeBinaryTempFileInDir(accessorDir, accessorBase)
		if err != nil {
			return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, err
		}
		defer func() { _ = accessorTmp.Close(); _ = os.Remove(accessorTmpName) }()
		destination = accessorTmp
	}
	metadataWriter := newSnapshotMetadataWriter(destination)
	if err := writeStateDomainChangeBinaryHeaderToVersion(metadataWriter, stateDomainChangeBinaryAccessorMagic, accessorRef.FromTxNum, accessorRef.ToTxNum, recordCount, c.version); err != nil {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, err
	}
	if err := writeStateDomainChangeBinaryTxRangeCount(metadataWriter, *groupCount); err != nil {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, err
	}
	var (
		exactSink    ethdb.KeyValueWriter
		finishExact  func() error
		releaseExact func()
		exactCount   *uint64
	)
	if c.version == stateDomainChangeBinaryVersionV5 {
		writer := &stateDomainChangeBinaryAccessorV5ExactETLWriter{file: acquireStateDomainChangeHistoryWriter(metadataWriter), expected: recordCount}
		exactSink, finishExact, releaseExact, exactCount = writer, writer.Finish, writer.Release, &writer.count
	} else {
		writer := &stateDomainChangeBinaryAccessorV3ExactETLWriter{file: acquireStateDomainChangeHistoryWriter(metadataWriter), expected: recordCount}
		exactSink, finishExact, releaseExact, exactCount = writer, writer.Finish, writer.Release, &writer.count
	}
	defer releaseExact()
	exactStats, err := c.exact.LoadInterruptible(exactSink, func() bool { return ctx.Err() != nil })
	if err != nil {
		if errors.Is(err, etl.ErrLoadInterrupted) && ctx.Err() != nil {
			err = ctx.Err()
		}
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, err
	}
	if *exactCount != recordCount {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, fmt.Errorf("snapshots: state-domain-change accessor v%d exact entries %d, want %d", c.version, *exactCount, recordCount)
	}
	if err := finishExact(); err != nil {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, err
	}
	tailWriter := acquireStateDomainChangeHistoryWriter(metadataWriter)
	defer releaseStateDomainChangeHistoryWriter(&tailWriter)
	exactEntrySize := uint64(stateDomainChangeBinaryAccessorV3ExactEntrySize)
	if c.version == stateDomainChangeBinaryVersionV5 {
		exactEntrySize = stateDomainChangeBinaryAccessorV5ExactEntrySize
	}
	payloadStart := uint64(stateDomainChangeBinaryHeaderSize+stateDomainChangeBinaryAccessorV3HeaderExtra) + recordCount*exactEntrySize + *groupCount*8
	if _, err := groupOffsetsTmp.Seek(0, io.SeekStart); err != nil {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, err
	}
	if err := writeStateDomainChangeBinaryAccessorV4GroupDirectory(ctx, tailWriter, groupOffsetsTmp, *groupCount, payloadStart, *groupPayloadOffset); err != nil {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, err
	}
	if _, err := groupPayloadTmp.Seek(0, io.SeekStart); err != nil {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, err
	}
	if _, err := groupOffsetsTmp.Seek(0, io.SeekStart); err != nil {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, err
	}
	var copyErr error
	if c.version == stateDomainChangeBinaryVersionV5 {
		_, copyErr = copyStateDomainChangeBinaryAccessorV5GroupPayloadFromOffsets(tailWriter, contextReader{ctx: ctx, r: groupPayloadTmp}, contextReader{ctx: ctx, r: groupOffsetsTmp}, *groupCount, *groupPayloadOffset)
	} else {
		_, copyErr = copyStateDomainChangeBinaryAccessorV4GroupPayloadFromOffsets(tailWriter, contextReader{ctx: ctx, r: groupPayloadTmp}, contextReader{ctx: ctx, r: groupOffsetsTmp}, *groupCount, *groupPayloadOffset)
	}
	if copyErr != nil {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, copyErr
	}
	if err := tailWriter.Flush(); err != nil {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, err
	}
	releaseStateDomainChangeHistoryWriter(&tailWriter)
	metadata := metadataWriter.Metadata()
	if !publish {
		accessorRef.Size = metadata.size
		accessorRef.Checksum = "sha256:" + hex.EncodeToString(metadata.checksum[:])
		return accessorRef, metadata, exactStats, nil
	}
	resultRef, err := finalizeStateDomainChangeHistoryFileWithMetadata(dir, accessorRef, accessorTmp, accessorTmpName, metadata, false)
	if err != nil {
		return SegmentRef{}, snapshotFileMetadata{}, etl.Stats{}, err
	}
	return resultRef, metadata, exactStats, nil
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

func finalizeStateDomainChangeBinaryCompanionsV6(dir string, historyRef SegmentRef, indexTmp *os.File, indexTmpName string, build *stateDomainChangeV6Build, recordCount uint64) (indexRef, accessorRef SegmentRef, accessorStats etl.Stats, err error) {
	return finalizeStateDomainChangeBinaryCompanionsV6Context(context.Background(), dir, historyRef, indexTmp, indexTmpName, build, recordCount)
}

func finalizeStateDomainChangeBinaryCompanionsV6Context(ctx context.Context, dir string, historyRef SegmentRef, indexTmp *os.File, indexTmpName string, build *stateDomainChangeV6Build, recordCount uint64) (indexRef, accessorRef SegmentRef, accessorStats etl.Stats, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	indexRef = SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentInverted, FromTxNum: historyRef.FromTxNum, ToTxNum: historyRef.ToTxNum, AggregationSteps: historyRef.AggregationSteps, Path: stateDomainChangeBinaryIndexPath(historyRef.Path)}
	accessorRef = SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccessor, FromTxNum: historyRef.FromTxNum, ToTxNum: historyRef.ToTxNum, AggregationSteps: historyRef.AggregationSteps, Path: stateDomainChangeBinaryAccessorPath(historyRef.Path)}
	var indexErr, accessorErr error
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		indexRef, indexErr = finalizeStateDomainChangeHistoryFileContext(ctx, dir, indexRef, indexTmp, indexTmpName, false)
		if indexErr != nil {
			cancel()
		}
	}()
	go func() {
		defer workers.Done()
		accessorRef, accessorStats, accessorErr = build.BuildAccessorContext(ctx, dir, accessorRef, recordCount)
		if accessorErr != nil {
			cancel()
		}
	}()
	workers.Wait()
	// A failed worker cancels its sibling. Report the actual failure, not the
	// sibling's cooperative stop, so corruption/I/O errors are not suppressed.
	for _, workerErr := range []error{indexErr, accessorErr} {
		if workerErr != nil && !errors.Is(workerErr, context.Canceled) && !errors.Is(workerErr, context.DeadlineExceeded) {
			return indexRef, accessorRef, accessorStats, workerErr
		}
	}
	return indexRef, accessorRef, accessorStats, errors.Join(indexErr, accessorErr)
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

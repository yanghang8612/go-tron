package snapshots

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

// stateDomainChangeHistoryBuildResult keeps build-only details out of the
// public snapshot API while letting tests prove that large accessor inputs spill
// through the same ETL path production uses.
type stateDomainChangeHistoryBuildResult struct {
	recordETL   etl.Stats
	refs        []SegmentRef
	accessorETL etl.Stats
}

// buildStateDomainChangeHistoryBinarySegmentsFromDB writes the production cold
// history trio without materializing a batch of StateDomainChange rows. The
// history payload is externally sorted into tx/sequence order; the key accessor
// is sorted through a second ETL pass so temporary memory is bounded by the
// collector buffer even when physical hot rows are not tx ordered.
func buildStateDomainChangeHistoryBinarySegmentsFromDB(db ethdb.Iteratee, dir string, ref SegmentRef, cfg DomainCfg, opts etl.Options) (result stateDomainChangeHistoryBuildResult, err error) {
	if ref.Kind == "" {
		ref.Kind = SegmentHistory
	}
	if ref.Dataset == "" {
		ref.Dataset = SegmentDatasetStateDomainChange
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

	if opts.TempDir == "" {
		opts.TempDir = filepath.Join(dir, "etl")
	}
	recordCollector, err := etl.NewCollector(opts)
	if err != nil {
		return result, fmt.Errorf("snapshots: create state-domain-change history record ETL collector: %w", err)
	}
	defer recordCollector.Close()
	accessorCollector, err := etl.NewCollector(opts)
	if err != nil {
		return result, fmt.Errorf("snapshots: create state-domain-change history accessor ETL collector: %w", err)
	}
	defer accessorCollector.Close()
	recordCount, err := collectStateDomainChangeHistoryRecords(db, cfg, ref.FromTxNum, ref.ToTxNum, recordCollector)
	if err != nil {
		return result, err
	}
	txRangeCount, err := countStateDomainChangeHistoryTxRanges(db, cfg, ref.FromTxNum, ref.ToTxNum)
	if err != nil {
		return result, err
	}
	if txRangeCount > (math.MaxUint64-uint64(stateDomainChangeBinaryHeaderSize)-8)/stateDomainChangeBinaryTxRangeSize {
		return result, fmt.Errorf("snapshots: state-domain-change tx range count %d overflows segment size", txRangeCount)
	}
	recordOffset := uint64(stateDomainChangeBinaryHeaderSize) + 8 + txRangeCount*stateDomainChangeBinaryTxRangeSize

	segmentTmp, segmentTmpName, err := createStateDomainChangeBinaryTempFile(dir, ref.Path)
	if err != nil {
		return result, err
	}
	defer func() {
		_ = segmentTmp.Close()
		_ = os.Remove(segmentTmpName)
	}()
	if err := writeStateDomainChangeBinaryHeaderTo(segmentTmp, stateDomainChangeBinarySegmentMagic, ref.FromTxNum, ref.ToTxNum, recordCount); err != nil {
		return result, err
	}
	if err := writeStateDomainChangeBinaryTxRangeTableFromDB(segmentTmp, db, cfg, ref.FromTxNum, ref.ToTxNum, txRangeCount); err != nil {
		return result, err
	}

	indexTmp, indexTmpName, err := createStateDomainChangeBinaryTempFile(dir, stateDomainChangeBinaryIndexPath(ref.Path))
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

	accessorTmp, accessorTmpName, err := createStateDomainChangeBinaryTempFile(dir, stateDomainChangeBinaryAccessorPath(ref.Path))
	if err != nil {
		return result, err
	}
	defer func() {
		_ = accessorTmp.Close()
		_ = os.Remove(accessorTmpName)
	}()
	if err := writeStateDomainChangeBinaryHeaderTo(accessorTmp, stateDomainChangeBinaryAccessorMagic, ref.FromTxNum, ref.ToTxNum, recordCount); err != nil {
		return result, err
	}
	if recordCount > (math.MaxUint64-uint64(stateDomainChangeBinaryHeaderSize))/8 {
		return result, fmt.Errorf("snapshots: state-domain-change accessor count %d overflows offset table", recordCount)
	}
	if err := writeZeroes(accessorTmp, recordCount*8); err != nil {
		return result, err
	}

	recordWriter := stateDomainChangeHistoryRecordETLWriter{
		segment:    segmentTmp,
		index:      indexTmp,
		accessor:   accessorCollector,
		ref:        ref,
		expected:   recordCount,
		segmentOff: recordOffset,
	}
	result.recordETL, err = recordCollector.Load(&recordWriter)
	if err != nil {
		return result, fmt.Errorf("snapshots: sort state-domain-change history records: %w", err)
	}
	if err := recordWriter.Finish(); err != nil {
		return result, err
	}
	if err := writeStateDomainChangeBinaryIndexCount(indexTmp, recordWriter.indexWritten); err != nil {
		return result, err
	}

	accessorWriter := stateDomainChangeBinaryAccessorETLWriter{
		file:          accessorTmp,
		ref:           ref,
		expected:      recordCount,
		payloadOffset: uint64(stateDomainChangeBinaryHeaderSize) + recordCount*8,
	}
	result.accessorETL, err = accessorCollector.Load(&accessorWriter)
	if err != nil {
		return result, fmt.Errorf("snapshots: sort state-domain-change history accessor: %w", err)
	}
	if accessorWriter.count != recordCount {
		return result, fmt.Errorf("snapshots: state-domain-change accessor emitted %d records, want %d", accessorWriter.count, recordCount)
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

	segmentRef, err = finalizeStateDomainChangeHistoryFile(dir, ref, segmentTmp, segmentTmpName, true, CompressHistorySegments)
	if err != nil {
		return result, err
	}
	indexRef = SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentInverted,
		FromTxNum: ref.FromTxNum,
		ToTxNum:   ref.ToTxNum,
		Path:      stateDomainChangeBinaryIndexPath(segmentRef.Path),
	}
	indexRef, err = finalizeStateDomainChangeHistoryFile(dir, indexRef, indexTmp, indexTmpName, false, false)
	if err != nil {
		return result, err
	}
	accessorRef = SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentAccessor,
		FromTxNum: ref.FromTxNum,
		ToTxNum:   ref.ToTxNum,
		Path:      stateDomainChangeBinaryAccessorPath(segmentRef.Path),
	}
	accessorRef, err = finalizeStateDomainChangeHistoryFile(dir, accessorRef, accessorTmp, accessorTmpName, false, CompressHistorySegments)
	if err != nil {
		return result, err
	}

	// Hot pruning trusts manifest coverage. Validate every finished companion
	// before returning so an unreadable or malformed trio can never be published.
	if err := checkStateDomainChangeBinarySegment(dir, segmentRef); err != nil {
		return result, fmt.Errorf("snapshots: state-domain-change history segment self-check: %w", err)
	}
	if err := checkStateDomainChangeBinaryIndex(dir, indexRef); err != nil {
		return result, fmt.Errorf("snapshots: state-domain-change history index self-check: %w", err)
	}
	if err := checkStateDomainChangeBinaryAccessor(dir, accessorRef); err != nil {
		return result, fmt.Errorf("snapshots: state-domain-change history accessor self-check: %w", err)
	}
	published = true
	result.refs = []SegmentRef{segmentRef, accessorRef, indexRef}
	return result, nil
}

func collectStateDomainChangeHistoryRecords(db ethdb.Iteratee, cfg DomainCfg, fromTxNum, toTxNum uint64, collector *etl.Collector) (uint64, error) {
	if collector == nil {
		return 0, errors.New("snapshots: nil state-domain-change history record ETL collector")
	}
	var count uint64
	err := cfg.IterateHotHistoryTxRangeChanges(db, fromTxNum, toTxNum, func(change *rawdb.StateDomainChange) (bool, error) {
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
		if err := collector.Put(stateDomainChangeHistoryRecordETLSortKey(change, count), value); err != nil {
			return false, err
		}
		count++
		return true, nil
	})
	return count, err
}

func countStateDomainChangeHistoryTxRanges(db ethdb.Iteratee, cfg DomainCfg, fromTxNum, toTxNum uint64) (uint64, error) {
	var count uint64
	err := iterateStateDomainChangeHistoryTxRanges(db, cfg, fromTxNum, toTxNum, func(*rawdb.StateTxRange) error {
		if count == math.MaxUint64 {
			return errors.New("snapshots: state-domain-change tx range count overflows")
		}
		count++
		return nil
	})
	return count, err
}

func writeStateDomainChangeBinaryTxRangeTableFromDB(w io.Writer, db ethdb.Iteratee, cfg DomainCfg, fromTxNum, toTxNum, count uint64) error {
	var countRaw [8]byte
	binary.BigEndian.PutUint64(countRaw[:], count)
	if _, err := w.Write(countRaw[:]); err != nil {
		return err
	}
	var emitted uint64
	if err := iterateStateDomainChangeHistoryTxRanges(db, cfg, fromTxNum, toTxNum, func(row *rawdb.StateTxRange) error {
		if emitted >= count {
			return fmt.Errorf("snapshots: state-domain-change tx range count exceeds preflight count %d", count)
		}
		if err := writeStateDomainChangeBinaryTxRangeEntry(w, row); err != nil {
			return err
		}
		emitted++
		return nil
	}); err != nil {
		return err
	}
	if emitted != count {
		return fmt.Errorf("snapshots: state-domain-change tx range count %d, want preflight count %d", emitted, count)
	}
	return nil
}

func iterateStateDomainChangeHistoryTxRanges(db ethdb.Iteratee, cfg DomainCfg, fromTxNum, toTxNum uint64, fn func(*rawdb.StateTxRange) error) error {
	var (
		previousBlock uint64
		havePrevious  bool
	)
	return cfg.IterateHotHistoryTxRanges(db, func(row *rawdb.StateTxRange) (bool, error) {
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

func encodeStateDomainChangeBinaryRecordFrame(change *rawdb.StateDomainChange) ([]byte, error) {
	payload, err := encodeStateDomainChangeRecord(change)
	if err != nil {
		return nil, err
	}
	if uint64(len(payload)) > math.MaxUint32 {
		return nil, fmt.Errorf("snapshots: state-domain-change record is too large: %d bytes", len(payload))
	}
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)
	return frame, nil
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

type stateDomainChangeHistoryRecordETLWriter struct {
	segment      *os.File
	index        *os.File
	accessor     *etl.Collector
	ref          SegmentRef
	expected     uint64
	count        uint64
	segmentOff   uint64
	indexWritten uint64
	currentIndex stateDomainChangeBinaryTxOffset
	haveIndex    bool
	previous     *rawdb.StateDomainChange
}

func (w *stateDomainChangeHistoryRecordETLWriter) Put(_ []byte, value []byte) error {
	if w == nil || w.segment == nil || w.index == nil || w.accessor == nil {
		return errors.New("snapshots: nil state-domain-change history record ETL writer")
	}
	if w.count >= w.expected {
		return fmt.Errorf("snapshots: state-domain-change history emitted more than %d records", w.expected)
	}
	change, err := decodeStateDomainChangeRecord(value)
	if err != nil {
		return err
	}
	if change.TxNum < w.ref.FromTxNum || change.TxNum > w.ref.ToTxNum {
		return fmt.Errorf("snapshots: state-domain-change tx %d outside segment range [%d,%d]", change.TxNum, w.ref.FromTxNum, w.ref.ToTxNum)
	}
	if w.previous != nil && compareStateDomainChangeForBinary(w.previous, change) > 0 {
		return errors.New("snapshots: state-domain-change record ETL rows are not ordered")
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

	frame, err := encodeStateDomainChangeBinaryRecordFrame(change)
	if err != nil {
		return err
	}
	if _, err := w.segment.Write(frame); err != nil {
		return err
	}
	entry := stateDomainChangeBinaryAccessorEntry{
		key:         stateDomainChangeBinaryAccessorKey(change),
		txNum:       change.TxNum,
		seq:         change.Seq,
		offset:      w.segmentOff,
		recordIndex: w.count,
	}
	entryFrame, err := encodeStateDomainChangeBinaryAccessorEntryFrame(entry)
	if err != nil {
		return err
	}
	if err := w.accessor.Put(stateDomainChangeBinaryAccessorETLSortKey(entry), entryFrame); err != nil {
		return err
	}
	if uint64(len(frame)) > math.MaxUint64-w.segmentOff {
		return errors.New("snapshots: state-domain-change segment offset overflows")
	}
	w.segmentOff += uint64(len(frame))
	w.count++
	w.previous = cloneStateDomainChangeForSegment(change)
	return nil
}

func (w *stateDomainChangeHistoryRecordETLWriter) Delete([]byte) error {
	return errors.New("snapshots: state-domain-change history record ETL writer does not support deletes")
}

func (w *stateDomainChangeHistoryRecordETLWriter) Finish() error {
	if w == nil {
		return errors.New("snapshots: nil state-domain-change history record ETL writer")
	}
	if err := w.flushIndex(); err != nil {
		return err
	}
	if w.count != w.expected {
		return fmt.Errorf("snapshots: state-domain-change history emitted %d records, want %d", w.count, w.expected)
	}
	return nil
}

func (w *stateDomainChangeHistoryRecordETLWriter) flushIndex() error {
	if !w.haveIndex {
		return nil
	}
	if err := writeStateDomainChangeBinaryIndexEntryTo(w.index, w.currentIndex); err != nil {
		return err
	}
	w.indexWritten++
	w.haveIndex = false
	return nil
}

func writeStateDomainChangeBinaryIndexCount(file *os.File, count uint64) error {
	if file == nil {
		return errors.New("snapshots: nil state-domain-change binary index file")
	}
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], count)
	_, err := file.WriteAt(raw[:], 28)
	return err
}

// stateDomainChangeBinaryAccessorETLSortKey preserves the binary accessor's
// comparison order while appending a unique fixed-width suffix. NUL escaping
// keeps prefix keys ordered before their extensions, so Collector never merges
// distinct rows with the same logical accessor key.
func stateDomainChangeBinaryAccessorETLSortKey(entry stateDomainChangeBinaryAccessorEntry) []byte {
	out := make([]byte, 0, len(entry.key)+34)
	for _, b := range entry.key {
		if b == 0 {
			out = append(out, 0, 0xff)
			continue
		}
		out = append(out, b)
	}
	out = append(out, 0, 0)
	var suffix [32]byte
	binary.BigEndian.PutUint64(suffix[0:8], entry.txNum)
	binary.BigEndian.PutUint64(suffix[8:16], entry.seq)
	binary.BigEndian.PutUint64(suffix[16:24], entry.offset)
	binary.BigEndian.PutUint64(suffix[24:32], entry.recordIndex)
	return append(out, suffix[:]...)
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

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

	recordWriter := stateDomainChangeHistoryRecordETLWriter{
		segment:    segmentTmp,
		index:      indexTmp,
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
	accessorRef, result.accessorETL, err = buildStateDomainChangeBinaryAccessorV4FromHistorySegment(dir, segmentRef, accessorRef, opts)
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
	payload, err := encodeStateDomainChangeRecordV5(change)
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
	if w == nil || w.segment == nil || w.index == nil {
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

type stateDomainChangeBinaryAccessorV3ExactETLWriter struct {
	file     *os.File
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

type stateDomainChangeBinaryAccessorV4GroupETLWriter struct {
	payload      *os.File
	offsets      *os.File
	groups       uint64
	records      uint64
	haveGroup    bool
	currentKey   [stateDomainChangeBinaryAccessorV3GroupKeySize]byte
	currentCount uint64
	countOffset  int64
}

func (w *stateDomainChangeBinaryAccessorV4GroupETLWriter) Put(_ []byte, value []byte) error {
	if w == nil || w.payload == nil || w.offsets == nil {
		return errors.New("snapshots: nil state-domain-change accessor v3 group ETL writer")
	}
	if len(value) != stateDomainChangeBinaryAccessorV3GroupKeySize+stateDomainChangeBinaryAccessorV4GroupEntrySize {
		return fmt.Errorf("snapshots: state-domain-change accessor v4 group value size %d, want %d", len(value), stateDomainChangeBinaryAccessorV3GroupKeySize+stateDomainChangeBinaryAccessorV4GroupEntrySize)
	}
	var key [stateDomainChangeBinaryAccessorV3GroupKeySize]byte
	copy(key[:], value[:stateDomainChangeBinaryAccessorV3GroupKeySize])
	prefix := binary.BigEndian.Uint32(value[stateDomainChangeBinaryAccessorV3GroupKeySize : stateDomainChangeBinaryAccessorV3GroupKeySize+4])
	offset := binary.BigEndian.Uint64(value[stateDomainChangeBinaryAccessorV3GroupKeySize+4 : stateDomainChangeBinaryAccessorV3GroupKeySize+12])
	recordIndex := binary.BigEndian.Uint32(value[stateDomainChangeBinaryAccessorV3GroupKeySize+12:])
	if offset < stateDomainChangeBinaryHeaderSize {
		return fmt.Errorf("snapshots: state-domain-change accessor v3 group record offset %d is invalid", offset)
	}
	if !w.haveGroup || !bytes.Equal(w.currentKey[:], key[:]) {
		if err := w.finishGroup(); err != nil {
			return err
		}
		pos, err := w.payload.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if pos < 0 {
			return errors.New("snapshots: state-domain-change accessor v3 group payload offset is negative")
		}
		var raw [8]byte
		binary.BigEndian.PutUint64(raw[:], uint64(pos))
		if _, err := w.offsets.Write(raw[:]); err != nil {
			return err
		}
		if _, err := w.payload.Write(key[:]); err != nil {
			return err
		}
		w.countOffset = pos + stateDomainChangeBinaryAccessorV3GroupKeySize
		if err := writeZeroes(w.payload, 8); err != nil {
			return err
		}
		w.currentKey = key
		w.currentCount = 0
		w.haveGroup = true
		w.groups++
	}
	var raw [stateDomainChangeBinaryAccessorV4GroupEntrySize]byte
	binary.BigEndian.PutUint32(raw[0:4], prefix)
	binary.BigEndian.PutUint64(raw[4:12], offset)
	binary.BigEndian.PutUint32(raw[12:], recordIndex)
	if _, err := w.payload.Write(raw[:]); err != nil {
		return err
	}
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
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], w.currentCount)
	if _, err := w.payload.WriteAt(raw[:], w.countOffset); err != nil {
		return err
	}
	w.haveGroup = false
	return nil
}

func (*stateDomainChangeBinaryAccessorV4GroupETLWriter) Delete([]byte) error {
	return errors.New("snapshots: state-domain-change accessor v4 group ETL writer does not support deletes")
}

func buildStateDomainChangeBinaryAccessorV4FromHistorySegment(dir string, segmentRef, accessorRef SegmentRef, opts etl.Options) (SegmentRef, etl.Stats, error) {
	if opts.TempDir == "" {
		opts.TempDir = filepath.Join(dir, "etl")
	}
	exactCollector, err := etl.NewCollector(opts)
	if err != nil {
		return SegmentRef{}, etl.Stats{}, fmt.Errorf("snapshots: create state-domain-change accessor v3 exact ETL collector: %w", err)
	}
	defer exactCollector.Close()
	groupCollector, err := etl.NewCollector(opts)
	if err != nil {
		return SegmentRef{}, etl.Stats{}, fmt.Errorf("snapshots: create state-domain-change accessor v3 group ETL collector: %w", err)
	}
	defer groupCollector.Close()

	segment, segmentSize, header, err := openHistorySegmentForRead(dir, segmentRef)
	if err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	defer segment.Close()
	if header.count > math.MaxUint32 {
		return SegmentRef{}, etl.Stats{}, fmt.Errorf("snapshots: state-domain-change accessor v3 count %d exceeds uint32 record index", header.count)
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
		key := stateDomainChangeBinaryAccessorKey(change)
		if recordIndex > math.MaxUint32 {
			return SegmentRef{}, etl.Stats{}, fmt.Errorf("snapshots: state-domain-change accessor v3 record index %d exceeds uint32", recordIndex)
		}
		hash := stateDomainChangeBinaryAccessorV3Hash(key)
		exactValue := make([]byte, stateDomainChangeBinaryAccessorV3ExactEntrySize)
		copy(exactValue[:stateDomainChangeBinaryAccessorV3HashSize], hash[:])
		binary.BigEndian.PutUint64(exactValue[stateDomainChangeBinaryAccessorV3HashSize:], offset)
		binary.BigEndian.PutUint32(exactValue[stateDomainChangeBinaryAccessorV3HashSize+8:], uint32(recordIndex))
		if err := exactCollector.Put(exactValue, exactValue); err != nil {
			return SegmentRef{}, etl.Stats{}, err
		}
		if groupKey, ok := stateDomainChangeBinaryAccessorV3GroupKey(change); ok {
			entry := stateDomainChangeBinaryAccessorEntry{key: key, txNum: change.TxNum, seq: change.Seq, offset: offset, recordIndex: recordIndex}
			groupValue := make([]byte, stateDomainChangeBinaryAccessorV3GroupKeySize+stateDomainChangeBinaryAccessorV4GroupEntrySize)
			copy(groupValue[:stateDomainChangeBinaryAccessorV3GroupKeySize], groupKey[:])
			binary.BigEndian.PutUint32(groupValue[stateDomainChangeBinaryAccessorV3GroupKeySize:stateDomainChangeBinaryAccessorV3GroupKeySize+4], stateDomainChangeBinaryAccessorV4LogicalPrefix(change.Key))
			binary.BigEndian.PutUint64(groupValue[stateDomainChangeBinaryAccessorV3GroupKeySize+4:stateDomainChangeBinaryAccessorV3GroupKeySize+12], offset)
			binary.BigEndian.PutUint32(groupValue[stateDomainChangeBinaryAccessorV3GroupKeySize+12:], uint32(recordIndex))
			if err := groupCollector.Put(stateDomainChangeBinaryAccessorETLSortKey(entry), groupValue); err != nil {
				return SegmentRef{}, etl.Stats{}, err
			}
		}
		offset = next
	}
	if offset != segmentSize {
		return SegmentRef{}, etl.Stats{}, fmt.Errorf("snapshots: state-domain-change accessor v3 source segment %q has %d trailing bytes", segmentRef.Path, segmentSize-offset)
	}

	exactTmp, exactTmpName, err := createStateDomainChangeBinaryTempFile(dir, accessorRef.Path+".exact")
	if err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	defer func() { _ = exactTmp.Close(); _ = os.Remove(exactTmpName) }()
	exactWriter := stateDomainChangeBinaryAccessorV3ExactETLWriter{file: exactTmp, expected: header.count}
	exactStats, err := exactCollector.Load(&exactWriter)
	if err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	if exactWriter.count != header.count {
		return SegmentRef{}, etl.Stats{}, fmt.Errorf("snapshots: state-domain-change accessor v3 exact entries %d, want %d", exactWriter.count, header.count)
	}
	if err := exactTmp.Sync(); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}

	groupPayloadTmp, groupPayloadName, err := createStateDomainChangeBinaryTempFile(dir, accessorRef.Path+".groups")
	if err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	defer func() { _ = groupPayloadTmp.Close(); _ = os.Remove(groupPayloadName) }()
	groupOffsetsTmp, groupOffsetsName, err := createStateDomainChangeBinaryTempFile(dir, accessorRef.Path+".group-offsets")
	if err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	defer func() { _ = groupOffsetsTmp.Close(); _ = os.Remove(groupOffsetsName) }()
	groupWriter := stateDomainChangeBinaryAccessorV4GroupETLWriter{payload: groupPayloadTmp, offsets: groupOffsetsTmp}
	if _, err := groupCollector.Load(&groupWriter); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	if err := groupWriter.finishGroup(); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	if groupWriter.groups > header.count || groupWriter.records > header.count {
		return SegmentRef{}, etl.Stats{}, fmt.Errorf("snapshots: state-domain-change accessor v3 groups %d records %d exceed segment count %d", groupWriter.groups, groupWriter.records, header.count)
	}
	if err := groupPayloadTmp.Sync(); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	if err := groupOffsetsTmp.Sync(); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}

	accessorTmp, accessorTmpName, err := createStateDomainChangeBinaryTempFile(dir, accessorRef.Path)
	if err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	defer func() { _ = accessorTmp.Close(); _ = os.Remove(accessorTmpName) }()
	if err := writeStateDomainChangeBinaryHeaderToVersion(accessorTmp, stateDomainChangeBinaryAccessorMagic, accessorRef.FromTxNum, accessorRef.ToTxNum, header.count, stateDomainChangeBinaryVersionV4); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	if err := writeStateDomainChangeBinaryTxRangeCount(accessorTmp, groupWriter.groups); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	if _, err := exactTmp.Seek(0, io.SeekStart); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	if _, err := io.Copy(accessorTmp, exactTmp); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	payloadStart := uint64(stateDomainChangeBinaryHeaderSize+stateDomainChangeBinaryAccessorV3HeaderExtra) + header.count*stateDomainChangeBinaryAccessorV3ExactEntrySize + groupWriter.groups*8
	if _, err := groupOffsetsTmp.Seek(0, io.SeekStart); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	for i := uint64(0); i < groupWriter.groups; i++ {
		var raw [8]byte
		if _, err := io.ReadFull(groupOffsetsTmp, raw[:]); err != nil {
			return SegmentRef{}, etl.Stats{}, err
		}
		relative := binary.BigEndian.Uint64(raw[:])
		if relative > math.MaxUint64-payloadStart {
			return SegmentRef{}, etl.Stats{}, errors.New("snapshots: state-domain-change accessor v3 group payload offset overflows")
		}
		binary.BigEndian.PutUint64(raw[:], payloadStart+relative)
		if _, err := accessorTmp.Write(raw[:]); err != nil {
			return SegmentRef{}, etl.Stats{}, err
		}
	}
	if _, err := groupPayloadTmp.Seek(0, io.SeekStart); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	if _, err := io.Copy(accessorTmp, groupPayloadTmp); err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	resultRef, err := finalizeStateDomainChangeHistoryFile(dir, accessorRef, accessorTmp, accessorTmpName, false, false)
	if err != nil {
		return SegmentRef{}, etl.Stats{}, err
	}
	return resultRef, exactStats, nil
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

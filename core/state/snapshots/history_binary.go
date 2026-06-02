package snapshots

import (
	"bytes"
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

const (
	stateDomainChangeBinaryVersion = uint32(1)

	stateDomainChangeBinaryHeaderSize     = 8 + 4 + 8 + 8 + 8
	stateDomainChangeBinaryIndexEntrySize = 8 + 8 + 8 + 8
	stateDomainChangeBinaryAccessorInts   = 8 + 8 + 8 + 8
)

var (
	stateDomainChangeBinarySegmentMagic  = [8]byte{'g', 't', 's', 'd', 'c', 's', 'e', 'g'}
	stateDomainChangeBinaryIndexMagic    = [8]byte{'g', 't', 's', 'd', 'c', 'i', 'd', 'x'}
	stateDomainChangeBinaryAccessorMagic = [8]byte{'g', 't', 's', 'd', 'c', 'k', 'v', '1'}
)

type stateDomainChangeBinaryHeader struct {
	fromTxNum uint64
	toTxNum   uint64
	count     uint64
}

type stateDomainChangeBinaryTxOffset struct {
	txNum       uint64
	offset      uint64
	recordIndex uint64
	count       uint64
}

type stateDomainChangeBinaryAccessorEntry struct {
	key         []byte
	txNum       uint64
	seq         uint64
	offset      uint64
	recordIndex uint64
}

func encodeStateDomainChangeRecord(change *rawdb.StateDomainChange) ([]byte, error) {
	if change == nil {
		return nil, errors.New("snapshots: nil state-domain-change record")
	}
	var buf bytes.Buffer
	writeUint64(&buf, change.BlockNum)
	buf.Write(change.BlockHash[:])
	writeUint64(&buf, change.TxNum)
	writeUint64(&buf, change.Seq)
	buf.WriteByte(byte(change.FlatDomain))
	buf.Write(change.Owner[:])
	writeUint64(&buf, change.Generation)
	writeUint16(&buf, uint16(change.Domain))
	if err := writeLengthPrefixedBytes(&buf, change.Key); err != nil {
		return nil, err
	}
	writeBool(&buf, change.PrevExists)
	if err := writeLengthPrefixedBytes(&buf, change.Prev); err != nil {
		return nil, err
	}
	writeBool(&buf, change.NextExists)
	if err := writeLengthPrefixedBytes(&buf, change.Next); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeStateDomainChangeRecord(data []byte) (*rawdb.StateDomainChange, error) {
	r := bytes.NewReader(data)
	change := new(rawdb.StateDomainChange)
	var err error
	if change.BlockNum, err = readUint64(r); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(r, change.BlockHash[:]); err != nil {
		return nil, err
	}
	if change.TxNum, err = readUint64(r); err != nil {
		return nil, err
	}
	if change.Seq, err = readUint64(r); err != nil {
		return nil, err
	}
	domain, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	change.FlatDomain = rawdb.StateFlatDomain(domain)
	if _, err := io.ReadFull(r, change.Owner[:]); err != nil {
		return nil, err
	}
	if change.Generation, err = readUint64(r); err != nil {
		return nil, err
	}
	rawDomain, err := readUint16(r)
	if err != nil {
		return nil, err
	}
	change.Domain = kvdomains.KVDomain(rawDomain)
	if change.Key, err = readLengthPrefixedBytes(r); err != nil {
		return nil, err
	}
	if change.PrevExists, err = readBool(r); err != nil {
		return nil, err
	}
	if change.Prev, err = readLengthPrefixedBytes(r); err != nil {
		return nil, err
	}
	if change.NextExists, err = readBool(r); err != nil {
		return nil, err
	}
	if change.Next, err = readLengthPrefixedBytes(r); err != nil {
		return nil, err
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("snapshots: state-domain-change record has %d trailing bytes", r.Len())
	}
	return change, nil
}

func writeStateDomainChangeBinaryFiles(dir string, ref SegmentRef, changes []*rawdb.StateDomainChange) (SegmentRef, SegmentRef, error) {
	segRef, idxRef, _, err := writeStateDomainChangeBinaryFilesWithAccessor(dir, ref, changes)
	return segRef, idxRef, err
}

func writeStateDomainChangeBinaryFilesWithAccessor(dir string, ref SegmentRef, changes []*rawdb.StateDomainChange) (SegmentRef, SegmentRef, SegmentRef, error) {
	if ref.Kind == "" {
		ref.Kind = SegmentHistory
	}
	if ref.Dataset == "" {
		ref.Dataset = SegmentDatasetStateDomainChange
	}
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, fmt.Errorf("snapshots: state-domain-change binary segment %q is %s/%s, want state-domain-change/history", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}

	normalized := normalizeStateDomainChangesForBinary(changes)
	segmentData, index, accessor, err := encodeStateDomainChangeBinarySegment(ref.FromTxNum, ref.ToTxNum, normalized)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	indexData, err := encodeStateDomainChangeBinaryIndex(ref.FromTxNum, ref.ToTxNum, index)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	accessorData, err := encodeStateDomainChangeBinaryAccessor(ref.FromTxNum, ref.ToTxNum, accessor)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}

	segRef := ref
	setStateDomainChangeBinaryRefMetadata(&segRef, segmentData)
	segRef.Path = contentAddressedSnapshotPath(segRef.Path, segRef.Checksum)
	if err := validateSegment(segRef, segRef.FromTxNum, segRef.ToTxNum); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	idxRef := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentInverted,
		FromTxNum: ref.FromTxNum,
		ToTxNum:   ref.ToTxNum,
		Path:      stateDomainChangeBinaryIndexPath(segRef.Path),
	}
	if err := validateSegment(idxRef, idxRef.FromTxNum, idxRef.ToTxNum); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	setStateDomainChangeBinaryRefMetadata(&idxRef, indexData)
	accessorRef := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentAccessor,
		FromTxNum: ref.FromTxNum,
		ToTxNum:   ref.ToTxNum,
		Path:      stateDomainChangeBinaryAccessorPath(segRef.Path),
	}
	if err := validateSegment(accessorRef, accessorRef.FromTxNum, accessorRef.ToTxNum); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	setStateDomainChangeBinaryRefMetadata(&accessorRef, accessorData)

	if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, segRef.Path), segmentData); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, idxRef.Path), indexData); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, accessorRef.Path), accessorData); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	return segRef, idxRef, accessorRef, nil
}

// historyCompressChunkSize is the uncompressed chunk size per compressed block
// in a cold history .seg. ~16 KiB balances ratio against per-lookup decompress
// cost; record frames span chunks freely (ReadAt is multi-block-safe).
const historyCompressChunkSize = 16384

// writeStateDomainChangeBinaryCompressedSegmentFiles writes a cold history
// segment whose .seg payload is block-compressed (magic gtcblk01) while its .idx
// and .kv accessors are byte-identical to the uncompressed writer's — they store
// the same uncompressed record offsets, which the codec's ReadAt resolves. A
// reader opened via openHistorySegmentForRead serves both formats transparently.
func writeStateDomainChangeBinaryCompressedSegmentFiles(dir string, ref SegmentRef, changes []*rawdb.StateDomainChange) (SegmentRef, SegmentRef, SegmentRef, error) {
	if ref.Kind == "" {
		ref.Kind = SegmentHistory
	}
	if ref.Dataset == "" {
		ref.Dataset = SegmentDatasetStateDomainChange
	}
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, fmt.Errorf("snapshots: state-domain-change binary segment %q is %s/%s, want state-domain-change/history", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}

	normalized := normalizeStateDomainChangesForBinary(changes)
	segmentData, index, accessor, err := encodeStateDomainChangeBinarySegment(ref.FromTxNum, ref.ToTxNum, normalized)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	indexData, err := encodeStateDomainChangeBinaryIndex(ref.FromTxNum, ref.ToTxNum, index)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	accessorData, err := encodeStateDomainChangeBinaryAccessor(ref.FromTxNum, ref.ToTxNum, accessor)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}

	// Compress the seg payload to a temp file, then content-address by the
	// compressed file's checksum.
	segRef := ref
	tmpAbs := filepath.Join(dir, ref.Path) + ".cbtmp"
	if err := os.MkdirAll(filepath.Dir(tmpAbs), 0o755); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := compressBlobToFile(dir, tmpAbs, segmentData, historyCompressChunkSize); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	size, checksum, err := stateDomainChangeBinaryFileMetadata(tmpAbs)
	if err != nil {
		_ = os.Remove(tmpAbs)
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	segRef.Size = size
	segRef.Checksum = checksum
	segRef.Path = contentAddressedSnapshotPath(ref.Path, checksum)
	if err := validateSegment(segRef, segRef.FromTxNum, segRef.ToTxNum); err != nil {
		_ = os.Remove(tmpAbs)
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	finalAbs := filepath.Join(dir, segRef.Path)
	if err := os.MkdirAll(filepath.Dir(finalAbs), 0o755); err != nil {
		_ = os.Remove(tmpAbs)
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := os.Rename(tmpAbs, finalAbs); err != nil {
		_ = os.Remove(tmpAbs)
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}

	idxRef := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentInverted,
		FromTxNum: ref.FromTxNum,
		ToTxNum:   ref.ToTxNum,
		Path:      stateDomainChangeBinaryIndexPath(segRef.Path),
	}
	setStateDomainChangeBinaryRefMetadata(&idxRef, indexData)
	accessorRef := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentAccessor,
		FromTxNum: ref.FromTxNum,
		ToTxNum:   ref.ToTxNum,
		Path:      stateDomainChangeBinaryAccessorPath(segRef.Path),
	}

	// .idx stays uncompressed (0.2% of the trio); compress the .kv accessor —
	// it's ~36% of the trio and the full duplicated keys + structured ints
	// compress ~2.3x. Its offset table still indexes the uncompressed logical
	// content, which openStateDomainChangeBinaryAccessorReader serves via ReadAt.
	if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, idxRef.Path), indexData); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	accessorAbs := filepath.Join(dir, accessorRef.Path)
	if err := os.MkdirAll(filepath.Dir(accessorAbs), 0o755); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := compressBlobToFile(dir, accessorAbs, accessorData, historyCompressChunkSize); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	accSize, accChecksum, err := stateDomainChangeBinaryFileMetadata(accessorAbs)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	accessorRef.Size = accSize
	accessorRef.Checksum = accChecksum
	return segRef, idxRef, accessorRef, nil
}

type stateDomainChangeBinaryCompactionSource struct {
	history         SegmentRef
	index           SegmentRef
	accessor        SegmentRef
	segmentHeader   stateDomainChangeBinaryHeader
	indexHeader     stateDomainChangeBinaryHeader
	accessorHeader  stateDomainChangeBinaryHeader
	segmentSize     uint64
	indexSize       uint64
	accessorSize    uint64
	payloadBase     uint64
	recordIndexBase uint64
}

func compactStateDomainChangeBinaryHistoryRun(dir string, cfg DomainCfg, selection historyCompactionSelection) ([]SegmentRef, error) {
	if cfg.Dataset != SegmentDatasetStateDomainChange {
		return nil, fmt.Errorf("snapshots: unsupported state-domain-change compaction dataset %s", cfg.Dataset)
	}
	sources, err := collectStateDomainChangeBinaryCompactionSources(dir, selection)
	if err != nil {
		return nil, err
	}
	segRef, err := writeCompactedStateDomainChangeBinarySegment(dir, cfg, selection, sources)
	if err != nil {
		return nil, err
	}
	idxRef, err := writeCompactedStateDomainChangeBinaryIndex(dir, segRef, sources)
	if err != nil {
		return nil, err
	}
	accessorRef, err := writeCompactedStateDomainChangeBinaryAccessor(dir, segRef, sources)
	if err != nil {
		return nil, err
	}
	return []SegmentRef{segRef, accessorRef, idxRef}, nil
}

func collectStateDomainChangeBinaryCompactionSources(dir string, selection historyCompactionSelection) ([]stateDomainChangeBinaryCompactionSource, error) {
	sources := make([]stateDomainChangeBinaryCompactionSource, 0, len(selection.candidates))
	for _, candidate := range selection.candidates {
		idxRef, ok := historyCompactionCompanion(candidate, SegmentInverted)
		if !ok {
			return nil, fmt.Errorf("snapshots: state-domain-change history %q missing index companion", candidate.history.Path)
		}
		accessorRef, ok := historyCompactionCompanion(candidate, SegmentAccessor)
		if !ok {
			return nil, fmt.Errorf("snapshots: state-domain-change history %q missing accessor companion", candidate.history.Path)
		}
		if err := checkStateDomainChangeBinarySegment(dir, candidate.history); err != nil {
			return nil, err
		}
		if err := checkStateDomainChangeBinaryIndex(dir, idxRef); err != nil {
			return nil, err
		}
		if err := checkStateDomainChangeBinaryAccessor(dir, accessorRef); err != nil {
			return nil, err
		}
		segmentFile, segmentHeader, segmentSize, err := openStateDomainChangeBinarySegmentReader(dir, candidate.history)
		if err != nil {
			return nil, err
		}
		_ = segmentFile.Close()
		indexFile, indexHeader, err := openStateDomainChangeBinaryIndexReader(dir, idxRef)
		if err != nil {
			return nil, err
		}
		indexStat, err := indexFile.Stat()
		_ = indexFile.Close()
		if err != nil {
			return nil, err
		}
		accessorFile, accessorHeader, accessorSize, err := openStateDomainChangeBinaryAccessorReader(dir, accessorRef)
		if err != nil {
			return nil, err
		}
		_ = accessorFile.Close()
		if accessorHeader.count != segmentHeader.count {
			return nil, fmt.Errorf("snapshots: state-domain-change accessor %q count %d, want segment count %d", accessorRef.Path, accessorHeader.count, segmentHeader.count)
		}
		sources = append(sources, stateDomainChangeBinaryCompactionSource{
			history:        candidate.history,
			index:          idxRef,
			accessor:       accessorRef,
			segmentHeader:  segmentHeader,
			indexHeader:    indexHeader,
			accessorHeader: accessorHeader,
			segmentSize:    segmentSize,
			indexSize:      uint64(indexStat.Size()),
			accessorSize:   accessorSize,
		})
	}
	return sources, nil
}

func historyCompactionCompanion(candidate historyCompactionCandidate, kind SegmentKind) (SegmentRef, bool) {
	for _, ref := range candidate.companions {
		if ref.Kind == kind {
			return ref, true
		}
	}
	return SegmentRef{}, false
}

func writeCompactedStateDomainChangeBinarySegment(dir string, cfg DomainCfg, selection historyCompactionSelection, sources []stateDomainChangeBinaryCompactionSource) (SegmentRef, error) {
	ref := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: selection.fromTxNum,
		ToTxNum:   selection.toTxNum,
		Path:      cfg.HistoryPath(selection.fromTxNum, selection.toTxNum),
	}
	totalRecords, err := stateDomainChangeBinaryCompactionRecordCount(sources)
	if err != nil {
		return SegmentRef{}, err
	}
	tmp, tmpName, err := createStateDomainChangeBinaryTempFile(dir, ref.Path)
	if err != nil {
		return SegmentRef{}, err
	}
	defer os.Remove(tmpName)
	if err := writeStateDomainChangeBinaryHeaderTo(tmp, stateDomainChangeBinarySegmentMagic, ref.FromTxNum, ref.ToTxNum, totalRecords); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	var recordIndexBase uint64
	for i := range sources {
		pos, err := tmp.Seek(0, io.SeekCurrent)
		if err != nil {
			_ = tmp.Close()
			return SegmentRef{}, err
		}
		if pos < 0 {
			_ = tmp.Close()
			return SegmentRef{}, fmt.Errorf("snapshots: compacted state-domain-change segment negative offset %d", pos)
		}
		sources[i].payloadBase = uint64(pos)
		sources[i].recordIndexBase = recordIndexBase
		if err := copyStateDomainChangeBinarySegmentPayload(dir, tmp, sources[i]); err != nil {
			_ = tmp.Close()
			return SegmentRef{}, err
		}
		recordIndexBase += sources[i].segmentHeader.count
	}
	size, checksum, err := closeAndHashStateDomainChangeBinaryTemp(tmp, tmpName)
	if err != nil {
		return SegmentRef{}, err
	}
	finalAbs := contentAddressedSnapshotPath(filepath.Join(dir, ref.Path), checksum)
	if err := publishStateDomainChangeBinaryTemp(tmpName, finalAbs); err != nil {
		return SegmentRef{}, err
	}
	rel, err := filepath.Rel(dir, finalAbs)
	if err != nil {
		return SegmentRef{}, err
	}
	ref.Path = filepath.ToSlash(rel)
	ref.Size = size
	ref.Checksum = checksum
	return ref, nil
}

func writeCompactedStateDomainChangeBinaryIndex(dir string, segRef SegmentRef, sources []stateDomainChangeBinaryCompactionSource) (SegmentRef, error) {
	ref := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentInverted,
		FromTxNum: segRef.FromTxNum,
		ToTxNum:   segRef.ToTxNum,
		Path:      stateDomainChangeBinaryIndexPath(segRef.Path),
	}
	totalIndexEntries, err := stateDomainChangeBinaryCompactionIndexCount(sources)
	if err != nil {
		return SegmentRef{}, err
	}
	tmp, tmpName, err := createStateDomainChangeBinaryTempFile(dir, ref.Path)
	if err != nil {
		return SegmentRef{}, err
	}
	defer os.Remove(tmpName)
	if err := writeStateDomainChangeBinaryHeaderTo(tmp, stateDomainChangeBinaryIndexMagic, ref.FromTxNum, ref.ToTxNum, totalIndexEntries); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	for i := range sources {
		if err := writeCompactedStateDomainChangeBinaryIndexEntries(dir, tmp, sources[i]); err != nil {
			_ = tmp.Close()
			return SegmentRef{}, err
		}
	}
	size, checksum, err := closeAndHashStateDomainChangeBinaryTemp(tmp, tmpName)
	if err != nil {
		return SegmentRef{}, err
	}
	if err := publishStateDomainChangeBinaryTemp(tmpName, filepath.Join(dir, ref.Path)); err != nil {
		return SegmentRef{}, err
	}
	ref.Size = size
	ref.Checksum = checksum
	return ref, nil
}

func writeCompactedStateDomainChangeBinaryAccessor(dir string, segRef SegmentRef, sources []stateDomainChangeBinaryCompactionSource) (SegmentRef, error) {
	ref := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentAccessor,
		FromTxNum: segRef.FromTxNum,
		ToTxNum:   segRef.ToTxNum,
		Path:      stateDomainChangeBinaryAccessorPath(segRef.Path),
	}
	totalRecords, err := stateDomainChangeBinaryCompactionRecordCount(sources)
	if err != nil {
		return SegmentRef{}, err
	}
	if totalRecords > (math.MaxUint64-uint64(stateDomainChangeBinaryHeaderSize))/8 {
		return SegmentRef{}, fmt.Errorf("snapshots: compacted state-domain-change accessor count %d overflows offset table", totalRecords)
	}
	tmp, tmpName, err := createStateDomainChangeBinaryTempFile(dir, ref.Path)
	if err != nil {
		return SegmentRef{}, err
	}
	defer os.Remove(tmpName)
	if err := writeStateDomainChangeBinaryHeaderTo(tmp, stateDomainChangeBinaryAccessorMagic, ref.FromTxNum, ref.ToTxNum, totalRecords); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := writeZeroes(tmp, totalRecords*8); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	cursors, err := openStateDomainChangeBinaryCompactionAccessorCursors(dir, sources)
	if err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	defer closeStateDomainChangeBinaryCompactionAccessorCursors(cursors)
	h := stateDomainChangeBinaryCompactionAccessorHeap(cursors)
	heap.Init(&h)
	var outIndex uint64
	for h.Len() > 0 {
		cursor := heap.Pop(&h).(*stateDomainChangeBinaryCompactionAccessorCursor)
		pos, err := tmp.Seek(0, io.SeekCurrent)
		if err != nil {
			_ = tmp.Close()
			return SegmentRef{}, err
		}
		if pos < 0 {
			_ = tmp.Close()
			return SegmentRef{}, fmt.Errorf("snapshots: compacted state-domain-change accessor negative offset %d", pos)
		}
		if err := writeStateDomainChangeBinaryAccessorOffsetAt(tmp, outIndex, uint64(pos)); err != nil {
			_ = tmp.Close()
			return SegmentRef{}, err
		}
		if err := writeStateDomainChangeBinaryAccessorEntryTo(tmp, cursor.mapped); err != nil {
			_ = tmp.Close()
			return SegmentRef{}, err
		}
		outIndex++
		ok, err := cursor.advance()
		if err != nil {
			_ = tmp.Close()
			return SegmentRef{}, err
		}
		if ok {
			heap.Push(&h, cursor)
		}
	}
	if outIndex != totalRecords {
		_ = tmp.Close()
		return SegmentRef{}, fmt.Errorf("snapshots: compacted state-domain-change accessor wrote %d entries, want %d", outIndex, totalRecords)
	}
	size, checksum, err := closeAndHashStateDomainChangeBinaryTemp(tmp, tmpName)
	if err != nil {
		return SegmentRef{}, err
	}
	if err := publishStateDomainChangeBinaryTemp(tmpName, filepath.Join(dir, ref.Path)); err != nil {
		return SegmentRef{}, err
	}
	ref.Size = size
	ref.Checksum = checksum
	return ref, nil
}

func stateDomainChangeBinaryCompactionRecordCount(sources []stateDomainChangeBinaryCompactionSource) (uint64, error) {
	var total uint64
	for _, source := range sources {
		if source.segmentHeader.count > math.MaxUint64-total {
			return 0, fmt.Errorf("snapshots: compacted state-domain-change record count overflows")
		}
		total += source.segmentHeader.count
	}
	return total, nil
}

func stateDomainChangeBinaryCompactionIndexCount(sources []stateDomainChangeBinaryCompactionSource) (uint64, error) {
	var total uint64
	for _, source := range sources {
		if source.indexHeader.count > math.MaxUint64-total {
			return 0, fmt.Errorf("snapshots: compacted state-domain-change index count overflows")
		}
		total += source.indexHeader.count
	}
	return total, nil
}

func copyStateDomainChangeBinarySegmentPayload(dir string, dst *os.File, source stateDomainChangeBinaryCompactionSource) error {
	if source.segmentSize < stateDomainChangeBinaryHeaderSize {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q size %d below header size", source.history.Path, source.segmentSize)
	}
	payloadSize := source.segmentSize - uint64(stateDomainChangeBinaryHeaderSize)
	if payloadSize > math.MaxInt64 {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q payload too large: %d", source.history.Path, payloadSize)
	}
	src, err := os.Open(filepath.Join(dir, source.history.Path))
	if err != nil {
		return err
	}
	defer src.Close()
	if _, err := src.Seek(stateDomainChangeBinaryHeaderSize, io.SeekStart); err != nil {
		return err
	}
	written, err := io.CopyN(dst, src, int64(payloadSize))
	if err != nil {
		return err
	}
	if uint64(written) != payloadSize {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func writeCompactedStateDomainChangeBinaryIndexEntries(dir string, dst *os.File, source stateDomainChangeBinaryCompactionSource) error {
	segmentFile, _, segmentSize, err := openStateDomainChangeBinarySegmentReader(dir, source.history)
	if err != nil {
		return err
	}
	defer segmentFile.Close()
	indexFile, indexHeader, err := openStateDomainChangeBinaryIndexReader(dir, source.index)
	if err != nil {
		return err
	}
	defer indexFile.Close()
	if indexHeader.count != source.indexHeader.count {
		return fmt.Errorf("snapshots: state-domain-change binary index %q count changed during compaction", source.index.Path)
	}
	for i := uint64(0); i < indexHeader.count; i++ {
		entry, err := readStateDomainChangeBinaryIndexEntryAt(indexFile, i)
		if err != nil {
			return err
		}
		if err := validateStateDomainChangeBinaryIndexEntryAgainstSegment(source, segmentFile, segmentSize, entry); err != nil {
			return err
		}
		mappedOffset, err := mapStateDomainChangeBinaryCompactionOffset(source, entry.offset)
		if err != nil {
			return err
		}
		recordIndex, err := mapStateDomainChangeBinaryCompactionRecordIndex(source, entry.recordIndex)
		if err != nil {
			return err
		}
		entry.offset = mappedOffset
		entry.recordIndex = recordIndex
		if err := writeStateDomainChangeBinaryIndexEntryTo(dst, entry); err != nil {
			return err
		}
	}
	return nil
}

func validateStateDomainChangeBinaryIndexEntryAgainstSegment(source stateDomainChangeBinaryCompactionSource, segment io.ReaderAt, segmentSize uint64, entry stateDomainChangeBinaryTxOffset) error {
	if entry.offset < stateDomainChangeBinaryHeaderSize || entry.offset >= segmentSize {
		return fmt.Errorf("snapshots: state-domain-change binary index %q entry offset %d outside segment %q", source.index.Path, entry.offset, source.history.Path)
	}
	change, _, err := readStateDomainChangeBinaryRecordAtBounded(segment, entry.offset, segmentSize)
	if err != nil {
		return err
	}
	if change.TxNum != entry.txNum {
		return fmt.Errorf("snapshots: state-domain-change binary index %q tx %d read segment tx %d", source.index.Path, entry.txNum, change.TxNum)
	}
	return nil
}

type stateDomainChangeBinaryCompactionAccessorCursor struct {
	source   stateDomainChangeBinaryCompactionSource
	segment  *os.File
	accessor historySegmentReader
	index    uint64
	entry    stateDomainChangeBinaryAccessorEntry
	mapped   stateDomainChangeBinaryAccessorEntry
	closed   bool
}

func openStateDomainChangeBinaryCompactionAccessorCursors(dir string, sources []stateDomainChangeBinaryCompactionSource) ([]*stateDomainChangeBinaryCompactionAccessorCursor, error) {
	cursors := make([]*stateDomainChangeBinaryCompactionAccessorCursor, 0, len(sources))
	for _, source := range sources {
		if source.accessorHeader.count == 0 {
			continue
		}
		segmentFile, _, segmentSize, err := openStateDomainChangeBinarySegmentReader(dir, source.history)
		if err != nil {
			closeStateDomainChangeBinaryCompactionAccessorCursors(cursors)
			return nil, err
		}
		if segmentSize != source.segmentSize {
			_ = segmentFile.Close()
			closeStateDomainChangeBinaryCompactionAccessorCursors(cursors)
			return nil, fmt.Errorf("snapshots: state-domain-change binary segment %q size changed during compaction", source.history.Path)
		}
		accessorFile, accessorHeader, _, err := openStateDomainChangeBinaryAccessorReader(dir, source.accessor)
		if err != nil {
			_ = segmentFile.Close()
			closeStateDomainChangeBinaryCompactionAccessorCursors(cursors)
			return nil, err
		}
		if accessorHeader.count != source.accessorHeader.count {
			_ = segmentFile.Close()
			_ = accessorFile.Close()
			closeStateDomainChangeBinaryCompactionAccessorCursors(cursors)
			return nil, fmt.Errorf("snapshots: state-domain-change binary accessor %q count changed during compaction", source.accessor.Path)
		}
		cursor := &stateDomainChangeBinaryCompactionAccessorCursor{
			source:   source,
			segment:  segmentFile,
			accessor: accessorFile,
		}
		ok, err := cursor.advance()
		if err != nil {
			_ = cursor.close()
			closeStateDomainChangeBinaryCompactionAccessorCursors(cursors)
			return nil, err
		}
		if ok {
			cursors = append(cursors, cursor)
		} else {
			_ = cursor.close()
		}
	}
	return cursors, nil
}

func closeStateDomainChangeBinaryCompactionAccessorCursors(cursors []*stateDomainChangeBinaryCompactionAccessorCursor) {
	for _, cursor := range cursors {
		_ = cursor.close()
	}
}

func (c *stateDomainChangeBinaryCompactionAccessorCursor) advance() (bool, error) {
	if c.index >= c.source.accessorHeader.count {
		return false, nil
	}
	entry, err := readStateDomainChangeBinaryAccessorEntryAtBounded(c.accessor, c.index, c.source.accessorSize)
	if err != nil {
		return false, err
	}
	if err := validateStateDomainChangeBinaryAccessorEntryAgainstSegment(c.source, c.segment, c.source.segmentSize, entry); err != nil {
		return false, err
	}
	mapped, err := mapStateDomainChangeBinaryCompactionAccessorEntry(c.source, entry)
	if err != nil {
		return false, err
	}
	c.entry = entry
	c.mapped = mapped
	c.index++
	return true, nil
}

func (c *stateDomainChangeBinaryCompactionAccessorCursor) close() error {
	if c == nil || c.closed {
		return nil
	}
	c.closed = true
	var err error
	if c.segment != nil {
		err = c.segment.Close()
		c.segment = nil
	}
	if c.accessor != nil {
		if closeErr := c.accessor.Close(); err == nil {
			err = closeErr
		}
		c.accessor = nil
	}
	return err
}

type stateDomainChangeBinaryCompactionAccessorHeap []*stateDomainChangeBinaryCompactionAccessorCursor

func (h stateDomainChangeBinaryCompactionAccessorHeap) Len() int { return len(h) }

func (h stateDomainChangeBinaryCompactionAccessorHeap) Less(i, j int) bool {
	return compareStateDomainChangeBinaryAccessorEntry(h[i].mapped, h[j].mapped) < 0
}

func (h stateDomainChangeBinaryCompactionAccessorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *stateDomainChangeBinaryCompactionAccessorHeap) Push(x interface{}) {
	*h = append(*h, x.(*stateDomainChangeBinaryCompactionAccessorCursor))
}

func (h *stateDomainChangeBinaryCompactionAccessorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func validateStateDomainChangeBinaryAccessorEntryAgainstSegment(source stateDomainChangeBinaryCompactionSource, segment io.ReaderAt, segmentSize uint64, entry stateDomainChangeBinaryAccessorEntry) error {
	if err := validateStateDomainChangeBinaryAccessorEntry(source.accessor, entry, entry.recordIndex); err != nil {
		return err
	}
	if entry.offset < stateDomainChangeBinaryHeaderSize || entry.offset >= segmentSize {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q entry offset %d outside segment %q", source.accessor.Path, entry.offset, source.history.Path)
	}
	change, _, err := readStateDomainChangeBinaryRecordAtBounded(segment, entry.offset, segmentSize)
	if err != nil {
		return err
	}
	if change.TxNum != entry.txNum || change.Seq != entry.seq {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q entry tx/seq [%d,%d] read record [%d,%d]", source.accessor.Path, entry.txNum, entry.seq, change.TxNum, change.Seq)
	}
	if !bytes.Equal(stateDomainChangeBinaryAccessorKey(change), entry.key) {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q key mismatch at offset %d", source.accessor.Path, entry.offset)
	}
	return nil
}

func mapStateDomainChangeBinaryCompactionAccessorEntry(source stateDomainChangeBinaryCompactionSource, entry stateDomainChangeBinaryAccessorEntry) (stateDomainChangeBinaryAccessorEntry, error) {
	offset, err := mapStateDomainChangeBinaryCompactionOffset(source, entry.offset)
	if err != nil {
		return stateDomainChangeBinaryAccessorEntry{}, err
	}
	recordIndex, err := mapStateDomainChangeBinaryCompactionRecordIndex(source, entry.recordIndex)
	if err != nil {
		return stateDomainChangeBinaryAccessorEntry{}, err
	}
	return stateDomainChangeBinaryAccessorEntry{
		key:         append([]byte(nil), entry.key...),
		txNum:       entry.txNum,
		seq:         entry.seq,
		offset:      offset,
		recordIndex: recordIndex,
	}, nil
}

func mapStateDomainChangeBinaryCompactionOffset(source stateDomainChangeBinaryCompactionSource, offset uint64) (uint64, error) {
	if offset < stateDomainChangeBinaryHeaderSize || offset >= source.segmentSize {
		return 0, fmt.Errorf("snapshots: state-domain-change binary segment %q offset %d outside payload", source.history.Path, offset)
	}
	relative := offset - uint64(stateDomainChangeBinaryHeaderSize)
	if relative > math.MaxUint64-source.payloadBase {
		return 0, fmt.Errorf("snapshots: compacted state-domain-change offset overflows")
	}
	return source.payloadBase + relative, nil
}

func mapStateDomainChangeBinaryCompactionRecordIndex(source stateDomainChangeBinaryCompactionSource, recordIndex uint64) (uint64, error) {
	if recordIndex >= source.segmentHeader.count {
		return 0, fmt.Errorf("snapshots: state-domain-change binary segment %q record index %d outside count %d", source.history.Path, recordIndex, source.segmentHeader.count)
	}
	if recordIndex > math.MaxUint64-source.recordIndexBase {
		return 0, fmt.Errorf("snapshots: compacted state-domain-change record index overflows")
	}
	return source.recordIndexBase + recordIndex, nil
}

func createStateDomainChangeBinaryTempFile(dir, relPath string) (*os.File, string, error) {
	abs := filepath.Join(dir, relPath)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return nil, "", err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".*.tmp")
	if err != nil {
		return nil, "", err
	}
	return tmp, tmp.Name(), nil
}

func closeAndHashStateDomainChangeBinaryTemp(file *os.File, tmpName string) (uint64, string, error) {
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return 0, "", err
	}
	if err := file.Close(); err != nil {
		return 0, "", err
	}
	return stateDomainChangeBinaryFileMetadata(tmpName)
}

func publishStateDomainChangeBinaryTemp(tmpName, finalAbs string) error {
	if err := os.MkdirAll(filepath.Dir(finalAbs), 0o755); err != nil {
		return err
	}
	return os.Rename(tmpName, finalAbs)
}

func stateDomainChangeBinaryFileMetadata(path string) (uint64, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer file.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		return 0, "", err
	}
	return uint64(size), "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func writeStateDomainChangeBinaryHeaderTo(w io.Writer, magic [8]byte, fromTxNum, toTxNum, count uint64) error {
	var header [stateDomainChangeBinaryHeaderSize]byte
	copy(header[:8], magic[:])
	binary.BigEndian.PutUint32(header[8:12], stateDomainChangeBinaryVersion)
	binary.BigEndian.PutUint64(header[12:20], fromTxNum)
	binary.BigEndian.PutUint64(header[20:28], toTxNum)
	binary.BigEndian.PutUint64(header[28:36], count)
	_, err := w.Write(header[:])
	return err
}

func writeStateDomainChangeBinaryIndexEntryTo(w io.Writer, entry stateDomainChangeBinaryTxOffset) error {
	var raw [stateDomainChangeBinaryIndexEntrySize]byte
	binary.BigEndian.PutUint64(raw[0:8], entry.txNum)
	binary.BigEndian.PutUint64(raw[8:16], entry.offset)
	binary.BigEndian.PutUint64(raw[16:24], entry.recordIndex)
	binary.BigEndian.PutUint64(raw[24:32], entry.count)
	_, err := w.Write(raw[:])
	return err
}

func writeStateDomainChangeBinaryAccessorOffsetAt(file *os.File, index, offset uint64) error {
	if index > (math.MaxInt64-stateDomainChangeBinaryHeaderSize)/8 {
		return fmt.Errorf("snapshots: state-domain-change accessor index too large: %d", index)
	}
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], offset)
	_, err := file.WriteAt(raw[:], int64(stateDomainChangeBinaryHeaderSize+index*8))
	return err
}

func writeStateDomainChangeBinaryAccessorEntryTo(w io.Writer, entry stateDomainChangeBinaryAccessorEntry) error {
	if len(entry.key) > math.MaxUint32 {
		return fmt.Errorf("snapshots: state-domain-change accessor key is too large: %d bytes", len(entry.key))
	}
	var head [4]byte
	binary.BigEndian.PutUint32(head[:], uint32(len(entry.key)))
	if _, err := w.Write(head[:]); err != nil {
		return err
	}
	if _, err := w.Write(entry.key); err != nil {
		return err
	}
	var ints [stateDomainChangeBinaryAccessorInts]byte
	binary.BigEndian.PutUint64(ints[0:8], entry.txNum)
	binary.BigEndian.PutUint64(ints[8:16], entry.seq)
	binary.BigEndian.PutUint64(ints[16:24], entry.offset)
	binary.BigEndian.PutUint64(ints[24:32], entry.recordIndex)
	_, err := w.Write(ints[:])
	return err
}

func writeZeroes(w io.Writer, n uint64) error {
	var zero [32 * 1024]byte
	for n > 0 {
		chunk := uint64(len(zero))
		if n < chunk {
			chunk = n
		}
		if _, err := w.Write(zero[:chunk]); err != nil {
			return err
		}
		n -= chunk
	}
	return nil
}

func readStateDomainChangeBinarySegment(dir string, ref SegmentRef) ([]*rawdb.StateDomainChange, error) {
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
		return nil, fmt.Errorf("snapshots: state-domain-change binary segment %q is %s/%s, want state-domain-change/history", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, err
	}
	if err := verifyStateDomainChangeBinaryRef(ref, data); err != nil {
		return nil, err
	}
	header, rest, err := decodeStateDomainChangeBinaryHeader(data, stateDomainChangeBinarySegmentMagic)
	if err != nil {
		return nil, err
	}
	if header.fromTxNum != ref.FromTxNum || header.toTxNum != ref.ToTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change binary segment %q range [%d,%d], want [%d,%d]", ref.Path, header.fromTxNum, header.toTxNum, ref.FromTxNum, ref.ToTxNum)
	}
	if header.count > uint64(len(rest))/4 {
		return nil, fmt.Errorf("snapshots: state-domain-change binary segment %q record count %d exceeds payload size %d", ref.Path, header.count, len(rest))
	}
	changes := make([]*rawdb.StateDomainChange, 0, header.count)
	for i := uint64(0); i < header.count; i++ {
		payload, next, err := decodeStateDomainChangeBinaryRecordFrame(rest)
		if err != nil {
			return nil, fmt.Errorf("snapshots: decode state-domain-change binary record %d: %w", i, err)
		}
		change, err := decodeStateDomainChangeRecord(payload)
		if err != nil {
			return nil, fmt.Errorf("snapshots: decode state-domain-change binary record %d: %w", i, err)
		}
		changes = append(changes, change)
		rest = next
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("snapshots: state-domain-change binary segment %q has %d trailing bytes", ref.Path, len(rest))
	}
	if err := validateStateDomainChangeBinaryRecords(ref.FromTxNum, ref.ToTxNum, changes); err != nil {
		return nil, err
	}
	return changes, nil
}

func readStateDomainChangeBinaryIndex(dir string, ref SegmentRef) ([]stateDomainChangeBinaryTxOffset, error) {
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentInverted {
		return nil, fmt.Errorf("snapshots: state-domain-change binary index %q is %s/%s, want state-domain-change/inverted", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, err
	}
	if err := verifyStateDomainChangeBinaryRef(ref, data); err != nil {
		return nil, err
	}
	header, rest, err := decodeStateDomainChangeBinaryHeader(data, stateDomainChangeBinaryIndexMagic)
	if err != nil {
		return nil, err
	}
	if header.fromTxNum != ref.FromTxNum || header.toTxNum != ref.ToTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change binary index %q range [%d,%d], want [%d,%d]", ref.Path, header.fromTxNum, header.toTxNum, ref.FromTxNum, ref.ToTxNum)
	}
	entrySize := uint64(stateDomainChangeBinaryIndexEntrySize)
	if header.count > uint64(len(rest))/entrySize {
		return nil, fmt.Errorf("snapshots: state-domain-change binary index %q entry count %d exceeds payload size %d", ref.Path, header.count, len(rest))
	}
	if uint64(len(rest)) != header.count*entrySize {
		return nil, fmt.Errorf("snapshots: state-domain-change binary index %q payload size %d, want %d", ref.Path, len(rest), header.count*entrySize)
	}
	index := make([]stateDomainChangeBinaryTxOffset, 0, header.count)
	for i := uint64(0); i < header.count; i++ {
		entry := stateDomainChangeBinaryTxOffset{
			txNum:       binary.BigEndian.Uint64(rest[0:8]),
			offset:      binary.BigEndian.Uint64(rest[8:16]),
			recordIndex: binary.BigEndian.Uint64(rest[16:24]),
			count:       binary.BigEndian.Uint64(rest[24:32]),
		}
		if entry.count == 0 {
			return nil, fmt.Errorf("snapshots: state-domain-change binary index %q entry %d has zero count", ref.Path, i)
		}
		if entry.txNum < ref.FromTxNum || entry.txNum > ref.ToTxNum {
			return nil, fmt.Errorf("snapshots: state-domain-change binary index %q tx %d outside range [%d,%d]", ref.Path, entry.txNum, ref.FromTxNum, ref.ToTxNum)
		}
		if i > 0 && entry.txNum <= index[i-1].txNum {
			return nil, fmt.Errorf("snapshots: state-domain-change binary index %q entries are not sorted", ref.Path)
		}
		index = append(index, entry)
		rest = rest[stateDomainChangeBinaryIndexEntrySize:]
	}
	return index, nil
}

func checkStateDomainChangeBinaryIndex(dir string, ref SegmentRef) error {
	indexFile, header, err := openStateDomainChangeBinaryIndexReader(dir, ref)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	if ref.Checksum != "" {
		if _, err := indexFile.Seek(0, io.SeekStart); err != nil {
			return err
		}
		hash := sha256.New()
		if _, err := io.Copy(hash, indexFile); err != nil {
			return err
		}
		if got := "sha256:" + hex.EncodeToString(hash.Sum(nil)); got != ref.Checksum {
			return fmt.Errorf("snapshots: segment %q checksum %s, want %s", ref.Path, got, ref.Checksum)
		}
	}

	var prev stateDomainChangeBinaryTxOffset
	for i := uint64(0); i < header.count; i++ {
		entry, err := readStateDomainChangeBinaryIndexEntryAt(indexFile, i)
		if err != nil {
			return err
		}
		if entry.count == 0 {
			return fmt.Errorf("snapshots: state-domain-change binary index %q entry %d has zero count", ref.Path, i)
		}
		if entry.txNum < ref.FromTxNum || entry.txNum > ref.ToTxNum {
			return fmt.Errorf("snapshots: state-domain-change binary index %q tx %d outside range [%d,%d]", ref.Path, entry.txNum, ref.FromTxNum, ref.ToTxNum)
		}
		if i > 0 && entry.txNum <= prev.txNum {
			return fmt.Errorf("snapshots: state-domain-change binary index %q entries are not sorted", ref.Path)
		}
		prev = entry
	}
	return nil
}

func checkStateDomainChangeBinarySegment(dir string, ref SegmentRef) error {
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q is %s/%s, want state-domain-change/history", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return err
	}
	segmentFile, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return err
	}
	defer segmentFile.Close()

	stat, err := segmentFile.Stat()
	if err != nil {
		return err
	}
	fileSize := uint64(stat.Size())
	if ref.Size != 0 && fileSize != ref.Size {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q size %d, want %d", ref.Path, stat.Size(), ref.Size)
	}
	if ref.Checksum != "" {
		hash := sha256.New()
		if _, err := io.Copy(hash, segmentFile); err != nil {
			return err
		}
		if got := "sha256:" + hex.EncodeToString(hash.Sum(nil)); got != ref.Checksum {
			return fmt.Errorf("snapshots: segment %q checksum %s, want %s", ref.Path, got, ref.Checksum)
		}
	}

	header, err := readStateDomainChangeBinaryHeaderAt(segmentFile, stateDomainChangeBinarySegmentMagic)
	if err != nil {
		return err
	}
	if header.fromTxNum != ref.FromTxNum || header.toTxNum != ref.ToTxNum {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q range [%d,%d], want [%d,%d]", ref.Path, header.fromTxNum, header.toTxNum, ref.FromTxNum, ref.ToTxNum)
	}
	if fileSize < stateDomainChangeBinaryHeaderSize {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q size %d below header size %d", ref.Path, fileSize, stateDomainChangeBinaryHeaderSize)
	}
	if header.count > (math.MaxUint64-uint64(stateDomainChangeBinaryHeaderSize))/4 {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q count %d overflows size", ref.Path, header.count)
	}
	minSize := uint64(stateDomainChangeBinaryHeaderSize) + header.count*4
	if fileSize < minSize {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q size %d below record table size %d", ref.Path, fileSize, minSize)
	}
	if _, err := segmentFile.Seek(stateDomainChangeBinaryHeaderSize, io.SeekStart); err != nil {
		return err
	}

	var prevTxNum uint64
	var prevSeq uint64
	for i := uint64(0); i < header.count; i++ {
		pos, err := segmentFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if pos < 0 {
			return fmt.Errorf("snapshots: state-domain-change binary segment %q negative offset %d", ref.Path, pos)
		}
		if uint64(pos) > fileSize-4 {
			return io.ErrUnexpectedEOF
		}
		change, err := readStateDomainChangeBinaryRecordFrom(segmentFile, fileSize-uint64(pos)-4)
		if err != nil {
			return fmt.Errorf("snapshots: decode state-domain-change binary record %d: %w", i, err)
		}
		if change.TxNum < ref.FromTxNum || change.TxNum > ref.ToTxNum {
			return fmt.Errorf("snapshots: state-domain-change tx %d outside segment range [%d,%d]", change.TxNum, ref.FromTxNum, ref.ToTxNum)
		}
		if i > 0 && (change.TxNum < prevTxNum || (change.TxNum == prevTxNum && change.Seq < prevSeq)) {
			return errors.New("snapshots: state-domain-change entries are not sorted")
		}
		prevTxNum = change.TxNum
		prevSeq = change.Seq
	}
	pos, err := segmentFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if pos < 0 {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q negative offset %d", ref.Path, pos)
	}
	if uint64(pos) != fileSize {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q has %d trailing bytes", ref.Path, fileSize-uint64(pos))
	}
	return nil
}

func readStateDomainChangeBinaryAccessor(dir string, ref SegmentRef) ([]stateDomainChangeBinaryAccessorEntry, error) {
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentAccessor {
		return nil, fmt.Errorf("snapshots: state-domain-change binary accessor %q is %s/%s, want state-domain-change/accessor", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, err
	}
	if err := verifyStateDomainChangeBinaryRef(ref, data); err != nil {
		return nil, err
	}
	header, rest, err := decodeStateDomainChangeBinaryHeader(data, stateDomainChangeBinaryAccessorMagic)
	if err != nil {
		return nil, err
	}
	if header.fromTxNum != ref.FromTxNum || header.toTxNum != ref.ToTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change binary accessor %q range [%d,%d], want [%d,%d]", ref.Path, header.fromTxNum, header.toTxNum, ref.FromTxNum, ref.ToTxNum)
	}
	offsetTableLen := header.count * 8
	if header.count > uint64(len(rest))/8 {
		return nil, fmt.Errorf("snapshots: state-domain-change binary accessor %q offset table count %d exceeds payload size %d", ref.Path, header.count, len(rest))
	}
	if uint64(len(rest)) < offsetTableLen {
		return nil, io.ErrUnexpectedEOF
	}
	offsetTable := rest[:offsetTableLen]
	entries := make([]stateDomainChangeBinaryAccessorEntry, 0, header.count)
	var prevEntryOffset uint64
	var maxNext uint64
	for i := uint64(0); i < header.count; i++ {
		entryOffset := binary.BigEndian.Uint64(offsetTable[i*8 : i*8+8])
		minOffset := uint64(stateDomainChangeBinaryHeaderSize) + offsetTableLen
		if entryOffset < minOffset || entryOffset >= uint64(len(data)) {
			return nil, fmt.Errorf("snapshots: state-domain-change binary accessor %q entry %d offset %d outside payload", ref.Path, i, entryOffset)
		}
		if i > 0 && entryOffset <= prevEntryOffset {
			return nil, fmt.Errorf("snapshots: state-domain-change binary accessor %q entry offsets are not strictly increasing", ref.Path)
		}
		entry, next, err := decodeStateDomainChangeBinaryAccessorEntryAt(data, entryOffset)
		if err != nil {
			return nil, err
		}
		if err := validateStateDomainChangeBinaryAccessorEntry(ref, entry, i); err != nil {
			return nil, err
		}
		if i > 0 {
			prev := entries[len(entries)-1]
			cmp := compareStateDomainChangeBinaryAccessorEntry(prev, entry)
			if cmp > 0 {
				return nil, fmt.Errorf("snapshots: state-domain-change binary accessor %q entries are not sorted", ref.Path)
			}
			if bytes.Equal(prev.key, entry.key) && entry.offset <= prev.offset {
				return nil, fmt.Errorf("snapshots: state-domain-change binary accessor %q offsets are not monotonic for key", ref.Path)
			}
		}
		entries = append(entries, entry)
		prevEntryOffset = entryOffset
		if next > maxNext {
			maxNext = next
		}
	}
	if header.count == 0 {
		if len(rest) != 0 {
			return nil, fmt.Errorf("snapshots: state-domain-change binary accessor %q has %d trailing bytes", ref.Path, len(rest))
		}
		return entries, nil
	}
	if maxNext != uint64(len(data)) {
		return nil, fmt.Errorf("snapshots: state-domain-change binary accessor %q has trailing bytes after offset table entries", ref.Path)
	}
	return entries, nil
}

func checkStateDomainChangeBinaryAccessor(dir string, ref SegmentRef) error {
	// Entry validation runs over the logical (uncompressed) view; the checksum is
	// over the physical (possibly compressed) file bytes.
	accessorFile, header, fileSize, err := openStateDomainChangeBinaryAccessorReader(dir, ref)
	if err != nil {
		return err
	}
	defer accessorFile.Close()

	if ref.Checksum != "" {
		_, got, err := stateDomainChangeBinaryFileMetadata(filepath.Join(dir, ref.Path))
		if err != nil {
			return err
		}
		if got != ref.Checksum {
			return fmt.Errorf("snapshots: segment %q checksum %s, want %s", ref.Path, got, ref.Checksum)
		}
	}

	offsetTableLen := header.count * 8
	minOffset := uint64(stateDomainChangeBinaryHeaderSize) + offsetTableLen
	if fileSize < minOffset {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q size %d below offset table size %d", ref.Path, fileSize, minOffset)
	}
	if header.count == 0 {
		if fileSize != uint64(stateDomainChangeBinaryHeaderSize) {
			return fmt.Errorf("snapshots: state-domain-change binary accessor %q has %d trailing bytes", ref.Path, fileSize-uint64(stateDomainChangeBinaryHeaderSize))
		}
		return nil
	}

	var prev stateDomainChangeBinaryAccessorEntry
	var prevOffset uint64
	var maxNext uint64
	for i := uint64(0); i < header.count; i++ {
		entryOffset, err := readStateDomainChangeBinaryAccessorEntryOffsetAt(accessorFile, i)
		if err != nil {
			return err
		}
		if entryOffset < minOffset || entryOffset >= fileSize {
			return fmt.Errorf("snapshots: state-domain-change binary accessor %q entry %d offset %d outside payload", ref.Path, i, entryOffset)
		}
		if i > 0 && entryOffset <= prevOffset {
			return fmt.Errorf("snapshots: state-domain-change binary accessor %q entry offsets are not strictly increasing", ref.Path)
		}
		entry, next, err := readStateDomainChangeBinaryAccessorEntryAtOffsetWithNextBounded(accessorFile, entryOffset, fileSize)
		if err != nil {
			return err
		}
		if err := validateStateDomainChangeBinaryAccessorEntry(ref, entry, i); err != nil {
			return err
		}
		if i > 0 {
			cmp := compareStateDomainChangeBinaryAccessorEntry(prev, entry)
			if cmp > 0 {
				return fmt.Errorf("snapshots: state-domain-change binary accessor %q entries are not sorted", ref.Path)
			}
			if bytes.Equal(prev.key, entry.key) && entry.offset <= prev.offset {
				return fmt.Errorf("snapshots: state-domain-change binary accessor %q offsets are not monotonic for key", ref.Path)
			}
		}
		prev = entry
		prevOffset = entryOffset
		if next > maxNext {
			maxNext = next
		}
	}
	if maxNext != fileSize {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q has trailing bytes after offset table entries", ref.Path)
	}
	return nil
}

func readStateDomainChangeBinarySegmentTxRange(dir string, ref SegmentRef, index []stateDomainChangeBinaryTxOffset, fromTxNum, toTxNum uint64) ([]*rawdb.StateDomainChange, error) {
	if toTxNum < fromTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
		return nil, fmt.Errorf("snapshots: state-domain-change binary segment %q is %s/%s, want state-domain-change/history", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return nil, err
	}
	file, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := uint64(stat.Size())
	if ref.Size != 0 {
		if uint64(stat.Size()) != ref.Size {
			return nil, fmt.Errorf("snapshots: state-domain-change binary segment %q size %d, want %d", ref.Path, stat.Size(), ref.Size)
		}
	}
	header, err := readStateDomainChangeBinaryHeaderAt(file, stateDomainChangeBinarySegmentMagic)
	if err != nil {
		return nil, err
	}
	if header.fromTxNum != ref.FromTxNum || header.toTxNum != ref.ToTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change binary segment %q range [%d,%d], want [%d,%d]", ref.Path, header.fromTxNum, header.toTxNum, ref.FromTxNum, ref.ToTxNum)
	}

	var changes []*rawdb.StateDomainChange
	for _, entry := range index {
		if entry.txNum < fromTxNum || entry.txNum > toTxNum {
			continue
		}
		offset := entry.offset
		for i := uint64(0); i < entry.count; i++ {
			change, nextOffset, err := readStateDomainChangeBinaryRecordAtBounded(file, offset, fileSize)
			if err != nil {
				return nil, err
			}
			if change.TxNum < fromTxNum || change.TxNum > toTxNum {
				return nil, fmt.Errorf("snapshots: state-domain-change binary index entry for tx %d read record tx %d", entry.txNum, change.TxNum)
			}
			changes = append(changes, change)
			offset = nextOffset
		}
	}
	return changes, nil
}

func readStateDomainChangeBinarySegmentTxRangeByIndexFile(dir string, ref SegmentRef, indexRef SegmentRef, fromTxNum, toTxNum uint64) ([]*rawdb.StateDomainChange, error) {
	var changes []*rawdb.StateDomainChange
	err := iterateStateDomainChangeBinarySegmentTxRangeByIndexFile(dir, ref, indexRef, fromTxNum, toTxNum, func(change *rawdb.StateDomainChange) (bool, error) {
		changes = append(changes, change)
		return true, nil
	})
	return changes, err
}

func iterateStateDomainChangeBinarySegmentTxRangeByIndexFile(dir string, ref SegmentRef, indexRef SegmentRef, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	if toTxNum < fromTxNum {
		return fmt.Errorf("snapshots: state-domain-change range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q is %s/%s, want state-domain-change/history", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return err
	}
	segmentFile, segmentSize, _, err := openHistorySegmentForRead(dir, ref)
	if err != nil {
		return err
	}
	defer segmentFile.Close()

	indexFile, indexHeader, err := openStateDomainChangeBinaryIndexReader(dir, indexRef)
	if err != nil {
		return err
	}
	defer indexFile.Close()
	if indexHeader.fromTxNum != ref.FromTxNum || indexHeader.toTxNum != ref.ToTxNum {
		return fmt.Errorf("snapshots: state-domain-change binary index %q range [%d,%d], want [%d,%d]", indexRef.Path, indexHeader.fromTxNum, indexHeader.toTxNum, ref.FromTxNum, ref.ToTxNum)
	}

	start, ok, err := stateDomainChangeBinaryIndexLowerBound(indexFile, indexHeader.count, fromTxNum)
	if err != nil || !ok {
		return err
	}
	for i := start; i < indexHeader.count; i++ {
		entry, err := readStateDomainChangeBinaryIndexEntryAt(indexFile, i)
		if err != nil {
			return err
		}
		if entry.txNum > toTxNum {
			return nil
		}
		offset := entry.offset
		for j := uint64(0); j < entry.count; j++ {
			change, nextOffset, err := readStateDomainChangeBinaryRecordAtBounded(segmentFile, offset, segmentSize)
			if err != nil {
				return err
			}
			if change.TxNum != entry.txNum {
				return fmt.Errorf("snapshots: state-domain-change binary index entry for tx %d read record tx %d", entry.txNum, change.TxNum)
			}
			if change.TxNum >= fromTxNum && change.TxNum <= toTxNum {
				cont, err := fn(change)
				if err != nil || !cont {
					return err
				}
			}
			offset = nextOffset
		}
	}
	return nil
}

func readStateDomainChangeBinaryTxRangeForBlockByIndexFile(dir string, ref SegmentRef, indexRef SegmentRef, blockNum uint64) (*rawdb.StateTxRange, bool, error) {
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
		return nil, false, fmt.Errorf("snapshots: state-domain-change binary segment %q is %s/%s, want state-domain-change/history", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return nil, false, err
	}
	segmentFile, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, false, err
	}
	defer segmentFile.Close()
	stat, err := segmentFile.Stat()
	if err != nil {
		return nil, false, err
	}
	segmentSize := uint64(stat.Size())
	if ref.Size != 0 {
		if uint64(stat.Size()) != ref.Size {
			return nil, false, fmt.Errorf("snapshots: state-domain-change binary segment %q size %d, want %d", ref.Path, stat.Size(), ref.Size)
		}
	}
	segmentHeader, err := readStateDomainChangeBinaryHeaderAt(segmentFile, stateDomainChangeBinarySegmentMagic)
	if err != nil {
		return nil, false, err
	}
	if segmentHeader.fromTxNum != ref.FromTxNum || segmentHeader.toTxNum != ref.ToTxNum {
		return nil, false, fmt.Errorf("snapshots: state-domain-change binary segment %q range [%d,%d], want [%d,%d]", ref.Path, segmentHeader.fromTxNum, segmentHeader.toTxNum, ref.FromTxNum, ref.ToTxNum)
	}

	indexFile, indexHeader, err := openStateDomainChangeBinaryIndexReader(dir, indexRef)
	if err != nil {
		return nil, false, err
	}
	defer indexFile.Close()
	if indexHeader.fromTxNum != ref.FromTxNum || indexHeader.toTxNum != ref.ToTxNum {
		return nil, false, fmt.Errorf("snapshots: state-domain-change binary index %q range [%d,%d], want [%d,%d]", indexRef.Path, indexHeader.fromTxNum, indexHeader.toTxNum, ref.FromTxNum, ref.ToTxNum)
	}

	start, ok, err := stateDomainChangeBinaryIndexBlockLowerBound(segmentFile, segmentSize, indexFile, indexHeader.count, blockNum)
	if err != nil || !ok {
		return nil, ok, err
	}
	var row *rawdb.StateTxRange
	for i := start; i < indexHeader.count; i++ {
		entry, err := readStateDomainChangeBinaryIndexEntryAt(indexFile, i)
		if err != nil {
			return nil, false, err
		}
		offset := entry.offset
		for j := uint64(0); j < entry.count; j++ {
			change, nextOffset, err := readStateDomainChangeBinaryRecordAtBounded(segmentFile, offset, segmentSize)
			if err != nil {
				return nil, false, err
			}
			if change.TxNum != entry.txNum {
				return nil, false, fmt.Errorf("snapshots: state-domain-change binary index entry for tx %d read record tx %d", entry.txNum, change.TxNum)
			}
			if change.BlockNum > blockNum {
				if row != nil {
					return row, true, nil
				}
				return nil, false, nil
			}
			if change.BlockNum == blockNum {
				if row == nil {
					row = &rawdb.StateTxRange{
						BlockNum:   change.BlockNum,
						BlockHash:  change.BlockHash,
						BeginTxNum: change.TxNum,
						EndTxNum:   change.TxNum,
					}
				} else {
					if change.BlockHash != row.BlockHash {
						return nil, false, fmt.Errorf("snapshots: state-domain-change binary segment %q has multiple hashes for block %d", ref.Path, blockNum)
					}
					if change.TxNum < row.BeginTxNum {
						row.BeginTxNum = change.TxNum
					}
					if change.TxNum > row.EndTxNum {
						row.EndTxNum = change.TxNum
					}
				}
			}
			offset = nextOffset
		}
	}
	if row != nil {
		return row, true, nil
	}
	return nil, false, nil
}

func readStateDomainChangeBinarySegmentByAccessorFile(dir string, ref SegmentRef, accessorRef SegmentRef, lookupKey []byte, fromTxNum, toTxNum uint64) ([]*rawdb.StateDomainChange, error) {
	var changes []*rawdb.StateDomainChange
	err := iterateStateDomainChangeBinarySegmentByAccessorFile(dir, ref, accessorRef, lookupKey, fromTxNum, toTxNum, func(change *rawdb.StateDomainChange) (bool, error) {
		changes = append(changes, change)
		return true, nil
	})
	return changes, err
}

func iterateStateDomainChangeBinarySegmentByAccessorFile(dir string, ref SegmentRef, accessorRef SegmentRef, lookupKey []byte, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	if toTxNum < fromTxNum {
		return fmt.Errorf("snapshots: state-domain-change range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	if len(lookupKey) == 0 {
		return errors.New("snapshots: empty state-domain-change accessor lookup key")
	}
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q is %s/%s, want state-domain-change/history", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return err
	}
	segmentFile, segmentSize, _, err := openHistorySegmentForRead(dir, ref)
	if err != nil {
		return err
	}
	defer segmentFile.Close()

	accessorFile, accessorHeader, accessorSize, err := openStateDomainChangeBinaryAccessorReader(dir, accessorRef)
	if err != nil {
		return err
	}
	defer accessorFile.Close()
	if accessorHeader.fromTxNum != ref.FromTxNum || accessorHeader.toTxNum != ref.ToTxNum {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q range [%d,%d], want [%d,%d]", accessorRef.Path, accessorHeader.fromTxNum, accessorHeader.toTxNum, ref.FromTxNum, ref.ToTxNum)
	}

	start, ok, err := stateDomainChangeBinaryAccessorLowerBound(accessorFile, accessorSize, accessorHeader.count, lookupKey)
	if err != nil || !ok {
		return err
	}
	for i := uint64(start); i < accessorHeader.count; i++ {
		entry, err := readStateDomainChangeBinaryAccessorEntryAtBounded(accessorFile, i, accessorSize)
		if err != nil {
			return err
		}
		if !bytes.Equal(entry.key, lookupKey) {
			if bytes.Compare(entry.key, lookupKey) > 0 {
				return nil
			}
			continue
		}
		if entry.txNum < fromTxNum || entry.txNum > toTxNum {
			continue
		}
		change, _, err := readStateDomainChangeBinaryRecordAtBounded(segmentFile, entry.offset, segmentSize)
		if err != nil {
			return err
		}
		if change.TxNum != entry.txNum || change.Seq != entry.seq {
			return fmt.Errorf("snapshots: state-domain-change accessor entry tx/seq [%d,%d] read record [%d,%d]", entry.txNum, entry.seq, change.TxNum, change.Seq)
		}
		if !bytes.Equal(stateDomainChangeBinaryAccessorKey(change), lookupKey) {
			return fmt.Errorf("snapshots: state-domain-change accessor key mismatch at offset %d", entry.offset)
		}
		cont, err := fn(change)
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func readStateDomainChangeBinarySegmentByAccessorEntries(dir string, ref SegmentRef, accessor []stateDomainChangeBinaryAccessorEntry, lookupKey []byte, fromTxNum, toTxNum uint64) ([]*rawdb.StateDomainChange, error) {
	if toTxNum < fromTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	if len(lookupKey) == 0 {
		return nil, errors.New("snapshots: empty state-domain-change accessor lookup key")
	}
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
		return nil, fmt.Errorf("snapshots: state-domain-change binary segment %q is %s/%s, want state-domain-change/history", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return nil, err
	}
	file, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := uint64(stat.Size())
	if ref.Size != 0 {
		if uint64(stat.Size()) != ref.Size {
			return nil, fmt.Errorf("snapshots: state-domain-change binary segment %q size %d, want %d", ref.Path, stat.Size(), ref.Size)
		}
	}
	header, err := readStateDomainChangeBinaryHeaderAt(file, stateDomainChangeBinarySegmentMagic)
	if err != nil {
		return nil, err
	}
	if header.fromTxNum != ref.FromTxNum || header.toTxNum != ref.ToTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change binary segment %q range [%d,%d], want [%d,%d]", ref.Path, header.fromTxNum, header.toTxNum, ref.FromTxNum, ref.ToTxNum)
	}

	start := sort.Search(len(accessor), func(i int) bool {
		return bytes.Compare(accessor[i].key, lookupKey) >= 0
	})
	var changes []*rawdb.StateDomainChange
	for i := start; i < len(accessor) && bytes.Equal(accessor[i].key, lookupKey); i++ {
		entry := accessor[i]
		if entry.txNum < fromTxNum || entry.txNum > toTxNum {
			continue
		}
		change, _, err := readStateDomainChangeBinaryRecordAtBounded(file, entry.offset, fileSize)
		if err != nil {
			return nil, err
		}
		if change.TxNum != entry.txNum || change.Seq != entry.seq {
			return nil, fmt.Errorf("snapshots: state-domain-change accessor entry tx/seq [%d,%d] read record [%d,%d]", entry.txNum, entry.seq, change.TxNum, change.Seq)
		}
		if !bytes.Equal(stateDomainChangeBinaryAccessorKey(change), lookupKey) {
			return nil, fmt.Errorf("snapshots: state-domain-change accessor key mismatch at offset %d", entry.offset)
		}
		changes = append(changes, change)
	}
	return changes, nil
}

func encodeStateDomainChangeBinarySegment(fromTxNum, toTxNum uint64, changes []*rawdb.StateDomainChange) ([]byte, []stateDomainChangeBinaryTxOffset, []stateDomainChangeBinaryAccessorEntry, error) {
	if toTxNum < fromTxNum {
		return nil, nil, nil, fmt.Errorf("snapshots: state-domain-change range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	var buf bytes.Buffer
	writeStateDomainChangeBinaryHeader(&buf, stateDomainChangeBinarySegmentMagic, fromTxNum, toTxNum, uint64(len(changes)))

	index := make([]stateDomainChangeBinaryTxOffset, 0)
	accessor := make([]stateDomainChangeBinaryAccessorEntry, 0, len(changes))
	for i, change := range changes {
		if change.TxNum < fromTxNum || change.TxNum > toTxNum {
			return nil, nil, nil, fmt.Errorf("snapshots: state-domain-change tx %d outside segment range [%d,%d]", change.TxNum, fromTxNum, toTxNum)
		}
		payload, err := encodeStateDomainChangeRecord(change)
		if err != nil {
			return nil, nil, nil, err
		}
		if len(payload) > math.MaxUint32 {
			return nil, nil, nil, fmt.Errorf("snapshots: state-domain-change record %d is too large: %d bytes", i, len(payload))
		}
		offset := uint64(buf.Len())
		writeUint32(&buf, uint32(len(payload)))
		buf.Write(payload)
		accessor = append(accessor, stateDomainChangeBinaryAccessorEntry{
			key:         stateDomainChangeBinaryAccessorKey(change),
			txNum:       change.TxNum,
			seq:         change.Seq,
			offset:      offset,
			recordIndex: uint64(i),
		})
		if len(index) == 0 || index[len(index)-1].txNum != change.TxNum {
			index = append(index, stateDomainChangeBinaryTxOffset{
				txNum:       change.TxNum,
				offset:      offset,
				recordIndex: uint64(i),
				count:       1,
			})
			continue
		}
		index[len(index)-1].count++
	}
	return buf.Bytes(), index, accessor, nil
}

func encodeStateDomainChangeBinaryIndex(fromTxNum, toTxNum uint64, index []stateDomainChangeBinaryTxOffset) ([]byte, error) {
	if toTxNum < fromTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	var buf bytes.Buffer
	writeStateDomainChangeBinaryHeader(&buf, stateDomainChangeBinaryIndexMagic, fromTxNum, toTxNum, uint64(len(index)))
	for i, entry := range index {
		if entry.count == 0 {
			return nil, fmt.Errorf("snapshots: state-domain-change index entry %d has zero count", i)
		}
		if entry.txNum < fromTxNum || entry.txNum > toTxNum {
			return nil, fmt.Errorf("snapshots: state-domain-change index tx %d outside segment range [%d,%d]", entry.txNum, fromTxNum, toTxNum)
		}
		if i > 0 && entry.txNum <= index[i-1].txNum {
			return nil, errors.New("snapshots: state-domain-change index entries are not sorted")
		}
		writeUint64(&buf, entry.txNum)
		writeUint64(&buf, entry.offset)
		writeUint64(&buf, entry.recordIndex)
		writeUint64(&buf, entry.count)
	}
	return buf.Bytes(), nil
}

func encodeStateDomainChangeBinaryAccessor(fromTxNum, toTxNum uint64, entries []stateDomainChangeBinaryAccessorEntry) ([]byte, error) {
	if toTxNum < fromTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	sorted := make([]stateDomainChangeBinaryAccessorEntry, len(entries))
	for i, entry := range entries {
		sorted[i] = stateDomainChangeBinaryAccessorEntry{
			key:         append([]byte(nil), entry.key...),
			txNum:       entry.txNum,
			seq:         entry.seq,
			offset:      entry.offset,
			recordIndex: entry.recordIndex,
		}
	}
	sort.Slice(sorted, func(i, j int) bool {
		return compareStateDomainChangeBinaryAccessorEntry(sorted[i], sorted[j]) < 0
	})

	var buf bytes.Buffer
	writeStateDomainChangeBinaryHeader(&buf, stateDomainChangeBinaryAccessorMagic, fromTxNum, toTxNum, uint64(len(sorted)))
	var offsets bytes.Buffer
	var payload bytes.Buffer
	payloadStart := uint64(stateDomainChangeBinaryHeaderSize + len(sorted)*8)
	for i, entry := range sorted {
		if len(entry.key) == 0 {
			return nil, fmt.Errorf("snapshots: state-domain-change accessor entry %d has empty key", i)
		}
		if len(entry.key) > math.MaxUint32 {
			return nil, fmt.Errorf("snapshots: state-domain-change accessor entry %d key is too large: %d bytes", i, len(entry.key))
		}
		if entry.txNum < fromTxNum || entry.txNum > toTxNum {
			return nil, fmt.Errorf("snapshots: state-domain-change accessor tx %d outside segment range [%d,%d]", entry.txNum, fromTxNum, toTxNum)
		}
		if entry.offset < stateDomainChangeBinaryHeaderSize {
			return nil, fmt.Errorf("snapshots: state-domain-change accessor entry %d has invalid offset %d", i, entry.offset)
		}
		if i > 0 && bytes.Equal(sorted[i-1].key, entry.key) && entry.offset <= sorted[i-1].offset {
			return nil, errors.New("snapshots: state-domain-change accessor offsets are not monotonic for key")
		}
		writeUint64(&offsets, payloadStart+uint64(payload.Len()))
		writeUint32(&payload, uint32(len(entry.key)))
		payload.Write(entry.key)
		writeUint64(&payload, entry.txNum)
		writeUint64(&payload, entry.seq)
		writeUint64(&payload, entry.offset)
		writeUint64(&payload, entry.recordIndex)
	}
	buf.Write(offsets.Bytes())
	buf.Write(payload.Bytes())
	return buf.Bytes(), nil
}

func decodeStateDomainChangeBinaryHeader(data []byte, magic [8]byte) (stateDomainChangeBinaryHeader, []byte, error) {
	if len(data) < stateDomainChangeBinaryHeaderSize {
		return stateDomainChangeBinaryHeader{}, nil, fmt.Errorf("snapshots: state-domain-change binary file is too small: %d bytes", len(data))
	}
	if !bytes.Equal(data[:8], magic[:]) {
		return stateDomainChangeBinaryHeader{}, nil, errors.New("snapshots: invalid state-domain-change binary magic")
	}
	version := binary.BigEndian.Uint32(data[8:12])
	if version != stateDomainChangeBinaryVersion {
		return stateDomainChangeBinaryHeader{}, nil, fmt.Errorf("snapshots: unsupported state-domain-change binary version %d", version)
	}
	return stateDomainChangeBinaryHeader{
		fromTxNum: binary.BigEndian.Uint64(data[12:20]),
		toTxNum:   binary.BigEndian.Uint64(data[20:28]),
		count:     binary.BigEndian.Uint64(data[28:36]),
	}, data[stateDomainChangeBinaryHeaderSize:], nil
}

func writeStateDomainChangeBinaryHeader(buf *bytes.Buffer, magic [8]byte, fromTxNum, toTxNum, count uint64) {
	buf.Write(magic[:])
	writeUint32(buf, stateDomainChangeBinaryVersion)
	writeUint64(buf, fromTxNum)
	writeUint64(buf, toTxNum)
	writeUint64(buf, count)
}

func decodeStateDomainChangeBinaryRecordFrame(data []byte) ([]byte, []byte, error) {
	if len(data) < 4 {
		return nil, nil, io.ErrUnexpectedEOF
	}
	length := binary.BigEndian.Uint32(data[:4])
	end := 4 + uint64(length)
	if uint64(len(data)) < end {
		return nil, nil, io.ErrUnexpectedEOF
	}
	return data[4:end], data[end:], nil
}

func readStateDomainChangeBinaryHeaderAt(r io.ReaderAt, magic [8]byte) (stateDomainChangeBinaryHeader, error) {
	header := make([]byte, stateDomainChangeBinaryHeaderSize)
	if _, err := r.ReadAt(header, 0); err != nil {
		return stateDomainChangeBinaryHeader{}, err
	}
	decoded, _, err := decodeStateDomainChangeBinaryHeader(header, magic)
	return decoded, err
}

// historySegmentReader is the io.ReaderAt + Closer a read path uses for a
// (possibly compressed) state-domain-change .seg. *os.File satisfies it for
// legacy segments; *compressedBlockReader satisfies it for compressed ones.
type historySegmentReader interface {
	io.ReaderAt
	io.Closer
}

// openHistorySegmentForRead opens a state-domain-change history .seg for reading
// records at uncompressed offsets. A legacy segment (magic gtsdcseg) is served
// straight from the file; a compressed segment (magic gtcblk01) is served through
// the block codec's ReadAt over its uncompressed logical bytes. Either way the
// returned reader and logicalSize address records at the SAME offsets the .idx /
// .kv accessors store, so downstream readers are identical for both formats.
func openHistorySegmentForRead(dir string, ref SegmentRef) (historySegmentReader, uint64, stateDomainChangeBinaryHeader, error) {
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
		return nil, 0, stateDomainChangeBinaryHeader{}, fmt.Errorf("snapshots: state-domain-change binary segment %q is %s/%s, want state-domain-change/history", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return nil, 0, stateDomainChangeBinaryHeader{}, err
	}
	path := filepath.Join(dir, ref.Path)
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, stateDomainChangeBinaryHeader{}, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, 0, stateDomainChangeBinaryHeader{}, err
	}
	if ref.Size != 0 && uint64(stat.Size()) != ref.Size {
		_ = file.Close()
		return nil, 0, stateDomainChangeBinaryHeader{}, fmt.Errorf("snapshots: state-domain-change binary segment %q size %d, want %d", ref.Path, stat.Size(), ref.Size)
	}
	var magic [8]byte
	if _, err := file.ReadAt(magic[:], 0); err != nil {
		_ = file.Close()
		return nil, 0, stateDomainChangeBinaryHeader{}, err
	}

	var reader historySegmentReader = file
	logicalSize := uint64(stat.Size())
	if string(magic[:]) == compressedBlockMagic {
		_ = file.Close() // the codec reopens path itself
		cr, err := openCompressedBlockReader(path)
		if err != nil {
			return nil, 0, stateDomainChangeBinaryHeader{}, err
		}
		reader = cr
		logicalSize = cr.UncompressedSize()
	}

	header, err := readStateDomainChangeBinaryHeaderAt(reader, stateDomainChangeBinarySegmentMagic)
	if err != nil {
		_ = reader.Close()
		return nil, 0, stateDomainChangeBinaryHeader{}, err
	}
	if header.fromTxNum != ref.FromTxNum || header.toTxNum != ref.ToTxNum {
		_ = reader.Close()
		return nil, 0, stateDomainChangeBinaryHeader{}, fmt.Errorf("snapshots: state-domain-change binary segment %q range [%d,%d], want [%d,%d]", ref.Path, header.fromTxNum, header.toTxNum, ref.FromTxNum, ref.ToTxNum)
	}
	return reader, logicalSize, header, nil
}

func openStateDomainChangeBinarySegmentReader(dir string, ref SegmentRef) (*os.File, stateDomainChangeBinaryHeader, uint64, error) {
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
		return nil, stateDomainChangeBinaryHeader{}, 0, fmt.Errorf("snapshots: state-domain-change binary segment %q is %s/%s, want state-domain-change/history", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return nil, stateDomainChangeBinaryHeader{}, 0, err
	}
	file, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, stateDomainChangeBinaryHeader{}, 0, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, stateDomainChangeBinaryHeader{}, 0, err
	}
	fileSize := uint64(stat.Size())
	if ref.Size != 0 && fileSize != ref.Size {
		_ = file.Close()
		return nil, stateDomainChangeBinaryHeader{}, 0, fmt.Errorf("snapshots: state-domain-change binary segment %q size %d, want %d", ref.Path, stat.Size(), ref.Size)
	}
	header, err := readStateDomainChangeBinaryHeaderAt(file, stateDomainChangeBinarySegmentMagic)
	if err != nil {
		_ = file.Close()
		return nil, stateDomainChangeBinaryHeader{}, 0, err
	}
	if header.fromTxNum != ref.FromTxNum || header.toTxNum != ref.ToTxNum {
		_ = file.Close()
		return nil, stateDomainChangeBinaryHeader{}, 0, fmt.Errorf("snapshots: state-domain-change binary segment %q range [%d,%d], want [%d,%d]", ref.Path, header.fromTxNum, header.toTxNum, ref.FromTxNum, ref.ToTxNum)
	}
	if header.count > (math.MaxUint64-uint64(stateDomainChangeBinaryHeaderSize))/4 {
		_ = file.Close()
		return nil, stateDomainChangeBinaryHeader{}, 0, fmt.Errorf("snapshots: state-domain-change binary segment %q count %d overflows size", ref.Path, header.count)
	}
	minSize := uint64(stateDomainChangeBinaryHeaderSize) + header.count*4
	if fileSize < minSize {
		_ = file.Close()
		return nil, stateDomainChangeBinaryHeader{}, 0, fmt.Errorf("snapshots: state-domain-change binary segment %q size %d below record table size %d", ref.Path, fileSize, minSize)
	}
	return file, header, fileSize, nil
}

func openStateDomainChangeBinaryIndexReader(dir string, ref SegmentRef) (*os.File, stateDomainChangeBinaryHeader, error) {
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentInverted {
		return nil, stateDomainChangeBinaryHeader{}, fmt.Errorf("snapshots: state-domain-change binary index %q is %s/%s, want state-domain-change/inverted", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return nil, stateDomainChangeBinaryHeader{}, err
	}
	file, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, stateDomainChangeBinaryHeader{}, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, stateDomainChangeBinaryHeader{}, err
	}
	if ref.Size != 0 && uint64(stat.Size()) != ref.Size {
		_ = file.Close()
		return nil, stateDomainChangeBinaryHeader{}, fmt.Errorf("snapshots: state-domain-change binary index %q size %d, want %d", ref.Path, stat.Size(), ref.Size)
	}
	header, err := readStateDomainChangeBinaryHeaderAt(file, stateDomainChangeBinaryIndexMagic)
	if err != nil {
		_ = file.Close()
		return nil, stateDomainChangeBinaryHeader{}, err
	}
	if header.fromTxNum != ref.FromTxNum || header.toTxNum != ref.ToTxNum {
		_ = file.Close()
		return nil, stateDomainChangeBinaryHeader{}, fmt.Errorf("snapshots: state-domain-change binary index %q range [%d,%d], want [%d,%d]", ref.Path, header.fromTxNum, header.toTxNum, ref.FromTxNum, ref.ToTxNum)
	}
	if header.count > (^uint64(0)-stateDomainChangeBinaryHeaderSize)/stateDomainChangeBinaryIndexEntrySize {
		_ = file.Close()
		return nil, stateDomainChangeBinaryHeader{}, fmt.Errorf("snapshots: state-domain-change binary index %q count %d overflows size", ref.Path, header.count)
	}
	wantSize := uint64(stateDomainChangeBinaryHeaderSize) + header.count*stateDomainChangeBinaryIndexEntrySize
	if uint64(stat.Size()) != wantSize {
		_ = file.Close()
		return nil, stateDomainChangeBinaryHeader{}, fmt.Errorf("snapshots: state-domain-change binary index %q size %d, want %d from count", ref.Path, stat.Size(), wantSize)
	}
	return file, header, nil
}

// openStateDomainChangeBinaryAccessorReader opens a .kv accessor for reading at
// uncompressed offsets, magic-dispatching like the .seg opener: a legacy accessor
// is served from the file, a compressed (gtcblk01) one through the codec's ReadAt.
// It returns the logical (uncompressed) size, so callers no longer Stat the file
// — that size is what the entry bounds-checks need, and for a compressed accessor
// the physical file is smaller.
func openStateDomainChangeBinaryAccessorReader(dir string, ref SegmentRef) (historySegmentReader, stateDomainChangeBinaryHeader, uint64, error) {
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentAccessor {
		return nil, stateDomainChangeBinaryHeader{}, 0, fmt.Errorf("snapshots: state-domain-change binary accessor %q is %s/%s, want state-domain-change/accessor", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return nil, stateDomainChangeBinaryHeader{}, 0, err
	}
	path := filepath.Join(dir, ref.Path)
	file, err := os.Open(path)
	if err != nil {
		return nil, stateDomainChangeBinaryHeader{}, 0, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, stateDomainChangeBinaryHeader{}, 0, err
	}
	if ref.Size != 0 && uint64(stat.Size()) != ref.Size {
		_ = file.Close()
		return nil, stateDomainChangeBinaryHeader{}, 0, fmt.Errorf("snapshots: state-domain-change binary accessor %q size %d, want %d", ref.Path, stat.Size(), ref.Size)
	}
	var magic [8]byte
	if _, err := file.ReadAt(magic[:], 0); err != nil {
		_ = file.Close()
		return nil, stateDomainChangeBinaryHeader{}, 0, err
	}
	var reader historySegmentReader = file
	logicalSize := uint64(stat.Size())
	if string(magic[:]) == compressedBlockMagic {
		_ = file.Close()
		cr, err := openCompressedBlockReader(path)
		if err != nil {
			return nil, stateDomainChangeBinaryHeader{}, 0, err
		}
		reader = cr
		logicalSize = cr.UncompressedSize()
	}
	header, err := readStateDomainChangeBinaryHeaderAt(reader, stateDomainChangeBinaryAccessorMagic)
	if err != nil {
		_ = reader.Close()
		return nil, stateDomainChangeBinaryHeader{}, 0, err
	}
	if header.fromTxNum != ref.FromTxNum || header.toTxNum != ref.ToTxNum {
		_ = reader.Close()
		return nil, stateDomainChangeBinaryHeader{}, 0, fmt.Errorf("snapshots: state-domain-change binary accessor %q range [%d,%d], want [%d,%d]", ref.Path, header.fromTxNum, header.toTxNum, ref.FromTxNum, ref.ToTxNum)
	}
	if minSize := uint64(stateDomainChangeBinaryHeaderSize) + header.count*8; logicalSize < minSize {
		_ = reader.Close()
		return nil, stateDomainChangeBinaryHeader{}, 0, fmt.Errorf("snapshots: state-domain-change binary accessor %q logical size %d below offset table size %d", ref.Path, logicalSize, minSize)
	}
	return reader, header, logicalSize, nil
}

func readStateDomainChangeBinaryIndexEntryAt(r io.ReaderAt, index uint64) (stateDomainChangeBinaryTxOffset, error) {
	if index > (math.MaxInt64-stateDomainChangeBinaryHeaderSize)/stateDomainChangeBinaryIndexEntrySize {
		return stateDomainChangeBinaryTxOffset{}, fmt.Errorf("snapshots: state-domain-change index entry index too large: %d", index)
	}
	var raw [stateDomainChangeBinaryIndexEntrySize]byte
	if _, err := r.ReadAt(raw[:], int64(stateDomainChangeBinaryHeaderSize+index*stateDomainChangeBinaryIndexEntrySize)); err != nil {
		return stateDomainChangeBinaryTxOffset{}, err
	}
	return stateDomainChangeBinaryTxOffset{
		txNum:       binary.BigEndian.Uint64(raw[0:8]),
		offset:      binary.BigEndian.Uint64(raw[8:16]),
		recordIndex: binary.BigEndian.Uint64(raw[16:24]),
		count:       binary.BigEndian.Uint64(raw[24:32]),
	}, nil
}

func readStateDomainChangeBinaryAccessorEntryAt(r io.ReaderAt, index uint64) (stateDomainChangeBinaryAccessorEntry, error) {
	return readStateDomainChangeBinaryAccessorEntryAtBounded(r, index, uint64(math.MaxInt64))
}

func readStateDomainChangeBinaryAccessorEntryAtBounded(r io.ReaderAt, index, fileSize uint64) (stateDomainChangeBinaryAccessorEntry, error) {
	offset, err := readStateDomainChangeBinaryAccessorEntryOffsetAt(r, index)
	if err != nil {
		return stateDomainChangeBinaryAccessorEntry{}, err
	}
	return readStateDomainChangeBinaryAccessorEntryAtOffsetBounded(r, offset, fileSize)
}

func readStateDomainChangeBinaryAccessorEntryOffsetAt(r io.ReaderAt, index uint64) (uint64, error) {
	if index > (math.MaxInt64-stateDomainChangeBinaryHeaderSize)/8 {
		return 0, fmt.Errorf("snapshots: state-domain-change accessor index too large: %d", index)
	}
	var offsetRaw [8]byte
	if _, err := r.ReadAt(offsetRaw[:], int64(stateDomainChangeBinaryHeaderSize+index*8)); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(offsetRaw[:]), nil
}

func readStateDomainChangeBinaryAccessorEntryAtOffset(r io.ReaderAt, offset uint64) (stateDomainChangeBinaryAccessorEntry, error) {
	entry, _, err := readStateDomainChangeBinaryAccessorEntryAtOffsetWithNext(r, offset)
	return entry, err
}

func readStateDomainChangeBinaryAccessorEntryAtOffsetWithNext(r io.ReaderAt, offset uint64) (stateDomainChangeBinaryAccessorEntry, uint64, error) {
	return readStateDomainChangeBinaryAccessorEntryAtOffsetWithNextBounded(r, offset, uint64(math.MaxInt64))
}

func readStateDomainChangeBinaryAccessorEntryAtOffsetBounded(r io.ReaderAt, offset, fileSize uint64) (stateDomainChangeBinaryAccessorEntry, error) {
	entry, _, err := readStateDomainChangeBinaryAccessorEntryAtOffsetWithNextBounded(r, offset, fileSize)
	return entry, err
}

func readStateDomainChangeBinaryAccessorEntryAtOffsetWithNextBounded(r io.ReaderAt, offset, fileSize uint64) (stateDomainChangeBinaryAccessorEntry, uint64, error) {
	if offset > math.MaxInt64 {
		return stateDomainChangeBinaryAccessorEntry{}, 0, fmt.Errorf("snapshots: state-domain-change accessor offset too large: %d", offset)
	}
	if offset > fileSize || fileSize-offset < 4 {
		return stateDomainChangeBinaryAccessorEntry{}, 0, io.ErrUnexpectedEOF
	}
	var keyLenRaw [4]byte
	if _, err := r.ReadAt(keyLenRaw[:], int64(offset)); err != nil {
		return stateDomainChangeBinaryAccessorEntry{}, 0, err
	}
	keyLen := binary.BigEndian.Uint32(keyLenRaw[:])
	entryLen := uint64(4) + uint64(keyLen) + stateDomainChangeBinaryAccessorInts
	if entryLen > fileSize-offset {
		return stateDomainChangeBinaryAccessorEntry{}, 0, io.ErrUnexpectedEOF
	}
	if entryLen > math.MaxInt32 && offset > uint64(math.MaxInt64)-entryLen {
		return stateDomainChangeBinaryAccessorEntry{}, 0, fmt.Errorf("snapshots: state-domain-change accessor entry too large: %d", entryLen)
	}
	payload := make([]byte, entryLen)
	if _, err := r.ReadAt(payload, int64(offset)); err != nil {
		return stateDomainChangeBinaryAccessorEntry{}, 0, err
	}
	entry, _, err := decodeStateDomainChangeBinaryAccessorEntryFrame(payload, offset)
	return entry, offset + entryLen, err
}

func decodeStateDomainChangeBinaryAccessorEntryAt(data []byte, offset uint64) (stateDomainChangeBinaryAccessorEntry, uint64, error) {
	if offset > uint64(len(data)) {
		return stateDomainChangeBinaryAccessorEntry{}, 0, io.ErrUnexpectedEOF
	}
	return decodeStateDomainChangeBinaryAccessorEntryFrame(data[offset:], offset)
}

func decodeStateDomainChangeBinaryAccessorEntryFrame(data []byte, absoluteOffset uint64) (stateDomainChangeBinaryAccessorEntry, uint64, error) {
	if len(data) < 4 {
		return stateDomainChangeBinaryAccessorEntry{}, 0, io.ErrUnexpectedEOF
	}
	keyLen := binary.BigEndian.Uint32(data[:4])
	entryLen := uint64(4) + uint64(keyLen) + stateDomainChangeBinaryAccessorInts
	if uint64(len(data)) < entryLen {
		return stateDomainChangeBinaryAccessorEntry{}, 0, io.ErrUnexpectedEOF
	}
	rest := data[4:]
	entry := stateDomainChangeBinaryAccessorEntry{
		key:         append([]byte(nil), rest[:keyLen]...),
		txNum:       binary.BigEndian.Uint64(rest[keyLen : keyLen+8]),
		seq:         binary.BigEndian.Uint64(rest[keyLen+8 : keyLen+16]),
		offset:      binary.BigEndian.Uint64(rest[keyLen+16 : keyLen+24]),
		recordIndex: binary.BigEndian.Uint64(rest[keyLen+24 : keyLen+32]),
	}
	return entry, absoluteOffset + entryLen, nil
}

func validateStateDomainChangeBinaryAccessorEntry(ref SegmentRef, entry stateDomainChangeBinaryAccessorEntry, index uint64) error {
	if len(entry.key) == 0 {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q entry %d has empty key", ref.Path, index)
	}
	if entry.txNum < ref.FromTxNum || entry.txNum > ref.ToTxNum {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q tx %d outside range [%d,%d]", ref.Path, entry.txNum, ref.FromTxNum, ref.ToTxNum)
	}
	if entry.offset < stateDomainChangeBinaryHeaderSize {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q entry %d has invalid offset %d", ref.Path, index, entry.offset)
	}
	return nil
}

func stateDomainChangeBinaryAccessorLowerBound(accessor io.ReaderAt, accessorSize uint64, count uint64, lookupKey []byte) (uint64, bool, error) {
	var foundErr error
	i := sort.Search(int(count), func(i int) bool {
		if foundErr != nil {
			return true
		}
		entry, err := readStateDomainChangeBinaryAccessorEntryAtBounded(accessor, uint64(i), accessorSize)
		if err != nil {
			foundErr = err
			return true
		}
		return bytes.Compare(entry.key, lookupKey) >= 0
	})
	if foundErr != nil {
		return 0, false, foundErr
	}
	return uint64(i), uint64(i) < count, nil
}

func stateDomainChangeBinaryIndexLowerBound(index io.ReaderAt, count uint64, txNum uint64) (uint64, bool, error) {
	var foundErr error
	i := sort.Search(int(count), func(i int) bool {
		if foundErr != nil {
			return true
		}
		entry, err := readStateDomainChangeBinaryIndexEntryAt(index, uint64(i))
		if err != nil {
			foundErr = err
			return true
		}
		return entry.txNum >= txNum
	})
	if foundErr != nil {
		return 0, false, foundErr
	}
	return uint64(i), uint64(i) < count, nil
}

func stateDomainChangeBinaryIndexBlockLowerBound(segment io.ReaderAt, segmentSize uint64, index io.ReaderAt, count uint64, blockNum uint64) (uint64, bool, error) {
	var foundErr error
	i := sort.Search(int(count), func(i int) bool {
		if foundErr != nil {
			return true
		}
		entry, err := readStateDomainChangeBinaryIndexEntryAt(index, uint64(i))
		if err != nil {
			foundErr = err
			return true
		}
		change, _, err := readStateDomainChangeBinaryRecordAtBounded(segment, entry.offset, segmentSize)
		if err != nil {
			foundErr = err
			return true
		}
		if change.TxNum != entry.txNum {
			foundErr = fmt.Errorf("snapshots: state-domain-change binary index entry for tx %d read record tx %d", entry.txNum, change.TxNum)
			return true
		}
		return change.BlockNum >= blockNum
	})
	if foundErr != nil {
		return 0, false, foundErr
	}
	return uint64(i), uint64(i) < count, nil
}

func readStateDomainChangeBinaryRecordAt(r io.ReaderAt, offset uint64) (*rawdb.StateDomainChange, uint64, error) {
	return readStateDomainChangeBinaryRecordAtBounded(r, offset, uint64(math.MaxInt64))
}

func readStateDomainChangeBinaryRecordAtBounded(r io.ReaderAt, offset, fileSize uint64) (*rawdb.StateDomainChange, uint64, error) {
	if offset > math.MaxInt64 {
		return nil, 0, fmt.Errorf("snapshots: state-domain-change record offset too large: %d", offset)
	}
	if offset > fileSize || fileSize-offset < 4 {
		return nil, 0, io.ErrUnexpectedEOF
	}
	var prefix [4]byte
	if _, err := r.ReadAt(prefix[:], int64(offset)); err != nil {
		return nil, 0, err
	}
	length := binary.BigEndian.Uint32(prefix[:])
	if uint64(length) > fileSize-offset-4 {
		return nil, 0, io.ErrUnexpectedEOF
	}
	payload := make([]byte, length)
	if _, err := r.ReadAt(payload, int64(offset)+4); err != nil {
		return nil, 0, err
	}
	change, err := decodeStateDomainChangeRecord(payload)
	if err != nil {
		return nil, 0, err
	}
	return change, offset + 4 + uint64(length), nil
}

func readStateDomainChangeBinaryRecordFrom(r io.Reader, maxPayload uint64) (*rawdb.StateDomainChange, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(prefix[:])
	if uint64(length) > maxPayload {
		return nil, io.ErrUnexpectedEOF
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return decodeStateDomainChangeRecord(payload)
}

func verifyStateDomainChangeBinaryRef(ref SegmentRef, data []byte) error {
	if ref.Size != 0 && uint64(len(data)) != ref.Size {
		return fmt.Errorf("snapshots: segment %q size %d, want %d", ref.Path, len(data), ref.Size)
	}
	if ref.Checksum != "" {
		sum := sha256.Sum256(data)
		if got := "sha256:" + hex.EncodeToString(sum[:]); got != ref.Checksum {
			return fmt.Errorf("snapshots: segment %q checksum %s, want %s", ref.Path, got, ref.Checksum)
		}
	}
	return nil
}

func setStateDomainChangeBinaryRefMetadata(ref *SegmentRef, data []byte) {
	sum := sha256.Sum256(data)
	ref.Size = uint64(len(data))
	ref.Checksum = "sha256:" + hex.EncodeToString(sum[:])
}

func writeStateDomainChangeBinaryFile(abs string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, abs)
}

func stateDomainChangeBinaryIndexPath(segmentPath string) string {
	ext := filepath.Ext(segmentPath)
	if ext == "" {
		return segmentPath + ".idx"
	}
	return segmentPath[:len(segmentPath)-len(ext)] + ".idx"
}

func stateDomainChangeBinaryAccessorPath(segmentPath string) string {
	ext := filepath.Ext(segmentPath)
	if ext == "" {
		return segmentPath + ".kv"
	}
	return segmentPath[:len(segmentPath)-len(ext)] + ".kv"
}

func stateDomainChangeBinaryAccessorKey(change *rawdb.StateDomainChange) []byte {
	return stateDomainChangeBinaryAccessorLookupKey(change.FlatDomain, change.Owner, change.Generation, change.Domain, change.Key)
}

func stateDomainChangeBinaryAccessorLookupKey(flatDomain rawdb.StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) []byte {
	id := owner.AccountID()
	out := make([]byte, 0, 1+len(id)+8+2+len(key))
	out = append(out, byte(flatDomain))
	out = append(out, id[:]...)
	if flatDomain == rawdb.StateFlatDomainKVLatest {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], generation)
		out = append(out, buf[:]...)
		var domainBuf [2]byte
		binary.BigEndian.PutUint16(domainBuf[:], uint16(domain))
		out = append(out, domainBuf[:]...)
		out = append(out, key...)
	}
	return out
}

func normalizeStateDomainChangesForBinary(changes []*rawdb.StateDomainChange) []*rawdb.StateDomainChange {
	out := make([]*rawdb.StateDomainChange, 0, len(changes))
	for _, change := range changes {
		if change != nil {
			out = append(out, cloneStateDomainChangeForSegment(change))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return compareStateDomainChangeForBinary(out[i], out[j]) < 0
	})
	return out
}

func validateStateDomainChangeBinaryRecords(fromTxNum, toTxNum uint64, changes []*rawdb.StateDomainChange) error {
	for i, change := range changes {
		if change == nil {
			return fmt.Errorf("snapshots: nil state-domain-change binary record %d", i)
		}
		if change.TxNum < fromTxNum || change.TxNum > toTxNum {
			return fmt.Errorf("snapshots: state-domain-change tx %d outside segment range [%d,%d]", change.TxNum, fromTxNum, toTxNum)
		}
		if i > 0 && compareStateDomainChangeForBinary(changes[i-1], change) > 0 {
			return errors.New("snapshots: state-domain-change binary records are not sorted")
		}
	}
	return nil
}

func compareStateDomainChangeForBinary(a, b *rawdb.StateDomainChange) int {
	if a.TxNum != b.TxNum {
		return compareUint64(a.TxNum, b.TxNum)
	}
	if a.Seq != b.Seq {
		return compareUint64(a.Seq, b.Seq)
	}
	if a.BlockNum != b.BlockNum {
		return compareUint64(a.BlockNum, b.BlockNum)
	}
	if cmp := bytes.Compare(a.BlockHash[:], b.BlockHash[:]); cmp != 0 {
		return cmp
	}
	if a.FlatDomain != b.FlatDomain {
		return compareUint8(uint8(a.FlatDomain), uint8(b.FlatDomain))
	}
	if cmp := bytes.Compare(a.Owner[:], b.Owner[:]); cmp != 0 {
		return cmp
	}
	if a.Generation != b.Generation {
		return compareUint64(a.Generation, b.Generation)
	}
	if a.Domain != b.Domain {
		return compareUint16(uint16(a.Domain), uint16(b.Domain))
	}
	if cmp := bytes.Compare(a.Key, b.Key); cmp != 0 {
		return cmp
	}
	if a.PrevExists != b.PrevExists {
		return compareBool(a.PrevExists, b.PrevExists)
	}
	if cmp := bytes.Compare(a.Prev, b.Prev); cmp != 0 {
		return cmp
	}
	if a.NextExists != b.NextExists {
		return compareBool(a.NextExists, b.NextExists)
	}
	return bytes.Compare(a.Next, b.Next)
}

func compareStateDomainChangeBinaryAccessorEntry(a, b stateDomainChangeBinaryAccessorEntry) int {
	if cmp := bytes.Compare(a.key, b.key); cmp != 0 {
		return cmp
	}
	if a.txNum != b.txNum {
		return compareUint64(a.txNum, b.txNum)
	}
	if a.seq != b.seq {
		return compareUint64(a.seq, b.seq)
	}
	if a.offset != b.offset {
		return compareUint64(a.offset, b.offset)
	}
	return compareUint64(a.recordIndex, b.recordIndex)
}

func compareUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func compareUint16(a, b uint16) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func compareUint8(a, b uint8) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func compareBool(a, b bool) int {
	switch {
	case !a && b:
		return -1
	case a && !b:
		return 1
	default:
		return 0
	}
}

func writeLengthPrefixedBytes(buf *bytes.Buffer, data []byte) error {
	if len(data) > math.MaxUint32 {
		return fmt.Errorf("snapshots: byte field is too large: %d bytes", len(data))
	}
	writeUint32(buf, uint32(len(data)))
	buf.Write(data)
	return nil
}

func readLengthPrefixedBytes(r *bytes.Reader) ([]byte, error) {
	length, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	if uint64(r.Len()) < uint64(length) {
		return nil, io.ErrUnexpectedEOF
	}
	if length == 0 {
		return nil, nil
	}
	out := make([]byte, length)
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, err
	}
	return out, nil
}

func writeBool(buf *bytes.Buffer, value bool) {
	if value {
		buf.WriteByte(1)
		return
	}
	buf.WriteByte(0)
}

func readBool(r *bytes.Reader) (bool, error) {
	raw, err := r.ReadByte()
	if err != nil {
		return false, err
	}
	switch raw {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("snapshots: invalid boolean byte %d", raw)
	}
}

func writeUint16(buf *bytes.Buffer, value uint16) {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], value)
	buf.Write(tmp[:])
}

func readUint16(r *bytes.Reader) (uint16, error) {
	var tmp [2]byte
	if _, err := io.ReadFull(r, tmp[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(tmp[:]), nil
}

func writeUint32(buf *bytes.Buffer, value uint32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], value)
	buf.Write(tmp[:])
}

func readUint32(r *bytes.Reader) (uint32, error) {
	var tmp [4]byte
	if _, err := io.ReadFull(r, tmp[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(tmp[:]), nil
}

func writeUint64(buf *bytes.Buffer, value uint64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], value)
	buf.Write(tmp[:])
}

func readUint64(r *bytes.Reader) (uint64, error) {
	var tmp [8]byte
	if _, err := io.ReadFull(r, tmp[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(tmp[:]), nil
}

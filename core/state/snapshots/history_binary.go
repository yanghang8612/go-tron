package snapshots

import (
	"bytes"
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
	"sync"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

const (
	stateDomainChangeBinaryVersionV1 = uint32(1)
	stateDomainChangeBinaryVersionV2 = uint32(2)
	stateDomainChangeBinaryVersionV3 = uint32(3)
	stateDomainChangeBinaryVersionV4 = uint32(4)
	stateDomainChangeBinaryVersionV5 = uint32(5)

	// Segment v5 follows Erigon's history/value split: immutable history rows
	// contain the transaction ordinal, logical key and previous value only.
	// Block identity is stored once in the StateTxRange table, record order is
	// carried by the index/accessor record ordinal, and the current value remains
	// in the flat latest domain. Index v2 and accessor v4 are independent file
	// formats and deliberately keep their existing versions.
	stateDomainChangeBinarySegmentVersion = stateDomainChangeBinaryVersionV5
	stateDomainChangeBinaryIndexVersion   = stateDomainChangeBinaryVersionV2

	stateDomainChangeBinaryHeaderSize     = 8 + 4 + 8 + 8 + 8
	stateDomainChangeBinaryIndexEntrySize = 8 + 8 + 8 + 8
	stateDomainChangeBinaryAccessorInts   = 8 + 8 + 8 + 8
	stateDomainChangeBinaryTxRangeSize    = 8 + common.HashLength + 8 + 8
)

var (
	stateDomainChangeBinarySegmentMagic  = [8]byte{'g', 't', 's', 'd', 'c', 's', 'e', 'g'}
	stateDomainChangeBinaryIndexMagic    = [8]byte{'g', 't', 's', 'd', 'c', 'i', 'd', 'x'}
	stateDomainChangeBinaryAccessorMagic = [8]byte{'g', 't', 's', 'd', 'c', 'k', 'v', '1'}
)

type stateDomainChangeBinaryHeader struct {
	version   uint32
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

// encodeStateDomainChangeRecordV5 emits the previous-value-only cold record.
// BlockNum, BlockHash and Seq are reconstructed from segment metadata and the
// immutable record ordinal by readStateDomainChangeBinaryRecordAtBoundedIndex.
func encodeStateDomainChangeRecordV5(change *rawdb.StateDomainChange) ([]byte, error) {
	if change == nil {
		return nil, errors.New("snapshots: nil state-domain-change record")
	}
	var buf bytes.Buffer
	writeUint64(&buf, change.TxNum)
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
	return buf.Bytes(), nil
}

func decodeStateDomainChangeRecordV5(data []byte) (*rawdb.StateDomainChange, error) {
	r := bytes.NewReader(data)
	change := new(rawdb.StateDomainChange)
	var err error
	if change.TxNum, err = readUint64(r); err != nil {
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
	if r.Len() != 0 {
		return nil, fmt.Errorf("snapshots: state-domain-change v5 record has %d trailing bytes", r.Len())
	}
	return change, nil
}

func writeStateDomainChangeBinaryFiles(dir string, ref SegmentRef, changes []*rawdb.StateDomainChange, txRanges ...[]*rawdb.StateTxRange) (SegmentRef, SegmentRef, error) {
	segRef, idxRef, _, err := writeStateDomainChangeBinaryFilesWithAccessor(dir, ref, changes, txRanges...)
	return segRef, idxRef, err
}

func writeStateDomainChangeBinaryFilesWithAccessor(dir string, ref SegmentRef, changes []*rawdb.StateDomainChange, txRanges ...[]*rawdb.StateTxRange) (SegmentRef, SegmentRef, SegmentRef, error) {
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
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, fmt.Errorf("snapshots: state-domain-change binary segment %q is %s/%s, want state-domain-change/history", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}

	normalized := normalizeStateDomainChangesForBinary(changes)
	normalizedTxRanges, err := normalizeStateTxRangesForBinary(ref.FromTxNum, ref.ToTxNum, normalized, firstStateTxRangeSet(txRanges))
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	segmentData, index, accessor, err := encodeStateDomainChangeBinarySegment(ref.FromTxNum, ref.ToTxNum, normalized, normalizedTxRanges)
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
		Dataset:          SegmentDatasetStateDomainChange,
		Kind:             SegmentInverted,
		FromTxNum:        ref.FromTxNum,
		ToTxNum:          ref.ToTxNum,
		AggregationSteps: ref.AggregationSteps,
		Path:             stateDomainChangeBinaryIndexPath(segRef.Path),
	}
	if err := validateSegment(idxRef, idxRef.FromTxNum, idxRef.ToTxNum); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	setStateDomainChangeBinaryRefMetadata(&idxRef, indexData)
	accessorRef := SegmentRef{
		Dataset:          SegmentDatasetStateDomainChange,
		Kind:             SegmentAccessor,
		FromTxNum:        ref.FromTxNum,
		ToTxNum:          ref.ToTxNum,
		AggregationSteps: ref.AggregationSteps,
		Path:             stateDomainChangeBinaryAccessorPath(segRef.Path),
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

// CompressHistorySegments gates whether new cold history payload segments are
// block-compressed. v3/v4 accessors deliberately remain raw: their fixed-width
// exact table is a random-read index, so zstd would trade away its point-lookup
// latency and allocation benefit. Legacy v1/v2 compressed accessors remain
// readable through the same magic-dispatch path.
var CompressHistorySegments = true

// writeHistorySegmentFiles is the cold-build emission entry point; it chooses the
// compressed or legacy writer per CompressHistorySegments.
func writeHistorySegmentFiles(dir string, ref SegmentRef, changes []*rawdb.StateDomainChange, txRanges ...[]*rawdb.StateTxRange) (SegmentRef, SegmentRef, SegmentRef, error) {
	if CompressHistorySegments {
		return writeStateDomainChangeBinaryCompressedSegmentFiles(dir, ref, changes, txRanges...)
	}
	return writeStateDomainChangeBinaryFilesWithAccessor(dir, ref, changes, txRanges...)
}

// historyCompressChunkSize is the uncompressed chunk size per compressed block
// in a cold history .seg. ~16 KiB balances ratio against per-lookup decompress
// cost; record frames span chunks freely (ReadAt is multi-block-safe).
const historyCompressChunkSize = 16384

// writeStateDomainChangeBinaryCompressedSegmentFiles writes a cold history
// segment whose .seg payload is block-compressed (magic gtcblk01). Its .idx and
// v3/v4 .kv accessors retain uncompressed record offsets, which the codec's ReadAt
// resolves. A reader opened via openHistorySegmentForRead serves either segment
// format transparently.
func writeStateDomainChangeBinaryCompressedSegmentFiles(dir string, ref SegmentRef, changes []*rawdb.StateDomainChange, txRanges ...[]*rawdb.StateTxRange) (SegmentRef, SegmentRef, SegmentRef, error) {
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
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, fmt.Errorf("snapshots: state-domain-change binary segment %q is %s/%s, want state-domain-change/history", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}

	normalized := normalizeStateDomainChangesForBinary(changes)
	normalizedTxRanges, err := normalizeStateTxRangesForBinary(ref.FromTxNum, ref.ToTxNum, normalized, firstStateTxRangeSet(txRanges))
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	segmentData, index, accessor, err := encodeStateDomainChangeBinarySegment(ref.FromTxNum, ref.ToTxNum, normalized, normalizedTxRanges)
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
		Dataset:          SegmentDatasetStateDomainChange,
		Kind:             SegmentInverted,
		FromTxNum:        ref.FromTxNum,
		ToTxNum:          ref.ToTxNum,
		AggregationSteps: ref.AggregationSteps,
		Path:             stateDomainChangeBinaryIndexPath(segRef.Path),
	}
	setStateDomainChangeBinaryRefMetadata(&idxRef, indexData)
	accessorRef := SegmentRef{
		Dataset:          SegmentDatasetStateDomainChange,
		Kind:             SegmentAccessor,
		FromTxNum:        ref.FromTxNum,
		ToTxNum:          ref.ToTxNum,
		AggregationSteps: ref.AggregationSteps,
		Path:             stateDomainChangeBinaryAccessorPath(segRef.Path),
	}

	// .idx and the v3/v4 exact/group accessor stay uncompressed. The accessor is a
	// random-read index rather than bulk payload; raw fixed-width hash probes
	// avoid repeated zstd block expansion during archive point lookups.
	if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, idxRef.Path), indexData); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	accessorAbs := filepath.Join(dir, accessorRef.Path)
	if err := os.MkdirAll(filepath.Dir(accessorAbs), 0o755); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := writeStateDomainChangeBinaryFile(accessorAbs, accessorData); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	accSize, accChecksum, err := stateDomainChangeBinaryFileMetadata(accessorAbs)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	accessorRef.Size = accSize
	accessorRef.Checksum = accChecksum

	// Self-validate: walk the just-written compressed segment back through the
	// production ReadAt path before returning. Snap-mode hot-history pruning
	// deletes hot rows on manifest coverage ALONE (no decode-walk), so an
	// unreadable compressed segment reaching the manifest would let pruning
	// delete hot rows whose only cold copy can't be read back — data loss. This
	// catches any writer/codec edge case at build time (bounded memory: ReadAt
	// decompresses one block at a time), so a bad segment never gets published.
	if err := validateCompressedHistorySegmentReadable(dir, segRef); err != nil {
		_ = os.Remove(finalAbs)
		_ = os.Remove(accessorAbs)
		_ = os.Remove(filepath.Join(dir, idxRef.Path))
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, fmt.Errorf("snapshots: compressed history segment self-check failed: %w", err)
	}
	return segRef, idxRef, accessorRef, nil
}

// validateCompressedHistorySegmentReadable walks every record of a written
// history segment through the production ReadAt decode path (block-by-block,
// bounded memory), confirming the segment is fully readable and its record
// frames chain to exactly the logical end.
func validateCompressedHistorySegmentReadable(dir string, segRef SegmentRef) error {
	reader, logicalSize, header, err := openHistorySegmentForRead(dir, segRef)
	if err != nil {
		return err
	}
	defer reader.Close()
	offset, err := validateStateDomainChangeBinaryTxRangeTableAt(reader, logicalSize, segRef, header)
	if err != nil {
		return err
	}
	for i := uint64(0); i < header.count; i++ {
		_, next, err := readStateDomainChangeBinaryRecordAtBoundedIndex(reader, offset, logicalSize, i)
		if err != nil {
			return fmt.Errorf("record %d: %w", i, err)
		}
		offset = next
	}
	if offset != logicalSize {
		return fmt.Errorf("record frames end at %d, want logical size %d", offset, logicalSize)
	}
	return nil
}

type stateDomainChangeBinaryCompactionSource struct {
	history       SegmentRef
	accessor      SegmentRef
	segmentHeader stateDomainChangeBinaryHeader
	segmentSize   uint64
	txRangeCount  uint64
	recordOffset  uint64
}

func compactStateDomainChangeBinaryHistoryRun(dir string, cfg DomainCfg, selection historyCompactionSelection) ([]SegmentRef, error) {
	if cfg.Dataset != SegmentDatasetStateDomainChange {
		return nil, fmt.Errorf("snapshots: unsupported state-domain-change compaction dataset %s", cfg.Dataset)
	}
	sources, err := collectStateDomainChangeBinaryCompactionSources(dir, selection)
	if err != nil {
		return nil, err
	}
	segRef, idxRef, accessorRef, err := writeCompactedStateDomainChangeBinaryFiles(dir, cfg, selection, sources)
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
		if err := verifyStateDomainChangeBinaryCompanionsAgainstSegment(dir, candidate.history, idxRef, accessorRef); err != nil {
			return nil, err
		}
		segmentFile, segmentHeader, segmentSize, err := openStateDomainChangeBinarySegmentReader(dir, candidate.history)
		if err != nil {
			return nil, err
		}
		txRangeCount, recordOffset, err := stateDomainChangeBinaryTxRangeTableBoundsAt(segmentFile, segmentSize, candidate.history, segmentHeader)
		_ = segmentFile.Close()
		if err != nil {
			return nil, err
		}
		sources = append(sources, stateDomainChangeBinaryCompactionSource{
			history:       candidate.history,
			accessor:      accessorRef,
			segmentHeader: segmentHeader,
			segmentSize:   segmentSize,
			txRangeCount:  txRangeCount,
			recordOffset:  recordOffset,
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

// finalizeCompactedHistoryFile publishes a freshly-built compaction temp as
// either block-compressed or raw, per CompressHistorySegments — so merges keep
// the compression that cold builds emit (otherwise retired/merged segments, the
// archive bulk over time, would silently revert to uncompressed). contentAddress
// appends the file checksum to the path (the .seg does this; the .kv/.idx derive
// their path from the .seg). The temp's bytes are the uncompressed logical
// content, so the published file is byte-addressable at the same offsets.
func finalizeStateDomainChangeHistoryFile(dir string, ref SegmentRef, tmp *os.File, tmpName string, contentAddress, compress bool) (SegmentRef, error) {
	if !compress {
		size, checksum, err := closeAndHashStateDomainChangeBinaryTemp(tmp, tmpName)
		if err != nil {
			return SegmentRef{}, err
		}
		return publishStateDomainChangeBinaryFinal(dir, ref, tmpName, size, checksum, contentAddress)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := tmp.Close(); err != nil {
		return SegmentRef{}, err
	}
	compTmp := tmpName + ".cb"
	if err := compressFileToFile(dir, compTmp, tmpName, historyCompressChunkSize); err != nil {
		return SegmentRef{}, err
	}
	defer os.Remove(compTmp)
	size, checksum, err := stateDomainChangeBinaryFileMetadata(compTmp)
	if err != nil {
		return SegmentRef{}, err
	}
	return publishStateDomainChangeBinaryFinal(dir, ref, compTmp, size, checksum, contentAddress)
}

func finalizeCompactedHistoryFile(dir string, ref SegmentRef, tmp *os.File, tmpName string, contentAddress bool) (SegmentRef, error) {
	return finalizeStateDomainChangeHistoryFile(dir, ref, tmp, tmpName, contentAddress, CompressHistorySegments)
}

// publishStateDomainChangeBinaryFinal renames src into its final (optionally
// content-addressed) path and stamps the ref's path/size/checksum.
func publishStateDomainChangeBinaryFinal(dir string, ref SegmentRef, src string, size uint64, checksum string, contentAddress bool) (SegmentRef, error) {
	finalRel := ref.Path
	if contentAddress {
		finalAbs := contentAddressedSnapshotPath(filepath.Join(dir, ref.Path), checksum)
		rel, err := filepath.Rel(dir, finalAbs)
		if err != nil {
			return SegmentRef{}, err
		}
		finalRel = filepath.ToSlash(rel)
	}
	if err := publishStateDomainChangeBinaryTemp(src, filepath.Join(dir, finalRel)); err != nil {
		return SegmentRef{}, err
	}
	ref.Path = finalRel
	ref.Size = size
	ref.Checksum = checksum
	return ref, nil
}

func writeCompactedStateDomainChangeBinaryFiles(dir string, cfg DomainCfg, selection historyCompactionSelection, sources []stateDomainChangeBinaryCompactionSource) (segRef SegmentRef, idxRef SegmentRef, accessorRef SegmentRef, err error) {
	ref := SegmentRef{
		Dataset:          SegmentDatasetStateDomainChange,
		Kind:             SegmentHistory,
		FromTxNum:        selection.fromTxNum,
		ToTxNum:          selection.toTxNum,
		AggregationSteps: selection.aggregationSteps,
		Path:             cfg.HistoryPath(selection.fromTxNum, selection.toTxNum),
	}
	totalRecords, err := stateDomainChangeBinaryCompactionRecordCount(sources)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if totalRecords > math.MaxUint32 {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, fmt.Errorf("snapshots: compacted state-domain-change accessor count %d exceeds uint32 record index", totalRecords)
	}
	collectors, err := newStateDomainChangeBinaryAccessorV4Collectors(etl.Options{TempDir: filepath.Join(dir, "etl")})
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	defer collectors.Close()
	tmp, tmpName, err := createStateDomainChangeBinaryTempFile(dir, ref.Path)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	defer os.Remove(tmpName)
	defer func() {
		_ = tmp.Close()
		if err == nil {
			return
		}
		for _, output := range []SegmentRef{segRef, idxRef, accessorRef} {
			if output.Path != "" {
				_ = os.Remove(filepath.Join(dir, output.Path))
			}
		}
	}()
	if err := writeStateDomainChangeBinaryHeaderTo(tmp, stateDomainChangeBinarySegmentMagic, ref.FromTxNum, ref.ToTxNum, totalRecords); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	txRangeCount, err := writeStateDomainChangeBinaryCompactionTxRanges(dir, tmp, sources)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if txRangeCount > (math.MaxUint64-uint64(stateDomainChangeBinaryHeaderSize)-8)/stateDomainChangeBinaryTxRangeSize {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, fmt.Errorf("snapshots: compacted state-domain-change tx range count %d overflows segment size", txRangeCount)
	}
	recordOffset := uint64(stateDomainChangeBinaryHeaderSize) + 8 + txRangeCount*stateDomainChangeBinaryTxRangeSize

	indexTmp, indexTmpName, err := createStateDomainChangeBinaryTempFile(dir, stateDomainChangeBinaryIndexPath(ref.Path))
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	defer os.Remove(indexTmpName)
	defer indexTmp.Close()
	if err := writeStateDomainChangeBinaryHeaderTo(indexTmp, stateDomainChangeBinaryIndexMagic, ref.FromTxNum, ref.ToTxNum, 0); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	recordWriter := stateDomainChangeHistoryRecordETLWriter{
		segment:    tmp,
		index:      indexTmp,
		ref:        ref,
		expected:   totalRecords,
		segmentOff: recordOffset,
	}
	for i := range sources {
		if err := copyStateDomainChangeBinarySegmentPayload(dir, &recordWriter, collectors, sources[i]); err != nil {
			return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
		}
	}
	if err := recordWriter.Finish(); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := writeStateDomainChangeBinaryIndexCount(indexTmp, recordWriter.indexWritten); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	segRef, err = finalizeCompactedHistoryFile(dir, ref, tmp, tmpName, true)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if CompressHistorySegments {
		if err := validateCompressedHistorySegmentReadable(dir, segRef); err != nil {
			return segRef, SegmentRef{}, SegmentRef{}, fmt.Errorf("snapshots: compacted compressed segment self-check failed: %w", err)
		}
	}
	idxRef = SegmentRef{
		Dataset:          SegmentDatasetStateDomainChange,
		Kind:             SegmentInverted,
		FromTxNum:        segRef.FromTxNum,
		ToTxNum:          segRef.ToTxNum,
		AggregationSteps: segRef.AggregationSteps,
		Path:             stateDomainChangeBinaryIndexPath(segRef.Path),
	}
	idxRef, err = finalizeStateDomainChangeHistoryFile(dir, idxRef, indexTmp, indexTmpName, false, false)
	if err != nil {
		return segRef, SegmentRef{}, SegmentRef{}, err
	}
	accessorRef = SegmentRef{
		Dataset:          SegmentDatasetStateDomainChange,
		Kind:             SegmentAccessor,
		FromTxNum:        segRef.FromTxNum,
		ToTxNum:          segRef.ToTxNum,
		AggregationSteps: segRef.AggregationSteps,
		Path:             stateDomainChangeBinaryAccessorPath(segRef.Path),
	}
	accessorRef, _, err = collectors.Build(dir, accessorRef, totalRecords)
	if err != nil {
		return segRef, idxRef, SegmentRef{}, err
	}
	return segRef, idxRef, accessorRef, nil
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

func writeStateDomainChangeBinaryCompactionTxRanges(dir string, dst *os.File, sources []stateDomainChangeBinaryCompactionSource) (uint64, error) {
	if err := writeStateDomainChangeBinaryTxRangeCount(dst, 0); err != nil {
		return 0, err
	}
	var written uint64
	if err := iterateMergedStateDomainChangeBinaryCompactionTxRanges(dir, sources, func(row *rawdb.StateTxRange) error {
		if written == math.MaxUint64 {
			return errors.New("snapshots: compacted state-domain-change tx range count overflows")
		}
		if err := writeStateDomainChangeBinaryTxRangeEntry(dst, row); err != nil {
			return err
		}
		written++
		return nil
	}); err != nil {
		return 0, err
	}
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], written)
	if _, err := dst.WriteAt(raw[:], stateDomainChangeBinaryHeaderSize); err != nil {
		return 0, err
	}
	return written, nil
}

// iterateMergedStateDomainChangeBinaryCompactionTxRanges streams the source
// tables in block order and merges duplicate block rows created when a cold
// history boundary splits a block's txNum range. It preserves the old
// normalizeStateTxRangesForBinary semantics without retaining all rows.
func iterateMergedStateDomainChangeBinaryCompactionTxRanges(dir string, sources []stateDomainChangeBinaryCompactionSource, fn func(*rawdb.StateTxRange) error) error {
	var pending *rawdb.StateTxRange
	emitPending := func() error {
		if pending == nil {
			return nil
		}
		if err := fn(pending); err != nil {
			return err
		}
		pending = nil
		return nil
	}
	if err := iterateStateDomainChangeBinaryCompactionTxRanges(dir, sources, func(row *rawdb.StateTxRange) error {
		if pending == nil {
			pending = cloneStateTxRangeForSegment(row)
			return nil
		}
		if row.BlockNum < pending.BlockNum {
			return fmt.Errorf("snapshots: compacted state-domain-change tx ranges regress from block %d to %d", pending.BlockNum, row.BlockNum)
		}
		if row.BlockNum == pending.BlockNum {
			if row.BlockHash != pending.BlockHash {
				return fmt.Errorf("snapshots: state-domain history has multiple hashes for block %d", row.BlockNum)
			}
			if row.BeginTxNum < pending.BeginTxNum {
				pending.BeginTxNum = row.BeginTxNum
			}
			if row.EndTxNum > pending.EndTxNum {
				pending.EndTxNum = row.EndTxNum
			}
			return nil
		}
		if err := emitPending(); err != nil {
			return err
		}
		pending = cloneStateTxRangeForSegment(row)
		return nil
	}); err != nil {
		return err
	}
	return emitPending()
}

func iterateStateDomainChangeBinaryCompactionTxRanges(dir string, sources []stateDomainChangeBinaryCompactionSource, fn func(*rawdb.StateTxRange) error) error {
	for _, source := range sources {
		reader, header, logicalSize, err := openStateDomainChangeBinarySegmentReader(dir, source.history)
		if err != nil {
			return err
		}
		if header != source.segmentHeader || logicalSize != source.segmentSize {
			_ = reader.Close()
			return fmt.Errorf("snapshots: state-domain-change binary segment %q changed during compaction", source.history.Path)
		}
		count, recordOffset, err := stateDomainChangeBinaryTxRangeTableBoundsAt(reader, logicalSize, source.history, header)
		if err != nil {
			_ = reader.Close()
			return err
		}
		if count != source.txRangeCount || recordOffset != source.recordOffset {
			_ = reader.Close()
			return fmt.Errorf("snapshots: state-domain-change binary segment %q tx range table changed during compaction", source.history.Path)
		}
		if recordOffset > math.MaxInt64 {
			_ = reader.Close()
			return fmt.Errorf("snapshots: state-domain-change binary segment %q tx range table exceeds int64", source.history.Path)
		}
		var previousBlock uint64
		for i := uint64(0); i < count; i++ {
			offset := uint64(stateDomainChangeBinaryHeaderSize) + 8 + i*stateDomainChangeBinaryTxRangeSize
			var raw [stateDomainChangeBinaryTxRangeSize]byte
			if _, err := reader.ReadAt(raw[:], int64(offset)); err != nil {
				_ = reader.Close()
				return err
			}
			row := decodeStateDomainChangeBinaryTxRange(raw[:])
			if row.EndTxNum < row.BeginTxNum || row.EndTxNum < source.history.FromTxNum || row.BeginTxNum > source.history.ToTxNum {
				_ = reader.Close()
				return fmt.Errorf("snapshots: invalid state-domain-change tx range for block %d in %q", row.BlockNum, source.history.Path)
			}
			if i > 0 && row.BlockNum <= previousBlock {
				_ = reader.Close()
				return fmt.Errorf("snapshots: state-domain-change tx ranges in %q are not sorted", source.history.Path)
			}
			if err := fn(row); err != nil {
				_ = reader.Close()
				return err
			}
			previousBlock = row.BlockNum
		}
		if err := reader.Close(); err != nil {
			return err
		}
	}
	return nil
}

func copyStateDomainChangeBinarySegmentPayload(dir string, dst *stateDomainChangeHistoryRecordETLWriter, collectors *stateDomainChangeBinaryAccessorV4Collectors, source stateDomainChangeBinaryCompactionSource) error {
	if dst == nil {
		return errors.New("snapshots: nil compacted state-domain-change record writer")
	}
	if collectors == nil {
		return errors.New("snapshots: nil compacted state-domain-change accessor collectors")
	}
	if source.segmentSize < source.recordOffset {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q size %d below record offset %d", source.history.Path, source.segmentSize, source.recordOffset)
	}
	reader, header, logicalSize, err := openStateDomainChangeBinarySegmentReader(dir, source.history)
	if err != nil {
		return err
	}
	defer reader.Close()
	if header != source.segmentHeader {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q changed during compaction", source.history.Path)
	}
	if logicalSize != source.segmentSize {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q logical size %d, want %d", source.history.Path, logicalSize, source.segmentSize)
	}
	_, recordOffset, err := stateDomainChangeBinaryTxRangeTableBoundsAt(reader, logicalSize, source.history, header)
	if err != nil {
		return err
	}
	if recordOffset != source.recordOffset {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q record offset %d, want %d", source.history.Path, recordOffset, source.recordOffset)
	}
	// Stream-decode every source version and emit v5 frames plus txNum index
	// entries. This keeps
	// compaction O(one record) in memory while ensuring legacy v1/v2 segments do
	// not leak duplicated block/hash/seq/next fields into merged cold history,
	// without rescanning the newly compressed output to build its index.
	offset := recordOffset
	for recordIndex := uint64(0); recordIndex < header.count; recordIndex++ {
		change, next, err := readStateDomainChangeBinaryRecordAtBoundedIndex(reader, offset, logicalSize, recordIndex)
		if err != nil {
			return err
		}
		if err := collectors.Collect(change, dst.segmentOff, dst.count); err != nil {
			return err
		}
		if err := dst.WriteChange(change); err != nil {
			return err
		}
		offset = next
	}
	if offset != logicalSize {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q has %d trailing bytes", source.history.Path, logicalSize-offset)
	}
	return nil
}

func validateStateDomainChangeBinaryAccessorEntryAgainstSegment(source stateDomainChangeBinaryCompactionSource, segment io.ReaderAt, segmentSize uint64, entry stateDomainChangeBinaryAccessorEntry) error {
	if err := validateStateDomainChangeBinaryAccessorEntry(source.accessor, entry, entry.recordIndex); err != nil {
		return err
	}
	if entry.offset < source.recordOffset || entry.offset >= segmentSize {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q entry offset %d outside segment %q", source.accessor.Path, entry.offset, source.history.Path)
	}
	change, _, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segment, entry.offset, segmentSize, entry.recordIndex)
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

func verifyStateDomainChangeBinaryCompanionsAgainstSegment(dir string, historyRef, indexRef, accessorRef SegmentRef) error {
	if err := CheckStateDomainChangeSegment(dir, historyRef); err != nil {
		return err
	}
	if err := CheckStateDomainChangeIndexSegment(dir, indexRef); err != nil {
		return err
	}
	if err := CheckStateDomainChangeAccessorSegment(dir, accessorRef); err != nil {
		return err
	}

	segment, segmentHeader, segmentSize, err := openStateDomainChangeBinarySegmentReader(dir, historyRef)
	if err != nil {
		return err
	}
	defer segment.Close()
	recordOffset, err := validateStateDomainChangeBinaryTxRangeTableAt(segment, segmentSize, historyRef, segmentHeader)
	if err != nil {
		return err
	}
	if segmentHeader.count > uint64(int(^uint(0)>>1)) {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q count %d exceeds verifier capacity", historyRef.Path, segmentHeader.count)
	}

	indexFile, indexHeader, err := openStateDomainChangeBinaryIndexReader(dir, indexRef)
	if err != nil {
		return err
	}
	defer indexFile.Close()
	if indexHeader.fromTxNum != segmentHeader.fromTxNum || indexHeader.toTxNum != segmentHeader.toTxNum {
		return fmt.Errorf("snapshots: state-domain-change binary index %q range [%d,%d], want segment range [%d,%d]",
			indexRef.Path, indexHeader.fromTxNum, indexHeader.toTxNum, segmentHeader.fromTxNum, segmentHeader.toTxNum)
	}

	accessorFile, accessorHeader, accessorSize, err := openStateDomainChangeBinaryAccessorReader(dir, accessorRef)
	if err != nil {
		return err
	}
	defer accessorFile.Close()
	if accessorHeader.fromTxNum != segmentHeader.fromTxNum || accessorHeader.toTxNum != segmentHeader.toTxNum {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q range [%d,%d], want segment range [%d,%d]",
			accessorRef.Path, accessorHeader.fromTxNum, accessorHeader.toTxNum, segmentHeader.fromTxNum, segmentHeader.toTxNum)
	}
	if accessorHeader.count != segmentHeader.count {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q count %d, want segment count %d", accessorRef.Path, accessorHeader.count, segmentHeader.count)
	}

	if err := verifyStateDomainChangeBinaryIndexCoverage(historyRef, indexRef, segment, segmentSize, recordOffset, segmentHeader.count, indexFile, indexHeader.count); err != nil {
		return err
	}
	if accessorHeader.version == stateDomainChangeBinaryVersionV3 {
		return verifyStateDomainChangeBinaryAccessorV3Coverage(historyRef, accessorRef, segment, segmentSize, segmentHeader.count, indexFile, indexHeader.count, accessorFile, accessorSize, accessorHeader)
	}
	if accessorHeader.version == stateDomainChangeBinaryVersionV4 {
		return verifyStateDomainChangeBinaryAccessorV4Coverage(historyRef, accessorRef, segment, segmentSize, segmentHeader.count, indexFile, indexHeader.count, accessorFile, accessorSize, accessorHeader)
	}
	return verifyStateDomainChangeBinaryAccessorCoverage(historyRef, accessorRef, segment, segmentSize, recordOffset, segmentHeader.count, accessorFile, accessorSize, accessorHeader.count)
}

func verifyStateDomainChangeBinaryIndexCoverage(historyRef, indexRef SegmentRef, segment io.ReaderAt, segmentSize, recordOffset, recordCount uint64, index io.ReaderAt, indexCount uint64) error {
	expectedRecordIndex := uint64(0)
	expectedOffset := recordOffset
	for i := uint64(0); i < indexCount; i++ {
		entry, err := readStateDomainChangeBinaryIndexEntryAt(index, i)
		if err != nil {
			return err
		}
		if entry.recordIndex != expectedRecordIndex {
			return fmt.Errorf("snapshots: state-domain-change binary index %q entry %d record index %d, want %d", indexRef.Path, i, entry.recordIndex, expectedRecordIndex)
		}
		if entry.offset != expectedOffset {
			return fmt.Errorf("snapshots: state-domain-change binary index %q entry %d offset %d, want %d", indexRef.Path, i, entry.offset, expectedOffset)
		}
		if entry.offset < recordOffset || entry.offset >= segmentSize {
			return fmt.Errorf("snapshots: state-domain-change binary index %q entry offset %d outside segment %q", indexRef.Path, entry.offset, historyRef.Path)
		}
		if entry.recordIndex >= recordCount {
			return fmt.Errorf("snapshots: state-domain-change binary index %q record index %d outside segment count %d", indexRef.Path, entry.recordIndex, recordCount)
		}
		if entry.count == 0 || entry.count > recordCount-entry.recordIndex {
			return fmt.Errorf("snapshots: state-domain-change binary index %q entry count %d at record index %d outside segment count %d",
				indexRef.Path, entry.count, entry.recordIndex, recordCount)
		}
		offset := entry.offset
		for j := uint64(0); j < entry.count; j++ {
			change, next, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segment, offset, segmentSize, entry.recordIndex+j)
			if err != nil {
				return err
			}
			if change.TxNum != entry.txNum {
				return fmt.Errorf("snapshots: state-domain-change binary index %q tx %d read segment tx %d", indexRef.Path, entry.txNum, change.TxNum)
			}
			offset = next
		}
		expectedRecordIndex += entry.count
		expectedOffset = offset
	}
	if expectedRecordIndex != recordCount {
		return fmt.Errorf("snapshots: state-domain-change binary index %q missing segment record %d", indexRef.Path, expectedRecordIndex)
	}
	return nil
}

func verifyStateDomainChangeBinaryAccessorCoverage(historyRef, accessorRef SegmentRef, segment io.ReaderAt, segmentSize, recordOffset, recordCount uint64, accessor io.ReaderAt, accessorSize, accessorCount uint64) error {
	if accessorCount != recordCount {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q count %d, want segment count %d", accessorRef.Path, accessorCount, recordCount)
	}
	seenWords := recordCount / 64
	if recordCount%64 != 0 {
		seenWords++
	}
	seen := make([]uint64, seenWords)
	source := stateDomainChangeBinaryCompactionSource{
		history:       historyRef,
		accessor:      accessorRef,
		segmentSize:   segmentSize,
		recordOffset:  recordOffset,
		segmentHeader: stateDomainChangeBinaryHeader{count: recordCount},
	}
	for i := uint64(0); i < accessorCount; i++ {
		entry, err := readStateDomainChangeBinaryAccessorEntryAtBounded(accessor, i, accessorSize)
		if err != nil {
			return err
		}
		if entry.recordIndex >= recordCount {
			return fmt.Errorf("snapshots: state-domain-change binary accessor %q record index %d outside segment count %d", accessorRef.Path, entry.recordIndex, recordCount)
		}
		word := entry.recordIndex / 64
		mask := uint64(1) << (entry.recordIndex % 64)
		if seen[word]&mask != 0 {
			return fmt.Errorf("snapshots: state-domain-change binary accessor %q covers segment record %d more than once", accessorRef.Path, entry.recordIndex)
		}
		if err := validateStateDomainChangeBinaryAccessorEntryAgainstSegment(source, segment, segmentSize, entry); err != nil {
			return err
		}
		seen[word] |= mask
	}
	// accessorCount equals recordCount and every in-range record index is unique,
	// therefore every segment record is covered without a second full scan.
	return nil
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
	version := stateDomainChangeBinaryIndexVersion
	if magic == stateDomainChangeBinarySegmentMagic {
		version = stateDomainChangeBinarySegmentVersion
	}
	return writeStateDomainChangeBinaryHeaderToVersion(w, magic, fromTxNum, toTxNum, count, version)
}

func writeStateDomainChangeBinaryHeaderToVersion(w io.Writer, magic [8]byte, fromTxNum, toTxNum, count uint64, version uint32) error {
	var header [stateDomainChangeBinaryHeaderSize]byte
	copy(header[:8], magic[:])
	binary.BigEndian.PutUint32(header[8:12], version)
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
	if ref.Checksum != "" {
		f, err := os.Open(filepath.Join(dir, ref.Path))
		if err != nil {
			return nil, err
		}
		h := sha256.New()
		_, copyErr := io.Copy(h, f)
		_ = f.Close()
		if copyErr != nil {
			return nil, copyErr
		}
		if got := "sha256:" + hex.EncodeToString(h.Sum(nil)); got != ref.Checksum {
			return nil, fmt.Errorf("snapshots: segment %q checksum %s, want %s", ref.Path, got, ref.Checksum)
		}
	}
	// Walk records block-by-block via ReadAt rather than materializing the whole
	// (decompressed) segment; peak memory is the result slice plus one block.
	reader, logicalSize, header, err := openHistorySegmentForRead(dir, ref)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	_, offset, err := readStateDomainChangeBinaryTxRangeTableAt(reader, logicalSize, ref, header)
	if err != nil {
		return nil, err
	}
	changes := make([]*rawdb.StateDomainChange, 0, header.count)
	for i := uint64(0); i < header.count; i++ {
		change, next, err := readStateDomainChangeBinaryRecordAtBoundedIndex(reader, offset, logicalSize, i)
		if err != nil {
			return nil, fmt.Errorf("snapshots: decode state-domain-change binary record %d: %w", i, err)
		}
		changes = append(changes, change)
		offset = next
	}
	if offset != logicalSize {
		return nil, fmt.Errorf("snapshots: state-domain-change binary segment %q has %d trailing bytes", ref.Path, logicalSize-offset)
	}
	if err := validateStateDomainChangeBinaryRecords(ref.FromTxNum, ref.ToTxNum, changes); err != nil {
		return nil, err
	}
	return changes, nil
}

func readStateDomainChangeBinaryTxRanges(dir string, ref SegmentRef) ([]*rawdb.StateTxRange, error) {
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
		return nil, fmt.Errorf("snapshots: state-domain-change binary segment %q is %s/%s, want state-domain-change/history", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return nil, err
	}
	reader, logicalSize, header, err := openHistorySegmentForRead(dir, ref)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txRanges, _, err := readStateDomainChangeBinaryTxRangeTableAt(reader, logicalSize, ref, header)
	return txRanges, err
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
	// Checksum streams the physical (possibly compressed) file; record validation
	// walks the logical view block-by-block via ReadAt — both bounded memory, so
	// validating a large cold segment during pruning does not spike RAM.
	if ref.Checksum != "" {
		f, err := os.Open(filepath.Join(dir, ref.Path))
		if err != nil {
			return err
		}
		h := sha256.New()
		_, copyErr := io.Copy(h, f)
		_ = f.Close()
		if copyErr != nil {
			return copyErr
		}
		if got := "sha256:" + hex.EncodeToString(h.Sum(nil)); got != ref.Checksum {
			return fmt.Errorf("snapshots: segment %q checksum %s, want %s", ref.Path, got, ref.Checksum)
		}
	}
	reader, fileSize, header, err := openHistorySegmentForRead(dir, ref)
	if err != nil {
		return err
	}
	defer reader.Close()
	offset, err := validateStateDomainChangeBinaryTxRangeTableAt(reader, fileSize, ref, header)
	if err != nil {
		return err
	}
	if header.count > (math.MaxUint64-offset)/4 {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q count %d overflows size", ref.Path, header.count)
	}
	if minSize := offset + header.count*4; fileSize < minSize {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q logical size %d below record table size %d", ref.Path, fileSize, minSize)
	}
	var prevTxNum, prevSeq uint64
	for i := uint64(0); i < header.count; i++ {
		change, next, err := readStateDomainChangeBinaryRecordAtBoundedIndex(reader, offset, fileSize, i)
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
		offset = next
	}
	if offset != fileSize {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q has %d trailing bytes", ref.Path, fileSize-offset)
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
	accessorFile, header, fileSize, err := openStateDomainChangeBinaryAccessorReader(dir, ref)
	if err != nil {
		return nil, err
	}
	defer accessorFile.Close()
	if ref.Checksum != "" {
		_, got, err := stateDomainChangeBinaryFileMetadata(filepath.Join(dir, ref.Path))
		if err != nil {
			return nil, err
		}
		if got != ref.Checksum {
			return nil, fmt.Errorf("snapshots: segment %q checksum %s, want %s", ref.Path, got, ref.Checksum)
		}
	}
	if header.version == stateDomainChangeBinaryVersionV3 {
		return readStateDomainChangeBinaryAccessorV3Debug(dir, ref, accessorFile, fileSize, header)
	}
	if header.version == stateDomainChangeBinaryVersionV4 {
		return readStateDomainChangeBinaryAccessorV4Debug(dir, ref, accessorFile, fileSize, header)
	}
	offsetTableLen := header.count * 8
	minOffset := uint64(stateDomainChangeBinaryHeaderSize) + offsetTableLen
	if fileSize < minOffset {
		return nil, fmt.Errorf("snapshots: state-domain-change binary accessor %q size %d below offset table size %d", ref.Path, fileSize, minOffset)
	}
	entries := make([]stateDomainChangeBinaryAccessorEntry, 0, header.count)
	var prevEntryOffset uint64
	var maxNext uint64
	for i := uint64(0); i < header.count; i++ {
		entryOffset, err := readStateDomainChangeBinaryAccessorEntryOffsetAt(accessorFile, i)
		if err != nil {
			return nil, err
		}
		if entryOffset < minOffset || entryOffset >= fileSize {
			return nil, fmt.Errorf("snapshots: state-domain-change binary accessor %q entry %d offset %d outside payload", ref.Path, i, entryOffset)
		}
		if i > 0 && entryOffset <= prevEntryOffset {
			return nil, fmt.Errorf("snapshots: state-domain-change binary accessor %q entry offsets are not strictly increasing", ref.Path)
		}
		entry, next, err := readStateDomainChangeBinaryAccessorEntryAtOffsetWithNextBounded(accessorFile, entryOffset, fileSize)
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
		if fileSize != uint64(stateDomainChangeBinaryHeaderSize) {
			return nil, fmt.Errorf("snapshots: state-domain-change binary accessor %q has %d trailing bytes", ref.Path, fileSize-uint64(stateDomainChangeBinaryHeaderSize))
		}
		return entries, nil
	}
	if maxNext != fileSize {
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
	if header.version == stateDomainChangeBinaryVersionV3 {
		return checkStateDomainChangeBinaryAccessorV3(accessorFile, fileSize, header)
	}
	if header.version == stateDomainChangeBinaryVersionV4 {
		return checkStateDomainChangeBinaryAccessorV4(accessorFile, fileSize, header)
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
	file, fileSize, _, err := openHistorySegmentForRead(dir, ref)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var changes []*rawdb.StateDomainChange
	for _, entry := range index {
		if entry.txNum < fromTxNum || entry.txNum > toTxNum {
			continue
		}
		offset := entry.offset
		for i := uint64(0); i < entry.count; i++ {
			change, nextOffset, err := readStateDomainChangeBinaryRecordAtBoundedIndex(file, offset, fileSize, entry.recordIndex+i)
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
			change, nextOffset, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segmentFile, offset, segmentSize, entry.recordIndex+j)
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
	segmentFile, segmentSize, segmentHeader, err := openHistorySegmentForRead(dir, ref)
	if err != nil {
		return nil, false, err
	}
	defer segmentFile.Close()
	row, hasTableRows, found, err := findStateDomainChangeBinaryTxRangeForBlock(segmentFile, segmentSize, ref, segmentHeader, blockNum)
	if err != nil {
		return nil, false, err
	}
	if hasTableRows {
		return row, found, nil
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
	var derived *rawdb.StateTxRange
	for i := start; i < indexHeader.count; i++ {
		entry, err := readStateDomainChangeBinaryIndexEntryAt(indexFile, i)
		if err != nil {
			return nil, false, err
		}
		offset := entry.offset
		for j := uint64(0); j < entry.count; j++ {
			change, nextOffset, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segmentFile, offset, segmentSize, entry.recordIndex+j)
			if err != nil {
				return nil, false, err
			}
			if change.TxNum != entry.txNum {
				return nil, false, fmt.Errorf("snapshots: state-domain-change binary index entry for tx %d read record tx %d", entry.txNum, change.TxNum)
			}
			if change.BlockNum > blockNum {
				if derived != nil {
					return derived, true, nil
				}
				return nil, false, nil
			}
			if change.BlockNum == blockNum {
				if derived == nil {
					derived = &rawdb.StateTxRange{
						BlockNum:   change.BlockNum,
						BlockHash:  change.BlockHash,
						BeginTxNum: change.TxNum,
						EndTxNum:   change.TxNum,
					}
				} else {
					if change.BlockHash != derived.BlockHash {
						return nil, false, fmt.Errorf("snapshots: state-domain-change binary segment %q has multiple hashes for block %d", ref.Path, blockNum)
					}
					if change.TxNum < derived.BeginTxNum {
						derived.BeginTxNum = change.TxNum
					}
					if change.TxNum > derived.EndTxNum {
						derived.EndTxNum = change.TxNum
					}
				}
			}
			offset = nextOffset
		}
	}
	if derived != nil {
		return derived, true, nil
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
	if accessorHeader.version == stateDomainChangeBinaryVersionV3 {
		return iterateStateDomainChangeBinarySegmentByAccessorV3Key(segmentFile, segmentSize, accessorFile, accessorSize, accessorHeader, lookupKey, fromTxNum, toTxNum, fn)
	}
	if accessorHeader.version == stateDomainChangeBinaryVersionV4 {
		return iterateStateDomainChangeBinarySegmentByAccessorV4Key(segmentFile, segmentSize, accessorFile, accessorSize, accessorHeader, lookupKey, fromTxNum, toTxNum, fn)
	}

	start, ok, err := stateDomainChangeBinaryAccessorKeyTxLowerBound(accessorFile, accessorSize, accessorHeader.count, lookupKey, fromTxNum)
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
		change, _, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segmentFile, entry.offset, segmentSize, entry.recordIndex)
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

func iterateStateDomainChangeBinarySegmentByAccessorPrefixFile(dir string, ref SegmentRef, accessorRef SegmentRef, lookupPrefix []byte, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	if toTxNum < fromTxNum {
		return fmt.Errorf("snapshots: state-domain-change range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	if len(lookupPrefix) == 0 {
		return errors.New("snapshots: empty state-domain-change accessor lookup prefix")
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
	if accessorHeader.version == stateDomainChangeBinaryVersionV3 {
		return iterateStateDomainChangeBinarySegmentByAccessorV3Prefix(segmentFile, segmentSize, accessorFile, accessorSize, accessorHeader, lookupPrefix, fromTxNum, toTxNum, fn)
	}
	if accessorHeader.version == stateDomainChangeBinaryVersionV4 {
		return iterateStateDomainChangeBinarySegmentByAccessorV4Prefix(segmentFile, segmentSize, accessorFile, accessorSize, accessorHeader, lookupPrefix, fromTxNum, toTxNum, fn)
	}

	start, ok, err := stateDomainChangeBinaryAccessorLowerBound(accessorFile, accessorSize, accessorHeader.count, lookupPrefix)
	if err != nil || !ok {
		return err
	}
	for i := uint64(start); i < accessorHeader.count; i++ {
		entry, err := readStateDomainChangeBinaryAccessorEntryAtBounded(accessorFile, i, accessorSize)
		if err != nil {
			return err
		}
		if !bytes.HasPrefix(entry.key, lookupPrefix) {
			return nil
		}
		if entry.txNum < fromTxNum || entry.txNum > toTxNum {
			continue
		}
		change, _, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segmentFile, entry.offset, segmentSize, entry.recordIndex)
		if err != nil {
			return err
		}
		if change.TxNum != entry.txNum || change.Seq != entry.seq {
			return fmt.Errorf("snapshots: state-domain-change accessor entry tx/seq [%d,%d] read record [%d,%d]", entry.txNum, entry.seq, change.TxNum, change.Seq)
		}
		if !bytes.HasPrefix(stateDomainChangeBinaryAccessorKey(change), lookupPrefix) {
			return fmt.Errorf("snapshots: state-domain-change accessor prefix mismatch at offset %d", entry.offset)
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
	file, fileSize, _, err := openHistorySegmentForRead(dir, ref)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	start := sort.Search(len(accessor), func(i int) bool {
		return bytes.Compare(accessor[i].key, lookupKey) >= 0
	})
	var changes []*rawdb.StateDomainChange
	for i := start; i < len(accessor) && bytes.Equal(accessor[i].key, lookupKey); i++ {
		entry := accessor[i]
		if entry.txNum < fromTxNum || entry.txNum > toTxNum {
			continue
		}
		change, _, err := readStateDomainChangeBinaryRecordAtBoundedIndex(file, entry.offset, fileSize, entry.recordIndex)
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

func encodeStateDomainChangeBinarySegment(fromTxNum, toTxNum uint64, changes []*rawdb.StateDomainChange, txRangeSets ...[]*rawdb.StateTxRange) ([]byte, []stateDomainChangeBinaryTxOffset, []stateDomainChangeBinaryAccessorEntry, error) {
	if toTxNum < fromTxNum {
		return nil, nil, nil, fmt.Errorf("snapshots: state-domain-change range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	txRanges, err := normalizeStateTxRangesForBinary(fromTxNum, toTxNum, changes, firstStateTxRangeSet(txRangeSets))
	if err != nil {
		return nil, nil, nil, err
	}
	var buf bytes.Buffer
	writeStateDomainChangeBinaryHeader(&buf, stateDomainChangeBinarySegmentMagic, fromTxNum, toTxNum, uint64(len(changes)))
	if err := writeStateDomainChangeBinaryTxRangeTable(&buf, txRanges); err != nil {
		return nil, nil, nil, err
	}

	index := make([]stateDomainChangeBinaryTxOffset, 0)
	accessor := make([]stateDomainChangeBinaryAccessorEntry, 0, len(changes))
	for i, change := range changes {
		if change.TxNum < fromTxNum || change.TxNum > toTxNum {
			return nil, nil, nil, fmt.Errorf("snapshots: state-domain-change tx %d outside segment range [%d,%d]", change.TxNum, fromTxNum, toTxNum)
		}
		payload, err := encodeStateDomainChangeRecordV5(change)
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
			seq:         uint64(i) + 1,
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

func writeStateDomainChangeBinaryTxRangeTable(w io.Writer, txRanges []*rawdb.StateTxRange) error {
	if uint64(len(txRanges)) > math.MaxUint64/stateDomainChangeBinaryTxRangeSize {
		return fmt.Errorf("snapshots: state-domain-change tx range count %d overflows size", len(txRanges))
	}
	if err := writeStateDomainChangeBinaryTxRangeCount(w, uint64(len(txRanges))); err != nil {
		return err
	}
	for i, row := range txRanges {
		if row == nil {
			return fmt.Errorf("snapshots: nil state tx range entry %d", i)
		}
		if err := writeStateDomainChangeBinaryTxRangeEntry(w, row); err != nil {
			return err
		}
	}
	return nil
}

func writeStateDomainChangeBinaryTxRangeCount(w io.Writer, count uint64) error {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], count)
	_, err := w.Write(raw[:])
	return err
}

func writeStateDomainChangeBinaryTxRangeEntry(w io.Writer, row *rawdb.StateTxRange) error {
	if row == nil {
		return errors.New("snapshots: nil state tx range entry")
	}
	if row.EndTxNum < row.BeginTxNum {
		return fmt.Errorf("snapshots: state tx range for block %d is inverted", row.BlockNum)
	}
	var raw [stateDomainChangeBinaryTxRangeSize]byte
	binary.BigEndian.PutUint64(raw[0:8], row.BlockNum)
	copy(raw[8:8+common.HashLength], row.BlockHash[:])
	binary.BigEndian.PutUint64(raw[8+common.HashLength:16+common.HashLength], row.BeginTxNum)
	binary.BigEndian.PutUint64(raw[16+common.HashLength:24+common.HashLength], row.EndTxNum)
	_, err := w.Write(raw[:])
	return err
}

func decodeStateDomainChangeBinaryTxRangeTable(ref SegmentRef, header stateDomainChangeBinaryHeader, data []byte) ([]*rawdb.StateTxRange, []byte, error) {
	if header.version == stateDomainChangeBinaryVersionV1 {
		return nil, data, nil
	}
	if header.version != stateDomainChangeBinaryVersionV2 && header.version != stateDomainChangeBinaryVersionV5 {
		return nil, nil, fmt.Errorf("snapshots: unsupported state-domain-change binary version %d", header.version)
	}
	if len(data) < 8 {
		return nil, nil, io.ErrUnexpectedEOF
	}
	count := binary.BigEndian.Uint64(data[:8])
	payload := data[8:]
	if count > uint64(len(payload))/stateDomainChangeBinaryTxRangeSize {
		return nil, nil, fmt.Errorf("snapshots: state-domain-change binary segment %q tx range count %d exceeds payload size %d", ref.Path, count, len(payload))
	}
	txRanges := make([]*rawdb.StateTxRange, 0, count)
	for i := uint64(0); i < count; i++ {
		raw := payload[:stateDomainChangeBinaryTxRangeSize]
		row := decodeStateDomainChangeBinaryTxRange(raw)
		if err := validateStateDomainChangeBinaryTxRange(ref, row, i, txRanges); err != nil {
			return nil, nil, err
		}
		txRanges = append(txRanges, row)
		payload = payload[stateDomainChangeBinaryTxRangeSize:]
	}
	return txRanges, payload, nil
}

func readStateDomainChangeBinaryTxRangeTableAt(r io.ReaderAt, fileSize uint64, ref SegmentRef, header stateDomainChangeBinaryHeader) ([]*rawdb.StateTxRange, uint64, error) {
	var txRanges []*rawdb.StateTxRange
	payloadOffset, err := iterateStateDomainChangeBinaryTxRangeTableAt(r, fileSize, ref, header, func(row *rawdb.StateTxRange) (bool, error) {
		txRanges = append(txRanges, row)
		return true, nil
	})
	if err != nil {
		return nil, 0, err
	}
	return txRanges, payloadOffset, nil
}

func stateDomainChangeBinaryTxRangeTableBoundsAt(r io.ReaderAt, fileSize uint64, ref SegmentRef, header stateDomainChangeBinaryHeader) (uint64, uint64, error) {
	if header.version == stateDomainChangeBinaryVersionV1 {
		return 0, stateDomainChangeBinaryHeaderSize, nil
	}
	if header.version != stateDomainChangeBinaryVersionV2 && header.version != stateDomainChangeBinaryVersionV5 {
		return 0, 0, fmt.Errorf("snapshots: unsupported state-domain-change binary version %d", header.version)
	}
	if fileSize < uint64(stateDomainChangeBinaryHeaderSize)+8 {
		return 0, 0, io.ErrUnexpectedEOF
	}
	var countRaw [8]byte
	if _, err := r.ReadAt(countRaw[:], stateDomainChangeBinaryHeaderSize); err != nil {
		return 0, 0, err
	}
	count := binary.BigEndian.Uint64(countRaw[:])
	if count > (math.MaxUint64-uint64(stateDomainChangeBinaryHeaderSize)-8)/stateDomainChangeBinaryTxRangeSize {
		return 0, 0, fmt.Errorf("snapshots: state-domain-change tx range count %d overflows size", count)
	}
	payloadOffset := uint64(stateDomainChangeBinaryHeaderSize) + 8 + count*stateDomainChangeBinaryTxRangeSize
	if payloadOffset > fileSize {
		return 0, 0, io.ErrUnexpectedEOF
	}
	return count, payloadOffset, nil
}

// findStateDomainChangeBinaryTxRangeForBlock performs a point lookup in the
// fixed-width, block-sorted tx-range table. Snapshot creation and remote
// bootstrap validate table ordering before the manifest becomes usable, so the
// archive read path can avoid decoding every block range for one lookup.
func findStateDomainChangeBinaryTxRangeForBlock(r io.ReaderAt, fileSize uint64, ref SegmentRef, header stateDomainChangeBinaryHeader, blockNum uint64) (*rawdb.StateTxRange, bool, bool, error) {
	count, payloadOffset, err := stateDomainChangeBinaryTxRangeTableBoundsAt(r, fileSize, ref, header)
	if err != nil {
		return nil, false, false, err
	}
	if count == 0 {
		return nil, false, false, nil
	}
	if payloadOffset > math.MaxInt64 {
		return nil, false, false, fmt.Errorf("snapshots: state-domain-change binary segment %q tx range table exceeds int64", ref.Path)
	}

	low, high := uint64(0), count
	for low < high {
		mid := low + (high-low)/2
		row, err := readStateDomainChangeBinaryTxRangeAt(r, ref, mid)
		if err != nil {
			return nil, true, false, err
		}
		if row.BlockNum < blockNum {
			low = mid + 1
		} else {
			high = mid
		}
	}
	if low == count {
		return nil, true, false, nil
	}
	row, err := readStateDomainChangeBinaryTxRangeAt(r, ref, low)
	if err != nil {
		return nil, true, false, err
	}
	if row.BlockNum != blockNum {
		return nil, true, false, nil
	}
	return row, true, true, nil
}

// findStateDomainChangeBinaryTxRangeForTxNum resolves the block-level context
// omitted by v5 records. StateTxRange is monotonic in both block and global
// txNum order, so a point read needs O(log blocks-per-segment) fixed-width reads.
func findStateDomainChangeBinaryTxRangeForTxNum(r io.ReaderAt, fileSize uint64, ref SegmentRef, header stateDomainChangeBinaryHeader, txNum uint64) (*rawdb.StateTxRange, error) {
	count, _, err := stateDomainChangeBinaryTxRangeTableBoundsAt(r, fileSize, ref, header)
	if err != nil {
		return nil, err
	}
	low, high := uint64(0), count
	for low < high {
		mid := low + (high-low)/2
		row, err := readStateDomainChangeBinaryTxRangeAt(r, ref, mid)
		if err != nil {
			return nil, err
		}
		if row.EndTxNum < txNum {
			low = mid + 1
		} else {
			high = mid
		}
	}
	if low >= count {
		return nil, fmt.Errorf("snapshots: state-domain-change tx %d is not covered by StateTxRange", txNum)
	}
	row, err := readStateDomainChangeBinaryTxRangeAt(r, ref, low)
	if err != nil {
		return nil, err
	}
	if txNum < row.BeginTxNum || txNum > row.EndTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change tx %d is not covered by StateTxRange", txNum)
	}
	return row, nil
}

func readStateDomainChangeBinaryTxRangeAt(r io.ReaderAt, ref SegmentRef, index uint64) (*rawdb.StateTxRange, error) {
	offset := uint64(stateDomainChangeBinaryHeaderSize) + 8 + index*stateDomainChangeBinaryTxRangeSize
	var raw [stateDomainChangeBinaryTxRangeSize]byte
	if _, err := r.ReadAt(raw[:], int64(offset)); err != nil {
		return nil, err
	}
	row := decodeStateDomainChangeBinaryTxRange(raw[:])
	if err := validateStateDomainChangeBinaryTxRange(ref, row, index, nil); err != nil {
		return nil, err
	}
	return row, nil
}

func validateStateDomainChangeBinaryTxRangeTableAt(r io.ReaderAt, fileSize uint64, ref SegmentRef, header stateDomainChangeBinaryHeader) (uint64, error) {
	return iterateStateDomainChangeBinaryTxRangeTableAt(r, fileSize, ref, header, nil)
}

// iterateStateDomainChangeBinaryTxRangeTableAt validates the fixed-width range
// table and streams rows to fn. It keeps only the preceding block number, so
// archive restore never needs a resident []StateTxRange for a whole segment.
func iterateStateDomainChangeBinaryTxRangeTableAt(r io.ReaderAt, fileSize uint64, ref SegmentRef, header stateDomainChangeBinaryHeader, fn func(*rawdb.StateTxRange) (bool, error)) (uint64, error) {
	count, payloadOffset, err := stateDomainChangeBinaryTxRangeTableBoundsAt(r, fileSize, ref, header)
	if err != nil {
		return 0, err
	}
	if payloadOffset > math.MaxInt64 {
		return 0, fmt.Errorf("snapshots: state-domain-change binary segment %q tx range table exceeds int64", ref.Path)
	}
	var previousBlock uint64
	for i := uint64(0); i < count; i++ {
		offset := uint64(stateDomainChangeBinaryHeaderSize) + 8 + i*stateDomainChangeBinaryTxRangeSize
		var raw [stateDomainChangeBinaryTxRangeSize]byte
		if _, err := r.ReadAt(raw[:], int64(offset)); err != nil {
			return 0, err
		}
		row := decodeStateDomainChangeBinaryTxRange(raw[:])
		if row.EndTxNum < row.BeginTxNum {
			return 0, fmt.Errorf("snapshots: state-domain-change binary segment %q tx range for block %d is inverted", ref.Path, row.BlockNum)
		}
		if row.EndTxNum < ref.FromTxNum || row.BeginTxNum > ref.ToTxNum {
			return 0, fmt.Errorf("snapshots: state-domain-change binary segment %q tx range for block %d outside range [%d,%d]", ref.Path, row.BlockNum, ref.FromTxNum, ref.ToTxNum)
		}
		if i > 0 && row.BlockNum <= previousBlock {
			return 0, fmt.Errorf("snapshots: state-domain-change binary segment %q tx ranges are not sorted", ref.Path)
		}
		previousBlock = row.BlockNum
		if fn != nil {
			cont, err := fn(row)
			if err != nil {
				return 0, err
			}
			if !cont {
				return payloadOffset, nil
			}
		}
	}
	return payloadOffset, nil
}

// iterateStateDomainChangeBinaryTxRanges opens a binary history segment and
// streams its explicit block-to-tx-range table. Blocks with no state changes
// still need these rows restored during snapshot bootstrap.
func iterateStateDomainChangeBinaryTxRanges(dir string, ref SegmentRef, fn func(*rawdb.StateTxRange) (bool, error)) error {
	reader, logicalSize, header, err := openHistorySegmentForRead(dir, ref)
	if err != nil {
		return err
	}
	defer reader.Close()
	_, err = iterateStateDomainChangeBinaryTxRangeTableAt(reader, logicalSize, ref, header, fn)
	return err
}

func decodeStateDomainChangeBinaryTxRange(raw []byte) *rawdb.StateTxRange {
	row := &rawdb.StateTxRange{
		BlockNum:   binary.BigEndian.Uint64(raw[0:8]),
		BeginTxNum: binary.BigEndian.Uint64(raw[8+common.HashLength : 16+common.HashLength]),
		EndTxNum:   binary.BigEndian.Uint64(raw[16+common.HashLength : 24+common.HashLength]),
	}
	copy(row.BlockHash[:], raw[8:8+common.HashLength])
	return row
}

func validateStateDomainChangeBinaryTxRange(ref SegmentRef, row *rawdb.StateTxRange, index uint64, previous []*rawdb.StateTxRange) error {
	if row == nil {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q tx range %d is nil", ref.Path, index)
	}
	if row.EndTxNum < row.BeginTxNum {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q tx range for block %d is inverted", ref.Path, row.BlockNum)
	}
	if row.EndTxNum < ref.FromTxNum || row.BeginTxNum > ref.ToTxNum {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q tx range for block %d outside range [%d,%d]", ref.Path, row.BlockNum, ref.FromTxNum, ref.ToTxNum)
	}
	if len(previous) > 0 && row.BlockNum <= previous[len(previous)-1].BlockNum {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q tx ranges are not sorted", ref.Path)
	}
	return nil
}

func encodeStateDomainChangeBinaryIndex(fromTxNum, toTxNum uint64, index []stateDomainChangeBinaryTxOffset) ([]byte, error) {
	if toTxNum < fromTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	var buf bytes.Buffer
	writeStateDomainChangeBinaryHeaderVersion(&buf, stateDomainChangeBinaryIndexMagic, fromTxNum, toTxNum, uint64(len(index)), stateDomainChangeBinaryIndexVersion)
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
	return encodeStateDomainChangeBinaryAccessorV4(fromTxNum, toTxNum, entries)
}

func decodeStateDomainChangeBinaryHeader(data []byte, magic [8]byte) (stateDomainChangeBinaryHeader, []byte, error) {
	if len(data) < stateDomainChangeBinaryHeaderSize {
		return stateDomainChangeBinaryHeader{}, nil, fmt.Errorf("snapshots: state-domain-change binary file is too small: %d bytes", len(data))
	}
	if !bytes.Equal(data[:8], magic[:]) {
		return stateDomainChangeBinaryHeader{}, nil, errors.New("snapshots: invalid state-domain-change binary magic")
	}
	version := binary.BigEndian.Uint32(data[8:12])
	if version != stateDomainChangeBinaryVersionV1 && version != stateDomainChangeBinaryVersionV2 && version != stateDomainChangeBinaryVersionV3 && version != stateDomainChangeBinaryVersionV4 && version != stateDomainChangeBinaryVersionV5 {
		return stateDomainChangeBinaryHeader{}, nil, fmt.Errorf("snapshots: unsupported state-domain-change binary version %d", version)
	}
	return stateDomainChangeBinaryHeader{
		version:   version,
		fromTxNum: binary.BigEndian.Uint64(data[12:20]),
		toTxNum:   binary.BigEndian.Uint64(data[20:28]),
		count:     binary.BigEndian.Uint64(data[28:36]),
	}, data[stateDomainChangeBinaryHeaderSize:], nil
}

func writeStateDomainChangeBinaryHeader(buf *bytes.Buffer, magic [8]byte, fromTxNum, toTxNum, count uint64) {
	version := stateDomainChangeBinaryIndexVersion
	if magic == stateDomainChangeBinarySegmentMagic {
		version = stateDomainChangeBinarySegmentVersion
	}
	writeStateDomainChangeBinaryHeaderVersion(buf, magic, fromTxNum, toTxNum, count, version)
}

func writeStateDomainChangeBinaryHeaderVersion(buf *bytes.Buffer, magic [8]byte, fromTxNum, toTxNum, count uint64, version uint32) {
	buf.Write(magic[:])
	writeUint32(buf, version)
	writeUint64(buf, fromTxNum)
	writeUint64(buf, toTxNum)
	writeUint64(buf, count)
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

// stateDomainChangeHistoryReader retains the segment header and a one-row
// StateTxRange cache. Sequential archive scans therefore restore block context
// with one fixed-width table read per block, while random accessor reads fall
// back to a logarithmic table lookup. The mutex keeps a shared reader race-safe.
type stateDomainChangeHistoryReader struct {
	historySegmentReader
	header      stateDomainChangeBinaryHeader
	logicalSize uint64
	ref         SegmentRef

	mu          sync.Mutex
	lastRange   *rawdb.StateTxRange
	lastIndex   uint64
	rangeCount  uint64
	rangesReady bool
}

func (r *stateDomainChangeHistoryReader) txRangeForTxNum(txNum uint64) (*rawdb.StateTxRange, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if row := r.lastRange; row != nil && txNum >= row.BeginTxNum && txNum <= row.EndTxNum {
		return row, nil
	}
	if !r.rangesReady {
		count, _, err := stateDomainChangeBinaryTxRangeTableBoundsAt(r.historySegmentReader, r.logicalSize, r.ref, r.header)
		if err != nil {
			return nil, err
		}
		r.rangeCount = count
		r.rangesReady = true
	}
	// The dominant path is a forward record scan. Advance one table row before
	// falling back to binary search so each new block costs one small ReadAt.
	if r.lastRange != nil && txNum > r.lastRange.EndTxNum && r.lastIndex+1 < r.rangeCount {
		next, err := readStateDomainChangeBinaryTxRangeAt(r.historySegmentReader, r.ref, r.lastIndex+1)
		if err != nil {
			return nil, err
		}
		if txNum >= next.BeginTxNum && txNum <= next.EndTxNum {
			r.lastIndex++
			r.lastRange = next
			return next, nil
		}
	}
	low, high := uint64(0), r.rangeCount
	for low < high {
		mid := low + (high-low)/2
		row, err := readStateDomainChangeBinaryTxRangeAt(r.historySegmentReader, r.ref, mid)
		if err != nil {
			return nil, err
		}
		if row.EndTxNum < txNum {
			low = mid + 1
		} else {
			high = mid
		}
	}
	if low >= r.rangeCount {
		return nil, fmt.Errorf("snapshots: state-domain-change tx %d is not covered by StateTxRange", txNum)
	}
	row, err := readStateDomainChangeBinaryTxRangeAt(r.historySegmentReader, r.ref, low)
	if err != nil {
		return nil, err
	}
	if txNum < row.BeginTxNum || txNum > row.EndTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change tx %d is not covered by StateTxRange", txNum)
	}
	r.lastIndex = low
	r.lastRange = row
	return row, nil
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
	return &stateDomainChangeHistoryReader{
		historySegmentReader: reader,
		header:               header,
		logicalSize:          logicalSize,
		ref:                  ref,
	}, logicalSize, header, nil
}

// openStateDomainChangeBinarySegmentReader is the compaction-facing opener; it
// now delegates to the magic-dispatching openHistorySegmentForRead so compaction
// reads compressed segment sources transparently, then adds the record-table
// sanity checks over the logical size.
func openStateDomainChangeBinarySegmentReader(dir string, ref SegmentRef) (historySegmentReader, stateDomainChangeBinaryHeader, uint64, error) {
	reader, logicalSize, header, err := openHistorySegmentForRead(dir, ref)
	if err != nil {
		return nil, stateDomainChangeBinaryHeader{}, 0, err
	}
	_, payloadOffset, err := stateDomainChangeBinaryTxRangeTableBoundsAt(reader, logicalSize, ref, header)
	if err != nil {
		_ = reader.Close()
		return nil, stateDomainChangeBinaryHeader{}, 0, err
	}
	if header.count > (math.MaxUint64-payloadOffset)/4 {
		_ = reader.Close()
		return nil, stateDomainChangeBinaryHeader{}, 0, fmt.Errorf("snapshots: state-domain-change binary segment %q count %d overflows size", ref.Path, header.count)
	}
	if minSize := payloadOffset + header.count*4; logicalSize < minSize {
		_ = reader.Close()
		return nil, stateDomainChangeBinaryHeader{}, 0, fmt.Errorf("snapshots: state-domain-change binary segment %q logical size %d below record table size %d", ref.Path, logicalSize, minSize)
	}
	return reader, header, logicalSize, nil
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
	if header.version == stateDomainChangeBinaryVersionV3 {
		if _, err := stateDomainChangeBinaryAccessorV3LayoutAt(reader, logicalSize, header); err != nil {
			_ = reader.Close()
			return nil, stateDomainChangeBinaryHeader{}, 0, fmt.Errorf("snapshots: state-domain-change binary accessor %q v3 layout: %w", ref.Path, err)
		}
	} else if header.version == stateDomainChangeBinaryVersionV4 {
		if _, err := stateDomainChangeBinaryAccessorV4LayoutAt(reader, logicalSize, header); err != nil {
			_ = reader.Close()
			return nil, stateDomainChangeBinaryHeader{}, 0, fmt.Errorf("snapshots: state-domain-change binary accessor %q v4 layout: %w", ref.Path, err)
		}
	} else if minSize := uint64(stateDomainChangeBinaryHeaderSize) + header.count*8; logicalSize < minSize {
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

func stateDomainChangeBinaryAccessorKeyTxLowerBound(accessor io.ReaderAt, accessorSize uint64, count uint64, lookupKey []byte, fromTxNum uint64) (uint64, bool, error) {
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
		cmp := bytes.Compare(entry.key, lookupKey)
		return cmp > 0 || (cmp == 0 && entry.txNum >= fromTxNum)
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
		change, _, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segment, entry.offset, segmentSize, entry.recordIndex)
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

// readStateDomainChangeBinaryRecordAtBoundedIndex is the segment-aware record
// reader. v1/v2 records carry their legacy context. v5 records recover Seq from
// the immutable record ordinal and block identity from the StateTxRange table.
func readStateDomainChangeBinaryRecordAtBoundedIndex(r io.ReaderAt, offset, fileSize, recordIndex uint64) (*rawdb.StateDomainChange, uint64, error) {
	header, err := stateDomainChangeBinaryHeaderForReader(r)
	if err != nil {
		return nil, 0, err
	}
	return readStateDomainChangeBinaryRecordFrame(r, offset, fileSize, header.version, recordIndex, true)
}

func stateDomainChangeBinaryHeaderForReader(r io.ReaderAt) (stateDomainChangeBinaryHeader, error) {
	if contextual, ok := r.(*stateDomainChangeHistoryReader); ok {
		return contextual.header, nil
	}
	return readStateDomainChangeBinaryHeaderAt(r, stateDomainChangeBinarySegmentMagic)
}

func readStateDomainChangeBinaryRecordFrame(r io.ReaderAt, offset, fileSize uint64, version uint32, recordIndex uint64, hydrateBlock bool) (*rawdb.StateDomainChange, uint64, error) {
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
	var change *rawdb.StateDomainChange
	var err error
	if version == stateDomainChangeBinaryVersionV5 {
		change, err = decodeStateDomainChangeRecordV5(payload)
		if err == nil {
			if recordIndex == math.MaxUint64 {
				return nil, 0, errors.New("snapshots: v5 state-domain-change record requires record index")
			}
			if hydrateBlock {
				var row *rawdb.StateTxRange
				if contextual, ok := r.(*stateDomainChangeHistoryReader); ok {
					row, err = contextual.txRangeForTxNum(change.TxNum)
				} else {
					header, headerErr := readStateDomainChangeBinaryHeaderAt(r, stateDomainChangeBinarySegmentMagic)
					if headerErr != nil {
						return nil, 0, headerErr
					}
					ref := SegmentRef{FromTxNum: header.fromTxNum, ToTxNum: header.toTxNum}
					row, err = findStateDomainChangeBinaryTxRangeForTxNum(r, fileSize, ref, header, change.TxNum)
				}
				if err == nil {
					change.BlockNum = row.BlockNum
					change.BlockHash = row.BlockHash
					change.Seq, err = stateDomainChangeBinaryV5Sequence(row, change.TxNum, recordIndex)
				}
			} else {
				change.Seq = recordIndex + 1
			}
		}
	} else {
		change, err = decodeStateDomainChangeRecord(payload)
	}
	if err != nil {
		return nil, 0, err
	}
	return change, offset + 4 + uint64(length), nil
}

// stateDomainChangeBinaryV5Sequence creates a stable hot-row sequence without
// storing Seq per cold record. A history boundary may split one block between
// segments, so recordIndex alone is insufficient. The high word identifies the
// transaction ordinal inside the block and the low word preserves record order
// inside the (single-owner) txNum segment. v4 accessors already cap a segment at
// MaxUint32 records, making the packing lossless for supported segments.
func stateDomainChangeBinaryV5Sequence(row *rawdb.StateTxRange, txNum, recordIndex uint64) (uint64, error) {
	if row == nil || txNum < row.BeginTxNum || txNum > row.EndTxNum {
		return 0, fmt.Errorf("snapshots: state-domain-change tx %d is outside its block range", txNum)
	}
	txOrdinal := txNum - row.BeginTxNum
	if txOrdinal > math.MaxUint32 {
		return 0, fmt.Errorf("snapshots: state-domain-change tx ordinal %d exceeds uint32", txOrdinal)
	}
	if recordIndex >= math.MaxUint32 {
		return 0, fmt.Errorf("snapshots: state-domain-change record index %d exceeds v5 sequence capacity", recordIndex)
	}
	return txOrdinal<<32 | (recordIndex + 1), nil
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

func stateDomainChangeBinaryAccessorLookupPrefix(owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte) []byte {
	return stateDomainChangeBinaryAccessorLookupKey(rawdb.StateFlatDomainKVLatest, owner, generation, domain, prefix)
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

func normalizeStateTxRangesForBinary(fromTxNum, toTxNum uint64, changes []*rawdb.StateDomainChange, txRanges []*rawdb.StateTxRange) ([]*rawdb.StateTxRange, error) {
	if toTxNum < fromTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	byBlock := make(map[uint64]*rawdb.StateTxRange)
	for _, row := range txRanges {
		if row == nil || row.EndTxNum < fromTxNum || row.BeginTxNum > toTxNum {
			continue
		}
		if err := mergeBinaryStateTxRange(byBlock, cloneStateTxRangeForSegment(row)); err != nil {
			return nil, err
		}
	}
	for _, change := range changes {
		if change == nil {
			continue
		}
		if change.TxNum < fromTxNum || change.TxNum > toTxNum {
			return nil, fmt.Errorf("snapshots: state-domain-change tx %d outside segment range [%d,%d]", change.TxNum, fromTxNum, toTxNum)
		}
		if err := mergeBinaryStateTxRange(byBlock, &rawdb.StateTxRange{
			BlockNum:   change.BlockNum,
			BlockHash:  change.BlockHash,
			BeginTxNum: change.TxNum,
			EndTxNum:   change.TxNum,
		}); err != nil {
			return nil, err
		}
	}
	out := make([]*rawdb.StateTxRange, 0, len(byBlock))
	for _, row := range byBlock {
		out = append(out, cloneStateTxRangeForSegment(row))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BlockNum < out[j].BlockNum })
	return out, nil
}

func mergeBinaryStateTxRange(byBlock map[uint64]*rawdb.StateTxRange, row *rawdb.StateTxRange) error {
	if row == nil {
		return nil
	}
	if row.EndTxNum < row.BeginTxNum {
		return fmt.Errorf("snapshots: state tx range for block %d is inverted", row.BlockNum)
	}
	if existing := byBlock[row.BlockNum]; existing != nil {
		if existing.BlockHash != row.BlockHash {
			return fmt.Errorf("snapshots: state-domain history has multiple hashes for block %d", row.BlockNum)
		}
		if row.BeginTxNum < existing.BeginTxNum {
			existing.BeginTxNum = row.BeginTxNum
		}
		if row.EndTxNum > existing.EndTxNum {
			existing.EndTxNum = row.EndTxNum
		}
		return nil
	}
	byBlock[row.BlockNum] = row
	return nil
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

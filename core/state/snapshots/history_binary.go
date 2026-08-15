package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
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
	stateDomainChangeBinaryVersionV6 = uint32(6)
	stateDomainChangeBinaryVersionV7 = uint32(7)

	// Segment v5 follows Erigon's history/value split: immutable history rows
	// contain the transaction ordinal, logical key and previous value only.
	// Block identity is stored once in the StateTxRange table, record order is
	// carried by the index/accessor record ordinal, and the current value remains
	// in the flat latest domain. Index v2 is an independent file format. Fresh
	// builds use accessor v5; older accessor versions remain readable.
	stateDomainChangeBinarySegmentVersion = stateDomainChangeBinaryVersionV5
	// The streaming builder first emits this fixed-width staging layout. The
	// published companion is rewritten to the framed V7 layout before publish.
	stateDomainChangeBinaryIndexVersion        = stateDomainChangeBinaryVersionV2
	stateDomainChangeBinaryIndexCurrentVersion = stateDomainChangeBinaryVersionV7

	stateDomainChangeBinaryHeaderSize     = 8 + 4 + 8 + 8 + 8
	stateDomainChangeBinaryIndexEntrySize = 8 + 8 + 8 + 8
	stateDomainChangeBinaryAccessorInts   = 8 + 8 + 8 + 8
	stateDomainChangeBinaryTxRangeSize    = 8 + common.HashLength + 8 + 8
)

var (
	stateDomainChangeBinarySegmentMagic  = [8]byte{'g', 't', 's', 'd', 'c', 's', 'e', 'g'}
	stateDomainChangeBinaryIndexMagic    = [8]byte{'g', 't', 's', 'd', 'c', 'i', 'd', 'x'}
	stateDomainChangeBinaryAccessorMagic = [8]byte{'g', 't', 's', 'd', 'c', 'k', 'v', '1'}
	stateDomainChangeBinaryZeroes        [32 * 1024]byte
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
	size, err := stateDomainChangeRecordV5Size(change)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, size)
	putStateDomainChangeRecordV5(payload, change)
	return payload, nil
}

func stateDomainChangeRecordV5Size(change *rawdb.StateDomainChange) (int, error) {
	if change == nil {
		return 0, errors.New("snapshots: nil state-domain-change record")
	}
	if uint64(len(change.Key)) > math.MaxUint32 {
		return 0, fmt.Errorf("snapshots: byte field is too large: %d bytes", len(change.Key))
	}
	if uint64(len(change.Prev)) > math.MaxUint32 {
		return 0, fmt.Errorf("snapshots: byte field is too large: %d bytes", len(change.Prev))
	}
	const scalarBytes = 8 + 1 + 8 + 2 + 4 + 1 + 4
	size := uint64(scalarBytes+len(change.Owner)) + uint64(len(change.Key)) + uint64(len(change.Prev))
	if size > math.MaxUint32 || size > uint64(^uint(0)>>1) {
		return 0, fmt.Errorf("snapshots: state-domain-change record is too large: %d bytes", size)
	}
	return int(size), nil
}

// putStateDomainChangeRecordV5 writes the fixed v5 layout into an exactly-sized
// destination. Callers that stream many records can retain and grow one frame
// buffer instead of allocating a payload and then copying it into a frame.
func putStateDomainChangeRecordV5(dst []byte, change *rawdb.StateDomainChange) {
	offset := 0
	binary.BigEndian.PutUint64(dst[offset:offset+8], change.TxNum)
	offset += 8
	dst[offset] = byte(change.FlatDomain)
	offset++
	copy(dst[offset:offset+len(change.Owner)], change.Owner[:])
	offset += len(change.Owner)
	binary.BigEndian.PutUint64(dst[offset:offset+8], change.Generation)
	offset += 8
	binary.BigEndian.PutUint16(dst[offset:offset+2], uint16(change.Domain))
	offset += 2
	binary.BigEndian.PutUint32(dst[offset:offset+4], uint32(len(change.Key)))
	offset += 4
	copy(dst[offset:offset+len(change.Key)], change.Key)
	offset += len(change.Key)
	dst[offset] = 0
	if change.PrevExists {
		dst[offset] = 1
	}
	offset++
	binary.BigEndian.PutUint32(dst[offset:offset+4], uint32(len(change.Prev)))
	offset += 4
	copy(dst[offset:], change.Prev)
}

func decodeStateDomainChangeRecordV5(data []byte) (*rawdb.StateDomainChange, error) {
	change := new(rawdb.StateDomainChange)
	if err := decodeStateDomainChangeRecordV5Into(change, data); err != nil {
		return nil, err
	}
	return change, nil
}

func decodeStateDomainChangeRecordV5Into(change *rawdb.StateDomainChange, data []byte) error {
	if change == nil {
		return errors.New("snapshots: nil state-domain-change v5 decode destination")
	}
	*change = rawdb.StateDomainChange{}
	// Callers retain data for the lifetime of the decoded change, so Key and Prev
	// can safely view that immutable payload.
	// Parsing the compact v5 layout directly removes a bytes.Reader plus two
	// field copies per record from sequential cold build/merge scans.
	const scalarBytes = 8 + 1 + 8 + 2
	minimum := scalarBytes + len(change.Owner) + 4 + 1 + 4
	if len(data) < minimum {
		return io.ErrUnexpectedEOF
	}
	offset := 0
	change.TxNum = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8
	change.FlatDomain = rawdb.StateFlatDomain(data[offset])
	offset++
	copy(change.Owner[:], data[offset:offset+len(change.Owner)])
	offset += len(change.Owner)
	change.Generation = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8
	change.Domain = kvdomains.KVDomain(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	keyLen := uint64(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4
	if keyLen > uint64(len(data)-offset) {
		return io.ErrUnexpectedEOF
	}
	if keyLen > 0 {
		change.Key = data[offset : offset+int(keyLen)]
	}
	offset += int(keyLen)
	if offset >= len(data) {
		return io.ErrUnexpectedEOF
	}
	switch data[offset] {
	case 0:
		change.PrevExists = false
	case 1:
		change.PrevExists = true
	default:
		return fmt.Errorf("snapshots: invalid boolean byte %d", data[offset])
	}
	offset++
	if len(data)-offset < 4 {
		return io.ErrUnexpectedEOF
	}
	prevLen := uint64(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4
	if prevLen > uint64(len(data)-offset) {
		return io.ErrUnexpectedEOF
	}
	if prevLen > 0 {
		change.Prev = data[offset : offset+int(prevLen)]
	}
	offset += int(prevLen)
	if offset != len(data) {
		return fmt.Errorf("snapshots: state-domain-change v5 record has %d trailing bytes", len(data)-offset)
	}
	return nil
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
const historyCompressChunkSize = 64 << 10

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
	if err := validateHistorySegmentReadable(dir, segRef); err != nil {
		_ = os.Remove(finalAbs)
		_ = os.Remove(accessorAbs)
		_ = os.Remove(filepath.Join(dir, idxRef.Path))
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, fmt.Errorf("snapshots: compressed history segment self-check failed: %w", err)
	}
	return segRef, idxRef, accessorRef, nil
}

// validateHistorySegmentReadable walks every record of a written history
// segment through the production ReadAt decode path (block-by-block for a
// compressed file, bounded memory), confirming the segment is fully readable
// and its record frames chain to exactly the logical end.
func validateHistorySegmentReadable(dir string, segRef SegmentRef) error {
	reader, logicalSize, header, err := openHistorySegmentForSequentialRead(dir, segRef)
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

// validateTrustedBuiltHistorySegment reopens a segment emitted by the current
// build transaction without replaying invariants already enforced by its tx-
// range and record writers. The compressed opener validates the complete block
// table and physical coverage; this layer binds it to the exact logical end and
// row counts observed while writing, then decodes representative first/middle/
// last payload blocks. External/imported files continue through the exhaustive
// validateHistorySegmentReadable and manifest companion checks.
func validateTrustedBuiltHistorySegment(dir string, segRef SegmentRef, expectedLogicalEnd, expectedRecords, expectedTxRanges uint64) error {
	reader, logicalSize, header, err := openHistorySegmentForSequentialRead(dir, segRef)
	if err != nil {
		return err
	}
	defer reader.Close()
	if header.version != stateDomainChangeBinaryVersionV5 && header.version != stateDomainChangeBinaryVersionV6 {
		return fmt.Errorf("snapshots: trusted history version %d is not a current cold format", header.version)
	}
	if header.count != expectedRecords {
		return fmt.Errorf("snapshots: trusted history record count %d, want %d", header.count, expectedRecords)
	}
	if logicalSize != expectedLogicalEnd {
		return fmt.Errorf("snapshots: trusted history logical size %d, want writer end %d", logicalSize, expectedLogicalEnd)
	}
	txRangeCount, payloadOffset, err := stateDomainChangeBinaryTxRangeTableBoundsAt(reader, logicalSize, segRef, header)
	if err != nil {
		return err
	}
	if txRangeCount != expectedTxRanges {
		return fmt.Errorf("snapshots: trusted history tx-range count %d, want %d", txRangeCount, expectedTxRanges)
	}
	if header.count > (math.MaxUint64-payloadOffset)/4 {
		return fmt.Errorf("snapshots: trusted history record count %d overflows logical size", header.count)
	}
	if minEnd := payloadOffset + header.count*4; minEnd > logicalSize {
		return fmt.Errorf("snapshots: trusted history minimum record end %d exceeds logical size %d", minEnd, logicalSize)
	}
	if logicalSize > math.MaxInt64 {
		return fmt.Errorf("snapshots: trusted history logical size %d exceeds int64", logicalSize)
	}

	positions := []uint64{logicalSize / 2}
	if header.count != 0 {
		positions = append(positions, payloadOffset)
	}
	if logicalSize != 0 {
		positions = append(positions, logicalSize-1)
	}
	seen := make(map[uint64]struct{}, len(positions))
	var one [1]byte
	for _, pos := range positions {
		if pos >= logicalSize {
			continue
		}
		if _, ok := seen[pos]; ok {
			continue
		}
		seen[pos] = struct{}{}
		if _, err := reader.ReadAt(one[:], int64(pos)); err != nil {
			return fmt.Errorf("snapshots: trusted history sample at %d: %w", pos, err)
		}
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

func compactStateDomainChangeBinaryHistoryRun(dir string, cfg DomainCfg, selection historyCompactionSelection) (refs []SegmentRef, err error) {
	if cfg.Dataset != SegmentDatasetStateDomainChange {
		return nil, fmt.Errorf("snapshots: unsupported state-domain-change compaction dataset %s", cfg.Dataset)
	}
	progress := newHistoryCompactionProgress(cfg.Dataset, selection.fromTxNum, selection.toTxNum, len(selection.candidates))
	defer func() { progress.finish(err) }()
	progress.setPhase(historyCompactionPhaseValidate)
	sources, err := collectStateDomainChangeBinaryCompactionSources(dir, selection, progress)
	if err != nil {
		return nil, err
	}
	segRef, idxRef, accessorRef, err := writeCompactedStateDomainChangeBinaryFiles(dir, cfg, selection, sources, progress)
	if err != nil {
		return nil, err
	}
	return []SegmentRef{segRef, accessorRef, idxRef}, nil
}

func collectStateDomainChangeBinaryCompactionSources(dir string, selection historyCompactionSelection, progress *historyCompactionProgress) ([]stateDomainChangeBinaryCompactionSource, error) {
	sources := make([]stateDomainChangeBinaryCompactionSource, 0, len(selection.candidates))
	for candidateIndex, candidate := range selection.candidates {
		idxRef, ok := historyCompactionCompanion(candidate, SegmentInverted)
		if !ok {
			return nil, fmt.Errorf("snapshots: state-domain-change history %q missing index companion", candidate.history.Path)
		}
		accessorRef, ok := historyCompactionCompanion(candidate, SegmentAccessor)
		if !ok {
			return nil, fmt.Errorf("snapshots: state-domain-change history %q missing accessor companion", candidate.history.Path)
		}
		// History is the canonical merge input; index and accessor files are
		// derived sidecars which the output rebuilds from that history stream.
		// Match Erigon's merge path by checking immutable object identity plus
		// companion headers here instead of randomly reading every history record
		// through both old sidecars before every merge. The sequential payload copy
		// below still decodes and validates every source record, while snapshot
		// installation and hot pruning retain the full cross-file coverage proof.
		if err := checkStateDomainChangeBinaryCompactionSource(dir, candidate.history, idxRef, accessorRef); err != nil {
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
		progress.setSourcesProcessed(uint64(candidateIndex + 1))
	}
	return sources, nil
}

func checkStateDomainChangeBinaryCompactionSource(dir string, historyRef, indexRef, accessorRef SegmentRef) error {
	// History is the only canonical merge input, so protect it with a complete
	// physical checksum. The index is not read by the merge and only needs the
	// range header gate below. Keep the accessor's structural check because its
	// key/posting invariants are an independent corruption signal; V6 performs
	// that check as one buffered sequential stream rather than one pread per row.
	if err := checkStateDomainChangeBinarySegmentChecksum(dir, historyRef); err != nil {
		return err
	}
	if err := checkStateDomainChangeBinaryAccessorLayoutContext(context.Background(), dir, accessorRef); err != nil {
		return err
	}

	segment, segmentHeader, _, err := openStateDomainChangeBinarySegmentReader(dir, historyRef)
	if err != nil {
		return err
	}
	if err := segment.Close(); err != nil {
		return err
	}

	index, indexHeader, err := openStateDomainChangeBinaryIndexReader(dir, indexRef)
	if err != nil {
		return err
	}
	if err := index.Close(); err != nil {
		return err
	}
	if indexHeader.fromTxNum != segmentHeader.fromTxNum || indexHeader.toTxNum != segmentHeader.toTxNum {
		return fmt.Errorf("snapshots: state-domain-change binary index %q range [%d,%d], want segment range [%d,%d]",
			indexRef.Path, indexHeader.fromTxNum, indexHeader.toTxNum, segmentHeader.fromTxNum, segmentHeader.toTxNum)
	}

	accessor, accessorHeader, _, err := openStateDomainChangeBinaryAccessorReader(dir, accessorRef)
	if err != nil {
		return err
	}
	if err := accessor.Close(); err != nil {
		return err
	}
	if accessorHeader.fromTxNum != segmentHeader.fromTxNum || accessorHeader.toTxNum != segmentHeader.toTxNum {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q range [%d,%d], want segment range [%d,%d]",
			accessorRef.Path, accessorHeader.fromTxNum, accessorHeader.toTxNum, segmentHeader.fromTxNum, segmentHeader.toTxNum)
	}
	if accessorHeader.count != segmentHeader.count {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q count %d, want segment count %d",
			accessorRef.Path, accessorHeader.count, segmentHeader.count)
	}
	return nil
}

func historyCompactionCompanion(candidate historyCompactionCandidate, kind SegmentKind) (SegmentRef, bool) {
	for _, ref := range candidate.companions {
		if ref.Kind == kind {
			return ref, true
		}
	}
	return SegmentRef{}, false
}

// stateDomainChangeHistoryTemp writes a history payload either directly to a
// raw temp (the compatibility gate) or into the seekable streaming compressor.
// Both variants expose WriteAt because the record and tx-range counts live in
// the retained fixed header. Finalize publishes only a fully synced, checksummed
// artifact, preserving the previous failure-atomic rename boundary.
type stateDomainChangeHistoryTemp struct {
	dir        string
	tmpName    string
	raw        *os.File
	compressed *compressedBlockStreamWriter
	finalized  bool
}

func createStateDomainChangeHistoryTemp(dir, relPath string, compress bool) (*stateDomainChangeHistoryTemp, error) {
	if !compress {
		raw, tmpName, err := createStateDomainChangeBinaryTempFile(dir, relPath)
		if err != nil {
			return nil, err
		}
		return &stateDomainChangeHistoryTemp{dir: dir, tmpName: tmpName, raw: raw}, nil
	}
	abs := filepath.Join(dir, relPath)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".*.cb.tmp")
	if err != nil {
		return nil, err
	}
	tmpName := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return nil, err
	}
	stream, err := newCompressedBlockStreamWriter(filepath.Dir(abs), historyCompressChunkSize, historyCompressionConcurrency(runtime.GOMAXPROCS(0)))
	if err != nil {
		_ = os.Remove(tmpName)
		return nil, err
	}
	return &stateDomainChangeHistoryTemp{dir: dir, tmpName: tmpName, compressed: stream}, nil
}

func (w *stateDomainChangeHistoryTemp) Write(p []byte) (int, error) {
	if w == nil {
		return 0, errors.New("snapshots: nil state-domain-change history temp")
	}
	if w.raw != nil {
		return w.raw.Write(p)
	}
	if w.compressed != nil {
		return w.compressed.Write(p)
	}
	return 0, errors.New("snapshots: state-domain-change history temp is closed")
}

func (w *stateDomainChangeHistoryTemp) WriteAt(p []byte, off int64) (int, error) {
	if w == nil {
		return 0, errors.New("snapshots: nil state-domain-change history temp")
	}
	if w.raw != nil {
		return w.raw.WriteAt(p, off)
	}
	if w.compressed != nil {
		return w.compressed.WriteAt(p, off)
	}
	return 0, errors.New("snapshots: state-domain-change history temp is closed")
}

func (w *stateDomainChangeHistoryTemp) Reset() error {
	if w == nil {
		return errors.New("snapshots: nil state-domain-change history temp")
	}
	if w.raw != nil {
		return resetStateDomainChangeHistoryTempFile(w.raw)
	}
	if w.compressed != nil {
		return w.compressed.Reset()
	}
	return errors.New("snapshots: state-domain-change history temp is closed")
}

func (w *stateDomainChangeHistoryTemp) Finalize(ref SegmentRef, contentAddress bool) (SegmentRef, error) {
	if w == nil || w.finalized {
		return SegmentRef{}, errors.New("snapshots: state-domain-change history temp is already finalized")
	}
	if w.raw != nil {
		size, checksum, err := closeAndHashStateDomainChangeBinaryTemp(w.raw, w.tmpName)
		if err != nil {
			return SegmentRef{}, err
		}
		w.raw = nil
		result, err := publishStateDomainChangeBinaryFinal(w.dir, ref, w.tmpName, size, checksum, contentAddress)
		if err == nil {
			w.finalized = true
		}
		return result, err
	}
	if w.compressed == nil {
		return SegmentRef{}, errors.New("snapshots: state-domain-change history temp is closed")
	}
	metadata, err := w.compressed.FinishWithMetadata(w.tmpName)
	if err != nil {
		w.compressed = nil
		return SegmentRef{}, err
	}
	w.compressed = nil
	checksum := "sha256:" + hex.EncodeToString(metadata.checksum[:])
	result, err := publishStateDomainChangeBinaryFinal(w.dir, ref, w.tmpName, metadata.size, checksum, contentAddress)
	if err == nil {
		w.finalized = true
	}
	return result, err
}

func (w *stateDomainChangeHistoryTemp) Close() {
	if w == nil || w.finalized {
		return
	}
	if w.compressed != nil {
		w.compressed.Abort()
		w.compressed = nil
	}
	if w.raw != nil {
		_ = w.raw.Close()
		w.raw = nil
	}
	_ = os.Remove(w.tmpName)
}

// finalizeStateDomainChangeHistoryFile publishes raw index/accessor temps. The
// compressed history payload uses stateDomainChangeHistoryTemp directly so it
// never materializes an uncompressed intermediate file.
func finalizeStateDomainChangeHistoryFile(dir string, ref SegmentRef, tmp *os.File, tmpName string, contentAddress bool) (SegmentRef, error) {
	size, checksum, err := closeAndHashStateDomainChangeBinaryTemp(tmp, tmpName)
	if err != nil {
		return SegmentRef{}, err
	}
	return publishStateDomainChangeBinaryFinal(dir, ref, tmpName, size, checksum, contentAddress)
}

func finalizeStateDomainChangeHistoryFileWithMetadata(dir string, ref SegmentRef, tmp *os.File, tmpName string, metadata snapshotFileMetadata, contentAddress bool) (SegmentRef, error) {
	if err := syncAndCloseStateDomainChangeBinaryTemp(tmp); err != nil {
		return SegmentRef{}, err
	}
	checksum := "sha256:" + hex.EncodeToString(metadata.checksum[:])
	return publishStateDomainChangeBinaryFinal(dir, ref, tmpName, metadata.size, checksum, contentAddress)
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

func writeCompactedStateDomainChangeBinaryFiles(dir string, cfg DomainCfg, selection historyCompactionSelection, sources []stateDomainChangeBinaryCompactionSource, progress *historyCompactionProgress) (segRef SegmentRef, idxRef SegmentRef, accessorRef SegmentRef, err error) {
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
	progress.setRecordTotal(totalRecords)
	v6Build, err := newStateDomainChangeV6Build(etl.Options{TempDir: filepath.Join(dir, "etl")}, dir, ref.Path)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	defer v6Build.Close()
	progress.setPhase(historyCompactionPhaseCollectKeys)
	for i := range sources {
		if err := collectStateDomainChangeBinarySegmentV6Keys(dir, v6Build, sources[i]); err != nil {
			return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
		}
		progress.setSourcesProcessed(uint64(i + 1))
	}
	progress.setPhase(historyCompactionPhaseBuildDictionary)
	if err := v6Build.FinishDictionary(); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	tmp, err := createStateDomainChangeHistoryTemp(dir, ref.Path, CompressHistorySegments)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	defer tmp.Close()
	defer func() {
		if err == nil {
			return
		}
		for _, output := range []SegmentRef{segRef, idxRef, accessorRef} {
			if output.Path != "" {
				_ = os.Remove(filepath.Join(dir, output.Path))
			}
		}
	}()
	if err := writeStateDomainChangeBinaryHeaderToVersion(tmp, stateDomainChangeBinarySegmentMagic, ref.FromTxNum, ref.ToTxNum, totalRecords, stateDomainChangeBinaryVersionV6); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := writeStateDomainChangeBinaryV6DictionaryCommitment(tmp, v6Build.dictionaryDigest); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	progress.setPhase(historyCompactionPhaseWriteTxRanges)
	txRangeCount, err := writeStateDomainChangeBinaryCompactionTxRanges(dir, tmp, sources)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if txRangeCount > (math.MaxUint64-uint64(stateDomainChangeBinaryHeaderSize)-8)/stateDomainChangeBinaryTxRangeSize {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, fmt.Errorf("snapshots: compacted state-domain-change tx range count %d overflows segment size", txRangeCount)
	}
	recordOffset := stateDomainChangeBinaryTxRangeTableStart(stateDomainChangeBinaryVersionV6) + 8 + txRangeCount*stateDomainChangeBinaryTxRangeSize

	indexTmp, indexTmpName, err := createStateDomainChangeBinaryTempFile(dir, stateDomainChangeBinaryIndexPath(ref.Path))
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	defer func() {
		_ = os.Remove(indexTmpName)
		_ = indexTmp.Close()
	}()
	if err := writeStateDomainChangeBinaryHeaderTo(indexTmp, stateDomainChangeBinaryIndexMagic, ref.FromTxNum, ref.ToTxNum, 0); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	recordWriter := newStateDomainChangeHistoryRecordWriterV6(tmp, indexTmp, v6Build, ref, totalRecords, recordOffset)
	defer recordWriter.Release()
	progress.setPhase(historyCompactionPhaseCopyRecords)
	var copiedRecords uint64
	for i := range sources {
		if err := copyStateDomainChangeBinarySegmentPayload(dir, recordWriter, v6Build, sources[i], progress, copiedRecords); err != nil {
			return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
		}
		copiedRecords += sources[i].segmentHeader.count
		progress.setRecordsProcessed(copiedRecords)
		progress.setSourcesProcessed(uint64(i + 1))
	}
	if err := recordWriter.Finish(); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := writeStateDomainChangeBinaryHeaderCount(indexTmp, recordWriter.indexWritten); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	indexTmp, indexTmpName, err = rewriteStateDomainChangeBinaryIndexV7(indexTmp, indexTmpName)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	progress.setPhase(historyCompactionPhaseFinalizeHistory)
	segRef, err = tmp.Finalize(ref, true)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	progress.setPhase(historyCompactionPhaseVerifyHistory)
	if err := validateTrustedBuiltHistorySegment(dir, segRef, recordWriter.segmentOff, totalRecords, txRangeCount); err != nil {
		return segRef, SegmentRef{}, SegmentRef{}, fmt.Errorf("snapshots: compacted history self-check failed: %w", err)
	}
	progress.setPhase(historyCompactionPhaseBuildAccessor)
	idxRef, accessorRef, _, err = finalizeStateDomainChangeBinaryCompanionsV6(dir, segRef, indexTmp, indexTmpName, v6Build, totalRecords)
	if err != nil {
		return segRef, idxRef, accessorRef, err
	}
	return segRef, idxRef, accessorRef, nil
}

func collectStateDomainChangeBinarySegmentV6Keys(dir string, build *stateDomainChangeV6Build, source stateDomainChangeBinaryCompactionSource) error {
	reader, header, logicalSize, err := openStateDomainChangeBinarySegmentSequentialReader(dir, source.history)
	if err != nil {
		return err
	}
	defer reader.Close()
	if header != source.segmentHeader || logicalSize != source.segmentSize {
		return fmt.Errorf("snapshots: state-domain-change source %q changed while collecting V6 keys", source.history.Path)
	}
	if header.version == stateDomainChangeBinaryVersionV6 {
		accessor, accessorHeader, accessorSize, err := openStateDomainChangeBinaryAccessorReader(dir, source.accessor)
		if err != nil {
			return err
		}
		defer accessor.Close()
		if (accessorHeader.version != stateDomainChangeBinaryVersionV6 && accessorHeader.version != stateDomainChangeBinaryVersionV7) || accessorHeader.fromTxNum != header.fromTxNum || accessorHeader.toTxNum != header.toTxNum || accessorHeader.count != header.count {
			return fmt.Errorf("snapshots: V6 accessor %q changed while collecting dictionary keys", source.accessor.Path)
		}
		if err := verifyStateDomainChangeBinaryV6DictionaryPair(reader, accessor, accessorSize); err != nil {
			return err
		}
		_, err = iterateStateDomainChangeBinaryAccessorV6Keys(accessor, accessorSize, func(_ uint32, key []byte) error {
			return build.CollectLogicalKey(key)
		})
		return err
	}
	_, offset, err := stateDomainChangeBinaryTxRangeTableBoundsAt(reader, logicalSize, source.history, header)
	if err != nil {
		return err
	}
	for i := uint64(0); i < header.count; i++ {
		change, next, err := readStateDomainChangeBinaryRecordAtBoundedIndex(reader, offset, logicalSize, i)
		if err != nil {
			return err
		}
		if err := build.CollectKey(change); err != nil {
			return err
		}
		offset = next
	}
	if offset != logicalSize {
		return fmt.Errorf("snapshots: state-domain-change source %q has trailing bytes", source.history.Path)
	}
	return nil
}

func stateDomainChangeBinaryCompactionV6KeyRemap(dir string, build *stateDomainChangeV6Build, source stateDomainChangeBinaryCompactionSource, segment io.ReaderAt) ([]uint32, error) {
	if build == nil || segment == nil || source.segmentHeader.version != stateDomainChangeBinaryVersionV6 {
		return nil, errors.New("snapshots: invalid V6 compaction key remap source")
	}
	accessor, accessorHeader, accessorSize, err := openStateDomainChangeBinaryAccessorReader(dir, source.accessor)
	if err != nil {
		return nil, err
	}
	defer accessor.Close()
	if (accessorHeader.version != stateDomainChangeBinaryVersionV6 && accessorHeader.version != stateDomainChangeBinaryVersionV7) || accessorHeader.fromTxNum != source.segmentHeader.fromTxNum || accessorHeader.toTxNum != source.segmentHeader.toTxNum || accessorHeader.count != source.segmentHeader.count {
		return nil, fmt.Errorf("snapshots: V6 accessor %q changed while building key remap", source.accessor.Path)
	}
	if err := verifyStateDomainChangeBinaryV6DictionaryPair(segment, accessor, accessorSize); err != nil {
		return nil, err
	}
	h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(accessor, accessorSize)
	if err != nil {
		return nil, err
	}
	if h.keyCount > uint64(^uint(0)>>1) {
		return nil, errors.New("snapshots: V6 source dictionary exceeds addressable memory")
	}
	remap := make([]uint32, int(h.keyCount))
	_, err = iterateStateDomainChangeBinaryAccessorV6Keys(accessor, accessorSize, func(sourceKeyID uint32, key []byte) error {
		targetKeyID, err := build.KeyID(key)
		if err != nil {
			return err
		}
		remap[sourceKeyID] = targetKeyID
		return nil
	})
	if err != nil {
		return nil, err
	}
	return remap, nil
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

func writeStateDomainChangeBinaryCompactionTxRanges(dir string, dst stateDomainChangeBinaryWriteAtWriter, sources []stateDomainChangeBinaryCompactionSource) (uint64, error) {
	if err := writeStateDomainChangeBinaryTxRangeCount(dst, 0); err != nil {
		return 0, err
	}
	var (
		written  uint64
		entryRaw [stateDomainChangeBinaryTxRangeSize]byte
	)
	if err := iterateMergedStateDomainChangeBinaryCompactionTxRanges(dir, sources, func(row *rawdb.StateTxRange) error {
		if written == math.MaxUint64 {
			return errors.New("snapshots: compacted state-domain-change tx range count overflows")
		}
		if err := putStateDomainChangeBinaryTxRangeEntry(&entryRaw, row); err != nil {
			return err
		}
		if _, err := dst.Write(entryRaw[:]); err != nil {
			return err
		}
		written++
		return nil
	}); err != nil {
		return 0, err
	}
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], written)
	if _, err := dst.WriteAt(raw[:], int64(stateDomainChangeBinaryTxRangeTableStart(stateDomainChangeBinaryVersionV6))); err != nil {
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
			offset := stateDomainChangeBinaryTxRangeTableStart(header.version) + 8 + i*stateDomainChangeBinaryTxRangeSize
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

func copyStateDomainChangeBinarySegmentPayload(dir string, dst *stateDomainChangeHistoryRecordWriter, build *stateDomainChangeV6Build, source stateDomainChangeBinaryCompactionSource, progress *historyCompactionProgress, recordBase uint64) error {
	if dst == nil || build == nil {
		return errors.New("snapshots: nil compacted state-domain-change record writer")
	}
	if source.segmentSize < source.recordOffset {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q size %d below record offset %d", source.history.Path, source.segmentSize, source.recordOffset)
	}
	reader, header, logicalSize, err := openStateDomainChangeBinarySegmentSequentialReader(dir, source.history)
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
	var v6KeyRemap []uint32
	if header.version == stateDomainChangeBinaryVersionV6 {
		v6KeyRemap, err = stateDomainChangeBinaryCompactionV6KeyRemap(dir, build, source, reader)
		if err != nil {
			return err
		}
		progress.addRemapRows(uint64(len(v6KeyRemap)))
	}
	var (
		compressedRecords *compressedBlockReader
		rangeReader       historySegmentReader
		rangeCursor       *stateDomainChangeTxRangeCursor
	)
	if header.version == stateDomainChangeBinaryVersionV5 {
		if contextual, ok := reader.(*stateDomainChangeHistoryReader); ok {
			compressedRecords, _ = contextual.historySegmentReader.(*compressedBlockReader)
		}
		if compressedRecords != nil {
			var rangeHeader stateDomainChangeBinaryHeader
			var rangeSize uint64
			rangeReader, rangeHeader, rangeSize, err = openStateDomainChangeBinarySegmentSequentialReader(dir, source.history)
			if err != nil {
				return err
			}
			defer rangeReader.Close()
			if rangeHeader != header || rangeSize != logicalSize {
				return fmt.Errorf("snapshots: state-domain-change binary segment %q changed while opening compaction range reader", source.history.Path)
			}
			rangeSource := io.ReaderAt(rangeReader)
			if contextual, ok := rangeReader.(*stateDomainChangeHistoryReader); ok {
				rangeSource = contextual.historySegmentReader
			}
			rangeCursor, err = newStateDomainChangeTxRangeCursor(rangeSource, rangeSize, source.history, rangeHeader)
			if err != nil {
				return err
			}
		}
	}
	// Stream-decode every source version and emit v5 frames plus txNum index
	// entries. This keeps
	// compaction O(one record) in memory while ensuring legacy v1/v2 segments do
	// not leak duplicated block/hash/seq/next fields into merged cold history,
	// without rescanning the newly compressed output to build its index.
	offset := recordOffset
	var v5Payloads [2][]byte
	var v5Changes [2]rawdb.StateDomainChange
	var compressedV5Scratch []byte
	var compressedV5Change rawdb.StateDomainChange
	var v6Payload []byte
	var v6Change rawdb.StateDomainChange
	for recordIndex := uint64(0); recordIndex < header.count; recordIndex++ {
		var (
			change *rawdb.StateDomainChange
			next   uint64
		)
		borrowedV5 := false
		if compressedRecords != nil {
			var payload []byte
			var borrowed bool
			payload, next, borrowed, err = compressedRecords.ReadRecordFrameAt(offset, compressedV5Scratch)
			if err == nil {
				if !borrowed {
					compressedV5Scratch = payload
				}
				err = decodeStateDomainChangeRecordV5Into(&compressedV5Change, payload)
			}
			if err == nil {
				var row *rawdb.StateTxRange
				row, err = rangeCursor.txRangeForTxNum(compressedV5Change.TxNum)
				if err == nil {
					err = hydrateStateDomainChangeBinaryRecordV5FromRange(row, recordIndex, &compressedV5Change)
				}
			}
			change = &compressedV5Change
			borrowedV5 = true
		} else if header.version == stateDomainChangeBinaryVersionV6 {
			var sourceKeyID uint32
			v6Payload, sourceKeyID, next, err = readStateDomainChangeBinaryRecordV6FrameInto(reader, offset, logicalSize, recordIndex, v6Payload, &v6Change)
			if err == nil && uint64(sourceKeyID) >= uint64(len(v6KeyRemap)) {
				err = errors.New("snapshots: V6 history key id outside source remap")
			}
			change = &v6Change
			if err == nil {
				err = dst.WriteBorrowedV6Change(change, v6KeyRemap[sourceKeyID])
			}
		} else if header.version == stateDomainChangeBinaryVersionV5 {
			// The writer retains only the immediately previous row for its order
			// check. Ping-pong two payload/change slots so Key and Prev remain
			// immutable across that comparison without allocating per record.
			slot := int(recordIndex & 1)
			v5Payloads[slot], next, err = readStateDomainChangeBinaryRecordV5FrameInto(reader, offset, logicalSize, recordIndex, v5Payloads[slot], &v5Changes[slot])
			change = &v5Changes[slot]
		} else {
			change, next, err = readStateDomainChangeBinaryRecordAtBoundedIndex(reader, offset, logicalSize, recordIndex)
		}
		if err != nil {
			return err
		}
		if header.version == stateDomainChangeBinaryVersionV6 {
			// The direct-remap branch already emitted the row without materializing
			// its logical key.
		} else if borrowedV5 {
			if err := dst.WriteBorrowedV5Change(change); err != nil {
				return err
			}
		} else {
			if err := dst.WriteChange(change); err != nil {
				return err
			}
		}
		offset = next
		if recordIndex&4095 == 4095 {
			progress.setRecordsProcessed(recordBase + recordIndex + 1)
		}
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
	return verifyStateDomainChangeBinaryCompanionsAgainstSegmentContext(context.Background(), dir, historyRef, indexRef, accessorRef)
}

func verifyStateDomainChangeBinaryCompanionChecksumsContext(ctx context.Context, dir string, historyRef, indexRef, accessorRef SegmentRef) error {
	for _, ref := range []SegmentRef{historyRef, indexRef, accessorRef} {
		if strings.TrimSpace(ref.Checksum) == "" {
			return fmt.Errorf("snapshots: segment %q has no checksum for cached semantic verification", ref.Path)
		}
	}
	if err := checkStateDomainChangeBinarySegmentChecksumContext(ctx, dir, historyRef); err != nil {
		return err
	}
	if err := checkStateDomainChangeBinaryIndexChecksumContext(ctx, dir, indexRef); err != nil {
		return err
	}
	return checkStateDomainChangeBinaryAccessorChecksumContext(ctx, dir, accessorRef)
}

func verifyStateDomainChangeBinaryCompanionsAgainstSegmentContext(ctx context.Context, dir string, historyRef, indexRef, accessorRef SegmentRef) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Index coverage below already walks every history record in physical order.
	// Keep the independent physical checksum gate here, then fold record framing,
	// range, ordering, and trailing-byte validation into that one coverage pass.
	if err := checkStateDomainChangeBinarySegmentChecksumContext(ctx, dir, historyRef); err != nil {
		return err
	}
	if err := checkStateDomainChangeBinaryIndexChecksumContext(ctx, dir, indexRef); err != nil {
		return err
	}
	if err := checkStateDomainChangeBinaryAccessorChecksumContext(ctx, dir, accessorRef); err != nil {
		return err
	}

	segment, segmentHeader, segmentSize, err := openStateDomainChangeBinarySegmentReader(dir, historyRef)
	if err != nil {
		return err
	}
	defer segment.Close()
	segmentReader := contextReaderAt{ctx: ctx, r: segment}
	recordOffset, err := validateStateDomainChangeBinaryTxRangeTableAt(segmentReader, segmentSize, historyRef, segmentHeader)
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
	indexReader := contextReaderAt{ctx: ctx, r: indexFile}
	if indexHeader.fromTxNum != segmentHeader.fromTxNum || indexHeader.toTxNum != segmentHeader.toTxNum {
		return fmt.Errorf("snapshots: state-domain-change binary index %q range [%d,%d], want segment range [%d,%d]",
			indexRef.Path, indexHeader.fromTxNum, indexHeader.toTxNum, segmentHeader.fromTxNum, segmentHeader.toTxNum)
	}

	accessorFile, accessorHeader, accessorSize, err := openStateDomainChangeBinaryAccessorReader(dir, accessorRef)
	if err != nil {
		return err
	}
	accessorOwned := true
	defer func() {
		if accessorOwned {
			_ = accessorFile.Close()
		}
	}()
	accessorReader := contextReaderAt{ctx: ctx, r: accessorFile}
	if accessorHeader.fromTxNum != segmentHeader.fromTxNum || accessorHeader.toTxNum != segmentHeader.toTxNum {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q range [%d,%d], want segment range [%d,%d]",
			accessorRef.Path, accessorHeader.fromTxNum, accessorHeader.toTxNum, segmentHeader.fromTxNum, segmentHeader.toTxNum)
	}
	if accessorHeader.count != segmentHeader.count {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q count %d, want segment count %d", accessorRef.Path, accessorHeader.count, segmentHeader.count)
	}
	if accessorHeader.version != stateDomainChangeBinaryVersionV4 && accessorHeader.version != stateDomainChangeBinaryVersionV5 {
		// Current accessors are rebuilt deterministically below; byte-identical output proves their
		// complete structure and semantics in one gate. Legacy layouts retain the
		// standalone structural scan before their older coverage algorithms.
		if err := CheckStateDomainChangeAccessorSegmentContext(ctx, dir, accessorRef); err != nil {
			return err
		}
	}

	if accessorHeader.version == stateDomainChangeBinaryVersionV4 || accessorHeader.version == stateDomainChangeBinaryVersionV5 {
		var collectors *stateDomainChangeBinaryAccessorV4Collectors
		var scratch string
		if accessorHeader.version == stateDomainChangeBinaryVersionV5 {
			collectors, scratch, err = newStateDomainChangeAccessorV5VerificationCollectors(dir)
		} else {
			collectors, scratch, err = newStateDomainChangeAccessorVerificationCollectors(dir)
		}
		if err != nil {
			return err
		}
		defer func() {
			collectors.Close()
			_ = os.RemoveAll(scratch)
		}()
		if err := verifyStateDomainChangeBinaryIndexCoverageWithVisitor(historyRef, indexRef, segmentReader, segmentSize, recordOffset, segmentHeader.count, indexReader, indexHeader.count, collectors.Collect); err != nil {
			return err
		}
		return verifyStateDomainChangeBinaryAccessorV4CollectedContext(ctx, scratch, accessorRef, segmentHeader.count, accessorReader, accessorSize, accessorHeader, collectors)
	}
	if err := verifyStateDomainChangeBinaryIndexCoverage(historyRef, indexRef, segmentReader, segmentSize, recordOffset, segmentHeader.count, indexReader, indexHeader.count); err != nil {
		return err
	}
	if accessorHeader.version == stateDomainChangeBinaryVersionV3 {
		return verifyStateDomainChangeBinaryAccessorV3Coverage(historyRef, accessorRef, segmentReader, segmentSize, segmentHeader.count, indexReader, indexHeader.count, accessorReader, accessorSize, accessorHeader)
	}
	if accessorHeader.version == stateDomainChangeBinaryVersionV6 {
		v6Header, err := decodeStateDomainChangeBinaryAccessorV6Header(accessorReader, accessorSize)
		if err != nil {
			return err
		}
		segmentDigest, err := readStateDomainChangeBinaryV6DictionaryCommitment(segmentReader)
		if err != nil {
			return err
		}
		if segmentDigest != v6Header.dictionaryDigest {
			return errors.New("snapshots: V6 history/accessor dictionary commitment mismatch")
		}
		if err := checkStateDomainChangeBinaryAccessorV6(accessorReader, accessorSize); err != nil {
			return err
		}
		return verifyStateDomainChangeBinaryAccessorV6Coverage(segmentReader, segmentSize, accessorReader, accessorSize)
	}
	if accessorHeader.version == stateDomainChangeBinaryVersionV7 {
		v7Header, err := decodeStateDomainChangeBinaryAccessorV7Header(accessorReader, accessorSize)
		if err != nil {
			return err
		}
		segmentDigest, err := readStateDomainChangeBinaryV6DictionaryCommitment(segmentReader)
		if err != nil {
			return err
		}
		if segmentDigest != v7Header.dictionaryDigest {
			return errors.New("snapshots: V7 history/accessor dictionary commitment mismatch")
		}
		if err := checkStateDomainChangeBinaryAccessorV7(accessorReader, accessorSize); err != nil {
			return err
		}
		return verifyStateDomainChangeBinaryAccessorV7Coverage(segmentReader, segmentSize, accessorReader, accessorSize)
	}
	return verifyStateDomainChangeBinaryAccessorCoverage(historyRef, accessorRef, segmentReader, segmentSize, recordOffset, segmentHeader.count, accessorReader, accessorSize, accessorHeader.count)
}

func verifyStateDomainChangeBinaryIndexCoverage(historyRef, indexRef SegmentRef, segment io.ReaderAt, segmentSize, recordOffset, recordCount uint64, index io.ReaderAt, indexCount uint64) error {
	return verifyStateDomainChangeBinaryIndexCoverageWithVisitor(historyRef, indexRef, segment, segmentSize, recordOffset, recordCount, index, indexCount, nil)
}

func verifyStateDomainChangeBinaryIndexCoverageWithVisitor(historyRef, indexRef SegmentRef, segment io.ReaderAt, segmentSize, recordOffset, recordCount uint64, index io.ReaderAt, indexCount uint64, visit func(*rawdb.StateDomainChange, uint64, uint64) error) error {
	expectedRecordIndex := uint64(0)
	expectedOffset := recordOffset
	var previousTxNum uint64
	for i := uint64(0); i < indexCount; i++ {
		entry, err := readStateDomainChangeBinaryIndexEntryAt(index, i)
		if err != nil {
			return err
		}
		if entry.txNum < indexRef.FromTxNum || entry.txNum > indexRef.ToTxNum {
			return fmt.Errorf("snapshots: state-domain-change binary index %q tx %d outside range [%d,%d]", indexRef.Path, entry.txNum, indexRef.FromTxNum, indexRef.ToTxNum)
		}
		if i > 0 && entry.txNum <= previousTxNum {
			return fmt.Errorf("snapshots: state-domain-change binary index %q entries are not sorted", indexRef.Path)
		}
		previousTxNum = entry.txNum
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
		var previousSeq uint64
		for j := uint64(0); j < entry.count; j++ {
			recordIndex := entry.recordIndex + j
			change, next, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segment, offset, segmentSize, recordIndex)
			if err != nil {
				return err
			}
			if change.TxNum != entry.txNum {
				return fmt.Errorf("snapshots: state-domain-change binary index %q tx %d read segment tx %d", indexRef.Path, entry.txNum, change.TxNum)
			}
			if j > 0 && change.Seq < previousSeq {
				return errors.New("snapshots: state-domain-change entries are not sorted")
			}
			previousSeq = change.Seq
			if visit != nil {
				if err := visit(change, offset, recordIndex); err != nil {
					return err
				}
			}
			offset = next
		}
		expectedRecordIndex += entry.count
		expectedOffset = offset
	}
	if expectedRecordIndex != recordCount {
		return fmt.Errorf("snapshots: state-domain-change binary index %q missing segment record %d", indexRef.Path, expectedRecordIndex)
	}
	if expectedOffset != segmentSize {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q has %d trailing bytes", historyRef.Path, segmentSize-expectedOffset)
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
	tempDir := filepath.Dir(abs)
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return nil, "", err
	}
	return createStateDomainChangeBinaryTempFileInDir(tempDir, filepath.Base(abs))
}

func createStateDomainChangeBinaryTempFileInDir(dir, base string) (*os.File, string, error) {
	tmp, err := os.CreateTemp(dir, "."+base+".*.tmp")
	if err != nil {
		return nil, "", err
	}
	return tmp, tmp.Name(), nil
}

func closeAndHashStateDomainChangeBinaryTemp(file *os.File, tmpName string) (uint64, string, error) {
	if err := syncAndCloseStateDomainChangeBinaryTemp(file); err != nil {
		return 0, "", err
	}
	return stateDomainChangeBinaryFileMetadata(tmpName)
}

func syncAndCloseStateDomainChangeBinaryTemp(file *os.File) error {
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return nil
}

func publishStateDomainChangeBinaryTemp(tmpName, finalAbs string) error {
	if err := os.MkdirAll(filepath.Dir(finalAbs), 0o755); err != nil {
		return err
	}
	return os.Rename(tmpName, finalAbs)
}

func stateDomainChangeBinaryFileMetadata(path string) (uint64, string, error) {
	return stateDomainChangeBinaryFileMetadataContext(context.Background(), path)
}

func stateDomainChangeBinaryFileMetadataContext(ctx context.Context, path string) (uint64, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer file.Close()
	hash := sha256.New()
	size, err := copyStateDomainChangeHistoryData(hash, contextReader{ctx: ctx, r: file})
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
	putStateDomainChangeBinaryIndexEntry(&raw, entry)
	_, err := w.Write(raw[:])
	return err
}

func putStateDomainChangeBinaryIndexEntry(raw *[stateDomainChangeBinaryIndexEntrySize]byte, entry stateDomainChangeBinaryTxOffset) {
	binary.BigEndian.PutUint64(raw[0:8], entry.txNum)
	binary.BigEndian.PutUint64(raw[8:16], entry.offset)
	binary.BigEndian.PutUint64(raw[16:24], entry.recordIndex)
	binary.BigEndian.PutUint64(raw[24:32], entry.count)
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
	for n > 0 {
		chunk := uint64(len(stateDomainChangeBinaryZeroes))
		if n < chunk {
			chunk = n
		}
		if _, err := w.Write(stateDomainChangeBinaryZeroes[:chunk]); err != nil {
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
	if err := checkStateDomainChangeBinaryIndexChecksum(dir, ref); err != nil {
		return nil, err
	}
	reader, header, err := openStateDomainChangeBinaryIndexReader(dir, ref)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	if header.fromTxNum != ref.FromTxNum || header.toTxNum != ref.ToTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change binary index %q range [%d,%d], want [%d,%d]", ref.Path, header.fromTxNum, header.toTxNum, ref.FromTxNum, ref.ToTxNum)
	}
	index := make([]stateDomainChangeBinaryTxOffset, 0, header.count)
	for i := uint64(0); i < header.count; i++ {
		entry, err := readStateDomainChangeBinaryIndexEntryAt(reader, i)
		if err != nil {
			return nil, err
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
	}
	return index, nil
}

func checkStateDomainChangeBinaryIndex(dir string, ref SegmentRef) error {
	if err := checkStateDomainChangeBinaryIndexChecksum(dir, ref); err != nil {
		return err
	}
	indexFile, header, err := openStateDomainChangeBinaryIndexReader(dir, ref)
	if err != nil {
		return err
	}
	defer indexFile.Close()

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

// checkStateDomainChangeBinaryIndexChecksum validates the immutable index
// object identity without walking its entry table. Companion coverage already
// consumes every entry and can enforce the structural invariants in that pass.
func checkStateDomainChangeBinaryIndexChecksum(dir string, ref SegmentRef) error {
	return checkStateDomainChangeBinaryIndexChecksumContext(context.Background(), dir, ref)
}

func checkStateDomainChangeBinaryIndexChecksumContext(ctx context.Context, dir string, ref SegmentRef) error {
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentInverted {
		return fmt.Errorf("snapshots: state-domain-change binary index %q is %s/%s, want state-domain-change/inverted", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return err
	}
	if ref.Checksum == "" {
		return nil
	}
	indexFile, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return err
	}
	defer indexFile.Close()
	if info, err := indexFile.Stat(); err != nil {
		return err
	} else if ref.Size != 0 && uint64(info.Size()) != ref.Size {
		return fmt.Errorf("snapshots: segment %q size %d, want %d", ref.Path, info.Size(), ref.Size)
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, contextReader{ctx: ctx, r: indexFile}); err != nil {
		return err
	}
	if got := "sha256:" + hex.EncodeToString(hash.Sum(nil)); got != ref.Checksum {
		return fmt.Errorf("snapshots: segment %q checksum %s, want %s", ref.Path, got, ref.Checksum)
	}
	return nil
}

func checkStateDomainChangeBinarySegment(dir string, ref SegmentRef) error {
	if err := checkStateDomainChangeBinarySegmentChecksum(dir, ref); err != nil {
		return err
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

// checkStateDomainChangeBinarySegmentChecksum validates the immutable physical
// object identity without decoding its logical record stream. Callers which
// already cover every record (notably companion verification) can share this
// gate and avoid a second decompression pass.
func checkStateDomainChangeBinarySegmentChecksum(dir string, ref SegmentRef) error {
	return checkStateDomainChangeBinarySegmentChecksumContext(context.Background(), dir, ref)
}

func checkStateDomainChangeBinarySegmentChecksumContext(ctx context.Context, dir string, ref SegmentRef) error {
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
		return fmt.Errorf("snapshots: state-domain-change binary segment %q is %s/%s, want state-domain-change/history", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return err
	}
	if ref.Checksum != "" {
		f, err := os.Open(filepath.Join(dir, ref.Path))
		if err != nil {
			return err
		}
		if info, statErr := f.Stat(); statErr != nil {
			_ = f.Close()
			return statErr
		} else if ref.Size != 0 && uint64(info.Size()) != ref.Size {
			_ = f.Close()
			return fmt.Errorf("snapshots: segment %q size %d, want %d", ref.Path, info.Size(), ref.Size)
		}
		h := sha256.New()
		_, copyErr := io.Copy(h, contextReader{ctx: ctx, r: f})
		_ = f.Close()
		if copyErr != nil {
			return copyErr
		}
		if got := "sha256:" + hex.EncodeToString(h.Sum(nil)); got != ref.Checksum {
			return fmt.Errorf("snapshots: segment %q checksum %s, want %s", ref.Path, got, ref.Checksum)
		}
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
	if header.version == stateDomainChangeBinaryVersionV5 {
		return readStateDomainChangeBinaryAccessorV5Debug(dir, ref, accessorFile, fileSize, header)
	}
	if header.version == stateDomainChangeBinaryVersionV6 {
		return readStateDomainChangeBinaryAccessorV6Debug(accessorFile, fileSize)
	}
	if header.version == stateDomainChangeBinaryVersionV7 {
		return readStateDomainChangeBinaryAccessorV7Debug(accessorFile, fileSize)
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
	return checkStateDomainChangeBinaryAccessorContext(context.Background(), dir, ref)
}

func checkStateDomainChangeBinaryAccessorChecksumContext(ctx context.Context, dir string, ref SegmentRef) error {
	if ref.Dataset != SegmentDatasetStateDomainChange || ref.Kind != SegmentAccessor {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q is %s/%s, want state-domain-change/accessor", ref.Path, ref.Dataset, ref.Kind)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return err
	}
	if strings.TrimSpace(ref.Checksum) == "" {
		return fmt.Errorf("snapshots: segment %q has no checksum", ref.Path)
	}
	size, checksum, err := stateDomainChangeBinaryFileMetadataContext(ctx, filepath.Join(dir, ref.Path))
	if err != nil {
		return err
	}
	if ref.Size != 0 && size != ref.Size {
		return fmt.Errorf("snapshots: segment %q size %d, want %d", ref.Path, size, ref.Size)
	}
	if checksum != ref.Checksum {
		return fmt.Errorf("snapshots: segment %q checksum %s, want %s", ref.Path, checksum, ref.Checksum)
	}
	return nil
}

func checkStateDomainChangeBinaryAccessorContext(ctx context.Context, dir string, ref SegmentRef) error {
	return checkStateDomainChangeBinaryAccessorValidationContext(ctx, dir, ref, true)
}

func checkStateDomainChangeBinaryAccessorLayoutContext(ctx context.Context, dir string, ref SegmentRef) error {
	return checkStateDomainChangeBinaryAccessorValidationContext(ctx, dir, ref, false)
}

func checkStateDomainChangeBinaryAccessorValidationContext(ctx context.Context, dir string, ref SegmentRef, verifyChecksum bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Entry validation runs over the logical (uncompressed) view; the checksum is
	// over the physical (possibly compressed) file bytes.
	accessorFile, header, fileSize, err := openStateDomainChangeBinaryAccessorReader(dir, ref)
	if err != nil {
		return err
	}
	defer accessorFile.Close()
	accessorReader := contextReaderAt{ctx: ctx, r: accessorFile}

	if verifyChecksum && ref.Checksum != "" {
		_, got, err := stateDomainChangeBinaryFileMetadataContext(ctx, filepath.Join(dir, ref.Path))
		if err != nil {
			return err
		}
		if got != ref.Checksum {
			return fmt.Errorf("snapshots: segment %q checksum %s, want %s", ref.Path, got, ref.Checksum)
		}
	}
	if (header.version == stateDomainChangeBinaryVersionV3 || header.version == stateDomainChangeBinaryVersionV4 || header.version == stateDomainChangeBinaryVersionV5 || header.version == stateDomainChangeBinaryVersionV6 || header.version == stateDomainChangeBinaryVersionV7) && fileSize > math.MaxInt64 {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q logical size %d exceeds int64", ref.Path, fileSize)
	}
	if header.version == stateDomainChangeBinaryVersionV3 {
		buffered := acquireStateDomainChangeAccessorValidationReader(accessorReader, fileSize)
		defer releaseStateDomainChangeAccessorValidationReader(&buffered)
		return checkStateDomainChangeBinaryAccessorV3(buffered, fileSize, header)
	}
	if header.version == stateDomainChangeBinaryVersionV4 {
		buffered := acquireStateDomainChangeAccessorValidationReader(accessorReader, fileSize)
		defer releaseStateDomainChangeAccessorValidationReader(&buffered)
		return checkStateDomainChangeBinaryAccessorV4(buffered, fileSize, header)
	}
	if header.version == stateDomainChangeBinaryVersionV5 {
		buffered := acquireStateDomainChangeAccessorValidationReader(accessorReader, fileSize)
		defer releaseStateDomainChangeAccessorValidationReader(&buffered)
		return checkStateDomainChangeBinaryAccessorV5(buffered, fileSize, header)
	}
	if header.version == stateDomainChangeBinaryVersionV6 {
		return checkStateDomainChangeBinaryAccessorV6(accessorReader, fileSize)
	}
	if header.version == stateDomainChangeBinaryVersionV7 {
		return checkStateDomainChangeBinaryAccessorV7(accessorReader, fileSize)
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
		entryOffset, err := readStateDomainChangeBinaryAccessorEntryOffsetAt(accessorReader, i)
		if err != nil {
			return err
		}
		if entryOffset < minOffset || entryOffset >= fileSize {
			return fmt.Errorf("snapshots: state-domain-change binary accessor %q entry %d offset %d outside payload", ref.Path, i, entryOffset)
		}
		if i > 0 && entryOffset <= prevOffset {
			return fmt.Errorf("snapshots: state-domain-change binary accessor %q entry offsets are not strictly increasing", ref.Path)
		}
		entry, next, err := readStateDomainChangeBinaryAccessorEntryAtOffsetWithNextBounded(accessorReader, entryOffset, fileSize)
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
	accessorOwned := true
	defer func() {
		if accessorOwned {
			_ = accessorFile.Close()
		}
	}()
	if accessorHeader.fromTxNum != ref.FromTxNum || accessorHeader.toTxNum != ref.ToTxNum {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q range [%d,%d], want [%d,%d]", accessorRef.Path, accessorHeader.fromTxNum, accessorHeader.toTxNum, ref.FromTxNum, ref.ToTxNum)
	}
	if accessorHeader.version == stateDomainChangeBinaryVersionV3 {
		return iterateStateDomainChangeBinarySegmentByAccessorV3Key(segmentFile, segmentSize, accessorFile, accessorSize, accessorHeader, lookupKey, fromTxNum, toTxNum, fn)
	}
	if accessorHeader.version == stateDomainChangeBinaryVersionV4 {
		return iterateStateDomainChangeBinarySegmentByAccessorV4Key(segmentFile, segmentSize, accessorFile, accessorSize, accessorHeader, lookupKey, fromTxNum, toTxNum, fn)
	}
	if accessorHeader.version == stateDomainChangeBinaryVersionV5 {
		return iterateStateDomainChangeBinarySegmentByAccessorV5Key(segmentFile, segmentSize, accessorFile, accessorSize, accessorHeader, lookupKey, fromTxNum, toTxNum, fn)
	}
	if accessorHeader.version == stateDomainChangeBinaryVersionV6 {
		if history, ok := segmentFile.(*stateDomainChangeHistoryReader); ok {
			if err := history.attachV6Accessor(accessorFile, accessorSize); err != nil {
				return err
			}
			accessorOwned = false
		}
		return iterateStateDomainChangeBinarySegmentByAccessorV6Key(segmentFile, segmentSize, accessorFile, accessorSize, lookupKey, fromTxNum, toTxNum, fn)
	}
	if accessorHeader.version == stateDomainChangeBinaryVersionV7 {
		if history, ok := segmentFile.(*stateDomainChangeHistoryReader); ok {
			if err := history.attachV6Accessor(accessorFile, accessorSize); err != nil {
				return err
			}
			accessorOwned = false
		}
		return iterateStateDomainChangeBinarySegmentByAccessorV7Key(segmentFile, segmentSize, accessorFile, accessorSize, lookupKey, fromTxNum, toTxNum, fn)
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
	accessorOwned := true
	defer func() {
		if accessorOwned {
			_ = accessorFile.Close()
		}
	}()
	if accessorHeader.fromTxNum != ref.FromTxNum || accessorHeader.toTxNum != ref.ToTxNum {
		return fmt.Errorf("snapshots: state-domain-change binary accessor %q range [%d,%d], want [%d,%d]", accessorRef.Path, accessorHeader.fromTxNum, accessorHeader.toTxNum, ref.FromTxNum, ref.ToTxNum)
	}
	if accessorHeader.version == stateDomainChangeBinaryVersionV3 {
		return iterateStateDomainChangeBinarySegmentByAccessorV3Prefix(segmentFile, segmentSize, accessorFile, accessorSize, accessorHeader, lookupPrefix, fromTxNum, toTxNum, fn)
	}
	if accessorHeader.version == stateDomainChangeBinaryVersionV4 {
		return iterateStateDomainChangeBinarySegmentByAccessorV4Prefix(segmentFile, segmentSize, accessorFile, accessorSize, accessorHeader, lookupPrefix, fromTxNum, toTxNum, fn)
	}
	if accessorHeader.version == stateDomainChangeBinaryVersionV5 {
		return iterateStateDomainChangeBinarySegmentByAccessorV5Prefix(segmentFile, segmentSize, accessorFile, accessorSize, accessorHeader, lookupPrefix, fromTxNum, toTxNum, fn)
	}
	if accessorHeader.version == stateDomainChangeBinaryVersionV6 {
		if history, ok := segmentFile.(*stateDomainChangeHistoryReader); ok {
			if err := history.attachV6Accessor(accessorFile, accessorSize); err != nil {
				return err
			}
			accessorOwned = false
		}
		return iterateStateDomainChangeBinarySegmentByAccessorV6Prefix(segmentFile, segmentSize, accessorFile, accessorSize, lookupPrefix, fromTxNum, toTxNum, fn)
	}
	if accessorHeader.version == stateDomainChangeBinaryVersionV7 {
		if history, ok := segmentFile.(*stateDomainChangeHistoryReader); ok {
			if err := history.attachV6Accessor(accessorFile, accessorSize); err != nil {
				return err
			}
			accessorOwned = false
		}
		return iterateStateDomainChangeBinarySegmentByAccessorV7Prefix(segmentFile, segmentSize, accessorFile, accessorSize, lookupPrefix, fromTxNum, toTxNum, fn)
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
	writeStateDomainChangeBinaryHeaderVersion(&buf, stateDomainChangeBinarySegmentMagic, fromTxNum, toTxNum, uint64(len(changes)), stateDomainChangeBinaryVersionV5)
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
	var raw [stateDomainChangeBinaryTxRangeSize]byte
	for i, row := range txRanges {
		if row == nil {
			return fmt.Errorf("snapshots: nil state tx range entry %d", i)
		}
		if err := putStateDomainChangeBinaryTxRangeEntry(&raw, row); err != nil {
			return err
		}
		if _, err := w.Write(raw[:]); err != nil {
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

func putStateDomainChangeBinaryTxRangeEntry(raw *[stateDomainChangeBinaryTxRangeSize]byte, row *rawdb.StateTxRange) error {
	if row == nil {
		return errors.New("snapshots: nil state tx range entry")
	}
	if row.EndTxNum < row.BeginTxNum {
		return fmt.Errorf("snapshots: state tx range for block %d is inverted", row.BlockNum)
	}
	binary.BigEndian.PutUint64(raw[0:8], row.BlockNum)
	copy(raw[8:8+common.HashLength], row.BlockHash[:])
	binary.BigEndian.PutUint64(raw[8+common.HashLength:16+common.HashLength], row.BeginTxNum)
	binary.BigEndian.PutUint64(raw[16+common.HashLength:24+common.HashLength], row.EndTxNum)
	return nil
}

func decodeStateDomainChangeBinaryTxRangeTable(ref SegmentRef, header stateDomainChangeBinaryHeader, data []byte) ([]*rawdb.StateTxRange, []byte, error) {
	if header.version == stateDomainChangeBinaryVersionV1 {
		return nil, data, nil
	}
	if header.version != stateDomainChangeBinaryVersionV2 && header.version != stateDomainChangeBinaryVersionV5 && header.version != stateDomainChangeBinaryVersionV6 {
		return nil, nil, fmt.Errorf("snapshots: unsupported state-domain-change binary version %d", header.version)
	}
	if header.version == stateDomainChangeBinaryVersionV6 {
		if len(data) < stateDomainChangeBinaryV6DictionaryCommitmentSize {
			return nil, nil, io.ErrUnexpectedEOF
		}
		data = data[stateDomainChangeBinaryV6DictionaryCommitmentSize:]
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
	if header.version != stateDomainChangeBinaryVersionV2 && header.version != stateDomainChangeBinaryVersionV5 && header.version != stateDomainChangeBinaryVersionV6 {
		return 0, 0, fmt.Errorf("snapshots: unsupported state-domain-change binary version %d", header.version)
	}
	tableStart := stateDomainChangeBinaryTxRangeTableStart(header.version)
	if fileSize < tableStart+8 {
		return 0, 0, io.ErrUnexpectedEOF
	}
	var countRaw [8]byte
	if _, err := r.ReadAt(countRaw[:], int64(tableStart)); err != nil {
		return 0, 0, err
	}
	count := binary.BigEndian.Uint64(countRaw[:])
	if count > (math.MaxUint64-tableStart-8)/stateDomainChangeBinaryTxRangeSize {
		return 0, 0, fmt.Errorf("snapshots: state-domain-change tx range count %d overflows size", count)
	}
	payloadOffset := tableStart + 8 + count*stateDomainChangeBinaryTxRangeSize
	if payloadOffset > fileSize {
		return 0, 0, io.ErrUnexpectedEOF
	}
	return count, payloadOffset, nil
}

func stateDomainChangeBinaryTxRangeTableStart(version uint32) uint64 {
	start := uint64(stateDomainChangeBinaryHeaderSize)
	if version == stateDomainChangeBinaryVersionV6 {
		start += stateDomainChangeBinaryV6DictionaryCommitmentSize
	}
	return start
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
		row, err := readStateDomainChangeBinaryTxRangeAt(r, ref, header.version, mid)
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
	row, err := readStateDomainChangeBinaryTxRangeAt(r, ref, header.version, low)
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
		row, err := readStateDomainChangeBinaryTxRangeAt(r, ref, header.version, mid)
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
	row, err := readStateDomainChangeBinaryTxRangeAt(r, ref, header.version, low)
	if err != nil {
		return nil, err
	}
	if txNum < row.BeginTxNum || txNum > row.EndTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change tx %d is not covered by StateTxRange", txNum)
	}
	return row, nil
}

func readStateDomainChangeBinaryTxRangeAt(r io.ReaderAt, ref SegmentRef, version uint32, index uint64) (*rawdb.StateTxRange, error) {
	row := new(rawdb.StateTxRange)
	if err := readStateDomainChangeBinaryTxRangeAtInto(r, ref, version, index, row); err != nil {
		return nil, err
	}
	return row, nil
}

func readStateDomainChangeBinaryTxRangeAtInto(r io.ReaderAt, ref SegmentRef, version uint32, index uint64, row *rawdb.StateTxRange) error {
	if row == nil {
		return errors.New("snapshots: nil state-domain-change tx range target")
	}
	offset := stateDomainChangeBinaryTxRangeTableStart(version) + 8 + index*stateDomainChangeBinaryTxRangeSize
	var raw [stateDomainChangeBinaryTxRangeSize]byte
	var err error
	if compressed, ok := r.(*compressedBlockReader); ok {
		_, err = compressed.ReadAt(raw[:], int64(offset))
	} else {
		_, err = r.ReadAt(raw[:], int64(offset))
	}
	if err != nil {
		return err
	}
	decodeStateDomainChangeBinaryTxRangeInto(row, raw[:])
	if err := validateStateDomainChangeBinaryTxRange(ref, row, index, nil); err != nil {
		return err
	}
	return nil
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
		offset := stateDomainChangeBinaryTxRangeTableStart(header.version) + 8 + i*stateDomainChangeBinaryTxRangeSize
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
	row := new(rawdb.StateTxRange)
	decodeStateDomainChangeBinaryTxRangeInto(row, raw)
	return row
}

func decodeStateDomainChangeBinaryTxRangeInto(row *rawdb.StateTxRange, raw []byte) {
	row.BlockNum = binary.BigEndian.Uint64(raw[0:8])
	row.BeginTxNum = binary.BigEndian.Uint64(raw[8+common.HashLength : 16+common.HashLength])
	row.EndTxNum = binary.BigEndian.Uint64(raw[16+common.HashLength : 24+common.HashLength])
	copy(row.BlockHash[:], raw[8:8+common.HashLength])
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
	return encodeStateDomainChangeBinaryAccessorV5(fromTxNum, toTxNum, entries)
}

func decodeStateDomainChangeBinaryHeader(data []byte, magic [8]byte) (stateDomainChangeBinaryHeader, []byte, error) {
	if len(data) < stateDomainChangeBinaryHeaderSize {
		return stateDomainChangeBinaryHeader{}, nil, fmt.Errorf("snapshots: state-domain-change binary file is too small: %d bytes", len(data))
	}
	if !bytes.Equal(data[:8], magic[:]) {
		return stateDomainChangeBinaryHeader{}, nil, errors.New("snapshots: invalid state-domain-change binary magic")
	}
	version := binary.BigEndian.Uint32(data[8:12])
	if version != stateDomainChangeBinaryVersionV1 && version != stateDomainChangeBinaryVersionV2 && version != stateDomainChangeBinaryVersionV3 && version != stateDomainChangeBinaryVersionV4 && version != stateDomainChangeBinaryVersionV5 && version != stateDomainChangeBinaryVersionV6 && version != stateDomainChangeBinaryVersionV7 {
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

type stateDomainChangeBinaryV6KeyResolver interface {
	v6Key(keyID uint32) ([]byte, error)
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
	dir         string

	mu          sync.Mutex
	lastRange   *rawdb.StateTxRange
	lastIndex   uint64
	rangeCount  uint64
	rangesReady bool
	v6Accessor  historySegmentReader
	v6Size      uint64
	v6Header    stateDomainChangeBinaryAccessorV6Header
	v6Ready     bool
	v6KeyCache  map[int][]stateDomainChangeBinaryAccessorV6Record
	v6CacheFIFO []int
}

func (r *stateDomainChangeHistoryReader) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var accessorErr error
	if r.v6Accessor != nil {
		accessorErr = r.v6Accessor.Close()
		r.v6Accessor = nil
	}
	segmentErr := r.historySegmentReader.Close()
	if segmentErr != nil {
		return segmentErr
	}
	return accessorErr
}

func (r *stateDomainChangeHistoryReader) v6Key(keyID uint32) ([]byte, error) {
	if r == nil || r.header.version != stateDomainChangeBinaryVersionV6 {
		return nil, errors.New("snapshots: V6 key requested from non-V6 history reader")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.v6Accessor == nil {
		accessorRef := SegmentRef{
			Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccessor,
			FromTxNum: r.ref.FromTxNum, ToTxNum: r.ref.ToTxNum,
			AggregationSteps: r.ref.AggregationSteps, Path: stateDomainChangeBinaryAccessorPath(r.ref.Path),
		}
		accessor, header, size, err := openStateDomainChangeBinaryAccessorReader(r.dir, accessorRef)
		if err != nil {
			return nil, err
		}
		if header.version != stateDomainChangeBinaryVersionV6 && header.version != stateDomainChangeBinaryVersionV7 {
			_ = accessor.Close()
			return nil, fmt.Errorf("snapshots: key-oriented history accessor has version %d", header.version)
		}
		r.v6Accessor, r.v6Size = accessor, size
	}
	if !r.v6Ready {
		v6Header, err := decodeStateDomainChangeBinaryAccessorKeyHeader(r.v6Accessor, r.v6Size)
		if err != nil {
			return nil, err
		}
		segmentDigest, err := readStateDomainChangeBinaryV6DictionaryCommitment(r.historySegmentReader)
		if err != nil {
			return nil, err
		}
		if segmentDigest != v6Header.dictionaryDigest {
			return nil, errors.New("snapshots: V6 history/accessor dictionary commitment mismatch")
		}
		r.v6Header = v6Header
		r.v6Ready = true
		r.v6KeyCache = make(map[int][]stateDomainChangeBinaryAccessorV6Record)
	}
	if uint64(keyID) >= r.v6Header.keyCount {
		return nil, errors.New("snapshots: V6 accessor key id outside dictionary")
	}
	blockIndex := int(keyID / stateDomainChangeBinaryAccessorV6BlockKeys)
	records, ok := r.v6KeyCache[blockIndex]
	if !ok {
		block, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(r.v6Accessor, r.v6Header, uint64(blockIndex))
		if err != nil {
			return nil, err
		}
		records, err = stateDomainChangeBinaryAccessorV6ReadBlock(r.v6Accessor, r.v6Size, r.v6Header, block, uint32(blockIndex*stateDomainChangeBinaryAccessorV6BlockKeys))
		if err != nil {
			return nil, err
		}
		if len(r.v6CacheFIFO) >= stateDomainChangeV6DictionaryCacheBlocks {
			delete(r.v6KeyCache, r.v6CacheFIFO[0])
			r.v6CacheFIFO = r.v6CacheFIFO[1:]
		}
		r.v6KeyCache[blockIndex] = records
		r.v6CacheFIFO = append(r.v6CacheFIFO, blockIndex)
	}
	within := int(keyID % stateDomainChangeBinaryAccessorV6BlockKeys)
	if within >= len(records) {
		return nil, errors.New("snapshots: V6 accessor key id outside block")
	}
	return records[within].key, nil
}

func (r *stateDomainChangeHistoryReader) attachV6Accessor(accessor historySegmentReader, size uint64) error {
	if r == nil || accessor == nil || r.header.version != stateDomainChangeBinaryVersionV6 {
		return errors.New("snapshots: invalid V6 history/accessor attachment")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.v6Accessor != nil {
		return errors.New("snapshots: V6 history accessor is already attached")
	}
	h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(accessor, size)
	if err != nil {
		return err
	}
	digest, err := readStateDomainChangeBinaryV6DictionaryCommitment(r.historySegmentReader)
	if err != nil {
		return err
	}
	if digest != h.dictionaryDigest {
		return errors.New("snapshots: V6 history/accessor dictionary commitment mismatch")
	}
	r.v6Accessor, r.v6Size, r.v6Header, r.v6Ready = accessor, size, h, true
	r.v6KeyCache = make(map[int][]stateDomainChangeBinaryAccessorV6Record)
	return nil
}

// stateDomainChangeTxRangeCursor is the compaction-only, single-threaded
// counterpart of stateDomainChangeHistoryReader.txRangeForTxNum. It reuses two
// fixed rows and reads the independent range stream directly, avoiding a mutex
// and one heap object for every block crossed by a sequential record scan.
type stateDomainChangeTxRangeCursor struct {
	reader  io.ReaderAt
	ref     SegmentRef
	version uint32
	count   uint64
	index   uint64
	current rawdb.StateTxRange
	scratch rawdb.StateTxRange
	have    bool
}

func newStateDomainChangeTxRangeCursor(reader io.ReaderAt, logicalSize uint64, ref SegmentRef, header stateDomainChangeBinaryHeader) (*stateDomainChangeTxRangeCursor, error) {
	count, _, err := stateDomainChangeBinaryTxRangeTableBoundsAt(reader, logicalSize, ref, header)
	if err != nil {
		return nil, err
	}
	return &stateDomainChangeTxRangeCursor{reader: reader, ref: ref, version: header.version, count: count}, nil
}

func (c *stateDomainChangeTxRangeCursor) txRangeForTxNum(txNum uint64) (*rawdb.StateTxRange, error) {
	if c == nil || c.reader == nil {
		return nil, errors.New("snapshots: nil state-domain-change tx range cursor")
	}
	if c.have && txNum >= c.current.BeginTxNum && txNum <= c.current.EndTxNum {
		return &c.current, nil
	}
	low := uint64(0)
	if c.have && txNum > c.current.EndTxNum {
		low = c.index + 1
		if low < c.count {
			if err := readStateDomainChangeBinaryTxRangeAtInto(c.reader, c.ref, c.version, low, &c.scratch); err != nil {
				return nil, err
			}
			if txNum >= c.scratch.BeginTxNum && txNum <= c.scratch.EndTxNum {
				c.current = c.scratch
				c.index = low
				c.have = true
				return &c.current, nil
			}
		}
	}
	high := c.count
	for low < high {
		mid := low + (high-low)/2
		if err := readStateDomainChangeBinaryTxRangeAtInto(c.reader, c.ref, c.version, mid, &c.scratch); err != nil {
			return nil, err
		}
		if c.scratch.EndTxNum < txNum {
			low = mid + 1
		} else {
			high = mid
		}
	}
	if low >= c.count {
		return nil, fmt.Errorf("snapshots: state-domain-change tx %d is not covered by StateTxRange", txNum)
	}
	if err := readStateDomainChangeBinaryTxRangeAtInto(c.reader, c.ref, c.version, low, &c.scratch); err != nil {
		return nil, err
	}
	if txNum < c.scratch.BeginTxNum || txNum > c.scratch.EndTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change tx %d is not covered by StateTxRange", txNum)
	}
	c.current = c.scratch
	c.index = low
	c.have = true
	return &c.current, nil
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
		next, err := readStateDomainChangeBinaryTxRangeAt(r.historySegmentReader, r.ref, r.header.version, r.lastIndex+1)
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
		row, err := readStateDomainChangeBinaryTxRangeAt(r.historySegmentReader, r.ref, r.header.version, mid)
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
	row, err := readStateDomainChangeBinaryTxRangeAt(r.historySegmentReader, r.ref, r.header.version, low)
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
	return openHistorySegmentForReadWithCacheLimit(dir, ref, cbCacheBlocks)
}

// openHistorySegmentForSequentialRead is the Erigon-style single-pass view:
// scans retain one decompressed block and recycle it at the next block, while
// the regular opener keeps its larger MRU for keyed and binary-search reads.
func openHistorySegmentForSequentialRead(dir string, ref SegmentRef) (historySegmentReader, uint64, stateDomainChangeBinaryHeader, error) {
	return openHistorySegmentForReadWithCacheLimit(dir, ref, 1)
}

func openHistorySegmentForReadWithCacheLimit(dir string, ref SegmentRef, compressedCacheLimit int) (historySegmentReader, uint64, stateDomainChangeBinaryHeader, error) {
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
		cr, err := openCompressedBlockReaderWithCacheLimit(path, compressedCacheLimit)
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
		dir:                  dir,
	}, logicalSize, header, nil
}

func openStateDomainChangeBinarySegmentReader(dir string, ref SegmentRef) (historySegmentReader, stateDomainChangeBinaryHeader, uint64, error) {
	return openStateDomainChangeBinarySegmentReaderWithCacheLimit(dir, ref, cbCacheBlocks)
}

// openStateDomainChangeBinarySegmentSequentialReader gives compaction streams
// one recycled decoded block. Record frames and StateTxRange rows use separate
// readers, so neither sequential stream can evict the other's hot block.
func openStateDomainChangeBinarySegmentSequentialReader(dir string, ref SegmentRef) (historySegmentReader, stateDomainChangeBinaryHeader, uint64, error) {
	return openStateDomainChangeBinarySegmentReaderWithCacheLimit(dir, ref, 1)
}

// openStateDomainChangeBinarySegmentReaderWithCacheLimit delegates to the
// magic-dispatching history opener, then applies record-table sanity checks to
// the uncompressed logical size.
func openStateDomainChangeBinarySegmentReaderWithCacheLimit(dir string, ref SegmentRef, compressedCacheLimit int) (historySegmentReader, stateDomainChangeBinaryHeader, uint64, error) {
	reader, logicalSize, header, err := openHistorySegmentForReadWithCacheLimit(dir, ref, compressedCacheLimit)
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

func openStateDomainChangeBinaryIndexReader(dir string, ref SegmentRef) (historySegmentReader, stateDomainChangeBinaryHeader, error) {
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
	if header.version == stateDomainChangeBinaryIndexCurrentVersion {
		reader, err := openStateDomainChangeBinaryIndexV7Reader(file, uint64(stat.Size()), header)
		if err != nil {
			_ = file.Close()
			return nil, stateDomainChangeBinaryHeader{}, fmt.Errorf("snapshots: state-domain-change binary index %q v7 layout: %w", ref.Path, err)
		}
		return reader, header, nil
	}
	if header.version != stateDomainChangeBinaryIndexVersion {
		_ = file.Close()
		return nil, stateDomainChangeBinaryHeader{}, fmt.Errorf("snapshots: unsupported state-domain-change index version %d", header.version)
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
	} else if header.version == stateDomainChangeBinaryVersionV5 {
		if _, err := stateDomainChangeBinaryAccessorV5LayoutAt(reader, logicalSize, header); err != nil {
			_ = reader.Close()
			return nil, stateDomainChangeBinaryHeader{}, 0, fmt.Errorf("snapshots: state-domain-change binary accessor %q v5 layout: %w", ref.Path, err)
		}
	} else if header.version == stateDomainChangeBinaryVersionV6 {
		if _, err := decodeStateDomainChangeBinaryAccessorV6Header(reader, logicalSize); err != nil {
			_ = reader.Close()
			return nil, stateDomainChangeBinaryHeader{}, 0, fmt.Errorf("snapshots: state-domain-change binary accessor %q v6 layout: %w", ref.Path, err)
		}
	} else if header.version == stateDomainChangeBinaryVersionV7 {
		if _, err := decodeStateDomainChangeBinaryAccessorV7Header(reader, logicalSize); err != nil {
			_ = reader.Close()
			return nil, stateDomainChangeBinaryHeader{}, 0, fmt.Errorf("snapshots: state-domain-change binary accessor %q v7 layout: %w", ref.Path, err)
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
	if version == stateDomainChangeBinaryVersionV6 {
		keyID, decoded, decodeErr := decodeStateDomainChangeRecordV6(payload)
		change, err = decoded, decodeErr
		if err == nil {
			contextual, ok := r.(stateDomainChangeBinaryV6KeyResolver)
			if !ok {
				err = errors.New("snapshots: V6 state-domain-change record requires contextual history reader")
			} else {
				var key []byte
				key, err = contextual.v6Key(keyID)
				if err == nil {
					err = decodeStateDomainChangeBinaryAccessorKey(key, change)
				}
			}
		}
		if err == nil {
			err = hydrateStateDomainChangeBinaryRecordV5(r, fileSize, recordIndex, hydrateBlock, change)
		}
	} else if version == stateDomainChangeBinaryVersionV5 {
		change, err = decodeStateDomainChangeRecordV5(payload)
		if err == nil {
			err = hydrateStateDomainChangeBinaryRecordV5(r, fileSize, recordIndex, hydrateBlock, change)
		}
	} else {
		change, err = decodeStateDomainChangeRecord(payload)
	}
	if err != nil {
		return nil, 0, err
	}
	return change, offset + 4 + uint64(length), nil
}

func readStateDomainChangeBinaryRecordV5FrameInto(r io.ReaderAt, offset, fileSize, recordIndex uint64, payload []byte, change *rawdb.StateDomainChange) ([]byte, uint64, error) {
	if offset > math.MaxInt64 {
		return payload, 0, fmt.Errorf("snapshots: state-domain-change record offset too large: %d", offset)
	}
	if offset > fileSize || fileSize-offset < 4 {
		return payload, 0, io.ErrUnexpectedEOF
	}
	var prefix [4]byte
	if _, err := r.ReadAt(prefix[:], int64(offset)); err != nil {
		return payload, 0, err
	}
	length := binary.BigEndian.Uint32(prefix[:])
	if uint64(length) > fileSize-offset-4 {
		return payload, 0, io.ErrUnexpectedEOF
	}
	if cap(payload) < int(length) {
		payload = make([]byte, length)
	} else {
		payload = payload[:length]
	}
	if _, err := r.ReadAt(payload, int64(offset)+4); err != nil {
		return payload, 0, err
	}
	if err := decodeStateDomainChangeRecordV5Into(change, payload); err != nil {
		return payload, 0, err
	}
	if err := hydrateStateDomainChangeBinaryRecordV5(r, fileSize, recordIndex, true, change); err != nil {
		return payload, 0, err
	}
	return payload, offset + 4 + uint64(length), nil
}

// readStateDomainChangeBinaryRecordV6FrameInto decodes the compact value while
// deliberately leaving its logical key unresolved. V6 compaction supplies a
// precomputed source-keyID to target-keyID mapping, avoiding one dictionary
// block lookup and decompression for every history record.
func readStateDomainChangeBinaryRecordV6FrameInto(r io.ReaderAt, offset, fileSize, recordIndex uint64, payload []byte, change *rawdb.StateDomainChange) ([]byte, uint32, uint64, error) {
	if offset > math.MaxInt64 {
		return payload, 0, 0, fmt.Errorf("snapshots: state-domain-change record offset too large: %d", offset)
	}
	if offset > fileSize || fileSize-offset < 4 {
		return payload, 0, 0, io.ErrUnexpectedEOF
	}
	var prefix [4]byte
	if _, err := r.ReadAt(prefix[:], int64(offset)); err != nil {
		return payload, 0, 0, err
	}
	length := binary.BigEndian.Uint32(prefix[:])
	if uint64(length) > fileSize-offset-4 {
		return payload, 0, 0, io.ErrUnexpectedEOF
	}
	if cap(payload) < int(length) {
		payload = make([]byte, length)
	} else {
		payload = payload[:length]
	}
	if _, err := r.ReadAt(payload, int64(offset)+4); err != nil {
		return payload, 0, 0, err
	}
	keyID, err := decodeStateDomainChangeRecordV6Into(payload, change)
	if err != nil {
		return payload, 0, 0, err
	}
	if err := hydrateStateDomainChangeBinaryRecordV5(r, fileSize, recordIndex, true, change); err != nil {
		return payload, 0, 0, err
	}
	return payload, keyID, offset + 4 + uint64(length), nil
}

func hydrateStateDomainChangeBinaryRecordV5(r io.ReaderAt, fileSize, recordIndex uint64, hydrateBlock bool, change *rawdb.StateDomainChange) error {
	if recordIndex == math.MaxUint64 {
		return errors.New("snapshots: v5 state-domain-change record requires record index")
	}
	if !hydrateBlock {
		change.Seq = recordIndex + 1
		return nil
	}
	var (
		row *rawdb.StateTxRange
		err error
	)
	if contextual, ok := r.(*stateDomainChangeHistoryReader); ok {
		row, err = contextual.txRangeForTxNum(change.TxNum)
	} else {
		header, headerErr := readStateDomainChangeBinaryHeaderAt(r, stateDomainChangeBinarySegmentMagic)
		if headerErr != nil {
			return headerErr
		}
		ref := SegmentRef{FromTxNum: header.fromTxNum, ToTxNum: header.toTxNum}
		row, err = findStateDomainChangeBinaryTxRangeForTxNum(r, fileSize, ref, header, change.TxNum)
	}
	if err != nil {
		return err
	}
	return hydrateStateDomainChangeBinaryRecordV5FromRange(row, recordIndex, change)
}

func hydrateStateDomainChangeBinaryRecordV5FromRange(row *rawdb.StateTxRange, recordIndex uint64, change *rawdb.StateDomainChange) error {
	if change == nil {
		return errors.New("snapshots: nil v5 state-domain-change record")
	}
	if row == nil {
		return errors.New("snapshots: nil v5 state-domain-change tx range")
	}
	change.BlockNum = row.BlockNum
	change.BlockHash = row.BlockHash
	var err error
	change.Seq, err = stateDomainChangeBinaryV5Sequence(row, change.TxNum, recordIndex)
	return err
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
	return appendStateDomainChangeBinaryAccessorLookupKey(nil, flatDomain, owner, generation, domain, key)
}

func appendStateDomainChangeBinaryAccessorLookupKey(out []byte, flatDomain rawdb.StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) []byte {
	id := owner.AccountID()
	required := 1 + len(id)
	if flatDomain == rawdb.StateFlatDomainKVLatest {
		required += 8 + 2 + len(key)
	}
	if cap(out) < required {
		out = make([]byte, 0, required)
	} else {
		out = out[:0]
	}
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

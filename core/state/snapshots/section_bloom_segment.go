package snapshots

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

const (
	SectionBloomSegmentVersion      = 1
	sectionBloomHeaderSize          = 8 + 8 + 8 + 8 + 8 + 8
	sectionBloomIndexEntrySize      = 8 + 8 + 8 + 8
	sectionBloomMaxDecodedBytesSize = rawdb.SectionBloomByteSize
)

var sectionBloomMagic = [8]byte{'g', 't', 's', 'b', 'l', 'm', '1', '\n'}

type SectionBloomSegment struct {
	ref    SegmentRef
	file   *os.File
	header sectionBloomHeader
}

type sectionBloomHeader struct {
	fromBlock     uint64
	toBlock       uint64
	rowCount      uint64
	indexOffset   uint64
	payloadOffset uint64
}

type sectionBloomIndexEntry struct {
	section  uint64
	bitIndex uint64
	offset   uint64
	length   uint64
}

type sectionBloomRow struct {
	section  uint64
	bitIndex uint64
	value    []byte
}

func SectionBloomSegmentPath(fromBlock, toBlock uint64) string {
	return fmt.Sprintf("log/section-bloom-%d-%d.seg", fromBlock, toBlock)
}

func BuildSectionBloomSegmentFromDB(db ethdb.Iteratee, dir, relPath string, fromBlock, toBlock uint64) (SegmentRef, error) {
	if db == nil {
		return SegmentRef{}, errors.New("snapshots: nil database")
	}
	if dir == "" {
		return SegmentRef{}, errors.New("snapshots: section bloom segment directory is empty")
	}
	if toBlock < fromBlock {
		return SegmentRef{}, fmt.Errorf("snapshots: section bloom range [%d,%d] is inverted", fromBlock, toBlock)
	}
	if relPath == "" {
		relPath = SectionBloomSegmentPath(fromBlock, toBlock)
	}
	ref := SegmentRef{
		Dataset:   SegmentDatasetSectionBloom,
		Kind:      SegmentSectionBloom,
		FromTxNum: fromBlock,
		ToTxNum:   toBlock,
		Path:      filepath.ToSlash(relPath),
	}
	if err := validateSegmentRef(ref); err != nil {
		return SegmentRef{}, err
	}
	rows, err := collectSectionBloomRows(db, fromBlock, toBlock)
	if err != nil {
		return SegmentRef{}, err
	}
	return writeSectionBloomSegmentRows(dir, ref, rows)
}

func CheckSectionBloomSegment(dir string, ref SegmentRef) error {
	if err := validateSectionBloomRef(ref); err != nil {
		return err
	}
	if err := checkSegmentFileMetadata(dir, ref, false); err != nil {
		return err
	}
	file, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	header, err := readSectionBloomHeader(file)
	if err != nil {
		return err
	}
	fileSize := uint64(stat.Size())
	if err := validateSectionBloomHeader(ref, header, fileSize); err != nil {
		return err
	}
	return checkSectionBloomIndex(file, ref, header, fileSize)
}

func OpenSectionBloomSegment(dir string, ref SegmentRef) (*SectionBloomSegment, error) {
	if err := validateSectionBloomRef(ref); err != nil {
		return nil, err
	}
	file, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, err
	}
	header, err := readSectionBloomHeader(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := validateSectionBloomHeader(ref, header, uint64(stat.Size())); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &SectionBloomSegment{ref: ref, file: file, header: header}, nil
}

func (s *SectionBloomSegment) Close() error {
	if s == nil || s.file == nil {
		return nil
	}
	return s.file.Close()
}

func (s *SectionBloomSegment) SectionBloom(section, bitIndex uint64) ([]byte, bool, error) {
	if s == nil || s.file == nil || bitIndex >= rawdb.SectionBloomBitSize {
		return nil, false, nil
	}
	if !sectionBloomRefCoversSection(s.ref, section) {
		return nil, false, nil
	}
	lo, hi := uint64(0), s.header.rowCount
	for lo < hi {
		mid := lo + (hi-lo)/2
		entry, err := readSectionBloomIndexEntryAt(s.file, sectionBloomIndexEntryOffset(s.header, mid))
		if err != nil {
			return nil, false, err
		}
		if compareSectionBloomEntry(entry, section, bitIndex) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo >= s.header.rowCount {
		return nil, false, nil
	}
	entry, err := readSectionBloomIndexEntryAt(s.file, sectionBloomIndexEntryOffset(s.header, lo))
	if err != nil {
		return nil, false, err
	}
	if entry.section != section || entry.bitIndex != bitIndex {
		return nil, false, nil
	}
	raw, err := readSectionBloomPayloadAt(s.file, entry.offset, entry.length)
	if err != nil {
		return nil, false, err
	}
	return raw, true, nil
}

func (s *SectionBloomSegment) IterateRows(fn func(section, bitIndex uint64, raw []byte) error) error {
	if s == nil || s.file == nil || fn == nil {
		return nil
	}
	for i := uint64(0); i < s.header.rowCount; i++ {
		entry, err := readSectionBloomIndexEntryAt(s.file, sectionBloomIndexEntryOffset(s.header, i))
		if err != nil {
			return err
		}
		raw, err := readSectionBloomPayloadAt(s.file, entry.offset, entry.length)
		if err != nil {
			return err
		}
		if err := fn(entry.section, entry.bitIndex, raw); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) SectionBloom(section, bitIndex uint64) ([]byte, bool, error) {
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return nil, false, err
	}
	for _, ref := range sectionBloomRefs(manifest) {
		if !sectionBloomRefCoversSection(ref, section) {
			continue
		}
		seg, err := OpenSectionBloomSegment(m.dir, ref)
		if err != nil {
			return nil, false, err
		}
		raw, ok, lookupErr := seg.SectionBloom(section, bitIndex)
		closeErr := seg.Close()
		if lookupErr != nil {
			return nil, false, lookupErr
		}
		if closeErr != nil {
			return nil, false, closeErr
		}
		if ok {
			return raw, true, nil
		}
	}
	return nil, false, nil
}

func sectionBloomRefs(manifest *Manifest) []SegmentRef {
	if manifest == nil {
		return nil
	}
	refs := make([]SegmentRef, 0)
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentSectionBloom || ref.normalizedDataset() != SegmentDatasetSectionBloom {
			continue
		}
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].ToTxNum != refs[j].ToTxNum {
			return refs[i].ToTxNum > refs[j].ToTxNum
		}
		if refs[i].FromTxNum != refs[j].FromTxNum {
			return refs[i].FromTxNum > refs[j].FromTxNum
		}
		return refs[i].Path < refs[j].Path
	})
	return refs
}

func collectSectionBloomRows(db ethdb.Iteratee, fromBlock, toBlock uint64) ([]sectionBloomRow, error) {
	fromSection := fromBlock / rawdb.SectionBloomBlockPerSection
	toSection := toBlock / rawdb.SectionBloomBlockPerSection
	var rows []sectionBloomRow
	if err := rawdb.IterateSectionBloomRows(db, func(section, bitIndex uint64, value []byte) (bool, error) {
		if section < fromSection || section > toSection {
			return true, nil
		}
		rows = append(rows, sectionBloomRow{
			section:  section,
			bitIndex: bitIndex,
			value:    append([]byte(nil), value...),
		})
		return true, nil
	}); err != nil {
		return nil, err
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].section != rows[j].section {
			return rows[i].section < rows[j].section
		}
		return rows[i].bitIndex < rows[j].bitIndex
	})
	for i := 1; i < len(rows); i++ {
		if rows[i-1].section == rows[i].section && rows[i-1].bitIndex == rows[i].bitIndex {
			return nil, fmt.Errorf("snapshots: duplicate section bloom row %d/%d", rows[i].section, rows[i].bitIndex)
		}
	}
	return rows, nil
}

func writeSectionBloomSegmentRows(dir string, ref SegmentRef, rows []sectionBloomRow) (SegmentRef, error) {
	rowCount := uint64(len(rows))
	indexBytes, overflow := checkedMul(rowCount, sectionBloomIndexEntrySize)
	if overflow {
		return SegmentRef{}, fmt.Errorf("snapshots: section bloom index entries %d overflow size", rowCount)
	}
	payloadOffset, overflow := checkedAdd(sectionBloomHeaderSize, indexBytes)
	if overflow {
		return SegmentRef{}, fmt.Errorf("snapshots: section bloom payload offset overflow")
	}
	if payloadOffset > math.MaxInt64 {
		return SegmentRef{}, fmt.Errorf("snapshots: section bloom payload offset %d overflows int64", payloadOffset)
	}
	abs := filepath.Join(dir, ref.Path)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return SegmentRef{}, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".*.tmp")
	if err != nil {
		return SegmentRef{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	header := sectionBloomHeader{
		fromBlock:     ref.FromTxNum,
		toBlock:       ref.ToTxNum,
		rowCount:      rowCount,
		indexOffset:   uint64(sectionBloomHeaderSize),
		payloadOffset: payloadOffset,
	}
	if err := writeSectionBloomHeader(tmp, header); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if _, err := tmp.Seek(int64(payloadOffset), io.SeekStart); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	for i, row := range rows {
		offset, err := tmp.Seek(0, io.SeekCurrent)
		if err != nil {
			_ = tmp.Close()
			return SegmentRef{}, err
		}
		if _, err := tmp.Write(row.value); err != nil {
			_ = tmp.Close()
			return SegmentRef{}, err
		}
		entry := sectionBloomIndexEntry{
			section:  row.section,
			bitIndex: row.bitIndex,
			offset:   uint64(offset),
			length:   uint64(len(row.value)),
		}
		if err := writeSectionBloomIndexEntryAt(tmp, header.indexOffset+uint64(i)*sectionBloomIndexEntrySize, entry); err != nil {
			_ = tmp.Close()
			return SegmentRef{}, err
		}
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := tmp.Close(); err != nil {
		return SegmentRef{}, err
	}
	size, checksum, err := stateDomainChangeBinaryFileMetadata(tmpName)
	if err != nil {
		return SegmentRef{}, err
	}
	ref.Size = size
	ref.Checksum = checksum
	ref.Path = contentAddressedSnapshotPath(ref.Path, ref.Checksum)
	finalAbs := filepath.Join(dir, ref.Path)
	if err := os.MkdirAll(filepath.Dir(finalAbs), 0o755); err != nil {
		return SegmentRef{}, err
	}
	if err := os.Rename(tmpName, finalAbs); err != nil {
		return SegmentRef{}, err
	}
	return ref, nil
}

func validateSectionBloomRef(ref SegmentRef) error {
	if err := validateSegmentRef(ref); err != nil {
		return err
	}
	if ref.Kind != SegmentSectionBloom {
		return fmt.Errorf("snapshots: expected %s segment, got %s", SegmentSectionBloom, ref.Kind)
	}
	if ref.normalizedDataset() != SegmentDatasetSectionBloom {
		return fmt.Errorf("snapshots: section bloom segment %q dataset %q, want %q", ref.Path, ref.Dataset, SegmentDatasetSectionBloom)
	}
	return nil
}

func validateSectionBloomHeader(ref SegmentRef, header sectionBloomHeader, fileSize uint64) error {
	if header.fromBlock != ref.FromTxNum || header.toBlock != ref.ToTxNum {
		return fmt.Errorf("snapshots: section bloom segment %q range [%d,%d], want [%d,%d]",
			ref.Path, header.fromBlock, header.toBlock, ref.FromTxNum, ref.ToTxNum)
	}
	if header.indexOffset != sectionBloomHeaderSize {
		return fmt.Errorf("snapshots: section bloom segment %q index offset %d, want %d", ref.Path, header.indexOffset, sectionBloomHeaderSize)
	}
	indexBytes, overflow := checkedMul(header.rowCount, sectionBloomIndexEntrySize)
	if overflow {
		return fmt.Errorf("snapshots: section bloom segment %q index size overflow", ref.Path)
	}
	wantPayloadOffset, overflow := checkedAdd(header.indexOffset, indexBytes)
	if overflow {
		return fmt.Errorf("snapshots: section bloom segment %q payload offset overflow", ref.Path)
	}
	if header.payloadOffset != wantPayloadOffset {
		return fmt.Errorf("snapshots: section bloom segment %q payload offset %d, want %d", ref.Path, header.payloadOffset, wantPayloadOffset)
	}
	if header.payloadOffset > fileSize {
		return fmt.Errorf("snapshots: section bloom segment %q payload offset %d exceeds size %d", ref.Path, header.payloadOffset, fileSize)
	}
	return nil
}

func checkSectionBloomIndex(file io.ReaderAt, ref SegmentRef, header sectionBloomHeader, fileSize uint64) error {
	var prev *sectionBloomIndexEntry
	expectedOffset := header.payloadOffset
	for i := uint64(0); i < header.rowCount; i++ {
		entry, err := readSectionBloomIndexEntryAt(file, sectionBloomIndexEntryOffset(header, i))
		if err != nil {
			return err
		}
		if !sectionBloomRefCoversSection(ref, entry.section) {
			return fmt.Errorf("snapshots: section bloom segment %q entry %d points to section %d outside source block range [%d,%d]",
				ref.Path, i, entry.section, ref.FromTxNum, ref.ToTxNum)
		}
		if entry.bitIndex >= rawdb.SectionBloomBitSize {
			return fmt.Errorf("snapshots: section bloom segment %q entry %d bit index %d exceeds %d",
				ref.Path, i, entry.bitIndex, rawdb.SectionBloomBitSize)
		}
		if prev != nil && compareSectionBloomEntries(*prev, entry) >= 0 {
			return fmt.Errorf("snapshots: section bloom segment %q index is not sorted", ref.Path)
		}
		if entry.length == 0 {
			return fmt.Errorf("snapshots: section bloom segment %q entry %d has empty payload", ref.Path, i)
		}
		end, overflow := checkedAdd(entry.offset, entry.length)
		if overflow || entry.offset != expectedOffset || end > fileSize {
			return fmt.Errorf("snapshots: section bloom segment %q entry %d payload [%d,%d] outside expected offset %d/size %d",
				ref.Path, i, entry.offset, end, expectedOffset, fileSize)
		}
		raw, err := readSectionBloomPayloadAt(file, entry.offset, entry.length)
		if err != nil {
			return err
		}
		decoded, err := rawdb.DecodeSectionBloomBitSet(raw)
		if err != nil {
			return fmt.Errorf("snapshots: section bloom segment %q entry %d decode %d/%d: %w", ref.Path, i, entry.section, entry.bitIndex, err)
		}
		if len(decoded) > sectionBloomMaxDecodedBytesSize {
			return fmt.Errorf("snapshots: section bloom segment %q entry %d decoded bitset has %d bytes, want <= %d",
				ref.Path, i, len(decoded), sectionBloomMaxDecodedBytesSize)
		}
		cp := entry
		prev = &cp
		expectedOffset = end
	}
	if expectedOffset != fileSize {
		return fmt.Errorf("snapshots: section bloom segment %q size %d, want %d", ref.Path, fileSize, expectedOffset)
	}
	return nil
}

func writeSectionBloomHeader(w io.Writer, header sectionBloomHeader) error {
	var raw [sectionBloomHeaderSize]byte
	copy(raw[0:8], sectionBloomMagic[:])
	binary.BigEndian.PutUint64(raw[8:16], header.fromBlock)
	binary.BigEndian.PutUint64(raw[16:24], header.toBlock)
	binary.BigEndian.PutUint64(raw[24:32], header.rowCount)
	binary.BigEndian.PutUint64(raw[32:40], header.indexOffset)
	binary.BigEndian.PutUint64(raw[40:48], header.payloadOffset)
	_, err := w.Write(raw[:])
	return err
}

func readSectionBloomHeader(r io.Reader) (sectionBloomHeader, error) {
	var raw [sectionBloomHeaderSize]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return sectionBloomHeader{}, io.ErrUnexpectedEOF
		}
		return sectionBloomHeader{}, err
	}
	if headerMagic := [8]byte(raw[0:8]); headerMagic != sectionBloomMagic {
		return sectionBloomHeader{}, errors.New("snapshots: invalid section bloom segment magic")
	}
	return sectionBloomHeader{
		fromBlock:     binary.BigEndian.Uint64(raw[8:16]),
		toBlock:       binary.BigEndian.Uint64(raw[16:24]),
		rowCount:      binary.BigEndian.Uint64(raw[24:32]),
		indexOffset:   binary.BigEndian.Uint64(raw[32:40]),
		payloadOffset: binary.BigEndian.Uint64(raw[40:48]),
	}, nil
}

func writeSectionBloomIndexEntryAt(file *os.File, offset uint64, entry sectionBloomIndexEntry) error {
	var raw [sectionBloomIndexEntrySize]byte
	binary.BigEndian.PutUint64(raw[0:8], entry.section)
	binary.BigEndian.PutUint64(raw[8:16], entry.bitIndex)
	binary.BigEndian.PutUint64(raw[16:24], entry.offset)
	binary.BigEndian.PutUint64(raw[24:32], entry.length)
	_, err := file.WriteAt(raw[:], int64(offset))
	return err
}

func readSectionBloomIndexEntryAt(file io.ReaderAt, offset uint64) (sectionBloomIndexEntry, error) {
	var raw [sectionBloomIndexEntrySize]byte
	if _, err := file.ReadAt(raw[:], int64(offset)); err != nil {
		return sectionBloomIndexEntry{}, err
	}
	return sectionBloomIndexEntry{
		section:  binary.BigEndian.Uint64(raw[0:8]),
		bitIndex: binary.BigEndian.Uint64(raw[8:16]),
		offset:   binary.BigEndian.Uint64(raw[16:24]),
		length:   binary.BigEndian.Uint64(raw[24:32]),
	}, nil
}

func readSectionBloomPayloadAt(file io.ReaderAt, offset, length uint64) ([]byte, error) {
	if length > uint64(^uint(0)>>1) || offset > math.MaxInt64 {
		return nil, fmt.Errorf("snapshots: section bloom payload offset=%d length=%d overflows", offset, length)
	}
	out := make([]byte, int(length))
	if len(out) == 0 {
		return out, nil
	}
	if _, err := file.ReadAt(out, int64(offset)); err != nil {
		return nil, err
	}
	return out, nil
}

func sectionBloomIndexEntryOffset(header sectionBloomHeader, ordinal uint64) uint64 {
	return header.indexOffset + ordinal*sectionBloomIndexEntrySize
}

func compareSectionBloomEntries(a, b sectionBloomIndexEntry) int {
	if a.section < b.section {
		return -1
	}
	if a.section > b.section {
		return 1
	}
	return compareSectionBloomUint64(a.bitIndex, b.bitIndex)
}

func compareSectionBloomEntry(entry sectionBloomIndexEntry, section, bitIndex uint64) int {
	if entry.section < section {
		return -1
	}
	if entry.section > section {
		return 1
	}
	return compareSectionBloomUint64(entry.bitIndex, bitIndex)
}

func compareSectionBloomUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func sectionBloomRefCoversSection(ref SegmentRef, section uint64) bool {
	if ref.ToTxNum < ref.FromTxNum {
		return false
	}
	fromSection := ref.FromTxNum / rawdb.SectionBloomBlockPerSection
	toSection := ref.ToTxNum / rawdb.SectionBloomBlockPerSection
	return section >= fromSection && section <= toSection
}

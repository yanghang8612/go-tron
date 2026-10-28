package snapshots

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"os"
)

const stateDomainChangeBinaryAccessorVersionV9 = uint32(9)
const historyOffsetPageRows = uint64(128)
const historyOffsetEntrySize = uint64(12)
const historyOffsetFooterSize = uint64(32)
const historyOffsetPageMaxBytes = 6 + 127*binary.MaxVarintLen64
const historyOffsetCachePages = 64

var historyOffsetMagic = [8]byte{'G', 'T', 'O', 'F', 'F', 'S', '0', '9'}

func isHistoryInlineAccessor(version uint32) bool {
	return version == stateDomainChangeBinaryAccessorVersionV8 || isHistoryOffsetAccessor(version)
}

func isHistoryOffsetAccessor(version uint32) bool {
	return version == stateDomainChangeBinaryAccessorVersionV9 || version == stateDomainChangeBinaryAccessorVersionV10 || version == stateDomainChangeBinaryAccessorVersionV11
}

func historyOffsetCRC(page uint64, entry, payload []byte) uint32 {
	var index [8]byte
	binary.BigEndian.PutUint64(index[:], page)
	c := crc32.ChecksumIEEE(index[:])
	c = crc32.Update(c, crc32.IEEETable, entry[:8])
	return crc32.Update(c, crc32.IEEETable, payload)
}

func decodeHistoryOffsetFooter(r io.ReaderAt, size, postingEnd uint64, h *stateDomainChangeBinaryAccessorV6Header) error {
	if size < historyOffsetFooterSize || postingEnd > size-historyOffsetFooterSize {
		return errors.New("snapshots: truncated V9 offset locator")
	}
	var raw [historyOffsetFooterSize]byte
	if _, err := r.ReadAt(raw[:], int64(size-historyOffsetFooterSize)); err != nil {
		return err
	}
	if string(raw[:8]) != string(historyOffsetMagic[:]) || binary.BigEndian.Uint64(raw[8:16]) != h.recordCount || binary.BigEndian.Uint32(raw[24:28]) != uint32(historyOffsetPageRows) || binary.BigEndian.Uint32(raw[28:]) != crc32.ChecksumIEEE(raw[:28]) {
		return errors.New("snapshots: invalid V9 offset locator footer")
	}
	h.offsetDataBase = postingEnd
	h.offsetDataLen = binary.BigEndian.Uint64(raw[16:24])
	dirLen := ceilDiv(h.recordCount, historyOffsetPageRows) * historyOffsetEntrySize
	remaining := size - historyOffsetFooterSize - postingEnd
	if h.offsetDataLen > remaining || remaining-h.offsetDataLen != dirLen || h.recordCount == 0 && h.offsetDataLen != 0 {
		return errors.New("snapshots: invalid V9 offset locator size")
	}
	h.offsetDirectory = postingEnd + h.offsetDataLen
	return nil
}

func decodeHistoryOffsetPage(h stateDomainChangeBinaryAccessorV6Header, page uint64, entry, raw []byte) ([]uint64, error) {
	if len(entry) != int(historyOffsetEntrySize) || len(raw) < 6 || len(raw) > historyOffsetPageMaxBytes || historyOffsetCRC(page, entry, raw) != binary.BigEndian.Uint32(entry[8:]) {
		return nil, errors.New("snapshots: V9 offset page checksum/length mismatch")
	}
	if page >= ceilDiv(h.recordCount, historyOffsetPageRows) {
		return nil, errors.New("snapshots: V9 offset page outside record count")
	}
	count := min(historyOffsetPageRows, h.recordCount-page*historyOffsetPageRows)
	first, err := stateDomainChangeBinaryAccessorV5Offset(raw[:6])
	if err != nil {
		return nil, err
	}
	offsets := make([]uint64, count)
	offsets[0] = first
	pos := 6
	for i := uint64(1); i < count; i++ {
		delta, n := binary.Uvarint(raw[pos:])
		if n <= 0 || delta == 0 || delta > stateDomainChangeBinaryAccessorV5MaxOffset-offsets[i-1] {
			return nil, errors.New("snapshots: invalid V9 offset page delta")
		}
		pos += n
		offsets[i] = offsets[i-1] + delta
	}
	if pos != len(raw) {
		return nil, errors.New("snapshots: V9 offset page trailing bytes")
	}
	return offsets, nil
}

func historyOffsetEntryBounds(h stateDomainChangeBinaryAccessorV6Header, raw []byte) (uint64, uint64, error) {
	off, err := stateDomainChangeBinaryAccessorV5Offset(raw[:6])
	n := uint64(binary.BigEndian.Uint16(raw[6:8]))
	if err != nil || off > h.offsetDataLen || n < 6 || n > historyOffsetPageMaxBytes || n > h.offsetDataLen-off {
		return 0, 0, errors.New("snapshots: V9 offset page outside payload")
	}
	return off, n, nil
}

// The cache is private to one iterator/request; immutable decoded pages only.
type historyAccessorOffsetResolver struct {
	r                                              io.ReaderAt
	h                                              stateDomainChangeBinaryAccessorV6Header
	pages                                          map[uint64][]uint64
	order                                          []uint64
	boundsHave                                     bool
	boundsFrom, boundsTo, boundsLower, boundsUpper uint64
}

func newHistoryAccessorOffsetResolver(r io.ReaderAt, h stateDomainChangeBinaryAccessorV6Header) *historyAccessorOffsetResolver {
	return &historyAccessorOffsetResolver{r: r, h: h}
}

func (r *historyAccessorOffsetResolver) Offset(ordinal uint32) (uint64, error) {
	if uint64(ordinal) >= r.h.recordCount {
		return 0, errors.New("snapshots: V9 locator ordinal outside segment")
	}
	page := uint64(ordinal) / historyOffsetPageRows
	values, ok := r.pages[page]
	if !ok {
		var entry [historyOffsetEntrySize]byte
		if _, err := r.r.ReadAt(entry[:], int64(r.h.offsetDirectory+page*historyOffsetEntrySize)); err != nil {
			return 0, err
		}
		off, n, err := historyOffsetEntryBounds(r.h, entry[:])
		if err != nil {
			return 0, err
		}
		raw := make([]byte, n)
		if _, err := r.r.ReadAt(raw, int64(r.h.offsetDataBase+off)); err != nil {
			return 0, err
		}
		values, err = decodeHistoryOffsetPage(r.h, page, entry[:], raw)
		if err != nil {
			return 0, err
		}
		if len(r.order) == historyOffsetCachePages {
			delete(r.pages, r.order[0])
			r.order = r.order[1:]
		}
		if r.pages == nil {
			r.pages = make(map[uint64][]uint64)
		}
		r.pages[page] = values
		r.order = append(r.order, page)
	}
	return values[uint64(ordinal)%historyOffsetPageRows], nil
}

// Full verification walks directory/payload in order, never one random read per row.
func checkHistoryAccessorOffsets(r io.ReaderAt, h stateDomainChangeBinaryAccessorV6Header) error {
	if !isHistoryOffsetAccessor(h.version) {
		return nil
	}
	count := ceilDiv(h.recordCount, historyOffsetPageRows)
	directory := bufio.NewReaderSize(io.NewSectionReader(r, int64(h.offsetDirectory), int64(count*historyOffsetEntrySize)), 64<<10)
	data := bufio.NewReaderSize(io.NewSectionReader(r, int64(h.offsetDataBase), int64(h.offsetDataLen)), 1<<20)
	var next, previous uint64
	var entry [historyOffsetEntrySize]byte
	var payload [historyOffsetPageMaxBytes]byte
	for page := uint64(0); page < count; page++ {
		if _, err := io.ReadFull(directory, entry[:]); err != nil {
			return err
		}
		off, n, err := historyOffsetEntryBounds(h, entry[:])
		if err != nil {
			return err
		}
		if off != next {
			return errors.New("snapshots: V9 offset pages are not contiguous")
		}
		if _, err := io.ReadFull(data, payload[:n]); err != nil {
			return err
		}
		values, err := decodeHistoryOffsetPage(h, page, entry[:], payload[:n])
		if err != nil {
			return err
		}
		if page > 0 && values[0] <= previous {
			return errors.New("snapshots: V9 offset pages are not ordered")
		}
		previous, next = values[len(values)-1], off+n
	}
	if next != h.offsetDataLen {
		return errors.New("snapshots: V9 offset payload coverage mismatch")
	}
	return nil
}

type historyAccessorOffsetBuilder struct {
	data, directory             *os.File
	dataName, directoryName     string
	dataWriter, directoryWriter *bufio.Writer
	page                        [historyOffsetPageRows]uint64
	rows, bytes                 uint64
	used                        int
	last                        uint64
}

func newHistoryAccessorOffsetBuilder(dir, base string) (*historyAccessorOffsetBuilder, error) {
	b := new(historyAccessorOffsetBuilder)
	var err error
	b.data, b.dataName, err = createStateDomainChangeBinaryTempFileInDir(dir, base+".offset-data")
	if err != nil {
		return nil, err
	}
	b.directory, b.directoryName, err = createStateDomainChangeBinaryTempFileInDir(dir, base+".offset-directory")
	if err != nil {
		b.Close()
		return nil, err
	}
	b.dataWriter = bufio.NewWriterSize(b.data, 1<<20)
	b.directoryWriter = bufio.NewWriterSize(b.directory, 64<<10)
	return b, nil
}

func (b *historyAccessorOffsetBuilder) Close() {
	if b == nil {
		return
	}
	for _, f := range []*os.File{b.data, b.directory} {
		if f != nil {
			_ = f.Close()
		}
	}
	for _, n := range []string{b.dataName, b.directoryName} {
		if n != "" {
			_ = os.Remove(n)
		}
	}
}

func (b *historyAccessorOffsetBuilder) Add(ordinal, offset uint64) error {
	if ordinal != b.rows || ordinal > math.MaxUint32 || offset > stateDomainChangeBinaryAccessorV5MaxOffset || b.rows > 0 && offset <= b.last {
		return errors.New("snapshots: V9 locator input is not in ordinal/offset order")
	}
	b.page[b.used] = offset
	b.used++
	b.rows++
	b.last = offset
	if b.used == int(historyOffsetPageRows) {
		return b.flushPage()
	}
	return nil
}

func (b *historyAccessorOffsetBuilder) flushPage() error {
	if b.used == 0 {
		return nil
	}
	var payload [historyOffsetPageMaxBytes]byte
	if err := putStateDomainChangeBinaryAccessorV5Offset(payload[:6], b.page[0]); err != nil {
		return err
	}
	raw := payload[:6]
	for i := 1; i < b.used; i++ {
		raw = binary.AppendUvarint(raw, b.page[i]-b.page[i-1])
	}
	var entry [historyOffsetEntrySize]byte
	if err := putStateDomainChangeBinaryAccessorV5Offset(entry[:6], b.bytes); err != nil {
		return err
	}
	binary.BigEndian.PutUint16(entry[6:8], uint16(len(raw)))
	page := (b.rows - 1) / historyOffsetPageRows
	binary.BigEndian.PutUint32(entry[8:], historyOffsetCRC(page, entry[:], raw))
	if _, err := b.dataWriter.Write(raw); err != nil {
		return err
	}
	if _, err := b.directoryWriter.Write(entry[:]); err != nil {
		return err
	}
	b.bytes += uint64(len(raw))
	b.used = 0
	return nil
}

func (b *historyAccessorOffsetBuilder) AppendTo(w io.Writer, count uint64) error {
	if b.rows != count {
		return errors.New("snapshots: V9 offset builder record count mismatch")
	}
	if err := b.flushPage(); err != nil {
		return err
	}
	if err := b.dataWriter.Flush(); err != nil {
		return err
	}
	if err := b.directoryWriter.Flush(); err != nil {
		return err
	}
	for _, f := range []*os.File{b.data, b.directory} {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if _, err := copyStateDomainChangeHistoryData(w, f); err != nil {
			return err
		}
	}
	var footer [historyOffsetFooterSize]byte
	copy(footer[:8], historyOffsetMagic[:])
	binary.BigEndian.PutUint64(footer[8:16], count)
	binary.BigEndian.PutUint64(footer[16:24], b.bytes)
	binary.BigEndian.PutUint32(footer[24:28], uint32(historyOffsetPageRows))
	binary.BigEndian.PutUint32(footer[28:], crc32.ChecksumIEEE(footer[:28]))
	_, err := w.Write(footer[:])
	return err
}

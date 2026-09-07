package snapshots

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"sort"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

// The reader loads only the sparse directory on open. CRC-protected metadata
// pages and complete zstd anchors are verified on demand. The manifest's
// whole-file SHA remains the cryptographic authentication boundary; CRC is a
// corruption check, not an adversarial authenticity claim.
type cdcReader struct {
	src                                io.ReaderAt
	dec                                *zstd.Decoder
	count, logical, tableOff, tableLen uint64
	sparse                             []uint64
	mu                                 sync.Mutex
	pages                              []cdcMetadataPage // fixed two-page MRU, at most 64KiB of decoded entries
	cache                              []cdcDecodedPage  // fixed two-anchor MRU, at most 256KiB
	compressed                         []byte
}

type cdcMetadataPage struct {
	index   uint64
	entries []cdcEntry
}
type cdcDecodedPage struct {
	anchor uint32
	bytes  []byte
}

var (
	cdcDecoderOnce  sync.Once
	cdcDecoder      *zstd.Decoder
	cdcDecoderError error
)

func boundedCDCDecoder() (*zstd.Decoder, error) {
	cdcDecoderOnce.Do(func() {
		cdcDecoder, cdcDecoderError = zstd.NewReader(nil, zstd.WithDecoderConcurrency(4),
			zstd.WithDecoderMaxMemory(historychunk.MaxSize), zstd.WithDecoderMaxWindow(historychunk.MaxSize),
			zstd.WithDecodeAllCapLimit(true), zstd.WithDecoderLowmem(true))
	})
	return cdcDecoder, cdcDecoderError
}

func openCDCReader(src io.ReaderAt, size uint64, header []byte) (*cdcReader, error) {
	if len(header) != compressedBlockHeaderSize || size < compressedBlockHeaderSize+cdcFooterSize+4 || size > math.MaxInt64 {
		return nil, errors.New("snapshots: invalid CDC file size")
	}
	if string(header[:8]) != compressedBlockMagic || binary.BigEndian.Uint32(header[8:12]) != compressedBlockCDCVersion || binary.BigEndian.Uint32(header[12:16]) != historychunk.MaxSize {
		return nil, errors.New("snapshots: invalid CDC header")
	}
	for _, b := range header[16:] {
		if b != 0 {
			return nil, errors.New("snapshots: nonzero CDC reserved header")
		}
	}
	var footer [cdcFooterSize]byte
	if _, err := src.ReadAt(footer[:], int64(size-cdcFooterSize)); err != nil {
		return nil, err
	}
	if string(footer[:8]) != compressedBlockCDCEndMagic || binary.BigEndian.Uint32(footer[40:44]) != crc32.ChecksumIEEE(footer[:40]) || binary.BigEndian.Uint32(footer[44:48]) != 0 {
		return nil, errors.New("snapshots: invalid CDC footer/checksum")
	}
	r := &cdcReader{src: src, count: binary.BigEndian.Uint64(footer[8:16]), logical: binary.BigEndian.Uint64(footer[16:24]),
		tableOff: binary.BigEndian.Uint64(footer[24:32]), tableLen: binary.BigEndian.Uint64(footer[32:40])}
	if r.count == 0 || r.count >= uint64(cdcAnchor) || r.logical == 0 || r.logical > math.MaxInt64 || r.count > r.logical {
		return nil, errors.New("snapshots: invalid CDC logical size/count")
	}
	pages := (r.count + cdcEntriesPerPage - 1) / cdcEntriesPerPage
	if pages > cdcMaxSparseBytes/8 {
		return nil, errors.New("snapshots: CDC sparse directory cap exceeded")
	}
	sparseBytes := pages*8 + 4
	wantTable := r.count*cdcEntrySize + pages*4
	if r.tableOff < compressedBlockHeaderSize || r.tableLen != wantTable || r.tableOff > size || r.tableLen > size-r.tableOff || sparseBytes+cdcFooterSize != size-r.tableOff-r.tableLen {
		return nil, errors.New("snapshots: invalid CDC directory bounds")
	}
	data := make([]byte, sparseBytes)
	if _, err := src.ReadAt(data, int64(r.tableOff+r.tableLen)); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint32(data[len(data)-4:]) != crc32.ChecksumIEEE(data[:len(data)-4]) {
		return nil, errors.New("snapshots: CDC sparse directory checksum mismatch")
	}
	r.sparse = make([]uint64, pages)
	for i := range r.sparse {
		x := binary.BigEndian.Uint64(data[i*8:])
		if x >= r.logical || i == 0 && x != 0 || i > 0 && x <= r.sparse[i-1] {
			return nil, errors.New("snapshots: invalid CDC sparse logical offsets")
		}
		r.sparse[i] = x
	}
	dec, err := boundedCDCDecoder()
	if err != nil {
		return nil, err
	}
	r.dec = dec
	return r, nil
}

func (r *cdcReader) page(index uint64) ([]cdcEntry, error) {
	for i := range r.pages {
		if r.pages[i].index == index {
			value := r.pages[i]
			copy(r.pages[1:i+1], r.pages[:i])
			r.pages[0] = value
			return value.entries, nil
		}
	}
	if index >= uint64(len(r.sparse)) {
		return nil, errors.New("snapshots: CDC page outside sparse directory")
	}
	start := index * cdcEntriesPerPage
	count := min(uint64(cdcEntriesPerPage), r.count-start)
	length := count*cdcEntrySize + 4
	data := make([]byte, length)
	position := r.tableOff + index*(cdcEntriesPerPage*cdcEntrySize+4)
	if _, err := r.src.ReadAt(data, int64(position)); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint32(data[len(data)-4:]) != crc32.ChecksumIEEE(data[:len(data)-4]) {
		return nil, errors.New("snapshots: CDC metadata page checksum mismatch")
	}
	entries := make([]cdcEntry, count)
	for i := range entries {
		entries[i] = getCDCEntry(data[i*cdcEntrySize:])
	}
	end := r.logical
	if index+1 < uint64(len(r.sparse)) {
		end = r.sparse[index+1]
	}
	for i, e := range entries {
		next := end
		if i+1 < len(entries) {
			next = entries[i+1].logical
		}
		if i == 0 && e.logical != r.sparse[index] || e.logical >= next || next-e.logical > historychunk.MaxSize ||
			e.physical < compressedBlockHeaderSize || e.physical >= r.tableOff || e.stored == 0 || e.stored > cdcMaxEncodedChunk || e.stored > r.tableOff-e.physical ||
			e.anchor != cdcAnchor && uint64(e.anchor) >= start+uint64(i) {
			return nil, errors.New("snapshots: invalid CDC metadata entry")
		}
	}
	value := cdcMetadataPage{index: index, entries: entries}
	if len(r.pages) < 2 {
		r.pages = append(r.pages, cdcMetadataPage{})
	}
	copy(r.pages[1:], r.pages[:len(r.pages)-1])
	r.pages[0] = value
	return entries, nil
}

func (r *cdcReader) entry(index uint64) (cdcEntry, uint64, error) {
	if index >= r.count {
		return cdcEntry{}, 0, errors.New("snapshots: CDC entry outside directory")
	}
	pageIndex := index / cdcEntriesPerPage
	entries, err := r.page(pageIndex)
	if err != nil {
		return cdcEntry{}, 0, err
	}
	within := int(index % cdcEntriesPerPage)
	e := entries[within]
	end := r.logical
	if within+1 < len(entries) {
		end = entries[within+1].logical
	} else if pageIndex+1 < uint64(len(r.sparse)) {
		end = r.sparse[pageIndex+1]
	}
	return e, end - e.logical, nil
}

func (r *cdcReader) find(offset uint64) (uint64, cdcEntry, uint64, error) {
	if offset >= r.logical {
		return 0, cdcEntry{}, 0, io.EOF
	}
	pageIndex := sort.Search(len(r.sparse), func(i int) bool { return r.sparse[i] > offset }) - 1
	if pageIndex < 0 {
		return 0, cdcEntry{}, 0, errors.New("snapshots: CDC sparse search before first page")
	}
	entries, err := r.page(uint64(pageIndex))
	if err != nil {
		return 0, cdcEntry{}, 0, err
	}
	within := sort.Search(len(entries), func(i int) bool { return entries[i].logical > offset }) - 1
	if within < 0 {
		return 0, cdcEntry{}, 0, errors.New("snapshots: CDC entry search before first entry")
	}
	index := uint64(pageIndex)*cdcEntriesPerPage + uint64(within)
	e, length, err := r.entry(index)
	return index, e, length, err
}

func (r *cdcReader) bytes(index uint64, e cdcEntry, rawLength uint64) ([]byte, error) {
	anchor := uint32(index)
	if e.anchor != cdcAnchor {
		anchor = e.anchor
		base, baseLength, err := r.entry(uint64(anchor))
		if err != nil {
			return nil, err
		}
		if base.anchor != cdcAnchor || base.physical != e.physical || base.stored != e.stored || baseLength != rawLength {
			return nil, errors.New("snapshots: CDC reference is not a matching full anchor")
		}
	}
	for i := range r.cache {
		if r.cache[i].anchor == anchor {
			value := r.cache[i]
			copy(r.cache[1:i+1], r.cache[:i])
			r.cache[0] = value
			return value.bytes, nil
		}
	}
	if rawLength == 0 || rawLength > historychunk.MaxSize || e.stored > cdcMaxEncodedChunk {
		return nil, errors.New("snapshots: CDC decoded/encoded chunk cap exceeded")
	}
	if cap(r.compressed) < int(e.stored) {
		r.compressed = make([]byte, e.stored)
	} else {
		r.compressed = r.compressed[:e.stored]
	}
	if _, err := r.src.ReadAt(r.compressed, int64(e.physical)); err != nil {
		return nil, err
	}
	var dst []byte
	if len(r.cache) == 2 && cap(r.cache[1].bytes) >= int(rawLength) {
		dst = r.cache[1].bytes[:0]
		r.cache = r.cache[:1] // recycled storage must not remain cached if decoding fails
	} else {
		dst = make([]byte, 0, historychunk.MaxSize)
	}
	// CapLimit must see the exact declared raw length even when recycled
	// storage has a larger capacity. Unknown-FCS frames remain output-bounded.
	storage := dst
	dst = dst[:0:int(rawLength)]
	decoded, err := r.dec.DecodeAll(r.compressed, dst)
	if err != nil {
		return nil, err
	}
	if uint64(len(decoded)) != rawLength {
		return nil, errors.New("snapshots: CDC anchor decoded length mismatch")
	}
	// DecodeAll receives only rawLength capacity, but keep our original bounded
	// backing capacity after success. Otherwise varied CDC chunk sizes would
	// shrink every reused slot and force allocations on the next larger chunk.
	if &decoded[0] == &storage[:int(rawLength)][0] {
		decoded = storage[:int(rawLength)]
	}
	if len(r.cache) < 2 {
		r.cache = append(r.cache, cdcDecodedPage{})
	}
	copy(r.cache[1:], r.cache[:len(r.cache)-1])
	r.cache[0] = cdcDecodedPage{anchor: anchor, bytes: decoded}
	return decoded, nil
}

func (r *cdcReader) readAtLocked(p []byte, offset uint64) (int, error) {
	n := 0
	for n < len(p) {
		if offset+uint64(n) >= r.logical {
			return n, io.EOF
		}
		index, e, length, err := r.find(offset + uint64(n))
		if err != nil {
			return n, err
		}
		data, err := r.bytes(index, e, length)
		if err != nil {
			return n, err
		}
		within := offset + uint64(n) - e.logical
		n += copy(p[n:], data[within:])
	}
	return n, nil
}

func (r *cdcReader) ReadAt(p []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, errors.New("snapshots: negative CDC read offset")
	}
	if len(p) == 0 {
		return 0, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.readAtLocked(p, uint64(offset))
}

func (r *cdcReader) BlockAt(offset uint64) ([]byte, uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	i, e, length, err := r.find(offset)
	if err != nil {
		return nil, 0, err
	}
	data, err := r.bytes(i, e, length)
	return append([]byte(nil), data...), e.logical, err
}

func (r *cdcReader) ReadRecordFrameAt(offset uint64, scratch []byte) ([]byte, uint64, bool, error) {
	if offset > r.logical || r.logical-offset < 4 {
		return scratch, 0, false, io.ErrUnexpectedEOF
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	i, e, length, err := r.find(offset)
	if err != nil {
		return scratch, 0, false, err
	}
	data, err := r.bytes(i, e, length)
	if err != nil {
		return scratch, 0, false, err
	}
	within := offset - e.logical
	var fixed [4]byte
	if length-within >= 4 {
		copy(fixed[:], data[within:within+4])
	} else if _, err := r.readAtLocked(fixed[:], offset); err != nil {
		return scratch, 0, false, err
	}
	payloadLength := uint64(binary.BigEndian.Uint32(fixed[:]))
	if payloadLength > r.logical-offset-4 {
		return scratch, 0, false, io.ErrUnexpectedEOF
	}
	next := offset + 4 + payloadLength
	if 4+payloadLength <= length-within {
		return data[within+4 : within+4+payloadLength], next, true, nil
	}
	if payloadLength > compressedBlockMaxAlloc() {
		return scratch, 0, false, fmt.Errorf("snapshots: CDC record length %d exceeds allocation limit", payloadLength)
	}
	if cap(scratch) < int(payloadLength) {
		scratch = make([]byte, payloadLength)
	} else {
		scratch = scratch[:payloadLength]
	}
	if _, err := r.readAtLocked(scratch, offset+4); err != nil {
		return scratch, 0, false, err
	}
	return scratch, next, false, nil
}

func decompressCDCBlob(data []byte) ([]byte, error) {
	r, err := openCDCReader(bytes.NewReader(data), uint64(len(data)), data[:compressedBlockHeaderSize])
	if err != nil {
		return nil, err
	}
	if r.logical > compressedBlockMaxAlloc() {
		return nil, errors.New("snapshots: CDC logical blob exceeds allocation limit")
	}
	out := make([]byte, r.logical)
	_, err = r.ReadAt(out, 0)
	return out, err
}

package snapshots

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const compressedBlockRecordColumnsVersion = uint32(5)
const compressedBlockRecordColumnsEndMagic = "gtcend05"
const historyRecordColumnStoredMax = 2*historyRecordColumnPageMax + 1024

var recordColumnDecoderOnce sync.Once
var recordColumnDecoder *zstd.Decoder
var recordColumnDecoderErr error

func recordColumnCodecDecoder() (*zstd.Decoder, error) {
	recordColumnDecoderOnce.Do(func() {
		recordColumnDecoder, recordColumnDecoderErr = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0), zstd.WithDecoderMaxMemory(4<<20), zstd.WithDecoderMaxWindow(4<<20), zstd.WithDecodeAllCapLimit(true), zstd.WithDecoderLowmem(true))
	})
	return recordColumnDecoder, recordColumnDecoderErr
}

func recordColumnFooterVersion(header []byte) bool {
	return len(header) >= 12 && binary.BigEndian.Uint32(header[8:12]) == compressedBlockRecordColumnsVersion
}

func encodeRecordColumnStoredPage(raw []byte, starts []uint32, enc *zstd.Encoder) ([]byte, error) {
	if len(raw) == 0 || len(raw) > historyRecordColumnPageMax {
		return nil, errHistoryRecordColumns
	}
	best := enc.EncodeAll(raw, nil)
	mode := byte(0)
	for keyMode := uint32(0); len(starts) > 0 && keyMode < 3; keyMode++ {
		transformed, e := encodeHistoryRecordColumns(raw, starts, keyMode)
		if e != nil {
			return nil, e
		}
		compressed := enc.EncodeAll(transformed, nil)
		if len(compressed) < len(best) {
			best = compressed
			mode = 1
		}
	}
	return append([]byte{mode}, best...), nil
}

// scratch is distinct from dst and the compressed input. Reader ownership is
// serialized by its existing mutex; retaining scratch avoids a fresh 128 KiB
// allocation on every cold-page miss. Returned canonical bytes never alias it.
func decodeRecordColumnStoredPage(comp []byte, expected uint64, dst, scratch []byte) (out, retained []byte, err error) {
	if expected == 0 || expected > historyRecordColumnPageMax || len(comp) < 2 || len(comp) > historyRecordColumnStoredMax || comp[0] > 1 {
		return nil, scratch, errHistoryRecordColumns
	}
	dec, e := recordColumnCodecDecoder()
	if e != nil {
		return nil, scratch, e
	}
	if uint64(cap(dst)) < expected {
		dst = make([]byte, 0, int(expected))
	} else {
		dst = dst[:0]
	}
	if comp[0] == 0 {
		out, e = dec.DecodeAll(comp[1:], dst)
		if e != nil {
			return nil, scratch, e
		}
		if uint64(len(out)) != expected {
			return nil, scratch, errHistoryRecordColumns
		}
		return out, scratch, nil
	}
	limit := int(expected) + historyRecordColumnHeader
	if cap(scratch) < limit {
		scratch = make([]byte, 0, limit)
	} else {
		scratch = scratch[:0]
	}
	transformed, e := dec.DecodeAll(comp[1:], scratch)
	if e != nil {
		return nil, scratch, e
	}
	out, e = decodeHistoryRecordColumns(transformed, expected, dst)
	return out, transformed, e
}

func validateRecordColumnBlocks(table []cbBlock, logical uint64) error {
	for i, b := range table {
		n, e := compressedBlockExpectedLen(table, logical, i)
		if e != nil {
			return e
		}
		if n == 0 || n > historyRecordColumnPageMax || b.compressedLen < 2 || b.compressedLen > historyRecordColumnStoredMax {
			return fmt.Errorf("%w: physical page bounds", errHistoryRecordColumns)
		}
	}
	return nil
}

func decompressRecordColumnBlob(data []byte, layout compressedBlockFooterLayout) ([]byte, error) {
	if e := validateRecordColumnBlocks(layout.table, layout.uncSize); e != nil {
		return nil, e
	}
	if layout.uncSize > compressedBlockMaxAlloc() {
		return nil, errHistoryRecordColumns
	}
	out := make([]byte, 0, int(layout.uncSize))
	var scratch []byte
	for i, b := range layout.table {
		n, _ := compressedBlockExpectedLen(layout.table, layout.uncSize, i)
		start := compressedBlockHeaderSize + b.compressedStart
		end := start + b.compressedLen
		if end > uint64(len(data)) {
			return nil, errHistoryRecordColumns
		}
		raw, retained, e := decodeRecordColumnStoredPage(data[start:end], n, out[len(out):len(out)], scratch)
		if e != nil {
			return nil, e
		}
		scratch = retained
		out = append(out, raw...)
	}
	return out, nil
}

type recordColumnStreamNotes struct {
	offsets  []uint64
	starts   []uint32
	encoder  recordColumnPageEncoder
	last     uint64
	haveLast bool
}

func newRecordColumnStreamWriter(dir string, prefixSize int) (*compressedBlockStreamWriter, error) {
	if prefixSize <= 0 || prefixSize > historyRecordColumnPageMax {
		return nil, errHistoryRecordColumns
	}
	body, e := newCompressedBlockStreamBodyVersion(dir, true, compressedBlockRecordColumnsVersion)
	if e != nil {
		return nil, e
	}
	body.encoded = acquireStateDomainChangeHistoryEncodedChunk(body.enc, historyRecordColumnPageMax)
	return &compressedBlockStreamWriter{dir: dir, chunkSize: historyRecordColumnPageMax, prefixSize: prefixSize, workers: 1, body: body, footer: true, first: acquireStateDomainChangeHistoryCompressionChunk(historyRecordColumnPageMax), recordHeaders: &recordColumnStreamNotes{}}, nil
}

func (w *compressedBlockStreamWriter) NoteRecordHeader(offset uint64) error {
	if w == nil || w.closed {
		return errHistoryRecordColumns
	}
	if w.recordHeaders == nil {
		return nil
	}
	s := w.recordHeaders
	const ahead = stateDomainChangeHistoryWriteBufferSize + historyRecordColumnPageMax
	if offset < w.logical || offset-w.logical > ahead || offset > ^uint64(0)-21 || s.haveLast && (offset < s.last || offset-s.last < 21) || len(s.offsets) >= ahead/21+1 {
		return fmt.Errorf("%w: record annotation bounds", errHistoryRecordColumns)
	}
	s.offsets = append(s.offsets, offset)
	s.last = offset
	s.haveLast = true
	return nil
}

func (w *compressedBlockStreamWriter) recordHeadersForPage(start uint64, size int) []uint32 {
	s := w.recordHeaders
	starts := s.starts[:0]
	end := start + uint64(size)
	consumed := 0
	for _, offset := range s.offsets {
		if offset >= end {
			break
		}
		consumed++
		if offset >= start && offset+21 <= end {
			starts = append(starts, uint32(offset-start))
		}
	}
	copy(s.offsets, s.offsets[consumed:])
	s.offsets = s.offsets[:len(s.offsets)-consumed]
	s.starts = starts
	return starts
}

// Resolve once, before bufio hides the underlying stream. Default formats pay
// only a predictable nil branch per V6 record and retain their buffered IO.
func (w *compressedBlockStreamWriter) historyRecordHeaderObserver() func(uint64) error {
	if w == nil || w.recordHeaders == nil {
		return nil
	}
	return w.NoteRecordHeader
}
func (w *stateDomainChangeHistoryTemp) historyRecordHeaderObserver() func(uint64) error {
	if w == nil || w.finalized {
		return nil
	}
	if sink, ok := w.compressed.(interface{ historyRecordHeaderObserver() func(uint64) error }); ok {
		return sink.historyRecordHeaderObserver()
	}
	return nil
}

func recordHeaderObserver(dst any) func(uint64) error {
	if sink, ok := dst.(interface{ historyRecordHeaderObserver() func(uint64) error }); ok {
		return sink.historyRecordHeaderObserver()
	}
	return nil
}

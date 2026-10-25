package snapshots

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const compressedBlockAccountDeltaVersion = uint32(4)
const compressedBlockAccountDeltaEndMagic = "gtcend04"

var accountDeltaDecoderOnce sync.Once
var accountDeltaDecoder *zstd.Decoder
var accountDeltaDecoderErr error

func accountDeltaCodecDecoder() (*zstd.Decoder, error) {
	accountDeltaDecoderOnce.Do(func() {
		accountDeltaDecoder, accountDeltaDecoderErr = zstd.NewReader(nil, zstd.WithDecoderConcurrency(4), zstd.WithDecoderMaxMemory(8<<20), zstd.WithDecoderMaxWindow(8<<20), zstd.WithDecodeAllCapLimit(true), zstd.WithDecoderLowmem(true))
	})
	return accountDeltaDecoder, accountDeltaDecoderErr
}

func encodeAccountDeltaStoredPage(raw []byte, notes []accountDeltaValue, depth uint8, enc *zstd.Encoder, original []byte) ([]byte, accountDeltaStats, error) {
	var stats accountDeltaStats
	if len(raw) == 0 || len(raw) > accountDeltaPageMax {
		return nil, stats, errAccountDelta
	}
	if original == nil {
		original = enc.EncodeAll(raw, nil)
	}
	out := append([]byte{0}, original...)
	if len(notes) == 0 {
		return out, stats, nil
	}
	best := len(original)
	for variant := 0; variant < 2; variant++ {
		transformed, s, err := encodeAccountDeltaColumnsOrder(raw, notes, depth, variant == 1, true)
		if err != nil {
			return nil, stats, err
		}
		compressed := enc.EncodeAll(transformed, nil)
		if len(compressed) < best {
			best = len(compressed)
			out = append([]byte{1}, compressed...)
			stats = s
		}
	}

	return out, stats, nil
}

func decodeAccountDeltaStoredPage(comp []byte, expected uint64, dst []byte) ([]byte, error) {
	if expected == 0 || expected > accountDeltaPageMax || len(comp) < 2 || len(comp) > 3*accountDeltaPageMax+2048 {
		return nil, errAccountDelta
	}
	dec, err := accountDeltaCodecDecoder()
	if err != nil {
		return nil, err
	}
	if comp[0] == 0 {
		if uint64(cap(dst)) < expected {
			dst = make([]byte, 0, int(expected))
		} else {
			dst = dst[:0]
		}
		out, err := dec.DecodeAll(comp[1:], dst)
		if err != nil {
			return nil, err
		}
		if uint64(len(out)) != expected {
			return nil, errAccountDelta
		}
		return out, nil
	}
	if comp[0] != 1 {
		return nil, errAccountDelta
	}
	transformed, err := dec.DecodeAll(comp[1:], make([]byte, 0, int(3*expected)+accountDeltaHeaderSize+accountDeltaColumnHeader+1))
	if err != nil {
		return nil, err
	}
	var out []byte
	if len(transformed) >= 4 && (string(transformed[:4]) == "ADP2" || string(transformed[:4]) == "ADP3") {
		out, _, err = decodeAccountDeltaColumns(transformed, expected, dst)
	} else {
		out, _, err = decodeAccountDeltaPage(transformed, expected, dst)
	}
	return out, err
}

type accountDeltaStreamValue struct {
	offset      uint64
	length, key uint32
}
type accountDeltaStreamNotes struct {
	values []accountDeltaStreamValue
	depth  uint8
}

func (w *compressedBlockStreamWriter) NoteAccountValue(key uint32, offset uint64, length uint32) error {
	if w == nil || w.closed {
		return errAccountDelta
	}
	if w.accountDeltas == nil || length == 0 || length > accountDeltaValueMax {
		return nil
	}
	if offset < w.logical || offset+uint64(length) < offset || len(w.accountDeltas.values) >= accountDeltaValuesMax+1 {
		return errAccountDelta
	}
	v := w.accountDeltas.values
	if len(v) > 0 && offset < v[len(v)-1].offset+uint64(v[len(v)-1].length) {
		return errAccountDelta
	}
	w.accountDeltas.values = append(v, accountDeltaStreamValue{offset, length, key})
	return nil
}

func (w *compressedBlockStreamWriter) accountNotesForPage(start uint64, size int) []accountDeltaValue {
	var notes []accountDeltaValue
	values := w.accountDeltas.values
	consumed := 0
	end := start + uint64(size)
	for _, v := range values {
		if v.offset >= end {
			break
		}
		consumed++
		if v.offset >= start && uint64(v.length) <= end-v.offset {
			notes = append(notes, accountDeltaValue{uint32(v.offset - start), v.length, v.key})
		}
	}
	copy(values, values[consumed:])
	w.accountDeltas.values = values[:len(values)-consumed]
	return notes
}

func newAccountDeltaStreamWriter(dir string, chunkSize, prefixSize int, depth uint8) (*compressedBlockStreamWriter, error) {
	if chunkSize <= 0 || chunkSize > accountDeltaPageMax || prefixSize <= 0 || prefixSize > chunkSize || depth == 0 || depth > accountDeltaDepthMax {
		return nil, errAccountDelta
	}
	body, err := newCompressedBlockStreamBodyFormat(dir, true, true)
	if err != nil {
		return nil, err
	}
	return &compressedBlockStreamWriter{dir: dir, chunkSize: chunkSize, prefixSize: prefixSize, workers: 1, body: body, footer: true, first: acquireStateDomainChangeHistoryCompressionChunk(chunkSize), accountDeltas: &accountDeltaStreamNotes{depth: depth}}, nil
}

func noteHistoryAccountValue(dst any, key uint32, offset uint64, length uint64) error {
	if length == 0 || length > accountDeltaValueMax {
		return nil
	}
	if w, ok := dst.(interface {
		NoteAccountValue(uint32, uint64, uint32) error
	}); ok {
		return w.NoteAccountValue(key, offset, uint32(length))
	}
	return nil
}

func (w *stateDomainChangeHistoryTemp) NoteAccountValue(key uint32, offset uint64, length uint32) error {
	if w == nil || w.finalized {
		return errAccountDelta
	}
	return noteHistoryAccountValue(w.compressed, key, offset, uint64(length))
}

func validateAccountDeltaBlocks(table []cbBlock, logical uint64) error {
	for i, b := range table {
		n, e := compressedBlockExpectedLen(table, logical, i)
		if e != nil {
			return e
		}
		if n == 0 || n > accountDeltaPageMax || b.compressedLen < 2 || b.compressedLen > 3*accountDeltaPageMax+2048 {
			return fmt.Errorf("%w: physical page bounds", errAccountDelta)
		}
	}
	return nil
}

func decompressAccountDeltaBlob(data []byte, layout compressedBlockFooterLayout) ([]byte, error) {
	if err := validateAccountDeltaBlocks(layout.table, layout.uncSize); err != nil {
		return nil, err
	}
	if layout.uncSize > compressedBlockMaxAlloc() {
		return nil, errAccountDelta
	}
	out := make([]byte, 0, int(layout.uncSize))
	for i, b := range layout.table {
		n, _ := compressedBlockExpectedLen(layout.table, layout.uncSize, i)
		start := compressedBlockHeaderSize + b.compressedStart
		end := start + b.compressedLen
		if end > uint64(len(data)) {
			return nil, errAccountDelta
		}
		raw, e := decodeAccountDeltaStoredPage(data[start:end], n, nil)
		if e != nil {
			return nil, e
		}
		out = append(out, raw...)
	}
	return out, nil
}

// Equality is over the complete canonical page, not over parsed account fields.
func checkAccountDeltaPage(raw, stored []byte) error {
	out, e := decodeAccountDeltaStoredPage(stored, uint64(len(raw)), nil)
	if e != nil {
		return e
	}
	if !bytes.Equal(raw, out) {
		return errAccountDelta
	}
	return nil
}

func accountDeltaFooterVersion(header []byte) bool {
	return len(header) >= 12 && binary.BigEndian.Uint32(header[8:12]) == compressedBlockAccountDeltaVersion
}

// Preserve the existing decoded-byte allowance when an experimental page is
// larger than 128 KiB. One complete independent page is always permitted;
// increasing page size must not silently multiply every open reader's cache.
func accountDeltaDecodedCacheLimit(header []byte, layout compressedBlockFooterLayout, requested int) int {
	if !accountDeltaFooterVersion(header) {
		return requested
	}
	var largest uint64
	for i := range layout.table {
		n, e := compressedBlockExpectedLen(layout.table, layout.uncSize, i)
		if e != nil {
			return 1
		}
		largest = max(largest, n)
	}
	if largest == 0 {
		return requested
	}
	budget := uint64(requested) * historyCompressChunkSize
	return min(requested, max(1, int(budget/largest)))
}

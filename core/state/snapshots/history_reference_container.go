package snapshots

// The reference container is an opt-in building block, not a history version or
// routing change. It exposes an unchanged virtual byte stream. All chunk data
// and both fixed-width directories are embedded in one file; there are no
// references to another segment or to the hot database.
import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/tronprotocol/go-tron/internal/historychunk"
)

const (
	historyReferenceMagic           = "GTHREF01"
	historyReferenceHeaderSize      = 128
	historyReferenceChunkEntrySize  = 64
	historyReferenceSpanEntrySize   = 32
	historyReferenceMaxChunk        = historychunk.MaxSize
	historyReferenceMaxMetadata     = 32 << 20
	historyReferenceMaxChunks       = 1 << 18
	historyReferenceMaxSpans        = 1 << 20
	historyReferenceMaxLogical      = 1 << 40
	historyReferenceMaxPhysical     = 64 << 30
	historyReferenceMaxPrefix       = 8 << 20
	historyReferenceMetadataPage    = 4096
	historyReferenceMetadataPages   = 16
	historyReferenceMaxCache        = 64 << 20
	historyReferenceMaxCacheEntries = 512
)

var (
	errHistoryReferenceCorrupt = errors.New("snapshots: corrupt history reference container")
	errHistoryReferenceBudget  = errors.New("snapshots: history reference container budget exceeded")
	errHistoryReferenceClosed  = errors.New("snapshots: history reference container closed")
)

type historyReferenceHeader struct {
	logical, physical, chunkDir, chunks, spanDir, spans uint64
	metadata                                            [sha256.Size]byte
}

func (h historyReferenceHeader) encode() [historyReferenceHeaderSize]byte {
	var b [historyReferenceHeaderSize]byte
	copy(b[:8], historyReferenceMagic)
	binary.BigEndian.PutUint32(b[8:12], 1)
	for i, n := range []uint64{h.logical, h.physical, h.chunkDir, h.chunks, h.spanDir, h.spans} {
		binary.BigEndian.PutUint64(b[16+i*8:], n)
	}
	copy(b[64:96], h.metadata[:])
	return b
}

func decodeHistoryReferenceHeader(b []byte, physical uint64) (historyReferenceHeader, error) {
	var h historyReferenceHeader
	if len(b) != historyReferenceHeaderSize || string(b[:8]) != historyReferenceMagic || binary.BigEndian.Uint32(b[8:12]) != 1 ||
		binary.BigEndian.Uint32(b[12:16]) != 0 || !historyReferenceZero(b[96:]) {
		return h, fmt.Errorf("%w: header", errHistoryReferenceCorrupt)
	}
	values := []*uint64{&h.logical, &h.physical, &h.chunkDir, &h.chunks, &h.spanDir, &h.spans}
	for i, p := range values {
		*p = binary.BigEndian.Uint64(b[16+i*8:])
	}
	copy(h.metadata[:], b[64:96])
	if h.physical != physical || physical > historyReferenceMaxPhysical || h.logical > historyReferenceMaxLogical ||
		h.chunks > historyReferenceMaxChunks || h.spans > historyReferenceMaxSpans ||
		h.chunks*historyReferenceChunkEntrySize+h.spans*historyReferenceSpanEntrySize > historyReferenceMaxMetadata ||
		h.chunkDir < historyReferenceHeaderSize || h.chunkDir > physical ||
		h.chunkDir+h.chunks*historyReferenceChunkEntrySize != h.spanDir ||
		h.spanDir > physical || h.spanDir+h.spans*historyReferenceSpanEntrySize != physical ||
		(h.logical == 0) != (h.spans == 0) {
		return historyReferenceHeader{}, fmt.Errorf("%w: sizes", errHistoryReferenceCorrupt)
	}
	return h, nil
}

type historyReferenceChunk struct {
	offset             uint64
	stored, raw, codec uint32
	digest             [sha256.Size]byte
}

func (c historyReferenceChunk) encode() [historyReferenceChunkEntrySize]byte {
	var b [historyReferenceChunkEntrySize]byte
	binary.BigEndian.PutUint64(b[:8], c.offset)
	binary.BigEndian.PutUint32(b[8:12], c.stored)
	binary.BigEndian.PutUint32(b[12:16], c.raw)
	binary.BigEndian.PutUint32(b[16:20], c.codec)
	copy(b[24:56], c.digest[:])
	return b
}

func decodeHistoryReferenceChunk(b []byte) (historyReferenceChunk, error) {
	var c historyReferenceChunk
	if len(b) != historyReferenceChunkEntrySize || !historyReferenceZero(b[20:24]) || !historyReferenceZero(b[56:64]) {
		return c, fmt.Errorf("%w: chunk directory", errHistoryReferenceCorrupt)
	}
	c.offset, c.stored, c.raw, c.codec = binary.BigEndian.Uint64(b[:8]), binary.BigEndian.Uint32(b[8:12]), binary.BigEndian.Uint32(b[12:16]), binary.BigEndian.Uint32(b[16:20])
	copy(c.digest[:], b[24:56])
	return c, nil
}

type historyReferenceSpan struct {
	logical               uint64
	chunk, offset, length uint32
}

func (s historyReferenceSpan) encode() [historyReferenceSpanEntrySize]byte {
	var b [historyReferenceSpanEntrySize]byte
	binary.BigEndian.PutUint64(b[:8], s.logical)
	binary.BigEndian.PutUint32(b[8:12], s.chunk)
	binary.BigEndian.PutUint32(b[12:16], s.offset)
	binary.BigEndian.PutUint32(b[16:20], s.length)
	return b
}

func decodeHistoryReferenceSpan(b []byte) (historyReferenceSpan, error) {
	if len(b) != historyReferenceSpanEntrySize || !historyReferenceZero(b[20:]) {
		return historyReferenceSpan{}, fmt.Errorf("%w: span directory", errHistoryReferenceCorrupt)
	}
	return historyReferenceSpan{binary.BigEndian.Uint64(b[:8]), binary.BigEndian.Uint32(b[8:12]), binary.BigEndian.Uint32(b[12:16]), binary.BigEndian.Uint32(b[16:20])}, nil
}

func historyReferenceZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

func historyReferenceReadAt(ctx context.Context, r io.ReaderAt, b []byte, off uint64) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if off > math.MaxInt64 || uint64(len(b)) > math.MaxInt64-off {
		return fmt.Errorf("%w: offset", errHistoryReferenceCorrupt)
	}
	n, err := r.ReadAt(b, int64(off))
	if err != nil {
		return err
	}
	if n != len(b) {
		return io.ErrUnexpectedEOF
	}
	return contextError(ctx)
}

func historyReferenceWrite(w io.Writer, b []byte) error {
	n, err := w.Write(b)
	if err == nil && n != len(b) {
		return io.ErrShortWrite
	}
	return err
}

func historyReferenceMetadataHash(ctx context.Context, r io.ReaderAt, h historyReferenceHeader) ([sha256.Size]byte, error) {
	h.metadata = [sha256.Size]byte{}
	header := h.encode()
	digest := sha256.New()
	_, _ = digest.Write(header[:])
	var scratch [64 << 10]byte
	for off := h.chunkDir; off < h.physical; {
		n := min(uint64(len(scratch)), h.physical-off)
		if err := historyReferenceReadAt(ctx, r, scratch[:n], off); err != nil {
			return [sha256.Size]byte{}, err
		}
		_, _ = digest.Write(scratch[:n])
		off += n
	}
	var out [sha256.Size]byte
	copy(out[:], digest.Sum(nil))
	return out, nil
}

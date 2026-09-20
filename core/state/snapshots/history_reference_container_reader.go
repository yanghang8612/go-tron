package snapshots

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

type historyReferencePage struct {
	offset uint64
	length int
	valid  bool
	data   [historyReferenceMetadataPage]byte
}

// historyReferenceReader serializes ReadAt, validation and Close. Data returned
// to callers never aliases a cache. First access performs one bounded streamed
// directory authentication/layout check (at most 32 MiB), then each lookup uses
// binary search and only the necessary independent chunks. There is no delta
// chain. Cache bytes are payload bytes, not total RSS or a hard I/O timeout.
type historyReferenceReader struct {
	mu                     sync.Mutex
	ctx                    context.Context
	file                   io.ReaderAt
	closer                 io.Closer
	header                 historyReferenceHeader
	closed, validated      bool
	pages                  [historyReferenceMetadataPages]historyReferencePage
	pageNext               int
	cache                  map[uint32][]byte
	cacheOrder             []uint32
	cacheBytes, cacheLimit int
	zstd                   *zstd.Decoder
}

func openHistoryReferenceReader(ctx context.Context, path string, cacheBytes int) (*historyReferenceReader, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err == nil && (!info.Mode().IsRegular() || info.Size() < historyReferenceHeaderSize) {
		err = fmt.Errorf("%w: file", errHistoryReferenceCorrupt)
	}
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	r, err := newHistoryReferenceReader(ctx, file, uint64(info.Size()), file, cacheBytes)
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return r, nil
}

func newHistoryReferenceReader(ctx context.Context, file io.ReaderAt, size uint64, closer io.Closer, cacheBytes int) (*historyReferenceReader, error) {
	if file == nil || size < historyReferenceHeaderSize || cacheBytes < 0 || cacheBytes > historyReferenceMaxCache {
		return nil, fmt.Errorf("%w: reader bounds", errHistoryReferenceBudget)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var b [historyReferenceHeaderSize]byte
	if err := historyReferenceReadAt(ctx, file, b[:], 0); err != nil {
		return nil, err
	}
	header, err := decodeHistoryReferenceHeader(b[:], size)
	if err != nil {
		return nil, err
	}
	return &historyReferenceReader{ctx: ctx, file: file, closer: closer, header: header, cacheLimit: cacheBytes, cache: make(map[uint32][]byte)}, nil
}

func (r *historyReferenceReader) UncompressedSize() uint64 { return r.header.logical }

func (r *historyReferenceReader) check(ctx context.Context) error {
	if r.closed {
		return errHistoryReferenceClosed
	}
	if err := contextError(r.ctx); err != nil {
		return err
	}
	return contextError(ctx)
}

func (r *historyReferenceReader) metadata(ctx context.Context, dst []byte, off uint64) error {
	if err := r.check(ctx); err != nil {
		return err
	}
	if off < r.header.chunkDir || off > r.header.physical || uint64(len(dst)) > r.header.physical-off {
		return fmt.Errorf("%w: metadata offset", errHistoryReferenceCorrupt)
	}
	for len(dst) > 0 {
		if err := r.check(ctx); err != nil {
			return err
		}
		pageOff := r.header.chunkDir + (off-r.header.chunkDir)/historyReferenceMetadataPage*historyReferenceMetadataPage
		var page *historyReferencePage
		for i := range r.pages {
			if r.pages[i].valid && r.pages[i].offset == pageOff {
				page = &r.pages[i]
				break
			}
		}
		if page == nil {
			page = &r.pages[r.pageNext]
			r.pageNext = (r.pageNext + 1) % len(r.pages)
			page.valid = false
			page.length = int(min(uint64(historyReferenceMetadataPage), r.header.physical-pageOff))
			if err := historyReferenceReadAt(ctx, r.file, page.data[:page.length], pageOff); err != nil {
				return err
			}
			page.offset, page.valid = pageOff, true
		}
		delta := int(off - pageOff)
		n := min(len(dst), page.length-delta)
		copy(dst[:n], page.data[delta:delta+n])
		dst = dst[n:]
		off += uint64(n)
	}
	return r.check(ctx)
}

func (r *historyReferenceReader) chunk(ctx context.Context, id uint32) (historyReferenceChunk, error) {
	if uint64(id) >= r.header.chunks {
		return historyReferenceChunk{}, fmt.Errorf("%w: chunk ID", errHistoryReferenceCorrupt)
	}
	var b [historyReferenceChunkEntrySize]byte
	if err := r.metadata(ctx, b[:], r.header.chunkDir+uint64(id)*historyReferenceChunkEntrySize); err != nil {
		return historyReferenceChunk{}, err
	}
	return decodeHistoryReferenceChunk(b[:])
}

func (r *historyReferenceReader) span(ctx context.Context, index uint64) (historyReferenceSpan, error) {
	if index >= r.header.spans {
		return historyReferenceSpan{}, fmt.Errorf("%w: span ID", errHistoryReferenceCorrupt)
	}
	var b [historyReferenceSpanEntrySize]byte
	if err := r.metadata(ctx, b[:], r.header.spanDir+index*historyReferenceSpanEntrySize); err != nil {
		return historyReferenceSpan{}, err
	}
	return decodeHistoryReferenceSpan(b[:])
}

// ValidateLayout authenticates the header/directories and checks every physical
// chunk and logical span range. Payload checksums are checked by ReadAt or the
// explicit ValidateAll import/build path, including otherwise unused chunks.
func (r *historyReferenceReader) ValidateLayout(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.validateLayout(ctx)
}

func (r *historyReferenceReader) validateLayout(ctx context.Context) error {
	if err := r.check(ctx); err != nil {
		return err
	}
	if r.validated {
		return nil
	}
	// Read through the same page cache used below, hashing the canonical header
	// with a zero digest. The backing file must remain immutable for reader life.
	h := r.header
	h.metadata = [sha256.Size]byte{}
	encoded := h.encode()
	digest := sha256.New()
	_, _ = digest.Write(encoded[:])
	var scratch [historyReferenceMetadataPage]byte
	for off := h.chunkDir; off < h.physical; {
		n := min(uint64(len(scratch)), h.physical-off)
		if err := r.metadata(ctx, scratch[:n], off); err != nil {
			return err
		}
		_, _ = digest.Write(scratch[:n])
		off += n
	}
	var got [sha256.Size]byte
	copy(got[:], digest.Sum(nil))
	if got != r.header.metadata {
		return fmt.Errorf("%w: metadata digest", errHistoryReferenceCorrupt)
	}
	physical := uint64(historyReferenceHeaderSize)
	for id := uint32(0); uint64(id) < h.chunks; id++ {
		chunk, err := r.chunk(ctx, id)
		if err != nil {
			return err
		}
		if chunk.offset != physical || chunk.raw == 0 || chunk.raw > historyReferenceMaxChunk || chunk.stored == 0 ||
			(chunk.codec != historyReferenceCodecRaw && chunk.codec != historyReferenceCodecSnappy && chunk.codec != historyReferenceCodecZstd) ||
			chunk.codec == historyReferenceCodecRaw && chunk.stored != chunk.raw ||
			(chunk.codec == historyReferenceCodecSnappy || chunk.codec == historyReferenceCodecZstd) && chunk.stored >= chunk.raw ||
			uint64(chunk.stored) > h.chunkDir-physical {
			return fmt.Errorf("%w: chunk layout", errHistoryReferenceCorrupt)
		}
		physical += uint64(chunk.stored)
	}
	if physical != h.chunkDir {
		return fmt.Errorf("%w: payload coverage", errHistoryReferenceCorrupt)
	}
	logical := uint64(0)
	for index := uint64(0); index < h.spans; index++ {
		span, err := r.span(ctx, index)
		if err != nil {
			return err
		}
		chunk, err := r.chunk(ctx, span.chunk)
		if err != nil {
			return err
		}
		if span.logical != logical || span.length == 0 || span.offset > chunk.raw || span.length > chunk.raw-span.offset ||
			uint64(span.length) > h.logical-logical {
			return fmt.Errorf("%w: span layout", errHistoryReferenceCorrupt)
		}
		logical += uint64(span.length)
	}
	if logical != h.logical {
		return fmt.Errorf("%w: logical coverage", errHistoryReferenceCorrupt)
	}
	if err := r.check(ctx); err != nil {
		return err
	}
	r.validated = true
	return nil
}

func (r *historyReferenceReader) loadChunk(ctx context.Context, id uint32) ([]byte, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	if raw, ok := r.cache[id]; ok {
		return raw, nil
	}
	chunk, err := r.chunk(ctx, id)
	if err != nil {
		return nil, err
	}
	stored := make([]byte, chunk.stored)
	if err = historyReferenceReadAt(ctx, r.file, stored, chunk.offset); err != nil {
		return nil, err
	}
	raw := stored
	if chunk.codec == historyReferenceCodecSnappy {
		n, err := snappy.DecodedLen(stored)
		if err != nil || n != int(chunk.raw) {
			return nil, fmt.Errorf("%w: Snappy decoded length", errHistoryReferenceCorrupt)
		}
		raw, err = snappy.Decode(make([]byte, int(chunk.raw)), stored)
		if err != nil {
			return nil, fmt.Errorf("%w: Snappy data: %v", errHistoryReferenceCorrupt, err)
		}
	} else if chunk.codec == historyReferenceCodecZstd {
		if r.zstd == nil {
			r.zstd, err = newHistoryReferenceZstdDecoder()
			if err != nil {
				return nil, err
			}
		}
		// A capacity equal to the authenticated directory declaration makes
		// DecodeAllCapLimit reject frames whose real output exceeds it.
		raw, err = r.zstd.DecodeAll(stored, make([]byte, 0, int(chunk.raw)))
		if err != nil {
			return nil, fmt.Errorf("%w: Zstd data: %v", errHistoryReferenceCorrupt, err)
		}
	}
	if len(raw) != int(chunk.raw) || sha256.Sum256(raw) != chunk.digest {
		return nil, fmt.Errorf("%w: chunk digest", errHistoryReferenceCorrupt)
	}
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	if len(raw) <= r.cacheLimit {
		for r.cacheBytes+len(raw) > r.cacheLimit || len(r.cacheOrder) >= historyReferenceMaxCacheEntries {
			old := r.cacheOrder[0]
			r.cacheOrder = r.cacheOrder[1:]
			r.cacheBytes -= len(r.cache[old])
			delete(r.cache, old)
		}
		r.cache[id] = raw
		r.cacheOrder = append(r.cacheOrder, id)
		r.cacheBytes += len(raw)
	}
	return raw, nil
}

// ValidateAll additionally authenticates every unique payload exactly once,
// even chunks not referenced by the virtual stream. It never expands the full
// logical stream and retains at most the configured cache plus one chunk.
func (r *historyReferenceReader) ValidateAll(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.validateLayout(ctx); err != nil {
		return err
	}
	for id := uint32(0); uint64(id) < r.header.chunks; id++ {
		if _, err := r.loadChunk(ctx, id); err != nil {
			return err
		}
	}
	return r.check(ctx)
}

func (r *historyReferenceReader) ReadAt(p []byte, off int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(r.ctx); err != nil {
		return 0, err
	}
	if off < 0 {
		return 0, fmt.Errorf("%w: negative ReadAt", errHistoryReferenceCorrupt)
	}
	if len(p) == 0 {
		return 0, nil
	}
	if err := r.validateLayout(r.ctx); err != nil {
		return 0, err
	}
	if uint64(off) >= r.header.logical {
		return 0, io.EOF
	}
	lo, hi := uint64(0), r.header.spans
	for lo < hi {
		mid := lo + (hi-lo)/2
		span, err := r.span(r.ctx, mid)
		if err != nil {
			return 0, err
		}
		if span.logical+uint64(span.length) <= uint64(off) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	n := 0
	for n < len(p) && uint64(off) < r.header.logical {
		span, err := r.span(r.ctx, lo)
		if err != nil {
			return n, err
		}
		raw, err := r.loadChunk(r.ctx, span.chunk)
		if err != nil {
			return n, err
		}
		delta := uint32(uint64(off) - span.logical)
		count := min(len(p)-n, int(span.length-delta))
		start := span.offset + delta
		copy(p[n:n+count], raw[start:start+uint32(count)])
		off += int64(count)
		n += count
		lo++
	}
	if err := r.check(r.ctx); err != nil {
		return n, err
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *historyReferenceReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	r.cache, r.cacheOrder = nil, nil
	r.cacheBytes = 0
	r.pages = [historyReferenceMetadataPages]historyReferencePage{}
	if r.zstd != nil {
		r.zstd.Close()
		r.zstd = nil
	}
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}

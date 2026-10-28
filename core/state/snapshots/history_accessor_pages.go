package snapshots

import (
	"context"
	"io"
	"os"
	"sync"
)

// Immutable history files repeatedly read small physical header, directory and
// frame fields. Coalesce only small reads on this open descriptor; large
// payloads still use one direct read. Validation remains in existing callers.
// Pages never survive Close or cross a descriptor/request boundary.
type historyAccessorPages struct {
	mu     sync.Mutex
	file   *os.File
	pages  *historyDirectoryPages
	prefix int
	// Parsed metadata belongs to this immutable open descriptor only. In
	// particular, it must never bypass authentication on a subsequent open.
	framedHeader *stateDomainChangeBinaryAccessorV6Header
	framedSize   uint64
}

func newHistoryAccessorPages(ctx context.Context, file *os.File, size uint64) *historyAccessorPages {
	return newHistoryPhysicalPages(ctx, file, size, historyHeaderPrefixBytes)
}

func newHistoryPhysicalPages(ctx context.Context, file *os.File, size uint64, prefix int) *historyAccessorPages {
	return &historyAccessorPages{file: file, pages: acquireHistoryDirectoryPages(ctx, file, 0, size), prefix: prefix}
}

func (r *historyAccessorPages) Name() string { return r.file.Name() }

func (r *historyAccessorPages) ReadAt(dst []byte, off int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pages == nil {
		return 0, os.ErrClosed
	}
	if err := contextError(r.pages.ctx); err != nil {
		return 0, err
	}
	if len(dst) == 0 || off < 0 || uint64(off) >= uint64(r.pages.end) || len(dst) > historyDirectoryPageSize {
		return r.file.ReadAt(dst, off)
	}
	// Match ReaderAt EOF semantics without fetching past the sized descriptor.
	if int64(len(dst)) > r.pages.end-off {
		count := int(r.pages.end - off)
		n, err := r.readWindow(dst[:count], off)
		if err == nil {
			err = io.EOF
		}
		return n, err
	}
	return r.readWindow(dst, off)
}

// Anchor a miss at the requested offset. Accessor directory pages start after
// their header, rather than at OS page boundaries; rounding a 4KiB request down
// would issue two reads and overfetch on every unaligned directory miss.
// Keep the tiny accessor prefix bounded separately from segment frame headers.
func (r *historyAccessorPages) readWindow(dst []byte, off int64) (int, error) {
	p := r.pages
	for i := 0; i < p.count; i++ {
		page := &p.pages[i]
		if page.offset >= 0 && off >= page.offset && off-page.offset <= int64(page.size) && int64(len(dst)) <= int64(page.size)-(off-page.offset) {
			return copy(dst, page.bytes[off-page.offset:]), nil
		}
	}
	index := p.next
	if p.count < historyDirectoryPageCount {
		index = p.count
	}
	page := &p.pages[index]
	page.offset = -1
	start, length := off, int64(historyDirectoryPageSize)
	if off < int64(r.prefix) && int64(len(dst)) <= int64(r.prefix)-off {
		start, length = 0, int64(r.prefix)
	} else if off >= max(int64(r.prefix), p.end-historyDirectoryPageSize) {
		// Several independent authenticated footers are read backwards from
		// EOF. An offset-anchored window cannot reuse the preceding footer;
		// one bounded final page covers all of these adjacent small reads.
		start = max(int64(r.prefix), p.end-historyDirectoryPageSize)
		length = p.end - start
	}
	size := int(min(length, p.end-start))
	n, err := p.source.ReadAt(page.bytes[:size], start)
	if err != nil {
		return 0, err
	}
	if n != size {
		return 0, io.ErrUnexpectedEOF
	}
	page.offset, page.size = start, size
	if p.count < historyDirectoryPageCount {
		p.count++
	} else {
		p.next = (p.next + 1) % historyDirectoryPageCount
	}
	return copy(dst, page.bytes[off-start:]), nil
}

func (r *historyAccessorPages) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pages == nil {
		return os.ErrClosed
	}
	releaseHistoryDirectoryPages(r.pages)
	r.pages = nil
	r.framedHeader = nil
	return r.file.Close()
}

// Accessor open, history attachment and a batched lookup all validate the same
// framed header. V11 validation includes a bounded transaction directory that
// is often larger than the physical page cache; reuse its validated structure
// rather than reading and decoding that directory three times in one open.
func cachedHistoryFramedHeader(r io.ReaderAt, size uint64) (stateDomainChangeBinaryAccessorV6Header, bool, error) {
	for {
		switch v := r.(type) {
		case contextReaderAt:
			if err := contextError(v.ctx); err != nil {
				return stateDomainChangeBinaryAccessorV6Header{}, false, err
			}
			r = v.r
		case *historyAccessorPages:
			v.mu.Lock()
			defer v.mu.Unlock()
			if v.pages == nil {
				return stateDomainChangeBinaryAccessorV6Header{}, false, os.ErrClosed
			}
			if err := contextError(v.pages.ctx); err != nil {
				return stateDomainChangeBinaryAccessorV6Header{}, false, err
			}
			if v.framedHeader != nil && v.framedSize == size {
				return *v.framedHeader, true, nil
			}
			return stateDomainChangeBinaryAccessorV6Header{}, false, nil
		default:
			return stateDomainChangeBinaryAccessorV6Header{}, false, nil
		}
	}
}

func rememberHistoryFramedHeader(r io.ReaderAt, size uint64, h stateDomainChangeBinaryAccessorV6Header) {
	for {
		switch v := r.(type) {
		case contextReaderAt:
			r = v.r
		case *historyAccessorPages:
			v.mu.Lock()
			defer v.mu.Unlock()
			if v.pages != nil {
				v.framedHeader, v.framedSize = &h, size
			}
			return
		default:
			return
		}
	}
}

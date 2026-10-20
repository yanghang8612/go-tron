package snapshots

import (
	"context"
	"io"
	"sync"
)

const historyDirectoryPageSize = 4096
const historyDirectoryPageCount = 8

type historyDirectoryPage struct {
	offset int64
	size   int
	bytes  [historyDirectoryPageSize]byte
}

// One open batch owns this scratch buffer. No page, reader or authenticated
// metadata survives the batch or crosses a request/file identity boundary.
type historyDirectoryPages struct {
	ctx         context.Context
	source      io.ReaderAt
	start, end  int64
	count, next int
	pages       [historyDirectoryPageCount]historyDirectoryPage
}

var historyDirectoryPagesPool = sync.Pool{New: func() any { return new(historyDirectoryPages) }}

func acquireHistoryDirectoryPages(ctx context.Context, source io.ReaderAt, start, length uint64) *historyDirectoryPages {
	p := historyDirectoryPagesPool.Get().(*historyDirectoryPages)
	p.ctx, p.source, p.start, p.end = ctx, source, int64(start), int64(start+length)
	p.count, p.next = 0, 0
	return p
}

func releaseHistoryDirectoryPages(p *historyDirectoryPages) {
	p.ctx, p.source = nil, nil
	p.count, p.next = 0, 0
	historyDirectoryPagesPool.Put(p)
}

func (p *historyDirectoryPages) ReadAt(dst []byte, off int64) (int, error) {
	if err := contextError(p.ctx); err != nil {
		return 0, err
	}
	if off < p.start || off > p.end || int64(len(dst)) > p.end-off {
		return p.source.ReadAt(dst, off)
	}
	total := 0
	for len(dst) > 0 {
		if err := contextError(p.ctx); err != nil {
			return total, err
		}
		pageOff := p.start + (off-p.start)/historyDirectoryPageSize*historyDirectoryPageSize
		var page *historyDirectoryPage
		for i := 0; i < p.count; i++ {
			if p.pages[i].offset == pageOff {
				page = &p.pages[i]
				break
			}
		}
		if page == nil {
			index := p.next
			if p.count < historyDirectoryPageCount {
				index = p.count
			}
			page = &p.pages[index]
			// Invalidate before reading: an error must never publish partial bytes or
			// preserve the identity of an entry whose buffer was overwritten.
			page.offset = -1
			size := int(min(int64(historyDirectoryPageSize), p.end-pageOff))
			n, err := p.source.ReadAt(page.bytes[:size], pageOff)
			if err != nil {
				return total, err
			}
			if n != size {
				return total, io.ErrUnexpectedEOF
			}
			page.offset, page.size = pageOff, size
			if p.count < historyDirectoryPageCount {
				p.count++
			} else {
				p.next = (p.next + 1) % historyDirectoryPageCount
			}
		}
		n := copy(dst, page.bytes[off-pageOff:page.size])
		total += n
		off += int64(n)
		dst = dst[n:]
	}
	return total, nil
}

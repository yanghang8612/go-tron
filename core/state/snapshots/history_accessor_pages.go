package snapshots

import (
	"context"
	"io"
	"os"
	"sync"
)

// A plain immutable accessor repeatedly reads its small header and directory
// fields. Coalesce only small reads on this open descriptor; large payloads
// still use one direct read. Validation remains in the existing callers.
// Pages never survive Close or cross a descriptor/request boundary.
type historyAccessorPages struct {
	mu    sync.Mutex
	file  *os.File
	pages *historyDirectoryPages
}

func newHistoryAccessorPages(ctx context.Context, file *os.File, size uint64) *historyAccessorPages {
	return &historyAccessorPages{file: file, pages: acquireHistoryDirectoryPages(ctx, file, 0, size)}
}

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
		n, err := r.pages.ReadAt(dst[:count], off)
		if err == nil {
			err = io.EOF
		}
		return n, err
	}
	return r.pages.ReadAt(dst, off)
}

func (r *historyAccessorPages) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pages == nil {
		return os.ErrClosed
	}
	releaseHistoryDirectoryPages(r.pages)
	r.pages = nil
	return r.file.Close()
}

package snapshots

import (
	"context"
	"io"
)

// contextReaderAt makes immutable-segment decoders cancellable without
// duplicating their framing and bounds checks. Verification loops perform
// frequent ReaderAt calls, so shutdown is observed at the next bounded read.
type contextReaderAt struct {
	ctx context.Context
	r   io.ReaderAt
}

func (r contextReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if r.ctx != nil {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
	}
	return r.r.ReadAt(p, off)
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if r.ctx != nil {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
	}
	return r.r.Read(p)
}

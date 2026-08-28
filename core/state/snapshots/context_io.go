package snapshots

import (
	"context"
	"errors"
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

func (r contextReaderAt) v6Key(keyID uint32) ([]byte, error) {
	if r.ctx != nil {
		if err := r.ctx.Err(); err != nil {
			return nil, err
		}
	}
	resolver, ok := r.r.(stateDomainChangeBinaryV6KeyResolver)
	if !ok {
		return nil, errors.New("snapshots: V6 key resolver is unavailable")
	}
	return resolver.v6Key(keyID)
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

type contextWriter struct {
	ctx context.Context
	w   io.Writer
}

func (w contextWriter) Write(p []byte) (int, error) {
	if err := contextError(w.ctx); err != nil {
		return 0, err
	}
	return w.w.Write(p)
}

func (r contextReader) Read(p []byte) (int, error) {
	if r.ctx != nil {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
	}
	return r.r.Read(p)
}

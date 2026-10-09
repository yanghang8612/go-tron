package snapshots

import (
	"context"
	"errors"
	"io"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

// contextReaderAt makes immutable-segment decoders cancellable without
// duplicating their framing and bounds checks. Verification loops perform
// frequent ReaderAt calls, so shutdown is observed at the next bounded read.
type contextReaderAt struct {
	ctx context.Context
	r   io.ReaderAt
}

// Limit cooperative reads outside decoder/cache locks. A codec's one block
// decode and the underlying syscall remain indivisible; this is not an I/O
// deadline or a hard wall-clock bound.
const workCheckpointIOChunk = 1 << 20

func (r contextReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if !maintenance.HasWorkCheckpoint(r.ctx) {
		if err := contextError(r.ctx); err != nil {
			return 0, err
		}
		return r.r.ReadAt(p, off)
	}
	total := 0
	for len(p) > 0 {
		chunk := p[:min(len(p), workCheckpointIOChunk)]
		if err := maintenance.WorkCheckpoint(r.ctx, uint64(len(chunk))); err != nil {
			return total, err
		}
		n, err := r.r.ReadAt(chunk, off)
		total += n
		if err != nil {
			return total, err
		}
		if n != len(chunk) {
			return total, io.ErrUnexpectedEOF
		}
		off += int64(n)
		p = p[n:]
	}
	return total, contextError(r.ctx)
}

func (r contextReaderAt) v6Key(keyID uint32) ([]byte, error) {
	if err := maintenance.WorkCheckpoint(r.ctx, 0); err != nil {
		return nil, err
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
	if !maintenance.HasWorkCheckpoint(w.ctx) {
		if err := contextError(w.ctx); err != nil {
			return 0, err
		}
		return w.w.Write(p)
	}
	total := 0
	for len(p) > 0 {
		chunk := p[:min(len(p), workCheckpointIOChunk)]
		if err := maintenance.WorkCheckpoint(w.ctx, uint64(len(chunk))); err != nil {
			return total, err
		}
		n, err := w.w.Write(chunk)
		total += n
		if err != nil {
			return total, err
		}
		if n != len(chunk) {
			return total, io.ErrShortWrite
		}
		p = p[n:]
	}
	return total, contextError(w.ctx)
}

func (r contextReader) Read(p []byte) (int, error) {
	if maintenance.HasWorkCheckpoint(r.ctx) {
		p = p[:min(len(p), workCheckpointIOChunk)]
		if err := maintenance.WorkCheckpoint(r.ctx, uint64(len(p))); err != nil {
			return 0, err
		}
	} else if err := contextError(r.ctx); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

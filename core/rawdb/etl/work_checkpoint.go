package etl

import "io"

const checkpointIOChunk = 1 << 20

func (c *Collector) checkpoint(n uint64) error {
	if c.opts.Checkpoint != nil {
		return c.opts.Checkpoint(n)
	}
	return nil
}

// These wrappers are installed only for cooperative collectors. Buffered run
// I/O may issue large direct operations, so split at the underlying boundary.
// The syscall and one bounded run sort are still indivisible.
type checkpointReader struct {
	r          io.Reader
	checkpoint func(uint64) error
}

func (r checkpointReader) Read(p []byte) (int, error) {
	p = p[:min(len(p), checkpointIOChunk)]
	if err := r.checkpoint(uint64(len(p))); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

type checkpointWriter struct {
	w          io.Writer
	checkpoint func(uint64) error
}

func (w checkpointWriter) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		chunk := p[:min(len(p), checkpointIOChunk)]
		if err := w.checkpoint(uint64(len(chunk))); err != nil {
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
	return total, nil
}

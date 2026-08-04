package snapshots

import (
	"bufio"
	"io"
	"sync"
)

// History builds create several large buffered writers in sequence: the tx
// range table, segment/index pair, and exact/group accessor files. Reusing the
// fixed-size buffers keeps that sequencing from turning into repeated large
// allocations while still bounding live memory by the maximum concurrency.
var stateDomainChangeHistoryWriterPool = sync.Pool{
	New: func() any {
		return bufio.NewWriterSize(io.Discard, stateDomainChangeHistoryWriteBufferSize)
	},
}

func acquireStateDomainChangeHistoryWriter(dst io.Writer) *bufio.Writer {
	writer := stateDomainChangeHistoryWriterPool.Get().(*bufio.Writer)
	writer.Reset(dst)
	return writer
}

// releaseStateDomainChangeHistoryWriter is deliberately idempotent so callers
// can defer it for error paths and still release eagerly after a successful
// Finish. Resetting to nil drops the underlying file reference and discards any
// unflushed bytes left by an aborted build before the buffer enters the pool.
func releaseStateDomainChangeHistoryWriter(writer **bufio.Writer) {
	if writer == nil || *writer == nil {
		return
	}
	buffered := *writer
	*writer = nil
	buffered.Reset(nil)
	stateDomainChangeHistoryWriterPool.Put(buffered)
}

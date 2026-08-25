package snapshots

import (
	"bufio"
	"errors"
	"io"
	"sync"

	"github.com/ethereum/go-ethereum/metrics"
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

type stateDomainChangeHistoryCopyBuffer [stateDomainChangeHistoryWriteBufferSize]byte

var stateDomainChangeHistoryCopyBufferPool = sync.Pool{
	New: func() any { return new(stateDomainChangeHistoryCopyBuffer) },
}

const stateDomainChangeAccessorGroupOffsetsPoolMaxCapacity = 1 << 20

var stateDomainChangeAccessorGroupOffsetsPool = sync.Pool{
	New: func() any { return new([]uint64) },
}

// V4 event-log validation compares compact posting streams in physical order.
// A reusable reader turns singleton-heavy dictionaries from one pread per key
// into bounded sequential reads without retaining segment-sized buffers.
const eventLogV4ValidationReadBufferSize = 256 << 10

var eventLogV4ValidationReaderPool = sync.Pool{
	New: func() any { return bufio.NewReaderSize(nil, eventLogV4ValidationReadBufferSize) },
}

func acquireEventLogV4ValidationReader(source io.Reader) *bufio.Reader {
	reader := eventLogV4ValidationReaderPool.Get().(*bufio.Reader)
	reader.Reset(source)
	return reader
}

func releaseEventLogV4ValidationReader(reader **bufio.Reader) {
	if reader == nil || *reader == nil {
		return
	}
	buffered := *reader
	*reader = nil
	buffered.Reset(nil)
	eventLogV4ValidationReaderPool.Put(buffered)
}

// Accessor integrity checks walk the exact table and group payload in logical
// order, but the format exposes both through ReaderAt. Raw accessor files would
// otherwise turn every small fixed-width entry into its own pread(2). Keep two
// independent windows so the group directory and group payload can advance in
// parallel without evicting each other on every entry.
const stateDomainChangeAccessorValidationWindowSize = 256 << 10

type stateDomainChangeAccessorValidationWindow struct {
	start int64
	end   int64
	valid bool
	data  [stateDomainChangeAccessorValidationWindowSize]byte
}

type stateDomainChangeAccessorValidationReader struct {
	source  io.ReaderAt
	size    int64
	next    int
	windows [2]stateDomainChangeAccessorValidationWindow
}

var stateDomainChangeAccessorValidationReaderPool = sync.Pool{
	New: func() any { return new(stateDomainChangeAccessorValidationReader) },
}

var (
	stateDomainChangeAccessorValidationSourceReadCounter = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"accessor_validation/source_reads", nil)
	stateDomainChangeAccessorValidationSourceByteCounter = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"accessor_validation/source_bytes", nil)
)

func acquireStateDomainChangeAccessorValidationReader(source io.ReaderAt, size uint64) *stateDomainChangeAccessorValidationReader {
	reader := stateDomainChangeAccessorValidationReaderPool.Get().(*stateDomainChangeAccessorValidationReader)
	reader.source = source
	reader.size = int64(size)
	reader.next = 0
	for i := range reader.windows {
		reader.windows[i].valid = false
	}
	return reader
}

func releaseStateDomainChangeAccessorValidationReader(reader **stateDomainChangeAccessorValidationReader) {
	if reader == nil || *reader == nil {
		return
	}
	buffered := *reader
	*reader = nil
	buffered.source = nil
	buffered.size = 0
	for i := range buffered.windows {
		buffered.windows[i].valid = false
	}
	stateDomainChangeAccessorValidationReaderPool.Put(buffered)
}

func (r *stateDomainChangeAccessorValidationReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("snapshots: negative accessor validation read offset")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r == nil || r.source == nil || off >= r.size {
		return 0, io.EOF
	}

	written := 0
	for written < len(p) && off < r.size {
		window := r.windowContaining(off)
		if window == nil {
			var err error
			window, err = r.fillWindow(off)
			if err != nil {
				return written, err
			}
		}
		start := off - window.start
		available := window.end - off
		need := int64(len(p) - written)
		if available > need {
			available = need
		}
		copy(p[written:written+int(available)], window.data[start:start+available])
		written += int(available)
		off += available
	}
	if written != len(p) {
		return written, io.EOF
	}
	return written, nil
}

func (r *stateDomainChangeAccessorValidationReader) windowContaining(off int64) *stateDomainChangeAccessorValidationWindow {
	for i := range r.windows {
		window := &r.windows[i]
		if window.valid && off >= window.start && off < window.end {
			return window
		}
	}
	return nil
}

func (r *stateDomainChangeAccessorValidationReader) fillWindow(off int64) (*stateDomainChangeAccessorValidationWindow, error) {
	window := &r.windows[r.next]
	r.next = (r.next + 1) % len(r.windows)
	start := off / stateDomainChangeAccessorValidationWindowSize * stateDomainChangeAccessorValidationWindowSize
	length := r.size - start
	if length > stateDomainChangeAccessorValidationWindowSize {
		length = stateDomainChangeAccessorValidationWindowSize
	}
	stateDomainChangeAccessorValidationSourceReadCounter.Inc(1)
	n, err := r.source.ReadAt(window.data[:length], start)
	if n > 0 {
		stateDomainChangeAccessorValidationSourceByteCounter.Inc(int64(n))
	}
	if int64(n) != length {
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	window.start = start
	window.end = start + length
	window.valid = true
	return window, nil
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

func copyStateDomainChangeHistoryData(dst io.Writer, src io.Reader) (int64, error) {
	buffer := stateDomainChangeHistoryCopyBufferPool.Get().(*stateDomainChangeHistoryCopyBuffer)
	defer stateDomainChangeHistoryCopyBufferPool.Put(buffer)

	// Keep the loop explicit instead of using io.CopyBuffer: optional
	// WriterTo/ReaderFrom fast paths bypass its caller-provided buffer and the
	// narrow interface wrappers needed to hide them escape once per copy. This
	// matches Erigon's pooled temporary-file assembly without per-copy objects.
	var written int64
	for {
		read, readErr := src.Read(buffer[:])
		if read > 0 {
			wrote, writeErr := dst.Write(buffer[:read])
			if wrote < 0 || wrote > read {
				wrote = 0
				if writeErr == nil {
					writeErr = io.ErrShortWrite
				}
			}
			written += int64(wrote)
			if writeErr != nil {
				return written, writeErr
			}
			if wrote != read {
				return written, io.ErrShortWrite
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return written, nil
			}
			return written, readErr
		}
	}
}

func acquireStateDomainChangeAccessorGroupOffsets() *[]uint64 {
	offsets := stateDomainChangeAccessorGroupOffsetsPool.Get().(*[]uint64)
	*offsets = (*offsets)[:0]
	return offsets
}

func releaseStateDomainChangeAccessorGroupOffsets(offsets **[]uint64) {
	if offsets == nil || *offsets == nil {
		return
	}
	buffer := *offsets
	*offsets = nil
	*buffer = (*buffer)[:0]
	if cap(*buffer) <= stateDomainChangeAccessorGroupOffsetsPoolMaxCapacity {
		stateDomainChangeAccessorGroupOffsetsPool.Put(buffer)
	}
}

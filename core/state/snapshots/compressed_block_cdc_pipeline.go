package snapshots

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tronprotocol/go-tron/internal/historychunk"
)

const (
	cdcMaxCompressionWorkers = 8
	cdcBulkInputBytes        = 8 << 20
	// Independent of the 64 MiB dedup dictionary and one producer chunk/prefix.
	// Includes retained encoded buffers, even after their jobs have drained.
	cdcMaxInflightBytes = 8 << 20
	cdcMaxPendingCharge = historychunk.MaxSize + cdcMaxEncodedChunk
)

type cdcPipelineStats struct {
	Workers, PeakPending                                    int
	PeakRawBytes, PeakEncodedBufferBytes, PeakReservedBytes uint64
}

type cdcEncodeFunc func(context.Context, []byte, []byte) ([]byte, error)

type cdcCompressionJob struct {
	raw, encoded []byte
	err          error
	done         chan struct{}
}

// Worker tasks only encode immutable owned bytes. The single writer owns the
// queue, dictionary, budget and final output. Completion order never determines
// physical offsets or which earlier occurrence becomes a dedup anchor.
type cdcCompressionPipeline struct {
	ctx                              context.Context
	cancel                           context.CancelFunc
	jobs                             chan *cdcCompressionJob
	wg                               sync.WaitGroup
	stopOnce                         sync.Once
	encode                           cdcEncodeFunc
	limit, pending, allocatedBuffers int
	pendingRaw                       uint64
	free                             [][]byte // producer-owned; no buffer returns until ordered drain
	stats                            cdcPipelineStats
}

func newCDCCompressionPipeline(ctx context.Context, workers int, encode cdcEncodeFunc) *cdcCompressionPipeline {
	ctx, cancel := context.WithCancel(ctx)
	limit := min(2*workers, cdcMaxInflightBytes/cdcMaxPendingCharge)
	p := &cdcCompressionPipeline{ctx: ctx, cancel: cancel, jobs: make(chan *cdcCompressionJob, limit), encode: encode, limit: limit}
	p.stats.Workers = workers
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.worker()
	}
	return p
}

func (p *cdcCompressionPipeline) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case job := <-p.jobs:
			if err := p.ctx.Err(); err != nil {
				job.err = err
			} else {
				job.encoded, job.err = p.encode(p.ctx, job.raw, job.encoded)
			}
			job.raw = nil
			close(job.done)
		}
	}
}

func (p *cdcCompressionPipeline) reserve(rawLength int) error {
	if rawLength <= 0 || rawLength > historychunk.MaxSize || p.pending >= p.limit {
		return errors.New("snapshots: CDC pipeline admission exceeds bounded slots")
	}
	// Check before dictionary ownership copies or allocating an encoded buffer.
	if uint64(p.pending+1)*cdcMaxPendingCharge > cdcMaxInflightBytes {
		return errors.New("snapshots: CDC pipeline admission exceeds byte limit")
	}
	p.pending++
	p.pendingRaw += uint64(rawLength)
	p.recordPeak()
	return nil
}

func (p *cdcCompressionPipeline) recordPeak() {
	p.stats.PeakPending = max(p.stats.PeakPending, p.pending)
	p.stats.PeakRawBytes = max(p.stats.PeakRawBytes, p.pendingRaw)
	encoded := uint64(p.allocatedBuffers) * cdcMaxEncodedChunk
	p.stats.PeakEncodedBufferBytes = max(p.stats.PeakEncodedBufferBytes, encoded)
	p.stats.PeakReservedBytes = max(p.stats.PeakReservedBytes, p.pendingRaw+encoded)
}

func (p *cdcCompressionPipeline) submit(raw []byte) (*cdcCompressionJob, error) {
	var dst []byte
	if n := len(p.free); n > 0 {
		dst = p.free[n-1]
		p.free[n-1] = nil
		p.free = p.free[:n-1]
	} else {
		if p.allocatedBuffers >= p.limit {
			return nil, errors.New("snapshots: CDC encoded buffer pool exceeds slots")
		}
		dst = make([]byte, 0, cdcMaxEncodedChunk)
		p.allocatedBuffers++
		p.recordPeak()
	}
	job := &cdcCompressionJob{raw: raw, encoded: dst[:0], done: make(chan struct{})}
	select {
	case p.jobs <- job:
		return job, nil
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	}
}

func (p *cdcCompressionPipeline) result(ctx context.Context, job *cdcCompressionJob) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	case <-job.done:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := p.ctx.Err(); err != nil {
			return nil, err
		}
		if job.err != nil {
			return nil, job.err
		}
		if len(job.encoded) == 0 || len(job.encoded) > cdcMaxEncodedChunk || cap(job.encoded) > cdcMaxEncodedChunk {
			return nil, fmt.Errorf("snapshots: CDC worker output exceeds reserved frame buffer: len=%d cap=%d", len(job.encoded), cap(job.encoded))
		}
		return job.encoded, nil
	}
}

func (p *cdcCompressionPipeline) release(rawLength int, job *cdcCompressionJob) {
	p.pending--
	p.pendingRaw -= uint64(rawLength)
	if job != nil {
		p.free = append(p.free, job.encoded[:0])
		job.encoded = nil
	}
}

// Stop does not wait for the producer to receive results. Buffered per-job
// completion prevents canceled/failed output from trapping a worker on send.
// A running EncodeAll can finish at most its already bounded 128 KiB frame.
func (p *cdcCompressionPipeline) stop() {
	if p == nil {
		return
	}
	p.stopOnce.Do(func() { p.cancel(); p.wg.Wait() })
	p.free = nil
}

type cdcPendingChunk struct {
	entry     cdcEntry
	anchor    *cdcDictionaryEntry // stable even when its LRU entry is evicted
	job       *cdcCompressionJob  // nil for a reference
	rawLength int
}

func (w *cdcStreamWriter) startPipeline() error {
	if w.enc.MaxEncodedSize(historychunk.MaxSize) > cdcMaxEncodedChunk {
		return errors.New("snapshots: CDC encoder maximum exceeds frame reservation")
	}
	encode := w.encodeChunk
	if encode == nil {
		enc := w.enc
		encode = func(ctx context.Context, raw, dst []byte) ([]byte, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			out := enc.EncodeAll(raw, dst)
			return out, ctx.Err()
		}
	}
	w.pipeline = newCDCCompressionPipeline(w.ctx, w.workers, encode)
	w.pending = make([]cdcPendingChunk, 0, w.pipeline.limit)
	return nil
}

func (w *cdcStreamWriter) drainCDCChunk(ctx context.Context) error {
	if len(w.pending) == 0 {
		return nil
	}
	item := w.pending[0]
	e := item.entry
	if item.job != nil {
		if item.anchor.ordinal != uint32(w.written+1) {
			return errors.New("snapshots: CDC anchor completion order differs from admitted ordinal")
		}
		encoded, err := w.pipeline.result(ctx, item.job)
		if err != nil {
			return err
		}
		if uint64(len(encoded)) > uint64(^uint64(0)>>1)-w.physical {
			return errors.New("snapshots: CDC physical size overflows")
		}
		e.physical, e.stored = w.physical, uint64(len(encoded))
		if _, err := w.bodyWriter.Write(encoded); err != nil {
			return err
		}
		w.physical += e.stored
		item.anchor.entry = e // only the ordered writer publishes a completed anchor
	} else {
		if err := contextError(ctx); err != nil {
			return err
		}
		if item.anchor.ordinal >= uint32(w.written+1) || item.anchor.entry.stored == 0 || item.anchor.entry.anchor != cdcAnchor {
			return errors.New("snapshots: CDC reference anchor has not completed in order")
		}
		e.physical, e.stored = item.anchor.entry.physical, item.anchor.entry.stored
	}
	var entry [cdcEntrySize]byte
	putCDCEntry(entry[:], e)
	if _, err := w.tableWriter.Write(entry[:]); err != nil {
		return err
	}
	w.written++
	w.pipeline.release(item.rawLength, item.job)
	copy(w.pending, w.pending[1:])
	w.pending[len(w.pending)-1] = cdcPendingChunk{}
	w.pending = w.pending[:len(w.pending)-1]
	return nil
}

func (w *cdcStreamWriter) drainCDC(ctx context.Context) error {
	for len(w.pending) > 0 {
		if err := w.drainCDCChunk(ctx); err != nil {
			return err
		}
	}
	if w.written != w.count {
		return errors.New("snapshots: CDC completed directory count differs from admitted chunks")
	}
	return nil
}

func (w *cdcStreamWriter) stopPipeline() {
	if w.pipeline == nil {
		return
	}
	w.pipeline.stop()
	stats := w.pipeline.stats
	dst := &w.stats.Pipeline
	dst.Workers = max(dst.Workers, stats.Workers)
	dst.PeakPending = max(dst.PeakPending, stats.PeakPending)
	dst.PeakRawBytes = max(dst.PeakRawBytes, stats.PeakRawBytes)
	dst.PeakEncodedBufferBytes = max(dst.PeakEncodedBufferBytes, stats.PeakEncodedBufferBytes)
	dst.PeakReservedBytes = max(dst.PeakReservedBytes, stats.PeakReservedBytes)
	w.pipeline = nil
	w.pending = nil
}

func (w *cdcStreamWriter) fail(err error) {
	if w.failure == nil {
		w.failure = err
	}
	w.stopPipeline()
}

package freezer

import (
	"hash/crc32"
	"runtime"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const (
	v2CompressionInputBytes = uint64(64 << 20)
	// Idle encoded destinations are additional to input buffers and in-flight
	// compression results. This bounds only retained reusable output capacity.
	v2CompressionOutputReuseBytes = uint64(32 << 20)
)

func v2CompressionWorkers() int {
	return max(1, min(8, runtime.GOMAXPROCS(0)-1))
}

type v2RawFrame struct {
	first   uint64
	records uint32
	data    []byte
}

type v2EncodedFrame struct {
	first      uint64
	records    uint32
	rawBytes   uint64
	compressed []byte
	checksum   uint32
}

func encodeV2Frame(encoder *zstd.Encoder, raw v2RawFrame) v2EncodedFrame {
	return encodeV2FrameInto(encoder, raw, nil)
}

func encodeV2FrameInto(encoder *zstd.Encoder, raw v2RawFrame, dst []byte) v2EncodedFrame {
	compressed := encoder.EncodeAll(raw.data, dst[:0])
	return v2EncodedFrame{first: raw.first, records: raw.records, rawBytes: uint64(len(raw.data)),
		compressed: compressed, checksum: crc32.Checksum(compressed, v2CRC)}
}

type v2FrameEncodeJob struct {
	raw       v2RawFrame
	dst       []byte
	footprint uint64
	done      chan v2EncodedFrame
}

// v2FrameOutputPool is private to the ordered writer. It caps idle capacity,
// not destinations leased to jobs or transient EncodeAll growth.
type v2FrameOutputPool struct {
	buffers [][]byte
	bytes   uint64
}

func (p *v2FrameOutputPool) take() []byte {
	if len(p.buffers) == 0 {
		return nil
	}
	n := len(p.buffers) - 1
	dst := p.buffers[n]
	p.buffers[n] = nil
	p.buffers = p.buffers[:n]
	p.bytes -= uint64(cap(dst))
	return dst[:0]
}

func (p *v2FrameOutputPool) put(dst []byte) {
	size := uint64(cap(dst))
	if size == 0 || size > v2CompressionOutputReuseBytes-p.bytes {
		return
	}
	p.buffers = append(p.buffers, dst[:0])
	p.bytes += size
}

// Read/transform and disk writes stay in one goroutine and preserve record
// order. Only independent compression/CRC work runs concurrently. Both queued
// frame count and input allocation are bounded; encoded results are released
// in source order. A frame exceeding the queue budget drains all prior work
// and uses the primary encoder alone, keeping exceptional large frames from
// expanding every worker's retained compression workspace. The write callback
// synchronously consumes compressed bytes; they are borrowed only until it returns.
func writeV2FramePipeline(workers int, frames uint64, primary *zstd.Encoder, newEncoder func() (*zstd.Encoder, error), read func([]byte) (v2RawFrame, error), write func(v2EncodedFrame) error) error {
	return writeV2FramePipelineWithCheckpoint(workers, frames, primary, newEncoder, read, write, 0, nil)
}

func writeV2FramePipelineWithCheckpoint(workers int, frames uint64, primary *zstd.Encoder, newEncoder func() (*zstd.Encoder, error), read func([]byte) (v2RawFrame, error), write func(v2EncodedFrame) error, windowFrames uint64, checkpoint func() error) error {
	encoders := []*zstd.Encoder{primary}
	defer func() {
		for _, encoder := range encoders[1:] {
			encoder.Close()
		}
	}()
	for i := 1; i < workers; i++ {
		encoder, err := newEncoder()
		if err != nil {
			return err
		}
		encoders = append(encoders, encoder)
	}
	jobs := make(chan *v2FrameEncodeJob, workers)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			encoder := encoders[index]
			for job := range jobs {
				select {
				case <-stop:
					return
				default:
				}
				encoded := encodeV2FrameInto(encoder, job.raw, job.dst)
				job.done <- encoded
			}
		}(i)
	}
	defer func() { close(stop); close(jobs); wg.Wait() }()
	var pending []*v2FrameEncodeJob
	var inputBytes uint64
	var reusable [][]byte
	var reusableBytes uint64
	// Only the ordered writer accesses this pool. A job exclusively owns its
	// destination until write returns successfully; workers never recycle it.
	var reusableOutput v2FrameOutputPool
	drain := func() error {
		job := pending[0]
		encoded := <-job.done
		pending[0] = nil
		pending = pending[1:]
		inputBytes -= job.footprint
		if err := write(encoded); err != nil {
			return err
		}
		job.dst = nil
		reusableOutput.put(encoded.compressed)
		encoded.compressed = nil
		if job.footprint <= v2CompressionInputBytes-reusableBytes {
			reusable = append(reusable, job.raw.data[:0])
			reusableBytes += job.footprint
		}
		job.raw.data = nil
		return nil
	}
	for i := uint64(0); i < frames; i++ {
		// The first frame was synchronously written before this pipeline. At a
		// preparation-window boundary, drain every prior compression job before
		// permitting the caller to hand off its shared I/O lease. Workers remain
		// idle and own no frame during the callback; the final prepared Read has
		// already joined its source/transform workers.
		if checkpoint != nil && windowFrames > 0 && (i+1)%windowFrames == 0 {
			for len(pending) > 0 {
				if err := drain(); err != nil {
					return err
				}
			}
			if err := checkpoint(); err != nil {
				return err
			}
		}
		if len(pending) >= workers*2 {
			if err := drain(); err != nil {
				return err
			}
		}
		var buffer []byte
		if n := len(reusable); n > 0 {
			buffer = reusable[n-1]
			reusable[n-1] = nil
			reusable = reusable[:n-1]
			reusableBytes -= uint64(cap(buffer))
		} else {
			buffer = make([]byte, 0, 64<<10)
		}
		raw, err := read(buffer)
		if err != nil {
			return err
		}
		footprint := uint64(cap(raw.data))
		for len(pending) > 0 && footprint > v2CompressionInputBytes-inputBytes {
			if err := drain(); err != nil {
				return err
			}
		}
		if footprint > v2CompressionInputBytes {
			// Every worker has completed before primary is used here.
			if err := write(encodeV2Frame(primary, raw)); err != nil {
				return err
			}
			continue
		}
		dst := reusableOutput.take()
		job := &v2FrameEncodeJob{raw: raw, dst: dst, footprint: footprint, done: make(chan v2EncodedFrame, 1)}
		pending = append(pending, job)
		inputBytes += footprint
		jobs <- job
	}
	for len(pending) > 0 {
		if err := drain(); err != nil {
			return err
		}
	}
	return nil
}

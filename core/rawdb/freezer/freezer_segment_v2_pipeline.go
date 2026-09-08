package freezer

import (
	"hash/crc32"
	"runtime"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const v2CompressionInputBytes = uint64(64 << 20)

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
	compressed := encoder.EncodeAll(raw.data, nil)
	return v2EncodedFrame{first: raw.first, records: raw.records, rawBytes: uint64(len(raw.data)),
		compressed: compressed, checksum: crc32.Checksum(compressed, v2CRC)}
}

type v2FrameEncodeJob struct {
	raw       v2RawFrame
	footprint uint64
	done      chan v2EncodedFrame
}

// Read/transform and disk writes stay in one goroutine and preserve record
// order. Only independent compression/CRC work runs concurrently. Both queued
// frame count and input allocation are bounded; encoded results are released
// in source order. A frame exceeding the queue budget drains all prior work
// and uses the primary encoder alone, keeping exceptional large frames from
// expanding every worker's retained compression workspace.
func writeV2FramePipeline(workers int, frames uint64, primary *zstd.Encoder, newEncoder func() (*zstd.Encoder, error), read func([]byte) (v2RawFrame, error), write func(v2EncodedFrame) error) error {
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
				encoded := encodeV2Frame(encoder, job.raw)
				job.done <- encoded
			}
		}(i)
	}
	defer func() { close(stop); close(jobs); wg.Wait() }()
	var pending []*v2FrameEncodeJob
	var inputBytes uint64
	var reusable [][]byte
	var reusableBytes uint64
	drain := func() error {
		job := pending[0]
		encoded := <-job.done
		pending[0] = nil
		pending = pending[1:]
		inputBytes -= job.footprint
		if err := write(encoded); err != nil {
			return err
		}
		if job.footprint <= v2CompressionInputBytes-reusableBytes {
			reusable = append(reusable, job.raw.data[:0])
			reusableBytes += job.footprint
		}
		job.raw.data = nil
		return nil
	}
	for i := uint64(0); i < frames; i++ {
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
		job := &v2FrameEncodeJob{raw: raw, footprint: footprint, done: make(chan v2EncodedFrame, 1)}
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

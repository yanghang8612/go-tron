package snapshots

import (
	"context"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"runtime"

	"github.com/tronprotocol/go-tron/internal/historychunk"
)

// Reuse the bounded immutable-frame encoder, without the CDC dictionary or
// deduplication. At most eight CPU workers own at most sixteen frames; its
// raw/encoded reservations are capped by cdcMaxInflightBytes (8 MiB), including
// retained encoded buffers. The producer owns one additional frame (normally
// 32 KiB, up to 128 KiB before the oversized-row fallback).
// An existing oversized protobuf row is drained/encoded synchronously, so it
// never multiplies that row's existing memory requirement by the worker count.
// This bounds extra pipeline memory, not all event dictionaries or process RSS.
const eventLogPayloadMaxWorkers = 8

type eventLogPayloadPending struct {
	frame     int
	rawLength int
	job       *cdcCompressionJob
}

func eventLogPayloadWorkers() int {
	return min(eventLogPayloadMaxWorkers, max(1, runtime.GOMAXPROCS(0)/2))
}

func newEventLogV3PayloadWriter(file *os.File, workers int) *eventLogV3PayloadWriter {
	return &eventLogV3PayloadWriter{
		file: file, buf: make([]byte, 0, eventLogV3PayloadTarget),
		workers: min(eventLogPayloadMaxWorkers, max(1, workers)),
	}
}

func (w *eventLogV3PayloadWriter) startPipeline() error {
	enc, _, err := cbCodec()
	if err != nil {
		return err
	}
	if enc.MaxEncodedSize(historychunk.MaxSize) > cdcMaxEncodedChunk {
		return errors.New("snapshots: event payload encoder exceeds frame reservation")
	}
	encode := w.encode
	if encode == nil {
		encode = func(ctx context.Context, raw, dst []byte) ([]byte, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return enc.EncodeAll(raw, dst), ctx.Err()
		}
	}
	w.pipeline = newCDCCompressionPipeline(context.Background(), w.workers, encode)
	w.pending = make([]eventLogPayloadPending, 0, w.pipeline.limit)
	return nil
}

func (w *eventLogV3PayloadWriter) flushCompressed(firstRow uint64) error {
	// Tiny segments avoid worker startup. Large single rows retain the old
	// accepted range and encoding; they cannot enter the bounded worker queue.
	if w.workers <= 1 || len(w.frames) < 3 || len(w.buf) == 0 || len(w.buf) > historychunk.MaxSize {
		if err := w.drain(); err != nil {
			return err
		}
		if err := w.flushSerial(firstRow); err != nil {
			return err
		}
		if cap(w.buf) > historychunk.MaxSize {
			w.buf = make([]byte, 0, eventLogV3PayloadTarget)
		}
		return nil
	}
	if w.pipeline == nil {
		if err := w.startPipeline(); err != nil {
			return err
		}
	}
	if len(w.pending) == w.pipeline.limit {
		if err := w.drainOne(); err != nil {
			return err
		}
	}
	if err := w.pipeline.reserve(len(w.buf)); err != nil {
		return err
	}
	// Input readers can reuse their protobuf and scratch buffers immediately
	// after add returns. Workers only see this owned immutable copy.
	job, err := w.pipeline.submit(append([]byte(nil), w.buf...))
	if err != nil {
		return err
	}
	w.pending = append(w.pending, eventLogPayloadPending{frame: len(w.frames), rawLength: len(w.buf), job: job})
	w.frames = append(w.frames, eventLogV3Frame{
		firstRow: firstRow, rowCount: w.rows, checksum: crc32.ChecksumIEEE(w.buf), rawLen: uint32(len(w.buf)),
	})
	w.buf, w.rows = w.buf[:0], 0
	return nil
}

func (w *eventLogV3PayloadWriter) drainOne() error {
	if len(w.pending) == 0 {
		return nil
	}
	item := w.pending[0]
	encoded, err := w.pipeline.result(context.Background(), item.job)
	if err != nil {
		return err
	}
	off, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if _, err := w.file.Write(encoded); err != nil {
		return err
	}
	frame := &w.frames[item.frame]
	frame.dataOff, frame.dataLen = uint64(off), uint32(len(encoded))
	w.pipeline.release(item.rawLength, item.job)
	copy(w.pending, w.pending[1:])
	w.pending[len(w.pending)-1] = eventLogPayloadPending{}
	w.pending = w.pending[:len(w.pending)-1]
	return nil
}

func (w *eventLogV3PayloadWriter) drain() error {
	for len(w.pending) > 0 {
		if err := w.drainOne(); err != nil {
			return err
		}
	}
	return nil
}

func (w *eventLogV3PayloadWriter) close() {
	if w.pipeline != nil {
		w.pipeline.stop()
	}
	w.pending = nil
}

package snapshots

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func recordColumnPipelineFixture() ([]byte, []uint64) {
	rng := rand.New(rand.NewSource(271828))
	raw := bytes.Repeat([]byte{0xa5}, historyHeaderPrefixBytes)
	var starts []uint64
	for i := 0; i < 18000; i++ {
		starts = append(starts, uint64(len(raw)))
		n := rng.Intn(160)
		if i%4000 == 123 {
			n = 3*historyRecordColumnPageMax + 17
		} else if i%1200 == 1199 {
			// The next header straddles a physical page by ten bytes.
			n = (historyRecordColumnPageMax - 10 - (len(raw)+21)%historyRecordColumnPageMax + historyRecordColumnPageMax) % historyRecordColumnPageMax
		}
		raw = binary.BigEndian.AppendUint32(raw, uint32(17+n))
		raw = binary.BigEndian.AppendUint32(raw, rng.Uint32())
		raw = binary.BigEndian.AppendUint64(raw, uint64(i/3+1))
		raw = append(raw, byte(i%2))
		raw = binary.BigEndian.AppendUint32(raw, uint32(n))
		value := make([]byte, n)
		if i%3 != 0 {
			rng.Read(value)
		}
		raw = append(raw, value...)
	}
	return append(raw, bytes.Repeat([]byte{0xe9}, 20)...), starts
}

func writeRecordColumnPipelineFixture(t testing.TB, w *compressedBlockStreamWriter, raw []byte, starts []uint64, partition int) {
	t.Helper()
	buffer := bufio.NewWriterSize(w, stateDomainChangeHistoryWriteBufferSize)
	if _, err := buffer.Write(raw[:starts[0]]); err != nil {
		t.Fatal(err)
	}
	for i, start := range starts {
		if err := w.NoteRecordHeader(start); err != nil {
			t.Fatal(i, err)
		}
		end := uint64(len(raw) - 20)
		if i+1 < len(starts) {
			end = starts[i+1]
		}
		for at := start; at < end; {
			next := min(end, at+uint64(partition))
			if _, err := buffer.Write(raw[at:next]); err != nil {
				t.Fatal(err)
			}
			at = next
		}
	}
	if _, err := buffer.Write(raw[len(raw)-20:]); err != nil {
		t.Fatal(err)
	}
	if err := buffer.Flush(); err != nil {
		t.Fatal(err)
	}
}

func finishRecordColumnPipelineFixture(t testing.TB, w *compressedBlockStreamWriter, raw []byte, path string) []byte {
	t.Helper()
	patch := []byte("prefix-backfill")
	if _, err := w.WriteAt(patch, 3); err != nil {
		t.Fatal(err)
	}
	want := bytes.Clone(raw)
	copy(want[3:], patch)
	pipeline := w.parallel
	if err := w.Finish(path); err != nil {
		t.Fatal(err)
	}
	if pipeline != nil && (len(pipeline.pool) != 0 || len(pipeline.encoded) != 0 || len(pipeline.annotations) != 0) {
		t.Fatal("finished pipeline retained pooled buffers")
	}
	blob, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := decompressBlockBlob(blob)
	if err != nil || !bytes.Equal(canonical, want) {
		t.Fatal("complete canonical oracle", err)
	}
	return blob
}

func TestRecordColumnParallelExactFilesAndReset(t *testing.T) {
	raw, starts := recordColumnPipelineFixture()
	var expected []byte
	for _, workers := range []int{1, 2, 4, 64} {
		dir := t.TempDir()
		stream, err := newHistoryCompressedStreamFormat(context.Background(), dir, historyCompressChunkSize, workers, "5")
		if err != nil {
			t.Fatal(err)
		}
		w := stream.(*compressedBlockStreamWriter)
		defer w.Abort()
		partition := 65537
		if workers == 2 {
			partition = 17
		}
		if workers > 2 {
			partition = 270001
		}
		writeRecordColumnPipelineFixture(t, w, raw, starts, partition)
		if w.workers != historyCompressionConcurrency(workers) || workers > 1 && w.parallel == nil {
			t.Fatal("factory ignored worker budget", workers, w.workers)
		}
		if workers > 1 && cap(w.recordHeaders.encoder.packed) != 0 {
			t.Fatal("serial encoder scratch retained after switching")
		}
		if workers == 4 {
			old := w.parallel
			if err := w.Reset(); err != nil {
				t.Fatal(err)
			}
			old.workers.Wait()
			old.reducer.Wait()
			if w.parallel != nil || w.logical != 0 || len(w.recordHeaders.offsets) != 0 || w.recordHeaders.haveLast {
				t.Fatal("Reset retained asynchronous state")
			}
			writeRecordColumnPipelineFixture(t, w, raw, starts, 8191)
		}
		got := finishRecordColumnPipelineFixture(t, w, raw, filepath.Join(dir, "parallel.seg"))
		if expected == nil {
			expected = got
		} else if !bytes.Equal(got, expected) {
			t.Fatal("workers or Write partition changed physical bytes", workers)
		}
	}
}

func TestRecordColumnOwnedEncoderDoesNotOverwritePendingResults(t *testing.T) {
	enc, _, err := cbCodec()
	if err != nil {
		t.Fatal(err)
	}
	var encoder recordColumnPageEncoder
	var held, copies [][]byte
	for i := int64(0); i < 12; i++ {
		raw, starts := recordColumnEncoderPage(i, historyRecordColumnPageMax)
		if i%4 == 0 {
			starts = nil
		}
		stored, err := encoder.encodeOwned(raw, starts, enc, make([]byte, 0, stateDomainChangeHistoryEncodedChunkCapacity))
		if err != nil {
			t.Fatal(err)
		}
		want, err := encodeRecordColumnStoredPage(raw, starts, enc)
		if err != nil || !bytes.Equal(stored, want) {
			t.Fatal("owned encode differs", err)
		}
		held, copies = append(held, stored), append(copies, bytes.Clone(stored))
		for j := range held {
			if !bytes.Equal(held[j], copies[j]) {
				t.Fatal("next encode overwrote a pending result", i, j)
			}
		}
	}
}

func TestRecordColumnParallelCopiesAnnotations(t *testing.T) {
	w, err := newRecordColumnStreamWriter(t.TempDir(), 128)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	p := newOrderedRecordColumnPipeline(w.body, 2)
	w.parallel = p
	var destinations [][]byte
	for range cap(p.encoded) {
		destinations = append(destinations, <-p.encoded)
	}
	raw, starts := recordColumnEncoderPage(17, historyRecordColumnPageMax)
	canonical, originalStarts := bytes.Clone(raw), append([]uint32(nil), starts...)
	if err := p.SubmitRecords(raw, starts); err != nil {
		t.Fatal(err)
	}
	// Workers are blocked on encoded destinations until after this mutation.
	for i := range starts {
		starts[i] = math.MaxUint32
	}
	for _, dst := range destinations {
		p.encoded <- dst
	}
	if err := p.Close(); err != nil {
		t.Fatal("queued annotations were borrowed", err)
	}
	if err := w.body.tmpWriter.Flush(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(w.body.tmpName)
	if err != nil {
		t.Fatal(err)
	}
	want, err := encodeRecordColumnStoredPage(canonical, originalStarts, w.body.enc)
	if err != nil || len(got) != compressedBlockHeaderSize+len(want) || !bytes.Equal(got[compressedBlockHeaderSize:], want) {
		t.Fatal("copied annotation oracle", err)
	}
}

func TestRecordColumnParallelBufferCapacityBound(t *testing.T) {
	enc, _, err := cbCodec()
	if err != nil {
		t.Fatal(err)
	}
	var encoder recordColumnPageEncoder
	dense, starts := recordColumnEncoderPage(6, historyRecordColumnPageMax)
	if _, err := encoder.encodeOwned(dense, starts, enc, make([]byte, 0, stateDomainChangeHistoryEncodedChunkCapacity)); err != nil {
		t.Fatal(err)
	}
	literal := binary.BigEndian.AppendUint32(nil, math.MaxUint32)
	literal = binary.BigEndian.AppendUint32(literal, math.MaxUint32)
	literal = binary.BigEndian.AppendUint64(literal, math.MaxUint64)
	literal = append(literal, 1)
	literal = binary.BigEndian.AppendUint32(literal, math.MaxUint32-17)
	literal = append(literal, make([]byte, historyRecordColumnPageMax-len(literal))...)
	if _, err := encoder.encodeOwned(literal, []uint32{0}, enc, make([]byte, 0, stateDomainChangeHistoryEncodedChunkCapacity)); err != nil {
		t.Fatal(err)
	}
	retained := cap(encoder.best) + cap(encoder.candidate) + cap(encoder.txs) + cap(encoder.lengths) + cap(encoder.flags) + cap(encoder.literals) + cap(encoder.packed)
	for _, keys := range encoder.keys {
		retained += cap(keys)
	}
	if retained > 5*historyRecordColumnPageMax || encoder.best != nil {
		t.Fatal("worker scratch bound or ownership", retained)
	}
	w, err := newRecordColumnStreamWriterWorkers(t.TempDir(), 128, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	workers, inFlight := w.workers, 2*w.workers
	p := newOrderedRecordColumnPipeline(w.body, workers)
	w.parallel = p
	if workers != maxHistoryCompressionWorkers || cap(p.pool) != inFlight || cap(p.encoded) != inFlight || cap(p.annotations) != inFlight {
		t.Fatal("unbounded queue or ignored worker cap")
	}
	// Charge worst-case reusable capacities, including doubled slice-growth
	// slack, the retained prefix, the body destination and its existing bufio.
	const maxHeaders = historyRecordColumnPageMax / 21
	const maxNotes = (stateDomainChangeHistoryWriteBufferSize+historyRecordColumnPageMax)/21 + 1
	bound := workers*5*historyRecordColumnPageMax + (inFlight+1)*(historyRecordColumnPageMax+stateDomainChangeHistoryEncodedChunkCapacity) +
		inFlight*2*maxHeaders*4 + 2*maxHeaders*4 + 2*maxNotes*8 + w.body.tmpWriter.Size()
	if bound > 6<<20 || enc.MaxEncodedSize(historyRecordColumnPageMax+historyRecordColumnHeader)+1 > stateDomainChangeHistoryEncodedChunkCapacity {
		t.Fatal("compression buffer capacity bound", bound)
	}
	t.Logf("private worker scratch %d B; four-worker compression buffer bound %d B", retained, bound)
}

type recordColumnFailWriter struct{ err error }

func (w recordColumnFailWriter) Write([]byte) (int, error) { return 0, w.err }

func TestRecordColumnParallelFailureDoesNotPublish(t *testing.T) {
	for _, failure := range []string{"invalid-header", "write"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			w, err := newRecordColumnStreamWriter(dir, 128)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			injected := errors.New("injected pipeline write")
			if failure == "write" {
				w.body.tmpWriter.Reset(recordColumnFailWriter{injected})
				if _, err := w.body.tmpWriter.Write(make([]byte, w.body.tmpWriter.Size())); err != nil {
					t.Fatal(err)
				}
			}
			p := newOrderedRecordColumnPipeline(w.body, 2)
			w.parallel = p
			raw, starts := recordColumnEncoderPage(17, historyRecordColumnPageMax)
			if failure == "invalid-header" {
				starts = []uint32{math.MaxUint32}
			}
			if err := p.SubmitRecords(raw, starts); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "target.seg")
			if err := os.WriteFile(path, []byte("old-target"), 0600); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- w.Finish(path) }()
			select {
			case err := <-done:
				if failure == "write" && !errors.Is(err, injected) || failure == "invalid-header" && !errors.Is(err, errHistoryRecordColumns) {
					t.Fatal("pipeline failure lost", failure, err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("failed pipeline did not join")
			}
			old, err := os.ReadFile(path)
			if err != nil || string(old) != "old-target" {
				t.Fatal("failed pipeline replaced target", err)
			}
			p.workers.Wait()
			p.reducer.Wait()
			files, _ := filepath.Glob(filepath.Join(dir, ".cbw-*"))
			if len(files) != 0 {
				t.Fatal("scratch leaked", files)
			}
		})
	}
}

type recordColumnCheckedContext struct {
	context.Context
	checked chan struct{}
	once    sync.Once
}

func (c *recordColumnCheckedContext) Err() error {
	err := c.Context.Err()
	c.once.Do(func() { close(c.checked) })
	return err
}

func TestRecordColumnParallelFullQueueCancelAbortReset(t *testing.T) {
	for _, action := range []string{"cancel", "abort", "reset"} {
		t.Run(action, func(t *testing.T) {
			dir := t.TempDir()
			w, err := newRecordColumnStreamWriterWorkers(dir, 128, 2)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			p := newOrderedRecordColumnPipeline(w.body, 2)
			w.parallel, w.bodyStart = p, true
			var held [][]byte
			for range cap(p.encoded) {
				held = append(held, <-p.encoded)
			}
			defer func() {
				for _, dst := range held {
					releaseStateDomainChangeHistoryEncodedChunk(&dst, historyRecordColumnPageMax)
				}
			}()
			for range cap(p.annotations) {
				if err := p.SubmitRecords(make([]byte, 128), nil); err != nil {
					t.Fatal(err)
				}
			}
			// A final page cannot acquire annotation storage from the full queue.
			w.chunk = make([]byte, 37)
			w.logical = uint64(4*128 + 37)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			checked := &recordColumnCheckedContext{Context: ctx, checked: make(chan struct{})}
			done := make(chan error, 1)
			path := filepath.Join(dir, "target.seg")
			if err := os.WriteFile(path, []byte("preserved"), 0600); err != nil {
				t.Fatal(err)
			}
			go func() {
				switch action {
				case "cancel":
					done <- w.finishContext(checked, path, nil)
				case "abort":
					w.Abort()
					done <- nil
				case "reset":
					done <- w.Reset()
				}
			}()
			if action == "cancel" {
				select {
				case <-checked.checked:
					cancel()
				case <-time.After(3 * time.Second):
					t.Fatal("finish did not start")
				}
			}
			select {
			case err := <-done:
				if action == "cancel" && !errors.Is(err, context.Canceled) || action != "cancel" && err != nil {
					t.Fatal(action, err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("full pipeline did not join", action)
			}
			p.workers.Wait()
			p.reducer.Wait()
			prior, err := os.ReadFile(path)
			if err != nil || string(prior) != "preserved" {
				t.Fatal("target changed", err)
			}
			if action == "reset" {
				payload := bytes.Repeat([]byte("after-reset"), 23)
				if _, err := w.Write(payload); err != nil {
					t.Fatal(err)
				}
				if err := w.Finish(path); err != nil {
					t.Fatal(err)
				}
				blob, _ := os.ReadFile(path)
				got, err := decompressBlockBlob(blob)
				if err != nil || !bytes.Equal(got, payload) {
					t.Fatal("reset retained queued bytes", err)
				}
			}
		})
	}
}

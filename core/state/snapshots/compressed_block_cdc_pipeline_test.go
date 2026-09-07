package snapshots

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/internal/historychunk"
)

func cdcEncodeWorkersForTest(t testing.TB, data []byte, workers int) ([]byte, cdcWriteStats) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "output.seg")
	stream, err := newHistoryCompressedStreamFormat(context.Background(), dir, historyCompressChunkSize, workers, "3")
	if err != nil {
		t.Fatal(err)
	}
	w := stream.(*cdcStreamWriter)
	defer w.Abort()
	for off := 0; off < len(data); {
		// Vary Write partitioning with the worker count. This crosses the bulk
		// threshold, bounded 8MiB windows, and a previously unfinished chunk.
		step := 193031
		if workers > 1 && off > 0 {
			step = 9 << 20
		}
		end := min(len(data), off+step)
		if _, err := w.Write(data[off:end]); err != nil {
			t.Fatal(err)
		}
		off = end
	}
	if err := w.Finish(path); err != nil {
		t.Fatal(err)
	}
	out, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if w.stats.Pipeline.PeakPending > 2*workers || w.stats.Pipeline.PeakReservedBytes > cdcMaxInflightBytes || w.stats.Pipeline.PeakRawBytes > uint64(2*workers*historychunk.MaxSize) {
		t.Fatalf("unbounded %+v", w.stats.Pipeline)
	}
	return out, w.stats
}

func TestCDCPipelineWorkersProduceIdenticalFiles(t *testing.T) {
	random := make([]byte, 4<<20)
	rand.New(rand.NewSource(907)).Read(random)
	for name, data := range map[string][]byte{"inserted-repeated": cdcRepeatedInput(2<<20, 5), "low-repeat": random, "low-entropy": bytes.Repeat([]byte("account-shaped-small-value\x00\x01"), 170000)} {
		t.Run(name, func(t *testing.T) {
			want, _ := cdcEncodeWorkersForTest(t, data, 1)
			for _, workers := range []int{2, 4, 8} {
				got, stats := cdcEncodeWorkersForTest(t, data, workers)
				if !bytes.Equal(got, want) {
					t.Fatalf("workers=%d changes exact serialized output", workers)
				}
				if stats.Pipeline.Workers != workers || stats.Pipeline.PeakPending == 0 {
					t.Fatalf("factory ignored workers=%d: %+v", workers, stats.Pipeline)
				}
				decoded, err := decompressBlockBlob(got)
				if err != nil || !bytes.Equal(decoded, data) {
					t.Fatalf("workers=%d roundtrip: %v", workers, err)
				}
			}
		})
	}
}

func cdcManualChunk(t *testing.T, w *cdcStreamWriter, raw []byte) {
	t.Helper()
	w.chunk = append(w.chunk[:0], raw...)
	w.logical += uint64(len(raw))
	if err := w.flushChunk(); err != nil {
		t.Fatal(err)
	}
}

func TestCDCPipelinePendingAnchorSurvivesLRUEviction(t *testing.T) {
	dir := t.TempDir()
	w, err := newCDCStreamWriterWorkers(context.Background(), dir, 64, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	prefix := bytes.Repeat([]byte{17}, 64)
	if _, err := w.Write(prefix); err != nil {
		t.Fatal(err)
	}
	a := bytes.Repeat([]byte{11}, 32<<10)
	b := bytes.Repeat([]byte{22}, 32<<10)
	secondDone := make(chan struct{})
	var second atomic.Bool
	w.encodeChunk = func(ctx context.Context, raw, dst []byte) ([]byte, error) {
		if raw[0] == 11 {
			select {
			case <-secondDone:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		out := w.enc.EncodeAll(raw, dst)
		if raw[0] == 22 && second.CompareAndSwap(false, true) {
			close(secondDone)
		}
		return out, nil
	}
	if err := w.startPipeline(); err != nil {
		t.Fatal(err)
	}
	cdcManualChunk(t, w, a)
	cdcManualChunk(t, w, a)
	old := w.pending[0].anchor
	if old.entry.stored != 0 || w.pending[1].anchor != old {
		t.Fatal("fixture did not reference pending anchor")
	}
	// Remove exactly the LRU identity while both queued entries retain its stable
	// handle, as real dictionary pressure can do before that anchor drains.
	element := w.dictionary[old.digest]
	delete(w.dictionary, old.digest)
	w.lru.Remove(element)
	w.dictionaryBytes -= len(old.bytes)
	cdcManualChunk(t, w, b)
	cdcManualChunk(t, w, a)
	if w.pending[3].anchor == old {
		t.Fatal("evicted identity unexpectedly reused")
	}
	path := filepath.Join(dir, "out.seg")
	if err := w.Finish(path); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decompressBlockBlob(encoded)
	want := append(bytes.Clone(prefix), a...)
	want = append(want, a...)
	want = append(want, b...)
	want = append(want, a...)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatal("pending anchor/reference bytes differ", err)
	}
	r, err := openCDCReader(bytes.NewReader(encoded), uint64(len(encoded)), encoded[:48])
	if err != nil {
		t.Fatal(err)
	}
	entry, _, err := r.entry(2)
	if err != nil || entry.anchor != 1 || entry.stored == 0 {
		t.Fatalf("reference not committed: %+v %v", entry, err)
	}
}

func cdcAssertNoScratch(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("scratch remains: %v %v", entries, err)
	}
}
func cdcAwaitWrite(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("worker join blocked")
		return nil
	}
}

func TestCDCPipelineCancelAndResetJoinWorkers(t *testing.T) {
	for _, action := range []string{"cancel-write", "cancel-finish", "reset", "abort"} {
		t.Run(action, func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			w, err := newCDCStreamWriterWorkers(ctx, dir, 64, 4)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			started := make(chan struct{}, 8)
			var active atomic.Int32
			w.encodeChunk = func(ctx context.Context, raw, dst []byte) ([]byte, error) {
				active.Add(1)
				defer active.Add(-1)
				started <- struct{}{}
				<-ctx.Done()
				return nil, ctx.Err()
			}
			if err := w.startPipeline(); err != nil {
				t.Fatal(err)
			}
			if action == "cancel-write" {
				data := make([]byte, 3<<20)
				rand.New(rand.NewSource(3)).Read(data)
				done := make(chan error, 1)
				go func() { _, err := w.Write(data); done <- err }()
				<-started
				cancel()
				if err := cdcAwaitWrite(t, done); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				if active.Load() != 0 || w.pipeline != nil {
					t.Fatal("Write returned before workers joined")
				}
				if err := w.Finish(filepath.Join(dir, "must-not-publish.seg")); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else {
				if _, err := w.Write(bytes.Repeat([]byte{17}, 64)); err != nil {
					t.Fatal(err)
				}
				cdcManualChunk(t, w, bytes.Repeat([]byte{3}, 32<<10))
				<-started
				switch action {
				case "cancel-finish":
					finishCtx, finishCancel := context.WithCancel(context.Background())
					done := make(chan error, 1)
					go func() {
						_, err := w.FinishWithMetadataContext(finishCtx, filepath.Join(dir, "must-not-publish.seg"))
						done <- err
					}()
					finishCancel()
					if err := cdcAwaitWrite(t, done); !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
				case "reset":
					if err := w.Reset(); err != nil {
						t.Fatal(err)
					}
					if active.Load() != 0 || w.workers != 4 || w.count != 0 || w.written != 0 || w.pipeline != nil {
						t.Fatal("Reset retained old pipeline state")
					}
					data := []byte("new independent stream")
					if _, err := w.Write(data); err != nil {
						t.Fatal(err)
					}
					path := filepath.Join(dir, "new.seg")
					if err := w.Finish(path); err != nil {
						t.Fatal(err)
					}
					encoded, _ := os.ReadFile(path)
					got, err := decompressBlockBlob(encoded)
					if err != nil || !bytes.Equal(got, data) {
						t.Fatal("stale reset result", err)
					}
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
				case "abort":
					w.Abort()
				}
			}
			if active.Load() != 0 || w.pipeline != nil {
				t.Fatal("operation returned before workers joined")
			}
			w.Abort()
			cdcAssertNoScratch(t, dir)
		})
	}
}

type cdcFailOutput struct{ err error }

func (w cdcFailOutput) Write([]byte) (int, error) { return 0, w.err }

func TestCDCPipelineWorkerAndOutputErrorsNeverPublish(t *testing.T) {
	for _, cause := range []string{"worker", "output"} {
		t.Run(cause, func(t *testing.T) {
			dir := t.TempDir()
			w, err := newCDCStreamWriterWorkers(context.Background(), dir, 64, 4)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			failure := fmt.Errorf("injected %s failure", cause)
			var active atomic.Int32
			w.encodeChunk = func(ctx context.Context, raw, dst []byte) ([]byte, error) {
				active.Add(1)
				defer active.Add(-1)
				if cause == "worker" {
					return nil, failure
				}
				return w.enc.EncodeAll(raw, dst), nil
			}
			if cause == "output" {
				w.metadata.dst = cdcFailOutput{failure}
			}
			if err := w.startPipeline(); err != nil {
				t.Fatal(err)
			}
			data := make([]byte, 4<<20)
			rand.New(rand.NewSource(5)).Read(data)
			_, err = w.Write(data)
			if !errors.Is(err, failure) {
				t.Fatalf("got %v", err)
			}
			if active.Load() != 0 || w.pipeline != nil {
				t.Fatal("Write error did not join workers")
			}
			if err := w.Finish(filepath.Join(dir, "must-not-publish.seg")); !errors.Is(err, failure) {
				t.Fatal(err)
			}
			cdcAssertNoScratch(t, dir)
		})
	}
}

func TestCDCPipelineRejectsUnsupportedWorkersBeforeScratch(t *testing.T) {
	dir := t.TempDir()
	if _, err := newCDCStreamWriterWorkers(context.Background(), dir, 64, cdcMaxCompressionWorkers+1); err == nil {
		t.Fatal("unbounded worker count accepted")
	}
	cdcAssertNoScratch(t, dir)
}

func TestCDCPipelineInvalidWriteAtJoinsAndKeepsAcceptedData(t *testing.T) {
	dir := t.TempDir()
	w, err := newCDCStreamWriterWorkers(context.Background(), dir, 64, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	data := make([]byte, 2<<20)
	rand.New(rand.NewSource(52)).Read(data)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if w.pipeline == nil {
		t.Fatal("fixture has no pipeline")
	}
	if _, err := w.WriteAt([]byte{1}, 64); err == nil {
		t.Fatal("invalid offset accepted")
	}
	if w.pipeline != nil || w.count != w.written {
		t.Fatal("invalid WriteAt left workers alive")
	}
	path := filepath.Join(dir, "out.seg")
	if err := w.Finish(path); err != nil {
		t.Fatal(err)
	}
	encoded, _ := os.ReadFile(path)
	got, err := decompressBlockBlob(encoded)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatal("invalid WriteAt changed accepted data", err)
	}
}

// Err remains the original context's Err. Done signals exactly when an
// asynchronous wait starts using the Finish caller's cancellation channel.
type cdcObservedWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *cdcObservedWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestCDCPipelineFinishCancellationWithFullQueueAndTail(t *testing.T) {
	dir := t.TempDir()
	w, err := newCDCStreamWriterWorkers(context.Background(), dir, 64, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	var active atomic.Int32
	w.encodeChunk = func(ctx context.Context, raw, dst []byte) ([]byte, error) {
		active.Add(1)
		defer active.Add(-1)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err := w.startPipeline(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(bytes.Repeat([]byte{99}, 64)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < w.pipeline.limit; i++ {
		cdcManualChunk(t, w, bytes.Repeat([]byte{byte(i)}, 32<<10))
	}
	w.chunk = append(w.chunk[:0], []byte("unflushed tail")...)
	w.logical += uint64(len(w.chunk))
	if len(w.pending) != w.pipeline.limit {
		t.Fatal("fixture did not fill queue")
	}
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &cdcObservedWaitContext{Context: base, entered: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err := w.FinishWithMetadataContext(ctx, filepath.Join(dir, "must-not-publish.seg"))
		done <- err
	}()
	select {
	case <-ctx.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("tail flush did not wait with Finish caller context")
	}
	cancel()
	if err := cdcAwaitWrite(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if active.Load() != 0 || w.pipeline != nil {
		t.Fatal("canceled tail flush left live workers")
	}
	cdcAssertNoScratch(t, dir)
}

func TestCDCPipelineOwnsInputUntilWorkersFinish(t *testing.T) {
	dir := t.TempDir()
	w, err := newCDCStreamWriterWorkers(context.Background(), dir, 64, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	gate := make(chan struct{})
	w.encodeChunk = func(ctx context.Context, raw, dst []byte) ([]byte, error) {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return w.enc.EncodeAll(raw, dst), nil
	}
	if err := w.startPipeline(); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 64+historychunk.MaxSize)
	rand.New(rand.NewSource(71)).Read(data)
	want := bytes.Clone(data)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if len(w.pending) == 0 {
		t.Fatal("fixture has no pending job")
	}
	clear(data) // the caller may immediately reuse its input after Write returns
	close(gate)
	path := filepath.Join(dir, "out.seg")
	if err := w.Finish(path); err != nil {
		t.Fatal(err)
	}
	encoded, _ := os.ReadFile(path)
	got, err := decompressBlockBlob(encoded)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatal("worker retained borrowed caller bytes", err)
	}
}

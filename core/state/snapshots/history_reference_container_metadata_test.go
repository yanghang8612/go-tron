package snapshots

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

type referenceMetadataCountingFile struct {
	*os.File
	reads, writes     int
	readErr, writeErr error
	shortWrite        bool
}

func (f *referenceMetadataCountingFile) ReadAt(p []byte, off int64) (int, error) {
	f.reads++
	if f.readErr != nil {
		return 0, f.readErr
	}
	return f.File.ReadAt(p, off)
}
func (f *referenceMetadataCountingFile) WriteAt(p []byte, off int64) (int, error) {
	f.writes++
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	if f.shortWrite {
		return len(p) - 1, nil
	}
	return f.File.WriteAt(p, off)
}
func (f *referenceMetadataCountingFile) Flush() error { return nil }

func TestHistoryReferenceContainerMetadataCacheFrozenOutput(t *testing.T) {
	var want []byte
	var wantStats HistoryReferenceContainerStats
	var oldReads, oldWrites int
	for _, cached := range []bool{false, true} {
		dir := t.TempDir()
		w, err := newHistoryReferenceWriter(context.Background(), dir, 128)
		if err != nil {
			t.Fatal(err)
		}
		defer w.Release()
		chunks := &referenceMetadataCountingFile{File: w.chunks}
		spans := &referenceMetadataCountingFile{File: w.spans}
		if cached {
			w.chunkMetadata = newHistoryReferenceMetadataCache(w.ctx, chunks)
			w.spanMetadata = newHistoryReferenceMetadataCache(w.ctx, spans)
		} else {
			// The frozen metadata path is the original exact 32/64-byte os.File I/O;
			// all container encoding/authentication code is otherwise identical.
			w.chunkMetadata = chunks
			w.spanMetadata = spans
		}
		random := rand.New(rand.NewSource(915))
		data := make([]byte, 8192)
		random.Read(data)
		id, err := w.StoreChunk(data)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = w.WriteLiteral(make([]byte, 128)); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 10000; i++ {
			header := bytes.Repeat([]byte{byte(i)}, 21)
			if _, err = w.WriteLiteral(header); err != nil {
				t.Fatal(err)
			}
			if err = w.WriteSpan(id, uint32(i%4000), 128); err != nil {
				t.Fatal(err)
			}
		}
		// Patch an old literal page after both metadata caches have evicted it.
		if _, err = w.WriteAt([]byte("patched-header"), 1); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "complete.r1")
		if _, _, err = w.Finalize(path); err != nil {
			t.Fatal(err)
		}
		output, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !cached {
			want = output
			wantStats = w.Stats()
			oldReads = chunks.reads + spans.reads
			oldWrites = chunks.writes + spans.writes
		} else {
			if !bytes.Equal(want, output) || wantStats != w.Stats() {
				t.Fatal("metadata buffering changed complete container bytes/stats")
			}
			if chunks.reads+spans.reads >= oldReads/10 || chunks.writes+spans.writes >= oldWrites/10 {
				t.Fatal("small metadata I/O was not bounded by pages", oldReads, oldWrites, chunks.reads+spans.reads, chunks.writes+spans.writes)
			}
			t.Logf("metadata I/O direct reads=%d writes=%d; cached reads=%d writes=%d", oldReads, oldWrites, chunks.reads+spans.reads, chunks.writes+spans.writes)
		}
	}
}

func TestHistoryReferenceContainerMetadataCacheEvictionAndFailures(t *testing.T) {
	for _, fault := range []string{"none", "short-write", "write-error", "read-error", "cancel"} {
		t.Run(fault, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "metadata")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			counted := &referenceMetadataCountingFile{File: file}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cache := newHistoryReferenceMetadataCache(ctx, counted)
			original := make([]byte, historyReferenceMetadataPages*historyReferenceMetadataPage)
			rand.New(rand.NewSource(11)).Read(original)
			for off := 0; off < len(original); off += 32 {
				if n, e := cache.WriteAt(original[off:off+32], int64(off)); e != nil || n != 32 {
					t.Fatal(n, e)
				}
			}
			sentinel := errors.New("metadata fault")
			switch fault {
			case "short-write":
				counted.shortWrite = true
			case "write-error":
				counted.writeErr = sentinel
			case "cancel":
				cancel()
			}
			extra := bytes.Repeat([]byte{99}, 32)
			n, err := cache.WriteAt(extra, int64(len(original))) // slot zero must be flushed
			if fault == "short-write" || fault == "write-error" || fault == "cancel" {
				want := sentinel
				if fault == "short-write" {
					want = io.ErrShortWrite
				}
				if fault == "cancel" {
					want = context.Canceled
				}
				if n != 0 || !errors.Is(err, want) {
					t.Fatal("eviction failure lost", n, err)
				}
				if !errors.Is(cache.Flush(), want) {
					t.Fatal("failed cache became usable")
				}
				return
			}
			if err != nil || n != 32 {
				t.Fatal(n, err)
			}
			if fault == "read-error" {
				counted.readErr = sentinel
			}
			var first [32]byte
			n, err = cache.ReadAt(first[:], 0) // evicts dirty tail, reloads durable first page
			if fault == "read-error" {
				if n != 0 || !errors.Is(err, sentinel) || !errors.Is(cache.Flush(), sentinel) {
					t.Fatal("read failure not sticky", n, err)
				}
				return
			}
			if err != nil || n != 32 || !bytes.Equal(first[:], original[:32]) {
				t.Fatal("reload differs", n, err)
			}
			if _, err = cache.WriteAt([]byte("overwrite"), 5); err != nil {
				t.Fatal(err)
			}
			copy(original[5:], []byte("overwrite"))
			if err = cache.Flush(); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(file.Name())
			if err != nil || !bytes.Equal(data, append(original, extra...)) {
				t.Fatal("flush changed layout or padded EOF", err)
			}
			if counted.writes > historyReferenceMetadataPages+3 {
				t.Fatal("flush wrote entries instead of pages", counted.writes)
			}
		})
	}
}

func TestHistoryReferenceContainerMetadataFlushFailureCannotFinalize(t *testing.T) {
	for _, table := range []string{"chunks", "spans"} {
		t.Run(table, func(t *testing.T) {
			dir := t.TempDir()
			w, err := newHistoryReferenceWriter(context.Background(), dir, 0)
			if err != nil {
				t.Fatal(err)
			}
			sentinel := errors.New("flush write failed")
			if table == "chunks" {
				w.chunkMetadata = newHistoryReferenceMetadataCache(w.ctx, &referenceMetadataCountingFile{File: w.chunks, writeErr: sentinel})
			} else {
				w.spanMetadata = newHistoryReferenceMetadataCache(w.ctx, &referenceMetadataCountingFile{File: w.spans, writeErr: sentinel})
			}
			if _, err = w.WriteLiteral([]byte("buffered complete data")); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "must-not-finalize")
			if _, _, err = w.Finalize(path); !errors.Is(err, sentinel) {
				t.Fatal("pending metadata error lost", err)
			}
			if _, err = os.Stat(path); !os.IsNotExist(err) {
				t.Fatal("failed metadata left output", err)
			}
			tmp := w.dir
			if err = w.Release(); err != nil {
				t.Fatal(err)
			}
			if _, err = os.Stat(tmp); !os.IsNotExist(err) {
				t.Fatal("failed metadata scratch not removed", err)
			}
			if w.chunkMetadata != nil || w.spanMetadata != nil {
				t.Fatal("cache retained after release")
			}
		})
	}
}

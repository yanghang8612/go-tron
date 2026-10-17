package objstorageprovider

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

var (
	writerTestWriteErr  = errors.New("injected write error")
	writerTestSyncErr   = errors.New("injected sync error")
	writerTestSyncToErr = errors.New("injected sync-to error")
	writerTestCloseErr  = errors.New("injected close error")
)

type writerBufferTestFile struct {
	vfs.File
	data                                   []byte
	events                                 []string
	writeErr, syncErr, syncToErr, closeErr error
	shortNil, partial                      bool
	syncToOffsets                          []int64
	syncToWritten                          []int64
}

func (f *writerBufferTestFile) Write(p []byte) (int, error) {
	n := len(p)
	if f.partial {
		n /= 2
	} else if f.shortNil {
		n--
	}
	if f.writeErr != nil && !f.partial {
		n = 0
	}
	if n < 0 {
		n = 0
	}
	f.events = append(f.events, fmt.Sprintf("write:%d/%d", n, len(p)))
	f.data = append(f.data, p[:n]...)
	return n, f.writeErr
}
func (f *writerBufferTestFile) Sync() error { f.events = append(f.events, "sync"); return f.syncErr }
func (f *writerBufferTestFile) SyncData() error {
	f.events = append(f.events, "sync-data")
	return f.syncErr
}
func (f *writerBufferTestFile) SyncTo(n int64) (bool, error) {
	f.events = append(f.events, fmt.Sprintf("sync-to:%d", n))
	f.syncToOffsets = append(f.syncToOffsets, n)
	f.syncToWritten = append(f.syncToWritten, int64(len(f.data)))
	return false, f.syncToErr
}
func (f *writerBufferTestFile) Close() error { f.events = append(f.events, "close"); return f.closeErr }
func (f *writerBufferTestFile) Fd() uintptr  { return 123 }
func TestFileWritableBufferedFinishAndAbort(t *testing.T) {
	t.Run("finish", func(t *testing.T) {
		f := new(writerBufferTestFile)
		w := newFileBufferedWritable(f)
		payload := bytes.Repeat([]byte{0x7a}, 37)
		if e := w.Write(payload); e != nil {
			t.Fatal(e)
		}
		if len(f.data) != 0 {
			t.Fatal("small payload was not buffered")
		}
		if e := w.Finish(); e != nil {
			t.Fatal(e)
		}
		if !bytes.Equal(f.data, payload) || strings.Join(f.events, ",") != "write:37/37,sync,close" {
			t.Fatal(f.events)
		}
		if w.file != nil || w.bw != nil {
			t.Fatal("Finish retained resources")
		}
	})
	t.Run("abort-residual", func(t *testing.T) {
		f := new(writerBufferTestFile)
		w := newFileBufferedWritable(f)
		if e := w.Write([]byte("unpublished tail")); e != nil {
			t.Fatal(e)
		}
		w.Abort()
		if len(f.data) != 0 || strings.Join(f.events, ",") != "close" || w.file != nil || w.bw != nil {
			t.Fatal("Abort flushed residual or retained resources", f.events)
		}
	})
	t.Run("abort-written-prefix", func(t *testing.T) {
		f := new(writerBufferTestFile)
		w := newFileBufferedWritable(vfs.NewSyncingFile(f, vfs.SyncingFileOptions{BytesPerSync: 512 << 10}))
		prefix := bytes.Repeat([]byte{0x5a}, 256<<10)
		if e := w.Write(prefix); e != nil {
			t.Fatal(e)
		}
		if e := w.Write([]byte("discard this residual")); e != nil {
			t.Fatal(e)
		}
		w.Abort()
		if !bytes.Equal(f.data, prefix) || strings.Join(f.events, ",") != "write:262144/262144,sync-data,close" {
			t.Fatal("Abort must preserve existing syncingFile prefix behavior", f.events)
		}
	})
}
func TestFileWritableBufferedErrors(t *testing.T) {
	for _, tc := range []struct {
		name                              string
		writeErr, syncErr, closeErr, want error
		short, partial, direct            bool
	}{
		{name: "flush-and-close-first-error", writeErr: writerTestWriteErr, closeErr: writerTestCloseErr, want: writerTestWriteErr},
		{name: "short-nil-flush", want: io.ErrShortWrite, short: true},
		{name: "partial-flush", writeErr: writerTestWriteErr, want: writerTestWriteErr, partial: true},
		{name: "partial-direct-write", writeErr: writerTestWriteErr, want: writerTestWriteErr, partial: true, direct: true},
		{name: "sync-and-close-first-error", syncErr: writerTestSyncErr, closeErr: writerTestCloseErr, want: writerTestSyncErr},
		{name: "close", closeErr: writerTestCloseErr, want: writerTestCloseErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &writerBufferTestFile{writeErr: tc.writeErr, syncErr: tc.syncErr, closeErr: tc.closeErr, shortNil: tc.short, partial: tc.partial}
			w := newFileBufferedWritable(f)
			n := 37
			if tc.direct {
				n = 256 << 10
			}
			payload := bytes.Repeat([]byte{3}, n)
			writeErr := w.Write(payload)
			if tc.direct {
				if !errors.Is(writeErr, tc.want) {
					t.Fatal("direct write error", writeErr)
				}
			} else if writeErr != nil {
				t.Fatal("buffered write failed before flush", writeErr)
			}
			if e := w.Finish(); !errors.Is(e, tc.want) {
				t.Fatal("first error changed", e, tc.want)
			}
			if f.events[len(f.events)-1] != "close" {
				t.Fatal("did not close after failure", f.events)
			}
			if tc.writeErr != nil || tc.short {
				for _, e := range f.events {
					if e == "sync" {
						t.Fatal("sync after failed flush", f.events)
					}
				}
			}
			if w.file != nil || w.bw != nil {
				t.Fatal("error retained resources")
			}
		})
	}
}
func TestFileWritableBufferedSyncToError(t *testing.T) {
	f := &writerBufferTestFile{syncToErr: writerTestSyncToErr, closeErr: writerTestCloseErr}
	w := newFileBufferedWritable(vfs.NewSyncingFile(f, vfs.SyncingFileOptions{BytesPerSync: 512 << 10}))
	payload := bytes.Repeat([]byte{7}, 3<<20)
	if e := w.Write(payload); !errors.Is(e, writerTestSyncToErr) {
		t.Fatal("expected original SyncTo error", e)
	}
	if e := w.Finish(); !errors.Is(e, writerTestSyncToErr) {
		t.Fatal("Finish changed sticky write/SyncTo error", e)
	}
	if len(f.syncToOffsets) != 1 || f.syncToOffsets[0] != (2<<20) {
		t.Fatal("sync tail/alignment changed", f.events)
	}
	if len(f.data) != len(payload) || f.events[len(f.events)-1] != "close" {
		t.Fatal("written prefix/close changed", f.events)
	}
}
func TestFileWritableBufferedSyncingThresholdContract(t *testing.T) {
	f := new(writerBufferTestFile)
	w := newFileBufferedWritable(vfs.NewSyncingFile(f, vfs.SyncingFileOptions{BytesPerSync: 512 << 10}))
	block := bytes.Repeat([]byte{0x3b}, 4096+5)
	for i := 0; i < 800; i++ {
		if e := w.Write(block); e != nil {
			t.Fatal(e)
		}
	}
	if e := w.Finish(); e != nil {
		t.Fatal(e)
	}
	if len(f.data) != 800*len(block) || len(f.syncToOffsets) == 0 {
		t.Fatal("missing written data or periodic sync", len(f.data), f.events)
	}
	previous := int64(0)
	for j, n := range f.syncToOffsets {
		if n%4096 != 0 || n-previous < 512<<10 || n > f.syncToWritten[j]-(1<<20) {
			t.Fatal("changed sync alignment/threshold/tail", f.syncToOffsets)
		}
		previous = n
	}
	if f.events[len(f.events)-2] != "sync-data" || f.events[len(f.events)-1] != "close" {
		t.Fatal("Finish must still durably sync then close", f.events)
	}
	t.Logf("periodic SyncTo offsets=%v; Flush granularity may move initiation offsets", f.syncToOffsets)
}

func TestFileWritableBoundedActiveBuffers(t *testing.T) {
	runtime.GC()
	var before, held runtime.MemStats
	runtime.ReadMemStats(&before)
	writers := make([]*fileBufferedWritable, 8)
	total := 0
	for i := range writers {
		writers[i] = newFileBufferedWritable(new(writerBufferTestFile))
		total += writers[i].bw.Size()
	}
	runtime.GC()
	runtime.ReadMemStats(&held)
	if total > 8*(64<<10) {
		t.Fatal("unbounded active writable buffers", total)
	}
	t.Logf("8 simultaneous buffers=%d bytes; Go held heap delta=%d bytes", total, int64(held.HeapAlloc)-int64(before.HeapAlloc))
	runtime.KeepAlive(writers)
	for _, w := range writers {
		w.Abort()
	}
}

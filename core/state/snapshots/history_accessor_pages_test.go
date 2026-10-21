package snapshots

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestHistoryAccessorPagesSmallReadsLargeBypassAndClose(t *testing.T) {
	data := make([]byte, historyDirectoryPageSize*12+31)
	for i := range data {
		data[i] = byte(i % 251)
	}
	path := filepath.Join(t.TempDir(), "accessor.kv")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := newHistoryAccessorPages(ctx, file, uint64(len(data)))
	defer r.Close()
	probe := &historyPageProbe{data: data}
	r.pages.source = probe
	for _, size := range []int{8, 36, 12, 120, 120, 12, 120} {
		buf := make([]byte, size)
		if _, err := r.ReadAt(buf, 0); err != nil || !bytes.Equal(buf, data[:size]) {
			t.Fatal(err)
		}
	}
	if probe.calls != 1 {
		t.Fatal("repeated header reads", probe.calls)
	}
	// Large direct I/O must not split a sequential read into thousands of pages.
	large := make([]byte, 4097)
	if _, err := r.ReadAt(large, 6000); err != nil || !bytes.Equal(large, data[6000:6000+len(large)]) {
		t.Fatal(err)
	}
	if probe.calls != 1 {
		t.Fatal("large read filled pages")
	}
	var wg sync.WaitGroup
	for n := 0; n < 10; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			buf := make([]byte, 29)
			off := int64(n*4096 + 4087)
			if _, err := r.ReadAt(buf, off); err != nil || !bytes.Equal(buf, data[off:off+29]) {
				t.Error("cross page", err)
			}
		}(n)
	}
	wg.Wait()
	if r.pages.count > historyDirectoryPageCount {
		t.Fatal("unbounded pages")
	}
	buf := make([]byte, 20)
	if n, err := r.ReadAt(buf, int64(len(data)-3)); n != 3 || !errors.Is(err, io.EOF) || !bytes.Equal(buf[:3], data[len(data)-3:]) {
		t.Fatal("EOF", n, err)
	}
	cancel()
	if _, err := r.ReadAt(buf, 0); !errors.Is(err, context.Canceled) {
		t.Fatal("cached cancellation", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ReadAt(buf, 0); !errors.Is(err, os.ErrClosed) {
		t.Fatal("descriptor leaked", err)
	}
	if _, err := r.ReadAt(buf, 0); !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
}

func TestHistoryAccessorPagesFreshOpenRejectsPhysicalCorruption(t *testing.T) {
	data := buildStateDomainChangeV7AccessorBytes(t, 2048)
	h, err := decodeStateDomainChangeBinaryAccessorV7Header(bytes.NewReader(data), uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "accessor.kv")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccessor, Path: "accessor.kv", FromTxNum: h.fromTxNum, ToTxNum: h.toTxNum, Size: uint64(len(data))}
	r, _, size, err := openStateDomainChangeBinaryAccessorReader(dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := r.(*historyAccessorPages); !ok {
		t.Fatal("plain accessor not buffered")
	}
	if _, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(r, h, 0); err != nil {
		t.Fatal(err)
	}
	r.Close()
	off := int(h.headerSize + h.blockCount*8)
	data[off+3] ^= 1
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	fresh, _, _, err := openStateDomainChangeBinaryAccessorReader(dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if _, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(fresh, h, 0); err == nil {
		t.Fatal("new reader reused corrupt directory bytes", size)
	}
}

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

type historyPhysicalProbe struct {
	historyPageProbe
	requested int
}

func (p *historyPhysicalProbe) ReadAt(dst []byte, off int64) (int, error) {
	p.requested += len(dst)
	return p.historyPageProbe.ReadAt(dst, off)
}

func TestHistoryAccessorPagesUnalignedReadAmplificationAndFailedReuse(t *testing.T) {
	data := bytes.Repeat([]byte{7}, 14*historyDirectoryPageSize)
	path := filepath.Join(t.TempDir(), "accessor.kv")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	r := newHistoryAccessorPages(context.Background(), file, uint64(len(data)))
	defer r.Close()
	probe := &historyPhysicalProbe{historyPageProbe: historyPageProbe{data: data}}
	r.pages.source = probe
	for _, size := range []int{8, 36, 12, 120} {
		if _, err := r.ReadAt(make([]byte, size), 0); err != nil {
			t.Fatal(err)
		}
	}
	if probe.calls != 1 || probe.requested != 128 {
		t.Fatal("small header read amplification", probe.calls, probe.requested)
	}
	page := make([]byte, 4096)
	if _, err := r.ReadAt(page, 120); err != nil {
		t.Fatal(err)
	}
	if probe.calls != 2 || probe.requested != 128+4096 {
		t.Fatal("unaligned directory split into multiple reads", probe.calls, probe.requested)
	}
	for i := 1; i < historyDirectoryPageCount; i++ {
		if _, err := r.ReadAt(page, int64(120+i*4096)); err != nil {
			t.Fatal(err)
		}
	}
	probe.fail = true
	if _, err := r.ReadAt(page, 120+10*4096); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	probe.fail = false
	data[0] = 9
	before := probe.calls
	if _, err := r.ReadAt(page[:8], 0); err != nil || page[0] != 9 || probe.calls != before+1 {
		t.Fatal("failed window reused stale bytes", err, page[0], probe.calls)
	}
}

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

func TestHistoryAccessorPagesCompressedSourceLifetimeAndValidation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "history.seg")
	raw := bytes.Repeat([]byte("history-physical-prefix-"), 9000)
	if err := compressBlobToFile(dir, path, raw, historyCompressChunkSize); err != nil {
		t.Fatal(err)
	}
	open := func() (*compressedBlockReader, *historyAccessorPages, error) {
		file, err := os.Open(path)
		if err != nil {
			return nil, nil, err
		}
		stat, err := file.Stat()
		if err != nil {
			file.Close()
			return nil, nil, err
		}
		pages := newHistoryAccessorPages(context.Background(), file, uint64(stat.Size()))
		var magic [8]byte
		if _, err := pages.ReadAt(magic[:], 0); err != nil {
			pages.Close()
			return nil, pages, err
		}
		reader, err := openCompressedBlockFileWithCacheLimitContext(context.Background(), pages, stat, 1)
		return reader, pages, err
	}
	reader, pages, err := open()
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 128)
	if _, err := reader.ReadAt(buf, 0); err != nil || !bytes.Equal(buf, raw[:128]) {
		t.Fatal("buffered compressed prefix", err)
	}
	if err := reader.Close(); err != nil || pages.pages != nil {
		t.Fatal("physical pages survived compressed close", err)
	}
	if _, err := pages.file.ReadAt(buf, 0); !errors.Is(err, os.ErrClosed) {
		t.Fatal("physical descriptor leaked", err)
	}
	// A later open must read changed physical bytes rather than pooled pages.
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte("BROKEN!!"), 0); err != nil {
		t.Fatal(err)
	}
	file.Close()
	if reader, pages, err = open(); err == nil {
		reader.Close()
		t.Fatal("fresh compressed open accepted stale magic")
	}
	if pages != nil && pages.pages != nil {
		t.Fatal("failed compressed open retained pages")
	}
}

func TestHistoryFramedHeaderDescriptorLifetime(t *testing.T) {
	data := buildStateDomainChangeV7AccessorBytes(t, 2048)
	path := filepath.Join(t.TempDir(), "accessor.kv")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	open := func(ctx context.Context) *historyAccessorPages {
		t.Helper()
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		return newHistoryAccessorPages(ctx, f, uint64(len(data)))
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := open(ctx)
	h, err := decodeStateDomainChangeBinaryAccessorV7Header(r, uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	// Evict the physical prefix. Parsed metadata, rather than just a warm
	// physical page, must survive repeated validation on this descriptor.
	for i := 0; i < historyDirectoryPageCount; i++ {
		off := int64(len(data) - historyDirectoryPageSize - i*historyDirectoryPageSize)
		if off < 128 {
			break
		}
		if _, err := r.ReadAt(make([]byte, 4096), off); err != nil {
			t.Fatal(err)
		}
	}
	probe := &historyPageProbe{data: data, fail: true}
	r.pages.source = probe
	for i := 0; i < 3; i++ {
		got, err := decodeStateDomainChangeBinaryAccessorV7Header(contextReaderAt{ctx: ctx, r: r}, uint64(len(data)))
		if err != nil || got.dictionaryDigest != h.dictionaryDigest || got.recordCount != h.recordCount {
			t.Fatal("same descriptor", got, err)
		}
	}
	if probe.calls != 0 {
		t.Fatal("validated header was physically reread", probe.calls)
	}
	if _, err := decodeStateDomainChangeBinaryAccessorV7Header(r, uint64(len(data)-1)); err == nil {
		t.Fatal("cached header accepted a different file size")
	}
	outer, cancelOuter := context.WithCancel(context.Background())
	cancelOuter()
	if _, err := decodeStateDomainChangeBinaryAccessorV7Header(contextReaderAt{ctx: outer, r: r}, uint64(len(data))); !errors.Is(err, context.Canceled) {
		t.Fatal("outer cancellation bypassed", err)
	}
	cancel()
	if _, err := decodeStateDomainChangeBinaryAccessorV7Header(r, uint64(len(data))); !errors.Is(err, context.Canceled) {
		t.Fatal("descriptor cancellation bypassed", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if r.framedHeader != nil {
		t.Fatal("parsed metadata survived close")
	}
	if _, err := decodeStateDomainChangeBinaryAccessorV7Header(r, uint64(len(data))); !errors.Is(err, os.ErrClosed) {
		t.Fatal("closed descriptor", err)
	}
	data[0] ^= 1
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	fresh := open(context.Background())
	defer fresh.Close()
	if _, err := decodeStateDomainChangeBinaryAccessorV7Header(fresh, uint64(len(data))); err == nil {
		t.Fatal("new descriptor accepted changed physical header")
	}
	if fresh.framedHeader != nil {
		t.Fatal("failed validation cached metadata")
	}
}

func TestHistoryFramedHeaderConcurrentReads(t *testing.T) {
	data := buildStateDomainChangeV7AccessorBytes(t, 512)
	path := filepath.Join(t.TempDir(), "accessor.kv")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	r := newHistoryAccessorPages(context.Background(), f, uint64(len(data)))
	defer r.Close()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 8; j++ {
				h, err := decodeStateDomainChangeBinaryAccessorV7Header(r, uint64(len(data)))
				if err != nil || h.keyCount != 512 {
					t.Error(h.keyCount, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

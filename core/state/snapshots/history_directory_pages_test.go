package snapshots

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

type historyPageProbe struct {
	data  []byte
	calls int
	fail  bool
}

func (p *historyPageProbe) ReadAt(dst []byte, off int64) (int, error) {
	p.calls++
	if p.fail {
		return 0, io.ErrUnexpectedEOF
	}
	return bytes.NewReader(p.data).ReadAt(dst, off)
}

func TestHistoryDirectoryPagesBoundedReuseAndIsolation(t *testing.T) {
	data := make([]byte, historyDirectoryPageSize*12+51)
	for i := range data {
		data[i] = byte(i % 251)
	}
	src := &historyPageProbe{data: data}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := acquireHistoryDirectoryPages(ctx, src, 120, uint64(len(data)-120))
	defer releaseHistoryDirectoryPages(p)
	var got [23]byte
	for i := 0; i < 3; i++ {
		if _, e := p.ReadAt(got[:], 137); e != nil || !bytes.Equal(got[:], data[137:160]) {
			t.Fatalf("read %x %v", got, e)
		}
	}
	if src.calls != 1 {
		t.Fatalf("calls %d, want one page", src.calls)
	}
	got[0] ^= 1
	if _, e := p.ReadAt(got[:], 137); e != nil || got[0] != data[137] {
		t.Fatal("output aliases a page")
	}
	// Cross a page and then cycle beyond capacity; old slots must not retain
	// their previous identities when overwritten.
	for i := 0; i < 12; i++ {
		off := int64(120 + i*historyDirectoryPageSize)
		if _, e := p.ReadAt(got[:], off); e != nil || !bytes.Equal(got[:], data[off:off+23]) {
			t.Fatalf("eviction read %d %v", i, e)
		}
	}
	if p.count != historyDirectoryPageCount {
		t.Fatal("unbounded page count")
	}
	if _, e := p.ReadAt(got[:], 137); e != nil || !bytes.Equal(got[:], data[137:160]) {
		t.Fatal("eviction changed bytes")
	}
	if _, e := p.ReadAt(got[:], 120+historyDirectoryPageSize-11); e != nil || !bytes.Equal(got[:], data[120+historyDirectoryPageSize-11:120+historyDirectoryPageSize+12]) {
		t.Fatal("cross page read")
	}
	cancel()
	if _, e := p.ReadAt(got[:], 137); !errors.Is(e, context.Canceled) {
		t.Fatal("cached read ignored cancel")
	}
	other := acquireHistoryDirectoryPages(context.Background(), &historyPageProbe{data: bytes.Repeat([]byte{99}, len(data))}, 120, uint64(len(data)-120))
	if _, e := other.ReadAt(got[:], 137); e != nil || got[0] != 99 {
		t.Fatal("cross reader page leak")
	}
	releaseHistoryDirectoryPages(other)
}

func TestHistoryDirectoryPagesFailedFillAndShortFinalPage(t *testing.T) {
	src := &historyPageProbe{data: bytes.Repeat([]byte{7}, historyDirectoryPageSize+23)}
	p := acquireHistoryDirectoryPages(context.Background(), src, 0, uint64(len(src.data)))
	defer releaseHistoryDirectoryPages(p)
	var got [8]byte
	if _, e := p.ReadAt(got[:], historyDirectoryPageSize+15); e != nil {
		t.Fatal(e)
	}
	src.fail = true
	if _, e := p.ReadAt(got[:], 0); !errors.Is(e, io.ErrUnexpectedEOF) {
		t.Fatal("missing failed fill")
	}
	src.fail = false
	if _, e := p.ReadAt(got[:], 0); e != nil || got[0] != 7 {
		t.Fatal("failed page published")
	}
	if _, e := p.ReadAt(got[:], int64(len(src.data)-3)); !errors.Is(e, io.EOF) {
		t.Fatal("lost ReaderAt EOF")
	}
}

func TestHistoryDirectoryCoalescingRetainsCRCAndBounds(t *testing.T) {
	data := buildStateDomainChangeV7AccessorBytes(t, 2048)
	h, e := decodeStateDomainChangeBinaryAccessorV7Header(bytes.NewReader(data), uint64(len(data)))
	if e != nil {
		t.Fatal(e)
	}
	src := &historyPageProbe{data: data}
	block, e := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(src, h, 1)
	if e != nil || len(block.firstKey) == 0 || src.calls != 2 {
		t.Fatalf("coalesced entry calls=%d err=%v", src.calls, e)
	}
	// Locate the first CRC-protected entry through the offset table and flip its
	// content. Coalescing/windowing must not bypass its existing CRC check.
	bad := append([]byte(nil), data...)
	off := int(h.headerSize + h.blockCount*8)
	bad[off+3] ^= 1
	if _, e := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(bytes.NewReader(bad), h, 0); e == nil {
		t.Fatal("directory CRC corruption accepted")
	}
	pages := acquireHistoryDirectoryPages(context.Background(), bytes.NewReader(bad), h.headerSize, h.blockDirLen)
	defer releaseHistoryDirectoryPages(pages)
	if _, e := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(pages, h, 0); e == nil {
		t.Fatal("page cache bypassed directory CRC")
	}
}

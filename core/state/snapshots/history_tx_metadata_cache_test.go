package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestHistoryTxMetadataExactBytesOwnsInputAndRejectsCandidateCollision(t *testing.T) {
	c := &historyTxMetadataCache[int, uint64]{maxBytes: 32, maxEntries: 2, rowBytes: 8}
	raw := []byte{1, 2, 3}
	c.put(1, raw, []uint64{9})
	raw[0] = 4
	if _, ok := c.get(1, raw); ok {
		t.Fatal("same key with different current bytes reused parse")
	}
	if rows, ok := c.get(1, []byte{1, 2, 3}); !ok || rows[0] != 9 {
		t.Fatal("cache retained caller scratch instead of its own bytes")
	}
	if _, ok := c.get(1, []byte{1, 2}); ok {
		t.Fatal("prefix accepted as full current content")
	}
	if c.bytes != 11 {
		t.Fatal("raw bytes missing from retained budget", c.bytes)
	}
}

func TestHistoryTxMetadataCacheBoundsAndConcurrentEviction(t *testing.T) {
	c := &historyTxMetadataCache[int, uint64]{maxBytes: 32, maxEntries: 3, rowBytes: 8}
	for _, k := range []int{1, 2, 3} {
		c.put(k, nil, make([]uint64, 1))
	}
	c.put(4, nil, make([]uint64, 2)) // count and byte eviction
	if _, ok := c.get(1, nil); ok {
		t.Fatal("oldest entry survived")
	}
	c.put(5, nil, make([]uint64, 3)) // multiple evictions
	if _, ok := c.get(5, nil); !ok {
		t.Fatal("new value missing")
	}
	c.put(6, nil, make([]uint64, 5)) // oversize bypass
	if _, ok := c.get(6, nil); ok {
		t.Fatal("oversize retained")
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := g*500 + i
				c.put(k, nil, make([]uint64, 1+i%3))
				c.get(k, nil)
			}
		}(g)
	}
	wg.Wait()
	if len(c.values) > 3 || len(c.fifo) != 3 || c.bytes > 32 {
		t.Fatal("unbounded cache")
	}
	total := 0
	for _, v := range c.values {
		total += len(v.raw) + cap(v.rows)*8
	}
	if total != c.bytes {
		t.Fatal("accounting mismatch", total, c.bytes)
	}
	for i := 0; i < len(c.values); i++ {
		if _, ok := c.values[c.fifo[(c.next+i)%len(c.fifo)]]; !ok {
			t.Fatal("ring lost a live value")
		}
	}
}

func TestHistoryTxScratchBoundAndOwnership(t *testing.T) {
	p := new(historyTxScratchPool)
	var buffers [][]byte
	for i := 0; i < 12; i++ {
		b := p.take(400000)
		b[0] = byte(i)
		buffers = append(buffers, b)
	}
	for i, b := range buffers {
		if b[0] != byte(i) {
			t.Fatal("active buffers alias")
		}
		p.release(b)
	}
	if p.bytes > historyTxScratchBytes || len(p.idle) > historyTxScratchCount {
		t.Fatal("unbounded scratch")
	}
	p.release(make([]byte, historyTxDirectoryLimit+1))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				b := p.take(4096)
				for k := range b {
					b[k] = 1
				}
				p.release(b)
			}
		}()
	}
	wg.Wait()
	if p.bytes > historyTxScratchBytes || len(p.idle) > historyTxScratchCount {
		t.Fatal("concurrent scratch bound")
	}
}

func TestHistoryTxDirectoryReuseReauthenticatesAndBindsBounds(t *testing.T) {
	data, _ := buildOwnerAccessorFixture(t, 11, 4200, true)
	h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(bytes.NewReader(data), uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	c := &historyTxMetadataCache[historyTxDirectoryKey, historyTxPage]{maxBytes: historyTxMetadataBytes, maxEntries: 2, rowBytes: 32}
	r1 := &boundedReadCounter{R: bytes.NewReader(data)}
	a, err := decodeHistoryTxFooterWithCache(r1, uint64(len(data)), h, c)
	if err != nil {
		t.Fatal(err)
	}
	r2 := &boundedReadCounter{R: bytes.NewReader(data)}
	b, err := decodeHistoryTxFooterWithCache(r2, uint64(len(data)), h, c)
	if err != nil || a.r == b.r || len(c.values) != 1 || &a.pages[0] != &b.pages[0] {
		t.Fatal("metadata not independently attached", err)
	}
	if r1.Calls != 2 || r2.Calls != 2 || r1.Bytes != r2.Bytes {
		t.Fatal("cache skipped current physical bytes")
	}
	if _, err = a.page(0); err != nil {
		t.Fatal(err)
	}
	if b.cache != nil || b.cacheBytes != 0 {
		t.Fatal("mutable page state leaked between tables")
	}
	if _, err = b.page(0); err != nil {
		t.Fatal(err)
	}
	if r2.Calls != 3 {
		t.Fatal("warm run parse skipped current page read")
	}
	for _, kind := range []string{"records", "from", "to"} {
		bad := h
		switch kind {
		case "records":
			bad.recordCount++
		case "from":
			bad.fromTxNum = a.pages[0].firstTx + 1
		case "to":
			bad.toTxNum = a.pages[0].firstTx - 1
		}
		if _, err := decodeHistoryTxFooterWithCache(bytes.NewReader(data), uint64(len(data)), bad, c); err == nil {
			t.Fatal("warm directory bypassed header bound", kind)
		}
	}
	bad := bytes.Clone(data)
	dir := a.prefix + a.dataLen
	bad[dir+26] = 1 // semantically invalid, but both CRCs repaired
	footer := bad[len(bad)-64:]
	binary.BigEndian.PutUint32(footer[52:56], crc32.ChecksumIEEE(bad[dir:uint64(len(bad)-64)]))
	binary.BigEndian.PutUint32(footer[60:], crc32.ChecksumIEEE(footer[:60]))
	if _, err := decodeHistoryTxFooterWithCache(bytes.NewReader(bad), uint64(len(bad)), h, c); err == nil {
		t.Fatal("changed authenticated bytes reused old parse")
	}
	if len(c.values) != 1 {
		t.Fatal("failed parse cached")
	}
}

func TestHistoryTxRunReuseBindsNeighborAndRecordBounds(t *testing.T) {
	data, _ := buildOwnerAccessorFixture(t, 11, 4200, true)
	open := func() *historyTxTable {
		h, e := decodeStateDomainChangeBinaryAccessorKeyHeader(bytes.NewReader(data), uint64(len(data)))
		if e != nil {
			t.Fatal(e)
		}
		return h.txRuns
	}
	a := open()
	runs, err := a.page(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"records", "to", "neighbor"} {
		b := open()
		b.pages = append([]historyTxPage(nil), b.pages...)
		switch kind {
		case "records":
			b.recordCount = uint64(b.pages[0].rows) - 1
		case "to":
			b.to = runs[len(runs)-1].tx - 1
		case "neighbor":
			if len(b.pages) < 2 {
				t.Fatal("fixture needs two pages")
			}
			b.pages[1].firstTx = runs[len(runs)-1].tx
		}
		if _, err = b.page(0); err == nil {
			t.Fatal("warm run parse bypassed bound", kind)
		}
	}
	bad := bytes.Clone(data)
	bad[a.prefix+a.pages[0].off] ^= 1
	b := open()
	b.r = bytes.NewReader(bad)
	if _, err = b.page(0); err == nil {
		t.Fatal("warm run parse masked page corruption")
	}
}

func TestHistoryTxMetadataWarmOpenRejectsRestoredMtimeCorruption(t *testing.T) {
	data, keys := buildOwnerAccessorFixture(t, 11, 4200, true)
	path := filepath.Join(t.TempDir(), "accessor.kv")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	open := func(ctx context.Context) *historyAccessorPages {
		f, e := os.Open(path)
		if e != nil {
			t.Fatal(e)
		}
		return newHistoryAccessorPages(ctx, f, uint64(len(data)))
	}
	r := open(context.Background())
	h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(r, uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	bad := bytes.Clone(data)
	bad[h.txRuns.prefix+h.txRuns.dataLen] ^= 1
	if err = os.WriteFile(path, bad, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	for _, key := range [][]byte{{0xff}, keys[1].key} {
		fresh := open(context.Background())
		_, _, _, err = stateDomainChangeBinaryAccessorV7Lookup(fresh, uint64(len(data)), key)
		fresh.Close()
		if err == nil {
			t.Fatal("fresh negative/singleton accepted stale directory")
		}
	}
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	fresh := open(ctx)
	cancel()
	if _, err = decodeStateDomainChangeBinaryAccessorKeyHeader(fresh, uint64(len(data))); !errors.Is(err, context.Canceled) {
		t.Fatal("warm canceled", err)
	}
	fresh.Close()
	if _, err = decodeStateDomainChangeBinaryAccessorKeyHeader(fresh, uint64(len(data))); !errors.Is(err, os.ErrClosed) {
		t.Fatal("warm closed", err)
	}
	if _, err = decodeHistoryTxFooter(bytes.NewReader(data[:len(data)-1]), uint64(len(data)), h); !errors.Is(err, io.EOF) {
		t.Fatal("warm truncated", err)
	}
}

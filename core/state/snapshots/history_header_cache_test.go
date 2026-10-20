package snapshots

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestHistoryHeaderCacheBoundAndOwnership(t *testing.T) {
	c := &historyHeaderCache{limit: 2}
	input := bytes.Repeat([]byte{1}, 256)
	a := historyHeaderKey{digest: sha256.Sum256([]byte("a")), decodedSize: 256}
	b := historyHeaderKey{digest: sha256.Sum256([]byte("b")), decodedSize: 256}
	d := historyHeaderKey{digest: sha256.Sum256([]byte("d")), decodedSize: 256}
	c.put(a, input)
	input[0] = 9
	v, ok := c.get(a)
	if !ok || v.bytes[0] != 1 {
		t.Fatal("input aliases cache")
	}
	v.bytes[0] = 8
	v2, _ := c.get(a)
	if v2.bytes[0] != 1 {
		t.Fatal("output aliases cache")
	}
	c.put(b, input)
	c.put(d, input)
	if _, ok := c.get(a); ok || len(c.entries) != 2 || len(c.fifo) != 2 {
		t.Fatal("cache did not evict")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				c.put(a, input)
				c.get(a)
			}
		}()
	}
	wg.Wait()
}

func TestHistoryHeaderCacheRereadsPhysicalBytes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "header.seg")
	raw := bytes.Repeat([]byte("header-data-"), 16000)
	if err := compressBlobToFile(dir, path, raw, historyCompressChunkSize); err != nil {
		t.Fatal(err)
	}
	c := &historyHeaderCache{limit: 2}
	read := func() error {
		r, err := openCompressedBlockReader(path)
		if err != nil {
			return err
		}
		defer r.Close()
		r.prefixCache = c
		out := make([]byte, 96)
		_, err = r.ReadAt(out, 0)
		if err == nil && !bytes.Equal(out, raw[:96]) {
			t.Fatal("wrong decoded header")
		}
		if len(r.cache) == 0 && len(c.entries) == 0 {
			t.Fatal("missing cache")
		}
		return err
	}
	if err := read(); err != nil {
		t.Fatal(err)
	}
	if err := read(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	r, err := openCompressedBlockReader(path)
	if err != nil {
		t.Fatal(err)
	}
	off := r.dataOff + r.table[0].compressedStart + r.table[0].compressedLen - 1
	r.Close()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	one := []byte{0}
	if _, err = f.ReadAt(one, int64(off)); err != nil {
		t.Fatal(err)
	}
	one[0] ^= 0x80
	if _, err = f.WriteAt(one, int64(off)); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if err := read(); err == nil {
		t.Fatal("same-size/mtime corrupted compressed bytes hit cache")
	}
	if err := os.Truncate(path, int64(off)); err != nil {
		t.Fatal(err)
	}
	if err := read(); err == nil {
		t.Fatal("truncated input hit cache")
	}
}

func BenchmarkHistoryHeaderReopen(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "header.seg")
	raw := bytes.Repeat([]byte("header-data-and-transaction-range-"), 16384)
	if err := compressBlobToFile(dir, path, raw, historyCompressChunkSize); err != nil {
		b.Fatal(err)
	}
	for _, enabled := range []bool{false, true} {
		name := "uncached"
		if enabled {
			name = "content-verified-prefix"
		}
		b.Run(name, func(b *testing.B) {
			c := &historyHeaderCache{limit: 32}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				r, err := openCompressedBlockReader(path)
				if err != nil {
					b.Fatal(err)
				}
				if enabled {
					r.prefixCache = c
				}
				var out [96]byte
				_, err = r.ReadAt(out[:], 0)
				r.Close()
				if err != nil || !bytes.Equal(out[:], raw[:96]) {
					b.Fatalf("read: %v", err)
				}
			}
		})
	}
}

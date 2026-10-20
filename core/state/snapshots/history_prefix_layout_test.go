package snapshots

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestHistorySmallPrefixLayoutAndBackpatch(t *testing.T) {
	for _, format := range []string{"1", "2"} {
		for _, size := range []int{0, 76, 127, 128, 129, historyCompressChunkSize + 128, 3*historyCompressChunkSize + 213} {
			t.Run(fmt.Sprintf("%s/%d", format, size), func(t *testing.T) {
				dir := t.TempDir()
				w, err := newHistoryCompressedStreamFormat(context.Background(), dir, historyCompressChunkSize, 2, format)
				if err != nil {
					t.Fatal(err)
				}
				defer w.Abort()
				raw := make([]byte, size)
				rand.New(rand.NewSource(320)).Read(raw)
				for start := 0; start < len(raw); {
					end := min(len(raw), start+93)
					if _, err := w.Write(raw[start:end]); err != nil {
						t.Fatal(err)
					}
					start = end
				}
				if size >= 76 {
					patch := []byte("12345678")
					if _, err := w.WriteAt(patch, 68); err != nil {
						t.Fatal(err)
					}
					copy(raw[68:], patch)
				}
				if _, err := w.WriteAt([]byte{1}, 128); err == nil {
					t.Fatal("backpatch outside prefix accepted")
				}
				path := filepath.Join(dir, "output.seg")
				if err := w.Finish(path); err != nil {
					t.Fatal(err)
				}
				r, err := openCompressedBlockReader(path)
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				if size > 128 && r.table[1].uncompressedStart != 128 {
					t.Fatal("prefix is not separate")
				}
				for i := 2; i < len(r.table); i++ {
					want := uint64(historyCompressChunkSize)
					if i == 2 {
						want -= historyHeaderPrefixBytes
					}
					if r.table[i].uncompressedStart-r.table[i-1].uncompressedStart != want {
						t.Fatal("body chunk size changed")
					}
				}
				got := make([]byte, size)
				if _, err := r.ReadAt(got, 0); err != nil && !(size == 0 && err == io.EOF) {
					t.Fatal(err)
				}
				if !bytes.Equal(got, raw) {
					t.Fatal("logical bytes/offsets changed")
				}
				for _, off := range []int{120, 127, 128, 131190} {
					if off+19 > size {
						continue
					}
					var part [19]byte
					if _, err := r.ReadAt(part[:], int64(off)); err != nil || !bytes.Equal(part[:], raw[off:off+19]) {
						t.Fatal("cross-block read", off, err)
					}
				}
			})
		}
	}
}

func TestHistorySmallPrefixWarmCacheCorruption(t *testing.T) {
	for _, format := range []string{"1", "2"} {
		for _, block := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/block%d", format, block), func(t *testing.T) {
				dir := t.TempDir()
				w, err := newHistoryCompressedStreamFormat(context.Background(), dir, historyCompressChunkSize, 1, format)
				if err != nil {
					t.Fatal(err)
				}
				defer w.Abort()
				raw := make([]byte, 2*historyCompressChunkSize)
				rand.New(rand.NewSource(181)).Read(raw)
				if _, err := w.Write(raw); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(dir, "corrupt.seg")
				if err := w.Finish(path); err != nil {
					t.Fatal(err)
				}
				cache := &historyHeaderCache{limit: 2}
				r, err := openCompressedBlockReader(path)
				if err != nil {
					t.Fatal(err)
				}
				r.prefixCache = cache
				var prefix [76]byte
				if _, err := r.ReadAt(prefix[:], 0); err != nil {
					t.Fatal(err)
				}
				entry := r.table[block]
				physical := r.dataOff + entry.compressedStart + entry.compressedLen - 1
				r.Close()
				info, _ := os.Stat(path)
				f, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				var one [1]byte
				if _, err := f.ReadAt(one[:], int64(physical)); err != nil {
					t.Fatal(err)
				}
				one[0] ^= 0x80
				if _, err := f.WriteAt(one[:], int64(physical)); err != nil {
					t.Fatal(err)
				}
				f.Close()
				if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
					t.Fatal(err)
				}
				r, err = openCompressedBlockReader(path)
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				r.prefixCache = cache
				if block == 1 {
					if _, err := r.ReadAt(prefix[:], 0); err != nil {
						t.Fatal("separate valid prefix unreadable", err)
					}
				}
				var data [16]byte
				if _, err := r.ReadAt(data[:], int64(entry.uncompressedStart)); err == nil {
					t.Fatal("corruption accepted after warm prefix cache")
				}
			})
		}
	}
}

func TestHistorySmallPrefixReset(t *testing.T) {
	dir := t.TempDir()
	w, err := newHistoryCompressedStreamFormat(context.Background(), dir, historyCompressChunkSize, 1, "2")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	if _, err := w.Write(bytes.Repeat([]byte{5}, 2*historyCompressChunkSize)); err != nil {
		t.Fatal(err)
	}
	if err := w.Reset(); err != nil {
		t.Fatal(err)
	}
	raw := bytes.Repeat([]byte{7}, 260)
	if _, err := w.Write(raw); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "reset.seg")
	if err := w.Finish(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decompressBlockBlob(data)
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatal("reset leaked prior prefix/body", err)
	}
}

func TestCompressedBlockOpenKeepsInspectedDescriptor(t *testing.T) {
	for _, format := range []string{"1", "2", "3"} {
		t.Run(format, func(t *testing.T) {
			dir := t.TempDir()
			w, err := newHistoryCompressedStreamFormat(context.Background(), dir, historyCompressChunkSize, 1, format)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			raw := bytes.Repeat([]byte("same-opened-artifact"), 9000)
			if _, err := w.Write(raw); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "original.seg")
			if err := w.Finish(path); err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			info, err := f.Stat()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Seek(17, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(path, path+".retired"); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("different inode"), 0600); err != nil {
				t.Fatal(err)
			}
			r, err := openCompressedBlockFileWithCacheLimitContext(context.Background(), f, info, 1)
			if err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(raw))
			if _, err := r.ReadAt(got, 0); err != nil || !bytes.Equal(got, raw) {
				t.Fatal("reopened path instead of original descriptor", err)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := f.ReadAt(got[:1], 0); !errors.Is(err, os.ErrClosed) {
				t.Fatal("descriptor ownership leaked", err)
			}
			bad, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			badInfo, _ := bad.Stat()
			if _, err := openCompressedBlockFileWithCacheLimitContext(context.Background(), bad, badInfo, 0); err == nil {
				t.Fatal("invalid cache accepted")
			}
			if _, err := bad.ReadAt(got[:1], 0); !errors.Is(err, os.ErrClosed) {
				t.Fatal("failed opener leaked descriptor", err)
			}
		})
	}
}

func TestHistorySmallPrefixPreservesLaterCompressedBlocks(t *testing.T) {
	raw := make([]byte, 3<<20+173)
	rand.New(rand.NewSource(7701)).Read(raw)
	for _, format := range []string{"1", "2"} {
		for _, workers := range []int{1, 4} {
			t.Run(fmt.Sprintf("%s/workers%d", format, workers), func(t *testing.T) {
				dir := t.TempDir()
				old, err := newCompressedBlockStreamWriterWithFooter(dir, historyCompressChunkSize, workers, format == "2")
				if err != nil {
					t.Fatal(err)
				}
				defer old.Abort()
				fresh, err := newHistoryCompressedStreamFormat(context.Background(), dir, historyCompressChunkSize, workers, format)
				if err != nil {
					t.Fatal(err)
				}
				defer fresh.Abort()
				for off := 0; off < len(raw); {
					end := min(len(raw), off+181103)
					if _, err := old.Write(raw[off:end]); err != nil {
						t.Fatal(err)
					}
					if _, err := fresh.Write(raw[off:end]); err != nil {
						t.Fatal(err)
					}
					off = end
				}
				oldPath, newPath := filepath.Join(dir, "old.seg"), filepath.Join(dir, "new.seg")
				if err := old.Finish(oldPath); err != nil {
					t.Fatal(err)
				}
				if err := fresh.Finish(newPath); err != nil {
					t.Fatal(err)
				}
				a, err := openCompressedBlockReader(oldPath)
				if err != nil {
					t.Fatal(err)
				}
				defer a.Close()
				b, err := openCompressedBlockReader(newPath)
				if err != nil {
					t.Fatal(err)
				}
				defer b.Close()
				if len(b.table) != len(a.table)+1 {
					t.Fatal("unexpected split count")
				}
				for i := 1; i < len(a.table); i++ {
					x, n, err := a.readCompressedBlock(i)
					if err != nil {
						t.Fatal(err)
					}
					y, m, err := b.readCompressedBlock(i + 1)
					if err != nil {
						t.Fatal(err)
					}
					if a.table[i].uncompressedStart != b.table[i+1].uncompressedStart || n != m || !bytes.Equal(x, y) {
						t.Fatal("later compressed block or logical offset changed", i)
					}
				}
			})
		}
	}
}

func BenchmarkHistorySmallPrefixHeaderReopen(b *testing.B) {
	raw := make([]byte, 8*historyCompressChunkSize)
	rand.New(rand.NewSource(541)).Read(raw)
	for _, small := range []bool{false, true} {
		b.Run(fmt.Sprint(small), func(b *testing.B) {
			dir := b.TempDir()
			var w historyCompressedStream
			var err error
			if small {
				w, err = newHistoryCompressedStreamFormat(context.Background(), dir, historyCompressChunkSize, 1, "2")
			} else {
				w, err = newCompressedBlockStreamWriterWithFooter(dir, historyCompressChunkSize, 1, true)
			}
			if err != nil {
				b.Fatal(err)
			}
			defer w.Abort()
			if _, err := w.Write(raw); err != nil {
				b.Fatal(err)
			}
			path := filepath.Join(dir, "header.seg")
			if err := w.Finish(path); err != nil {
				b.Fatal(err)
			}
			cache := &historyHeaderCache{limit: 2}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, err := openCompressedBlockReader(path)
				if err != nil {
					b.Fatal(err)
				}
				r.prefixCache = cache
				var header [76]byte
				if _, err := r.ReadAt(header[:], 0); err != nil {
					b.Fatal(err)
				}
				if err := r.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

package sstable

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/golang/snappy"
)

type decompressionErrorReadable struct {
	data  []byte
	calls int
}

func (r *decompressionErrorReadable) ReadAt(_ context.Context, p []byte, off int64) error {
	r.calls++
	if off < 0 || off+int64(len(p)) > int64(len(r.data)) {
		return io.ErrUnexpectedEOF
	}
	copy(p, r.data[off:])
	return nil
}
func (r *decompressionErrorReadable) Close() error { return nil }
func (r *decompressionErrorReadable) Size() int64  { return int64(len(r.data)) }
func (r *decompressionErrorReadable) NewReadHandle(context.Context) objstorage.ReadHandle {
	return &decompressionErrorReadHandle{r}
}

type decompressionErrorReadHandle struct{ r *decompressionErrorReadable }

func (h *decompressionErrorReadHandle) ReadAt(c context.Context, p []byte, o int64) error {
	return h.r.ReadAt(c, p, o)
}
func (h *decompressionErrorReadHandle) Close() error                                 { return nil }
func (h *decompressionErrorReadHandle) SetupForCompaction()                          {}
func (h *decompressionErrorReadHandle) RecordCacheHit(context.Context, int64, int64) {}
func newDecompressionErrorReader(t *testing.T) (*Reader, *cache.Cache, BlockHandle, *decompressionErrorReadable) {
	t.Helper()
	// Valid decoded-length varint4096; truncated COPY_4 body is invalid Snappy.
	body := []byte{0x80, 0x20, 0xff}
	n, _, e := decompressedLen(snappyCompressionBlockType, body)
	if e != nil || n != 4096 {
		t.Fatal("fixture does not reach decoded allocation", n, e)
	}
	if _, e = decompressInto(snappyCompressionBlockType, body, make([]byte, n)); e == nil {
		t.Fatal("fixture unexpectedly decompresses")
	}
	data := append(append([]byte(nil), body...), byte(snappyCompressionBlockType))
	var sum [4]byte
	check := checksummer{checksumType: ChecksumTypeCRC32c}
	binary.LittleEndian.PutUint32(sum[:], check.checksum(body, data[len(body):]))
	data = append(data, sum[:]...)
	bh := BlockHandle{Offset: 0, Length: uint64(len(body))}
	if e = checkChecksum(ChecksumTypeCRC32c, data, bh, base.FileNum(1)); e != nil {
		t.Fatal("fixture must pass checksum", e)
	}
	c := cache.New(1 << 20)
	source := &decompressionErrorReadable{data: data}
	r := &Reader{opts: (ReaderOptions{Cache: c}).ensureDefaults(), cacheID: c.NewID(), fileNum: base.FileNum(1).DiskFileNum(), checksumType: ChecksumTypeCRC32c, readable: source}
	return r, c, bh, source
}
func TestReadBlockDecompressionErrorReleasesPool(t *testing.T) {
	for _, pooled := range []bool{false, true} {
		t.Run(fmt.Sprint(pooled), func(t *testing.T) {
			r, c, bh, source := newDecompressionErrorReader(t)
			defer c.Unref()
			var pool BufferPool
			var p *BufferPool
			if pooled {
				pool.Init(2)
				p = &pool
			}
			for i := 0; i < 8; i++ {
				h, e := r.readBlock(context.Background(), bh, nil, nil, nil, p)
				if e == nil || !errors.Is(e, base.ErrCorruption) || h.Get() != nil {
					t.Fatal("decode error must propagate without a value", e)
				}
				if m := c.Metrics(); m.Count != 0 || m.Size != 0 {
					t.Fatal("decode failure entered cache", m)
				}
			}
			if source.calls != 8 {
				t.Fatal("decode error was cached/suppressed", source.calls)
			}
			if pooled {
				inUse := 0
				for i := range pool.pool {
					if pool.pool[i].b != nil {
						inUse++
						pool.pool[i].b = nil
					}
				}
				pool.Release()
				if inUse != 0 {
					t.Fatalf("decompression error retained %d in-use pooled buffers", inUse)
				}
			}
		})
	}
}

// replaceDecompressionFixtureBody preserves the real block checksum path.
func replaceDecompressionFixtureBody(source *decompressionErrorReadable, body []byte) BlockHandle {
	data := append(append([]byte(nil), body...), byte(snappyCompressionBlockType))
	var sum [4]byte
	check := checksummer{checksumType: ChecksumTypeCRC32c}
	binary.LittleEndian.PutUint32(sum[:], check.checksum(body, data[len(body):]))
	source.data = append(data, sum[:]...)
	return BlockHandle{Offset: 0, Length: uint64(len(body))}
}

func TestReadBlockDecompressionHealthyAndEarlyErrors(t *testing.T) {
	for _, pooled := range []bool{false, true} {
		for _, kind := range []string{"healthy", "checksum", "length"} {
			t.Run(fmt.Sprintf("%t/%s", pooled, kind), func(t *testing.T) {
				r, c, _, source := newDecompressionErrorReader(t)
				defer c.Unref()
				value := bytes.Repeat([]byte("valid decoded block"), 257)
				body := snappy.Encode(nil, value)
				if kind == "length" {
					body = []byte{0x80}
				}
				bh := replaceDecompressionFixtureBody(source, body)
				if kind == "checksum" {
					source.data[len(source.data)-1] ^= 1
				}
				var pool BufferPool
				var p *BufferPool
				if pooled {
					pool.Init(2)
					p = &pool
				}
				for i := 0; i < 2; i++ {
					h, err := r.readBlock(context.Background(), bh, nil, nil, nil, p)
					if kind == "healthy" {
						if err != nil || !bytes.Equal(h.Get(), value) {
							t.Fatalf("healthy decode: %v", err)
						}
						h.Release()
					} else if err == nil || h.Get() != nil {
						t.Fatalf("%s must fail before decoded allocation: %v", kind, err)
					}
				}
				expectedReads := 2
				if kind == "healthy" && !pooled {
					expectedReads = 1
				}
				if source.calls != expectedReads {
					t.Fatalf("actual reads %d, want %d", source.calls, expectedReads)
				}
				m := c.Metrics()
				if kind == "healthy" && !pooled {
					if m.Count != 1 || m.Size != int64(len(value)) {
						t.Fatalf("healthy cache: %+v", m)
					}
				} else if m.Count != 0 || m.Size != 0 {
					t.Fatalf("unexpected cache admission: %+v", m)
				}
				if pooled {
					for i := range pool.pool {
						if pool.pool[i].b != nil {
							t.Fatal("released path left a pooled buffer in use")
						}
					}
					pool.Release()
				}
			})
		}
	}
}

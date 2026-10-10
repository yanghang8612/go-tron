package snapshots

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
)

type referencePayloadCounter struct {
	io.ReaderAt
	end   uint64
	reads int
}

func (r *referencePayloadCounter) ReadAt(p []byte, off int64) (int, error) {
	if off >= historyReferenceHeaderSize && uint64(off) < r.end {
		r.reads++
	}
	return r.ReaderAt.ReadAt(p, off)
}

// Use real writer-produced literal/value spans. Count physical payload reads,
// excluding authenticated directories, rather than timing a cached filesystem.
func TestHistoryCompactionRecordCacheAlternatingChunks(t *testing.T) {
	data, want, _ := referenceContainerFixture(t, 0)
	read := func(limit int) (int, uint64) {
		t.Helper()
		counter := &referencePayloadCounter{ReaderAt: bytes.NewReader(data)}
		r, err := newHistoryReferenceReader(context.Background(), counter, uint64(len(data)), nil, limit)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		counter.end = r.header.chunkDir
		var got []byte
		offset := int64(0)
		part := func(n int) {
			t.Helper()
			b := make([]byte, n)
			if _, err := r.ReadAt(b, offset); err != nil {
				t.Fatal(err)
			}
			got = append(got, b...)
			offset += int64(n)
			if r.cacheBytes > limit || len(r.cache) > historyReferenceMaxCacheEntries {
				t.Fatal("cache exceeded bound")
			}
		}
		part(64)
		for i := 0; i < 300; i++ {
			part(len(fmt.Sprintf("row %05d:", i)))
			part(1000 + i*3)
		}
		if !bytes.Equal(got, want) {
			t.Fatal("record/value stream changed")
		}
		return counter.reads, r.header.chunks
	}
	oldReads, _ := read(historyReferenceMaxChunk)
	newReads, chunks := read(cbCacheBlocks * historyReferenceMaxChunk)
	if oldReads < 300 || newReads != int(chunks) {
		t.Fatalf("payload reads old=%d new=%d unique chunks=%d", oldReads, newReads, chunks)
	}
	t.Logf("payload reads: 128KiB=%d, 2MiB=%d, unique chunks=%d", oldReads, newReads, chunks)
}

func TestHistoryCompactionRecordCacheScopeAndCancellation(t *testing.T) {
	dir, _, selection, _ := referenceMergeFixture(t, "codec2")
	for i, candidate := range selection.candidates {
		ctx, cancel := context.WithCancel(context.Background())
		r, _, _, err := openStateDomainChangeBinaryCompactionRecordReader(ctx, dir, candidate.history)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		underlying := r.(*stateDomainChangeHistoryReader).historySegmentReader
		if i == 0 {
			ref := underlying.(*historyReferenceReader)
			if ref.cacheLimit != cbCacheBlocks*historyReferenceMaxChunk {
				t.Fatalf("R1 record cache %d", ref.cacheLimit)
			}
		} else if cr := underlying.(*compressedBlockReader); cr.cacheLimit != 1 {
			t.Fatalf("ordinary compressed cache %d", cr.cacheLimit)
		}
		var header [8]byte
		if _, err := r.ReadAt(header[:], 0); err != nil {
			t.Fatal(err)
		}
		cancel()
		if _, err := r.ReadAt(header[:], 0); i == 0 && !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled warm read: %v", err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			ref := underlying.(*historyReferenceReader)
			if ref.cacheBytes != 0 || len(ref.cache) != 0 {
				t.Fatal("close retained payloads")
			}
		}
		// The independent sequential range stream keeps its original budget.
		ranges, _, _, err := openStateDomainChangeBinarySegmentSequentialReader(dir, candidate.history)
		if err != nil {
			t.Fatal(err)
		}
		underlying = ranges.(*stateDomainChangeHistoryReader).historySegmentReader
		if i == 0 && underlying.(*historyReferenceReader).cacheLimit != historyReferenceMaxChunk || i == 1 && underlying.(*compressedBlockReader).cacheLimit != 1 {
			t.Fatal("range reader budget changed")
		}
		if err := ranges.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

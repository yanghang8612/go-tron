package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/golang/snappy"
)

// HistoryReferenceContainerStats describes format bytes, not allocator/RSS
// usage. Chunks includes literal pages; StoredChunks counts deduplicated source
// chunks supplied through StoreChunk. No process-global metric is registered.
type HistoryReferenceContainerStats struct {
	LogicalBytes     uint64 `json:"logical_bytes"`
	PhysicalBytes    uint64 `json:"physical_bytes"`
	Chunks           uint64 `json:"chunks"`
	UniqueChunkBytes uint64 `json:"unique_chunk_bytes"`
	StoredChunks     uint64 `json:"stored_chunks"`
	StoredChunkBytes uint64 `json:"stored_chunk_bytes"`
	Spans            uint64 `json:"spans"`
}

type historyReferenceChunkKey struct {
	digest [sha256.Size]byte
	length uint32
}

// historyReferenceWriter has one owner. Its scratch files are private and
// removed by Release; only Finalize's self-contained output survives. Both
// directories are files, not slices. The dedup map is bounded by MaxChunks and
// the combined directory budget; those limits are not an exact RSS claim.
type historyReferenceWriter struct {
	ctx                context.Context
	dir                string
	raw, chunks, spans *os.File
	dictionary         map[historyReferenceChunkKey]uint32
	prefixLimit        uint64
	rawBytes           uint64
	stats              HistoryReferenceContainerStats
	last               historyReferenceSpan
	literal            []byte
	literalID          uint32
	hasLiteral         bool
	finished, released bool
	err                error
}

func newHistoryReferenceWriter(ctx context.Context, dir string, prefixLimit int) (*historyReferenceWriter, error) {
	if prefixLimit < 0 || prefixLimit > historyReferenceMaxPrefix {
		return nil, fmt.Errorf("%w: prefix", errHistoryReferenceBudget)
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tmp, err := os.MkdirTemp(dir, ".history-reference-")
	if err != nil {
		return nil, err
	}
	w := &historyReferenceWriter{ctx: ctx, dir: tmp, prefixLimit: uint64(prefixLimit), dictionary: make(map[historyReferenceChunkKey]uint32)}
	for _, item := range []struct {
		name   string
		target **os.File
	}{{"chunks.raw", &w.raw}, {"chunks.dir", &w.chunks}, {"spans.dir", &w.spans}} {
		*item.target, err = os.OpenFile(filepath.Join(tmp, item.name), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err != nil {
			return nil, errors.Join(err, w.Release())
		}
	}
	return w, nil
}

func (w *historyReferenceWriter) Stats() HistoryReferenceContainerStats {
	if w == nil {
		return HistoryReferenceContainerStats{}
	}
	return w.stats
}
func (w *historyReferenceWriter) LogicalSize() uint64 { return w.Stats().LogicalBytes }

func (w *historyReferenceWriter) check() error {
	if w == nil || w.released || w.finished {
		return errHistoryReferenceClosed
	}
	if w.err != nil {
		return w.err
	}
	return contextError(w.ctx)
}

func (w *historyReferenceWriter) fail(err error) error {
	if err != nil && w.err == nil {
		w.err = err
	}
	return err
}

func (w *historyReferenceWriter) budget(chunks, spans, raw uint64) error {
	if chunks > historyReferenceMaxChunks || spans > historyReferenceMaxSpans ||
		chunks*historyReferenceChunkEntrySize+spans*historyReferenceSpanEntrySize > historyReferenceMaxMetadata ||
		raw > historyReferenceMaxPhysical-historyReferenceMaxMetadata-historyReferenceHeaderSize {
		return errHistoryReferenceBudget
	}
	return nil
}

func (w *historyReferenceWriter) chunk(id uint32) (historyReferenceChunk, error) {
	if uint64(id) >= w.stats.Chunks {
		return historyReferenceChunk{}, fmt.Errorf("%w: chunk ID", errHistoryReferenceCorrupt)
	}
	var raw [historyReferenceChunkEntrySize]byte
	if err := historyReferenceReadAt(w.ctx, w.chunks, raw[:], uint64(id)*historyReferenceChunkEntrySize); err != nil {
		return historyReferenceChunk{}, err
	}
	return decodeHistoryReferenceChunk(raw[:])
}

func (w *historyReferenceWriter) setChunk(id uint32, chunk historyReferenceChunk) error {
	b := chunk.encode()
	n, err := w.chunks.WriteAt(b[:], int64(id)*historyReferenceChunkEntrySize)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	return err
}

func (w *historyReferenceWriter) reserveChunk() (uint32, error) {
	if err := w.budget(w.stats.Chunks+1, w.stats.Spans, w.rawBytes); err != nil {
		return 0, err
	}
	id := uint32(w.stats.Chunks)
	if err := w.setChunk(id, historyReferenceChunk{}); err != nil {
		return 0, err
	}
	w.stats.Chunks++
	return id, nil
}

func (w *historyReferenceWriter) store(id uint32, p []byte, digest [sha256.Size]byte, literal bool) error {
	if err := w.budget(w.stats.Chunks, w.stats.Spans, w.rawBytes+uint64(len(p))); err != nil {
		return err
	}
	chunk := historyReferenceChunk{offset: w.rawBytes, raw: uint32(len(p)), stored: uint32(len(p)), digest: digest}
	if literal {
		chunk.codec = 2
	} // scratch-only marker; never allowed on disk.
	if err := historyReferenceWrite(w.raw, p); err != nil {
		return err
	}
	if err := w.setChunk(id, chunk); err != nil {
		return err
	}
	w.rawBytes += uint64(len(p))
	w.stats.UniqueChunkBytes += uint64(len(p))
	return nil
}

func (w *historyReferenceWriter) StoreChunk(p []byte) (uint32, error) {
	if err := w.check(); err != nil {
		return 0, err
	}
	if len(p) == 0 || len(p) > historyReferenceMaxChunk {
		return 0, w.fail(fmt.Errorf("%w: chunk length", errHistoryReferenceBudget))
	}
	return w.StoreChunkWithDigest(p, sha256.Sum256(p))
}

// StoreChunkWithDigest treats the supplied digest only as a candidate key. A
// miss verifies SHA; a hit compares all already-owned bytes before reuse. No
// callback buffer or supplied digest becomes an unauthenticated dependency.
func (w *historyReferenceWriter) StoreChunkWithDigest(p []byte, digest [sha256.Size]byte) (uint32, error) {
	if err := w.check(); err != nil {
		return 0, err
	}
	if len(p) == 0 || len(p) > historyReferenceMaxChunk {
		return 0, w.fail(fmt.Errorf("%w: chunk length", errHistoryReferenceBudget))
	}
	key := historyReferenceChunkKey{digest, uint32(len(p))}
	if id, ok := w.dictionary[key]; ok {
		chunk, err := w.chunk(id)
		if err != nil {
			return 0, w.fail(err)
		}
		scratch := make([]byte, len(p))
		if err := historyReferenceReadAt(w.ctx, w.raw, scratch, chunk.offset); err != nil {
			return 0, w.fail(err)
		}
		if !bytes.Equal(p, scratch) {
			return 0, w.fail(fmt.Errorf("%w: digest candidate bytes differ", errHistoryReferenceCorrupt))
		}
		return id, nil
	}
	if sha256.Sum256(p) != digest {
		return 0, w.fail(fmt.Errorf("%w: supplied chunk digest", errHistoryReferenceCorrupt))
	}
	id, err := w.reserveChunk()
	if err == nil {
		err = w.store(id, p, digest, false)
	}
	if err != nil {
		return 0, w.fail(err)
	}
	w.dictionary[key] = id
	w.stats.StoredChunks++
	w.stats.StoredChunkBytes += uint64(len(p))
	return id, nil
}

func (w *historyReferenceWriter) appendSpan(id, off, length uint32) error {
	if length == 0 {
		return fmt.Errorf("%w: zero span", errHistoryReferenceCorrupt)
	}
	if uint64(length) > historyReferenceMaxLogical-w.stats.LogicalBytes {
		return errHistoryReferenceBudget
	}
	span := historyReferenceSpan{w.stats.LogicalBytes, id, off, length}
	merge := w.stats.Spans > 0 && w.last.chunk == id && uint64(w.last.offset)+uint64(w.last.length) == uint64(off) &&
		uint64(w.last.length)+uint64(length) <= historyReferenceMaxChunk
	index := w.stats.Spans
	if merge {
		span = w.last
		span.length += length
		index--
	} else if err := w.budget(w.stats.Chunks, index+1, w.rawBytes); err != nil {
		return err
	}
	b := span.encode()
	n, err := w.spans.WriteAt(b[:], int64(index)*historyReferenceSpanEntrySize)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return err
	}
	if !merge {
		w.stats.Spans++
	}
	w.last = span
	w.stats.LogicalBytes += uint64(length)
	return nil
}

func (w *historyReferenceWriter) WriteSpan(id, off, length uint32) error {
	if err := w.check(); err != nil {
		return err
	}
	chunk, err := w.chunk(id)
	if err == nil && (chunk.codec != 0 || length == 0 || off > chunk.raw || length > chunk.raw-off) {
		err = fmt.Errorf("%w: source span bounds", errHistoryReferenceCorrupt)
	}
	if err == nil {
		err = w.appendSpan(id, off, length)
	}
	return w.fail(err)
}

func (w *historyReferenceWriter) flushLiteral() error {
	if !w.hasLiteral {
		return nil
	}
	if err := w.store(w.literalID, w.literal, sha256.Sum256(w.literal), true); err != nil {
		return err
	}
	w.hasLiteral = false
	w.literal = w.literal[:0]
	return nil
}

func (w *historyReferenceWriter) Write(p []byte) (int, error) { return w.WriteLiteral(p) }

func (w *historyReferenceWriter) WriteLiteral(p []byte) (int, error) {
	if err := w.check(); err != nil {
		return 0, err
	}
	written := 0
	for len(p) != 0 {
		if err := w.check(); err != nil {
			return written, err
		}
		if !w.hasLiteral {
			id, err := w.reserveChunk()
			if err != nil {
				return written, w.fail(err)
			}
			w.literalID, w.hasLiteral = id, true
		}
		n := min(len(p), historyReferenceMaxChunk-len(w.literal))
		if err := w.appendSpan(w.literalID, uint32(len(w.literal)), uint32(n)); err != nil {
			return written, w.fail(err)
		}
		w.literal = append(w.literal, p[:n]...)
		written += n
		p = p[n:]
		if len(w.literal) == historyReferenceMaxChunk {
			if err := w.flushLiteral(); err != nil {
				return written, w.fail(err)
			}
		}
	}
	return written, nil
}

func (w *historyReferenceWriter) span(index uint64) (historyReferenceSpan, error) {
	var b [historyReferenceSpanEntrySize]byte
	if err := historyReferenceReadAt(w.ctx, w.spans, b[:], index*historyReferenceSpanEntrySize); err != nil {
		return historyReferenceSpan{}, err
	}
	return decodeHistoryReferenceSpan(b[:])
}

// WriteAt only patches already-written literal bytes in the retained logical
// prefix. Source chunks cannot be edited, including ones shared by many spans.
func (w *historyReferenceWriter) WriteAt(p []byte, off int64) (int, error) {
	if err := w.check(); err != nil {
		return 0, err
	}
	if off < 0 || uint64(off) > w.prefixLimit || uint64(len(p)) > w.prefixLimit-uint64(off) ||
		uint64(off) > w.stats.LogicalBytes || uint64(len(p)) > w.stats.LogicalBytes-uint64(off) {
		return 0, w.fail(fmt.Errorf("%w: WriteAt outside literal prefix", errHistoryReferenceBudget))
	}
	written := 0
	for len(p) > 0 {
		lo, hi := uint64(0), w.stats.Spans
		for lo < hi {
			mid := lo + (hi-lo)/2
			span, err := w.span(mid)
			if err != nil {
				return written, w.fail(err)
			}
			if span.logical+uint64(span.length) <= uint64(off) {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		span, err := w.span(lo)
		if err != nil {
			return written, w.fail(err)
		}
		delta := uint32(uint64(off) - span.logical)
		n := min(len(p), int(span.length-delta))
		pos := span.offset + delta
		if w.hasLiteral && span.chunk == w.literalID {
			copy(w.literal[pos:pos+uint32(n)], p[:n])
		} else {
			chunk, err := w.chunk(span.chunk)
			if err != nil {
				return written, w.fail(err)
			}
			if chunk.codec != 2 {
				return written, w.fail(fmt.Errorf("%w: WriteAt targets source chunk", errHistoryReferenceCorrupt))
			}
			buf := make([]byte, chunk.raw)
			if err := historyReferenceReadAt(w.ctx, w.raw, buf, chunk.offset); err != nil {
				return written, w.fail(err)
			}
			if sha256.Sum256(buf) != chunk.digest {
				return written, w.fail(fmt.Errorf("%w: literal scratch checksum", errHistoryReferenceCorrupt))
			}
			copy(buf[pos:pos+uint32(n)], p[:n])
			chunk.digest = sha256.Sum256(buf)
			count, err := w.raw.WriteAt(buf, int64(chunk.offset))
			if err == nil && count != len(buf) {
				err = io.ErrShortWrite
			}
			if err == nil {
				err = w.setChunk(span.chunk, chunk)
			}
			if err != nil {
				return written, w.fail(err)
			}
		}
		written += n
		off += int64(n)
		p = p[n:]
	}
	return written, nil
}

// Finalize creates a new file, syncs it and checks its full virtual layout and
// all chunk checksums. It does not publish a manifest or rename an existing
// file. Failure removes only the new output it created. The returned checksum
// includes every physical byte and uses the standard "sha256:" prefix.
func (w *historyReferenceWriter) Finalize(path string) (size uint64, checksum string, err error) {
	if err = w.check(); err != nil {
		return 0, "", err
	}
	if err = w.flushLiteral(); err != nil {
		return 0, "", w.fail(err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return 0, "", w.fail(err)
	}
	defer func() {
		err = errors.Join(err, file.Close())
		if err != nil {
			err = errors.Join(err, os.Remove(path))
			w.fail(err)
		}
	}()
	if err = historyReferenceWrite(file, make([]byte, historyReferenceHeaderSize)); err != nil {
		return 0, "", err
	}
	physical := uint64(historyReferenceHeaderSize)
	for id := uint32(0); uint64(id) < w.stats.Chunks; id++ {
		chunk, e := w.chunk(id)
		if e != nil {
			return 0, "", e
		}
		if chunk.raw == 0 || chunk.raw > historyReferenceMaxChunk {
			return 0, "", fmt.Errorf("%w: incomplete scratch chunk", errHistoryReferenceCorrupt)
		}
		raw := make([]byte, chunk.raw)
		if e = historyReferenceReadAt(w.ctx, w.raw, raw, chunk.offset); e != nil {
			return 0, "", e
		}
		if sha256.Sum256(raw) != chunk.digest {
			return 0, "", fmt.Errorf("%w: scratch chunk digest", errHistoryReferenceCorrupt)
		}
		stored := snappy.Encode(nil, raw)
		chunk.codec = 1
		if len(stored) >= len(raw) {
			stored, chunk.codec = raw, 0
		}
		chunk.offset, chunk.stored = physical, uint32(len(stored))
		if e = historyReferenceWrite(file, stored); e != nil {
			return 0, "", e
		}
		if e = w.setChunk(id, chunk); e != nil {
			return 0, "", e
		}
		physical += uint64(len(stored))
	}
	header := historyReferenceHeader{logical: w.stats.LogicalBytes, chunkDir: physical, chunks: w.stats.Chunks, spans: w.stats.Spans}
	header.spanDir = physical + w.stats.Chunks*historyReferenceChunkEntrySize
	header.physical = header.spanDir + w.stats.Spans*historyReferenceSpanEntrySize
	for _, entry := range []struct {
		file   *os.File
		length uint64
	}{{w.chunks, w.stats.Chunks * historyReferenceChunkEntrySize}, {w.spans, w.stats.Spans * historyReferenceSpanEntrySize}} {
		var scratch [64 << 10]byte
		for off := uint64(0); off < entry.length; {
			n := min(uint64(len(scratch)), entry.length-off)
			if err = historyReferenceReadAt(w.ctx, entry.file, scratch[:n], off); err != nil {
				return 0, "", err
			}
			if err = historyReferenceWrite(file, scratch[:n]); err != nil {
				return 0, "", err
			}
			off += n
		}
	}
	if header.metadata, err = historyReferenceMetadataHash(w.ctx, file, header); err != nil {
		return 0, "", err
	}
	b := header.encode()
	count, err := file.WriteAt(b[:], 0)
	if err == nil && count != len(b) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return 0, "", err
	}
	if err = file.Sync(); err != nil {
		return 0, "", err
	}
	reader, err := openHistoryReferenceReader(w.ctx, path, 0)
	if err != nil {
		return 0, "", err
	}
	err = errors.Join(reader.ValidateAll(w.ctx), reader.Close())
	if err != nil {
		return 0, "", err
	}
	hash := sha256.New()
	var scratch [64 << 10]byte
	for off := uint64(0); off < header.physical; {
		n := min(uint64(len(scratch)), header.physical-off)
		if err = historyReferenceReadAt(w.ctx, file, scratch[:n], off); err != nil {
			return 0, "", err
		}
		_, _ = hash.Write(scratch[:n])
		off += n
	}
	w.finished = true
	w.stats.PhysicalBytes = header.physical
	return header.physical, fmt.Sprintf("sha256:%x", hash.Sum(nil)), nil
}

func (w *historyReferenceWriter) Release() error {
	if w == nil || w.released {
		return nil
	}
	w.released = true
	var err error
	for _, file := range []*os.File{w.raw, w.chunks, w.spans} {
		if file != nil {
			err = errors.Join(err, file.Close())
		}
	}
	w.dictionary, w.literal = nil, nil
	return errors.Join(err, os.RemoveAll(w.dir))
}

package snapshots

// Frozen from e913c72d274d3f76f07b61f1d44ae1df986cbee9 before the digest reuse
// change: complete writer methods and writer-owned pipeline methods. Only the
// receiver/constructor and private type names were changed. The unchanged compression worker,
// metadata writer and reader are shared; candidate lookup and LRU behavior are
// independent of the production implementation under test.

import (
	"bufio"
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"

	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

type frozenCDCDictionaryEntry struct {
	digest  [sha256.Size]byte
	bytes   []byte
	entry   cdcEntry
	ordinal uint32
}

type frozenCDCPendingChunk struct {
	entry     cdcEntry
	anchor    *frozenCDCDictionaryEntry
	job       *cdcCompressionJob
	rawLength int
}

type frozenCDCWriteStats struct {
	InputBytes, AnchorBytes, ReusedBytes, Anchors, References uint64
	Pipeline                                                  cdcPipelineStats
}

type frozenCDCStreamWriter struct {
	ctx                      context.Context
	dir                      string
	prefixSize               int
	workers                  int
	pipeline                 *cdcCompressionPipeline
	pending                  []frozenCDCPendingChunk
	encodeChunk              cdcEncodeFunc // test fault injection; nil selects production EncodeAll
	written                  uint64
	failure                  error
	enc                      *zstd.Encoder
	body, table              *os.File
	bodyName, tableName      string
	bodyWriter, tableWriter  *bufio.Writer
	metadata                 *snapshotMetadataWriter
	first, chunk, encoded    []byte
	split                    historychunk.Splitter
	logical, physical, count uint64 // count excludes retained first chunk
	dictionary               map[[sha256.Size]byte]*list.Element
	lru                      list.List
	dictionaryBytes          int
	closed                   bool
	stats                    frozenCDCWriteStats
}

func newFrozenCDCStreamWriterWorkers(ctx context.Context, dir string, prefixSize, workers int) (*frozenCDCStreamWriter, error) {
	if workers < 1 {
		workers = 1
	}
	if workers > cdcMaxCompressionWorkers {
		return nil, fmt.Errorf("snapshots: CDC workers %d exceeds maximum %d", workers, cdcMaxCompressionWorkers)
	}

	if prefixSize <= 0 || prefixSize > historychunk.MaxSize {
		return nil, errors.New("snapshots: CDC retained prefix must be in [1,128KiB]")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	enc, err := sharedCDCEncoder()
	if err != nil {
		return nil, err
	}
	w := &frozenCDCStreamWriter{ctx: ctx, dir: dir, prefixSize: prefixSize, workers: workers, enc: enc}
	if err := w.initialize(); err != nil {
		w.Abort()
		return nil, err
	}
	return w, nil
}

func (w *frozenCDCStreamWriter) initialize() (err error) {
	if err := contextError(w.ctx); err != nil {
		return err
	}
	w.body, err = os.CreateTemp(w.dir, ".history-cdc-body-*.tmp")
	if err != nil {
		return err
	}
	w.bodyName = w.body.Name()
	w.table, err = os.CreateTemp(w.dir, ".history-cdc-table-*.tmp")
	if err != nil {
		return err
	}
	w.tableName = w.table.Name()
	w.metadata = newSnapshotMetadataWriter(w.body)
	w.bodyWriter = bufio.NewWriterSize(w.metadata, 64<<10)
	w.tableWriter = bufio.NewWriterSize(w.table, 32<<10)
	w.first = make([]byte, 0, w.prefixSize)
	w.chunk = make([]byte, 0, historychunk.MaxSize)
	w.dictionary = make(map[[sha256.Size]byte]*list.Element)
	w.physical = compressedBlockHeaderSize
	var header [compressedBlockHeaderSize]byte
	copy(header[:8], compressedBlockMagic)
	binary.BigEndian.PutUint32(header[8:12], compressedBlockCDCVersion)
	binary.BigEndian.PutUint32(header[12:16], historychunk.MaxSize)
	_, err = w.bodyWriter.Write(header[:])
	return err
}

func (w *frozenCDCStreamWriter) Write(p []byte) (written int, err error) {
	if w == nil || w.closed || w.bodyWriter == nil {
		return 0, errors.New("snapshots: CDC writer closed")
	}
	if w.failure != nil {
		return 0, w.failure
	}
	defer func() {
		if err != nil {
			w.fail(err)
		}
	}()
	if err := contextError(w.ctx); err != nil {
		return 0, err
	}
	if uint64(len(p)) > math.MaxInt64-w.logical {
		return 0, errors.New("snapshots: CDC logical size overflows")
	}
	if len(w.first) < w.prefixSize {
		n := min(len(p), w.prefixSize-len(w.first))
		w.first = append(w.first, p[:n]...)
		w.logical += uint64(n)
		written += n
		p = p[n:]
	}
	for len(p) > 0 {
		if err := contextError(w.ctx); err != nil {
			return written, err
		}
		if w.workers > 1 && len(p) >= 1<<20 {
			// Bound candidate/cut scratch and cancellation latency independently
			// of the caller's value size. Cuts joins all stripe workers before
			// returning; only the single writer advances logical offsets.
			window := p[:min(len(p), cdcBulkInputBytes)]
			cuts := w.split.Cuts(window, w.workers)
			off := 0
			for _, end := range cuts {
				n := end - off
				w.chunk = append(w.chunk, window[off:end]...)
				w.logical += uint64(n)
				written += n
				off = end
				if err := w.flushChunk(); err != nil {
					return written, err
				}
			}
			n := len(window) - off
			w.chunk = append(w.chunk, window[off:]...)
			w.logical += uint64(n)
			written += n
			p = p[len(window):]
			continue
		}
		n, cut := w.split.Next(p)
		w.chunk = append(w.chunk, p[:n]...)
		w.logical += uint64(n)
		written += n
		p = p[n:]
		if cut {
			if err := w.flushChunk(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

func (w *frozenCDCStreamWriter) WriteAt(p []byte, off int64) (int, error) {
	if w == nil || w.closed || w.bodyWriter == nil {
		return 0, errors.New("snapshots: CDC writer closed")
	}
	if w.failure != nil {
		return 0, w.failure
	}
	if err := contextError(w.ctx); err != nil {
		w.fail(err)
		return 0, err
	}
	if off < 0 || off > int64(len(w.first)) || int64(len(p)) > int64(len(w.first))-off {
		// Preserve the recoverable invalid-offset contract, but no worker may
		// outlive an error return. Already accepted data remains a valid stream.
		if err := w.drainCDC(w.ctx); err != nil {
			w.fail(err)
			return 0, err
		}
		w.stopPipeline()
		return 0, errors.New("snapshots: CDC WriteAt outside retained prefix")
	}
	return copy(w.first[int(off):], p), nil
}

func (w *frozenCDCStreamWriter) flushChunk() error { return w.flushChunkContext(w.ctx) }

func (w *frozenCDCStreamWriter) flushChunkContext(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if len(w.chunk) == 0 {
		return nil
	}
	if err := contextError(w.ctx); err != nil {
		return err
	}
	if w.count+1 >= uint64(cdcAnchor) {
		return errors.New("snapshots: CDC chunk ordinal exceeds uint32")
	}
	if w.pipeline == nil && w.workers > 1 && w.logical >= minParallelHistoryCompressionBytes {
		if err := w.startPipeline(); err != nil {
			return err
		}
	}
	if w.pipeline != nil {
		return w.flushChunkParallel(ctx)
	}
	e := cdcEntry{logical: w.logical - uint64(len(w.chunk)), anchor: cdcAnchor}
	digest := sha256.Sum256(w.chunk)
	if candidate := w.dictionary[digest]; candidate != nil && bytes.Equal(candidate.Value.(*frozenCDCDictionaryEntry).bytes, w.chunk) {
		old := candidate.Value.(*frozenCDCDictionaryEntry)
		e.physical, e.stored, e.anchor = old.entry.physical, old.entry.stored, old.ordinal
		w.lru.MoveToFront(candidate)
		w.stats.References++
		w.stats.ReusedBytes += uint64(len(w.chunk))
	} else {
		w.encoded = w.enc.EncodeAll(w.chunk, w.encoded[:0])
		if len(w.encoded) > cdcMaxEncodedChunk {
			return errors.New("snapshots: CDC encoded chunk exceeds cap")
		}
		e.physical, e.stored = w.physical, uint64(len(w.encoded))
		if e.stored > math.MaxInt64-w.physical {
			return errors.New("snapshots: CDC physical size overflows")
		}
		if _, err := w.bodyWriter.Write(w.encoded); err != nil {
			return err
		}
		w.physical += e.stored
		w.remember(digest, e, uint32(w.count+1))
		w.stats.Anchors++
		w.stats.AnchorBytes += uint64(len(w.chunk))
	}
	var entry [cdcEntrySize]byte
	putCDCEntry(entry[:], e)
	if _, err := w.tableWriter.Write(entry[:]); err != nil {
		return err
	}
	w.count++
	w.written++
	w.chunk = w.chunk[:0]
	return nil
}

func (w *frozenCDCStreamWriter) flushChunkParallel(ctx context.Context) error {
	for len(w.pending) >= w.pipeline.limit {
		if err := w.drainCDCChunk(ctx); err != nil {
			return err
		}
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := w.pipeline.reserve(len(w.chunk)); err != nil {
		return err
	}
	e := cdcEntry{logical: w.logical - uint64(len(w.chunk)), anchor: cdcAnchor}
	digest := sha256.Sum256(w.chunk)
	item := frozenCDCPendingChunk{entry: e, rawLength: len(w.chunk)}
	if candidate := w.dictionary[digest]; candidate != nil && bytes.Equal(candidate.Value.(*frozenCDCDictionaryEntry).bytes, w.chunk) {
		item.anchor = candidate.Value.(*frozenCDCDictionaryEntry)
		item.entry.anchor = item.anchor.ordinal
		w.lru.MoveToFront(candidate)
		w.stats.References++
		w.stats.ReusedBytes += uint64(len(w.chunk))
	} else {
		item.anchor = w.remember(digest, e, uint32(w.count+1))
		job, err := w.pipeline.submit(item.anchor.bytes)
		if err != nil {
			return err
		}
		item.job = job
		w.stats.Anchors++
		w.stats.AnchorBytes += uint64(len(w.chunk))
	}
	w.pending = append(w.pending, item)
	w.count++
	w.chunk = w.chunk[:0]
	return nil
}

func (w *frozenCDCStreamWriter) remember(digest [sha256.Size]byte, entry cdcEntry, ordinal uint32) *frozenCDCDictionaryEntry {
	remove := func(element *list.Element) {
		old := element.Value.(*frozenCDCDictionaryEntry)
		delete(w.dictionary, old.digest)
		w.dictionaryBytes -= len(old.bytes)
		w.lru.Remove(element)
	}
	if old := w.dictionary[digest]; old != nil {
		remove(old)
	} // unequal hash candidate: never deduplicate it
	for w.dictionaryBytes+len(w.chunk) > cdcDictionaryBytes || w.lru.Len() >= cdcDictionaryEntries {
		remove(w.lru.Back())
	}
	value := &frozenCDCDictionaryEntry{digest: digest, bytes: append([]byte(nil), w.chunk...), entry: entry, ordinal: ordinal}
	w.dictionary[digest] = w.lru.PushFront(value)
	w.dictionaryBytes += len(value.bytes)
	return value
}

func (w *frozenCDCStreamWriter) Finish(path string) error {
	_, err := w.FinishWithMetadataContext(context.Background(), path)
	return err
}

func (w *frozenCDCStreamWriter) FinishWithMetadataContext(ctx context.Context, path string) (metadata snapshotFileMetadata, err error) {
	if w == nil || w.closed || w.body == nil {
		return metadata, errors.New("snapshots: CDC writer closed")
	}
	defer w.Abort()
	if w.failure != nil {
		return metadata, w.failure
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err = contextError(w.ctx); err != nil {
		return metadata, err
	}
	if err = contextError(ctx); err != nil {
		return metadata, err
	}
	if err = w.flushChunkContext(ctx); err != nil {
		return metadata, err
	}
	if len(w.first) == 0 {
		return metadata, errors.New("snapshots: empty CDC stream")
	}
	if err = w.drainCDC(ctx); err != nil {
		return metadata, err
	}
	w.stopPipeline() // all tasks joined before the retained first frame/finalization
	w.dictionary = nil
	w.lru.Init()
	w.dictionaryBytes = 0
	w.metadata.dst = contextWriter{ctx: ctx, w: w.body}
	w.encoded = w.enc.EncodeAll(w.first, w.encoded[:0])
	if len(w.encoded) > cdcMaxEncodedChunk || uint64(len(w.encoded)) > math.MaxInt64-w.physical {
		return metadata, errors.New("snapshots: CDC encoded prefix exceeds cap")
	}
	prefix := cdcEntry{physical: w.physical, stored: uint64(len(w.encoded)), anchor: cdcAnchor}
	if _, err = w.bodyWriter.Write(w.encoded); err != nil {
		return metadata, err
	}
	w.physical += uint64(len(w.encoded))
	if err = w.tableWriter.Flush(); err != nil {
		return metadata, err
	}
	count := w.count + 1
	pages := (count + cdcEntriesPerPage - 1) / cdcEntriesPerPage
	if pages > cdcMaxSparseBytes/8 {
		return metadata, errors.New("snapshots: CDC sparse directory cap exceeded")
	}
	tableOffset := w.physical
	var page []byte
	page = make([]byte, 0, cdcEntriesPerPage*cdcEntrySize+4)
	sparse := make([]byte, 0, pages*8+4)
	tableReader := bufio.NewReaderSize(io.NewSectionReader(w.table, 0, int64(w.count*cdcEntrySize)), 32<<10)
	for ordinal := uint64(0); ordinal < count; ordinal++ {
		if err = contextError(ctx); err != nil {
			return metadata, err
		}
		if err = contextError(w.ctx); err != nil {
			return metadata, err
		}
		var entry [cdcEntrySize]byte
		if ordinal == 0 {
			putCDCEntry(entry[:], prefix)
		} else if _, err = io.ReadFull(tableReader, entry[:]); err != nil {
			return metadata, err
		}
		if ordinal%cdcEntriesPerPage == 0 {
			sparse = append(sparse, entry[:8]...)
		}
		page = append(page, entry[:]...)
		if ordinal%cdcEntriesPerPage == cdcEntriesPerPage-1 || ordinal+1 == count {
			page = binary.BigEndian.AppendUint32(page, crc32.ChecksumIEEE(page))
			if _, err = w.bodyWriter.Write(page); err != nil {
				return metadata, err
			}
			w.physical += uint64(len(page))
			page = page[:0]
		}
	}
	tableLen := w.physical - tableOffset
	sparse = binary.BigEndian.AppendUint32(sparse, crc32.ChecksumIEEE(sparse))
	if _, err = w.bodyWriter.Write(sparse); err != nil {
		return metadata, err
	}
	var footer [cdcFooterSize]byte
	copy(footer[:8], compressedBlockCDCEndMagic)
	binary.BigEndian.PutUint64(footer[8:16], count)
	binary.BigEndian.PutUint64(footer[16:24], w.logical)
	binary.BigEndian.PutUint64(footer[24:32], tableOffset)
	binary.BigEndian.PutUint64(footer[32:40], tableLen)
	binary.BigEndian.PutUint32(footer[40:44], crc32.ChecksumIEEE(footer[:40]))
	if _, err = w.bodyWriter.Write(footer[:]); err != nil {
		return metadata, err
	}
	if err = w.bodyWriter.Flush(); err != nil {
		return metadata, err
	}
	if err = contextError(ctx); err != nil {
		return metadata, err
	}
	if err = w.body.Sync(); err != nil {
		return metadata, err
	}
	if err = w.body.Close(); err != nil {
		return metadata, err
	}
	w.body = nil
	if err = contextError(ctx); err != nil {
		return metadata, err
	}
	if err = os.Rename(w.bodyName, path); err != nil {
		return metadata, err
	}
	w.bodyName = "" // publication succeeded: Abort must never remove its target
	w.stats.InputBytes = w.logical
	w.stats.Anchors++
	w.stats.AnchorBytes += uint64(len(w.first))
	metadata = w.metadata.Metadata()
	// Successful artifact finishes only; this does not assert manifest publication.
	historyCDCFilesCounter.Inc(1)
	historyCDCInputCounter.Inc(int64(w.stats.InputBytes))
	historyCDCReusedCounter.Inc(int64(w.stats.ReusedBytes))
	historyCDCStoredCounter.Inc(int64(metadata.size))
	return metadata, nil
}

func (w *frozenCDCStreamWriter) Abort() {
	if w == nil {
		return
	}
	w.stopPipeline() // workers never outlive file/scratch ownership
	if w.body != nil {
		_ = w.body.Close()
		w.body = nil
	}
	if w.table != nil {
		_ = w.table.Close()
		w.table = nil
	}
	if w.bodyName != "" {
		_ = os.Remove(w.bodyName)
		w.bodyName = ""
	}
	if w.tableName != "" {
		_ = os.Remove(w.tableName)
		w.tableName = ""
	}
	w.enc = nil // shared EncodeAll pool; owned by the process
	w.first, w.chunk, w.encoded = nil, nil, nil
	w.dictionary = nil
	w.lru.Init()
	w.bodyWriter, w.tableWriter = nil, nil
	w.closed = true
}

func (w *frozenCDCStreamWriter) Reset() error {
	if w == nil || w.closed {
		return errors.New("snapshots: CDC writer closed")
	}
	ctx, dir, prefixSize, workers := w.ctx, w.dir, w.prefixSize, w.workers
	w.Abort()
	next, err := newFrozenCDCStreamWriterWorkers(ctx, dir, prefixSize, workers)
	if err != nil {
		return err
	}
	*w = *next
	return nil
}

func (w *frozenCDCStreamWriter) startPipeline() error {
	if w.enc.MaxEncodedSize(historychunk.MaxSize) > cdcMaxEncodedChunk {
		return errors.New("snapshots: CDC encoder maximum exceeds frame reservation")
	}
	encode := w.encodeChunk
	if encode == nil {
		enc := w.enc
		encode = func(ctx context.Context, raw, dst []byte) ([]byte, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			out := enc.EncodeAll(raw, dst)
			return out, ctx.Err()
		}
	}
	w.pipeline = newCDCCompressionPipeline(w.ctx, w.workers, encode)
	w.pending = make([]frozenCDCPendingChunk, 0, w.pipeline.limit)
	return nil
}

func (w *frozenCDCStreamWriter) drainCDCChunk(ctx context.Context) error {
	if len(w.pending) == 0 {
		return nil
	}
	item := w.pending[0]
	e := item.entry
	if item.job != nil {
		if item.anchor.ordinal != uint32(w.written+1) {
			return errors.New("snapshots: CDC anchor completion order differs from admitted ordinal")
		}
		encoded, err := w.pipeline.result(ctx, item.job)
		if err != nil {
			return err
		}
		if uint64(len(encoded)) > uint64(^uint64(0)>>1)-w.physical {
			return errors.New("snapshots: CDC physical size overflows")
		}
		e.physical, e.stored = w.physical, uint64(len(encoded))
		if _, err := w.bodyWriter.Write(encoded); err != nil {
			return err
		}
		w.physical += e.stored
		item.anchor.entry = e // only the ordered writer publishes a completed anchor
	} else {
		if err := contextError(ctx); err != nil {
			return err
		}
		if item.anchor.ordinal >= uint32(w.written+1) || item.anchor.entry.stored == 0 || item.anchor.entry.anchor != cdcAnchor {
			return errors.New("snapshots: CDC reference anchor has not completed in order")
		}
		e.physical, e.stored = item.anchor.entry.physical, item.anchor.entry.stored
	}
	var entry [cdcEntrySize]byte
	putCDCEntry(entry[:], e)
	if _, err := w.tableWriter.Write(entry[:]); err != nil {
		return err
	}
	w.written++
	w.pipeline.release(item.rawLength, item.job)
	copy(w.pending, w.pending[1:])
	w.pending[len(w.pending)-1] = frozenCDCPendingChunk{}
	w.pending = w.pending[:len(w.pending)-1]
	return nil
}

func (w *frozenCDCStreamWriter) drainCDC(ctx context.Context) error {
	for len(w.pending) > 0 {
		if err := w.drainCDCChunk(ctx); err != nil {
			return err
		}
	}
	if w.written != w.count {
		return errors.New("snapshots: CDC completed directory count differs from admitted chunks")
	}
	return nil
}

func (w *frozenCDCStreamWriter) stopPipeline() {
	if w.pipeline == nil {
		return
	}
	w.pipeline.stop()
	stats := w.pipeline.stats
	dst := &w.stats.Pipeline
	dst.Workers = max(dst.Workers, stats.Workers)
	dst.PeakPending = max(dst.PeakPending, stats.PeakPending)
	dst.PeakRawBytes = max(dst.PeakRawBytes, stats.PeakRawBytes)
	dst.PeakEncodedBufferBytes = max(dst.PeakEncodedBufferBytes, stats.PeakEncodedBufferBytes)
	dst.PeakReservedBytes = max(dst.PeakReservedBytes, stats.PeakReservedBytes)
	w.pipeline = nil
	w.pending = nil
}

func (w *frozenCDCStreamWriter) fail(err error) {
	if w.failure == nil {
		w.failure = err
	}
	w.stopPipeline()
}

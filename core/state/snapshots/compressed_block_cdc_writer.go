package snapshots

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
	"path/filepath"
	"sync"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

// Container V3 preserves the complete V6 logical byte stream. Its CDC chunks
// either own one complete zstd frame or name an earlier directory anchor with
// exactly the same bytes. References never reference references. The footer
// table is paged, checksummed, and located by a small checksummed sparse index.
const (
	compressedBlockCDCVersion  = uint32(3)
	compressedBlockCDCEndMagic = "gtcend03"
	cdcEntriesPerPage          = 1024
	cdcEntrySize               = 28
	cdcFooterSize              = 48
	cdcAnchor                  = uint32(math.MaxUint32)
	cdcDictionaryBytes         = 64 << 20
	cdcDictionaryEntries       = 8192
	cdcMaxSparseBytes          = 16 << 20
	cdcMaxEncodedChunk         = historychunk.MaxSize + 1024
)

type historyCompressedStream interface {
	io.Writer
	io.WriterAt
	Reset() error
	Abort()
	Finish(string) error
	FinishWithMetadataContext(context.Context, string) (snapshotFileMetadata, error)
}

func newHistoryCompressedStream(ctx context.Context, dir string, prefixSize, workers int) (historyCompressedStream, error) {
	return newHistoryCompressedStreamFormat(ctx, dir, prefixSize, workers, os.Getenv("GTRON_HISTORY_COMPRESSION_FORMAT"))
}

// Selection is per artifact. Never mutate the process environment while a
// concurrent builder/merge may be choosing its own format.
func newHistoryCompressedStreamFormat(ctx context.Context, dir string, prefixSize, workers int, format string) (historyCompressedStream, error) {
	switch format {
	case "3":
		return newCDCStreamWriterWorkers(ctx, dir, prefixSize, workers)
	case "2", "auto":
		return newCompressedBlockStreamWriterWithFooter(dir, prefixSize, workers, true)
	case "", "1":
		return newCompressedBlockStreamWriterWithFooter(dir, prefixSize, workers, false)
	default:
		return nil, fmt.Errorf("snapshots: invalid history compression format %q", format)
	}
}

var (
	cdcEncoderOnce          sync.Once
	cdcEncoder              *zstd.Encoder
	cdcEncoderErr           error
	historyCDCFilesCounter  = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"compression_cdc/files", nil)
	historyCDCInputCounter  = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"compression_cdc/input_bytes", nil)
	historyCDCReusedCounter = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"compression_cdc/reused_bytes", nil)
	historyCDCStoredCounter = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"compression_cdc/stored_bytes", nil)
)

func sharedCDCEncoder() (*zstd.Encoder, error) {
	cdcEncoderOnce.Do(func() {
		cdcEncoder, cdcEncoderErr = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(cdcMaxCompressionWorkers), zstd.WithWindowSize(historychunk.MaxSize), zstd.WithEncoderCRC(true))
	})
	return cdcEncoder, cdcEncoderErr
}

type cdcEntry struct {
	logical, physical, stored uint64
	anchor                    uint32
}

func putCDCEntry(p []byte, e cdcEntry) {
	binary.BigEndian.PutUint64(p[0:8], e.logical)
	binary.BigEndian.PutUint64(p[8:16], e.physical)
	binary.BigEndian.PutUint64(p[16:24], e.stored)
	binary.BigEndian.PutUint32(p[24:28], e.anchor)
}

func getCDCEntry(p []byte) cdcEntry {
	return cdcEntry{binary.BigEndian.Uint64(p[0:8]), binary.BigEndian.Uint64(p[8:16]), binary.BigEndian.Uint64(p[16:24]), binary.BigEndian.Uint32(p[24:28])}
}

type cdcDictionaryEntry struct {
	digest  [sha256.Size]byte
	bytes   []byte
	entry   cdcEntry
	ordinal uint32
}

type cdcStreamWriter struct {
	ctx                      context.Context
	dir                      string
	prefixSize               int
	workers                  int
	pipeline                 *cdcCompressionPipeline
	pending                  []cdcPendingChunk
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
	stats                    cdcWriteStats
}

type cdcWriteStats struct {
	InputBytes, AnchorBytes, ReusedBytes, Anchors, References uint64
	Pipeline                                                  cdcPipelineStats
}

func newCDCStreamWriter(ctx context.Context, dir string, prefixSize int) (*cdcStreamWriter, error) {
	return newCDCStreamWriterWorkers(ctx, dir, prefixSize, 1)
}

func newCDCStreamWriterWorkers(ctx context.Context, dir string, prefixSize, workers int) (*cdcStreamWriter, error) {
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
	w := &cdcStreamWriter{ctx: ctx, dir: dir, prefixSize: prefixSize, workers: workers, enc: enc}
	if err := w.initialize(); err != nil {
		w.Abort()
		return nil, err
	}
	return w, nil
}

func (w *cdcStreamWriter) initialize() (err error) {
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

func (w *cdcStreamWriter) Write(p []byte) (written int, err error) {
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

func (w *cdcStreamWriter) WriteAt(p []byte, off int64) (int, error) {
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

func (w *cdcStreamWriter) flushChunk() error { return w.flushChunkContext(w.ctx) }

func (w *cdcStreamWriter) flushChunkContext(ctx context.Context) error {
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
	if candidate := w.dictionary[digest]; candidate != nil && bytes.Equal(candidate.Value.(*cdcDictionaryEntry).bytes, w.chunk) {
		old := candidate.Value.(*cdcDictionaryEntry)
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

func (w *cdcStreamWriter) flushChunkParallel(ctx context.Context) error {
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
	item := cdcPendingChunk{entry: e, rawLength: len(w.chunk)}
	if candidate := w.dictionary[digest]; candidate != nil && bytes.Equal(candidate.Value.(*cdcDictionaryEntry).bytes, w.chunk) {
		item.anchor = candidate.Value.(*cdcDictionaryEntry)
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

func (w *cdcStreamWriter) remember(digest [sha256.Size]byte, entry cdcEntry, ordinal uint32) *cdcDictionaryEntry {
	remove := func(element *list.Element) {
		old := element.Value.(*cdcDictionaryEntry)
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
	value := &cdcDictionaryEntry{digest: digest, bytes: append([]byte(nil), w.chunk...), entry: entry, ordinal: ordinal}
	w.dictionary[digest] = w.lru.PushFront(value)
	w.dictionaryBytes += len(value.bytes)
	return value
}

func (w *cdcStreamWriter) Finish(path string) error {
	_, err := w.FinishWithMetadataContext(context.Background(), path)
	return err
}

func (w *cdcStreamWriter) FinishWithMetadataContext(ctx context.Context, path string) (metadata snapshotFileMetadata, err error) {
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

func (w *cdcStreamWriter) Abort() {
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

func (w *cdcStreamWriter) Reset() error {
	if w == nil || w.closed {
		return errors.New("snapshots: CDC writer closed")
	}
	ctx, dir, prefixSize, workers := w.ctx, w.dir, w.prefixSize, w.workers
	w.Abort()
	next, err := newCDCStreamWriterWorkers(ctx, dir, prefixSize, workers)
	if err != nil {
		return err
	}
	*w = *next
	return nil
}

// An automatic merge preserves CDC when any verified input already uses it.
// Old-only inputs keep V2: dictionary-only collection is not evidence of value
// repetition, and automatic merging must not introduce an extra value scan.
func historyMergeCompressionFormat(dir string, sources []stateDomainChangeBinaryCompactionSource) (string, error) {
	format := os.Getenv("GTRON_HISTORY_COMPRESSION_FORMAT")
	if format != "auto" {
		return format, nil
	}
	for _, source := range sources {
		file, err := os.Open(filepath.Join(dir, source.history.Path))
		if err != nil {
			return "", err
		}
		var header [16]byte
		_, readErr := io.ReadFull(file, header[:])
		closeErr := file.Close()
		if readErr != nil {
			return "", readErr
		}
		if closeErr != nil {
			return "", closeErr
		}
		if string(header[:8]) == compressedBlockMagic && binary.BigEndian.Uint32(header[8:12]) == compressedBlockCDCVersion {
			return "3", nil
		}
	}
	return "2", nil
}

package snapshots

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

const historyReferenceMergeMaxSources = 128
const historyReferenceMergeLegacyRecordLimit = 128 << 20

// copyValueSpans translates a validated logical interval to destination-owned
// chunks. mapping is local to exactly this immutable source, bounded at 1 MiB.
// The source mutex covers load and owned copy: no cached payload alias escapes.
// Destination chunks are authenticated again by StoreChunkWithDigest.
func (r *historyReferenceReader) copyValueSpans(ctx context.Context, dst *historyReferenceWriter, mapping []uint32, off, length uint64) ([]historyReferenceValueSpan, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.validateLayout(ctx); err != nil {
		return nil, err
	}
	if uint64(len(mapping)) != r.header.chunks || off > r.header.logical || length > r.header.logical-off {
		return nil, fmt.Errorf("%w: merge value interval", errHistoryReferenceCorrupt)
	}
	if length == 0 {
		return nil, r.check(ctx)
	}
	lo, hi := uint64(0), r.header.spans
	for lo < hi {
		mid := lo + (hi-lo)/2
		span, err := r.span(ctx, mid)
		if err != nil {
			return nil, err
		}
		if span.logical+uint64(span.length) <= off {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	var spans []historyReferenceValueSpan
	for length > 0 {
		if err := r.check(ctx); err != nil {
			return nil, err
		}
		span, err := r.span(ctx, lo)
		if err != nil {
			return nil, err
		}
		if off < span.logical || off >= span.logical+uint64(span.length) {
			return nil, errHistoryReferenceCorrupt
		}
		delta := off - span.logical
		n := min(length, uint64(span.length)-delta)
		id := mapping[span.chunk]
		if id == math.MaxUint32 {
			chunk, err := r.chunk(ctx, span.chunk)
			if err != nil {
				return nil, err
			}
			raw, err := r.loadChunk(ctx, span.chunk)
			if err != nil {
				return nil, err
			}
			// StoreChunkWithDigest copies into its own scratch file synchronously;
			// it does not retain raw. Keeping the source lock pins this cache entry.
			id, err = dst.StoreChunkWithDigest(raw, chunk.digest)
			if err != nil {
				return nil, err
			}
			mapping[span.chunk] = id
		}
		if uint64(len(spans)) >= historyReferenceMaxSpans {
			return nil, errHistoryReferenceBudget
		}
		spans = append(spans, historyReferenceValueSpan{id, span.offset + uint32(delta), uint32(n)})
		off += n
		length -= n
		lo++
	}
	return spans, r.check(ctx)
}

// referenceMergeSourceReader borrows the contextual source for dictionary and
// tx binding. Non-R1 V6 files are converted once into a private R1 scratch file;
// old CDC anchors retain their boundaries. No trio is installed or published.
func referenceMergeSourceReader(ctx context.Context, dir string, source stateDomainChangeBinaryCompactionSource, original *stateDomainChangeHistoryReader) (reader *historyReferenceReader, release func() error, err error) {
	if r, ok := original.historySegmentReader.(*historyReferenceReader); ok {
		return r, func() error { return nil }, nil
	}
	path := filepath.Join(dir, source.history.Path)
	if err = historyReferenceTranscodePreflight(ctx, path, source.history.Size); err != nil {
		return nil, nil, err
	}
	w, err := newHistoryReferenceWriter(ctx, filepath.Join(dir, "etl"), 0)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() error { return w.Release() }
	defer func() {
		if err != nil {
			err = errors.Join(err, cleanup())
		}
	}()
	if compressed, ok := original.historySegmentReader.(*compressedBlockReader); ok {
		if compressed.cdc != nil {
			err = historyReferenceTranscodeCDC(ctx, w, compressed.cdc)
		} else {
			for i := range compressed.table {
				if err = ctx.Err(); err != nil {
					break
				}
				var raw []byte
				raw, err = compressed.blockBytes(i)
				if err != nil {
					break
				}
				if err = historyReferenceTranscodeBytes(w, raw); err != nil {
					break
				}
			}
		}
	} else {
		var scratch [historyReferenceMaxChunk]byte
		for off := uint64(0); off < source.segmentSize; {
			n := min(uint64(len(scratch)), source.segmentSize-off)
			if err = historyReferenceReadAt(ctx, original, scratch[:n], off); err != nil {
				break
			}
			if err = historyReferenceTranscodeBytes(w, scratch[:n]); err != nil {
				break
			}
			off += n
		}
	}
	if err != nil {
		return nil, nil, err
	}
	if w.LogicalSize() != source.segmentSize {
		return nil, nil, errors.New("snapshots: merge transcode logical size differs")
	}
	output := filepath.Join(w.dir, "merge-source.r1")
	if _, _, err = w.Finalize(output); err != nil {
		return nil, nil, err
	}
	reader, err = openHistoryReferenceReader(ctx, output, historyReferenceMaxChunk)
	if err != nil {
		return nil, nil, err
	}
	return reader, func() error { return errors.Join(reader.Close(), cleanup()) }, nil
}

// scanReferenceMergeSource never materializes a V6 Prev. Headers and contextual
// keys are small; each selected chunk is imported once into dst. Legacy v1-v5
// has an explicit one-record owning fallback (128 MiB frame maximum), retaining
// at most two records for the existing complete order comparison. Codec buffers,
// ETL, bounded dictionary cache and container tables are additional, not RSS.
func scanReferenceMergeSource(ctx context.Context, dir string, source stateDomainChangeBinaryCompactionSource, dst *historyReferenceWriter, emit func(*rawdb.StateDomainChange, uint64, []historyReferenceValueSpan) error) (err error) {
	reader, header, size, err := openStateDomainChangeBinarySegmentSequentialReader(dir, source.history)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, reader.Close()) }()
	if header != source.segmentHeader || size != source.segmentSize {
		return errors.New("snapshots: reference merge source identity changed")
	}
	count, offset, err := stateDomainChangeBinaryTxRangeTableBoundsAt(reader, size, source.history, header)
	if err != nil {
		return err
	}
	if count != source.txRangeCount || offset != source.recordOffset {
		return errors.New("snapshots: reference merge source range table changed")
	}
	contextual, ok := reader.(*stateDomainChangeHistoryReader)
	if !ok {
		return errors.New("snapshots: missing contextual reference merge reader")
	}
	var r *historyReferenceReader
	var mapping []uint32
	if header.version == stateDomainChangeBinaryVersionV6 {
		var release func() error
		r, release, err = referenceMergeSourceReader(ctx, dir, source, contextual)
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, release()) }()
		if err = r.ValidateLayout(ctx); err != nil {
			return err
		}
		mapping = make([]uint32, int(r.header.chunks))
		for i := range mapping {
			mapping[i] = math.MaxUint32
		}
	}
	var previousTx uint64
	var previousLegacy *rawdb.StateDomainChange
	for i := uint64(0); i < header.count; i++ {
		if err = ctx.Err(); err != nil {
			return err
		}
		var row *rawdb.StateDomainChange
		var prev, next uint64
		var spans []historyReferenceValueSpan
		if r != nil {
			if offset > size || size-offset < 21 {
				return io.ErrUnexpectedEOF
			}
			var frame [21]byte
			if err = historyReferenceReadAt(ctx, r, frame[:], offset); err != nil {
				return err
			}
			payload := uint64(binary.BigEndian.Uint32(frame[:4]))
			prev = uint64(binary.BigEndian.Uint32(frame[17:]))
			if payload != 17+prev || payload > size-offset-4 || frame[16] > 1 {
				return errors.New("snapshots: malformed reference merge V6 record")
			}
			row = &rawdb.StateDomainChange{TxNum: binary.BigEndian.Uint64(frame[8:16]), PrevExists: frame[16] == 1}
			key, err := contextual.v6Key(binary.BigEndian.Uint32(frame[4:8]))
			if err != nil {
				return err
			}
			if err = decodeStateDomainChangeBinaryAccessorKey(key, row); err != nil {
				return err
			}
			txRange, err := contextual.txRangeForTxNum(row.TxNum)
			if err != nil {
				return err
			}
			if err = hydrateStateDomainChangeBinaryRecordV5FromRange(txRange, i, row); err != nil {
				return err
			}
			spans, err = r.copyValueSpans(ctx, dst, mapping, offset+21, prev)
			if err != nil {
				return err
			}
			next = offset + 4 + payload
		} else {
			if offset > size || size-offset < 4 {
				return io.ErrUnexpectedEOF
			}
			var length [4]byte
			if err = historyReferenceReadAt(ctx, reader, length[:], offset); err != nil {
				return err
			}
			if binary.BigEndian.Uint32(length[:]) > historyReferenceMergeLegacyRecordLimit {
				return fmt.Errorf("%w: legacy merge record", errHistoryReferenceBudget)
			}
			row, next, err = readStateDomainChangeBinaryRecordAtBoundedIndex(reader, offset, size, i)
			if err != nil {
				return err
			}
			if previousLegacy != nil && compareStateDomainChangeForBinary(previousLegacy, row) > 0 {
				return errStateDomainChangeHistoryRecordsNotOrdered
			}
			previousLegacy = row
			prev = uint64(len(row.Prev))
			for off := 0; off < len(row.Prev); {
				end := min(len(row.Prev), off+historyReferenceMaxChunk)
				id, err := dst.StoreChunk(row.Prev[off:end])
				if err != nil {
					return err
				}
				spans = append(spans, historyReferenceValueSpan{id, 0, uint32(end - off)})
				off = end
			}
		}
		if row.TxNum < source.history.FromTxNum || row.TxNum > source.history.ToTxNum || i > 0 && row.TxNum < previousTx {
			return errStateDomainChangeHistoryRecordsNotOrdered
		}
		previousTx = row.TxNum
		if err = emit(row, prev, spans); err != nil {
			return err
		}
		offset = next
	}
	if offset != size {
		return errors.New("snapshots: reference merge source trailing bytes")
	}
	if r != nil {
		// Unreferenced chunks are still part of the self-contained source.
		// Used Prev chunks were authenticated when imported; finish the rest
		// without rereading every used chunk a second time.
		r.mu.Lock()
		for id, mapped := range mapping {
			if mapped == math.MaxUint32 {
				if _, err = r.loadChunk(ctx, uint32(id)); err != nil {
					break
				}
			}
		}
		r.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

// compactStateDomainChangeReferenceHistoryRunContext prepares a single R1/V7
// trio from sources admitted/authenticated by the ordinary compaction collector.
// It never publishes a manifest or deletes active input files. The caller keeps
// the existing source/logical/record budgets, lease and atomic integration gate.
// At most 128 source descriptors, one <=1MiB chunk-ID map, 256MiB on-disk metadata
// spool, existing ETL thresholds and R1 directory limits are permitted.
func compactStateDomainChangeReferenceHistoryRunContext(ctx context.Context, dir string, cfg DomainCfg, selection historyCompactionSelection, sources []stateDomainChangeBinaryCompactionSource, progress *historyCompactionProgress) (refs []SegmentRef, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if cfg.Dataset != SegmentDatasetStateDomainChange || len(sources) == 0 || len(sources) > historyReferenceMergeMaxSources || selection.toTxNum < selection.fromTxNum {
		return nil, errors.New("snapshots: invalid reference merge selection")
	}
	total, err := stateDomainChangeBinaryCompactionRecordCount(sources)
	if err != nil {
		return nil, err
	}
	if total > math.MaxUint32 {
		return nil, errHistoryReferenceBudget
	}
	for i, s := range sources {
		if s.history.FromTxNum > s.history.ToTxNum || i == 0 && s.history.FromTxNum != selection.fromTxNum || i > 0 && (sources[i-1].history.ToTxNum == math.MaxUint64 || s.history.FromTxNum != sources[i-1].history.ToTxNum+1) {
			return nil, errors.New("snapshots: noncontiguous reference merge sources")
		}
	}
	if sources[len(sources)-1].history.ToTxNum != selection.toTxNum {
		return nil, errors.New("snapshots: reference merge range differs")
	}
	ref := SegmentRef{Dataset: cfg.Dataset, Kind: SegmentHistory, FromTxNum: selection.fromTxNum, ToTxNum: selection.toTxNum, AggregationSteps: selection.aggregationSteps, Path: cfg.HistoryPath(selection.fromTxNum, selection.toTxNum)}
	if err = validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return nil, err
	}
	temp := filepath.Join(dir, "etl")
	if err = os.MkdirAll(temp, 0700); err != nil {
		return nil, err
	}
	v6, err := newStateDomainChangeV6Build(etl.Options{TempDir: temp}, dir, ref.Path)
	if err != nil {
		return nil, err
	}
	defer v6.Close()
	container, err := newHistoryReferenceWriter(ctx, temp, int(stateDomainChangeBinaryTxRangeTableStart(stateDomainChangeBinaryVersionV6)+8))
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, container.Release())
		if err != nil {
			refs = nil
		}
	}()
	spool, err := os.CreateTemp(temp, "history-reference-merge-*.spool")
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, spool.Close())
		err = errors.Join(err, os.Remove(spool.Name()))
		if err != nil {
			refs = nil
		}
	}()
	spoolHash := sha256.New()
	out := bufio.NewWriterSize(io.MultiWriter(spool, spoolHash), 256<<10)
	var rows, spoolBytes uint64
	progress.setRecordTotal(total)
	progress.setPhase(historyCompactionPhaseCopyRecords)
	for i, source := range sources {
		// Match the old merger's complete V6 dictionary, including legal keys
		// with no postings. This reads metadata only, never Prev payloads.
		if source.segmentHeader.version == stateDomainChangeBinaryVersionV6 {
			if err = collectStateDomainChangeBinarySegmentV6Keys(ctx, dir, v6, source); err != nil {
				return nil, err
			}
		}
		err = scanReferenceMergeSource(ctx, dir, source, container, func(row *rawdb.StateDomainChange, prev uint64, spans []historyReferenceValueSpan) error {
			if rows >= total {
				return errors.New("snapshots: reference merge excess rows")
			}
			if source.segmentHeader.version != stateDomainChangeBinaryVersionV6 {
				if err := v6.CollectKey(row); err != nil {
					return err
				}
			}
			metadata := *row
			metadata.Prev, metadata.Next, metadata.NextExists = nil, nil, false
			n, err := writeHistoryReferenceSpoolRow(out, &metadata, prev, spans, historyReferenceSpoolLimit-spoolBytes)
			if err != nil {
				return err
			}
			spoolBytes += n
			rows++
			return nil
		})
		if err != nil {
			return nil, err
		}
		progress.setSourcesProcessed(uint64(i + 1))
		progress.setRecordsProcessed(rows)
	}
	if rows != total {
		return nil, errors.New("snapshots: reference merge missing rows")
	}
	if err = out.Flush(); err != nil {
		return nil, err
	}
	progress.setPhase(historyCompactionPhaseBuildDictionary)
	if err = v6.FinishDictionaryContext(ctx); err != nil {
		return nil, err
	}
	if err = writeStateDomainChangeBinaryHeaderToVersion(container, stateDomainChangeBinarySegmentMagic, ref.FromTxNum, ref.ToTxNum, total, stateDomainChangeBinaryVersionV6); err != nil {
		return nil, err
	}
	if err = writeStateDomainChangeBinaryV6DictionaryCommitment(container, v6.dictionaryDigest); err != nil {
		return nil, err
	}
	progress.setPhase(historyCompactionPhaseWriteTxRanges)
	txCount, err := writeStateDomainChangeBinaryCompactionTxRanges(ctx, dir, container, sources)
	if err != nil {
		return nil, err
	}
	// This is the last source read: bind the copied records AND the reopened
	// tx-range tables to the originally admitted physical identities before
	// any final artifact is installed.
	for _, source := range sources {
		if err = checkStateDomainChangeBinarySegmentChecksumContext(ctx, dir, source.history); err != nil {
			return nil, err
		}
	}
	index, indexName, err := createStateDomainChangeBinaryTempFile(dir, stateDomainChangeBinaryIndexPath(ref.Path))
	if err != nil {
		return nil, err
	}
	defer func() { _ = index.Close(); _ = os.Remove(indexName) }()
	if err = writeStateDomainChangeBinaryHeaderTo(index, stateDomainChangeBinaryIndexMagic, ref.FromTxNum, ref.ToTxNum, 0); err != nil {
		return nil, err
	}
	rw := newHistoryReferenceRecordWriter(container, index, v6, ref)
	if _, err = spool.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	replayHash := sha256.New()
	input := bufio.NewReaderSize(io.TeeReader(io.LimitReader(spool, int64(spoolBytes)+1), replayHash), 256<<10)
	for i := uint64(0); i < total; i++ {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		row, prev, spans, e := readHistoryReferenceSpoolRow(input)
		if e != nil {
			return nil, e
		}
		if err = rw.Write(row, prev, spans); err != nil {
			return nil, err
		}
	}
	if _, e := input.ReadByte(); !errors.Is(e, io.EOF) {
		return nil, errors.New("snapshots: reference merge spool trailing data")
	}
	if !bytes.Equal(spoolHash.Sum(nil), replayHash.Sum(nil)) {
		return nil, errors.New("snapshots: reference merge spool checksum mismatch")
	}
	if err = rw.Finish(); err != nil {
		return nil, err
	}
	if err = writeStateDomainChangeBinaryHeaderCount(index, rw.indexCount); err != nil {
		return nil, err
	}
	// Match the ordinary merger's V7 tx index, not the temporary V2 stream.
	// Keep the old temp variables on rewrite failure for deferred cleanup.
	rewritten, rewrittenName, err := rewriteStateDomainChangeBinaryIndexV7Context(ctx, index, indexName)
	if err != nil {
		return nil, err
	}
	index, indexName = rewritten, rewrittenName
	progress.setPhase(historyCompactionPhaseFinalizeHistory)
	output := filepath.Join(container.dir, "merged.r1")
	size, checksum, err := container.Finalize(output)
	if err != nil {
		return nil, err
	}
	seg, err := publishStateDomainChangeBinaryFinal(dir, ref, output, size, checksum, true)
	if err != nil {
		return nil, err
	}
	progress.setPhase(historyCompactionPhaseBuildAccessor)
	idx, accessor, _, err := finalizeStateDomainChangeBinaryCompanionsV6Context(ctx, dir, seg, index, indexName, v6, total)
	if err != nil {
		return nil, err
	}
	if err = validateBuiltStateDomainChangeBinaryFiles(dir, seg, idx, accessor, container.LogicalSize(), rw.indexCount, total, txCount); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return []SegmentRef{seg, accessor, idx}, nil
}

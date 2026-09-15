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

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

const (
	historyReferenceSpoolLimit = 256 << 20
	historyReferenceRowLimit   = 16 << 20
	historyReferenceBlockLimit = 5000
)

// HistoryReferenceBuildStats counts actual work in the experimental one-source-
// pass builder. SpoolBytes excludes Prev payloads; it is not a heap/RSS bound.
type HistoryReferenceBuildStats struct {
	SourceBlocks   uint64                         `json:"source_blocks"`
	SharedBlocks   uint64                         `json:"shared_blocks"`
	FallbackBlocks uint64                         `json:"fallback_blocks"`
	DecodedBytes   uint64                         `json:"source_decoded_bytes"`
	Rows           uint64                         `json:"rows"`
	PrevBytes      uint64                         `json:"prev_bytes"`
	PrevSpans      uint64                         `json:"prev_spans"`
	SpoolBytes     uint64                         `json:"spool_bytes"`
	LogicalBytes   uint64                         `json:"logical_bytes"`
	Container      HistoryReferenceContainerStats `json:"container"`
}

type historyReferenceValueSpan struct {
	chunk, offset, length uint32
}

// BuildDiagnosticStateHistoryReferenceTrioContext writes a self-contained
// reference container and ordinary V7 companions in a private output directory.
// It never publishes a manifest, changes source state, or permits hot pruning.
// The caller owns and authenticates the canonical range of the pinned view.
func BuildDiagnosticStateHistoryReferenceTrioContext(ctx context.Context, view rawdb.StateHistoryReadView, dir string, fromTx, toTx, fromBlock, toBlock uint64, relPath string, opts etl.Options) (refs []SegmentRef, stats HistoryReferenceBuildStats, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, stats, err
	}
	if view == nil || toTx < fromTx || toBlock < fromBlock || toBlock-fromBlock >= historyReferenceBlockLimit || !isStateDomainChangeBinarySegmentPath(relPath) {
		return nil, stats, errors.New("snapshots: invalid reference history range, view or path")
	}
	ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: fromTx, ToTxNum: toTx, AggregationSteps: 1, Path: relPath}
	if err := validateSegment(ref, fromTx, toTx); err != nil {
		return nil, stats, err
	}
	if opts.TempDir == "" {
		opts.TempDir = filepath.Join(dir, "etl")
	}
	if err := os.MkdirAll(opts.TempDir, 0700); err != nil {
		return nil, stats, err
	}
	v6, err := newStateDomainChangeV6Build(opts, dir, relPath)
	if err != nil {
		return nil, stats, err
	}
	defer v6.Close()
	prefix := stateDomainChangeBinaryTxRangeTableStart(stateDomainChangeBinaryVersionV6) + 8 + (toBlock-fromBlock+1)*stateDomainChangeBinaryTxRangeSize
	if prefix > math.MaxInt {
		return nil, stats, errors.New("snapshots: reference history prefix exceeds int")
	}
	container, err := newHistoryReferenceWriter(ctx, opts.TempDir, int(prefix))
	if err != nil {
		return nil, stats, err
	}
	defer func() {
		err = errors.Join(err, container.Release())
		if err != nil {
			refs = nil
		}
	}()
	spool, err := os.CreateTemp(opts.TempDir, "history-reference-rows-*.tmp")
	if err != nil {
		return nil, stats, err
	}
	defer func() { _ = spool.Close(); _ = os.Remove(spool.Name()) }()
	spoolHash := sha256.New()
	spoolOut := bufio.NewWriterSize(io.MultiWriter(spool, spoolHash), 256<<10)
	var chunkScratch []byte
	var previousTx uint64
	havePrevious := false
	err = rawdb.IterateStateHistorySpanBlocks(ctx, view, fromBlock, toBlock, fromTx, toTx, func(block *rawdb.StateHistorySpanBlock) (bool, error) {
		info := block.Info()
		stats.SourceBlocks++
		if info.SharedPack {
			stats.SharedBlocks++
		}
		if info.Fallback {
			stats.FallbackBlocks++
		}
		stats.DecodedBytes += info.DecodedBytes
		ids := make([]uint32, block.ChunkCount())
		stored := make([]bool, len(ids))
		return block.IterateRows(func(row *rawdb.StateHistorySpanRow) (bool, error) {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			change := &row.Change
			if len(change.Prev) != 0 || len(change.Next) != 0 || row.PrevLength > math.MaxUint32-17 || (!change.PrevExists && row.PrevLength != 0) || change.TxNum < fromTx || change.TxNum > toTx {
				return false, errors.New("snapshots: invalid authenticated reference history row")
			}
			if havePrevious && change.TxNum < previousTx {
				return false, errStateDomainChangeHistoryRecordsNotOrdered
			}
			previousTx, havePrevious = change.TxNum, true
			if stats.Rows >= math.MaxUint32 {
				return false, errors.New("snapshots: reference history row count exceeds accessor limit")
			}
			spans := make([]historyReferenceValueSpan, len(row.PrevSpans))
			var length uint64
			for i, span := range row.PrevSpans {
				if uint64(span.ChunkIndex) >= uint64(len(ids)) {
					return false, errors.New("snapshots: reference source chunk index out of bounds")
				}
				chunk, err := block.Chunk(int(span.ChunkIndex))
				if err != nil {
					return false, err
				}
				if span.Length == 0 || uint64(span.Offset)+uint64(span.Length) > uint64(chunk.Length) {
					return false, errors.New("snapshots: reference source span out of bounds")
				}
				if !stored[span.ChunkIndex] {
					if cap(chunkScratch) < int(chunk.Length) {
						chunkScratch = make([]byte, chunk.Length)
					}
					chunkScratch = chunkScratch[:chunk.Length]
					n, err := block.CopyChunk(int(span.ChunkIndex), chunkScratch)
					if err != nil {
						return false, err
					}
					if n != len(chunkScratch) {
						return false, io.ErrUnexpectedEOF
					}
					id, err := container.StoreChunkWithDigest(chunkScratch, chunk.Digest)
					if err != nil {
						return false, err
					}
					ids[span.ChunkIndex], stored[span.ChunkIndex] = id, true
				}
				spans[i] = historyReferenceValueSpan{ids[span.ChunkIndex], span.Offset, span.Length}
				length += uint64(span.Length)
			}
			if length != row.PrevLength {
				return false, errors.New("snapshots: reference Prev length mismatch")
			}
			if err := v6.CollectKey(change); err != nil {
				return false, err
			}
			n, err := writeHistoryReferenceSpoolRow(spoolOut, change, row.PrevLength, spans, historyReferenceSpoolLimit-stats.SpoolBytes)
			if err != nil {
				return false, err
			}
			stats.Rows++
			stats.PrevBytes += length
			stats.PrevSpans += uint64(len(spans))
			stats.SpoolBytes += n
			return true, nil
		})
	})
	if err != nil {
		return nil, stats, fmt.Errorf("snapshots: collect reference history: %w", err)
	}
	if err := spoolOut.Flush(); err != nil {
		return nil, stats, err
	}
	if err := v6.FinishDictionaryContext(ctx); err != nil {
		return nil, stats, err
	}
	if err := writeStateDomainChangeBinaryHeaderToVersion(container, stateDomainChangeBinarySegmentMagic, fromTx, toTx, stats.Rows, stateDomainChangeBinaryVersionV6); err != nil {
		return nil, stats, err
	}
	if err := writeStateDomainChangeBinaryV6DictionaryCommitment(container, v6.dictionaryDigest); err != nil {
		return nil, stats, err
	}
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	ranges := cfg.IterateHotHistoryTxRangeBorrowed
	cfg.IterateHotHistoryTxRangeBorrowed = func(db ethdb.Iteratee, from, to uint64, fn func(*rawdb.StateTxRange) (bool, error)) error {
		return ranges(db, from, to, func(row *rawdb.StateTxRange) (bool, error) {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			return fn(row)
		})
	}
	// Range cancellation is checked by the source view and again before each
	// metadata row; the fixed view preserves exactly the same block identities.
	txCount, err := writeStateDomainChangeBinaryTxRangeTableFromDB(container, view, cfg, fromTx, toTx, &stateDomainChangeHistoryBlockRange{from: fromBlock, to: toBlock})
	if err != nil {
		return nil, stats, err
	}
	indexPath := stateDomainChangeBinaryIndexPath(relPath)
	index, indexName, err := createStateDomainChangeBinaryTempFile(dir, indexPath)
	if err != nil {
		return nil, stats, err
	}
	defer func() { _ = index.Close(); _ = os.Remove(indexName) }()
	if err := writeStateDomainChangeBinaryHeaderTo(index, stateDomainChangeBinaryIndexMagic, fromTx, toTx, 0); err != nil {
		return nil, stats, err
	}
	rw := newHistoryReferenceRecordWriter(container, index, v6, ref)
	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return nil, stats, err
	}
	replayHash := sha256.New()
	input := bufio.NewReaderSize(io.TeeReader(io.LimitReader(spool, int64(stats.SpoolBytes)+1), replayHash), 256<<10)
	for n := uint64(0); n < stats.Rows; n++ {
		if err := ctx.Err(); err != nil {
			return nil, stats, err
		}
		row, prev, spans, err := readHistoryReferenceSpoolRow(input)
		if err != nil {
			return nil, stats, err
		}
		if err := rw.Write(row, prev, spans); err != nil {
			return nil, stats, err
		}
	}
	if _, err := input.ReadByte(); !errors.Is(err, io.EOF) {
		return nil, stats, errors.New("snapshots: reference history spool trailing data")
	}
	if !bytes.Equal(spoolHash.Sum(nil), replayHash.Sum(nil)) {
		return nil, stats, errors.New("snapshots: reference history spool checksum mismatch")
	}
	if err := rw.Finish(); err != nil {
		return nil, stats, err
	}
	stats.LogicalBytes = container.LogicalSize()
	if err := writeStateDomainChangeBinaryHeaderCount(index, rw.indexCount); err != nil {
		return nil, stats, err
	}
	abs := filepath.Join(dir, relPath)
	if err := os.MkdirAll(filepath.Dir(abs), 0755); err != nil {
		return nil, stats, err
	}
	outputDir, err := os.MkdirTemp(filepath.Dir(abs), ".history-reference-output-")
	if err != nil {
		return nil, stats, err
	}
	defer os.RemoveAll(outputDir)
	name := filepath.Join(outputDir, "segment")
	size, checksum, err := container.Finalize(name)
	if err != nil {
		return nil, stats, err
	}
	stats.Container = container.Stats()
	seg, err := publishStateDomainChangeBinaryFinal(dir, ref, name, size, checksum, true)
	if err != nil {
		return nil, stats, err
	}
	idx, accessor, _, err := finalizeStateDomainChangeBinaryCompanionsV6Context(ctx, dir, seg, index, indexName, v6, stats.Rows)
	if err != nil {
		return nil, stats, err
	}
	if err := validateBuiltStateDomainChangeBinaryFiles(dir, seg, idx, accessor, stats.LogicalBytes, rw.indexCount, stats.Rows, txCount); err != nil {
		return nil, stats, err
	}
	if err := ctx.Err(); err != nil {
		return nil, stats, err
	}
	return []SegmentRef{seg, accessor, idx}, stats, nil
}

func writeHistoryReferenceSpoolRow(w io.Writer, row *rawdb.StateDomainChange, prev uint64, spans []historyReferenceValueSpan, remaining uint64) (uint64, error) {
	meta, err := encodeStateDomainChangeRecord(row)
	if err != nil {
		return 0, err
	}
	size := uint64(16) + uint64(len(meta)) + uint64(len(spans))*12
	if size > historyReferenceRowLimit || size > remaining || len(spans) > math.MaxUint32 {
		return 0, errors.New("snapshots: reference history metadata spool budget exceeded")
	}
	var head [16]byte
	binary.BigEndian.PutUint32(head[:4], uint32(len(meta)))
	binary.BigEndian.PutUint64(head[4:12], prev)
	binary.BigEndian.PutUint32(head[12:], uint32(len(spans)))
	if _, err := w.Write(head[:]); err != nil {
		return 0, err
	}
	if _, err := w.Write(meta); err != nil {
		return 0, err
	}
	var b [12]byte
	for _, span := range spans {
		binary.BigEndian.PutUint32(b[:4], span.chunk)
		binary.BigEndian.PutUint32(b[4:8], span.offset)
		binary.BigEndian.PutUint32(b[8:], span.length)
		if _, err := w.Write(b[:]); err != nil {
			return 0, err
		}
	}
	return size, nil
}

func readHistoryReferenceSpoolRow(r io.Reader) (*rawdb.StateDomainChange, uint64, []historyReferenceValueSpan, error) {
	var head [16]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return nil, 0, nil, err
	}
	metaLen, prev, count := binary.BigEndian.Uint32(head[:4]), binary.BigEndian.Uint64(head[4:12]), binary.BigEndian.Uint32(head[12:])
	if uint64(metaLen)+uint64(count)*12+16 > historyReferenceRowLimit || prev > math.MaxUint32-17 {
		return nil, 0, nil, errors.New("snapshots: reference history spool row bounds")
	}
	meta := make([]byte, metaLen)
	if _, err := io.ReadFull(r, meta); err != nil {
		return nil, 0, nil, err
	}
	row, err := decodeStateDomainChangeRecord(meta)
	if err != nil {
		return nil, 0, nil, err
	}
	if len(row.Prev) != 0 || len(row.Next) != 0 || (!row.PrevExists && prev != 0) {
		return nil, 0, nil, errors.New("snapshots: invalid reference history spool metadata")
	}
	spans := make([]historyReferenceValueSpan, count)
	var b [12]byte
	var total uint64
	for i := range spans {
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return nil, 0, nil, err
		}
		spans[i] = historyReferenceValueSpan{binary.BigEndian.Uint32(b[:4]), binary.BigEndian.Uint32(b[4:8]), binary.BigEndian.Uint32(b[8:])}
		if spans[i].length == 0 {
			return nil, 0, nil, errors.New("snapshots: zero-length reference history span")
		}
		total += uint64(spans[i].length)
	}
	if total != prev {
		return nil, 0, nil, errors.New("snapshots: reference history spool span total mismatch")
	}
	return row, prev, spans, nil
}

type historyReferenceRecordWriter struct {
	container         *historyReferenceWriter
	index             *bufio.Writer
	v6                *stateDomainChangeV6Build
	ref               SegmentRef
	count, indexCount uint64
	current           stateDomainChangeBinaryTxOffset
	haveIndex         bool
}

func newHistoryReferenceRecordWriter(container *historyReferenceWriter, index io.Writer, v6 *stateDomainChangeV6Build, ref SegmentRef) *historyReferenceRecordWriter {
	return &historyReferenceRecordWriter{container: container, index: bufio.NewWriterSize(index, 256<<10), v6: v6, ref: ref}
}

func (w *historyReferenceRecordWriter) Write(row *rawdb.StateDomainChange, prev uint64, spans []historyReferenceValueSpan) error {
	if row == nil || row.TxNum < w.ref.FromTxNum || row.TxNum > w.ref.ToTxNum || prev > math.MaxUint32-17 || w.count >= math.MaxUint32 {
		return errors.New("snapshots: reference record identity or length invalid")
	}
	offset := w.container.LogicalSize()
	w.v6.keyScratch = appendStateDomainChangeBinaryAccessorLookupKey(w.v6.keyScratch[:0], row.FlatDomain, row.Owner, row.Generation, row.Domain, row.Key)
	id, err := w.v6.KeyID(w.v6.keyScratch)
	if err != nil {
		return err
	}
	if err := w.v6.CollectPosting(id, row.TxNum, offset, w.count); err != nil {
		return err
	}
	if w.haveIndex && row.TxNum < w.current.txNum {
		return errStateDomainChangeHistoryRecordsNotOrdered
	}
	if !w.haveIndex || row.TxNum != w.current.txNum {
		if err := w.flushIndex(); err != nil {
			return err
		}
		w.current = stateDomainChangeBinaryTxOffset{txNum: row.TxNum, offset: offset, recordIndex: w.count}
		w.haveIndex = true
	}
	w.current.count++
	var header [21]byte
	binary.BigEndian.PutUint32(header[:4], uint32(17+prev))
	putStateDomainChangeRecordV6(header[4:], row, id)
	binary.BigEndian.PutUint32(header[17:], uint32(prev))
	if _, err := w.container.Write(header[:]); err != nil {
		return err
	}
	for _, span := range spans {
		if err := w.container.WriteSpan(span.chunk, span.offset, span.length); err != nil {
			return err
		}
	}
	if w.container.LogicalSize() != offset+21+prev {
		return errors.New("snapshots: reference record logical length mismatch")
	}
	w.count++
	return nil
}

func (w *historyReferenceRecordWriter) flushIndex() error {
	if !w.haveIndex {
		return nil
	}
	var entry [stateDomainChangeBinaryIndexEntrySize]byte
	putStateDomainChangeBinaryIndexEntry(&entry, w.current)
	if _, err := w.index.Write(entry[:]); err != nil {
		return err
	}
	w.indexCount++
	w.haveIndex = false
	return nil
}

func (w *historyReferenceRecordWriter) Finish() error {
	if err := w.flushIndex(); err != nil {
		return err
	}
	return w.index.Flush()
}

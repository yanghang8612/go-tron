package snapshots

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"sort"
	"time"
)

// ExperimentalHistoryPostingBenchOptions bounds all source work. Selected keys
// are equally spaced in dictionary order, not claimed to represent chain-wide
// compressibility. A key is never truncated; oversized lists are reported.
type ExperimentalHistoryPostingBenchOptions struct {
	MaxKeys               uint64 `json:"max_keys"`
	MaxPostings           uint64 `json:"max_postings"`
	MaxSourceRecords      uint64 `json:"max_source_records"`
	MaxSourceLogicalBytes uint64 `json:"max_source_logical_bytes"`
	QuerySamplesPerKey    uint64 `json:"query_samples_per_key"`
	LocatorStride         uint32 `json:"locator_stride"`
}

type ExperimentalHistoryPostingBenchReport struct {
	HistoryPath                 string                                 `json:"history_path"`
	AccessorPath                string                                 `json:"accessor_path"`
	SourceKeys                  uint64                                 `json:"source_keys"`
	SourceRecords               uint64                                 `json:"source_records"`
	SourceLogicalBytes          uint64                                 `json:"source_logical_bytes"`
	SourceAccessorLogicalBytes  uint64                                 `json:"source_accessor_logical_bytes"`
	SourceAccessorPhysicalBytes uint64                                 `json:"source_accessor_physical_bytes"`
	SourcePostingSectionBytes   uint64                                 `json:"source_posting_section_bytes"`
	SelectedKeys                uint64                                 `json:"selected_keys"`
	SelectedPostings            uint64                                 `json:"selected_postings"`
	SkippedKeys                 uint64                                 `json:"skipped_keys"`
	SkippedPostings             uint64                                 `json:"skipped_postings"`
	AllKeysMeasured             bool                                   `json:"all_keys_measured"`
	V7PostingBytes              uint64                                 `json:"v7_posting_bytes"`
	DensePostingBytes           uint64                                 `json:"dense_posting_bytes"`
	IndirectPostingBytes        uint64                                 `json:"indirect_posting_bytes"`
	FullLocatorBytes            uint64                                 `json:"full_locator_bytes"`
	IndirectPlusFullLocator     uint64                                 `json:"indirect_plus_full_locator_bytes"`
	LocatorBuildNanos           int64                                  `json:"locator_build_ns"`
	V7DecodeNanos               int64                                  `json:"v7_decode_ns"`
	DenseEncodeNanos            int64                                  `json:"dense_encode_ns"`
	IndirectEncodeNanos         int64                                  `json:"indirect_encode_ns"`
	DenseOpenNanos              int64                                  `json:"dense_open_ns"`
	IndirectOpenNanos           int64                                  `json:"indirect_open_ns"`
	PointQueries                uint64                                 `json:"point_queries"`
	V7PointNanos                int64                                  `json:"v7_point_ns"`
	DensePointNanos             int64                                  `json:"dense_point_ns"`
	IndirectPointNanos          int64                                  `json:"indirect_point_ns"`
	DenseScanNanos              int64                                  `json:"dense_scan_ns"`
	IndirectScanNanos           int64                                  `json:"indirect_scan_ns"`
	LocatorBuildStats           ExperimentalHistoryLookupStats         `json:"locator_build_stats"`
	IndirectPointStats          ExperimentalHistoryLookupStats         `json:"indirect_point_stats"`
	IndirectScanStats           ExperimentalHistoryLookupStats         `json:"indirect_scan_stats"`
	OracleEqual                 bool                                   `json:"oracle_equal"`
	Options                     ExperimentalHistoryPostingBenchOptions `json:"options"`
	Caveats                     []string                               `json:"caveats"`
}

// ExperimentHistoryPostings reads only immutable source .seg/.kv files. It does
// not open chaindata, write scratch, publish, prune, or start any lifecycle.
// Actual serialized blobs are built in memory. All selected postings and query
// results are checked against the production V7 logical decoder. This is an
// experiment, not a source authentication or deletion authorization routine.
func ExperimentHistoryPostings(ctx context.Context, dir string, historyRef, accessorRef SegmentRef, opts ExperimentalHistoryPostingBenchOptions) (report ExperimentalHistoryPostingBenchReport, err error) {
	if opts.MaxKeys == 0 {
		opts.MaxKeys = 512
	}
	if opts.MaxPostings == 0 {
		opts.MaxPostings = 1_000_000
	}
	if opts.MaxSourceRecords == 0 {
		opts.MaxSourceRecords = 2_000_000
	}
	if opts.MaxSourceLogicalBytes == 0 {
		opts.MaxSourceLogicalBytes = 8 << 30
	}
	if opts.QuerySamplesPerKey == 0 {
		opts.QuerySamplesPerKey = 16
	}
	if opts.LocatorStride == 0 {
		opts.LocatorStride = 64
	}
	if opts.MaxKeys > 1_000_000 || opts.MaxPostings > 10_000_000 || opts.QuerySamplesPerKey > 1024 || opts.LocatorStride > 1024 {
		return report, fmt.Errorf("experimental postings: excessive benchmark options")
	}
	report.HistoryPath, report.AccessorPath, report.Options = historyRef.Path, accessorRef.Path, opts
	report.Caveats = []string{
		"No production format integration; source reads only; CRC and V7 oracle equality are not chain authentication.",
		"Posting bytes exclude unchanged key dictionary; indirect total charges the actual full-segment locator even for sampled keys.",
		"Key sampling is stratified dictionary order; no chain-wide size projection. Oversized whole keys are skipped and counted.",
		"Point timings use preloaded serialized posting lists for both codecs; dictionary lookup and value hydration are excluded. Indirect reads the real logical segment through its production decompressor with a warm OS/cache workload.",
		"Header bytes are logical ReadAt requests, not physical disk I/O or decompression bytes; latency is measured wall clock. Locator construction performs one sequential record-header walk over the full bounded source.",
		"MaxSourceRecords bounds locator construction only. Later indirect scan work is bounded by selected postings times locator stride; point work is bounded by reported query count times stride, including selected-frame validation reads.",
	}
	if historyRef.Dataset != SegmentDatasetStateDomainChange || historyRef.Kind != SegmentHistory || accessorRef.Dataset != SegmentDatasetStateDomainChange || accessorRef.Kind != SegmentAccessor || historyRef.FromTxNum != accessorRef.FromTxNum || historyRef.ToTxNum != accessorRef.ToTxNum {
		return report, fmt.Errorf("experimental postings: mismatched source companions")
	}
	segment, logicalSize, header, err := openHistorySegmentForRead(dir, historyRef)
	if err != nil {
		return report, err
	}
	defer segment.Close()
	accessor, ah, size, err := openStateDomainChangeBinaryAccessorReader(dir, accessorRef)
	if err != nil {
		return report, err
	}
	defer accessor.Close()
	if header.version != stateDomainChangeBinaryVersionV6 || ah.version != stateDomainChangeBinaryVersionV7 {
		return report, fmt.Errorf("experimental postings: requires V6 history and V7 accessor")
	}
	h, err := decodeStateDomainChangeBinaryAccessorV7Header(accessor, size)
	if err != nil {
		return report, err
	}
	if h.recordCount != header.count || header.count > opts.MaxSourceRecords || logicalSize > opts.MaxSourceLogicalBytes {
		return report, fmt.Errorf("experimental postings: mismatched count or full-source scan budget exceeded (records=%d logical_bytes=%d)", header.count, logicalSize)
	}
	dictionaryDigest, err := readStateDomainChangeBinaryV6DictionaryCommitment(segment)
	if err != nil {
		return report, err
	}
	if dictionaryDigest != h.dictionaryDigest {
		return report, fmt.Errorf("experimental postings: history/accessor dictionary commitment mismatch")
	}
	report.SourceKeys, report.SourceRecords, report.SourceLogicalBytes = h.keyCount, h.recordCount, logicalSize
	report.SourceAccessorLogicalBytes, report.SourceAccessorPhysicalBytes, report.SourcePostingSectionBytes = size, accessorRef.Size, h.postingLen
	_, firstOffset, err := stateDomainChangeBinaryTxRangeTableBoundsAt(segment, logicalSize, historyRef, header)
	if err != nil {
		return report, err
	}
	source := ExperimentalHistoryRecordSource{Reader: segment, LogicalSize: logicalSize, MaxScanRecords: opts.MaxSourceRecords}
	started := time.Now()
	locatorBytes, locatorStats, err := BuildExperimentalHistoryLocator(ctx, source, firstOffset, header.count, opts.LocatorStride)
	report.LocatorBuildNanos, report.LocatorBuildStats = time.Since(started).Nanoseconds(), locatorStats
	if err != nil {
		return report, err
	}
	report.FullLocatorBytes = uint64(len(locatorBytes))
	locator, err := OpenExperimentalHistoryLocator(locatorBytes)
	if err != nil {
		return report, err
	}
	// Query/scan budgets may cover several sparse walks per posting; each
	// individual lookup still skips strictly fewer than LocatorStride records.
	source.MaxScanRecords = math.MaxUint64
	selected := min(opts.MaxKeys, h.keyCount)
	var cachedBlock uint64 = math.MaxUint64
	var keyRows []stateDomainChangeBinaryAccessorV6Record
	for sample := uint64(0); sample < selected; sample++ {
		if err := contextError(ctx); err != nil {
			return report, err
		}
		keyID := sample * h.keyCount / selected
		blockIndex := keyID / stateDomainChangeBinaryAccessorV6BlockKeys
		if blockIndex != cachedBlock {
			block, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(accessor, h, blockIndex)
			if err != nil {
				return report, err
			}
			keyRows, err = stateDomainChangeBinaryAccessorV6ReadBlock(accessor, size, h, block, uint32(blockIndex*stateDomainChangeBinaryAccessorV6BlockKeys))
			if err != nil {
				return report, err
			}
			cachedBlock = blockIndex
		}
		record := keyRows[keyID%stateDomainChangeBinaryAccessorV6BlockKeys]
		if uint64(record.postings) > opts.MaxPostings-report.SelectedPostings {
			report.SkippedKeys++
			report.SkippedPostings += uint64(record.postings)
			continue
		}
		frames, err := stateDomainChangeBinaryAccessorV7PostingFrames(accessor, h, record)
		if err != nil {
			return report, err
		}
		listLen, err := stateDomainChangeBinaryAccessorV7PostingListLength(accessor, h, record)
		if err != nil {
			return report, err
		}
		if listLen > uint64(math.MaxInt) {
			return report, fmt.Errorf("experimental postings: oversized list")
		}
		listBytes := make([]byte, listLen)
		base := h.headerSize + h.blockDirLen + h.keyDataLen + record.postingOff
		if _, err := accessor.ReadAt(listBytes, int64(base)); err != nil {
			return report, err
		}
		memoryV7 := experimentalPostingOffsetReader{reader: bytes.NewReader(listBytes), base: int64(base)}
		// V7 single-frame reads normally prefetch into the following key's
		// bytes. The isolated memory list ends here, so expose that exact end
		// while preserving every logical field and the production decoder.
		memoryHeader := h
		memoryHeader.postingLen = record.postingOff + listLen
		started = time.Now()
		oracle := make([]ExperimentalHistoryPosting, 0, record.postings)
		for f, frame := range frames {
			postings, err := stateDomainChangeBinaryAccessorV7ReadFrame(memoryV7, memoryHeader, record, frame)
			if err != nil {
				return report, err
			}
			frames[f].firstTx = postings[0].txNum
			for _, p := range postings {
				oracle = append(oracle, ExperimentalHistoryPosting{TxNum: p.txNum, Offset: p.offset, RecordOrdinal: uint64(p.recordIndex)})
			}
		}
		report.V7DecodeNanos += time.Since(started).Nanoseconds()
		if uint64(len(oracle)) != uint64(record.postings) {
			return report, fmt.Errorf("experimental postings: oracle count mismatch")
		}
		started = time.Now()
		denseData, err := EncodeExperimentalHistoryPostings(ctx, record.key, h.fromTxNum, oracle, false)
		report.DenseEncodeNanos += time.Since(started).Nanoseconds()
		if err != nil {
			return report, err
		}
		started = time.Now()
		indirectData, err := EncodeExperimentalHistoryPostings(ctx, record.key, h.fromTxNum, oracle, true)
		report.IndirectEncodeNanos += time.Since(started).Nanoseconds()
		if err != nil {
			return report, err
		}
		started = time.Now()
		dense, err := OpenExperimentalHistoryPostings(ctx, record.key, h.fromTxNum, uint64(len(oracle)), denseData)
		report.DenseOpenNanos += time.Since(started).Nanoseconds()
		if err != nil {
			return report, err
		}
		started = time.Now()
		indirect, err := OpenExperimentalHistoryPostings(ctx, record.key, h.fromTxNum, uint64(len(oracle)), indirectData)
		report.IndirectOpenNanos += time.Since(started).Nanoseconds()
		if err != nil {
			return report, err
		}
		for variant, index := range []*ExperimentalHistoryPostings{dense, indirect} {
			position := 0
			started = time.Now()
			stats, err := index.Scan(ctx, locator, source, func(p ExperimentalHistoryPosting) error {
				if position >= len(oracle) || p != oracle[position] {
					return fmt.Errorf("experimental postings: V7 scan oracle mismatch keyID=%d posting=%d", keyID, position)
				}
				position++
				return nil
			})
			elapsed := time.Since(started).Nanoseconds()
			if err != nil || position != len(oracle) {
				return report, fmt.Errorf("experimental postings: incomplete oracle scan: %w", err)
			}
			if variant == 0 {
				report.DenseScanNanos += elapsed
			} else {
				report.IndirectScanNanos += elapsed
				experimentalAddLookupStats(&report.IndirectScanStats, stats)
			}
		}
		if len(oracle) > 0 {
			queries := []uint64{oracle[0].TxNum, oracle[len(oracle)-1].TxNum}
			if oracle[0].TxNum > 0 {
				queries = append(queries, oracle[0].TxNum-1)
			}
			if oracle[len(oracle)-1].TxNum < math.MaxUint64 {
				queries = append(queries, oracle[len(oracle)-1].TxNum+1)
			}
			for q := uint64(0); q < opts.QuerySamplesPerKey; q++ {
				target := oracle[q*uint64(len(oracle))/opts.QuerySamplesPerKey].TxNum
				queries = append(queries, target)
				if target < math.MaxUint64 {
					queries = append(queries, target+1)
				}
			}
			for _, target := range queries {
				if err := contextError(ctx); err != nil {
					return report, err
				}
				at := sort.Search(len(oracle), func(i int) bool { return oracle[i].TxNum >= target })
				for variant, index := range []*ExperimentalHistoryPostings{dense, indirect} {
					started = time.Now()
					got, found, stats, err := index.LowerBound(ctx, target, locator, source)
					elapsed := time.Since(started).Nanoseconds()
					if err != nil || found != (at < len(oracle)) || found && got != oracle[at] {
						return report, fmt.Errorf("experimental postings: lower_bound oracle mismatch target=%d: %w", target, err)
					}
					if variant == 0 {
						report.DensePointNanos += elapsed
					} else {
						report.IndirectPointNanos += elapsed
						experimentalAddLookupStats(&report.IndirectPointStats, stats)
					}
					exact, exactFound, _, err := index.Exact(ctx, target, locator, source)
					wantExact := at < len(oracle) && oracle[at].TxNum == target
					if err != nil || exactFound != wantExact || exactFound && exact != oracle[at] {
						return report, fmt.Errorf("experimental postings: exact oracle mismatch: %w", err)
					}
				}
				started = time.Now()
				v7, found, err := experimentalV7PostingLowerBound(memoryV7, memoryHeader, record, frames, target)
				report.V7PointNanos += time.Since(started).Nanoseconds()
				if err != nil || found != (at < len(oracle)) || found && v7 != oracle[at] {
					return report, fmt.Errorf("experimental postings: V7 lower_bound oracle mismatch: %w", err)
				}
				report.PointQueries++
			}
		}
		report.SelectedKeys++
		report.SelectedPostings += uint64(len(oracle))
		report.V7PostingBytes += uint64(len(listBytes))
		report.DensePostingBytes += uint64(len(denseData))
		report.IndirectPostingBytes += uint64(len(indirectData))
	}
	report.IndirectPlusFullLocator = report.IndirectPostingBytes + report.FullLocatorBytes
	report.AllKeysMeasured = report.SelectedKeys == report.SourceKeys
	report.OracleEqual = true
	return report, contextError(ctx)
}

type experimentalPostingOffsetReader struct {
	reader io.ReaderAt
	base   int64
}

func (r experimentalPostingOffsetReader) ReadAt(p []byte, off int64) (int, error) {
	if off < r.base {
		return 0, io.ErrUnexpectedEOF
	}
	return r.reader.ReadAt(p, off-r.base)
}

func experimentalV7PostingLowerBound(reader io.ReaderAt, h stateDomainChangeBinaryAccessorV6Header, record stateDomainChangeBinaryAccessorV6Record, frames []stateDomainChangeBinaryAccessorV7Frame, target uint64) (ExperimentalHistoryPosting, bool, error) {
	f := sort.Search(len(frames), func(i int) bool { return frames[i].firstTx >= target })
	if f > 0 {
		f--
	}
	for ; f < len(frames); f++ {
		rows, err := stateDomainChangeBinaryAccessorV7ReadFrame(reader, h, record, frames[f])
		if err != nil {
			return ExperimentalHistoryPosting{}, false, err
		}
		at := sort.Search(len(rows), func(i int) bool { return rows[i].txNum >= target })
		if at < len(rows) {
			p := rows[at]
			return ExperimentalHistoryPosting{TxNum: p.txNum, Offset: p.offset, RecordOrdinal: uint64(p.recordIndex)}, true, nil
		}
	}
	return ExperimentalHistoryPosting{}, false, nil
}

func experimentalAddLookupStats(dst *ExperimentalHistoryLookupStats, src ExperimentalHistoryLookupStats) {
	dst.FramesDecoded += src.FramesDecoded
	dst.RecordsSkipped += src.RecordsSkipped
	dst.RecordFramesValidated += src.RecordFramesValidated
	dst.LogicalBytesSkipped += src.LogicalBytesSkipped
	dst.HeaderBytesRead += src.HeaderBytesRead
}

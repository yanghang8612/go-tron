package snapshots

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
)

// These are admission estimates, never input authentication or deletion proof.
// Readers inspect physical headers/trailers, CDC sparse metadata, and one fixed
// accessor header. They do not decode a history payload or scan record/Prev data.
// Keeping one value per immutable ref lets selectors cache cheap source reads
// while considering several groups and continue beyond an oversized boundary.
type historyReferenceCompactionInputBudget struct {
	HasReference bool
	Records      uint64
	LogicalBytes uint64
	// Chunks/Spans bound imported Prev data, before new record literals. Old
	// compressed inner versions are deliberately unknown: include one possible
	// per-record split for the owning legacy branch as well as V6 conversion.
	ChunksUpper uint64
	SpansUpper  uint64
	RawUpper    uint64
	// A legacy V6 source must first fit its private transcode container, even if
	// the final merge would import only a fraction of its chunks.
	ConvertedChunksUpper uint64
	ConvertedSpansUpper  uint64
	NeedsConversionBound bool
	Overflow             bool
}

type historyReferenceCompactionBudget struct {
	HasReference       bool
	Deferred           bool
	Reason             string
	Sources            uint64
	Records            uint64
	InputLogicalBytes  uint64
	ChunksUpper        uint64
	SpansUpper         uint64
	MetadataBytesUpper uint64
	LiteralBytesUpper  uint64
	RawBytesUpper      uint64
	LogicalBytesUpper  uint64
	PatchPrefixBytes   uint64
}

// estimateHistoryReferenceCompactionBudget is conservative, including no
// cross-source dedup or adjacent-span coalescing. For V6, cutting R disjoint Prev
// intervals into S ordered source spans introduces at most R extra pieces.
// Record headers introduce at most R + literal-pages spans. We allow 3R extra
// spans to include the unknown legacy owning branch. A full source logical size
// bounds its tx-table literal bytes; this can greatly overestimate Prev-heavy
// inputs, but avoids decoding even their retained first compressed block.
//
// A Deferred result means the UPPER BOUND is too large, not that the actual merge
// cannot fit. Callers must treat an oversized source as a selection boundary and
// try later leaves. Do not repeatedly invoke a writer to discover these limits.
// This gate covers container/source-transcode limits only; existing input, ETL,
// 256MiB metadata-spool and legacy per-record limits remain independent gates.
func estimateHistoryReferenceCompactionBudget(inputs []historyReferenceCompactionInputBudget) historyReferenceCompactionBudget {
	r := historyReferenceCompactionBudget{Sources: uint64(len(inputs)), PatchPrefixBytes: stateDomainChangeBinaryTxRangeTableStart(stateDomainChangeBinaryVersionV6) + 8}
	var chunks, spans, raw uint64
	var overflow bool
	for _, in := range inputs {
		r.HasReference = r.HasReference || in.HasReference
		overflow = overflow || in.Overflow
		r.Records = historyReferenceCompactionBudgetAdd(r.Records, in.Records, &overflow)
		r.InputLogicalBytes = historyReferenceCompactionBudgetAdd(r.InputLogicalBytes, in.LogicalBytes, &overflow)
		chunks = historyReferenceCompactionBudgetAdd(chunks, in.ChunksUpper, &overflow)
		spans = historyReferenceCompactionBudgetAdd(spans, in.SpansUpper, &overflow)
		raw = historyReferenceCompactionBudgetAdd(raw, in.RawUpper, &overflow)
		if in.NeedsConversionBound {
			metadata := historyReferenceCompactionBudgetAdd(historyReferenceCompactionBudgetMul(in.ConvertedChunksUpper, historyReferenceChunkEntrySize, &overflow), historyReferenceCompactionBudgetMul(in.ConvertedSpansUpper, historyReferenceSpanEntrySize, &overflow), &overflow)
			if in.ConvertedChunksUpper > historyReferenceMaxChunks || in.ConvertedSpansUpper > historyReferenceMaxSpans || metadata > historyReferenceMaxMetadata {
				r.Deferred, r.Reason = true, "reference-source-conversion-bound"
			}
		}
	}
	// Unlike the hot block builder, merge patches only the fixed 76-byte header
	// and tx-count. The complete tx-range table is sequential literal output;
	// applying the 8MiB WriteAt prefix limit to that table would be incorrect.
	recordLiterals := historyReferenceCompactionBudgetMul(r.Records, 21, &overflow)
	r.LiteralBytesUpper = historyReferenceCompactionBudgetAdd(r.InputLogicalBytes, recordLiterals, &overflow)
	r.LiteralBytesUpper = historyReferenceCompactionBudgetAdd(r.LiteralBytesUpper, r.PatchPrefixBytes, &overflow)
	literalPages := historyReferenceCompactionCeilChunks(r.LiteralBytesUpper)
	r.ChunksUpper = historyReferenceCompactionBudgetAdd(chunks, literalPages, &overflow)
	r.SpansUpper = historyReferenceCompactionBudgetAdd(spans, historyReferenceCompactionBudgetMul(r.Records, 3, &overflow), &overflow)
	r.SpansUpper = historyReferenceCompactionBudgetAdd(r.SpansUpper, literalPages, &overflow)
	r.MetadataBytesUpper = historyReferenceCompactionBudgetAdd(historyReferenceCompactionBudgetMul(r.ChunksUpper, historyReferenceChunkEntrySize, &overflow), historyReferenceCompactionBudgetMul(r.SpansUpper, historyReferenceSpanEntrySize, &overflow), &overflow)
	r.RawBytesUpper = historyReferenceCompactionBudgetAdd(raw, r.LiteralBytesUpper, &overflow)
	// Tx tables plus Prev are subsets of source logical data. V6 record headers
	// may replace a legacy representation; count their complete size once more.
	r.LogicalBytesUpper = r.LiteralBytesUpper
	switch {
	case overflow:
		r.Deferred, r.Reason = true, "reference-estimate-overflow"
	case r.Sources == 0 || r.Sources > historyReferenceMergeMaxSources:
		r.Deferred, r.Reason = true, "reference-source-count"
	case r.Records > math.MaxUint32:
		r.Deferred, r.Reason = true, "reference-record-count"
	case r.Deferred:
		// Retain the individual conversion-bound reason.
	case r.ChunksUpper > historyReferenceMaxChunks:
		r.Deferred, r.Reason = true, "reference-chunk-bound"
	case r.SpansUpper > historyReferenceMaxSpans:
		r.Deferred, r.Reason = true, "reference-span-bound"
	case r.MetadataBytesUpper > historyReferenceMaxMetadata:
		r.Deferred, r.Reason = true, "reference-metadata-bound"
	case r.RawBytesUpper > historyReferenceMaxPhysical-historyReferenceMaxMetadata-historyReferenceHeaderSize:
		r.Deferred, r.Reason = true, "reference-raw-bound"
	case r.LogicalBytesUpper > historyReferenceMaxLogical:
		r.Deferred, r.Reason = true, "reference-logical-bound"
	case r.PatchPrefixBytes > historyReferenceMaxPrefix:
		r.Deferred, r.Reason = true, "reference-prefix-bound"
	}
	return r
}

func readHistoryReferenceCompactionBudget(ctx context.Context, dir string, candidates []historyCompactionCandidate) (historyReferenceCompactionBudget, error) {
	inputs := make([]historyReferenceCompactionInputBudget, 0, len(candidates))
	for _, candidate := range candidates {
		input, err := readHistoryReferenceCompactionInputBudget(ctx, dir, candidate)
		if err != nil {
			return historyReferenceCompactionBudget{}, err
		}
		inputs = append(inputs, input)
	}
	return estimateHistoryReferenceCompactionBudget(inputs), contextError(ctx)
}

func readHistoryReferenceCompactionInputBudget(ctx context.Context, dir string, candidate historyCompactionCandidate) (out historyReferenceCompactionInputBudget, err error) {
	if err = contextError(ctx); err != nil {
		return out, err
	}
	history := candidate.history
	if history.Dataset != SegmentDatasetStateDomainChange || history.Kind != SegmentHistory {
		return out, errors.New("snapshots: reference admission requires state history")
	}
	if err = validateSegmentRef(history); err != nil {
		return out, err
	}
	accessor, ok := historyCompactionCompanion(candidate, SegmentAccessor)
	if !ok || accessor.Dataset != history.Dataset || accessor.Kind != SegmentAccessor || accessor.FromTxNum != history.FromTxNum || accessor.ToTxNum != history.ToTxNum {
		return out, errors.New("snapshots: reference admission accessor coverage differs")
	}
	if err = validateSegmentRef(accessor); err != nil {
		return out, err
	}
	a, err := os.Open(filepath.Join(dir, accessor.Path))
	if err != nil {
		return out, err
	}
	aInfo, e := a.Stat()
	if e == nil && (!aInfo.Mode().IsRegular() || aInfo.Size() < stateDomainChangeBinaryHeaderSize || accessor.Size != 0 && uint64(aInfo.Size()) != accessor.Size) {
		e = errors.New("snapshots: reference admission accessor physical size/type differs")
	}
	var aHeader stateDomainChangeBinaryHeader
	if e == nil {
		aHeader, e = readStateDomainChangeBinaryHeaderAt(contextReaderAt{ctx: ctx, r: a}, stateDomainChangeBinaryAccessorMagic)
	}
	e = errors.Join(e, a.Close())
	if e != nil {
		return out, e
	}
	if aHeader.fromTxNum != history.FromTxNum || aHeader.toTxNum != history.ToTxNum {
		return out, errors.New("snapshots: reference admission accessor header range differs")
	}
	out.Records = aHeader.count
	f, err := os.Open(filepath.Join(dir, history.Path))
	if err != nil {
		return out, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	info, err := f.Stat()
	if err != nil {
		return out, err
	}
	if !info.Mode().IsRegular() || info.Size() < stateDomainChangeBinaryHeaderSize || history.Size != 0 && uint64(info.Size()) != history.Size {
		return out, errors.New("snapshots: reference admission history physical size/type differs")
	}
	size := uint64(info.Size())
	reader := contextReaderAt{ctx: ctx, r: f}
	var magic [8]byte
	if _, err = reader.ReadAt(magic[:], 0); err != nil {
		return out, err
	}
	var converted uint64
	unknownLegacy := false
	switch {
	case string(magic[:]) == historyReferenceMagic:
		var raw [historyReferenceHeaderSize]byte
		if _, err = reader.ReadAt(raw[:], 0); err != nil {
			return out, err
		}
		h, e := decodeHistoryReferenceHeader(raw[:], size)
		if e != nil {
			return out, e
		}
		out.HasReference, out.LogicalBytes, out.ChunksUpper, out.SpansUpper = true, h.logical, h.chunks, h.spans
		// Imported chunks can contain bytes outside tiny selected intervals;
		// logical size is not an upper bound on the owned raw chunk payload.
		out.RawUpper = historyReferenceCompactionBudgetMul(h.chunks, historyReferenceMaxChunk, &out.Overflow)
		return out, contextError(ctx)
	case magic == stateDomainChangeBinarySegmentMagic:
		h, e := readStateDomainChangeBinaryHeaderAt(reader, stateDomainChangeBinarySegmentMagic)
		if e != nil {
			return out, e
		}
		if h.fromTxNum != history.FromTxNum || h.toTxNum != history.ToTxNum || h.count != out.Records {
			return out, errors.New("snapshots: reference admission raw history/accessor identity differs")
		}
		out.LogicalBytes = size
		converted = historyReferenceCompactionCeilChunks(size)
		unknownLegacy = h.version != stateDomainChangeBinaryVersionV6
		out.NeedsConversionBound = !unknownLegacy
	case string(magic[:]) == compressedBlockMagic:
		var h [compressedBlockHeaderSize]byte
		if _, err = reader.ReadAt(h[:], 0); err != nil {
			return out, err
		}
		var count uint64
		switch binary.BigEndian.Uint32(h[8:12]) {
		case compressedBlockVersion:
			out.LogicalBytes, err = readHistoryCompactionContainerLogicalBytes(reader, size, h[:])
			count = binary.BigEndian.Uint64(h[24:32])
		case compressedBlockFooterVersion:
			layout, e := readCompressedBlockFooterInfo(reader, size, h[:])
			err = e
			out.LogicalBytes, count = layout.uncSize, layout.blockCount
		case compressedBlockCDCVersion:
			cdc, e := openCDCReader(reader, size, h[:])
			if e != nil {
				return out, e
			}
			out.LogicalBytes, count, converted = cdc.logical, cdc.count, cdc.count
		default:
			return out, errors.New("snapshots: unsupported reference admission compression version")
		}
		if err != nil {
			return out, err
		}
		if converted == 0 {
			if count == 0 || out.LogicalBytes == 0 {
				return out, errors.New("snapshots: reference admission has empty compression table")
			}
			// For positive page lengths li, sum ceil(li/M) is at most
			// ceil(sum(li)/M)+pages-1. No potentially huge first-page decode.
			converted = historyReferenceCompactionBudgetAdd(historyReferenceCompactionCeilChunks(out.LogicalBytes), count-1, &out.Overflow)
		}
		unknownLegacy, out.NeedsConversionBound = true, true
	default:
		return out, errors.New("snapshots: unknown reference admission history magic")
	}
	out.ConvertedChunksUpper, out.ConvertedSpansUpper = converted, converted
	out.ChunksUpper, out.SpansUpper, out.RawUpper = converted, converted, out.LogicalBytes
	if unknownLegacy {
		out.ChunksUpper = historyReferenceCompactionBudgetAdd(out.ChunksUpper, out.Records, &out.Overflow)
		// The common 3R allowance in the combined estimate includes the legacy
		// per-record span split. Only chunk import needs this separate term.
	}
	return out, contextError(ctx)
}

func historyReferenceCompactionCeilChunks(n uint64) uint64 {
	chunks := n / historyReferenceMaxChunk
	if n%historyReferenceMaxChunk != 0 {
		chunks++
	}
	return chunks
}

func historyReferenceCompactionBudgetAdd(a, b uint64, overflow *bool) uint64 {
	if math.MaxUint64-a < b {
		*overflow = true
		return math.MaxUint64
	}
	return a + b
}

func historyReferenceCompactionBudgetMul(a, b uint64, overflow *bool) uint64 {
	if a != 0 && b > math.MaxUint64/a {
		*overflow = true
		return math.MaxUint64
	}
	return a * b
}

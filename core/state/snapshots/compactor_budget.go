package snapshots

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
)

type historyCompactionInputCost struct {
	bytes        uint64
	records      uint64
	logicalBytes uint64
}

func historyCompactionNoSelectionReason(cfg CompactionConfig) string {
	if cfg.BusyLeafOnly {
		return "leaf-target-or-budget"
	}
	return ""
}

func historyCompactionWithinBudget(cfg CompactionConfig, bytes, logicalBytes, records, sources uint64) bool {
	return (cfg.MaxInputBytes == 0 || bytes <= cfg.MaxInputBytes) &&
		(cfg.MaxInputLogicalBytes == 0 || logicalBytes <= cfg.MaxInputLogicalBytes) &&
		(cfg.MaxInputRecords == 0 || records <= cfg.MaxInputRecords) &&
		(cfg.MaxSources == 0 || sources <= cfg.MaxSources)
}

// The accessor's small header supplies the record count without scanning or
// decompressing the canonical history. This is admission metadata only: the
// normal compactor still authenticates all inputs and cross-checks the count
// against canonical history before it constructs any replacement output.
func readHistoryCompactionInputCost(ctx context.Context, dir string, candidate historyCompactionCandidate) (historyCompactionInputCost, error) {
	var cost historyCompactionInputCost
	for _, ref := range append([]SegmentRef{candidate.history}, candidate.companions...) {
		if err := contextError(ctx); err != nil {
			return cost, err
		}
		stat, err := os.Stat(filepath.Join(dir, ref.Path))
		if err != nil {
			return cost, err
		}
		if stat.Size() < 0 || ref.Size != 0 && uint64(stat.Size()) != ref.Size {
			return cost, fmt.Errorf("snapshots: compaction input %q size %d, want %d", ref.Path, stat.Size(), ref.Size)
		}
		if uint64(stat.Size()) > math.MaxUint64-cost.bytes {
			return cost, errors.New("snapshots: compaction input bytes overflow")
		}
		cost.bytes += uint64(stat.Size())
	}
	logical, err := readHistoryCompactionLogicalBytes(ctx, dir, candidate.history)
	if err != nil {
		return cost, err
	}
	cost.logicalBytes = logical
	accessor, ok := historyCompactionCompanion(candidate, SegmentAccessor)
	if !ok {
		return cost, errors.New("snapshots: budgeted compaction requires a record-count accessor")
	}
	f, header, _, err := openStateDomainChangeBinaryAccessorReader(dir, accessor)
	if err != nil {
		return cost, err
	}
	err = f.Close()
	if err != nil {
		return cost, err
	}
	cost.records = header.count
	return cost, contextError(ctx)
}

// selectBudgetedHistoryCompactionLeaves keeps an already merged range immutable
// while catch-up is busy. A run is ready when it fills a source/step budget or
// adding its next original leaf would exceed the byte/record budget. Thus every
// original leaf is rewritten at most once, and two newly published small leaves
// do not trigger a merge on every build. An oversized leaf is a boundary, not a
// failing job that strands all subsequent work.
func selectBudgetedHistoryCompactionLeaves(ctx context.Context, candidates []historyCompactionCandidate, cfg CompactionConfig, readCost func(historyCompactionCandidate) (historyCompactionInputCost, error)) (historyCompactionSelection, bool, error) {
	maxSources := cfg.MaxSources
	maxSteps := cfg.MaxSteps
	if maxSteps == 0 {
		maxSteps = defaultCompactionMaxSteps
	}
	if maxSources == 0 || maxSources > maxSteps {
		maxSources = maxSteps
	}
	if maxSources < 2 {
		return historyCompactionSelection{}, false, nil
	}
	var selection historyCompactionSelection
	reset := func() { selection = historyCompactionSelection{} }
	for _, candidate := range candidates {
		if err := contextError(ctx); err != nil {
			return historyCompactionSelection{}, false, err
		}
		if candidate.history.effectiveAggregationSteps() != 1 {
			reset()
			continue
		}
		if len(selection.candidates) > 0 && !historySegmentsAreContiguous(selection.candidates[len(selection.candidates)-1].history, candidate.history) {
			reset()
		}
		cost, err := readCost(candidate)
		if err != nil {
			return historyCompactionSelection{}, false, err
		}
		overflow := cost.bytes > math.MaxUint64-selection.inputBytes || cost.logicalBytes > math.MaxUint64-selection.inputLogicalBytes || cost.records > math.MaxUint64-selection.inputRecords
		if overflow || !historyCompactionWithinBudget(cfg, selection.inputBytes+cost.bytes, selection.inputLogicalBytes+cost.logicalBytes, selection.inputRecords+cost.records, uint64(len(selection.candidates)+1)) {
			if len(selection.candidates) >= 2 {
				return selection, true, nil
			}
			reset()
		}
		if !historyCompactionWithinBudget(cfg, cost.bytes, cost.logicalBytes, cost.records, 1) {
			continue
		}
		selection.candidates = append(selection.candidates, candidate)
		selection.inputBytes += cost.bytes
		selection.inputLogicalBytes += cost.logicalBytes
		selection.inputRecords += cost.records
		selection.aggregationSteps++
		selection.fromTxNum = selection.candidates[0].history.FromTxNum
		selection.toTxNum = candidate.history.ToTxNum
		if uint64(len(selection.candidates)) >= maxSources ||
			cfg.MaxInputBytes > 0 && selection.inputBytes == cfg.MaxInputBytes ||
			cfg.MaxInputLogicalBytes > 0 && selection.inputLogicalBytes == cfg.MaxInputLogicalBytes ||
			cfg.MaxInputRecords > 0 && selection.inputRecords == cfg.MaxInputRecords {
			if len(selection.candidates) >= 2 {
				return selection, true, nil
			}
			reset()
		}
	}
	return historyCompactionSelection{}, false, nil
}

// Admission needs the logical size, not the block directory or any decoded
// payload. V2's existing footer-info reader uses a fixed trailer. V3's reader
// authenticates only its bounded sparse directory; it decodes no anchors.
// V1 has the size in its fixed header. Full canonical validation remains the
// publication gate after admission, including the V1/V2 table consistency.
func readHistoryCompactionLogicalBytes(ctx context.Context, dir string, ref SegmentRef) (uint64, error) {
	if err := contextError(ctx); err != nil {
		return 0, err
	}
	f, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return 0, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if stat.Size() < stateDomainChangeBinaryHeaderSize || ref.Size != 0 && uint64(stat.Size()) != ref.Size {
		return 0, errors.New("snapshots: invalid compaction history physical size")
	}
	size := uint64(stat.Size())
	var magic [8]byte
	reader := contextReaderAt{ctx: ctx, r: f}
	if _, err := reader.ReadAt(magic[:], 0); err != nil {
		return 0, err
	}
	if magic == stateDomainChangeBinarySegmentMagic {
		return size, nil
	}
	if string(magic[:]) != compressedBlockMagic {
		return 0, errors.New("snapshots: unknown compaction history magic")
	}
	var header [compressedBlockHeaderSize]byte
	if _, err := reader.ReadAt(header[:], 0); err != nil {
		return 0, err
	}
	return readHistoryCompactionContainerLogicalBytes(reader, size, header[:])
}

func readHistoryCompactionContainerLogicalBytes(reader io.ReaderAt, size uint64, header []byte) (uint64, error) {
	if len(header) != compressedBlockHeaderSize || string(header[:8]) != compressedBlockMagic {
		return 0, errors.New("snapshots: invalid compaction compression header")
	}
	switch binary.BigEndian.Uint32(header[8:12]) {
	case compressedBlockFooterVersion:
		info, err := readCompressedBlockFooterInfo(reader, size, header)
		return info.uncSize, err
	case compressedBlockCDCVersion:
		reader, err := openCDCReader(reader, size, header)
		if err != nil {
			return 0, err
		}
		return reader.logical, nil
	case compressedBlockVersion:
		count := binary.BigEndian.Uint64(header[24:32])
		tableLen, err := compressedBlockTableLen(count)
		if err != nil {
			return 0, err
		}
		end, overflow := checkedAdd(compressedBlockHeaderSize, tableLen)
		if overflow || end > size || binary.BigEndian.Uint64(header[40:48]) != end || binary.BigEndian.Uint32(header[12:16]) == 0 {
			return 0, errors.New("snapshots: invalid compaction compressed-block bounds")
		}
		return binary.BigEndian.Uint64(header[32:40]), nil
	default:
		return 0, errors.New("snapshots: unsupported compaction compression format")
	}
}

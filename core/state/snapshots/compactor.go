package snapshots

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

const defaultCompactionMaxSteps = uint64(256)

// CompactionConfig controls history segment compaction.
type CompactionConfig struct {
	MaxSteps uint64
	// MinSteps defers small merges until an aligned run reaches this logical
	// span. Zero keeps the normal two-step minimum. Catch-up builders set it to
	// MaxSteps so fresh sync writes each frozen range once instead of rewriting
	// the same leaves through every geometric level.
	MinSteps       uint64
	DeleteObsolete bool
}

// HistoryCompactionResult describes a registered history-domain compaction pass.
type HistoryCompactionResult struct {
	Merged bool
	// MergePasses is one for CompactHistoryDomain. Runner summaries may drain
	// multiple ready ranges and aggregate every emitted output for work metrics.
	MergePasses      int
	Dataset          SegmentDataset
	FromTxNum        uint64
	ToTxNum          uint64
	AggregationSteps uint64
	SegmentsMerged   int
	Segments         []SegmentRef
}

type historyCompactionCandidate struct {
	history    SegmentRef
	companions []SegmentRef
}

type historyCompactionSelection struct {
	candidates       []historyCompactionCandidate
	fromTxNum        uint64
	toTxNum          uint64
	aggregationSteps uint64
}

// CompactHistoryDomain merges the frontmost continuous run of binary history
// segments for a registered history domain and publishes the replacement refs.
func CompactHistoryDomain(dir string, dataset SegmentDataset, cfg CompactionConfig) (HistoryCompactionResult, error) {
	if dir == "" {
		return HistoryCompactionResult{}, errors.New("snapshots: compaction directory is empty")
	}
	maxSteps := cfg.MaxSteps
	if maxSteps == 0 {
		maxSteps = defaultCompactionMaxSteps
	}
	minSteps := cfg.MinSteps
	if minSteps < 2 {
		minSteps = 2
	}
	if minSteps > maxSteps {
		minSteps = maxSteps
	}

	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return HistoryCompactionResult{}, nil
		}
		return HistoryCompactionResult{}, err
	}
	historyCfg, ok := DefaultDomainRegistry().Dataset(dataset)
	if !ok || !historyCfg.HasHistory {
		return HistoryCompactionResult{}, nil
	}
	if historyCfg.CompactHistory == nil && (historyCfg.OpenHistory == nil || historyCfg.WriteHistory == nil) {
		return HistoryCompactionResult{}, fmt.Errorf("snapshots: history domain %s missing compaction codec", historyCfg.Dataset)
	}
	selection, ok := selectHistoryCompactionRunAtLeast(manifest, historyCfg, minSteps, maxSteps)
	if !ok {
		return HistoryCompactionResult{Dataset: historyCfg.Dataset}, nil
	}

	var refs []SegmentRef
	if historyCfg.CompactHistory != nil {
		refs, err = historyCfg.CompactHistory(dir, historyCfg, selection)
		if err != nil {
			return HistoryCompactionResult{}, err
		}
	} else {
		var changes []*rawdb.StateDomainChange
		for _, candidate := range selection.candidates {
			segmentChanges, err := historyCfg.OpenHistory(dir, candidate.history)
			if err != nil {
				return HistoryCompactionResult{}, err
			}
			for _, change := range segmentChanges {
				changes = append(changes, cloneStateDomainChangeForSegment(change))
			}
		}
		segRef, idxRef, accessorRef, err := historyCfg.WriteHistory(dir, SegmentRef{
			Dataset:          historyCfg.Dataset,
			Kind:             SegmentHistory,
			FromTxNum:        selection.fromTxNum,
			ToTxNum:          selection.toTxNum,
			AggregationSteps: selection.aggregationSteps,
			Path:             historyCfg.HistoryPath(selection.fromTxNum, selection.toTxNum),
		}, changes)
		if err != nil {
			return HistoryCompactionResult{}, err
		}
		refs = nonZeroSegmentRefs(segRef, accessorRef, idxRef)
	}

	if _, err := NewAggregator(dir).Integrate(selection.fromTxNum, selection.toTxNum, refs); err != nil {
		return HistoryCompactionResult{}, err
	}

	result := HistoryCompactionResult{
		Merged:           true,
		MergePasses:      1,
		Dataset:          historyCfg.Dataset,
		FromTxNum:        selection.fromTxNum,
		ToTxNum:          selection.toTxNum,
		AggregationSteps: selection.aggregationSteps,
		SegmentsMerged:   len(selection.candidates),
		Segments:         refs,
	}
	if cfg.DeleteObsolete {
		if err := deleteObsoleteHistoryCompactionFiles(dir, selection.candidates, refs); err != nil {
			return result, err
		}
	}
	return result, nil
}

func selectHistoryCompactionRun(manifest *Manifest, cfg DomainCfg, maxSteps uint64) (historyCompactionSelection, bool) {
	return selectHistoryCompactionRunAtLeast(manifest, cfg, 2, maxSteps)
}

func selectHistoryCompactionRunAtLeast(manifest *Manifest, cfg DomainCfg, minSteps, maxSteps uint64) (historyCompactionSelection, bool) {
	if manifest == nil {
		return historyCompactionSelection{}, false
	}
	candidates := historyCompactionCandidates(manifest, cfg)
	if len(candidates) < 2 {
		return historyCompactionSelection{}, false
	}
	if maxSteps == 0 {
		maxSteps = defaultCompactionMaxSteps
	}
	if minSteps < 2 {
		minSteps = 2
	}
	if minSteps > maxSteps {
		minSteps = maxSteps
	}

	// Erigon merges the earliest maximally aligned power-of-two step range and
	// caps immutable files at stepsInFrozenFile. Transaction density varies per
	// TRON block, so the manifest carries logical aggregation steps rather than
	// pretending that a block batch is a fixed transaction-number span.
	for runStart := 0; runStart < len(candidates); {
		runEnd := runStart + 1
		for runEnd < len(candidates) && historySegmentsAreContiguous(candidates[runEnd-1].history, candidates[runEnd].history) {
			runEnd++
		}
		if selection, ok := selectAlignedHistoryCompactionRunAtLeast(candidates[runStart:runEnd], minSteps, maxSteps); ok {
			return selection, true
		}
		runStart = runEnd
	}
	return historyCompactionSelection{}, false
}

func selectAlignedHistoryCompactionRun(candidates []historyCompactionCandidate, maxSteps uint64) (historyCompactionSelection, bool) {
	return selectAlignedHistoryCompactionRunAtLeast(candidates, 2, maxSteps)
}

func selectAlignedHistoryCompactionRunAtLeast(candidates []historyCompactionCandidate, minSteps, maxSteps uint64) (historyCompactionSelection, bool) {
	if len(candidates) < 2 || maxSteps == 0 {
		return historyCompactionSelection{}, false
	}
	boundary := map[uint64]int{0: 0}
	var logicalEnd uint64
	bestStart, bestEnd := -1, -1
	var bestLogicalStart, bestSteps uint64
	for i, candidate := range candidates {
		steps := candidate.history.effectiveAggregationSteps()
		if steps > math.MaxUint64-logicalEnd {
			break
		}
		logicalEnd += steps
		span := logicalEnd & (^logicalEnd + 1) // rightmost set bit
		if span > maxSteps {
			span = maxSteps
		}
		if span < minSteps {
			boundary[logicalEnd] = i + 1
			continue
		}
		logicalStart := logicalEnd - span
		start, aligned := boundary[logicalStart]
		if aligned && i+1-start >= 2 {
			// Match Erigon's earliest-first rule while allowing a later endpoint
			// with the same start to widen and absorb an inner candidate.
			if bestStart < 0 || logicalStart <= bestLogicalStart {
				bestStart, bestEnd = start, i+1
				bestLogicalStart, bestSteps = logicalStart, span
			}
		}
		boundary[logicalEnd] = i + 1
	}
	if bestStart < 0 {
		return historyCompactionSelection{}, false
	}
	selected := append([]historyCompactionCandidate(nil), candidates[bestStart:bestEnd]...)
	return historyCompactionSelection{
		candidates:       selected,
		fromTxNum:        selected[0].history.FromTxNum,
		toTxNum:          selected[len(selected)-1].history.ToTxNum,
		aggregationSteps: bestSteps,
	}, true
}

func historyCompactionCandidates(manifest *Manifest, cfg DomainCfg) []historyCompactionCandidate {
	out := make([]historyCompactionCandidate, 0)
	for _, ref := range manifest.Segments {
		if ref.normalizedDataset() != cfg.Dataset || ref.Kind != SegmentHistory || !cfg.IsHistoryBinarySegmentPath(ref.Path) {
			continue
		}
		var companions []SegmentRef
		if cfg.HasHistoryInvertedIndex {
			idxRef, ok := cfg.HistoryIndexRef(manifest, ref)
			if !ok {
				continue
			}
			companions = append(companions, idxRef)
		}
		if cfg.HasHistoryAccessor {
			accessorRef, ok := cfg.HistoryAccessorRef(manifest, ref)
			if !ok {
				continue
			}
			companions = append(companions, accessorRef)
		}
		out = append(out, historyCompactionCandidate{
			history:    ref,
			companions: companions,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].history.FromTxNum != out[j].history.FromTxNum {
			return out[i].history.FromTxNum < out[j].history.FromTxNum
		}
		if out[i].history.ToTxNum != out[j].history.ToTxNum {
			return out[i].history.ToTxNum < out[j].history.ToTxNum
		}
		return out[i].history.Path < out[j].history.Path
	})
	return out
}

func historySegmentsAreContiguous(prev, next SegmentRef) bool {
	return prev.ToTxNum != math.MaxUint64 && next.FromTxNum == prev.ToTxNum+1
}

func deleteObsoleteHistoryCompactionFiles(dir string, candidates []historyCompactionCandidate, newRefs []SegmentRef) error {
	keep := make(map[string]struct{}, len(newRefs))
	for _, ref := range newRefs {
		if ref.Path != "" {
			keep[ref.Path] = struct{}{}
		}
	}
	// A published immutable manifest is an active downloader lease even after
	// the live manifest has retired its input segments. Compaction may unlink
	// only files that are absent from every retained catalog generation; the
	// regular retired-file pass reclaims them after the lease grace expires.
	published, err := LoadPublishedSnapshotManifests(dir)
	if err != nil {
		return fmt.Errorf("snapshots: load published manifest leases before compaction cleanup: %w", err)
	}
	for _, generation := range published {
		for _, ref := range generation.Manifest.Segments {
			keep[ref.Path] = struct{}{}
		}
	}
	for _, candidate := range candidates {
		for _, ref := range append([]SegmentRef{candidate.history}, candidate.companions...) {
			if _, ok := keep[ref.Path]; ok {
				continue
			}
			if err := os.Remove(filepath.Join(dir, ref.Path)); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("snapshots: remove obsolete segment %q: %w", ref.Path, err)
			}
		}
	}
	return nil
}

func nonZeroSegmentRefs(refs ...SegmentRef) []SegmentRef {
	out := make([]SegmentRef, 0, len(refs))
	for _, ref := range refs {
		if ref.Kind != "" || ref.Path != "" || ref.Dataset != "" {
			out = append(out, ref)
		}
	}
	return out
}

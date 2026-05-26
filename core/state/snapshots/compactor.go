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

const defaultCompactionMinSegments = 2

// CompactionConfig controls history segment compaction.
type CompactionConfig struct {
	MinSegments    int
	MaxTxSpan      uint64
	DeleteObsolete bool
}

// HistoryCompactionResult describes a registered history-domain compaction pass.
type HistoryCompactionResult struct {
	Merged         bool
	Dataset        SegmentDataset
	FromTxNum      uint64
	ToTxNum        uint64
	SegmentsMerged int
	Segments       []SegmentRef
}

type historyCompactionCandidate struct {
	history    SegmentRef
	companions []SegmentRef
}

type historyCompactionSelection struct {
	candidates []historyCompactionCandidate
	fromTxNum  uint64
	toTxNum    uint64
}

// CompactHistoryDomain merges the frontmost continuous run of binary history
// segments for a registered history domain and publishes the replacement refs.
func CompactHistoryDomain(dir string, dataset SegmentDataset, cfg CompactionConfig) (HistoryCompactionResult, error) {
	if dir == "" {
		return HistoryCompactionResult{}, errors.New("snapshots: compaction directory is empty")
	}
	minSegments := cfg.MinSegments
	if minSegments <= 1 {
		minSegments = defaultCompactionMinSegments
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
	selection, ok := selectHistoryCompactionRun(manifest, historyCfg, minSegments, cfg.MaxTxSpan)
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
			Dataset:   historyCfg.Dataset,
			Kind:      SegmentHistory,
			FromTxNum: selection.fromTxNum,
			ToTxNum:   selection.toTxNum,
			Path:      historyCfg.HistoryPath(selection.fromTxNum, selection.toTxNum),
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
		Merged:         true,
		Dataset:        historyCfg.Dataset,
		FromTxNum:      selection.fromTxNum,
		ToTxNum:        selection.toTxNum,
		SegmentsMerged: len(selection.candidates),
		Segments:       refs,
	}
	if cfg.DeleteObsolete {
		if err := deleteObsoleteHistoryCompactionFiles(dir, selection.candidates, refs); err != nil {
			return result, err
		}
	}
	return result, nil
}

func selectHistoryCompactionRun(manifest *Manifest, cfg DomainCfg, minSegments int, maxTxSpan uint64) (historyCompactionSelection, bool) {
	if manifest == nil {
		return historyCompactionSelection{}, false
	}
	candidates := historyCompactionCandidates(manifest, cfg)
	if len(candidates) < minSegments {
		return historyCompactionSelection{}, false
	}

	for start := 0; start <= len(candidates)-minSegments; start++ {
		selected := []historyCompactionCandidate{candidates[start]}
		fromTxNum := candidates[start].history.FromTxNum
		toTxNum := candidates[start].history.ToTxNum
		if txSpanExceedsMax(fromTxNum, toTxNum, maxTxSpan) {
			continue
		}
		for _, candidate := range candidates[start+1:] {
			if !historySegmentsAreContiguous(selected[len(selected)-1].history, candidate.history) {
				break
			}
			nextToTxNum := candidate.history.ToTxNum
			if txSpanExceedsMax(fromTxNum, nextToTxNum, maxTxSpan) {
				break
			}
			selected = append(selected, candidate)
			toTxNum = nextToTxNum
		}
		if len(selected) >= minSegments {
			return historyCompactionSelection{
				candidates: selected,
				fromTxNum:  fromTxNum,
				toTxNum:    toTxNum,
			}, true
		}
	}
	return historyCompactionSelection{}, false
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

func txSpanExceedsMax(fromTxNum, toTxNum, maxTxSpan uint64) bool {
	if maxTxSpan == 0 {
		return false
	}
	if toTxNum < fromTxNum {
		return true
	}
	return toTxNum-fromTxNum >= maxTxSpan
}

func deleteObsoleteHistoryCompactionFiles(dir string, candidates []historyCompactionCandidate, newRefs []SegmentRef) error {
	keep := make(map[string]struct{}, len(newRefs))
	for _, ref := range newRefs {
		if ref.Path != "" {
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

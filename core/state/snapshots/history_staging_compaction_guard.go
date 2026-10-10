package snapshots

import (
	"errors"
	"path/filepath"
	"sync"
)

// A semantic proof yields the heavy-work lease between bounded read quanta.
// Retaining physical files alone does not keep them active in the manifest.
// These narrow, process-local range claims stop a competing merge from retiring
// the exact bucket being proved, while other archive ranges remain runnable.
// Claims are ephemeral and cannot grant authentication or survive a restart.
type historyProofRange struct {
	from, to uint64
	merging  bool
}

var historyProofRanges = struct {
	sync.Mutex
	next uint64
	dirs map[string]map[uint64]historyProofRange
}{dirs: make(map[string]map[uint64]historyProofRange)}

var ErrHistoryStagingCompactionBusy = errors.New("snapshots: history proof range is being compacted")

func historyRangesOverlap(a, b historyProofRange) bool { return a.from <= b.to && b.from <= a.to }
func claimHistoryProofRange(dir string, from, to uint64, merging bool) (func(), bool, error) {
	if from > to {
		return nil, false, errors.New("snapshots: invalid history proof range")
	}
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, false, err
	}
	historyProofRanges.Lock()
	defer historyProofRanges.Unlock()
	ranges := historyProofRanges.dirs[dir]
	candidate := historyProofRange{from: from, to: to, merging: merging}
	for _, existing := range ranges {
		if (merging || existing.merging) && historyRangesOverlap(candidate, existing) {
			return nil, false, nil
		}
	}
	if ranges == nil {
		ranges = make(map[uint64]historyProofRange)
		historyProofRanges.dirs[dir] = ranges
	}
	historyProofRanges.next++
	id := historyProofRanges.next
	ranges[id] = candidate
	return sync.OnceFunc(func() {
		historyProofRanges.Lock()
		defer historyProofRanges.Unlock()
		delete(historyProofRanges.dirs[dir], id)
		if len(historyProofRanges.dirs[dir]) == 0 {
			delete(historyProofRanges.dirs, dir)
		}
	}), true, nil
}

// TryProtectHistoryStagingProofRange must precede every cooperative proof read.
// A competing merge which already owns the range wins; callers retry later.
func TryProtectHistoryStagingProofRange(dir string, from, to uint64) (func(), bool, error) {
	return claimHistoryProofRange(dir, from, to, false)
}
func filterHistoryProofProtectedCandidates(dir string, candidates []historyCompactionCandidate) []historyCompactionCandidate {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil
	}
	historyProofRanges.Lock()
	defer historyProofRanges.Unlock()
	if len(historyProofRanges.dirs[dir]) == 0 {
		return candidates
	}
	out := make([]historyCompactionCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		protected := false
		for _, r := range historyProofRanges.dirs[dir] {
			if !r.merging && historyRangesOverlap(r, historyProofRange{from: candidate.history.FromTxNum, to: candidate.history.ToTxNum}) {
				protected = true
				break
			}
		}
		if !protected {
			out = append(out, candidate)
		}
	}
	return out
}

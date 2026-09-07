package historychunk

import "sync"

const (
	bulkMinimumBytes        = 1 << 20
	bulkMaxWorkers          = 8
	bulkCandidatesPerWorker = 16 << 10
)

type bulkCandidates struct {
	positions []int
	endHash   uint64
	overflow  bool
}

// Cuts consumes p and returns offsets immediately after every completed chunk.
// An unfinished tail is excluded and remains in Pending, as with Next. Unlike
// Next, this convenience method allocates an offset list. Large inputs may scan
// Gear candidates in parallel; emitted boundaries and final state are exactly
// the scalar algorithm's, independent of worker count or Write partitioning.
// All workers finish before return and borrow p only for the duration of this call.
func (s *Splitter) Cuts(p []byte, workers int) []int {
	workers = min(bulkMaxWorkers, max(1, workers))
	if len(p) < bulkMinimumBytes || workers == 1 {
		return s.scalarCuts(p)
	}
	var results [bulkMaxWorkers]bulkCandidates
	var wg sync.WaitGroup
	initialHash := s.hash
	for i := 0; i < workers; i++ {
		stripe := len(p) / workers
		start, end := stripe*i, stripe*(i+1)
		if i == workers-1 {
			end = len(p)
		}
		wg.Add(1)
		go func(i, start, end int) {
			defer wg.Done()
			// uint64 Gear loses every bit of its initial state after 64 shifts.
			// Every admissible boundary is >=MinSize after a reset, so these
			// unreset scan candidates equal Next's hash at every admissible cut.
			h := initialHash
			if start > 0 {
				h = 0
				for _, b := range p[start-64 : start] {
					h = h<<1 + gear[b]
				}
			}
			limit := min(bulkCandidatesPerWorker, (end-start)/MinSize*4+32)
			r := &results[i]
			r.positions = make([]int, 0, limit)
			for j, b := range p[start:end] {
				h = h<<1 + gear[b]
				if h&(TargetSize-1) == 0 {
					if len(r.positions) == limit {
						r.overflow = true
						return
					}
					r.positions = append(r.positions, start+j+1)
				}
			}
			r.endHash = h
		}(i, start, end)
	}
	wg.Wait()
	for i := 0; i < workers; i++ {
		if results[i].overflow {
			return s.scalarCuts(p)
		}
	}
	return s.consumeCandidates(p, results[:workers])
}

func (s *Splitter) scalarCuts(p []byte) []int {
	var cuts []int
	for off := 0; off < len(p); {
		n, cut := s.Next(p[off:])
		off += n
		if cut {
			cuts = append(cuts, off)
		}
	}
	return cuts
}

func (s *Splitter) consumeCandidates(p []byte, groups []bulkCandidates) []int {
	initialSize := s.size
	lastCut, group, candidate := 0, 0, 0
	var cuts []int
	for {
		minimum := lastCut + max(1, MinSize-initialSize)
		maximum := lastCut + MaxSize - initialSize
		for group < len(groups) {
			if candidate == len(groups[group].positions) {
				group++
				candidate = 0
				continue
			}
			if groups[group].positions[candidate] < minimum {
				candidate++
				continue
			}
			break
		}
		next := maximum
		if group < len(groups) && groups[group].positions[candidate] <= maximum {
			next = groups[group].positions[candidate]
		}
		if next > len(p) {
			break
		}
		cuts = append(cuts, next)
		lastCut, initialSize = next, 0
	}
	s.size = initialSize + len(p) - lastCut
	if s.size == 0 {
		s.hash = 0
	} else if s.size >= 64 {
		s.hash = groups[len(groups)-1].endHash
	} else {
		// A reset in the last 63 bytes must erase the earlier scan state.
		s.hash = 0
		for _, b := range p[lastCut:] {
			s.hash = s.hash<<1 + gear[b]
		}
	}
	return cuts
}

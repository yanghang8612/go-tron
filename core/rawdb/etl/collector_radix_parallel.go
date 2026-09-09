package etl

import (
	"runtime"
	"sync/atomic"
	"time"
)

const (
	radixEntryOrderParallelMin = 128 << 10
	radixEntryOrderGroupMin    = 32 << 10
	radixEntryOrderMaxWorkers  = 4
)

func radixEntryOrderWorkers(rows int) int {
	if rows < radixEntryOrderParallelMin {
		return 1
	}
	return min(radixEntryOrderMaxWorkers, max(1, runtime.GOMAXPROCS(0)/2))
}

// A group contains whole radix buckets, never pieces of a bucket. Their key
// ranges are already disjoint and ordered, so independent sorting requires no
// merge or additional row/order array. Every group owns at least 32K rows.
func groupRadixEntryRanges(ranges []entryOrderRange, workers int) [][]entryOrderRange {
	workers = min(workers, radixEntryOrderMaxWorkers)
	if workers < 2 || len(ranges) < 2 {
		return nil
	}
	rows, largest := 0, 0
	for _, r := range ranges {
		length := r.hi - r.lo
		rows += length
		largest = max(largest, length)
	}
	if rows < radixEntryOrderParallelMin || rows-largest < radixEntryOrderGroupMin {
		return nil
	}
	workers = min(workers, rows/radixEntryOrderGroupMin)
	groups := make([][]entryOrderRange, 0, workers)
	start, grouped, target := 0, 0, rows/workers
	for i, r := range ranges {
		grouped += r.hi - r.lo
		if grouped >= target && len(groups) < workers-1 && rows-grouped >= radixEntryOrderGroupMin {
			groups = append(groups, ranges[start:i+1])
			start = i + 1
			rows -= grouped
			grouped = 0
			target = max(radixEntryOrderGroupMin, rows/(workers-len(groups)))
		}
	}
	if start < len(ranges) {
		groups = append(groups, ranges[start:])
	}
	return groups
}

// Only the caller invokes interrupted, whose closure need not be goroutine
// safe. Workers observe an internal atomic stop bit. Results never outlive this
// call: even cancellation joins every worker before pooled scratch is returned
// or the collector's arena may be reused. No worker performs any file/DB I/O.
func runParallelRadixEntryRanges(order, scratch []uint32, entries []entry, groups [][]entryOrderRange, interrupted func() bool) error {
	var stopped atomic.Bool
	check := func() bool {
		if stopped.Load() {
			return true
		}
		if interrupted != nil && interrupted() {
			stopped.Store(true)
			return true
		}
		return false
	}
	sortGroup := func(ranges []entryOrderRange, stop func() bool) {
		for _, r := range ranges {
			if _, err := radixSortEntryOrderWorkers(order, scratch, entries, r, 1, stop); err != nil {
				stopped.Store(true)
				return
			}
		}
	}
	done := make(chan struct{}, len(groups)-1)
	remaining := len(groups) - 1
	defer func() {
		// Also join if a caller-owned interruption callback panics.
		stopped.Store(true)
		for remaining > 0 {
			<-done
			remaining--
		}
	}()
	for _, group := range groups[1:] {
		go func() {
			defer func() { done <- struct{}{} }()
			sortGroup(group, stopped.Load)
		}()
	}
	sortGroup(groups[0], check)
	if interrupted == nil {
		for remaining > 0 {
			<-done
			remaining--
		}
	} else {
		// The caller can finish its small group before another worker's large
		// group. Continue observing shutdown while joining the remaining work.
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for remaining > 0 {
			select {
			case <-done:
				remaining--
			case <-ticker.C:
				check()
			}
		}
	}
	if check() {
		return ErrLoadInterrupted
	}
	return nil
}

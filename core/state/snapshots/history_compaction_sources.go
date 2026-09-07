package snapshots

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/metrics"
)

var historySourceWorkersGauge = metrics.GetOrRegisterGauge(defaultColdSnapshotMetrics+"compaction/source_workers", nil)

func historyCompactionSourceWorkers() (int, error) {
	switch value := strings.TrimSpace(os.Getenv("GTRON_HISTORY_COMPACTION_SOURCE_WORKERS")); value {
	case "", "1":
		return 1, nil
	case "2":
		return 2, nil
	default:
		return 0, fmt.Errorf("snapshots: invalid GTRON_HISTORY_COMPACTION_SOURCE_WORKERS %q (want 1 or 2)", value)
	}
}

type historySourceRead func(context.Context, historyCompactionCandidate) (stateDomainChangeBinaryCompactionSource, error)

// Source objects are independent and immutable. Preserve source/error order,
// authenticate every source before output construction, and join all readers
// before returning so failure/cancellation cannot outlive the input leases.
func collectHistorySourcesOrdered(ctx context.Context, candidates []historyCompactionCandidate, workers int, read historySourceRead, progress func(uint64)) ([]stateDomainChangeBinaryCompactionSource, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if workers != 1 && workers != 2 {
		return nil, fmt.Errorf("snapshots: unsupported source worker count %d", workers)
	}
	workers = min(workers, len(candidates))
	historySourceWorkersGauge.Update(int64(workers))
	sources := make([]stateDomainChangeBinaryCompactionSource, len(candidates))
	if workers <= 1 {
		for i, candidate := range candidates {
			source, err := read(ctx, candidate)
			if err != nil {
				return nil, err
			}
			sources[i] = source
			progress(uint64(i + 1))
		}
		return sources, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	type result struct {
		done   chan struct{}
		source stateDomainChangeBinaryCompactionSource
		err    error
	}
	results := make([]result, len(candidates))
	for i := range results {
		results[i].done = make(chan struct{})
	}
	var next atomic.Int64
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				i := int(next.Add(1) - 1)
				if i >= len(candidates) {
					return
				}
				results[i].source, results[i].err = read(ctx, candidates[i])
				close(results[i].done)
				if results[i].err != nil {
					// The ordered consumer selects the earliest source error.
					return
				}
			}
		}()
	}
	for i := range results {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-results[i].done:
		}
		if results[i].err != nil {
			return nil, results[i].err
		}
		sources[i] = results[i].source
		progress(uint64(i + 1))
	}
	return sources, nil
}

func readHistoryCompactionSource(ctx context.Context, dir string, candidate historyCompactionCandidate) (stateDomainChangeBinaryCompactionSource, error) {
	var source stateDomainChangeBinaryCompactionSource
	if err := contextError(ctx); err != nil {
		return source, err
	}
	idxRef, ok := historyCompactionCompanion(candidate, SegmentInverted)
	if !ok {
		return source, fmt.Errorf("snapshots: state-domain-change history %q missing index companion", candidate.history.Path)
	}
	accessorRef, ok := historyCompactionCompanion(candidate, SegmentAccessor)
	if !ok {
		return source, fmt.Errorf("snapshots: state-domain-change history %q missing accessor companion", candidate.history.Path)
	}
	// Keep the existing complete history checksum and sidecar structural gates.
	// The later payload copy still decodes/validates every canonical record and
	// rebuilds its derived sidecars; installation/pruning retain full coverage.
	if err := checkStateDomainChangeBinaryCompactionSource(ctx, dir, candidate.history, idxRef, accessorRef); err != nil {
		return source, err
	}
	f, header, size, err := openStateDomainChangeBinarySegmentReader(dir, candidate.history)
	if err != nil {
		return source, err
	}
	count, offset, err := stateDomainChangeBinaryTxRangeTableBoundsAt(f, size, candidate.history, header)
	closeErr := f.Close()
	if err != nil {
		return source, err
	}
	if closeErr != nil {
		return source, closeErr
	}
	return stateDomainChangeBinaryCompactionSource{history: candidate.history, accessor: accessorRef,
		segmentHeader: header, segmentSize: size, txRangeCount: count, recordOffset: offset}, nil
}

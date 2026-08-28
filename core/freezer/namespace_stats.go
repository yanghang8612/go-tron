package freezer

import "time"

// Size observations must not turn a successful freeze into an unbounded scan
// of a catch-up backlog. Limit rows and logical bytes as well as elapsed time:
// the time limit is soft because a single DB iterator operation cannot be
// interrupted. The byte limit may be exceeded by at most the final row. No
// iterator or snapshot survives a sample, so this cannot pin old SST versions
// between freezer passes.
const (
	hotBlockSizeMaxRows     = uint64(1024)
	hotBlockSizeMaxBytes    = uint64(8 << 20)
	hotBlockSizeMaxDuration = 25 * time.Millisecond
)

type namespaceSizeBudget struct {
	rows     uint64
	bytes    uint64
	duration time.Duration
}

// hotBlockSizeSample is immutable after publication. Publishing one pointer
// keeps size, completeness, row count and the observation time consistent in
// concurrent Runner.Snapshot calls.
type hotBlockSizeSample struct {
	bytes     uint64
	rows      uint64
	complete  bool
	sampledAt time.Time
}

func (r *Runner) sampleHotBlockNamespaceSize() error {
	sample, err := r.scanHotBlockNamespaceSize(namespaceSizeBudget{
		rows: hotBlockSizeMaxRows, bytes: hotBlockSizeMaxBytes, duration: hotBlockSizeMaxDuration,
	})
	if err != nil {
		return err
	}
	r.pebbleSizeSample.Store(sample)
	return nil
}

// scanHotBlockNamespaceSize reads one bounded prefix from one iterator view.
// A complete sample is the logical b-* size at that view; an incomplete sample
// is only a lower bound, never an extrapolation from the sampled rows. Budgets
// include iterator creation time, and are checked before every Next call.
// Reaching a budget exactly is conservatively incomplete without probing one
// extra row. Storage errors and cancellation discard the entire new sample.
func (r *Runner) scanHotBlockNamespaceSize(budget namespaceSizeBudget) (*hotBlockSizeSample, error) {
	if err := r.checkStopping(); err != nil {
		return nil, err
	}
	sample := &hotBlockSizeSample{sampledAt: time.Now()}
	it := r.chain.DB().NewIterator(blockNamespacePrefix, nil)
	defer it.Release()
	for {
		if err := r.checkStopping(); err != nil {
			return nil, err
		}
		if err := it.Error(); err != nil {
			return nil, err
		}
		if sample.rows >= budget.rows || sample.bytes >= budget.bytes || time.Since(sample.sampledAt) >= budget.duration {
			return sample, nil
		}
		if !it.Next() {
			if err := it.Error(); err != nil {
				return nil, err
			}
			if err := r.checkStopping(); err != nil {
				return nil, err
			}
			sample.complete = true
			return sample, nil
		}
		sample.bytes += uint64(len(it.Key()) + len(it.Value()))
		sample.rows++
	}
}

package domains

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

var (
	commitmentFoldCallsCounter           = metrics.NewRegisteredCounter("state/commitment/fold/calls", nil)
	commitmentFoldInputUpdatesCounter    = metrics.NewRegisteredCounter("state/commitment/fold/input_updates", nil)
	commitmentFoldResolvedOpsCounter     = metrics.NewRegisteredCounter("state/commitment/fold/resolved_ops", nil)
	commitmentFoldWallNanosCounter       = metrics.NewRegisteredCounter("state/commitment/fold/wall_nanos", nil)
	commitmentFoldErrorsCounter          = metrics.NewRegisteredCounter("state/commitment/fold/errors", nil)
	commitmentFoldChangedCounter         = metrics.NewRegisteredCounter("state/commitment/fold/changed", nil)
	commitmentFoldUnchangedCounter       = metrics.NewRegisteredCounter("state/commitment/fold/unchanged", nil)
	commitmentFoldParallelCallsCounter   = metrics.NewRegisteredCounter("state/commitment/fold/parallel/calls", nil)
	commitmentFoldParallelSplitsCounter  = metrics.NewRegisteredCounter("state/commitment/fold/parallel/active_splits", nil)
	commitmentFoldParallelWorkersCounter = metrics.NewRegisteredCounter(
		"state/commitment/fold/parallel/workers", nil,
	)
	commitmentFoldNodeHashesCounter         = metrics.NewRegisteredCounter("state/commitment/fold/node_hashes", nil)
	commitmentFoldNodeHashBytesCounter      = metrics.NewRegisteredCounter("state/commitment/fold/node_hash_bytes", nil)
	commitmentFoldNodeHashRoundsCounter     = metrics.NewRegisteredCounter("state/commitment/fold/node_hash_keccak_rounds", nil)
	commitmentFoldNodeHashOneRoundCounter   = metrics.NewRegisteredCounter("state/commitment/fold/node_hash_one_round", nil)
	commitmentFoldNodeHashMultiRoundCounter = metrics.NewRegisteredCounter(
		"state/commitment/fold/node_hash_multi_round", nil,
	)
	commitmentPipelineEnabledGauge         = metrics.NewRegisteredGauge("state/commitment/pipeline/enabled", nil)
	commitmentPipelineJobsCounter          = metrics.NewRegisteredCounter("state/commitment/pipeline/jobs", nil)
	commitmentPipelineErrorsCounter        = metrics.NewRegisteredCounter("state/commitment/pipeline/errors", nil)
	commitmentPipelineInflightGauge        = metrics.NewRegisteredGauge("state/commitment/pipeline/inflight", nil)
	commitmentPipelineMaxInflightGauge     = metrics.NewRegisteredGauge("state/commitment/pipeline/max_inflight", nil)
	commitmentBranchBaseOpenCounter        = metrics.NewRegisteredCounter("state/commitment/branch_base/opens", nil)
	commitmentBranchDeltaHitCounter        = metrics.NewRegisteredCounter("state/commitment/branch_base/delta_hits", nil)
	commitmentBranchTombstoneCounter       = metrics.NewRegisteredCounter("state/commitment/branch_base/tombstones", nil)
	commitmentBranchColdHitCounter         = metrics.NewRegisteredCounter("state/commitment/branch_base/cold_hits", nil)
	commitmentBranchColdMissCounter        = metrics.NewRegisteredCounter("state/commitment/branch_base/cold_misses", nil)
	commitmentBranchRotationOpenCounter    = metrics.NewRegisteredCounter("state/commitment/branch_rotation/opens", nil)
	commitmentBranchLegacyHitCounter       = metrics.NewRegisteredCounter("state/commitment/branch_rotation/legacy_hits", nil)
	commitmentBranchLegacyMissCounter      = metrics.NewRegisteredCounter("state/commitment/branch_rotation/legacy_misses", nil)
	commitmentBranchFrozenDeltaHitCounter  = metrics.NewRegisteredCounter("state/commitment/branch_rotation/frozen_delta_hits", nil)
	commitmentBranchFrozenDeltaMissCounter = metrics.NewRegisteredCounter("state/commitment/branch_rotation/frozen_delta_misses", nil)
	commitmentBranchFrozenTombstoneCounter = metrics.NewRegisteredCounter("state/commitment/branch_rotation/frozen_tombstones", nil)
)

var commitmentPipelineMaxInflight atomic.Int64

// commitmentFoldStats is owned by one Fold invocation. Parallel root workers
// write to separate sibling instances and the caller merges them after Wait,
// keeping the node-hash hot path free of atomics. Only publish performs the
// process-wide counter updates, once per completed Fold.
type commitmentFoldStats struct {
	started          time.Time
	inputUpdates     uint64
	resolvedOps      uint64
	changed          bool
	parallelCalls    uint64
	parallelSplits   uint64
	parallelWorkers  uint64
	nodeHashes       uint64
	nodeHashBytes    uint64
	nodeHashRounds   uint64
	oneRoundHashes   uint64
	multiRoundHashes uint64
}

var commitmentFoldStatsPool = sync.Pool{
	New: func() any { return new(commitmentFoldStats) },
}

type commitmentSiblingFoldStats [maxFoldNibbles]commitmentFoldStats

var commitmentSiblingFoldStatsPool = sync.Pool{
	New: func() any { return new(commitmentSiblingFoldStats) },
}

func beginCommitmentFoldStats(inputUpdates int) *commitmentFoldStats {
	s := commitmentFoldStatsPool.Get().(*commitmentFoldStats)
	*s = commitmentFoldStats{
		started:      time.Now(),
		inputUpdates: uint64(inputUpdates),
	}
	return s
}

func borrowCommitmentSiblingFoldStats() *commitmentSiblingFoldStats {
	s := commitmentSiblingFoldStatsPool.Get().(*commitmentSiblingFoldStats)
	*s = commitmentSiblingFoldStats{}
	return s
}

func returnCommitmentSiblingFoldStats(s *commitmentSiblingFoldStats) {
	if s != nil {
		commitmentSiblingFoldStatsPool.Put(s)
	}
}

func (s *commitmentFoldStats) observeNodeHashPreimage(preimageBytes uint64) {
	if s == nil {
		return
	}
	// The preimage is one domain byte plus one nibble and one 32-byte hash per
	// present child. Keccak-256 has a 136-byte rate; because these lengths can
	// never equal an exact multiple of 136, ceiling division is also the exact
	// number of permutations including the padded tail.
	rounds := (preimageBytes + 135) / 136
	s.nodeHashes++
	s.nodeHashBytes += preimageBytes
	s.nodeHashRounds += rounds
	if rounds == 1 {
		s.oneRoundHashes++
	} else {
		s.multiRoundHashes++
	}
}

func (s *commitmentFoldStats) merge(other *commitmentFoldStats) {
	if s == nil || other == nil {
		return
	}
	s.nodeHashes += other.nodeHashes
	s.nodeHashBytes += other.nodeHashBytes
	s.nodeHashRounds += other.nodeHashRounds
	s.oneRoundHashes += other.oneRoundHashes
	s.multiRoundHashes += other.multiRoundHashes
}

func finishCommitmentFoldStats(s *commitmentFoldStats, failed bool) {
	if s == nil {
		return
	}
	commitmentFoldCallsCounter.Inc(1)
	commitmentFoldInputUpdatesCounter.Inc(int64(s.inputUpdates))
	commitmentFoldResolvedOpsCounter.Inc(int64(s.resolvedOps))
	commitmentFoldWallNanosCounter.Inc(time.Since(s.started).Nanoseconds())
	if failed {
		commitmentFoldErrorsCounter.Inc(1)
	} else if s.changed {
		commitmentFoldChangedCounter.Inc(1)
	} else {
		commitmentFoldUnchangedCounter.Inc(1)
	}
	commitmentFoldParallelCallsCounter.Inc(int64(s.parallelCalls))
	commitmentFoldParallelSplitsCounter.Inc(int64(s.parallelSplits))
	commitmentFoldParallelWorkersCounter.Inc(int64(s.parallelWorkers))
	commitmentFoldNodeHashesCounter.Inc(int64(s.nodeHashes))
	commitmentFoldNodeHashBytesCounter.Inc(int64(s.nodeHashBytes))
	commitmentFoldNodeHashRoundsCounter.Inc(int64(s.nodeHashRounds))
	commitmentFoldNodeHashOneRoundCounter.Inc(int64(s.oneRoundHashes))
	commitmentFoldNodeHashMultiRoundCounter.Inc(int64(s.multiRoundHashes))
	commitmentFoldStatsPool.Put(s)
}

func observeCommitmentPipelineSubmit(inflight int64) {
	commitmentPipelineJobsCounter.Inc(1)
	commitmentPipelineInflightGauge.Update(inflight)
	for {
		previous := commitmentPipelineMaxInflight.Load()
		if inflight <= previous || commitmentPipelineMaxInflight.CompareAndSwap(previous, inflight) {
			break
		}
	}
	commitmentPipelineMaxInflightGauge.Update(commitmentPipelineMaxInflight.Load())
}

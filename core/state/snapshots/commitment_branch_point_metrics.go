package snapshots

import (
	"os"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

var (
	commitmentBranchPointPhysicalCallsCounter = metrics.NewRegisteredCounter(
		"state/snapshot/commitment_branch/point_read/physical/calls", nil,
	)
	commitmentBranchPointPhysicalBytesCounter = metrics.NewRegisteredCounter(
		"state/snapshot/commitment_branch/point_read/physical/bytes", nil,
	)
	commitmentBranchPointPhysicalNanosCounter = metrics.NewRegisteredCounter(
		"state/snapshot/commitment_branch/point_read/physical/nanos", nil,
	)
	commitmentBranchPointPhysicalErrorsCounter = metrics.NewRegisteredCounter(
		"state/snapshot/commitment_branch/point_read/physical/errors", nil,
	)
	commitmentBranchPointPhysicalShortReadsCounter = metrics.NewRegisteredCounter(
		"state/snapshot/commitment_branch/point_read/physical/short_reads", nil,
	)
	commitmentBranchPointLocalitySamplesCounter = metrics.NewRegisteredCounter(
		"state/snapshot/commitment_branch/point_read/locality/samples", nil,
	)
	commitmentBranchPointLocalitySameBlockCounter = metrics.NewRegisteredCounter(
		"state/snapshot/commitment_branch/point_read/locality/same_block", nil,
	)
	commitmentBranchPointLocalityAdjacentBlockCounter = metrics.NewRegisteredCounter(
		"state/snapshot/commitment_branch/point_read/locality/adjacent_block", nil,
	)
	commitmentBranchPointLocalityOffsetJumpBytesCounter = metrics.NewRegisteredCounter(
		"state/snapshot/commitment_branch/point_read/locality/offset_jump_bytes", nil,
	)
)

const commitmentBranchPointMetricLanes = 17 // sixteen first nibbles plus root/other

type commitmentBranchPointReadLane struct {
	lastBlockPlusOne atomic.Uint64
	observationCount atomic.Uint64
}

// commitmentBranchPointReadMetrics keeps locality state per first-nibble lane.
// Ordered commitment gives each normal lane one owner, while atomics preserve
// diagnostic correctness for concurrent API callers without putting a mutex in
// front of ReadAt. The resulting order is lane-local within one immutable view.
type commitmentBranchPointReadMetrics struct {
	lanes      [commitmentBranchPointMetricLanes]commitmentBranchPointReadLane
	sampleSeed uint64
}

func newCommitmentBranchPointReadMetrics(txNum, fileSize uint64) commitmentBranchPointReadMetrics {
	seed := commitmentBranchPointMix64(txNum) ^ commitmentBranchPointMix64(fileSize+0x9e3779b97f4a7c15)
	return commitmentBranchPointReadMetrics{sampleSeed: seed}
}

func commitmentBranchPointMetricLane(prefix []byte) int {
	if len(prefix) > 0 && prefix[0] < 16 {
		return int(prefix[0])
	}
	return commitmentBranchPointMetricLanes - 1
}

const commitmentBranchPointLocalitySampleMask = 15

func commitmentBranchPointMix64(value uint64) uint64 {
	value ^= value >> 30
	value *= 0xbf58476d1ce4e5b9
	value ^= value >> 27
	value *= 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func commitmentBranchPointShouldSample(ordinal, seed uint64, lane int) bool {
	value := ordinal ^ seed ^ (uint64(lane+1) * 0x9e3779b97f4a7c15)
	return commitmentBranchPointMix64(value)&commitmentBranchPointLocalitySampleMask == 0
}

func (m *commitmentBranchPointReadMetrics) readAt(file *os.File, dst []byte, offset int64, block int, entries []latestBinaryBTreeEntry, prefix []byte) (int, error) {
	laneIndex := commitmentBranchPointMetricLane(prefix)
	lane := &m.lanes[laneIndex]
	previousBlock := lane.lastBlockPlusOne.Swap(uint64(block) + 1)
	sampleLocality := commitmentBranchPointShouldSample(lane.observationCount.Add(1), m.sampleSeed, laneIndex) && previousBlock != 0
	started := time.Now()
	n, err := file.ReadAt(dst, offset)
	elapsed := time.Since(started)

	commitmentBranchPointPhysicalCallsCounter.Inc(1)
	commitmentBranchPointPhysicalBytesCounter.Inc(int64(n))
	commitmentBranchPointPhysicalNanosCounter.Inc(elapsed.Nanoseconds())
	if err != nil {
		commitmentBranchPointPhysicalErrorsCounter.Inc(1)
	}
	if n != len(dst) {
		commitmentBranchPointPhysicalShortReadsCounter.Inc(1)
	}

	if sampleLocality {
		previousIndex := int(previousBlock - 1)
		if previousIndex < 0 || previousIndex >= len(entries) {
			return n, err
		}
		blockJump := int64(block) - int64(previousBlock-1)
		if blockJump < 0 {
			blockJump = -blockJump
		}
		offsetJump := offset - int64(entries[previousIndex].segmentOffset)
		if offsetJump < 0 {
			offsetJump = -offsetJump
		}
		commitmentBranchPointLocalitySamplesCounter.Inc(1)
		commitmentBranchPointLocalityOffsetJumpBytesCounter.Inc(offsetJump)
		if blockJump == 0 {
			commitmentBranchPointLocalitySameBlockCounter.Inc(1)
		}
		if blockJump == 1 {
			commitmentBranchPointLocalityAdjacentBlockCounter.Inc(1)
		}
	}
	return n, err
}

package snapshots

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/metrics"
)

func TestCommitmentBranchPointReadMetricsPhysicalAndLaneLocality(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "commitment-point-*.seg")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.Write(make([]byte, 32)); err != nil {
		t.Fatal(err)
	}

	counters := map[string]*metrics.Counter{
		"calls":             commitmentBranchPointPhysicalCallsCounter,
		"bytes":             commitmentBranchPointPhysicalBytesCounter,
		"errors":            commitmentBranchPointPhysicalErrorsCounter,
		"short reads":       commitmentBranchPointPhysicalShortReadsCounter,
		"locality samples":  commitmentBranchPointLocalitySamplesCounter,
		"same block":        commitmentBranchPointLocalitySameBlockCounter,
		"adjacent block":    commitmentBranchPointLocalityAdjacentBlockCounter,
		"offset jump bytes": commitmentBranchPointLocalityOffsetJumpBytesCounter,
	}
	before := make(map[string]int64, len(counters))
	for name, counter := range counters {
		before[name] = counter.Snapshot().Count()
	}

	entries := []latestBinaryBTreeEntry{
		{segmentOffset: 0},
		{segmentOffset: 4},
		{segmentOffset: 12},
		{segmentOffset: 20},
		{segmentOffset: 30},
	}
	var observed commitmentBranchPointReadMetrics
	setNextSample := func(lane int, want bool) {
		sequence := observed.lanes[lane].observationCount.Load() + 1
		for commitmentBranchPointShouldSample(sequence, observed.sampleSeed, lane) != want {
			sequence++
		}
		observed.lanes[lane].observationCount.Store(sequence - 1)
	}
	setNextSample(3, false)
	if n, err := observed.readAt(file, make([]byte, 4), 0, 0, entries, []byte{3}); err != nil || n != 4 {
		t.Fatalf("initial readAt = %d, %v", n, err)
	}
	for _, read := range []struct {
		offset int64
		block  int
	}{{0, 0}, {4, 1}, {20, 3}} {
		setNextSample(3, true)
		if n, err := observed.readAt(file, make([]byte, 4), read.offset, read.block, entries, []byte{3}); err != nil || n != 4 {
			t.Fatalf("readAt(%d, %d) = %d, %v", read.offset, read.block, n, err)
		}
	}
	if n, err := observed.readAt(file, make([]byte, 4), 30, 4, entries, []byte{3}); n != 2 || !errors.Is(err, io.EOF) {
		t.Fatalf("short readAt = %d, %v", n, err)
	}

	want := map[string]int64{
		"calls":             5,
		"bytes":             18,
		"errors":            1,
		"short reads":       1,
		"locality samples":  3,
		"same block":        1,
		"adjacent block":    1,
		"offset jump bytes": 20,
	}
	for name, counter := range counters {
		if got := counter.Snapshot().Count() - before[name]; got != want[name] {
			t.Fatalf("%s delta = %d, want %d", name, got, want[name])
		}
	}
}

func TestCommitmentBranchPointLocalitySamplingHasNoFixedLanePhase(t *testing.T) {
	metrics := newCommitmentBranchPointReadMetrics(1234, 5678)
	samples := func(lane int) []uint64 {
		var result []uint64
		for ordinal := uint64(1); ordinal <= 256; ordinal++ {
			if commitmentBranchPointShouldSample(ordinal, metrics.sampleSeed, lane) {
				result = append(result, ordinal)
			}
		}
		return result
	}
	first := samples(1)
	second := samples(2)
	if len(first) < 2 || len(second) < 2 {
		t.Fatalf("insufficient samples: %v / %v", first, second)
	}
	if first[0] == second[0] && first[1] == second[1] {
		t.Fatalf("lane seeds kept the same sample phase: %v / %v", first[:2], second[:2])
	}
	allFixed := true
	for i := 1; i < len(first); i++ {
		if first[i]-first[i-1] != commitmentBranchPointLocalitySampleMask+1 {
			allFixed = false
			break
		}
	}
	if allFixed {
		t.Fatalf("sampling retained a fixed 16-call phase: %v", first)
	}
}

func TestCommitmentBranchPointReadMetricsDoNotMixLanesOrRoot(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "commitment-lanes-*.seg")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.Write(make([]byte, 16)); err != nil {
		t.Fatal(err)
	}
	entries := []latestBinaryBTreeEntry{{segmentOffset: 0}, {segmentOffset: 4}, {segmentOffset: 8}}
	beforeSamples := commitmentBranchPointLocalitySamplesCounter.Snapshot().Count()
	beforeSame := commitmentBranchPointLocalitySameBlockCounter.Snapshot().Count()
	beforeAdjacent := commitmentBranchPointLocalityAdjacentBlockCounter.Snapshot().Count()
	beforeJump := commitmentBranchPointLocalityOffsetJumpBytesCounter.Snapshot().Count()

	var observed commitmentBranchPointReadMetrics
	setNextSample := func(lane int, want bool) {
		sequence := observed.lanes[lane].observationCount.Load() + 1
		for commitmentBranchPointShouldSample(sequence, observed.sampleSeed, lane) != want {
			sequence++
		}
		observed.lanes[lane].observationCount.Store(sequence - 1)
	}
	setNextSample(1, false)
	setNextSample(2, false)
	setNextSample(commitmentBranchPointMetricLanes-1, false)
	read := func(offset int64, block int, prefix []byte) {
		t.Helper()
		if n, err := observed.readAt(file, make([]byte, 4), offset, block, entries, prefix); err != nil || n != 4 {
			t.Fatalf("readAt(%d, %d, %v) = %d, %v", offset, block, prefix, n, err)
		}
	}
	read(0, 0, []byte{1})
	read(8, 2, []byte{2})
	read(0, 0, nil)
	setNextSample(1, true)
	setNextSample(2, true)
	setNextSample(commitmentBranchPointMetricLanes-1, true)
	read(0, 0, []byte{1}) // same block in lane 1
	read(4, 1, []byte{2}) // adjacent to lane 2's block 2, not lane 1's block 0
	read(4, 1, nil)       // root/other has independent locality state

	if got := commitmentBranchPointLocalitySamplesCounter.Snapshot().Count() - beforeSamples; got != 3 {
		t.Fatalf("locality sample delta = %d, want 3", got)
	}
	if got := commitmentBranchPointLocalitySameBlockCounter.Snapshot().Count() - beforeSame; got != 1 {
		t.Fatalf("same-block delta = %d, want 1", got)
	}
	if got := commitmentBranchPointLocalityAdjacentBlockCounter.Snapshot().Count() - beforeAdjacent; got != 2 {
		t.Fatalf("adjacent-block delta = %d, want 2", got)
	}
	if got := commitmentBranchPointLocalityOffsetJumpBytesCounter.Snapshot().Count() - beforeJump; got != 8 {
		t.Fatalf("offset-jump delta = %d, want 8", got)
	}
}

package pebbledb

import (
	"math/bits"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

// These observations describe the synchronous execution stack at a completed
// SST VFS read. They do not identify an async loader's initiator or device IO.
const physicalReadCallerFrames = 96
const physicalReadCallerMask = 255
const physicalReadCallerSalt uint64 = 0x52f438ade9c760b1

var physicalReadOpenNonce atomic.Uint64

type physicalReadCallerRole uint8

const (
	callerCommitmentDemand physicalReadCallerRole = iota
	callerCommitmentPrefetch
	callerStatePrefetch
	callerPooledFlat
	callerColdBuilder
	callerPebbleCompaction
	callerUnknown
	callerTruncated
	callerAmbiguous
	callerRoleCount
)

var physicalReadCallerNames = [callerRoleCount]string{
	"commitment_demand", "commitment_prefetch", "state_prefetch", "pooled_flat_stack", "cold_builder_stack", "pebble_compaction_path", "unknown", "unknown_truncated", "unknown_ambiguous",
}

type physicalReadCallerCounter struct{ samples, bytes, nanos, errors, short *metrics.Counter }
type physicalReadCallerBuckets [callerRoleCount]physicalReadCallerCounter
type physicalReadCallerMetrics struct{ fd [3]physicalReadCallerBuckets }

func newPhysicalReadCallerMetrics(namespace string) *physicalReadCallerMetrics {
	m := new(physicalReadCallerMetrics)
	for fd, name := range [...]string{"random", "sequential", "other"} {
		for role, roleName := range physicalReadCallerNames {
			prefix := namespace + "disk/physical/read/sst/source_sample/fd/" + name + "/" + roleName + "/"
			m.fd[fd][role] = physicalReadCallerCounter{metrics.GetOrRegisterCounter(prefix+"samples", nil), metrics.GetOrRegisterCounter(prefix+"returned_bytes", nil), metrics.GetOrRegisterCounter(prefix+"readat_nanos", nil), metrics.GetOrRegisterCounter(prefix+"errors", nil), metrics.GetOrRegisterCounter(prefix+"short_reads", nil)}
		}
	}
	return m
}
func (m *physicalReadCallerMetrics) forFD(all *physicalReadMetrics, fd *physicalReadFDMetrics) *physicalReadCallerBuckets {
	if fd == &all.fdRandom {
		return &m.fd[0]
	}
	if fd == &all.fdSequential {
		return &m.fd[1]
	}
	return &m.fd[2]
}
func physicalReadCallerSeed(fileSeed, nonce uint64) uint64 {
	return physicalReadMix64(fileSeed ^ physicalReadCallerSalt ^ nonce)
}
func physicalReadCallerShouldSample(ordinal, seed uint64) bool {
	return physicalReadMix64(ordinal^seed)&physicalReadCallerMask == 0
}

// Isolated from the unsampled ReadAt path: the PC array and Frames iterator are
// temporary and never stored on files, readers, goroutines or a global map.
func (m *physicalReadCallerBuckets) observe(n int, err error, short bool, elapsed time.Duration) {
	var pcs [physicalReadCallerFrames + 1]uintptr
	count := runtime.Callers(2, pcs[:])
	role := callerTruncated
	if count <= physicalReadCallerFrames {
		role = physicalReadClassifyFrames(runtime.CallersFrames(pcs[:count]))
	}
	m.record(role, n, err, short, elapsed)
}
func (m *physicalReadCallerBuckets) record(role physicalReadCallerRole, n int, err error, short bool, elapsed time.Duration) {
	c := &m[role]
	c.samples.Inc(1)
	c.bytes.Inc(int64(n))
	c.nanos.Inc(elapsed.Nanoseconds())
	if err != nil {
		c.errors.Inc(1)
	}
	if short {
		c.short.Inc(1)
	}
}

var physicalReadColdBuilderMarkers = [...]struct{ function, closurePrefix string }{
	{"github.com/tronprotocol/go-tron/core/state/snapshots.BuildStateHistoryReferenceTrioContext", "github.com/tronprotocol/go-tron/core/state/snapshots.BuildStateHistoryReferenceTrioContext.func"},
	{"github.com/tronprotocol/go-tron/core/state/snapshots.BuildStateHistoryReferenceTrioReadContext", "github.com/tronprotocol/go-tron/core/state/snapshots.BuildStateHistoryReferenceTrioReadContext.func"},
	{"github.com/tronprotocol/go-tron/core/state/snapshots.buildStateDomainChangeHistoryBinarySegmentsFromDBRangeContextExecution", "github.com/tronprotocol/go-tron/core/state/snapshots.buildStateDomainChangeHistoryBinarySegmentsFromDBRangeContextExecution.func"},
	{"github.com/tronprotocol/go-tron/core/state/snapshots.BuildStateDomainChangeHistorySegmentFromDB", "github.com/tronprotocol/go-tron/core/state/snapshots.BuildStateDomainChangeHistorySegmentFromDB.func"},
	{"github.com/tronprotocol/go-tron/core/state/snapshots.BuildEventLogV4SegmentFromReader", "github.com/tronprotocol/go-tron/core/state/snapshots.BuildEventLogV4SegmentFromReader.func"},
	{"github.com/tronprotocol/go-tron/core/state/snapshots.BuildEventLogV4SegmentFromChain", "github.com/tronprotocol/go-tron/core/state/snapshots.BuildEventLogV4SegmentFromChain.func"},
}

type physicalReadCallerClassifier struct {
	roles         uint16
	pooled, typed bool
}

func (c *physicalReadCallerClassifier) add(name string) {
	const bb = "github.com/tronprotocol/go-tron/core/blockbuffer."
	switch name {
	case bb + "(*commitmentParentReadSession).leadCommitmentParentView":
		c.roles |= 1 << callerCommitmentDemand
	case bb + "(*commitmentParentReadSession).leadCommitmentParentPrefetch":
		c.roles |= 1 << callerCommitmentPrefetch
	case bb + "(*Buffer).PrefetchBatch":
		c.roles |= 1 << callerStatePrefetch
	case bb + "readBaseIntoCachePooledKey":
		c.pooled = true
	case bb + "(*Buffer).GetNoCopyCachedStateKVLatest", bb + "(*LayerView).GetNoCopyCachedStateKVLatest", bb + "(*Buffer).GetNoCopyCachedStateAccountLatest", bb + "(*LayerView).GetNoCopyCachedStateAccountLatest":
		c.typed = true
	case "github.com/cockroachdb/pebble.(*DB).runCompaction":
		c.roles |= 1 << callerPebbleCompaction
	}
	// Explicit builder functions and their synchronous/worker closures. Generic
	// Runner.OnePass is deliberately not a builder marker.
	for _, marker := range physicalReadColdBuilderMarkers {
		if name == marker.function || strings.HasPrefix(name, marker.closurePrefix) {
			c.roles |= 1 << callerColdBuilder
			break
		}
	}
}
func (c *physicalReadCallerClassifier) result() physicalReadCallerRole {
	if c.pooled && c.typed {
		c.roles |= 1 << callerPooledFlat
	}
	if c.roles == 0 {
		return callerUnknown
	}
	if bits.OnesCount16(c.roles) != 1 {
		return callerAmbiguous
	}
	return physicalReadCallerRole(bits.TrailingZeros16(c.roles))
}
func physicalReadClassifyFrames(frames *runtime.Frames) physicalReadCallerRole {
	var c physicalReadCallerClassifier
	for count := 0; ; count++ {
		frame, more := frames.Next()
		if count >= physicalReadCallerFrames {
			return callerTruncated
		}
		c.add(frame.Function)
		if !more {
			return c.result()
		}
	}
}

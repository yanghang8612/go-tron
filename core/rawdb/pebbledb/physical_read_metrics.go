package pebbledb

import (
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/ethereum/go-ethereum/metrics"
)

// physicalReadMetrics observes calls at Pebble's VFS boundary. For the
// default filesystem an SST ReadAt maps to the platform's positional-read
// primitive (pread on Unix), although the kernel may still satisfy it from the
// page cache. These counters deliberately do not claim that every call reached
// the storage device.
type physicalReadMetrics struct {
	calls           *metrics.Counter
	bytes           *metrics.Counter
	nanos           *metrics.Counter
	errors          *metrics.Counter
	shortReads      *metrics.Counter
	localitySamples *metrics.Counter
	sameOffset      *metrics.Counter
	offsetJumpBytes *metrics.Counter
}

func newPhysicalReadMetrics(namespace string) *physicalReadMetrics {
	prefix := namespace + "disk/physical/read/sst/"
	return &physicalReadMetrics{
		calls:           metrics.GetOrRegisterCounter(prefix+"calls", nil),
		bytes:           metrics.GetOrRegisterCounter(prefix+"bytes", nil),
		nanos:           metrics.GetOrRegisterCounter(prefix+"nanos", nil),
		errors:          metrics.GetOrRegisterCounter(prefix+"errors", nil),
		shortReads:      metrics.GetOrRegisterCounter(prefix+"short_reads", nil),
		localitySamples: metrics.GetOrRegisterCounter(prefix+"locality/samples", nil),
		sameOffset:      metrics.GetOrRegisterCounter(prefix+"locality/same_offset", nil),
		offsetJumpBytes: metrics.GetOrRegisterCounter(prefix+"locality/offset_jump_bytes", nil),
	}
}

type physicalReadFS struct {
	vfs.FS
	metrics *physicalReadMetrics
}

func newPhysicalReadFS(fs vfs.FS, namespace string) vfs.FS {
	return &physicalReadFS{FS: fs, metrics: newPhysicalReadMetrics(namespace)}
}

func (fs *physicalReadFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	file, err := fs.FS.Open(name, opts...)
	if err != nil || !strings.HasSuffix(name, ".sst") {
		return file, err
	}
	return &physicalReadFile{File: file, metrics: fs.metrics, sampleSeed: physicalReadNameSeed(name)}, nil
}

type physicalReadFile struct {
	vfs.File
	metrics *physicalReadMetrics

	// lastOffsetPlusOne uses zero as the unseen sentinel. Swap gives concurrent
	// readers of one SST a cheap total observation order without serialising the
	// actual ReadAt calls. Locality is therefore temporal/file-local, not tied to
	// a particular commitment lane.
	lastOffsetPlusOne atomic.Int64
	observationCount  atomic.Uint64
	sampleSeed        uint64
}

// Locality is diagnostic rather than accounting data. Sampling one in every
// ~64 calls through a file-seeded ordinal hash avoids a fixed phase that could
// alias a periodic Pebble access pattern, while still comparing the sampled
// call with its immediately preceding offset.
const physicalReadLocalitySampleMask = 63

func physicalReadMix64(value uint64) uint64 {
	value ^= value >> 30
	value *= 0xbf58476d1ce4e5b9
	value ^= value >> 27
	value *= 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func physicalReadNameSeed(name string) uint64 {
	seed := uint64(1469598103934665603)
	for i := 0; i < len(name); i++ {
		seed ^= uint64(name[i])
		seed *= 1099511628211
	}
	return physicalReadMix64(seed)
}

func physicalReadShouldSample(ordinal, seed uint64) bool {
	return physicalReadMix64(ordinal^seed)&physicalReadLocalitySampleMask == 0
}

func (f *physicalReadFile) ReadAt(p []byte, offset int64) (int, error) {
	previous := f.lastOffsetPlusOne.Swap(offset + 1)
	sampleLocality := physicalReadShouldSample(f.observationCount.Add(1), f.sampleSeed) && previous != 0
	started := time.Now()
	n, err := f.File.ReadAt(p, offset)
	elapsed := time.Since(started)

	f.metrics.calls.Inc(1)
	f.metrics.bytes.Inc(int64(n))
	f.metrics.nanos.Inc(elapsed.Nanoseconds())
	if err != nil {
		f.metrics.errors.Inc(1)
	}
	if n != len(p) {
		f.metrics.shortReads.Inc(1)
	}
	if sampleLocality {
		jump := offset - (previous - 1)
		if jump < 0 {
			jump = -jump
		}
		f.metrics.localitySamples.Inc(1)
		f.metrics.offsetJumpBytes.Inc(jump)
		if jump == 0 {
			f.metrics.sameOffset.Inc(1)
		}
	}
	return n, err
}

package pebbledb

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

var sourceProbeError = errors.New("source-probe-error")

type sourceProbeFile struct{ vfs.File }

func (*sourceProbeFile) ReadAt(p []byte, off int64) (int, error) {
	if len(p) > 0 {
		p[0] = byte(off)
		p[len(p)-1] = 0x7a
	}
	switch off % 4 {
	case 0:
		return len(p), nil
	case 1:
		return len(p) / 2, nil
	case 2:
		return len(p) / 2, sourceProbeError
	default:
		return len(p), sourceProbeError
	}
}
func (*sourceProbeFile) Close() error { return nil }

type sourceProbeFS struct{ vfs.FS }

func (*sourceProbeFS) Open(string, ...vfs.OpenOption) (vfs.File, error) {
	return &sourceProbeFile{}, nil
}

//go:noinline
func sourceProbeRead(f vfs.File, p []byte, off int64, depth int) (int, error) {
	if depth == 0 {
		return f.ReadAt(p, off)
	}
	return sourceProbeRead(f, p, off, depth-1)
}

func TestPhysicalReadCallerClassify(t *testing.T) {
	const bb = "github.com/tronprotocol/go-tron/core/blockbuffer."
	cases := []struct {
		name   string
		frames []string
		want   physicalReadCallerRole
	}{
		{"demand", []string{bb + "(*commitmentParentReadSession).leadCommitmentParentView"}, callerCommitmentDemand},
		{"prefetch", []string{bb + "(*commitmentParentReadSession).leadCommitmentParentPrefetch"}, callerCommitmentPrefetch},
		{"state", []string{bb + "(*Buffer).PrefetchBatch"}, callerStatePrefetch},
		{"flat", []string{bb + "readBaseIntoCachePooledKey", bb + "(*LayerView).GetNoCopyCachedStateKVLatest"}, callerPooledFlat},
		{"generic-not-flat", []string{bb + "readBaseIntoCachePooledKey", bb + "(*Buffer).GetNoCopyCachedKeyParts"}, callerUnknown},
		{"typed-only", []string{bb + "(*Buffer).GetNoCopyCachedStateKVLatest"}, callerUnknown},
		{"runner-not-builder", []string{"github.com/tronprotocol/go-tron/core/state/snapshots.(*Runner).OnePass"}, callerUnknown},
		{"builder-closure", []string{"github.com/tronprotocol/go-tron/core/state/snapshots.buildStateDomainChangeHistoryBinarySegmentsFromDBRangeContextExecution.func1"}, callerColdBuilder},
		{"compaction", []string{"github.com/cockroachdb/pebble.(*DB).runCompaction"}, callerPebbleCompaction},
		{"duplicate", []string{bb + "(*Buffer).PrefetchBatch", bb + "(*Buffer).PrefetchBatch"}, callerStatePrefetch},
		{"conflict", []string{bb + "(*Buffer).PrefetchBatch", bb + "(*commitmentParentReadSession).leadCommitmentParentView"}, callerAmbiguous},
		{"synchronous-loader-preserves-marker", []string{"github.com/cockroachdb/pebble.(*tableCacheValue).load", bb + "(*Buffer).PrefetchBatch"}, callerStatePrefetch},
		{"loader-without-marker", []string{"github.com/cockroachdb/pebble.(*tableCacheValue).load"}, callerUnknown},
		{"foreign-suffix", []string{"untrusted." + bb + "(*Buffer).PrefetchBatch"}, callerUnknown},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var c physicalReadCallerClassifier
			for _, f := range tt.frames {
				c.add(f)
			}
			if got := c.result(); got != tt.want {
				t.Fatalf("role=%v want%v", got, tt.want)
			}
		})
	}
	var pc [physicalReadCallerFrames + 1]uintptr
	n := runtime.Callers(1, pc[:])
	if got := physicalReadClassifyFrames(runtime.CallersFrames(pc[:n])); got != callerUnknown {
		t.Fatalf("unit stack role%v", got)
	}
}
func sourceCallerTotals(b *physicalReadCallerBuckets) (samples, bytes, ns, errors, short int64) {
	for _, c := range b {
		samples += c.samples.Snapshot().Count()
		bytes += c.bytes.Snapshot().Count()
		ns += c.nanos.Snapshot().Count()
		errors += c.errors.Snapshot().Count()
		short += c.short.Snapshot().Count()
	}
	return
}
func callerForceNextSample(f *physicalReadFile) {
	for seed := uint64(1); ; seed++ {
		if physicalReadCallerShouldSample(f.observationCount.Load()+1, seed) {
			f.callerSeed = seed
			return
		}
	}
}
func TestPhysicalReadCallerReturnedResultsAndFirstRead(t *testing.T) {
	fs := newPhysicalReadFS(&sourceProbeFS{}, t.Name()+"/").(*physicalReadFS)
	for i, opts := range [][]vfs.OpenOption{{vfs.RandomReadsOption}, {vfs.SequentialReadsOption}, nil} {
		f, err := fs.Open("one.sst", opts...)
		if err != nil {
			t.Fatal(err)
		}
		pf := f.(*physicalReadFile)
		prior := pf.sampleSeed
		for j := 0; j < 4; j++ {
			callerForceNextSample(pf)
			buf := []byte{0xcc, 0xcc, 0xcc, 0xcc}
			n, err := pf.ReadAt(buf, int64(j))
			wantN := 4
			if j == 1 || j == 2 {
				wantN = 2
			}
			if n != wantN || (j < 2 && err != nil) || (j >= 2 && err != sourceProbeError) || buf[0] != byte(j) || buf[1] != 0xcc || buf[3] != 0x7a {
				t.Fatalf("nerrbuffer=%d,%v,%x", n, err, buf)
			}
			if pf.sampleSeed != prior {
				t.Fatal("locality seed changed")
			}
		}
		samples, bytes, _, errs, short := sourceCallerTotals(&fs.metrics.caller.fd[i])
		if samples != 4 || bytes != 12 || errs != 2 || short != 2 {
			t.Fatalf("totals=%d/%d/%d/%d", samples, bytes, errs, short)
		}
		f.Close()
	}
	if fs.metrics.calls.Snapshot().Count() != 12 || fs.metrics.bytes.Snapshot().Count() != 36 || fs.metrics.errors.Snapshot().Count() != 6 || fs.metrics.shortReads.Snapshot().Count() != 6 {
		t.Fatal("original exact totals changed")
	}
}
func TestPhysicalReadCallerNonceAndLocality(t *testing.T) {
	fs := newPhysicalReadFS(&sourceProbeFS{}, t.Name()+"/")
	a, _ := fs.Open("same.sst")
	b, _ := fs.Open("same.sst")
	defer a.Close()
	defer b.Close()
	x, y := a.(*physicalReadFile), b.(*physicalReadFile)
	if x.sampleSeed != y.sampleSeed || x.callerSeed == y.callerSeed {
		t.Fatal("nonce/locality contract")
	}
	n := 0
	for i := uint64(1); i <= 65536; i++ {
		if physicalReadCallerShouldSample(i, x.callerSeed) {
			n++
		}
		if physicalReadShouldSample(i, x.sampleSeed) != (physicalReadMix64(i^physicalReadNameSeed("same.sst"))&63 == 0) {
			t.Fatal("old locality changed")
		}
	}
	if n < 128 || n > 384 {
		t.Fatalf("fixed sample count unreasonable:%d", n)
	}
}
func TestPhysicalReadCallerUnsampledAlloc(t *testing.T) {
	fs := newPhysicalReadFS(&sourceProbeFS{}, t.Name()+"/")
	f, _ := fs.Open("one.sst")
	defer f.Close()
	pf := f.(*physicalReadFile)
	p := make([]byte, 4096)
	seed := uint64(1)
	for physicalReadCallerShouldSample(1, seed) {
		seed++
	}
	pf.callerSeed = seed
	if n := testing.AllocsPerRun(1000, func() { pf.observationCount.Store(0); pf.ReadAt(p, 0) }); n != 0 {
		t.Fatalf("unsampled allocs=%v", n)
	}
}
func TestPhysicalReadCallerTruncationAndConcurrentCounts(t *testing.T) {
	fs := newPhysicalReadFS(&sourceProbeFS{}, t.Name()+"/").(*physicalReadFS)
	f, _ := fs.Open("one.sst")
	pf := f.(*physicalReadFile)
	defer f.Close()
	callerForceNextSample(pf)
	sourceProbeRead(f, make([]byte, 32), 0, 112)
	if got := fs.metrics.caller.fd[2][callerTruncated].samples.Snapshot().Count(); got != 1 {
		t.Fatalf("deep stack samples=%d", got)
	}
	var wg sync.WaitGroup
	before, _, _, _, _ := sourceCallerTotals(pf.callerMetrics)
	// Direct record tests atomic bucket accounting; concurrent ReadAt selection
	// independently uses a single per-file atomic ordinal, not mutable role tags.
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := make([]byte, 4)
			for j := 0; j < 512; j++ {
				pf.ReadAt(p, 0)
				pf.callerMetrics.record(callerUnknown, 4, nil, false, time.Nanosecond)
			}
		}()
	}
	wg.Wait()
	after, _, _, _, _ := sourceCallerTotals(pf.callerMetrics)
	if after-before < 4096 {
		t.Fatal(fmt.Sprint("lost concurrent samples ", after-before))
	}
}

// SourceAttributionTestFS exists only in the test binary. Selecting every test
// read verifies stack coverage, not performance or production sample rate.
func SourceAttributionTestFS(fs vfs.FS, namespace string) vfs.FS {
	return &sourceAllTestFS{FS: newPhysicalReadFS(fs, namespace)}
}

type sourceAllTestFS struct{ vfs.FS }

func (fs *sourceAllTestFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name, opts...)
	if err != nil {
		return f, err
	}
	if pf, ok := f.(*physicalReadFile); ok {
		return &sourceAllTestFile{File: f, physical: pf}, nil
	}
	return f, nil
}

type sourceAllTestFile struct {
	vfs.File
	physical *physicalReadFile
	mu       sync.Mutex
}

func (f *sourceAllTestFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	callerForceNextSample(f.physical)
	return f.File.ReadAt(p, off)
}

// SourceAttributionTestCounts returns copies of aggregate test counts only.
func SourceAttributionTestCounts(fs vfs.FS) map[string]int64 {
	inner := fs.(*sourceAllTestFS).FS.(*physicalReadFS)
	out := map[string]int64{}
	for _, fd := range inner.metrics.caller.fd {
		for role, c := range fd {
			out[physicalReadCallerNames[role]] += c.samples.Snapshot().Count()
		}
	}
	return out
}

func sourceInlineCapableWrapper(fn func()) { fn() }
func TestPhysicalReadCallerInlineFrames(t *testing.T) {
	found := false
	sourceInlineCapableWrapper(func() {
		var p [32]uintptr
		n := runtime.Callers(1, p[:])
		f := runtime.CallersFrames(p[:n])
		for {
			v, more := f.Next()
			if v.Function == "github.com/tronprotocol/go-tron/core/rawdb/pebbledb.sourceInlineCapableWrapper" {
				found = true
			}
			if !more {
				break
			}
		}
	})
	if !found {
		t.Fatal("CallersFrames lost inline-capable wrapper")
	}
}

func TestPhysicalReadCallerClassifierAlloc(t *testing.T) {
	if n := testing.AllocsPerRun(1000, func() { var c physicalReadCallerClassifier; c.add("github.com/none/none.none") }); n != 0 {
		t.Fatalf("classifier allocations/frame=%v", n)
	}
}

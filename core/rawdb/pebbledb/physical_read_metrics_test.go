package pebbledb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/ethereum/go-ethereum/metrics"
)

func TestPhysicalReadFSObservesSSTReadAtAndLocality(t *testing.T) {
	fs := vfs.NewMem()
	file, err := fs.Create("000001.sst")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	namespace := "test/physical_read/"
	observed := newPhysicalReadFS(fs, namespace)
	opened, err := observed.Open("000001.sst")
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()

	physical := opened.(*physicalReadFile)
	setNextSample := func(want bool) {
		sequence := physical.observationCount.Load() + 1
		for physicalReadShouldSample(sequence, physical.sampleSeed) != want {
			sequence++
		}
		physical.observationCount.Store(sequence - 1)
	}
	setNextSample(false)
	if n, err := physical.ReadAt(make([]byte, 4), 0); err != nil || n != 4 {
		t.Fatalf("initial ReadAt = %d, %v", n, err)
	}
	for _, offset := range []int64{0, 4, 20} {
		setNextSample(true)
		if n, err := opened.ReadAt(make([]byte, 4), offset); err != nil || n != 4 {
			t.Fatalf("ReadAt(%d) = %d, %v", offset, n, err)
		}
	}
	if n, err := opened.ReadAt(make([]byte, 4), 30); n != 2 || !errors.Is(err, io.EOF) {
		t.Fatalf("short ReadAt = %d, %v", n, err)
	}

	m := observed.(*physicalReadFS).metrics
	assertCounter := func(name string, counter *metrics.Counter, want int64) {
		t.Helper()
		if got := counter.Snapshot().Count(); got != want {
			t.Fatalf("%s = %d, want %d", name, got, want)
		}
	}
	assertCounter("calls", m.calls, 5)
	assertCounter("bytes", m.bytes, 18)
	assertCounter("errors", m.errors, 1)
	assertCounter("short reads", m.shortReads, 1)
	assertCounter("locality samples", m.localitySamples, 3)
	assertCounter("same offset", m.sameOffset, 1)
	assertCounter("offset jump bytes", m.offsetJumpBytes, 20)
	assertCounter("other FD calls", m.fdOther.calls, 5)
	assertCounter("other FD bytes", m.fdOther.bytes, 18)
}

func TestPhysicalReadFSClassifiesSSTOpenMode(t *testing.T) {
	fs := vfs.NewMem()
	file, err := fs.Create("000001.sst")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(make([]byte, 16)); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	observed := newPhysicalReadFS(fs, "test/physical_read_fd_mode/")
	m := observed.(*physicalReadFS).metrics
	for _, test := range []struct {
		name string
		opts []vfs.OpenOption
		fd   *physicalReadFDMetrics
	}{
		{name: "random", opts: []vfs.OpenOption{vfs.RandomReadsOption}, fd: &m.fdRandom},
		{name: "sequential", opts: []vfs.OpenOption{vfs.SequentialReadsOption}, fd: &m.fdSequential},
		{name: "sequential then random", opts: []vfs.OpenOption{vfs.SequentialReadsOption, vfs.RandomReadsOption}, fd: &m.fdRandom},
		{name: "random then sequential", opts: []vfs.OpenOption{vfs.RandomReadsOption, vfs.SequentialReadsOption}, fd: &m.fdSequential},
		{name: "other", opts: []vfs.OpenOption{physicalReadUncomparableOpenOption{1}}, fd: &m.fdOther},
	} {
		t.Run(test.name, func(t *testing.T) {
			opened, err := observed.Open("000001.sst", test.opts...)
			if err != nil {
				t.Fatal(err)
			}
			physical := opened.(*physicalReadFile)
			if physical.fdMetrics != test.fd {
				t.Fatalf("FD metrics = %p, want %p", physical.fdMetrics, test.fd)
			}
			if n, err := physical.ReadAt(make([]byte, 4), 0); err != nil || n != 4 {
				t.Fatalf("ReadAt = %d, %v", n, err)
			}
			if err := opened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}

	for name, fd := range map[string]*physicalReadFDMetrics{
		"random":     &m.fdRandom,
		"sequential": &m.fdSequential,
		"other":      &m.fdOther,
	} {
		want := int64(1)
		if name != "other" {
			want = 2
		}
		if got := fd.calls.Snapshot().Count(); got != want {
			t.Fatalf("%s calls = %d, want %d", name, got, want)
		}
		if got := fd.bytes.Snapshot().Count(); got != want*4 {
			t.Fatalf("%s bytes = %d, want %d", name, got, want*4)
		}
	}
	if got := m.calls.Snapshot().Count(); got != 5 {
		t.Fatalf("total calls = %d, want 5", got)
	}
	if got := m.bytes.Snapshot().Count(); got != 20 {
		t.Fatalf("total bytes = %d, want 20", got)
	}
	classifiedNanos := m.fdRandom.nanos.Snapshot().Count() +
		m.fdSequential.nanos.Snapshot().Count() +
		m.fdOther.nanos.Snapshot().Count()
	if totalNanos := m.nanos.Snapshot().Count(); classifiedNanos != totalNanos {
		t.Fatalf("classified nanos = %d, total nanos = %d", classifiedNanos, totalNanos)
	}
}

type physicalReadUncomparableOpenOption []byte

func (physicalReadUncomparableOpenOption) Apply(vfs.File) {}

type physicalReadPrefetchErrorFile struct {
	vfs.File
	err    error
	calls  int
	offset int64
	length int64
}

func (f *physicalReadPrefetchErrorFile) Prefetch(offset, length int64) error {
	f.calls++
	f.offset = offset
	f.length = length
	return f.err
}

func TestPhysicalReadFileObservesPrefetchHints(t *testing.T) {
	fs := vfs.NewMem()
	file, err := fs.Create("000001.sst")
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	file, err = fs.Open("000001.sst")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	m := newPhysicalReadMetrics("test/physical_read_prefetch/")
	physical := &physicalReadFile{File: file, metrics: m, fdMetrics: &m.fdOther}
	if err := physical.Prefetch(4, 64<<10); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("prefetch failed")
	captured := &physicalReadPrefetchErrorFile{File: file, err: wantErr}
	physical.File = captured
	if err := physical.Prefetch(8, 128<<10); !errors.Is(err, wantErr) {
		t.Fatalf("Prefetch error = %v, want %v", err, wantErr)
	}
	if captured.calls != 1 || captured.offset != 8 || captured.length != 128<<10 {
		t.Fatalf("Prefetch forwarding = calls %d offset %d length %d", captured.calls, captured.offset, captured.length)
	}

	if got := m.prefetchCalls.Snapshot().Count(); got != 2 {
		t.Fatalf("prefetch calls = %d, want 2", got)
	}
	if got := m.prefetchBytes.Snapshot().Count(); got != 192<<10 {
		t.Fatalf("prefetch requested bytes = %d, want %d", got, 192<<10)
	}
	if got := m.prefetchErrors.Snapshot().Count(); got != 1 {
		t.Fatalf("prefetch errors = %d, want 1", got)
	}
}

func TestPhysicalReadLocalitySamplingHasNoFixedPhase(t *testing.T) {
	samples := func(name string) []uint64 {
		seed := physicalReadNameSeed(name)
		var result []uint64
		for ordinal := uint64(1); ordinal <= 512; ordinal++ {
			if physicalReadShouldSample(ordinal, seed) {
				result = append(result, ordinal)
			}
		}
		return result
	}
	first := samples("000001.sst")
	second := samples("000002.sst")
	if len(first) < 2 || len(second) < 2 {
		t.Fatalf("insufficient samples: %v / %v", first, second)
	}
	if first[0] == second[0] && first[1] == second[1] {
		t.Fatalf("file seeds kept the same sample phase: %v / %v", first[:2], second[:2])
	}
	allFixed := true
	for i := 1; i < len(first); i++ {
		if first[i]-first[i-1] != physicalReadLocalitySampleMask+1 {
			allFixed = false
			break
		}
	}
	if allFixed {
		t.Fatalf("sampling retained a fixed 64-call phase: %v", first)
	}
}

func TestPhysicalReadFSIgnoresNonSSTFiles(t *testing.T) {
	fs := vfs.NewMem()
	file, err := fs.Create("MANIFEST-000001")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("manifest")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	observed := newPhysicalReadFS(fs, "test/physical_read_ignore/")
	opened, err := observed.Open("MANIFEST-000001")
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	if _, err := opened.ReadAt(make([]byte, 4), 0); err != nil {
		t.Fatal(err)
	}
	if got := observed.(*physicalReadFS).metrics.calls.Snapshot().Count(); got != 0 {
		t.Fatalf("non-SST calls = %d, want 0", got)
	}
}

func TestPhysicalReadFSIsInstalledOnPebbleSSTReads(t *testing.T) {
	dir := t.TempDir()
	namespace := "test/physical_read_pebble/"
	db, err := New(dir, 16, 16, namespace, false, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte{0x5a}, 4096)
	for i := 0; i < 64; i++ {
		if err := db.Put([]byte(fmt.Sprintf("key-%04d", i)), want); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = New(dir, 16, 16, namespace, false, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	calls := metrics.GetOrRegisterCounter(namespace+"disk/physical/read/sst/calls", nil)
	before := calls.Snapshot().Count()
	got, err := db.Get([]byte("key-0063"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("value size/content mismatch: %d bytes", len(got))
	}
	if delta := calls.Snapshot().Count() - before; delta <= 0 {
		t.Fatalf("SST ReadAt calls delta = %d, want positive", delta)
	}
}

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

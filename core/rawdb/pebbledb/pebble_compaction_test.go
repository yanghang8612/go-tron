package pebbledb

import (
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestCompactionConcurrencyReservesForegroundCPU(t *testing.T) {
	for _, tc := range []struct {
		procs int
		want  int
	}{
		{procs: -1, want: 1},
		{procs: 0, want: 1},
		{procs: 1, want: 1},
		{procs: 2, want: 1},
		{procs: 3, want: 2},
		{procs: 4, want: 2},
		{procs: 8, want: 4},
	} {
		if got := compactionConcurrency(tc.procs); got != tc.want {
			t.Errorf("compactionConcurrency(%d) = %d, want %d", tc.procs, got, tc.want)
		}
	}
}

func TestCompactionPressureThresholdsReserveRoutineCapacity(t *testing.T) {
	if l0CompactionConcurrency != 10 {
		t.Fatalf("L0 concurrency threshold = %d, want 10", l0CompactionConcurrency)
	}
	if compactionDebtConcurrency != 2<<30 {
		t.Fatalf("debt concurrency threshold = %d, want 2 GiB", compactionDebtConcurrency)
	}
}

func TestDefaultLBaseMatchesBulkSyncMemtable(t *testing.T) {
	opts := DefaultOptions()
	if opts.LBaseMaxBytes != int64(opts.MemTableSizeBytes) {
		t.Fatalf("lbase max=%d memtable=%d", opts.LBaseMaxBytes, opts.MemTableSizeBytes)
	}
	if opts.LBaseMaxBytes != 256<<20 {
		t.Fatalf("lbase max=%d, want 256 MiB", opts.LBaseMaxBytes)
	}
}

func TestLevelOptionsScaleTargetAndPreserveFilters(t *testing.T) {
	const base = int64(8 << 20)
	levels := levelOptions(base)
	if len(levels) != 7 {
		t.Fatalf("levels = %d, want 7", len(levels))
	}
	for i, level := range levels {
		if want := base << i; level.TargetFileSize != want {
			t.Errorf("level %d target = %d, want %d", i, level.TargetFileSize, want)
		}
		if i < len(levels)-1 && level.FilterPolicy == nil {
			t.Errorf("level %d lost Bloom filter", i)
		}
	}
	if levels[len(levels)-1].FilterPolicy != nil {
		t.Fatal("last level unexpectedly has a Bloom filter")
	}
	if levels[0].Compression != pebble.DefaultCompression {
		t.Fatalf("compression = %v, want Pebble default", levels[0].Compression)
	}
}

package pebbledb

import "testing"

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
	if compactionDebtConcurrency != 1<<30 {
		t.Fatalf("debt concurrency threshold = %d, want 1 GiB", compactionDebtConcurrency)
	}
}

package maintenance

import (
	"math"
	"testing"
	"time"
)

func TestStoragePressureHardLimitReached(t *testing.T) {
	now := time.Unix(1000, 0)
	for _, tc := range []struct {
		name     string
		pressure StoragePressure
		want     bool
	}{
		{name: "missing"},
		{name: "stall", pressure: StoragePressure{WriteStalled: true}, want: true},
		{name: "ordinary debt is not a hard limit", pressure: StoragePressure{CompactionDebt: math.MaxUint64}},
		{name: "memtable below", pressure: StoragePressure{MemTableCount: 6, MemTableStopWritesThreshold: 8}},
		{name: "memtable boundary", pressure: StoragePressure{MemTableCount: 7, MemTableStopWritesThreshold: 8}, want: true},
		{name: "memtable no threshold", pressure: StoragePressure{MemTableCount: 100}},
		{name: "memtable no count", pressure: StoragePressure{MemTableStopWritesThreshold: 1}},
		{name: "memtable negative count", pressure: StoragePressure{MemTableCount: -1, MemTableStopWritesThreshold: 1}},
		{name: "memtable minimum", pressure: StoragePressure{MemTableCount: 1, MemTableStopWritesThreshold: 1}, want: true},
		{name: "L0 below", pressure: StoragePressure{L0Sublevels: 47, L0StopWritesThreshold: 64}},
		{name: "L0 boundary", pressure: StoragePressure{L0Sublevels: 48, L0StopWritesThreshold: 64}, want: true},
		{name: "L0 rounded boundary", pressure: StoragePressure{L0Sublevels: 14, L0StopWritesThreshold: 19}, want: true},
		{name: "L0 no threshold", pressure: StoragePressure{L0Sublevels: 100}},
		{name: "L0 no count", pressure: StoragePressure{L0StopWritesThreshold: 1}},
		{name: "L0 negative count", pressure: StoragePressure{L0Sublevels: -1, L0StopWritesThreshold: 1}},
		{name: "L0 minimum", pressure: StoragePressure{L0Sublevels: 1, L0StopWritesThreshold: 1}, want: true},
		{name: "L0 large threshold below", pressure: StoragePressure{L0Sublevels: 1, L0StopWritesThreshold: math.MaxInt}},
		{name: "L0 large threshold above", pressure: StoragePressure{L0Sublevels: math.MaxInt, L0StopWritesThreshold: math.MaxInt}, want: true},
		{name: "busy shared disk is not a hard limit", pressure: StoragePressure{DeviceAvailable: true, DeviceSampledAt: now, DeviceBusyPPM: 1_000_000, DeviceQueueMilli: 100_000, DeviceAwait: time.Second}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := tc.pressure
			p.Available, p.SampledAt = true, now
			if got := p.HardLimitReached(now); got != tc.want {
				t.Fatalf("HardLimitReached=%v want=%v for %+v", got, tc.want, p)
			}
		})
	}
}

func TestStoragePressureHardLimitRequiresFreshEngineSample(t *testing.T) {
	now := time.Unix(1000, 0)
	for _, tc := range []struct {
		name      string
		available bool
		at        time.Time
		want      bool
	}{
		{"unavailable", false, now, false},
		{"zero timestamp", true, time.Time{}, false},
		{"current", true, now, true},
		{"old boundary", true, now.Add(-15 * time.Second), true},
		{"stale", true, now.Add(-15*time.Second - time.Nanosecond), false},
		{"future boundary", true, now.Add(time.Second), true},
		{"future", true, now.Add(time.Second + time.Nanosecond), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := StoragePressure{Available: tc.available, SampledAt: tc.at, WriteStalled: true}
			if got := p.HardLimitReached(now); got != tc.want {
				t.Fatalf("HardLimitReached=%v want=%v for %+v", got, tc.want, p)
			}
		})
	}
}

package main

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

type historyLoadEngineFunc func() maintenance.StoragePressure

func (f historyLoadEngineFunc) MaintenancePressure() maintenance.StoragePressure { return f() }

func historyTestDiskstats(device historyDeviceID, c historyDiskCounters) []byte {
	return []byte(fmt.Sprintf("%d %d nvme-device %d 0 0 %d %d 0 0 %d 0 %d %d 0 0 0 0 0 0\n",
		device.major, device.minor, c.reads, c.readMillis, c.writes, c.writeMillis, c.busyMillis, c.weightedMillis))
}

func TestHistoryDiskstatsMatchesFilesystemDevice(t *testing.T) {
	device := historyDeviceID{259, 3}
	want := historyDiskCounters{1, 2, 30, 40, 50, 60}
	raw := append(historyTestDiskstats(historyDeviceID{259, 0}, historyDiskCounters{99, 99, 99, 99, 99, 99}), historyTestDiskstats(device, want)...)
	got, err := parseHistoryDiskstats(raw, device)
	if err != nil || got != want {
		t.Fatalf("matched counters: %+v, %v", got, err)
	}
	for name, raw := range map[string][]byte{
		"unknown device": historyTestDiskstats(historyDeviceID{8, 0}, want),
		"truncated":      []byte("259 3 device 1 2"),
		"negative":       []byte("259 3 device -1 0 0 0 1 0 0 0 0 0 0"),
		"overflow":       []byte("259 3 device 18446744073709551616 0 0 0 1 0 0 0 0 0 0"),
		"oversized":      []byte(strings.Repeat(" ", historyDiskstatsReadLimit+1)),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseHistoryDiskstats(raw, device); err == nil {
				t.Fatal("accepted invalid or unavailable counters")
			}
		})
	}
}

func TestRuntimeHistoryLoadCachesDeviceButNotEngine(t *testing.T) {
	now := time.Unix(1000, 0)
	device := historyDeviceID{259, 3}
	counters := historyDiskCounters{100, 100, 100, 100, 100, 100}
	reads, locates, engineReads := 0, 0, 0
	p := &runtimeHistoryLoadProbe{
		path: "datadir", now: func() time.Time { return now },
		engine: historyLoadEngineFunc(func() maintenance.StoragePressure {
			engineReads++
			return maintenance.StoragePressure{Available: true, SampledAt: now, WriteStalled: engineReads == 3, CompactionDebt: uint64(engineReads)}
		}),
		locate: func(path string) (historyDeviceID, error) {
			if path != "datadir" {
				t.Fatalf("wrong device path %q", path)
			}
			locates++
			return device, nil
		},
		read: func() ([]byte, error) { reads++; return historyTestDiskstats(device, counters), nil },
	}
	if got := p.sample(); !got.Available || got.DeviceAvailable || !got.DeviceSampledAt.IsZero() {
		t.Fatalf("first observation must not infer idle: %+v", got)
	}
	now = now.Add(5 * time.Second)
	counters = historyDiskCounters{200, 200, 1100, 1100, 4600, 12600}
	got := p.sample()
	if !got.DeviceAvailable || got.DeviceBusyPPM != 900_000 || got.DeviceQueueMilli != 2500 || got.DeviceAwait != 10*time.Millisecond || got.DeviceSampledAt != now {
		t.Fatalf("device ratios: %+v", got)
	}
	now = now.Add(4 * time.Second)
	cached := p.sample()
	if !cached.WriteStalled || cached.CompactionDebt != 3 || cached.SampledAt != now || cached.DeviceSampledAt != got.DeviceSampledAt || cached.DeviceBusyPPM != got.DeviceBusyPPM || reads != 2 || locates != 2 {
		t.Fatalf("device cache suppressed fresh engine pressure: %+v, reads=%d locates=%d", cached, reads, locates)
	}
}

func TestRuntimeHistoryLoadFailureRequiresNewBaseline(t *testing.T) {
	for _, failure := range []string{"read", "locate", "missing", "reset", "changed device", "stale", "clock backwards"} {
		t.Run(failure, func(t *testing.T) {
			now := time.Unix(1000, 0)
			device := historyDeviceID{259, 3}
			c := historyDiskCounters{}
			readErr, locateErr := error(nil), error(nil)
			missing := false
			p := &runtimeHistoryLoadProbe{
				now:    func() time.Time { return now },
				locate: func(string) (historyDeviceID, error) { return device, locateErr },
				read: func() ([]byte, error) {
					if missing {
						return []byte{}, nil
					}
					return historyTestDiskstats(device, c), readErr
				},
			}
			advance := func() {
				now = now.Add(5 * time.Second)
				c.reads += 100
				c.readMillis += 1000
				c.busyMillis += 4000
				c.weightedMillis += 5000
			}
			p.sample()
			advance()
			if got := p.sample(); got.Available || !got.DeviceAvailable {
				t.Fatalf("device availability must be independent of missing engine: %+v", got)
			}
			advance()
			switch failure {
			case "read":
				readErr = errors.New("read failed")
			case "locate":
				locateErr = errors.New("stat failed")
			case "missing":
				missing = true
			case "reset":
				c.reads = 0
			case "changed device":
				device.minor++
			case "stale":
				now = now.Add(historyDeviceMaxSampleSpan)
			case "clock backwards":
				now = now.Add(-10 * time.Second)
			}
			if got := p.sample(); got.DeviceAvailable || !got.DeviceSampledAt.IsZero() || got.DeviceBusyPPM != 0 {
				t.Fatalf("invalid sample reused old availability: %+v", got)
			}
			readErr, locateErr, missing = nil, nil, false
			if failure == "read" || failure == "locate" || failure == "missing" {
				advance()
				if got := p.sample(); got.DeviceAvailable {
					t.Fatalf("read failure must discard prior baseline: %+v", got)
				}
			}
			advance()
			if got := p.sample(); !got.DeviceAvailable || got.DeviceBusyPPM != 800_000 {
				t.Fatalf("fresh pair did not recover: %+v", got)
			}
		})
	}
}

func TestHistoryDeviceDeltaUnknownAndSaturation(t *testing.T) {
	now := time.Unix(1000, 0)
	if got := historyDeviceDelta(historyDiskCounters{}, historyDiskCounters{busyMillis: 5000, weightedMillis: 90000}, 5*time.Second, now); got.DeviceAvailable {
		t.Fatalf("all I/O in flight must not report observed zero await: %+v", got)
	}
	before := historyDiskCounters{100, 100, 100, 100, 100, 100}
	for i := range 6 {
		after := historyDiskCounters{200, 200, 200, 200, 200, 200}
		fields := []*uint64{&after.reads, &after.writes, &after.readMillis, &after.writeMillis, &after.busyMillis, &after.weightedMillis}
		*fields[i] = 99
		if got := historyDeviceDelta(before, after, 5*time.Second, now); got.DeviceAvailable {
			t.Fatalf("counter %d rollback was accepted: %+v", i, got)
		}
	}
	got := historyDeviceDelta(historyDiskCounters{}, historyDiskCounters{reads: 1, readMillis: math.MaxUint64, busyMillis: math.MaxUint64, weightedMillis: math.MaxUint64}, 5*time.Second, now)
	if !got.DeviceAvailable || got.DeviceBusyPPM != 1_000_000 || got.DeviceAwait != time.Duration(math.MaxInt64) || got.DeviceQueueMilli == 0 {
		t.Fatalf("counter scaling wrapped into idle: %+v", got)
	}
	for _, after := range []historyDiskCounters{
		{reads: math.MaxUint64, writes: 1},
		{reads: 1, readMillis: math.MaxUint64, writeMillis: 1},
	} {
		if got := historyDeviceDelta(historyDiskCounters{}, after, 5*time.Second, now); got.DeviceAvailable {
			t.Fatalf("counter sum overflow was accepted: %+v", got)
		}
	}
}

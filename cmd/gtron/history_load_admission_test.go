package main

import (
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/state/pruning"
)

func TestHistoryPostingPressureNeverRefreshesDevice(t *testing.T) {
	now := time.Now()
	stalled, engineReads := false, 0
	load := &runtimeHistoryLoadProbe{
		engine: historyLoadEngineFunc(func() maintenance.StoragePressure {
			engineReads++
			return maintenance.StoragePressure{Available: true, SampledAt: now, WriteStalled: stalled}
		}),
		locate: func(string) (historyDeviceID, error) {
			t.Fatal("locked admission located device")
			return historyDeviceID{}, nil
		},
		read:   func() ([]byte, error) { t.Fatal("locked admission read proc"); return nil, nil },
		now:    func() time.Time { t.Fatal("locked admission entered device refresh clock"); return now },
		cached: maintenance.StoragePressure{DeviceAvailable: true, DeviceSampledAt: now, DeviceAwait: time.Millisecond, DeviceQueueMilli: 1000},
	}
	p := &runtimeHistoryResources{load: load}
	if got := p.postingPressure(); got.Available || engineReads != 0 {
		t.Fatal("stopped admission read or exposed resources")
	}
	p.running.Store(true)
	if got := p.postingPressure(); !pruning.PostingPrunePressureReady(got, now) || engineReads != 1 {
		t.Fatalf("fresh cached admission=%+v reads=%d", got, engineReads)
	}
	stalled = true
	if got := p.postingPressure(); pruning.PostingPrunePressureReady(got, now) || !got.WriteStalled || engineReads != 2 {
		t.Fatalf("cached device hid new engine stall: %+v", got)
	}
	stalled = false
	load.mu.Lock()
	busy := p.postingPressure()
	// The global hard-pressure callback does not need the device mutex.
	engine := p.enginePressure()
	load.mu.Unlock()
	if busy.Available || busy.DeviceAvailable || !engine.Available || engineReads != 3 {
		t.Fatal("busy device sampler blocked or bypassed fail-closed posting admission")
	}
	load.cached.DeviceSampledAt = now.Add(-16 * time.Second)
	if got := p.postingPressure(); pruning.PostingPrunePressureReady(got, now) || got.DeviceSampledAt != load.cached.DeviceSampledAt {
		t.Fatal("stale cached telemetry was refreshed or accepted")
	}
	p.running.Store(false)
	if got := p.postingPressure(); got.Available || got.DeviceAvailable {
		t.Fatal("stopped sampler exposed admission capacity")
	}
}

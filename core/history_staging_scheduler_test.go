package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

func TestHistoryStagingAdmissionBoundaries(t *testing.T) {
	now := time.Now()
	base := maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now, DeviceBusyPPM: 870_000, DeviceQueueMilli: 4_600, DeviceAwait: 700 * time.Microsecond}
	for _, tc := range []struct {
		name   string
		change func(*maintenance.StoragePressure)
		want   historyStagingAdmission
	}{
		{"observed busy device", func(*maintenance.StoragePressure) {}, historyStagingBusy},
		{"old idle envelope", func(p *maintenance.StoragePressure) { p.DeviceQueueMilli = 999; p.DeviceAwait = 19 * time.Millisecond }, historyStagingIdle},
		{"unknown engine", func(p *maintenance.StoragePressure) { p.Available = false }, historyStagingEngineUnknown},
		{"stale engine", func(p *maintenance.StoragePressure) { p.SampledAt = now.Add(-16 * time.Second) }, historyStagingEngineStale},
		{"future engine", func(p *maintenance.StoragePressure) { p.SampledAt = now.Add(2 * time.Second) }, historyStagingEngineStale},
		{"stall", func(p *maintenance.StoragePressure) { p.WriteStalled = true }, historyStagingEngineHard},
		{"memtable threshold", func(p *maintenance.StoragePressure) { p.MemTableStopWritesThreshold = 4; p.MemTableCount = 3 }, historyStagingEngineHard},
		{"L0 threshold", func(p *maintenance.StoragePressure) { p.L0StopWritesThreshold = 64; p.L0Sublevels = 48 }, historyStagingEngineHard},
		{"unknown device", func(p *maintenance.StoragePressure) { p.DeviceAvailable = false }, historyStagingDeviceUnknown},
		{"stale device", func(p *maintenance.StoragePressure) { p.DeviceSampledAt = now.Add(-16 * time.Second) }, historyStagingDeviceStale},
		{"future device", func(p *maintenance.StoragePressure) { p.DeviceSampledAt = now.Add(2 * time.Second) }, historyStagingDeviceStale},
		{"busy boundary", func(p *maintenance.StoragePressure) { p.DeviceBusyPPM = 900_000 }, historyStagingDeviceBusy},
		{"queue boundary", func(p *maintenance.StoragePressure) { p.DeviceQueueMilli = 8_000 }, historyStagingDeviceQueue},
		{"await boundary", func(p *maintenance.StoragePressure) { p.DeviceAwait = 5 * time.Millisecond }, historyStagingDeviceAwait},
		{"busy upper bound", func(p *maintenance.StoragePressure) {
			p.DeviceBusyPPM = 899_999
			p.DeviceQueueMilli = 7_999
			p.DeviceAwait = 5*time.Millisecond - time.Nanosecond
		}, historyStagingBusy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := base
			tc.change(&p)
			if got := classifyHistoryStagingPressure(p, now); got != tc.want {
				t.Fatalf("got %d want %d", got, tc.want)
			}
		})
	}
}

func TestHistoryStagingMoverHighUtilizationAdmission(t *testing.T) {
	now := time.Now()
	base := maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now,
		DeviceBusyPPM: 919_519, DeviceQueueMilli: 4_045, DeviceAwait: 1_136_501 * time.Nanosecond}
	for _, tc := range []struct {
		name   string
		change func(*maintenance.StoragePressure)
		want   historyStagingAdmission
	}{
		{"observed low latency", func(*maintenance.StoragePressure) {}, historyStagingBusy},
		{"ninety percent", func(p *maintenance.StoragePressure) { p.DeviceBusyPPM = 900_000 }, historyStagingBusy},
		{"fully busy upper envelope", func(p *maintenance.StoragePressure) {
			p.DeviceBusyPPM = 1_000_000
			p.DeviceQueueMilli = 7_999
			p.DeviceAwait = 2*time.Millisecond - time.Nanosecond
		}, historyStagingBusy},
		{"invalid utilization", func(p *maintenance.StoragePressure) { p.DeviceBusyPPM = 1_000_001 }, historyStagingDeviceBusy},
		{"queue boundary", func(p *maintenance.StoragePressure) { p.DeviceQueueMilli = 8_000 }, historyStagingDeviceBusy},
		{"latency boundary", func(p *maintenance.StoragePressure) { p.DeviceAwait = 2 * time.Millisecond }, historyStagingDeviceBusy},
		{"negative latency", func(p *maintenance.StoragePressure) { p.DeviceAwait = -time.Nanosecond }, historyStagingDeviceBusy},
		{"high latency cannot enter idle", func(p *maintenance.StoragePressure) {
			p.DeviceQueueMilli = 0
			p.DeviceAwait = 19 * time.Millisecond
		}, historyStagingDeviceBusy},
		{"unknown engine", func(p *maintenance.StoragePressure) { p.Available = false }, historyStagingEngineUnknown},
		{"stale engine", func(p *maintenance.StoragePressure) { p.SampledAt = now.Add(-16 * time.Second) }, historyStagingEngineStale},
		{"future engine", func(p *maintenance.StoragePressure) { p.SampledAt = now.Add(2 * time.Second) }, historyStagingEngineStale},
		{"stall", func(p *maintenance.StoragePressure) { p.WriteStalled = true }, historyStagingEngineHard},
		{"memtable hard limit", func(p *maintenance.StoragePressure) {
			p.MemTableStopWritesThreshold, p.MemTableCount = 4, 3
		}, historyStagingEngineHard},
		{"L0 hard limit", func(p *maintenance.StoragePressure) {
			p.L0StopWritesThreshold, p.L0Sublevels = 64, 48
		}, historyStagingEngineHard},
		{"unknown device", func(p *maintenance.StoragePressure) { p.DeviceAvailable = false }, historyStagingDeviceUnknown},
		{"stale device", func(p *maintenance.StoragePressure) { p.DeviceSampledAt = now.Add(-16 * time.Second) }, historyStagingDeviceStale},
		{"future device", func(p *maintenance.StoragePressure) { p.DeviceSampledAt = now.Add(2 * time.Second) }, historyStagingDeviceStale},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := base
			tc.change(&p)
			if got := classifyHistoryStagingMoverPressure(p, now); got != tc.want {
				t.Fatalf("mover admission = %d want %d", got, tc.want)
			}
			// The original classifier remains strict for index GC.
			if tc.want == historyStagingBusy && classifyHistoryStagingPressure(p, now) != historyStagingDeviceBusy {
				t.Fatal("high utilization exception escaped the mover policy")
			}
		})
	}
	for _, blockedEngine := range []string{"hot", "stage"} {
		p := base
		p.WriteStalled = true
		m := &HistoryStagingMover{cfg: HistoryStagingMoverConfig{
			HotPressure: func() maintenance.StoragePressure {
				if blockedEngine == "hot" {
					return p
				}
				return base
			},
			StagePressure: func() maintenance.StoragePressure {
				if blockedEngine == "stage" {
					return p
				}
				return base
			},
		}}
		if got := m.admission(now); got != historyStagingEngineHard {
			t.Fatalf("%s hard limit bypassed: %d", blockedEngine, got)
		}
	}
}

func TestHistoryStagingQuantumYieldsTokenAndRetainsProgress(t *testing.T) {
	for _, trigger := range []string{"bytes", "time", "long row", "busy device"} {
		t.Run(trigger, func(t *testing.T) {
			now := time.Now()
			gate := maintenance.NewHeavyWorkGateWithCooldownAfter(0, 250*time.Millisecond)
			probe := func() maintenance.StoragePressure {
				p := maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now, DeviceQueueMilli: 4_000, DeviceAwait: time.Millisecond}
				if trigger == "busy device" {
					p.DeviceBusyPPM = 1_000_000
				}
				return p
			}
			m := &HistoryStagingMover{cfg: HistoryStagingMoverConfig{HeavyWorkGate: gate, HotPressure: probe, StagePressure: probe}}
			release, ok := m.acquireHeavy(false)
			if !ok {
				t.Fatal("initial lease rejected")
			}
			q := &historyStagingQuantum{m: m, ctx: context.Background(), release: release, started: now, now: func() time.Time { return now }}
			m.quantum = q
			defer q.close()
			waits := 0
			q.wait = func(_ context.Context, d time.Duration) error {
				waits++
				want := time.Second
				if trigger == "long row" {
					want = 8 * time.Second
				}
				if d != want {
					t.Fatalf("recovery %v want %v", d, want)
				}
				if q.release != nil {
					t.Fatal("lease retained during recovery")
				}
				other, ok := gate.TryAcquire()
				if !ok {
					t.Fatal("other maintenance could not run during yield")
				}
				other()
				now = now.Add(d)
				return nil
			}
			work := uint64(32 << 20)
			if trigger == "time" {
				work = 1
				now = now.Add(101 * time.Millisecond)
			}
			if trigger == "long row" {
				work = 1
				now = now.Add(2 * time.Second)
			}
			if err := q.checkpoint(work); err != nil {
				t.Fatal(err)
			}
			if waits != 1 || q.release == nil || q.bytes != 0 {
				t.Fatalf("quantum did not resume: waits=%d bytes=%d", waits, q.bytes)
			}
			if err := q.checkpoint(123); err != nil {
				t.Fatal(err)
			}
			if waits != 1 || q.bytes != 123 {
				t.Fatal("resumed quantum restarted or lost its progress")
			}
		})
	}
}

func TestHistoryStagingQuantumPausesOnMidCopyPressure(t *testing.T) {
	for _, pressure := range []string{"hard", "unknown", "stale", "busy latency", "busy queue"} {
		t.Run(pressure, func(t *testing.T) {
			now := time.Now()
			blocked := false
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			gate := maintenance.NewHeavyWorkGateWithCooldownAfter(0, 250*time.Millisecond)
			probe := func() maintenance.StoragePressure {
				p := maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now}
				if blocked {
					switch pressure {
					case "hard":
						p.WriteStalled = true
					case "unknown":
						p.DeviceAvailable = false
					case "stale":
						p.SampledAt = now.Add(-16 * time.Second)
					case "busy latency":
						p.DeviceBusyPPM = 1_000_000
						p.DeviceAwait = 2 * time.Millisecond
					case "busy queue":
						p.DeviceBusyPPM = 1_000_000
						p.DeviceQueueMilli = 8_000
					}
				}
				return p
			}
			m := &HistoryStagingMover{cfg: HistoryStagingMoverConfig{HeavyWorkGate: gate, HotPressure: probe, StagePressure: probe}}
			release, _ := m.acquireHeavy(false)
			q := &historyStagingQuantum{m: m, ctx: ctx, release: release, started: now, now: func() time.Time { return now }}
			m.quantum = q
			defer q.close()
			blocked = true
			now = now.Add(101 * time.Millisecond)
			waits := 0
			q.wait = func(_ context.Context, d time.Duration) error {
				waits++
				if q.release != nil {
					t.Fatal("copy reacquired under pressure")
				}
				other, ok := gate.TryAcquire()
				if !ok {
					t.Fatal("paused copy blocked other worker")
				}
				other()
				now = now.Add(d)
				if waits == 2 {
					cancel()
					return ctx.Err()
				}
				return nil
			}
			if err := q.checkpoint(1); !errors.Is(err, context.Canceled) {
				t.Fatalf("got %v want cancellation", err)
			}
			if waits != 2 || q.release != nil || m.reservation != nil {
				t.Fatal("pressure/cancel retained lease or reservation")
			}
		})
	}
}

func TestHistoryStagingReservationHonorsActiveLeaseAndCooldown(t *testing.T) {
	gate := maintenance.NewHeavyWorkGateWithCooldownAfter(time.Hour, 0)
	other, _ := gate.TryAcquire()
	m := &HistoryStagingMover{cfg: HistoryStagingMoverConfig{HeavyWorkGate: gate}}
	if _, ok := m.acquireHeavy(true); ok || m.reservation == nil {
		t.Fatal("ready mover failed to reserve behind active work")
	}
	other()
	if _, ok := m.acquireHeavy(true); ok {
		t.Fatal("reservation bypassed another worker's recovery")
	}
	m.cancelReservation()
	if _, ok := m.acquireHeavy(true); ok || m.reservation != nil {
		t.Fatal("mover immediately renewed canceled reservation")
	}
	if got := historyStagingRecovery(2*time.Second, 15*time.Second); got != 8*time.Second {
		t.Fatalf("long quantum recovery=%v", got)
	}
}

func TestHistoryStagingQuantumProbeCostIsIndependentOfRows(t *testing.T) {
	now := time.Now()
	probes := 0
	probe := func() maintenance.StoragePressure {
		probes++
		return maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now}
	}
	m := &HistoryStagingMover{cfg: HistoryStagingMoverConfig{HeavyWorkGate: maintenance.NewHeavyWorkGateWithCooldownAfter(0, time.Second), HotPressure: probe, StagePressure: probe}}
	release, _ := m.acquireHeavy(false)
	q := &historyStagingQuantum{m: m, ctx: context.Background(), release: release, started: now, now: func() time.Time { return now }, wait: func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }}
	m.quantum = q
	defer q.close()
	for i := 0; i < 100_000; i++ {
		if err := q.checkpoint(1); err != nil {
			t.Fatal(err)
		}
	}
	if probes != 0 {
		t.Fatalf("tiny rows caused %d probes", probes)
	}
	if err := q.recheck(); err != nil {
		t.Fatal(err)
	}
	if probes != 2 {
		t.Fatalf("forced boundary probes=%d want 2", probes)
	}
	if err := q.checkpoint(32 << 20); err != nil {
		t.Fatal(err)
	}
	if probes != 8 {
		t.Fatalf("quantum/re-admission probes=%d want 8", probes)
	}
}

func TestHistoryStagingQuantumSpaceFailureIsTerminal(t *testing.T) {
	now := time.Now()
	probe := func() maintenance.StoragePressure {
		return maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now}
	}
	spaceErr := errors.New("transient statfs failure")
	spaceCalls := 0
	m := &HistoryStagingMover{cfg: HistoryStagingMoverConfig{HeavyWorkGate: maintenance.NewHeavyWorkGateWithCooldownAfter(0, time.Second), HotPressure: probe, StagePressure: probe}}
	m.cfg.Limits.FreeBytes = func() (uint64, error) {
		spaceCalls++
		if spaceCalls == 1 {
			return 0, spaceErr
		}
		return 1 << 40, nil
	}
	m.cfg.Limits.MinFreeBytes = 1
	release, _ := m.acquireHeavy(false)
	q := &historyStagingQuantum{m: m, ctx: context.Background(), release: release, started: now, now: func() time.Time { return now }, wait: func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }}
	m.quantum = q
	defer q.close()
	if err := q.checkpoint(32 << 20); !errors.Is(err, spaceErr) {
		t.Fatalf("got %v want statfs failure", err)
	}
	if q.release != nil || q.terminal == nil {
		t.Fatal("failed space check did not terminate scheduler lease")
	}
	if err := q.recheck(); !errors.Is(err, spaceErr) {
		t.Fatalf("terminal scheduler resumed: %v", err)
	}
	if spaceCalls != 1 {
		t.Fatal("later space recovery allowed work without the token")
	}
	other, ok := m.cfg.HeavyWorkGate.TryAcquire()
	if !ok {
		t.Fatal("failed quantum retained token")
	}
	other()
}

func TestHistoryStagingMemoryDeferralTerminatesQuantum(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*HistoryStagingMemory)
		want   historyStagingAdmission
	}{
		{"unknown", func(p *HistoryStagingMemory) { p.Available = false }, historyStagingMemoryUnknown},
		{"stale", func(p *HistoryStagingMemory) { p.SampledAt = p.SampledAt.Add(-16 * time.Second) }, historyStagingMemoryStale},
		{"future", func(p *HistoryStagingMemory) { p.SampledAt = p.SampledAt.Add(2 * time.Second) }, historyStagingMemoryStale},
		{"OOM", func(p *HistoryStagingMemory) { p.UnderOOM = true }, historyStagingMemoryOOM},
		{"low", func(p *HistoryStagingMemory) { p.HeadroomBytes = (2 << 30) - 1 }, historyStagingMemoryLow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			observation := HistoryStagingMemory{Available: true, SampledAt: now, HeadroomBytes: 2 << 30}
			probe := func() maintenance.StoragePressure {
				return maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now}
			}
			m := &HistoryStagingMover{cfg: HistoryStagingMoverConfig{HeavyWorkGate: maintenance.NewHeavyWorkGateWithCooldownAfter(0, time.Second), HotPressure: probe, StagePressure: probe, MemoryProbe: func() HistoryStagingMemory { return observation }}}
			if got := m.memoryAdmission(now); got != historyStagingIdle {
				t.Fatalf("valid boundary denied: %d", got)
			}
			release, _ := m.acquireHeavy(false)
			q := &historyStagingQuantum{m: m, ctx: context.Background(), release: release, started: now, now: func() time.Time { return now }, wait: func(context.Context, time.Duration) error {
				t.Fatal("memory deferral retained buffers while waiting")
				return nil
			}}
			m.quantum = q
			defer q.close()
			tc.change(&observation)
			if got := m.memoryAdmission(now); got != tc.want {
				t.Fatalf("mode=%d want %d", got, tc.want)
			}
			if err := q.checkpoint(32 << 20); !errors.Is(err, ErrHistoryStagingResourceDeferred) {
				t.Fatalf("got %v want expected resource deferral", err)
			}
			if q.release != nil || q.terminal == nil {
				t.Fatal("memory deferral retained its lease")
			}
			observation = HistoryStagingMemory{Available: true, SampledAt: now, HeadroomBytes: 8 << 30}
			if err := q.recheck(); !errors.Is(err, ErrHistoryStagingResourceDeferred) {
				t.Fatalf("terminal memory deferral continued later phase: %v", err)
			}
		})
	}
}

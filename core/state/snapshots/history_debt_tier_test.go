package snapshots

import (
	"runtime"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

func TestHistoryDebtTierBoundariesAndProtection(t *testing.T) {
	for _, baseline := range []uint64{3 << 30, 30 << 30} {
		threshold := max(uint64(historyDebtGrowthMinBytes), baseline/100)
		for _, delta := range []uint64{threshold, threshold*4 - 1, threshold * 4} {
			t.Run(time.Duration(delta).String(), func(t *testing.T) {
				now := time.Now()
				p := healthyHistoryLoad(now)
				r := historyLoadMetricsFixture(t, &p)
				for i := 0; i < 7; i++ {
					at := now.Add(time.Duration(i-6) * 5 * time.Second)
					p.SampledAt, p.DeviceSampledAt = at, at
					p.CompactionDebt = baseline + delta*uint64(i)/6
					r.refreshHistoryLoad(at)
				}
				moderate := delta < threshold*4
				if r.historyLoad.debtModerateReady(now) != moderate || r.historyLoad.cpuBurstReady(now) != moderate {
					t.Fatalf("tier: %+v", r.historyLoad)
				}
				want := uint64(200000)
				if moderate {
					want = 600000
				}
				if got := r.historyLoad.dutyPPM(); got != want {
					t.Fatalf("duty %d want %d", got, want)
				}
				duration, bytes := r.historyLoad.targets()
				if duration != 2*time.Second || bytes != 32<<20 {
					t.Fatalf("middle tier enlarged batch: %s/%d", duration, bytes)
				}
				if !moderate {
					return
				}
				for name, alter := range map[string]func(*maintenance.StoragePressure){
					"unknown-device": func(p *maintenance.StoragePressure) { p.DeviceAvailable = false },
					"stale-device":   func(p *maintenance.StoragePressure) { p.DeviceSampledAt = now.Add(-time.Hour) },
					"latency":        func(p *maintenance.StoragePressure) { p.DeviceAwait = 3 * time.Millisecond },
					"l0":             func(p *maintenance.StoragePressure) { p.L0Sublevels = p.L0CompactionThreshold },
					"unknown-l0":     func(p *maintenance.StoragePressure) { p.L0CompactionThreshold = 0 },
					"stall":          func(p *maintenance.StoragePressure) { p.StallCount++ },
					"hard":           func(p *maintenance.StoragePressure) { p.WriteStalled = true },
					"unknown-engine": func(p *maintenance.StoragePressure) { p.Available = false },
					"reset":          func(p *maintenance.StoragePressure) { p.SampledAt = now.Add(-time.Second) },
				} {
					t.Run(name, func(t *testing.T) {
						saved := r.historyLoad
						original := p
						alter(&p)
						r.refreshHistoryLoad(now)
						r.refreshHistoryLoad(now)
						if r.historyLoad.debtModerateReady(now) || r.historyLoad.cpuBurstReady(now) {
							t.Fatal("protection left CPU burst enabled")
						}
						r.historyLoad = saved
						p = original
					})
				}
				if r.historyLoad.debtModerateReady(now.Add(time.Hour)) {
					t.Fatal("expired middle tier")
				}
			})
		}
	}
}

func TestHistoryDebtMiddleTierRetainsBusyResourceAdmission(t *testing.T) {
	r := busyResourceFixture(t)
	now := time.Now()
	p := healthyHistoryLoad(now)
	r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure { return p }
	for i := 0; i < 7; i++ {
		at := now.Add(time.Duration(i-6) * 5 * time.Second)
		p.SampledAt, p.DeviceSampledAt = at, at
		p.CompactionDebt = 3<<30 + uint64(i)*(128<<20)
		r.refreshHistoryLoad(at)
	}
	result, err := r.OnePass()
	if err != nil || !result.Built || !result.HistoryBusyResourceReady || !result.HistoryForcedBusy {
		t.Fatalf("middle tier did not admit resource-backed work: %+v %v", result, err)
	}
	if r.historyLoad.dutyPPM() != 600000 {
		t.Fatal("middle duty overwritten by CPU burst")
	}
}

func TestHistoryDebtTierReusedHealthyEvidenceCannotPromote(t *testing.T) {
	now := time.Now()
	p := healthyHistoryLoad(now)
	r := historyLoadMetricsFixture(t, &p)
	for i := 0; i < 7; i++ {
		at := now.Add(time.Duration(i-6) * 5 * time.Second)
		p.SampledAt, p.DeviceSampledAt = at, at
		p.CompactionDebt = 3<<30 + uint64(i)*(128<<20)
		r.refreshHistoryLoad(at)
	}
	r.refreshHistoryLoad(now)
	if !r.historyLoad.debtModerateReady(now) {
		t.Fatal("reused moderate evidence lost tier")
	}
	p.DeviceAwait = 3 * time.Millisecond
	r.refreshHistoryLoad(now)
	if r.historyLoad.debtModerateReady(now) {
		t.Fatal("slow evidence admitted")
	}
	p.DeviceAwait = time.Millisecond
	r.refreshHistoryLoad(now)
	if r.historyLoad.debtModerateReady(now) {
		t.Fatal("same engine observation promoted tier")
	}
	p.SampledAt = now.Add(-16 * time.Second)
	r.refreshHistoryLoad(now)
	if r.historyLoad.debtModerate || r.historyLoad.cpuBurstReady(now) {
		t.Fatal("stale engine inherited tier")
	}
}

func TestHistoryDebtMiddleTierAllowsPairButSevereClosesIt(t *testing.T) {
	old := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(old)
	now := time.Now()
	p := healthyHistoryLoad(now)
	r := parallelHistoryEventRunner(&coldBuilderChain{syncRemainingOK: true, syncRemaining: 1000}, t.TempDir(), 8, 4)
	r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure { return p }
	for i := 0; i < 7; i++ {
		at := now.Add(time.Duration(i-6) * 5 * time.Second)
		p.SampledAt, p.DeviceSampledAt = at, at
		p.CompactionDebt = 3<<30 + uint64(i)*(128<<20)
		r.refreshHistoryLoad(at)
	}
	if !r.parallelHistoryEventReady(true, false, now) {
		t.Fatal("middle tier blocked independent pair admission")
	}
	p.SampledAt, p.DeviceSampledAt = now.Add(5*time.Second), now.Add(5*time.Second)
	p.CompactionDebt = 5 << 30
	r.refreshHistoryLoad(p.SampledAt)
	if r.parallelHistoryEventReady(true, false, p.SampledAt) || r.historyLoad.debtModerate {
		t.Fatal("severe trend retained pair")
	}
}

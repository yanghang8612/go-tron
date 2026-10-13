package snapshots

import (
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

func moderateDebtRecoveryFixture(t *testing.T) (*Runner, *maintenance.StoragePressure, time.Time) {
	t.Helper()
	// Leave room for the recovery steps while dutyPPM also checks wall-clock
	// freshness. No sleeps or future observations are needed by this trace.
	now := time.Now().Add(-15 * time.Second)
	p := healthyHistoryLoad(now)
	r := historyLoadMetricsFixture(t, &p)
	for i := 0; i < 7; i++ {
		at := now.Add(time.Duration(i-6) * 5 * time.Second)
		p.SampledAt, p.DeviceSampledAt = at, at
		p.CompactionDebt = 3<<30 + uint64(i)*(128<<20)
		r.refreshHistoryLoad(at)
	}
	if !r.historyLoad.debtModerateReady(now) {
		t.Fatal("fixture did not admit moderate debt")
	}
	return r, &p, now
}

func TestHistoryDebtRecoveryKeepsMiddleDutyUntilGoodSamples(t *testing.T) {
	r, p, now := moderateDebtRecoveryFixture(t)
	now = now.Add(5 * time.Second)
	p.SampledAt, p.DeviceSampledAt, p.CompactionDebt = now, now, 3<<30
	r.refreshHistoryLoad(now)
	if r.historyLoad.level != 1 || r.historyLoad.good != 1 || r.historyLoad.dutyPPM() != 600000 ||
		r.historyLoad.reasons != historyLoadReasonPressureHysteresis|historyLoadReasonRecoverySamples ||
		r.historyLoad.metrics["debt_recovery"].Snapshot().Value() != 1 || r.historyLoad.metrics["debt_tier"].Snapshot().Value() != 0 {
		t.Fatalf("first healthy point lost admitted middle duty: %+v", r.historyLoad)
	}
	for i := 0; i < 10; i++ {
		r.refreshHistoryLoad(now)
	}
	if r.historyLoad.good != 1 || r.historyLoad.dutyPPM() != 600000 {
		t.Fatal("reused healthy point changed recovery")
	}
	for i, want := range []uint64{800000, 900000} {
		now = now.Add(5 * time.Second)
		p.SampledAt, p.DeviceSampledAt = now, now
		r.refreshHistoryLoad(now)
		if r.historyLoad.dutyPPM() != want || r.historyLoad.level != i+2 || r.historyLoad.debtModerate ||
			r.historyLoad.metrics["debt_recovery"].Snapshot().Value() != 0 {
			t.Fatalf("ordinary healthy recovery changed: %+v", r.historyLoad)
		}
	}
}

func TestHistoryDebtRecoveryRevokesOnProtectionAndCannotReacquireReusedProbe(t *testing.T) {
	for name, change := range map[string]func(*maintenance.StoragePressure, time.Time){
		"hard":           func(p *maintenance.StoragePressure, _ time.Time) { p.WriteStalled = true },
		"new-stall":      func(p *maintenance.StoragePressure, _ time.Time) { p.StallCount++ },
		"slow-device":    func(p *maintenance.StoragePressure, _ time.Time) { p.DeviceAwait = 3 * time.Millisecond },
		"unknown-device": func(p *maintenance.StoragePressure, _ time.Time) { p.DeviceAvailable = false },
		"stale-device":   func(p *maintenance.StoragePressure, n time.Time) { p.DeviceSampledAt = n.Add(-16 * time.Second) },
		"l0":             func(p *maintenance.StoragePressure, _ time.Time) { p.L0Sublevels = p.L0CompactionThreshold },
		"unknown-l0":     func(p *maintenance.StoragePressure, _ time.Time) { p.L0CompactionThreshold = 0 },
		"unknown-engine": func(p *maintenance.StoragePressure, _ time.Time) { p.Available = false },
		"stale-engine":   func(p *maintenance.StoragePressure, n time.Time) { p.SampledAt = n.Add(-16 * time.Second) },
		"reset":          func(p *maintenance.StoragePressure, n time.Time) { p.SampledAt = n.Add(-6 * time.Second) },
		"severe":         func(p *maintenance.StoragePressure, _ time.Time) { p.CompactionDebt = 6 << 30 },
	} {
		t.Run(name, func(t *testing.T) {
			r, p, now := moderateDebtRecoveryFixture(t)
			now = now.Add(5 * time.Second)
			p.SampledAt, p.DeviceSampledAt, p.CompactionDebt = now, now, 3<<30
			change(p, now)
			r.refreshHistoryLoad(now)
			if r.historyLoad.debtModerateReady(now) || r.historyLoad.debtModerate || r.historyLoad.metrics["debt_recovery"].Snapshot().Value() != 0 {
				t.Fatal("protection retained recovery middle tier")
			}
			// A reused observation cannot restore the middle tier after a loss
			// of evidence. Ordinary fresh-sample recovery remains independent.
			if name == "slow-device" || name == "l0" || name == "unknown-l0" {
				p.DeviceAwait, p.L0Sublevels, p.L0CompactionThreshold = time.Millisecond, 2, 8
				r.refreshHistoryLoad(now)
				if r.historyLoad.debtModerate {
					t.Fatal("reused observation reacquired carryover")
				}
			}
		})
	}
}

func TestHistoryDebtRecoveryDoesNotInheritAfterGapOrSeverePressure(t *testing.T) {
	for _, severe := range []bool{false, true} {
		t.Run(map[bool]string{false: "gap", true: "severe"}[severe], func(t *testing.T) {
			r, p, now := moderateDebtRecoveryFixture(t)
			if severe {
				now = now.Add(5 * time.Second)
				p.SampledAt, p.DeviceSampledAt, p.CompactionDebt = now, now, 6<<30
				r.refreshHistoryLoad(now)
				now = now.Add(5 * time.Second)
			} else {
				now = now.Add(historyDebtTrendMaxGap + time.Second)
			}
			p.SampledAt, p.DeviceSampledAt, p.CompactionDebt = now, now, 3<<30
			r.refreshHistoryLoad(now)
			if r.historyLoad.debtModerate || r.historyLoad.dutyPPM() != 200000 {
				t.Fatal("non-comparable or severe predecessor inherited middle duty")
			}
		})
	}
}

func TestHistoryDebtRecoveryNeverShortensInstalledDeadline(t *testing.T) {
	r, p, now := moderateDebtRecoveryFixture(t)
	now = now.Add(5 * time.Second)
	p.SampledAt, p.DeviceSampledAt, p.CompactionDebt = now, now, 3<<30
	r.refreshHistoryLoad(now)
	r.cfg.HistoryCatchupMode = HistoryCatchupThroughput
	deadline := now.Add(time.Minute)
	r.extendHistoryNotBefore(deadline)
	result := PassResult{HistoryBuildAttempted: true, HistoryForcedBusy: true}
	r.applyThroughputRecovery(&result, now, now.Add(3*time.Second), nil)
	if got := r.historyNotBefore.Load(); got != deadline.UnixNano() || result.HistoryMinRecovery != 2*time.Second {
		t.Fatalf("deadline shortened or middle recovery lost: %+v", result)
	}
}

package snapshots

import (
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

func TestHistoryDebtTrendFallingSawtooth(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	p := healthyHistoryLoad(now)
	r := historyLoadMetricsFixture(t, &p)
	for i := 0; i < 28; i++ {
		p.SampledAt, p.DeviceSampledAt = now, now
		// Two local rises per cycle, yet every 30-second window has
		// lower net debt. Local troughs must not trigger a 20% duty.
		p.CompactionDebt = 8<<30 - uint64(i*20)*(1<<20) + uint64(i%3*40)*(1<<20)
		r.refreshHistoryLoad(now)
		if r.historyLoad.level == 1 || r.historyLoad.reasons&historyLoadReasonDebtGrowth != 0 {
			t.Fatalf("falling sawtooth pressured at point %d: level=%d debt=%d trend=%+v", i, r.historyLoad.level, p.CompactionDebt, r.historyLoad.debtTrend)
		}
		now = now.Add(5 * time.Second)
	}
	if r.historyLoad.debtTrendCount != historyDebtTrendPoints {
		t.Fatalf("unbounded or incomplete trend: %d", r.historyLoad.debtTrendCount)
	}
}

func TestHistoryDebtTrendFallingSawtoothReleasesPressure(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	p := healthyHistoryLoad(now)
	r := historyLoadMetricsFixture(t, &p)
	for i := 0; i < 15; i++ {
		p.SampledAt, p.DeviceSampledAt = now, now
		p.CompactionDebt = 8<<30 - uint64(i*20)*(1<<20) + uint64(i%3*40)*(1<<20)
		p.WriteStalled = i == 0
		r.refreshHistoryLoad(now)
		if i == 0 && (r.historyLoad.level != 1 || !r.historyLoad.hard) {
			t.Fatal("initial hard pressure was not immediate")
		}
		if i == 9 && r.historyLoad.level != 3 {
			t.Fatalf("falling debt sawtooth kept hysteresis at level %d", r.historyLoad.level)
		}
		now = now.Add(5 * time.Second)
	}
}

func TestHistoryDebtTrendRisingWithRetracements(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	p := healthyHistoryLoad(now)
	r := historyLoadMetricsFixture(t, &p)
	for i, deltaMiB := range []uint64{0, 50, 100, 70, 130, 180, 170} {
		p.SampledAt, p.DeviceSampledAt = now, now
		p.CompactionDebt = 3<<30 + deltaMiB*(1<<20)
		r.refreshHistoryLoad(now)
		if i < 6 && r.historyLoad.reasons&historyLoadReasonDebtGrowth != 0 {
			t.Fatalf("premature debt pressure at point %d", i)
		}
		now = now.Add(5 * time.Second)
	}
	if r.historyLoad.level != 1 || r.historyLoad.reasons&historyLoadReasonDebtGrowth == 0 {
		t.Fatalf("net growth with retracement escaped pressure: level=%d reasons=%d", r.historyLoad.level, r.historyLoad.reasons)
	}
	if r.historyLoad.debtTrendCount != 7 {
		t.Fatalf("trend points = %d", r.historyLoad.debtTrendCount)
	}
	assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/debt_trend_span_ns", int64(30*time.Second))
	assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/debt_trend_net_bytes", 170<<20)
	assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/debt_trend_positive_steps", 4)
}

func TestHistoryDebtTrendMonotonicAndSparse(t *testing.T) {
	for _, cadence := range []time.Duration{5 * time.Second, time.Minute} {
		t.Run(cadence.String(), func(t *testing.T) {
			now := time.Unix(1_700_000_000, 0)
			p := healthyHistoryLoad(now)
			r := historyLoadMetricsFixture(t, &p)
			steps := 7
			if cadence == time.Minute {
				steps = 3
			}
			for i := 0; i < steps; i++ {
				p.SampledAt, p.DeviceSampledAt = now, now
				p.CompactionDebt = 3<<30 + uint64(i)*(1<<20)
				r.refreshHistoryLoad(now)
				if i < steps-1 && r.historyLoad.reasons&historyLoadReasonDebtGrowth != 0 {
					t.Fatalf("premature pressure at %d", i)
				}
				for range 3 {
					r.refreshHistoryLoad(now)
				}
				if r.historyLoad.debtTrendCount != i+1 {
					t.Fatalf("reused timestamp added point: %d", r.historyLoad.debtTrendCount)
				}
				now = now.Add(cadence)
			}
			if r.historyLoad.reasons&historyLoadReasonDebtGrowth == 0 || r.historyLoad.level != 1 {
				t.Fatal("sustained monotonic growth did not pressure")
			}
		})
	}
}

func TestHistoryDebtTrendSparseHighAnchorDoesNotHideNewGrowth(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	p := healthyHistoryLoad(now)
	r := historyLoadMetricsFixture(t, &p)
	for i, debtGiB := range []float64{8, 5, 5.5, 6} {
		p.SampledAt, p.DeviceSampledAt = now, now
		p.CompactionDebt = uint64(debtGiB * float64(uint64(1)<<30))
		r.refreshHistoryLoad(now)
		if i < 3 && r.historyLoad.reasons&historyLoadReasonDebtGrowth != 0 {
			t.Fatalf("early pressure at point %d", i)
		}
		now = now.Add(time.Minute)
	}
	if r.historyLoad.level != 1 || r.historyLoad.reasons&historyLoadReasonDebtGrowth == 0 {
		t.Fatal("old high anchor hid two-minute net debt growth")
	}
	assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/debt_trend_points", 3)
	assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/debt_trend_net_bytes", 1<<30)
}

func TestHistoryDebtTrendSparsePlateauReleasesOldGrowth(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	p := healthyHistoryLoad(now)
	r := historyLoadMetricsFixture(t, &p)
	for i, debtGiB := range []float64{3, 3.5, 4, 4, 4, 4, 4} {
		p.SampledAt, p.DeviceSampledAt = now, now
		p.CompactionDebt = uint64(debtGiB * float64(uint64(1)<<30))
		r.refreshHistoryLoad(now)
		if i == 2 && (r.historyLoad.level != 1 || r.historyLoad.reasons&historyLoadReasonDebtGrowth == 0) {
			t.Fatal("initial sparse growth did not pressure")
		}
		if i >= 4 && r.historyLoad.reasons&historyLoadReasonDebtGrowth != 0 {
			t.Fatalf("old growth persisted after recent flat window at %d", i)
		}
		if i == 6 && r.historyLoad.level != 3 {
			t.Fatalf("flat debt did not finish recovery: level=%d", r.historyLoad.level)
		}
		now = now.Add(time.Minute)
	}
	assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/debt_trend_net_bytes", 0)
	assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/debt_trend_growth_points", 0)
}

func TestHistoryDebtTrendRetracementExtendsRecentWindow(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	p := healthyHistoryLoad(now)
	r := historyLoadMetricsFixture(t, &p)
	for i, debtGiB := range []float64{8, 8.2, 8.1, 8.3} {
		p.SampledAt, p.DeviceSampledAt = now, now
		p.CompactionDebt = uint64(debtGiB * float64(uint64(1)<<30))
		r.refreshHistoryLoad(now)
		if i < 3 && r.historyLoad.reasons&historyLoadReasonDebtGrowth != 0 {
			t.Fatalf("premature pressure at %d", i)
		}
		now = now.Add(15 * time.Second)
	}
	if r.historyLoad.level != 1 || r.historyLoad.reasons&historyLoadReasonDebtGrowth == 0 {
		t.Fatal("recent net growth with retracement escaped pressure")
	}
	assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/debt_trend_points", 3)
	assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/debt_trend_positive_steps", 1)
	assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/debt_trend_growth_points", 4)
	assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/debt_trend_growth_positive_steps", 2)
}

func TestHistoryDebtTrendPressureRecoveryAndRenewedGrowth(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	p := healthyHistoryLoad(now)
	r := historyLoadMetricsFixture(t, &p)
	step := func(debt uint64) {
		p.SampledAt, p.DeviceSampledAt = now, now
		p.CompactionDebt = debt
		r.refreshHistoryLoad(now)
		now = now.Add(5 * time.Second)
	}
	for i := 0; i < 7; i++ {
		step(3<<30 + uint64(i)*(1<<20))
	}
	if r.historyLoad.level != 1 || r.historyLoad.reasons&historyLoadReasonDebtGrowth == 0 {
		t.Fatal("initial sustained growth did not pressure")
	}
	for i := 0; i < 12; i++ {
		step(3<<30 + 6<<20 - uint64(i*20)*(1<<20) + uint64(i%3*40)*(1<<20))
	}
	if r.historyLoad.level != 3 || r.historyLoad.reasons&historyLoadReasonDebtGrowth != 0 {
		t.Fatal("falling sawtooth did not finish recovery")
	}
	base := p.CompactionDebt
	for _, deltaMiB := range []uint64{0, 50, 100, 70, 130, 180, 170} {
		step(base + deltaMiB*(1<<20))
	}
	if r.historyLoad.level != 1 || r.historyLoad.reasons&historyLoadReasonDebtGrowth == 0 {
		t.Fatal("renewed net growth with retracements escaped pressure")
	}
}

func TestHistoryDebtTrendRecoveryRespectsDeviceAndL0(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*maintenance.StoragePressure)
	}{
		{"device pressure", func(p *maintenance.StoragePressure) {
			p.DeviceBusyPPM, p.DeviceQueueMilli, p.DeviceAwait = 950_000, 2_000, 5*time.Millisecond
		}},
		{"L0 soft threshold", func(p *maintenance.StoragePressure) { p.L0Sublevels = p.L0CompactionThreshold }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(1_700_000_000, 0)
			p := healthyHistoryLoad(now)
			r := historyLoadMetricsFixture(t, &p)
			for i := 0; i < 9; i++ {
				p.SampledAt, p.DeviceSampledAt = now, now
				p.CompactionDebt = 8<<30 - uint64(i*20)*(1<<20) + uint64(i%3*40)*(1<<20)
				p.WriteStalled = i == 0
				if i > 0 {
					tc.change(&p)
				}
				r.refreshHistoryLoad(now)
				if i >= 6 && (r.historyLoad.level == 3 || r.historyLoad.cpuBurstReady(now)) {
					t.Fatalf("falling debt bypassed %s at point %d: level=%d", tc.name, i, r.historyLoad.level)
				}
				now = now.Add(5 * time.Second)
			}
		})
	}
}

func TestHistoryDebtTrendInvalidObservationsResetWindow(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*maintenance.StoragePressure, time.Time)
	}{
		{"unavailable", func(p *maintenance.StoragePressure, _ time.Time) { p.Available = false }},
		{"stale", func(p *maintenance.StoragePressure, now time.Time) { p.SampledAt = now.Add(-16 * time.Second) }},
		{"future", func(p *maintenance.StoragePressure, now time.Time) { p.SampledAt = now.Add(2 * time.Second) }},
		{"backward timestamp", func(p *maintenance.StoragePressure, now time.Time) { p.SampledAt = now.Add(-6 * time.Second) }},
		{"stall counter reset", func(p *maintenance.StoragePressure, _ time.Time) { p.StallCount = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(1_700_000_000, 0)
			p := healthyHistoryLoad(now)
			p.StallCount = 1
			r := historyLoadMetricsFixture(t, &p)
			for i := 0; i < 3; i++ {
				p.SampledAt, p.DeviceSampledAt = now, now
				p.CompactionDebt = 3<<30 + uint64(i)*(1<<20)
				r.refreshHistoryLoad(now)
				now = now.Add(5 * time.Second)
			}
			tc.change(&p, now)
			r.refreshHistoryLoad(now)
			if tc.name == "backward timestamp" || tc.name == "stall counter reset" {
				if r.historyLoad.debtTrendCount != 1 {
					t.Fatalf("reset retained trend: %d", r.historyLoad.debtTrendCount)
				}
			} else if r.historyLoad.debtTrendCount != 0 {
				t.Fatalf("unknown engine retained trend: %d", r.historyLoad.debtTrendCount)
			}
			if r.historyLoad.reasons&historyLoadReasonDebtGrowth != 0 {
				t.Fatal("invalid observation invented debt pressure")
			}
		})
	}
}

func TestHistoryDebtTrendGapAndUnknownDevice(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	p := healthyHistoryLoad(now)
	p.DeviceAvailable = false
	r := historyLoadMetricsFixture(t, &p)
	for i := 0; i < 7; i++ {
		p.SampledAt = now
		p.CompactionDebt = 3<<30 + uint64(i)*(1<<20)
		r.refreshHistoryLoad(now)
		now = now.Add(5 * time.Second)
	}
	if r.historyLoad.level != 1 || r.historyLoad.reasons&historyLoadReasonDebtGrowth == 0 || r.historyLoad.cpuBurstReady(now) {
		t.Fatal("unknown device erased engine debt pressure or admitted CPU burst")
	}
	now = now.Add(2 * time.Minute)
	p.SampledAt = now
	p.CompactionDebt += 1 << 20
	r.refreshHistoryLoad(now)
	if r.historyLoad.debtTrendCount != 1 || r.historyLoad.reasons&historyLoadReasonDebtGrowth != 0 {
		t.Fatalf("long observation gap retained trend: points=%d reasons=%d", r.historyLoad.debtTrendCount, r.historyLoad.reasons)
	}
}

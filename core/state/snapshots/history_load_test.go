package snapshots

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

func healthyHistoryLoad(now time.Time) maintenance.StoragePressure {
	return maintenance.StoragePressure{Available: true, SampledAt: now,
		L0Sublevels: 2, L0CompactionThreshold: 8, L0StopWritesThreshold: 192,
		MemTableCount: 1, MemTableStopWritesThreshold: 4,
		DeviceAvailable: true, DeviceSampledAt: now, DeviceBusyPPM: 500_000,
		DeviceQueueMilli: 500, DeviceAwait: time.Millisecond}
}

func TestHistoryLoadHysteresisAndFreshness(t *testing.T) {
	r := throughputFixture(t)
	now := time.Now()
	p := healthyHistoryLoad(now)
	r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure { return p }
	step := func() { p.SampledAt, p.DeviceSampledAt = now, now; r.refreshHistoryLoad(now) }
	step()
	for i := 0; i < 20; i++ {
		r.refreshHistoryLoad(now)
	}
	if r.historyLoad.level != 2 || r.historyLoad.good != 1 {
		t.Fatal("reused sample raised budget", r.historyLoad)
	}
	for i := 0; i < 2; i++ {
		now = now.Add(5 * time.Second)
		step()
	}
	if r.historyLoad.level != 3 {
		t.Fatal("fresh healthy samples did not recover", r.historyLoad)
	}
	p.WriteStalled = true
	step()
	if !r.historyLoad.hard || r.historyLoad.level != 1 || r.historyLoad.good != 0 {
		t.Fatal("stall did not reduce budget", r.historyLoad)
	}
	p.WriteStalled = false
	now = now.Add(5 * time.Second)
	step()
	if r.historyLoad.level != 1 {
		t.Fatal("one good sample erased pressure", r.historyLoad)
	}
	now = now.Add(5 * time.Second)
	step()
	if r.historyLoad.level != 2 {
		t.Fatal("second good sample did not begin recovery", r.historyLoad)
	}
	now = now.Add(5 * time.Second)
	step()
	if r.historyLoad.level != 3 {
		t.Fatal("third good sample did not recover", r.historyLoad)
	}
	r.refreshHistoryLoad(now.Add(16 * time.Second))
	if r.historyLoad.level != 0 || r.historyLoad.hard {
		t.Fatal("stale sample treated as live", r.historyLoad)
	}
	now = now.Add(20 * time.Second)
	p.DeviceAvailable = false
	step()
	for i := 0; i < 4; i++ {
		now = now.Add(5 * time.Second)
		step()
	}
	if r.historyLoad.level != 2 {
		t.Fatal("unknown shared device admitted maximum budget", r.historyLoad)
	}
}

func TestHistoryLoadDebtAndSharedDevicePressure(t *testing.T) {
	r := throughputFixture(t)
	now := time.Now()
	p := healthyHistoryLoad(now)
	p.CompactionDebt = 3 << 30
	r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure { return p }
	r.refreshHistoryLoad(now)
	for i := 0; i < 2; i++ {
		now = now.Add(5 * time.Second)
		p.SampledAt, p.DeviceSampledAt = now, now
		p.CompactionDebt += 1 << 20
		r.refreshHistoryLoad(now)
	}
	if r.historyLoad.level != 1 || r.historyLoad.hard {
		t.Fatal("growing debt was not soft pressure", r.historyLoad)
	}
	p.CompactionDebt = 0
	p.DeviceBusyPPM, p.DeviceQueueMilli, p.DeviceAwait = 999_000, 4000, 10*time.Millisecond
	now = now.Add(5 * time.Second)
	p.SampledAt, p.DeviceSampledAt = now, now
	r.refreshHistoryLoad(now)
	if r.historyLoad.level != 1 || r.historyLoad.hard {
		t.Fatal("shared queue signal", r.historyLoad)
	}
	// Busy alone does not imply the physical device's bandwidth limit.
	p.DeviceQueueMilli, p.DeviceAwait = 500, time.Millisecond
	for i := 0; i < 3; i++ {
		now = now.Add(5 * time.Second)
		p.SampledAt, p.DeviceSampledAt = now, now
		r.refreshHistoryLoad(now)
	}
	if r.historyLoad.level != 3 {
		t.Fatal("busy alone starved cold work", r.historyLoad)
	}
}

func TestHistoryLoadDensityJumpShrinksBothBounds(t *testing.T) {
	r := throughputFixture(t)
	r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure { return maintenance.StoragePressure{} }
	r.historyLoad.level = 3
	r.historyLoad.history = historyWorkSample{blocks: 1000, txnums: 100_000, work: 4 * time.Second, bytes: 64 << 20}
	b, tx := r.adaptiveHistoryBatchLimits(5000, 390625)
	if b != 1250 || tx != 125000 {
		t.Fatalf("growth exceeds 25%%: %d %d", b, tx)
	}
	// Model a density transition: the same block/tx count takes 100 times
	// longer and produces 100 times more bytes. No height-based heuristic.
	r.historyLoad.history.work *= 100
	r.historyLoad.history.bytes *= 100
	b, tx = r.adaptiveHistoryBatchLimits(5000, 390625)
	if b != 20 || tx != 2000 {
		t.Fatalf("dense batch not reduced: %d %d", b, tx)
	}
	r.historyLoad.level = 1
	b, tx = r.adaptiveHistoryBatchLimits(5000, 390625)
	if b != 5 || tx != 500 {
		t.Fatalf("pressure did not further reduce dense range: %d %d", b, tx)
	}
	r.historyLoad.history.work = time.Duration(math.MaxInt64)
	r.historyLoad.history.bytes = math.MaxUint64
	b, tx = r.adaptiveHistoryBatchLimits(5000, 390625)
	if b != 1 || tx != 1 {
		t.Fatalf("complete-block floor/overflow: %d %d", b, tx)
	}
	if r.adaptiveEventBatchLimit(65536) != 2 {
		t.Fatal("independent event gap started with full freezer segment")
	}
}

func TestHistoryHardPressureRetainsPruneOpportunity(t *testing.T) {
	r := throughputFixture(t)
	p := healthyHistoryLoad(time.Now())
	p.WriteStalled = true
	r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure { p.SampledAt = time.Now(); return p }
	called := false
	result, err := r.OnePassWithMaintenanceContext(context.Background(), func(_ context.Context, result PassResult) error {
		called = true
		if !result.HistoryLoadDeferred || result.HistoryBuildAttempted {
			t.Fatal("load gate ran after expensive build")
		}
		return nil
	})
	if err != nil || !called || result.HistoryBuildAttempted || !result.HistoryLoadDeferred || result.Compaction.Merged || result.HistoryRetryAfter <= 0 {
		t.Fatalf("hard pressure/callback: %+v %v", result, err)
	}
	p.WriteStalled = false
	result, err = r.OnePass()
	if err != nil || !result.Built || result.HistoryBatchBlocks != 2 {
		t.Fatalf("recovery did not use a bounded initial slice: %+v %v", result, err)
	}
}

func TestHistoryRecoveryExcludesLongMergeButRecordsWholeMaintenance(t *testing.T) {
	r := throughputFixture(t)
	r.historyLoad.level = 3
	start := time.Now()
	result := PassResult{Built: true, HistoryBuildAttempted: true, HistoryForcedBusy: true, CompactionDuration: 15 * time.Minute}
	r.applyThroughputRecovery(&result, start, start.Add(15*time.Minute+6*time.Second), nil)
	if result.HistoryMaintenanceDuration != 15*time.Minute+6*time.Second || result.HistoryRecoveryCost != 6*time.Second || result.HistoryMinRecovery != 4*time.Second {
		t.Fatalf("merge poisoned history budget: %+v", result)
	}
	r.historyLoad.level = 0
	if got := r.throughputRecovery(120*time.Second, false); got != 480*time.Second {
		t.Fatal("oversized ordinary block lost its real recovery cost", got)
	}
	for _, level := range []int{0, 1, 2, 3} {
		r.historyLoad.level = level
		if got := r.throughputRecovery(time.Duration(math.MaxInt64), false); got <= time.Hour {
			t.Fatal("overflowed recovery", got)
		}
		if got := r.throughputRecovery(time.Millisecond, true); got < time.Minute {
			t.Fatal("failure did not back off", got)
		}
	}
}

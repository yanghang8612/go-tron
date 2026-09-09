package freezer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
)

func transactionIndexPressureAt(now time.Time) maintenance.StoragePressure {
	p := healthyTransactionIndexPressure()
	p.SampledAt, p.DeviceSampledAt = now, now
	return p
}

func TestTransactionIndexPressureReasonsAndRepeatedSamples(t *testing.T) {
	now := time.Unix(1800000000, 0)
	for _, tc := range []struct {
		name string
		edit func(*maintenance.StoragePressure)
		want transactionIndexHealthReason
	}{
		{"engine_unknown", func(p *maintenance.StoragePressure) { p.Available = false }, txIndexHealthEngineUnknown},
		{"engine_stale", func(p *maintenance.StoragePressure) { p.SampledAt = now.Add(-16 * time.Second) }, txIndexHealthEngineStale},
		{"engine_future", func(p *maintenance.StoragePressure) { p.SampledAt = now.Add(2 * time.Second) }, txIndexHealthEngineStale},
		{"device_unknown", func(p *maintenance.StoragePressure) { p.DeviceAvailable = false }, txIndexHealthDeviceUnknown},
		{"device_negative_await", func(p *maintenance.StoragePressure) { p.DeviceAwait = -1 }, txIndexHealthDeviceUnknown},
		{"device_stale", func(p *maintenance.StoragePressure) { p.DeviceSampledAt = now.Add(-16 * time.Second) }, txIndexHealthDeviceStale},
		{"write_stall", func(p *maintenance.StoragePressure) { p.WriteStalled = true; p.DeviceAvailable = false }, txIndexHealthWriteStalled},
		{"memtable_hard", func(p *maintenance.StoragePressure) { p.MemTableStopWritesThreshold = 4; p.MemTableCount = 3 }, txIndexHealthMemTableHard},
		{"l0_hard", func(p *maintenance.StoragePressure) { p.L0Sublevels = 9 }, txIndexHealthL0Hard},
		{"l0_soft", func(p *maintenance.StoragePressure) { p.L0Sublevels = 8 }, txIndexHealthL0Soft},
		{"device_latency", func(p *maintenance.StoragePressure) { p.DeviceAwait = 3 * time.Millisecond }, txIndexHealthDeviceLatency},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := transactionIndexPressureAt(now)
			tc.edit(&p)
			s := transactionIndexPressureState{}
			for i := 0; i < 3; i++ {
				if reason := s.assess(p, now); reason != tc.want {
					t.Fatalf("repeated sample %d reason=%d want=%d", i, reason, tc.want)
				}
			}
		})
	}
	s := transactionIndexPressureState{}
	p := transactionIndexPressureAt(now)
	p.CompactionDebt = 2 << 30
	if got := s.assess(p, now); got != txIndexHealthHealthy {
		t.Fatalf("large debt without growth was rejected: %d", got)
	}
	for i := 1; i <= 2; i++ {
		now = now.Add(5 * time.Second)
		p = transactionIndexPressureAt(now)
		p.CompactionDebt = (2 << 30) + uint64(i)
		for repeat := 0; repeat < 5; repeat++ {
			reason := s.assess(p, now)
			want := txIndexHealthHealthy
			if i == 2 {
				want = txIndexHealthDebtGrowth
			}
			if reason != want || s.debtRises != i {
				t.Fatalf("sample %d repeat %d reason/rises=%d/%d", i, repeat, reason, s.debtRises)
			}
		}
	}
	// A healthy sample may begin recovery, but reusing its cached device or
	// engine observation cannot finish it, even via repeated explicit callers.
	now = now.Add(5 * time.Second)
	p = transactionIndexPressureAt(now)
	if reason := s.assess(p, now); reason != txIndexHealthRecovering {
		t.Fatalf("first recovery observation=%d", reason)
	}
	for i := 0; i < 10; i++ {
		if reason := s.assess(p, now.Add(time.Second)); reason != txIndexHealthRecovering || s.goodSamples != 1 {
			t.Fatalf("duplicate sample completed recovery: %d good=%d", reason, s.goodSamples)
		}
	}
	p.SampledAt = now.Add(5 * time.Second)
	if reason := s.assess(p, p.SampledAt); reason != txIndexHealthRecovering {
		t.Fatalf("old cached device completed recovery: %d", reason)
	}
	p.DeviceSampledAt = p.SampledAt
	if reason := s.assess(p, p.SampledAt); reason != txIndexHealthHealthy {
		t.Fatalf("two distinct healthy observations failed to recover: %d", reason)
	}
}

func TestTransactionIndexPressureStallAndCounterResetNeedNewEvidence(t *testing.T) {
	now := time.Unix(1800000000, 0)
	s := transactionIndexPressureState{}
	p := transactionIndexPressureAt(now)
	_ = s.assess(p, now)
	p.SampledAt = now.Add(time.Second)
	p.StallCount = 1
	for i := 0; i < 3; i++ {
		if got := s.assess(p, p.SampledAt); got != txIndexHealthNewStall {
			t.Fatalf("same stall observation lost reason: %d", got)
		}
	}
	p = transactionIndexPressureAt(now.Add(6 * time.Second))
	p.StallCount = 1
	if got := s.assess(p, p.SampledAt); got != txIndexHealthRecovering {
		t.Fatalf("stall stopped without first healthy evidence: %d", got)
	}
	p = transactionIndexPressureAt(now.Add(11 * time.Second)) // counter reset
	if got := s.assess(p, p.SampledAt); got != txIndexHealthRecovering || s.goodSamples != 1 {
		t.Fatalf("counter reset counted as second good sample: %d/%d", got, s.goodSamples)
	}
}

type transactionIndexPressureStore struct {
	*auditTransactionIndexMaintenanceStore
	afterPublish func()
}

func (s *transactionIndexPressureStore) PublishTransactionIndexRun(result rawdbfreezer.TransactionIndexBuildResult) error {
	if err := s.auditTransactionIndexMaintenanceStore.PublishTransactionIndexRun(result); err != nil {
		return err
	}
	if s.afterPublish != nil {
		s.afterPublish()
	}
	return nil
}

func TestTransactionIndexSoftCompletionRecoversBeforeFiveMinutes(t *testing.T) {
	r, store := newTransactionBudgetFixture(t)
	r.cfg.MetricsNamespace = "test/txindex-soft-recovery/"
	now := time.Now()
	r.txIndexBudget.clock = func() time.Time { return now }
	soft := false
	r.cfg.TransactionIndexLoadProbe = func() maintenance.StoragePressure {
		p := transactionIndexPressureAt(now)
		if soft {
			p.L0Sublevels = 8
		}
		return p
	}
	r.freezer = &transactionIndexPressureStore{auditTransactionIndexMaintenanceStore: store, afterPublish: func() {
		now = now.Add(2 * time.Second)
		soft = true
	}}
	start := now
	if changed, err := r.MaintainTransactionIndexOnce(); err != nil || !changed || store.coverage != 8192 {
		t.Fatalf("real first leaf=%t/%v coverage=%d", changed, err, store.coverage)
	}
	b := &r.txIndexBudget
	if !b.notBefore.Equal(start.Add(10*time.Second)) || !b.conservativeUntil.IsZero() || !b.retry.Equal(now.Add(15*time.Second)) {
		t.Fatalf("soft pressure mixed work and probe deadlines: work=%s legacy=%s retry=%s", b.notBefore, b.conservativeUntil, b.retry)
	}
	metric := func(suffix string) int64 { return runnerGaugeValue(t, r.cfg.MetricsNamespace+"txindex/budget/"+suffix) }
	if metric("admission/healthy") != 1 || metric("completion/healthy") != 0 || metric("completion/reason") != int64(txIndexHealthL0Soft) {
		t.Fatal("admission health masked completion pressure")
	}
	if metric("wait/deadline_unix_nano") != b.retry.UnixNano() || metric("wait/remaining_at_sample") != int64(15*time.Second) || metric("work_not_before_unix_nano") != b.notBefore.UnixNano() {
		t.Fatal("wait metrics do not describe the distinct probe and work deadlines")
	}
	soft = false
	now = b.retry
	if changed, err := r.MaintainTransactionIndexOnce(); changed || err != nil {
		t.Fatalf("single healthy sample restarted work: %t/%v", changed, err)
	}
	now = b.retry
	r.freezer = store
	if changed, err := r.MaintainTransactionIndexOnce(); !changed || err != nil || store.coverage != 16384 {
		t.Fatalf("stable health retained five-minute cap: %t/%v coverage=%d", changed, err, store.coverage)
	}
	if now.Sub(start) >= time.Minute || !b.conservativeUntil.IsZero() {
		t.Fatal("soft recovery retained a conservative five-minute deadline")
	}
}

func TestTransactionIndexPressureEvidencePrecedesLargeBatchWorkDeadline(t *testing.T) {
	r, _ := newTransactionBudgetFixture(t)
	now := time.Now()
	r.txIndexBudget.clock = func() time.Time { return now }
	soft := false
	r.cfg.TransactionIndexLoadProbe = func() maintenance.StoragePressure {
		p := transactionIndexPressureAt(now)
		if soft {
			p.DeviceAwait = 3 * time.Millisecond
		}
		return p
	}
	lease, ok := r.beginTransactionIndexBudget()
	if !ok {
		t.Fatal("first batch not admitted")
	}
	now = now.Add(20 * time.Second) // first unusually costly batch, no prediction
	soft = true
	lease.Close()
	b := &r.txIndexBudget
	workDeadline := b.notBefore
	if !workDeadline.Equal(now.Add(80 * time.Second)) {
		t.Fatal("actual complete work did not impose 4x recovery")
	}
	soft = false
	for i := 0; i < 2; i++ {
		now = b.retry
		if next, admitted := r.beginTransactionIndexBudget(); admitted {
			next.Close()
			t.Fatal("healthy evidence bypassed large-batch work recovery")
		}
	}
	if b.pressure.recovering || !b.retry.Equal(workDeadline) || !b.notBefore.Equal(workDeadline) {
		t.Fatalf("did not accumulate stable evidence before work deadline: recovering=%t retry=%s work=%s", b.pressure.recovering, b.retry, b.notBefore)
	}
	// Other maintenance may still own the gate when recovery completes. That
	// does not erase stable pressure evidence or create another stability wait.
	gate := maintenance.NewHeavyWorkGate()
	r.cfg.HeavyWorkGate = gate
	background, _ := gate.TryAcquire()
	now = workDeadline
	if next, admitted := r.beginTransactionIndexBudget(); admitted {
		next.Close()
		t.Fatal("overlapped active history lease")
	}
	if b.pressure.recovering || !b.reservation.Active() {
		t.Fatal("gate contention lost stable health or ready reservation")
	}
	background()
	now = b.retry
	next, admitted := r.beginTransactionIndexBudget()
	if !admitted {
		t.Fatal("ready reserved index needed another stable-health window")
	}
	next.Close()
}

func TestTransactionIndexUnknownDuringSoftRecoveryRetainsConservativeFloor(t *testing.T) {
	r, _ := newTransactionBudgetFixture(t)
	now := time.Now()
	r.txIndexBudget.clock = func() time.Time { return now }
	mode := "healthy"
	r.cfg.TransactionIndexLoadProbe = func() maintenance.StoragePressure {
		p := transactionIndexPressureAt(now)
		switch mode {
		case "soft":
			p.DeviceAwait = 3 * time.Millisecond
		case "unknown":
			return maintenance.StoragePressure{}
		}
		return p
	}
	started := now
	lease, _ := r.beginTransactionIndexBudget()
	now = now.Add(time.Second)
	mode = "soft"
	lease.Close()
	now = r.txIndexBudget.retry
	mode = "unknown"
	if next, admitted := r.beginTransactionIndexBudget(); admitted {
		next.Close()
		t.Fatal("unknown probe accelerated soft recovery")
	}
	want := started.Add(5 * time.Minute)
	if !r.txIndexBudget.conservativeUntil.Equal(want) {
		t.Fatalf("unknown did not upgrade the work floor: %s", r.txIndexBudget.conservativeUntil)
	}
	mode = "healthy"
	for i := 0; i < 2; i++ {
		now = r.txIndexBudget.retry
		if next, admitted := r.beginTransactionIndexBudget(); admitted {
			next.Close()
			t.Fatal("healthy recheck discarded the unknown-observation floor")
		}
	}
	if !r.txIndexBudget.retry.Equal(want) || r.txIndexBudget.pressure.recovering {
		t.Fatal("unknown floor or stable evidence was lost")
	}
}

func TestTransactionIndexErrorBackoffAndConcurrentCompletionAreIndependent(t *testing.T) {
	r, _ := newTransactionBudgetFixture(t)
	now := time.Now()
	r.txIndexBudget.clock = func() time.Time { return now }
	r.cfg.HeavyMaintenanceErrorBackoff = 30 * time.Minute
	r.cfg.TransactionIndexLoadProbe = func() maintenance.StoragePressure { return transactionIndexPressureAt(now) }
	gate := maintenance.NewHeavyWorkGate()
	r.cfg.HeavyWorkGate = gate
	lease, ok := r.beginTransactionIndexBudget()
	if !ok {
		t.Fatal("failed to admit test lease")
	}
	now = now.Add(2 * time.Second)
	r.lastTxIndexMaintenanceError.Store(now.UnixNano())
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); lease.Close() }()
	}
	wg.Wait()
	b := &r.txIndexBudget
	if b.totalWork != 2*time.Second || !b.retry.Equal(now.Add(30*time.Minute)) {
		t.Fatalf("completion recharged or error backoff lost: work=%s retry=%s", b.totalWork, b.retry)
	}
	if release, ok := gate.TryAcquire(); !ok {
		t.Fatal("failed completion leaked gate token")
	} else {
		release()
	}
	now = now.Add(time.Minute)
	if next, admitted := r.beginTransactionIndexBudget(); admitted {
		next.Close()
		t.Fatal("healthy sample erased actual error backoff")
	}
	if !b.retry.Equal(time.Unix(0, r.lastTxIndexMaintenanceError.Load()).Add(30 * time.Minute)) {
		t.Fatal("error retry was replaced by a short pressure probe")
	}
}

func TestTransactionIndexCancellationAfterPublicationDoesNotPruneOrLeakGate(t *testing.T) {
	r, store := newTransactionBudgetFixture(t)
	r.cfg.MetricsNamespace = "test/txindex-canceled-pressure/"
	r.cfg.HeavyMaintenanceErrorBackoff = 30 * time.Minute
	gate := maintenance.NewHeavyWorkGate()
	r.cfg.HeavyWorkGate = gate
	r.freezer = &transactionIndexPressureStore{auditTransactionIndexMaintenanceStore: store, afterPublish: r.pauseCancel}
	changed, err := r.MaintainTransactionIndexOnce()
	if !errors.Is(err, context.Canceled) || !changed || store.coverage != 8192 {
		t.Fatalf("canceled published leaf=%t/%v coverage=%d", changed, err, store.coverage)
	}
	pruned, initialized, progressErr := r.transactionIndexPruneProgress(store.coverage)
	if progressErr != nil || !initialized || pruned != 0 {
		t.Fatalf("cancellation advanced durable delete cursor: %d/%t/%v", pruned, initialized, progressErr)
	}
	if got := runnerGaugeValue(t, r.cfg.MetricsNamespace+"txindex/budget/completion/reason"); got != int64(txIndexHealthWorkError) {
		t.Fatalf("cancellation was reported as a healthy completion: %d", got)
	}
	if release, ok := gate.TryAcquire(); !ok {
		t.Fatal("canceled published leaf leaked the gate lease")
	} else {
		release()
	}
	r.resetTransactionIndexBudget()
	if !r.transactionIndexRetryDeadline().IsZero() || r.txIndexBudget.reservation.Active() {
		t.Fatal("shutdown retained a pressure timer or reservation")
	}
}

func TestTransactionIndexProbeFreshnessIncludesCompleteReadCost(t *testing.T) {
	r, _ := newTransactionBudgetFixture(t)
	now := time.Now()
	r.txIndexBudget.clock = func() time.Time { return now }
	r.cfg.TransactionIndexLoadProbe = func() maintenance.StoragePressure {
		now = now.Add(2 * time.Second)
		return transactionIndexPressureAt(now)
	}
	lease, admitted := r.beginTransactionIndexBudget()
	if !admitted {
		t.Fatal("sample collected by a slow probe was misclassified as future data")
	}
	now = now.Add(20 * time.Second)
	lease.Close()
	if r.txIndexBudget.totalWork != 24*time.Second || !r.txIndexBudget.notBefore.Equal(now.Add(96*time.Second)) {
		t.Fatalf("complete probe cost omitted: work=%s notBefore=%s", r.txIndexBudget.totalWork, r.txIndexBudget.notBefore)
	}
}

func TestTransactionIndexCompletionProbePanicReturnsGateOnce(t *testing.T) {
	r, _ := newTransactionBudgetFixture(t)
	gate := maintenance.NewHeavyWorkGate()
	r.cfg.HeavyWorkGate = gate
	calls := 0
	r.cfg.TransactionIndexLoadProbe = func() maintenance.StoragePressure {
		calls++
		if calls > 1 {
			panic("completion probe failed")
		}
		return healthyTransactionIndexPressure()
	}
	lease, admitted := r.beginTransactionIndexBudget()
	if !admitted {
		t.Fatal("test lease was not admitted")
	}
	func() {
		defer func() {
			if got := recover(); got != "completion probe failed" {
				t.Fatalf("completion panic not propagated: %v", got)
			}
		}()
		lease.Close()
	}()
	work := r.txIndexBudget.totalWork
	lease.Close()
	if calls != 2 || r.txIndexBudget.totalWork != work {
		t.Fatal("second release repeated a failed completion probe/accounting")
	}
	if release, ok := gate.TryAcquire(); !ok {
		t.Fatal("completion probe panic leaked the gate token")
	} else {
		release()
	}
}

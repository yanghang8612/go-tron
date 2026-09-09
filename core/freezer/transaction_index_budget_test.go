package freezer

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
)

func healthyTransactionIndexPressure() maintenance.StoragePressure {
	now := time.Now()
	return maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now, DeviceAwait: time.Millisecond, L0CompactionThreshold: 4, L0StopWritesThreshold: 12}
}

func newTransactionBudgetFixture(t *testing.T) (*Runner, *auditTransactionIndexMaintenanceStore) {
	t.Helper()
	chain := newFakeChain()
	chain.setSolidified(-1)
	t.Cleanup(func() { _ = chain.db.Close() })
	store := &auditTransactionIndexMaintenanceStore{FreezerStore: wrapFreezer(newFreezer(t)), dir: t.TempDir(), v2Coverage: 16 * defaultV2SegmentBlocks}
	r := New(chain, store, Config{Enabled: true, V2Enabled: true, TransactionIndexEnabled: true, TransactionIndexPrefixBits: 8,
		SyncActive: func() bool { return true }, TransactionIndexLoadProbe: healthyTransactionIndexPressure,
		CatchupMaintenanceInterval: 5 * time.Minute, Interval: time.Hour})
	return r, store
}

func TestTransactionIndexBudgetReplacesFixedCapAndRetainsUnknownFallback(t *testing.T) {
	r, store := newTransactionBudgetFixture(t)
	if changed, err := r.MaintainTransactionIndexOnce(); err != nil || !changed || store.coverage != 8192 {
		t.Fatalf("first bounded quantum = %t/%v coverage=%d", changed, err, store.coverage)
	}
	if changed, err := r.MaintainTransactionIndexOnce(); err != nil || changed {
		t.Fatalf("did not yield after first quantum: %t/%v", changed, err)
	}
	r.txIndexBudget.notBefore = time.Now().Add(-time.Second)
	if changed, err := r.MaintainTransactionIndexOnce(); err != nil || !changed || store.coverage != 16384 {
		t.Fatalf("healthy duty still has five-minute cap = %t/%v coverage=%d", changed, err, store.coverage)
	}
	r.txIndexBudget.notBefore = time.Now().Add(-time.Second)
	r.cfg.TransactionIndexLoadProbe = func() maintenance.StoragePressure { return maintenance.StoragePressure{} }
	if changed, err := r.MaintainTransactionIndexOnce(); err != nil || changed {
		t.Fatalf("unknown engine/device was accelerated: %t/%v", changed, err)
	}
	if wait := time.Until(r.txIndexBudget.conservativeUntil); wait < 4*time.Minute {
		t.Fatalf("unknown sample lost conservative work interval: %s", wait)
	}
	if wait := time.Until(r.txIndexBudget.retry); wait > transactionIndexHardRecheck {
		t.Fatalf("unknown sample did not schedule a bounded observation: %s", wait)
	}
}

func TestTransactionIndexBudgetRejectsPressureAndCancelsReservation(t *testing.T) {
	r, store := newTransactionBudgetFixture(t)
	gate := maintenance.NewHeavyWorkGate()
	r.cfg.HeavyWorkGate = gate
	background, ok := gate.TryAcquire()
	if !ok {
		t.Fatal("could not hold simulated history work")
	}
	defer background()
	if changed, err := r.MaintainTransactionIndexOnce(); err != nil || changed || !r.txIndexBudget.reservation.Active() {
		t.Fatalf("eligible blocked index did not reserve next turn: %t/%v", changed, err)
	}
	r.cfg.TransactionIndexLoadProbe = func() maintenance.StoragePressure {
		p := healthyTransactionIndexPressure()
		p.WriteStalled = true
		return p
	}
	if changed, err := r.MaintainTransactionIndexOnce(); err != nil || changed || r.txIndexBudget.reservation.Active() {
		t.Fatalf("pressure did not cancel waiting index: %t/%v", changed, err)
	}
	background()
	if release, ok := gate.TryAcquire(); !ok {
		t.Fatal("canceled index blocked other maintenance")
	} else {
		release()
	}
	// An already-caught-up index must never reserve the gate or renew a timer.
	store.v2Coverage = store.coverage
	r.cfg.TransactionIndexLoadProbe = healthyTransactionIndexPressure
	if changed, err := r.MaintainTransactionIndexOnce(); err != nil || changed || !r.txIndexBudget.retry.IsZero() || r.txIndexBudget.reservation.Active() {
		t.Fatalf("unready index retained a retry/reservation: %t/%v", changed, err)
	}
}

func TestTransactionIndexBudgetFreshnessAndRecoveryCost(t *testing.T) {
	for _, mode := range []string{"stale_engine", "stale_device", "unknown_device", "high_latency", "l0_pressure", "write_stall"} {
		t.Run(mode, func(t *testing.T) {
			p := healthyTransactionIndexPressure()
			switch mode {
			case "stale_engine":
				p.SampledAt = p.SampledAt.Add(-16 * time.Second)
			case "stale_device":
				p.DeviceSampledAt = p.DeviceSampledAt.Add(-16 * time.Second)
			case "unknown_device":
				p.DeviceAvailable = false
			case "high_latency":
				p.DeviceAwait = 3 * time.Millisecond
			case "l0_pressure":
				p.L0Sublevels = 8
			case "write_stall":
				p.WriteStalled = true
			}
			if (&transactionIndexBudget{}).healthy(p, time.Now()) {
				t.Fatal("unsafe measurement qualified for acceleration")
			}
		})
	}
	if got := transactionIndexWorkRecovery(20*time.Second, 200_000); got != 80*time.Second {
		t.Fatalf("complete work cost not retained: %s", got)
	}
	if got := transactionIndexWorkRecovery(time.Millisecond, 200_000); got != 3*time.Second {
		t.Fatalf("small batch has no minimum yield: %s", got)
	}
	// A sync transition after admission must not turn a small adaptive slot into
	// an unrestricted idle merge.
	r, store := newTransactionBudgetFixture(t)
	r.cfg.SyncActive = func() bool { return false }
	store.merge = true
	ctx := context.WithValue(context.Background(), transactionIndexBoundedContextKey{}, true)
	if changed, err := r.maintainTransactionIndexOnceContext(ctx); err != nil || !changed || store.mergeCalls.Load() != 0 || store.coverage != 8192 {
		t.Fatalf("bounded context allowed idle merge: %t/%v merges=%d coverage=%d", changed, err, store.mergeCalls.Load(), store.coverage)
	}
}

type transactionBudgetNotifyingStore struct {
	*auditTransactionIndexMaintenanceStore
	published chan uint64
}

func (s *transactionBudgetNotifyingStore) PublishTransactionIndexRun(result rawdbfreezer.TransactionIndexBuildResult) error {
	if err := s.auditTransactionIndexMaintenanceStore.PublishTransactionIndexRun(result); err != nil {
		return err
	}
	s.published <- result.EndBlock
	return nil
}

func TestTransactionIndexBudgetLoopRetriesBeforeOrdinaryInterval(t *testing.T) {
	r, store := newTransactionBudgetFixture(t)
	notifying := &transactionBudgetNotifyingStore{auditTransactionIndexMaintenanceStore: store, published: make(chan uint64, 8)}
	r.freezer = notifying
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	for _, want := range []uint64{8192, 16384} {
		select {
		case got := <-notifying.published:
			if got != want {
				t.Fatalf("timer published %d, want %d", got, want)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("index waited for the one-hour body ticker or fixed five-minute interval")
		}
	}
}

func TestTransactionIndexBudgetConcurrentExplicitAndLifecycleRequests(t *testing.T) {
	for _, withGate := range []bool{false, true} {
		r, _, _ := newTransactionReplayFixture(t)
		r.chain.(*fakeChain).setSolidified(-1)
		r.cfg.Interval = time.Millisecond
		r.cfg.TransactionIndexLoadProbe = healthyTransactionIndexPressure
		if withGate {
			r.cfg.HeavyWorkGate = maintenance.NewHeavyWorkGate()
		}
		if err := r.Start(); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					if _, err := r.MaintainTransactionIndexOnce(); err != nil {
						t.Errorf("concurrent maintenance: %v", err)
						return
					}
					_ = r.transactionIndexRetryDeadline()
					runtime.Gosched()
				}
			}()
		}
		wg.Wait()
		if err := r.Stop(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTransactionIndexBudgetContendedExpiredTimerDoesNotSpin(t *testing.T) {
	r, _ := newTransactionBudgetFixture(t)
	var observations atomic.Uint64
	r.cfg.SyncActive = func() bool { observations.Add(1); return true }
	r.txIndexBudget.retry = time.Now().Add(-time.Minute)
	r.txIndexMu.Lock() // An explicit caller already owns a complete leaf.
	defer r.txIndexMu.Unlock()
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	// The old one-millisecond overdue fallback invoked this callback hundreds
	// of times while the caller held the mutex. There is no body ticker here.
	time.Sleep(200 * time.Millisecond)
	if err := r.Stop(); err != nil {
		t.Fatal(err)
	}
	if attempts := observations.Load(); attempts > 2 {
		t.Fatalf("contended expired timer spun: %d sync observations", attempts)
	}
	if delay := transactionIndexRetryDelay(time.Now().Add(-time.Second), time.Now()); delay != transactionIndexRecovery {
		t.Fatalf("overdue contention delay=%s", delay)
	}
}

func TestTransactionIndexProgressLogSamplesHealthyQuantaAndPreservesIdle(t *testing.T) {
	r := &Runner{}
	now := time.Unix(5000, 0)
	if emit, suppressed := r.transactionIndexProgressLogDecision(true, false, now); !emit || suppressed != 0 {
		t.Fatalf("first healthy progress = %t/%d", emit, suppressed)
	}
	for i := 1; i < 10; i++ {
		if emit, _ := r.transactionIndexProgressLogDecision(true, false, now.Add(time.Duration(i)*3*time.Second)); emit {
			t.Fatalf("healthy leaf %d emitted another Info", i)
		}
	}
	if emit, suppressed := r.transactionIndexProgressLogDecision(true, false, now.Add(30*time.Second)); !emit || suppressed != 9 {
		t.Fatalf("aggregated progress = %t/%d, want true/9", emit, suppressed)
	}
	if emit, _ := r.transactionIndexProgressLogDecision(false, false, now.Add(31*time.Second)); !emit {
		t.Fatal("idle maintenance completion was suppressed")
	}
	if emit, _ := r.transactionIndexProgressLogDecision(true, false, now.Add(32*time.Second)); !emit {
		t.Fatal("return to healthy sync did not report progress")
	}
	if emit, _ := r.transactionIndexProgressLogDecision(true, true, now.Add(33*time.Second)); !emit {
		t.Fatal("final debt repayment was suppressed")
	}
	if quietTransactionIndexContext(nil) || quietTransactionIndexContext(context.Background()) {
		t.Fatal("ordinary maintenance unexpectedly lost Info details")
	}
	quiet := context.WithValue(context.Background(), transactionIndexQuietContextKey{}, true)
	if !quietTransactionIndexContext(quiet) {
		t.Fatal("healthy bounded context did not choose quiet detail logging")
	}
}

func TestTransactionIndexSmallBatchYieldsGateBeforeItsOwnRetry(t *testing.T) {
	r, store := newTransactionBudgetFixture(t)
	chain := r.chain.(*fakeChain)
	chain.db = &auditSyncDelayDB{KeyValueStore: chain.db, delay: 40 * time.Millisecond}
	gate := maintenance.NewHeavyWorkGateWithCooldown(15 * time.Second)
	r.cfg.HeavyWorkGate = gate
	if changed, err := r.MaintainTransactionIndexOnce(); err != nil || !changed {
		t.Fatalf("bounded batch = %t/%v", changed, err)
	}
	remaining := gate.CooldownRemaining()
	work := r.txIndexBudget.totalWork
	if remaining <= 0 || remaining > min(3*time.Second, work+100*time.Millisecond) {
		t.Fatalf("small batch held global recovery=%s after work=%s", remaining, work)
	}
	if remaining >= time.Until(r.transactionIndexRetryDeadline()) {
		t.Fatal("index retained the entire importer-only recovery as its next gate turn")
	}
	time.Sleep(remaining + 25*time.Millisecond)
	next, ok := gate.TryAcquireWithCooldown(0)
	if !ok {
		t.Fatal("other maintenance cannot run after measured recovery")
	}
	next()
	if changed, err := r.MaintainTransactionIndexOnce(); err != nil || changed || store.coverage != 8192 {
		t.Fatalf("shorter global pause removed index's own retry budget: %t/%v coverage=%d", changed, err, store.coverage)
	}
}

func TestTransactionIndexReleaseRechecksPressureBeforeReducingGlobalRecovery(t *testing.T) {
	for _, mode := range []string{"unknown", "stalled", "stale_device"} {
		t.Run(mode, func(t *testing.T) {
			r, _ := newTransactionBudgetFixture(t)
			gate := maintenance.NewHeavyWorkGateWithCooldown(7 * time.Second)
			r.cfg.HeavyWorkGate = gate
			calls := 0
			r.cfg.TransactionIndexLoadProbe = func() maintenance.StoragePressure {
				calls++
				p := healthyTransactionIndexPressure()
				if calls == 1 {
					return p
				}
				switch mode {
				case "unknown":
					return maintenance.StoragePressure{}
				case "stalled":
					p.WriteStalled = true
				case "stale_device":
					p.DeviceSampledAt = p.DeviceSampledAt.Add(-time.Minute)
				}
				return p
			}
			if changed, err := r.MaintainTransactionIndexOnce(); err != nil || !changed {
				t.Fatalf("batch = %t/%v", changed, err)
			}
			if remaining := gate.CooldownRemaining(); remaining < 5*time.Second || remaining > 7*time.Second {
				t.Fatalf("completion pressure lost configured default: %s", remaining)
			}
			if wait := time.Until(r.txIndexBudget.conservativeUntil); wait < 4*time.Minute {
				t.Fatalf("completion pressure removed conservative index work wait: %s", wait)
			}
			if wait := time.Until(r.transactionIndexRetryDeadline()); wait > transactionIndexHardRecheck {
				t.Fatalf("completion pressure did not schedule a bounded observation: %s", wait)
			}
		})
	}
}

func TestTransactionIndexFailedBatchKeepsDefaultGlobalRecovery(t *testing.T) {
	r, store := newTransactionBudgetFixture(t)
	store.body = []byte{0xff} // Real body decode fails after healthy admission.
	gate := maintenance.NewHeavyWorkGateWithCooldown(7 * time.Second)
	r.cfg.HeavyWorkGate = gate
	if changed, err := r.MaintainTransactionIndexOnce(); err == nil || changed {
		t.Fatalf("malformed body accepted: %t/%v", changed, err)
	}
	if remaining := gate.CooldownRemaining(); remaining < 5*time.Second || remaining > 7*time.Second {
		t.Fatalf("failed batch received accelerated global recovery: %s", remaining)
	}
}

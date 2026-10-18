package freezer

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
)

type directRetryObservedIndex struct {
	*freezerWriter
	observed *atomic.Bool
}

type directRetryBlockingChain struct {
	*fakeChain
	entered, release chan struct{}
	once             sync.Once
}

func (c *directRetryBlockingChain) ReadBlockRawStrict(n uint64) ([]byte, bool, error) {
	c.once.Do(func() { close(c.entered); <-c.release })
	return c.fakeChain.ReadBlockRawStrict(n)
}

func (f *directRetryObservedIndex) TransactionIndexCoverage() uint64 {
	f.observed.Store(true)
	return f.freezerWriter.TransactionIndexCoverage()
}

func newDirectRetryFixture(t *testing.T, gate *maintenance.HeavyWorkGate) (*Runner, *fakeChain) {
	t.Helper()
	c := newFakeChain()
	for n := uint64(0); n < 4; n++ {
		c.plantBlock(t, n)
	}
	c.setSolidified(3)
	c.receiptLogsCovered = true
	r := New(c, wrapFreezer(newFreezer(t)), Config{
		Enabled: true, Interval: time.Hour, V2Enabled: true, DirectV2: true,
		V2FrameBlocks: 2, V2SegmentBlocks: 4, ExternalizeV2ReceiptLogs: true,
		HeavyWorkGate:    gate,
		MetricsNamespace: "test/" + t.Name() + "/",
	})
	return r, c
}

func TestDirectV2RetryStopWhileExternalAdmissionBlocked(t *testing.T) {
	g := maintenance.NewHeavyWorkGate()
	entered, proceed, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	g.SetAdmissionCheck(func() bool { close(entered); <-proceed; return false })
	r, _ := newDirectRetryFixture(t, g)
	go func() { defer close(done); _, _ = r.OnePass() }()
	<-entered
	r.BeginStop()
	close(proceed)
	<-done
	if !r.directV2RetryDeadline().IsZero() || runnerGaugeValue(t, r.cfg.MetricsNamespace+"v2/retry/deadline_unix_ns") != 0 || runnerGaugeValue(t, r.cfg.MetricsNamespace+"v2/retry/reason") != 0 {
		t.Fatal("external pass installed a hint after BeginStop cleared it")
	}
}

func TestDirectV2RetryConcurrentReplacementMetrics(t *testing.T) {
	r, _ := newDirectRetryFixture(t, maintenance.NewHeavyWorkGate())
	for range 100 {
		now := time.Now()
		r.directV2RetryAt.Store(now.Add(-time.Second).UnixNano())
		var joined sync.WaitGroup
		joined.Add(2)
		go func() { defer joined.Done(); r.takeDirectV2Retry(now) }()
		go func() { defer joined.Done(); r.deferDirectV2Admission() }()
		joined.Wait()
		at := r.directV2RetryAt.Load()
		if at <= now.UnixNano() || runnerGaugeValue(t, r.cfg.MetricsNamespace+"v2/retry/deadline_unix_ns") != at || runnerGaugeValue(t, r.cfg.MetricsNamespace+"v2/retry/reason") != 1 {
			t.Fatal("old consumer cleared a future hint or its observation")
		}
	}
}

func TestDirectV2RetryExternalOnePassOwnsWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := maintenance.NewHeavyWorkGate()
		release, _ := g.TryAcquireNamed(maintenance.HeavyWorkHistory)
		r, c := newDirectRetryFixture(t, g)
		r.Start()
		defer r.Stop()
		synctest.Wait()
		blocker := &directRetryBlockingChain{fakeChain: c, entered: make(chan struct{}), release: make(chan struct{})}
		r.chain = blocker
		release()
		done := make(chan struct{})
		go func() { defer close(done); _, _ = r.OnePass() }()
		<-blocker.entered
		time.Sleep(10 * time.Second)
		synctest.Wait()
		if !r.directV2RetryDeadline().IsZero() || r.passesCompleted.Load() != 1 {
			t.Fatal("old timer spun while external OnePass owned workMu")
		}
		close(blocker.release)
		<-done
		if r.freezer.(V2Compactor).V2Coverage() != 4 {
			t.Fatal("external public pass failed to publish after releasing its reader")
		}
	})
}

func TestDirectV2RetryDoesNotSchedulePrunedLegacySource(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, _ := newDirectRetryFixture(t, maintenance.NewHeavyWorkGate())
		f := r.freezer.(*freezerWriter).f
		for n := uint64(0); n < 4; n++ {
			if _, err := f.ModifyAncients(func(op rawdb.AncientWriteOp) error {
				for _, row := range []struct {
					table string
					data  []byte
				}{{rawdbAncientBlocks, blockBytes(n)}, {rawdbAncientTxInfos, txInfosBytes(n)}, {rawdbAncientStateRoots, stateRootBytes(n)}} {
					if err := op.AppendRaw(row.table, n, row.data); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := f.MigrateV2(rawdbfreezer.V2MigrationOptions{Tables: []string{rawdbAncientBlocks, rawdbAncientTxInfos, rawdbAncientStateRoots}, SegmentBlocks: 2, FrameBlocks: 1, MaxSegments: 1, Online: true}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.TruncateTail(3); err != nil {
			t.Fatal(err)
		}
		r.Start()
		defer r.Stop()
		synctest.Wait()
		before := r.Snapshot()
		if before.V2SourcePrunedDeferred != 1 || !r.directV2RetryDeadline().IsZero() {
			t.Fatal("pruned source entered active direct retry")
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if r.Snapshot().PassesCompleted != before.PassesCompleted || r.Snapshot().V2SourcePrunedDeferred != 1 {
			t.Fatal("pruned legacy source retried before ordinary cadence")
		}
	})
}

// This regression uses the real direct writer and durable freezer coverage.
// With the previous coarse loop it remains at coverage zero until the hour
// ticker, despite the blocking history owner releasing at t=2 seconds.
func TestDirectV2AdmissionRetryBeforeCoarseTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := maintenance.NewHeavyWorkGate()
		history, ok := gate.TryAcquireNamed(maintenance.HeavyWorkHistory)
		if !ok {
			t.Fatal("history did not acquire")
		}
		r, c := newDirectRetryFixture(t, gate)
		if err := r.Start(); err != nil {
			t.Fatal(err)
		}
		defer r.Stop()
		synctest.Wait()
		if r.Snapshot().PassesCompleted != 1 || r.Snapshot().V2BlocksCompacted != 0 {
			t.Fatal("initial blocked pass changed durable coverage")
		}
		time.Sleep(2 * time.Second)
		if got := r.freezer.(V2Compactor).V2Coverage(); got != 0 {
			t.Fatal("freezer preempted the actual history lease")
		}
		history()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if got := r.freezer.(V2Compactor).V2Coverage(); got != 4 {
			t.Fatalf("coverage=%d, want complete durable segment before coarse tick", got)
		}
		if _, ok, err := rawdb.ReadBlockRawStrict(c.db, 3); err != nil || ok {
			t.Fatalf("durable publication did not prune hot body: present=%v err=%v", ok, err)
		}
		// Successful work has no new direct retry: no success-driven loop.
		passes := r.Snapshot().PassesCompleted
		time.Sleep(time.Minute)
		synctest.Wait()
		if r.Snapshot().PassesCompleted != passes {
			t.Fatal("success created a fast follow-up pass")
		}
	})
}

func TestDirectV2AdmissionRetryHonorsCooldownAndColdLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := maintenance.NewHeavyWorkGateWithCooldown(7 * time.Second)
		release, _ := gate.TryAcquireNamed(maintenance.HeavyWorkHistory)
		release()
		r, _ := newDirectRetryFixture(t, gate)
		r.Start()
		defer r.Stop()
		synctest.Wait()
		time.Sleep(6 * time.Second)
		synctest.Wait()
		if r.Snapshot().PassesCompleted != 1 || r.freezer.(V2Compactor).V2Coverage() != 0 {
			t.Fatal("retry shortened existing recovery")
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if r.freezer.(V2Compactor).V2Coverage() != 4 {
			t.Fatal("known recovery expiry fell back to coarse tick")
		}
	})
}

func TestDirectV2AdmissionRetryBoundedBusyAndStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := maintenance.NewHeavyWorkGate()
		release, _ := g.TryAcquireNamed(maintenance.HeavyWorkHistory)
		defer release()
		r, _ := newDirectRetryFixture(t, g)
		r.Start()
		synctest.Wait()
		time.Sleep(10 * time.Second)
		synctest.Wait()
		if got := r.Snapshot().PassesCompleted; got != 4 {
			t.Fatalf("busy retries=%d, want initial plus t3,t6,t9", got)
		}
		if g.Observation().Owner != maintenance.HeavyWorkHistory {
			t.Fatal("retry stole a live cold owner")
		}
		r.Stop()
		before := r.Snapshot().PassesCompleted
		time.Sleep(time.Hour)
		synctest.Wait()
		if !r.directV2RetryDeadline().IsZero() || r.Snapshot().PassesCompleted != before {
			t.Fatal("Stop retained timer or performed late work")
		}
	})
}

func TestDirectV2AdmissionRetryRechecksEligibilityAndErrors(t *testing.T) {
	for _, name := range []string{"incomplete", "receipts", "promotion", "error", "work_busy"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				g := maintenance.NewHeavyWorkGate()
				release, _ := g.TryAcquireNamed(maintenance.HeavyWorkHistory)
				r, c := newDirectRetryFixture(t, g)
				var promotionAllowed atomic.Bool
				promotionAllowed.Store(true)
				r.cfg.V2PromotionAllowed = promotionAllowed.Load
				r.Start()
				synctest.Wait()
				switch name {
				case "incomplete":
					c.setSolidified(2)
				case "receipts":
					c.mu.Lock()
					c.receiptLogsCovered = false
					c.mu.Unlock()
				case "promotion":
					promotionAllowed.Store(false)
				case "error":
					c.mu.Lock()
					c.receiptLogsErr = errors.New("coverage I/O failed")
					c.mu.Unlock()
				case "work_busy":
					r.workMu.Lock()
				}
				release()
				time.Sleep(4 * time.Second)
				synctest.Wait()
				if name == "work_busy" {
					r.workMu.Unlock()
				}
				if !r.directV2RetryDeadline().IsZero() {
					t.Fatal("consumed hint retained a fast retry after loss of eligibility/error/workMu busy")
				}
				passes := r.Snapshot().PassesCompleted
				time.Sleep(time.Minute)
				synctest.Wait()
				if r.Snapshot().PassesCompleted != passes || r.freezer.(V2Compactor).V2Coverage() != 0 {
					t.Fatal("stale retry spun or bypassed an eligibility/error gate")
				}
				r.Stop()
			})
		})
	}
}

func TestDirectV2RetryDisabledOrLegacyPlanClearsHint(t *testing.T) {
	for _, direct := range []bool{false, true} {
		r, _ := newDirectRetryFixture(t, maintenance.NewHeavyWorkGate())
		// Configuration is immutable once Start launches its goroutine. Exercise
		// these explicit no-loop entrypoints without mutating a live config.
		r.cfg.DirectV2 = direct
		r.cfg.Enabled = !direct
		r.directV2RetryAt.Store(time.Now().Add(-time.Second).UnixNano())
		if _, err := r.OnePass(); err != nil || !r.directV2RetryDeadline().IsZero() {
			t.Fatalf("disabled/legacy plan retained stale hint: %v", err)
		}
	}
}

func TestDirectV2AdmissionRetryDueIndexFirst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := maintenance.NewHeavyWorkGate()
		release, _ := g.TryAcquireNamed(maintenance.HeavyWorkHistory)
		r, _ := newDirectRetryFixture(t, g)
		var indexObserved atomic.Bool
		var afterInitial atomic.Bool
		r.freezer = &directRetryObservedIndex{freezerWriter: r.freezer.(*freezerWriter), observed: &indexObserved}
		r.cfg.SyncActive = func() bool { return true }
		r.cfg.TransactionIndexEnabled = true
		r.cfg.TransactionIndexLoadProbe = func() maintenance.StoragePressure {
			return maintenance.StoragePressure{} // Existing conservative index retry.
		}
		r.cfg.V2PromotionAllowed = func() bool {
			if afterInitial.Load() && !indexObserved.Load() {
				t.Error("new body timer ran before an already-due index quantum")
			}
			return true
		}
		r.Start()
		defer r.Stop()
		synctest.Wait()
		indexObserved.Store(false)
		afterInitial.Store(true)
		r.txIndexBudget.mu.Lock()
		r.txIndexBudget.retry = time.Now().Add(3 * time.Second)
		r.txIndexBudget.mu.Unlock()
		release()
		time.Sleep(4 * time.Second)
		synctest.Wait()
		if !indexObserved.Load() || r.freezer.(V2Compactor).V2Coverage() != 4 {
			t.Fatal("index was swallowed or body failed after the index opportunity")
		}
	})
}

func TestDirectV2RetryDeadlineConsumption(t *testing.T) {
	r, _ := newDirectRetryFixture(t, maintenance.NewHeavyWorkGate())
	now := time.Now()
	r.directV2RetryAt.Store(now.Add(time.Second).UnixNano())
	if r.takeDirectV2Retry(now) || r.directV2RetryDeadline().IsZero() {
		t.Fatal("future replacement hint was consumed early")
	}
	if !r.takeDirectV2Retry(now.Add(2*time.Second)) || r.takeDirectV2Retry(now.Add(2*time.Second)) {
		t.Fatal("expired hint was not consumed exactly once")
	}
	if directV2RetryDelay(now.Add(-time.Hour), now) != 3*time.Second {
		t.Fatal("past deadline can spin")
	}
}

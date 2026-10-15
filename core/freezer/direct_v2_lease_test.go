package freezer

import (
	"context"
	"fmt"
	"github.com/tronprotocol/go-tron/core/maintenance"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestDirectV2LeaseFairnessAfterThirdOwnerAndRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := maintenance.NewHeavyWorkGateWithCooldown(time.Second)
		release, _ := g.TryAcquireNamed(maintenance.HeavyWorkDirectV2)
		r := New(newFakeChain(), wrapFreezer(newFreezer(t)), Config{Enabled: true, HeavyWorkGate: g})
		lease := &directV2Lease{r: r, release: release, since: time.Now().Add(-11 * time.Second)}
		defer lease.Close()
		if _, ok := g.TryAcquireNamed(maintenance.HeavyWorkHistory); ok {
			t.Fatal("history entered active freezer lease")
		}
		if !lease.hooks().Due() {
			t.Fatal("fresh contender did not make long lease yieldable")
		}
		var resumed atomic.Bool
		go func() {
			if err := lease.yield(context.Background()); err != nil {
				t.Error(err)
			}
			resumed.Store(true)
		}()
		time.Sleep(time.Second)
		third, ok := g.TryAcquire()
		if !ok {
			t.Fatal("third owner did not obtain released token")
		}
		time.Sleep(5 * time.Second)
		if resumed.Load() {
			t.Fatal("freezer resumed across active third owner")
		}
		third()
		time.Sleep(time.Second) // Latest recovery just expired. History next retries at t=9.
		if resumed.Load() {
			t.Fatal("freezer raced the third owner's recovery deadline")
		}
		time.Sleep(2 * time.Second)
		history, ok := g.TryAcquireNamed(maintenance.HeavyWorkHistory)
		if !ok {
			t.Fatal("history missed its ordinary retry opportunity")
		}
		time.Sleep(time.Second)
		if resumed.Load() {
			t.Fatal("freezer overlapped history")
		}
		history()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if !resumed.Load() || g.Observation().HistoryAcquisitions != 1 {
			t.Fatal("no actual history handoff/resume")
		}
	})
}
func TestDirectV2LeaseStaleContentionCancelAndNewContextRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := maintenance.NewHeavyWorkGate()
		release, _ := g.TryAcquireNamed(maintenance.HeavyWorkDirectV2)
		r := New(newFakeChain(), wrapFreezer(newFreezer(t)), Config{Enabled: true, HeavyWorkGate: g})
		l := &directV2Lease{r: r, release: release, since: time.Now().Add(-11 * time.Second)}
		if l.hooks().Due() {
			t.Fatal("yield without contender")
		}
		g.TryAcquireNamed(maintenance.HeavyWorkHistory)
		time.Sleep(11 * time.Second)
		if l.hooks().Due() {
			t.Fatal("stale history contention remained eligible")
		}
		g.TryAcquireNamed(maintenance.HeavyWorkHistory)
		if !l.hooks().Due() {
			t.Fatal("fresh retry ignored")
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := l.yield(ctx); err != context.Canceled {
			t.Fatal(err)
		}
		if l.release != nil || !g.CanTryAcquire() {
			t.Fatal("canceled yield leaked token")
		}
		// The sync-activation fallback uses a different non-canceled context. It
		// must acquire and install a new token rather than reuse the old callback.
		if err := l.ensure(context.Background()); err != nil {
			t.Fatal(err)
		}
		if g.CanTryAcquire() {
			t.Fatal("retry ran without shared lease")
		}
		l.Close()
		l.Close()
		if !g.CanTryAcquire() {
			t.Fatal("latest lease not released")
		}
	})
}
func TestDirectV2LeaseMissingContenderOpportunityTimesOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := maintenance.NewHeavyWorkGateWithCooldown(time.Second)
		release, _ := g.TryAcquireNamed(maintenance.HeavyWorkDirectV2)
		r := New(newFakeChain(), wrapFreezer(newFreezer(t)), Config{Enabled: true, HeavyWorkGate: g})
		l := &directV2Lease{r: r, release: release, since: time.Now().Add(-11 * time.Second)}
		defer l.Close()
		g.TryAcquireNamed(maintenance.HeavyWorkHistory)
		started := time.Now()
		if err := l.yield(context.Background()); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(started); elapsed < 4*time.Second || elapsed > 5*time.Second {
			t.Fatalf("unbounded or no real opportunity: %s", elapsed)
		}
		if g.Observation().HistoryAcquisitions != 0 {
			t.Fatal("timeout reported as history progress")
		}
	})
}
func TestRunnerWorkMuRejectsAllMaintenanceEntrypoints(t *testing.T) {
	r := New(newFakeChain(), wrapFreezer(newFreezer(t)), Config{Enabled: true, V2Enabled: true, TransactionIndexEnabled: true})
	r.workMu.Lock()
	defer r.workMu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if n, err := r.OnePass(); n != 0 || err != nil {
			t.Error(n, err)
		}
		if n, err := r.CompactV2Once(); n != 0 || err != nil {
			t.Error(n, err)
		}
		if changed, err := r.MaintainTransactionIndexOnce(); changed || err != nil {
			t.Error(changed, err)
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("public entrypoint waited for job or acquired gate before workMu")
	}
}

type yieldingSyncTransitionStore struct {
	*freezerWriter
	gate     *maintenance.HeavyWorkGate
	runner   *Runner
	activate func()
	calls    int
	t        *testing.T
}

func (s *yieldingSyncTransitionStore) MigrateV2(o rawdbfreezer.V2MigrationOptions) (rawdbfreezer.V2MigrationResult, error) {
	s.calls++
	if s.calls == 1 {
		if o.TransactionIndexEntries == nil || o.CooperativeYield == nil {
			s.t.Fatal("not a cooperative fused build")
		}
		original := o.CooperativeYield.Yield
		o.CooperativeYield.Due = func() bool { return true }
		o.CooperativeYield.Yield = func(ctx context.Context) error {
			checked := make(chan error, 1)
			go func() {
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for {
					if release, ok := s.gate.TryAcquireNamed(maintenance.HeavyWorkHistory); ok {
						// A real cold participant can enter both reader/tail-prune locks. Public
						// Runner methods must reject BEFORE acquiring gate or migration locks.
						if got := s.f.V1Tail(); got != 0 {
							checked <- fmt.Errorf("partial V1 head %d", got)
							release()
							return
						}
						if _, err := s.f.TruncateTail(0); err != nil {
							checked <- err
							release()
							return
						}
						if n, err := s.runner.OnePass(); n != 0 || err != nil {
							checked <- fmt.Errorf("concurrent OnePass %d %v", n, err)
							release()
							return
						}
						if n, err := s.runner.CompactV2Once(); n != 0 || err != nil {
							checked <- fmt.Errorf("concurrent V2 %d %v", n, err)
							release()
							return
						}
						if changed, err := s.runner.MaintainTransactionIndexOnce(); changed || err != nil {
							checked <- fmt.Errorf("concurrent index %v %v", changed, err)
							release()
							return
						}
						s.activate()
						<-ctx.Done()
						release()
						checked <- nil
						return
					}
					select {
					case <-ctx.Done():
						checked <- ctx.Err()
						return
					case <-ticker.C:
					}
				}
			}()
			err := original(ctx)
			if checkErr := <-checked; checkErr != nil {
				s.t.Error(checkErr)
			}
			return err
		}
	} else {
		if s.gate.CanTryAcquire() {
			s.t.Fatal("unfused retry started without new lease")
		}
		if o.TransactionIndexEntries != nil {
			s.t.Fatal("retry still fused")
		}
	}
	return s.freezerWriter.MigrateV2(o)
}
func TestDirectV2CooperativeSyncCancellationReacquiresForUnfusedRetry(t *testing.T) {
	chain := newDirectV2PreparedFixture(t, 4, 1)
	chain.setSolidified(3)
	base := newFreezer(t)
	g := maintenance.NewHeavyWorkGate()
	var syncing atomic.Bool
	store := &yieldingSyncTransitionStore{freezerWriter: wrapFreezer(base).(*freezerWriter), gate: g, activate: func() { syncing.Store(true) }, t: t}
	r := New(chain, store, Config{Enabled: true, V2Enabled: true, DirectV2: true, V2FrameBlocks: 2, V2SegmentBlocks: 4, TransactionIndexEnabled: true, TransactionIndexPrefixBits: 8, HeavyWorkGate: g, SyncActive: syncing.Load})
	store.runner = r
	n, err := r.OnePass()
	if err != nil || n != 4 || store.calls != 2 {
		t.Fatalf("freeze=%d calls=%d err=%v", n, store.calls, err)
	}
	if base.V2Coverage() != 4 || base.TransactionIndexCoverage() != 0 || !g.CanTryAcquire() {
		t.Fatalf("coverage=%d index=%d gate available=%v", base.V2Coverage(), base.TransactionIndexCoverage(), g.CanTryAcquire())
	}
}

func TestDirectV2LeaseOpportunityWaitsForReservationExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := maintenance.NewHeavyWorkGate()
		release, _ := g.TryAcquireNamed(maintenance.HeavyWorkDirectV2)
		reservation := g.ReserveNext(8 * time.Second)
		defer reservation.Cancel()
		r := New(newFakeChain(), wrapFreezer(newFreezer(t)), Config{Enabled: true, HeavyWorkGate: g})
		l := &directV2Lease{r: r, release: release, since: time.Now().Add(-11 * time.Second)}
		defer l.Close()
		var resumed atomic.Bool
		go func() {
			if err := l.yield(context.Background()); err != nil {
				t.Error(err)
			}
			resumed.Store(true)
		}()
		time.Sleep(9 * time.Second)
		if resumed.Load() {
			t.Fatal("reservation lifetime counted as cold retry opportunity")
		}
		cold, ok := g.TryAcquireNamed(maintenance.HeavyWorkHistory)
		if !ok {
			t.Fatal("history did not get opportunity after reservation expired")
		}
		cold()
		time.Sleep(time.Second)
		synctest.Wait()
		if !resumed.Load() {
			t.Fatal("no resume after observed history acquisition")
		}
	})
}
func TestDirectV2LeaseResumeDoesNotBypassPressure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := maintenance.NewHeavyWorkGate()
		release, _ := g.TryAcquireNamed(maintenance.HeavyWorkDirectV2)
		g.SetAdmissionCheck(func() bool { return false })
		r := New(newFakeChain(), wrapFreezer(newFreezer(t)), Config{Enabled: true, HeavyWorkGate: g})
		l := &directV2Lease{r: r, release: release, since: time.Now().Add(-11 * time.Second)}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := l.yield(ctx); err != context.DeadlineExceeded {
			t.Fatal(err)
		}
		if l.release != nil || g.Observation().RejectedAdmission == 0 {
			t.Fatal("resumption bypassed pressure")
		}
		g.SetAdmissionCheck(nil)
		if !g.CanTryAcquire() {
			t.Fatal("canceled pressure wait leaked token")
		}
	})
}

package freezer

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

func recoveryTestLease(t *testing.T, g *maintenance.HeavyWorkGate, probe func() maintenance.StoragePressure) *directV2Lease {
	t.Helper()
	r := New(newFakeChain(), wrapFreezer(newFreezer(t)), Config{Enabled: true, HeavyWorkGate: g, TransactionIndexLoadProbe: probe})
	l := &directV2Lease{r: r}
	if !l.tryAcquire() {
		t.Fatal("no initial lease")
	}
	t.Cleanup(l.Close)
	return l
}
func TestDirectV2RecoveryHealthyYieldThenFinalDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := maintenance.NewHeavyWorkGateWithCooldownAfter(15*time.Second, 250*time.Millisecond)
		l := recoveryTestLease(t, g, healthyTransactionIndexPressure)
		time.Sleep(12 * time.Second)
		l.releaseForYield(context.Background())
		if got := g.CooldownRemaining(); got != 3*time.Second {
			t.Fatalf("healthy pause recovery %s", got)
		}
		// ensure must install a new, conservative release decision.
		if err := l.ensure(context.Background()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(11 * time.Second)
		l.Close()
		if got := g.CooldownRemaining(); got != 15*time.Second {
			t.Fatalf("final inherited optimistic recovery: %s", got)
		}
	})
}
func TestDirectV2RecoveryEvidenceAndUnknownDefault(t *testing.T) {
	for _, name := range []string{"missing_probe", "unknown", "same_engine", "same_device", "same_both", "future_engine", "future_device", "stale_engine", "slow_probe", "soft_device", "hard_stall"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				g := maintenance.NewHeavyWorkGateWithCooldown(15 * time.Second)
				var begin maintenance.StoragePressure
				calls := 0
				probe := func() maintenance.StoragePressure {
					calls++
					p := healthyTransactionIndexPressure()
					if calls == 1 {
						begin = p
						return p
					}
					switch name {
					case "unknown":
						p.Available = false
					case "same_engine":
						p.SampledAt = begin.SampledAt
					case "same_device":
						p.DeviceSampledAt = begin.DeviceSampledAt
					case "same_both":
						p = begin
					case "future_engine":
						p.SampledAt = time.Now().Add(2 * time.Second)
					case "future_device":
						p.DeviceSampledAt = time.Now().Add(2 * time.Second)
					case "stale_engine":
						p.SampledAt = time.Now().Add(-16 * time.Second)
					case "slow_probe":
						time.Sleep(16 * time.Second)
					case "soft_device":
						p.DeviceAwait = 3 * time.Millisecond
					case "hard_stall":
						p.WriteStalled = true
					}
					return p
				}
				if name == "missing_probe" {
					probe = nil
				}
				l := recoveryTestLease(t, g, probe)
				time.Sleep(10 * time.Second)
				l.releaseForYield(context.Background())
				if got := g.CooldownRemaining(); got != 15*time.Second {
					t.Fatalf("unsafe %s recovery %s", name, got)
				}
			})
		})
	}
}
func TestDirectV2RecoveryPressureRecoveryRequiresDistinctLeases(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := maintenance.NewHeavyWorkGateWithCooldown(15 * time.Second)
		soft := true
		l := recoveryTestLease(t, g, func() maintenance.StoragePressure {
			p := healthyTransactionIndexPressure()
			if soft {
				p.DeviceAwait = 3 * time.Millisecond
			}
			return p
		})
		time.Sleep(10 * time.Second)
		l.releaseForYield(context.Background())
		if g.CooldownRemaining() != 15*time.Second {
			t.Fatal("soft release optimistic")
		}
		time.Sleep(15 * time.Second)
		soft = false
		if !l.tryAcquire() || l.recovery.begin != txIndexHealthRecovering {
			t.Fatal("first new healthy observation recovered prematurely")
		}
		time.Sleep(10 * time.Second)
		l.releaseForYield(context.Background())
		if g.CooldownRemaining() != 15*time.Second {
			t.Fatal("recovering begin allowed short release")
		}
		time.Sleep(15 * time.Second)
		if !l.tryAcquire() || l.recovery.begin != txIndexHealthHealthy {
			t.Fatal("two distinct observations did not recover")
		}
		time.Sleep(12 * time.Second)
		l.releaseForYield(context.Background())
		if g.CooldownRemaining() != 3*time.Second {
			t.Fatal("healthy following lease did not use bounded recovery")
		}
	})
}
func TestDirectV2RecoveryDebtTrendPersistsAcrossLeases(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := maintenance.NewHeavyWorkGateWithCooldown(15 * time.Second)
		debt := uint64(2 << 30)
		l := recoveryTestLease(t, g, func() maintenance.StoragePressure {
			p := healthyTransactionIndexPressure()
			p.CompactionDebt = debt
			return p
		})
		time.Sleep(10 * time.Second)
		debt++
		l.releaseForYield(context.Background())
		time.Sleep(g.CooldownRemaining())
		if !l.tryAcquire() {
			t.Fatal("reacquire")
		}
		time.Sleep(10 * time.Second)
		debt++
		l.releaseForYield(context.Background())
		if l.pressure.debtRises != 2 || g.CooldownRemaining() != 15*time.Second {
			t.Fatalf("lost job trend rises=%d recovery=%s", l.pressure.debtRises, g.CooldownRemaining())
		}
		if l.r.txIndexBudget.pressure.lastDebt != 0 {
			t.Fatal("modified transaction-index budget state")
		}
	})
}
func TestDirectV2RecoveryCancelDuringProbeAndProbePanics(t *testing.T) {
	for _, mode := range []string{"cancel_before", "cancel_during", "begin_panic", "end_panic"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				g := maintenance.NewHeavyWorkGateWithCooldownAfter(15*time.Second, 250*time.Millisecond)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				calls := 0
				r := New(newFakeChain(), wrapFreezer(newFreezer(t)), Config{Enabled: true, HeavyWorkGate: g, TransactionIndexLoadProbe: func() maintenance.StoragePressure {
					calls++
					if mode == "begin_panic" && calls == 1 {
						time.Sleep(time.Second)
						panic("begin probe")
					}
					if calls == 2 {
						if mode == "end_panic" {
							panic("end probe")
						}
						if mode == "cancel_during" {
							cancel()
						}
					}
					return healthyTransactionIndexPressure()
				}})
				l := &directV2Lease{r: r}
				var recovered any
				func() {
					defer func() { recovered = recover() }()
					if !l.tryAcquire() {
						t.Fatal("no lease")
					}
					time.Sleep(10 * time.Second)
					if mode == "cancel_before" {
						cancel()
					}
					l.releaseForYield(ctx)
				}()
				if (mode == "begin_panic" || mode == "end_panic") && recovered == nil {
					t.Fatal("panic not propagated")
				}
				if l.release != nil || g.CooldownRemaining() != 15*time.Second {
					t.Fatalf("cancel/panic leak or optimistic release: %s", g.CooldownRemaining())
				}
			})
		})
	}
}
func TestDirectV2RecoveryThirdOwnerDeadlineAndHistoryOpportunity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := maintenance.NewHeavyWorkGateWithCooldown(15 * time.Second)
		l := recoveryTestLease(t, g, healthyTransactionIndexPressure)
		time.Sleep(12 * time.Second)
		g.NotifyHistoryContention()
		if !l.hooks().Due() {
			t.Fatal("actual Due false")
		}
		done := make(chan error, 1)
		go func() { done <- l.yield(context.Background()) }()
		time.Sleep(3 * time.Second)
		synctest.Wait()
		third, ok := g.TryAcquire()
		if !ok {
			t.Fatal("no third lease after bounded3s recovery")
		}
		time.Sleep(5 * time.Second)
		third()
		time.Sleep(15 * time.Second)
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("resumed at third cooldown before history retry")
		default:
		}
		time.Sleep(time.Second)
		cold, ok := g.TryAcquireNamedWithCooldown(maintenance.HeavyWorkHistory, time.Second)
		if !ok {
			t.Fatal("cold normal retry missed")
		}
		time.Sleep(time.Second)
		cold()
		time.Sleep(time.Second)
		synctest.Wait()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("did not resume")
		}
		if g.Observation().HistoryAcquisitions != 1 {
			t.Fatal("no history handoff")
		}
	})
}
func TestDirectV2RecoveryPolicyDefaultZeroAndBounds(t *testing.T) {
	p := directV2Recovery{cooperative: true}
	for _, tc := range []struct{ held, base, want time.Duration }{{10 * time.Second, 0, 0}, {time.Second, 15 * time.Second, time.Second}, {12 * time.Second, 15 * time.Second, 3 * time.Second}, {time.Minute, 2 * time.Second, 2 * time.Second}} {
		if got := p.cooldown(tc.held, tc.base); got != tc.want {
			t.Fatalf("got%s want%s", got, tc.want)
		}
	}
	p.cooperative = false
	if p.cooldown(time.Second, 15*time.Second) != 15*time.Second {
		t.Fatal("normal close policy not default")
	}
}

func TestDirectV2RecoveryTerminalFinishAlwaysDefault(t *testing.T) {
	for _, terminal := range []string{"success", "source_error", "canceled"} {
		t.Run(terminal, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				g := maintenance.NewHeavyWorkGateWithCooldown(15 * time.Second)
				calls := 0
				l := recoveryTestLease(t, g, func() maintenance.StoragePressure { calls++; return healthyTransactionIndexPressure() })
				l.jobStart = time.Now()
				time.Sleep(12 * time.Second)
				var err error
				if terminal == "source_error" {
					err = errors.New("source failed")
				}
				if terminal == "canceled" {
					err = context.Canceled
				}
				l.finish(err)
				if g.CooldownRemaining() != 15*time.Second || calls != 1 {
					t.Fatalf("terminal optimistic/probed: %s calls%d", g.CooldownRemaining(), calls)
				}
			})
		})
	}
}
func TestDirectV2RecoveryCanceledResumeProbeDefaultsAndReturnsToken(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := maintenance.NewHeavyWorkGateWithCooldown(15 * time.Second)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		calls := 0
		r := New(newFakeChain(), wrapFreezer(newFreezer(t)), Config{Enabled: true, HeavyWorkGate: g, TransactionIndexLoadProbe: func() maintenance.StoragePressure {
			calls++
			time.Sleep(time.Second)
			cancel()
			return healthyTransactionIndexPressure()
		}})
		l := &directV2Lease{r: r}
		if err := l.ensure(ctx); err != context.Canceled {
			t.Fatalf("resume canceled probe: %v", err)
		}
		if l.release != nil || g.CooldownRemaining() != 15*time.Second || calls != 1 {
			t.Fatal("canceled resume retained token or shortened recovery")
		}
	})
}
